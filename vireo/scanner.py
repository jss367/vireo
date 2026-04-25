"""Scan folders, discover photos, read metadata, populate database."""

import contextlib
import hashlib
import json
import logging
import multiprocessing
import os
import sys
from collections import defaultdict, deque
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
from pathlib import Path

import imagehash
from db import commit_with_retry
from image_loader import RAW_EXTENSIONS, SUPPORTED_EXTENSIONS, extract_working_copy
from metadata import extract_metadata
from PIL import Image
from xmp import read_hierarchical_keywords, read_keywords

log = logging.getLogger(__name__)

# scan() runs inside JobRunner/pipeline_job background threads, so the
# default POSIX "fork" start method is unsafe here: forking a
# multithreaded process can deadlock. Prefer "forkserver" (POSIX, cheap)
# and fall back to "spawn" (universal).
_SCAN_MP_METHOD = (
    "forkserver"
    if "forkserver" in multiprocessing.get_all_start_methods()
    else "spawn"
)

# Windows' ProcessPoolExecutor raises ValueError when max_workers > 61
# (the WaitForMultipleObjects handle limit). Clamp on Windows so scans
# don't fail on high-core-count machines or misconfigured scan_workers.
_WINDOWS_MAX_WORKERS = 61


def compute_file_hash(file_path, chunk_size=65536):
    """Compute SHA-256 hash of a file. Returns hex digest string."""
    h = hashlib.sha256()
    with open(file_path, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def _compute_file_features(path_str):
    """Compute (phash, file_hash) for one image.

    Module-level so ProcessPoolExecutor can pickle it. Mirrors the
    best-effort behavior the main scan loop used to have inline: any
    failure yields None for that field rather than raising.
    """
    phash = None
    with contextlib.suppress(Exception), Image.open(path_str) as img:
        phash = str(imagehash.phash(img))
    file_hash = None
    with contextlib.suppress(Exception):
        file_hash = compute_file_hash(path_str)
    return phash, file_hash


def _resolve_worker_count(files_to_process):
    """Decide how many workers to use for feature computation.

    Returns 1 (sequential) when the batch is tiny or config disables
    parallelism; otherwise honors ``scan_workers`` (0 = auto, cap at
    cpu_count and batch size).
    """
    n = len(files_to_process)
    if n < 8:
        return 1
    try:
        import config as cfg
        configured = int(cfg.get("scan_workers") or 0)
    except Exception:
        configured = 0
    if configured == 1:
        return 1
    cpu = os.cpu_count() or 1
    if configured <= 0:
        workers = cpu
    else:
        workers = min(configured, cpu)
    if sys.platform == "win32":
        workers = min(workers, _WINDOWS_MAX_WORKERS)
    return max(1, min(workers, n))


def _import_keywords_for_photo(db, photo_id, xmp_path_str):
    """Read flat and hierarchical keywords from XMP and populate the database."""
    flat_keywords = read_keywords(xmp_path_str)
    hier_keywords = read_hierarchical_keywords(xmp_path_str)

    # Build hierarchy from lr:hierarchicalSubject
    # e.g., 'Birds|Raptors|Black kite' creates Birds -> Raptors -> Black kite
    for hier in hier_keywords:
        parts = hier.split("|")
        parent_id = None
        for part in parts:
            kid = db.add_keyword(part, parent_id=parent_id)
            parent_id = kid
        # Tag with the leaf keyword
        db.tag_photo(photo_id, parent_id)

    # Also add any flat keywords not already covered by hierarchy
    existing_kw_names = {k["name"] for k in db.get_photo_keywords(photo_id)}
    for kw in flat_keywords:
        if kw not in existing_kw_names:
            kid = db.add_keyword(kw)
            db.tag_photo(photo_id, kid)


def _extract_dimensions(exif_group, file_group, extension=None):
    """Extract width and height from ExifTool metadata groups.

    For standard images (JPEG, PNG, etc.):
    1. EXIF:ExifImageWidth / EXIF:ExifImageHeight
    2. EXIF:ImageWidth / EXIF:ImageHeight
    3. File:ImageWidth / File:ImageHeight

    For RAW files (NEF, CR2, ARW, etc.), ExifImageWidth/Height contains the
    embedded JPEG thumbnail dimensions (e.g. 160x120), not the actual image.
    Priority for RAW:
    1. File:ImageWidth / File:ImageHeight (actual decoded dimensions)
    2. EXIF:ImageWidth / EXIF:ImageHeight
    """
    is_raw = extension and extension.lower() in RAW_EXTENSIONS

    if is_raw:
        width = file_group.get("ImageWidth")
        if width is None:
            width = exif_group.get("ImageWidth")
        height = file_group.get("ImageHeight")
        if height is None:
            height = exif_group.get("ImageHeight")
    else:
        width = exif_group.get("ExifImageWidth")
        if width is None:
            width = exif_group.get("ImageWidth")
        if width is None:
            width = file_group.get("ImageWidth")
        height = exif_group.get("ExifImageHeight")
        if height is None:
            height = exif_group.get("ImageHeight")
        if height is None:
            height = file_group.get("ImageHeight")

    if width is not None:
        width = int(width)
    if height is not None:
        height = int(height)
    return width, height


def _extract_timestamp(exif_group):
    """Extract and normalize timestamp from ExifTool EXIF group.

    Checks EXIF:DateTimeOriginal first, then EXIF:CreateDate.
    If SubSecTimeOriginal (or SubSecTime) is present and numeric,
    it is included as fractional seconds for sub-second precision.
    Returns ISO format string or None.
    """
    dto = exif_group.get("DateTimeOriginal") or exif_group.get("CreateDate")
    if not dto:
        return None
    try:
        dt = datetime.strptime(str(dto), "%Y:%m:%d %H:%M:%S")
        # Attempt to add sub-second precision
        subsec = exif_group.get("SubSecTimeOriginal") or exif_group.get("SubSecTime")
        if subsec is not None:
            subsec_str = str(subsec).strip()
            if subsec_str.isdigit():
                # Pad or truncate to 6 digits (microseconds)
                us_str = subsec_str[:6].ljust(6, "0")
                dt = dt.replace(microsecond=int(us_str))
        return dt.isoformat()
    except (ValueError, TypeError):
        log.debug("Unparseable EXIF timestamp dropped: %r", dto)
        return None


def _pair_raw_jpeg_companions(db):
    """Find raw+JPEG pairs in the same folder and merge them.

    When both IMG_001.cr3 and IMG_001.jpg exist in the same folder,
    keep the raw as the primary photo and set companion_path to the JPEG filename.
    Delete the duplicate JPEG-only photo record.
    """
    raw_exts = {".nef", ".cr2", ".cr3", ".arw", ".raf", ".dng", ".rw2", ".orf"}
    jpeg_exts = {".jpg", ".jpeg"}

    rows = db.conn.execute(
        "SELECT id, folder_id, filename, extension FROM photos"
        " WHERE companion_path IS NULL"
        " OR (companion_path IS NOT NULL AND extension IN"
        " ('.nef','.cr2','.cr3','.arw','.raf','.dng','.rw2','.orf'))"
        " ORDER BY folder_id, filename"
    ).fetchall()

    # Group by folder_id + base name (without extension)
    groups = defaultdict(list)
    for row in rows:
        base = os.path.splitext(row["filename"])[0]
        groups[(row["folder_id"], base)].append(dict(row))

    for (_folder_id, _base), members in groups.items():
        if len(members) < 2:
            continue

        raws = [m for m in members if m["extension"] in raw_exts]
        jpegs = [m for m in members if m["extension"] in jpeg_exts]

        if not raws or not jpegs:
            continue

        # Use first raw as primary, first JPEG as companion
        primary = raws[0]
        companion = jpegs[0]

        # Transfer metadata from companion to primary if primary lacks it
        transfer_cols = "timestamp, rating, flag, latitude, longitude, exif_data, focal_length, width, height"
        primary_full = db.conn.execute(
            f"SELECT {transfer_cols} FROM photos WHERE id = ?",
            (primary["id"],),
        ).fetchone()
        companion_full = db.conn.execute(
            f"SELECT {transfer_cols} FROM photos WHERE id = ?",
            (companion["id"],),
        ).fetchone()

        updates = []
        params = []
        if not primary_full["timestamp"] and companion_full["timestamp"]:
            updates.append("timestamp = ?")
            params.append(companion_full["timestamp"])
        if primary_full["rating"] == 0 and companion_full["rating"] != 0:
            updates.append("rating = ?")
            params.append(companion_full["rating"])
        if primary_full["flag"] == "none" and companion_full["flag"] != "none":
            updates.append("flag = ?")
            params.append(companion_full["flag"])
        if primary_full["latitude"] is None and companion_full["latitude"] is not None:
            updates.extend(["latitude = ?", "longitude = ?"])
            params.extend([companion_full["latitude"], companion_full["longitude"]])
        if not primary_full["exif_data"] and companion_full["exif_data"]:
            updates.append("exif_data = ?")
            params.append(companion_full["exif_data"])
        if primary_full["focal_length"] is None and companion_full["focal_length"] is not None:
            updates.append("focal_length = ?")
            params.append(companion_full["focal_length"])
        if not primary_full["width"] and companion_full["width"]:
            updates.extend(["width = ?", "height = ?"])
            params.extend([companion_full["width"], companion_full["height"]])
        if updates:
            params.append(primary["id"])
            db.conn.execute(
                f"UPDATE photos SET {', '.join(updates)} WHERE id = ?", params
            )

        # Transfer keywords from companion to primary
        companion_keywords = db.conn.execute(
            "SELECT keyword_id FROM photo_keywords WHERE photo_id = ?",
            (companion["id"],),
        ).fetchall()
        for kw in companion_keywords:
            db.conn.execute(
                "INSERT OR IGNORE INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
                (primary["id"], kw["keyword_id"]),
            )

        db.conn.execute(
            "UPDATE photos SET companion_path = ? WHERE id = ?",
            (companion["filename"], primary["id"]),
        )

        # Transfer detections (and their cascaded predictions) from companion to primary.
        # Detections are linked to photos; predictions cascade through detection_id.
        db.conn.execute(
            "UPDATE detections SET photo_id = ? WHERE photo_id = ?",
            (primary["id"], companion["id"]),
        )

        # Transfer pending_changes from companion to primary. No dedup needed
        # here (unlike the inat_submissions block below): pending_changes has
        # no UNIQUE constraint that would crash on collision, and duplicate
        # rows from a raw+JPEG pairing are harmless and vanishingly unlikely.
        db.conn.execute(
            "UPDATE pending_changes SET photo_id = ? WHERE photo_id = ?",
            (primary["id"], companion["id"]),
        )
        # Transfer iNaturalist submissions: deduplicate on (photo_id, observation_id)
        # before reassigning to avoid UNIQUE constraint violation.
        db.conn.execute(
            """DELETE FROM inat_submissions
               WHERE photo_id = ? AND observation_id IN (
                   SELECT observation_id FROM inat_submissions WHERE photo_id = ?
               )""",
            (companion["id"], primary["id"]),
        )
        db.conn.execute(
            "UPDATE inat_submissions SET photo_id = ? WHERE photo_id = ?",
            (primary["id"], companion["id"]),
        )
        # Remove keyword associations then the duplicate JPEG record
        db.conn.execute("DELETE FROM photo_keywords WHERE photo_id = ?", (companion["id"],))
        db.conn.execute("DELETE FROM photos WHERE id = ?", (companion["id"],))

    commit_with_retry(db.conn)


def _invalidate_derived_caches(db, vireo_dir, photo_id, thumb_cache_dir=None):
    """Delete cached thumbnail / working copy / tracked preview for a photo.

    Called when the scanner detects that an existing photo's source content
    has changed (different file_hash). Thumbnails, working copies, and
    preview-pyramid sizes are all derived from the source bytes, so they're
    stale as soon as the source changes.

    Scope is intentionally O(1) per photo — untracked preview files
    (no preview_cache row) are handled by
    ``_sweep_untracked_previews_for_photos`` once at the end of
    ``scan()`` instead, so large rescans don't re-enumerate previews/
    for every invalidated photo (O(N × M) work).

    Also clears ``working_copy_path`` in the database so the scanner's
    working-copy extraction pass at the end of ``scan()`` picks this row
    back up and rebuilds the working copy.

    Requires an explicit ``vireo_dir``: DB path and cache root are
    independently configurable (--db vs --thumb-dir), so we can't guess
    the cache location from the DB. No-op when the caller omits it.

    ``thumb_cache_dir`` overrides the thumbnail location. ``--thumb-dir``
    may point to any directory name — it is not constrained to
    ``vireo_dir/thumbnails``. Callers that have the configured value
    (Flask routes, audit entry points) should pass it here or stale
    thumbs survive; ``previews/`` and ``working/`` are always siblings
    of ``vireo_dir`` by convention and need no override.
    """
    if not vireo_dir:
        return

    thumb_dir = thumb_cache_dir or os.path.join(vireo_dir, "thumbnails")
    thumb_path = os.path.join(thumb_dir, f"{photo_id}.jpg")
    if os.path.exists(thumb_path):
        try:
            os.remove(thumb_path)
        except OSError:
            log.debug("Could not delete stale thumbnail %s", thumb_path, exc_info=True)

    wc_file = os.path.join(vireo_dir, "working", f"{photo_id}.jpg")
    if os.path.exists(wc_file):
        try:
            os.remove(wc_file)
        except OSError:
            log.debug("Could not delete stale working copy %s", wc_file, exc_info=True)
    db.conn.execute(
        "UPDATE photos SET working_copy_path = NULL WHERE id = ?",
        (photo_id,),
    )

    # Preview pyramid + its LRU accounting. Only drop a preview_cache row
    # for sizes whose file was successfully removed (or was already
    # missing): if unlink fails (e.g. Windows file lock) and we drop the
    # row anyway, the serve path's lazy-adoption shortcut re-adopts the
    # stranded file and hands out stale pre-change bytes. Mirrors the
    # self-healing semantics in preview_cache.evict_if_over_quota.
    preview_dir = os.path.join(vireo_dir, "previews")
    rows = db.conn.execute(
        "SELECT size FROM preview_cache WHERE photo_id = ?", (photo_id,)
    ).fetchall()
    deleted_sizes = []
    for row in rows:
        size = row["size"]
        path = os.path.join(preview_dir, f"{photo_id}_{size}.jpg")
        try:
            os.remove(path)
        except FileNotFoundError:
            deleted_sizes.append(size)
        except OSError:
            log.debug("Could not delete stale preview %s", path, exc_info=True)
        else:
            deleted_sizes.append(size)

    if deleted_sizes:
        db.conn.executemany(
            "DELETE FROM preview_cache WHERE photo_id = ? AND size = ?",
            [(photo_id, s) for s in deleted_sizes],
        )


def _sweep_untracked_previews_for_photos(db, vireo_dir, photo_ids):
    """Batched sweep of preview files with no preview_cache row.

    Legacy / orphan preview files (written by older code paths or left
    over from interrupted inserts) would be lazy-adopted on the next
    ``/photos/<id>/preview`` request and served as valid cache hits.
    After a content change that's stale data — the app serves
    pre-change bytes. We sweep them.

    Runs once per ``scan()`` call, enumerating ``previews/`` at most
    one time regardless of how many photos were invalidated. Files
    whose ``(photo_id, size)`` still has a live preview_cache row are
    preserved (row-driven cleanup in ``_invalidate_derived_caches``
    keeps rows when unlink fails, and we must not orphan those files
    here either).
    """
    if not vireo_dir or not photo_ids:
        return
    preview_dir = os.path.join(vireo_dir, "previews")
    if not os.path.isdir(preview_dir):
        return

    photo_ids_set = {int(p) for p in photo_ids}
    ids_list = list(photo_ids_set)
    # Chunk to stay under SQLITE_MAX_VARIABLE_NUMBER (default 999 on
    # older builds). Without this, a rescan that invalidates thousands
    # of photos crashes scan post-processing with "too many SQL variables".
    _CHUNK = 900
    still_tracked: set[tuple[int, int]] = set()
    for i in range(0, len(ids_list), _CHUNK):
        chunk = ids_list[i : i + _CHUNK]
        ph = ",".join("?" * len(chunk))
        rows = db.conn.execute(
            f"SELECT photo_id, size FROM preview_cache WHERE photo_id IN ({ph})",
            chunk,
        ).fetchall()
        still_tracked.update((r["photo_id"], r["size"]) for r in rows)

    try:
        entries = os.listdir(preview_dir)
    except OSError:
        return
    for fname in entries:
        if not fname.endswith(".jpg"):
            continue
        stem = fname[: -len(".jpg")]
        parts = stem.rsplit("_", 1)
        if len(parts) != 2:
            continue
        try:
            pid = int(parts[0])
            size = int(parts[1])
        except ValueError:
            continue
        if pid not in photo_ids_set:
            continue
        if (pid, size) in still_tracked:
            continue
        path = os.path.join(preview_dir, fname)
        try:
            os.remove(path)
        except OSError:
            log.debug("Could not delete untracked preview %s", path, exc_info=True)


def _subtree_like_pattern(path, sep=None):
    """Build a SQLite LIKE parameter that matches ``path`` + any descendant.

    Intended for use with ``LIKE ? ESCAPE '\\'``. Escapes ``\\``, ``%``, and
    ``_`` inside the path and the separator, so literal wildcards in folder
    names don't leak into sibling matches and — critically on Windows — the
    trailing backslash separator doesn't turn the appended ``%`` into a
    literal character.

    Trailing separators on the input are collapsed to exactly one before the
    wildcard. Without this, ``"/photos/"`` and the filesystem root ``"/"``
    produce ``"//%"``, which matches nothing.
    """
    if sep is None:
        sep = os.sep

    while path.endswith(sep):
        path = path[: -len(sep)]

    def _escape(s):
        return s.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")

    return _escape(path) + _escape(sep) + "%"


def _extract_working_copies(db, vireo_dir, progress_callback=None, status_callback=None, scope=None):
    """Extract working copies for all RAW photos missing one.

    For each RAW photo without a working_copy_path, extract a JPEG working
    copy into ``<vireo_dir>/working/<photo_id>.jpg``.  When the photo has a
    companion JPEG (RAW+JPEG pair), the companion is used as the extraction
    source because the in-camera JPEG is higher quality than extracting from
    the RAW.

    ``scope`` restricts which folders are considered:
      * ``None`` (default) — library-wide backfill (every missing WC).
      * list/tuple of entries — only folders matching an entry are eligible.
        Each entry is either:
          - a path string → matches the folder and every descendant (subtree);
          - a ``(path, "exact")`` tuple → matches the folder only;
          - a ``(path, "subtree")`` tuple → explicit form of the string case.
      * empty list/tuple — no-op (used by callers that want an explicit
        "scan matched nothing" signal instead of backfilling everything).
    """
    import config as cfg

    if scope is not None and len(scope) == 0:
        return

    user_cfg = cfg.load()
    wc_max_size = user_cfg.get("working_copy_max_size", 4096)
    wc_quality = user_cfg.get("working_copy_quality", 92)

    # Select photos that need a working copy:
    #   - All RAW files without one.
    #   - Large JPEGs (width or height exceeds working_copy_max_size)
    #     without one — these get a downsized working copy so every
    #     derivative (thumbnail, preview) reads from the same canonical
    #     image. Skipped when wc_max_size <= 0 (the "full resolution"
    #     sentinel), where there is no cap to enforce.
    placeholders = ",".join("?" for _ in RAW_EXTENSIONS)
    params = list(RAW_EXTENSIONS)
    jpeg_clause = ""
    if wc_max_size and wc_max_size > 0:
        jpeg_clause = (
            " OR (LOWER(p.extension) IN ('.jpg', '.jpeg', 'jpg', 'jpeg')"
            "     AND (p.width > ? OR p.height > ?))"
        )
        params.extend([wc_max_size, wc_max_size])

    scope_clause = ""
    if scope is not None:
        scope_terms = []
        for entry in scope:
            if isinstance(entry, tuple):
                path, mode = entry
            else:
                path, mode = entry, "subtree"
            path = str(path)
            if mode == "exact":
                scope_terms.append("f.path = ?")
                params.append(path)
            else:
                # Subtree match. The LIKE pattern needs to escape `_`, `%`,
                # and the escape char itself — both in the path and in the
                # separator — so (a) literal wildcards in folder names don't
                # leak into siblings (`2024_06` matching `2024A06`) and
                # (b) the Windows `\` separator doesn't turn the trailing
                # `%` into a literal under ESCAPE '\\'.
                scope_terms.append("(f.path = ? OR f.path LIKE ? ESCAPE '\\')")
                params.extend([path, _subtree_like_pattern(path)])
        scope_clause = " AND (" + " OR ".join(scope_terms) + ")"

    rows = db.conn.execute(
        f"""
        SELECT p.id, p.filename, p.companion_path, p.working_copy_path,
               p.extension, p.width, p.height,
               f.path AS folder_path
          FROM photos p
          JOIN folders f ON f.id = p.folder_id
         WHERE p.working_copy_path IS NULL
           AND (
               p.extension IN ({placeholders})
               {jpeg_clause}
           )
           {scope_clause}
        """,
        params,
    ).fetchall()

    if not rows:
        return

    if status_callback:
        status_callback(f"Extracting {len(rows)} working copies...")

    for _i, row in enumerate(rows):
        wc_rel = f"working/{row['id']}.jpg"
        wc_abs = os.path.join(vireo_dir, wc_rel)

        # Prefer companion JPEG if available
        source = os.path.join(row["folder_path"], row["filename"])
        if row["companion_path"]:
            companion = os.path.join(row["folder_path"], row["companion_path"])
            if os.path.isfile(companion):
                source = companion

        if extract_working_copy(source, wc_abs, max_size=wc_max_size, quality=wc_quality):
            db.conn.execute(
                "UPDATE photos SET working_copy_path=? WHERE id=?",
                (wc_rel, row["id"]),
            )

    commit_with_retry(db.conn)


def scan(root, db, progress_callback=None, incremental=False, extract_full_metadata=True, photo_callback=None, skip_paths=None, status_callback=None, recursive=True, restrict_dirs=None, restrict_files=None, vireo_dir=None, thumb_cache_dir=None):
    """Walk a folder tree, discover photos, read metadata, populate database.

    Args:
        root: path to the root folder to scan
        db: Database instance
        progress_callback: optional callable(current, total) for progress reporting
        incremental: if True, skip files unchanged since last scan
        extract_full_metadata: if True, store full ExifTool JSON in exif_data column
        photo_callback: optional callable(photo_id, path_str) called after each photo is committed
        skip_paths: optional set of absolute path strings to exclude from scanning
        status_callback: optional callable(message) for phase status updates
        recursive: if True (default), scan subfolders; if False, only scan root directory
        restrict_dirs: optional list of directory paths to scan instead of the
            full tree. When provided, only files in these directories are
            discovered (non-recursively), but ``root`` is still used as the
            folder hierarchy root so parent links are preserved correctly.
        restrict_files: optional iterable of absolute file paths. When
            provided alongside ``restrict_dirs``, only files whose path
            is in this set are discovered — untracked files in the same
            directory are ignored. Used by the pipeline's repair path to
            touch only photos already in the DB.
        vireo_dir: optional path to the vireo data directory (e.g. ``~/.vireo``).
            When provided, working copies are extracted for RAW photos after
            companion pairing, and derived-cache invalidation fires on
            content-changed photos.
        thumb_cache_dir: optional override for the thumbnail cache
            directory. ``--thumb-dir`` is independently configurable and
            can point anywhere — defaulting to ``vireo_dir/thumbnails``
            silently misses the real cache when those diverge. Callers
            with the configured value (Flask routes, audit entry points)
            should pass it. When omitted, falls back to
            ``vireo_dir/thumbnails``.
    """
    root_path = Path(root)
    if not root_path.is_dir():
        log.warning("Root path does not exist or is not a directory: %s", root)
        return

    # Discover all image files (incremental enumeration for progress reporting)
    log.info("Discovering files in %s ...", root)
    if status_callback:
        status_callback("Discovering files...")
    image_files = []
    if restrict_dirs is not None:
        # Only enumerate files in the specified directories (non-recursive).
        # root is still used as the folder hierarchy root for _ensure_folder.
        restrict_files_set = set(restrict_files) if restrict_files is not None else None
        for d in restrict_dirs:
            dp = Path(d)
            if dp.is_dir():
                for f in dp.iterdir():
                    if (f.is_file()
                            and f.suffix.lower() in SUPPORTED_EXTENSIONS
                            and not f.name.startswith(".")
                            and (skip_paths is None or str(f) not in skip_paths)
                            and (restrict_files_set is None
                                 or str(f) in restrict_files_set)):
                        image_files.append(f)
    else:
        candidates = root_path.rglob("*") if recursive else root_path.iterdir()
        for checked, f in enumerate(candidates, 1):
            if checked % 500 == 0 and status_callback:
                status_callback(f"Discovering files... ({len(image_files)} found)")
            if (f.is_file()
                    and f.suffix.lower() in SUPPORTED_EXTENSIONS
                    and not f.name.startswith(".")
                    and (skip_paths is None or str(f) not in skip_paths)):
                image_files.append(f)
    image_files.sort()

    total = len(image_files)
    log.info("Found %d images in %s", total, root)
    if progress_callback:
        progress_callback(0, total)

    # Build existing photo lookup for incremental mode
    existing_photos = {}
    exif_extracted = set()  # photo IDs where ExifTool has already run
    if incremental:
        all_photos = db.get_photos(per_page=999999)
        for p in all_photos:
            # Key by folder_id + filename won't work easily, so use a second lookup
            existing_photos[p["id"]] = p
        # Build a path-based lookup: we need folder path + filename
        existing_by_path = {}
        folders = {f["id"]: f["path"] for f in db.get_folder_tree()}
        for p in all_photos:
            folder_path = folders.get(p["folder_id"], "")
            full_path = os.path.join(folder_path, p["filename"])
            existing_by_path[full_path] = p
        # Track which photos have had ExifTool metadata extracted (exif_data
        # is non-NULL). Photos with NULL exif_data need re-extraction.
        for row in db.conn.execute("SELECT id FROM photos WHERE exif_data IS NOT NULL"):
            exif_extracted.add(row["id"])

    # Build folder cache: path -> folder_id
    folder_cache = {}

    def _ensure_folder(folder_path):
        """Ensure a folder and all its parents exist in the DB. Returns folder_id."""
        folder_str = str(folder_path)
        if folder_str in folder_cache:
            return folder_cache[folder_str]

        parent_id = None
        if folder_path != root_path:
            parent_id = _ensure_folder(folder_path.parent)

        folder_id = db.add_folder(
            path=folder_str,
            name=folder_path.name,
            parent_id=parent_id,
        )
        folder_cache[folder_str] = folder_id
        return folder_id

    # Track folders whose scan touched them (so we can flag them 'partial'
    # if anything between the pre-pass and scan completion dies midway) and
    # the outer scan scope as a fallback. The scope matters when
    # ``touched_folder_ids`` is empty — e.g. a pre-pass XMP commit that
    # aborts before the main loop has added any folder, or a successful
    # no-op incremental scan that processes zero files.
    touched_folder_ids = set()
    # Photo IDs whose derived caches were invalidated this scan. Collected
    # so the untracked-preview sweep can run once as a batch instead of
    # per-photo (avoids O(N × M) directory walks on large rescans).
    invalidated_photo_ids: set[int] = set()
    scoped_paths = {str(root_path)}
    if restrict_dirs is not None:
        scoped_paths.update(str(d) for d in restrict_dirs)

    def _update_folder_status(new_status, only_from_partial):
        """Stamp folders in the scan scope with ``new_status``.

        Applies to every folder matched by ``scoped_paths`` (outer roots)
        OR ``touched_folder_ids`` (folder rows the main loop has reached).
        When ``only_from_partial`` is True, restricts the UPDATE to rows
        already in ``'partial'`` — used on the success path so completed
        scans don't clobber ``'missing'`` or future statuses.
        """
        guard = " AND status = 'partial'" if only_from_partial else ""
        if scoped_paths:
            path_placeholders = ",".join("?" * len(scoped_paths))
            db.conn.execute(
                f"UPDATE folders SET status = ? "
                f"WHERE path IN ({path_placeholders}){guard}",
                (new_status, *scoped_paths),
            )
        if touched_folder_ids:
            id_placeholders = ",".join("?" * len(touched_folder_ids))
            db.conn.execute(
                f"UPDATE folders SET status = ? "
                f"WHERE id IN ({id_placeholders}){guard}",
                (new_status, *touched_folder_ids),
            )
        commit_with_retry(db.conn)

    # First pass: determine which files need full processing (for incremental mode).
    # Handle XMP-only changes inline; collect files needing metadata extraction.
    files_to_process = []
    processed_count = 0
    try:
        for image_path in image_files:
            stat = image_path.stat()
            file_mtime = stat.st_mtime
            xmp_path = image_path.with_suffix(".xmp")
            xmp_mtime = xmp_path.stat().st_mtime if xmp_path.exists() else None

            if incremental:
                full_path_str = str(image_path)
                existing = existing_by_path.get(full_path_str)
                if existing:
                    file_unchanged = existing["file_mtime"] == file_mtime
                    xmp_unchanged = existing["xmp_mtime"] == xmp_mtime
                    # Re-process if ExifTool never ran for this photo (both
                    # timestamp and exif_data are NULL). Photos with genuinely
                    # missing timestamps (screenshots, exports) will have
                    # exif_data set after one extraction attempt.
                    # Also flag rows where a RAW file has absurdly small
                    # dimensions (<1000px) — that's the embedded JPEG thumb
                    # leaking through when ExifTool's File group was missing
                    # on the original scan.
                    dims_suspect = (
                        existing["extension"] in RAW_EXTENSIONS
                        and existing["width"] is not None
                        and existing["width"] < 1000
                    )
                    metadata_missing = (
                        (existing["timestamp"] is None or dims_suspect)
                        and existing["id"] not in exif_extracted
                    )

                    if file_unchanged and xmp_unchanged and not metadata_missing:
                        processed_count += 1
                        if photo_callback:
                            photo_callback(existing["id"], full_path_str)
                        if progress_callback:
                            progress_callback(processed_count, total)
                        continue

                    # XMP changed: re-import keywords
                    if not xmp_unchanged and xmp_mtime is not None:
                        _import_keywords_for_photo(db, existing["id"], str(xmp_path))
                        db.conn.execute(
                            "UPDATE photos SET xmp_mtime = ? WHERE id = ?",
                            (xmp_mtime, existing["id"]),
                        )
                        commit_with_retry(db.conn)

                    if file_unchanged and not metadata_missing:
                        processed_count += 1
                        if photo_callback:
                            photo_callback(existing["id"], full_path_str)
                        if progress_callback:
                            progress_callback(processed_count, total)
                        continue

            files_to_process.append(image_path)
    except BaseException:
        # Pre-pass died (e.g. non-retryable DB error on an XMP commit).
        # Route through the same partial-status path as a main-loop failure
        # so users see the badge and can rescan.
        try:
            db.conn.rollback()
        except Exception:
            log.exception("Rollback after pre-pass failure also failed")
        try:
            _update_folder_status("partial", only_from_partial=False)
        except Exception:
            log.exception("Failed to flag folders partial after pre-pass failure")
        raise

    # Batch extract metadata via ExifTool only for files that need processing
    paths_to_extract = [str(ip) for ip in files_to_process]
    if paths_to_extract and status_callback:
        status_callback(f"Extracting metadata ({len(paths_to_extract)} files)...")
    metadata_map = extract_metadata(paths_to_extract) if paths_to_extract else {}

    # Compute phash + file_hash in parallel across all files that need
    # processing. These are the two per-file operations that actually read
    # every byte of the image; everything else in the loop is cheap DB or
    # dict work. Results stream in order, so workers keep computing the tail
    # while the main thread commits the head — no O(n) buffer of features.
    workers = _resolve_worker_count(files_to_process)
    if paths_to_extract and status_callback:
        status_callback(
            f"Hashing {len(paths_to_extract)} files ({workers} worker{'s' if workers != 1 else ''})..."
        )

    def _iter_features():
        if workers > 1:
            mp_ctx = multiprocessing.get_context(_SCAN_MP_METHOD)
            with ProcessPoolExecutor(max_workers=workers, mp_context=mp_ctx) as pool:
                # Bounded in-flight window instead of pool.map(): on Python
                # 3.11 Executor.map eagerly submits every input, so on a
                # 200k-file scan we would hold 200k queued futures in RAM.
                # A few submissions per worker is enough to keep them fed
                # while the main thread drains results in order.
                max_in_flight = workers * 4
                pending = deque()
                inputs = zip(files_to_process, paths_to_extract, strict=True)
                for image_path, path_str in inputs:
                    pending.append((image_path, pool.submit(_compute_file_features, path_str)))
                    if len(pending) >= max_in_flight:
                        done_path, done_fut = pending.popleft()
                        yield done_path, done_fut.result()
                while pending:
                    done_path, done_fut = pending.popleft()
                    yield done_path, done_fut.result()
        else:
            for image_path, path_str in zip(files_to_process, paths_to_extract, strict=True):
                yield image_path, _compute_file_features(path_str)

    try:
        for image_path, (phash, file_hash) in _iter_features():
            folder_id = _ensure_folder(image_path.parent)
            touched_folder_ids.add(folder_id)

            # File stats
            stat = image_path.stat()
            file_size = stat.st_size
            file_mtime = stat.st_mtime

            # XMP sidecar
            xmp_path = image_path.with_suffix(".xmp")
            xmp_mtime = xmp_path.stat().st_mtime if xmp_path.exists() else None

            # Get pre-extracted metadata for this file
            file_meta = metadata_map.get(str(image_path), {})
            file_group = file_meta.get("File", {})
            exif_group = file_meta.get("EXIF", {})
            composite = file_meta.get("Composite", {})

            # Dimensions from ExifTool (works for all file types including RAW)
            width, height = _extract_dimensions(exif_group, file_group, extension=image_path.suffix.lower())

            # Fallback if ExifTool didn't provide dimensions
            if width is None or height is None:
                ext = image_path.suffix.lower()
                if ext in RAW_EXTENSIONS:
                    try:
                        import rawpy

                        with rawpy.imread(str(image_path)) as raw:
                            width = raw.sizes.width
                            height = raw.sizes.height
                    except Exception:
                        log.debug("Could not read RAW dimensions from %s", image_path)
                else:
                    try:
                        with Image.open(str(image_path)) as img:
                            width, height = img.size
                    except Exception:
                        log.debug("Could not read dimensions from %s", image_path)

            # Timestamp from ExifTool
            timestamp = _extract_timestamp(exif_group)

            # Focal length
            focal_length = exif_group.get("FocalLength")
            if focal_length is not None:
                focal_length = float(focal_length)

            # Burst ID (ImageUniqueID)
            burst_id = exif_group.get("ImageUniqueID")
            if burst_id:
                burst_id = str(burst_id)

            # GPS coordinates — ExifTool with -n gives decimal degrees directly
            latitude = composite.get("GPSLatitude")
            if latitude is None:
                latitude = exif_group.get("GPSLatitude")
            longitude = composite.get("GPSLongitude")
            if longitude is None:
                longitude = exif_group.get("GPSLongitude")

            # Pre-check: capture prior content identity AND whether the
            # row existed before add_photo touches it. Brand-new rows
            # have no derived caches to flush — skipping invalidation
            # for them avoids O(N) wasted UPDATE + commit round-trips on
            # large initial scans.
            existing_row = db.conn.execute(
                "SELECT file_hash FROM photos WHERE folder_id = ? AND filename = ?",
                (folder_id, image_path.name),
            ).fetchone()
            row_already_existed = existing_row is not None
            prev_file_hash = existing_row["file_hash"] if existing_row else None

            photo_id = db.add_photo(
                folder_id=folder_id,
                filename=image_path.name,
                extension=image_path.suffix.lower(),
                file_size=file_size,
                file_mtime=file_mtime,
                xmp_mtime=xmp_mtime,
                timestamp=timestamp,
                width=width,
                height=height,
            )

            # Update metadata columns (also fixes existing photos that were
            # inserted before ExifTool metadata was available)
            updates = []
            update_params = []
            if timestamp is not None:
                updates.append("timestamp=?")
                update_params.append(timestamp)
            if width is not None:
                updates.append("width=?")
                update_params.append(width)
            if height is not None:
                updates.append("height=?")
                update_params.append(height)
            if latitude is not None:
                updates.extend(["latitude=?", "longitude=?"])
                update_params.extend([latitude, longitude])
            if phash is not None:
                updates.append("phash=?")
                update_params.append(phash)
            if focal_length is not None:
                updates.append("focal_length=?")
                update_params.append(focal_length)
            if burst_id is not None:
                updates.append("burst_id=?")
                update_params.append(burst_id)
            if file_hash is not None:
                updates.append("file_hash=?")
                update_params.append(file_hash)
            if file_meta and extract_full_metadata:
                updates.append("exif_data=?")
                update_params.append(json.dumps(file_meta))
            elif file_meta:
                # Store minimal marker so we know ExifTool ran (even when
                # extract_full_metadata is off) — prevents perpetual retry
                updates.append("exif_data=COALESCE(exif_data, ?)")
                update_params.append("{}")
            if updates:
                update_params.append(photo_id)
                db.conn.execute(
                    f"UPDATE photos SET {', '.join(updates)} WHERE id=?",
                    update_params,
                )
                commit_with_retry(db.conn)

            # Content-change self-heal: when the computed hash differs
            # from what was stored before this scan, derived caches are
            # stale. Includes the NULL → concrete transition for legacy
            # rows that predate hash tracking — we can't prove their
            # caches match current bytes, so safer to flush and
            # regenerate. Gated on ``row_already_existed`` so brand-new
            # inserts (prev_file_hash is always NULL there) don't
            # trigger pointless UPDATE + commit round-trips on large
            # initial scans. Requires explicit vireo_dir; callers must
            # pass it (scan can't guess because --db and --thumb-dir
            # are independently configurable).
            if (row_already_existed
                    and file_hash is not None
                    and prev_file_hash != file_hash
                    and vireo_dir):
                _invalidate_derived_caches(
                    db, vireo_dir, photo_id, thumb_cache_dir=thumb_cache_dir,
                )
                invalidated_photo_ids.add(photo_id)
                commit_with_retry(db.conn)

            # Import XMP keywords if sidecar exists — must land BEFORE the
            # duplicate auto-resolve hook below, so if this row turns out to be
            # the loser its keywords are visible to apply_duplicate_resolution's
            # metadata query and get merged onto the winner. Otherwise the
            # keywords would be stranded on the rejected row.
            if xmp_path.exists():
                _import_keywords_for_photo(db, photo_id, str(xmp_path))

            # Trigger duplicate auto-resolve now that file_hash AND XMP keywords
            # are committed. add_photo was called without the hash, so the hook
            # there was a no-op — we own firing it here.
            if file_hash is not None:
                db.check_and_resolve_duplicates_for_hash(file_hash)

            if photo_callback:
                photo_callback(photo_id, str(image_path))

            processed_count += 1
            if progress_callback:
                progress_callback(processed_count, total)
    except BaseException:
        # Per-file loop died mid-way (DB error, signal, etc). Roll back any
        # half-applied write so the partial-status UPDATE below runs on a
        # clean transaction, then flag every folder in scope as 'partial' so
        # callers can detect and re-scan.
        try:
            db.conn.rollback()
        except Exception:
            log.exception("Rollback after scan failure also failed")
        try:
            _update_folder_status("partial", only_from_partial=False)
        except Exception:
            log.exception("Failed to flag folders partial after scan failure")
        raise
    else:
        # Per-file loop completed cleanly. Clear any stale 'partial' flag on
        # scanned folders so a successful rescan restores full visibility.
        # Uses both the scan scope (root + restrict_dirs) AND the touched
        # folder ids: a successful no-op incremental scan has an empty
        # ``touched_folder_ids`` set but must still clear the badge for the
        # roots the user asked us to scan; a recursive scan that failed and
        # then succeeds needs the touched ids to reach touched subfolders.
        try:
            _update_folder_status("ok", only_from_partial=True)
        except Exception:
            log.exception("Failed to clear partial flag after scan success")

    # Pair raw+JPEG companions: raw is primary, JPEG becomes companion_path.
    # Wrap post-processing so folder counts are always updated, even on failure.
    # On exception, roll back any uncommitted partial writes before updating
    # counts — otherwise update_folder_counts()'s commit would persist
    # half-applied pairing or working-copy records.
    try:
        _pair_raw_jpeg_companions(db)

        # Extract working copies for RAW photos (after pairing so companion is known).
        # Scope to the folders the caller just scanned so a fresh import doesn't
        # trigger library-wide backfill for every pre-existing large JPEG.
        # Match-mode mirrors what scan() actually traversed: restrict_dirs and
        # non-recursive scans only touch direct children, so the scope uses an
        # exact-folder match; a recursive walk from `root` matches the subtree.
        if vireo_dir:
            if restrict_dirs is not None:
                wc_scope = [(str(d), "exact") for d in restrict_dirs]
            elif not recursive:
                wc_scope = [(str(root_path), "exact")]
            else:
                wc_scope = [str(root_path)]
            _extract_working_copies(
                db, vireo_dir, progress_callback, status_callback, scope=wc_scope,
            )

        # Batched untracked-preview sweep. One os.listdir(previews/) for
        # the whole scan instead of one per invalidated photo — essential
        # when a rescan touches thousands of content-changed files.
        if invalidated_photo_ids:
            _sweep_untracked_previews_for_photos(
                db, vireo_dir, invalidated_photo_ids,
            )
    except BaseException:
        db.conn.rollback()
        raise
    finally:
        db.update_folder_counts()
    log.info("Scan complete: %d photos indexed", total)
