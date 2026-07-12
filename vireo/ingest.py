"""Smart ingest: copy and organize photos from external source to destination."""

import contextlib
import errno
import logging
import os
import posixpath
import shutil
import sys
from datetime import datetime
from pathlib import Path

from image_loader import (
    IMAGE_EXTENSIONS,
    RAW_EXTENSIONS,
    SUPPORTED_EXTENSIONS,
    is_excluded_scan_path,
    safe_iter_dir,
    safe_scan_walk,
)
from import_dedup import (
    CatalogIndex,
    DuplicateChecker,
    source_capture_timestamps,
    stored_metadata_key,
)
from scanner import compute_file_hash

log = logging.getLogger(__name__)

_WINDOWS = sys.platform == "win32"


def _escape_sql_like(s):
    """Escape SQL LIKE wildcard metacharacters in a literal string.

    SQLite's LIKE treats ``%`` and ``_`` as wildcards unconditionally. The
    ESCAPE clause lets us declare an escape character so a literal ``%``
    or ``_`` in the pattern is matched literally. We pair this helper with
    ``... LIKE ? ESCAPE '\\'`` at the call site.
    """
    return s.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _slash_normpath(s):
    # On Windows, both ``\`` and ``/`` are valid separators and stored paths
    # may use either; replace backslashes first, then collapse ``..`` segments
    # with posixpath.normpath so the prefix check in _path_under_root works
    # regardless of which separator the caller used.
    #
    # On POSIX, ``\`` is a valid filename character rather than a separator.
    # Converting it would conflate a literal sibling like ``/photos\archive``
    # (one folder whose name happens to contain a backslash) with a child of
    # ``/photos``, letting it slip through the subtree containment check.
    # Run posixpath.normpath directly so backslashes stay literal.
    raw = str(s)
    if _WINDOWS and len(raw) == 2 and raw[1] == ":" and raw[0].isalpha():
        # ``C:`` (drive letter and colon, no separator) is drive-relative:
        # it means the current directory on drive C, NOT the root of C
        # drive. Resolve via os.path.abspath so the prefix check
        # distinguishes ``C:`` from ``C:\`` — without this, both collapse
        # to ``c:`` and every ``C:\...`` row is wrongly accepted as inside
        # a destination given as ``C:``.
        raw = os.path.abspath(raw)
    normalized = posixpath.normpath(raw.replace("\\", "/") if _WINDOWS else raw)
    return "" if normalized == "." else normalized.rstrip("/")


def _case_fold_path(s):
    """Case-fold a path for comparison when the host filesystem is
    case-insensitive (Windows NTFS/FAT). On POSIX hosts this is a no-op,
    preserving case-sensitive matching.

    The previous ``Path(...).is_relative_to(...)`` containment check was
    case-insensitive on ``WindowsPath``; the slash-string comparison in
    ``_path_under_root`` is not, so without this fold a destination passed
    as ``c:\\photos`` would fail to match folder rows scanned as
    ``C:\\Photos\\...`` and ``duplicate_folders`` would come back empty.
    """
    return s.lower() if _WINDOWS else s


def _path_under_root(candidate, root):
    candidate_norm = _case_fold_path(_slash_normpath(candidate))
    root_norm = _case_fold_path(_slash_normpath(root))
    if root_norm in {"", "/"}:
        if _WINDOWS:
            # On Windows, a destination given as ``/`` or ``\`` is
            # drive-relative: it means the current drive root (e.g. ``C:\``),
            # not every drive or UNC share. Resolve to the absolute drive
            # root via ``os.path.abspath`` and recheck containment so a
            # folder row on ``D:\...`` or ``\\server\share\...`` isn't
            # accepted into a destination the user gave as ``/`` and then
            # leaked into ``duplicate_folders`` for the restricted scan.
            drive_root = _case_fold_path(_slash_normpath(os.path.abspath(root)))
            if drive_root and drive_root not in {"", "/"}:
                return (
                    candidate_norm == drive_root
                    or candidate_norm.startswith(drive_root + "/")
                )
        return os.path.isabs(os.path.normpath(candidate))
    return candidate_norm == root_norm or candidate_norm.startswith(root_norm + "/")


def _is_unsafe_path(s):
    """Check if a path string could escape the destination directory."""
    if not s:
        return False
    # Reject backslashes (Windows traversal), absolute paths, '..' segments,
    # and colons (Windows drive-relative paths like C:2026 or C:%Y)
    if s.startswith("/") or "\\" in s or ":" in s:
        return True
    p = Path(s)
    if p.is_absolute():
        return True
    return ".." in p.parts


def _sanitize_template(template):
    """Reject folder templates that could escape the destination directory."""
    if template and _is_unsafe_path(template):
        raise ValueError(f"unsafe folder template: {template!r}")
    return template


def build_destination_path(exif_timestamp, template="%Y/%Y-%m-%d"):
    """Build relative destination folder path from EXIF timestamp.

    Args:
        exif_timestamp: datetime object from EXIF, or None
        template: strftime format string for folder structure

    Returns:
        Relative path string, or "unsorted" if no timestamp
    """
    _sanitize_template(template)
    if exif_timestamp is None:
        return "unsorted"
    result = exif_timestamp.strftime(template)
    # Double-check the rendered result is still safe
    if result and _is_unsafe_path(result):
        raise ValueError(f"folder template produced unsafe path: {result!r}")
    return result


def _source_file_timestamps(files, capture_times=None):
    """Resolve timestamps for date-folder planning.

    EXIF capture time first (lightweight reader, then batched ExifTool —
    see import_dedup.source_capture_timestamps), falling back to file
    modification time. The mtime fallback is fine here because these
    timestamps only pick destination folders; duplicate identity uses the
    EXIF-only map directly. Pass ``capture_times`` (a str(path) -> datetime
    map) to reuse already-resolved EXIF reads instead of re-reading.
    """
    if capture_times is not None:
        timestamps = {f: capture_times.get(str(f)) for f in files}
    else:
        timestamps = source_capture_timestamps(files)

    for source_file, exif_dt in timestamps.items():
        if exif_dt is None:
            with contextlib.suppress(OSError, ValueError, OverflowError):
                timestamps[source_file] = datetime.fromtimestamp(
                    source_file.stat().st_mtime
                )
    return timestamps


def preview_destination(sources, destination, folder_template="%Y/%Y-%m-%d",
                        file_types="both", recursive=True, exclude_paths=None):
    """Dry-run preview of destination folder structure.

    Scans source files, reads EXIF timestamps, and groups them by the
    folder template without copying anything.

    Returns:
        dict with folders list, total_photos, total_folders,
        new_folders, existing_folders
    """
    all_files = []
    for src in sources:
        all_files.extend(discover_source_files(src, file_types, recursive=recursive))
    if exclude_paths:
        skip = set(exclude_paths)
        all_files = [f for f in all_files if str(f) not in skip]

    timestamps = _source_file_timestamps(all_files)
    folder_counts = {}
    file_destinations = []
    for source_file in all_files:
        rel_folder = build_destination_path(timestamps.get(source_file), folder_template)
        if not rel_folder:
            rel_folder = "."
        folder_counts[rel_folder] = folder_counts.get(rel_folder, 0) + 1
        file_destinations.append({
            "path": str(source_file),
            "folder": rel_folder,
            "full_folder": str(
                Path(destination)
                if rel_folder == "."
                else Path(destination) / rel_folder
            ),
        })

    dest_path = Path(destination)
    folders = []
    for path in sorted(folder_counts):
        check_path = dest_path if path == "." else dest_path / path
        folders.append({
            "path": path,
            "full_path": str(check_path),
            "count": folder_counts[path],
            "exists": check_path.is_dir(),
        })

    new_count = sum(1 for f in folders if not f["exists"])
    existing_count = sum(1 for f in folders if f["exists"])

    return {
        "folders": folders,
        "total_photos": len(all_files),
        "total_folders": len(folders),
        "new_folders": new_count,
        "existing_folders": existing_count,
        "files": file_destinations,
    }


def discover_source_files(
    source_dir, file_types="both", recursive=True, onerror=None,
):
    """Discover image files in source directory.

    Args:
        source_dir: path to source directory (e.g., SD card mount)
        file_types: "raw", "jpeg", "both", or a list of extensions
            (e.g. [".jpg", ".nef"])
        recursive: if True (default), scan subfolders; if False, only scan root
        onerror: optional callable ``onerror(OSError)`` invoked when the
            underlying walk cannot enter or list a directory (permission
            denied, TCC block, unreadable removable-media subtree, etc.).
            Silently swallowing these hides sources from the enumeration
            and lets ``safe_to_format`` go green over a card whose files
            were never seen; the import job passes a collector that flips
            the ledger unsafe.

    Returns:
        Sorted list of Path objects for matching files
    """
    source_path = Path(source_dir)
    # prune_scan_dirs filters only children of the walked root; if the
    # selected source is, or sits inside, an other-app data bundle (e.g.
    # user picks ``~/Pictures/Photos Library.photoslibrary`` or a child
    # like ``.../Photos Library.photoslibrary/originals`` as an import
    # source), os.walk would still open it and trip the macOS TCC prompt.
    # This must run BEFORE ``source_path.is_dir()`` — is_dir follows
    # symlinks and stat's the target, so for a directly selected bundle
    # (or a symlink to one) the existence test alone is enough to trip TCC.
    if is_excluded_scan_path(source_path):
        # Root is an excluded data-bundle. Legacy ingest() and the
        # UI-preview callers silently drop these (they picked the wrong
        # thing; no user contract to enumerate it). But when import_job
        # passes an ``onerror`` collector it is holding safe_to_format
        # against every enumeration being provably clean; a source root
        # we refuse to walk is exactly the case where the pill would
        # otherwise go green over a card whose files were never seen.
        # Emit a synthetic OSError so the ledger records it as a
        # discovery failure. See PR #1107 review (P1 line 927).
        if onerror is not None:
            onerror(PermissionError(
                errno.EACCES,
                "source is an excluded data bundle",
                str(source_path),
            ))
        return []
    if not source_path.is_dir():
        # Root does not resolve to a directory: nonexistent, unmounted
        # removable media, permission-denied on the root, or a plain
        # file. Same reasoning as above — silent [] hides "we saw
        # nothing" from safe_to_format. Emit a synthetic OSError for
        # onerror callers so discover'd == 0 is treated as unsafe.
        if onerror is not None:
            onerror(FileNotFoundError(
                errno.ENOENT,
                "source is not an accessible directory",
                str(source_path),
            ))
        return []

    if isinstance(file_types, list):
        allowed = {ext.lower() for ext in file_types}
    elif file_types == "raw":
        allowed = RAW_EXTENSIONS
    elif file_types == "jpeg":
        allowed = IMAGE_EXTENSIONS
    else:
        allowed = SUPPORTED_EXTENSIONS

    if recursive:
        # safe_scan_walk skips other-app data bundles (e.g.
        # "Photos Library.photoslibrary") without stat-following any
        # symlinked child that points at one — picking ~/Pictures as an
        # import source would otherwise walk the whole Photos library and
        # re-trip the macOS "access data from other apps" prompt.
        #
        # Stream the walk into the filter below rather than collecting
        # every walked path first: a source like a home directory or
        # external disk root can yield millions of non-image filenames,
        # and materializing those would make memory grow with the whole
        # tree (and stall the preview before any photo is copied). The
        # previous Path.rglob path was likewise consumed lazily by
        # ``sorted()``.
        def _candidate_paths():
            for dirpath, _dirnames, filenames in safe_scan_walk(
                str(source_path), onerror=onerror,
            ):
                for name in filenames:
                    yield Path(dirpath) / name
        candidates = _candidate_paths()
    else:
        # safe_iter_dir drops excluded bundle children before the
        # is_file() filter below would stat them. With recursion off, a
        # source like ~/Pictures still contains ``Photos
        # Library.photoslibrary`` (or a symlink to one) as a direct
        # child; a bare iterdir + is_file() would stat that bundle and
        # re-trip the macOS "access data from other apps" TCC prompt
        # this guard exists to avoid, even though the extension filter
        # would have rejected it afterwards. ``safe_iter_dir`` is itself
        # a generator — pass it straight through to the filter so we
        # don't materialize the directory listing twice.
        candidates = safe_iter_dir(str(source_path), onerror=onerror)
    return sorted(
        f
        for f in candidates
        if f.is_file()
        and f.suffix.lower() in allowed
        and not f.name.startswith(".")
    )


def ingest(
    source_dir,
    destination_dir,
    db,
    file_types="both",
    folder_template="%Y/%Y-%m-%d",
    skip_duplicates=True,
    progress_callback=None,
    extra_known_hashes=None,
    skip_paths=None,
    recursive=True,
    verify_by_hash=False,
    duplicate_checker=None,
):
    """Copy and organize photos from source to destination.

    Args:
        source_dir: path to source (e.g., /Volumes/SD_CARD)
        destination_dir: path to destination (e.g., /Volumes/NAS/Photos)
        db: Database instance (used for duplicate identity lookup)
        file_types: "raw", "jpeg", or "both"
        folder_template: strftime format for destination subfolder
        skip_duplicates: if True, skip files that duplicate a cataloged
            photo. By default duplicates are identified by metadata —
            (filename, size, EXIF capture time) — with a content-hash
            fallback for files whose metadata is missing or placeholder;
            see import_dedup for the exact rules and failure modes.
        progress_callback: optional callable(current, total, filename)
        extra_known_hashes: optional set of content hashes to treat as
            known in addition to the catalog. Kept for callers that only
            have hashes; note it disables the size shortcut on the hash
            fallback. Prefer passing a shared ``duplicate_checker``.
        verify_by_hash: if True, identify duplicates by content hash alone
            (reads every byte of every source file — the historical exact
            behavior).
        duplicate_checker: optional import_dedup.DuplicateChecker to use
            (and mutate) instead of building one from ``db``. Pass a shared
            instance when calling ingest() in a loop so files copied by
            earlier iterations are treated as duplicates by later ones even
            though they have not been scanned into the DB yet. Its
            ``verify_by_hash`` takes precedence over the argument above.

    Returns:
        dict with counts: copied, skipped_duplicate, failed, total
    """
    files = discover_source_files(source_dir, file_types, recursive=recursive)
    if skip_paths:
        files = [f for f in files if str(f) not in skip_paths]
    total = len(files)

    # Duplicate oracle over the whole catalog. A source file matching any
    # existing photo in the DB (even in another library root) is skipped
    # so we don't silently duplicate bytes on disk. See import_dedup for
    # the identity rules (metadata-first with hash fallback by default;
    # hash-everything when verify_by_hash).
    checker = None
    dup_token_folders: dict[tuple, set[str]] = {}
    if skip_duplicates:
        checker = duplicate_checker
        if checker is None:
            checker = DuplicateChecker(
                CatalogIndex.from_db(db), verify_by_hash=verify_by_hash,
            )
        if extra_known_hashes:
            checker.add_known_hashes(extra_known_hashes)
        # Destination-scoped identity -> set-of-folder-paths map for populating
        # the caller's post-ingest scan restrict_dirs. The same hash can
        # legitimately appear in more than one destination subfolder (e.g.,
        # the user copied the same photo into multiple date folders), and
        # every matching folder needs to be walked so all of them get
        # linked to the active workspace. Four guards, layered:
        #   1. SQL ``f.status IN ('ok', 'partial')`` — exclude folders the DB
        #      already knows are missing (cheap and visible to static
        #      analysis). Partially-scanned folders still contain valid
        #      indexed hashes, so we must consult them here to avoid
        #      re-importing bytes we already know about.
        #   2. SQL prefix match on ``f.path`` with an explicit ``ESCAPE``
        #      clause — rough subtree cut so we don't haul the whole
        #      library into memory on large DBs. Escaping is required
        #      because destination paths may legally contain SQL LIKE
        #      wildcard characters (``_`` and ``%``).
        #   3. Python slash-normalized component-prefix comparison — strict
        #      path-component matching that catches any residual LIKE wildcard
        #      leaks. ``os.path.normpath`` is applied first so ``..`` segments
        #      in a stored folder path can't lexically appear to be under the
        #      destination while actually resolving outside it.
        #   4. Python ``Path.is_dir`` on the raw stored path — catches
        #      stale ``status IN ('ok', 'partial')`` rows when the folder
        #      was deleted since the last scan and the caller didn't
        #      refresh folder health first.
        # A folder passes only if all four guards agree.
        #
        # The SQL prefilter compares against ``dest_path_str`` (derived
        # from ``str(Path(destination_dir))``, i.e. raw lexical form minus
        # the trailing slash that ``Path`` already strips). We deliberately
        # do NOT apply ``os.path.normpath`` before querying: scanner.scan
        # persists folder paths via ``str(Path(...))``, which keeps ``..``
        # segments intact, so a library that was previously scanned with
        # an unnormalized root (e.g. ``/mnt/photos/../library``) stores
        # rows with those ``..`` segments in place. A pre-normalized query
        # string would silently drop those rows from the prefilter and
        # leave ``known_hash_folders`` empty for duplicate-only ingests
        # into such destinations. ``rstrip("/")`` is kept so the root
        # destination (``"/"``) produces LIKE prefix ``"/%"`` rather than
        # ``"//%"``.
        #
        dest_path_str = str(Path(destination_dir))
        # Strip first so the LIKE prefix for root ("/") becomes "/%"
        # rather than "//%". The equality side falls back to "/" for root.
        # Backslashes are only normalized on Windows, where they are
        # separators; on POSIX they are literal filename characters and
        # converting them would match siblings like "/photos\archive"
        # against destination "/photos".
        if _WINDOWS:
            dest_path_sql_stripped = dest_path_str.replace("\\", "/").rstrip("/")
        else:
            dest_path_sql_stripped = dest_path_str.rstrip("/")
        dest_path_sql = dest_path_sql_stripped or "/"
        dest_like_prefix = _escape_sql_like(dest_path_sql_stripped) + "/%"
        # On Windows the filesystem is case-insensitive AND accepts both
        # separators, so normalize the stored f.path to forward slashes and
        # lower-case both sides to preserve the old Path.is_relative_to
        # behaviour on WindowsPath. On POSIX, do neither: stored paths use
        # the host's literal byte sequence and a literal sibling whose name
        # contains "\" must not be folded into a child of the destination.
        #
        # SQLite's built-in LOWER() only folds ASCII, but Python's str.lower()
        # is Unicode-aware: a stored folder row like 'C:\Älbum\2026' would
        # lower in SQLite to 'c:/Älbum/2026' (Ä stays) while the Python-side
        # destination 'c:\älbum' lowers to 'c:/älbum', so the prefilter would
        # drop the row before the _path_under_root post-filter (which uses
        # Unicode-aware folding via _case_fold_path) ever sees it. Register a
        # Unicode-aware LOWER function on the connection so both sides agree.
        if _WINDOWS:
            db.conn.create_function(
                "LOWER_UNICODE", 1,
                lambda s: s.lower() if s is not None else None,
            )
            path_sql_expr = "LOWER_UNICODE(REPLACE(f.path, '\\', '/'))"
            dest_path_sql_param = dest_path_sql.lower()
            dest_like_prefix_param = dest_like_prefix.lower()
        else:
            path_sql_expr = "f.path"
            dest_path_sql_param = dest_path_sql
            dest_like_prefix_param = dest_like_prefix
        if dest_path_sql_stripped:
            folder_rows = db.conn.execute(
                f"""SELECT p.file_hash, p.filename, p.file_size, p.timestamp,
                          f.path AS folder_path
                   FROM photos p
                   JOIN folders f ON p.folder_id = f.id
                   WHERE (p.file_hash IS NOT NULL OR p.timestamp IS NOT NULL)
                     AND f.status IN ('ok', 'partial')
                     AND (
                       {path_sql_expr} = ?
                       OR {path_sql_expr} LIKE ? ESCAPE '\\'
                     )""",
                (dest_path_sql_param, dest_like_prefix_param),
            ).fetchall()
        else:
            folder_rows = db.conn.execute(
                """SELECT p.file_hash, p.filename, p.file_size, p.timestamp,
                          f.path AS folder_path
                   FROM photos p
                   JOIN folders f ON p.folder_id = f.id
                   WHERE (p.file_hash IS NOT NULL OR p.timestamp IS NOT NULL)
                     AND f.status IN ('ok', 'partial')"""
            ).fetchall()
        for r in folder_rows:
            folder_path = r["folder_path"]
            # Normalise both sides before the subtree check: a stored path
            # like "/dest/sub/../other" is lexically NOT relative to
            # "/dest/sub" but IS relative to "/dest". Without normpath,
            # is_relative_to gives the wrong answer for paths with ".."
            # segments that happen to share a prefix with dest.
            if not _path_under_root(folder_path, dest_path_str):
                continue
            if not Path(folder_path).is_dir():
                continue
            # Index the row under both identity forms — the checker may
            # match a source either way, and each match token must map to
            # the folders holding the original.
            if r["file_hash"] is not None:
                dup_token_folders.setdefault(
                    ("hash", r["file_hash"]), set(),
                ).add(folder_path)
            row_key = stored_metadata_key(
                r["filename"], r["file_size"], r["timestamp"],
            )
            if row_key is not None:
                dup_token_folders.setdefault(
                    ("key", row_key), set(),
                ).add(folder_path)

    copied = 0
    skipped_duplicate = 0
    failed = 0
    copied_paths = []
    duplicate_folders: set[str] = set()
    emitted = 0

    # Pass 1: partition into duplicates vs. survivors. The checker's EXIF
    # prepass batches header reads across the whole card; only files with
    # missing/placeholder metadata (and a plausible size twin) get their
    # bytes read for the hash fallback — or every file when verify_by_hash.
    if checker is not None:
        checker.prepare(files)
    to_copy: list[Path] = []
    for source_file in files:
        if checker is not None:
            try:
                token = checker.match(source_file)
            except Exception as e:
                log.warning("Failed to ingest %s: %s", source_file, e)
                failed += 1
                emitted += 1
                if progress_callback:
                    progress_callback(emitted, total, source_file.name)
                continue

            if token is not None:
                skipped_duplicate += 1
                # Record every destination folder that holds a copy of
                # this file, not just one. The pipeline uses this set
                # verbatim as restrict_dirs, so if we only report one
                # folder the others never get linked to the active
                # workspace.
                duplicate_folders.update(
                    dup_token_folders.get(token, ())
                )
                emitted += 1
                if progress_callback:
                    progress_callback(emitted, total, source_file.name)
                continue

        to_copy.append(source_file)

    # Pass 2: resolve folder-planning timestamps for survivors and copy.
    # In the default metadata mode the checker already resolved every
    # file's EXIF time for the duplicate gate — reuse those reads and only
    # add the mtime fallback. In verify/no-skip modes, batch the
    # EXIF/ExifTool prepass over just the survivors.
    timestamps = _source_file_timestamps(
        to_copy,
        capture_times=(
            {str(f): checker.capture_time(f) for f in to_copy}
            if checker is not None and not checker.verify_by_hash
            else None
        ),
    )

    # Records the destination folder where each identity token actually
    # landed during this batch. Populated only when pass 2 marks an
    # identity as known (successful copy or exact-match destination
    # collision) so an intra-batch duplicate can report the correct
    # post-import scan folder.
    batch_dest_folders: dict[tuple, str] = {}

    for source_file in to_copy:
        try:
            # Intra-batch dedup: a byte-identical sibling earlier in this
            # batch already landed (or matched an existing destination
            # file). Skip without re-copying. We intentionally defer the
            # checker.record() mark to pass 2 success rather than
            # recording it in pass 1 — if the primary's copy fails (e.g.
            # the destination filename collides with a directory of the
            # same name), the sibling still gets a chance to import the
            # bytes.
            if checker is not None:
                token = checker.match(source_file)
                if token is not None:
                    skipped_duplicate += 1
                    dest = batch_dest_folders.get(token)
                    if dest is not None:
                        duplicate_folders.add(dest)
                    emitted += 1
                    if progress_callback:
                        progress_callback(emitted, total, source_file.name)
                    continue

            rel_folder = build_destination_path(
                timestamps.get(source_file), folder_template
            )
            dest_folder = Path(destination_dir) / rel_folder
            dest_folder.mkdir(parents=True, exist_ok=True)

            dest_file = dest_folder / source_file.name

            # Handle filename collision (different file, same name)
            if dest_file.exists():
                src_size = source_file.stat().st_size
                dest_size = dest_file.stat().st_size
                # Zero-byte ↔ zero-byte at the same destination path IS
                # the same file (every empty file is bit-identical to
                # every other), but zero-byte files carry no duplicate
                # identity so the content-match branch below can't
                # recognise this collision, and a ``skip_duplicates``
                # retry of an interrupted card import would otherwise
                # fall through to the numeric-suffix branch and start
                # creating ``name_1.ext``, ``name_2.ext`` placeholder
                # copies of the same empty file forever. The checker is
                # deliberately NOT updated — zero-byte files are kept out
                # of the duplicate-identity index everywhere else, and
                # this skip mirrors that.
                if src_size == 0 and dest_size == 0:
                    skipped_duplicate += 1
                    duplicate_folders.add(str(dest_folder))
                    emitted += 1
                    if progress_callback:
                        progress_callback(emitted, total, source_file.name)
                    continue
                # Same size could be the same bytes — settle it by exact
                # content, never by metadata (a wrong skip here would
                # silently drop a photo). Different size proves a
                # different file with no reads at all.
                if src_size == dest_size:
                    src_hash = (
                        checker.content_hash(source_file)
                        if checker is not None
                        else compute_file_hash(str(source_file))
                    )
                    dest_hash = compute_file_hash(str(dest_file))
                    if src_hash is not None and src_hash == dest_hash:
                        # Exact same file already there
                        skipped_duplicate += 1
                        if checker is not None:
                            for token in checker.record(source_file):
                                batch_dest_folders[token] = str(dest_folder)
                        duplicate_folders.add(str(dest_folder))
                        emitted += 1
                        if progress_callback:
                            progress_callback(emitted, total, source_file.name)
                        continue
                # Different file, same name — add numeric suffix
                stem = dest_file.stem
                suffix = dest_file.suffix
                counter = 1
                while dest_file.exists():
                    dest_file = dest_folder / f"{stem}_{counter}{suffix}"
                    counter += 1

            shutil.copy2(str(source_file), str(dest_file))
            if checker is not None:
                for token in checker.record(source_file):
                    batch_dest_folders[token] = str(dest_folder)
            copied_paths.append(str(dest_file))
            copied += 1

        except Exception as e:
            log.warning("Failed to ingest %s: %s", source_file, e)
            failed += 1

        emitted += 1
        if progress_callback:
            progress_callback(emitted, total, source_file.name)

    return {
        "copied": copied,
        "skipped_duplicate": skipped_duplicate,
        "failed": failed,
        "total": total,
        "copied_paths": copied_paths,
        "duplicate_folders": sorted(duplicate_folders),
    }
