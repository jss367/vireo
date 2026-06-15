"""Smart ingest: copy and organize photos from external source to destination."""

import contextlib
import logging
import os
import posixpath
import shutil
import sys
from datetime import datetime
from pathlib import Path

from grouping import read_exif_timestamp
from image_loader import IMAGE_EXTENSIONS, RAW_EXTENSIONS, SUPPORTED_EXTENSIONS
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
    # Replace backslashes first, then collapse with posixpath.normpath. The
    # reverse order is unsafe on POSIX hosts: os.path.normpath there doesn't
    # recognize backslashes as separators, so a stored Windows-style path
    # like ``C:\dest\sub\..\other`` would survive normalization with its
    # ``..`` intact and bypass the prefix check in _path_under_root.
    normalized = posixpath.normpath(str(s).replace("\\", "/"))
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

    folder_counts = {}
    for source_file in all_files:
        exif_dt = None
        with contextlib.suppress(OSError, ValueError):
            exif_dt = read_exif_timestamp(str(source_file))
        if exif_dt is None:
            with contextlib.suppress(OSError, ValueError, OverflowError):
                exif_dt = datetime.fromtimestamp(source_file.stat().st_mtime)

        rel_folder = build_destination_path(exif_dt, folder_template)
        if not rel_folder:
            rel_folder = "."
        folder_counts[rel_folder] = folder_counts.get(rel_folder, 0) + 1

    dest_path = Path(destination)
    folders = []
    for path in sorted(folder_counts):
        check_path = dest_path if path == "." else dest_path / path
        folders.append({
            "path": path,
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
    }


def discover_source_files(source_dir, file_types="both", recursive=True):
    """Discover image files in source directory.

    Args:
        source_dir: path to source directory (e.g., SD card mount)
        file_types: "raw", "jpeg", "both", or a list of extensions
            (e.g. [".jpg", ".nef"])
        recursive: if True (default), scan subfolders; if False, only scan root

    Returns:
        Sorted list of Path objects for matching files
    """
    source_path = Path(source_dir)
    if not source_path.is_dir():
        return []

    if isinstance(file_types, list):
        allowed = {ext.lower() for ext in file_types}
    elif file_types == "raw":
        allowed = RAW_EXTENSIONS
    elif file_types == "jpeg":
        allowed = IMAGE_EXTENSIONS
    else:
        allowed = SUPPORTED_EXTENSIONS

    candidates = source_path.rglob("*") if recursive else source_path.iterdir()
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
):
    """Copy and organize photos from source to destination.

    Args:
        source_dir: path to source (e.g., /Volumes/SD_CARD)
        destination_dir: path to destination (e.g., /Volumes/NAS/Photos)
        db: Database instance (used for duplicate hash lookup)
        file_types: "raw", "jpeg", or "both"
        folder_template: strftime format for destination subfolder
        skip_duplicates: if True, skip files whose hash matches existing file
        progress_callback: optional callable(current, total, filename)
        extra_known_hashes: optional set of hashes to treat as known in
            addition to those already in the DB.  Pass a shared mutable set
            when calling ingest() in a loop so that files copied by earlier
            iterations are treated as duplicates by later ones even though
            they have not been scanned into the DB yet.

    Returns:
        dict with counts: copied, skipped_duplicate, failed, total
    """
    files = discover_source_files(source_dir, file_types, recursive=recursive)
    if skip_paths:
        files = [f for f in files if str(f) not in skip_paths]
    total = len(files)

    # Load known hashes from database for duplicate detection and merge with
    # any hashes accumulated by previous ingest() calls in the same session.
    known_hashes = set()
    known_hash_folders: dict[str, set[str]] = {}
    if skip_duplicates:
        # Global hash set for dedup decisions. A source file matching any
        # existing photo in the DB (even in another library root) is skipped
        # so we don't silently duplicate bytes on disk.
        rows = db.conn.execute(
            "SELECT file_hash FROM photos WHERE file_hash IS NOT NULL"
        ).fetchall()
        known_hashes = {r["file_hash"] for r in rows}
        # Destination-scoped hash -> set-of-folder-paths map for populating
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
        dest_path_sql_stripped = dest_path_str.replace("\\", "/").rstrip("/")
        dest_path_sql = dest_path_sql_stripped or "/"
        dest_like_prefix = _escape_sql_like(dest_path_sql_stripped) + "/%"
        # On Windows the filesystem is case-insensitive: a destination passed
        # as "c:\photos" must still match folder rows scanned as "C:\Photos\..."
        # to preserve the old Path.is_relative_to behaviour on WindowsPath.
        # Lower-case both sides on Windows; leave POSIX comparisons untouched.
        if _WINDOWS:
            path_sql_expr = "LOWER(REPLACE(f.path, '\\', '/'))"
            dest_path_sql_param = dest_path_sql.lower()
            dest_like_prefix_param = dest_like_prefix.lower()
        else:
            path_sql_expr = "REPLACE(f.path, '\\', '/')"
            dest_path_sql_param = dest_path_sql
            dest_like_prefix_param = dest_like_prefix
        if dest_path_sql_stripped:
            folder_rows = db.conn.execute(
                f"""SELECT p.file_hash, f.path AS folder_path
                   FROM photos p
                   JOIN folders f ON p.folder_id = f.id
                   WHERE p.file_hash IS NOT NULL
                     AND f.status IN ('ok', 'partial')
                     AND (
                       {path_sql_expr} = ?
                       OR {path_sql_expr} LIKE ? ESCAPE '\\'
                     )""",
                (dest_path_sql_param, dest_like_prefix_param),
            ).fetchall()
        else:
            folder_rows = db.conn.execute(
                """SELECT p.file_hash, f.path AS folder_path
                   FROM photos p
                   JOIN folders f ON p.folder_id = f.id
                   WHERE p.file_hash IS NOT NULL
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
            known_hash_folders.setdefault(r["file_hash"], set()).add(folder_path)
        if extra_known_hashes:
            known_hashes |= extra_known_hashes

    copied = 0
    skipped_duplicate = 0
    failed = 0
    copied_paths = []
    duplicate_folders: set[str] = set()

    for i, source_file in enumerate(files):
        try:
            # Compute hash for duplicate detection
            file_hash = compute_file_hash(str(source_file))

            if skip_duplicates and file_hash in known_hashes:
                skipped_duplicate += 1
                # Record every destination folder that holds a copy of
                # this file, not just one. The pipeline uses this set
                # verbatim as restrict_dirs, so if we only report one
                # folder the others never get linked to the active
                # workspace.
                duplicate_folders.update(
                    known_hash_folders.get(file_hash, ())
                )
                if progress_callback:
                    progress_callback(i + 1, total, source_file.name)
                continue

            # Determine destination folder from EXIF date
            exif_dt = None
            try:
                exif_dt = read_exif_timestamp(str(source_file))
            except (OSError, ValueError):
                log.debug("Could not read EXIF timestamp from %s", source_file)
            if exif_dt is None:
                # Fall back to file modification time
                with contextlib.suppress(OSError, ValueError, OverflowError):
                    exif_dt = datetime.fromtimestamp(source_file.stat().st_mtime)

            rel_folder = build_destination_path(exif_dt, folder_template)
            dest_folder = Path(destination_dir) / rel_folder
            dest_folder.mkdir(parents=True, exist_ok=True)

            dest_file = dest_folder / source_file.name

            # Handle filename collision (different file, same name)
            if dest_file.exists():
                dest_hash = compute_file_hash(str(dest_file))
                if file_hash == dest_hash:
                    # Exact same file already there
                    skipped_duplicate += 1
                    known_hashes.add(file_hash)
                    duplicate_folders.add(str(dest_folder))
                    if progress_callback:
                        progress_callback(i + 1, total, source_file.name)
                    continue
                # Different file, same name — add numeric suffix
                stem = dest_file.stem
                suffix = dest_file.suffix
                counter = 1
                while dest_file.exists():
                    dest_file = dest_folder / f"{stem}_{counter}{suffix}"
                    counter += 1

            shutil.copy2(str(source_file), str(dest_file))
            known_hashes.add(file_hash)
            copied_paths.append(str(dest_file))
            copied += 1

        except Exception as e:
            log.warning("Failed to ingest %s: %s", source_file, e)
            failed += 1

        if progress_callback:
            progress_callback(i + 1, total, source_file.name)

    return {
        "copied": copied,
        "skipped_duplicate": skipped_duplicate,
        "failed": failed,
        "total": total,
        "copied_paths": copied_paths,
        "duplicate_folders": sorted(duplicate_folders),
    }
