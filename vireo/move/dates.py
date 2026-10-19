"""Moving a folder's photos into capture-date subfolders.

``plan_folder_date_moves`` groups a folder subtree's tracked photos by the
date folder a template renders for each. ``move_folder_by_date`` then moves
each group with ``move_photos``.
"""

import contextlib
import os
import posixpath
import sys
from datetime import datetime

from .paths import _case_insensitive_root, _path_equal_or_descends
from .photos import move_photos


def _photo_capture_datetime(photo):
    """Resolve one photo's date-folder timestamp: EXIF capture time, else file
    mtime. Shared by the date-move planner and the UI's example-folder samples
    so a label can never name a folder the move wouldn't create.
    """
    try:
        from ..capture_time import _capture_datetime
    except ImportError:
        from capture_time import _capture_datetime

    capture_dt = _capture_datetime(photo)
    if capture_dt is None and photo["file_mtime"] is not None:
        try:
            capture_dt = datetime.fromtimestamp(float(photo["file_mtime"]))
        except (OSError, OverflowError, TypeError, ValueError):
            capture_dt = None
    return capture_dt


def _folder_subtree_photos(db, folder_id):
    """Tracked photos in a folder and its descendants, ordered by id.

    Only the columns date-folder planning actually reads are selected. A
    ``p.*`` here is expensive at library scale: ``photos`` carries two DINOv2
    embedding BLOBs and the full ``exif_data`` JSON, which on a 60k-photo
    subtree is ~800MB pulled into Python to compute one date per row. The
    keystroke-debounced move preflight runs this on every destination edit.

    Descendant matching is alias-aware (symlinks, case-folding volumes,
    Windows separators) — see the inline notes below.
    """
    folder = db.moves_merge.folder_path_row(folder_id)
    if not folder:
        raise ValueError("Folder not found")

    # Fold '\' to '/' only on Windows, where either separator can appear in
    # stored paths. On POSIX '\' is a legal filename character — folding it
    # would collapse a sibling like '/photos/a\b' onto the descendants of
    # '/photos/a/b/...' and drag unrelated rows into this move. This mirrors
    # the platform-aware treatment in ``ingest.py``.
    #
    # Also case-fold on Windows: `C:\Photos` and `c:\photos\2026` refer to
    # the same subtree on Windows' case-insensitive FS, and without folding
    # the SQL prefix comparison would drop the differently-cased descendant.
    # SQLite's built-in LOWER() only folds ASCII, so mirror the ingest
    # prefilter's Unicode-aware LOWER_UNICODE helper so both sides agree on
    # non-ASCII stems (e.g. `C:\Älbum`).
    if sys.platform == "win32":
        sql_functions = [(
            "LOWER_UNICODE", 1,
            lambda s: s.lower() if s is not None else None,
        )]
        root = folder["path"].replace("\\", "/").rstrip("/").lower()
        prefix = root + "/"
        path_expr = "LOWER_UNICODE(REPLACE(f.path, '\\', '/'))"
        # Include both true descendants and a separate catalog row that is
        # an exact case/separator alias of the selected folder. The latter is
        # not covered by ``f.id = ?`` and has no trailing slash for the prefix
        # predicate, but Windows resolves both rows to the same directory.
        descendant_predicate = (
            f"({path_expr} = ? OR substr({path_expr}, 1, ?) = ?)"
        )
        descendant_params = (root, len(prefix), prefix)
    else:
        # Always route POSIX descendant matching through the alias-aware
        # containment helper. A raw lexical SQL prefix misses two real-world
        # aliasing surfaces:
        #   - Symlinks. The selected folder ``/photos/card`` may resolve to
        #     ``/mnt/card`` while a tracked child row is stored as
        #     ``/mnt/card/day``. ``substr(f.path, ...)`` compares strings and
        #     never touches the FS, so the descendant is silently dropped
        #     from the date-move plan even though it belongs under the
        #     selected subtree.
        #   - Case-insensitive POSIX volumes (default macOS APFS, mounted
        #     CIFS, opt-in APFS on Linux). Lexical prefixes omit
        #     differently-cased descendants; scoping the fold via
        #     ``_case_insensitive_root`` keeps mixed mount trees safe.
        # ``_path_equal_or_descends`` collapses both — plus normcase on
        # Windows — via ``realpath`` and ``samefile``. On a plain
        # case-sensitive POSIX FS with no symlinks the helper still degrades
        # to a fast string comparison inside ``realpath``. Cache by folder
        # path because the join can visit the same folder once per photo.
        case_insensitive_root = _case_insensitive_root(folder["path"])
        descendant_cache = {}

        def _date_move_descends(candidate):
            if candidate not in descendant_cache:
                descendant_cache[candidate] = int(
                    _path_equal_or_descends(
                        candidate,
                        folder["path"],
                        case_insensitive_root=case_insensitive_root,
                    )
                )
            return descendant_cache[candidate]

        sql_functions = [(
            "VIREO_DATE_MOVE_DESCENDS", 1, _date_move_descends,
        )]
        descendant_predicate = "VIREO_DATE_MOVE_DESCENDS(f.path) = 1"
        descendant_params = ()
    return db.moves_merge.date_move_subtree_photos(
        folder_id, descendant_predicate, descendant_params, sql_functions,
    )


def folder_date_move_photo_ids(db, folder_id):
    """The same physical subtree the date-move planner will process.

    A linked root can contain detached descendants. Preview their impact too,
    since moving a physical tree affects photos beyond workspace browse scope.
    Callers must validate access to the selected root before using this helper.
    """
    return [row["id"] for row in _folder_subtree_photos(db, folder_id)]


def plan_folder_date_moves(db, folder_id, destination, folder_template):
    """Plan a folder's tracked photos into capture-date destinations.

    The date comes from the same catalog capture-time helper used elsewhere;
    file mtime is the fallback, matching import's folder-planning behavior.
    Returns a list ordered by rendered relative path so previews and jobs are
    deterministic. Each item contains ``relative_path``, ``destination``,
    ``photo_ids``, and ``photo_count``.
    """
    return plan_folder_date_moves_with_capture_dates(
        db, folder_id, destination, folder_template)[0]


def plan_folder_date_moves_with_capture_dates(
    db, folder_id, destination, folder_template,
):
    """``plan_folder_date_moves`` plus the capture datetimes behind the plan.

    The move page labels each date-format preset with an example folder name,
    and that example has to be a folder this very plan would create. Handing
    back the resolved datetimes lets the preflight endpoint render those
    labels from the same scan that produced the plan — one pass over the
    subtree, and no way for the label to drift from the folder list beside it.

    Returns ``(plans, capture_datetimes)``; ``capture_datetimes`` holds one
    entry per tracked photo, ``None`` where no date could be resolved.
    """
    if not isinstance(destination, str) or not os.path.isabs(destination):
        raise ValueError("destination must be an absolute path")
    if not isinstance(folder_template, str) or not folder_template.strip():
        raise ValueError("folder_template must be a non-empty string")

    try:
        from ..ingest import build_destination_path
    except ImportError:
        from ingest import build_destination_path

    photos = _folder_subtree_photos(db, folder_id)

    groups = {}
    capture_dts = []
    template = folder_template.strip()
    for photo in photos:
        capture_dt = _photo_capture_datetime(photo)
        capture_dts.append(capture_dt)
        relative = build_destination_path(capture_dt, template, photo["filename"])
        if not relative:
            raise ValueError("folder template produced an empty path")
        # Canonicalize harmless dot components before grouping or joining.
        # Catalog folder paths are keyed by their exact text, so preserving
        # ``./`` here could create a second row for ``/archive/./2026`` beside
        # the existing ``/archive/2026`` even though both names address the
        # same directory (and produce different developed-output hashes).
        relative = posixpath.normpath(relative)
        if relative == ".":
            raise ValueError("folder template produced an empty path")
        groups.setdefault(relative, []).append(int(photo["id"]))

    # Defense-in-depth: ``build_destination_path`` already rejects unsafe
    # templates and unsafe rendered results (see ``ingest._is_unsafe_path``),
    # but ``plan_folder_date_moves`` and ``move_folder_by_date`` are public
    # functions that could be invoked without the API-layer template guard.
    # Verify the joined absolute path really sits under ``destination`` so a
    # traversal that slips past the string-level checks can't escape the
    # chosen root.
    destination_root = os.path.realpath(destination)
    _refuse_non_directory_date_destination(destination)
    plans = []
    for relative in sorted(groups):
        plans.append(_date_move_plan_item(
            destination, destination_root, relative, groups[relative]))
    return plans, capture_dts


def _refuse_non_directory_date_destination(destination):
    """Raise ``ValueError`` if ``destination`` or its first existing
    ancestor is not a directory."""
    # Reject a destination root that is itself a regular file or dangling
    # symlink. The ancestor walk in ``_date_move_plan_item`` only checks
    # paths *inside* ``destination`` (``depth`` starts at 1), so without this
    # hoisted check a template like ``%Y/%m`` against ``destination='/archive'``
    # where ``/archive`` is a plain file would pass preflight — the
    # rendered leaf ``/archive/2026/07`` does not lexist yet — and
    # ``os.makedirs`` in the worker would then raise
    # ``NotADirectoryError``/``FileExistsError`` instead of returning
    # the structured preflight error this guard is meant to provide.
    if os.path.lexists(destination) and not os.path.isdir(destination):
        raise ValueError(
            f"date destination already exists and is not a directory: "
            f"{destination}"
        )
    # When ``destination`` does not lexist yet, walk its own ancestors
    # upward until one lexists. The check above and the intermediate-
    # ancestor loop in ``_date_move_plan_item`` only cover ``destination``
    # itself and paths *inside* it; without this walk a request like
    # ``destination='/archive/root'`` where ``/archive`` is a plain file
    # (or dangling symlink) passes preflight and ``os.makedirs`` in the
    # worker then raises ``NotADirectoryError``/``FileExistsError``
    # instead of returning the structured date-destination error this
    # guard is meant to provide.
    if not os.path.lexists(destination):
        ancestor = os.path.dirname(destination)
        previous = None
        while ancestor and ancestor != previous:
            if os.path.lexists(ancestor):
                if not os.path.isdir(ancestor):
                    raise ValueError(
                        f"date destination already exists and is not a "
                        f"directory: {ancestor}"
                    )
                break
            previous = ancestor
            ancestor = os.path.dirname(ancestor)


def _date_move_plan_item(destination, destination_root, relative, photo_ids):
    """Validate one rendered date folder and return its plan entry."""
    candidate = os.path.join(destination, *relative.split("/"))
    resolved = os.path.realpath(candidate)
    try:
        common = os.path.commonpath([destination_root, resolved])
    except ValueError:
        common = ""
    if common != destination_root:
        raise ValueError(
            f"folder template produced an unsafe path: {relative!r}"
        )
    # ``lexists`` (not ``exists``) is needed so a dangling symlink still
    # trips this guard: ``exists`` follows the link and reports False,
    # but ``os.makedirs(exist_ok=True)`` in ``move_photos`` would then
    # raise ``FileExistsError`` for that same lexists-true entry and
    # crash the background job instead of surfacing a normal collision.
    #
    # Walk every intermediate ancestor between ``destination`` and the
    # rendered candidate too. A nested template like ``%Y/%m`` renders
    # ``2026/07``; if ``destination/2026`` is already a regular file (or
    # a dangling symlink), the final candidate does not lexist yet and
    # only the leaf check misses it, but ``os.makedirs`` in the worker
    # would then raise ``NotADirectoryError``/``FileExistsError`` and
    # abort the background job with an opaque exception instead of
    # returning a structured preflight error.
    parts = relative.split("/")
    for depth in range(1, len(parts) + 1):
        ancestor = os.path.join(destination, *parts[:depth])
        if os.path.lexists(ancestor) and not os.path.isdir(ancestor):
            raise ValueError(
                f"date destination already exists and is not a directory: "
                f"{ancestor}"
            )
    return {
        "relative_path": relative,
        "destination": candidate,
        "photo_ids": photo_ids,
        "photo_count": len(photo_ids),
    }


def move_folder_by_date(db, folder_id, destination, folder_template,
                        progress_cb=None, developed_dir="", keep_visible=True):
    """Move a folder's tracked photos into capture-date subfolders.

    Each photo uses ``move_photos``' copy/verify/catalog-update/delete order,
    including XMP and RAW/JPEG companions. Existing same-name files are never
    overwritten. Unlike ``move_folder``, this intentionally moves photos (and
    their companions), not unrelated untracked files in the source tree.
    After a successful move, remove the selected source only if empty;
    otherwise report the remaining files for optional cleanup in Jobs.

    When ``developed_dir`` is set (matching the caller's configured
    ``darktable_output_dir``), each moved photo's developed-output file is
    rebased to the new folder's key so exports/full-resolution lookups
    still find the render instead of falling back to RAW. Rebasing has to
    happen per photo here (not per source folder as ``move_folder`` does)
    because photos in one source folder can fan out to many date
    destinations.
    """
    source_row = db.moves_merge.folder_path_row(folder_id)
    source_path = source_row["path"] if source_row else None
    source_device = None
    source_inode = None
    if source_path:
        with contextlib.suppress(OSError):
            source_stat = os.stat(source_path)
            source_device, source_inode = source_stat.st_dev, source_stat.st_ino
    groups = plan_folder_date_moves(
        db, folder_id, destination, folder_template,
    )
    if not groups:
        return {
            "moved": 0,
            "errors": ["No tracked photos found in the source folder"],
            "destinations": [],
            "destination_count": 0,
        }
    total = sum(group["photo_count"] for group in groups)
    completed = 0
    moved = 0
    already_in_place = 0
    errors = []
    destinations = []

    # Share one listing cache across all groups so the developed-output
    # subdirs of each source folder are listed once per move-folder-by-date
    # run rather than once per photo. Without this the per-photo relocate
    # helper's ``os.listdir`` would run N times for a folder with N
    # developed photos, degrading large jobs quadratically.
    developed_listing_cache = {}
    for group in groups:
        group_start = completed

        def group_progress(current, _total, filename, _start=group_start):
            if progress_cb:
                progress_cb(_start + current, total, filename,
                            "Organizing by capture date")

        result = move_photos(
            db,
            group["photo_ids"],
            group["destination"],
            progress_cb=group_progress,
            developed_dir=developed_dir,
            developed_listing_cache=developed_listing_cache,
            keep_visible=keep_visible,
        )
        group_moved = int(result.get("moved", 0))
        moved += group_moved
        already_in_place += int(result.get("already_in_place", 0))
        errors.extend(result.get("errors") or [])
        completed += group["photo_count"]
        destinations.append({
            "path": group["destination"],
            "planned": group["photo_count"],
            "moved": group_moved,
            "already_in_place": int(result.get("already_in_place", 0)),
        })

    result = {
        "moved": moved,
        "already_in_place": already_in_place,
        "errors": errors,
        "destinations": destinations,
        "destination_count": len(destinations),
    }
    if moved and not errors and source_path:
        try:
            from ..move_cleanup import finish_source
        except ImportError:
            from move_cleanup import finish_source
        result["source_cleanup"] = finish_source(db, source_path, source_device, source_inode)
    return result
