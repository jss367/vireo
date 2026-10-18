"""Photo and folder move operations with copy-verify-delete safety.

The package's modules, by responsibility:

- ``photos``: ``move_photos``, which moves individual photos one at a time
  (``_PhotoMove`` and its collaborators).
- ``dates``: ``move_folder_by_date`` and the capture-date folder planning
  behind it.
- ``folder``: ``move_folder``, which moves a whole folder tree, and its
  coordinator ``_FolderMove``. The phases live in ``folder_target``
  (destination and preflight), ``folder_transfer`` (copy and verification)
  and ``folder_catalog`` (the catalog rewrite and the post-commit rebases).
- ``transfer``: copying files (single-file copy, rsync with progress and a
  stall watchdog, the shutil fallback, Finder-metadata excludes).
- ``verify``: checking a copy against its source, before and after it is
  made, locally and over SSH.
- ``remote``: SSH and rsync plumbing for NAS targets.
- ``paths``: destination naming and alias-aware path containment.

The catalog SQL lives in ``vireo/repositories/moves_merge.py``
(``db.moves_merge``).

This module re-exports what code outside the package imports. A module
inside the package looks a name up in its own globals, so a test that
patches one of those names patches the submodule that calls it (for
example ``move.folder_transfer._run_rsync_streamed``), not this package.
"""

from move.dates import (
    _folder_subtree_photos,
    folder_date_move_photo_ids,
    move_folder_by_date,
    plan_folder_date_moves,
    plan_folder_date_moves_with_capture_dates,
)
from move.folder import move_folder
from move.folder_catalog import _plan_moved_file_mtimes
from move.paths import (
    _case_insensitive_root,
    _is_case_insensitive_path,
    _path_equal_or_descends,
    _samefile_or_false,
    _samefile_tristate,
    _tracked_destination_ancestor,
    _tracked_destination_overlap,
    normalize_destination_name,
    resolve_folder_dest,
)
from move.photos import _is_same_directory, _xmp_path, move_photos
from move.remote import (
    _BUNDLED_RSYNC_CANDIDATES,
    _platform_rsync_candidates,
    _remote_dir_exists,
    _remote_free_bytes,
    _remote_mkdir_p,
    _ssh_rsh_string,
    build_remote_move_spec,
    is_gnu_rsync,
    remote_preflight,
    resolve_rsync_bin,
    resolve_ssh_bin,
    rsync_dest_spec,
    rsync_install_guidance,
    sanitize_subpath,
    test_remote_connection,
)
from move.transfer import (
    _copy_and_verify,
    _escape_rsync_pattern,
    _rsync_finder_metadata_exclude_file,
    _rsync_finder_metadata_excludes,
    _run_rsync_streamed,
)
from move.verify import (
    _find_content_conflict,
    _find_remote_content_conflict,
    _first_missing_source_file,
    _remote_verify_complete,
    preview_merge,
    remote_verify_files,
)

__all__ = [
    "_BUNDLED_RSYNC_CANDIDATES",
    "_case_insensitive_root",
    "_copy_and_verify",
    "_escape_rsync_pattern",
    "_find_content_conflict",
    "_find_remote_content_conflict",
    "_first_missing_source_file",
    "_folder_subtree_photos",
    "_is_case_insensitive_path",
    "_is_same_directory",
    "_path_equal_or_descends",
    "_plan_moved_file_mtimes",
    "_platform_rsync_candidates",
    "_remote_dir_exists",
    "_remote_free_bytes",
    "_remote_mkdir_p",
    "_remote_verify_complete",
    "_rsync_finder_metadata_exclude_file",
    "_rsync_finder_metadata_excludes",
    "_run_rsync_streamed",
    "_samefile_or_false",
    "_samefile_tristate",
    "_ssh_rsh_string",
    "_tracked_destination_ancestor",
    "_tracked_destination_overlap",
    "_xmp_path",
    "build_remote_move_spec",
    "folder_date_move_photo_ids",
    "is_gnu_rsync",
    "move_folder",
    "move_folder_by_date",
    "move_photos",
    "normalize_destination_name",
    "plan_folder_date_moves",
    "plan_folder_date_moves_with_capture_dates",
    "preview_merge",
    "remote_preflight",
    "remote_verify_files",
    "resolve_folder_dest",
    "resolve_rsync_bin",
    "resolve_ssh_bin",
    "rsync_dest_spec",
    "rsync_install_guidance",
    "sanitize_subpath",
    "test_remote_connection",
]
