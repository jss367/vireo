"""Chained-import helpers: which folders a chained NAS move relocates.

The chained import→process→move flow mirrors the local archive layout onto
the remote target: a folder at ``<local_archive_root>/2026/trip`` moves to
``<remote_path>/2026/trip``. This module derives the minimal non-nested set
of imported folders to move — moving an ancestor also moves its descendants,
so a nested destination folder must not get its own move job.
"""

import os


def minimal_move_set(archive_root, folders):
    """Return the minimal covering set of folders to move, with subpaths.

    ``folders`` is an iterable of ``(folder_id, path)`` for catalog folders
    that received imported photos. Returns a list of
    ``{"folder_id": int, "subpath": str}`` where ``subpath`` is the folder's
    path relative to ``archive_root`` in POSIX form (the move job's remote
    subpath). Folders outside the root — and the root itself — are skipped:
    request-time validation prevents both, so this is defensive, and moving
    the root would sweep unrelated shoots into the transfer.
    """
    root = os.path.realpath(archive_root)
    inside = []
    for folder_id, path in folders:
        real = os.path.realpath(path)
        try:
            common = os.path.commonpath([real, root])
        except ValueError:
            continue  # different drives — cannot be under the root
        if common != root or real == root:
            continue
        inside.append((folder_id, real))
    # Shortest paths first so ancestors are considered before descendants;
    # keep a folder only when no already-kept ancestor covers it.
    inside.sort(key=lambda item: (len(item[1]), item[1]))
    kept = []
    for folder_id, real in inside:
        covered = any(
            os.path.commonpath([real, kept_path]) == kept_path
            for _, kept_path in kept
        )
        if not covered:
            kept.append((folder_id, real))
    return [
        {
            "folder_id": folder_id,
            "subpath": os.path.relpath(real, root).replace(os.sep, "/"),
        }
        for folder_id, real in kept
    ]
