"""Chained-import helpers: which folders a chained NAS move relocates.

The chained import→process→move flow mirrors the local archive layout onto
the remote target: a folder at ``<local_archive_root>/2026/trip`` moves to
``<remote_path>/2026/trip``. This module derives the minimal non-nested set
of imported folders to move — moving an ancestor also moves its descendants,
so a nested destination folder must not get its own move job.
"""

import os


def minimal_move_set(archive_root, folders):
    """Return ``(moves, skipped)``: the minimal covering move set plus skips.

    ``folders`` is an iterable of ``(folder_id, path)`` for catalog folders
    that received imported photos. ``moves`` is a list of
    ``{"folder_id": int, "subpath": str}`` where ``subpath`` is the folder's
    path relative to ``archive_root`` in POSIX form (the move job's remote
    subpath). ``skipped`` lists folders that cannot get a move job, as
    ``{"folder_id": int, "reason": "root" | "outside_root"}``: the root
    itself (an empty folder template catalogs photos directly on the root,
    and moving the root would sweep unrelated shoots into the transfer) and
    folders outside the root (request-time validation prevents this, so it
    is defensive). Callers must surface skips — photos in a skipped folder
    stay local after the chain completes.
    """
    root = os.path.realpath(archive_root)
    inside, skipped = [], []
    for folder_id, path in folders:
        real = os.path.realpath(path)
        try:
            common = os.path.commonpath([real, root])
        except ValueError:
            # Different drives — cannot be under the root.
            skipped.append({"folder_id": folder_id, "reason": "outside_root"})
            continue
        if real == root:
            skipped.append({"folder_id": folder_id, "reason": "root"})
            continue
        if common != root:
            skipped.append({"folder_id": folder_id, "reason": "outside_root"})
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
    moves = [
        {
            "folder_id": folder_id,
            "subpath": os.path.relpath(real, root).replace(os.sep, "/"),
        }
        for folder_id, real in kept
    ]
    return moves, skipped
