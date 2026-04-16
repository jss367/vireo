"""Detect image files present on disk but not yet ingested into a workspace."""
import os
from pathlib import Path

from image_loader import SUPPORTED_EXTENSIONS


def _known_paths_for_workspace(db, workspace_id):
    """Return the set of absolute paths of photos already ingested into the workspace."""
    rows = db.conn.execute(
        """SELECT f.path AS folder_path, p.filename
           FROM photos p
           JOIN folders f ON f.id = p.folder_id
           JOIN workspace_folders wf ON wf.folder_id = f.id
           WHERE wf.workspace_id = ?""",
        (workspace_id,),
    ).fetchall()
    return {os.path.join(r["folder_path"], r["filename"]) for r in rows}


def _mapped_roots(db, workspace_id):
    """Return the workspace's mapped roots — folders whose parent is not also linked.

    Skips folders marked 'missing'.
    """
    rows = db.conn.execute(
        """SELECT f.id, f.path, f.parent_id, f.status
           FROM folders f
           JOIN workspace_folders wf ON wf.folder_id = f.id
           WHERE wf.workspace_id = ? AND f.status = 'ok'""",
        (workspace_id,),
    ).fetchall()
    linked_ids = {r["id"] for r in rows}
    return [
        {"id": r["id"], "path": r["path"]}
        for r in rows
        if r["parent_id"] is None or r["parent_id"] not in linked_ids
    ]


def count_new_images_for_workspace(db, workspace_id, sample_limit=5):
    """Return {'new_count': int, 'per_root': [...], 'sample': [abs_path, ...]}.

    Walks each mapped root recursively, collects image files, and diffs against
    the set of photo paths already ingested into the workspace.
    """
    known = _known_paths_for_workspace(db, workspace_id)
    roots = _mapped_roots(db, workspace_id)

    per_root = []
    sample = []
    total = 0
    for root in roots:
        root_path = root["path"]
        if not os.path.isdir(root_path):
            per_root.append({"folder_id": root["id"], "path": root_path, "new_count": 0})
            continue

        root_new = 0
        for dirpath, _dirnames, filenames in os.walk(root_path):
            for name in filenames:
                ext = Path(name).suffix.lower()
                if ext not in SUPPORTED_EXTENSIONS:
                    continue
                full = os.path.join(dirpath, name)
                if full in known:
                    continue
                root_new += 1
                if len(sample) < sample_limit:
                    sample.append(full)

        total += root_new
        per_root.append({"folder_id": root["id"], "path": root_path, "new_count": root_new})

    return {"new_count": total, "per_root": per_root, "sample": sample}
