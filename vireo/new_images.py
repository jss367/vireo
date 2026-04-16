"""Detect image files present on disk but not yet ingested into a workspace."""
import os
import threading
import time
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
                # Mirror ``vireo/scanner.py``: skip dotfiles (e.g. macOS
                # AppleDouble sidecars ``._IMG_0001.JPG``) so we don't count
                # files the scanner will never ingest, which would otherwise
                # produce a stuck "new images" banner.
                if name.startswith("."):
                    continue
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


class NewImagesCache:
    """In-memory per-workspace cache with a TTL ceiling.

    Thread-safe. Invalidation takes a list of workspace_ids (computed by the
    caller from the set of folder_ids touched by a scan).

    A per-workspace generation counter protects against a race between an
    in-flight compute and a concurrent invalidation: a caller snapshots the
    generation before starting the walk and passes it to :meth:`set`; if
    invalidation bumps the generation during the walk, the stale result is
    silently dropped instead of repopulating the cache.
    """

    def __init__(self, ttl_seconds=300):
        self._ttl = ttl_seconds
        self._entries = {}  # workspace_id -> (result_dict, set_at_monotonic)
        self._generations = {}  # workspace_id -> int
        self._lock = threading.Lock()

    def get(self, workspace_id):
        with self._lock:
            entry = self._entries.get(workspace_id)
            if entry is None:
                return None
            result, set_at = entry
            if time.monotonic() - set_at > self._ttl:
                del self._entries[workspace_id]
                return None
            return result

    def get_generation(self, workspace_id):
        """Return the current generation for a workspace (0 if unseen)."""
        with self._lock:
            return self._generations.get(workspace_id, 0)

    def set(self, workspace_id, result, generation=None):
        """Store ``result`` for ``workspace_id``.

        If ``generation`` is provided and no longer matches the current
        generation for the workspace (i.e. an invalidation ran after the
        caller snapshotted it), the write is silently dropped. Callers that
        don't care about the race can omit ``generation`` and the write is
        unconditional.
        """
        with self._lock:
            if generation is not None:
                current = self._generations.get(workspace_id, 0)
                if generation != current:
                    return
            self._entries[workspace_id] = (result, time.monotonic())

    def invalidate_workspaces(self, workspace_ids):
        with self._lock:
            for wid in workspace_ids:
                self._entries.pop(wid, None)
                self._generations[wid] = self._generations.get(wid, 0) + 1

    def clear(self):
        with self._lock:
            self._entries.clear()
            self._generations.clear()


_shared_cache = NewImagesCache()


def get_shared_cache():
    """Return the process-wide shared NewImagesCache.

    Per-thread and per-request Database instances all reference the same
    cache so invalidation from scan workers is visible to API readers.
    """
    return _shared_cache
