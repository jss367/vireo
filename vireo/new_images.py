"""Detect image files present on disk but not yet ingested into a workspace."""
import logging
import os
import threading
import time
from pathlib import Path

from image_loader import SUPPORTED_EXTENSIONS

log = logging.getLogger(__name__)


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


def mapped_roots(db, workspace_id):
    """Return the workspace's mapped roots — linked folders whose ancestor chain
    contains no other linked folder. Skips folders marked 'missing'. Folders
    flagged ``'partial'`` from an interrupted scan are kept so a rescan can
    pick up where the previous one stopped.

    Checking only the immediate parent would over-include when an intermediate
    folder was unlinked but a deeper descendant is still linked (e.g. /A linked,
    /A/B unlinked, /A/B/C linked — both /A and /A/B/C would otherwise be roots
    and the walk would double-count files under /A/B/C).
    """
    rows = db.conn.execute(
        """SELECT f.id, f.path, f.parent_id
           FROM folders f
           JOIN workspace_folders wf ON wf.folder_id = f.id
           WHERE wf.workspace_id = ? AND f.status IN ('ok', 'partial')""",
        (workspace_id,),
    ).fetchall()
    linked_ids = {r["id"] for r in rows}
    if not linked_ids:
        return []

    # Load parent_id for every folder — needed to walk arbitrary-depth ancestor
    # chains where intermediate folders may not be linked.
    parent_of = {
        r["id"]: r["parent_id"]
        for r in db.conn.execute("SELECT id, parent_id FROM folders").fetchall()
    }

    def has_linked_ancestor(folder_id):
        parent = parent_of.get(folder_id)
        while parent is not None:
            if parent in linked_ids:
                return True
            parent = parent_of.get(parent)
        return False

    return [
        {"id": r["id"], "path": r["path"]}
        for r in rows
        if not has_linked_ancestor(r["id"])
    ]


def count_new_images_for_workspace(db, workspace_id, sample_limit=5):
    """Return {'new_count': int, 'per_root': [...], 'sample': [abs_path, ...]}.

    Walks each mapped root recursively, collects image files, and diffs against
    the set of photo paths already ingested into the workspace.
    """
    known = _known_paths_for_workspace(db, workspace_id)
    roots = mapped_roots(db, workspace_id)

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
                if sample_limit is None or len(sample) < sample_limit:
                    sample.append(full)

        total += root_new
        per_root.append({"folder_id": root["id"], "path": root_path, "new_count": root_new})

    return {"new_count": total, "per_root": per_root, "sample": sample}


class NewImagesCache:
    """In-memory per-``(db_path, workspace_id)`` cache with a TTL ceiling.

    Thread-safe. Keyed by the compound ``(db_path, workspace_id)`` so that two
    :class:`Database` instances pointed at different SQLite files cannot read
    each other's cached results — ``workspace_id=1`` (the default workspace)
    is reused across databases, and without the db_path scope tests or
    multi-database embeddings would cross-contaminate.

    Invalidation takes a list of workspace_ids (computed by the caller from
    the set of folder_ids touched by a scan) plus the originating ``db_path``;
    only entries and generations for that db are affected.

    A per-key generation counter protects against a race between an
    in-flight compute and a concurrent invalidation: a caller snapshots the
    generation before starting the walk and passes it to :meth:`set`; if
    invalidation bumps the generation during the walk, the stale result is
    silently dropped instead of repopulating the cache.
    """

    def __init__(self, ttl_seconds=300):
        self._ttl = ttl_seconds
        # key=(db_path, workspace_id) -> (result_dict, set_at_monotonic)
        self._entries = {}
        # key=(db_path, workspace_id) -> int
        self._generations = {}
        # key=(db_path, workspace_id) -> Event signalling the in-flight
        # background compute has finished (success or failure).
        self._inflight = {}
        self._lock = threading.Lock()

    def get(self, db_path, workspace_id):
        key = (db_path, workspace_id)
        with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                return None
            result, set_at = entry
            if time.monotonic() - set_at > self._ttl:
                del self._entries[key]
                return None
            return result

    def get_generation(self, db_path, workspace_id):
        """Return the current generation for ``(db_path, workspace_id)`` (0 if unseen)."""
        key = (db_path, workspace_id)
        with self._lock:
            return self._generations.get(key, 0)

    def set(self, db_path, workspace_id, result, generation=None):
        """Store ``result`` for ``(db_path, workspace_id)``.

        If ``generation`` is provided and no longer matches the current
        generation for the key (i.e. an invalidation ran after the
        caller snapshotted it), the write is silently dropped. Callers that
        don't care about the race can omit ``generation`` and the write is
        unconditional.
        """
        key = (db_path, workspace_id)
        with self._lock:
            if generation is not None:
                current = self._generations.get(key, 0)
                if generation != current:
                    return
            self._entries[key] = (result, time.monotonic())

    def invalidate_workspaces(self, db_path, workspace_ids):
        with self._lock:
            for wid in workspace_ids:
                key = (db_path, wid)
                self._entries.pop(key, None)
                self._generations[key] = self._generations.get(key, 0) + 1

    def kickoff_compute(self, db_path, workspace_id, compute_fn):
        """Ensure a background compute is running for ``(db_path, workspace_id)``.

        If no compute is in flight, spawn a daemon thread that calls
        ``compute_fn()`` and stores the result in the cache. If one is already
        in flight, the existing thread is reused. Either way, returns an
        ``Event`` that fires when the in-flight compute finishes (success or
        failure) so the caller can ``Event.wait(timeout=...)`` to optionally
        block briefly for a fresh result.

        The generation snapshot is taken inside the lock at kickoff time so a
        concurrent invalidation can still drop the stale write — same race
        protection as :meth:`get_new_images_for_workspace`.
        """
        key = (db_path, workspace_id)
        with self._lock:
            existing = self._inflight.get(key)
            if existing is not None:
                return existing
            event = threading.Event()
            self._inflight[key] = event
            generation = self._generations.get(key, 0)

        def worker():
            try:
                result = compute_fn()
                self.set(db_path, workspace_id, result, generation=generation)
            except Exception:
                log.exception(
                    "new-images background compute failed for %s ws=%s",
                    db_path, workspace_id,
                )
            finally:
                with self._lock:
                    self._inflight.pop(key, None)
                event.set()

        threading.Thread(
            target=worker, daemon=True, name="new-images-compute",
        ).start()
        return event

    def clear(self):
        with self._lock:
            self._entries.clear()
            self._generations.clear()
            self._inflight.clear()


_shared_cache = NewImagesCache()


def get_shared_cache():
    """Return the process-wide shared NewImagesCache.

    Per-thread and per-request Database instances all reference the same
    cache so invalidation from scan workers is visible to API readers.
    """
    return _shared_cache


def invalidate_new_images_after_scan(db, root):
    """Invalidate the new-images cache for every workspace linked to any folder
    touched by a scan of ``root``.

    Uses a LIKE query because ``scanner.scan`` auto-registers subfolders as
    their own ``folders`` rows (see ``vireo/db.py`` ``add_folder``), so we
    need to invalidate caches for all workspaces that reference any of those
    descendant folders, not just the explicit scan root.

    Lives in this module (not ``app.py``) so non-Flask code paths such as
    ``pipeline_job.py`` can import it without pulling in the app module.
    """
    # Canonicalize the root to match what the scanner stores. scanner.scan passes
    # folder paths through str(Path(...)), which strips trailing slashes but
    # preserves `..` segments. Using os.path.normpath here would resolve `..` and
    # produce a mismatch against the stored path, leaving the cache stale after a
    # successful scan.
    root = str(Path(root))
    # LIKE wildcards (%, _) in `root` are not escaped. Worst case is a harmless
    # over-invalidation that triggers a re-walk. The descendant pattern uses
    # os.sep so it matches what the scanner stores via str(Path(...)) on both
    # POSIX and Windows.
    touched_ids = [r["id"] for r in db.conn.execute(
        "SELECT id FROM folders WHERE path = ? OR path LIKE ?",
        (root, root.rstrip("/\\") + os.sep + "%"),
    ).fetchall()]
    db.invalidate_new_images_cache_for_folders(touched_ids)
