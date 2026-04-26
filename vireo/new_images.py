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


def count_new_images_for_workspace(db, workspace_id, sample_limit=5,
                                   progress_callback=None,
                                   progress_every=250):
    """Return {'new_count': int, 'per_root': [...], 'sample': [abs_path, ...]}.

    Walks each mapped root recursively, collects image files, and diffs against
    the set of photo paths already ingested into the workspace.

    ``progress_callback``, if given, is invoked as
    ``progress_callback(files_checked, new_found)`` once every
    ``progress_every`` files traversed (counting all candidate filenames,
    including ones we skip), and once at the end with the final totals.
    Callers use this to surface live progress for transparency without
    needing to refactor the walk.
    """
    known = _known_paths_for_workspace(db, workspace_id)
    roots = mapped_roots(db, workspace_id)

    per_root = []
    sample = []
    total = 0
    files_checked = 0
    last_emitted = 0

    def _maybe_emit():
        nonlocal last_emitted
        if progress_callback is None:
            return
        if files_checked - last_emitted >= progress_every:
            progress_callback(files_checked, total)
            last_emitted = files_checked

    for root in roots:
        root_path = root["path"]
        if not os.path.isdir(root_path):
            per_root.append({"folder_id": root["id"], "path": root_path, "new_count": 0})
            continue

        root_new = 0
        for dirpath, _dirnames, filenames in os.walk(root_path):
            for name in filenames:
                files_checked += 1
                # Mirror ``vireo/scanner.py``: skip dotfiles (e.g. macOS
                # AppleDouble sidecars ``._IMG_0001.JPG``) so we don't count
                # files the scanner will never ingest, which would otherwise
                # produce a stuck "new images" banner.
                if name.startswith("."):
                    _maybe_emit()
                    continue
                ext = Path(name).suffix.lower()
                if ext not in SUPPORTED_EXTENSIONS:
                    _maybe_emit()
                    continue
                full = os.path.join(dirpath, name)
                if full in known:
                    _maybe_emit()
                    continue
                root_new += 1
                total += 1
                if sample_limit is None or len(sample) < sample_limit:
                    sample.append(full)
                _maybe_emit()

        per_root.append({"folder_id": root["id"], "path": root_path, "new_count": root_new})

    if progress_callback is not None:
        progress_callback(files_checked, total)

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

    # Persistent compute failures (unreachable volume, DB error) suppress
    # retries for this long so we don't hammer the failing resource on every
    # navbar poll. Short enough that natural recovery is observable; long
    # enough that ten open tabs can't hot-loop the disk.
    ERROR_BACKOFF_SECONDS = 30

    def __init__(self, ttl_seconds=300):
        self._ttl = ttl_seconds
        # key=(db_path, workspace_id) -> (result_dict, set_at_monotonic)
        self._entries = {}
        # key=(db_path, workspace_id) -> int
        self._generations = {}
        # key=(db_path, workspace_id) -> (Event, generation) for the in-flight
        # background compute. Only one compute per key runs at a time; stale-
        # generation kickoffs queue a rerun token rather than spawning a
        # parallel walk (see ``_rerun_pending``).
        self._inflight = {}
        # key=(db_path, workspace_id) -> compute_fn for a deferred rerun. Set
        # when a kickoff arrives during an in-flight stale-generation compute:
        # rather than fan out a second concurrent ``os.walk`` (a real risk on
        # the scan path where ``invalidate_workspaces`` fires per discovered
        # folder), we let the current thread finish and have it spawn one
        # follow-up using the latest compute_fn. Multiple kickoffs collapse to
        # the same slot — last writer wins, so we never queue a backlog.
        self._rerun_pending = {}
        # key=(db_path, workspace_id) -> (error_message, set_at_monotonic).
        # Recent failures suppress retries within ``ERROR_BACKOFF_SECONDS``.
        self._errors = {}
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
                # Drop any recorded failure too: the cache key has moved on
                # (folder/workspace change, completed scan), so the old error
                # no longer reflects the current state and must not gate a
                # fresh recompute via the 30s backoff window in
                # ``kickoff_compute``.
                self._errors.pop(key, None)
                self._generations[key] = self._generations.get(key, 0) + 1

    def get_recent_error(self, db_path, workspace_id):
        """Return the most recent compute error if it's still inside the
        backoff window, else None. Stale entries are cleared lazily."""
        key = (db_path, workspace_id)
        with self._lock:
            entry = self._errors.get(key)
            if entry is None:
                return None
            err_msg, set_at = entry
            if time.monotonic() - set_at > self.ERROR_BACKOFF_SECONDS:
                del self._errors[key]
                return None
            return err_msg

    def kickoff_compute(self, db_path, workspace_id, compute_fn, on_spawn=None):
        """Ensure a background compute is running for ``(db_path, workspace_id)``.

        If a recent compute failed (within ``ERROR_BACKOFF_SECONDS``), no new
        compute is started — readers should call :meth:`get_recent_error` and
        surface the failure instead of looping pending forever. Returns a
        pre-set Event so callers that ``wait`` don't block.

        Otherwise: if no compute is in flight, spawn a daemon thread that
        calls ``compute_fn()`` and stores the result in the cache. If one is
        already in flight — for the current or an older generation — the
        existing thread is reused; the caller waits on its event. When the
        in-flight thread is stale (an ``invalidate_workspaces`` ran mid-walk),
        the latest ``compute_fn`` is stashed as a deferred rerun token and
        the worker spawns one follow-up after it finishes. This keeps at most
        one walk per key in flight even when generations advance repeatedly
        (e.g. the scan path's per-folder ``invalidate_workspaces``), avoiding
        a fan-out of concurrent ``os.walk`` jobs that would thrash disk/CPU
        on large libraries.

        ``on_spawn`` is an optional callable invoked exactly once if (and
        only if) this kickoff causes a new background worker to be spawned
        — not when an in-flight worker is reused or when the error backoff
        short-circuits the call. It is called after the in-flight slot has
        been claimed but before the worker thread starts, with the spawned
        ``threading.Event`` as its only argument so the caller can block on
        it (e.g. from a separate JobRunner thread that wants to mirror the
        worker's lifecycle). If ``on_spawn`` returns a callable, that
        callable is used as a ``progress_callback(files_checked, new_found)``
        and passed to ``compute_fn`` via keyword. Use this to register a
        transparency-only job entry that streams progress while the walk
        runs.

        Returns an ``Event`` that fires when the in-flight compute finishes
        so the caller can ``Event.wait(timeout=...)`` to optionally block
        briefly for the result. After a stale-generation compute finishes,
        the deferred rerun runs asynchronously — callers re-poll to pick up
        its result, exactly as they re-poll any ``pending: true`` response.

        The generation snapshot is taken inside the lock at kickoff time so a
        concurrent invalidation can still drop the stale write — same race
        protection as :meth:`get_new_images_for_workspace`.
        """
        key = (db_path, workspace_id)
        with self._lock:
            err_entry = self._errors.get(key)
            if err_entry is not None:
                _err_msg, set_at = err_entry
                if time.monotonic() - set_at <= self.ERROR_BACKOFF_SECONDS:
                    # Suppress retry inside the backoff window.
                    done = threading.Event()
                    done.set()
                    return done
                # Backoff window elapsed — let a fresh attempt run.
                del self._errors[key]

            generation = self._generations.get(key, 0)
            existing = self._inflight.get(key)
            if existing is not None:
                existing_event, existing_generation = existing
                if existing_generation != generation:
                    # Stale-generation compute is running. Coalesce: stash the
                    # latest compute_fn so the worker spawns one follow-up
                    # when it finishes, instead of starting a parallel walk.
                    # Last writer wins — multiple kickoffs collapse to one
                    # rerun, so a burst of polls during scan-time invalidation
                    # storms can't queue a backlog of walks.
                    self._rerun_pending[key] = compute_fn
                return existing_event
            event = threading.Event()
            self._inflight[key] = (event, generation)

        # Run on_spawn outside the cache lock so user code (e.g. JobRunner
        # registration) cannot deadlock the cache on a contended lock.
        progress_cb = None
        if on_spawn is not None:
            try:
                progress_cb = on_spawn(event)
            except Exception:
                log.exception(
                    "new-images on_spawn callback raised; continuing without it"
                )
                progress_cb = None

        def worker():
            try:
                if progress_cb is not None:
                    result = compute_fn(progress_callback=progress_cb)
                else:
                    result = compute_fn()
                self.set(db_path, workspace_id, result, generation=generation)
                # Successful compute clears any prior failure so a transient
                # error doesn't keep suppressing retries after recovery.
                # Generation-guarded so a stale thread finishing after a
                # fresh one started doesn't wipe the fresh thread's error.
                with self._lock:
                    if self._generations.get(key, 0) == generation:
                        self._errors.pop(key, None)
            except Exception as e:
                log.exception(
                    "new-images background compute failed for %s ws=%s",
                    db_path, workspace_id,
                )
                with self._lock:
                    # Drop the failure if the generation moved while we were
                    # running. Mirrors the stale-write guard in :meth:`set`:
                    # if ``invalidate_workspaces`` ran mid-compute (workspace
                    # switched, scan completed), this error is for a key that
                    # has already moved on and must not force the next
                    # request into the 30s backoff window.
                    if self._generations.get(key, 0) == generation:
                        self._errors[key] = (str(e) or e.__class__.__name__,
                                             time.monotonic())
            finally:
                with self._lock:
                    # Only clear the in-flight slot if it still belongs to
                    # this thread. Defensive: under the coalescing design
                    # nothing else writes to this slot while we're alive,
                    # but the identity check keeps the invariant locally
                    # checkable rather than relying on global reasoning.
                    current = self._inflight.get(key)
                    if current is not None and current[0] is event:
                        del self._inflight[key]
                    rerun_fn = self._rerun_pending.pop(key, None)
                event.set()
                if rerun_fn is not None:
                    # A kickoff arrived during this compute while the
                    # generation was stale. Fire the deferred rerun now that
                    # the in-flight slot is free; it picks up the current
                    # generation inside the lock. Recursion depth is bounded:
                    # each rerun consumes its token and a new one is only
                    # added by another stale-generation kickoff arriving
                    # during the next worker.
                    self.kickoff_compute(db_path, workspace_id, rerun_fn)

        threading.Thread(
            target=worker, daemon=True, name="new-images-compute",
        ).start()
        return event

    def clear(self):
        with self._lock:
            self._entries.clear()
            self._generations.clear()
            self._inflight.clear()
            self._errors.clear()
            self._rerun_pending.clear()


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
