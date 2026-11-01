"""The process-wide cache of new-images results, per catalog and workspace.

``new_images`` computes the results (walking the workspace's folders);
this module only holds them, so the ``Database`` façade can invalidate
entries when folders or workspaces change without importing the walker.
"""

import logging
import threading
import time

log = logging.getLogger(__name__)


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
    # Results that skipped an offline root live only this long, matching the
    # ``VolumeReachability`` offline TTL so a reconnected share is re-walked
    # on the same schedule it is re-probed.
    PARTIAL_RESULT_TTL_SECONDS = 30

    # Full-library walks can take minutes on network storage. Automatic
    # refreshes share the missing-originals check's half-hour cadence;
    # imports/scans and explicit rechecks still invalidate immediately.
    def __init__(self, ttl_seconds=1800):
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
        # key=(db_path, workspace_id) -> (compute_fn, on_spawn, can_start) for a rerun. Set
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
            ttl = self._ttl
            if result.get("unreachable_roots"):
                # A walk that skipped an offline volume is a *partial* answer.
                # Holding it for the full TTL would keep reporting the folder
                # offline (and hide its new files) for minutes after the share
                # reconnects, so it expires at the reachability gate's own
                # offline retry cadence instead.
                ttl = min(ttl, self.PARTIAL_RESULT_TTL_SECONDS)
            if time.monotonic() - set_at > ttl:
                del self._entries[key]
                return None
            return result

    def has_inflight(self, db_path, workspace_id):
        """Return True if a background compute is currently running for
        ``(db_path, workspace_id)``. Callers use this to coalesce onto an
        existing walk instead of tearing it down with a fresh invalidation.
        """
        key = (db_path, workspace_id)
        with self._lock:
            return key in self._inflight

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

    def kickoff_compute(self, db_path, workspace_id, compute_fn, on_spawn=None, can_start=None):
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
        protection as :func:`new_images.get_new_images_for_workspace`.

        ``can_start`` optionally defers automatic work while foreground jobs
        are active. It is rechecked for deferred reruns, which retain their
        ``on_spawn`` registration and progress callback.
        """
        # Automatic discovery yields to foreground work, including reruns
        # requested while an earlier generation was still walking. Polling
        # will retry once the foreground job finishes.
        if can_start is not None and not can_start():
            done = threading.Event()
            done.set()
            return done
        key = (db_path, workspace_id)
        event, generation = self._claim_compute_slot(
            key, compute_fn, on_spawn, can_start,
        )
        if generation is None:
            return event

        # Run on_spawn outside the cache lock so user code (e.g. JobRunner
        # registration) cannot deadlock the cache on a contended lock.
        progress_cb = None
        if on_spawn is not None:
            try:
                progress_cb = on_spawn(event)
            except Exception:  # broad: a caller's callback must not stop the compute
                log.exception(
                    "new-images on_spawn callback raised; continuing without it"
                )
                progress_cb = None

        def worker():
            self._run_compute(
                key, db_path, workspace_id, compute_fn, progress_cb,
                event, generation,
            )

        threading.Thread(
            target=worker, daemon=True, name="new-images-compute",
        ).start()
        return event

    def _claim_compute_slot(self, key, compute_fn, on_spawn, can_start):
        """Claim the in-flight slot for ``key`` under the cache lock.

        Returns ``(event, generation)`` for a new worker to run, or
        ``(event, None)`` when no worker should start: a fresh entry is
        cached, a recent failure is backing off, or a compute is already in
        flight (stale ones get a rerun token).
        """
        with self._lock:
            # Race guard: a worker finishing between the caller's
            # ``cache.get()`` (which returned None) and this call would
            # write the result to ``_entries`` and then clear
            # ``_inflight`` — under separate lock acquires — so by the
            # time we arrive both are visible but the ``_inflight`` slot
            # is gone. Without this check we would spawn a redundant walk
            # that duplicates the just-finished one. If a fresh entry is
            # already cached, return an already-set event so the caller's
            # follow-up ``cache.get()`` picks it up.
            entry = self._entries.get(key)
            if entry is not None:
                _result, set_at = entry
                if time.monotonic() - set_at <= self._ttl:
                    done = threading.Event()
                    done.set()
                    return done, None

            err_entry = self._errors.get(key)
            if err_entry is not None:
                _err_msg, set_at = err_entry
                if time.monotonic() - set_at <= self.ERROR_BACKOFF_SECONDS:
                    # Suppress retry inside the backoff window.
                    done = threading.Event()
                    done.set()
                    return done, None
                # Backoff window elapsed — let a fresh attempt run.
                del self._errors[key]

            # Skip the spawn if a fresh, complete walk already sits in the
            # cache. Without this, a caller that saw an empty cache moments
            # ago can race the in-flight worker's finally block: worker
            # populates the cache, then clears its in-flight slot; the
            # caller then arrives here and sees no in-flight, so we'd fan
            # out a second walk over the same directories. All async walks
            # run with sample_limit=None (so sample_complete=True), so
            # gating on that flag is a precise "a completed walk exists"
            # check — sync ``get_new_images_for_workspace`` writes at
            # sample_limit=5 (sample_complete potentially False) and must
            # still fall through to spawn a real walk.
            entry = self._entries.get(key)
            if entry is not None:
                cached_result, set_at = entry
                if (time.monotonic() - set_at <= self._ttl
                        and cached_result.get("sample_complete")):
                    done = threading.Event()
                    done.set()
                    return done, None

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
                    self._rerun_pending[key] = (compute_fn, on_spawn, can_start)
                return existing_event, None
            event = threading.Event()
            self._inflight[key] = (event, generation)
        return event, generation

    def _run_compute(self, key, db_path, workspace_id, compute_fn, progress_cb,
                     event, generation):
        """Body of a background compute worker: store the result or error,
        free the in-flight slot, then fire any deferred rerun."""
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
        except Exception as e:  # broad: background compute boundary; the error is stored and shown to the next request
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
                compute_again, spawn_again, start_again = rerun_fn
                self.kickoff_compute(
                    db_path, workspace_id, compute_again,
                    on_spawn=spawn_again, can_start=start_again,
                )

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
