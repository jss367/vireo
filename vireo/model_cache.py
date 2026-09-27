"""Process-wide refcounted cache for loaded model objects.

Used to share a single loaded Classifier / TimmClassifier / ONNX session
across concurrent pipeline runs so two pipelines using the same model
don't double up on VRAM. When the last user releases an entry, an idle
timer arms; if no one re-acquires within ``idle_secs`` seconds the
cached value is dropped and (for GPU sessions) VRAM is freed.

Typical use:

    cache = get_default_cache()
    with cache.acquire(("model-id", labels_fp), lambda: load_classifier(...)) as clf:
        ... run inference ...

The factory is invoked at most once per (key, load-window) — concurrent
acquirers of the same key block until the first load returns and then
all receive the same object.
"""

import contextlib
import logging
import threading

log = logging.getLogger(__name__)

# Polling interval used by acquire() when a waiter passes cancel_check and is
# blocked on another caller's factory load. Short enough that a cancel takes
# effect within a fraction of a second; long enough that idle contention is
# negligible even with many concurrent waiters on the same key.
_WAITER_POLL_SECS = 0.1


def _is_caller_cancelled_error(exc):
    # These sentinels represent a producer-local stop, not a shared
    # failure of the underlying model. ``ClassificationCancelled`` fires
    # when the caller's classify job is cancelled mid-flight; the newer
    # ``ResourceWaitCancelled`` fires when the caller's bound resource
    # probe cancels a construction lease (see ``resource_ledger`` /
    # ``onnx_runtime.create_session``); ``ClassifierLoadPaused`` fires
    # when the producer's job was paused and it stepped out of the
    # factory (after checkpointing its label embeddings) so this lock is
    # not held for the whole pause. Either way, an unpaused, uncancelled
    # sibling job waiting for the same cache entry is still healthy —
    # its wait must release and retry against a fresh entry instead of
    # inheriting the producer's stop.
    return exc.__class__.__name__ in (
        "ClassificationCancelled",
        "ResourceWaitCancelled",
        "ClassifierLoadPaused",
    )


def _make_cancellation_error():
    """Build a caller-visible cancellation exception without importing at load."""
    try:
        from classifier import ClassificationCancelled
    except Exception:
        # Cancellation must still surface as a cancellation when the
        # classifier module cannot load; a local stand-in keeps the name.
        class ClassificationCancelled(RuntimeError):
            pass
    return ClassificationCancelled("classification cancelled")


class _Entry:
    __slots__ = (
        "key", "load_lock", "value", "refcount", "idle_timer", "evict_token",
        "load_error",
    )

    def __init__(self, key):
        # Current cache key. Updated by ModelCache._rekey when the factory
        # mutates on-disk state in ways that change a fingerprint baked
        # into the key (e.g. classifier self-heal redownload replacing
        # corrupt weights). _release looks the entry up by this so a
        # rekeyed entry still gets correctly identity-matched.
        self.key = key
        # Held while a factory is running so concurrent acquirers wait for the
        # first load to complete instead of all loading in parallel.
        self.load_lock = threading.Lock()
        self.value = None
        self.refcount = 0
        self.idle_timer = None
        # Fresh sentinel installed each time _release arms an idle timer.
        # _evict compares the token it was scheduled with against the
        # entry's current token; a stale timer whose callback fires after
        # acquire+release cycled a new timer in finds a mismatch and bails.
        self.evict_token = None
        self.load_error = None


class _Handle:
    """Context manager returned by ModelCache.acquire().

    ``__enter__`` returns the loaded value; ``__exit__`` (or ``release()``)
    decrements the refcount exactly once, even if called multiple times.
    """

    def __init__(self, cache, entry, value):
        self._cache = cache
        self._entry = entry
        self._value = value
        self._released = False

    @property
    def value(self):
        """The loaded value, without entering the context manager.

        Lets callers that wrap ``acquire`` post-process the instance
        (e.g. ``acquire_cached_classifier`` firing a classifier's
        ``notify_reuse`` hook) without reaching into ``_value``.
        """
        return self._value

    def __enter__(self):
        return self._value

    def __exit__(self, exc_type, exc, tb):
        self.release()

    def __del__(self):
        # Safety net for legacy/script callers that unwind before their normal
        # explicit release. ``release`` is idempotent and does only in-memory
        # bookkeeping plus timer creation.
        with contextlib.suppress(Exception):
            self.release()

    def release(self):
        if self._released:
            return
        self._released = True
        self._cache._release_entry(self._entry)


class ModelCache:
    """Refcounted cache with idle-timer eviction.

    Thread-safe. The internal ``_global_lock`` is held only for short
    bookkeeping operations (entry creation, refcount mutation, timer
    management). Slow factory loads run under a per-entry ``load_lock``
    so other cache keys aren't blocked.
    """

    def __init__(self, idle_secs=300.0):
        self._global_lock = threading.Lock()
        self._entries = {}
        self._idle_secs = idle_secs

    def acquire(self, key, factory, post_load_key=None, cancel_check=None):
        """Acquire a refcounted handle to the value for ``key``.

        On cache miss, ``factory()`` is called to produce the value. On a
        concurrent acquire of the same missing key, only one thread calls
        the factory; the rest wait and receive the same value.

        ``post_load_key`` is an optional callable ``(value) -> key`` invoked
        once after the factory returns. If the returned key differs from
        the original, the entry is atomically rekeyed so the next acquirer
        looks it up under the corrected key. Use this when the factory
        mutates on-disk state in ways that change a fingerprint baked into
        the key (e.g. ONNX self-heal redownload replaces corrupt weights
        and the post-load file stats no longer match the pre-load ones).

        ``cancel_check`` is an optional zero-arg callable returning ``True``
        when this caller wants to abandon its wait. When another caller is
        already producing the value, ``acquire`` polls the load lock with a
        short timeout so a cancel here takes effect within a fraction of a
        second instead of blocking for the full duration of the shared
        factory (which for BioCLIP embedding computation can be minutes and
        would otherwise also stall ``JobRunner.shutdown``). The shared
        producer keeps running for other waiters; only this waiter unwinds.
        """
        while True:
            with self._global_lock:
                entry = self._entries.get(key)
                if entry is None:
                    entry = _Entry(key)
                    self._entries[key] = entry
                entry.refcount += 1
                if entry.idle_timer is not None:
                    entry.idle_timer.cancel()
                    entry.idle_timer = None
                # Invalidate any in-flight stale callback whose cancel() lost
                # the race with the timer thread firing.
                entry.evict_token = None

            if entry.load_lock.acquire(blocking=False):
                # Uncontended fast path: nobody else is producing this
                # entry, so we own the load_lock immediately without
                # touching the resource ledger. Uncontended acquires must
                # not count as waits — track_external_wait() records both
                # duration and count, and per-photo detection/masking
                # runs would inflate wait_count with zero-duration entries
                # on every cache hit or first-caller miss.
                pass
            else:
                # Contended: another caller is already producing this
                # entry's value under load_lock. That wait can be minutes
                # long for a cold BioCLIP / SAM2 / DINOv2 load — and if
                # the producer is itself blocked on the ONNX construction
                # lease, this waiter is resource-blocked without ever
                # reaching ResourceLedger.acquire(). Expose it as an
                # external wait so it feeds the same per-job live and
                # persisted resource-wait diagnostics as the
                # session-cache locks in onnx_runtime.
                from resource_ledger import get_resource_ledger

                with get_resource_ledger().track_external_wait():
                    if cancel_check is not None:
                        # Waiter-local cancellation: poll the load_lock so
                        # this caller can abandon its wait while another
                        # caller's factory is still running. The other
                        # caller is unaffected and their factory continues.
                        while not entry.load_lock.acquire(
                            timeout=_WAITER_POLL_SECS,
                        ):
                            if cancel_check():
                                self._release_entry(entry)
                                raise _make_cancellation_error()
                    else:
                        entry.load_lock.acquire()

            try:
                if entry.value is None and entry.load_error is None:
                    # ``factory()`` typically calls ``create_session`` which
                    # waits for the construction CPU lease under the bound
                    # resource cancel probe. That probe parks on pause via
                    # ``_pause_checkpoint``; the parking would happen while
                    # this thread still owns ``entry.load_lock``, and any
                    # unpaused peer waiting for the same cache key would
                    # then block for the whole pause window (Codex
                    # discussion_r3790941020 — sibling of the
                    # ``acquire_session_cache_lock`` fix in ``e3693467``).
                    # Rebind the resource cancel probe to the pure-cancel
                    # probe (never parks on pause) for the factory call,
                    # matching the pattern the session-cache lock's
                    # ``_post_acquire_recheck`` uses. Cancel still fires;
                    # pause is observed at the next outer
                    # ``_pause_checkpoint`` after ``load_lock`` is released.
                    from resource_ledger import (
                        bind_resource_cancel_check,
                        resolve_resource_pure_cancel_check,
                    )

                    pure_cancel_check = resolve_resource_pure_cancel_check()
                    factory_ctx = (
                        bind_resource_cancel_check(pure_cancel_check)
                        if pure_cancel_check is not None
                        else contextlib.nullcontext()
                    )
                    try:
                        with factory_ctx:
                            value = factory()
                    except BaseException as e:
                        # Surface to this caller and any waiters under the
                        # load_lock; remove the entry so future acquires retry.
                        entry.load_error = e
                        self._abandon(key, entry)
                        raise
                    entry.value = value
                    if post_load_key is not None:
                        try:
                            actual_key = post_load_key(value)
                        except Exception:
                            log.exception(
                                "ModelCache: post_load_key callback raised; "
                                "leaving entry keyed by the pre-load key"
                            )
                            actual_key = key
                        if actual_key != key:
                            self._rekey(key, actual_key, entry)
                elif entry.load_error is not None:
                    # A prior acquirer's factory raised; this acquirer should
                    # also see shared load failures and not get a phantom value.
                    # Caller-local cancellation is different: this waiter may
                    # still be healthy, so release the abandoned entry and
                    # retry against a fresh cache entry.
                    load_error = entry.load_error
                    self._release_entry(entry)
                    if _is_caller_cancelled_error(load_error):
                        continue
                    raise load_error
                return _Handle(self, entry, entry.value)
            finally:
                entry.load_lock.release()

    def _release_entry(self, entry):
        """Decrement refcount on the specific entry the caller acquired.

        Identity-checked: if the entry currently under ``entry.key`` is no
        longer this entry (e.g. abandoned after a failed load, then
        replaced by a later acquirer), this is a no-op. The orphan entry's
        refcount is meaningless because nothing can find it anymore, and a
        new entry under the same key must not be touched by a caller that
        never incremented it. Uses ``entry.key`` so a rekeyed entry is
        still correctly located.
        """
        timer = None
        with self._global_lock:
            lookup_key = entry.key
            current = self._entries.get(lookup_key)
            if current is not entry:
                return
            if entry.refcount > 0:
                entry.refcount -= 1
            if entry.refcount == 0 and entry.value is not None:
                # Arm idle eviction. Use a daemon Timer so process exit isn't
                # blocked by a pending eviction window. The fresh token lets
                # _evict tell a stale timer (whose callback raced past
                # cancel()) apart from a live one — only the live timer's
                # token will still match entry.evict_token when _evict runs.
                token = object()
                entry.evict_token = token
                timer = threading.Timer(
                    self._idle_secs, self._evict, args=(entry, token),
                )
                timer.daemon = True
                entry.idle_timer = timer
        if timer is not None:
            timer.start()

    def _evict(self, entry, token):
        with self._global_lock:
            lookup_key = entry.key
            current = self._entries.get(lookup_key)
            if current is not entry:
                return
            if entry.refcount > 0:
                # Re-acquired in the gap between timer fire and lock; abort.
                return
            if entry.evict_token is not token:
                # Stale timer: an acquire+release cycle replaced our token
                # with a fresh one while our callback was queued. The new
                # timer (or none, if still acquired) owns the eviction
                # decision now.
                return
            # Drop the entry; entry.value will be garbage-collected (and any
            # __del__ on the model object — ONNX session close — runs then).
            del self._entries[lookup_key]
            log.debug("ModelCache: evicted %r after idle window", lookup_key)

    def _abandon(self, key, entry):
        """Remove a failed-load entry. Called with no locks held."""
        with self._global_lock:
            current = self._entries.get(key)
            if current is entry:
                # Only remove if no later acquire replaced this entry.
                del self._entries[key]

    def _rekey(self, old_key, new_key, entry):
        """Move ``entry`` from ``old_key`` to ``new_key`` atomically.

        Called from inside ``acquire`` while still holding ``entry.load_lock``
        so concurrent acquirers on ``old_key`` join this entry (they
        already incremented its refcount and are blocked on load_lock).
        Best-effort: if a separate entry has already been created under
        ``new_key`` (a racing acquirer computed the post-load key first),
        keep this entry where it is. The duplicate will idle-evict; we
        accept a brief VRAM doubling over corrupting a sibling entry's
        refcount.
        """
        with self._global_lock:
            current = self._entries.get(old_key)
            if current is not entry:
                # Already moved or abandoned.
                return
            if new_key in self._entries:
                log.warning(
                    "ModelCache: cannot rekey %r -> %r (target exists); "
                    "leaving entry under original key. Next acquire on the "
                    "post-load key will use the existing target entry.",
                    old_key, new_key,
                )
                return
            del self._entries[old_key]
            self._entries[new_key] = entry
            entry.key = new_key

    # Test hook: exposed deliberately so tests can assert eviction without
    # snooping on _entries directly. Not part of the public API.
    def _has_entry(self, key):
        with self._global_lock:
            return key in self._entries


_default = None
_default_lock = threading.Lock()


def get_default_cache(idle_secs=300.0):
    """Return the process-wide default ModelCache, creating it on first call."""
    global _default
    with _default_lock:
        if _default is None:
            _default = ModelCache(idle_secs=idle_secs)
        return _default


def reset_default_cache_for_tests():
    """Drop the default cache. Tests use this to isolate from prior runs."""
    global _default
    with _default_lock:
        _default = None
