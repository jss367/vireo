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

import logging
import threading

log = logging.getLogger(__name__)


class _Entry:
    __slots__ = ("load_lock", "value", "refcount", "idle_timer", "load_error")

    def __init__(self):
        # Held while a factory is running so concurrent acquirers wait for the
        # first load to complete instead of all loading in parallel.
        self.load_lock = threading.Lock()
        self.value = None
        self.refcount = 0
        self.idle_timer = None
        self.load_error = None


class _Handle:
    """Context manager returned by ModelCache.acquire().

    ``__enter__`` returns the loaded value; ``__exit__`` (or ``release()``)
    decrements the refcount exactly once, even if called multiple times.
    """

    def __init__(self, cache, key, entry, value):
        self._cache = cache
        self._key = key
        self._entry = entry
        self._value = value
        self._released = False

    def __enter__(self):
        return self._value

    def __exit__(self, exc_type, exc, tb):
        self.release()

    def release(self):
        if self._released:
            return
        self._released = True
        self._cache._release(self._key, self._entry)


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

    def acquire(self, key, factory):
        """Acquire a refcounted handle to the value for ``key``.

        On cache miss, ``factory()`` is called to produce the value. On a
        concurrent acquire of the same missing key, only one thread calls
        the factory; the rest wait and receive the same value.
        """
        with self._global_lock:
            entry = self._entries.get(key)
            if entry is None:
                entry = _Entry()
                self._entries[key] = entry
            entry.refcount += 1
            if entry.idle_timer is not None:
                entry.idle_timer.cancel()
                entry.idle_timer = None

        with entry.load_lock:
            if entry.value is None and entry.load_error is None:
                try:
                    value = factory()
                except BaseException as e:
                    # Surface to this caller and any waiters under the
                    # load_lock; remove the entry so future acquires retry.
                    entry.load_error = e
                    self._abandon(key, entry)
                    raise
                entry.value = value
            elif entry.load_error is not None:
                # A prior acquirer's factory raised; this acquirer should
                # also see the failure and not get a phantom value. Release
                # the original entry by identity — between abandon and now,
                # another acquirer may have created a fresh entry under the
                # same key, and decrementing it here would corrupt its
                # refcount and cause spurious eviction of a model still in
                # use.
                self._release(key, entry)
                raise entry.load_error
            return _Handle(self, key, entry, entry.value)

    def _release(self, key, entry):
        """Decrement refcount on the specific entry the caller acquired.

        Identity-checked: if the entry under ``key`` has been replaced
        (e.g. the original was abandoned after a failed load, and a later
        acquirer installed a fresh entry), this is a no-op. The orphan
        entry's refcount is meaningless because nothing can find it
        anymore, and the new entry must not be touched by a caller that
        never incremented it.
        """
        timer = None
        with self._global_lock:
            current = self._entries.get(key)
            if current is not entry:
                return
            if entry.refcount > 0:
                entry.refcount -= 1
            if entry.refcount == 0 and entry.value is not None:
                # Arm idle eviction. Use a daemon Timer so process exit isn't
                # blocked by a pending eviction window.
                timer = threading.Timer(self._idle_secs, self._evict, args=(key,))
                timer.daemon = True
                entry.idle_timer = timer
        if timer is not None:
            timer.start()

    def _evict(self, key):
        with self._global_lock:
            entry = self._entries.get(key)
            if entry is None:
                return
            if entry.refcount > 0:
                # Re-acquired in the gap between timer fire and lock; abort.
                return
            # Drop the entry; entry.value will be garbage-collected (and any
            # __del__ on the model object — ONNX session close — runs then).
            del self._entries[key]
            log.debug("ModelCache: evicted %r after idle window", key)

    def _abandon(self, key, entry):
        """Remove a failed-load entry. Called with no locks held."""
        with self._global_lock:
            current = self._entries.get(key)
            if current is entry:
                # Only remove if no later acquire replaced this entry.
                del self._entries[key]

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
