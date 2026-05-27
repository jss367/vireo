"""Process-wide locks coordinating concurrent pipeline runs.

Two primitives:

* ``acquire_gpu()`` — a single-holder semaphore that every GPU-using stage
  (classify, detect, extract_masks, eye_keypoints) wraps around its
  per-batch inference call. Must be released between batches so a long
  classify doesn't completely starve a second pipeline that just wants
  one quick GPU op.

* ``acquire_workspace_regroup(workspace_id)`` — a per-workspace lock so
  two pipelines targeting the same workspace serialise on regroup but
  not on earlier stages.

Lock order (acquire outermost first): ``_progress_lock`` →
``JobRunner._lock`` → ``acquire_workspace_regroup`` → ``acquire_gpu``.
Never invert. The GPU lock is the innermost; nothing else may be acquired
while it is held.
"""

import threading

# Single GPU operation at a time across the whole process. Size 1 by
# design — see docs/plans/2026-05-26-pipeline-concurrency-design.md
# "Concurrency model" for the rationale.
_GPU_SEMAPHORE = threading.Semaphore(1)


def acquire_gpu():
    """Context manager for the process-wide GPU semaphore.

    Use around a single batch of inference, not a whole stage::

        for batch in batches:
            with acquire_gpu():
                results = model.run(batch)
            process(results)
    """
    return _GpuLockContext()


class _GpuLockContext:
    def __enter__(self):
        _GPU_SEMAPHORE.acquire()
        return self

    def __exit__(self, exc_type, exc, tb):
        _GPU_SEMAPHORE.release()


# Per-workspace regroup locks. Created lazily on first request. Entries
# are never removed — workspace IDs are stable integers and the lock
# objects are tiny, so accumulating one per workspace the user has ever
# regrouped against is harmless.
_REGROUP_LOCKS: dict = {}
_REGROUP_LOCKS_GUARD = threading.Lock()


def acquire_workspace_regroup(workspace_id):
    """Context manager for the regroup lock keyed by ``workspace_id``.

    Two pipelines targeting the same workspace serialise here; pipelines
    targeting different workspaces don't interact.
    """
    if workspace_id is None:
        # Treat unspecified workspace as a single shared lock. In practice
        # callers always pass a real id; this branch only protects against
        # latent bugs that would silently make the lock global.
        workspace_id = "__unspecified__"
    with _REGROUP_LOCKS_GUARD:
        lock = _REGROUP_LOCKS.get(workspace_id)
        if lock is None:
            lock = threading.Lock()
            _REGROUP_LOCKS[workspace_id] = lock
    return lock


# Test hook so unit tests can assert lock identity without snooping on
# the module-private dict directly.
def _workspace_regroup_lock_for_tests(workspace_id):
    return acquire_workspace_regroup(workspace_id)
