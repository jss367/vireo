"""Process-wide locks coordinating concurrent pipeline runs.

Two primitives:

* ``acquire_gpu()`` — a single-holder semaphore that every GPU-using stage
  (classify, detect, extract_masks, eye_keypoints) wraps around its
  per-batch inference call. Must be released between batches so a long
  classify doesn't completely starve a second pipeline that just wants
  one quick GPU op.

* ``acquire_workspace_regroup(workspace_id)`` — a per-workspace lock
  held across BOTH ``regroup_stage`` and ``miss_stage`` so two pipelines
  targeting the same workspace can't interleave on the workspace-scoped
  grouping state (``burst_id`` writes, ``pipeline_results_ws*.json``,
  and the ``miss_computed_at`` timestamp paired with that grouping).
  Pipelines on different workspaces never contend.

Lock order (acquire outermost first): ``_progress_lock`` →
``acquire_workspace_regroup`` → ``JobRunner._lock`` → ``acquire_gpu``.
``JobRunner._lock`` is a brief leaf lock taken by ``runner.update_step``
inside the workspace critical section; this is safe because no code
path under ``JobRunner._lock`` acquires ``acquire_workspace_regroup``,
so there is no cycle. ``acquire_gpu`` is the innermost: nothing else
may be acquired while it is held.
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


# Providers that actually run on a GPU device. ONNXRuntime's other
# providers (CPUExecutionProvider, and anything not listed) execute on
# the CPU, so taking the GPU semaphore for them would needlessly block
# other pipelines' real GPU work.
_GPU_PROVIDERS = ("CUDAExecutionProvider", "CoreMLExecutionProvider")


def _session_uses_gpu(session):
    """Return True if ``session`` is actually executing on a GPU provider.

    ``InferenceSession.get_providers()`` returns the providers ONNX
    Runtime decided to use after construction, so this reflects reality
    even when CoreML was requested but excluded (e.g. for external-data
    models). Falls back to ``True`` if the session doesn't expose
    ``get_providers`` — conservative default that matches the unconditional-
    lock behavior we had before this check existed.
    """
    try:
        providers = session.get_providers()
    except Exception:
        return True
    return any(p in _GPU_PROVIDERS for p in providers)


def acquire_gpu_if_session_uses_it(session):
    """Context manager: take the GPU semaphore only for GPU-running sessions.

    CPU-only ONNX sessions (Apple Silicon with external-data models,
    CPU-only installs) skip the lock so they don't block concurrent
    pipelines' real GPU work. GPU-running sessions still serialise.

    Usage::

        with acquire_gpu_if_session_uses_it(session):
            outputs = session.run(None, feeds)
    """
    if _session_uses_gpu(session):
        return _GpuLockContext()
    return _NoOpContext()


class _NoOpContext:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


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


# Per-photo locks for mask extraction. Two concurrent pipelines whose
# collections overlap can both reach extract_masks_stage with the same
# photo. The conflict has two distinct sources, and the lock has to
# cover BOTH:
#
#   1. The deterministic ``masks/{photo_id}.{variant}.png`` file path
#      (per-variant collision). Two pipelines with the SAME variant
#      would corrupt each other's PNG bytes.
#
#   2. The denormalised writes to the ``photos`` row —
#      ``set_active_mask_variant`` (mask_path, crop_complete,
#      subject_tenengrad, bg_tenengrad) and ``update_photo_embeddings``
#      (dino_subject_embedding, dino_global_embedding) — happen
#      regardless of variant. Two pipelines processing the same photo
#      with DIFFERENT variants can still interleave these writes,
#      leaving photos.active_mask_variant pointing at one variant
#      while photos.dino_subject_embedding was cropped from the
#      other's mask.
#
# Because (2) crosses variants, the key is ``photo_id`` only.
# Concurrency loss: two pipelines on the same photo with different
# SAM variants now serialise on the whole extract-masks body. This is
# rare in practice (it requires two workspaces sharing folders AND
# configured with different SAM variants), and the alternative —
# splitting into a per-variant lock for the mask file + a per-photo
# lock for the row writes — would invert the lock order (a worker
# holding the inner lock then trying to take the outer would deadlock).
_PHOTO_MASK_LOCKS: dict = {}
_PHOTO_MASK_LOCKS_GUARD = threading.Lock()


def acquire_photo_mask(photo_id):
    """Context manager for the per-photo mask-write lock.

    Held across the get_photo_mask → generate_mask → save_mask →
    upsert_photo_mask → set_active_mask_variant → update_photo_embeddings
    sequence in ``extract_masks_stage`` so two pipelines hitting the
    same photo serialise. Pipelines on different photos don't contend.
    """
    with _PHOTO_MASK_LOCKS_GUARD:
        lock = _PHOTO_MASK_LOCKS.get(photo_id)
        if lock is None:
            lock = threading.Lock()
            _PHOTO_MASK_LOCKS[photo_id] = lock
    return lock


def _photo_mask_lock_for_tests(photo_id):
    return acquire_photo_mask(photo_id)
