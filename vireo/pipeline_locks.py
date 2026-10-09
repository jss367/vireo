"""Process-wide locks coordinating concurrent pipeline runs.

Two primitives:

* ``acquire_inference_resources()`` — a provider-aware inference lease.
  Pure-GPU sessions (no ``CPUExecutionProvider`` registered) take only
  the single-holder accelerator semaphore. CPU-only sessions atomically
  claim their configured CPU permits plus the exclusive ``cpu_ml`` lane.
  Mixed accelerator+CPU sessions (the default Vireo shape — CUDA / CoreML
  registered alongside ``CPUExecutionProvider`` for unsupported-op
  fallback) claim BOTH: ledger-first (CPU permits + ``cpu_ml``), then
  the GPU semaphore. Without the CPU claim on the mixed path, an
  op-level CPU fallback would run the session's native ONNX pool
  alongside a concurrent CPU inference / scan / regroup and overrun
  the process-wide CPU budget.

* ``acquire_workspace_regroup(workspace_id)`` — a per-workspace lock
  held across BOTH ``regroup_stage`` and ``miss_stage`` so two pipelines
  targeting the same workspace can't interleave on the workspace-scoped
  grouping state (``burst_id`` writes, ``pipeline_results_ws*.json``,
  and the ``miss_computed_at`` timestamp paired with that grouping).
  Pipelines on different workspaces never contend.

Lock order (acquire outermost first): ``_progress_lock`` →
``acquire_workspace_regroup`` → ``JobRunner._lock`` → inference lease.
``JobRunner._lock`` is a brief leaf lock taken by ``runner.update_step``
inside the workspace critical section; this is safe because no code
path under ``JobRunner._lock`` acquires ``acquire_workspace_regroup``,
so there is no cycle. Resource-ledger accounting is completed before a
lease is returned; its mutex is never held during inference. The accelerator
semaphore remains innermost: nothing else may be acquired while it is held.
"""

import logging
import threading

log = logging.getLogger(__name__)

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
    def __init__(self, cancel_check=None):
        self._cancel_check = cancel_check

    def __enter__(self):
        from resource_ledger import (
            ResourceWaitCancelled,
            get_resource_ledger,
            resolve_resource_cancel_check,
        )

        cancel_check = resolve_resource_cancel_check(self._cancel_check)
        if cancel_check is not None and cancel_check():
            raise ResourceWaitCancelled(
                "Cancelled while waiting for GPU inference resources"
            )
        if _GPU_SEMAPHORE.acquire(blocking=False):
            return self

        # Accelerator coordination intentionally remains a semaphore rather
        # than a ledger lane, but contention must still feed the same live and
        # persisted per-job resource wait diagnostics as CPU claims.
        with get_resource_ledger().track_external_wait():
            while True:
                if cancel_check is not None and cancel_check():
                    raise ResourceWaitCancelled(
                        "Cancelled while waiting for GPU inference resources"
                    )
                if _GPU_SEMAPHORE.acquire(timeout=0.2):
                    if cancel_check is None:
                        return self
                    # ``cancel_check`` may PARK — the pipeline bound
                    # probe is ``pipeline_job._pause_checkpoint``, which
                    # blocks until Resume when Pause is pending. Release
                    # the semaphore BEFORE that call so the paused
                    # participant does not retain the process-wide GPU
                    # slot for the duration of the pause and block
                    # unrelated unpaused GPU jobs.
                    #
                    # On probe = True (cancel, or cancel-through-pause):
                    #   raise; semaphore is already released.
                    # On probe = False (no cancel, or paused-then-
                    #   resumed): commit ownership via a non-blocking
                    #   reacquire. If it succeeds we own the slot
                    #   honestly. If another waiter grabbed the slot
                    #   while we were parked, fall through to another
                    #   polling iteration.
                    _GPU_SEMAPHORE.release()
                    if cancel_check():
                        raise ResourceWaitCancelled(
                            "Cancelled while waiting for GPU inference "
                            "resources",
                        )
                    if _GPU_SEMAPHORE.acquire(blocking=False):
                        return self
                    continue

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
        log.debug("Session providers unreadable; assuming GPU", exc_info=True)
        return True
    return any(p in _GPU_PROVIDERS for p in providers)


class _CompoundInferenceContext:
    """Compound lease: CPU permits + ``cpu_ml`` lane + GPU semaphore.

    For mixed-provider accelerator sessions (CUDA / CoreML alongside
    ``CPUExecutionProvider`` — Vireo's default shape from
    :func:`onnx_runtime.get_providers`), ONNX Runtime may execute some
    ops on the accelerator and fall back to the session's own CPU
    thread pool for unsupported ops. ``get_providers()`` reports the
    registered provider list, not actual node placement, so we cannot
    tell at claim time whether any given call will fall back — the
    safe default is to claim BOTH resources.

    Without the CPU claim, the fallback CPU pool would run alongside
    a concurrent CPU inference / scan / regroup and blow through the
    process-wide CPU budget: the whole point of the ledger is to make
    that impossible.

    Acquires in a release-and-retry loop rather than a static
    ledger-first / semaphore-last order:

    1. Take the CPU lease (may park on Pause; we hold no other
       resource here so that is fine).
    2. Try a non-blocking acquire of the GPU semaphore. If it
       succeeds, both are held atomically.
    3. If the GPU semaphore is contended: RELEASE the CPU lease,
       wait for the semaphore with parking allowed, release it
       immediately, then loop back to step 1.

    Step 3 is what stops this compound context from re-introducing
    the exact pause-park bug ``_GpuLockContext.__enter__`` was fixed
    for: if we held the CPU permits + ``cpu_ml`` lane through a
    parking GPU wait, an unpaused CPU inference or scan would be
    blocked for the whole pause even though this participant is
    doing no work. Releasing the CPU lease around any wait that can
    park keeps unpaused peers unblocked.

    Preserves the "GPU semaphore is innermost" invariant documented
    at the top of this module: on success, the CPU lease is held
    first, then the GPU semaphore is acquired without any additional
    wait. ``__exit__`` releases in reverse (GPU first, then CPU).
    """

    def __init__(self, ledger, request, cancel_check):
        self._ledger = ledger
        self._request = request
        self._cancel_check = cancel_check
        self._cpu_lease = None
        self._gpu_held = False

    def __enter__(self):
        while True:
            cpu_lease = self._ledger.acquire(
                self._request, cancel_check=self._cancel_check,
            )
            cpu_lease.__enter__()
            # Non-blocking GPU acquire. On success both are held and
            # we return without ever having parked while holding both.
            if _GPU_SEMAPHORE.acquire(blocking=False):
                self._cpu_lease = cpu_lease
                self._gpu_held = True
                return self
            # GPU is contended. Release the CPU lease so any unpaused
            # CPU inference / scan is not blocked while this
            # participant waits (and possibly parks) for the GPU.
            cpu_lease.__exit__(None, None, None)
            # Wait for the GPU semaphore via ``_GpuLockContext`` so
            # this wait feeds the same live/persisted per-job
            # diagnostics and honours the pause-park release semantics
            # already built into it. Release immediately; the next
            # loop iteration atomically re-acquires CPU + GPU.
            with _GpuLockContext(self._cancel_check):
                pass

    def __exit__(self, exc_type, exc, tb):
        try:
            if self._gpu_held:
                _GPU_SEMAPHORE.release()
                self._gpu_held = False
        finally:
            if self._cpu_lease is not None:
                self._cpu_lease.__exit__(exc_type, exc, tb)
                self._cpu_lease = None


def _build_cpu_request(session, label):
    """Construct the ``ResourceRequest`` for this session's CPU claim.

    Shared between the CPU-only path and the mixed accelerator+CPU
    fallback path — same permits, same ``cpu_ml`` lane; only the label
    differs so wait diagnostics distinguish the two.
    """
    from onnx_runtime import session_cpu_threads
    from resource_ledger import (
        CpuRequest,
        ResourceRequest,
        cpu_inference_request,
        get_resource_ledger,
    )

    ledger = get_resource_ledger()
    threads = session_cpu_threads(
        session, default=cpu_inference_request(ledger.cpu_capacity).preferred,
    )
    threads = max(1, min(int(threads), ledger.cpu_capacity))
    return ledger, ResourceRequest(
        cpu=CpuRequest(threads, threads, threads),
        lanes=("cpu_ml",),
        label=label,
    )


def acquire_inference_resources(session, *, cancel_check=None):
    """Return the enforceable inference lease for an ONNX session.

    CPU-only sessions claim the exact CPU thread count configured when the
    session was constructed and the initial exclusive ``cpu_ml`` lane.
    Mixed-provider accelerator sessions (CUDA/CoreML + CPU fallback —
    Vireo's default) claim BOTH the CPU budget AND the GPU semaphore so
    unsupported-op fallback to the session's native CPU pool cannot
    overrun the process-wide CPU budget. Pure-accelerator sessions
    (no CPUExecutionProvider registered) take only the GPU semaphore.
    Unknown providers take the conservative accelerator+CPU path.

    ``cancel_check`` is threaded into the CPU claim so a cancelled
    classify, detection, mask, or embedding worker wakes promptly while
    another inference call owns the ``cpu_ml`` lane or the required
    permits — the same guarantee scanner hashing already gets. When
    omitted, the ledger falls back to whatever probe the current job
    established via :func:`resource_ledger.bind_resource_cancel_check`
    at its top level.

    Usage::

        with acquire_inference_resources(session):
            outputs = session.run(None, feeds)
    """
    if _session_uses_gpu(session):
        # Determine provider mix. On any failure treat as compound to
        # match the conservative "unknown providers" branch below.
        try:
            providers = set(session.get_providers())
        except Exception:
            log.debug("Session providers unreadable; treating as compound", exc_info=True)
            providers = None
        if providers is None or "CPUExecutionProvider" in providers:
            ledger, request = _build_cpu_request(
                session, label="mixed accelerator+CPU ONNX inference",
            )
            return _CompoundInferenceContext(ledger, request, cancel_check)
        return _GpuLockContext(cancel_check)
    try:
        providers = set(session.get_providers())
    except Exception:
        log.debug("Session providers unreadable; treating as compound", exc_info=True)
        # Unknown provider surface: fall through to the same
        # conservative compound path — matches the pre-branch behavior
        # for accelerator sessions with unreadable provider lists.
        ledger, request = _build_cpu_request(
            session, label="mixed accelerator+CPU ONNX inference",
        )
        return _CompoundInferenceContext(ledger, request, cancel_check)
    if providers != {"CPUExecutionProvider"}:
        ledger, request = _build_cpu_request(
            session, label="mixed accelerator+CPU ONNX inference",
        )
        return _CompoundInferenceContext(ledger, request, cancel_check)

    ledger, request = _build_cpu_request(session, label="CPU ONNX inference")
    return ledger.acquire(request, cancel_check=cancel_check)


def acquire_gpu_if_session_uses_it(session, *, cancel_check=None):
    """Backward-compatible name for provider-aware inference coordination."""
    return acquire_inference_resources(session, cancel_check=cancel_check)


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
#      subject_tenengrad, bg_tenengrad) and ``masks_features.update_embeddings``
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

    Held across the ``masks_features.get_mask`` → generate_mask → save_mask →
    ``masks_features.upsert_mask`` → set_active_mask_variant →
    ``masks_features.update_embeddings`` sequence in ``extract_masks_stage`` and the standalone extract-masks
    route so concurrent writers hitting the same photo serialise. Pipelines
    on different photos don't contend.
    """
    with _PHOTO_MASK_LOCKS_GUARD:
        lock = _PHOTO_MASK_LOCKS.get(photo_id)
        if lock is None:
            lock = threading.Lock()
            _PHOTO_MASK_LOCKS[photo_id] = lock
    return lock


def _photo_mask_lock_for_tests(photo_id):
    return acquire_photo_mask(photo_id)


# In-flight archive destinations claimed by local-processing pipeline runs.
# Two pipelines aimed at the same new ``final_destination`` can both pass the
# DB-only overlap check before either one has created the folder row (jobs.py
# allows up to SLOT_CAP=2 pipeline jobs concurrently), and the second would
# only fail inside ``move_folder`` after staging and processing everything.
# Reserving the destination up front rejects the duplicate run before any
# expensive work starts.
_ARCHIVE_DESTINATIONS: set = set()
_ARCHIVE_DESTINATIONS_GUARD = threading.Lock()


def _normalize_archive_destination(path):
    """Absolute, symlink-resolved, case-normalised form of ``path``.

    ``realpath`` resolves symlinks (including partial resolution when the
    leaf doesn't exist yet — the existing prefix is followed and the
    missing tail is preserved). ``normcase`` folds case on Windows. Two
    spellings that point at the same physical destination — a symlink and
    its real path, a case-only alias on case-insensitive Windows —
    collapse to the same key, so reservation/release can't drift apart and
    a reservation lookup hits even when the caller used a different
    spelling.

    Case-insensitive POSIX (default macOS APFS) needs an FS probe to fold
    case, which ``_paths_overlap`` does via ``move._path_equal_or_descends``
    for the comparison itself. Keeping the stored key as
    ``normcase(realpath(...))`` is enough because reserve/release are
    paired with the same Python string in pipeline_job.
    """
    import os

    return os.path.normcase(os.path.realpath(path))


def _paths_overlap(a, b):
    """Return True if ``a`` and ``b`` are equal or one descends from the other.

    Defers to ``move._path_equal_or_descends`` so the reservation check
    folds the same alias surface (symlinks, Windows case folding,
    case-insensitive POSIX) that ``move_folder``'s own overlap check runs
    later. Without this, two pipelines targeting the same physical
    destination via different spellings could both pass the reservation
    check and race into the same archive root before the move-time guard
    runs.
    """
    from move import _path_equal_or_descends

    if a == b:
        return True
    return (_path_equal_or_descends(a, b)
            or _path_equal_or_descends(b, a))


def try_reserve_archive_destination(path):
    """Claim ``path`` for an in-flight local-processing archive.

    Returns True on first claim, False if another running pipeline already
    owns an overlapping destination — equal to ``path``, an ancestor of it,
    or a descendant of it. Equal paths obviously collide; nested paths also
    collide because ``move_folder`` would either create the parent's tracked
    row inside its child's archive root, or move a parent into a destination
    that the child has already claimed, leaving overlapping folder roots in
    the catalog. Paths are normalised to absolute, so ``/Photos/Shoot`` and
    ``./Photos/Shoot`` collide. Must be paired with
    ``release_archive_destination`` once the run terminates (success or
    failure) so retries and follow-on runs can re-acquire.
    """
    normalized = _normalize_archive_destination(path)
    with _ARCHIVE_DESTINATIONS_GUARD:
        for existing in _ARCHIVE_DESTINATIONS:
            if _paths_overlap(existing, normalized):
                return False
        _ARCHIVE_DESTINATIONS.add(normalized)
        return True


def release_archive_destination(path):
    """Release a previously reserved archive destination."""
    normalized = _normalize_archive_destination(path)
    with _ARCHIVE_DESTINATIONS_GUARD:
        _ARCHIVE_DESTINATIONS.discard(normalized)


def _archive_destinations_for_tests():
    with _ARCHIVE_DESTINATIONS_GUARD:
        return set(_ARCHIVE_DESTINATIONS)
