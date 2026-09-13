"""Shared ONNX Runtime utilities for model inference.

Provides ONNX session creation with automatic hardware provider selection,
image preprocessing, and common post-processing operations.
"""

import contextlib
import logging
import os
import threading
import weakref
from collections import OrderedDict

import numpy as np
from PIL import Image

log = logging.getLogger(__name__)

_SESSION_THREADS_LOCK = threading.Lock()
_SESSION_CPU_THREADS = weakref.WeakKeyDictionary()
_SESSION_CPU_THREADS_FALLBACK = OrderedDict()
_SESSION_CPU_THREADS_FALLBACK_LIMIT = 64


@contextlib.contextmanager
def acquire_session_cache_lock(lock, *, label="ONNX session cache"):
    """Acquire a model-session cache lock with bound pause/cancel support.

    The pre-acquire probe is pause-aware: a Pause request parks the caller
    BEFORE any lock is taken, so no other pipeline is blocked while the
    paused job waits for Resume. Once the lock is held, the post-acquire
    recheck uses the pure-cancel probe instead — a Pause arriving in the
    tiny window between lock acquisition and yield must NOT park the
    holder inside ``wait_if_paused``, or every unpaused peer waiting on
    the same DINO/detector/SAM/keypoint session cache would block until
    Resume. Cancel still releases the lock and raises.
    """
    from resource_ledger import (
        ResourceWaitCancelled,
        get_resource_ledger,
        resolve_resource_cancel_check,
        resolve_resource_pure_cancel_check,
    )

    cancel_check = resolve_resource_cancel_check()
    pure_cancel_check = resolve_resource_pure_cancel_check()
    if cancel_check is not None and cancel_check():
        raise ResourceWaitCancelled(f"Cancelled while waiting for {label}")

    def _post_acquire_recheck():
        """Return True to keep the lock; release + raise otherwise.

        Uses the pure-cancel probe so a Pause pending between
        ``lock.acquire()`` and this call does not park the holder. The
        probe itself may raise (a bound pipeline probe can surface an
        unrelated internal error). Either way — cancel returned True, or
        the probe raised — the lock we just acquired must be released so
        a cancelled/errored caller doesn't hold the cache mutex for the
        duration of its unwind.
        """
        if pure_cancel_check is None:
            return True
        try:
            cancelled = pure_cancel_check()
        except BaseException:
            lock.release()
            raise
        if cancelled:
            lock.release()
            raise ResourceWaitCancelled(
                f"Cancelled while waiting for {label}",
            )
        return True

    if lock.acquire(blocking=False):
        # Symmetry with the timed-acquire branch below: a probe that
        # raises must not leak the lock. The pre-acquire probe (above)
        # only guards ENTRY; the same probe running after we own the
        # lock is the release-safe path.
        _post_acquire_recheck()
        try:
            yield
        finally:
            lock.release()
        return

    # Cache-lock contention is downstream of model construction, so expose it
    # through the same per-job resource timing as the construction lease.
    with get_resource_ledger().track_external_wait():
        while True:
            if cancel_check is not None and cancel_check():
                raise ResourceWaitCancelled(
                    f"Cancelled while waiting for {label}",
                )
            if lock.acquire(timeout=0.2):
                # A release during the 0.2s acquire window can succeed
                # while cancellation (or the interactive text-search
                # deadline) has just fired. Without this recheck an
                # already-cancelled eye-keypoint participant that woke
                # here would kick off a fresh multi-hundred-megabyte
                # download when the previous holder exited with only
                # one file on disk. Release and raise so the cancel
                # wins the race, matching the GPU-lease recheck in
                # ``pipeline_locks._GpuLockContext``. ``_post_acquire_recheck``
                # uses the PURE-cancel probe — a Pause arriving in this
                # race window must NOT park the holder inside
                # ``wait_if_paused`` or every unpaused peer waiting on
                # the same model cache would block until Resume. It also
                # releases the lock if the probe itself raises, keeping
                # a bug in a bound probe from leaking the cache mutex.
                _post_acquire_recheck()
                break
    try:
        yield
    finally:
        lock.release()


# Substrings that identify onnxruntime load failures rooted in the file
# bytes themselves (corrupt protobuf, truncated graph, missing external
# data sidecar). Seeing one of these in an exception message means the
# on-disk model is unusable and a fresh download is the right remedy.
# Non-matching failures (permission denied, is-a-directory, out-of-memory,
# CUDA init errors, provider/compat issues) must NOT trigger self-heal.
#
# Intentionally narrow: generic phrases like "load model from" or
# "failed to load model" appear in every onnxruntime load error
# including non-corruption cases (provider load failures, ABI/compat
# mismatches). Matching those would make us delete + redownload
# multi-GB model files while the real root cause sits unresolved.
_CORRUPT_MODEL_MARKERS = (
    "invalid_protobuf",
    "protobuf parsing failed",
    "model_path must not be empty",
    "external data file",
    "no graph",
)
# Intentionally NOT included:
# - "invalid_graph" / INVALID_GRAPH: also emitted for opset/op
#   compatibility problems (e.g. installed onnxruntime is too old
#   for the model's opset). Deleting a valid-but-incompatible
#   model and redownloading the same bytes would not help and
#   just masks the real root cause (upgrade onnxruntime).


def _looks_like_corrupt_model(err):
    """Return True when an exception from create_session looks like an
    on-disk corruption signal (as opposed to an OS / environment error).

    Purely a string-matching heuristic against the onnxruntime message.
    OSError subclasses (PermissionError, IsADirectoryError, FileNotFoundError)
    are explicitly excluded — those are environment issues, not corruption,
    and blowing away the file would be actively harmful.
    """
    if isinstance(err, OSError):
        return False
    msg = str(err).lower()
    return any(marker in msg for marker in _CORRUPT_MODEL_MARKERS)


def _sibling_paths_to_purge(model_path):
    """Return the set of paths that must be removed alongside ``model_path``
    when self-healing a corrupt model.

    For ONNX graphs that use external data the .onnx file references a
    companion .onnx.data sidecar; purging both ensures the redownload
    starts from a clean slate and no stale bytes from an aborted earlier
    download can survive.
    """
    paths = [model_path]
    sidecar = model_path + ".data"
    if os.path.exists(sidecar):
        paths.append(sidecar)
    return paths


def create_session_with_self_heal(model_path, redownload=None):
    """Load an ONNX session, self-healing on corrupt / truncated model files.

    Wraps :func:`create_session` so that a load failure rooted in the
    on-disk bytes (corrupt protobuf, truncated graph, missing external
    data sidecar) triggers a single recovery attempt: delete the broken
    files, invoke the caller-supplied ``redownload`` callable, then retry
    session creation exactly once. On the second failure raise a
    user-facing :class:`RuntimeError` chained to the underlying
    onnxruntime error — never loop.

    Non-corruption errors (``PermissionError``, ``IsADirectoryError``,
    out-of-memory, CUDA init failures) are re-raised unchanged so the
    user or caller can react appropriately. We never delete the file in
    that path — the bytes are almost certainly fine.

    Args:
        model_path: absolute path to the .onnx file.
        redownload: optional zero-argument callable that replaces the
            removed file(s) with a fresh copy. If ``None``, the wrapper
            has no recovery strategy and re-raises the original error
            without touching the filesystem.

    Returns:
        An ``onnxruntime.InferenceSession`` for ``model_path``.
    """
    try:
        return create_session(model_path)
    except Exception as first_err:
        if not _looks_like_corrupt_model(first_err):
            raise
        if redownload is None:
            # Caller has no recovery strategy (e.g. custom user-supplied
            # model with no known download source). Re-raise the original
            # error so the user isn't silently losing their file.
            raise

        log.warning(
            "ONNX model %s failed to load, looks like corruption: %s. "
            "Deleting on-disk files and triggering redownload.",
            model_path, first_err,
        )

        # Delete the graph and any external-data sidecar BEFORE invoking
        # redownload so a resumable downloader can't mistake the corrupt
        # stub for a partial download to pick up from.
        for path in _sibling_paths_to_purge(model_path):
            with contextlib.suppress(OSError):
                os.unlink(path)
                log.info("Self-heal: removed %s", path)

        try:
            redownload()
        except Exception as redl_err:
            # Download itself failed (network, disk full, HF API down).
            # Re-raise with context so the caller sees both errors.
            raise RuntimeError(
                f"Self-heal of {model_path} failed: redownload raised "
                f"{type(redl_err).__name__}: {redl_err}"
            ) from redl_err

        try:
            return create_session(model_path)
        except Exception as second_err:
            # A second failure means the fresh download is also unusable,
            # or the root cause wasn't actually on-disk corruption. Do
            # NOT loop — surface a clear message chained to the original
            # error so logs show both.
            raise RuntimeError(
                f"Model at {model_path} still failed to load after "
                f"self-heal redownload. Original error: {first_err}. "
                f"Retry error: {second_err}. "
                "Open Settings → Models and click Repair, or check "
                "~/.vireo/vireo.log for details."
            ) from second_err


def get_providers():
    """Return ONNX Runtime execution providers in priority order.

    Checks which providers are actually available in the installed
    onnxruntime package and returns them in preference order:
    CoreML (Apple) > CUDA (NVIDIA) > CPU (fallback).
    """
    import onnxruntime as ort

    available = set(ort.get_available_providers())
    providers = []
    for p in ["CoreMLExecutionProvider", "CUDAExecutionProvider"]:
        if p in available:
            providers.append(p)
    providers.append("CPUExecutionProvider")
    return providers


def _remember_session_cpu_threads(session, threads):
    """Associate an ONNX session with its enforceable CPU thread budget."""
    threads = max(1, int(threads))
    with contextlib.suppress(Exception):
        # The Python InferenceSession wrapper normally accepts attributes.
        # Keeping the value on the object lets top-level and package-qualified
        # imports observe the same budget in mixed test/tooling environments.
        session._vireo_cpu_threads = threads
    with _SESSION_THREADS_LOCK:
        try:
            _SESSION_CPU_THREADS[session] = threads
        except TypeError:
            # Some extension-backed test doubles are not weak-referenceable.
            # Retain the object with the value so an id reused after eviction
            # can never inherit another session's budget. Keep this uncommon
            # compatibility path bounded; production sessions use the weak map.
            key = id(session)
            _SESSION_CPU_THREADS_FALLBACK[key] = (session, threads)
            _SESSION_CPU_THREADS_FALLBACK.move_to_end(key)
            while (
                len(_SESSION_CPU_THREADS_FALLBACK)
                > _SESSION_CPU_THREADS_FALLBACK_LIMIT
            ):
                _SESSION_CPU_THREADS_FALLBACK.popitem(last=False)


def session_cpu_threads(session, default=None):
    """Return the configured CPU threads for ``session`` when known."""
    with contextlib.suppress(Exception):
        threads = int(session._vireo_cpu_threads)
        if threads >= 1:
            return threads
    with _SESSION_THREADS_LOCK:
        try:
            threads = _SESSION_CPU_THREADS.get(session)
        except TypeError:
            threads = None
        if threads is None:
            key = id(session)
            fallback = _SESSION_CPU_THREADS_FALLBACK.get(key)
            if fallback is not None and fallback[0] is session:
                _SESSION_CPU_THREADS_FALLBACK.move_to_end(key)
                threads = fallback[1]
            elif fallback is not None:
                # Treat an id-keyed hit as advisory until identity is proven.
                _SESSION_CPU_THREADS_FALLBACK.pop(key, None)
    return threads if threads is not None else default


def create_session(model_path, providers=None, *, cancel_check=None):
    """Create an ONNX Runtime InferenceSession with best available provider.

    Args:
        model_path: path to .onnx file
        providers: optional explicit provider order. The default uses Vireo's
            hardware selection; CPU-only models can force CPU execution.

    Returns:
        ort.InferenceSession
    """
    import os

    import onnxruntime as ort

    providers = list(providers) if providers is not None else get_providers()

    # Models with external data (.onnx.data sidecar files) skip CoreML for two
    # independent reasons, either of which alone would justify it:
    #
    #  1. onnxruntime 1.24 raises "model_path must not be empty" while
    #     initializing such a model under CoreML. Fixed upstream by 1.30.
    #  2. Every external-data model Vireo ships is a large ViT (dinov2,
    #     bioclip ViT-H/14, inat21 eva02-L), and CoreML is *slower* than CPU
    #     on those regardless of the load bug. Measured on an M3 Max with
    #     onnxruntime 1.30, where the load succeeds: dinov2-vit-b14 runs
    #     0.55s/image on CPU and 1.57s/image on CoreML, because CoreML claims
    #     314 of 565 nodes and shatters the graph into 98 partitions.
    #
    # So do not drop this when the onnxruntime floor moves past 1.30 — the
    # crash goes away, the slowdown does not. See vireo/masking.py for the
    # same trade-off on SAM2, and vireo/detector.py for the CNN counterexample
    # where CoreML wins by ~11x.
    if str(model_path).endswith(".onnx") and os.path.exists(str(model_path) + ".data"):
        before = list(providers)
        providers = [p for p in providers if p != "CoreMLExecutionProvider"]
        if providers != before:
            log.info(
                "Model %s uses external data (.onnx.data); "
                "excluding CoreMLExecutionProvider (slower than CPU for "
                "these ViT models, and unloadable on onnxruntime < 1.30)",
                model_path,
            )

    from resource_ledger import (
        ResourceRequest,
        cpu_inference_request,
        get_resource_ledger,
    )

    ledger = get_resource_ledger()
    # The session's ONNX native pool is sized ONCE at construction and reused
    # for every subsequent inference call, so its thread count must reflect
    # the intended inference profile — not the transient construction-time
    # grant. If we instead used ``lease.cpu_permits`` here, a construction
    # request that landed while another CPU phase (e.g. a scan) held most of
    # the budget would receive only its minimum grant, and the cached session
    # would then be capped at that reduced count forever, throttling every
    # inference call until the process restarts.
    inference_profile = cpu_inference_request(ledger.cpu_capacity)
    session_thread_count = inference_profile.preferred
    # Require the construction lease to hold ``session_thread_count`` permits
    # too. ONNX exercises its ``intra_op_num_threads`` pool during graph
    # optimization and kernel prepacking, so a smaller construction grant
    # would let those native threads exceed the process CPU budget for the
    # duration of the load — precisely when the ledger is already tight.
    # Waiting for the full inference budget is the right trade: model loads
    # are rare and one-time, while oversubscribing the CPU during a
    # contentious construction hurts every concurrent job.
    cpu_request = inference_profile
    request = ResourceRequest(
        cpu=cpu_request,
        lanes=("model_construction",),
        label="ONNX model construction",
    )
    with ledger.acquire(request, cancel_check=cancel_check):
        session_options = ort.SessionOptions()
        session_options.intra_op_num_threads = session_thread_count
        # Keep inter-op parallelism at one so ONNX cannot multiply the CPU
        # grant by running several graph branches, each with its own intra-op
        # pool.
        session_options.inter_op_num_threads = 1
        log.info(
            "Loading ONNX model: %s (providers: %s, CPU threads: %d)",
            model_path, providers, session_thread_count,
        )
        session = ort.InferenceSession(
            model_path,
            sess_options=session_options,
            providers=providers,
        )
        _remember_session_cpu_threads(session, session_thread_count)
    actual = session.get_providers()
    log.info("ONNX session using: %s", actual)
    return session


def preprocess_image(image, size, mean, std, center_crop=False):
    """Preprocess a PIL Image for ONNX model input.

    Args:
        image: PIL Image
        size: (height, width) tuple
        mean: per-channel mean for normalization (list of 3 floats)
        std: per-channel std for normalization (list of 3 floats)
        center_crop: if True, resize so shortest edge matches then center crop

    Returns:
        numpy float32 array of shape (1, 3, H, W)
    """
    img = image.convert("RGB")

    if center_crop:
        # Resize so shortest edge = target, then center crop
        target_h, target_w = size
        w, h = img.size
        scale = max(target_h / h, target_w / w)
        new_w = int(w * scale + 0.5)
        new_h = int(h * scale + 0.5)
        img = img.resize((new_w, new_h), Image.BICUBIC)
        left = (new_w - target_w) // 2
        top = (new_h - target_h) // 2
        img = img.crop((left, top, left + target_w, top + target_h))
    else:
        target_h, target_w = size
        img = img.resize((target_w, target_h), Image.BICUBIC)

    arr = np.array(img, dtype=np.float32) / 255.0
    mean = np.array(mean, dtype=np.float32).reshape(1, 1, 3)
    std = np.array(std, dtype=np.float32).reshape(1, 1, 3)
    arr = (arr - mean) / std
    arr = arr.transpose(2, 0, 1)  # HWC -> CHW
    return arr[np.newaxis, ...]  # add batch dim


def softmax(logits, axis=-1):
    """Compute softmax probabilities from logits.

    Args:
        logits: numpy array of raw model outputs
        axis: axis along which to compute softmax

    Returns:
        numpy array of probabilities (same shape as input)
    """
    e = np.exp(logits - np.max(logits, axis=axis, keepdims=True))
    return e / e.sum(axis=axis, keepdims=True)


def nms(boxes, scores, iou_threshold=0.5):
    """Non-maximum suppression on bounding boxes.

    Args:
        boxes: numpy array (N, 4) in [x1, y1, x2, y2] format
        scores: numpy array (N,) of confidence scores
        iou_threshold: IoU threshold for suppression

    Returns:
        list of indices to keep
    """
    if len(boxes) == 0:
        return []

    x1 = boxes[:, 0]
    y1 = boxes[:, 1]
    x2 = boxes[:, 2]
    y2 = boxes[:, 3]
    areas = (x2 - x1) * (y2 - y1)

    order = scores.argsort()[::-1]
    keep = []

    while len(order) > 0:
        i = order[0]
        keep.append(i)
        if len(order) == 1:
            break

        xx1 = np.maximum(x1[i], x1[order[1:]])
        yy1 = np.maximum(y1[i], y1[order[1:]])
        xx2 = np.minimum(x2[i], x2[order[1:]])
        yy2 = np.minimum(y2[i], y2[order[1:]])

        inter = np.maximum(0, xx2 - xx1) * np.maximum(0, yy2 - yy1)
        iou = inter / (areas[i] + areas[order[1:]] - inter + 1e-6)

        remaining = np.where(iou <= iou_threshold)[0]
        order = order[remaining + 1]

    return keep
