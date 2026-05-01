"""DINOv2 embedding extraction for the culling pipeline.

Computes dense visual embeddings using DINOv2 ViT models for:
- Subject crop embeddings (primary grouping feature for encounters)
- Global image embeddings (secondary scene-level context)

Models are loaded as ONNX sessions from ~/.vireo/models/dinov2-{variant}/.
"""

import logging
import os
import threading

import numpy as np
import onnx_runtime

log = logging.getLogger(__name__)

# Variant -> embedding dimension
DINOV2_VARIANTS = {
    "vit-s14": 384,
    "vit-b14": 768,
    "vit-l14": 1024,
}

# Rough size estimate surfaced in the download progress message, per variant.
_DINOV2_SIZE_HINT = {
    "vit-s14": "~85 MB",
    "vit-b14": "~350 MB",
    "vit-l14": "~1.2 GB",
}

# DINOv2 native input size
DINOV2_INPUT_SIZE = 518

# ImageNet normalization
_IMAGENET_MEAN = [0.485, 0.456, 0.406]
_IMAGENET_STD = [0.229, 0.224, 0.225]

_session = None
_variant_loaded = None

_dinov2_download_lock = threading.Lock()


def _dinov2_model_path(variant):
    model_dir = os.path.join(
        os.path.expanduser("~"), ".vireo", "models", f"dinov2-{variant}"
    )
    return model_dir, os.path.join(model_dir, "model.onnx")


def dinov2_status(variant):
    """Report whether DINOv2 weights for ``variant`` are already on disk.

    Both the graph stub (``model.onnx``) and external-data sidecar
    (``model.onnx.data``) must be present — a graph-only state is treated
    as not-ready so the readiness UI matches what the loader actually
    requires (see ``ensure_dinov2_weights``).
    """
    _, model_path = _dinov2_model_path(variant)
    data_path = model_path + ".data"
    return {
        "variant": variant,
        "ready": os.path.isfile(model_path) and os.path.isfile(data_path),
        "size_hint": _DINOV2_SIZE_HINT.get(variant, ""),
    }


def ensure_dinov2_weights(variant="vit-b14", progress_callback=None):
    """Ensure DINOv2 ONNX weights for ``variant`` are on disk.

    DINOv2 ONNX exports use external-data layout: ``model.onnx`` is a
    ~1 MB graph stub that references the real weights in a sibling
    ``model.onnx.data`` sidecar (~85 MB / ~346 MB / ~1.2 GB depending on
    variant).  BOTH files must be on disk — without the sidecar, ONNX
    Runtime fails with the opaque
    ``model_path must not be empty`` error on every inference call.

    Returns the graph path when both files are already downloaded.
    Otherwise fetches whichever is missing from Hugging Face into
    ``~/.vireo/models/dinov2-{variant}/``.  Raises RuntimeError on
    failure so callers can abort rather than silently run without
    embeddings.

    Args:
        variant: one of vit-s14, vit-b14, vit-l14
        progress_callback: optional callable(phase: str, current: int,
            total: int) invoked once before the download starts and once
            after it completes.
    """
    if variant not in DINOV2_VARIANTS:
        raise ValueError(
            f"Unknown DINOv2 variant: {variant}. "
            f"Choose from: {list(DINOV2_VARIANTS.keys())}"
        )

    model_dir, model_path = _dinov2_model_path(variant)
    data_path = model_path + ".data"

    # Require BOTH files to short-circuit.  A lingering stub-only
    # ``model.onnx`` from a pre-#550 UI download (or a past partial
    # download) must NOT skip the sidecar fetch — otherwise ONNX Runtime
    # keeps crashing with ``model_path must not be empty`` on every
    # pipeline run and the broken state never self-heals.
    if os.path.isfile(model_path) and os.path.isfile(data_path):
        return model_path

    with _dinov2_download_lock:
        if os.path.isfile(model_path) and os.path.isfile(data_path):
            return model_path

        os.makedirs(model_dir, exist_ok=True)

        size_hint = _DINOV2_SIZE_HINT.get(variant, "")
        if progress_callback:
            progress_callback(
                f"Downloading DINOv2 {variant} ({size_hint}, first run only)...",
                0, 2,
            )
        log.info(
            "DINOv2 weights missing for %s — downloading from Hugging Face",
            variant,
        )

        tmp_path = model_path + ".download"
        tmp_data_path = data_path + ".download"
        data_backup_path = data_path + ".prev"
        had_prior_sidecar = os.path.isfile(data_path)
        try:
            import contextlib
            import shutil

            from huggingface_hub import HfApi, hf_hub_download
            from models import ONNX_REPO

            # Resolve a pinned revision upfront so BOTH fetches target the
            # same commit.  Without this, a push to jss367/vireo-onnx-models
            # between the graph and sidecar fetches could produce a
            # mismatched pair that ONNX Runtime refuses to load.  Network
            # failure here is non-fatal — fall back to the repo's default
            # branch and accept the (tiny) race window; this is no worse
            # than the pre-fix behaviour.
            try:
                pinned_rev = HfApi().model_info(ONNX_REPO).sha
            except Exception as e:  # noqa: BLE001
                log.info(
                    "Could not resolve HF revision for %s (%s); "
                    "falling back to default branch",
                    ONNX_REPO, e,
                )
                pinned_rev = None

            cached_graph = hf_hub_download(
                repo_id=ONNX_REPO,
                filename="model.onnx",
                subfolder=f"dinov2-{variant}",
                revision=pinned_rev,
            )
            shutil.copy2(cached_graph, tmp_path)

            if progress_callback:
                progress_callback(
                    f"Downloading DINOv2 {variant} weights sidecar...", 1, 2,
                )

            cached_data = hf_hub_download(
                repo_id=ONNX_REPO,
                filename="model.onnx.data",
                subfolder=f"dinov2-{variant}",
                revision=pinned_rev,
            )
            shutil.copy2(cached_data, tmp_data_path)

            # Promote the pair atomically *as a pair*.  Sidecar goes first
            # so ONNX Runtime never sees a graph that references a
            # not-yet-written sidecar.  If the graph replace then fails
            # (e.g. Windows file lock on the existing `model.onnx`),
            # roll the sidecar back so we don't leave a new-sidecar /
            # old-graph mismatch on disk that the noop check would
            # short-circuit on the next run.
            if had_prior_sidecar:
                os.replace(data_path, data_backup_path)
            try:
                os.replace(tmp_data_path, data_path)
                os.replace(tmp_path, model_path)
            except Exception:
                with contextlib.suppress(OSError):
                    os.unlink(data_path)
                if had_prior_sidecar:
                    with contextlib.suppress(OSError):
                        os.replace(data_backup_path, data_path)
                raise
            else:
                if had_prior_sidecar:
                    with contextlib.suppress(OSError):
                        os.unlink(data_backup_path)
        except Exception as e:
            import contextlib

            with contextlib.suppress(OSError):
                os.unlink(tmp_path)
            with contextlib.suppress(OSError):
                os.unlink(tmp_data_path)
            with contextlib.suppress(OSError):
                os.unlink(data_backup_path)
            raise RuntimeError(
                f"Failed to download DINOv2 weights ({variant}): {e}. "
                "Check your network connection and retry, or download "
                "manually from the pipeline models page."
            ) from e

        if not (os.path.isfile(model_path) and os.path.isfile(data_path)):
            raise RuntimeError(
                "DINOv2 download completed but weights are missing at "
                f"{model_dir}."
            )

        # Size totals both files — reporting just the 1 MB graph makes a
        # successful install look broken.
        total_bytes = os.path.getsize(model_path) + os.path.getsize(data_path)
        size_mb = round(total_bytes / 1024 / 1024, 1)
        log.info("DINOv2 weights downloaded (%s, %s MB)", variant, size_mb)
        if progress_callback:
            progress_callback(
                f"DINOv2 {variant} ready ({size_mb} MB)", 2, 2,
            )

        return model_path


def get_embedding_dim(variant="vit-b14"):
    """Return the embedding dimension for a DINOv2 variant.

    Args:
        variant: one of vit-s14, vit-b14, vit-l14

    Returns:
        int -- embedding dimension
    """
    if variant not in DINOV2_VARIANTS:
        raise ValueError(
            f"Unknown DINOv2 variant: {variant}. "
            f"Choose from: {list(DINOV2_VARIANTS.keys())}"
        )
    return DINOV2_VARIANTS[variant]


def _get_dinov2_session(variant="vit-b14"):
    """Load DINOv2 ONNX session (cached singleton).

    Args:
        variant: one of vit-s14, vit-b14, vit-l14

    Returns:
        ort.InferenceSession
    """
    global _session, _variant_loaded

    if _session is not None and _variant_loaded == variant:
        return _session

    if variant not in DINOV2_VARIANTS:
        raise ValueError(
            f"Unknown DINOv2 variant: {variant}. "
            f"Choose from: {list(DINOV2_VARIANTS.keys())}"
        )

    model_dir = os.path.join(
        os.path.expanduser("~"), ".vireo", "models", f"dinov2-{variant}"
    )
    model_path = os.path.join(model_dir, "model.onnx")

    if not os.path.isfile(model_path):
        raise FileNotFoundError(
            f"DINOv2 ONNX model not found at {model_path}. "
            f"Download it first via the models page."
        )

    log.info("Loading DINOv2 ONNX (%s)...", variant)
    sess = onnx_runtime.create_session(model_path)

    _session = sess
    _variant_loaded = variant
    log.info(
        "DINOv2 ONNX loaded (%s, %d-dim)", variant, DINOV2_VARIANTS[variant]
    )
    return sess


def embed(image, variant="vit-b14"):
    """Compute a DINOv2 CLS token embedding for an image.

    Args:
        image: PIL Image (any size -- will be resized to 518x518)
        variant: DINOv2 model variant

    Returns:
        numpy float32 array of shape (embedding_dim,)
    """
    return embed_batch([image], variant=variant)[0]


def embed_batch(images, variant="vit-b14"):
    """Compute DINOv2 CLS token embeddings for a batch of images in one
    inference call.

    DINOv2 ONNX is forced onto CPU on Apple Silicon (CoreML crashes on
    external-data models, see ``onnx_runtime.create_session``). Per-call
    overhead — preprocessing + ORT session.run setup — is non-trivial, so
    the pipeline's "subject crop + global proxy" pattern previously paid
    that cost twice per photo. Batching them into one call cuts that
    overhead in half.

    Args:
        images: non-empty list of PIL Images. Each is resized + center
            cropped to 518x518.
        variant: DINOv2 model variant.

    Returns:
        numpy float32 array of shape ``(len(images), embedding_dim)``.
    """
    if not images:
        raise ValueError("embed_batch requires at least one image")

    session = _get_dinov2_session(variant)

    tensors = [
        onnx_runtime.preprocess_image(
            img,
            size=(DINOV2_INPUT_SIZE, DINOV2_INPUT_SIZE),
            mean=_IMAGENET_MEAN,
            std=_IMAGENET_STD,
            center_crop=True,
        )
        for img in images
    ]
    batch = np.concatenate(tensors, axis=0)

    input_name = session.get_inputs()[0].name
    outputs = session.run(None, {input_name: batch})

    return outputs[0].astype(np.float32)


def embed_subject(crop_image, variant="vit-b14"):
    """Compute DINOv2 embedding for a subject crop.

    This is the primary grouping feature (s_i in the design doc).

    Args:
        crop_image: PIL Image of the subject crop
        variant: DINOv2 model variant

    Returns:
        numpy float32 array of shape (embedding_dim,)
    """
    return embed(crop_image, variant=variant)


def embed_global(proxy_image, variant="vit-b14"):
    """Compute DINOv2 embedding for the full working image.

    This is the secondary scene-level context (g_i in the design doc).

    Args:
        proxy_image: PIL Image at working resolution
        variant: DINOv2 model variant

    Returns:
        numpy float32 array of shape (embedding_dim,)
    """
    return embed(proxy_image, variant=variant)


def embedding_to_blob(embedding):
    """Convert a numpy embedding to bytes for SQLite BLOB storage.

    Args:
        embedding: numpy float32 array

    Returns:
        bytes
    """
    return embedding.astype(np.float32).tobytes()


def blob_to_embedding(blob, variant="vit-b14"):
    """Convert a SQLite BLOB back to a numpy embedding.

    Args:
        blob: bytes from the database
        variant: DINOv2 variant (used to validate expected dimension)

    Returns:
        numpy float32 array of shape (embedding_dim,)
    """
    return np.frombuffer(blob, dtype=np.float32)
