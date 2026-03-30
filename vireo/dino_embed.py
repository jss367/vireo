"""DINOv2 embedding extraction for the culling pipeline.

Computes dense visual embeddings using DINOv2 ViT models for:
- Subject crop embeddings (primary grouping feature for encounters)
- Global image embeddings (secondary scene-level context)

Models are loaded as ONNX sessions from ~/.vireo/models/dinov2-{variant}/.
"""

import logging
import os

import numpy as np
import onnx_runtime

log = logging.getLogger(__name__)

# Variant -> embedding dimension
DINOV2_VARIANTS = {
    "vit-s14": 384,
    "vit-b14": 768,
    "vit-l14": 1024,
}

# DINOv2 native input size
DINOV2_INPUT_SIZE = 518

# ImageNet normalization
_IMAGENET_MEAN = [0.485, 0.456, 0.406]
_IMAGENET_STD = [0.229, 0.224, 0.225]

_session = None
_variant_loaded = None


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
    session = _get_dinov2_session(variant)

    input_tensor = onnx_runtime.preprocess_image(
        image,
        size=(DINOV2_INPUT_SIZE, DINOV2_INPUT_SIZE),
        mean=_IMAGENET_MEAN,
        std=_IMAGENET_STD,
        center_crop=True,
    )

    input_name = session.get_inputs()[0].name
    outputs = session.run(None, {input_name: input_tensor})

    embedding = outputs[0].squeeze(0)
    return embedding.astype(np.float32)


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
