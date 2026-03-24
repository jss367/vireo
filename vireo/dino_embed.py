"""DINOv2 embedding extraction for the culling pipeline.

Computes dense visual embeddings using DINOv2 ViT models for:
- Subject crop embeddings (primary grouping feature for encounters)
- Global image embeddings (secondary scene-level context)

Models are loaded from the facebookresearch/dinov2 torch hub.
"""

import logging

import numpy as np
from PIL import Image

log = logging.getLogger(__name__)

# Variant → (torch hub model name, embedding dimension)
DINOV2_VARIANTS = {
    "vit-s14": ("dinov2_vits14", 384),
    "vit-b14": ("dinov2_vitb14", 768),
    "vit-l14": ("dinov2_vitl14", 1024),
}

# DINOv2 native input size
DINOV2_INPUT_SIZE = 518

_dinov2_model = None
_dinov2_variant_loaded = None
_dinov2_transform = None
_dinov2_device = None


def get_embedding_dim(variant="vit-b14"):
    """Return the embedding dimension for a DINOv2 variant.

    Args:
        variant: one of vit-s14, vit-b14, vit-l14

    Returns:
        int — embedding dimension
    """
    if variant not in DINOV2_VARIANTS:
        raise ValueError(
            f"Unknown DINOv2 variant: {variant}. "
            f"Choose from: {list(DINOV2_VARIANTS.keys())}"
        )
    return DINOV2_VARIANTS[variant][1]


def _get_dinov2_model(variant="vit-b14"):
    """Load DINOv2 model (cached singleton). Downloads weights on first use.

    Args:
        variant: one of vit-s14, vit-b14, vit-l14

    Returns:
        (model, transform, device) tuple
    """
    global _dinov2_model, _dinov2_variant_loaded, _dinov2_transform, _dinov2_device

    if _dinov2_model is not None and _dinov2_variant_loaded == variant:
        return _dinov2_model, _dinov2_transform, _dinov2_device

    if variant not in DINOV2_VARIANTS:
        raise ValueError(
            f"Unknown DINOv2 variant: {variant}. "
            f"Choose from: {list(DINOV2_VARIANTS.keys())}"
        )

    try:
        import torch
        from torchvision import transforms
    except ImportError:
        raise RuntimeError(
            "PyTorch and torchvision not installed. Run: pip install torch torchvision\n"
            "These are required for DINOv2 embeddings."
        )

    hub_name = DINOV2_VARIANTS[variant][0]

    device = "cpu"
    if torch.cuda.is_available():
        device = "cuda"
    elif hasattr(torch.backends, "mps") and torch.backends.mps.is_available():
        device = "mps"

    log.info("Loading DINOv2 (%s) on %s...", variant, device)
    model = torch.hub.load("facebookresearch/dinov2", hub_name, trust_repo=True)
    model = model.to(device)
    model.eval()

    transform = transforms.Compose([
        transforms.Resize(DINOV2_INPUT_SIZE, interpolation=transforms.InterpolationMode.BICUBIC),
        transforms.CenterCrop(DINOV2_INPUT_SIZE),
        transforms.ToTensor(),
        transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]),
    ])

    _dinov2_model = model
    _dinov2_variant_loaded = variant
    _dinov2_transform = transform
    _dinov2_device = device
    log.info("DINOv2 loaded (%s, %d-dim)", variant, DINOV2_VARIANTS[variant][1])
    return model, transform, device


def embed(image, variant="vit-b14"):
    """Compute a DINOv2 CLS token embedding for an image.

    Args:
        image: PIL Image (any size — will be resized to 518x518)
        variant: DINOv2 model variant

    Returns:
        numpy float32 array of shape (embedding_dim,)
    """
    import torch

    model, transform, device = _get_dinov2_model(variant)

    img_rgb = image.convert("RGB")
    tensor = transform(img_rgb).unsqueeze(0).to(device)

    with torch.inference_mode():
        embedding = model(tensor)

    return embedding.squeeze(0).cpu().numpy().astype(np.float32)


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
