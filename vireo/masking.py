"""SAM2-based subject masking pipeline for wildlife photos.

Given a MegaDetector bounding box, refines it to a pixel-level mask using SAM2,
then provides utilities for subject cropping and background blurring.

Masks are saved as single-channel PNGs in ~/.vireo/masks/{photo_id}.png.
"""

import logging
import os

import numpy as np
from PIL import Image, ImageFilter

log = logging.getLogger(__name__)

# SAM2 variant → (config name, checkpoint name) for huggingface hub
SAM2_VARIANTS = {
    "sam2-tiny": ("sam2.1_hiera_t", "sam2.1_hiera_tiny.pt"),
    "sam2-small": ("sam2.1_hiera_s", "sam2.1_hiera_small.pt"),
    "sam2-base-plus": ("sam2.1_hiera_b+", "sam2.1_hiera_base_plus.pt"),
    "sam2-large": ("sam2.1_hiera_l", "sam2.1_hiera_large.pt"),
}

_sam2_predictor = None
_sam2_variant_loaded = None


def _get_sam2_predictor(variant="sam2-small"):
    """Load SAM2 image predictor (cached singleton). Downloads weights on first use.

    Args:
        variant: one of sam2-tiny, sam2-small, sam2-base-plus, sam2-large

    Returns:
        SAM2ImagePredictor instance
    """
    global _sam2_predictor, _sam2_variant_loaded

    if _sam2_predictor is not None and _sam2_variant_loaded == variant:
        return _sam2_predictor

    if variant not in SAM2_VARIANTS:
        raise ValueError(
            f"Unknown SAM2 variant: {variant}. "
            f"Choose from: {list(SAM2_VARIANTS.keys())}"
        )

    try:
        import torch
        logging.getLogger("sam2").setLevel(logging.WARNING)
        from sam2.build_sam import build_sam2_hf
        from sam2.sam2_image_predictor import SAM2ImagePredictor
    except ImportError:
        raise RuntimeError(
            "SAM2 not installed. Run: pip install sam-2\n"
            "This provides SAM2 for subject segmentation masks."
        )

    device = "cpu"
    if torch.cuda.is_available():
        device = "cuda"
    elif hasattr(torch.backends, "mps") and torch.backends.mps.is_available():
        device = "mps"

    hf_model_id = "facebook/sam2.1-hiera-small"
    # Map variant to HF model ID
    variant_to_hf = {
        "sam2-tiny": "facebook/sam2.1-hiera-tiny",
        "sam2-small": "facebook/sam2.1-hiera-small",
        "sam2-base-plus": "facebook/sam2.1-hiera-base-plus",
        "sam2-large": "facebook/sam2.1-hiera-large",
    }
    hf_model_id = variant_to_hf[variant]

    log.info("Loading SAM2 (%s) on %s...", variant, device)
    model = build_sam2_hf(hf_model_id, device=device)
    _sam2_predictor = SAM2ImagePredictor(model)
    _sam2_variant_loaded = variant
    log.info("SAM2 loaded")
    return _sam2_predictor


def generate_mask(image, detection_box, variant="sam2-small"):
    """Generate a pixel-level mask from a MegaDetector bounding box using SAM2.

    Args:
        image: PIL Image (working resolution proxy)
        detection_box: dict with {x, y, w, h} in normalized 0-1 coordinates
        variant: SAM2 model variant

    Returns:
        numpy boolean array (H, W) — True where subject is, or None on failure
    """
    import torch

    predictor = _get_sam2_predictor(variant)
    img_array = np.array(image)
    h, w = img_array.shape[:2]

    # Convert normalized box to pixel coordinates [x1, y1, x2, y2]
    bx = detection_box["x"] * w
    by = detection_box["y"] * h
    bw = detection_box["w"] * w
    bh = detection_box["h"] * h
    box = np.array([bx, by, bx + bw, by + bh])

    try:
        with torch.inference_mode():
            predictor.set_image(img_array)
            masks, scores, _ = predictor.predict(
                box=box,
                multimask_output=True,
            )
        # Pick the highest-scoring mask
        best_idx = int(np.argmax(scores))
        return masks[best_idx].astype(bool)
    except Exception:
        log.warning("SAM2 mask generation failed", exc_info=True)
        return None


def save_mask(mask, masks_dir, photo_id):
    """Save a boolean mask as a single-channel PNG.

    Args:
        mask: numpy boolean array (H, W)
        masks_dir: directory for mask files (e.g. ~/.vireo/masks/)
        photo_id: photo ID for the filename

    Returns:
        path to the saved mask file
    """
    os.makedirs(masks_dir, exist_ok=True)
    path = os.path.join(masks_dir, f"{photo_id}.png")
    # Convert bool mask to uint8 (0 or 255)
    mask_img = Image.fromarray((mask.astype(np.uint8) * 255), mode="L")
    mask_img.save(path, format="PNG")
    return path


def load_mask(masks_dir, photo_id):
    """Load a mask from disk.

    Args:
        masks_dir: directory containing mask files
        photo_id: photo ID

    Returns:
        numpy boolean array (H, W), or None if not found
    """
    path = os.path.join(masks_dir, f"{photo_id}.png")
    if not os.path.exists(path):
        return None
    with Image.open(path) as mask_img:
        return np.array(mask_img.convert("L")) > 127


def crop_subject(image, mask, margin=0.15):
    """Crop the subject from an image using its mask with a margin.

    Args:
        image: PIL Image
        mask: numpy boolean array (H, W)
        margin: fraction of bounding box size to add as padding (0.15 = 15%)

    Returns:
        PIL Image of the cropped subject, or None if mask is empty
    """
    ys, xs = np.where(mask)
    if len(ys) == 0:
        return None

    y_min, y_max = int(ys.min()), int(ys.max())
    x_min, x_max = int(xs.min()), int(xs.max())

    box_h = y_max - y_min
    box_w = x_max - x_min
    pad_h = int(box_h * margin)
    pad_w = int(box_w * margin)

    h, w = mask.shape
    crop_y1 = max(0, y_min - pad_h)
    crop_y2 = min(h, y_max + pad_h)
    crop_x1 = max(0, x_min - pad_w)
    crop_x2 = min(w, x_max + pad_w)

    return image.crop((crop_x1, crop_y1, crop_x2, crop_y2))


def blur_background(image, mask, radius=51):
    """Neutralize background by heavy Gaussian blur outside the subject mask.

    Preserves the subject pixels exactly. Background gets blurred to remove
    detail while preserving rough color context. This avoids the artificial
    edges that black-fill or mean-color-fill would create.

    Args:
        image: PIL Image
        mask: numpy boolean array (H, W)
        radius: Gaussian blur radius (default 51px per design doc)

    Returns:
        PIL Image with background blurred
    """
    blurred = image.filter(ImageFilter.GaussianBlur(radius=radius))
    # Composite: subject from original, background from blurred
    mask_img = Image.fromarray((mask.astype(np.uint8) * 255), mode="L")
    return Image.composite(image, blurred, mask_img)


def crop_completeness(mask):
    """Compute what fraction of the mask perimeter does NOT touch the frame edge.

    A bird fully within the frame scores 1.0. A bird with half its perimeter
    clipped by the frame edge scores ~0.5.

    Args:
        mask: numpy boolean array (H, W)

    Returns:
        float in [0, 1]
    """
    h, w = mask.shape
    if not mask.any():
        return 0.0

    # Find perimeter pixels: mask pixels that have at least one non-mask neighbor
    # Use a simple approach: dilate - original = perimeter
    from scipy.ndimage import binary_dilation

    dilated = binary_dilation(mask, iterations=1)
    perimeter = dilated & ~mask

    # Also include mask pixels on the edge of the frame
    edge_mask = np.zeros_like(mask)
    edge_mask[0, :] = mask[0, :]
    edge_mask[-1, :] = mask[-1, :]
    edge_mask[:, 0] = mask[:, 0]
    edge_mask[:, -1] = mask[:, -1]
    perimeter = perimeter | edge_mask

    total_perimeter = perimeter.sum()
    if total_perimeter == 0:
        return 1.0

    # Count perimeter pixels that touch the frame edge
    edge_perimeter = 0
    edge_perimeter += perimeter[0, :].sum()
    edge_perimeter += perimeter[-1, :].sum()
    edge_perimeter += perimeter[1:-1, 0].sum()
    edge_perimeter += perimeter[1:-1, -1].sum()

    return float(1.0 - edge_perimeter / total_perimeter)


def render_proxy(image_path, longest_edge=1536):
    """Load an image at working resolution for the pipeline.

    Args:
        image_path: path to the source image
        longest_edge: maximum dimension (default 1536 per design doc)

    Returns:
        PIL Image or None on failure
    """
    from image_loader import load_image

    return load_image(image_path, max_size=longest_edge)
