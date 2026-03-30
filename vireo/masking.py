"""SAM2-based subject masking pipeline for wildlife photos.

Given a MegaDetector bounding box, refines it to a pixel-level mask using SAM2
ONNX models, then provides utilities for subject cropping and background blurring.

Masks are saved as single-channel PNGs in ~/.vireo/masks/{photo_id}.png.
"""

import logging
import os

import numpy as np
import onnx_runtime
from PIL import Image, ImageFilter

log = logging.getLogger(__name__)

# SAM2 variant names (used for model directory lookup)
SAM2_VARIANTS = {
    "sam2-tiny": "sam2-tiny",
    "sam2-small": "sam2-small",
    "sam2-base-plus": "sam2-base-plus",
    "sam2-large": "sam2-large",
}

# SAM2 image encoder native input size
SAM2_INPUT_SIZE = 1024

# ImageNet normalization (used by SAM2 image encoder)
_IMAGENET_MEAN = [0.485, 0.456, 0.406]
_IMAGENET_STD = [0.229, 0.224, 0.225]

_encoder_session = None
_decoder_session = None
_sam2_variant_loaded = None


def _get_sam2_sessions(variant="sam2-small"):
    """Load SAM2 ONNX sessions (cached singleton with variant tracking).

    Args:
        variant: one of sam2-tiny, sam2-small, sam2-base-plus, sam2-large

    Returns:
        (image_encoder_session, mask_decoder_session) tuple
    """
    global _encoder_session, _decoder_session, _sam2_variant_loaded

    if (
        _encoder_session is not None
        and _decoder_session is not None
        and _sam2_variant_loaded == variant
    ):
        return _encoder_session, _decoder_session

    if variant not in SAM2_VARIANTS:
        raise ValueError(
            f"Unknown SAM2 variant: {variant}. "
            f"Choose from: {list(SAM2_VARIANTS.keys())}"
        )

    model_dir = os.path.join(
        os.path.expanduser("~"), ".vireo", "models", variant
    )
    encoder_path = os.path.join(model_dir, "image_encoder.onnx")
    decoder_path = os.path.join(model_dir, "mask_decoder.onnx")

    if not os.path.isfile(encoder_path):
        raise FileNotFoundError(
            f"SAM2 image encoder not found at {encoder_path}. "
            f"Download it first via the models page."
        )
    if not os.path.isfile(decoder_path):
        raise FileNotFoundError(
            f"SAM2 mask decoder not found at {decoder_path}. "
            f"Download it first via the models page."
        )

    log.info("Loading SAM2 ONNX (%s)...", variant)
    enc_sess = onnx_runtime.create_session(encoder_path)
    dec_sess = onnx_runtime.create_session(decoder_path)

    _encoder_session = enc_sess
    _decoder_session = dec_sess
    _sam2_variant_loaded = variant
    log.info("SAM2 ONNX loaded (%s)", variant)
    return enc_sess, dec_sess


def generate_mask(image, detection_box, variant="sam2-small"):
    """Generate a pixel-level mask from a MegaDetector bounding box using SAM2.

    Args:
        image: PIL Image (working resolution proxy)
        detection_box: dict with {x, y, w, h} in normalized 0-1 coordinates
        variant: SAM2 model variant

    Returns:
        numpy boolean array (H, W) — True where subject is, or None on failure
    """
    encoder_session, decoder_session = _get_sam2_sessions(variant)
    img_array = np.array(image.convert("RGB"))
    orig_h, orig_w = img_array.shape[:2]

    # Convert normalized box to pixel coordinates [x1, y1, x2, y2]
    bx = detection_box["x"] * orig_w
    by = detection_box["y"] * orig_h
    bw = detection_box["w"] * orig_w
    bh = detection_box["h"] * orig_h

    try:
        # Step 1: Preprocess image and run encoder
        input_tensor = onnx_runtime.preprocess_image(
            image,
            size=(SAM2_INPUT_SIZE, SAM2_INPUT_SIZE),
            mean=_IMAGENET_MEAN,
            std=_IMAGENET_STD,
        )

        enc_input_name = encoder_session.get_inputs()[0].name
        enc_outputs = encoder_session.run(None, {enc_input_name: input_tensor})
        image_embeddings = enc_outputs[0]  # (1, C, H', W')

        # Step 2: Encode box prompt as numpy arrays
        # The image encoder resizes to SAM2_INPUT_SIZE x SAM2_INPUT_SIZE,
        # so the decoder expects prompt coordinates in that same space.
        # Scale the original-pixel box into encoder input coordinates.
        scale_x = SAM2_INPUT_SIZE / orig_w
        scale_y = SAM2_INPUT_SIZE / orig_h
        enc_x1 = bx * scale_x
        enc_y1 = by * scale_y
        enc_x2 = (bx + bw) * scale_x
        enc_y2 = (by + bh) * scale_y

        # For box prompts: point_coords has shape (1, 2, 2) with top-left
        # and bottom-right corners; point_labels has shape (1, 2) with
        # values [2, 3] (SAM2 box prompt markers)
        point_coords = np.array(
            [[[enc_x1, enc_y1], [enc_x2, enc_y2]]], dtype=np.float32
        )  # (1, 2, 2)
        point_labels = np.array([[2, 3]], dtype=np.float32)  # (1, 2)

        # No previous mask input
        mask_input = np.zeros((1, 1, 256, 256), dtype=np.float32)
        has_mask_input = np.array([0], dtype=np.float32)
        orig_im_size = np.array([orig_h, orig_w], dtype=np.int64)

        # Step 3: Run mask decoder
        # Build decoder input dict from session input names
        decoder_inputs = {}
        dec_input_names = [inp.name for inp in decoder_session.get_inputs()]

        # Map expected input names to our arrays
        input_map = {
            "image_embeddings": image_embeddings,
            "point_coords": point_coords,
            "point_labels": point_labels,
            "mask_input": mask_input,
            "has_mask_input": has_mask_input,
            "orig_im_size": orig_im_size,
        }
        for name in dec_input_names:
            if name in input_map:
                decoder_inputs[name] = input_map[name]

        dec_outputs = decoder_session.run(None, decoder_inputs)
        # Outputs: masks (1, N, H, W) and scores (1, N)
        masks = dec_outputs[0]  # (1, N, H, W)
        scores = dec_outputs[1]  # (1, N)

        # Step 4: Pick highest-scoring mask
        best_idx = int(np.argmax(scores.flatten()))

        # Extract the best mask and resize to original dimensions if needed
        best_mask = masks[0, best_idx]  # (H_out, W_out)

        if best_mask.shape != (orig_h, orig_w):
            # Resize mask to original image dimensions
            mask_img = Image.fromarray(
                (best_mask > 0).astype(np.uint8) * 255, mode="L"
            )
            mask_img = mask_img.resize(
                (orig_w, orig_h), Image.BILINEAR
            )
            best_mask = np.array(mask_img) > 127

        return best_mask.astype(bool)
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
