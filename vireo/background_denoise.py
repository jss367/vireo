"""Optional camera-aware background smoothing with a protected subject.

This is a final detail pass: existing edits stay intact in the protected
area. No neural model or RAW-specific conversion is involved.
"""

import cv2
import numpy as np
from float_image import FloatImage
from PIL import Image
from scipy.ndimage import binary_fill_holes


def protection_mask(mask, *, corrected=False):
    """Fill enclosed SAM holes before crop/rotation can open them to an edge.

    Preserve explicitly painted holes. Keep soft edges in either case.
    """
    if corrected:
        return mask
    pixels = np.asarray(mask, dtype=np.uint8)
    filled = binary_fill_holes(pixels >= 128)
    return Image.fromarray(np.maximum(pixels, filled.astype(np.uint8) * 255))


def protection_weight(mask, scale, feather=0):
    """Protect the core + 8 native pixels, then fade outward over 16 pixels.

    Feather extends only the outside transition; it never erodes the core.
    OpenCV keeps the distance buffer float32 even for full-resolution RAWs.
    """
    pixels = np.asarray(mask, dtype=np.float32) / 255.0
    core = pixels >= 0.5
    if not core.any():
        return np.ones_like(pixels)  # An empty mask cannot identify a safe background.
    distance = cv2.distanceTransform((~core).astype(np.uint8), cv2.DIST_L2, 5)
    width = max((16.0 + feather) * scale, 0.001)
    t = np.clip((distance - 8.0 * scale) / width, 0.0, 1.0)
    return np.maximum(pixels, 1.0 - t * t * (3.0 - 2.0 * t))


def apply_background_denoise(image, weight, amount, scale, camera_metadata):
    """Denoise the rendered background; preserve protected pixels exactly."""
    from camera_denoise import resolve_profile
    from detail import apply_detail

    if np.all(weight >= 1):
        return image
    if image.mode not in ("RGB", "RGBA") or "transparency" in image.info:
        has_alpha = "A" in image.getbands() or "transparency" in image.info
        image = image.convert("RGBA" if has_alpha else "RGB")
    denoised = apply_detail(
        image, noise_reduction=amount, denoise_mode="camera",
        noise_profile=resolve_profile(camera_metadata), scale=scale,
    )
    before = np.asarray(image, dtype=np.float32)
    after = np.asarray(denoised, dtype=np.float32)
    blended = before + (after - before) * (1.0 - weight[..., None])
    if denoised is not image:
        denoised.close()
    if isinstance(image, FloatImage):
        return FloatImage(blended, encoding=image.encoding)
    if image.mode == "RGBA":
        blended[..., 3] = before[..., 3]
    return Image.fromarray(np.clip(blended + 0.5, 0, 255).astype(np.uint8))
