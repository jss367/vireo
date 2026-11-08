"""Subject-aware quality feature extraction for the culling pipeline.

All features are computed relative to the SAM2 mask — scoring the bird,
not the frame. Requires a mask from masking.py.

Features:
- Tenengrad sharpness (subject and background ring)
- Exposure stats (clip fractions and median luminance on subject)
- Background separation (variance of background pixels)
- Crop pHash (perceptual hash of masked subject crop)
"""

import logging

import numpy as np
from PIL import Image
from scoring import DETAIL_SCALE_PX

log = logging.getLogger(__name__)


def _background_ring(mask, dilation_frac=0.10):
    """Produce a background ring mask by dilating and subtracting the original.

    The ring width adapts to subject size: dilation radius = 10% of the
    mask's equivalent diameter (2 * sqrt(area / pi)).

    Args:
        mask: boolean array (H, W)
        dilation_frac: fraction of equivalent diameter to dilate (default 0.10)

    Returns:
        boolean array (H, W) — True for background ring pixels
    """
    from scipy.ndimage import binary_dilation

    mask_area = mask.sum()
    if mask_area == 0:
        return np.zeros_like(mask)

    equiv_diameter = 2.0 * np.sqrt(mask_area / np.pi)
    radius = max(1, int(equiv_diameter * dilation_frac))

    dilated = binary_dilation(mask, iterations=radius)
    ring = dilated & ~mask
    return ring


def _to_grayscale_array(image):
    """Convert a PIL Image to a float64 grayscale numpy array."""
    return np.array(image.convert("L"), dtype=np.float64)


def _tenengrad(gray, mask_region=None):
    """Compute Tenengrad (sum of squared Sobel gradients) on a grayscale image.

    Args:
        gray: float64 array (H, W)
        mask_region: optional boolean array (H, W) — only score these pixels

    Returns:
        float — mean squared gradient magnitude over the region
    """
    from scipy.ndimage import sobel

    gx = sobel(gray, axis=1)
    gy = sobel(gray, axis=0)
    gradient_sq = gx**2 + gy**2

    if mask_region is not None:
        pixels = gradient_sq[mask_region]
        if len(pixels) == 0:
            return 0.0
        return float(pixels.mean())
    return float(gradient_sq.mean())


def _multiscale_tenengrad(gray, mask_region, scales=(1.0, 0.5, 0.25)):
    """Compute Tenengrad at multiple scales and return the weighted mean.

    Larger scales capture coarse focus; smaller scales capture fine detail.
    Weights: linearly decreasing (most weight on full resolution).

    Args:
        gray: float64 array (H, W)
        mask_region: boolean array (H, W)
        scales: tuple of scale factors (1.0 = original resolution)

    Returns:
        float — weighted mean Tenengrad across scales
    """
    weights = np.linspace(1.0, 0.5, len(scales))
    weights /= weights.sum()

    total = 0.0
    for scale, w in zip(scales, weights):
        if scale == 1.0:
            total += w * _tenengrad(gray, mask_region)
        else:
            h, w_px = gray.shape
            new_h, new_w = max(1, int(h * scale)), max(1, int(w_px * scale))
            scaled_img = Image.fromarray(gray).resize(
                (new_w, new_h), Image.LANCZOS
            )
            scaled_gray = np.array(scaled_img, dtype=np.float64)

            # Scale the mask to match
            mask_img = Image.fromarray(mask_region.astype(np.uint8) * 255).resize(
                (new_w, new_h), Image.NEAREST
            )
            scaled_mask = np.array(mask_img) > 127

            total += w * _tenengrad(scaled_gray, scaled_mask)

    return total


def compute_subject_tenengrad(image, mask):
    """Multi-scale Tenengrad sharpness on the subject mask region.

    Args:
        image: PIL Image (should be original or high resolution for accuracy)
        mask: boolean array (H, W)

    Returns:
        float — sharpness score (higher = sharper)
    """
    gray = _to_grayscale_array(image)

    # Resize mask to match image if needed
    if gray.shape != mask.shape:
        mask_img = Image.fromarray(mask.astype(np.uint8) * 255)
        mask_img = mask_img.resize((gray.shape[1], gray.shape[0]), Image.NEAREST)
        mask = np.array(mask_img) > 127

    if not mask.any():
        return 0.0

    return round(_multiscale_tenengrad(gray, mask), 2)


def compute_bg_tenengrad(image, mask):
    """Tenengrad sharpness on the background ring around the subject.

    Background ring = dilated mask (by 10% of equivalent diameter) minus original mask.

    Args:
        image: PIL Image
        mask: boolean array (H, W)

    Returns:
        float — background sharpness score
    """
    gray = _to_grayscale_array(image)

    if gray.shape != mask.shape:
        mask_img = Image.fromarray(mask.astype(np.uint8) * 255)
        mask_img = mask_img.resize((gray.shape[1], gray.shape[0]), Image.NEAREST)
        mask = np.array(mask_img) > 127

    ring = _background_ring(mask)
    if not ring.any():
        return 0.0

    return round(_tenengrad(gray, ring), 2)


def compute_exposure_stats(image, mask):
    """Compute exposure statistics on the subject mask region.

    Args:
        image: PIL Image
        mask: boolean array (H, W)

    Returns:
        dict with:
            subject_clip_high: fraction of subject pixels > 250
            subject_clip_low: fraction of subject pixels < 5
            subject_y_median: median luminance (0-255) of subject
    """
    gray = _to_grayscale_array(image)

    if gray.shape != mask.shape:
        mask_img = Image.fromarray(mask.astype(np.uint8) * 255)
        mask_img = mask_img.resize((gray.shape[1], gray.shape[0]), Image.NEAREST)
        mask = np.array(mask_img) > 127

    subject_pixels = gray[mask]
    if len(subject_pixels) == 0:
        return {
            "subject_clip_high": 0.0,
            "subject_clip_low": 0.0,
            "subject_y_median": 0.0,
        }

    n = len(subject_pixels)
    return {
        "subject_clip_high": round(float((subject_pixels > 250).sum() / n), 4),
        "subject_clip_low": round(float((subject_pixels < 5).sum() / n), 4),
        "subject_y_median": round(float(np.median(subject_pixels)), 2),
    }


def compute_bg_separation(image, mask):
    """Compute background separation (pixel variance of background region).

    Lower variance = smoother bokeh = better background separation.
    The raw value is returned; normalization within encounters happens downstream.

    Args:
        image: PIL Image
        mask: boolean array (H, W)

    Returns:
        float — background pixel variance (higher = busier background)
    """
    gray = _to_grayscale_array(image)

    if gray.shape != mask.shape:
        mask_img = Image.fromarray(mask.astype(np.uint8) * 255)
        mask_img = mask_img.resize((gray.shape[1], gray.shape[0]), Image.NEAREST)
        mask = np.array(mask_img) > 127

    bg_pixels = gray[~mask]
    if len(bg_pixels) == 0:
        return 0.0

    return round(float(np.var(bg_pixels)), 2)


def compute_crop_phash(image, mask, hash_size=8):
    """Compute perceptual hash of the masked subject crop.

    Uses blur_background + crop_subject to produce a clean subject image,
    then computes a 64-bit pHash.

    Args:
        image: PIL Image
        mask: boolean array (H, W)
        hash_size: pHash grid size (default 8 → 64-bit hash)

    Returns:
        str — hex-encoded pHash, or None if crop fails
    """
    import imagehash
    from masking import blur_background, crop_subject

    # Produce masked crop: blur background, then crop to subject bbox
    masked = blur_background(image, mask, radius=51)
    crop = crop_subject(masked, mask, margin=0.15)
    if crop is None:
        return None

    return str(imagehash.phash(crop, hash_size=hash_size))


def compute_noise_estimate(image, mask):
    """Estimate image noise from smooth background regions.

    Uses the Laplacian variance on background pixels (outside the subject mask).
    In smooth background areas, high Laplacian variance indicates noise.
    Lower values = cleaner image.

    Args:
        image: PIL Image
        mask: boolean array (H, W)

    Returns:
        float — noise estimate (higher = noisier)
    """
    from scipy.ndimage import laplace

    gray = _to_grayscale_array(image)

    if gray.shape != mask.shape:
        mask_img = Image.fromarray(mask.astype(np.uint8) * 255)
        mask_img = mask_img.resize((gray.shape[1], gray.shape[0]), Image.NEAREST)
        mask = np.array(mask_img) > 127

    # Use background ring (not the full background — avoids bokeh blur
    # which would artificially lower the estimate)
    ring = _background_ring(mask, dilation_frac=0.15)
    if not ring.any():
        return 0.0

    # Laplacian on the ring region
    lap = laplace(gray)
    ring_values = lap[ring]

    return round(float(np.var(ring_values)), 2)


def compute_eye_tenengrad(image, eye_xy, bbox, k=0.08):
    """Multi-scale Tenengrad in a small window around an eye keypoint.

    Window side length = ``k * min(bbox_w, bbox_h)``, minimum 8 px, clamped
    to the image bounds. Reuses _multiscale_tenengrad so the raw scale of
    this value is directly comparable to subject_tenengrad (same operator,
    different region).

    Args:
        image: PIL.Image (original resolution).
        eye_xy: (x, y) eye keypoint in image-pixel space.
        bbox: (x0, y0, x1, y1) MegaDetector bbox, image-pixel space. Only
            chooses the window size proportional to the subject — it does
            not clamp the window (image bounds handle that).
        k: window side as fraction of min(bbox_w, bbox_h). Default 0.08.

    Returns:
        float — raw multi-scale Tenengrad in the window, 0.0 if the window
        is empty after clamping to image bounds.
    """
    x, y = eye_xy
    x0, y0, x1, y1 = bbox
    side = max(8, int(round(k * min(x1 - x0, y1 - y0))))
    half = side // 2
    img_w, img_h = image.size
    wx0 = max(0, int(x - half))
    wy0 = max(0, int(y - half))
    wx1 = min(img_w, int(x + half))
    wy1 = min(img_h, int(y + half))
    if wx1 <= wx0 or wy1 <= wy0:
        return 0.0
    window = image.crop((wx0, wy0, wx1, wy1))
    gray = _to_grayscale_array(window)
    mask = np.ones_like(gray, dtype=bool)
    return round(_multiscale_tenengrad(gray, mask), 2)


# Subject detail is the one sharpness measurement meant to be compared across
# encounters: the subject is resampled so its bounding box spans
# scoring.DETAIL_SCALE_PX (the scale the cross-encounter composite reads it
# at) before the gradients are summed, so a bird that filled the frame and
# one that was a speck in it are measured on the same footing (how much
# detail the frame holds when the bird is viewed at one size). The
# proxy-scale subject_tenengrad is not comparable that way: at a fixed frame
# scale, a small subject's edges fall on fewer pixels and read as sharper.
#
# DETAIL_MARGIN is the context kept around the subject box so the Sobel
# window at the mask edge reads real pixels, as a fraction of the box's
# width and height.
DETAIL_MARGIN = 0.08


def frame_size(photo):
    """A photo row's native ``(width, height)``, or None when unrecorded."""
    try:
        width, height = photo["width"], photo["height"]
    except (KeyError, IndexError):
        return None
    return (width, height) if width and height else None


def mask_bbox_fractions(mask):
    """The mask's bounding box as (x0, y0, x1, y1) fractions of its frame, or None when empty."""
    ys, xs = np.nonzero(mask)
    if len(ys) == 0:
        return None
    h, w = mask.shape
    return (xs.min() / w, ys.min() / h, (xs.max() + 1) / w, (ys.max() + 1) / h)


def detail_decode_edge(mask):
    """The longest frame edge, in pixels, that lets the subject span DETAIL_SCALE_PX.

    A frame decoded smaller than this is upsampled to the detail scale, so
    the measurement would read interpolated pixels rather than the detail
    the source holds. Callers cap it at the source's native size: a subject
    too small for the scale even at native resolution is measured upsampled,
    and the low value honestly reports how little detail the frame has.
    Returns None for an empty mask.
    """
    bbox = mask_bbox_fractions(mask)
    if bbox is None:
        return None
    x0, y0, x1, y1 = bbox
    mask_h, mask_w = mask.shape
    # The box's longer side as a fraction of the frame's longer side (the
    # mask shares the frame's aspect), so a tall box in a wide frame is not
    # read as a fraction of the short side.
    frac = max((x1 - x0) * mask_w, (y1 - y0) * mask_h) / max(mask_w, mask_h)
    return int(np.ceil(DETAIL_SCALE_PX / frac))


def compute_subject_detail(image, mask, frame_size=None):
    """Multi-scale Tenengrad over the subject resampled to DETAIL_SCALE_PX.

    The subject's bounding box (from ``mask``, at any resolution) is cropped
    from ``image`` with DETAIL_MARGIN of context, resampled so the box's
    longer side is DETAIL_SCALE_PX pixels (LANCZOS; the mask follows with
    NEAREST), and measured with the same operator as subject_tenengrad.

    Args:
        image: PIL Image of the whole frame, ideally decoded at or above
            ``detail_decode_edge(mask)`` so no interpolation is measured.
        mask: boolean array (H, W) of the subject, any resolution.
        frame_size: ``(width, height)`` of the native source, used only to
            report the subject's size in source pixels; defaults to ``image.size``.

    Returns:
        dict with ``subject_detail`` (the measurement; 0.0 for an empty
        mask) and ``subject_detail_px`` (the subject box's longer side in
        source pixels, None for an empty mask).
    """
    bbox = mask_bbox_fractions(mask)
    if bbox is None:
        return {"subject_detail": 0.0, "subject_detail_px": None}
    x0, y0, x1, y1 = bbox
    img_w, img_h = image.size
    native_w, native_h = frame_size or image.size
    box_w, box_h = (x1 - x0) * img_w, (y1 - y0) * img_h
    scale = DETAIL_SCALE_PX / max(box_w, box_h)
    mx, my = (x1 - x0) * DETAIL_MARGIN, (y1 - y0) * DETAIL_MARGIN
    cx0, cy0 = max(0.0, x0 - mx), max(0.0, y0 - my)
    cx1, cy1 = min(1.0, x1 + mx), min(1.0, y1 + my)
    crop = image.crop((int(cx0 * img_w), int(cy0 * img_h),
                       int(np.ceil(cx1 * img_w)), int(np.ceil(cy1 * img_h))))
    size = (max(1, round(crop.width * scale)), max(1, round(crop.height * scale)))
    gray = _to_grayscale_array(crop.resize(size, Image.LANCZOS))
    mask_h, mask_w = mask.shape
    region = Image.fromarray(mask.astype(np.uint8) * 255).crop(
        (int(cx0 * mask_w), int(cy0 * mask_h), int(np.ceil(cx1 * mask_w)), int(np.ceil(cy1 * mask_h))),
    ).resize(size, Image.NEAREST)
    region = np.array(region) > 127
    if not region.any():
        return {"subject_detail": 0.0, "subject_detail_px": None}
    return {
        "subject_detail": round(_multiscale_tenengrad(gray, region), 2),
        "subject_detail_px": round(max((x1 - x0) * native_w, (y1 - y0) * native_h), 1),
    }


def measure_subject_detail(proxy, mask, *, source_path, frame_size=None):
    """Subject detail from the proxy, or from a larger decode of the source.

    The proxy serves when the subject already spans DETAIL_SCALE_PX in it;
    a smaller subject reads ``source_path`` again at the edge the scale
    needs (the loader caps it at the native size). Returns the
    ``compute_subject_detail`` dict, or {} when the source cannot be read,
    so the measurement stays unmade rather than interpolated.
    """
    from image_loader import load_image

    needed = detail_decode_edge(mask)
    if needed is None:
        return {"subject_detail": 0.0, "subject_detail_px": None}
    if needed <= max(proxy.size):
        return compute_subject_detail(proxy, mask, frame_size=frame_size)
    image = load_image(source_path, max_size=needed)
    if image is None:
        return {}
    try:
        return compute_subject_detail(image, mask, frame_size=frame_size)
    finally:
        image.close()


def measure_cached_subject_detail(mask_path, source_path, frame_size=None):
    """Subject detail for a saved mask PNG whose row predates the measurement.

    Reads the mask from disk and the source at the edge the detail scale
    needs; SAM does not run. Returns the ``compute_subject_detail`` dict,
    or None when the mask or the source cannot be read.
    """
    from image_loader import load_image

    try:
        with Image.open(mask_path) as mask_image:
            mask = np.asarray(mask_image.convert("L")) > 127
    except OSError:
        log.warning("Could not read mask %s for the subject detail measurement", mask_path, exc_info=True)
        return None
    needed = detail_decode_edge(mask)
    if needed is None:
        return {"subject_detail": 0.0, "subject_detail_px": None}
    image = load_image(source_path, max_size=needed)
    if image is None:
        return None
    try:
        return compute_subject_detail(image, mask, frame_size=frame_size)
    finally:
        image.close()


def compute_all_quality_features(image, mask):
    """Compute all quality features for a photo in one call.

    Args:
        image: PIL Image (working resolution proxy or higher)
        mask: boolean array (H, W)

    Returns:
        dict with all feature values ready for
        ``db.masks_features.update_pipeline_features()``
    """
    exposure = compute_exposure_stats(image, mask)

    return {
        "subject_tenengrad": compute_subject_tenengrad(image, mask),
        "bg_tenengrad": compute_bg_tenengrad(image, mask),
        "subject_clip_high": exposure["subject_clip_high"],
        "subject_clip_low": exposure["subject_clip_low"],
        "subject_y_median": exposure["subject_y_median"],
        "bg_separation": compute_bg_separation(image, mask),
        "phash_crop": compute_crop_phash(image, mask),
        "noise_estimate": compute_noise_estimate(image, mask),
    }
