"""Experimental per-subject analysis from linear, 16-bit-derived RAW RGB.

All metering precedes rendering. These are *rendered RGB* clipping estimates,
not sensor saturation measurements. No original file or editing recipe changes.
"""

import logging
import os

import numpy as np
from PIL import Image
from scipy.ndimage import binary_erosion

log = logging.getLogger(__name__)
RECIPE = "linear-raw-subject-v1"


def interior_mask(mask):
    """Exclude a narrow boundary, retaining small/thin subjects when necessary."""
    mask = np.asarray(mask, dtype=bool)
    radius = max(1, min(8, round(np.sqrt(mask.sum() / np.pi) * 0.02)))
    core = binary_erosion(mask, iterations=radius)
    if core.sum() < max(16, mask.sum() * 0.25):
        return mask.copy(), False
    return core, True


def resize_linear(rgb, size):
    if (rgb.shape[1], rgb.shape[0]) == size:
        return rgb
    # Pillow RGB would quantize to 8 bits. Resize each float channel instead.
    return np.stack([
        np.asarray(Image.fromarray(rgb[..., c]).resize(size, Image.Resampling.BOX))
        for c in range(3)
    ], axis=-1)


def decode_linear(path, max_size=1536, *, return_native=False):
    """Decode RAW to a float proxy, optionally also returning native uint16 RGB.

    Unsupported files return None; an embedded JPEG is never called linear RAW.
    """
    from image_loader import RAW_EXTENSIONS, _is_bayer_sensor

    if os.path.splitext(path)[1].lower() not in RAW_EXTENSIONS:
        return None
    import rawpy

    try:
        with rawpy.imread(path) as raw:
            kwargs = dict(
                gamma=(1, 1), output_bps=16, no_auto_bright=True,
                use_camera_wb=True, bright=1.0,
                highlight_mode=rawpy.HighlightMode.Clip,
                half_size=not return_native and max(raw.sizes.width, raw.sizes.height) // 2 >= max_size,
            )
            if _is_bayer_sensor(raw):
                kwargs["demosaic_algorithm"] = rawpy.DemosaicAlgorithm.PPG
            rgb = raw.postprocess(**kwargs)
        # Bound float allocation: first resize the uint16 channels in float.
        h, w = rgb.shape[:2]
        scale = min(1.0, max_size / max(h, w))
        size = (max(1, round(w * scale)), max(1, round(h * scale)))
        linear = np.stack([
            np.asarray(Image.fromarray(rgb[..., c].astype(np.float32)).resize(
                size, Image.Resampling.BOX,
            )) / np.float32(65535)
            for c in range(3)
        ], axis=-1)
        return (linear, rgb) if return_native else linear
    except (rawpy.LibRawError, rawpy.NotSupportedError, OSError, ValueError):
        log.warning("Linear RAW decode unavailable for %s; using normal image path", path)
        return None


def meter_subject(linear, mask):
    """Choose a bounded EV shift without forcing all plumage to middle gray."""
    mask = np.asarray(mask, dtype=bool)
    if mask.shape != linear.shape[:2]:
        raise ValueError("Subject mask and linear image dimensions must match")
    if not np.isfinite(linear).all() or np.any(linear < 0):
        raise ValueError("Linear RGB must be finite and nonnegative")
    core, eroded = interior_mask(mask)
    pixels = linear[core]
    if not len(pixels):
        return {"status": "empty_mask", "exposure_ev": 0.0, "sample_count": 0}
    luminance = pixels @ np.array([0.2126, 0.7152, 0.0722], dtype=np.float32)
    median = float(np.median(luminance))
    # A broad acceptable interval retains dark/white plumage. Exposure alone
    # cannot distinguish dark reflectance from underexposure; boosts stay small.
    target = float(np.clip(median, 0.08, 0.65))
    ev = float(np.clip(np.log2(target / max(median, 1e-6)), -2.0, 2.0))
    high = float(np.percentile(pixels.max(axis=1), 99))
    if ev > 0 and high > 0:
        ev = min(ev, max(0.0, float(np.log2(0.98 / high))))
    # Black pixels contain no recoverable signal. Do not report a fake rescue.
    if median <= 1e-6:
        ev = 0.0
    return {
        "status": "ok", "exposure_ev": ev, "sample_count": int(len(pixels)),
        "boundary_excluded": eroded, "linear_y_median": median,
        "rendered_clip_high": float(np.mean(pixels.max(axis=1) >= 0.995)),
        "rendered_clip_high_full_mask": float(np.mean(linear[mask].max(axis=1) >= 0.995)),
        "linear_clip_low": float(np.mean(luminance <= 1.0 / 65535)),
    }


def render_linear(linear, exposure_ev=0.0):
    """Apply exposure in linear light, then encode standard sRGB for models."""
    rgb = np.clip(linear * np.float32(2.0 ** exposure_ev), 0, 1)
    srgb = np.where(rgb <= 0.0031308, 12.92 * rgb, 1.055 * rgb ** (1 / 2.4) - 0.055)
    return Image.fromarray(np.rint(srgb * 255).astype(np.uint8))


def analyze_subject(linear, mask):
    from quality import compute_all_quality_features

    meter = meter_subject(linear, mask)
    original = render_linear(linear)
    corrected = render_linear(linear, meter["exposure_ev"])
    try:
        original_quality = compute_all_quality_features(original, mask)
        corrected_quality = compute_all_quality_features(corrected, mask)
        core, _ = interior_mask(mask)
        if core.any():
            # Preserve the legacy 0–255 exposure score units, but avoid sky
            # contamination in the highlight term on this analysis path.
            for img, features in ((original, original_quality), (corrected, corrected_quality)):
                gray = np.asarray(img.convert("L"))
                features["subject_clip_high"] = round(float(np.mean(gray[core] > 250)), 4)
    except Exception:
        corrected.close()
        raise
    finally:
        original.close()
    report = {
        "recipe": RECIPE, **meter,
        "analysis_width": int(linear.shape[1]), "analysis_height": int(linear.shape[0]),
        "frame_linear_y_median": float(np.median(
            linear @ np.array([0.2126, 0.7152, 0.0722], dtype=np.float32)
        )),
        "original_quality": original_quality,
        "corrected_quality": corrected_quality,
    }
    return corrected, report


def scoring_features(report):
    """Corrected detail/noise scores with exposure defects from before correction."""
    features = dict(report["corrected_quality"])
    for key in ("subject_clip_high", "subject_clip_low", "subject_y_median"):
        features[key] = report["original_quality"][key]
    return features


class RawAnalysisSession:
    """One-frame cache, owned by a pipeline stage; no process-global image state."""

    def __init__(self, max_size=1536, sam2_variant="sam2-small", preserve_detail=False):
        self.max_size = max_size
        self.sam2_variant = sam2_variant
        self.preserve_detail = preserve_detail
        self._key = None
        self._linear = None
        self._native = None
        self._weights_ready = False
        self._weights_failed = False

    def load(self, path):
        try:
            stat = os.stat(path)
        except OSError:
            self._key, self._linear = None, None
            self._native = None
            return None
        key = (os.path.realpath(path), stat.st_size, stat.st_mtime_ns)
        if key != self._key:
            self._linear = None
            self._native = None
            self._key = key
            if self.preserve_detail:
                decoded = decode_linear(path, self.max_size, return_native=True)
                if decoded is not None:
                    self._linear, self._native = decoded
            else:
                self._linear = decode_linear(path, self.max_size)
        return self._linear

    def source_metadata(self):
        return {
            "source_mtime_ns": self._key[2], "source_size": self._key[1],
            "sam2_variant": self.sam2_variant,
        }

    def prepare(self, path, detection):
        """Return a corrected frame and report; normal loader handles fallbacks."""
        if self._weights_failed or detection is None or dict(detection).get("detector_model") == "full-image":
            return None, None
        linear = self.load(path)
        if linear is None:
            return None, None
        from image_loader import load_image
        from masking import ensure_sam2_weights, generate_mask
        from resource_ledger import ResourceWaitCancelled

        try:
            if not self._weights_ready:
                ensure_sam2_weights(self.sam2_variant)
                self._weights_ready = True

            # Segment a familiar camera-rendered preview; map its mask to the
            # oriented RAW frame. Never map across mismatched aspect ratios.
            preview = load_image(path, max_size=self.max_size)
            if preview is None:
                return None, None
            try:
                h, w = linear.shape[:2]
                if abs(preview.width / preview.height - w / h) > 0.02:
                    return None, None
                resized = preview.resize((w, h), Image.Resampling.LANCZOS)
                try:
                    box = {k: detection["box_" + k] for k in ("x", "y", "w", "h")}
                    mask = generate_mask(resized, box, variant=self.sam2_variant)
                finally:
                    resized.close()
            finally:
                preview.close()
        except ResourceWaitCancelled:
            # Cooperative shutdown is not a masking failure. Re-raise so the
            # pipeline stage aborts promptly.
            raise
        except Exception:
            if not self._weights_ready:
                # A missing/offline model cannot recover per subject. Retry on
                # the next pipeline session, without repeating network waits.
                self._weights_failed = True
            # Unavailable weights, corrupt ONNX, or an inference error must
            # not abort classification — the documented fallback is the
            # normal image path.
            log.warning(
                "Subject masking unavailable for %s; using normal image path",
                path, exc_info=True,
            )
            return None, None
        if mask is None or not mask.any():
            log.warning("No usable subject mask for %s; using normal image path", path)
            return None, None
        corrected, report = analyze_subject(linear, mask)
        if self._native is not None:
            # Meter at proxy resolution, but crop native 16-bit pixels before
            # reducing to the classifier's working size. Small birds otherwise
            # lose most of their feather detail during whole-frame resizing.
            corrected.close()
            h, w = self._native.shape[:2]
            x, y, bw, bh = (detection["box_" + k] for k in ("x", "y", "w", "h"))
            x1, y1 = max(0, int((x - bw * 0.2) * w)), max(0, int((y - bh * 0.2) * h))
            x2, y2 = min(w, int((x + bw * 1.2) * w)), min(h, int((y + bh * 1.2) * h))
            native_crop = self._native[y1:y2, x1:x2]
            if min(native_crop.shape[:2]) < 50:
                native_crop = self._native
            ch, cw = native_crop.shape[:2]
            scale = min(1, 1024 / max(ch, cw))
            size = (max(1, round(cw * scale)), max(1, round(ch * scale)))
            crop_linear = np.stack([
                np.asarray(Image.fromarray(native_crop[..., c].astype(np.float32)).resize(
                    size, Image.Resampling.BOX,
                )) / np.float32(65535) for c in range(3)
            ], axis=-1)
            corrected = render_linear(crop_linear, report["exposure_ev"])
            corrected.info["_vireo_subject_crop"] = True
        report.update(self.source_metadata())
        return corrected, report
