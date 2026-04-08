"""Load images from various formats (JPEG, PNG, TIFF, NEF, CR2, ARW, etc.).

Performance notes:
- RAW decode is the bottleneck (~1.7s full, ~0.5s half-size for a 45MP NEF)
- We use half_size=True when the target is ≤ half the sensor resolution (3x faster)
- PIL resize and JPEG encode are negligible (<0.15s)
- libraw (via rawpy) is already C — Rust/numba won't help here

RAW strategy (JPEG-first):
- Modern cameras embed a full-resolution JPEG in the RAW file (the same image
  the camera would produce in RAW+JPEG mode). For a photo organizer, that's
  both faster to decode and sufficient in quality.
- It also works for RAW variants libraw cannot decode. Example: Nikon Z 8
  "High Efficiency*" (HE*) files use TicoRAW compression that libraw 0.22
  cannot decode. The embedded JPEG is our only path for those files.
- We prefer the embedded JPEG whenever it meets the requested size, and
  fall back to it when demosaic-based decode raises.
"""

import io
import logging
import os
from pathlib import Path

from PIL import Image, ImageOps

log = logging.getLogger(__name__)

RAW_EXTENSIONS = {".nef", ".cr2", ".cr3", ".arw", ".raf", ".dng", ".rw2", ".orf"}
IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".tiff", ".tif", ".bmp", ".webp"}
SUPPORTED_EXTENSIONS = IMAGE_EXTENSIONS | RAW_EXTENSIONS


def load_image(file_path, max_size=1024):
    """Load an image file and return a PIL Image, resized to max_size.

    Supports JPEG, PNG, TIFF, and RAW formats (NEF, CR2, ARW, etc.).
    For RAW files, prefers the embedded full-res JPEG preview when it meets
    the requested size; falls back to demosaic-based decode otherwise.
    Returns None if the file cannot be loaded.

    Args:
        file_path: Path to the image file
        max_size: Maximum dimension (longest side). None or 0 for full resolution.

    Returns:
        PIL.Image.Image or None
    """
    path = Path(file_path)
    ext = path.suffix.lower()

    if ext not in SUPPORTED_EXTENSIONS:
        return None

    try:
        if ext in RAW_EXTENSIONS:
            img = _load_raw(path, max_size)
        else:
            img = Image.open(str(path))
            img = ImageOps.exif_transpose(img)
            img = img.convert("RGB")

        if img is None:
            return None

        if max_size and max_size > 0 and max(img.size) > max_size:
            img.thumbnail((max_size, max_size), Image.LANCZOS)

        return img
    except Exception as e:
        log.warning("Failed to load image: %s — %s", file_path, e)
        return None


def _load_standard(path, max_size):
    """Load a standard image file (JPEG, PNG, TIFF, etc.) via PIL.

    Opens the file, converts to RGB, and resizes to max_size if needed.
    This is the fast path — no RAW decoding involved.

    Args:
        path: path to the image file
        max_size: maximum dimension (longest side). None or 0 for full resolution.

    Returns:
        PIL.Image.Image or None
    """
    try:
        img = Image.open(str(path))
        img = ImageOps.exif_transpose(img)
        img = img.convert("RGB")
        if max_size and max_size > 0 and max(img.size) > max_size:
            img.thumbnail((max_size, max_size), Image.LANCZOS)
        return img
    except Exception as e:
        log.warning("Failed to load standard image: %s — %s", path, e)
        return None


def load_working_image(photo, vireo_dir, max_size=1024, folders=None):
    """Load a photo's working image — the fast path for all pixel operations.

    Uses the pre-extracted working copy JPEG if available,
    otherwise falls back to loading the original file directly.

    Args:
        photo: photo dict with working_copy_path, folder_id, filename
        vireo_dir: path to ~/.vireo/
        max_size: maximum dimension (longest side). None for full resolution.
        folders: optional {folder_id: path} mapping (required when working_copy_path is NULL)

    Returns:
        PIL.Image.Image or None
    """
    if photo.get("working_copy_path"):
        wc_path = os.path.join(vireo_dir, photo["working_copy_path"])
        if os.path.exists(wc_path):
            return _load_standard(wc_path, max_size)

    # No working copy — load original (may be JPEG or RAW)
    if folders is None:
        return None
    folder_path = folders.get(photo["folder_id"], "")
    source_path = os.path.join(folder_path, photo["filename"])
    return load_image(source_path, max_size)


def extract_working_copy(source_path, output_path, max_size=4096, quality=92):
    """Extract a JPEG working copy from an image file.

    Args:
        source_path: path to source image (RAW or JPEG)
        output_path: where to save the working copy JPEG
        max_size: max dimension (longest side). 0 or None for full resolution.
        quality: JPEG quality (1-95)

    Returns:
        True on success, False on failure
    """
    try:
        img = load_image(source_path, max_size=max_size or None)
        if img is None:
            return False
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        img.save(output_path, "JPEG", quality=quality)
        return True
    except Exception:
        log.warning("Failed to extract working copy from %s", source_path,
                    exc_info=True)
        return False


def _load_raw(path, max_size):
    """Load a RAW file using JPEG-first strategy.

    1. Try the embedded JPEG preview; use it if it's big enough for max_size.
    2. Otherwise demosaic via rawpy.postprocess().
    3. If postprocess fails, fall back to the embedded JPEG (even if small).
    """
    import rawpy

    with rawpy.imread(str(path)) as raw:
        embedded = _extract_embedded_jpeg(raw)

        # JPEG-first: if the embedded preview is large enough for the request,
        # use it and skip the slower RAW decode entirely.
        if (embedded is not None and max_size and max_size > 0
                and max(embedded.size) >= max_size):
            return embedded

        # Otherwise demosaic the sensor data, falling back to the embedded
        # JPEG if libraw can't decode this RAW variant.
        try:
            return _postprocess_raw(raw, max_size)
        except Exception as e:
            if embedded is not None:
                log.info(
                    "RAW decode failed for %s, using embedded JPEG (%dx%d): %s",
                    path, embedded.size[0], embedded.size[1], e,
                )
                return embedded
            raise


def _extract_embedded_jpeg(raw):
    """Return the embedded JPEG preview as a PIL Image, or None if unavailable."""
    import rawpy
    try:
        thumb = raw.extract_thumb()
    except Exception:
        return None
    if thumb.format != rawpy.ThumbFormat.JPEG:
        return None
    try:
        img = Image.open(io.BytesIO(thumb.data))
        img.load()
        # Apply EXIF orientation so portrait/rotated RAW files are upright.
        # Without this, cameras that record orientation in the EXIF header
        # (which is common in embedded JPEGs) would be returned sideways.
        img = ImageOps.exif_transpose(img)
        return img.convert("RGB")
    except Exception:
        return None


def _postprocess_raw(raw, max_size):
    """Demosaic raw sensor data into a PIL Image.

    Uses half-size decode when the target fits, which is ~3x faster and still
    produces ~4000x2700 for a 45MP sensor.
    """
    use_half = False
    if max_size and max_size > 0:
        sensor_long = max(raw.sizes.width, raw.sizes.height)
        half_long = sensor_long // 2
        if max_size <= half_long:
            use_half = True
    rgb = raw.postprocess(half_size=use_half)
    return Image.fromarray(rgb)
