"""Load images from various formats (JPEG, PNG, TIFF, NEF, CR2, ARW, etc.).

Performance notes:
- RAW decode is the bottleneck (~1.7s full, ~0.5s half-size for a 45MP NEF)
- We use half_size=True when the target is ≤ half the sensor resolution (3x faster)
- PIL resize and JPEG encode are negligible (<0.15s)
- libraw (via rawpy) is already C — Rust/numba won't help here
"""

import logging
from pathlib import Path

from PIL import Image

log = logging.getLogger(__name__)

RAW_EXTENSIONS = {".nef", ".cr2", ".cr3", ".arw", ".raf", ".dng", ".rw2", ".orf"}
IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".tiff", ".tif", ".bmp", ".webp"}
SUPPORTED_EXTENSIONS = IMAGE_EXTENSIONS | RAW_EXTENSIONS


def load_image(file_path, max_size=1024):
    """Load an image file and return a PIL Image, resized to max_size.

    Supports JPEG, PNG, TIFF, and RAW formats (NEF, CR2, ARW, etc.).
    Uses half-size RAW decoding when possible (3x faster for large files).
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
            import rawpy

            with rawpy.imread(str(path)) as raw:
                # Use half_size decode when we don't need full resolution.
                # Half-size is ~3x faster and still produces ~4000x2700 for a 45MP sensor.
                # Only use full decode for 1:1 (max_size=None/0) or very large targets.
                use_half = False
                if max_size and max_size > 0:
                    sensor_long = max(raw.sizes.width, raw.sizes.height)
                    half_long = sensor_long // 2
                    # If target fits within half-size output, use half decode
                    if max_size <= half_long:
                        use_half = True

                rgb = raw.postprocess(half_size=use_half)
            img = Image.fromarray(rgb)
        else:
            img = Image.open(str(path))
            img = img.convert("RGB")

        if max_size and max_size > 0 and max(img.size) > max_size:
            img.thumbnail((max_size, max_size), Image.LANCZOS)

        return img
    except Exception as e:
        log.warning("Failed to load image: %s — %s", file_path, e)
        return None
