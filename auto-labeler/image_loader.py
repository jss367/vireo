"""Load images from various formats (JPEG, PNG, TIFF, NEF, CR2, ARW, etc.)."""

import logging
from pathlib import Path
from PIL import Image

log = logging.getLogger(__name__)

RAW_EXTENSIONS = {'.nef', '.cr2', '.cr3', '.arw', '.raf', '.dng', '.rw2', '.orf'}
IMAGE_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.tiff', '.tif', '.bmp', '.webp'}
SUPPORTED_EXTENSIONS = IMAGE_EXTENSIONS | RAW_EXTENSIONS


def load_image(file_path, max_size=1024):
    """Load an image file and return a PIL Image, resized to max_size.

    Supports JPEG, PNG, TIFF, and RAW formats (NEF, CR2, ARW, etc.).
    Returns None if the file cannot be loaded.

    Args:
        file_path: Path to the image file
        max_size: Maximum dimension (longest side). None to skip resizing.

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
            raw = rawpy.imread(str(path))
            rgb = raw.postprocess()
            img = Image.fromarray(rgb)
        else:
            img = Image.open(str(path))
            img = img.convert('RGB')

        if max_size and max(img.size) > max_size:
            img.thumbnail((max_size, max_size))

        return img
    except Exception:
        log.warning("Failed to load image: %s", file_path)
        return None
