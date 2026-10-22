"""The application loader's supported source formats; never a generic Pillow fallback."""
from pathlib import Path

RAW_EXTENSIONS = {'.nef', '.cr2', '.cr3', '.arw', '.raf', '.dng', '.rw2', '.orf'}
IMAGE_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.tiff', '.tif', '.bmp', '.webp'}
SUPPORTED_EXTENSIONS = IMAGE_EXTENSIONS | RAW_EXTENSIONS


def source_kind_for_path(path):
    extension = Path(path).suffix.lower()
    if extension not in SUPPORTED_EXTENSIONS:
        raise ValueError(f'Unsupported application source format {extension or "(none)"}; no generic Pillow fallback')
    return 'original_embedded_jpeg' if extension in RAW_EXTENSIONS else 'original_raster'
