# vireo/tests/test_image_loader.py
import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))


def test_load_jpeg():
    """load_image returns a PIL Image for a JPEG file."""
    from image_loader import load_image
    from PIL import Image

    with tempfile.NamedTemporaryFile(suffix='.jpg', delete=False) as f:
        img = Image.new('RGB', (100, 100), color='red')
        img.save(f.name)
        result = load_image(f.name)
        assert isinstance(result, Image.Image)
        assert result.size == (100, 100)
        os.unlink(f.name)


def test_load_image_resizes_large():
    """load_image resizes images with longest side > max_size."""
    from image_loader import load_image
    from PIL import Image

    with tempfile.NamedTemporaryFile(suffix='.jpg', delete=False) as f:
        img = Image.new('RGB', (4000, 2000), color='blue')
        img.save(f.name)
        result = load_image(f.name, max_size=1024)
        assert max(result.size) == 1024
        assert result.size == (1024, 512)
        os.unlink(f.name)


def test_load_unsupported_returns_none():
    """load_image returns None for unsupported file types."""
    from image_loader import load_image

    with tempfile.NamedTemporaryFile(suffix='.txt', delete=False) as f:
        f.write(b"not an image")
        f.flush()
        result = load_image(f.name)
        assert result is None
        os.unlink(f.name)


def test_load_corrupt_file_returns_none():
    """load_image returns None for a corrupt image file."""
    from image_loader import load_image

    with tempfile.NamedTemporaryFile(suffix='.jpg', delete=False) as f:
        f.write(b"not a real jpeg")
        f.flush()
        result = load_image(f.name)
        assert result is None
        os.unlink(f.name)
