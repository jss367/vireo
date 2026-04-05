# vireo/tests/test_image_loader.py
import io
import os
import sys
import tempfile
from types import SimpleNamespace

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))


def _jpeg_bytes(size, color='green'):
    """Return JPEG-encoded bytes of an image with the given size."""
    from PIL import Image
    buf = io.BytesIO()
    Image.new('RGB', size, color=color).save(buf, format='JPEG')
    return buf.getvalue()


class _FakeRaw:
    """Stand-in for a rawpy raw-file handle, used via monkeypatched imread."""

    def __init__(self, *, embedded_jpeg=None, postprocess_error=None,
                 postprocess_size=(6000, 4000), sensor_size=(6000, 4000)):
        self._embedded_jpeg = embedded_jpeg
        self._postprocess_error = postprocess_error
        self._postprocess_size = postprocess_size
        self.sizes = SimpleNamespace(width=sensor_size[0], height=sensor_size[1])
        self.postprocess_calls = 0

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def extract_thumb(self):
        import rawpy
        if self._embedded_jpeg is None:
            raise rawpy.LibRawNoThumbnailError(b'no thumbnail')
        return SimpleNamespace(format=rawpy.ThumbFormat.JPEG,
                               data=self._embedded_jpeg)

    def postprocess(self, half_size=False):
        self.postprocess_calls += 1
        if self._postprocess_error is not None:
            raise self._postprocess_error
        import numpy as np
        w, h = self._postprocess_size
        if half_size:
            w, h = w // 2, h // 2
        return np.zeros((h, w, 3), dtype=np.uint8)


def _install_fake_raw(monkeypatch, fake_raw):
    """Patch rawpy.imread to return the given fake, regardless of path."""
    import rawpy
    monkeypatch.setattr(rawpy, 'imread', lambda path: fake_raw)
    return fake_raw


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


def test_raw_uses_embedded_jpeg_when_large_enough(tmp_path, monkeypatch):
    """JPEG-first: when the embedded JPEG ≥ max_size, use it and skip decode."""
    from image_loader import load_image

    nef = tmp_path / "test.nef"
    nef.write_bytes(b"fake NEF content")

    fake = _install_fake_raw(monkeypatch, _FakeRaw(
        embedded_jpeg=_jpeg_bytes((4000, 2000)),
    ))

    result = load_image(str(nef), max_size=1920)

    assert result is not None
    assert max(result.size) == 1920  # downscaled from 4000
    assert result.size == (1920, 960)
    assert fake.postprocess_calls == 0, "postprocess should be skipped"


def test_raw_falls_back_to_embedded_on_postprocess_failure(tmp_path, monkeypatch):
    """HE* case: postprocess raises, but we still return the embedded JPEG."""
    import rawpy
    from image_loader import load_image

    nef = tmp_path / "test.nef"
    nef.write_bytes(b"fake NEF content")

    # Embedded JPEG is too small for max_size, so postprocess is attempted.
    # Postprocess fails (simulating libraw's HE* decoder failure).
    # Expect fallback to the embedded JPEG, even though it's smaller.
    _install_fake_raw(monkeypatch, _FakeRaw(
        embedded_jpeg=_jpeg_bytes((1600, 1067)),
        postprocess_error=rawpy.LibRawFileUnsupportedError(
            b'Unsupported file format or not RAW file'
        ),
    ))

    result = load_image(str(nef), max_size=2048)

    assert result is not None
    assert max(result.size) == 1600, "should return the embedded JPEG as-is"


def test_raw_uses_postprocess_when_embedded_too_small(tmp_path, monkeypatch):
    """When embedded JPEG is smaller than max_size, RAW decode is used."""
    from image_loader import load_image

    nef = tmp_path / "test.nef"
    nef.write_bytes(b"fake NEF content")

    fake = _install_fake_raw(monkeypatch, _FakeRaw(
        embedded_jpeg=_jpeg_bytes((800, 533)),
        postprocess_size=(6000, 4000),
    ))

    result = load_image(str(nef), max_size=2048)

    assert result is not None
    assert fake.postprocess_calls == 1
    assert max(result.size) == 2048  # downscaled from postprocess output


def test_raw_returns_none_when_no_embedded_and_postprocess_fails(tmp_path, monkeypatch):
    """No embedded JPEG and postprocess fails → return None (current contract)."""
    import rawpy
    from image_loader import load_image

    nef = tmp_path / "test.nef"
    nef.write_bytes(b"fake NEF content")

    _install_fake_raw(monkeypatch, _FakeRaw(
        embedded_jpeg=None,
        postprocess_error=rawpy.LibRawFileUnsupportedError(b'broken'),
    ))

    result = load_image(str(nef), max_size=1024)
    assert result is None
