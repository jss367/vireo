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


def _jpeg_bytes_with_orientation(size, orientation, color='green'):
    """Return JPEG bytes encoding an image with the given EXIF orientation tag.

    orientation: integer EXIF orientation value (1–8).
    The pixel data is stored with dimensions matching `size`, but the EXIF
    header instructs viewers to apply a rotation/flip.  For example,
    orientation=6 means "rotate 90° CW", so a (100, 200) stored image
    should display as (200, 100) after transposition.
    """
    from PIL import Image

    img = Image.new('RGB', size, color=color)
    exif = img.getexif()
    exif[0x0112] = orientation  # EXIF Orientation tag
    buf = io.BytesIO()
    img.save(buf, format='JPEG', exif=exif.tobytes())
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


def test_embedded_jpeg_exif_orientation_applied(tmp_path, monkeypatch):
    """Embedded JPEG EXIF orientation must be applied so portrait RAWs aren't sideways.

    This is a regression test for the fix to the JPEG-first path: without
    ImageOps.exif_transpose the returned image could be rotated 90° relative
    to its correct orientation.  We encode a 100x200 landscape image with
    EXIF orientation=6 ("rotate 90° CW") and expect the extracted image to
    come back as 200x100 after transposition.
    """
    from image_loader import load_image

    nef = tmp_path / "test.nef"
    nef.write_bytes(b"fake NEF content")

    # Store a 2000x1000 image with EXIF orientation=6 ("rotate 90° CW").
    # After transposition the correct display dimensions become 1000x2000.
    # max_size=1920 is smaller than max(2000, 1000)=2000, so the JPEG-first
    # path is taken (postprocess is skipped entirely).
    _install_fake_raw(monkeypatch, _FakeRaw(
        embedded_jpeg=_jpeg_bytes_with_orientation((2000, 1000), orientation=6),
    ))

    result = load_image(str(nef), max_size=1920)

    assert result is not None
    assert result.mode == "RGB"
    # After EXIF transpose (90° CW): 2000×1000 stored → 1000×2000 display.
    # load_image then thumbnails the longest side (2000) down to 1920,
    # giving (960, 1920).
    assert result.size == (960, 1920), (
        f"Expected (960, 1920) after EXIF orientation transpose + resize, got {result.size}"
    )


# ── extract_working_copy tests ──────────────────────────────────────────


def test_extract_working_copy_basic(tmp_path):
    """extract_working_copy saves a capped JPEG from a standard image file."""
    from image_loader import extract_working_copy
    from PIL import Image

    # Create a real JPEG source image
    source = tmp_path / "photo.jpg"
    img = Image.new("RGB", (5000, 3333), color="blue")
    img.save(str(source), "JPEG")

    output = tmp_path / "working" / "42.jpg"
    result = extract_working_copy(str(source), str(output), max_size=4096, quality=92)

    assert result is True
    assert output.exists()
    out_img = Image.open(str(output))
    assert max(out_img.size) <= 4096


def test_extract_working_copy_full_resolution(tmp_path):
    """extract_working_copy at max_size=0 preserves full resolution."""
    from image_loader import extract_working_copy
    from PIL import Image

    source = tmp_path / "photo.jpg"
    img = Image.new("RGB", (6000, 4000), color="green")
    img.save(str(source), "JPEG")

    output = tmp_path / "working" / "42.jpg"
    result = extract_working_copy(str(source), str(output), max_size=0, quality=92)

    assert result is True
    out_img = Image.open(str(output))
    assert out_img.size == (6000, 4000)


def test_extract_working_copy_missing_source_returns_false(tmp_path):
    """extract_working_copy returns False when the source file does not exist."""
    from image_loader import extract_working_copy

    output = tmp_path / "working" / "42.jpg"
    result = extract_working_copy(
        str(tmp_path / "nonexistent.jpg"), str(output), max_size=4096
    )

    assert result is False
    assert not output.exists()
