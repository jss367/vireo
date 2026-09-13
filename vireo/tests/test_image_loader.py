# vireo/tests/test_image_loader.py
import contextlib
import io
import os
import sys
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
        self.postprocess_kwargs = []

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

    def postprocess(self, half_size=False, **kwargs):
        self.postprocess_calls += 1
        self.postprocess_kwargs.append({"half_size": half_size, **kwargs})
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


def test_load_jpeg(tmp_path):
    """load_image returns a PIL Image for a JPEG file."""
    from image_loader import load_image
    from PIL import Image

    path = tmp_path / "photo.jpg"
    img = Image.new('RGB', (100, 100), color='red')
    img.save(path)
    result = load_image(path)
    assert isinstance(result, Image.Image)
    assert result.size == (100, 100)


def test_load_image_resizes_large(tmp_path):
    """load_image resizes images with longest side > max_size."""
    from image_loader import load_image
    from PIL import Image

    path = tmp_path / "large.jpg"
    img = Image.new('RGB', (4000, 2000), color='blue')
    img.save(path)
    result = load_image(path, max_size=1024)
    assert max(result.size) == 1024
    assert result.size == (1024, 512)


def test_load_unsupported_returns_none(tmp_path):
    """load_image returns None for unsupported file types."""
    from image_loader import load_image

    path = tmp_path / "not-image.txt"
    path.write_bytes(b"not an image")
    result = load_image(path)
    assert result is None


def test_load_corrupt_file_returns_none(tmp_path):
    """load_image returns None for a corrupt image file."""
    from image_loader import load_image

    path = tmp_path / "corrupt.jpg"
    path.write_bytes(b"not a real jpeg")
    result = load_image(path)
    assert result is None


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


def test_raw_camera_rendered_mode_uses_near_full_embedded_jpeg(
    tmp_path, monkeypatch,
):
    """Full-size lightbox display keeps the camera rendition used by previews."""
    from image_loader import RAW_DECODE_CAMERA_RENDERED, load_image

    nef = tmp_path / "test.nef"
    nef.write_bytes(b"fake NEF content")

    fake = _install_fake_raw(monkeypatch, _FakeRaw(
        embedded_jpeg=_jpeg_bytes((8256, 5504), color="green"),
        sensor_size=(8288, 5520),
        postprocess_size=(8288, 5520),
    ))

    result = load_image(
        str(nef), max_size=None, raw_decode=RAW_DECODE_CAMERA_RENDERED,
    )

    assert result is not None
    assert result.size == (8256, 5504)
    assert fake.postprocess_calls == 0


def test_raw_camera_rendered_mode_rejects_cropped_embedded_jpeg(
    tmp_path, monkeypatch,
):
    """A matching long edge cannot hide a substantially cropped short edge."""
    from image_loader import RAW_DECODE_CAMERA_RENDERED, load_image

    nef = tmp_path / "test.nef"
    nef.write_bytes(b"fake NEF content")

    fake = _install_fake_raw(monkeypatch, _FakeRaw(
        embedded_jpeg=_jpeg_bytes((6000, 3376)),
        sensor_size=(6000, 4000),
        postprocess_size=(6000, 4000),
    ))

    result = load_image(
        str(nef), max_size=None, raw_decode=RAW_DECODE_CAMERA_RENDERED,
    )

    assert result is not None
    assert result.size == (6000, 4000)
    assert fake.postprocess_calls == 1


def test_raw_preserve_highlights_mode_bypasses_embedded_jpeg(tmp_path, monkeypatch):
    """Edit-quality RAW loads demosaic instead of using camera-rendered JPEGs."""
    import rawpy
    from image_loader import RAW_DECODE_PRESERVE_HIGHLIGHTS, load_image

    nef = tmp_path / "test.nef"
    nef.write_bytes(b"fake NEF content")

    fake = _install_fake_raw(monkeypatch, _FakeRaw(
        embedded_jpeg=_jpeg_bytes((4000, 2000)),
        postprocess_size=(6000, 4000),
    ))

    result = load_image(
        str(nef),
        max_size=1920,
        raw_decode=RAW_DECODE_PRESERVE_HIGHLIGHTS,
    )

    assert result is not None
    assert fake.postprocess_calls == 1
    assert max(result.size) == 1920
    kwargs = fake.postprocess_kwargs[-1]
    assert kwargs["half_size"] is True
    assert kwargs["use_camera_wb"] is True
    assert kwargs["no_auto_bright"] is True
    assert kwargs["bright"] == 1.0
    assert kwargs["highlight_mode"] == rawpy.HighlightMode.Blend


def test_raw_demosaic_uses_ppg(tmp_path, monkeypatch):
    """Full-size decodes demosaic with PPG, not libraw's default AHD.

    AHD costs ~1.7s on a 45MP NEF against PPG's ~1.0s for output that
    matches to 40-52 dB PSNR, so PPG is the default for every demosaic
    Vireo runs. Guards the kwarg because nothing downstream would fail
    visibly if it were dropped — the decode would just get slower again.
    """
    import rawpy
    from image_loader import load_image

    nef = tmp_path / "test.nef"
    nef.write_bytes(b"fake NEF content")

    fake = _install_fake_raw(monkeypatch, _FakeRaw(
        embedded_jpeg=_jpeg_bytes((1600, 1067)),
        postprocess_size=(6000, 4000),
    ))

    result = load_image(str(nef), max_size=None)

    assert result is not None
    assert fake.postprocess_calls == 1
    kwargs = fake.postprocess_kwargs[-1]
    assert kwargs["demosaic_algorithm"] == rawpy.DemosaicAlgorithm.PPG
    assert kwargs["half_size"] is False


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


def test_raw_fallback_log_claims_full_output_only_when_embedded_matches_sensor(
    tmp_path, monkeypatch, caplog
):
    """The fallback log line must only claim "full camera output" when the
    embedded JPEG actually matches the sensor's reported dimensions.

    Regression guard for PR #764: an earlier revision asserted "full camera
    output" unconditionally, which is misleading when the embedded preview
    is genuinely smaller than the sensor (a real degradation).
    """
    import logging

    import rawpy
    from image_loader import load_image

    nef = tmp_path / "test.nef"
    nef.write_bytes(b"fake NEF content")

    # Case 1: embedded JPEG matches sensor (HE*/TicoRAW shape) → claim it.
    _install_fake_raw(monkeypatch, _FakeRaw(
        embedded_jpeg=_jpeg_bytes((5392, 3592)),
        sensor_size=(5392, 3592),
        postprocess_error=rawpy.LibRawFileUnsupportedError(b'unsupported'),
    ))
    with caplog.at_level(logging.INFO, logger='image_loader'):
        caplog.clear()
        load_image(str(nef), max_size=8000)
    msgs = [r.message for r in caplog.records]
    assert any("full camera output" in m for m in msgs), msgs

    # Case 2: embedded JPEG smaller than sensor → don't claim it.
    _install_fake_raw(monkeypatch, _FakeRaw(
        embedded_jpeg=_jpeg_bytes((1600, 1067)),
        sensor_size=(6000, 4000),
        postprocess_error=rawpy.LibRawFileUnsupportedError(b'unsupported'),
    ))
    with caplog.at_level(logging.INFO, logger='image_loader'):
        caplog.clear()
        load_image(str(nef), max_size=2048)
    msgs = [r.message for r in caplog.records]
    assert not any("full camera output" in m for m in msgs), msgs

    # Case 3: embedded shares the sensor's long edge but is cropped on the
    # short edge (e.g. 6000×3376 preview against a 6000×4000 sensor). A
    # long-edge-only check would mislabel this as full output; we want
    # both axes verified.
    _install_fake_raw(monkeypatch, _FakeRaw(
        embedded_jpeg=_jpeg_bytes((6000, 3376)),
        sensor_size=(6000, 4000),
        postprocess_error=rawpy.LibRawFileUnsupportedError(b'unsupported'),
    ))
    with caplog.at_level(logging.INFO, logger='image_loader'):
        caplog.clear()
        load_image(str(nef), max_size=8000)
    msgs = [r.message for r in caplog.records]
    assert not any("full camera output" in m for m in msgs), msgs


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


def test_raw_preserve_highlights_falls_back_to_embedded_on_decode_failure(
    tmp_path, monkeypatch
):
    """Unsupported RAWs still use embedded JPEG fallback in edit-quality mode."""
    import rawpy
    from image_loader import RAW_DECODE_PRESERVE_HIGHLIGHTS, load_image

    nef = tmp_path / "test.nef"
    nef.write_bytes(b"fake NEF content")

    _install_fake_raw(monkeypatch, _FakeRaw(
        embedded_jpeg=_jpeg_bytes((1600, 1067)),
        postprocess_error=rawpy.LibRawFileUnsupportedError(b'unsupported'),
    ))

    result = load_image(
        str(nef),
        max_size=2048,
        raw_decode=RAW_DECODE_PRESERVE_HIGHLIGHTS,
    )

    assert result is not None
    assert max(result.size) == 1600


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


def test_extract_working_copy_uses_highlight_preserving_raw_decode(
    tmp_path, monkeypatch
):
    """Working-copy extraction is the edit-quality path for RAW sources."""
    import image_loader
    from PIL import Image

    calls = []

    def fake_load_image(source_path, max_size=1024, raw_decode=None):
        calls.append({
            "source_path": source_path,
            "max_size": max_size,
            "raw_decode": raw_decode,
        })
        return Image.new("RGB", (100, 50), color="white")

    monkeypatch.setattr(image_loader, "load_image", fake_load_image)

    output = tmp_path / "working" / "42.jpg"
    result = image_loader.extract_working_copy(
        str(tmp_path / "photo.nef"),
        str(output),
        max_size=4096,
        quality=92,
    )

    assert result is True
    assert output.exists()
    assert calls == [{
        "source_path": str(tmp_path / "photo.nef"),
        "max_size": 4096,
        "raw_decode": image_loader.RAW_DECODE_PRESERVE_HIGHLIGHTS,
    }]


def test_extract_working_copy_missing_source_returns_false(tmp_path):
    """extract_working_copy returns False when the source file does not exist."""
    from image_loader import extract_working_copy

    output = tmp_path / "working" / "42.jpg"
    result = extract_working_copy(
        str(tmp_path / "nonexistent.jpg"), str(output), max_size=4096
    )

    assert result is False
    assert not output.exists()


def test_extract_working_copy_removes_partial_output_on_failure(
    tmp_path, monkeypatch,
):
    """A partial output left by a mid-encode failure must be removed.

    Without cleanup, ``working/<id>.jpg`` still exists after the failed
    encode. It is skipped by the on-demand write's cacheable-open logic
    (which decodes the file to measure it), but scanner backfill's post-
    loop quota accounting counts every canonical file. That would let a
    batch of encoding failures leave corrupt bytes in the cache that
    ``PIL.Image.open`` in later routes 500s on and that eviction skips
    under the 60-second writer grace.
    """
    from image_loader import extract_working_copy
    from PIL import Image

    source = tmp_path / "photo.jpg"
    Image.new("RGB", (100, 100), color=(255, 0, 0)).save(str(source), "JPEG")
    output = tmp_path / "working" / "42.jpg"

    real_save = Image.Image.save

    def fail_after_writing(self, fp, *args, **kwargs):
        # Simulate ``img.save`` writing partial JPEG bytes and then
        # crashing (out-of-disk, corrupt tables, etc.).
        if isinstance(fp, str):
            with open(fp, "wb") as fh:
                fh.write(b"\xff\xd8\xff\xe0truncated jpeg header")
        raise OSError("simulated encoder failure")

    monkeypatch.setattr(Image.Image, "save", fail_after_writing)

    result = extract_working_copy(str(source), str(output), max_size=0)

    Image.Image.save = real_save

    assert result is False
    assert not output.exists()


def test_extract_working_copy_failure_preserves_concurrent_publication(
    tmp_path, monkeypatch,
):
    """A failed writer cleans only its private tempfile, not output_path."""
    from image_loader import extract_working_copy
    from PIL import Image

    source = tmp_path / "photo.jpg"
    Image.new("RGB", (100, 100), color=(255, 0, 0)).save(source, "JPEG")
    output = tmp_path / "working" / "42.jpg"
    output.parent.mkdir()
    published = b"valid bytes published by another request"
    output.write_bytes(published)

    def fail_after_writing(_self, fp, *args, **kwargs):
        with open(fp, "wb") as handle:
            handle.write(b"partial private bytes")
        raise OSError("simulated encoder failure")

    monkeypatch.setattr(Image.Image, "save", fail_after_writing)

    result = extract_working_copy(str(source), str(output), max_size=0)

    assert result is False
    assert output.read_bytes() == published
    assert not list(output.parent.glob("*.jpg.tmp"))


def test_extract_working_copy_uses_publication_guard(tmp_path):
    from image_loader import extract_working_copy
    from PIL import Image

    source = tmp_path / "photo.jpg"
    output = tmp_path / "working" / "42.jpg"
    Image.new("RGB", (100, 100), color=(255, 0, 0)).save(source, "JPEG")
    events = []

    @contextlib.contextmanager
    def guard():
        events.append("enter")
        try:
            yield
        finally:
            events.append("exit")

    assert extract_working_copy(
        str(source), str(output), max_size=0, publication_guard=guard,
    )
    assert output.exists()
    assert events == ["enter", "exit"]


# ── load_working_image tests ──────────────────────────────────────────


def test_load_working_image_uses_working_copy(tmp_path):
    """load_working_image loads from working copy when available."""
    from image_loader import load_working_image
    from PIL import Image

    # Create a working copy JPEG
    working_dir = tmp_path / "working"
    working_dir.mkdir()
    wc_path = working_dir / "42.jpg"
    Image.new("RGB", (4096, 2731)).save(str(wc_path), "JPEG")

    photo = {"working_copy_path": "working/42.jpg", "folder_id": 1, "filename": "test.nef"}
    img = load_working_image(photo, str(tmp_path), max_size=1024)

    assert img is not None
    assert max(img.size) <= 1024


def test_load_working_image_uses_original_for_jpeg(tmp_path):
    """load_working_image uses original file when working_copy_path is NULL."""
    from image_loader import load_working_image
    from PIL import Image

    # Create a JPEG original
    folder = tmp_path / "photos"
    folder.mkdir()
    orig = folder / "test.jpg"
    Image.new("RGB", (3000, 2000)).save(str(orig), "JPEG")

    folders = {1: str(folder)}
    photo = {"working_copy_path": None, "folder_id": 1, "filename": "test.jpg"}
    img = load_working_image(photo, str(tmp_path), max_size=1024, folders=folders)

    assert img is not None
    assert max(img.size) <= 1024


def test_load_working_image_retries_original_after_working_copy_eviction(
    tmp_path, monkeypatch,
):
    """A selected working copy lost before open falls back to its source."""
    import image_loader
    from PIL import Image

    working_dir = tmp_path / "working"
    working_dir.mkdir()
    wc_path = working_dir / "42.jpg"
    Image.new("RGB", (800, 600), color="blue").save(wc_path, "JPEG")
    source_dir = tmp_path / "photos"
    source_dir.mkdir()
    source_path = source_dir / "test.jpg"
    Image.new("RGB", (800, 600), color="red").save(source_path, "JPEG")

    real_load_standard = image_loader._load_standard

    def evict_before_open(path, max_size):
        if os.path.abspath(path) == os.path.abspath(wc_path):
            os.unlink(wc_path)
            return None
        return real_load_standard(path, max_size)

    monkeypatch.setattr(image_loader, "_load_standard", evict_before_open)
    photo = {
        "working_copy_path": "working/42.jpg",
        "folder_id": 1,
        "filename": "test.jpg",
    }

    img, input_source = image_loader.load_working_image(
        photo, str(tmp_path), max_size=1024,
        folders={1: str(source_dir)}, return_source=True,
    )

    assert img is not None
    assert input_source == "original"
    assert img.getpixel((0, 0))[0] > img.getpixel((0, 0))[2]


def test_load_working_image_returns_none_when_no_working_copy_no_folders(tmp_path):
    """load_working_image returns None when working_copy_path is set but file is missing, and folders is None."""
    from image_loader import load_working_image

    photo = {"working_copy_path": "working/missing.jpg", "folder_id": 1, "filename": "test.nef"}
    img = load_working_image(photo, str(tmp_path), max_size=1024)

    assert img is None


# ── get_canonical_image_path tests ──────────────────────────────────────────


def test_get_canonical_image_path_prefers_working_copy(tmp_path):
    """When working_copy_path is set and file exists, returns working copy path."""
    from image_loader import get_canonical_image_path

    vireo_dir = tmp_path / "vireo"
    (vireo_dir / "working").mkdir(parents=True)
    wc = vireo_dir / "working" / "42.jpg"
    wc.write_bytes(b"fake")

    photo = {"working_copy_path": "working/42.jpg", "folder_id": 1, "filename": "src.jpg"}
    folders = {1: "/some/folder"}

    result = get_canonical_image_path(photo, str(vireo_dir), folders)
    assert os.path.normpath(result) == os.path.normpath(str(wc))


def test_get_canonical_image_path_falls_back_to_source(tmp_path):
    """When no working_copy_path, returns folder/filename."""
    from image_loader import get_canonical_image_path

    photo = {"working_copy_path": None, "folder_id": 1, "filename": "src.jpg"}
    folders = {1: "/some/folder"}

    result = get_canonical_image_path(photo, str(tmp_path), folders)
    assert os.path.normpath(result) == os.path.normpath("/some/folder/src.jpg")


def test_get_canonical_image_path_wc_missing_falls_back(tmp_path, caplog):
    """When working_copy_path is set but file missing, warn and fall back to source."""
    import logging

    from image_loader import get_canonical_image_path

    photo = {"working_copy_path": "working/99.jpg", "folder_id": 1, "filename": "src.jpg"}
    folders = {1: "/some/folder"}

    with caplog.at_level(logging.WARNING):
        result = get_canonical_image_path(photo, str(tmp_path), folders)

    assert os.path.normpath(result) == os.path.normpath("/some/folder/src.jpg")
    assert any("working copy missing" in r.message.lower() for r in caplog.records)


# -------- transient libraw I/O retry --------

def test_load_image_retries_once_on_libraw_io_error(tmp_path, monkeypatch):
    """A NAS hiccup that fails one rawpy.imread() call typically clears
    on retry. load_image must give the file a second chance — without it
    the user is stuck with cached question marks until they refresh."""
    import image_loader
    import rawpy

    src = tmp_path / "DSC_0001.NEF"
    src.write_bytes(b"placeholder")  # need an existing file path

    embedded = _jpeg_bytes((1024, 768))
    fake = _FakeRaw(embedded_jpeg=embedded,
                    postprocess_error=rawpy.LibRawFileUnsupportedError(
                        b"unsupported"))

    calls = {"n": 0}

    def imread_flaky(_path):
        calls["n"] += 1
        if calls["n"] == 1:
            raise rawpy.LibRawIOError(b"Input/output error")
        return fake

    monkeypatch.setattr(rawpy, "imread", imread_flaky)

    img = image_loader.load_image(str(src), max_size=200)
    assert img is not None
    assert calls["n"] == 2  # initial + one retry


def test_load_image_does_not_retry_on_unsupported_format(tmp_path, monkeypatch):
    """Non-I/O libraw errors are deterministic — retrying is wasted work
    and would slow down legitimately-bad files."""
    import image_loader
    import rawpy

    src = tmp_path / "DSC_0002.NEF"
    src.write_bytes(b"placeholder")

    calls = {"n": 0}

    def imread_unsupported(_path):
        calls["n"] += 1
        raise rawpy.LibRawFileUnsupportedError(b"truly bad file")

    monkeypatch.setattr(rawpy, "imread", imread_unsupported)

    img = image_loader.load_image(str(src), max_size=200)
    assert img is None
    assert calls["n"] == 1  # no retry


def test_is_excluded_scan_dir_matches_app_managed_library_bundles():
    from image_loader import is_excluded_scan_dir

    assert is_excluded_scan_dir("Photos Library.photoslibrary")
    assert is_excluded_scan_dir("Music Library.musiclibrary")
    assert is_excluded_scan_dir("Old Library.photolibrary")
    assert is_excluded_scan_dir("Aperture.aplibrary")
    assert is_excluded_scan_dir("PHOTOS LIBRARY.PHOTOSLIBRARY")  # case-insensitive
    assert is_excluded_scan_dir("MUSIC LIBRARY.MUSICLIBRARY")  # case-insensitive
    assert is_excluded_scan_dir("Photo Booth Library")
    # Real photo folders must NOT be excluded.
    assert not is_excluded_scan_dir("2026")
    assert not is_excluded_scan_dir("Pictures")
    assert not is_excluded_scan_dir("photoslibrary_backup")  # not a suffix match
    assert not is_excluded_scan_dir("musiclibrary_backup")  # not a suffix match


def test_prune_scan_dirs_removes_in_place_and_reports():
    from image_loader import prune_scan_dirs

    dirnames = ["2026", "Photos Library.photoslibrary", "January"]
    removed = prune_scan_dirs(dirnames)

    assert removed == ["Photos Library.photoslibrary"]
    assert dirnames == ["2026", "January"]  # mutated in place


def test_prune_scan_dirs_noop_when_clean():
    from image_loader import prune_scan_dirs

    dirnames = ["2026", "January"]
    assert prune_scan_dirs(dirnames) == []
    assert dirnames == ["2026", "January"]


def test_is_excluded_scan_path_matches_nested_paths():
    """The root-level guard must reject a path that *sits inside* an excluded
    bundle, not only one whose leaf name is the bundle. Without this, a user
    picking ``.../Photos Library.photoslibrary/originals`` directly — or a
    stale folder row carried over from before the guard existed — still
    drives os.walk into the protected bundle and re-trips macOS TCC."""
    from image_loader import is_excluded_scan_path

    # Leaf-is-bundle (the case the leaf-only check already caught).
    assert is_excluded_scan_path("/Users/me/Pictures/Photos Library.photoslibrary")
    # Nested under a bundle — the case the leaf-only check missed.
    assert is_excluded_scan_path(
        "/Users/me/Pictures/Photos Library.photoslibrary/originals"
    )
    assert is_excluded_scan_path(
        "/Users/me/Pictures/Photos Library.photoslibrary/originals/2024/01"
    )
    assert is_excluded_scan_path(
        "/Users/me/Photo Booth Library/Pictures/IMG.jpg"
    )
    assert is_excluded_scan_path(
        "/Users/me/Music/Music Library.musiclibrary/Library.musicdb"
    )
    # Case-insensitive on the bundle component.
    assert is_excluded_scan_path(
        "/Users/me/PHOTOS LIBRARY.PHOTOSLIBRARY/originals"
    )
    assert is_excluded_scan_path(
        "/Users/me/MUSIC LIBRARY.MUSICLIBRARY/Library.musicdb"
    )
    # Real photo folders must NOT be excluded.
    assert not is_excluded_scan_path("/Users/me/Pictures/2026/January")
    assert not is_excluded_scan_path("/Users/me/Pictures")
    # Substring matches on non-bundle component names must NOT trigger.
    assert not is_excluded_scan_path(
        "/Users/me/Pictures/photoslibrary_backup/2024"
    )
    assert not is_excluded_scan_path(
        "/Users/me/Music/musiclibrary_backup/2024"
    )


def test_is_excluded_scan_path_resolves_symlinked_root(tmp_path):
    """A symlink whose target is (or sits inside) an excluded bundle must
    be rejected. Without resolving, ``Path(path).parts`` reveals nothing
    about the bundle, but ``Path.is_dir()`` / ``os.walk()`` follow the
    link anyway — so the walker would still open the protected subtree
    and re-trip the macOS TCC prompt this guard exists to avoid.
    """
    import pytest
    if sys.platform == "win32":
        pytest.skip("POSIX symlinks required")
    from image_loader import is_excluded_scan_path

    bundle = tmp_path / "Photos Library.photoslibrary"
    (bundle / "originals").mkdir(parents=True)

    # Direct symlink at the bundle itself.
    direct_link = tmp_path / "PhotoLibLink"
    os.symlink(str(bundle), str(direct_link))
    assert is_excluded_scan_path(str(direct_link))

    # Symlink at a child *inside* the bundle.
    child_link = tmp_path / "OriginalsLink"
    os.symlink(str(bundle / "originals"), str(child_link))
    assert is_excluded_scan_path(str(child_link))

    # Intermediate symlink on the way to the bundle subtree.
    alias_dir = tmp_path / "Aliases"
    alias_dir.mkdir()
    os.symlink(str(bundle), str(alias_dir / "MyLib"))
    assert is_excluded_scan_path(str(alias_dir / "MyLib" / "originals"))

    # A symlink to a normal folder must NOT be excluded.
    normal = tmp_path / "real_photos"
    normal.mkdir()
    normal_link = tmp_path / "PhotosLink"
    os.symlink(str(normal), str(normal_link))
    assert not is_excluded_scan_path(str(normal_link))


def test_is_excluded_scan_path_tolerates_non_string_inputs():
    """Truthy non-string JSON primitives (``{"root": 123}``,
    ``{"source": true}``) can reach this helper before the route's
    ``os.path.isdir`` validation. ``Path(int)``/``Path(bool)`` raise
    ``TypeError``; the helper must swallow that and return False so the
    downstream ``isdir`` check still produces the route's 400 response
    rather than a 500.
    """
    from image_loader import is_excluded_scan_path

    assert is_excluded_scan_path(123) is False
    assert is_excluded_scan_path(True) is False
    assert is_excluded_scan_path(0.5) is False
    assert is_excluded_scan_path(["/Users/me/Pictures"]) is False
    assert is_excluded_scan_path({"path": "/Users/me/Pictures"}) is False


def test_is_excluded_scan_path_resolves_symlinks_without_realpath(
    tmp_path, monkeypatch,
):
    """Symlink resolution must not go through ``os.path.realpath``.

    ``realpath`` walks the resolved chain by ``lstat``-ing every
    component along the way — including the bundle target once a link
    points at it — which is exactly the kind of stat the macOS
    "access data from other apps" TCC prompt watches for. Fail the
    test if the helper ever calls ``os.path.realpath`` on an input
    that's a symlink (or contains one). With purely textual resolution
    (``os.path.islink`` + ``os.readlink``) the call must never happen.
    """
    import pytest
    if sys.platform == "win32":
        pytest.skip("POSIX symlinks required")
    import os.path as ospath

    import image_loader

    real_realpath = ospath.realpath
    calls = []

    def trapped_realpath(p, *args, **kwargs):
        calls.append(p)
        return real_realpath(p, *args, **kwargs)

    monkeypatch.setattr(image_loader.os.path, "realpath", trapped_realpath)

    bundle = tmp_path / "Photos Library.photoslibrary"
    (bundle / "originals").mkdir(parents=True)

    direct_link = tmp_path / "PhotoLibLink"
    os.symlink(str(bundle), str(direct_link))
    chained_link = tmp_path / "ChainLink"
    os.symlink(str(direct_link), str(chained_link))
    alias_dir = tmp_path / "Aliases"
    alias_dir.mkdir()
    os.symlink(str(bundle), str(alias_dir / "MyLib"))

    assert image_loader.is_excluded_scan_path(str(direct_link))
    assert image_loader.is_excluded_scan_path(str(chained_link))
    assert image_loader.is_excluded_scan_path(str(alias_dir / "MyLib"))
    assert image_loader.is_excluded_scan_path(str(alias_dir / "MyLib" / "originals"))

    assert calls == [], (
        f"is_excluded_scan_path must not call os.path.realpath "
        f"(it lstats inside the bundle); got calls for {calls}"
    )


def test_safe_iter_dir_skips_bundle_children_and_yields_paths(tmp_path):
    """``safe_iter_dir`` mirrors ``Path.iterdir`` but drops excluded
    bundle children — direct ``Photos Library.photoslibrary`` entries
    or symlinks pointing at one — before the caller can stat them with
    ``f.is_file()``. Without this, a non-recursive scan/ingest of a
    parent like ``~/Pictures`` would stat the bundle target via
    ``is_file`` (which follows symlinks) and re-trip the macOS TCC
    "access data from other apps" prompt this guard exists to avoid.
    """
    import pytest
    if sys.platform == "win32":
        pytest.skip("POSIX symlinks required")
    from image_loader import safe_iter_dir

    bundle = tmp_path / "Photos Library.photoslibrary"
    (bundle / "originals").mkdir(parents=True)
    (bundle / "originals" / "managed.jpg").write_bytes(b"")

    root = tmp_path / "photos"
    root.mkdir()
    (root / "real.jpg").write_bytes(b"")
    (root / "Photos Library.photoslibrary").mkdir()
    (root / "Photos Library.photoslibrary" / "managed.jpg").write_bytes(b"")
    os.symlink(str(bundle), str(root / "LibraryAlias"))
    (root / "sub").mkdir()
    (root / "sub" / "deep.jpg").write_bytes(b"")  # not yielded — only direct children

    names = {p.name for p in safe_iter_dir(str(root))}

    assert "real.jpg" in names
    assert "sub" in names
    assert "Photos Library.photoslibrary" not in names
    assert "LibraryAlias" not in names


def test_safe_iter_dir_deduplicates_duplicate_scandir_entries(tmp_path, monkeypatch):
    """Some SMB mounts can return duplicate names from one ``scandir`` call."""
    import image_loader
    from image_loader import safe_iter_dir

    root = tmp_path / "photos"
    root.mkdir()
    (root / "real.jpg").write_bytes(b"")

    real_scandir = image_loader.os.scandir

    class FakeScandir:
        def __init__(self, entries):
            self.entries = entries

        def __iter__(self):
            return iter(self.entries)

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    def fake_scandir(path):
        if path == str(root):
            return FakeScandir([
                SimpleNamespace(
                    name="real.jpg",
                    path=str(root / "real.jpg"),
                    is_symlink=lambda: False,
                    is_dir=lambda follow_symlinks=True: False,
                ),
                SimpleNamespace(
                    name="real.jpg",
                    path=str(root / "real.jpg"),
                    is_symlink=lambda: False,
                    is_dir=lambda follow_symlinks=True: False,
                ),
            ])
        return real_scandir(path)

    monkeypatch.setattr(image_loader.os, "scandir", fake_scandir)

    assert [p.name for p in safe_iter_dir(str(root))] == ["real.jpg"]


def test_safe_iter_dir_surfaces_permission_errors_via_onerror(tmp_path):
    """Mirrors ``safe_scan_walk``: a directory the kernel refuses to
    open is reported via the callback, not swallowed. Scanner's
    non-recursive partial-discovery callback depends on this.
    """
    import pytest
    if sys.platform == "win32":
        pytest.skip("POSIX permissions required")
    if hasattr(os, "geteuid") and os.geteuid() == 0:
        pytest.skip("root bypasses POSIX mode bits; cannot deny self")
    from image_loader import safe_iter_dir

    forbidden = tmp_path / "forbidden"
    forbidden.mkdir()
    (forbidden / "hidden.jpg").write_bytes(b"")
    os.chmod(str(forbidden), 0o000)

    errors = []
    try:
        list(safe_iter_dir(str(forbidden), onerror=errors.append))
        assert any(str(forbidden) in str(getattr(e, "filename", "") or e)
                   for e in errors), (
            f"denied path not reported via onerror callback: {errors}"
        )
    finally:
        os.chmod(str(forbidden), 0o755)


def test_safe_scan_walk_skips_symlinked_excluded_bundle(tmp_path, caplog):
    """A child symlink whose target is an excluded bundle must be dropped
    before any stat that follows the link.

    ``os.walk`` classifies each child by calling ``DirEntry.is_dir()``,
    which follows symlinks. For a child like ``LibraryAlias -> Photos
    Library.photoslibrary`` that classification stat alone reaches into
    the protected bundle and re-trips the macOS TCC "access data from
    other apps" prompt this guard exists to avoid — even though
    ``prune_scan_dirs`` would have removed the entry from recursion
    afterwards. ``safe_scan_walk`` must read the symlink target textually
    (``os.readlink``, no stat-follow) and skip the entry before
    classification.
    """
    import logging

    import pytest
    if sys.platform == "win32":
        pytest.skip("POSIX symlinks required")
    from image_loader import safe_scan_walk

    bundle = tmp_path / "Photos Library.photoslibrary"
    (bundle / "originals").mkdir(parents=True)
    (bundle / "originals" / "managed.jpg").write_bytes(b"")

    root = tmp_path / "photos"
    root.mkdir()
    (root / "real.jpg").write_bytes(b"")
    os.symlink(str(bundle), str(root / "LibraryAlias"))

    seen = []
    with caplog.at_level(logging.INFO, logger="image_loader"):
        for _dirpath, dirnames, filenames in safe_scan_walk(str(root)):
            for name in filenames + dirnames:
                seen.append(name)

    assert "real.jpg" in seen
    # The symlink must not surface as a child dir (else os.walk would
    # recurse) or a file (else os.path.isfile would follow it and trip
    # the same prompt).
    assert "LibraryAlias" not in seen
    assert "managed.jpg" not in seen
    # The skip was deliberate, not just a missed entry — the log line
    # records what we dropped under this root.
    assert any(
        "LibraryAlias" in r.getMessage()
        for r in caplog.records
        if r.name == "image_loader"
    )


def test_safe_scan_walk_skips_direct_bundle_child_without_stat(tmp_path):
    """A direct (non-symlinked) bundle child must be dropped by name
    before any ``is_dir()`` call. ``os.walk``'s classification stat on
    the bundle root itself is enough to trip the macOS TCC prompt, so a
    name match has to win before any stat happens.
    """
    from image_loader import safe_scan_walk

    root = tmp_path / "photos"
    root.mkdir()
    (root / "real.jpg").write_bytes(b"")
    bundle = root / "Photos Library.photoslibrary"
    (bundle / "originals" / "0").mkdir(parents=True)
    (bundle / "originals" / "0" / "managed.jpg").write_bytes(b"")

    seen_paths = []
    for dirpath, dirnames, filenames in safe_scan_walk(str(root)):
        for name in filenames:
            seen_paths.append(os.path.join(dirpath, name))
        for name in dirnames:
            seen_paths.append(os.path.join(dirpath, name))

    assert any(p.endswith("real.jpg") for p in seen_paths)
    assert not any(".photoslibrary" in p for p in seen_paths)
    assert not any("managed.jpg" in p for p in seen_paths)


def test_safe_scan_walk_skips_file_symlink_into_excluded_bundle(tmp_path):
    """A file-named symlink whose target sits inside an excluded bundle
    must be dropped during walk classification, not surfaced as a filename.

    A previous version of ``_symlink_target_is_excluded`` checked only the
    *basename* of the symlink target. For a link like
    ``IMG.jpg -> ../Photos Library.photoslibrary/originals/IMG.jpg`` that
    basename (``IMG.jpg``) doesn't match any bundle, so the link surfaced
    in ``filenames``; downstream callers then ran ``os.path.isfile`` /
    ``Path.is_file`` which followed the link, stat'd the managed Photos
    file, and re-tripped the macOS TCC prompt this guard exists to avoid.
    Check every component of the target path, not just the basename.
    """
    import pytest
    if sys.platform == "win32":
        pytest.skip("POSIX symlinks required")
    from image_loader import safe_scan_walk

    bundle = tmp_path / "Photos Library.photoslibrary"
    (bundle / "originals").mkdir(parents=True)
    (bundle / "originals" / "managed.jpg").write_bytes(b"")

    root = tmp_path / "photos"
    root.mkdir()
    (root / "real.jpg").write_bytes(b"")
    # Relative link reaching into the bundle (the case the basename-only
    # check missed).
    os.symlink(
        os.path.join("..", "Photos Library.photoslibrary",
                     "originals", "managed.jpg"),
        str(root / "IMG.jpg"),
    )
    # Absolute link, same shape — covers callers that pass absolute targets.
    os.symlink(
        str(bundle / "originals" / "managed.jpg"),
        str(root / "ABS.jpg"),
    )

    seen = set()
    for _dirpath, dirnames, filenames in safe_scan_walk(str(root)):
        seen.update(filenames)
        seen.update(dirnames)

    assert "real.jpg" in seen
    assert "IMG.jpg" not in seen
    assert "ABS.jpg" not in seen


def test_safe_scan_walk_skips_chained_symlink_into_bundle(tmp_path):
    """A symlink whose *immediate* target is a plain path but resolves
    through another link into an excluded bundle must be dropped.

    Two chain shapes ``_symlink_target_is_excluded`` must catch:

    * ``LibraryAlias -> MidAlias`` where ``MidAlias -> Photos
      Library.photoslibrary``: the first readlink yields ``MidAlias``,
      whose parts don't match any bundle, so a one-hop check would
      surface the entry. Downstream ``Path.is_dir`` / ``os.path.isfile``
      then follow both hops and stat the protected bundle — re-tripping
      the macOS TCC "access data from other apps" prompt.
    * ``IMG.jpg -> MidAlias/originals/IMG.jpg`` (same MidAlias): the
      file-named link's immediate target is a path through MidAlias.
      ``os.path.isfile`` follows the chain into the managed Photos file.

    Use ``is_excluded_scan_path`` on the readlink target so the chain is
    walked component-by-component textually (each ``islink`` confined to
    the link node, never resolving deep enough to touch the bundle).
    """
    import pytest
    if sys.platform == "win32":
        pytest.skip("POSIX symlinks required")
    from image_loader import safe_scan_walk

    bundle = tmp_path / "Photos Library.photoslibrary"
    (bundle / "originals").mkdir(parents=True)
    (bundle / "originals" / "managed.jpg").write_bytes(b"")

    root = tmp_path / "photos"
    root.mkdir()
    (root / "real.jpg").write_bytes(b"")
    # The intermediate link MidAlias sits adjacent to the entries that
    # reference it through their own targets.
    os.symlink(str(bundle), str(root / "MidAlias"))
    # Chained directory link: LibraryAlias -> MidAlias -> bundle.
    os.symlink("MidAlias", str(root / "LibraryAlias"))
    # Chained file link whose target path goes through MidAlias.
    os.symlink(
        os.path.join("MidAlias", "originals", "managed.jpg"),
        str(root / "IMG.jpg"),
    )

    seen = set()
    for _dirpath, dirnames, filenames in safe_scan_walk(str(root)):
        seen.update(filenames)
        seen.update(dirnames)

    assert "real.jpg" in seen
    # All three chained entries must be dropped before any stat that
    # would follow the chain into the bundle.
    assert "MidAlias" not in seen
    assert "LibraryAlias" not in seen
    assert "IMG.jpg" not in seen
    assert "managed.jpg" not in seen


def test_safe_scan_walk_matches_os_walk_for_normal_trees(tmp_path):
    """Outside of bundle exclusion, ``safe_scan_walk`` yields the same
    files as the normal walker for the same recursion semantics
    (``followlinks=False``). Pin this so future tweaks don't accidentally
    drop legitimate photos.
    """
    from image_loader import safe_scan_walk

    (tmp_path / "a").mkdir()
    (tmp_path / "a" / "b").mkdir()
    (tmp_path / "top.jpg").write_bytes(b"")
    (tmp_path / "a" / "mid.jpg").write_bytes(b"")
    (tmp_path / "a" / "b" / "deep.jpg").write_bytes(b"")

    collected = set()
    for dirpath, _dirnames, filenames in safe_scan_walk(str(tmp_path)):
        for name in filenames:
            collected.add(os.path.relpath(
                os.path.join(dirpath, name), str(tmp_path)
            ))

    assert collected == {
        "top.jpg",
        os.path.join("a", "mid.jpg"),
        os.path.join("a", "b", "deep.jpg"),
    }


def test_safe_scan_walk_deduplicates_duplicate_scandir_entries(tmp_path, monkeypatch):
    """Duplicate directory entries must not inflate new-image counts."""
    import image_loader
    from image_loader import safe_scan_walk

    root = tmp_path / "photos"
    root.mkdir()
    (root / "real.jpg").write_bytes(b"")
    (root / "sub").mkdir()
    (root / "sub" / "deep.jpg").write_bytes(b"")

    real_scandir = image_loader.os.scandir

    def file_entry(path, name):
        return SimpleNamespace(
            name=name,
            path=str(path / name),
            is_symlink=lambda: False,
            is_dir=lambda follow_symlinks=True: False,
        )

    def dir_entry(path, name):
        return SimpleNamespace(
            name=name,
            path=str(path / name),
            is_symlink=lambda: False,
            is_dir=lambda follow_symlinks=True: True,
        )

    class FakeScandir:
        def __init__(self, entries):
            self.entries = entries

        def __iter__(self):
            return iter(self.entries)

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    def fake_scandir(path):
        if path == str(root):
            return FakeScandir([
                file_entry(root, "real.jpg"),
                file_entry(root, "real.jpg"),
                dir_entry(root, "sub"),
                dir_entry(root, "sub"),
            ])
        if path == str(root / "sub"):
            return FakeScandir([
                file_entry(root / "sub", "deep.jpg"),
                file_entry(root / "sub", "deep.jpg"),
            ])
        return real_scandir(path)

    monkeypatch.setattr(image_loader.os, "scandir", fake_scandir)

    rows = list(safe_scan_walk(str(root)))

    assert [(os.path.relpath(d, str(root)), dirs, files) for d, dirs, files in rows] == [
        (".", ["sub"], ["real.jpg"]),
        ("sub", [], ["deep.jpg"]),
    ]


def test_safe_scan_walk_surfaces_permission_errors_via_onerror(tmp_path):
    """``safe_scan_walk`` must mirror ``os.walk(onerror=...)``: a directory
    the kernel refuses to enter is reported via the callback, not swallowed.
    Regression guard: the scanner's partial-discovery callback relies on
    this to surface denied paths.
    """
    import pytest
    if sys.platform == "win32":
        pytest.skip("POSIX permissions required")
    if hasattr(os, "geteuid") and os.geteuid() == 0:
        pytest.skip("root bypasses POSIX mode bits; cannot deny self")
    from image_loader import safe_scan_walk

    (tmp_path / "ok.jpg").write_bytes(b"")
    forbidden = tmp_path / "forbidden"
    forbidden.mkdir()
    (forbidden / "hidden.jpg").write_bytes(b"")
    os.chmod(str(forbidden), 0o000)

    errors = []
    try:
        for _dirpath, _dirnames, _filenames in safe_scan_walk(
            str(tmp_path), onerror=errors.append
        ):
            pass
        assert any(str(forbidden) in str(getattr(e, "filename", "") or e)
                   for e in errors), (
            f"denied path not reported via onerror callback: {errors}"
        )
    finally:
        os.chmod(str(forbidden), 0o755)


def test_safe_scan_walk_cancels_while_enumerating_single_large_directory(
    tmp_path, monkeypatch,
):
    """A cancel request must fire while ``safe_scan_walk`` is still
    consuming a single directory's ``os.scandir`` iterator, not only
    between yields. ``os.walk``-style semantics collect every child's
    classification stat into ``dirs``/``nondirs`` lists before the tuple
    is yielded — for a directory with millions of entries that means a
    per-yield check by the caller cannot pause or cancel until the whole
    enumeration finishes. Emulate the pathology with a fake ``os.scandir``
    that streams many entries into a single directory tuple.
    """
    import image_loader
    import pytest
    from image_loader import ScanCancelled, safe_scan_walk

    root = tmp_path / "photos"
    root.mkdir()

    real_scandir = image_loader.os.scandir

    class LongScandir:
        def __init__(self, entries):
            self.entries = entries

        def __iter__(self):
            return iter(self.entries)

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    # 1024 fake entries — enough to cross several 256-entry cancel poll
    # boundaries. A caller-side per-yield check would not fire until the
    # whole list was buffered.
    fake_entries = [
        SimpleNamespace(
            name=f"file_{i:04d}.dat",
            path=str(root / f"file_{i:04d}.dat"),
            is_symlink=lambda: False,
            is_dir=lambda follow_symlinks=True: False,
        )
        for i in range(1024)
    ]

    def fake_scandir(path):
        if path == str(root):
            return LongScandir(fake_entries)
        return real_scandir(path)

    monkeypatch.setattr(image_loader.os, "scandir", fake_scandir)

    calls = {"n": 0}

    def cancel_after_top_level_check():
        calls["n"] += 1
        # Let the pre-loop guard pass (call #1) so the test actually
        # exercises the in-scandir poll rather than the entry check.
        return calls["n"] > 1

    with pytest.raises(ScanCancelled):
        for _row in safe_scan_walk(
            str(root), cancel_check=cancel_after_top_level_check,
        ):
            pytest.fail("safe_scan_walk yielded before honoring cancel")

    # Cancel fired via the in-scandir poll, not via a per-yield check.
    # A single-directory tree only yields after the scandir loop
    # finishes, so if the cancel had to wait for a yield we would have
    # classified all 1024 fake entries first. Any n between 2 and 5 is a
    # cancel that landed while the walker was still filling the buffer;
    # the walker checks the callback every 256 entries starting at
    # i=256.
    assert 2 <= calls["n"] <= 6, calls


def test_safe_scan_walk_reports_error_raised_while_iterating(tmp_path, monkeypatch):
    """An ``OSError`` raised *during* ``scandir`` iteration (macOS raises
    ``ENOTCONN`` this way when an SMB share drops mid-walk) must go through
    ``onerror`` like ``os.walk`` does, not escape and kill the whole walk."""
    import errno

    import image_loader
    from image_loader import safe_scan_walk

    root = tmp_path / "photos"
    root.mkdir()
    (root / "ok.jpg").write_bytes(b"")
    dropped = root / "dropped"
    dropped.mkdir()
    (dropped / "gone.jpg").write_bytes(b"")

    real_scandir = image_loader.os.scandir

    class _DisconnectedIterator:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def __iter__(self):
            return self

        def __next__(self):
            raise OSError(errno.ENOTCONN, "Socket is not connected", str(dropped))

    def fake_scandir(path):
        if str(path) == str(dropped):
            return _DisconnectedIterator()
        return real_scandir(path)

    monkeypatch.setattr(image_loader.os, "scandir", fake_scandir)

    errors = []
    visited = [
        dirpath for dirpath, _dirs, _files in safe_scan_walk(str(root), onerror=errors.append)
    ]
    assert str(root) in visited
    assert str(dropped) not in visited
    assert len(errors) == 1
    assert errors[0].errno == errno.ENOTCONN


def test_safe_scan_walk_iteration_error_without_onerror_is_swallowed(tmp_path, monkeypatch):
    import errno

    import image_loader
    from image_loader import safe_scan_walk

    root = tmp_path / "photos"
    root.mkdir()
    real_scandir = image_loader.os.scandir

    class _Broken:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def __iter__(self):
            return self

        def __next__(self):
            raise OSError(errno.EIO, "Input/output error")

    monkeypatch.setattr(
        image_loader.os, "scandir",
        lambda p: _Broken() if str(p) == str(root) else real_scandir(p),
    )
    assert list(safe_scan_walk(str(root))) == []


def test_safe_scan_walk_on_entry_fires_for_every_entry(tmp_path):
    from image_loader import safe_scan_walk

    root = tmp_path / "photos"
    root.mkdir()
    for i in range(5):
        (root / f"{i}.jpg").write_bytes(b"")
    (root / "sub").mkdir()
    (root / "sub" / "deep.jpg").write_bytes(b"")
    beats = []
    list(safe_scan_walk(str(root), on_entry=lambda: beats.append(1)))
    assert len(beats) == 7, "one heartbeat per directory entry, including subdirectories"


def test_load_working_image_does_not_stamp_by_default(tmp_path):
    """Job callers must not flatten the eviction recency signal.

    ``load_working_image`` is shared by classification, sharpness and
    culling, which walk the whole library. If it stamped unconditionally,
    one pipeline run would mark every working copy as recently used and
    quota eviction would have nothing left to order by.
    """
    import os
    import time

    from image_loader import load_working_image
    from PIL import Image

    vireo_dir = tmp_path / "vireo"
    working = vireo_dir / "working"
    working.mkdir(parents=True)
    wc = working / "7.jpg"
    Image.new("RGB", (64, 48), (10, 20, 30)).save(str(wc), "JPEG")
    stale = time.time() - 30 * 24 * 3600
    os.utime(str(wc), (stale, stale))

    photo = {"id": 7, "working_copy_path": "working/7.jpg", "folder_id": 1}
    image = load_working_image(photo, str(vireo_dir), max_size=32)

    assert image is not None
    assert os.stat(str(wc)).st_atime == stale, (
        "a job-path read stamped the working copy; every library-wide pass "
        "would then look like recent user activity"
    )


def test_load_working_image_restores_atime_after_unmarked_decode(
    tmp_path, monkeypatch,
):
    """A ``record_access=False`` read must not leak an atime bump.

    Regression for a P2 codex finding on PR #1607 against ``3d625e07``.
    ``_evict_once`` uses ``max(mtime_ns, atime_ns)`` as its recency
    key. On strictatime and default relatime filesystems, PIL's
    ``open`` inside ``_load_standard`` advances ``atime`` as a side
    effect of the decode — so every job-path walk of the library
    (sharpness, classify, culling) would refresh every working copy
    it processed and flatten the LRU signal that the interactive
    touches from ``touch_working_copy_access`` are meant to buy.

    ``tmp_path`` in CI is typically on a filesystem where a passive
    read doesn't visibly bump atime, so simulate the OS behavior by
    stamping atime forward from inside ``_load_standard``. Without
    the restore, ``st_atime`` after the call would reflect the
    simulated bump; with the fix, it is restored to the pre-read
    value.
    """
    import os
    import time

    import image_loader
    from image_loader import load_working_image
    from PIL import Image

    vireo_dir = tmp_path / "vireo"
    working = vireo_dir / "working"
    working.mkdir(parents=True)
    wc = working / "7.jpg"
    Image.new("RGB", (64, 48), (10, 20, 30)).save(str(wc), "JPEG")
    stale = time.time() - 30 * 24 * 3600
    os.utime(str(wc), (stale, stale))

    real_load_standard = image_loader._load_standard

    def atime_bumping_load(path, max_size):
        image = real_load_standard(path, max_size)
        # Simulate a strictatime FS: the read bumped atime to "now".
        # Mtime stays as extract left it.
        now = time.time()
        try:
            current_mtime = os.stat(path).st_mtime
        except OSError:
            current_mtime = stale
        os.utime(path, (now, current_mtime))
        return image

    monkeypatch.setattr(image_loader, "_load_standard", atime_bumping_load)

    photo = {"id": 7, "working_copy_path": "working/7.jpg", "folder_id": 1}
    image = load_working_image(photo, str(vireo_dir), max_size=32)

    assert image is not None
    stat_after = os.stat(str(wc))
    assert stat_after.st_atime == stale, (
        "``record_access=False`` did not neutralize the read's atime "
        f"bump: atime is {stat_after.st_atime} (expected {stale}). A "
        "background library traversal would refresh every working "
        "copy's recency, defeating the opt-in contract this parameter "
        "documents"
    )
    assert stat_after.st_mtime == stale, (
        "restore also advanced mtime; that key must stay stable so "
        "the external-edit handoff and display-cache freshness gates "
        "don't see spurious content changes"
    )


def test_load_working_image_preserves_publisher_write_over_restore(
    tmp_path, monkeypatch,
):
    """The restore must not overwrite a concurrent publisher's fresh mtime.

    If a publisher atomically replaces the working copy during our
    decode, the file's mtime advances to the publisher's write time.
    Restoring atime to the pre-read snapshot when mtime has moved
    would peg the file's recency back to the stale pre-read value,
    letting the fresh publish look older than it is — and eviction
    would then reclaim their newly-published bytes ahead of files
    that really are cold. Skip the restore whenever the observed
    mtime no longer matches what we captured.
    """
    import os
    import time

    import image_loader
    from image_loader import load_working_image
    from PIL import Image

    vireo_dir = tmp_path / "vireo"
    working = vireo_dir / "working"
    working.mkdir(parents=True)
    wc = working / "7.jpg"
    Image.new("RGB", (64, 48), (10, 20, 30)).save(str(wc), "JPEG")
    stale = time.time() - 30 * 24 * 3600
    os.utime(str(wc), (stale, stale))

    real_load_standard = image_loader._load_standard
    publisher_mtime = time.time() - 60

    def publish_over_load(path, max_size):
        image = real_load_standard(path, max_size)
        # Simulate a competing publisher writing over the file while we
        # decoded: their write set a fresh mtime AND atime.
        os.utime(path, (publisher_mtime, publisher_mtime))
        return image

    monkeypatch.setattr(image_loader, "_load_standard", publish_over_load)

    photo = {"id": 7, "working_copy_path": "working/7.jpg", "folder_id": 1}
    image = load_working_image(photo, str(vireo_dir), max_size=32)

    assert image is not None
    stat_after = os.stat(str(wc))
    assert stat_after.st_mtime == publisher_mtime, (
        "restore erased the concurrent publisher's mtime — their "
        "fresh write would then look older than the pre-read stale "
        "value and be reclaimed ahead of really-cold files: "
        f"mtime={stat_after.st_mtime} (publisher wrote {publisher_mtime})"
    )
    assert stat_after.st_atime == publisher_mtime, (
        "restore erased the publisher's atime too: "
        f"atime={stat_after.st_atime} (publisher wrote {publisher_mtime})"
    )


def test_load_working_image_stamps_when_access_is_recorded(tmp_path):
    """Request paths opt in and the stamp lands on atime, not mtime."""
    import os
    import time

    from image_loader import load_working_image
    from PIL import Image

    vireo_dir = tmp_path / "vireo"
    working = vireo_dir / "working"
    working.mkdir(parents=True)
    wc = working / "7.jpg"
    Image.new("RGB", (64, 48), (10, 20, 30)).save(str(wc), "JPEG")
    stale = time.time() - 30 * 24 * 3600
    os.utime(str(wc), (stale, stale))

    photo = {"id": 7, "working_copy_path": "working/7.jpg", "folder_id": 1}
    image = load_working_image(
        photo, str(vireo_dir), max_size=32, record_access=True,
    )

    assert image is not None
    stat_result = os.stat(str(wc))
    assert stat_result.st_atime > stale, (
        "an opted-in read left the working copy at its old recency, so it "
        "stays first in the eviction queue while the user is looking at it"
    )
    assert stat_result.st_mtime == stale, (
        "the stamp moved mtime, which is the content-version key the "
        "external-edit handoff and the display-cache gates read"
    )


def test_load_working_image_decode_is_not_serialized_by_the_eviction_guard(
    tmp_path, monkeypatch,
):
    """Interactive decodes must not hold the process-wide eviction lock.

    ``working_copy_publication_guard`` is a single process-wide
    ``RLock``, and the quota pass holds it across a ``scandir`` of the
    entire working-copy cache (measured at 42ms warm / 543ms cold on a
    17k-file cache, and this backfill exists to grow that cache past
    70k). Wrapping the decode in it too would make every interactive
    image read — 1:1 zoom, lightbox prefetch, burst-review sharpness
    regions — queue behind every other one and behind eviction. That
    stall is the failure mode this work is meant to remove, so the
    guard covers the recency stamp only.

    The residual races are benign and pre-existing: an unlink in the
    exists/open window falls through to the original source, and a
    publisher replacing the path between decode and touch costs one
    misattributed stamp on a file that is in active use either way.
    """
    import image_loader
    import working_copy_cache
    from PIL import Image

    vireo_dir = tmp_path / "vireo"
    working = vireo_dir / "working"
    working.mkdir(parents=True)
    wc = working / "7.jpg"
    Image.new("RGB", (64, 48), (10, 20, 30)).save(str(wc), "JPEG")

    lock_held_when_decoded = []
    lock_held_when_touched = []
    real_load_standard = image_loader._load_standard
    real_touch = working_copy_cache.touch_working_copy_access

    def spy_load_standard(path, max_size):
        # ``_eviction_lock`` is an ``RLock``; ``_is_owned`` reports
        # whether the calling thread already holds it.
        lock_held_when_decoded.append(
            working_copy_cache._eviction_lock._is_owned()
        )
        return real_load_standard(path, max_size)

    def spy_touch(path):
        lock_held_when_touched.append(
            working_copy_cache._eviction_lock._is_owned()
        )
        return real_touch(path)

    monkeypatch.setattr(image_loader, "_load_standard", spy_load_standard)
    monkeypatch.setattr(
        working_copy_cache, "touch_working_copy_access", spy_touch,
    )

    photo = {"id": 7, "working_copy_path": "working/7.jpg", "folder_id": 1}
    image = image_loader.load_working_image(
        photo, str(vireo_dir), max_size=32, record_access=True,
    )

    assert image is not None
    assert lock_held_when_decoded, (
        "test setup: the decode path was never called; the working "
        "copy may have looked missing"
    )
    assert not any(lock_held_when_decoded), (
        "``load_working_image(record_access=True)`` decoded the working "
        "copy while holding ``working_copy_publication_guard``. That "
        "lock is process-wide and eviction holds it across a full "
        "cache scandir, so every interactive image read in the app "
        "would serialize behind it: "
        f"{lock_held_when_decoded!r}"
    )
    assert lock_held_when_touched and all(lock_held_when_touched), (
        "the recency stamp must still run under the guard — an "
        "unguarded touch races ``_evict_once``'s identity check, which "
        "then skips the file as replaced and leaves the cache above a "
        f"lowered quota: {lock_held_when_touched!r}"
    )
