"""Old boxes and masks are reused only where every decode gives one frame."""
import io

import numpy as np
import pytest
from decoded_frame import DecodedFrame
from image_loader import RAW_DECODE_ALIGNED, load_image
from masking import PromptFrameError, require_aligned_detector_prompt, require_original_reviewer_frame
from PIL import Image
from test_image_loader import _FakeRaw, _install_fake_raw

SENSOR, PREVIEW, CROPPED = (828, 552), (824, 548), (640, 360)


def _raw(tmp_path, monkeypatch, *, sensor=SENSOR, preview=PREVIEW):
    """A NEF whose camera JPEG holds a textured subject at its center."""
    jpeg = None
    if preview:
        pixels = np.full((preview[1], preview[0], 3), 120, np.uint8)
        y, x = preview[1] // 4, preview[0] // 4
        pixels[y:3 * y:2, x:3 * x] = 250
        encoded = io.BytesIO()
        Image.fromarray(pixels).save(encoded, "JPEG", quality=95)
        jpeg = encoded.getvalue()
    _install_fake_raw(monkeypatch, _FakeRaw(embedded_jpeg=jpeg, sensor_size=sensor, postprocess_size=sensor))
    source = tmp_path / "bird.nef"
    source.touch()
    return source


def _aligned(source):
    return load_image(source, max_size=400, raw_decode=RAW_DECODE_ALIGNED)


def _unmarked_mask(path, size):
    mask = np.zeros((size[1], size[0]), np.uint8)
    mask[size[1] // 4:3 * size[1] // 4, size[0] // 4:3 * size[0] // 4] = 255
    Image.fromarray(mask).save(path)
    return path


@pytest.mark.parametrize("native", [
    (8256, 5504), (8288, 5520), (5392, 3592), (5408, 3608), (5504, 8256), (4032, 3024), (3000, 40), (7, 5000),
])
@pytest.mark.parametrize("longest", [683, 1024, 1536, 2048, 9000])
def test_a_mask_of_the_size_thumbnail_gives_lies_in_the_frame(native, longest):
    with Image.new("1", native) as image:
        image.thumbnail((longest, longest), Image.Resampling.NEAREST)
        size = image.size
    frame = DecodedFrame("id", native, "embedded-preview")
    assert frame.holds_mask(None, size)
    assert not frame.holds_mask(None, (size[0], size[1] + 2) if size[0] >= size[1] else (size[0] + 2, size[1]))


def test_camera_preview_masks_are_told_from_sensor_masks_by_size():
    # D850 and Z 8 DX: preview and sensor proxies at the two proxy settings.
    for native, preview_sizes, sensor_sizes in (
        ((8256, 5504), ((1536, 1024), (2048, 1365)), ((1536, 1023), (2048, 1364))),
        ((5392, 3592), ((1536, 1023), (2048, 1364)), ((1536, 1025), (2048, 1366))),
    ):
        frame = DecodedFrame("id", native, "embedded-preview")
        assert all(frame.holds_mask(None, size) for size in preview_sizes)
        assert not any(frame.holds_mask(None, size) for size in sensor_sizes)


def test_a_recorded_identity_decides_over_the_size():
    frame = DecodedFrame("id", (800, 600), "image")
    assert frame.holds_mask("id", (1, 1))
    assert not frame.holds_mask("other", (400, 300))
    assert not DecodedFrame(None, (800, 600), "image").holds_mask("id", (400, 300))
    # Without the native size (a working copy) nothing vouches for an unmarked mask.
    assert not DecodedFrame("id").holds_mask(None, (400, 300))


@pytest.mark.parametrize(("preview", "accepted"), [(PREVIEW, True), (CROPPED, False), (None, False)])
def test_an_unbound_box_is_accepted_only_on_a_near_full_camera_preview(tmp_path, monkeypatch, preview, accepted):
    source = _raw(tmp_path, monkeypatch, preview=preview)
    with _aligned(source) as image:
        if accepted:
            require_aligned_detector_prompt(image, "older-runtime", detector_model="megadetector-v6")
            frame = require_original_reviewer_frame(source, "older-runtime", detector_model="megadetector-v6")
            assert frame == DecodedFrame.of(image)
            assert frame.native_size == preview
        else:
            with pytest.raises(PromptFrameError, match="original-frame proof"):
                require_aligned_detector_prompt(image, "older-runtime", detector_model="megadetector-v6")


def test_reviewer_boxes_and_companion_boxes_still_need_their_own_proof(tmp_path, monkeypatch):
    from detection_id import COMPANION_FRAME_RUNTIME

    with _aligned(_raw(tmp_path, monkeypatch)) as image:
        with pytest.raises(PromptFrameError, match="confirm or redraw"):
            require_aligned_detector_prompt(image, "reviewer-v1", detector_model="reviewer-selected")
        with pytest.raises(PromptFrameError, match="JPEG companion"):
            require_aligned_detector_prompt(image, COMPANION_FRAME_RUNTIME, detector_model="megadetector-v6")


@pytest.mark.parametrize(("preview", "size", "reused"), [
    (PREVIEW, (412, 274), True),  # the preview at half size
    (PREVIEW, (412, 275), False),  # another frame's aspect ratio
    (CROPPED, (640, 360), False),
])
def test_detail_of_an_unmarked_mask_is_measured_only_in_its_own_frame(tmp_path, monkeypatch, preview, size, reused):
    from quality import MASK_CACHE_INVALID, measure_cached_subject_detail

    source = _raw(tmp_path, monkeypatch, preview=preview)
    measured = measure_cached_subject_detail(_unmarked_mask(tmp_path / "mask.png", size), source, SENSOR)
    if reused:
        assert measured["subject_detail"] > 0
    else:
        assert measured is MASK_CACHE_INVALID


def test_focus_crop_shades_with_an_unmarked_mask_of_the_same_frame(tmp_path, monkeypatch):
    from web.media_crops import _focus_masked_crop

    source = _raw(tmp_path, monkeypatch)
    mask = _unmarked_mask(tmp_path / "mask.png", (412, 274))
    with load_image(source, max_size=None, raw_decode=RAW_DECODE_ALIGNED) as image:
        crop = _focus_masked_crop(image, (150, 200, 260, 300), mask.read_bytes(), True)
        # Background pixels darken; the subject keeps the camera's values.
        assert crop.getpixel((5, 50))[0] < 60
        assert crop.getpixel((105, 51))[0] > 100
        other = _unmarked_mask(tmp_path / "other.png", (412, 260))
        with pytest.raises(ValueError, match="incompatible"):
            _focus_masked_crop(image, (150, 200, 260, 300), other.read_bytes(), True)


def test_process_reuses_an_unmarked_mask_without_running_sam(tmp_path, monkeypatch):
    from types import SimpleNamespace

    from pipeline_stages.features import _MaskPass

    source = _raw(tmp_path, monkeypatch)
    path = _unmarked_mask(tmp_path / "mask.png", (412, 274))
    prompt = (.25, .25, .5, .5)
    existing = {"detector_model": "megadetector-v6", "path": str(path), "subject_detail": None,
                **dict(zip(("prompt_x", "prompt_y", "prompt_w", "prompt_h"), prompt, strict=True))}
    state = {"active_mask_variant": "sam2-large", "dino_embedding_variant": "vit-b14", "quality_input_recipe": None}
    stage = object.__new__(_MaskPass)
    stage.sam2_variant, stage.dinov2_variant = "sam2-large", "vit-b14"
    stage.require_original_reviewer_frame = require_original_reviewer_frame
    stage.run = SimpleNamespace(params=SimpleNamespace(raw_subject_analysis=False))
    stage.thread_db = SimpleNamespace(masks_features=SimpleNamespace(
        get_mask=lambda *_args: existing, photo_mask_state=lambda *_args: state))
    item = SimpleNamespace(entry={"detector_model": "megadetector-v6", "prompt": prompt}, photo_id=1,
                           image_path=str(source), detector_runtime="legacy")
    assert stage._reuse_cached_mask(item)
    _unmarked_mask(path, (412, 275))
    assert not stage._reuse_cached_mask(item)
