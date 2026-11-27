"""A remount of the photo volume must not orphan the evidence saved before it."""

import hashlib
import io
import json
import os

import pytest
from decoded_frame import DecodedFrame
from frame_identity import (
    LEGACY_DEVICE_WINDOW,
    LegacyFrame,
    SourceState,
    frame_identity,
    frame_matches,
    generation_matches,
    source_generation,
)
from image_loader import RAW_DECODE_ALIGNED, aligned_source_state, load_image
from PIL import Image
from PIL.PngImagePlugin import PngInfo
from quality import saved_mask_matches_frame

STATE = SourceState("/Volumes/Photography/DSC_0001.NEF", 905969669, 1748937, 52_000_000, 10**18, 10**18 + 5)


def _remounted(state, device):
    return SourceState(state.realpath, device, state.inode, state.size, state.mtime_ns, state.ctime_ns)


def _legacy_frame_id(state, rendition, native_size, device):
    """The identity masks and boxes carried before the device number was dropped."""
    before = [state.realpath, device, state.inode, state.size, state.mtime_ns, state.ctime_ns]
    return hashlib.sha256(json.dumps([1, RAW_DECODE_ALIGNED, before, rendition, list(native_size)]).encode()).hexdigest()


def _legacy_generation(state, device):
    before = [state.realpath, device, state.inode, state.size, state.mtime_ns, state.ctime_ns]
    return hashlib.sha256(json.dumps(before).encode()).hexdigest()


def test_identities_ignore_the_volume_device_number():
    remounted = _remounted(STATE, STATE.device + 1)
    assert source_generation(remounted) == source_generation(STATE)
    assert frame_identity(remounted, RAW_DECODE_ALIGNED, "image", (5392, 3592)) == frame_identity(
        STATE, RAW_DECODE_ALIGNED, "image", (5392, 3592))


@pytest.mark.parametrize("field", ["realpath", "inode", "size", "mtime_ns", "ctime_ns"])
def test_identities_still_follow_the_file_generation(field):
    changed = SourceState(**{**STATE.__dict__, field: "/other" if field == "realpath" else getattr(STATE, field) + 1})
    assert source_generation(changed) != source_generation(STATE)
    assert frame_identity(changed, RAW_DECODE_ALIGNED, "image", (10, 8)) != frame_identity(
        STATE, RAW_DECODE_ALIGNED, "image", (10, 8))


def test_a_frame_id_saved_under_an_earlier_mount_still_matches():
    legacy = LegacyFrame(STATE, RAW_DECODE_ALIGNED, "embedded-preview", (5392, 3592))
    info = {"vireo_subject_frame_id": frame_identity(STATE, RAW_DECODE_ALIGNED, "embedded-preview", (5392, 3592)),
            "vireo_legacy_frame": legacy}
    for device in (STATE.device, STATE.device - 1, STATE.device + 3, STATE.device - LEGACY_DEVICE_WINDOW):
        assert frame_matches(_legacy_frame_id(STATE, "embedded-preview", (5392, 3592), device), info)


def test_a_legacy_frame_id_of_another_rendition_or_generation_does_not_match():
    info = {"vireo_subject_frame_id": "x" * 64,
            "vireo_legacy_frame": LegacyFrame(STATE, RAW_DECODE_ALIGNED, "embedded-preview", (5392, 3592))}
    previous = SourceState(**{**STATE.__dict__, "mtime_ns": STATE.mtime_ns - 1})
    assert not frame_matches(_legacy_frame_id(STATE, "image", (5392, 3592), STATE.device - 1), info)
    assert not frame_matches(_legacy_frame_id(STATE, "embedded-preview", (5408, 3600), STATE.device - 1), info)
    assert not frame_matches(_legacy_frame_id(previous, "embedded-preview", (5392, 3592), STATE.device - 1), info)
    far = STATE.device - LEGACY_DEVICE_WINDOW - 1
    assert not frame_matches(_legacy_frame_id(STATE, "embedded-preview", (5392, 3592), far), info)


def test_a_working_copy_matches_only_the_identity_stamped_on_it():
    stamped = _legacy_frame_id(STATE, "image", (10, 8), STATE.device)
    assert frame_matches(stamped, {"vireo_subject_frame_id": stamped})
    assert not frame_matches(_legacy_frame_id(STATE, "image", (10, 8), STATE.device - 1),
                             {"vireo_subject_frame_id": stamped})
    assert not frame_matches(None, {"vireo_subject_frame_id": None})
    assert not frame_matches("", {"vireo_subject_frame_id": ""})


def test_a_generation_recorded_under_an_earlier_mount_still_matches():
    assert generation_matches(source_generation(STATE), _remounted(STATE, STATE.device + 7))
    assert generation_matches(_legacy_generation(STATE, STATE.device - 1), STATE)
    moved = SourceState(**{**STATE.__dict__, "inode": STATE.inode + 1})
    assert not generation_matches(_legacy_generation(moved, STATE.device - 1), STATE)
    assert not generation_matches(None, STATE)


def _decode(path):
    image = load_image(str(path), max_size=400, raw_decode=RAW_DECODE_ALIGNED)
    assert image is not None
    return image


def _decoded(tmp_path):
    path = tmp_path / "photo.jpg"
    Image.new("RGB", (800, 600), (90, 120, 60)).save(path)
    return path, _decode(path)


def test_a_mask_saved_before_the_volume_was_remounted_is_still_held(tmp_path):
    path, image = _decoded(tmp_path)
    state = aligned_source_state(path)
    assert image.info["vireo_subject_frame_id"] == frame_identity(state, RAW_DECODE_ALIGNED, "image", (800, 600))
    saved_before_remount = _legacy_frame_id(state, "image", (800, 600), state.device - 1)

    assert DecodedFrame.of(image).holds_mask(saved_before_remount, (400, 300))
    assert saved_mask_matches_frame((400, 300), None, image, mask_frame_id=saved_before_remount)

    os.utime(path, ns=(state.mtime_ns + 10**9, state.mtime_ns + 10**9))
    assert not DecodedFrame.of(_decode(path)).holds_mask(saved_before_remount, (400, 300))


def test_the_focus_mask_overlay_accepts_a_mask_from_before_the_remount(tmp_path):
    from focus_crop import shade_focus_mask

    path, image = _decoded(tmp_path)
    state = aligned_source_state(path)
    meta = PngInfo()
    meta.add_text("vireo_subject_frame_id", _legacy_frame_id(state, "image", (800, 600), state.device - 1))
    mask = io.BytesIO()
    Image.new("L", (400, 300), 255).save(mask, "PNG", pnginfo=meta)
    window = (0, 0, 200, 150)
    shaded = shade_focus_mask(image.crop(window), mask.getvalue(), image.size, window, Image,
                              frame=DecodedFrame.of(image), require_frame_id=True)
    assert shaded.size == (200, 150)
