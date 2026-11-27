"""Synthetic detector -> catalog -> SAM prompt coordinate lineage."""
from types import SimpleNamespace

import numpy as np
import pytest
from PIL import Image
from services import detection_store, photo_ingest, workspace_folders


@pytest.fixture
def cropped_raw_scene(tmp_path, monkeypatch):
    import detector
    import image_loader

    pixels = np.zeros((1000, 1600, 3), np.uint8)
    pixels[650:750, 640:960, 0] = 240
    pixels[650:750, 640:960, 1] = 180
    sensor = Image.fromarray(pixels)
    preview = sensor.crop((0, 100, 1600, 900))
    source = tmp_path / "synthetic.nef"
    source.write_bytes(b"synthetic RAW; loader and inference are patched")

    def load(_path, max_size=1024, raw_decode=image_loader.RAW_DECODE_JPEG_FIRST):
        use_preview = raw_decode not in (image_loader.RAW_DECODE_CAMERA_RENDERED, image_loader.RAW_DECODE_ALIGNED) and max_size and max_size <= 1600
        image = (preview if use_preview else sensor).copy()
        if raw_decode in (image_loader.RAW_DECODE_CAMERA_RENDERED, image_loader.RAW_DECODE_ALIGNED):
            image.info["vireo_rejected_preview_edge"] = 1600
        if raw_decode == image_loader.RAW_DECODE_ALIGNED:
            image.info["vireo_aligned_rendition"] = "sensor"
            image.info["vireo_subject_frame_id"] = image_loader.aligned_frame_identity(
                _path, image, image_loader.aligned_source_state(_path))
        if max_size:
            image.thumbnail((max_size, max_size))
        return image

    def infer(_session, _name, image, _floor):
        ys, xs = np.where(image[:, :, 0] > 150)
        height, width = image.shape[:2]
        return [{"box": {"x": xs.min() / width, "y": ys.min() / height,
                         "w": (xs.max() + 1 - xs.min()) / width,
                         "h": (ys.max() + 1 - ys.min()) / height},
                 "confidence": .95, "category": "animal"}]

    monkeypatch.setattr(image_loader, "load_image", load)
    monkeypatch.setattr(detector, "_get_session", lambda: SimpleNamespace(
        get_inputs=lambda: [SimpleNamespace(name="pixels")],
    ))
    monkeypatch.setattr(detector, "_infer_array", infer)
    return source


@pytest.mark.parametrize("weak", [False, True])
def test_detected_box_reaches_sam_in_the_same_sensor_coordinates(db, cropped_raw_scene, monkeypatch, weak):
    import computation_cache
    import detector
    import masking
    from subject_analysis import analyze_detection

    source = cropped_raw_scene
    if weak:
        real_infer = detector._infer_array
        inference_calls = []

        def weak_then_strong(*args):
            result = real_infer(*args)
            result[0]["confidence"] = .05 if not inference_calls else .95
            inference_calls.append(1)
            return result

        monkeypatch.setattr(detector, "_infer_array", weak_then_strong)
        monkeypatch.setattr(detector, "_tiled_fallback", lambda *_args: [])
    folder = workspace_folders.add_folder(db, str(source.parent), name="Synthetic")
    pid = photo_ingest.add_photo(db, folder_id=folder, filename=source.name, extension=".nef",
                                 file_size=source.stat().st_size, file_mtime=source.stat().st_mtime)
    detections = detector.detect_animals(source)
    if weak:
        assert len(inference_calls) == 2
    ids = detection_store.write_detection_batch(
        db, pid, "megadetector-v6", detections, runtime_fingerprint="canonical-runtime",
    )
    selected = dict(detection_store.get_detections(db, pid, min_conf=0)[0])
    assert selected["id"] == ids[0]
    monkeypatch.setattr(computation_cache, "megadetector_runtime_fingerprint", lambda: "canonical-runtime")
    prompts = []

    def segment(image, box, variant):
        prompts.append(box)
        # Assert the semantic region, not just matching mask/image dimensions.
        assert box == pytest.approx({"x": .4, "y": .65, "w": .2, "h": .1}, abs=.003)
        return np.asarray(image)[:, :, 0] > 150

    monkeypatch.setattr(masking, "generate_mask", segment)
    analyze_detection(db, db.photos.get(pid), selected, {}, lambda: None, defer_publication=True)
    assert len(prompts) == 1


def test_historical_cropped_preview_prompt_is_refused_before_sam(db, cropped_raw_scene, monkeypatch):
    import computation_cache
    import masking
    from subject_analysis import analyze_detection

    source = cropped_raw_scene
    folder = workspace_folders.add_folder(db, str(source.parent), name="Synthetic")
    pid = photo_ingest.add_photo(db, folder_id=folder, filename=source.name, extension=".nef",
                                 file_size=source.stat().st_size, file_mtime=source.stat().st_mtime)
    detection_store.write_detection_batch(db, pid, "megadetector-v6", [{
        "box": {"x": .4, "y": .6875, "w": .2, "h": .125}, "confidence": .95,
    }], runtime_fingerprint="older-runtime")
    selected = dict(detection_store.get_detections(db, pid, min_conf=0)[0])
    monkeypatch.setattr(computation_cache, "megadetector_runtime_fingerprint", lambda: "canonical-runtime")
    monkeypatch.setattr(masking, "generate_mask", lambda *_args, **_kwargs: pytest.fail("SAM must not run"))
    with pytest.raises(ValueError, match="original-frame proof"):
        analyze_detection(db, db.photos.get(pid), selected, {}, lambda: None, defer_publication=True)
    assert dict(detection_store.get_detections(db, pid, min_conf=0)[0]) == selected


def test_camera_mask_without_verified_prompt_provenance_is_not_reused(cropped_raw_scene, tmp_path):
    from PIL.PngImagePlugin import PngInfo
    from quality import MASK_CACHE_INVALID, measure_cached_subject_detail

    metadata = PngInfo()
    metadata.add_text("vireo_mask_frame", "camera_rendered")
    stale = tmp_path / "old-mask.png"
    # Correct sensor aspect ratio does not prove that the old SAM box was aligned.
    Image.new("L", (1024, 640), 255).save(stale, pnginfo=metadata)
    assert measure_cached_subject_detail(stale, cropped_raw_scene, (1600, 1000)) is MASK_CACHE_INVALID


def test_detector_runtime_identity_changes_with_decode_recipe(tmp_path, monkeypatch):
    import computation_cache
    import detector

    fake_weights = tmp_path / "synthetic-model.onnx"
    fake_weights.write_bytes(b"synthetic bytes; never loaded as a model")
    monkeypatch.setattr(detector, "MEGADETECTOR_ONNX_PATH", str(fake_weights))
    current = computation_cache.megadetector_runtime_fingerprint()
    fingerprint = computation_cache.runtime_fingerprint

    def historical_fingerprint(descriptor):
        descriptor["preprocess"]["version"] = 9
        descriptor["preprocess"].pop("raw_decode")
        return fingerprint(descriptor)

    monkeypatch.setattr(computation_cache, "runtime_fingerprint", historical_fingerprint)
    old = computation_cache.megadetector_runtime_fingerprint()
    monkeypatch.setattr(computation_cache, "runtime_fingerprint", fingerprint)
    assert current != old
    assert computation_cache._is_recognized_detector_runtime("megadetector-v6", current)
    assert not computation_cache._is_recognized_detector_runtime("megadetector-v6", old)


def test_redrawing_on_aligned_original_repairs_legacy_reviewer_box(app_and_db, cropped_raw_scene, tmp_path):
    import io

    from image_loader import subject_box_image_path
    from subjects import analyze_photo

    app, db = app_and_db
    source = cropped_raw_scene
    folder = workspace_folders.add_folder(db, str(source.parent), name="Reviewer frame")
    pid = photo_ingest.add_photo(db, folder, source.name, ".nef", source.stat().st_size, source.stat().st_mtime)
    # A JPEG companion can have different framing even with the original online.
    companion = tmp_path / "working.jpg"
    Image.new("RGB", (1600, 800), "green").save(companion)
    db.conn.execute("UPDATE photos SET working_copy_path=? WHERE id=?", (companion.name, pid))
    db.commit()
    photo = db.photos.get(pid)
    path = subject_box_image_path(photo, str(tmp_path), {folder: str(source.parent)})
    assert path == str(source)
    client = app.test_client()
    url = f"/api/photos/{pid}/subjects/reviewer-box"
    box = {"x": .4, "y": .65, "w": .2, "h": .1}
    previous = client.put(url, json={"box": box}).get_json()["primary_detection_id"]
    with pytest.raises(ValueError, match="confirm or redraw"):
        analyze_photo(db, pid, path, detection_id=previous)
    response = client.get(f"/photos/{pid}/preview?view=subject&size=1920")
    assert response.status_code == 200
    with Image.open(io.BytesIO(response.data)) as frame:
        assert frame.size == (1600, 1000)
        assert frame.getpixel((800, 700))[0] > 200
    token = response.headers["X-Vireo-Subject-Frame"]
    result = client.put(url, json={"operation": "edit", "revision": client.get(f"/api/photos/{pid}/subjects").json["subjects"][0]["revision"],
                                   "detection_id": previous, "box": box, "frame_token": token}).get_json()
    assert result["primary_detection_id"] == previous
    assert analyze_photo(db, pid, path, detection_id=previous) == 1
    assert client.post("/api/undo").status_code == 200
    with pytest.raises(ValueError, match="confirm or redraw"):
        analyze_photo(db, pid, path, detection_id=previous, force=True)
    assert client.post("/api/redo").status_code == 200
    assert analyze_photo(db, pid, path, detection_id=previous, force=True) == 1


@pytest.mark.parametrize("tiled_switch", [False, True])
def test_detector_frame_proof_tracks_transient_thumbnail_failures(db, tmp_path, monkeypatch, tiled_switch):
    import detector
    import image_loader
    import masking
    import quality
    from detection_id import detection_prompt_runtime
    from test_image_loader import _FakeRaw, _install_fake_raw, _jpeg_bytes

    source = tmp_path / "fake.nef"
    source.write_bytes(b"synthetic fake RAW; never opened by libraw")
    raw = _install_fake_raw(monkeypatch, _FakeRaw(
        embedded_jpeg=_jpeg_bytes((600, 400)), sensor_size=(600, 400), postprocess_size=(600, 400)))
    import rawpy

    thumb_available = [True]
    extract = raw.extract_thumb

    def extract_thumb():
        if not thumb_available[0]:
            raise rawpy.LibRawIOError(b"synthetic transient thumbnail failure")
        return extract()

    monkeypatch.setattr(raw, "extract_thumb", extract_thumb)
    monkeypatch.setattr(detector, "_get_session", lambda: SimpleNamespace(get_inputs=lambda: [SimpleNamespace(name="pixels")]))
    calls = []

    def infer(*args):
        calls.append(1)
        if tiled_switch and len(calls) == 1:
            # The first pass reads the preview; the tiled read loses it.
            thumb_available[0] = False
            confidence = .01
        else:
            confidence = .95
        return [{"box": {"x": .2, "y": .2, "w": .4, "h": .4}, "confidence": confidence, "category": "animal"}]

    monkeypatch.setattr(detector, "_infer_array", infer)
    monkeypatch.setattr(detector, "_tiled_fallback", lambda *args: [])
    detections = detector.detect_animals(source)
    folder = workspace_folders.add_folder(db, str(tmp_path))
    pid = photo_ingest.add_photo(db, folder, source.name, ".nef", 100, 1)
    did, = detection_store.write_detection_batch(db, pid, "megadetector-v6", detections, "synthetic-model")
    selected = dict(db.detections.row(did))
    assert selected["frame_id"] == detections[0]["frame_id"]
    thumb_available[0] = False
    with image_loader.load_image(source, raw_decode=image_loader.RAW_DECODE_ALIGNED) as sensor:
        assert "vireo_rejected_preview_edge" not in sensor.info
        if tiled_switch:
            masking.require_aligned_detector_prompt(sensor, detection_prompt_runtime(selected), detector_model="megadetector-v6")
        else:
            with pytest.raises(masking.PromptFrameError, match="frame changed"):
                masking.require_aligned_detector_prompt(sensor, detection_prompt_runtime(selected), detector_model="megadetector-v6")
            with pytest.raises(masking.PromptFrameError, match="frame changed"):
                masking.require_original_reviewer_frame(source, detection_prompt_runtime(selected), detector_model="megadetector-v6")
            assert quality.measure_subject_detail(np.ones((400, 600), bool), source_path=source,
                                                  expected_frame_id=detections[0]["frame_id"]) == {}
