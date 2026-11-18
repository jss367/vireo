"""Original-frame confirmation and recovery without any model inference."""
import io
from pathlib import Path

import pytest
from PIL import Image
from services import photo_ingest, workspace_folders


@pytest.fixture
def original_photo(app_and_db, tmp_path):
    app, db = app_and_db
    folder = workspace_folders.add_folder(db, str(tmp_path), name="Synthetic frame")
    source = tmp_path / "original.png"
    Image.new("RGB", (320, 200), "green").save(source)
    pid = photo_ingest.add_photo(db, folder, source.name, ".png", source.stat().st_size, source.stat().st_mtime)
    db.commit()
    return app, db, pid, source


def _token(client, pid):
    response = client.get(f"/photos/{pid}/preview?view=subject&size=1920")
    assert response.status_code == 200
    with Image.open(io.BytesIO(response.data)) as image:
        assert image.size == (320, 200)
    return response.headers["X-Vireo-Subject-Frame"]


def _save(client, pid, token):
    return client.put(f"/api/photos/{pid}/subjects/reviewer-box", json={
        "box": {"x": .2, "y": .2, "w": .4, "h": .4}, "frame_token": token,
    })


def test_client_frame_label_is_not_a_verification(original_photo):
    app, db, pid, _source = original_photo
    client = app.test_client()
    response = client.put(f"/api/photos/{pid}/subjects/reviewer-box", json={
        "box": {"x": .2, "y": .2, "w": .4, "h": .4}, "frame": "aligned-original-v1",
    })
    assert response.status_code == 400
    assert db.subjects.choice(pid) is None


def test_served_frame_token_is_photo_bound_and_tamper_resistant(original_photo):
    app, db, pid, source = original_photo
    client = app.test_client()
    token = _token(client, pid)
    photo = db.photos.get(pid)
    other = photo_ingest.add_photo(db, photo["folder_id"], "different.png", ".png", source.stat().st_size, 1)
    db.commit()
    assert _save(client, other, token).status_code == 400
    pieces = token.split(".")
    pieces[-1] = ("A" if pieces[-1][0] != "A" else "B") + pieces[-1][1:]
    assert _save(client, pid, ".".join(pieces)).status_code == 400
    assert db.subjects.choice(pid) is None
    assert _save(client, pid, token).status_code == 200


def test_source_change_while_drawing_preserves_the_previous_choice(original_photo):
    app, db, pid, source = original_photo
    client = app.test_client()
    first = _save(client, pid, _token(client, pid)).get_json()["primary_detection_id"]
    before = dict(db.subjects.choice(pid))
    stale = _token(client, pid)
    Image.new("RGB", (320, 200), "red").save(source)
    response = _save(client, pid, stale)
    assert response.status_code == 400 and "changed while drawing" in response.get_json()["error"]
    assert dict(db.subjects.choice(pid)) == before
    assert _save(client, pid, _token(client, pid)).get_json()["primary_detection_id"] == first


def test_redrawing_on_a_changed_frame_replaces_the_previous_box(original_photo):
    from subjects import analyze_photo, retained

    app, db, pid, source = original_photo
    client = app.test_client()
    old = _save(client, pid, _token(client, pid)).get_json()["primary_detection_id"]
    Image.new("RGB", (320, 200), "blue").save(source)
    response = client.put(f"/api/photos/{pid}/subjects/reviewer-box", json={
        "box": {"x": .3, "y": .1, "w": .2, "h": .5}, "frame_token": _token(client, pid),
    })
    new = response.get_json()["primary_detection_id"]
    assert new != old
    assert [row["id"] for row in retained(db, pid)] == [new]
    assert analyze_photo(db, pid, str(source)) == 1
    assert client.post("/api/undo").status_code == 200
    assert [row["id"] for row in retained(db, pid)] == [old]


def test_cached_reviewer_proof_checked_when_another_box_needs_analysis(original_photo, monkeypatch):
    import image_loader
    from masking import PromptFrameError
    from subjects import analyze_photo

    app, db, pid, source = original_photo
    client = app.test_client()
    _save(client, pid, _token(client, pid))
    assert analyze_photo(db, pid, str(source)) == 1
    box = {"x": .1, "y": .1, "w": .2, "h": .2}
    other, = db.detections.upsert_rows(pid, "megadetector-v6", [{"box": box, "confidence": .95}])
    db.commit()
    original_load = image_loader.load_image

    def switched_frame(*args, **kwargs):
        image = original_load(*args, **kwargs)
        image.info["vireo_subject_frame_id"] = "changed-rendition"
        return image

    monkeypatch.setattr(image_loader, "load_image", switched_frame)
    with pytest.raises(PromptFrameError, match="frame changed"):
        analyze_photo(db, pid, str(source))
    assert db.subjects.analysis(other) is None


def test_confirmed_frame_is_checked_again_during_analysis_and_history_replay(original_photo):
    from masking import PromptFrameError
    from subjects import analyze_photo

    app, db, pid, source = original_photo
    client = app.test_client()
    did = _save(client, pid, _token(client, pid)).get_json()["primary_detection_id"]
    assert analyze_photo(db, pid, str(source), detection_id=did) == 1
    Image.new("RGB", (320, 200), "red").save(source)
    with pytest.raises(PromptFrameError, match="frame changed"):
        analyze_photo(db, pid, str(source), detection_id=did, force=True)
    assert client.post("/api/undo").status_code == 200
    assert client.post("/api/redo").status_code == 200
    with pytest.raises(PromptFrameError, match="frame changed"):
        analyze_photo(db, pid, str(source), detection_id=did, force=True)


def test_offline_working_copy_can_be_browsed_but_cannot_authorize_new_analysis(original_photo):
    from app_paths import media_root
    from image_loader import get_canonical_image_path
    from subjects import analyze_photo, select_reviewer_box

    app, db, pid, source = original_photo
    root = Path(media_root(app.config["THUMB_CACHE_DIR"]))
    working = root / "synthetic-working.jpg"
    working.parent.mkdir(parents=True, exist_ok=True)
    Image.new("RGB", (320, 200), "green").save(working)
    db.conn.execute("UPDATE photos SET working_copy_path=? WHERE id=?", (working.name, pid))
    db.commit()
    did = select_reviewer_box(db, pid, {"x": .2, "y": .2, "w": .4, "h": .4})
    source.unlink()
    photo = db.photos.get(pid)
    assert get_canonical_image_path(photo, str(root), {photo["folder_id"]: str(source.parent)}) == str(working)
    client = app.test_client()
    assert client.get(f"/photos/{pid}/preview?view=source&size=1920").status_code == 200
    assert client.get(f"/photos/{pid}/preview?view=subject&size=1920").status_code == 404
    with pytest.raises(ValueError, match="reconnect the original"):
        analyze_photo(db, pid, str(working), detection_id=did, force=True)
    assert db.subjects.choice(pid)["detection_id"] == did


def test_frame_token_expires_and_cannot_cross_workspaces(original_photo, monkeypatch):
    from itsdangerous import TimestampSigner
    from services.subject_frames import verified_reviewer_runtime

    app, db, pid, _source = original_photo
    token = _token(app.test_client(), pid)
    current = TimestampSigner.get_timestamp
    monkeypatch.setattr(TimestampSigner, "get_timestamp", lambda self: current(self) + 3600)
    with pytest.raises(ValueError, match="expired"):
        verified_reviewer_runtime(db, pid, token)
    monkeypatch.setattr(TimestampSigner, "get_timestamp", current)
    db.set_active_workspace(db.create_workspace("Synthetic other"))
    with pytest.raises(ValueError, match="another photo or workspace"):
        verified_reviewer_runtime(db, pid, token)


def test_confirmation_retires_cache_rows_without_deleting_masks_or_predictions(original_photo, tmp_path):
    from repositories.masks_features import MaskQuality

    app, db, pid, _source = original_photo
    client = app.test_client()
    did = client.put(f"/api/photos/{pid}/subjects/reviewer-box", json={
        "box": {"x": .2, "y": .2, "w": .4, "h": .4},
    }).get_json()["primary_detection_id"]
    immutable = tmp_path / "immutable-mask.png"
    Image.new("L", (320, 200), 255).save(immutable)
    before = immutable.read_bytes()
    db.masks_features.upsert_mask(pid, "sam2-small", str(immutable), "reviewer-selected", (.2, .2, .4, .4),
                                  MaskQuality(subject_detail=123, subject_detail_px=100))
    db.predictions.add(did, "Synthetic bird", .9, "synthetic-classifier", status="accepted")
    db.conn.execute("UPDATE photos SET rating=5 WHERE id=?", (pid,))
    db.commit()
    predictions = [dict(row) for row in db.subjects.visible_predictions(db.require_workspace_id(), pid, 0)]
    assert _save(client, pid, _token(client, pid)).get_json()["primary_detection_id"] == did
    assert db.masks_features.get_mask(pid, "sam2-small") is None
    assert immutable.read_bytes() == before
    assert [dict(row) for row in db.subjects.visible_predictions(db.require_workspace_id(), pid, 0)] == predictions
    assert db.photos.get(pid)["rating"] == 5


def test_pipeline_cache_hit_revalidates_the_reviewer_frame(original_photo):
    from types import SimpleNamespace

    from masking import PromptFrameError, require_original_reviewer_frame
    from pipeline_stages.features import _MaskPass

    app, db, pid, source = original_photo
    client = app.test_client()
    did = _save(client, pid, _token(client, pid)).get_json()["primary_detection_id"]
    runtime = db.conn.execute("SELECT runtime_fingerprint FROM detections WHERE id=?", (did,)).fetchone()[0]
    Image.new("RGB", (320, 200), "red").save(source)
    stage = object.__new__(_MaskPass)
    stage.sam2_variant = "sam2-small"
    stage.require_original_reviewer_frame = require_original_reviewer_frame
    stage.thread_db = SimpleNamespace(masks_features=SimpleNamespace(get_mask=lambda *_args: {"subject_detail": 123}))
    item = SimpleNamespace(entry={}, photo_id=pid, image_path=str(source), detector_runtime=runtime)
    with pytest.raises(PromptFrameError, match="frame changed"):
        stage._reuse_cached_mask(item)


def test_aligned_frame_identity_is_size_independent_and_distinguishes_actual_renditions(tmp_path, monkeypatch):
    from image_loader import RAW_DECODE_ALIGNED, load_image
    from test_image_loader import _FakeRaw, _install_fake_raw, _jpeg_bytes

    source = tmp_path / "synthetic.nef"
    source.write_bytes(b"synthetic RAW")
    _install_fake_raw(monkeypatch, _FakeRaw(
        embedded_jpeg=_jpeg_bytes((600, 400)), sensor_size=(600, 400), postprocess_size=(600, 400),
    ))
    with load_image(source, max_size=128, raw_decode=RAW_DECODE_ALIGNED) as small:
        preview_id = small.info["vireo_subject_frame_id"]
    with load_image(source, max_size=None, raw_decode=RAW_DECODE_ALIGNED) as full:
        assert full.info["vireo_subject_frame_id"] == preview_id
    _install_fake_raw(monkeypatch, _FakeRaw(sensor_size=(600, 400), postprocess_size=(600, 400)))
    with load_image(source, max_size=128, raw_decode=RAW_DECODE_ALIGNED) as demosaic:
        assert demosaic.info["vireo_subject_frame_id"] != preview_id


def test_detail_measurement_refuses_a_different_frame_on_reload(tmp_path, monkeypatch):
    import numpy as np
    import quality

    image = Image.new("RGB", (100, 80))
    image.info["vireo_subject_frame_id"] = "different-frame"
    monkeypatch.setattr(quality, "load_detail_source", lambda *_args: image)
    assert quality.measure_subject_detail(np.ones((80, 100), bool), source_path="synthetic",
                                          expected_frame_id="segmented-frame") == {}


@pytest.mark.parametrize("legacy_version", [48, 50])
def test_migration_retires_derived_box_scores_but_preserves_human_data(original_photo, tmp_path, legacy_version):
    import sqlite3

    import schema
    from repositories.masks_features import MaskQuality
    from subjects import select_reviewer_box

    _app, db, pid, _source = original_photo
    did = select_reviewer_box(db, pid, {"x": .2, "y": .2, "w": .4, "h": .4})
    path = tmp_path / "preserved-mask.png"
    Image.new("L", (100, 80), 255).save(path)
    db.masks_features.upsert_mask(pid, "sam2-small", str(path), "reviewer-selected", (.2, .2, .4, .4),
                                  MaskQuality(subject_detail=123, subject_detail_px=40))
    db.conn.execute("INSERT INTO detection_subjects VALUES (?, 'old-alias', '{}', 90, 1, '{}')", (did,))
    db.conn.execute("INSERT INTO subject_ranking_analysis VALUES (?, 'old-frame', '{}', ?)", (did, b"synthetic"))
    db.conn.execute("INSERT INTO subject_raw_analysis VALUES (?, 'old-frame', '{}', 0)", (did,))
    db.predictions.add(did, "Synthetic bird", .9, "synthetic-classifier", status="accepted")
    other, = db.detections.upsert_rows(pid, "full-image", [{"box": {"x": .1, "y": .1, "w": .1, "h": .1}, "confidence": .9}], "current")
    db.conn.execute("INSERT INTO subject_ranking_analysis VALUES (?, 'unrelated', '{}', ?)", (other, b"unrelated"))

    db.conn.execute("UPDATE photos SET rating=5, quality_score=90, subject_sharpness=123, subject_detail=123 WHERE id=?", (pid,))
    db.conn.execute(f"PRAGMA user_version={legacy_version}")
    db.commit()
    schema.ensure_schema(db.job_history.catalog_file())
    with sqlite3.connect(db.job_history.catalog_file()) as conn:
        assert conn.execute("SELECT COUNT(*) FROM detection_subjects").fetchone()[0] == 0
        assert conn.execute("SELECT rating, quality_score, subject_sharpness, subject_detail FROM photos WHERE id=?", (pid,)).fetchone() == (5, None, None, None)
        assert conn.execute("SELECT detection_id FROM photo_subject_choices WHERE photo_id=?", (pid,)).fetchone()[0] == did
        assert conn.execute("SELECT path FROM photo_masks WHERE photo_id=?", (pid,)).fetchone() is None
        assert conn.execute("SELECT detection_id, source_key FROM subject_ranking_analysis").fetchall() == [(other, "unrelated")]
        assert conn.execute("SELECT COUNT(*) FROM subject_raw_analysis").fetchone()[0] == 0
        assert conn.execute("SELECT species FROM predictions WHERE detection_id=?", (did,)).fetchone()[0] == "Synthetic bird"

    assert path.exists()


def test_history_retires_masks_from_a_different_verified_rendition(original_photo, tmp_path, monkeypatch):
    import image_loader
    from repositories.masks_features import MaskQuality

    app, db, pid, _source = original_photo
    actual_load = image_loader.load_image
    rendition = ["embedded-preview"]

    def load(path, max_size=1024, raw_decode=image_loader.RAW_DECODE_JPEG_FIRST):
        image = actual_load(path, max_size=None, raw_decode=raw_decode)
        if image is not None and raw_decode == image_loader.RAW_DECODE_ALIGNED:
            # A transient preview extraction failure can switch an unchanged
            # RAW between its eligible embedded preview and sensor rendering.
            image.info["vireo_aligned_rendition"] = rendition[0]
            image.info["vireo_subject_frame_id"] = image_loader.aligned_frame_identity(
                path, image, image_loader.aligned_source_state(path))
        if image is not None and max_size:
            image.thumbnail((max_size, max_size))
        return image

    monkeypatch.setattr(image_loader, "load_image", load)
    client = app.test_client()
    first = _save(client, pid, _token(client, pid)).get_json()["primary_detection_id"]
    before = db.conn.execute("SELECT runtime_fingerprint FROM detections WHERE id=?", (first,)).fetchone()[0]
    rendition[0] = "sensor"
    assert _save(client, pid, _token(client, pid)).status_code == 200
    mask = tmp_path / "sensor-mask.png"
    Image.new("L", (320, 200), 255).save(mask)
    db.masks_features.upsert_mask(pid, "sam2-small", str(mask), "reviewer-selected", (.2, .2, .4, .4),
                                  MaskQuality(subject_detail=123, subject_detail_px=100))
    db.masks_features.upsert_embedding(pid, "test", b"frame B", f"det:{first}")
    db.commit()
    rendition[0] = "embedded-preview"
    assert client.post("/api/undo").status_code == 200
    assert db.conn.execute("SELECT runtime_fingerprint FROM detections WHERE id=?", (first,)).fetchone()[0] == before
    # Same numeric box and photo are insufficient to reuse the sensor mask
    # during a quality-only retry after restoring the preview-frame proof.
    assert db.masks_features.get_mask(pid, "sam2-small") is None
    assert mask.exists()
    assert db.conn.execute("SELECT 1 FROM photo_embeddings WHERE photo_id=? AND variant=?",
                           (pid, f"det:{first}")).fetchone() is None
    import subject_analysis
    from services.analysis_retry import retry_features

    def analyze(*args, existing_mask_path=None, **kwargs):
        assert existing_mask_path is None
        raise ValueError("synthetic analysis boundary")

    monkeypatch.setattr(subject_analysis, "analyze_detection", analyze)
    with pytest.raises(ValueError, match="synthetic analysis boundary"):
        retry_features(db, pid, db.get_effective_config({}), str(tmp_path), lambda: None, quality_only=True)
    # Redo must retire any newly computed cache for the restored A frame too.
    db.masks_features.upsert_mask(pid, "sam2-small", str(mask), "reviewer-selected", (.2, .2, .4, .4),
                                  MaskQuality(subject_detail=456, subject_detail_px=100))
    db.commit()
    rendition[0] = "sensor"
    assert client.post("/api/redo").status_code == 200
    assert db.masks_features.get_mask(pid, "sam2-small") is None
    assert mask.exists()


@pytest.mark.parametrize("version", [51, 52, 53])
def test_detector_frame_upgrade_preserves_choices_but_requires_original_redetection(original_photo, version):
    import schema
    from reviewer_helpers import original_frame_detections
    from services.detection_store import write_detection_batch
    from subjects import analyze_photo

    _app, db, pid, source = original_photo
    rows = [{"box": dict(x=.2, y=.2, w=.4, h=.4), "confidence": .9}]
    did, = write_detection_batch(db, pid, "megadetector-v6", rows)
    db.subjects.set_choice(pid, did)
    db.subjects.set_primary_state(pid, did)
    db.predictions.add(did, "Synthetic bird", .9, "synthetic-classifier")
    db.conn.execute("INSERT INTO detection_subjects VALUES (?, 'legacy', '{}', 90, 1, '{}')", (did,))
    db.conn.execute("UPDATE photos SET rating=5, quality_score=90 WHERE id=?", (pid,))
    db.conn.execute("ALTER TABLE detections DROP COLUMN frame_id")
    db.conn.execute(f"PRAGMA user_version={version}")
    db.commit()
    schema._apply_pending(db.conn)
    assert db.detections.row(did)["frame_id"] is None
    assert db.subjects.choice(pid)["detection_id"] == did
    assert db.photos.get(pid)["rating"] == 5
    assert db.photos.get(pid)["quality_score"] is None
    assert db.subjects.analysis(did) is None
    with pytest.raises(ValueError, match="lack original-frame proof"):
        analyze_photo(db, pid, str(source))
    assert write_detection_batch(db, pid, "megadetector-v6", original_frame_detections(source, rows)) == [did]
    assert db.conn.execute("SELECT species FROM predictions WHERE detection_id=?", (did,)).fetchone()[0] == "Synthetic bird"
    assert analyze_photo(db, pid, str(source)) == 1
