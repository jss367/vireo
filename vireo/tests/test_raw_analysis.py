"""Synthetic RAW values exercise metering, color encoding and pipeline inputs."""

import json
from types import SimpleNamespace
from unittest.mock import Mock

import numpy as np
import pytest
import raw_analysis as ra
from PIL import Image


def test_each_subject_gets_its_own_exposure():
    linear = np.full((100, 200, 3), 0.4, dtype=np.float32)
    linear[:, :100] = 0.01
    dark = np.zeros((100, 200), dtype=bool)
    dark[10:90, 10:90] = True
    bright = np.zeros_like(dark)
    bright[10:90, 110:190] = True
    assert ra.meter_subject(linear, dark)["exposure_ev"] == 2
    assert ra.meter_subject(linear, bright)["exposure_ev"] == 0


def test_sky_boundary_does_not_count_as_subject_clipping():
    linear = np.full((100, 100, 3), 0.02, dtype=np.float32)
    mask = np.zeros((100, 100), dtype=bool)
    mask[10:90, 10:90] = True
    core, eroded = ra.interior_mask(mask)
    assert eroded
    linear[mask & ~core] = 1
    stats = ra.meter_subject(linear, mask)
    assert stats["rendered_clip_high"] == 0
    assert stats["rendered_clip_high_full_mask"] > 0
    assert stats["exposure_ev"] == pytest.approx(2)
    image, report = ra.analyze_subject(linear, mask)
    image.close()
    assert report["original_quality"]["subject_clip_high"] == 0


def test_small_subject_keeps_pixels_and_reports_boundary_fallback():
    linear = np.full((8, 8, 3), 0.1, dtype=np.float32)
    mask = np.zeros((8, 8), dtype=bool)
    mask[3, 3] = True
    stats = ra.meter_subject(linear, mask)
    assert stats["sample_count"] == 1
    assert not stats["boundary_excluded"]


def test_empty_black_and_invalid_inputs():
    linear = np.zeros((8, 8, 3), dtype=np.float32)
    empty = np.zeros((8, 8), dtype=bool)
    assert ra.meter_subject(linear, empty)["status"] == "empty_mask"
    assert ra.meter_subject(linear, ~empty)["exposure_ev"] == 0
    with pytest.raises(ValueError, match="dimensions"):
        ra.meter_subject(linear, empty[:2])
    linear[0, 0, 0] = np.nan
    with pytest.raises(ValueError, match="finite"):
        ra.meter_subject(linear, ~empty)


def test_highlights_limit_shadow_boost_and_detect_single_channel_clipping():
    linear = np.full((100, 100, 3), 0.01, dtype=np.float32)
    linear[20:40, 20:40, 0] = 1
    stats = ra.meter_subject(linear, np.ones((100, 100), dtype=bool))
    assert stats["exposure_ev"] == 0
    assert stats["rendered_clip_high"] > 0


def test_render_encodes_srgb_after_linear_exposure():
    linear = np.full((4, 4, 3), 0.045, dtype=np.float32)
    image = ra.render_linear(linear, 2)
    # Linear 0.18 is approximately sRGB 0.461, not an 8-bit value of 46.
    assert image.getpixel((0, 0)) == (118, 118, 118)
    np.testing.assert_allclose(linear, 0.045)


def test_scoring_keeps_original_exposure_and_records_both_quality_sets():
    rng = np.random.default_rng(42)
    linear = rng.uniform(0.005, 0.02, (64, 64, 3)).astype(np.float32)
    image, report = ra.analyze_subject(linear, np.ones((64, 64), dtype=bool))
    image.close()
    features = ra.scoring_features(report)
    assert report["exposure_ev"] == 2
    assert report["corrected_quality"]["subject_y_median"] > report["original_quality"]["subject_y_median"]
    assert features["subject_y_median"] == report["original_quality"]["subject_y_median"]
    assert features["subject_tenengrad"] == report["corrected_quality"]["subject_tenengrad"]
    json.dumps(report, allow_nan=False)


def test_decoder_requests_linear_16_bit_and_keeps_sub_8_bit_detail(monkeypatch):
    import rawpy

    rgb = np.full((4, 4, 3), 1000, dtype=np.uint16)
    rgb[0, 0] = 1001
    raw = Mock()
    raw.sizes = SimpleNamespace(width=4, height=4)
    raw.postprocess.return_value = rgb
    context = Mock()
    context.__enter__ = Mock(return_value=raw)
    context.__exit__ = Mock(return_value=False)
    monkeypatch.setattr(rawpy, "imread", lambda path: context)
    monkeypatch.setattr("image_loader._is_bayer_sensor", lambda raw: True)
    linear = ra.decode_linear("bird.NEF", max_size=4)
    kwargs = raw.postprocess.call_args.kwargs
    assert kwargs["gamma"] == (1, 1)
    assert kwargs["output_bps"] == 16
    assert kwargs["no_auto_bright"] is True
    assert linear.dtype == np.float32
    assert linear[0, 0, 0] > linear[1, 1, 0]
    assert linear[0, 0, 0] == pytest.approx(1001 / 65535)
    proxy, native = ra.decode_linear("bird.NEF", max_size=2, return_native=True)
    assert raw.postprocess.call_args.kwargs["half_size"] is False
    assert proxy.shape == (2, 2, 3)
    assert native is rgb


def test_decoder_falls_back_for_jpeg_and_unsupported_raw(monkeypatch):
    import rawpy

    reader = Mock(side_effect=rawpy.LibRawFileUnsupportedError("unsupported"))
    monkeypatch.setattr(rawpy, "imread", reader)
    assert ra.decode_linear("bird.jpg") is None
    reader.assert_not_called()
    assert ra.decode_linear("bird.nef") is None


def test_session_decodes_once_for_multiple_subjects_and_reloads_changed_file(tmp_path, monkeypatch):
    path = tmp_path / "bird.nef"
    path.write_bytes(b"raw")
    decoder = Mock(return_value=np.ones((10, 10, 3), dtype=np.float32))
    monkeypatch.setattr(ra, "decode_linear", decoder)
    session = ra.RawAnalysisSession()
    first = session.load(str(path))
    assert session.load(str(path)) is first
    assert decoder.call_count == 1
    path.write_bytes(b"changed raw")
    session.load(str(path))
    assert decoder.call_count == 2


def test_classifier_receives_corrected_crop_and_report(tmp_path, monkeypatch):
    from classify_job import _prepare_image

    path = tmp_path / "birds.nef"
    path.write_bytes(b"raw")
    linear = np.full((100, 200, 3), 0.3, dtype=np.float32)
    linear[:, :100] = 0.01
    monkeypatch.setattr(ra, "decode_linear", lambda *a: linear)
    monkeypatch.setattr("image_loader.load_image", lambda *a, **k: Image.new("RGB", (200, 100)))
    monkeypatch.setattr("masking.ensure_sam2_weights", lambda *a, **k: None)

    def mask_for_box(image, box, **kwargs):
        mask = np.zeros((100, 200), dtype=bool)
        left = round(box["x"] * 200)
        mask[20:80, left:left + 60] = True
        return mask

    monkeypatch.setattr("masking.generate_mask", mask_for_box)
    session = ra.RawAnalysisSession()
    photo = {"id": 1, "folder_id": 2, "filename": path.name}
    evs = []
    for x in (0.1, 0.6):
        detection = {"box_x": x, "box_y": 0.2, "box_w": 0.3, "box_h": 0.6}
        image, _, _ = _prepare_image(photo, {2: str(tmp_path)}, detection, raw_analysis=session)
        evs.append(image.info["_vireo_raw_analysis"]["exposure_ev"])
        assert image.size == (84, 84)
        assert image.info["_vireo_input_source"] == "original"
        image.close()
    assert evs == [2, 0]


def test_classifier_normal_loader_fallback(monkeypatch):
    from classify_job import _prepare_image

    session = Mock()
    session.prepare.return_value = (None, None)
    monkeypatch.setattr("classify_job.load_image", lambda *a, **k: Image.new("RGB", (100, 100), "red"))
    image, _, _ = _prepare_image(
        {"folder_id": 1, "filename": "unsupported.nef"}, {1: "/photos"}, None,
        raw_analysis=session,
    )
    assert image.getpixel((0, 0)) == (255, 0, 0)
    assert "_vireo_raw_analysis" not in image.info


def test_small_subject_crop_uses_native_detail_before_resize(tmp_path, monkeypatch):
    from classify_job import _prepare_image

    path = tmp_path / "bird.nef"
    path.write_bytes(b"raw")
    proxy = np.full((40, 40, 3), 0.01, dtype=np.float32)
    native = np.full((400, 400, 3), 1000, dtype=np.uint16)
    native[:, ::2] = 3000
    decoder = Mock(return_value=(proxy, native))
    monkeypatch.setattr(ra, "decode_linear", decoder)
    monkeypatch.setattr("image_loader.load_image", lambda *a, **k: Image.new("RGB", (40, 40)))
    monkeypatch.setattr("masking.ensure_sam2_weights", lambda *a, **k: None)
    monkeypatch.setattr("masking.generate_mask", lambda *a, **k: np.ones((40, 40), dtype=bool))
    session = ra.RawAnalysisSession(max_size=40, preserve_detail=True)
    image, _, _ = _prepare_image(
        {"folder_id": 1, "filename": path.name}, {1: str(tmp_path)},
        {"box_x": 0.25, "box_y": 0.25, "box_w": 0.25, "box_h": 0.25},
        raw_analysis=session,
    )
    # The proxy subject is just 10px wide. Its native padded crop is 140px,
    # retains alternating columns, and must not be cropped a second time.
    assert image.size == (140, 140)
    pixels = np.asarray(image)
    assert abs(int(pixels[0, 0, 0]) - int(pixels[0, 1, 0])) > 20
    decoder.assert_called_once_with(str(path), 40, return_native=True)
    image.close()


def test_corrected_runtime_does_not_match_normal_runtime():
    from computation_cache import classifier_runtime_fingerprint

    model = {"name": "test"}
    regular = classifier_runtime_fingerprint(model, "a" * 64, "b" * 64)
    corrected = classifier_runtime_fingerprint(
        {**model, "raw_subject_analysis": ra.RECIPE}, "a" * 64, "b" * 64,
    )
    assert regular is not None and corrected is not None
    assert regular != corrected


@pytest.mark.parametrize("preview_size", [(100, 100), (200, 100)])
def test_session_falls_back_for_empty_mask_or_mismatched_geometry(tmp_path, monkeypatch, preview_size):
    path = tmp_path / "bird.nef"
    path.write_bytes(b"raw")
    monkeypatch.setattr(ra, "decode_linear", lambda *a: np.ones((100, 100, 3), dtype=np.float32))
    monkeypatch.setattr("image_loader.load_image", lambda *a, **k: Image.new("RGB", preview_size))
    monkeypatch.setattr("masking.ensure_sam2_weights", lambda *a, **k: None)
    generate = Mock(return_value=np.zeros((100, 100), dtype=bool))
    monkeypatch.setattr("masking.generate_mask", generate)
    detection = {"box_x": 0.1, "box_y": 0.1, "box_w": 0.8, "box_h": 0.8}
    assert ra.RawAnalysisSession().prepare(str(path), detection) == (None, None)
    assert generate.call_count == (1 if preview_size == (100, 100) else 0)


def test_session_falls_back_when_sam2_weights_unavailable(tmp_path, monkeypatch):
    """A failure downloading SAM2 weights must not abort classification."""
    path = tmp_path / "bird.nef"
    path.write_bytes(b"raw")
    monkeypatch.setattr(ra, "decode_linear", lambda *a: np.ones((100, 100, 3), dtype=np.float32))
    monkeypatch.setattr("image_loader.load_image", lambda *a, **k: Image.new("RGB", (100, 100)))

    def raise_weights_error(*args, **kwargs):
        raise RuntimeError("Failed to download SAM2 weights")

    monkeypatch.setattr("masking.ensure_sam2_weights", raise_weights_error)
    generate = Mock(return_value=np.ones((100, 100), dtype=bool))
    monkeypatch.setattr("masking.generate_mask", generate)
    detection = {"box_x": 0.1, "box_y": 0.1, "box_w": 0.8, "box_h": 0.8}
    session = ra.RawAnalysisSession()
    assert session.prepare(str(path), detection) == (None, None)
    assert generate.call_count == 0


def test_session_falls_back_when_generate_mask_raises(tmp_path, monkeypatch):
    """A raised error from generate_mask must be caught (only None means empty)."""
    path = tmp_path / "bird.nef"
    path.write_bytes(b"raw")
    monkeypatch.setattr(ra, "decode_linear", lambda *a: np.ones((100, 100, 3), dtype=np.float32))
    monkeypatch.setattr("image_loader.load_image", lambda *a, **k: Image.new("RGB", (100, 100)))
    monkeypatch.setattr("masking.ensure_sam2_weights", lambda *a, **k: None)

    def raise_mask_error(*args, **kwargs):
        raise FileNotFoundError("SAM2 image encoder not found")

    monkeypatch.setattr("masking.generate_mask", raise_mask_error)
    detection = {"box_x": 0.1, "box_y": 0.1, "box_w": 0.8, "box_h": 0.8}
    assert ra.RawAnalysisSession().prepare(str(path), detection) == (None, None)


def test_session_preserves_cooperative_cancellation(tmp_path, monkeypatch):
    """ResourceWaitCancelled must propagate so pipeline shutdown is prompt."""
    from resource_ledger import ResourceWaitCancelled

    path = tmp_path / "bird.nef"
    path.write_bytes(b"raw")
    monkeypatch.setattr(ra, "decode_linear", lambda *a: np.ones((100, 100, 3), dtype=np.float32))
    monkeypatch.setattr("image_loader.load_image", lambda *a, **k: Image.new("RGB", (100, 100)))
    monkeypatch.setattr("masking.ensure_sam2_weights", lambda *a, **k: None)

    def cancel(*args, **kwargs):
        raise ResourceWaitCancelled("shutdown")

    monkeypatch.setattr("masking.generate_mask", cancel)
    detection = {"box_x": 0.1, "box_y": 0.1, "box_w": 0.8, "box_h": 0.8}
    with pytest.raises(ResourceWaitCancelled):
        ra.RawAnalysisSession().prepare(str(path), detection)


def test_existing_catalog_gains_analysis_schema(tmp_path):
    from db import Database

    path = str(tmp_path / "old.db")
    db = Database(path)
    db.conn.execute("ALTER TABLE photos DROP COLUMN quality_input_recipe")
    db.conn.execute("DROP TABLE subject_raw_analysis")
    db.conn.commit()
    db.close()
    migrated = Database(path)
    migrated.conn.execute("SELECT quality_input_recipe FROM photos")
    migrated.conn.execute("SELECT report_json FROM subject_raw_analysis")
    migrated.close()


def test_reports_persist_per_detection_and_cascade(tmp_path):
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    folder = db.add_folder(str(tmp_path))
    photo = db.add_photo(folder, "bird.nef", ".nef", 100, 1)
    db.save_detections(photo, [
        {"box": {"x": x, "y": 0.1, "w": 0.2, "h": 0.3}, "confidence": 0.9, "category": "animal"}
        for x in (0.1, 0.6)
    ], detector_model="megadetector-v6")
    for index, det in enumerate(db.get_detections(photo)):
        db.save_subject_raw_analysis(det["id"], {"recipe": ra.RECIPE, "exposure_ev": index})
    rows = db.conn.execute("SELECT report_json FROM subject_raw_analysis").fetchall()
    assert sorted(json.loads(row[0])["exposure_ev"] for row in rows) == [0, 1]
    db.conn.execute("DELETE FROM detections WHERE photo_id=?", (photo,))
    assert db.conn.execute("SELECT count(*) FROM subject_raw_analysis").fetchone()[0] == 0
    db.close()


@pytest.mark.parametrize("old_quality_columns", [False, True])
def test_mask_recipe_migration_preserves_active_and_invalidates_unknown_history(tmp_path, old_quality_columns):
    from db import Database

    path = str(tmp_path / "old-masks.db")
    db = Database(path)
    folder = db.add_folder(str(tmp_path))
    photo = db.add_photo(folder, "bird.nef", ".nef", 100, 1)
    detections = db.save_detections(photo, [{
        "box": {"x": 0.1, "y": 0.1, "w": 0.8, "h": 0.8},
        "confidence": 0.9, "category": "animal",
    }], detector_model="megadetector-v6")
    db.save_subject_raw_analysis(detections[0], {"recipe": ra.RECIPE})
    db.record_classifier_run(detections[0], "model", "labels", 1)
    db.conn.execute("ALTER TABLE classifier_runs DROP COLUMN input_recipe")
    for variant in ("sam2-small", "sam2-large"):
        db.upsert_photo_mask(photo, variant, "/mask.png", "megadetector-v6", 0.1, 0.1, 0.8, 0.8)
    db.set_active_mask_variant(photo, "sam2-large")
    db.update_photo_pipeline_features(photo, quality_input_recipe=ra.RECIPE)
    db.conn.execute("ALTER TABLE photo_masks DROP COLUMN quality_input_recipe")
    if old_quality_columns:
        for field in ("subject_clip_high", "subject_clip_low", "subject_y_median", "bg_separation", "phash_crop", "noise_estimate"):
            db.conn.execute(f"ALTER TABLE photo_masks DROP COLUMN {field}")
        db.update_photo_pipeline_features(photo, bg_separation=42, noise_estimate=5, phash_crop="1234")
    db.conn.commit()
    db.close()

    migrated = Database(path)
    assert migrated.get_classifier_run_keys(detections[0]) == set()
    assert migrated.conn.execute("SELECT input_recipe FROM classifier_runs").fetchone()[0] == "unknown-raw-recipe"
    assert migrated.get_photo_mask(photo, "sam2-large")["quality_input_recipe"] == ra.RECIPE
    expected = "unknown-mask-quality-recipe" if old_quality_columns else "unknown-raw-analysis-recipe"
    assert migrated.get_photo_mask(photo, "sam2-small")["quality_input_recipe"] == expected
    if old_quality_columns:
        active = migrated.get_photo_mask(photo, "sam2-large")
        assert (active["bg_separation"], active["noise_estimate"], active["phash_crop"]) == (42, 5, "1234")
    migrated.close()


@pytest.mark.parametrize("kind", ["pending", "match"])
def test_reinference_replaces_candidates_preserving_surviving_reviews(tmp_path, kind):
    from classify_job import _store_match_prediction, _store_pending_detection_prediction
    from db import Database

    db = Database(str(tmp_path / "refresh.db"))
    workspace = db._active_workspace_id
    folder = db.add_folder(str(tmp_path))
    photo = db.add_photo(folder, "bird.nef", ".nef", 100, 1)
    detection = db.save_detections(photo, [{
        "box": {"x": 0.1, "y": 0.1, "w": 0.8, "h": 0.8},
        "confidence": 0.9, "category": "animal",
    }], detector_model="megadetector-v6")[0]
    db.add_prediction(detection, "Robin", 0.2, "model", status="accepted",
                      taxonomy={"genus": "old"}, labels_fingerprint="labels")
    original_id = db.conn.execute("SELECT id FROM predictions").fetchone()[0]
    other_workspace = db.create_workspace("Other")
    db.add_workspace_folder(other_workspace, folder)
    db.set_active_workspace(other_workspace)
    db.add_prediction(detection, "Robin", 0.2, "model", status="rejected", labels_fingerprint="labels")
    db.set_active_workspace(workspace)
    db.add_prediction(detection, "Stale species", 0.99, "model", labels_fingerprint="labels")
    db.add_prediction(detection, "Other labels", 0.7, "model", labels_fingerprint="other-labels")
    db.add_prediction(detection, "Other model", 0.7, "other-model", labels_fingerprint="labels")
    db.record_classifier_run(detection, "model", "labels", prediction_count=2)
    item = {
        "detection_id": detection, "prediction": "Robin", "confidence": 0.9,
        "taxonomy": {"genus": "Turdus", "scientific_name": "Turdus migratorius"},
        "alternatives": [{"species": "Sparrow", "confidence": 0.1}],
        "_replace_prediction_outputs": True,
    }
    if kind == "match":
        _store_match_prediction(db, item, "model", "labels")
    else:
        _store_pending_detection_prediction(db, item, "model", "labels", "new", group_id="new-group")
    rows = db.conn.execute(
        "SELECT * FROM predictions WHERE detection_id=? AND classifier_model='model' "
        "AND labels_fingerprint='labels' ORDER BY confidence DESC", (detection,),
    ).fetchall()
    assert [r["species"] for r in rows] == ["Robin", "Sparrow"]
    assert rows[0]["id"] == original_id
    assert rows[0]["confidence"] == 0.9
    assert rows[0]["taxonomy_genus"] == "Turdus"
    reviews = db.conn.execute(
        "SELECT workspace_id, status FROM prediction_review WHERE prediction_id=?", (original_id,),
    ).fetchall()
    assert {r["workspace_id"]: r["status"] for r in reviews} == {
        workspace: "accepted", other_workspace: "rejected",
    }
    assert db.conn.execute("SELECT count(*) FROM predictions").fetchone()[0] == 4
    assert db.conn.execute("SELECT count(*) FROM classifier_runs").fetchone()[0] == 1
    # A previous alternative promoted to top-1 returns to pending.
    item.update(prediction="Sparrow", confidence=0.95, alternatives=[])
    _store_pending_detection_prediction(db, item, "model", "labels", "new")
    assert db.conn.execute(
        "SELECT status FROM prediction_review r JOIN predictions p ON p.id=r.prediction_id "
        "WHERE p.species='Sparrow' AND r.workspace_id=?", (workspace,),
    ).fetchone()[0] == "pending"
    # A later normal-image reinference can replace the RAW candidates too.
    item.update(prediction="Warbler", confidence=0.8, alternatives=[], taxonomy=None)
    _store_pending_detection_prediction(db, item, "model", "labels", "new")
    rows = db.conn.execute(
        "SELECT species FROM predictions WHERE classifier_model='model' AND labels_fingerprint='labels'"
    ).fetchall()
    assert [r[0] for r in rows] == ["Warbler"]
    db.close()


@pytest.mark.parametrize("status", ["pending", "accepted", "rejected"])
@pytest.mark.parametrize("new_group", [None, "new-burst"])
def test_reinference_refreshes_burst_metadata(tmp_path, status, new_group):
    from classify_job import _store_pending_detection_prediction
    from db import Database

    db = Database(str(tmp_path / "burst.db"))
    workspace = db._active_workspace_id
    folder = db.add_folder(str(tmp_path))
    photo = db.add_photo(folder, "bird.nef", ".nef", 100, 1)
    detection = db.save_detections(photo, [{
        "box": {"x": 0.1, "y": 0.1, "w": 0.8, "h": 0.8},
        "confidence": 0.9, "category": "animal",
    }], detector_model="megadetector-v6")[0]
    db.add_prediction(detection, "Robin", 0.2, "model", status=status,
                      group_id="old-burst", vote_count=3, total_votes=4, individual='{"Robin":3}')
    item = {"detection_id": detection, "prediction": "Robin", "confidence": 0.9,
            "_replace_prediction_outputs": True}
    _store_pending_detection_prediction(
        db, item, "model", "legacy", "new", group_id=new_group,
        vote_count=2 if new_group else None, total_votes=2 if new_group else None,
        individual='{"Robin":2}' if new_group else None,
    )
    row = db.conn.execute("SELECT * FROM prediction_review WHERE workspace_id=?", (workspace,)).fetchone()
    assert row["status"] == status
    assert row["group_id"] == new_group
    assert row["vote_count"] == (2 if new_group else None)
    assert row["total_votes"] == (2 if new_group else None)
    assert row["individual"] == ('{"Robin":2}' if new_group else None)
    db.close()


@pytest.mark.parametrize("status", ["accepted", "rejected"])
def test_normal_cache_gate_rejects_reviewed_raw_recipe(tmp_path, status):
    from db import Database

    db = Database(str(tmp_path / "recipe.db"))
    folder = db.add_folder(str(tmp_path))
    photo = db.add_photo(folder, "bird.nef", ".nef", 100, 1)
    detection = db.save_detections(photo, [{
        "box": {"x": 0.1, "y": 0.1, "w": 0.8, "h": 0.8},
        "confidence": 0.9, "category": "animal",
    }], detector_model="megadetector-v6")[0]
    db.add_prediction(detection, "Robin", 0.9, "model", status=status)
    db.record_classifier_run(detection, "model", "legacy", 1,
                             runtime_fingerprint="raw-runtime", input_recipe=ra.RECIPE)
    from classify_job import _all_photos_cache_satisfied

    assert not _all_photos_cache_satisfied(db, [photo], classifier_model="model", labels_fingerprint="legacy")
    assert db.get_classifier_run_keys(detection) == set()
    assert db.get_classifier_run_keys(detection, "normal-runtime") == set()
    assert db.get_classifier_run_key_gate(detection, "normal-runtime") == (set(), {("model", "legacy")})
    assert db.get_classifier_run_cache_hits([photo], "model", "legacy") == set()
    # Normal runtime changes keep the existing manual-review exception.
    db.record_classifier_run(detection, "model", "legacy", 1, runtime_fingerprint="normal-runtime")
    assert db.get_classifier_run_key_gate(detection, "new-normal-runtime") == ({("model", "legacy")}, set())
    assert db.get_classifier_run_cache_hits([photo], "model", "legacy") == {photo}
    db.close()
