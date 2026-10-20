"""Saved quality suggestions converge on live evidence without deciding for users."""

import json
from copy import deepcopy
from pathlib import Path

import config as cfg
import pytest
from app import create_app
from pipeline import load_results, load_results_raw, save_results_raw
from repositories.masks_features import MaskQuality, PipelineFeatures
from services import photo_ingest
from subject_readiness import REQUIRED_QUALITY_FIELDS, SCORE_FIELDS
from test_subject_readiness import complete_photo


@pytest.fixture(autouse=True)
def isolated_config(tmp_path, monkeypatch):
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    cfg.save({"pipeline": {"sam2_variant": "sam2-small", "weak_detection_rescue_enabled": False}})


def _photo(db, tmp_path, name="duck.jpg"):
    folder = db.add_folder(str(tmp_path / name), name=name)
    return photo_ingest.add_photo(db, folder, name, ".jpg", 100, 1.)


def _analyze(db, tmp_path, pid, **changes):
    mask = tmp_path / f"{pid}-mask.png"
    mask.write_bytes(b"scratch-mask")
    quality = {key: complete_photo()[key] for key in REQUIRED_QUALITY_FIELDS}
    quality.update(changes)
    db.masks_features.update_pipeline_features(pid, PipelineFeatures(
        mask_path=str(mask), **{k: v for k, v in quality.items() if k != "subject_size"},
    ))
    db.photos.update_quality(pid, subject_size=quality["subject_size"])
    db.detections.save(pid, [{"box": {"x": .1, "y": .1, "w": .4, "h": .4},
                              "confidence": .9, "category": "animal"}], detector_model="megadetector-v6")
    db.model_runs.record_detector_run(pid, "megadetector-v6", 1)
    db.masks_features.upsert_mask(pid, "sam2-small", str(mask), "megadetector-v6",
                                 (.1, .1, .4, .4), MaskQuality(**quality))
    db.masks_features.set_active_variant(pid, "sam2-small", .2)


def _old_review(ids):
    return {"photos": [{"id": pid, "label": "REJECT", "flag": "none", "rating": 0,
                        "reject_reasons": ["no_subject_mask"], "quality_composite": 0.,
                        "rarity_protected": True, "review_confirmed": True} for pid in ids],
            "encounters": [{"photo_ids": ids, "photo_count": len(ids), "burst_count": 1,
                            "confirmed_species": "Mandarin Duck", "species_confirmed": True,
                            "bursts": [{"photo_ids": ids, "species_override": {
                                "species": "Mandarin Duck", "confirmed": True}}]}],
            "summary": {"reject_count": len(ids)}}


def test_reopening_reloads_analysis_and_current_decisions_without_writes(db, tmp_path):
    ids = [_photo(db, tmp_path, f"duck{i}.jpg") for i in range(4)]
    old = _old_review(ids)
    save_results_raw(old, str(tmp_path), db.active_workspace_id)
    db.photo_review.set_flag(ids[1], "flagged")
    db.photo_review.set_flag(ids[2], "rejected")
    db.photo_review.set_rating(ids[1], 5)
    _analyze(db, tmp_path, ids[3])
    db.commit()
    before = list(db.conn.iterdump())
    result = load_results(str(tmp_path), db.active_workspace_id, db=db)
    rows = {p["id"]: p for p in result["photos"]}
    assert [rows[pid]["label"] for pid in ids[:3]] == ["REVIEW", "KEEP", "REJECT"]
    # Ranking may retain a complete frame for manual review when absolute
    # usability has not been validated; recovery must still load its measurements.
    assert rows[ids[3]]["label"] in {"KEEP", "REVIEW"}
    assert rows[ids[3]]["analysis_status"] == "complete"
    assert rows[ids[3]]["quality_composite"] is not None
    assert rows[ids[1]]["flag"] == "flagged" and rows[ids[1]]["rating"] == 5
    assert rows[ids[0]]["analysis_reasons"] == ["detection_not_run"]
    assert all(rows[ids[0]][key] is None for key in SCORE_FIELDS)
    assert rows[ids[0]]["reject_reasons"] == []
    assert not rows[ids[0]].get("rarity_protected")
    assert all(p["review_confirmed"] for p in result["photos"])
    assert [{key: group[key] for key in original} for group, original in
            zip(result["encounters"], old["encounters"], strict=True)] == old["encounters"]
    assert result["summary"]["keep_count"] == 1 + (rows[ids[3]]["label"] == "KEEP")
    assert result["summary"]["reject_count"] == 1
    assert result["summary"]["review_count"] == 1 + (rows[ids[3]]["label"] == "REVIEW")
    assert list(db.conn.iterdump()) == before
    persisted = (tmp_path / f"pipeline_results_ws{db.active_workspace_id}.json").read_bytes()
    assert load_results(str(tmp_path), db.active_workspace_id, db=db) == result
    assert (tmp_path / f"pipeline_results_ws{db.active_workspace_id}.json").read_bytes() == persisted
    assert list(db.conn.iterdump()) == before
    # A later unflag is authoritative too; the recovered KEEP is still only
    # a derived cache entry, not a newly persisted photographer decision.
    db.photo_review.set_flag(ids[1], "none")
    cleared = load_results(str(tmp_path), db.active_workspace_id, db=db)
    assert next(p for p in cleared["photos"] if p["id"] == ids[1])["label"] == "REVIEW"


def test_species_only_saved_review_does_not_acquire_quality_suggestions(db, tmp_path, monkeypatch):
    pid = _photo(db, tmp_path)
    saved = _old_review([pid])
    saved["review_mode"] = "species"
    saved["photos"][0]["label"] = "REVIEW"
    save_results_raw(saved, str(tmp_path), db.active_workspace_id)

    def quality_not_requested(*args, **kwargs):
        pytest.fail("Species review must not start quality triage")

    monkeypatch.setattr("pipeline.load_photo_features", quality_not_requested)
    result = load_results(str(tmp_path), db.active_workspace_id, db=db)
    assert {k: result["photos"][0][k] for k in saved["photos"][0]} == saved["photos"][0]
    assert result["review_mode"] == "species"


@pytest.mark.parametrize("state", ["not_run", "failed", "stale", "complete"])
def test_historic_reason_is_not_relabelled_without_current_evidence(db, tmp_path, monkeypatch, state):
    pid = _photo(db, tmp_path)
    save_results_raw(_old_review([pid]), str(tmp_path), db.active_workspace_id)
    current = complete_photo(pid, crop_complete=0.)
    if state != "complete":
        current["analysis_provenance"]["segmentation"] = {
            "status": "complete" if state == "stale" else state, "current": state != "stale",
        }
    monkeypatch.setattr("pipeline.load_photo_features", lambda *a, **kw: [deepcopy(current)])
    row = load_results(str(tmp_path), db.active_workspace_id, db=db)["photos"][0]
    if state == "complete":
        assert row["label"] == "REJECT"
        assert any(r.startswith("crop_incomplete") for r in row["reject_reasons"])
        assert row["analysis_status"] == "complete"
    else:
        assert row["label"] == "REVIEW"
        assert row["analysis_reasons"] == [f"segmentation_{state}"]


def test_reopening_updates_settings_source_and_comparison_cohort(db, tmp_path):
    ids = [_photo(db, tmp_path, f"duck{i}.jpg") for i in range(2)]
    for pid, sharpness in zip(ids, [100., 1000.], strict=True):
        _analyze(db, tmp_path, pid, subject_tenengrad=sharpness)
    save_results_raw(_old_review(ids), str(tmp_path), db.active_workspace_id)
    first = load_results(str(tmp_path), db.active_workspace_id, db=db)
    assert all(p["analysis_status"] == "complete" for p in first["photos"])
    # Removing a member from the saved grouping changes the relative focus cohort.
    edited = load_results_raw(str(tmp_path), db.active_workspace_id)
    edited["photos"] = edited["photos"][:1]
    save_results_raw(edited, str(tmp_path), db.active_workspace_id)
    single = load_results(str(tmp_path), db.active_workspace_id, db=db)
    assert single["photos"][0]["focus_score"] != first["photos"][0]["focus_score"]
    assert single["encounters"][0]["photo_count"] == 1
    db.workspaces.update(db.active_workspace_id, config_overrides={"pipeline": {"sam2_variant": "sam2-large"}})
    changed = load_results(str(tmp_path), db.active_workspace_id, db=db)
    assert changed["photos"][0]["label"] == "REVIEW"
    assert changed["photos"][0]["analysis_reasons"] == ["segmentation_stale"]
    # Reverting settings recovers the complete current input, then source changes invalidate it.
    db.workspaces.update(db.active_workspace_id, config_overrides=None)
    assert load_results(str(tmp_path), db.active_workspace_id, db=db)["photos"][0]["analysis_status"] == "complete"
    from computation_cache import source_input
    db.model_runs.record_detector_run(ids[0], "megadetector-v6", 1, "r", source_input(
        "a" * 64, "vireo-detector-source-v1")[1])
    db.conn.execute("UPDATE photos SET file_hash=? WHERE id=?", ("b" * 64, ids[0]))
    db.commit()
    assert load_results(str(tmp_path), db.active_workspace_id, db=db)["photos"][0]["analysis_reasons"] == ["detection_stale"]


def test_saved_recovery_stays_in_its_workspace(db, tmp_path):
    pid = _photo(db, tmp_path)
    ws1 = db.active_workspace_id
    ws2 = db.workspaces.create("Other")
    old = _old_review([pid])
    save_results_raw(old, str(tmp_path), ws1)
    save_results_raw(old, str(tmp_path), ws2)
    db.set_active_workspace(ws2)
    assert load_results(str(tmp_path), ws2, db=db)["photos"] == []
    assert load_results_raw(str(tmp_path), ws1)["photos"][0]["label"] == "REJECT"
    with pytest.raises(ValueError, match="workspace"):
        load_results(str(tmp_path), ws1, db=db)
    db.set_active_workspace(ws1)
    assert load_results(str(tmp_path), ws1, db=db)["photos"][0]["label"] == "REVIEW"


@pytest.mark.parametrize("endpoint", ["/api/pipeline/results", "/api/pipeline/page-init"])
def test_saved_view_entry_points_recover_exact_reported_membership(db, tmp_path, endpoint):
    fixture = json.loads((Path(__file__).parent / "data/saved_review_mandarin.json").read_text())
    original_rejects = {p["id"] for p in fixture["photos"] if p["label"] == "REJECT"}
    assert len(fixture["photos"]) == 71 and len(original_rejects) == 61
    folder = db.add_folder(str(tmp_path / "ducks"), name="Ducks")
    for photo in fixture["photos"]:
        db.conn.execute("INSERT INTO photos (id, folder_id, filename, extension, timestamp, flag, rating) "
                        "VALUES (?, ?, ?, '.NEF', ?, ?, 0)",
                        (photo["id"], folder, photo["filename"], photo["timestamp"], photo["flag"]))
    db.commit()
    save_results_raw(fixture, str(tmp_path), db.active_workspace_id)
    before = list(db.conn.iterdump())
    app = create_app(str(tmp_path / "test.db"), str(tmp_path / "thumbs"), api_token="recovery-test")
    app.config["TESTING"] = True
    with app.test_client() as client:
        response = client.get(endpoint, headers={"X-Vireo-Token": "recovery-test"})
        assert response.status_code == 200
        payload = response.get_json()
        result = payload["results"] if endpoint.endswith("page-init") else payload
        rows = {p["id"]: p for p in result["photos"]}
        assert set(rows) == set(fixture["encounters"][0]["photo_ids"])
        assert all(rows[pid]["label"] == "REVIEW" for pid in original_rejects)
        assert rows[216716]["label"] == "KEEP" and rows[216716]["flag"] == "flagged"
        assert result["summary"]["keep_count"] == 1
        assert result["summary"]["review_count"] == 70
        assert result["summary"]["reject_count"] == 0
        assert client.get(endpoint, headers={"X-Vireo-Token": "recovery-test"}).get_json() == payload
    # Application startup can maintain its own bookkeeping, so compare photo decisions directly.
    assert [line for line in db.conn.iterdump() if line.startswith('INSERT INTO "photos"')] == [
        line for line in before if line.startswith('INSERT INTO "photos"')]
