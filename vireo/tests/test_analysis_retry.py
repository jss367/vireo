"""Selected analysis recovery uses scratch inputs, real jobs and isolated model fakes."""

import threading
from pathlib import Path

import numpy as np
import pytest
from PIL import Image
from services import photo_ingest
from services.analysis_retry import explain_selected, run_retry
from testing.waits import synchronization_timeout
from web.background_jobs import JobCancelled


@pytest.fixture
def retry_library(app_and_db, tmp_path, monkeypatch):
    app, db = app_and_db
    folder = db.add_folder(str(tmp_path), name="Selected analysis")
    ids = []
    for i in range(3):
        path = tmp_path / f"duck-{i}.jpg"
        Image.new("RGB", (100, 80), (100, 160, 80)).save(path)
        ids.append(photo_ingest.add_photo(db, folder, path.name, ".jpg", path.stat().st_size, path.stat().st_mtime))
    calls = {"detection": [], "segmentation": []}

    def detect(path):
        calls["detection"].append(Path(path).name)
        return [{"box": {"x": .1, "y": .1, "w": .7, "h": .7}, "confidence": .9, "category": "animal"}]

    def mask(image, box, variant):
        calls["segmentation"].append(variant)
        pixels = np.zeros((image.height, image.width), bool)
        pixels[8:64, 10:80] = True
        return pixels

    monkeypatch.setattr("detector.detect_animals", detect)
    monkeypatch.setattr("computation_cache.megadetector_runtime_fingerprint", lambda: "legacy")
    monkeypatch.setattr("masking.generate_mask", mask)
    monkeypatch.setattr("subject_analysis.measure_eye", lambda *a: {})
    return app, db, ids, calls


class Context:
    def __init__(self, db, cancel=None):
        self.db = db
        self.cancel = cancel

    def thread_db(self):
        from db import Database
        worker = Database(self.db.job_history.catalog_file())
        worker.set_active_workspace(self.db.require_workspace_id())
        return worker

    def checkpoint(self, job):
        if self.cancel and self.cancel():
            raise JobCancelled("Job cancelled")


def execute(db, ids, directory, *, cancel=None):
    effective = db.get_effective_config({"pipeline": {"sam2_variant": "sam2-small", "eye_detect_enabled": False}})
    job = {"id": "scratch-retry", "progress": {}, "errors": []}
    result = run_retry(Context(db, cancel), job, ids, effective, str(directory))
    return result, job


def test_success_scope_and_saved_review_refresh(retry_library, tmp_path):
    from pipeline import load_results_raw, save_results_raw

    _, db, ids, calls = retry_library
    cache = {"photos": [{"id": pid, "flag": "none", "rating": 0, "label": "REJECT"} for pid in ids],
             "encounters": [{"photo_ids": ids, "bursts": [{"photo_ids": ids}], "species_confirmed": True}], "summary": {}}
    save_results_raw(cache, str(tmp_path), db.require_workspace_id())
    result, job = execute(db, ids[:2], tmp_path)
    assert result["ok"] and job["progress"]["current"] == 2
    assert calls["detection"] == ["duck-0.jpg", "duck-1.jpg"]
    assert len(calls["segmentation"]) == 2
    assert all(photo["status"] == "complete" for photo in result["photos"])
    assert db.conn.execute("SELECT mask_path FROM photos WHERE id=?", (ids[2],)).fetchone()[0] is None
    saved = load_results_raw(str(tmp_path), db.require_workspace_id())
    assert saved["encounters"][0]["photo_ids"] == ids and saved["encounters"][0]["species_confirmed"]
    assert saved["summary"]["reject_count"] == 0
    assert next(p for p in saved["photos"] if p["id"] == ids[2])["label"] == "REVIEW"


def test_retry_recomputes_incomplete_cached_quality_without_detection_or_segmentation(retry_library, tmp_path, monkeypatch):
    _, db, ids, calls = retry_library
    execute(db, [ids[0]], tmp_path)
    db.conn.execute("UPDATE photos SET subject_clip_low=NULL WHERE id=?", (ids[0],))
    db.commit()
    report = explain_selected(db, [ids[0]], db.get_effective_config({}))
    assert report["photos"][0]["required_stages"] == ["quality"]
    calls["detection"].clear()
    calls["segmentation"].clear()
    result, _ = execute(db, [ids[0]], tmp_path)
    assert result["photos"][0]["status"] == "complete"
    assert result["photos"][0]["stages_run"] == ["quality"]
    assert calls == {"detection": [], "segmentation": []}
    assert db.conn.execute("SELECT subject_clip_low FROM photos WHERE id=?", (ids[0],)).fetchone()[0] is not None


def test_persistent_failure_is_review_and_leaves_prior_mask_generation(retry_library, tmp_path, monkeypatch):
    from pipeline import load_photo_features, run_triage

    _, db, ids, _ = retry_library
    execute(db, [ids[0]], tmp_path)
    old_path = db.conn.execute("SELECT mask_path FROM photos WHERE id=?", (ids[0],)).fetchone()[0]
    before = Path(old_path).read_bytes()
    db.conn.execute("UPDATE photo_masks SET prompt_x=.5 WHERE photo_id=?", (ids[0],))
    db.commit()
    monkeypatch.setattr("masking.generate_mask", lambda *a, **k: None)
    result, _ = execute(db, [ids[0]], tmp_path)
    assert result["ok"] is False and "No subject mask" in result["errors"][0]["error"]
    assert db.conn.execute("SELECT mask_path FROM photos WHERE id=?", (ids[0],)).fetchone()[0] == old_path and Path(old_path).read_bytes() == before
    photos = load_photo_features(db, photo_ids=[ids[0]])
    run_triage([{"photos": photos}])
    assert photos[0]["label"] == "REVIEW"


def test_completed_negative_reruns_detection_and_explains_review(retry_library, tmp_path, monkeypatch):
    _, db, ids, calls = retry_library
    db.model_runs.record_detector_run(ids[0], "megadetector-v6", 0)
    report = explain_selected(db, [ids[0]], db.get_effective_config({}))
    assert "Detection completed" in report["photos"][0]["explanation"]
    monkeypatch.setattr("detector.detect_animals", lambda path: calls["detection"].append(path) or [])
    result, _ = execute(db, [ids[0]], tmp_path)
    assert result["ok"] and len(calls["detection"]) == 1 and not calls["segmentation"]
    assert result["photos"][0]["status"] == "review"
    assert result["photos"][0]["stages_run"] == ["detection"]
    assert result["photos"][0]["analysis"]["reasons"] == ["no_qualifying_subject"]


def test_concurrent_flag_and_rating_survive_inference_and_refresh(retry_library, tmp_path, monkeypatch):
    from db import Database
    _, db, ids, _ = retry_library
    entered, release = threading.Event(), threading.Event()
    original = __import__("masking").generate_mask

    def gated(*args, **kwargs):
        entered.set()
        assert release.wait(synchronization_timeout(5))
        return original(*args, **kwargs)

    monkeypatch.setattr("masking.generate_mask", gated)
    path, ws = db.job_history.catalog_file(), db.require_workspace_id()
    result = []

    def worker():
        own = Database(path)
        own.set_active_workspace(ws)
        try:
            result.append(execute(own, [ids[0]], tmp_path)[0])
        finally:
            own.close()

    thread = threading.Thread(target=worker)
    thread.start()
    try:
        assert entered.wait(synchronization_timeout(5))
        db.photo_review.set_flag(ids[0], "flagged")
        db.photo_review.set_rating(ids[0], 5)
    finally:
        release.set()
        thread.join(synchronization_timeout(5))
    assert not thread.is_alive() and result[0]["ok"]
    assert (db.photos.get(ids[0])["flag"], db.photos.get(ids[0])["rating"]) == ("flagged", 5)


@pytest.mark.parametrize("endpoint", ["analysis-readiness", "analysis-retry"])
def test_api_rejects_unbounded_malformed_and_foreign_selection(retry_library, endpoint):
    app, db, ids, _ = retry_library
    client = app.test_client()
    for values in ([], [True], ["1"], ids * 70, [10**12], [2**64]):
        assert client.post(f"/api/photos/{endpoint}", json={"photo_ids": values}).status_code == 400
    other = db.create_workspace("Foreign")
    assert client.post(f"/api/workspaces/{other}/activate").status_code == 200
    assert client.post(f"/api/photos/{endpoint}", json={"photo_ids": ids}).status_code == 400


def test_cancel_before_publication_leaves_no_new_active_generation(retry_library, tmp_path, monkeypatch):
    _, db, ids, _ = retry_library
    cancelled = []
    original = __import__("masking").generate_mask

    def cancel_mask(*args, **kwargs):
        result = original(*args, **kwargs)
        cancelled.append(True)
        return result

    monkeypatch.setattr("masking.generate_mask", cancel_mask)
    with pytest.raises(JobCancelled):
        execute(db, [ids[0]], tmp_path, cancel=lambda: bool(cancelled))
    assert db.conn.execute("SELECT mask_path FROM photos WHERE id=?", (ids[0],)).fetchone()[0] is None
    assert not list((tmp_path / "masks").glob("*.png"))


def test_api_job_failure_persists_explanation_and_workspace_guard(retry_library, monkeypatch):
    from wait import wait_for_job_via_client
    app, db, ids, _ = retry_library
    client = app.test_client()
    workspace = db.require_workspace_id()
    headers = {"X-Vireo-Token": "test-token-123"}
    assert client.post("/api/v1/photos/analysis-retry", headers=headers,
                       json={"photo_ids": [ids[0]], "expected_workspace_id": workspace + 1}).status_code == 409
    preview = client.post("/api/v1/photos/analysis-readiness", headers=headers, json={"photo_ids": [ids[0]]})
    assert preview.status_code == 200 and preview.json["workspace_id"] == workspace
    monkeypatch.setattr("masking.generate_mask", lambda *a, **k: None)
    response = client.post("/api/v1/photos/analysis-retry", headers=headers,
                           json={"photo_ids": [ids[0]], "expected_workspace_id": workspace})
    assert response.status_code == 200 and response.json["workspace_id"] == workspace
    job = wait_for_job_via_client(client, response.json["job_id"], wait_for_history=True)
    assert job["status"] == "failed" and job["result"]["errors"][0]["photo_id"] == ids[0]
    after = client.post("/api/photos/analysis-readiness", json={"photo_ids": [ids[0]]}).json["photos"][0]
    assert after["last_attempt"]["status"] == "failed" and "No subject mask" in after["explanation"]


def test_source_changed_during_inference_never_publishes_active_mask(retry_library, tmp_path, monkeypatch):
    _, db, ids, _ = retry_library
    original = __import__("masking").generate_mask
    def mutate_source(*args, **kwargs):
        result = original(*args, **kwargs)
        with (tmp_path / "duck-0.jpg").open("ab") as file:
            file.write(b"changed")
        return result
    monkeypatch.setattr("masking.generate_mask", mutate_source)
    result, _ = execute(db, [ids[0]], tmp_path)
    assert not result["ok"] and "changed" in result["errors"][0]["error"]
    assert db.conn.execute("SELECT mask_path FROM photos WHERE id=?", (ids[0],)).fetchone()[0] is None


def test_weak_context_anchors_are_read_without_becoming_retry_targets(retry_library, tmp_path):
    from services import detection_store
    from services.analysis_retry import _current
    _, db, ids, calls = retry_library
    for index, pid in enumerate(ids):
        db.conn.execute("UPDATE photos SET timestamp=? WHERE id=?", (f"2026-10-21T10:00:0{index}", pid))
        did = detection_store.write_detection_batch(db, pid, "megadetector-v6", [
            {"box": {"x": .1, "y": .1, "w": .7, "h": .7}, "confidence": .15 if index == 1 else .9, "category": "animal"}])[0]
        if index != 1:
            db.predictions.add(did, "Mandarin Duck", .95, "inat21", category="match")
    db.commit()
    effective = db.get_effective_config({})
    selected = _current(db, [ids[1]], effective)
    assert len(selected) == 1 and selected[0]["subject_uncertain"]
    result, _ = execute(db, [ids[1]], tmp_path)
    assert result["ok"] and result["photos"][0]["status"] == "complete"
    assert calls["detection"] == [] and len(calls["segmentation"]) == 1
    assert result["requested_photo_ids"] == [ids[1]]


def test_visibility_removed_during_inference_never_publishes(retry_library, tmp_path, monkeypatch):
    _, db, ids, _ = retry_library
    original = __import__("masking").generate_mask
    folder_id = db.photos.get(ids[0])["folder_id"]
    def hide_during_analysis(*args, **kwargs):
        pixels = original(*args, **kwargs)
        db.remove_workspace_folder(db.require_workspace_id(), folder_id)
        return pixels
    monkeypatch.setattr("masking.generate_mask", hide_during_analysis)
    result, _ = execute(db, [ids[0]], tmp_path)
    assert not result["ok"] and "visible" in result["errors"][0]["error"]
    assert db.conn.execute("SELECT mask_path FROM photos WHERE id=?", (ids[0],)).fetchone()[0] is None
    assert result["readiness"]["photos"] == []


def test_job_captures_submission_workspace_even_after_switch(retry_library, monkeypatch):
    from wait import wait_for_job_via_client
    app, db, ids, calls = retry_library
    client = app.test_client()
    original = __import__("detector").detect_animals
    entered, release = threading.Event(), threading.Event()
    def gated(path):
        entered.set()
        assert release.wait(synchronization_timeout(5))
        return original(path)
    monkeypatch.setattr("detector.detect_animals", gated)
    workspace = db.require_workspace_id()
    response = client.post("/api/photos/analysis-retry", json={"photo_ids": [ids[0]], "expected_workspace_id": workspace})
    try:
        assert entered.wait(synchronization_timeout(5))
        other = db.create_workspace("Other tab")
        assert client.post(f"/api/workspaces/{other}/activate").status_code == 200
    finally:
        release.set()
    job = wait_for_job_via_client(client, response.json["job_id"], wait_for_history=True)
    assert job["status"] == "completed" and job["workspace_id"] == workspace
    assert job["result"]["readiness"]["workspace_id"] == workspace
    assert calls["detection"] == ["duck-0.jpg"]
    assert client.get("/api/pipeline/page-init").json["workspace_id"] == other


def _damage_cached_quality(db, pid, *, invalid=True):
    import json

    from subject_readiness import REQUIRED_QUALITY_FIELDS
    row = db.conn.execute("SELECT a.* FROM subject_ranking_analysis a JOIN detections d ON d.id=a.detection_id WHERE d.photo_id=?", (pid,)).fetchone()
    features = json.loads(row["features"])
    if invalid:
        features["subject_clip_low"] = None
    else:
        features.update(dict.fromkeys(REQUIRED_QUALITY_FIELDS, 0.0))
        for optional in ("eye_x", "eye_y", "eye_conf", "eye_tenengrad", "noise_estimate"):
            features.pop(optional, None)
    encoded = json.dumps(features)
    db.conn.execute("UPDATE subject_ranking_analysis SET features=? WHERE detection_id=? AND source_key=?", (encoded, row["detection_id"], row["source_key"]))
    db.conn.execute("UPDATE photos SET subject_clip_low=NULL WHERE id=?", (pid,))
    db.commit()
    return row["detection_id"], row["source_key"], encoded, row["mask_png"]


@pytest.mark.parametrize("damage", ["quality", "mask", "json"])
def test_incomplete_ranking_generation_is_repaired_with_active_publication(retry_library, tmp_path, damage):
    import json
    _, db, ids, calls = retry_library
    execute(db, [ids[0]], tmp_path)
    did, key, damaged, _ = _damage_cached_quality(db, ids[0])
    if damage == "mask":
        db.conn.execute("UPDATE subject_ranking_analysis SET mask_png=? WHERE detection_id=?", (b"not a PNG", did))
    elif damage == "json":
        db.conn.execute("UPDATE subject_ranking_analysis SET features=? WHERE detection_id=?", ("corrupt JSON", did))
    db.commit()
    calls["segmentation"].clear()
    result, _ = execute(db, [ids[0]], tmp_path)
    assert result["ok"] and result["photos"][0]["status"] == "complete"
    repaired = db.subjects.ranking_analysis(did, key)
    assert repaired["features"]["subject_clip_low"] is not None
    assert repaired["features"] != json.loads(damaged)
    assert calls["segmentation"] == []  # the current valid mask supplies quality only


def test_valid_zero_generation_and_optional_fallbacks_remain_immutable(retry_library, tmp_path):
    _, db, ids, _ = retry_library
    execute(db, [ids[0]], tmp_path)
    did, key, encoded, png = _damage_cached_quality(db, ids[0], invalid=False)
    result, _ = execute(db, [ids[0]], tmp_path)
    assert result["ok"]
    row = db.conn.execute("SELECT features,mask_png FROM subject_ranking_analysis WHERE detection_id=? AND source_key=?", (did, key)).fetchone()
    assert tuple(row) == (encoded, png)


@pytest.mark.parametrize("failure", ["inference", "cancel", "publication", "inputs"])
def test_invalid_generation_survives_failed_cancelled_or_rolled_back_repair(retry_library, tmp_path, monkeypatch, failure):
    _, db, ids, _ = retry_library
    execute(db, [ids[0]], tmp_path)
    did, key, encoded, png = _damage_cached_quality(db, ids[0])
    old_path = db.conn.execute("SELECT mask_path FROM photos WHERE id=?", (ids[0],)).fetchone()[0]
    cancelled = []
    original = __import__("quality").compute_all_quality_features
    def fail_quality(*args, **kwargs):
        if failure == "inference":
            raise ValueError("quality retry failed")
        pixels = original(*args, **kwargs)
        if failure == "inputs":
            db.predictions.add(did, "Mallard", .9, "inat21", category="match")
        cancelled.append(True)
        return pixels
    monkeypatch.setattr("quality.compute_all_quality_features", fail_quality)
    if failure == "publication":
        def late_failure(*args, **kwargs):
            raise ValueError("late active publication failed")
        # This runs after cached retirement+insert, and before commit.
        monkeypatch.setattr("db.Database.commit_with_retry", late_failure)
    if failure == "cancel":
        with pytest.raises(JobCancelled):
            execute(db, [ids[0]], tmp_path, cancel=lambda: bool(cancelled))
    else:
        result, _ = execute(db, [ids[0]], tmp_path)
        assert not result["ok"]
    row = db.conn.execute("SELECT features,mask_png FROM subject_ranking_analysis WHERE detection_id=? AND source_key=?", (did, key)).fetchone()
    assert tuple(row) == (encoded, png)
    assert db.conn.execute("SELECT mask_path FROM photos WHERE id=?", (ids[0],)).fetchone()[0] == old_path
    assert Path(old_path).read_bytes() == png


@pytest.mark.parametrize("quality_only", [True, False])
def test_quality_only_preserves_mask_and_embeddings_while_remask_invalidates(retry_library, tmp_path, quality_only):
    _, db, ids, calls = retry_library
    execute(db, [ids[0]], tmp_path)
    old_path = db.conn.execute("SELECT mask_path FROM photos WHERE id=?", (ids[0],)).fetchone()[0]
    subject = np.arange(768, dtype=np.float32).tobytes()
    background = np.arange(768, dtype=np.float32)[::-1].tobytes()
    db.masks_features.update_embeddings(ids[0], subject, background, variant="vit-b14")
    if quality_only:
        db.conn.execute("UPDATE photos SET subject_clip_low=NULL WHERE id=?", (ids[0],))
    else:
        db.conn.execute("UPDATE photo_masks SET prompt_x=.4 WHERE photo_id=?", (ids[0],))
    db.commit()
    calls["segmentation"].clear()
    result, _ = execute(db, [ids[0]], tmp_path)
    assert result["ok"]
    row = db.conn.execute("SELECT mask_path,dino_subject_embedding,dino_global_embedding,dino_embedding_variant FROM photos WHERE id=?", (ids[0],)).fetchone()
    if quality_only:
        assert tuple(row) == (old_path, subject, background, "vit-b14")
        assert calls["segmentation"] == []
    else:
        assert row["mask_path"] != old_path and tuple(row)[1:] == (None, None, None)
        assert len(calls["segmentation"]) == 1


def test_detector_runtime_change_during_inference_publishes_nothing(retry_library, monkeypatch):
    from services.analysis_retry import retry_detection
    from services.detection_store import write_detection_batch
    _, db, ids, _ = retry_library
    pid = ids[0]
    write_detection_batch(db, pid, "megadetector-v6", [
        {"box": {"x": .1, "y": .1, "w": .7, "h": .7}, "confidence": .9, "category": "animal"}], "before")
    before_run = tuple(db.conn.execute("SELECT * FROM detector_runs WHERE photo_id=?", (pid,)).fetchone())
    before_boxes = [tuple(row) for row in db.conn.execute("SELECT * FROM detections WHERE photo_id=?", (pid,))]
    runtime = ["before"]
    monkeypatch.setattr("computation_cache.megadetector_runtime_fingerprint", lambda: runtime[0])
    def changed_model(path):
        runtime[0] = "after"
        return []  # must not erase the old boxes or stamp an old result with the new runtime
    monkeypatch.setattr("detector.detect_animals", changed_model)
    with pytest.raises(ValueError, match="runtime changed"):
        retry_detection(db, pid, lambda: None)
    assert tuple(db.conn.execute("SELECT * FROM detector_runs WHERE photo_id=?", (pid,)).fetchone()) == before_run
    assert [tuple(row) for row in db.conn.execute("SELECT * FROM detections WHERE photo_id=?", (pid,))] == before_boxes


@pytest.mark.parametrize("installed", [True, False])
def test_detector_first_use_prepares_identity_before_inference(retry_library, monkeypatch, installed):
    from services.analysis_retry import retry_detection
    _, db, ids, calls = retry_library
    runtime = [None]
    prepared = []
    monkeypatch.setattr("computation_cache.megadetector_runtime_fingerprint", lambda: runtime[0])
    def prepare():
        prepared.append(True)
        if installed:
            runtime[0] = "freshly-installed"
    monkeypatch.setattr("detector.ensure_megadetector_weights", prepare)
    if installed:
        retry_detection(db, ids[0], lambda: None)
        assert db.model_runs.detector_runtime_fingerprint(ids[0], "megadetector-v6") == "freshly-installed"
        assert calls["detection"] == ["duck-0.jpg"]
    else:
        with pytest.raises(ValueError, match="identity is unavailable"):
            retry_detection(db, ids[0], lambda: None)
        assert calls["detection"] == []
        assert db.model_runs.detector_runtime_fingerprint(ids[0], "megadetector-v6") is None
    assert prepared == [True]


def test_api_refreshes_database_results_when_thumbnail_cache_is_elsewhere(retry_library, tmp_path):
    import os

    from pipeline import load_results_raw, save_results_raw
    from wait import wait_for_job_via_client
    app, db, ids, _ = retry_library
    results_dir = os.path.dirname(db.job_history.catalog_file())
    masks_dir = tmp_path / "external-media-cache"
    app.config["THUMB_CACHE_DIR"] = str(masks_dir / "thumbnails")
    cache = {"photos": [{"id": pid, "flag": "none", "rating": 0, "label": "REJECT"} for pid in ids],
             "encounters": [{"photo_ids": ids, "bursts": [{"photo_ids": ids}]}], "summary": {}}
    save_results_raw(cache, results_dir, db.require_workspace_id())
    client = app.test_client()
    response = client.post("/api/photos/analysis-retry", json={"photo_ids": [ids[0]]})
    job = wait_for_job_via_client(client, response.json["job_id"], wait_for_history=True)
    assert job["status"] == "completed"
    saved = load_results_raw(results_dir, db.require_workspace_id())
    assert saved["summary"]["reject_count"] == 0
    assert next(photo for photo in saved["photos"] if photo["id"] == ids[0])["selection_eligible"]
    path = db.conn.execute("SELECT mask_path FROM photos WHERE id=?", (ids[0],)).fetchone()[0]
    assert Path(path).parent == masks_dir / "masks"
    assert load_results_raw(str(masks_dir), db.require_workspace_id()) is None


@pytest.mark.parametrize("failed", [False, True])
def test_remask_cleans_only_committed_predecessor(retry_library, tmp_path, monkeypatch, failed):
    _, db, ids, _ = retry_library
    execute(db, [ids[0]], tmp_path)
    old_path = Path(db.conn.execute("SELECT mask_path FROM photos WHERE id=?", (ids[0],)).fetchone()[0])
    original = old_path.read_bytes()
    db.conn.execute("UPDATE photo_masks SET prompt_x=.4 WHERE photo_id=?", (ids[0],))
    db.commit()
    if failed:
        def fail_commit(*args, **kwargs):
            raise ValueError("late remask commit failed")
        monkeypatch.setattr("db.Database.commit_with_retry", fail_commit)
    result, _ = execute(db, [ids[0]], tmp_path)
    current = Path(db.conn.execute("SELECT mask_path FROM photos WHERE id=?", (ids[0],)).fetchone()[0])
    assert result["ok"] is not failed and current.exists()
    assert sorted((tmp_path / "masks").glob("*.png")) == [current]
    if failed:
        assert current == old_path and old_path.read_bytes() == original
    else:
        assert current != old_path and not old_path.exists()


def test_cancel_during_regroup_wait_finishes_without_waiting_for_holder(retry_library, tmp_path, monkeypatch):
    from db import Database
    from pipeline import load_results_raw, save_results_raw
    from pipeline_locks import acquire_workspace_regroup
    from services import analysis_retry
    _, db, ids, _ = retry_library
    execute(db, [ids[0]], tmp_path)
    cache = {"photos": [{"id": ids[0]}], "encounters": [{"photo_ids": [ids[0]]}], "summary": {"sentinel": True}}
    save_results_raw(cache, str(tmp_path), db.require_workspace_id())
    entered, cancelled = threading.Event(), threading.Event()
    original_refresh = analysis_retry._refresh_review
    def refresh(*args, **kwargs):
        entered.set()
        return original_refresh(*args, **kwargs)
    monkeypatch.setattr(analysis_retry, "_refresh_review", refresh)
    path, ws = db.job_history.catalog_file(), db.require_workspace_id()
    outcomes = []
    def worker():
        own = Database(path)
        own.set_active_workspace(ws)
        try:
            execute(own, [ids[0]], tmp_path, cancel=cancelled.is_set)
        except Exception as error:
            outcomes.append(error)
        finally:
            own.close()
    lock = acquire_workspace_regroup(ws)
    lock.acquire()
    thread = threading.Thread(target=worker)
    thread.start()
    try:
        assert entered.wait(synchronization_timeout(5))
        cancelled.set()
        thread.join(synchronization_timeout(5))
        assert not thread.is_alive() and len(outcomes) == 1 and isinstance(outcomes[0], JobCancelled)
        assert load_results_raw(str(tmp_path), ws) == cache
    finally:
        lock.release()
        thread.join(synchronization_timeout(5))
