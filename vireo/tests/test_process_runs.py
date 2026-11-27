"""Describing a Process run and starting exactly the run described."""

import json
import os
from pathlib import Path

import config
import models
import pipeline_job
import pytest
from db import Database
from services import catalog_deletion, detection_store, process_runs, workspace_config
from wait import wait_for_job_via_client

MODEL = {
    "id": "m1", "name": "Test model", "model_str": "x", "model_type": "timm",
    "downloaded": True, "weights_path": "w",
}


@pytest.fixture
def one_model(monkeypatch):
    monkeypatch.setattr(models, "get_models", lambda: [MODEL])
    monkeypatch.setattr(models, "get_active_model", lambda: MODEL)


@pytest.fixture
def started(monkeypatch):
    """The params of every run started; the job itself does nothing."""
    runs = []

    def fake(job, runner, db_path, workspace_id, params, **kwargs):
        runs.append(params)
        return {"ok": True}

    monkeypatch.setattr(pipeline_job, "run_pipeline_job", fake)
    return runs


def _ids(db):
    return sorted(db.photos.get_ids())


def _plan(client, **body):
    return client.post("/api/pipeline/plan", json={"describe_run": True, **body})


def _run_of(client, **body):
    response = _plan(client, **body)
    assert response.status_code == 200, response.get_json()
    return response.get_json()["run"]


def _start(client, run, **body):
    return client.post("/api/jobs/pipeline", json={"expected_run": run, **body})


def _collection_ids(db, name_part):
    return [
        json.loads(c["rules"])[0]["value"] for c in db.collections.list_all() if name_part in c["name"]
    ]


def test_photo_digest_ignores_order_and_repeats():
    assert process_runs.photo_digest([3, 1, 2, 2]) == process_runs.photo_digest([1, 2, 3])
    assert process_runs.photo_digest([1, 23]) != process_runs.photo_digest([12, 3])


def test_every_scope_describes_the_same_photos(app_and_db, one_model):
    app, db = app_and_db
    client = app.test_client()
    ids = _ids(db)
    folder = db.folders.id_by_path("/photos/2024")
    album = db.collections.add("Album", json.dumps([{"field": "photo_ids", "value": ids}]))
    db.commit()
    runs = [
        _run_of(client, photo_ids=ids),
        _run_of(client, folder_ids=[folder]),
        _run_of(client, collection_id=album),
    ]
    assert {run["photo_digest"] for run in runs} == {process_runs.photo_digest(ids)}
    assert runs[0]["photo_count"] == len(ids)
    assert runs[0]["models"] == [{"id": "m1", "name": "Test model"}]
    assert runs[0]["workspace_id"] == db.active_workspace_id


def test_exclusions_leave_the_description(app_and_db, one_model):
    app, db = app_and_db
    ids = _ids(db)
    folder = db.folders.id_by_path("/photos/2024")
    run = _run_of(app.test_client(), folder_ids=[folder], exclude_photo_ids=[ids[0]])
    assert run["photo_digest"] == process_runs.photo_digest(ids[1:])


def test_the_plan_describes_a_run_only_when_asked_and_scoped(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    plain = client.post("/api/pipeline/plan", json={"photo_ids": _ids(db)}).get_json()
    assert "run" not in plain and plain["scope"]["photo_count"] == 3
    assert _plan(client).status_code == 400


def test_a_run_with_no_model_classifies_and_replaces_nothing(app_and_db, monkeypatch):
    app, db = app_and_db
    monkeypatch.setattr(models, "get_active_model", lambda: None)
    run = _run_of(app.test_client(), photo_ids=_ids(db), reclassify=True)
    assert run["models"] == [] and run["misses"] is False
    assert run["replaced_reviews"]["predictions"] == 0


@pytest.mark.parametrize("body, misses", [
    ({}, True),
    ({"miss_enabled": False}, False),
    ({"miss_enabled": True, "skip_regroup": True}, False),
    ({"miss_enabled": True, "skip_classify": True}, False),
])
def test_misses_follow_the_run_then_the_workspace(app_and_db, one_model, body, misses):
    app, db = app_and_db
    assert _run_of(app.test_client(), photo_ids=_ids(db), **body)["misses"] is misses


def test_the_workspace_species_lists_are_named(app_and_db, one_model):
    app, db = app_and_db
    workspace_config.set_workspace_active_labels(db, ["/lists/birds.txt"])
    assert _run_of(app.test_client(), photo_ids=_ids(db))["labels_files"] == ["/lists/birds.txt"]
    run = _run_of(app.test_client(), photo_ids=_ids(db), labels_files=["/lists/frogs.txt"])
    assert run["labels_files"] == ["/lists/frogs.txt"]


def _prediction(db, photo_id, status, detector="megadetector-v6", species="Robin", x=0.1):
    """A prediction on a box of its own, with a review; returns the prediction id."""
    det = detection_store.write_detection_batch(
        db, photo_id, detector,
        [{"box": {"x": x, "y": 0.1, "w": 0.3, "h": 0.3}, "confidence": 0.9, "category": "animal"}],
    )[0]
    db.predictions.add(det, species, 0.9, model="m1", labels_fingerprint="fp")
    prediction = db.conn.execute("SELECT id FROM predictions WHERE detection_id = ?", (det,)).fetchone()["id"]
    _review(db, prediction, status)
    return prediction


def _review(db, prediction, status):
    db.conn.execute(
        "INSERT OR REPLACE INTO prediction_review (prediction_id, workspace_id, status) VALUES (?, ?, ?)",
        (prediction, db.active_workspace_id, status),
    )
    db.commit()


def test_reclassify_counts_the_decisions_it_may_discard(app_and_db, one_model):
    app, db = app_and_db
    client = app.test_client()
    first, second, third = _ids(db)
    _prediction(db, first, "accepted")
    _prediction(db, second, "pending")
    _prediction(db, third, "rejected", detector="reviewer-selected")
    reviews = _run_of(client, photo_ids=[first, second, third], reclassify=True)["replaced_reviews"]
    assert (reviews["predictions"], reviews["photos"]) == (1, 1)
    # Reusing earlier results discards nothing.
    assert _run_of(client, photo_ids=[first])["replaced_reviews"]["predictions"] == 0


def test_the_same_number_of_different_decisions_is_a_changed_run(app_and_db, one_model, started):
    app, db = app_and_db
    client = app.test_client()
    ids = _ids(db)
    accepted, pending = _prediction(db, ids[0], "accepted"), _prediction(db, ids[1], "pending")
    run = _run_of(client, photo_ids=ids, reclassify=True)
    _review(db, accepted, "pending")
    _review(db, pending, "accepted")
    response = _start(client, run, photo_ids=ids, reclassify=True)
    assert response.status_code == 409 and "reviewed predictions" in response.get_json()["error"]
    assert started == []


@pytest.mark.parametrize("column, value", [("group_id", "g2"), ("individual", "Ana"), ("vote_count", 4),
                                           ("total_votes", 9), ("reviewed_at", "2026-01-01")])
def test_any_part_of_a_reviewed_decision_is_part_of_the_run(app_and_db, one_model, column, value):
    app, db = app_and_db
    ids = _ids(db)
    prediction = _prediction(db, ids[0], "accepted")
    before = _run_of(app.test_client(), photo_ids=ids, reclassify=True)["replaced_reviews"]
    db.conn.execute(f"UPDATE prediction_review SET {column} = ? WHERE prediction_id = ?", (value, prediction))
    db.commit()
    after = _run_of(app.test_client(), photo_ids=ids, reclassify=True)["replaced_reviews"]
    assert after["predictions"] == before["predictions"] == 1 and after["digest"] != before["digest"]


@pytest.mark.parametrize("change, named", [
    ("review", "reviewed predictions"), ("species list", "species lists"), ("mask model", "stage settings"),
    ("other setting", "stage settings"), ("root setting", "stage settings"), ("deleted photo", "photos in scope"),
    ("collection gained a photo", "photos in scope"), ("collection deleted", "photos in scope"),
])
def test_a_run_that_changes_in_the_queue_fails_before_its_first_stage(
    app_and_db, one_model, started, monkeypatch, change, named,
):
    app, db = app_and_db
    client = app.test_client()
    ids = _ids(db)
    prediction = _prediction(db, ids[0], "pending")
    body = {"photo_ids": ids, "reclassify": True}
    if change == "collection gained a photo":
        body["photo_ids"] = ids[:-1]
    run = _run_of(client, **body)
    confirm = process_runs.refuse_changed_run

    def change_then_confirm(*args, **kwargs):
        worker = Database(str(app.config["DB_PATH"]))
        worker.set_active_workspace(db.active_workspace_id)
        if change == "review":
            _review(worker, prediction, "accepted")
        elif change == "species list":
            workspace_config.set_workspace_active_labels(worker, ["/lists/new.txt"])
        elif change.startswith("collection"):
            made = next(c["id"] for c in worker.collections.list_all() if c["name"].startswith("Process "))
            if change == "collection deleted":
                worker.conn.execute("DELETE FROM collections WHERE id = ?", (made,))
            else:
                worker.conn.execute("UPDATE collections SET rules = ? WHERE id = ?",
                                    (json.dumps([{"field": "photo_ids", "value": ids}]), made))
            worker.commit()
        elif change == "deleted photo":
            catalog_deletion.delete_photos(worker, [ids[-1]])
        else:
            overrides = {
                "mask model": {"pipeline": {"sam2_variant": "sam2-tiny"}},
                "other setting": {"pipeline": {"burst_time_gap": 9}},
                "root setting": {"similarity_threshold": 0.31},
            }[change]
            worker.conn.execute("UPDATE workspaces SET config_overrides = ?", (json.dumps(overrides),))
            worker.commit()
        worker.close()
        return confirm(*args, **kwargs)

    monkeypatch.setattr(process_runs, "refuse_changed_run", change_then_confirm)
    response = _start(client, run, **body)
    assert response.status_code == 200, response.get_json()
    job = wait_for_job_via_client(client, response.get_json()["job_id"])
    assert job["status"] == "failed" and named in json.dumps(job["errors"]), job
    assert started == []


@pytest.mark.parametrize("change", ["mask model", "custom classifier", "model definition", "taxonomy"])
def test_replaced_model_files_are_a_changed_run(app_and_db, one_model, started, monkeypatch, tmp_path, change):
    import taxonomy

    app, db = app_and_db
    client = app.test_client()
    body = {"photo_ids": _ids(db)}
    masks = Path(models.DEFAULT_MODELS_DIR) / "sam2-small" / "encoder.onnx"
    weights = tmp_path / "custom" / "model.onnx"
    for path in (masks, weights):
        path.parent.mkdir(parents=True)
        path.write_bytes(b"one")
    entry = {**MODEL, "weights_path": str(weights.parent)}
    monkeypatch.setattr(models, "get_models", lambda: [entry])
    run = _run_of(client, **body)
    assert _run_of(client, **body) == run
    if change == "model definition":
        entry["model_str"] = "another"
    else:
        path = {"mask model": masks, "custom classifier": weights,
                "taxonomy": Path(taxonomy.TAXONOMY_JSON_PATH)}[change]
        path.write_bytes(b"a replacement")
    response = _start(client, run, **body)
    assert response.status_code == 409 and "installed model files" in response.get_json()["error"]
    assert started == []


def test_installing_another_classifier_does_not_change_the_run(app_and_db, one_model, monkeypatch):
    app, db = app_and_db
    client = app.test_client()
    before = _run_of(client, photo_ids=_ids(db))
    known = Path(models.DEFAULT_MODELS_DIR) / models.KNOWN_MODELS[0]["id"]
    custom = Path(models.DEFAULT_MODELS_DIR) / "somebody-elses-model"
    for folder in (known, custom):
        folder.mkdir(parents=True)
        (folder / "image_encoder.onnx").write_bytes(b"new")
    single = Path(models.DEFAULT_MODELS_DIR) / "one-file.onnx"
    single.write_bytes(b"new")
    monkeypatch.setattr(models, "get_models", lambda: [
        MODEL, {"id": "custom", "weights_path": str(custom)}, {"id": "single", "weights_path": str(single)}])
    assert _run_of(client, photo_ids=_ids(db)) == before
    # The same file in a folder no classifier owns belongs to a shared stage model.
    shared = Path(models.DEFAULT_MODELS_DIR) / "megadetector-v6"
    shared.mkdir()
    (shared / "model.onnx").write_bytes(b"new")
    assert _run_of(client, photo_ids=_ids(db))["model_files"] != before["model_files"]


@pytest.mark.parametrize("change", [
    {"skip_regroup": False}, {"skip_extract_masks": True}, {"skip_eye_keypoints": True}, {"review_mode": "species"},
    {"raw_subject_analysis": True}, {"reclassify": True}, {"miss_enabled": True}, {"eye_detect_override": True},
    {"preview_max_size": 0}, {"download_taxonomy": False}, {"skip_classify": True},
])
def test_every_option_is_part_of_the_run_described(app_and_db, one_model, started, change):
    app, db = app_and_db
    client = app.test_client()
    body = {"photo_ids": _ids(db), "skip_regroup": True, "miss_enabled": False}
    run = _run_of(client, **body)
    response = _start(client, run, **{**body, **change})
    assert response.status_code == 409 and "stages and options" in response.get_json()["error"]
    assert started == []


def test_the_default_process_is_not_a_stage_setting(app_and_db, one_model):
    app, db = app_and_db
    before = _run_of(app.test_client(), photo_ids=_ids(db))
    db.conn.execute("UPDATE workspaces SET config_overrides = ?", (json.dumps({"pipeline": {"default_process_id": 2}}),))
    db.commit()
    assert _run_of(app.test_client(), photo_ids=_ids(db)) == before


def test_settings_no_stage_reads_do_not_change_the_run(app_and_db, one_model):
    app, db = app_and_db
    before = _run_of(app.test_client(), photo_ids=_ids(db))
    overrides = {"photos_per_page": 7, "browse_stack_time_gap": 9, "inat_token": "x", "unknown_record": 1}
    db.conn.execute("UPDATE workspaces SET config_overrides = ?", (json.dumps(overrides),))
    db.commit()
    assert _run_of(app.test_client(), photo_ids=_ids(db)) == before


def test_excluded_settings_exist():
    """A setting renamed in the defaults would otherwise become part of every run unnoticed."""
    assert set(config.DEFAULTS) >= process_runs.NOT_STAGE_SETTINGS


def test_photos_on_an_offline_drive_stay_in_a_queued_run(app_and_db, one_model, started):
    app, db = app_and_db
    client = app.test_client()
    body = {"photo_ids": _ids(db)}
    run = _run_of(client, **body)
    db.conn.execute("UPDATE folders SET status = 'missing'")
    db.commit()
    response = _start(client, run, **body)
    assert response.status_code == 200, response.get_json()
    assert wait_for_job_via_client(client, response.get_json()["job_id"])["status"] == "completed"


def test_an_unchanged_queued_run_starts(app_and_db, one_model, started):
    app, db = app_and_db
    client = app.test_client()
    body = {"photo_ids": _ids(db), "reclassify": True}
    _prediction(db, body["photo_ids"][0], "accepted")
    response = _start(client, _run_of(client, **body), **body)
    assert wait_for_job_via_client(client, response.get_json()["job_id"])["status"] == "completed"
    assert len(started) == 1


@pytest.mark.parametrize("field, text", [
    ({"labels_file": "/lists/a.txt"}, "labels_file"),
    ({"collection_id": 1}, "cannot be combined"), ({"miss_enabled": "false"}, "miss_enabled"),
    ({"source_paths": []}, "source_paths"), ({"hash_duplicate_paths": []}, "hash_duplicate_paths"),
])
def test_a_described_run_refuses_what_the_launch_would_not_run(app_and_db, one_model, started, field, text):
    app, db = app_and_db
    client = app.test_client()
    body = {"folder_ids": [db.folders.id_by_path("/photos/2024")]}
    run = _run_of(client, **body)
    plan = _plan(client, **body, **field)
    assert plan.status_code == 400 and text in plan.get_json()["error"]
    assert _start(client, run, **body, **field).status_code == 400
    assert started == []


def test_a_photo_scope_runs_exactly_those_photos(app_and_db, one_model, started):
    app, db = app_and_db
    ids = _ids(db)[:2]
    response = app.test_client().post("/api/jobs/pipeline", json={"photo_ids": ids})
    assert response.status_code == 200, response.get_json()
    assert _collection_ids(db, "Process 2 photos") == [ids]


def test_a_described_collection_is_fixed_before_it_is_queued(app_and_db, one_model, started):
    app, db = app_and_db
    client = app.test_client()
    ids = _ids(db)
    album = db.collections.add("Album", json.dumps([{"field": "photo_ids", "value": ids[:2]}]))
    db.commit()
    run = _run_of(client, collection_id=album)
    response = _start(client, run, collection_id=album)
    assert response.status_code == 200, response.get_json()
    assert _collection_ids(db, "Process Album") == [ids[:2]]


def test_a_changed_scope_starts_nothing(app_and_db, one_model, started):
    app, db = app_and_db
    client = app.test_client()
    ids = _ids(db)
    album = db.collections.add("Album", json.dumps([{"field": "photo_ids", "value": ids[:2]}]))
    db.commit()
    run = _run_of(client, collection_id=album)
    db.conn.execute("UPDATE collections SET rules = ? WHERE id = ?",
                    (json.dumps([{"field": "photo_ids", "value": ids}]), album))
    db.commit()
    response = _start(client, run, collection_id=album)
    assert response.status_code == 409 and "(the photos in scope)" in response.get_json()["error"]
    other = _start(client, {**_run_of(client, photo_ids=ids), "workspace_id": 999}, photo_ids=ids)
    assert other.status_code == 409 and "(the active workspace)" in other.get_json()["error"]
    assert started == [] and _collection_ids(db, "Process ") == []


@pytest.mark.parametrize("expected", [None, [], {}, {"workspace_id": 1, "photo_digest": "a"}])
def test_a_malformed_expected_run_is_refused(app_and_db, started, expected):
    app, db = app_and_db
    response = app.test_client().post("/api/jobs/pipeline", json={"photo_ids": _ids(db), "expected_run": expected})
    assert response.status_code == 400 and started == []


@pytest.mark.parametrize("body, status, text", [
    ({"photo_ids": []}, 400, "photo_ids required"),
    ({"photo_ids": ["1"]}, 400, "integers"),
    ({"photo_ids": [2**63]}, 400, "64-bit"),
    ({"photo_ids": [999]}, 403, "active workspace"),
    ({"photo_ids": [1], "folder_ids": [1]}, 400, "cannot be combined with folder_ids"),
    ({"photo_ids": [1], "collection_id": 1}, 400, "cannot be combined with collection_id"),
    ({"photo_ids": [1], "exclude_photo_ids": [1]}, 400, "cannot be combined with exclude_photo_ids"),
])
def test_both_routes_refuse_the_same_photo_scopes(app_and_db, started, body, status, text):
    app, _ = app_and_db
    for path in ("/api/pipeline/plan", "/api/jobs/pipeline"):
        response = app.test_client().post(path, json=body)
        assert response.status_code == status, (path, response.get_json())
        assert text in response.get_json()["error"], path
    assert started == []


def test_an_unknown_collection_cannot_be_the_expected_run(app_and_db, started):
    app, db = app_and_db
    response = app.test_client().post("/api/jobs/pipeline", json={"collection_id": 999, "expected_run": {}})
    assert response.status_code == 404 and started == []


def test_folder_plan_and_started_job_agree_after_exclusions(app_and_db, one_model, started):
    app, db = app_and_db
    client = app.test_client()
    photos = {row["filename"]: row["id"] for row in db.conn.execute("SELECT id, filename FROM photos")}
    body = {
        "folder_ids": [db.folders.id_by_path("/photos/2024")],
        "exclude_paths": [os.path.join("/photos/2024/January", "bird2.jpg")],
        "exclude_photo_ids": [photos["bird1.jpg"]],
    }
    expected = [photos["bird3.jpg"]]
    run = _run_of(client, **body)
    assert run["photo_digest"] == process_runs.photo_digest(expected)
    response = _start(client, run, **body)
    assert response.status_code == 200, response.get_json()
    assert wait_for_job_via_client(client, response.get_json()["job_id"])["status"] == "completed"
    assert len(started) == 1
    assert sorted(p["id"] for p in db.collections.get_photos(started[0].collection_id)) == expected
