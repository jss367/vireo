"""A rejected region is not a subject anywhere, and the rejection is reversible.

The report behind this: a stump classified as Mule Deer, a charred branch
as Great Curassow. Rejecting the region must drop its predictions from
Review, species evidence, grouping and primary selection together, apply no
species to any other frame, and undo in one step.
"""

import json
import os

import numpy as np
import pytest
from PIL import Image
from pipeline import load_photo_features, refresh_cache_predictions_for_photos
from services import detection_store, photo_ingest, subject_verdicts, workspace_folders
from services.edit_history import redo_last_undo, undo_last_edit
from subjects import analyze_photo, payload, sync_primary

STUMP = {"box": {"x": .05, "y": .1, "w": .35, "h": .8}, "confidence": .95, "category": "animal"}
BIRD = {"box": {"x": .6, "y": .1, "w": .35, "h": .8}, "confidence": .7, "category": "animal"}


@pytest.fixture
def scene(db, tmp_path):
    """One photo: a confident stump box and a weaker real-bird box."""
    folder = workspace_folders.add_folder(db, str(tmp_path), name="Scene")
    path = tmp_path / "DSC_3539.jpg"
    pixels = np.full((160, 320, 3), 90, dtype=np.uint8)
    pixels[:, 160:] = np.where((np.indices((160, 160)).sum(axis=0) // 4 % 2)[..., None], 160, 60)
    Image.fromarray(pixels).save(path)
    photo_id = photo_ingest.add_photo(db, folder_id=folder, filename=path.name, extension=".jpg",
                                      file_size=path.stat().st_size, file_mtime=1)
    stump, bird = detection_store.write_detection_batch(db, photo_id, "megadetector-v6", [STUMP, BIRD])
    db.predictions.add(stump, "Mule Deer", .93, "bioclip")
    db.predictions.add(bird, "Hairy Woodpecker", .61, "bioclip")
    db.commit()
    return photo_id, stump, bird, path


def _verdict(db, det):
    return db.detections.row(det)["subject_verdict"]


def test_rejected_region_leaves_every_retained_read(db, scene):
    photo_id, stump, bird, _ = scene
    assert [d["id"] for d in detection_store.get_detections(db, photo_id)] == [stump, bird]

    result = subject_verdicts.set_subject_verdict(db, stump, "not_animal", source="reviewer", reason="a stump")
    assert result["changed"] and result["previous_verdict"] is None
    assert _verdict(db, stump) == "not_animal"
    # Retained reads: by photo, in bulk, and at any floor.
    assert [d["id"] for d in detection_store.get_detections(db, photo_id)] == [bird]
    assert [d["id"] for d in detection_store.get_detections(db, photo_id, min_conf=0)] == [bird]
    assert [d["id"] for d in detection_store.get_detections_for_photos(db, [photo_id])[photo_id]] == [bird]
    # Only the readers that show what was rejected ask for it back.
    assert {d["id"] for d in detection_store.get_detections(db, photo_id, min_conf=0, include_rejected=True)} == {stump, bird}
    # Species evidence for the pipeline comes from the bird alone.
    features = load_photo_features(db, photo_ids=[photo_id])[0]
    assert [entry[0] for entry in features["species_top5"]] == ["Hairy Woodpecker"]
    assert features["detection_box"]["x"] == pytest.approx(.6)
    # Review's listing no longer offers the stump's species.
    assert {row["species"] for row in db.predictions.get_rows(photo_ids=[photo_id])} == {"Hairy Woodpecker"}
    # Nothing was deleted: the prediction rows wait for a restore.
    assert db.conn.execute("SELECT COUNT(*) FROM predictions WHERE detection_id = ?", (stump,)).fetchone()[0] == 1


def test_rejecting_the_primary_promotes_the_next_box_and_restoring_returns_it(db, scene):
    photo_id, stump, bird, path = scene
    analyze_photo(db, photo_id, path)
    sync_primary(db, photo_id)
    db.commit()
    assert payload(db, photo_id)["primary_detection_id"] in (stump, bird)
    subject_verdicts.set_subject_verdict(db, stump, "not_animal")
    after = payload(db, photo_id)
    assert after["primary_detection_id"] == bird
    assert [s["id"] for s in after["subjects"]] == [bird]
    assert [r["id"] for r in after["rejected"]] == [stump]
    assert after["rejected"][0]["predictions"][0]["species"] == "Mule Deer"
    assert db.subjects.primary_state(photo_id)["detection_id"] == bird

    restored = subject_verdicts.set_subject_verdict(db, stump, None)
    assert restored["changed"] and restored["previous_verdict"] == "not_animal"
    assert _verdict(db, stump) is None
    assert payload(db, photo_id)["rejected"] == []
    assert {s["id"] for s in payload(db, photo_id)["subjects"]} == {stump, bird}


def test_verdict_survives_a_rerun_that_finds_the_same_box_and_goes_with_a_retired_one(db, scene):
    photo_id, stump, bird, _ = scene
    subject_verdicts.set_subject_verdict(db, stump, "not_animal")
    # Same boxes again: content-addressed ids, the verdict column untouched.
    again = detection_store.write_detection_batch(db, photo_id, "megadetector-v6", [STUMP, BIRD])
    assert again == [stump, bird]
    assert _verdict(db, stump) == "not_animal"
    # The detector no longer proposes the stump: its row and verdict go.
    detection_store.write_detection_batch(db, photo_id, "megadetector-v6", [BIRD])
    assert db.detections.row(stump) is None
    assert [d["id"] for d in detection_store.get_detections(db, photo_id)] == [bird]


def test_verdict_survives_runtime_fingerprint_replacement(db, scene):
    photo_id, stump, bird, _ = scene
    detection_store.write_detection_batch(
        db, photo_id, "megadetector-v6", [STUMP, BIRD], runtime_fingerprint="rt-a",
    )
    subject_verdicts.set_subject_verdict(db, stump, "not_animal")
    # A new runtime retires the old (photo, model) rows before upserting;
    # the id is content-addressed so the stump's id is unchanged under rt-b
    # and the reviewer's verdict must survive the retirement DELETE.
    again = detection_store.write_detection_batch(
        db, photo_id, "megadetector-v6", [STUMP, BIRD], runtime_fingerprint="rt-b",
    )
    assert again == [stump, bird]
    assert _verdict(db, stump) == "not_animal"
    # A forced replace without a prior detector_runs row (legacy state)
    # takes the same retirement path and must also preserve the verdict.
    db.conn.execute(
        "DELETE FROM detector_runs WHERE photo_id = ? AND detector_model = ?",
        (photo_id, "megadetector-v6"),
    )
    db.commit()
    detection_store.write_detection_batch(
        db, photo_id, "megadetector-v6", [STUMP, BIRD],
        runtime_fingerprint="rt-c", force_runtime_replace=True,
    )
    assert _verdict(db, stump) == "not_animal"


def test_verdict_refuses_full_image_unknown_and_foreign_regions(db, scene, tmp_path):
    photo_id, stump, bird, _ = scene
    with pytest.raises(subject_verdicts.SubjectVerdictError) as missing:
        subject_verdicts.set_subject_verdict(db, 424242, "not_animal")
    assert missing.value.status == 404
    full = detection_store.write_detection_batch(db, photo_id, "full-image", [
        {"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 1.0, "category": "animal"}])[0]
    with pytest.raises(subject_verdicts.SubjectVerdictError) as whole:
        subject_verdicts.set_subject_verdict(db, full, "not_animal")
    assert whole.value.status == 400
    with pytest.raises(subject_verdicts.SubjectVerdictError):
        subject_verdicts.validate_verdict("maybe")
    with pytest.raises(subject_verdicts.SubjectVerdictError):
        subject_verdicts.validate_provenance({"reason": "x" * 301})
    # A repeated verdict writes nothing.
    subject_verdicts.set_subject_verdict(db, stump, "not_animal")
    history_count = len(db.edit_history.list_recent())
    assert subject_verdicts.set_subject_verdict(db, stump, "not_animal")["changed"] is False
    assert len(db.edit_history.list_recent()) == history_count
    # A photo outside the active workspace is not reachable.
    other = db.create_workspace("Other")
    db.set_active_workspace(other)
    with pytest.raises(subject_verdicts.SubjectVerdictError) as foreign:
        subject_verdicts.set_subject_verdict(db, bird, "not_animal")
    assert foreign.value.status == 404


def test_verdict_is_one_undo_step_with_provenance(db, scene):
    photo_id, stump, bird, _ = scene
    subject_verdicts.set_subject_verdict(db, stump, "not_animal", source="Claude", reason="charred branch")
    entry = db.edit_history.list_recent()[0]
    assert entry["action_type"] == "subject_verdict"
    assert entry["description"] == "Rejected region as not an animal — Claude: charred branch"
    assert entry["source"] == "Claude" and entry["reason"] == "charred branch"
    assert json.loads(entry["new_value"]) == {"detection_id": stump, "verdict": "not_animal"}

    assert undo_last_edit(db)["id"] == entry["id"]
    assert _verdict(db, stump) is None
    assert [d["id"] for d in detection_store.get_detections(db, photo_id)] == [stump, bird]
    assert redo_last_undo(db)["id"] == entry["id"]
    assert _verdict(db, stump) == "not_animal"
    assert [d["id"] for d in detection_store.get_detections(db, photo_id)] == [bird]
    # The primary follows each replay.
    assert db.subjects.primary_state(photo_id)["detection_id"] == bird
    undo_last_edit(db)
    assert db.subjects.primary_state(photo_id)["detection_id"] == stump


@pytest.mark.timeout(10)
def test_every_verdict_write_refuses_at_once_while_analysis_holds_the_photo(db, scene):
    """A verdict, an undo and a redo all refuse (409) while the photo lock is held, and never wait.

    Subject analysis holds the per-photo mask lock across its read-then-write
    of the primary state, so a verdict written meanwhile could be projected
    over by stale reads. ``/api/undo`` and ``/api/redo`` hold SQLite's writer
    lock through ``under_prediction_decision_lock`` when ``replay_history``
    runs, and analysis takes the photo lock before the writer lock, so a
    replay that waited for the photo lock would deadlock until the busy
    timeout. The timeout mark pins the non-blocking acquire; the rolled-back
    verdict pins that a refused replay changes nothing.
    """
    import pipeline_locks

    photo_id, stump, _, _ = scene
    held = pipeline_locks.acquire_photo_mask(photo_id)
    assert held.acquire(blocking=False)
    try:
        with pytest.raises(subject_verdicts.SubjectVerdictError) as refused:
            subject_verdicts.set_subject_verdict(db, stump, "not_animal")
        assert refused.value.status == 409
        assert _verdict(db, stump) is None and not db.in_transaction
    finally:
        held.release()
    subject_verdicts.set_subject_verdict(db, stump, "not_animal")
    assert held.acquire(blocking=False)
    try:
        with db.transaction(immediate=True):
            with pytest.raises(subject_verdicts.SubjectVerdictError) as refused:
                undo_last_edit(db)
            assert refused.value.status == 409
        assert _verdict(db, stump) == "not_animal"
    finally:
        held.release()
    assert undo_last_edit(db) is not None
    assert _verdict(db, stump) is None
    assert held.acquire(blocking=False)
    try:
        with db.transaction(immediate=True), pytest.raises(subject_verdicts.SubjectVerdictError):
            redo_last_undo(db)
        assert _verdict(db, stump) is None
    finally:
        held.release()
    assert redo_last_undo(db) is not None
    assert _verdict(db, stump) == "not_animal"


def test_verdict_reads_the_region_only_under_the_writer_lock(db, scene):
    """``BEGIN IMMEDIATE`` precedes the detection, visibility and prior-verdict reads.

    Two overlapping rejections that both read the prior verdict before
    taking the writer lock would both record ``old_value`` null, and undoing
    the second would restore the region while the first stays applied; a
    detach committed between the visibility check and the lock would let a
    catalog-wide verdict through on a photo the workspace no longer shows.
    Reading under the lock closes both windows, and an unchanged verdict
    still writes nothing.
    """
    photo_id, stump, _, _ = scene
    for verdict in ("not_animal", "not_animal"):
        statements = []
        db.conn.set_trace_callback(statements.append)
        try:
            result = subject_verdicts.set_subject_verdict(db, stump, verdict)
        finally:
            db.conn.set_trace_callback(None)
        begin = next(i for i, sql in enumerate(statements) if sql.startswith("BEGIN IMMEDIATE"))
        detection_reads = [i for i, sql in enumerate(statements) if "FROM detections" in sql]
        visibility_reads = [i for i, sql in enumerate(statements) if "photo_workspace_visibility" in sql]
        assert detection_reads and visibility_reads, statements
        assert begin < min(detection_reads + visibility_reads), statements
        assert result["previous_verdict"] == (None if result["changed"] else "not_animal")
    assert not db.in_transaction
    # The second, unchanged verdict took the lock, read, and wrote nothing.
    writes = [sql for sql in statements if sql.startswith(("UPDATE", "INSERT", "DELETE"))]
    assert writes == [], writes
    assert _verdict(db, stump) == "not_animal"


def test_sequential_verdicts_record_the_state_each_one_replaces(db, scene):
    """Reject, restore, reject: each history row's old value is the verdict it replaced.

    Pins the shape the race would break: a second rejection that read its
    prior state before the first committed would record null twice.
    """
    photo_id, stump, _, _ = scene
    subject_verdicts.set_subject_verdict(db, stump, "not_animal")
    subject_verdicts.set_subject_verdict(db, stump, None)
    subject_verdicts.set_subject_verdict(db, stump, "not_animal")
    # Newest first: each step's old value is the verdict the step before it wrote.
    for expected_old, expected_new in ((None, "not_animal"), ("not_animal", None), (None, "not_animal")):
        entry, items = db.edit_history.next_undo()
        assert entry["action_type"] == "subject_verdict" and len(items) == 1
        assert json.loads(items[0]["old_value"])["verdict"] == expected_old
        assert json.loads(items[0]["new_value"])["verdict"] == expected_new
        assert _verdict(db, stump) == expected_new
        undo_last_edit(db)
        assert _verdict(db, stump) == expected_old


def test_cache_refresh_recomputes_only_the_photos_evidence(db, scene, tmp_path):
    photo_id, stump, bird, _ = scene
    sibling = photo_ingest.add_photo(db, folder_id=db.photos.get(photo_id)["folder_id"],
                                     filename="DSC_3540.jpg", extension=".jpg", file_size=1, file_mtime=2)
    sibling_det = detection_store.write_detection_batch(db, sibling, "megadetector-v6", [STUMP])[0]
    db.predictions.add(sibling_det, "Mule Deer", .9, "bioclip")
    db.commit()
    ws = db.require_workspace_id()
    cache_dir = str(tmp_path / "cache")
    os.makedirs(cache_dir)
    photos = [dict(p) for p in load_photo_features(db, photo_ids=[photo_id, sibling])]
    for p in photos:
        p.pop("dino_subject_embedding", None)
        p.pop("dino_global_embedding", None)
    results = {
        "photos": photos,
        "encounters": [{
            "species": ["Mule Deer", .9], "species_predictions": [], "photo_ids": [photo_id, sibling],
            "confirmed_species": None, "confirmed_species_list": [], "species_confirmed": False,
            "photo_count": 2, "burst_count": 1, "time_range": [None, None],
            "bursts": [{"photo_ids": [photo_id, sibling], "species_predictions": [],
                        "species_override": {"species": "Mule Deer", "confirmed": False}}],
        }],
        "summary": {},
    }
    path = os.path.join(cache_dir, f"pipeline_results_ws{ws}.json")
    with open(path, "w") as f:
        json.dump(results, f)

    subject_verdicts.set_subject_verdict(db, stump, "not_animal")
    assert refresh_cache_predictions_for_photos(db, cache_dir, ws, [photo_id]) is True
    with open(path) as f:
        data = json.load(f)
    by_id = {p["id"]: p for p in data["photos"]}
    assert [e[0] for e in by_id[photo_id]["species_top5"]] == ["Hairy Woodpecker"]
    assert [s["detection_id"] for s in by_id[photo_id]["subjects"]] == [bird]
    # The sibling frame keeps its own evidence: nothing was applied to it.
    assert [e[0] for e in by_id[sibling]["species_top5"]] == ["Mule Deer"]
    encounter = data["encounters"][0]
    assert encounter["species"][0] == "Mule Deer"  # the sibling still votes for it
    assert {v["species"] for v in encounter["species_predictions"]} == {"Mule Deer", "Hairy Woodpecker"}
    assert encounter["bursts"][0]["species_override"] == {"species": "Mule Deer", "confirmed": False}

    subject_verdicts.set_subject_verdict(db, sibling_det, "not_animal")
    assert refresh_cache_predictions_for_photos(db, cache_dir, ws, [sibling]) is True
    with open(path) as f:
        data = json.load(f)
    encounter = data["encounters"][0]
    assert encounter["species"][0] == "Hairy Woodpecker"
    assert encounter["bursts"][0]["species_override"] == {"species": "Hairy Woodpecker", "confirmed": False}
    # Photos the cache does not hold change nothing.
    assert refresh_cache_predictions_for_photos(db, cache_dir, ws, [999999]) is False


def _detections(db, photo_id):
    return detection_store.write_detection_batch(db, photo_id, "megadetector-v6", [STUMP, BIRD])


def test_route_rejects_restores_and_undoes_with_the_subject_payload(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    photo_id = db.photos.list_page()[0]["id"]
    stump, bird = _detections(db, photo_id)
    db.predictions.add(stump, "Zebra", .97, "bioclip")
    db.commit()

    missing = client.post(f"/api/detections/{stump}/subject-verdict", json={})
    assert missing.status_code == 400
    bad = client.post(f"/api/detections/{stump}/subject-verdict", json={"verdict": "maybe"})
    assert bad.status_code == 400
    unknown = client.post("/api/detections/424242/subject-verdict", json={"verdict": "not_animal"})
    assert unknown.status_code == 404

    response = client.post(f"/api/detections/{stump}/subject-verdict",
                           json={"verdict": "not_animal", "source": "Claude", "reason": "stump"})
    assert response.status_code == 200, response.json
    body = response.json
    assert body["ok"] and body["changed"] and body["verdict"] == "not_animal"
    assert body["photo_id"] == photo_id
    assert [s["id"] for s in body["subjects"]["subjects"]] == [bird]
    assert [r["id"] for r in body["subjects"]["rejected"]] == [stump]
    assert "pipeline_cache_stale" not in body
    assert client.get("/api/undo/status").json["count"] == 1
    # The subject panel's own read agrees, and the crop of a rejected region still renders.
    assert [r["id"] for r in client.get(f"/api/photos/{photo_id}/subjects").json["rejected"]] == [stump]
    # Review no longer lists the stump's Zebra.
    listing = client.get("/api/predictions", query_string={"photo_id": photo_id}).json["predictions"]
    assert all(p["detection_id"] != stump for p in listing)

    # While subject analysis holds the photo, undo and redo refuse with the
    # same 409 the verdict route gives, and the history entry stays put.
    import pipeline_locks
    held = pipeline_locks.acquire_photo_mask(photo_id)
    assert held.acquire(blocking=False)
    try:
        busy = client.post("/api/undo")
        assert busy.status_code == 409 and "analysis is running" in busy.json["error"]
        assert db.detections.row(stump)["subject_verdict"] == "not_animal"
        assert client.get("/api/undo/status").json["count"] == 1
    finally:
        held.release()

    assert client.post("/api/undo").status_code == 200
    assert db.detections.row(stump)["subject_verdict"] is None
    assert held.acquire(blocking=False)
    try:
        assert client.post("/api/redo").status_code == 409
        assert db.detections.row(stump)["subject_verdict"] is None
    finally:
        held.release()
    assert client.post("/api/redo").status_code == 200
    assert db.detections.row(stump)["subject_verdict"] == "not_animal"

    restored = client.post(f"/api/detections/{stump}/subject-verdict", json={"verdict": None})
    assert restored.status_code == 200
    assert restored.json["changed"] and restored.json["subjects"]["rejected"] == []
    again = client.post(f"/api/detections/{stump}/subject-verdict", json={"verdict": None})
    assert again.status_code == 200 and again.json["changed"] is False


def test_route_reports_a_stale_pipeline_cache_instead_of_failing(app_and_db, monkeypatch):
    app, db = app_and_db
    client = app.test_client()
    photo_id = db.photos.list_page()[0]["id"]
    stump, _ = _detections(db, photo_id)
    import pipeline

    def explode(*args, **kwargs):
        raise OSError("disk full")

    monkeypatch.setattr(pipeline, "refresh_cache_predictions_for_photos", explode)
    response = client.post(f"/api/detections/{stump}/subject-verdict", json={"verdict": "not_animal"})
    assert response.status_code == 200
    assert response.json["pipeline_cache_stale"] is True
    assert "Process Review" in response.json["warning"]
    assert db.detections.row(stump)["subject_verdict"] == "not_animal"


def _seed_cache(cache_dir, workspace_id, photos):
    serialized = [dict(p) for p in photos]
    for p in serialized:
        p.pop("dino_subject_embedding", None)
        p.pop("dino_global_embedding", None)
    seed = {"photos": serialized, "encounters": [], "summary": {}}
    with open(os.path.join(cache_dir, f"pipeline_results_ws{workspace_id}.json"), "w") as f:
        json.dump(seed, f)


def test_cache_refresh_rewrites_every_workspace_that_sees_the_photo(db, scene, tmp_path):
    photo_id, stump, bird, _ = scene
    first_ws = db.require_workspace_id()
    other_ws = db.create_workspace("Other")
    folder_id = db.photos.get(photo_id)["folder_id"]
    db.set_active_workspace(first_ws)
    workspace_folders.add_workspace_folder(db, other_ws, folder_id)
    assert {first_ws, other_ws} <= db.photo_visibility.workspace_ids_for_photo(photo_id)

    cache_dir = str(tmp_path / "cache")
    os.makedirs(cache_dir)
    photos = load_photo_features(db, photo_ids=[photo_id])
    _seed_cache(cache_dir, first_ws, photos)
    _seed_cache(cache_dir, other_ws, photos)

    subject_verdicts.set_subject_verdict(db, stump, "not_animal")
    db_path = os.path.join(cache_dir, "test.db")
    assert subject_verdicts.refresh_pipeline_cache(db, db_path, [photo_id]) is True
    # The verdict is catalog-wide, so every workspace that lists the photo
    # has its cached species rewritten with its own effective config.
    for ws in (first_ws, other_ws):
        with open(os.path.join(cache_dir, f"pipeline_results_ws{ws}.json")) as f:
            cached = json.load(f)
        by_id = {p["id"]: p for p in cached["photos"]}
        assert [e[0] for e in by_id[photo_id]["species_top5"]] == ["Hairy Woodpecker"], (
            f"workspace {ws} cache still names the rejected region's species"
        )
    # The active workspace is restored after the multi-workspace refresh.
    assert db.active_workspace_id == first_ws


def test_cache_refresh_holds_the_workspace_regroup_lock(db, scene, tmp_path, monkeypatch):
    photo_id, stump, _, _ = scene
    cache_dir = str(tmp_path / "cache")
    os.makedirs(cache_dir)
    ws = db.require_workspace_id()
    _seed_cache(cache_dir, ws, load_photo_features(db, photo_ids=[photo_id]))

    import pipeline
    import pipeline_locks
    events = []
    real_acquire = pipeline_locks.acquire_workspace_regroup

    class _Trace:
        def __init__(self, inner): self.inner = inner
        def __enter__(self):
            events.append(("acquired", self.workspace_id))
            self.inner.__enter__()
            return self
        def __exit__(self, *exc):
            events.append(("released", self.workspace_id))
            return self.inner.__exit__(*exc)

    def tracer(workspace_id):
        events.append(("asked", workspace_id))
        inner = real_acquire(workspace_id)
        trace = _Trace(inner)
        trace.workspace_id = workspace_id
        return trace

    real_refresh = pipeline.refresh_cache_predictions_for_photos

    def spy(db_, dir_, ws_, ids):
        events.append(("refresh", ws_))
        return real_refresh(db_, dir_, ws_, ids)

    monkeypatch.setattr(pipeline_locks, "acquire_workspace_regroup", tracer)
    monkeypatch.setattr(pipeline, "refresh_cache_predictions_for_photos", spy)

    subject_verdicts.set_subject_verdict(db, stump, "not_animal")
    db_path = os.path.join(cache_dir, "test.db")
    assert subject_verdicts.refresh_pipeline_cache(db, db_path, [photo_id]) is True
    # The lock is acquired before the refresh reads the cache and released
    # after it writes, so an overlapping regroup, detach or species
    # confirmation cannot interleave with the load-modify-dump.
    assert events == [("asked", ws), ("acquired", ws), ("refresh", ws), ("released", ws)]


def test_species_evidence_names_the_rejection_and_drops_the_candidate(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    photo_id = db.photos.list_page()[0]["id"]
    stump, bird = _detections(db, photo_id)
    db.predictions.add(stump, "Great Curassow", .96, "bioclip")
    db.predictions.add(bird, "Hairy Woodpecker", .6, "bioclip")
    db.commit()
    before = client.post("/api/photos/species-evidence", json={"photo_ids": [photo_id]}).json
    assert {c["species_identity"]["name"] for c in before["candidate_selections"]} == {"Great Curassow", "Hairy Woodpecker"}
    subject_verdicts.set_subject_verdict(db, stump, "not_animal")
    after = client.post("/api/photos/species-evidence", json={"photo_ids": [photo_id]}).json
    assert {c["species_identity"]["name"] for c in after["candidate_selections"]} == {"Hairy Woodpecker"}
    subject = next(s for s in after["photos"][0]["subjects"] if s["detection_id"] == stump)
    assert subject["retained"] is False
    assert any("rejected this region" in reason for reason in subject["review_reasons"])
