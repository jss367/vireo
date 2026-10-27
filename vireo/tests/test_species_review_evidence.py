"""Agent evidence never promotes suggestions or capture groups to confirmed IDs."""

import pytest
from labels_fingerprint import compute_fingerprint
from services import workspace_config


def region(db, tmp_path, monkeypatch, names=("Robin", "Mallard")):
    import services.species_review_evidence as evidence

    path = tmp_path / "california.txt"
    path.write_text("\n".join(names))
    meta = {"labels_file": str(path), "name": "California birds"}
    monkeypatch.setattr(evidence, "get_saved_labels", lambda: [meta])
    workspace_config.set_workspace_active_labels(db, [str(path)])
    return compute_fingerprint(list(names)), path


def detect(db, photo=1, *, model="detector", x=.1, score=.9):
    return db.detections.save(photo, [{"box": {"x": x, "y": .2, "w": .2, "h": .3},
                                     "confidence": score, "category": "animal"}], detector_model=model)[0]


def packet(app, ids):
    response = app.test_client().post("/api/photos/species-evidence", json={"photo_ids": ids})
    assert response.status_code == 200, response.get_json()
    return response.get_json()


def test_disagreement_is_bound_to_each_subject_and_read_is_nonmutating(app_and_db, tmp_path, monkeypatch):
    app, db = app_and_db
    fp, _ = region(db, tmp_path, monkeypatch)
    first, second = detect(db), detect(db, model="another detector", x=.6)
    db.predictions.add(first, "Robin", .9999, "model A", labels_fingerprint=fp)
    db.predictions.add(first, "Mallard", .8, "model B", labels_fingerprint=fp)
    db.predictions.add(second, "Mallard", .9, "model A", labels_fingerprint=fp)
    db.photo_review.set_flag(1, "flagged")
    before = db.conn.total_changes
    history_before = len(db.edit_history.list_recent())
    result = packet(app, [1, 2])
    # The request uses its own connection; also check persistent edit state.
    assert db.conn.total_changes == before
    assert len(db.edit_history.list_recent()) == history_before
    assert db.species_provenance.assignments(1) == []
    assert db.photos.get(1)["flag"] == "flagged"
    assert result["automatic_assignment_eligible"] is False
    assert result["continuity"]["verified"] is False
    subjects = {s["detection_id"]: s for s in result["photos"][0]["subjects"]}
    assert all(p["evidence"]["disagreement"] for p in subjects[first]["predictions"])
    assert not subjects[second]["predictions"][0]["evidence"]["disagreement"]
    assert all(not p["evidence"]["score"]["calibrated_correctness"]
               for s in subjects.values() for p in s["predictions"])
    assert f"detection_id={first}" in subjects[first]["crop_url"]
    assert f"detection_id={second}" in subjects[second]["crop_url"]
    assert any("Multiple localized regions" in r for r in result["photos"][0]["review_reasons"])
    assert any("Animal absence is not established" in r for r in result["photos"][1]["review_reasons"])
    assert all(not c["propagation_verified"] for c in result["candidate_selections"])
    assert all(c["photo_ids"] == [1] for c in result["candidate_selections"])


def test_changed_region_selection_exposes_old_run_without_reusing_it(app_and_db, tmp_path, monkeypatch):
    app, db = app_and_db
    fp, ca = region(db, tmp_path, monkeypatch, names=("Robin",))
    wa = tmp_path / "washington.txt"
    wa.write_text("Mallard")
    original_fp = compute_fingerprint(["Robin", "Mallard"])
    db.model_runs.upsert_labels_fingerprint(original_fp, "California + Washington", [str(ca), str(wa)], 2)
    det = detect(db)
    db.predictions.add(det, "Robin", .99, "model A", labels_fingerprint=original_fp)
    result = packet(app, [1])
    assert result["current_label_selection"]["source"] == "workspace"
    assert result["current_label_selection"]["fingerprint"] == fp
    assert result["current_label_selection"]["lists"][0]["name"] == "California birds"
    assert result["stored_label_sets"][0]["sources"] == [str(ca), str(wa)]
    prediction = result["photos"][0]["subjects"][0]["predictions"][0]
    assert prediction["matches_current_label_selection"] is False
    assert any("different species list" in r for r in result["photos"][0]["review_reasons"])
    assert result["candidate_selections"] == []


def test_historical_alternatives_remain_visible_but_are_not_candidates(app_and_db, tmp_path, monkeypatch):
    app, db = app_and_db
    fp, _ = region(db, tmp_path, monkeypatch)
    det = detect(db)
    db.predictions.add(det, "Old wrong bird", 1, "model A", labels_fingerprint="old")
    db.predictions.add(det, "Robin", .7, "model A", labels_fingerprint=fp)
    result = packet(app, [1])
    predictions = result["photos"][0]["subjects"][0]["predictions"]
    old = next(p for p in predictions if p["species"] == "Old wrong bird")
    assert old["historical"] and not old["current_review_row"]
    assert [c["species_identity"]["name"] for c in result["candidate_selections"]] == ["Robin"]


def test_full_image_and_deferred_evidence_do_not_hide_uncertainty(app_and_db, tmp_path, monkeypatch):
    app, db = app_and_db
    fp, _ = region(db, tmp_path, monkeypatch)
    fallback = detect(db, model="full-image", score=0)
    db.predictions.add(fallback, "Robin", 1, "model A", labels_fingerprint=fp)
    det = detect(db, photo=2)
    db.predictions.add(det, "Mallard", .99, "model A", labels_fingerprint=fp)
    db.predictions.set_evidence_review(det, "not_enough_evidence")
    db.commit()
    result = packet(app, [1, 2])
    subject = result["photos"][0]["subjects"][0]
    assert not subject["localized"] and subject["crop_url"] is None
    assert not subject["propagation_verified"]
    second = result["photos"][1]["subjects"][0]
    assert second["predictions"][0]["evidence"]["subject"]["decision"] == "not_enough_evidence"
    assert any("Not enough evidence" in r for r in second["review_reasons"])
    assert result["candidate_selections"] == []


@pytest.mark.parametrize("body", [None, [], {}, {"photo_ids": []}, {"photo_ids": [True]},
    {"photo_ids": [0]}, {"photo_ids": [1.0]}, {"photo_ids": [1, 1]}, {"photo_ids": list(range(1, 102))}])
def test_invalid_or_oversized_scope_is_rejected(app_and_db, body):
    app, _ = app_and_db
    assert app.test_client().post("/api/photos/species-evidence", json=body).status_code == 400


def test_missing_or_hidden_photo_fails_the_whole_selection(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    assert client.post("/api/photos/species-evidence", json={"photo_ids": [1, 999]}).status_code == 404
    other = db.create_workspace("Empty workspace")
    assert client.post(f"/api/workspaces/{other}/activate").status_code == 200
    assert client.post("/api/photos/species-evidence", json={"photo_ids": [1]}).status_code == 404


def test_versioned_evidence_requires_runtime_token(app_and_db):
    app, _ = app_and_db
    client = app.test_client()
    assert client.post("/api/v1/photos/species-evidence", json={"photo_ids": [1]}).status_code == 401
    response = client.post("/api/v1/photos/species-evidence", json={"photo_ids": [1]},
                           headers={"X-Vireo-Token": "test-token-123"})
    assert response.status_code == 200
