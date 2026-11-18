"""Region removal undo must preserve decisions from every workspace."""
import pytest
from services import workspace_folders
from test_reviewer_frame_proofs import _edit, _save, _token
from test_reviewer_frame_proofs import original_photo as _original_photo

original_photo = _original_photo


@pytest.mark.parametrize("remove_primary", [True, False])
def test_region_removal_undo_restores_reviewed_predictions(original_photo, remove_primary):
    app, db, pid, path = original_photo
    client = app.test_client()
    token = _token(client, pid)
    first = _save(client, pid, token).json["primary_detection_id"]
    second = client.put(f"/api/photos/{pid}/subjects/reviewer-box", json={
        "box": {"x": .7, "y": .1, "w": .2, "h": .3}, "frame_token": token,
    }).json["primary_detection_id"]
    region = second if remove_primary else first
    original_ws = db.require_workspace_id()
    other_ws = db.workspaces.create("Synthetic other workspace")
    db.set_active_workspace(other_ws)
    workspace_folders.add_folder(db, str(path.parent), name="Shared synthetic frame")
    db.set_active_workspace(original_ws)
    for species, status in (("Fictional bird A", "accepted"), ("Fictional bird B", "rejected")):
        db.predictions.add(region, species, .9, "synthetic-classifier", status=status)
    predictions = [row[0] for row in db.conn.execute("SELECT id FROM predictions WHERE detection_id=? ORDER BY id", (region,))]
    for pred, status in zip(predictions, ("rejected", "accepted"), strict=True):
        db.predictions.set_review_status(pred, other_ws, status, individual="fictional individual", group_id="synthetic group")
    db.predictions.set_evidence_review(region, "subject_valid")
    db.set_active_workspace(other_ws)
    db.predictions.set_evidence_review(region, "invalid_subject")
    db.set_active_workspace(original_ws)
    db.edit_history.ensure_prediction_instances(predictions)
    db.conn.execute("UPDATE prediction_review SET vote_count=3,total_votes=4,reviewed_at='2020-01-02 03:04:05'")
    db.commit()
    def saved():
        return [dict(row) for row in db.conn.execute("SELECT * FROM predictions WHERE detection_id=? ORDER BY id", (region,))], [
            dict(row) for row in db.conn.execute("SELECT * FROM prediction_review WHERE prediction_id IN (?, ?) ORDER BY prediction_id, workspace_id", predictions)], [
            dict(row) for row in db.conn.execute("SELECT * FROM prediction_history_instances WHERE prediction_id IN (?, ?) ORDER BY prediction_id", predictions)], [
            dict(row) for row in db.conn.execute("SELECT * FROM detection_evidence_reviews WHERE detection_id=? ORDER BY workspace_id", (region,))]
    before = saved()
    body = {**_edit(client, pid, region), "operation": "remove"}
    assert client.put(f"/api/photos/{pid}/subjects/reviewer-box", json=body).status_code == 200
    assert db.detections.row(region) is None
    assert saved() == ([], [], [], [])
    for _ in range(2):
        assert client.post("/api/undo").status_code == 200
        assert saved() == before
        assert not db.conn.execute("PRAGMA foreign_key_check").fetchall()
        assert client.post("/api/redo").status_code == 200
        assert saved() == ([], [], [], [])


def test_redo_refuses_to_delete_newer_species_decisions(original_photo):
    app, db, pid, _ = original_photo
    client = app.test_client()
    region = _save(client, pid, _token(client, pid)).json["primary_detection_id"]
    db.predictions.add(region, "Fictional bird", .9, "synthetic", status="accepted")
    pred = db.conn.execute("SELECT id FROM predictions WHERE detection_id=?", (region,)).fetchone()[0]
    db.commit()
    assert client.put(f"/api/photos/{pid}/subjects/reviewer-box", json={**_edit(client, pid, region), "operation": "remove"}).status_code == 200
    assert client.post("/api/undo").status_code == 200
    db.predictions.set_review_status(pred, db.require_workspace_id(), "rejected")
    db.commit()
    before = dict(db.conn.execute("SELECT * FROM prediction_review WHERE prediction_id=?", (pred,)).fetchone())
    assert client.post("/api/redo").status_code == 409
    assert db.detections.row(region) is not None
    assert dict(db.conn.execute("SELECT * FROM prediction_review WHERE prediction_id=?", (pred,)).fetchone()) == before


def test_undo_refuses_reused_prediction_identity_without_partial_restoration(original_photo):
    app, db, pid, _ = original_photo
    client = app.test_client()
    token = _token(client, pid)
    region = _save(client, pid, token).json["primary_detection_id"]
    other = client.put(f"/api/photos/{pid}/subjects/reviewer-box", json={
        "box": {"x": .7, "y": .1, "w": .2, "h": .3}, "frame_token": token,
    }).json["primary_detection_id"]
    for species in ("Fictional old A", "Fictional old B"):
        db.predictions.add(region, species, .9, "synthetic", status="accepted")
    old_ids = [row[0] for row in db.conn.execute("SELECT id FROM predictions WHERE detection_id=? ORDER BY id", (region,))]
    pred = old_ids[-1]
    db.commit()
    assert client.put(f"/api/photos/{pid}/subjects/reviewer-box", json={**_edit(client, pid, region), "operation": "remove"}).status_code == 200
    db.conn.execute("INSERT INTO predictions(id,detection_id,classifier_model,species,confidence) VALUES (?,?,'synthetic','Fictional new bird',.8)", (pred, other))
    db.predictions.set_review_status(pred, db.require_workspace_id(), "rejected")
    db.commit()
    assert db.conn.execute("SELECT detection_id FROM predictions WHERE id=?", (pred,)).fetchone()[0] == other
    assert client.post("/api/undo").status_code == 409
    assert db.detections.row(region) is None
    assert db.conn.execute("SELECT detection_id FROM predictions WHERE id=?", (pred,)).fetchone()[0] == other
    assert not db.conn.execute("SELECT 1 FROM predictions WHERE id=?", (old_ids[0],)).fetchone()
    assert not db.conn.execute("PRAGMA foreign_key_check").fetchall()


def test_failed_history_record_rolls_back_prediction_cascade(original_photo, monkeypatch):
    from services.subject_selections import set_selection

    app, db, pid, _ = original_photo
    client = app.test_client()
    region = _save(client, pid, _token(client, pid)).json["primary_detection_id"]
    db.predictions.add(region, "Fictional bird", .9, "synthetic", status="accepted")
    db.commit()
    before = db.predictions.region_removal_state(region)
    def fail(*args, **kwargs):
        raise ValueError("fictional history failure")
    monkeypatch.setattr(db, "record_edit", fail)
    with pytest.raises(ValueError, match="history failure"):
        set_selection(db, pid, {**_edit(client, pid, region), "operation": "remove"}, reviewer_box=True)
    assert db.detections.row(region) is not None
    assert db.predictions.region_removal_state(region) == before


def test_undo_of_older_addition_refuses_to_cascade_newer_decisions(original_photo):
    app, db, pid, _ = original_photo
    client = app.test_client()
    region = _save(client, pid, _token(client, pid)).json["primary_detection_id"]
    db.predictions.add(region, "Fictional bird", .9, "synthetic", status="accepted")
    db.commit()
    before = db.predictions.region_removal_state(region)
    assert client.post("/api/undo").status_code == 409
    assert db.detections.row(region) is not None
    assert db.predictions.region_removal_state(region) == before
