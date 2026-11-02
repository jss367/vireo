"""Regression coverage for the remaining current-prediction ranking surfaces."""

import pytest
from services import photo_ingest, review_decisions, workspace_folders


@pytest.fixture
def catalog(db, tmp_path, monkeypatch):
    import config as cfg

    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    # A deliberately different global floor catches readers ignoring workspace config.
    cfg.save({"detector_confidence": 0.95})
    db.workspaces.update(db.active_workspace_id, config_overrides={"detector_confidence": 0.5})
    db.commit()
    folder = workspace_folders.add_folder(db, str(tmp_path / "photos"), name="photos")
    pid = photo_ingest.add_photo(db, folder, "bird.jpg", ".jpg", 100, 1.0)
    return pid, folder


def _prediction(db, pid, species, score, *, detection=None, floor=0.8,
                model="classifier", fingerprint="new", detector="md", created="2026-01-01"):
    if detection is None:
        detection = db.conn.execute(
            "INSERT INTO detections (photo_id, detector_model, detector_confidence) VALUES (?, ?, ?)",
            (pid, detector, floor),
        ).lastrowid
    prediction = db.conn.execute(
        "INSERT INTO predictions (detection_id, classifier_model, labels_fingerprint, "
        "species, confidence, created_at) VALUES (?, ?, ?, ?, ?, ?)",
        (detection, model, fingerprint, species, score, created),
    ).lastrowid
    db.commit()
    return detection, prediction


def _ranked(db, pid):
    """Read each production ranking that the sweep changed."""
    cull = db.culling.top_prediction(pid)
    highlight = db.species_curation.get_highlights_candidates(None, photo_id=pid)[0]
    confirm = db.predictions.top_unrejected_by_photo([pid]).get(pid)
    return (
        cull["species"] if cull else None,
        highlight["predicted_species"],
        confirm["species"] if confirm else None,
    )


def test_rankings_use_latest_fingerprint_per_detection_and_model(db, catalog):
    pid, _ = catalog
    detection, _ = _prediction(db, pid, "Old", 0.99, fingerprint="old", created="2026-02-01")
    # Equal timestamps must choose the greater prediction id, regardless of score.
    _, current = _prediction(db, pid, "Current", 0.6, detection=detection, created="2026-02-01")
    _prediction(db, pid, "Hidden", 1.0, floor=0.1)
    assert _ranked(db, pid) == ("Current",) * 3
    assert db.predictions.top_unrejected_by_photo([pid])[pid]["id"] == current
    # Fingerprint selection is per classifier, not just per detection.
    _prediction(db, pid, "Other model", 0.7, detection=detection, model="other", fingerprint="other")
    assert _ranked(db, pid) == ("Other model",) * 3
    # created_at precedes id: a later-inserted old timestamp cannot replace the current set.
    _prediction(db, pid, "Old timestamp", 1.0, detection=detection, fingerprint="older", created="2025-01-01")
    assert _ranked(db, pid) == ("Other model",) * 3


def test_threshold_changes_and_full_image_anchors(db, catalog):
    pid, _ = catalog
    _prediction(db, pid, "Low", 0.99, floor=0.3)
    _prediction(db, pid, "Visible", 0.6, floor=0.8)
    assert _ranked(db, pid) == ("Visible",) * 3
    db.workspaces.update(db.active_workspace_id, config_overrides={"detector_confidence": 0.2})
    db.commit()
    assert _ranked(db, pid) == ("Low",) * 3
    db.workspaces.update(db.active_workspace_id, config_overrides={"detector_confidence": 0.9})
    db.commit()
    assert _ranked(db, pid) == (None,) * 3
    _prediction(db, pid, "Full image", 0.5, floor=0, detector="full-image")
    assert _ranked(db, pid) == ("Full image",) * 3
    db.species_curation.add_highlight("Full image", pid)
    assert db.species_curation.get_highlights(eligible_only=True) == {"Full image": {pid: 1}}
    # A fresh workspace with no override uses the documented default 0.2.
    import config as cfg
    cfg.save({})
    db.workspaces.update(db.active_workspace_id, config_overrides={})
    db.commit()
    assert _ranked(db, pid) == ("Low",) * 3


def test_highlight_eligibility_tracks_ranking_but_keeps_saved_choices_and_keywords(db, catalog):
    pid, _ = catalog
    detection, _ = _prediction(db, pid, "Old", 0.99, fingerprint="old")
    _, current = _prediction(db, pid, "Current", 0.6, detection=detection)
    _prediction(db, pid, "Hidden", 1.0, floor=0.1)
    db.species_curation.add_highlight("Current", pid)
    assert db.species_curation.get_highlights(eligible_only=True) == {"Current": {pid: 1}}
    assert db.species_curation.get_highlights("Current", eligible_only=True) == {"Current": {pid: 1}}
    # A rejection in another workspace cannot change this workspace's ranking.
    other = db.create_workspace("Other", config_overrides={"detector_confidence": 0.9})
    db.commit()
    review_decisions.set_review_status(db, current, other, "rejected")
    assert _ranked(db, pid) == ("Current",) * 3
    db.workspaces.update(db.active_workspace_id, config_overrides={"detector_confidence": 0.9})
    db.commit()
    assert db.species_curation.get_highlights(eligible_only=True) == {}
    assert db.species_curation.get_highlights() == {"Current": {pid: 1}}
    # A user's accepted keyword remains eligible even when all detector evidence is hidden.
    keyword = db.add_keyword("Current", is_species=True)
    db.tag_photo(pid, keyword, source="manual")
    db.commit()
    assert db.species_curation.get_highlights(eligible_only=True) == {"Current": {pid: 1}}


def test_misses_override_uses_current_fingerprints_but_preserves_weak_context(db, catalog):
    pid, _ = catalog
    detection, _ = _prediction(db, pid, "Old", 0.99, fingerprint="old", floor=0.1)
    _prediction(db, pid, "Current", 0.4, detection=detection)
    def row(workspace):
        return dict(db.detections.workspace_miss_rows(workspace, 0.5)[0])
    assert row(db.active_workspace_id)["detection_conf"] is None
    assert row(db.active_workspace_id)["max_prediction_conf"] == 0.4
    # Deliberate below-detector-floor classifier rescue stays available.
    _prediction(db, pid, "Strong current", 0.9, detection=detection)
    assert row(db.active_workspace_id)["max_prediction_conf"] == 0.9
    _prediction(db, pid, "Full image", 0.95, floor=0, detector="full-image")
    assert row(db.active_workspace_id)["max_prediction_conf"] == 0.95
    other = db.create_workspace("Other")
    db.commit()
    assert db.detections.workspace_miss_rows(other, 0.5) == []


def test_highlight_candidates_remain_workspace_scoped(db, catalog):
    pid, _ = catalog
    _prediction(db, pid, "Current", 0.8)
    other = db.create_workspace("Other")
    db.commit()
    db.set_active_workspace(other)
    assert db.species_curation.get_highlights_candidates(None, photo_id=pid) == []


def test_highlights_confirmation_uses_the_same_current_visible_prediction(app_and_db):
    app, db = app_and_db
    pid = db.conn.execute("SELECT id FROM photos ORDER BY id LIMIT 1").fetchone()[0]
    db.workspaces.update(db.active_workspace_id, config_overrides={"detector_confidence": 0.5})
    db.commit()
    detection, old = _prediction(db, pid, "Old bird", 0.99, fingerprint="old")
    _, current = _prediction(db, pid, "Current bird", 0.7, detection=detection)
    _, hidden = _prediction(db, pid, "Hidden bird", 1.0, floor=0.1)
    response = app.test_client().post("/api/highlights/confirm", json={"photo_ids": [pid]})
    assert response.status_code == 200
    assert db.predictions.get_review_status(current, db.active_workspace_id) == "accepted"
    assert db.predictions.get_review_status(old, db.active_workspace_id) == "pending"
    assert db.predictions.get_review_status(hidden, db.active_workspace_id) == "pending"
    assert "Current bird" in {k["name"] for k in db.photo_keywords.get_for_photo(pid)}
