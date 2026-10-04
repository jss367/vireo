"""Database loading, evidence preservation, and scope guards for continuity."""

from datetime import datetime, timedelta

import pytest


@pytest.fixture
def sequence_db(tmp_path, monkeypatch):
    import config
    from db import Database

    monkeypatch.setattr(config, "CONFIG_PATH", str(tmp_path / "config.json"))
    db = Database(str(tmp_path / "photos.db"))
    folder = db.add_folder(str(tmp_path / "photos"))
    ids = []
    for i in range(3):
        ids.append(
            db.add_photo(
                folder,
                f"{i}.jpg",
                ".jpg",
                100,
                1,
                timestamp=(datetime(2026, 1, 1) + timedelta(seconds=i * 0.1)).isoformat(),
            )
        )
    yield db, ids
    db.close()


def classify(db, pid, confidence, species="Bluebird", score=0.99, model="megadetector-v6", x=0.2):
    did = db.write_detection_batch(
        pid,
        model,
        [
            {
                "box": {"x": x, "y": 0.2, "w": 0.3, "h": 0.3},
                "confidence": confidence,
                "category": "animal",
            }
        ],
    )[0]
    db.add_prediction(did, species, score, "classifier")
    return did


def test_weak_crop_recovers_without_full_image_or_keyword_labels(sequence_db):
    from pipeline import load_photo_features, run_grouping

    db, ids = sequence_db
    for i, pid in enumerate(ids):
        classify(db, pid, 0.06 if i == 1 else 0.8)
    original = db.conn.execute("SELECT * FROM predictions ORDER BY id").fetchall()
    photos = load_photo_features(db, effective_config={})
    middle = photos[1]
    assert middle["subject_uncertain"] and not middle["subject_absent"]
    assert middle["weak_detection_context"]["evidence"] == "crop_sequence"
    assert middle["subjects"] == []  # does not invent a normal-confidence subject
    assert len(run_grouping(photos)) == 1
    assert db.conn.execute("SELECT * FROM predictions ORDER BY id").fetchall() == original
    assert all(not db.get_photo_keywords(pid) for pid in ids)
    disabled = load_photo_features(db, effective_config={"pipeline": {"weak_detection_rescue_enabled": False}})
    assert disabled[1]["subject_absent"]


def test_isolated_species_abstention_preserves_subject_prediction_and_trace(sequence_db):
    from pipeline import load_photo_features, rebuild_species_predictions, run_grouping, run_triage, serialize_results

    db, ids = sequence_db
    for i, pid in enumerate(ids):
        classify(db, pid, 0.7, "Chickadee" if i == 1 else "Nuthatch", 0.828 if i == 1 else 0.99)
    photos = load_photo_features(db, effective_config={})
    assert photos[1]["species_top5"][0][0] == "Chickadee"
    assert photos[1]["grouping_species_top5"] == []
    assert photos[1]["subjects"][0]["predictions"][0][0] == "Chickadee"
    assert photos[1]["isolated_species_context"]["original_species_top5"][0][0] == "Chickadee"
    groups = run_grouping(photos, emit_trace=True)
    assert len(groups) == 1 and groups[0]["species"][0] == "Nuthatch"
    assert any(t["decision"] == "kept_species_continuity" for t in groups[0]["trace"])
    groups, triaged = run_triage(groups)
    assert triaged[1]["species_top5"][0][0] == "Chickadee"
    saved = serialize_results({"photos": triaged, "encounters": groups, "summary": {}})
    assert saved["photos"][1]["isolated_species_context"] == photos[1]["isolated_species_context"]
    assert saved["photos"][1]["species_top5"][0][0] == "Chickadee"
    predictions = rebuild_species_predictions(saved, ids)
    assert any(p["species"] == "Chickadee" for p in predictions)
    assert all(not db.get_photo_keywords(pid) for pid in ids)


def test_additional_evidence_respects_latest_and_pinned_label_sets(sequence_db):
    from pipeline import load_photo_features

    db, ids = sequence_db
    detections = [classify(db, pid, 0.06 if i == 1 else 0.8) for i, pid in enumerate(ids)]
    db.add_prediction(detections[1], "Other bird", 0.99, "classifier", labels_fingerprint="new")
    assert load_photo_features(db, effective_config={})[1]["subject_absent"]
    assert load_photo_features(db, effective_config={}, labels_fingerprint="legacy")[1]["subject_uncertain"]


@pytest.mark.parametrize("top_k", [1, 5])
def test_hidden_agreeing_crop_model_vetoes_isolated_repair(sequence_db, top_k):
    from pipeline import load_photo_features, run_grouping

    db, ids = sequence_db
    for i, pid in enumerate(ids):
        did = classify(db, pid, 0.7, "Chickadee" if i == 1 else "Nuthatch", 0.828 if i == 1 else 0.99)
        if i == 1:
            db.add_prediction(did, "Chickadee", 0.827, "independent-classifier")
    photos = load_photo_features(db, config={"top_k_predictions": top_k}, effective_config={})
    assert "isolated_species_context" not in photos[1]
    assert photos[1]["species_top5"][0][0] == "Chickadee"
    assert len(run_grouping(photos)) == 3


def test_partial_scope_never_borrows_an_out_of_scope_anchor(sequence_db):
    from pipeline import load_photo_features

    db, ids = sequence_db
    for i, pid in enumerate(ids):
        classify(db, pid, 0.06 if i == 1 else 0.8)
    photos = load_photo_features(db, effective_config={}, photo_ids=ids[1:])
    assert len(photos) == 2 and photos[0]["subject_absent"]


def test_unrelated_workspace_does_not_supply_a_matching_anchor(sequence_db):
    from pipeline import load_photo_features

    db, ids = sequence_db
    for i, pid in enumerate(ids):
        classify(db, pid, 0.06 if i == 1 else 0.8)
    # A requested ID from outside this workspace must not widen the query.
    db.conn.execute(
        "DELETE FROM workspace_folders WHERE folder_id=(SELECT folder_id FROM photos WHERE id=?)", (ids[0],)
    )
    db.conn.commit()
    assert load_photo_features(db, effective_config={}, photo_ids=ids) == []


@pytest.mark.parametrize("source", ["secondary", "full_image"])
def test_conflicting_independent_evidence_vetoes_new_weak_crop_rescue(sequence_db, source):
    from pipeline import load_photo_features

    db, ids = sequence_db
    for i, pid in enumerate(ids):
        classify(db, pid, 0.06 if i == 1 else 0.8)
    if source == "full_image":
        classify(db, ids[1], 0, "Other bird", 0.99, "full-image")
    else:
        did, second = db.write_detection_batch(
            ids[1],
            "megadetector-v6",
            [
                {"box": {"x": 0.2, "y": 0.2, "w": 0.3, "h": 0.3}, "confidence": 0.06, "category": "animal"},
                {"box": {"x": 0.7, "y": 0.2, "w": 0.1, "h": 0.1}, "confidence": 0.02, "category": "animal"},
            ],
        )
        db.add_prediction(did, "Bluebird", 0.99, "classifier")
        db.add_prediction(second, "Other bird", 0.99, "classifier")
    assert load_photo_features(db, effective_config={})[1]["subject_absent"]


def test_default_long_context_loads_real_evidence_and_preserves_tags(sequence_db):
    from pipeline import load_photo_features, run_grouping, serialize_results

    db, ids = sequence_db
    # Five seconds is beyond the old three-second evidence preselection.
    db.conn.execute("UPDATE photos SET timestamp=? WHERE id=?", ("2026-01-01T00:00:05", ids[-1]))
    db.conn.commit()
    for i, pid in enumerate(ids):
        classify(db, pid, 0.06 if i == 1 else 0.8, score=0.4 if i == 1 else 0.99)
    original = db.conn.execute("SELECT * FROM predictions ORDER BY id").fetchall()
    photos = load_photo_features(db, effective_config={})
    middle = photos[1]
    assert middle["subject_uncertain"] and not middle["subject_absent"]
    assert middle["weak_detection_context"]["support"] == "anchor_context"
    assert middle["grouping_species_top5"] == []
    groups = run_grouping(photos, emit_trace=True)
    assert len(groups) == 1 and groups[0]["species"][0] == "Bluebird"
    saved = serialize_results({"photos": photos, "encounters": groups, "summary": {}})
    assert saved["photos"][1]["weak_detection_context"] == middle["weak_detection_context"]
    assert db.conn.execute("SELECT * FROM predictions ORDER BY id").fetchall() == original
    assert all(not db.get_photo_keywords(pid) for pid in ids)
    assert load_photo_features(db, effective_config={}, photo_ids=ids[1:])[0]["subject_absent"]
