"""Saved quality suggestions converge on live evidence without deciding for users."""

import json
from copy import deepcopy
from pathlib import Path

import config as cfg
import pytest
from app import create_app
from pipeline import load_results, load_results_raw, save_results_raw
from repositories.masks_features import MaskQuality, PipelineFeatures
from services import photo_ingest, workspace_folders
from subject_readiness import REQUIRED_QUALITY_FIELDS, SCORE_FIELDS
from test_subject_readiness import complete_photo


@pytest.fixture(autouse=True)
def isolated_config(tmp_path, monkeypatch):
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    cfg.save({"pipeline": {"sam2_variant": "sam2-small", "weak_detection_rescue_enabled": False}})


def _photo(db, tmp_path, name="duck.jpg"):
    folder = workspace_folders.add_folder(db, str(tmp_path / name), name=name)
    return photo_ingest.add_photo(db, folder, name, ".jpg", 100, 1.)


def _analyze(db, tmp_path, pid, **changes):
    mask = tmp_path / f"{pid}-mask.png"
    mask.write_bytes(b"scratch-mask")
    quality = {key: complete_photo()[key] for key in REQUIRED_QUALITY_FIELDS}
    quality.update(changes)
    db.masks_features.update_pipeline_features(pid, PipelineFeatures(
        mask_path=str(mask), **{k: v for k, v in quality.items() if k != "subject_size"},
    ))
    db.commit()
    db.photos.update_quality(pid, subject_size=quality["subject_size"])
    db.detections.save(pid, [{"box": {"x": .1, "y": .1, "w": .4, "h": .4},
                              "confidence": .9, "category": "animal"}], detector_model="megadetector-v6")
    db.model_runs.record_detector_run(pid, "megadetector-v6", 1)
    db.commit()
    db.masks_features.upsert_mask(pid, "sam2-small", str(mask), "megadetector-v6",
                                 (.1, .1, .4, .4), MaskQuality(**quality))
    db.masks_features.set_active_variant(pid, "sam2-small", .2)
    db.commit()


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
    from services.photo_review import PhotoReviewService

    ids = [_photo(db, tmp_path, f"duck{i}.jpg") for i in range(4)]
    old = _old_review(ids)
    save_results_raw(old, str(tmp_path), db.active_workspace_id)
    PhotoReviewService(db).set_flag(ids[1], "flagged")
    PhotoReviewService(db).set_flag(ids[2], "rejected")
    db.photo_review.set_rating(ids[1], 5)
    db.commit()
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
    for group, original in zip(result["encounters"], old["encounters"], strict=True):
        assert {key: group[key] for key in original if key != "bursts"} == {
            key: value for key, value in original.items() if key != "bursts"}
        assert [{key: burst[key] for key in original_burst} for burst, original_burst in
                zip(group["bursts"], original["bursts"], strict=True)] == original["bursts"]
    assert result["summary"]["keep_count"] == 1 + (rows[ids[3]]["label"] == "KEEP")
    assert result["summary"]["reject_count"] == 1
    assert result["summary"]["review_count"] == 1 + (rows[ids[3]]["label"] == "REVIEW")
    assert list(db.conn.iterdump()) == before
    persisted = (tmp_path / f"pipeline_results_ws{db.active_workspace_id}.json").read_bytes()
    assert load_results(str(tmp_path), db.active_workspace_id, db=db) == result
    assert (tmp_path / f"pipeline_results_ws{db.active_workspace_id}.json").read_bytes() == persisted
    assert list(db.conn.iterdump()) == before
    # A later confirmed clear is authoritative too.
    PhotoReviewService(db).set_flag(ids[1], "none")
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
    db.commit()
    changed = load_results(str(tmp_path), db.active_workspace_id, db=db)
    assert changed["photos"][0]["label"] == "REVIEW"
    assert changed["photos"][0]["analysis_reasons"] == ["segmentation_stale"]
    # Reverting settings recovers the complete current input, then source changes invalidate it.
    db.workspaces.update(db.active_workspace_id, config_overrides=None)
    db.commit()
    assert load_results(str(tmp_path), db.active_workspace_id, db=db)["photos"][0]["analysis_status"] == "complete"
    from computation_cache import source_input
    db.model_runs.record_detector_run(ids[0], "megadetector-v6", 1, "r", source_input(
        "a" * 64, "vireo-detector-source-v1")[1])
    db.commit()
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
    folder = workspace_folders.add_folder(db, str(tmp_path / "ducks"), name="Ducks")
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
        assert rows[216716]["label"] == "REVIEW" and rows[216716]["flag"] == "flagged"
        assert rows[216716]["flag_confirmation_status"] == "unverified"
        assert result["summary"]["keep_count"] == 0
        assert result["summary"]["review_count"] == 71
        assert result["summary"]["reject_count"] == 0
        assert client.get(endpoint, headers={"X-Vireo-Token": "recovery-test"}).get_json() == payload
    # Application startup can maintain its own bookkeeping, so compare photo decisions directly.
    assert [line for line in db.conn.iterdump() if line.startswith('INSERT INTO "photos"')] == [
        line for line in before if line.startswith('INSERT INTO "photos"')]


def _classification(db, pid, species, confidence=.9, fingerprint='current'):
    detections = db.detections.get(pid, 0, None)
    if not detections:
        db.detections.save(pid, [{'box': {'x': .1, 'y': .1, 'w': .4, 'h': .4},
                                  'confidence': .9, 'category': 'animal'}], detector_model='megadetector-v6')
        db.commit()
        detections = db.detections.get(pid, 0, None)
    db.predictions.add(detections[0]['id'], species, confidence, 'review-classifier',
                       labels_fingerprint=fingerprint)
    db.commit()
    return db.conn.execute('SELECT id FROM predictions WHERE detection_id=? AND species=? AND labels_fingerprint=?',
                           (detections[0]['id'], species, fingerprint)).fetchone()['id']


def test_reopening_rebuilds_species_aggregates_after_classifier_changes(db, tmp_path):
    ids = [_photo(db, tmp_path, f'duck{i}.jpg') for i in range(2)]
    for pid in ids:
        _classification(db, pid, 'Old Species', .99, 'old-labels')
    saved = _old_review(ids)
    saved['encounters'][0]['bursts'][0]['species_override'] = {'species': 'Old Species', 'confirmed': False}
    save_results_raw(saved, str(tmp_path), db.active_workspace_id)
    initial = load_results(str(tmp_path), db.active_workspace_id, db=db)
    assert initial['encounters'][0]['species'] == ['Old Species', .99]
    for pid in ids:
        _classification(db, pid, 'Current Species', .85, 'new-labels')
    db.commit()
    before = list(db.conn.iterdump())
    refreshed = load_results(str(tmp_path), db.active_workspace_id, db=db)
    encounter = refreshed['encounters'][0]
    assert encounter['species'] == ['Current Species', .85]
    assert encounter['species_predictions'] == [{
        'species': 'Current Species', 'species_key': 'name:current species', 'count': 2,
        'avg_confidence': .85, 'models': [{'model': 'review-classifier', 'confidence': .85, 'photo_count': 2}],
    }]
    burst = encounter['bursts'][0]
    assert burst['species_predictions'] == encounter['species_predictions']
    assert burst['species_override'] == {'species': 'Current Species', 'confirmed': False}
    assert encounter['confirmed_species'] == 'Mandarin Duck' and encounter['species_confirmed']
    assert list(db.conn.iterdump()) == before
    persisted = (tmp_path / f'pipeline_results_ws{db.active_workspace_id}.json').read_bytes()
    assert load_results(str(tmp_path), db.active_workspace_id, db=db) == refreshed
    assert (tmp_path / f'pipeline_results_ws{db.active_workspace_id}.json').read_bytes() == persisted


def test_species_aggregates_clear_when_current_evidence_disappears(db, tmp_path):
    pid = _photo(db, tmp_path)
    prediction = _classification(db, pid, 'Old Species')
    saved = _old_review([pid])
    burst = saved['encounters'][0]['bursts'][0]
    burst['species'] = ['Old Species', .9]
    burst['species_override'] = {'species': 'Old Species', 'confirmed': False}
    save_results_raw(saved, str(tmp_path), db.active_workspace_id)
    assert load_results(str(tmp_path), db.active_workspace_id, db=db)['encounters'][0]['species'][0] == 'Old Species'
    db.conn.execute('DELETE FROM predictions WHERE id=?', (prediction,))
    db.commit()
    refreshed = load_results(str(tmp_path), db.active_workspace_id, db=db)
    encounter = refreshed['encounters'][0]
    assert refreshed['photos'][0]['species_top5'] == []
    assert encounter['species'] == [None, 0.] and encounter['species_predictions'] == []
    burst = encounter['bursts'][0]
    assert burst['species'] == [None, 0.] and burst['species_predictions'] == []
    assert burst['species_override'] is None
    assert encounter['confirmed_species'] == 'Mandarin Duck' and encounter['species_confirmed']


@pytest.mark.parametrize('override', [
    {'species': 'Confirmed Bird', 'confirmed': True},
    {'species': 'Confirmed Bird', 'confirmed': True, 'species_list': ['Confirmed Bird', 'Other Bird']},
    {'species': 'Edited Bird', 'confirmed': False, 'species_list': ['Edited Bird']},
    {'species': None, 'confirmed': False, 'species_list': []},
])
def test_species_refresh_preserves_authoritative_burst_edits(db, tmp_path, override):
    pid = _photo(db, tmp_path)
    _classification(db, pid, 'Current Species', .8)
    saved = _old_review([pid])
    saved['encounters'][0]['confirmed_species_list'] = ['Mandarin Duck', 'Saved Bird']
    saved['encounters'][0]['bursts'][0]['species_override'] = deepcopy(override)
    save_results_raw(saved, str(tmp_path), db.active_workspace_id)
    refreshed = load_results(str(tmp_path), db.active_workspace_id, db=db)
    encounter = refreshed['encounters'][0]
    assert encounter['bursts'][0]['species_override'] == override
    assert encounter['confirmed_species_list'] == ['Mandarin Duck', 'Saved Bird']
    assert encounter['confirmed_species'] == 'Mandarin Duck' and encounter['species_confirmed']
    assert encounter['species'] == ['Current Species', .8]
    assert encounter['bursts'][0]['species_predictions'][0]['species'] == 'Current Species'


def test_species_refresh_uses_surviving_members_and_retains_raw_bursts(db, tmp_path):
    ids = [_photo(db, tmp_path, f'bird{i}.jpg') for i in range(3)]
    for pid, name, confidence in zip(ids, ['First Bird', 'Second Bird', 'Excluded Bird'], [.7, .9, .99], strict=True):
        _classification(db, pid, name, confidence)
    saved = _old_review(ids[:2])
    for photo in saved['photos']:
        photo['grouping_species_top5'] = [['Stale Grouping Hint', .99, 'old-classifier']]
    encounter = saved['encounters'][0]
    encounter.update(photo_ids=ids, photo_count=3, burst_count=2,
                     grouping_edit={'detached_from': 7}, time_range=['saved-start', 'saved-end'])
    encounter['bursts'] = [
        {'photo_ids': [ids[0], ids[2]], 'species_predictions': [{'species': 'Old Bird'}],
         'species_override': {'species': 'Old Bird', 'confirmed': False}, 'grouping_edit': 'detached'},
        [ids[1], ids[2]],
    ]
    save_results_raw(saved, str(tmp_path), db.active_workspace_id)
    before = list(db.conn.iterdump())
    refreshed = load_results(str(tmp_path), db.active_workspace_id, db=db)
    encounter = refreshed['encounters'][0]
    assert encounter['photo_ids'] == ids[:2] and encounter['photo_count'] == 2
    assert encounter['burst_count'] == 2
    assert encounter['grouping_edit'] == {'detached_from': 7}
    assert encounter['time_range'] == ['saved-start', 'saved-end']
    assert encounter['species'] == ['Second Bird', .45]
    assert {p['species']: (p['count'], p['avg_confidence']) for p in encounter['species_predictions']} == {
        'First Bird': (1, .7), 'Second Bird': (1, .9),
    }
    burst = encounter['bursts'][0]
    assert burst['photo_ids'] == [ids[0]] and burst['grouping_edit'] == 'detached'
    assert burst['species_predictions'][0]['species'] == 'First Bird'
    assert burst['species_predictions'][0]['count'] == 1
    assert burst['species_override'] == {'species': 'First Bird', 'confirmed': False}
    assert encounter['bursts'][1] == [ids[1]]
    assert all(photo['review_confirmed'] for photo in refreshed['photos'])
    assert list(db.conn.iterdump()) == before


def _review_with_detachable_bursts(db, tmp_path, ids, override):
    saved = _old_review(ids)
    saved['encounters'][0]['burst_count'] = 2
    saved['encounters'][0]['bursts'] = [
        {'photo_ids': ids[:2], 'species_override': deepcopy(override)},
        {'photo_ids': ids[2:], 'species_override': {'species': 'Old Species', 'confirmed': False}},
    ]
    for pid in ids:
        _classification(db, pid, 'Old Species', .9, 'old-labels')
    save_results_raw(saved, str(tmp_path), db.active_workspace_id)
    return load_results(str(tmp_path), db.active_workspace_id, db=db)


def _group_membership(results):
    return [(encounter['photo_ids'], [burst['photo_ids'] if isinstance(burst, dict) else burst
                                    for burst in encounter['bursts']]) for encounter in results['encounters']]


@pytest.mark.parametrize('override', [
    {'species': 'Edited Bird', 'confirmed': False, 'species_list': ['Edited Bird']},
    {'species': None, 'confirmed': False, 'species_list': []},
])
def test_classifier_refresh_preserves_real_grouping_undo_redo(app_and_db, tmp_path, override):
    app, db = app_and_db
    ids = [_photo(db, tmp_path, f'history{i}.jpg') for i in range(3)]
    original = _review_with_detachable_bursts(db, tmp_path, ids, override)
    client = app.test_client()
    detach = client.post('/api/pipeline/detach-burst', json={'encounter_index': 0, 'burst_index': 0})
    assert detach.status_code == 200, detach.get_json()
    detached = load_results_raw(str(tmp_path), db.active_workspace_id)
    history_id = client.get('/api/undo/status').get_json()['id']
    for pid in ids:
        _classification(db, pid, 'Current Species', .8, 'new-labels')
    refreshed = load_results(str(tmp_path), db.active_workspace_id, db=db)
    assert all(encounter['species'] == ['Current Species', .8] for encounter in refreshed['encounters'])
    assert _group_membership(refreshed) == _group_membership(detached)
    decisions = [(db.photos.get(pid)['flag'], db.photos.get(pid)['rating']) for pid in ids]
    undo = client.post('/api/undo')
    assert undo.status_code == 200, undo.get_json()
    assert undo.get_json()['entry_id'] == history_id
    restored = load_results(str(tmp_path), db.active_workspace_id, db=db)
    assert _group_membership(restored) == _group_membership(original)
    assert restored['encounters'][0]['species'] == ['Current Species', .8]
    assert restored['encounters'][0]['bursts'][0]['species_override'] == override
    redo = client.post('/api/redo')
    assert redo.status_code == 200, redo.get_json()
    assert redo.get_json()['entry_id'] == history_id
    replayed = load_results(str(tmp_path), db.active_workspace_id, db=db)
    assert _group_membership(replayed) == _group_membership(detached)
    burst = next(burst for encounter in replayed['encounters'] for burst in encounter['bursts']
                 if burst['photo_ids'] == ids[:2])
    assert burst['species_override'] == override
    assert all(encounter['species'] == ['Current Species', .8] for encounter in replayed['encounters'])
    assert [(db.photos.get(pid)['flag'], db.photos.get(pid)['rating']) for pid in ids] == decisions


@pytest.mark.parametrize('newer_edit', ['membership', 'species_list'])
def test_classifier_refresh_does_not_hide_actual_newer_grouping_edits(app_and_db, tmp_path, newer_edit):
    app, db = app_and_db
    ids = [_photo(db, tmp_path, f'history{i}.jpg') for i in range(3)]
    _review_with_detachable_bursts(db, tmp_path, ids, {'species': 'Edited Bird', 'confirmed': False,
                                                   'species_list': ['Edited Bird']})
    client = app.test_client()
    assert client.post('/api/pipeline/detach-burst', json={'encounter_index': 0, 'burst_index': 0}).status_code == 200
    for pid in ids:
        _classification(db, pid, 'Current Species', .8, 'new-labels')
    newer = load_results(str(tmp_path), db.active_workspace_id, db=db)
    detached = next(encounter for encounter in newer['encounters'] if encounter['photo_ids'] == ids[:2])
    if newer_edit == 'membership':
        detached['bursts'] = [{'photo_ids': [pid]} for pid in ids[:2]]
        detached['burst_count'] = 2
    else:
        detached['bursts'][0]['species_override'] = {'species': None, 'confirmed': False, 'species_list': []}
    save_results_raw(newer, str(tmp_path), db.active_workspace_id)
    response = client.post('/api/undo')
    assert response.status_code == 409, response.get_json()
    assert load_results_raw(str(tmp_path), db.active_workspace_id)['encounters'] == newer['encounters']
    assert not client.get('/api/undo/status').get_json()['available']


@pytest.mark.parametrize('override', [
    {'species': 'Edited Bird', 'confirmed': True},
    {'species': 'Edited Bird', 'confirmed': False, 'species_list': ['Edited Bird']},
    {'species': None, 'confirmed': False, 'species_list': []},
])
def test_classifier_refresh_preserves_burst_label_undo_redo(app_and_db, tmp_path, override):
    app, db = app_and_db
    ids = [_photo(db, tmp_path, f'label-history{i}.jpg') for i in range(3)]
    _review_with_detachable_bursts(db, tmp_path, ids, override)
    client = app.test_client()
    clear = client.post('/api/pipeline/save-cache', json={'clear_override': {
        'encounter_photo_ids': ids, 'burst_photo_ids': ids[:2], 'expected_override': override,
    }})
    assert clear.status_code == 200, clear.get_json()
    for pid in ids:
        _classification(db, pid, 'Current Species', .8, 'new-labels')
    refreshed = load_results(str(tmp_path), db.active_workspace_id, db=db)
    assert refreshed['encounters'][0]['bursts'][0]['species_override'] is None
    undo = client.post('/api/undo')
    assert undo.status_code == 200, undo.get_json()
    restored = load_results(str(tmp_path), db.active_workspace_id, db=db)
    assert restored['encounters'][0]['bursts'][0]['species_override'] == override
    assert restored['encounters'][0]['species'] == ['Current Species', .8]
    redo = client.post('/api/redo')
    assert redo.status_code == 200, redo.get_json()
    replayed = load_results(str(tmp_path), db.active_workspace_id, db=db)
    assert replayed['encounters'][0]['bursts'][0]['species_override'] is None
    assert replayed['encounters'][0]['species'] == ['Current Species', .8]
