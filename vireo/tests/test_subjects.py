import numpy as np
import pytest
from PIL import Image
from subjects import analyze_photo, payload, select_primary


@pytest.fixture
def subject_photo(db, tmp_path):
    folder = db.add_folder(str(tmp_path), name="Subjects")
    path = tmp_path / "pair.jpg"
    pixels = np.full((160, 320, 3), 90, dtype=np.uint8)
    # A sharp, lower-confidence animal on the right; blurry animal on left.
    pixels[:, 160:] = np.where((np.indices((160, 160)).sum(axis=0) // 4 % 2)[..., None], 160, 60)
    Image.fromarray(pixels).save(path)
    photo_id = db.add_photo(folder_id=folder, filename=path.name, extension=".jpg", file_size=path.stat().st_size, file_mtime=1)
    ids = db.write_detection_batch(photo_id, "megadetector-v6", [
        {"box": {"x": .05, "y": .1, "w": .35, "h": .8}, "confidence": .95, "category": "animal"},
        {"box": {"x": .6, "y": .1, "w": .35, "h": .8}, "confidence": .7, "category": "animal"},
        {"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": .05, "category": "animal"},
        {"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": .99, "category": "person"},
    ])
    return photo_id, ids, path


def test_analyzes_every_retained_subject_and_selects_quality(db, subject_photo):
    photo_id, ids, path = subject_photo
    assert analyze_photo(db, photo_id, path) == 2
    result = payload(db, photo_id)
    assert len(result["subjects"]) == 2
    assert result["primary_detection_id"] == ids[1]
    assert db.get_detections(photo_id)[0]["id"] == ids[1]
    assert db.get_detections_for_photos([photo_id])[photo_id][0]["id"] == ids[1]
    for subject in result["subjects"]:
        analysis = subject["analysis"]
        assert 0 <= analysis["quality_score"] <= 1
        assert -2 <= analysis["exposure_ev"] <= 2
        assert analysis["crop"]["w"] > 0
    assert analyze_photo(db, photo_id, path) == 0


def test_manual_choice_survives_reprocessing_and_preserves_edits(db, subject_photo):
    photo_id, ids, path = subject_photo
    analyze_photo(db, photo_id, path)
    recipe = db.set_photo_edit_recipe(photo_id, {"crop": {"x": .2, "y": .2, "w": .5, "h": .5}, "adjustments": {"exposure": 1.1}})
    select_primary(db, photo_id, ids[0])
    analyze_photo(db, photo_id, path, force=True)
    assert payload(db, photo_id)["primary_detection_id"] == ids[0]
    assert db.get_photo_edit_recipe(photo_id) == recipe
    select_primary(db, photo_id, None)
    assert payload(db, photo_id)["primary_detection_id"] == ids[1]
    assert db.get_photo_edit_recipe(photo_id) == recipe


def test_selection_rejects_hidden_nonanimal_and_foreign_detections(db, subject_photo):
    photo_id, ids, path = subject_photo
    for value in [ids[2], ids[3], True, str(ids[0]), -1]:
        with pytest.raises(ValueError):
            select_primary(db, photo_id, value)
    assert payload(db, photo_id)["selection"] == "automatic"


def test_analysis_invalidated_by_source_change(db, subject_photo):
    photo_id, ids, path = subject_photo
    analyze_photo(db, photo_id, path)
    Image.new("RGB", (320, 160), (240, 240, 240)).save(path)
    assert analyze_photo(db, photo_id, path) == 2
    assert all(s["analysis"]["exposure_ev"] < 0 for s in payload(db, photo_id)["subjects"])


def test_switch_clears_other_subjects_features(db, subject_photo):
    photo_id, ids, path = subject_photo
    analyze_photo(db, photo_id, path)
    db.conn.execute("UPDATE photos SET mask_path='old.png', eye_x=.7, dino_subject_embedding=X'01' WHERE id=?", (photo_id,))
    select_primary(db, photo_id, ids[0])
    photo = db.conn.execute("SELECT * FROM photos WHERE id=?", (photo_id,)).fetchone()
    assert photo["mask_path"] is None
    assert photo["eye_x"] is None
    assert photo["dino_subject_embedding"] is None


def test_choice_remembered_when_detection_temporarily_disappears(db, subject_photo):
    photo_id, ids, path = subject_photo
    select_primary(db, photo_id, ids[0])
    original = dict(db.get_detections(photo_id)[0])
    db.clear_detections(photo_id)
    assert payload(db, photo_id)["choice_unavailable"]
    db.write_detection_batch(photo_id, original["detector_model"], [{
        "box": {k: original["box_" + k] for k in "xywh"}, "confidence": .95, "category": "animal"}])
    assert payload(db, photo_id)["primary_detection_id"] == ids[0]
    assert not payload(db, photo_id)["choice_unavailable"]


def test_subject_api_workspace_and_validation(app_and_db):
    app, db = app_and_db
    photo_id = db.get_photos()[0]["id"]
    ids = db.write_detection_batch(photo_id, "megadetector-v6", [{"box": {"x": .1, "y": .2, "w": .3, "h": .4}, "confidence": .9, "category": "animal"}])
    client = app.test_client()
    assert client.get(f"/api/photos/{photo_id}/subjects").get_json()["primary_detection_id"] == ids[0]
    response = client.put(f"/api/photos/{photo_id}/primary-subject", json={"detection_id": ids[0]})
    assert response.status_code == 200
    assert response.get_json()["selection"] == "manual"
    assert client.put(f"/api/photos/{photo_id}/primary-subject", json={}).status_code == 400
    assert client.put(f"/api/photos/{photo_id}/primary-subject", json={"detection_id": True}).status_code == 400
    assert client.get("/api/photos/999999/subjects").status_code == 404


def test_stale_masks_follow_selected_subject(db, subject_photo):
    photo_id, ids, path = subject_photo
    analyze_photo(db, photo_id, path)
    det = db.get_detections(photo_id)[0]
    db.conn.execute("""INSERT INTO photo_masks(photo_id,variant,path,created_at,detector_model,prompt_x,prompt_y,prompt_w,prompt_h)
        VALUES (?,'test','mask.png',1,?,?,?,?,?)""", (photo_id,det["detector_model"],*(det["box_" + k] for k in "xywh")))
    assert db.find_stale_masks() == []
    select_primary(db, photo_id, ids[0])
    assert len(db.find_stale_masks()) == 1
    assert db.count_extract_stale("test") == 0


def test_unanalyzed_choice_does_not_inherit_previous_subject_score(db, subject_photo):
    photo_id, ids, path = subject_photo
    analyze_photo(db, photo_id, path)
    db.conn.execute("DELETE FROM detection_subjects WHERE detection_id=?", (ids[0],))
    select_primary(db, photo_id, ids[0])
    photo = db.conn.execute("SELECT quality_score, subject_size FROM photos WHERE id=?", (photo_id,)).fetchone()
    assert photo['quality_score'] is None
    assert photo['subject_size'] is None


def test_offline_source_does_not_poison_analysis_cache(db, subject_photo):
    photo_id, ids, path = subject_photo
    path.unlink()
    with pytest.raises(OSError):
        analyze_photo(db, photo_id, path)
    assert all(s['analysis'] is None for s in payload(db, photo_id)['subjects'])


def test_cancelled_analysis_publishes_nothing(db, subject_photo):
    photo_id, ids, path = subject_photo
    from web.background_jobs import JobCancelled

    def cancel():
        raise JobCancelled('Cancelled')

    with pytest.raises(JobCancelled):
        analyze_photo(db, photo_id, path, checkpoint=cancel)
    assert all(s['analysis'] is None for s in payload(db, photo_id)['subjects'])


def test_purging_old_detection_projects_remaining_primary(db, subject_photo):
    photo_id, ids, path = subject_photo
    analyze_photo(db, photo_id, path)
    select_primary(db, photo_id, ids[1])
    db.delete_detections_by_ids([ids[1]])
    result = payload(db, photo_id)
    assert result['primary_detection_id'] == ids[0]
    assert result['choice_unavailable']
    score = db.conn.execute('SELECT quality_score FROM photos WHERE id=?', (photo_id,)).fetchone()[0]
    assert score == result['subjects'][0]['analysis']['quality_score']


def test_subject_routes_do_not_expose_other_workspaces(app_and_db):
    app, db = app_and_db
    photo_id = db.get_photos()[0]['id']
    client = app.test_client()
    other = db.create_workspace('Empty workspace')
    response = client.post(f'/api/workspaces/{other}/activate')
    assert response.status_code == 200
    assert client.get(f'/api/photos/{photo_id}/subjects').status_code == 404
    assert client.put(f'/api/photos/{photo_id}/primary-subject', json={'detection_id': None}).status_code == 404
    assert client.post(f'/api/photos/{photo_id}/subjects/analyze').status_code == 404


def test_predictions_stay_attached_to_each_subject_and_respect_review(db, subject_photo):
    photo_id, ids, path = subject_photo
    analyze_photo(db, photo_id, path)
    db.add_prediction(ids[0], 'American Robin', .9, 'bioclip', labels_fingerprint='old')
    db.add_prediction(ids[0], 'House Finch', .85, 'bioclip', labels_fingerprint='current')
    db.add_prediction(ids[1], 'Blue Jay', .95, 'bioclip', status='rejected')
    db.add_prediction(ids[1], 'Steller’s Jay', .8, 'bioclip')
    result = payload(db, photo_id)
    by_id = {s['id']: s for s in result['subjects']}
    assert [p['species'] for p in by_id[ids[0]]['predictions']] == ['House Finch']
    assert [p['species'] for p in by_id[ids[1]]['predictions']] == ["Steller's Jay"]
    select_primary(db, photo_id, ids[0])
    assert payload(db, photo_id)['subjects'][0]['predictions'][0]['species'] == 'House Finch'


def test_cannot_reactivate_mask_from_previous_primary(db, subject_photo):
    photo_id, ids, path = subject_photo
    analyze_photo(db, photo_id, path)
    det = db.get_detections(photo_id)[0]
    db.conn.execute("""INSERT INTO photo_masks(photo_id,variant,path,created_at,detector_model,prompt_x,prompt_y,prompt_w,prompt_h)
        VALUES (?,'test','mask.png',1,?,?,?,?,?)""", (photo_id, det['detector_model'], *(det['box_' + k] for k in 'xywh')))
    db.set_active_mask_variant(photo_id, 'test')
    select_primary(db, photo_id, ids[0])
    with pytest.raises(ValueError, match='another subject'):
        db.set_active_mask_variant(photo_id, 'test')


def test_activate_mask_survives_floor_change_without_state_sync(db, subject_photo):
    """The guard checks the current effective primary, not the cached
    ``photo_subject_state``: a workspace-level ``detector_confidence``
    raise (or a peer workspace running with a different floor) can flip
    the primary before subject analysis or selection has a chance to
    refresh the cache. Both mask-extraction paths resolve the primary
    using the current floor, so the freshly written mask must be
    activatable even while the cached state still names the previous
    subject.
    """
    photo_id, ids, path = subject_photo
    analyze_photo(db, photo_id, path)
    # Initial primary is ids[1] (sharp/higher-quality subject). Insert a
    # mask matching it and activate; photo_subject_state now names ids[1].
    det_hi = next(d for d in db.get_detections(photo_id) if d['id'] == ids[1])
    db.conn.execute(
        "INSERT INTO photo_masks(photo_id,variant,path,created_at,"
        "detector_model,prompt_x,prompt_y,prompt_w,prompt_h) "
        "VALUES (?,'test','mask.png',1,?,?,?,?,?)",
        (photo_id, det_hi['detector_model'],
         *(det_hi['box_' + k] for k in 'xywh')),
    )
    db.set_active_mask_variant(photo_id, 'test')
    # Raise the workspace's detector_confidence above ids[1]'s 0.7 so the
    # current effective primary flips to ids[0] (0.95) WITHOUT syncing
    # photo_subject_state, which still names ids[1].
    db.update_workspace(db._ws_id(), config_overrides={'detector_confidence': 0.8})
    det_lo = next(d for d in db.get_detections(photo_id, min_conf=0.8)
                  if d['id'] == ids[0])
    # A re-extraction under the new floor would rewrite the mask row with
    # the new primary's prompt; activating that mask must not raise even
    # though the cached state still points at the previous subject.
    db.upsert_photo_mask(
        photo_id=photo_id, variant='test', path='mask.png',
        detector_model=det_lo['detector_model'],
        prompt_x=det_lo['box_x'], prompt_y=det_lo['box_y'],
        prompt_w=det_lo['box_w'], prompt_h=det_lo['box_h'],
    )
    db.set_active_mask_variant(photo_id, 'test')


def test_eye_predictions_use_primary_even_when_other_detection_is_more_confident(db, subject_photo):
    photo_id, ids, path = subject_photo
    analyze_photo(db, photo_id, path)
    for index, detection_id in enumerate(ids[:2]):
        db.add_prediction(detection_id, f'Bird {index}', .9, 'bioclip',
                          taxonomy={'class': 'Aves', 'scientific_name': 'Test bird'})
    # A production photo has ``mask_path`` set only through
    # ``set_active_mask_variant``, which requires a matching photo_masks
    # row for the current primary. The stale-mask predicate now insists
    # on the same invariant.
    primary_det = next(d for d in db.get_detections(photo_id)
                       if d['category'] == 'animal'
                       and d['detector_model'] != 'full-image')
    db.conn.execute(
        "INSERT INTO photo_masks(photo_id,variant,path,created_at,"
        "detector_model,prompt_x,prompt_y,prompt_w,prompt_h) "
        "VALUES (?,'test','mask.png',1,?,?,?,?,?)",
        (photo_id, primary_det['detector_model'],
         *(primary_det['box_' + k] for k in 'xywh')),
    )
    db.set_active_mask_variant(photo_id, 'test')
    rows = db.list_photos_for_eye_keypoint_stage([photo_id])
    assert len(rows) == 1
    assert rows[0]['box_x'] == .6


def test_eye_stage_survives_floor_change_without_state_sync(db, subject_photo):
    """The eye-stage predicate resolves the primary against the current
    ``detector_confidence`` floor, not the cached ``photo_subject_state``.
    A workspace raising the floor above the stored subject — or a peer
    workspace sharing the photo writing that state at a different floor —
    would otherwise exclude every detection: the stored subject fails
    the confidence join and the current above-floor primary fails the
    state-ID check. Both mask-extraction paths already re-resolve the
    primary at the current floor, so the eye stage must too or it would
    never compute eye focus for the new primary until analysis or
    selection refreshed the cache. A stale mask (belonging to the
    previous primary) still excludes the photo — the Process pipeline
    regenerates masks first, and the standalone eye stage must wait
    until that regeneration lands so it doesn't stamp an eye result
    against a wrong-subject mask.
    """
    photo_id, ids, path = subject_photo
    analyze_photo(db, photo_id, path)
    for detection_id in ids[:2]:
        db.add_prediction(detection_id, 'Test', .9, 'bioclip',
                          taxonomy={'class': 'Aves', 'scientific_name': 'Test bird'})
    det_hi = next(d for d in db.get_detections(photo_id) if d['id'] == ids[1])
    db.conn.execute(
        "INSERT INTO photo_masks(photo_id,variant,path,created_at,"
        "detector_model,prompt_x,prompt_y,prompt_w,prompt_h) "
        "VALUES (?,'test','mask.png',1,?,?,?,?,?)",
        (photo_id, det_hi['detector_model'],
         *(det_hi['box_' + k] for k in 'xywh')),
    )
    db.set_active_mask_variant(photo_id, 'test')
    # Baseline: cached state names ids[1] (initial primary at floor 0.2);
    # eye stage surfaces the photo on ids[1]'s box.
    baseline = db.list_photos_for_eye_keypoint_stage([photo_id])
    assert len(baseline) == 1 and baseline[0]['box_x'] == .6
    # Raise the workspace's floor above ids[1]'s 0.7. ids[0] (0.95) is
    # now the effective primary, but photo_subject_state still names
    # ids[1] — which now falls below the confidence join. Under the old
    # predicate every detection would be excluded; under the fix, ids[0]
    # surfaces once the mask has been regenerated for it.
    db.update_workspace(db._ws_id(), config_overrides={'detector_confidence': 0.8})
    # Before regeneration the cached mask still points at ids[1]'s
    # prompt, so the stale-mask predicate excludes the photo — the eye
    # stage will not run keypoint inference against a wrong-subject
    # mask.
    assert db.list_photos_for_eye_keypoint_stage([photo_id]) == []
    # Regenerate the mask against the new primary (as extract_masks_stage
    # would). photo_subject_state is intentionally not touched — the eye
    # stage must resolve the primary at the current floor.
    det_lo = next(d for d in db.get_detections(photo_id, min_conf=0.8)
                  if d['id'] == ids[0])
    db.upsert_photo_mask(
        photo_id=photo_id, variant='test', path='mask.png',
        detector_model=det_lo['detector_model'],
        prompt_x=det_lo['box_x'], prompt_y=det_lo['box_y'],
        prompt_w=det_lo['box_w'], prompt_h=det_lo['box_h'],
    )
    db.set_active_mask_variant(photo_id, 'test')
    rows = db.list_photos_for_eye_keypoint_stage([photo_id])
    assert len(rows) == 1
    assert rows[0]['box_x'] == .05


def test_empty_redetection_clears_previous_subject_quality(db, subject_photo):
    photo_id, ids, path = subject_photo
    analyze_photo(db, photo_id, path)
    db.clear_detections(photo_id)
    assert analyze_photo(db, photo_id, path) == 0
    assert db.conn.execute('SELECT quality_score FROM photos WHERE id=?', (photo_id,)).fetchone()[0] is None


@pytest.mark.parametrize("cancel_during_detection", [False, True])
@pytest.mark.parametrize("replacement", [False, True])
def test_reclassify_cancellation_clears_previous_subject(db, subject_photo, monkeypatch,
                                                       cancel_during_detection, replacement):
    from types import SimpleNamespace

    import classify_job
    from resource_ledger import ResourceWaitCancelled

    photo_id, ids, path = subject_photo
    analyze_photo(db, photo_id, path)
    db.conn.execute("UPDATE photos SET mask_path='old.png', eye_x=.7, dino_subject_embedding=X'01' WHERE id=?", (photo_id,))
    db.conn.commit()
    # Standalone Classify clears immediately before entering _detect_batch.
    db.clear_detections(photo_id)

    def detect(_path):
        if cancel_during_detection:
            raise ResourceWaitCancelled("Stopped")
        return ([{"box": {"x": .1, "y": .1, "w": .2, "h": .2},
                  "confidence": .9, "category": "animal"}] if replacement else [])

    monkeypatch.setattr(classify_job, "detect_animals", detect)
    monkeypatch.setattr(classify_job, "get_primary_detection", lambda detections: None)
    monkeypatch.setattr("subjects.analyze_image", lambda *args: pytest.fail("Stop must not start analysis"))
    runner = SimpleNamespace(is_cancelled=lambda _: True)
    photo = dict(db.conn.execute("SELECT * FROM photos WHERE id=?", (photo_id,)).fetchone())
    with pytest.raises(ResourceWaitCancelled):
        classify_job._detect_batch([photo], {photo["folder_id"]: str(path.parent)},
                                   runner, {"id": 1}, True, db, det_conf_threshold=.2)
    state = db.conn.execute("SELECT detection_id FROM photo_subject_state WHERE photo_id=?", (photo_id,)).fetchone()
    if replacement and not cancel_during_detection:
        assert state["detection_id"] == db.get_detections(photo_id)[0]["id"]
    else:
        assert state is None
    row = db.conn.execute("SELECT * FROM photos WHERE id=?", (photo_id,)).fetchone()
    for column in ("mask_path", "eye_x", "eye_kp_fingerprint", "dino_subject_embedding", "quality_score"):
        assert row[column] is None


@pytest.mark.parametrize("manual", [False, True])
def test_live_misses_preview_uses_primary_and_keeps_maximum_confidence(db, subject_photo, manual):
    from misses import _attach_primary_detections

    photo_id, ids, path = subject_photo
    if manual:
        select_primary(db, photo_id, ids[1])
    else:
        analyze_photo(db, photo_id, path)
    rows = [{"id": photo_id}]
    _attach_primary_detections(db, rows, .2)
    assert rows[0]["detection_conf"] == .7
    assert rows[0]["raw_detection_conf"] == .95
    import json
    assert json.loads(rows[0]["detection_box"])["x"] == .6
    _attach_primary_detections(db, rows, .8)
    assert rows[0]["detection_conf"] == .95
    _attach_primary_detections(db, rows, .99)
    assert rows[0]["detection_box"] is None
    assert rows[0]["raw_detection_conf"] == .95


@pytest.mark.parametrize("empty, known_run", [(False, False), (False, True), (True, True)])
def test_detectorless_cached_run_clears_ineligible_primary(db, subject_photo, monkeypatch, empty, known_run):
    import classify_job

    photo_id, ids, path = subject_photo
    analyze_photo(db, photo_id, path)
    db.conn.execute("UPDATE photos SET mask_path='old.png', eye_x=.7, dino_subject_embedding=X'01' WHERE id=?", (photo_id,))
    if empty:
        db.clear_detections(photo_id)
        db.write_detection_batch(photo_id, "megadetector-v6", [])
    else:
        db.conn.execute("UPDATE detections SET detector_confidence=.1 WHERE photo_id=?", (photo_id,))
    db.conn.commit()
    path.unlink()  # Cleanup requires no source read or detector module.
    monkeypatch.setattr(classify_job, "detect_animals", None)
    photo = dict(db.conn.execute("SELECT * FROM photos WHERE id=?", (photo_id,)).fetchone())
    classify_job._detect_batch([photo], {photo["folder_id"]: str(path.parent)},
        None, {"id": 1}, False, db, det_conf_threshold=.2,
        already_detected_ids=db.get_detector_run_photo_ids("megadetector-v6") if known_run else set())
    row = db.conn.execute("SELECT * FROM photos WHERE id=?", (photo_id,)).fetchone()
    for column in ("mask_path", "eye_x", "dino_subject_embedding", "quality_score"):
        assert row[column] is None
    assert db.conn.execute("SELECT 1 FROM photo_subject_state WHERE photo_id=?", (photo_id,)).fetchone() is None
