"""Audits compare projected features with their recorded per-photo policy."""

import pytest
from subjects import retained, sync_primary
from test_one_primary_subject import _analysis, _folder


def _photo(db, *, floor=.05, model='megadetector-v6', confidence=.1):
    fid = _folder(db)
    pid = db.conn.execute(
        "INSERT INTO photos(folder_id,filename,quality_score,active_mask_variant) "
        "VALUES (?,'synthetic-rescue.jpg',.8,'test')", (fid,),
    ).lastrowid
    det = db.detections.save(pid, [dict(
        box=dict(x=.1, y=.1, w=.3, h=.3), confidence=confidence, category='animal',
    )], detector_model='megadetector-v6')[0]
    _analysis(db, det, .8)
    sync_primary(db, pid, min_conf=floor, detector_model=model)
    db.conn.execute("""INSERT INTO photo_masks(photo_id,variant,path,created_at,detector_model,
        prompt_x,prompt_y,prompt_w,prompt_h) SELECT photo_id,'test','synthetic-mask',1,
        detector_model,box_x,box_y,box_w,box_h FROM detections WHERE id=?""", (det,))
    db.commit()
    return pid, det


@pytest.mark.parametrize('floor,confidence', [(.05, .1), (0, .001), (.2, .9)])
def test_subject_audit_accepts_mask_and_quality_projected_under_recorded_floor(db, floor, confidence):
    pid, det = _photo(db, floor=floor, confidence=confidence)
    state = db.subjects.primary_state(pid)
    assert state['detection_id'] == det
    assert state['min_conf'] == floor
    assert db.audit.subject_findings(.2) == []


@pytest.mark.parametrize('model,stale', [('megadetector-v6', False), (None, True)])
def test_subject_audit_respects_recorded_detector_model(db, model, stale):
    pid, det = _photo(db, model=model)
    db.detections.save(pid, [dict(
        box=dict(x=.6, y=.6, w=.3, h=.3), confidence=.99, category='animal',
    )], detector_model='other-detector')
    db.commit()
    assert retained(db, pid, .05, detector_model='megadetector-v6')[0]['id'] == det
    rows = db.audit.subject_findings(.2)
    assert [(row['id'], row['stale_mask'], row['stale_score']) for row in rows] == ([(pid, 1, 1)] if stale else [])


@pytest.mark.parametrize('change', ['rejected', 'below_recorded_floor', 'changed_choice', 'legacy_policy'])
def test_subject_audit_recorded_policy_does_not_hide_stale_provenance(db, change):
    pid, det = _photo(db)
    if change == 'rejected':
        db.conn.execute("UPDATE detections SET subject_verdict='not_animal' WHERE id=?", (det,))
    elif change == 'below_recorded_floor':
        db.conn.execute('UPDATE detections SET detector_confidence=.01 WHERE id=?', (det,))
    elif change == 'legacy_policy':
        db.conn.execute('UPDATE photo_subject_state SET min_conf=NULL,detector_model=NULL WHERE photo_id=?', (pid,))
    else:
        other = db.detections.save(pid, [dict(
            box=dict(x=.6, y=.6, w=.2, h=.2), confidence=.15, category='animal',
        )], detector_model='megadetector-v6')[0]
        db.conn.execute('INSERT INTO photo_subject_choices(photo_id,detection_id) VALUES (?,?)', (pid, other))
    db.commit()
    rows = db.audit.subject_findings(.2)
    assert [(row['id'], row['stale_mask'], row['stale_score']) for row in rows] == [(pid, 1, 1)]
