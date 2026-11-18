"""Primary selection alone cannot establish provenance for legacy scores."""

import pytest
from subjects import sync_primary
from test_one_primary_subject import _analysis, _folder, _regions


def _legacy_photo(db, confidence=.9):
    fid = _folder(db)
    pid = db.conn.execute(
        "INSERT INTO photos(folder_id,filename,quality_score) VALUES (?,'synthetic-legacy.jpg',.8)",
        (fid,),
    ).lastrowid
    det = _regions(db, pid, ((.1, .1, .3, .3), confidence))[0]
    db.commit()
    return pid, det


def test_syncing_legacy_primary_without_measurement_does_not_clear_audit(db):
    pid, det = _legacy_photo(db)
    assert db.subjects.primary_state(pid) is None
    assert db.audit.subject_findings(.2)[0]['stale_score'] == 1

    sync_primary(db, pid, min_conf=.2)
    db.commit()

    assert db.subjects.primary_state(pid)['detection_id'] == det
    before = db.conn.total_changes
    rows = db.audit.subject_findings(.2)
    assert [(r['id'], r['stale_score']) for r in rows] == [(pid, 1)]
    assert db.conn.execute('SELECT quality_score FROM photos WHERE id=?', (pid,)).fetchone()[0] == .8
    assert db.conn.total_changes == before


@pytest.mark.parametrize('confidence,floor', [(.9, .2), (.1, .05), (.001, 0)])
def test_projected_measured_score_is_clean_including_weak_rescue(db, confidence, floor):
    pid, det = _legacy_photo(db, confidence)
    _analysis(db, det, .6)
    sync_primary(db, pid, min_conf=floor, detector_model='megadetector-v6')
    db.commit()
    assert db.conn.execute('SELECT quality_score FROM photos WHERE id=?', (pid,)).fetchone()[0] == .6
    assert db.audit.subject_findings(.2) == []


@pytest.mark.parametrize('change', ['other_detection', 'retired_measurement', 'different_score'])
def test_audit_requires_the_projected_primarys_matching_measurement(db, change):
    pid, det = _legacy_photo(db)
    if change == 'other_detection':
        other = _regions(db, pid, ((.1, .1, .3, .3), .9), ((.6, .6, .2, .2), .3))[1]
        _analysis(db, other, .8)
        db.conn.execute('INSERT INTO photo_subject_choices(photo_id,detection_id) VALUES (?,?)', (pid, det))
    else:
        _analysis(db, det, .8)
    sync_primary(db, pid, min_conf=.2)
    if change == 'retired_measurement':
        db.subjects.clear_analysis(det)
    elif change == 'different_score':
        db.conn.execute('UPDATE photos SET quality_score=.99 WHERE id=?', (pid,))
    db.commit()
    assert db.subjects.primary_state(pid)['detection_id'] == det
    assert db.audit.subject_findings(.2)[0]['stale_score'] == 1
