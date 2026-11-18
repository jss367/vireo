"""Reference crops use the same saved selection policy as the audit."""
import json

import config as cfg
import pytest
from services.subject_audit import subject_audit
from test_one_primary_subject import _analysis
from test_subject_audit_rescue_policy import _photo


def _references(db, pid):
    db.collections.add('Subject audit references', json.dumps([{'field': 'photo_ids', 'value': [pid]}]))
    db.commit()
    return subject_audit(db)['references']


@pytest.mark.parametrize('floor,confidence', [(.05, .1), (0, .001)])
def test_reference_crop_keeps_the_recorded_weak_subject(db, floor, confidence):
    pid, did = _photo(db, floor=floor, confidence=confidence)
    assert db.audit.subject_findings(.2) == []
    assert _references(db, pid)[0]['detection_id'] == did


def test_reference_crop_respects_recorded_detector_model(db):
    pid, did = _photo(db, floor=.05)
    other, = db.detections.save(pid, [dict(box=dict(x=.6, y=.6, w=.2, h=.2), confidence=.99)], 'other-detector')
    _analysis(db, other, .99)
    db.commit()
    assert _references(db, pid)[0]['detection_id'] == did


def test_reference_with_no_recorded_floor_uses_workspace_floor(db, tmp_path, monkeypatch):
    global_config = tmp_path / 'synthetic-config.json'
    global_config.write_text(json.dumps({'detector_confidence': .05}))
    monkeypatch.setattr(cfg, 'CONFIG_PATH', str(global_config))
    db.workspaces.update(db.require_workspace_id(), config_overrides={'detector_confidence': .2})
    pid, _ = _photo(db, floor=.05)
    db.conn.execute('UPDATE photo_subject_state SET min_conf=NULL WHERE photo_id=?', (pid,))
    db.commit()
    assert cfg.load()['detector_confidence'] == .05
    assert db.get_effective_config(cfg.load())['detector_confidence'] == .2
    assert _references(db, pid)[0]['detection_id'] is None


def test_reference_species_is_null_when_its_only_prediction_is_rejected(db):
    pid, did = _photo(db, floor=.2, confidence=.9)
    db.predictions.add(did, 'Synthetic bird', .9, 'synthetic-classifier', status='rejected')
    db.commit()
    row, = _references(db, pid)
    assert row['detection_id'] == did
    assert row['species'] is None
