"""Audit reports describe one catalog state without committing their caller."""
import json
from contextlib import closing

import config as cfg
import pytest
from db import Database
from repositories.subjects import SubjectsRepository
from services import subject_audit as audit_service
from subjects import select_primary, sync_primary
from test_one_primary_subject import _folder


@pytest.fixture(autouse=True)
def synthetic_config(tmp_path, monkeypatch):
    path = tmp_path / 'synthetic-config.json'
    path.write_text(json.dumps({'detector_confidence': .2}))
    monkeypatch.setattr(cfg, 'CONFIG_PATH', str(path))


def _scene(db):
    folder = _folder(db, '/synthetic-audit-snapshot')
    pid = db.conn.execute("INSERT INTO photos(folder_id,filename,rating) VALUES (?,'synthetic.jpg',0)", (folder,)).lastrowid
    a1, a2 = db.detections.save(pid, [
        dict(box=dict(x=.1, y=.1, w=.2, h=.2), confidence=.6),
        dict(box=dict(x=.4, y=.4, w=.2, h=.2), confidence=.95),
    ], 'model-a')
    b, = db.detections.save(pid, [dict(box=dict(x=.7, y=.7, w=.2, h=.2), confidence=.99)], 'model-b')
    for did, name in [(a1, 'Before choice'), (a2, 'Never primary'), (b, 'After choice')]:
        db.predictions.add(did, name, .9, 'synthetic-classifier')
    db.subjects.set_choice(pid, a1)
    sync_primary(db, pid, min_conf=.2, detector_model='model-a')
    db.collections.add_import('Subject audit references', [pid])
    db.commit()
    return pid, a1, a2, b


def _switch_after_policy(monkeypatch, reader, writer, pid, new_id):
    original = SubjectsRepository.primary_state
    switched = []

    def read_policy(repository, photo_id):
        policy = original(repository, photo_id)
        if repository.conn is reader.conn and photo_id == pid and not switched:
            switched.append(photo_id)
            select_primary(writer, pid, new_id)
        return policy

    monkeypatch.setattr(SubjectsRepository, 'primary_state', read_policy)
    return switched


def test_reference_policy_ranking_and_caption_share_one_read_snapshot(db, monkeypatch):
    pid, before, never, after = _scene(db)
    with closing(Database(db.job_history.catalog_file(), initialize_schema=False)) as writer:
        writer.set_active_workspace(db.require_workspace_id())
        switched = _switch_after_policy(monkeypatch, db, writer, pid, after)
        report = audit_service.subject_audit(db)
        row, = report['references']
        assert switched == [pid]
        assert row['detection_id'] != never, row
        assert row['detection_id'] == before
        assert row['species'] == 'Before choice'
        assert not db.in_transaction
        assert db.subjects.choice(pid)['detection_id'] == after
        # The read snapshot must end before recording this point-in-time report.
        with db.transaction():
            db.audit.record_run('subjects', len(report['findings']))
        assert writer.audit.get_runs()['subjects']['problem_count'] == len(report['findings'])


def test_report_does_not_commit_a_callers_pending_write(db):
    pid, _, _, _ = _scene(db)
    db.conn.execute('UPDATE photos SET rating=5 WHERE id=?', (pid,))
    audit_service.subject_audit(db)
    assert db.in_transaction
    with closing(Database(db.job_history.catalog_file(), initialize_schema=False)) as observer:
        assert observer.conn.execute('SELECT rating FROM photos WHERE id=?', (pid,)).fetchone()[0] == 0
    db.rollback()
    assert db.conn.execute('SELECT rating FROM photos WHERE id=?', (pid,)).fetchone()[0] == 0


def test_report_keeps_an_owned_transaction_and_its_callbacks(db):
    pid, _, _, _ = _scene(db)
    committed = []
    with db.transaction():
        db.conn.execute('UPDATE photos SET rating=5 WHERE id=?', (pid,))
        db.after_commit(lambda: committed.append(True))
        audit_service.subject_audit(db)
        assert db.in_transaction and committed == []
    assert committed == [True]


@pytest.mark.parametrize("pending", [False, True])
def test_report_failure_releases_its_snapshot_without_ending_the_caller(db, monkeypatch, pending):
    pid, _, _, _ = _scene(db)
    if pending:
        db.conn.execute("UPDATE photos SET rating=5 WHERE id=?", (pid,))

    def fail(_db):
        raise ValueError('synthetic report failure')

    monkeypatch.setattr(audit_service, '_review_buckets', fail)
    with pytest.raises(ValueError, match='synthetic report failure'):
        audit_service.subject_audit(db)
    assert db.in_transaction is pending
    if pending:
        assert db.conn.execute('SELECT rating FROM photos WHERE id=?', (pid,)).fetchone()[0] == 5
        db.rollback()
        assert db.conn.execute('SELECT rating FROM photos WHERE id=?', (pid,)).fetchone()[0] == 0
