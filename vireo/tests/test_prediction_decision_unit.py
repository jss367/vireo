"""``prediction_decisions.decision``: the decision lock as one unit of work.

Every prediction-decision route runs its checks and writes through
``under_prediction_decision_lock``, built on ``decision``. These pin the
rules the routes now rely on instead of a rollback at each exit.
"""

import contextlib
import sqlite3

import pytest
from services import prediction_decisions
from services.prediction_decisions import DecisionRefused


def _json_error(message, status=400):
    return ({"error": message}, status)


def _write(db, name):
    db.conn.execute("INSERT INTO db_meta (key, value) VALUES (?, '1')", (f"decision-{name}",))


def _committed(db):
    with contextlib.closing(sqlite3.connect(db._db_path)) as other:
        rows = other.execute(
            "SELECT key FROM db_meta WHERE key LIKE 'decision-%' ORDER BY key",
        ).fetchall()
    return [key.removeprefix("decision-") for (key,) in rows]


def _lock_is_held(db):
    with contextlib.closing(sqlite3.connect(db._db_path, timeout=0)) as other:
        try:
            other.execute("BEGIN IMMEDIATE")
        except sqlite3.OperationalError:
            return True
        other.rollback()
        return False


def test_a_decision_holds_the_lock_and_commits_once(db):
    with prediction_decisions.decision(db, json_error=_json_error):
        assert _lock_is_held(db)
        assert db.in_unit_of_work
        _write(db, "a")
        assert _committed(db) == []
    assert _committed(db) == ["a"]
    assert not _lock_is_held(db)


def test_a_decision_rolls_back_when_its_block_raises(db):
    with pytest.raises(RuntimeError), prediction_decisions.decision(db, json_error=_json_error):
        _write(db, "a")
        raise RuntimeError("boom")
    assert _committed(db) == []
    assert not db.in_transaction


def test_a_refused_decision_rolls_back(db):
    refusal = _json_error("stale", 409)
    with pytest.raises(DecisionRefused) as refused, prediction_decisions.decision(db, json_error=_json_error):
        _write(db, "a")
        raise DecisionRefused(refusal)
    assert refused.value.response is refusal
    assert _committed(db) == []


def test_a_busy_lock_refuses_with_503_before_the_block_runs(db, monkeypatch):
    def busy():
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(db, "begin_immediate", busy)
    ran = []
    with pytest.raises(DecisionRefused) as refused, prediction_decisions.decision(db, json_error=_json_error):
        ran.append(True)
    assert ran == []
    assert refused.value.response[1] == 503


def test_prediction_ids_are_revalidated_inside_the_lock(db, monkeypatch):
    monkeypatch.setattr(prediction_decisions, "out_of_workspace_prediction_ids", lambda _db, ids: {ids[0]})
    ran = []
    result = prediction_decisions.under_prediction_decision_lock(
        db, lambda: ran.append(True), json_error=_json_error, prediction_ids=[7],
    )
    assert ran == []
    assert result == ({"error": "Prediction does not belong to the active workspace"}, 404)


def test_an_error_response_rolls_back_whatever_was_written(db):
    def work():
        _write(db, "partial")
        return _json_error("member vanished", 409)

    result = prediction_decisions.under_prediction_decision_lock(db, work, json_error=_json_error)
    assert result == ({"error": "member vanished"}, 409)
    assert _committed(db) == []
    assert not db.in_transaction


def test_a_success_commits_and_runs_after_commit(db):
    seen = []

    def work():
        _write(db, "done")
        db.after_commit(lambda: seen.append(_committed(db)))
        return {"ok": True}

    result = prediction_decisions.under_prediction_decision_lock(db, work, json_error=_json_error)
    assert result == {"ok": True}
    assert seen == [["done"]]


def test_a_none_result_commits(db):
    def work():
        _write(db, "none")

    assert prediction_decisions.under_prediction_decision_lock(db, work, json_error=_json_error) is None
    assert _committed(db) == ["none"]


def test_a_legacy_commit_inside_the_decision_is_held(db):
    def work():
        _write(db, "first")
        db.commit()
        assert _committed(db) == []
        _write(db, "second")
        return {"ok": True}

    prediction_decisions.under_prediction_decision_lock(db, work, json_error=_json_error)
    assert _committed(db) == ["first", "second"]
