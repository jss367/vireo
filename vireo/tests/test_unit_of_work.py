"""``Database.transaction()``: the unit of work every owner commits through.

See ``docs/plans/2026-10-25-unit-of-work-design.md``. Each rule below is one
the migration relies on: an owner's block is one transaction, nested blocks
are savepoints, data methods that still commit or roll back on their own
cannot split or half-commit a unit, and follow-up work waits for the commit.
"""

import contextlib
import logging
import sqlite3

import pytest
import unit_of_work
from unit_of_work import TransactionOwnershipError


def _write(db, name):
    db.conn.execute("INSERT INTO db_meta (key, value) VALUES (?, '1')", (f"uow-{name}",))


def _committed(db):
    """The ``uow-*`` markers another connection can see, sorted."""
    with contextlib.closing(sqlite3.connect(db._db_path)) as other:
        rows = other.execute("SELECT key FROM db_meta WHERE key LIKE 'uow-%' ORDER BY key").fetchall()
    return [key.removeprefix("uow-") for (key,) in rows]


def _writer_lock_is_held(db):
    with contextlib.closing(sqlite3.connect(db._db_path, timeout=0)) as other:
        try:
            other.execute("BEGIN IMMEDIATE")
        except sqlite3.OperationalError:
            return True
        other.rollback()
        return False


def test_a_unit_commits_once_when_its_block_exits(db):
    with db.transaction():
        assert db.in_unit_of_work
        _write(db, "a")
        _write(db, "b")
        assert _committed(db) == []
    assert not db.in_unit_of_work
    assert not db.in_transaction
    assert _committed(db) == ["a", "b"]


def test_a_plain_unit_begins_at_its_first_write(db):
    # Reads and slow work before the first write hold no lock.
    with db.transaction():
        assert not db.in_transaction
        assert not _writer_lock_is_held(db)
        _write(db, "a")
        assert db.in_transaction
        assert _writer_lock_is_held(db)


def test_an_immediate_unit_holds_the_writer_lock_from_its_start(db):
    with db.transaction(immediate=True):
        assert db.in_transaction
        assert _writer_lock_is_held(db)
    assert not _writer_lock_is_held(db)


def test_an_immediate_unit_refuses_an_open_transaction(db):
    # As BEGIN IMMEDIATE does: the caller's open work is neither adopted nor
    # rolled back.
    _write(db, "caller")
    with pytest.raises(sqlite3.OperationalError), db.transaction(immediate=True):
        pass
    assert db.in_transaction
    db.commit()
    assert _committed(db) == ["caller"]


def test_a_plain_unit_adopts_an_open_transaction(db):
    # The migration rule for unconverted callers: as ``with conn:`` did.
    db.begin_immediate()
    _write(db, "caller")
    with db.transaction():
        _write(db, "unit")
    assert _committed(db) == ["caller", "unit"]


def test_a_unit_rolls_back_when_its_block_raises(db):
    with pytest.raises(RuntimeError, match="boom"), db.transaction():
        _write(db, "a")
        raise RuntimeError("boom")
    assert not db.in_transaction
    assert not db.in_unit_of_work
    assert _committed(db) == []


def test_a_nested_unit_is_a_savepoint(db):
    with db.transaction():
        _write(db, "outer")
        with pytest.raises(RuntimeError), db.transaction():
            _write(db, "inner")
            raise RuntimeError("inner failure")
        # The caller caught the inner failure: only the inner unit is undone.
        _write(db, "after")
        with db.transaction():
            _write(db, "kept")
        assert _committed(db) == []
    assert _committed(db) == ["after", "kept", "outer"]


def test_a_nested_unit_begins_its_owners_transaction(db):
    # A SAVEPOINT outside a transaction would begin one its RELEASE commits.
    with db.transaction():
        assert not db.in_transaction
        with db.transaction():
            assert db.in_transaction
            _write(db, "a")
        assert db.in_transaction
        assert _committed(db) == []
    assert _committed(db) == ["a"]


def test_an_inner_exception_that_propagates_rolls_back_the_whole_unit(db):
    with pytest.raises(RuntimeError), db.transaction():
        _write(db, "outer")
        with db.transaction():
            _write(db, "inner")
            raise RuntimeError("boom")
    assert _committed(db) == []


def test_a_legacy_commit_inside_a_unit_is_held(db):
    with db.transaction():
        _write(db, "a")
        db.conn.commit()
        db.commit()
        db.commit_with_retry()
        assert db.in_transaction
        assert _committed(db) == []
    assert _committed(db) == ["a"]


def test_a_legacy_with_conn_block_inside_a_unit_is_a_savepoint(db):
    with db.transaction():
        _write(db, "outer")
        with pytest.raises(RuntimeError), db.conn:
            _write(db, "inner")
            raise RuntimeError("boom")
        with db.conn:
            _write(db, "kept")
        assert _committed(db) == []
    assert _committed(db) == ["kept", "outer"]


def test_with_conn_outside_a_unit_still_commits(db):
    with db.conn:
        _write(db, "a")
    assert _committed(db) == ["a"]


def test_a_legacy_rollback_dooms_the_unit(db):
    # Today's ``except: self.conn.rollback(); raise`` in a data method, with a
    # caller that swallows the error: the unit's earlier writes are gone, so
    # it must not commit the writes made after them.
    with pytest.raises(TransactionOwnershipError), db.transaction():
        _write(db, "before")
        with contextlib.suppress(ValueError):
            try:
                _write(db, "failing")
                raise ValueError("data method failure")
            except ValueError:
                db.conn.rollback()
                raise
        _write(db, "after")
        with db.transaction():
            _write(db, "nested-after")
    assert not db.in_transaction
    assert _committed(db) == []
    # The next unit starts clean.
    with db.transaction():
        _write(db, "next")
    assert _committed(db) == ["next"]


def test_a_legacy_rollback_with_nothing_written_after_ends_the_unit(db):
    # ``db.rollback(); return`` inside a unit (a refused plan): the unit ends
    # rolled back, as it always did, without an ownership error.
    with db.transaction():
        _write(db, "refused")
        db.rollback()
    assert not db.in_transaction
    assert _committed(db) == []


def test_a_doomed_unit_reports_the_propagating_error_instead(db):
    with pytest.raises(ValueError, match="original"), db.transaction():
        _write(db, "a")
        db.rollback()
        raise ValueError("original")
    assert _committed(db) == []


def test_after_commit_runs_once_the_unit_is_durable(db):
    seen = []
    with db.transaction():
        _write(db, "a")
        db.after_commit(lambda: seen.append(_committed(db)))
        with db.transaction():
            db.after_commit(lambda: seen.append("nested"))
        assert seen == []
    assert seen == [["a"], "nested"]


def test_after_commit_is_dropped_on_rollback(db):
    seen = []
    with pytest.raises(RuntimeError), db.transaction():
        db.after_commit(lambda: seen.append("outer"))
        raise RuntimeError("boom")
    with db.transaction():
        with pytest.raises(RuntimeError), db.transaction():
            db.after_commit(lambda: seen.append("rolled back"))
            raise RuntimeError("boom")
        db.after_commit(lambda: seen.append("kept"))
    assert seen == ["kept"]


def test_after_commit_outside_a_unit_runs_at_once(db):
    seen = []
    db.after_commit(lambda: seen.append(1))
    assert seen == [1]


def test_a_failing_after_commit_callback_is_logged_not_raised(db, caplog):
    seen = []

    def fail():
        raise OSError("disk")

    with caplog.at_level(logging.ERROR, logger="unit_of_work"), db.transaction():
        _write(db, "a")
        db.after_commit(fail)
        db.after_commit(lambda: seen.append("next"))
    assert _committed(db) == ["a"]
    assert seen == ["next"]
    assert "after_commit callback" in caplog.text


def test_species_provenance_snapshots_follow_the_savepoint(db):
    db._species_provenance_before = {(1, 1): {"before": "outer"}}
    with db.transaction():
        db._species_provenance_before[(2, 2)] = {"before": "unit"}
        with pytest.raises(RuntimeError), db.transaction():
            db._species_provenance_before[(3, 3)] = {"before": "rolled back"}
            raise RuntimeError("boom")
        assert set(db._species_provenance_before) == {(1, 1), (2, 2)}
    with pytest.raises(RuntimeError), db.transaction():
        raise RuntimeError("boom")
    assert db._species_provenance_before == {}


def test_retry_busy_retries_a_locked_begin(monkeypatch):
    monkeypatch.setattr(unit_of_work.time, "sleep", lambda _seconds: None)
    attempts = []

    class Conn:
        def execute(self, sql):
            attempts.append(sql)
            if len(attempts) < 3:
                raise sqlite3.OperationalError("database is locked")

    unit_of_work._begin_immediate(Conn(), retry_busy=True)
    assert attempts == ["BEGIN IMMEDIATE"] * 3
    attempts.clear()
    with pytest.raises(sqlite3.OperationalError, match="locked"):
        unit_of_work._begin_immediate(Conn(), retry_busy=False)
    assert attempts == ["BEGIN IMMEDIATE"]


def test_closing_over_an_open_transaction_is_reported(tmp_path, monkeypatch):
    from db import Database

    reported = []
    monkeypatch.setattr(unit_of_work, "discarded_transaction_listeners", [reported.append])
    database = Database(str(tmp_path / "catalog.db"))
    _write(database, "lost")
    database.close()
    assert reported == [str(tmp_path / "catalog.db")]

    database = Database(str(tmp_path / "catalog.db"))
    database.close()
    assert len(reported) == 1



def test_after_commit_follows_a_legacy_with_conn_savepoint(db):
    # A legacy ``with conn:`` inside a unit is a savepoint, so callbacks
    # registered in it share its fate, including from a unit nested in it.
    seen = []
    with db.transaction():
        db.after_commit(lambda: seen.append("outer"))
        with pytest.raises(RuntimeError), db.conn:
            _write(db, "failed")
            db.after_commit(lambda: seen.append("failed legacy block"))
            with db.transaction():
                db.after_commit(lambda: seen.append("nested in failed block"))
            raise RuntimeError("boom")
        with db.conn:
            _write(db, "kept")
            with db.transaction():
                db.after_commit(lambda: seen.append("nested in kept block"))
            db.after_commit(lambda: seen.append("kept legacy block"))
    assert _committed(db) == ["kept"]
    assert seen == ["outer", "nested in kept block", "kept legacy block"]


def test_a_refused_immediate_unit_leaves_the_callers_snapshot(db):
    # The caller's open legacy transaction and its provenance snapshot both
    # survive a refused ``immediate`` entry, for the caller to record.
    _write(db, "caller")
    db._species_provenance_before[(5, 6)] = {"before": "caller"}
    with pytest.raises(sqlite3.OperationalError), db.transaction(immediate=True):
        pass
    assert db._species_provenance_before == {(5, 6): {"before": "caller"}}
    assert db.in_transaction
    db.commit()
    assert _committed(db) == ["caller"]


class _CustomFollowUpError(Exception):
    pass


@pytest.mark.parametrize("error", [AttributeError, AssertionError, _CustomFollowUpError])
def test_any_ordinary_after_commit_failure_is_contained(db, caplog, error):
    seen = []

    def fail():
        raise error("follow-up failed")

    with caplog.at_level(logging.ERROR, logger="unit_of_work"), db.transaction():
        _write(db, "durable")
        db.after_commit(fail)
        db.after_commit(lambda: seen.append("later"))
    assert _committed(db) == ["durable"]
    assert seen == ["later"]
    assert "after_commit callback" in caplog.text

def test_a_failed_rollback_does_not_hide_the_failure(db, monkeypatch, caplog):
    real_rollback = db.conn.rollback

    def failing_rollback():
        raise sqlite3.OperationalError("disk I/O error")

    monkeypatch.setattr(db.conn, "rollback", failing_rollback)
    with caplog.at_level(logging.ERROR, logger="unit_of_work"):
        with pytest.raises(ValueError, match="original"), db.transaction():
            _write(db, "a")
            raise ValueError("original")
    assert "Rollback failed after a unit of work failed" in caplog.text
    monkeypatch.setattr(db.conn, "rollback", real_rollback)
    db.rollback()
    assert _committed(db) == []


def test_a_joining_unit_leaves_a_callers_transaction_to_the_caller(db):
    # ``join=True`` (a service callable from an unconverted owner): inside a
    # transaction someone else began it is a savepoint and never commits.
    db.begin_immediate()
    _write(db, "caller")
    with db.transaction(immediate=True, join=True):
        _write(db, "service")
    assert db.in_transaction
    assert _committed(db) == []
    with pytest.raises(RuntimeError), db.transaction(immediate=True, join=True):
        _write(db, "failed")
        raise RuntimeError("boom")
    assert db.in_transaction
    db.commit()
    assert _committed(db) == ["caller", "service"]


def test_a_joining_unit_with_nothing_open_owns_its_transaction(db):
    with db.transaction(immediate=True, join=True):
        assert _writer_lock_is_held(db)
        _write(db, "own")
    assert not db.in_transaction
    assert _committed(db) == ["own"]


def test_after_commit_is_refused_inside_a_joined_unit(db):
    # Savepoint release is not a durable commit, so a callback that ran at
    # the end of a joined block could perform an irreversible side effect
    # for writes the unconverted owner later rolls back. The attempt is
    # refused: callers that need durable follow-up work check
    # ``owns_transaction`` and run it after the join block.
    db.begin_immediate()
    _write(db, "caller")
    seen = []
    with pytest.raises(TransactionOwnershipError, match="joined unit"):
        with db.transaction(immediate=True, join=True):
            _write(db, "service")
            db.after_commit(lambda: seen.append("ran"))
    assert seen == []
    # The caller's transaction is still open and uncommitted; the owner
    # decides what to do next.
    assert db.in_transaction
    db.rollback()
    assert _committed(db) == []


def test_after_commit_is_refused_in_a_nested_unit_inside_a_joined_one(db):
    # A nested unit above a joined outer has no durable commit of its own to
    # hook into either; the refusal propagates to the whole joined stack.
    db.begin_immediate()
    _write(db, "caller")
    seen = []
    with pytest.raises(TransactionOwnershipError, match="joined unit"):
        with db.transaction(immediate=True, join=True):
            with db.transaction():
                db.after_commit(lambda: seen.append("ran"))
    assert seen == []
    db.rollback()
    assert _committed(db) == []


def test_a_failed_joined_unit_keeps_the_callers_provenance_snapshot(db):
    # A joined rollback releases its savepoint but leaves the caller's SQL
    # transaction (and the writes it already made) alive. The caller's
    # ``_species_provenance_before`` entries describe those earlier writes
    # and must survive the failed service call so the caller can still
    # record their history correctly.
    db.begin_immediate()
    _write(db, "caller")
    db._species_provenance_before[(5, 6)] = {"before": "caller"}
    with pytest.raises(RuntimeError), db.transaction(immediate=True, join=True):
        db._species_provenance_before[(7, 8)] = {"before": "joined service"}
        _write(db, "service")
        raise RuntimeError("boom")
    assert db._species_provenance_before == {(5, 6): {"before": "caller"}}
    assert db.in_transaction
    db.commit()
    assert _committed(db) == ["caller"]


def test_after_commit_still_runs_for_a_joined_unit_with_no_owner(db):
    # ``join=True`` entered with nothing open falls through to an owned
    # outermost unit, so its ``after_commit`` runs at the owned commit as
    # usual.
    seen = []
    with db.transaction(immediate=True, join=True):
        _write(db, "own")
        db.after_commit(lambda: seen.append(_committed(db)))
        assert seen == []
    assert seen == [["own"]]


def test_a_failed_joined_set_flag_preserves_history_and_undo_for_caller_species_removal(db, tmp_path):
    # End-to-end regression for the in-memory survival pinned above. A real
    # attributed species removal snapshots its before-state; a joined
    # ``PhotoReviewService.set_flag`` whose ``observed_flag`` is stale
    # raises ``FlagConflict`` and its savepoint rolls back. The caller
    # catches it, finishes the pending ``keyword_remove`` through
    # ``record_edit`` and commits. The persisted history row must carry the
    # original attribution, and Undo must restore it on the real
    # association.
    import json

    from services import photo_ingest, workspace_folders
    from services.edit_history import undo_last_edit
    from services.photo_review import FlagConflict, PhotoReviewService

    fid = workspace_folders.add_folder(db, str(tmp_path / "photos"))
    photo_id = photo_ingest.add_photo(db, fid, "bird.jpg", ".jpg", 100, 1.0)
    kid = db.add_keyword("Robin", is_species=True)
    db.tag_photo(photo_id, kid, source="accept", provenance={
        "method": "direct_visual", "reviewer": "Original reviewer",
        "reason": "Clear plumage markings",
    })
    before = db.species_provenance.state(photo_id, kid)
    assert before is not None and before["species_provenance"]
    db.commit()

    db.begin_immediate()
    db.untag_photo(photo_id, kid, _record_history=True)
    assert db.species_provenance.state(photo_id, kid) is None
    assert (photo_id, kid) in db._species_provenance_before
    with pytest.raises(FlagConflict):
        PhotoReviewService(db).set_flag(photo_id, "rejected", observed_flag="flagged")
    # The joined savepoint rolled back, but the caller's open transaction
    # and the snapshot that describes its earlier untag survive.
    assert db.in_transaction
    assert (photo_id, kid) in db._species_provenance_before

    db.record_edit(
        "keyword_remove", "Removed Robin", str(kid),
        [{"photo_id": photo_id, "old_value": "", "new_value": str(kid)}],
    )
    db.commit()

    with contextlib.closing(sqlite3.connect(db._db_path)) as other:
        row = other.execute(
            "SELECT species_provenance FROM edit_history_items ORDER BY id DESC LIMIT 1"
        ).fetchone()
    assert row is not None and row[0], "persisted history row is missing provenance"
    saved = json.loads(row[0])[str(kid)]["before"]
    assert saved is not None
    assert "direct_visual" in saved["species_provenance"]
    assert "Original reviewer" in saved["species_provenance"]

    undo_last_edit(db)
    assert db.species_provenance.state(photo_id, kid) == before
