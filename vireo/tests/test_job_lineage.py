"""Who caused a job, what it follows, and how Vireo's own runs are kept.

See ``job_runner.lineage`` (origin, parent, reason) and
``JobHistory.system_disposition`` / ``prune_operations`` (retention).
"""

import json
import sqlite3
import threading

import pytest
import schema
from canonical_schema import SCHEMA_VERSION
from db import Database
from jobs import JobLineage, JobRunner
from testing.waits import synchronization_timeout
from wait import wait_for_job_via_runner


@pytest.fixture
def runner_db(tmp_path):
    db = Database(str(tmp_path / "lineage.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    runner = JobRunner(db=db)
    yield runner, db, ws_id
    runner.shutdown()
    db.close()


def _run(runner, job_type, result, *, ws_id, lineage=None, config=None):
    def work(job):
        if isinstance(result, Exception):
            raise result
        return result

    job_id = runner.start(
        job_type, work, workspace_id=ws_id, lineage=lineage, config=config,
        pausable=False,
    )
    return wait_for_job_via_runner(runner, job_id, wait_for_history=True)


def _rows(db, job_type):
    db.conn.row_factory = sqlite3.Row
    return [dict(r) for r in db.conn.execute(
        "SELECT * FROM job_history WHERE type = ? ORDER BY started_at", (job_type,),
    ).fetchall()]


SYSTEM = JobLineage.system("Regular check")


# -- lineage on the job and its row ----------------------------------------


def test_default_job_is_the_users_with_no_parent(runner_db):
    runner, db, ws_id = runner_db
    job = _run(runner, "scan", {"ok": True}, ws_id=ws_id)
    assert (job["origin"], job["parent_job_id"], job["reason"]) == ("user", None, None)
    [row] = _rows(db, "scan")
    assert (row["origin"], row["parent_job_id"], row["reason"]) == ("user", None, None)


def test_system_job_records_origin_and_reason(runner_db):
    runner, db, ws_id = runner_db
    job = _run(runner, "maintenance", {"removed": 3}, ws_id=ws_id, lineage=SYSTEM)
    assert job["origin"] == "system" and job["reason"] == "Regular check"
    [row] = _rows(db, "maintenance")
    assert row["origin"] == "system" and row["reason"] == "Regular check"


def test_system_job_row_exists_while_it_runs(runner_db):
    """No longer ephemeral: a crash mid-run leaves a row the startup sweep fails."""
    runner, db, ws_id = runner_db
    started, release = threading.Event(), threading.Event()

    def work(job):
        started.set()
        release.wait(synchronization_timeout(5))
        return {}

    job_id = runner.start("backfill", work, workspace_id=ws_id, lineage=SYSTEM,
                         pausable=False)
    try:
        assert started.wait(synchronization_timeout(5))
        # The running row is written before the work function is called.
        row = db.conn.execute(
            "SELECT status, origin FROM job_history WHERE id = ?", (job_id,),
        ).fetchone()
        assert tuple(row) == ("running", "system")
    finally:
        release.set()
        wait_for_job_via_runner(runner, job_id, wait_for_history=True)


def test_follow_up_inherits_its_live_parents_origin(runner_db):
    runner, db, ws_id = runner_db
    release = threading.Event()
    parent = runner.start(
        "import", lambda job: release.wait(synchronization_timeout(5)) and {},
        workspace_id=ws_id, lineage=SYSTEM, pausable=False,
    )
    try:
        child = _run(
            runner, "pipeline", {"ok": True}, ws_id=ws_id,
            lineage=JobLineage.following(parent),
        )
    finally:
        release.set()
        wait_for_job_via_runner(runner, parent, wait_for_history=True)
    assert child["parent_job_id"] == parent
    assert child["origin"] == "system"


def test_follow_up_inherits_a_finished_parents_origin_from_history(runner_db):
    runner, db, ws_id = runner_db
    parent = _run(runner, "import", {"ok": True}, ws_id=ws_id)
    with runner._lock:
        runner._jobs.pop(parent["id"])
    child = _run(
        runner, "move-folder", {"ok": True}, ws_id=ws_id,
        lineage=JobLineage(origin="system", parent_job_id=parent["id"]),
    )
    # The parent's origin wins: a follow-up of your import is your work.
    assert child["origin"] == "user"
    assert _rows(db, "move-folder")[0]["parent_job_id"] == parent["id"]


def test_queued_pipeline_carries_lineage_on_its_row_and_view(runner_db):
    runner, db, ws_id = runner_db
    release = threading.Event()
    blockers = [
        runner.enqueue_pipeline(
            lambda job: release.wait(synchronization_timeout(5)) and {},
            workspace_id=ws_id,
        )
        for _ in range(2)
    ]
    try:
        queued = runner.enqueue_pipeline(
            lambda job: {}, workspace_id=ws_id,
            lineage=JobLineage.following("import-1"),
        )
        view = runner.get(queued)
        assert view["status"] == "queued"
        assert (view["origin"], view["parent_job_id"]) == ("user", "import-1")
        row = db.conn.execute(
            "SELECT origin, parent_job_id FROM job_history WHERE id = ?", (queued,),
        ).fetchone()
        assert tuple(row) == ("user", "import-1")
    finally:
        release.set()
        for job_id in blockers:
            wait_for_job_via_runner(runner, job_id, wait_for_history=True)
    job = wait_for_job_via_runner(runner, queued, wait_for_history=True)
    assert job["parent_job_id"] == "import-1"


def test_unknown_origin_is_rejected():
    with pytest.raises(ValueError):
        JobLineage(origin="robot")


# -- retention of Vireo's own runs ------------------------------------------


def test_routine_system_run_that_changed_nothing_leaves_no_row(runner_db):
    runner, db, ws_id = runner_db
    _run(runner, "check", {"found": 0, "noop": True}, ws_id=ws_id, lineage=SYSTEM)
    assert _rows(db, "check") == []


def test_users_run_that_changed_nothing_is_kept(runner_db):
    runner, db, ws_id = runner_db
    _run(runner, "check", {"found": 0, "noop": True}, ws_id=ws_id)
    assert len(_rows(db, "check")) == 1


def test_no_op_after_a_failure_is_kept_then_later_no_ops_drop(runner_db):
    runner, db, ws_id = runner_db
    _run(runner, "check", RuntimeError("NAS offline"), ws_id=ws_id, lineage=SYSTEM)
    _run(runner, "check", {"noop": True}, ws_id=ws_id, lineage=SYSTEM)
    _run(runner, "check", {"noop": True}, ws_id=ws_id, lineage=SYSTEM)
    assert [r["status"] for r in _rows(db, "check")] == ["failed", "completed"]


def test_no_op_after_a_run_that_found_something_is_kept(runner_db):
    """The latest row must not keep claiming 12 missing photos once they're back."""
    runner, db, ws_id = runner_db
    _run(runner, "check", {"missing": 12}, ws_id=ws_id, lineage=SYSTEM)
    _run(runner, "check", {"missing": 0, "noop": True}, ws_id=ws_id, lineage=SYSTEM)
    assert len(_rows(db, "check")) == 2


def test_repeated_system_failures_collapse_into_one_row(runner_db):
    runner, db, ws_id = runner_db
    first = _run(runner, "check", RuntimeError("NAS offline"), ws_id=ws_id, lineage=SYSTEM)
    _run(runner, "check", RuntimeError("NAS offline"), ws_id=ws_id, lineage=SYSTEM)
    last = _run(runner, "check", RuntimeError("NAS offline"), ws_id=ws_id, lineage=SYSTEM)
    [row] = _rows(db, "check")
    assert row["id"] == last["id"]
    assert row["repeat_count"] == 3
    assert row["repeat_since"] == first["started_at"]
    assert json.loads(row["result"])["error"] == "NAS offline"


def test_a_different_error_starts_a_new_row(runner_db):
    runner, db, ws_id = runner_db
    _run(runner, "check", RuntimeError("NAS offline"), ws_id=ws_id, lineage=SYSTEM)
    _run(runner, "check", RuntimeError("timed out"), ws_id=ws_id, lineage=SYSTEM)
    rows = _rows(db, "check")
    assert [json.loads(r["result"])["error"] for r in rows] == ["NAS offline", "timed out"]
    assert [r["repeat_count"] for r in rows] == [1, 1]


def test_repeated_user_failures_stay_separate(runner_db):
    runner, db, ws_id = runner_db
    for _ in range(2):
        _run(runner, "check", RuntimeError("NAS offline"), ws_id=ws_id)
    assert [r["repeat_count"] for r in _rows(db, "check")] == [1, 1]


def test_system_failures_of_different_scopes_do_not_collapse(runner_db):
    runner, db, ws_id = runner_db
    for folder in (1, 2):
        _run(
            runner, "check", RuntimeError("offline"), ws_id=ws_id,
            lineage=JobLineage.system("Regular check", scope=f"folder:{folder}"),
            config={"folder_id": folder},
        )
    assert len(_rows(db, "check")) == 2


def test_task_identity_ignores_display_config(runner_db):
    """Renaming the workspace changes config text, not which task this is."""
    runner, db, ws_id = runner_db
    first = _run(runner, "check", RuntimeError("offline"), ws_id=ws_id,
                 lineage=SYSTEM, config={"workspace_name": "Birds"})
    last = _run(runner, "check", RuntimeError("offline"), ws_id=ws_id,
                lineage=SYSTEM, config={"workspace_name": "Birds 2026"})
    [row] = _rows(db, "check")
    assert (row["id"], row["repeat_count"], row["repeat_since"]) == (
        last["id"], 2, first["started_at"],
    )


def test_all_clear_after_a_rename_still_replaces_the_actionable_result(runner_db):
    runner, db, ws_id = runner_db
    _run(runner, "check", {"new_count": 4}, ws_id=ws_id,
         lineage=SYSTEM, config={"workspace_name": "Birds"})
    _run(runner, "check", {"new_count": 0, "noop": True}, ws_id=ws_id,
         lineage=SYSTEM, config={"workspace_name": "Birds 2026"})
    rows = _rows(db, "check")
    assert [json.loads(r["result"])["new_count"] for r in rows] == [4, 0]
    assert all(r["task_scope"] is None for r in rows)


def test_system_successes_that_found_something_never_collapse(runner_db):
    """Equal summaries can hide different details (which folder was offline)."""
    runner, db, ws_id = runner_db
    _run(runner, "check", {"new_count": 0, "unreachable_roots": ["/volume-a"]},
         ws_id=ws_id, lineage=SYSTEM)
    _run(runner, "check", {"new_count": 0, "unreachable_roots": ["/volume-b"]},
         ws_id=ws_id, lineage=SYSTEM)
    rows = _rows(db, "check")
    assert [json.loads(r["result"])["unreachable_roots"] for r in rows] == [
        ["/volume-a"], ["/volume-b"],
    ]
    assert [r["repeat_count"] for r in rows] == [1, 1]


def test_a_success_breaks_a_failure_streak(runner_db):
    runner, db, ws_id = runner_db
    _run(runner, "check", RuntimeError("offline"), ws_id=ws_id, lineage=SYSTEM)
    _run(runner, "check", {"missing": 1}, ws_id=ws_id, lineage=SYSTEM)
    _run(runner, "check", RuntimeError("offline"), ws_id=ws_id, lineage=SYSTEM)
    assert [r["status"] for r in _rows(db, "check")] == ["failed", "completed", "failed"]


# -- pruning by operation ----------------------------------------------------


def _insert(db, job_id, *, ws_id, started_at, origin="user", parent=None, status="completed"):
    db.conn.execute(
        "INSERT INTO job_history (id, type, status, started_at, workspace_id, "
        "origin, parent_job_id) VALUES (?, 'import', ?, ?, ?, ?, ?)",
        (job_id, status, started_at, ws_id, origin, parent),
    )


def test_pruning_keeps_an_operation_and_its_follow_ups_together(runner_db, monkeypatch):
    from job_runner import history

    runner, db, ws_id = runner_db
    monkeypatch.setattr(history, "HISTORY_OPERATIONS_PER_ORIGIN", 2)
    _insert(db, "old-import", ws_id=ws_id, started_at="2026-01-01T00:00:00")
    _insert(db, "old-process", ws_id=ws_id, started_at="2026-01-01T00:01:00",
            parent="old-import")
    _insert(db, "mid-import", ws_id=ws_id, started_at="2026-01-02T00:00:00")
    # A follow-up newer than every root still belongs to its old import.
    _insert(db, "mid-move", ws_id=ws_id, started_at="2026-02-01T00:00:00",
            parent="mid-import")
    db.conn.commit()
    _run(runner, "scan", {"ok": True}, ws_id=ws_id)
    ids = {r[0] for r in db.conn.execute("SELECT id FROM job_history")}
    assert "old-import" not in ids and "old-process" not in ids
    assert {"mid-import", "mid-move"} <= ids


def test_pruning_never_touches_an_operation_with_live_work(runner_db, monkeypatch):
    from job_runner import history

    runner, db, ws_id = runner_db
    monkeypatch.setattr(history, "HISTORY_OPERATIONS_PER_ORIGIN", 1)
    _insert(db, "old-import", ws_id=ws_id, started_at="2026-01-01T00:00:00")
    _insert(db, "queued-process", ws_id=ws_id, started_at="2026-01-01T00:01:00",
            parent="old-import", status="queued")
    db.conn.commit()
    _run(runner, "scan", {"ok": True}, ws_id=ws_id)
    ids = {r[0] for r in db.conn.execute("SELECT id FROM job_history")}
    assert {"old-import", "queued-process"} <= ids


def test_system_runs_have_their_own_quota(runner_db, monkeypatch):
    from job_runner import history

    runner, db, ws_id = runner_db
    monkeypatch.setattr(history, "HISTORY_OPERATIONS_PER_ORIGIN", 1)
    _insert(db, "users-import", ws_id=ws_id, started_at="2026-01-01T00:00:00")
    db.conn.commit()
    _run(runner, "check", {"missing": 1}, ws_id=ws_id, lineage=SYSTEM)
    _run(runner, "check", {"missing": 2}, ws_id=ws_id, lineage=SYSTEM)
    ids = {r[0] for r in db.conn.execute("SELECT id FROM job_history")}
    assert "users-import" in ids
    assert len(_rows(db, "check")) == 1


# -- migration ---------------------------------------------------------------


def test_migration_31_backfills_parent_and_origin(tmp_path):
    path = str(tmp_path / "pre-lineage.db")
    Database(path).close()
    with sqlite3.connect(path) as conn:
        conn.execute("DROP INDEX idx_job_history_parent")
        for column in ("origin", "parent_job_id", "reason", "repeat_count",
                       "repeat_since", "task_scope"):
            conn.execute(f"ALTER TABLE job_history DROP COLUMN {column}")
        conn.executemany(
            "INSERT INTO job_history (id, type, status, config) VALUES (?, ?, 'completed', ?)",
            [
                ("import-1", "import", "{}"),
                ("pipeline-1", "pipeline", json.dumps({"chained_from": "import-1"})),
                ("walk-1", "new_images_walk", "{}"),
                ("maint-1", "retained-cache-maintenance", None),
                ("junk-1", "scan", "not json"),
            ],
        )
        conn.execute("PRAGMA user_version = 30")
        conn.commit()
        schema._apply_pending(conn)
        assert conn.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION
        rows = {
            r[0]: r[1:] for r in conn.execute(
                "SELECT id, origin, parent_job_id, repeat_count FROM job_history",
            )
        }
    assert rows == {
        "import-1": ("user", None, 1),
        "pipeline-1": ("user", "import-1", 1),
        "walk-1": ("system", None, 1),
        "maint-1": ("system", None, 1),
        "junk-1": ("user", None, 1),
    }


# -- checkpoints never resurrect a finished row ------------------------------


@pytest.mark.parametrize("outcome", ["noop-drop", "failure-collapse"])
def test_stale_checkpoint_cannot_resurrect_a_finished_system_row(runner_db, outcome):
    """A checkpoint that snapshotted the job while it ran must not re-create its row.

    Barrier: the checkpoint captures the running job and parks before
    writing; the job then finishes. Its final write must wait for the
    checkpoint, so no stale ``running`` row survives to be failed as
    "interrupted" at the next start.
    """
    runner, db, ws_id = runner_db
    if outcome == "failure-collapse":
        _run(runner, "check", RuntimeError("offline"), ws_id=ws_id, lineage=SYSTEM)
    started, finish = threading.Event(), threading.Event()
    captured, release = threading.Event(), threading.Event()

    def work(job):
        started.set()
        finish.wait(synchronization_timeout(5))
        if outcome == "failure-collapse":
            raise RuntimeError("offline")
        return {"noop": True}

    job_id = runner.start("check", work, workspace_id=ws_id, lineage=SYSTEM,
                         pausable=False)
    assert started.wait(synchronization_timeout(5))
    write = runner._history.write_checkpoints

    def parked_write(snapshots):
        captured.set()
        release.wait(synchronization_timeout(5))
        return write(snapshots)

    runner._history.write_checkpoints = parked_write
    checkpoint = threading.Thread(target=runner.checkpoint_live_jobs)
    checkpoint.start()
    try:
        assert captured.wait(synchronization_timeout(5))
        finish.set()
        live = runner._jobs[job_id]
        for _ in range(200):
            if live["status"] != "running":
                break
            release.wait(0.01)
        assert live["status"] in ("completed", "failed")
        # The final write waits for the parked checkpoint.
        release.wait(0.3)
        assert not live["_persisted"]
    finally:
        release.set()
        checkpoint.join(synchronization_timeout(5))
        runner._history.write_checkpoints = write
    wait_for_job_via_runner(runner, job_id, wait_for_history=True)
    rows = _rows(db, "check")
    assert all(r["status"] != "running" for r in rows), rows
    if outcome == "noop-drop":
        assert rows == []
    else:
        [row] = rows
        assert (row["id"], row["status"], row["repeat_count"]) == (job_id, "failed", 2)


# -- reading history -----------------------------------------------------------


def test_newer_system_rows_do_not_hide_older_user_operations(runner_db):
    runner, db, ws_id = runner_db
    _insert(db, "users-import", ws_id=ws_id, started_at="2026-01-01T00:00:00")
    _insert(db, "users-process", ws_id=ws_id, started_at="2026-01-01T00:05:00",
            parent="users-import")
    for i in range(150):
        _insert(db, f"check-{i}", ws_id=ws_id if i % 2 else None,
                started_at=f"2026-02-01T00:{i // 60:02d}:{i % 60:02d}", origin="system")
    db.conn.commit()
    # A mixed window is the newest rows of either origin, capped at limit.
    mixed = runner.get_history(db, limit=100)
    assert len(mixed) == 100 and all(r["origin"] == "system" for r in mixed)
    assert len(runner.get_history(db, limit=1)) == 1
    # "Started by you" reads the user's own window, follow-up included.
    users = runner.get_history(db, limit=100, origin="user")
    assert [r["id"] for r in users] == ["users-process", "users-import"]
    vireos = runner.get_history(db, limit=100, origin="system")
    assert len(vireos) == 100 and any(r["workspace_id"] is None for r in vireos)


def test_history_api_reads_one_origin_and_rejects_others(tmp_path, monkeypatch):
    import config as cfg
    import models
    from app import create_app

    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    monkeypatch.setattr(models, "DEFAULT_MODELS_DIR", str(tmp_path / "vireo-models"))
    monkeypatch.setattr(models, "CONFIG_PATH", str(tmp_path / "models.json"))
    db_path = str(tmp_path / "api.db")
    db = Database(db_path)
    ws_id = db.ensure_default_workspace()
    _insert(db, "users-scan", ws_id=ws_id, started_at="2026-01-01T00:00:00")
    _insert(db, "vireo-check", ws_id=None, started_at="2026-01-02T00:00:00", origin="system")
    db.conn.commit()
    db.close()
    app = create_app(db_path=db_path, thumb_cache_dir=str(tmp_path / "thumbs"))
    client = app.test_client()
    assert [r["id"] for r in client.get("/api/jobs/history?limit=1").get_json()] == ["vireo-check"]
    assert [r["id"] for r in client.get(
        "/api/jobs/history?limit=1&origin=user").get_json()] == ["users-scan"]
    assert client.get("/api/jobs/history?origin=robot").status_code == 400


def test_automatic_new_images_walk_that_skipped_local_copies_is_kept():
    from web.workspaces import _new_images_walk_job_payload

    found_nothing = {"checked": 40, "found": 0}
    assert _new_images_walk_job_payload(found_nothing, {})["noop"] is True
    assert _new_images_walk_job_payload(
        found_nothing, {"local_copy_excluded": ["/Volumes/NAS/trip"]},
    )["noop"] is False
    assert _new_images_walk_job_payload(
        found_nothing, {"unreachable_roots": ["/Volumes/NAS"]},
    )["noop"] is False
