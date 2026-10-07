"""Unit tests for the shared background-job launch prologue."""

from db import Database
from flask import Flask, jsonify
from web.background_jobs import JobLaunch, make_background_job


class FakeRunner:
    def __init__(self):
        self.calls = []

    def start(self, job_type, work, config=None, **kwargs):
        self.calls.append((job_type, work, config, kwargs))
        return f"{job_type}-1"


class FakeDb:
    def __init__(self, db_path):
        self.db_path = db_path
        self._active_workspace_id = None

    def set_active_workspace(self, workspace_id):
        self._active_workspace_id = workspace_id

    @property
    def active_workspace_id(self):
        return self._active_workspace_id


def _make_app(runner, workspace_id=7, db_factory=FakeDb):
    app = Flask(__name__)
    request_db = FakeDb("request")
    request_db._active_workspace_id = workspace_id
    background_job = make_background_job(
        lambda: runner, lambda: request_db, "/tmp/x.db", db_factory
    )
    return app, background_job


def test_decorator_injects_launch_context_and_returns_job_id():
    runner = FakeRunner()
    app, background_job = _make_app(runner)
    seen = {}

    @app.route("/api/jobs/demo", methods=["POST"])
    @background_job
    def api_job_demo(ctx):
        seen["ctx"] = ctx

        def work(job):
            return {"ok": True}

        return ctx.start("demo", work, config={"a": 1}, pausable=True)

    resp = app.test_client().post("/api/jobs/demo")

    assert resp.status_code == 200
    assert resp.get_json() == {"job_id": "demo-1"}
    assert isinstance(seen["ctx"], JobLaunch)
    assert seen["ctx"].runner is runner
    assert seen["ctx"].workspace_id == 7
    job_type, work, config, kwargs = runner.calls[0]
    assert job_type == "demo"
    assert work({}) == {"ok": True}
    assert config == {"a": 1}
    assert kwargs == {"workspace_id": 7, "pausable": True}


def test_decorator_preserves_view_name_and_url_arguments():
    runner = FakeRunner()
    app, background_job = _make_app(runner)

    @app.route("/api/folders/<int:folder_id>/rescan", methods=["POST"])
    @background_job
    def api_folder_rescan(ctx, folder_id):
        return ctx.start("scan", lambda job: None, config={"folder": folder_id})

    assert "api_folder_rescan" in app.view_functions
    resp = app.test_client().post("/api/folders/42/rescan")
    assert resp.get_json() == {"job_id": "scan-1"}
    assert runner.calls[0][2] == {"folder": 42}


def test_route_can_reject_request_without_starting_a_job():
    runner = FakeRunner()
    app, background_job = _make_app(runner)

    @app.route("/api/jobs/demo", methods=["POST"])
    @background_job
    def api_job_demo(ctx):
        return jsonify({"error": "nope"}), 400

    resp = app.test_client().post("/api/jobs/demo")
    assert resp.status_code == 400
    assert runner.calls == []


def test_start_adds_extra_response_keys_and_allows_workspace_override():
    runner = FakeRunner()
    ctx = JobLaunch(runner, 3, "/tmp/x.db", FakeDb)
    app = Flask(__name__)
    with app.app_context():
        resp = ctx.start(
            "export", lambda job: None, extra={"total": 5}, workspace_id=9
        )
        assert resp.get_json() == {"job_id": "export-1", "total": 5}
    assert runner.calls[0][3] == {"workspace_id": 9}


def test_start_job_returns_id_and_defaults_workspace():
    runner = FakeRunner()
    ctx = JobLaunch(runner, 3, "/tmp/x.db", FakeDb)
    assert ctx.start_job("sync", lambda job: None) == "sync-1"
    assert runner.calls[0][3] == {"workspace_id": 3}


def test_thread_db_opens_worker_connection_bound_to_workspace():
    ctx = JobLaunch(FakeRunner(), 11, "/tmp/x.db", FakeDb)
    db = ctx.thread_db()
    assert db.db_path == "/tmp/x.db"
    assert db._active_workspace_id == 11


def test_thread_db_leaves_workspace_unset_when_request_had_none():
    ctx = JobLaunch(FakeRunner(), None, "/tmp/x.db", FakeDb)
    assert ctx.thread_db()._active_workspace_id is None


def test_thread_db_with_real_database(tmp_path):
    db_path = str(tmp_path / "t.db")
    request_db = Database(db_path)
    ws_id = request_db._active_workspace_id
    request_db.conn.close()

    ctx = JobLaunch(FakeRunner(), ws_id, db_path, Database)
    worker = ctx.thread_db()
    try:
        assert worker._ws_id() == ws_id
    finally:
        worker.conn.close()


def test_runner_is_looked_up_per_request():
    first, second = FakeRunner(), FakeRunner()
    holder = {"runner": first}
    app = Flask(__name__)
    request_db = FakeDb("request")
    background_job = make_background_job(
        lambda: holder["runner"], lambda: request_db, "/tmp/x.db", FakeDb
    )

    @app.route("/api/jobs/demo", methods=["POST"])
    @background_job
    def api_job_demo(ctx):
        return ctx.start("demo", lambda job: None)

    client = app.test_client()
    client.post("/api/jobs/demo")
    holder["runner"] = second
    client.post("/api/jobs/demo")
    assert len(first.calls) == 1
    assert len(second.calls) == 1
