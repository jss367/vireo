"""Exercise the same stdio command Settings checks, against a scratch catalog."""

import json
import sys
import threading
from contextlib import asynccontextmanager, nullcontext
from pathlib import Path
from types import SimpleNamespace

import anyio
import mcp_diagnostics as diagnostics
import pytest
from mcp_server import BAD_TOKEN_MESSAGE, NOT_RUNNING_MESSAGE
from werkzeug.serving import make_server

TOKEN = "test-token-123"
APP = Path(__file__).resolve().parents[1] / "app.py"


@pytest.fixture
def served_catalog(app_and_db, monkeypatch, tmp_path):
    app, db = app_and_db
    http = make_server("127.0.0.1", 0, app, threaded=True)
    thread = threading.Thread(target=http.serve_forever, daemon=True)
    thread.start()
    # No real runtime credentials or photos are used, even by a child process.
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("USERPROFILE", str(tmp_path))
    monkeypatch.setenv("VIREO_URL", f"http://127.0.0.1:{http.server_port}")
    monkeypatch.setenv("VIREO_TOKEN", TOKEN)
    monkeypatch.setattr(diagnostics, "server_command", lambda: [sys.executable, str(APP), "--mcp"])
    try:
        yield app, db
    finally:
        from testing.waits import synchronization_timeout
        http.shutdown()
        thread.join(timeout=synchronization_timeout(10))
        http.server_close()


def test_check_endpoint_initializes_stdio_and_reads_only(served_catalog):
    app, db = served_catalog
    reads = []

    @app.before_request
    def record_requests():
        from flask import request
        reads.append((request.method, request.path))

    before = db.photos.list_page()
    workspace = db.active_workspace_id
    response = app.test_client().post("/api/system/mcp-check", headers={"X-Vireo-Token": TOKEN})
    result = response.get_json()
    assert response.status_code == 200
    assert result["ok"], result
    assert result["status"]["active_workspace"]["id"] == workspace
    assert result["status"]["photo_count"] == 3
    assert result["folders"]
    assert result["folders"][0]["path"]
    assert db.photos.list_page() == before
    assert db.active_workspace_id == workspace
    assert all(method == "GET" for method, path in reads if path != "/api/system/mcp-check")
    assert "connected" not in result
    assert TOKEN not in json.dumps(result)


@pytest.mark.parametrize("transfer_running", [False, True])
def test_check_neither_reserves_nor_is_blocked_by_workspace_transfer(app_and_db, monkeypatch, transfer_running):
    app, db = app_and_db
    runner = app._job_runner
    reservation = runner.workspace_mutation(db.active_workspace_id, exclusive=True, label="NAS transfer")
    calls = []
    reserve = runner.workspace_mutation

    def spy(*args, **kwargs):
        calls.append((args, kwargs))
        return reserve(*args, **kwargs)

    monkeypatch.setattr(runner, "workspace_mutation", spy)
    monkeypatch.setattr(diagnostics, "check_connection", lambda: {"ok": True})
    with reservation if transfer_running else nullcontext():
        response = app.test_client().post("/api/system/mcp-check", headers={"X-Vireo-Token": TOKEN})
    assert response.status_code == 200
    assert response.get_json() == {"ok": True}
    assert calls == []


@pytest.mark.parametrize("override", ["wrong-token", "not-running"])
def test_catalog_failure_is_actionable_and_never_echoes_credentials(served_catalog, monkeypatch, override):
    if override == "wrong-token":
        monkeypatch.setenv("VIREO_TOKEN", "never-echo-this-secret")
        expected = BAD_TOKEN_MESSAGE
    else:
        # A missing runtime record is deterministic, unlike racing a free port.
        monkeypatch.delenv("VIREO_URL")
        monkeypatch.delenv("VIREO_TOKEN")
        expected = NOT_RUNNING_MESSAGE
    result = diagnostics.check_connection()
    assert not result["ok"]
    assert expected in result["message"]
    assert "never-echo-this-secret" not in json.dumps(result)


def test_missing_executable_reports_setup_remedy(monkeypatch, tmp_path):
    monkeypatch.setattr(diagnostics, "server_command", lambda: [str(tmp_path / "missing executable"), "--mcp"])
    result = diagnostics.check_connection()
    assert result == {"ok": False, "message": diagnostics.STARTUP_MESSAGE}


def test_check_timeout_is_bounded_and_explained(monkeypatch):
    @asynccontextmanager
    async def stalled_server(*args, **kwargs):
        await anyio.sleep_forever()
        yield  # pragma: no cover - timeout must fire before a handshake

    monkeypatch.setattr(diagnostics, "stdio_client", stalled_server)
    assert diagnostics.check_connection(timeout=0.01) == {
        "ok": False, "message": diagnostics.TIMEOUT_MESSAGE,
    }


@pytest.mark.parametrize("payload", ["runtime-token-secret", '{"error":"runtime-token-secret"}'])
def test_arbitrary_protocol_errors_are_not_shown(monkeypatch, payload):
    @asynccontextmanager
    async def broken_server(*args, **kwargs):
        raise ExceptionGroup("runtime-token-secret", [ValueError(payload)])
        yield  # pragma: no cover

    monkeypatch.setattr(diagnostics, "stdio_client", broken_server)
    result = diagnostics.check_connection()
    assert result == {"ok": False, "message": diagnostics.STARTUP_MESSAGE}
    assert "runtime-token-secret" not in json.dumps(result)


def test_check_requires_authentication_before_starting_child(app_and_db, monkeypatch):
    app, _ = app_and_db

    def forbidden():
        pytest.fail("unauthenticated request started a process")

    monkeypatch.setattr(diagnostics, "check_connection", forbidden)
    app.config["BROWSER_AUTH_ENABLED"] = True
    client = app.test_client()
    assert client.post("/api/system/mcp-check").status_code == 401
    client.get("/settings")
    assert client.post("/api/system/mcp-check").status_code == 403


def test_crashed_child_never_exposes_its_output(monkeypatch, tmp_path):
    child = tmp_path / "broken server.py"
    child.write_text("import sys\nprint('runtime-token-secret', file=sys.stderr)\nsys.exit(1)\n")
    monkeypatch.setattr(diagnostics, "server_command", lambda: [sys.executable, str(child)])
    result = diagnostics.check_connection()
    assert result == {"ok": False, "message": diagnostics.STARTUP_MESSAGE}


def test_malformed_tool_response_has_safe_diagnostic(monkeypatch):
    class Session:
        async def call_tool(self, name, arguments):
            return SimpleNamespace(is_error=True, content=[SimpleNamespace(type="text", text="runtime-token-secret")])

    with pytest.raises(ValueError, match="read-only catalog check failed"):
        anyio.run(diagnostics._read_tool, Session(), "vireo_status")
