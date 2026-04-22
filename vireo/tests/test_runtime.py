import json
import os
import stat


def test_write_runtime_json_atomic_and_locked_down(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    os.makedirs(tmp_path / ".vireo")

    from runtime import write_runtime_json

    write_runtime_json(
        port=54321,
        pid=12345,
        version="0.0.1",
        db_path="/tmp/x.db",
        token="tok",
        mode="headless",
    )

    path = tmp_path / ".vireo" / "runtime.json"
    assert path.exists()

    data = json.loads(path.read_text())
    assert data["port"] == 54321
    assert data["pid"] == 12345
    assert data["version"] == "0.0.1"
    assert data["db_path"] == "/tmp/x.db"
    assert data["token"] == "tok"
    assert data["mode"] == "headless"
    assert "started_at" in data  # ISO8601 timestamp

    # chmod 600 — only the user can read the token
    mode = stat.S_IMODE(os.stat(path).st_mode)
    assert mode == 0o600


def test_write_runtime_json_overwrites_existing(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    os.makedirs(tmp_path / ".vireo")
    (tmp_path / ".vireo" / "runtime.json").write_text('{"stale": true}')

    from runtime import write_runtime_json

    write_runtime_json(port=1, pid=2, version="v", db_path="/x", token="t", mode="gui")

    data = json.loads((tmp_path / ".vireo" / "runtime.json").read_text())
    assert "stale" not in data
    assert data["port"] == 1


def test_read_runtime_json_returns_dict(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    os.makedirs(tmp_path / ".vireo")
    (tmp_path / ".vireo" / "runtime.json").write_text(
        '{"port": 1234, "token": "abc"}'
    )

    from runtime import read_runtime_json

    data = read_runtime_json()
    assert data == {"port": 1234, "token": "abc"}


def test_read_runtime_json_missing_returns_none(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))

    from runtime import read_runtime_json

    assert read_runtime_json() is None


def test_read_runtime_json_malformed_returns_none(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    os.makedirs(tmp_path / ".vireo")
    (tmp_path / ".vireo" / "runtime.json").write_text("not json{{{")

    from runtime import read_runtime_json

    assert read_runtime_json() is None


def test_delete_runtime_json_is_idempotent(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    os.makedirs(tmp_path / ".vireo")
    (tmp_path / ".vireo" / "runtime.json").write_text("{}")

    from runtime import delete_runtime_json

    delete_runtime_json()
    assert not (tmp_path / ".vireo" / "runtime.json").exists()
    delete_runtime_json()  # second call should not raise


import http.server
import json as _json
import threading


class _HealthHandler(http.server.BaseHTTPRequestHandler):
    expected_token = "goodtoken"

    def do_GET(self):
        if self.path != "/api/v1/health":
            self.send_response(404)
            self.end_headers()
            return
        if self.headers.get("X-Vireo-Token") != self.expected_token:
            self.send_response(401)
            self.end_headers()
            return
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"status":"ok"}')

    def log_message(self, *a, **kw):  # silence
        pass


def _start_fake_server(token):
    _HealthHandler.expected_token = token
    server = http.server.HTTPServer(("127.0.0.1", 0), _HealthHandler)
    port = server.server_address[1]
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    return server, port


def test_guard_no_file_returns_proceed(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    from runtime import check_single_instance

    assert check_single_instance() == ("proceed", None)


def test_guard_healthy_peer_returns_conflict(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    os.makedirs(tmp_path / ".vireo")

    server, port = _start_fake_server("goodtoken")
    try:
        (tmp_path / ".vireo" / "runtime.json").write_text(_json.dumps({
            "port": port, "pid": 99999, "token": "goodtoken",
        }))
        from runtime import check_single_instance
        status, info = check_single_instance()
        assert status == "conflict"
        assert info["port"] == port
        assert info["pid"] == 99999
    finally:
        server.shutdown()


def test_guard_stale_file_is_cleaned_and_proceeds(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    os.makedirs(tmp_path / ".vireo")

    # Port 1 is almost certainly not bound by anything listening.
    (tmp_path / ".vireo" / "runtime.json").write_text(_json.dumps({
        "port": 1, "pid": 99999, "token": "x",
    }))

    from runtime import check_single_instance
    status, info = check_single_instance()
    assert status == "proceed"
    assert not (tmp_path / ".vireo" / "runtime.json").exists()


def test_guard_malformed_file_is_cleaned_and_proceeds(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    os.makedirs(tmp_path / ".vireo")
    (tmp_path / ".vireo" / "runtime.json").write_text("not json")

    from runtime import check_single_instance
    status, info = check_single_instance()
    assert status == "proceed"
    assert not (tmp_path / ".vireo" / "runtime.json").exists()
