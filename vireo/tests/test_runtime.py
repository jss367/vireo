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

    # Port 1 is almost certainly not bound by anything listening. PID 0 is
    # treated as unconditionally dead by the liveness check, so the guard
    # must classify this file as stale.
    (tmp_path / ".vireo" / "runtime.json").write_text(_json.dumps({
        "port": 1, "pid": 0, "token": "x",
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


class _Always500Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(500)
        self.end_headers()

    def log_message(self, *a, **kw):  # silence
        pass


class _Always404Handler(http.server.BaseHTTPRequestHandler):
    """Simulates an unrelated local HTTP service that reused our port."""

    def do_GET(self):
        self.send_response(404)
        self.end_headers()

    def log_message(self, *a, **kw):  # silence
        pass


def _start_fake_500_server():
    server = http.server.HTTPServer(("127.0.0.1", 0), _Always500Handler)
    port = server.server_address[1]
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    return server, port


def _start_fake_404_server():
    server = http.server.HTTPServer(("127.0.0.1", 0), _Always404Handler)
    port = server.server_address[1]
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    return server, port


def test_guard_401_is_stale_and_proceeds(tmp_path, monkeypatch):
    """A peer that responds 401 must be treated as stale, not a live Vireo.

    The token in runtime.json is always the one our own writer emitted for
    the running instance, so a 401 implies the port was reused by an
    unrelated local service (or state drift). Refusing to start on any
    HTTP response caused a false `already_running` in that case.
    """
    monkeypatch.setenv("HOME", str(tmp_path))
    os.makedirs(tmp_path / ".vireo")

    server, port = _start_fake_server("goodtoken")
    try:
        runtime_path = tmp_path / ".vireo" / "runtime.json"
        runtime_path.write_text(_json.dumps({
            "port": port, "pid": 77777, "token": "WRONG-TOKEN",
        }))
        from runtime import check_single_instance
        status, _info = check_single_instance()
        assert status == "proceed"
        assert not runtime_path.exists()
    finally:
        server.shutdown()


def test_guard_peer_returning_500_is_stale_and_proceeds(tmp_path, monkeypatch):
    """A 500 response is not proof of a live Vireo peer — treat as stale."""
    monkeypatch.setenv("HOME", str(tmp_path))
    os.makedirs(tmp_path / ".vireo")

    server, port = _start_fake_500_server()
    try:
        runtime_path = tmp_path / ".vireo" / "runtime.json"
        runtime_path.write_text(_json.dumps({
            "port": port, "pid": 88888, "token": "anything",
        }))
        from runtime import check_single_instance
        status, _info = check_single_instance()
        assert status == "proceed"
        assert not runtime_path.exists()
    finally:
        server.shutdown()


def test_guard_booting_peer_preserves_runtime_json(tmp_path, monkeypatch):
    """A peer that wrote runtime.json but isn't listening on HTTP yet must
    not have its runtime.json deleted — that would break discovery for
    external callers even though the peer is still running.

    Reproduce by pointing runtime.json at a port where nobody is
    listening (probe will fail with connection-refused) while recording
    a live PID. The guard should report conflict and keep the file.
    """
    monkeypatch.setenv("HOME", str(tmp_path))
    os.makedirs(tmp_path / ".vireo")

    runtime_path = tmp_path / ".vireo" / "runtime.json"
    runtime_path.write_text(_json.dumps({
        "port": 1,  # nothing listening → connection refused
        "pid": os.getpid(),  # live PID → peer is booting, not dead
        "token": "anything",
    }))

    from runtime import check_single_instance
    status, info = check_single_instance()
    assert status == "conflict"
    assert info["port"] == 1
    assert info["pid"] == os.getpid()
    # The live peer's runtime.json must remain intact.
    assert runtime_path.exists()


def test_guard_unrelated_404_service_is_stale_and_proceeds(tmp_path, monkeypatch):
    """If the advertised port is now answered by an unrelated HTTP service,
    treat runtime.json as stale so Vireo can start on a fresh port."""
    monkeypatch.setenv("HOME", str(tmp_path))
    os.makedirs(tmp_path / ".vireo")

    server, port = _start_fake_404_server()
    try:
        runtime_path = tmp_path / ".vireo" / "runtime.json"
        runtime_path.write_text(_json.dumps({
            "port": port, "pid": 66666, "token": "anything",
        }))
        from runtime import check_single_instance
        status, _info = check_single_instance()
        assert status == "proceed"
        assert not runtime_path.exists()
    finally:
        server.shutdown()


def test_guard_preserves_runtime_json_when_probe_fails_and_holder_alive(
    tmp_path, monkeypatch
):
    """Codex P1 regression: when a 2nd process probes during the 1st
    instance's startup window (PID written but Flask not listening yet),
    the probe raises connection-refused. Previously, check_single_instance
    deleted the live instance's runtime.json, breaking discovery. With
    the fix, we preserve the file if the holder PID is alive and return
    conflict.
    """
    import socket as _socket

    monkeypatch.setenv("HOME", str(tmp_path))
    os.makedirs(tmp_path / ".vireo")

    # Reserve a port then close it so the probe gets connection-refused.
    sock = _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    refused_port = sock.getsockname()[1]
    sock.close()

    runtime_path = tmp_path / ".vireo" / "runtime.json"
    runtime_path.write_text(_json.dumps({
        "port": refused_port,
        "pid": os.getpid(),  # definitely alive
        "token": "any",
    }))

    from runtime import check_single_instance
    status, info = check_single_instance(probe_timeout_s=0.2)
    assert status == "conflict"
    assert info["port"] == refused_port
    assert info["pid"] == os.getpid()
    # Critically: file is NOT deleted while the holder is alive.
    assert runtime_path.exists()


def test_guard_deletes_runtime_json_when_probe_fails_and_holder_dead(
    tmp_path, monkeypatch
):
    """Counterpart to the above: a dead-holder stale file is still cleaned up."""
    import socket as _socket

    monkeypatch.setenv("HOME", str(tmp_path))
    os.makedirs(tmp_path / ".vireo")

    sock = _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    refused_port = sock.getsockname()[1]
    sock.close()

    runtime_path = tmp_path / ".vireo" / "runtime.json"
    # PID 1 on macOS/Linux is launchd/init — always alive. Use a high,
    # implausible PID instead; pair with ProcessLookupError via direct mock.
    # Simplest: patch _pid_alive via monkeypatch.
    import runtime
    monkeypatch.setattr(runtime, "_pid_alive", lambda _p: False)

    runtime_path.write_text(_json.dumps({
        "port": refused_port,
        "pid": 99999,
        "token": "any",
    }))

    status, _info = runtime.check_single_instance(probe_timeout_s=0.2)
    assert status == "proceed"
    assert not runtime_path.exists()


def test_generate_token_is_random_and_urlsafe():
    from runtime import generate_token
    a = generate_token()
    b = generate_token()
    assert a != b
    assert len(a) >= 32
    # URL-safe base64: only alphanumerics and -_
    import re
    assert re.fullmatch(r"[A-Za-z0-9_-]+", a)


# ---------------------------------------------------------------------------
# acquire_single_instance — atomic reservation via runtime.lock
# ---------------------------------------------------------------------------


def test_acquire_on_empty_slot_creates_lock(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    os.makedirs(tmp_path / ".vireo")

    from runtime import acquire_single_instance
    status, info = acquire_single_instance(pid=os.getpid())
    assert status == "acquired"
    assert info is None

    lock_path = tmp_path / ".vireo" / "runtime.lock"
    assert lock_path.exists()
    assert lock_path.read_text().strip() == str(os.getpid())
    # External runtime.json must not be created by the reservation step.
    assert not (tmp_path / ".vireo" / "runtime.json").exists()


def test_acquire_conflicts_with_healthy_peer(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    os.makedirs(tmp_path / ".vireo")

    server, port = _start_fake_server("goodtoken")
    try:
        runtime_path = tmp_path / ".vireo" / "runtime.json"
        runtime_path.write_text(_json.dumps({
            "port": port, "pid": 12345, "token": "goodtoken",
        }))
        from runtime import acquire_single_instance
        status, info = acquire_single_instance(pid=os.getpid())
        assert status == "conflict"
        assert info["port"] == port
        assert info["pid"] == 12345
        # The live peer's runtime.json must not have been touched.
        assert runtime_path.exists()
    finally:
        server.shutdown()


def test_acquire_replaces_stale_runtime_json(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    os.makedirs(tmp_path / ".vireo")

    runtime_path = tmp_path / ".vireo" / "runtime.json"
    # Port 1 is not bound by anything listening — probe will fail. PID 0
    # is unconditionally dead, so the guard must classify the file as
    # stale rather than treating it as a booting peer.
    runtime_path.write_text(_json.dumps({
        "port": 1, "pid": 0, "token": "stale",
    }))

    from runtime import acquire_single_instance
    status, _info = acquire_single_instance(pid=os.getpid())
    assert status == "acquired"
    # Stale runtime.json was cleaned up by the probe.
    assert not runtime_path.exists()
    assert (tmp_path / ".vireo" / "runtime.lock").exists()


def test_acquire_preserves_runtime_json_when_peer_is_booting(tmp_path, monkeypatch):
    """When `acquire_single_instance` finds a runtime.json whose HTTP
    probe fails (connection refused) but whose PID is still alive, it
    must report conflict without wiping the peer's runtime.json.

    Regression for the race where a second process starts while the
    first is mid-boot: the first process has written runtime.json and
    holds runtime.lock but hasn't opened its HTTP port yet.
    """
    monkeypatch.setenv("HOME", str(tmp_path))
    os.makedirs(tmp_path / ".vireo")

    runtime_path = tmp_path / ".vireo" / "runtime.json"
    runtime_path.write_text(_json.dumps({
        "port": 1,  # nothing listening
        "pid": os.getpid(),  # peer is alive, just not serving yet
        "token": "anything",
    }))
    lock_path = tmp_path / ".vireo" / "runtime.lock"
    lock_path.write_text(str(os.getpid()))

    from runtime import acquire_single_instance
    status, info = acquire_single_instance(pid=os.getpid() + 1)
    assert status == "conflict"
    assert info["pid"] == os.getpid()
    # External discovery must still work — do not delete the peer's file.
    assert runtime_path.exists()
    # Lock must be preserved too.
    assert lock_path.exists()


def test_acquire_conflicts_with_lock_from_live_pid(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    os.makedirs(tmp_path / ".vireo")

    lock_path = tmp_path / ".vireo" / "runtime.lock"
    lock_path.write_text(str(os.getpid()))  # live PID

    from runtime import acquire_single_instance
    status, info = acquire_single_instance(pid=os.getpid() + 1)
    assert status == "conflict"
    assert info["pid"] == os.getpid()
    # Lock must still exist — we did not steal it.
    assert lock_path.exists()


def test_acquire_clears_stale_lock(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    os.makedirs(tmp_path / ".vireo")

    lock_path = tmp_path / ".vireo" / "runtime.lock"
    lock_path.write_text("0")  # PID 0 is never a real process

    from runtime import acquire_single_instance
    status, _info = acquire_single_instance(pid=os.getpid())
    assert status == "acquired"
    # The stale lock was replaced by ours.
    assert lock_path.read_text().strip() == str(os.getpid())


def test_acquire_handles_garbage_lock_contents(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    os.makedirs(tmp_path / ".vireo")

    lock_path = tmp_path / ".vireo" / "runtime.lock"
    lock_path.write_text("not a pid")

    from runtime import acquire_single_instance
    status, _info = acquire_single_instance(pid=os.getpid())
    assert status == "acquired"


def test_release_single_instance_is_idempotent(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    os.makedirs(tmp_path / ".vireo")

    lock_path = tmp_path / ".vireo" / "runtime.lock"
    lock_path.write_text(str(os.getpid()))

    from runtime import release_single_instance
    release_single_instance()
    assert not lock_path.exists()
    release_single_instance()  # second call must not raise


def test_acquire_is_atomic_across_concurrent_calls(tmp_path, monkeypatch):
    """Two threads racing to acquire: exactly one wins, the other conflicts."""
    monkeypatch.setenv("HOME", str(tmp_path))
    os.makedirs(tmp_path / ".vireo")

    from runtime import acquire_single_instance

    results = []
    errors = []
    start = threading.Event()

    def worker():
        start.wait()
        try:
            # Both workers pass our own PID so the loser's probe of the
            # winner's PID classifies the holder as alive.
            results.append(acquire_single_instance(pid=os.getpid()))
        except Exception as e:  # pragma: no cover — shouldn't happen
            errors.append(e)

    threads = [threading.Thread(target=worker) for _ in range(2)]
    for t in threads:
        t.start()
    start.set()
    for t in threads:
        t.join(timeout=5)

    assert not errors
    statuses = sorted(s for s, _ in results)
    assert statuses == ["acquired", "conflict"]
