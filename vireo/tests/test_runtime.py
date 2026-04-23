import json
import os
import stat
import subprocess
import sys

import pytest


@pytest.fixture(autouse=True)
def _reset_runtime_lock_state():
    """Ensure each test starts with a clean flock / module-global state.

    The single-instance guard keeps the lock fd on a module global so
    the kernel-managed flock persists across function calls within a
    process. In tests, that state leaks between test cases, so we
    release any held flock before and after each test.
    """
    import runtime  # noqa: PLC0415
    runtime.release_single_instance()
    yield
    runtime.release_single_instance()


def _spawn_flock_holder(lock_path):
    """Spawn a subprocess that takes an exclusive fcntl.flock on lock_path
    and holds it until killed. Returns the Popen handle; caller must
    terminate it in a finally block.

    Used to simulate a real live-peer flock holder in tests. We cannot
    simply write to the lock file, because the guard now trusts the
    kernel-managed flock, not the PID bytes inside the file.
    """
    code = (
        "import fcntl, os, sys, time;"
        f"fd = os.open({str(lock_path)!r}, os.O_CREAT | os.O_RDWR, 0o600);"
        "fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB);"
        "os.ftruncate(fd, 0);"
        "os.write(fd, str(os.getpid()).encode());"
        "sys.stdout.write(str(os.getpid()) + '\\n');"
        "sys.stdout.flush();"
        "time.sleep(60)"
    )
    proc = subprocess.Popen(
        [sys.executable, "-c", code],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    # Wait for the holder to signal ready (its PID on stdout).
    line = proc.stdout.readline().decode().strip()
    assert line.isdigit(), f"holder did not signal ready: {line!r}"
    return proc, int(line)


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


def test_write_runtime_json_tmp_file_is_never_world_readable(tmp_path, monkeypatch):
    """The temp file holding the token must be 0600 from the moment it
    exists on disk — not only after a trailing chmod. A later
    write_text()+chmod sequence leaves a window where a co-tenant on a
    multi-user host can read the auth token."""
    monkeypatch.setenv("HOME", str(tmp_path))
    os.makedirs(tmp_path / ".vireo")

    import runtime as rt

    observed = {}
    real_replace = os.replace

    def spy_replace(src, dst):
        # Inspect the tmp file's mode right before it is renamed into place.
        observed["tmp_mode"] = stat.S_IMODE(os.stat(src).st_mode)
        return real_replace(src, dst)

    monkeypatch.setattr(rt.os, "replace", spy_replace)

    rt.write_runtime_json(
        port=1, pid=2, version="v", db_path="/x", token="secret", mode="headless"
    )

    assert observed["tmp_mode"] == 0o600


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


def test_read_runtime_json_non_utf8_returns_none(tmp_path, monkeypatch):
    """A corrupted runtime.json containing non-UTF-8 bytes must be treated
    as malformed and cleaned up, NOT raise UnicodeDecodeError out of
    startup and block launching until the user manually deletes it."""
    monkeypatch.setenv("HOME", str(tmp_path))
    os.makedirs(tmp_path / ".vireo")
    # 0xff / 0xfe / 0xfd are invalid as a UTF-8 start byte.
    (tmp_path / ".vireo" / "runtime.json").write_bytes(b"\xff\xfe\xfd")

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
        # Carry the Vireo service marker the probe validates.
        self.wfile.write(b'{"service":"vireo","status":"ok"}')

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

    The guard trusts the kernel-managed flock to decide whether the peer
    is alive. We simulate the booting peer by spawning a subprocess that
    actually holds the flock while the HTTP port is unbound.
    """
    monkeypatch.setenv("HOME", str(tmp_path))
    os.makedirs(tmp_path / ".vireo")

    lock_path = tmp_path / ".vireo" / "runtime.lock"
    holder, holder_pid = _spawn_flock_holder(lock_path)
    try:
        runtime_path = tmp_path / ".vireo" / "runtime.json"
        runtime_path.write_text(_json.dumps({
            "port": 1,  # nothing listening → connection refused
            "pid": holder_pid,
            "token": "anything",
        }))

        from runtime import check_single_instance
        status, info = check_single_instance()
        assert status == "conflict"
        assert info["port"] == 1
        assert info["pid"] == holder_pid
        # The live peer's runtime.json must remain intact.
        assert runtime_path.exists()
    finally:
        holder.terminate()
        holder.wait(timeout=5)


class _CatchAll200Handler(http.server.BaseHTTPRequestHandler):
    """Simulates an unrelated local service that returns 200 for everything,
    including /api/v1/health — but without the Vireo service marker."""

    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"status":"ok"}')

    def log_message(self, *a, **kw):  # silence
        pass


def _start_fake_catchall_server():
    server = http.server.HTTPServer(("127.0.0.1", 0), _CatchAll200Handler)
    port = server.server_address[1]
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    return server, port


def test_guard_unrelated_200_without_service_marker_is_stale(tmp_path, monkeypatch):
    """Codex P2 regression: a 200 alone is not proof of a live Vireo peer —
    an unrelated local service that happens to bind Vireo's old port and
    returns 200 for /api/v1/health would falsely cause `already_running`.
    The probe now also validates a Vireo-specific service marker in the
    response body.
    """
    monkeypatch.setenv("HOME", str(tmp_path))
    os.makedirs(tmp_path / ".vireo")

    server, port = _start_fake_catchall_server()
    try:
        runtime_path = tmp_path / ".vireo" / "runtime.json"
        runtime_path.write_text(_json.dumps({
            "port": port, "pid": 55555, "token": "anything",
        }))
        from runtime import check_single_instance
        status, _info = check_single_instance()
        assert status == "proceed"
        assert not runtime_path.exists()
    finally:
        server.shutdown()


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
    instance's startup window (runtime.json written but Flask not
    listening yet), the probe raises connection-refused. Previously,
    check_single_instance deleted the live instance's runtime.json,
    breaking discovery. With the fix we trust the kernel flock: a held
    flock means a live Vireo peer, so preserve the file and conflict.
    """
    import socket as _socket

    monkeypatch.setenv("HOME", str(tmp_path))
    os.makedirs(tmp_path / ".vireo")

    # Reserve a port then close it so the probe gets connection-refused.
    sock = _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    refused_port = sock.getsockname()[1]
    sock.close()

    lock_path = tmp_path / ".vireo" / "runtime.lock"
    holder, holder_pid = _spawn_flock_holder(lock_path)
    try:
        runtime_path = tmp_path / ".vireo" / "runtime.json"
        runtime_path.write_text(_json.dumps({
            "port": refused_port,
            "pid": holder_pid,
            "token": "any",
        }))

        from runtime import check_single_instance
        status, info = check_single_instance(probe_timeout_s=0.2)
        assert status == "conflict"
        assert info["port"] == refused_port
        assert info["pid"] == holder_pid
        # Critically: file is NOT deleted while the flock is held.
        assert runtime_path.exists()
    finally:
        holder.terminate()
        holder.wait(timeout=5)


def test_guard_deletes_runtime_json_when_probe_fails_and_lock_unheld(
    tmp_path, monkeypatch
):
    """Self-healing regression (Codex P2): if runtime.json is stale after
    a hard crash — recorded PID may even have been recycled — but no
    process holds the flock, the guard must classify the file as stale
    and clean it up so the next startup can proceed.

    Before the flock-based guard, check_single_instance trusted `pid_alive`
    alone, and a recycled PID would cause conflict indefinitely. With
    the flock-based guard, the kernel releases the flock when the crashed
    process dies, so "unheld flock" reliably means "stale".
    """
    import socket as _socket

    monkeypatch.setenv("HOME", str(tmp_path))
    os.makedirs(tmp_path / ".vireo")

    sock = _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    refused_port = sock.getsockname()[1]
    sock.close()

    # Leftover lock file from crashed process — no live flock on it.
    lock_path = tmp_path / ".vireo" / "runtime.lock"
    lock_path.write_text(str(os.getpid()))  # PID looks alive, but no flock

    runtime_path = tmp_path / ".vireo" / "runtime.json"
    runtime_path.write_text(_json.dumps({
        "port": refused_port,
        "pid": os.getpid(),  # would fool a naive PID check
        "token": "any",
    }))

    from runtime import check_single_instance
    status, _info = check_single_instance(probe_timeout_s=0.2)
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
    probe fails (connection refused) but another process holds the
    flock, it must report conflict without wiping the peer's files.

    Regression for the race where a second process starts while the
    first is mid-boot: the first process has taken the flock and
    written runtime.json but hasn't opened its HTTP port yet.
    """
    monkeypatch.setenv("HOME", str(tmp_path))
    os.makedirs(tmp_path / ".vireo")

    lock_path = tmp_path / ".vireo" / "runtime.lock"
    holder, holder_pid = _spawn_flock_holder(lock_path)
    try:
        runtime_path = tmp_path / ".vireo" / "runtime.json"
        runtime_path.write_text(_json.dumps({
            "port": 1,  # nothing listening
            "pid": holder_pid,
            "token": "anything",
        }))

        from runtime import acquire_single_instance
        status, info = acquire_single_instance(pid=os.getpid())
        assert status == "conflict"
        assert info["pid"] == holder_pid
        # External discovery must still work — do not delete the peer's file.
        assert runtime_path.exists()
        # Lock file must be preserved too.
        assert lock_path.exists()
    finally:
        holder.terminate()
        holder.wait(timeout=5)


def test_acquire_conflicts_with_held_flock(tmp_path, monkeypatch):
    """If another process holds the flock on runtime.lock, acquire must
    report conflict. We spawn a real flock holder rather than planting
    a text file, because the guard trusts the kernel-managed lock, not
    the PID bytes inside the file (which would be vulnerable to PID
    recycling after an unclean crash)."""
    monkeypatch.setenv("HOME", str(tmp_path))
    os.makedirs(tmp_path / ".vireo")

    lock_path = tmp_path / ".vireo" / "runtime.lock"
    holder, holder_pid = _spawn_flock_holder(lock_path)
    try:
        from runtime import acquire_single_instance
        status, info = acquire_single_instance(pid=os.getpid())
        assert status == "conflict"
        assert info["pid"] == holder_pid
        # Lock must still exist — we did not steal it.
        assert lock_path.exists()
    finally:
        holder.terminate()
        holder.wait(timeout=5)


def test_acquire_self_heals_after_crash_with_pid_recycling(tmp_path, monkeypatch):
    """Self-healing regression (Codex P2): after an unclean crash the
    lock file persists, but the kernel releases the flock. Even if the
    dead PID has been recycled to an unrelated process, the next startup
    must reclaim the slot. The file-level liveness check alone would
    falsely conflict here — only the kernel-managed flock gets this right.
    """
    monkeypatch.setenv("HOME", str(tmp_path))
    os.makedirs(tmp_path / ".vireo")

    lock_path = tmp_path / ".vireo" / "runtime.lock"
    # Simulate a crashed process's leftover lock file. PID bytes look
    # alive (we use our own PID — the test process), but no flock is
    # held on the file.
    lock_path.write_text(str(os.getpid()))

    from runtime import acquire_single_instance, release_single_instance
    try:
        status, _info = acquire_single_instance(pid=os.getpid())
        assert status == "acquired"
        # Our PID is now recorded in the lock file.
        assert lock_path.read_text().strip() == str(os.getpid())
    finally:
        release_single_instance()


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


def test_acquire_surfaces_lock_open_errors_not_conflict(tmp_path, monkeypatch):
    """If `os.open` on the lock file fails (e.g. filesystem fault, permission
    denied on ~/.vireo), that must surface as an OSError — NOT be silently
    converted to a ('conflict', ...) that main() reports as already_running.
    The two conditions need different remediation."""
    monkeypatch.setenv("HOME", str(tmp_path))
    os.makedirs(tmp_path / ".vireo")

    import runtime as rt

    def boom(*_a, **_kw):
        raise PermissionError(13, "simulated permission denied")

    # Swap in a failing os.open only for the lock-file call. Leave
    # everything else alone so check_single_instance still works.
    monkeypatch.setattr(rt.os, "open", boom)

    with pytest.raises(PermissionError):
        rt.acquire_single_instance(pid=os.getpid())


def test_acquire_surfaces_diag_write_failure_not_conflict(tmp_path, monkeypatch):
    """If writing the diagnostic PID into runtime.lock fails (ENOSPC, EIO),
    acquire_single_instance must surface the OSError — not swallow it and
    retry, which would eventually return ('conflict', ...) and cause
    main() to misreport a filesystem fault as already_running."""
    monkeypatch.setenv("HOME", str(tmp_path))
    os.makedirs(tmp_path / ".vireo")

    import runtime as rt

    def boom(*_a, **_kw):
        raise OSError(28, "simulated ENOSPC")

    # Only ftruncate raises — os.open, os.lseek still work, so the lock
    # is actually acquired before the failing diagnostic write.
    monkeypatch.setattr(rt.os, "ftruncate", boom)

    with pytest.raises(OSError):
        rt.acquire_single_instance(pid=os.getpid())

    # Lock must not remain held by this process — a subsequent acquire
    # (with ftruncate restored by monkeypatch teardown) must succeed.


def test_release_single_instance_is_idempotent(tmp_path, monkeypatch):
    """release() must be idempotent and leave the lock file on disk.

    We intentionally keep runtime.lock around after release — flock binds
    to inodes, so unlinking creates a race where a surviving opener on
    the old inode and a new starter on a fresh inode can both hold locks
    simultaneously. Leaving the file in place forces convergence on one
    inode."""
    monkeypatch.setenv("HOME", str(tmp_path))
    os.makedirs(tmp_path / ".vireo")

    from runtime import acquire_single_instance, release_single_instance

    status, _ = acquire_single_instance(pid=os.getpid())
    assert status == "acquired"

    lock_path = tmp_path / ".vireo" / "runtime.lock"
    assert lock_path.exists()

    release_single_instance()
    # File is deliberately preserved — see docstring.
    assert lock_path.exists()
    release_single_instance()  # second call must not raise


def test_release_then_reacquire_converges_on_same_inode(tmp_path, monkeypatch):
    """After release, a subsequent acquire must lock the same inode.

    If release() unlinked the lock file, a newcomer could open a brand-new
    inode and hold a lock concurrently with a lingering opener on the old
    inode, defeating the single-instance guarantee. This test verifies
    the inode number does not change across a release/re-acquire cycle."""
    monkeypatch.setenv("HOME", str(tmp_path))
    os.makedirs(tmp_path / ".vireo")
    lock_path = tmp_path / ".vireo" / "runtime.lock"

    from runtime import acquire_single_instance, release_single_instance

    status, _ = acquire_single_instance(pid=os.getpid())
    assert status == "acquired"
    inode_before = os.stat(lock_path).st_ino

    release_single_instance()

    status, _ = acquire_single_instance(pid=os.getpid())
    assert status == "acquired"
    inode_after = os.stat(lock_path).st_ino

    assert inode_before == inode_after


def test_write_runtime_json_survives_partial_os_write(tmp_path, monkeypatch):
    """os.write is allowed to do partial writes even on regular files under
    signal/resource pressure. write_runtime_json must loop until the full
    payload has been flushed; otherwise os.replace atomically promotes a
    truncated (malformed) JSON file and discovery fails."""
    monkeypatch.setenv("HOME", str(tmp_path))
    os.makedirs(tmp_path / ".vireo")

    import runtime as rt

    real_write = os.write
    call_count = {"n": 0}

    def chunked_write(fd, buf):
        # First call writes 1 byte, subsequent calls flush the rest —
        # forces the caller's loop to iterate.
        call_count["n"] += 1
        if call_count["n"] == 1:
            return real_write(fd, bytes(buf[:1]))
        return real_write(fd, bytes(buf))

    monkeypatch.setattr(rt.os, "write", chunked_write)

    rt.write_runtime_json(
        port=1, pid=2, version="v", db_path="/x", token="tok", mode="headless"
    )

    # File must exist and be fully-formed JSON.
    data = json.loads((tmp_path / ".vireo" / "runtime.json").read_text())
    assert data["token"] == "tok"
    assert data["port"] == 1
    assert call_count["n"] >= 2  # loop actually iterated


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
