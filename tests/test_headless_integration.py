"""End-to-end tests for headless sidecar mode.

These spawn the real `python vireo/app.py` subprocess against a temp HOME
and temp database, then drive it via HTTP exactly as an external caller
would. They are more expensive than unit tests but the single-instance
guard, runtime.json writer, and `/api/v1/*` auth are load-bearing enough
to warrant real-process coverage.
"""

import json
import os
import signal
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
APP = REPO / "vireo" / "app.py"


def _wait_for_runtime(runtime_path: Path, timeout: float = 20.0) -> dict:
    """Poll runtime.json until it exists and is readable."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if runtime_path.exists():
            try:
                return json.loads(runtime_path.read_text())
            except json.JSONDecodeError:
                pass
        time.sleep(0.1)
    raise TimeoutError(f"{runtime_path} never appeared")


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _spawn(home: Path, db: Path, port: int):
    env = os.environ.copy()
    env["HOME"] = str(home)
    env["PYTHONUNBUFFERED"] = "1"
    return subprocess.Popen(
        [sys.executable, str(APP),
         "--headless", "--port", str(port), "--db", str(db)],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def _http_get(url: str, token: str, timeout: float = 2.0):
    req = urllib.request.Request(url, headers={"X-Vireo-Token": token})
    return urllib.request.urlopen(req, timeout=timeout)


@pytest.fixture
def headless_home(tmp_path):
    home = tmp_path / "home"
    home.mkdir()
    (home / ".vireo").mkdir()
    return home


def test_headless_spawn_writes_runtime_and_serves_health(headless_home, tmp_path):
    db = tmp_path / "vireo.db"
    port = _free_port()
    proc = _spawn(headless_home, db, port)
    try:
        runtime = headless_home / ".vireo" / "runtime.json"
        data = _wait_for_runtime(runtime)
        assert data["port"] == port
        assert data["mode"] == "headless"
        assert data["pid"] == proc.pid
        assert len(data["token"]) >= 32

        resp = _http_get(
            f"http://127.0.0.1:{port}/api/v1/health", data["token"],
        )
        assert resp.status == 200
        assert json.loads(resp.read())["status"] == "ok"
    finally:
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()


def test_second_spawn_refuses_with_already_running(headless_home, tmp_path):
    db = tmp_path / "vireo.db"
    first_port = _free_port()
    first = _spawn(headless_home, db, first_port)
    try:
        runtime = headless_home / ".vireo" / "runtime.json"
        _wait_for_runtime(runtime)

        second_port = _free_port()
        second = _spawn(headless_home, db, second_port)
        out, err = second.communicate(timeout=15)

        assert second.returncode != 0, "second instance should have exited"
        err_text = err.decode()
        # The guard writes a machine-readable JSON line to stderr.
        last_line = [ln for ln in err_text.strip().splitlines() if ln.startswith("{")]
        assert last_line, f"no JSON error in stderr: {err_text!r}"
        payload = json.loads(last_line[-1])
        assert payload["error"] == "already_running"
        assert payload["port"] == first_port

        # First instance must still be healthy.
        data = json.loads(runtime.read_text())
        resp = _http_get(
            f"http://127.0.0.1:{first_port}/api/v1/health", data["token"],
        )
        assert resp.status == 200
    finally:
        first.send_signal(signal.SIGTERM)
        try:
            first.wait(timeout=10)
        except subprocess.TimeoutExpired:
            first.kill()
            first.wait()


def test_stale_runtime_json_is_replaced(headless_home, tmp_path):
    # Plant a stale runtime.json pointing at an unused port.
    stale = {
        "port": 1,  # almost certainly not bound
        "pid": 999999,
        "token": "stale",
        "version": "0.0.0",
        "db_path": "/nowhere",
        "mode": "headless",
        "started_at": "2000-01-01T00:00:00Z",
    }
    runtime = headless_home / ".vireo" / "runtime.json"
    runtime.write_text(json.dumps(stale))

    db = tmp_path / "vireo.db"
    port = _free_port()
    proc = _spawn(headless_home, db, port)
    try:
        # Poll for the new contents (different port).
        deadline = time.monotonic() + 20
        data = None
        while time.monotonic() < deadline:
            if runtime.exists():
                try:
                    candidate = json.loads(runtime.read_text())
                    if candidate.get("port") == port:
                        data = candidate
                        break
                except json.JSONDecodeError:
                    pass
            time.sleep(0.1)
        assert data is not None, "runtime.json was not refreshed"
        assert data["pid"] == proc.pid
        assert data["token"] != "stale"
    finally:
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
