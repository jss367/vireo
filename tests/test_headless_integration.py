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
