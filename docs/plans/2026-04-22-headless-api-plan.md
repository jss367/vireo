# Headless API Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make Vireo reliably scriptable by external callers (agents, CI) when only the packaged `.app` is installed, by adding a runtime-discovery file, a single-instance guard, a versioned `/api/v1/*` API, and a local auth token.

**Architecture:** The sidecar writes `~/.vireo/runtime.json` at startup with port, PID, token, and metadata. A single-instance guard at the top of `main()` refuses to start if a healthy peer is running. A new `/api/v1/*` surface aliases a small stable subset of endpoints, gated by an `X-Vireo-Token` header matching the value in `runtime.json`. The existing `/api/*` surface remains unchanged so the Tauri UI keeps working.

**Tech Stack:** Python 3.11+, Flask 3, pytest. Existing dependencies only — no new packages.

**Design reference:** `docs/plans/2026-04-22-headless-api-design.md`.

---

## Preconditions

- Work happens on branch `headless-app-feasibility` in `/Users/julius/conductor/workspaces/vireo/boston`.
- Before starting: confirm tests pass on the current tree.

Run:
```bash
python -m pytest tests/test_workspaces.py vireo/tests/test_db.py vireo/tests/test_app.py vireo/tests/test_photos_api.py vireo/tests/test_edits_api.py vireo/tests/test_jobs_api.py vireo/tests/test_darktable_api.py vireo/tests/test_config.py -v
```
Expected: all pass.

---

## Task 1: `runtime.json` atomic writer

**Files:**
- Create: `vireo/runtime.py`
- Test: `vireo/tests/test_runtime.py`

**Step 1: Write the failing test**

Create `vireo/tests/test_runtime.py`:

```python
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
```

**Step 2: Run test to verify it fails**

Run: `python -m pytest vireo/tests/test_runtime.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'runtime'`.

**Step 3: Write minimal implementation**

Create `vireo/runtime.py`:

```python
"""Runtime discovery file for the Vireo sidecar.

Writes `~/.vireo/runtime.json` so external callers can discover the running
instance (port, auth token, PID). Also provides the single-instance guard.
"""

import json
import os
from datetime import UTC, datetime
from pathlib import Path


def _runtime_path() -> Path:
    return Path(os.path.expanduser("~/.vireo/runtime.json"))


def write_runtime_json(
    *, port: int, pid: int, version: str, db_path: str, token: str, mode: str
) -> None:
    """Atomically write runtime.json with 0600 permissions."""
    path = _runtime_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "port": port,
        "pid": pid,
        "version": version,
        "db_path": db_path,
        "started_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "mode": mode,
        "token": token,
    }
    tmp = path.with_suffix(path.suffix + ".tmp")
    # Write then chmod before replace, so the target is never world-readable.
    tmp.write_text(json.dumps(payload, indent=2))
    os.chmod(tmp, 0o600)
    os.replace(tmp, path)
```

**Step 4: Run test to verify it passes**

Run: `python -m pytest vireo/tests/test_runtime.py -v`
Expected: both tests PASS.

**Step 5: Commit**

```bash
git add vireo/runtime.py vireo/tests/test_runtime.py
git commit -m "feat(runtime): atomic runtime.json writer with 0600 perms"
```

---

## Task 2: `runtime.json` reader + cleanup

**Files:**
- Modify: `vireo/runtime.py`
- Modify: `vireo/tests/test_runtime.py`

**Step 1: Write the failing test**

Append to `vireo/tests/test_runtime.py`:

```python
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
```

**Step 2: Run test to verify it fails**

Run: `python -m pytest vireo/tests/test_runtime.py -v`
Expected: FAIL — `read_runtime_json` / `delete_runtime_json` not found.

**Step 3: Write minimal implementation**

Append to `vireo/runtime.py`:

```python
def read_runtime_json() -> dict | None:
    """Return runtime.json contents, or None if missing / malformed."""
    path = _runtime_path()
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return None


def delete_runtime_json() -> None:
    """Remove runtime.json if present. Idempotent."""
    try:
        _runtime_path().unlink()
    except FileNotFoundError:
        pass
```

**Step 4: Run test to verify it passes**

Run: `python -m pytest vireo/tests/test_runtime.py -v`
Expected: all PASS.

**Step 5: Commit**

```bash
git add vireo/runtime.py vireo/tests/test_runtime.py
git commit -m "feat(runtime): read and delete helpers for runtime.json"
```

---

## Task 3: Single-instance guard

**Files:**
- Modify: `vireo/runtime.py`
- Modify: `vireo/tests/test_runtime.py`

**Step 1: Write the failing test**

Append to `vireo/tests/test_runtime.py`:

```python
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
```

**Step 2: Run test to verify it fails**

Run: `python -m pytest vireo/tests/test_runtime.py -v`
Expected: FAIL — `check_single_instance` not found.

**Step 3: Write minimal implementation**

Append to `vireo/runtime.py`:

```python
import urllib.error
import urllib.request


def check_single_instance(probe_timeout_s: float = 0.5) -> tuple[str, dict | None]:
    """Check whether another Vireo instance is healthy on the advertised port.

    Returns:
        ("proceed", None)  — no peer found, or stale file cleaned up.
        ("conflict", info) — healthy peer. `info` has keys `port`, `pid`.
    """
    data = read_runtime_json()
    if data is None:
        # Missing or malformed. If malformed, the file still exists on disk;
        # delete it so the next caller has a clean slate.
        delete_runtime_json()
        return ("proceed", None)

    port = data.get("port")
    token = data.get("token", "")
    if not isinstance(port, int):
        delete_runtime_json()
        return ("proceed", None)

    try:
        req = urllib.request.Request(
            f"http://127.0.0.1:{port}/api/v1/health",
            headers={"X-Vireo-Token": token},
        )
        with urllib.request.urlopen(req, timeout=probe_timeout_s) as resp:
            if resp.status == 200:
                return ("conflict", {"port": port, "pid": data.get("pid")})
    except (urllib.error.URLError, TimeoutError, OSError):
        pass

    # Probe failed — peer is dead. Clean up and proceed.
    delete_runtime_json()
    return ("proceed", None)
```

**Step 4: Run test to verify it passes**

Run: `python -m pytest vireo/tests/test_runtime.py -v`
Expected: all PASS.

**Step 5: Commit**

```bash
git add vireo/runtime.py vireo/tests/test_runtime.py
git commit -m "feat(runtime): single-instance guard via health probe"
```

---

## Task 4: Token generator

**Files:**
- Modify: `vireo/runtime.py`
- Modify: `vireo/tests/test_runtime.py`

**Step 1: Write the failing test**

Append to `vireo/tests/test_runtime.py`:

```python
def test_generate_token_is_random_and_urlsafe():
    from runtime import generate_token
    a = generate_token()
    b = generate_token()
    assert a != b
    assert len(a) >= 32
    # URL-safe base64: only alphanumerics and -_
    import re
    assert re.fullmatch(r"[A-Za-z0-9_-]+", a)
```

**Step 2: Run test to verify it fails**

Run: `python -m pytest vireo/tests/test_runtime.py::test_generate_token_is_random_and_urlsafe -v`
Expected: FAIL — `generate_token` not found.

**Step 3: Write minimal implementation**

Append to `vireo/runtime.py`:

```python
import secrets


def generate_token() -> str:
    """Return a URL-safe random token suitable for API auth."""
    return secrets.token_urlsafe(32)
```

**Step 4: Run test to verify it passes**

Run: `python -m pytest vireo/tests/test_runtime.py -v`
Expected: all PASS.

**Step 5: Commit**

```bash
git add vireo/runtime.py vireo/tests/test_runtime.py
git commit -m "feat(runtime): random token generator"
```

---

## Task 5: Token-gated middleware for `/api/v1/*`

**Files:**
- Modify: `vireo/app.py` (add middleware inside `create_app`, immediately before the existing `/api/health` route around line 524)
- Modify: `vireo/tests/conftest.py` (app fixture must provide a known token)
- Create: `vireo/tests/test_api_v1_auth.py`

**Step 1: Write the failing test**

Create `vireo/tests/test_api_v1_auth.py`:

```python
def test_api_v1_requires_token(app_and_db):
    """GET /api/v1/health without a token → 401."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get("/api/v1/health")
    assert resp.status_code == 401


def test_api_v1_wrong_token_rejected(app_and_db):
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get("/api/v1/health", headers={"X-Vireo-Token": "wrong"})
    assert resp.status_code == 401


def test_api_v1_correct_token_accepted(app_and_db):
    app, _ = app_and_db
    client = app.test_client()
    token = app.config["API_TOKEN"]
    resp = client.get("/api/v1/health", headers={"X-Vireo-Token": token})
    assert resp.status_code == 200
    assert resp.get_json() == {"status": "ok"}


def test_internal_api_does_not_require_token(app_and_db):
    """Existing /api/* routes are unaffected by the v1 middleware."""
    app, _ = app_and_db
    client = app.test_client()
    # /api/health is an internal route and must keep working without a token
    resp = client.get("/api/health")
    assert resp.status_code == 200
```

Also modify `vireo/tests/conftest.py` — change `create_app` call in `app_and_db` and `client_with_photo` to pass a known token. Replace:

```python
app = create_app(db_path=db_path, thumb_cache_dir=thumb_dir)
```
with:
```python
app = create_app(db_path=db_path, thumb_cache_dir=thumb_dir, api_token="test-token-123")
```

And in `client_with_photo`:
```python
app = create_app(db_path=db_path, thumb_cache_dir=str(thumb_dir), api_token="test-token-123")
```

**Step 2: Run test to verify it fails**

Run: `python -m pytest vireo/tests/test_api_v1_auth.py -v`
Expected: FAIL — `/api/v1/health` returns 404 (route doesn't exist yet), and `create_app` rejects the `api_token` kwarg.

**Step 3: Write minimal implementation**

Modify `vireo/app.py`:

1. Change `create_app(db_path, thumb_cache_dir=None)` (line 378) to `create_app(db_path, thumb_cache_dir=None, api_token=None)`.

2. Right after `app.config["THUMB_CACHE_DIR"] = ...` (around line 391), add:
   ```python
   app.config["API_TOKEN"] = api_token
   ```

3. Right after the `_log_requests` after_request handler (before line 524's `@app.route("/api/health")`), add:
   ```python
   @app.before_request
   def _enforce_api_v1_token():
       if not request.path.startswith("/api/v1/"):
           return None
       expected = app.config.get("API_TOKEN")
       if not expected:
           # No token configured → deny all v1 traffic.
           return json_error("API token not configured", 401)
       if request.headers.get("X-Vireo-Token") != expected:
           return json_error("Invalid or missing X-Vireo-Token", 401)
       return None
   ```

4. Add a `/api/v1/health` route. Place it right after the existing `/api/health` route (around line 527):
   ```python
   @app.route("/api/v1/health")
   def api_v1_health():
       return jsonify({"status": "ok"})
   ```

**Step 4: Run test to verify it passes**

Run: `python -m pytest vireo/tests/test_api_v1_auth.py vireo/tests/test_app.py -v`
Expected: all PASS. `test_app.py` still passes because the middleware only affects `/api/v1/*`.

**Step 5: Commit**

```bash
git add vireo/app.py vireo/tests/test_api_v1_auth.py vireo/tests/conftest.py
git commit -m "feat(api): token-gated /api/v1 surface with /api/v1/health"
```

---

## Task 6: `/api/v1/version` and `/api/v1/shutdown` aliases

**Files:**
- Modify: `vireo/app.py`
- Modify: `vireo/tests/test_api_v1_auth.py`

**Step 1: Write the failing test**

Append to `vireo/tests/test_api_v1_auth.py`:

```python
def test_api_v1_version(app_and_db):
    app, _ = app_and_db
    client = app.test_client()
    token = app.config["API_TOKEN"]
    resp = client.get("/api/v1/version", headers={"X-Vireo-Token": token})
    assert resp.status_code == 200
    assert "version" in resp.get_json()


def test_api_v1_shutdown_token_only(app_and_db, monkeypatch):
    """Unlike /api/shutdown, /api/v1/shutdown uses the token only (no
    X-Vireo-Shutdown header). The token itself blocks cross-origin attacks
    because browsers cannot set custom headers without CORS preflight."""
    app, _ = app_and_db
    client = app.test_client()
    token = app.config["API_TOKEN"]

    # Don't actually send SIGTERM during tests — stub os.kill.
    import os as _os
    killed = []
    monkeypatch.setattr(_os, "kill", lambda pid, sig: killed.append((pid, sig)))

    resp = client.post("/api/v1/shutdown", headers={"X-Vireo-Token": token})
    assert resp.status_code == 200
    assert resp.get_json()["status"] == "shutting_down"
    # The shutdown timer runs in a thread with a 0.5s delay; we don't need to
    # wait for it here — the response is the contract.
```

**Step 2: Run test to verify it fails**

Run: `python -m pytest vireo/tests/test_api_v1_auth.py -v`
Expected: FAIL — `/api/v1/version` and `/api/v1/shutdown` return 404.

**Step 3: Write minimal implementation**

In `vireo/app.py`, add right after `/api/v1/health`:

```python
@app.route("/api/v1/version")
def api_v1_version():
    return api_version()  # reuse existing implementation

@app.route("/api/v1/shutdown", methods=["POST"])
def api_v1_shutdown():
    import signal
    import threading

    def _shutdown():
        os.kill(os.getpid(), signal.SIGTERM)

    threading.Timer(0.5, _shutdown).start()
    return jsonify({"status": "shutting_down"})
```

Note: `api_version` is defined later in `create_app` (line 3162). Flask closures resolve at request time, not definition time, so the forward reference is fine — but only if the function is defined in the same scope. Verify by running the test.

If the forward reference is an issue (it shouldn't be, but Flask's debugger can surprise), move the `/api/v1/version` route to live adjacent to `/api/version` at line 3162 instead of clustering all v1 routes together.

**Step 4: Run test to verify it passes**

Run: `python -m pytest vireo/tests/test_api_v1_auth.py -v`
Expected: all PASS.

**Step 5: Commit**

```bash
git add vireo/app.py vireo/tests/test_api_v1_auth.py
git commit -m "feat(api): /api/v1/version and /api/v1/shutdown"
```

---

## Task 7: `/api/v1` read aliases for photos, collections, workspaces, keywords

**Files:**
- Modify: `vireo/app.py`
- Create: `vireo/tests/test_api_v1_aliases.py`

**Step 1: Write the failing test**

Create `vireo/tests/test_api_v1_aliases.py`:

```python
def _auth(app):
    return {"X-Vireo-Token": app.config["API_TOKEN"]}


def test_api_v1_photos_returns_list(app_and_db):
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get("/api/v1/photos", headers=_auth(app))
    assert resp.status_code == 200
    assert isinstance(resp.get_json(), (list, dict))


def test_api_v1_photo_by_id(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    # pick any existing photo
    photos = db.list_photos()
    pid = photos[0]["id"]
    resp = client.get(f"/api/v1/photos/{pid}", headers=_auth(app))
    assert resp.status_code == 200
    assert resp.get_json()["id"] == pid


def test_api_v1_collections(app_and_db):
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get("/api/v1/collections", headers=_auth(app))
    assert resp.status_code == 200


def test_api_v1_workspaces(app_and_db):
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get("/api/v1/workspaces", headers=_auth(app))
    assert resp.status_code == 200


def test_api_v1_keywords(app_and_db):
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get("/api/v1/keywords", headers=_auth(app))
    assert resp.status_code == 200
    names = {k["name"] for k in resp.get_json()}
    assert "Cardinal" in names


def test_api_v1_workspace_activate(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    ws_id = db.list_workspaces()[0]["id"]
    resp = client.post(
        f"/api/v1/workspaces/{ws_id}/activate", headers=_auth(app)
    )
    assert resp.status_code == 200
```

Before running: verify the exact shape of `db.list_photos()` and `db.list_workspaces()` return values by grepping — if the call signatures differ, adjust the test. (e.g. `Database` may use different names like `get_photos`, `get_workspaces`.) Replace the test if necessary to use whatever the `@app.route("/api/photos")` handler already uses for its data source.

**Step 2: Run test to verify it fails**

Run: `python -m pytest vireo/tests/test_api_v1_aliases.py -v`
Expected: FAIL — all `/api/v1/...` endpoints return 404.

**Step 3: Write minimal implementation**

In `vireo/app.py`, register aliases using `add_url_rule` immediately before `return app` (around line 7903). This approach routes both `/api/photos` and `/api/v1/photos` to the same view function, avoiding copy-pasted handlers:

```python
# --- /api/v1/* aliases over the stable subset of /api/* ---
# These are the endpoints advertised to external callers in docs/headless-api.md.
# Keep this list tight — expanding it locks the surface.
_V1_ALIASES = [
    # (v1 path, existing endpoint name, methods)
    ("/api/v1/photos", "api_photos", ["GET"]),
    ("/api/v1/photos/<int:photo_id>", "api_photo", ["GET"]),
    ("/api/v1/collections", "api_collections", ["GET"]),
    ("/api/v1/collections/<int:collection_id>/photos",
     "api_collection_photos", ["GET"]),
    ("/api/v1/workspaces", "api_workspaces_list", ["GET"]),
    ("/api/v1/workspaces/<int:ws_id>/activate",
     "api_workspace_activate", ["POST"]),
    ("/api/v1/keywords", "api_keywords", ["GET"]),
]

for v1_path, endpoint_name, methods in _V1_ALIASES:
    view = app.view_functions.get(endpoint_name)
    if view is None:
        raise RuntimeError(
            f"Cannot alias {v1_path}: endpoint '{endpoint_name}' not registered"
        )
    app.add_url_rule(
        v1_path,
        endpoint=f"v1_{endpoint_name}",
        view_func=view,
        methods=methods,
    )
```

**Note on endpoint names:** Flask's default endpoint name is the function name. The helper name next to each route (e.g. `def api_photos(...)`) is the endpoint name. Before running, `grep -nE "def api_(photos|photo|collections|collection_photos|workspaces_list|workspace_activate|keywords)\b" vireo/app.py` and verify the names match. If any differ (e.g. `api_list_photos` vs `api_photos`), update the `_V1_ALIASES` list.

**Step 4: Run test to verify it passes**

Run: `python -m pytest vireo/tests/test_api_v1_aliases.py -v`
Expected: all PASS.

**Step 5: Commit**

```bash
git add vireo/app.py vireo/tests/test_api_v1_aliases.py
git commit -m "feat(api): /api/v1 read aliases for photos, collections, workspaces, keywords"
```

---

## Task 8: `--headless` flag and wire writer + guard into `main()`

**Files:**
- Modify: `vireo/app.py` (`main()` around lines 7906–7992)

**Step 1: Write the failing test**

We don't need a unit test for CLI plumbing; the integration test in Task 9 covers this end-to-end. But add a small smoke test first.

Create `vireo/tests/test_main_cli.py`:

```python
import subprocess
import sys
from pathlib import Path


def test_help_includes_headless_flag():
    repo = Path(__file__).resolve().parents[2]
    result = subprocess.run(
        [sys.executable, str(repo / "vireo" / "app.py"), "--help"],
        capture_output=True, text=True, timeout=30,
    )
    assert result.returncode == 0
    assert "--headless" in result.stdout
```

**Step 2: Run test to verify it fails**

Run: `python -m pytest vireo/tests/test_main_cli.py -v`
Expected: FAIL — `--headless` not in help output.

**Step 3: Write minimal implementation**

Modify `main()` in `vireo/app.py`:

1. Add the flag after `parser.add_argument("--no-browser", action="store_true")` (around line 7919):
   ```python
   parser.add_argument(
       "--headless",
       action="store_true",
       help="Run without opening a browser; write runtime.json and enable "
            "the /api/v1 API. Use this when invoking the sidecar directly "
            "from scripts or agents.",
   )
   ```

2. Remove the legacy `--port 0` → `~/.vireo/port` block (lines 7951–7956). The Tauri Rust code does not read this file; `runtime.json` supersedes it.

3. After the port is resolved (after line 7949) and before `create_app(...)` (line 7958), add:
   ```python
   from runtime import (
       check_single_instance,
       delete_runtime_json,
       generate_token,
       write_runtime_json,
   )

   status, info = check_single_instance()
   if status == "conflict":
       import sys as _sys
       _sys.stderr.write(json.dumps({
           "error": "already_running",
           "port": info["port"],
           "pid": info["pid"],
       }) + "\n")
       raise SystemExit(1)

   api_token = generate_token()
   mode = "headless" if args.headless else "gui"
   ```

4. Pass the token to `create_app`:
   ```python
   app = create_app(
       db_path=args.db, thumb_cache_dir=args.thumb_dir, api_token=api_token,
   )
   ```

5. Immediately before `app.run(...)` (line 7992), write runtime.json and register cleanup:
   ```python
   import atexit
   import signal as _signal

   # Look up the running version from pyproject (same fallback chain as /api/version).
   try:
       from importlib.metadata import version as pkg_version
       ver = pkg_version("vireo")
   except Exception:
       ver = "0.0.0"

   write_runtime_json(
       port=port, pid=os.getpid(), version=ver, db_path=args.db,
       token=api_token, mode=mode,
   )
   atexit.register(delete_runtime_json)
   _signal.signal(_signal.SIGTERM, lambda *_: (delete_runtime_json(), os._exit(0)))
   ```

6. Make `--headless` imply `--no-browser`. At the top of `main()` after `args = parser.parse_args()`:
   ```python
   if args.headless:
       args.no_browser = True
   ```

**Step 4: Run test to verify it passes**

Run: `python -m pytest vireo/tests/test_main_cli.py vireo/tests/test_app.py vireo/tests/test_api_v1_auth.py vireo/tests/test_api_v1_aliases.py vireo/tests/test_runtime.py -v`
Expected: all PASS.

**Step 5: Commit**

```bash
git add vireo/app.py vireo/tests/test_main_cli.py
git commit -m "feat(app): --headless flag; wire runtime.json + single-instance guard"
```

---

## Task 9: Integration test — spawn headless, discover, hit /api/v1/health

**Files:**
- Create: `tests/test_headless_integration.py`

**Step 1: Write the failing test**

Create `tests/test_headless_integration.py`:

```python
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
```

**Step 2: Run test to verify it passes after Task 8**

Run: `python -m pytest tests/test_headless_integration.py -v`
Expected: PASS. (If the test is failing, the fault is in Tasks 1–8 — fix the real bug, do not adjust the test.)

**Step 3: Commit**

```bash
git add tests/test_headless_integration.py
git commit -m "test: headless sidecar spawn writes runtime.json and serves /api/v1/health"
```

---

## Task 10: Integration test — second spawn is rejected

**Files:**
- Modify: `tests/test_headless_integration.py`

**Step 1: Write the failing test**

Append to `tests/test_headless_integration.py`:

```python
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
```

**Step 2: Run test**

Run: `python -m pytest tests/test_headless_integration.py -v`
Expected: both tests PASS.

**Step 3: Commit**

```bash
git add tests/test_headless_integration.py
git commit -m "test: second headless spawn exits with already_running"
```

---

## Task 11: Integration test — stale runtime.json is cleaned up

**Files:**
- Modify: `tests/test_headless_integration.py`

**Step 1: Write the failing test**

Append to `tests/test_headless_integration.py`:

```python
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
```

**Step 2: Run test**

Run: `python -m pytest tests/test_headless_integration.py -v`
Expected: all three tests PASS.

**Step 3: Commit**

```bash
git add tests/test_headless_integration.py
git commit -m "test: stale runtime.json is replaced on next spawn"
```

---

## Task 12: Integration test — graceful shutdown removes runtime.json

**Files:**
- Modify: `tests/test_headless_integration.py`

**Step 1: Write the failing test**

Append to `tests/test_headless_integration.py`:

```python
def test_shutdown_endpoint_removes_runtime_json(headless_home, tmp_path):
    db = tmp_path / "vireo.db"
    port = _free_port()
    proc = _spawn(headless_home, db, port)
    try:
        runtime = headless_home / ".vireo" / "runtime.json"
        data = _wait_for_runtime(runtime)

        req = urllib.request.Request(
            f"http://127.0.0.1:{port}/api/v1/shutdown",
            method="POST",
            headers={"X-Vireo-Token": data["token"]},
        )
        urllib.request.urlopen(req, timeout=3).read()

        # Wait for the process to exit (shutdown timer + signal).
        proc.wait(timeout=10)
        assert proc.returncode == 0 or proc.returncode is not None

        # runtime.json should be gone.
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline and runtime.exists():
            time.sleep(0.1)
        assert not runtime.exists(), "runtime.json still present after shutdown"
    finally:
        if proc.poll() is None:
            proc.send_signal(signal.SIGTERM)
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
```

**Step 2: Run test**

Run: `python -m pytest tests/test_headless_integration.py -v`
Expected: all four tests PASS.

**Step 3: Commit**

```bash
git add tests/test_headless_integration.py
git commit -m "test: /api/v1/shutdown removes runtime.json on exit"
```

---

## Task 13: User-facing docs

**Files:**
- Create: `docs/headless-api.md`
- Modify: `README.md` (add a single-line link under a "Scripting / automation" heading)

**Step 1: Write `docs/headless-api.md`**

Create `docs/headless-api.md`:

````markdown
# Vireo headless API

Vireo is primarily a desktop app, but its Flask server also exposes a small
stable HTTP API for scripts and agents. This page documents what callers can
rely on.

## Discovering a running instance

When the Vireo sidecar starts (either as part of the Tauri GUI or directly
from the command line), it writes `~/.vireo/runtime.json`:

```json
{
  "port": 54321,
  "pid": 12345,
  "version": "0.8.28",
  "db_path": "/Users/you/.vireo/vireo.db",
  "started_at": "2026-04-22T19:30:00Z",
  "mode": "gui",
  "token": "…"
}
```

The file is `chmod 600` (user-only read) because the token is sensitive.

**Liveness:** treat `runtime.json` as authoritative only after confirming
`GET http://127.0.0.1:<port>/api/v1/health` (with the token) returns 200.
If the file is stale (process died without cleanup), the next sidecar start
will replace it automatically.

## Spawning a headless instance

If no instance is running, spawn one from the installed `.app`:

```bash
/Applications/Vireo.app/Contents/Resources/bin/vireo-server \
  --headless --port 0 --db ~/.vireo/vireo.db
```

`--port 0` picks a free port. Poll `~/.vireo/runtime.json` until it appears
(typically <2 s), then read the port and token from it.

**Only one Vireo instance can run at a time.** If the GUI is already open,
the headless spawn will exit with a non-zero status and print a JSON error
to stderr:

```json
{"error":"already_running","port":54321,"pid":12345}
```

In that case, just connect to the running instance using the port and token
in `runtime.json` — both modes serve the same API.

## Authentication

Every `/api/v1/*` request must include the token from `runtime.json`:

```
X-Vireo-Token: <token>
```

Missing or wrong token → 401.

## Stable endpoints

Endpoints under `/api/v1` are covered by a semver contract: breaking changes
require bumping the version prefix. Everything else under `/api/*` is
internal to the GUI and may change at any time.

Current stable set:

| Method | Path | Description |
| --- | --- | --- |
| GET  | `/api/v1/health` | Liveness probe. |
| GET  | `/api/v1/version` | `{"version": "x.y.z"}`. |
| POST | `/api/v1/shutdown` | Gracefully stop the sidecar. |
| GET  | `/api/v1/photos` | List/search photos in the active workspace. |
| GET  | `/api/v1/photos/<id>` | One photo's metadata. |
| GET  | `/api/v1/collections` | Collections in the active workspace. |
| GET  | `/api/v1/collections/<id>/photos` | Photos in a collection. |
| GET  | `/api/v1/workspaces` | All workspaces. |
| POST | `/api/v1/workspaces/<id>/activate` | Switch the active workspace. |
| GET  | `/api/v1/keywords` | Keyword tree. |

Request/response shapes mirror the internal `/api/*` endpoints — see source
or open `/api/v1/<path>` with the token in your browser's dev tools for
concrete examples.

## Worked example

```bash
# 1. Locate the instance.
PORT=$(jq -r .port ~/.vireo/runtime.json)
TOKEN=$(jq -r .token ~/.vireo/runtime.json)

# 2. Probe health.
curl -sf -H "X-Vireo-Token: $TOKEN" "http://127.0.0.1:$PORT/api/v1/health"

# 3. List photos.
curl -sf -H "X-Vireo-Token: $TOKEN" "http://127.0.0.1:$PORT/api/v1/photos"

# 4. Shut down (only if you spawned the instance yourself).
curl -sf -X POST -H "X-Vireo-Token: $TOKEN" \
     "http://127.0.0.1:$PORT/api/v1/shutdown"
```
````

**Step 2: Link from `README.md`**

Add a short section to `README.md` pointing at the new doc. One paragraph, max.

**Step 3: Verify both files render cleanly**

Open them in a Markdown previewer (or just `cat`) and check for broken fences.

**Step 4: Commit**

```bash
git add docs/headless-api.md README.md
git commit -m "docs: user-facing guide for the /api/v1 headless surface"
```

---

## Task 14: Final verification

**Step 1: Run the full project test suite**

Run:
```bash
python -m pytest tests/ vireo/tests/ -v
```
Expected: all pass, including the new runtime, auth, alias, CLI, and integration tests.

**Step 2: Manual smoke test (optional, recommended before merge)**

In a terminal:
```bash
python vireo/app.py --headless --port 0 --db /tmp/vireo-smoke.db
```

In a second terminal:
```bash
cat ~/.vireo/runtime.json
PORT=$(jq -r .port ~/.vireo/runtime.json)
TOKEN=$(jq -r .token ~/.vireo/runtime.json)
curl -sf -H "X-Vireo-Token: $TOKEN" http://127.0.0.1:$PORT/api/v1/health
curl -sf -X POST -H "X-Vireo-Token: $TOKEN" http://127.0.0.1:$PORT/api/v1/shutdown
```

Confirm:
- `runtime.json` appears with the right fields and `chmod 600`.
- Health probe returns `{"status":"ok"}`.
- Shutdown endpoint returns, process exits cleanly, `runtime.json` is removed.

**Step 3: Open the PR**

Use the commit log as the PR description skeleton. Link to
`docs/plans/2026-04-22-headless-api-design.md` for context.

```bash
gh pr create --title "Headless API for external callers" --body "$(cat <<'EOF'
## Summary
- Add `~/.vireo/runtime.json` (port, PID, token, version, mode) so external
  callers can discover a running Vireo instance.
- Single-instance guard: sidecar refuses to start if a healthy peer is
  running; detects and cleans up stale runtime files.
- New `/api/v1/*` surface with `X-Vireo-Token` auth, covering health,
  version, shutdown, photos, collections, workspaces, and keywords.
- `--headless` flag on the sidecar for explicit non-GUI spawns.
- User-facing docs at `docs/headless-api.md`.

Design: `docs/plans/2026-04-22-headless-api-design.md`.

## Test plan
- [x] Unit tests: runtime.json writer/reader, single-instance guard, token gen
- [x] API tests: /api/v1 token auth, /api/v1 alias coverage
- [x] Integration: spawn → discover → health; second spawn → already_running;
      stale runtime.json replaced; /api/v1/shutdown removes runtime.json
- [ ] Manual smoke: `python vireo/app.py --headless --port 0` + curl

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

---

## Non-tasks (out of scope)

- Installing a `vireo` CLI launcher at `/usr/local/bin`. Users invoke the
  in-bundle binary directly.
- Supporting two Vireo instances simultaneously.
- Freezing every `/api/*` endpoint; only `/api/v1/*` is stable.
- Remote / non-loopback access.
- Migrating the Tauri Rust sidecar code to read `runtime.json`. The Rust
  code still picks a port itself and passes it via `--port`; the new file
  is purely additive for external callers.
