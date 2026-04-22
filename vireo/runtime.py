"""Runtime discovery file for the Vireo sidecar.

Writes `~/.vireo/runtime.json` so external callers can discover the running
instance (port, auth token, PID). Also provides the single-instance guard,
including an atomic reservation step (via a separate lock file) so two
near-simultaneous launches cannot both observe an empty slot and both
start serving.
"""

import contextlib
import errno
import json
import os
import secrets
import urllib.error
import urllib.request
from datetime import UTC, datetime
from pathlib import Path


def _runtime_path() -> Path:
    return Path(os.path.expanduser("~/.vireo/runtime.json"))


def _lock_path() -> Path:
    return Path(os.path.expanduser("~/.vireo/runtime.lock"))


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
    with contextlib.suppress(FileNotFoundError):
        _runtime_path().unlink()


def _pid_alive(pid) -> bool:
    """Best-effort liveness check for a PID from another process.

    On Unix, `os.kill(pid, 0)` raises ProcessLookupError when the pid is
    gone. On Windows, os.kill with signal 0 is not fully portable; fall
    back to treating unknown errors as "alive" so the guard errs on the
    side of refusing to start a second instance.
    """
    if not isinstance(pid, int) or pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        # Process exists but we can't signal it.
        return True
    except OSError:
        return True


def check_single_instance(probe_timeout_s: float = 0.5) -> tuple[str, dict | None]:
    """Check whether another Vireo instance is healthy on the advertised port.

    Returns:
        ("proceed", None)  — no peer found, or stale file cleaned up.
        ("conflict", info) — live peer. `info` has keys `port`, `pid`.

    The probe requires a 200 from `/api/v1/health` (with the token from
    runtime.json) to classify the peer as alive. Non-200 responses — 401,
    404, 500, etc. — are treated as stale: the port may have been reused
    by an unrelated local service, so we clean the file and proceed.

    Connection-level failures (refused/timeout) are ambiguous: the peer
    may be dead, or it may be a peer still booting that wrote
    runtime.json but hasn't started listening yet. We disambiguate via
    the PID — a live PID means "booting or transient", and we must not
    delete runtime.json under the running peer, because that would
    break external discovery even though the peer is still running.
    """
    data = read_runtime_json()
    if data is None:
        # Missing or malformed. If malformed, the file still exists on disk;
        # delete it so the next caller has a clean slate.
        delete_runtime_json()
        return ("proceed", None)

    port = data.get("port")
    token = data.get("token", "")
    pid = data.get("pid")
    if not isinstance(port, int) or not isinstance(token, str):
        delete_runtime_json()
        return ("proceed", None)

    try:
        req = urllib.request.Request(
            f"http://127.0.0.1:{port}/api/v1/health",
            headers={"X-Vireo-Token": token},
        )
        with urllib.request.urlopen(req, timeout=probe_timeout_s) as resp:
            if resp.status == 200:
                return ("conflict", {"port": port, "pid": pid})
            # Any other 2xx is unexpected from our own health endpoint; fall
            # through to the stale path.
    except urllib.error.HTTPError:
        # Non-2xx (401, 404, 500, ...) — port is held by something that isn't
        # answering our health contract. Treat as stale.
        pass
    except (urllib.error.URLError, TimeoutError, OSError):
        # Connection refused / timeout. If the advertised PID is still
        # alive, this is almost certainly a peer that wrote runtime.json
        # and is still booting (or briefly paused). Preserve runtime.json
        # so external callers can still discover it once HTTP is up, and
        # report conflict to the caller.
        if _pid_alive(pid):
            return ("conflict", {"port": port, "pid": pid})
        # PID dead — peer is gone, file is stale.

    delete_runtime_json()
    return ("proceed", None)


def acquire_single_instance(
    pid: int, probe_timeout_s: float = 0.5, max_retries: int = 5
) -> tuple[str, dict | None]:
    """Atomically reserve the single-instance slot via a lock file.

    Creates `~/.vireo/runtime.lock` with `O_CREAT | O_EXCL`, writing the
    caller's PID. This closes the race where two near-simultaneous
    launches both call `check_single_instance`, both see no runtime.json,
    and both start serving before either has written one. Only one
    process can win the O_EXCL create; the other returns "conflict".

    The lock file is separate from runtime.json to keep runtime.json's
    external contract clean — it still either does not exist or contains
    the full payload of a running instance.

    Returns:
        ("acquired", None) — caller holds the lock. Must call
            `release_single_instance` on shutdown (e.g. via atexit /
            SIGTERM handlers).
        ("conflict", info) — a peer is alive; caller must not start.

    If a stale `runtime.lock` is found (holder PID no longer alive), it
    is removed and the caller retries. If a live `runtime.json` peer is
    found, we return conflict regardless of lock state.
    """
    lock_path = _lock_path()
    lock_path.parent.mkdir(parents=True, exist_ok=True)

    for _attempt in range(max_retries):
        # First, probe runtime.json for a live peer. A healthy peer always
        # wins, even if the lock happens to be missing.
        status, info = check_single_instance(probe_timeout_s=probe_timeout_s)
        if status == "conflict":
            return ("conflict", info)

        # Publish the lock atomically with its PID payload. We write the PID
        # to a per-caller temp file first, then use os.link to move it into
        # place — os.link fails if the destination exists, and the file is
        # never observable in an empty state. This closes the race where a
        # concurrent caller sees a freshly-created empty lock file, reads
        # PID=0, classifies it as stale, and unlinks the winner's lock.
        tmp = lock_path.parent / f"runtime.lock.tmp.{os.getpid()}.{pid}"
        try:
            tmp.write_text(str(pid))
            os.chmod(tmp, 0o600)
            try:
                os.link(str(tmp), str(lock_path))
            except FileExistsError:
                # Another process holds (or left) the lock.
                holder_pid = _read_lock_holder(lock_path)
                if _pid_alive(holder_pid):
                    return ("conflict", {"port": None, "pid": holder_pid})
                # Stale lock — remove and retry. If someone else won the
                # unlink/recreate race in the meantime, the next loop
                # iteration will observe their live lock and conflict.
                with contextlib.suppress(FileNotFoundError):
                    lock_path.unlink()
                continue
            except OSError as e:
                if e.errno == errno.EEXIST:
                    continue
                raise
            else:
                return ("acquired", None)
        finally:
            with contextlib.suppress(FileNotFoundError):
                tmp.unlink()

    # Exhausted retries — some other process keeps winning the race. Treat
    # as a conflict rather than looping forever.
    return ("conflict", {"port": None, "pid": None})


def release_single_instance() -> None:
    """Remove the reservation lock file. Idempotent."""
    with contextlib.suppress(FileNotFoundError):
        _lock_path().unlink()


def _read_lock_holder(lock_path: Path) -> int:
    try:
        return int(lock_path.read_text().strip())
    except (OSError, ValueError):
        return 0


def generate_token() -> str:
    """Return a URL-safe random token suitable for API auth."""
    return secrets.token_urlsafe(32)
