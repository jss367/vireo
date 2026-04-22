"""Runtime discovery file for the Vireo sidecar.

Writes `~/.vireo/runtime.json` so external callers can discover the running
instance (port, auth token, PID). Also provides the single-instance guard,
anchored to an `fcntl.flock` on `~/.vireo/runtime.lock`. The kernel
releases the flock when the owning process dies, so the guard is
self-healing after unclean crashes (SIGKILL, power loss, OOM) — the next
startup reclaims the slot without manual cleanup, even if the dead PID
has been recycled by an unrelated process.
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

try:
    import fcntl  # POSIX only
    _HAS_FCNTL = True
except ImportError:
    _HAS_FCNTL = False


def _runtime_path() -> Path:
    return Path(os.path.expanduser("~/.vireo/runtime.json"))


def _lock_path() -> Path:
    return Path(os.path.expanduser("~/.vireo/runtime.lock"))


# Module-global FD. When this process owns the single-instance slot, we
# hold an exclusive fcntl.flock on the lock file via this FD. Keeping
# the FD open is what keeps the lock held; closing it (or the process
# dying) releases it.
_lock_fd: int | None = None


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


def _is_lock_held_by_peer() -> bool:
    """Return True if another process currently holds the runtime flock.

    Opens the lock file and attempts a non-blocking exclusive flock. If
    we can acquire it, nobody else holds it (we release immediately and
    report no peer). If flock raises BlockingIOError / EAGAIN, another
    process holds the lock — and since the kernel releases flocks on
    process exit, that other process is definitely alive, regardless
    of PID reuse.

    On platforms without fcntl (Windows) we fall back to the PID stored
    in the lock file as a best-effort signal.
    """
    lock_path = _lock_path()
    if not lock_path.exists():
        return False
    if not _HAS_FCNTL:
        return _pid_alive(_read_lock_holder(lock_path))
    try:
        fd = os.open(str(lock_path), os.O_RDONLY)
    except OSError:
        return False
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except (BlockingIOError, OSError):
            return True
        with contextlib.suppress(OSError):
            fcntl.flock(fd, fcntl.LOCK_UN)
        return False
    finally:
        os.close(fd)


def check_single_instance(probe_timeout_s: float = 0.5) -> tuple[str, dict | None]:
    """Check whether another Vireo instance is healthy on the advertised port.

    Returns:
        ("proceed", None)  — no peer found, or stale file cleaned up.
        ("conflict", info) — live peer. `info` has keys `port`, `pid`.

    A 200 from `/api/v1/health` (with the token from runtime.json)
    definitively classifies the peer as alive. Non-200 responses — 401,
    404, 500, etc. — are treated as stale: the port may have been reused
    by an unrelated local service.

    Connection-level failures (refused/timeout) are ambiguous: the peer
    may be dead, or it may be a peer still booting that wrote runtime.json
    before Flask bound. We disambiguate via the flock — a held lock means
    a live owner (kernel-guaranteed, PID-recycling-proof), an unheld lock
    means the owner crashed and runtime.json is stale.
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
        # Connection refused / timeout. Trust the kernel-managed flock,
        # not the raw PID — a live flock means a live Vireo peer (possibly
        # still booting), while an unheld flock means the previous owner
        # is gone and runtime.json is stale, even if the PID happens to
        # have been recycled by an unrelated process.
        if _is_lock_held_by_peer():
            return ("conflict", {"port": port, "pid": pid})

    delete_runtime_json()
    return ("proceed", None)


def acquire_single_instance(
    pid: int, probe_timeout_s: float = 0.5, max_retries: int = 5
) -> tuple[str, dict | None]:
    """Atomically reserve the single-instance slot via fcntl.flock.

    Opens `~/.vireo/runtime.lock` and takes an exclusive, non-blocking
    fcntl.flock. Only one process can hold the flock at a time, so the
    guard both (a) closes the boot race where two near-simultaneous
    launches both see an empty runtime.json and both start serving, and
    (b) self-heals after unclean crashes — the kernel releases the lock
    when the owning process dies, so the next startup can reclaim it
    even if the dead PID has been recycled.

    The PID written inside the lock file is diagnostic only; liveness is
    determined by the flock, not by reading the file.

    Returns:
        ("acquired", None) — caller holds the lock. Must call
            `release_single_instance` on shutdown (e.g. via atexit /
            SIGTERM handlers) to release the flock and unlink the file.
        ("conflict", info) — a peer is alive; caller must not start.

    On platforms without fcntl (Windows), falls back to an
    `O_CREAT | O_EXCL` + PID-liveness approach, which provides the same
    boot-race protection but not the crash-self-healing guarantee.
    """
    global _lock_fd
    lock_path = _lock_path()
    lock_path.parent.mkdir(parents=True, exist_ok=True)

    if not _HAS_FCNTL:
        return _acquire_without_flock(pid, probe_timeout_s, max_retries)

    for _attempt in range(max_retries):
        # A healthy peer detected via runtime.json always wins, even if
        # the flock happens to be unheld at this instant.
        status, info = check_single_instance(probe_timeout_s=probe_timeout_s)
        if status == "conflict":
            return ("conflict", info)

        try:
            fd = os.open(str(lock_path), os.O_CREAT | os.O_RDWR, 0o600)
        except OSError:
            continue

        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except (BlockingIOError, OSError):
            # Another process holds the lock — a real peer is alive
            # (kernel-guaranteed).
            holder_pid = _read_lock_holder(lock_path)
            os.close(fd)
            return ("conflict", {"port": None, "pid": holder_pid})

        # Lock acquired. Write our PID for diagnostics, then keep the FD
        # open so the flock persists until release / process exit.
        try:
            os.ftruncate(fd, 0)
            os.write(fd, str(pid).encode())
        except OSError:
            with contextlib.suppress(OSError):
                fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)
            continue

        _lock_fd = fd
        return ("acquired", None)

    return ("conflict", {"port": None, "pid": None})


def _acquire_without_flock(
    pid: int, probe_timeout_s: float, max_retries: int
) -> tuple[str, dict | None]:
    """Non-flock fallback for platforms without fcntl (Windows).

    Uses atomic hardlink publishing (os.link) so the lock file is never
    observable in an empty state, then classifies staleness by PID
    liveness. Does not self-heal from crash + PID-recycling (the core
    weakness Codex P2 flagged); that path needs a kernel-managed lock.
    """
    lock_path = _lock_path()
    for _attempt in range(max_retries):
        status, info = check_single_instance(probe_timeout_s=probe_timeout_s)
        if status == "conflict":
            return ("conflict", info)

        tmp = lock_path.parent / f"runtime.lock.tmp.{os.getpid()}.{pid}"
        try:
            tmp.write_text(str(pid))
            os.chmod(tmp, 0o600)
            try:
                os.link(str(tmp), str(lock_path))
            except FileExistsError:
                holder_pid = _read_lock_holder(lock_path)
                if _pid_alive(holder_pid):
                    return ("conflict", {"port": None, "pid": holder_pid})
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

    return ("conflict", {"port": None, "pid": None})


def release_single_instance() -> None:
    """Release the flock, close the FD, and unlink the lock file. Idempotent."""
    global _lock_fd
    if _lock_fd is not None:
        if _HAS_FCNTL:
            with contextlib.suppress(OSError):
                fcntl.flock(_lock_fd, fcntl.LOCK_UN)
        with contextlib.suppress(OSError):
            os.close(_lock_fd)
        _lock_fd = None
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
