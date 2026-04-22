"""Runtime discovery file for the Vireo sidecar.

Writes `~/.vireo/runtime.json` so external callers can discover the running
instance (port, auth token, PID). Also provides the single-instance guard,
anchored to a kernel-managed file lock on `~/.vireo/runtime.lock`
(`fcntl.flock` on POSIX, `msvcrt.locking` on Windows). The kernel
releases the lock when the owning process dies, so the guard is
self-healing after unclean crashes (SIGKILL, power loss, OOM) — the next
startup reclaims the slot without manual cleanup, even if the dead PID
has been recycled by an unrelated process.
"""

import contextlib
import json
import os
import secrets
import urllib.error
import urllib.request
from datetime import UTC, datetime
from pathlib import Path

try:
    import fcntl  # POSIX
    _HAS_FCNTL = True
except ImportError:
    _HAS_FCNTL = False

try:
    import msvcrt  # Windows
    _HAS_MSVCRT = True
except ImportError:
    _HAS_MSVCRT = False


# A Vireo-specific marker echoed by /api/v1/health. The probe verifies this
# value so an unrelated local service that happens to return 200 for
# /api/v1/health cannot be mistaken for a live Vireo peer and cause a
# false `already_running`.
SERVICE_MARKER = "vireo"

# One byte is enough to uniquely identify the lock region on Windows;
# msvcrt.locking takes a length and locks a byte range starting at the
# current file position.
_LOCK_BYTES = 1


def _runtime_path() -> Path:
    return Path(os.path.expanduser("~/.vireo/runtime.json"))


def _lock_path() -> Path:
    return Path(os.path.expanduser("~/.vireo/runtime.lock"))


# Module-global FD. When this process owns the single-instance slot, we
# hold an exclusive kernel lock on the lock file via this FD. Keeping
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
    # Create with 0600 from creation. Using write_text() + chmod leaves a
    # window where the umask-masked default (e.g. 0644) is visible to other
    # local users — long enough for a co-tenant to read the auth token.
    # O_EXCL guarantees the `mode` argument is applied (an existing file
    # from a prior crash would keep its old — possibly permissive — perms),
    # so we unlink any leftover tmp first.
    with contextlib.suppress(FileNotFoundError):
        tmp.unlink()
    data = json.dumps(payload, indent=2).encode()
    fd = os.open(str(tmp), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        # os.write is allowed to do partial writes even on regular files
        # under signal/resource pressure. If we os.replace() a truncated
        # payload into place, external callers read malformed runtime.json
        # and discovery fails — loop until the full buffer is flushed.
        view = memoryview(data)
        while view:
            n = os.write(fd, view)
            if n <= 0:
                raise OSError("runtime.json.tmp: write returned zero bytes")
            view = view[n:]
    finally:
        os.close(fd)
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

    Used only for diagnostic info (info["pid"] on conflict); the guard
    itself trusts the kernel-managed file lock, not this check.
    """
    if not isinstance(pid, int) or pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return True


def _try_take_lock(fd: int) -> bool:
    """Take an exclusive, non-blocking, kernel-managed lock on fd.

    Returns True on success (we now own the lock for the lifetime of this
    FD), False if another process already holds it. The kernel releases
    the lock automatically when the owning process exits or the FD is
    closed, which is what makes the guard self-healing after unclean
    crashes and invulnerable to PID reuse.
    """
    if _HAS_FCNTL:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return True
        except (BlockingIOError, OSError):
            return False
    if _HAS_MSVCRT:
        try:
            os.lseek(fd, 0, os.SEEK_SET)
            msvcrt.locking(fd, msvcrt.LK_NBLCK, _LOCK_BYTES)
            return True
        except OSError:
            return False
    # No kernel-managed locking available; callers must treat this as
    # an unsupported platform. We avoid raising here so the module can
    # still import; release_single_instance handles the no-op case.
    return False


def _release_lock(fd: int) -> None:
    """Release the kernel lock on fd. Idempotent; swallows errors."""
    if _HAS_FCNTL:
        with contextlib.suppress(OSError):
            fcntl.flock(fd, fcntl.LOCK_UN)
    elif _HAS_MSVCRT:
        with contextlib.suppress(OSError):
            os.lseek(fd, 0, os.SEEK_SET)
            msvcrt.locking(fd, msvcrt.LK_UNLCK, _LOCK_BYTES)


def _is_lock_held_by_peer() -> bool:
    """Return True if another process currently holds the runtime lock.

    Opens the lock file and tries to acquire it non-blocking. If we
    succeed, nobody else holds it (release immediately, report no peer).
    If the kernel refuses, another process holds it — and since the
    kernel releases locks on process death, "held" reliably means "live
    peer", regardless of PID reuse.
    """
    lock_path = _lock_path()
    if not lock_path.exists():
        return False
    try:
        fd = os.open(str(lock_path), os.O_RDWR)
    except OSError:
        return False
    try:
        if _try_take_lock(fd):
            _release_lock(fd)
            return False
        return True
    finally:
        os.close(fd)


def check_single_instance(probe_timeout_s: float = 0.5) -> tuple[str, dict | None]:
    """Check whether another Vireo instance is healthy on the advertised port.

    Returns:
        ("proceed", None)  — no peer found, or stale file cleaned up.
        ("conflict", info) — live peer. `info` has keys `port`, `pid`.

    A 200 from `/api/v1/health` (with the token from runtime.json) is
    a necessary but not sufficient signal — we also require the response
    body to carry the Vireo service marker, so an unrelated local
    service that happens to return 200 on that path cannot be mistaken
    for a live Vireo peer. Non-200 responses are treated as stale.

    Connection-level failures (refused/timeout) are ambiguous: the peer
    may be dead, or it may be a peer still booting that wrote runtime.json
    before Flask bound. We disambiguate via the kernel-managed lock —
    held = live owner (PID-reuse-proof), unheld = owner crashed, stale.
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
            if resp.status == 200 and _response_is_vireo(resp):
                return ("conflict", {"port": port, "pid": pid})
            # 200 without the Vireo marker, or any other 2xx, is not
            # proof of a live Vireo peer — fall through to stale.
    except urllib.error.HTTPError:
        # Non-2xx (401, 404, 500, ...) — port is held by something that isn't
        # answering our health contract. Treat as stale.
        pass
    except (urllib.error.URLError, TimeoutError, OSError):
        # Connection refused / timeout. Trust the kernel-managed lock,
        # not the raw PID — a held lock means a live Vireo peer (possibly
        # still booting), while an unheld lock means the previous owner
        # is gone and runtime.json is stale, even if the PID happens to
        # have been recycled by an unrelated process.
        if _is_lock_held_by_peer():
            return ("conflict", {"port": port, "pid": pid})

    delete_runtime_json()
    return ("proceed", None)


def _response_is_vireo(resp) -> bool:
    """Return True if the health response carries the Vireo service marker.

    Reading the body is bounded to a few hundred bytes — /api/v1/health
    is tiny, and we only need to inspect the JSON envelope.
    """
    try:
        body = resp.read(1024)
        data = json.loads(body.decode("utf-8", errors="replace"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return False
    return isinstance(data, dict) and data.get("service") == SERVICE_MARKER


def acquire_single_instance(
    pid: int, probe_timeout_s: float = 0.5, max_retries: int = 5
) -> tuple[str, dict | None]:
    """Atomically reserve the single-instance slot via a kernel file lock.

    Opens `~/.vireo/runtime.lock` and takes an exclusive, non-blocking
    lock (fcntl.flock on POSIX, msvcrt.locking on Windows). Only one
    process can hold the lock at a time, so the guard both (a) closes
    the boot race where two near-simultaneous launches both see an
    empty runtime.json and both start serving, and (b) self-heals after
    unclean crashes — the kernel releases the lock when the owning
    process dies, so the next startup can reclaim it even if the dead
    PID has been recycled by an unrelated process.

    The PID written inside the lock file is diagnostic only; liveness
    is determined by the kernel lock, not by reading the file.

    Returns:
        ("acquired", None) — caller holds the lock. Must call
            `release_single_instance` on shutdown (e.g. via atexit /
            SIGTERM handlers) to release the lock and unlink the file.
        ("conflict", info) — a peer is alive; caller must not start.
    """
    global _lock_fd
    lock_path = _lock_path()
    lock_path.parent.mkdir(parents=True, exist_ok=True)

    for _attempt in range(max_retries):
        # A healthy peer detected via runtime.json always wins, even if
        # the lock happens to be unheld at this instant.
        status, info = check_single_instance(probe_timeout_s=probe_timeout_s)
        if status == "conflict":
            return ("conflict", info)

        # Open the lock file. If this raises (e.g. unreadable `~/.vireo`,
        # permission denied, filesystem fault), let the OSError propagate:
        # misclassifying a startup fault as "already_running" sends the
        # user to the wrong remediation and can block startup indefinitely
        # when no peer exists.
        fd = os.open(str(lock_path), os.O_CREAT | os.O_RDWR, 0o600)

        if not _try_take_lock(fd):
            # Another process holds the lock — a real peer is alive
            # (kernel-guaranteed).
            holder_pid = _read_lock_holder(lock_path)
            os.close(fd)
            return ("conflict", {"port": None, "pid": holder_pid})

        # Lock acquired. Write our PID for diagnostics, then keep the FD
        # open so the lock persists until release / process exit.
        try:
            os.lseek(fd, 0, os.SEEK_SET)
            os.ftruncate(fd, 0)
            os.write(fd, str(pid).encode())
        except OSError:
            _release_lock(fd)
            os.close(fd)
            continue

        _lock_fd = fd
        return ("acquired", None)

    return ("conflict", {"port": None, "pid": None})


def release_single_instance() -> None:
    """Release the kernel lock and close the FD. Idempotent.

    The lock file itself is intentionally left on disk. flock() binds to
    the inode, not the path — if we unlink after close, a newcomer that
    already opened the old inode and holds a lock on it keeps that lock,
    while the next starter creates a fresh inode and also locks it. Two
    processes end up holding locks on different inodes, defeating the
    single-instance guarantee. Leaving the file in place forces every
    process to converge on the same inode.
    """
    global _lock_fd
    if _lock_fd is not None:
        _release_lock(_lock_fd)
        with contextlib.suppress(OSError):
            os.close(_lock_fd)
        _lock_fd = None


def _read_lock_holder(lock_path: Path) -> int:
    try:
        return int(lock_path.read_text().strip())
    except (OSError, ValueError):
        return 0


def generate_token() -> str:
    """Return a URL-safe random token suitable for API auth."""
    return secrets.token_urlsafe(32)
