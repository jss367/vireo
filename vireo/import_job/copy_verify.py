"""Copy-and-verify and destination-side hashing.

``copy_and_hash_verify`` copies one file through a temporary name and
promotes it only once its hash matches the source;
``_hash_dest_file`` reads destination bytes under the Stop signal and
a stall watchdog.
"""

from __future__ import annotations

import contextlib
import errno
import hashlib
import logging
import os
import shutil
import stat
import sys
import threading
import time
import uuid
from typing import TYPE_CHECKING

from file_replace import NoReplaceRenameUnsupported, rename_no_replace
from import_dedup import compute_file_hash

if sys.platform != "win32":
    import fcntl

if TYPE_CHECKING:
    from collections.abc import Callable

    from import_job.import_types import StopCheck

log = logging.getLogger(__name__)


# FAT/exFAT and some network shares reject hard links. A verified temp
# still needs no-overwrite publication on those destinations.
_HARDLINK_UNSUPPORTED_ERRNOS = frozenset(
    getattr(errno, name)
    for name in ("EPERM", "ENOTSUP", "EOPNOTSUPP", "EACCES", "EXDEV")
    if hasattr(errno, name)
)


def copy_and_hash_verify(
    src: str, dst: str, *, src_hash: str | None = None, raise_on_io_error: bool = False,
    on_verified: Callable[[str, int, int], None] | None = None,
) -> tuple[bool, str | None]:
    """Stage, verify and publish a sibling copy without overwriting a destination.

    The temporary name has a fixed length so any valid destination basename
    can be copied. Publishing uses a hard link; on filesystems without links
    it uses an atomic no-replace rename, and where that is missing too (macOS
    SMB shares) a check-then-rename under a shared lock file in the destination
    folder. The lock coordinates Vireo publishers. An interrupted copy can
    leave a hidden temp file. By default all failures return (False, None).
    Callers setting ``raise_on_io_error`` get the filesystem error so
    they can distinguish it from a hash mismatch.
    """
    dst_dir = os.path.dirname(dst)
    tmp = os.path.join(dst_dir, f".vireo-import-{uuid.uuid4().hex}.tmp")
    try:
        if dst_dir:
            os.makedirs(dst_dir, exist_ok=True)
        shutil.copy2(src, tmp)
        snapshot = os.stat(tmp) if on_verified else None
        copied_hash = compute_file_hash(tmp)
        expected = src_hash if src_hash is not None else compute_file_hash(src)
        if copied_hash is None or expected is None or copied_hash != expected:
            log.warning(
                "Hash verification failed for %s -> %s (expected %s, got %s)",
                src, dst, expected, copied_hash,
            )
            return False, None
        try:
            os.link(tmp, dst)
        except OSError as exc:
            if exc.errno not in _HARDLINK_UNSUPPORTED_ERRNOS:
                raise
            _publish_without_links(tmp, dst, dst_dir)
            tmp = None
        if on_verified and snapshot:
            on_verified(copied_hash, snapshot.st_size, snapshot.st_mtime_ns)
        return True, copied_hash
    except OSError as exc:
        if raise_on_io_error:
            raise
        log.warning("Copy failed for %s -> %s: %s", src, dst, exc)
        return False, None
    finally:
        if tmp is not None:
            with contextlib.suppress(OSError):
                os.unlink(tmp)


def _publish_without_links(tmp: str, dst: str, dst_dir: str) -> None:
    """Rename the verified ``tmp`` onto a free ``dst``; FileExistsError if taken."""
    try:
        rename_no_replace(tmp, dst)
    except NoReplaceRenameUnsupported:
        log.info("No atomic no-replace rename on %s; using a locked rename", dst_dir or ".")
        _locked_rename_no_replace(tmp, dst, dst_dir)


_PUBLISH_LOCK_BUSY = {errno.EBUSY, errno.EAGAIN, errno.EWOULDBLOCK}
_PUBLISH_LOCK_TIMEOUT = 5.0


def _try_publish_lock(path: str) -> int:
    """Return a descriptor holding the lock, or raise without waiting."""
    flags = os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK
    if sys.platform == "darwin":
        # smbfs rejects flock on directories. O_EXLOCK on a regular file
        # uses an exclusive open on both the client and the SMB server.
        flags |= os.O_EXLOCK
    fd = os.open(path, flags, 0o666)
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise OSError(errno.EINVAL, "The import lock must be a regular file", path)
        if sys.platform != "darwin":
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        os.close(fd)
        raise
    return fd


def _acquire_publish_lock(path: str) -> int:
    deadline = time.monotonic() + _PUBLISH_LOCK_TIMEOUT
    while True:
        try:
            return _try_publish_lock(path)
        except OSError as exc:
            if exc.errno not in _PUBLISH_LOCK_BUSY or time.monotonic() >= deadline:
                raise
            time.sleep(0.05)


def _verify_publish_lock(path: str) -> None:
    """An independent open must be excluded before publication may proceed."""
    try:
        probe_fd = _try_publish_lock(path)
    except OSError as exc:
        if exc.errno in _PUBLISH_LOCK_BUSY:
            return
        raise
    os.close(probe_fd)
    raise OSError(errno.ENOLCK, "The filesystem did not enforce the exclusive lock", path)


def _locked_rename_no_replace(tmp: str, dst: str, dst_dir: str) -> None:
    """Serialize Vireo publishers on filesystems without an exclusive rename."""
    if sys.platform == "win32":
        os.rename(tmp, dst)
        return

    # Keep the hidden lock file so waiters always lock the same inode. A
    # process exit releases its open descriptor without leaving a lock held.
    lock_path = os.path.join(dst_dir, ".vireo-import.lock")
    with contextlib.ExitStack() as resources:
        try:
            lock_fd = _acquire_publish_lock(lock_path)
            resources.callback(os.close, lock_fd)
            _verify_publish_lock(lock_path)
        except OSError as exc:
            detail = exc.strerror or str(exc)
            raise OSError(
                exc.errno, f"Could not lock the destination folder for a safe import: {detail}", lock_path,
            ) from exc
        try:
            os.lstat(dst)
        except FileNotFoundError:
            pass
        else:
            raise FileExistsError(errno.EEXIST, os.strerror(errno.EEXIST), dst)
        os.rename(tmp, dst)


# Seconds a destination-side hash read may go without producing a single
# chunk before it is declared stalled. Same philosophy as the rsync stall
# watchdog in move/transfer.py: bound silence, not total runtime — a slow but moving
# mount read never trips this, a wedged SMB session does.
DEST_HASH_STALL_TIMEOUT = 120.0


class DestReadCancelled(OSError):
    """Stop arrived while a destination-side read was in flight.

    Subclasses OSError so any call site without explicit cancel handling
    treats it as an ordinary unreadable-file result (safe by default),
    while the sites that would otherwise record a failure catch it first
    and convert it into the job's normal cancelled exit.
    """


def _hash_dest_file(path: str, cancel_check: StopCheck, *,
                    stall_timeout: float = DEST_HASH_STALL_TIMEOUT) -> str:
    """SHA-256 a destination/mount-side file without letting a sick
    network mount hold cancellation hostage.

    ``compute_file_hash`` is a plain blocking read: against a stale SMB
    mount a single file can pin the worker for the mount's own timeout
    (tens of minutes on macOS) per read while Stop goes unobserved —
    cancellation is only polled between files, so the job sits in
    "cancelling" for hours. Run the chunked read in a watcher-supervised
    daemon thread instead:

    - ``cancel_check()`` returns True → ``DestReadCancelled`` within a
      second, even mid-read.
    - no chunk lands for ``stall_timeout`` → plain ``OSError``, the same
      shape an unreadable file already produces at every call site.

    The abandoned worker cannot be killed — the read is stuck in an
    uninterruptible kernel call — so it is orphaned: a daemon thread that
    exits when the mount finally returns, never blocking shutdown.
    """
    if cancel_check():
        # The open() itself blocks on a dead mount; don't even touch it.
        raise DestReadCancelled(f"import cancelled before reading {path}")

    result = {}
    activity = {"t": time.monotonic()}
    done = threading.Event()

    def _worker() -> None:
        h = hashlib.sha256()
        try:
            with open(path, "rb") as f:
                while True:
                    chunk = f.read(65536)
                    activity["t"] = time.monotonic()
                    if not chunk:
                        break
                    h.update(chunk)
            result["hash"] = h.hexdigest()
        except BaseException as exc:  # broad: handed to the caller's thread, which re-raises it
            result["exc"] = exc
        finally:
            done.set()

    worker = threading.Thread(
        target=_worker, daemon=True, name="import-dest-hash",
    )
    worker.start()
    while not done.wait(0.5):
        if cancel_check():
            raise DestReadCancelled(
                f"import cancelled while reading {path}")
        if time.monotonic() - activity["t"] > stall_timeout:
            raise OSError(
                f"read of {path} stalled (no data for "
                f"{stall_timeout:.0f}s; the mount is likely dead)")
    if "exc" in result:
        raise result["exc"]
    return result["hash"]


def _rehash_dest_or_none(path: str, stop_requested: StopCheck) -> str | None:
    """Re-hash a landed destination file, returning None on read failure.

    The stamping loop's last-line check that the bytes currently at the
    destination path still match the hash this import recorded —
    necessary any time the scan-side hash is missing (paired-JPEG row
    deletion) or NULL (zero-byte convention aside, a NULL means the
    scan-side read failed between landing and scan). ``None``
    unambiguously means unreadable: ``_LandedFile.verified_hash`` is
    never None, so callers comparing against it cannot confuse a read
    failure with a legitimately empty file. Re-raises
    ``DestReadCancelled`` from a stop request.
    """
    try:
        return _hash_dest_file(path, stop_requested)
    except DestReadCancelled:
        raise
    except OSError:
        return None
