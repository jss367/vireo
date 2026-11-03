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
import threading
import time
import uuid
from typing import TYPE_CHECKING

from import_dedup import compute_file_hash

if TYPE_CHECKING:
    from import_job.import_types import StopCheck

log = logging.getLogger(__name__)


# POSIX advisory lock used by the hardlinkless-FS promote fallback (see
# copy_and_hash_verify below). Unavailable on Windows; Vireo targets
# macOS/Linux so this import normally succeeds. If it fails, the
# fallback promote path degrades gracefully to the previous
# check-then-rename behavior (documented in that block).
try:
    import fcntl
except ImportError:  # pragma: no cover - Windows fallback
    fcntl = None


def copy_and_hash_verify(src: str, dst: str, *, src_hash: str | None = None) -> tuple[bool, str | None]:
    """Copy ``src`` to ``dst`` and verify the landed bytes by content hash.

    The copy goes to a hidden sibling temp path first; only a copy whose
    hash matches the source is promoted into ``dst``. Promotion prefers a
    no-overwrite ``os.link`` (atomic on POSIX same-FS) over ``os.replace``
    — imports have no pipeline-slot lock, so two concurrent jobs targeting
    the same destination/date folder with the same filename can both pass
    their pre-copy collision check before either promotes; ``os.replace``
    would silently overwrite the first job's already-verified archive
    copy, and its ``safe_to_format`` would still report green after the
    bytes it verified are gone. A raced promote is surfaced as a copy
    failure instead.

    When the destination filesystem does not support hard links (exFAT/FAT,
    some SMB/NFS mounts — os.link raises OSError with EPERM/ENOTSUP/
    EOPNOTSUPP), fall back to a check-then-rename promotion serialized on
    a directory-level ``fcntl.flock`` of the destination folder. That
    preserves both crash-safety (no zero-byte placeholder file) and
    no-overwrite race protection against concurrent imports targeting the
    same destination/date folder — the fallback block below documents the
    tradeoffs. Imports do not fail across every file on FAT-family
    archives or hardlinkless NAS shares.

    On mismatch (or race) the temp copy is removed and any pre-existing
    ``dst`` is left untouched.

    Args:
        src: source file path (e.g. on the card)
        dst: final destination path in the archive
        src_hash: optional already-computed source hash (e.g. the
            DuplicateChecker's cached value) to avoid re-reading the
            source.

    Returns:
        (True, file_hash) on verified success, (False, None) on failure.
    """
    dst_dir = os.path.dirname(dst)
    if dst_dir:
        os.makedirs(dst_dir, exist_ok=True)
    tmp = os.path.join(
        dst_dir, f".{os.path.basename(dst)}.{uuid.uuid4().hex}.tmp"
    )
    try:
        shutil.copy2(src, tmp)
        copied_hash = compute_file_hash(tmp)
        expected = src_hash if src_hash is not None else compute_file_hash(src)
        if copied_hash is None or expected is None or copied_hash != expected:
            log.warning(
                "Hash verification failed for %s -> %s (expected %s, got %s)",
                src, dst, expected, copied_hash,
            )
            return (False, None)
        # Atomic no-overwrite promote: os.link raises FileExistsError if
        # ``dst`` was created between the caller's collision check and
        # this instant. tmp lives in the same directory as dst, so link
        # stays same-filesystem and portable across the NAS mounts that
        # are the real archive target.
        try:
            os.link(tmp, dst)
        except FileExistsError:
            log.warning(
                "Destination raced during copy (concurrent import?): %s",
                dst,
            )
            return (False, None)
        except OSError as link_err:
            # Hard links unsupported on this destination filesystem
            # (FAT/exFAT return EPERM; some SMB/NFS mounts return
            # ENOTSUP/EOPNOTSUPP; Windows exFAT can return EACCES).
            # Without a fallback promotion path every file on
            # hardlinkless archives buckets as a copy failure and
            # imports are unusable on those destinations. See
            # ``_rename_promote_locked`` for the fallback.
            if not _fs_lacks_hardlinks(link_err):
                raise
            log.info(
                "os.link unsupported on %s (%s); using rename fallback",
                dst_dir, link_err,
            )
            if not _rename_promote_locked(src, tmp, dst, dst_dir):
                return (False, None)
            tmp = None
            return (True, copied_hash)
        os.unlink(tmp)
        tmp = None
        return (True, copied_hash)
    except OSError as e:
        log.warning("Copy failed for %s -> %s: %s", src, dst, e)
        return (False, None)
    finally:
        if tmp is not None:
            with contextlib.suppress(OSError):
                os.unlink(tmp)


def _rename_promote_locked(src: str, tmp: str, dst: str, dst_dir: str) -> bool:
    """Promote the verified ``tmp`` to ``dst`` without a hard link.

    Returns True once ``tmp`` has been renamed onto ``dst`` (the temp no
    longer exists), False when ``dst`` appeared first or the rename
    failed (the caller's cleanup removes ``tmp``).
    """
    # Fall back to existence-check + os.rename, wrapped in a
    # directory-level POSIX advisory lock. The verified temp
    # stays hidden until it moves atomically over to ``dst``.
    # Do NOT reserve the final path as an O_EXCL placeholder
    # before renaming — a crash between placeholder creation
    # and os.replace would leave a zero-byte stray at the
    # intended archive name, and a retry treats that
    # placeholder as "existing archive file", suffixes the
    # real photo to ``name_1.ext``, and orphans the empty
    # file. That violates the crash-recovery invariant that a
    # dead run leaves only valid archive copies or hidden
    # temps.
    #
    # A bare check-then-rename loses a concurrent-import race:
    # two hardlinkless-FS jobs targeting the same
    # destination/date folder could both pass exists() before
    # either rename(), and the later rename would silently
    # overwrite the first job's already-verified archive copy
    # (its ``safe_to_format`` would still report green after
    # its bytes are gone). Serialize the critical section on
    # an exclusive ``fcntl.flock`` of the destination
    # directory: FD-scoped, so a crash releases it
    # automatically — no placeholder cleanup burden, and the
    # zero-byte crash-safety invariant is preserved. On mounts
    # where flock silently no-ops (some remote FSes mounted
    # ``nolock``) this degrades to a bare check-then-rename;
    # per-workspace/per-date destinations make overlapping runs
    # unusual there.
    lock_fd = None
    try:
        try:
            lock_fd = os.open(dst_dir, os.O_RDONLY)
        except OSError:
            lock_fd = None
        if lock_fd is not None and fcntl is not None:
            with contextlib.suppress(OSError):
                fcntl.flock(lock_fd, fcntl.LOCK_EX)
        if os.path.exists(dst):
            log.warning(
                "Destination raced during copy "
                "(concurrent import?): %s",
                dst,
            )
            return False
        try:
            os.rename(tmp, dst)
        except OSError as rep_err:
            log.warning(
                "Fallback promote failed for %s -> %s: %s",
                src, dst, rep_err,
            )
            return False
        return True
    finally:
        if lock_fd is not None:
            with contextlib.suppress(OSError):
                os.close(lock_fd)


# errno values that mean "this filesystem doesn't support hard links",
# not "the operation was denied for some other reason". Kept narrow so a
# genuine permission error on a link-supporting FS still surfaces as a
# copy failure instead of silently falling back to the placeholder path.
# EPERM is the canonical Linux answer for FAT/exFAT; ENOTSUP/EOPNOTSUPP
# come from various BSD-family kernels and userspace filesystems; EACCES
# has been observed on Windows exFAT via WSL. EXDEV (cross-device link)
# also lands here — same-directory tmp should never trip it, but treating
# it as "hard link not usable here" and using the O_EXCL fallback is
# strictly safer than propagating.
_HARDLINK_UNSUPPORTED_ERRNOS = frozenset(
    e for e in (
        getattr(errno, "EPERM", None),
        getattr(errno, "ENOTSUP", None),
        getattr(errno, "EOPNOTSUPP", None),
        getattr(errno, "EACCES", None),
        getattr(errno, "EXDEV", None),
    ) if e is not None
)


def _fs_lacks_hardlinks(err: OSError) -> bool:
    """True when ``err`` from os.link indicates a hardlinkless target FS."""
    return getattr(err, "errno", None) in _HARDLINK_UNSUPPORTED_ERRNOS


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
