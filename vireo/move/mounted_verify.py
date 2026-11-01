"""Verify mounted NAS copies without reading their contents back over SMB.

Settings only suggest an SSH route; a fresh nonce in the actual destination
must be readable through that route before its checksums can be trusted.
The mounted view still receives the usual structural checks. Unavailable
SSH or an unproven mapping falls back to the full local byte comparison.
"""

import contextlib
import logging
import os
import posixpath
import secrets
import shlex
import subprocess
import tempfile

from job_runner.subprocess_control import run_controlled_process

from .remote import (
    _ssh_command,
    _ssh_target,
    resolve_rsync_bin,
    resolve_ssh_bin,
    rsync_dest_spec,
    ssh_base_args,
)
from .transfer import FINDER_METADATA_FILES
from .verify import _first_missing_source_file, _remote_verify_complete

try:
    from ..proc import no_window_kwargs
except ImportError:
    from proc import no_window_kwargs

log = logging.getLogger("move")
VERIFY_BATCH_SIZE = 128


def mounted_verification_target(destination, settings):
    """Find one unambiguous saved SSH route for this exact mounted folder.

    This never changes the transfer destination or catalog path. The nonce
    check below proves the mapping afresh, even if Settings changed since
    the import was queued. Relative and symlink-redirected mappings are not
    eligible for the optimization.
    """
    candidates = []
    for entry in settings.get("remote_targets") or []:
        # Settings are normally validated on write. Still refuse malformed
        # routes here: this optional optimization must not break local moves.
        target = _verification_route(entry)
        if not target:
            continue
        mount = target["mount_path"]
        if not os.path.isabs(mount):
            continue
        mount = os.path.abspath(mount)
        dest = os.path.abspath(destination)
        try:
            if os.path.commonpath([mount, dest]) != mount:
                continue
            relative = os.path.relpath(dest, mount)
            expected = os.path.normpath(os.path.join(os.path.realpath(mount), relative))
            if os.path.realpath(dest) != expected:
                continue
        except ValueError:
            continue
        remote_path = posixpath.join(target["remote_path"], *relative.split(os.sep))
        candidates.append({**target, "verify_path": posixpath.normpath(remote_path)})
    if len(candidates) != 1:
        return None
    target = candidates[0]
    target["rsync_bin"] = resolve_rsync_bin(settings.get("rsync_bin", "") or "")
    target["ssh_bin"] = resolve_ssh_bin(settings.get("ssh_bin", "") or "")
    if not target["rsync_bin"] or not target["ssh_bin"]:
        return None
    return target


def _verification_route(entry):
    """Read just the connection fields needed by this optional verifier."""
    if not isinstance(entry, dict):
        return None
    fields = ("host", "user", "remote_path", "mount_path")
    if any(not isinstance(entry.get(k), str) or not entry[k].strip() for k in fields):
        return None
    target = {k: entry[k].strip() for k in fields}
    if not target["remote_path"].startswith("/"):
        return None
    key = entry.get("ssh_key") or ""
    if not isinstance(key, str):
        return None
    target.update(port=entry.get("port") or 22, ssh_key=key)
    return target


def _probe_matches(remote, name, expected, control):
    """Read a tiny identity marker; no photo bytes cross this connection."""
    path = posixpath.join(remote["verify_path"], name)
    cmd = ([_ssh_command(remote)] + ssh_base_args(remote)
           + [_ssh_target(remote), "cat -- " + shlex.quote(path)])
    if control:
        result = run_controlled_process(cmd, control, timeout=20)
    else:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=20,
                                **no_window_kwargs())
    return result.returncode == 0 and result.stdout == expected


def _source_files(source, is_merge):
    """Snapshot source names and identities; do not silently skip walk errors."""
    def walk_error(error):
        raise error

    files = {}
    for root, _, names in os.walk(source, onerror=walk_error):
        for name in names:
            path = os.path.join(root, name)
            if (is_merge and name in FINDER_METADATA_FILES
                    and not os.path.islink(path) and os.path.isfile(path)):
                continue
            stat = os.lstat(path)
            relative = os.path.relpath(path, source)
            files[relative] = (stat.st_dev, stat.st_ino, stat.st_size,
                               stat.st_mtime_ns, stat.st_ctime_ns, stat.st_mode)
    return files


def _verify_batches(source, remote, files, progress, control):
    """Checksum bounded batches so progress advances during a large verify."""
    names = list(files)
    total = len(names)
    progress(0, total, "", "Verifying copy on NAS")
    for offset in range(0, total, VERIFY_BATCH_SIZE):
        batch = names[offset:offset + VERIFY_BATCH_SIZE]
        # rsync owns the order within the batch; don't claim its first
        # manifest entry is the file currently being read on the NAS.
        progress(offset, total, "", "Verifying copy on NAS")
        # NUL separation preserves whitespace. The ./ prefix also prevents
        # rsync treating root names starting with # or ; as list comments,
        # even with --from0, and silently skipping their checksums.
        # Close before rsync opens it (also required on Windows).
        fd, manifest = tempfile.mkstemp(prefix="vireo-verify-", suffix=".files")
        try:
            with os.fdopen(fd, "wb") as stream:
                for name in batch:
                    stream.write(b"./" + os.fsencode(name.replace(os.sep, "/")) + b"\0")
            mismatch = _remote_verify_complete(
                remote["rsync_bin"], source, rsync_dest_spec(remote, remote["verify_path"]),
                remote, files_from=manifest, control=control,
            )
        finally:
            os.unlink(manifest)
        if mismatch is not None:
            return mismatch
        progress(offset + len(batch), total, "", "Verifying copy on NAS")
    return None


def verify_mounted_copy(source, destination, remote, *, is_merge, progress, control=None):
    """Return (handled, mismatch); False requests full byte-comparison fallback.

    A checksum mismatch is a handled failure, never a reason to skip safety
    checks. SSH/tool/probe failures are inconclusive and use the original
    verifier. Cancellation propagates, and the move coordinator restarts
    verification if the control generation changed during a pause.
    """
    marker = None
    try:
        progress(0, 0, "", "Checking NAS verification")
        fd, marker = tempfile.mkstemp(prefix=".vireo-verify-", dir=destination)
        nonce = secrets.token_hex(32)
        with os.fdopen(fd, "w") as stream:
            stream.write(nonce)
        marker_stat = os.stat(marker)
        if not _probe_matches(remote, os.path.basename(marker), nonce, control):
            log.warning("NAS verification unavailable: SSH path does not prove the mounted destination")
            return False, None
        files = _source_files(source, is_merge)

        def report_structure(checked, name):
            progress(checked, 0, name, "Checking NAS verification")

        mismatch = _first_missing_source_file(source, destination, is_merge=is_merge,
                                               progress=report_structure)
        if mismatch is not None:
            return True, mismatch
        mismatch = _verify_batches(source, remote, files, progress, control)
        if mismatch is not None:
            name, detail = mismatch
            if name == "__ERROR__":
                log.warning("NAS checksum verification unavailable: %s; comparing bytes instead", detail)
                return False, None
            return True, name
        # Re-prove the route and mounted structure after hashing. A remount,
        # replaced file/link, or changed source must not authorize deletion.
        if not _probe_matches(remote, os.path.basename(marker), nonce, control):
            return False, None
        current_stat = os.stat(marker)
        if (current_stat.st_dev, current_stat.st_ino) != (marker_stat.st_dev, marker_stat.st_ino):
            return False, None
        if files != _source_files(source, is_merge):
            return True, "source files changed during checksum verification"
        mismatch = _first_missing_source_file(
            source, destination, is_merge=is_merge, progress=report_structure)
        if mismatch is None:
            progress(len(files), len(files), "", "Verifying copy on NAS")
        return True, mismatch
    except (OSError, subprocess.SubprocessError) as exc:
        log.warning("NAS checksum verification unavailable: %s; comparing bytes instead", exc)
        return False, None
    finally:
        if marker is not None:
            with contextlib.suppress(OSError):
                os.unlink(marker)
