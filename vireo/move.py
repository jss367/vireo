"""Photo and folder move operations with copy-verify-delete safety."""

import contextlib
import filecmp
import logging
import os
import posixpath
import shlex
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime

try:
    from .db import _chunks, _join_subtree_path, _subtree_prefix, _subtree_relative
    from .proc import no_window_kwargs
    from .staged_copy import copy_via_temp
except ImportError:
    from db import _chunks, _join_subtree_path, _subtree_prefix, _subtree_relative
    from proc import no_window_kwargs
    from staged_copy import copy_via_temp

log = logging.getLogger(__name__)

# How long the remote verification rsync (a --checksum dry-run over SSH) may
# run before we give up and treat the move as unverified. This only gates the
# *verify* step, never the transfer: a timeout here is conservative-safe — it
# returns a verification failure, so the originals are preserved rather than
# deleted against an unconfirmed copy.
REMOTE_VERIFY_TIMEOUT = 7200  # 2 hours

# How long rsync may make NO forward progress (no file transferred and no
# stderr activity) before we treat it as wedged and kill it. This is a STALL
# watchdog, not a total-runtime cap: a healthy copy of thousands of RAW files
# over a slow network share can legitimately run for many hours, and it is
# allowed to as long as it keeps moving data. The window is generous because
# rsync is silent during its initial file-list build and the destination scan
# a merge (--ignore-existing) performs, which over a slow SMB share can take
# several minutes before the first file transfers.
#
# Known limitation: progress is detected per FILE (rsync emits one
# --out-format=%n line per item, not continuous byte progress), so a single
# file whose transfer exceeds this window over a very slow link could be
# killed mid-flight. That can't happen for Vireo's data — source files are RAW
# frames (tens of MB; ~12s/file even on the slow SMB share this was built for),
# orders of magnitude under the window — so it's accepted rather than paying
# the complexity of char-level --info=progress2 byte-progress parsing.
RSYNC_STALL_TIMEOUT = 1800  # 30 minutes


def _xmp_path(filepath):
    """Return the XMP sidecar path for a file, or None if it doesn't exist.

    Matches ``.XMP`` too (as ``offline_cache._xmp_source_for`` does): on a
    case-sensitive volume an uppercase sidecar would otherwise be left
    behind at the source when its photo moves.
    """
    stem = os.path.splitext(filepath)[0]
    for xmp in (stem + ".xmp", stem + ".XMP"):
        if os.path.isfile(xmp):
            return xmp
    return None


def _companion_files(photo, src_dir):
    """Return list of extra files to move alongside a photo (XMP + companion RAW/JPEG)."""
    extras = []
    xmp = _xmp_path(os.path.join(src_dir, photo["filename"]))
    if xmp:
        extras.append(os.path.basename(xmp))
    if photo["companion_path"]:
        comp = os.path.join(src_dir, photo["companion_path"])
        if os.path.isfile(comp):
            extras.append(photo["companion_path"])
    return extras


def _copy_and_verify(src, dst):
    """Copy a single file and verify size matches. Returns True on success.

    The copy goes through a hidden sibling temp file (``copy_via_temp``),
    so a failed copy never leaves a truncated file at ``dst`` that would
    block every retry as "already exists" and be cataloged by a rescan.
    ``OSError`` from the copy propagates for the caller to record against
    this one photo.
    """
    copy_via_temp(src, dst)
    if os.path.getsize(src) != os.path.getsize(dst):
        os.remove(dst)
        return False
    return True


def sanitize_subpath(subpath):
    """Normalize an optional relative subpath under a remote target's base.

    Rejects absolute paths and any ``..`` traversal so a move can't escape the
    configured remote_path/mount_path. Returns a clean ``/``-joined relative
    path (possibly empty).
    """
    raw = (subpath or "").strip()
    if not raw:
        return ""
    sub = raw.replace("\\", "/")
    # Reject absolute inputs BEFORE the segment loop strips leading separators.
    # Without this, '/foo' and '\foo' silently land as 'foo' and slip into a
    # different target than the user typed; Windows drive prefixes ('C:...')
    # would also slip through to a non-relative target.
    if sub.startswith("/") or (len(sub) >= 2 and sub[1] == ":"):
        raise ValueError("Subpath must be relative")
    parts = []
    for seg in sub.split("/"):
        if seg in ("", "."):
            continue
        if seg == "..":
            raise ValueError("Subpath may not contain '..'")
        parts.append(seg)
    return "/".join(parts)


def build_remote_move_spec(target, subpath, rsync_bin, ssh_bin=""):
    """Build the ``remote`` dict ``move_folder`` expects from a config target.

    ``target`` is a validated dict from ``config.get_remote_target`` (host,
    user, port, ssh_key, remote_path, mount_path, bwlimit_kbps). ``subpath``
    is an optional relative path under both base paths. The SSH base is joined
    POSIX-style (the NAS is POSIX); the mount base uses the local separator.
    Raises ValueError on a bad subpath.
    """
    sub = sanitize_subpath(subpath)
    ssh_base = target["remote_path"]
    mount_base = target.get("mount_path", "")
    if sub:
        ssh_base = posixpath.join(ssh_base, sub)
        if mount_base:
            mount_base = os.path.join(mount_base, *sub.split("/"))
    return {
        "host": target["host"],
        "user": target["user"],
        "port": target.get("port", 22),
        "ssh_key": target.get("ssh_key", ""),
        "bwlimit_kbps": target.get("bwlimit_kbps", 0),
        "rsync_bin": rsync_bin,
        "ssh_bin": resolve_ssh_bin(ssh_bin or target.get("ssh_bin", "")),
        "ssh_dest_base": ssh_base,
        "mount_dest_base": mount_base,
    }


def normalize_destination_name(destination_name):
    """Return a safe, single-component folder name for a move destination.

    Folder moves accept a destination *parent* separately from the name of the
    folder that lands inside it.  Keeping the leaf name separate makes rename-
    while-moving explicit and prevents an entered name from escaping the
    selected parent.  Both slash styles are rejected because moves can target
    a POSIX NAS from a Windows client (and vice versa).  Colons are rejected
    too: on Windows ``os.path.join(r"D:\\archive", "C:shoot")`` returns the
    drive-relative ``"C:shoot"``, so accepting a drive-qualified leaf would
    let the entered name escape the selected parent and land the copy — and
    the repointed ``catalog_path`` — outside the chosen destination.

    An empty value means "keep the source folder name" and is returned as an
    empty string for backwards-compatible callers.
    """
    if destination_name is None:
        return ""
    if not isinstance(destination_name, str):
        raise ValueError("Folder name must be a string")
    name = destination_name.strip()
    if not name:
        return ""
    if (
        name in (".", "..")
        or "/" in name
        or "\\" in name
        or ":" in name
        or "\0" in name
    ):
        raise ValueError(
            "Folder name must be a single name without slashes or colons"
        )
    return name


def resolve_folder_dest(folder_path, folder_name, destination,
                        destination_name=""):
    """Compute the final landing path for a folder move.

    The source folder is placed *inside* destination. By default it keeps its
    name (moving /local/birds to /nas/photos yields /nas/photos/birds), while
    ``destination_name`` allows an explicit rename during the move.
    Shared by move_folder() and the preflight route so the resolved path
    is computed in exactly one place.
    """
    name = normalize_destination_name(destination_name) or folder_name \
        or os.path.basename(folder_path.rstrip("/\\"))
    return os.path.join(destination, name)


def _copy_tree_with_progress(src_path, dest_path, skip_existing, total_files,
                             progress_cb):
    """Recursively copy src_path into dest_path, reporting each file copied.

    ``skip_existing`` mirrors rsync ``--ignore-existing`` (merge/resume): a
    destination file that already exists is never overwritten. Used only as
    the shutil fallback when rsync is unavailable, so a fresh move (creating
    dest_path) and a merge share one progress-emitting walk.
    """
    # os.walk swallows scandir errors by default, so an unreadable source
    # subdirectory would be silently skipped here — and the fresh-move count
    # verification (also a default os.walk) would skip it too, so the counts
    # match and the catalog update + rmtree(src) proceed on an incomplete
    # copy. shutil.copytree (the old fallback) raised instead; re-raise so the
    # caller aborts the move before anything is deleted.
    def _raise(err):
        raise err

    copied = 0
    created_dirs = []
    for root, dirs, files in os.walk(src_path, onerror=_raise):
        rel = os.path.relpath(root, src_path)
        target_dir = dest_path if rel == "." else os.path.join(dest_path, rel)
        os.makedirs(target_dir, exist_ok=True)
        created_dirs.append((root, target_dir))
        # os.walk lists a symlinked subdirectory in `dirs` but, with its
        # default followlinks=False, never recurses into it — so its contents
        # would be silently dropped here while the post-copy file-count
        # verification (also a default os.walk) skips it on both sides and
        # still matches, letting rmtree(src) delete the originals. Recreate
        # each directory symlink as a symlink at the destination, matching the
        # primary rsync -a path (which preserves symlinks rather than
        # following them) and keeping the verification counts consistent.
        for d in dirs:
            src_sub = os.path.join(root, d)
            if not os.path.islink(src_sub):
                continue
            dst_sub = os.path.join(target_dir, d)
            if skip_existing and os.path.lexists(dst_sub):
                continue
            os.symlink(os.readlink(src_sub), dst_sub)
        for fn in files:
            # Merges skip Finder metadata (``.DS_Store``) — the destination
            # keeps its own copy, and the source's is discarded when the
            # source tree is removed after a successful merge. Fresh moves
            # carry them along, matching the primary rsync path.
            if skip_existing and fn in FINDER_METADATA_FILES:
                continue
            src_file = os.path.join(root, fn)
            dst_file = os.path.join(target_dir, fn)
            # lexists (not exists): a broken or symlinked destination entry
            # still counts as present for a merge, so we never dereference it
            # and write through to its target. exists() returns False for a
            # broken symlink and would fall through to copy2 below.
            if skip_existing and os.path.lexists(dst_file):
                continue
            if os.path.islink(src_file):
                # Preserve the symlink rather than copy2's dereferenced target,
                # matching rsync -a and the directory-symlink handling above.
                os.symlink(os.readlink(src_file), dst_file)
            else:
                shutil.copy2(src_file, dst_file)
            copied += 1
            if progress_cb:
                progress_cb(copied, total_files, fn, "Copying files")

    # Mirror directory metadata (mode, mtime) for a fresh move, matching the
    # primary rsync -a path and the shutil.copytree this fallback replaced.
    # os.makedirs creates dirs with default permissions, so without this a
    # private 0700 source folder would land as 0755 and the original metadata
    # is lost once the source is deleted. copy2 above already preserves file
    # metadata. Run after all contents exist so child writes don't re-bump a
    # parent's mtime. Skipped on a merge: a pre-existing destination dir keeps
    # the user's own metadata rather than being overwritten with the source's.
    if not skip_existing:
        for src_dir, target_dir in created_dirs:
            shutil.copystat(src_dir, target_dir)


# Path candidates for a GNU rsync usable for remote (SSH) moves. macOS's
# /usr/bin/rsync is Apple's openrsync, which can't drive rsync-over-SSH to a
# GNU rsync peer; it is intentionally absent here AND the resolver verifies
# any PATH/`/usr/bin/rsync` candidate with is_gnu_rsync before returning it,
# so a host with only openrsync still returns None. A packaged build drops a
# bundled static GNU rsync next to this module (vireo/bin/rsync) or in the
# app's Resources dir; dev machines fall back to a Homebrew/MacPorts install
# or the distro's `/usr/bin/rsync` (GNU rsync on every Linux distro).
_BUNDLED_RSYNC_CANDIDATES = (
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "bin", "rsync"),
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "Resources", "rsync"),
    "/opt/homebrew/bin/rsync",
    "/usr/local/bin/rsync",
    "/opt/local/bin/rsync",
)

# Candidates that may resolve to Apple's openrsync on macOS (PATH lookup and
# the system /usr/bin/rsync). These are checked LAST so an explicit config or
# bundled GNU rsync always wins, and each is probed with is_gnu_rsync before
# being returned — so a macOS host with only openrsync still gets None, while
# every Linux distro (where /usr/bin/rsync IS GNU rsync) gets remote moves.
_FALLBACK_RSYNC_CANDIDATES = ("/usr/bin/rsync",)


def _is_executable_file(path):
    """True if ``path`` is a regular file the OS would treat as executable.

    On POSIX this is the X bit. Windows has no execute bit (``os.access(p,
    os.X_OK)`` returns True for any regular file), so executability there is
    defined by file extension via PATHEXT — match that.
    """
    if not path or not os.path.isfile(path):
        return False
    if os.name == "nt":
        ext = os.path.splitext(path)[1].lower()
        pathext = [e.lower() for e in os.environ.get(
            "PATHEXT", ".COM;.EXE;.BAT;.CMD").split(os.pathsep)]
        return ext in pathext
    return os.access(path, os.X_OK)


def _platform_rsync_candidates():
    """Standard GNU rsync locations on non-macOS platforms.

    The openrsync-at-/usr/bin avoidance baked into _BUNDLED_RSYNC_CANDIDATES is
    macOS-specific: every other major OS ships GNU rsync as ``/usr/bin/rsync``
    and on ``$PATH``, so on Linux/BSD/Windows a usable rsync is normally just
    there and ``resolve_rsync_bin("")`` should find it without the user setting
    anything. We probe ``$PATH`` via shutil.which first so a custom rsync
    earlier on PATH (e.g. /usr/local/bin) is preferred over /usr/bin's, and
    fall back to /usr/bin/rsync explicitly in case PATH is unset/sparse in a
    headless context. Returns an empty tuple on darwin so this never
    short-circuits the Homebrew/MacPorts candidates above.
    """
    if sys.platform == "darwin":
        return ()
    cands = []
    found = shutil.which("rsync")
    if found:
        cands.append(found)
    if os.name == "nt":
        for env_var in ("PROGRAMFILES", "PROGRAMFILES(X86)", "PROGRAMW6432"):
            base = os.environ.get(env_var)
            if base:
                cands.append(os.path.join(base, "cwRsync", "bin", "rsync.exe"))
        system_drive = os.environ.get("SYSTEMDRIVE", "C:")
        cands.extend([
            os.path.join(system_drive + os.sep, "msys64", "usr", "bin", "rsync.exe"),
            os.path.join(system_drive + os.sep, "cygwin64", "bin", "rsync.exe"),
        ])
    cands.append("/usr/bin/rsync")
    return tuple(cands)


def rsync_install_guidance() -> dict:
    """Installation help for the server's platform, shared by setup UIs."""
    commands = []
    if sys.platform == "darwin":
        commands = ["brew install rsync"]
        hint = (
            "Install GNU rsync with Homebrew: brew install rsync. "
            "The rsync bundled with macOS is not supported for Vireo's SSH transfers. "
            "Vireo detects the Homebrew installation automatically; retry afterward."
        )
    elif sys.platform.startswith("linux"):
        commands = ["sudo apt install rsync", "sudo dnf install rsync"]
        hint = (
            "Install GNU rsync with your distribution's package manager: "
            "sudo apt install rsync (Debian/Ubuntu) or sudo dnf install rsync (Fedora). "
            "Then retry."
        )
    else:
        hint = "Install GNU rsync and configure its executable under Settings → Paths."
    if commands:
        hint += " For a custom installation, set the GNU rsync path under Settings → Paths."
    return {"hint": hint, "commands": commands}


def resolve_rsync_bin(configured=""):
    """Return an absolute path to a GNU rsync binary for remote moves, or None.

    Resolution order: an explicit ``configured`` path (the ``rsync_bin``
    config value), the ``VIREO_RSYNC_BIN`` environment override, the
    bundled/known-install candidates, then on non-macOS platforms ``$PATH``
    and ``/usr/bin/rsync`` (where GNU rsync normally lives). Apple's openrsync
    at /usr/bin/rsync is never auto-selected on macOS — it can't do
    rsync-over-SSH — so a macOS host with only that returns None, and the
    caller surfaces a clear "install GNU rsync" error rather than failing
    mid-transfer.
    """
    candidates = []
    if configured:
        candidates.append(configured)
    env = os.environ.get("VIREO_RSYNC_BIN")
    if env:
        candidates.append(env)
    candidates.extend(_BUNDLED_RSYNC_CANDIDATES)
    candidates.extend(_platform_rsync_candidates())
    seen = set()
    for c in candidates:
        if not c:
            continue
        ap = os.path.abspath(c)
        if ap in seen:
            continue
        seen.add(ap)
        if _is_executable_file(ap):
            return ap
    return None


def resolve_ssh_bin(configured=""):
    """Resolve OpenSSH without requiring it to be on a GUI process's PATH."""
    candidates = [configured, os.environ.get("VIREO_SSH_BIN"), shutil.which("ssh")]
    if os.name == "nt":
        system_root = os.environ.get("SYSTEMROOT", r"C:\Windows")
        candidates.append(os.path.join(system_root, "System32", "OpenSSH", "ssh.exe"))
    for candidate in candidates:
        if candidate and _is_executable_file(os.path.abspath(candidate)):
            return os.path.abspath(candidate)
    return None


def _ssh_command(remote):
    return remote.get("ssh_bin") or resolve_ssh_bin() or "ssh"


def is_gnu_rsync(rsync_bin):
    """True if ``rsync_bin`` looks like GNU rsync (not Apple openrsync).

    Used by the connection test to give a precise error before a move: Apple's
    openrsync reports ``openrsync:`` in --version and can't drive SSH. Failures
    to execute return False (treated as unusable).
    """
    try:
        out = subprocess.run([rsync_bin, "--version"], capture_output=True,
                             text=True, timeout=10,
                             **no_window_kwargs()).stdout.lower()
    except (OSError, subprocess.SubprocessError):
        return False
    return "openrsync" not in out and "rsync" in out


def ssh_base_args(remote):
    """Base ``ssh`` option args (list) for connecting to a remote target.

    Non-interactive (BatchMode) so a headless job thread can never hang on a
    password prompt; ``accept-new`` trust-on-first-use for the host key so the
    first move from a freshly configured target doesn't fail host-key
    verification. Port and identity file are added only when set.
    """
    args = ["-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=accept-new",
            "-o", "ConnectTimeout=10"]
    port = remote.get("port") or 22
    with contextlib.suppress(TypeError, ValueError):
        if int(port) != 22:
            args += ["-p", str(int(port))]
    key = remote.get("ssh_key")
    if key:
        args += ["-i", key]
    return args


def _ssh_rsh_string(remote):
    """The ``-e`` value for rsync: a shell-quoted ``ssh ...`` command string.

    rsync's ``-e`` argument is parsed with popt-style tokenisation that
    respects shell-style quoting — quoting an identity-file path with spaces
    keeps it as a single argument when rsync re-splits the string. Use
    ``shlex.join`` so a key path like ``/Users/me/My Keys/id_ed25519`` (or a
    port flag carrying any whitespace) survives the round-trip intact.
    """
    return shlex.join([_ssh_command(remote)] + ssh_base_args(remote))


def _ssh_target(remote):
    return f'{remote["user"]}@{remote["host"]}'


def _rsync_host_token(host):
    """Bracket a host for rsync's ``user@host:path`` syntax when needed.

    The rsync remote-shell form uses the FIRST colon as the host/path
    separator (see the rsync(1) manpage). An IPv6 literal like
    ``2001:db8::1`` therefore can't be passed bare: ``me@2001:db8::1:/path``
    ships to ssh as host ``2001`` and remote command ``db8::1:/path``, so
    rsync's preflight probe of THIS routine's target can succeed (we use
    direct ssh, where user@host parses cleanly without brackets) while the
    actual transfer goes to the wrong host or fails entirely. Wrapping the
    host in brackets disambiguates it and is rsync's documented IPv6 form;
    DNS names and IPv4 addresses don't contain colons so are passed through
    unchanged. Plain ssh invocations stay unbracketed because OpenSSH parses
    ``user@host`` unambiguously without a trailing path component.
    """
    if ":" in host:
        return f"[{host}]"
    return host


def rsync_dest_spec(remote, path):
    """Format ``user@host:path`` for rsync, bracketing the host iff it's IPv6.

    The single place every rsync transfer/verify spec is built so the IPv6
    wrap can't be forgotten at one call site and silently land in the wrong
    place. Display strings (the move preflight's ``resolved_dest``, the
    move-form's SSH preview) use this too, so a value the user sees and a
    value rsync would actually accept are the same string.
    """
    return f'{remote["user"]}@{_rsync_host_token(remote["host"])}:{path}'


def _remote_mkdir_p(remote, path):
    """Create ``path`` (and any missing intermediate parents) on the remote
    host via SSH. Idempotent — ``mkdir -p`` accepts an already-existing
    directory.

    Without this, a remote move into a configured subpath like ``USA/2026``
    that has never been written before fails at rsync with
    ``mkdir ... failed: No such file or directory`` — rsync creates the
    leaf folder but not its intermediate parents. The UI advertises a
    free-text subpath, so users wouldn't otherwise know to pre-create
    every level manually on the NAS.

    Returns ``(True, "")`` on success or ``(False, detail)`` where
    ``detail`` is a short message suitable for surfacing to the user.
    """
    cmd = ([_ssh_command(remote)] + ssh_base_args(remote) + [_ssh_target(remote)]
           + [f"mkdir -p {shlex.quote(path)}"])
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=30,
                           **no_window_kwargs())
    except (OSError, subprocess.SubprocessError) as exc:
        return False, str(exc)
    if r.returncode != 0:
        return False, r.stderr.strip() or f"ssh mkdir exit {r.returncode}"
    return True, ""


def _remote_dir_exists(remote, path):
    """Probe whether ``path`` is a directory on the remote host (via SSH).

    Returns True if it is, False if the SSH command ran and reported it isn't,
    and None if the probe itself couldn't run cleanly — SSH connect failure,
    auth failure, timeout, ``ssh`` binary missing, etc. The caller MUST treat
    None as "inconclusive, refuse the move" rather than as "destination
    absent": if a transient SSH glitch on an actually-existing destination
    were collapsed to False, the move would proceed as a fresh transfer
    (omitting --ignore-existing) and rsync would happily overwrite same-name
    files before the post-transfer --checksum verify could preserve the
    originals.

    ``test -d`` exit codes: 0 = directory, 1 = not a directory / absent. SSH
    returns the remote command's exit code on success, or 255 when SSH itself
    couldn't complete the session — so any other return code (255 or
    otherwise unexpected) is bucketed with the OSError/SubprocessError path.
    """
    cmd = ([_ssh_command(remote)] + ssh_base_args(remote)
           + [_ssh_target(remote), f"test -d {shlex.quote(path)}"])
    try:
        rc = subprocess.run(cmd, capture_output=True, timeout=30,
                            **no_window_kwargs()).returncode
    except (OSError, subprocess.SubprocessError):
        return None
    if rc == 0:
        return True
    if rc == 1:
        return False
    return None


def _remote_free_bytes(remote, path):
    """Free bytes on the remote filesystem holding ``path``, via ``df -Pk``
    over SSH.

    Returns None when the probe couldn't run or parse — callers must treat
    that as "unknown, skip the check", never as 0 or "plenty", so a flaky
    link can't fabricate an out-of-space refusal or wave through a full
    volume. ``df -P`` (POSIX output format) pins the layout: one header
    line, then one line per filesystem with available KiB in column 4 —
    GNU, BusyBox, and Synology's df all honor it. ``path`` must exist on
    the remote (probe the configured base, not a not-yet-created leaf).
    """
    cmd = ([_ssh_command(remote)] + ssh_base_args(remote) + [_ssh_target(remote)]
           + [f"df -Pk {shlex.quote(path)}"])
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=30,
                           **no_window_kwargs())
    except (OSError, subprocess.SubprocessError):
        return None
    if r.returncode != 0:
        return None
    lines = [ln for ln in r.stdout.splitlines() if ln.strip()]
    if len(lines) < 2:
        return None
    try:
        return int(lines[-1].split()[3]) * 1024
    except (IndexError, ValueError):
        return None


def remote_preflight(remote, dest_path, file_cap=1000):
    """Probe a remote destination for the move UI's merge/resume prompt.

    Returns ``(exists, file_count, truncated, reachable, error)``:
      * reachable=False + error — couldn't connect/run over SSH.
      * exists — whether ``dest_path`` is already a directory on the remote.
      * file_count/truncated — capped count of files already there (so the
        UI can say "N files present" before a merge), via
        ``find | head | wc -l`` so a huge tree can't hang the probe.
    """
    target = _ssh_target(remote)
    base = [_ssh_command(remote)] + ssh_base_args(remote) + [target]
    q = shlex.quote(dest_path)
    probe = base + [f"if [ -d {q} ]; then echo EXISTS; else echo NOPE; fi"]
    try:
        r = subprocess.run(probe, capture_output=True, text=True, timeout=30,
                           **no_window_kwargs())
    except (OSError, subprocess.SubprocessError) as exc:
        return (False, 0, False, False, str(exc))
    if r.returncode != 0:
        return (False, 0, False, False,
                r.stderr.strip() or "SSH connection failed")
    if "EXISTS" not in r.stdout:
        return (False, 0, False, True, None)
    cnt_cmd = base + [
        f"find {q} -type f 2>/dev/null | head -n {file_cap + 1} | wc -l"]
    try:
        c = subprocess.run(cnt_cmd, capture_output=True, text=True, timeout=120,
                           **no_window_kwargs())
        n = int((c.stdout or "0").strip() or 0)
    except (OSError, subprocess.SubprocessError, ValueError):
        return (True, 0, False, True, None)
    return (True, min(n, file_cap), n > file_cap, True, None)


def test_remote_connection(remote, rsync_bin):
    """Run the checks the settings UI shows when testing a remote target.

    ``remote`` is a coerced target dict; ``rsync_bin`` is a resolved GNU rsync
    path (or "" if none/openrsync). Returns a dict with per-check booleans
    (``ssh``, ``remote_path_writable``, ``rsync_ok``, ``remote_rsync_ok``),
    an overall ``ok``, and a human ``message``.

    Both rsync ends are probed: rsync-over-SSH needs a working rsync on the
    REMOTE side too (the ``--rsync-path`` program the local rsync invokes
    after SSH connects). On a Synology NAS, that program is gated by DSM's
    "Enable rsync service" toggle: SSH and the remote path can be reachable
    while the remote rsync binary is absent or disabled, so without this
    probe the test reports "Connection OK" and the user only discovers the
    misconfiguration when a real move fails mid-transfer.
    """
    result = {"ok": False, "ssh": False, "remote_path_writable": False,
              "rsync_ok": bool(rsync_bin), "remote_rsync_ok": False,
              "message": ""}
    target = _ssh_target(remote)
    base = [_ssh_command(remote)] + ssh_base_args(remote) + [target]
    try:
        r = subprocess.run(base + ["echo vireo_ok"], capture_output=True,
                           text=True, timeout=20, **no_window_kwargs())
    except (OSError, subprocess.SubprocessError) as exc:
        result["message"] = f"SSH connection failed: {exc}"
        return result
    if r.returncode != 0 or "vireo_ok" not in r.stdout:
        result["message"] = (r.stderr.strip()
                             or "SSH connection failed — check host, user, "
                                "and that your key is authorized.")
        return result
    result["ssh"] = True
    rp = shlex.quote(remote["remote_path"])
    try:
        w = subprocess.run(
            base + [f"test -d {rp} && test -w {rp} && echo WRITABLE"],
            capture_output=True, text=True, timeout=20, **no_window_kwargs())
    except (OSError, subprocess.SubprocessError) as exc:
        result["message"] = f"SSH connection failed: {exc}"
        return result
    if "WRITABLE" not in w.stdout:
        result["message"] = (
            f"Connected, but '{remote['remote_path']}' isn't a writable "
            f"directory for {remote['user']}. Check the path and that the "
            f"Synology rsync service is enabled.")
        return result
    result["remote_path_writable"] = True
    if not rsync_bin:
        result["message"] = (
            "SSH and the remote path are reachable, but no GNU rsync was "
            "found for the transfer. " + rsync_install_guidance()["hint"])
        result["rsync_install_commands"] = rsync_install_guidance()["commands"]
        return result
    # Probe the REMOTE rsync. `rsync --version` is cheap and side-effect-free;
    # any non-zero exit (or a missing binary, which the remote shell reports
    # as "command not found" / exit 127) means an actual move would fail at
    # the rsync handshake. On Synology, the setuid rsync only appears on PATH
    # when DSM's "Enable rsync service" toggle is on — the most common cause
    # of this check failing on an otherwise reachable NAS.
    try:
        rr = subprocess.run(
            base + ["rsync --version 2>/dev/null | head -n 1"],
            capture_output=True, text=True, timeout=20, **no_window_kwargs())
    except (OSError, subprocess.SubprocessError) as exc:
        result["message"] = (
            f"SSH and the remote path are reachable, but couldn't probe the "
            f"remote rsync: {exc}")
        return result
    if rr.returncode != 0 or "rsync" not in rr.stdout.lower():
        result["message"] = (
            "SSH and the remote path are reachable, but rsync isn't "
            "available on the remote — moves would fail at handshake. On a "
            "Synology NAS, enable Control Panel → File Services → rsync → "
            "Enable rsync service. Otherwise, install rsync on the remote.")
        return result
    result["remote_rsync_ok"] = True
    result["ok"] = True
    result["message"] = "Connection OK — SSH, remote path, and rsync all good."
    return result


def _remote_verify_complete(rsync_bin, src_path, rsync_target, remote,
                            *, is_merge=False):
    """Independent verification that the remote copy is complete and correct.

    The local move's safety check walks the destination filesystem before
    deleting originals; a remote destination isn't locally walkable, so this
    runs ``rsync -an --checksum`` (a dry run) and inspects what it WOULD
    transfer. rsync prints (via ``--out-format=%n``) only items it would
    change, comparing by *checksum* — so any regular-file line means that
    file is missing at the destination OR present with different content.
    Returns:
      * ``None`` — every source file is present at the destination with
        matching content; safe to delete originals.
      * ``(name, None)`` — first source file still absent/different; the
        move must preserve originals.
      * ``("__ERROR__", detail)`` — the verification rsync itself failed or
        timed out; treated as a verification failure (originals preserved).
    Bandwidth-cheap: the NAS checksums its own local disk and only hashes
    cross the wire, so no bulk data is re-transferred.

    ``is_merge`` excludes Finder metadata (``.DS_Store``) on both sides so a
    ``.DS_Store`` that the merge deliberately did not copy is never reported
    as a verification failure. A fresh move carries these along and asks
    for a full compare.
    """
    exclude_ctx = (_rsync_finder_metadata_exclude_file(src_path)
                   if is_merge else contextlib.nullcontext([]))
    with exclude_ctx as metadata_excludes:
        cmd = [rsync_bin, "-an", "--checksum", "--out-format=%n",
               "-e", _ssh_rsh_string(remote),
               *metadata_excludes,
               src_path + "/", rsync_target + "/"]
        try:
            proc = subprocess.run(cmd, capture_output=True, text=True,
                                  timeout=REMOTE_VERIFY_TIMEOUT,
                                  **no_window_kwargs())
        except subprocess.TimeoutExpired:
            return ("__ERROR__", f"verification timed out after "
                    f"{REMOTE_VERIFY_TIMEOUT // 60} minutes")
        except OSError as exc:
            return ("__ERROR__", str(exc))
        if proc.returncode != 0:
            return ("__ERROR__",
                    proc.stderr.strip() or f"rsync exit {proc.returncode}")
        for line in proc.stdout.splitlines():
            name = line.rstrip("\n")
            if not name or name.endswith("/"):
                continue  # directory entry, not a file needing transfer
            return (name, None)
        return None


def remote_verify_files(rsync_bin, src_specs, rsync_target, remote,
                        dest_is_dir=True):
    """Verify an explicit list of source FILES against a remote path.

    The file-list counterpart to ``_remote_verify_complete`` (which compares
    whole directories). The import job rsyncs a batch's card files flat by
    basename into ``rsync_target/``; this runs ``rsync -an --checksum
    <card files...> rsync_target/`` — a dry run that reports (via
    ``--out-format=%n``) every listed file whose counterpart at the remote is
    missing or differs by checksum. Because the sources are the actual CARD
    files, this genuinely confirms the card's bytes landed intact on the NAS
    (comparing the local SMB mount view against the NAS would be
    near-tautological — same physical storage — and would never catch a
    corrupt transfer). Basename-flat comparison lines up with how the
    transfer landed the files.

    ``dest_is_dir`` (default True) treats ``rsync_target`` as a directory
    (``rsync_target/``), so each source lands under its own basename. Set it
    False to verify a single source file against an explicit remote FILE path
    (no trailing ``/``): the import job uses this to verify a collision-
    renamed file (card ``DSC_0001.jpg`` landed at NAS ``DSC_0001_1.jpg``)
    against its actual NAS name.

    Returns:
      * ``None`` — every listed source file is present at the remote with
        matching content.
      * ``(name, None)`` — first source file (rsync's ``%n`` relative name)
        still absent/different at the remote.
      * ``("__ERROR__", detail)`` — the verification rsync itself failed or
        timed out; treated as a verification failure.

    Bandwidth-cheap: the NAS checksums its own local disk and only hashes
    cross the wire.
    """
    if not src_specs:
        return None
    # ``--copy-links`` matches the import-transfer rsync (``_run_remote_import
    # _job`` passes it): the transfer sends the REFERENCED file bytes for a
    # symlinked source, so the source-side hash here must be computed on the
    # same referenced file (not the symlink itself). Without it, ``rsync
    # -an`` (which includes ``-l``) sees a symlink at the source and a real
    # file on the NAS, treats them as mismatched, and reports the just-
    # transferred file as verification-failed. See PR #1113 review.
    cmd = [rsync_bin, "-an", "--checksum", "--copy-links",
           "--out-format=%n", "-e", _ssh_rsh_string(remote)]
    cmd += list(src_specs)
    cmd += [rsync_target + "/" if dest_is_dir else rsync_target]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True,
                              timeout=REMOTE_VERIFY_TIMEOUT,
                              **no_window_kwargs())
    except subprocess.TimeoutExpired:
        return ("__ERROR__", f"verification timed out after "
                f"{REMOTE_VERIFY_TIMEOUT // 60} minutes")
    except OSError as exc:
        return ("__ERROR__", str(exc))
    if proc.returncode != 0:
        return ("__ERROR__",
                proc.stderr.strip() or f"rsync exit {proc.returncode}")
    for line in proc.stdout.splitlines():
        name = line.rstrip("\n")
        if not name or name.endswith("/"):
            continue  # directory entry, not a file needing transfer
        return (name, None)
    return None


def _run_rsync_streamed(src_path, dest_spec, rsync_flags, total_files,
                        progress_cb, rsync_bin="rsync", extra_args=None,
                        stall_timeout=RSYNC_STALL_TIMEOUT, src_specs=None,
                        src_specs_dest_is_dir=True, cancel_check=None):
    """Run rsync, reporting each transferred file through progress_cb.

    rsync's ``--out-format=%n`` prints the relative name of every item it
    transfers (directories end in ``/``). Streaming that line-by-line lets
    the move job show live per-file progress instead of a frozen bar while a
    large copy runs, without changing rsync's copy semantics.

    ``dest_spec`` is a local path for a local move or ``user@host:/path`` for
    a remote (SSH) move; ``rsync_bin`` selects the binary (a bundled GNU rsync
    for remote moves) and ``extra_args`` carries the SSH transport flags
    (``-e ssh ...``, ``--partial``, ``--bwlimit``). ``rsync_flags`` is a list;
    empty strings are dropped so a fresh move can pass no extra flag.

    By default the whole ``src_path`` directory is transferred (``src_path
    + "/"``), the shape ``move_folder`` uses. ``src_specs`` overrides this
    with an explicit list of source *file* paths — the import job's per-batch
    remote copy passes the batch's card files (which land flat by basename
    under ``dest_spec/``), so no local staging tree is materialized. When
    ``src_specs`` is given, ``dest_spec`` is suffixed with ``/`` (files land
    inside the destination directory) unless ``src_specs_dest_is_dir`` is
    False — the import job's collision-rename path passes a single source
    file and an explicit remote FILE path (e.g.
    ``user@host:/dir/DSC_0001_1.jpg``) so the renamed file lands under the
    chosen name rather than its own basename.

    Returns ``(returncode, stderr, timed_out)``. ``timed_out`` is True when
    rsync made no forward progress for ``stall_timeout`` seconds and was
    killed. This is a STALL watchdog rather than a total-runtime cap: a
    healthy but slow transfer (e.g. thousands of RAW files over a network
    share) runs for as long as it keeps moving data, while a genuinely wedged
    rsync still gets reaped. The clock resets on every transferred file and
    on stderr activity, so only true silence trips it.

    ``cancel_check`` (optional callable -> bool) is the caller's Stop
    signal: when it returns True the subprocess is killed within a couple
    of seconds instead of draining the batch. The kill surfaces as an
    ordinary nonzero returncode with ``timed_out`` False — the caller
    already holds the cancel flag it passed in, so it can tell a
    cancellation from a transfer failure without a fourth return value.

    stdout is attached to a pty, not a pipe, wherever the platform allows.
    Apple's openrsync block-buffers stdout when it's a pipe, so its
    ``--out-format`` lines arrive in one burst at process exit — the parent
    sees total silence while a healthy transfer runs, and the watchdog kills
    any copy slower than stall_timeout (a NAS archive of a large shoot can
    never finish). On a pty both openrsync and GNU rsync line-buffer, so
    per-file names stream as they transfer.
    """
    cmd = [rsync_bin, "-a", "--out-format=%n"]
    cmd += list(extra_args or [])
    cmd += [f for f in (rsync_flags or []) if f]
    if src_specs is not None:
        cmd += list(src_specs)
        cmd += [dest_spec + "/" if src_specs_dest_is_dir else dest_spec]
    else:
        cmd += [src_path + "/", dest_spec + "/"]
    master_fd = slave_fd = None
    if hasattr(os, "openpty"):
        try:
            master_fd, slave_fd = os.openpty()
        except OSError:
            master_fd = slave_fd = None
    stdout_arg = subprocess.PIPE if slave_fd is None else slave_fd
    try:
        proc = subprocess.Popen(
            cmd, stdout=stdout_arg, stderr=subprocess.PIPE, text=True,
            **no_window_kwargs(),
        )
    except BaseException:
        # Popen can raise after os.openpty() succeeded (bad rsync_bin,
        # PermissionError, KeyboardInterrupt). Close both pty fds before
        # re-raising so a failed invocation doesn't leak two fds each time.
        for fd in (master_fd, slave_fd):
            if fd is not None:
                os.close(fd)
        raise
    stdout_stream = proc.stdout
    if slave_fd is not None:
        os.close(slave_fd)
        if stdout_stream is None:
            stdout_stream = os.fdopen(master_fd, "r", errors="replace")
            master_fd = None  # closed via stdout_stream below
    if master_fd is not None:
        os.close(master_fd)
    state = {"timed_out": False}
    # Last time rsync showed any sign of life (process start counts, so the
    # silent file-list/scan phase before the first transfer isn't a stall).
    last_activity = {"t": time.monotonic()}
    done = threading.Event()

    def _watchdog():
        # Poll well below stall_timeout so a stall is detected promptly once
        # the window elapses, without busy-waiting. With a cancel_check the
        # poll drops to 1s: Stop has to kill the subprocess within a couple
        # of seconds, not at the stall watchdog's leisurely interval.
        poll = 1.0 if cancel_check is not None else min(stall_timeout, 30)
        while not done.wait(poll):
            if cancel_check is not None and cancel_check():
                proc.kill()
                return
            if time.monotonic() - last_activity["t"] > stall_timeout:
                state["timed_out"] = True
                proc.kill()
                return

    watchdog = threading.Thread(target=_watchdog, daemon=True)
    watchdog.start()

    # Drain stderr on a separate thread. rsync can emit a lot of stderr (e.g.
    # many permission-denied or "file vanished" notices); if that fills the
    # pipe buffer while this thread is blocked reading stdout, rsync blocks on
    # the stderr write and neither side progresses — a deadlock that the stall
    # watchdog would only break after stall_timeout. Reading both streams
    # concurrently keeps the error surfacing promptly, matching the old
    # subprocess.run(capture_output=True) behavior. Reading stderr is also a
    # liveness signal, so it resets the stall clock too.
    stderr_chunks = []

    def _drain_stderr():
        # Iterate line-by-line rather than read(4096): a fixed-size read on a
        # buffered text stream blocks until the buffer fills or EOF, so sparse
        # rsync diagnostics ("file vanished", permission notices) wouldn't
        # refresh last_activity until 4096 chars had accumulated — long enough
        # that the watchdog could kill a transfer that's still emitting
        # stderr. Line iteration yields each message as it's flushed, so every
        # stderr emission counts as liveness.
        for line in proc.stderr:
            stderr_chunks.append(line)
            last_activity["t"] = time.monotonic()

    stderr_thread = threading.Thread(target=_drain_stderr)
    stderr_thread.start()

    copied = 0
    try:
        try:
            for line in stdout_stream:
                # The pty's line discipline translates \n to \r\n (ONLCR),
                # so strip \r too or every filename grows a trailing CR.
                name = line.rstrip("\r\n")
                last_activity["t"] = time.monotonic()
                if not name or name.endswith("/"):
                    continue  # directory entry, not a file
                copied += 1
                if progress_cb:
                    progress_cb(copied, total_files, os.path.basename(name),
                                "Copying files")
        except OSError:
            # Reading the pty master after the child exits raises EIO on
            # Linux (macOS returns plain EOF) — both just mean end-of-output.
            pass
        proc.wait()
    finally:
        done.set()
        if stdout_stream is not None and stdout_stream is not proc.stdout:
            with contextlib.suppress(OSError):
                stdout_stream.close()
    stderr_thread.join()
    watchdog.join()
    return proc.returncode, "".join(stderr_chunks), state["timed_out"]


# Finder writes .DS_Store (window layout, icon positions) into every folder it
# opens, so a staged import the user browsed and the archive folder it merges
# into each hold their own copy with different bytes. That is not a collision
# between photos and must not refuse the merge. A merge ignores these on both
# sides: the pre-merge conflict check, the copy and the post-copy verification
# all skip them, so the destination keeps its own copy and the source's is
# discarded only when the whole source tree is removed after a successful
# merge. If any of those steps refuses the merge, the originals -- .DS_Store
# included -- are left untouched.
FINDER_METADATA_FILES = frozenset({".DS_Store"})


def _escape_rsync_pattern(component):
    """Escape rsync wildmatch metacharacters in a literal path component so
    the resulting include/exclude pattern matches only that spelling. rsync
    treats ``*``, ``?`` and ``[`` as wildcards in patterns (``**`` matches
    multiple components; ``\\`` escapes the next character), so an on-disk
    directory literally named ``a*`` would otherwise turn the generated
    ``--exclude=/a*/.DS_Store`` into a glob that also drops an unrelated
    ``abc/.DS_Store`` subtree — the transfer and verification skip those
    entries, and the post-merge source removal then destroys them.

    Backslash is intentionally encoded as the character class ``[\\\\]``
    rather than doubled. In wildmatch a bare ``\\`` escapes the next
    character (which fails for a trailing ``\\`` and is only interpreted as
    an escape when the pattern contains at least one other wildcard), so
    doubling to ``\\\\`` would emit a pattern that matches two literal
    backslashes on filesystems where wildmatch isn't triggered — a real
    sibling with two backslashes would then swap places with the intended
    target and the transfer/verify would drop its ``.DS_Store`` subtree
    from the merge, letting the post-merge source removal delete it. A
    character class matches exactly one literal ``\\`` under wildmatch
    (its brackets also force wildmatch mode for the other escapes in this
    same pattern) and is a stable rewrite for every literal component.

    A single character-by-character pass rather than chained ``str.replace``
    calls: the class ``[\\\\]`` inserted for a backslash contains a ``[``
    and two ``\\`` characters that would themselves be re-escaped by a
    later replace, either doubling the escape or breaking the class.
    """
    out = []
    for ch in component:
        if ch == "\\":
            # A character class matching exactly one literal backslash.
            # Emits 4 chars: [ \ \ ]
            out.append("[\\\\]")
        elif ch in "*?[":
            out.append("\\" + ch)
        else:
            out.append(ch)
    return "".join(out)


def _rsync_finder_metadata_exclude_patterns(src_path):
    """Anchored rsync exclude patterns for regular Finder metadata FILES
    under ``src_path`` (one pattern per file, without any ``--exclude=``
    prefix — the caller decides whether to splat them as argv flags or
    stream them through ``--exclude-from``).

    Used for merges only. A fresh move still carries these along, matching
    the pre-fix behavior; only a merge into a destination that already
    holds its own copies needs to skip them so a Finder-managed difference
    doesn't fail the copy or the verification.

    The exclude has to skip regular files while leaving directories and
    symlinks with the same name alone: a pattern that matches every entry
    type would silently drop those from the transfer, the conflict probe
    and the checksum verify, and ``shutil.rmtree(src_path)`` after a
    "successful" merge would then delete them from the source without ever
    copying them. rsync's filter language can't distinguish entry type on
    its own, so we walk ``src_path`` ourselves, collect the regular-file
    ``.DS_Store`` entries (not directories, not symlinks — a ``.DS_Store``
    symlink to a directory is classified as a symlink by rsync and would
    slip past an ``--include=<name>/`` guard; a FIFO/socket/device lands in
    ``os.walk``'s ``files`` list too, and ``rsync -a`` would recreate it
    from ``-D``, so an exclude would drop it from every side of the merge
    while the post-merge ``shutil.rmtree`` still deletes it from the
    source), and emit an anchored ``/<relpath>`` for each. Every parent
    component in the relative path is passed through
    ``_escape_rsync_pattern`` so a real directory whose name contains rsync
    wildmatch metacharacters (``*``/``?``/``[``/``\\``) doesn't widen the
    exclude into a glob that catches unrelated sibling subtrees. The
    excludes apply to both sides of every rsync command they're passed to
    (transfer, ``--existing`` conflict probe, ``--checksum`` verify), so a
    same-path Finder-managed difference at the destination is skipped
    alongside its source twin, and every other entry type — including
    symlinks and any real directory that happens to be named
    ``.DS_Store`` — is untouched.
    """
    patterns = []
    if not os.path.isdir(src_path):
        return patterns
    for root, _, files in os.walk(src_path):
        for fn in files:
            if fn not in FINDER_METADATA_FILES:
                continue
            full = os.path.join(root, fn)
            if os.path.islink(full):
                continue
            if not os.path.isfile(full):
                continue
            rel = os.path.relpath(full, src_path).replace(os.sep, "/")
            escaped = "/".join(_escape_rsync_pattern(part)
                               for part in rel.split("/"))
            patterns.append(f"/{escaped}")
    return patterns


def _rsync_finder_metadata_excludes(src_path):
    """rsync ``--exclude=PATTERN`` args, one per regular Finder metadata
    file under ``src_path``. Thin wrapper around
    ``_rsync_finder_metadata_exclude_patterns`` for tests and callers that
    build a small argv directly; production merge callers should prefer
    ``_rsync_finder_metadata_exclude_file`` to keep argv fixed-size even
    when the source tree carries thousands of ``.DS_Store`` files (each
    ``--exclude=`` is its own argv entry, and ``execve``'s argument-size
    limit is finite on every platform — ~256 KB on older macOS — so a
    large enough merge would fail with ``E2BIG`` before rsync could start).
    """
    return [f"--exclude={p}"
            for p in _rsync_finder_metadata_exclude_patterns(src_path)]


@contextlib.contextmanager
def _rsync_finder_metadata_exclude_file(src_path):
    """Yield rsync flags that exclude every regular ``.DS_Store`` file
    under ``src_path`` via ``--exclude-from=<file>`` plus ``--from0``.

    Passing the patterns through a file (rather than a fresh ``--exclude=``
    argv entry per file, as ``_rsync_finder_metadata_excludes`` returns)
    keeps rsync's argv fixed-size no matter how many ``.DS_Store`` files
    the merge source contains. A photo archive with thousands of them can
    otherwise exceed the platform's ``execve`` argument-size limit before
    rsync starts (macOS is the tightest, historically ~256 KB) — the
    ``E2BIG`` that results is a plain ``OSError`` at ``subprocess.run``
    time, and the primary transfer only catches ``FileNotFoundError``, so
    it would escape the normal move-error path.

    Records are written NUL-delimited (bytes via ``os.fsencode``) and
    rsync is asked to parse them that way with ``--from0``. Newline is a
    legal POSIX filename byte, so a plain ``\\n`` join would split one
    pattern like ``/foo\\nbar/.DS_Store`` into two filter records
    (``/foo`` and ``bar/.DS_Store``) — rsync would then exclude an
    unrelated ``/foo`` subtree on both the transfer and the ``--checksum``
    verify, letting the post-merge ``shutil.rmtree`` destroy source files
    that never landed at the destination. NUL is the one byte the
    filesystem cannot put in a filename, so it is the only safe record
    separator.

    Yields ``[]`` when there are no patterns (no temp file is created);
    otherwise yields ``['--from0', '--exclude-from=<file>']`` flags and
    cleans up the file on exit.
    """
    patterns = _rsync_finder_metadata_exclude_patterns(src_path)
    if not patterns:
        yield []
        return
    fd, path = tempfile.mkstemp(
        prefix="vireo-rsync-excludes-", suffix=".bin")
    try:
        with os.fdopen(fd, "wb") as f:
            for pattern in patterns:
                f.write(os.fsencode(pattern))
                f.write(b"\0")
        yield ["--from0", f"--exclude-from={path}"]
    finally:
        with contextlib.suppress(OSError):
            os.unlink(path)


def _find_content_conflict(src_path, dest_path):
    """Return the relative path of the first source file that ALSO exists at
    dest_path but with different content, or None. Run before a merge copies
    anything: a same-name destination file is only safe to treat as "already
    there" if its bytes match the source. A size match alone is not enough —
    filecmp with shallow=False compares contents — so we never overwrite or
    later delete the source over a genuinely different destination file.

    Finder metadata (``.DS_Store``) is ignored only for REGULAR non-symlink
    source files: those are the ones ``_rsync_finder_metadata_excludes``
    drops from the transfer, so a same-name Finder-managed difference at
    the destination is not a photo collision and the source's own copy is
    preserved until the whole tree is removed after a successful merge. A
    ``.DS_Store`` symlink is NOT excluded by the rsync helper (which keeps
    such symlinks) and ``--ignore-existing`` then leaves any pre-existing
    destination entry in place; a basename-only skip here would then let
    a same-sized destination file (different bytes, so a real collision)
    pass the pre-merge check, and the verifier's size-only compare would
    also accept it (``os.path.getsize`` follows the source link), silently
    losing the source symlink when the tree is removed. So the exemption
    applies only to regular non-symlink Finder metadata files; every other
    entry type (symlink, missing source) falls through to the normal
    content check.
    """
    for root, _, files in os.walk(src_path):
        rel = os.path.relpath(root, src_path)
        for fn in files:
            src_file = os.path.join(root, fn)
            if fn in FINDER_METADATA_FILES and \
                    not os.path.islink(src_file) and \
                    os.path.isfile(src_file):
                continue
            rel_name = fn if rel == "." else os.path.join(rel, fn)
            dst_file = os.path.join(dest_path, rel_name)
            if os.path.isfile(dst_file) and \
                    not filecmp.cmp(src_file, dst_file, shallow=False):
                return rel_name
    return None


def _find_remote_content_conflict(rsync_bin, src_path, rsync_target, remote):
    """Remote counterpart to ``_find_content_conflict`` — find the first
    source file whose same-name twin at the remote destination differs in
    content. The destination is on the NAS so we can't filecmp it directly;
    instead run ``rsync -an --existing --checksum`` over SSH, which only
    inspects items that ALREADY exist at the receiver and reports any whose
    bytes don't match. Missing destination files are skipped — those are
    legitimate transfers the merge will perform, not conflicts.
    Returns:
      * ``None`` — no same-name file differs; safe to start the merge.
      * ``(name, None)`` — first colliding file (rsync's relative path).
      * ``("__ERROR__", detail)`` — the conflict probe itself failed; the
        caller treats this as a refusal so a half-transferred state never
        lands on the NAS.
    Without this, ``--ignore-existing`` would still copy every MISSING
    source file before the post-transfer ``--checksum`` verify could spot
    the conflict — leaving the newly-copied files orphaned on the NAS,
    breaking the local-merge contract that a content conflict cancels with
    nothing changed.

    Finder metadata (``.DS_Store``) is excluded on both sides so a Finder
    difference between the local staging tree and the NAS never refuses the
    merge; the local counterpart applies the same skip.
    """
    with _rsync_finder_metadata_exclude_file(src_path) as metadata_excludes:
        cmd = [rsync_bin, "-an", "--existing", "--checksum",
               "--out-format=%n",
               "-e", _ssh_rsh_string(remote),
               *metadata_excludes,
               src_path + "/", rsync_target + "/"]
        try:
            proc = subprocess.run(cmd, capture_output=True, text=True,
                                  timeout=REMOTE_VERIFY_TIMEOUT,
                                  **no_window_kwargs())
        except subprocess.TimeoutExpired:
            return ("__ERROR__", f"conflict check timed out after "
                    f"{REMOTE_VERIFY_TIMEOUT // 60} minutes")
        except OSError as exc:
            return ("__ERROR__", str(exc))
        if proc.returncode != 0:
            return ("__ERROR__",
                    proc.stderr.strip() or f"rsync exit {proc.returncode}")
        for line in proc.stdout.splitlines():
            name = line.rstrip("\n")
            if not name or name.endswith("/"):
                continue  # directory entry, not a file rsync would transfer
            return (name, None)
        return None


def _first_missing_source_file(src_path, dest_path, *, verify_contents=False,
                               is_merge=False, progress=None):
    """Return the relative path of the first source file absent (or
    size-mismatched, or a symlink) at dest_path, or None if every source
    file is present and matches. Used to verify a merge before deleting
    originals.

    With ``verify_contents``, compare fresh bytes as well before allowing
    managed temporary originals to be removed.

    A symlinked destination entry is treated as missing: `os.path.isfile` /
    `os.path.getsize` follow the link, so a symlink pointing back into the
    source tree (directly, or via a symlinked parent directory) would pass
    a size compare even though no independent copy exists at the
    destination — and the post-copy `shutil.rmtree(src_path)` would then
    destroy the only copy. `lexists` lets a broken symlink count as
    missing instead of crashing later checks; `islink` catches the direct
    case; `samefile` catches the symlinked-parent case where `src_file`
    and `dst_file` resolve to the same inode.

    Finder metadata (``.DS_Store``) is skipped for REGULAR non-symlink
    source files, and only when ``is_merge`` says the caller is verifying
    a merge into an existing destination: those are the ones
    ``_rsync_finder_metadata_excludes`` drops from the transfer, so a
    missing or differently-sized destination counterpart is expected and
    must not refuse the delete. A fresh (non-merge) move with
    ``verify_contents`` also reaches this verifier, but there ``.DS_Store``
    IS transferred by the primary rsync path and the shutil fallback, so
    the destination must hold a matching copy before ``rmtree`` removes
    the source — a destination ``.DS_Store`` that disappears between copy
    and verify would otherwise be silently forgiven and take the source's
    copy with it. A ``.DS_Store`` symlink is NOT excluded by the rsync
    helper (which preserves symlinks with that name), but
    ``--ignore-existing`` skips it whenever the destination already holds
    an entry at that path; without a real check here, the source tree
    would then be deleted with the symlink never landing at the
    destination. So the exemption applies only to regular non-symlink
    files under a merge; every other entry type (symlink, directory,
    missing source) and every fresh-move call fall through to the normal
    verification.

    ``progress``, when given, is called as ``progress(checked, rel_name)``
    before each file is examined and ``progress(checked, "")`` once every
    file has passed, where ``checked`` counts the source files done so far.
    """
    checked = 0
    for root, _, files in os.walk(src_path):
        rel = os.path.relpath(root, src_path)
        for fn in files:
            src_file = os.path.join(root, fn)
            rel_name = fn if rel == "." else os.path.join(rel, fn)
            if progress:
                progress(checked, rel_name)
            checked += 1
            if is_merge and fn in FINDER_METADATA_FILES and \
                    not os.path.islink(src_file) and \
                    os.path.isfile(src_file):
                continue
            dst_file = os.path.join(dest_path, rel_name)
            if not os.path.lexists(dst_file) or os.path.islink(dst_file):
                return rel_name
            try:
                if os.path.samefile(src_file, dst_file):
                    return rel_name
            except OSError:
                return rel_name
            if not os.path.isfile(dst_file) or \
                    os.path.getsize(src_file) != os.path.getsize(dst_file):
                return rel_name
            if verify_contents:
                # Read fresh bytes: filecmp caches by stat signatures and a
                # pre-copy conflict check may already have populated it.
                with open(src_file, "rb") as source, open(dst_file, "rb") as dest:
                    while chunk := source.read(1024 * 1024):
                        if chunk != dest.read(len(chunk)):
                            return rel_name
                    if dest.read(1):
                        return rel_name
    if progress:
        progress(checked, "")
    return None


def _verifier_would_accept_skip(src_file, dst_file):
    """True iff a same-name entry at ``dst_file`` would be accepted as
    already-present by ``_first_missing_source_file`` (the post-copy
    verifier the merge gates the source delete on). Mirrors that
    verifier's structural predicates — destination must exist, must NOT be
    a symlink, must NOT resolve to the same inode as the source, and must
    be a regular file. The size check the verifier also performs is
    deliberately omitted: this backs ``preview_merge``, which is name-only
    by design and must not stat the destination tree's bytes. Same-size
    content collisions are caught separately by ``_find_content_conflict``
    before the merge runs; this only filters entries the verifier would
    reject structurally (symlink, directory, broken samefile probe), where
    rsync ``--ignore-existing`` skips the copy but the verifier then
    refuses to delete the originals with ``Verification failed``.
    """
    if not os.path.lexists(dst_file) or os.path.islink(dst_file):
        return False
    try:
        if os.path.samefile(src_file, dst_file):
            return False
    except OSError:
        return False
    return os.path.isfile(dst_file)


def preview_merge(src_path, dest_path):
    """Classify how a merge of ``src_path`` into an existing ``dest_path``
    would play out, the same way the ``rsync --ignore-existing`` copy does:
    every source file absent at the destination is copied; every source file
    already present (by name) is left untouched.

    Returns a dict with ``will_copy``, ``will_skip``, ``will_block``, and
    ``source_total`` (their sum). The total counts *every* file under
    ``src_path`` — XMP sidecars and other companions rsync carries along,
    not just tracked photos — so it reflects what actually transfers,
    unlike a tracked-photo count.

    This is a name-only classification, matching what rsync actually copies:
    a same-name destination *file* whose bytes differ still counts as a
    skip here, because rsync would skip it. That genuine collision is
    caught separately by ``_find_content_conflict``, which refuses the
    whole merge before anything is copied — so this preview never reads
    file contents and stays fast on large trees.

    ``will_block`` covers source files whose destination entry is something
    the post-copy verifier (``_first_missing_source_file``) refuses to
    accept as already-present — a symlink, a directory, or a path that
    resolves to the same inode as the source. ``rsync --ignore-existing``
    silently skips those entries by name, but the verifier then rejects
    them and the merge aborts with "Verification failed", so they must not
    be presented to the user as "already present and will be left
    untouched". Surfaced separately so the confirm dialog can warn that
    the merge would not complete instead of implying a no-op resume.

    It also covers source files that are themselves symlinks with no
    destination entry yet. ``rsync -a`` (and the shutil fallback's
    ``os.symlink``) recreate them as symlinks at the destination rather
    than materializing a regular file — and the verifier then rejects the
    freshly-created symlink and aborts the merge. Telling the user the
    file will be copied when the job deterministically fails at verify
    after creating it is the same false promise as the destination-entry
    case, so it's classified as blocked too.

    Directory symlinks under ``src_path`` count as one transfer item each:
    the rsync ``-a`` path and the shutil fallback (see
    ``_copy_tree_with_progress``) both recreate them as symlinks at the
    destination without descending, so omitting them would let the confirm
    dialog say "All 0 files are already present" for a source that is
    actually just a directory symlink, undercounting what the move
    transfers. The verifier does not check directory entries, so they stay
    name-only (skip iff anything exists at the destination name).
    """
    will_copy = 0
    will_skip = 0
    will_block = 0
    # os.walk defaults to followlinks=False, so a symlinked subdirectory
    # appears in `dirs` but is not descended into — which is exactly the
    # transfer semantics the merge applies, so each such entry is one item.
    for root, dirs, files in os.walk(src_path):
        rel = os.path.relpath(root, src_path)
        for d in dirs:
            if not os.path.islink(os.path.join(root, d)):
                continue
            rel_name = d if rel == "." else os.path.join(rel, d)
            if os.path.lexists(os.path.join(dest_path, rel_name)):
                will_skip += 1
            else:
                will_copy += 1
        for fn in files:
            src_file = os.path.join(root, fn)
            # Merges discard Finder-managed regular ``.DS_Store`` files, so
            # they don't count as a transfer here. Every other entry type
            # with that name (symlink to a file, FIFO/socket, ...) is NOT
            # excluded by ``_rsync_finder_metadata_excludes`` — rsync would
            # copy it (or fail verification if the destination twin blocks
            # it). Apply the same regular-non-symlink predicate the exclude
            # helper uses so a source ``.DS_Store`` symlink still surfaces
            # as a copy/skip/block instead of a silent no-op that the merge
            # dialog reports as "0 files".
            if fn in FINDER_METADATA_FILES and not os.path.islink(src_file) \
                    and os.path.isfile(src_file):
                continue
            rel_name = fn if rel == "." else os.path.join(rel, fn)
            dst_file = os.path.join(dest_path, rel_name)
            if not os.path.lexists(dst_file):
                # A source-file symlink gets recreated as a symlink at the
                # destination by rsync -a / the shutil fallback's os.symlink;
                # the verifier then rejects that symlink via its islink check
                # and the merge aborts with "Verification failed". Surface it
                # as blocked so the dialog never promises a copy that won't
                # survive verification.
                if os.path.islink(src_file):
                    will_block += 1
                else:
                    will_copy += 1
            elif _verifier_would_accept_skip(src_file, dst_file):
                will_skip += 1
            else:
                will_block += 1
    return {
        "will_copy": will_copy,
        "will_skip": will_skip,
        "will_block": will_block,
        "source_total": will_copy + will_skip + will_block,
    }


def _samefile_tristate(a, b):
    """Whether two paths resolve to the same inode, or None if samefile
    raised (broken symlink, permission error, transient race — the probe
    is INCONCLUSIVE, not negative). Callers that need a plain boolean wrap
    with `_samefile_or_false`; callers that have to differentiate
    "definitely different" from "couldn't check" use this directly."""
    try:
        return os.path.samefile(a, b)
    except OSError:
        return None


def _samefile_or_false(a, b):
    result = _samefile_tristate(a, b)
    return False if result is None else result


def _walk_up_paths(p):
    prev = None
    while p and p != prev:
        yield p
        prev = p
        p = os.path.dirname(p)


def _is_case_insensitive_path(path):
    """Whether the filesystem holding `path` treats two case variants as the
    same name (default macOS APFS, Windows).

    Walks up to the deepest existing ancestor of `path`, then creates a
    short-lived probe dir *inside* it with a known case-flippable suffix
    and asks samefile whether the swapped spelling resolves to the same
    inode. The probe name didn't exist a moment ago, so the result reflects
    FS behavior rather than a pre-existing user alias (hard link / symlink
    on a case-sensitive FS), and is conclusive in both directions — True on
    a case-folding FS, raises (and `_samefile_or_false` returns False) on a
    case-sensitive one. Probing inside the ancestor rather than via its own
    basename matters when the deepest existing ancestor is a mount point —
    a case-sensitive APFS volume mounted at `/Volumes/Photos` under default
    case-insensitive macOS HFS+ would otherwise be misread as
    case-insensitive (the basename probe tests `/Volumes`'s handling of
    `Photos`, not what the mounted volume does below it).

    Falls back to scanning existing children for NEGATIVE evidence only
    when the temp probe can't write (read-only ancestor): if any
    letter-bearing child's case-flipped spelling also exists as a DISTINCT
    file (`samefile` == False), the FS is case-sensitive — that can't
    happen on a case-folding FS. `samefile` == True via a pre-existing
    child name is never trusted, since it could be a user-created hard link
    or symlink alias on a case-sensitive FS. A previous version of this
    function scanned children first as an optimization to short-circuit on
    that definitive False, but `move_folder()` reaches this on every move,
    and on the typical case-sensitive destination with no case-twin
    children every per-entry samefile probe is inconclusive (the flipped
    name doesn't exist; samefile raises) — turning a single move into
    O(entries) wasted stats before the temp probe ran anyway.

    Returns False on case-sensitive POSIX (Linux ext4/btrfs, opt-in APFS)
    and when no probe is possible at all (ancestor not a directory, or
    read-only AND no child evidence) — the safe default, since spuriously
    folding case could merge two genuinely distinct paths.
    """
    if os.name == "nt":
        return True
    cur = path
    while cur and not os.path.exists(cur):
        parent = os.path.dirname(cur)
        if parent == cur:
            return False
        cur = parent
    if not cur or not os.path.isdir(cur):
        return False
    try:
        probe = tempfile.mkdtemp(prefix=".vireo_case_probe_", suffix="A",
                                 dir=cur)
    except OSError:
        probe = None
    if probe is not None:
        try:
            flipped = probe[:-1] + probe[-1].swapcase()
            return _samefile_or_false(probe, flipped)
        finally:
            with contextlib.suppress(OSError):
                os.rmdir(probe)
    # Temp probe denied (read-only ancestor). Scan existing children for a
    # definitive False (two distinct case-twin entries — impossible on a
    # case-folding FS). With no temp probe available we have no way to
    # confirm a positive answer, so True via a child name is not trusted
    # and the function returns False if no definitive False is found.
    try:
        entries = os.listdir(cur)
    except OSError:
        return False
    for entry in entries:
        flipped = entry.swapcase()
        if flipped == entry:
            continue
        if _samefile_tristate(
            os.path.join(cur, entry),
            os.path.join(cur, flipped),
        ) is False:
            return False
    return False


def _case_insensitive_root(path):
    """Realpath of the deepest existing ancestor of `path` whose filesystem
    folds case, or None when no such ancestor exists. Used to scope the
    case-folded fallback in `_path_equal_or_descends` to the subtree that
    actually folds — see that function's docstring for why a full-path
    `.lower()` would otherwise collapse genuinely distinct paths on the
    parent (case-sensitive) filesystem.

    Walks the same deepest-existing-ancestor path as the probe inside
    `_is_case_insensitive_path` and delegates to it for the fold check, so
    test monkeypatches of `_is_case_insensitive_path` still take effect.
    On Windows, the boundary is the deepest existing ancestor's drive root.
    """
    if os.name == "nt":
        if not _is_case_insensitive_path(path):
            return None
        cur = path
        while cur and not os.path.exists(cur):
            parent = os.path.dirname(cur)
            if parent == cur:
                return None
            cur = parent
        if not cur:
            return None
        drive, _ = os.path.splitdrive(os.path.realpath(cur))
        return (drive + os.sep) if drive else os.path.realpath(cur)
    if not _is_case_insensitive_path(path):
        return None
    cur = path
    while cur and not os.path.exists(cur):
        parent = os.path.dirname(cur)
        if parent == cur:
            return None
        cur = parent
    if not cur or not os.path.isdir(cur):
        return None
    return os.path.realpath(cur)


# Sentinel for "case-insensitive root not yet probed" — distinct from the
# probed-and-None result (the FS is case-sensitive). Callers inside a loop
# pass the probed value, including a literal None, so lazy re-probing per
# row never happens — the regression `_tracked_destination_overlap` is
# guarding against would otherwise re-run the probe per scanned row on any
# case-sensitive POSIX host (Linux ext4, opt-in APFS).
_UNPROBED_CI_ROOT = object()


def _path_equal_or_descends(candidate, ancestor,
                            case_insensitive_root=_UNPROBED_CI_ROOT):
    """True if `candidate` resolves to the same directory as `ancestor`, or is
    a descendant of it.

    Folds together every directory-alias surface the move guards need:
      - Symlinks: os.path.realpath.
      - Windows case folding: os.path.normcase.
      - Case-insensitive POSIX (default macOS APFS), where the above two are
        not enough — os.path.realpath does not fold case and os.path.normcase
        is a no-op on POSIX, so paths differing only by case string-compare
        unequal even though they resolve to the same inode: os.path.samefile
        (device + inode) is FS-truth on every platform and is used as a
        fallback, including a walk-up ancestor check for the descendant case
        where `candidate` itself doesn't exist yet.
      - Two missing leaves on case-insensitive POSIX, where neither path
        exists yet so samefile has nothing to compare: probe the FS for
        case-insensitivity via the deepest existing ancestor and, if so,
        redo the string compare case-folded — but only over the portion of
        the path actually on the case-folding filesystem.

    `case_insensitive_root`: realpath of the deepest existing case-insensitive
    ancestor of `ancestor`, or None for case-sensitive (no case-folded fallback
    runs). Pass the result of `_case_insensitive_root(ancestor)` if you've
    already computed it (e.g., inside a loop with a fixed ancestor) so the
    probe doesn't re-run per call. Omit (sentinel default) to probe lazily;
    an explicit None means "already probed, no case-insensitive root" and
    skips the lazy probe.

    Scoping the case fold to inside this root matters when only part of the
    path tree folds — a case-insensitive APFS/CIFS volume mounted at
    `/mnt/photos` on a case-sensitive Linux root FS, for example. A stale row
    `/MNT/photos/dst/src` and a move into `/mnt/photos/dst/src` are distinct
    paths because `/MNT` does not resolve to `/mnt` on the parent FS; folding
    the full path with `.lower()` would wrongly refuse the valid move.
    """
    real_c = os.path.normcase(os.path.realpath(candidate))
    real_a = os.path.normcase(os.path.realpath(ancestor))
    if real_c == real_a or real_c.startswith(real_a + os.sep):
        return True

    if os.path.exists(candidate) and os.path.exists(ancestor) \
            and _samefile_or_false(candidate, ancestor):
        return True

    # Containment via case-only alias: walk up candidate's existing ancestors
    # looking for one whose inode matches `ancestor`. Handles the case where
    # the candidate leaf doesn't exist yet but its parent (or an ancestor) is
    # a case-only alias of `ancestor` on a case-insensitive POSIX filesystem.
    if os.path.exists(ancestor):
        for anc in _walk_up_paths(os.path.dirname(candidate)):
            if os.path.exists(anc) and _samefile_or_false(anc, ancestor):
                return True

    # Both leaves missing on a case-insensitive POSIX volume: e.g., a stale
    # folders.path row that differs from the resolved destination only by
    # case before either path has been created on disk. samefile can't fold
    # case for paths that don't exist; probe the FS for case-insensitivity
    # and redo the string compare case-folded so the stale row is still
    # caught before any copy. Restrict the case-folded compare to the subtree
    # below the probed case-insensitive ancestor — anything above it is on
    # the parent (case-sensitive) FS and must match exactly, character-for-
    # character, or we'd collapse distinct paths.
    if case_insensitive_root is _UNPROBED_CI_ROOT:
        case_insensitive_root = _case_insensitive_root(ancestor)
    if case_insensitive_root:
        root = case_insensitive_root
        # `root` may already end with `os.sep` — the filesystem root "/"
        # (deepest existing ancestor is `/` when everything below the
        # destination is missing on a case-insensitive POSIX volume mounted
        # at /), or a Windows drive root like "C:\\". `root + os.sep` would
        # double the boundary to "//" / "C:\\\\" and never match any real
        # path, silently skipping the case-folded compare and letting a
        # stale row like `/photos/src` slip past a move into `/Photos/src`.
        root_with_sep = root if root.endswith(os.sep) else root + os.sep
        if not (real_a == root or real_a.startswith(root_with_sep)):
            return False  # ancestor isn't actually inside the probed root.
        # The case-fold root can appear under a case-only alias in the
        # candidate (stale row `/Photos/DST/src` against probed root
        # `/Photos/dst`). Match the root prefix case-insensitively, then
        # confirm via samefile that the candidate's variant is the same
        # on-disk directory — that distinguishes a real case-fold alias
        # from a distinct path on a case-sensitive parent FS (e.g. `/mnt`
        # vs `/MNT` mount-point pair on Linux), where the case-only twin
        # of the root doesn't exist and samefile raises.
        real_c_low = real_c.lower()
        root_low = root.lower()
        root_low_with_sep = root_with_sep.lower()
        if real_c_low != root_low \
                and not real_c_low.startswith(root_low_with_sep):
            return False  # candidate is above or beside the case-fold subtree.
        candidate_root = real_c[:len(root)]
        if candidate_root != root \
                and not _samefile_or_false(candidate_root, root):
            return False  # case-variant of the root is distinct on the parent FS.
        suffix_c = real_c[len(root):].lower()
        suffix_a = real_a[len(root):].lower()
        # suffix_a == "" means real_a IS the root itself; we've already
        # established real_c is at or under root, so real_c descends from
        # real_a unconditionally. Without this, a root like `/` strips
        # to "" for the ancestor while leaving "photos/src" for the
        # candidate — and `suffix_c.startswith("" + os.sep)` is False
        # because the leading separator was already consumed by the root.
        if suffix_a == "" or suffix_c == suffix_a \
                or suffix_c.startswith(suffix_a + os.sep):
            return True
    return False


def _destination_overlaps_source(src_path, dest_path):
    """True if dest_path equals src_path or one is a descendant of the other.

    The post-copy rmtree(src_path) would delete the only copy of the files if
    dest and src refer to the same on-disk directory, so this is checked
    before any copy. See `_path_equal_or_descends` for the alias surface.
    """
    return (_path_equal_or_descends(dest_path, src_path)
            or _path_equal_or_descends(src_path, dest_path))


def _tracked_destination_overlap(db, folder_id, dest_path):
    """Return another tracked folder at or below dest_path, if one exists.

    When both an exact-match row (a tracked folder alias-equal to
    ``dest_path``) AND a strict-descendant row exist, the exact-match row
    is returned. Otherwise the caller's exact-vs-descendant branch would
    fire non-deterministically based on the arbitrary order SQLite happened
    to return the rows in (e.g. ``/Photos/USA`` inserted before its later-
    scanned parent ``/Photos``): selecting the exact tracked parent would
    get rejected as the unsupported "wrap around a tracked subfolder" case
    just because the child row was seen first.

    The case-insensitive root of `dest_path` is probed once and reused for
    every row — otherwise the probe (an os.listdir of the deepest existing
    ancestor, plus samefile of two child paths) re-runs per non-matching row,
    making this O(tracked_folders × destination_entries) before any copy on
    large catalogs or network-backed destinations.
    """
    dest_ci_root = _case_insensitive_root(dest_path)
    descendant = None
    for row in db.conn.execute(
        "SELECT id, path FROM folders WHERE id != ?", (folder_id,)
    ):
        if _path_equal_or_descends(
            row["path"], dest_path,
            case_insensitive_root=dest_ci_root,
        ):
            # Prefer an exact (alias-folded) match: ``row["path"]`` is
            # at-or-below ``dest_path``, so if ``dest_path`` is ALSO
            # at-or-below ``row["path"]`` the two paths alias-fold to the
            # same directory and this row IS the destination the caller
            # can accept as a merge. Otherwise the row is a strict
            # descendant (the "wrap around" case); remember it as a
            # fallback in case no exact-match row turns up later.
            if _path_equal_or_descends(dest_path, row["path"]):
                return row
            if descendant is None:
                descendant = row
    return descendant


def _rebase_under_stored_ancestor(catalog_path, ancestor_path):
    """Return ``catalog_path`` with any alias-prefix folded to STORED
    ``ancestor_path``.

    ``catalog_path`` is at or below ``ancestor_path`` per
    ``_tracked_destination_ancestor``, but the match may have been via
    ``_path_equal_or_descends``' alias surface (symlink resolution, Windows
    case-fold via ``normcase``, POSIX case-insensitive ``samefile``). In those
    cases ``catalog_path``'s leading components differ character-for-character
    from ``ancestor_path`` even though they resolve to the same on-disk
    directory. ``merge_staged_tree_into_archive`` reads parent rows by exact
    ``WHERE path = ?`` — an alias-prefixed base would miss the tracked rows and
    create a parallel alias-path row set outside the managed archive tree
    (with ``parent_id=NULL``), so re-root the base on the stored form here.

    The exact-overlap branch of ``move_folder`` passes ``tracked["path"]`` as
    the reconcile base directly (there IS no suffix — the destination IS the
    tracked folder); this helper is for the ancestor case where a real
    relative suffix has to be reattached below the stored ancestor.
    """
    if catalog_path == ancestor_path:
        return ancestor_path
    prefix = ancestor_path + os.sep
    if catalog_path.startswith(prefix):
        return catalog_path
    # Symlink alias: realpath collapses the link. On POSIX ``normcase`` is a
    # no-op so the raw realpath is enough; on Windows ``normcase`` also
    # folds case so the ``normcase(realpath(...))`` compare handles both.
    real_ancestor = os.path.realpath(ancestor_path)
    real_catalog = os.path.realpath(catalog_path)
    if real_catalog == real_ancestor:
        return ancestor_path
    real_prefix = real_ancestor + os.sep
    if real_catalog.startswith(real_prefix):
        return ancestor_path + real_catalog[len(real_ancestor):]
    norm_ancestor = os.path.normcase(real_ancestor)
    norm_catalog = os.path.normcase(real_catalog)
    if norm_catalog == norm_ancestor:
        return ancestor_path
    if norm_catalog.startswith(os.path.normcase(real_prefix)):
        return ancestor_path + real_catalog[len(real_ancestor):]
    # Case-only POSIX alias: neither ``realpath`` nor ``normcase`` folds it,
    # so walk up ``catalog_path`` looking for an existing ancestor whose inode
    # matches ``ancestor_path`` — the same samefile fallback
    # ``_path_equal_or_descends`` uses to accept the destination in the first
    # place. Then join the remaining components onto ``ancestor_path``.
    parts = []
    cur = catalog_path
    while cur:
        parent = os.path.dirname(cur)
        if parent == cur:
            break
        if os.path.exists(cur) and _samefile_or_false(cur, ancestor_path):
            if not parts:
                return ancestor_path
            return os.path.join(ancestor_path, *parts)
        parts.insert(0, os.path.basename(cur))
        cur = parent
    # No alias match after all. ``_tracked_destination_ancestor`` already
    # matched via one of the surfaces above, so this shouldn't happen — if
    # something in the alias surface changes and the walk falls through,
    # keep today's behaviour (catalog_path unchanged) rather than silently
    # corrupting the reconcile base.
    return catalog_path


def _tracked_destination_ancestor(db, folder_id, dest_path):
    """Return a tracked folder that is an ancestor of dest_path, if any.

    The complement of ``_tracked_destination_overlap``: that one catches
    tracked folders AT or BELOW ``dest_path``; this one catches tracked
    folders ABOVE it on the path tree.

    The pipeline's local-processing archive step uses ``db.move_folder_path``
    to repoint the catalog after rsync, but ``move_folder_path`` only rewrites
    the moved folder's own path (and cascades to its tracked children). It
    does NOT reparent the moved row under a tracked ancestor of the new path.
    If the user picks an archive destination inside an already-tracked root
    (catalog manages ``/Photos`` and they pick ``/Photos/NewShoot``), the
    archive move succeeds on disk but leaves the catalog with two unrelated
    workspace roots whose path strings overlap — a permanently confusing
    folder tree that breaks future scans of the ancestor root. Reject
    upfront so the user picks a different archive folder before the pipeline
    spends time staging and processing.

    Passes ``case_insensitive_root=None`` to skip the per-row case-fold
    probe; the realpath/normcase comparison at the top of
    ``_path_equal_or_descends`` already catches every same-case ancestor,
    which is the only practical case for archive destinations (the user is
    typing a fresh subfolder name into a UI, not chasing a stale catalog row
    that differs only by case).
    """
    for row in db.conn.execute(
        "SELECT id, path FROM folders WHERE id != ?", (folder_id,)
    ):
        if _path_equal_or_descends(
            dest_path, row["path"],
            case_insensitive_root=None,
        ):
            return row
    return None


def _photo_capture_datetime(photo):
    """Resolve one photo's date-folder timestamp: EXIF capture time, else file
    mtime. Shared by the date-move planner and the UI's example-folder samples
    so a label can never name a folder the move wouldn't create.
    """
    try:
        from .capture_time import _capture_datetime
    except ImportError:
        from capture_time import _capture_datetime

    capture_dt = _capture_datetime(photo)
    if capture_dt is None and photo["file_mtime"] is not None:
        try:
            capture_dt = datetime.fromtimestamp(float(photo["file_mtime"]))
        except (OSError, OverflowError, TypeError, ValueError):
            capture_dt = None
    return capture_dt


def _folder_subtree_photos(db, folder_id):
    """Tracked photos in a folder and its descendants, ordered by id.

    Only the columns date-folder planning actually reads are selected. A
    ``p.*`` here is expensive at library scale: ``photos`` carries two DINOv2
    embedding BLOBs and the full ``exif_data`` JSON, which on a 60k-photo
    subtree is ~800MB pulled into Python to compute one date per row. The
    keystroke-debounced move preflight runs this on every destination edit.

    Descendant matching is alias-aware (symlinks, case-folding volumes,
    Windows separators) — see the inline notes below.
    """
    folder = db.conn.execute(
        "SELECT path FROM folders WHERE id = ?", (folder_id,)
    ).fetchone()
    if not folder:
        raise ValueError("Folder not found")

    # Fold '\' to '/' only on Windows, where either separator can appear in
    # stored paths. On POSIX '\' is a legal filename character — folding it
    # would collapse a sibling like '/photos/a\b' onto the descendants of
    # '/photos/a/b/...' and drag unrelated rows into this move. This mirrors
    # the platform-aware treatment in ``ingest.py``.
    #
    # Also case-fold on Windows: `C:\Photos` and `c:\photos\2026` refer to
    # the same subtree on Windows' case-insensitive FS, and without folding
    # the SQL prefix comparison would drop the differently-cased descendant.
    # SQLite's built-in LOWER() only folds ASCII, so mirror the ingest
    # prefilter's Unicode-aware LOWER_UNICODE helper so both sides agree on
    # non-ASCII stems (e.g. `C:\Älbum`).
    if sys.platform == "win32":
        db.conn.create_function(
            "LOWER_UNICODE", 1,
            lambda s: s.lower() if s is not None else None,
        )
        root = folder["path"].replace("\\", "/").rstrip("/").lower()
        prefix = root + "/"
        path_expr = "LOWER_UNICODE(REPLACE(f.path, '\\', '/'))"
        # Include both true descendants and a separate catalog row that is
        # an exact case/separator alias of the selected folder. The latter is
        # not covered by ``f.id = ?`` and has no trailing slash for the prefix
        # predicate, but Windows resolves both rows to the same directory.
        descendant_predicate = (
            f"({path_expr} = ? OR substr({path_expr}, 1, ?) = ?)"
        )
        descendant_params = (root, len(prefix), prefix)
    else:
        # Always route POSIX descendant matching through the alias-aware
        # containment helper. A raw lexical SQL prefix misses two real-world
        # aliasing surfaces:
        #   - Symlinks. The selected folder ``/photos/card`` may resolve to
        #     ``/mnt/card`` while a tracked child row is stored as
        #     ``/mnt/card/day``. ``substr(f.path, ...)`` compares strings and
        #     never touches the FS, so the descendant is silently dropped
        #     from the date-move plan even though it belongs under the
        #     selected subtree.
        #   - Case-insensitive POSIX volumes (default macOS APFS, mounted
        #     CIFS, opt-in APFS on Linux). Lexical prefixes omit
        #     differently-cased descendants; scoping the fold via
        #     ``_case_insensitive_root`` keeps mixed mount trees safe.
        # ``_path_equal_or_descends`` collapses both — plus normcase on
        # Windows — via ``realpath`` and ``samefile``. On a plain
        # case-sensitive POSIX FS with no symlinks the helper still degrades
        # to a fast string comparison inside ``realpath``. Cache by folder
        # path because the join can visit the same folder once per photo.
        case_insensitive_root = _case_insensitive_root(folder["path"])
        descendant_cache = {}

        def _date_move_descends(candidate):
            if candidate not in descendant_cache:
                descendant_cache[candidate] = int(
                    _path_equal_or_descends(
                        candidate,
                        folder["path"],
                        case_insensitive_root=case_insensitive_root,
                    )
                )
            return descendant_cache[candidate]

        db.conn.create_function(
            "VIREO_DATE_MOVE_DESCENDS", 1, _date_move_descends,
        )
        descendant_predicate = "VIREO_DATE_MOVE_DESCENDS(f.path) = 1"
        descendant_params = ()
    return db.conn.execute(
        f"""SELECT p.id, p.filename, p.exif_data, p.timestamp, p.file_mtime
           FROM photos p
           JOIN folders f ON f.id = p.folder_id
           WHERE f.id = ?
              OR {descendant_predicate}
           ORDER BY p.id""",
        (folder_id, *descendant_params),
    ).fetchall()


def folder_date_move_photo_ids(db, folder_id):
    """The same physical subtree the date-move planner will process.

    A linked root can contain detached descendants. Preview their impact too,
    since moving a physical tree affects photos beyond workspace browse scope.
    Callers must validate access to the selected root before using this helper.
    """
    return [row["id"] for row in _folder_subtree_photos(db, folder_id)]


def plan_folder_date_moves(db, folder_id, destination, folder_template):
    """Plan a folder's tracked photos into capture-date destinations.

    The date comes from the same catalog capture-time helper used elsewhere;
    file mtime is the fallback, matching import's folder-planning behavior.
    Returns a list ordered by rendered relative path so previews and jobs are
    deterministic. Each item contains ``relative_path``, ``destination``,
    ``photo_ids``, and ``photo_count``.
    """
    return plan_folder_date_moves_with_capture_dates(
        db, folder_id, destination, folder_template)[0]


def plan_folder_date_moves_with_capture_dates(
    db, folder_id, destination, folder_template,
):
    """``plan_folder_date_moves`` plus the capture datetimes behind the plan.

    The move page labels each date-format preset with an example folder name,
    and that example has to be a folder this very plan would create. Handing
    back the resolved datetimes lets the preflight endpoint render those
    labels from the same scan that produced the plan — one pass over the
    subtree, and no way for the label to drift from the folder list beside it.

    Returns ``(plans, capture_datetimes)``; ``capture_datetimes`` holds one
    entry per tracked photo, ``None`` where no date could be resolved.
    """
    if not isinstance(destination, str) or not os.path.isabs(destination):
        raise ValueError("destination must be an absolute path")
    if not isinstance(folder_template, str) or not folder_template.strip():
        raise ValueError("folder_template must be a non-empty string")

    try:
        from .ingest import build_destination_path
    except ImportError:
        from ingest import build_destination_path

    photos = _folder_subtree_photos(db, folder_id)

    groups = {}
    capture_dts = []
    template = folder_template.strip()
    for photo in photos:
        capture_dt = _photo_capture_datetime(photo)
        capture_dts.append(capture_dt)
        relative = build_destination_path(capture_dt, template, photo["filename"])
        if not relative:
            raise ValueError("folder template produced an empty path")
        # Canonicalize harmless dot components before grouping or joining.
        # Catalog folder paths are keyed by their exact text, so preserving
        # ``./`` here could create a second row for ``/archive/./2026`` beside
        # the existing ``/archive/2026`` even though both names address the
        # same directory (and produce different developed-output hashes).
        relative = posixpath.normpath(relative)
        if relative == ".":
            raise ValueError("folder template produced an empty path")
        groups.setdefault(relative, []).append(int(photo["id"]))

    # Defense-in-depth: ``build_destination_path`` already rejects unsafe
    # templates and unsafe rendered results (see ``ingest._is_unsafe_path``),
    # but ``plan_folder_date_moves`` and ``move_folder_by_date`` are public
    # functions that could be invoked without the API-layer template guard.
    # Verify the joined absolute path really sits under ``destination`` so a
    # traversal that slips past the string-level checks can't escape the
    # chosen root.
    destination_root = os.path.realpath(destination)
    # Reject a destination root that is itself a regular file or dangling
    # symlink. The ancestor walk below only checks paths *inside*
    # ``destination`` (``depth`` starts at 1), so without this hoisted
    # check a template like ``%Y/%m`` against ``destination='/archive'``
    # where ``/archive`` is a plain file would pass preflight — the
    # rendered leaf ``/archive/2026/07`` does not lexist yet — and
    # ``os.makedirs`` in the worker would then raise
    # ``NotADirectoryError``/``FileExistsError`` instead of returning
    # the structured preflight error this guard is meant to provide.
    if os.path.lexists(destination) and not os.path.isdir(destination):
        raise ValueError(
            f"date destination already exists and is not a directory: "
            f"{destination}"
        )
    # When ``destination`` does not lexist yet, walk its own ancestors
    # upward until one lexists. The check above and the intermediate-
    # ancestor loop below only cover ``destination`` itself and paths
    # *inside* it; without this walk a request like
    # ``destination='/archive/root'`` where ``/archive`` is a plain file
    # (or dangling symlink) passes preflight and ``os.makedirs`` in the
    # worker then raises ``NotADirectoryError``/``FileExistsError``
    # instead of returning the structured date-destination error this
    # guard is meant to provide.
    if not os.path.lexists(destination):
        ancestor = os.path.dirname(destination)
        previous = None
        while ancestor and ancestor != previous:
            if os.path.lexists(ancestor):
                if not os.path.isdir(ancestor):
                    raise ValueError(
                        f"date destination already exists and is not a "
                        f"directory: {ancestor}"
                    )
                break
            previous = ancestor
            ancestor = os.path.dirname(ancestor)
    plans = []
    for relative in sorted(groups):
        candidate = os.path.join(destination, *relative.split("/"))
        resolved = os.path.realpath(candidate)
        try:
            common = os.path.commonpath([destination_root, resolved])
        except ValueError:
            common = ""
        if common != destination_root:
            raise ValueError(
                f"folder template produced an unsafe path: {relative!r}"
            )
        # ``lexists`` (not ``exists``) is needed so a dangling symlink still
        # trips this guard: ``exists`` follows the link and reports False,
        # but ``os.makedirs(exist_ok=True)`` in ``move_photos`` would then
        # raise ``FileExistsError`` for that same lexists-true entry and
        # crash the background job instead of surfacing a normal collision.
        #
        # Walk every intermediate ancestor between ``destination`` and the
        # rendered candidate too. A nested template like ``%Y/%m`` renders
        # ``2026/07``; if ``destination/2026`` is already a regular file (or
        # a dangling symlink), the final candidate does not lexist yet and
        # only the leaf check misses it, but ``os.makedirs`` in the worker
        # would then raise ``NotADirectoryError``/``FileExistsError`` and
        # abort the background job with an opaque exception instead of
        # returning a structured preflight error.
        parts = relative.split("/")
        for depth in range(1, len(parts) + 1):
            ancestor = os.path.join(destination, *parts[:depth])
            if os.path.lexists(ancestor) and not os.path.isdir(ancestor):
                raise ValueError(
                    f"date destination already exists and is not a directory: "
                    f"{ancestor}"
                )
        plans.append({
            "relative_path": relative,
            "destination": candidate,
            "photo_ids": groups[relative],
            "photo_count": len(groups[relative]),
        })
    return plans, capture_dts


def move_folder_by_date(db, folder_id, destination, folder_template,
                        progress_cb=None, developed_dir="", keep_visible=True):
    """Move a folder's tracked photos into capture-date subfolders.

    Each photo uses ``move_photos``' copy/verify/catalog-update/delete order,
    including XMP and RAW/JPEG companions. Existing same-name files are never
    overwritten. Unlike ``move_folder``, this intentionally moves photos (and
    their companions), not unrelated untracked files in the source tree.
    After a successful move, remove the selected source only if empty;
    otherwise report the remaining files for optional cleanup in Jobs.

    When ``developed_dir`` is set (matching the caller's configured
    ``darktable_output_dir``), each moved photo's developed-output file is
    rebased to the new folder's key so exports/full-resolution lookups
    still find the render instead of falling back to RAW. Rebasing has to
    happen per photo here (not per source folder as ``move_folder`` does)
    because photos in one source folder can fan out to many date
    destinations.
    """
    source_row = db.conn.execute("SELECT path FROM folders WHERE id = ?", (folder_id,)).fetchone()
    source_path = source_row["path"] if source_row else None
    source_device = None
    source_inode = None
    if source_path:
        with contextlib.suppress(OSError):
            source_stat = os.stat(source_path)
            source_device, source_inode = source_stat.st_dev, source_stat.st_ino
    groups = plan_folder_date_moves(
        db, folder_id, destination, folder_template,
    )
    if not groups:
        return {
            "moved": 0,
            "errors": ["No tracked photos found in the source folder"],
            "destinations": [],
            "destination_count": 0,
        }
    total = sum(group["photo_count"] for group in groups)
    completed = 0
    moved = 0
    already_in_place = 0
    errors = []
    destinations = []

    # Share one listing cache across all groups so the developed-output
    # subdirs of each source folder are listed once per move-folder-by-date
    # run rather than once per photo. Without this the per-photo relocate
    # helper's ``os.listdir`` would run N times for a folder with N
    # developed photos, degrading large jobs quadratically.
    developed_listing_cache = {}
    for group in groups:
        group_start = completed

        def group_progress(current, _total, filename, _start=group_start):
            if progress_cb:
                progress_cb(_start + current, total, filename,
                            "Organizing by capture date")

        result = move_photos(
            db,
            group["photo_ids"],
            group["destination"],
            progress_cb=group_progress,
            developed_dir=developed_dir,
            developed_listing_cache=developed_listing_cache,
            keep_visible=keep_visible,
        )
        group_moved = int(result.get("moved", 0))
        moved += group_moved
        already_in_place += int(result.get("already_in_place", 0))
        errors.extend(result.get("errors") or [])
        completed += group["photo_count"]
        destinations.append({
            "path": group["destination"],
            "planned": group["photo_count"],
            "moved": group_moved,
            "already_in_place": int(result.get("already_in_place", 0)),
        })
        if progress_cb and completed > group_start + group_moved:
            # Keep the overall bar advancing when a photo was skipped because
            # of a missing source or collision (move_photos only reports
            # successful items).
            progress_cb(completed, total, "", "Organizing by capture date")

    result = {
        "moved": moved,
        "already_in_place": already_in_place,
        "errors": errors,
        "destinations": destinations,
        "destination_count": len(destinations),
    }
    if moved and not errors and source_path:
        try:
            from .move_cleanup import finish_source
        except ImportError:
            from move_cleanup import finish_source
        result["source_cleanup"] = finish_source(db, source_path, source_device, source_inode)
    return result


def _has_untracked_destination_developed(
    destination, stem, developed_dir, case_insensitive=False,
    source_file=None,
):
    """Return True when the destination developed dir(s) already contain
    a same-stem file that no catalog row owns.

    ``move_photos`` guards the catalog side (rejecting two different
    source folders that both want the same destination stem), but the
    developed-render lookup in ``export._iter_developed_outputs``
    resolves by destination folder + stem alone. A leftover file at
    ``<destination>/developed/<stem>.*`` or
    ``<developed_dir>/<developed_folder_key(destination)>/<stem>.*``
    -- from a previously deleted photo, a manually-placed render, or a
    partial copy from a prior aborted move -- would silently become the
    moved photo's developed output. Detect that case so ``move_photos``
    can refuse the move rather than repoint the row against a mismatched
    render.
    """
    try:
        from .export import developed_folder_key
    except ImportError:
        from export import developed_folder_key
    if not destination or not stem:
        return False
    subdirs = [os.path.join(destination, "developed")]
    if developed_dir:
        subdirs.append(
            os.path.join(developed_dir, developed_folder_key(destination))
        )
    for subdir in subdirs:
        if not subdir or not os.path.isdir(subdir):
            continue
        try:
            names = os.listdir(subdir)
        except OSError:
            continue
        expected_stem = stem.casefold() if case_insensitive else stem
        for name in names:
            candidate_stem = os.path.splitext(name)[0]
            if case_insensitive:
                candidate_stem = candidate_stem.casefold()
            if candidate_stem != expected_stem:
                continue
            candidate = os.path.join(subdir, name)
            # A tracked source folder may itself be named ``developed`` and
            # be moved into its parent. In that shape, the default-layout
            # probe points straight back at the source original; it is not an
            # untracked render and will be removed after the verified move.
            if source_file and _samefile_or_false(candidate, source_file):
                continue
            if os.path.isfile(candidate):
                return True
    return False


def _blocked_destination_developed_path(destination, developed_dir):
    """Return a non-directory entry that would block render relocation."""
    try:
        from .export import developed_folder_key
    except ImportError:
        from export import developed_folder_key

    targets = [os.path.join(destination, "developed")]
    if developed_dir:
        targets.append(
            os.path.join(developed_dir, developed_folder_key(destination))
        )
    for target in targets:
        if os.path.lexists(target) and not os.path.isdir(target):
            return target
    return None


def _refuse_every_photo(photo_ids, conflict_msg):
    """Return the ``move_photos`` result that refuses the whole batch."""
    log.warning("Move refused: %s", conflict_msg)
    return {
        "moved": 0,
        "errors": [
            f"{pid}: {conflict_msg}" for pid in photo_ids
        ] or [conflict_msg],
        "destination_folder_id": None,
    }


def _is_same_directory(folder_id, folder_path, dest_folder_id, destination):
    """Whether a photo's folder already is the move destination.

    ``destination`` has been through ``catalog_folder_path``, so a case or
    alias spelling of a cataloged folder resolves to that folder's row and
    the ids match. ``samefile`` covers two catalog rows naming one
    directory (e.g. both spellings on a case-insensitive volume).
    """
    if folder_id == dest_folder_id:
        return True
    try:
        return os.path.samefile(folder_path, destination)
    except OSError:
        return False


def move_photos(db, photo_ids, destination, progress_cb=None,
                developed_dir="", developed_listing_cache=None, cancel_check=None,
                pause_requested=None, pause_callback=None, keep_visible=True):
    """Move individual photos to a destination directory.

    Args:
        db: Database instance
        photo_ids: list of photo IDs to move
        destination: absolute path to target directory
        progress_cb: optional callback(current, total, filename)
        developed_dir: optional path to the configured
            ``darktable_output_dir``. When set, each moved photo's
            developed-output file — nested under a hash of its folder
            path via ``export.developed_folder_key`` — is rebased to
            match the new folder key so exports/full-resolution lookups
            still find the render instead of falling back to RAW.
        developed_listing_cache: optional dict shared across successive
            ``move_photos`` calls (e.g. from ``move_folder_by_date``) to
            amortize ``os.listdir`` on the per-source-folder developed
            subdirs. Without this, fanning N photos from one source
            folder across many destination groups relists the same
            developed subdir N times.

    With separate pause hooks, cancel_check must be cancellation-only.
    pause_requested is a non-parking probe; pause_callback runs after counts
    are reconciled, so a paused move leaves the folder tree consistent.

    Returns dict with keys: moved (int), already_in_place (int: photos
    already in the destination and left untouched), errors (list of str)
    """
    if developed_listing_cache is None:
        developed_listing_cache = {}
    from file_identity import catalog_folder_path

    # ``os.makedirs(..., exist_ok=True)`` raises ``FileExistsError`` when the
    # path exists but is a regular file. That would abort the whole batch
    # with an opaque exception, and for date-organized moves the preflight
    # can't rely on ``os.path.isdir`` alone to catch it. Detect that case
    # here and return a structured error so the caller reports every
    # affected photo instead of a mid-run crash.
    if os.path.lexists(destination) and not os.path.isdir(destination):
        return _refuse_every_photo(
            photo_ids, f"destination path is not a directory: {destination}",
        )
    os.makedirs(destination, exist_ok=True)
    destination = catalog_folder_path(db, destination)
    blocked_developed = _blocked_destination_developed_path(
        destination, developed_dir,
    )
    if blocked_developed:
        return _refuse_every_photo(
            photo_ids,
            "developed output path is not a directory: "
            f"{blocked_developed}",
        )
    total = len(photo_ids)
    move = _PhotoMove(db, destination, developed_dir, developed_listing_cache)
    move.keep_visible = keep_visible
    move.ensure_destination_folder()
    move.load_destination_stem_origins()
    move.load_source_stem_counts(photo_ids)

    try:
        for i, pid in enumerate(photo_ids):
            if pause_requested and pause_requested():
                db.update_folder_counts()
                if pause_callback:
                    pause_callback()
            if cancel_check and cancel_check():
                break
            item = move.check_photo(pid)
            if item is None:
                continue
            if move.is_already_in_place(item):
                if progress_cb:
                    progress_cb(i + 1, total, item.photo["filename"])
                continue
            if not move.copy_files(item):
                continue
            move.update_catalog(item)
            move.relocate_developed(item)
            move.remove_originals(item)

            move.moved += 1

            if progress_cb:
                progress_cb(i + 1, total, item.photo["filename"])
    finally:
        # Always update folder counts so they stay consistent even if an
        # exception interrupts the move loop after some photos were committed.
        if move.moved > 0:
            db.update_folder_counts()

    return {"moved": move.moved, "already_in_place": move.already_in_place,
            "errors": move.errors,
            "destination_folder_id": move.dest_folder_id}


# ``destination_stem_origins.get`` default for a stem no destination row holds.
_NO_DESTINATION_STEM = object()


@dataclass
class _PhotoToMove:
    """One photo that passed the source lookup in ``move_photos``."""

    pid: object
    photo: dict
    src_dir: str
    src_file: str
    stem: str
    stem_key: str
    dst_file: str = ""
    companions: list = field(default_factory=list)
    xmp_companion: str = ""
    preserve_source_render: bool = False


class _PhotoMove:
    """Run-wide state for one ``move_photos`` call.

    Each pre-copy check records its error and returns None/False so
    ``move_photos`` skips to the next photo. The phases from the catalog
    update on never skip.
    """

    def __init__(self, db, destination, developed_dir, developed_listing_cache):
        self.db = db
        self.destination = destination
        self.developed_dir = developed_dir
        self.developed_listing_cache = developed_listing_cache
        self.moved = 0
        self.already_in_place = 0
        self.in_place_folders = {}
        self.errors = []
        self.managed_default_developed = {}
        self.copied_xmp_companions = set()

        # Both the photo destination and a separately configured developed-output
        # directory can impose case-folded render names. For example, originals
        # may land on case-sensitive ext4 while renders land on a default macOS
        # APFS volume; ``IMG.CR3`` and ``img.NEF`` coexist in the former but their
        # ``*.jpg`` renders collide in the latter. Fold whenever either output
        # volume is case-insensitive before every lookup/write into
        # ``destination_stem_origins``.
        self.render_case_insensitive = _is_case_insensitive_path(destination)
        if developed_dir:
            self.render_case_insensitive = self.render_case_insensitive or \
                _is_case_insensitive_path(developed_dir)

        self.dest_folder_id = None
        self.workspace_linked = False
        self.destination_stem_origins = {}
        self.destination_stem_exact = {}
        self.photos_map = {}
        self.source_stem_counts = {}

    def stem_key(self, raw_stem):
        return raw_stem.casefold() if self.render_case_insensitive else raw_stem

    def ensure_destination_folder(self):
        """Ensure destination folder record exists (workspace link deferred until first successful move)."""
        db = self.db
        destination = self.destination
        dest_row = db.conn.execute("SELECT id FROM folders WHERE path = ?", (destination,)).fetchone()
        if dest_row:
            self.dest_folder_id = dest_row["id"]
        else:
            # Insert folder record without auto-linking to workspace (add_folder would auto-link).
            # Set parent_id from the nearest existing ancestor so the destination
            # nests correctly in the browse tree instead of floating as a root.
            cur = db.conn.execute(
                "INSERT OR IGNORE INTO folders (path, name, parent_id) VALUES (?, ?, ?)",
                (destination, os.path.basename(destination),
                 db.nearest_ancestor_folder_id(destination)),
            )
            db.conn.commit()
            if cur.rowcount > 0:
                self.dest_folder_id = cur.lastrowid
            else:
                self.dest_folder_id = db.conn.execute(
                    "SELECT id FROM folders WHERE path = ?", (destination,)
                ).fetchone()["id"]

    def load_destination_stem_origins(self):
        """Record which source folder each destination stem came from.

        Provenance is keyed by the source folder's **path** (not folders.id).
        SQLite ``INTEGER PRIMARY KEY`` without AUTOINCREMENT can reuse a freed
        rowid after ``Database.delete_folder``; storing the reusable id would
        let a new unrelated folder that lands on the same rowid compare equal
        to a stale reference and bypass the collision guard in
        ``refuse_render_collision``.
        """
        destination_stem_origins = self.destination_stem_origins
        destination_stem_exact = self.destination_stem_exact
        for row in self.db.conn.execute(
            "SELECT filename, last_move_source_folder_path "
            "FROM photos WHERE folder_id = ?",
            (self.dest_folder_id,),
        ):
            exact_stem = os.path.splitext(row["filename"])[0]
            stem = self.stem_key(exact_stem)
            origin = row["last_move_source_folder_path"]
            known_origin = destination_stem_origins.get(
                stem, _NO_DESTINATION_STEM,
            )
            if known_origin is _NO_DESTINATION_STEM:
                destination_stem_origins[stem] = origin
                destination_stem_exact[stem] = exact_stem
            elif known_origin != origin or \
                    destination_stem_exact[stem] != exact_stem:
                # Conflicting or partly unknown provenance cannot prove that a
                # new same-stem photo shares the existing developed render. On a
                # folding render volume, case-only stems from the same source are
                # distinct source renders too, so exact spelling is part of the
                # proof even though the destination lookup key is folded.
                destination_stem_origins[stem] = None

    def load_source_stem_counts(self, photo_ids):
        """Load the photos and count same-stem rows in each source folder."""
        db = self.db
        self.photos_map = db.get_photos_by_ids(photo_ids)
        source_stem_counts = self.source_stem_counts
        for source_folder_id in {
            photo["folder_id"] for photo in self.photos_map.values()
        }:
            for row in db.conn.execute(
                "SELECT filename FROM photos WHERE folder_id = ?",
                (source_folder_id,),
            ):
                source_stem = os.path.splitext(row["filename"])[0]
                key = (source_folder_id, source_stem)
                source_stem_counts[key] = source_stem_counts.get(key, 0) + 1

    def check_photo(self, pid):
        """Look up one photo and run every pre-copy refusal.

        Returns the photo to move, or None once the reason it is skipped has
        been recorded.
        """
        photo = self.photos_map.get(pid)
        if not photo:
            self.errors.append(f"Photo {pid} not found in database")
            return None

        folder_row = self.db.conn.execute(
            "SELECT path FROM folders WHERE id = ?", (photo["folder_id"],)
        ).fetchone()
        src_dir = folder_row["path"]
        src_file = os.path.join(src_dir, photo["filename"])
        stem = os.path.splitext(photo["filename"])[0]

        if not os.path.isfile(src_file):
            log.warning("Move skipped for %s: source file missing", photo["filename"])
            self.errors.append(f"{photo['filename']}: source file missing")
            return None

        if self.folder_is_destination(photo["folder_id"], src_dir):
            return _PhotoToMove(
                pid=pid, photo=photo, src_dir=src_dir, src_file=src_file,
                stem=stem, stem_key=self.stem_key(stem),
            )
        item = _PhotoToMove(
            pid=pid, photo=photo, src_dir=src_dir, src_file=src_file,
            stem=stem, stem_key=self.stem_key(stem),
        )
        if self.refuse_render_collision(item):
            return None
        if self.refuse_destination_collision(item):
            return None
        return item

    def folder_is_destination(self, folder_id, src_dir):
        if folder_id not in self.in_place_folders:
            self.in_place_folders[folder_id] = _is_same_directory(
                folder_id, src_dir, self.dest_folder_id, self.destination,
            )
        return self.in_place_folders[folder_id]

    def is_already_in_place(self, item):
        """Count photos already in the destination without touching their files."""
        if self.folder_is_destination(item.photo["folder_id"], item.src_dir):
            self.already_in_place += 1
            return True
        return False

    def refuse_render_collision(self, item):
        """Refuse a photo whose developed render would collide at the destination.

        Developed outputs are addressed by destination folder + stem,
        not by the original extension. Same-stem photos from one source
        folder intentionally share a render (RAW+JPEG), but two source
        folders can hold distinct renders with the same filename. Do not
        merge the latter into one destination and silently make one row
        display/export the other's edit.
        """
        photo = item.photo
        existing_origin = self.destination_stem_origins.get(
            item.stem_key, _NO_DESTINATION_STEM,
        )
        if existing_origin is not _NO_DESTINATION_STEM \
                and (existing_origin != item.src_dir or
                     self.destination_stem_exact.get(item.stem_key) != item.stem):
            log.warning(
                "Move skipped for %s: developed render stem collides at "
                "destination", photo["filename"],
            )
            self.errors.append(
                f"{photo['filename']}: developed render stem already "
                "exists at destination"
            )
            return True

        # The catalog-only check above can't see files on disk that no
        # tracked photo owns: a leftover render from a previously
        # deleted photo, a manually-placed file, or a partial copy from
        # an aborted earlier move. ``_iter_developed_outputs`` resolves
        # developed renders by destination folder + stem alone, so an
        # untracked ``<destination>/developed/<stem>.*`` or
        # ``<developed_dir>/<developed_folder_key(destination)>/<stem>.*``
        # would be silently served as this photo's developed output
        # after the row is repointed. Treat it as a move collision
        # before touching the row.
        if existing_origin is _NO_DESTINATION_STEM and \
                _has_untracked_destination_developed(
                    self.destination, item.stem, self.developed_dir,
                    case_insensitive=self.render_case_insensitive,
                    source_file=item.src_file,
                ):
            log.warning(
                "Move skipped for %s: developed render already exists "
                "at destination", photo["filename"],
            )
            self.errors.append(
                f"{photo['filename']}: developed render already exists "
                "at destination"
            )
            return True
        return False

    def refuse_destination_collision(self, item):
        """Refuse a photo whose file or a companion already exists at the destination."""
        photo = item.photo
        src_dir = item.src_dir
        item.dst_file = os.path.join(self.destination, photo["filename"])
        if os.path.exists(item.dst_file):
            log.warning("Move skipped for %s: already exists at destination", photo["filename"])
            self.errors.append(f"{photo['filename']}: already exists at destination")
            return True

        # Gather companion files
        item.companions = _companion_files(photo, src_dir)
        _src_xmp = _xmp_path(item.src_file)
        item.xmp_companion = (
            os.path.basename(_src_xmp) if _src_xmp
            else os.path.splitext(photo["filename"])[0] + ".xmp"
        )

        # Check companion collisions
        for comp in item.companions:
            if os.path.exists(os.path.join(self.destination, comp)):
                # Same-stem photos intentionally share one XMP. If an
                # earlier row in this batch already copied that exact
                # source sidecar into this destination, reuse the verified
                # copy instead of treating the later sibling as a
                # collision. A pre-existing untracked XMP still blocks.
                if comp == item.xmp_companion and \
                        (src_dir, comp) in self.copied_xmp_companions:
                    continue
                self.errors.append(f"{comp}: companion file already exists at destination")
                return True
        return False

    def copy_files(self, item):
        """Copy and verify the photo and its companions; False skips the photo."""
        photo = item.photo
        src_dir = item.src_dir
        destination = self.destination
        copied_xmp_companions = self.copied_xmp_companions
        # Copy main file. A copy error (disk full, share dropped) fails
        # this photo only; the rest of the batch still moves.
        try:
            copied_ok = _copy_and_verify(item.src_file, item.dst_file)
        except OSError as e:
            log.warning("Move skipped for %s: copy failed: %s", photo["filename"], e)
            self.errors.append(f"{photo['filename']}: copy failed: {e}")
            return False
        if not copied_ok:
            log.warning("Move skipped for %s: verification failed after copy", photo["filename"])
            self.errors.append(f"{photo['filename']}: verification failed after copy")
            return False

        # Copy companions
        copied_companions = []
        for comp in item.companions:
            comp_src = os.path.join(src_dir, comp)
            comp_dst = os.path.join(destination, comp)
            if comp == item.xmp_companion and \
                    (src_dir, comp) in copied_xmp_companions:
                continue
            try:
                comp_copied = _copy_and_verify(comp_src, comp_dst)
                comp_error = "companion verification failed"
            except OSError as e:
                comp_copied = False
                comp_error = f"companion copy failed: {e}"
            if not comp_copied:
                self.errors.append(f"{comp}: {comp_error}")
                # Clean up what we copied
                os.remove(item.dst_file)
                for cc in copied_companions:
                    os.remove(os.path.join(destination, cc))
                    if cc == item.xmp_companion:
                        copied_xmp_companions.discard((src_dir, cc))
                return False
            copied_companions.append(comp)
            if comp == item.xmp_companion:
                copied_xmp_companions.add((src_dir, comp))
        return True

    def update_catalog(self, item):
        """Repoint the verified photo's row at the destination folder."""
        db = self.db
        link_destination = not self.workspace_linked and db.active_workspace_id is not None
        # Link destination and repoint the photo together, before deleting
        # originals. A failed visibility write must not leave a folder-wide
        # link exposing unrelated destination siblings after a failed move.
        db.conn.execute("SAVEPOINT photo_move_visibility")
        try:
            if link_destination:
                db._add_workspace_folder_no_commit(
                    db.active_workspace_id, self.dest_folder_id, restore_removed=True,
                )
            db.photo_visibility.preserve_for_move(item.pid, self.keep_visible)
            db.conn.execute(
                "UPDATE photos SET folder_id = ?, "
                "last_move_source_folder_path = ? WHERE id = ?",
                (self.dest_folder_id, item.src_dir, item.pid),
            )
            db.conn.execute("RELEASE photo_move_visibility")
            db.conn.commit()
        except BaseException:
            db.conn.execute("ROLLBACK TO photo_move_visibility")
            db.conn.execute("RELEASE photo_move_visibility")
            raise
        if link_destination:
            self.workspace_linked = True
            db._new_images_cache.invalidate_workspaces(db._db_path, [db.active_workspace_id])
        # Pin the stem to the proven source folder path so a same-source
        # sibling can follow in this call while a distinct source is
        # still rejected. Using the path (not folders.id) survives a
        # later delete/re-create of the source folder that would reuse
        # the same rowid.
        self.destination_stem_origins[item.stem_key] = item.src_dir
        self.destination_stem_exact[item.stem_key] = item.stem

    def relocate_developed(self, item):
        """Rebase this photo's developed-output file(s) for the new folder.

        Runs BEFORE removing originals. Both develop-job layouts need to
        move — the configured ``darktable_output_dir`` (hashed under
        ``developed_folder_key``) and the default ``<folder>/developed/``
        subdir the job writes to when no output dir is configured. The
        folder_id update in ``update_catalog`` just invalidated both lookups;
        without this rebase, ``_iter_developed_outputs`` probes the destination
        folder and misses renders left under the old source folder, so
        exports/full-resolution fall back to the RAW. Doing it before
        cleanup means a subsequent os.remove failure (read-only source
        dir, locked file on Windows) still leaves catalog and developed
        renders in agreement at the new location.
        """
        try:
            from .export import (
                relocate_default_developed_file,
                relocate_developed_file,
            )
        except ImportError:
            from export import (
                relocate_default_developed_file,
                relocate_developed_file,
            )
        src_dir = item.src_dir
        self.release_source_stem(item)
        if self.developed_dir:
            relocate_developed_file(
                self.developed_dir, src_dir, self.destination, item.stem,
                self.developed_listing_cache, item.preserve_source_render,
            )
        default_developed_path = os.path.join(src_dir, "developed")
        if src_dir not in self.managed_default_developed:
            # A real catalog folder may legitimately be named
            # ``developed``. Treating its matching-stem originals as
            # generated renders would move them before their own folder
            # group is processed. Skip the default-layout relocation
            # whenever that directory is managed (or contains another
            # managed folder); its tracked photos move through the normal
            # catalog path instead.
            self.managed_default_developed[src_dir] = bool(
                _tracked_destination_overlap(
                    self.db, item.photo["folder_id"], default_developed_path,
                )
            )
        if not self.managed_default_developed[src_dir]:
            relocate_default_developed_file(
                src_dir, self.destination, item.stem,
                self.developed_listing_cache, item.preserve_source_render,
            )

    def release_source_stem(self, item):
        """Count the moved row out of its source stem.

        Sets ``item.preserve_source_render``: True while a same-stem sibling
        is still in the source folder.
        """
        db = self.db
        src_dir = item.src_dir
        stem = item.stem
        source_stem_counts = self.source_stem_counts
        source_stem_key = (item.photo["folder_id"], stem)
        source_stem_counts[source_stem_key] = max(
            0, source_stem_counts.get(source_stem_key, 1) - 1,
        )
        item.preserve_source_render = source_stem_counts[source_stem_key] > 0
        if not item.preserve_source_render:
            # No same-stem sibling remains in the source folder now
            # that this row moved out. Expire the destination
            # provenance for the stem so a later rescan/import of an
            # unrelated ``IMG.*`` back into the same source path
            # can't slip past the same-stem developed-render
            # collision guard by matching this row's stale origin.
            # Developed-output lookup at the destination is keyed by
            # folder + stem only, so a spoofed provenance would let
            # the new photo display/export this row's edit. Clear
            # every destination row (across all destinations, in
            # case fanout by date sent same-source siblings to
            # different folders) whose stored provenance is this
            # drained source; the ``os.path.splitext`` filter in
            # Python keeps ``foo.bar`` and ``foo`` from
            # collapsing under a naive ``LIKE 'foo.%'``.
            stale_ids = [
                row["id"] for row in db.conn.execute(
                    "SELECT id, filename FROM photos "
                    "WHERE last_move_source_folder_path = ?",
                    (src_dir,),
                )
                if os.path.splitext(row["filename"])[0] == stem
            ]
            if stale_ids:
                placeholders = ",".join("?" for _ in stale_ids)
                db.conn.execute(
                    f"UPDATE photos SET "
                    f"last_move_source_folder_path = NULL "
                    f"WHERE id IN ({placeholders})",
                    stale_ids,
                )
                db.conn.commit()
            self.destination_stem_origins[item.stem_key] = None

    def remove_originals(self, item):
        """Delete the source original and companions, now that it is safe.

        The catalog and developed outputs are already at the new location,
        so a cleanup failure here is post-commit: report it as a per-photo
        error so the caller can surface the leftover originals, but keep the
        batch moving. Without this catch a single OSError (read-only source
        directory, locked file on Windows) would abort every remaining photo
        in the batch with the catalog already repointed for the ones
        processed so far.
        """
        try:
            os.remove(item.src_file)
            for comp in item.companions:
                # Keep a shared XMP at the source until the final
                # same-stem catalog row leaves. Date-organized moves call
                # move_photos once per destination group; preserving the
                # sidecar after an early group lets each later group copy
                # the metadata before the final sibling removes it.
                if comp == item.xmp_companion and item.preserve_source_render:
                    continue
                comp_src = os.path.join(item.src_dir, comp)
                if os.path.isfile(comp_src):
                    os.remove(comp_src)
        except OSError as exc:
            log.warning(
                "Post-commit cleanup of %s failed: %s",
                item.src_file, exc,
            )
            self.errors.append(
                f"{item.photo['filename']}: original not deleted ({exc})"
            )


def _plan_moved_file_mtimes(db, src_path, dest_path, progress_cb=None):
    """Plan re-stamping ``photos.file_mtime`` from the copy at ``dest_path``.

    A copy does not always carry the source's timestamp across. rsync
    writes each file under a temp name, sets the times on it, then renames
    it into place -- and a rename is not guaranteed to preserve the mtime.
    On a macOS smbfs mount against a Synology share (measured: ``copy2``
    to the final name keeps the timestamp, the same file renamed into
    place comes back stamped with the current time) every transferred file
    lands with a fresh mtime, so the catalog is left describing a
    timestamp that no file on disk has.

    That matters because the incremental scan's "unchanged" test is
    ``file_mtime`` + ``file_size`` (see ``scanner.scan``). A folder whose
    recorded timestamps no longer match disk is re-hashed and re-phashed
    in full on the next scan, only to discover that nothing changed --
    tens of GB read back over the wire for a folder of RAWs on a network
    mount.

    Only rows the catalog still describes accurately are re-stamped: the
    source file must still carry the stored ``file_mtime`` and
    ``file_size``. A photo edited since its last scan fails that test and
    keeps its stale timestamp, so the next scan still reprocesses it.
    Adopting the destination's timestamp for those would be worse than the
    problem being fixed here -- the row would look current while its hash,
    phash and metadata still described the pre-edit bytes, and no later
    scan would ever revisit it.

    A source file that cannot be stat'd is skipped: verification walked the
    source tree, so it never made a claim about a row whose file is already
    gone, and there is nothing to re-stamp. A DESTINATION file that cannot
    be stat'd is different -- see below.

    Call after the copy is verified and before the originals are removed,
    while both sides are still readable and the rows still name the source
    files.

    Returns ``(updates, unreadable)``. ``updates`` is ``executemany``
    parameters for the rows to re-stamp; read-only itself, so the caller
    applies them once the catalog update it belongs with has gone through
    and a cascade that fails leaves no half-applied timestamps behind.
    ``problem`` describes a destination file that contradicts what
    verification established -- missing, unreadable, or a different size
    from the source still sitting next to it -- or None.

    That second return value exists because this pass is the last thing
    that looks at the destination before ``shutil.rmtree`` deletes the
    originals. Verification established "every source file is present at
    the destination" minutes earlier; a source that is still here whose
    copy has since vanished or gone unreadable means that no longer holds,
    and ``check_staged_mount`` will not notice -- it re-checks mount
    identity and availability, not the files. Swallowing that would throw
    away first-hand evidence at the worst possible moment, so it is handed
    back for the caller to treat as a verification failure.
    """
    # The repo's literal subtree predicate, not a raw LIKE: LIKE would read
    # ``_`` and ``%`` in a real folder path as wildcards and match
    # case-insensitively, pulling in sibling trees this move never touched --
    # and their rows would then resolve to files outside ``dest_path``. It
    # also normalizes separators, so Windows descendants stored with
    # backslashes are matched rather than silently skipped.
    #
    # It is a PREFILTER, not the authority. ``_path_for_subtree_match`` folds
    # ``\\`` to ``/`` on every platform, and ``\\`` is a legal filename
    # character on POSIX -- so moving ``/photos/shoot\\1`` normalizes to the
    # same prefix as the unrelated ``/photos/shoot/1`` tree. Every row is
    # re-checked below against the move module's own alias-folding
    # containment test (symlinks, Windows case folding, case-insensitive
    # POSIX), which is FS truth rather than string shape.
    prefix = _subtree_prefix(src_path)
    rows = db.conn.execute(
        """SELECT p.id, p.filename, p.file_mtime, p.file_size,
                  f.path AS folder_path
           FROM photos p JOIN folders f ON f.id = p.folder_id
           WHERE f.path = ?
              OR substr(REPLACE(f.path, '\\', '/'), 1, ?) = ?""",
        (src_path, len(prefix), prefix),
    ).fetchall()
    updates = []
    # Hoisted: the probe is per-ancestor, and the containment answer depends
    # only on the folder, so a tree of thousands of photos costs one realpath
    # per distinct folder rather than one per photo.
    ci_root = _case_insensitive_root(src_path)
    contained = {}
    total_rows = len(rows)
    # Announced even when there is nothing to check, so the step always
    # starts and finishes rather than being skipped over.
    if progress_cb:
        progress_cb(0, total_rows, "", "Checking timestamps")
    for index, row in enumerate(rows):
        # One stat per side per photo, on a mount that may be slow enough
        # for that to be visible, so count the rows as they go.
        if progress_cb and index % 100 == 0:
            progress_cb(index, total_rows, row["filename"],
                        "Checking timestamps")
        folder_path = row["folder_path"]
        if folder_path not in contained:
            contained[folder_path] = _path_equal_or_descends(
                folder_path, src_path, ci_root)
        if not contained[folder_path]:
            # Prefilter slack: this row is not actually in the moved tree.
            continue
        stored_mtime, stored_size = row["file_mtime"], row["file_size"]
        if stored_mtime is None or stored_size is None:
            continue
        src_file = os.path.join(folder_path, row["filename"])
        if os.path.islink(src_file):
            # Scanner discovery admits file symlinks, and ``os.stat`` below
            # follows them -- but a relative target resolves against the
            # directory holding the link, so the same target string can point
            # at a different file once the link has moved. Re-stamping from
            # whatever the destination-side link resolves to would tell the
            # incremental scanner that bytes it has never seen are unchanged,
            # leaving this row's hash and metadata describing the wrong file.
            # Leave it stale; a rescan resolves the link itself.
            continue
        # Prefix-strip rather than ``os.path.relpath``: relpath is happy to
        # walk out of the subtree with ``..`` if a row ever slipped past the
        # predicate above, which would point this at a file the move never
        # copied.
        relative = _subtree_relative(folder_path, src_path)
        dst_file = _join_subtree_path(
            dest_path,
            f"{relative}/{row['filename']}" if relative else row["filename"],
        )
        try:
            src_st = os.stat(src_file)
        except OSError:
            # A row whose file is already gone: not something this move
            # copied, and not something verification vouched for.
            continue
        try:
            dst_st = os.stat(dst_file)
        except OSError:
            return [], (f"'{dst_file}' is missing or unreadable at the "
                        f"destination")
        if src_st.st_mtime != stored_mtime or src_st.st_size != stored_size:
            # The row does not describe the file being moved -- it was
            # edited (or replaced) since its last scan. Leave it stale so
            # the scan that would have caught that still does.
            continue
        if dst_st.st_size != stored_size:
            # The guard above already confirmed the source still matches the
            # row, so a differently sized copy differs from the file this
            # move is about to delete. On a merge the verification below
            # rejects it; a fresh local move only compares file counts, so
            # nothing else ever would, and the rmtree would take the intact
            # original with it. Skipping the row would leave that damage
            # unreported -- the same mistake as swallowing a failed stat.
            return [], (f"'{dst_file}' does not match the source's size at "
                        f"the destination")
        if dst_st.st_mtime == stored_mtime:
            # The timestamp survived the copy: nothing to correct.
            continue
        updates.append((dst_st.st_mtime, row["id"], stored_mtime, stored_size))
    if progress_cb:
        progress_cb(total_rows, total_rows, "", "Checking timestamps")
    return updates, None


def move_folder(db, folder_id, destination, progress_cb=None, developed_dir="",
                merge=False, remote=None, reject_tracked_ancestor=False,
                allow_tracked_merge=False, destination_name="", verify_contents=False,
                pre_commit_check=None, thumb_cache_dir=None):
    """Move an entire folder (and subfolders) to a destination.

    The folder is placed inside the destination, preserving its name unless
    ``destination_name`` explicitly renames it. E.g., moving /local/birds to
    /nas/photos creates /nas/photos/birds by default.

    Args:
        db: Database instance
        folder_id: ID of the source folder
        destination: absolute path to parent destination directory. Ignored
            for a remote move (the destination comes from ``remote``).
        destination_name: optional new name for the folder at the destination.
            Must be one path component. Empty preserves the source name.
        verify_contents: compare every local destination file byte-for-byte
            before updating the catalog and deleting the source originals.
        pre_commit_check: optional callback that raises if the destination
            is no longer safe, after verification and before catalog changes.
        progress_cb: optional callback(current, total, filename)
        merge: when False (default), refuse to write into a destination
            that already exists — the safe all-or-nothing behavior. When
            True, merge/resume into the existing destination: a pre-copy scan
            refuses the merge if any same-name file differs in content;
            otherwise rsync (``--ignore-existing``) copies only the files
            missing at the destination and never overwrites one already there
            (this is how an interrupted move is resumed). Originals are deleted
            only after every source file is verified present at the
            destination. A failed merge never removes the destination, since it
            may hold the user's pre-existing files. Finder ``.DS_Store`` files
            are ignored on both sides: the copy skips them, the conflict and
            verification checks skip them, and the source's copy is discarded
            only when the whole source tree is removed after success — a
            failed merge leaves the source ``.DS_Store`` in place.
        developed_dir: optional path to the configured
            `darktable_output_dir`. When set, the folder's developed
            subdirectory — nested under a hash of its source path, see
            `export.developed_folder_key` — is rebased to match the new
            path after the move. Without this, exports silently fall
            back to RAW for every previously-developed photo in the
            moved folder.
        remote: optional dict to transfer over SSH instead of to a local
            path. Keys: ``host``, ``user``, ``port``, ``ssh_key``,
            ``bwlimit_kbps``, ``rsync_bin`` (a GNU rsync path), and the two
            destination *parents* — ``ssh_dest_base`` (the NAS-side
            filesystem path rsync writes to) and ``mount_dest_base`` (the
            local path, e.g. an SMB mount, where Vireo can read those same
            files afterward). The transfer and verification use the SSH
            path; the catalog is repointed at the mount path once the move
            succeeds, so the photos stay in the library and resolve whenever
            the NAS is mounted.
        reject_tracked_ancestor: when True, also reject a destination inside
            another tracked folder. Normal user-initiated moves can validly
            move into a tracked destination parent; local-processing archive
            commits opt into this stricter guard because they create a new
            top-level archive root and cannot be reparented under the
            existing catalog row.
        allow_tracked_merge: when True, a tracked destination (overlap or, with
            ``reject_tracked_ancestor``, ancestor) is no longer an error.
            Instead, after the verified file copy, the staged folder/photo rows
            are folded into the existing archive rows via
            ``db.merge_staged_tree_into_archive`` (new folders repointed,
            identical-filename photos dropped) rather than a path cascade. Only
            the local-processing archive commit opts in; manual and remote
            moves keep the default refusal. The result then carries ``merge``
            (the reconciliation counts) and ``merged_into_existing`` (the
            tracked archive path).
        thumb_cache_dir: optional path to the thumbnail cache directory. When
            a timestamp-losing transfer advances a row's ``file_mtime``, the
            thumbnail endpoint's freshness invariant (``cached_mtime >=
            file_mtime``) treats the existing cached thumbnail as stale and
            regenerates on first access. If this is set, the corresponding
            thumbnail files are ``os.utime``d alongside the DB update so the
            already-correct pixels stay served without a regeneration.

    Returns dict with keys: moved (int), errors (list of str). When the
    catalog has already been repointed at the new destination but deleting
    the source originals afterwards fails, an extra ``cleanup_error`` (str)
    is included — the archive is committed and ``errors`` stays empty, but
    callers should still surface the leftover originals to the user.
    """
    folder = db.conn.execute(
        "SELECT id, path, name FROM folders WHERE id = ?", (folder_id,)
    ).fetchone()
    if not folder:
        return {"moved": 0, "errors": ["Folder not found"]}

    src_path = folder["path"]
    # rstrip separators before basename() so a legacy row stored with a
    # trailing '/' or '\\' still yields the folder leaf; without it a
    # nameless folder row falls back to an empty landing_name here, and the
    # copy lands directly in the selected parent (or merges into it) even
    # though preflight — which uses the same rstrip in resolve_folder_dest
    # and the remote branch — approves ``<parent>/<source-leaf>``.
    folder_name = folder["name"] or os.path.basename(src_path.rstrip("/\\"))
    try:
        landing_name = normalize_destination_name(destination_name) or folder_name
    except ValueError as exc:
        return {"moved": 0, "errors": [str(exc)]}

    move = _FolderMove(
        db, folder_id, src_path, landing_name,
        progress_cb=progress_cb, developed_dir=developed_dir, merge=merge,
        remote=remote, reject_tracked_ancestor=reject_tracked_ancestor,
        allow_tracked_merge=allow_tracked_merge,
        verify_contents=verify_contents, pre_commit_check=pre_commit_check,
        thumb_cache_dir=thumb_cache_dir,
    )
    error = move.resolve_destination_views(destination)
    if error is not None:
        return error

    # Validation (overlap, tracked-folder, and the per-file content-conflict
    # scan a merge runs) can take a noticeable moment on a large tree, so name
    # the phase before it starts rather than leaving the bar blank.
    move.progress(0, 0, "", "Checking destination")
    for check in (move.refuse_source_overlap,
                  move.resolve_tracked_destination,
                  move.probe_destination,
                  move.refuse_content_conflict):
        error = check()
        if error is not None:
            return error

    log.info("%s folder %s -> %s",
             "Merging" if move.dest_exists else "Moving", src_path,
             move.rsync_target)

    for phase in (move.ensure_remote_parent,
                  move.copy_tree,
                  move.plan_mtime_corrections,
                  move.verify_copy):
        error = phase()
        if error is not None:
            return error

    move.update_catalog()
    move.apply_mtime_corrections()
    move.relocate_developed_dirs()
    move.remove_originals()
    move.progress(move.total_files, move.total_files, folder_name, "Done")
    return move.result()


def _fresh_copy_mismatch(src_path, transfer_dest, progress=None):
    """Describe how a fresh move's destination differs from its source.

    Fresh move into a destination we created: walk the source and check
    each file has a same-sized counterpart at the destination. Recount
    the source here rather than reusing the pre-copy `total_files` — if
    a file appeared in the source after that upfront count (and rsync
    didn't pick it up), a stale count could spuriously match a naive
    dst_count and the rmtree below would delete the never-copied file.

    A whole-tree file count alone is not sufficient: ``_plan_moved_file_mtimes``
    above stats every catalog photo sequentially and can run for minutes
    on a network mount, so a destination photo stat'd early in that pass
    could be truncated or replaced afterwards while the pass keeps working
    through the rest of the tree. Neither the pass's remaining iterations
    nor a count-only check would notice, and rmtree(src) would then delete
    the intact original. Per-file size verification is the last thing
    that touches the destination before the catalog update, closing the
    window opened by the planning pass.

    Symlinks are matched structurally: rsync -a (and the shutil fallback)
    preserves source symlinks as destination symlinks with the same
    target string; os.path.getsize would follow the link, so lstat sizes
    and readlink targets are compared instead.

    Returns the mismatch description, or None when the copy is complete.
    ``progress`` is called as in ``_first_missing_source_file``.
    """
    src_count = 0
    for root, _dirs, files in os.walk(src_path):
        rel = os.path.relpath(root, src_path)
        for fn in files:
            src_file = os.path.join(root, fn)
            rel_name = fn if rel == "." else os.path.join(rel, fn)
            if progress:
                progress(src_count, rel_name)
            src_count += 1
            dst_file = os.path.join(transfer_dest, rel_name)
            if not os.path.lexists(dst_file):
                return f"'{rel_name}' missing at destination"
            src_is_link = os.path.islink(src_file)
            dst_is_link = os.path.islink(dst_file)
            if src_is_link != dst_is_link:
                return (
                    f"'{rel_name}' type mismatch (symlink vs regular file) "
                    f"at destination")
            if src_is_link:
                if os.readlink(src_file) != os.readlink(dst_file):
                    return (
                        f"'{rel_name}' symlink target mismatch at destination")
                continue
            try:
                src_size = os.stat(src_file, follow_symlinks=False).st_size
                dst_size = os.stat(dst_file, follow_symlinks=False).st_size
            except OSError:
                return f"'{rel_name}' unreadable at destination"
            if src_size != dst_size:
                return (
                    f"'{rel_name}' size mismatch at destination "
                    f"(source={src_size}, dest={dst_size})")
    # Extras at destination — a leftover rsync temp file, or anything
    # else the source-driven walk above never looked for — would leave
    # the fresh destination in a state we do not fully understand.
    # Count both sides after the size check so a mismatch that a
    # per-source-file walk catches is not attributed to a stray extra.
    dst_count = sum(1 for _, _, f in os.walk(transfer_dest) for _ in f)
    if src_count != dst_count:
        return f"file count mismatch: source={src_count}, dest={dst_count}"
    if progress:
        progress(src_count, "")
    return None


class _FolderMove:
    """Run-wide state for one ``move_folder`` call.

    Each pre-commit phase returns the result dict to abort with, or None to
    continue; ``move_folder`` returns the first one it gets. The post-commit
    phases never abort.
    """

    def __init__(self, db, folder_id, src_path, landing_name, *, progress_cb,
                 developed_dir, merge, remote, reject_tracked_ancestor,
                 allow_tracked_merge, verify_contents, pre_commit_check,
                 thumb_cache_dir):
        self.db = db
        self.folder_id = folder_id
        self.src_path = src_path
        self.landing_name = landing_name
        self.progress_cb = progress_cb
        self.developed_dir = developed_dir
        self.merge = merge
        self.remote = remote
        self.reject_tracked_ancestor = reject_tracked_ancestor
        self.allow_tracked_merge = allow_tracked_merge
        self.verify_contents = verify_contents
        self.pre_commit_check = pre_commit_check
        self.thumb_cache_dir = thumb_cache_dir
        self.transfer_dest = None
        self.rsync_target = None
        self.catalog_path = None
        self.merge_into_tracked = None
        self.merge_reconcile_base = None
        self.dest_exists = False
        self.total_files = 0
        self.rsync_bin = None
        self.mtime_updates = []
        self.total_photos = 0
        self.merge_counts = None
        self.mtimes_refreshed = 0
        self.cleanup_error = None

    def progress(self, current, total, filename, phase):
        if self.progress_cb:
            self.progress_cb(current, total, filename, phase)

    def resolve_destination_views(self, destination):
        """Resolve the three destination views.

          transfer_dest — where rsync writes (NAS-side path for remote, local
            path otherwise); also the path used for existence/verify.
          rsync_target  — transfer_dest addressed for rsync (user@host:path
            remote, bare path local).
          catalog_path  — where the catalog points AFTER the move (the local
            mount path for remote, same as transfer_dest local). The local
            destination and catalog coincide; for remote they diverge because
            the NAS path isn't reachable through the local filesystem.
        """
        remote = self.remote
        landing_name = self.landing_name
        if remote:
            ssh_base = remote.get("ssh_dest_base") or ""
            # Same hazard as the mount-path check below, on the NAS side: a
            # relative ssh_dest_base like "Photos" would ship to rsync as
            # ``user@host:Photos/<folder>`` and resolve under the SSH user's
            # remote cwd — but the catalog gets repointed to the absolute
            # mount_dest_base, so a verified copy can live at a different
            # remote location than the path Vireo records before originals are
            # deleted. ``_coerce_remote_target`` already drops relative-path
            # entries at the config boundary; this is the defense-in-depth
            # check for callers (tests, direct use) that build a remote dict
            # themselves. POSIX-absolute (startswith "/") because the NAS is
            # POSIX — os.path.isabs would accept ``C:\foo`` on Windows.
            if not ssh_base.startswith("/"):
                return {"moved": 0, "errors": [
                    "Remote target needs an absolute remote (NAS) path before "
                    "moving files — otherwise rsync would write under the SSH "
                    "user's cwd, not where the catalog will point. Set the "
                    "remote path under Settings → Remote targets."
                ]}
            mount_base = remote.get("mount_dest_base") or ""
            # Without an absolute mount path, resolve_folder_dest below would
            # produce a relative catalog_path like 'Birds' — then after the SSH
            # copy succeeds and originals are deleted, the catalog row points at
            # a non-resolving location relative to the server cwd. The
            # /api/jobs/move-folder route also validates this, but move_folder is
            # called directly from tests and other code paths; checking here too
            # means the bug can't slip past whichever caller forgets.
            if not os.path.isabs(mount_base):
                return {"moved": 0, "errors": [
                    "Remote target needs an absolute local mount path before "
                    "moving files — otherwise the catalog would point at a "
                    "relative location after the move. Set the mount path under "
                    "Settings → Remote targets."
                ]}
            # The NAS side is POSIX, so the SSH dest must be joined with '/' even
            # when this code runs on Windows; os.path.join would produce a
            # backslash and rsync would treat it as a single path segment.
            self.transfer_dest = posixpath.join(remote["ssh_dest_base"], landing_name)
            # Join landing_name directly rather than routing it back through
            # resolve_folder_dest: that helper calls normalize_destination_name,
            # which would re-trim/reject a value we've already resolved. When the
            # user didn't request a rename, landing_name is the raw folder_name
            # (potentially with surrounding whitespace, or POSIX-legal ``:``/``\``
            # on Linux/macOS filesystems that allow them). Preflight preserves
            # those characters — the move job must too, or the copy lands at a
            # different path than preflight showed and the catalog repoints to
            # yet another (trimmed) path.
            self.catalog_path = os.path.join(mount_base, landing_name)
            self.rsync_target = rsync_dest_spec(remote, self.transfer_dest)
        else:
            self.transfer_dest = os.path.join(destination, landing_name)
            self.catalog_path = self.transfer_dest
            self.rsync_target = self.transfer_dest
        return None

    def refuse_source_overlap(self):
        """Refuse a destination that overlaps the source.

        Moving a folder into itself (or into one of its own descendants) would
        make the post-copy rmtree(src) delete the only copy of the files. This
        is especially dangerous for a merge, where a destination equal to the
        source passes verification trivially (every source file is already
        "there") before the delete wipes everything. See
        _destination_overlaps_source for the alias surface (symlinks, Windows
        case folding, case-insensitive POSIX).

        The NAS-side transfer_dest can't alias the local source tree for a
        remote move — but the LOCAL MOUNT PATH the catalog is repointed to
        (catalog_path) absolutely can if the source already lives on the same
        mount. e.g. src=/Volumes/Photography/trip with remote mount_path=
        /Volumes/Photography would copy a tree onto itself over SSH; the
        checksum verify passes (everything is "already there") and then the
        rmtree(src) deletes the only copy. So check the local-facing path:
        transfer_dest for local moves, catalog_path for remote.
        """
        overlap_src_check = (self.catalog_path if self.remote
                             else self.transfer_dest)
        if _destination_overlaps_source(self.src_path, overlap_src_check):
            return {"moved": 0, "errors": [
                f"Destination overlaps the source folder: {overlap_src_check}"
            ]}
        return None

    def resolve_tracked_destination(self):
        """Refuse moving into — or around — a destination Vireo already tracks.

        A tracked folder is refused regardless of whether that path currently
        exists on disk. A correct tracked-tree merge needs recursive
        folder/photo reconciliation we don't do here; a partial attempt would
        leave folders pointing at the deleted source path, or collide on the
        folders.path UNIQUE constraint when the source's children cascade onto
        a tracked descendant. Match the destination itself and anything below
        it. The cases this feature exists for — resuming an interrupted move,
        or moving into an untracked folder — never hit this.

        For remote, check against `catalog_path` (the local mount path the
        catalog is repointed to after the move) rather than `transfer_dest` (the
        NAS-side path, which isn't in the local catalog). Without this guard, a
        remote move into a mount path that overlaps an already-scanned folder
        would copy the whole tree over SSH and then hit folders.path UNIQUE on
        the post-move db.move_folder_path cascade.

        Comparison goes through _path_equal_or_descends so symlink aliases,
        Windows case folding, AND case-only aliases on case-insensitive POSIX
        (default macOS APFS) all collapse to the same tracked row — otherwise
        a destination reached via any of those would slip past and leave two
        folder rows managing the same on-disk tree.

        When ``allow_tracked_merge`` is set (the local-processing archive commit
        opts in), a tracked destination is NOT an error: instead we remember the
        tracked path in ``merge_into_tracked`` and, after the verified file copy,
        reconcile the catalog by folding the staged folder/photo rows into the
        existing archive rows rather than calling ``db.move_folder_path`` (which
        would collide on folders.path UNIQUE). Default (flag off) behaviour is
        byte-for-byte unchanged: both tracked-destination cases refuse the move.
        """
        db = self.db
        overlap_check_path = (self.catalog_path if self.remote
                              else self.transfer_dest)
        # ``merge_reconcile_base`` is the catalog path the staged tree is
        # reconciled ONTO. Distinct from ``merge_into_tracked`` (the user-facing
        # "existing archive" label): for an exact overlap the reconciliation
        # base must be the STORED tracked path, not ``catalog_path``, because
        # ``catalog_path`` may be an alias (symlink / case-only fold) of the
        # tracked folder. The files rsync to the same on-disk location either
        # way, but ``merge_staged_tree_into_archive`` does exact
        # ``WHERE path = ?`` catalog lookups that only match the row stored under
        # the tracked path — rebasing onto the alias would miss it and create a
        # second folder row for the same archive.
        tracked = _tracked_destination_overlap(
            db, self.folder_id, overlap_check_path)
        if tracked:
            # ``_tracked_destination_overlap`` returns any tracked row at-or-below
            # ``overlap_check_path``. The opt-in merge only covers the "into an
            # existing archive" case where the tracked row IS the destination;
            # a tracked row STRICTLY BELOW the destination is the "wrap a fresh
            # parent around an existing tracked subtree" case (e.g. /Photos/USA
            # tracked, destination /Photos). The reconciliation would rebase the
            # staged tree onto the wrapper path and leave the pre-existing tracked
            # descendant with unchanged parentage — two overlapping catalog
            # subtrees managing the same on-disk area. Refuse even when
            # ``allow_tracked_merge`` is set. Uses the same alias-folding surface
            # as the overlap probe (symlinks, Windows case-fold, case-insensitive
            # POSIX) so a case-only alias of the destination still counts as the
            # tracked row itself, not a wrapping parent.
            tracked_is_destination = _path_equal_or_descends(
                overlap_check_path, tracked["path"],
            )
            if not self.allow_tracked_merge or not tracked_is_destination:
                return {"moved": 0, "errors": [
                    f"Destination overlaps a folder Vireo already manages "
                    f"({tracked['path']}). Merging into or around a tracked folder "
                    f"isn't supported."
                ]}
            self.merge_into_tracked = tracked["path"]
            # Reconcile onto the STORED tracked path (not the possibly-aliased
            # ``catalog_path``) so the existing archive rows are found, not
            # duplicated. See the ``merge_reconcile_base`` note above.
            self.merge_reconcile_base = tracked["path"]
        if self.reject_tracked_ancestor and self.merge_into_tracked is None:
            ancestor = _tracked_destination_ancestor(
                db, self.folder_id, overlap_check_path)
            if ancestor:
                if not self.allow_tracked_merge:
                    return {"moved": 0, "errors": [
                        f"Destination is inside a folder Vireo already manages "
                        f"({ancestor['path']}). Pick an untracked archive destination."
                    ]}
                # Merge into the existing archive root that contains the
                # destination. The staged tree lands at its own resolved
                # catalog_path (inside the tracked ancestor); the reconciliation
                # rebases staged rows onto that path and leaves the ancestor's own
                # rows untouched. The user-facing "existing archive" base we
                # report is the managed-archive root (``ancestor["path"]``), not
                # the staged landing path inside it.
                #
                # The reconciliation base is the STORED ancestor's path with the
                # relative-below-ancestor suffix appended, NOT the user-entered
                # ``catalog_path`` — those two only agree when the ancestor probe
                # matched by pure string prefix. When it matched via a symlink,
                # Windows case-fold, or POSIX case-only alias (catalog stores
                # ``/Photos``, user selects ``/photos/NewShoot``),
                # ``catalog_path`` has an alias prefix that ``merge_staged_tree_
                # into_archive``'s exact ``WHERE path = ?`` parent lookups miss.
                # That would land the staged root with ``parent_id=NULL`` under an
                # alias-prefixed path, spawning a parallel row set outside the
                # managed archive tree. Fold the alias prefix to the stored form
                # here.
                self.merge_into_tracked = ancestor["path"]
                self.merge_reconcile_base = _rebase_under_stored_ancestor(
                    self.catalog_path, ancestor["path"])
        return None

    def probe_destination(self):
        remote = self.remote
        transfer_dest = self.transfer_dest
        if remote:
            probe = _remote_dir_exists(remote, transfer_dest)
            if probe is None:
                # Refuse rather than proceed as a fresh transfer: a transient SSH
                # failure on a real existing destination would otherwise omit
                # --ignore-existing and let rsync overwrite same-name files before
                # the post-transfer --checksum verify could preserve the originals.
                return {"moved": 0, "errors": [
                    f"Couldn't probe remote destination via SSH: "
                    f"{rsync_dest_spec(remote, transfer_dest)}. "
                    f"Refusing the move so a transient SSH error isn't confused "
                    f"with an absent destination."
                ]}
            self.dest_exists = probe
        else:
            self.dest_exists = os.path.exists(transfer_dest)
        if self.dest_exists and not self.merge:
            return {
                "moved": 0,
                "errors": [f"Destination already exists: {transfer_dest}"],
                "needs_merge": True,
            }
        return None

    def refuse_content_conflict(self):
        """Refuse if any same-name file already at the destination differs.

        Never overwrite or later delete the user's data over a real
        collision — only files that are byte-identical (a genuine resume)
        may be treated as already-moved. Both branches enforce the same
        contract: a content conflict cancels the move with NOTHING copied
        or deleted on either end. Finder ``.DS_Store`` files are ignored on
        both sides; see ``FINDER_METADATA_FILES``.
        """
        if not self.dest_exists:
            return None
        src_path = self.src_path
        remote = self.remote
        if remote:
            # The destination lives on the NAS, so the walk is delegated to
            # rsync over SSH: ``-an --existing --checksum`` inspects only
            # files that already exist at the receiver and reports any whose
            # bytes don't match. Without this pre-copy check, the actual
            # transfer (--ignore-existing + --partial-dir) would still copy
            # every MISSING source file before the post-transfer --checksum
            # verify could surface the conflict — leaving stray newly-copied
            # files orphaned on the NAS instead of cancelling cleanly. A
            # probe error is also treated as a refusal so a flaky link can't
            # downgrade the "nothing changed" guarantee.
            remote_rsync = remote.get("rsync_bin")
            if not remote_rsync:
                return {"moved": 0, "errors": [
                    "No usable GNU rsync binary is available for remote moves. "
                    "Install it or set the GNU rsync path in Settings."
                ]}
            conflict = _find_remote_content_conflict(
                remote_rsync, src_path, self.rsync_target, remote)
            if conflict is not None:
                name, detail = conflict
                if name == "__ERROR__":
                    return {"moved": 0, "errors": [
                        f"Pre-merge content check could not run ({detail}). "
                        f"Nothing was copied — re-run when the connection is "
                        f"stable."
                    ]}
                return {"moved": 0, "errors": [
                    f"Conflict: '{name}' already exists at the remote "
                    f"destination with different content. Nothing was copied "
                    f"or deleted."
                ]}
        else:
            conflict = _find_content_conflict(src_path, self.transfer_dest)
            if conflict is not None:
                return {"moved": 0, "errors": [
                    f"Conflict: '{conflict}' already exists at the destination "
                    f"with different content. Nothing was copied or deleted."
                ]}
        return None

    def ensure_remote_parent(self):
        """For a fresh remote move, ensure the destination's PARENT directory
        exists on the NAS.

        rsync creates the leaf folder itself but not its intermediate parents,
        so a configured subpath like ``USA/2026`` that has never been written
        before would fail with ``mkdir ... failed: No such file or directory``
        even though every preceding check passed. Skip on a merge — if the leaf
        exists, the parents must too. Skip when the parent is empty (the bare
        base "/", or a transfer_dest with no parent component) since mkdir-p
        there is meaningless.
        """
        if self.remote and not self.dest_exists:
            parent_dir = posixpath.dirname(self.transfer_dest)
            if parent_dir and parent_dir != "/":
                ok, detail = _remote_mkdir_p(self.remote, parent_dir)
                if not ok:
                    return {"moved": 0, "errors": [
                        f"Couldn't create the remote destination's parent "
                        f"directory '{parent_dir}' on the NAS: {detail}. "
                        f"Check permissions or pre-create the subpath."
                    ]}
        return None

    def copy_tree(self):
        src_path = self.src_path
        remote = self.remote
        dest_exists = self.dest_exists
        progress_cb = self.progress_cb
        # Count source files up front so the copy phase reports against a real
        # denominator from the first file. This count is the progress denominator
        # only — the fresh-move verification below deliberately recounts the
        # source at verify time rather than trusting this pre-copy number.
        total_files = sum(1 for _, _, files in os.walk(src_path) for _ in files)
        self.total_files = total_files
        self.progress(0, total_files, "", "Copying files")

        # Use rsync for a robust copy. A merge/resume uses --ignore-existing so
        # rsync only creates files absent at the destination and NEVER overwrites
        # a file already there: this resumes an interrupted move (missing files get
        # copied, already-copied ones are left alone) while guaranteeing a merge
        # cannot destroy pre-existing destination data. A local fresh move uses
        # --checksum for integrity; a remote fresh move skips it (the destination
        # is empty, and the post-transfer --checksum dry-run verifies integrity).
        # Any genuine same-name collision (an existing dest file that differs from
        # the source) is left untouched here and caught by the post-copy
        # verification below, which then refuses to delete the originals.
        #
        # Remote uses --partial-dir (instead of plain --partial) so a stalled or
        # cancelled transfer leaves the partial in `.rsync-partial/` rather than
        # at the destination filename. That keeps --ignore-existing honest: only
        # *complete* dest files are skipped, so the next run resumes the partial
        # from `.rsync-partial/` instead of treating it as already-moved (which
        # would then fail the --checksum verify forever, stranding the partial
        # until the user manually deletes it).
        # Prefer a discovered GNU rsync for local moves on POSIX. Finder-launched
        # macOS apps usually inherit a sparse PATH, so a bare ``rsync`` resolves
        # to Apple's legacy openrsync even when Homebrew GNU rsync is installed.
        # openrsync has been observed spinning after a transient SMB short read;
        # GNU rsync exits with a useful error instead. Windows rsync distributions
        # expect POSIX-style paths and can misread a native ``C:\...`` source as
        # remote-shell syntax, so retain the prior bare-name behavior there. Keep
        # the bare-name fallback on POSIX when GNU rsync is unavailable (and the
        # shutil fallback below when no rsync exists at all).
        rsync_bin = "rsync"
        if sys.platform != "win32":
            rsync_bin = resolve_rsync_bin() or rsync_bin
        extra_args = None
        if remote:
            rsync_bin = remote.get("rsync_bin")
            if not rsync_bin:
                return {"moved": 0, "errors": [
                    "No usable GNU rsync binary is available for remote moves. "
                    "Install it or set the GNU rsync path in Settings."
                ]}
            extra_args = [
                "-e", _ssh_rsh_string(remote),
                "--partial-dir=.rsync-partial",
            ]
            if remote.get("bwlimit_kbps"):
                extra_args.append(f"--bwlimit={int(remote['bwlimit_kbps'])}")
            rsync_flags = ["--ignore-existing"] if dest_exists else []
        else:
            rsync_flags = ["--ignore-existing"] if dest_exists else ["--checksum"]
        self.rsync_bin = rsync_bin
        # A merge deliberately skips Finder metadata (``.DS_Store``). Without the
        # exclude, rsync ``--ignore-existing`` would leave a same-name file at the
        # destination in place -- fine on its own -- but the post-copy verifier
        # would then compare bytes/size and refuse the delete, so the originals
        # would be preserved for a difference that is not a photo collision.
        # Excluding on the copy AND the checks keeps the source's copy in place
        # until the whole tree is removed after a successful merge; a failed
        # merge leaves it untouched with the rest of the source.
        exclude_ctx = (_rsync_finder_metadata_exclude_file(src_path)
                       if dest_exists else contextlib.nullcontext([]))
        with exclude_ctx as metadata_excludes:
            rsync_flags += metadata_excludes
            try:
                returncode, stderr, timed_out = _run_rsync_streamed(
                    src_path, self.rsync_target, rsync_flags, total_files,
                    progress_cb, rsync_bin=rsync_bin, extra_args=extra_args,
                )
            except FileNotFoundError:
                if remote:
                    # No shutil fallback over SSH — the binary path was resolved
                    # before the move started, so this means it vanished. Surface
                    # it plainly.
                    return {"moved": 0, "errors": [
                        f"GNU rsync not found at '{rsync_bin}'. Install GNU rsync "
                        f"or set its path in Settings."
                    ]}
                # Local rsync missing: fall back to shutil. skip_existing mirrors
                # --ignore-existing for a merge; a fresh move copies everything.
                try:
                    _copy_tree_with_progress(
                        src_path, self.catalog_path, dest_exists, total_files,
                        progress_cb,
                    )
                    returncode, stderr, timed_out = 0, "", False
                except Exception as exc:
                    log.warning("Copy fallback failed for %s", src_path, exc_info=True)
                    # Only remove a destination we created — never one that
                    # pre-existed (a merge target may hold the user's own files).
                    if not dest_exists:
                        shutil.rmtree(self.catalog_path, ignore_errors=True)
                    return {"moved": 0, "errors": [f"Copy failed: {exc}"]}

        if timed_out:
            mins = RSYNC_STALL_TIMEOUT // 60
            # rsync can emit the real cause (for example, the exact NAS file that
            # returned a short read) and then wedge instead of exiting.  Do not
            # throw that diagnostic away in favor of the generic watchdog text.
            # Bound it because stderr can contain one warning per source file.
            detail = stderr.strip()
            if len(detail) > 1000:
                detail = "…" + detail[-999:]
            reported = f" rsync reported: {detail}" if detail else ""
            return {"moved": 0, "errors": [
                f"rsync stalled — no progress for over {mins} minutes, so the "
                f"copy was stopped. Originals are untouched; re-run with "
                f"merge/resume to continue from where it left off.{reported}"
            ]}
        if returncode != 0:
            return {"moved": 0, "errors": [f"rsync failed: {stderr.strip()}"]}
        # rsync emits only transferred files. A successful merge also handled
        # the already-present files (and deliberately excluded Finder metadata),
        # so finish the phase's source-file count before entering verification.
        self.progress(total_files, total_files, "", "Copying files")
        return None

    def plan_mtime_corrections(self):
        """Read the destination's timestamps BEFORE verifying, not after.

        This pass stats every catalogued photo on both sides, which on a
        network mount is minutes of wall clock. Running it between
        verification and the ``rmtree`` below would push those minutes into
        the one window where a destination file going missing, or being
        replaced, is never noticed before the originals are deleted. Ordering
        it ahead of verification means the byte-level check remains the last
        thing that touches the destination, exactly as it was before this pass
        existed -- so no re-verification is owed, and the expensive one never
        runs twice.

        Safe to read this early: rsync has finished, so the destination is
        final, and the plan is read-only until it is applied after the cascade.

        Local destinations only. For a remote move ``transfer_dest`` lives on
        the far side of an SSH connection and cannot be stat'd, and reaching
        the same files back through ``catalog_path`` would walk a mount that
        need not even be mounted for the transfer to have succeeded -- for a
        rename rsync performed on the remote filesystem, where the timestamp
        is preserved anyway.
        """
        if not self.remote:
            self.mtime_updates, problem = _plan_moved_file_mtimes(
                self.db, self.src_path, self.transfer_dest,
                progress_cb=self.progress_cb,
            )
            if problem is not None:
                # Verification below would catch this too. Failing here just
                # spares the user a full byte-for-byte pass over a destination
                # already known to be incomplete.
                #
                # Clean up the same way the fresh-move count check does. A
                # destination this move created is ours to remove, and leaving a
                # partial tree behind would turn the documented all-or-nothing
                # retry into one that demands a merge. A destination that was
                # already there is never removed -- it may hold the user's own
                # files.
                if not self.dest_exists:
                    shutil.rmtree(self.transfer_dest, ignore_errors=True)
                return {"moved": 0, "errors": [
                    f"Verification failed: {problem}. Originals preserved."
                ]}
        return None

    def verify_copy(self):
        """Verify before deleting originals."""
        src_path = self.src_path
        transfer_dest = self.transfer_dest
        dest_exists = self.dest_exists
        if self.remote:
            # One rsync dry-run with no per-file output: no count to show.
            self.progress(0, 0, "", "Verifying copy")
        else:
            # Recounted rather than reusing the pre-copy ``total_files`` so the
            # bar's denominator is the tree the verifier actually walks.
            verify_total = sum(1 for _, _, files in os.walk(src_path)
                               for _ in files)

            def report(checked, name):
                self.progress(checked, max(verify_total, checked), name,
                              "Verifying copy")
            report(0, "")
        if self.remote:
            # The local filesystem can't be walked to confirm a remote copy, so
            # run a --checksum dry-run over SSH: any file it would still transfer
            # is missing or differs at the destination. Covers both fresh and
            # merge moves, and is the safety backstop replacing the local
            # content-conflict and file-count checks.
            verify = _remote_verify_complete(self.rsync_bin, src_path,
                                             self.rsync_target, self.remote,
                                             is_merge=dest_exists)
            if verify is not None:
                name, detail = verify
                if name == "__ERROR__":
                    return {"moved": 0, "errors": [
                        f"Verification could not be completed ({detail}). "
                        f"Originals preserved."
                    ]}
                return {"moved": 0, "errors": [
                    f"Verification failed: '{name}' is missing or differs at the "
                    f"destination. Originals preserved."
                ]}
        elif dest_exists or self.verify_contents:
            # Merge: the destination may legitimately hold extra unrelated
            # files (and leftover temp files from an interrupted run), so a
            # count comparison is meaningless. Instead require that every
            # source file is present at the destination with a matching size.
            missing = _first_missing_source_file(
                src_path, transfer_dest,
                verify_contents=self.verify_contents, is_merge=dest_exists,
                progress=report)
            if missing is not None:
                return {"moved": 0, "errors": [
                    f"Verification failed: '{missing}' missing, size mismatch, "
                    f"or symlinked at destination. Originals preserved."
                ]}
        else:
            verify_error = _fresh_copy_mismatch(src_path, transfer_dest,
                                                progress=report)
            if verify_error is not None:
                shutil.rmtree(transfer_dest, ignore_errors=True)
                return {"moved": 0, "errors": [
                    f"Verification failed: {verify_error}. Originals preserved."
                ]}
        return None

    def update_catalog(self):
        """Update DB first: cascade folder paths.

        Safer — if rmtree fails, the old folder becomes an orphan on disk
        rather than the DB pointing to deleted paths. Unless the caller opted
        into merging (``merge_into_tracked``), a merge into an already-tracked
        destination is refused above, so catalog_path is never a different
        existing folder row in the cascade branch and that cascade (root + all
        descendants) cannot collide with folders.path UNIQUE. When merging into
        a tracked archive we instead reconcile the staged rows into the
        existing archive rows below. For a remote move catalog_path is the
        local mount path, so the catalog keeps resolving to the photos whenever
        the NAS is mounted.
        """
        db = self.db
        src_path = self.src_path
        # Count photos for progress
        all_photos = db.conn.execute(
            """SELECT p.id FROM photos p
               JOIN folders f ON f.id = p.folder_id
               WHERE f.path = ? OR f.path LIKE ?""",
            (src_path, src_path + "/%"),
        ).fetchall()
        self.total_photos = len(all_photos)

        # One database transaction: no per-item count to show.
        self.progress(0, 0, "", "Updating catalog")
        if self.pre_commit_check:
            self.pre_commit_check()
        if self.merge_into_tracked is not None:
            # Destination is a tracked archive and the caller opted into merging:
            # fold the staged folder/photo rows into the existing archive rows
            # instead of a path cascade (which would collide on folders.path).
            # ``merge_reconcile_base`` is the STORED tracked path for an exact
            # overlap (so alias/case-fold destinations still match the existing
            # rows) and ``catalog_path`` for the ancestor case; see where it is set.
            self.merge_counts = db.merge_staged_tree_into_archive(
                self.folder_id, self.merge_reconcile_base)
        else:
            db.move_folder_path(self.folder_id, self.catalog_path,
                                new_name=self.landing_name)
        db.update_folder_counts()

    def apply_mtime_corrections(self):
        """Only now that the rows live at the destination.

        Guarded on the values the plan was built from, so a concurrent scan
        that committed a fresh stat of its own meanwhile wins instead of being
        overwritten from a stale snapshot -- and an id the merge dropped as an
        already-present collision simply matches nothing.
        """
        db = self.db
        mtime_updates = self.mtime_updates
        if not mtime_updates:
            return
        # Carry the mtime-pinned working-copy markers along with the
        # correction. Both record "the ``file_mtime`` this decision was made
        # against": ``working_copy_evicted_mtime`` marks a rendition the
        # quota deliberately dropped (scanner's backfill clause treats
        # ``!= file_mtime`` as "the file changed, redo it"), and
        # ``working_copy_failed_mtime`` marks one whose extraction failed
        # (``render_source`` retries as soon as the two differ). Re-stamping
        # ``file_mtime`` alone would silently invalidate both, and every
        # moved folder would re-read its RAWs over the NAS to regenerate
        # renditions that were dropped on purpose, or to retry extractions
        # that will fail exactly as before.
        #
        # Only a marker pinned to the timestamp being corrected moves. The
        # bytes are unchanged, so those decisions still hold; anything
        # recorded against a different timestamp -- including the ``-1``
        # sentinel used when a row had no mtime at all -- was made against a
        # state this correction knows nothing about, and is left alone.
        cursor = db.conn.executemany(
            "UPDATE photos SET file_mtime = ?,"
            " working_copy_evicted_mtime = CASE"
            "   WHEN working_copy_evicted_mtime IS ? THEN ?"
            "   ELSE working_copy_evicted_mtime END,"
            " working_copy_failed_mtime = CASE"
            "   WHEN working_copy_failed_mtime IS ? THEN ?"
            "   ELSE working_copy_failed_mtime END"
            " WHERE id = ? AND file_mtime IS ? AND file_size IS ?",
            [(fresh, stale, fresh, stale, fresh, photo_id, stale, size)
             for fresh, photo_id, stale, size in mtime_updates],
        )
        self.mtimes_refreshed = cursor.rowcount
        # Same reasoning as the working-copy markers, one table over.
        # ``offline_originals.source_mtime`` records the ``file_mtime`` its
        # cached copy was taken from, and ``offline_cache`` treats a
        # mismatch as "the original changed" -- so correcting ``file_mtime``
        # alone would mark every cached original stale and have the next
        # cache preparation re-copy bytes that never changed. Only a row
        # pinned to the timestamp being corrected, for a file of the same
        # size, moves with it.
        db.conn.executemany(
            "UPDATE offline_originals SET source_mtime = ?"
            " WHERE photo_id = ? AND source_mtime IS ? AND source_size IS ?",
            mtime_updates,
        )
        db.conn.commit()
        log.info(
            "Re-stamped file_mtime for %d photo(s) under %s from the copy "
            "at %s -- the transfer did not carry their timestamps across",
            self.mtimes_refreshed, self.src_path, self.catalog_path,
        )
        if self.thumb_cache_dir and mtime_updates:
            self._align_thumbnail_mtimes()

    def _align_thumbnail_mtimes(self):
        """Touch each cached thumbnail to its photo's corrected timestamp.

        The thumbnail endpoint gates cache freshness on ``cached_mtime >=
        photos.file_mtime``, and ``generate_thumbnail`` pegs a rendered
        thumbnail's file mtime to the source ``file_mtime`` it was made
        from. Advancing ``file_mtime`` alone would leave every cached
        thumbnail in the moved folder pinned below the new value: the next
        fetch would treat it as stale and regenerate, even though the
        pixels still describe the same unchanged bytes. Touch each
        thumbnail file to the corrected timestamp so the invariant
        continues to hold. Best-effort: a failure to touch is non-fatal --
        the worst case is a one-shot regeneration on next access, exactly
        the pre-fix behavior.

        Everything here runs AFTER the catalog commit above, so it is
        wrapped whole: an exception escaping at this point would leave the
        catalog repointed at the destination, the originals still sitting
        at the source, and the move reported as failed. Nothing about
        thumbnail freshness is worth that half-state, so any failure is
        logged and swallowed -- the cost is one regeneration on next
        access, which is exactly the behaviour without this block.
        """
        db = self.db
        try:
            fresh_by_id = {photo_id: fresh
                           for fresh, photo_id, _, _ in self.mtime_updates}
            # Chunked: a folder of a few thousand photos would otherwise
            # bind one variable per id and trip SQLite's
            # SQLITE_MAX_VARIABLE_NUMBER (999 on older builds) -- and the
            # transfer this exists for moved 1099 in one go.
            thumb_rows = []
            for chunk in _chunks(list(fresh_by_id)):
                placeholders = ",".join(["?"] * len(chunk))
                thumb_rows.extend(db.conn.execute(
                    f"SELECT id, thumb_path FROM photos"
                    f" WHERE id IN ({placeholders})"
                    f" AND thumb_path IS NOT NULL",
                    chunk,
                ).fetchall())
            for row in thumb_rows:
                fresh = fresh_by_id.get(row["id"])
                if fresh is None:
                    continue
                thumb_file = os.path.join(
                    self.thumb_cache_dir, row["thumb_path"])
                try:
                    os.utime(thumb_file, (fresh, fresh))
                except OSError:
                    log.debug(
                        "Could not align thumbnail mtime for photo %s "
                        "at %s", row["id"], thumb_file, exc_info=True,
                    )
        except Exception:
            log.exception(
                "Could not align thumbnail mtimes under %s after the "
                "move; they will regenerate on next access", self.src_path,
            )

    def relocate_developed_dirs(self):
        """Rebase any developed-output subdirs nested under the configured
        darktable_output_dir.

        `developed_folder_key` hashes the folder's path, so the DB update above
        just invalidated the old subdir's implicit key — rename it on disk to
        match the new path, and cascade to any descendant folders whose paths
        also shifted.

        On the merge path the catalog was reparented onto
        ``merge_reconcile_base``, not ``catalog_path`` — those two only
        differ when the tracked destination was reached via a symlink or
        case-alias (see where ``merge_reconcile_base`` is set), but when they
        do differ the developed-dir key is derived from the STORED path, not
        the alias. Relocating from ``src_path`` to the aliased
        ``catalog_path`` would move renders under the alias-path hash and
        exports (which read the catalog's stored path) would look under the
        stored-path hash, miss them, and fall back to RAW. Use the same
        reconciled base the catalog uses.
        """
        src_path = self.src_path
        developed_dir = self.developed_dir
        developed_base = (self.merge_reconcile_base
                          if self.merge_into_tracked is not None
                          else self.catalog_path)
        if developed_dir:
            from export import relocate_developed_dir
            relocate_developed_dir(developed_dir, src_path, developed_base)
            # SQL LIKE treats `_` and `%` (and the escape char) as wildcards,
            # all of which are valid POSIX path characters. Without a strict
            # prefix guard, an unrelated folder like `/dXst/birds/fake` would
            # match a pattern like `/d_st/birds/%` and feed a bogus computed
            # old_path into relocate_developed_dir, mis-rebasing the wrong
            # developed subdir. Filter results by a literal prefix check.
            descendant_rows = self.db.conn.execute(
                "SELECT path FROM folders WHERE path LIKE ?",
                (developed_base + "/%",),
            ).fetchall()
            prefix = developed_base + "/"
            for row in descendant_rows:
                new_child = row["path"]
                if not new_child.startswith(prefix):
                    continue
                old_child = src_path + new_child[len(developed_base):]
                relocate_developed_dir(developed_dir, old_child, new_child)

    def remove_originals(self):
        """Delete originals.

        The catalog already points at the new destination, so anything that
        goes wrong from here is post-commit: the archive is already published.
        Record a ``cleanup_error`` so the caller can warn the user about
        leftover originals without misreporting the move as failed (which
        would also leave the archive's tracked row in place while telling the
        user their data is still in staging).
        """
        src_path = self.src_path
        # A single rmtree: no per-item count to show.
        self.progress(0, 0, "", "Removing originals")
        log.info("Verification passed, deleting originals: %s", src_path)
        try:
            shutil.rmtree(src_path)
        except OSError as e:
            log.exception("Post-commit cleanup of %s failed", src_path)
            self.cleanup_error = str(e)

    def result(self):
        merge_counts = self.merge_counts
        result = {"moved": self.total_photos, "errors": [],
                  "mtimes_refreshed": self.mtimes_refreshed}
        if self.merge_into_tracked is not None:
            # ``dropped_photo_ids`` is a cleanup handle for the caller (thumbnails,
            # previews, offline copies of the deleted staged photos), not a
            # user-facing count. Lift it off ``merge_counts`` so ``result["merge"]``
            # stays a stable dict of display numbers that gets serialized straight
            # into the archive-stage summary/API payload.
            dropped = merge_counts.pop("dropped_photo_ids", None) or []
            # ``preserved_edit_count`` is likewise a caller-facing signal (the
            # NAS transfer's residual check adds it to "still need a sync"), not
            # a user-facing display number, so lift it off ``merge_counts`` too.
            # ``preserved_off_staging_identities`` is the subset the caller adds
            # to residual -- see ``merge_staged_tree_into_archive`` for why the
            # full count would double-report the phantom/intra-staged remaps.
            # Reported as identities (not a raw rowcount) so the caller can
            # filter out rows the pre-transfer drain already classified as
            # undeliverable and rows in sibling workspaces this sync would not
            # have written anyway.
            preserved_edits = merge_counts.pop("preserved_edit_count", 0) or 0
            preserved_off_staging_identities = merge_counts.pop(
                "preserved_off_staging_identities", None) or []
            result["merge"] = merge_counts
            result["merged_into_existing"] = self.merge_into_tracked
            # On the merge path ``total_photos`` counts every staged source photo,
            # including identical ones that were dropped as ``already_present``.
            # Report ``moved`` as the photos actually added to the archive.
            result["moved"] = merge_counts["new_photos"]
            if dropped:
                result["dropped_photo_ids"] = dropped
            if preserved_edits:
                result["preserved_edit_count"] = preserved_edits
            if preserved_off_staging_identities:
                result["preserved_off_staging_identities"] = (
                    preserved_off_staging_identities)
        if self.cleanup_error is not None:
            result["cleanup_error"] = self.cleanup_error
        return result
