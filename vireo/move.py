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

try:
    from .proc import no_window_kwargs
except ImportError:
    from proc import no_window_kwargs

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
    """Return the XMP sidecar path for a file, or None if it doesn't exist."""
    xmp = os.path.splitext(filepath)[0] + ".xmp"
    return xmp if os.path.isfile(xmp) else None


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
    """Copy a single file and verify size matches. Returns True on success."""
    shutil.copy2(src, dst)
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
            "found for the transfer. Install GNU rsync for your platform or "
            "set its path under Settings → Paths.")
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


def _remote_verify_complete(rsync_bin, src_path, rsync_target, remote):
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
    """
    cmd = [rsync_bin, "-an", "--checksum", "--out-format=%n",
           "-e", _ssh_rsh_string(remote),
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
        return ("__ERROR__", proc.stderr.strip() or f"rsync exit {proc.returncode}")
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
                        src_specs_dest_is_dir=True):
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
    rsync — which the job's cancel path can't reach, since it never sees the
    subprocess — still gets reaped. The clock resets on every transferred
    file and on stderr activity, so only true silence trips it.

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
        # the window elapses, without busy-waiting.
        while not done.wait(min(stall_timeout, 30)):
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


def _find_content_conflict(src_path, dest_path):
    """Return the relative path of the first source file that ALSO exists at
    dest_path but with different content, or None. Run before a merge copies
    anything: a same-name destination file is only safe to treat as "already
    there" if its bytes match the source. A size match alone is not enough —
    filecmp with shallow=False compares contents — so we never overwrite or
    later delete the source over a genuinely different destination file."""
    for root, _, files in os.walk(src_path):
        rel = os.path.relpath(root, src_path)
        for fn in files:
            src_file = os.path.join(root, fn)
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
    """
    cmd = [rsync_bin, "-an", "--existing", "--checksum",
           "--out-format=%n",
           "-e", _ssh_rsh_string(remote),
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
        return ("__ERROR__", proc.stderr.strip() or f"rsync exit {proc.returncode}")
    for line in proc.stdout.splitlines():
        name = line.rstrip("\n")
        if not name or name.endswith("/"):
            continue  # directory entry, not a file rsync would transfer
        return (name, None)
    return None


def _first_missing_source_file(src_path, dest_path):
    """Return the relative path of the first source file absent (or
    size-mismatched, or a symlink) at dest_path, or None if every source
    file is present and matches. Used to verify a merge before deleting
    originals.

    A symlinked destination entry is treated as missing: `os.path.isfile` /
    `os.path.getsize` follow the link, so a symlink pointing back into the
    source tree (directly, or via a symlinked parent directory) would pass
    a size compare even though no independent copy exists at the
    destination — and the post-copy `shutil.rmtree(src_path)` would then
    destroy the only copy. `lexists` lets a broken symlink count as
    missing instead of crashing later checks; `islink` catches the direct
    case; `samefile` catches the symlinked-parent case where `src_file`
    and `dst_file` resolve to the same inode.
    """
    for root, _, files in os.walk(src_path):
        rel = os.path.relpath(root, src_path)
        for fn in files:
            src_file = os.path.join(root, fn)
            rel_name = fn if rel == "." else os.path.join(rel, fn)
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


def move_photos(db, photo_ids, destination, progress_cb=None):
    """Move individual photos to a destination directory.

    Args:
        db: Database instance
        photo_ids: list of photo IDs to move
        destination: absolute path to target directory
        progress_cb: optional callback(current, total, filename)

    Returns dict with keys: moved (int), errors (list of str)
    """
    os.makedirs(destination, exist_ok=True)
    total = len(photo_ids)
    moved = 0
    errors = []

    # Ensure destination folder record exists (workspace link deferred until first successful move)
    dest_row = db.conn.execute("SELECT id FROM folders WHERE path = ?", (destination,)).fetchone()
    if dest_row:
        dest_folder_id = dest_row["id"]
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
            dest_folder_id = cur.lastrowid
        else:
            dest_folder_id = db.conn.execute(
                "SELECT id FROM folders WHERE path = ?", (destination,)
            ).fetchone()["id"]
    workspace_linked = False

    photos_map = db.get_photos_by_ids(photo_ids)

    try:
        for i, pid in enumerate(photo_ids):
            photo = photos_map.get(pid)
            if not photo:
                errors.append(f"Photo {pid} not found in database")
                continue

            folder_row = db.conn.execute(
                "SELECT path FROM folders WHERE id = ?", (photo["folder_id"],)
            ).fetchone()
            src_dir = folder_row["path"]
            src_file = os.path.join(src_dir, photo["filename"])

            if not os.path.isfile(src_file):
                log.warning("Move skipped for %s: source file missing", photo["filename"])
                errors.append(f"{photo['filename']}: source file missing")
                continue

            dst_file = os.path.join(destination, photo["filename"])
            if os.path.exists(dst_file):
                log.warning("Move skipped for %s: already exists at destination", photo["filename"])
                errors.append(f"{photo['filename']}: already exists at destination")
                continue

            # Gather companion files
            companions = _companion_files(photo, src_dir)

            # Check companion collisions
            comp_collision = False
            for comp in companions:
                if os.path.exists(os.path.join(destination, comp)):
                    errors.append(f"{comp}: companion file already exists at destination")
                    comp_collision = True
                    break
            if comp_collision:
                continue

            # Copy main file
            if not _copy_and_verify(src_file, dst_file):
                log.warning("Move skipped for %s: verification failed after copy", photo["filename"])
                errors.append(f"{photo['filename']}: verification failed after copy")
                continue

            # Copy companions
            comp_ok = True
            copied_companions = []
            for comp in companions:
                comp_src = os.path.join(src_dir, comp)
                comp_dst = os.path.join(destination, comp)
                if not _copy_and_verify(comp_src, comp_dst):
                    errors.append(f"{comp}: companion verification failed")
                    # Clean up what we copied
                    os.remove(dst_file)
                    for cc in copied_companions:
                        os.remove(os.path.join(destination, cc))
                    comp_ok = False
                    break
                copied_companions.append(comp)

            if not comp_ok:
                continue

            # Verification passed — link destination folder to workspace on first success
            if not workspace_linked and db._active_workspace_id is not None:
                db.add_workspace_folder(db._active_workspace_id, dest_folder_id)
                workspace_linked = True

            # Update DB before deleting originals
            # This ensures a crash leaves duplicates (safe) rather than orphans
            db.conn.execute(
                "UPDATE photos SET folder_id = ? WHERE id = ?",
                (dest_folder_id, pid),
            )
            db.conn.commit()

            # Now safe to delete originals
            os.remove(src_file)
            for comp in companions:
                comp_src = os.path.join(src_dir, comp)
                if os.path.isfile(comp_src):
                    os.remove(comp_src)

            moved += 1

            if progress_cb:
                progress_cb(i + 1, total, photo["filename"])
    finally:
        # Always update folder counts so they stay consistent even if an
        # exception interrupts the move loop after some photos were committed.
        if moved > 0:
            db.update_folder_counts()

    return {"moved": moved, "errors": errors, "destination_folder_id": dest_folder_id}


def move_folder(db, folder_id, destination, progress_cb=None, developed_dir="",
                merge=False, remote=None, reject_tracked_ancestor=False,
                allow_tracked_merge=False, destination_name=""):
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
            may hold the user's pre-existing files.
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
    folder_name = folder["name"] or os.path.basename(src_path)
    try:
        landing_name = normalize_destination_name(destination_name) or folder_name
    except ValueError as exc:
        return {"moved": 0, "errors": [str(exc)]}

    # Three destination views:
    #   transfer_dest — where rsync writes (NAS-side path for remote, local
    #     path otherwise); also the path used for existence/verify.
    #   rsync_target  — transfer_dest addressed for rsync (user@host:path
    #     remote, bare path local).
    #   catalog_path  — where the catalog points AFTER the move (the local
    #     mount path for remote, same as transfer_dest local). The local
    #     destination and catalog coincide; for remote they diverge because
    #     the NAS path isn't reachable through the local filesystem.
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
        transfer_dest = posixpath.join(remote["ssh_dest_base"], landing_name)
        # Join landing_name directly rather than routing it back through
        # resolve_folder_dest: that helper calls normalize_destination_name,
        # which would re-trim/reject a value we've already resolved. When the
        # user didn't request a rename, landing_name is the raw folder_name
        # (potentially with surrounding whitespace, or POSIX-legal ``:``/``\``
        # on Linux/macOS filesystems that allow them). Preflight preserves
        # those characters — the move job must too, or the copy lands at a
        # different path than preflight showed and the catalog repoints to
        # yet another (trimmed) path.
        catalog_path = os.path.join(mount_base, landing_name)
        rsync_target = rsync_dest_spec(remote, transfer_dest)
    else:
        transfer_dest = os.path.join(destination, landing_name)
        catalog_path = transfer_dest
        rsync_target = transfer_dest

    # Validation (overlap, tracked-folder, and the per-file content-conflict
    # scan a merge runs) can take a noticeable moment on a large tree, so name
    # the phase before it starts rather than leaving the bar blank.
    if progress_cb:
        progress_cb(0, 0, "", "Checking destination")

    # Refuse a destination that overlaps the source. Moving a folder into
    # itself (or into one of its own descendants) would make the post-copy
    # rmtree(src) delete the only copy of the files. This is especially
    # dangerous for a merge, where a destination equal to the source passes
    # verification trivially (every source file is already "there") before the
    # delete wipes everything. See _destination_overlaps_source for the alias
    # surface (symlinks, Windows case folding, case-insensitive POSIX).
    #
    # The NAS-side transfer_dest can't alias the local source tree for a
    # remote move — but the LOCAL MOUNT PATH the catalog is repointed to
    # (catalog_path) absolutely can if the source already lives on the same
    # mount. e.g. src=/Volumes/Photography/trip with remote mount_path=
    # /Volumes/Photography would copy a tree onto itself over SSH; the
    # checksum verify passes (everything is "already there") and then the
    # rmtree(src) deletes the only copy. So check the local-facing path:
    # transfer_dest for local moves, catalog_path for remote.
    overlap_src_check = catalog_path if remote else transfer_dest
    if _destination_overlaps_source(src_path, overlap_src_check):
        return {"moved": 0, "errors": [
            f"Destination overlaps the source folder: {overlap_src_check}"
        ]}

    # Refuse moving into — or around — a destination Vireo already tracks as a
    # folder, regardless of whether that path currently exists on disk. A
    # correct tracked-tree merge needs recursive folder/photo reconciliation we
    # don't do here; a partial attempt would leave folders pointing at the
    # deleted source path, or collide on the folders.path UNIQUE constraint
    # when the source's children cascade onto a tracked descendant. Match the
    # destination itself and anything below it. The cases this feature exists
    # for — resuming an interrupted move, or moving into an untracked folder —
    # never hit this.
    #
    # For remote, check against `catalog_path` (the local mount path the
    # catalog is repointed to after the move) rather than `transfer_dest` (the
    # NAS-side path, which isn't in the local catalog). Without this guard, a
    # remote move into a mount path that overlaps an already-scanned folder
    # would copy the whole tree over SSH and then hit folders.path UNIQUE on
    # the post-move db.move_folder_path cascade.
    #
    # Comparison goes through _path_equal_or_descends so symlink aliases,
    # Windows case folding, AND case-only aliases on case-insensitive POSIX
    # (default macOS APFS) all collapse to the same tracked row — otherwise
    # a destination reached via any of those would slip past and leave two
    # folder rows managing the same on-disk tree.
    #
    # When ``allow_tracked_merge`` is set (the local-processing archive commit
    # opts in), a tracked destination is NOT an error: instead we remember the
    # tracked path in ``merge_into_tracked`` and, after the verified file copy,
    # reconcile the catalog by folding the staged folder/photo rows into the
    # existing archive rows rather than calling ``db.move_folder_path`` (which
    # would collide on folders.path UNIQUE). Default (flag off) behaviour is
    # byte-for-byte unchanged: both tracked-destination cases refuse the move.
    overlap_check_path = catalog_path if remote else transfer_dest
    merge_into_tracked = None
    # The catalog path the staged tree is reconciled ONTO. Distinct from
    # ``merge_into_tracked`` (the user-facing "existing archive" label): for an
    # exact overlap the reconciliation base must be the STORED tracked path, not
    # ``catalog_path``, because ``catalog_path`` may be an alias (symlink /
    # case-only fold) of the tracked folder. The files rsync to the same on-disk
    # location either way, but ``merge_staged_tree_into_archive`` does exact
    # ``WHERE path = ?`` catalog lookups that only match the row stored under the
    # tracked path — rebasing onto the alias would miss it and create a second
    # folder row for the same archive.
    merge_reconcile_base = None
    tracked = _tracked_destination_overlap(db, folder_id, overlap_check_path)
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
        if not allow_tracked_merge or not tracked_is_destination:
            return {"moved": 0, "errors": [
                f"Destination overlaps a folder Vireo already manages "
                f"({tracked['path']}). Merging into or around a tracked folder "
                f"isn't supported."
            ]}
        merge_into_tracked = tracked["path"]
        # Reconcile onto the STORED tracked path (not the possibly-aliased
        # ``catalog_path``) so the existing archive rows are found, not
        # duplicated. See the ``merge_reconcile_base`` note above.
        merge_reconcile_base = tracked["path"]
    if reject_tracked_ancestor and merge_into_tracked is None:
        ancestor = _tracked_destination_ancestor(db, folder_id, overlap_check_path)
        if ancestor:
            if not allow_tracked_merge:
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
            merge_into_tracked = ancestor["path"]
            merge_reconcile_base = _rebase_under_stored_ancestor(
                catalog_path, ancestor["path"])

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
        dest_exists = probe
    else:
        dest_exists = os.path.exists(transfer_dest)
    if dest_exists and not merge:
        return {
            "moved": 0,
            "errors": [f"Destination already exists: {transfer_dest}"],
            "needs_merge": True,
        }

    if dest_exists:
        # Refuse if any same-name file already at the destination differs in
        # content. Never overwrite or later delete the user's data over a real
        # collision — only files that are byte-identical (a genuine resume)
        # may be treated as already-moved. Both branches enforce the same
        # contract: a content conflict cancels the move with NOTHING copied
        # or deleted on either end.
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
                remote_rsync, src_path, rsync_target, remote)
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
            conflict = _find_content_conflict(src_path, transfer_dest)
            if conflict is not None:
                return {"moved": 0, "errors": [
                    f"Conflict: '{conflict}' already exists at the destination "
                    f"with different content. Nothing was copied or deleted."
                ]}

    log.info("%s folder %s -> %s",
             "Merging" if dest_exists else "Moving", src_path, rsync_target)

    # For a fresh remote move, ensure the destination's PARENT directory
    # exists on the NAS. rsync creates the leaf folder itself but not its
    # intermediate parents, so a configured subpath like ``USA/2026`` that
    # has never been written before would fail with ``mkdir ... failed: No
    # such file or directory`` even though every preceding check passed.
    # Skip on a merge — if the leaf exists, the parents must too. Skip when
    # the parent is empty (the bare base "/", or a transfer_dest with no
    # parent component) since mkdir-p there is meaningless.
    if remote and not dest_exists:
        parent_dir = posixpath.dirname(transfer_dest)
        if parent_dir and parent_dir != "/":
            ok, detail = _remote_mkdir_p(remote, parent_dir)
            if not ok:
                return {"moved": 0, "errors": [
                    f"Couldn't create the remote destination's parent "
                    f"directory '{parent_dir}' on the NAS: {detail}. "
                    f"Check permissions or pre-create the subpath."
                ]}

    # Count source files up front so the copy phase reports against a real
    # denominator from the first file. This count is the progress denominator
    # only — the fresh-move verification below deliberately recounts the
    # source at verify time rather than trusting this pre-copy number.
    total_files = sum(1 for _, _, files in os.walk(src_path) for _ in files)
    if progress_cb:
        progress_cb(0, total_files, "", "Copying files")

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
    rsync_bin = "rsync"
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

    try:
        returncode, stderr, timed_out = _run_rsync_streamed(
            src_path, rsync_target, rsync_flags, total_files, progress_cb,
            rsync_bin=rsync_bin, extra_args=extra_args,
        )
    except FileNotFoundError:
        if remote:
            # No shutil fallback over SSH — the binary path was resolved before
            # the move started, so this means it vanished. Surface it plainly.
            return {"moved": 0, "errors": [
                f"GNU rsync not found at '{rsync_bin}'. Install GNU rsync or "
                f"set its path in Settings."
            ]}
        # Local rsync missing: fall back to shutil. skip_existing mirrors
        # --ignore-existing for a merge; a fresh move copies everything.
        try:
            _copy_tree_with_progress(
                src_path, catalog_path, dest_exists, total_files, progress_cb,
            )
            returncode, stderr, timed_out = 0, "", False
        except Exception as exc:
            # Only remove a destination we created — never one that
            # pre-existed (a merge target may hold the user's own files).
            if not dest_exists:
                shutil.rmtree(catalog_path, ignore_errors=True)
            return {"moved": 0, "errors": [f"Copy failed: {exc}"]}

    if timed_out:
        mins = RSYNC_STALL_TIMEOUT // 60
        return {"moved": 0, "errors": [
            f"rsync stalled — no progress for over {mins} minutes, so the "
            f"copy was stopped. Originals are untouched; re-run with "
            f"merge/resume to continue from where it left off."
        ]}
    if returncode != 0:
        return {"moved": 0, "errors": [f"rsync failed: {stderr.strip()}"]}

    # Verify before deleting originals.
    if progress_cb:
        progress_cb(total_files, total_files, "", "Verifying copy")
    if remote:
        # The local filesystem can't be walked to confirm a remote copy, so
        # run a --checksum dry-run over SSH: any file it would still transfer
        # is missing or differs at the destination. Covers both fresh and
        # merge moves, and is the safety backstop replacing the local
        # content-conflict and file-count checks.
        verify = _remote_verify_complete(rsync_bin, src_path, rsync_target, remote)
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
    elif dest_exists:
        # Merge: the destination may legitimately hold extra unrelated
        # files (and leftover temp files from an interrupted run), so a
        # count comparison is meaningless. Instead require that every
        # source file is present at the destination with a matching size.
        missing = _first_missing_source_file(src_path, transfer_dest)
        if missing is not None:
            return {"moved": 0, "errors": [
                f"Verification failed: '{missing}' missing, size mismatch, "
                f"or symlinked at destination. Originals preserved."
            ]}
    else:
        # Fresh move into a destination we created: a whole-tree file
        # count is a cheap, sufficient integrity check. Recount the source
        # here rather than reusing the pre-copy `total_files` — if a file
        # appeared in the source after that upfront count (and rsync didn't
        # pick it up), a stale count could spuriously match `dst_count` and
        # the rmtree below would delete the never-copied file. The fresh
        # walk catches that mismatch and preserves the originals.
        src_count = sum(1 for _, _, files in os.walk(src_path) for _ in files)
        dst_count = sum(1 for _, _, files in os.walk(transfer_dest) for _ in files)
        if src_count != dst_count:
            shutil.rmtree(transfer_dest, ignore_errors=True)
            return {"moved": 0, "errors": [
                f"File count mismatch: source={src_count}, dest={dst_count}. Originals preserved."
            ]}

    # Count photos for progress
    all_photos = db.conn.execute(
        """SELECT p.id FROM photos p
           JOIN folders f ON f.id = p.folder_id
           WHERE f.path = ? OR f.path LIKE ?""",
        (src_path, src_path + "/%"),
    ).fetchall()
    total_photos = len(all_photos)

    # Update DB first: cascade folder paths (safer — if rmtree fails, the old
    # folder becomes an orphan on disk rather than the DB pointing to deleted
    # paths). Unless the caller opted into merging (``merge_into_tracked``), a
    # merge into an already-tracked destination is refused above, so
    # catalog_path is never a different existing folder row in the cascade
    # branch and that cascade (root + all descendants) cannot collide with
    # folders.path UNIQUE. When merging into a tracked archive we instead
    # reconcile the staged rows into the existing archive rows below.
    # For a remote move catalog_path is the local mount path, so the catalog
    # keeps resolving to the photos whenever the NAS is mounted.
    if progress_cb:
        progress_cb(total_files, total_files, "", "Updating catalog")
    merge_counts = None
    if merge_into_tracked is not None:
        # Destination is a tracked archive and the caller opted into merging:
        # fold the staged folder/photo rows into the existing archive rows
        # instead of a path cascade (which would collide on folders.path).
        # ``merge_reconcile_base`` is the STORED tracked path for an exact
        # overlap (so alias/case-fold destinations still match the existing
        # rows) and ``catalog_path`` for the ancestor case; see where it is set.
        merge_counts = db.merge_staged_tree_into_archive(
            folder_id, merge_reconcile_base)
    else:
        db.move_folder_path(folder_id, catalog_path, new_name=landing_name)
    db.update_folder_counts()

    # Rebase any developed-output subdirs nested under the configured
    # darktable_output_dir. `developed_folder_key` hashes the folder's
    # path, so the DB update above just invalidated the old subdir's
    # implicit key — rename it on disk to match the new path, and cascade
    # to any descendant folders whose paths also shifted.
    #
    # On the merge path the catalog was reparented onto
    # ``merge_reconcile_base``, not ``catalog_path`` — those two only
    # differ when the tracked destination was reached via a symlink or
    # case-alias (see where ``merge_reconcile_base`` is set), but when they
    # do differ the developed-dir key is derived from the STORED path, not
    # the alias. Relocating from ``src_path`` to the aliased
    # ``catalog_path`` would move renders under the alias-path hash and
    # exports (which read the catalog's stored path) would look under the
    # stored-path hash, miss them, and fall back to RAW. Use the same
    # reconciled base the catalog uses.
    developed_base = (merge_reconcile_base
                      if merge_into_tracked is not None
                      else catalog_path)
    if developed_dir:
        from export import relocate_developed_dir
        relocate_developed_dir(developed_dir, src_path, developed_base)
        # SQL LIKE treats `_` and `%` (and the escape char) as wildcards,
        # all of which are valid POSIX path characters. Without a strict
        # prefix guard, an unrelated folder like `/dXst/birds/fake` would
        # match a pattern like `/d_st/birds/%` and feed a bogus computed
        # old_path into relocate_developed_dir, mis-rebasing the wrong
        # developed subdir. Filter results by a literal prefix check.
        descendant_rows = db.conn.execute(
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

    # Delete originals. The catalog already points at the new destination,
    # so anything that goes wrong from here is post-commit: the archive is
    # already published. Return a ``cleanup_error`` so the caller can warn
    # the user about leftover originals without misreporting the move as
    # failed (which would also leave the archive's tracked row in place
    # while telling the user their data is still in staging).
    if progress_cb:
        progress_cb(total_files, total_files, "", "Removing originals")
    log.info("Verification passed, deleting originals: %s", src_path)
    cleanup_error = None
    try:
        shutil.rmtree(src_path)
    except OSError as e:
        log.exception("Post-commit cleanup of %s failed", src_path)
        cleanup_error = str(e)

    if progress_cb:
        progress_cb(total_files, total_files, folder_name, "Done")

    result = {"moved": total_photos, "errors": []}
    if merge_into_tracked is not None:
        # ``dropped_photo_ids`` is a cleanup handle for the caller (thumbnails,
        # previews, offline copies of the deleted staged photos), not a
        # user-facing count. Lift it off ``merge_counts`` so ``result["merge"]``
        # stays a stable dict of display numbers that gets serialized straight
        # into the archive-stage summary/API payload.
        dropped = merge_counts.pop("dropped_photo_ids", None) or []
        result["merge"] = merge_counts
        result["merged_into_existing"] = merge_into_tracked
        # On the merge path ``total_photos`` counts every staged source photo,
        # including identical ones that were dropped as ``already_present``.
        # Report ``moved`` as the photos actually added to the archive.
        result["moved"] = merge_counts["new_photos"]
        if dropped:
            result["dropped_photo_ids"] = dropped
    if cleanup_error is not None:
        result["cleanup_error"] = cleanup_error
    return result
