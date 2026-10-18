"""SSH and rsync plumbing for moves to a remote (NAS) target.

Building the ``remote`` dict ``move_folder`` takes, finding a GNU rsync and
an OpenSSH binary, the ``ssh`` arguments and ``user@host:path`` specs every
transfer uses, and the SSH probes (``mkdir -p``, directory existence, free
space, the move UI's preflight and the settings connection test).
"""

import contextlib
import os
import posixpath
import shlex
import shutil
import subprocess
import sys

try:
    from ..proc import no_window_kwargs
except ImportError:
    from proc import no_window_kwargs

# ``vireo/``: a packaged build ships GNU rsync beside the app's modules.
_VIREO_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


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


# Path candidates for a GNU rsync usable for remote (SSH) moves. macOS's
# /usr/bin/rsync is Apple's openrsync, which can't drive rsync-over-SSH to a
# GNU rsync peer; it is intentionally absent here AND the resolver verifies
# any PATH/`/usr/bin/rsync` candidate with is_gnu_rsync before returning it,
# so a host with only openrsync still returns None. A packaged build drops a
# bundled static GNU rsync next to this module (vireo/bin/rsync) or in the
# app's Resources dir; dev machines fall back to a Homebrew/MacPorts install
# or the distro's `/usr/bin/rsync` (GNU rsync on every Linux distro).
_BUNDLED_RSYNC_CANDIDATES = (
    os.path.join(_VIREO_DIR, "bin", "rsync"),
    os.path.join(_VIREO_DIR, "..", "Resources", "rsync"),
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
