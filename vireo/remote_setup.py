"""Discovery + one-time setup helpers behind the NAS setup wizard.

Pure logic with an injectable command-runner seam (the ``move.py``
pattern): production passes nothing and gets ``subprocess``; tests pass
fakes. Nothing here imports Flask or touches the database.
"""

import contextlib
import ipaddress
import os
import queue as _queue
import re
import secrets
import select
import shlex
import socket
import subprocess
import sys
import threading
import time
import urllib.parse

# `mount` line: "<source> on <mount point> (<fstype>, opt, ...)".
# Sources for the network filesystems we accept happen to be space-free
# (smbfs/afp URL-encode spaces; nfs is host:/path), but macOS also emits
# multi-word sources for automount triggers such as ``map auto_home on
# /System/Volumes/Data/home (autofs, ...)``. Restricting the source to
# ``\S+`` silently dropped those lines and let paths beneath the automount
# be classified as local — the exact failure mode the fail-closed probe
# path is designed to avoid. Anchor the split by requiring the mount point
# to start with ``/`` instead: the non-greedy source stops at the FIRST
# ``" on /"`` boundary, which unambiguously separates ``<src>`` from the
# absolute mount path.
_MOUNT_RE = re.compile(r"^(?P<src>.+?) on (?P<mp>/.*) \((?P<opts>[^()]*)\)$")
# smbfs/afp source: //[user@]host/share  (URL-encoded)
_SMB_SRC_RE = re.compile(r"^//(?:(?P<user>[^@/]+)@)?(?P<host>[^/]+)/(?P<share>.+)$")
# nfs source: host:/export/path — host may be a hostname, IPv4, or a
# bracketed IPv6 address (`[fe80::1%en0]:/exports/photos`). The colon count
# in an IPv6 literal forces the brackets so the host/path split stays
# unambiguous, so accept either form and normalize the host below.
_NFS_SRC_RE = re.compile(
    r"^(?:\[(?P<hostv6>[^\]]+)\]|(?P<host>[^:/]+)):(?P<path>/.*)$"
)

_NETWORK_FS = ("smbfs", "nfs", "afpfs", "webdav")
# autofs is deliberately excluded: an autofs entry is an automount trigger,
# not local storage, and the target it resolves to can be an unavailable
# network share. Treating it as local would let ``_path_on_network_volume``
# route callers through in-process ``stat``/``isfile`` calls that trip the
# automount and block indefinitely — the exact failure mode the fail-closed
# probe path is designed to avoid.
_LOCAL_FS = frozenset({
    "apfs", "cd9660", "devfs", "exfat", "fdesc", "hfs",
    "msdos", "nullfs", "procfs", "tmpfs", "udf", "union",
})


def platform_supported():
    """True where the wizard can meaningfully enumerate mounted NAS shares.

    Only macOS today: mount-line parsing (``_MOUNT_RE`` + URL-decoded smbfs
    sources) is written against ``/sbin/mount`` output. Linux and Windows
    format ``mount`` differently, so the wizard's mounts endpoint returns
    ``unsupported_platform`` there. Named as its own function so tests that
    exercise the wizard end-to-end can flip it without stomping on
    ``sys.platform`` globally.
    """
    return sys.platform == "darwin"


def parse_mount_table(text):
    """Parse macOS ``mount`` output without classifying its filesystems."""
    rows = []
    for line in text.splitlines():
        match = _MOUNT_RE.match(line.strip())
        if not match:
            continue
        rows.append({
            "source": match.group("src"),
            "mount_point": match.group("mp"),
            "fs_type": (match.group("opts").split(",")[0] or "").strip(),
        })
    return rows


def mount_type_is_network_or_unknown(fs_type):
    """Fail closed for mount types that are not explicitly known-local.

    New network filesystem implementations must not silently gain local-file
    semantics in safety-sensitive callers such as Trash routing. Unknown local
    filesystems may take a slower Finder path until added to ``_LOCAL_FS``.
    """
    return (fs_type or "").strip().lower() not in _LOCAL_FS


def parse_mount_output(text):
    """Parse ``mount`` output into network-share rows the wizard can offer."""
    rows = []
    for mount in parse_mount_table(text):
        fs_type = mount["fs_type"]
        if fs_type not in _NETWORK_FS:
            continue
        src, mount_point = mount["source"], mount["mount_point"]
        if fs_type == "nfs":
            n = _NFS_SRC_RE.match(src)
            if not n:
                continue
            # Return the plain address for IPv6 hosts (matches the smbfs
            # path above): socket.create_connection and friendly_host_name
            # both want `fe80::1%en0`, not `[fe80::1%en0]`.
            host = n.group("hostv6") or n.group("host")
            rows.append({
                "fs_type": fs_type, "host": host,
                "share": os.path.basename(n.group("path").rstrip("/")),
                "mount_point": mount_point, "user": "",
            })
            continue
        s = _SMB_SRC_RE.match(src)
        if not s:
            continue
        unq = urllib.parse.unquote
        # SMB URL-authority form wraps IPv6 in brackets (`[fe80::1%25en0]`).
        # Keep the URL form out of downstream consumers: socket.create_connection
        # and friendly_host_name want the plain address (`fe80::1%en0`), and
        # the brackets confuse both. Strip them once here.
        host = unq(s.group("host"))
        if host.startswith("[") and host.endswith("]"):
            host = host[1:-1]
        rows.append({
            "fs_type": fs_type, "host": host,
            "share": unq(s.group("share").rstrip("/")),
            "mount_point": mount_point, "user": unq(s.group("user") or ""),
        })
    return rows


def _reverse_dns(ip):
    """Default resolver: PTR lookup with a hard 2s cap — gethostbyaddr has
    no timeout parameter and can hang for many seconds on networks that
    drop PTR queries, which would stall the wizard's mounts endpoint.

    Runs the lookup in a daemon thread and reads the answer through a
    bounded queue. The daemon flag is what makes the leak-free story true:
    a hung ``gethostbyaddr`` cannot block interpreter shutdown, and each
    stuck lookup costs exactly one OS thread that dies the moment the
    kernel returns from the syscall. ``ThreadPoolExecutor`` doesn't work
    here — its worker threads are NON-daemon and are joined via an
    ``atexit`` hook, so a stuck PTR query would silently block process
    exit, and dropping the executor with ``shutdown(wait=False)`` leaks
    a non-daemon worker per timed-out lookup.
    """
    result_q = _queue.Queue(maxsize=1)

    def _worker():
        try:
            result_q.put(("ok", socket.gethostbyaddr(ip)))
        except BaseException as exc:  # pylint: disable=broad-except
            result_q.put(("err", exc))

    threading.Thread(target=_worker, daemon=True).start()
    try:
        kind, value = result_q.get(timeout=2)
    except _queue.Empty:
        raise TimeoutError(f"reverse DNS for {ip} timed out") from None
    if kind == "err":
        raise value
    return value[0]


def friendly_host_name(host, resolver=None):
    """Reverse-resolve an IP host to a name (Tailscale MagicDNS, mDNS, DNS);
    non-IP hosts and failed lookups pass through unchanged."""
    try:
        ipaddress.ip_address(host)
    except ValueError:
        return host
    try:
        name = (resolver or _reverse_dns)(host)
    except (OSError, ValueError):
        # No PTR record, timeout (TimeoutError), or an undecodable name: a
        # missing friendly name is routine, so the raw address stands.
        return host
    return (name or host).rstrip(".") or host


def vireo_key_paths():
    """Paths of the dedicated wizard-managed keypair. Lives under ~/.vireo
    (not ~/.ssh) so setup never touches or clobbers user-managed keys."""
    priv = os.path.join(os.path.expanduser("~"), ".vireo", "ssh",
                        "vireo_ed25519")
    return priv, priv + ".pub"


def ensure_vireo_key(run=subprocess.run, ssh_keygen_bin="ssh-keygen"):
    """Generate the Vireo keypair if missing; return (private, public) paths.

    No passphrase: background rsync jobs must run unattended. The key never
    leaves this machine and grants only what the NAS account grants.

    Repairs partial state: if only ``priv`` exists (interrupted generation,
    or the ``.pub`` was deleted), regenerate the public key with
    ``ssh-keygen -y -f priv`` rather than falling through to a fresh
    ``-f priv`` that would sit forever on the non-interactive overwrite
    prompt. Always normalizes ``priv`` to ``0600`` so an existing key with
    loose permissions doesn't cause ssh to reject it later.
    """
    priv, pub = vireo_key_paths()
    key_dir = os.path.dirname(priv)
    os.makedirs(key_dir, mode=0o700, exist_ok=True)
    os.chmod(key_dir, 0o700)  # makedirs mode is ignored for existing dirs
    if os.path.exists(priv):
        with contextlib.suppress(OSError):
            os.chmod(priv, 0o600)
        if os.path.exists(pub):
            return priv, pub
        # Rebuild the .pub from the existing private key. ``-y`` reads the
        # private key and prints the corresponding public key to stdout —
        # no interactive prompts, no risk of clobbering ``priv``.
        r = run([ssh_keygen_bin, "-y", "-f", priv],
                capture_output=True, text=True, timeout=15)
        pub_line = (getattr(r, "stdout", "") or "").strip()
        if r.returncode != 0 or not pub_line:
            detail = (getattr(r, "stderr", "") or "").strip()
            raise RuntimeError(
                f"ssh-keygen -y failed: {detail or 'unknown error'}")
        # -y output omits the trailing comment; add "vireo" to match what
        # fresh generation produces so the rest of the app doesn't see two
        # shapes of the same key.
        if len(pub_line.split()) == 2:
            pub_line = pub_line + " vireo"
        with open(pub, "w") as f:
            f.write(pub_line + "\n")
        with contextlib.suppress(OSError):
            os.chmod(pub, 0o644)
        return priv, pub
    r = run([ssh_keygen_bin, "-t", "ed25519", "-N", "", "-f", priv,
             "-C", "vireo"], capture_output=True, text=True, timeout=30)
    if r.returncode != 0 or not os.path.exists(priv):
        detail = (getattr(r, "stderr", "") or "").strip()
        raise RuntimeError(f"ssh-keygen failed: {detail or 'unknown error'}")
    os.chmod(priv, 0o600)
    return priv, pub


def _ssh_option_args(port, key, batch=True, password_only=False):
    """Mirror of move.ssh_base_args for wizard probes (kept local so this
    module stays import-light; see that docstring for the option rationale).
    ``batch=False`` drops BatchMode so a password prompt can reach a pty.
    ``password_only=True`` disables public-key auth entirely so the pty
    driver reaches the password prompt without first getting stuck on a
    passphrase prompt for an encrypted default identity."""
    args = []
    if batch:
        args += ["-o", "BatchMode=yes"]
    args += ["-o", "StrictHostKeyChecking=accept-new",
             "-o", "ConnectTimeout=10"]
    try:
        if int(port or 22) != 22:
            args += ["-p", str(int(port))]
    except (TypeError, ValueError):
        pass
    if password_only:
        # Without these, ssh first offers any ssh-agent identities and any
        # ~/.ssh/id_* defaults; an encrypted one prompts for its PASSPHRASE
        # ("Enter passphrase for key ..."), which _PASSWORD_PROMPT_RE does
        # not match, and the pty driver waits out the 30s timeout instead
        # of ever seeing the NAS password prompt it is designed to answer.
        args += ["-o", "PubkeyAuthentication=no",
                 "-o", "PreferredAuthentications=password"]
    if key:
        # IdentitiesOnly=yes is what makes ``key_auth_works`` a truthful check.
        # Without it, ssh still consults ssh-agent and any default identities,
        # so an ambient agent identity already authorized on the NAS lets `-i
        # <vireo-key>` succeed on the agent's key rather than ours — the wizard
        # would then skip install-key and save a target that stops working the
        # moment the agent identity is unavailable.
        args += ["-o", "IdentitiesOnly=yes", "-i", key]
    return args


def key_auth_works(host, user, port, key, ssh_bin, run=subprocess.run):
    """True when passwordless (key) SSH login works right now."""
    argv = ([ssh_bin] + _ssh_option_args(port, key)
            + [f"{user}@{host}", "echo vireo_ok"])
    try:
        r = run(argv, capture_output=True, text=True, timeout=20)
    except (OSError, subprocess.SubprocessError):
        return False
    return r.returncode == 0 and "vireo_ok" in (r.stdout or "")


def build_install_argv(host, user, port, key_pub_line, ssh_bin):
    """ssh argv that appends our public key to the NAS user's
    authorized_keys — what ssh-copy-id does, minus the extra binary.
    No BatchMode: the password prompt must reach the pty driver. The
    append is idempotent (grep before echo) so re-runs are safe."""
    quoted = shlex.quote(key_pub_line.strip())
    snippet = (
        f"umask 077; mkdir -p ~/.ssh; touch ~/.ssh/authorized_keys; "
        f"grep -qxF {quoted} ~/.ssh/authorized_keys || "
        f"echo {quoted} >> ~/.ssh/authorized_keys"
    )
    return ([ssh_bin] + _ssh_option_args(port, key="", batch=False,
                                         password_only=True)
            + [f"{user}@{host}", snippet])


_PASSWORD_PROMPT_RE = re.compile(r"[Pp]assword[^\n]*:")

# Runs in a fresh Python interpreter (spawned via subprocess), NOT as a
# preexec_fn callback in the forked child. That distinction matters: this
# install-key path runs inside the 16-thread Waitress worker pool, and the
# subprocess docs warn that preexec_fn is unsafe under threads because the
# forked child inherits every lock the parent's other threads were holding
# and can deadlock before exec. A subprocess helper avoids that entirely —
# the child process is a brand-new Python with no inherited threads or
# locks, and Popen without preexec_fn uses posix_spawn/vfork under the
# hood (thread-safe). The helper does the pty setup real ssh needs
# (setsid + TIOCSCTTY so /dev/tty resolves) plus a defensive ECHO=off,
# then execvp's the real ssh argv. Written as a one-line -c snippet so
# there's no separate file to ship or find at runtime.
_PTY_SPAWN_HELPER = (
    "import os,sys,fcntl,termios;"
    "os.setsid();"
    "fcntl.ioctl(0,termios.TIOCSCTTY,0);"
    # ECHO=off before ssh takes over: a fresh pty defaults to ECHO=ON, and
    # the password we write later would then echo into the parent's read
    # buffer where an unclassified failure could return it as `detail`.
    # ssh sets ECHO off itself when it prompts, but pre-disabling closes
    # the race; the redaction below is the belt-and-braces backstop.
    "a=termios.tcgetattr(0);"
    "a[3]&=~(termios.ECHO|termios.ECHOE|termios.ECHOK|termios.ECHONL);"
    "termios.tcsetattr(0,termios.TCSANOW,a);"
    "os.execvp(sys.argv[1],sys.argv[1:])"
)


def _pty_helper_prefix():
    """argv prefix that wraps ssh in the pty setup helper.

    In a PyInstaller ``--onefile`` build (the production packaging on
    macOS via ``scripts/build_sidecar.py``) ``sys.executable`` is the
    frozen ``vireo-server`` binary, not a Python interpreter, so the
    ``[sys.executable, "-c", helper]`` shape used in dev would relaunch
    the whole app with an unrecognized ``-c`` and never exec ssh. The
    frozen build instead routes through the app's ``--pty-spawn-helper``
    subcommand (dispatched in ``app.py`` before ``main()``'s argparse
    runs), which performs the identical setsid + TIOCSCTTY + ECHO=off +
    execvp dance.
    """
    if getattr(sys, "frozen", False):
        return [sys.executable, "--pty-spawn-helper"]
    return [sys.executable, "-c", _PTY_SPAWN_HELPER]


def install_key_with_password(spawn_argv, password, timeout=30):
    """Drive ``spawn_argv`` through a pty, answering one password prompt.

    Returns ``{"ok": bool, "error": code-or-None}`` (plus ``"detail"`` for
    unclassified failures). Error codes: ``wrong_password`` (a second
    prompt appeared — never answered twice), ``password_auth_disabled``,
    ``host_key``, ``timeout``, ``ssh_failed``.

    The password must never reach logs, exceptions, or the returned dict —
    it is written to the pty and nowhere else.
    """
    # pty is POSIX-only; import here so `remote_setup` still loads on
    # Windows (the wizard itself is macOS-only in production).
    import pty

    master, slave = pty.openpty()
    # Spawn the real ssh through a Python helper (see _PTY_SPAWN_HELPER):
    # the helper does the setsid+TIOCSCTTY+ECHO=off dance and then execs
    # ssh. No preexec_fn — Popen without one is thread-safe under
    # Waitress, and the helper Python is a fresh process with no
    # inherited locks so its setup code cannot deadlock.
    try:
        proc = subprocess.Popen(
            [*_pty_helper_prefix(), *spawn_argv],
            stdin=slave, stdout=slave, stderr=slave)
    except OSError as exc:
        os.close(master)
        os.close(slave)
        return {"ok": False, "error": "ssh_failed", "detail": str(exc)}
    os.close(slave)

    output = ""
    answered = False
    deadline = time.monotonic() + timeout
    try:
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                proc.kill()
                return {"ok": False, "error": "timeout"}
            ready, _, _ = select.select([master], [], [], min(remaining, 0.5))
            if ready:
                try:
                    chunk = os.read(master, 4096)
                except OSError:  # EIO on macOS/Linux when the child exits
                    chunk = b""
                if chunk:
                    output += chunk.decode("utf-8", "replace")
                    prompts = len(_PASSWORD_PROMPT_RE.findall(output))
                    if prompts >= 2:
                        # A re-prompt means the first answer was rejected.
                        # Never answer twice — fail fast as wrong password.
                        proc.kill()
                        return {"ok": False, "error": "wrong_password"}
                    if prompts == 1 and not answered:
                        answered = True
                        os.write(master, (password + "\n").encode())
                    continue
            if proc.poll() is not None:
                break
    finally:
        os.close(master)
        if proc.poll() is None:
            proc.kill()
        proc.wait()

    if proc.returncode == 0:
        return {"ok": True, "error": None}
    # Classify from the ACCUMULATED output — the failure text and EOF can
    # arrive in a single read, after the loop has already answered once.
    if len(_PASSWORD_PROMPT_RE.findall(output)) >= 2:
        return {"ok": False, "error": "wrong_password"}
    if "Host key verification failed" in output:
        return {"ok": False, "error": "host_key"}
    if "Permission denied" in output and not answered:
        return {"ok": False, "error": "password_auth_disabled"}
    # Belt-and-braces on top of the pty ECHO=off above: never let the
    # password reach the returned detail even if something along the way
    # (an unusual terminal driver, a stubborn ssh build) echoed it back.
    safe = output.replace(password, "<redacted>") if password else output
    return {"ok": False, "error": "ssh_failed",
            "detail": safe[-400:].strip()}


def port_reachable(host, port, timeout=5):
    """True when a TCP connection to host:port succeeds within timeout."""
    try:
        sock = socket.create_connection((host, int(port)), timeout=timeout)
    except (OSError, ValueError):
        return False
    sock.close()
    return True


class MountNotWritable(RuntimeError):
    """The local mount refused the nonce write — the wizard needs a
    writable share (so does the move workflow it is setting up)."""


def _ssh_exec(host, user, port, key, ssh_bin, command, run, timeout=20):
    argv = ([ssh_bin] + _ssh_option_args(port, key)
            + [f"{user}@{host}", command])
    return run(argv, capture_output=True, text=True, timeout=timeout)


# Candidate export roots checked for the share, in likelihood order:
# Synology (/volume*), QNAP (/share, /shares), TrueNAS/generic (/mnt/*),
# plain Linux servers (/srv/*).
_SHARE_CANDIDATE_ROOTS = '/volume*/"$S" /share/"$S" /shares/"$S" /mnt/*/"$S" /srv/*/"$S"'


def locate_share(mount_point, share, host, user, port, key, ssh_bin,
                 run=subprocess.run):
    """Prove which NAS-side directory backs ``mount_point``.

    Writes a nonce file through the mount, then asks the NAS (over SSH)
    which candidate share directory contains that exact file. A match is
    the verified remote path — heuristics could pick a same-named share on
    the wrong volume, and the chained move DELETES local originals, so
    only proof is acceptable here. Returns None when no candidate matched.
    """
    nonce = ".vireo-probe-" + secrets.token_hex(8)
    nonce_path = os.path.join(mount_point, nonce)
    try:
        with open(nonce_path, "w") as f:
            f.write("vireo share-locate probe; safe to delete\n")
    except OSError as exc:
        raise MountNotWritable(str(exc)) from exc
    try:
        cmd = (f"S={shlex.quote(share)}; "
               f"for p in {_SHARE_CANDIDATE_ROOTS}; do "
               f'[ -e "$p/{nonce}" ] && {{ echo "FOUND:$p"; break; }}; '
               f"done; true")
        try:
            r = _ssh_exec(host, user, port, key, ssh_bin, cmd, run)
        except (OSError, subprocess.SubprocessError):
            return None
        for line in (r.stdout or "").splitlines():
            if line.startswith("FOUND:"):
                return line[len("FOUND:"):].strip() or None
        return None
    finally:
        # Mount-side cleanup only: the nonce lives on the share, so this
        # removes it regardless of which NAS path it appeared at.
        with contextlib.suppress(OSError):
            os.unlink(nonce_path)


def list_remote_dirs(path, host, user, port, key, ssh_bin,
                     run=subprocess.run):
    """Immediate subdirectory names of ``path`` on the NAS, for the
    fallback browser when the nonce probe finds nothing."""
    cmd = (f"find {shlex.quote(path)} -mindepth 1 -maxdepth 1 -type d "
           f"-exec basename {{}} \\;")
    try:
        r = _ssh_exec(host, user, port, key, ssh_bin, cmd, run)
    except (OSError, subprocess.SubprocessError):
        return []
    if r.returncode != 0:
        return []
    names = [n for n in (r.stdout or "").splitlines() if n.strip()]
    return sorted(names, key=str.casefold)


def list_network_mounts(run=subprocess.run, resolver=None):
    """Enumerate mounted network shares, each with a display-friendly host.

    Reverse-DNS results are memoized per unique host across the rows, so N
    shares from the same IP-based NAS cost one PTR lookup instead of N.
    Without this, a single unresponsive PTR server would stall the mounts
    endpoint by ``2s × number_of_shares`` (the ``_reverse_dns`` cap).
    """
    try:
        r = run(["mount"], capture_output=True, text=True, timeout=10)
    except (OSError, subprocess.SubprocessError):
        return []
    rows = parse_mount_output(r.stdout or "")
    friendly_by_host = {}
    for row in rows:
        host = row["host"]
        if host not in friendly_by_host:
            friendly_by_host[host] = friendly_host_name(host, resolver=resolver)
        row["friendly_host"] = friendly_by_host[host]
        row["display_name"] = row["friendly_host"].split(".", 1)[0]
    return rows
