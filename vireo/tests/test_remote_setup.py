import os
import socket
import subprocess
import sys
import time

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import remote_setup

SMB = "//julius_admin@100.80.236.59/Photography on /Volumes/Photography (smbfs, nodev, nosuid, mounted by julius)"
SMB_SPACES = "//guest@My%20NAS._smb._tcp.local/Photo%20Library on /Volumes/Photo Library (smbfs, nodev, nosuid, mounted by julius)"
NFS = "truenas:/mnt/tank/photos on /Volumes/photos (nfs, nodev, nosuid, mounted by julius)"
LOCAL = "/dev/disk3s1s1 on / (apfs, sealed, local, read-only, journaled)"
AUTOFS = "map auto_home on /System/Volumes/Data/home (autofs, automounted, nobrowse)"


class FakeRun:
    def __init__(self, stdout="", returncode=0):
        self.calls = []
        self.stdout, self.returncode = stdout, returncode

    def __call__(self, argv, **kw):
        self.calls.append(argv)
        return subprocess.CompletedProcess(argv, self.returncode,
                                           stdout=self.stdout, stderr="")


def test_parse_smbfs_mount():
    rows = remote_setup.parse_mount_output(SMB + "\n" + LOCAL + "\n" + AUTOFS)
    assert rows == [{
        "fs_type": "smbfs", "host": "100.80.236.59",
        "share": "Photography", "mount_point": "/Volumes/Photography",
        "user": "julius_admin",
    }]


def test_parse_smbfs_url_encoding_and_spaces():
    (row,) = remote_setup.parse_mount_output(SMB_SPACES)
    assert row["share"] == "Photo Library"
    assert row["host"] == "My NAS._smb._tcp.local"
    assert row["mount_point"] == "/Volumes/Photo Library"
    assert row["user"] == "guest"


def test_parse_nfs_mount():
    (row,) = remote_setup.parse_mount_output(NFS)
    assert row == {"fs_type": "nfs", "host": "truenas",
                   "share": "photos", "mount_point": "/Volumes/photos",
                   "user": ""}


def test_parse_ignores_non_network_and_garbage():
    assert remote_setup.parse_mount_output(LOCAL + "\nnot a mount line\n") == []


def test_parse_mount_table_preserves_unclassified_and_complex_mount_points():
    unusual = (
        "server:/archive on /Volumes/Photo on Film (Archive) "
        "(sshfs, nodev)"
    )

    assert remote_setup.parse_mount_table(LOCAL + "\n" + unusual) == [
        {"source": "/dev/disk3s1s1", "mount_point": "/", "fs_type": "apfs"},
        {
            "source": "server:/archive",
            "mount_point": "/Volumes/Photo on Film (Archive)",
            "fs_type": "sshfs",
        },
    ]


def test_parse_mount_table_keeps_multiword_autofs_sources():
    # macOS emits automount entries like ``map auto_home on
    # /System/Volumes/Data/home (autofs, ...)`` where the source itself
    # contains a space. Dropping those rows would let paths beneath the
    # trigger fall through to the local-classification branch and reach
    # the blocking in-process ``stat``/``isfile`` path when the target is
    # an unavailable network share — exactly the failure the fail-closed
    # probe is meant to prevent.
    auto_home = (
        "map auto_home on /System/Volumes/Data/home "
        "(autofs, automounted, nobrowse)"
    )
    fstab = (
        "map -fstab on /System/Volumes/Data/Network/Servers "
        "(autofs, automounted, nobrowse)"
    )

    rows = remote_setup.parse_mount_table(auto_home + "\n" + fstab)

    assert rows == [
        {
            "source": "map auto_home",
            "mount_point": "/System/Volumes/Data/home",
            "fs_type": "autofs",
        },
        {
            "source": "map -fstab",
            "mount_point": "/System/Volumes/Data/Network/Servers",
            "fs_type": "autofs",
        },
    ]
    # Retained rows must classify as network-or-unknown so callers route
    # them through the bounded probe path rather than the in-process
    # ``stat``/``isfile`` that trips the automount.
    assert all(
        remote_setup.mount_type_is_network_or_unknown(row["fs_type"])
        for row in rows
    )


def test_unknown_mount_types_fail_closed_as_possibly_network_backed():
    assert remote_setup.mount_type_is_network_or_unknown("apfs") is False
    assert remote_setup.mount_type_is_network_or_unknown("exfat") is False
    assert remote_setup.mount_type_is_network_or_unknown("smbfs") is True
    assert remote_setup.mount_type_is_network_or_unknown("sshfs") is True
    # autofs is an automount trigger, not local storage: its target can be an
    # unavailable network share, so callers must route through the bounded
    # probe path rather than issuing in-process stat/isfile that would trip
    # the automount and block.
    assert remote_setup.mount_type_is_network_or_unknown("autofs") is True


def test_parse_afpfs_and_ipv6_hosts():
    afp = "//julius@mynas._afpovertcp._tcp.local/Media on /Volumes/Media (afpfs, nodev, nosuid, mounted by julius)"
    v6 = "//admin@[fe80::1%25en0]/Backup on /Volumes/Backup (smbfs, nodev, nosuid, mounted by julius)"
    rows = remote_setup.parse_mount_output(afp + "\n" + v6)
    assert rows[0]["fs_type"] == "afpfs" and rows[0]["share"] == "Media"
    # Brackets are the URL-authority spelling; socket.create_connection and
    # friendly_host_name want the plain address, so parse_mount_output strips
    # them while preserving the scope id.
    assert rows[1]["host"] == "fe80::1%en0" and rows[1]["share"] == "Backup"


def test_parse_nfs_ipv6_mount():
    # IPv6 NFS mounts wrap the host in brackets to keep the host/path split
    # unambiguous (`[fe80::1]:/exports/photos`). NFS sources aren't
    # URL-encoded the way smbfs URLs are, so the scope id appears verbatim.
    # Without accepting the bracketed form the mount is silently dropped
    # from the wizard.
    v6 = "[fe80::1%en0]:/exports/photos on /Volumes/photos (nfs, nodev, nosuid, mounted by julius)"
    plain = "[2001:db8::5]:/mnt/tank/backup on /Volumes/backup (nfs, nodev, nosuid, mounted by julius)"
    rows = remote_setup.parse_mount_output(v6 + "\n" + plain)
    assert rows[0] == {
        "fs_type": "nfs", "host": "fe80::1%en0",
        "share": "photos", "mount_point": "/Volumes/photos", "user": "",
    }
    assert rows[1] == {
        "fs_type": "nfs", "host": "2001:db8::5",
        "share": "backup", "mount_point": "/Volumes/backup", "user": "",
    }


def test_list_network_mounts_runs_mount_and_resolves():
    run = FakeRun(stdout=SMB)
    rows = remote_setup.list_network_mounts(
        run=run, resolver=lambda ip: "synology-nas.tail1234.ts.net")
    assert run.calls == [["mount"]]
    assert rows[0]["friendly_host"] == "synology-nas.tail1234.ts.net"
    assert rows[0]["display_name"] == "synology-nas"


def test_list_network_mounts_caches_reverse_dns_per_host():
    # Two shares on the same IP-based NAS + one share on a different NAS:
    # the resolver must be called exactly once per unique host so that a
    # slow (2s-capped) PTR lookup can't stall the wizard by N × 2s.
    same_host_a = "//u@100.80.236.59/Photography on /Volumes/Photography (smbfs, nodev, nosuid, mounted by j)"
    same_host_b = "//u@100.80.236.59/Video on /Volumes/Video (smbfs, nodev, nosuid, mounted by j)"
    other_host = "//u@100.80.236.60/Backup on /Volumes/Backup (smbfs, nodev, nosuid, mounted by j)"
    run = FakeRun(stdout="\n".join([same_host_a, same_host_b, other_host]))
    calls = []

    def resolver(ip):
        calls.append(ip)
        return {"100.80.236.59": "nas-a.ts.net",
                "100.80.236.60": "nas-b.ts.net"}[ip]

    rows = remote_setup.list_network_mounts(run=run, resolver=resolver)
    assert [r["friendly_host"] for r in rows] == [
        "nas-a.ts.net", "nas-a.ts.net", "nas-b.ts.net"]
    assert sorted(calls) == ["100.80.236.59", "100.80.236.60"]


def test_pty_helper_prefix_uses_c_snippet_in_dev(monkeypatch):
    """In a normal Python run, sys.executable IS a python interpreter, so
    the pty helper is passed via ``-c`` — no separate file needed at
    runtime, no dependency on the frozen build's subcommand handler."""
    monkeypatch.delattr(remote_setup.sys, "frozen", raising=False)
    prefix = remote_setup._pty_helper_prefix()
    assert prefix[0] == remote_setup.sys.executable
    assert prefix[1] == "-c"
    assert "TIOCSCTTY" in prefix[2]  # the actual helper snippet


def test_pty_helper_prefix_uses_subcommand_in_frozen_build(monkeypatch):
    """In a PyInstaller --onefile build (the production packaging via
    scripts/build_sidecar.py), sys.executable is the frozen ``vireo-server``
    binary, not python — ``-c`` would relaunch the whole app with an
    unrecognized flag and never reach ssh. The prefix routes through the
    ``--pty-spawn-helper`` subcommand instead, which app.py dispatches
    before argparse sees the ssh argv."""
    monkeypatch.setattr(remote_setup.sys, "frozen", True, raising=False)
    prefix = remote_setup._pty_helper_prefix()
    assert prefix == [remote_setup.sys.executable, "--pty-spawn-helper"]


def test_friendly_host_passthrough_for_hostnames_and_failed_reverse():
    # Non-IP hosts pass through; resolver failures fall back to the raw host.
    assert remote_setup.friendly_host_name("mynas.local", resolver=None) == "mynas.local"

    def boom(ip):
        raise OSError("no PTR")

    assert remote_setup.friendly_host_name("100.80.236.59", resolver=boom) == "100.80.236.59"


# --- Vireo key management -------------------------------------------------


def test_key_paths_under_vireo_home(tmp_path, monkeypatch):
    (tmp_path / ".vireo").mkdir()
    monkeypatch.setenv("HOME", str(tmp_path))
    priv, pub = remote_setup.vireo_key_paths()
    assert priv == str(tmp_path / ".vireo" / "ssh" / "vireo_ed25519")
    assert pub == priv + ".pub"


@pytest.mark.skipif(sys.platform == "win32",
                    reason="POSIX chmod bits are ignored on Windows")
def test_ensure_vireo_key_generates_once(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    calls = []

    def fake_run(argv, **kw):
        calls.append(argv)
        priv, pub = remote_setup.vireo_key_paths()
        open(priv, "w").write("KEY")
        open(pub, "w").write("ssh-ed25519 AAAA vireo")
        return subprocess.CompletedProcess(argv, 0, stdout="", stderr="")

    priv, pub = remote_setup.ensure_vireo_key(run=fake_run)
    assert calls and calls[0][0] == "ssh-keygen"
    assert "-N" in calls[0] and "-t" in calls[0]
    assert oct(os.stat(os.path.dirname(priv)).st_mode & 0o777) == "0o700"
    # Second call: key exists, no regeneration.
    remote_setup.ensure_vireo_key(run=fake_run)
    assert len(calls) == 1


def test_ensure_vireo_key_raises_on_keygen_failure(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    run = FakeRun(returncode=1)
    with pytest.raises(RuntimeError):
        remote_setup.ensure_vireo_key(run=run)


@pytest.mark.skipif(sys.platform == "win32",
                    reason="POSIX chmod bits are ignored on Windows")
def test_ensure_vireo_key_recovers_missing_pub_from_existing_priv(
        tmp_path, monkeypatch):
    """If the .pub was deleted (or a prior generation was interrupted after
    writing priv), rebuild the public key with ``ssh-keygen -y`` instead
    of re-running ``ssh-keygen -f priv`` — the latter blocks forever on the
    non-interactive overwrite prompt and the wizard cannot repair itself."""
    monkeypatch.setenv("HOME", str(tmp_path))
    priv, pub = remote_setup.vireo_key_paths()
    os.makedirs(os.path.dirname(priv), exist_ok=True)
    open(priv, "w").write("EXISTING PRIVATE KEY")
    os.chmod(priv, 0o644)  # loose perms — must be normalized to 0600

    calls = []

    def fake_run(argv, **kw):
        calls.append(argv)
        return subprocess.CompletedProcess(
            argv, 0, stdout="ssh-ed25519 AAAAREGENERATED\n", stderr="")

    out_priv, out_pub = remote_setup.ensure_vireo_key(run=fake_run)
    assert out_priv == priv and out_pub == pub
    # Exactly one ssh-keygen invocation, and it's the non-interactive
    # -y derivation — never the interactive -f overwrite path.
    assert len(calls) == 1
    assert calls[0][:2] == ["ssh-keygen", "-y"] and "-f" in calls[0]
    assert "-t" not in calls[0]  # no regeneration attempted
    # The rebuilt .pub carries the vireo comment for parity with fresh gen.
    assert open(pub).read().strip() == "ssh-ed25519 AAAAREGENERATED vireo"
    # Loose priv perms were tightened.
    assert oct(os.stat(priv).st_mode & 0o777) == "0o600"


@pytest.mark.skipif(sys.platform == "win32",
                    reason="POSIX chmod bits are ignored on Windows")
def test_ensure_vireo_key_raises_when_pub_derivation_fails(
        tmp_path, monkeypatch):
    """If ssh-keygen -y fails on a corrupted private key, surface the
    error rather than silently overwriting the private key."""
    monkeypatch.setenv("HOME", str(tmp_path))
    priv, _ = remote_setup.vireo_key_paths()
    os.makedirs(os.path.dirname(priv), exist_ok=True)
    open(priv, "w").write("CORRUPTED")

    def fake_run(argv, **kw):
        return subprocess.CompletedProcess(
            argv, 1, stdout="", stderr="Load key: invalid format")

    with pytest.raises(RuntimeError, match="ssh-keygen -y failed"):
        remote_setup.ensure_vireo_key(run=fake_run)


def test_reverse_dns_worker_is_daemon(monkeypatch):
    """Hung PTR queries must leave only daemon threads behind so process
    shutdown never blocks. A ThreadPoolExecutor here would silently leak
    non-daemon workers that Python's atexit hook then joins on exit."""
    import threading as _t

    spawned = []
    real_start = _t.Thread.start

    def spy_start(self):
        spawned.append(self)
        return real_start(self)

    monkeypatch.setattr(_t.Thread, "start", spy_start)
    # Fast fake resolver so the test doesn't spend real time waiting.
    monkeypatch.setattr(remote_setup.socket, "gethostbyaddr",
                        lambda ip: ("cachedname", [], [ip]))

    assert remote_setup._reverse_dns("192.0.2.1") == "cachedname"
    assert spawned, "no worker thread was spawned"
    assert all(t.daemon for t in spawned), (
        "reverse-DNS worker must be a daemon so a hung lookup can't block exit")


def test_reverse_dns_times_out_when_lookup_hangs(monkeypatch):
    """A hung ``gethostbyaddr`` must surface as ``TimeoutError`` and not
    block on a shutdown(wait=True) after the guard fires."""
    import threading as _t

    def hang(_ip):
        _t.Event().wait()  # simulates a PTR query that never returns

    monkeypatch.setattr(remote_setup.socket, "gethostbyaddr", hang)
    # Shrink the 2s guard so the test doesn't sit on it.
    real_get = remote_setup._queue.Queue.get

    def quick_get(self, timeout=None):
        return real_get(self, timeout=0.05)

    monkeypatch.setattr(remote_setup._queue.Queue, "get", quick_get)

    start = time.monotonic()
    with pytest.raises(TimeoutError):
        remote_setup._reverse_dns("192.0.2.1")
    # If the code had done shutdown(wait=True), this would take much longer;
    # the daemon-thread + queue approach returns as soon as the wait expires.
    assert time.monotonic() - start < 1.0


def test_reverse_dns_propagates_lookup_error(monkeypatch):
    """Real resolver errors (e.g. no PTR record) must surface as an
    exception so ``friendly_host_name`` can fall back to the raw host."""
    def boom(_ip):
        raise socket.gaierror("no PTR")

    monkeypatch.setattr(remote_setup.socket, "gethostbyaddr", boom)
    with pytest.raises(socket.gaierror):
        remote_setup._reverse_dns("192.0.2.1")


def test_key_auth_works_builds_batchmode_ssh(tmp_path):
    run = FakeRun(stdout="vireo_ok\n")
    ok = remote_setup.key_auth_works(
        host="nas", user="admin", port=2222, key="/k", ssh_bin="/usr/bin/ssh",
        run=run)
    assert ok is True
    argv = run.calls[0]
    assert argv[0] == "/usr/bin/ssh"
    assert "BatchMode=yes" in argv
    assert "-p" in argv and "2222" in argv and "-i" in argv and "/k" in argv
    # IdentitiesOnly=yes prevents an ambient ssh-agent identity from making
    # the probe pass on the agent's key instead of the wizard-managed one.
    assert "IdentitiesOnly=yes" in argv
    assert argv[-2:] == ["admin@nas", "echo vireo_ok"]


def test_key_auth_works_omits_identities_only_when_no_key():
    """When no key is passed the probe shouldn't force IdentitiesOnly — the
    install-key path uses this shape (key='') and must remain free to
    negotiate password auth."""
    run = FakeRun(stdout="", returncode=255)
    remote_setup.key_auth_works(
        host="nas", user="admin", port=22, key="", ssh_bin="ssh", run=run)
    argv = run.calls[0]
    assert "IdentitiesOnly=yes" not in argv
    assert "-i" not in argv


def test_key_auth_works_false_on_denied(tmp_path):
    run = FakeRun(stdout="", returncode=255)
    assert remote_setup.key_auth_works(
        host="nas", user="admin", port=22, key="", ssh_bin="ssh",
        run=run) is False


def test_port_reachable(monkeypatch):
    class FakeSock:
        def close(self):
            pass

    monkeypatch.setattr(remote_setup.socket, "create_connection",
                        lambda addr, timeout: FakeSock())
    assert remote_setup.port_reachable("nas", 22) is True

    def refuse(addr, timeout):
        raise OSError("refused")

    monkeypatch.setattr(remote_setup.socket, "create_connection", refuse)
    assert remote_setup.port_reachable("nas", 22) is False


# --- Password-driven key install (pty) ------------------------------------

# install_key_with_password uses the `pty` module, which is POSIX-only
# (Windows Python raises ModuleNotFoundError: No module named 'termios'),
# so these tests only run on POSIX platforms — the feature itself is
# macOS-only in production.
pytestmark_pty = pytest.mark.skipif(
    sys.platform == "win32",
    reason="pty/termios are POSIX-only; install-key runs on macOS")


STUB = r'''#!/usr/bin/env python3
import sys
mode = sys.argv[1]  # ok | wrongpw | nopw | hostkey | unclassified
if mode == "nopw":
    sys.stderr.write("admin@nas: Permission denied (publickey).\n")
    sys.exit(255)
if mode == "hostkey":
    sys.stderr.write("Host key verification failed.\n")
    sys.exit(255)
sys.stderr.write("admin@nas's password: ")
sys.stderr.flush()
pw = input()
if mode == "unclassified":
    # ssh accepted the password prompt, then failed for some other reason
    # the classifier doesn't match — plus deliberately echo the password
    # back to prove redaction of `detail` catches the case where something
    # DID echo it (pty ECHO=off is the primary defense; this is the
    # belt-and-braces test).
    sys.stderr.write("echo-check:" + pw + "\n")
    sys.stderr.write("bash: /nas/authorized_keys: Read-only file system\n")
    sys.exit(1)
if mode == "ok" and pw == "sekret":
    sys.exit(0)
sys.stderr.write("Permission denied, please try again.\nadmin@nas's password: ")
sys.stderr.flush()
sys.exit(255)
'''


def _stub(tmp_path, mode):
    p = tmp_path / "fake_ssh.py"
    p.write_text(STUB)
    p.chmod(0o755)
    return [sys.executable, str(p), mode]


@pytestmark_pty
def test_install_key_success(tmp_path):
    res = remote_setup.install_key_with_password(
        spawn_argv=_stub(tmp_path, "ok"), password="sekret", timeout=15)
    assert res == {"ok": True, "error": None}


@pytestmark_pty
def test_install_key_wrong_password(tmp_path):
    res = remote_setup.install_key_with_password(
        spawn_argv=_stub(tmp_path, "wrongpw"), password="nope", timeout=15)
    assert res["ok"] is False and res["error"] == "wrong_password"


@pytestmark_pty
def test_install_key_password_auth_disabled(tmp_path):
    res = remote_setup.install_key_with_password(
        spawn_argv=_stub(tmp_path, "nopw"), password="x", timeout=15)
    assert res["ok"] is False and res["error"] == "password_auth_disabled"


@pytestmark_pty
def test_install_key_host_key_rejected(tmp_path):
    res = remote_setup.install_key_with_password(
        spawn_argv=_stub(tmp_path, "hostkey"), password="x", timeout=15)
    assert res["ok"] is False and res["error"] == "host_key"


@pytestmark_pty
def test_password_never_in_result_text(tmp_path):
    res = remote_setup.install_key_with_password(
        spawn_argv=_stub(tmp_path, "wrongpw"), password="hunter2", timeout=15)
    assert "hunter2" not in repr(res)


@pytestmark_pty
def test_unclassified_failure_redacts_password_from_detail(tmp_path):
    """An unclassified ssh failure (post-auth remote error) may include
    output that echoed the password back — pty ECHO=off is the primary
    defense, and detail redaction is the belt-and-braces backstop that
    keeps the password out of the returned dict either way."""
    res = remote_setup.install_key_with_password(
        spawn_argv=_stub(tmp_path, "unclassified"),
        password="hunter2_secret_pw", timeout=15)
    assert res["ok"] is False
    assert res["error"] == "ssh_failed"
    # Password must not appear anywhere in the returned dict — including detail.
    assert "hunter2_secret_pw" not in repr(res)
    assert "<redacted>" in res.get("detail", "")


def test_platform_supported_reflects_sys_platform(monkeypatch):
    """The wizard's mount enumeration only works on macOS today; the app
    endpoint routes through this helper so tests can flip it cleanly."""
    monkeypatch.setattr(remote_setup.sys, "platform", "darwin")
    assert remote_setup.platform_supported() is True
    monkeypatch.setattr(remote_setup.sys, "platform", "linux")
    assert remote_setup.platform_supported() is False


# --- Nonce share-locate + remote dir listing ------------------------------


def test_locate_share_writes_nonce_probes_and_cleans_up(tmp_path):
    mount = tmp_path / "mnt"
    mount.mkdir()
    seen = {}

    def fake_run(argv, **kw):
        seen["cmd"] = argv[-1]
        # Server-side: pretend /volume1/Photography contains the nonce.
        return subprocess.CompletedProcess(
            argv, 0, stdout="FOUND:/volume1/Photography\n", stderr="")

    path = remote_setup.locate_share(
        mount_point=str(mount), share="Photography",
        host="nas", user="admin", port=22, key="/k", ssh_bin="ssh",
        run=fake_run)
    assert path == "/volume1/Photography"
    assert ".vireo-probe-" in seen["cmd"] and "/volume*/" in seen["cmd"]
    assert list(mount.iterdir()) == []          # nonce cleaned up


def test_locate_share_no_match_returns_none_and_cleans_up(tmp_path):
    mount = tmp_path / "mnt"
    mount.mkdir()
    fake_run = FakeRun(stdout="")               # no candidate matched
    assert remote_setup.locate_share(
        mount_point=str(mount), share="Photography", host="nas",
        user="admin", port=22, key="/k", ssh_bin="ssh", run=fake_run) is None
    assert list(mount.iterdir()) == []


@pytest.mark.skipif(hasattr(os, "geteuid") and os.geteuid() == 0,
                    reason="root ignores directory write bits")
@pytest.mark.skipif(sys.platform == "win32",
                    reason="Windows ignores POSIX chmod, so read-only bit does not apply")
def test_locate_share_readonly_mount_raises(tmp_path):
    mount = tmp_path / "mnt"
    mount.mkdir()
    mount.chmod(0o500)          # no write bit — nonce write must fail cleanly
    try:
        with pytest.raises(remote_setup.MountNotWritable):
            remote_setup.locate_share(
                mount_point=str(mount), share="S", host="h", user="u",
                port=22, key="", ssh_bin="ssh", run=FakeRun())
    finally:
        mount.chmod(0o700)      # let pytest clean tmp_path up


def test_list_remote_dirs_parses_and_quotes(tmp_path):
    run = FakeRun(stdout="Raw Files\nExports\n")
    dirs = remote_setup.list_remote_dirs(
        path="/volume1/My Photos", host="nas", user="admin", port=22,
        key="", ssh_bin="ssh", run=run)
    assert dirs == ["Exports", "Raw Files"]
    assert "'/volume1/My Photos'" in run.calls[0][-1]


def test_list_remote_dirs_empty_on_error():
    assert remote_setup.list_remote_dirs(
        path="/nope", host="h", user="u", port=22, key="", ssh_bin="ssh",
        run=FakeRun(returncode=255)) == []


def test_build_install_argv_no_batchmode_and_idempotent_append():
    argv = remote_setup.build_install_argv(
        host="nas", user="admin", port=22,
        key_pub_line="ssh-ed25519 AAAA vireo", ssh_bin="/usr/bin/ssh")
    assert argv[0] == "/usr/bin/ssh"
    assert "BatchMode=yes" not in argv          # password prompt must reach the pty
    assert "StrictHostKeyChecking=accept-new" in argv
    assert argv[-2] == "admin@nas"
    snippet = argv[-1]
    assert "umask 077" in snippet and "mkdir -p ~/.ssh" in snippet
    assert "grep -qxF" in snippet and "authorized_keys" in snippet
    assert "ssh-ed25519 AAAA vireo" in snippet


def test_build_install_argv_forces_password_only_auth():
    """An encrypted ~/.ssh/id_* would otherwise trigger a passphrase prompt
    ("Enter passphrase for key ..."), which the pty driver's
    _PASSWORD_PROMPT_RE doesn't match, hanging the wizard until the 30s
    timeout. Disabling pubkey auth for this one bootstrap invocation makes
    ssh go straight to the password prompt the driver is designed to answer."""
    argv = remote_setup.build_install_argv(
        host="nas", user="admin", port=22,
        key_pub_line="ssh-ed25519 AAAA vireo", ssh_bin="ssh")
    assert "PubkeyAuthentication=no" in argv
    assert "PreferredAuthentications=password" in argv


def test_key_auth_works_argv_still_negotiates_pubkey():
    """The pubkey probe must NOT inherit the install-key path's password-only
    lockdown — that would make every key_auth_works() call return False."""
    run = FakeRun(stdout="vireo_ok\n")
    remote_setup.key_auth_works(
        host="nas", user="admin", port=22, key="/k", ssh_bin="ssh", run=run)
    argv = run.calls[0]
    assert "PubkeyAuthentication=no" not in argv
    assert "PreferredAuthentications=password" not in argv
