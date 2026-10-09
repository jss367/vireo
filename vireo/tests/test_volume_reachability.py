import errno
import os
import sys

from testing.waits import synchronization_timeout

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest
import volume_reachability as vr


@pytest.fixture(autouse=True)
def _clear_shared_gate():
    vr.get_shared().clear()
    yield
    vr.get_shared().clear()


@pytest.fixture(autouse=True)
def _posix_shaped_inputs_stay_posix(monkeypatch):
    """Keep ``/Volumes/...``, ``/mnt/...`` and ``//server/share`` literals
    POSIX-shaped on every host.

    On Windows ``os.path.abspath("/mnt/archive")`` becomes ``D:\\mnt\\archive``,
    which would add a spurious ``D:/`` drive candidate and double every probe
    in these tests. Real Windows inputs (``tmp_path``, ``C:/...``) are left to
    the real functions.
    """
    import posixpath

    # Capture originals first: on POSIX ``posixpath`` *is* ``os.path``, so
    # calling ``posixpath.normpath`` after patching would recurse.
    real_abspath, real_normpath = os.path.abspath, os.path.normpath
    posix_normpath = posixpath.normpath

    def abspath(p):
        p = os.fspath(p)
        return posix_normpath(p) if p.startswith("/") else real_abspath(p)

    def normpath(p):
        p = os.fspath(p)
        if p.startswith("//"):
            return "//" + posix_normpath(p[2:]).lstrip("/")
        return posix_normpath(p) if p.startswith("/") else real_normpath(p)

    monkeypatch.setattr(vr.os.path, "abspath", abspath)
    monkeypatch.setattr(vr.os.path, "normpath", normpath)


class _FakeScandir:
    def __init__(self, names):
        self._it = iter(names)

    def __enter__(self):
        return self._it

    def __exit__(self, *exc):
        return False


class _FakeClock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


def test_mount_root_candidates_recognizes_mount_shaped_prefixes():
    assert vr.mount_root_candidates("/Volumes/Photography/Raw Files/2026") == [
        "/Volumes/Photography"
    ]
    assert vr.mount_root_candidates("/mnt/nas/photos") == ["/mnt/nas"]
    assert vr.mount_root_candidates("/media/julius/card/DCIM") == [
        "/media/julius/card"
    ]
    assert vr.mount_root_candidates("//server/share/photos") == ["//server/share"]


def test_mount_root_candidates_normalizes_dot_segments_before_extraction(monkeypatch):
    monkeypatch.setattr(vr, "_system_mount_roots", lambda: set())
    monkeypatch.setattr(vr, "_MOUNT_BASELINE", {})
    probed = []
    monkeypatch.setattr(
        vr, "_bounded_link_target",
        lambda path, timeout=None: probed.append(path),
    )

    assert vr.mount_root_candidates("/mnt/NAS/../localphotos") == [
        "/mnt/localphotos"
    ]
    assert "/mnt/NAS" not in probed


def test_mount_root_candidates_keeps_unc_share_anchor_through_dot_segments(monkeypatch):
    monkeypatch.setattr(vr, "_system_mount_roots", lambda: set())
    monkeypatch.setattr(vr, "_MOUNT_BASELINE", {})
    monkeypatch.setattr(
        vr, "_bounded_link_target",
        lambda path, timeout=None: pytest.fail(f"UNC lookup: {path}"),
    )

    assert vr.mount_root_candidates(r"\\server\share\..\photos") == [
        "//server/share"
    ]


def test_mount_root_candidates_empty_for_ordinary_local_paths(tmp_path):
    if sys.platform == "win32":
        pytest.skip("every absolute Windows path has a drive-letter candidate")
    assert vr.mount_root_candidates(str(tmp_path / "shoot")) == []


def test_mount_root_candidates_uses_mount_table_for_custom_root(monkeypatch):
    """A custom mount boundary is found without a direct lookup on it."""
    if sys.platform == "win32":
        pytest.skip("custom POSIX mount roots do not apply on Windows")
    monkeypatch.setattr(vr, "_system_mount_roots", lambda: {"/srv/photos"})
    monkeypatch.setattr(vr, "_MOUNT_BASELINE", {})
    bounded = []
    monkeypatch.setattr(
        vr, "_bounded_link_target",
        lambda path, timeout=None: bounded.append(path),
    )
    direct = []
    monkeypatch.setattr(
        vr, "_is_link_or_junction",
        lambda path: direct.append(path) or False,
    )

    assert vr.mount_root_candidates("/srv/photos/2026/shoot") == ["/srv/photos"]
    assert bounded == ["/srv", "/srv/photos"]
    assert not any(path.startswith("/srv/photos") for path in direct)


def test_mount_root_candidates_uses_history_for_detached_custom_root(monkeypatch):
    """A custom root remains a safe boundary after it leaves the mount table."""
    if sys.platform == "win32":
        pytest.skip("custom POSIX mount roots do not apply on Windows")
    monkeypatch.setattr(vr, "_system_mount_roots", lambda: set())
    monkeypatch.setattr(vr, "_MOUNT_BASELINE", {})
    monkeypatch.setattr(vr, "_bounded_link_target", lambda path, timeout=None: None)
    monkeypatch.setattr(
        vr, "_is_link_or_junction",
        lambda path: pytest.fail(f"direct lookup crossed custom mount: {path}")
        if path.startswith("/Users/me/mnt/photos") else False,
    )
    vr.seed_known_mount_roots({"/Users/me/mnt/photos"})

    assert vr.mount_root_candidates("/Users/me/mnt/photos/2026") == [
        "/Users/me/mnt/photos"
    ]


def test_linux_mount_table_roots_decode_custom_mount_path(tmp_path):
    mountinfo = tmp_path / "mountinfo"
    mountinfo.write_text(
        "41 29 0:35 / / rw,relatime - ext4 /dev/root rw\n"
        "57 41 0:52 / /srv/photo\\040archive rw - nfs server:/photos rw\n",
        encoding="utf-8",
    )

    assert vr._linux_mount_table_roots(str(mountinfo)) == {
        "/", "/srv/photo archive",
    }


def test_darwin_mount_table_roots_uses_metadata_command_not_mount_path():
    from types import SimpleNamespace

    calls = []

    def run(argv, **kwargs):
        calls.append((argv, kwargs))
        return SimpleNamespace(
            returncode=0,
            stdout=(
                "map auto_home on /System/Volumes/Data/home (autofs, automounted)\n"
                "server:/photos on /Users/me/mnt/photos (nfs)\n"
            ),
        )

    assert vr._darwin_mount_table_roots(run=run) == {
        "/System/Volumes/Data/home", "/Users/me/mnt/photos",
    }
    assert calls[0][0] == ["/sbin/mount"]
    assert calls[0][1]["timeout"] == vr.MOUNT_QUERY_TIMEOUT_SECS


def test_is_offline_error_classifies_volume_loss_not_missing_files():
    assert vr.is_offline_error(OSError(errno.ENOTCONN, "Socket is not connected"))
    assert vr.is_offline_error(OSError(errno.EIO, "Input/output error"))
    assert not vr.is_offline_error(OSError(errno.ENOENT, "No such file"))
    assert not vr.is_offline_error(OSError(errno.EACCES, "Permission denied"))
    assert not vr.is_offline_error(RuntimeError("not an OSError"))


@pytest.mark.parametrize("winerror", [53, 55, 59, 64, 67])
def test_is_offline_error_classifies_windows_network_loss(winerror):
    exc = OSError(errno.ENOENT, "translated network failure")
    exc.winerror = winerror
    assert vr.is_offline_error(exc)


def test_check_skips_probe_for_paths_without_mount_root(tmp_path):
    if sys.platform == "win32":
        pytest.skip("every absolute Windows path has a drive-letter candidate")
    probes = []
    gate = vr.VolumeReachability(probe=lambda root: probes.append(root) or True)
    assert gate.check(str(tmp_path)) == (None, True)
    assert probes == []


def test_check_caches_reachable_verdict_within_ttl():
    probes = []
    clock = _FakeClock()

    def probe(root):
        probes.append(root)
        return True

    gate = vr.VolumeReachability(ttl_seconds=30, probe=probe, clock=clock)
    assert gate.check("/Volumes/NAS/a") == ("/Volumes/NAS", True)
    assert gate.check("/Volumes/NAS/b") == ("/Volumes/NAS", True)
    assert probes == ["/Volumes/NAS"], "second check within TTL must not re-probe"
    clock.now += 31
    gate.check("/Volumes/NAS/c")
    assert probes == ["/Volumes/NAS", "/Volumes/NAS"], "expired verdict re-probes"


def test_check_reports_offline_root_and_caches_it_for_backoff_window():
    probes = []
    clock = _FakeClock()

    def probe(root):
        probes.append(root)
        return False

    gate = vr.VolumeReachability(offline_ttl_seconds=30, probe=probe, clock=clock)
    assert gate.check("/Volumes/NAS/a") == ("/Volumes/NAS", False)
    assert gate.check("/Volumes/NAS/a") == ("/Volumes/NAS", False)
    assert len(probes) == 1
    clock.now += 31
    assert gate.check("/Volumes/NAS/a") == ("/Volumes/NAS", False)
    assert len(probes) == 2


def test_mark_offline_short_circuits_subsequent_checks_without_probing():
    probes = []
    gate = vr.VolumeReachability(probe=lambda root: probes.append(root) or True)
    gate.mark_offline("/Volumes/NAS")
    assert gate.check("/Volumes/NAS/shoot") == ("/Volumes/NAS", False)
    assert probes == []


def test_alias_check_returns_resolved_mount_for_mid_walk_invalidation(monkeypatch):
    probes = []

    def candidates(path):
        if path.startswith("/mnt/archive"):
            return ["/mnt/archive", "/mnt/NAS"], True
        return ["/mnt/NAS"], True

    monkeypatch.setattr(vr, "mount_root_candidates_checked", candidates)
    gate = vr.VolumeReachability(probe=lambda root: probes.append(root) or True)
    mount_root, reachable = gate.check("/mnt/archive/photos")
    assert (mount_root, reachable) == ("/mnt/NAS", True)

    gate.mark_offline(mount_root)
    assert gate.check("/mnt/NAS/photos") == ("/mnt/NAS", False)
    assert probes == ["/mnt/archive", "/mnt/NAS"]


def test_mark_offline_ignores_missing_root():
    gate = vr.VolumeReachability(probe=lambda root: True)
    gate.mark_offline(None)
    gate.mark_offline("")
    assert gate.check("/Volumes/NAS/x") == ("/Volumes/NAS", True)


def test_probe_exception_is_treated_as_offline():
    def probe(root):
        raise RuntimeError("boom")

    gate = vr.VolumeReachability(probe=probe)
    assert gate.check("/mnt/nas/x") == ("/mnt/nas", False)


def test_clear_forgets_verdicts():
    calls = []
    gate = vr.VolumeReachability(probe=lambda root: calls.append(root) or True)
    gate.check("/mnt/nas/x")
    gate.clear()
    gate.check("/mnt/nas/x")
    assert len(calls) == 2


def test_generic_probe_times_out_as_offline(monkeypatch):
    import threading

    release = threading.Event()

    def slow_isdir(path):
        release.wait(synchronization_timeout(5))
        return True

    monkeypatch.setattr(vr.os.path, "isdir", slow_isdir)
    try:
        assert vr._probe_root_generic("/mnt/stuck", timeout=0.05) is False
    finally:
        release.set()


def test_generic_probe_returns_isdir_result(tmp_path, monkeypatch):
    monkeypatch.setattr(vr, "_MOUNT_BASELINE", {})
    (tmp_path / "photo.jpg").write_bytes(b"")
    assert vr._probe_root_generic(str(tmp_path), timeout=1) is True
    assert vr._probe_root_generic(str(tmp_path / "nope"), timeout=1) is False
    # A readable empty directory with no prior mount history is a valid root.
    (tmp_path / "empty").mkdir()
    assert vr._probe_root_generic(str(tmp_path / "empty"), timeout=1) is True


def test_app_reexports_probe_machinery_by_historical_names():
    """``app`` and ``pipeline_job`` keep their private aliases so existing
    call sites and tests continue to see one shared probe registry."""
    import app as app_module
    import pipeline_job

    assert app_module._network_root_reachable is vr.network_root_reachable
    assert app_module._NETWORK_PROBES is vr._NETWORK_PROBES
    assert app_module._NETWORK_PROBE_LOCK is vr._NETWORK_PROBE_LOCK
    assert pipeline_job._archive_mount_root_candidates is vr.mount_root_candidates


def test_generic_probe_reuses_wedged_thread_instead_of_stacking(monkeypatch):
    """A root whose ``isdir`` never returns must not accumulate one thread per
    check: while the first probe is alive, later checks fail closed at once."""
    import threading

    release = threading.Event()
    started = []

    def slow_isdir(path):
        started.append(path)
        release.wait(synchronization_timeout(5))
        return True

    monkeypatch.setattr(vr.os.path, "isdir", slow_isdir)
    monkeypatch.setattr(vr, "_GENERIC_PROBES", {})
    try:
        assert vr._probe_root_generic("/mnt/stuck", timeout=0.05) is False
        assert vr._probe_root_generic("/mnt/stuck", timeout=0.05) is False
        assert started == ["/mnt/stuck"], "second check must reuse the live probe"
        assert "/mnt/stuck" in vr._GENERIC_PROBES
        probe = vr._GENERIC_PROBES["/mnt/stuck"]
    finally:
        release.set()
    probe.join(1)
    assert "/mnt/stuck" not in vr._GENERIC_PROBES, "finished probe unregisters itself"


def test_generic_probe_global_cap_fails_closed(monkeypatch):
    import threading

    release = threading.Event()
    monkeypatch.setattr(vr.os.path, "isdir", lambda p: release.wait(synchronization_timeout(5)) or True)
    monkeypatch.setattr(vr, "_GENERIC_PROBES", {})
    monkeypatch.setattr(vr, "_MAX_GENERIC_PROBES", 2)
    try:
        assert vr._probe_root_generic("/mnt/a", timeout=0.02) is False
        assert vr._probe_root_generic("/mnt/b", timeout=0.02) is False
        assert vr._probe_root_generic("/mnt/c", timeout=0.02) is False
        assert set(vr._GENERIC_PROBES) == {"/mnt/a", "/mnt/b"}, "third root never spawned"
    finally:
        release.set()


def test_mount_root_candidates_follows_local_alias_without_touching_share(tmp_path, monkeypatch):
    """An alias into a share resolves to the share's mount root, but resolution
    stops at the mount-shaped prefix — nothing under it is ever stat'ed."""
    if sys.platform == "win32":
        pytest.skip("POSIX symlinks")
    alias = tmp_path / "photos"
    alias.symlink_to("/Volumes/NAS/photos")

    touched = []
    real_islink = os.path.islink

    def spy_islink(p):
        touched.append(str(p))
        return real_islink(p)

    monkeypatch.setattr(vr.os.path, "islink", spy_islink)
    monkeypatch.setattr(
        vr, "_bounded_link_target",
        lambda p, timeout=None: "/Volumes/NAS/photos" if str(p) == str(alias) else None,
    )
    assert vr.mount_root_candidates(str(alias / "2026" / "shoot")) == ["/Volumes/NAS"]
    assert not any(t.startswith("/Volumes/NAS") for t in touched), touched
    monkeypatch.setattr(vr.os.path, "realpath", lambda p: pytest.fail("realpath must not be used"))
    assert vr.mount_root_candidates(str(alias)) == ["/Volumes/NAS"]


def test_mount_root_candidates_symlink_loop_is_bounded(tmp_path):
    if sys.platform == "win32":
        pytest.skip("POSIX symlinks")
    (tmp_path / "a").symlink_to(tmp_path / "b")
    (tmp_path / "b").symlink_to(tmp_path / "a")
    assert vr.mount_root_candidates(str(tmp_path / "a" / "x")) == []


def test_mount_root_candidates_uses_only_bounded_link_lookups(monkeypatch):
    """All POSIX components are bounded; UNC prefixes are never inspected."""
    monkeypatch.setattr(vr, "_system_mount_roots", lambda: set())
    monkeypatch.setattr(vr, "_MOUNT_BASELINE", {})
    bounded = []
    monkeypatch.setattr(vr, "_bounded_link_target", lambda p, timeout=None: bounded.append(p))
    monkeypatch.setattr(vr.os.path, "realpath", lambda p: pytest.fail(f"realpath({p!r})"))
    direct = []
    real_islink = os.path.islink
    monkeypatch.setattr(vr.os.path, "islink", lambda p: direct.append(p) or real_islink(p))

    assert vr.mount_root_candidates("//server/share/photos/2026") == ["//server/share"]
    assert vr.mount_root_candidates("/Volumes/NAS/photos") == ["/Volumes/NAS"]
    assert vr.mount_root_candidates("/mnt/nas/photos") == ["/mnt/nas"]
    assert not any(p.startswith("//") for p in bounded), bounded
    assert set(bounded) == {"/Volumes", "/Volumes/NAS", "/mnt", "/mnt/nas"}
    assert direct == [], f"unbounded link lookup: {direct}"


def test_mount_resolution_never_probes_protected_bundle_components(monkeypatch):
    """Direct and symlink-expanded bundle paths stop before their TCC boundary."""
    monkeypatch.setattr(vr, "_system_mount_roots", lambda: set())
    monkeypatch.setattr(vr, "_MOUNT_BASELINE", {})
    alias = "/Users/julius/PhotoLib"
    bundle = "/Users/julius/Pictures/Photos Library.photoslibrary"
    probed = []

    def bounded_target(path, timeout=None):
        probed.append(path)
        assert ".photoslibrary" not in path.lower()
        return bundle if path == alias else None

    monkeypatch.setattr(vr, "_bounded_link_target", bounded_target)

    assert vr.mount_root_candidates(f"{bundle}/originals") == []
    assert vr.mount_root_candidates(f"{alias}/originals") == []
    assert alias in probed


def test_unknown_custom_root_timeout_fails_closed_without_direct_lookup(monkeypatch):
    monkeypatch.setattr(vr, "_system_mount_roots", lambda: set())
    monkeypatch.setattr(vr, "_MOUNT_BASELINE", {})
    monkeypatch.setattr(vr, "_LINK_TARGET_CACHE", {})
    bounded = []

    def bounded_target(path, timeout=None):
        bounded.append(path)
        return vr.INCONCLUSIVE if path == "/srv/photos" else None

    monkeypatch.setattr(vr, "_bounded_link_target", bounded_target)
    monkeypatch.setattr(
        vr, "_is_link_or_junction",
        lambda path: pytest.fail(f"direct link lookup: {path}"),
    )

    candidates = vr.mount_root_candidates("/srv/photos/2026")
    assert candidates == ["/srv/photos"]
    assert candidates.conclusive is False
    assert bounded == ["/srv", "/srv/photos"]
    assert vr.VolumeReachability().check("/srv/photos/2026") == (
        "/srv/photos", False,
    )


def test_record_known_mount_roots_serializes_concurrent_merges(tmp_path, monkeypatch):
    """Two catalog workers must not overwrite one another's mount evidence."""
    import threading
    import time

    from db import Database

    db_path = str(tmp_path / "test.db")
    first = Database(db_path)
    second = Database(db_path)
    original_load = vr.load_known_mount_roots

    def delayed_load(db):
        roots = original_load(db)
        # Before BEGIN IMMEDIATE guarded the read, this widened the race so
        # both workers read [] and the second UPSERT reliably lost one root.
        time.sleep(0.05)
        return roots

    monkeypatch.setattr(vr, "load_known_mount_roots", delayed_load)
    start = threading.Barrier(2)

    def record(db, root):
        start.wait()
        vr.record_known_mount_roots(db, {root: True})

    workers = [
        threading.Thread(target=record, args=(first, "/srv/photos")),
        threading.Thread(target=record, args=(second, "/archive/nas")),
    ]
    try:
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join(timeout=synchronization_timeout(5))
        assert not any(worker.is_alive() for worker in workers)
        assert original_load(first) == {"/srv/photos", "/archive/nas"}
    finally:
        first.close()
        second.close()


def test_mount_root_candidates_follows_mount_shaped_alias_to_real_mount(monkeypatch):
    """``/mnt/archive -> /mnt/NAS``: both the alias and the real mount are
    reported, so the pipeline's mounted-to-unmounted guards see the mount
    that can actually detach."""
    links = {"/mnt/archive": "/mnt/NAS"}
    monkeypatch.setattr(vr, "_bounded_link_target", lambda p, timeout=None: links.get(p))
    assert vr.mount_root_candidates("/mnt/archive/photos/2026") == ["/mnt/archive", "/mnt/NAS"]


def test_bounded_link_target_reads_local_symlink_and_times_out_on_wedged_mount(tmp_path, monkeypatch):
    if sys.platform == "win32":
        pytest.skip("POSIX symlinks")
    link = tmp_path / "archive"
    link.symlink_to("/mnt/NAS")
    assert vr._bounded_link_target(str(link)) == "/mnt/NAS"
    assert vr._bounded_link_target(str(tmp_path)) is None

    import threading
    release = threading.Event()
    monkeypatch.setattr(vr.os.path, "islink", lambda p: release.wait(synchronization_timeout(5)) or False)
    monkeypatch.setattr(vr, "_BOUNDED_LINK_PROBES", {})
    monkeypatch.setattr(vr, "_ABANDONED_LINK_PROBES", set())
    try:
        assert vr._bounded_link_target("/mnt/stuck", timeout=0.05) is vr.INCONCLUSIVE
        # Reused while alive: no second thread, immediate (inconclusive) answer.
        assert vr._bounded_link_target("/mnt/stuck", timeout=0.05) is vr.INCONCLUSIVE
        assert list(vr._BOUNDED_LINK_PROBES) == ["/mnt/stuck"]
    finally:
        release.set()


def test_bounded_link_target_keeps_filesystem_errors_inconclusive(monkeypatch):
    monkeypatch.setattr(vr, "_BOUNDED_LINK_PROBES", {})
    monkeypatch.setattr(vr, "_ABANDONED_LINK_PROBES", set())
    monkeypatch.setattr(vr, "_LINK_TARGET_CACHE", {})

    def fail(_path):
        raise OSError(errno.EIO, "offline")

    monkeypatch.setattr(vr.os.path, "islink", fail)

    assert vr._bounded_link_target("/srv/photos", timeout=1) is vr.INCONCLUSIVE
    assert "/srv/photos" not in vr._LINK_TARGET_CACHE


def test_resolver_skips_unc_server_prefix(monkeypatch):
    calls = []
    real = os.path.islink

    def spy(p):
        calls.append(str(p))
        return real(p)

    monkeypatch.setattr(vr.os.path, "islink", spy)
    monkeypatch.setattr(vr, "_bounded_link_target", lambda p, timeout=None: None)
    vr._resolve_symlinks_until_mount_shaped(
        "//server/share/photos",
        lambda p: "//x/y" if p.startswith("//") and len([q for q in p.split("/") if q]) >= 2 else None,
    )
    # ``os.path`` is process-global and daemon probe threads from earlier
    # tests may still be calling it, so assert only on UNC prefixes.
    unc = [c for c in calls if c.startswith("//")]
    assert unc == [], f"UNC server prefix was stat'ed: {unc}"


def test_generic_probe_recognises_detached_mount_stub(monkeypatch):
    """Linux keeps the mount-point directory after a share detaches. A root
    seen as a real mount once must read as offline when it later exists only
    as a plain directory; a root that was never a mount stays online."""
    monkeypatch.setattr(vr, "_MOUNT_BASELINE", {})
    monkeypatch.setattr(vr, "_GENERIC_PROBES", {})
    monkeypatch.setattr(vr.os.path, "isdir", lambda p: True)
    mounted = {"/mnt/NAS": True}
    monkeypatch.setattr(vr.os.path, "ismount", lambda p: mounted.get(p, False))
    monkeypatch.setattr(vr.os, "scandir", lambda p: _FakeScandir(["photo.jpg"]))

    assert vr._probe_root_generic("/mnt/NAS", timeout=1) is True
    assert vr._probe_root_generic("/mnt/photos", timeout=1) is True, "never a mount: plain dir is fine"
    mounted["/mnt/NAS"] = False  # share detached, stub directory remains
    assert vr._probe_root_generic("/mnt/NAS", timeout=1) is False
    assert vr._probe_root_generic("/mnt/photos", timeout=1) is True
    mounted["/mnt/NAS"] = True  # reconnected
    assert vr._probe_root_generic("/mnt/NAS", timeout=1) is True


def test_generic_probe_missing_directory_is_offline(monkeypatch):
    monkeypatch.setattr(vr, "_MOUNT_BASELINE", {})
    monkeypatch.setattr(vr, "_GENERIC_PROBES", {})
    monkeypatch.setattr(vr.os.path, "isdir", lambda p: False)
    monkeypatch.setattr(vr.os.path, "ismount", lambda p: pytest.fail("ismount on a missing dir"))
    assert vr._probe_root_generic("/mnt/gone", timeout=1) is False


def test_bounded_link_target_global_cap(monkeypatch):
    import threading

    release = threading.Event()
    monkeypatch.setattr(vr.os.path, "islink", lambda p: release.wait(synchronization_timeout(5)) or False)
    monkeypatch.setattr(vr, "_BOUNDED_LINK_PROBES", {})
    monkeypatch.setattr(vr, "_ABANDONED_LINK_PROBES", set())
    monkeypatch.setattr(vr, "_MAX_BOUNDED_LINK_PROBES", 2)
    try:
        assert vr._bounded_link_target("/mnt/a", timeout=0.02) is vr.INCONCLUSIVE
        assert vr._bounded_link_target("/mnt/b", timeout=0.02) is vr.INCONCLUSIVE
        assert vr._bounded_link_target("/mnt/c", timeout=0.02) is vr.INCONCLUSIVE
        assert set(vr._BOUNDED_LINK_PROBES) == {"/mnt/a", "/mnt/b"}, "third path never spawned"
    finally:
        release.set()


def test_bounded_link_target_waits_for_healthy_same_path_probe(monkeypatch):
    import threading
    import time

    release = threading.Event()
    started = threading.Event()
    calls = []

    def slow_islink(path):
        calls.append(path)
        started.set()
        assert release.wait(synchronization_timeout(2))
        return False

    monkeypatch.setattr(vr.os.path, "islink", slow_islink)
    monkeypatch.setattr(vr, "_BOUNDED_LINK_PROBES", {})
    monkeypatch.setattr(vr, "_ABANDONED_LINK_PROBES", set())
    monkeypatch.setattr(vr, "_LINK_TARGET_CACHE", {})
    results = []

    first = threading.Thread(
        target=lambda: results.append(vr._bounded_link_target("/home", timeout=1)),
    )
    second = threading.Thread(
        target=lambda: results.append(vr._bounded_link_target("/home", timeout=1)),
    )
    first.start()
    assert started.wait(synchronization_timeout(1))
    second.start()
    time.sleep(0.05)
    assert second.is_alive()
    assert calls == ["/home"]
    release.set()
    first.join(synchronization_timeout(2))
    second.join(synchronization_timeout(2))

    assert not first.is_alive() and not second.is_alive()
    assert results == [None, None]
    assert calls == ["/home"], "same-path waiter must reuse the completed outcome"


def test_saturated_link_probes_fall_back_to_cached_real_mount(monkeypatch):
    """Under saturation the alias's real mount must still be reported when an
    earlier conclusive answer exists — never silently downgraded to "plain
    directory" (which would let an import write into a detached stub)."""
    monkeypatch.setattr(vr, "_bounded_link_target", lambda p, timeout=None: vr.INCONCLUSIVE)
    # Earlier answers preserve full alias resolution, but a fresh timeout must
    # still make the current result inconclusive so synchronous guards abort.
    monkeypatch.setattr(
        vr, "_LINK_TARGET_CACHE",
        {"/mnt": None, "/mnt/archive": "/mnt/NAS", "/mnt/NAS": None},
    )
    cands, conclusive = vr.mount_root_candidates_checked("/mnt/archive/photos")
    assert cands == ["/mnt/archive", "/mnt/NAS"]
    assert conclusive is False
    # The local parent and alias are cached, but the real mount is not: the
    # real mount is still reported (so pipeline guards see it), while the
    # result is not confident because that prefix could not be inspected.
    monkeypatch.setattr(
        vr, "_LINK_TARGET_CACHE",
        {"/mnt": None, "/mnt/archive": "/mnt/NAS"},
    )
    cands, conclusive = vr.mount_root_candidates_checked("/mnt/archive/photos")
    assert cands == ["/mnt/archive", "/mnt/NAS"]
    assert conclusive is False


def test_inconclusive_prefix_without_cache_fails_closed_in_gate(monkeypatch):
    monkeypatch.setattr(vr, "_LINK_TARGET_CACHE", {})
    monkeypatch.setattr(vr, "_bounded_link_target", lambda p, timeout=None: vr.INCONCLUSIVE)
    cands, conclusive = vr.mount_root_candidates_checked("/mnt/archive/photos")
    assert cands == ["/mnt/archive"]
    assert conclusive is False
    probes = []
    gate = vr.VolumeReachability(probe=lambda root: probes.append(root) or True)
    assert gate.check("/mnt/archive/photos") == ("/mnt/archive", False)
    assert probes == [], "no point probing a root whose prefix could not be inspected"


def test_conclusive_link_answers_are_cached(tmp_path, monkeypatch):
    if sys.platform == "win32":
        pytest.skip("POSIX symlinks")
    monkeypatch.setattr(vr, "_LINK_TARGET_CACHE", {})
    monkeypatch.setattr(vr, "_BOUNDED_LINK_PROBES", {})
    link = tmp_path / "archive"
    link.symlink_to("/mnt/NAS")
    assert vr._bounded_link_target(str(link)) == "/mnt/NAS"
    assert vr._bounded_link_target(str(tmp_path)) is None
    assert {str(link): "/mnt/NAS", str(tmp_path): None} == vr._LINK_TARGET_CACHE


def test_generic_probe_cold_start_accepts_empty_unmounted_root(monkeypatch):
    """An empty readable directory is not evidence of a detached share.

    With no prior mounted baseline it may be a legitimate local root, so it
    stays online just like a populated unmounted directory.
    """
    monkeypatch.setattr(vr, "_MOUNT_BASELINE", {})
    monkeypatch.setattr(vr, "_GENERIC_PROBES", {})
    monkeypatch.setattr(vr.os.path, "isdir", lambda p: True)
    monkeypatch.setattr(vr.os.path, "ismount", lambda p: False)
    contents = {"/mnt/empty": [], "/mnt/photos": ["2026"]}
    monkeypatch.setattr(vr.os, "scandir", lambda p: _FakeScandir(contents[p]))

    assert vr._probe_root_generic("/mnt/empty", timeout=1) is True
    assert vr._MOUNT_BASELINE["/mnt/empty"] is False
    assert vr._probe_root_generic("/mnt/photos", timeout=1) is True
    assert vr._MOUNT_BASELINE["/mnt/photos"] is False


def test_generic_probe_rejects_empty_stub_from_persisted_mount_history(monkeypatch):
    """Durable evidence disambiguates a detached stub after process restart."""
    monkeypatch.setattr(vr, "_MOUNT_BASELINE", {})
    monkeypatch.setattr(vr, "_GENERIC_PROBES", {})
    monkeypatch.setattr(vr.os.path, "isdir", lambda p: True)
    monkeypatch.setattr(vr.os.path, "ismount", lambda p: False)
    monkeypatch.setattr(vr.os, "scandir", lambda p: _FakeScandir([]))

    vr.seed_known_mount_roots({"/mnt/NAS"})

    assert vr._probe_root_generic("/mnt/NAS", timeout=1) is False
    assert vr.was_observed_mounted("/mnt/NAS") is True


def test_generic_probe_accepts_empty_alias_before_resolved_mount(monkeypatch):
    """A healthy empty share remains reachable through an unmounted alias."""
    monkeypatch.setattr(vr, "_MOUNT_BASELINE", {})
    monkeypatch.setattr(vr, "_GENERIC_PROBES", {})
    monkeypatch.setattr(vr.os.path, "isdir", lambda p: True)
    monkeypatch.setattr(vr.os.path, "ismount", lambda p: p == "/mnt/NAS")
    monkeypatch.setattr(vr.os, "scandir", lambda p: _FakeScandir([]))
    monkeypatch.setattr(
        vr, "mount_root_candidates_checked",
        lambda p: (["/mnt/archive", "/mnt/NAS"], True),
    )
    gate = vr.VolumeReachability(
        probe=lambda root: vr._probe_root_generic(root, timeout=1),
    )

    assert gate.check("/mnt/archive/photos") == ("/mnt/NAS", True)
    assert vr._MOUNT_BASELINE == {"/mnt/archive": False, "/mnt/NAS": True}


def test_generic_probe_requires_readable_listing_even_when_mounted(monkeypatch):
    """A share still in the mount table but with a dead server: ``isdir`` and
    ``ismount`` succeed from cached metadata, the listing does not. Only the
    listing counts."""
    import errno

    monkeypatch.setattr(vr, "_MOUNT_BASELINE", {})
    monkeypatch.setattr(vr, "_GENERIC_PROBES", {})
    monkeypatch.setattr(vr.os.path, "isdir", lambda p: True)
    monkeypatch.setattr(vr.os.path, "ismount", lambda p: True)

    def dead_scandir(p):
        raise OSError(errno.EIO, "Input/output error", p)

    monkeypatch.setattr(vr.os, "scandir", dead_scandir)
    assert vr._probe_root_generic("/mnt/NAS", timeout=1) is False
    assert "/mnt/NAS" not in vr._MOUNT_BASELINE


def test_mount_root_candidates_carry_confidence_on_the_same_object(monkeypatch):
    """Candidates and confidence must come from one resolution: the returned
    list carries ``conclusive`` so callers cannot pair a truncated candidate
    list with a later, luckier confidence check."""
    monkeypatch.setattr(vr, "_LINK_TARGET_CACHE", {})
    monkeypatch.setattr(vr, "_bounded_link_target", lambda p, timeout=None: vr.INCONCLUSIVE)
    cands = vr.mount_root_candidates("/mnt/archive/photos")
    assert isinstance(cands, list) and cands == ["/mnt/archive"]
    assert cands.conclusive is False
    monkeypatch.setattr(vr, "_bounded_link_target", lambda p, timeout=None: None)
    assert vr.mount_root_candidates("/mnt/archive/photos").conclusive is True


def test_darwin_probe_requires_a_directory_listing():
    """``stat`` can answer from cached attributes on a dead SMB mount; the
    macOS probe now enumerates the root and accepts only a directory
    listing (``.``/``..`` present)."""
    import subprocess
    from types import SimpleNamespace

    calls = []

    def fake_run(argv, **kwargs):
        calls.append(argv)
        return SimpleNamespace(returncode=0, stdout=".\n..\nphotos\n")

    assert vr._probe_root_darwin("/Volumes/NAS", run=fake_run) is True
    assert calls[0][:3] == ["/bin/ls", "-1", "-f"] and calls[0][3] == "/Volumes/NAS"

    file_like = lambda argv, **k: SimpleNamespace(returncode=0, stdout="/Volumes/NAS\n")  # noqa: E731
    assert vr._probe_root_darwin("/Volumes/NAS", run=file_like) is False
    failed = lambda argv, **k: SimpleNamespace(returncode=1, stdout="")  # noqa: E731
    assert vr._probe_root_darwin("/Volumes/NAS", run=failed) is False

    def timed_out(argv, **kwargs):
        raise subprocess.TimeoutExpired(argv, kwargs.get("timeout"))

    assert vr._probe_root_darwin("/Volumes/NAS", run=timed_out) is False


def test_probe_root_uses_directory_read_on_darwin(monkeypatch):
    monkeypatch.setattr(vr.sys, "platform", "darwin")
    monkeypatch.setattr(vr, "_MOUNT_BASELINE", {})
    seen = []
    monkeypatch.setattr(vr, "_probe_root_darwin", lambda root, timeout=None: seen.append(root) or True)
    monkeypatch.setattr(vr, "network_root_reachable", lambda *a, **k: pytest.fail("stat-only probe must not decide"))
    assert vr.probe_root("/Volumes/NAS") is True
    assert seen == ["/Volumes/NAS"]


def test_probe_root_rejects_known_darwin_mount_missing_from_fresh_table(monkeypatch):
    monkeypatch.setattr(vr.sys, "platform", "darwin")
    monkeypatch.setattr(vr, "_MOUNT_BASELINE", {"/srv/photos": True})
    monkeypatch.setattr(vr, "_probe_root_darwin", lambda root, timeout=None: True)
    refreshes = []
    monkeypatch.setattr(
        vr, "_system_mount_roots",
        lambda force_refresh=False: refreshes.append(force_refresh) or set(),
    )

    assert vr.probe_root("/srv/photos") is False
    assert refreshes == [True]
    monkeypatch.setattr(
        vr, "_system_mount_roots",
        lambda force_refresh=False: {"/srv/photos"},
    )
    assert vr.probe_root("/srv/photos") is True


def test_bounded_process_probe_waits_for_healthy_same_root_probe(monkeypatch):
    import threading
    import time

    monkeypatch.setattr(vr, "_NETWORK_PROBES", {})
    monkeypatch.setattr(vr, "_ABANDONED_NETWORK_PROBES", set())
    first_started = threading.Event()
    release_first = threading.Event()
    created = []

    class Process:
        returncode = 0

        def __init__(self, first):
            self.first = first

        def communicate(self, timeout=None):
            if self.first:
                first_started.set()
                assert release_first.wait(synchronization_timeout(2))
            return "ok", ""

        def kill(self):
            return None

    def popen(*_args, **_kwargs):
        process = Process(not created)
        created.append(process)
        return process

    results = []

    def probe():
        results.append(vr._bounded_process_probe(
            ["probe"], "/srv/photos", 1,
            accept=lambda stdout: stdout == "ok", popen=popen,
        ))

    first = threading.Thread(target=probe)
    second = threading.Thread(target=probe)
    first.start()
    assert first_started.wait(synchronization_timeout(1))
    second.start()
    time.sleep(0.05)
    assert second.is_alive()
    assert len(created) == 1, "same-root contender must wait before spawning"
    release_first.set()
    first.join(synchronization_timeout(2))
    second.join(synchronization_timeout(2))

    assert results == [True, True]
    assert len(created) == 2


def test_abandoned_probe_wakes_same_root_waiter(monkeypatch):
    import threading
    import time

    root = "/srv/photos"
    release_reaper = threading.Event()

    class Process:
        def communicate(self):
            release_reaper.wait(synchronization_timeout(2))
            return "", ""

        def kill(self):
            return None

    process = Process()
    monkeypatch.setattr(vr, "_NETWORK_PROBES", {root: process})
    monkeypatch.setattr(vr, "_ABANDONED_NETWORK_PROBES", set())
    result = []
    started = threading.Event()

    def wait_for_root():
        started.set()
        before = time.monotonic()
        result.append((vr._reserve_network_probe(root, wait_timeout=1), time.monotonic() - before))

    waiter = threading.Thread(target=wait_for_root)
    waiter.start()
    assert started.wait(synchronization_timeout(1))
    time.sleep(0.05)
    try:
        vr._abandon_network_probe(root, process)
        waiter.join(0.5)
        assert not waiter.is_alive()
        assert result[0][0] is False
        assert result[0][1] < 0.5
    finally:
        release_reaper.set()


def test_record_known_mount_roots_commits_pending_implicit_transaction(tmp_path):
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    try:
        db.conn.execute(
            "INSERT INTO db_meta(key, value) VALUES (?, ?)",
            ("pending_before_mount_history", "preserved"),
        )
        assert db.conn.in_transaction

        vr.record_known_mount_roots(db, {"/srv/photos": True})

        assert not db.conn.in_transaction
        assert db.get_meta("pending_before_mount_history") == "preserved"
        assert vr.load_known_mount_roots(db) == {"/srv/photos"}
    finally:
        db.close()


def test_resolver_follows_junction_below_windows_drive_root(monkeypatch):
    """``C:\\Photos -> \\\\server\\share`` (a junction): the drive root is a
    traversal start, not a terminal mount, so the UNC share is reported."""
    links = {"C:/Photos": "\\\\?\\UNC\\server\\share"}
    monkeypatch.setattr(vr, "_is_link_or_junction", lambda p: p in links)
    monkeypatch.setattr(vr.os, "readlink", lambda p: links[p])
    monkeypatch.setattr(vr, "_bounded_link_target", lambda p, timeout=None: links.get(p))
    resolved = vr._resolve_symlinks_until_mount_shaped("C:/Photos/2026", vr._mount_shaped_candidate)
    assert resolved == "//server/share/2026"
    # End to end, with abspath neutralised (POSIX host running a Windows-shaped path).
    monkeypatch.setattr(vr.os.path, "abspath", lambda p: p)
    monkeypatch.setattr(vr.os.path, "normpath", lambda p: p)
    cands = vr.mount_root_candidates("C:/Photos/2026")
    assert cands == ["C:/", "//server/share"]
    assert cands.conclusive is True


def test_normalize_link_target_strips_windows_prefixes():
    assert vr._normalize_link_target("\\\\?\\UNC\\server\\share\\x") == "//server/share/x"
    assert vr._normalize_link_target("\\\\?\\D:\\archive") == "D:/archive"
    assert vr._normalize_link_target("/mnt/NAS") == "/mnt/NAS"


def test_resolver_stops_at_mapped_network_drive_without_touching_it(monkeypatch):
    """``Z:`` mapped to a share: the drive root is the mount, and no component
    below it may be inspected (an lstat there can block while the server is
    away). Local ``C:`` keeps being traversed."""
    monkeypatch.setattr(vr, "_drive_is_remote", lambda d: d.upper().startswith("Z:"))
    monkeypatch.setattr(vr, "_is_link_or_junction", lambda p: pytest.fail(f"lookup under mapped drive: {p}"))
    monkeypatch.setattr(vr, "_bounded_link_target", lambda p, timeout=None: pytest.fail(f"lookup under mapped drive: {p}"))
    assert vr._resolve_symlinks_until_mount_shaped("Z:/Photos/2026", vr._mount_shaped_candidate) == "Z:/Photos/2026"
    monkeypatch.setattr(vr.os.path, "abspath", lambda p: p)
    monkeypatch.setattr(vr.os.path, "normpath", lambda p: p)
    cands = vr.mount_root_candidates("Z:/Photos/2026")
    assert cands == ["Z:/"] and cands.conclusive is True


def test_drive_is_remote_is_false_off_windows():
    if sys.platform == "win32":
        pytest.skip("Windows asks the mount manager")
    assert vr._drive_is_remote("Z:") is False


def test_invalidate_caches_drops_verdicts_and_rereads_the_mount_table(monkeypatch):
    """The manual "Check again" path: both 30s caches that keep automatic
    polls off a dead share are dropped up front, so a share the user just
    remounted is probed again immediately."""
    gate = vr.get_shared()
    refreshes = []
    monkeypatch.setattr(
        vr, "_system_mount_roots",
        lambda **kwargs: refreshes.append(kwargs) or set(),
    )
    try:
        with gate._lock:
            gate._verdicts["/Volumes/NAS"] = (False, gate._clock())
        vr.invalidate_caches()
        assert gate._verdicts == {}
        assert refreshes == [{"force_refresh": True}]
    finally:
        gate.clear()


def test_clear_discards_a_probe_that_was_already_running():
    """A probe launched before ``clear`` must not publish its answer after it.

    A probe of a dead share takes seconds, so the manual recheck can land
    mid-probe. Publishing that pre-clear "offline" would make the fresh walk
    skip a volume the user has just remounted — the exact failure the recheck
    exists to prevent.
    """
    import threading

    started = threading.Event()
    release = threading.Event()
    probes = []

    def probe(root):
        probes.append(root)
        if len(probes) == 1:
            started.set()
            release.wait(synchronization_timeout(5))
            return False
        return True

    gate = vr.VolumeReachability(probe=probe)
    worker = threading.Thread(
        target=lambda: gate.root_reachable("/Volumes/NAS"), daemon=True,
    )
    worker.start()
    assert started.wait(synchronization_timeout(2))

    gate.clear()
    release.set()
    worker.join(synchronization_timeout(5))
    assert not worker.is_alive()

    # The in-flight answer was dropped rather than written back...
    assert gate._verdicts == {}
    # ...so the next caller probes again and sees the remounted volume.
    assert gate.root_reachable("/Volumes/NAS") is True
    assert probes == ["/Volumes/NAS", "/Volumes/NAS"]


class _WedgedProcess:
    """Killed probe still stuck in uninterruptible I/O: never exits until the
    test releases it, so ``poll()`` keeps reporting it as running."""

    def __init__(self):
        import threading
        self.released = threading.Event()

    def kill(self):
        pass

    def poll(self):
        return 0 if self.released.is_set() else None

    def communicate(self, timeout=None):
        self.released.wait(synchronization_timeout(10))
        return ("", "")


def test_invalidate_caches_lets_a_wedged_macos_probe_be_retried(monkeypatch):
    """A probe that timed out keeps its root registered so automatic polls
    fail fast. An explicit recheck must be able to probe anyway — otherwise
    "Check again" answers offline from the registration alone, without ever
    looking at the volume the user just remounted."""
    root = "/Volumes/WedgedNAS"
    process = _WedgedProcess()
    monkeypatch.setattr(vr, "_system_mount_roots", lambda **kwargs: set())
    try:
        with vr._NETWORK_PROBE_CONDITION:
            vr._NETWORK_PROBES[root] = process
        vr._abandon_network_probe(root, process)
        assert vr._reserve_network_probe(root) is False

        vr.invalidate_caches()

        assert vr._reserve_network_probe(root) is True
        # The wedged process is untracked as a blocker but still counts
        # against the global cap while it runs.
        assert process in vr._FORGOTTEN_NETWORK_PROBES
    finally:
        vr._release_network_probe(root, vr._NETWORK_PROBE_RESERVED)
        process.released.set()
        with vr._NETWORK_PROBE_CONDITION:
            vr._NETWORK_PROBES.pop(root, None)
            vr._ABANDONED_NETWORK_PROBES.discard(root)
            vr._FORGOTTEN_NETWORK_PROBES.discard(process)


def test_invalidate_caches_lets_a_wedged_generic_probe_be_retried(monkeypatch):
    """Same for the non-macOS thread probe: a live wedged thread makes every
    later check fail closed without probing."""
    import threading

    root = "/mnt/wedged"
    release = threading.Event()
    stuck = threading.Thread(target=lambda: release.wait(synchronization_timeout(10)), daemon=True)
    stuck.start()
    monkeypatch.setattr(vr, "_system_mount_roots", lambda **kwargs: set())
    monkeypatch.setattr(vr, "_classify_generic_root", lambda r: True)
    try:
        with vr._GENERIC_PROBE_LOCK:
            vr._GENERIC_PROBES[root] = stuck
        assert vr._probe_root_generic(root, 0.5) is False

        vr.invalidate_caches()

        assert vr._probe_root_generic(root, 2) is True
        assert stuck in vr._FORGOTTEN_GENERIC_PROBES
    finally:
        release.set()
        stuck.join(synchronization_timeout(5))
        with vr._GENERIC_PROBE_LOCK:
            vr._GENERIC_PROBES.pop(root, None)
            vr._FORGOTTEN_GENERIC_PROBES.discard(stuck)


def test_mark_offline_from_a_walk_that_predates_clear_is_dropped():
    """A walk that started before the manual recheck can surface its delayed
    ENOTCONN afterwards. Republishing it would undo the clear and send the
    next check straight past a volume the user has just remounted."""
    gate = vr.VolumeReachability(probe=lambda root: True)
    generation = gate.current_generation()

    gate.clear()
    gate.mark_offline("/Volumes/NAS", generation=generation)
    assert gate._verdicts == {}

    # An observation from a walk that started after the clear still counts,
    # and a caller with no generation to quote is unaffected.
    gate.mark_offline("/Volumes/NAS", generation=gate.current_generation())
    assert gate._verdicts["/Volumes/NAS"][0] is False
    gate.clear()
    gate.mark_offline("/Volumes/NAS")
    assert gate._verdicts["/Volumes/NAS"][0] is False


def test_a_probe_that_times_out_during_invalidation_does_not_block_the_recheck(
    monkeypatch,
):
    """The window a one-shot sweep cannot close: a probe still inside its
    timeout when the recheck lands, abandoning itself immediately after. It
    dates from before the remount, so it must not answer for the volume."""
    root = "/Volumes/LateNAS"
    process = _WedgedProcess()
    monkeypatch.setattr(vr, "_system_mount_roots", lambda **kwargs: set())
    epoch_at_probe_start = vr.current_invalidation_epoch()
    try:
        # The recheck runs while the probe is still in flight...
        vr.invalidate_caches()
        # ...and only then does the probe give up and register its root.
        with vr._NETWORK_PROBE_CONDITION:
            vr._NETWORK_PROBES[root] = process
        vr._abandon_network_probe(root, process, epoch_at_probe_start)

        assert vr._reserve_network_probe(root) is True
        assert process in vr._FORGOTTEN_NETWORK_PROBES
    finally:
        vr._release_network_probe(root, vr._NETWORK_PROBE_RESERVED)
        process.released.set()
        with vr._NETWORK_PROBE_CONDITION:
            vr._NETWORK_PROBES.pop(root, None)
            vr._ABANDONED_NETWORK_PROBES.discard(root)
            vr._ABANDONED_PROBE_EPOCHS.pop(root, None)
            vr._FORGOTTEN_NETWORK_PROBES.discard(process)


def test_a_generic_probe_started_before_the_recheck_does_not_block_it(monkeypatch):
    """Same window on the thread probe: one that was already running when the
    recheck landed cannot stand in for a look at the volume now."""
    import threading

    root = "/mnt/late"
    release = threading.Event()
    stuck = threading.Thread(target=lambda: release.wait(synchronization_timeout(10)), daemon=True)
    stuck.start()
    monkeypatch.setattr(vr, "_system_mount_roots", lambda **kwargs: set())
    monkeypatch.setattr(vr, "_classify_generic_root", lambda r: True)
    try:
        with vr._GENERIC_PROBE_LOCK:
            vr._GENERIC_PROBES[root] = stuck
            vr._GENERIC_PROBE_EPOCHS[root] = vr.current_invalidation_epoch()
        assert vr._probe_root_generic(root, 0.5) is False

        vr.invalidate_caches()

        assert vr._probe_root_generic(root, 2) is True
    finally:
        release.set()
        stuck.join(synchronization_timeout(5))
        with vr._GENERIC_PROBE_LOCK:
            vr._GENERIC_PROBES.pop(root, None)
            vr._GENERIC_PROBE_EPOCHS.pop(root, None)
            vr._FORGOTTEN_GENERIC_PROBES.discard(stuck)
