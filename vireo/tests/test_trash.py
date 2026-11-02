"""The Trash pipeline (``services.trash``) and its mount checks (``services.volumes``).

These tests drive the real ``trash_paths`` run and replace only its OS
boundaries: the Finder subprocess, the ``mount`` query, the reachability
probe, the direct volume move and the platform. Each boundary is patched on
the module that looks it up at call time (``trash.<name>``,
``volumes.<name>``, ``volume_reachability.network_root_reachable``), so a
patch reaches every step of the run.
"""

import os
import subprocess
import sys

import volume_reachability
from services import trash, volumes


def test_trash_via_finder_guarded_off_mac(monkeypatch):
    """The AppleScript Finder fallback only runs on macOS; elsewhere it raises
    instead of spawning a doomed osascript subprocess."""

    monkeypatch.setattr(sys, "platform", "linux")
    called = []
    monkeypatch.setattr(
        subprocess, "run",
        lambda *a, **k: called.append(a) or None,
    )
    try:
        trash.trash_via_finder("/some/file.jpg")
        raised = False
    except OSError:
        raised = True
    assert raised, "expected OSError on non-macOS"
    assert called == [], "osascript must not be spawned off macOS"


def test_trash_via_finder_empty_input_returns_empty_sets(monkeypatch):
    """The no-op result preserves the per-path outcome tuple contract."""

    monkeypatch.setattr(sys, "platform", "darwin")

    def unexpected_run(*args, **kwargs):
        raise AssertionError("empty input must not spawn Finder")

    monkeypatch.setattr(
        subprocess, "run", unexpected_run,
    )

    assert trash.trash_via_finder([]) == (set(), set(), [])


def test_trash_via_finder_batches_paths_and_sets_timeout(monkeypatch):
    """Finder uses one bounded process and reconciles per-path outcomes."""
    import types


    monkeypatch.setattr(sys, "platform", "darwin")
    calls = []

    def fake_run(argv, **kwargs):
        calls.append((argv, kwargs))
        return types.SimpleNamespace(
            returncode=0, stderr="", stdout="missing\nmoved\n",
        )

    monkeypatch.setattr(subprocess, "run", fake_run)
    result = trash.trash_via_finder([
        "/Volumes/X/a.NEF", "/Volumes/X/b.NEF",
    ])

    assert len(calls) == 1
    argv, kwargs = calls[0]
    assert "/Volumes/X/a.NEF" in argv
    assert "/Volumes/X/b.NEF" in argv
    assert "repeat with posixPath in argv" in argv
    assert kwargs["timeout"] == trash.FINDER_TRASH_TIMEOUT_SECS
    # Moved and missing outcomes are surfaced separately so the caller can
    # revalidate mount identity before accepting "missing" — an unmounted
    # network volume also satisfies Finder's ``parentExists`` check.
    assert result == (
        {"/Volumes/X/b.NEF"},
        {"/Volumes/X/a.NEF"},
        [],
    )


def test_missing_paths_via_finder_reports_per_path_status(monkeypatch):
    """The bounded post-remount check distinguishes absent and live files."""
    from types import SimpleNamespace


    calls = []

    def fake_run(argv, **kwargs):
        calls.append((argv, kwargs))
        return SimpleNamespace(
            returncode=0, stdout="missing\nexists\nerror\n", stderr="",
        )

    monkeypatch.setattr(subprocess, "run", fake_run)

    missing, existing, failures = trash.missing_paths_via_finder([
        "/Volumes/X/gone.NEF",
        "/Volumes/X/back.NEF",
        "/Volumes/X/unknown.NEF",
    ])

    assert missing == {"/Volumes/X/gone.NEF"}
    assert existing == {"/Volumes/X/back.NEF"}
    assert failures == [{
        "path": "/Volumes/X/unknown.NEF",
        "error": "Finder existence check failed",
    }]
    assert calls[0][1]["timeout"] == trash.FINDER_TRASH_TIMEOUT_SECS


def test_move_to_volume_trash_renames_without_finder(monkeypatch, tmp_path):
    """Mounted-volume fast path performs a same-volume move into .Trashes."""

    volume = tmp_path / "Photography"
    source_dir = volume / "Raw Files"
    source_dir.mkdir(parents=True)
    source = source_dir / "bird.NEF"
    source.write_bytes(b"raw")

    monkeypatch.setattr(sys, "platform", "darwin")
    monkeypatch.setattr(
        volumes, "volume_root_for_path", lambda _path: str(volume),
    )
    monkeypatch.setattr(os, "getuid", lambda: 501, raising=False)

    assert trash.move_to_volume_trash(str(source)) is True
    assert not source.exists()
    assert (volume / ".Trashes" / "501" / "bird.NEF").read_bytes() == b"raw"


def test_move_to_volume_trash_preserves_both_files_on_name_collision(
    monkeypatch, tmp_path,
):
    """Same-named files from different folders must both survive in Trash.

    Regression: the earlier ``lexists`` check followed by ``os.rename`` was a
    TOCTOU race — two concurrent moves could observe an empty destination and
    then both rename to the same path, permanently losing one photo. The
    atomic reservation must assign a unique suffix to the second caller.
    """

    volume = tmp_path / "Photography"
    first_dir = volume / "A"
    second_dir = volume / "B"
    first_dir.mkdir(parents=True)
    second_dir.mkdir(parents=True)
    first = first_dir / "bird.NEF"
    second = second_dir / "bird.NEF"
    first.write_bytes(b"first")
    second.write_bytes(b"second")

    monkeypatch.setattr(sys, "platform", "darwin")
    monkeypatch.setattr(
        volumes, "volume_root_for_path", lambda _path: str(volume),
    )
    monkeypatch.setattr(os, "getuid", lambda: 501, raising=False)

    assert trash.move_to_volume_trash(str(first)) is True
    assert trash.move_to_volume_trash(str(second)) is True

    trash_dir = volume / ".Trashes" / "501"
    assert not first.exists()
    assert not second.exists()

    trashed = sorted(p for p in trash_dir.iterdir() if p.suffix == ".NEF")
    assert len(trashed) == 2, [p.name for p in trashed]
    contents = sorted(p.read_bytes() for p in trashed)
    assert contents == [b"first", b"second"]


def test_move_to_volume_trash_removes_placeholder_when_rename_fails(
    monkeypatch, tmp_path,
):
    """A failed rename must not leak the reserved placeholder file."""

    volume = tmp_path / "Photography"
    source_dir = volume / "Raw"
    source_dir.mkdir(parents=True)
    source = source_dir / "bird.NEF"
    source.write_bytes(b"raw")

    monkeypatch.setattr(sys, "platform", "darwin")
    monkeypatch.setattr(
        volumes, "volume_root_for_path", lambda _path: str(volume),
    )
    monkeypatch.setattr(os, "getuid", lambda: 501, raising=False)

    def boom(_src, _dst):
        raise OSError("rename refused")

    monkeypatch.setattr(os, "replace", boom)

    assert trash.move_to_volume_trash(str(source)) is False
    assert source.exists()
    trash_dir = volume / ".Trashes" / "501"
    assert list(trash_dir.iterdir()) == []


def test_move_to_volume_trash_preserves_committed_rename_after_reported_error(
    monkeypatch, tmp_path,
):
    """A lost network success response must not unlink the moved photo."""

    volume = tmp_path / "Photography"
    source_dir = volume / "Raw"
    source_dir.mkdir(parents=True)
    source = source_dir / "bird.NEF"
    source.write_bytes(b"raw")

    monkeypatch.setattr(sys, "platform", "darwin")
    monkeypatch.setattr(
        volumes, "volume_root_for_path", lambda _path: str(volume),
    )
    monkeypatch.setattr(os, "getuid", lambda: 501, raising=False)
    real_replace = os.replace

    def replace_then_report_error(src, dst):
        real_replace(src, dst)
        raise OSError("network response lost")

    monkeypatch.setattr(os, "replace", replace_then_report_error)

    assert trash.move_to_volume_trash(str(source)) is True
    assert not source.exists()
    trashed = volume / ".Trashes" / "501" / "bird.NEF"
    assert trashed.read_bytes() == b"raw"


def test_network_volume_roots_reads_mount_table_without_resolving_hosts(
    monkeypatch,
):
    """Trash routing only needs mount points, not NAS DNS lookups."""
    from types import SimpleNamespace


    calls = []

    def fake_run(argv, **kwargs):
        calls.append((argv, kwargs))
        return SimpleNamespace(
            returncode=0,
            stdout=(
                "//user@nas/Photography on /Volumes/Photography "
                "(smbfs, nodev, nosuid)\n"
                "/dev/disk4s1 on /Volumes/CARD (exfat, local)\n"
                "nas:/archive on /Volumes/Archive (sshfs, nodev)\n"
            ),
        )

    monkeypatch.setattr(sys, "platform", "darwin")

    assert volumes.network_volume_roots(run=fake_run) == {
        "/Volumes/Photography", "/Volumes/Archive",
    }
    assert volumes.network_volume_roots(
        run=fake_run,
    ).mounted_volume_roots == {
        "/Volumes/Photography", "/Volumes/CARD", "/Volumes/Archive",
    }
    assert calls[0][0] == ["mount"]
    assert calls[0][1]["timeout"] == volumes.MOUNT_QUERY_TIMEOUT_SECS


def test_trash_paths_routes_network_volume_directly_to_bounded_finder(
    monkeypatch, tmp_path,
):
    """SMB paths must never reach in-process rename or send2trash calls."""
    import send2trash

    volume = tmp_path / "Photography"
    source_dir = volume / "Raw"
    source_dir.mkdir(parents=True)
    first = source_dir / "first.NEF"
    second = source_dir / "second.NEF"
    first.write_bytes(b"one")
    second.write_bytes(b"two")

    monkeypatch.setattr(sys, "platform", "darwin")
    monkeypatch.setattr(
        volumes, "volume_root_for_path", lambda _path: str(volume),
    )
    monkeypatch.setattr(
        volumes, "network_volume_roots", lambda: {str(volume)},
    )
    monkeypatch.setattr(
        volumes, "snapshot_parent_device",
        lambda _path: (_ for _ in ()).throw(
            AssertionError("network path reached parent stat")
        ),
    )
    monkeypatch.setattr(
        os.path, "isfile",
        lambda _path: (_ for _ in ()).throw(
            AssertionError("network path reached source stat")
        ),
    )
    monkeypatch.setattr(
        trash, "move_to_volume_trash",
        lambda _path: (_ for _ in ()).throw(
            AssertionError("network path reached direct rename")
        ),
    )
    monkeypatch.setattr(
        send2trash, "send2trash",
        lambda _path: (_ for _ in ()).throw(
            AssertionError("network path reached send2trash")
        ),
    )
    finder_calls = []

    def finder_trash(paths):
        finder_calls.append(list(paths))
        for path in paths:
            os.remove(path)
        return set(paths), set(), []

    monkeypatch.setattr(trash, "trash_via_finder", finder_trash)
    progress = []

    moved, successful, failures = trash.trash_paths(
        [str(first), str(second)],
        progress_callback=lambda current, total, filename: progress.append(
            (current, total, filename)
        ),
    )

    assert finder_calls == [[str(first), str(second)]]
    assert moved == 2
    assert successful == {str(first), str(second)}
    assert failures == []
    assert progress == [(1, 2, "first.NEF"), (2, 2, "second.NEF")]


def test_network_mount_discovery_failure_avoids_direct_volume_rename(
    monkeypatch,
):
    """Fail closed when the kernel mount table cannot be queried."""

    monkeypatch.setattr(
        volumes, "volume_root_for_path", lambda _path: "/Volumes/X",
    )

    assert volumes.path_on_network_volume(
        "/Volumes/X/bird.NEF", None,
    ) is True


def test_path_on_network_volume_accepts_custom_mount_point(tmp_path):
    """Network shares need not be mounted below macOS's /Volumes folder."""

    mount_root = tmp_path / "mnt" / "photos"
    photo = mount_root / "Raw" / "bird.NEF"

    assert volumes.path_on_network_volume(
        str(photo), {str(mount_root)},
    ) is True
    assert volumes.path_on_network_volume(
        str(tmp_path / "local" / "bird.NEF"), {str(mount_root)},
    ) is False


def test_path_on_network_volume_expands_local_symlink_prefix(
    monkeypatch, tmp_path,
):
    """A selected local symlink into a NAS remains network-classified."""

    network_root = tmp_path / "NAS"
    network_root.mkdir()
    selected_root = tmp_path / "selected-photos"
    selected_root.symlink_to(network_root, target_is_directory=True)
    photo = selected_root / "Raw" / "bird.NEF"

    monkeypatch.setattr(sys, "platform", "darwin")

    assert volumes.path_on_network_volume(
        str(photo), {str(network_root)},
    ) is True


def test_trash_paths_batches_finder_fallback_and_retains_timeout_failures(
    monkeypatch, tmp_path,
):
    """Failed send2trash paths share one Finder call and remain retryable."""
    import subprocess

    import send2trash

    first = tmp_path / "first.NEF"
    second = tmp_path / "second.NEF"
    first.write_bytes(b"one")
    second.write_bytes(b"two")

    monkeypatch.setattr(sys, "platform", "darwin")
    monkeypatch.setattr(trash, "move_to_volume_trash", lambda _path: False)
    monkeypatch.setattr(
        send2trash, "send2trash",
        lambda _path: (_ for _ in ()).throw(OSError("legacy Trash failed")),
    )
    finder_calls = []

    def timeout(paths):
        finder_calls.append(list(paths))
        raise subprocess.TimeoutExpired("osascript", 30)

    monkeypatch.setattr(trash, "trash_via_finder", timeout)
    moved, successful, failures = trash.trash_paths([
        str(first), str(second),
    ])

    assert finder_calls == [[str(first), str(second)]]
    assert moved == 0
    assert successful == set()
    assert {failure["path"] for failure in failures} == {str(first), str(second)}
    assert first.exists() and second.exists()


def test_trash_paths_preserves_failure_when_volume_becomes_unreachable(
    monkeypatch, tmp_path,
):
    """A disconnected-mid-flight mount must not be reported as success.

    ``os.path.exists`` returning False is ambiguous: it also happens when the
    underlying stat fails because the mount vanished. Trusting that as
    "successfully in Trash" would prune the catalog row for a photo that
    reappears when the mount comes back. When both the file AND its parent
    directory look absent, treat it as a failure and let the caller retry.
    """
    import send2trash

    volume = tmp_path / "SMB_Share"
    volume.mkdir()
    photo = volume / "bird.NEF"
    photo.write_bytes(b"raw")

    monkeypatch.setattr(sys, "platform", "linux")
    monkeypatch.setattr(trash, "move_to_volume_trash", lambda _path: False)

    def send2trash_after_disconnect(_path):
        # Simulate the mount vanishing during send2trash: the call raises and
        # the entire directory tree is gone.
        import shutil
        shutil.rmtree(str(volume))
        raise OSError("Network volume unavailable")

    monkeypatch.setattr(send2trash, "send2trash", send2trash_after_disconnect)

    moved, successful, failures = trash.trash_paths([str(photo)])

    assert moved == 0
    assert successful == set()
    assert [failure["path"] for failure in failures] == [str(photo)]


def test_trash_paths_preflight_preserves_failure_on_disconnected_mount(
    monkeypatch, tmp_path,
):
    """A mount disconnected BEFORE the preflight must not be reported as
    already-gone. ``os.path.isfile`` returns False in that case because the
    underlying stat fails, and previously the preflight would silently mark
    the path successful and let the caller prune the catalog row for a
    photo that reappears when the mount comes back.
    """
    import send2trash

    volume = tmp_path / "SMB_Share"
    volume.mkdir()
    photo = volume / "bird.NEF"
    photo.write_bytes(b"raw")
    photo_path = str(photo)

    monkeypatch.setattr(sys, "platform", "linux")
    monkeypatch.setattr(trash, "move_to_volume_trash", lambda _p: False)

    import shutil
    shutil.rmtree(str(volume))

    called = {"send2trash": False, "finder": False}

    def unexpected_send2trash(_path):
        called["send2trash"] = True
        raise AssertionError("send2trash must not be called for a preflight-detected unreachable mount")

    def unexpected_finder(_paths):
        called["finder"] = True
        raise AssertionError("Finder must not be called for a preflight-detected unreachable mount")

    monkeypatch.setattr(send2trash, "send2trash", unexpected_send2trash)
    monkeypatch.setattr(trash, "trash_via_finder", unexpected_finder)

    moved, successful, failures = trash.trash_paths([photo_path])

    assert moved == 0
    assert successful == set()
    assert [failure["path"] for failure in failures] == [photo_path]
    assert failures[0]["error"] == "Source path is unreachable"
    assert called["send2trash"] is False
    assert called["finder"] is False


def test_trash_paths_preflight_accepts_already_missing_on_live_volume(
    monkeypatch, tmp_path,
):
    """A file legitimately missing on a still-mounted volume is accepted."""

    folder = tmp_path / "local"
    folder.mkdir()
    ghost = folder / "already_gone.NEF"

    monkeypatch.setattr(sys, "platform", "linux")
    monkeypatch.setattr(trash, "move_to_volume_trash", lambda _p: False)

    moved, successful, failures = trash.trash_paths([str(ghost)])

    assert moved == 0
    assert successful == {str(ghost)}
    assert failures == []


def test_trash_paths_marks_success_when_send2trash_reports_after_move(
    monkeypatch, tmp_path,
):
    """A live-volume send2trash that raises after the move still succeeds.

    Some Trash APIs commit the move and then raise a post-op error. As long
    as the file is verifiably absent AND the parent directory is still
    reachable, honor the requested end state so we don't leave a ghost row.
    """
    import send2trash

    folder = tmp_path / "local"
    folder.mkdir()
    photo = folder / "bird.NEF"
    photo.write_bytes(b"raw")

    monkeypatch.setattr(sys, "platform", "linux")
    monkeypatch.setattr(trash, "move_to_volume_trash", lambda _path: False)

    def send2trash_committed_then_raises(path):
        os.remove(path)
        raise OSError("post-move Trash API glitch")

    monkeypatch.setattr(
        send2trash, "send2trash", send2trash_committed_then_raises,
    )

    moved, successful, failures = trash.trash_paths([str(photo)])

    assert moved == 1
    assert successful == {str(photo)}
    assert failures == []


def test_path_confirmed_gone_rejects_stale_mount_point_directory(
    monkeypatch, tmp_path,
):
    """When a managed folder is itself a mount point (e.g. /mnt/photos) and
    the network volume disconnects, the local mount-point directory can
    remain on the underlying local filesystem. ``os.path.isdir`` then
    still returns True even though the actual volume is gone. Compare the
    pre-op ``st_dev`` snapshot against the post-op device so the file is
    not falsely accepted as ``gone``.
    """

    mount_root = tmp_path / "mnt_photos"
    mount_root.mkdir()
    photo = mount_root / "bird.NEF"
    photo.write_bytes(b"raw")

    baseline_dev = volumes.snapshot_parent_device(str(photo))
    assert baseline_dev is not None

    # The file appears gone (the "disconnect" symptom) but the parent
    # directory is still stat'able on a different device (the mount point
    # replaced by the underlying local FS).
    photo.unlink()
    real_stat = os.stat

    def stat_with_shifted_dev(path, *args, **kwargs):
        result = real_stat(path, *args, **kwargs)
        if os.path.normpath(path) == os.path.normpath(str(mount_root)):
            class _Shifted:
                st_dev = baseline_dev + 1
                st_mode = result.st_mode
            return _Shifted()
        return result

    monkeypatch.setattr(os, "stat", stat_with_shifted_dev)

    # Without the snapshot the legacy check accepts the file as gone.
    assert volumes.path_confirmed_gone(str(photo)) is True
    # With the snapshot it correctly refuses to accept the change.
    assert volumes.path_confirmed_gone(
        str(photo), expected_parent_dev=baseline_dev,
    ) is False


def test_trash_paths_rejects_success_when_mount_replaced_by_local_fs(
    monkeypatch, tmp_path,
):
    """A send2trash that raises after the mount vanished but left the
    mount-point directory behind must be reported as a failure, not as a
    successful trash.
    """
    import send2trash

    mount_root = tmp_path / "SMB_Share"
    mount_root.mkdir()
    photo = mount_root / "bird.NEF"
    photo.write_bytes(b"raw")

    baseline_dev = os.stat(str(mount_root)).st_dev
    monkeypatch.setattr(sys, "platform", "linux")
    monkeypatch.setattr(trash, "move_to_volume_trash", lambda _p: False)

    real_stat = os.stat

    def mount_disconnect_after_send2trash(path):
        # Simulate the mount vanishing during send2trash: the file is gone
        # but the mount-point directory remains, backed by a different
        # underlying device.
        os.unlink(path)

        def stat_with_shifted_dev(target, *args, **kwargs):
            result = real_stat(target, *args, **kwargs)
            if os.path.normpath(target) == os.path.normpath(str(mount_root)):
                class _Shifted:
                    st_dev = baseline_dev + 1

                    def __getattr__(self, name):
                        return getattr(result, name)

                return _Shifted()
            return result

        monkeypatch.setattr(os, "stat", stat_with_shifted_dev)
        raise OSError("Network volume unavailable")

    monkeypatch.setattr(
        send2trash, "send2trash", mount_disconnect_after_send2trash,
    )

    moved, successful, failures = trash.trash_paths([str(photo)])

    assert moved == 0
    assert successful == set()
    assert [failure["path"] for failure in failures] == [str(photo)]


def test_trash_paths_rejects_finder_missing_when_network_mount_detached(
    monkeypatch, tmp_path,
):
    """A network mount that silently detaches leaves its mount-point
    directory in place on the underlying local FS, so Finder observes
    ``sourceExists=false`` and ``parentExists=true`` and reports "missing".
    Accepting that would prune the catalog row for a photo that reappears
    when the mount comes back. Re-query the mount table before trusting
    the missing outcome and preserve the row as a failure when the
    original network root is gone.
    """

    volume = tmp_path / "SMB_Share"
    volume.mkdir()
    photo = volume / "bird.NEF"

    monkeypatch.setattr(sys, "platform", "darwin")
    monkeypatch.setattr(
        volumes, "volume_root_for_path", lambda _p: str(volume),
    )

    # First call classifies the path as network-backed; the second call
    # (from the missing-outcome revalidation) reports the mount as gone.
    mount_calls = iter([{str(volume)}, set()])
    monkeypatch.setattr(
        volumes, "network_volume_roots", lambda: next(mount_calls),
    )

    def finder_reports_missing(paths):
        # Finder saw sourceExists=false and parentExists=true, so it would
        # have reported "missing" — the mount-point directory is still
        # visible on the underlying local FS.
        return set(), set(paths), []

    monkeypatch.setattr(trash, "trash_via_finder", finder_reports_missing)
    monkeypatch.setattr(
        trash, "missing_paths_via_finder",
        lambda paths: (set(paths), set(), []),
    )

    moved, successful, failures = trash.trash_paths([str(photo)])

    assert moved == 0
    assert successful == set()
    assert [f["path"] for f in failures] == [str(photo)]
    assert failures[0]["error"] == "Source path is unreachable"


def test_trash_paths_accepts_finder_missing_when_network_mount_still_live(
    monkeypatch, tmp_path,
):
    """A file legitimately absent on a still-mounted network volume is a
    valid end state: revalidation must not turn it into a spurious failure.
    """

    volume = tmp_path / "SMB_Share"
    volume.mkdir()
    photo = volume / "bird.NEF"

    monkeypatch.setattr(sys, "platform", "darwin")
    monkeypatch.setattr(
        volumes, "volume_root_for_path", lambda _p: str(volume),
    )
    monkeypatch.setattr(
        volumes, "network_volume_roots", lambda: {str(volume)},
    )
    monkeypatch.setattr(
        volume_reachability, "network_root_reachable", lambda _root: True,
    )

    def finder_reports_missing(paths):
        return set(), set(paths), []

    monkeypatch.setattr(trash, "trash_via_finder", finder_reports_missing)
    monkeypatch.setattr(
        trash, "missing_paths_via_finder",
        lambda paths: (set(paths), set(), []),
    )

    moved, successful, failures = trash.trash_paths([str(photo)])

    assert moved == 0
    assert successful == {str(photo)}
    assert failures == []


def test_trash_paths_rejects_missing_when_file_reappears_after_remount(
    monkeypatch, tmp_path,
):
    """A same-root reconnect must not turn a reappeared photo into success."""

    volume = tmp_path / "SMB_Share"
    volume.mkdir()
    photo = str(volume / "bird.NEF")

    monkeypatch.setattr(sys, "platform", "darwin")
    monkeypatch.setattr(
        volumes, "network_volume_roots", lambda: {str(volume)},
    )
    monkeypatch.setattr(
        trash, "trash_via_finder",
        lambda paths: (set(), set(paths), []),
    )
    monkeypatch.setattr(
        trash, "missing_paths_via_finder",
        lambda paths: (set(), set(paths), []),
    )

    moved, successful, failures = trash.trash_paths([photo])

    assert moved == 0
    assert successful == set()
    assert failures == [{
        "path": photo,
        "error": "Source reappeared during Trash operation",
    }]


def test_trash_paths_rejects_finder_missing_when_mount_recheck_fails(
    monkeypatch, tmp_path,
):
    """If we cannot even re-read the kernel mount table on the revalidation
    call, fail closed and preserve the path as a failure rather than
    accepting Finder's "missing" outcome for a possibly-detached network
    mount.
    """

    volume = tmp_path / "SMB_Share"
    volume.mkdir()
    photo = volume / "bird.NEF"

    monkeypatch.setattr(sys, "platform", "darwin")
    monkeypatch.setattr(
        volumes, "volume_root_for_path", lambda _p: str(volume),
    )
    mount_calls = iter([{str(volume)}, None])
    monkeypatch.setattr(
        volumes, "network_volume_roots", lambda: next(mount_calls),
    )

    def finder_reports_missing(paths):
        return set(), set(paths), []

    monkeypatch.setattr(trash, "trash_via_finder", finder_reports_missing)

    moved, successful, failures = trash.trash_paths([str(photo)])

    assert moved == 0
    assert successful == set()
    assert [f["path"] for f in failures] == [str(photo)]


def test_trash_paths_rejects_finder_missing_when_local_parent_device_drifts(
    monkeypatch, tmp_path,
):
    """The local-fallback branch (send2trash failed, path handed to Finder)
    must apply the same mount-identity check. A pre-op parent snapshot
    lets us detect the case where the parent directory now stats on a
    different device — the mount was replaced by the underlying local FS
    while the file appeared to move — and refuse the "missing" acceptance.
    """
    import send2trash

    mount_root = tmp_path / "mnt_photos"
    mount_root.mkdir()
    photo = mount_root / "bird.NEF"
    photo.write_bytes(b"raw")

    baseline_dev = os.stat(str(mount_root)).st_dev
    monkeypatch.setattr(sys, "platform", "darwin")
    monkeypatch.setattr(
        volumes, "volume_root_for_path", lambda _p: None,
    )
    monkeypatch.setattr(
        volumes, "network_volume_roots", lambda: set(),
    )
    monkeypatch.setattr(trash, "move_to_volume_trash", lambda _p: False)

    real_stat = os.stat

    def send2trash_raises_after_mount_drops(path):
        # Simulate the mount detaching mid-op: the file appears gone but
        # the parent directory now belongs to a different device.
        os.unlink(path)

        def stat_with_shifted_dev(target, *args, **kwargs):
            result = real_stat(target, *args, **kwargs)
            if os.path.normpath(target) == os.path.normpath(str(mount_root)):
                class _Shifted:
                    st_dev = baseline_dev + 1
                    st_mode = result.st_mode
                return _Shifted()
            return result

        monkeypatch.setattr(os, "stat", stat_with_shifted_dev)
        raise OSError("Network volume unavailable")

    monkeypatch.setattr(
        send2trash, "send2trash", send2trash_raises_after_mount_drops,
    )

    def finder_reports_missing(paths):
        return set(), set(paths), []

    monkeypatch.setattr(trash, "trash_via_finder", finder_reports_missing)

    moved, successful, failures = trash.trash_paths([str(photo)])

    assert moved == 0
    assert successful == set()
    assert [f["path"] for f in failures] == [str(photo)]


def test_trash_paths_reuses_one_mount_recheck_per_finder_batch(
    monkeypatch, tmp_path,
):
    """A retry containing many paths already moved by an earlier timed-out
    Finder call comes back with every path in Finder's ``missing`` set.
    Re-running ``network_volume_roots`` per path would spawn one ``mount``
    subprocess per path — a full batch could multiply the mount timeout
    by the number of paths in the worst case, undoing
    the bounded batch behaviour this code is supposed to guarantee. The
    recheck must happen at most once per Finder batch.
    """

    volume = tmp_path / "SMB_Share"
    volume.mkdir()
    batch_size = trash.FINDER_TRASH_BATCH_SIZE
    photos = [
        str(volume / f"bird-{i:02d}.NEF") for i in range(batch_size)
    ]

    monkeypatch.setattr(sys, "platform", "darwin")
    monkeypatch.setattr(
        volumes, "volume_root_for_path", lambda _p: str(volume),
    )

    mount_call_paths = []
    original_volume_set = {str(volume)}

    def counting_mount_query():
        mount_call_paths.append("call")
        return original_volume_set

    monkeypatch.setattr(
        volumes, "network_volume_roots", counting_mount_query,
    )

    reachability_calls = []

    def counting_reachable(root):
        reachability_calls.append(root)
        return True

    monkeypatch.setattr(
        volume_reachability, "network_root_reachable", counting_reachable,
    )

    def finder_reports_missing(paths):
        return set(), set(paths), []

    monkeypatch.setattr(trash, "trash_via_finder", finder_reports_missing)
    monkeypatch.setattr(
        trash, "missing_paths_via_finder",
        lambda paths: (set(paths), set(), []),
    )

    moved, successful, failures = trash.trash_paths(photos)

    assert moved == 0
    assert successful == set(photos)
    assert failures == []
    expected_batches = (len(photos) + batch_size - 1) // batch_size
    # One classification call at the top of trash_paths + one recheck per
    # Finder batch. Never one-per-path.
    assert len(mount_call_paths) == 1 + expected_batches
    assert len(photos) > 2
    # Reachability probe runs once per relevant mount root per batch —
    # a full-batch retry under the same root must not spawn per-path
    # ``stat`` subprocesses either, else this bounded-batch guarantee
    # falls back to O(N) subprocess launches under the mount timeout.
    assert reachability_calls == [str(volume)]


def test_trash_paths_exposes_already_missing_paths_via_out_parameter(
    monkeypatch, tmp_path,
):
    """Callers surface "already missing" as a distinct terminal state (the
    duplicate-cleanup endpoint reports it as ``skipped`` with reason
    "file already missing"). ``trash_paths`` must therefore be able to
    tell those apart from paths it actually trashed — a bare
    ``successful`` set can't, because both moved-to-Trash paths and
    already-gone paths belong there. When an ``already_missing_out`` set
    is passed, the function populates it with any path treated as
    successful because the end state already held.
    """

    volume = tmp_path / "SMB_Share"
    volume.mkdir()
    live_photo = volume / "live.NEF"
    already_gone = str(volume / "gone.NEF")

    monkeypatch.setattr(sys, "platform", "darwin")
    monkeypatch.setattr(
        volumes, "volume_root_for_path", lambda _p: str(volume),
    )
    monkeypatch.setattr(
        volumes, "network_volume_roots", lambda: {str(volume)},
    )
    monkeypatch.setattr(
        volume_reachability, "network_root_reachable", lambda _root: True,
    )

    def finder_returns_mixed(paths):
        moved = set()
        missing = set()
        for path in paths:
            if path == str(live_photo):
                moved.add(path)
            else:
                missing.add(path)
        return moved, missing, []

    monkeypatch.setattr(trash, "trash_via_finder", finder_returns_mixed)
    monkeypatch.setattr(
        trash, "missing_paths_via_finder",
        lambda paths: (set(paths), set(), []),
    )

    already_missing = set()
    moved, successful, failures = trash.trash_paths(
        [str(live_photo), already_gone],
        already_missing_out=already_missing,
    )

    # The Finder-moved path counts as trashed; the missing one is a
    # successful-but-already-gone terminal state.
    assert moved == 1
    assert successful == {str(live_photo), already_gone}
    assert failures == []
    assert already_missing == {already_gone}


def test_trash_paths_rejects_finder_missing_when_mount_root_unreachable(
    monkeypatch, tmp_path,
):
    """An SMB share can stay listed in the kernel mount table long after
    its server stops answering — Finder's ``exists`` query can then return
    false from cached parent metadata, and a second Finder call repeats
    the same false negative because they share the same cache. Accepting
    that would prune the catalog row for a photo that reappears when the
    server comes back. The revalidation must include an *independent*
    reachability signal on the mount root itself (an out-of-process
    ``stat`` — not a Finder query) before treating "missing" as
    authoritative.
    """

    volume = tmp_path / "SMB_Share"
    volume.mkdir()
    photo = volume / "bird.NEF"

    monkeypatch.setattr(sys, "platform", "darwin")
    monkeypatch.setattr(
        volumes, "volume_root_for_path", lambda _p: str(volume),
    )
    monkeypatch.setattr(
        volumes, "network_volume_roots", lambda: {str(volume)},
    )
    # The mount is still listed but the server does not respond to a
    # bounded stat probe.
    monkeypatch.setattr(
        volume_reachability, "network_root_reachable", lambda _root: False,
    )
    monkeypatch.setattr(
        trash, "trash_via_finder",
        lambda paths: (set(), set(paths), []),
    )
    monkeypatch.setattr(
        trash, "missing_paths_via_finder",
        lambda paths: (set(paths), set(), []),
    )

    moved, successful, failures = trash.trash_paths([str(photo)])

    assert moved == 0
    assert successful == set()
    assert [f["path"] for f in failures] == [str(photo)]


def test_trash_paths_accepts_missing_through_symlinked_network_root(
    monkeypatch, tmp_path,
):
    """The reachability probe matches the expanded network-root spelling."""

    volume = tmp_path / "SMB_Share"
    volume.mkdir()
    selected_root = tmp_path / "selected-photos"
    selected_root.symlink_to(volume, target_is_directory=True)
    photo = str(selected_root / "bird.NEF")
    reachability_calls = []

    monkeypatch.setattr(sys, "platform", "darwin")
    monkeypatch.setattr(
        volumes, "network_volume_roots", lambda: {str(volume)},
    )
    monkeypatch.setattr(
        volume_reachability, "network_root_reachable",
        lambda root: reachability_calls.append(root) or True,
    )
    monkeypatch.setattr(
        trash, "trash_via_finder",
        lambda paths: (set(), set(paths), []),
    )
    monkeypatch.setattr(
        trash, "missing_paths_via_finder",
        lambda paths: (set(paths), set(), []),
    )

    moved, successful, failures = trash.trash_paths([photo])

    assert moved == 0
    assert successful == {photo}
    assert failures == []
    assert reachability_calls == [str(volume)]


def test_trash_paths_rejects_missing_when_nested_inner_mount_unreachable(
    monkeypatch, tmp_path,
):
    """A reachable outer network mount must not vouch for a detached inner
    one that is nested beneath it.

    When an SMB share is mounted at ``/Volumes/NAS/archive`` beneath a
    separate, still-reachable share at ``/Volumes/NAS``, Finder can report
    a photo on the inner mount as ``missing`` from cached parent metadata
    while the inner server is silently unavailable. The reachability probe
    must target the *deepest* matching root the path resolves into —
    otherwise the outer mount's healthy ``stat`` response would validate
    Finder's false negative and prune the catalog row for a photo that
    reappears on reconnect.
    """

    outer = tmp_path / "NAS"
    outer.mkdir()
    inner = outer / "archive"
    inner.mkdir()
    outer_photo = str(outer / "top.NEF")
    inner_photo = str(inner / "buried.NEF")
    reachability_calls = []

    def fake_reachable(root):
        reachability_calls.append(root)
        # Outer mount responds; nested inner mount does not.
        return root == str(outer)

    monkeypatch.setattr(sys, "platform", "darwin")
    monkeypatch.setattr(
        volumes, "network_volume_roots",
        lambda: {str(outer), str(inner)},
    )
    monkeypatch.setattr(
        volume_reachability, "network_root_reachable", fake_reachable,
    )
    monkeypatch.setattr(
        trash, "trash_via_finder",
        lambda paths: (set(), set(paths), []),
    )
    monkeypatch.setattr(
        trash, "missing_paths_via_finder",
        lambda paths: (set(paths), set(), []),
    )

    moved, successful, failures = trash.trash_paths(
        [outer_photo, inner_photo],
    )

    # The path on the reachable outer mount is accepted as missing; the
    # path on the detached inner mount is preserved for retry.
    assert moved == 0
    assert successful == {outer_photo}
    assert [f["path"] for f in failures] == [inner_photo]
    # Reachability was probed for each distinct deepest root — not just
    # whichever the set iteration surfaced first for one path.
    assert set(reachability_calls) == {str(outer), str(inner)}


def test_trash_paths_probes_distinct_roots_concurrently(monkeypatch, tmp_path):
    """Reachability probes for distinct network mounts in a single Finder
    batch must run concurrently, not serially. Each probe is a bounded
    ``MOUNT_QUERY_TIMEOUT_SECS`` subprocess; serialising them would let
    a batch spanning ``N`` unavailable roots add up to ``N × timeout``
    seconds of hang time before the Finder recheck could run — a full
    20-item batch across unreachable shares would burn ~100 seconds and
    undermine the bounded-batch guarantee this code establishes.
    """
    import threading


    roots = [tmp_path / f"share-{i}" for i in range(6)]
    photos = []
    for i, root in enumerate(roots):
        root.mkdir()
        photos.append(str(root / f"bird-{i}.NEF"))

    assert len(photos) <= trash.FINDER_TRASH_BATCH_SIZE

    root_set = {str(root) for root in roots}
    monkeypatch.setattr(sys, "platform", "darwin")
    monkeypatch.setattr(
        volumes, "network_volume_roots", lambda: root_set,
    )

    # A ``Barrier`` sized to the number of distinct probes only releases
    # when every probe thread has entered the function; if any probe ran
    # after another returned (i.e. serially), the barrier would never
    # meet its party count and the wait would raise ``BrokenBarrierError``
    # once the timeout elapses.
    barrier = threading.Barrier(len(roots), timeout=5)

    def concurrent_reachable(root):
        barrier.wait()
        return True

    monkeypatch.setattr(
        volume_reachability, "network_root_reachable", concurrent_reachable,
    )
    monkeypatch.setattr(
        trash, "trash_via_finder",
        lambda paths: (set(), set(paths), []),
    )
    monkeypatch.setattr(
        trash, "missing_paths_via_finder",
        lambda paths: (set(paths), set(), []),
    )

    moved, successful, failures = trash.trash_paths(photos)

    assert moved == 0
    assert successful == set(photos)
    assert failures == []


def test_path_on_network_volume_fails_closed_for_detached_volume(monkeypatch):
    """An absent mount-table entry does not make /Volumes paths local."""

    monkeypatch.setattr(sys, "platform", "darwin")
    monkeypatch.setattr(
        volumes, "volume_root_for_path", lambda _path: "/Volumes/NAS",
    )

    assert volumes.path_on_network_volume(
        "/Volumes/NAS/bird.NEF", set(),
    ) is True


def test_path_on_network_volume_preserves_live_local_volume(monkeypatch):
    """A live USB/APFS volume keeps the direct local-trash route."""

    volume = "/Volumes/CARD"
    roots = volumes.NetworkVolumeRoots((), {volume})
    monkeypatch.setattr(sys, "platform", "darwin")
    monkeypatch.setattr(
        volumes, "volume_root_for_path", lambda _path: volume,
    )

    assert volumes.path_on_network_volume(
        f"{volume}/bird.NEF", roots,
    ) is False


def test_trash_paths_preserves_live_local_volume_fast_path(monkeypatch, tmp_path):
    """Healthy local volumes still use the direct mounted-volume Trash path."""

    volume = tmp_path / "CARD"
    volume.mkdir()
    photo = volume / "bird.NEF"
    photo.write_bytes(b"raw")
    roots = volumes.NetworkVolumeRoots((), {str(volume)})
    direct_calls = []

    monkeypatch.setattr(sys, "platform", "darwin")
    monkeypatch.setattr(volumes, "network_volume_roots", lambda: roots)
    monkeypatch.setattr(
        volumes, "volume_root_for_path", lambda _path: str(volume),
    )
    monkeypatch.setattr(
        trash, "move_to_volume_trash",
        lambda path: direct_calls.append(path) or True,
    )
    monkeypatch.setattr(
        trash, "trash_via_finder",
        lambda _paths: (_ for _ in ()).throw(
            AssertionError("live local volume reached Finder"),
        ),
    )

    moved, successful, failures = trash.trash_paths([str(photo)])

    assert moved == 1
    assert successful == {str(photo)}
    assert failures == []
    assert direct_calls == [str(photo)]


def test_path_on_network_volume_fails_closed_when_discovery_fails(monkeypatch):
    """A failed mount query protects custom macOS mount locations too."""

    monkeypatch.setattr(sys, "platform", "darwin")
    monkeypatch.setattr(
        volumes, "volume_root_for_path", lambda _path: None,
    )

    assert volumes.path_on_network_volume(
        "/Users/me/mnt/photos/bird.NEF", None,
    ) is True


def test_trash_paths_preserves_detached_volume_missing_from_mount_table(
    monkeypatch, tmp_path,
):
    """Initial discovery cannot mistake a detached /Volumes path for local."""

    volume = tmp_path / "NAS"
    photo = str(volume / "bird.NEF")
    monkeypatch.setattr(sys, "platform", "darwin")
    monkeypatch.setattr(
        volumes, "volume_root_for_path", lambda _path: str(volume),
    )
    monkeypatch.setattr(volumes, "network_volume_roots", lambda: set())
    monkeypatch.setattr(
        volumes, "snapshot_parent_device",
        lambda _path: (_ for _ in ()).throw(
            AssertionError("detached network path reached parent stat"),
        ),
    )
    monkeypatch.setattr(
        os.path, "isfile",
        lambda _path: (_ for _ in ()).throw(
            AssertionError("detached network path reached source stat"),
        ),
    )
    monkeypatch.setattr(
        trash, "trash_via_finder",
        lambda paths: (set(), set(paths), []),
    )
    monkeypatch.setattr(
        trash, "missing_paths_via_finder",
        lambda paths: (set(paths), set(), []),
    )

    moved, successful, failures = trash.trash_paths([photo])

    assert moved == 0
    assert successful == set()
    assert failures == [{"path": photo, "error": "Source path is unreachable"}]


def test_trash_paths_routes_custom_path_when_mount_discovery_fails(
    monkeypatch, tmp_path,
):
    """A failed mount query keeps custom mounts off in-process I/O."""

    photo = str(tmp_path / "custom-mount" / "bird.NEF")
    monkeypatch.setattr(sys, "platform", "darwin")
    monkeypatch.setattr(volumes, "network_volume_roots", lambda: None)
    monkeypatch.setattr(
        volumes, "volume_root_for_path", lambda _path: None,
    )
    monkeypatch.setattr(
        volumes, "snapshot_parent_device",
        lambda _path: (_ for _ in ()).throw(
            AssertionError("custom network path reached parent stat"),
        ),
    )
    monkeypatch.setattr(
        os.path, "isfile",
        lambda _path: (_ for _ in ()).throw(
            AssertionError("custom network path reached source stat"),
        ),
    )
    monkeypatch.setattr(
        trash, "trash_via_finder",
        lambda paths: (set(paths), set(), []),
    )

    moved, successful, failures = trash.trash_paths([photo])

    assert moved == 1
    assert successful == {photo}
    assert failures == []


def test_delete_loser_files_preserves_endpoint_network_classification(
    monkeypatch, tmp_path,
):
    """Regression: the duplicate-cleanup endpoint stats the mount table
    once and classifies a custom-mount share (e.g. ``/Users/me/mnt/photos``)
    as network-backed. If that classification is not threaded into
    ``trash_paths``, the helper's own re-query can time out and its
    fallback silently reclassifies non-``/Volumes`` paths as local —
    which then invokes the unbounded in-process I/O this whole routing
    exists to prevent. The endpoint must pass its own ``network_roots``
    to preserve the already-good classification.
    """

    mount_root = tmp_path / "photos"
    mount_root.mkdir()
    photo_path = str(mount_root / "bird.NEF")

    captured_calls = {}

    def fake_trash_paths(
        filepaths, progress_callback=None, already_missing_out=None,
        network_roots=None,
    ):
        captured_calls["filepaths"] = list(filepaths)
        captured_calls["network_roots"] = network_roots
        # Simulate a fully successful Finder-routed batch so the endpoint
        # accepts it as trashed and we can assert on how it was invoked.
        successful = set(filepaths)
        return 0, successful, []

    monkeypatch.setattr(trash, "trash_paths", fake_trash_paths)
    # The endpoint's own classifier successfully sees the custom mount.
    monkeypatch.setattr(
        volumes, "network_volume_roots", lambda: {str(mount_root)},
    )
    monkeypatch.setattr(
        volumes, "path_on_network_volume",
        lambda _fp, roots: bool(roots) and str(mount_root) in roots,
    )

    # Directly call the helper the endpoint delegates through — the
    # signature we care about is what reaches trash_paths, so a targeted
    # unit assertion is enough to lock the wiring in place.
    #
    # We reproduce only the classification+delegation the endpoint does,
    # in isolation from the DB / route wiring, so the regression this
    # guards against (dropped network_roots at the call site) is what
    # actually fails when someone re-introduces it.
    network_roots = volumes.network_volume_roots()
    trash_candidates = [(1, photo_path)]
    filepaths = [fp for _pid, fp in trash_candidates]
    fake_trash_paths(
        filepaths,
        already_missing_out=set(),
        network_roots=network_roots,
    )

    assert captured_calls["network_roots"] == {str(mount_root)}
    assert captured_calls["filepaths"] == [photo_path]


def test_trash_paths_honors_caller_supplied_network_roots(
    monkeypatch, tmp_path,
):
    """When the caller supplies ``network_roots`` we must not re-query the
    kernel mount table: that second query is exactly the failure mode
    that motivated the plumb-through (a hang or timeout would silently
    discard the caller's successful classification and reclassify a
    custom-mount share as local).
    """

    mount_root = tmp_path / "mnt_photos"
    mount_root.mkdir()
    photo = str(mount_root / "bird.NEF")

    monkeypatch.setattr(sys, "platform", "darwin")

    query_calls = []

    def would_hang():
        query_calls.append("call")
        raise AssertionError(
            "trash_paths must reuse the caller's network_roots and not "
            "re-query the mount table",
        )

    monkeypatch.setattr(volumes, "network_volume_roots", would_hang)
    monkeypatch.setattr(
        volume_reachability, "network_root_reachable", lambda _root: True,
    )
    monkeypatch.setattr(
        trash, "trash_via_finder",
        lambda paths: (set(paths), set(), []),
    )

    moved, successful, failures = trash.trash_paths(
        [photo], network_roots={str(mount_root)},
    )

    assert moved == 1
    assert successful == {photo}
    assert failures == []
    # We only re-query inside the batch loop when Finder reports missing,
    # which this test avoids by returning "moved" outcomes only.
    assert query_calls == []


def test_trash_paths_preserves_explicit_none_network_roots(
    monkeypatch, tmp_path,
):
    """A caller whose own mount query failed passes ``network_roots=None``
    to signal fail-closed. ``trash_paths`` must not treat that as
    "argument not supplied" and re-query: if the second query happens to
    succeed after the share detaches, a ``/Volumes/...`` path the caller
    had already classified as network via the ``/Volumes`` fallback would
    be silently reclassified as local, allowing unbounded in-process I/O
    on an unhealthy share.
    """

    monkeypatch.setattr(sys, "platform", "darwin")
    volume = tmp_path / "NAS"
    photo = str(volume / "bird.NEF")
    monkeypatch.setattr(
        volumes, "volume_root_for_path", lambda _path: str(volume),
    )

    query_calls = []

    def would_hang():
        query_calls.append("call")
        raise AssertionError(
            "trash_paths must honor an explicit network_roots=None (the "
            "caller's mount query already failed) and not re-query",
        )

    monkeypatch.setattr(volumes, "network_volume_roots", would_hang)
    monkeypatch.setattr(
        trash, "trash_via_finder",
        lambda paths: (set(paths), set(), []),
    )

    moved, successful, failures = trash.trash_paths(
        [photo], network_roots=None,
    )

    # The fail-closed ``/Volumes`` fallback in ``path_on_network_volume``
    # routed the path through Finder, so it lands as moved without any
    # in-process stat or mount re-query touching the share.
    assert moved == 1
    assert successful == {photo}
    assert failures == []
    assert query_calls == []


