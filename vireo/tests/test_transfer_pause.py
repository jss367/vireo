"""Pause transfers only after all writers stop; reverify before deleting."""

import os
import shutil
import sys
import threading

import pytest
from db import Database
from job_runner.subprocess_control import run_controlled_process
from job_runner.work_control import SerializedWork, WorkControl
from jobs import JobRunner
from services import photo_ingest, workspace_folders
from testing.waits import synchronization_timeout
from wait import wait_for_job, wait_for_job_via_runner


@pytest.fixture
def transfer_env(tmp_path, monkeypatch):
    import move.folder_transfer as transfer
    import power

    monkeypatch.setattr(power, "start_platform_inhibitor", lambda reason: None)
    # Exercise the real per-file copy/verify/catalog/delete path independently
    # of which rsync binary happens to be installed on the test machine.
    def no_rsync(*args, **kwargs):
        raise FileNotFoundError()

    monkeypatch.setattr(transfer, "_run_rsync_streamed", no_rsync)
    db_path = str(tmp_path / "library.db")
    source = tmp_path / "originals"
    source.mkdir()
    with Database(db_path) as db:
        workspace_id = db.require_workspace_id()
        folder_id = workspace_folders.add_folder(db, str(source))
        for name, content in (("a.jpg", b"first photo"), ("b.jpg", b"second photo")):
            path = source / name
            path.write_bytes(content)
            photo_ingest.add_photo(
                db, folder_id=folder_id, filename=name, extension=".jpg",
                file_size=path.stat().st_size, file_mtime=path.stat().st_mtime,
            )
    runner = JobRunner()
    destination = tmp_path / "archive"
    destination.mkdir()
    yield runner, db_path, workspace_id, folder_id, source, destination
    runner.shutdown(timeout=synchronization_timeout(5))


def start_move(env, *, lock=None, verify_contents=True):
    from move import move_folder

    runner, db_path, workspace_id, folder_id, source, destination = env

    def work(job):
        with Database(db_path) as db:
            db.set_active_workspace(workspace_id)
            with SerializedWork(runner, job["id"], lock) as control:
                result = move_folder(
                    db, folder_id, str(destination), control=control,
                    verify_contents=verify_contents,
                )
                return {**result, "ok": not result.get("errors")}

    return runner.start("move-folder", work, workspace_id=workspace_id)


def wait_status(runner, job_id, status):
    return wait_for_job(lambda: runner.get(job_id), terminal=(status,), timeout=5)


@pytest.mark.parametrize("action", ["resume", "cancel", "collision"])
def test_folder_copy_pause_releases_lock_and_preserves_originals(transfer_env, monkeypatch, action):
    import move.folder_transfer as transfer

    runner, db_path, workspace_id, folder_id, source, destination = transfer_env
    entered, release = threading.Event(), threading.Event()
    original = transfer._copy_tree_with_progress
    copied = []

    def copy_with_boundary(src, dst, skip, total, progress, **kwargs):
        def report(current, count, name, phase):
            copied.append(name)
            if len(copied) == 1:
                entered.set()
                assert release.wait(synchronization_timeout(5))
            progress(current, count, name, phase)

        return original(src, dst, skip, total, report, **kwargs)

    monkeypatch.setattr(transfer, "_copy_tree_with_progress", copy_with_boundary)
    lock = threading.Lock()
    job_id = start_move(transfer_env, lock=lock)
    try:
        assert entered.wait(synchronization_timeout(5))
        assert runner.pause_job(job_id)
        release.set()
        wait_status(runner, job_id, "paused")
        assert lock.acquire(blocking=False), "paused transfer retained the batch lock"
        lock.release()
        assert len(copied) == 1
        other_file = destination / source.name / "another-transfer.jpg"
        other_file.write_bytes(b"another transfer's only copy")
        assert (source / "a.jpg").exists() and (source / "b.jpg").exists()
        with Database(db_path) as db:
            assert db.folders.get(folder_id)["path"] == str(source)
        if action == "collision":
            outstanding = ({"a.jpg", "b.jpg"} - set(copied)).pop()
            conflicting = destination / source.name / outstanding
            conflicting.write_bytes(b"another transfer's photo")
            assert runner.resume_job(job_id)
            job = wait_for_job_via_runner(runner, job_id)
            assert job["status"] == "failed", job
            assert conflicting.read_bytes() == b"another transfer's photo"
            assert source.exists()
        elif action == "resume":
            assert runner.resume_job(job_id)
            job = wait_for_job_via_runner(runner, job_id)
            assert job["status"] == "completed", job
            assert not source.exists()
            assert (destination / source.name / "a.jpg").read_bytes() == b"first photo"
            assert (destination / source.name / "b.jpg").read_bytes() == b"second photo"
        else:
            assert runner.cancel_job(job_id)
            job = wait_for_job_via_runner(runner, job_id)
            assert job["status"] == "cancelled", job
            assert len(copied) == 1
            assert source.exists()
        assert other_file.read_bytes() == b"another transfer's only copy"
    finally:
        release.set()
        runner.cancel_job(job_id)


def test_failed_fresh_verification_after_pause_preserves_other_files(transfer_env, monkeypatch):
    import move.folder_transfer as transfer

    runner, _, _, _, source, destination = transfer_env
    entered, release = threading.Event(), threading.Event()
    original = transfer._fresh_copy_mismatch

    def verify(src, dst, progress):
        def report(current, name):
            if not entered.is_set():
                entered.set()
                assert release.wait(synchronization_timeout(5))
            progress(current, name)

        return original(src, dst, progress=report)

    monkeypatch.setattr(transfer, "_fresh_copy_mismatch", verify)
    job_id = start_move(transfer_env, verify_contents=False)
    try:
        assert entered.wait(synchronization_timeout(5))
        assert runner.pause_job(job_id)
        release.set()
        wait_status(runner, job_id, "paused")
        other = destination / source.name / "another-transfer.jpg"
        other.write_bytes(b"another transfer's only copy")
        assert runner.resume_job(job_id)
        job = wait_for_job_via_runner(runner, job_id)
        assert job["status"] == "failed", job
        assert other.read_bytes() == b"another transfer's only copy"
        assert source.exists()
    finally:
        release.set()
        runner.cancel_job(job_id)


@pytest.mark.parametrize("change", ["corrupt_copy", "move_source"])
def test_pause_during_verification_invalidates_earlier_checks(transfer_env, monkeypatch, change):
    import move.folder_transfer as transfer

    runner, db_path, _, folder_id, source, destination = transfer_env
    checked, release = threading.Event(), threading.Event()
    original = transfer._first_missing_source_file
    verified = []

    def verify(src, dst, **kwargs):
        progress = kwargs["progress"]

        def report(current, name):
            if current == 1 and not verified:
                # The verifier already accepted the first file. Corrupt that
                # file while paused; continuing from the second is unsafe.
                verified.append(next(iter(os.listdir(src))))
                checked.set()
                assert release.wait(synchronization_timeout(5))
            progress(current, name)

        return original(src, dst, **{**kwargs, "progress": report})

    monkeypatch.setattr(transfer, "_first_missing_source_file", verify)
    job_id = start_move(transfer_env)
    try:
        assert checked.wait(synchronization_timeout(5))
        assert runner.pause_job(job_id)
        release.set()
        wait_status(runner, job_id, "paused")
        surviving_source = source
        if change == "corrupt_copy":
            (destination / source.name / verified[0]).write_bytes(b"damaged copy")
        else:
            surviving_source = source.with_name("relocated-originals")
            source.rename(surviving_source)
            with Database(db_path) as db:
                db.moves_merge.move_folder_path(folder_id, str(surviving_source))
        assert runner.resume_job(job_id)
        job = wait_for_job_via_runner(runner, job_id)
        assert job["status"] == "failed", job
        assert surviving_source.exists(), "stale pre-pause verification deleted the originals"
        with Database(db_path) as db:
            assert db.folders.get(folder_id)["path"] == str(surviving_source)
        assert not job.get("control_unavailable_reason"), "unsafe copy reached the commit"
    finally:
        release.set()
        runner.cancel_job(job_id)


@pytest.mark.parametrize("action", ["resume", "cancel"])
def test_subprocess_is_reaped_before_pause_is_published(tmp_path, action):
    runner = JobRunner()
    started = tmp_path / "started"
    complete = tmp_path / "complete"
    # The process records its pid, then waits on a file so the test can
    # synchronize without assuming how long Python takes to start on CI.
    script = (
        "import os,pathlib,sys,time; "
        "pathlib.Path(sys.argv[1]).write_text(str(os.getpid())); "
        "\nwhile not pathlib.Path(sys.argv[2]).exists(): time.sleep(.01)\n"
    )

    def work(job):
        result = run_controlled_process(
            [sys.executable, "-c", script, str(started), str(complete)],
            WorkControl(runner, job["id"]), timeout=synchronization_timeout(10),
        )
        return {"ok": result.returncode == 0}

    job_id = runner.start("send-to-nas", work)
    try:
        wait_for_job(lambda: {"status": "ready"} if started.exists() else None,
                     terminal=("ready",), timeout=5)
        pid = int(started.read_text())
        assert runner.pause_job(job_id)
        wait_status(runner, job_id, "paused")
        if sys.platform != "win32":
            with pytest.raises(ProcessLookupError):
                os.kill(pid, 0)
        if action == "resume":
            complete.touch()
            assert runner.resume_job(job_id)
            assert wait_for_job_via_runner(runner, job_id)["status"] == "completed"
            assert int(started.read_text()) != pid
        else:
            assert runner.cancel_job(job_id)
            assert wait_for_job_via_runner(runner, job_id)["status"] == "cancelled"
    finally:
        complete.touch()
        runner.cancel_job(job_id)
        runner.shutdown(timeout=synchronization_timeout(5))


def test_real_rsync_copy_stops_and_resumes_without_losing_files(transfer_env, monkeypatch):
    import move.folder_transfer as transfer
    import move.transfer as streamed

    rsync = shutil.which("rsync")
    if rsync is None:
        pytest.skip("rsync is not installed")
    runner, _, _, _, source, destination = transfer_env
    # The first attempt is deliberately bandwidth-limited; Resume runs at
    # normal speed so this tests a live interruption without a long copy.
    (source / "a.jpg").write_bytes(os.urandom(1024 * 1024))
    expected = (source / "a.jpg").read_bytes()
    started = threading.Event()
    processes = []
    original_spawn = streamed._spawn_rsync_on_pty

    def spawn(cmd):
        proc, stdout = original_spawn(cmd)
        processes.append(proc)
        started.set()
        return proc, stdout

    def copy(*args, **kwargs):
        kwargs["rsync_bin"] = rsync
        if not processes:
            kwargs["extra_args"] = [*(kwargs.get("extra_args") or []), "--bwlimit=128"]
        return streamed._run_rsync_streamed(*args, **kwargs)

    monkeypatch.setattr(streamed, "_spawn_rsync_on_pty", spawn)
    monkeypatch.setattr(transfer, "_run_rsync_streamed", copy)
    job_id = start_move(transfer_env)
    try:
        assert started.wait(synchronization_timeout(5))
        assert runner.pause_job(job_id)
        wait_status(runner, job_id, "paused")
        assert processes[0].poll() is not None
        assert source.exists()
        assert runner.resume_job(job_id)
        job = wait_for_job_via_runner(runner, job_id)
        assert job["status"] == "completed", job
        assert len(processes) >= 2
        assert not source.exists()
        assert (destination / source.name / "a.jpg").read_bytes() == expected
        assert (destination / source.name / "b.jpg").read_bytes() == b"second photo"
    finally:
        runner.cancel_job(job_id)


def test_final_commit_exposes_and_enforces_unavailable_controls(transfer_env, monkeypatch):
    from move.folder_catalog import _FolderCatalogRewrite

    runner = transfer_env[0]
    entered, release = threading.Event(), threading.Event()
    original = _FolderCatalogRewrite.update_catalog

    def commit(catalog):
        entered.set()
        assert release.wait(synchronization_timeout(5))
        return original(catalog)

    monkeypatch.setattr(_FolderCatalogRewrite, "update_catalog", commit)
    job_id = start_move(transfer_env)
    try:
        assert entered.wait(synchronization_timeout(5))
        job = runner.get(job_id)
        assert job["cancellable"] is False
        assert "Finishing safely" in job["control_unavailable_reason"]
        assert runner.pause_job(job_id) is False
        assert runner.cancel_job(job_id) is False
        release.set()
        assert wait_for_job_via_runner(runner, job_id)["status"] == "completed"
    finally:
        release.set()


@pytest.mark.parametrize("entry", ["matching", "wrong_target", "file", "directory", "source_alias"])
@pytest.mark.parametrize("link_kind", ["directory", "file", "broken"])
def test_resume_verifies_skipped_symlinks(transfer_env, monkeypatch, entry, link_kind):
    import move.folder_transfer as transfer

    runner, db_path, _, folder_id, source, destination = transfer_env
    nested = source / "nested"
    nested.mkdir()
    external = source.parent / "external"
    external.mkdir()
    (external / "keep.txt").write_text("external files must remain untouched")
    expected = external if link_kind == "directory" else external / ("keep.txt" if link_kind == "file" else "missing.txt")
    link = nested / "link"
    try:
        link.symlink_to(expected, target_is_directory=link_kind == "directory")
    except OSError:
        pytest.skip("Directory symlinks are unavailable")
    original_target = os.readlink(link)
    entered, release = threading.Event(), threading.Event()
    original = transfer._copy_tree_with_progress

    def copy_with_boundary(src, dst, skip, total, progress, **kwargs):
        def report(current, count, name, phase):
            if not entered.is_set():
                entered.set()
                assert release.wait(synchronization_timeout(5))
            progress(current, count, name, phase)

        return original(src, dst, skip, total, report, **kwargs)

    monkeypatch.setattr(transfer, "_copy_tree_with_progress", copy_with_boundary)
    job_id = start_move(transfer_env)
    try:
        assert entered.wait(synchronization_timeout(5))
        assert runner.pause_job(job_id)
        release.set()
        wait_status(runner, job_id, "paused")
        landing = destination / source.name / "nested"
        if entry == "source_alias":
            landing.symlink_to(nested, target_is_directory=True)
        else:
            landing.mkdir()
            target = landing / link.name
            if entry == "matching":
                target.symlink_to(expected, target_is_directory=link_kind == "directory")
            elif entry == "wrong_target":
                target.symlink_to(source.parent, target_is_directory=True)
            elif entry == "file":
                target.write_text("another writer's file")
            else:
                target.mkdir()
        assert runner.resume_job(job_id)
        job = wait_for_job_via_runner(runner, job_id)
        assert job["status"] == ("completed" if entry == "matching" else "failed"), job
        if entry == "matching":
            assert not source.exists()
            assert os.readlink(landing / link.name) == original_target
        else:
            assert source.exists()
            assert os.readlink(link) == original_target
            with Database(db_path) as db:
                assert db.folders.get(folder_id)["path"] == str(source)
            if entry == "file":
                assert (landing / link.name).read_text() == "another writer's file"
        assert (external / "keep.txt").read_text() == "external files must remain untouched"
    finally:
        release.set()
        runner.cancel_job(job_id)


@pytest.mark.parametrize("entry", ["matching", "missing", "wrong_target", "file", "directory", "source_alias"])
@pytest.mark.parametrize("link_kind", ["directory", "file", "broken"])
def test_both_folder_verifiers_check_symlinks(tmp_path, entry, link_kind):
    from move.verify import _find_content_conflict, _first_missing_source_file, _fresh_copy_mismatch, preview_merge

    source, destination, external = (tmp_path / name for name in ("source", "destination", "external"))
    for path in (source, destination, external):
        path.mkdir()
    (external / "target.txt").write_text("external file")
    expected = external if link_kind == "directory" else external / ("target.txt" if link_kind == "file" else "missing.txt")
    (source / "nested").mkdir()
    link = source / "nested" / "link"
    try:
        link.symlink_to(expected, target_is_directory=link_kind == "directory")
    except OSError:
        pytest.skip("Directory symlinks are unavailable")
    landing = destination / "nested"
    if entry == "source_alias":
        landing.symlink_to(source / "nested", target_is_directory=True)
    else:
        landing.mkdir()
        target = landing / link.name
        if entry == "matching":
            target.symlink_to(expected, target_is_directory=link_kind == "directory")
        elif entry == "wrong_target":
            target.symlink_to(tmp_path, target_is_directory=True)
        elif entry == "file":
            target.touch()
        elif entry == "directory":
            target.mkdir()
    for verify in (_first_missing_source_file, _fresh_copy_mismatch):
        result = verify(str(source), str(destination))
        assert (result is None) == (entry == "matching"), (verify.__name__, result)
    assert preview_merge(str(source), str(destination)) == {
        "will_copy": int(entry == "missing"),
        "will_skip": int(entry == "matching"),
        "will_block": int(entry not in ("missing", "matching")),
        "source_total": 1,
    }
    conflict = _find_content_conflict(str(source), str(destination))
    assert (conflict is None) == (entry in ("matching", "missing")), conflict


@pytest.mark.parametrize("link_kind", ["directory", "file"])
def test_folder_merge_rejects_symlink_conflicts_before_copying(transfer_env, monkeypatch, link_kind):
    import move

    _, db_path, workspace_id, folder_id, source, destination = transfer_env
    targets = []
    for name in ("first-target", "second-target"):
        target = source.parent / name
        target.mkdir()
        (target / "photo.bin").write_bytes(b"identical target bytes")
        targets.append(target if link_kind == "directory" else target / "photo.bin")
    landing = destination / source.name
    landing.mkdir()
    try:
        (source / "link").symlink_to(targets[0], target_is_directory=link_kind == "directory")
        (landing / "link").symlink_to(targets[1], target_is_directory=link_kind == "directory")
    except OSError:
        pytest.skip("Symlinks are unavailable")
    original_target = os.readlink(landing / "link")

    def must_not_copy(*args, **kwargs):
        pytest.fail("a conflicting merge started copying before refusing it")

    monkeypatch.setattr(move.folder_transfer, "_run_rsync_streamed", must_not_copy)
    with Database(db_path) as db:
        db.set_active_workspace(workspace_id)
        result = move.move_folder(db, folder_id, str(destination), merge=True)
        assert db.folders.get(folder_id)["path"] == str(source)
    assert any("Conflict" in error for error in result["errors"]), result
    assert sorted(path.name for path in landing.iterdir()) == ["link"]
    assert os.readlink(landing / "link") == original_target
    assert source.exists()


@pytest.mark.parametrize("redirection", ["subdirectory", "inside_tree", "root"])
def test_resumed_fallback_never_writes_through_destination_directory_links(transfer_env, monkeypatch, redirection):
    import move.folder_transfer as transfer

    runner, db_path, _, folder_id, source, destination = transfer_env
    (source / "nested").mkdir()
    (source / "nested" / "nested.jpg").write_bytes(b"nested photo")
    external = source.parent / "external"
    external.mkdir()
    (external / "sentinel.txt").write_text("untouched")
    entered, release = threading.Event(), threading.Event()
    original = transfer._copy_tree_with_progress

    def copy_with_boundary(src, dst, skip, total, progress, **kwargs):
        def report(current, count, name, phase):
            if not entered.is_set():
                entered.set()
                assert release.wait(synchronization_timeout(5))
            progress(current, count, name, phase)

        return original(src, dst, skip, total, report, **kwargs)

    monkeypatch.setattr(transfer, "_copy_tree_with_progress", copy_with_boundary)
    job_id = start_move(transfer_env)
    try:
        assert entered.wait(synchronization_timeout(5))
        assert runner.pause_job(job_id)
        release.set()
        wait_status(runner, job_id, "paused")
        landing = destination / source.name
        if redirection == "inside_tree":
            external = landing / "other-files"
            external.mkdir()
            (external / "sentinel.txt").write_text("untouched")
        if redirection == "root":
            landing.rename(destination / "completed-prefix")
            redirected = landing
        else:
            redirected = landing / "nested"
        try:
            redirected.symlink_to(external, target_is_directory=True)
        except OSError:
            pytest.skip("Directory symlinks are unavailable")
        original_link = os.readlink(redirected)
        assert runner.resume_job(job_id)
        job = wait_for_job_via_runner(runner, job_id)
        assert job["status"] == "failed", job
        assert (source / "nested" / "nested.jpg").read_bytes() == b"nested photo"
        assert sorted(path.name for path in external.iterdir()) == ["sentinel.txt"]
        assert (external / "sentinel.txt").read_text() == "untouched"
        assert os.readlink(redirected) == original_link
        with Database(db_path) as db:
            assert db.folders.get(folder_id)["path"] == str(source)
    finally:
        release.set()
        runner.cancel_job(job_id)


@pytest.mark.parametrize("linked_root", [False, True])
def test_verifiers_reject_external_directory_aliases_with_independent_bytes(tmp_path, linked_root):
    from move.verify import _find_content_conflict, _first_missing_source_file, _fresh_copy_mismatch, preview_merge

    source, destination, external = (tmp_path / name for name in ("source", "destination", "external"))
    (source / "nested").mkdir(parents=True)
    (external / "nested").mkdir(parents=True)
    (source / "nested" / "photo.bin").write_bytes(b"same bytes")
    (external / "nested" / "photo.bin").write_bytes(b"same bytes")
    try:
        if linked_root:
            destination.symlink_to(external, target_is_directory=True)
        else:
            destination.mkdir()
            (destination / "nested").symlink_to(external / "nested", target_is_directory=True)
    except OSError:
        pytest.skip("Directory symlinks are unavailable")
    assert not os.path.samefile(source / "nested" / "photo.bin", destination / "nested" / "photo.bin")
    for verify in (_find_content_conflict, _first_missing_source_file, _fresh_copy_mismatch):
        assert verify(str(source), str(destination)) is not None, verify.__name__
    assert preview_merge(str(source), str(destination)) == {
        "will_copy": 0, "will_skip": 0, "will_block": 1, "source_total": 1,
    }


def test_configured_alias_above_destination_tree_remains_supported(tmp_path):
    from move.verify import _find_content_conflict, _first_missing_source_file, _fresh_copy_mismatch, preview_merge

    source, physical = tmp_path / "source", tmp_path / "physical"
    source.mkdir()
    physical.mkdir()
    alias = tmp_path / "configured-alias"
    try:
        alias.symlink_to(physical, target_is_directory=True)
    except OSError:
        pytest.skip("Directory symlinks are unavailable")
    destination = alias / "landing"
    destination.mkdir()
    (source / "photo.bin").write_bytes(b"photo")
    (destination / "photo.bin").write_bytes(b"photo")
    for verify in (_find_content_conflict, _first_missing_source_file, _fresh_copy_mismatch):
        assert verify(str(source), str(destination)) is None, verify.__name__
    assert preview_merge(str(source), str(destination))["will_skip"] == 1


@pytest.mark.parametrize("broken", [False, True])
def test_fallback_merge_preserves_finder_named_file_symlinks(transfer_env, broken):
    from move import move_folder

    _, db_path, workspace_id, folder_id, source, destination = transfer_env
    target = source.parent / "external-file"
    if not broken:
        target.write_bytes(b"external")
    try:
        (source / ".DS_Store").symlink_to(target)
    except OSError:
        pytest.skip("Symlinks are unavailable")
    expected_target = os.readlink(source / ".DS_Store")
    (destination / source.name).mkdir()
    with Database(db_path) as db:
        db.set_active_workspace(workspace_id)
        result = move_folder(db, folder_id, str(destination), merge=True)
    assert not result["errors"], result
    assert not source.exists()
    assert os.readlink(destination / source.name / ".DS_Store") == expected_target


def test_resumed_fresh_fallback_copies_finder_metadata(tmp_path):
    """A fresh move resumed after Pause must still carry ``.DS_Store`` files.

    Only merges discard Finder metadata; the fresh verifier walks the
    source and checks each file reaches the destination, so a fresh
    resume that silently skipped ``.DS_Store`` would fail verification
    for a genuinely valid destination.
    """
    from move.transfer import _copy_tree_with_progress

    class _ResumedControl:
        """Minimal WorkControl stand-in: resumed fresh copy, no cancel."""

        generation = 1

        def checkpoint(self):
            return None

    source = tmp_path / "source"
    source.mkdir()
    (source / "photo.jpg").write_bytes(b"photo bytes")
    (source / ".DS_Store").write_bytes(b"window layout")
    destination = tmp_path / "destination"
    destination.mkdir()
    total_files = sum(1 for _, _, files in os.walk(source) for _ in files)
    _copy_tree_with_progress(
        str(source), str(destination), False, total_files, None,
        control=_ResumedControl(),
    )
    assert (destination / "photo.jpg").read_bytes() == b"photo bytes"
    assert (destination / ".DS_Store").read_bytes() == b"window layout"


def test_merge_fallback_still_discards_regular_finder_metadata(tmp_path):
    """The merge path keeps discarding regular ``.DS_Store`` after the split."""
    from move.transfer import _copy_tree_with_progress

    source = tmp_path / "source"
    source.mkdir()
    (source / "photo.jpg").write_bytes(b"photo bytes")
    (source / ".DS_Store").write_bytes(b"source window layout")
    destination = tmp_path / "destination"
    destination.mkdir()
    total_files = sum(1 for _, _, files in os.walk(source) for _ in files)
    _copy_tree_with_progress(
        str(source), str(destination), True, total_files, None,
    )
    assert (destination / "photo.jpg").read_bytes() == b"photo bytes"
    assert not (destination / ".DS_Store").exists()
