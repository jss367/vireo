import os
import shutil
import threading
from pathlib import Path

import pytest
import services.local_workspace as local_workspace
from db import Database
from services.local_workspace import (
    LocalWorkspaceConflict,
    LocalWorkspaceError,
    discard_local,
    stage_workspace,
    status,
    sync_back,
    workspace_dir,
)
from wait import wait_for_job_via_client


@pytest.fixture
def local_workspace_env(tmp_path):
    source = tmp_path / "nas" / "photos"
    child = source / "2026"
    child.mkdir(parents=True)
    (source / "empty-folder").mkdir()
    (source / "root.jpg").write_bytes(b"root-original")
    (child / "bird.jpg").write_bytes(b"bird-original")
    (child / "bird.xmp").write_text("original metadata", encoding="utf-8")

    vireo_dir = tmp_path / "vireo"
    vireo_dir.mkdir()
    db = Database(str(vireo_dir / "vireo.db"))
    workspace_id = db._active_workspace_id
    root_id = db.add_folder(str(source), name="photos")
    child_id = db.add_folder(
        str(child),
        name="2026",
        parent_id=root_id,
        workspace_root=False,
    )
    db.add_photo(
        child_id,
        "bird.jpg",
        ".jpg",
        (child / "bird.jpg").stat().st_size,
        (child / "bird.jpg").stat().st_mtime,
    )
    yield {
        "db": db,
        "workspace_id": workspace_id,
        "root_id": root_id,
        "child_id": child_id,
        "source": source,
        "child": child,
        "vireo_dir": vireo_dir,
    }
    db.close()


def _folder_path(db, folder_id):
    return db.conn.execute("SELECT path FROM folders WHERE id=?", (folder_id,)).fetchone()["path"]


def _source_fs_preserves_case(directory: Path) -> bool:
    probe = directory / "VireoCaseProbe"
    probe.write_bytes(b"")
    try:
        return not (directory / "vireocaseprobe").exists()
    finally:
        probe.unlink()


def test_stage_modify_and_sync_back(local_workspace_env):
    env = local_workspace_env
    result = stage_workspace(env["db"], env["workspace_id"], str(env["vireo_dir"]))

    assert result["files"] == 3
    local_root = Path(_folder_path(env["db"], env["root_id"]))
    local_child = Path(_folder_path(env["db"], env["child_id"]))
    assert local_root != env["source"]
    assert local_child == local_root / "2026"
    assert (local_root / "empty-folder").is_dir()
    assert (local_child / "bird.jpg").read_bytes() == b"bird-original"

    (local_child / "bird.jpg").write_bytes(b"bird-locally-edited")
    (local_child / "new.xmp").write_text("new metadata", encoding="utf-8")
    os.unlink(local_child / "bird.xmp")

    current = status(env["db"], env["workspace_id"], str(env["vireo_dir"]))
    assert current["state"] == "active"
    assert current["changes"] == {"created": 1, "modified": 1, "deleted": 1}

    synced = sync_back(
        env["db"],
        env["workspace_id"],
        str(env["vireo_dir"]),
        allow_deletions=True,
    )

    assert synced["created_or_modified"] == 2
    assert synced["deleted"] == 1
    assert (env["child"] / "bird.jpg").read_bytes() == b"bird-locally-edited"
    assert (env["child"] / "new.xmp").read_text(encoding="utf-8") == "new metadata"
    assert not (env["child"] / "bird.xmp").exists()
    assert _folder_path(env["db"], env["root_id"]) == str(env["source"])
    assert _folder_path(env["db"], env["child_id"]) == str(env["child"])
    assert status(env["db"], env["workspace_id"], str(env["vireo_dir"])) == {
        "state": "remote",
        "workspace_id": env["workspace_id"],
    }
    assert not workspace_dir(str(env["vireo_dir"]), env["workspace_id"]).exists()


def test_stage_aborts_when_source_directory_cannot_be_read(local_workspace_env, monkeypatch):
    env = local_workspace_env
    real_walk = local_workspace.os.walk

    def failing_walk(path, *args, **kwargs):
        if os.path.normpath(path) == os.path.normpath(env["source"]):
            kwargs["onerror"](PermissionError("NAS directory denied"))
        return real_walk(path, *args, **kwargs)

    monkeypatch.setattr(local_workspace.os, "walk", failing_walk)

    with pytest.raises(LocalWorkspaceError, match="NAS directory denied"):
        stage_workspace(env["db"], env["workspace_id"], str(env["vireo_dir"]))

    assert _folder_path(env["db"], env["root_id"]) == str(env["source"])
    assert not workspace_dir(str(env["vireo_dir"]), env["workspace_id"]).exists()


def test_staging_temp_file_cannot_overwrite_real_sibling(local_workspace_env, monkeypatch):
    env = local_workspace_env
    sibling = env["source"] / "collision.vireo-copying"
    base = env["source"] / "collision"
    sibling.write_bytes(b"legitimate sibling")
    base.write_bytes(b"base contents")
    real_walk_entries = local_workspace._walk_entries

    def sibling_first(root):
        yield from sorted(real_walk_entries(root), reverse=True)

    monkeypatch.setattr(local_workspace, "_walk_entries", sibling_first)
    stage_workspace(env["db"], env["workspace_id"], str(env["vireo_dir"]))
    local_root = Path(_folder_path(env["db"], env["root_id"]))

    assert (local_root / "collision").read_bytes() == b"base contents"
    assert (local_root / "collision.vireo-copying").read_bytes() == b"legitimate sibling"


@pytest.mark.skipif(os.name == "nt", reason="Windows test runners may not permit symlinks")
def test_stage_rejects_symlink_that_escapes_workspace(local_workspace_env):
    env = local_workspace_env
    outside = env["source"].parent / "outside.jpg"
    outside.write_bytes(b"outside")
    os.symlink(outside, env["source"] / "escape.jpg")

    with pytest.raises(LocalWorkspaceError, match="Symlink escapes or uses an absolute target"):
        stage_workspace(env["db"], env["workspace_id"], str(env["vireo_dir"]))

    assert _folder_path(env["db"], env["root_id"]) == str(env["source"])


@pytest.mark.skipif(os.name == "nt", reason="Windows test runners may not permit symlinks")
def test_stage_rejects_symlink_that_leaves_and_reenters_root(local_workspace_env):
    env = local_workspace_env
    subfolder = env["source"] / "subfolder"
    subfolder.mkdir()
    os.symlink("../../photos/root.jpg", subfolder / "reentered.jpg")
    assert (subfolder / "reentered.jpg").resolve() == (env["source"] / "root.jpg").resolve()

    with pytest.raises(LocalWorkspaceError, match="Symlink escapes or uses"):
        stage_workspace(env["db"], env["workspace_id"], str(env["vireo_dir"]))


@pytest.mark.skipif(os.name == "nt", reason="Windows test runners may not permit symlinks")
def test_stage_rejects_symlinked_source_root(local_workspace_env, tmp_path):
    # If the selected workspace root is itself a symlink, os.path.isdir()
    # follows it and staging activates, but sync_back later lstats the same
    # source path and refuses to publish through the link. Reject symlinked
    # roots before entering the activation transaction instead.
    env = local_workspace_env
    real_source = tmp_path / "real-photos"
    real_source.mkdir()
    shutil.copytree(str(env["source"]), str(real_source / "photos"))
    shutil.rmtree(str(env["source"]))
    os.symlink(str(real_source / "photos"), str(env["source"]))

    with pytest.raises(LocalWorkspaceError, match="symlink"):
        stage_workspace(env["db"], env["workspace_id"], str(env["vireo_dir"]))

    # Catalog untouched and managed tree cleaned up.
    assert _folder_path(env["db"], env["root_id"]) == str(env["source"])
    assert not workspace_dir(str(env["vireo_dir"]), env["workspace_id"]).exists()


@pytest.mark.skipif(os.name == "nt", reason="Windows test runners may not permit symlinks")
def test_stage_rejects_source_swapped_to_symlink_before_copy(local_workspace_env, tmp_path, monkeypatch):
    # A source entry that was a regular file during _walk_entries can be
    # swapped for a symlink (or FIFO) before _copy_regular_with_hash opens
    # it. A naive open() would follow the link and copy bytes from outside
    # the workspace with a straight face. The copy path must re-validate the
    # source type and refuse.
    env = local_workspace_env
    outside = tmp_path / "outside-secrets.txt"
    outside.write_bytes(b"not part of the workspace")

    real_walk_entries = local_workspace._walk_entries
    swapped = {"done": False}

    def swap_after_walk(root):
        for entry in real_walk_entries(root):
            rel, full, _st = entry
            yield entry
            if not swapped["done"] and rel == os.path.join("2026", "bird.jpg"):
                os.unlink(full)
                os.symlink(str(outside), full)
                swapped["done"] = True

    monkeypatch.setattr(local_workspace, "_walk_entries", swap_after_walk)

    with pytest.raises(LocalWorkspaceError, match="regular file"):
        stage_workspace(env["db"], env["workspace_id"], str(env["vireo_dir"]))

    assert swapped["done"], "test did not exercise the source swap"
    # Catalog untouched: nothing was rebased and the outside file was not read.
    assert _folder_path(env["db"], env["root_id"]) == str(env["source"])
    assert not workspace_dir(str(env["vireo_dir"]), env["workspace_id"]).exists()


@pytest.mark.skipif(os.name == "nt", reason="Windows test runners may not permit symlinks")
def test_stage_rejects_source_parent_swapped_to_symlink_before_copy(
    local_workspace_env, tmp_path, monkeypatch,
):
    # An intermediate source ancestor can be replaced with a symlink between
    # _collect_source_entries and the per-file open in _copy_regular_with_hash.
    # O_NOFOLLOW only refuses the final path component; a bare open() would
    # follow the symlinked parent and copy bytes from outside the workspace
    # (staging would then record them as part of the managed copy). The fd
    # descent must refuse when any ancestor is now a symlink.
    env = local_workspace_env
    outside_tree = tmp_path / "outside-tree"
    outside_tree.mkdir()
    (outside_tree / "bird.jpg").write_bytes(b"outside-leak")
    (outside_tree / "bird.xmp").write_text("outside metadata", encoding="utf-8")

    real_copy_entry = local_workspace._copy_entry
    swapped = {"done": False}

    def swap_after_first_copy(source, destination, st, source_root, cancel_check=None):
        result = real_copy_entry(source, destination, st, source_root, cancel_check)
        # Once the walk-time entries are locked in and root.jpg has already
        # copied cleanly, swap the "2026" subdirectory to a symlink so the
        # follow-up bird.jpg/bird.xmp copies see a symlinked parent.
        if not swapped["done"] and source == str(env["source"] / "root.jpg"):
            shutil.rmtree(env["child"])
            os.symlink(str(outside_tree), str(env["child"]))
            swapped["done"] = True
        return result

    monkeypatch.setattr(local_workspace, "_copy_entry", swap_after_first_copy)

    with pytest.raises(LocalWorkspaceError):
        stage_workspace(env["db"], env["workspace_id"], str(env["vireo_dir"]))

    assert swapped["done"], "test did not exercise the parent-directory swap"
    # Catalog untouched: nothing was rebased.
    assert _folder_path(env["db"], env["root_id"]) == str(env["source"])
    # No outside-tree bytes landed in the managed copy (the whole staging tree
    # is torn down on failure, but if any bytes survive the cleanup they must
    # not be the outside-tree leak).
    managed = workspace_dir(str(env["vireo_dir"]), env["workspace_id"])
    if managed.exists():
        for path in managed.rglob("*"):
            if path.is_file():
                assert b"outside-leak" not in path.read_bytes()


@pytest.mark.skipif(os.name == "nt", reason="Windows test runners may not permit symlinks")
def test_stage_replaces_symlinked_workspace_dir_debris(local_workspace_env, tmp_path):
    # A stale (or user-created) symlink at local-workspaces/<id> would survive
    # shutil.rmtree(ignore_errors=True) and make the next stage's
    # ``base / "files"`` writes land inside the linked-to tree instead of the
    # managed area. Stage must reject/unlink the symlink and then create a
    # real directory in its place, without touching the linked-to tree.
    env = local_workspace_env
    outside_tree = tmp_path / "outside-tree"
    outside_tree.mkdir()
    base = workspace_dir(str(env["vireo_dir"]), env["workspace_id"])
    base.parent.mkdir(parents=True, exist_ok=True)
    os.symlink(str(outside_tree), str(base))
    assert base.is_symlink()

    result = stage_workspace(env["db"], env["workspace_id"], str(env["vireo_dir"]))
    assert result["files"] == 3

    # The stale symlink is gone and a real directory was created in its place.
    assert not base.is_symlink()
    assert base.is_dir()
    # The linked-to tree was not written into.
    assert list(outside_tree.iterdir()) == []


@pytest.mark.skipif(os.name == "nt", reason="Windows test runners may not permit symlinks")
def test_stage_rejects_symlink_target_swapped_after_walk(local_workspace_env, tmp_path, monkeypatch):
    # A source symlink can be repointed between _collect_source_entries and
    # the copy pass. If we trusted the walk-time containment check, the copy
    # would follow readlink() on the swapped target and publish an
    # absolute/escaping link into the managed tree. The copy pass must
    # re-validate containment on the current target and refuse.
    env = local_workspace_env
    # Relative link that stays inside the source root passes the walk-time
    # containment check; the swap below turns it into an absolute escaping
    # link that only the copy pass can catch.
    link_path = env["source"] / "safe-link.jpg"
    os.symlink("root.jpg", link_path)
    outside = tmp_path / "outside.jpg"
    outside.write_bytes(b"outside")

    real_walk_entries = local_workspace._walk_entries
    swapped = {"done": False}

    def swap_after_walk(root):
        for entry in real_walk_entries(root):
            rel, full, _st = entry
            yield entry
            if not swapped["done"] and rel == "safe-link.jpg":
                os.unlink(full)
                os.symlink(str(outside), full)
                swapped["done"] = True

    monkeypatch.setattr(local_workspace, "_walk_entries", swap_after_walk)

    with pytest.raises(LocalWorkspaceError, match="Symlink escapes"):
        stage_workspace(env["db"], env["workspace_id"], str(env["vireo_dir"]))

    assert swapped["done"], "test did not exercise the symlink swap"
    # Catalog untouched and no unsafe link was published into the managed tree.
    assert _folder_path(env["db"], env["root_id"]) == str(env["source"])
    assert not workspace_dir(str(env["vireo_dir"]), env["workspace_id"]).exists()


def test_stage_rejects_case_colliding_source_paths(local_workspace_env, monkeypatch):
    # On case-insensitive local storage (Windows/macOS defaults) two source
    # paths differing only in case would land on the same destination; the
    # second copy silently overwrites the first while the manifest keeps
    # both, so sync can later delete one original and overwrite the other.
    env = local_workspace_env
    # The bug can only be exercised when the source can actually hold both
    # case variants; on a case-folding source tmp_path the two writes below
    # collapse into one file and there is nothing to detect.
    if not _source_fs_preserves_case(env["child"]):
        pytest.skip("source tmp_path is on a case-insensitive filesystem")
    (env["child"] / "Bird.jpg").write_bytes(b"uppercase")

    monkeypatch.setattr(local_workspace, "_dest_case_insensitive", lambda _base: True)

    with pytest.raises(LocalWorkspaceError, match="only differ in case"):
        stage_workspace(env["db"], env["workspace_id"], str(env["vireo_dir"]))

    # The catalog is untouched and the managed tree is cleaned up.
    assert _folder_path(env["db"], env["root_id"]) == str(env["source"])
    assert not workspace_dir(str(env["vireo_dir"]), env["workspace_id"]).exists()


def test_stage_rejects_unicode_normalization_colliding_source_paths(local_workspace_env, monkeypatch):
    # macOS (HFS+/APFS) and Windows also fold canonically equivalent Unicode
    # forms — e.g. NFC "é" ("é") and NFD "é" ("é") — to the same
    # destination name. A source that holds both forms would silently
    # overwrite one copy while the manifest still recorded both entries,
    # leading to false deletes/overwrites on sync. Regression for review
    # thread PRRT_kwDORn8c-s6QQGcc.
    env = local_workspace_env
    if not _source_fs_preserves_case(env["child"]):
        pytest.skip("source tmp_path is on a case-insensitive filesystem")
    nfc = env["child"] / "é-bird.jpg"
    nfd = env["child"] / "é-bird.jpg"
    if nfc == nfd or nfc.exists() or nfd.exists():
        pytest.skip("source tmp_path collapses Unicode normalization forms")
    try:
        nfc.write_bytes(b"nfc")
        nfd.write_bytes(b"nfd")
    except OSError:
        pytest.skip("source tmp_path rejects distinct NFC/NFD filenames")
    if not (nfc.exists() and nfd.exists()):
        pytest.skip("source tmp_path collapses Unicode normalization forms")

    monkeypatch.setattr(local_workspace, "_dest_case_insensitive", lambda _base: True)

    with pytest.raises(LocalWorkspaceError, match="Unicode normalization"):
        stage_workspace(env["db"], env["workspace_id"], str(env["vireo_dir"]))
    # Catalog untouched, managed tree cleaned up.
    assert _folder_path(env["db"], env["root_id"]) == str(env["source"])
    assert not workspace_dir(str(env["vireo_dir"]), env["workspace_id"]).exists()


def test_stage_accepts_case_variants_on_case_sensitive_destination(local_workspace_env, monkeypatch):
    # The check must not fire when the destination truly preserves case,
    # or every source with a case-only pair becomes unstageable.
    env = local_workspace_env
    # Requires both source and destination to actually be case-sensitive:
    # otherwise the two writes collapse into one file (source) or the
    # copies collapse into one on the destination regardless of the probe.
    if not _source_fs_preserves_case(env["child"]) or not _source_fs_preserves_case(env["vireo_dir"]):
        pytest.skip("tmp_path is on a case-insensitive filesystem")
    (env["child"] / "Bird.jpg").write_bytes(b"uppercase")

    monkeypatch.setattr(local_workspace, "_dest_case_insensitive", lambda _base: False)

    stage_workspace(env["db"], env["workspace_id"], str(env["vireo_dir"]))
    local_child = Path(_folder_path(env["db"], env["child_id"]))
    assert (local_child / "bird.jpg").read_bytes() == b"bird-original"
    assert (local_child / "Bird.jpg").read_bytes() == b"uppercase"


def test_folder_status_survives_stage_and_sync(local_workspace_env):
    env = local_workspace_env
    env["db"].conn.execute("UPDATE folders SET status='partial' WHERE id=?", (env["child_id"],))
    env["db"].conn.commit()

    stage_workspace(env["db"], env["workspace_id"], str(env["vireo_dir"]))
    staged_status = (
        env["db"].conn.execute("SELECT status FROM folders WHERE id=?", (env["child_id"],)).fetchone()["status"]
    )
    assert staged_status == "partial"

    sync_back(env["db"], env["workspace_id"], str(env["vireo_dir"]))
    restored_status = (
        env["db"].conn.execute("SELECT status FROM folders WHERE id=?", (env["child_id"],)).fetchone()["status"]
    )
    assert restored_status == "partial"


def test_path_rebases_invalidate_new_image_cache(local_workspace_env):
    env = local_workspace_env
    cache = env["db"]._new_images_cache
    db_path = env["db"]._db_path
    workspace_id = env["workspace_id"]
    generation = cache.get_generation(db_path, workspace_id)

    stage_workspace(env["db"], workspace_id, str(env["vireo_dir"]))
    assert cache.get_generation(db_path, workspace_id) == generation + 1
    discard_local(env["db"], workspace_id, str(env["vireo_dir"]))
    assert cache.get_generation(db_path, workspace_id) == generation + 2

    stage_workspace(env["db"], workspace_id, str(env["vireo_dir"]))
    assert cache.get_generation(db_path, workspace_id) == generation + 3
    sync_back(env["db"], workspace_id, str(env["vireo_dir"]))
    assert cache.get_generation(db_path, workspace_id) == generation + 4


def test_sync_refuses_source_changes_and_preserves_local_workspace(local_workspace_env):
    env = local_workspace_env
    stage_workspace(env["db"], env["workspace_id"], str(env["vireo_dir"]))
    local_child = Path(_folder_path(env["db"], env["child_id"]))
    (local_child / "bird.jpg").write_bytes(b"local edit")
    (env["child"] / "bird.jpg").write_bytes(b"changed on nas")

    with pytest.raises(LocalWorkspaceConflict) as exc_info:
        sync_back(env["db"], env["workspace_id"], str(env["vireo_dir"]))

    assert str(env["child"] / "bird.jpg") in exc_info.value.paths
    assert (local_child / "bird.jpg").read_bytes() == b"local edit"
    assert status(env["db"], env["workspace_id"], str(env["vireo_dir"]))["state"] == "active"


def test_sync_detects_source_change_with_preserved_size_and_mtime(local_workspace_env):
    env = local_workspace_env
    source_file = env["child"] / "bird.jpg"
    original_stat = source_file.stat()
    stage_workspace(env["db"], env["workspace_id"], str(env["vireo_dir"]))
    local_child = Path(_folder_path(env["db"], env["child_id"]))
    (local_child / "bird.jpg").write_bytes(b"locally edited")
    source_file.write_bytes(b"BIRD-EXTERNAL")
    os.utime(source_file, ns=(original_stat.st_atime_ns, original_stat.st_mtime_ns))

    with pytest.raises(LocalWorkspaceConflict) as exc_info:
        sync_back(env["db"], env["workspace_id"], str(env["vireo_dir"]))

    assert str(source_file) in exc_info.value.paths


def test_sync_preserves_source_edit_to_locally_unchanged_file(local_workspace_env):
    # A file the user never touched locally is never published, so an
    # outside edit to it must survive the sync instead of raising a conflict.
    env = local_workspace_env
    source_file = env["child"] / "bird.jpg"
    original_stat = source_file.stat()
    stage_workspace(env["db"], env["workspace_id"], str(env["vireo_dir"]))
    local_child = Path(_folder_path(env["db"], env["child_id"]))
    (local_child / "bird.xmp").write_text("edited metadata", encoding="utf-8")
    source_file.write_bytes(b"BIRD-EXTERNAL")
    os.utime(source_file, ns=(original_stat.st_atime_ns, original_stat.st_mtime_ns))

    sync_back(env["db"], env["workspace_id"], str(env["vireo_dir"]))

    assert source_file.read_bytes() == b"BIRD-EXTERNAL"
    assert (env["child"] / "bird.xmp").read_text(encoding="utf-8") == "edited metadata"


def test_sync_hashes_only_at_risk_files(local_workspace_env, monkeypatch):
    # Conflict verification must be proportional to the change set: an
    # untouched file is never read from the (slow) source during sync.
    env = local_workspace_env
    stage_workspace(env["db"], env["workspace_id"], str(env["vireo_dir"]))
    local_child = Path(_folder_path(env["db"], env["child_id"]))
    (local_child / "bird.xmp").write_text("edited metadata", encoding="utf-8")

    hashed = []
    real_sha256 = local_workspace._sha256

    def counting_sha256(path, cancel_check=None):
        hashed.append(path)
        return real_sha256(path, cancel_check)

    monkeypatch.setattr(local_workspace, "_sha256", counting_sha256)
    sync_back(env["db"], env["workspace_id"], str(env["vireo_dir"]))

    assert hashed == [str(env["child"] / "bird.xmp")]


def test_sync_refuses_missing_managed_local_root(local_workspace_env):
    env = local_workspace_env
    stage_workspace(env["db"], env["workspace_id"], str(env["vireo_dir"]))
    local_root = Path(_folder_path(env["db"], env["root_id"]))
    shutil.rmtree(local_root)

    current = status(env["db"], env["workspace_id"], str(env["vireo_dir"]))
    assert current["state"] == "recovery"
    with pytest.raises(LocalWorkspaceError, match="Managed local folder is unavailable"):
        sync_back(
            env["db"],
            env["workspace_id"],
            str(env["vireo_dir"]),
            allow_deletions=True,
        )
    assert (env["child"] / "bird.jpg").read_bytes() == b"bird-original"


@pytest.mark.skipif(os.name == "nt", reason="Windows test runners may not permit symlinks")
def test_sync_rejects_absolute_symlink_inside_managed_tree(local_workspace_env):
    env = local_workspace_env
    stage_workspace(env["db"], env["workspace_id"], str(env["vireo_dir"]))
    local_root = Path(_folder_path(env["db"], env["root_id"]))
    os.symlink(local_root / "root.jpg", local_root / "absolute-link.jpg")

    with pytest.raises(LocalWorkspaceError, match="absolute target"):
        sync_back(env["db"], env["workspace_id"], str(env["vireo_dir"]))

    assert not (env["source"] / "absolute-link.jpg").exists()


def test_sync_requires_explicit_confirmation_for_deletions(local_workspace_env):
    env = local_workspace_env
    stage_workspace(env["db"], env["workspace_id"], str(env["vireo_dir"]))
    local_child = Path(_folder_path(env["db"], env["child_id"]))
    os.unlink(local_child / "bird.xmp")

    with pytest.raises(LocalWorkspaceError, match="confirm deletions"):
        sync_back(env["db"], env["workspace_id"], str(env["vireo_dir"]))

    assert (env["child"] / "bird.xmp").exists()


def test_sync_temp_file_cannot_overwrite_real_sibling(local_workspace_env):
    env = local_workspace_env
    sibling = env["child"] / "bird.jpg.vireo-syncing"
    sibling.write_bytes(b"legitimate sibling")
    stage_workspace(env["db"], env["workspace_id"], str(env["vireo_dir"]))
    local_child = Path(_folder_path(env["db"], env["child_id"]))
    (local_child / "bird.jpg").write_bytes(b"edited locally")

    sync_back(env["db"], env["workspace_id"], str(env["vireo_dir"]))

    assert (env["child"] / "bird.jpg").read_bytes() == b"edited locally"
    assert sibling.read_bytes() == b"legitimate sibling"


def test_sync_recovery_persists_before_first_publish_and_resumes_cleanly(local_workspace_env, monkeypatch):
    env = local_workspace_env
    stage_workspace(env["db"], env["workspace_id"], str(env["vireo_dir"]))
    local_child = Path(_folder_path(env["db"], env["child_id"]))
    (local_child / "bird.jpg").write_bytes(b"edited-locally")
    (local_child / "bird.xmp").write_text("edited metadata", encoding="utf-8")

    real_publish = local_workspace._atomic_publish
    published = {"count": 0}

    def crashing_publish(local_path, remote_path):
        # Perform the real publish first so the source truly gets mutated,
        # then simulate a mid-sync process death after the first file.
        real_publish(local_path, remote_path)
        published["count"] += 1
        if published["count"] == 1:
            raise RuntimeError("simulated crash after first source publish")

    monkeypatch.setattr(local_workspace, "_atomic_publish", crashing_publish)
    with pytest.raises(RuntimeError, match="simulated crash"):
        sync_back(env["db"], env["workspace_id"], str(env["vireo_dir"]))

    # The manifest must record the interruption before any source mutation
    # so a plain Discard cannot silently strip the managed copy without
    # undoing the already-published edit.
    current = status(env["db"], env["workspace_id"], str(env["vireo_dir"]))
    assert current["state"] == "recovery"
    assert current.get("recovery_kind") == "sync"
    with pytest.raises(LocalWorkspaceError, match="Finish the sync-back"):
        discard_local(env["db"], env["workspace_id"], str(env["vireo_dir"]))

    # A second sync_back call must resume, publish the remaining edits, and
    # leave the source with every local change.
    monkeypatch.setattr(local_workspace, "_atomic_publish", real_publish)
    sync_back(env["db"], env["workspace_id"], str(env["vireo_dir"]))

    assert (env["child"] / "bird.jpg").read_bytes() == b"edited-locally"
    assert (env["child"] / "bird.xmp").read_text(encoding="utf-8") == "edited metadata"
    assert _folder_path(env["db"], env["child_id"]) == str(env["child"])
    assert status(env["db"], env["workspace_id"], str(env["vireo_dir"]))["state"] == "remote"


def test_sync_recovery_resumes_deletions_without_reconfirmation(local_workspace_env, monkeypatch):
    env = local_workspace_env
    stage_workspace(env["db"], env["workspace_id"], str(env["vireo_dir"]))
    local_child = Path(_folder_path(env["db"], env["child_id"]))
    (local_child / "bird.jpg").write_bytes(b"edited-locally")
    os.unlink(local_child / "bird.xmp")

    real_publish = local_workspace._atomic_publish

    def crashing_publish(local_path, remote_path):
        real_publish(local_path, remote_path)
        raise RuntimeError("simulated crash mid-sync")

    monkeypatch.setattr(local_workspace, "_atomic_publish", crashing_publish)
    with pytest.raises(RuntimeError, match="simulated crash"):
        sync_back(
            env["db"],
            env["workspace_id"],
            str(env["vireo_dir"]),
            allow_deletions=True,
        )

    # Recovery must not require the caller to re-confirm the same deletion
    # the user already approved when the interrupted sync began.
    monkeypatch.setattr(local_workspace, "_atomic_publish", real_publish)
    sync_back(env["db"], env["workspace_id"], str(env["vireo_dir"]))

    assert (env["child"] / "bird.jpg").read_bytes() == b"edited-locally"
    assert not (env["child"] / "bird.xmp").exists()


def test_discard_restores_catalog_without_changing_source(local_workspace_env):
    env = local_workspace_env
    stage_workspace(env["db"], env["workspace_id"], str(env["vireo_dir"]))
    local_child = Path(_folder_path(env["db"], env["child_id"]))
    (local_child / "bird.jpg").write_bytes(b"discard me")

    result = discard_local(env["db"], env["workspace_id"], str(env["vireo_dir"]))

    assert result == {"ok": True, "discarded": True}
    assert (env["child"] / "bird.jpg").read_bytes() == b"bird-original"
    assert _folder_path(env["db"], env["child_id"]) == str(env["child"])


def test_sync_rejects_deletions_beyond_confirmed_count(local_workspace_env):
    # The deletion confirmation is bound to the count the user saw; more
    # deletions appearing afterwards must re-prompt, never silently delete.
    env = local_workspace_env
    stage_workspace(env["db"], env["workspace_id"], str(env["vireo_dir"]))
    local_root = Path(_folder_path(env["db"], env["root_id"]))
    os.unlink(local_root / "2026" / "bird.xmp")
    os.unlink(local_root / "root.jpg")

    with pytest.raises(LocalWorkspaceError, match="confirm again"):
        sync_back(
            env["db"],
            env["workspace_id"],
            str(env["vireo_dir"]),
            allow_deletions=True,
            confirmed_deletions=1,
        )
    assert (env["child"] / "bird.xmp").exists()
    assert (env["source"] / "root.jpg").exists()

    sync_back(
        env["db"],
        env["workspace_id"],
        str(env["vireo_dir"]),
        allow_deletions=True,
        confirmed_deletions=2,
    )
    assert not (env["child"] / "bird.xmp").exists()
    assert not (env["source"] / "root.jpg").exists()


def test_restore_merges_rows_created_at_source_paths_while_staged(local_workspace_env):
    # An import (or raw scan) can re-create a folder row at the original NAS
    # path while the workspace is staged. Restore must merge it into the
    # staged row instead of wedging both sync and discard.
    env = local_workspace_env
    stage_workspace(env["db"], env["workspace_id"], str(env["vireo_dir"]))
    (env["child"] / "imported.jpg").write_bytes(b"imported")
    interloper_id = env["db"].add_folder(str(env["child"]), name="2026")
    assert interloper_id != env["child_id"]
    env["db"].add_photo(interloper_id, "imported.jpg", ".jpg", 8, 0.0)

    discard_local(env["db"], env["workspace_id"], str(env["vireo_dir"]))

    rows = env["db"].conn.execute("SELECT id FROM folders WHERE path=?", (str(env["child"]),)).fetchall()
    assert [row["id"] for row in rows] == [env["child_id"]]
    photo = env["db"].conn.execute(
        "SELECT folder_id FROM photos WHERE filename='imported.jpg'"
    ).fetchone()
    assert photo["folder_id"] == env["child_id"]


def test_discard_from_interrupted_sync_requires_acknowledgement(local_workspace_env, monkeypatch):
    # An interrupted sync must not be a dead end: discard stays available
    # behind an explicit acknowledgement that unpublished changes are lost.
    env = local_workspace_env
    stage_workspace(env["db"], env["workspace_id"], str(env["vireo_dir"]))
    local_child = Path(_folder_path(env["db"], env["child_id"]))
    (local_child / "bird.jpg").write_bytes(b"edited-published")
    (local_child / "bird.xmp").write_text("edited-unpublished", encoding="utf-8")

    real_publish = local_workspace._atomic_publish
    published = {"count": 0}

    def crashing_publish(local_path, remote_path):
        real_publish(local_path, remote_path)
        published["count"] += 1
        if published["count"] == 1:
            raise RuntimeError("simulated crash after first source publish")

    monkeypatch.setattr(local_workspace, "_atomic_publish", crashing_publish)
    with pytest.raises(RuntimeError, match="simulated crash"):
        sync_back(env["db"], env["workspace_id"], str(env["vireo_dir"]))

    with pytest.raises(LocalWorkspaceError, match="Finish the sync-back"):
        discard_local(env["db"], env["workspace_id"], str(env["vireo_dir"]))

    result = discard_local(
        env["db"], env["workspace_id"], str(env["vireo_dir"]), acknowledge_published=True
    )
    assert result == {"ok": True, "discarded": True}
    assert _folder_path(env["db"], env["child_id"]) == str(env["child"])
    # The published edit stays on the source; the unpublished one is gone.
    assert (env["child"] / "bird.jpg").read_bytes() == b"edited-published"
    assert (env["child"] / "bird.xmp").read_text(encoding="utf-8") == "original metadata"
    assert not workspace_dir(str(env["vireo_dir"]), env["workspace_id"]).exists()


def test_sync_refuses_source_file_blocking_new_local_directory(local_workspace_env):
    # A file created on the source where local work created a directory must
    # surface as a conflict before the sync starts publishing, so the
    # workspace never enters an unfinishable 'syncing' state.
    env = local_workspace_env
    stage_workspace(env["db"], env["workspace_id"], str(env["vireo_dir"]))
    local_child = Path(_folder_path(env["db"], env["child_id"]))
    (local_child / "newdir").mkdir()
    (local_child / "newdir" / "a.jpg").write_bytes(b"a")
    (env["child"] / "newdir").write_bytes(b"i am a file")

    with pytest.raises(LocalWorkspaceConflict) as exc_info:
        sync_back(env["db"], env["workspace_id"], str(env["vireo_dir"]))

    assert str(env["child"] / "newdir") in exc_info.value.paths
    assert status(env["db"], env["workspace_id"], str(env["vireo_dir"]))["state"] == "active"


@pytest.mark.skipif(os.name == "nt", reason="Windows test runners may not permit symlinks")
def test_sync_refuses_symlinked_source_root_replaced_after_staging(local_workspace_env, tmp_path):
    # If the source root itself is replaced with a symlink after staging,
    # os.path.isdir would follow the link and sync would delete/publish
    # through it into a directory outside the recorded workspace. The
    # root check must use lstat so the topology change is caught before
    # the sync enters the syncing state.
    env = local_workspace_env
    stage_workspace(env["db"], env["workspace_id"], str(env["vireo_dir"]))
    local_root = Path(_folder_path(env["db"], env["root_id"]))
    (local_root / "root.jpg").write_bytes(b"edited-locally")

    outside = tmp_path / "elsewhere"
    outside.mkdir()
    (outside / "root.jpg").write_bytes(b"outside-file")
    shutil.rmtree(env["source"])
    os.symlink(outside, env["source"])

    with pytest.raises(LocalWorkspaceError, match="symlink"):
        sync_back(env["db"], env["workspace_id"], str(env["vireo_dir"]))

    # Nothing was written through the symlink target.
    assert (outside / "root.jpg").read_bytes() == b"outside-file"
    # The workspace never entered the syncing state.
    assert status(env["db"], env["workspace_id"], str(env["vireo_dir"]))["state"] == "active"


@pytest.mark.skipif(os.name == "nt", reason="Windows test runners may not permit symlinks")
def test_sync_refuses_symlinked_managed_local_root_replaced_after_staging(local_workspace_env, tmp_path):
    # If the managed local root itself is replaced with a symlink after
    # staging, os.path.isdir would follow the link and status/sync would
    # walk that outside tree instead of the recorded managed copy — sync
    # could then publish or delete source files based on files that were
    # never in the managed copy. The managed-root check must use lstat.
    env = local_workspace_env
    stage_workspace(env["db"], env["workspace_id"], str(env["vireo_dir"]))
    local_root = Path(_folder_path(env["db"], env["root_id"]))

    outside = tmp_path / "impostor"
    outside.mkdir()
    (outside / "root.jpg").write_bytes(b"impostor-file")
    shutil.rmtree(local_root)
    os.symlink(outside, local_root)

    # Status degrades to recovery so Discard remains reachable without
    # touching source files.
    current = status(env["db"], env["workspace_id"], str(env["vireo_dir"]))
    assert current["state"] == "recovery"
    assert str(local_root) in current["missing_local_paths"]

    with pytest.raises(LocalWorkspaceError, match="symlink"):
        sync_back(env["db"], env["workspace_id"], str(env["vireo_dir"]), allow_deletions=True)

    # No source file was rewritten to the impostor contents and no source
    # file was deleted because the impostor was empty of the real names.
    assert (env["source"] / "root.jpg").read_bytes() == b"root-original"
    assert (env["child"] / "bird.jpg").read_bytes() == b"bird-original"


def test_restore_catalog_is_atomic_across_merge_and_state_cleanup(local_workspace_env, monkeypatch):
    # _restore_catalog performs a self-healing merge, a two-phase rename to
    # avoid folders.path UNIQUE trips, a stray-row rebase, and drops the
    # local-workspace state — all inside a single BEGIN IMMEDIATE. If
    # _merge_into_existing were to commit mid-flow, a later failure would
    # leave a partially-restored catalog with local-workspace state still
    # present. Simulate a failure after the first merge and assert the
    # rollback undoes the merge too.
    env = local_workspace_env
    stage_workspace(env["db"], env["workspace_id"], str(env["vireo_dir"]))

    # An import at the original NAS path while staged materializes a
    # second folder row that will trigger the self-heal merge branch.
    (env["child"] / "imported.jpg").write_bytes(b"imported")
    interloper_id = env["db"].add_folder(str(env["child"]), name="2026")
    assert interloper_id != env["child_id"]
    env["db"].add_photo(interloper_id, "imported.jpg", ".jpg", 8, 0.0)

    # Fail _relink_parents_by_path, which runs AFTER the self-heal merge
    # (and after every rebase). If _merge_into_existing had committed
    # mid-flow, the interloper row would be gone after rollback and the
    # catalog would be inconsistent with the still-present local-workspace
    # state. Because it now runs inside the caller's transaction, rollback
    # unwinds the merge too.
    original_relink = env["db"]._relink_parents_by_path

    def failing_relink(*args, **kwargs):
        raise RuntimeError("simulated failure after merge")

    monkeypatch.setattr(env["db"], "_relink_parents_by_path", failing_relink)
    with pytest.raises(RuntimeError, match="simulated failure"):
        discard_local(env["db"], env["workspace_id"], str(env["vireo_dir"]))
    monkeypatch.setattr(env["db"], "_relink_parents_by_path", original_relink)

    # The interloper row survived the rollback: the self-heal merge did
    # NOT commit ahead of the rest of the restore.
    still_present = env["db"].conn.execute(
        "SELECT id FROM folders WHERE id=?", (interloper_id,)
    ).fetchone()
    assert still_present is not None
    # Local-workspace state is still present too — the failed restore did
    # not partially clean it up.
    lw_row = env["db"].conn.execute(
        "SELECT workspace_id FROM local_workspaces WHERE workspace_id=?", (env["workspace_id"],)
    ).fetchone()
    assert lw_row is not None

    # A subsequent discard succeeds cleanly — the workspace is not wedged.
    discard_local(env["db"], env["workspace_id"], str(env["vireo_dir"]))
    lw_row_after = env["db"].conn.execute(
        "SELECT workspace_id FROM local_workspaces WHERE workspace_id=?", (env["workspace_id"],)
    ).fetchone()
    assert lw_row_after is None


def test_sync_recovery_refuses_new_deletions_that_were_not_confirmed(local_workspace_env, monkeypatch):
    # If a file is deleted from the managed local tree after the first sync
    # attempt was interrupted, clicking Finish Sync-back must NOT silently
    # authorize that new source deletion — the user only confirmed the
    # deletions that existed when the first sync began.
    env = local_workspace_env
    stage_workspace(env["db"], env["workspace_id"], str(env["vireo_dir"]))
    local_child = Path(_folder_path(env["db"], env["child_id"]))
    (local_child / "bird.jpg").write_bytes(b"edited-locally")
    # One deletion confirmed at the start of the first sync.
    os.unlink(local_child / "bird.xmp")

    real_publish = local_workspace._atomic_publish

    def crashing_publish(local_path, remote_path):
        real_publish(local_path, remote_path)
        raise RuntimeError("simulated crash mid-sync")

    monkeypatch.setattr(local_workspace, "_atomic_publish", crashing_publish)
    with pytest.raises(RuntimeError, match="simulated crash"):
        sync_back(
            env["db"],
            env["workspace_id"],
            str(env["vireo_dir"]),
            allow_deletions=True,
            confirmed_deletions=1,
        )

    # A cleanup tool (or the user) removes a second file after the crash.
    local_root = Path(_folder_path(env["db"], env["root_id"]))
    os.unlink(local_root / "root.jpg")

    monkeypatch.setattr(local_workspace, "_atomic_publish", real_publish)
    with pytest.raises(LocalWorkspaceError, match="not part of your original confirmation"):
        sync_back(env["db"], env["workspace_id"], str(env["vireo_dir"]))

    # The unconfirmed new deletion did not reach the source.
    assert (env["source"] / "root.jpg").exists()
    # Recovery state persists so the user can re-confirm through the UI.
    assert status(env["db"], env["workspace_id"], str(env["vireo_dir"]))["state"] == "recovery"


def test_sync_recovery_accepts_fresh_count_confirmation_for_new_deletions(local_workspace_env, monkeypatch):
    # If a new deletion appears after an interrupted sync, the recovery UI
    # sends a fresh count-bound confirmation. The service must accept it,
    # publish the new deletion, and rewrite the recovery marker so a second
    # interruption resumes against the newly-confirmed set.
    env = local_workspace_env
    stage_workspace(env["db"], env["workspace_id"], str(env["vireo_dir"]))
    local_child = Path(_folder_path(env["db"], env["child_id"]))
    local_root = Path(_folder_path(env["db"], env["root_id"]))
    (local_child / "bird.jpg").write_bytes(b"edited-locally")
    os.unlink(local_child / "bird.xmp")

    real_publish = local_workspace._atomic_publish

    def crashing_publish(local_path, remote_path):
        real_publish(local_path, remote_path)
        raise RuntimeError("simulated crash mid-sync")

    monkeypatch.setattr(local_workspace, "_atomic_publish", crashing_publish)
    with pytest.raises(RuntimeError, match="simulated crash"):
        sync_back(
            env["db"],
            env["workspace_id"],
            str(env["vireo_dir"]),
            allow_deletions=True,
            confirmed_deletions=1,
        )

    # After the crash the user deletes another file locally, then finishes the
    # sync-back with a fresh count-bound confirmation covering both deletions.
    os.unlink(local_root / "root.jpg")
    monkeypatch.setattr(local_workspace, "_atomic_publish", real_publish)
    sync_back(
        env["db"],
        env["workspace_id"],
        str(env["vireo_dir"]),
        allow_deletions=True,
        confirmed_deletions=2,
    )

    assert not (env["source"] / "root.jpg").exists()
    assert not (env["child"] / "bird.xmp").exists()
    assert (env["child"] / "bird.jpg").read_bytes() == b"edited-locally"


def test_sync_recovery_rejects_stale_count_confirmation_for_new_deletions(local_workspace_env, monkeypatch):
    # A resume with a fresh count that no longer matches the current deletion
    # set must refuse — the confirmation is stale and the UI has to re-prompt.
    env = local_workspace_env
    stage_workspace(env["db"], env["workspace_id"], str(env["vireo_dir"]))
    local_child = Path(_folder_path(env["db"], env["child_id"]))
    local_root = Path(_folder_path(env["db"], env["root_id"]))
    (local_child / "bird.jpg").write_bytes(b"edited-locally")
    os.unlink(local_child / "bird.xmp")

    real_publish = local_workspace._atomic_publish

    def crashing_publish(local_path, remote_path):
        real_publish(local_path, remote_path)
        raise RuntimeError("simulated crash mid-sync")

    monkeypatch.setattr(local_workspace, "_atomic_publish", crashing_publish)
    with pytest.raises(RuntimeError, match="simulated crash"):
        sync_back(
            env["db"],
            env["workspace_id"],
            str(env["vireo_dir"]),
            allow_deletions=True,
            confirmed_deletions=1,
        )

    os.unlink(local_root / "root.jpg")
    monkeypatch.setattr(local_workspace, "_atomic_publish", real_publish)
    with pytest.raises(LocalWorkspaceError, match="confirm again"):
        sync_back(
            env["db"],
            env["workspace_id"],
            str(env["vireo_dir"]),
            allow_deletions=True,
            confirmed_deletions=1,
        )

    # The unconfirmed new deletion did not reach the source.
    assert (env["source"] / "root.jpg").exists()


@pytest.mark.skipif(os.name == "nt", reason="Windows test runners may not permit symlinks")
def test_sync_refuses_symlinked_source_ancestor_for_new_local_file(local_workspace_env, tmp_path):
    # If a source-side parent is replaced with a symlink after staging,
    # os.path.isdir would follow the link and the publish would silently
    # write the local file into the symlink target outside the workspace.
    # Detect the topology change before entering the syncing state.
    env = local_workspace_env
    stage_workspace(env["db"], env["workspace_id"], str(env["vireo_dir"]))
    local_child = Path(_folder_path(env["db"], env["child_id"]))
    (local_child / "new-subdir").mkdir()
    (local_child / "new-subdir" / "newfile.jpg").write_bytes(b"published-locally")

    outside = tmp_path / "elsewhere"
    outside.mkdir()
    shutil.rmtree(env["child"])
    os.symlink(outside, env["child"])

    with pytest.raises(LocalWorkspaceConflict) as exc_info:
        sync_back(env["db"], env["workspace_id"], str(env["vireo_dir"]), allow_deletions=True)

    assert str(env["child"]) in exc_info.value.paths
    # No new file was written into the symlink target.
    assert list(outside.iterdir()) == []
    # The workspace never entered syncing state.
    assert status(env["db"], env["workspace_id"], str(env["vireo_dir"]))["state"] == "active"


def test_sync_refuses_symlinked_source_ancestor_for_deleted_local_file(local_workspace_env, tmp_path):
    # If the only local mutation is a deletion, the ancestor check must still
    # cover its source parent: an os.unlink on ``a/file`` after ``a`` was
    # replaced by a symlink would delete outside the workspace.
    env = local_workspace_env
    stage_workspace(env["db"], env["workspace_id"], str(env["vireo_dir"]))
    local_child = Path(_folder_path(env["db"], env["child_id"]))
    os.unlink(local_child / "bird.jpg")
    os.unlink(local_child / "bird.xmp")

    outside = tmp_path / "elsewhere"
    outside.mkdir()
    decoy = outside / "bird.jpg"
    decoy.write_bytes(b"outside-do-not-delete")
    shutil.rmtree(env["child"])
    os.symlink(outside, env["child"])

    with pytest.raises(LocalWorkspaceConflict) as exc_info:
        sync_back(
            env["db"],
            env["workspace_id"],
            str(env["vireo_dir"]),
            allow_deletions=True,
            confirmed_deletions=2,
        )

    assert str(env["child"]) in exc_info.value.paths
    # No file was deleted through the symlink target.
    assert decoy.exists()
    # The workspace never entered syncing state.
    assert status(env["db"], env["workspace_id"], str(env["vireo_dir"]))["state"] == "active"


def test_local_file_to_directory_replacement_syncs(local_workspace_env):
    env = local_workspace_env
    stage_workspace(env["db"], env["workspace_id"], str(env["vireo_dir"]))
    local_root = Path(_folder_path(env["db"], env["root_id"]))
    os.unlink(local_root / "root.jpg")
    (local_root / "root.jpg").mkdir()
    (local_root / "root.jpg" / "inner.jpg").write_bytes(b"inner")

    sync_back(env["db"], env["workspace_id"], str(env["vireo_dir"]), allow_deletions=True)

    assert (env["source"] / "root.jpg").is_dir()
    assert (env["source"] / "root.jpg" / "inner.jpg").read_bytes() == b"inner"


def test_local_directory_to_file_replacement_syncs(local_workspace_env):
    env = local_workspace_env
    stage_workspace(env["db"], env["workspace_id"], str(env["vireo_dir"]))
    local_root = Path(_folder_path(env["db"], env["root_id"]))
    shutil.rmtree(local_root / "2026")
    (local_root / "2026").write_bytes(b"now a file")

    sync_back(env["db"], env["workspace_id"], str(env["vireo_dir"]), allow_deletions=True)

    assert (env["source"] / "2026").is_file()
    assert (env["source"] / "2026").read_bytes() == b"now a file"


@pytest.mark.skipif(not hasattr(os, "mkfifo"), reason="FIFOs unavailable on this platform")
def test_special_file_degrades_status_and_blocks_sync_but_not_discard(local_workspace_env):
    env = local_workspace_env
    stage_workspace(env["db"], env["workspace_id"], str(env["vireo_dir"]))
    local_root = Path(_folder_path(env["db"], env["root_id"]))
    os.mkfifo(local_root / "pipe")

    current = status(env["db"], env["workspace_id"], str(env["vireo_dir"]))
    assert current["state"] == "active"
    assert "Unsupported special file" in current["changes_error"]
    assert current["sync_available"] is False

    with pytest.raises(LocalWorkspaceError, match="Unsupported special file"):
        sync_back(env["db"], env["workspace_id"], str(env["vireo_dir"]))

    discard_local(env["db"], env["workspace_id"], str(env["vireo_dir"]))
    assert _folder_path(env["db"], env["root_id"]) == str(env["source"])


@pytest.mark.skipif(os.name == "nt", reason="Windows test runners may not permit symlinks")
def test_escaping_symlink_degrades_status_instead_of_failing(local_workspace_env):
    # The Workspace page must keep rendering (and keep Discard reachable)
    # when the local tree contains something sync would refuse.
    env = local_workspace_env
    stage_workspace(env["db"], env["workspace_id"], str(env["vireo_dir"]))
    local_root = Path(_folder_path(env["db"], env["root_id"]))
    os.symlink(local_root / "root.jpg", local_root / "absolute-link.jpg")

    current = status(env["db"], env["workspace_id"], str(env["vireo_dir"]))
    assert current["state"] == "active"
    assert "Symlink" in current["changes_error"]

    discard_local(env["db"], env["workspace_id"], str(env["vireo_dir"]))
    assert _folder_path(env["db"], env["root_id"]) == str(env["source"])


def test_stage_cancel_via_begin_commit_cleans_up(local_workspace_env):
    # A cancellation that lands right before activation aborts cleanly:
    # the catalog is untouched and the partial copy is removed.
    env = local_workspace_env
    with pytest.raises(local_workspace.LocalWorkspaceCancelled):
        stage_workspace(
            env["db"],
            env["workspace_id"],
            str(env["vireo_dir"]),
            begin_commit=lambda: False,
        )

    assert _folder_path(env["db"], env["root_id"]) == str(env["source"])
    assert not workspace_dir(str(env["vireo_dir"]), env["workspace_id"]).exists()
    assert status(env["db"], env["workspace_id"], str(env["vireo_dir"]))["state"] == "remote"


def test_stage_rejects_folders_shared_with_another_workspace(local_workspace_env):
    env = local_workspace_env
    other_workspace = env["db"].create_workspace("Shared")
    env["db"].add_workspace_folder(other_workspace, env["root_id"])

    with pytest.raises(LocalWorkspaceError, match="also used by another workspace"):
        stage_workspace(env["db"], env["workspace_id"], str(env["vireo_dir"]))


def test_stage_rejects_folder_covered_by_another_workspace_root(tmp_path):
    source_root = tmp_path / "nas" / "photos"
    source_root.mkdir(parents=True)
    db = Database(str(tmp_path / "vireo.db"))
    first_workspace = db._active_workspace_id
    db.set_active_workspace(None)
    root_id = db.add_folder(str(source_root), name="photos")
    db.set_active_workspace(first_workspace)
    db.add_workspace_folder(first_workspace, root_id)

    nested = source_root / "2026"
    nested.mkdir()
    (nested / "bird.jpg").write_bytes(b"bird")
    second_workspace = db.create_workspace("Nested")
    db.set_active_workspace(second_workspace)
    nested_id = db.add_folder(str(nested), name="2026")

    # The ancestor workspace was linked before the nested folder existed, so
    # it has no exact workspace_folders row for nested_id.
    exact_link = db.conn.execute(
        "SELECT 1 FROM workspace_folders WHERE workspace_id=? AND folder_id=?",
        (first_workspace, nested_id),
    ).fetchone()
    assert exact_link is None

    with pytest.raises(LocalWorkspaceError, match="overlaps a root used by another workspace"):
        stage_workspace(db, second_workspace, str(tmp_path / "local-data"))
    db.close()


def test_stage_rejects_source_root_from_active_local_workspace(tmp_path):
    source_root = tmp_path / "nas" / "photos"
    source_root.mkdir(parents=True)
    (source_root / "bird.jpg").write_bytes(b"bird")
    vireo_dir = tmp_path / "vireo"
    vireo_dir.mkdir()
    db = Database(str(vireo_dir / "vireo.db"))
    first_workspace = db._active_workspace_id
    db.add_folder(str(source_root), name="photos")
    stage_workspace(db, first_workspace, str(vireo_dir))

    nested = source_root / "2026"
    nested.mkdir()
    (nested / "new.jpg").write_bytes(b"new")
    second_workspace = db.create_workspace("Nested")
    db.set_active_workspace(second_workspace)
    db.add_folder(str(nested), name="2026")

    with pytest.raises(LocalWorkspaceError, match="overlaps a root used by another workspace"):
        stage_workspace(db, second_workspace, str(vireo_dir))
    db.close()


def test_stage_reports_overlap_when_roots_share_normalized_key(tmp_path, monkeypatch):
    # Two workspace roots that normalize to the same key (e.g. case-only
    # variants on a case-insensitive filesystem) used to raise TypeError from
    # the tuple sort before the informative overlap error could run.
    source_root = tmp_path / "nas" / "photos"
    source_root.mkdir(parents=True)
    other_root = tmp_path / "nas" / "extras"
    other_root.mkdir(parents=True)
    (source_root / "bird.jpg").write_bytes(b"bird")
    (other_root / "bug.jpg").write_bytes(b"bug")
    db = Database(str(tmp_path / "vireo.db"))
    workspace_id = db._active_workspace_id
    db.add_folder(str(source_root), name="photos")
    db.add_folder(str(other_root), name="extras")

    monkeypatch.setattr(local_workspace, "_norm", lambda _path: "COLLIDES")

    with pytest.raises(LocalWorkspaceError, match="Workspace roots overlap"):
        stage_workspace(db, workspace_id, str(tmp_path / "local-data"))
    db.close()


def test_shared_folder_check_handles_more_than_sqlite_variable_limit(tmp_path):
    source_root = tmp_path / "nas" / "photos"
    source_root.mkdir(parents=True)
    vireo_dir = tmp_path / "vireo"
    vireo_dir.mkdir()
    db = Database(str(vireo_dir / "vireo.db"))
    workspace_id = db._active_workspace_id
    root_id = db.add_folder(str(source_root), name="photos")
    other_workspace = db.create_workspace("Shared descendant")

    rows = [(folder_id, str(source_root / f"folder-{folder_id}"), root_id) for folder_id in range(1000, 2105)]
    db.conn.executemany(
        "INSERT INTO folders (id, path, name, parent_id) VALUES (?, ?, '', ?)",
        rows,
    )
    db.conn.executemany(
        "INSERT INTO workspace_folders (workspace_id, folder_id, is_root) VALUES (?, ?, 0)",
        [(workspace_id, folder_id) for folder_id, _path, _parent in rows],
    )
    shared_id = rows[-1][0]
    db.conn.execute(
        "INSERT INTO workspace_folders (workspace_id, folder_id, is_root) VALUES (?, ?, 0)",
        (other_workspace, shared_id),
    )
    db.conn.commit()

    with pytest.raises(LocalWorkspaceError, match="also used by another workspace"):
        stage_workspace(db, workspace_id, str(vireo_dir))
    db.close()


def test_work_locally_http_job_flow(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    from app import create_app

    source = tmp_path / "nas" / "photos"
    source.mkdir(parents=True)
    (source / "bird.jpg").write_bytes(b"original")
    vireo_dir = tmp_path / "vireo"
    thumbs = vireo_dir / "thumbnails"
    thumbs.mkdir(parents=True)
    db_path = str(vireo_dir / "vireo.db")

    db = Database(db_path)
    folder_id = db.add_folder(str(source), name="photos")
    workspace_id = db._active_workspace_id
    db.close()

    app = create_app(db_path, thumb_cache_dir=str(thumbs))
    app.config["TESTING"] = True
    with app.test_client() as client:
        assert client.get("/api/workspaces/active/local-workspace").get_json()["state"] == "remote"

        # Hold the first request inside runner.start while a second request
        # arrives. The transition lock must let only one job register.
        import web.local_workspace as local_workspace_routes

        start_entered = threading.Event()
        allow_start = threading.Event()
        allow_job = threading.Event()
        original_start = app._job_runner.start

        def slow_start(*args, **kwargs):
            start_entered.set()
            assert allow_start.wait(timeout=5)
            return original_start(*args, **kwargs)

        def slow_stage(*args, **kwargs):
            assert allow_job.wait(timeout=5)
            return {"ok": True, "files": 0, "bytes": 0, "local_path": ""}

        responses = []

        def submit_stage():
            with app.test_client() as thread_client:
                response = thread_client.post("/api/workspaces/active/local-workspace/stage", json={})
                responses.append((response.status_code, response.get_json()))

        with monkeypatch.context() as patcher:
            patcher.setattr(app._job_runner, "start", slow_start)
            patcher.setattr(local_workspace_routes, "stage_workspace", slow_stage)
            first = threading.Thread(target=submit_stage)
            second = threading.Thread(target=submit_stage)
            first.start()
            assert start_entered.wait(timeout=5)
            second.start()
            allow_start.set()
            first.join(timeout=5)
            second.join(timeout=5)
            assert sorted(code for code, _body in responses) == [202, 409]
            job_id = next(body["job_id"] for code, body in responses if code == 202)
            # A fresh status read while the job runs must report the live
            # job, so other tabs render progress instead of recovery UI.
            during = client.get("/api/workspaces/active/local-workspace").get_json()
            assert during["job"] == {"id": job_id, "type": "work-locally-stage"}
            allow_job.set()
            assert wait_for_job_via_client(client, job_id)["status"] == "completed"

        response = client.post("/api/workspaces/active/local-workspace/stage", json={})
        assert response.status_code == 202
        stage_job = wait_for_job_via_client(client, response.get_json()["job_id"])
        assert stage_job["status"] == "completed"

        local_status = client.get("/api/workspaces/active/local-workspace").get_json()
        assert local_status["state"] == "active"
        other_workspace = client.post("/api/workspaces", json={"name": "Other"}).get_json()["id"]
        assert client.post(f"/api/workspaces/{other_workspace}/activate").status_code == 200
        blocked_delete = client.delete(f"/api/workspaces/{workspace_id}")
        # A workspace with local work is a resolvable conflict (409), matching
        # every other "blocked because of local work" guard on this PR — a
        # plain 400 would misclassify it as a bad request and clients that
        # branch on 409 to show "resolve local work first" would miss it.
        assert blocked_delete.status_code == 409
        assert "sync or discard" in blocked_delete.get_json()["error"]
        assert client.post(f"/api/workspaces/{workspace_id}/activate").status_code == 200

        check_db = Database(db_path)
        local_path = _folder_path(check_db, folder_id)
        check_db.close()
        Path(local_path, "bird.jpg").write_bytes(b"edited")

        response = client.post(
            "/api/workspaces/active/local-workspace/sync",
            json={"confirm_deletions": False},
        )
        assert response.status_code == 202
        sync_job = wait_for_job_via_client(client, response.get_json()["job_id"])
        assert sync_job["status"] == "completed"
        assert (source / "bird.jpg").read_bytes() == b"edited"
        assert client.get("/api/workspaces/active/local-workspace").get_json()["state"] == "remote"

    final_db = Database(db_path)
    assert _folder_path(final_db, folder_id) == str(source)
    assert final_db._active_workspace_id == workspace_id
    final_db.close()


def test_sync_http_rejects_confirm_deletions_without_count(tmp_path, monkeypatch):
    # The workflow binds deletion authorization to the count the user saw.
    # A stale client or direct API caller that sends ``confirm_deletions: true``
    # without ``confirmed_deletion_count`` must be rejected instead of letting
    # sync delete however many source files happen to match at execution time.
    monkeypatch.setenv("HOME", str(tmp_path))
    from app import create_app

    source = tmp_path / "nas" / "photos"
    source.mkdir(parents=True)
    (source / "bird.jpg").write_bytes(b"original")
    (source / "extra.jpg").write_bytes(b"extra-original")
    vireo_dir = tmp_path / "vireo"
    thumbs = vireo_dir / "thumbnails"
    thumbs.mkdir(parents=True)
    db_path = str(vireo_dir / "vireo.db")

    db = Database(db_path)
    folder_id = db.add_folder(str(source), name="photos")
    db.close()

    app = create_app(db_path, thumb_cache_dir=str(thumbs))
    app.config["TESTING"] = True
    with app.test_client() as client:
        response = client.post("/api/workspaces/active/local-workspace/stage", json={})
        assert response.status_code == 202
        assert wait_for_job_via_client(client, response.get_json()["job_id"])["status"] == "completed"

        check_db = Database(db_path)
        local_path = _folder_path(check_db, folder_id)
        check_db.close()
        os.unlink(Path(local_path, "bird.jpg"))
        os.unlink(Path(local_path, "extra.jpg"))

        rejected = client.post(
            "/api/workspaces/active/local-workspace/sync",
            json={"confirm_deletions": True},
        )
        assert rejected.status_code == 400
        assert "confirmed_deletion_count" in rejected.get_json()["error"]
        # Source files must survive the rejected request.
        assert (source / "bird.jpg").exists()
        assert (source / "extra.jpg").exists()

        # A properly bound confirmation still succeeds.
        accepted = client.post(
            "/api/workspaces/active/local-workspace/sync",
            json={"confirm_deletions": True, "confirmed_deletion_count": 2},
        )
        assert accepted.status_code == 202
        job = wait_for_job_via_client(client, accepted.get_json()["job_id"])
        assert job["status"] == "completed"
        assert not (source / "bird.jpg").exists()
        assert not (source / "extra.jpg").exists()


def test_discard_http_flow_guards_stale_page_state(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    from app import create_app

    source = tmp_path / "nas" / "photos"
    source.mkdir(parents=True)
    (source / "bird.jpg").write_bytes(b"original")
    vireo_dir = tmp_path / "vireo"
    thumbs = vireo_dir / "thumbnails"
    thumbs.mkdir(parents=True)
    db_path = str(vireo_dir / "vireo.db")

    db = Database(db_path)
    folder_id = db.add_folder(str(source), name="photos")
    db.close()

    app = create_app(db_path, thumb_cache_dir=str(thumbs))
    app.config["TESTING"] = True
    with app.test_client() as client:
        response = client.post("/api/workspaces/active/local-workspace/stage", json={})
        assert response.status_code == 202
        assert wait_for_job_via_client(client, response.get_json()["job_id"])["status"] == "completed"

        # A stale page that rendered 'Clean Up Incomplete Copy' before the
        # stage finished must not discard the now-healthy workspace.
        stale = client.post(
            "/api/workspaces/active/local-workspace/discard",
            json={"confirm": True, "expected_state": "staging"},
        )
        assert stale.status_code == 409
        assert "changed since this page loaded" in stale.get_json()["error"]

        response = client.post(
            "/api/workspaces/active/local-workspace/discard",
            json={"confirm": True, "expected_state": "active"},
        )
        assert response.status_code == 202
        assert wait_for_job_via_client(client, response.get_json()["job_id"])["status"] == "completed"
        assert client.get("/api/workspaces/active/local-workspace").get_json()["state"] == "remote"

    final_db = Database(db_path)
    assert _folder_path(final_db, folder_id) == str(source)
    final_db.close()


def test_local_workspace_folders_folder_id_fk_cascades_on_folder_delete(local_workspace_env):
    """Deleting the underlying folder cascades to local_workspace_folders.

    The API-level guards refuse a folder delete while a workspace has it
    staged, but the FK is the belt-and-suspenders backstop for any code path
    that reaches ``DELETE FROM folders`` without going through the guard —
    otherwise the ``local_workspace_folders`` row would dangle and later
    sync/discard would fail to restore the catalog.
    """
    env = local_workspace_env
    db = env["db"]
    stage_workspace(db, env["workspace_id"], str(env["vireo_dir"]))
    child_id = env["child_id"]
    claim = db.conn.execute(
        "SELECT COUNT(*) AS n FROM local_workspace_folders WHERE workspace_id=? AND folder_id=?",
        (env["workspace_id"], child_id),
    ).fetchone()["n"]
    assert claim == 1
    # Bypass the API guard by going straight to raw SQL — this is what the
    # FK protects against. Clear the unrelated ``photos.folder_id`` and
    # ``workspace_folders.folder_id`` FKs on this leaf first; those
    # constraints predate this PR and are unrelated to the
    # local_workspace_folders cascade we're verifying.
    db.conn.execute("DELETE FROM photos WHERE folder_id=?", (child_id,))
    db.conn.execute("DELETE FROM workspace_folders WHERE folder_id=?", (child_id,))
    db.conn.execute("DELETE FROM folders WHERE id=?", (child_id,))
    db.conn.commit()
    dangling = db.conn.execute(
        "SELECT COUNT(*) AS n FROM local_workspace_folders WHERE workspace_id=? AND folder_id=?",
        (env["workspace_id"], child_id),
    ).fetchone()["n"]
    assert dangling == 0


def test_local_workspace_folders_fk_migration_backfills_existing_dbs(tmp_path):
    """A DB whose ``local_workspace_folders`` was created without the FK
    is rebuilt with the constraint on the next Database open."""
    db_path = str(tmp_path / "legacy.db")
    legacy = Database(db_path)
    # Drop the table and re-create it in the pre-fix shape (folder_id
    # without a REFERENCES clause) so the migration has something to fix.
    legacy.conn.execute("DROP TABLE local_workspace_folders")
    legacy.conn.execute(
        """CREATE TABLE local_workspace_folders (
            workspace_id    INTEGER NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
            folder_id       INTEGER NOT NULL,
            source_path     TEXT NOT NULL,
            local_path      TEXT NOT NULL,
            original_status TEXT NOT NULL DEFAULT 'ok',
            is_root         INTEGER NOT NULL DEFAULT 0,
            root_index      INTEGER,
            PRIMARY KEY (workspace_id, folder_id)
        )"""
    )
    legacy.conn.commit()
    fk_before = legacy.conn.execute(
        "PRAGMA foreign_key_list(local_workspace_folders)"
    ).fetchall()
    assert not any(row["from"] == "folder_id" for row in fk_before)
    legacy.close()

    # Re-open the DB: the migration inside Database.__init__ rebuilds the
    # table with the folder_id → folders(id) FK.
    upgraded = Database(db_path)
    fk_after = upgraded.conn.execute(
        "PRAGMA foreign_key_list(local_workspace_folders)"
    ).fetchall()
    assert any(
        row["from"] == "folder_id" and row["table"] == "folders" and row["on_delete"] == "CASCADE"
        for row in fk_after
    )
    upgraded.close()


def test_folder_delete_route_holds_stage_boundary_lock_around_guard(tmp_path, monkeypatch):
    """``api_folder_delete`` runs its guard-plus-delete under stage_boundary_lock.

    That is the atomicity CodeRabbit's Critical finding requires: a stage's
    ``INSERT INTO local_workspace_folders`` also holds the same lock, so a
    concurrent stage cannot claim the folder in the window between
    ``folder_has_local_workspace`` and ``db.delete_folder``. Verify the
    route acquires the lock by swapping it for a tracking wrapper.
    """
    monkeypatch.setenv("HOME", str(tmp_path))
    from app import create_app
    from services import local_workspace as service

    source = tmp_path / "nas" / "photos"
    source.mkdir(parents=True)
    (source / "bird.jpg").write_bytes(b"original")
    vireo_dir = tmp_path / "vireo"
    thumbs = vireo_dir / "thumbnails"
    thumbs.mkdir(parents=True)
    db_path = str(vireo_dir / "vireo.db")

    db = Database(db_path)
    folder_id = db.add_folder(str(source), name="photos")
    db.close()

    # Instrument the shared lock by replacing it with a wrapper that
    # counts (guard, mutation) critical sections. Restored after the test.
    original_stage_guard = service._STAGE_GUARD

    class _CountingLock:
        def __init__(self, inner):
            self._inner = inner
            self.enters = 0
            self.exits = 0

        def acquire(self, *args, **kwargs):
            result = self._inner.acquire(*args, **kwargs)
            if result:
                self.enters += 1
            return result

        def release(self):
            self.exits += 1
            return self._inner.release()

        def __enter__(self):
            self.acquire()
            return self

        def __exit__(self, exc_type, exc, tb):
            self.release()
            return False

    counting = _CountingLock(original_stage_guard)
    monkeypatch.setattr(service, "_STAGE_GUARD", counting)

    app = create_app(db_path, thumb_cache_dir=str(thumbs))
    app.config["TESTING"] = True
    with app.test_client() as client:
        response = client.delete(f"/api/folders/{folder_id}")
        assert response.status_code == 200

    # The route acquired and released the shared boundary lock at least once
    # around its (guard, mutation). A route that skipped the lock would
    # leave both counters at zero, breaking the atomicity guarantee.
    assert counting.enters >= 1
    assert counting.exits == counting.enters


@pytest.mark.skipif(os.name == "nt", reason="Windows test runners may not permit symlinks")
def test_stage_reads_symlink_target_once_during_copy(local_workspace_env, monkeypatch):
    """The copy pass must read the source symlink once, not once per check.

    The old shape called ``_symlink_stays_within`` (which readlinks) and then
    re-read the target with a second ``os.readlink`` before ``os.symlink``.
    A NAS-side swap between those two reads could pass the containment check
    against the safe target and publish the swapped-in escaping target into
    the managed tree. The fix collapses that to a single readlink; the total
    per source is 1 (walk-time check) + 1 (copy pass) = 2.
    """
    env = local_workspace_env
    link_path = env["source"] / "safe-link.jpg"
    os.symlink("root.jpg", link_path)

    calls = []
    real_readlink = local_workspace.os.readlink

    def counting_readlink(path):
        if os.path.samefile(path, str(link_path)):
            calls.append(path)
        return real_readlink(path)

    monkeypatch.setattr(local_workspace.os, "readlink", counting_readlink)
    stage_workspace(env["db"], env["workspace_id"], str(env["vireo_dir"]))
    # Two calls total: one from the walk-time containment check, one from the
    # copy pass. Three or more calls means the copy pass reopened the TOCTOU
    # window this test guards.
    assert len(calls) == 2


def test_status_endpoint_does_not_surface_unrelated_workspace_jobs(tmp_path, monkeypatch):
    """Unrelated jobs on the active workspace must not appear as transfer jobs.

    The workspace page's job watcher treats any ``payload["job"]`` as a
    stage/sync/discard, so a pipeline or scan job running on the same
    workspace would render as "Copying workspace locally..." until it
    finished. The status endpoint filters to the three transfer types so
    unrelated jobs no longer leak into the Work Locally panel.
    """
    monkeypatch.setenv("HOME", str(tmp_path))
    from app import create_app

    source = tmp_path / "nas" / "photos"
    source.mkdir(parents=True)
    (source / "bird.jpg").write_bytes(b"original")
    vireo_dir = tmp_path / "vireo"
    thumbs = vireo_dir / "thumbnails"
    thumbs.mkdir(parents=True)
    db_path = str(vireo_dir / "vireo.db")

    db = Database(db_path)
    db.add_folder(str(source), name="photos")
    workspace_id = db._active_workspace_id
    db.close()

    app = create_app(db_path, thumb_cache_dir=str(thumbs))
    app.config["TESTING"] = True

    hold = threading.Event()
    started = threading.Event()

    def slow_scan(_job):
        started.set()
        assert hold.wait(timeout=5)
        return {"ok": True}

    with app.test_client() as client:
        # Register a non-transfer job on the same workspace. The exact job
        # type doesn't matter as long as it is not one of the three
        # LOCAL_WORKSPACE_JOB_TYPES the status endpoint whitelists.
        job_id = app._job_runner.start(
            "scan", slow_scan, workspace_id=workspace_id
        )
        try:
            assert started.wait(timeout=5)
            payload = client.get("/api/workspaces/active/local-workspace").get_json()
            # No "job" key: the scan is not a Work Locally transfer, so the
            # panel must not report it as one.
            assert "job" not in payload
        finally:
            hold.set()

        # Sanity-check the whitelist: a real transfer job still surfaces.
        stage_response = client.post("/api/workspaces/active/local-workspace/stage", json={})
        assert stage_response.status_code == 202
        stage_job_id = stage_response.get_json()["job_id"]
        during = client.get("/api/workspaces/active/local-workspace").get_json()
        # The status endpoint may race with the job completing on very fast
        # runners; either state (surfaced or already finished) is acceptable
        # as long as the type check whitelisted the transfer job.
        if "job" in during:
            assert during["job"] == {"id": stage_job_id, "type": "work-locally-stage"}
        wait_for_job_via_client(client, stage_job_id)


def test_fk_migration_survives_pending_db_meta_transaction(tmp_path):
    """The FK rebuild must not race an implicit txn from earlier migrations.

    Older DBs upgraded on a build that predates the ``eye_kp_fingerprint_backfill``
    marker: opening one runs an INSERT into ``db_meta`` (implicit sqlite3
    transaction) immediately before the FK migration below. Without the
    ``self.conn.commit()`` guard, ``BEGIN IMMEDIATE`` then raises
    "cannot start a transaction within a transaction" and Vireo refuses to
    open the DB instead of rebuilding the table.
    """
    db_path = str(tmp_path / "legacy.db")
    legacy = Database(db_path)
    legacy.conn.execute("DROP TABLE local_workspace_folders")
    legacy.conn.execute(
        """CREATE TABLE local_workspace_folders (
            workspace_id    INTEGER NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
            folder_id       INTEGER NOT NULL,
            source_path     TEXT NOT NULL,
            local_path      TEXT NOT NULL,
            original_status TEXT NOT NULL DEFAULT 'ok',
            is_root         INTEGER NOT NULL DEFAULT 0,
            root_index      INTEGER,
            PRIMARY KEY (workspace_id, folder_id)
        )"""
    )
    # Force the earlier eye_kp_fingerprint_backfill migration to run on the
    # next open — its INSERT into db_meta opens the implicit transaction
    # that used to collide with BEGIN IMMEDIATE.
    legacy.conn.execute(
        "DELETE FROM db_meta WHERE key='eye_kp_fingerprint_backfill'"
    )
    legacy.conn.commit()
    legacy.close()

    upgraded = Database(db_path)
    fk_after = upgraded.conn.execute(
        "PRAGMA foreign_key_list(local_workspace_folders)"
    ).fetchall()
    assert any(
        row["from"] == "folder_id" and row["table"] == "folders"
        for row in fk_after
    )
    marker = upgraded.conn.execute(
        "SELECT value FROM db_meta WHERE key='eye_kp_fingerprint_backfill'"
    ).fetchone()
    assert marker is not None
    upgraded.close()


def test_sync_recovery_republishes_restored_confirmed_deletion(local_workspace_env, monkeypatch):
    """Recovery must republish a confirmed-deletion file the user restored.

    The first sync attempt already unlinked the source file (it was in the
    user's confirmed deletion set). Restoring the local copy before "Finish
    Sync-back" would otherwise fall through as "unchanged" — restore then
    removes the local copy while the source stays deleted — or, if restored
    with new content, raise a source-missing conflict recovery cannot get out
    of. Both cases must republish the current local file back to source.
    """
    env = local_workspace_env
    stage_workspace(env["db"], env["workspace_id"], str(env["vireo_dir"]))
    local_child = Path(_folder_path(env["db"], env["child_id"]))

    # Snapshot the staged baseline metadata: restoring bird.jpg with these
    # exact values makes it match baseline in ``_local_changes`` (Case A —
    # falls out of both ``changed`` and ``deleted``, so nothing publishes it
    # without the recovery-republish fix).
    baseline_jpg_stat = os.stat(local_child / "bird.jpg")

    # Two confirmed deletions; one will be restored with original metadata,
    # the other with different content after the crash.
    os.unlink(local_child / "bird.jpg")
    os.unlink(local_child / "bird.xmp")

    real_unlink = local_workspace.os.unlink
    source_unlinks = {"count": 0}
    source_root_str = str(env["source"])

    def crashing_unlink(path):
        real_unlink(path)
        # Only trip on source-side deletions (the sync deletion loop). Temp
        # files created inside ``_atomic_publish`` are named ``.<basename>``
        # and are exempt so a restored file's later republish still works.
        basename = os.path.basename(str(path))
        if source_root_str in str(path) and not basename.startswith("."):
            source_unlinks["count"] += 1
            # Let both deletions run so recovery has both keys to reason
            # about, then simulate a mid-sync death.
            if source_unlinks["count"] == 2:
                raise RuntimeError("simulated crash after source deletions")

    monkeypatch.setattr(local_workspace.os, "unlink", crashing_unlink)
    with pytest.raises(RuntimeError, match="simulated crash"):
        sync_back(
            env["db"],
            env["workspace_id"],
            str(env["vireo_dir"]),
            allow_deletions=True,
            confirmed_deletions=2,
        )
    monkeypatch.setattr(local_workspace.os, "unlink", real_unlink)

    assert not (env["child"] / "bird.jpg").exists()
    assert not (env["child"] / "bird.xmp").exists()
    assert status(env["db"], env["workspace_id"], str(env["vireo_dir"]))["state"] == "recovery"

    # Case A: restore bird.jpg with its original content AND original mtime,
    # so ``_same_metadata`` returns True and the key doesn't reach ``changed``
    # or ``deleted`` on its own.
    (local_child / "bird.jpg").write_bytes(b"bird-original")
    os.utime(
        local_child / "bird.jpg",
        ns=(baseline_jpg_stat.st_atime_ns, baseline_jpg_stat.st_mtime_ns),
    )
    # Case B: restore bird.xmp with different content. It lands in ``changed``
    # naturally and — without the conflict-scan bypass — the missing source
    # would raise a source-side conflict recovery cannot recover from.
    (local_child / "bird.xmp").write_text("edited-after-restore", encoding="utf-8")

    result = sync_back(env["db"], env["workspace_id"], str(env["vireo_dir"]))

    assert result["ok"] is True
    assert (env["child"] / "bird.jpg").read_bytes() == b"bird-original"
    assert (env["child"] / "bird.xmp").read_text(encoding="utf-8") == "edited-after-restore"
    assert status(env["db"], env["workspace_id"], str(env["vireo_dir"]))["state"] == "remote"


def test_sync_recovery_leaves_unrestored_deletions_deleted(local_workspace_env, monkeypatch):
    """Recovery still deletes confirmed-deletion files the user did not restore.

    Complement to the republish test: a confirmed deletion whose local copy
    remains absent on resume must not be resurrected — the user already
    approved that deletion, the first attempt already ran ``os.unlink`` on
    the source, and no republish path should touch it.
    """
    env = local_workspace_env
    stage_workspace(env["db"], env["workspace_id"], str(env["vireo_dir"]))
    local_child = Path(_folder_path(env["db"], env["child_id"]))
    os.unlink(local_child / "bird.jpg")

    real_unlink = local_workspace.os.unlink

    def crashing_unlink(path):
        real_unlink(path)
        raise RuntimeError("simulated crash after source deletion")

    monkeypatch.setattr(local_workspace.os, "unlink", crashing_unlink)
    with pytest.raises(RuntimeError, match="simulated crash"):
        sync_back(
            env["db"],
            env["workspace_id"],
            str(env["vireo_dir"]),
            allow_deletions=True,
            confirmed_deletions=1,
        )
    monkeypatch.setattr(local_workspace.os, "unlink", real_unlink)

    # User does not restore the local file. Resume should complete cleanly
    # and leave the source deleted.
    sync_back(env["db"], env["workspace_id"], str(env["vireo_dir"]))
    assert not (env["child"] / "bird.jpg").exists()
    assert status(env["db"], env["workspace_id"], str(env["vireo_dir"]))["state"] == "remote"


def test_scan_endpoint_refuses_while_working_locally(tmp_path, monkeypatch):
    """``POST /api/jobs/scan`` must refuse while the active workspace is staged.

    ``scanner.scan()`` calls ``db.add_folder(link_to_workspace=True)`` for
    every discovered folder, so a scan started against a new root while
    local work is active would add folder/workspace_folders rows that the
    manifest and ``local_workspace_folders`` don't cover — sync and discard
    could not rebase or remove them. The scan endpoint mirrors the
    folder-add/remove/move-folders guards and returns 409.
    """
    monkeypatch.setenv("HOME", str(tmp_path))
    from app import create_app

    source = tmp_path / "nas" / "photos"
    source.mkdir(parents=True)
    (source / "bird.jpg").write_bytes(b"original")
    other_source = tmp_path / "nas" / "other"
    other_source.mkdir(parents=True)
    (other_source / "kestrel.jpg").write_bytes(b"another root")

    vireo_dir = tmp_path / "vireo"
    thumbs = vireo_dir / "thumbnails"
    thumbs.mkdir(parents=True)
    db_path = str(vireo_dir / "vireo.db")

    db = Database(db_path)
    db.add_folder(str(source), name="photos")
    workspace_id = db._active_workspace_id
    db.close()

    app = create_app(db_path, thumb_cache_dir=str(thumbs))
    app.config["TESTING"] = True

    with app.test_client() as client:
        # Sanity: a scan is accepted before any local staging.
        pre = client.post("/api/jobs/scan", json={"root": str(other_source)})
        assert pre.status_code == 200
        # Let the scan complete so it can't collide with the stage below.
        wait_for_job_via_client(client, pre.get_json()["job_id"])

        # Stage the workspace locally, then attempt to scan a new root.
        stage_response = client.post(
            "/api/workspaces/active/local-workspace/stage", json={}
        )
        assert stage_response.status_code == 202
        wait_for_job_via_client(client, stage_response.get_json()["job_id"])

        third = tmp_path / "nas" / "third"
        third.mkdir()
        (third / "owl.jpg").write_bytes(b"third root")
        blocked = client.post("/api/jobs/scan", json={"root": str(third)})
        assert blocked.status_code == 409
        assert "working locally" in blocked.get_json()["error"].lower()

        # After discarding the local copy, the scan is allowed again.
        discarded = client.post(
            "/api/workspaces/active/local-workspace/discard",
            json={"confirm": True},
        )
        assert discarded.status_code == 202
        wait_for_job_via_client(client, discarded.get_json()["job_id"])

        allowed = client.post("/api/jobs/scan", json={"root": str(third)})
        assert allowed.status_code == 200


def test_scan_endpoint_refuses_while_transition_job_is_pending(tmp_path, monkeypatch):
    """A stage/sync/discard job queued or running but not yet at its
    ``local_workspaces`` write must still block a scan.

    ``has_local_workspace`` only sees a completed stage claim, so a scan
    enqueued in the window between "transition job registered" and
    "transition worker inserts the state row" would silently pass the
    workspace-state guard and later add folder/workspace_folders rows
    outside the manifest that the transition is about to build.
    """
    monkeypatch.setenv("HOME", str(tmp_path))
    from app import create_app

    source = tmp_path / "nas" / "photos"
    source.mkdir(parents=True)
    (source / "bird.jpg").write_bytes(b"original")

    vireo_dir = tmp_path / "vireo"
    thumbs = vireo_dir / "thumbnails"
    thumbs.mkdir(parents=True)
    db_path = str(vireo_dir / "vireo.db")

    db = Database(db_path)
    db.add_folder(str(source), name="photos")
    workspace_id = db._active_workspace_id
    db.close()

    app = create_app(db_path, thumb_cache_dir=str(thumbs))
    app.config["TESTING"] = True

    hold = threading.Event()
    started = threading.Event()

    def slow_transition(_job):
        started.set()
        assert hold.wait(timeout=5)
        return {"ok": True}

    with app.test_client() as client:
        # Simulate a stage/sync/discard job that has been enqueued and
        # started but hasn't yet reached the state-row insert. The
        # ``local_workspaces`` row is deliberately never written by this
        # stub — the pending-transition guard alone must reject the scan.
        job_id = app._job_runner.start(
            "work-locally-stage", slow_transition, workspace_id=workspace_id
        )
        try:
            assert started.wait(timeout=5)

            other = tmp_path / "nas" / "other"
            other.mkdir()
            (other / "kestrel.jpg").write_bytes(b"another root")
            blocked = client.post("/api/jobs/scan", json={"root": str(other)})
            assert blocked.status_code == 409
            body = blocked.get_json()["error"].lower()
            assert "work-locally-stage" in body
        finally:
            hold.set()
        wait_for_job_via_client(client, job_id)


def test_move_folder_endpoint_refuses_while_transition_job_is_pending(tmp_path, monkeypatch):
    """The move-folder guard must also cover pending stage transitions.

    ``folder_has_local_workspace`` only sees a completed stage claim.
    Moving a folder that a queued/running stage worker is about to claim
    would rewrite the ``folders`` row out from under the transition and
    drop the workspace into missing-local recovery.
    """
    monkeypatch.setenv("HOME", str(tmp_path))
    from app import create_app

    source = tmp_path / "nas" / "photos"
    source.mkdir(parents=True)
    (source / "bird.jpg").write_bytes(b"original")

    vireo_dir = tmp_path / "vireo"
    thumbs = vireo_dir / "thumbnails"
    thumbs.mkdir(parents=True)
    db_path = str(vireo_dir / "vireo.db")

    db = Database(db_path)
    folder_id = db.add_folder(str(source), name="photos")
    workspace_id = db._active_workspace_id
    db.close()

    app = create_app(db_path, thumb_cache_dir=str(thumbs))
    app.config["TESTING"] = True

    hold = threading.Event()
    started = threading.Event()

    def slow_transition(_job):
        started.set()
        assert hold.wait(timeout=5)
        return {"ok": True}

    destination = tmp_path / "nas" / "moved"

    with app.test_client() as client:
        job_id = app._job_runner.start(
            "work-locally-stage", slow_transition, workspace_id=workspace_id
        )
        try:
            assert started.wait(timeout=5)

            blocked = client.post(
                "/api/jobs/move-folder",
                json={"folder_id": folder_id, "destination": str(destination)},
            )
            assert blocked.status_code == 409
            body = blocked.get_json()["error"].lower()
            assert "work-locally-stage" in body
        finally:
            hold.set()
        wait_for_job_via_client(client, job_id)
