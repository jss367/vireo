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
        assert blocked_delete.status_code == 400
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
