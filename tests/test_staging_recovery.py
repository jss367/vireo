import os

import pytest
import staging_recovery
from services import photo_ingest, workspace_folders
from staging_recovery import (
    delete_verified_staging,
    discover_orphaned_staging,
    verify_orphaned_staging,
)


@pytest.fixture
def client(tmp_path, monkeypatch):
    import config as cfg
    import models
    from app import create_app

    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    monkeypatch.setattr(models, "DEFAULT_MODELS_DIR", str(tmp_path / "vireo-models"))
    monkeypatch.setattr(models, "CONFIG_PATH", str(tmp_path / "models.json"))

    app = create_app(str(tmp_path / "test.db"))
    app.config["TESTING"] = True
    with app.test_client() as c:
        yield c


def _write(path, data=b"abc"):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    return path


def _catalog_photo(db, folder_path, filename, size):
    folder_id = workspace_folders.add_folder(db, str(folder_path), name=os.path.basename(str(folder_path)))
    return photo_ingest.add_photo(db, folder_id, filename, os.path.splitext(filename)[1], size, 1.0)


def test_orphaned_staging_verified_files_can_be_deleted(db, tmp_path):
    vireo_dir = tmp_path / "vireo"
    staged = _write(
        vireo_dir / "staging" / "pipeline-1" / "USA" / "2026" / "2026-07-03" / "a.nef"
    )
    archive_file = _write(
        tmp_path / "archive" / "USA" / "2026" / "2026-07-03" / "a.nef"
    )
    _catalog_photo(db, archive_file.parent, "a.nef", staged.stat().st_size)

    discovered = discover_orphaned_staging(str(vireo_dir))
    assert discovered[0]["source_root"].endswith(os.path.join("pipeline-1", "USA"))

    result = verify_orphaned_staging(
        db, str(vireo_dir), str(vireo_dir / "staging" / "pipeline-1")
    )

    assert result["status"] == "safe_to_delete"
    assert result["can_delete"] is True
    assert result["verified"] == 1
    assert result["inferred_destination"] == str(tmp_path / "archive" / "USA")

    assert result["verified_bytes"] == staged.stat().st_size
    assert result["details"] == []

    outcome = delete_verified_staging(
        db, str(vireo_dir), str(vireo_dir / "staging" / "pipeline-1")
    )

    assert not (vireo_dir / "staging" / "pipeline-1").exists()
    assert archive_file.read_bytes() == b"abc"
    assert outcome["deleted"] == 1
    assert outcome["deleted_bytes"] == 3
    assert outcome["kept"] == 0
    assert outcome["folder_removed"] is True
    assert outcome["status"] == "deleted"
    assert outcome["ok"] is True


def test_orphaned_staging_missing_archive_copy_blocks_delete(db, tmp_path):
    vireo_dir = tmp_path / "vireo"
    staged = _write(
        vireo_dir / "staging" / "pipeline-2" / "USA" / "2026" / "2026-07-03" / "b.nef"
    )
    archive_dir = tmp_path / "archive" / "USA" / "2026" / "2026-07-03"
    archive_dir.mkdir(parents=True)
    _catalog_photo(db, archive_dir, "b.nef", staged.stat().st_size)

    result = verify_orphaned_staging(
        db, str(vireo_dir), str(vireo_dir / "staging" / "pipeline-2")
    )

    assert result["status"] == "needs_import"
    assert result["can_delete"] is False
    assert result["unaccounted"] == 1
    assert "not present" in result["details"][0]["reason"]

    outcome = delete_verified_staging(
        db, str(vireo_dir), str(vireo_dir / "staging" / "pipeline-2")
    )

    assert staged.exists()
    assert outcome["deleted"] == 0
    assert outcome["kept"] == 1
    assert outcome["folder_removed"] is False
    assert outcome["status"] == "needs_import"


def test_orphaned_staging_ignores_matching_archive_in_other_workspace(db, tmp_path):
    active_ws = db._active_workspace_id
    other_ws = db.create_workspace("Other")
    db.commit()
    vireo_dir = tmp_path / "vireo"
    staged = _write(
        vireo_dir / "staging" / "pipeline-other-ws" / "USA" / "2026" / "a.nef"
    )
    archive_file = _write(tmp_path / "archive-other" / "USA" / "2026" / "a.nef")

    db.set_active_workspace(other_ws)
    _catalog_photo(db, archive_file.parent, "a.nef", staged.stat().st_size)
    db.set_active_workspace(active_ws)

    result = verify_orphaned_staging(
        db, str(vireo_dir), str(vireo_dir / "staging" / "pipeline-other-ws")
    )

    assert result["status"] == "needs_import"
    assert result["can_delete"] is False
    assert result["unaccounted"] == 1
    assert result["verified"] == 0


def test_orphaned_staging_requires_matching_archive_relative_folder(db, tmp_path):
    vireo_dir = tmp_path / "vireo"
    staged = _write(
        vireo_dir / "staging" / "pipeline-path" / "USA" / "2026" / "2026-07-03" / "a.nef"
    )
    archive_file = _write(
        tmp_path / "archive" / "USA" / "2026" / "2026-07-04" / "a.nef"
    )
    _catalog_photo(db, archive_file.parent, "a.nef", staged.stat().st_size)

    result = verify_orphaned_staging(
        db, str(vireo_dir), str(vireo_dir / "staging" / "pipeline-path")
    )

    assert result["status"] == "needs_import"
    assert result["can_delete"] is False
    assert result["unaccounted"] == 1
    assert result["verified"] == 0


def test_orphaned_staging_requires_matching_destination_leaf(db, tmp_path):
    vireo_dir = tmp_path / "vireo"
    staged = _write(vireo_dir / "staging" / "pipeline-leaf" / "USA" / "a.nef")
    archive_file = _write(tmp_path / "archive" / "Brazil" / "a.nef")
    _catalog_photo(db, archive_file.parent, "a.nef", staged.stat().st_size)

    result = verify_orphaned_staging(
        db, str(vireo_dir), str(vireo_dir / "staging" / "pipeline-leaf")
    )

    assert result["status"] == "needs_import"
    assert result["can_delete"] is False
    assert result["unaccounted"] == 1
    assert result["verified"] == 0


def test_orphaned_staging_reuses_archive_folder_listing(db, tmp_path, monkeypatch):
    vireo_dir = tmp_path / "vireo"
    staged_a = _write(vireo_dir / "staging" / "pipeline-cache" / "USA" / "a.nef")
    staged_b = _write(vireo_dir / "staging" / "pipeline-cache" / "USA" / "b.nef")
    archive_dir = tmp_path / "archive" / "USA"
    _write(archive_dir / "a.nef", staged_a.read_bytes())
    _write(archive_dir / "b.nef", staged_b.read_bytes())
    _catalog_photo(db, archive_dir, "a.nef", staged_a.stat().st_size)
    _catalog_photo(db, archive_dir, "b.nef", staged_b.stat().st_size)

    real_listdir = os.listdir
    archive_list_calls = []

    def counting_listdir(path):
        if os.path.realpath(path) == os.path.realpath(archive_dir):
            archive_list_calls.append(path)
        return real_listdir(path)

    monkeypatch.setattr(staging_recovery.os, "listdir", counting_listdir)

    result = verify_orphaned_staging(
        db, str(vireo_dir), str(vireo_dir / "staging" / "pipeline-cache")
    )

    assert result["status"] == "safe_to_delete"
    assert result["verified"] == 2
    assert len(archive_list_calls) == 1


def test_orphaned_staging_unreachable_archive_is_not_called_missing(db, tmp_path):
    vireo_dir = tmp_path / "vireo"
    staged = _write(
        vireo_dir / "staging" / "pipeline-3" / "USA" / "2026" / "2026-07-03" / "c.nef"
    )
    archive_dir = tmp_path / "offline-archive" / "USA" / "2026" / "2026-07-03"
    _catalog_photo(db, archive_dir, "c.nef", staged.stat().st_size)

    result = verify_orphaned_staging(
        db, str(vireo_dir), str(vireo_dir / "staging" / "pipeline-3")
    )

    assert result["status"] == "unreachable"
    assert result["can_delete"] is False
    assert result["unreachable"] == 1
    assert "not enumerable" in result["details"][0]["reason"]


def _staged_with_archive(db, tmp_path, name, staged_bytes=b"abc", archive_bytes=None):
    """One staged file under ``pipeline-<name>/USA`` with a cataloged archive
    file of the same name and size."""
    vireo_dir = tmp_path / "vireo"
    root = vireo_dir / "staging" / f"pipeline-{name}"
    staged = _write(root / "USA" / "a.nef", staged_bytes)
    archive_file = _write(
        tmp_path / "archive" / "USA" / "a.nef",
        staged_bytes if archive_bytes is None else archive_bytes,
    )
    _catalog_photo(db, archive_file.parent, "a.nef", len(staged_bytes))
    return vireo_dir, root, staged, archive_file


def test_same_name_and_size_with_different_bytes_is_not_verified(db, tmp_path):
    """A name and size match is not evidence: the staged file may be the only
    intact copy of bytes the archive file does not hold."""
    vireo_dir, root, staged, archive_file = _staged_with_archive(
        db, tmp_path, "bytes", staged_bytes=b"good bytes", archive_bytes=b"bad! bytes",
    )

    result = verify_orphaned_staging(db, str(vireo_dir), str(root))

    assert result["verified"] == 0
    assert result["unaccounted"] == 1
    assert result["can_delete"] is False
    assert result["status"] == "needs_import"
    assert "different contents" in result["details"][0]["reason"]

    outcome = delete_verified_staging(db, str(vireo_dir), str(root))

    assert staged.read_bytes() == b"good bytes"
    assert archive_file.read_bytes() == b"bad! bytes"
    assert outcome["deleted"] == 0
    assert outcome["kept"] == 1
    assert outcome["folder_removed"] is False


def test_delete_compares_again_and_keeps_a_file_that_no_longer_matches(db, tmp_path):
    """The delete does its own comparison, so an archive file that changed
    after the verification the user saw protects the staged copy."""
    vireo_dir, root, staged, archive_file = _staged_with_archive(db, tmp_path, "recheck")
    assert verify_orphaned_staging(db, str(vireo_dir), str(root))["can_delete"] is True

    archive_file.write_bytes(b"xyz")
    outcome = delete_verified_staging(db, str(vireo_dir), str(root))

    assert staged.read_bytes() == b"abc"
    assert outcome["deleted"] == 0
    assert outcome["folder_removed"] is False


def test_delete_removes_matching_files_and_keeps_the_rest(db, tmp_path):
    vireo_dir, root, staged, _archive_file = _staged_with_archive(db, tmp_path, "mixed")
    stray = _write(root / "USA" / "notes.txt", b"only copy")
    other = _write(root / "USA" / "2026" / "b.nef", b"not archived")

    result = verify_orphaned_staging(db, str(vireo_dir), str(root))
    assert (result["verified"], result["unaccounted"], result["can_delete"]) == (1, 2, False)

    outcome = delete_verified_staging(db, str(vireo_dir), str(root))

    assert not staged.exists()
    assert stray.read_bytes() == b"only copy"
    assert other.read_bytes() == b"not archived"
    assert outcome["deleted"] == 1
    assert outcome["kept"] == 2
    assert outcome["folder_removed"] is False
    assert outcome["status"] == "needs_import"
    assert sorted(d["rel_path"] for d in outcome["details"]) == [
        os.path.join("2026", "b.nef"), "notes.txt",
    ]


def test_unreadable_subfolder_is_reported_and_survives_the_delete(db, tmp_path, monkeypatch):
    """A folder the pass cannot list holds files nobody compared, so it is
    named as unchecked and nothing in it is removed."""
    vireo_dir, root, staged, _archive_file = _staged_with_archive(db, tmp_path, "locked")
    hidden = _write(root / "USA" / "locked" / "c.nef", b"never listed")
    locked = os.path.realpath(hidden.parent)
    real_scandir = os.scandir

    def scandir(path):
        if os.path.realpath(path) == locked:
            raise PermissionError(13, "Permission denied", locked)
        return real_scandir(path)

    monkeypatch.setattr(staging_recovery.os, "scandir", scandir)

    result = verify_orphaned_staging(db, str(vireo_dir), str(root))

    assert result["verified"] == 1
    assert result["unchecked"] == 1
    assert result["can_delete"] is False
    assert result["status"] == "unchecked"
    assert result["details"][0]["rel_path"] == "locked"
    assert "could not be read" in result["details"][0]["reason"]

    outcome = delete_verified_staging(db, str(vireo_dir), str(root))

    assert not staged.exists()
    assert hidden.read_bytes() == b"never listed"
    assert outcome["deleted"] == 1
    assert outcome["kept"] == 1
    assert outcome["folder_removed"] is False
    assert outcome["details"][0]["rel_path"] == "locked"


def test_links_are_reported_and_never_followed_or_deleted(db, tmp_path):
    vireo_dir, root, staged, _archive_file = _staged_with_archive(db, tmp_path, "links")
    outside = _write(tmp_path / "elsewhere" / "keep.nef", b"outside staging")
    link = root / "USA" / "link"
    try:
        os.symlink(outside.parent, link, target_is_directory=True)
    except (OSError, NotImplementedError):
        pytest.skip("symbolic links are not available here")

    result = verify_orphaned_staging(db, str(vireo_dir), str(root))

    assert result["file_count"] == 1
    assert result["unchecked"] == 1
    assert result["can_delete"] is False
    assert "is a link" in result["details"][0]["reason"]

    outcome = delete_verified_staging(db, str(vireo_dir), str(root))

    assert not staged.exists()
    assert os.path.islink(link)
    assert outside.read_bytes() == b"outside staging"
    assert outcome["folder_removed"] is False


def test_archive_path_that_is_the_staged_file_itself_is_not_a_second_copy(db, tmp_path):
    """A cataloged path that leads back to the staged file would compare
    equal to itself; deleting on that would delete the only copy."""
    vireo_dir = tmp_path / "vireo"
    root = vireo_dir / "staging" / "pipeline-self"
    staged = _write(root / "USA" / "a.nef")
    archive_dir = tmp_path / "archive" / "USA"
    archive_dir.mkdir(parents=True)
    try:
        os.link(staged, archive_dir / "a.nef")
    except (OSError, NotImplementedError):
        pytest.skip("hard links are not available here")
    _catalog_photo(db, archive_dir, "a.nef", staged.stat().st_size)

    result = verify_orphaned_staging(db, str(vireo_dir), str(root))

    assert result["verified"] == 0
    assert result["unaccounted"] == 1
    assert "not a second copy" in result["details"][0]["reason"]
    delete_verified_staging(db, str(vireo_dir), str(root))
    assert staged.exists()


def test_file_that_changes_after_its_comparison_is_kept(db, tmp_path, monkeypatch):
    vireo_dir, root, staged, _archive_file = _staged_with_archive(db, tmp_path, "changed")
    real_compare = staging_recovery._compare_contents

    def compare_then_rewrite(staged_file, archive_path, pause):
        verdict = real_compare(staged_file, archive_path, pause)
        with open(staged_file.path, "wb") as f:
            f.write(b"rewritten after the comparison")
        return verdict

    monkeypatch.setattr(staging_recovery, "_compare_contents", compare_then_rewrite)

    outcome = delete_verified_staging(db, str(vireo_dir), str(root))

    assert staged.read_bytes() == b"rewritten after the comparison"
    assert outcome["deleted"] == 0
    assert outcome["unchecked"] == 1
    assert "changed after it was compared" in outcome["details"][0]["reason"]


def test_file_that_cannot_be_deleted_fails_the_cleanup(db, tmp_path, monkeypatch):
    vireo_dir, root, staged, _archive_file = _staged_with_archive(db, tmp_path, "stuck")

    def refuse(path):
        raise PermissionError(13, "Permission denied", str(path))

    monkeypatch.setattr(staging_recovery.os, "unlink", refuse)

    outcome = delete_verified_staging(db, str(vireo_dir), str(root))

    assert staged.exists()
    assert outcome["ok"] is False
    assert outcome["delete_failed"] == 1
    assert outcome["status"] == "delete_failed"
    assert outcome["errors_total"] == 1
    assert outcome["errors"][0].startswith("a.nef: matches the archive but could not be deleted")


def test_file_added_during_the_delete_keeps_its_folder(db, tmp_path, monkeypatch):
    """Only empty directories are removed, so a file that appears after the
    walk is never deleted with its folder."""
    vireo_dir, root, staged, _archive_file = _staged_with_archive(db, tmp_path, "late")
    late = root / "USA" / "late.nef"
    real_delete = staging_recovery._delete_compared_file

    def delete_then_add(staged_file):
        verdict = real_delete(staged_file)
        late.write_bytes(b"arrived late")
        return verdict

    monkeypatch.setattr(staging_recovery, "_delete_compared_file", delete_then_add)

    outcome = delete_verified_staging(db, str(vireo_dir), str(root))

    assert not staged.exists()
    assert late.read_bytes() == b"arrived late"
    assert outcome["folder_removed"] is False
    assert outcome["kept"] == 1
    assert "could not be removed" in outcome["details"][0]["reason"]


def test_details_are_bounded_and_the_total_is_not(db, tmp_path, monkeypatch):
    monkeypatch.setattr(staging_recovery, "DETAIL_LIMIT", 2)
    vireo_dir = tmp_path / "vireo"
    root = vireo_dir / "staging" / "pipeline-many"
    for index in range(5):
        _write(root / "USA" / f"{index}.nef")

    result = verify_orphaned_staging(db, str(vireo_dir), str(root))

    assert result["unaccounted"] == 5
    assert result["details_total"] == 5
    assert len(result["details"]) == 2


def test_progress_reports_each_compared_file(db, tmp_path):
    vireo_dir, root, _staged, _archive_file = _staged_with_archive(db, tmp_path, "progress")
    _write(root / "USA" / "b.nef")
    seen = []

    verify_orphaned_staging(
        db, str(vireo_dir), str(root),
        progress_callback=lambda current, total, name: seen.append((current, total, name)),
    )

    assert seen == [(1, 2, "a.nef"), (2, 2, "b.nef")]


def test_process_pipeline_rejects_retired_import_fields(client, tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    destination = tmp_path / "archive"

    resp = client.post(
        "/api/jobs/pipeline",
        json={
            "source": str(source),
            "destination": str(destination),
            "skip_classify": True,
            "skip_extract_masks": True,
            "skip_regroup": True,
        },
    )

    assert resp.status_code == 400
    assert "use Import" in resp.get_json()["error"]


def test_process_plan_rejects_retired_import_fields(client):
    resp = client.post(
        "/api/pipeline/plan",
        json={"source_paths": [], "local_processing": True},
    )

    assert resp.status_code == 400
    assert "import/archive fields" in resp.get_json()["error"]
