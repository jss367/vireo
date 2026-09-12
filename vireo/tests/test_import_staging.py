import pytest
from import_staging import plan_staged_import


def test_staging_is_isolated_and_retries_keep_mount_evidence(tmp_path, monkeypatch):
    import pipeline_job

    destination = str(tmp_path / "NAS" / "trip")
    vireo_dir = str(tmp_path / ".vireo")
    monkeypatch.setattr(pipeline_job, "_archive_mount_baseline", lambda *a: {"/mnt/photos": True})
    first, _ = plan_staged_import(vireo_dir, destination)
    second, _ = plan_staged_import(vireo_dir, destination)
    assert first["destination"] != second["destination"]
    monkeypatch.setattr(pipeline_job, "_archive_mount_baseline", lambda *a: {"/mnt/photos": False})
    retry, target = plan_staged_import(vireo_dir, destination, parent=first)
    assert retry == first
    assert target["mount_baseline"]["/mnt/photos"] is True


def test_staging_freezes_mounted_destination_symlink(tmp_path):
    original = tmp_path / "original-NAS"
    replacement = tmp_path / "other-NAS"
    original.mkdir()
    replacement.mkdir()
    alias = tmp_path / "photos"
    alias.symlink_to(original, target_is_directory=True)
    plan, target = plan_staged_import(str(tmp_path / ".vireo"), str(alias / "trip"))
    alias.unlink()
    alias.symlink_to(replacement, target_is_directory=True)
    assert target["mount_path"] == str(original)
    with pytest.raises(ValueError, match="destination has changed"):
        plan_staged_import(str(tmp_path / ".vireo"), str(alias / "trip"), parent=plan)
