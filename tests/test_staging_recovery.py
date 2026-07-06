import os

import pytest
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
    folder_id = db.add_folder(str(folder_path), name=os.path.basename(str(folder_path)))
    return db.add_photo(folder_id, filename, os.path.splitext(filename)[1], size, 1.0)


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

    delete_verified_staging(
        db, str(vireo_dir), str(vireo_dir / "staging" / "pipeline-1")
    )
    assert not (vireo_dir / "staging" / "pipeline-1").exists()


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
    with pytest.raises(ValueError):
        delete_verified_staging(
            db, str(vireo_dir), str(vireo_dir / "staging" / "pipeline-2")
        )
    assert (vireo_dir / "staging" / "pipeline-2").exists()


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
    assert "import/archive fields" in resp.get_json()["error"]


def test_process_plan_rejects_retired_import_fields(client):
    resp = client.post(
        "/api/pipeline/plan",
        json={"source_paths": [], "local_processing": True},
    )

    assert resp.status_code == 400
    assert "import/archive fields" in resp.get_json()["error"]
