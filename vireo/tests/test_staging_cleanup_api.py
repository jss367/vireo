"""``DELETE /api/import/orphaned-staging``: the job that removes staged files.

The route unlinks files, so it must be told the user agreed, must refuse a
path that is not a staging folder before any job starts, and must delete
only the files whose archive copy is identical.
"""
from pathlib import Path

from app_paths import media_root
from services import photo_ingest, workspace_folders
from wait import wait_for_job_via_client


def _write(path, data=b"abc"):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    return path


def _staging_root(app, name):
    return Path(media_root(app.config["THUMB_CACHE_DIR"])) / "staging" / name


def test_delete_refuses_without_confirmation_and_starts_no_job(app_and_db):
    app, _db = app_and_db
    root = _staging_root(app, "pipeline-unconfirmed")
    staged = _write(root / "USA" / "a.nef")
    client = app.test_client()

    for body in ({"path": str(root)}, {"path": str(root), "confirm_permanent": "true"}):
        resp = client.delete("/api/import/orphaned-staging", json=body)

        assert resp.status_code == 400
        assert "confirm_permanent" in resp.get_json()["error"]
    assert app._job_runner.list_jobs() == []
    assert staged.exists()


def test_delete_refuses_a_path_outside_staging(app_and_db, tmp_path):
    app, _db = app_and_db
    outside = tmp_path / "pipeline-elsewhere"
    kept = _write(outside / "a.nef")

    resp = app.test_client().delete("/api/import/orphaned-staging", json={
        "path": str(outside), "confirm_permanent": True,
    })

    assert resp.status_code == 400
    assert "outside the Vireo staging directory" in resp.get_json()["error"]
    assert app._job_runner.list_jobs() == []
    assert kept.exists()


def test_delete_job_removes_identical_files_and_reports_what_it_kept(app_and_db, tmp_path):
    app, db = app_and_db
    root = _staging_root(app, "pipeline-route")
    staged = _write(root / "USA" / "a.nef")
    kept = _write(root / "USA" / "b.nef", b"not archived")
    archive_file = _write(tmp_path / "archive" / "USA" / "a.nef")
    folder_id = workspace_folders.add_folder(db, str(archive_file.parent), name="USA")
    photo_ingest.add_photo(db, folder_id, "a.nef", ".nef", staged.stat().st_size, 1.0)
    db.commit()
    client = app.test_client()

    resp = client.delete("/api/import/orphaned-staging", json={
        "path": str(root), "confirm_permanent": True,
    })
    assert resp.status_code == 200
    finished = wait_for_job_via_client(client, resp.get_json()["job_id"])

    assert finished["status"] == "completed"
    assert finished["type"] == "staging-delete"
    result = finished["result"]
    assert (result["deleted"], result["kept"], result["folder_removed"]) == (1, 1, False)
    assert result["details"][0]["rel_path"] == "b.nef"
    assert not staged.exists()
    assert kept.read_bytes() == b"not archived"
    assert archive_file.read_bytes() == b"abc"


def test_delete_job_removes_a_fully_matching_folder(app_and_db, tmp_path):
    app, db = app_and_db
    root = _staging_root(app, "pipeline-clean")
    staged = _write(root / "USA" / "2026" / "a.nef")
    archive_file = _write(tmp_path / "archive" / "USA" / "2026" / "a.nef")
    folder_id = workspace_folders.add_folder(db, str(archive_file.parent), name="2026")
    photo_ingest.add_photo(db, folder_id, "a.nef", ".nef", staged.stat().st_size, 1.0)
    db.commit()
    client = app.test_client()

    resp = client.delete("/api/import/orphaned-staging", json={
        "path": str(root), "confirm_permanent": True,
    })
    finished = wait_for_job_via_client(client, resp.get_json()["job_id"])

    assert finished["status"] == "completed"
    assert finished["result"]["folder_removed"] is True
    assert not root.exists()
    assert archive_file.exists()
    assert client.get("/api/import/orphaned-staging").get_json()["items"] == []
