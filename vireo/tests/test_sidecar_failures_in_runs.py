"""A scan inside Process or an import names the sidecars it could not import.

#2294 made a standalone scan job fail and name each photo whose XMP sidecar
keywords were not imported. The same scan run as the Process pipeline's scan
stage, or as part of an import, only logged it. Each of those runs now records
one error line per sidecar (the job ends failed, per the rollup rule), counts
them in its scan or import step summary, and still catalogs and processes
every photo.
"""

import os

import pytest
from PIL import Image
from test_import_services import import_service  # noqa: F401  (fixture)

CORRUPT_XMP = "<x:xmpmeta xmlns:x='adobe:ns:meta/'><broken"
UNREADABLE = "Vireo could not read it, or it is not valid XMP"


def _line(xmp_path, filename="heron.jpg"):
    return (
        f"{filename}: keywords in {xmp_path} were not imported "
        f"({UNREADABLE}). Vireo tries again on the next scan."
    )


def _folder_with_corrupt_sidecar(folder):
    """``heron.jpg`` with an unreadable own sidecar, plus a plain ``egret.jpg``."""
    folder.mkdir(parents=True, exist_ok=True)
    Image.new("RGB", (60, 40), "blue").save(folder / "heron.jpg")
    Image.new("RGB", (60, 40), "red").save(folder / "egret.jpg")
    (folder / "heron.xmp").write_text(CORRUPT_XMP)
    return folder / "heron.xmp"


@pytest.fixture
def no_metadata_warning(monkeypatch):
    """Keep step summaries independent of whether exiftool is installed."""
    import metadata
    from services import import_in_place

    monkeypatch.setattr(metadata, "scan_metadata_warning", lambda: None)
    monkeypatch.setattr(import_in_place, "scan_metadata_warning", lambda: None)


def _last_step_update(runner, step_id):
    updates = [kw for _job, step, kw in runner.step_updates if step == step_id and "status" in kw]
    return updates[-1]


def test_process_scan_stage_names_the_photo_and_runs_on(tmp_path, no_metadata_warning):
    from db import Database
    from pipeline_job import PipelineParams, run_pipeline_job
    from test_pipeline_job import FakeRunner, _make_job

    root = tmp_path / "photos"
    xmp = _folder_with_corrupt_sidecar(root)
    db_path = str(tmp_path / "test.db")
    ws_id = Database(db_path)._active_workspace_id
    runner = FakeRunner()
    job = _make_job()

    result = run_pipeline_job(job, runner, db_path, ws_id, PipelineParams(
        source=str(root), skip_classify=True, skip_extract_masks=True, skip_regroup=True,
    ))

    expected = "[scan] " + _line(os.path.realpath(xmp))
    assert job["errors"] == [expected]
    # The run fails on it (not a note) but every stage still ran.
    assert result["ok"] is False
    assert expected in result["errors"] and expected not in result["notes"]
    stages = [data["stages"] for _id, kind, data in runner.events
              if kind == "progress" and "stages" in data][-1]
    assert stages["scan"]["status"] == "completed"
    assert stages["scan"]["error_count"] == 1
    assert stages["thumbnails"]["status"] == "completed"
    scan_step = _last_step_update(runner, "scan")
    assert scan_step["status"] == "failed"
    assert scan_step["summary"] == "2 photos (keywords from 1 sidecar not imported)"
    assert scan_step["error"] == _line(os.path.realpath(xmp))
    assert scan_step["error_count"] == 1
    assert len(Database(db_path).photos.list_page(per_page=10)) == 2


def test_process_without_sidecar_failures_reports_nothing(tmp_path, no_metadata_warning):
    from db import Database
    from pipeline_job import PipelineParams, run_pipeline_job
    from test_pipeline_job import FakeRunner, _make_job

    root = tmp_path / "photos"
    root.mkdir()
    Image.new("RGB", (60, 40), "blue").save(root / "heron.jpg")
    db_path = str(tmp_path / "test.db")
    ws_id = Database(db_path)._active_workspace_id
    runner = FakeRunner()
    job = _make_job()

    result = run_pipeline_job(job, runner, db_path, ws_id, PipelineParams(
        source=str(root), skip_classify=True, skip_extract_masks=True, skip_regroup=True,
    ))

    assert job["errors"] == [] and result["ok"] is True
    scan_step = _last_step_update(runner, "scan")
    assert scan_step == {"status": "completed", "summary": "1 photos"}


@pytest.mark.parametrize("method", ["import_in_place", "import_photos"])
def test_import_names_the_photo_and_still_processes(
    import_service, tmp_path, method, no_metadata_warning,  # noqa: F811  (the imported fixture)
):
    """The job fails on the sidecar, but its photos are imported and processed.

    ``ok`` is the job's verdict and fails; ``photos_ok`` says every photo was
    imported, and after-import processing keys on it. Card safety is about
    the originals, so a copy import still says the card is safe to format.
    """
    from wait import wait_for_job_via_runner

    service, db, runner = import_service

    source = tmp_path / "card"
    archive = tmp_path / "archive"
    if method == "import_in_place":
        xmp = _folder_with_corrupt_sidecar(source)
    else:
        # A card carries no sidecars; the archive folder the photo lands in
        # can (left by an earlier import of the same file name).
        _folder_with_corrupt_sidecar(source)
        os.remove(source / "heron.xmp")
        (archive / "trip").mkdir(parents=True)
        xmp = archive / "trip" / "heron.xmp"
        xmp.write_text(CORRUPT_XMP)
    process_id = db.processes.create("After import")
    service.enqueue_process_job.return_value = ("processing", None, None)

    response = getattr(service, method)(db, {
        "sources": [str(source)], "destination": str(archive),
        "folder_template": "trip", "after_import": process_id,
    })
    assert isinstance(response, dict), response
    job = wait_for_job_via_runner(runner, response["job_id"], wait_for_history=True)

    expected = _line(os.path.realpath(xmp))
    assert job["status"] == "failed", job
    result = job["result"]
    assert result["ok"] is False and result["photos_ok"] is True
    assert result["errors"] == [expected]
    assert result["sidecar_failures"] == [expected]
    assert len(result["photo_ids"]) == 2
    assert result.get("process_job_id") == "processing", result.get("after_import_skipped")
    service.enqueue_process_job.assert_called_once()
    steps = {step["id"]: step for step in job["steps"]}
    if method == "import_in_place":
        assert result["failed"] == 0
        assert steps["scan"]["status"] == "failed"
        assert steps["scan"]["summary"] == "2 photos, keywords from 1 sidecar not imported"
    else:
        assert result["failed"] == 0 and result["safe_to_format"] is True
        assert result["chained"] is True
        assert steps["import"]["status"] == "failed"
        assert steps["import"]["summary"] == (
            "2 copied, 0 already present, 0 failed of 2 discovered, "
            "keywords from 1 sidecar not imported"
        )


def test_import_full_names_the_photo(app_and_db, tmp_path, no_metadata_warning):
    from wait import wait_for_job_via_client

    app, _db = app_and_db
    root = tmp_path / "photos"
    xmp = _folder_with_corrupt_sidecar(root)
    client = app.test_client()

    resp = client.post("/api/jobs/import-full", json={"source": str(root), "copy": False})
    assert resp.status_code == 200, resp.get_json()
    job = wait_for_job_via_client(client, resp.get_json()["job_id"])

    assert job["status"] == "failed", job
    assert job["errors"] == [_line(os.path.realpath(xmp))]
    assert job["result"]["photos_indexed"] == 2
    steps = {step["id"]: step for step in job["steps"]}
    assert steps["scan"]["status"] == "failed"
    assert steps["scan"]["summary"] == "2 photos (keywords from 1 sidecar not imported)"
    assert steps["collection"]["status"] == "completed"
