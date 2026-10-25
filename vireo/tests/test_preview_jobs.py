"""Preview warmup failures reach the job API and persisted history."""

import json
import threading
from pathlib import Path

import pytest
from PIL import Image
from preview_materializer import PreviewMaterializationError
from services import photo_ingest
from services import preview_warmup as warmup
from testing.waits import synchronization_timeout
from wait import wait_for_job_via_client


def _source(db, photo_id):
    photo = db.photos.get(photo_id)
    return Path(db.folders.get(photo["folder_id"])["path"]) / photo["filename"]


def _second_photo(db, first):
    source = _source(db, first).with_name("second.jpg")
    Image.new("RGB", (80, 60), "blue").save(source)
    return photo_ingest.add_photo(
        db, db.photos.get(first)["folder_id"], source.name, ".jpg",
        source.stat().st_size, source.stat().st_mtime, width=80, height=60,
    )


@pytest.mark.parametrize("unreadable", ["missing", "corrupt"])
@pytest.mark.parametrize("mixed", [False, True])
def test_failed_preview_job_reports_errors_and_persists_failure(
    client_with_photo, unreadable, mixed,
):
    app, db, first = client_with_photo
    photo_ids = [first, _second_photo(db, first)] if mixed else [first]
    source = _source(db, first)
    if unreadable == "missing":
        source.unlink()
    else:
        source.write_bytes(b"not a decodable JPEG")
    client = app.test_client()

    response = client.post("/api/jobs/previews", json={"photo_ids": photo_ids})
    assert response.status_code == 200
    job_id = response.json["job_id"]
    job = wait_for_job_via_client(client, job_id, wait_for_history=True)

    assert job["status"] == "failed"
    result = job["result"]
    assert result["generated"] == int(mixed)
    assert result["failed"] == 1
    assert result["skipped"] == 0
    assert result["total"] == len(photo_ids)
    assert result["ok"] is False
    assert result["errors_total"] == 1
    assert len(result["errors"]) == 1
    error = result["errors"][0]
    assert error["photo_id"] == first
    assert error["filename"] == "test.jpg"
    assert "preview source" in error["error"]
    assert "test.jpg" in job["errors"][0]
    assert "1 failed" in job["summary"]
    assert any("test.jpg" in detail for detail in job["result_details"])

    persisted = db.job_history.get(job_id)
    assert persisted["status"] == "failed"
    assert persisted["error_count"] == 1
    assert json.loads(persisted["result"])["errors_total"] == 1
    history = client.get("/api/jobs/history").json
    saved = next(row for row in history if row["id"] == job_id)
    assert saved["status"] == "failed"
    assert saved["error_count"] == 1
    if mixed:
        assert db.caches.preview_get(photo_ids[1], 1920) is not None


def test_successful_preview_job_has_no_failure_verdict(client_with_photo):
    app, db, photo_id = client_with_photo
    client = app.test_client()
    job_id = client.post("/api/jobs/previews", json={"photo_ids": [photo_id]}).json["job_id"]
    job = wait_for_job_via_client(client, job_id, wait_for_history=True)
    assert job["status"] == "completed"
    assert job["result"]["ok"] is True
    assert job["result"]["errors"] == []
    assert job["result"]["errors_total"] == 0
    assert db.job_history.get(job_id)["error_count"] == 0


def test_preview_error_sample_preserves_full_history_count(client_with_photo, monkeypatch):
    app, db, first = client_with_photo
    folder_id = db.photos.get(first)["folder_id"]
    photo_ids = [first] + [
        photo_ingest.add_photo(db, folder_id, f"missing-{i}.jpg", ".jpg", 1, 1.0)
        for i in range(54)
    ]

    def unavailable(db, photo, folder_path, **kwargs):
        raise PreviewMaterializationError("source unavailable")

    monkeypatch.setattr(warmup, "materialize_preview", unavailable)
    client = app.test_client()
    job_id = client.post("/api/jobs/previews", json={"photo_ids": photo_ids}).json["job_id"]
    job = wait_for_job_via_client(client, job_id, wait_for_history=True)

    assert job["status"] == "failed"
    assert job["result"]["failed"] == job["result"]["errors_total"] == 55
    assert len(job["result"]["errors"]) == len(job["errors"]) == 50
    assert db.job_history.get(job_id)["error_count"] == 55
    assert "55 failed" in job["summary"]


def test_cancellation_wins_over_preview_failure_and_skips_eviction(client_with_photo, monkeypatch):
    app, db, first = client_with_photo
    second = _second_photo(db, first)
    entered, release = threading.Event(), threading.Event()
    attempted, evictions = [], []

    def unavailable(db, photo, folder_path, **kwargs):
        attempted.append(photo["id"])
        entered.set()
        assert release.wait(synchronization_timeout(5))
        raise PreviewMaterializationError("source unavailable")

    monkeypatch.setattr(warmup, "materialize_preview", unavailable)
    monkeypatch.setattr(warmup, "evict_preview_cache_if_over_quota", lambda *args: evictions.append(True))
    client = app.test_client()
    job_id = client.post("/api/jobs/previews", json={"photo_ids": [first, second]}).json["job_id"]
    try:
        assert entered.wait(synchronization_timeout(5))
        assert client.post(f"/api/jobs/{job_id}/cancel").status_code == 200
        release.set()
        job = wait_for_job_via_client(client, job_id, wait_for_history=True)
        assert job["status"] == "cancelled"
        assert job["result"]["failed"] == 1
        assert attempted == [first]
        assert evictions == []
        assert db.job_history.get(job_id)["status"] == "cancelled"
    finally:
        release.set()
        app._job_runner.cancel_job(job_id)
