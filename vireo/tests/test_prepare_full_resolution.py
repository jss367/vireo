import io
import os

from PIL import Image
from wait import wait_for_job_via_client


def test_prepare_full_resolution_caches_selected_original(client_with_photo):
    app, db, photo_id = client_with_photo
    client = app.test_client()

    response = client.post(
        "/api/jobs/prepare-full-resolution",
        json={"photo_ids": [photo_id]},
    )

    assert response.status_code == 200
    job_id = response.get_json()["job_id"]
    job = wait_for_job_via_client(client, job_id)
    assert job["status"] == "completed", job
    result = job["result"]
    assert result["ok"] is True
    assert result["ready"] == 1
    assert result["copied"] == 1
    assert result["reused"] == 0
    assert result["failed"] == 0
    assert result["total"] == 1
    assert result["bytes"] > 0
    assert result["errors"] == []

    cached = db.offline_original_get(photo_id)
    assert cached is not None
    assert cached["status"] == "cached"
    cached_path = os.path.join(
        os.path.dirname(app.config["THUMB_CACHE_DIR"]),
        cached["original_path"],
    )
    assert os.path.isfile(cached_path)

    repeated = client.post(
        "/api/jobs/prepare-full-resolution",
        json={"photo_ids": [photo_id]},
    )
    repeated_job = wait_for_job_via_client(
        client, repeated.get_json()["job_id"],
    )
    assert repeated_job["status"] == "completed"
    assert repeated_job["result"]["ready"] == 1
    assert repeated_job["result"]["copied"] == 0
    assert repeated_job["result"]["reused"] == 1


def test_prepare_full_resolution_reuses_edited_render(
    client_with_photo, monkeypatch,
):
    import image_loader

    app, db, photo_id = client_with_photo
    client = app.test_client()
    db.set_photo_edit_recipe(photo_id, {"rotation": 90})
    original_load_image = image_loader.load_image
    load_calls = []

    def tracking_load_image(path, *args, **kwargs):
        load_calls.append(str(path))
        return original_load_image(path, *args, **kwargs)

    monkeypatch.setattr(image_loader, "load_image", tracking_load_image)

    started = client.post(
        "/api/jobs/prepare-full-resolution",
        json={"photo_ids": [photo_id]},
    )
    job = wait_for_job_via_client(client, started.get_json()["job_id"])
    assert job["status"] == "completed", job
    assert len(load_calls) == 1

    # The next lightbox request must serve the prepared JPEG rather than
    # decoding and applying the edit recipe again.
    rendered = client.get(f"/photos/{photo_id}/original")
    assert rendered.status_code == 200
    assert len(load_calls) == 1
    with Image.open(io.BytesIO(rendered.data)) as image:
        assert image.size == (600, 800)

    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    originals_dir = os.path.join(vireo_dir, "originals")
    assert any(
        name.startswith(f"{photo_id}_") and name.endswith(".jpg")
        for name in os.listdir(originals_dir)
    )


def test_prepare_full_resolution_validates_photo_ids(client_with_photo):
    app, _db, photo_id = client_with_photo
    client = app.test_client()

    assert client.post(
        "/api/jobs/prepare-full-resolution", json={"photo_ids": []}
    ).status_code == 400
    assert client.post(
        "/api/jobs/prepare-full-resolution", json={"photo_ids": [True]}
    ).status_code == 400
    assert client.post(
        "/api/jobs/prepare-full-resolution", json={"photo_ids": [1.5]}
    ).status_code == 400
    assert client.post(
        "/api/jobs/prepare-full-resolution", json=[photo_id]
    ).status_code == 400


def test_photo_cache_cleanup_removes_prepared_render(tmp_path):
    from preview_cache import cleanup_cached_files_for_deleted_photos

    vireo_dir = tmp_path / "vireo"
    thumb_dir = vireo_dir / "thumbnails"
    originals_dir = vireo_dir / "originals"
    thumb_dir.mkdir(parents=True)
    originals_dir.mkdir()
    rendered = originals_dir / "17_0123456789abcdef.jpg"
    rendered.write_bytes(b"prepared image")

    cleanup_cached_files_for_deleted_photos(
        str(thumb_dir), [{"photo_id": 17, "filename": "bird.jpg"}],
    )

    assert not rendered.exists()
