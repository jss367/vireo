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


def test_prepared_render_invalidates_when_companion_changes(
    client_with_photo, monkeypatch,
):
    import image_loader

    app, db, photo_id = client_with_photo
    client = app.test_client()
    folder_path = db.conn.execute(
        "SELECT f.path FROM photos p JOIN folders f ON f.id=p.folder_id "
        "WHERE p.id=?",
        (photo_id,),
    ).fetchone()["path"]
    raw_path = os.path.join(folder_path, "companion.NEF")
    with open(raw_path, "wb") as raw_file:
        raw_file.write(b"unsupported raw")
    companion_path = os.path.join(folder_path, "companion.jpg")
    Image.new("RGB", (800, 600), "blue").save(companion_path, "JPEG")
    raw_stat = os.stat(raw_path)
    db.conn.execute(
        """UPDATE photos
           SET filename='companion.NEF', extension='.nef',
               companion_path='companion.jpg', width=800, height=600,
               file_size=?, file_mtime=?, working_copy_path=NULL
           WHERE id=?""",
        (raw_stat.st_size, raw_stat.st_mtime, photo_id),
    )
    db.conn.commit()
    db.set_photo_edit_recipe(photo_id, {"rotation": 90})

    original_load_image = image_loader.load_image
    load_calls = []

    def tracking_load_image(path, *args, **kwargs):
        load_calls.append(str(path))
        if str(path).lower().endswith(".nef"):
            return None
        return original_load_image(path, *args, **kwargs)

    monkeypatch.setattr(image_loader, "load_image", tracking_load_image)

    assert client.get(f"/photos/{photo_id}/original").status_code == 200
    assert client.get(f"/photos/{photo_id}/original").status_code == 200
    assert len(load_calls) == 2

    old_stat = os.stat(companion_path)
    Image.new("RGB", (800, 600), "green").save(companion_path, "JPEG")
    os.utime(
        companion_path,
        ns=(old_stat.st_atime_ns, old_stat.st_mtime_ns + 1_000_000_000),
    )

    assert client.get(f"/photos/{photo_id}/original").status_code == 200
    assert len(load_calls) == 3
    assert load_calls[-1] == companion_path


def test_preferred_offline_source_rejects_stale_companion(client_with_photo):
    from offline_cache import cache_photo_original, resolve_original_path

    app, db, photo_id = client_with_photo
    photo = db.get_photo(photo_id)
    folder_path = db.conn.execute(
        "SELECT path FROM folders WHERE id=?", (photo["folder_id"],),
    ).fetchone()["path"]
    folders = {photo["folder_id"]: folder_path}
    companion_path = os.path.join(folder_path, "companion.jpg")
    Image.new("RGB", (800, 600), "blue").save(companion_path, "JPEG")
    db.conn.execute(
        "UPDATE photos SET companion_path=? WHERE id=?",
        ("companion.jpg", photo_id),
    )
    db.conn.commit()
    photo = db.get_photo(photo_id)
    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])

    assert cache_photo_original(
        db, photo, vireo_dir, folders,
    )["status"] == "cached"
    _cached_path, used_cache = resolve_original_path(
        db, photo, vireo_dir, folders, prefer_cached=True,
    )
    assert used_cache is True

    old_stat = os.stat(companion_path)
    Image.new("RGB", (800, 600), "green").save(companion_path, "JPEG")
    os.utime(
        companion_path,
        ns=(old_stat.st_atime_ns, old_stat.st_mtime_ns + 1_000_000_000),
    )

    source_path, used_cache = resolve_original_path(
        db, photo, vireo_dir, folders, prefer_cached=True,
    )
    assert used_cache is False
    assert source_path == os.path.join(folder_path, photo["filename"])


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
