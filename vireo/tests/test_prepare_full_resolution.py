import io
import os
import time
from pathlib import Path

import pytest
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


def test_prepared_render_invalidates_when_primary_changes_before_scan(
    client_with_photo, monkeypatch,
):
    import image_loader

    app, db, photo_id = client_with_photo
    client = app.test_client()
    db.set_photo_edit_recipe(photo_id, {"rotation": 90})
    photo = db.get_photo(photo_id)
    folder_path = db.conn.execute(
        "SELECT path FROM folders WHERE id=?", (photo["folder_id"],),
    ).fetchone()["path"]
    primary_path = os.path.join(folder_path, photo["filename"])
    original_load_image = image_loader.load_image
    load_calls = []

    def tracking_load_image(path, *args, **kwargs):
        load_calls.append(str(path))
        return original_load_image(path, *args, **kwargs)

    monkeypatch.setattr(image_loader, "load_image", tracking_load_image)

    assert client.get(f"/photos/{photo_id}/original").status_code == 200
    assert client.get(f"/photos/{photo_id}/original").status_code == 200
    assert len(load_calls) == 1

    old_stat = os.stat(primary_path)
    Image.new("RGB", (800, 600), "purple").save(primary_path, "JPEG")
    os.utime(
        primary_path,
        ns=(old_stat.st_atime_ns, old_stat.st_mtime_ns + 1_000_000_000),
    )

    assert client.get(f"/photos/{photo_id}/original").status_code == 200
    assert len(load_calls) == 2


def test_preferred_offline_source_rejects_stale_primary(client_with_photo):
    from offline_cache import cache_photo_original, resolve_original_path

    app, db, photo_id = client_with_photo
    photo = db.get_photo(photo_id)
    folder_path = db.conn.execute(
        "SELECT path FROM folders WHERE id=?", (photo["folder_id"],),
    ).fetchone()["path"]
    folders = {photo["folder_id"]: folder_path}
    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    primary_path = os.path.join(folder_path, photo["filename"])

    assert cache_photo_original(
        db, photo, vireo_dir, folders,
    )["status"] == "cached"
    old_stat = os.stat(primary_path)
    Image.new("RGB", (800, 600), "purple").save(primary_path, "JPEG")
    os.utime(
        primary_path,
        ns=(old_stat.st_atime_ns, old_stat.st_mtime_ns + 1_000_000_000),
    )

    source_path, used_cache = resolve_original_path(
        db, photo, vireo_dir, folders, prefer_cached=True,
    )
    assert used_cache is False
    assert source_path == primary_path


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

    assert cache_photo_original(
        db, photo, vireo_dir, folders,
    )["status"] == "cached"
    os.unlink(companion_path)
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


def test_prepared_render_rejected_when_mtime_predates_source(client_with_photo):
    """A stale ``originals/<id>_<sig>.jpg`` must not serve the previous
    owner's pixels after a recycled-rowid purge failed to delete it.

    ``purge_cached_files_for_recycled_id`` backdates any undeletable
    ``originals/<id>_<sig>.jpg`` survivor to mtime 0 when the recycled row
    inherits its previous owner's cached derivative. That is a no-op
    unless ``_prepared_full_resolution_render`` refuses to serve files
    whose mtime is older than the photo's ``file_mtime`` — the existence-
    and-size probe alone can't distinguish a backdated survivor from a
    fresh render, and ``/photos/<id>/original`` would happily send the
    stale JPEG.

    Prime the render endpoint, replace the cached JPEG with recognisable
    sentinel bytes, backdate its mtime, and confirm the next request
    re-renders (returning a valid JPEG whose bytes differ from the
    sentinel).
    """
    app, db, photo_id = client_with_photo
    client = app.test_client()

    # An edit recipe is what routes ``/original`` through the render-and-
    # cache path (``_full_resolution_render_path``). Without one, the
    # endpoint just streams the raw source file — nothing lands in
    # ``originals/`` and there's no signature-keyed cache to invalidate.
    db.set_photo_edit_recipe(photo_id, {"rotation": 90})

    # Prime the cache: the first request writes the prepared render.
    primed = client.get(f"/photos/{photo_id}/original")
    assert primed.status_code == 200
    # Release the WSGI file wrapper's handle on ``cache_path`` before we
    # try to overwrite it below. On Windows, ``send_file``'s underlying
    # handle is held until the response is closed, and ``os.replace`` on
    # the second request would then fail with ``WinError 5``.
    primed.close()

    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    originals_dir = os.path.join(vireo_dir, "originals")
    cached_names = [
        name for name in os.listdir(originals_dir)
        if name.startswith(f"{photo_id}_") and name.endswith(".jpg")
    ]
    assert len(cached_names) == 1, cached_names
    cache_path = os.path.join(originals_dir, cached_names[0])

    # Replace with sentinel bytes and backdate to a mtime before the
    # photo's file_mtime — the shape of a survivor that ``os.utime((0, 0))``
    # from ``purge_cached_files_for_recycled_id`` leaves behind when it
    # couldn't unlink the file.
    sentinel = b"NOT A VALID JPEG, previous owner's leftover render"
    with open(cache_path, "wb") as fh:
        fh.write(sentinel)
    os.utime(cache_path, (0, 0))

    resp = client.get(f"/photos/{photo_id}/original")
    assert resp.status_code == 200
    assert resp.data != sentinel, (
        "``/original`` served the backdated survivor verbatim; on a "
        "recycled rowid that would be the previous owner's pixels"
    )
    # And the endpoint must have written a *new* JPEG (either overwriting
    # the sentinel path or a fresh sibling), whose mtime is newer than
    # the backdated survivor's — the freshness guard's inverse.
    with Image.open(io.BytesIO(resp.data)) as fresh:
        assert fresh.format == "JPEG"


def test_prepared_render_not_regenerated_every_request_for_future_mtime(
    client_with_photo,
):
    """A future-dated source must not cause a re-render on every request.

    ``_prepared_full_resolution_render`` rejects renders whose mtime
    predates ``photos.file_mtime``. Renders are written with the wall
    clock, so a source whose ``file_mtime`` is in the future — clock skew
    on the writing machine, archives that preserve future timestamps —
    fails that gate the instant it's written, and every request pays a
    full-resolution decode plus the edit pipeline again.

    ``serve_thumbnail`` documents and guards this exact trap for the much
    cheaper thumbnail path; the render path needs the same mtime peg.
    """
    app, db, photo_id = client_with_photo
    client = app.test_client()

    db.set_photo_edit_recipe(photo_id, {"rotation": 90})
    # Put the source's mtime well ahead of the wall clock.
    future = time.time() + 86400
    db.conn.execute(
        "UPDATE photos SET file_mtime=? WHERE id=?", (future, photo_id),
    )
    db.conn.commit()

    assert client.get(f"/photos/{photo_id}/original").status_code == 200

    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    originals_dir = os.path.join(vireo_dir, "originals")
    cached = [
        name for name in os.listdir(originals_dir)
        if name.startswith(f"{photo_id}_") and name.endswith(".jpg")
    ]
    assert len(cached) == 1, cached
    cache_path = os.path.join(originals_dir, cached[0])

    # The cached render must satisfy its own freshness gate, or the next
    # request re-renders from scratch.
    assert os.path.getmtime(cache_path) >= future, (
        "the just-written render already fails the freshness gate, so "
        "every request re-renders at full resolution"
    )

    # Prove it: a second request must reuse the cache rather than rewrite.
    before = os.path.getmtime(cache_path)
    assert client.get(f"/photos/{photo_id}/original").status_code == 200
    assert os.path.getmtime(cache_path) == before, (
        "the second request rewrote the prepared render instead of "
        "serving the cached one"
    )


@pytest.mark.parametrize(
    "deletion_stage",
    ["before_turn", "missing_source", "after_copy", "before_render", "during_render"],
)
def test_preparation_skips_concurrently_deleted_photos(
    client_with_photo, monkeypatch, deletion_stage,
):
    """Delete through a separate connection at deterministic job boundaries."""
    import image_loader
    import offline_cache
    from db import Database
    from preview_cache import cleanup_cached_files_for_deleted_photos

    app, db, photo_id = client_with_photo
    photo = db.get_photo(photo_id)
    folder = db.conn.execute(
        "SELECT path FROM folders WHERE id=?", (photo["folder_id"],),
    ).fetchone()["path"]
    # A later photo proves rollback/skip allows the job to keep working.
    other_path = os.path.join(folder, "survivor.jpg")
    Image.new("RGB", (80, 60), "blue").save(other_path)
    other_id = db.add_photo(
        folder_id=photo["folder_id"], filename="survivor.jpg", extension=".jpg",
        file_size=os.path.getsize(other_path), file_mtime=os.path.getmtime(other_path),
        width=80, height=60,
    )
    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    deleted = []

    def delete_selected():
        with_db = Database(app.config["DB_PATH"])
        try:
            result = with_db.delete_photos([photo_id])
            assert result["deleted"] == 1
        finally:
            with_db.close()
        os.unlink(os.path.join(folder, photo["filename"]))
        cleanup_cached_files_for_deleted_photos(
            app.config["THUMB_CACHE_DIR"], result["files"], vireo_dir=vireo_dir,
        )
        deleted.append(photo_id)

    original_cache = offline_cache.cache_photo_original

    def cache_with_deletion(thread_db, selected, *args, **kwargs):
        if selected["id"] == photo_id and deletion_stage == "missing_source":
            delete_selected()
        result = original_cache(thread_db, selected, *args, **kwargs)
        if selected["id"] == photo_id and deletion_stage == "before_render":
            delete_selected()
        return result

    monkeypatch.setattr(offline_cache, "cache_photo_original", cache_with_deletion)
    if deletion_stage == "before_turn":
        original_tree = Database.get_folder_tree

        def folder_tree_with_deletion(self, *args, **kwargs):
            result = original_tree(self, *args, **kwargs)
            if not deleted:
                delete_selected()
            return result

        monkeypatch.setattr(Database, "get_folder_tree", folder_tree_with_deletion)
    elif deletion_stage == "after_copy":
        original_copy = offline_cache._copy_atomic

        def copy_with_deletion(src, dst):
            original_copy(src, dst)
            if not deleted:
                delete_selected()
                # Simulate a copy publishing after deletion's cache cleanup.
                Image.new("RGB", (800, 600), "red").save(dst)

        monkeypatch.setattr(offline_cache, "_copy_atomic", copy_with_deletion)
    elif deletion_stage == "during_render":
        db.set_photo_edit_recipe(photo_id, {"rotation": 90})
        original_load = image_loader.load_image

        def load_with_deletion(*args, **kwargs):
            result = original_load(*args, **kwargs)
            if not deleted:
                delete_selected()
            return result

        monkeypatch.setattr(image_loader, "load_image", load_with_deletion)

    client = app.test_client()
    response = client.post(
        "/api/jobs/prepare-full-resolution", json={"photo_ids": [photo_id, other_id]},
    )
    assert response.status_code == 200
    job = wait_for_job_via_client(client, response.get_json()["job_id"])
    assert deleted == [photo_id]
    assert job["status"] == "completed", job
    result = job["result"]
    assert result["ok"] is True
    assert result["skipped_deleted"] == 1
    assert result["ready"] == 1
    assert result["failed"] == 0
    assert result["total"] == 2
    assert result["errors"] == []
    assert job["progress"]["current"] == 2
    assert db.offline_original_get(photo_id) is None
    assert db.offline_original_get(other_id)["status"] == "cached"
    for subdir in ("offline/originals", "originals"):
        directory = Path(vireo_dir) / subdir
        assert not list(directory.glob(f"{photo_id}.*"))
        assert not list(directory.glob(f"{photo_id}_*"))


@pytest.mark.parametrize("failure", ["missing_source", "copy_error", "render_error", "integrity_error"])
def test_preparation_preserves_errors_for_existing_photos(
    client_with_photo, monkeypatch, failure,
):
    import sqlite3

    import image_loader
    import offline_cache

    app, db, photo_id = client_with_photo
    if failure == "missing_source":
        photo = db.get_photo(photo_id)
        folder = db.conn.execute(
            "SELECT path FROM folders WHERE id=?", (photo["folder_id"],),
        ).fetchone()["path"]
        os.unlink(os.path.join(folder, photo["filename"]))
    elif failure == "render_error":
        db.set_photo_edit_recipe(photo_id, {"rotation": 90})
        monkeypatch.setattr(image_loader, "load_image", lambda *a, **kw: None)
    else:
        def fail_cache(*args, **kwargs):
            if failure == "integrity_error":
                raise sqlite3.IntegrityError("FOREIGN KEY constraint failed")
            raise OSError("copy failed")

        monkeypatch.setattr(offline_cache, "cache_photo_original", fail_cache)

    client = app.test_client()
    response = client.post(
        "/api/jobs/prepare-full-resolution", json={"photo_ids": [photo_id]},
    )
    job = wait_for_job_via_client(client, response.get_json()["job_id"])
    assert job["status"] == "failed", job
    result = job["result"]
    assert result["ok"] is False
    assert result["failed"] == 1
    assert result["skipped_deleted"] == 0
    assert result["ready"] == 0
    assert len(result["errors"]) == 1
    assert db.get_photo(photo_id) is not None
