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
    prepared_loads = len(load_calls)

    # The next lightbox request must serve the prepared JPEG rather than
    # decoding and applying the edit recipe again.
    rendered = client.get(f"/photos/{photo_id}/original")
    assert rendered.status_code == 200
    assert len(load_calls) == prepared_loads
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
    assert result["copied"] == 1
    assert result["reused"] == 0
    assert result["bytes"] == db.offline_original_get(other_id)["bytes"]
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


@pytest.mark.parametrize(
    "stage", ["before_turn", "after_copy", "during_render", "raw_render", "working_copy_render", "fallback_render"],
)
def test_preparation_rejects_recycled_photo_id(client_with_photo, monkeypatch, stage):
    import image_loader
    import offline_cache
    from db import Database
    from preview_cache import cleanup_cached_files_for_deleted_photos

    app, db, photo_id = client_with_photo
    selected = db.get_photo(photo_id)
    folder = db.conn.execute(
        "SELECT path FROM folders WHERE id=?", (selected["folder_id"],),
    ).fetchone()["path"]
    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    replacement_path = os.path.join(folder, "replacement.jpg")
    replacement_bytes = []
    replacement_cache = []
    replacement_artifacts = {}
    if stage in ("raw_render", "working_copy_render", "fallback_render"):
        suffix = ".tif" if stage == "working_copy_render" else ".nef"
        name = "old" + suffix
        Path(folder, selected["filename"]).rename(Path(folder, name))
        db.conn.execute("UPDATE photos SET filename=?, extension=? WHERE id=?", (name, suffix, photo_id))
        db.conn.commit()
        selected = db.get_photo(photo_id)

    def replace_photo():
        other_db = Database(app.config["DB_PATH"])
        try:
            deleted = other_db.delete_photos([photo_id])
            cleanup_cached_files_for_deleted_photos(app.config["THUMB_CACHE_DIR"], deleted["files"])
            Image.new("RGB", (800, 600), "blue").save(replacement_path)
            replacement_bytes.append(Path(replacement_path).read_bytes())
            new_id = other_db.add_photo(
                folder_id=selected["folder_id"], filename="replacement.jpg", extension=".jpg",
                file_size=selected["file_size"], file_mtime=selected["file_mtime"],
                width=800, height=600,
            )
            assert new_id == photo_id  # Reproduce SQLite recycling the highest ID.
            # Independent producers have already cached the replacement.
            # Old preparation must not purge any of these families.
            for relative in (
                f"thumbnails/{photo_id}.jpg", f"previews/{photo_id}_1920.jpg",
                f"masks/{photo_id}.png", f"external-edits/{photo_id}.jpg",
                f"inat-uploads/{photo_id}.jpg", f"originals/{photo_id}.display.jpg",
                f"originals/{photo_id}_replacement.jpg",
            ):
                path = Path(vireo_dir, relative)
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(b"replacement artifact")
                replacement_artifacts[path] = path.read_bytes()
            if stage == "before_turn":
                # Work not yet started must leave the new owner's cache alone.
                cached = offline_cache.cache_photo_original(
                    other_db, other_db.get_photo(new_id), vireo_dir,
                    {selected["folder_id"]: folder},
                )
                replacement_cache.append(cached["path"])
        finally:
            other_db.close()

    if stage == "before_turn":
        original_tree = Database.get_folder_tree

        def replace_before_turn(self, *args, **kwargs):
            result = original_tree(self, *args, **kwargs)
            if not replacement_bytes:
                replace_photo()
            return result

        monkeypatch.setattr(Database, "get_folder_tree", replace_before_turn)
    elif stage == "after_copy":
        original_copy = offline_cache._copy_atomic

        def publish_after_replacement(src, dst):
            original_copy(src, dst)
            old_bytes = Path(dst).read_bytes()
            replace_photo()
            Path(dst).write_bytes(old_bytes)

        monkeypatch.setattr(offline_cache, "_copy_atomic", publish_after_replacement)
    elif stage == "during_render":
        db.set_photo_edit_recipe(photo_id, {"rotation": 90})
        original_load = image_loader.load_image

        def replace_during_render(*args, **kwargs):
            image = original_load(*args, **kwargs)
            if not replacement_bytes:
                replace_photo()
            return image

        monkeypatch.setattr(image_loader, "load_image", replace_during_render)
    elif stage == "fallback_render":
        monkeypatch.setattr(image_loader, "extract_working_copy", lambda *a, **kw: False)

        def fallback_with_replacement(*args, **kwargs):
            replace_photo()
            return Image.new("RGB", (800, 600), "red")

        monkeypatch.setattr(image_loader, "load_image", fallback_with_replacement)
    else:
        def extract_with_replacement(source, destination, **kwargs):
            Image.new("RGB", (800, 600), "red").save(destination, "JPEG")
            replace_photo()
            return True

        monkeypatch.setattr(image_loader, "extract_working_copy", extract_with_replacement)

    client = app.test_client()
    started = client.post("/api/jobs/prepare-full-resolution", json={"photo_ids": [photo_id]})
    job = wait_for_job_via_client(client, started.get_json()["job_id"])
    assert job["status"] == "completed", job
    assert job["result"]["skipped_deleted"] == 1
    assert job["result"]["ready"] == 0
    assert job["result"]["copied"] == 0
    assert job["result"]["reused"] == 0
    assert job["result"]["bytes"] == 0
    assert job["result"]["errors"] == []
    assert db.get_photo(photo_id)["filename"] == "replacement.jpg"
    assert Path(replacement_path).read_bytes() == replacement_bytes[0]
    for path, expected in replacement_artifacts.items():
        assert path.read_bytes() == expected, path
    assert not list(Path(vireo_dir, "originals").glob("*.tmp"))
    assert not list(Path(vireo_dir, "working").glob("*.tmp"))
    if stage == "working_copy_render":
        assert db.get_photo(photo_id)["working_copy_path"] is None
    if stage == "before_turn":
        assert Path(replacement_cache[0]).read_bytes() == replacement_bytes[0]
        assert db.offline_original_get(photo_id) is not None
    else:
        assert db.offline_original_get(photo_id) is None
    response = client.get(f"/photos/{photo_id}/original")
    assert response.status_code == 200
    assert response.data == replacement_bytes[0]
    response.close()


def test_preparation_does_not_count_deleted_reused_cache(client_with_photo, monkeypatch):
    import offline_cache

    app, db, photo_id = client_with_photo
    client = app.test_client()
    first = client.post("/api/jobs/prepare-full-resolution", json={"photo_ids": [photo_id]})
    assert wait_for_job_via_client(client, first.get_json()["job_id"])["status"] == "completed"
    original_cache = offline_cache.cache_photo_original

    def delete_reused(thread_db, *args, **kwargs):
        result = original_cache(thread_db, *args, **kwargs)
        assert result["status"] == "skipped"
        thread_db.delete_photos([photo_id])
        return result

    monkeypatch.setattr(offline_cache, "cache_photo_original", delete_reused)
    second = client.post("/api/jobs/prepare-full-resolution", json={"photo_ids": [photo_id]})
    job = wait_for_job_via_client(client, second.get_json()["job_id"])
    assert job["status"] == "completed", job
    assert job["result"]["skipped_deleted"] == 1
    assert job["result"]["reused"] == 0
    assert job["result"]["copied"] == 0
    assert job["result"]["bytes"] == 0


@pytest.mark.parametrize("replacement_endpoint", ["prepare-full-resolution", "offline-cache"])
def test_replacement_cache_writer_waits_for_stale_preparation_cleanup(
    client_with_photo, monkeypatch, replacement_endpoint,
):
    import contextlib
    import threading

    import offline_cache
    from preview_cache import cleanup_cached_files_for_deleted_photos

    app, db, photo_id = client_with_photo
    selected = db.get_photo(photo_id)
    folder = db.conn.execute(
        "SELECT path FROM folders WHERE id=?", (selected["folder_id"],),
    ).fetchone()["path"]
    old_copy_waiting = threading.Event()
    release_old_copy = threading.Event()
    replacement_created = threading.Event()
    replacement_attempted = threading.Event()
    replacement_published = threading.Event()
    original_copy = offline_cache._copy_atomic
    original_guard = offline_cache.original_preparation_guard

    def hold_old_copy(src, dst):
        if os.path.basename(src) == selected["filename"]:
            old_bytes = Path(src).read_bytes()
            old_copy_waiting.set()
            assert release_old_copy.wait(10)
            # Publish after the deletion and recycled-ID cleanup have run.
            Path(dst).parent.mkdir(parents=True, exist_ok=True)
            Path(dst).write_bytes(old_bytes)
        else:
            original_copy(src, dst)
            replacement_published.set()

    @contextlib.contextmanager
    def observe_guard(vireo_dir, pid):
        if replacement_created.is_set():
            replacement_attempted.set()
        with original_guard(vireo_dir, pid):
            yield

    monkeypatch.setattr(offline_cache, "_copy_atomic", hold_old_copy)
    monkeypatch.setattr(offline_cache, "original_preparation_guard", observe_guard)
    client = app.test_client()
    first = client.post("/api/jobs/prepare-full-resolution", json={"photo_ids": [photo_id]})
    try:
        assert old_copy_waiting.wait(10)
        deleted = db.delete_photos([photo_id])
        cleanup_cached_files_for_deleted_photos(app.config["THUMB_CACHE_DIR"], deleted["files"])
        replacement = Path(folder) / "replacement.jpg"
        Image.new("RGB", (800, 600), "blue").save(replacement)
        new_id = db.add_photo(
            folder_id=selected["folder_id"], filename=replacement.name, extension=".jpg",
            file_size=replacement.stat().st_size, file_mtime=replacement.stat().st_mtime,
            width=800, height=600,
        )
        assert new_id == photo_id
        replacement_created.set()
        second = client.post(f"/api/jobs/{replacement_endpoint}", json={"photo_ids": [photo_id]})
        assert second.status_code == 200
        assert replacement_attempted.wait(10)
        assert not replacement_published.wait(0.1)
    finally:
        release_old_copy.set()
    stale_job = wait_for_job_via_client(client, first.get_json()["job_id"])
    replacement_job = wait_for_job_via_client(client, second.get_json()["job_id"])
    assert stale_job["status"] == "completed", stale_job
    assert stale_job["result"]["skipped_deleted"] == 1
    assert replacement_job["status"] == "completed", replacement_job
    assert replacement_published.is_set()
    cached = db.offline_original_get(photo_id)
    assert cached["status"] == "cached"
    cached_path = Path(app.config["THUMB_CACHE_DIR"]).parent / cached["original_path"]
    assert cached_path.read_bytes() == replacement.read_bytes()
    assert replacement_job["result"]["bytes"] == replacement.stat().st_size
    if replacement_endpoint == "prepare-full-resolution":
        assert replacement_job["result"]["ready"] == 1
        assert replacement_job["result"]["copied"] == 1


@pytest.mark.parametrize("extension", [".nef", ".tif"])
def test_preparation_publishes_current_source_extraction(client_with_photo, monkeypatch, extension):
    import image_loader

    app, db, photo_id = client_with_photo
    photo = db.get_photo(photo_id)
    folder = db.conn.execute("SELECT path FROM folders WHERE id=?", (photo["folder_id"],)).fetchone()["path"]
    filename = "extract" + extension
    Path(folder, photo["filename"]).rename(Path(folder, filename))
    db.conn.execute("UPDATE photos SET filename=?, extension=? WHERE id=?", (filename, extension, photo_id))
    db.conn.commit()

    def extract(source, destination, **kwargs):
        Image.new("RGB", (800, 600), "blue").save(destination, "JPEG")
        return True

    monkeypatch.setattr(image_loader, "extract_working_copy", extract)
    # The renamed JPEG is not a decodable RAW, and unedited RAW previews
    # decode the source itself; this test is about the /original extraction.
    import preview_materializer

    def stub_preview(*args, **kwargs):
        buf = io.BytesIO()
        Image.new("RGB", (80, 60), "blue").save(buf, "JPEG")
        return buf.getvalue()

    monkeypatch.setattr(preview_materializer, "render_preview_bytes", stub_preview)
    client = app.test_client()
    started = client.post("/api/jobs/prepare-full-resolution", json={"photo_ids": [photo_id]})
    job = wait_for_job_via_client(client, started.get_json()["job_id"])
    assert job["status"] == "completed", job
    assert job["result"]["ready"] == 1
    assert job["result"]["copied"] == 1
    assert job["result"]["skipped_deleted"] == 0
    vireo_dir = Path(app.config["THUMB_CACHE_DIR"]).parent
    if extension == ".nef":
        rendered = vireo_dir / "originals" / f"{photo_id}.display.jpg"
    else:
        rendered = vireo_dir / db.get_photo(photo_id)["working_copy_path"]
    with Image.open(rendered) as image:
        assert image.size == (800, 600)


def test_lightbox_fit_preview_sizes_follow_preview_max_size():
    from preview_cache import lightbox_fit_preview_sizes

    assert lightbox_fit_preview_sizes(None) == [1920, 2560, 3840]
    assert lightbox_fit_preview_sizes(1920) == [1920, 2560, 3840]
    # /full already covers 2560, so the lightbox never asks for that tier.
    assert lightbox_fit_preview_sizes(3000) == [3000, 3840]
    assert lightbox_fit_preview_sizes(4096) == [4096]
    # /full redirects to /original, which preparation already renders.
    assert lightbox_fit_preview_sizes(0) == []
    # _lbPickSourceKey has only 2560 and 3840 explicit tiers — a
    # preview_max_size below 1920 jumps straight from /full to 2560.
    # Including 1920 here would warm a tier the lightbox never asks for.
    assert lightbox_fit_preview_sizes(1280) == [1280, 2560, 3840]
    assert lightbox_fit_preview_sizes(1600) == [1600, 2560, 3840]


def test_preparation_warms_every_lightbox_fit_preview(client_with_photo, monkeypatch):
    import image_loader

    app, db, photo_id = client_with_photo
    client = app.test_client()
    db.set_photo_edit_recipe(photo_id, {"rotation": 90})
    started = client.post(
        "/api/jobs/prepare-full-resolution", json={"photo_ids": [photo_id]},
    )
    job = wait_for_job_via_client(client, started.get_json()["job_id"])
    assert job["status"] == "completed", job
    assert job["result"]["ready"] == 1

    preview_dir = Path(os.path.dirname(app.config["THUMB_CACHE_DIR"]), "previews")
    for size in (1920, 2560, 3840):
        assert (preview_dir / f"{photo_id}_{size}.jpg").is_file(), size
        assert db.preview_cache_get(photo_id, size), size

    # Flipping through the lightbox at fit is now a cache hit on every tier.
    def no_decode(*args, **kwargs):
        raise AssertionError("lightbox fit view decoded after preparation")

    monkeypatch.setattr(image_loader, "load_image", no_decode)
    for url in (
        f"/photos/{photo_id}/full",
        f"/photos/{photo_id}/preview?size=2560",
        f"/photos/{photo_id}/preview?size=3840",
    ):
        response = client.get(url)
        assert response.status_code == 200, url
        with Image.open(io.BytesIO(response.data)) as image:
            assert image.size == (600, 800), url
        response.close()


def test_preparation_skips_previews_when_full_is_original(client_with_photo):
    import config as cfg

    app, db, photo_id = client_with_photo
    client = app.test_client()
    saved = cfg.load()
    saved["preview_max_size"] = 0
    cfg.save(saved)

    started = client.post(
        "/api/jobs/prepare-full-resolution", json={"photo_ids": [photo_id]},
    )
    job = wait_for_job_via_client(client, started.get_json()["job_id"])
    assert job["status"] == "completed", job
    assert job["result"]["ready"] == 1
    preview_dir = Path(os.path.dirname(app.config["THUMB_CACHE_DIR"]), "previews")
    assert not list(preview_dir.glob(f"{photo_id}_*.jpg"))


def test_preview_evicted_during_warming_marks_photo_failed(
    client_with_photo,
):
    """A tiny preview_cache_max_mb lets eviction delete a just-warmed tier.

    _serve_preview still returns 200 off the in-memory bytes, so without
    this check the job would record the photo as ready even though the
    next lightbox request has to decode again — defeating the point of
    Prepare Full Resolution. The job must report the photo as not ready.
    """
    import config as cfg

    app, db, photo_id = client_with_photo
    client = app.test_client()
    saved = cfg.load()
    # Any eviction target below the sum of warmed tiers forces at least
    # one previously-warmed preview to be removed when the next tier is
    # published. 0 is the clearest signal (every row is evicted
    # immediately), and matches a user who turned the preview cache off.
    saved["preview_cache_max_mb"] = 0
    cfg.save(saved)

    started = client.post(
        "/api/jobs/prepare-full-resolution", json={"photo_ids": [photo_id]},
    )
    job = wait_for_job_via_client(client, started.get_json()["job_id"])
    result = job["result"]
    assert result["ok"] is False, result
    assert result["ready"] == 0, result
    assert result["failed"] == 1, result
    assert any("evicted" in e for e in result["errors"]), result["errors"]
    # No tier survived — the lightbox will decode on its next pass.
    preview_dir = Path(os.path.dirname(app.config["THUMB_CACHE_DIR"]), "previews")
    assert not list(preview_dir.glob(f"{photo_id}_*.jpg"))


def test_preview_failure_marks_photo_failed(client_with_photo, monkeypatch):
    import preview_materializer

    app, db, photo_id = client_with_photo
    client = app.test_client()

    def broken_render(*args, **kwargs):
        raise preview_materializer.PreviewMaterializationError("decode failed")

    monkeypatch.setattr(preview_materializer, "render_preview_bytes", broken_render)
    started = client.post(
        "/api/jobs/prepare-full-resolution", json={"photo_ids": [photo_id]},
    )
    job = wait_for_job_via_client(client, started.get_json()["job_id"])
    result = job["result"]
    assert result["ok"] is False
    assert result["ready"] == 0
    assert result["failed"] == 1
    assert len(result["errors"]) == 1


def test_preview_publication_refused_after_photo_id_recycled(client_with_photo, monkeypatch):
    import preview_materializer
    from db import Database
    from preview_cache import cleanup_cached_files_for_deleted_photos

    app, db, photo_id = client_with_photo
    selected = db.get_photo(photo_id)
    folder = db.conn.execute(
        "SELECT path FROM folders WHERE id=?", (selected["folder_id"],),
    ).fetchone()["path"]
    preview_dir = Path(os.path.dirname(app.config["THUMB_CACHE_DIR"]), "previews")
    original_render = preview_materializer.render_preview_bytes
    replaced = []

    def render_then_replace(*args, **kwargs):
        data = original_render(*args, **kwargs)
        if not replaced:
            other_db = Database(app.config["DB_PATH"])
            try:
                deleted = other_db.delete_photos([photo_id])
                cleanup_cached_files_for_deleted_photos(
                    app.config["THUMB_CACHE_DIR"], deleted["files"],
                )
                Image.new("RGB", (800, 600), "blue").save(
                    os.path.join(folder, "replacement.jpg"),
                )
                new_id = other_db.add_photo(
                    folder_id=selected["folder_id"], filename="replacement.jpg",
                    extension=".jpg", file_size=selected["file_size"],
                    file_mtime=selected["file_mtime"], width=800, height=600,
                )
                assert new_id == photo_id
            finally:
                other_db.close()
            replaced.append(True)
        return data

    monkeypatch.setattr(preview_materializer, "render_preview_bytes", render_then_replace)
    client = app.test_client()
    started = client.post(
        "/api/jobs/prepare-full-resolution", json={"photo_ids": [photo_id]},
    )
    job = wait_for_job_via_client(client, started.get_json()["job_id"])
    assert replaced
    assert job["status"] == "completed", job
    assert job["result"]["skipped_deleted"] == 1
    assert job["result"]["ready"] == 0
    # The old photo's pixels never reached the recycled ID's preview cache.
    assert not list(preview_dir.glob(f"{photo_id}_*.jpg"))
    assert db.preview_cache_get(photo_id, 1920) is None


def test_cross_photo_eviction_demotes_earlier_ready_photo(
    client_with_photo, monkeypatch,
):
    """A later photo's warming can evict an earlier photo's already-warmed
    tiers once the combined selection exceeds ``preview_cache_max_mb``.

    The in-loop per-photo check runs before subsequent photos touch the
    cache, so it cannot see this: without the final cross-selection pass,
    the job counts every photo as ready even though the earlier ones will
    decode again at the next lightbox request. Verify readiness is corrected
    to match what the lightbox actually sees.
    """
    from web import media as media_mod

    app, db, pid1 = client_with_photo
    client = app.test_client()

    row = db.conn.execute(
        "SELECT f.id AS folder_id, f.path FROM photos p "
        "JOIN folders f ON f.id=p.folder_id WHERE p.id=?",
        (pid1,),
    ).fetchone()
    folder_id, folder_path = row["folder_id"], row["path"]
    src2 = os.path.join(folder_path, "test2.jpg")
    Image.new("RGB", (800, 600), (40, 180, 90)).save(src2, "JPEG", quality=85)
    pid2 = db.add_photo(
        folder_id=folder_id, filename="test2.jpg", extension=".jpg",
        file_size=os.path.getsize(src2), file_mtime=os.path.getmtime(src2),
        width=800, height=600,
    )

    preview_dir = Path(os.path.dirname(app.config["THUMB_CACHE_DIR"]), "previews")

    # Simulate quota that fits one photo's tiers but not both: after
    # pid2 starts publishing its tiers, eviction wipes pid1's entries.
    real_evict = media_mod.evict_preview_cache_if_over_quota
    purged = []

    def cross_evict(db_arg, dir_arg):
        # Fire once, as soon as pid2 has published something — all of
        # pid1's tiers get evicted in bulk, mimicking an LRU pass over a
        # quota that only holds the current photo's tier set.
        if not purged and db_arg.preview_cache_get(pid2, 2560):
            import contextlib
            for size in (1920, 2560, 3840):
                f = preview_dir / f"{pid1}_{size}.jpg"
                with contextlib.suppress(FileNotFoundError):
                    f.unlink()
                db_arg.conn.execute(
                    "DELETE FROM preview_cache WHERE photo_id=? AND size=?",
                    (pid1, size),
                )
            db_arg.conn.commit()
            purged.append(True)
        return real_evict(db_arg, dir_arg)

    monkeypatch.setattr(
        media_mod, "evict_preview_cache_if_over_quota", cross_evict,
    )

    started = client.post(
        "/api/jobs/prepare-full-resolution",
        json={"photo_ids": [pid1, pid2]},
    )
    job = wait_for_job_via_client(client, started.get_json()["job_id"])
    result = job["result"]
    assert purged, "cross-photo eviction hook never fired"
    # Final cross-selection pass must demote pid1 from ready → failed.
    assert result["failed"] == 1, result
    assert result["ready"] == 1, result
    assert result["ok"] is False, result
    assert any("evicted during warming" in e for e in result["errors"]), result
    assert any("test.jpg" in e for e in result["errors"]), result
    # pid2's tiers survived.
    for size in (1920, 2560, 3840):
        assert db.preview_cache_get(pid2, size), size
        assert (preview_dir / f"{pid2}_{size}.jpg").is_file(), size
    # pid1's tiers were evicted and the final pass caught it.
    for size in (1920, 2560, 3840):
        assert db.preview_cache_get(pid1, size) is None, size
        assert not (preview_dir / f"{pid1}_{size}.jpg").exists(), size


def test_preview_publication_refused_when_recipe_changes_mid_render(
    client_with_photo, monkeypatch,
):
    """A recipe save racing a preparation render must not publish stale pixels.

    ``photo_source_matches`` only compares source-asset fields, so with
    the old guard the worker could publish bytes rendered without the new
    recipe after the edit endpoint had already invalidated the cache. Since
    ordinary preview filenames are not recipe-keyed, the next lightbox
    request would then serve those stale pixels. The guard now also refuses
    publication if the stored recipe changed under it.
    """
    import preview_materializer
    from db import Database

    app, db, photo_id = client_with_photo
    client = app.test_client()
    preview_dir = Path(os.path.dirname(app.config["THUMB_CACHE_DIR"]), "previews")
    original_render = preview_materializer.render_preview_bytes
    recipe_saves = []

    def render_then_save_recipe(*args, **kwargs):
        data = original_render(*args, **kwargs)
        if not recipe_saves:
            # Simulate a concurrent /api/photos/<id>/edit-recipe save on a
            # different connection, as the live edit endpoint would do.
            other = Database(app.config["DB_PATH"])
            try:
                other.set_photo_edit_recipe(photo_id, {"rotation": 90})
            finally:
                other.close()
            recipe_saves.append(True)
        return data

    monkeypatch.setattr(
        preview_materializer, "render_preview_bytes", render_then_save_recipe,
    )

    started = client.post(
        "/api/jobs/prepare-full-resolution", json={"photo_ids": [photo_id]},
    )
    job = wait_for_job_via_client(client, started.get_json()["job_id"])
    assert recipe_saves, "recipe save hook never fired"
    result = job["result"]
    # Publication was refused on the preview whose render raced the save:
    # the photo cannot be ready, and no preview file was published under
    # the recipe-unaware filename.
    assert result["ready"] == 0, result
    assert result["failed"] == 1, result
    assert not list(preview_dir.glob(f"{photo_id}_*.jpg"))
    assert db.preview_cache_get(photo_id, 1920) is None


def test_preparation_guard_accepts_matching_recipe(client_with_photo):
    """Preparation of an edited photo publishes normally when nothing races it.

    The recipe-check guard added for the mid-render race must not false-trip
    on the ordinary edited-photo case: when the recipe stays the same from
    capture through publication, both ``/original`` and ``/preview`` renders
    publish and the photo is reported ready.
    """
    app, db, photo_id = client_with_photo
    db.set_photo_edit_recipe(photo_id, {"rotation": 90})
    client = app.test_client()

    started = client.post(
        "/api/jobs/prepare-full-resolution", json={"photo_ids": [photo_id]},
    )
    job = wait_for_job_via_client(client, started.get_json()["job_id"])
    result = job["result"]
    assert result["ok"] is True, result
    assert result["ready"] == 1, result
    assert result["failed"] == 0, result
    preview_dir = Path(os.path.dirname(app.config["THUMB_CACHE_DIR"]), "previews")
    for size in (1920, 2560, 3840):
        assert db.preview_cache_get(photo_id, size), size
        assert (preview_dir / f"{photo_id}_{size}.jpg").is_file(), size


@pytest.mark.parametrize("cache_fault", [None, "expired", "wrong_state", "publication_failure"])
def test_prepare_raw_jpeg_pair_warms_paired_jpeg_tiers(
    client_with_photo, monkeypatch, cache_fault,
):
    """A RAW+JPEG pair's paired-JPEG preview tiers are a cache hit after prep.

    The lightbox defaults a RAW+JPEG pair to the JPEG and appends
    ``?source=jpeg`` to every render URL; ``_serve_preview`` bypasses the
    ordinary ``(photo_id, size)`` cache for source-specific requests and
    writes a paired shadow cache instead. Without warming the paired tiers
    the first fit-size request still decoded the companion JPEG, so the
    job must report the pair ready only when the paired tiers are on disk.
    """
    import image_loader

    app, db, photo_id = client_with_photo
    client = app.test_client()
    folder_path = db.conn.execute(
        "SELECT f.path FROM photos p JOIN folders f ON f.id=p.folder_id "
        "WHERE p.id=?",
        (photo_id,),
    ).fetchone()["path"]
    # Convert the fixture's photo into a RAW+JPEG pair by renaming the
    # primary to a RAW extension and adding a companion JPEG next to it.
    # A real NEF decoder isn't available in tests; the stub only has to
    # make ``is_raw_jpeg_pair`` see a RAW primary. The ``load_image``
    # patch below mirrors what production does when a RAW can't decode
    # for the ordinary ``/preview`` and ``/original`` paths: fall back
    # to the companion JPEG. The ``?source=jpeg`` paired path never
    # touches the RAW at all.
    raw_path = os.path.join(folder_path, "paired.NEF")
    with open(raw_path, "wb") as raw_file:
        raw_file.write(b"stub raw bytes")
    companion_path = os.path.join(folder_path, "paired.jpg")
    Image.new("RGB", (800, 600), (90, 170, 60)).save(
        companion_path, "JPEG", quality=85,
    )
    raw_stat = os.stat(raw_path)
    db.conn.execute(
        """UPDATE photos
           SET filename='paired.NEF', extension='.nef',
               companion_path='paired.jpg', width=800, height=600,
               file_size=?, file_mtime=?, working_copy_path=NULL
           WHERE id=?""",
        (raw_stat.st_size, raw_stat.st_mtime, photo_id),
    )
    db.conn.commit()
    # Setting a recipe routes /original through ``serve_edited`` whose
    # ``_rescue_failed_edit_decode`` falls back to the companion when
    # the RAW can't decode — the same fallback the ``/preview`` path
    # uses. This keeps the stub NEF harmless so the test can focus on
    # the paired-cache warming the finding is about.
    db.set_photo_edit_recipe(photo_id, {"rotation": 90})

    original_load_image = image_loader.load_image
    paired_jpeg_decodes = []

    def paired_aware_load_image(path, *args, **kwargs):
        if str(path).lower().endswith(".nef"):
            return None  # force the companion-JPEG fallback
        if str(path) == companion_path:
            paired_jpeg_decodes.append(str(path))
        return original_load_image(path, *args, **kwargs)

    monkeypatch.setattr(
        image_loader, "load_image", paired_aware_load_image,
    )

    from types import SimpleNamespace

    from web import job_launchers, media

    if cache_fault == "expired":
        # Advance the renderer's clock at the final cross-selection check,
        # after all three tiers passed their immediate warming checks.
        clock = {"offset": 0}
        monkeypatch.setattr(media, "time", SimpleNamespace(
            time=lambda: time.time() + clock["offset"],
        ))
        original_check = job_launchers._paired_jpeg_preview_exists
        checks = []

        def expire_before_final_check(*args):
            checks.append(args)
            if len(checks) > 3:
                clock["offset"] = media._PAIRED_PREVIEW_TTL_SEC + 1
            return original_check(*args)

        monkeypatch.setattr(job_launchers, "_paired_jpeg_preview_exists",
                            expire_before_final_check)
    elif cache_fault in {"wrong_state", "publication_failure"}:
        original_write = media.atomic_write_bytes

        def fail_current_paired_publication(data, path):
            if "_jpeg_" in str(path) and Path(path).parent.name == "paired":
                if cache_fault == "wrong_state":
                    # A nonempty artifact for a different source/render
                    # state cannot stand in for the failed current write.
                    wrong = Path(str(path).rsplit("_", 1)[0] + "_wrongstate.jpg")
                    wrong.parent.mkdir(parents=True, exist_ok=True)
                    wrong.write_bytes(data)
                raise OSError("simulated paired artifact publication failure")
            return original_write(data, path)

        monkeypatch.setattr(media, "atomic_write_bytes", fail_current_paired_publication)

    started = client.post(
        "/api/jobs/prepare-full-resolution", json={"photo_ids": [photo_id]},
    )
    job = wait_for_job_via_client(client, started.get_json()["job_id"])
    assert job["status"] == ("failed" if cache_fault else "completed"), job
    result = job["result"]
    if cache_fault is not None:
        assert result["ready"] == 0, result
        assert result["failed"] == 1, result
        assert result["ok"] is False, result
        return
    assert result["ok"] is True, result
    assert result["ready"] == 1, result
    assert result["failed"] == 0, result

    preview_dir = Path(os.path.dirname(app.config["THUMB_CACHE_DIR"]), "previews")
    paired_dir = preview_dir / "paired"
    assert paired_dir.is_dir()
    for size in (1920, 2560, 3840):
        matches = list(paired_dir.glob(f"{photo_id}_{size}_jpeg_*.jpg"))
        assert matches, f"no paired JPEG preview for size {size}"
        assert any(m.stat().st_size > 0 for m in matches), size

    decodes_after_prep = len(paired_jpeg_decodes)

    # The lightbox default: /preview?size=N&source=jpeg must now be a
    # cache hit — no further companion decodes after preparation.
    for size in (1920, 2560, 3840):
        url = f"/photos/{photo_id}/preview?size={size}&source=jpeg"
        response = client.get(url)
        assert response.status_code == 200, (url, response.status_code)
        with Image.open(io.BytesIO(response.data)) as image:
            # The paired path renders the companion as-authored, so the
            # response is the companion's own geometry rather than the
            # catalog row's (which, for a RAW+JPEG pair, may differ).
            assert image.size[0] > 0 and image.size[1] > 0, url
        response.close()
    assert len(paired_jpeg_decodes) == decodes_after_prep, (
        "lightbox JPEG fit view decoded again after preparation: "
        f"{paired_jpeg_decodes[decodes_after_prep:]}"
    )


def test_prepare_non_pair_does_not_warm_paired_cache(client_with_photo):
    """A plain JPEG photo must not produce paired shadow cache entries.

    Non-pair photos never request ``?source=jpeg`` from the lightbox,
    so warming a paired tier for them would waste both a decode and a
    cache slot. The pair gate must only fire for RAW primaries with
    JPEG companions.
    """
    app, db, photo_id = client_with_photo
    client = app.test_client()

    started = client.post(
        "/api/jobs/prepare-full-resolution", json={"photo_ids": [photo_id]},
    )
    job = wait_for_job_via_client(client, started.get_json()["job_id"])
    assert job["status"] == "completed", job
    assert job["result"]["ready"] == 1

    preview_dir = Path(os.path.dirname(app.config["THUMB_CACHE_DIR"]), "previews")
    paired_dir = preview_dir / "paired"
    # No paired entries exist (and the directory does not need to).
    assert not paired_dir.exists() or not list(
        paired_dir.glob(f"{photo_id}_*.jpg")
    )
