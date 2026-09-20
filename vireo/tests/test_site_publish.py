import json
import os
import threading

import pytest
from PIL import Image
from wait import wait_for_job_via_client


def _seed_publish_app(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    import config as cfg
    import models
    from app import create_app
    from db import Database

    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    monkeypatch.setattr(
        models, "DEFAULT_MODELS_DIR", str(tmp_path / "vireo-models"),
    )
    monkeypatch.setattr(models, "CONFIG_PATH", str(tmp_path / "models.json"))

    photos_dir = tmp_path / "photos"
    photos_dir.mkdir()
    Image.new("RGB", (1200, 800), (180, 30, 40)).save(photos_dir / "cardinal.jpg")
    Image.new("RGB", (900, 900), (90, 120, 40)).save(photos_dir / "sparrow.jpg")
    Image.new("RGB", (1000, 700), (40, 80, 150)).save(photos_dir / "mystery.jpg")

    vireo_dir = tmp_path / "vireo"
    thumb_dir = vireo_dir / "thumbs"
    thumb_dir.mkdir(parents=True)
    db_path = str(vireo_dir / "vireo.db")

    db = Database(db_path)
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    fid = db.add_folder(str(photos_dir), name="photos")

    p1 = db.add_photo(
        folder_id=fid,
        filename="cardinal.jpg",
        extension=".jpg",
        file_size=1000,
        file_mtime=1.0,
        timestamp="2024-01-15T10:00:00",
        width=1200,
        height=800,
    )
    p2 = db.add_photo(
        folder_id=fid,
        filename="sparrow.jpg",
        extension=".jpg",
        file_size=1000,
        file_mtime=2.0,
        timestamp="2024-02-01T10:00:00",
        width=900,
        height=900,
    )
    p3 = db.add_photo(
        folder_id=fid,
        filename="mystery.jpg",
        extension=".jpg",
        file_size=1000,
        file_mtime=3.0,
        timestamp="2024-03-01T10:00:00",
        width=1000,
        height=700,
    )
    db.conn.execute("UPDATE photos SET quality_score = 0.9 WHERE id = ?", (p1,))
    db.conn.execute("UPDATE photos SET quality_score = 0.7 WHERE id = ?", (p2,))
    db.conn.execute("UPDATE photos SET quality_score = 0.8 WHERE id = ?", (p3,))
    db.conn.execute("UPDATE photos SET mask_path = ? WHERE id = ?", ("masks/cardinal.png", p1))
    db.conn.execute("UPDATE photos SET mask_path = ? WHERE id = ?", ("masks/mystery.png", p3))

    cardinal = db.add_keyword("Northern Cardinal", is_species=True)
    sparrow = db.add_keyword("House Sparrow", is_species=True)
    backyard = db.add_keyword("Backyard", kw_type="location")
    db.tag_photo(p1, cardinal)
    db.tag_photo(p1, backyard)
    db.tag_photo(p2, sparrow)
    db.conn.commit()

    app = create_app(db_path=db_path, thumb_cache_dir=str(thumb_dir))
    return app, db, {"photos_dir": photos_dir, "vireo_dir": vireo_dir, "p1": p1}


def test_publish_site_job_writes_life_list_highlights_and_images(tmp_path, monkeypatch):
    app, db, _meta = _seed_publish_app(tmp_path, monkeypatch)
    client = app.test_client()
    dest = tmp_path / "published"

    resp = client.post("/api/jobs/publish-site", json={
        "destination": str(dest),
        "include_highlights": True,
        "photos_per_species": 2,
        "limit_per_bucket": 2,
        "max_size": 512,
    })
    assert resp.status_code == 200

    job = wait_for_job_via_client(client, resp.get_json()["job_id"])
    assert job["status"] == "completed"
    assert job["result"]["exported_images"] == 3
    assert job["result"]["errors"] == []

    site = json.loads((dest / "data" / "site.json").read_text())
    life = json.loads((dest / "data" / "life-list.json").read_text())
    highlights = json.loads((dest / "data" / "highlights.json").read_text())

    assert site["schema_version"] == 1
    assert site["counts"]["life_list_species"] == 2
    assert life["meta"]["species_count"] == 2
    assert "folders" not in highlights
    cardinal = next(e for e in life["species"] if e["species"] == "Northern Cardinal")
    assert cardinal["locations"] == []
    assert cardinal["best"]["image"].startswith("images/photos/")
    assert (dest / cardinal["best"]["image"]).exists()
    assert [b["species"] for b in highlights["buckets"]] == [
        "Northern Cardinal",
        "House Sparrow",
    ]
    assert "mask_path" not in highlights["buckets"][0]["photos"][0]
    unidentified = highlights["unidentified"]["photos"][0]
    assert "mask_path" not in unidentified
    assert unidentified["image"].startswith("images/photos/unidentified-")
    assert (dest / unidentified["image"]).exists()

    db.close()


def test_publish_site_defaults_to_one_life_list_photo_and_no_highlights(
    tmp_path, monkeypatch,
):
    app, db, _meta = _seed_publish_app(tmp_path, monkeypatch)
    client = app.test_client()
    dest = tmp_path / "published"

    resp = client.post("/api/jobs/publish-site", json={
        "destination": str(dest),
    })
    assert resp.status_code == 200

    job = wait_for_job_via_client(client, resp.get_json()["job_id"])
    assert job["status"] == "completed"
    assert job["config"]["include_life_list"] is True
    assert job["config"]["photos_per_species"] == 1
    assert job["config"]["include_highlights"] is False
    assert job["result"]["exported_images"] == 2

    life = json.loads((dest / "data" / "life-list.json").read_text())
    highlights = json.loads((dest / "data" / "highlights.json").read_text())
    assert life["meta"]["species_count"] == 2
    assert all(len(entry["photos"]) == 1 for entry in life["species"])
    assert highlights["buckets"] == []
    assert highlights["unidentified"]["photos"] == []

    db.close()


def test_publish_site_preflight_reports_exact_unique_photo_count(
    tmp_path, monkeypatch,
):
    app, db, _meta = _seed_publish_app(tmp_path, monkeypatch)
    client = app.test_client()

    default = client.post("/api/jobs/publish-site/preflight", json={})
    assert default.status_code == 200
    assert default.get_json() == {
        "life_list_species": 2,
        "highlight_buckets": 0,
        "unidentified_photos": 0,
        "image_count": 2,
        "data_file_count": 3,
    }

    with_highlights = client.post("/api/jobs/publish-site/preflight", json={
        "include_highlights": True,
        "limit_per_bucket": 3,
    })
    assert with_highlights.status_code == 200
    assert with_highlights.get_json() == {
        "life_list_species": 2,
        "highlight_buckets": 2,
        "unidentified_photos": 1,
        "image_count": 3,
        "data_file_count": 3,
    }

    db.close()


def test_publish_site_can_publish_highlights_without_life_list(
    tmp_path, monkeypatch,
):
    app, db, _meta = _seed_publish_app(tmp_path, monkeypatch)
    client = app.test_client()
    dest = tmp_path / "published"

    resp = client.post("/api/jobs/publish-site", json={
        "destination": str(dest),
        "include_life_list": False,
        "include_highlights": True,
        "limit_per_bucket": 1,
    })
    assert resp.status_code == 200

    job = wait_for_job_via_client(client, resp.get_json()["job_id"])
    assert job["status"] == "completed"
    assert job["result"]["exported_images"] == 3

    life = json.loads((dest / "data" / "life-list.json").read_text())
    highlights = json.loads((dest / "data" / "highlights.json").read_text())
    assert life["species"] == []
    assert [bucket["species"] for bucket in highlights["buckets"]] == [
        "Northern Cardinal",
        "House Sparrow",
    ]
    assert len(highlights["unidentified"]["photos"]) == 1

    db.close()


def test_publish_site_requires_at_least_one_content_section(app_and_db):
    app, _db = app_and_db
    resp = app.test_client().post("/api/jobs/publish-site/preflight", json={
        "include_life_list": False,
        "include_highlights": False,
    })
    assert resp.status_code == 400
    assert resp.get_json()["error"] == "select Life List, Highlights, or both"


def test_publish_site_job_can_include_locations(tmp_path, monkeypatch):
    app, db, _meta = _seed_publish_app(tmp_path, monkeypatch)
    client = app.test_client()
    dest = tmp_path / "published"

    resp = client.post("/api/jobs/publish-site", json={
        "destination": str(dest),
        "include_locations": True,
    })
    assert resp.status_code == 200
    wait_for_job_via_client(client, resp.get_json()["job_id"])

    life = json.loads((dest / "data" / "life-list.json").read_text())
    cardinal = next(e for e in life["species"] if e["species"] == "Northern Cardinal")
    assert cardinal["locations"] == ["Backyard"]

    db.close()


def test_publish_site_uses_developed_render_when_max_exceeds_original(tmp_path, monkeypatch):
    app, db, meta = _seed_publish_app(tmp_path, monkeypatch)
    client = app.test_client()
    dest = tmp_path / "published"
    developed_dir = tmp_path / "developed"

    from export import developed_folder_key
    from site_publish import publish_site

    developed_subdir = developed_dir / developed_folder_key(str(meta["photos_dir"]))
    developed_subdir.mkdir(parents=True)
    Image.new("RGB", (1200, 800), (20, 210, 40)).save(
        developed_subdir / "cardinal.jpg",
        "JPEG",
        quality=95,
    )

    life_list = client.get("/api/life-list").get_json()
    highlights = client.get("/api/highlights?scope=workspace").get_json()
    result = publish_site(
        db=db,
        vireo_dir=str(meta["vireo_dir"]),
        destination=str(dest),
        life_list=life_list,
        highlights=highlights,
        options={
            "developed_dir": str(developed_dir),
            "max_size": 2400,
            "quality": 95,
        },
    )

    assert result["errors"] == []
    life = json.loads((dest / "data" / "life-list.json").read_text())
    cardinal = next(e for e in life["species"] if e["species"] == "Northern Cardinal")
    assert cardinal["locations"] == []
    with Image.open(dest / cardinal["best"]["image"]) as out:
        red, green, _blue = out.getpixel((0, 0))
    assert green > red

    db.close()


def test_publish_site_job_rejects_relative_destination(app_and_db):
    app, _db = app_and_db
    resp = app.test_client().post("/api/jobs/publish-site", json={
        "destination": "relative/out",
    })
    assert resp.status_code == 400


@pytest.mark.parametrize("source", ["original", "default_developed", "custom_developed"])
@pytest.mark.parametrize("cropped", [False, True])
def test_publish_renders_saved_edits_and_developed_images(tmp_path, monkeypatch, source, cropped):
    import config as cfg
    from export import developed_folder_key

    app, db, meta = _seed_publish_app(tmp_path, monkeypatch)
    if cropped:
        db.set_photo_edit_recipe(meta["p1"], {"crop": {"x": 0, "y": 0, "w": 0.5, "h": 1}})
    if source != "original":
        if source == "custom_developed":
            output = tmp_path / "custom-developed"
            cfg.save({"darktable_output_dir": str(output)})
            developed = output / developed_folder_key(str(meta["photos_dir"]))
        else:
            developed = meta["photos_dir"] / "developed"
        developed.mkdir(parents=True)
        Image.new("RGB", (1200, 800), (0, 0, 255)).save(developed / "cardinal.jpg")

    client = app.test_client()
    destination = tmp_path / "published"
    response = client.post("/api/jobs/publish-site", json={
        "destination": str(destination), "max_size": 800,
    })
    job = wait_for_job_via_client(client, response.get_json()["job_id"])
    assert job["status"] == "completed"
    assert job["result"]["errors"] == []
    manifest = json.loads((destination / "data/life-list.json").read_text())
    cardinal = next(s for s in manifest["species"] if s["species"] == "Northern Cardinal")
    with Image.open(destination / cardinal["best"]["image"]) as image:
        assert image.size == ((600, 800) if cropped else (800, 533))
        red, _, blue = image.getpixel((0, 0))
        assert (red > blue) if source == "original" else (blue > red)
    db.close()


@pytest.mark.parametrize("job_type", ["export", "publish-site"])
def test_export_jobs_stop_between_photos_when_cancelled(tmp_path, monkeypatch, job_type):
    import export

    app, db, _meta = _seed_publish_app(tmp_path, monkeypatch)
    started, release = threading.Event(), threading.Event()
    original_load = export.load_image
    loaded = []

    def controlled_load(path, *args, **kwargs):
        loaded.append(path)
        if len(loaded) == 1:
            started.set()
            assert release.wait(10), "cancel request never arrived"
        return original_load(path, *args, **kwargs)

    monkeypatch.setattr(export, "load_image", controlled_load)
    client = app.test_client()
    destination = tmp_path / "cancelled"
    destination.mkdir()
    data = destination / "data"
    data.mkdir()
    (data / "site.json").write_text('{"previous":true}')
    options = {"destination": str(destination), "max_size": 512}
    if job_type == "export":
        options["photo_ids"] = [r["id"] for r in db.conn.execute("SELECT id FROM photos")]
    response = client.post(f"/api/jobs/{job_type}", json=options)
    assert response.status_code == 200
    job_id = response.get_json()["job_id"]
    try:
        assert started.wait(10), "export never started"
        assert client.post(f"/api/jobs/{job_id}/cancel").status_code == 200
    finally:
        release.set()
    job = wait_for_job_via_client(client, job_id)
    assert job["status"] == "cancelled"
    assert len(loaded) == 1
    assert len(list(destination.rglob("*.jpg"))) == (1 if job_type == "export" else 0)
    count_key = "exported" if job_type == "export" else "exported_images"
    assert job["result"][count_key] == 1
    assert (data / "site.json").read_text() == '{"previous":true}'
    db.close()


@pytest.mark.parametrize("boundary", [
    "before_commit", "site.json", "life-list.json", "highlights.json",
])
def test_publish_cancellation_is_coordinated_with_manifest_commit(
    tmp_path, monkeypatch, boundary,
):
    import site_publish
    from jobs import JobRunner

    app, db, _meta = _seed_publish_app(tmp_path, monkeypatch)
    reached, release = threading.Event(), threading.Event()
    original_begin = JobRunner.begin_uncancellable
    original_copy = site_publish.shutil.copyfile

    def wait_for_cancel():
        reached.set()
        assert release.wait(10), "cancel request never arrived"

    def controlled_begin(runner, job_id):
        if boundary == "before_commit":
            wait_for_cancel()
        return original_begin(runner, job_id)

    def controlled_copy(source, destination):
        if getattr(destination, "name", None) == boundary:
            wait_for_cancel()
        return original_copy(source, destination)

    monkeypatch.setattr(JobRunner, "begin_uncancellable", controlled_begin)
    monkeypatch.setattr(site_publish.shutil, "copyfile", controlled_copy)
    destination = tmp_path / "published"
    data = destination / "data"
    data.mkdir(parents=True)
    filenames = ["site.json", "life-list.json", "highlights.json"]
    for filename in filenames:
        (data / filename).write_text('{"previous":true}')

    client = app.test_client()
    response = client.post("/api/jobs/publish-site", json={
        "destination": str(destination), "max_size": 512,
    })
    assert response.status_code == 200
    job_id = response.get_json()["job_id"]
    try:
        assert reached.wait(10), "publish never reached the commit boundary"
        if boundary != "before_commit":
            assert client.post(f"/api/jobs/{job_id}/pause").status_code == 409
        cancelled = client.post(f"/api/jobs/{job_id}/cancel")
        assert cancelled.status_code == (200 if boundary == "before_commit" else 404)
    finally:
        release.set()

    job = wait_for_job_via_client(client, job_id)
    if boundary == "before_commit":
        assert job["status"] == "cancelled"
        assert job["result"]["data_files"] == []
        for filename in filenames:
            assert (data / filename).read_text() == '{"previous":true}'
    else:
        assert job["status"] == "completed"
        assert job["result"]["data_files"] == [f"data/{name}" for name in filenames]
        manifests = [json.loads((data / name).read_text()) for name in filenames]
        assert manifests[0]["generated_at"] == manifests[1]["meta"]["generated_at"]
        assert manifests[0]["generated_at"] == manifests[2]["meta"]["generated_at"]
    db.close()


@pytest.mark.parametrize("action", ["resume", "cancel", "shutdown"])
def test_publish_handoff_honors_pending_pause(tmp_path, monkeypatch, action):
    from jobs import JobRunner
    from wait import wait_for_job

    app, db, _meta = _seed_publish_app(tmp_path, monkeypatch)
    reached, release = threading.Event(), threading.Event()
    original_begin = JobRunner.begin_uncancellable

    def controlled_begin(runner, job_id):
        reached.set()
        assert release.wait(10)
        return original_begin(runner, job_id)

    monkeypatch.setattr(JobRunner, "begin_uncancellable", controlled_begin)
    destination = tmp_path / "published"
    data = destination / "data"
    data.mkdir(parents=True)
    names = ["site.json", "life-list.json", "highlights.json"]
    for name in names:
        (data / name).write_text('{"previous":true}')
    client = app.test_client()
    runner = app._job_runner
    response = client.post("/api/jobs/publish-site", json={
        "destination": str(destination), "max_size": 512,
    })
    assert response.status_code == 200
    job_id = response.get_json()["job_id"]
    try:
        assert reached.wait(10)
        assert client.post(f"/api/jobs/{job_id}/pause").status_code == 200
        release.set()
        wait_for_job(lambda: runner.get(job_id), terminal=("paused",), timeout=5)
        for name in names:
            assert (data / name).read_text() == '{"previous":true}'
        assert list(destination.glob(".vireo-publish-*"))
        if action == "shutdown":
            assert runner.shutdown(timeout=5)
        else:
            assert client.post(f"/api/jobs/{job_id}/{action}").status_code == 200
        job = wait_for_job_via_client(client, job_id)
        assert job["status"] == ("completed" if action == "resume" else "cancelled")
        assert not list(destination.glob(".vireo-publish-*"))
        for name in names:
            assert ((data / name).read_text() == '{"previous":true}') == (action != "resume")
    finally:
        release.set()
        runner.shutdown(timeout=5)
        db.close()


@pytest.mark.parametrize("phase", ["image", "manifest"])
@pytest.mark.parametrize("outcome", ["cancel", "complete", "error"])
def test_republish_stages_files_until_commit(tmp_path, monkeypatch, phase, outcome):
    import site_publish

    app, db, meta = _seed_publish_app(tmp_path, monkeypatch)
    client = app.test_client()
    destination = tmp_path / "published"
    options = {"destination": str(destination), "max_size": 512}
    response = client.post("/api/jobs/publish-site", json=options)
    job = wait_for_job_via_client(client, response.get_json()["job_id"])
    assert job["status"] == "completed"
    previous = {
        path.relative_to(destination): path.read_bytes()
        for path in destination.rglob("*") if path.is_file()
    }
    for path in meta["photos_dir"].glob("*.jpg"):
        Image.new("RGB", (1200, 800), (0, 0, 255)).save(path)

    reached, release = threading.Event(), threading.Event()
    original_export = site_publish._export_image
    original_write = site_publish._write_json

    def pause_once():
        if reached.is_set():
            return
        reached.set()
        assert release.wait(10), "publish was never released"
        if outcome == "error":
            raise OSError("staging write failed")

    def controlled_export(*args, **kwargs):
        result = original_export(*args, **kwargs)
        if phase == "image":
            pause_once()
        return result

    def controlled_write(path, payload):
        result = original_write(path, payload)
        if phase == "manifest":
            pause_once()
        return result

    monkeypatch.setattr(site_publish, "_export_image", controlled_export)
    monkeypatch.setattr(site_publish, "_write_json", controlled_write)
    response = client.post("/api/jobs/publish-site", json=options)
    job_id = response.get_json()["job_id"]
    try:
        assert reached.wait(10), "republish never reached staging"
        for path, contents in previous.items():
            assert (destination / path).read_bytes() == contents
        if outcome == "cancel":
            assert client.post(f"/api/jobs/{job_id}/cancel").status_code == 200
    finally:
        release.set()

    job = wait_for_job_via_client(client, job_id)
    assert job["status"] == {
        "cancel": "cancelled", "complete": "completed", "error": "failed",
    }[outcome]
    assert not list(destination.glob(".vireo-publish-*"))
    current = {
        path.relative_to(destination): path.read_bytes()
        for path in destination.rglob("*") if path.is_file()
    }
    assert current.keys() == previous.keys()
    if outcome == "complete":
        for path, contents in previous.items():
            assert current[path] != contents
        for path in destination.rglob("*.jpg"):
            with Image.open(path) as image:
                red, _, blue = image.getpixel((0, 0))
                assert blue > red
    else:
        assert current == previous
    db.close()


@pytest.mark.skipif(os.name != "posix", reason="POSIX write permissions")
@pytest.mark.parametrize("protection", ["directories", "manifest"])
def test_republish_checks_access_before_commit(tmp_path, monkeypatch, protection):
    app, db, meta = _seed_publish_app(tmp_path, monkeypatch)
    client = app.test_client()
    destination = tmp_path / "published"
    options = {"destination": str(destination), "max_size": 512}
    response = client.post("/api/jobs/publish-site", json=options)
    job = wait_for_job_via_client(client, response.get_json()["job_id"])
    assert job["status"] == "completed"
    previous = {
        path.relative_to(destination): path.read_bytes()
        for path in destination.rglob("*") if path.is_file()
    }
    for path in meta["photos_dir"].glob("*.jpg"):
        Image.new("RGB", (1200, 800), (0, 0, 255)).save(path)

    protected = (
        [destination / "data", destination / "images/photos"]
        if protection == "directories" else [destination / "data/highlights.json"]
    )
    try:
        for path in protected:
            path.chmod(0o555 if path.is_dir() else 0o444)
            if os.access(path, os.W_OK):
                pytest.skip("current user can bypass write protection")
        response = client.post("/api/jobs/publish-site", json=options)
        job = wait_for_job_via_client(client, response.get_json()["job_id"])
        if protection == "directories":
            assert job["status"] == "completed"
            for path, contents in previous.items():
                assert (destination / path).read_bytes() != contents
        else:
            assert job["status"] == "failed"
            for path, contents in previous.items():
                assert (destination / path).read_bytes() == contents
        assert not list(destination.glob(".vireo-publish-*"))
    finally:
        for path in protected:
            path.chmod(0o755 if path.is_dir() else 0o644)
        db.close()


@pytest.mark.parametrize('failure_point', ['backup', 'image', 'manifest'])
@pytest.mark.parametrize('protected_directories', [False, True])
def test_failed_republish_restores_previous_site(
    tmp_path, monkeypatch, failure_point, protected_directories,
):
    import errno
    from pathlib import Path

    import site_publish

    app, db, meta = _seed_publish_app(tmp_path, monkeypatch)
    client = app.test_client()
    destination = tmp_path / 'published'
    options = {'destination': str(destination), 'max_size': 512}
    response = client.post('/api/jobs/publish-site', json=options)
    assert wait_for_job_via_client(client, response.get_json()['job_id'])['status'] == 'completed'
    previous = {p.relative_to(destination): (p.read_bytes(), p.stat())
                for p in destination.rglob('*') if p.is_file()}
    for path in meta['photos_dir'].glob('*.jpg'):
        Image.new('RGB', (1200, 800), (0, 0, 255)).save(path)
    original_copy = site_publish.shutil.copyfile

    def fail_copy(source, target):
        target = Path(target)
        fail = (failure_point == 'backup' and target.parent.name == 'rollback'
                or failure_point == 'image' and target.parent == destination / 'images/photos'
                or failure_point == 'manifest' and target == destination / 'data/life-list.json')
        if fail:
            target.write_bytes(b'partial file')
            raise OSError(errno.ENOSPC, 'Injected disk full')
        return original_copy(source, target)

    monkeypatch.setattr(site_publish.shutil, 'copyfile', fail_copy)
    protected = [destination / 'data', destination / 'images/photos'] if protected_directories else []
    try:
        for path in protected:
            path.chmod(0o555)
            if os.access(path, os.W_OK):
                pytest.skip('current user bypasses directory protection')
        response = client.post('/api/jobs/publish-site', json=options)
        job = wait_for_job_via_client(client, response.get_json()['job_id'])
        assert job['status'] == 'failed'
        for rel, (contents, stat) in previous.items():
            path = destination / rel
            assert path.read_bytes() == contents
            assert path.stat().st_ino == stat.st_ino
            assert path.stat().st_mode == stat.st_mode
        assert not list(destination.glob('.vireo-publish-*'))
    finally:
        for path in protected:
            path.chmod(0o755)
        db.close()


def test_failed_first_publish_removes_partial_new_files(tmp_path, monkeypatch):
    import site_publish

    app, db, _meta = _seed_publish_app(tmp_path, monkeypatch)
    client = app.test_client()
    destination = tmp_path / 'published'
    original_copy = site_publish.shutil.copyfile

    def fail_manifest(source, target):
        if target == destination / 'data/life-list.json':
            target.write_bytes(b'partial')
            raise OSError('Disconnected during publish')
        return original_copy(source, target)

    monkeypatch.setattr(site_publish.shutil, 'copyfile', fail_manifest)
    response = client.post('/api/jobs/publish-site', json={'destination': str(destination), 'max_size': 512})
    assert wait_for_job_via_client(client, response.get_json()['job_id'])['status'] == 'failed'
    assert not [p for p in destination.rglob('*') if p.is_file()]
    db.close()


def test_failed_rollback_retains_recoverable_backups(tmp_path, monkeypatch):
    import site_publish

    app, db, _meta = _seed_publish_app(tmp_path, monkeypatch)
    client = app.test_client()
    destination = tmp_path / 'published'
    options = {'destination': str(destination), 'max_size': 512}
    response = client.post('/api/jobs/publish-site', json=options)
    assert wait_for_job_via_client(client, response.get_json()['job_id'])['status'] == 'completed'
    previous = {p.relative_to(destination).as_posix(): p.read_bytes()
                for p in destination.rglob('*') if p.is_file()}
    original_copy = site_publish.shutil.copyfile

    def fail_manifest(source, target):
        if target == destination / 'data/life-list.json':
            target.write_bytes(b'partial')
            raise OSError('Disconnected during publish')
        return original_copy(source, target)

    def fail_restore(*args, **kwargs):
        raise OSError('Volume remains unavailable')

    monkeypatch.setattr(site_publish.shutil, 'copyfile', fail_manifest)
    monkeypatch.setattr(site_publish.shutil, 'copyfileobj', fail_restore)
    response = client.post('/api/jobs/publish-site', json=options)
    job = wait_for_job_via_client(client, response.get_json()['job_id'])
    assert job['status'] == 'failed'
    backups = list(destination.glob('.vireo-publish-*'))
    assert len(backups) == 1
    recovery = json.loads((backups[0] / 'recovery.json').read_text())
    for rel, contents in previous.items():
        assert (backups[0] / recovery[rel]).read_bytes() == contents
    assert str(backups[0]) in job['error']
    db.close()


@pytest.mark.parametrize('case_alias', [False, True])
def test_publish_waiting_for_same_destination_can_be_cancelled(tmp_path, monkeypatch, case_alias):
    import site_publish

    entered = threading.Event()
    release = threading.Event()
    calls = []
    results = []

    def held_commit(destination, staging, paths, cancel_check, begin_commit):
        calls.append(staging)
        if len(calls) > 1:
            return True
        entered.set()
        assert release.wait(5), 'commit was not released'
        return True

    monkeypatch.setattr(site_publish, '_commit_site_locked', held_commit)
    first = threading.Thread(target=lambda: results.append(
        site_publish._commit_site(tmp_path, tmp_path / 'first', [])))
    first.start()
    try:
        assert entered.wait(5), 'first commit never started'
        destination = tmp_path.with_name(tmp_path.name.upper()) if case_alias else tmp_path
        assert not site_publish._commit_site(destination, tmp_path / 'second', [], lambda: True)
        assert calls == [tmp_path / 'first']
    finally:
        release.set()
        first.join(5)
    assert not first.is_alive()
    assert results == [True]
