import contextlib
import json
import os

from wait import wait_for_job_via_client


def _run_missing_originals_check(client, folder_id=None):
    body = {}
    if folder_id is not None:
        body["folder_id"] = folder_id
    resp = client.post("/api/photos/missing/check", json=body)
    assert resp.status_code in (200, 202)
    data = resp.get_json()
    if data.get("pending"):
        wait_for_job_via_client(client, data["job_id"])
    url = "/api/photos/missing"
    if folder_id is not None:
        url += f"?folder_id={folder_id}"
    payload = client.get(url).get_json()
    assert payload["status"] == "ready"
    assert "photos" in payload
    return payload


def test_index_redirects_to_browse(app_and_db, monkeypatch, tmp_path):
    """GET / redirects to /browse when classification is usable (a label-free
    Tree-of-Life model needs no species list). The classification-readiness
    gate is now disk-aware — it checks that the ToL artifacts are actually
    installed — so the mocked model needs a real weights dir with the
    artifact stubs, otherwise the redirect falls through to /welcome."""
    import models
    weights = tmp_path / "bioclip-2"
    weights.mkdir()
    (weights / "tol_embeddings.npy").write_bytes(b"stub")
    (weights / "tol_classes.json").write_bytes(b"[]")
    monkeypatch.setattr(models, "get_active_model", lambda: {
        "id": "bioclip-2", "name": "BioCLIP-2", "downloaded": True,
        "model_str": "hf-hub:imageomics/bioclip-2",
        "weights_path": str(weights),
    })
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get('/')
    assert resp.status_code == 302
    assert '/browse' in resp.headers['Location']


def test_browse_page(app_and_db):
    """GET /browse returns 200 and includes pending-XMP sync UI."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get('/browse')
    assert resp.status_code == 200
    html = resp.get_data(as_text=True)
    assert 'id="syncBanner"' in html
    assert 'function refreshPendingSyncBanner()' in html


def test_browse_discloses_raw_jpeg_pairs_and_offers_source_switch(app_and_db):
    """The paired-file behavior must be discoverable in Browse rather than
    existing only as an internal ``companion_path`` or delete-dialog detail."""
    app, _ = app_and_db
    html = app.test_client().get("/browse").get_data(as_text=True)

    assert "JPEG · RAW pair" in html
    assert 'id="lightboxSourceControl"' in html
    assert "Viewing JPEG · Show RAW" in html
    assert "Has JPEG Companion" in html
    assert "_vireoPairSourceByPhoto[key] = 'jpeg'" in html
    assert "_vireoPairPendingSourceByPhoto" in html
    assert "A failed request leaves the old source intact" in html
    assert "_lbCurrentSrcKey === 'full'" in html
    assert "_lbLoadDetections(photoId)" in html
    assert "_vireoPairSourceImageLoaded(photoId, 'jpeg', pairImg)" in html


def test_api_add_keyword_accepts_existing_keyword_id(app_and_db):
    """POST /api/photos/<id>/keywords can attach an existing keyword by id."""
    app, db = app_and_db
    client = app.test_client()
    photo = db.conn.execute(
        "SELECT id FROM photos WHERE filename = 'bird3.jpg'"
    ).fetchone()
    keyword = db.conn.execute(
        "SELECT id, name FROM keywords WHERE name = 'Cardinal'"
    ).fetchone()

    resp = client.post(
        f"/api/photos/{photo['id']}/keywords",
        json={"keyword_id": keyword["id"]},
    )

    assert resp.status_code == 200
    names = {k["name"] for k in db.get_photo_keywords(photo["id"])}
    assert "Cardinal" in names


def test_help_static_assets_served(app_and_db):
    """The help modal's JS, JSON, and vendored Fuse library must be served.

    The shared navbar includes <script src="/static/help.js"> and
    <script src="/static/vendor/fuse.min.js">, and help.js fetches
    /static/help.json at runtime. If any of these 404, F1 and the
    navbar ? icon silently do nothing.
    """
    app, _ = app_and_db
    client = app.test_client()
    for path in ('/static/help.js', '/static/help.json', '/static/vendor/fuse.min.js'):
        resp = client.get(path)
        assert resp.status_code == 200, f"{path} returned {resp.status_code}"


def test_api_folders(app_and_db):
    """GET /api/folders returns folder tree."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get('/api/folders')
    assert resp.status_code == 200
    data = resp.get_json()
    assert len(data) == 2
    paths = {f['path'] for f in data}
    assert '/photos/2024' in paths


def test_api_photos_extensions_returns_distinct_lowercased(app_and_db):
    """GET /api/photos/extensions returns sorted, lowercased, workspace-scoped extensions."""
    app, db = app_and_db
    # Fixture seeds three .jpg photos. Add a mixed-case + raw to verify
    # collapsing and sorting.
    fid = db.get_folder_tree()[0]['id']
    db.add_photo(folder_id=fid, filename='upper.JPG', extension='.JPG',
                 file_size=1, file_mtime=99.0)
    db.add_photo(folder_id=fid, filename='raw.nef', extension='.nef',
                 file_size=1, file_mtime=99.0)

    client = app.test_client()
    resp = client.get('/api/photos/extensions')
    assert resp.status_code == 200
    data = resp.get_json()
    assert data == ['.jpg', '.nef']


def test_api_photos_extensions_scoped_to_active_workspace(app_and_db):
    """Extensions from another workspace's folders don't leak into the response."""
    app, db = app_and_db
    default_ws = db._active_workspace_id
    other_ws = db.create_workspace("Other")
    db.set_active_workspace(other_ws)
    other_fid = db.add_folder('/other/photos', name='other')
    db.add_photo(folder_id=other_fid, filename='only.cr2', extension='.cr2',
                 file_size=1, file_mtime=1.0)
    db.set_active_workspace(default_ws)

    client = app.test_client()
    resp = client.get('/api/photos/extensions')
    assert resp.status_code == 200
    # .cr2 belongs to "Other" workspace and must not appear here.
    assert '.cr2' not in resp.get_json()
    assert '.jpg' in resp.get_json()


def test_offline_cache_job_copies_original_and_xmp(client_with_photo):
    """Selected photos can be copied into the managed offline cache."""
    app, db, pid = client_with_photo
    client = app.test_client()

    photo = db.get_photo(pid)
    folder = db.conn.execute(
        "SELECT path FROM folders WHERE id=?", (photo["folder_id"],)
    ).fetchone()
    stem, _ = os.path.splitext(photo["filename"])
    xmp_path = os.path.join(folder["path"], f"{stem}.xmp")
    with open(xmp_path, "w", encoding="utf-8") as fh:
        fh.write("<x:xmpmeta></x:xmpmeta>")

    resp = client.post("/api/jobs/offline-cache", json={"photo_ids": [pid]})
    assert resp.status_code == 200
    job = wait_for_job_via_client(client, resp.get_json()["job_id"])
    assert job["status"] == "completed"

    row = db.offline_original_get(pid)
    assert row is not None
    assert row["status"] == "cached"
    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    assert os.path.isfile(os.path.join(vireo_dir, row["original_path"]))
    assert os.path.isfile(os.path.join(vireo_dir, row["xmp_path"]))


def test_offline_cache_picks_up_uppercase_xmp_sidecar(client_with_photo):
    """Offline cache copies sidecars whose extension is `.XMP` (not just `.xmp`)."""
    app, db, pid = client_with_photo
    client = app.test_client()

    photo = db.get_photo(pid)
    folder = db.conn.execute(
        "SELECT path FROM folders WHERE id=?", (photo["folder_id"],)
    ).fetchone()
    stem, _ = os.path.splitext(photo["filename"])
    xmp_path = os.path.join(folder["path"], f"{stem}.XMP")
    with open(xmp_path, "w", encoding="utf-8") as fh:
        fh.write("<x:xmpmeta></x:xmpmeta>")

    resp = client.post("/api/jobs/offline-cache", json={"photo_ids": [pid]})
    assert resp.status_code == 200
    job = wait_for_job_via_client(client, resp.get_json()["job_id"])
    assert job["status"] == "completed"

    row = db.offline_original_get(pid)
    assert row is not None
    assert row["status"] == "cached"
    assert row["xmp_path"], "expected uppercase .XMP sidecar to be cached"
    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    cached_xmp_abs = os.path.join(vireo_dir, row["xmp_path"])
    assert os.path.isfile(cached_xmp_abs)
    # Verify the .XMP source content actually made it into the cache so
    # this can't accidentally pass by caching an empty or wrong file.
    with open(cached_xmp_abs, encoding="utf-8") as fh:
        assert fh.read() == "<x:xmpmeta></x:xmpmeta>"


def test_offline_cache_refreshes_and_removes_xmp_sidecar(client_with_photo):
    """Re-caching refreshes changed sidecars and removes stale cached ones."""
    app, db, pid = client_with_photo
    client = app.test_client()

    photo = db.get_photo(pid)
    folder = db.conn.execute(
        "SELECT path FROM folders WHERE id=?", (photo["folder_id"],)
    ).fetchone()
    companion_name = "test-companion.jpg"
    companion_path = os.path.join(folder["path"], companion_name)
    with open(companion_path, "w", encoding="utf-8") as fh:
        fh.write("companion 1")
    db.conn.execute(
        "UPDATE photos SET companion_path=? WHERE id=?", (companion_name, pid)
    )
    db.conn.commit()

    stem, _ = os.path.splitext(photo["filename"])
    xmp_path = os.path.join(folder["path"], f"{stem}.xmp")
    with open(xmp_path, "w", encoding="utf-8") as fh:
        fh.write("version 1")

    resp = client.post("/api/jobs/offline-cache", json={"photo_ids": [pid]})
    assert resp.status_code == 200
    job = wait_for_job_via_client(client, resp.get_json()["job_id"])
    assert job["status"] == "completed"

    row = db.offline_original_get(pid)
    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    cached_xmp = os.path.join(vireo_dir, row["xmp_path"])
    cached_companion = os.path.join(vireo_dir, row["companion_path"])
    assert open(cached_xmp, encoding="utf-8").read() == "version 1"
    assert open(cached_companion, encoding="utf-8").read() == "companion 1"

    with open(xmp_path, "w", encoding="utf-8") as fh:
        fh.write("version 2")
    with open(companion_path, "w", encoding="utf-8") as fh:
        fh.write("companion 2")
    newer = os.path.getmtime(xmp_path) + 10
    os.utime(xmp_path, (newer, newer))
    os.utime(companion_path, (newer, newer))

    resp = client.post("/api/jobs/offline-cache", json={"photo_ids": [pid]})
    assert resp.status_code == 200
    job = wait_for_job_via_client(client, resp.get_json()["job_id"])
    assert job["status"] == "completed"
    row = db.offline_original_get(pid)
    cached_xmp = os.path.join(vireo_dir, row["xmp_path"])
    cached_companion = os.path.join(vireo_dir, row["companion_path"])
    assert open(cached_xmp, encoding="utf-8").read() == "version 2"
    assert open(cached_companion, encoding="utf-8").read() == "companion 2"

    os.remove(xmp_path)
    os.remove(companion_path)
    resp = client.post("/api/jobs/offline-cache", json={"photo_ids": [pid]})
    assert resp.status_code == 200
    job = wait_for_job_via_client(client, resp.get_json()["job_id"])
    assert job["status"] == "completed"
    row = db.offline_original_get(pid)
    assert row["xmp_path"] is None
    assert row["companion_path"] is None
    assert not os.path.exists(cached_xmp)
    assert not os.path.exists(cached_companion)


def test_original_route_uses_offline_cache_when_source_missing(client_with_photo):
    """Full-res viewing falls back to the cached original when the source is gone."""
    app, db, pid = client_with_photo
    client = app.test_client()

    resp = client.post("/api/jobs/offline-cache", json={"photo_ids": [pid]})
    assert resp.status_code == 200
    job = wait_for_job_via_client(client, resp.get_json()["job_id"])
    assert job["status"] == "completed"

    photo = db.get_photo(pid)
    folder = db.conn.execute(
        "SELECT path FROM folders WHERE id=?", (photo["folder_id"],)
    ).fetchone()
    source = os.path.join(folder["path"], photo["filename"])
    with open(source, "rb") as fh:
        source_bytes = fh.read()
    os.remove(source)
    assert not os.path.isfile(source)

    offline_resp = client.get(f"/photos/{pid}/original")
    assert offline_resp.status_code == 200
    assert offline_resp.data == source_bytes


def test_original_route_uses_offline_cache_despite_recent_raw_failure_marker(
    client_with_photo,
):
    """A RAW failure marker should not block an available offline original."""
    app, db, pid = client_with_photo
    client = app.test_client()

    resp = client.post("/api/jobs/offline-cache", json={"photo_ids": [pid]})
    assert resp.status_code == 200
    job = wait_for_job_via_client(client, resp.get_json()["job_id"])
    assert job["status"] == "completed"

    photo = db.get_photo(pid)
    folder = db.conn.execute(
        "SELECT path FROM folders WHERE id=?", (photo["folder_id"],)
    ).fetchone()
    source = os.path.join(folder["path"], photo["filename"])
    os.remove(source)
    assert not os.path.isfile(source)

    db.conn.execute(
        """UPDATE photos
           SET filename='missing.NEF', extension='.nef',
               working_copy_path=NULL,
               working_copy_failed_at=datetime('now'),
               working_copy_failed_mtime=?
           WHERE id=?""",
        (photo["file_mtime"], pid),
    )
    db.conn.commit()

    offline_resp = client.get(f"/photos/{pid}/original")

    assert offline_resp.status_code == 200
    row = db.offline_original_get(pid)
    assert row is not None and row["bytes"] > 0
    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    with open(os.path.join(vireo_dir, row["original_path"]), "rb") as f:
        assert offline_resp.data == f.read()


def test_offline_cache_rerun_preserves_cache_when_source_missing(client_with_photo):
    """Re-running Make Offline with the source unavailable keeps the cached copy."""
    app, db, pid = client_with_photo
    client = app.test_client()

    resp = client.post("/api/jobs/offline-cache", json={"photo_ids": [pid]})
    assert resp.status_code == 200
    job = wait_for_job_via_client(client, resp.get_json()["job_id"])
    assert job["status"] == "completed"

    row = db.offline_original_get(pid)
    assert row is not None and row["status"] == "cached"
    cached_original_path = row["original_path"]
    cached_bytes = row["bytes"]
    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    assert os.path.isfile(os.path.join(vireo_dir, cached_original_path))

    photo = db.get_photo(pid)
    folder = db.conn.execute(
        "SELECT path FROM folders WHERE id=?", (photo["folder_id"],)
    ).fetchone()
    source = os.path.join(folder["path"], photo["filename"])
    os.remove(source)
    assert not os.path.isfile(source)

    resp = client.post("/api/jobs/offline-cache", json={"photo_ids": [pid]})
    assert resp.status_code == 200
    job = wait_for_job_via_client(client, resp.get_json()["job_id"])
    assert job["status"] == "completed"
    assert job["result"]["skipped"] == 1
    assert job["result"]["failed"] == 0

    row = db.offline_original_get(pid)
    assert row["status"] == "cached"
    assert row["original_path"] == cached_original_path
    assert row["bytes"] == cached_bytes
    assert os.path.isfile(os.path.join(vireo_dir, cached_original_path))

    offline_resp = client.get(f"/photos/{pid}/original")
    assert offline_resp.status_code == 200
    assert len(offline_resp.data) == cached_bytes


def test_offline_cache_rejects_non_object_body(client_with_photo):
    """POST with a top-level non-object JSON body returns 400, not 500."""
    app, _, _ = client_with_photo
    client = app.test_client()
    resp = client.post("/api/jobs/offline-cache", json=[1, 2, 3])
    assert resp.status_code == 400
    assert resp.get_json()["error"] == "request body must be a JSON object"
    resp = client.post("/api/jobs/offline-cache", json="oops")
    assert resp.status_code == 400
    assert resp.get_json()["error"] == "request body must be a JSON object"


def test_offline_cache_rejects_non_integer_photo_ids(client_with_photo):
    """Float and bool photo_ids are rejected instead of silently coerced."""
    app, _, _ = client_with_photo
    client = app.test_client()
    # Floats would otherwise truncate via int() and cache the wrong photo.
    resp = client.post("/api/jobs/offline-cache", json={"photo_ids": [1.9]})
    assert resp.status_code == 400
    assert resp.get_json()["error"] == "photo_ids must be integers"
    resp = client.post("/api/jobs/offline-cache", json={"photo_ids": [2.0]})
    assert resp.status_code == 400
    assert resp.get_json()["error"] == "photo_ids must be integers"
    # bool is an int subclass; reject it too.
    resp = client.post("/api/jobs/offline-cache", json={"photo_ids": [True]})
    assert resp.status_code == 400
    assert resp.get_json()["error"] == "photo_ids must be integers"


def test_offline_cache_files_removed_when_photo_deleted(client_with_photo):
    """Deleting a photo removes its offline cache files from disk."""
    app, db, pid = client_with_photo
    client = app.test_client()

    photo = db.get_photo(pid)
    folder = db.conn.execute(
        "SELECT path FROM folders WHERE id=?", (photo["folder_id"],)
    ).fetchone()
    stem, _ = os.path.splitext(photo["filename"])
    xmp_path = os.path.join(folder["path"], f"{stem}.xmp")
    with open(xmp_path, "w", encoding="utf-8") as fh:
        fh.write("<x:xmpmeta></x:xmpmeta>")

    resp = client.post("/api/jobs/offline-cache", json={"photo_ids": [pid]})
    assert resp.status_code == 200
    job = wait_for_job_via_client(client, resp.get_json()["job_id"])
    assert job["status"] == "completed"

    row = db.offline_original_get(pid)
    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    cached_original = os.path.join(vireo_dir, row["original_path"])
    cached_xmp = os.path.join(vireo_dir, row["xmp_path"])
    assert os.path.isfile(cached_original)
    assert os.path.isfile(cached_xmp)

    resp = client.post("/api/audit/remove-orphans", json={"photo_ids": [pid]})
    assert resp.status_code == 200

    assert db.offline_original_get(pid) is None
    assert not os.path.exists(cached_original)
    assert not os.path.exists(cached_xmp)


def test_api_coverage(app_and_db):
    """GET /api/coverage returns workspace-level and per-folder coverage."""
    app, db = app_and_db
    # Mark one photo as having a thumbnail so at least one stage is non-zero.
    db.conn.execute(
        "UPDATE photos SET thumb_path = '/t/x.jpg' WHERE filename = 'bird1.jpg'"
    )
    db.conn.commit()
    client = app.test_client()
    resp = client.get('/api/coverage')
    assert resp.status_code == 200
    data = resp.get_json()
    assert 'overall' in data
    assert 'folders' in data
    assert data['overall']['total'] == 3
    assert data['overall']['thumbnail'] == 1
    # Per-folder rows carry the same keys
    paths = {f['path']: f for f in data['folders']}
    assert '/photos/2024' in paths
    assert paths['/photos/2024']['total'] == 2
    assert paths['/photos/2024']['thumbnail'] == 1  # bird1.jpg lives here
    assert paths['/photos/2024/January']['total'] == 1


def test_api_folder_get_returns_linked_folder(app_and_db):
    """GET /api/folders/<id> returns id/name/path for a folder in the active ws."""
    app, db = app_and_db
    fid = db.get_folder_tree()[0]['id']
    client = app.test_client()
    resp = client.get(f'/api/folders/{fid}')
    assert resp.status_code == 200
    body = resp.get_json()
    assert body['id'] == fid
    assert body['path'] == '/photos/2024'


def test_api_folder_get_rejects_other_workspace(app_and_db):
    """GET /api/folders/<id> must 404 when folder is not linked to the active
    workspace — otherwise absolute paths leak across workspace boundaries via
    the folder-tree Copy Path action.
    """
    app, db = app_and_db
    default_ws = db._active_workspace_id
    other_ws = db.create_workspace("Other")
    db.set_active_workspace(other_ws)
    # Folder added while other_ws is active; only linked to other_ws.
    other_fid = db.add_folder('/secret/ws', name='secret')
    db.set_active_workspace(default_ws)
    client = app.test_client()
    resp = client.get(f'/api/folders/{other_fid}')
    assert resp.status_code == 404


def test_api_keywords(app_and_db):
    """GET /api/keywords returns keyword tree."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get('/api/keywords')
    assert resp.status_code == 200
    data = resp.get_json()
    names = {k['name'] for k in data}
    assert 'Cardinal' in names
    assert 'Sparrow' in names


def test_logs_recent(app_and_db):
    """GET /api/logs/recent returns recent log entries."""
    app, _ = app_and_db
    client = app.test_client()

    resp = client.get('/api/logs/recent?count=10')
    assert resp.status_code == 200
    data = resp.get_json()
    assert isinstance(data, list)


def test_logs_page(app_and_db):
    """GET /logs returns 200."""
    app, _ = app_and_db
    client = app.test_client()

    resp = client.get('/logs')
    assert resp.status_code == 200


def test_storage_page_has_preview_cache_field(app_and_db):
    """Storage page renders the preview_cache_max_mb input and cache controls."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get('/storage')
    assert resp.status_code == 200
    assert b'cfgPreviewCacheMaxMb' in resp.data
    assert b'clearPreviewCache' in resp.data


def test_storage_page_bounds_large_cache_file_listings(app_and_db):
    """Thumbnail and preview modals request a bounded first page."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get('/storage')
    assert resp.status_code == 200
    assert b'STORAGE_MODAL_FILE_LIMIT = 500' in resp.data
    assert b'&limit=' in resp.data
    assert b'Select all shown' in resp.data
    assert b'Delete Entire Cache' in resp.data


def test_storage_page_has_health_cleanup_and_location_controls(app_and_db):
    """Storage surfaces capacity, safety guidance, refresh, and folder actions."""
    app, _ = app_and_db
    page = app.test_client().get('/storage')
    assert page.status_code == 200
    for marker in (
        b'storageFreeSize', b'storageReclaimableSize', b'clearSafeCaches',
        b'openStorageFolder', b'refreshStoragePage', b'Safe to clear',
        b'Download again', b'cannot currently be reclaimed separately',
        b'loadMasksCard(s && s.masks)',
    ):
        assert marker in page.data
    assert page.data.count(b"{name: 'Database'") == 1


def test_api_storage_includes_offline_originals(app_and_db):
    """Storage totals include Vireo-managed offline originals."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get('/api/storage')
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["offline_originals"]["count"] == 0
    assert data["offline_originals"]["size"] == 0
    assert data["offline_originals"]["path"].endswith("offline")


def test_api_storage_reports_volume_reclaimable_and_masks(
    app_and_db, tmp_path,
):
    app, db = app_and_db
    _seed_masks(db, tmp_path)

    data = app.test_client().get('/api/storage').get_json()
    assert data["masks"]["size"] == 450
    assert data["masks"]["total_bytes"] == 450
    assert len(data["masks"]["variants"]) == 2
    assert data["reclaimable"] == sum(
        data[name]["size"] for name in ("thumbnails", "previews", "embeddings")
    )
    assert data["total"] == sum([
        data["database"]["size"], data["thumbnails"]["size"],
        data["previews"]["size"], data["embeddings"]["size"],
        data["models"]["size"], data["hf_cache"]["size"],
        data["offline_originals"]["size"], data["masks"]["size"],
    ])
    assert data["storage_root"] == str(tmp_path)
    assert data["locations"]
    assert data["volumes"]
    assert all(volume["name"] for volume in data["volumes"])
    assert all(volume["free"] >= 0 for volume in data["volumes"])
    assert all(volume["capacity"] > 0 for volume in data["volumes"])


def test_api_storage_reports_each_backing_volume(
    app_and_db, tmp_path, monkeypatch,
):
    app, _ = app_and_db
    import app as app_module
    import classifier

    embedding_dir = tmp_path / "separate-embedding-volume"
    embedding_dir.mkdir()
    (embedding_dir / "labels.npy").write_bytes(b"embedding")
    monkeypatch.setattr(classifier, "CACHE_DIR", str(embedding_dir))

    real_ismount = app_module.os.path.ismount
    simulated_mounts = {str(tmp_path), str(embedding_dir)}
    monkeypatch.setattr(
        app_module.os.path, "ismount",
        lambda path: path in simulated_mounts or real_ismount(path),
    )
    disk_usage = app_module.shutil._ntuple_diskusage
    monkeypatch.setattr(
        app_module.shutil, "disk_usage",
        lambda path: disk_usage(
            1000, 900 if path == str(embedding_dir) else 600,
            100 if path == str(embedding_dir) else 400,
        ),
    )

    data = app.test_client().get('/api/storage').get_json()
    by_mount = {volume["mount_path"]: volume for volume in data["volumes"]}
    assert by_mount[str(tmp_path)]["free"] == 400
    assert by_mount[str(embedding_dir)]["free"] == 100
    embedding_location = next(
        location for location in data["locations"]
        if location["id"] == "embeddings"
    )
    assert embedding_location["volume"]["mount_path"] == str(embedding_dir)


def test_clear_safe_storage_caches(app_and_db, tmp_path, monkeypatch):
    app, db = app_and_db
    import classifier

    preview_dir = tmp_path / "previews"
    preview_dir.mkdir()
    (preview_dir / "1_1200.jpg").write_bytes(b"preview")
    embedding_dir = tmp_path / "embedding-cache"
    embedding_dir.mkdir()
    (embedding_dir / "labels.npy").write_bytes(b"embedding")
    monkeypatch.setattr(classifier, "CACHE_DIR", str(embedding_dir))

    response = app.test_client().post('/api/storage/clear-safe')
    assert response.status_code == 200
    assert response.get_json()["cleared"] == [
        "thumbnails", "previews", "embeddings",
    ]
    assert not (tmp_path / "thumbs").exists()
    assert not preview_dir.exists()
    assert not embedding_dir.exists()
    assert db.conn.execute("SELECT COUNT(*) FROM preview_cache").fetchone()[0] == 0


def test_open_storage_folder_uses_server_selected_path(
    app_and_db, monkeypatch,
):
    app, _ = app_and_db
    import app as app_module

    calls = []

    class Result:
        returncode = 0
        stderr = ""
        stdout = ""

    monkeypatch.setattr(
        app_module.subprocess, "run",
        lambda command, **kwargs: calls.append(command) or Result(),
    )
    response = app.test_client().post('/api/storage/open-folder')
    assert response.status_code == 200
    assert response.get_json() == {"ok": True}
    assert calls
    assert calls[0][-1] == app.config["THUMB_CACHE_DIR"].rsplit(os.sep, 1)[0]


def test_detection_cache_stats_endpoint(app_and_db):
    """GET /api/detection-cache/stats reports global photo/model counts.

    The cache is shared across workspaces, so the stat must reflect
    every detector_runs row regardless of the active workspace.
    """
    app, db = app_and_db
    client = app.test_client()

    # Zero state: no detector runs recorded yet.
    resp = client.get('/api/detection-cache/stats')
    assert resp.status_code == 200
    data = resp.get_json()
    assert data == {"photo_count": 0, "model_count": 0}

    # Storage page advertises the stat with the right DOM hooks.
    page = client.get('/storage')
    assert page.status_code == 200
    assert b'detectionCacheStats' in page.data
    assert b'photos' in page.data

    # Record runs for two photos across two models and re-check.
    photos = db.conn.execute("SELECT id FROM photos ORDER BY id").fetchall()
    p1, p2 = photos[0]["id"], photos[1]["id"]
    db.record_detector_run(p1, "megadetector-v6", box_count=2)
    db.record_detector_run(p2, "megadetector-v6", box_count=0)
    db.record_detector_run(p1, "megadetector-v5", box_count=1)

    resp = client.get('/api/detection-cache/stats')
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["photo_count"] == 2
    assert data["model_count"] == 2


def test_encounter_species_confirm(app_and_db):
    """POST /api/encounters/species tags photos with species keyword."""
    app, db = app_and_db
    client = app.test_client()

    # Get photo IDs
    photos = db.conn.execute("SELECT id FROM photos").fetchall()
    photo_ids = [p["id"] for p in photos]

    resp = client.post('/api/encounters/species',
                       json={"species": "Blue Jay", "photo_ids": photo_ids})
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["ok"] is True
    assert data["species"] == "Blue Jay"
    assert data["photo_count"] == len(photo_ids)

    # Verify keyword was created with is_species=True
    kw = db.conn.execute(
        "SELECT * FROM keywords WHERE name = 'Blue Jay'").fetchone()
    assert kw is not None
    assert kw["is_species"] == 1

    # Verify all photos are tagged
    for pid in photo_ids:
        tags = db.get_photo_keywords(pid)
        species_tags = [t for t in tags if t["name"] == "Blue Jay"]
        assert len(species_tags) == 1

    # Verify pending changes queued
    pending = db.get_pending_changes()
    kw_adds = [c for c in pending if c["change_type"] == "keyword_add"
               and c["value"] == "Blue Jay"]
    assert len(kw_adds) == len(photo_ids)


def test_encounter_species_confirm_reuses_hierarchical_taxon(app_and_db):
    """Confirming Verdin must not attach a top-level duplicate to a photo
    that already carries the hierarchical Verdin taxon."""
    app, db = app_and_db
    client = app.test_client()
    db.conn.execute(
        "INSERT INTO taxa (id, name, common_name, rank) "
        "VALUES (2912, 'Auriparus flaviceps', 'Verdin', 'species')"
    )
    db.conn.commit()
    photo_ids = [
        row["id"] for row in db.conn.execute(
            "SELECT id FROM photos ORDER BY id LIMIT 2"
        ).fetchall()
    ]
    nested_photo, untagged_photo = photo_ids
    birds = db.add_keyword("1Birds")
    family = db.add_keyword("Penduline tits", parent_id=birds)
    nested = db.add_keyword("Verdin", parent_id=family)
    db.tag_photo(nested_photo, nested)

    resp = client.post(
        "/api/encounters/species",
        json={"species": "Verdin", "photo_ids": photo_ids},
    )
    assert resp.status_code == 200
    root = db.conn.execute(
        "SELECT id FROM keywords WHERE name = 'Verdin' AND parent_id IS NULL"
    ).fetchone()["id"]

    nested_ids = {row["id"] for row in db.get_photo_keywords(nested_photo)}
    untagged_ids = {row["id"] for row in db.get_photo_keywords(untagged_photo)}
    assert nested in nested_ids
    assert root not in nested_ids
    assert root in untagged_ids
    species_by_photo = db.get_species_keywords_for_photos(photo_ids)
    assert species_by_photo[nested_photo].count("Verdin") == 1
    assert species_by_photo[untagged_photo].count("Verdin") == 1
    additions = [
        row for row in db.get_pending_changes()
        if row["change_type"] == "keyword_add" and row["value"] == "Verdin"
    ]
    assert [row["photo_id"] for row in additions] == [untagged_photo]


def test_encounter_species_confirm_ignores_corrupt_pipeline_cache(app_and_db):
    """A bad pipeline cache must not turn species confirmation into a 500."""
    app, db = app_and_db
    client = app.test_client()

    photo_id = db.conn.execute("SELECT id FROM photos ORDER BY id LIMIT 1").fetchone()["id"]
    cache_dir = os.path.dirname(db._db_path)
    cache_path = os.path.join(cache_dir, f"pipeline_results_ws{db._active_workspace_id}.json")
    with open(cache_path, "w") as f:
        f.write('{"photos": [], "encounters": [{"photo_ids"')

    resp = client.post(
        "/api/encounters/species",
        json={"species": "Blue Jay", "photo_ids": [photo_id]},
    )

    assert resp.status_code == 200
    assert any(t["name"] == "Blue Jay" for t in db.get_photo_keywords(photo_id))
    assert not os.path.exists(cache_path)
    assert len([
        name for name in os.listdir(cache_dir)
        if name.startswith(f"pipeline_results_ws{db._active_workspace_id}.json.corrupt-")
    ]) == 1


def test_encounter_species_validation(app_and_db):
    """POST /api/encounters/species validates required fields."""
    app, _ = app_and_db
    client = app.test_client()

    resp = client.post('/api/encounters/species', json={"species": ""})
    assert resp.status_code == 400

    resp = client.post('/api/encounters/species',
                       json={"species": "Robin", "photo_ids": []})
    assert resp.status_code == 400


def test_encounter_species_rejects_invalid_photo_ids(app_and_db):
    """POST /api/encounters/species rejects stale/invalid photo_ids without partial writes."""
    app, db = app_and_db
    client = app.test_client()

    photos = db.conn.execute("SELECT id FROM photos").fetchall()
    valid_id = photos[0]["id"]
    bogus_id = 99999

    resp = client.post('/api/encounters/species',
                       json={"species": "Robin", "photo_ids": [valid_id, bogus_id]})
    assert resp.status_code == 400
    assert "99999" in resp.get_json()["error"]

    # Verify nothing was written for the valid ID either
    tags = db.get_photo_keywords(valid_id)
    assert not any(t["name"] == "Robin" for t in tags)
    pending = db.get_pending_changes()
    assert not any(c["value"] == "Robin" for c in pending)


def test_encounter_species_tags_sub_threshold_photos_with_warning(app_and_db):
    """Explicit species confirmation must tag every submitted photo.

    A weak detector row is useful warning context, but it must not silently
    override a user-confirmed species label. Regression for DSC_3682.NEF:
    a correct Allen's hummingbird prediction was skipped because its only
    detector row was below the threshold.
    """
    app, db = app_and_db
    client = app.test_client()

    photos = db.conn.execute("SELECT id FROM photos ORDER BY id").fetchall()
    p_real, p_noise, p_undetected = photos[0]["id"], photos[1]["id"], photos[2]["id"]

    db.save_detections(
        p_real,
        [{"box": {"x": 0.3, "y": 0.3, "w": 0.4, "h": 0.4},
          "confidence": 0.9, "category": "animal"}],
        detector_model="megadetector-v6",
    )
    db.save_detections(
        p_noise,
        [{"box": {"x": 0.01, "y": 0.01, "w": 0.98, "h": 0.98},
          "confidence": 0.02, "category": "animal"}],
        detector_model="megadetector-v6",
    )
    # p_undetected has no detection rows at all.

    resp = client.post(
        "/api/encounters/species",
        json={"species": "Mountain Chickadee",
              "photo_ids": [p_real, p_noise, p_undetected]},
    )
    assert resp.status_code == 200
    data = resp.get_json()

    real_tags = {k["name"] for k in db.get_photo_keywords(p_real)}
    noise_tags = {k["name"] for k in db.get_photo_keywords(p_noise)}
    undetected_tags = {k["name"] for k in db.get_photo_keywords(p_undetected)}

    assert "Mountain Chickadee" in real_tags, (
        "photo with high-confidence detection must still be tagged"
    )
    assert "Mountain Chickadee" in noise_tags, (
        "explicit user confirmation must tag the submitted photo even when "
        "the detector confidence is below threshold"
    )
    assert "Mountain Chickadee" in undetected_tags, (
        "photo with no detections at all is a manual-tagging case (user "
        "asserting species on a photo never run through the pipeline) — "
        "this must continue to work"
    )

    assert data["photo_count"] == 3, (
        f"photo_count should reflect all explicitly-tagged photos, got {data['photo_count']}"
    )
    assert data.get("skipped_photo_ids") == []
    low_conf = data.get("low_confidence_photo_ids") or []
    assert p_noise in low_conf, (
        f"low_confidence_photo_ids should list the weak detector photo so "
        f"the UI can warn; got {low_conf!r}"
    )


def test_encounter_species_all_sub_threshold_still_confirms_cache(app_and_db):
    """Even all-low-confidence submissions are explicit confirmations."""
    import json as _json
    app, db = app_and_db
    client = app.test_client()

    photos = db.conn.execute("SELECT id FROM photos ORDER BY id").fetchall()
    p_noise = photos[0]["id"]

    db.save_detections(
        p_noise,
        [{"box": {"x": 0.01, "y": 0.01, "w": 0.98, "h": 0.98},
          "confidence": 0.02, "category": "animal"}],
        detector_model="megadetector-v6",
    )

    cache_dir = os.path.dirname(app.config["DB_PATH"])
    ws_id = db._active_workspace_id
    sentinel_summary = {"total_photos": 999}
    sentinel_encounters = [
        {
            "species": ["Sparrow", 0.8],
            "confirmed_species": None,
            "species_confirmed": False,
            "photo_count": 1,
            "photo_ids": [p_noise],
            "bursts": [],
            "species_predictions": [],
        },
    ]
    os.makedirs(cache_dir, exist_ok=True)
    with open(os.path.join(cache_dir, f"pipeline_results_ws{ws_id}.json"), "w") as fh:
        _json.dump(
            {"encounters": sentinel_encounters, "summary": sentinel_summary},
            fh,
        )

    resp = client.post(
        "/api/encounters/species",
        json={"species": "Mountain Chickadee", "photo_ids": [p_noise]},
    )
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["photo_count"] == 1
    assert data["skipped_photo_ids"] == []
    assert data["low_confidence_photo_ids"] == [p_noise]
    assert "Mountain Chickadee" in {k["name"] for k in db.get_photo_keywords(p_noise)}

    assert "encounters" in data, (
        "response should include server-updated encounters when a cache exists"
    )
    assert data["encounters"][0]["species_confirmed"] is True
    assert data["encounters"][0]["confirmed_species"] == "Mountain Chickadee"
    assert data["summary"]["confirmed_count"] == 1
    assert data["summary"]["unconfirmed_count"] == 0


def test_encounter_species_updates_pipeline_cache(app_and_db):
    """POST /api/encounters/species updates species_confirmed in pipeline cache."""
    import json as _json
    app, db = app_and_db
    client = app.test_client()

    photos = db.conn.execute("SELECT id FROM photos").fetchall()
    photo_ids = [p["id"] for p in photos]

    # Create pipeline cache with unconfirmed encounter
    cache_dir = os.path.dirname(app.config["DB_PATH"])
    ws_id = db._active_workspace_id
    results = {
        "encounters": [
            {
                "species": ["Sparrow", 0.8],
                "confirmed_species": None,
                "species_predictions": [],
                "species_confirmed": False,
                "photo_count": len(photo_ids),
                "burst_count": 0,
                "time_range": [None, None],
                "photo_ids": photo_ids,
            }
        ],
        "photos": [{"id": pid, "label": "KEEP", "filename": f"{pid}.jpg"} for pid in photo_ids],
        "summary": {"total_photos": len(photo_ids), "encounter_count": 1, "burst_count": 0,
                     "keep_count": len(photo_ids), "review_count": 0, "reject_count": 0, "rarity_protected": 0},
    }
    path = os.path.join(cache_dir, f"pipeline_results_ws{ws_id}.json")
    with open(path, "w") as f:
        _json.dump(results, f)

    # Confirm species
    resp = client.post("/api/encounters/species",
                       json={"species": "Blue Jay", "photo_ids": photo_ids})
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["summary"]["confirmed_count"] == 1
    assert body["summary"]["unconfirmed_count"] == 0

    # Read cache back and check
    with open(path) as f:
        updated = _json.load(f)
    enc = updated["encounters"][0]
    assert enc["species_confirmed"] is True
    assert enc["confirmed_species"] == "Blue Jay"
    assert updated["summary"]["confirmed_count"] == 1
    assert updated["summary"]["unconfirmed_count"] == 0


def _seed_encounter_cache(app, db, photo_ids, *, confirmed_species=None, bursts=None):
    """Write a pipeline cache with one encounter for the given photos."""
    import json as _json
    cache_dir = os.path.dirname(app.config["DB_PATH"])
    ws_id = db._active_workspace_id
    enc = {
        "species": ["Sparrow", 0.8],
        "confirmed_species": confirmed_species,
        "species_predictions": [],
        "species_confirmed": confirmed_species is not None,
        "photo_count": len(photo_ids),
        "burst_count": len(bursts) if bursts else 0,
        "time_range": [None, None],
        "photo_ids": photo_ids,
    }
    if bursts is not None:
        enc["bursts"] = bursts
    results = {
        "encounters": [enc],
        "photos": [{"id": pid, "label": "KEEP", "filename": f"{pid}.jpg"} for pid in photo_ids],
        "summary": {"total_photos": len(photo_ids), "encounter_count": 1,
                    "burst_count": enc["burst_count"],
                    "keep_count": len(photo_ids), "review_count": 0,
                    "reject_count": 0, "rarity_protected": 0},
    }
    path = os.path.join(cache_dir, f"pipeline_results_ws{ws_id}.json")
    with open(path, "w") as f:
        _json.dump(results, f)
    return path


def test_encounter_species_change_cancels_pending_add(app_and_db):
    """Changing the confirmed species before sync cancels the stale add."""
    app, db = app_and_db
    client = app.test_client()
    # The conftest fixture pre-tags one photo with "Sparrow" via a direct
    # db.tag_photo (no pending add). Restrict this test to photos that do NOT
    # already carry Sparrow so the first confirm genuinely queues a pending
    # add for every photo — that pending add is what the replacement should
    # cancel (rather than queueing a keyword_remove for a never-synced tag).
    sparrow_pre = db.conn.execute(
        """SELECT pk.photo_id FROM photo_keywords pk
           JOIN keywords k ON k.id = pk.keyword_id
           WHERE k.name = 'Sparrow'"""
    ).fetchall()
    pre_tagged = {r["photo_id"] for r in sparrow_pre}
    photo_ids = [
        p["id"] for p in db.conn.execute("SELECT id FROM photos").fetchall()
        if p["id"] not in pre_tagged
    ]
    assert photo_ids, "expected at least one photo not pre-tagged with Sparrow"

    _seed_encounter_cache(app, db, photo_ids)

    # First confirm as Sparrow
    resp = client.post("/api/encounters/species",
                       json={"species": "Sparrow", "photo_ids": photo_ids})
    assert resp.status_code == 200

    # Still pending (not synced yet) — change to Blue Jay
    resp = client.post("/api/encounters/species",
                       json={"species": "Blue Jay", "photo_ids": photo_ids})
    assert resp.status_code == 200
    assert resp.get_json()["previous_species"] == "Sparrow"

    # Photos should now have Blue Jay but not Sparrow
    for pid in photo_ids:
        names = {k["name"] for k in db.get_photo_keywords(pid)}
        assert "Blue Jay" in names
        assert "Sparrow" not in names

    # Pending changes should contain keyword_add:Blue Jay only.
    # The Sparrow add had not synced, so it should be cancelled, not followed
    # by a keyword_remove (otherwise the sidecar would see a remove for a
    # keyword that was never written).
    changes = [dict(c) for c in db.get_pending_changes()]
    values_by_type = {(c["change_type"], c["value"]) for c in changes}
    assert ("keyword_add", "Blue Jay") in values_by_type
    assert ("keyword_add", "Sparrow") not in values_by_type
    assert ("keyword_remove", "Sparrow") not in values_by_type


def test_encounter_species_change_queues_remove_after_sync(app_and_db):
    """If the previous species was already synced, changing it queues a remove."""
    app, db = app_and_db
    client = app.test_client()
    photo_ids = [p["id"] for p in db.conn.execute("SELECT id FROM photos").fetchall()]

    _seed_encounter_cache(app, db, photo_ids)

    # Confirm as Sparrow, then simulate a completed sync by clearing pending.
    resp = client.post("/api/encounters/species",
                       json={"species": "Sparrow", "photo_ids": photo_ids})
    assert resp.status_code == 200
    db.conn.execute("DELETE FROM pending_changes")
    db.conn.commit()

    # Change to Blue Jay
    resp = client.post("/api/encounters/species",
                       json={"species": "Blue Jay", "photo_ids": photo_ids})
    assert resp.status_code == 200

    # Photos have Blue Jay, not Sparrow
    for pid in photo_ids:
        names = {k["name"] for k in db.get_photo_keywords(pid)}
        assert "Blue Jay" in names
        assert "Sparrow" not in names

    # Now a keyword_remove:Sparrow must be queued so the XMP drops the
    # already-written Sparrow tag.
    values_by_type = {(c["change_type"], c["value"]) for c in db.get_pending_changes()}
    assert ("keyword_remove", "Sparrow") in values_by_type
    assert ("keyword_add", "Blue Jay") in values_by_type


def test_burst_override_change_untags_previous(app_and_db):
    """Changing a burst override removes the previously overridden species."""
    app, db = app_and_db
    client = app.test_client()
    photo_ids = [p["id"] for p in db.conn.execute("SELECT id FROM photos").fetchall()]
    burst_ids = photo_ids[:1]

    bursts = [{
        "photo_ids": burst_ids,
        "species_predictions": [],
        "species_override": None,
    }]
    _seed_encounter_cache(app, db, photo_ids, bursts=bursts)

    # Override burst 0 to Sparrow
    resp = client.post("/api/encounters/species",
                       json={"species": "Sparrow", "photo_ids": burst_ids,
                             "burst_index": 0})
    assert resp.status_code == 200
    db.conn.execute("DELETE FROM pending_changes")
    db.conn.commit()

    # Now change the burst override to Junco
    resp = client.post("/api/encounters/species",
                       json={"species": "Junco", "photo_ids": burst_ids,
                             "burst_index": 0})
    assert resp.status_code == 200
    assert resp.get_json()["previous_species"] == "Sparrow"

    for pid in burst_ids:
        names = {k["name"] for k in db.get_photo_keywords(pid)}
        assert "Junco" in names
        assert "Sparrow" not in names

    values = {(c["change_type"], c["value"]) for c in db.get_pending_changes()}
    assert ("keyword_remove", "Sparrow") in values
    assert ("keyword_add", "Junco") in values


def test_encounter_species_confirm_same_species_noop_on_keywords(app_and_db):
    """Re-confirming the same species doesn't queue a remove."""
    app, db = app_and_db
    client = app.test_client()
    photo_ids = [p["id"] for p in db.conn.execute("SELECT id FROM photos").fetchall()]

    _seed_encounter_cache(app, db, photo_ids)
    client.post("/api/encounters/species",
                json={"species": "Sparrow", "photo_ids": photo_ids})
    db.conn.execute("DELETE FROM pending_changes")
    db.conn.commit()

    resp = client.post("/api/encounters/species",
                       json={"species": "Sparrow", "photo_ids": photo_ids})
    assert resp.status_code == 200
    assert resp.get_json()["previous_species"] is None

    values = {c["change_type"] for c in db.get_pending_changes()}
    assert "keyword_remove" not in values


def test_encounter_species_replacement_is_atomic_in_history(app_and_db):
    """Replacing the encounter species records one history entry, not two."""
    app, db = app_and_db
    client = app.test_client()
    photo_ids = [p["id"] for p in db.conn.execute("SELECT id FROM photos").fetchall()]

    _seed_encounter_cache(app, db, photo_ids)
    client.post("/api/encounters/species",
                json={"species": "Sparrow", "photo_ids": photo_ids})
    # Clear history from the initial confirm so we're only looking at the
    # replacement.
    db.conn.execute("DELETE FROM edit_history")
    db.conn.commit()

    resp = client.post("/api/encounters/species",
                       json={"species": "Blue Jay", "photo_ids": photo_ids})
    assert resp.status_code == 200

    history = db.get_edit_history()
    assert len(history) == 1
    assert history[0]['action_type'] == 'species_replace'
    assert 'Sparrow' in history[0]['description']
    assert 'Blue Jay' in history[0]['description']


def test_encounter_species_replacement_undo_restores_previous(app_and_db):
    """One undo after a replacement restores the previous species, not neither."""
    app, db = app_and_db
    client = app.test_client()
    photo_ids = [p["id"] for p in db.conn.execute("SELECT id FROM photos").fetchall()]

    _seed_encounter_cache(app, db, photo_ids)
    client.post("/api/encounters/species",
                json={"species": "Sparrow", "photo_ids": photo_ids})
    client.post("/api/encounters/species",
                json={"species": "Blue Jay", "photo_ids": photo_ids})

    # One undo should swap the photos back to Sparrow.
    undone = db.undo_last_edit()
    assert undone is not None
    assert undone['action_type'] == 'species_replace'

    for pid in photo_ids:
        names = {k["name"] for k in db.get_photo_keywords(pid)}
        assert "Sparrow" in names
        assert "Blue Jay" not in names

    # Neither species was synced, so undo should cancel the pending swap
    # outright rather than queue a keyword_remove for a never-written tag.
    values = {(c["change_type"], c["value"]) for c in db.get_pending_changes()}
    assert ("keyword_remove", "Blue Jay") not in values
    assert ("keyword_remove", "Sparrow") not in values
    # Original keyword_add:Sparrow is back in the queue because the replace
    # had cancelled it — and undoing the replace's own add (Blue Jay) means
    # the sidecar state matches what was there before Blue Jay was confirmed.
    assert ("keyword_add", "Sparrow") in values
    assert ("keyword_add", "Blue Jay") not in values


def test_encounter_species_replacement_undo_after_sync_queues_swap(app_and_db):
    """If the replacement already synced, undo queues the reverse XMP ops."""
    app, db = app_and_db
    client = app.test_client()
    photo_ids = [p["id"] for p in db.conn.execute("SELECT id FROM photos").fetchall()]

    _seed_encounter_cache(app, db, photo_ids)
    client.post("/api/encounters/species",
                json={"species": "Sparrow", "photo_ids": photo_ids})
    client.post("/api/encounters/species",
                json={"species": "Blue Jay", "photo_ids": photo_ids})
    # Pretend the replacement has synced: drop all pending changes.
    db.conn.execute("DELETE FROM pending_changes")
    db.conn.commit()

    undone = db.undo_last_edit()
    assert undone is not None
    assert undone['action_type'] == 'species_replace'

    for pid in photo_ids:
        names = {k["name"] for k in db.get_photo_keywords(pid)}
        assert "Sparrow" in names
        assert "Blue Jay" not in names

    values = {(c["change_type"], c["value"]) for c in db.get_pending_changes()}
    assert ("keyword_remove", "Blue Jay") in values
    assert ("keyword_add", "Sparrow") in values


def test_encounter_species_replacement_redo_reapplies(app_and_db):
    """Redo after undo re-applies the replacement."""
    app, db = app_and_db
    client = app.test_client()
    photo_ids = [p["id"] for p in db.conn.execute("SELECT id FROM photos").fetchall()]

    _seed_encounter_cache(app, db, photo_ids)
    client.post("/api/encounters/species",
                json={"species": "Sparrow", "photo_ids": photo_ids})
    client.post("/api/encounters/species",
                json={"species": "Blue Jay", "photo_ids": photo_ids})

    db.undo_last_edit()
    redone = db.redo_last_undo()
    assert redone is not None
    assert redone['action_type'] == 'species_replace'

    for pid in photo_ids:
        names = {k["name"] for k in db.get_photo_keywords(pid)}
        assert "Blue Jay" in names
        assert "Sparrow" not in names


def test_encounter_species_no_op_when_all_already_tagged(app_and_db):
    """Confirming a species every submitted photo already carries records NO
    new keyword_add edit, so there's nothing on the undo stack to later
    destructively remove the pre-existing keyword.
    """
    app, db = app_and_db
    client = app.test_client()
    photo_ids = [p["id"] for p in db.conn.execute("SELECT id FROM photos").fetchall()]

    _seed_encounter_cache(app, db, photo_ids)
    # First confirm tags every photo and records one keyword_add.
    resp = client.post("/api/encounters/species",
                       json={"species": "Blue Jay", "photo_ids": photo_ids})
    assert resp.status_code == 200
    assert len(db.get_edit_history()) == 1

    # Re-confirm the SAME species on the SAME photos: all already tagged.
    resp = client.post("/api/encounters/species",
                       json={"species": "Blue Jay", "photo_ids": photo_ids})
    assert resp.status_code == 200
    assert resp.get_json()["ok"] is True

    # No new edit-history entry: the redundant confirm was a no-op.
    assert len(db.get_edit_history()) == 1

    # The pre-existing keyword survives — undoing leaves it intact (there's
    # nothing the redundant confirm could have queued to remove).
    for pid in photo_ids:
        names = {k["name"] for k in db.get_photo_keywords(pid)}
        assert "Blue Jay" in names


def test_prediction_accept_with_equivalent_hierarchy_records_no_tag_edit(app_and_db):
    """Accepting a prediction already represented by a hierarchical species
    changes review status without creating redoable root-tag history."""
    app, db = app_and_db
    client = app.test_client()
    db.conn.execute(
        "INSERT OR IGNORE INTO taxa (id, name, common_name, rank) "
        "VALUES (2912, 'Auriparus flaviceps', 'Verdin', 'species')"
    )
    db.conn.commit()
    folder_id = db.get_folder_tree()[0]["id"]
    photo_id = db.add_photo(
        folder_id=folder_id, filename="verdin.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    parent = db.add_keyword("Penduline tits")
    nested = db.add_keyword("Verdin", parent_id=parent)
    db.tag_photo(photo_id, nested)
    root = db.add_keyword("Verdin", is_species=True)
    assert db.get_photos_with_equivalent_species([photo_id], root) == {photo_id}
    detection_id = db.save_detections(
        photo_id,
        [{
            "box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
            "confidence": 0.9,
            "category": "animal",
        }],
        detector_model="MDV6",
    )[0]
    db.add_prediction(detection_id, "Verdin", 0.95, "bioclip")
    prediction_id = db.conn.execute(
        "SELECT id FROM predictions WHERE detection_id = ?",
        (detection_id,),
    ).fetchone()["id"]
    before = len([
        row for row in db.get_edit_history()
        if row["action_type"] == "prediction_accept"
    ])

    response = client.post(f"/api/predictions/{prediction_id}/accept")

    assert response.status_code == 200
    after = len([
        row for row in db.get_edit_history()
        if row["action_type"] == "prediction_accept"
    ])
    assert after == before
    names = [row["name"] for row in db.get_photo_keywords(photo_id)]
    assert names.count("Verdin") == 1
    assert db.conn.execute(
        "SELECT status FROM prediction_review WHERE prediction_id = ? "
        "AND workspace_id = ?",
        (prediction_id, db._active_workspace_id),
    ).fetchone()["status"] == "accepted"


def test_subject_accept_with_equivalent_hierarchy_records_no_tag_edit(app_and_db):
    """Additional-subject acceptance mirrors regular acceptance when the
    species already exists through a hierarchy leaf."""
    app, db = app_and_db
    client = app.test_client()
    db.conn.execute(
        "INSERT OR IGNORE INTO taxa (id, name, common_name, rank) "
        "VALUES (2912, 'Auriparus flaviceps', 'Verdin', 'species')"
    )
    db.conn.commit()
    folder_id = db.get_folder_tree()[0]["id"]
    photo_id = db.add_photo(
        folder_id=folder_id, filename="subject-verdin.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    parent = db.add_keyword("Penduline tits")
    nested = db.add_keyword("Verdin", parent_id=parent)
    db.tag_photo(photo_id, nested)
    detection_id = db.save_detections(
        photo_id,
        [{
            "box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
            "confidence": 0.9,
            "category": "animal",
        }],
        detector_model="MDV6",
    )[0]
    db.add_prediction(detection_id, "Verdin", 0.95, "bioclip")
    prediction_id = db.conn.execute(
        "SELECT id FROM predictions WHERE detection_id = ?",
        (detection_id,),
    ).fetchone()["id"]

    response = client.post(f"/api/predictions/{prediction_id}/accept-subject")

    assert response.status_code == 200
    assert not [
        row for row in db.get_edit_history()
        if row["action_type"] == "prediction_accept"
    ]
    names = [row["name"] for row in db.get_photo_keywords(photo_id)]
    assert names.count("Verdin") == 1
    assert db.conn.execute(
        "SELECT status FROM prediction_review WHERE prediction_id = ? "
        "AND workspace_id = ?",
        (prediction_id, db._active_workspace_id),
    ).fetchone()["status"] == "accepted"


def test_encounter_species_records_only_newly_tagged(app_and_db):
    """A mixed set (some already carry the species, some don't) records edit
    items ONLY for the newly-tagged photos.
    """
    app, db = app_and_db
    client = app.test_client()
    photo_ids = [p["id"] for p in db.conn.execute("SELECT id FROM photos").fetchall()]
    assert len(photo_ids) >= 2

    # Pre-tag the first photo with the species keyword directly.
    kid = db.add_keyword("Blue Jay", is_species=True)
    db.tag_photo(photo_ids[0], kid)

    _seed_encounter_cache(app, db, photo_ids)
    resp = client.post("/api/encounters/species",
                       json={"species": "Blue Jay", "photo_ids": photo_ids})
    assert resp.status_code == 200

    history = db.get_edit_history()
    assert len(history) == 1
    assert history[0]["action_type"] == "keyword_add"
    # Only the (len - 1) newly-tagged photos are in the edit items.
    assert history[0]["item_count"] == len(photo_ids) - 1

    # Undoing removes the species only from the photos that were newly tagged;
    # the pre-tagged photo keeps it.
    db.undo_last_edit()
    assert "Blue Jay" in {k["name"] for k in db.get_photo_keywords(photo_ids[0])}
    for pid in photo_ids[1:]:
        assert "Blue Jay" not in {k["name"] for k in db.get_photo_keywords(pid)}


def test_encounter_species_replacement_only_for_changed_photos(app_and_db):
    """Replacement records the replace only for photos that actually had the
    old species, and the add side only for photos that actually gained the new
    one.
    """
    app, db = app_and_db
    client = app.test_client()
    photo_ids = [p["id"] for p in db.conn.execute("SELECT id FROM photos").fetchall()]
    assert len(photo_ids) >= 2

    _seed_encounter_cache(app, db, photo_ids)
    # Confirm Sparrow on ALL photos.
    client.post("/api/encounters/species",
                json={"species": "Sparrow", "photo_ids": photo_ids})
    # Then untag the last photo's Sparrow so it no longer carries the old
    # species (simulates a partially-tagged burst on re-confirm).
    sparrow_id = db.conn.execute(
        "SELECT id FROM keywords WHERE name='Sparrow' AND is_species=1"
    ).fetchone()["id"]
    db.untag_photo(photo_ids[-1], sparrow_id)
    db.conn.execute("DELETE FROM edit_history")
    db.conn.commit()

    # Replace Sparrow -> Blue Jay across all photos.
    resp = client.post("/api/encounters/species",
                       json={"species": "Blue Jay", "photo_ids": photo_ids})
    assert resp.status_code == 200

    history = db.get_edit_history()
    assert len(history) == 1
    assert history[0]["action_type"] == "species_replace"
    # All photos gained Blue Jay (none had it), so all are recorded.
    assert history[0]["item_count"] == len(photo_ids)
    for pid in photo_ids:
        assert "Blue Jay" in {k["name"] for k in db.get_photo_keywords(pid)}
        assert "Sparrow" not in {k["name"] for k in db.get_photo_keywords(pid)}


def test_encounter_species_rejects_photo_ids_not_in_burst(app_and_db):
    """A valid burst_index plus photo_ids from a different burst must be rejected."""
    app, db = app_and_db
    client = app.test_client()
    photo_ids = [p["id"] for p in db.conn.execute("SELECT id FROM photos").fetchall()]
    assert len(photo_ids) >= 2

    # Two bursts: burst 0 holds photo_ids[:1], burst 1 holds photo_ids[1:].
    bursts = [
        {"photo_ids": photo_ids[:1], "species_predictions": [], "species_override": None},
        {"photo_ids": photo_ids[1:], "species_predictions": [], "species_override": None},
    ]
    _seed_encounter_cache(app, db, photo_ids, bursts=bursts)

    # burst_index 0 is in range, but we're submitting photos from burst 1.
    resp = client.post("/api/encounters/species",
                       json={"species": "Blue Jay",
                             "photo_ids": photo_ids[1:],
                             "burst_index": 0})
    assert resp.status_code == 400
    assert "bursts[0]" in resp.get_json()["error"]

    # Nothing should have been written.
    for pid in photo_ids:
        names = {k["name"] for k in db.get_photo_keywords(pid)}
        assert "Blue Jay" not in names
    assert not db.get_pending_changes()


def test_encounter_species_rejects_out_of_range_burst_index(app_and_db):
    """A stale burst_index must not silently fall through to an encounter update."""
    app, db = app_and_db
    client = app.test_client()
    photo_ids = [p["id"] for p in db.conn.execute("SELECT id FROM photos").fetchall()]

    # Encounter has exactly one burst.
    bursts = [{
        "photo_ids": photo_ids[:1],
        "species_predictions": [],
        "species_override": None,
    }]
    _seed_encounter_cache(app, db, photo_ids, bursts=bursts)

    resp = client.post("/api/encounters/species",
                       json={"species": "Blue Jay",
                             "photo_ids": photo_ids[:1],
                             "burst_index": 99})
    assert resp.status_code == 400
    assert "burst_index" in resp.get_json()["error"]

    # Nothing should have been written.
    for pid in photo_ids[:1]:
        names = {k["name"] for k in db.get_photo_keywords(pid)}
        assert "Blue Jay" not in names
    assert not db.get_pending_changes()


def test_encounter_species_replacement_removes_hierarchical_previous(app_and_db):
    """Replacing a confirmed species removes its nested hierarchy leaf and
    records enough history for undo and redo to restore the exact tags."""
    app, db = app_and_db
    client = app.test_client()
    db.conn.execute(
        "INSERT OR IGNORE INTO taxa (id, name, common_name, rank) "
        "VALUES (2912, 'Auriparus flaviceps', 'Verdin', 'species')"
    )
    db.conn.commit()
    photo_id = db.conn.execute(
        "SELECT id FROM photos ORDER BY id LIMIT 1"
    ).fetchone()["id"]
    parent = db.add_keyword("Penduline tits")
    nested = db.add_keyword("Verdin", parent_id=parent)
    alternate_parent = db.add_keyword("Desert birds")
    alternate_nested = db.add_keyword("Verdin", parent_id=alternate_parent)
    db.tag_photo(photo_id, nested)
    db.tag_photo(photo_id, alternate_nested)
    _seed_encounter_cache(
        app, db, [photo_id], confirmed_species="Verdin",
    )

    response = client.post(
        "/api/encounters/species",
        json={"species": "Blue Jay", "photo_ids": [photo_id]},
    )

    assert response.status_code == 200
    names = {row["name"] for row in db.get_photo_keywords(photo_id)}
    assert "Verdin" not in names
    assert "Blue Jay" in names
    tagged_ids = {row["id"] for row in db.get_photo_keywords(photo_id)}
    assert nested not in tagged_ids
    assert alternate_nested not in tagged_ids
    history = db.get_edit_history()
    assert history[0]["action_type"] == "species_replace"

    db.undo_last_edit()
    names = {row["name"] for row in db.get_photo_keywords(photo_id)}
    assert "Verdin" in names
    assert "Blue Jay" not in names
    tagged_ids = {row["id"] for row in db.get_photo_keywords(photo_id)}
    assert nested in tagged_ids
    assert alternate_nested in tagged_ids

    db.redo_last_undo()
    names = {row["name"] for row in db.get_photo_keywords(photo_id)}
    assert "Verdin" not in names
    assert "Blue Jay" in names
    tagged_ids = {row["id"] for row in db.get_photo_keywords(photo_id)}
    assert nested not in tagged_ids
    assert alternate_nested not in tagged_ids


def test_encounter_species_replacement_retags_same_taxon_alias(app_and_db):
    """Same-taxon replacement (e.g. common name → scientific alias) must still
    end up tagging the photo with the new keyword row instead of stripping the
    old row and leaving the photo without any species.

    ``get_photos_with_equivalent_species`` matches by taxon_id, so a photo
    tagged with the previous species alias gets flagged as "already carrying"
    the new one. Without excluding rows scheduled for removal from that
    precheck, the replacement loop untags the old row, ``newly_tagged`` stays
    empty, and the photo ends up carrying no species keyword.
    """
    app, db = app_and_db
    client = app.test_client()
    db.conn.execute(
        "INSERT OR IGNORE INTO taxa (id, name, common_name, rank) "
        "VALUES (2912, 'Auriparus flaviceps', 'Verdin', 'species')"
    )
    db.conn.commit()
    photo_id = db.conn.execute(
        "SELECT id FROM photos ORDER BY id LIMIT 1"
    ).fetchone()["id"]
    # Prior confirmation left a linked root "Verdin" row plus a hierarchical
    # leaf that also carries the taxon — replacement must strip both and
    # still leave the photo with the new scientific-name row.
    root_verdin = db.add_keyword("Verdin", is_species=True)
    parent = db.add_keyword("Penduline tits")
    nested_verdin = db.add_keyword("Verdin", parent_id=parent)
    # Backfill taxon on the nested leaf so the same-taxon removal loop
    # includes it (matches mark_species_keywords' link behavior).
    db.conn.execute(
        "UPDATE keywords SET taxon_id = 2912, is_species = 1, type = 'taxonomy' "
        "WHERE id = ?",
        (nested_verdin,),
    )
    db.conn.commit()
    db.tag_photo(photo_id, root_verdin)
    db.tag_photo(photo_id, nested_verdin)
    _seed_encounter_cache(app, db, [photo_id], confirmed_species="Verdin")

    resp = client.post(
        "/api/encounters/species",
        json={"species": "Auriparus flaviceps", "photo_ids": [photo_id]},
    )
    assert resp.status_code == 200

    def _species_names_on(pid):
        return sorted(
            row["name"] for row in db.conn.execute(
                """SELECT k.name FROM keywords k
                   JOIN photo_keywords pk ON pk.keyword_id = k.id
                   WHERE pk.photo_id = ?
                     AND (k.is_species = 1 OR k.type = 'taxonomy')""",
                (pid,),
            ).fetchall()
        )

    remaining = _species_names_on(photo_id)
    assert remaining == ["Auriparus flaviceps"], (
        f"expected only the new scientific-name row, got {remaining!r}"
    )
    history = db.get_edit_history()
    assert history[0]["action_type"] == "species_replace"

    db.undo_last_edit()
    restored = _species_names_on(photo_id)
    assert "Verdin" in restored
    assert "Auriparus flaviceps" not in restored

    db.redo_last_undo()
    redone = _species_names_on(photo_id)
    assert redone == ["Auriparus flaviceps"]


def test_encounter_species_replacement_preserves_linked_homonym(app_and_db):
    """When ``previous_species`` resolves to an unlinked legacy row and the
    photo carries a distinct linked same-key homonym, the linked row is a
    different species and must not be scheduled for removal.

    Regression: the ``old_target_taxon_id is None`` branch matched every
    species row on the photo by display key, so an encounter replacement
    where the cache's confirmed species was legacy ``Robin`` (NULL taxon)
    would delete a taxonomy-linked ``robin`` from the same photo.
    """
    app, db = app_and_db
    client = app.test_client()
    db.conn.execute(
        "INSERT OR IGNORE INTO taxa (id, name, common_name, rank) "
        "VALUES (400, 'Turdus migratorius', 'American Robin', 'species')"
    )
    db.conn.commit()
    photo_id = db.conn.execute(
        "SELECT id FROM photos ORDER BY id LIMIT 1"
    ).fetchone()["id"]

    # Unlinked legacy species row — this becomes ``old_kid_row``.
    legacy_root = db.add_keyword("Robin", is_species=True)
    db.conn.execute(
        "UPDATE keywords SET taxon_id = NULL, type = 'taxonomy' WHERE id = ?",
        (legacy_root,),
    )
    # Distinct linked same-key row anywhere in the catalog. add_keyword's
    # (name, parent_id, type) UNIQUE constraint dedupes at parent_id=NULL,
    # so insert this one under a hierarchy parent to make it a separate row.
    hierarchy_parent = db.add_keyword("Turdidae")
    linked_homonym = db.conn.execute(
        "INSERT INTO keywords (name, parent_id, is_species, type, taxon_id) "
        "VALUES ('Robin', ?, 1, 'taxonomy', 400)",
        (hierarchy_parent,),
    ).lastrowid
    db.tag_photo(photo_id, linked_homonym)
    db.conn.commit()

    _seed_encounter_cache(app, db, [photo_id], confirmed_species="Robin")

    resp = client.post(
        "/api/encounters/species",
        json={"species": "Blue Jay", "photo_ids": [photo_id]},
    )
    assert resp.status_code == 200

    tagged_ids = {row["id"] for row in db.get_photo_keywords(photo_id)}
    assert linked_homonym in tagged_ids, (
        "Linked same-key homonym is a distinct species — it must survive "
        "an encounter replacement whose previous species is unlinked and "
        "only coincidentally shares the normalized name"
    )


def test_encounter_species_replacement_preserves_legacy_homonym(app_and_db):
    """When the previous species is a linked taxon and the catalog also
    holds another taxonomy row with the SAME normalized key bound to a
    different taxon (a homonym: legacy ``Robin`` alongside taxonomy
    ``Robin`` pointing at a different species), a replacement must not
    match unlinked NULL-taxon same-key rows as the old species. Otherwise
    a legacy ``Robin`` tag on the photo (potentially the intended
    homonym) is queued for removal purely because its normalized key
    coincides with the swapped-out species name.
    """
    app, db = app_and_db
    client = app.test_client()
    db.conn.execute(
        "INSERT OR IGNORE INTO taxa (id, name, common_name, rank) "
        "VALUES (100, 'Erithacus rubecula', 'Robin', 'species')"
    )
    db.conn.execute(
        "INSERT OR IGNORE INTO taxa (id, name, common_name, rank) "
        "VALUES (200, 'Turdus migratorius', 'American Robin', 'species')"
    )
    db.conn.commit()
    photo_id = db.conn.execute(
        "SELECT id FROM photos ORDER BY id LIMIT 1"
    ).fetchone()["id"]

    # Linked "old" species Erithacus rubecula (taxon 100), name "Robin".
    old_root = db.add_keyword("Robin", is_species=True)
    db.conn.execute(
        "UPDATE keywords SET taxon_id = 100, type = 'taxonomy' WHERE id = ?",
        (old_root,),
    )
    # A DIFFERENT taxonomy row anywhere in the catalog whose normalized key
    # is also "robin" but points at a different taxon (American Robin).
    # add_keyword's (name, parent_id, type) UNIQUE constraint means we insert
    # directly under a hierarchy parent to make it a distinct row.
    homonym_parent = db.add_keyword("Turdidae")
    db.conn.execute(
        "INSERT INTO keywords (name, parent_id, is_species, type, taxon_id) "
        "VALUES ('Robin', ?, 1, 'taxonomy', 200)",
        (homonym_parent,),
    )
    # A separate legacy species leaf on the photo whose taxon has not
    # been backfilled (species-like but taxon_id IS NULL). This mimics
    # an upgraded catalog where a hierarchy leaf was flagged as
    # taxonomy but never linked. Because its normalized key coincides
    # with the swapped-out species, without the homonym guard the
    # replacement would treat it as the old species and untag it.
    legacy_parent = db.add_keyword("Old Field Guide")
    legacy_leaf = db.add_keyword("Robin", parent_id=legacy_parent)
    db.conn.execute(
        "UPDATE keywords SET taxon_id = NULL, is_species = 1, type = 'taxonomy' "
        "WHERE id = ?",
        (legacy_leaf,),
    )
    db.tag_photo(photo_id, old_root)
    db.tag_photo(photo_id, legacy_leaf)
    db.conn.commit()

    _seed_encounter_cache(app, db, [photo_id], confirmed_species="Robin")

    resp = client.post(
        "/api/encounters/species",
        json={"species": "Blue Jay", "photo_ids": [photo_id]},
    )
    assert resp.status_code == 200

    tagged_ids = {row["id"] for row in db.get_photo_keywords(photo_id)}
    # Linked root "Robin" was the confirmed old species — must be untagged.
    assert old_root not in tagged_ids
    # Legacy leaf "Robin" is an ambiguous homonym — must survive; the
    # replacement should not treat it as the old species just because
    # the normalized key matches.
    assert legacy_leaf in tagged_ids


def test_encounter_species_replacement_ignores_nested_homonym(app_and_db):
    """Old-species lookup must be scoped to root species keywords only."""
    app, db = app_and_db
    client = app.test_client()
    photo_ids = [p["id"] for p in db.conn.execute("SELECT id FROM photos").fetchall()]

    _seed_encounter_cache(app, db, photo_ids)

    # Confirm as Sparrow (creates root species keyword).
    resp = client.post("/api/encounters/species",
                       json={"species": "Sparrow", "photo_ids": photo_ids})
    assert resp.status_code == 200
    root_sparrow_id = db.conn.execute(
        "SELECT id FROM keywords WHERE name = 'Sparrow' AND parent_id IS NULL"
    ).fetchone()["id"]

    # Create a non-species homonym "Sparrow" nested under another keyword. If
    # the replacement lookup were scoped by name only, it could resolve here
    # and leave the real species tag intact.
    parent = db.add_keyword("Birds")
    db.conn.execute(
        "INSERT INTO keywords (name, parent_id, is_species) VALUES ('Sparrow', ?, 0)",
        (parent,),
    )
    db.conn.commit()

    # Change species — the root Sparrow tag must still be removed.
    resp = client.post("/api/encounters/species",
                       json={"species": "Blue Jay", "photo_ids": photo_ids})
    assert resp.status_code == 200

    for pid in photo_ids:
        kw_ids = {k["id"] for k in db.get_photo_keywords(pid)}
        assert root_sparrow_id not in kw_ids


def test_encounter_species_replacement_finds_taxonomy_only_previous(app_and_db):
    """Replacement path resolves ``previous_species`` via the taxonomy-only rule.

    When ``previous_species`` (drawn from the pipeline cache's
    ``confirmed_species``) resolves to a stored row that carries
    ``type='taxonomy'`` but ``is_species=0`` (update_keyword with an explicit
    type doesn't set the legacy column), the ``old_kid`` lookup must find it.
    Without ``OR type='taxonomy'`` it misses, ``is_replacement`` stays true
    but no ``old_kid`` is resolved, so the previously-tagged photo keeps the
    old tag while ``add_keyword`` adds the new species alongside it —
    leaving two taxonomy tags on the same photo.
    """
    app, db = app_and_db
    client = app.test_client()

    prev_kid = db.conn.execute(
        "INSERT INTO keywords (name, type, is_species) VALUES ('apapane', 'taxonomy', 0)"
    ).lastrowid
    db.conn.commit()

    photo_id = db.conn.execute(
        "SELECT id FROM photos ORDER BY id LIMIT 1"
    ).fetchone()["id"]
    db.tag_photo(photo_id, prev_kid)

    _seed_encounter_cache(app, db, [photo_id], confirmed_species="apapane")

    resp = client.post(
        '/api/encounters/species',
        json={"species": "iiwi", "photo_ids": [photo_id]},
    )
    assert resp.status_code == 200
    assert resp.get_json().get("previous_species") == "apapane"

    remaining = db.conn.execute(
        """SELECT k.name FROM keywords k
           JOIN photo_keywords pk ON pk.keyword_id = k.id
           WHERE pk.photo_id = ?
             AND (k.is_species = 1 OR k.type = 'taxonomy')""",
        (photo_id,),
    ).fetchall()
    names = sorted(row["name"] for row in remaining)
    assert names == ["iiwi"], (
        f"expected only the new species after replacement, got {names!r}"
    )


def test_encounter_species_replacement_queues_stored_previous_name(app_and_db):
    """When the pipeline cache still holds a legacy quoted species like
    `‘apapane` but the DB row is stored under the normalized `apapane`, the
    replacement path must queue keyword_remove with the STORED normalized
    name so it cancels an outstanding keyword_add for the same normalized
    key. Queuing the raw quoted value would leave both the stale add and a
    quoted remove in the pending set, and the next XMP sync would then
    write the stray-quote spelling back to the sidecar. The v5 migration
    normalizes DB rows but not pipeline cache files, so this state remains
    reachable.
    """
    app, db = app_and_db
    client = app.test_client()
    photo_ids = [p["id"] for p in db.conn.execute("SELECT id FROM photos").fetchall()]

    # Pretend an earlier confirm queued a keyword_add for the normalized
    # spelling. This is the state we need cancellation to fire against.
    resp = client.post(
        "/api/encounters/species",
        json={"species": "Apapane", "photo_ids": photo_ids},
    )
    assert resp.status_code == 200
    values = {(c["change_type"], c["value"]) for c in db.get_pending_changes()}
    assert ("keyword_add", "Apapane") in values

    # Seed the pipeline cache's confirmed_species with the LEGACY quoted
    # spelling — an upgraded pipeline cache written before keyword
    # normalization landed. The DB row is stored as clean `Apapane`.
    _seed_encounter_cache(app, db, photo_ids, confirmed_species="‘Apapane")

    resp = client.post(
        "/api/encounters/species",
        json={"species": "Blue Jay", "photo_ids": photo_ids},
    )
    assert resp.status_code == 200

    values = {(c["change_type"], c["value"]) for c in db.get_pending_changes()}
    # The stale keyword_add for the normalized spelling must be cancelled by
    # a remove that targets the same normalized value — not the raw quoted
    # cache value. If the queue used the cache spelling, both would linger.
    assert ("keyword_add", "Apapane") not in values
    assert ("keyword_remove", "‘Apapane") not in values
    assert ("keyword_add", "Blue Jay") in values


def test_encounter_species_replacement_queues_stored_case_previous_name(app_and_db):
    """Cache/stored spelling can differ by case only (SQLite NOCASE keeps
    them together, but pending_changes.value is exact-match). When
    replacing a species whose cache reads `saffron finch` but the stored
    keyword row and outstanding pending add are `Saffron Finch`, the
    remove queued for the old species must use the stored spelling so it
    cancels the pending add. Queuing the cache spelling would leave both
    add(`Saffron Finch`) and remove(`saffron finch`) in the pending set;
    sync_to_xmp then treats them as a paired rename and writes the old
    species back to the sidecar.
    """
    app, db = app_and_db
    client = app.test_client()
    photo_ids = [p["id"] for p in db.conn.execute("SELECT id FROM photos").fetchall()]

    # Earlier confirm queues keyword_add('Saffron Finch') under the DB's
    # canonical case.
    resp = client.post(
        "/api/encounters/species",
        json={"species": "Saffron Finch", "photo_ids": photo_ids},
    )
    assert resp.status_code == 200
    values = {(c["change_type"], c["value"]) for c in db.get_pending_changes()}
    assert ("keyword_add", "Saffron Finch") in values

    # Cache confirmed_species drifts to a case-variant spelling (upgraded
    # cache written by a client that normalized case differently).
    _seed_encounter_cache(app, db, photo_ids, confirmed_species="saffron finch")

    resp = client.post(
        "/api/encounters/species",
        json={"species": "Blue Jay", "photo_ids": photo_ids},
    )
    assert resp.status_code == 200

    values = {(c["change_type"], c["value"]) for c in db.get_pending_changes()}
    # The stale add must be cancelled by a remove that targets the same
    # stored spelling — not the lowercase cache value.
    assert ("keyword_add", "Saffron Finch") not in values
    assert ("keyword_remove", "saffron finch") not in values
    assert ("keyword_add", "Blue Jay") in values


def test_species_search(app_and_db):
    """GET /api/species/search returns matching species from keywords."""
    app, db = app_and_db
    client = app.test_client()

    # Add a species keyword
    db.add_keyword("American Robin", is_species=True)
    db.add_keyword("Robin Redbreast", is_species=True)

    resp = client.get('/api/species/search?q=robin')
    assert resp.status_code == 200
    data = resp.get_json()
    assert len(data) >= 2
    names_lower = [n.lower() for n in data]
    assert any("robin" in n for n in names_lower)

    db.add_keyword("Western Tanager", is_species=True)
    db.add_keyword("Common Tern", is_species=True)

    resp = client.get('/api/species/search?q=tern')
    assert resp.status_code == 200
    names = resp.get_json()
    assert "Western Tanager" in names
    assert "Common Tern" in names

    resp = client.get('/api/species/search?q=tern&whole_word=1')
    assert resp.status_code == 200
    names = resp.get_json()
    assert "Western Tanager" not in names
    assert "Common Tern" in names

    resp = client.get('/api/species/search?q=Tern&match_case=1')
    assert resp.status_code == 200
    names = resp.get_json()
    assert "Western Tanager" not in names
    assert "Common Tern" in names

    # Too short query returns empty
    resp = client.get('/api/species/search?q=r')
    assert resp.status_code == 200
    assert resp.get_json() == []


def test_pipeline_review_page(app_and_db):
    """GET /pipeline/review returns 200 with explicit summary units."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get('/pipeline/review')
    assert resp.status_code == 200
    html = resp.get_data(as_text=True)
    assert "Inventory" in html
    assert "Triage" in html
    assert "Species Review" in html
    assert "Keep Photos" in html
    assert "Review Units Confirmed" in html
    assert "Species confirmation is counted by review unit" in html


def test_classify_route_removed(app_and_db):
    """GET /classify should return 404 after removal."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get('/classify')
    assert resp.status_code == 404


def test_pipeline_regroup_accepts_collection_id(app_and_db):
    """POST /api/jobs/regroup accepts collection_id parameter."""
    app, db = app_and_db
    client = app.test_client()

    # Create a smart collection containing all photos
    photos = db.conn.execute("SELECT id FROM photos").fetchall()
    photo_ids = [p["id"] for p in photos]
    cid = db.add_collection("test-pipeline", '[{"field":"photo_ids","value":' + str(photo_ids) + '}]')

    # The job will fail because no pipeline features exist, but the route
    # should accept collection_id without error
    resp = client.post('/api/jobs/regroup', json={"collection_id": cid})
    assert resp.status_code == 200
    data = resp.get_json()
    assert "job_id" in data


def test_static_css_served(app_and_db):
    """vireo-base.css is served from /static/."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get('/static/vireo-base.css')
    assert resp.status_code == 200
    assert 'text/css' in resp.content_type
    body = resp.data.decode()
    assert 'box-sizing: border-box' in body


def test_pages_link_base_css(app_and_db):
    """Every page includes a <link> to vireo-base.css."""
    app, _ = app_and_db
    client = app.test_client()
    pages = ['/browse', '/lightroom', '/audit', '/logs',
             '/settings', '/storage', '/workspace', '/pipeline', '/dashboard',
             '/review', '/cull', '/pipeline/review', '/map', '/shortcuts']
    for page in pages:
        resp = client.get(page)
        assert resp.status_code == 200, f"{page} returned {resp.status_code}"
        html = resp.data.decode()
        assert 'vireo-base.css' in html, f"{page} missing vireo-base.css link"


def test_compare_page(app_and_db):
    """GET /compare returns 200."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get('/compare')
    assert resp.status_code == 200


def test_compare_link_in_navbar(app_and_db):
    """The navbar includes a link to /compare."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get('/compare')
    assert b'/compare' in resp.data
    assert b'Compare' in resp.data


def test_compare_predictions_api(app_and_db):
    """GET /api/predictions/compare returns per-photo, per-model data."""
    app, db = app_and_db

    # Get photo IDs
    photos = db.conn.execute("SELECT id FROM photos").fetchall()
    photo_ids = [p["id"] for p in photos]

    # Create a collection containing all photos
    import json
    rules = json.dumps([{"field": "photo_ids", "value": photo_ids}])
    cid = db.add_collection("Test Collection", rules)

    # Create detections, then add predictions from two models
    det_ids_0 = db.save_detections(photo_ids[0], [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.3}, "confidence": 0.9, "category": "animal"},
    ], detector_model="MDV6")
    det_ids_1 = db.save_detections(photo_ids[1], [
        {"box": {"x": 0.2, "y": 0.2, "w": 0.4, "h": 0.4}, "confidence": 0.85, "category": "animal"},
    ], detector_model="MDV6")
    db.add_prediction(det_ids_0[0], "Cardinal", 0.95, "model-a")
    db.add_prediction(det_ids_0[0], "Blue Jay", 0.80, "model-b")
    db.add_prediction(det_ids_1[0], "Sparrow", 0.90, "model-a")
    db.add_prediction(det_ids_1[0], "Sparrow", 0.88, "model-b")

    client = app.test_client()
    resp = client.get(f"/api/predictions/compare?collection_id={cid}")
    assert resp.status_code == 200
    data = resp.get_json()

    assert "models" in data
    assert set(data["models"]) == {"model-a", "model-b"}
    assert "photos" in data
    assert len(data["photos"]) >= 2

    # Check structure of a photo entry
    photo = next(
        item for item in data["photos"]
        if item["photo_id"] == photo_ids[0]
    )
    assert "photo_id" in photo
    assert "filename" in photo
    assert "predictions" in photo
    assert isinstance(photo["predictions"], dict)  # keyed by model name
    assert "subjects" in photo
    subject = next(
        item for item in photo["subjects"]
        if item["detection_id"] == det_ids_0[0]
    )
    assert subject["box"] == {
        "x": 0.1, "y": 0.1, "w": 0.3, "h": 0.3,
    }
    assert set(subject["predictions"]) == {"model-a", "model-b"}
    # Each model maps to a list of predictions (multi-detection support)
    for model_preds in photo["predictions"].values():
        assert isinstance(model_preds, list)
        assert len(model_preds) >= 1
        assert "species" in model_preds[0]
        assert "confidence" in model_preds[0]
        assert "detection_id" in model_preds[0]


def test_compare_predictions_api_preserves_unclassified_subject(app_and_db):
    """A qualifying box remains visible when no classifier prediction exists."""
    app, db = app_and_db
    photo_id = db.conn.execute(
        "SELECT id FROM photos ORDER BY id LIMIT 1"
    ).fetchone()["id"]
    det_id = db.save_detections(
        photo_id,
        [{
            "box": {"x": 0.55, "y": 0.4, "w": 0.2, "h": 0.2},
            "confidence": 0.45,
            "category": "animal",
        }],
        detector_model="MDV6",
    )[0]
    cid = db.add_collection(
        "Unclassified Subject",
        json.dumps([{"field": "photo_ids", "value": [photo_id]}]),
    )

    response = app.test_client().get(
        f"/api/predictions/compare?collection_id={cid}"
    )

    assert response.status_code == 200
    subjects = response.get_json()["photos"][0]["subjects"]
    subject = next(item for item in subjects if item["detection_id"] == det_id)
    assert subject["predictions"] == {}


def test_compare_predictions_api_exposes_miss_flags(app_and_db):
    """Miss flags on a photo must reach the compare payload so the
    'Hide marked misses' exclusion and 'marked miss' badge can work."""
    app, db = app_and_db

    photos = db.conn.execute("SELECT id FROM photos ORDER BY id").fetchall()
    p_miss = photos[0]["id"]
    p_clean = photos[1]["id"]
    db.conn.execute(
        "UPDATE photos SET miss_no_subject=1, miss_clipped=1, miss_oof=1 WHERE id=?",
        (p_miss,),
    )
    db.conn.commit()

    rules = json.dumps([{"field": "photo_ids", "value": [p_miss, p_clean]}])
    cid = db.add_collection("Miss Flag Collection", rules)

    resp = app.test_client().get(f"/api/predictions/compare?collection_id={cid}")
    assert resp.status_code == 200
    by_id = {row["photo_id"]: row for row in resp.get_json()["photos"]}

    assert by_id[p_miss]["miss_no_subject"] is True
    assert by_id[p_miss]["miss_clipped"] is True
    assert by_id[p_miss]["miss_oof"] is True
    assert by_id[p_clean]["miss_no_subject"] is False
    assert by_id[p_clean]["miss_clipped"] is False
    assert by_id[p_clean]["miss_oof"] is False


def test_compare_ignores_resolved_predictions_for_needs_review(app_and_db):
    """When every prediction on a photo is already reviewed, no needs_review."""
    app, db = app_and_db
    photo_id = db.conn.execute(
        "SELECT id FROM photos ORDER BY id LIMIT 1"
    ).fetchone()["id"]
    species_id = db.add_keyword("Cardinal", is_species=True)
    db.tag_photo(photo_id, species_id)
    rules = json.dumps([{"field": "photo_ids", "value": [photo_id]}])
    cid = db.add_collection("Single Photo", rules)

    det_ids = db.save_detections(photo_id, [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.3}, "confidence": 0.9, "category": "animal"},
        {"box": {"x": 0.2, "y": 0.2, "w": 0.4, "h": 0.4}, "confidence": 0.85, "category": "animal"},
    ], detector_model="MDV6")
    db.add_prediction(
        det_ids[0], "Blue Jay", 0.95, "model-a", status="accepted",
    )
    db.add_prediction(
        det_ids[1], "Cardinal", 0.92, "model-b", status="accepted",
    )

    resp = app.test_client().get(f"/api/predictions/compare?collection_id={cid}")

    assert resp.status_code == 200
    row = resp.get_json()["photos"][0]
    # Both predictions are already reviewed (accepted). The row falls back
    # to the highest-priority category among all predictions, but nothing is
    # pending so it must not appear in the Needs review count/filter.
    assert row["row_category"] == "conflict"
    assert row["needs_review"] is False
    assert resp.get_json()["summary"]["needs_review"] == 0


def test_compare_flags_pending_match_as_needs_review(app_and_db):
    """A pending match prediction must still surface in the needs-review
    count/filter.

    classify_job stores a pending prediction with ``category="match"`` when a
    photo-level match is ambiguous (e.g. multiple recognized species keywords
    in the sidecar), and expects Compare to route the user to it. Before this
    fix, ``/api/predictions/compare`` excluded ``match`` from ``needs_review``,
    so those deliberately-pending detections disappeared from the queue.
    """

    app, db = app_and_db
    photo_id = db.conn.execute(
        "SELECT id FROM photos ORDER BY id LIMIT 1"
    ).fetchone()["id"]
    species_id = db.add_keyword("Cardinal", is_species=True)
    db.tag_photo(photo_id, species_id)
    rules = json.dumps([{"field": "photo_ids", "value": [photo_id]}])
    cid = db.add_collection("Pending Match", rules)

    det_ids = db.save_detections(photo_id, [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.3},
         "confidence": 0.9, "category": "animal"},
    ], detector_model="MDV6")
    db.add_prediction(det_ids[0], "Cardinal", 0.92, "model-a")

    resp = app.test_client().get(f"/api/predictions/compare?collection_id={cid}")

    assert resp.status_code == 200
    payload = resp.get_json()
    row = payload["photos"][0]
    assert row["row_category"] == "match"
    assert row["needs_review"] is True
    assert payload["summary"]["needs_review"] == 1


def test_compare_accepted_matches_are_not_marked_missing(app_and_db):
    """Accepted match predictions still make the photo a match row."""
    app, db = app_and_db
    photo_id = db.conn.execute(
        "SELECT id FROM photos ORDER BY id LIMIT 1"
    ).fetchone()["id"]
    species_id = db.add_keyword("Cardinal", is_species=True)
    db.tag_photo(photo_id, species_id)
    rules = json.dumps([{"field": "photo_ids", "value": [photo_id]}])
    cid = db.add_collection("Accepted Matches", rules)

    det_ids = db.save_detections(photo_id, [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.3}, "confidence": 0.9, "category": "animal"},
    ], detector_model="MDV6")
    db.add_prediction(det_ids[0], "Cardinal", 0.95, "model-a", status="accepted")
    db.add_prediction(det_ids[0], "Cardinal", 0.93, "model-b", status="accepted")

    resp = app.test_client().get(f"/api/predictions/compare?collection_id={cid}")

    assert resp.status_code == 200
    data = resp.get_json()
    row = data["photos"][0]
    assert row["row_category"] == "match"
    assert row["row_label"] == "Match"
    assert row["needs_review"] is False
    assert data["summary"]["missing_predictions"] == 0


def test_replace_prediction_keywords_updates_grouped_photos(app_and_db):
    """Replacing a grouped prediction removes old species keywords from the group."""
    app, db = app_and_db
    photos = db.conn.execute("SELECT id FROM photos ORDER BY id LIMIT 2").fetchall()
    photo_ids = [p["id"] for p in photos]

    old_one = db.add_keyword("Old Species One", is_species=True)
    old_two = db.add_keyword("Old Species Two", is_species=True)
    db.tag_photo(photo_ids[0], old_one)
    db.tag_photo(photo_ids[1], old_two)

    first_det = db.save_detections(photo_ids[0], [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.3}, "confidence": 0.9, "category": "animal"},
    ], detector_model="MDV6")[0]
    second_det = db.save_detections(photo_ids[1], [
        {"box": {"x": 0.2, "y": 0.2, "w": 0.4, "h": 0.4}, "confidence": 0.85, "category": "animal"},
    ], detector_model="MDV6")[0]
    db.add_prediction(
        first_det, "New Species", 0.95, "model-a", group_id="group-1",
    )
    db.add_prediction(
        second_det, "New Species", 0.92, "model-a", group_id="group-1",
    )
    pred = db.conn.execute(
        """SELECT id FROM predictions
           WHERE detection_id = ? AND classifier_model = ?""",
        (first_det, "model-a"),
    ).fetchone()

    resp = app.test_client().post(
        f"/api/predictions/{pred['id']}/replace-keywords"
    )

    assert resp.status_code == 200
    for pid in photo_ids:
        names = {k["name"] for k in db.get_photo_keywords(pid)}
        assert "New Species" in names
        assert "Old Species One" not in names
        assert "Old Species Two" not in names

    # The DB rows are gone, but sync_to_xmp only strips a sidecar keyword
    # when a matching keyword_remove pending change exists. Without one the
    # old species would silently linger in the XMP files.
    changes = db.get_pending_changes()
    removed = {
        (c["photo_id"], c["value"])
        for c in changes
        if c["change_type"] == "keyword_remove"
    }
    assert (photo_ids[0], "Old Species One") in removed
    assert (photo_ids[1], "Old Species Two") in removed
    for pid in photo_ids:
        assert any(
            c["photo_id"] == pid
            and c["change_type"] == "keyword_add"
            and c["value"] == "New Species"
            for c in changes
        )


def test_replace_prediction_keywords_migrates_species_curation(app_and_db):
    """Replacing species via prediction moves ordered highlights and
    representative preferences from the old species to the new one.
    Without this migration, a curated photo silently loses its
    Highlights/Life-List position when its species is swapped."""
    app, db = app_and_db
    photo_id = db.conn.execute(
        "SELECT id FROM photos ORDER BY id LIMIT 1"
    ).fetchone()["id"]
    old_kid = db.add_keyword("Old Species", is_species=True)
    db.tag_photo(photo_id, old_kid)
    db.add_species_highlight("Old Species", photo_id)
    db.set_species_representative("Old Species", photo_id)

    det_id = db.save_detections(photo_id, [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.3},
         "confidence": 0.9, "category": "animal"},
    ], detector_model="MDV6")[0]
    db.add_prediction(det_id, "New Species", 0.95, "model-a")
    pred = db.conn.execute(
        """SELECT id FROM predictions
           WHERE detection_id = ? AND classifier_model = ?""",
        (det_id, "model-a"),
    ).fetchone()

    resp = app.test_client().post(
        f"/api/predictions/{pred['id']}/replace-keywords"
    )
    assert resp.status_code == 200

    ws_id = db._ws_id()
    highlights = db.conn.execute(
        """SELECT species FROM species_highlights
           WHERE workspace_id = ? AND photo_id = ?""",
        (ws_id, photo_id),
    ).fetchall()
    assert [r["species"] for r in highlights] == ["New Species"]

    prefs = db.conn.execute(
        """SELECT purpose, species FROM photo_preferences
           WHERE workspace_id = ? AND photo_id = ?""",
        (ws_id, photo_id),
    ).fetchall()
    assert {(p["purpose"], p["species"]) for p in prefs} == {
        ("species_representative", "New Species"),
    }


def test_api_predictions_include_bounding_box(app_and_db):
    """GET /api/predictions should return bounding box data from detections."""
    app, db = app_and_db
    photos = db.conn.execute("SELECT id FROM photos").fetchall()
    pid = photos[0]["id"]
    det_ids = db.save_detections(pid, [
        {"box": {"x": 0.1, "y": 0.2, "w": 0.3, "h": 0.4}, "confidence": 0.9, "category": "animal"},
    ], detector_model="MDV6")
    db.add_prediction(det_ids[0], species="Elk", confidence=0.9, model="bioclip")

    client = app.test_client()
    resp = client.get("/api/predictions")
    data = resp.get_json()
    assert len(data) == 1
    assert data[0]["box_x"] == 0.1
    assert data[0]["box_y"] == 0.2
    assert data[0]["box_w"] == 0.3
    assert data[0]["box_h"] == 0.4
    assert data[0]["photo_id"] == pid


def test_api_predictions_multiple_detections(app_and_db):
    """GET /api/predictions should return one prediction per detection."""
    app, db = app_and_db
    photos = db.conn.execute("SELECT id FROM photos").fetchall()
    pid = photos[0]["id"]
    det_ids = db.save_detections(pid, [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.2, "h": 0.3}, "confidence": 0.95, "category": "animal"},
        {"box": {"x": 0.5, "y": 0.5, "w": 0.2, "h": 0.3}, "confidence": 0.80, "category": "animal"},
    ], detector_model="MDV6")
    db.add_prediction(det_ids[0], species="Elk", confidence=0.92, model="bioclip")
    db.add_prediction(det_ids[1], species="Magpie", confidence=0.85, model="bioclip")

    client = app.test_client()
    resp = client.get("/api/predictions")
    data = resp.get_json()
    assert len(data) == 2
    species = {d["species"] for d in data}
    assert species == {"Elk", "Magpie"}


def test_api_detections_endpoint(app_and_db):
    """GET /api/detections/<photo_id> returns all detections for a photo."""
    app, db = app_and_db
    photos = db.conn.execute("SELECT id FROM photos").fetchall()
    pid = photos[0]["id"]
    db.save_detections(pid, [
        {"box": {"x": 0.1, "y": 0.2, "w": 0.3, "h": 0.4}, "confidence": 0.9, "category": "animal"},
        {"box": {"x": 0.5, "y": 0.6, "w": 0.2, "h": 0.1}, "confidence": 0.7, "category": "animal"},
    ], detector_model="MDV6")

    client = app.test_client()
    resp = client.get(f"/api/detections/{pid}")
    assert resp.status_code == 200
    data = resp.get_json()
    assert len(data) == 2
    # Sorted by confidence descending
    assert data[0]["detector_confidence"] >= data[1]["detector_confidence"]
    assert data[0]["box_x"] == 0.1


def test_api_photo_pipeline_detections(app_and_db):
    """GET /api/photos/<id>/pipeline returns detections and predictions with box data."""
    app, db = app_and_db
    photos = db.conn.execute("SELECT id FROM photos").fetchall()
    pid = photos[0]["id"]
    det_ids = db.save_detections(pid, [
        {"box": {"x": 0.1, "y": 0.2, "w": 0.3, "h": 0.4}, "confidence": 0.9, "category": "animal"},
    ], detector_model="MDV6")
    db.add_prediction(det_ids[0], species="Robin", confidence=0.88, model="bioclip")

    client = app.test_client()
    resp = client.get(f"/api/photos/{pid}/pipeline")
    assert resp.status_code == 200
    data = resp.get_json()
    assert "detections" in data
    assert len(data["detections"]) == 1
    assert data["detections"][0]["box_x"] == 0.1
    assert "predictions" in data
    assert len(data["predictions"]) == 1
    assert data["predictions"][0]["species"] == "Robin"
    assert data["predictions"][0]["box_x"] == 0.1
    # crop_box should be computed from primary detection
    assert "crop_box" in data


def test_api_photo_pipeline_omits_binary_embeddings(app_and_db):
    """Pipeline inspector payload must stay JSON-serializable after extract."""
    app, db = app_and_db
    pid = db.conn.execute("SELECT id FROM photos LIMIT 1").fetchone()["id"]
    db.conn.execute(
        "UPDATE photos SET dino_subject_embedding = ?, dino_global_embedding = ? "
        "WHERE id = ?",
        (b"\x00\x01subject", b"\x02\x03global", pid),
    )
    db.conn.commit()

    client = app.test_client()
    resp = client.get(f"/api/photos/{pid}/pipeline")
    assert resp.status_code == 200
    data = resp.get_json()
    assert "dino_subject_embedding" not in data
    assert "dino_global_embedding" not in data


def test_api_photo_pipeline_predictions_honor_threshold_and_fingerprint(app_and_db):
    """The pipeline-debug endpoint's `predictions` list must apply the same
    detector_confidence floor and fingerprint scoping as `detections`,
    so the two lists never disagree.
    """
    app, db = app_and_db
    pid = db.conn.execute("SELECT id FROM photos LIMIT 1").fetchone()["id"]
    # Two detections on the same photo:
    #   high-conf (0.9) — in active threshold → must surface predictions
    #   low-conf  (0.05) — below default 0.2 → must be hidden
    det_high, det_low = db.save_detections(pid, [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
         "confidence": 0.9, "category": "animal"},
        {"box": {"x": 0.6, "y": 0.6, "w": 0.3, "h": 0.3},
         "confidence": 0.05, "category": "animal"},
    ], detector_model="MDV6")
    # Stale and current fingerprints on the high-conf detection.
    db.conn.execute(
        "INSERT INTO predictions (detection_id, classifier_model, "
        "labels_fingerprint, species, confidence, created_at) "
        "VALUES (?, 'bioclip-2', 'fp-old', 'Finch', 0.95, '2026-01-01')",
        (det_high,),
    )
    db.conn.execute(
        "INSERT INTO predictions (detection_id, classifier_model, "
        "labels_fingerprint, species, confidence, created_at) "
        "VALUES (?, 'bioclip-2', 'fp-new', 'Robin', 0.85, '2026-04-24')",
        (det_high,),
    )
    # Prediction on the below-threshold detection — must NOT surface.
    db.conn.execute(
        "INSERT INTO predictions (detection_id, classifier_model, "
        "labels_fingerprint, species, confidence, created_at) "
        "VALUES (?, 'bioclip-2', 'fp-new', 'Sparrow', 0.9, '2026-04-24')",
        (det_low,),
    )
    db.conn.commit()

    client = app.test_client()
    resp = client.get(f"/api/photos/{pid}/pipeline")
    assert resp.status_code == 200
    data = resp.get_json()

    # Only the high-conf detection passes the threshold.
    assert len(data["detections"]) == 1, (
        f"detections list must apply detector_confidence floor; got "
        f"{len(data['detections'])}"
    )
    # Predictions must match: no stale-fingerprint species, no
    # below-threshold species.
    species = [p["species"] for p in data["predictions"]]
    assert species == ["Robin"], (
        f"predictions must match detections (one current-fingerprint "
        f"row, no stale, no below-threshold); got {species}"
    )
    diag = data["classification_diagnostics"]
    assert diag["raw_detection_count"] == 2
    assert diag["visible_detection_count"] == 1
    assert diag["hidden_detection_count"] == 1
    assert diag["current_prediction_count"] == 2
    assert diag["visible_prediction_count"] == 1
    assert diag["hidden_prediction_count"] == 1


def test_api_photo_pipeline_diagnoses_threshold_hidden_predictions(app_and_db):
    """A photo can be classified while the inspector has no visible predictions.

    Low-confidence detections are stored globally and may have predictions from
    the run that produced them. The visible inspector lists still honor the
    active detector threshold, but diagnostics must make the hidden state clear.
    """
    app, db = app_and_db
    pid = db.conn.execute("SELECT id FROM photos LIMIT 1").fetchone()["id"]
    det_id = db.save_detections(pid, [
        {"box": {"x": 0.6, "y": 0.6, "w": 0.3, "h": 0.3},
         "confidence": 0.05, "category": "animal"},
    ], detector_model="MDV6")[0]
    db.conn.execute(
        "INSERT INTO predictions (detection_id, classifier_model, "
        "labels_fingerprint, species, confidence, created_at) "
        "VALUES (?, 'bioclip-2', 'fp-new', 'Sparrow', 0.9, '2026-04-24')",
        (det_id,),
    )
    db.conn.execute(
        "INSERT INTO classifier_runs (detection_id, classifier_model, "
        "labels_fingerprint, prediction_count) VALUES (?, 'bioclip-2', 'fp-new', 1)",
        (det_id,),
    )
    db.conn.commit()

    client = app.test_client()
    resp = client.get(f"/api/photos/{pid}/pipeline")
    assert resp.status_code == 200
    data = resp.get_json()

    assert data["detections"] == []
    assert data["predictions"] == []
    diag = data["classification_diagnostics"]
    assert diag["raw_detection_count"] == 1
    assert diag["visible_detection_count"] == 0
    assert diag["hidden_detection_count"] == 1
    assert diag["current_prediction_count"] == 1
    assert diag["visible_prediction_count"] == 0
    assert diag["hidden_prediction_count"] == 1
    assert diag["classifier_run_count"] == 1
    assert diag["hidden_classifier_run_count"] == 1


def test_api_photo_pipeline_diagnoses_full_image_predictions(app_and_db):
    """Synthetic full-image anchors are not below-threshold detector boxes."""
    app, db = app_and_db
    pid = db.conn.execute("SELECT id FROM photos LIMIT 1").fetchone()["id"]
    db.update_workspace(db._active_workspace_id, config_overrides={
        "detector_confidence": 0.0,
    })
    det_id = db.save_detections(pid, [
        {"box": {"x": 0, "y": 0, "w": 1, "h": 1},
         "confidence": 0, "category": "animal"},
    ], detector_model="full-image")[0]
    db.conn.execute(
        "INSERT INTO predictions (detection_id, classifier_model, "
        "labels_fingerprint, species, confidence, created_at) "
        "VALUES (?, 'bioclip-2', 'fp-new', 'Robin', 0.8, '2026-04-24')",
        (det_id,),
    )
    db.conn.execute(
        "INSERT INTO classifier_runs (detection_id, classifier_model, "
        "labels_fingerprint, prediction_count) VALUES (?, 'bioclip-2', 'fp-new', 1)",
        (det_id,),
    )
    db.conn.commit()

    client = app.test_client()
    resp = client.get(f"/api/photos/{pid}/pipeline")
    assert resp.status_code == 200
    data = resp.get_json()

    assert data["detections"] == []
    assert data["predictions"] == []
    diag = data["classification_diagnostics"]
    assert diag["raw_detection_count"] == 0
    assert diag["hidden_detection_count"] == 0
    assert diag["current_prediction_count"] == 0
    assert diag["hidden_prediction_count"] == 0
    assert diag["classifier_run_count"] == 0
    assert diag["full_image_prediction_count"] == 1
    assert diag["full_image_classifier_run_count"] == 1


def test_compare_predictions_api_requires_collection(app_and_db):
    """GET /api/predictions/compare without collection_id returns 400."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get("/api/predictions/compare")
    assert resp.status_code == 400


def test_pipeline_has_model_checkboxes(app_and_db):
    """Pipeline page uses checkboxes for model selection, not a single select."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get('/pipeline')
    assert resp.status_code == 200
    assert b'model-checkbox' in resp.data
    assert b'id="cfgModel"' not in resp.data  # old single select removed


def test_pipeline_exposes_inline_label_download_modal(app_and_db):
    """Pipeline page lets users download species labels without leaving."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get('/pipeline')
    assert resp.status_code == 200
    html = resp.data.decode()
    assert 'openPipelineLabelsModal()' in html
    assert 'id="pipelineLabelsModal"' in html
    assert 'id="pipelineFetchLabelsBtn"' in html


def test_fetch_labels_returns_embedding_precompute_metadata_without_inline_compute(
    app_and_db, monkeypatch, tmp_path,
):
    """Species-list download should finish before label embeddings compute."""
    import classifier
    import labels
    import models

    labels_dir = tmp_path / "labels"
    monkeypatch.setattr(labels, "LABELS_DIR", str(labels_dir))
    monkeypatch.setattr(
        labels,
        "fetch_species_list",
        lambda *args, **kwargs: ["Blue Jay", "American Robin", "Blue Jay"],
    )
    monkeypatch.setattr(
        models,
        "get_active_model",
        lambda: {
            "id": "bioclip-2.5-vith14",
            "name": "BioCLIP-2.5",
            "downloaded": True,
            "model_type": "bioclip",
            "model_str": "hf-hub:imageomics/bioclip-2.5-vith14",
            "weights_path": str(tmp_path / "model"),
        },
    )
    monkeypatch.setattr(
        classifier,
        "_resolve_model_dir",
        lambda *args, **kwargs: str(tmp_path / "model"),
    )
    monkeypatch.setattr(
        classifier,
        "_embedding_cache_path",
        lambda *args, **kwargs: str(tmp_path / "missing-cache.npy"),
    )

    classifier_calls = []

    def fail_if_classifier_constructed(*args, **kwargs):
        classifier_calls.append((args, kwargs))
        raise AssertionError("fetch-labels must not compute embeddings inline")

    monkeypatch.setattr(classifier, "Classifier", fail_if_classifier_constructed)

    app, _ = app_and_db
    client = app.test_client()
    resp = client.post(
        "/api/jobs/fetch-labels",
        json={
            "place_id": 1,
            "place_name": "Test Place",
            "taxon_groups": ["birds"],
        },
    )

    assert resp.status_code == 200
    job = wait_for_job_via_client(client, resp.get_json()["job_id"])

    assert job["status"] == "completed"
    assert classifier_calls == []
    assert job["result"]["species_count"] == 2
    assert job["result"]["embedding_precompute"] == {
        "model_id": "bioclip-2.5-vith14",
        "model_name": "BioCLIP-2.5",
        "labels_file": job["result"]["labels_file"],
    }


def test_cull_page_uses_pipeline_controls(app_and_db):
    """Cull exposes the same threshold sliders as pipeline review."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get('/cull')
    assert resp.status_code == 200
    html = resp.data.decode()
    assert 'id="slRejectCrop"' in html
    assert 'id="slWTime"' in html
    assert 'id="slEncCut"' in html
    assert 'id="slBurstEmb"' in html
    assert "/api/pipeline/regroup-live" in html
    assert "/api/jobs/cull" not in html
    # Cull must send collection_id to the pipeline endpoints so scoring
    # is computed against the selected collection only. Without this,
    # KEEP/REVIEW/REJECT labels are influenced by photos outside the
    # collection and the client-only filter just hides them post-hoc.
    assert "collection_id" in html
    # Filtering via the API + per_page=999999 is a footgun — the
    # collection-photos endpoint clamps per_page to 500, so large
    # collections silently truncate. Server-side scoping replaces it.
    assert "per_page=999999" not in html
    # Pipeline encounters can serialize species as [name, confidence]; Cull
    # should group by the stable species name instead of the full tuple.
    assert "function speciesName" in html
    assert "Array.isArray(value)" in html


def test_static_vireo_utils_served(app_and_db):
    """vireo-utils.js is served from /static/."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get('/static/vireo-utils.js')
    assert resp.status_code == 200
    assert 'javascript' in resp.content_type
    body = resp.data.decode()
    assert 'function escapeHtml' in body
    assert 'function escapeAttr' in body


def test_pages_include_vireo_utils(app_and_db):
    """Every page includes vireo-utils.js via _navbar.html."""
    app, _ = app_and_db
    client = app.test_client()
    pages = ['/browse', '/lightroom', '/audit', '/logs',
             '/settings', '/storage', '/workspace', '/pipeline', '/dashboard',
             '/review', '/cull', '/variants', '/compare', '/map']
    for page in pages:
        resp = client.get(page)
        assert resp.status_code == 200, f"{page} returned {resp.status_code}"
        html = resp.data.decode()
        assert 'vireo-utils.js' in html, f"{page} missing vireo-utils.js script tag"


def test_map_page(app_and_db):
    """GET /map returns 200."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get('/map')
    assert resp.status_code == 200


def test_pages_no_inline_escapeHtml(app_and_db):
    """No page template should still define escapeHtml inline."""
    app, _ = app_and_db
    client = app.test_client()
    pages = ['/browse', '/lightroom', '/audit', '/logs',
             '/settings', '/storage', '/workspace', '/pipeline', '/dashboard',
             '/review', '/cull', '/variants', '/compare', '/map']
    for page in pages:
        resp = client.get(page)
        html = resp.data.decode()
        # The function should exist (via vireo-utils.js) but not be
        # defined inline in a <script> block on the page itself.
        # We check that "function escapeHtml" does NOT appear in the
        # page body outside of the vireo-utils.js src tag.
        # Simple heuristic: count occurrences — should be 0 in inline script.
        # The <script src="...vireo-utils.js"> tag won't contain the function text.
        assert html.count('function escapeHtml') == 0, \
            f"{page} still has inline escapeHtml definition"


def test_browse_calendar_day_sets_bare_date(app_and_db):
    """Heatmap day click must set #dateTo to a bare date.

    #dateTo is <input type="date">: assigning 'YYYY-MM-DDT23:59:59' is
    rejected and silently blanks the input, so the request went out with
    no upper bound. The backend pads bare dates to end-of-day
    (_inclusive_date_to), so the suffix is never needed client-side.
    """
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get('/browse')
    assert resp.status_code == 200
    assert "T23:59:59" not in resp.data.decode()


def test_health_endpoint(app_and_db):
    """GET /api/health returns 200 with status ok."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get("/api/health")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["status"] == "ok"


def test_shutdown_endpoint(app_and_db):
    """POST /api/shutdown returns 200 and signals shutdown."""
    from unittest.mock import MagicMock, patch

    app, _ = app_and_db
    client = app.test_client()
    # GET should not be allowed
    resp = client.get("/api/shutdown")
    assert resp.status_code == 405
    # POST without X-Vireo-Shutdown header is rejected (CSRF protection)
    resp = client.post("/api/shutdown")
    assert resp.status_code == 403
    # POST with header triggers shutdown (mock Timer so SIGTERM is never sent)
    mock_timer = MagicMock()
    with patch("threading.Timer", return_value=mock_timer) as mock_timer_cls:
        resp = client.post(
            "/api/shutdown", headers={"X-Vireo-Shutdown": "1"}
        )
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["status"] == "shutting_down"
        mock_timer_cls.assert_called_once()
        mock_timer.start.assert_called_once()


def test_pipeline_page_init_api(app_and_db):
    """GET /api/pipeline/page-init returns pipeline initialization data."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get('/api/pipeline/page-init')
    assert resp.status_code == 200
    data = resp.get_json()
    assert 'total_photos' in data
    assert 'pipeline_config' in data
    assert 'results' in data
    # Verify pipeline_config has expected keys
    pc = data['pipeline_config']
    assert 'sam2_variant' in pc
    assert 'dinov2_variant' in pc
    assert 'proxy_longest_edge' in pc
    # Pipeline page renders the eye-keypoints opt-in checkbox from this flag.
    assert 'eye_detect_enabled' in pc
    # total_photos should match our fixture data (3 photos)
    assert data['total_photos'] == 3


def test_pipeline_page_init_omits_recent_destinations(app_and_db):
    """recent_destinations left page-init with the Destination card: the
    process page no longer copies files anywhere, so leaking the import
    history here would just invite the UI to grow a destination again."""
    import config as cfg
    app, _ = app_and_db
    config = cfg.load()
    config.setdefault("ingest", {})["recent_destinations"] = ["/photos/out1", "/photos/out2"]
    cfg.save(config)
    with app.test_client() as c:
        resp = c.get("/api/pipeline/page-init")
        assert resp.status_code == 200
        data = resp.get_json()
        assert "recent_destinations" not in data


def test_templates_jinja_free_except_includes():
    """All .html templates must be free of Jinja2 syntax except {% include '...' %}."""
    import os
    import re

    templates_dir = os.path.join(os.path.dirname(__file__), '..', 'templates')
    templates_dir = os.path.normpath(templates_dir)

    # Patterns that match Jinja2 block tags and expression tags
    jinja_block_re = re.compile(r'\{%.*?%\}', re.DOTALL)
    jinja_expr_re = re.compile(r'\{\{.*?\}\}', re.DOTALL)
    # Allowed: {% include '...' %} or {% include "..." %}
    include_re = re.compile(r"\{%\s*include\s+['\"].*?['\"]\s*%\}")

    violations = []

    for fname in sorted(os.listdir(templates_dir)):
        if not fname.endswith('.html'):
            continue
        fpath = os.path.join(templates_dir, fname)
        with open(fpath, encoding='utf-8') as f:
            lines = f.readlines()
        for lineno, line in enumerate(lines, start=1):
            # Check for {{ ... }} expressions — never allowed
            for m in jinja_expr_re.finditer(line):
                violations.append(f"{fname}:{lineno}: {m.group().strip()}")
            # Check for {% ... %} blocks — only includes are allowed
            for m in jinja_block_re.finditer(line):
                if not include_re.fullmatch(m.group()):
                    violations.append(f"{fname}:{lineno}: {m.group().strip()}")

    assert violations == [], (
        "Jinja2 syntax found in templates (only {% include '...' %} is allowed):\n"
        + "\n".join(violations)
    )


def test_file_manager_labels_per_platform(monkeypatch):
    """Reveal/placeholder wording is OS-appropriate so Linux/Windows users
    don't see macOS-only 'Finder' terminology."""
    import app as app_module

    monkeypatch.setattr(app_module.sys, "platform", "linux")
    linux = app_module._file_manager_labels()
    assert linux["reveal"] == "Reveal in File Manager"
    assert linux["editor_placeholder"].startswith("/")
    assert "Applications" not in linux["editor_placeholder"]

    monkeypatch.setattr(app_module.sys, "platform", "darwin")
    mac = app_module._file_manager_labels()
    assert mac["reveal"] == "Reveal in Finder"
    assert mac["editor_placeholder"].endswith(".app")

    monkeypatch.setattr(app_module.sys, "platform", "win32")
    win = app_module._file_manager_labels()
    assert "Explorer" in win["reveal"]
    assert win["editor_placeholder"].endswith(".exe")


def test_config_defaults_js_exposes_platform_globals(app_and_db):
    """/config-defaults.js publishes the platform-aware globals the templates
    read (they are Jinja-free, so this is the only injection channel)."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get('/config-defaults.js')
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert 'window.VIREO_REVEAL_LABEL' in body
    assert 'window.VIREO_EDITOR_PATH_PLACEHOLDER' in body
    assert 'window.VIREO_PLATFORM' in body


def test_trash_via_finder_guarded_off_mac(monkeypatch):
    """The AppleScript Finder fallback only runs on macOS; elsewhere it raises
    instead of spawning a doomed osascript subprocess."""
    import app as app_module

    monkeypatch.setattr(app_module.sys, "platform", "linux")
    called = []
    monkeypatch.setattr(
        app_module.subprocess, "run",
        lambda *a, **k: called.append(a) or None,
    )
    try:
        app_module._trash_via_finder("/some/file.jpg")
        raised = False
    except OSError:
        raised = True
    assert raised, "expected OSError on non-macOS"
    assert called == [], "osascript must not be spawned off macOS"


def test_navbar_js_fallbacks_match_python_constants():
    """The hardcoded fallback lists in _navbar.html must mirror the
    canonical Python lists. The navbar's JS uses these fallbacks when
    /api/workspace/tabs fails — drift would mean a broken navbar in
    failure mode (e.g. a removed page still in the JS list).
    """
    import json
    import os
    import re

    from app import ALL_PAGES
    from db import DEFAULT_TABS

    template_path = os.path.normpath(
        os.path.join(os.path.dirname(__file__), '..', 'templates', '_navbar.html')
    )
    with open(template_path, encoding='utf-8') as f:
        text = f.read()

    tabs_match = re.search(
        r'window\.NAV_DEFAULT_TABS\s*=\s*(\[[^\]]*\])', text, re.DOTALL
    )
    pages_match = re.search(
        r'window\.NAV_ALL_PAGES\s*=\s*(\[.*?\n\];)', text, re.DOTALL
    )
    assert tabs_match, "window.NAV_DEFAULT_TABS not found in _navbar.html"
    assert pages_match, "window.NAV_ALL_PAGES not found in _navbar.html"

    # Coerce JS-ish list literals to JSON: single→double quotes, strip
    # trailing semicolon, quote bare object keys.
    def js_to_json(s):
        s = s.rstrip(';').strip()
        s = s.replace("'", '"')
        s = re.sub(r'(\b)(id|label|href|keywords)(\s*:)', r'\1"\2"\3', s)
        return s

    js_tabs = json.loads(js_to_json(tabs_match.group(1)))
    js_pages = json.loads(js_to_json(pages_match.group(1)))

    assert js_tabs == list(DEFAULT_TABS), (
        f"window.NAV_DEFAULT_TABS in _navbar.html drifted from db.DEFAULT_TABS.\n"
        f"  JS:     {js_tabs}\n"
        f"  Python: {list(DEFAULT_TABS)}"
    )
    assert js_pages == ALL_PAGES, (
        "window.NAV_ALL_PAGES in _navbar.html drifted from app.ALL_PAGES."
    )


def test_bottom_panel_has_history_tab(app_and_db):
    """The bottom panel includes a History tab."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get('/browse')
    html = resp.data.decode()
    assert "switchBpTab('history')" in html
    assert 'id="bpHistory"' in html


def test_text_search_requires_query(app_and_db):
    """Text search returns 400 when no query provided."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get("/api/photos/search")
    assert resp.status_code == 400


def test_text_search_no_active_model(app_and_db):
    """Text search returns empty results when no model is downloaded."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get("/api/photos/search?q=bird+in+flight")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["results"] == []
    assert data["total_matches"] == 0


def test_text_search_timm_model_returns_unsupported(app_and_db, monkeypatch):
    """Text search returns error when active model is timm (no CLIP embeddings)."""
    app, _ = app_and_db
    client = app.test_client()
    monkeypatch.setattr(
        "models.get_active_model",
        lambda: {
            "name": "iNat21 (EVA-02 Large)",
            "model_type": "timm",
            "model_str": "hf-hub:timm/eva02",
            "downloaded": True,
        },
    )
    resp = client.get("/api/photos/search?q=bird+on+water")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["results"] == []
    assert data["total_matches"] == 0
    # Should indicate text search is not supported for this model type
    assert data.get("reason") == "model_no_text_search"


def test_text_search_no_embeddings_returns_reason(app_and_db, monkeypatch):
    """Text search explains when no embeddings exist for the active model."""
    app, _ = app_and_db
    client = app.test_client()
    monkeypatch.setattr(
        "models.get_active_model",
        lambda: {
            "name": "BioCLIP-2",
            "model_type": "bioclip",
            "model_str": "hf-hub:imageomics/bioclip-2",
            "weights_path": "/fake/path",
            "downloaded": True,
        },
    )
    resp = client.get("/api/photos/search?q=bird+on+water")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["results"] == []
    assert data["total_matches"] == 0
    # Should indicate no embeddings exist for this model
    assert data.get("reason") == "no_embeddings"


def test_text_search_returns_ranked_enriched_results(app_and_db, monkeypatch):
    """Text search ranks by embedding similarity and returns browse card metadata."""
    import numpy as np

    app, db = app_and_db
    client = app.test_client()
    monkeypatch.setattr(
        "models.get_active_model",
        lambda: {
            "name": "BioCLIP-2",
            "model_type": "bioclip",
            "model_str": "hf-hub:imageomics/bioclip-2",
            "downloaded": True,
        },
    )
    monkeypatch.setattr(
        "text_encoder.encode_text",
        lambda query, model_str, pretrained_str=None: np.array([1.0, 0.0], dtype=np.float32),
    )

    rows = db.conn.execute(
        "SELECT id, filename FROM photos ORDER BY id"
    ).fetchall()
    by_name = {row["filename"]: row["id"] for row in rows}
    p1 = by_name["bird1.jpg"]
    p2 = by_name["bird2.jpg"]
    p3 = by_name["bird3.jpg"]
    for pid, emb in [
        (p1, [1.0, 0.0]),
        (p2, [0.6, 0.8]),
        (p3, [0.0, 1.0]),
    ]:
        db.upsert_photo_embedding(
            pid, "BioCLIP-2", np.array(emb, dtype=np.float32).tobytes()
        )
    db.save_detections(
        p1,
        [{"box": {"x": 0.1, "y": 0.2, "w": 0.3, "h": 0.4},
          "confidence": 0.91, "category": "bird"}],
        detector_model="test-detector",
    )
    species_id = db.add_keyword("Search Cardinal", is_species=True)
    db.tag_photo(p1, species_id)

    resp = client.get("/api/photos/search?q=bird&threshold=0.15")

    assert resp.status_code == 200
    data = resp.get_json()
    assert [r["photo"]["id"] for r in data["results"]] == [p1, p2]
    assert data["total_matches"] == 2
    first = data["results"][0]["photo"]
    assert "Search Cardinal" in first["species"]
    assert first["detections"][0]["category"] == "bird"


def test_text_search_applies_browse_scope_filters(app_and_db, monkeypatch):
    """Text search honors normal browse filters, collections, and visible folders."""
    import json

    import numpy as np

    app, db = app_and_db
    client = app.test_client()
    monkeypatch.setattr(
        "models.get_active_model",
        lambda: {
            "name": "BioCLIP-2",
            "model_type": "bioclip",
            "model_str": "hf-hub:imageomics/bioclip-2",
            "downloaded": True,
        },
    )
    monkeypatch.setattr(
        "text_encoder.encode_text",
        lambda query, model_str, pretrained_str=None: np.array([1.0, 0.0], dtype=np.float32),
    )

    rows = db.conn.execute(
        "SELECT id, filename FROM photos ORDER BY id"
    ).fetchall()
    by_name = {row["filename"]: row["id"] for row in rows}
    p1 = by_name["bird1.jpg"]
    p2 = by_name["bird2.jpg"]
    p3 = by_name["bird3.jpg"]
    missing_fid = db.add_folder("/photos/missing", name="missing")
    missing_pid = db.add_photo(
        folder_id=missing_fid,
        filename="missing.jpg",
        extension=".jpg",
        file_size=1,
        file_mtime=1.0,
        timestamp="2024-01-01T00:00:00",
    )
    db.conn.execute(
        "UPDATE folders SET status = 'missing' WHERE id = ?", (missing_fid,)
    )
    db.conn.commit()
    for pid, emb in [
        (p1, [1.0, 0.0]),
        (p2, [0.6, 0.8]),
        (p3, [0.0, 1.0]),
        (missing_pid, [2.0, 0.0]),
    ]:
        db.upsert_photo_embedding(
            pid, "BioCLIP-2", np.array(emb, dtype=np.float32).tobytes()
        )

    rating_resp = client.get("/api/photos/search?q=bird&threshold=-1&rating_min=5")
    assert rating_resp.status_code == 200
    assert [r["photo"]["id"] for r in rating_resp.get_json()["results"]] == [p3]

    cid = db.add_collection(
        "Search Scope",
        json.dumps([{"field": "photo_ids", "value": [p2, p3, missing_pid]}]),
    )
    collection_resp = client.get(
        f"/api/photos/search?q=bird&threshold=-1&collection_id={cid}"
    )
    assert collection_resp.status_code == 200
    assert [r["photo"]["id"] for r in collection_resp.get_json()["results"]] == [p2, p3]


def test_settings_has_edit_history_config(app_and_db):
    """Settings page includes the max_edit_history config field."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get('/settings')
    html = resp.data.decode()
    assert 'max_edit_history' in html


def test_pipeline_detach_burst(app_and_db):
    """POST /api/pipeline/detach-burst moves a burst to a new encounter."""
    import json as _json
    app, db = app_and_db
    client = app.test_client()

    # Create fake pipeline results in cache
    cache_dir = os.path.dirname(app.config["DB_PATH"])
    ws_id = db._active_workspace_id
    results = {
        "encounters": [
            {
                "species": ["Robin", 0.9],
                "confirmed_species": None,
                "species_predictions": [{"species": "Robin", "count": 3, "models": [{"model": "m1", "confidence": 0.9, "photo_count": 3}]}],
                "species_confirmed": False,
                "photo_count": 3,
                "burst_count": 2,
                "time_range": [None, None],
                "photo_ids": [1, 2, 3],
                "bursts": [
                    {"photo_ids": [1, 2], "species_predictions": [], "species_override": None},
                    {"photo_ids": [3], "species_predictions": [], "species_override": None},
                ],
            }
        ],
        "photos": [
            {"id": 1, "label": "KEEP", "filename": "a.jpg", "species_top5": [["Robin", 0.9, "m1"]]},
            {"id": 2, "label": "KEEP", "filename": "b.jpg", "species_top5": [["Robin", 0.85, "m1"]]},
            {"id": 3, "label": "REVIEW", "filename": "c.jpg", "species_top5": [["Eagle", 0.8, "m1"]]},
        ],
        "summary": {"total_photos": 3, "encounter_count": 1, "burst_count": 2,
                     "keep_count": 2, "review_count": 1, "reject_count": 0, "rarity_protected": 0},
    }
    path = os.path.join(cache_dir, f"pipeline_results_ws{ws_id}.json")
    with open(path, "w") as f:
        _json.dump(results, f)

    resp = client.post("/api/pipeline/detach-burst",
                       json={"encounter_index": 0, "burst_index": 1})
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["ok"] is True
    # Original encounter should now have 1 burst, new encounter created
    assert len(data["encounters"]) == 2
    assert len(data["encounters"][0]["bursts"]) == 1
    assert data["encounters"][1]["photo_ids"] == [3]
    assert data["encounters"][0]["species"] == ["Robin", 0.875]
    assert data["encounters"][1]["species"] == ["Eagle", 0.8]
    assert data["encounters"][1]["confirmed_species"] is None
    assert data["encounters"][1]["species_confirmed"] is False
    # Remaining encounter predictions should only reflect photos 1,2
    remaining_species = [sp["species"] for sp in data["encounters"][0]["species_predictions"]]
    assert "Robin" in remaining_species
    assert "Eagle" not in remaining_species
    # New encounter predictions should reflect photo 3
    new_species = [sp["species"] for sp in data["encounters"][1]["species_predictions"]]
    assert "Eagle" in new_species


def test_pipeline_detach_burst_computes_time_ranges(app_and_db):
    """detach-burst must compute time_range from photo timestamps for both the
    new encounter and the shrunken source encounter. A [None, None] range would
    sort detached encounters to the extremes under the review page's time sorts
    and render a blank time label in the encounter header."""
    import json as _json
    app, db = app_and_db
    client = app.test_client()

    cache_dir = os.path.dirname(app.config["DB_PATH"])
    ws_id = db._active_workspace_id
    results = {
        "encounters": [
            {
                "species": ["Robin", 0.9],
                "confirmed_species": None,
                "species_predictions": [],
                "species_confirmed": False,
                "photo_count": 3,
                "burst_count": 2,
                # Deliberately stale/missing — the handler must recompute it.
                "time_range": [None, None],
                "photo_ids": [1, 2, 3],
                "bursts": [
                    {"photo_ids": [1, 2], "species_predictions": [], "species_override": None},
                    {"photo_ids": [3], "species_predictions": [], "species_override": None},
                ],
            }
        ],
        "photos": [
            {"id": 1, "label": "KEEP", "filename": "a.jpg", "timestamp": "2024-01-01T10:00:00", "species_top5": []},
            {"id": 2, "label": "KEEP", "filename": "b.jpg", "timestamp": "2024-01-01T10:00:05", "species_top5": []},
            {"id": 3, "label": "REVIEW", "filename": "c.jpg", "timestamp": "2024-01-01T10:05:00", "species_top5": []},
        ],
        "summary": {"total_photos": 3, "encounter_count": 1, "burst_count": 2,
                     "keep_count": 2, "review_count": 1, "reject_count": 0, "rarity_protected": 0},
    }
    path = os.path.join(cache_dir, f"pipeline_results_ws{ws_id}.json")
    with open(path, "w") as f:
        _json.dump(results, f)

    resp = client.post("/api/pipeline/detach-burst",
                       json={"encounter_index": 0, "burst_index": 1})
    assert resp.status_code == 200
    data = resp.get_json()
    # Source encounter range recomputed to the surviving photos (1, 2).
    assert data["encounters"][0]["time_range"] == ["2024-01-01T10:00:00", "2024-01-01T10:00:05"]
    # New encounter range computed from the detached photo (3), not [None, None].
    assert data["encounters"][1]["photo_ids"] == [3]
    assert data["encounters"][1]["time_range"] == ["2024-01-01T10:05:00", "2024-01-01T10:05:00"]


def test_pipeline_detach_photo(app_and_db):
    """POST /api/pipeline/detach-photo moves a photo to a new burst."""
    import json as _json
    app, db = app_and_db
    client = app.test_client()

    cache_dir = os.path.dirname(app.config["DB_PATH"])
    ws_id = db._active_workspace_id
    results = {
        "encounters": [
            {
                "species": ["Robin", 0.9],
                "confirmed_species": None,
                "species_predictions": [],
                "species_confirmed": False,
                "photo_count": 3,
                "burst_count": 1,
                "time_range": [None, None],
                "photo_ids": [1, 2, 3],
                "bursts": [
                    {"photo_ids": [1, 2, 3], "species_predictions": [], "species_override": None},
                ],
            }
        ],
        "photos": [
            {"id": 1, "label": "KEEP", "filename": "a.jpg", "species_top5": [["Robin", 0.9, "m1"]]},
            {"id": 2, "label": "KEEP", "filename": "b.jpg", "species_top5": [["Robin", 0.85, "m1"]]},
            {"id": 3, "label": "REVIEW", "filename": "c.jpg", "species_top5": [["Eagle", 0.8, "m1"]]},
        ],
        "summary": {"total_photos": 3, "encounter_count": 1, "burst_count": 1,
                     "keep_count": 2, "review_count": 1, "reject_count": 0, "rarity_protected": 0},
    }
    path = os.path.join(cache_dir, f"pipeline_results_ws{ws_id}.json")
    with open(path, "w") as f:
        _json.dump(results, f)

    resp = client.post("/api/pipeline/detach-photo",
                       json={"encounter_index": 0, "burst_index": 0, "photo_id": 3})
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["ok"] is True
    # Original burst should have 2 photos, new burst with 1 photo
    enc = data["encounters"][0]
    assert len(enc["bursts"]) == 2
    assert enc["bursts"][0]["photo_ids"] == [1, 2]
    assert enc["bursts"][1]["photo_ids"] == [3]
    # Source burst predictions should only reflect photos 1,2
    src_species = [sp["species"] for sp in enc["bursts"][0]["species_predictions"]]
    assert "Robin" in src_species
    assert "Eagle" not in src_species
    # New burst predictions should reflect photo 3
    new_species = [sp["species"] for sp in enc["bursts"][1]["species_predictions"]]
    assert "Eagle" in new_species
    assert enc["bursts"][1]["species_override"] == {
        "species": "Eagle",
        "confirmed": False,
    }


def test_pipeline_detach_photo_confidence_weighted_override(app_and_db):
    """The derived species_override on a detached single-photo burst uses the
    same confidence-weighted vote as encounter_species_label — not the
    prediction count. For a photo whose top-5 is [A .90, B .44, B .44] the
    override must be A (weight 0.90 > 0.88), even though B appears twice.
    This is the invariant the client-side detach mirror
    (candidateSpeciesOverrideFromPhotos in pipeline_review.html) has to
    match; a divergence would let the local save-cache path persist a
    different unconfirmed override than the server detach-photo endpoint."""
    import json as _json
    app, db = app_and_db
    client = app.test_client()

    cache_dir = os.path.dirname(app.config["DB_PATH"])
    ws_id = db._active_workspace_id
    results = {
        "encounters": [
            {
                "species": ["Robin", 0.9],
                "confirmed_species": None,
                "species_predictions": [],
                "species_confirmed": False,
                "photo_count": 2,
                "burst_count": 1,
                "time_range": [None, None],
                "photo_ids": [1, 2],
                "bursts": [
                    {"photo_ids": [1, 2], "species_predictions": [], "species_override": None},
                ],
            }
        ],
        "photos": [
            {"id": 1, "label": "KEEP", "filename": "a.jpg",
             "species_top5": [["Robin", 0.9, "m1"]]},
            {"id": 2, "label": "REVIEW", "filename": "b.jpg",
             "species_top5": [
                 ["Alpha", 0.90, "m1"],
                 ["Beta", 0.44, "m1"],
                 ["Beta", 0.44, "m2"],
             ]},
        ],
        "summary": {"total_photos": 2, "encounter_count": 1, "burst_count": 1,
                     "keep_count": 1, "review_count": 1, "reject_count": 0, "rarity_protected": 0},
    }
    path = os.path.join(cache_dir, f"pipeline_results_ws{ws_id}.json")
    with open(path, "w") as f:
        _json.dump(results, f)

    resp = client.post("/api/pipeline/detach-photo",
                       json={"encounter_index": 0, "burst_index": 0, "photo_id": 2})
    assert resp.status_code == 200
    data = resp.get_json()
    enc = data["encounters"][0]
    detached = enc["bursts"][1]
    assert detached["photo_ids"] == [2]
    assert detached["species_override"] == {"species": "Alpha", "confirmed": False}


def test_pipeline_detach_photo_partial_confirm_leaves_override_null(app_and_db):
    """When the source encounter is in the mixed/partially-confirmed state
    (species_confirmed=False but confirmed_species set — e.g. some photos
    confirmed as species A, others still unconfirmed), detaching a photo
    must NOT stamp the new burst with an unconfirmed classifier-guess
    override. The confirm endpoint reads species_override.species without
    checking the confirmed flag, so a guess override there would be picked
    up as previous_species on the next burst confirm — the code would then
    try to untag the guess instead of the actual prior species, leaving
    the photo with both the old and new species keywords. Leaving the
    override empty makes the confirm endpoint fall back to
    enc.confirmed_species as previous_species instead.
    """
    import json as _json
    app, db = app_and_db
    client = app.test_client()

    cache_dir = os.path.dirname(app.config["DB_PATH"])
    ws_id = db._active_workspace_id
    results = {
        "encounters": [
            {
                "species": ["Robin", 0.9],
                # Partial-confirm state: dominant prior species is known
                # but not all photos agree, so species_confirmed is False.
                "confirmed_species": "Robin",
                "species_predictions": [],
                "species_confirmed": False,
                "photo_count": 3,
                "burst_count": 1,
                "time_range": [None, None],
                "photo_ids": [1, 2, 3],
                "bursts": [
                    {"photo_ids": [1, 2, 3], "species_predictions": [],
                     "species_override": None},
                ],
            }
        ],
        "photos": [
            {"id": 1, "label": "KEEP", "filename": "a.jpg",
             "species_top5": [["Robin", 0.9, "m1"]]},
            {"id": 2, "label": "KEEP", "filename": "b.jpg",
             "species_top5": [["Robin", 0.85, "m1"]]},
            {"id": 3, "label": "REVIEW", "filename": "c.jpg",
             "species_top5": [["Eagle", 0.8, "m1"]]},
        ],
        "summary": {"total_photos": 3, "encounter_count": 1, "burst_count": 1,
                     "keep_count": 2, "review_count": 1, "reject_count": 0,
                     "rarity_protected": 0},
    }
    path = os.path.join(cache_dir, f"pipeline_results_ws{ws_id}.json")
    with open(path, "w") as f:
        _json.dump(results, f)

    resp = client.post("/api/pipeline/detach-photo",
                       json={"encounter_index": 0, "burst_index": 0,
                             "photo_id": 3})
    assert resp.status_code == 200
    data = resp.get_json()
    enc = data["encounters"][0]
    detached = enc["bursts"][1]
    assert detached["photo_ids"] == [3]
    # Critically: no unconfirmed "Eagle" guess should be stamped here.
    # Otherwise the confirm endpoint would treat "Eagle" as previous_species
    # instead of the real prior confirmed_species "Robin".
    assert detached["species_override"] is None


def test_pipeline_detach_burst_predictionless_does_not_inherit_parent_species(app_and_db):
    """When the detached burst's photos have no species predictions,
    ``encounter_species_label`` returns ``(None, 0.0)``. The new encounter
    must remain unlabeled rather than inheriting the source encounter's
    label — the parent species almost certainly came from the sibling
    burst we just left behind, so advertising it as a one-click candidate
    on the unrelated detached photos would reintroduce the stale-inherited-
    label bug this change removes. Mirror check for the shrunken source
    encounter when the only predicted burst is detached away from
    unclassified siblings.
    """
    import json as _json
    app, db = app_and_db
    client = app.test_client()

    cache_dir = os.path.dirname(app.config["DB_PATH"])
    ws_id = db._active_workspace_id
    results = {
        "encounters": [
            {
                # Encounter-level label was inherited from burst [1,2] alone;
                # burst [3] has no predictions of its own.
                "species": ["Robin", 0.9],
                "confirmed_species": None,
                "species_predictions": [],
                "species_confirmed": False,
                "photo_count": 3,
                "burst_count": 2,
                "time_range": [None, None],
                "photo_ids": [1, 2, 3],
                "bursts": [
                    {"photo_ids": [1, 2], "species_predictions": [], "species_override": None},
                    {"photo_ids": [3], "species_predictions": [], "species_override": None},
                ],
            }
        ],
        "photos": [
            {"id": 1, "label": "KEEP", "filename": "a.jpg",
             "species_top5": [["Robin", 0.9, "m1"]]},
            {"id": 2, "label": "KEEP", "filename": "b.jpg",
             "species_top5": [["Robin", 0.85, "m1"]]},
            # Photo 3 has no predictions — the burst we detach is unclassified.
            {"id": 3, "label": "REVIEW", "filename": "c.jpg",
             "species_top5": []},
        ],
        "summary": {"total_photos": 3, "encounter_count": 1, "burst_count": 2,
                     "keep_count": 2, "review_count": 1, "reject_count": 0, "rarity_protected": 0},
    }
    path = os.path.join(cache_dir, f"pipeline_results_ws{ws_id}.json")
    with open(path, "w") as f:
        _json.dump(results, f)

    resp = client.post("/api/pipeline/detach-burst",
                       json={"encounter_index": 0, "burst_index": 1})
    assert resp.status_code == 200
    data = resp.get_json()
    # Sanity: two encounters, detached photo is on the new one.
    assert len(data["encounters"]) == 2
    assert data["encounters"][1]["photo_ids"] == [3]
    # The detached encounter's photos have no predictions of their own,
    # so its species must be [None, 0.0] — not the parent's Robin label.
    assert data["encounters"][1]["species"] == [None, 0.0]

    # Now the reverse: detach the only predicted burst and leave an
    # encounter of unclassified photos behind. The remaining encounter
    # must not keep the pre-detach Robin label.
    with open(path, "w") as f:
        _json.dump(results, f)
    resp = client.post("/api/pipeline/detach-burst",
                       json={"encounter_index": 0, "burst_index": 0})
    assert resp.status_code == 200
    data = resp.get_json()
    # Source encounter now holds only photo 3 (no predictions).
    assert data["encounters"][0]["photo_ids"] == [3]
    assert data["encounters"][0]["species"] == [None, 0.0]
    # And the detached burst (photos 1, 2) keeps its own Robin label.
    assert data["encounters"][1]["photo_ids"] == [1, 2]
    assert data["encounters"][1]["species"][0] == "Robin"


def test_pipeline_detach_burst_clears_stale_trace(app_and_db):
    """detach-burst must drop the source encounter's per-pair trace because
    pairs involving the detached photos are no longer present in the
    encounter — leaving the old trace would surface stale decisions in the
    review sidebar."""
    import json as _json
    app, db = app_and_db
    client = app.test_client()

    cache_dir = os.path.dirname(app.config["DB_PATH"])
    ws_id = db._active_workspace_id
    results = {
        "encounters": [
            {
                "species": ["Robin", 0.9],
                "confirmed_species": None,
                "species_predictions": [],
                "species_confirmed": False,
                "photo_count": 3,
                "burst_count": 2,
                "time_range": [None, None],
                "photo_ids": [1, 2, 3],
                "trace": [
                    {"i": 0, "j": 1, "decision": "keep", "score": 0.7},
                    {"i": 1, "j": 2, "decision": "keep", "score": 0.6},
                ],
                "bursts": [
                    {"photo_ids": [1, 2], "species_predictions": [], "species_override": None},
                    {"photo_ids": [3], "species_predictions": [], "species_override": None},
                ],
            }
        ],
        "photos": [
            {"id": 1, "label": "KEEP", "filename": "a.jpg", "species_top5": [["Robin", 0.9, "m1"]]},
            {"id": 2, "label": "KEEP", "filename": "b.jpg", "species_top5": [["Robin", 0.85, "m1"]]},
            {"id": 3, "label": "REVIEW", "filename": "c.jpg", "species_top5": [["Eagle", 0.8, "m1"]]},
        ],
        "summary": {"total_photos": 3, "encounter_count": 1, "burst_count": 2,
                     "keep_count": 2, "review_count": 1, "reject_count": 0, "rarity_protected": 0},
    }
    path = os.path.join(cache_dir, f"pipeline_results_ws{ws_id}.json")
    with open(path, "w") as f:
        _json.dump(results, f)

    resp = client.post("/api/pipeline/detach-burst",
                       json={"encounter_index": 0, "burst_index": 1})
    assert resp.status_code == 200
    data = resp.get_json()
    # Source encounter should no longer carry the pre-detach trace.
    assert "trace" not in data["encounters"][0]
    # New encounter created from detached burst has no trace either.
    assert "trace" not in data["encounters"][1]


def test_pipeline_detach_photo_preserves_trace(app_and_db):
    """detach-photo only restructures bursts within the encounter — the
    encounter's photo set and ordering is unchanged, so the per-pair trace
    (which describes adjacent-photo decisions) remains valid and should
    not be discarded."""
    import json as _json
    app, db = app_and_db
    client = app.test_client()

    cache_dir = os.path.dirname(app.config["DB_PATH"])
    ws_id = db._active_workspace_id
    trace = [
        {"i": 0, "j": 1, "decision": "keep", "score": 0.7},
        {"i": 1, "j": 2, "decision": "keep", "score": 0.6},
    ]
    results = {
        "encounters": [
            {
                "species": ["Robin", 0.9],
                "confirmed_species": None,
                "species_predictions": [],
                "species_confirmed": False,
                "photo_count": 3,
                "burst_count": 1,
                "time_range": [None, None],
                "photo_ids": [1, 2, 3],
                "trace": trace,
                "bursts": [
                    {"photo_ids": [1, 2, 3], "species_predictions": [], "species_override": None},
                ],
            }
        ],
        "photos": [
            {"id": 1, "label": "KEEP", "filename": "a.jpg", "species_top5": [["Robin", 0.9, "m1"]]},
            {"id": 2, "label": "KEEP", "filename": "b.jpg", "species_top5": [["Robin", 0.85, "m1"]]},
            {"id": 3, "label": "REVIEW", "filename": "c.jpg", "species_top5": [["Eagle", 0.8, "m1"]]},
        ],
        "summary": {"total_photos": 3, "encounter_count": 1, "burst_count": 1,
                     "keep_count": 2, "review_count": 1, "reject_count": 0, "rarity_protected": 0},
    }
    path = os.path.join(cache_dir, f"pipeline_results_ws{ws_id}.json")
    with open(path, "w") as f:
        _json.dump(results, f)

    resp = client.post("/api/pipeline/detach-photo",
                       json={"encounter_index": 0, "burst_index": 0, "photo_id": 3})
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["encounters"][0]["trace"] == trace


def test_encounter_species_auto_detaches_mixed_burst(app_and_db):
    """Confirming a burst to a species different from its encounter auto-detaches it."""
    import json as _json
    app, db = app_and_db
    client = app.test_client()

    cache_dir = os.path.dirname(app.config["DB_PATH"])
    ws_id = db._active_workspace_id
    results = {
        "encounters": [
            {
                "species": ["Bald Eagle", 0.9],
                "confirmed_species": None,
                "species_predictions": [{"species": "Bald Eagle", "count": 3, "models": []}],
                "species_confirmed": False,
                "photo_count": 3,
                "burst_count": 2,
                "time_range": ["2024-06-10T09:00:00", "2024-06-10T09:05:00"],
                "photo_ids": [1, 2, 3],
                "bursts": [
                    {"photo_ids": [1, 2], "species_predictions": [], "species_override": None},
                    {"photo_ids": [3], "species_predictions": [], "species_override": None},
                ],
            }
        ],
        "photos": [
            {"id": 1, "label": "KEEP", "filename": "a.jpg", "timestamp": "2024-06-10T09:00:00", "species_top5": [["Bald Eagle", 0.9, "m1"]]},
            {"id": 2, "label": "KEEP", "filename": "b.jpg", "timestamp": "2024-06-10T09:00:02", "species_top5": [["Bald Eagle", 0.9, "m1"]]},
            {"id": 3, "label": "REVIEW", "filename": "c.jpg", "timestamp": "2024-06-10T09:05:00", "species_top5": [["Golden Eagle", 0.6, "m1"]]},
        ],
        "summary": {"total_photos": 3, "encounter_count": 1, "burst_count": 2,
                     "keep_count": 2, "review_count": 1, "reject_count": 0, "rarity_protected": 0},
    }
    path = os.path.join(cache_dir, f"pipeline_results_ws{ws_id}.json")
    with open(path, "w") as f:
        _json.dump(results, f)

    # Confirm burst 1 (photo 3) as Golden Eagle — differs from encounter's Bald Eagle
    resp = client.post("/api/encounters/species",
                       json={"species": "Golden Eagle", "photo_ids": [3], "burst_index": 1})
    assert resp.status_code == 200

    # Response must include updated encounters so the client can refresh its
    # local state and avoid overwriting the detach via a later save-cache POST.
    body = resp.get_json()
    assert "encounters" in body
    assert "summary" in body
    assert len(body["encounters"]) == 2

    with open(path) as f:
        updated = _json.load(f)
    encounters = updated["encounters"]
    # Original encounter should no longer contain burst with photo 3
    assert len(encounters) == 2
    bald_enc = next(e for e in encounters if 1 in e["photo_ids"])
    eagle_enc = next(e for e in encounters if 3 in e["photo_ids"])
    assert bald_enc is not eagle_enc
    assert bald_enc["photo_ids"] == [1, 2]
    assert eagle_enc["photo_ids"] == [3]
    assert eagle_enc["species_confirmed"] is True
    assert eagle_enc["confirmed_species"] == "Golden Eagle"


def test_encounter_species_auto_detach_clears_stale_trace(app_and_db):
    """Auto-detach mutates encounter photo_ids; the trace (keyed to original
    composition) must be dropped so the algorithm-trace panel doesn't render
    pair indices that no longer match the post-detach photo set."""
    import json as _json
    app, db = app_and_db
    client = app.test_client()

    cache_dir = os.path.dirname(app.config["DB_PATH"])
    ws_id = db._active_workspace_id
    results = {
        "encounters": [
            {
                "species": ["Bald Eagle", 0.9],
                "confirmed_species": None,
                "species_predictions": [{"species": "Bald Eagle", "count": 3, "models": []}],
                "species_confirmed": False,
                "photo_count": 3,
                "burst_count": 2,
                "time_range": ["2024-06-10T09:00:00", "2024-06-10T09:05:00"],
                "photo_ids": [1, 2, 3],
                "bursts": [
                    {"photo_ids": [1, 2], "species_predictions": [], "species_override": None},
                    {"photo_ids": [3], "species_predictions": [], "species_override": None},
                ],
                # Pre-existing trace from the original 3-photo grouping.
                "trace": [
                    {"pair_index": 0, "score": 0.8, "decision": "kept", "components": {},
                     "thresholds": {}, "dt_seconds": 2.0},
                    {"pair_index": 1, "score": 0.5, "decision": "kept", "components": {},
                     "thresholds": {}, "dt_seconds": 298.0},
                ],
            }
        ],
        "photos": [
            {"id": 1, "label": "KEEP", "filename": "a.jpg", "timestamp": "2024-06-10T09:00:00", "species_top5": [["Bald Eagle", 0.9, "m1"]]},
            {"id": 2, "label": "KEEP", "filename": "b.jpg", "timestamp": "2024-06-10T09:00:02", "species_top5": [["Bald Eagle", 0.9, "m1"]]},
            {"id": 3, "label": "REVIEW", "filename": "c.jpg", "timestamp": "2024-06-10T09:05:00", "species_top5": [["Golden Eagle", 0.6, "m1"]]},
        ],
        "summary": {"total_photos": 3, "encounter_count": 1, "burst_count": 2,
                     "keep_count": 2, "review_count": 1, "reject_count": 0, "rarity_protected": 0},
    }
    path = os.path.join(cache_dir, f"pipeline_results_ws{ws_id}.json")
    with open(path, "w") as f:
        _json.dump(results, f)

    resp = client.post("/api/encounters/species",
                       json={"species": "Golden Eagle", "photo_ids": [3], "burst_index": 1})
    assert resp.status_code == 200
    body = resp.get_json()

    bald_enc = next(e for e in body["encounters"] if 1 in e["photo_ids"])
    golden_enc = next(e for e in body["encounters"] if 3 in e["photo_ids"])
    # The source encounter had its composition changed — trace must be gone.
    assert "trace" not in bald_enc, "auto-detach left stale trace on source encounter"
    # The new encounter created from the detached burst was never grouped, so
    # it has no trace either (correct — would be misleading otherwise).
    assert "trace" not in golden_enc


def test_encounter_species_confirm_single_burst_does_not_detach(app_and_db):
    """Confirming the only burst in an encounter does not detach (nothing to split from)."""
    import json as _json
    app, db = app_and_db
    client = app.test_client()

    cache_dir = os.path.dirname(app.config["DB_PATH"])
    ws_id = db._active_workspace_id
    results = {
        "encounters": [
            {
                "species": ["Bald Eagle", 0.9],
                "confirmed_species": None,
                "species_predictions": [],
                "species_confirmed": False,
                "photo_count": 2,
                "burst_count": 1,
                "time_range": ["2024-06-10T09:00:00", "2024-06-10T09:00:02"],
                "photo_ids": [1, 2],
                "bursts": [
                    {"photo_ids": [1, 2], "species_predictions": [], "species_override": None},
                ],
            }
        ],
        "photos": [
            {"id": 1, "label": "KEEP", "filename": "a.jpg", "timestamp": "2024-06-10T09:00:00"},
            {"id": 2, "label": "KEEP", "filename": "b.jpg", "timestamp": "2024-06-10T09:00:02"},
        ],
        "summary": {"total_photos": 2, "encounter_count": 1, "burst_count": 1,
                     "keep_count": 2, "review_count": 0, "reject_count": 0, "rarity_protected": 0},
    }
    path = os.path.join(cache_dir, f"pipeline_results_ws{ws_id}.json")
    with open(path, "w") as f:
        _json.dump(results, f)

    resp = client.post("/api/encounters/species",
                       json={"species": "Golden Eagle", "photo_ids": [1, 2], "burst_index": 0})
    assert resp.status_code == 200

    with open(path) as f:
        updated = _json.load(f)
    # Still one encounter, burst stays put, override recorded
    assert len(updated["encounters"]) == 1
    enc = updated["encounters"][0]
    assert len(enc["bursts"]) == 1
    assert enc["bursts"][0]["species_override"] == {"species": "Golden Eagle", "confirmed": True}


def test_encounter_species_burst_confirm_does_not_detach_on_variant_spelling(app_and_db):
    """Burst confirm must compare `enc_species` against the submitted species
    by keyword_match_key: a cached pre-normalization spelling of the same
    species (e.g. legacy `‘Apapane` from before the migration ran vs the
    stored/submitted `Apapane`) is the same species and must not trigger
    _auto_detach_burst_for_species. A raw `!=` compare would treat them as
    different and split the burst out even though nothing actually changed.
    """
    import json as _json
    app, db = app_and_db
    client = app.test_client()

    cache_dir = os.path.dirname(app.config["DB_PATH"])
    ws_id = db._active_workspace_id
    # Encounter has 2 bursts; confirmed_species carries the legacy quoted
    # spelling. The clean form is what the user is submitting.
    results = {
        "encounters": [
            {
                "species": ["Apapane", 0.9],
                "confirmed_species": "‘Apapane",
                "species_predictions": [],
                "species_confirmed": True,
                "photo_count": 2,
                "burst_count": 2,
                "time_range": ["2024-06-10T09:00:00", "2024-06-10T09:00:02"],
                "photo_ids": [1, 2],
                "bursts": [
                    {"photo_ids": [1], "species_predictions": [], "species_override": None},
                    {"photo_ids": [2], "species_predictions": [], "species_override": None},
                ],
            }
        ],
        "photos": [
            {"id": 1, "label": "KEEP", "filename": "a.jpg", "timestamp": "2024-06-10T09:00:00"},
            {"id": 2, "label": "KEEP", "filename": "b.jpg", "timestamp": "2024-06-10T09:00:02"},
        ],
        "summary": {"total_photos": 2, "encounter_count": 1, "burst_count": 2,
                     "keep_count": 2, "review_count": 0, "reject_count": 0, "rarity_protected": 0},
    }
    path = os.path.join(cache_dir, f"pipeline_results_ws{ws_id}.json")
    with open(path, "w") as f:
        _json.dump(results, f)

    resp = client.post("/api/encounters/species",
                       json={"species": "Apapane", "photo_ids": [1], "burst_index": 0})
    assert resp.status_code == 200

    with open(path) as f:
        updated = _json.load(f)
    # Same species (variant-only difference in confirmed_species) — burst
    # must NOT be split off. Still exactly one encounter with both bursts.
    assert len(updated["encounters"]) == 1
    assert len(updated["encounters"][0]["bursts"]) == 2


def test_encounter_species_detach_merges_into_adjacent_encounter(app_and_db):
    """Detaching a second burst merges it into an adjacent encounter with matching confirmed species."""
    import json as _json
    app, db = app_and_db
    client = app.test_client()

    cache_dir = os.path.dirname(app.config["DB_PATH"])
    ws_id = db._active_workspace_id
    # Original encounter has 3 bursts, all "Bald Eagle" predictions.
    # After first burst confirmed Golden Eagle and auto-detached, confirming another
    # burst to Golden Eagle should merge into the detached encounter (adjacent in time).
    results = {
        "encounters": [
            {
                "species": ["Bald Eagle", 0.9],
                "confirmed_species": None,
                "species_predictions": [],
                "species_confirmed": False,
                "photo_count": 3,
                "burst_count": 3,
                "time_range": ["2024-06-10T09:00:00", "2024-06-10T09:10:00"],
                "photo_ids": [1, 2, 3],
                "bursts": [
                    {"photo_ids": [1], "species_predictions": [], "species_override": None},
                    {"photo_ids": [2], "species_predictions": [], "species_override": None},
                    {"photo_ids": [3], "species_predictions": [], "species_override": None},
                ],
            }
        ],
        "photos": [
            {"id": 1, "label": "KEEP", "filename": "a.jpg", "timestamp": "2024-06-10T09:00:00"},
            {"id": 2, "label": "KEEP", "filename": "b.jpg", "timestamp": "2024-06-10T09:05:00"},
            {"id": 3, "label": "KEEP", "filename": "c.jpg", "timestamp": "2024-06-10T09:10:00"},
        ],
        "summary": {"total_photos": 3, "encounter_count": 1, "burst_count": 3,
                     "keep_count": 3, "review_count": 0, "reject_count": 0, "rarity_protected": 0},
    }
    path = os.path.join(cache_dir, f"pipeline_results_ws{ws_id}.json")
    with open(path, "w") as f:
        _json.dump(results, f)

    # Confirm burst 2 (photo 3) as Golden Eagle -> detaches to new Golden Eagle encounter
    resp = client.post("/api/encounters/species",
                       json={"species": "Golden Eagle", "photo_ids": [3], "burst_index": 2})
    assert resp.status_code == 200

    # Now confirm burst (photo 2, still in original encounter) as Golden Eagle.
    # Its burst_index in the original encounter is now 1 (after photo 3 detached).
    with open(path) as f:
        mid = _json.load(f)
    bald_idx = next(i for i, e in enumerate(mid["encounters"]) if 1 in e["photo_ids"])
    burst_idx_in_bald = next(
        i for i, b in enumerate(mid["encounters"][bald_idx]["bursts"]) if 2 in b["photo_ids"]
    )
    resp = client.post("/api/encounters/species",
                       json={"species": "Golden Eagle", "photo_ids": [2],
                             "burst_index": burst_idx_in_bald,
                             "encounter_index": bald_idx})
    assert resp.status_code == 200

    with open(path) as f:
        final = _json.load(f)
    # Expect 2 encounters: original Bald Eagle (photo 1), one Golden Eagle with photos 2 & 3
    assert len(final["encounters"]) == 2
    golden = next(e for e in final["encounters"] if e.get("confirmed_species") == "Golden Eagle")
    assert set(golden["photo_ids"]) == {2, 3}
    assert len(golden["bursts"]) == 2
    bald = next(e for e in final["encounters"] if e is not golden)
    assert bald["photo_ids"] == [1]


def test_keyword_duplicates_scoped_by_workspace(app_and_db):
    """Keyword duplicates endpoint only reports duplicates within the active workspace."""
    app, db = app_and_db
    ws = db._active_workspace_id

    # Default workspace already has photos from conftest — add a case-variant keyword
    # Must insert directly to bypass add_keyword's case-insensitive dedup
    cur = db.conn.execute(
        "INSERT INTO keywords (name, is_species) VALUES (?, 0)", ("cardinal",)
    )
    db.conn.commit()
    k = cur.lastrowid
    # Tag a photo in the current workspace with the variant
    photos = db.get_photos()
    db.tag_photo(photos[0]["id"], k)

    # Create workspace B with its own folder and photo
    ws_b = db.create_workspace("B")
    db.set_active_workspace(ws_b)
    fid_b = db.add_folder("/photos/b", name="b")
    pid_b = db.add_photo(folder_id=fid_b, filename="b.jpg", extension=".jpg",
                         file_size=100, file_mtime=1.0)
    # Insert a case-variant of "Sparrow" directly to bypass dedup
    cur_b = db.conn.execute(
        "INSERT INTO keywords (name, is_species) VALUES (?, 0)", ("sparrow",)
    )
    db.conn.commit()
    k_b = cur_b.lastrowid
    db.tag_photo(pid_b, k_b)

    # Switch back to default workspace for the API call
    db.set_active_workspace(ws)

    with app.test_client() as c:
        # In workspace A, should see Cardinal/cardinal dupe but not Sparrow/sparrow
        resp = c.get("/api/keywords/duplicates")
        data = resp.get_json()
        dupe_names = []
        for d in data:
            for v in d["variants"]:
                dupe_names.append(v["name"])
        assert "Cardinal" in dupe_names or "cardinal" in dupe_names
        # sparrow dupe is only in ws_b, should not appear
        assert "sparrow" not in dupe_names


def test_keyword_duplicates_scoped_by_slot(app_and_db):
    """Same-name keywords in different slots (parent_id, type) are not reported.

    The cleanup endpoint merges only within
    (keyword_match_key, parent_id, type, species-bearing).
    If the duplicates listing groups solely by lowered name, users would see
    persistent false positives — a taxonomy `Robin` and an individual `Robin`,
    or `Springfield` under Illinois vs Missouri — that never disappear after
    clicking Clean.
    """
    app, db = app_and_db
    photos = db.get_photos()
    p1 = photos[0]["id"]
    p2 = photos[1]["id"]

    # Same name under two different parents — legitimately distinct places
    illinois = db.add_keyword("Illinois")
    missouri = db.add_keyword("Missouri")
    sfd_il = db.add_keyword("Springfield", parent_id=illinois)
    sfd_mo = db.add_keyword("Springfield", parent_id=missouri)
    db.tag_photo(p1, sfd_il)
    db.tag_photo(p2, sfd_mo)

    # Same name at root but different type — species vs general
    # Insert directly to bypass add_keyword's dedup, and match how the migration
    # allows type-distinct roots to coexist (UNIQUE(name, parent_id) permits it
    # because parent_id is NULL — NULLs don't compare equal in SQLite).
    cur1 = db.conn.execute(
        "INSERT INTO keywords (name, type, is_species) VALUES (?, 'taxonomy', 1)",
        ("Robin",),
    )
    cur2 = db.conn.execute(
        "INSERT INTO keywords (name, type, is_species) VALUES (?, 'general', 0)",
        ("Robin",),
    )
    db.conn.commit()
    db.tag_photo(p1, cur1.lastrowid)
    db.tag_photo(p2, cur2.lastrowid)

    # Same name/parent/type but different effective species identity. Legacy
    # databases may contain type=general,is_species=1 species alongside an
    # ordinary general homonym; cleanup must not offer or merge this pair.
    species_general = db.conn.execute(
        "INSERT INTO keywords (name, type, is_species) "
        "VALUES (?, 'general', 1)",
        ("robin",),
    ).lastrowid
    db.conn.commit()
    db.tag_photo(p1, species_general)

    with app.test_client() as c:
        resp = c.get("/api/keywords/duplicates")
        assert resp.status_code == 200
        data = resp.get_json()
        dupe_names = {v["name"] for d in data for v in d["variants"]}
        # Neither slot-distinct name should be reported as a duplicate
        assert "Springfield" not in dupe_names
        assert "Robin" not in dupe_names
        assert "robin" not in dupe_names


def test_all_keywords_scoped_by_workspace(app_and_db):
    """GET /api/keywords/all only returns keywords used in the active workspace, plus ancestors."""
    app, db = app_and_db
    ws_a = db._active_workspace_id

    # Create parent keyword "Birds" and child "Hawk" under it
    k_birds = db.add_keyword("Birds")
    k_hawk = db.add_keyword("Hawk", parent_id=k_birds)
    # Tag a photo in workspace A with the child only
    photos_a = db.get_photos()
    db.tag_photo(photos_a[0]["id"], k_hawk)

    # Create workspace B with its own folder, photo, and keyword "Penguin"
    ws_b = db.create_workspace("B")
    db.set_active_workspace(ws_b)
    fid_b = db.add_folder("/photos/b", name="b")
    pid_b = db.add_photo(folder_id=fid_b, filename="b.jpg", extension=".jpg",
                         file_size=100, file_mtime=1.0)
    k_penguin = db.add_keyword("Penguin")
    db.tag_photo(pid_b, k_penguin)

    # Switch back to workspace A
    db.set_active_workspace(ws_a)

    with app.test_client() as c:
        resp = c.get("/api/keywords/all")
        data = resp.get_json()
        names = [k["name"] for k in data]
        # Child keyword tagged in workspace A — present
        assert "Hawk" in names
        # Parent keyword not tagged but is ancestor of Hawk — present with
        # descendant photo count, plus direct count for callers that need it.
        assert "Birds" in names
        birds = next(k for k in data if k["name"] == "Birds")
        assert birds["photo_count"] == 1
        assert birds["direct_photo_count"] == 0
        # Keyword only in workspace B — absent
        assert "Penguin" not in names


def test_set_active_labels_scoped_to_workspace(app_and_db, tmp_path):
    """Setting active labels stores them in workspace config_overrides, not global file."""
    app, db = app_and_db

    # Create a fake label file
    labels_dir = tmp_path / "labels"
    labels_dir.mkdir(exist_ok=True)
    label_path = str(labels_dir / "test-birds.txt")
    with open(label_path, "w") as f:
        f.write("Robin\nJay\n")

    with app.test_client() as c:
        resp = c.post("/api/labels/active",
                       json={"labels_files": [label_path]},
                       content_type="application/json")
        assert resp.status_code == 200

    # Verify it's stored in workspace config_overrides
    result = db.get_workspace_active_labels()
    assert result == [label_path]


def test_labels_list_returns_workspace_active(app_and_db, tmp_path):
    """GET /api/labels returns active labels from the workspace, not global."""
    app, db = app_and_db

    # Set workspace-specific active labels
    labels_dir = tmp_path / "labels"
    labels_dir.mkdir(exist_ok=True)
    label_path = str(labels_dir / "test-birds.txt")
    with open(label_path, "w") as f:
        f.write("Robin\nJay\n")
    meta_path = str(labels_dir / "test-birds.json")
    import json as _json
    with open(meta_path, "w") as f:
        _json.dump({"name": "Test Birds", "labels_file": label_path, "species_count": 2}, f)

    db.set_workspace_active_labels([label_path])

    import labels as labels_mod
    orig_labels_dir = labels_mod.LABELS_DIR
    labels_mod.LABELS_DIR = str(labels_dir)
    try:
        with app.test_client() as c:
            resp = c.get("/api/labels")
            data = resp.get_json()
            active_files = [a.get("labels_file") for a in data["active"]]
            assert label_path in active_files
    finally:
        labels_mod.LABELS_DIR = orig_labels_dir


def test_pipeline_page_init_includes_workspace_overrides(app_and_db):
    """page-init response includes workspace config overrides."""
    app, db = app_and_db
    # Set a workspace override first
    db.update_workspace(db._active_workspace_id, config_overrides={"review_min_confidence": 25})
    with app.test_client() as c:
        resp = c.get("/api/pipeline/page-init")
        assert resp.status_code == 200
        data = resp.get_json()
        assert "workspace_overrides" in data
        assert data["workspace_overrides"]["review_min_confidence"] == 25


def test_review_min_confidence_persists_in_workspace(app_and_db):
    """review_min_confidence can be saved and read from workspace config."""
    app, db = app_and_db
    with app.test_client() as c:
        # Save threshold
        resp = c.post("/api/workspaces/active/config",
                       json={"review_min_confidence": 40},
                       content_type="application/json")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["overrides"]["review_min_confidence"] == 40

        # Read it back
        resp = c.get("/api/workspaces/active/config")
        assert resp.status_code == 200
        assert resp.get_json()["review_min_confidence"] == 40


def test_workspace_config_post_preserves_non_whitelisted_keys(app_and_db):
    """POST /api/workspaces/active/config merges into existing overrides,
    preserving keys not in the whitelist (e.g. active_labels)."""
    app, db = app_and_db
    # Pre-set overrides with a non-whitelisted key
    db.update_workspace(db._active_workspace_id,
                        config_overrides={"active_labels": ["/path/to/birds.txt"],
                                          "classification_threshold": 0.5})
    with app.test_client() as c:
        # POST only review_min_confidence
        resp = c.post("/api/workspaces/active/config",
                       json={"review_min_confidence": 30},
                       content_type="application/json")
        assert resp.status_code == 200
        overrides = resp.get_json()["overrides"]
        # New key saved
        assert overrides["review_min_confidence"] == 30
        # Whitelisted key preserved
        assert overrides["classification_threshold"] == 0.5
        # Non-whitelisted key preserved
        assert overrides["active_labels"] == ["/path/to/birds.txt"]


def test_ws_detector_confidence_slider_has_explicit_default(app_and_db):
    """The wsVal_detector_confidence range input must carry value="20".

    HTML5 range inputs with no `value` initialize to (min+max)/2, so a
    5..50 range silently sits at ~27 on first render. toggleWsOverride
    only applies its 20 default when `!input.value`, which is never
    falsy for range elements. Without the explicit attribute, enabling
    the override would persist ~0.27 instead of 0.20 and unexpectedly
    hide low-confidence detections.
    """
    import re
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get('/settings')
    assert resp.status_code == 200
    m = re.search(
        rb'<input[^>]*\bid="wsVal_detector_confidence"[^>]*\bvalue="20"',
        resp.data,
    )
    assert m, ('wsVal_detector_confidence range needs explicit value="20" '
               'to avoid HTML5 range midpoint default (would persist ~0.27)')


def test_get_all_keywords(app_and_db):
    """GET /api/keywords/all returns only keywords used in the active workspace."""
    app, db = app_and_db
    client = app.test_client()
    # conftest already created 'Cardinal' (tagged to p1) and 'Sparrow' (tagged to p2)
    # Add an untagged keyword — should NOT appear since it has no photos in workspace
    db.add_keyword("favorite")

    resp = client.get("/api/keywords/all")
    assert resp.status_code == 200
    data = resp.get_json()
    names = [k["name"] for k in data]
    assert "Cardinal" in names
    assert "Sparrow" in names
    assert "favorite" not in names
    cardinal = next(k for k in data if k["name"] == "Cardinal")
    assert cardinal["photo_count"] >= 1
    assert "type" in cardinal


def test_update_keyword_type(app_and_db):
    """PUT /api/keywords/<id> updates keyword type."""
    app, db = app_and_db
    client = app.test_client()
    kid = db.add_keyword("Tim")
    resp = client.put(f"/api/keywords/{kid}", json={"type": "individual"})
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["ok"] is True
    row = db.conn.execute("SELECT type FROM keywords WHERE id = ?", (kid,)).fetchone()
    assert row["type"] == "individual"


def test_update_keyword_type_invalid(app_and_db):
    """PUT /api/keywords/<id> rejects invalid types."""
    app, db = app_and_db
    client = app.test_client()
    kid = db.add_keyword("test")
    resp = client.put(f"/api/keywords/{kid}", json={"type": "invalid_type"})
    assert resp.status_code == 400


def test_update_keyword_name(app_and_db):
    """PUT /api/keywords/<id> can rename a keyword."""
    app, db = app_and_db
    client = app.test_client()
    kid = db.add_keyword("old_name")
    resp = client.put(f"/api/keywords/{kid}", json={"name": "new_name"})
    assert resp.status_code == 200
    row = db.conn.execute("SELECT name FROM keywords WHERE id = ?", (kid,)).fetchone()
    assert row["name"] == "new_name"


def test_rename_keyword_queues_sidecar_changes(app_and_db):
    """Renaming a keyword queues remove+add pending changes for affected photos."""
    app, db = app_and_db
    client = app.test_client()
    kid = db.add_keyword("OldBird")
    # conftest photos: p1 is in folder '/photos/2024'
    p1 = db.conn.execute("SELECT id FROM photos LIMIT 1").fetchone()["id"]
    db.tag_photo(p1, kid)
    # Clear any prior pending changes
    db.conn.execute("DELETE FROM pending_changes")
    db.conn.commit()

    resp = client.put(f"/api/keywords/{kid}", json={"name": "NewBird"})
    assert resp.status_code == 200

    changes = db.conn.execute(
        "SELECT change_type, value FROM pending_changes WHERE photo_id = ? ORDER BY id",
        (p1,),
    ).fetchall()
    actions = [(c["change_type"], c["value"]) for c in changes]
    assert ("keyword_remove", "OldBird") in actions
    assert ("keyword_add", "NewBird") in actions


def test_rename_keyword_normalizes_edge_quotes_and_queues_clean_name(app_and_db):
    """A rename request with stray edge quotes must store the normalized
    value AND queue that normalized value to sidecars. Otherwise the DB
    holds the clean spelling while pending changes and history reference
    the raw quoted variant, and XMP sync would then write the wrong name.
    """
    app, db = app_and_db
    client = app.test_client()
    kid = db.add_keyword("OldBird")
    p1 = db.conn.execute("SELECT id FROM photos LIMIT 1").fetchone()["id"]
    db.tag_photo(p1, kid)
    db.conn.execute("DELETE FROM pending_changes")
    db.conn.commit()

    resp = client.put(f"/api/keywords/{kid}", json={"name": "‘apapane"})
    assert resp.status_code == 200

    row = db.conn.execute(
        "SELECT name FROM keywords WHERE id = ?", (kid,)
    ).fetchone()
    assert row["name"] == "apapane"

    changes = db.conn.execute(
        "SELECT change_type, value FROM pending_changes WHERE photo_id = ? ORDER BY id",
        (p1,),
    ).fetchall()
    actions = [(c["change_type"], c["value"]) for c in changes]
    assert ("keyword_remove", "OldBird") in actions
    assert ("keyword_add", "apapane") in actions
    assert ("keyword_add", "‘apapane") not in actions


def test_rename_keyword_rejects_empty_after_normalization(app_and_db):
    """PUT /api/keywords/<id> with a quote-only name must be rejected at
    the boundary — same contract as add_keyword — instead of storing an
    invisible empty-string keyword row."""
    app, db = app_and_db
    client = app.test_client()
    kid = db.add_keyword("Real")

    resp = client.put(f"/api/keywords/{kid}", json={"name": "'"})
    assert resp.status_code == 400

    row = db.conn.execute(
        "SELECT name FROM keywords WHERE id = ?", (kid,)
    ).fetchone()
    assert row["name"] == "Real"


def test_rename_keyword_merges_into_normalized_peer_toplevel(app_and_db):
    """Renaming a top-level keyword to a name that normalizes to an existing
    top-level peer must merge into that peer instead of writing a second row.
    SQLite treats NULL parent_ids as distinct under UNIQUE(name, parent_id),
    so without the peer check the rename would silently produce two peer
    `apapane` rows for later duplicate cleanup to mop up."""
    app, db = app_and_db
    client = app.test_client()
    p1 = db.conn.execute("SELECT id FROM photos LIMIT 1").fetchone()["id"]
    apapane_id = db.add_keyword("apapane")
    other_id = db.add_keyword("Other")
    db.tag_photo(p1, other_id)
    db.conn.execute("DELETE FROM pending_changes")
    db.conn.commit()

    resp = client.put(f"/api/keywords/{other_id}", json={"name": "‘apapane"})
    assert resp.status_code == 200

    rows = db.conn.execute(
        "SELECT id FROM keywords WHERE parent_id IS NULL "
        "AND name = 'apapane'"
    ).fetchall()
    assert [row["id"] for row in rows] == [apapane_id]
    other_row = db.conn.execute(
        "SELECT id FROM keywords WHERE id = ?", (other_id,)
    ).fetchone()
    assert other_row is None
    tag_ids = {
        row["keyword_id"]
        for row in db.conn.execute(
            "SELECT keyword_id FROM photo_keywords WHERE photo_id = ?", (p1,)
        ).fetchall()
    }
    assert apapane_id in tag_ids
    assert other_id not in tag_ids

    changes = db.conn.execute(
        "SELECT change_type, value FROM pending_changes WHERE photo_id = ? "
        "ORDER BY id",
        (p1,),
    ).fetchall()
    actions = [(c["change_type"], c["value"]) for c in changes]
    assert ("keyword_remove", "Other") in actions
    assert ("keyword_add", "apapane") in actions
    assert ("keyword_add", "‘apapane") not in actions


def test_rename_keyword_merges_into_normalized_peer_child(app_and_db):
    """Same guard for child keywords: without the peer check, two rows under
    the same parent with normalized-equal names would violate
    UNIQUE(name, parent_id) at UPDATE time and surface as a 500. The merge
    turns that into an in-place consolidation instead."""
    app, db = app_and_db
    client = app.test_client()
    parent_id = db.add_keyword("Birds")
    apapane_id = db.add_keyword("apapane", parent_id=parent_id)
    other_id = db.add_keyword("Other", parent_id=parent_id)

    resp = client.put(f"/api/keywords/{other_id}", json={"name": "‘apapane"})
    assert resp.status_code == 200

    rows = db.conn.execute(
        "SELECT id FROM keywords WHERE parent_id = ? AND name = 'apapane'",
        (parent_id,),
    ).fetchall()
    assert [row["id"] for row in rows] == [apapane_id]
    other_row = db.conn.execute(
        "SELECT id FROM keywords WHERE id = ?", (other_id,)
    ).fetchone()
    assert other_row is None


def test_rename_keyword_does_not_merge_across_types_toplevel(app_and_db):
    """Cross-type collisions at the top level must not be merged into a
    peer. Silently moving photos from an 'individual' keyword onto a
    same-named 'taxonomy' peer would rewrite the tag's semantics; the
    dedup boundary elsewhere is (name, parent_id, type). SQLite treats
    NULL parent_ids as distinct under UNIQUE(name, parent_id), so a
    cross-type rename at the top level is allowed to produce a
    coexisting row rather than a silent merge."""
    app, db = app_and_db
    client = app.test_client()
    p1 = db.conn.execute("SELECT id FROM photos LIMIT 1").fetchone()["id"]
    taxonomy_id = db.add_keyword("apapane", kw_type="taxonomy")
    other_id = db.add_keyword("Other", kw_type="individual")
    db.tag_photo(p1, other_id)

    resp = client.put(f"/api/keywords/{other_id}", json={"name": "‘apapane"})
    assert resp.status_code == 200

    # The individual row survives with its normalized name; the taxonomy
    # peer is untouched.
    survivor = db.conn.execute(
        "SELECT id, name, type FROM keywords WHERE id = ?", (other_id,)
    ).fetchone()
    assert survivor is not None
    assert survivor["name"] == "apapane"
    assert survivor["type"] == "individual"
    taxonomy_row = db.conn.execute(
        "SELECT type FROM keywords WHERE id = ?", (taxonomy_id,)
    ).fetchone()
    assert taxonomy_row["type"] == "taxonomy"
    # The individual tag must NOT have silently retargeted onto the
    # taxonomy peer.
    tag_ids = {
        row["keyword_id"]
        for row in db.conn.execute(
            "SELECT keyword_id FROM photo_keywords WHERE photo_id = ?", (p1,)
        ).fetchall()
    }
    assert other_id in tag_ids
    assert taxonomy_id not in tag_ids


def test_rename_keyword_does_not_merge_across_types_child(app_and_db):
    """Cross-type collisions under the same parent surface as an error
    rather than silently retagging photos across types. UNIQUE(name,
    parent_id) fires when the plain UPDATE lands on a cross-type peer."""
    app, db = app_and_db
    client = app.test_client()
    parent_id = db.add_keyword("Birds")
    db.add_keyword("apapane", parent_id=parent_id, kw_type="taxonomy")
    other_id = db.add_keyword("Other", parent_id=parent_id, kw_type="individual")

    resp = client.put(f"/api/keywords/{other_id}", json={"name": "‘apapane"})
    # A cross-type same-name collision under a shared parent hits the
    # table-level UNIQUE(name, parent_id) at UPDATE time and surfaces to
    # the caller, rather than being silently absorbed into the wrong-typed
    # peer.
    assert resp.status_code >= 400
    # The original individual keyword is preserved with its old name.
    survivor = db.conn.execute(
        "SELECT name, type FROM keywords WHERE id = ?", (other_id,)
    ).fetchone()
    assert survivor is not None
    assert survivor["name"] == "Other"
    assert survivor["type"] == "individual"


def test_rename_keyword_updates_photo_preferences(app_and_db):
    """Representative-photo preferences follow species keyword renames."""
    app, db = app_and_db
    client = app.test_client()
    kid = db.add_keyword("OldBird", is_species=True)
    p1 = db.conn.execute("SELECT id FROM photos LIMIT 1").fetchone()["id"]
    db.tag_photo(p1, kid)
    db.set_photo_preference("life_list", "OldBird", p1)
    db.set_photo_preference("highlights", "OldBird", p1)

    resp = client.put(f"/api/keywords/{kid}", json={"name": "NewBird"})
    assert resp.status_code == 200

    assert db.get_photo_preferences("life_list") == {"NewBird": p1}
    assert db.get_photo_preferences("highlights") == {"NewBird": p1}


def test_rename_keyword_updates_species_representative(app_and_db):
    """species_representative preferences follow species keyword renames."""
    app, db = app_and_db
    client = app.test_client()
    kid = db.add_keyword("OldBird", is_species=True)
    p1 = db.conn.execute("SELECT id FROM photos LIMIT 1").fetchone()["id"]
    db.tag_photo(p1, kid)
    db.set_photo_preference("species_representative", "OldBird", p1)

    resp = client.put(f"/api/keywords/{kid}", json={"name": "NewBird"})
    assert resp.status_code == 200

    assert db.get_photo_preferences("species_representative") == {"NewBird": p1}


def test_rename_keyword_photo_preferences_keep_existing_target(app_and_db):
    """Renaming into an existing preference keeps the target preference."""
    app, db = app_and_db
    client = app.test_client()
    kid_old = db.add_keyword("OldBird", is_species=True)
    rows = db.conn.execute("SELECT id FROM photos ORDER BY id LIMIT 2").fetchall()
    p_old = rows[0]["id"]
    p_new = rows[1]["id"]
    db.tag_photo(p_old, kid_old)
    db.set_photo_preference("life_list", "OldBird", p_old)
    db.set_photo_preference("life_list", "NewBird", p_new)

    resp = client.put(f"/api/keywords/{kid_old}", json={"name": "NewBird"})
    assert resp.status_code == 200

    assert db.get_photo_preferences("life_list") == {"NewBird": p_new}


def test_rename_keyword_updates_species_highlights(app_and_db):
    """Ordered species highlights follow species-keyword renames."""
    app, db = app_and_db
    client = app.test_client()
    kid = db.add_keyword("OldBird", is_species=True)
    rows = db.conn.execute("SELECT id FROM photos ORDER BY id LIMIT 2").fetchall()
    p1, p2 = rows[0]["id"], rows[1]["id"]
    db.tag_photo(p1, kid)
    db.tag_photo(p2, kid)
    db.add_species_highlight("OldBird", p1)
    db.add_species_highlight("OldBird", p2)

    resp = client.put(f"/api/keywords/{kid}", json={"name": "NewBird"})
    assert resp.status_code == 200

    assert db.get_species_highlights("OldBird") == {}
    highlights = db.get_species_highlights("NewBird")
    assert highlights == {"NewBird": {p1: 1, p2: 2}}


def test_rename_keyword_species_highlights_merge_into_existing_target(app_and_db):
    """Renaming into an existing highlights bucket appends after the target's rows."""
    app, db = app_and_db
    client = app.test_client()
    kid_old = db.add_keyword("OldBird", is_species=True)
    kid_new = db.add_keyword("NewBird", is_species=True)
    rows = db.conn.execute("SELECT id FROM photos ORDER BY id LIMIT 3").fetchall()
    p_target = rows[0]["id"]
    p_moving_a = rows[1]["id"]
    p_moving_b = rows[2]["id"]
    db.tag_photo(p_target, kid_new)
    db.tag_photo(p_moving_a, kid_old)
    db.tag_photo(p_moving_b, kid_old)
    db.add_species_highlight("NewBird", p_target)
    db.add_species_highlight("OldBird", p_moving_a)
    db.add_species_highlight("OldBird", p_moving_b)

    resp = client.put(f"/api/keywords/{kid_old}", json={"name": "NewBird"})
    assert resp.status_code == 200

    assert db.get_species_highlights("OldBird") == {}
    assert db.get_species_highlights("NewBird") == {
        "NewBird": {p_target: 1, p_moving_a: 2, p_moving_b: 3}
    }


def test_rename_keyword_species_highlights_dedupe_existing_photo(app_and_db):
    """A photo already highlighted under the target species keeps its target rank."""
    app, db = app_and_db
    client = app.test_client()
    kid_old = db.add_keyword("OldBird", is_species=True)
    kid_new = db.add_keyword("NewBird", is_species=True)
    rows = db.conn.execute("SELECT id FROM photos ORDER BY id LIMIT 2").fetchall()
    p_target = rows[0]["id"]
    p_shared = rows[1]["id"]
    db.tag_photo(p_target, kid_new)
    db.tag_photo(p_shared, kid_old)
    db.tag_photo(p_shared, kid_new)
    db.add_species_highlight("NewBird", p_target)
    db.add_species_highlight("NewBird", p_shared)
    db.add_species_highlight("OldBird", p_shared)

    resp = client.put(f"/api/keywords/{kid_old}", json={"name": "NewBird"})
    assert resp.status_code == 200

    assert db.get_species_highlights("OldBird") == {}
    assert db.get_species_highlights("NewBird") == {
        "NewBird": {p_target: 1, p_shared: 2}
    }


def test_rename_species_highlights_species_chunks_scoped_photo_ids(
    app_and_db, monkeypatch,
):
    """rename_species_highlights_species must chunk the IN(...) clause when
    called with more photos than SQLite's bound-parameter cap. Without the
    chunk, an unbounded IN clause on legacy SQLite builds
    (SQLITE_MAX_VARIABLE_NUMBER=999) raises OperationalError and strands the
    highlight rows under the renamed species."""
    from vireo import db as db_module

    _app, db = app_and_db
    ws = db._ws_id()
    kid_old = db.add_keyword("OldBird", is_species=True)
    fid = db.add_folder("/renamed", name="renamed")
    db.add_workspace_folder(ws, fid)

    # Seed enough photos+highlight rows to exceed the shrunk parameter cap
    # in a single un-chunked query.
    monkeypatch.setattr(db_module, "_SQLITE_PARAM_CHUNK_SIZE", 3)
    photo_ids = []
    for i in range(7):
        pid = db.add_photo(
            folder_id=fid, filename=f"p{i}.jpg", extension=".jpg",
            file_size=1, file_mtime=float(i),
        )
        db.tag_photo(pid, kid_old)
        db.add_species_highlight("OldBird", pid)
        photo_ids.append(pid)

    pairs = [(pid, ws) for pid in photo_ids]
    moved = db.rename_species_highlights_species("OldBird", "NewBird", pairs)

    assert moved == len(photo_ids)
    assert db.get_species_highlights("OldBird") == {}
    highlights = db.get_species_highlights("NewBird")
    # Ranks preserve the original bucket order (1..N).
    assert highlights == {
        "NewBird": {pid: rank for rank, pid in enumerate(photo_ids, start=1)}
    }


def test_apply_ordered_highlights_preserves_order_when_no_visible_match():
    """When a species has highlights elsewhere in the workspace but none are
    present in the current bucket, _apply_ordered_highlights must not re-sort
    the bucket. Re-sorting would drop the picked-first order that
    _highlight_score_bucket already applied on the visible photos."""
    from app import _apply_ordered_highlights

    class FakeDb:
        def get_species_highlights(self, eligible_only=False):
            assert eligible_only is True
            return {"Robin": {999: 1}}

    original = [
        {"id": 1, "highlight_score": 0.4, "flag": "flagged"},
        {"id": 2, "highlight_score": 0.9, "flag": "none"},
    ]
    buckets = [{"species": "Robin", "photos": list(original)}]
    _apply_ordered_highlights(FakeDb(), buckets)
    assert [p["id"] for p in buckets[0]["photos"]] == [1, 2]
    assert all(p["is_highlighted"] is False for p in buckets[0]["photos"])
    assert all(p["highlight_rank"] is None for p in buckets[0]["photos"])


def test_apply_ordered_highlights_resorts_when_visible_match():
    """When at least one visible photo is a stored highlight, the bucket must
    be re-sorted so the highlighted photo leads and follows the stored rank."""
    from app import _apply_ordered_highlights

    class FakeDb:
        def get_species_highlights(self, eligible_only=False):
            assert eligible_only is True
            return {"Robin": {2: 1}}

    buckets = [{
        "species": "Robin",
        "photos": [
            {"id": 1, "highlight_score": 0.9, "flag": "flagged"},
            {"id": 2, "highlight_score": 0.4, "flag": "none"},
        ],
    }]
    _apply_ordered_highlights(FakeDb(), buckets)
    order = [p["id"] for p in buckets[0]["photos"]]
    assert order == [2, 1]
    marks = {p["id"]: p["is_highlighted"] for p in buckets[0]["photos"]}
    assert marks == {1: False, 2: True}


def test_highlight_score_bucket_orders_picks_then_scored_then_unscored():
    """Highlights ordering (picked_first) forms three contiguous regions:
    picks (scored first, then unscored in capture order), scored non-picks by
    score, then unscored non-picks in capture order. No rich metrics are set,
    so highlight_score collapses to quality_score (+0.08 pick bonus)."""
    from app import _highlight_score_bucket

    photos = [
        # scored non-picks (out of order to prove score sorts them)
        {"id": 4, "quality_score": 0.3, "flag": "none", "timestamp": "2024-01-07"},
        {"id": 5, "quality_score": 0.9, "flag": "none", "timestamp": "2024-01-06"},
        # unscored non-picks (later capture times, shuffled)
        {"id": 6, "quality_score": None, "flag": "none", "timestamp": "2024-03-02"},
        {"id": 7, "quality_score": None, "flag": "none", "timestamp": "2024-03-01"},
        # picks: one scored, two unscored (shuffled capture times)
        {"id": 1, "quality_score": 0.5, "flag": "flagged", "timestamp": "2024-01-05"},
        {"id": 2, "quality_score": None, "flag": "flagged", "timestamp": "2024-01-09"},
        {"id": 3, "quality_score": None, "flag": "flagged", "timestamp": "2024-01-02"},
    ]
    _highlight_score_bucket(photos, picked_first=True)
    # picks first (scored pick 1, then unscored picks 3<2 by capture time),
    # then scored non-picks by score (5 before 4),
    # then unscored non-picks by capture time (7 before 6).
    assert [p["id"] for p in photos] == [1, 3, 2, 5, 4, 7, 6]


def test_highlight_score_bucket_picked_first_false_unchanged():
    """Regression: the non-picks-first path (life list / best-photo) still
    ranks purely by score descending, ignoring flag and capture time."""
    from app import _highlight_score_bucket

    photos = [
        {"id": 1, "quality_score": 0.4, "flag": "flagged", "timestamp": "2024-01-01"},
        {"id": 2, "quality_score": 0.9, "flag": "none", "timestamp": "2024-01-09"},
        {"id": 3, "quality_score": 0.6, "flag": "none", "timestamp": "2024-01-02"},
    ]
    _highlight_score_bucket(photos, picked_first=False)
    assert [p["id"] for p in photos] == [2, 3, 1]


def test_bucket_unanalyzed_count_counts_unscored_non_picks_only():
    from app import _bucket_unanalyzed_count

    photos = [
        {"id": 1, "quality_score": 0.5, "flag": "flagged"},   # scored pick
        {"id": 2, "quality_score": None, "flag": "flagged"},  # unscored pick (excluded)
        {"id": 3, "quality_score": 0.4, "flag": "none"},      # scored non-pick
        {"id": 4, "quality_score": None, "flag": "none"},     # unscored non-pick
        {"id": 5, "quality_score": None, "flag": "none"},     # unscored non-pick
    ]
    assert _bucket_unanalyzed_count(photos) == 2
    assert _bucket_unanalyzed_count([]) == 0
    assert _bucket_unanalyzed_count(None) == 0


def _set_photo_quality_flag(db, photo_id, quality=None, flag=None):
    db.conn.execute(
        "UPDATE photos SET quality_score = ?, flag = ? WHERE id = ?",
        (quality, flag, photo_id),
    )
    db.conn.commit()


def _seed_anianiau_bucket(db):
    """Seed one species folder mirroring the real bug: a scored pick, two
    unscored picks, scored/unscored non-picks, and a rejected photo. Returns
    (folder_id, {label: photo_id})."""
    fid = db.add_folder('/photos/hawaii', name='hawaii')
    kid = db.add_keyword('Anianiau', is_species=True)
    spec = [
        # label, quality, flag, timestamp
        ("scored_pick",   0.45, "flagged", "2024-01-05T10:00:00"),
        ("unscored_pickB", None, "flagged", "2024-01-02T10:00:00"),
        ("unscored_pickA", None, "flagged", "2024-01-09T10:00:00"),
        ("np_high",       0.90, None,       "2024-01-06T10:00:00"),
        ("np_low",        0.40, None,       "2024-01-07T10:00:00"),
        ("unp_early",     None, None,       "2024-03-01T10:00:00"),
        ("unp_late",      None, None,       "2024-03-02T10:00:00"),
        ("rejected",      0.99, "rejected", "2024-01-01T10:00:00"),
    ]
    ids = {}
    for i, (label, quality, flag, ts) in enumerate(spec):
        pid = db.add_photo(
            folder_id=fid, filename=f"{label}.jpg", extension='.jpg',
            file_size=1000 + i, file_mtime=float(i), timestamp=ts,
        )
        db.tag_photo(pid, kid)
        _set_photo_quality_flag(db, pid, quality=quality, flag=flag)
        ids[label] = pid
    return fid, ids


def test_highlights_candidates_include_unscored_picks_and_order(app_and_db):
    """Unscored photos now flow into Highlights: all three picks appear (the
    real bug was two vanishing), ordered picks -> scored -> unscored, with
    is_analyzed flags and unanalyzed_count set for the divider."""
    from app import (
        _apply_highlight_preferences,
        _apply_ordered_highlights,
        _collect_highlight_buckets,
    )

    app, db = app_and_db
    fid, ids = _seed_anianiau_bucket(db)

    candidates = db.get_highlights_candidates(fid, min_quality=0.0)
    buckets, _unid = _collect_highlight_buckets(candidates, 0.70)
    _apply_ordered_highlights(db, buckets)
    # unanalyzed_count is assigned in _apply_highlight_preferences (which
    # always runs after ordering in the real flow) so its tail count
    # reflects any curated promotion. Mirror the full pipeline here.
    _apply_highlight_preferences(db, buckets)
    bucket = next(b for b in buckets if b["species"] == "Anianiau")

    order = [p["filename"] for p in bucket["photos"]]
    assert order == [
        "scored_pick.jpg",       # scored pick leads
        "unscored_pickB.jpg",    # unscored picks next, capture order
        "unscored_pickA.jpg",
        "np_high.jpg",           # scored non-picks by score
        "np_low.jpg",
        "unp_early.jpg",         # unscored non-picks last, capture order
        "unp_late.jpg",
    ]
    # rejected photo is still excluded entirely
    assert "rejected.jpg" not in order

    by_name = {p["filename"]: p for p in bucket["photos"]}
    assert by_name["scored_pick.jpg"]["is_analyzed"] is True
    assert by_name["unscored_pickA.jpg"]["is_analyzed"] is False
    assert by_name["unp_early.jpg"]["is_analyzed"] is False
    # Only unscored non-picks form the labeled "not yet analyzed" tail.
    assert bucket["unanalyzed_count"] == 2


def test_highlights_candidates_exclude_unscored_when_quality_floor_raised(app_and_db):
    """Raising the quality floor above 0 drops unscored photos (no measured
    quality) and low-scored ones, leaving only photos above the floor."""
    from app import _collect_highlight_buckets

    app, db = app_and_db
    fid, ids = _seed_anianiau_bucket(db)

    candidates = db.get_highlights_candidates(fid, min_quality=0.5)
    buckets, _unid = _collect_highlight_buckets(candidates, 0.70)
    bucket = next(b for b in buckets if b["species"] == "Anianiau")
    names = {p["filename"] for p in bucket["photos"]}

    # Only np_high (0.90) clears the 0.5 floor; the rejected 0.99 stays excluded.
    assert names == {"np_high.jpg"}


def test_highlight_bucket_canonicalizes_accepted_hierarchy_species():
    """Accepted hierarchy aliases use the same canonical bucket key as
    curation setters and root species rows."""
    from app import _collect_highlight_buckets

    candidates = [{
        "id": 1,
        "filename": "verdin.jpg",
        "species": "Desert Verdin",
        "quality_score": 0.8,
    }]
    buckets, _unidentified = _collect_highlight_buckets(
        candidates,
        0.7,
        canonicalize_species=lambda _name: "Verdin",
    )

    assert [bucket["species"] for bucket in buckets] == ["Verdin"]


def test_highlights_unscored_only_workspace_returns_content_with_empty_folders(app_and_db):
    """Unscored-only workspaces must not silently blank the Highlights page.

    ``get_folders_with_quality_data`` is scored-only, so the payload's
    ``folders`` list is empty when nothing has been analyzed. But the
    highlights payload can still return eligible content (buckets and
    unidentified photos) via the min_quality<=0 admission of unscored
    candidates. The frontend gate must key off actual content, not folder
    presence — this test pins the API contract so a future regression that
    drops content when ``folders == []`` is caught here.
    """
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get("/api/highlights")
    assert resp.status_code == 200
    data = resp.get_json()

    # The scored-only folder dropdown is legitimately empty in this fixture.
    assert data["folders"] == []
    # ...but the payload still carries eligible content — the workspace has
    # three unscored, unrejected photos in the unidentified section.
    assert data["unidentified"]["photo_count"] == 3
    assert data["meta"]["eligible"] == 3


def test_highlights_unanalyzed_count_reflects_tail_after_representative_promotion(app_and_db):
    """Curated promotion of an unscored photo must not misplace the divider.

    ``_apply_highlight_preferences`` runs after the initial bucket sort and
    can promote an unscored, non-flagged photo to the front when it's saved
    as the species representative. The tail count anchors the "Not yet
    analyzed" divider, so under the old (total-unscored-non-picks) semantic
    a promoted unscored rep would inflate the count and the frontend divider
    would misfire above analyzed photos. The tail semantic (trailing
    contiguous run of unscored non-picks) fixes that: after promotion, the
    count is the number of unscored non-picks that remain at the actual
    tail, not the total in the bucket.
    """
    from app import (
        _apply_highlight_preferences,
        _apply_ordered_highlights,
        _collect_highlight_buckets,
    )

    _app, db = app_and_db
    fid = db.add_folder('/photos/curate', name='curate')
    kid = db.add_keyword('Iiwi', is_species=True)
    scored_a = db.add_photo(
        folder_id=fid, filename='scored_a.jpg', extension='.jpg',
        file_size=1000, file_mtime=1.0, timestamp='2024-01-01T10:00:00',
    )
    scored_b = db.add_photo(
        folder_id=fid, filename='scored_b.jpg', extension='.jpg',
        file_size=1001, file_mtime=2.0, timestamp='2024-01-02T10:00:00',
    )
    unscored_rep = db.add_photo(
        folder_id=fid, filename='unscored_rep.jpg', extension='.jpg',
        file_size=1002, file_mtime=3.0, timestamp='2024-01-03T10:00:00',
    )
    unscored_tail = db.add_photo(
        folder_id=fid, filename='unscored_tail.jpg', extension='.jpg',
        file_size=1003, file_mtime=4.0, timestamp='2024-01-04T10:00:00',
    )
    for pid in (scored_a, scored_b, unscored_rep, unscored_tail):
        db.tag_photo(pid, kid)
    _set_photo_quality_flag(db, scored_a, quality=0.9)
    _set_photo_quality_flag(db, scored_b, quality=0.4)
    # unscored_rep and unscored_tail deliberately left with quality_score=NULL.

    # Sanity-check the pre-curation ordering: unscored non-picks form a
    # contiguous tail, so the tail count equals the total unscored non-pick
    # count (2). This is the state the divider was designed for.
    candidates = db.get_highlights_candidates(fid, min_quality=0.0)
    buckets, _unid = _collect_highlight_buckets(candidates, 0.70)
    _apply_ordered_highlights(db, buckets)
    _apply_highlight_preferences(db, buckets)
    bucket = next(b for b in buckets if b["species"] == "Iiwi")
    assert [p["filename"] for p in bucket["photos"]] == [
        "scored_a.jpg", "scored_b.jpg",
        "unscored_rep.jpg", "unscored_tail.jpg",
    ]
    assert bucket["unanalyzed_count"] == 2

    # Now promote unscored_rep to species representative. Preferences run
    # after the initial sort and push it to the front, so the tail shrinks
    # to just unscored_tail — anything else would place the divider above
    # analyzed content.
    db.set_species_representative("Iiwi", unscored_rep)

    candidates = db.get_highlights_candidates(fid, min_quality=0.0)
    buckets, _unid = _collect_highlight_buckets(candidates, 0.70)
    _apply_ordered_highlights(db, buckets)
    _apply_highlight_preferences(db, buckets)
    bucket = next(b for b in buckets if b["species"] == "Iiwi")

    order = [p["filename"] for p in bucket["photos"]]
    assert order[0] == "unscored_rep.jpg"  # rep promoted to the front
    assert order[-1] == "unscored_tail.jpg"  # tail intact
    assert bucket["unanalyzed_count"] == 1  # tail-only, not 2


def test_bucket_unanalyzed_count_ignores_non_trailing_unscored():
    """Only the trailing contiguous run of unscored non-picks counts.

    Curated ordering can leave an unscored, non-flagged photo above analyzed
    content; those interior unscored photos must NOT be counted, or the
    frontend divider would misfire on the first one and leave analyzed
    photos below the label.
    """
    from app import _bucket_unanalyzed_count

    # Interior unscored rep, then a contiguous analyzed run, then two tail
    # photos: only the trailing pair counts.
    photos = [
        {"id": 1, "quality_score": None, "flag": "none"},   # promoted, interior
        {"id": 2, "quality_score": 0.9, "flag": "none"},
        {"id": 3, "quality_score": 0.5, "flag": "none"},
        {"id": 4, "quality_score": None, "flag": "none"},   # tail
        {"id": 5, "quality_score": None, "flag": "none"},   # tail
    ]
    assert _bucket_unanalyzed_count(photos) == 2

    # No trailing tail at all — an analyzed photo at the end means the
    # divider must not fire.
    photos = [
        {"id": 1, "quality_score": None, "flag": "none"},
        {"id": 2, "quality_score": 0.9, "flag": "none"},
    ]
    assert _bucket_unanalyzed_count(photos) == 0


def test_rename_homonym_non_species_keyword_leaves_species_preferences(app_and_db):
    """Renaming an unrelated same-name keyword must not rewrite species prefs."""
    app, db = app_and_db
    client = app.test_client()
    species_kid = db.add_keyword("Robin", is_species=True)
    parent_kid = db.add_keyword("Places", kw_type="general")
    place_kid = db.add_keyword("Robin", parent_id=parent_kid, kw_type="general")
    rows = db.conn.execute("SELECT id FROM photos ORDER BY id LIMIT 2").fetchall()
    species_photo = rows[0]["id"]
    place_photo = rows[1]["id"]
    db.tag_photo(species_photo, species_kid)
    db.tag_photo(place_photo, place_kid)
    db.set_photo_preference("life_list", "Robin", species_photo)

    resp = client.put(f"/api/keywords/{place_kid}", json={"name": "Backyard Robin"})
    assert resp.status_code == 200

    assert db.get_photo_preferences("life_list") == {"Robin": species_photo}


def test_delete_keyword_queues_sidecar_removals(app_and_db):
    """Deleting a keyword queues removal pending changes for affected photos."""
    app, db = app_and_db
    client = app.test_client()
    kid = db.add_keyword("ToDelete")
    p1 = db.conn.execute("SELECT id FROM photos LIMIT 1").fetchone()["id"]
    db.tag_photo(p1, kid)
    db.conn.execute("DELETE FROM pending_changes")
    db.conn.commit()

    resp = client.delete(f"/api/keywords/{kid}")
    assert resp.status_code == 200

    changes = db.conn.execute(
        "SELECT change_type, value FROM pending_changes WHERE photo_id = ?",
        (p1,),
    ).fetchall()
    assert any(c["change_type"] == "keyword_remove" and c["value"] == "ToDelete" for c in changes)
    # Keyword should be gone
    assert db.conn.execute("SELECT id FROM keywords WHERE id = ?", (kid,)).fetchone() is None


def test_rename_with_invalid_type_queues_nothing(app_and_db):
    """PUT with invalid type + name returns 400 and queues no sidecar changes."""
    app, db = app_and_db
    client = app.test_client()
    kid = db.add_keyword("StableKeyword")
    p1 = db.conn.execute("SELECT id FROM photos LIMIT 1").fetchone()["id"]
    db.tag_photo(p1, kid)
    db.conn.execute("DELETE FROM pending_changes")
    db.conn.commit()

    resp = client.put(f"/api/keywords/{kid}", json={"name": "Renamed", "type": "invalid"})
    assert resp.status_code == 400

    # No sidecar changes should have been queued
    count = db.conn.execute(
        "SELECT COUNT(*) as cnt FROM pending_changes WHERE photo_id = ?", (p1,)
    ).fetchone()["cnt"]
    assert count == 0
    # Keyword name should be unchanged
    row = db.conn.execute("SELECT name FROM keywords WHERE id = ?", (kid,)).fetchone()
    assert row["name"] == "StableKeyword"


def test_rename_keyword_queues_for_all_workspaces(app_and_db):
    """Renaming a keyword queues sidecar changes for photos in all workspaces."""
    app, db = app_and_db
    client = app.test_client()

    # Create a second workspace with its own folder and photo
    ws2 = db.create_workspace("Second")
    fid2 = db.add_folder("/photos/ws2", name="ws2")
    db.add_workspace_folder(ws2, fid2)
    p_ws2 = db.add_photo(folder_id=fid2, filename="ws2bird.jpg", extension=".jpg",
                         file_size=100, file_mtime=1.0, timestamp="2024-01-01T00:00:00")

    # Tag photos in both workspaces with the same keyword
    kid = db.add_keyword("SharedBird")
    p_ws1 = db.conn.execute("SELECT id FROM photos LIMIT 1").fetchone()["id"]
    db.tag_photo(p_ws1, kid)
    db.tag_photo(p_ws2, kid)
    db.conn.execute("DELETE FROM pending_changes")
    db.conn.commit()

    # Rename keyword (active workspace is ws1)
    resp = client.put(f"/api/keywords/{kid}", json={"name": "RenamedBird"})
    assert resp.status_code == 200

    # Check pending changes for ws1 photo
    ws1_id = db._ws_id()
    ws1_changes = db.conn.execute(
        "SELECT change_type, value FROM pending_changes WHERE photo_id = ? AND workspace_id = ?",
        (p_ws1, ws1_id),
    ).fetchall()
    assert any(c["change_type"] == "keyword_remove" and c["value"] == "SharedBird" for c in ws1_changes)
    assert any(c["change_type"] == "keyword_add" and c["value"] == "RenamedBird" for c in ws1_changes)

    # Check pending changes for ws2 photo — should also be queued under ws2
    ws2_changes = db.conn.execute(
        "SELECT change_type, value FROM pending_changes WHERE photo_id = ? AND workspace_id = ?",
        (p_ws2, ws2),
    ).fetchall()
    assert any(c["change_type"] == "keyword_remove" and c["value"] == "SharedBird" for c in ws2_changes)
    assert any(c["change_type"] == "keyword_add" and c["value"] == "RenamedBird" for c in ws2_changes)


def test_delete_keyword_queues_for_all_workspaces(app_and_db):
    """Deleting a keyword queues sidecar removals for photos in all workspaces."""
    app, db = app_and_db
    client = app.test_client()

    ws2 = db.create_workspace("Second")
    fid2 = db.add_folder("/photos/ws2del", name="ws2del")
    db.add_workspace_folder(ws2, fid2)
    p_ws2 = db.add_photo(folder_id=fid2, filename="ws2del.jpg", extension=".jpg",
                         file_size=100, file_mtime=1.0, timestamp="2024-01-01T00:00:00")

    kid = db.add_keyword("SharedDelete")
    p_ws1 = db.conn.execute("SELECT id FROM photos LIMIT 1").fetchone()["id"]
    db.tag_photo(p_ws1, kid)
    db.tag_photo(p_ws2, kid)
    db.conn.execute("DELETE FROM pending_changes")
    db.conn.commit()

    resp = client.delete(f"/api/keywords/{kid}")
    assert resp.status_code == 200

    ws1_id = db._ws_id()
    ws1_changes = db.conn.execute(
        "SELECT change_type, value FROM pending_changes WHERE photo_id = ? AND workspace_id = ?",
        (p_ws1, ws1_id),
    ).fetchall()
    assert any(c["change_type"] == "keyword_remove" and c["value"] == "SharedDelete" for c in ws1_changes)

    ws2_changes = db.conn.execute(
        "SELECT change_type, value FROM pending_changes WHERE photo_id = ? AND workspace_id = ?",
        (p_ws2, ws2),
    ).fetchall()
    assert any(c["change_type"] == "keyword_remove" and c["value"] == "SharedDelete" for c in ws2_changes)


def test_shortcuts_page(app_and_db):
    """GET /shortcuts returns 200."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get('/shortcuts')
    assert resp.status_code == 200


def test_shortcuts_link_in_navbar(app_and_db):
    """The navbar includes a link to /shortcuts."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get('/shortcuts')
    assert b'/shortcuts' in resp.data
    assert b'Shortcuts' in resp.data


def test_settings_no_shortcuts_editor(app_and_db):
    """Settings page no longer contains the shortcuts editor."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get('/settings')
    html = resp.data.decode()
    assert 'shortcutsEditor' not in html


def test_shortcuts_cheat_sheet_in_navbar(app_and_db):
    """Every page includes the shortcuts cheat sheet overlay."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get('/browse')
    html = resp.data.decode()
    assert 'shortcutsCheatSheet' in html


def test_api_browse_home(app_and_db, tmp_path, monkeypatch):
    """GET /api/browse without path returns home directory listing."""
    monkeypatch.setenv("HOME", str(tmp_path))
    sub = tmp_path / "Documents"
    sub.mkdir()
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get('/api/browse')
    assert resp.status_code == 200
    data = resp.get_json()
    assert data['path'] == str(tmp_path)
    names = [d['name'] for d in data['dirs']]
    assert 'Documents' in names


def test_api_browse_with_path(app_and_db, tmp_path):
    """GET /api/browse?path=... returns subdirectories."""
    parent = tmp_path / "photos"
    parent.mkdir()
    (parent / "2024").mkdir()
    (parent / "2025").mkdir()
    (parent / "file.txt").write_text("hi")  # should not appear
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get(f'/api/browse?path={parent}')
    assert resp.status_code == 200
    data = resp.get_json()
    assert data['path'] == str(parent)
    names = [d['name'] for d in data['dirs']]
    assert '2024' in names
    assert '2025' in names
    assert 'file.txt' not in names


def test_api_browse_hides_dotfiles(app_and_db, tmp_path):
    """GET /api/browse hides dot-prefixed directories."""
    (tmp_path / ".hidden").mkdir()
    (tmp_path / "visible").mkdir()
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get(f'/api/browse?path={tmp_path}')
    data = resp.get_json()
    names = [d['name'] for d in data['dirs']]
    assert 'visible' in names
    assert '.hidden' not in names


def test_api_browse_invalid_path(app_and_db):
    """GET /api/browse with invalid path returns 400."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get('/api/browse?path=/nonexistent/path/xyz')
    assert resp.status_code == 400


def test_api_browse_rejects_macos_other_app_bundle_root(app_and_db, tmp_path, monkeypatch):
    """GET /api/browse must reject an app-managed bundle root BEFORE ``os.path.isdir``.

    The folder picker passes the user-selected path straight to this endpoint;
    ``os.path.isdir`` on a Photos or Music Library bundle (or a symlink to one)
    trips the macOS "access data from other apps" TCC prompt the exclusion guards
    exist to avoid, so the check has to happen first. Monkey-patch
    ``os.path.isdir`` to fail loudly if it ever runs on the bundle path.
    """
    bundles = [
        tmp_path / "Photos Library.photoslibrary",
        tmp_path / "Music Library.musiclibrary",
    ]
    for bundle in bundles:
        bundle.mkdir()

    real_isdir = os.path.isdir

    def guarded_isdir(p):
        if any(str(p) == str(bundle) for bundle in bundles):
            raise AssertionError(f"os.path.isdir called on excluded bundle: {p}")
        return real_isdir(p)

    monkeypatch.setattr(os.path, "isdir", guarded_isdir)
    app, _ = app_and_db
    client = app.test_client()
    for bundle in bundles:
        resp = client.get(f'/api/browse?path={bundle}')
        assert resp.status_code == 400


def test_api_browse_skips_macos_other_app_bundle_children(app_and_db, tmp_path, monkeypatch):
    """GET /api/browse must omit app-managed bundle children from the listing
    without stat'ing them.

    When the picker opens ``~/Pictures``, this endpoint enumerates every child
    and would normally call ``os.path.isdir`` on each. For a sibling like
    ``Photos Library.photoslibrary`` or ``Music Library.musiclibrary`` that
    stat itself trips the macOS TCC prompt; verify the guard skips it before
    any stat and that regular siblings are still returned.
    """
    parent = tmp_path / "pictures"
    parent.mkdir()
    (parent / "real").mkdir()
    photos_bundle = parent / "Photos Library.photoslibrary"
    photos_bundle.mkdir()
    (photos_bundle / "originals").mkdir()
    music_bundle = parent / "Music Library.musiclibrary"
    music_bundle.mkdir()
    (music_bundle / "Media.localized").mkdir()
    bundles = [photos_bundle, music_bundle]

    real_isdir = os.path.isdir

    def guarded_isdir(p):
        if any(str(p).startswith(str(bundle)) for bundle in bundles):
            raise AssertionError(f"os.path.isdir called on excluded bundle child: {p}")
        return real_isdir(p)

    monkeypatch.setattr(os.path, "isdir", guarded_isdir)
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get(f'/api/browse?path={parent}')
    assert resp.status_code == 200
    names = [d['name'] for d in resp.get_json()['dirs']]
    assert 'real' in names
    assert 'Photos Library.photoslibrary' not in names
    assert 'Music Library.musiclibrary' not in names


def test_api_browse_mkdir(app_and_db, tmp_path):
    """POST /api/browse/mkdir creates a new directory."""
    new_dir = str(tmp_path / "new_folder")
    app, _ = app_and_db
    client = app.test_client()
    resp = client.post('/api/browse/mkdir',
                       json={"path": new_dir},
                       content_type='application/json')
    assert resp.status_code == 200
    data = resp.get_json()
    assert data['name'] == 'new_folder'
    assert data['path'] == new_dir
    assert os.path.isdir(new_dir)


def test_api_browse_mkdir_nested(app_and_db, tmp_path):
    """POST /api/browse/mkdir creates nested directories."""
    new_dir = str(tmp_path / "a" / "b" / "c")
    app, _ = app_and_db
    client = app.test_client()
    resp = client.post('/api/browse/mkdir',
                       json={"path": new_dir},
                       content_type='application/json')
    assert resp.status_code == 200
    assert os.path.isdir(new_dir)


def test_api_browse_mkdir_relative_path(app_and_db):
    """POST /api/browse/mkdir rejects relative paths."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.post('/api/browse/mkdir',
                       json={"path": "relative/path"},
                       content_type='application/json')
    assert resp.status_code == 400


def test_api_browse_mkdir_missing_path(app_and_db):
    """POST /api/browse/mkdir rejects missing path."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.post('/api/browse/mkdir',
                       json={},
                       content_type='application/json')
    assert resp.status_code == 400


def test_api_browse_photo_counts_recursive(app_and_db, tmp_path):
    """POST /api/browse/photo-counts returns recursive photo counts per path."""
    # Folder with photos at root
    a = tmp_path / "a"
    a.mkdir()
    (a / "one.jpg").write_bytes(b"x")
    (a / "two.jpg").write_bytes(b"x")
    # Folder with photos only in subfolder (recursive must find them)
    b = tmp_path / "b"
    b.mkdir()
    (b / "nested").mkdir()
    (b / "nested" / "deep.jpg").write_bytes(b"x")
    # Folder with no photos
    c = tmp_path / "c"
    c.mkdir()
    (c / "readme.txt").write_text("hi")

    app, _ = app_and_db
    client = app.test_client()
    resp = client.post('/api/browse/photo-counts',
                       json={"paths": [str(a), str(b), str(c)],
                             "file_types": [".jpg"]},
                       content_type='application/json')
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["counts"][str(a)] == 2
    assert data["counts"][str(b)] == 1
    assert data["counts"][str(c)] == 0


def test_api_browse_photo_counts_empty_paths(app_and_db):
    """POST /api/browse/photo-counts with empty paths returns empty counts."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.post('/api/browse/photo-counts',
                       json={"paths": [], "file_types": [".jpg"]},
                       content_type='application/json')
    assert resp.status_code == 200
    assert resp.get_json()["counts"] == {}


def test_api_browse_photo_counts_skips_missing(app_and_db, tmp_path):
    """POST /api/browse/photo-counts tolerates paths that don't exist."""
    real = tmp_path / "real"
    real.mkdir()
    (real / "img.jpg").write_bytes(b"x")
    missing = str(tmp_path / "does_not_exist")

    app, _ = app_and_db
    client = app.test_client()
    resp = client.post('/api/browse/photo-counts',
                       json={"paths": [str(real), missing],
                             "file_types": [".jpg"]},
                       content_type='application/json')
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["counts"][str(real)] == 1
    assert data["counts"][missing] == 0


def test_api_browse_photo_counts_skips_non_string_entries(app_and_db, tmp_path):
    """POST /api/browse/photo-counts skips non-string path entries (no 500)."""
    real = tmp_path / "real"
    real.mkdir()
    (real / "img.jpg").write_bytes(b"x")

    app, _ = app_and_db
    client = app.test_client()
    resp = client.post('/api/browse/photo-counts',
                       json={"paths": [str(real), {}, [], 42, None],
                             "file_types": [".jpg"]},
                       content_type='application/json')
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["counts"] == {str(real): 1}


def test_api_browse_photo_counts_skips_macos_other_app_bundles(app_and_db, tmp_path):
    """POST /api/browse/photo-counts must reject macOS app-managed library
    bundles (``.photoslibrary`` etc.) BEFORE ``os.path.isdir`` runs.

    The folder browser fires this endpoint with every child of the picker
    root the moment it opens — for ``~/Pictures`` that includes ``Photos
    Library.photoslibrary``. ``os.path.isdir`` on that path itself trips
    the macOS "access data from other apps" TCC prompt the exclusion
    guards exist to avoid, so the check has to happen first.
    """
    real = tmp_path / "real"
    real.mkdir()
    (real / "img.jpg").write_bytes(b"x")
    bundle = tmp_path / "Photos Library.photoslibrary"
    bundle.mkdir()
    (bundle / "originals").mkdir()
    (bundle / "originals" / "managed.jpg").write_bytes(b"x")
    nested = bundle / "originals"

    app, _ = app_and_db
    client = app.test_client()
    resp = client.post(
        '/api/browse/photo-counts',
        json={
            "paths": [str(real), str(bundle), str(nested)],
            "file_types": [".jpg"],
        },
        content_type='application/json',
    )
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["counts"][str(real)] == 1
    # Bundle root and a path nested inside it must both report 0 without
    # recursing into the managed contents.
    assert data["counts"][str(bundle)] == 0
    assert data["counts"][str(nested)] == 0


def test_api_browse_photo_counts_respects_file_types(app_and_db, tmp_path):
    """POST /api/browse/photo-counts only counts files matching requested types."""
    d = tmp_path / "mixed"
    d.mkdir()
    (d / "a.jpg").write_bytes(b"x")
    (d / "b.nef").write_bytes(b"x")
    (d / "c.txt").write_text("hi")

    app, _ = app_and_db
    client = app.test_client()
    # Only request .nef
    resp = client.post('/api/browse/photo-counts',
                       json={"paths": [str(d)], "file_types": [".nef"]},
                       content_type='application/json')
    assert resp.status_code == 200
    assert resp.get_json()["counts"][str(d)] == 1


def _read_workspace_overrides(db, ws_id):
    """Helper: read and JSON-decode the config_overrides column for ws_id."""
    import json
    ws = db.get_workspace(ws_id)
    raw = ws["config_overrides"] if ws else None
    if not raw:
        return {}
    return json.loads(raw) if isinstance(raw, str) else raw


def test_put_subject_types_persists_valid_values(app_and_db):
    """PUT /api/workspaces/<id>/subject-types persists requested types."""
    app, db = app_and_db
    ws_id = db.create_workspace("ws-subject-1")
    client = app.test_client()
    resp = client.put(
        f"/api/workspaces/{ws_id}/subject-types",
        json={"types": ["taxonomy", "genre"]},
        content_type="application/json",
    )
    assert resp.status_code == 200
    body = resp.get_json()
    assert set(body["types"]) == {"taxonomy", "genre"}
    overrides = _read_workspace_overrides(db, ws_id)
    assert set(overrides.get("subject_types", [])) == {"taxonomy", "genre"}


def test_put_subject_types_drops_unknown_values(app_and_db):
    """Unknown type values are dropped silently (logged)."""
    app, db = app_and_db
    ws_id = db.create_workspace("ws-subject-2")
    client = app.test_client()
    resp = client.put(
        f"/api/workspaces/{ws_id}/subject-types",
        json={"types": ["taxonomy", "bogus"]},
        content_type="application/json",
    )
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["types"] == ["taxonomy"]
    overrides = _read_workspace_overrides(db, ws_id)
    assert overrides.get("subject_types") == ["taxonomy"]


def test_put_subject_types_empty_list_allowed(app_and_db):
    """Empty list is allowed (effectively disables the queue's filter)."""
    app, db = app_and_db
    ws_id = db.create_workspace("ws-subject-3")
    client = app.test_client()
    resp = client.put(
        f"/api/workspaces/{ws_id}/subject-types",
        json={"types": []},
        content_type="application/json",
    )
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["types"] == []
    overrides = _read_workspace_overrides(db, ws_id)
    assert overrides.get("subject_types") == []


def test_put_subject_types_rejects_non_list(app_and_db):
    """Non-list 'types' value returns 400."""
    app, db = app_and_db
    ws_id = db.create_workspace("ws-subject-4")
    client = app.test_client()
    resp = client.put(
        f"/api/workspaces/{ws_id}/subject-types",
        json={"types": "not-a-list"},
        content_type="application/json",
    )
    assert resp.status_code == 400


def test_put_subject_types_preserves_other_overrides(app_and_db):
    """Setting subject_types must not clobber other config_overrides keys."""
    app, db = app_and_db
    ws_id = db.create_workspace("ws-subject-5")
    db.update_workspace(ws_id, config_overrides={"classification_threshold": 0.42})
    client = app.test_client()
    resp = client.put(
        f"/api/workspaces/{ws_id}/subject-types",
        json={"types": ["taxonomy"]},
        content_type="application/json",
    )
    assert resp.status_code == 200
    overrides = _read_workspace_overrides(db, ws_id)
    assert overrides.get("subject_types") == ["taxonomy"]
    assert overrides.get("classification_threshold") == 0.42


def test_get_active_subject_types_returns_effective_config(app_and_db, tmp_path, monkeypatch):
    """Regression: GET /api/workspaces/active/subject-types returns the
    EFFECTIVE config (global merged with workspace overrides), not just
    the override JSON. The settings UI needs this so checkboxes match
    actual behavior even when only the global config has been customized."""
    import config as cfg
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "global-config.json"))
    # Customize ONLY the global config (no workspace override). The
    # endpoint must return ["taxonomy"], not the hardcoded default
    # ["taxonomy", "individual", "genre"].
    cfg.set("subject_types", ["taxonomy"])
    app, db = app_and_db
    client = app.test_client()
    resp = client.get("/api/workspaces/active/subject-types")
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["types"] == ["taxonomy"], (
        "Endpoint must merge global config — workspace settings UI would "
        "otherwise render wrong checkbox state when only global is set."
    )


def test_put_subject_types_drops_non_string_entries(app_and_db):
    """Regression: non-string JSON entries (e.g. nested lists/objects) must
    be dropped, not crash with TypeError on `x in frozenset`."""
    app, db = app_and_db
    ws_id = db.create_workspace("ws-subject-malformed")
    client = app.test_client()
    resp = client.put(
        f"/api/workspaces/{ws_id}/subject-types",
        json={"types": ["taxonomy", ["nested"], {"obj": 1}, 42, None, "genre"]},
        content_type="application/json",
    )
    assert resp.status_code == 200, (
        f"Expected non-string entries to be dropped, got {resp.status_code}: "
        f"{resp.get_data(as_text=True)}"
    )
    body = resp.get_json()
    assert set(body["types"]) == {"taxonomy", "genre"}


def test_put_subject_types_normalizes_non_dict_config_overrides(app_and_db):
    """Regression: PUT /api/workspaces/<id>/subject-types must not 500 when
    `config_overrides` was previously persisted as a non-dict JSON value.
    The column is arbitrary JSON (api_update_workspace stores whatever the
    client sends), so a list/string/number can sit there from a prior PUT.
    The endpoint must coerce it back to {} before assigning subject_types."""
    app, db = app_and_db
    ws_id = db.create_workspace("ws-subject-non-dict")
    # Plant a list-shaped config_overrides directly via update_workspace,
    # which json.dumps()es whatever it receives.
    db.update_workspace(ws_id, config_overrides=["unexpected", "list"])
    client = app.test_client()
    resp = client.put(
        f"/api/workspaces/{ws_id}/subject-types",
        json={"types": ["taxonomy"]},
        content_type="application/json",
    )
    assert resp.status_code == 200, (
        f"Non-dict config_overrides must be normalized, not crash: "
        f"{resp.status_code} {resp.get_data(as_text=True)}"
    )
    overrides = _read_workspace_overrides(db, ws_id)
    assert isinstance(overrides, dict)
    assert overrides.get("subject_types") == ["taxonomy"]


def test_add_keyword_route_handles_non_string_type(app_and_db):
    """Regression: POST /api/photos/<id>/keywords with a non-string `type`
    JSON value must not 500 — the bad value should be ignored and the
    keyword still created with its default type."""
    app, db = app_and_db
    folder_id = db.add_folder("/tmp/p")
    db.add_workspace_folder(db._active_workspace_id, folder_id)
    photo_id = db.add_photo(
        folder_id, "p.jpg", extension=".jpg", file_size=1, file_mtime=1.0,
    )
    client = app.test_client()
    resp = client.post(
        f"/api/photos/{photo_id}/keywords",
        json={"name": "MaybeWildlife", "type": []},  # bogus list value
        content_type="application/json",
    )
    assert resp.status_code == 200, (
        f"Non-string type must not 500 the route: "
        f"{resp.status_code} {resp.get_data(as_text=True)}"
    )
    # The keyword should exist; its type wasn't overridden by the bad value.
    row = db.conn.execute(
        "SELECT type FROM keywords WHERE name = 'MaybeWildlife'"
    ).fetchone()
    assert row is not None
    # add_keyword's auto-detect runs in the absence of a valid override.
    # Anything other than a crash is acceptable here; the point is no 500.


def test_wildlife_excluded_route_does_not_add_landscape_keyword(app_and_db):
    """Not Wildlife is workflow state, not a hidden Landscape keyword."""
    app, db = app_and_db
    folder_id = db.add_folder("/tmp/p")
    db.add_workspace_folder(db._active_workspace_id, folder_id)
    photo_id = db.add_photo(
        folder_id, "p.jpg", extension=".jpg", file_size=1, file_mtime=1.0,
    )
    client = app.test_client()

    resp = client.post(
        f"/api/photos/{photo_id}/wildlife_excluded",
        json={"excluded": True},
        content_type="application/json",
    )

    assert resp.status_code == 200
    assert resp.get_json()["wildlife_excluded"] is True
    row = db.conn.execute(
        "SELECT wildlife_excluded FROM photos WHERE id = ?", (photo_id,)
    ).fetchone()
    assert row["wildlife_excluded"] == 1
    keywords = [dict(k) for k in db.get_photo_keywords(photo_id)]
    assert all(k["name"] != "Landscape" for k in keywords)


def test_batch_keyword_route_handles_non_string_type(app_and_db):
    """Same regression for the batch endpoint."""
    app, db = app_and_db
    folder_id = db.add_folder("/tmp/q")
    db.add_workspace_folder(db._active_workspace_id, folder_id)
    photo_id = db.add_photo(
        folder_id, "q.jpg", extension=".jpg", file_size=1, file_mtime=1.0,
    )
    client = app.test_client()
    resp = client.post(
        "/api/batch/keyword",
        json={"photo_ids": [photo_id], "name": "BatchTag", "type": {"x": 1}},
        content_type="application/json",
    )
    assert resp.status_code == 200, (
        f"Non-string type must not 500 the batch route: "
        f"{resp.status_code} {resp.get_data(as_text=True)}"
    )


def test_add_keyword_route_rejects_name_that_normalizes_to_empty(app_and_db):
    """Names like `'` are non-empty as raw text (so the `not name` guard
    passes) but strip to '' during normalization. The route must return a
    clean 400 rather than 500-ing on add_keyword's ValueError, and must not
    insert an empty keyword row."""
    app, db = app_and_db
    folder_id = db.add_folder("/tmp/pe")
    db.add_workspace_folder(db._active_workspace_id, folder_id)
    photo_id = db.add_photo(
        folder_id, "pe.jpg", extension=".jpg", file_size=1, file_mtime=1.0,
    )
    client = app.test_client()
    before = db.conn.execute("SELECT COUNT(*) FROM keywords").fetchone()[0]
    for empty in ("'", '"', "‘", "“”"):
        resp = client.post(
            f"/api/photos/{photo_id}/keywords",
            json={"name": empty},
            content_type="application/json",
        )
        assert resp.status_code == 400, (
            f"expected 400 for name={empty!r}, got {resp.status_code} "
            f"{resp.get_data(as_text=True)}"
        )
    after = db.conn.execute("SELECT COUNT(*) FROM keywords").fetchone()[0]
    assert after == before
    assert (
        db.conn.execute("SELECT COUNT(*) FROM keywords WHERE name = ''").fetchone()[0]
        == 0
    )


def test_batch_keyword_route_rejects_name_that_normalizes_to_empty(app_and_db):
    """Same normalized-empty guard on the batch endpoint."""
    app, db = app_and_db
    folder_id = db.add_folder("/tmp/qe")
    db.add_workspace_folder(db._active_workspace_id, folder_id)
    photo_id = db.add_photo(
        folder_id, "qe.jpg", extension=".jpg", file_size=1, file_mtime=1.0,
    )
    client = app.test_client()
    before = db.conn.execute("SELECT COUNT(*) FROM keywords").fetchone()[0]
    resp = client.post(
        "/api/batch/keyword",
        json={"photo_ids": [photo_id], "name": "‘"},
        content_type="application/json",
    )
    assert resp.status_code == 400, (
        f"expected 400 for edge-quote-only name, got {resp.status_code} "
        f"{resp.get_data(as_text=True)}"
    )
    assert (
        db.conn.execute("SELECT COUNT(*) FROM keywords").fetchone()[0] == before
    )


def test_add_keyword_route_queues_stored_name_after_normalization(app_and_db):
    """When a stray-quote name is submitted, the pending-change queue must
    record the stored (normalized) name — not the raw request string —
    so a later delete (which reads k.name from the DB) queues the same
    string and the add/remove pair cancels instead of both persisting to
    XMP."""
    app, db = app_and_db
    folder_id = db.add_folder("/tmp/qn")
    db.add_workspace_folder(db._active_workspace_id, folder_id)
    photo_id = db.add_photo(
        folder_id, "qn.jpg", extension=".jpg", file_size=1, file_mtime=1.0,
    )
    db.conn.execute("DELETE FROM pending_changes")
    db.conn.commit()
    client = app.test_client()

    resp = client.post(
        f"/api/photos/{photo_id}/keywords",
        json={"name": "‘quailquail"},
        content_type="application/json",
    )
    assert resp.status_code == 200
    kid = resp.get_json()["keyword_id"]
    stored = db.conn.execute(
        "SELECT name FROM keywords WHERE id = ?", (kid,)
    ).fetchone()["name"]
    assert stored == "quailquail"

    queued_add = db.conn.execute(
        "SELECT value FROM pending_changes "
        "WHERE photo_id = ? AND change_type = 'keyword_add'",
        (photo_id,),
    ).fetchall()
    assert [row["value"] for row in queued_add] == [stored], (
        "keyword_add queue must record the stored name so it matches what a "
        "later keyword_remove would queue via k.name"
    )

    resp = client.delete(f"/api/photos/{photo_id}/keywords/{kid}")
    assert resp.status_code == 200
    remaining = db.conn.execute(
        "SELECT change_type, value FROM pending_changes WHERE photo_id = ?",
        (photo_id,),
    ).fetchall()
    assert remaining == [], (
        f"add/remove should cancel, but pending_changes still has "
        f"{[dict(r) for r in remaining]}"
    )


def test_batch_keyword_route_queues_stored_name_after_normalization(app_and_db):
    """Batch endpoint must queue the stored keyword name too, so the
    per-photo add/remove pair cancels the same way the single endpoint does."""
    app, db = app_and_db
    folder_id = db.add_folder("/tmp/qb")
    db.add_workspace_folder(db._active_workspace_id, folder_id)
    photo_id = db.add_photo(
        folder_id, "qb.jpg", extension=".jpg", file_size=1, file_mtime=1.0,
    )
    db.conn.execute("DELETE FROM pending_changes")
    db.conn.commit()
    client = app.test_client()

    resp = client.post(
        "/api/batch/keyword",
        json={"photo_ids": [photo_id], "name": "‘quailquail"},
        content_type="application/json",
    )
    assert resp.status_code == 200
    kid = db.conn.execute(
        "SELECT keyword_id FROM photo_keywords WHERE photo_id = ?", (photo_id,)
    ).fetchone()["keyword_id"]
    stored = db.conn.execute(
        "SELECT name FROM keywords WHERE id = ?", (kid,)
    ).fetchone()["name"]
    assert stored == "quailquail"

    queued_add = db.conn.execute(
        "SELECT value FROM pending_changes "
        "WHERE photo_id = ? AND change_type = 'keyword_add'",
        (photo_id,),
    ).fetchall()
    assert [row["value"] for row in queued_add] == [stored]

    resp = client.delete(f"/api/photos/{photo_id}/keywords/{kid}")
    assert resp.status_code == 200
    remaining = db.conn.execute(
        "SELECT change_type, value FROM pending_changes WHERE photo_id = ?",
        (photo_id,),
    ).fetchall()
    assert remaining == []


def test_selection_keyword_suggestions_return_partial_and_shared_keywords(app_and_db):
    """Multi-select keyword data includes both partial and shared keywords."""
    app, db = app_and_db
    ids = [
        row["id"]
        for row in db.conn.execute(
            "SELECT id FROM photos ORDER BY filename"
        ).fetchall()
    ]
    client = app.test_client()

    resp = client.post(
        "/api/selection/keyword-suggestions",
        json={"photo_ids": ids},
        content_type="application/json",
    )

    assert resp.status_code == 200
    data = resp.get_json()
    by_name = {item["name"]: item for item in data["suggestions"]}
    assert by_name["Cardinal"]["count"] == 1
    assert by_name["Cardinal"]["missing_count"] == 2
    assert sorted(by_name["Cardinal"]["missing_photo_ids"]) == sorted(ids[1:])
    assert by_name["Sparrow"]["count"] == 1
    assert by_name["Sparrow"]["missing_count"] == 2

    shared_id = db.add_keyword("Shared selection keyword")
    for photo_id in ids:
        db.tag_photo(photo_id, shared_id)
    resp = client.post(
        "/api/selection/keyword-suggestions",
        json={"photo_ids": ids},
        content_type="application/json",
    )
    assert resp.status_code == 200
    data = resp.get_json()
    keywords_by_name = {item["name"]: item for item in data["keywords"]}
    assert keywords_by_name["Cardinal"]["present_photo_ids"] == [ids[0]]
    assert sorted(keywords_by_name["Cardinal"]["missing_photo_ids"]) == sorted(ids[1:])
    assert keywords_by_name["Sparrow"]["present_photo_ids"] == [ids[1]]
    assert keywords_by_name["Shared selection keyword"] == {
        "id": shared_id,
        "name": "Shared selection keyword",
        "type": "general",
        "count": len(ids),
        "missing_count": 0,
        "present_photo_ids": ids,
        "missing_photo_ids": [],
    }


def test_selection_keyword_suggestions_chunks_large_selection(app_and_db):
    """Large selection suggestions must not exceed SQLite's variable limit."""
    app, db = app_and_db
    folder_id = db.get_folder_tree()[0]["id"]
    ids = [
        db.add_photo(
            folder_id,
            f"large-suggestion-{idx}.jpg",
            extension=".jpg",
            file_size=1,
            file_mtime=1.0,
        )
        for idx in range(1000)
    ]
    keyword_id = db.add_keyword("Large Suggestion")
    db.tag_photo(ids[0], keyword_id)
    client = app.test_client()

    resp = client.post(
        "/api/selection/keyword-suggestions",
        json={"photo_ids": ids},
        content_type="application/json",
    )

    assert resp.status_code == 200
    by_name = {item["name"]: item for item in resp.get_json()["suggestions"]}
    assert by_name["Large Suggestion"]["count"] == 1
    assert by_name["Large Suggestion"]["missing_count"] == 999
    assert len(by_name["Large Suggestion"]["missing_photo_ids"]) == 999


def test_batch_keyword_route_accepts_existing_keyword_id(app_and_db):
    """The fill-missing-keywords button preserves pre-existing links on undo."""
    app, db = app_and_db
    rows = db.conn.execute(
        "SELECT id, filename FROM photos ORDER BY filename"
    ).fetchall()
    ids = [row["id"] for row in rows]
    cardinal_id = db.conn.execute(
        "SELECT id FROM keywords WHERE name = 'Cardinal'"
    ).fetchone()["id"]
    client = app.test_client()

    resp = client.post(
        "/api/batch/keyword",
        json={"photo_ids": ids, "keyword_id": cardinal_id},
        content_type="application/json",
    )

    assert resp.status_code == 200
    assert resp.get_json()["updated"] == 2
    tagged = db.conn.execute(
        """SELECT photo_id FROM photo_keywords
           WHERE keyword_id = ?
           ORDER BY photo_id""",
        (cardinal_id,),
    ).fetchall()
    assert [row["photo_id"] for row in tagged] == sorted(ids)

    undo_resp = client.post("/api/undo")
    assert undo_resp.status_code == 200
    tagged_after_undo = db.conn.execute(
        """SELECT photo_id FROM photo_keywords
           WHERE keyword_id = ?
           ORDER BY photo_id""",
        (cardinal_id,),
    ).fetchall()
    assert [row["photo_id"] for row in tagged_after_undo] == [ids[0]]


def test_batch_keyword_remove_route_removes_existing_keyword_id(app_and_db):
    """Selected-keyword removal should only affect selected photos that have it."""
    app, db = app_and_db
    rows = db.conn.execute(
        "SELECT id, filename FROM photos ORDER BY filename"
    ).fetchall()
    ids = [row["id"] for row in rows]
    cardinal_id = db.conn.execute(
        "SELECT id FROM keywords WHERE name = 'Cardinal'"
    ).fetchone()["id"]
    client = app.test_client()

    resp = client.post(
        "/api/batch/keyword-remove",
        json={"photo_ids": ids, "keyword_id": cardinal_id},
        content_type="application/json",
    )

    assert resp.status_code == 200
    assert resp.get_json()["updated"] == 1
    tagged = db.conn.execute(
        """SELECT photo_id FROM photo_keywords
           WHERE keyword_id = ?""",
        (cardinal_id,),
    ).fetchall()
    assert tagged == []

    pending = db.conn.execute(
        """SELECT photo_id, change_type, value FROM pending_changes
           WHERE change_type = 'keyword_remove' AND value = 'Cardinal'"""
    ).fetchall()
    assert [row["photo_id"] for row in pending] == [ids[0]]

    undo_resp = client.post("/api/undo")
    assert undo_resp.status_code == 200
    tagged_after_undo = db.conn.execute(
        """SELECT photo_id FROM photo_keywords
           WHERE keyword_id = ?
           ORDER BY photo_id""",
        (cardinal_id,),
    ).fetchall()
    assert [row["photo_id"] for row in tagged_after_undo] == [ids[0]]


def test_batch_keyword_remove_cancels_pending_add_queued_with_variant_spelling(app_and_db):
    """Batch remove must cancel a pending add that was queued via a
    stray-quote spelling variant.

    Keyword normalization happens at the choke points: POSTing `‘apapane`
    stores both the keyword row and its pending `keyword_add` under the
    clean spelling `apapane`. A later batch remove of the clean keyword
    must find and cancel that still-unsynced pending add instead of
    queuing a `keyword_remove` alongside it — otherwise the next XMP sync
    would see a remove for a keyword that was never written.
    """
    app, db = app_and_db
    rows = db.conn.execute(
        "SELECT id, filename FROM photos ORDER BY filename"
    ).fetchall()
    ids = [row["id"] for row in rows]
    client = app.test_client()

    # Queue the pending add through the API with a variant spelling.
    resp = client.post(
        f"/api/photos/{ids[1]}/keywords",
        json={"name": "‘apapane"},
        content_type="application/json",
    )
    assert resp.status_code == 200
    kid = resp.get_json()["keyword_id"]
    stored = db.conn.execute(
        "SELECT name FROM keywords WHERE id = ?", (kid,)
    ).fetchone()["name"]
    assert stored == "apapane"

    # The pending change is stored under the clean spelling, not the raw
    # request variant.
    pending = db.conn.execute(
        "SELECT value FROM pending_changes "
        "WHERE photo_id = ? AND change_type = 'keyword_add'",
        (ids[1],),
    ).fetchall()
    assert [row["value"] for row in pending] == ["apapane"]

    resp = client.post(
        "/api/batch/keyword-remove",
        json={"photo_ids": ids, "keyword_id": kid},
        content_type="application/json",
    )
    assert resp.status_code == 200
    assert resp.get_json()["updated"] == 1

    still_tagged = db.conn.execute(
        "SELECT photo_id FROM photo_keywords WHERE keyword_id = ?",
        (kid,),
    ).fetchall()
    assert still_tagged == []

    remaining = db.conn.execute(
        "SELECT change_type, value FROM pending_changes "
        "WHERE photo_id = ? AND value = 'apapane'",
        (ids[1],),
    ).fetchall()
    assert remaining == [], (
        "the pending add should be cancelled, not left alongside a remove"
    )


def test_batch_keyword_remove_undo_restores_pending_add(app_and_db):
    """Add → bulk remove → undo must leave a pending sidecar write.

    Bulk remove of a not-yet-synced pending add cancels the pending
    `keyword_add` (via `_queue_keyword_remove`) instead of queuing a
    `keyword_remove`. Undoing the recorded `keyword_remove` retags the
    photo but must also re-queue the `keyword_add` so the restored
    keyword is actually written back to the sidecar; otherwise the tag
    silently diverges from disk.
    """
    app, db = app_and_db
    photo_id = db.conn.execute(
        "SELECT id FROM photos ORDER BY filename LIMIT 1"
    ).fetchone()["id"]
    sparrow_id = db.conn.execute(
        "SELECT id FROM keywords WHERE name = 'Sparrow'"
    ).fetchone()["id"]
    client = app.test_client()

    add_resp = client.post(
        "/api/batch/keyword",
        json={"photo_ids": [photo_id], "keyword_id": sparrow_id},
        content_type="application/json",
    )
    assert add_resp.status_code == 200
    pending_add = db.conn.execute(
        """SELECT photo_id FROM pending_changes
           WHERE change_type = 'keyword_add' AND value = 'Sparrow'"""
    ).fetchall()
    assert [row["photo_id"] for row in pending_add] == [photo_id]

    remove_resp = client.post(
        "/api/batch/keyword-remove",
        json={"photo_ids": [photo_id], "keyword_id": sparrow_id},
        content_type="application/json",
    )
    assert remove_resp.status_code == 200
    pending_after_remove = db.conn.execute(
        """SELECT change_type FROM pending_changes
           WHERE photo_id = ? AND value = 'Sparrow'""",
        (photo_id,),
    ).fetchall()
    assert pending_after_remove == []

    undo_resp = client.post("/api/undo")
    assert undo_resp.status_code == 200
    tagged = db.conn.execute(
        "SELECT 1 FROM photo_keywords WHERE photo_id = ? AND keyword_id = ?",
        (photo_id, sparrow_id),
    ).fetchone()
    assert tagged is not None, "undo should restore the Sparrow tag"
    pending_after_undo = db.conn.execute(
        """SELECT change_type, value FROM pending_changes
           WHERE photo_id = ? AND value = 'Sparrow'""",
        (photo_id,),
    ).fetchall()
    assert [(row["change_type"], row["value"]) for row in pending_after_undo] == [
        ("keyword_add", "Sparrow"),
    ]

    redo_resp = client.post("/api/redo")
    assert redo_resp.status_code == 200
    tagged_after_redo = db.conn.execute(
        "SELECT 1 FROM photo_keywords WHERE photo_id = ? AND keyword_id = ?",
        (photo_id, sparrow_id),
    ).fetchone()
    assert tagged_after_redo is None, "redo should re-remove the Sparrow tag"
    pending_after_redo = db.conn.execute(
        """SELECT change_type FROM pending_changes
           WHERE photo_id = ? AND value = 'Sparrow'""",
        (photo_id,),
    ).fetchall()
    assert pending_after_redo == [], (
        "redo of the cancel-a-pending-add remove must leave no pending "
        "change — mirroring the original bulk remove that cancelled the add"
    )


def test_batch_keyword_route_chunks_large_existing_keyword_lookup(app_and_db):
    """Large batch keyword adds must not exceed SQLite's variable limit."""
    app, db = app_and_db
    folder_id = db.get_folder_tree()[0]["id"]
    ids = [
        db.add_photo(
            folder_id,
            f"large-batch-{idx}.jpg",
            extension=".jpg",
            file_size=1,
            file_mtime=1.0,
        )
        for idx in range(1005)
    ]
    client = app.test_client()

    resp = client.post(
        "/api/batch/keyword",
        json={"photo_ids": ids, "name": "Large Batch"},
        content_type="application/json",
    )

    assert resp.status_code == 200
    assert resp.get_json()["updated"] == len(ids)


def test_create_app_runs_wildlife_backfill_synchronously_on_first_boot(tmp_path, monkeypatch):
    """Regression: on first boot after upgrade (wildlife_backfill_done
    marker unset), create_app must complete the species-marking +
    Wildlife-backfill pipeline synchronously before returning, so user
    edits via the served HTTP API can't race with the one-shot backfill
    overwriting their Wildlife removals."""
    from unittest.mock import MagicMock, patch

    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    import config as cfg
    import models
    from app import create_app

    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    monkeypatch.setattr(models, "DEFAULT_MODELS_DIR", str(tmp_path / "vireo-models"))
    monkeypatch.setattr(models, "CONFIG_PATH", str(tmp_path / "models.json"))

    db_path = str(tmp_path / "test.db")
    thumb_dir = str(tmp_path / "thumbs")
    os.makedirs(thumb_dir)

    # Pre-create the DB with the marker UNSET (simulates first boot
    # post-upgrade).
    db = Database(db_path)
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    assert db.get_meta(Database._WILDLIFE_BACKFILL_DONE_KEY) != "1", (
        "Pre-condition: marker should be unset before create_app"
    )
    db.close()

    # Stub taxonomy loading to return a non-None object so the sync path
    # runs mark_species + backfill. mark_species_keywords gets a real DB
    # call but the stub taxonomy returns None for every lookup, so no
    # rows are actually changed — we just need it to complete without
    # raising so the marker gets set.
    fake_tax = MagicMock()
    fake_tax.lookup.return_value = None

    with patch("taxonomy.load_local_taxonomy", return_value=fake_tax):
        app = create_app(
            db_path=db_path, thumb_cache_dir=thumb_dir, api_token="test",
        )
        assert app is not None

    # After create_app returns, the marker MUST be set — the synchronous
    # path ran. (If it had been async, we'd be racing the background
    # thread here and the marker might or might not be set yet.)
    db2 = Database(db_path)
    assert db2.get_meta(Database._WILDLIFE_BACKFILL_DONE_KEY) == "1", (
        "Wildlife backfill marker must be set synchronously by create_app "
        "on first boot — otherwise user edits race the backfill."
    )
    db2.close()


def test_create_app_repairs_duplicate_species_after_taxonomy_marking(
    tmp_path, monkeypatch,
):
    """Startup must type/link legacy hierarchy leaves before stamping the
    one-shot duplicate-species repair marker."""
    from unittest.mock import MagicMock, patch

    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    import config as cfg
    import models
    from app import create_app

    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    monkeypatch.setattr(models, "DEFAULT_MODELS_DIR", str(tmp_path / "vireo-models"))
    monkeypatch.setattr(models, "CONFIG_PATH", str(tmp_path / "models.json"))

    db_path = str(tmp_path / "test.db")
    thumb_dir = str(tmp_path / "thumbs")
    os.makedirs(thumb_dir)
    db = Database(db_path)
    fid = db.add_folder("/photos", name="photos")
    pid = db.add_photo(
        folder_id=fid, filename="a.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    db.conn.execute(
        "INSERT INTO taxa (id, inat_id, name, common_name, rank) "
        "VALUES (2912, 2912, 'Auriparus flaviceps', 'Verdin', 'species')"
    )
    parent = db.add_keyword("Penduline tits")
    nested = db.conn.execute(
        "INSERT INTO keywords (name, parent_id, is_species, type) "
        "VALUES ('Verdin', ?, 0, 'general')",
        (parent,),
    ).lastrowid
    root = db.add_keyword("Verdin", is_species=True)
    db.tag_photo(pid, nested)
    db.tag_photo(pid, root)
    db.conn.execute(
        "DELETE FROM db_meta WHERE key = ?",
        (Database._DUPLICATE_PHOTO_SPECIES_REPAIR_KEY,),
    )
    db.conn.commit()
    db.close()

    fake_tax = MagicMock()
    fake_tax.lookup.side_effect = lambda name: (
        {"taxon_id": 2912} if name == "Verdin" else None
    )
    with patch("taxonomy.load_local_taxonomy", return_value=fake_tax):
        create_app(db_path=db_path, thumb_cache_dir=thumb_dir, api_token="test")

    db2 = Database(db_path, initialize_schema=False)
    tagged_ids = {row["id"] for row in db2.get_photo_keywords(pid)}
    assert nested in tagged_ids
    assert root not in tagged_ids
    assert db2.get_meta(Database._DUPLICATE_PHOTO_SPECIES_REPAIR_KEY) == "1"
    db2.close()


def test_create_app_runs_keyword_normalization_migration_on_file_db(
    tmp_path, monkeypatch,
):
    """Regression: create_app must run the one-shot keyword-name
    normalization migration on the startup connection for a file-backed
    database. Database.__init__ only runs it when initialize_schema=True,
    and every connection this app opens (startup init_db and every
    per-request _get_db) passes initialize_schema=False, so without an
    explicit run in create_app upgraded DBs could serve requests with
    ‘apapane-style variant rows still present.
    """
    from unittest.mock import MagicMock, patch

    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    import config as cfg
    import models
    from app import create_app

    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    monkeypatch.setattr(models, "DEFAULT_MODELS_DIR", str(tmp_path / "vireo-models"))
    monkeypatch.setattr(models, "CONFIG_PATH", str(tmp_path / "models.json"))

    db_path = str(tmp_path / "test.db")
    thumb_dir = str(tmp_path / "thumbs")
    os.makedirs(thumb_dir)

    # Seed a legacy edge-quote variant keyword row that the write-side
    # normalization would reject at runtime; only the migration can heal
    # it. Insert via raw SQL to bypass add_keyword's normalization.
    db = Database(db_path)
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    db.conn.execute("DELETE FROM db_meta WHERE key = 'keyword_names_normalized'")
    kid = db.conn.execute(
        "INSERT INTO keywords (name, parent_id, is_species, type) "
        "VALUES (?, NULL, 1, 'taxonomy')",
        ("‘apapane",),
    ).lastrowid
    db.conn.commit()
    assert db.get_meta("keyword_names_normalized") != "1", (
        "Pre-condition: normalization marker should be unset before create_app"
    )
    db.close()

    fake_tax = MagicMock()
    fake_tax.lookup.return_value = None

    with patch("taxonomy.load_local_taxonomy", return_value=fake_tax):
        app = create_app(
            db_path=db_path, thumb_cache_dir=thumb_dir, api_token="test",
        )
        assert app is not None

    # After create_app returns, the seeded variant row must have been
    # normalized in place and the marker set.
    db2 = Database(db_path, initialize_schema=False)
    row = db2.conn.execute(
        "SELECT name FROM keywords WHERE id = ?", (kid,)
    ).fetchone()
    assert row is not None
    assert row["name"] == "apapane", (
        "create_app must normalize legacy variant keyword names before "
        "serving requests, but the '‘apapane' row is still stored verbatim."
    )
    assert db2.get_meta("keyword_names_normalized") == "1", (
        "keyword_names_normalized marker must be set after create_app "
        "runs the one-shot migration synchronously."
    )
    db2.close()


def test_add_keyword_route_does_not_clobber_existing_individual_type(app_and_db):
    """Regression: POST /api/photos/<id>/keywords with type='taxonomy' for a
    keyword that already exists as 'individual' must not silently rewrite
    the existing row's type. add_keyword's reconciliation logic (only
    upgrades 'general') must run instead of a force-UPDATE."""
    app, db = app_and_db
    folder_id = db.add_folder("/tmp/p")
    db.add_workspace_folder(db._active_workspace_id, folder_id)
    photo_id = db.add_photo(
        folder_id, "p.jpg", extension=".jpg", file_size=1, file_mtime=1.0,
    )
    # Pre-create an 'individual' keyword named "Charlie" (e.g. user's pet).
    existing_kid = db.add_keyword("Charlie", kw_type="individual")
    client = app.test_client()
    resp = client.post(
        f"/api/photos/{photo_id}/keywords",
        json={"name": "Charlie", "type": "taxonomy"},
        content_type="application/json",
    )
    assert resp.status_code == 200
    # The existing row's type must remain 'individual' (not silently
    # rewritten to 'taxonomy' by a force-UPDATE).
    row = db.conn.execute(
        "SELECT type FROM keywords WHERE id = ?", (existing_kid,),
    ).fetchone()
    assert row["type"] == "individual", (
        f"Force-UPDATE silently rewrote a user-typed keyword. "
        f"Expected type='individual', got {row['type']!r}"
    )


def test_get_active_subject_types_workspace_override_wins(app_and_db, tmp_path, monkeypatch):
    """When a workspace override is set, it overrides the global default."""
    import config as cfg
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "global-config.json"))
    cfg.set("subject_types", ["taxonomy"])  # global = wildlife only
    app, db = app_and_db
    # Override the active workspace to include genre too
    ws_id = db._active_workspace_id
    db.update_workspace(ws_id, config_overrides={"subject_types": ["taxonomy", "genre"]})
    client = app.test_client()
    resp = client.get("/api/workspaces/active/subject-types")
    assert resp.status_code == 200
    body = resp.get_json()
    assert set(body["types"]) == {"taxonomy", "genre"}


def test_workspace_page_no_scan_button(app_and_db):
    """Workspace page should not expose the retired Scan & Add action."""
    app, _ = app_and_db
    with app.test_client() as c:
        resp = c.get('/workspace')
        assert resp.status_code == 200
        html = resp.data.decode()
        assert 'Scan &amp; Add' not in html
        assert 'scanAndAddFolder' not in html


def test_workspace_page_has_import_photos_link(app_and_db):
    """Workspace page should send users to Import when they need photos."""
    app, _ = app_and_db
    with app.test_client() as c:
        resp = c.get('/workspace')
        assert resp.status_code == 200
        html = resp.data.decode()
        assert 'href="/import"' in html
        assert 'Import Photos' in html


# -- Missing folder API tests --


def test_api_folders_missing(app_and_db):
    """GET /api/folders/missing returns missing folders with counts."""
    app, db = app_and_db
    db.conn.execute("UPDATE folders SET status = 'missing' WHERE path = '/photos/2024'")
    db.conn.commit()

    client = app.test_client()
    resp = client.get("/api/folders/missing")
    assert resp.status_code == 200
    data = resp.get_json()
    assert len(data) == 1
    assert data[0]["path"] == "/photos/2024"
    assert data[0]["photo_count"] >= 1


def test_api_folders_check_health(app_and_db):
    """POST /api/folders/check-health triggers health check and returns missing folders."""
    app, db = app_and_db
    client = app.test_client()
    resp = client.post("/api/folders/check-health")
    assert resp.status_code == 200
    data = resp.get_json()
    assert "changed" in data
    assert "missing" in data
    assert isinstance(data["missing"], list)


def test_api_photos_missing_uncached_is_immediate(app_and_db, monkeypatch):
    """GET /api/photos/missing returns cache status without scanning."""
    from db import Database
    app, _db = app_and_db
    client = app.test_client()

    def fail_scan(*_args, **_kwargs):
        raise AssertionError("GET /api/photos/missing must not scan")

    monkeypatch.setattr(Database, "get_missing_photos", fail_scan)

    resp = client.get("/api/photos/missing")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["status"] == "not_ready"
    assert data["pending"] is False
    assert data["photos"] == []


def test_api_photos_missing(app_and_db, tmp_path):
    """Background check caches photos with absent source files."""
    from PIL import Image
    app, db = app_and_db
    client = app.test_client()

    # Replace the seed folder with one that exists on disk and seed one
    # real file + one ghost row so missing detection has something to find.
    real_dir = tmp_path / "live"
    real_dir.mkdir()
    Image.new("RGB", (10, 10)).save(real_dir / "here.jpg")
    fid = db.add_folder(str(real_dir), name="live")
    pid_present = db.add_photo(folder_id=fid, filename="here.jpg",
                               extension=".jpg", file_size=1, file_mtime=1.0)
    pid_ghost = db.add_photo(folder_id=fid, filename="ghost.NEF",
                             extension=".nef", file_size=42, file_mtime=2.0,
                             timestamp="2024-03-08T10:00:00")
    # Pre-existing seed photos in /photos/2024 also have absent sources;
    # simplify the assertion by removing them from the workspace.
    db.delete_photos([
        row["id"] for row in db.conn.execute(
            "SELECT id FROM photos WHERE folder_id != ?", (fid,)
        ).fetchall()
    ])

    # Cache a thumb for the ghost so the UI can still preview it.
    Image.new("RGB", (10, 10)).save(
        os.path.join(app.config["THUMB_CACHE_DIR"], f"{pid_ghost}.jpg"),
    )
    # Drop an XMP sidecar next to the ghost.
    (real_dir / "ghost.xmp").write_text("<x:xmpmeta/>")

    data = _run_missing_originals_check(client)
    assert data["status"] == "ready"
    assert [row["id"] for row in data["photos"]] == [pid_ghost]
    row = data["photos"][0]
    assert row["filename"] == "ghost.NEF"
    assert row["folder_path"] == str(real_dir)
    assert row["timestamp"] == "2024-03-08T10:00:00"
    assert row["has_thumb"] is True
    assert row["has_preview"] is False
    assert row["has_working_copy"] is False
    assert row["has_xmp_sidecar"] is True


def test_api_photos_missing_cached_result_does_not_rescan(app_and_db, monkeypatch, tmp_path):
    """A cached Missing Originals result is served without filesystem work."""
    from db import Database

    app, db = app_and_db
    client = app.test_client()

    real_dir = tmp_path / "live"
    real_dir.mkdir()
    fid = db.add_folder(str(real_dir), name="live")
    pid_ghost = db.add_photo(
        folder_id=fid,
        filename="ghost.NEF",
        extension=".nef",
        file_size=42,
        file_mtime=2.0,
    )
    db.delete_photos([
        row["id"] for row in db.conn.execute(
            "SELECT id FROM photos WHERE folder_id != ?", (fid,)
        ).fetchall()
    ])

    cached = _run_missing_originals_check(client)
    assert [row["id"] for row in cached["photos"]] == [pid_ghost]

    def fail_scan(*_args, **_kwargs):
        raise AssertionError("cached GET must not rescan")

    monkeypatch.setattr(Database, "get_missing_photos", fail_scan)

    resp = client.get("/api/photos/missing")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["status"] == "ready"
    assert [row["id"] for row in data["photos"]] == [pid_ghost]


def test_api_photos_missing_automatic_uses_fresh_cache(app_and_db, monkeypatch, tmp_path):
    """Automatic checks should not rescan when the cached result is still fresh."""
    from db import Database

    app, db = app_and_db
    client = app.test_client()

    real_dir = tmp_path / "live"
    real_dir.mkdir()
    fid = db.add_folder(str(real_dir), name="live")
    pid_ghost = db.add_photo(
        folder_id=fid,
        filename="ghost.NEF",
        extension=".nef",
        file_size=42,
        file_mtime=2.0,
    )
    db.delete_photos([
        row["id"] for row in db.conn.execute(
            "SELECT id FROM photos WHERE folder_id != ?", (fid,)
        ).fetchall()
    ])

    cached = _run_missing_originals_check(client)
    assert [row["id"] for row in cached["photos"]] == [pid_ghost]

    def fail_scan(*_args, **_kwargs):
        raise AssertionError("fresh automatic check must not rescan")

    monkeypatch.setattr(Database, "get_missing_photos", fail_scan)

    resp = client.post("/api/photos/missing/check", json={"automatic": True})
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["status"] == "ready"
    assert data["pending"] is False
    assert [row["id"] for row in data["photos"]] == [pid_ghost]


def test_api_photos_missing_automatic_gate_uses_scan_start_time(
    app_and_db, tmp_path
):
    """Automatic freshness gate must compare against scan-start, not scan-end.

    Regression: the navbar re-arms its 30-minute automatic timer from POST
    time, but the server used to gate against ``set_at`` (scan-completion
    time). On any scan that took real wall-clock time, the next tick fired
    with ``set_at`` under the 30-minute threshold and got skipped — actual
    filesystem scans then ran only every second tick and deletions could
    stay undiscovered for nearly an hour.
    """
    import time

    from db import Database

    app, db = app_and_db
    client = app.test_client()

    real_dir = tmp_path / "live"
    real_dir.mkdir()
    fid = db.add_folder(str(real_dir), name="live")
    pid_ghost = db.add_photo(
        folder_id=fid,
        filename="ghost.NEF",
        extension=".nef",
        file_size=42,
        file_mtime=2.0,
    )
    db.delete_photos([
        row["id"] for row in db.conn.execute(
            "SELECT id FROM photos WHERE folder_id != ?", (fid,)
        ).fetchall()
    ])

    # Seed the cache to simulate a scan that started > 30 min ago but
    # only completed recently. The old (buggy) gate would compare ``now``
    # against ``set_at`` and treat this as fresh; the fix compares against
    # ``started_at`` so the next automatic tick rescans as intended.
    key = (db._db_path, db._active_workspace_id, None)
    now = time.monotonic()
    stale_seconds = getattr(
        Database, "_MISSING_ORIGINALS_STALE_SECONDS", 30 * 60
    )
    with app._missing_originals_lock:
        app._missing_originals_cache[key] = {
            "photos": [],
            "checked_at": "2026-01-01T00:00:00Z",
            "set_at": now - 60,  # completed 1 minute ago
            "started_at": now - (stale_seconds + 60),  # scheduled long ago
        }

    resp = client.post("/api/photos/missing/check", json={"automatic": True})
    # A rescan was actually launched — either it finished synchronously
    # (returning ready with the just-found ghost) or it's still pending.
    assert resp.status_code in (200, 202)
    data = resp.get_json()
    if data.get("pending"):
        wait_for_job_via_client(client, data["job_id"])
        follow = client.get("/api/photos/missing").get_json()
        assert follow["status"] == "ready"
        assert [row["id"] for row in follow["photos"]] == [pid_ghost]
    else:
        assert data["status"] == "ready"
        assert [row["id"] for row in data["photos"]] == [pid_ghost]


def test_api_photos_missing_check_coalesces_duplicate_jobs(app_and_db, monkeypatch):
    """Duplicate refreshes for the same scope reuse the active background job."""
    import time

    from db import Database

    app, db = app_and_db
    client = app.test_client()

    real_get_missing = Database.get_missing_photos

    def slow_get_missing(self, *args, **kwargs):
        time.sleep(0.2)
        return real_get_missing(self, *args, **kwargs)

    monkeypatch.setattr(Database, "get_missing_photos", slow_get_missing)

    first = client.post("/api/photos/missing/check", json={})
    second = client.post("/api/photos/missing/check", json={})

    assert first.status_code == 202
    assert second.status_code == 202
    first_data = first.get_json()
    second_data = second.get_json()
    assert first_data["pending"] is True
    assert second_data["pending"] is True
    assert first_data["job_id"] == second_data["job_id"]
    wait_for_job_via_client(client, first_data["job_id"])


def test_api_photos_missing_stale_in_flight_result_is_discarded(
    app_and_db, monkeypatch, tmp_path
):
    """An invalidation mid-scan must drop the in-flight scan's stale snapshot.

    Regression: when a batch delete fired while a long missing-originals
    scan was walking the disk, the completing scan wrote its
    pre-invalidation photo list back into the ready cache — resurrecting
    just-deleted photos in the banner/modal until a later scan overwrote
    them.
    """
    import threading

    from db import Database

    app, db = app_and_db
    client = app.test_client()

    real_dir = tmp_path / "live"
    real_dir.mkdir()
    fid = db.add_folder(str(real_dir), name="live")
    pid_ghost = db.add_photo(
        folder_id=fid,
        filename="ghost.NEF",
        extension=".nef",
        file_size=42,
        file_mtime=2.0,
    )
    # Simplify assertions by removing seed rows from other folders.
    db.delete_photos([
        row["id"] for row in db.conn.execute(
            "SELECT id FROM photos WHERE folder_id != ?", (fid,)
        ).fetchall()
    ])

    snapshot_captured = threading.Event()
    release = threading.Event()
    real_get_missing = Database.get_missing_photos

    def slow_get_missing(self, *args, **kwargs):
        # Take the "pre-delete" snapshot first so the scan's result would
        # otherwise resurrect pid_ghost, then park until the test has
        # fired the batch delete that bumps the generation counter.
        photos = list(real_get_missing(self, *args, **kwargs))
        snapshot_captured.set()
        assert release.wait(timeout=5.0)
        return photos

    monkeypatch.setattr(Database, "get_missing_photos", slow_get_missing)

    started = client.post("/api/photos/missing/check", json={})
    assert started.status_code == 202
    job_id = started.get_json()["job_id"]
    assert snapshot_captured.wait(timeout=5.0)

    # Batch delete the ghost row. This drops it from the DB and fires
    # _invalidate_missing_originals_cache, which bumps the in-flight
    # scan's generation counter.
    delete_resp = client.post(
        "/api/batch/delete",
        json={"photo_ids": [pid_ghost], "mode": "vireo"},
    )
    assert delete_resp.status_code == 200, delete_resp.get_json()
    assert delete_resp.get_json()["deleted"] == 1

    release.set()
    job = wait_for_job_via_client(client, job_id)
    assert job["status"] == "completed"
    assert job["result"]["stale"] is True

    # The scan finished successfully but its results were discarded, so
    # the endpoint reports no cache — not a ready payload still holding
    # the just-deleted pid_ghost.
    resp = client.get("/api/photos/missing")
    assert resp.status_code == 200
    payload = resp.get_json()
    assert payload["status"] == "not_ready", payload
    assert payload["photos"] == []


def test_api_photos_missing_automatic_skips_during_heavy_job(app_and_db):
    """Automatic idle checks must not start while heavy jobs are running."""
    import time

    app, db = app_and_db
    client = app.test_client()

    scan_job = app._job_runner.start(
        "scan",
        lambda job: (time.sleep(0.2), {"ok": True})[1],
        workspace_id=db._active_workspace_id,
        ephemeral=True,
    )
    resp = client.post("/api/photos/missing/check", json={"automatic": True})
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["suppressed"] is True
    assert data["reason"] == "heavy_job_active"
    assert data["status"] == "skipped"
    wait_for_job_via_client(client, scan_job)


def test_api_photos_missing_automatic_skips_during_missing_originals_scan(app_and_db):
    """A folder-scoped scan already walking disk must suppress the automatic
    workspace-wide check.

    Regression: workspace and folder-scoped scans have distinct cache keys, so
    the same-key in-flight coalescing does not catch them. Without treating
    ``missing_originals_scan`` as a heavy job, the 30-minute automatic timer
    can kick off a second walk over the same tree while a folder scan is
    still running.
    """
    import threading
    import time

    app, db = app_and_db
    client = app.test_client()

    # Hold a fake missing_originals_scan job in the running queue long enough
    # to exercise the automatic-check gate. A real folder-scoped scan would
    # register the same job type; we don't need the actual scan logic to
    # observe the heavy-job suppression.
    release = threading.Event()

    def _hold_running(job):
        release.wait(timeout=5.0)
        return {"ok": True}

    scan_job = app._job_runner.start(
        "missing_originals_scan",
        _hold_running,
        workspace_id=db._active_workspace_id,
        ephemeral=True,
    )
    try:
        # Give the runner a moment to move the job to "running" so
        # _missing_originals_heavy_job_active sees it.
        for _ in range(50):
            jobs = app._job_runner.list_jobs()
            if any(
                j.get("id") == scan_job and j.get("status") in ("running", "queued")
                for j in jobs
            ):
                break
            time.sleep(0.02)

        resp = client.post(
            "/api/photos/missing/check", json={"automatic": True}
        )
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["suppressed"] is True, data
        assert data["reason"] == "heavy_job_active", data
        assert data["status"] == "skipped", data
    finally:
        release.set()
        wait_for_job_via_client(client, scan_job)


def test_api_folder_delete_invalidates_missing_cache(app_and_db, tmp_path):
    """Deleting a folder must clear the Missing Originals cache.

    Regression: ``api_folder_delete`` cascades photo-row deletion but never
    hit ``_invalidate_missing_originals_cache``. A ready payload built before
    the delete would keep listing photos from the now-removed folder in the
    banner/modal, offering them for removal a second time.
    """
    app, db = app_and_db
    client = app.test_client()

    real_dir = tmp_path / "live"
    real_dir.mkdir()
    fid = db.add_folder(str(real_dir), name="live")
    pid_ghost = db.add_photo(
        folder_id=fid,
        filename="ghost.NEF",
        extension=".nef",
        file_size=42,
        file_mtime=2.0,
    )
    # Drop unrelated seed photos so the cache payload is deterministic.
    db.delete_photos([
        row["id"] for row in db.conn.execute(
            "SELECT id FROM photos WHERE folder_id != ?", (fid,)
        ).fetchall()
    ])

    cached = _run_missing_originals_check(client)
    assert [row["id"] for row in cached["photos"]] == [pid_ghost]

    resp = client.delete(f"/api/folders/{fid}")
    assert resp.status_code == 200

    payload = client.get("/api/photos/missing").get_json()
    assert payload["status"] == "not_ready", payload
    assert payload["photos"] == []


def test_api_folder_relocate_invalidates_missing_cache(app_and_db, tmp_path):
    """Relocating a folder must clear the Missing Originals cache.

    Regression: ``api_folder_relocate`` rewrites ``folders.path`` and flips
    status to ``ok`` (and can merge/delete rows via the missing→existing
    branch), but never called ``_invalidate_missing_originals_cache``.
    After moving a missing folder to a path where the originals exist, a
    ready cached payload would keep offering the pre-relocation ghost
    rows for removal — and the modal's remove flow would happily delete
    photos whose originals just came back online.
    """
    app, db = app_and_db
    client = app.test_client()

    # Folder exists on disk (get_missing_photos skips folders whose root
    # is offline), but the tracked photo file isn't there — so it shows up
    # as a ghost.
    old_dir = tmp_path / "orig"
    old_dir.mkdir()
    fid = db.add_folder(str(old_dir), name="orig")
    pid_ghost = db.add_photo(
        folder_id=fid,
        filename="ghost.NEF",
        extension=".nef",
        file_size=42,
        file_mtime=2.0,
    )
    # Strip unrelated seed photos so the cached payload is deterministic.
    db.delete_photos([
        row["id"] for row in db.conn.execute(
            "SELECT id FROM photos WHERE folder_id != ?", (fid,)
        ).fetchall()
    ])

    cached = _run_missing_originals_check(client)
    assert [row["id"] for row in cached["photos"]] == [pid_ghost], cached

    # Relocate to a new path where the original actually exists.
    new_dir = tmp_path / "moved"
    new_dir.mkdir()
    (new_dir / "ghost.NEF").write_bytes(b"stub")

    resp = client.post(
        f"/api/folders/{fid}/relocate",
        json={"path": str(new_dir)},
    )
    assert resp.status_code == 200, resp.get_json()

    # Cache must be invalidated: GET falls through to not_ready rather
    # than serving the pre-relocate ghost payload.
    payload = client.get("/api/photos/missing").get_json()
    assert payload["status"] == "not_ready", payload
    assert payload["photos"] == []


def test_api_folders_check_health_invalidates_missing_cache(app_and_db, tmp_path):
    """A folder health flip must clear the Missing Originals cache.

    Regression: ``api_folders_check_health`` calls
    ``db.check_folder_health`` which flips folders to/from ``missing`` as
    disk state changes. Without invalidation, a ready cached payload
    survives the flip: a folder going ok→missing hides the new ghosts,
    and missing→ok keeps resurfacing photos whose originals just came
    back.
    """
    app, db = app_and_db
    client = app.test_client()

    live_dir = tmp_path / "live"
    live_dir.mkdir()
    fid = db.add_folder(str(live_dir), name="live")
    (live_dir / "keep.NEF").write_bytes(b"stub")
    db.add_photo(
        folder_id=fid,
        filename="keep.NEF",
        extension=".nef",
        file_size=len(b"stub"),
        file_mtime=2.0,
    )
    db.delete_photos([
        row["id"] for row in db.conn.execute(
            "SELECT id FROM photos WHERE folder_id != ?", (fid,)
        ).fetchall()
    ])

    # Prime a ready cache while the folder is healthy — no ghosts.
    cached = _run_missing_originals_check(client)
    assert cached["photos"] == [], cached

    # Now the folder disappears from disk. The next check-health call
    # flips its status to missing, which turns every one of its photos
    # into a ghost. A stale cache would still say "no ghosts".
    import shutil
    shutil.rmtree(live_dir)

    resp = client.post("/api/folders/check-health")
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["changed"] >= 1

    payload = client.get("/api/photos/missing").get_json()
    assert payload["status"] == "not_ready", payload


def test_workspace_delete_and_create_invalidate_missing_cache(app_and_db):
    """Workspace create/delete must clear their id from the missing cache.

    Regression: the cache key is ``(db_path, workspace_id, folder_id)``,
    and SQLite ``INTEGER PRIMARY KEY`` can reuse the rowid of a deleted
    workspace for the next ``create_workspace``. If the deleted workspace
    left a ready Missing Originals payload behind, ``GET /api/photos/missing``
    on the freshly created workspace would serve the previous workspace's
    ghost photos and folder paths until a rescan overwrote the entry.
    """
    app, db = app_and_db
    client = app.test_client()

    # Create a second workspace and give it a fake ready cache entry.
    resp = client.post(
        "/api/workspaces",
        json={"name": "temp-ws"},
    )
    assert resp.status_code == 200
    ws = resp.get_json()
    ws_id = ws["id"]
    key = (db._db_path, ws_id, None)
    with app._missing_originals_lock:
        app._missing_originals_cache[key] = {
            "photos": [{"id": 99999, "filename": "ghost.jpg"}],
            "checked_at": "2026-01-01T00:00:00Z",
            "set_at": 0.0,
        }

    # Delete the workspace via the API and confirm the cache entry
    # (which is keyed on this ws_id) is gone. Without invalidation on
    # delete, a subsequent workspace that reused this rowid would still
    # see the stale payload.
    resp = client.delete(f"/api/workspaces/{ws_id}")
    assert resp.status_code == 200
    with app._missing_originals_lock:
        assert key not in app._missing_originals_cache

    # Prime a stale cache entry directly at the next id SQLite could
    # hand out, then create a workspace to verify the create path also
    # clears the specific workspace's key even if the store somehow
    # retained one (e.g. a concurrent write racing between the delete
    # invalidation and the new insert).
    next_id_row = db.conn.execute(
        "SELECT seq FROM sqlite_sequence WHERE name = 'workspaces'"
    ).fetchone()
    likely_next_id = (next_id_row["seq"] + 1) if next_id_row else ws_id + 1
    poison_key = (db._db_path, likely_next_id, None)
    with app._missing_originals_lock:
        app._missing_originals_cache[poison_key] = {
            "photos": [{"id": 42, "filename": "poison.jpg"}],
            "checked_at": "2026-01-01T00:00:00Z",
            "set_at": 0.0,
        }
    resp = client.post("/api/workspaces", json={"name": "fresh-ws"})
    assert resp.status_code == 200
    new_ws_id = resp.get_json()["id"]
    with app._missing_originals_lock:
        assert (db._db_path, new_ws_id, None) not in app._missing_originals_cache


def test_api_photos_missing_automatic_respects_failure_backoff(app_and_db, monkeypatch):
    """A failed manual scan suppresses automatic retries during backoff."""
    from db import Database

    app, _db = app_and_db
    client = app.test_client()

    def fail_scan(self, *args, **kwargs):
        raise RuntimeError("network volume unavailable")

    monkeypatch.setattr(Database, "get_missing_photos", fail_scan)

    started = client.post("/api/photos/missing/check", json={})
    assert started.status_code == 202
    job = wait_for_job_via_client(client, started.get_json()["job_id"])
    assert job["status"] == "failed"

    retry = client.post("/api/photos/missing/check", json={"automatic": True})
    assert retry.status_code == 200
    data = retry.get_json()
    assert data["status"] == "error"
    assert data["suppressed"] is True
    assert data["reason"] == "backoff"
    assert data["backoff_seconds"] > 0


def test_api_photos_missing_worker_db_open_failure_clears_inflight(
    app_and_db, monkeypatch,
):
    """A worker DB-open failure must not leave the scope permanently pending."""
    import app as app_module

    app, db = app_and_db
    client = app.test_client()
    key = (db._db_path, db._active_workspace_id, None)
    captured = {}

    real_database = app_module.Database

    class FailingDatabase:
        def __init__(self, path):
            raise RuntimeError("open failed")

    def run_with_broken_worker_db(job_type, work, **kwargs):
        job = {"id": "missing-originals-fail", "progress": {}}
        app_module.Database = FailingDatabase
        try:
            work(job)
        except RuntimeError as exc:
            captured["error"] = str(exc)
        finally:
            app_module.Database = real_database
        return job["id"]

    monkeypatch.setattr(app._job_runner, "start", run_with_broken_worker_db)

    resp = client.post("/api/photos/missing/check", json={})
    assert resp.status_code == 202, resp.get_json()
    assert captured["error"] == "open failed"
    with app._missing_originals_lock:
        assert key not in app._missing_originals_inflight
        assert app._missing_originals_errors[key]["error"] == "open failed"


def test_api_photos_missing_progress_phase_only_sent_via_runner(
    app_and_db, monkeypatch,
):
    """Missing Originals progress must not resize job["progress"] directly."""
    from db import Database

    app, _db = app_and_db
    client = app.test_client()
    pushed = []

    class GuardedProgress(dict):
        def __setitem__(self, key, value):
            if key not in self:
                raise AssertionError(f"unexpected progress key resize: {key}")
            super().__setitem__(key, value)

    def fake_missing_photos(self, folder_id=None, progress_callback=None, **kwargs):
        if progress_callback is not None:
            progress_callback({
                "photos_considered": 7,
                "total_photos": 10,
                "missing_found": 2,
                "folders_checked": 1,
                "current_folder": "/photos",
            })
        return []

    def fake_start(job_type, work, **kwargs):
        job = {
            "id": "missing-progress",
            "progress": GuardedProgress({
                "current": 0,
                "total": 0,
                "current_file": "",
            }),
        }
        work(job)
        return job["id"]

    def fake_push_event(job_id, event_type, data):
        pushed.append((job_id, event_type, data))

    monkeypatch.setattr(Database, "get_missing_photos", fake_missing_photos)
    monkeypatch.setattr(app._job_runner, "start", fake_start)
    monkeypatch.setattr(app._job_runner, "push_event", fake_push_event)

    resp = client.post("/api/photos/missing/check", json={})
    assert resp.status_code == 200, resp.get_json()
    progress_events = [data for _jid, typ, data in pushed if typ == "progress"]
    assert progress_events
    assert progress_events[-1]["phase"] == (
        "1 folders checked, 7 photos considered, 2 missing"
    )


def test_get_missing_photos_honors_cancel_callback(app_and_db):
    """The low-level missing-originals walk must be cooperatively cancellable."""
    import pytest
    from db import MissingPhotosCancelled

    _app, db = app_and_db

    with pytest.raises(MissingPhotosCancelled):
        db.get_missing_photos(cancel_callback=lambda: True)


def test_api_photos_missing_cancel_does_not_write_ready_cache(
    app_and_db, monkeypatch,
):
    """Cancelling a Missing Originals job must stop without publishing results.

    Regression: JobRunner marked the job cancelled, but the worker kept walking
    the filesystem and could write a ready cache before terminal status flipped.
    """
    import threading
    import time

    from db import Database, MissingPhotosCancelled

    app, db = app_and_db
    client = app.test_client()
    scan_entered = threading.Event()

    def slow_missing_photos(
        self,
        folder_id=None,
        progress_callback=None,
        cancel_callback=None,
    ):
        scan_entered.set()
        deadline = time.time() + 3.0
        while time.time() < deadline:
            if cancel_callback is not None and cancel_callback():
                raise MissingPhotosCancelled("missing photos scan cancelled")
            time.sleep(0.01)
        return [{
            "id": 999,
            "folder_path": os.getcwd(),
            "filename": "would-have-been-cached.jpg",
            "timestamp": None,
            "working_copy_path": None,
        }]

    monkeypatch.setattr(Database, "get_missing_photos", slow_missing_photos)

    started = client.post("/api/photos/missing/check", json={})
    assert started.status_code == 202, started.get_json()
    job_id = started.get_json()["job_id"]
    assert scan_entered.wait(timeout=1.0)

    cancelled = client.post(f"/api/jobs/{job_id}/cancel")
    assert cancelled.status_code == 200, cancelled.get_json()
    assert cancelled.get_json()["cancelled"] is True

    job = wait_for_job_via_client(client, job_id)
    assert job["status"] == "cancelled"

    key = (db._db_path, db._active_workspace_id, None)
    with app._missing_originals_lock:
        assert key not in app._missing_originals_cache
        assert key not in app._missing_originals_errors
        assert key not in app._missing_originals_inflight
    payload = client.get("/api/photos/missing").get_json()
    assert payload["status"] == "not_ready", payload


def test_api_photos_missing_prefers_fresher_error_over_ready_cache(app_and_db):
    """A failed refresh must not be hidden behind an older ready cache.

    Regression: after a successful scan populated the cache, a later
    "Check now" that failed (e.g. NAS went offline) would leave the
    ready photo list in place while recording the failure in
    ``_missing_originals_errors``. The old ``_missing_originals_payload``
    branch always won over the error, so ``GET /api/photos/missing``
    would keep returning ``status: "ready"`` with the pre-failure photo
    list — hiding the failure and letting the user act on stale ghosts
    whose originals may have been restored between scans.
    """
    app, db = app_and_db
    client = app.test_client()

    key = (db._db_path, db._active_workspace_id, None)
    # Fresher error must supersede the cached ready payload.
    with app._missing_originals_lock:
        app._missing_originals_cache[key] = {
            "photos": [{"id": 1, "filename": "ghost.jpg"}],
            "checked_at": "2026-01-01T00:00:00Z",
            "set_at": 1.0,
        }
        app._missing_originals_errors[key] = {
            "error": "network volume unavailable",
            "checked_at": "2026-01-01T00:05:00Z",
            "set_at": 2.0,
            "backoff_until": 999999.0,
        }
    payload = client.get("/api/photos/missing").get_json()
    assert payload["status"] == "error", payload
    assert payload["error"] == "network volume unavailable"
    assert payload["photos"] == []

    # An older error (already superseded by a successful cache) must
    # not clobber the ready state — the preference only fires when
    # the error's set_at is fresher than the cache's.
    with app._missing_originals_lock:
        app._missing_originals_errors[key] = {
            "error": "old",
            "checked_at": "2025-12-31T23:59:00Z",
            "set_at": 0.5,
            "backoff_until": 999999.0,
        }
    payload = client.get("/api/photos/missing").get_json()
    assert payload["status"] == "ready", payload
    assert payload["photos"] == [{"id": 1, "filename": "ghost.jpg"}]

    # A fresh error alongside an in-flight scan must keep reporting
    # "pending" so the modal doesn't flicker to an error state while
    # the user's own refresh is still running.
    with app._missing_originals_lock:
        app._missing_originals_errors[key] = {
            "error": "network volume unavailable",
            "checked_at": "2026-01-01T00:05:00Z",
            "set_at": 2.0,
            "backoff_until": 999999.0,
        }
        app._missing_originals_inflight[key] = "job-xyz"
    payload = client.get("/api/photos/missing").get_json()
    assert payload["status"] == "pending", payload


def test_scan_job_invalidates_missing_originals_cache(app_and_db, tmp_path):
    """A rescan must clear the Missing Originals cache even when the
    pre-scan folder-health check doesn't flip anything.

    Regression: the scan work function only invalidated the cache when
    ``check_folder_health`` returned a nonzero change count. But a
    normal scan (e.g. Browse's "Rescan this Folder" after the user
    restored an original) still commits photo rows and can make a
    ready missing-originals payload stale. Without a post-scan
    invalidation, ``GET /api/photos/missing`` would keep serving the
    pre-scan ghost list until a separate missing-originals scan.
    """
    app, db = app_and_db
    client = app.test_client()

    real_dir = tmp_path / "live"
    real_dir.mkdir()
    (real_dir / "keep.jpg").write_bytes(b"jpegbytes")
    fid = db.add_folder(str(real_dir), name="live")

    # Seed a stale ready cache directly (the folder is healthy, so
    # the scan's own pre-flight would not invalidate it via the
    # existing health-flip path).
    key = (db._db_path, db._active_workspace_id, None)
    with app._missing_originals_lock:
        app._missing_originals_cache[key] = {
            "photos": [{"id": 99, "filename": "stale.jpg"}],
            "checked_at": "2026-01-01T00:00:00Z",
            "set_at": 0.0,
        }

    resp = client.post(f"/api/folders/{fid}/rescan", json={})
    assert resp.status_code == 200, resp.get_json()
    job_id = resp.get_json()["job_id"]
    wait_for_job_via_client(client, job_id)

    with app._missing_originals_lock:
        assert key not in app._missing_originals_cache


def test_import_full_scan_in_place_invalidates_missing_originals_cache(
    app_and_db, tmp_path,
):
    """A scan-in-place import (POST /api/jobs/import-full, copy=false) must
    drop the Missing Originals cache once its ``do_scan`` runs.

    Regression: the scan job's ``finally`` block invalidated the
    new-images cache but not the missing-originals cache, so a ready
    ghost payload survived even after the import touched disk. If the
    user restored an original before running Import Photos over the
    same folder, GET ``/api/photos/missing`` would keep serving the
    pre-import ghost list until a separate missing-originals scan
    replaced the entry. See Codex review on c4cc32ec.
    """
    from PIL import Image

    app, db = app_and_db
    client = app.test_client()

    source = tmp_path / "import_src"
    source.mkdir()
    Image.new("RGB", (10, 10)).save(str(source / "keep.jpg"))

    key = (db._db_path, db._active_workspace_id, None)
    with app._missing_originals_lock:
        app._missing_originals_cache[key] = {
            "photos": [{"id": 4242, "filename": "stale.jpg"}],
            "checked_at": "2026-01-01T00:00:00Z",
            "set_at": 0.0,
        }

    resp = client.post("/api/jobs/import-full", json={
        "source": str(source),
        "copy": False,
        "file_types": [".jpg"],
    })
    assert resp.status_code == 200, resp.get_json()
    wait_for_job_via_client(client, resp.get_json()["job_id"])

    with app._missing_originals_lock:
        assert key not in app._missing_originals_cache


def test_import_in_place_invalidates_missing_originals_cache(
    app_and_db, tmp_path,
):
    """POST /api/jobs/import-in-place must drop the Missing Originals cache
    once its ``do_scan`` runs.

    Regression: the in-place import's ``finally`` block invalidated the
    new-images cache but not the missing-originals cache, so a ready
    ghost payload survived even after the import touched disk. If the
    user restored an original before importing that folder in place,
    GET ``/api/photos/missing`` would keep serving the pre-import ghost
    list. See Codex review on c4cc32ec.
    """
    from PIL import Image

    app, db = app_and_db
    client = app.test_client()

    source = tmp_path / "in_place_src"
    source.mkdir()
    Image.new("RGB", (10, 10)).save(str(source / "keep.jpg"))

    key = (db._db_path, db._active_workspace_id, None)
    with app._missing_originals_lock:
        app._missing_originals_cache[key] = {
            "photos": [{"id": 4343, "filename": "stale.jpg"}],
            "checked_at": "2026-01-01T00:00:00Z",
            "set_at": 0.0,
        }

    resp = client.post("/api/jobs/import-in-place", json={
        "sources": [str(source)],
        "after_import": None,
    })
    assert resp.status_code == 200, resp.get_json()
    wait_for_job_via_client(client, resp.get_json()["job_id"])

    with app._missing_originals_lock:
        assert key not in app._missing_originals_cache


def test_import_photos_invalidates_missing_originals_cache(
    app_and_db, tmp_path, monkeypatch,
):
    """POST /api/jobs/import-photos must drop the Missing Originals cache
    once ``run_import_job`` finishes.

    Regression: unlike scan-in-place and import-in-place, the copy-mode
    import job (``/api/jobs/import-photos``) never invalidated the
    missing-originals cache after ``run_import_job`` completed. That job
    can flip destination folders from ``missing`` to ``ok`` and scans
    landed files, so a ready ghost payload survived even after the
    import touched disk. Verify the ``finally`` block drops the cached
    entry — and drops it even when the import raises, since rows land
    incrementally.
    """
    app, db = app_and_db
    client = app.test_client()

    source = tmp_path / "import_src"
    source.mkdir()
    (source / "keep.jpg").write_bytes(b"stub")
    destination = tmp_path / "archive"
    destination.mkdir()

    key = (db._db_path, db._active_workspace_id, None)

    def _seed_cache():
        with app._missing_originals_lock:
            app._missing_originals_cache[key] = {
                "photos": [{"id": 4444, "filename": "stale.jpg"}],
                "checked_at": "2026-01-01T00:00:00Z",
                "set_at": 0.0,
            }

    # Stub ``run_import_job`` so we exercise the endpoint's ``finally``
    # block without depending on the real ingest+scan pipeline. The
    # invariant under test is "cache is dropped after the job runs",
    # not "the import succeeds"; we cover the happy path and the
    # mid-run failure path separately.
    import import_job as import_job_module

    def _stub_run_import_job(job, runner, db_path, active_ws, params):
        return {"ok": True, "photo_ids": []}

    _seed_cache()
    monkeypatch.setattr(
        import_job_module, "run_import_job", _stub_run_import_job,
    )
    resp = client.post(
        "/api/jobs/import-photos",
        json={
            "sources": [str(source)],
            "destination": str(destination),
            "after_import": None,
        },
    )
    assert resp.status_code == 200, resp.get_json()
    wait_for_job_via_client(client, resp.get_json()["job_id"])
    with app._missing_originals_lock:
        assert key not in app._missing_originals_cache

    # Same invariant when the import raises: rows land incrementally,
    # so a mid-run failure can still leave the cache stale.
    _seed_cache()

    def _boom(job, runner, db_path, active_ws, params):
        raise RuntimeError("boom")

    monkeypatch.setattr(import_job_module, "run_import_job", _boom)
    resp = client.post(
        "/api/jobs/import-photos",
        json={
            "sources": [str(source)],
            "destination": str(destination),
            "after_import": None,
        },
    )
    assert resp.status_code == 200, resp.get_json()
    wait_for_job_via_client(client, resp.get_json()["job_id"])
    with app._missing_originals_lock:
        assert key not in app._missing_originals_cache


def test_api_audit_remove_orphans_invalidates_missing_cache(app_and_db, tmp_path):
    """Removing orphaned photo rows must clear the Missing Originals cache.

    Regression: without invalidation, the banner and modal keep serving the
    pre-delete cache verbatim, so a photo the user just removed via the
    Audit page reappears in the ghost list until a later manual or
    automatic rescan.
    """
    app, db = app_and_db
    client = app.test_client()

    real_dir = tmp_path / "live"
    real_dir.mkdir()
    fid = db.add_folder(str(real_dir), name="live")
    pid_ghost = db.add_photo(
        folder_id=fid,
        filename="ghost.NEF",
        extension=".nef",
        file_size=42,
        file_mtime=2.0,
    )
    db.delete_photos([
        row["id"] for row in db.conn.execute(
            "SELECT id FROM photos WHERE folder_id != ?", (fid,)
        ).fetchall()
    ])

    cached = _run_missing_originals_check(client)
    assert [row["id"] for row in cached["photos"]] == [pid_ghost]

    resp = client.post(
        "/api/audit/remove-orphans", json={"photo_ids": [pid_ghost]}
    )
    assert resp.status_code == 200
    assert resp.get_json()["removed"] == 1

    payload = client.get("/api/photos/missing").get_json()
    assert payload["status"] == "not_ready", payload
    assert payload["photos"] == []


def test_api_audit_import_untracked_invalidates_missing_cache(
    app_and_db, tmp_path, monkeypatch,
):
    """Audit imports must clear the Missing Originals cache after scanning.

    Regression: ``audit.import_untracked`` runs scanner.scan over the selected
    parent directory, so it can reconcile a restored original. Without cache
    invalidation, the banner and modal keep serving the pre-import ghost list.
    """
    app, db = app_and_db
    client = app.test_client()

    img_path = tmp_path / "shoot" / "IMG_0001.JPG"
    img_path.parent.mkdir()
    img_path.write_bytes(b"stub")

    key = (db._db_path, db._active_workspace_id, None)
    with app._missing_originals_lock:
        app._missing_originals_cache[key] = {
            "photos": [{"id": 5555, "filename": "stale.jpg"}],
            "checked_at": "2026-01-01T00:00:00Z",
            "set_at": 0.0,
        }

    import audit as audit_module
    import metadata

    def _stub_import_untracked(db_arg, paths, **kwargs):
        assert db_arg._db_path == db._db_path
        assert paths == [str(img_path)]

    monkeypatch.setattr(audit_module, "import_untracked", _stub_import_untracked)
    monkeypatch.setattr(metadata, "exiftool_available", lambda: True)

    resp = client.post(
        "/api/audit/import-untracked",
        json={"paths": [str(img_path)]},
    )
    assert resp.status_code == 200, resp.get_json()
    assert resp.get_json()["imported"] == 1
    with app._missing_originals_lock:
        assert key not in app._missing_originals_cache


def test_api_duplicates_delete_loser_files_invalidates_missing_cache(
    app_and_db, monkeypatch, tmp_path,
):
    """Trashing duplicate losers must clear the Missing Originals cache.

    Regression: the duplicate-cleanup path calls ``db.delete_photos`` after
    trashing loser files; without invalidation, a photo removed here still
    shows up as a ghost in the banner/modal until the next rescan.
    """
    app, db = app_and_db
    client = app.test_client()

    real_dir = tmp_path / "live"
    real_dir.mkdir()
    fid = db.add_folder(str(real_dir), name="live")
    winner_file = real_dir / "winner.jpg"
    winner_file.write_bytes(b"jpeg-winner")
    loser_file = real_dir / "loser.jpg"
    loser_file.write_bytes(b"jpeg-loser")

    # Add the photos without a hash so the ``add_photo`` auto-resolve
    # hook doesn't reject either row on insert. We then set the shared
    # hash + the loser's rejected flag directly, which is the exact
    # state left behind by an earlier ``apply_duplicate_resolution``.
    pid_winner = db.add_photo(
        folder_id=fid, filename="winner.jpg", extension=".jpg",
        file_size=len(b"jpeg-winner"), file_mtime=1.0,
    )
    pid_loser = db.add_photo(
        folder_id=fid, filename="loser.jpg", extension=".jpg",
        file_size=len(b"jpeg-loser"), file_mtime=2.0,
    )
    db.conn.execute(
        "UPDATE photos SET file_hash='deadbeef' WHERE id IN (?, ?)",
        (pid_winner, pid_loser),
    )
    db.conn.execute(
        "UPDATE photos SET flag='rejected' WHERE id=?", (pid_loser,),
    )
    db.conn.commit()
    # Make the winner the anchor row and drop unrelated seed photos so the
    # missing-originals scan is small and deterministic.
    db.delete_photos([
        row["id"] for row in db.conn.execute(
            "SELECT id FROM photos WHERE folder_id != ? AND id NOT IN (?, ?)",
            (fid, pid_winner, pid_loser),
        ).fetchall()
    ])

    # Introduce a ghost row so the cache actually contains something to
    # invalidate — otherwise the "cache cleared" assertion below is
    # trivially satisfied even without the fix.
    pid_ghost = db.add_photo(
        folder_id=fid, filename="ghost.NEF", extension=".nef",
        file_size=1, file_mtime=3.0,
    )
    cached = _run_missing_originals_check(client)
    assert pid_ghost in [row["id"] for row in cached["photos"]]

    # Stub send2trash so the test doesn't shell out to the platform trash
    # implementation; the endpoint's contract is "trashed then row-deleted"
    # and only the row-delete side is what invalidates the cache.
    import send2trash as _send2trash_mod

    def fake_send2trash(path):
        with contextlib.suppress(FileNotFoundError):
            os.remove(path)

    monkeypatch.setattr(_send2trash_mod, "send2trash", fake_send2trash)

    resp = client.post(
        "/api/duplicates/delete-loser-files",
        json={"photo_ids": [pid_loser]},
    )
    assert resp.status_code == 200, resp.get_json()
    assert resp.get_json()["trashed"] == 1

    payload = client.get("/api/photos/missing").get_json()
    assert payload["status"] == "not_ready", payload
    assert payload["photos"] == []


def test_batch_delete_invalidates_missing_cache_across_workspaces(
    app_and_db, tmp_path,
):
    """Deleting a photo must invalidate every workspace's Missing Originals cache.

    Regression: photos are global (a folder can be linked into more than
    one workspace), so removing a row from workspace A must clear
    workspace B's ready cache too. Otherwise switching to B keeps serving
    a stale payload that still lists the just-deleted photo until B's
    next scan.
    """
    app, db = app_and_db
    client = app.test_client()

    default_ws = db._active_workspace_id
    other_ws = db.create_workspace("Other")

    shared_dir = tmp_path / "shared"
    shared_dir.mkdir()
    fid = db.add_folder(str(shared_dir), name="shared")
    db.add_workspace_folder(other_ws, fid)

    pid_ghost = db.add_photo(
        folder_id=fid,
        filename="ghost.NEF",
        extension=".nef",
        file_size=42,
        file_mtime=2.0,
    )
    db.delete_photos([
        row["id"] for row in db.conn.execute(
            "SELECT id FROM photos WHERE folder_id != ?", (fid,)
        ).fetchall()
    ])

    # Populate the cache for both workspaces so there's actually
    # something for the delete path to invalidate in each.
    db.set_active_workspace(other_ws)
    other_cache = _run_missing_originals_check(client)
    assert [row["id"] for row in other_cache["photos"]] == [pid_ghost]

    db.set_active_workspace(default_ws)
    default_cache = _run_missing_originals_check(client)
    assert [row["id"] for row in default_cache["photos"]] == [pid_ghost]

    # Delete the ghost while workspace A is active.
    resp = client.post(
        "/api/batch/delete",
        json={"photo_ids": [pid_ghost], "mode": "vireo"},
    )
    assert resp.status_code == 200, resp.get_json()
    assert resp.get_json()["deleted"] == 1

    # The active workspace's cache is gone …
    payload = client.get("/api/photos/missing").get_json()
    assert payload["status"] == "not_ready", payload
    assert payload["photos"] == []

    # … and so is workspace B's, even though it wasn't the caller.
    db.set_active_workspace(other_ws)
    payload = client.get("/api/photos/missing").get_json()
    assert payload["status"] == "not_ready", payload
    assert payload["photos"] == []


def test_api_photos_missing_delete_sidecars(app_and_db, tmp_path):
    """POST /api/photos/missing/delete-sidecars removes orphan XMP files for ghost photos.

    Safety guards:
    - Only acts on photo_ids that exist in the active workspace.
    - Only deletes .xmp/.XMP, only when the source is genuinely missing.
    """
    app, db = app_and_db
    client = app.test_client()

    real_dir = tmp_path / "live"
    real_dir.mkdir()
    # Ghost: NEF gone, XMP remains
    ghost_xmp = real_dir / "ghost.xmp"
    ghost_xmp.write_text("<x:xmpmeta/>")
    # Decoy: original still there — endpoint must refuse to touch its sidecar
    decoy_jpg = real_dir / "decoy.jpg"
    decoy_jpg.write_bytes(b"jpeg")
    decoy_xmp = real_dir / "decoy.xmp"
    decoy_xmp.write_text("<x:xmpmeta/>")

    fid = db.add_folder(str(real_dir), name="live")
    pid_ghost = db.add_photo(folder_id=fid, filename="ghost.NEF",
                             extension=".nef", file_size=1, file_mtime=1.0)
    pid_decoy = db.add_photo(folder_id=fid, filename="decoy.jpg",
                             extension=".jpg", file_size=1, file_mtime=1.0)

    resp = client.post("/api/photos/missing/delete-sidecars", json={
        "photo_ids": [pid_ghost, pid_decoy],
    })
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["deleted"] == 1
    assert data["skipped"] == 1
    assert not ghost_xmp.exists(), "ghost sidecar should be deleted"
    assert decoy_xmp.exists(), "sidecar with present original must not be deleted"


def test_api_photos_missing_remove_skips_restored_originals(app_and_db, tmp_path):
    """POST /api/photos/missing/remove re-checks each original before deletion.

    Ready /api/photos/missing payloads are cached for up to 30 minutes without
    a filesystem recheck. If a user restores a file between the last scan and
    clicking Remove, trusting the cache would delete a valid Vireo row. The
    endpoint's job is to re-check per photo and refuse to delete rows whose
    original came back.
    """
    app, db = app_and_db
    client = app.test_client()

    real_dir = tmp_path / "live"
    real_dir.mkdir()
    # Ghost stays missing: nothing on disk for this row.
    fid = db.add_folder(str(real_dir), name="live")
    pid_ghost = db.add_photo(folder_id=fid, filename="ghost.NEF",
                             extension=".nef", file_size=1, file_mtime=1.0)
    # Restored: original file is back on disk (simulated), sidecar too.
    pid_restored = db.add_photo(folder_id=fid, filename="restored.NEF",
                                extension=".nef", file_size=1, file_mtime=1.0)
    (real_dir / "restored.NEF").write_bytes(b"raw")
    (real_dir / "restored.xmp").write_text("<x:xmpmeta/>")
    # Ghost sidecar left on disk — should be cleaned when delete_sidecars=True.
    (real_dir / "ghost.xmp").write_text("<x:xmpmeta/>")

    resp = client.post("/api/photos/missing/remove", json={
        "photo_ids": [pid_ghost, pid_restored],
        "delete_sidecars": True,
        "mode": "vireo",
    })
    assert resp.status_code == 200, resp.get_data(as_text=True)
    data = resp.get_json()
    assert data["deleted"] == 1
    assert data["restored"] == [pid_restored]
    assert data["folder_offline"] == []
    assert data["skipped"] == 0
    assert data["sidecars_deleted"] == 1

    # Ghost row is gone, restored row is kept.
    assert db.get_photo(pid_ghost) is None
    assert db.get_photo(pid_restored) is not None
    # Ghost sidecar was cleaned up, restored one was left alone.
    assert not (real_dir / "ghost.xmp").exists()
    assert (real_dir / "restored.xmp").exists()


def test_api_photos_missing_remove_skips_offline_folder(app_and_db, tmp_path):
    """POST /api/photos/missing/remove must not delete rows from an offline folder.

    A ready ``/api/photos/missing`` cache is served for up to 30 minutes
    without a filesystem recheck. If a NAS/SMB mount goes offline between
    the last scan and the user clicking Remove, ``os.path.exists(src)``
    returns False for every row — but that is evidence the folder is
    unreachable, not that the originals are gone. If any file was
    restored before the mount dropped, a naive "still missing" check
    would silently delete a valid Vireo row. The endpoint must skip
    deletion when the folder is unreachable and surface the deferred
    IDs so the modal can explain what happened.
    """
    app, db = app_and_db
    client = app.test_client()

    real_dir = tmp_path / "live"
    real_dir.mkdir()
    fid = db.add_folder(str(real_dir), name="live")
    pid = db.add_photo(folder_id=fid, filename="ghost.NEF",
                       extension=".nef", file_size=1, file_mtime=1.0)
    # Ghost sidecar left on disk — must not be deleted while folder offline.
    (real_dir / "ghost.xmp").write_text("<x:xmpmeta/>")

    # Simulate the parent folder/NAS mount going offline by removing the
    # folder from disk after the DB row is set up. ``os.path.isdir`` will
    # now return False so the endpoint must treat this as ambiguous.
    import shutil
    shutil.rmtree(real_dir)

    resp = client.post("/api/photos/missing/remove", json={
        "photo_ids": [pid],
        "delete_sidecars": True,
        "mode": "vireo",
    })
    assert resp.status_code == 200, resp.get_data(as_text=True)
    data = resp.get_json()
    assert data["deleted"] == 0
    assert data["restored"] == []
    assert data["folder_offline"] == [pid]
    assert data["sidecars_deleted"] == 0
    # Row must still be present — we did not delete it.
    assert db.get_photo(pid) is not None


def test_api_photos_missing_remove_skips_unreadable_folder(
    app_and_db, tmp_path, monkeypatch,
):
    """A present but unreadable folder is still unverified for removal.

    Regression: ``os.path.isdir`` can succeed for a NAS/local folder that the
    process cannot traverse. In that state ``os.path.exists(child)`` may return
    false for every original, so removal must defer instead of deleting rows.
    """
    app, db = app_and_db
    client = app.test_client()

    real_dir = tmp_path / "live"
    real_dir.mkdir()
    fid = db.add_folder(str(real_dir), name="live")
    pid = db.add_photo(
        folder_id=fid,
        filename="ghost.NEF",
        extension=".nef",
        file_size=1,
        file_mtime=1.0,
    )
    sidecar = real_dir / "ghost.xmp"
    sidecar.write_text("<x:xmpmeta/>")

    import app as app_module

    real_scandir = app_module.os.scandir

    def unreadable_scandir(path):
        if os.fspath(path) == str(real_dir):
            raise PermissionError("permission denied")
        return real_scandir(path)

    monkeypatch.setattr(app_module.os, "scandir", unreadable_scandir)

    resp = client.post("/api/photos/missing/remove", json={
        "photo_ids": [pid],
        "delete_sidecars": True,
        "mode": "vireo",
    })
    assert resp.status_code == 200, resp.get_data(as_text=True)
    data = resp.get_json()
    assert data["deleted"] == 0
    assert data["restored"] == []
    assert data["folder_offline"] == [pid]
    assert data["sidecars_deleted"] == 0
    assert sidecar.exists()
    assert db.get_photo(pid) is not None


def test_api_photos_missing_delete_sidecars_skips_offline_folder(app_and_db, tmp_path):
    """delete-sidecars must skip when the parent folder is unreachable.

    Same reasoning as the /remove endpoint: an offline mount makes
    ``os.path.exists`` uninformative, so touching the .xmp for a photo
    whose original may have been restored risks deleting a valid sidecar.
    """
    app, db = app_and_db
    client = app.test_client()

    real_dir = tmp_path / "live"
    real_dir.mkdir()
    fid = db.add_folder(str(real_dir), name="live")
    pid = db.add_photo(folder_id=fid, filename="ghost.NEF",
                       extension=".nef", file_size=1, file_mtime=1.0)

    import shutil
    shutil.rmtree(real_dir)

    resp = client.post("/api/photos/missing/delete-sidecars", json={
        "photo_ids": [pid],
    })
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["deleted"] == 0
    assert data["skipped"] == 1


def test_api_photos_missing_delete_sidecars_skips_unreadable_folder(
    app_and_db, tmp_path, monkeypatch,
):
    """Present-but-unreadable folder must not have its sidecars deleted.

    Regression: ``os.path.isdir`` returns True for a NAS/local folder the
    process cannot traverse; in that state ``os.path.exists(child)`` may
    return false for every original even though the file is still there,
    so unlinking the paired .xmp would remove a valid sidecar. Mirrors
    the ``/api/photos/missing/remove`` accessibility check.
    """
    app, db = app_and_db
    client = app.test_client()

    real_dir = tmp_path / "live"
    real_dir.mkdir()
    fid = db.add_folder(str(real_dir), name="live")
    pid = db.add_photo(
        folder_id=fid,
        filename="ghost.NEF",
        extension=".nef",
        file_size=1,
        file_mtime=1.0,
    )
    sidecar = real_dir / "ghost.xmp"
    sidecar.write_text("<x:xmpmeta/>")

    import app as app_module

    real_scandir = app_module.os.scandir

    def unreadable_scandir(path):
        if os.fspath(path) == str(real_dir):
            raise PermissionError("permission denied")
        return real_scandir(path)

    monkeypatch.setattr(app_module.os, "scandir", unreadable_scandir)

    resp = client.post("/api/photos/missing/delete-sidecars", json={
        "photo_ids": [pid],
    })
    assert resp.status_code == 200, resp.get_data(as_text=True)
    data = resp.get_json()
    assert data["deleted"] == 0
    assert data["skipped"] == 1
    assert sidecar.exists()


def test_api_photos_missing_remove_rejects_ids_outside_workspace(app_and_db, tmp_path):
    """Unknown or out-of-workspace photo_ids must not affect anything.

    Symmetry with /api/photos/missing/delete-sidecars: the endpoint resolves
    IDs against the active workspace and refuses to touch rows or files it
    can't own.
    """
    app, db = app_and_db
    client = app.test_client()

    resp = client.post("/api/photos/missing/remove", json={
        "photo_ids": [999_999],
        "mode": "vireo",
    })
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["deleted"] == 0
    assert data["restored"] == []
    assert data["folder_offline"] == []
    assert data["skipped"] == 1


def test_api_photos_missing_delete_sidecars_rejects_untracked_paths(app_and_db, tmp_path):
    """Endpoint must not delete .xmp files outside the active workspace.

    Regression: an earlier draft accepted client-supplied (folder_path, filename)
    pairs and unlinked any matching .xmp on disk as long as the named "original"
    was absent. That allowed a crafted request to delete arbitrary .xmp files.
    The fix: only act on photo_ids that resolve to a row in the active workspace.
    """
    app, db = app_and_db
    client = app.test_client()

    # An attacker-controlled directory entirely outside Vireo's library —
    # we drop a sidecar there to confirm the endpoint won't touch it.
    untracked_dir = tmp_path / "outside"
    untracked_dir.mkdir()
    poached = untracked_dir / "victim.xmp"
    poached.write_text("<x:xmpmeta/>")

    # A high photo id that doesn't exist in the DB (active workspace).
    resp = client.post("/api/photos/missing/delete-sidecars", json={
        "photo_ids": [999_999],
    })
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["deleted"] == 0
    assert data["skipped"] == 1
    assert poached.exists(), "untracked sidecar must not be touched"


def test_api_photos_missing_detects_preview_variants(app_and_db, tmp_path):
    """has_preview must catch both `{id}_{size}.jpg` and legacy `{id}.jpg`.

    Indexing the cache once per request (instead of per-photo glob) is the
    perf optimization; this test proves both filename shapes still land as
    True so the behavior change is invisible to callers.
    """
    from PIL import Image
    app, db = app_and_db
    client = app.test_client()

    real_dir = tmp_path / "live"
    real_dir.mkdir()
    fid = db.add_folder(str(real_dir), name="live")
    pid_sized = db.add_photo(folder_id=fid, filename="sized.NEF",
                             extension=".nef", file_size=1, file_mtime=1.0)
    pid_legacy = db.add_photo(folder_id=fid, filename="legacy.NEF",
                              extension=".nef", file_size=1, file_mtime=1.0)
    pid_no_preview = db.add_photo(folder_id=fid, filename="bare.NEF",
                                  extension=".nef", file_size=1, file_mtime=1.0)
    db.delete_photos([
        row["id"] for row in db.conn.execute(
            "SELECT id FROM photos WHERE folder_id != ?", (fid,)
        ).fetchall()
    ])

    thumb_dir = app.config["THUMB_CACHE_DIR"]
    vireo_dir = os.path.dirname(thumb_dir)
    preview_dir = os.path.join(vireo_dir, "previews")
    os.makedirs(preview_dir, exist_ok=True)
    Image.new("RGB", (10, 10)).save(os.path.join(preview_dir, f"{pid_sized}_1920.jpg"))
    Image.new("RGB", (10, 10)).save(os.path.join(preview_dir, f"{pid_legacy}.jpg"))
    # Stray non-numeric filename in the cache must not crash the indexer.
    Image.new("RGB", (10, 10)).save(os.path.join(preview_dir, "stray.jpg"))

    payload = _run_missing_originals_check(client)
    by_id = {r["id"]: r for r in payload["photos"]}
    assert by_id[pid_sized]["has_preview"] is True
    assert by_id[pid_legacy]["has_preview"] is True
    assert by_id[pid_no_preview]["has_preview"] is False


def test_api_photos_missing_reports_default_working_copy(app_and_db, tmp_path):
    """has_working_copy must reflect on-disk reality even when working_copy_path is NULL.

    Legacy rows backfilled before working_copy_path was tracked still have a
    file at <vireo>/working/<id>.jpg, and the batch-delete path removes it
    on row removal. If the modal said no working copy existed, users would
    decide to delete based on stale info.
    """
    from PIL import Image
    app, db = app_and_db
    client = app.test_client()

    real_dir = tmp_path / "live"
    real_dir.mkdir()
    fid = db.add_folder(str(real_dir), name="live")
    pid = db.add_photo(folder_id=fid, filename="ghost.NEF",
                       extension=".nef", file_size=1, file_mtime=1.0)
    db.delete_photos([
        row["id"] for row in db.conn.execute(
            "SELECT id FROM photos WHERE folder_id != ?", (fid,)
        ).fetchall()
    ])

    # Drop a working-copy file at the default location, leave the DB
    # column NULL — same shape legacy/backfill state has.
    thumb_dir = app.config["THUMB_CACHE_DIR"]
    vireo_dir = os.path.dirname(thumb_dir)
    working_dir = os.path.join(vireo_dir, "working")
    os.makedirs(working_dir, exist_ok=True)
    Image.new("RGB", (10, 10)).save(os.path.join(working_dir, f"{pid}.jpg"))
    assert db.get_photo(pid)["working_copy_path"] is None

    data = _run_missing_originals_check(client)["photos"]
    assert len(data) == 1
    assert data[0]["has_working_copy"] is True


def test_api_photos_missing_excludes_present_files(app_and_db, tmp_path):
    """A photo whose source still exists must not show up as missing."""
    from PIL import Image
    app, db = app_and_db
    client = app.test_client()

    real_dir = tmp_path / "ok"
    real_dir.mkdir()
    Image.new("RGB", (10, 10)).save(real_dir / "still_here.jpg")
    fid = db.add_folder(str(real_dir), name="ok")
    db.add_photo(folder_id=fid, filename="still_here.jpg", extension=".jpg",
                 file_size=1, file_mtime=1.0)
    db.delete_photos([
        row["id"] for row in db.conn.execute(
            "SELECT id FROM photos WHERE folder_id != ?", (fid,)
        ).fetchall()
    ])

    payload = _run_missing_originals_check(client)
    assert payload["photos"] == []


def test_api_folder_relocate(app_and_db, tmp_path):
    """POST /api/folders/<id>/relocate updates path and status."""
    app, db = app_and_db
    fid = db.conn.execute("SELECT id FROM folders WHERE path = '/photos/2024'").fetchone()["id"]
    db.conn.execute("UPDATE folders SET status = 'missing' WHERE id = ?", (fid,))
    db.conn.commit()

    new_path = str(tmp_path / "relocated")
    os.makedirs(new_path)

    client = app.test_client()
    resp = client.post(f"/api/folders/{fid}/relocate", json={"path": new_path})
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["status"] == "ok"

    row = db.conn.execute("SELECT status, path FROM folders WHERE id = ?", (fid,)).fetchone()
    assert row["status"] == "ok"
    assert row["path"] == new_path


def test_api_folder_relocate_conflict_after_revalidation_invalidates_missing_cache(
    app_and_db, tmp_path,
):
    """A relocate conflict can still mutate folder health and stale the cache.

    ``Database.relocate_folder`` revalidates a missing source folder whose old
    path came back online, commits ``status='ok'``, then raises if the requested
    new path is already tracked. The API returns 409, but the ready Missing
    Originals cache must still be dropped because the folder is now online.
    """
    app, db = app_and_db
    source_path = tmp_path / "source"
    target_path = tmp_path / "target"
    source_path.mkdir()
    target_path.mkdir()
    source_id = db.add_folder(str(source_path), name="source")
    db.add_folder(str(target_path), name="target")
    db.conn.execute(
        "UPDATE folders SET status = 'missing' WHERE id = ?", (source_id,)
    )
    db.conn.commit()

    key = (db._db_path, db._active_workspace_id, None)
    with app._missing_originals_lock:
        app._missing_originals_cache[key] = {
            "photos": [{"id": 8888, "filename": "hidden.jpg"}],
            "checked_at": "2026-01-01T00:00:00Z",
            "set_at": 0.0,
        }

    client = app.test_client()
    resp = client.post(
        f"/api/folders/{source_id}/relocate",
        json={"path": str(target_path)},
    )
    assert resp.status_code == 409
    row = db.conn.execute(
        "SELECT status FROM folders WHERE id = ?", (source_id,)
    ).fetchone()
    assert row["status"] == "ok"
    with app._missing_originals_lock:
        assert key not in app._missing_originals_cache


def test_api_folder_relocate_rebases_configured_developed_dir(app_and_db, tmp_path):
    """POST /api/folders/<id>/relocate rebases the darktable output subdir.

    Regression: /api/folders/<id>/relocate only called db.relocate_folder,
    never invoking relocate_developed_dir. Any previously-developed photo
    in a relocated folder would silently fall back to RAW on export
    because the path-derived key changes when the folder's path changes.
    """
    import config as cfg
    from export import developed_folder_key

    app, db = app_and_db

    old_dir = str(tmp_path / "old_birds")
    new_dir = str(tmp_path / "new_birds")
    os.makedirs(new_dir)

    fid = db.add_folder(old_dir, name="birds")
    db.conn.execute("UPDATE folders SET status = 'missing' WHERE id = ?", (fid,))
    db.conn.commit()

    developed = tmp_path / "darktable_out"
    developed.mkdir()
    old_key = developed_folder_key(old_dir)
    (developed / old_key).mkdir()
    (developed / old_key / "IMG_0001.jpg").write_bytes(b"developed-bytes")

    cfg.save({"darktable_output_dir": str(developed)})

    client = app.test_client()
    resp = client.post(f"/api/folders/{fid}/relocate", json={"path": new_dir})
    assert resp.status_code == 200

    new_key = developed_folder_key(new_dir)
    assert old_key != new_key
    assert not (developed / old_key).exists(), "old developed subdir should be gone"
    assert (developed / new_key / "IMG_0001.jpg").read_bytes() == b"developed-bytes"


def test_api_folder_relocate_rebases_cascaded_child_developed_dirs(app_and_db, tmp_path):
    """Cascaded child folders must also have their developed subdirs rebased."""
    import config as cfg
    from export import developed_folder_key

    app, db = app_and_db

    old_parent = str(tmp_path / "old_parent")
    new_parent = str(tmp_path / "new_parent")
    old_child = os.path.join(old_parent, "child")
    new_child = os.path.join(new_parent, "child")
    os.makedirs(new_child)

    pfid = db.add_folder(old_parent, name="parent")
    cfid = db.add_folder(old_child, name="child", parent_id=pfid)
    db.conn.execute(
        "UPDATE folders SET status = 'missing' WHERE id IN (?, ?)", (pfid, cfid)
    )
    db.conn.commit()

    developed = tmp_path / "darktable_out"
    developed.mkdir()
    old_child_key = developed_folder_key(old_child)
    (developed / old_child_key).mkdir()
    (developed / old_child_key / "IMG_0002.jpg").write_bytes(b"child-developed")

    cfg.save({"darktable_output_dir": str(developed)})

    client = app.test_client()
    resp = client.post(f"/api/folders/{pfid}/relocate", json={"path": new_parent})
    assert resp.status_code == 200

    new_child_key = developed_folder_key(new_child)
    assert old_child_key != new_child_key
    assert not (developed / old_child_key).exists()
    assert (developed / new_child_key / "IMG_0002.jpg").read_bytes() == b"child-developed"


def test_api_folder_relocate_merge(app_and_db, tmp_path):
    """POST /api/folders/<id>/relocate merges into existing folder when paths conflict."""
    app, db = app_and_db

    dir_a = str(tmp_path / "folder_a")
    dir_b = str(tmp_path / "folder_b")
    os.makedirs(dir_a)
    os.makedirs(dir_b)

    fid_a = db.add_folder(dir_a, name="a")
    fid_b = db.add_folder(dir_b, name="b")
    db.conn.execute("UPDATE folders SET status = 'missing' WHERE id = ?", (fid_a,))
    db.conn.commit()

    # Remove dir_a from disk so the source is truly missing
    os.rmdir(dir_a)

    # Create photo1.jpg on disk in the target folder
    (tmp_path / "folder_b" / "photo1.jpg").write_bytes(b"\xff\xd8")

    # Add a photo to each folder
    db.add_photo(fid_a, "photo1.jpg", ".jpg", 1000, 1.0)
    db.add_photo(fid_b, "photo2.jpg", ".jpg", 1000, 1.0)

    client = app.test_client()
    resp = client.post(f"/api/folders/{fid_a}/relocate", json={"path": dir_b})
    assert resp.status_code == 200
    assert resp.get_json()["status"] == "ok"

    # Folder A should be gone
    assert db.conn.execute("SELECT id FROM folders WHERE id = ?", (fid_a,)).fetchone() is None
    # Both photos should now be in folder B
    photos = db.conn.execute("SELECT filename FROM photos WHERE folder_id = ?", (fid_b,)).fetchall()
    filenames = {p["filename"] for p in photos}
    assert filenames == {"photo1.jpg", "photo2.jpg"}


def test_api_folder_delete(app_and_db):
    """DELETE /api/folders/<id> removes folder and its photos."""
    app, db = app_and_db
    fid = db.conn.execute("SELECT id FROM folders WHERE path = '/photos/2024/January'").fetchone()["id"]
    photo_count_before = db.conn.execute(
        "SELECT COUNT(*) FROM photos WHERE folder_id = ?", (fid,)
    ).fetchone()[0]
    assert photo_count_before > 0

    client = app.test_client()
    resp = client.delete(f"/api/folders/{fid}")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["deleted_photos"] == photo_count_before

    assert db.conn.execute("SELECT id FROM folders WHERE id = ?", (fid,)).fetchone() is None


def test_api_folder_delete_removes_preview_files(app_and_db, tmp_path):
    """Folder delete must unlink on-disk preview files, not just drop DB rows.

    The preview_cache FK cascades on photo delete, so rows vanish — but
    unless we explicitly unlink the files, they become untracked bytes
    that eviction can't reclaim.
    """
    app, db = app_and_db
    fid = db.conn.execute(
        "SELECT id FROM folders WHERE path = '/photos/2024/January'"
    ).fetchone()["id"]
    photo_ids = [r["id"] for r in db.conn.execute(
        "SELECT id FROM photos WHERE folder_id = ?", (fid,)
    ).fetchall()]
    assert photo_ids

    thumb_dir = app.config["THUMB_CACHE_DIR"]
    vireo_dir = os.path.dirname(thumb_dir)
    preview_dir = os.path.join(vireo_dir, "previews")
    os.makedirs(preview_dir, exist_ok=True)

    created = []
    for pid in photo_ids:
        sized = os.path.join(preview_dir, f"{pid}_1920.jpg")
        legacy = os.path.join(preview_dir, f"{pid}.jpg")
        with open(sized, "wb") as f:
            f.write(b"\xff\xd8\xff\xd9")  # minimal JPEG SOI/EOI
        with open(legacy, "wb") as f:
            f.write(b"\xff\xd8\xff\xd9")
        db.preview_cache_insert(pid, 1920, 4)
        created.extend([sized, legacy])

    client = app.test_client()
    resp = client.delete(f"/api/folders/{fid}")
    assert resp.status_code == 200

    for path in created:
        assert not os.path.exists(path), f"Preview file leaked after folder delete: {path}"


def test_folder_health_check_runs_at_startup(app_and_db):
    """The app marks non-existent folders as missing after startup."""
    app, db = app_and_db
    # Folders in test fixture use fake paths that don't exist on disk.
    # The health check should mark them missing.
    changed = db.check_folder_health()
    assert changed >= 1  # /photos/2024 and /photos/2024/January don't exist

    missing = db.get_missing_folders()
    assert len(missing) >= 1


def test_highlights_page(app_and_db):
    """GET /highlights returns 200."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get("/highlights")
    assert resp.status_code == 200


def test_highlights_page_renders_after_redesign(app_and_db):
    """The page template still renders against the new API shape."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get("/highlights")
    assert resp.status_code == 200
    assert b"Auto-ID confidence" in resp.data
    assert b"Per row" in resp.data


def test_highlights_get_includes_unscored_photos(app_and_db):
    """Unscored photos now surface on Highlights (the page used to be empty
    until analysis ran). The fixture's three unanalyzed photos — none carrying
    an accepted species keyword — appear in the unidentified section, marked
    not-yet-analyzed so the divider can label them."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get("/api/highlights")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["buckets"] == []
    unid = data["unidentified"]
    assert unid["photo_count"] == 3
    assert unid["unanalyzed_count"] == 3
    assert all(p["is_analyzed"] is False for p in unid["photos"])
    assert data["meta"]["eligible"] == 3


def test_highlights_buckets_by_accepted_species(app_and_db):
    """Photos with accepted species keywords populate species buckets."""
    app, db = app_and_db
    client = app.test_client()
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES ('/b', 'b', 'ok')"
    ).lastrowid
    db.conn.execute(
        "INSERT INTO workspace_folders (workspace_id, folder_id) VALUES (?, ?)",
        (db._ws_id(), fid),
    )
    apapane_kw = db.conn.execute(
        "INSERT INTO keywords (name, type, is_species) VALUES ('ʻApapane', 'taxonomy', 1)"
    ).lastrowid
    for i, q in enumerate([0.9, 0.7, 0.5]):
        pid = db.conn.execute(
            "INSERT INTO photos (folder_id, filename, quality_score, flag) "
            "VALUES (?, ?, ?, 'none')",
            (fid, f"a{i}.jpg", q),
        ).lastrowid
        db.conn.execute(
            "INSERT INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
            (pid, apapane_kw),
        )
    db.conn.commit()

    resp = client.get(f"/api/highlights?folder_id={fid}")
    assert resp.status_code == 200
    data = resp.get_json()
    assert len(data["buckets"]) == 1
    bucket = data["buckets"][0]
    assert bucket["species"] == "ʻApapane"
    assert bucket["is_accepted"] is True
    assert bucket["photo_count"] == 3
    assert bucket["best_quality"] == 0.9
    # Photos ordered by quality_score desc
    qs = [p["quality_score"] for p in bucket["photos"]]
    assert qs == sorted(qs, reverse=True)


def test_highlights_expose_pre_pick_base_score(app_and_db):
    """Each photo carries `highlight_base_score` = pre-pick-bonus baseline.

    Regression for Codex feedback on PR #1176 (line 874): the client-side
    lightbox pick handler must recompute highlight_score without losing
    precision to clamping. A flagged photo whose raw score is 0.97 gets
    +0.08 → 1.05, clamped and cached as highlight_score=1.0; subtracting
    the bonus on unpick would give 0.92 while the correct value is 0.97.
    Exposing the pre-bonus baseline lets the client compute the exact same
    value the backend would on a full reload.
    """
    app, db = app_and_db
    client = app.test_client()
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES ('/b', 'b', 'ok')"
    ).lastrowid
    db.conn.execute(
        "INSERT INTO workspace_folders (workspace_id, folder_id) VALUES (?, ?)",
        (db._ws_id(), fid),
    )
    robin_kw = db.conn.execute(
        "INSERT INTO keywords (name, type, is_species) VALUES ('Robin', 'taxonomy', 1)"
    ).lastrowid
    picked_id = db.conn.execute(
        "INSERT INTO photos (folder_id, filename, quality_score, flag) "
        "VALUES (?, 'picked.jpg', 0.97, 'flagged')",
        (fid,),
    ).lastrowid
    unpicked_id = db.conn.execute(
        "INSERT INTO photos (folder_id, filename, quality_score, flag) "
        "VALUES (?, 'unpicked.jpg', 0.60, 'none')",
        (fid,),
    ).lastrowid
    for pid in (picked_id, unpicked_id):
        db.conn.execute(
            "INSERT INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
            (pid, robin_kw),
        )
    db.conn.commit()

    resp = client.get(f"/api/highlights?folder_id={fid}")
    assert resp.status_code == 200
    photos = {p["id"]: p for p in resp.get_json()["buckets"][0]["photos"]}
    # Picked photo: 0.97 + 0.08 = 1.05, clamped to 1.0 for highlight_score.
    # highlight_base_score keeps the pre-bonus 0.97 so the client can
    # reverse the pick without under-counting.
    assert photos[picked_id]["highlight_score"] == 1.0
    assert photos[picked_id]["highlight_base_score"] == 0.97
    # Unpicked photo: no bonus, so base == score.
    assert photos[unpicked_id]["highlight_score"] == 0.60
    assert photos[unpicked_id]["highlight_base_score"] == 0.60


def test_highlights_bucket_returns_full_bucket_ordering_keys(app_and_db):
    """/api/highlights/bucket returns full-bucket best_score/best_timestamp
    so a client refetch of a paged bucket doesn't shrink the ordering
    keys to just the loaded window.

    Regression for Codex feedback on PR #1176 (line 923): after a
    lightbox pick, the client calls refetchBucketFromZero to mirror the
    server's post-pick order. When has_more is still true (large bucket
    with a low limit) the response was only the first page, so the
    client recomputed bucket.best_score from that partial slice — if the
    highest-scored photo lives past the loaded window, the bucket's
    Recommended/Best sort key would silently shrink and the species
    would drop below its true position until a full reload.
    """
    app, db = app_and_db
    client = app.test_client()
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES ('/pg', 'pg', 'ok')"
    ).lastrowid
    db.conn.execute(
        "INSERT INTO workspace_folders (workspace_id, folder_id) VALUES (?, ?)",
        (db._ws_id(), fid),
    )
    kw = db.conn.execute(
        "INSERT INTO keywords (name, type, is_species) VALUES ('Robin', 'taxonomy', 1)"
    ).lastrowid
    # Three photos; the highest-scored one (0.9) sits at offset 2 so
    # a limit=1 request would miss it and a naive client recompute
    # from the loaded window would land on 0.7, not 0.9.
    photo_ids = []
    for filename, quality, ts in [
        ("hi.jpg", 0.7, 3000),
        ("mid.jpg", 0.5, 2000),
        ("top.jpg", 0.9, 1000),
    ]:
        pid = db.conn.execute(
            "INSERT INTO photos (folder_id, filename, quality_score, flag, timestamp) "
            "VALUES (?, ?, ?, 'none', ?)",
            (fid, filename, quality, ts),
        ).lastrowid
        db.conn.execute(
            "INSERT INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
            (pid, kw),
        )
        photo_ids.append(pid)
    db.conn.commit()

    resp = client.get(
        "/api/highlights/bucket",
        query_string={
            "folder_id": fid,
            "species": "Robin",
            "offset": 0,
            "limit": 1,
        },
    )
    assert resp.status_code == 200
    data = resp.get_json()
    # Only the first page came back...
    assert data["has_more"] is True
    assert len(data["photos"]) == 1
    # ...but the ordering keys reflect the ENTIRE filtered bucket, so
    # the client can copy them onto bucket.best_score / best_timestamp
    # without shrinking the sort key to whatever the loaded window
    # happens to hold.
    assert data["best_score"] == 0.9
    # best_timestamp mirrors the backend `top.get("timestamp")` on
    # photos[0] of the full ordered bucket, not the loaded slice.
    assert data["best_timestamp"] == data["photos"][0]["timestamp"]


def test_highlights_bucket_mixed_accepted_predicted_is_not_accepted(app_and_db):
    """A bucket whose photos are a mix of accepted-tag and prediction-only
    must report is_accepted=False. The "Keyword confirmed" badge means every
    photo in the row is keyword-confirmed, not just some of it."""
    app, db = app_and_db
    client = app.test_client()
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES ('/m', 'm', 'ok')"
    ).lastrowid
    db.conn.execute(
        "INSERT INTO workspace_folders (workspace_id, folder_id) VALUES (?, ?)",
        (db._ws_id(), fid),
    )
    apapane_kw = db.conn.execute(
        "INSERT INTO keywords (name, type, is_species) VALUES ('ʻApapane', 'taxonomy', 1)"
    ).lastrowid

    # Photo 1: accepted ʻApapane keyword.
    accepted_pid = db.conn.execute(
        "INSERT INTO photos (folder_id, filename, quality_score, flag) "
        "VALUES (?, 'a.jpg', 0.9, 'none')",
        (fid,),
    ).lastrowid
    db.conn.execute(
        "INSERT INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
        (accepted_pid, apapane_kw),
    )

    # Photo 2: no accepted keyword, only a prediction of ʻApapane above the
    # default threshold so it lands in the same bucket.
    predicted_pid = db.conn.execute(
        "INSERT INTO photos (folder_id, filename, quality_score, flag) "
        "VALUES (?, 'b.jpg', 0.7, 'none')",
        (fid,),
    ).lastrowid
    did = db.conn.execute(
        "INSERT INTO detections (photo_id, detector_confidence) VALUES (?, 0.9)",
        (predicted_pid,),
    ).lastrowid
    db.conn.execute(
        "INSERT INTO predictions (detection_id, classifier_model, species, confidence) "
        "VALUES (?, 'm', 'ʻApapane', 0.95)",
        (did,),
    )
    db.conn.commit()

    resp = client.get(f"/api/highlights?folder_id={fid}&confidence_threshold=0.7")
    assert resp.status_code == 200
    data = resp.get_json()
    assert len(data["buckets"]) == 1
    bucket = data["buckets"][0]
    assert bucket["species"] == "ʻApapane"
    assert bucket["photo_count"] == 2
    # Mixed bucket: one accepted, one prediction-only → NOT fully confirmed.
    assert bucket["is_accepted"] is False


def test_highlights_confirmation_filter_splits_confirmed_and_unconfirmed(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES ('/hf', 'hf', 'ok')"
    ).lastrowid
    db.conn.execute(
        "INSERT INTO workspace_folders (workspace_id, folder_id) VALUES (?, ?)",
        (db._ws_id(), fid),
    )
    apapane_kw = db.conn.execute(
        "INSERT INTO keywords (name, type, is_species) VALUES ('ʻApapane', 'taxonomy', 1)"
    ).lastrowid

    accepted_pid = db.conn.execute(
        "INSERT INTO photos (folder_id, filename, quality_score, flag) "
        "VALUES (?, 'accepted.jpg', 0.9, 'none')",
        (fid,),
    ).lastrowid
    db.conn.execute(
        "INSERT INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
        (accepted_pid, apapane_kw),
    )

    predicted_pid = db.conn.execute(
        "INSERT INTO photos (folder_id, filename, quality_score, flag) "
        "VALUES (?, 'predicted.jpg', 0.8, 'none')",
        (fid,),
    ).lastrowid
    did = db.conn.execute(
        "INSERT INTO detections (photo_id, detector_confidence) VALUES (?, 0.9)",
        (predicted_pid,),
    ).lastrowid
    db.conn.execute(
        "INSERT INTO predictions (detection_id, classifier_model, species, confidence) "
        "VALUES (?, 'm', 'ʻApapane', 0.95)",
        (did,),
    )

    unidentified_pid = db.conn.execute(
        "INSERT INTO photos (folder_id, filename, quality_score, flag) "
        "VALUES (?, 'unknown.jpg', 0.7, 'none')",
        (fid,),
    ).lastrowid
    db.conn.commit()

    resp = client.get(f"/api/highlights?folder_id={fid}&confirmation=all")
    data = resp.get_json()
    assert data["meta"]["eligible"] == 3
    assert data["buckets"][0]["photo_count"] == 2
    assert data["buckets"][0]["is_accepted"] is False
    assert data["unidentified"]["photo_count"] == 1

    resp = client.get(f"/api/highlights?folder_id={fid}&confirmation=confirmed")
    data = resp.get_json()
    assert data["meta"]["confirmation"] == "confirmed"
    assert data["meta"]["eligible"] == 1
    assert data["buckets"][0]["photo_count"] == 1
    assert data["buckets"][0]["photos"][0]["id"] == accepted_pid
    assert data["buckets"][0]["is_accepted"] is True
    assert data["unidentified"]["photo_count"] == 0

    resp = client.get(f"/api/highlights?folder_id={fid}&confirmation=unconfirmed")
    data = resp.get_json()
    assert data["meta"]["confirmation"] == "unconfirmed"
    assert data["meta"]["eligible"] == 2
    assert data["buckets"][0]["photo_count"] == 1
    assert data["buckets"][0]["photos"][0]["id"] == predicted_pid
    assert data["buckets"][0]["is_accepted"] is False
    assert data["unidentified"]["photo_count"] == 1
    assert data["unidentified"]["photos"][0]["id"] == unidentified_pid

    resp = client.get(
        "/api/highlights/bucket",
        query_string={
            "folder_id": fid,
            "species": "ʻApapane",
            "confirmation": "confirmed",
        },
    )
    data = resp.get_json()
    assert data["photo_count"] == 1
    assert data["photos"][0]["id"] == accepted_pid

    resp = client.get(
        "/api/highlights/bucket",
        query_string={
            "folder_id": fid,
            "species": "ʻApapane",
            "confirmation": "unconfirmed",
        },
    )
    data = resp.get_json()
    assert data["photo_count"] == 1
    assert data["photos"][0]["id"] == predicted_pid


def test_highlights_curation_filters_combine_independently(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES ('/curated', 'curated', 'ok')"
    ).lastrowid
    db.conn.execute(
        "INSERT INTO workspace_folders (workspace_id, folder_id) VALUES (?, ?)",
        (db._ws_id(), fid),
    )

    photo_ids = {}
    keyword_ids = {}
    for index, species in enumerate(("Alpha Bird", "Beta Bird", "Gamma Bird", "Delta Bird")):
        keyword_id = db.conn.execute(
            "INSERT INTO keywords (name, type, is_species) VALUES (?, 'taxonomy', 1)",
            (species,),
        ).lastrowid
        photo_id = db.conn.execute(
            "INSERT INTO photos (folder_id, filename, quality_score, flag) "
            "VALUES (?, ?, ?, 'none')",
            (fid, f"{index}.jpg", 0.9 - index * 0.1),
        ).lastrowid
        db.conn.execute(
            "INSERT INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
            (photo_id, keyword_id),
        )
        photo_ids[species] = photo_id
        keyword_ids[species] = keyword_id

    alpha_alternate = db.conn.execute(
        "INSERT INTO photos (folder_id, filename, quality_score, flag) "
        "VALUES (?, 'alpha-alternate.jpg', 0.65, 'none')",
        (fid,),
    ).lastrowid
    db.conn.execute(
        "INSERT INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
        (alpha_alternate, keyword_ids["Alpha Bird"]),
    )
    beta_alternate = db.conn.execute(
        "INSERT INTO photos (folder_id, filename, quality_score, flag) "
        "VALUES (?, 'beta-alternate.jpg', 0.6, 'none')",
        (fid,),
    ).lastrowid
    db.conn.execute(
        "INSERT INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
        (beta_alternate, keyword_ids["Beta Bird"]),
    )
    db.conn.commit()

    # Alpha has only a highlight, Beta only a representative, Gamma has both,
    # and Delta has neither. Every pair of filters should intersect cleanly.
    db.add_species_highlight("Alpha Bird", photo_ids["Alpha Bird"])
    db.set_species_representative("Beta Bird", photo_ids["Beta Bird"])
    db.add_species_highlight("Gamma Bird", photo_ids["Gamma Bird"])
    db.set_species_representative("Gamma Bird", photo_ids["Gamma Bird"])

    def species_for(**filters):
        response = client.get(
            "/api/highlights",
            query_string={"folder_id": fid, **filters},
        )
        assert response.status_code == 200
        return {bucket["species"] for bucket in response.get_json()["buckets"]}

    assert species_for(highlight_selection="yes") == {"Alpha Bird", "Gamma Bird"}
    assert species_for(species_representative="yes") == {"Beta Bird", "Gamma Bird"}
    assert species_for(
        highlight_selection="yes", species_representative="yes"
    ) == {"Gamma Bird"}
    assert species_for(
        confirmation="confirmed",
        highlight_selection="no",
        species_representative="no",
    ) == {"Delta Bird"}

    # Rejecting Alpha's selected photo leaves another eligible Alpha photo in
    # the bucket. The stored rank is retained for undo, but it must not make the
    # active-selection filter report that Alpha still has a chosen highlight.
    db.update_photo_flag(photo_ids["Alpha Bird"], "rejected")
    assert species_for(highlight_selection="yes") == {"Gamma Bird"}
    assert species_for(
        highlight_selection="no", species_representative="no"
    ) == {"Alpha Bird", "Delta Bird"}
    assert db.get_species_highlights("Alpha Bird") == {
        "Alpha Bird": {photo_ids["Alpha Bird"]: 1}
    }
    db.update_photo_flag(photo_ids["Alpha Bird"], "none")
    assert species_for(highlight_selection="yes") == {"Alpha Bird", "Gamma Bird"}

    # Representative preferences follow the same active-state rule while
    # keeping their stored row available for an un-reject.
    db.update_photo_flag(photo_ids["Beta Bird"], "rejected")
    assert species_for(species_representative="yes") == {"Gamma Bird"}
    assert db.get_species_representatives() == {
        "Beta Bird": photo_ids["Beta Bird"],
        "Gamma Bird": photo_ids["Gamma Bird"],
    }
    db.update_photo_flag(photo_ids["Beta Bird"], "none")
    assert species_for(species_representative="yes") == {"Beta Bird", "Gamma Bird"}

    response = client.get(
        "/api/highlights",
        query_string={
            "folder_id": fid,
            "highlight_selection": "not-a-filter",
            "species_representative": "not-a-filter",
        },
    )
    assert response.get_json()["meta"]["highlight_selection"] == "all"
    assert response.get_json()["meta"]["species_representative"] == "all"


def test_highlights_curation_filter_ignores_rejected_selected_prediction(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES ('/pred-filter', 'pred-filter', 'ok')"
    ).lastrowid
    db.conn.execute(
        "INSERT INTO workspace_folders (workspace_id, folder_id) VALUES (?, ?)",
        (db._ws_id(), fid),
    )

    photo_ids = []
    prediction_ids = []
    for index in range(2):
        photo_id = db.conn.execute(
            "INSERT INTO photos (folder_id, filename, quality_score, flag) "
            "VALUES (?, ?, ?, 'none')",
            (fid, f"predicted-{index}.jpg", 0.9 - index * 0.1),
        ).lastrowid
        detection_id = db.conn.execute(
            "INSERT INTO detections (photo_id, detector_confidence) VALUES (?, 0.95)",
            (photo_id,),
        ).lastrowid
        prediction_id = db.conn.execute(
            "INSERT INTO predictions "
            "(detection_id, classifier_model, labels_fingerprint, species, confidence) "
            "VALUES (?, 'test-model', 'test-labels', 'Prediction Bird', ?)",
            (detection_id, 0.95 - index * 0.05),
        ).lastrowid
        photo_ids.append(photo_id)
        prediction_ids.append(prediction_id)
    db.conn.commit()
    db.add_species_highlight("Prediction Bird", photo_ids[0])

    response = client.get(
        "/api/highlights",
        query_string={"folder_id": fid, "highlight_selection": "yes"},
    )
    assert {b["species"] for b in response.get_json()["buckets"]} == {
        "Prediction Bird"
    }

    # Rejecting the selected photo's prediction removes that photo from the
    # species bucket, but the second prediction keeps the species visible.
    db.update_prediction_status(prediction_ids[0], "rejected")
    response = client.get(
        "/api/highlights",
        query_string={"folder_id": fid, "highlight_selection": "yes"},
    )
    assert response.get_json()["buckets"] == []
    response = client.get(
        "/api/highlights",
        query_string={"folder_id": fid, "highlight_selection": "no"},
    )
    assert {b["species"] for b in response.get_json()["buckets"]} == {
        "Prediction Bird"
    }


def test_highlights_predictions_above_threshold_populate_buckets(app_and_db):
    """Predictions at or above confidence_threshold count as the photo's species."""
    app, db = app_and_db
    client = app.test_client()
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES ('/p', 'p', 'ok')"
    ).lastrowid
    db.conn.execute(
        "INSERT INTO workspace_folders (workspace_id, folder_id) VALUES (?, ?)",
        (db._ws_id(), fid),
    )
    pid = db.conn.execute(
        "INSERT INTO photos (folder_id, filename, quality_score, flag) "
        "VALUES (?, 'p.jpg', 0.6, 'none')",
        (fid,),
    ).lastrowid
    did = db.conn.execute(
        "INSERT INTO detections (photo_id, detector_confidence) VALUES (?, 0.9)",
        (pid,),
    ).lastrowid
    db.conn.execute(
        "INSERT INTO predictions (detection_id, classifier_model, species, confidence) "
        "VALUES (?, 'm', 'ʻIʻiwi', 0.82)",
        (did,),
    )
    db.conn.commit()

    # Threshold 0.70 — prediction wins, populates species bucket
    resp = client.get(f"/api/highlights?folder_id={fid}&confidence_threshold=0.70")
    data = resp.get_json()
    assert len(data["buckets"]) == 1
    assert data["buckets"][0]["species"] == "ʻIʻiwi"
    assert data["buckets"][0]["is_accepted"] is False
    photo = data["buckets"][0]["photos"][0]
    assert photo["prediction_id"] is not None
    assert photo["predicted_species"] == "ʻIʻiwi"
    assert photo["predicted_confidence"] == 0.82
    assert photo["is_confirmable_prediction"] is True
    assert data["unidentified"]["photo_count"] == 0

    # Threshold 0.90 — prediction below threshold, photo falls to Unidentified
    resp = client.get(f"/api/highlights?folder_id={fid}&confidence_threshold=0.90")
    data = resp.get_json()
    assert data["buckets"] == []
    assert data["unidentified"]["photo_count"] == 1
    photo = data["unidentified"]["photos"][0]
    assert photo["prediction_id"] is not None
    assert photo["predicted_species"] == "ʻIʻiwi"
    assert photo["is_confirmable_prediction"] is False


def test_highlights_confirm_accepts_current_prediction(app_and_db):
    """POST /api/highlights/confirm accepts the server-resolved top prediction."""
    app, db = app_and_db
    client = app.test_client()
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES ('/hc', 'hc', 'ok')"
    ).lastrowid
    db.conn.execute(
        "INSERT INTO workspace_folders (workspace_id, folder_id) VALUES (?, ?)",
        (db._ws_id(), fid),
    )
    pid = db.conn.execute(
        "INSERT INTO photos (folder_id, filename, quality_score, flag) "
        "VALUES (?, 'confirm.jpg', 0.9, 'none')",
        (fid,),
    ).lastrowid
    det = db.save_detections(
        pid,
        [{"box": {"x": 0.1, "y": 0.1, "w": 0.2, "h": 0.2}, "confidence": 0.9}],
        detector_model="MDV6",
    )[0]
    db.add_prediction(det, "Bald Eagle", 0.91, "m")
    db.add_prediction(det, "House Sparrow", 0.42, "m")
    pred = db.conn.execute(
        "SELECT id FROM predictions WHERE detection_id = ? AND species = ?",
        (det, "Bald Eagle"),
    ).fetchone()
    sibling = db.conn.execute(
        "SELECT id FROM predictions WHERE detection_id = ? AND species = ?",
        (det, "House Sparrow"),
    ).fetchone()

    resp = client.post("/api/highlights/confirm", json={"photo_ids": [pid]})
    assert resp.status_code == 200
    assert resp.get_json()["ok"] is True
    assert db.get_review_status(pred["id"], db._ws_id()) == "accepted"
    assert db.get_review_status(sibling["id"], db._ws_id()) == "rejected"
    assert "Bald Eagle" in {kw["name"] for kw in db.get_photo_keywords(pid)}
    history = db.get_edit_history(limit=1)
    assert history[0]["action_type"] == "prediction_accept"


def test_highlights_confirm_accepts_reviewed_prediction(app_and_db):
    """Highlights Confirm accepts non-rejected predictions it displays."""
    app, db = app_and_db
    client = app.test_client()
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES ('/hcr', 'hcr', 'ok')"
    ).lastrowid
    db.conn.execute(
        "INSERT INTO workspace_folders (workspace_id, folder_id) VALUES (?, ?)",
        (db._ws_id(), fid),
    )
    pid = db.conn.execute(
        "INSERT INTO photos (folder_id, filename, quality_score, flag) "
        "VALUES (?, 'reviewed.jpg', 0.9, 'none')",
        (fid,),
    ).lastrowid
    det = db.save_detections(
        pid,
        [{"box": {"x": 0.1, "y": 0.1, "w": 0.2, "h": 0.2}, "confidence": 0.9}],
        detector_model="MDV6",
    )[0]
    db.add_prediction(det, "Bald Eagle", 0.91, "m", status="reviewed")
    pred = db.conn.execute(
        "SELECT id FROM predictions WHERE detection_id = ? AND species = ?",
        (det, "Bald Eagle"),
    ).fetchone()

    resp = client.post("/api/highlights/confirm", json={"photo_ids": [pid]})
    assert resp.status_code == 200
    assert db.get_review_status(pred["id"], db._ws_id()) == "accepted"
    assert "Bald Eagle" in {kw["name"] for kw in db.get_photo_keywords(pid)}


def test_highlights_confirm_skips_taxonomy_keyword_photo(app_and_db):
    """Taxonomy keywords already count as confirmed for Highlights confirm."""
    app, db = app_and_db
    client = app.test_client()
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES ('/hct', 'hct', 'ok')"
    ).lastrowid
    db.conn.execute(
        "INSERT INTO workspace_folders (workspace_id, folder_id) VALUES (?, ?)",
        (db._ws_id(), fid),
    )
    taxonomy_kid = db.conn.execute(
        "INSERT INTO keywords (name, type, is_species) VALUES ('Bald Eagle', 'taxonomy', 0)"
    ).lastrowid
    pid = db.conn.execute(
        "INSERT INTO photos (folder_id, filename, quality_score, flag) "
        "VALUES (?, 'taxonomy.jpg', 0.9, 'none')",
        (fid,),
    ).lastrowid
    db.tag_photo(pid, taxonomy_kid)
    det = db.save_detections(
        pid,
        [{"box": {"x": 0.1, "y": 0.1, "w": 0.2, "h": 0.2}, "confidence": 0.9}],
        detector_model="MDV6",
    )[0]
    db.add_prediction(det, "Bald Eagle", 0.91, "m")
    pred = db.conn.execute(
        "SELECT id FROM predictions WHERE detection_id = ? AND species = ?",
        (det, "Bald Eagle"),
    ).fetchone()

    resp = client.post("/api/highlights/confirm", json={"photo_ids": [pid]})
    assert resp.status_code == 200
    assert resp.get_json()["skipped"] == [
        {"photo_id": pid, "reason": "already_confirmed"}
    ]
    assert db.get_review_status(pred["id"], db._ws_id()) == "pending"


def test_highlights_confirm_group_limited_to_submitted_photos(app_and_db):
    """Grouped Highlights confirm does not tag hidden or unsubmitted group photos."""
    app, db = app_and_db
    client = app.test_client()
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES ('/hcg', 'hcg', 'ok')"
    ).lastrowid
    db.conn.execute(
        "INSERT INTO workspace_folders (workspace_id, folder_id) VALUES (?, ?)",
        (db._ws_id(), fid),
    )
    pid1 = db.conn.execute(
        "INSERT INTO photos (folder_id, filename, quality_score, flag) "
        "VALUES (?, 'group-1.jpg', 0.9, 'none')",
        (fid,),
    ).lastrowid
    pid2 = db.conn.execute(
        "INSERT INTO photos (folder_id, filename, quality_score, flag) "
        "VALUES (?, 'group-2.jpg', 0.9, 'none')",
        (fid,),
    ).lastrowid
    existing_kid = db.add_keyword("House Sparrow", is_species=True)
    db.tag_photo(pid2, existing_kid)
    det1 = db.save_detections(
        pid1,
        [{"box": {"x": 0.1, "y": 0.1, "w": 0.2, "h": 0.2}, "confidence": 0.9}],
        detector_model="MDV6",
    )[0]
    det2 = db.save_detections(
        pid2,
        [{"box": {"x": 0.1, "y": 0.1, "w": 0.2, "h": 0.2}, "confidence": 0.9}],
        detector_model="MDV6",
    )[0]
    db.add_prediction(det1, "Bald Eagle", 0.91, "m", group_id="group-1")
    db.add_prediction(det2, "Bald Eagle", 0.89, "m", group_id="group-1")
    pred1 = db.conn.execute(
        "SELECT id FROM predictions WHERE detection_id = ? AND species = ?",
        (det1, "Bald Eagle"),
    ).fetchone()
    pred2 = db.conn.execute(
        "SELECT id FROM predictions WHERE detection_id = ? AND species = ?",
        (det2, "Bald Eagle"),
    ).fetchone()

    resp = client.post("/api/highlights/confirm", json={"photo_ids": [pid1]})
    assert resp.status_code == 200
    body = resp.get_json()
    assert [item["photo_id"] for item in body["affected"]] == [pid1]
    assert db.get_review_status(pred1["id"], db._ws_id()) == "accepted"
    assert db.get_review_status(pred2["id"], db._ws_id()) == "pending"
    assert "Bald Eagle" in {kw["name"] for kw in db.get_photo_keywords(pid1)}
    pid2_keywords = {kw["name"] for kw in db.get_photo_keywords(pid2)}
    assert "House Sparrow" in pid2_keywords
    assert "Bald Eagle" not in pid2_keywords


def test_highlights_relabel_rejects_prediction_and_replaces_species(app_and_db):
    """POST /api/highlights/relabel retags photos and rejects the stale prediction."""
    app, db = app_and_db
    client = app.test_client()
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES ('/hr', 'hr', 'ok')"
    ).lastrowid
    db.conn.execute(
        "INSERT INTO workspace_folders (workspace_id, folder_id) VALUES (?, ?)",
        (db._ws_id(), fid),
    )
    old_kid = db.add_keyword("Bald Eagle", is_species=True)
    pid = db.conn.execute(
        "INSERT INTO photos (folder_id, filename, quality_score, flag) "
        "VALUES (?, 'relabel.jpg', 0.8, 'none')",
        (fid,),
    ).lastrowid
    db.tag_photo(pid, old_kid)
    det = db.save_detections(
        pid,
        [{"box": {"x": 0.1, "y": 0.1, "w": 0.2, "h": 0.2}, "confidence": 0.9}],
        detector_model="MDV6",
    )[0]
    db.add_prediction(det, "Bald Eagle", 0.88, "m")
    pred = db.conn.execute(
        "SELECT id FROM predictions WHERE detection_id = ? AND species = ?",
        (det, "Bald Eagle"),
    ).fetchone()

    resp = client.post(
        "/api/highlights/relabel",
        json={"photo_ids": [pid], "species": "House Sparrow"},
    )
    assert resp.status_code == 200
    assert resp.get_json()["ok"] is True
    assert db.get_review_status(pred["id"], db._ws_id()) == "rejected"
    keywords = {kw["name"] for kw in db.get_photo_keywords(pid)}
    assert "House Sparrow" in keywords
    assert "Bald Eagle" not in keywords
    history = db.get_edit_history(limit=1)
    assert history[0]["action_type"] == "species_replace"


def test_highlights_relabel_undo_restores_rejected_prediction(app_and_db):
    """Undoing a Highlights relabel restores both species tags and prediction status."""
    app, db = app_and_db
    client = app.test_client()
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES ('/hru', 'hru', 'ok')"
    ).lastrowid
    db.conn.execute(
        "INSERT INTO workspace_folders (workspace_id, folder_id) VALUES (?, ?)",
        (db._ws_id(), fid),
    )
    old_kid = db.add_keyword("Bald Eagle", is_species=True)
    old_taxonomy_kid = db.add_keyword("Haliaeetus", kw_type="taxonomy")
    pid = db.conn.execute(
        "INSERT INTO photos (folder_id, filename, quality_score, flag) "
        "VALUES (?, 'undo-relabel.jpg', 0.8, 'none')",
        (fid,),
    ).lastrowid
    db.tag_photo(pid, old_kid)
    db.tag_photo(pid, old_taxonomy_kid)
    det = db.save_detections(
        pid,
        [{"box": {"x": 0.1, "y": 0.1, "w": 0.2, "h": 0.2}, "confidence": 0.9}],
        detector_model="MDV6",
    )[0]
    db.add_prediction(det, "Bald Eagle", 0.88, "m")
    pred = db.conn.execute(
        "SELECT id FROM predictions WHERE detection_id = ? AND species = ?",
        (det, "Bald Eagle"),
    ).fetchone()

    resp = client.post(
        "/api/highlights/relabel",
        json={"photo_ids": [pid], "species": "House Sparrow"},
    )
    assert resp.status_code == 200
    assert db.get_review_status(pred["id"], db._ws_id()) == "rejected"

    undone = db.undo_last_edit()
    assert undone["action_type"] == "species_replace"
    assert db.get_review_status(pred["id"], db._ws_id()) == "pending"
    keywords = {kw["name"] for kw in db.get_photo_keywords(pid)}
    assert "Bald Eagle" in keywords
    assert "Haliaeetus" in keywords
    assert "House Sparrow" not in keywords

    redone = db.redo_last_undo()
    assert redone["action_type"] == "species_replace"
    assert db.get_review_status(pred["id"], db._ws_id()) == "rejected"
    keywords = {kw["name"] for kw in db.get_photo_keywords(pid)}
    assert "House Sparrow" in keywords
    assert "Bald Eagle" not in keywords
    assert "Haliaeetus" not in keywords


def test_highlights_relabel_prediction_only_undo_restores_prediction(app_and_db):
    """Undoing relabel on a prediction-only photo restores the predicted bucket."""
    app, db = app_and_db
    client = app.test_client()
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES ('/hrup', 'hrup', 'ok')"
    ).lastrowid
    db.conn.execute(
        "INSERT INTO workspace_folders (workspace_id, folder_id) VALUES (?, ?)",
        (db._ws_id(), fid),
    )
    pid = db.conn.execute(
        "INSERT INTO photos (folder_id, filename, quality_score, flag) "
        "VALUES (?, 'undo-predicted.jpg', 0.8, 'none')",
        (fid,),
    ).lastrowid
    det = db.save_detections(
        pid,
        [{"box": {"x": 0.1, "y": 0.1, "w": 0.2, "h": 0.2}, "confidence": 0.9}],
        detector_model="MDV6",
    )[0]
    db.add_prediction(det, "Bald Eagle", 0.88, "m")
    pred = db.conn.execute(
        "SELECT id FROM predictions WHERE detection_id = ? AND species = ?",
        (det, "Bald Eagle"),
    ).fetchone()

    resp = client.post(
        "/api/highlights/relabel",
        json={"photo_ids": [pid], "species": "House Sparrow"},
    )
    assert resp.status_code == 200
    assert db.get_review_status(pred["id"], db._ws_id()) == "rejected"
    assert db.get_edit_history(limit=1)[0]["action_type"] == "keyword_add"

    undone = db.undo_last_edit()
    assert undone["action_type"] == "keyword_add"
    assert db.get_review_status(pred["id"], db._ws_id()) == "pending"
    assert "House Sparrow" not in {kw["name"] for kw in db.get_photo_keywords(pid)}


def test_highlights_relabel_unidentified_sets_species(app_and_db):
    """Relabel also works for unidentified photos with no prediction to reject."""
    app, db = app_and_db
    client = app.test_client()
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES ('/hu', 'hu', 'ok')"
    ).lastrowid
    db.conn.execute(
        "INSERT INTO workspace_folders (workspace_id, folder_id) VALUES (?, ?)",
        (db._ws_id(), fid),
    )
    pid = db.conn.execute(
        "INSERT INTO photos (folder_id, filename, quality_score, flag) "
        "VALUES (?, 'unid.jpg', 0.7, 'none')",
        (fid,),
    ).lastrowid
    db.conn.commit()

    resp = client.post(
        "/api/highlights/relabel",
        json={"photo_ids": [pid], "species": "Song Sparrow"},
    )
    assert resp.status_code == 200
    assert "Song Sparrow" in {kw["name"] for kw in db.get_photo_keywords(pid)}


def test_highlights_relabel_moves_species_highlights_to_new_bucket(app_and_db):
    """Relabel migrates ordered species_highlights rows to the new species."""
    app, db = app_and_db
    client = app.test_client()
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES ('/hrh', 'hrh', 'ok')"
    ).lastrowid
    db.conn.execute(
        "INSERT INTO workspace_folders (workspace_id, folder_id) VALUES (?, ?)",
        (db._ws_id(), fid),
    )
    old_kid = db.add_keyword("Bald Eagle", is_species=True)
    p1 = db.conn.execute(
        "INSERT INTO photos (folder_id, filename, quality_score, flag) "
        "VALUES (?, 'h1.jpg', 0.8, 'none')",
        (fid,),
    ).lastrowid
    p2 = db.conn.execute(
        "INSERT INTO photos (folder_id, filename, quality_score, flag) "
        "VALUES (?, 'h2.jpg', 0.8, 'none')",
        (fid,),
    ).lastrowid
    db.tag_photo(p1, old_kid)
    db.tag_photo(p2, old_kid)
    db.add_species_highlight("Bald Eagle", p1)
    db.add_species_highlight("Bald Eagle", p2)

    resp = client.post(
        "/api/highlights/relabel",
        json={"photo_ids": [p1, p2], "species": "Golden Eagle"},
    )
    assert resp.status_code == 200

    assert db.get_species_highlights("Bald Eagle") == {}
    assert db.get_species_highlights("Golden Eagle") == {
        "Golden Eagle": {p1: 1, p2: 2}
    }


def test_highlights_relabel_merges_into_existing_new_species_bucket(app_and_db):
    """Relabel appends after rows already present in the destination bucket."""
    app, db = app_and_db
    client = app.test_client()
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES ('/hrm', 'hrm', 'ok')"
    ).lastrowid
    db.conn.execute(
        "INSERT INTO workspace_folders (workspace_id, folder_id) VALUES (?, ?)",
        (db._ws_id(), fid),
    )
    db.add_keyword("Bald Eagle", is_species=True)
    new_kid = db.add_keyword("Golden Eagle", is_species=True)
    p_dest = db.conn.execute(
        "INSERT INTO photos (folder_id, filename, quality_score, flag) "
        "VALUES (?, 'dest.jpg', 0.8, 'none')",
        (fid,),
    ).lastrowid
    p_moving = db.conn.execute(
        "INSERT INTO photos (folder_id, filename, quality_score, flag) "
        "VALUES (?, 'moving.jpg', 0.8, 'none')",
        (fid,),
    ).lastrowid
    db.tag_photo(p_dest, new_kid)
    db.tag_photo(p_moving, db.add_keyword("Bald Eagle", is_species=True))
    db.add_species_highlight("Golden Eagle", p_dest)
    db.add_species_highlight("Bald Eagle", p_moving)

    resp = client.post(
        "/api/highlights/relabel",
        json={"photo_ids": [p_moving], "species": "Golden Eagle"},
    )
    assert resp.status_code == 200

    assert db.get_species_highlights("Bald Eagle") == {}
    assert db.get_species_highlights("Golden Eagle") == {
        "Golden Eagle": {p_dest: 1, p_moving: 2}
    }


def test_highlights_relabel_migrates_species_representative(app_and_db):
    """Relabel moves species_representative preferences to the new species
    so `get_species_representatives()` returns the photo under its new bucket
    rather than stranding it under the old species."""
    app, db = app_and_db
    client = app.test_client()
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES ('/hrrep', 'hrrep', 'ok')"
    ).lastrowid
    db.conn.execute(
        "INSERT INTO workspace_folders (workspace_id, folder_id) VALUES (?, ?)",
        (db._ws_id(), fid),
    )
    old_kid = db.add_keyword("Bald Eagle", is_species=True)
    db.add_keyword("Golden Eagle", is_species=True)
    pid = db.conn.execute(
        "INSERT INTO photos (folder_id, filename, quality_score, flag) "
        "VALUES (?, 'rep.jpg', 0.9, 'none')",
        (fid,),
    ).lastrowid
    db.tag_photo(pid, old_kid)
    db.set_species_representative("Bald Eagle", pid)

    resp = client.post(
        "/api/highlights/relabel",
        json={"photo_ids": [pid], "species": "Golden Eagle"},
    )
    assert resp.status_code == 200

    reps = db.get_species_representatives()
    assert reps.get("Golden Eagle") == pid
    assert "Bald Eagle" not in reps


def test_highlights_relabel_undo_restores_curation(app_and_db):
    """Undoing a Highlights relabel restores the migrated ordered-highlight
    row and species_representative preference back under the old species,
    so the photo isn't stranded under the new species when the relabel
    itself is undone. Redo re-applies the migration."""
    app, db = app_and_db
    client = app.test_client()
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES ('/hrundo', 'hrundo', 'ok')"
    ).lastrowid
    db.conn.execute(
        "INSERT INTO workspace_folders (workspace_id, folder_id) VALUES (?, ?)",
        (db._ws_id(), fid),
    )
    old_kid = db.add_keyword("Bald Eagle", is_species=True)
    db.add_keyword("Golden Eagle", is_species=True)
    pid = db.conn.execute(
        "INSERT INTO photos (folder_id, filename, quality_score, flag) "
        "VALUES (?, 'undo-cur.jpg', 0.9, 'none')",
        (fid,),
    ).lastrowid
    db.tag_photo(pid, old_kid)
    db.set_species_representative("Bald Eagle", pid)
    db.add_species_highlight("Bald Eagle", pid)

    resp = client.post(
        "/api/highlights/relabel",
        json={"photo_ids": [pid], "species": "Golden Eagle"},
    )
    assert resp.status_code == 200
    assert db.get_species_representatives().get("Golden Eagle") == pid
    assert pid in (db.get_species_highlights("Golden Eagle") or {}).get(
        "Golden Eagle", {}
    )

    undone = db.undo_last_edit()
    assert undone is not None
    assert undone["action_type"] == "species_replace"
    reps = db.get_species_representatives()
    assert reps.get("Bald Eagle") == pid
    assert "Golden Eagle" not in reps
    hl = db.get_species_highlights()
    assert pid in (hl.get("Bald Eagle") or {})
    assert "Golden Eagle" not in hl

    redone = db.redo_last_undo()
    assert redone is not None
    assert redone["action_type"] == "species_replace"
    reps_redo = db.get_species_representatives()
    assert reps_redo.get("Golden Eagle") == pid
    assert "Bald Eagle" not in reps_redo
    hl_redo = db.get_species_highlights()
    assert pid in (hl_redo.get("Golden Eagle") or {})
    assert "Bald Eagle" not in hl_redo


def test_highlights_relabel_undo_preserves_original_rank(app_and_db):
    """Undoing a relabel puts the highlighted photo back at its original
    rank rather than dumping it at MAX(rank)+1 of the old bucket, so
    curated order survives a round-trip through undo."""
    app, db = app_and_db
    client = app.test_client()
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES ('/hrrank', 'hrrank', 'ok')"
    ).lastrowid
    db.conn.execute(
        "INSERT INTO workspace_folders (workspace_id, folder_id) VALUES (?, ?)",
        (db._ws_id(), fid),
    )
    old_kid = db.add_keyword("Old Rank Bird", is_species=True)
    db.add_keyword("New Rank Bird", is_species=True)
    p1 = db.conn.execute(
        "INSERT INTO photos (folder_id, filename, quality_score, flag) "
        "VALUES (?, 'rank-1.jpg', 0.9, 'none')",
        (fid,),
    ).lastrowid
    p2 = db.conn.execute(
        "INSERT INTO photos (folder_id, filename, quality_score, flag) "
        "VALUES (?, 'rank-2.jpg', 0.9, 'none')",
        (fid,),
    ).lastrowid
    db.tag_photo(p1, old_kid)
    db.tag_photo(p2, old_kid)
    db.add_species_highlight("Old Rank Bird", p1)
    db.add_species_highlight("Old Rank Bird", p2)

    # Sanity: original ranks are 1 (p1) and 2 (p2).
    assert db.get_species_highlights("Old Rank Bird") == {
        "Old Rank Bird": {p1: 1, p2: 2}
    }

    resp = client.post(
        "/api/highlights/relabel",
        json={"photo_ids": [p1], "species": "New Rank Bird"},
    )
    assert resp.status_code == 200

    undone = db.undo_last_edit()
    assert undone is not None

    restored = db.get_species_highlights("Old Rank Bird")["Old Rank Bird"]
    # Before the fix, p1 landed at rank 3 (MAX(rank)+1) and appeared
    # after p2 in the ordered list.
    assert restored[p1] == 1
    assert restored[p2] == 2


def test_highlights_relabel_prediction_only_undo_restores_curation(app_and_db):
    """Predicted-only relabels record `keyword_add`, not `species_replace`,
    but can still carry a `curation` payload when the photo already held a
    representative or highlight under another species. Undo/redo must
    move those rows back and forth just like the `species_replace` case."""
    app, db = app_and_db
    client = app.test_client()
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES ('/hrpo', 'hrpo', 'ok')"
    ).lastrowid
    db.conn.execute(
        "INSERT INTO workspace_folders (workspace_id, folder_id) VALUES (?, ?)",
        (db._ws_id(), fid),
    )
    db.add_keyword("New Species", is_species=True)
    pid = db.conn.execute(
        "INSERT INTO photos (folder_id, filename, quality_score, flag) "
        "VALUES (?, 'pred-cur.jpg', 0.9, 'none')",
        (fid,),
    ).lastrowid
    # No prior species tag: only representative + highlight rows exist
    # for an unrelated species. This is what makes the relabel record as
    # `keyword_add` (has_old_species stays False) rather than
    # `species_replace`.
    db.set_species_representative("Predicted Bird", pid)
    db.add_species_highlight("Predicted Bird", pid)
    det = db.save_detections(
        pid,
        [{"box": {"x": 0.1, "y": 0.1, "w": 0.2, "h": 0.2}, "confidence": 0.9}],
        detector_model="MDV6",
    )[0]
    db.add_prediction(det, "Predicted Bird", 0.88, "m")
    db.conn.commit()

    resp = client.post(
        "/api/highlights/relabel",
        json={"photo_ids": [pid], "species": "New Species"},
    )
    assert resp.status_code == 200
    assert db.get_edit_history(limit=1)[0]["action_type"] == "keyword_add"
    assert db.get_species_representatives().get("New Species") == pid
    assert pid in (db.get_species_highlights("New Species") or {}).get(
        "New Species", {}
    )

    undone = db.undo_last_edit()
    assert undone is not None
    assert undone["action_type"] == "keyword_add"
    reps = db.get_species_representatives()
    assert reps.get("Predicted Bird") == pid
    assert "New Species" not in reps
    hl = db.get_species_highlights()
    assert pid in (hl.get("Predicted Bird") or {})
    assert "New Species" not in hl

    redone = db.redo_last_undo()
    assert redone is not None
    assert redone["action_type"] == "keyword_add"
    reps_redo = db.get_species_representatives()
    assert reps_redo.get("New Species") == pid
    assert "Predicted Bird" not in reps_redo
    hl_redo = db.get_species_highlights()
    assert pid in (hl_redo.get("New Species") or {})
    assert "Predicted Bird" not in hl_redo


def test_highlights_relabel_undo_preserves_existing_destination_highlight(app_and_db):
    """When a photo is highlighted under both the old and new species,
    the relabel drops the old-bucket row and leaves the pre-existing
    destination row alone. Undo must not delete that destination row —
    it wasn't created by the relabel."""
    app, db = app_and_db
    client = app.test_client()
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES ('/hrdst', 'hrdst', 'ok')"
    ).lastrowid
    db.conn.execute(
        "INSERT INTO workspace_folders (workspace_id, folder_id) VALUES (?, ?)",
        (db._ws_id(), fid),
    )
    old_kid = db.add_keyword("Both Bird A", is_species=True)
    db.add_keyword("Both Bird B", is_species=True)
    pid = db.conn.execute(
        "INSERT INTO photos (folder_id, filename, quality_score, flag) "
        "VALUES (?, 'both.jpg', 0.9, 'none')",
        (fid,),
    ).lastrowid
    db.tag_photo(pid, old_kid)
    # Highlighted under both species before the relabel.
    db.add_species_highlight("Both Bird A", pid)
    db.add_species_highlight("Both Bird B", pid)

    resp = client.post(
        "/api/highlights/relabel",
        json={"photo_ids": [pid], "species": "Both Bird B"},
    )
    assert resp.status_code == 200
    # Old-bucket row dropped; destination row retained.
    assert db.get_species_highlights("Both Bird A") == {}
    assert pid in (db.get_species_highlights("Both Bird B") or {}).get(
        "Both Bird B", {}
    )

    undone = db.undo_last_edit()
    assert undone is not None
    hl = db.get_species_highlights()
    # Old bucket restored AND destination row survives — before the fix
    # the undo deleted the pre-existing (Both Bird B, pid) row.
    assert pid in (hl.get("Both Bird A") or {})
    assert pid in (hl.get("Both Bird B") or {})


def test_highlights_relabel_undo_restores_representative_over_collision(app_and_db):
    """When the destination species already has a different representative,
    rename_photo_preferences_species's INSERT OR IGNORE is ignored and no
    row for our photo is written at the new species, but the old-species
    row is still deleted. Undo must restore the old-species representative
    even though there's no destination row for this photo to key off of."""
    app, db = app_and_db
    client = app.test_client()
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES ('/hrcol', 'hrcol', 'ok')"
    ).lastrowid
    db.conn.execute(
        "INSERT INTO workspace_folders (workspace_id, folder_id) VALUES (?, ?)",
        (db._ws_id(), fid),
    )
    old_kid = db.add_keyword("Rep Old", is_species=True)
    db.add_keyword("Rep New", is_species=True)
    p_moving = db.conn.execute(
        "INSERT INTO photos (folder_id, filename, quality_score, flag) "
        "VALUES (?, 'moving.jpg', 0.9, 'none')",
        (fid,),
    ).lastrowid
    p_incumbent = db.conn.execute(
        "INSERT INTO photos (folder_id, filename, quality_score, flag) "
        "VALUES (?, 'incumbent.jpg', 0.9, 'none')",
        (fid,),
    ).lastrowid
    db.tag_photo(p_moving, old_kid)
    db.set_species_representative("Rep Old", p_moving)
    db.set_species_representative("Rep New", p_incumbent)

    resp = client.post(
        "/api/highlights/relabel",
        json={"photo_ids": [p_moving], "species": "Rep New"},
    )
    assert resp.status_code == 200
    reps_after = db.get_species_representatives()
    # Old-species rep dropped; Rep New keeps its original incumbent.
    assert "Rep Old" not in reps_after
    assert reps_after.get("Rep New") == p_incumbent

    undone = db.undo_last_edit()
    assert undone is not None
    reps = db.get_species_representatives()
    # Before the fix, the old-species representative stayed stranded
    # because the destination row for p_moving never existed at "Rep New".
    assert reps.get("Rep Old") == p_moving
    assert reps.get("Rep New") == p_incumbent


def test_highlights_relabel_undo_preserves_preexisting_target_representative(
    app_and_db,
):
    """When the relabeled photo was already a global representative for
    the destination species — e.g. a multi-species photo picked as rep
    for both A and B before relabeling A→B — the relabel's
    ``INSERT OR IGNORE`` keeps the pre-existing ``(B, photo_id)`` row and
    only deletes the old-species row. Undo must not erase that
    pre-existing target-species rep while processing ``pref_prev``."""
    app, db = app_and_db
    client = app.test_client()
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES ('/prekeep', 'prekeep', 'ok')"
    ).lastrowid
    db.conn.execute(
        "INSERT INTO workspace_folders (workspace_id, folder_id) VALUES (?, ?)",
        (db._ws_id(), fid),
    )
    old_kid = db.add_keyword("Rep Both A", is_species=True)
    new_kid = db.add_keyword("Rep Both B", is_species=True)
    pid = db.conn.execute(
        "INSERT INTO photos (folder_id, filename, quality_score, flag) "
        "VALUES (?, 'both.jpg', 0.9, 'none')",
        (fid,),
    ).lastrowid
    db.tag_photo(pid, old_kid)
    db.tag_photo(pid, new_kid)
    db.set_species_representative("Rep Both A", pid)
    db.set_species_representative("Rep Both B", pid)

    resp = client.post(
        "/api/highlights/relabel",
        json={"photo_ids": [pid], "species": "Rep Both B"},
    )
    assert resp.status_code == 200
    reps_after = db.get_species_representatives()
    # A has been retagged away and its rep row moved into B (whose row
    # was pre-existing, so INSERT OR IGNORE was a no-op).
    assert "Rep Both A" not in reps_after
    assert reps_after.get("Rep Both B") == pid

    undone = db.undo_last_edit()
    assert undone is not None
    reps_undone = db.get_species_representative_lists()
    # Both reps must be present after undo: A is restored from pref_prev,
    # and B must survive the pref_prev-driven cleanup because the target
    # rep row pre-existed the relabel.
    assert pid in (reps_undone.get("Rep Both A") or [])
    assert pid in (reps_undone.get("Rep Both B") or [])


def test_highlights_relabel_migrates_cross_workspace_representative(app_and_db):
    """A photo picked as a global representative in a different workspace
    has no compatibility row in the relabel-workspace's ``photo_preferences``,
    only a global ``species_representatives`` row. Relabel must still move
    that global row to the new species; otherwise the representative
    disappears from both species after the retag."""
    app, db = app_and_db
    client = app.test_client()
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES ('/xwsrep', 'xwsrep', 'ok')"
    ).lastrowid
    ws_a = db._active_workspace_id
    ws_b = db.create_workspace("XWSRep Other")
    for ws in (ws_a, ws_b):
        db.conn.execute(
            "INSERT INTO workspace_folders (workspace_id, folder_id) VALUES (?, ?)",
            (ws, fid),
        )
    old_kid = db.add_keyword("Bald Eagle", is_species=True)
    db.add_keyword("Golden Eagle", is_species=True)
    pid = db.conn.execute(
        "INSERT INTO photos (folder_id, filename, quality_score, flag) "
        "VALUES (?, 'xrep.jpg', 0.9, 'none')",
        (fid,),
    ).lastrowid
    db.tag_photo(pid, old_kid)
    # Select the representative from workspace B, so the compatibility
    # photo_preferences row lives under ws_b — not under ws_a where we
    # relabel from. The global species_representatives row is workspace-
    # independent and stays.
    db.set_active_workspace(ws_b)
    db.set_species_representative("Bald Eagle", pid)
    db.set_active_workspace(ws_a)

    # Sanity: no local photo_preferences row for this photo in ws_a.
    local_pref = db.conn.execute(
        """SELECT 1 FROM photo_preferences
           WHERE workspace_id = ? AND species = ? AND photo_id = ?""",
        (ws_a, "Bald Eagle", pid),
    ).fetchone()
    assert local_pref is None

    resp = client.post(
        "/api/highlights/relabel",
        json={"photo_ids": [pid], "species": "Golden Eagle"},
    )
    assert resp.status_code == 200

    reps = db.get_species_representatives()
    assert reps.get("Golden Eagle") == pid
    assert "Bald Eagle" not in reps

    # Undo/redo round-trips the global rep even when there's no local
    # pref row to piggyback on.
    undone = db.undo_last_edit()
    assert undone is not None
    reps_undone = db.get_species_representatives()
    assert reps_undone.get("Bald Eagle") == pid
    assert "Golden Eagle" not in reps_undone

    redone = db.redo_last_undo()
    assert redone is not None
    reps_redone = db.get_species_representatives()
    assert reps_redone.get("Golden Eagle") == pid
    assert "Bald Eagle" not in reps_redone


def test_highlights_relabel_ignores_stale_representative_rows(app_and_db):
    """A photo can retain a global ``species_representatives`` row for a
    species it no longer carries (untag_photo doesn't clear the global
    rep row, and a rep picked from another workspace has no active-
    workspace pref row to sweep it through the preference pass).
    Relabeling the photo's current species must not renaming that stale
    rep into the target species — it should stay under the old species,
    and only the rep for the species actually being replaced should
    move."""
    app, db = app_and_db
    client = app.test_client()
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES ('/stalerep', 'stalerep', 'ok')"
    ).lastrowid
    ws_a = db._active_workspace_id
    ws_b = db.create_workspace("StaleRep Other")
    for ws in (ws_a, ws_b):
        db.conn.execute(
            "INSERT INTO workspace_folders (workspace_id, folder_id) VALUES (?, ?)",
            (ws, fid),
        )
    stale_kid = db.add_keyword("Bald Eagle", is_species=True)
    current_kid = db.add_keyword("Osprey", is_species=True)
    db.add_keyword("Golden Eagle", is_species=True)
    pid = db.conn.execute(
        "INSERT INTO photos (folder_id, filename, quality_score, flag) "
        "VALUES (?, 'stale.jpg', 0.9, 'none')",
        (fid,),
    ).lastrowid
    db.tag_photo(pid, stale_kid)
    db.tag_photo(pid, current_kid)
    # Select the Bald Eagle rep from ws_b — the compat pref row lives
    # under ws_b, so the active workspace ws_a's preference-rename pass
    # won't sweep this rep during a ws_a relabel. Only the direct rep-
    # only pass could pick it up, which is the code path under test.
    db.set_active_workspace(ws_b)
    db.set_species_representative("Bald Eagle", pid)
    db.set_active_workspace(ws_a)
    # The current-species rep is set from the active workspace and has
    # a local pref row, so its migration goes through the preference
    # pass and is unrelated to the rep-only filter.
    db.set_species_representative("Osprey", pid)
    # Untag Bald Eagle from the photo. The global species_representatives
    # row remains, so the photo now carries only Osprey as a taxonomy
    # keyword but still has a stale (Bald Eagle, pid) rep row.
    db.untag_photo(pid, stale_kid)

    stale_before = db.conn.execute(
        "SELECT 1 FROM species_representatives WHERE species = ? AND photo_id = ?",
        ("Bald Eagle", pid),
    ).fetchone()
    assert stale_before is not None
    # And no ws_a pref row exists for Bald Eagle either — confirms the
    # preference pass won't touch it.
    local_pref = db.conn.execute(
        """SELECT 1 FROM photo_preferences
           WHERE workspace_id = ? AND species = ? AND photo_id = ?""",
        (ws_a, "Bald Eagle", pid),
    ).fetchone()
    assert local_pref is None

    resp = client.post(
        "/api/highlights/relabel",
        json={"photo_ids": [pid], "species": "Golden Eagle"},
    )
    assert resp.status_code == 200

    reps = db.get_species_representatives()
    # The Osprey rep moved to Golden Eagle (the relabel target).
    assert reps.get("Golden Eagle") == pid
    assert "Osprey" not in reps
    # The stale Bald Eagle rep stays under Bald Eagle — it was never
    # part of the relabel and must not be renamed to Golden Eagle.
    assert reps.get("Bald Eagle") == pid


def test_highlights_relabel_ignores_stale_representative_with_local_pref(app_and_db):
    """When a photo has a stale ``photo_preferences`` row for a species it
    no longer carries (from setting the rep in the active workspace and
    then untagging that species), a relabel of the photo's current species
    must not sweep the stale row through ``rename_photo_preferences_species``
    — which would also rename the matching global ``species_representatives``
    row into the target species, losing the preserved state and making the
    target look manually selected."""
    app, db = app_and_db
    client = app.test_client()
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES ('/stalepref', 'stalepref', 'ok')"
    ).lastrowid
    ws_id = db._active_workspace_id
    db.conn.execute(
        "INSERT INTO workspace_folders (workspace_id, folder_id) VALUES (?, ?)",
        (ws_id, fid),
    )
    stale_kid = db.add_keyword("Bald Eagle", is_species=True)
    current_kid = db.add_keyword("Osprey", is_species=True)
    db.add_keyword("Golden Eagle", is_species=True)
    pid = db.conn.execute(
        "INSERT INTO photos (folder_id, filename, quality_score, flag) "
        "VALUES (?, 'stalepref.jpg', 0.9, 'none')",
        (fid,),
    ).lastrowid
    db.tag_photo(pid, stale_kid)
    db.tag_photo(pid, current_kid)
    # Set both reps from the active workspace so each has a local
    # compat pref row in ws_id. This makes the stale row travel through
    # the pref-covered pass rather than the rep-only pass.
    db.set_species_representative("Bald Eagle", pid)
    db.set_species_representative("Osprey", pid)
    # Untag Bald Eagle. The pref row and global rep row for Bald Eagle
    # both remain (untag_photo does not clear either), so the photo now
    # carries only Osprey as a taxonomy keyword but retains stale
    # (Bald Eagle, pid) rows in both tables.
    db.untag_photo(pid, stale_kid)

    stale_pref = db.conn.execute(
        """SELECT 1 FROM photo_preferences
           WHERE workspace_id = ? AND species = ? AND photo_id = ?""",
        (ws_id, "Bald Eagle", pid),
    ).fetchone()
    assert stale_pref is not None
    stale_rep = db.conn.execute(
        "SELECT 1 FROM species_representatives WHERE species = ? AND photo_id = ?",
        ("Bald Eagle", pid),
    ).fetchone()
    assert stale_rep is not None

    resp = client.post(
        "/api/highlights/relabel",
        json={"photo_ids": [pid], "species": "Golden Eagle"},
    )
    assert resp.status_code == 200

    reps = db.get_species_representatives()
    # The Osprey rep moved to Golden Eagle (the relabel target).
    assert reps.get("Golden Eagle") == pid
    assert "Osprey" not in reps
    # The stale Bald Eagle rep stays under Bald Eagle — the pref-covered
    # rename must not carry it into Golden Eagle.
    assert reps.get("Bald Eagle") == pid
    # The stale Bald Eagle pref row is likewise left untouched.
    stale_pref_after = db.conn.execute(
        """SELECT 1 FROM photo_preferences
           WHERE workspace_id = ? AND species = ? AND photo_id = ?""",
        (ws_id, "Bald Eagle", pid),
    ).fetchone()
    assert stale_pref_after is not None


def test_highlights_relabel_ignores_stale_reps_on_prediction_only_relabel(app_and_db):
    """A prediction-only relabel (no taxonomy keywords → ``keyword_add``,
    not ``species_replace``) must not sweep stale global
    ``species_representatives`` or ``photo_preferences`` rows for species
    the photo previously carried and then had untagged. Only curation for
    a species matching the photo's active prediction should migrate to
    the target species."""
    app, db = app_and_db
    client = app.test_client()
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES ('/predstale', 'predstale', 'ok')"
    ).lastrowid
    db.conn.execute(
        "INSERT INTO workspace_folders (workspace_id, folder_id) VALUES (?, ?)",
        (db._ws_id(), fid),
    )
    stale_kid = db.add_keyword("Old Bird", is_species=True)
    db.add_keyword("New Species", is_species=True)
    pid = db.conn.execute(
        "INSERT INTO photos (folder_id, filename, quality_score, flag) "
        "VALUES (?, 'pred-stale.jpg', 0.9, 'none')",
        (fid,),
    ).lastrowid
    # Photo used to be tagged Old Bird and was picked as its rep; then the
    # keyword was untagged. The rep/pref rows survive untag_photo, so the
    # photo now carries no taxonomy but retains stale curation for Old
    # Bird — a legitimate scenario Codex flagged for prediction-only
    # relabels.
    db.tag_photo(pid, stale_kid)
    db.set_species_representative("Old Bird", pid)
    db.add_species_highlight("Old Bird", pid)
    db.untag_photo(pid, stale_kid)
    # Prediction added AFTER untag so the "predicted-only" relabel path
    # runs (has_old_species stays False → action_type = "keyword_add").
    # The prediction is for a *different* species than the stale curation,
    # so the prediction-species migration guard must skip the stale rows.
    det = db.save_detections(
        pid,
        [{"box": {"x": 0.1, "y": 0.1, "w": 0.2, "h": 0.2}, "confidence": 0.9}],
        detector_model="MDV6",
    )[0]
    db.add_prediction(det, "Predicted Bird", 0.88, "m")
    db.conn.commit()

    resp = client.post(
        "/api/highlights/relabel",
        json={"photo_ids": [pid], "species": "New Species"},
    )
    assert resp.status_code == 200
    assert db.get_edit_history(limit=1)[0]["action_type"] == "keyword_add"

    reps = db.get_species_representatives()
    # Stale Old Bird rep survives — it is NOT renamed to New Species.
    assert reps.get("Old Bird") == pid
    assert "New Species" not in reps
    hl = db.get_species_highlights()
    assert pid in (hl.get("Old Bird") or {})
    assert "New Species" not in hl


def test_highlights_relabel_preserves_higher_rank_taxonomy_curation(app_and_db):
    """When a photo carries a linked higher-rank taxonomy keyword (family,
    genus, ...) with its own curation, the relabel query intentionally
    leaves that keyword attached — the ``t.rank = 'species' OR t.rank IS
    NULL`` filter on ``old_rows`` restricts removal to species-rank rows.
    The source-curation snapshot must apply the same rank filter, otherwise
    ``_accept_curation_source`` migrates the higher-rank tag's curation to
    the new species even though the higher-rank keyword still holds the
    photo — leaving the family/genus keyword attached with its curation
    stripped away.
    """
    app, db = app_and_db
    client = app.test_client()
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) "
        "VALUES ('/higherrank', 'higherrank', 'ok')"
    ).lastrowid
    ws_id = db._ws_id()
    db.conn.execute(
        "INSERT INTO workspace_folders (workspace_id, folder_id) VALUES (?, ?)",
        (ws_id, fid),
    )
    # Family-rank taxon and a linked taxonomy keyword for it. The relabel
    # removal query filters to species-rank rows (species or NULL rank),
    # so this family keyword is intentionally left attached.
    family_taxon = db.conn.execute(
        "INSERT INTO taxa (inat_id, name, common_name, rank, kingdom) "
        "VALUES (5040, 'Corvidae', 'Corvids', 'family', 'Animalia')"
    ).lastrowid
    family_kid = db.conn.execute(
        "INSERT INTO keywords (name, parent_id, is_species, type, taxon_id) "
        "VALUES ('Corvids', NULL, 1, 'taxonomy', ?)",
        (family_taxon,),
    ).lastrowid
    # Destination species.
    db.add_keyword("Common Raven", is_species=True)
    pid = db.conn.execute(
        "INSERT INTO photos (folder_id, filename, quality_score, flag) "
        "VALUES (?, 'higherrank.jpg', 0.9, 'none')",
        (fid,),
    ).lastrowid
    db.tag_photo(pid, family_kid)
    # Curation on the family-rank keyword: a highlight and a global
    # representative. Both must stay under 'Corvids' after the relabel
    # because the keyword itself is not being removed.
    db.add_species_highlight("Corvids", pid)
    db.set_species_representative("Corvids", pid)
    db.conn.commit()

    resp = client.post(
        "/api/highlights/relabel",
        json={"photo_ids": [pid], "species": "Common Raven"},
    )
    assert resp.status_code == 200, resp.get_data(as_text=True)

    # The family-rank keyword must still be attached to the photo — the
    # old_rows removal query excludes non-species ranks.
    attached = {
        r["id"] for r in db.conn.execute(
            "SELECT k.id FROM photo_keywords pk "
            "JOIN keywords k ON k.id = pk.keyword_id "
            "WHERE pk.photo_id = ?",
            (pid,),
        ).fetchall()
    }
    assert family_kid in attached, (
        "family-rank taxonomy keyword must remain attached — its curation "
        "cannot be swept into the relabel target"
    )

    # The highlight and representative for the family name must remain
    # under 'Corvids', not be migrated to 'Common Raven'.
    hl = db.get_species_highlights()
    assert pid in (hl.get("Corvids") or {}), (
        "family-rank highlight must stay under its own species key"
    )
    assert pid not in (hl.get("Common Raven") or {}), (
        "family-rank highlight must not migrate to the new species while "
        "the family keyword is still attached to the photo"
    )
    reps = db.get_species_representatives()
    assert reps.get("Corvids") == pid, (
        "family-rank representative must stay under its own species key"
    )
    assert reps.get("Common Raven") != pid, (
        "family-rank representative must not migrate to the new species "
        "while the family keyword is still attached to the photo"
    )


def test_highlights_relabel_does_not_fold_non_ascii_case_variants(app_and_db):
    """`_accept_curation_source` must key by ``keyword_match_key`` (SQLite's
    ASCII-only NOCASE fold), not Python's ``str.lower()``. SQLite/add_keyword
    keep ``Éclair`` and ``éclair`` as distinct species rows, but
    ``str.lower()`` folds them together — so a stale ``éclair`` curation
    row on a photo currently carrying ``Éclair`` would be swept into the
    relabel target if the filter used ``.lower()``. Under the fix, the
    stale row stays under ``éclair``.
    """
    app, db = app_and_db
    client = app.test_client()
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) "
        "VALUES ('/nonascii', 'nonascii', 'ok')"
    ).lastrowid
    ws_id = db._ws_id()
    db.conn.execute(
        "INSERT INTO workspace_folders (workspace_id, folder_id) VALUES (?, ?)",
        (ws_id, fid),
    )
    # Two DISTINCT species keywords: `Éclair` (uppercase É) and `éclair`
    # (lowercase é). add_keyword's NOCASE dedupe is ASCII-only, so they
    # coexist. Insert directly to avoid any casing-convention rewrite.
    kid_upper = db.conn.execute(
        "INSERT INTO keywords (name, parent_id, is_species, type) "
        "VALUES ('Éclair', NULL, 1, 'taxonomy')"
    ).lastrowid
    kid_target = db.add_keyword("Warbler", is_species=True)
    pid = db.conn.execute(
        "INSERT INTO photos (folder_id, filename, quality_score, flag) "
        "VALUES (?, 'nonascii.jpg', 0.9, 'none')",
        (fid,),
    ).lastrowid
    # Photo currently carries `Éclair`.
    db.tag_photo(pid, kid_upper)
    # Stale species_highlights row keyed to the DIFFERENT species `éclair`
    # — SQLite treats it as unrelated to the tagged `Éclair`. The photo
    # has no `éclair` tag, so this row must stay under `éclair` after a
    # relabel of its actual (Éclair) tag.
    db.conn.execute(
        "INSERT INTO species_highlights (workspace_id, species, photo_id, rank) "
        "VALUES (?, 'éclair', ?, 0)",
        (ws_id, pid),
    )
    db.conn.commit()

    resp = client.post(
        "/api/highlights/relabel",
        json={"photo_ids": [pid], "species": "Warbler"},
    )
    assert resp.status_code == 200, resp.get_data(as_text=True)

    # The lowercase `éclair` highlight is STALE — the photo doesn't carry
    # that species — so it must remain under `éclair`, not migrate to the
    # relabel target.
    remaining = db.conn.execute(
        "SELECT species FROM species_highlights "
        "WHERE workspace_id = ? AND photo_id = ?",
        (ws_id, pid),
    ).fetchall()
    remaining_species = {r["species"] for r in remaining}
    assert "éclair" in remaining_species, (
        "stale non-ASCII-case-variant curation row must not be folded "
        "onto the relabel target"
    )
    assert "Warbler" not in remaining_species


def test_highlights_relabel_queues_remove_for_non_ascii_case_variant(app_and_db):
    """Relabeling a photo from `Éclair` to a distinct SQLite species must
    queue a `keyword_remove` for the old spelling. SQLite's NOCASE fold
    is ASCII-only, so `Éclair` and `éclair` live as separate keyword rows
    with separate ids — but Python `.lower()` folds them equal. A
    name-based `.lower()` skip on the remove would leave the old spelling
    in the sidecar even though the tag was flipped in the DB, so the
    next XMP sync would still export `Éclair`. Comparing by keyword id
    sidesteps the ASCII/Unicode fold mismatch.
    """
    app, db = app_and_db
    client = app.test_client()
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) "
        "VALUES ('/nonascii-remove', 'nonascii-remove', 'ok')"
    ).lastrowid
    ws_id = db._ws_id()
    db.conn.execute(
        "INSERT INTO workspace_folders (workspace_id, folder_id) VALUES (?, ?)",
        (ws_id, fid),
    )
    kid_upper = db.conn.execute(
        "INSERT INTO keywords (name, parent_id, is_species, type) "
        "VALUES ('Éclair', NULL, 1, 'taxonomy')"
    ).lastrowid
    pid = db.conn.execute(
        "INSERT INTO photos (folder_id, filename, quality_score, flag) "
        "VALUES (?, 'nonascii-remove.jpg', 0.9, 'none')",
        (fid,),
    ).lastrowid
    db.tag_photo(pid, kid_upper)
    db.conn.commit()

    resp = client.post(
        "/api/highlights/relabel",
        json={"photo_ids": [pid], "species": "éclair"},
    )
    assert resp.status_code == 200, resp.get_data(as_text=True)

    rows = db.conn.execute(
        "SELECT name FROM keywords WHERE name IN ('Éclair', 'éclair')"
    ).fetchall()
    assert {r["name"] for r in rows} == {"Éclair", "éclair"}, (
        "SQLite NOCASE is ASCII-only, so the two spellings must remain "
        "as distinct rows"
    )
    tagged = db.conn.execute(
        "SELECT k.name FROM photo_keywords pk "
        "JOIN keywords k ON k.id = pk.keyword_id "
        "WHERE pk.photo_id = ?",
        (pid,),
    ).fetchall()
    tagged_names = {r["name"] for r in tagged}
    assert "éclair" in tagged_names
    assert "Éclair" not in tagged_names, (
        "photo should carry only the new species row, not the old one"
    )
    pending = db.conn.execute(
        "SELECT change_type, value FROM pending_changes "
        "WHERE workspace_id = ? AND photo_id = ?",
        (ws_id, pid),
    ).fetchall()
    remove_values = {
        r["value"] for r in pending if r["change_type"] == "keyword_remove"
    }
    add_values = {
        r["value"] for r in pending if r["change_type"] == "keyword_add"
    }
    assert "Éclair" in remove_values, (
        "keyword_remove must be queued for the pre-existing non-ASCII "
        "case variant; a name-based `.lower()` compare would have folded "
        "it equal to the new species and skipped the remove, leaving the "
        "old spelling in the exported XMP"
    )
    assert "éclair" in add_values


def test_highlights_relabel_undo_preserves_representative_order(app_and_db):
    """Undoing a relabel that moved a secondary representative must
    restore it at its original ``selected_order`` — otherwise
    ``_set_global_species_representative`` would assign MAX+1 and promote
    the restored photo above the pre-existing primary representative for
    the same species."""
    app, db = app_and_db
    client = app.test_client()
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES ('/reporder', 'reporder', 'ok')"
    ).lastrowid
    db.conn.execute(
        "INSERT INTO workspace_folders (workspace_id, folder_id) VALUES (?, ?)",
        (db._ws_id(), fid),
    )
    old_kid = db.add_keyword("Order Bird", is_species=True)
    db.add_keyword("Other Bird", is_species=True)
    p_primary = db.conn.execute(
        "INSERT INTO photos (folder_id, filename, quality_score, flag) "
        "VALUES (?, 'primary.jpg', 0.9, 'none')",
        (fid,),
    ).lastrowid
    p_secondary = db.conn.execute(
        "INSERT INTO photos (folder_id, filename, quality_score, flag) "
        "VALUES (?, 'secondary.jpg', 0.9, 'none')",
        (fid,),
    ).lastrowid
    db.tag_photo(p_primary, old_kid)
    db.tag_photo(p_secondary, old_kid)
    # Select secondary first so primary ends up newest and lists first.
    db.set_species_representative("Order Bird", p_secondary)
    db.set_species_representative("Order Bird", p_primary)
    assert db.get_species_representative_lists()["Order Bird"] == [
        p_primary, p_secondary,
    ]

    resp = client.post(
        "/api/highlights/relabel",
        json={"photo_ids": [p_secondary], "species": "Other Bird"},
    )
    assert resp.status_code == 200
    assert db.get_species_representative_lists()["Order Bird"] == [p_primary]

    undone = db.undo_last_edit()
    assert undone is not None
    # Before the fix, undo called _set_global_species_representative and
    # p_secondary landed at MAX(selected_order)+1 — jumping it ahead of
    # p_primary.
    restored = db.get_species_representative_lists()["Order Bird"]
    assert restored == [p_primary, p_secondary]


def test_highlights_relabel_redo_preserves_representative_order(app_and_db):
    """Redoing a previously-undone relabel that moved a secondary
    representative must restore it at the original ``selected_order``
    captured before the first relabel. Before the fix, redo called
    ``_set_global_species_representative`` and pushed the redone
    representative above pre-existing entries under ``new_species``,
    changing which photo shows up as the Life List/Highlights primary
    after an undo/redo round trip."""
    app, db = app_and_db
    client = app.test_client()
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES ('/reporder-redo', 'reporder-redo', 'ok')"
    ).lastrowid
    db.conn.execute(
        "INSERT INTO workspace_folders (workspace_id, folder_id) VALUES (?, ?)",
        (db._ws_id(), fid),
    )
    old_kid = db.add_keyword("Redo Order Bird", is_species=True)
    new_kid = db.add_keyword("Redo Other Bird", is_species=True)
    p_source = db.conn.execute(
        "INSERT INTO photos (folder_id, filename, quality_score, flag) "
        "VALUES (?, 'source.jpg', 0.9, 'none')",
        (fid,),
    ).lastrowid
    p_target_primary = db.conn.execute(
        "INSERT INTO photos (folder_id, filename, quality_score, flag) "
        "VALUES (?, 'target_primary.jpg', 0.9, 'none')",
        (fid,),
    ).lastrowid
    db.tag_photo(p_source, old_kid)
    db.tag_photo(p_target_primary, new_kid)
    # Select p_source (in the old species) first so p_target_primary
    # takes the newer selected_order in the shared global counter and
    # stays the primary of "Redo Other Bird" after the relabel migrates
    # p_source in at its lower captured order.
    db.set_species_representative("Redo Order Bird", p_source)
    db.set_species_representative("Redo Other Bird", p_target_primary)
    assert db.get_species_representative_lists()["Redo Other Bird"] == [
        p_target_primary,
    ]

    resp = client.post(
        "/api/highlights/relabel",
        json={"photo_ids": [p_source], "species": "Redo Other Bird"},
    )
    assert resp.status_code == 200
    after_relabel = db.get_species_representative_lists()["Redo Other Bird"]
    # p_target_primary's global order is newer than p_source's, so the
    # migrated p_source lands as a secondary rep, not the primary.
    assert after_relabel == [p_target_primary, p_source]

    undone = db.undo_last_edit()
    assert undone is not None
    assert db.get_species_representative_lists()["Redo Other Bird"] == [
        p_target_primary,
    ]

    redone = db.redo_last_undo()
    assert redone is not None
    # Before the fix, redo called _set_global_species_representative and
    # p_source landed at MAX(selected_order)+1, promoting it above
    # p_target_primary. With the captured order restored on redo, the
    # round trip preserves both the list and its primary photo.
    redone_list = db.get_species_representative_lists()["Redo Other Bird"]
    assert redone_list == after_relabel


def test_rename_species_representatives_species_chunks_large_photo_lists(tmp_path):
    """``rename_species_representatives_species`` must chunk the IN(...)
    clause so a species tagged on thousands of photos doesn't blow
    SQLite's ``SQLITE_MAX_VARIABLE_NUMBER`` on legacy builds. Before the
    fix, ``rename_photo_preferences_species`` funneled every affected
    photo through a single unchunked IN clause and raised
    ``too many SQL variables``."""
    from vireo.db import Database

    db_path = tmp_path / "chunk-reps.db"
    db = Database(str(db_path))
    ws_id = db._ws_id()
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES ('/chunk', 'chunk', 'ok')"
    ).lastrowid
    db.conn.execute(
        "INSERT INTO workspace_folders (workspace_id, folder_id) VALUES (?, ?)",
        (ws_id, fid),
    )
    pid_start = db.conn.execute(
        "INSERT INTO photos (folder_id, filename) VALUES (?, 'p0.jpg')",
        (fid,),
    ).lastrowid
    # 1500 > 999 (legacy SQLITE_MAX_VARIABLE_NUMBER), so an unchunked
    # IN(...) with new_species, old_species, plus every id would exceed
    # the parameter cap.
    photo_ids = [pid_start]
    for i in range(1, 1500):
        photo_ids.append(db.conn.execute(
            "INSERT INTO photos (folder_id, filename) VALUES (?, ?)",
            (fid, f"p{i}.jpg"),
        ).lastrowid)
    for pid in photo_ids:
        db.conn.execute(
            """INSERT INTO species_representatives
                   (species, photo_id, selected_order)
               VALUES ('Old Species', ?, ?)""",
            (pid, pid),
        )
    db.conn.commit()

    moved = db.rename_species_representatives_species(
        "Old Species", "New Species", photo_ids=photo_ids,
    )
    assert moved == len(photo_ids)

    remaining_old = db.conn.execute(
        "SELECT COUNT(*) AS c FROM species_representatives WHERE species = ?",
        ("Old Species",),
    ).fetchone()["c"]
    moved_new = db.conn.execute(
        "SELECT COUNT(*) AS c FROM species_representatives WHERE species = ?",
        ("New Species",),
    ).fetchone()["c"]
    assert remaining_old == 0
    assert moved_new == len(photo_ids)


def test_backfill_species_highlights_from_legacy_preferences(tmp_path):
    """On upgraded databases, legacy photo_preferences rows with
    purpose='highlights' should seed species_highlights so pre-existing
    Highlights picks stay visible under the new ordered-highlights UI."""
    from vireo.db import Database

    db_path = tmp_path / "legacy.db"
    db = Database(str(db_path))
    ws_id = db._ws_id()
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES ('/legacy', 'legacy', 'ok')"
    ).lastrowid
    db.conn.execute(
        "INSERT INTO workspace_folders (workspace_id, folder_id) VALUES (?, ?)",
        (ws_id, fid),
    )
    pid = db.conn.execute(
        "INSERT INTO photos (folder_id, filename, quality_score, flag) "
        "VALUES (?, 'legacy.jpg', 0.9, 'none')",
        (fid,),
    ).lastrowid
    db.conn.execute(
        """INSERT INTO photo_preferences
               (workspace_id, purpose, species, photo_id,
                created_at, updated_at)
           VALUES (?, 'highlights', 'Legacy Bird', ?,
                   datetime('now'), datetime('now'))""",
        (ws_id, pid),
    )
    # Clear the one-shot marker so re-running the backfill picks up the
    # newly-added legacy row (simulates opening a DB that predated the
    # ordered-highlights feature).
    db.conn.execute(
        "DELETE FROM db_meta WHERE key = ?",
        (db._SPECIES_HIGHLIGHTS_BACKFILL_KEY,),
    )
    db.conn.commit()
    db.backfill_species_highlights_from_legacy_preferences()

    hl = db.get_species_highlights()
    assert pid in (hl.get("Legacy Bird") or {})

    # Marker set so subsequent calls are no-ops even if the row is missing.
    marker = db.conn.execute(
        "SELECT value FROM db_meta WHERE key = ?",
        (db._SPECIES_HIGHLIGHTS_BACKFILL_KEY,),
    ).fetchone()
    assert marker is not None
    db.close()


def test_highlights_accepted_species_wins_over_higher_confidence_prediction(app_and_db):
    """Manual species tag is authoritative even when a high-confidence
    prediction disagrees."""
    app, db = app_and_db
    client = app.test_client()
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES ('/c', 'c', 'ok')"
    ).lastrowid
    db.conn.execute(
        "INSERT INTO workspace_folders (workspace_id, folder_id) VALUES (?, ?)",
        (db._ws_id(), fid),
    )
    accepted_kw = db.conn.execute(
        "INSERT INTO keywords (name, type, is_species) VALUES ('Real Bird', 'taxonomy', 1)"
    ).lastrowid
    pid = db.conn.execute(
        "INSERT INTO photos (folder_id, filename, quality_score, flag) "
        "VALUES (?, 'x.jpg', 0.7, 'none')",
        (fid,),
    ).lastrowid
    db.conn.execute(
        "INSERT INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
        (pid, accepted_kw),
    )
    did = db.conn.execute(
        "INSERT INTO detections (photo_id, detector_confidence) VALUES (?, 0.9)",
        (pid,),
    ).lastrowid
    db.conn.execute(
        "INSERT INTO predictions (detection_id, classifier_model, species, confidence) "
        "VALUES (?, 'm', 'Wrong Bird', 0.99)",
        (did,),
    )
    db.conn.commit()

    resp = client.get(f"/api/highlights?folder_id={fid}&confidence_threshold=0.5")
    data = resp.get_json()
    assert len(data["buckets"]) == 1
    assert data["buckets"][0]["species"] == "Real Bird"
    assert data["buckets"][0]["is_accepted"] is True


def _seed_waxbill_casing_scenario(db):
    """One accepted `Common waxbill` photo plus one photo whose only signal
    is a raw title-cased prediction `Common Waxbill` (label-file casing —
    prediction rows are external vocabulary and stay verbatim)."""
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES ('/wax', 'wax', 'ok')"
    ).lastrowid
    db.conn.execute(
        "INSERT INTO workspace_folders (workspace_id, folder_id) VALUES (?, ?)",
        (db._ws_id(), fid),
    )
    waxbill_kw = db.conn.execute(
        "INSERT INTO keywords (name, type, is_species) "
        "VALUES ('Common waxbill', 'taxonomy', 1)"
    ).lastrowid
    accepted_pid = db.conn.execute(
        "INSERT INTO photos (folder_id, filename, quality_score, flag) "
        "VALUES (?, 'accepted.jpg', 0.9, 'none')",
        (fid,),
    ).lastrowid
    db.conn.execute(
        "INSERT INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
        (accepted_pid, waxbill_kw),
    )
    predicted_pid = db.conn.execute(
        "INSERT INTO photos (folder_id, filename, quality_score, flag) "
        "VALUES (?, 'predicted.jpg', 0.8, 'none')",
        (fid,),
    ).lastrowid
    did = db.conn.execute(
        "INSERT INTO detections (photo_id, detector_confidence) VALUES (?, 0.9)",
        (predicted_pid,),
    ).lastrowid
    db.conn.execute(
        "INSERT INTO predictions "
        "(detection_id, classifier_model, species, confidence) "
        "VALUES (?, 'm', 'Common Waxbill', 0.95)",
        (did,),
    )
    db.conn.commit()
    return fid, accepted_pid, predicted_pid


def test_highlights_merges_prediction_casing_with_accepted_keyword(app_and_db):
    """A title-cased classifier label and a sentence-cased accepted keyword
    are the same species: one Highlights bucket, keyed by the keyword
    spelling. Prediction rows keep label-file casing (external vocabulary),
    so the merge must happen at bucket-collection time."""
    app, db = app_and_db
    client = app.test_client()
    fid, accepted_pid, predicted_pid = _seed_waxbill_casing_scenario(db)

    resp = client.get(f"/api/highlights?folder_id={fid}&confidence_threshold=0.7")
    assert resp.status_code == 200
    data = resp.get_json()
    waxbill_buckets = [
        b for b in data["buckets"] if b["species"].lower() == "common waxbill"
    ]
    assert len(waxbill_buckets) == 1
    bucket = waxbill_buckets[0]
    assert bucket["species"] == "Common waxbill"
    assert {p["id"] for p in bucket["photos"]} == {accepted_pid, predicted_pid}


def test_species_highlights_add_canonicalizes_prediction_cased_label(app_and_db):
    """Starring a photo in a prediction-cased bucket must store and surface
    the highlight under the keyword spelling.

    Regression for the silently-dropped star: the client sends the bucket
    label (`Common Waxbill` from the classifier), but the eligibility
    queries compare species strings exact against `keywords.name`
    (`Common waxbill`). Without canonicalization at the route/setter, the
    star writes a `Common Waxbill` row that no query ever matches again."""
    app, db = app_and_db
    client = app.test_client()
    fid, _accepted_pid, predicted_pid = _seed_waxbill_casing_scenario(db)

    resp = client.post(
        "/api/species-highlights",
        json={"species": "Common Waxbill", "photo_id": predicted_pid},
    )
    assert resp.status_code == 200

    rows = db.conn.execute(
        "SELECT species FROM species_highlights WHERE photo_id = ?",
        (predicted_pid,),
    ).fetchall()
    assert [r["species"] for r in rows] == ["Common waxbill"]

    resp = client.get(f"/api/highlights?folder_id={fid}&confidence_threshold=0.7")
    bucket = next(
        b for b in resp.get_json()["buckets"]
        if b["species"] == "Common waxbill"
    )
    assert bucket["has_highlight_selection"] is True
    starred = next(p for p in bucket["photos"] if p["id"] == predicted_pid)
    assert starred["is_highlighted"] is True

    # Un-starring with yet another casing of the same label must find and
    # remove the canonical row rather than silently deleting nothing.
    resp = client.delete(
        "/api/species-highlights",
        json={"species": "COMMON WAXBILL", "photo_id": predicted_pid},
    )
    assert resp.status_code == 200
    assert resp.get_json()["removed"] == 1


def test_species_highlights_add_preserves_ambiguous_homonym_species(app_and_db):
    """When two intentionally-distinct root species keywords share a
    NOCASE key (legacy general ``Robin`` alongside taxonomy ``robin``),
    curation requests from a specific bucket must land on that bucket's
    exact spelling. Silently collapsing to one canonical spelling would
    make the eligibility precheck (which compares ``bucket["species"]``
    exact) reject the request coming from the other homonym's bucket."""
    app, db = app_and_db
    client = app.test_client()
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES ('/robin', 'robin', 'ok')"
    ).lastrowid
    db.conn.execute(
        "INSERT INTO workspace_folders (workspace_id, folder_id) VALUES (?, ?)",
        (db._ws_id(), fid),
    )
    legacy_kw = db.conn.execute(
        "INSERT INTO keywords (name, type, is_species) "
        "VALUES ('Robin', 'general', 1)"
    ).lastrowid
    db.conn.execute(
        "INSERT INTO keywords (name, type, is_species) "
        "VALUES ('robin', 'taxonomy', 1)"
    )
    legacy_pid = db.conn.execute(
        "INSERT INTO photos (folder_id, filename, quality_score, flag) "
        "VALUES (?, 'legacy.jpg', 0.9, 'none')",
        (fid,),
    ).lastrowid
    db.conn.execute(
        "INSERT INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
        (legacy_pid, legacy_kw),
    )
    db.conn.commit()

    resp = client.get(f"/api/highlights?folder_id={fid}&confidence_threshold=0.5")
    assert resp.status_code == 200
    buckets = resp.get_json()["buckets"]
    legacy_bucket = next(b for b in buckets if b["species"] == "Robin")
    assert legacy_pid in {p["id"] for p in legacy_bucket["photos"]}

    resp = client.post(
        "/api/species-highlights",
        json={"species": "Robin", "photo_id": legacy_pid},
    )
    assert resp.status_code == 200

    rows = db.conn.execute(
        "SELECT species FROM species_highlights WHERE photo_id = ?",
        (legacy_pid,),
    ).fetchall()
    assert [r["species"] for r in rows] == ["Robin"]


def test_highlights_relabel_snapshots_match_add_keyword_pick_for_homonyms(
    app_and_db,
):
    """When two intentionally-distinct species keywords share a NOCASE key
    (legacy general ``Robin`` alongside taxonomy ``robin``), the relabel
    snapshots must key on the SAME spelling ``add_keyword`` will store.
    ``resolve_species_display_name`` preserves the caller spelling
    (``Robin``) for bucket/parse/setter symmetry, but ``add_keyword``'s
    typed lookup picks the taxonomy row (``robin``); before the fix the
    snapshot compared destination curation against ``Robin`` while the
    rename actually landed on ``robin``, so undo would delete a pre-existing
    ``robin`` highlight it never created."""
    app, db = app_and_db
    client = app.test_client()
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES ('/hrhom', 'hrhom', 'ok')"
    ).lastrowid
    db.conn.execute(
        "INSERT INTO workspace_folders (workspace_id, folder_id) VALUES (?, ?)",
        (db._ws_id(), fid),
    )
    old_kid = db.add_keyword("Legacy Species", is_species=True)
    # Legacy general row with is_species=1 (a pre-migration state that
    # the v2 sweep intentionally preserves for ambiguous homonyms), plus
    # the taxonomy row add_keyword's typed lookup will pick.
    legacy_robin = db.conn.execute(
        "INSERT INTO keywords (name, type, is_species) "
        "VALUES ('Robin', 'general', 1)"
    ).lastrowid
    taxonomy_robin = db.conn.execute(
        "INSERT INTO keywords (name, type, is_species) "
        "VALUES ('robin', 'taxonomy', 1)"
    ).lastrowid
    # The photo being relabelled currently carries the "Legacy Species"
    # tag; the relabel will move it to whichever Robin row add_keyword
    # picks (the taxonomy row).
    pid = db.conn.execute(
        "INSERT INTO photos (folder_id, filename, quality_score, flag) "
        "VALUES (?, 'moving.jpg', 0.9, 'none')",
        (fid,),
    ).lastrowid
    db.tag_photo(pid, old_kid)
    db.add_species_highlight("Legacy Species", pid)
    # Pre-existing destination highlight, keyed on the taxonomy spelling
    # (which is what add_keyword would store for a "Robin" request).
    db.conn.execute(
        "INSERT INTO species_highlights (workspace_id, species, photo_id, rank) "
        "VALUES (?, 'robin', ?, 0)",
        (db._ws_id(), pid),
    )
    db.conn.commit()

    resp = client.post(
        "/api/highlights/relabel",
        json={"photo_ids": [pid], "species": "Robin"},
    )
    assert resp.status_code == 200
    # add_keyword's typed lookup returns the taxonomy row, so the tag
    # lands on `robin`. The pre-existing `robin` highlight survives.
    tagged = db.conn.execute(
        "SELECT k.name FROM photo_keywords pk "
        "JOIN keywords k ON k.id = pk.keyword_id "
        "WHERE pk.photo_id = ? AND (k.is_species = 1 OR k.type = 'taxonomy')",
        (pid,),
    ).fetchall()
    assert [r["name"] for r in tagged] == ["robin"]
    hl = db.get_species_highlights()
    assert pid in (hl.get("robin") or {})

    undone = db.undo_last_edit()
    assert undone is not None
    hl_after_undo = db.get_species_highlights()
    # The pre-existing `robin` highlight must survive the undo — before
    # the fix the snapshot recorded dst_existed=False (compared against
    # "Robin") and undo deleted the user's pre-existing row.
    assert pid in (hl_after_undo.get("robin") or {}), (
        "Undo deleted pre-existing robin highlight because snapshots keyed "
        "on the wrong spelling"
    )
    # And the original bucket is restored.
    assert pid in (hl_after_undo.get("Legacy Species") or {})
    # Silence the unused-variable warnings on the setup rows: keeping
    # them named documents the two homonym roles.
    assert legacy_robin != taxonomy_robin


def test_species_highlight_eligibility_accepted_species_exact_match(app_and_db):
    """When a photo's accepted keyword resolves to one specific spelling of
    an ambiguous homonym, a stored highlight for the OTHER homonym must
    stay ineligible. Applying COLLATE NOCASE across the whole eligibility
    COALESCE relaxed the accepted-keyword branch too: a stored highlight
    keyed ``Robin`` would then match a photo whose accepted keyword
    subquery returned ``robin``, silently making the wrong species bucket
    appear to have a highlight."""
    app, db = app_and_db
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES ('/elig', 'elig', 'ok')"
    ).lastrowid
    db.conn.execute(
        "INSERT INTO workspace_folders (workspace_id, folder_id) VALUES (?, ?)",
        (db._ws_id(), fid),
    )
    # Two intentionally-distinct root species keywords sharing a NOCASE
    # key — the migration explicitly preserves this shape.
    legacy_kid = db.conn.execute(
        "INSERT INTO keywords (name, type, is_species) "
        "VALUES ('Robin', 'general', 1)"
    ).lastrowid
    taxonomy_kid = db.conn.execute(
        "INSERT INTO keywords (name, type, is_species) "
        "VALUES ('robin', 'taxonomy', 1)"
    ).lastrowid
    pid = db.conn.execute(
        "INSERT INTO photos (folder_id, filename, quality_score, flag) "
        "VALUES (?, 'elig.jpg', 0.9, 'none')",
        (fid,),
    ).lastrowid
    # The photo's accepted keyword is the TAXONOMY row (lowercase
    # ``robin``); the accepted-branch of the eligibility COALESCE
    # returns exactly ``robin``.
    db.conn.execute(
        "INSERT INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
        (pid, taxonomy_kid),
    )
    # Stored highlight keyed on the OTHER homonym's spelling
    # (uppercase ``Robin``, the general row) — a plausible remnant when
    # ambiguous-homonym curation is left alone by the v2 sweep.
    db.conn.execute(
        "INSERT INTO species_highlights (workspace_id, species, photo_id, rank) "
        "VALUES (?, 'Robin', ?, 0)",
        (db._ws_id(), pid),
    )
    db.conn.commit()

    eligible = db.get_species_highlights("Robin", eligible_only=True)
    # Before the fix the whole-COALESCE NOCASE would match ``Robin`` to
    # the accepted ``robin`` and mark the highlight eligible. With the
    # branch-specific NOCASE, the accepted keyword compares EXACT.
    assert eligible == {}, (
        "Cross-homonym match: stored 'Robin' highlight should NOT be "
        "eligible for a photo whose accepted keyword is 'robin'"
    )
    # The same-spelling case still passes (sanity check the accepted
    # branch still resolves an exact match).
    db.conn.execute(
        "DELETE FROM photo_keywords WHERE photo_id = ?", (pid,)
    )
    db.conn.execute(
        "INSERT INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
        (pid, legacy_kid),
    )
    db.conn.commit()
    same_spelling = db.get_species_highlights("Robin", eligible_only=True)
    assert pid in (same_spelling.get("Robin") or {})


def test_resolve_species_display_name_prefers_taxonomy_over_general_homonym(
    app_and_db,
):
    """When a taxonomy species row and a non-species general row share a
    NOCASE key (e.g. taxonomy ``Common waxbill`` alongside a hand-tagged
    ``Common Waxbill`` general), ``add_keyword(is_species=True)`` picks
    the taxonomy row. ``resolve_species_display_name`` must mirror that
    pick — otherwise the bucket collection keys prediction-only photos
    under ``Common Waxbill``, curation setters store rows under the
    non-taxonomy spelling, and once the prediction is accepted the
    stored highlight no longer matches the accepted keyword spelling
    (``Common waxbill``) and silently disappears from the bucket."""
    _, db = app_and_db
    # Taxonomy species row (the one add_keyword prefers).
    db.conn.execute(
        "INSERT INTO keywords (name, type, is_species) "
        "VALUES ('Common waxbill', 'taxonomy', 1)"
    )
    # Separate non-species general row with classifier casing —
    # someone hand-tagged the same spelling as a plain keyword.
    db.conn.execute(
        "INSERT INTO keywords (name, type, is_species) "
        "VALUES ('Common Waxbill', 'general', 0)"
    )
    db.conn.commit()

    # Every caller spelling must land on the taxonomy row's stored
    # spelling: that's what add_keyword(is_species=True) will pick, and
    # bucket / parse / setter paths all agree on the same string.
    assert db.resolve_species_display_name("Common Waxbill") == "Common waxbill"
    assert db.resolve_species_display_name("common waxbill") == "Common waxbill"
    assert db.resolve_species_display_name("COMMON WAXBILL") == "Common waxbill"

    # Cross-check the invariant: add_keyword(is_species=True) actually
    # returns the taxonomy row — otherwise the resolve fix would be
    # tracking a different behavior than reality.
    picked_id = db.add_keyword("Common Waxbill", is_species=True)
    picked_name = db.conn.execute(
        "SELECT name FROM keywords WHERE id = ?", (picked_id,)
    ).fetchone()["name"]
    assert picked_name == "Common waxbill"


def test_resolve_species_display_name_promotes_non_species_general_alone(
    app_and_db,
):
    """When only a non-species general row shares the NOCASE key,
    ``add_keyword(is_species=True)`` finds and promotes it in place —
    is_species/type flip but the stored name is untouched. Resolve must
    return that stored name so curation keys on the string the promoted
    row will carry, not on a case-convention-derived spelling that
    doesn't exist."""
    _, db = app_and_db
    db.conn.execute(
        "INSERT INTO keywords (name, type, is_species) "
        "VALUES ('Common Waxbill', 'general', 0)"
    )
    db.conn.commit()

    # No taxonomy candidate for this NOCASE key, so add_keyword will
    # promote the general row rather than insert a fresh taxonomy row.
    # Resolve returns the general's stored spelling — the string
    # curation setters must key on.
    assert db.resolve_species_display_name("common waxbill") == "Common Waxbill"

    # add_keyword promotes in place; the row's name doesn't change.
    picked_id = db.add_keyword("common waxbill", is_species=True)
    row = db.conn.execute(
        "SELECT name, type, is_species FROM keywords WHERE id = ?",
        (picked_id,),
    ).fetchone()
    assert row["name"] == "Common Waxbill"
    assert row["type"] == "taxonomy"
    assert row["is_species"] == 1


def test_highlights_save(app_and_db):
    """POST /api/highlights/save creates a static collection."""
    app, db = app_and_db
    client = app.test_client()
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES ('/save_test', 'save_test', 'ok')"
    ).lastrowid
    db.conn.execute(
        "INSERT INTO workspace_folders (workspace_id, folder_id) VALUES (?, ?)",
        (db._ws_id(), fid),
    )
    pid = db.conn.execute(
        "INSERT INTO photos (folder_id, filename, quality_score) VALUES (?, 'a.jpg', 0.8)",
        (fid,),
    ).lastrowid
    db.conn.commit()

    resp = client.post("/api/highlights/save", json={
        "photo_ids": [pid],
        "name": "Highlights - save_test",
    })
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["ok"] is True
    assert "id" in data

    # Verify collection was created
    collections = db.get_collections()
    names = [c["name"] for c in collections]
    assert "Highlights - save_test" in names


def test_highlights_scope_workspace_blends_folders(app_and_db):
    """scope=workspace blends candidates across every folder in the
    active workspace (matches existing folder-scope behavior)."""
    app, db = app_and_db
    client = app.test_client()
    apapane_kw = db.conn.execute(
        "INSERT INTO keywords (name, type, is_species) VALUES ('ʻApapane', 'taxonomy', 1)"
    ).lastrowid
    for fname in ("2024-01-15", "2024-01-16"):
        fid = db.conn.execute(
            "INSERT INTO folders (path, name, status) VALUES (?, ?, 'ok')",
            (f"/shoot/{fname}", fname),
        ).lastrowid
        db.conn.execute(
            "INSERT INTO workspace_folders (workspace_id, folder_id) VALUES (?, ?)",
            (db._ws_id(), fid),
        )
        pid = db.conn.execute(
            "INSERT INTO photos (folder_id, filename, quality_score, flag) "
            "VALUES (?, ?, 0.8, 'none')",
            (fid, f"{fname}.jpg"),
        ).lastrowid
        db.conn.execute(
            "INSERT INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
            (pid, apapane_kw),
        )
    db.conn.commit()

    resp = client.get("/api/highlights?scope=workspace")
    data = resp.get_json()
    assert len(data["buckets"]) == 1
    assert data["buckets"][0]["photo_count"] == 2


def test_api_import_folder_preview(app_and_db, tmp_path):
    """POST /api/import/folder-preview returns file discovery results."""
    app, db = app_and_db

    # Create test images in a temp folder
    source = tmp_path / "source_photos"
    source.mkdir()
    from PIL import Image
    for name in ["a.jpg", "b.jpg", "c.png"]:
        Image.new("RGB", (200, 150)).save(str(source / name))
    # Non-image file should be excluded
    (source / "readme.txt").write_text("ignore me")

    client = app.test_client()
    resp = client.post("/api/import/folder-preview", json={
        "folders": [str(source)],
        "file_types": [".jpg", ".jpeg", ".png"],
    })
    assert resp.status_code == 200
    data = resp.get_json()

    assert data["total_count"] == 3
    assert data["total_size"] > 0
    assert ".jpg" in data["type_breakdown"]
    assert data["type_breakdown"][".jpg"] == 2
    assert data["type_breakdown"][".png"] == 1
    assert len(data["files"]) == 3
    assert data["duplicate_count"] == 0


def test_api_import_folder_preview_duplicate_count_deferred(app_and_db, tmp_path):
    """Folder preview returns duplicate_count=0 (duplicate detection deferred)."""
    app, db = app_and_db

    source = tmp_path / "source_dupes"
    source.mkdir()
    from PIL import Image
    Image.new("RGB", (100, 100)).save(str(source / "bird1.jpg"))
    Image.new("RGB", (100, 100)).save(str(source / "newbird.jpg"))

    client = app.test_client()
    resp = client.post("/api/import/folder-preview", json={
        "folders": [str(source)],
        "file_types": [".jpg", ".jpeg"],
    })
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["total_count"] == 2
    assert data["duplicate_count"] == 0


def test_api_import_folder_preview_summary_only_omits_file_rows(app_and_db, tmp_path):
    """Summary-only folder preview keeps count cheap for import source rows."""
    app, _ = app_and_db

    source = tmp_path / "source_summary"
    source.mkdir()
    from PIL import Image
    Image.new("RGB", (100, 100)).save(str(source / "bird1.jpg"))
    Image.new("RGB", (100, 100)).save(str(source / "bird2.jpg"))
    (source / "notes.txt").write_text("ignore me")

    client = app.test_client()
    resp = client.post("/api/import/folder-preview", json={
        "folders": [str(source)],
        "file_types": "both",
        "summary_only": True,
    })
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["total_count"] == 2
    assert data["type_breakdown"][".jpg"] == 2
    assert data["files"] == []


def test_api_import_folder_preview_no_folders(app_and_db):
    """Folder preview returns error when no folders provided."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.post("/api/import/folder-preview", json={})
    assert resp.status_code == 400


def test_api_import_folder_preview_nonexistent(app_and_db):
    """Folder preview returns error for non-existent folder."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.post("/api/import/folder-preview", json={
        "folders": ["/nonexistent/path/xyz"],
        "file_types": [".jpg"],
    })
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["total_count"] == 0


def test_api_import_folder_preview_subfolders(app_and_db, tmp_path):
    """Folder preview groups files by subfolder."""
    app, _ = app_and_db

    source = tmp_path / "nested"
    (source / "sub1").mkdir(parents=True)
    (source / "sub2").mkdir(parents=True)
    from PIL import Image
    Image.new("RGB", (100, 100)).save(str(source / "root.jpg"))
    Image.new("RGB", (100, 100)).save(str(source / "sub1" / "a.jpg"))
    Image.new("RGB", (100, 100)).save(str(source / "sub2" / "b.jpg"))

    client = app.test_client()
    resp = client.post("/api/import/folder-preview", json={
        "folders": [str(source)],
        "file_types": [".jpg", ".jpeg"],
    })
    data = resp.get_json()
    assert data["total_count"] == 3

    # Files should have subfolder info
    subfolders = set()
    for f in data["files"]:
        subfolders.add(f["subfolder"])
    assert len(subfolders) == 3  # root, sub1, sub2


def test_api_import_folder_preview_multi_source_same_basename(app_and_db, tmp_path):
    """Multi-source preview disambiguates folders with same basename."""
    app, _ = app_and_db

    # Two sources with identical leaf names and overlapping subfolders
    card_a = tmp_path / "mnt" / "cardA" / "DCIM"
    card_b = tmp_path / "mnt" / "cardB" / "DCIM"
    (card_a / "100CANON").mkdir(parents=True)
    (card_b / "100CANON").mkdir(parents=True)
    from PIL import Image
    Image.new("RGB", (100, 100)).save(str(card_a / "100CANON" / "a.jpg"))
    Image.new("RGB", (100, 100)).save(str(card_b / "100CANON" / "b.jpg"))

    client = app.test_client()
    resp = client.post("/api/import/folder-preview", json={
        "folders": [str(card_a), str(card_b)],
        "file_types": [".jpg", ".jpeg"],
    })
    data = resp.get_json()
    assert data["total_count"] == 2

    # Subfolders must be distinct even though both have 100CANON
    subfolders = {f["subfolder"] for f in data["files"]}
    assert len(subfolders) == 2
    # Should use parent to disambiguate: cardA/DCIM/100CANON vs cardB/DCIM/100CANON
    for sf in subfolders:
        assert "DCIM" in sf
        assert "100CANON" in sf


def test_api_import_folder_preview_thumbnail(app_and_db, tmp_path):
    """GET /api/import/folder-preview/thumbnail returns a JPEG thumbnail."""
    app, _ = app_and_db

    # Create a test image
    source = tmp_path / "thumb_test"
    source.mkdir()
    from PIL import Image
    img = Image.new("RGB", (800, 600), color=(255, 0, 0))
    img_path = source / "photo.jpg"
    img.save(str(img_path))

    client = app.test_client()
    resp = client.get(f"/api/import/folder-preview/thumbnail?path={img_path}")
    assert resp.status_code == 200
    assert resp.content_type == "image/jpeg"
    assert len(resp.data) > 0

    # Verify the returned image is resized (200px long edge)
    import io
    thumb = Image.open(io.BytesIO(resp.data))
    assert max(thumb.size) == 200


def test_api_import_folder_preview_thumbnail_missing(app_and_db):
    """Thumbnail endpoint returns 404 for non-existent file, with
    Cache-Control: no-store so a transient libraw / NAS hiccup doesn't
    pin question marks in the user's preview grid for the cache lifetime.
    """
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get("/api/import/folder-preview/thumbnail?path=/no/such/file.jpg")
    assert resp.status_code == 404
    # Browsers must NOT cache this failure — next page load should retry.
    cc = resp.headers.get("Cache-Control", "")
    assert "no-store" in cc, f"expected no-store on 404, got {cc!r}"


def test_api_import_folder_preview_thumbnail_unloadable_returns_404_no_store(
    app_and_db, tmp_path, monkeypatch,
):
    """When the file exists but image_loader returns None (libraw failure,
    unsupported format, etc.), the endpoint must 404 *without* caching —
    the failure is often transient (NAS contention) and a cached negative
    would block recovery on next render."""
    app, _ = app_and_db
    src = tmp_path / "broken.nef"
    src.write_bytes(b"not actually a NEF")  # exists, but unloadable
    import image_loader
    monkeypatch.setattr(image_loader, "load_image", lambda *a, **kw: None)

    client = app.test_client()
    resp = client.get(
        "/api/import/folder-preview/thumbnail?path=" + str(src),
    )
    assert resp.status_code == 404
    assert "no-store" in resp.headers.get("Cache-Control", "")


def test_api_import_folder_preview_thumbnail_success_is_cacheable(
    app_and_db, tmp_path,
):
    """Successful thumbnail responses keep the existing 5-min cache so
    the preview doesn't re-decode RAWs on every grid scroll."""
    app, _ = app_and_db
    from PIL import Image
    src = tmp_path / "ok.jpg"
    Image.new("RGB", (300, 200), "red").save(src)
    client = app.test_client()
    resp = client.get(
        "/api/import/folder-preview/thumbnail?path=" + str(src),
    )
    assert resp.status_code == 200
    cc = resp.headers.get("Cache-Control", "")
    assert "max-age" in cc and "no-store" not in cc


def test_api_import_folder_preview_thumbnail_no_path(app_and_db):
    """Thumbnail endpoint returns 400 when path param is missing."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get("/api/import/folder-preview/thumbnail")
    assert resp.status_code == 400


def test_api_import_full_accepts_exclude_paths(app_and_db, tmp_path):
    """POST /api/jobs/import-full accepts exclude_paths parameter."""
    app, _ = app_and_db

    source = tmp_path / "import_src"
    source.mkdir()
    from PIL import Image
    Image.new("RGB", (100, 100)).save(str(source / "keep.jpg"))
    Image.new("RGB", (100, 100)).save(str(source / "skip.jpg"))

    client = app.test_client()
    resp = client.post("/api/jobs/import-full", json={
        "source": str(source),
        "copy": False,
        "file_types": [".jpg", ".jpeg"],
        "exclude_paths": [str(source / "skip.jpg")],
    })
    assert resp.status_code == 200
    data = resp.get_json()
    assert "job_id" in data


def test_system_info_megadetector_weights_missing(app_and_db, monkeypatch, tmp_path):
    """/api/system/info reports weights_missing (not installed) when only the
    detector module imports but the ONNX weights file is absent.
    """
    import detector
    missing_path = str(tmp_path / "does_not_exist.onnx")
    monkeypatch.setattr(detector, "MEGADETECTOR_ONNX_PATH", missing_path)

    app, _ = app_and_db
    client = app.test_client()
    resp = client.get("/api/system/info")
    assert resp.status_code == 200
    data = resp.get_json()

    assert data["megadetector"] == "weights_missing"
    assert data["megadetector_weights"] == "not downloaded"
    assert "weights not downloaded" in data["megadetector_detail"].lower()


def test_system_info_megadetector_installed_when_weights_present(app_and_db, monkeypatch, tmp_path):
    """/api/system/info reports installed only when weights are on disk."""
    import detector
    weights = tmp_path / "model.onnx"
    weights.write_bytes(b"\x00" * 1024)
    monkeypatch.setattr(detector, "MEGADETECTOR_ONNX_PATH", str(weights))

    app, _ = app_and_db
    client = app.test_client()
    resp = client.get("/api/system/info")
    assert resp.status_code == 200
    data = resp.get_json()

    assert data["megadetector"] == "installed"
    assert data["megadetector_weights"] == "downloaded"


def test_pipeline_models_dinov2_incomplete_without_data_sidecar(
    app_and_db, monkeypatch, tmp_path,
):
    """DINOv2 reports 'incomplete' when only model.onnx is on disk.

    DINOv2 uses external-data ONNX: the ~1 MB model.onnx graph is useless
    without the companion model.onnx.data weights file. Without this check
    the status endpoint used to report "downloaded 1.0 MB" for a broken
    install that couldn't actually run.
    """
    monkeypatch.setenv("HOME", str(tmp_path))
    variant_dir = tmp_path / ".vireo" / "models" / "dinov2-vit-b14"
    variant_dir.mkdir(parents=True)
    (variant_dir / "model.onnx").write_bytes(b"\x00" * 1024)  # graph only

    app, _ = app_and_db
    client = app.test_client()
    resp = client.get("/api/models/pipeline")
    assert resp.status_code == 200
    entry = next(m for m in resp.get_json()["models"] if m["id"] == "vit-b14")
    assert entry["status"] == "incomplete"
    assert entry["size"] is None


def test_pipeline_models_dinov2_downloaded_sums_graph_and_data(
    app_and_db, monkeypatch, tmp_path,
):
    """DINOv2 reports 'downloaded' with total size once both files exist."""
    monkeypatch.setenv("HOME", str(tmp_path))
    variant_dir = tmp_path / ".vireo" / "models" / "dinov2-vit-b14"
    variant_dir.mkdir(parents=True)
    (variant_dir / "model.onnx").write_bytes(b"\x00" * (1 * 1024 * 1024))
    (variant_dir / "model.onnx.data").write_bytes(b"\x00" * (10 * 1024 * 1024))

    app, _ = app_and_db
    client = app.test_client()
    resp = client.get("/api/models/pipeline")
    assert resp.status_code == 200
    entry = next(m for m in resp.get_json()["models"] if m["id"] == "vit-b14")
    assert entry["status"] == "downloaded"
    assert entry["size"] == "11.0 MB"


def test_embedding_matrix_excludes_timm_models(app_and_db, monkeypatch, tmp_path):
    """Timm models don't use per-label text embeddings, so the matrix should
    not list them — otherwise Settings renders a 'Compute' button that fails
    because timm model dirs lack image_encoder.onnx."""
    labels_file = tmp_path / "birds.txt"
    labels_file.write_text("robin\nsparrow\n")

    monkeypatch.setattr(
        "models.get_models",
        lambda: [
            {
                "id": "bioclip-vit-b-16",
                "name": "BioCLIP",
                "model_type": "bioclip",
                "model_str": "ViT-B-16",
                "weights_path": str(tmp_path),
                "downloaded": True,
            },
            {
                "id": "timm-inat21-eva02-l",
                "name": "iNat21 (EVA-02 Large)",
                "model_type": "timm",
                "model_str": "hf-hub:timm/eva02",
                "weights_path": str(tmp_path),
                "downloaded": True,
            },
        ],
    )
    monkeypatch.setattr(
        "labels.get_saved_labels",
        lambda: [{"name": "Birds", "labels_file": str(labels_file)}],
    )

    app, _ = app_and_db
    client = app.test_client()
    resp = client.get("/api/embedding-matrix")
    assert resp.status_code == 200
    data = resp.get_json()
    model_ids = [m["id"] for m in data["models"]]
    assert "bioclip-vit-b-16" in model_ids
    assert "timm-inat21-eva02-l" not in model_ids


def test_precompute_embeddings_rejects_timm_models(app_and_db, monkeypatch):
    """Hitting precompute-embeddings for a timm model must fail fast instead
    of trying to load a non-existent image_encoder.onnx from the timm dir."""
    monkeypatch.setattr(
        "models.get_models",
        lambda: [
            {
                "id": "timm-inat21-eva02-l",
                "name": "iNat21 (EVA-02 Large)",
                "model_type": "timm",
                "model_str": "hf-hub:timm/eva02",
                "weights_path": "/fake",
                "downloaded": True,
            },
        ],
    )

    app, _ = app_and_db
    client = app.test_client()
    resp = client.post(
        "/api/jobs/precompute-embeddings",
        json={"model_id": "timm-inat21-eva02-l", "labels_file": "/tmp/x.txt"},
    )
    assert resp.status_code == 200
    job_id = resp.get_json()["job_id"]

    runner = app._job_runner
    import time
    deadline = time.time() + 5
    while time.time() < deadline:
        job = runner.get(job_id)
        if job and job.get("status") in ("completed", "failed"):
            break
        time.sleep(0.05)
    job = runner.get(job_id)
    assert job["status"] == "failed"
    assert any("fixed class head" in e for e in (job.get("errors") or []))


# ---------------------------------------------------------------------------
# /api/storage/masks endpoints
# ---------------------------------------------------------------------------

def _seed_masks(db, tmp_path):
    """Insert a few photo_masks rows + on-disk files for storage tests.

    Layout:
      photo 1: sam2-small (active), sam2-large
      photo 2: sam2-small
    Returns the masks directory path.
    """
    masks_dir = tmp_path / "masks"
    masks_dir.mkdir(exist_ok=True)
    pids = [r["id"] for r in db.conn.execute(
        "SELECT id FROM photos ORDER BY id LIMIT 2"
    ).fetchall()]
    p1, p2 = pids[0], pids[1]

    for pid, var, size in [
        (p1, "sam2-small", 100),
        (p1, "sam2-large", 200),
        (p2, "sam2-small", 150),
    ]:
        f = masks_dir / f"{pid}.{var}.png"
        f.write_bytes(b"x" * size)
        db.upsert_photo_mask(
            photo_id=pid, variant=var, path=str(f),
            detector_model="megadetector-v6",
            prompt_x=0, prompt_y=0, prompt_w=10, prompt_h=10,
        )
    # Both photos get an active variant set: this mirrors what the
    # pipeline always does after upsert_photo_mask, so the seed reflects
    # realistic state. delete_inactive_masks now skips photos with NULL
    # active (the partial-state case) rather than treating them as
    # entirely-inactive.
    db.set_active_mask_variant(p1, "sam2-small")
    db.set_active_mask_variant(p2, "sam2-small")
    return masks_dir, p1, p2


def test_api_storage_masks_returns_summary(app_and_db, tmp_path):
    app, db = app_and_db
    _seed_masks(db, tmp_path)
    client = app.test_client()
    r = client.get("/api/storage/masks")
    assert r.status_code == 200
    data = r.get_json()
    assert "variants" in data
    assert "total_bytes" in data
    assert "stale_count" in data
    assert "path" in data
    by_var = {v["variant"]: v for v in data["variants"]}
    assert by_var["sam2-small"]["count"] == 2
    assert by_var["sam2-small"]["active_count"] == 2
    assert by_var["sam2-large"]["count"] == 1
    assert by_var["sam2-large"]["active_count"] == 0
    assert data["total_bytes"] == 100 + 200 + 150


def test_api_storage_masks_empty(app_and_db):
    app, _db = app_and_db
    client = app.test_client()
    r = client.get("/api/storage/masks")
    assert r.status_code == 200
    data = r.get_json()
    assert data["variants"] == []
    assert data["total_bytes"] == 0
    assert data["stale_count"] == 0


def test_api_storage_masks_delete_variant(app_and_db, tmp_path):
    app, db = app_and_db
    masks_dir, _p1, _p2 = _seed_masks(db, tmp_path)
    client = app.test_client()
    # sam2-large is not active anywhere — should delete fine.
    r = client.post(
        "/api/storage/masks/delete-variant",
        json={"variant": "sam2-large"},
    )
    assert r.status_code == 200
    body = r.get_json()
    assert body["ok"] is True
    assert body["deleted"] == 1
    # File removed
    assert not any(p.name.endswith(".sam2-large.png") for p in masks_dir.iterdir())
    # Row removed
    n = db.conn.execute(
        "SELECT COUNT(*) FROM photo_masks WHERE variant='sam2-large'"
    ).fetchone()[0]
    assert n == 0


def test_api_storage_masks_delete_variant_refuses_active(app_and_db, tmp_path):
    app, db = app_and_db
    _seed_masks(db, tmp_path)
    client = app.test_client()
    # sam2-small is active for photo 1 — should refuse with 400.
    r = client.post(
        "/api/storage/masks/delete-variant",
        json={"variant": "sam2-small"},
    )
    assert r.status_code == 400
    body = r.get_json()
    assert "active" in body["error"].lower()


def test_api_storage_masks_delete_variant_requires_name(app_and_db):
    app, _db = app_and_db
    client = app.test_client()
    r = client.post("/api/storage/masks/delete-variant", json={})
    assert r.status_code == 400
    assert "variant" in r.get_json()["error"].lower()


def test_api_storage_masks_delete_inactive(app_and_db, tmp_path):
    app, db = app_and_db
    _seed_masks(db, tmp_path)
    client = app.test_client()
    r = client.post("/api/storage/masks/delete-inactive")
    assert r.status_code == 200
    body = r.get_json()
    assert body["ok"] is True
    # We seeded 3 rows; both photos have sam2-small active. The only
    # inactive row is photo1's sam2-large.
    assert body["deleted"] == 1
    n = db.conn.execute("SELECT COUNT(*) FROM photo_masks").fetchone()[0]
    assert n == 2


def test_api_storage_masks_delete_stale(app_and_db, tmp_path):
    app, db = app_and_db
    masks_dir, p1, _p2 = _seed_masks(db, tmp_path)
    # Add a detection that matches sam2-small's prompt for p1, so it's not stale.
    db.conn.execute(
        "INSERT INTO detections(photo_id, detector_model, box_x, box_y, "
        "box_w, box_h, detector_confidence, category) "
        "VALUES (?, 'megadetector-v6', 0, 0, 10, 10, 0.9, 'animal')",
        (p1,),
    )
    db.conn.commit()
    # Add a mask whose prompt does NOT match any detection — stale.
    f = masks_dir / f"{p1}.sam3-small.png"
    f.write_bytes(b"x" * 50)
    db.upsert_photo_mask(
        photo_id=p1, variant="sam3-small", path=str(f),
        detector_model="megadetector-v6",
        prompt_x=999, prompt_y=999, prompt_w=10, prompt_h=10,
    )
    client = app.test_client()
    r = client.post("/api/storage/masks/delete-stale")
    assert r.status_code == 200
    body = r.get_json()
    assert body["ok"] is True
    assert body["deleted"] >= 1
    # The stale sam3-small row is gone.
    n = db.conn.execute(
        "SELECT COUNT(*) FROM photo_masks WHERE variant='sam3-small'"
    ).fetchone()[0]
    assert n == 0


def test_storage_masks_uses_global_threshold_not_active_workspace(
    app_and_db, tmp_path
):
    """``/api/storage/masks`` and ``/api/storage/masks/delete-stale``
    are global-storage endpoints, so their stale set must not depend on
    which workspace is active. Concretely: a permissive workspace
    (detector_confidence=0.1) has a matching low-confidence detection
    keeping the mask fresh; a strict workspace
    (detector_confidence=0.5) would consider that detection invisible
    and the mask stale. Switching to the strict workspace must NOT
    cause the storage endpoint to count or delete this mask, because
    it's still valid under the permissive workspace's settings.
    """
    app, db = app_and_db
    masks_dir = tmp_path / "masks"
    masks_dir.mkdir(exist_ok=True)
    pid = db.conn.execute(
        "SELECT id FROM photos ORDER BY id LIMIT 1"
    ).fetchone()["id"]
    # One detection at 0.3 confidence — visible to a 0.1 floor, hidden
    # by a 0.5 floor.
    db.conn.execute(
        "INSERT INTO detections(photo_id, detector_model, box_x, box_y, "
        "box_w, box_h, detector_confidence, category) "
        "VALUES (?, 'megadetector-v6', 0.10, 0.10, 0.10, 0.10, 0.30, 'animal')",
        (pid,),
    )
    f = masks_dir / f"{pid}.sam2-small.png"
    f.write_bytes(b"x" * 50)
    db.upsert_photo_mask(
        photo_id=pid, variant="sam2-small", path=str(f),
        detector_model="megadetector-v6",
        prompt_x=0.10, prompt_y=0.10, prompt_w=0.10, prompt_h=0.10,
    )
    db.conn.commit()
    permissive = db.create_workspace(
        "permissive", config_overrides={"detector_confidence": 0.1}
    )
    strict = db.create_workspace(
        "strict", config_overrides={"detector_confidence": 0.5}
    )
    # Activate the strict workspace. Pre-fix this would push stale_count
    # to 1 (and delete-stale would delete the mask) because the endpoint
    # used the active workspace's 0.5 floor.
    db.set_active_workspace(strict)
    client = app.test_client()
    r = client.get("/api/storage/masks")
    assert r.status_code == 200
    assert r.get_json()["stale_count"] == 0, (
        "global storage view should not flip stale_count when the active "
        "workspace tightens its detector threshold; another workspace "
        "still considers the mask fresh"
    )
    r = client.post("/api/storage/masks/delete-stale")
    assert r.status_code == 200
    assert r.get_json()["deleted"] == 0
    # Mask + DB row still there.
    assert f.exists()
    n = db.conn.execute(
        "SELECT COUNT(*) FROM photo_masks WHERE photo_id=? AND variant=?",
        (pid, "sam2-small"),
    ).fetchone()[0]
    assert n == 1
    # Symmetry: switching to permissive doesn't change the answer either.
    db.set_active_workspace(permissive)
    r = client.get("/api/storage/masks")
    assert r.get_json()["stale_count"] == 0


def test_regroup_live_returns_per_encounter_trace(tmp_path, monkeypatch):
    """/api/pipeline/regroup-live response should expose the per-cut-point
    trace on each multi-photo encounter so the pipeline-review sidebar can
    render the "how was this encounter formed" panel.
    """
    from datetime import datetime, timedelta

    import numpy as np
    from db import Database
    from dino_embed import embedding_to_blob

    monkeypatch.setenv("HOME", str(tmp_path))
    import config as cfg
    import models
    from app import create_app

    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    monkeypatch.setattr(models, "DEFAULT_MODELS_DIR", str(tmp_path / "vireo-models"))
    monkeypatch.setattr(models, "CONFIG_PATH", str(tmp_path / "models.json"))

    db_path = str(tmp_path / "test.db")
    thumb_dir = str(tmp_path / "thumbs")
    os.makedirs(thumb_dir)

    db = Database(db_path)
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    fid = db.add_folder(str(tmp_path), name="photos")
    db.add_workspace_folder(ws_id, fid)

    base_time = datetime(2026, 3, 20, 10, 0, 0)
    # Two encounters of 3 photos each, separated by 5 minutes (hard time cut).
    for enc_idx in range(2):
        emb_base = np.zeros(768, dtype=np.float32)
        emb_base[enc_idx * 100: enc_idx * 100 + 100] = 1.0
        emb_base = emb_base / np.linalg.norm(emb_base)
        enc_offset = enc_idx * 300
        for i in range(3):
            ts = base_time + timedelta(seconds=enc_offset + i * 2)
            pid = db.add_photo(
                fid, f"enc{enc_idx}_p{i}.jpg", ".jpg", 1000, 1.0,
                timestamp=ts.isoformat(), width=4000, height=3000,
            )
            emb = emb_base + np.random.RandomState(pid).randn(768).astype(np.float32) * 0.01
            emb = emb / np.linalg.norm(emb)
            db.update_photo_pipeline_features(
                pid,
                mask_path=f"/masks/{pid}.png",
                subject_tenengrad=200 + i * 50,
                bg_tenengrad=30 + i * 5,
                crop_complete=0.85 + i * 0.03,
                bg_separation=50.0 - i * 10,
                subject_clip_high=0.01,
                subject_clip_low=0.01,
                subject_y_median=120.0,
                phash_crop=f"{pid:016x}",
            )
            db.update_photo_embeddings(
                pid,
                dino_subject_embedding=embedding_to_blob(emb),
                dino_global_embedding=embedding_to_blob(emb),
            )
            db.update_photo_quality(pid, subject_size=0.08 + i * 0.02)
            det_ids = db.save_detections(pid, [
                {"box": {"x": 0.2, "y": 0.2, "w": 0.4, "h": 0.4}, "confidence": 0.9},
            ], detector_model="megadetector")
            species = "robin" if enc_idx == 0 else "eagle"
            db.add_prediction(det_ids[0], species, 0.9 - i * 0.05, "bioclip", category="match")
    db.close()

    app = create_app(db_path=db_path, thumb_cache_dir=thumb_dir, api_token="t")
    client = app.test_client()

    resp = client.post("/api/pipeline/regroup-live", json={"config": {}})
    assert resp.status_code == 200, resp.get_json()
    data = resp.get_json()
    assert "encounters" in data
    assert len(data["encounters"]) >= 1, data
    multi_photo = [e for e in data["encounters"] if e["photo_count"] >= 2]
    assert multi_photo, "test setup should yield at least one multi-photo encounter"
    for enc in multi_photo:
        assert "trace" in enc, f"trace missing on encounter: {enc.keys()}"
        assert len(enc["trace"]) == enc["photo_count"] - 1, (
            f"trace len {len(enc['trace'])} != photo_count-1 {enc['photo_count']-1}"
        )
        sample = enc["trace"][0]
        assert "score" in sample
        assert "decision" in sample
        assert "components" in sample


def test_regroup_live_scopes_to_collection(tmp_path, monkeypatch):
    """/api/pipeline/regroup-live with collection_id must:

    1. Only score photos in the collection — the response's photos and
       encounters reference only that subset.
    2. Reject invalid collection_id values.
    3. Not clobber the workspace-wide cached pipeline state, so the
       pipeline-review page keeps seeing its own results after Cull
       runs a scoped analysis.
    """
    from datetime import datetime, timedelta

    import numpy as np
    from db import Database
    from dino_embed import embedding_to_blob

    monkeypatch.setenv("HOME", str(tmp_path))
    import config as cfg
    import models
    from app import create_app
    from pipeline import load_results_raw

    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    monkeypatch.setattr(models, "DEFAULT_MODELS_DIR", str(tmp_path / "vireo-models"))
    monkeypatch.setattr(models, "CONFIG_PATH", str(tmp_path / "models.json"))

    db_path = str(tmp_path / "test.db")
    thumb_dir = str(tmp_path / "thumbs")
    os.makedirs(thumb_dir)

    db = Database(db_path)
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    fid = db.add_folder(str(tmp_path), name="photos")
    db.add_workspace_folder(ws_id, fid)

    base_time = datetime(2026, 3, 20, 10, 0, 0)
    collection_pids = []
    other_pids = []
    for enc_idx in range(2):
        emb_base = np.zeros(768, dtype=np.float32)
        emb_base[enc_idx * 100: enc_idx * 100 + 100] = 1.0
        emb_base = emb_base / np.linalg.norm(emb_base)
        enc_offset = enc_idx * 300
        for i in range(3):
            ts = base_time + timedelta(seconds=enc_offset + i * 2)
            pid = db.add_photo(
                fid, f"enc{enc_idx}_p{i}.jpg", ".jpg", 1000, 1.0,
                timestamp=ts.isoformat(), width=4000, height=3000,
            )
            emb = emb_base + np.random.RandomState(pid).randn(768).astype(np.float32) * 0.01
            emb = emb / np.linalg.norm(emb)
            db.update_photo_pipeline_features(
                pid,
                mask_path=f"/masks/{pid}.png",
                subject_tenengrad=200 + i * 50,
                bg_tenengrad=30 + i * 5,
                crop_complete=0.85 + i * 0.03,
                bg_separation=50.0 - i * 10,
                subject_clip_high=0.01,
                subject_clip_low=0.01,
                subject_y_median=120.0,
                phash_crop=f"{pid:016x}",
            )
            db.update_photo_embeddings(
                pid,
                dino_subject_embedding=embedding_to_blob(emb),
                dino_global_embedding=embedding_to_blob(emb),
            )
            db.update_photo_quality(pid, subject_size=0.08 + i * 0.02)
            det_ids = db.save_detections(pid, [
                {"box": {"x": 0.2, "y": 0.2, "w": 0.4, "h": 0.4}, "confidence": 0.9},
            ], detector_model="megadetector")
            species = "robin" if enc_idx == 0 else "eagle"
            db.add_prediction(det_ids[0], species, 0.9 - i * 0.05, "bioclip", category="match")
            (collection_pids if enc_idx == 0 else other_pids).append(pid)
    db.set_photo_edit_recipe(collection_pids[0], {"rotation": 90})

    import json as _json
    cid = db.add_collection(
        "robins-only",
        _json.dumps([{"field": "photo_ids", "value": collection_pids}]),
    )
    db.close()

    app = create_app(db_path=db_path, thumb_cache_dir=thumb_dir, api_token="t")
    client = app.test_client()

    # First, a workspace-wide run primes the workspace cache.
    resp = client.post("/api/pipeline/regroup-live", json={"config": {}})
    assert resp.status_code == 200, resp.get_json()
    full = resp.get_json()
    full_photo_ids = {p["id"] for p in full["photos"]}
    assert collection_pids[0] in full_photo_ids
    assert other_pids[0] in full_photo_ids

    cache_dir = os.path.dirname(db_path)
    cached_before = load_results_raw(cache_dir, ws_id)
    assert cached_before is not None
    cached_before_pids = {p["id"] for p in cached_before["photos"]}

    # Scoped run — only the collection's photos should appear.
    resp = client.post(
        "/api/pipeline/regroup-live",
        json={"config": {}, "collection_id": cid},
    )
    assert resp.status_code == 200, resp.get_json()
    scoped = resp.get_json()
    scoped_pids = {p["id"] for p in scoped["photos"]}
    assert scoped_pids == set(collection_pids), (
        f"scoped response leaked non-collection photos: "
        f"{scoped_pids ^ set(collection_pids)}"
    )
    scoped_recipes = {p["id"]: p.get("edit_recipe") for p in scoped["photos"]}
    assert scoped_recipes[collection_pids[0]] == {"version": 1, "rotation": 90}
    for enc in scoped["encounters"]:
        for pid in enc["photo_ids"]:
            assert pid in collection_pids, (
                f"encounter contains out-of-scope photo {pid}"
            )

    # Workspace cache must be untouched — pipeline-review should still
    # see the workspace-wide results, not the Cull-scoped subset.
    cached_after = load_results_raw(cache_dir, ws_id)
    cached_after_pids = {p["id"] for p in cached_after["photos"]}
    assert cached_after_pids == cached_before_pids, (
        "scoped regroup-live clobbered the workspace pipeline cache"
    )

    # Reflow with collection_id behaves the same.
    resp = client.post(
        "/api/pipeline/reflow",
        json={"config": {}, "collection_id": cid},
    )
    assert resp.status_code == 200, resp.get_json()
    reflow = resp.get_json()
    reflow_pids = {p["id"] for p in reflow["photos"]}
    assert reflow_pids == set(collection_pids)
    reflow_recipes = {p["id"]: p.get("edit_recipe") for p in reflow["photos"]}
    assert reflow_recipes[collection_pids[0]] == {"version": 1, "rotation": 90}
    cached_after_reflow = load_results_raw(cache_dir, ws_id)
    assert {p["id"] for p in cached_after_reflow["photos"]} == cached_before_pids

    # Validation: non-numeric collection_id is rejected.
    resp = client.post(
        "/api/pipeline/regroup-live",
        json={"config": {}, "collection_id": "not-a-number"},
    )
    assert resp.status_code == 400
    resp = client.post(
        "/api/pipeline/reflow",
        json={"config": {}, "collection_id": True},
    )
    assert resp.status_code == 400


def test_save_grouping_defaults_persists_to_config(tmp_path, monkeypatch):
    """POST /api/pipeline/save-grouping-defaults should persist whitelisted
    grouping keys into the global config file via cfg.save()."""
    monkeypatch.setenv("HOME", str(tmp_path))
    import config as cfg
    import models
    from app import create_app

    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    monkeypatch.setattr(models, "DEFAULT_MODELS_DIR", str(tmp_path / "vireo-models"))
    monkeypatch.setattr(models, "CONFIG_PATH", str(tmp_path / "models.json"))

    db_path = str(tmp_path / "test.db")
    thumb_dir = str(tmp_path / "thumbs")
    os.makedirs(thumb_dir)

    app = create_app(db_path=db_path, thumb_cache_dir=thumb_dir, api_token="t")
    client = app.test_client()

    payload = {"pipeline": {
        "w_species": 0.40,
        "hard_cut_score": 0.55,
        "species_hard_cut_confidence": 0.85,
        "species_hard_cut_margin": 0.65,
        "tau_enc": 30.0,
    }}
    resp = client.post("/api/pipeline/save-grouping-defaults", json=payload)
    assert resp.status_code == 200, resp.get_json()
    body = resp.get_json()
    assert body.get("saved") == payload["pipeline"]

    saved = cfg.load()
    assert saved["pipeline"]["w_species"] == 0.40
    assert saved["pipeline"]["hard_cut_score"] == 0.55
    assert saved["pipeline"]["species_hard_cut_confidence"] == 0.85
    assert saved["pipeline"]["species_hard_cut_margin"] == 0.65
    assert saved["pipeline"]["tau_enc"] == 30.0


def test_save_grouping_defaults_recovers_from_corrupt_pipeline_section(tmp_path, monkeypatch):
    """If a hand-edit left config.json with a non-dict pipeline value, the
    endpoint should overwrite it cleanly rather than crash with AttributeError
    inside .update()."""
    import json as _json

    monkeypatch.setenv("HOME", str(tmp_path))
    import config as cfg
    import models
    from app import create_app

    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    monkeypatch.setattr(models, "DEFAULT_MODELS_DIR", str(tmp_path / "vireo-models"))
    monkeypatch.setattr(models, "CONFIG_PATH", str(tmp_path / "models.json"))

    # Pre-seed config.json with a corrupt pipeline value (string, not dict).
    with open(cfg.CONFIG_PATH, "w") as f:
        _json.dump({"pipeline": "oops-i-edited-this-by-hand"}, f)

    db_path = str(tmp_path / "test.db")
    thumb_dir = str(tmp_path / "thumbs")
    os.makedirs(thumb_dir)
    app = create_app(db_path=db_path, thumb_cache_dir=thumb_dir, api_token="t")
    client = app.test_client()

    resp = client.post("/api/pipeline/save-grouping-defaults",
                       json={"pipeline": {"w_species": 0.40}})
    assert resp.status_code == 200, resp.get_json()
    saved = cfg.load()
    assert saved["pipeline"]["w_species"] == 0.40


def test_save_grouping_defaults_rejects_bad_values(tmp_path, monkeypatch):
    """POST /api/pipeline/save-grouping-defaults must reject invalid types or
    out-of-range values before they corrupt the persistent config."""
    monkeypatch.setenv("HOME", str(tmp_path))
    import config as cfg
    import models
    from app import create_app

    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    monkeypatch.setattr(models, "DEFAULT_MODELS_DIR", str(tmp_path / "vireo-models"))
    monkeypatch.setattr(models, "CONFIG_PATH", str(tmp_path / "models.json"))

    db_path = str(tmp_path / "test.db")
    thumb_dir = str(tmp_path / "thumbs")
    os.makedirs(thumb_dir)

    app = create_app(db_path=db_path, thumb_cache_dir=thumb_dir, api_token="t")
    client = app.test_client()

    # Wrong type for a numeric field.
    resp = client.post(
        "/api/pipeline/save-grouping-defaults",
        json={"pipeline": {"hard_cut_time": "abc"}},
    )
    assert resp.status_code == 400
    # Out-of-range weight (must be 0..1).
    resp = client.post(
        "/api/pipeline/save-grouping-defaults",
        json={"pipeline": {"w_species": 1.5}},
    )
    assert resp.status_code == 400
    # Negative threshold.
    resp = client.post(
        "/api/pipeline/save-grouping-defaults",
        json={"pipeline": {"hard_cut_score": -0.1}},
    )
    assert resp.status_code == 400
    # bool should not satisfy "must be a number" silently.
    resp = client.post(
        "/api/pipeline/save-grouping-defaults",
        json={"pipeline": {"w_time": True}},
    )
    assert resp.status_code == 400
    # NaN / inf must be rejected.
    resp = client.post(
        "/api/pipeline/save-grouping-defaults",
        json={"pipeline": {"tau_enc": float("inf")}},
    )
    assert resp.status_code == 400
    # Zero for tau constants would crash encounters.sim_time
    # (exp(-dt/tau) divides by tau) — must be rejected.
    resp = client.post(
        "/api/pipeline/save-grouping-defaults",
        json={"pipeline": {"tau_enc": 0.0}},
    )
    assert resp.status_code == 400
    resp = client.post(
        "/api/pipeline/save-grouping-defaults",
        json={"pipeline": {"merge_tau": 0.0}},
    )
    assert resp.status_code == 400
    # Unknown keys (e.g. removed thresholds) must be rejected.
    resp = client.post(
        "/api/pipeline/save-grouping-defaults",
        json={"pipeline": {"burst_phash_threshold": 12}},
    )
    assert resp.status_code == 400

    # No bad payload should have been written to disk — for any key we
    # attempted to corrupt, the persisted value is still a valid number
    # (either the default fell through, or the load merge filled it).
    saved = cfg.load()
    pipe = saved.get("pipeline", {})
    if "hard_cut_time" in pipe:
        assert isinstance(pipe["hard_cut_time"], int | float)
    if "w_species" in pipe:
        assert isinstance(pipe["w_species"], int | float)
        assert 0.0 <= pipe["w_species"] <= 1.0
    if "hard_cut_score" in pipe:
        assert isinstance(pipe["hard_cut_score"], int | float)
        assert 0.0 <= pipe["hard_cut_score"] <= 1.0
    if "w_time" in pipe:
        assert isinstance(pipe["w_time"], int | float)
        assert pipe["w_time"] is not True  # bool guard
    if "tau_enc" in pipe:
        import math as _math
        assert isinstance(pipe["tau_enc"], int | float)
        assert _math.isfinite(pipe["tau_enc"])


def test_collections_list_survives_one_unresolvable_rule(app_and_db):
    """A single collection whose rule can't be resolved must not 500 the
    whole /api/collections list. Before this, one bad rule raised an
    unhandled ValueError, the endpoint 500'd, and every collection
    dropdown in the UI came back empty. The bad collection should degrade
    to photo_count=None with count_error=True; the others still count.
    """
    import json
    app, db = app_and_db
    client = app.test_client()

    good = db.add_collection(
        "Rating 5", json.dumps([{"field": "rating", "op": ">=", "value": 5}])
    )
    bad = db.add_collection(
        "Broken", json.dumps([{"field": "nonexistent_field", "op": "is", "value": 1}])
    )

    resp = client.get("/api/collections")
    assert resp.status_code == 200
    by_id = {c["id"]: c for c in resp.get_json()}

    assert by_id[good]["photo_count"] == 1
    assert "count_error" not in by_id[good]

    assert by_id[bad]["photo_count"] is None
    assert by_id[bad]["count_error"] is True


def test_browse_init_flags_degraded_without_counting(app_and_db, monkeypatch):
    """/api/browse/init must mark collections with unresolvable rules as
    degraded (count_error=True) so the sidebar first paint disables them —
    but it must NOT run COUNT(DISTINCT p.id) per collection to figure that
    out. That N+1 is what the async loadCollectionCounts() in
    bootstrapBrowse() was designed to avoid, and re-adding it to the
    critical first-paint path makes Browse wait on every smart-collection
    query.
    """
    import json
    import sqlite3

    from db import Database

    app, db = app_and_db
    client = app.test_client()

    good = db.add_collection(
        "Rating 5", json.dumps([{"field": "rating", "op": ">=", "value": 5}])
    )
    bad = db.add_collection(
        "Broken", json.dumps([{"field": "nonexistent_field", "op": "is", "value": 1}])
    )

    # If browse init still ran a full count per collection, this monkeypatch
    # would blow up the request. The endpoint must derive the count_error
    # flag from rule validation alone, with actual counts left to the
    # client's async /api/collections call.
    def boom(self, _cid):
        raise sqlite3.OperationalError("count_collection_photos should not run on browse init")

    monkeypatch.setattr(Database, "count_collection_photos", boom)

    resp = client.get("/api/browse/init")
    assert resp.status_code == 200
    by_id = {c["id"]: c for c in resp.get_json()["collections"]}

    # The healthy collection is neither degraded nor eagerly counted.
    assert by_id[good].get("count_error") in (None, False)
    assert "photo_count" not in by_id[good]

    # The broken one is still flagged so the sidebar renders it disabled
    # before loadCollectionCounts() has a chance to run.
    assert by_id[bad]["count_error"] is True


def test_collections_list_surfaces_non_rule_failures(app_and_db, monkeypatch):
    """The count_error path is for rule-validation failures only. If
    count_collection_photos raises a genuine infrastructure error (locked or
    corrupt DB, bad generated query), /api/collections must NOT silently
    downgrade it to a count_error row — the pickers would then tell the user
    to fix the rule when the real problem is DB-side. Non-ValueError errors
    must bubble up so the 5xx surfaces where a human will see it.
    """
    import json
    import sqlite3

    from db import Database

    app, db = app_and_db
    client = app.test_client()

    db.add_collection(
        "Rating 5", json.dumps([{"field": "rating", "op": ">=", "value": 5}])
    )

    # Patch the class so the per-request Database instance built by
    # _get_db() is affected too — the route does not share the fixture's
    # instance.
    def boom(self, _cid):
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(Database, "count_collection_photos", boom)

    resp = client.get("/api/collections")
    # Not a 200 with count_error — a real 5xx so the incident is visible.
    assert resp.status_code >= 500


def test_collection_photos_returns_400_for_unresolvable_rule(app_and_db):
    """When a collection's rules can't be resolved, /photos, /photo-ids and
    /api/import/collection-preview must return a 400, not 500. Otherwise the
    pipeline picker (which just shows every collection from /api/collections)
    would advertise a source whose downstream endpoints crash the moment a
    user selects it — leaving the UI stuck.
    """
    import json
    app, db = app_and_db
    client = app.test_client()

    bad = db.add_collection(
        "Broken", json.dumps([{"field": "nonexistent_field", "op": "is", "value": 1}])
    )

    resp = client.get(f"/api/collections/{bad}/photos")
    assert resp.status_code == 400
    assert "error" in resp.get_json()

    resp = client.get(f"/api/collections/{bad}/photo-ids")
    assert resp.status_code == 400
    assert "error" in resp.get_json()

    resp = client.post(
        "/api/import/collection-preview",
        json={"collection_id": bad},
    )
    assert resp.status_code == 400
    assert "error" in resp.get_json()


def test_pipeline_picker_disables_degraded_collections(app_and_db):
    """The pipeline page's collection picker must not offer degraded
    collections as selectable — count_error entries render with a `disabled`
    attribute so users can't accidentally pick a source that would 500 the
    downstream /photos endpoint.
    """
    app, _db = app_and_db
    client = app.test_client()
    html = client.get("/pipeline").get_data(as_text=True)
    # The renderer keys off c.count_error and adds ' disabled' to the option
    # (plus a tooltip explaining why it's unavailable). Assert the branch is
    # actually in the template rather than probing it from the DOM.
    assert "c.count_error" in html
    assert "unavailable" in html


def test_collection_pickers_disable_degraded_collections(app_and_db):
    """Every page that renders a collection picker from /api/collections must
    honor count_error. Before this fix only the pipeline picker did — the
    review, cull, compare, pipeline-review, and browse pickers still appended
    every collection as selectable, so picking a broken one 400'd the
    downstream request. Regression guard: the same count_error /
    'unavailable' branch that exists on the pipeline page must exist on each
    of these pages too.
    """
    app, _db = app_and_db
    client = app.test_client()
    for route in ("/review", "/cull", "/compare", "/pipeline/review", "/browse"):
        html = client.get(route).get_data(as_text=True)
        assert "count_error" in html, (
            f"{route} does not check count_error on its collection picker"
        )
        assert "unavailable" in html, (
            f"{route} does not label degraded collections as unavailable"
        )


def test_degraded_collections_never_advertise_manual_add(app_and_db, monkeypatch):
    """A count_error collection must not report can_add_photos=True from
    either /api/collections or /api/browse/init. The add-to-collection
    modal filters only on can_add_photos, and /api/collections/<id>/add-photos
    reaches set(ids_rule["value"]) — which 500s on any malformed photo_ids
    payload. Defense-in-depth alongside the picker guards: degraded rows
    are surfaced (so the user can edit them) but never offered as an
    append target.
    """
    import json

    from db import Database

    app, db = app_and_db
    client = app.test_client()

    # A static photo_ids collection whose count query fails at rule-resolve
    # time: the shape looks like a manual-add target, but the DB can't
    # count it, so it lands in the count_error path.
    bad = db.add_collection(
        "Broken static",
        json.dumps([
            {"field": "photo_ids", "value": [1, 2, 3]},
            {"field": "nonexistent_field", "op": "is", "value": 1},
        ]),
    )

    resp = client.get("/api/collections")
    assert resp.status_code == 200
    by_id = {c["id"]: c for c in resp.get_json()}
    assert by_id[bad]["count_error"] is True
    assert by_id[bad]["can_add_photos"] is False, (
        "degraded /api/collections row must not advertise manual-add support"
    )

    # /api/browse/init derives count_error from cheap rule validation, so
    # patch that instead of count_collection_photos to trigger the branch.
    monkeypatch.setattr(Database, "rules_resolvable", lambda self, rules: False)

    resp = client.get("/api/browse/init")
    assert resp.status_code == 200
    by_id = {c["id"]: c for c in resp.get_json()["collections"]}
    assert by_id[bad]["count_error"] is True
    assert by_id[bad]["can_add_photos"] is False, (
        "degraded /api/browse/init row must not advertise manual-add support"
    )


def test_browse_filter_by_collection_guards_degraded_rows():
    """The Browse sidebar renders every collection from /api/collections as a
    clickable filter target (filterByCollection). Before this fix,
    left-clicking a degraded row hit /api/collections/<id>/photos, which now
    400s, leaving Browse advertising a source that can't load. Regression
    guard: filterByCollection must bail out at the top for count_error rows
    (with a toast) rather than firing the request.
    """
    from pathlib import Path
    src = Path(__file__).parent.parent / "templates" / "browse.html"
    text = src.read_text(encoding="utf-8")
    fn_start = text.find("async function filterByCollection")
    assert fn_start != -1, "filterByCollection function not found"
    # Grab enough of the function body to include the guard block. The guard
    # must reference count_error and return before the normal load path runs.
    body = text[fn_start:fn_start + 2000]
    assert "count_error" in body, (
        "browse.html filterByCollection does not check count_error"
    )
    guard_end = body.find("return;")
    fetch_start = body.find("loadPhotos")
    assert guard_end != -1 and fetch_start != -1 and guard_end < fetch_start, (
        "browse.html filterByCollection does not early-return before loading"
    )


def test_review_switch_collection_does_not_silently_widen_scope():
    """When /api/collections/<id>/photos fails, the review page must not fall
    back to `allPredictions.slice()` — that silently widened the scope back
    to every prediction, the opposite of what the user asked for. Regression
    guard on the template source itself.
    """
    from pathlib import Path
    src = Path(__file__).parent.parent / "templates" / "review.html"
    text = src.read_text(encoding="utf-8")
    # Locate the switchCollection function and the catch branch inside it.
    fn_start = text.find("async function switchCollection")
    assert fn_start != -1, "switchCollection function not found"
    fn_end = text.find("\n}", fn_start)
    assert fn_end != -1
    body = text[fn_start:fn_end]
    # The old silent fallback assigned allPredictions.slice() from the catch;
    # the new behavior keeps the scope empty and surfaces a toast.
    assert "predictions = allPredictions.slice()" not in body.split("catch")[1], (
        "review.html still silently widens scope on collection load failure"
    )
    assert "predictions = []" in body
    assert "showToast" in body


def test_collection_preview_returns_match_count(app_and_db):
    """POST /api/collections/preview returns the count of photos that
    would match an unsaved rules list. Powers the smart-collection
    modal's live "Matches: N photos" readout.
    """
    app, db = app_and_db
    client = app.test_client()
    collections_before = len(db.get_collections())

    # The fixture creates 3 photos; p1 has rating=3, p3 has rating=5.
    # Empty rules -> all photos in workspace.
    resp = client.post("/api/collections/preview", json={"rules": []})
    assert resp.status_code == 200
    assert resp.get_json()["count"] == 3

    # rating >= 4 -> only p3 (rating=5).
    resp = client.post(
        "/api/collections/preview",
        json={"rules": [{"field": "rating", "op": ">=", "value": 4}]},
    )
    assert resp.status_code == 200
    assert resp.get_json()["count"] == 1

    # No collection row was persisted as a side effect of previewing.
    assert len(db.get_collections()) == collections_before


def test_collection_preview_rejects_malformed_rules(app_and_db):
    """Malformed rules return 400, not 500 — the route must not crash on
    untrusted input from the modal.
    """
    app, _db = app_and_db
    client = app.test_client()

    resp = client.post("/api/collections/preview", json={"rules": "not a list"})
    assert resp.status_code == 400
    assert "error" in resp.get_json()

    resp = client.post(
        "/api/collections/preview",
        json={"rules": [{"op": "is", "value": 5}]},  # missing 'field'
    )
    assert resp.status_code == 400

    # A value that SQLite can't bind as a parameter (e.g. a nested object)
    # must also surface as 400 rather than a 500 from sqlite3.InterfaceError.
    resp = client.post(
        "/api/collections/preview",
        json={"rules": [{"field": "rating", "op": ">=", "value": {"x": 1}}]},
    )
    assert resp.status_code == 400
    assert "error" in resp.get_json()

    # Scalar-only fields must reject list values up front; otherwise SQLite
    # raises ProgrammingError at bind time and the route would 500.
    for field, op in [("rating", ">="), ("flag", "is"),
                      ("extension", "is"), ("color_label", "is")]:
        resp = client.post(
            "/api/collections/preview",
            json={"rules": [{"field": field, "op": op, "value": [1]}]},
        )
        assert resp.status_code == 400, (
            f"expected 400 for list value on {field!r}/{op!r}"
        )
        assert "error" in resp.get_json()

    # Top-level JSON that isn't an object (list, number, string) must also
    # return 400 — the route reads `body.get("rules", ...)`, which would
    # otherwise raise AttributeError and surface as a 500.
    for bad_body in ([], [1, 2, 3], 5, "hello"):
        resp = client.post(
            "/api/collections/preview",
            json=bad_body,
        )
        assert resp.status_code == 400, f"expected 400 for body {bad_body!r}"
        assert "error" in resp.get_json()


def test_collection_preview_rejects_invalid_json(app_and_db):
    """Syntactically broken JSON returns 400 — silent=True would otherwise
    coerce it to None and the route would happily return a 200 match count
    for a request the client never actually made successfully.
    """
    app, _db = app_and_db
    client = app.test_client()

    resp = client.post(
        "/api/collections/preview",
        data="{not valid json",
        content_type="application/json",
    )
    assert resp.status_code == 400
    assert "error" in resp.get_json()

    # An empty body, however, is still valid — equivalent to {} — and should
    # match every photo in the workspace (the same behavior as {"rules": []}).
    resp = client.post(
        "/api/collections/preview",
        data="",
        content_type="application/json",
    )
    assert resp.status_code == 200
    assert "count" in resp.get_json()


def test_collection_preview_does_not_mask_db_failures(app_and_db, monkeypatch):
    """Real backend faults from sqlite3 must surface as 5xx — not be
    rewritten as 400 by the route's exception handler. Catching the base
    sqlite3.Error here would hide locked-DB / OperationalError incidents.
    """
    import sqlite3

    from db import Database

    app, _db = app_and_db
    client = app.test_client()

    # Patch the class so the per-request Database instance built by
    # _get_db() is affected too — the route does not share the fixture's
    # instance.
    def boom(self, _rules):
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(Database, "count_photos_for_rules", boom)

    resp = client.post("/api/collections/preview", json={"rules": []})
    # Flask's default for an unhandled exception is 500; the important thing
    # is that it's *not* a 400, which would mislabel a backend fault as a
    # client error.
    assert resp.status_code >= 500


# -- Regression tests: route hardening (workspace verification, validation,
# config locking) --


def test_update_workspace_unknown_id_returns_404(app_and_db):
    """PUT /api/workspaces/<id> must 404 for an unknown workspace instead of
    crashing on dict(None) after update_workspace silently no-ops."""
    app, _db = app_and_db
    client = app.test_client()
    resp = client.put('/api/workspaces/999999', json={"name": "ghost"})
    assert resp.status_code == 404
    assert "error" in resp.get_json()


def test_update_workspace_rejects_non_dict_config_overrides(app_and_db):
    """PUT /api/workspaces/<id> must reject non-dict config_overrides —
    persisting e.g. a list would crash the labels accessors later."""
    app, db = app_and_db
    client = app.test_client()
    ws_id = db._active_workspace_id

    for bad in ([], "x", 5, True):
        resp = client.put(f'/api/workspaces/{ws_id}',
                          json={"config_overrides": bad})
        assert resp.status_code == 400, f"expected 400 for {bad!r}"

    # None (clear) and a dict remain accepted.
    resp = client.put(f'/api/workspaces/{ws_id}',
                      json={"config_overrides": {"classification_threshold": 0.5}})
    assert resp.status_code == 200
    resp = client.put(f'/api/workspaces/{ws_id}', json={"config_overrides": None})
    assert resp.status_code == 200


def test_add_workspace_folder_unknown_ids_return_404(app_and_db):
    """POST /api/workspaces/<ws>/folders must 404 for unknown workspace or
    folder ids instead of hitting the workspace_folders FK and 500ing."""
    app, db = app_and_db
    client = app.test_client()
    ws_id = db._active_workspace_id
    fid = db.get_folder_tree()[0]["id"]

    resp = client.post(f'/api/workspaces/{ws_id}/folders',
                       json={"folder_id": 999999})
    assert resp.status_code == 404
    assert "Folder" in resp.get_json()["error"]

    resp = client.post('/api/workspaces/999999/folders',
                       json={"folder_id": fid})
    assert resp.status_code == 404
    assert "Workspace" in resp.get_json()["error"]


def test_setup_complete_does_not_pin_defaults(app_and_db):
    """POST /api/setup/complete must persist only setup_complete (plus keys
    the user already set) — not the full DEFAULTS-merged config."""
    import json as _json

    import config as cfg

    app, _db = app_and_db
    with open(cfg.CONFIG_PATH, "w") as f:
        _json.dump({"inat_token": "abc"}, f)

    client = app.test_client()
    resp = client.post('/api/setup/complete')
    assert resp.status_code == 200

    with open(cfg.CONFIG_PATH) as f:
        raw = _json.load(f)
    assert raw == {"inat_token": "abc", "setup_complete": True}


def test_detach_endpoints_reject_non_integer_indices(app_and_db):
    """detach-burst/detach-photo must 400 on non-integer indices instead of
    raising TypeError at the `enc_idx < 0` comparison."""
    app, _db = app_and_db
    client = app.test_client()

    resp = client.post('/api/pipeline/detach-burst',
                       json={"encounter_index": "0", "burst_index": 0})
    assert resp.status_code == 400
    assert "encounter_index" in resp.get_json()["error"]

    resp = client.post('/api/pipeline/detach-burst',
                       json={"encounter_index": 0, "burst_index": [1]})
    assert resp.status_code == 400
    assert "burst_index" in resp.get_json()["error"]

    resp = client.post('/api/pipeline/detach-photo',
                       json={"encounter_index": "0", "burst_index": 0,
                             "photo_id": 1})
    assert resp.status_code == 400
    assert "encounter_index" in resp.get_json()["error"]

    resp = client.post('/api/pipeline/detach-photo',
                       json={"encounter_index": 0, "burst_index": True,
                             "photo_id": 1})
    assert resp.status_code == 400
    assert "burst_index" in resp.get_json()["error"]


def test_sync_discard_reports_true_count(app_and_db):
    """POST /api/sync/discard must report rows actually deleted, not the
    request size — stale/foreign ids are silently skipped by clear_pending."""
    app, db = app_and_db
    client = app.test_client()
    pid = db.conn.execute("SELECT id FROM photos LIMIT 1").fetchone()["id"]
    db.queue_change(pid, "keyword_add", "Test Bird")
    change_id = db.get_pending_changes()[0]["id"]

    resp = client.post('/api/sync/discard',
                       json={"change_ids": [change_id, 999999]})
    assert resp.status_code == 200
    assert resp.get_json()["discarded"] == 1
    assert db.get_pending_changes() == []


def test_audit_resolve_validates_direction_and_photo_id(app_and_db):
    """POST /api/audit/resolve must 400 on unknown directions (resolve_drift
    silently no-ops on them) and non-integer photo_id."""
    app, db = app_and_db
    client = app.test_client()
    pid = db.conn.execute("SELECT id FROM photos LIMIT 1").fetchone()["id"]

    resp = client.post('/api/audit/resolve',
                       json={"photo_id": pid, "direction": "use_database"})
    assert resp.status_code == 400

    resp = client.post('/api/audit/resolve',
                       json={"photo_id": "abc", "direction": "use_db"})
    assert resp.status_code == 400

    resp = client.post('/api/audit/resolve',
                       json={"photo_id": pid, "direction": "use_db"})
    assert resp.status_code == 200

    # resolve-all shares the same silent no-op hazard.
    resp = client.post('/api/audit/resolve-all', json={"direction": "bogus"})
    assert resp.status_code == 400


def test_encounter_species_chunks_large_photo_id_lists(app_and_db):
    """POST /api/encounters/species must chunk its IN-clause queries so id
    lists beyond the SQLite bound-parameter cap don't raise OperationalError."""
    app, db = app_and_db
    client = app.test_client()
    fid = db.get_folder_tree()[0]["id"]
    # More ids than one 900-param chunk so the query must split.
    photo_ids = [
        db.add_photo(folder_id=fid, filename=f"chunk{i}.jpg", extension=".jpg",
                     file_size=1, file_mtime=1.0)
        for i in range(950)
    ]

    resp = client.post('/api/encounters/species',
                       json={"species": "Chunk Finch", "photo_ids": photo_ids})
    assert resp.status_code == 200
    assert resp.get_json()["photo_count"] == len(photo_ids)
    # Both chunks were validated and tagged.
    for pid in (photo_ids[0], photo_ids[-1]):
        assert any(t["name"] == "Chunk Finch" for t in db.get_photo_keywords(pid))


def test_api_exiftool_status_reports_missing(app_and_db, monkeypatch):
    """/api/exiftool/status surfaces a missing binary with an install hint."""
    app, db = app_and_db
    import metadata
    monkeypatch.setattr(metadata.shutil, "which", lambda name: None)

    client = app.test_client()
    resp = client.get('/api/exiftool/status')
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["available"] is False
    assert data["path"] == ""
    assert data["hint"]


def test_api_exiftool_status_reports_present(app_and_db, monkeypatch):
    """/api/exiftool/status reports the resolved path and version when found."""
    app, db = app_and_db
    import metadata
    monkeypatch.setattr(metadata.shutil, "which", lambda name: "/usr/bin/exiftool")

    class _Result:
        returncode = 0
        stdout = "12.76\n"

    monkeypatch.setattr(metadata.subprocess, "run", lambda *a, **k: _Result())

    client = app.test_client()
    resp = client.get('/api/exiftool/status')
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["available"] is True
    assert data["path"] == "/usr/bin/exiftool"
    assert data["version"] == "12.76"


def _touch_jpeg(path):
    """Create a real 1x1 JPEG at ``path`` for endpoint-level scan tests."""
    from PIL import Image
    os.makedirs(os.path.dirname(path), exist_ok=True)
    Image.new("RGB", (1, 1), "white").save(path, "JPEG")


def test_api_audit_import_untracked_warns_when_exiftool_missing(
    app_and_db, tmp_path, monkeypatch,
):
    """/api/audit/import-untracked surfaces the degraded-scan warning when
    ExifTool is unavailable. Without it the UI silently refreshes after a
    scan that lost capture date / GPS / camera info."""
    app, _db = app_and_db
    import metadata
    monkeypatch.setattr(metadata, "exiftool_available", lambda: False)

    img_path = tmp_path / "shoot" / "IMG_0001.JPG"
    _touch_jpeg(str(img_path))

    client = app.test_client()
    resp = client.post(
        "/api/audit/import-untracked",
        json={"paths": [str(img_path)]},
    )
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["ok"] is True
    assert data["imported"] == 1
    assert "warning" in data
    assert "ExifTool" in data["warning"]


def test_api_audit_import_untracked_silent_when_exiftool_present(
    app_and_db, tmp_path, monkeypatch,
):
    """When ExifTool runs cleanly the endpoint must NOT inject a warning
    field — a stray warning would render as a false-positive toast on every
    audit import."""
    app, _db = app_and_db
    import metadata
    monkeypatch.setattr(metadata, "exiftool_available", lambda: True)

    img_path = tmp_path / "shoot" / "IMG_0001.JPG"
    _touch_jpeg(str(img_path))

    client = app.test_client()
    resp = client.post(
        "/api/audit/import-untracked",
        json={"paths": [str(img_path)]},
    )
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["ok"] is True
    assert data["imported"] == 1
    assert "warning" not in data


# ---------------------------------------------------------------------------
# Import page (import/process split PR 3)
# ---------------------------------------------------------------------------


def test_import_page_returns_200(app_and_db):
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get("/import")
    assert resp.status_code == 200
    html = resp.data.decode()
    assert "navbar" in html
    # Core controls, by id: the import mode radios, the after-import strategy
    # menu (including the import-only null choice), the duplicate preview
    # trigger, the safe-to-format pill container, and the start button posting
    # to the in-place and copy import endpoints.
    assert 'id="modeInPlace"' in html
    assert 'id="modeCopy"' in html
    assert 'id="afterImportSelect"' in html
    assert 'value="__none__"' in html  # "None — import only" option
    assert "/api/import/check-duplicates" in html
    assert "Trust likely duplicates" in html
    assert "Verify duplicates byte-for-byte" in html
    assert "capture time to the second" in html
    assert "res.unverified_duplicates_only" in html
    assert 'id="safeToFormatPill"' in html
    assert "/api/jobs/import-in-place" in html
    assert "/api/jobs/import-photos" in html


def test_import_page_resolves_default_process_client_side(app_and_db):
    """Templates are Jinja-free by convention, so the after-import menu's
    default resolves in page JS from the workspace's config_overrides
    merged over /api/config, and the process options load from
    /api/processes — assert the wiring exists (behavior is covered by the
    user-first scenario)."""
    app, _ = app_and_db
    client = app.test_client()
    html = client.get("/import").data.decode()
    assert "/api/workspaces/active" in html
    assert "default_process_id" in html
    assert "/api/processes" in html
    assert "config_overrides" in html


def test_import_page_offers_common_and_custom_folder_templates(app_and_db):
    """Folder organization is approachable without removing strftime's
    flexibility or breaking custom templates loaded from config."""
    app, _ = app_and_db
    client = app.test_client()
    html = client.get("/import").data.decode()

    assert 'id="folderTemplatePreset"' in html
    assert '%Y/%m/%d — 2026/07/12' in html
    assert '%Y/%Y-%m-%d — 2026/2026-07-12' in html
    assert '<option value="__custom__">Custom…</option>' in html
    assert 'id="folderTemplate"' in html
    assert "function selectedFolderTemplate()" in html
    assert "preset.value = commonOption ? template : '__custom__'" in html


def test_process_page_has_no_import_source(app_and_db):
    """The wizard is the Process page now: the Import Photos radio flow is
    gone (it lives at /import); Folders / Collection / New images scopes
    and the strategy menu are present. A compatibility source-folder
    browse/type control was restored so users can point Process at
    raw folders directly — it is present but does not re-add the full
    Import radio or /api/jobs/import-full entry point."""
    app, _ = app_and_db
    client = app.test_client()
    html = client.get("/pipeline").data.decode()
    assert 'id="radioImport"' not in html
    assert "/api/jobs/import-full" not in html
    assert 'id="radioFolders"' in html
    assert 'id="radioCollection"' in html
    assert 'id="radioNewImages"' in html
    assert 'id="strategySelect"' in html
    assert "folder_ids" in html
    assert "Open Import to add photos" in html
    assert "<title>Vireo - Process</title>" in html


def test_browse_empty_state_import_link_targets_import_page(app_and_db):
    app, _ = app_and_db
    client = app.test_client()
    html = client.get("/browse").data.decode()
    assert 'href="/import"' in html
    assert 'href="/pipeline" style="display:inline-block' not in html


def test_native_import_commands_route_to_import_page():
    import os

    repo_root = os.path.normpath(
        os.path.join(os.path.dirname(__file__), "..", "..")
    )
    navbar_path = os.path.join(repo_root, "vireo", "templates", "_navbar.html")
    menu_path = os.path.join(repo_root, "src-tauri", "src", "menu.rs")

    with open(navbar_path, encoding="utf-8") as f:
        navbar = f.read()
    with open(menu_path, encoding="utf-8") as f:
        menu = f.read()

    assert "ids::NAV_IMPORT => Some(\"/import\")" in menu
    assert "ids::NAV_LIGHTROOM => Some(\"/lightroom\")" in menu
    # File → Import Lightroom Catalog must be a route (not a command) so it
    # still works after Open in Browser — command dispatch is a no-op in
    # browser mode, but route_for_id opens the URL in the user's browser.
    assert "ids::FILE_IMPORT_LIGHTROOM => Some(\"/lightroom\")" in menu
    assert "FILE_IMPORT_LIGHTROOM => Some(\"import_lightroom\")" not in menu
    assert "case 'import_photos':\n        nativeMenuRoute('/import');" in navbar
    assert '"Import Lightroom Catalog..."' in menu
    # Import Folder... must stay a distinct action from Import Photos...:
    # it deep-links into Copy-to-archive with the source picker open.
    assert (
        "nativeMenuRoute('/import?mode=copy&pick=source');" in navbar
    )


def test_native_shell_owns_external_navigation_policy():
    """The native shell must deny external and child-webview navigation."""
    import os

    repo_root = os.path.normpath(
        os.path.join(os.path.dirname(__file__), "..", "..")
    )
    lib_path = os.path.join(repo_root, "src-tauri", "src", "lib.rs")
    config_path = os.path.join(repo_root, "src-tauri", "tauri.conf.json")

    with open(lib_path, encoding="utf-8") as f:
        lib = f.read()
    with open(config_path, encoding="utf-8") as f:
        config = f.read()

    assert '"create": false' in config
    assert ".on_navigation(move |url|" in lib
    assert "navigation::handle_navigation(&navigation_app, url)" in lib
    assert ".on_new_window(" in lib
    assert "NewWindowResponse::Deny" in lib
    assert ".on_download(" in lib
    assert "handle_download(&download_app, event)" in lib


def test_pipeline_plan_accepts_folder_scope(app_and_db):
    """The Process page's folder scope must produce truthful readiness
    pills: the plan is computed over the folders' subtree photos, not the
    whole workspace (CORE_PHILOSOPHY: no cheaper-proxy counts)."""
    app, db = app_and_db
    root = db.conn.execute(
        "SELECT id FROM folders WHERE path = '/photos/2024'"
    ).fetchone()["id"]
    client = app.test_client()
    resp = client.post("/api/pipeline/plan", json={"folder_ids": [root]})
    assert resp.status_code == 200
    scope = resp.get_json()["scope"]
    # Fixture: 2 photos on the root + 1 in its child folder.
    assert scope["photo_count"] == 3


def test_pipeline_plan_folder_scope_unlinked_404(app_and_db):
    app, db = app_and_db
    original_ws = db._active_workspace_id
    other = db.create_workspace("PlanOther")
    db.set_active_workspace(other)
    foreign = db.add_folder("/photos/plan-foreign", name="plan-foreign")
    db.set_active_workspace(original_ws)
    client = app.test_client()
    resp = client.post("/api/pipeline/plan", json={"folder_ids": [foreign]})
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Life list explorer (taxonomic completeness)
# ---------------------------------------------------------------------------

def _seed_bird_taxonomy(db):
    """Insert a tiny Aves subtree: class Aves > 2 orders > families > genera > species.
    Returns dict of name -> taxa id. (Mirror of the helper in test_db.py.)"""
    rows = [
        (3,     "Aves",           "Birds",         "class",   None,             "Animalia"),
        (7251,  "Passeriformes",  "Perching Birds", "order",  "Aves",           "Animalia"),
        (67566, "Passerellidae",  "New World Sparrows", "family", "Passeriformes", "Animalia"),
        (9100,  "Melospiza",      None,            "genus",   "Passerellidae",  "Animalia"),
        (9101,  "Melospiza melodia", "Song Sparrow", "species", "Melospiza",    "Animalia"),
        (9102,  "Melospiza georgiana", "Swamp Sparrow", "species", "Melospiza",  "Animalia"),
        (9200,  "Zonotrichia",    None,            "genus",   "Passerellidae",  "Animalia"),
        (9201,  "Zonotrichia albicollis", "White-throated Sparrow", "species", "Zonotrichia", "Animalia"),
        (4000,  "Anseriformes",   "Waterfowl",     "order",   "Aves",           "Animalia"),
        (4100,  "Anatidae",       "Ducks",         "family",  "Anseriformes",   "Animalia"),
        (4200,  "Anas",           None,            "genus",   "Anatidae",       "Animalia"),
        (4201,  "Anas platyrhynchos", "Mallard",   "species", "Anas",           "Animalia"),
    ]
    ids = {}
    for inat_id, name, common, rank, parent, kingdom in rows:
        parent_id = ids.get(parent)
        cur = db.conn.execute(
            "INSERT INTO taxa (inat_id, name, common_name, rank, parent_id, kingdom)"
            " VALUES (?,?,?,?,?,?)",
            (inat_id, name, common, rank, parent_id, kingdom),
        )
        ids[name] = cur.lastrowid
    db.conn.commit()
    return ids


def test_build_explorer_payload_rollup(db):
    from app import _build_explorer_payload
    ids = _seed_bird_taxonomy(db)
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)
    fid = db.add_folder('/p', name='p')
    p = db.add_photo(folder_id=fid, filename='a.jpg', extension='.jpg',
                     file_size=1, file_mtime=1.0)
    k = db.add_keyword('Song Sparrow')
    db.tag_photo(p, k)
    db.conn.execute("UPDATE keywords SET is_species=1, taxon_id=? WHERE id=?",
                    (ids['Melospiza melodia'], k))
    db.conn.commit()

    payload = _build_explorer_payload(db)
    assert payload['taxonomy_ready'] is True
    assert payload['root']['name'] == 'Aves'
    s = payload['summary']
    assert s['species'] == {'found': 1, 'total': 4}     # 4 species seeded
    assert s['genus'] == {'found': 1, 'total': 3}
    assert s['family'] == {'found': 1, 'total': 2}
    assert s['order'] == {'found': 1, 'total': 2}
    # Passeriformes order node carries family child counts + species rollup
    orders = {n['name']: n for n in payload['nodes']}
    passeri = orders['Passeriformes']
    assert passeri['found_species'] == 1 and passeri['total_species'] == 3
    assert passeri['child_rank'] == 'family'
    assert passeri['found_children'] == 1 and passeri['total_children'] == 1


def test_build_explorer_payload_not_ready(db):
    from app import _build_explorer_payload
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)
    payload = _build_explorer_payload(db)
    assert payload['taxonomy_ready'] is False
    assert payload['nodes'] == []


def test_build_explorer_payload_rejects_non_class_root(db):
    from app import _build_explorer_payload
    ids = _seed_bird_taxonomy(db)
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)
    # An order-rank taxon is NOT a valid explorer root: reject it.
    payload = _build_explorer_payload(db, root_id=ids['Passeriformes'])
    assert payload['taxonomy_ready'] is True
    assert payload['valid_root'] is False
    assert payload['root'] is None
    assert payload['nodes'] == []
    assert payload['summary'] == {}
    # The default (Aves, a class) is a valid root.
    default_payload = _build_explorer_payload(db)
    assert default_payload['taxonomy_ready'] is True
    assert default_payload['valid_root'] is not False
    assert default_payload['root']['name'] == 'Aves'


def test_build_explorer_payload_multi_found_species_rollup(db):
    from app import _build_explorer_payload
    ids = _seed_bird_taxonomy(db)
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)
    fid = db.add_folder('/p', name='p')
    p1 = db.add_photo(folder_id=fid, filename='a.jpg', extension='.jpg',
                      file_size=1, file_mtime=1.0)
    p2 = db.add_photo(folder_id=fid, filename='b.jpg', extension='.jpg',
                      file_size=1, file_mtime=2.0)
    # Tag BOTH Melospiza species as found (two keywords, one per taxon).
    k1 = db.add_keyword('Song Sparrow')
    db.tag_photo(p1, k1)
    db.conn.execute("UPDATE keywords SET is_species=1, taxon_id=? WHERE id=?",
                    (ids['Melospiza melodia'], k1))
    k2 = db.add_keyword('Swamp Sparrow')
    db.tag_photo(p2, k2)
    db.conn.execute("UPDATE keywords SET is_species=1, taxon_id=? WHERE id=?",
                    (ids['Melospiza georgiana'], k2))
    db.conn.commit()

    payload = _build_explorer_payload(db)
    nodes = {n['name']: n for n in payload['nodes']}
    # Passeriformes order -> Passerellidae family -> Melospiza + Zonotrichia genera.
    passeri = nodes['Passeriformes']
    passerellidae = {c['name']: c for c in passeri['children']}['Passerellidae']
    melospiza = {c['name']: c for c in passerellidae['children']}['Melospiza']
    # Both Melospiza species found.
    assert melospiza['found_species'] == 2
    assert melospiza['total_species'] == 2
    # Family: both found species roll up, but only ONE genus (Melospiza) has any.
    assert passerellidae['found_species'] == 2
    assert passerellidae['found_children'] == 1
    # Zonotrichia albicollis is NOT tagged -> Zonotrichia genus has none.
    zonotrichia = {c['name']: c for c in passerellidae['children']}['Zonotrichia']
    assert zonotrichia['found_species'] == 0
    # Summary: 2 species found, but only 1 genus counts as found.
    assert payload['summary']['species']['found'] == 2
    assert payload['summary']['genus']['found'] == 1


def test_build_explorer_species_leaf(db):
    from app import _build_explorer_species
    ids = _seed_bird_taxonomy(db)
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)
    fid = db.add_folder('/p', name='p')
    p = db.add_photo(folder_id=fid, filename='a.jpg', extension='.jpg',
                     file_size=1, file_mtime=1.0)
    k = db.add_keyword('Song Sparrow')
    db.tag_photo(p, k)
    db.conn.execute("UPDATE keywords SET is_species=1, taxon_id=? WHERE id=?",
                    (ids['Melospiza melodia'], k))
    db.conn.commit()
    out = _build_explorer_species(db, ids['Melospiza'])
    by = {s['name']: s for s in out['species']}
    assert by['Melospiza melodia']['found'] is True
    assert by['Melospiza melodia']['photo']['filename'] == 'a.jpg'
    assert by['Melospiza georgiana']['found'] is False
    assert by['Melospiza georgiana'].get('photo') is None
    # found first, then missing; each block alphabetical
    assert [s['found'] for s in out['species']] == [True, False]


def test_api_explorer_endpoint(app_and_db):
    app, db = app_and_db
    ids = _seed_bird_taxonomy(db)
    # Link the fixture's existing 'Cardinal' keyword to a bird taxon so it counts.
    db.conn.execute("UPDATE keywords SET is_species=1, taxon_id=? WHERE name='Cardinal'",
                    (ids['Melospiza melodia'],))
    db.conn.commit()
    client = app.test_client()
    r = client.get('/api/life-list/explorer')
    assert r.status_code == 200
    data = r.get_json()
    assert data['taxonomy_ready'] is True
    assert data['root']['name'] == 'Aves'
    assert data['summary']['order']['total'] == 2
    # species leaf
    r2 = client.get(f"/api/life-list/explorer/species?genus={ids['Melospiza']}")
    assert r2.status_code == 200
    assert {s['name'] for s in r2.get_json()['species']} == \
        {'Melospiza melodia', 'Melospiza georgiana'}


def test_api_explorer_not_ready(app_and_db):
    app, db = app_and_db  # fixture has no taxa
    r = app.test_client().get('/api/life-list/explorer')
    assert r.status_code == 200
    assert r.get_json()['taxonomy_ready'] is False


def _seed_mammal_taxon(db):
    """Add a second class-rank taxon (Mammalia) so multi-class selector tests
    have somewhere to switch to. Returns the class row id."""
    cur = db.conn.execute(
        "INSERT INTO taxa (inat_id, name, common_name, rank, parent_id, kingdom)"
        " VALUES (?,?,?,?,?,?)",
        (40151, 'Mammalia', 'Mammals', 'class', None, 'Animalia'),
    )
    db.conn.commit()
    return cur.lastrowid


def test_build_explorer_payload_always_includes_default_aves_class(db):
    # Codex P2: after switching away from Aves, the default Birds class must
    # stay in the selector so the user has an in-page way back to it. Recomputing
    # `classes` only from *found* taxa would otherwise drop Birds when the user's
    # only tagged species are outside Aves.
    from app import _build_explorer_payload
    ids = _seed_bird_taxonomy(db)
    mammalia_id = _seed_mammal_taxon(db)
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)
    # User has tagged species — but NONE of them are birds.
    fid = db.add_folder('/p', name='p')
    # No mammal species seeded; leave the mammal tag unmatched so `found` is
    # empty but the class ancestor list would still not include Aves without
    # the fix. That still isolates the "default Aves always present" behavior.

    # No found taxa at all -> Aves must still be in the returned classes when
    # the root is the default (Aves).
    payload_default = _build_explorer_payload(db)
    assert any(c['name'] == 'Aves' for c in payload_default['classes'])

    # Switching to Mammalia (a class the user has no *matched* species in):
    # the returned classes list must STILL include Aves as a fallback, so the
    # client can rebuild the selector without losing Birds.
    payload_mammal = _build_explorer_payload(db, root_id=mammalia_id)
    class_names = [c['name'] for c in payload_mammal['classes']]
    assert 'Aves' in class_names, (
        "Default Aves class must remain in the selector after switching to a "
        "non-Aves class, otherwise the user has no in-page way back to Birds"
    )
