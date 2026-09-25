"""Tests for photo deletion API and database cleanup."""
import builtins
import json
import os

from PIL import Image
from wait import wait_for_job_via_client


def test_delete_photos_removes_from_db(app_and_db):
    """Deleting photos removes them from the photos table."""
    app, db = app_and_db
    photos = db.get_photos()
    pid = photos[0]["id"]

    result = db.delete_photos([pid])

    assert result["deleted"] == 1
    assert db.get_photo(pid) is None


def test_delete_photos_removes_keywords(app_and_db):
    """Deleting a photo removes its keyword associations."""
    app, db = app_and_db
    photos = db.get_photos()
    # bird1.jpg has keyword 'Cardinal' (from conftest)
    pid = photos[0]["id"]
    assert len(db.get_photo_keywords(pid)) > 0

    db.delete_photos([pid])

    rows = db.conn.execute(
        "SELECT * FROM photo_keywords WHERE photo_id = ?", (pid,)
    ).fetchall()
    assert len(rows) == 0


def test_delete_photos_removes_predictions(app_and_db):
    """Deleting a photo removes its predictions."""
    app, db = app_and_db
    photos = db.get_photos()
    pid = photos[0]["id"]
    det_ids = db.save_detections(pid, [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.4}, "confidence": 0.9, "category": "animal"}
    ], detector_model="MDV6")
    db.add_prediction(det_ids[0], 'Cardinal', 0.95, 'test-model')

    db.delete_photos([pid])

    rows = db.conn.execute(
        """SELECT pr.* FROM predictions pr
           JOIN detections d ON d.id = pr.detection_id
           WHERE d.photo_id = ?""", (pid,)
    ).fetchall()
    assert len(rows) == 0


def test_delete_photos_removes_pending_changes(app_and_db):
    """Deleting a photo removes its pending changes."""
    app, db = app_and_db
    photos = db.get_photos()
    pid = photos[0]["id"]
    db.queue_change(pid, "rating", "5")

    db.delete_photos([pid])

    rows = db.conn.execute(
        "SELECT * FROM pending_changes WHERE photo_id = ?", (pid,)
    ).fetchall()
    assert len(rows) == 0


def test_delete_photos_updates_folder_count(app_and_db):
    """Deleting photos decrements the folder's photo_count."""
    app, db = app_and_db
    photos = db.get_photos()
    fid = photos[0]["folder_id"]
    original_count = db.conn.execute(
        "SELECT photo_count FROM folders WHERE id = ?", (fid,)
    ).fetchone()["photo_count"]

    # Delete one photo from this folder
    db.delete_photos([photos[0]["id"]])

    new_count = db.conn.execute(
        "SELECT photo_count FROM folders WHERE id = ?", (fid,)
    ).fetchone()["photo_count"]
    assert new_count == original_count - 1


def test_delete_photos_cleans_collection_rules(app_and_db):
    """Deleting photos removes their IDs from static collection rules."""
    app, db = app_and_db
    photos = db.get_photos()
    pid1, pid2 = photos[0]["id"], photos[1]["id"]

    rules = [{"field": "photo_ids", "value": [pid1, pid2, 9999]}]
    cid = db.add_collection("Test Collection", json.dumps(rules))

    db.delete_photos([pid1])

    row = db.conn.execute("SELECT rules FROM collections WHERE id = ?", (cid,)).fetchone()
    updated_rules = json.loads(row["rules"])
    assert pid1 not in updated_rules[0]["value"]
    assert pid2 in updated_rules[0]["value"]
    assert 9999 in updated_rules[0]["value"]


def test_delete_photos_returns_file_info(app_and_db):
    """delete_photos returns folder paths and photo IDs for file cleanup."""
    app, db = app_and_db
    photos = db.get_photos()
    pid = photos[0]["id"]

    result = db.delete_photos([pid])

    assert result["deleted"] == 1
    assert len(result["files"]) == 1
    assert result["files"][0]["photo_id"] == pid
    assert "folder_path" in result["files"][0]
    assert "filename" in result["files"][0]


def test_delete_photos_skips_missing_ids(app_and_db):
    """Deleting non-existent photo IDs is silently skipped."""
    app, db = app_and_db

    result = db.delete_photos([99999])

    assert result["deleted"] == 0
    assert result["files"] == []


def test_delete_photos_batch(app_and_db):
    """Deleting multiple photos works in a single call."""
    app, db = app_and_db
    photos = db.get_photos()
    all_ids = [p["id"] for p in photos]

    result = db.delete_photos(all_ids)

    assert result["deleted"] == len(all_ids)
    for pid in all_ids:
        assert db.get_photo(pid) is None


def test_delete_photos_resolves_companions(app_and_db):
    """When include_companions=True, companion photos are also deleted."""
    app, db = app_and_db
    photos = db.get_photos()
    # Use photos[0] and photos[2] which are in the same folder (fid)
    pid1, pid2 = photos[0]["id"], photos[2]["id"]

    # Set pid2 as companion of pid1
    db.conn.execute(
        "UPDATE photos SET companion_path = ? WHERE id = ?",
        (photos[2]["filename"], pid1),
    )
    db.conn.commit()

    result = db.delete_photos([pid1], include_companions=True)

    assert result["deleted"] == 2
    assert db.get_photo(pid1) is None
    assert db.get_photo(pid2) is None


def test_delete_photos_empty_list(app_and_db):
    """Calling delete_photos with empty list is a no-op."""
    app, db = app_and_db

    result = db.delete_photos([])

    assert result["deleted"] == 0


def test_api_batch_delete_vireo_mode(app_and_db):
    """API endpoint removes photos from DB without touching disk."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pid = photos[0]["id"]

    resp = client.post("/api/batch/delete", json={
        "photo_ids": [pid],
        "mode": "vireo",
    })

    assert resp.status_code == 200
    data = resp.get_json()
    assert data["ok"] is True
    assert data["deleted"] == 1
    assert db.get_photo(pid) is None


def test_api_job_batch_delete_vireo_mode(app_and_db):
    """Background delete job removes photos and returns the normal result."""
    app, db = app_and_db
    client = app.test_client()
    pid = db.get_photos()[0]["id"]

    resp = client.post("/api/jobs/batch-delete", json={
        "photo_ids": [pid],
        "mode": "vireo",
    })

    assert resp.status_code == 200
    job = wait_for_job_via_client(client, resp.get_json()["job_id"])
    assert job["status"] == "completed"
    assert job["result"]["ok"] is True
    assert job["result"]["deleted"] == 1
    assert db.get_photo(pid) is None


def test_delete_modal_has_persistent_stage_progress(app_and_db):
    """Disk deletes show stable bars instead of reusing one resetting bar."""
    app, _ = app_and_db
    html = app.test_client().get("/browse").data.decode()

    stage_ids = [
        "deleteProgressFilesStage",
        "deleteProgressCatalogStage",
        "deleteProgressCacheStage",
    ]
    positions = [html.index(f'id="{stage_id}"') for stage_id in stage_ids]
    assert positions == sorted(positions)
    assert "Move files to Trash" in html
    assert "Remove photos from Vireo" in html
    assert "Clean up previews and cached files" in html
    assert "? ['files', 'catalog', 'cache']" in html
    assert ": ['catalog', 'cache'];" in html
    assert (
        "phase === 'Pruning pipeline cache' || "
        "phase === 'Cleaning cached files'"
    ) in html
    assert "'Updating review cache\\u2026'" in html


def test_delete_modal_marks_files_stage_partial_on_trash_failure(app_and_db):
    """When Trash fails, the files stage must render a failure state.

    Without this, the 'Finishing' phase blindly marks every stage complete
    (green check) even though files were retained on disk and the user is
    about to be prompted about permanent deletion. Guard both directions:
    the partial state must exist in the DOM/CSS and script, and the
    'complete' write path must refuse to overwrite a stage flagged as
    failed via ``dataset.failed``.
    """
    app, _ = app_and_db
    client = app.test_client()
    html = client.get("/browse").data.decode()
    css = client.get("/static/vireo-navbar.css").data.decode()

    # A partial CSS state (styled distinctly from complete) must exist so
    # the row can convey "processed with errors" instead of green complete.
    assert "/static/vireo-navbar.css" in html
    assert ".delete-progress-stage.partial" in css
    # The set-stage helper must know about the partial state.
    assert "state === 'partial'" in html
    # The updateDeleteProgress function must honour a backend-reported
    # failure count so the Files stage transitions to partial instead of
    # active-at-100% when the disk phase finishes with retained files.
    assert "markDeleteStageFailed" in html
    assert "var failed = Number(data.failed || 0);" in html
    # The complete-write path must respect an already-failed stage so a
    # later 'Finishing' or subsequent phase can't erase the failure.
    assert "state === 'complete' && row.dataset.failed" in html


def test_batch_delete_progress_reports_failed_count_on_trash_failure(
    app_and_db, tmp_path, monkeypatch,
):
    """The disk-phase completion emit must carry ``failed`` so the UI can
    keep the Files stage in a failure state instead of showing it green.

    Without a ``failed`` field on the payload, the frontend has no way to
    tell that files were retained and reports 'Move files to Trash ✓
    Complete' during subsequent cleanup phases.
    """
    import app as appmod

    app, db = app_and_db
    client = app.test_client()
    photo = db.get_photos()[0]
    folder_path = str(tmp_path / "trash_fail_progress")
    db.conn.execute(
        "UPDATE folders SET path = ? WHERE id = ?",
        (folder_path, photo["folder_id"]),
    )
    db.conn.commit()
    os.makedirs(folder_path, exist_ok=True)
    Image.new("RGB", (10, 10)).save(
        os.path.join(folder_path, photo["filename"]),
    )

    def fail_trash(paths, progress_callback=None):
        paths = list(paths)
        return 0, set(), [
            {"path": path, "error": "SMB Trash unavailable"} for path in paths
        ]

    monkeypatch.setattr(appmod, "_trash_paths", fail_trash)

    resp = client.post("/api/jobs/batch-delete", json={
        "photo_ids": [photo["id"]],
        "mode": "disk",
    })
    assert resp.status_code == 200
    job_id = resp.get_json()["job_id"]

    job = wait_for_job_via_client(client, job_id)
    assert job["status"] == "completed"
    assert job["result"]["failed_photo_ids"] == [photo["id"]]

    # Inspect emitted progress events for the disk-phase completion and
    # the Finishing emit. Both must carry ``failed`` so the UI can hold
    # the Files stage in a partial (not green ✓) state.
    events = app._job_runner.get_events(job_id)
    disk_progress = [
        e for e in events
        if e.get("type") == "progress"
        and e.get("data", {}).get("phase") == "Moving files to Trash"
    ]
    assert disk_progress, "expected at least one Moving files to Trash event"
    last_disk = disk_progress[-1]["data"]
    assert last_disk.get("failed") == 1, (
        "disk-phase completion must report the failed count so the UI can "
        f"render a partial state instead of green complete: {last_disk!r}"
    )

    finishing = [
        e for e in events
        if e.get("type") == "progress"
        and e.get("data", {}).get("phase") == "Finishing"
    ]
    assert finishing, "expected a Finishing progress event"
    assert finishing[-1]["data"].get("failed") == 1
    # Finishing must also carry a per-stage failure map so a single event is
    # enough for the frontend to render the correct state -- otherwise a
    # dropped or reordered intermediate emit would let the disk stage revert
    # to green complete under Finishing's blanket "mark all complete" branch.
    finishing_stage_failures = finishing[-1]["data"].get("stage_failures")
    assert finishing_stage_failures == {"files": 1, "catalog": 0}, (
        f"Finishing must attribute failures per-stage: "
        f"{finishing_stage_failures!r}"
    )
    # The disk-phase completion must also attribute its failure to the files
    # stage so the frontend can render partial without waiting for Finishing.
    assert last_disk.get("stage_failures") == {"files": 1}


def test_batch_delete_progress_marks_catalog_stage_partial_on_revalidation_skip(
    app_and_db, tmp_path, monkeypatch,
):
    """A concurrent move mid-delete leaves the catalog row and the UI must
    surface it -- Finishing.failed alone isn't enough; the frontend also
    needs a per-stage attribution so the catalog bar shows partial rather
    than green ✓ complete."""
    import app as appmod

    app, db = app_and_db
    client = app.test_client()
    source_folder = str(tmp_path / "source")
    dest_folder = str(tmp_path / "dest")
    src_fid = db.add_folder(source_folder, name="source")
    dst_fid = db.add_folder(dest_folder, name="dest")
    pid = db.add_photo(
        folder_id=src_fid, filename="bird.jpg", extension=".jpg",
        file_size=10, file_mtime=1.0,
    )
    os.makedirs(source_folder, exist_ok=True)
    os.makedirs(dest_folder, exist_ok=True)
    Image.new("RGB", (10, 10)).save(os.path.join(dest_folder, "bird.jpg"))

    def concurrent_move_then_trash(paths, progress_callback=None):
        db.conn.execute(
            "UPDATE photos SET folder_id = ? WHERE id = ?",
            (dst_fid, pid),
        )
        db.conn.commit()
        return 0, set(paths), []

    monkeypatch.setattr(appmod, "_trash_paths", concurrent_move_then_trash)
    resp = client.post("/api/jobs/batch-delete", json={
        "photo_ids": [pid],
        "mode": "disk",
    })
    assert resp.status_code == 200
    job_id = resp.get_json()["job_id"]
    job = wait_for_job_via_client(client, job_id)
    assert job["status"] == "completed"

    events = app._job_runner.get_events(job_id)

    # The last "Removed from Vireo" emit must carry ``failed`` (and its
    # per-stage attribution) so the frontend transitions the catalog stage
    # from complete → partial. Without this, only the Finishing event
    # would carry the retained count, and by then the frontend has
    # already blindly promoted every stage to green complete.
    catalog_progress = [
        e for e in events
        if e.get("type") == "progress"
        and e.get("data", {}).get("phase") == "Removed from Vireo"
    ]
    assert catalog_progress, (
        "expected a Removed from Vireo emit that reports the skipped count"
    )
    last_catalog = catalog_progress[-1]["data"]
    assert last_catalog.get("failed") == 1, (
        f"catalog stage completion must report skipped rows: {last_catalog!r}"
    )
    assert last_catalog.get("stage_failures") == {"catalog": 1}

    # Finishing must combine both stages' failure counts so a single event
    # can rebuild the full partial state on the frontend even if per-stage
    # emits were dropped.
    finishing = [
        e for e in events
        if e.get("type") == "progress"
        and e.get("data", {}).get("phase") == "Finishing"
    ]
    assert finishing, "expected a Finishing progress event"
    assert finishing[-1]["data"].get("failed") == 1
    assert finishing[-1]["data"].get("stage_failures") == {
        "files": 0, "catalog": 1,
    }


def test_delete_modal_marks_catalog_stage_partial_from_finishing_map(
    app_and_db,
):
    """The frontend must consume ``Finishing.stage_failures`` so a single
    Finishing event is enough to render partial per stage, even if a
    per-stage emit was dropped or reordered. Without the map, the
    Finishing branch would fall through to ``setDeleteProgressStage(stage,
    'complete')`` on every stage and quietly turn a retained catalog into
    a green ✓ complete row."""
    app, _ = app_and_db
    html = app.test_client().get("/browse").data.decode()

    # The Finishing branch must read the per-stage map and route each stage
    # with recorded failures through markDeleteStageFailed instead of
    # unconditionally marking complete.
    assert "data.stage_failures" in html
    assert (
        "stageFailures[stage]" in html or "stage_failures[stage]" in html
    ), "Finishing must look up per-stage failure counts"
    # A "Removed from Vireo" event that carries a failed count must also
    # route through markDeleteStageFailed so the catalog stage transitions
    # from complete → partial mid-run.
    assert "phase === 'Removed from Vireo'" in html
    # The relabelled count reflects the per-photo semantics of failed_ids:
    # one retained photo can leave more than one file on disk (a companion
    # plus its unattempted primary), so "photo" is accurate where "file"
    # would silently underreport.
    assert "'1 photo retained'" in html
    assert "' photos retained'" in html


def test_api_batch_delete_tolerates_pipeline_prune_system_exit(app_and_db, monkeypatch):
    """A packaged-app lazy import failure must not strand the delete request."""
    from db import Database

    app, db = app_and_db
    client = app.test_client()
    pid = db.get_photos()[0]["id"]

    def fail_prune(self, ids):
        raise SystemExit("packaged executable was moved")

    monkeypatch.setattr(Database, "prune_pipeline_cache_for_ids", fail_prune)

    resp = client.post("/api/batch/delete", json={
        "photo_ids": [pid],
        "mode": "vireo",
    })

    assert resp.status_code == 200
    data = resp.get_json()
    assert data["ok"] is True
    assert data["deleted"] == 1
    assert db.get_photo(pid) is None


def test_delete_photos_tolerates_pipeline_import_system_exit(app_and_db, monkeypatch):
    """Direct DB deletes should also treat pipeline cache pruning as optional."""
    app, db = app_and_db
    pid = db.get_photos()[0]["id"]
    real_import = builtins.__import__

    def guarded_import(name, *args, **kwargs):
        if name == "pipeline":
            raise SystemExit("packaged executable was moved")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", guarded_import)

    result = db.delete_photos([pid])

    assert result["deleted"] == 1
    assert db.get_photo(pid) is None


def test_api_batch_delete_tolerates_cache_cleanup_system_exit(app_and_db, monkeypatch):
    """Preview/thumbnail cleanup failures should not prevent a delete response."""
    app, db = app_and_db
    client = app.test_client()
    pid = db.get_photos()[0]["id"]
    real_import = builtins.__import__

    def guarded_import(name, *args, **kwargs):
        if name == "preview_cache":
            raise SystemExit("packaged executable was moved")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", guarded_import)

    resp = client.post("/api/batch/delete", json={
        "photo_ids": [pid],
        "mode": "vireo",
    })

    assert resp.status_code == 200
    data = resp.get_json()
    assert data["ok"] is True
    assert data["deleted"] == 1
    assert db.get_photo(pid) is None


def test_api_batch_delete_purges_sized_preview_variants(app_and_db):
    """Delete removes every <id>_<size>.jpg preview variant, not just <id>.jpg.

    Without this, SQLite id reuse could cause a newly inserted photo to be
    served stale bytes from a previous photo's cached preview.
    """
    app, db = app_and_db
    client = app.test_client()
    pid = db.get_photos()[0]["id"]

    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    preview_dir = os.path.join(vireo_dir, "previews")
    os.makedirs(preview_dir, exist_ok=True)

    # Seed cache with both legacy <id>.jpg and sized variants
    legacy = os.path.join(preview_dir, f"{pid}.jpg")
    v1920 = os.path.join(preview_dir, f"{pid}_1920.jpg")
    v2560 = os.path.join(preview_dir, f"{pid}_2560.jpg")
    for p in (legacy, v1920, v2560):
        Image.new("RGB", (10, 10)).save(p, "JPEG")

    resp = client.post("/api/batch/delete", json={
        "photo_ids": [pid],
        "mode": "vireo",
    })
    assert resp.status_code == 200

    assert not os.path.exists(legacy)
    assert not os.path.exists(v1920)
    assert not os.path.exists(v2560)


def test_api_batch_delete_disk_mode(app_and_db, tmp_path):
    """API endpoint in disk mode moves files to trash (or deletes them)."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pid = photos[0]["id"]
    photo = db.get_photo(pid)

    # Point folder to a writable tmp_path location and create a real file
    folder_path = str(tmp_path / "disk_photos")
    db.conn.execute(
        "UPDATE folders SET path = ? WHERE id = ?",
        (folder_path, photo["folder_id"]),
    )
    db.conn.commit()
    os.makedirs(folder_path, exist_ok=True)
    real_file = os.path.join(folder_path, photo["filename"])
    Image.new("RGB", (10, 10)).save(real_file)

    resp = client.post("/api/batch/delete", json={
        "photo_ids": [pid],
        "mode": "disk",
    })

    assert resp.status_code == 200
    data = resp.get_json()
    assert data["ok"] is True
    assert data["deleted"] == 1
    assert db.get_photo(pid) is None


def test_api_batch_delete_disk_failure_retains_catalog_row(
    app_and_db, tmp_path, monkeypatch,
):
    """A Trash failure leaves both the original and its Vireo row retryable."""
    import app as appmod

    app, db = app_and_db
    client = app.test_client()
    photo = db.get_photos()[0]
    folder_path = str(tmp_path / "disk_photos")
    db.conn.execute(
        "UPDATE folders SET path = ? WHERE id = ?",
        (folder_path, photo["folder_id"]),
    )
    db.conn.commit()
    os.makedirs(folder_path, exist_ok=True)
    real_file = os.path.join(folder_path, photo["filename"])
    Image.new("RGB", (10, 10)).save(real_file)

    def fail_trash(paths, progress_callback=None):
        paths = list(paths)
        return 0, set(), [
            {"path": path, "error": "SMB Trash unavailable"} for path in paths
        ]

    monkeypatch.setattr(appmod, "_trash_paths", fail_trash)
    resp = client.post("/api/batch/delete", json={
        "photo_ids": [photo["id"]],
        "mode": "disk",
    })

    assert resp.status_code == 200
    data = resp.get_json()
    assert data["deleted"] == 0
    assert data["trashed"] == 0
    assert data["failed_photo_ids"] == [photo["id"]]
    assert data["trash_failed"][0]["photo_id"] == photo["id"]
    assert os.path.exists(real_file)
    assert db.get_photo(photo["id"]) is not None


def test_api_batch_delete_disk_partial_failure_deletes_only_successes(
    app_and_db, tmp_path, monkeypatch,
):
    """A mixed filesystem result deletes only successfully trashed rows."""
    import app as appmod

    app, db = app_and_db
    client = app.test_client()
    folder_path = str(tmp_path / "partial")
    fid = db.add_folder(folder_path, name="partial")
    success_id = db.add_photo(
        folder_id=fid, filename="success.jpg", extension=".jpg",
        file_size=10, file_mtime=1.0,
    )
    failed_id = db.add_photo(
        folder_id=fid, filename="failed.jpg", extension=".jpg",
        file_size=10, file_mtime=2.0,
    )
    os.makedirs(folder_path, exist_ok=True)
    success_path = os.path.join(folder_path, "success.jpg")
    failed_path = os.path.join(folder_path, "failed.jpg")
    Image.new("RGB", (10, 10)).save(success_path)
    Image.new("RGB", (10, 10)).save(failed_path)

    def partial_trash(paths, progress_callback=None):
        paths = list(paths)
        successes = {p for p in paths if p == success_path}
        for p in successes:
            os.remove(p)
        failures = [
            {"path": p, "error": "simulated failure"}
            for p in paths if p not in successes
        ]
        return len(successes), successes, failures

    monkeypatch.setattr(appmod, "_trash_paths", partial_trash)
    resp = client.post("/api/batch/delete", json={
        "photo_ids": [success_id, failed_id],
        "mode": "disk",
    })

    assert resp.status_code == 200
    data = resp.get_json()
    assert data["deleted"] == 1
    assert data["trashed"] == 1
    assert data["failed_photo_ids"] == [failed_id]
    assert db.get_photo(success_id) is None
    assert db.get_photo(failed_id) is not None
    assert not os.path.exists(success_path)
    assert os.path.exists(failed_path)


def test_api_batch_delete_skips_row_whose_identity_changed_mid_delete(
    app_and_db, tmp_path, monkeypatch,
):
    """A concurrent move-photos must not turn a disk delete into an orphan.

    The disk-mode trash step reports success when it finds the resolved path
    already missing — but that "missing" file could have been removed by a
    concurrent ``/api/jobs/move-photos`` job that ALSO committed a new
    ``folder_id`` for the same row. Deleting the row by id after the fact
    would strand the moved file in the destination folder with no catalog
    entry. Simulate that race by relocating the row inside the trash stub
    and confirm the catalog row is preserved and reported retained.
    """
    import app as appmod

    app, db = app_and_db
    client = app.test_client()
    source_folder = str(tmp_path / "source")
    dest_folder = str(tmp_path / "dest")
    src_fid = db.add_folder(source_folder, name="source")
    dst_fid = db.add_folder(dest_folder, name="dest")
    pid = db.add_photo(
        folder_id=src_fid, filename="bird.jpg", extension=".jpg",
        file_size=10, file_mtime=1.0,
    )
    os.makedirs(source_folder, exist_ok=True)
    os.makedirs(dest_folder, exist_ok=True)
    dest_file = os.path.join(dest_folder, "bird.jpg")
    Image.new("RGB", (10, 10)).save(dest_file)

    def concurrent_move_then_trash(paths, progress_callback=None):
        # Emulate move-photos committing a new folder_id while the trash
        # stub runs. The source file was already removed by the "move" so
        # the real trash step (had it run) would treat the path as
        # successfully absent.
        db.conn.execute(
            "UPDATE photos SET folder_id = ? WHERE id = ?",
            (dst_fid, pid),
        )
        db.conn.commit()
        return 0, set(paths), []

    monkeypatch.setattr(appmod, "_trash_paths", concurrent_move_then_trash)
    resp = client.post("/api/batch/delete", json={
        "photo_ids": [pid],
        "mode": "disk",
    })

    assert resp.status_code == 200
    data = resp.get_json()
    assert data["deleted"] == 0
    assert data["failed_photo_ids"] == [pid]
    assert any(
        entry.get("photo_id") == pid for entry in data["trash_failed"]
    )
    row = db.get_photo(pid)
    assert row is not None, "moved row must survive a racing disk delete"
    assert row["folder_id"] == dst_fid
    assert os.path.exists(dest_file), "moved file must not be orphaned"


def test_api_batch_delete_treats_concurrently_deleted_row_as_completed(
    app_and_db, tmp_path, monkeypatch,
):
    """A row already removed by a racing delete must not resurface as failed.

    Two overlapping delete requests can race — the first commits the row's
    removal before the second reaches identity revalidation. When the
    second's SELECT returns nothing for that id, the requested end state
    already holds; reporting it as ``failed_photo_ids`` would leave the
    client showing a photo that is absent from both catalog and disk until
    reload.
    """
    import app as appmod

    app, db = app_and_db
    client = app.test_client()
    folder_path = str(tmp_path / "raced")
    fid = db.add_folder(folder_path, name="raced")
    pid = db.add_photo(
        folder_id=fid, filename="bird.jpg", extension=".jpg",
        file_size=10, file_mtime=1.0,
    )
    os.makedirs(folder_path, exist_ok=True)

    def concurrent_delete_then_trash(paths, progress_callback=None):
        # A racing delete finished the same row before revalidation runs.
        db.conn.execute("DELETE FROM photos WHERE id = ?", (pid,))
        db.conn.commit()
        return 0, set(paths), []

    monkeypatch.setattr(appmod, "_trash_paths", concurrent_delete_then_trash)
    resp = client.post("/api/batch/delete", json={
        "photo_ids": [pid],
        "mode": "disk",
    })

    assert resp.status_code == 200
    data = resp.get_json()
    assert data["failed_photo_ids"] == [], (
        "row that was already deleted must not be reported as failed"
    )
    assert data["trash_failed"] == []
    assert db.get_photo(pid) is None


def test_api_batch_delete_disk_targets_absolute_companion_path(
    app_and_db, tmp_path, monkeypatch,
):
    """An absolute ``companion_path`` must not be joined with folder_path.

    ``companion_path`` is stored as either a bare filename (same folder) or
    an absolute path; joining an absolute path with the folder would silently
    target the wrong file, either failing the operation or trashing an
    unrelated photo without ever pruning the correct catalog row.
    """
    import app as appmod

    app, db = app_and_db
    client = app.test_client()
    photo = db.get_photos()[0]
    folder_path = str(tmp_path / "abs")
    sidecar_folder = str(tmp_path / "sidecars")
    db.conn.execute(
        "UPDATE folders SET path = ? WHERE id = ?",
        (folder_path, photo["folder_id"]),
    )
    absolute_companion = os.path.join(sidecar_folder, "bird.xmp")
    db.conn.execute(
        "UPDATE photos SET companion_path = ? WHERE id = ?",
        (absolute_companion, photo["id"]),
    )
    db.conn.commit()
    os.makedirs(folder_path, exist_ok=True)
    os.makedirs(sidecar_folder, exist_ok=True)
    primary = os.path.join(folder_path, photo["filename"])
    Image.new("RGB", (10, 10)).save(primary)
    with open(absolute_companion, "w") as f:
        f.write("sidecar")

    seen_paths = []

    def record_trash(paths, progress_callback=None):
        paths = list(paths)
        seen_paths.append(paths)
        removed = set()
        for p in paths:
            if os.path.isfile(p):
                os.remove(p)
                removed.add(p)
        return len(removed), removed, []

    monkeypatch.setattr(appmod, "_trash_paths", record_trash)
    resp = client.post("/api/batch/delete", json={
        "photo_ids": [photo["id"]],
        "mode": "disk",
        "include_companions": True,
    })

    assert resp.status_code == 200
    all_paths = [p for call in seen_paths for p in call]
    assert absolute_companion in all_paths, (
        "absolute companion path must be trashed as-is, not joined "
        "with folder_path"
    )
    # Only the primary and the absolute companion should be trashed. Any
    # additional path (e.g. a naively ``os.path.join``-ed variant of the
    # absolute companion) indicates the code failed to detect that
    # ``companion_path`` was already absolute.
    unexpected = set(all_paths) - {primary, absolute_companion}
    assert not unexpected, (
        f"unexpected paths trashed alongside primary/companion: {unexpected}"
    )
    assert not os.path.exists(absolute_companion)
    assert not os.path.exists(primary)
    assert db.get_photo(photo["id"]) is None


def test_api_batch_delete_skips_row_whose_folder_path_changed_mid_delete(
    app_and_db, tmp_path, monkeypatch,
):
    """A concurrent /api/jobs/move-folder must not turn a disk delete into an orphan.

    A folder move keeps each photo's ``folder_id`` and ``filename`` unchanged
    while renaming ``folders.path``. Comparing only ``(folder_id, filename)``
    would pass, and the delete would strip the catalog row even though the
    copied file now lives at the new folder path. The revalidation must also
    include the resolved folder path.
    """
    import app as appmod

    app, db = app_and_db
    client = app.test_client()
    original_folder = str(tmp_path / "original")
    renamed_folder = str(tmp_path / "renamed")
    fid = db.add_folder(original_folder, name="original")
    pid = db.add_photo(
        folder_id=fid, filename="bird.jpg", extension=".jpg",
        file_size=10, file_mtime=1.0,
    )
    os.makedirs(renamed_folder, exist_ok=True)
    moved_file = os.path.join(renamed_folder, "bird.jpg")
    Image.new("RGB", (10, 10)).save(moved_file)

    def concurrent_folder_rename_then_trash(paths, progress_callback=None):
        # Simulate move-folder committing a new folders.path mid-delete while
        # keeping folder_id and filename intact — the same photo row now
        # points at moved_file instead of the resolved original_folder path.
        db.conn.execute(
            "UPDATE folders SET path = ? WHERE id = ?",
            (renamed_folder, fid),
        )
        db.conn.commit()
        return 0, set(paths), []

    monkeypatch.setattr(
        appmod, "_trash_paths", concurrent_folder_rename_then_trash,
    )
    resp = client.post("/api/batch/delete", json={
        "photo_ids": [pid],
        "mode": "disk",
    })

    assert resp.status_code == 200
    data = resp.get_json()
    assert data["deleted"] == 0
    assert data["failed_photo_ids"] == [pid]
    assert any(
        entry.get("photo_id") == pid for entry in data["trash_failed"]
    )
    row = db.get_photo(pid)
    assert row is not None, "renamed-folder row must survive a racing disk delete"
    assert os.path.exists(moved_file), "moved file must not be orphaned"


def test_api_batch_delete_disk_revalidates_companion_pairing_before_delete(
    app_and_db, tmp_path, monkeypatch,
):
    """A concurrent scan that pairs RAW+JPEG must not orphan the JPEG on disk.

    A scan pairing step commits ``UPDATE photos SET companion_path`` on the
    surviving RAW and ``DELETE FROM photos`` on the merged JPEG row while
    keeping the RAW's ``(folder_id, filename, folder_path)`` untouched.
    Comparing only that tuple would still match, and the delete would trash
    the RAW file + remove the RAW row while never touching the JPEG file —
    leaving the JPEG on disk with no catalog entry. The revalidation must
    also include ``companion_path`` so the newly-paired row is skipped and
    reported retained.
    """
    import app as appmod

    app, db = app_and_db
    client = app.test_client()
    folder_path = str(tmp_path / "raw_pair")
    fid = db.add_folder(folder_path, name="raw_pair")
    raw_id = db.add_photo(
        folder_id=fid, filename="bird.nef", extension=".nef",
        file_size=10, file_mtime=1.0,
    )
    os.makedirs(folder_path, exist_ok=True)
    raw_file = os.path.join(folder_path, "bird.nef")
    jpeg_file = os.path.join(folder_path, "bird.jpg")
    with open(raw_file, "wb") as f:
        f.write(b"raw bytes")
    Image.new("RGB", (10, 10)).save(jpeg_file)

    def concurrent_pair_then_trash(paths, progress_callback=None):
        # Emulate scanner pairing committing a new companion_path on the RAW
        # mid-delete. The (folder_id, filename, folder_path) tuple is
        # unchanged, but the row now claims the JPEG as its companion and
        # any real trash step would need to include that file too.
        db.conn.execute(
            "UPDATE photos SET companion_path = 'bird.jpg' WHERE id = ?",
            (raw_id,),
        )
        db.conn.commit()
        return 0, set(paths), []

    monkeypatch.setattr(appmod, "_trash_paths", concurrent_pair_then_trash)
    resp = client.post("/api/batch/delete", json={
        "photo_ids": [raw_id],
        "mode": "disk",
        "include_companions": True,
    })

    assert resp.status_code == 200
    data = resp.get_json()
    assert data["deleted"] == 0
    assert data["failed_photo_ids"] == [raw_id]
    assert any(
        entry.get("photo_id") == raw_id for entry in data["trash_failed"]
    )
    row = db.get_photo(raw_id)
    assert row is not None, (
        "newly-paired row must survive a racing disk delete"
    )
    assert row["companion_path"] == "bird.jpg"
    assert os.path.exists(jpeg_file), (
        "companion file must not be orphaned by a stale-tuple delete"
    )


def test_api_batch_delete_companion_failure_does_not_move_primary(
    app_and_db, tmp_path, monkeypatch,
):
    """An untracked companion must succeed before its primary is touched."""
    import app as appmod

    app, db = app_and_db
    client = app.test_client()
    photo = db.get_photos()[0]
    folder_path = str(tmp_path / "companions")
    db.conn.execute(
        "UPDATE folders SET path = ? WHERE id = ?",
        (folder_path, photo["folder_id"]),
    )
    db.conn.execute(
        "UPDATE photos SET companion_path = 'bird.xmp' WHERE id = ?",
        (photo["id"],),
    )
    db.conn.commit()
    os.makedirs(folder_path, exist_ok=True)
    primary = os.path.join(folder_path, photo["filename"])
    companion = os.path.join(folder_path, "bird.xmp")
    Image.new("RGB", (10, 10)).save(primary)
    with open(companion, "w") as f:
        f.write("sidecar")

    calls = []

    def fail_companion(paths, progress_callback=None):
        paths = list(paths)
        calls.append(paths)
        return 0, set(), [
            {"path": path, "error": "sidecar locked"} for path in paths
        ]

    monkeypatch.setattr(appmod, "_trash_paths", fail_companion)
    resp = client.post("/api/batch/delete", json={
        "photo_ids": [photo["id"]],
        "mode": "disk",
        "include_companions": True,
    })

    assert resp.status_code == 200
    assert calls == [[companion]], "primary must not be attempted after dependency failure"
    assert os.path.exists(primary)
    assert os.path.exists(companion)
    assert db.get_photo(photo["id"]) is not None


def test_api_batch_delete_removes_thumbnails(app_and_db):
    """Deleting a photo removes its thumbnail file."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pid = photos[0]["id"]
    thumb_dir = app.config["THUMB_CACHE_DIR"]
    thumb_path = os.path.join(thumb_dir, f"{pid}.jpg")
    assert os.path.exists(thumb_path)

    client.post("/api/batch/delete", json={
        "photo_ids": [pid],
        "mode": "vireo",
    })

    assert not os.path.exists(thumb_path)


def test_api_batch_delete_removes_working_copy(app_and_db):
    """Deleting a photo removes its working copy file."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pid = photos[0]["id"]

    # Create a working copy file for this photo
    thumb_dir = app.config["THUMB_CACHE_DIR"]
    working_dir = os.path.join(os.path.dirname(thumb_dir), "working")
    os.makedirs(working_dir, exist_ok=True)
    working_path = os.path.join(working_dir, f"{pid}.jpg")
    Image.new("RGB", (100, 100)).save(working_path)
    assert os.path.exists(working_path)

    client.post("/api/batch/delete", json={
        "photo_ids": [pid],
        "mode": "vireo",
    })

    assert not os.path.exists(working_path)


def test_api_batch_delete_requires_photo_ids(app_and_db):
    """API returns error when photo_ids is missing."""
    app, db = app_and_db
    client = app.test_client()

    resp = client.post("/api/batch/delete", json={"mode": "vireo"})

    assert resp.status_code == 400


def test_api_batch_delete_invalid_mode(app_and_db):
    """API returns error for unknown mode."""
    app, db = app_and_db
    client = app.test_client()

    resp = client.post("/api/batch/delete", json={
        "photo_ids": [1],
        "mode": "invalid",
    })

    assert resp.status_code == 400


def test_api_batch_delete_chunks_large_photo_id_lists(app_and_db, monkeypatch):
    """Route must chunk photo_ids before calling delete_photos so SQLite's
    bound-parameter cap (~999 on legacy builds) can't trip on bulk deletes."""
    import app as appmod
    from db import Database

    app, db = app_and_db
    client = app.test_client()

    fid = db.add_folder('/photos/bulk', name='bulk')
    bulk_ids = [
        db.add_photo(
            folder_id=fid,
            filename=f"bulk{i}.jpg",
            extension='.jpg',
            file_size=10,
            file_mtime=float(i),
        )
        for i in range(5)
    ]

    def small_chunked(seq, size=2):
        for i in range(0, len(seq), size):
            yield seq[i:i + size]
    monkeypatch.setattr(appmod, "_chunked", small_chunked)

    chunks_seen = []
    real_delete = Database.delete_photos

    def spy(self, photo_ids, **kwargs):
        chunks_seen.append(list(photo_ids))
        return real_delete(self, photo_ids, **kwargs)

    monkeypatch.setattr(Database, "delete_photos", spy)

    resp = client.post("/api/batch/delete", json={
        "photo_ids": bulk_ids,
        "mode": "vireo",
    })

    assert resp.status_code == 200
    assert resp.get_json()["deleted"] == len(bulk_ids)
    assert len(chunks_seen) >= 3, f"expected chunked calls, got {chunks_seen}"
    assert all(len(c) <= 2 for c in chunks_seen)
    for pid in bulk_ids:
        assert db.get_photo(pid) is None


def test_api_batch_delete_chunked_loop_is_atomic_on_failure(app_and_db, monkeypatch):
    """A failure in a later chunk must roll back earlier chunks so DB rows
    and cached files don't drift apart. Without a shared transaction the
    earlier chunks' commits would survive, leaving the route 500ing after
    deleting only part of the selection."""
    import app as appmod
    from db import Database

    app, db = app_and_db
    client = app.test_client()

    fid = db.add_folder('/photos/atomic', name='atomic')
    bulk_ids = [
        db.add_photo(
            folder_id=fid,
            filename=f"atomic{i}.jpg",
            extension='.jpg',
            file_size=10,
            file_mtime=float(i),
        )
        for i in range(5)
    ]

    def small_chunked(seq, size=2):
        for i in range(0, len(seq), size):
            yield seq[i:i + size]
    monkeypatch.setattr(appmod, "_chunked", small_chunked)

    real_delete = Database.delete_photos
    calls = {"n": 0}

    def flaky(self, photo_ids, **kwargs):
        calls["n"] += 1
        if calls["n"] == 2:
            raise RuntimeError("simulated SQLite error mid-loop")
        return real_delete(self, photo_ids, **kwargs)

    monkeypatch.setattr(Database, "delete_photos", flaky)

    resp = client.post("/api/batch/delete", json={
        "photo_ids": bulk_ids,
        "mode": "vireo",
    })

    # Route should fail visibly rather than silently dropping rows.
    assert resp.status_code >= 500
    # No DB rows should be missing — the first chunk's DML must roll back
    # along with the failed chunk's, restoring the all-or-nothing semantics
    # the single-call path had.
    for pid in bulk_ids:
        assert db.get_photo(pid) is not None, f"photo {pid} was deleted despite rollback"


def test_api_batch_delete_chunked_failure_preserves_pipeline_cache(
    app_and_db, monkeypatch
):
    """When a later chunk fails and the route rolls the DB back, the
    pipeline review cache must NOT have already been pruned for the
    earlier chunks' photos — those rows still exist, so pruning them
    would orphan their pipeline entries with no way to restore them
    (the cache file is non-transactional)."""
    import app as appmod
    from db import Database

    app, db = app_and_db
    client = app.test_client()

    fid = db.add_folder('/photos/pcache', name='pcache')
    bulk_ids = [
        db.add_photo(
            folder_id=fid,
            filename=f"pcache{i}.jpg",
            extension='.jpg',
            file_size=10,
            file_mtime=float(i),
        )
        for i in range(5)
    ]

    # Seed a pipeline review cache that references every photo.
    cache_dir = os.path.dirname(db._db_path)
    cache_path = os.path.join(
        cache_dir, f"pipeline_results_ws{db._active_workspace_id}.json"
    )
    cache = {
        "encounters": [{
            "species": None,
            "photo_count": len(bulk_ids),
            "burst_count": 1,
            "photo_ids": list(bulk_ids),
            "bursts": [{
                "photo_ids": list(bulk_ids),
                "species_predictions": [],
                "species_override": None,
            }],
        }],
        "photos": [{"id": pid, "label": "KEEP"} for pid in bulk_ids],
        "summary": {
            "total_photos": len(bulk_ids),
            "encounter_count": 1,
            "burst_count": 1,
            "keep_count": len(bulk_ids),
            "review_count": 0,
            "reject_count": 0,
            "rarity_protected": 0,
        },
    }
    with open(cache_path, "w") as f:
        json.dump(cache, f)

    def small_chunked(seq, size=2):
        for i in range(0, len(seq), size):
            yield seq[i:i + size]
    monkeypatch.setattr(appmod, "_chunked", small_chunked)

    real_delete = Database.delete_photos
    calls = {"n": 0}

    def flaky(self, photo_ids, **kwargs):
        calls["n"] += 1
        if calls["n"] == 2:
            raise RuntimeError("simulated SQLite error mid-loop")
        return real_delete(self, photo_ids, **kwargs)

    monkeypatch.setattr(Database, "delete_photos", flaky)

    resp = client.post("/api/batch/delete", json={
        "photo_ids": bulk_ids,
        "mode": "vireo",
    })

    assert resp.status_code >= 500

    # The on-disk pipeline cache must still reference every original
    # photo — none were actually deleted (the DB rolled back), so the
    # cache must not have been pre-pruned for the first chunk.
    with open(cache_path) as f:
        cache_after = json.load(f)
    assert [p["id"] for p in cache_after["photos"]] == list(bulk_ids)
    assert cache_after["encounters"][0]["photo_ids"] == list(bulk_ids)
    assert cache_after["encounters"][0]["bursts"][0]["photo_ids"] == list(bulk_ids)
    assert cache_after["summary"]["total_photos"] == len(bulk_ids)


def test_api_batch_delete_chunked_success_prunes_pipeline_cache(
    app_and_db, monkeypatch
):
    """On the happy path (all chunks commit), the pipeline cache must
    still be pruned of the deleted photos exactly once after the outer
    commit — i.e., deferring the prune doesn't drop it on the floor."""
    import app as appmod

    app, db = app_and_db
    client = app.test_client()

    fid = db.add_folder('/photos/pchunk', name='pchunk')
    bulk_ids = [
        db.add_photo(
            folder_id=fid,
            filename=f"pchunk{i}.jpg",
            extension='.jpg',
            file_size=10,
            file_mtime=float(i),
        )
        for i in range(5)
    ]
    survivor = db.add_photo(
        folder_id=fid,
        filename="survivor.jpg",
        extension='.jpg',
        file_size=10,
        file_mtime=99.0,
    )

    cache_dir = os.path.dirname(db._db_path)
    cache_path = os.path.join(
        cache_dir, f"pipeline_results_ws{db._active_workspace_id}.json"
    )
    all_in_cache = list(bulk_ids) + [survivor]
    cache = {
        "encounters": [{
            "species": None,
            "photo_count": len(all_in_cache),
            "burst_count": 1,
            "photo_ids": list(all_in_cache),
            "bursts": [{
                "photo_ids": list(all_in_cache),
                "species_predictions": [],
                "species_override": None,
            }],
        }],
        "photos": [{"id": pid, "label": "KEEP"} for pid in all_in_cache],
        "summary": {
            "total_photos": len(all_in_cache),
            "encounter_count": 1,
            "burst_count": 1,
            "keep_count": len(all_in_cache),
            "review_count": 0,
            "reject_count": 0,
            "rarity_protected": 0,
        },
    }
    with open(cache_path, "w") as f:
        json.dump(cache, f)

    def small_chunked(seq, size=2):
        for i in range(0, len(seq), size):
            yield seq[i:i + size]
    monkeypatch.setattr(appmod, "_chunked", small_chunked)

    resp = client.post("/api/batch/delete", json={
        "photo_ids": bulk_ids,
        "mode": "vireo",
    })
    assert resp.status_code == 200

    with open(cache_path) as f:
        cache_after = json.load(f)
    assert [p["id"] for p in cache_after["photos"]] == [survivor]
    assert cache_after["encounters"][0]["photo_ids"] == [survivor]
    assert cache_after["summary"]["total_photos"] == 1


def test_api_batch_delete_ignores_raw_paths(app_and_db, tmp_path):
    """A client-supplied ``paths`` list never deletes anything.

    Any file directly inside a catalogued folder used to be removable by
    naming it in ``paths`` with ``mode: disk_permanent``, whether or not it
    was still catalogued or belonged to the active workspace. Failed Trash
    operations keep their catalog rows, so retries go by photo id.
    """
    app, db = app_and_db
    client = app.test_client()

    db.add_folder(str(tmp_path), name=tmp_path.name)
    victim = str(tmp_path / "photo1.jpg")
    Image.new("RGB", (10, 10)).save(victim)

    resp = client.post("/api/batch/delete", json={
        "mode": "disk_permanent",
        "paths": [victim],
    })
    assert resp.status_code == 400
    assert os.path.exists(victim)

    resp = client.post("/api/jobs/batch-delete", json={
        "mode": "disk_permanent",
        "paths": [victim],
    })
    assert resp.status_code == 400
    assert os.path.exists(victim)


def _photo_only_in_other_workspace(db, tmp_path):
    """Create a photo whose folder is linked only to a second workspace.

    Leaves the original workspace active (and most recently opened, so the
    app's per-request Database restores it).
    """
    home_ws = db._active_workspace_id
    other_ws = db.create_workspace("Other")
    db.set_active_workspace(other_ws)
    folder = tmp_path / "other"
    folder.mkdir()
    fid = db.add_folder(str(folder), name="other")
    path = folder / "theirs.jpg"
    Image.new("RGB", (10, 10)).save(str(path))
    pid = db.add_photo(
        folder_id=fid, filename="theirs.jpg", extension=".jpg",
        file_size=os.path.getsize(path), file_mtime=1.0,
    )
    db.set_active_workspace(home_ws)
    return pid, str(path)


def test_api_batch_delete_skips_photos_outside_active_workspace(app_and_db, tmp_path):
    """Photos are global, so an id from another workspace must not be
    deleted (or its file trashed) from the active one."""
    app, db = app_and_db
    client = app.test_client()
    pid, path = _photo_only_in_other_workspace(db, tmp_path)
    assert client.get(f"/api/photos/{pid}").status_code == 404

    for mode in ("vireo", "disk_permanent"):
        resp = client.post("/api/batch/delete", json={
            "photo_ids": [pid], "mode": mode,
        })
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["deleted"] == 0
        assert data["trashed"] == 0

    assert db.get_photo(pid) is not None
    assert os.path.exists(path)


def test_job_batch_delete_skips_photos_outside_active_workspace(app_and_db, tmp_path):
    app, db = app_and_db
    client = app.test_client()
    pid, path = _photo_only_in_other_workspace(db, tmp_path)
    visible = db.get_photos()[0]["id"]

    resp = client.post("/api/jobs/batch-delete", json={
        "photo_ids": [pid, visible], "mode": "vireo",
    })
    assert resp.status_code == 200
    job = wait_for_job_via_client(client, resp.get_json()["job_id"])
    assert job["status"] == "completed"
    assert job["result"]["deleted"] == 1

    assert db.get_photo(pid) is not None
    assert db.get_photo(visible) is None
    assert os.path.exists(path)


def test_api_batch_delete_rejects_non_integer_ids(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    resp = client.post("/api/batch/delete", json={
        "photo_ids": ["abc"], "mode": "vireo",
    })
    assert resp.status_code == 400


def test_batch_delete_with_null_photo_ids_reports_route_result(app_and_db):
    """The request-logging hook runs after the view; a null ``photo_ids``
    must not turn the view's response into a bare 500."""
    app, db = app_and_db
    client = app.test_client()
    resp = client.post("/api/batch/delete", json={
        "photo_ids": None, "mode": "vireo",
    })
    assert resp.status_code == 400
    resp = client.post("/api/jobs/batch-delete", json={
        "photo_ids": 7, "mode": "vireo",
    })
    assert resp.status_code == 400


def test_api_batch_delete_disk_deletes_companion_file(app_and_db, tmp_path):
    """Disk mode deletes companion files when include_companions is true."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pid = photos[0]["id"]

    # Set up companion_path on the photo
    db.conn.execute(
        "UPDATE photos SET companion_path = 'companion.jpg' WHERE id = ?",
        (pid,),
    )
    folder_path = str(tmp_path / "disk_photos")
    db.conn.execute(
        "UPDATE folders SET path = ? WHERE id = ?",
        (folder_path, photos[0]["folder_id"]),
    )
    db.conn.commit()

    # Create both primary and companion files
    os.makedirs(folder_path, exist_ok=True)
    primary_file = os.path.join(folder_path, photos[0]["filename"])
    companion_file = os.path.join(folder_path, "companion.jpg")
    Image.new("RGB", (10, 10)).save(primary_file)
    Image.new("RGB", (10, 10)).save(companion_file)

    resp = client.post("/api/batch/delete", json={
        "photo_ids": [pid],
        "mode": "disk",
        "include_companions": True,
    })

    assert resp.status_code == 200
    data = resp.get_json()
    assert data["ok"] is True
    # Both primary and companion should have been trashed
    assert data["trashed"] == 2
    assert not os.path.exists(primary_file)
    assert not os.path.exists(companion_file)


def test_api_batch_delete_disk_skips_companion_when_unchecked(app_and_db, tmp_path):
    """Disk mode leaves companion files when include_companions is false."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pid = photos[0]["id"]

    db.conn.execute(
        "UPDATE photos SET companion_path = 'companion.jpg' WHERE id = ?",
        (pid,),
    )
    folder_path = str(tmp_path / "disk_photos")
    db.conn.execute(
        "UPDATE folders SET path = ? WHERE id = ?",
        (folder_path, photos[0]["folder_id"]),
    )
    db.conn.commit()

    os.makedirs(folder_path, exist_ok=True)
    primary_file = os.path.join(folder_path, photos[0]["filename"])
    companion_file = os.path.join(folder_path, "companion.jpg")
    Image.new("RGB", (10, 10)).save(primary_file)
    Image.new("RGB", (10, 10)).save(companion_file)

    resp = client.post("/api/batch/delete", json={
        "photo_ids": [pid],
        "mode": "disk",
        "include_companions": False,
    })

    assert resp.status_code == 200
    data = resp.get_json()
    assert data["trashed"] == 1
    assert not os.path.exists(primary_file)
    assert os.path.exists(companion_file)


def test_api_batch_delete_disk_permanent_with_photo_ids(app_and_db, tmp_path):
    """disk_permanent mode with photo_ids permanently deletes files."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pid = photos[0]["id"]

    folder_path = str(tmp_path / "disk_photos")
    db.conn.execute(
        "UPDATE folders SET path = ? WHERE id = ?",
        (folder_path, photos[0]["folder_id"]),
    )
    db.conn.commit()

    os.makedirs(folder_path, exist_ok=True)
    real_file = os.path.join(folder_path, photos[0]["filename"])
    Image.new("RGB", (10, 10)).save(real_file)

    resp = client.post("/api/batch/delete", json={
        "photo_ids": [pid],
        "mode": "disk_permanent",
    })

    assert resp.status_code == 200
    data = resp.get_json()
    assert data["ok"] is True
    assert data["deleted"] == 1
    assert data["trashed"] == 1
    assert not os.path.exists(real_file)
    assert db.get_photo(pid) is None


def test_delete_photos_prunes_pipeline_cache(app_and_db):
    """Deleting photos strips them from the pipeline review cache so
    they don't render as blank cards on the pipeline review page."""
    app, db = app_and_db
    photos = db.get_photos()
    pid_to_delete = photos[0]["id"]
    surviving_ids = [p["id"] for p in photos[1:]]

    cache_dir = os.path.dirname(db._db_path)
    cache_path = os.path.join(
        cache_dir, f"pipeline_results_ws{db._active_workspace_id}.json"
    )
    cache = {
        "encounters": [{
            "species": None,
            "photo_count": len(photos),
            "burst_count": 1,
            "photo_ids": [p["id"] for p in photos],
            "bursts": [{
                "photo_ids": [p["id"] for p in photos],
                "species_predictions": [],
                "species_override": None,
            }],
        }],
        "photos": [{"id": p["id"], "label": "KEEP"} for p in photos],
        "summary": {
            "total_photos": len(photos),
            "encounter_count": 1,
            "burst_count": 1,
            "keep_count": len(photos),
            "review_count": 0,
            "reject_count": 0,
            "rarity_protected": 0,
        },
    }
    with open(cache_path, "w") as f:
        json.dump(cache, f)

    db.delete_photos([pid_to_delete])

    with open(cache_path) as f:
        pruned = json.load(f)
    assert [p["id"] for p in pruned["photos"]] == surviving_ids
    assert pruned["encounters"][0]["photo_ids"] == surviving_ids
    assert pruned["encounters"][0]["bursts"][0]["photo_ids"] == surviving_ids
    assert pruned["encounters"][0]["photo_count"] == len(surviving_ids)
    assert pruned["summary"]["total_photos"] == len(surviving_ids)
    assert pruned["summary"]["keep_count"] == len(surviving_ids)


def test_delete_photos_no_pipeline_cache_does_not_raise(app_and_db):
    """delete_photos succeeds even when no pipeline cache file exists."""
    app, db = app_and_db
    photos = db.get_photos()

    cache_dir = os.path.dirname(db._db_path)
    cache_path = os.path.join(
        cache_dir, f"pipeline_results_ws{db._active_workspace_id}.json"
    )
    assert not os.path.exists(cache_path)

    result = db.delete_photos([photos[0]["id"]])
    assert result["deleted"] == 1


def test_delete_photos_with_companions_chunks_expanded_ids(tmp_path):
    """``include_companions=True`` can double the id count inside
    ``delete_photos`` (each input id may pull in its companion), so the
    internal ``IN (?, ?, …)`` DELETEs must chunk on the expanded list,
    not on the caller's input. The api/batch/delete endpoint pre-chunks
    by 900 (under SQLite's legacy 999 ``SQLITE_LIMIT_VARIABLE_NUMBER``),
    but after companion expansion the all_ids list can reach ~1800 —
    which then trips "too many SQL variables" after the file-trash
    step already ran.

    The host's actual cap is build-dependent (999 on old, 250000+ on
    new), so we lower the cap via ``setlimit`` to the legacy value. The
    input chunk (900) stays under it, but the expanded all_ids (1800)
    would exceed it without internal chunking.
    """
    import sqlite3

    from db import Database

    db = Database(str(tmp_path / "test.db"))
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)
    fid = db.add_folder(str(tmp_path / "lib"), name="lib")

    # 900 primaries + 900 companions = 1800 expanded ids.
    primary_ids = []
    companion_filenames = []
    for i in range(900):
        comp_name = f"img_{i:04d}.jpg.xmp"
        companion_filenames.append(comp_name)
        pid = db.add_photo(
            folder_id=fid, filename=f"img_{i:04d}.jpg", extension=".jpg",
            file_size=100, file_mtime=1.0,
        )
        db.add_photo(
            folder_id=fid, filename=comp_name, extension=".xmp",
            file_size=10, file_mtime=1.0,
        )
        primary_ids.append(pid)

    # Link primary → companion so include_companions resolves the sidecar.
    db.conn.executemany(
        "UPDATE photos SET companion_path = ? WHERE id = ?",
        list(zip(companion_filenames, primary_ids, strict=True)),
    )
    db.conn.commit()

    # Legacy SQLite cap — 900 input fits, 1800 expanded does not.
    db.conn.setlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, 999)

    result = db.delete_photos(primary_ids, include_companions=True)

    assert result["deleted"] == 1800  # primaries + companions
    remaining = db.conn.execute("SELECT COUNT(*) AS n FROM photos").fetchone()["n"]
    assert remaining == 0
