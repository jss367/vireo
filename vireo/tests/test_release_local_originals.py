"""Releasing managed copies must never delete photos or their catalog data."""

import os
import threading
from pathlib import Path

import pytest
from offline_cache import cache_photo_original, release_photo_original, resolve_original_path
from services import photo_ingest, workspace_folders
from testing.waits import synchronization_timeout
from wait import wait_for_job_via_client


def cache_fixture(fixture, *, sidecars=False):
    app, db, pid = fixture
    photo = db.photos.get(pid)
    folder = db.folders.get(photo["folder_id"])
    source = Path(folder["path"]) / photo["filename"]
    if sidecars:
        source.with_suffix(".xmp").write_text("source metadata")
        companion = source.with_name("companion.jpg")
        companion.write_bytes(b"companion image")
        db.conn.execute("UPDATE photos SET companion_path=? WHERE id=?", (companion.name, pid))
        db.commit()
        photo = db.photos.get(pid)
    base = Path(app.config["THUMB_CACHE_DIR"]).parent
    cache_photo_original(db, photo, str(base), {folder["id"]: folder["path"]})
    return base, source, dict(db.caches.offline_original_get(pid))


def run_release(app, payload):
    client = app.test_client()
    response = client.post("/api/jobs/release-local-originals", json=payload)
    assert response.status_code == 200, response.get_json()
    return wait_for_job_via_client(client, response.get_json()["job_id"])


def test_release_preserves_sources_metadata_and_generated_assets(client_with_photo):
    app, db, pid = client_with_photo
    base, source, row = cache_fixture(client_with_photo, sidecars=True)
    db.photo_review.set_rating(pid, 4)
    db.commit()
    db.set_photo_edit_recipe(pid, {"rotation": 90})
    db.commit()
    keyword = db.add_keyword("Hawk")
    db.tag_photo(pid, keyword)
    db.commit()
    before = dict(db.photos.get(pid))
    thumb = Path(app.config["THUMB_CACHE_DIR"]) / f"{pid}.jpg"
    thumb.write_bytes(b"keep thumbnail")
    source_bytes = source.read_bytes()

    job = run_release(app, {"photo_ids": [pid, pid]})
    assert job["status"] == "completed", job
    assert job["result"]["released"] == 1
    assert job["result"]["bytes"] == row["bytes"]
    assert db.caches.offline_original_get(pid) is None
    assert db.caches.offline_original_total_bytes() == 0
    assert dict(db.photos.get(pid)) == before
    assert thumb.read_bytes() == b"keep thumbnail"
    assert source.read_bytes() == source_bytes
    assert source.with_suffix(".xmp").read_text() == "source metadata"
    assert source.with_name("companion.jpg").read_bytes() == b"companion image"
    for column in ("original_path", "xmp_path", "companion_path"):
        assert not (base / row[column]).exists()
    resolved, offline = resolve_original_path(
        db, db.photos.get(pid), str(base), {before["folder_id"]: str(source.parent)},
        prefer_cached=True,
    )
    assert resolved == str(source)
    assert offline is False
    repeated = run_release(app, {"photo_ids": [pid]})
    assert repeated["result"]["not_cached"] == 1
    assert repeated["result"]["bytes"] == 0


@pytest.mark.parametrize("problem", ["missing", "changed", "companion_missing", "xmp_missing"])
def test_release_keeps_copies_when_sources_cannot_replace_them(client_with_photo, problem):
    app, db, pid = client_with_photo
    base, source, row = cache_fixture(client_with_photo, sidecars=True)
    if problem == "missing":
        source.unlink()
    elif problem == "changed":
        source.write_bytes(b"changed source image")
    elif problem == "companion_missing":
        source.with_name("companion.jpg").unlink()
    else:
        source.with_suffix(".xmp").unlink()
    job = run_release(app, {"photo_ids": [pid]})
    assert job["result"]["kept"] == 1
    assert job["result"]["bytes"] == 0
    assert job["result"]["kept_reasons"]
    assert dict(db.caches.offline_original_get(pid)) == row
    for column in ("original_path", "xmp_path", "companion_path"):
        assert (base / row[column]).is_file()


def test_release_accepts_newer_source_metadata(client_with_photo):
    app, db, pid = client_with_photo
    base, source, row = cache_fixture(client_with_photo, sidecars=True)
    source.with_suffix(".xmp").write_text("new source edits")
    assert run_release(app, {"photo_ids": [pid]})["result"]["released"] == 1
    assert source.with_suffix(".xmp").read_text() == "new source edits"


@pytest.mark.parametrize("unsafe", ["source_path", "symlink_directory", "symlink_file"])
def test_release_never_follows_cache_paths_into_source_storage(client_with_photo, unsafe):
    app, db, pid = client_with_photo
    base, source, row = cache_fixture(client_with_photo)
    cached = base / row["original_path"]
    original_bytes = source.read_bytes()
    if unsafe == "source_path":
        db.conn.execute("UPDATE offline_originals SET original_path=? WHERE photo_id=?", (str(source), pid))
        db.commit()
    elif unsafe == "symlink_file":
        cached.unlink()
        cached.symlink_to(source)
    else:
        cached.unlink()
        cached.parent.rmdir()
        cached.parent.symlink_to(source.parent, target_is_directory=True)
    outcome = release_photo_original(db, pid, str(base))
    assert outcome["status"] == "kept"
    assert source.read_bytes() == original_bytes
    assert db.caches.offline_original_get(pid) is not None


def test_release_retries_partial_file_removal_and_counts_actual_bytes(client_with_photo, monkeypatch):
    app, db, pid = client_with_photo
    base, source, row = cache_fixture(client_with_photo, sidecars=True)
    real_unlink = os.unlink

    def fail_original(path, *args, **kwargs):
        if str(path) == str(base / row["original_path"]):
            raise PermissionError("file is busy")
        return real_unlink(path, *args, **kwargs)

    monkeypatch.setattr(os, "unlink", fail_original)
    job = run_release(app, {"photo_ids": [pid]})
    assert job["status"] == "failed", job
    assert job["result"]["failed"] == 1
    assert job["result"]["bytes"] == row["bytes"] - source.stat().st_size
    assert db.caches.offline_original_get(pid) is not None
    monkeypatch.setattr(os, "unlink", real_unlink)
    retried = run_release(app, {"photo_ids": [pid]})
    assert retried["result"]["released"] == 1
    assert retried["result"]["bytes"] == source.stat().st_size
    assert source.is_file()


def test_release_rechecks_photo_identity_after_network_probes(client_with_photo, monkeypatch):
    app, db, pid = client_with_photo
    base, source, row = cache_fixture(client_with_photo)
    begin = db.begin_immediate

    def relocate_before_lock():
        db.conn.execute("UPDATE photos SET filename='replacement.jpg' WHERE id=?", (pid,))
        db.commit()
        begin()

    monkeypatch.setattr(db, "begin_immediate", relocate_before_lock)
    result = release_photo_original(db, pid, str(base))
    assert result["status"] == "kept"
    assert (base / row["original_path"]).is_file()
    assert source.is_file()


def test_release_does_not_act_on_a_replaced_selection(client_with_photo):
    app, db, pid = client_with_photo
    base, source, row = cache_fixture(client_with_photo)
    selected = dict(db.photos.get(pid))
    selected["filename"] = "older-photo.jpg"
    result = release_photo_original(db, pid, str(base), selected_photo=selected)
    assert result["status"] == "kept"
    assert (base / row["original_path"]).is_file()


def test_release_keeps_copy_if_source_is_unreadable(client_with_photo, monkeypatch):
    import offline_cache

    app, db, pid = client_with_photo
    base, source, row = cache_fixture(client_with_photo)

    def denied(*args, **kwargs):
        raise PermissionError("source is not readable")

    monkeypatch.setattr(offline_cache, "open", denied, raising=False)
    result = release_photo_original(db, pid, str(base))
    assert result["status"] == "kept"
    assert (base / row["original_path"]).is_file()


def test_release_cancel_keeps_unprocessed_copies(client_with_photo, monkeypatch):
    app, db, pid = client_with_photo
    base, source, row = cache_fixture(client_with_photo)
    folder_id = db.photos.get(pid)["folder_id"]
    second = source.with_name("second.jpg")
    second.write_bytes(source.read_bytes())
    second_id = photo_ingest.add_photo(db, folder_id, second.name, ".jpg", file_size=second.stat().st_size, file_mtime=second.stat().st_mtime)
    cache_photo_original(db, db.photos.get(second_id), str(base), {folder_id: str(source.parent)})
    push = app._job_runner.push_event

    def cancel_after_first(job_id, event_type, data):
        push(job_id, event_type, data)
        if event_type == "progress" and data.get("phase") == "Releasing local originals":
            app._job_runner.cancel_job(job_id)

    monkeypatch.setattr(app._job_runner, "push_event", cancel_after_first)
    job = run_release(app, {"photo_ids": [pid, second_id]})
    assert job["status"] == "cancelled", job
    assert job["result"]["released"] == 1
    assert job["result"]["processed"] == 1
    assert job["result"]["bytes"] == row["bytes"]
    assert db.caches.offline_original_get(second_id) is not None
    assert source.is_file() and second.is_file()


def test_release_waits_for_preparation_guard(client_with_photo):
    from db import Database
    from offline_cache import original_preparation_guard

    app, db, pid = client_with_photo
    base, source, row = cache_fixture(client_with_photo)
    entered = threading.Event()
    finished = threading.Event()
    results = []

    def release():
        worker = Database(db._db_path)
        try:
            entered.set()
            results.append(release_photo_original(worker, pid, str(base)))
        finally:
            worker.close()
            finished.set()

    with original_preparation_guard(str(base), pid):
        thread = threading.Thread(target=release)
        thread.start()
        assert entered.wait(synchronization_timeout(5))
        assert not finished.is_set()
        assert (base / row["original_path"]).is_file()
    thread.join(synchronization_timeout(5))
    assert finished.is_set()
    assert results[0]["status"] == "released"


@pytest.mark.parametrize("body", [None, [], {}, {"photo_ids": []}, {"photo_ids": [True]},
    {"photo_ids": [1.5]}, {"photo_ids": ["1"]}, {"folder_id": True},
    {"folder_id": "1"}, {"all_cached": False}, {"all_cached": 1},
    {"all_cached": True, "photo_ids": [1]}])
def test_release_rejects_invalid_scope(client_with_photo, body):
    app, db, pid = client_with_photo
    response = app.test_client().post("/api/jobs/release-local-originals", json=body)
    assert response.status_code == 400


def test_release_selection_rejects_other_workspace_photos(client_with_photo):
    app, db, pid = client_with_photo
    base, source, row = cache_fixture(client_with_photo)
    other = db.create_workspace("Unrelated")
    client = app.test_client()
    assert client.post(f"/api/workspaces/{other}/activate").status_code == 200
    response = client.post("/api/jobs/release-local-originals", json={"photo_ids": [pid]})
    assert response.status_code == 403
    response = client.post("/api/jobs/release-local-originals", json={"folder_id": db.photos.get(pid)["folder_id"]})
    assert response.status_code == 403
    assert (base / row["original_path"]).is_file()
    # Storage's explicit all-cached action includes other workspaces.
    assert run_release(app, {"all_cached": True})["result"]["released"] == 1


def test_release_selection_reports_pathless_missing_source_as_not_cached(
    client_with_photo,
):
    """Explicit photo selection reaches ``release_photo_original`` for every
    requested id, including photos whose ``offline_originals`` row was left
    behind by a failed cache attempt. Such rows have ``status='missing_source'``
    and no managed paths, so the release helper must match Storage's
    cached-only accounting and report them as ``not_cached`` instead of
    inflating the released count with zero-byte "releases".
    """
    app, db, pid = client_with_photo
    photo = db.photos.get(pid)
    db.caches.offline_original_upsert(
        pid, original_path=None, xmp_path=None, companion_path=None,
        bytes_=0, source_size=photo["file_size"],
        source_mtime=photo["file_mtime"], cached_at=0.0,
        status="missing_source", error="source file missing",
    )
    row_before = dict(db.caches.offline_original_get(pid))

    job = run_release(app, {"photo_ids": [pid]})
    assert job["status"] == "completed", job
    result = job["result"]
    assert result["released"] == 0
    assert result["not_cached"] == 1
    assert result["bytes"] == 0
    # The row carries the failed-cache diagnostic; releasing nothing must not
    # drop it, so a later probe can still show why the cache failed.
    assert dict(db.caches.offline_original_get(pid)) == row_before


def test_release_caps_kept_reasons_sample_but_keeps_total_count(
    client_with_photo, monkeypatch,
):
    """A release whose source volume is unreachable adds a ``kept`` entry per
    photo. The whole result dict is serialized into every checkpoint, into the
    ``job_history`` row, and into the final SSE event, so an all-cached release
    across a large catalog would re-serialize a multi-megabyte reasons list on
    every tick. Cap the list at a bounded sample; the ``kept`` counter still
    reports the real total.
    """
    import offline_cache
    from web.job_launchers import _MAX_KEPT_REASONS

    app, db, pid = client_with_photo
    folder_id = db.photos.get(pid)["folder_id"]
    extras = _MAX_KEPT_REASONS + 10
    photo_ids = [pid]
    for i in range(extras):
        photo_ids.append(
            photo_ingest.add_photo(
                db, folder_id, f"missing_{i}.jpg", ".jpg", file_size=1, file_mtime=0.0,
            )
        )

    def pretend_kept(*args, **kwargs):
        return {"status": "kept", "bytes": 0, "reason": "Source folder is unavailable"}

    monkeypatch.setattr(offline_cache, "release_photo_original", pretend_kept)

    job = run_release(app, {"photo_ids": photo_ids})
    assert job["status"] == "completed", job
    result = job["result"]
    assert result["kept"] == len(photo_ids)
    assert len(result["kept_reasons"]) == _MAX_KEPT_REASONS


def test_release_caps_error_reasons_sample_but_keeps_total_count(
    client_with_photo, monkeypatch,
):
    """Mirror of the ``kept_reasons`` cap for per-photo failures: if every
    unlink fails (managed cache directory unwritable, say), the per-photo
    error strings would otherwise be re-serialized into every checkpoint,
    the ``job_history`` row and the SSE payload (which also carries
    ``job["errors"]`` separately). Cap the sample on both ``result["errors"]``
    and ``job["errors"]``; publish the real ``failed`` count via
    ``errors_total`` so ``_error_total`` and the persisted ``error_count``
    stay accurate.
    """
    import offline_cache
    from web.job_launchers import _MAX_ERROR_REASONS

    app, db, pid = client_with_photo
    folder_id = db.photos.get(pid)["folder_id"]
    extras = _MAX_ERROR_REASONS + 10
    photo_ids = [pid]
    for i in range(extras):
        photo_ids.append(
            photo_ingest.add_photo(
                db, folder_id, f"missing_{i}.jpg", ".jpg", file_size=1, file_mtime=0.0,
            )
        )

    def pretend_failed(*args, **kwargs):
        return {"status": "failed", "bytes": 0, "reason": "cache directory unwritable"}

    monkeypatch.setattr(offline_cache, "release_photo_original", pretend_failed)

    job = run_release(app, {"photo_ids": photo_ids})
    # A result with ``ok=False`` ends the job as "failed" (the runner's
    # rollup rule for a work function that recorded errors in the job).
    assert job["status"] == "failed", job
    result = job["result"]
    assert result["failed"] == len(photo_ids)
    assert len(result["errors"]) == _MAX_ERROR_REASONS
    assert len(job["errors"]) == _MAX_ERROR_REASONS
    assert result["errors_total"] == len(photo_ids)


def test_release_cancellation_after_cap_still_publishes_errors_total(
    client_with_photo, monkeypatch,
):
    """Cancellation mid-run must still publish the real failure total: the
    checkpoint path and the startup-sweep +1 both read ``errors_total``, so
    a run cancelled after the sample cap is reached must set it while the
    loop is still running, not only in the final-return path.
    """
    import offline_cache
    from web.job_launchers import _MAX_ERROR_REASONS

    app, db, pid = client_with_photo
    folder_id = db.photos.get(pid)["folder_id"]
    # Exceed the cap by a visible margin so ``errors_total`` must track the
    # live total, not the capped slice length.
    extras = _MAX_ERROR_REASONS + 10
    photo_ids = [pid]
    for i in range(extras):
        photo_ids.append(
            photo_ingest.add_photo(
                db, folder_id, f"missing_{i}.jpg", ".jpg", file_size=1, file_mtime=0.0,
            )
        )

    def pretend_failed(*args, **kwargs):
        return {"status": "failed", "bytes": 0, "reason": "cache directory unwritable"}

    monkeypatch.setattr(offline_cache, "release_photo_original", pretend_failed)

    push = app._job_runner.push_event
    cancel_at = _MAX_ERROR_REASONS + 5

    def cancel_after_cap(job_id, event_type, data):
        push(job_id, event_type, data)
        if (
            event_type == "progress"
            and data.get("phase") == "Releasing local originals"
            and (data.get("current") or 0) >= cancel_at
        ):
            app._job_runner.cancel_job(job_id)

    monkeypatch.setattr(app._job_runner, "push_event", cancel_after_cap)

    job = run_release(app, {"photo_ids": photo_ids})
    assert job["status"] == "cancelled", job
    result = job["result"]
    # The loop was interrupted after at least ``cancel_at`` failures and
    # before processing every photo, so ``errors_total`` must reflect the
    # live ``failed`` count — not the capped sample length and not a value
    # the final-return path would have otherwise set.
    assert result["failed"] >= cancel_at
    assert result["failed"] < len(photo_ids)
    assert len(result["errors"]) == _MAX_ERROR_REASONS
    assert result["errors_total"] == result["failed"]


def test_release_folder_includes_cached_descendants_only(client_with_photo):
    app, db, pid = client_with_photo
    base, source, row = cache_fixture(client_with_photo)
    root_id = db.photos.get(pid)["folder_id"]
    child = source.parent / "nested"
    child.mkdir()
    image = child / "nested.jpg"
    image.write_bytes(source.read_bytes())
    folder_id = workspace_folders.add_folder(db, str(child), parent_id=root_id)
    second_id = photo_ingest.add_photo(db, folder_id, image.name, ".jpg", file_size=image.stat().st_size, file_mtime=image.stat().st_mtime)
    cache_photo_original(db, db.photos.get(second_id), str(base), {folder_id: str(child)})
    job = run_release(app, {"folder_id": root_id})
    assert job["result"]["released"] == 2
    assert job["result"]["total"] == 2
    assert source.is_file() and image.is_file()
