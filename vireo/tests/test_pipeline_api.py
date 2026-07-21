"""Tests for the full-chain import pipeline endpoint."""
import json
import os
import shutil
import tempfile
import time
from datetime import datetime
from unittest.mock import patch

import pytest
from app import create_app
from PIL import Image
from wait import wait_for_job_via_client


@pytest.fixture
def setup(tmp_path):
    db_path = str(tmp_path / "test.db")
    thumb_dir = str(tmp_path / "thumbs")
    os.makedirs(thumb_dir, exist_ok=True)
    app = create_app(db_path, thumb_dir)
    app.config["TESTING"] = True

    import config as cfg
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    return app, db_path


def test_import_full_returns_job_id(setup):
    app, db_path = setup
    # Create a source dir with a JPEG
    src = tempfile.mkdtemp()
    dest = tempfile.mkdtemp()
    try:
        # Create a minimal JPEG file (smallest valid JPEG)
        jpeg_bytes = bytes([
            0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x10, 0x4A, 0x46, 0x49, 0x46, 0x00,
            0x01, 0x01, 0x00, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00, 0xFF, 0xD9
        ])
        with open(os.path.join(src, "test.jpg"), "wb") as f:
            f.write(jpeg_bytes)

        with app.test_client() as c:
            resp = c.post("/api/jobs/import-full", json={
                "source": src,
                "destination": dest,
            })
            data = resp.get_json()
            assert resp.status_code == 200
            assert "job_id" in data
            assert data["job_id"].startswith("import-full-")
    finally:
        shutil.rmtree(src, ignore_errors=True)
        shutil.rmtree(dest, ignore_errors=True)


def test_extract_readiness_reports_missing_models(setup, tmp_path, monkeypatch):
    """Default state with no weights on disk: both models report not-ready
    with the variant-specific size hint, so the UI can warn before launch."""
    app, _ = setup
    monkeypatch.setenv("HOME", str(tmp_path))

    with app.test_client() as c:
        resp = c.get("/api/pipeline/extract-readiness"
                     "?sam2_variant=sam2-tiny&dinov2_variant=vit-s14")
        assert resp.status_code == 200
        data = resp.get_json()

        assert data["sam2"]["variant"] == "sam2-tiny"
        assert data["sam2"]["ready"] is False
        assert "MB" in data["sam2"]["size_hint"]
        assert data["sam2_known"] is True

        assert data["dinov2"]["variant"] == "vit-s14"
        assert data["dinov2"]["ready"] is False
        assert "MB" in data["dinov2"]["size_hint"]
        assert data["dinov2_known"] is True


def test_extract_readiness_reports_ready_when_files_present(
    setup, tmp_path, monkeypatch
):
    """When all required weight files exist on disk, ``ready`` flips true.
    DINOv2 needs both the graph stub and the external-data sidecar — a
    graph-only state must NOT report ready, because the loader fails."""
    app, _ = setup
    monkeypatch.setenv("HOME", str(tmp_path))

    sam2_dir = tmp_path / ".vireo" / "models" / "sam2-tiny"
    sam2_dir.mkdir(parents=True)
    (sam2_dir / "image_encoder.onnx").write_bytes(b"x")
    (sam2_dir / "mask_decoder.onnx").write_bytes(b"x")

    dinov2_dir = tmp_path / ".vireo" / "models" / "dinov2-vit-s14"
    dinov2_dir.mkdir(parents=True)
    (dinov2_dir / "model.onnx").write_bytes(b"x")
    (dinov2_dir / "model.onnx.data").write_bytes(b"x")

    with app.test_client() as c:
        resp = c.get("/api/pipeline/extract-readiness"
                     "?sam2_variant=sam2-tiny&dinov2_variant=vit-s14")
        data = resp.get_json()
        assert data["sam2"]["ready"] is True
        assert data["dinov2"]["ready"] is True


def test_extract_readiness_flags_unknown_variants(setup, tmp_path, monkeypatch):
    """An unknown variant (possible from a stale workspace override, since
    /api/pipeline/config accepts any string) must come back with
    ``*_known=False`` so the UI can flag the bad config instead of
    falsely promising a download that ``ensure_sam2_weights`` /
    DINOv2 session validation will reject at extract time."""
    app, _ = setup
    monkeypatch.setenv("HOME", str(tmp_path))

    with app.test_client() as c:
        resp = c.get("/api/pipeline/extract-readiness"
                     "?sam2_variant=sam2-bogus&dinov2_variant=vit-bogus")
        data = resp.get_json()
        assert data["sam2_known"] is False
        assert data["sam2"]["ready"] is False
        assert data["dinov2_known"] is False
        assert data["dinov2"]["ready"] is False


def test_extract_readiness_dinov2_graph_only_is_not_ready(
    setup, tmp_path, monkeypatch
):
    """Regression guard: a stub-only model.onnx (e.g. lingering from a
    pre-#550 partial download) must surface as not-ready so the user
    sees the re-download warning instead of a silent ONNX Runtime
    crash on first run."""
    app, _ = setup
    monkeypatch.setenv("HOME", str(tmp_path))

    dinov2_dir = tmp_path / ".vireo" / "models" / "dinov2-vit-b14"
    dinov2_dir.mkdir(parents=True)
    (dinov2_dir / "model.onnx").write_bytes(b"x")
    # Sidecar deliberately missing.

    with app.test_client() as c:
        resp = c.get("/api/pipeline/extract-readiness"
                     "?dinov2_variant=vit-b14")
        data = resp.get_json()
        assert data["dinov2"]["ready"] is False


def test_import_full_requires_source_and_destination(setup):
    app, db_path = setup
    with app.test_client() as c:
        resp = c.post("/api/jobs/import-full", json={"source": "/tmp/x"})
        assert resp.status_code == 400

        resp = c.post("/api/jobs/import-full", json={"destination": "/tmp/x"})
        assert resp.status_code == 400


def test_import_full_rejects_nonexistent_source(setup):
    app, db_path = setup
    with app.test_client() as c:
        resp = c.post("/api/jobs/import-full", json={
            "source": "/nonexistent/path",
            "destination": "/tmp/dest",
        })
        assert resp.status_code == 400
        assert "not found" in resp.get_json()["error"].lower()


def test_import_full_rejects_relative_destination(setup):
    app, db_path = setup
    src = tempfile.mkdtemp()
    try:
        with app.test_client() as c:
            resp = c.post("/api/jobs/import-full", json={
                "source": src,
                "destination": "relative/path",
            })
            assert resp.status_code == 400
            assert "absolute" in resp.get_json()["error"].lower()
    finally:
        shutil.rmtree(src, ignore_errors=True)


@pytest.mark.skip(reason="retired pipeline local-processing import/archive path")
def test_pipeline_local_processing_requires_destination(setup):
    app, db_path = setup
    src = tempfile.mkdtemp()
    try:
        with app.test_client() as c:
            resp = c.post("/api/jobs/pipeline", json={
                "sources": [src],
                "local_processing": True,
                "skip_classify": True,
                "skip_extract_masks": True,
                "skip_regroup": True,
            })
            assert resp.status_code == 400
            assert "destination" in resp.get_json()["error"].lower()
    finally:
        shutil.rmtree(src, ignore_errors=True)


@pytest.mark.skip(reason="retired pipeline local-processing import/archive path")
def test_pipeline_local_processing_rejects_filesystem_root_destination(
    setup, tmp_path
):
    app, _db_path = setup
    src = tmp_path / "card"
    src.mkdir()

    with app.test_client() as c:
        resp = c.post("/api/jobs/pipeline", json={
            "sources": [str(src)],
            "destination": os.path.abspath(os.sep),
            "local_processing": True,
            "skip_classify": True,
            "skip_extract_masks": True,
            "skip_regroup": True,
        })
        assert resp.status_code == 400
        assert "filesystem root" in resp.get_json()["error"].lower()


@pytest.mark.skip(reason="retired pipeline local-processing import/archive path")
def test_pipeline_local_processing_rejects_collection_id(setup, tmp_path):
    # Collection pipelines set skip_scan and never run ingest, so the
    # staging folder is never created/indexed. Without this rejection
    # the job would burn through every processing stage and then fail
    # at archive_stage with "local staging folder was not indexed".
    # The destination+collection_id guard fires first for this shape and
    # rejects it with a scope-specific message; the older
    # local_processing+collection_id guard still catches the no-destination
    # variant (covered separately by test_jobs_api.py).
    app, _db_path = setup
    dest = tmp_path / "archive"
    with app.test_client() as c:
        resp = c.post("/api/jobs/pipeline", json={
            "collection_id": 1,
            "destination": str(dest),
            "local_processing": True,
            "skip_classify": True,
            "skip_extract_masks": True,
            "skip_regroup": True,
        })
        assert resp.status_code == 400
        err = resp.get_json()["error"].lower()
        assert "destination is not allowed with collection_id" in err


@pytest.mark.skip(reason="retired pipeline local-processing import/archive path")
def test_pipeline_local_processing_rejects_collection_id_with_stale_sources(
    setup, tmp_path,
):
    # run_pipeline_job sets skip_scan = collection_id is not None, so a
    # request that mixes collection_id with a stale source/sources field
    # still skips ingest — the staging folder never gets created or
    # indexed and archive_stage fails with "local staging folder was not
    # indexed". Same shape as above (destination + collection_id +
    # local_processing) so the destination-scope guard rejects it first;
    # the stale sources field must not slip past.
    app, _db_path = setup
    src = tmp_path / "card"
    src.mkdir()
    dest = tmp_path / "archive"
    with app.test_client() as c:
        resp = c.post("/api/jobs/pipeline", json={
            "collection_id": 1,
            "sources": [str(src)],
            "destination": str(dest),
            "local_processing": True,
            "skip_classify": True,
            "skip_extract_masks": True,
            "skip_regroup": True,
        })
        assert resp.status_code == 400
        err = resp.get_json()["error"].lower()
        assert "destination is not allowed with collection_id" in err


@pytest.mark.skip(reason="retired pipeline local-processing import/archive path")
def test_pipeline_local_processing_archives_to_final_destination(
    setup, tmp_path, monkeypatch
):
    app, db_path = setup
    src = tmp_path / "card"
    src.mkdir()
    final_parent = tmp_path / "nas"
    final_parent.mkdir()
    final_dest = final_parent / "Photos"

    img = Image.new("RGB", (16, 16), "white")
    img.save(src / "test.jpg")

    import local_processing

    monkeypatch.setattr(local_processing, "MIN_DERIVED_OVERHEAD_BYTES", 0)
    monkeypatch.setattr(local_processing, "RESERVED_FREE_BYTES", 0)

    with app.test_client() as c:
        resp = c.post("/api/jobs/pipeline", json={
            "sources": [str(src)],
            "destination": str(final_dest),
            "local_processing": True,
            "folder_template": "",
            "skip_classify": True,
            "skip_extract_masks": True,
            "skip_regroup": True,
        })
        assert resp.status_code == 200
        job = wait_for_job_via_client(c, resp.get_json()["job_id"])

    assert job["status"] == "completed", job
    assert (final_dest / "test.jpg").is_file()
    assert job["result"]["archive"]["final_destination"] == str(final_dest)


@pytest.mark.skip(reason="retired pipeline local-processing import/archive path")
def test_pipeline_local_processing_all_duplicates_is_clean_noop(
    setup, tmp_path, monkeypatch
):
    """Re-running a local-processing import whose files are ALL already in
    the library must complete as a clean no-op: ingest skips every file,
    nothing reaches staging, and the archive stage merges the empty staging
    root into the existing archive without failing or duplicating photos.
    Regression guard for the "restart a failed import by re-running it"
    flow — the retry must stay a safe no-op when everything already made
    it across on the earlier attempt."""
    app, db_path = setup
    src = tmp_path / "card"
    src.mkdir()
    final_parent = tmp_path / "nas"
    final_parent.mkdir()
    final_dest = final_parent / "Photos"

    img = Image.new("RGB", (16, 16), "white")
    img.save(src / "test.jpg")

    import local_processing

    monkeypatch.setattr(local_processing, "MIN_DERIVED_OVERHEAD_BYTES", 0)
    monkeypatch.setattr(local_processing, "RESERVED_FREE_BYTES", 0)

    body = {
        "sources": [str(src)],
        "destination": str(final_dest),
        "local_processing": True,
        "folder_template": "",
        "skip_duplicates": True,
        "skip_classify": True,
        "skip_extract_masks": True,
        "skip_regroup": True,
    }
    with app.test_client() as c:
        resp = c.post("/api/jobs/pipeline", json=body)
        assert resp.status_code == 200
        first = wait_for_job_via_client(c, resp.get_json()["job_id"])
        assert first["status"] == "completed", first

        # Second run: the same card, every file now a known duplicate.
        resp = c.post("/api/jobs/pipeline", json=body)
        assert resp.status_code == 200
        second = wait_for_job_via_client(c, resp.get_json()["job_id"])

    assert second["status"] == "completed", second
    assert second["result"]["archive"]["moved"] == 0, second
    # Pin the path under test: the file must have been SKIPPED by the
    # duplicate gate, not re-copied and then deduplicated at the merge.
    ingest_step = next(
        s for s in second.get("steps", []) if s.get("id") == "ingest"
    )
    assert "skipped" in (ingest_step.get("summary") or ""), ingest_step
    assert "copied" not in (ingest_step.get("summary") or ""), ingest_step
    # The first run's archived file is untouched and still cataloged once.
    assert (final_dest / "test.jpg").is_file()
    from db import Database
    db = Database(db_path)
    n = db.conn.execute(
        "SELECT COUNT(*) FROM photos WHERE filename = 'test.jpg'"
    ).fetchone()[0]
    db.close()
    assert n == 1


@pytest.mark.skip(reason="retired pipeline local-processing import/archive path")
def test_pipeline_local_processing_merges_into_tracked_destination(
    setup, tmp_path, monkeypatch
):
    """A second local-processing import whose archive destination IS an
    already-managed folder MERGES into it instead of failing. The new files
    land at the templated date subfolder under the existing base, the prior
    shoot is untouched, and the catalog gains no duplicate folders.path row."""
    app, db_path = setup

    final_parent = tmp_path / "nas"
    final_parent.mkdir()
    final_dest = final_parent / "Photos"
    final_dest.mkdir()

    import local_processing
    monkeypatch.setattr(local_processing, "MIN_DERIVED_OVERHEAD_BYTES", 0)
    monkeypatch.setattr(local_processing, "RESERVED_FREE_BYTES", 0)

    # Seed an existing managed archive: a prior shoot on disk + in the
    # catalog, with the base linked to the active workspace as a root. This
    # matches the post-state of an earlier successful local-processing run.
    prior_dir = final_dest / "2025" / "2025-01-01"
    prior_dir.mkdir(parents=True)
    prior_file = prior_dir / "old.jpg"
    Image.new("RGB", (16, 16), "green").save(prior_file)

    from db import Database
    db = Database(db_path)
    base_id = db.add_folder(str(final_dest))
    prior_id = db.add_folder(
        str(prior_dir), parent_id=base_id, workspace_root=False
    )
    db.add_photo(
        prior_id, "old.jpg", ".jpg",
        prior_file.stat().st_size, prior_file.stat().st_mtime,
    )
    db.close()

    # New shoot whose mtime puts it in a date subfolder not yet present.
    src = tmp_path / "card2"
    src.mkdir()
    new_file = src / "again.jpg"
    Image.new("RGB", (16, 16), "blue").save(new_file)
    new_mtime = datetime(2026, 6, 30, 12, 0, 0).timestamp()
    os.utime(str(new_file), (new_mtime, new_mtime))

    with app.test_client() as c:
        resp = c.post("/api/jobs/pipeline", json={
            "sources": [str(src)],
            "destination": str(final_dest),
            "local_processing": True,
            "folder_template": "%Y/%Y-%m-%d",
            "skip_classify": True,
            "skip_extract_masks": True,
            "skip_regroup": True,
        })
        assert resp.status_code == 200
        job = wait_for_job_via_client(c, resp.get_json()["job_id"])

    # Job merged successfully (no storage failure, no tracked-overlap error).
    assert job["status"] == "completed", job
    error_text = (job.get("error") or "") + str(job.get("result", ""))
    assert "Vireo already manages" not in error_text, job

    # New file landed at the templated destination, merged alongside the
    # pre-existing prior shoot which is untouched.
    new_landed = final_dest / "2026" / "2026-06-30" / "again.jpg"
    assert new_landed.is_file(), list(final_dest.rglob("*"))
    assert prior_file.is_file()

    # Merge breakdown surfaced on the archive payload.
    merge = job["result"]["archive"]["merge"]
    assert merge["new_photos"] >= 1, merge
    assert merge["new_folders"] >= 1, merge

    # No duplicate folders.path: exactly one row each for the base and the
    # new leaf, and the new photo is attached under the new leaf.
    db = Database(db_path)
    try:
        for path in (str(final_dest), str(final_dest / "2026" / "2026-06-30")):
            rows = db.conn.execute(
                "SELECT id FROM folders WHERE path = ?", (path,)
            ).fetchall()
            assert len(rows) == 1, (path, rows)
        leaf_id = db.conn.execute(
            "SELECT id FROM folders WHERE path = ?",
            (str(final_dest / "2026" / "2026-06-30"),),
        ).fetchone()["id"]
        attached = db.conn.execute(
            "SELECT filename FROM photos WHERE folder_id = ?", (leaf_id,)
        ).fetchall()
        assert {r["filename"] for r in attached} == {"again.jpg"}, attached
        # Prior shoot row + photo untouched.
        assert db.conn.execute(
            "SELECT 1 FROM photos WHERE filename = ?", ("old.jpg",)
        ).fetchone() is not None
    finally:
        db.close()


@pytest.mark.skip(reason="retired pipeline local-processing import/archive path")
def test_pipeline_local_processing_merges_into_subfolder_of_tracked_root(
    setup, tmp_path, monkeypatch
):
    """An archive destination INSIDE an already-tracked folder (catalog
    manages /Photos and the user picks /Photos/NewShoot) MERGES into the
    existing managed tree instead of failing. The staged tree is rebased onto
    the destination, parented under the tracked ancestor, with no duplicate
    folders.path row and no stray second workspace root."""
    app, db_path = setup

    tracked_root = tmp_path / "nas_ancestor" / "Photos"
    tracked_root.mkdir(parents=True)
    # Nested archive destination INSIDE the existing tracked root.
    final_dest = tracked_root / "NewShoot"

    import local_processing
    monkeypatch.setattr(local_processing, "MIN_DERIVED_OVERHEAD_BYTES", 0)
    monkeypatch.setattr(local_processing, "RESERVED_FREE_BYTES", 0)

    from db import Database
    db = Database(db_path)
    db.add_folder(str(tracked_root))
    db.close()

    src = tmp_path / "card_ancestor"
    src.mkdir()
    new_file = src / "shot.jpg"
    Image.new("RGB", (16, 16), "red").save(new_file)
    new_mtime = datetime(2026, 6, 30, 12, 0, 0).timestamp()
    os.utime(str(new_file), (new_mtime, new_mtime))

    with app.test_client() as c:
        resp = c.post("/api/jobs/pipeline", json={
            "sources": [str(src)],
            "destination": str(final_dest),
            "local_processing": True,
            "folder_template": "%Y/%Y-%m-%d",
            "skip_classify": True,
            "skip_extract_masks": True,
            "skip_regroup": True,
        })
        assert resp.status_code == 200
        job = wait_for_job_via_client(c, resp.get_json()["job_id"])

    assert job["status"] == "completed", job
    error_text = (job.get("error") or "") + str(job.get("result", ""))
    assert "Vireo already manages" not in error_text, job

    # New file landed at the templated destination INSIDE the tracked root.
    landed = final_dest / "2026" / "2026-06-30" / "shot.jpg"
    assert landed.is_file(), list(tracked_root.rglob("*"))

    db = Database(db_path)
    try:
        # No duplicate folders.path for the leaf or the destination base.
        for path in (str(final_dest),
                     str(final_dest / "2026" / "2026-06-30")):
            rows = db.conn.execute(
                "SELECT id FROM folders WHERE path = ?", (path,)
            ).fetchall()
            assert len(rows) == 1, (path, rows)
        # The tracked ancestor remains the single workspace root; the merged
        # subtree is NOT a second root.
        ws = db._active_workspace_id
        root_paths = {
            r["path"] for r in db.conn.execute(
                """SELECT f.path FROM workspace_folders wf
                   JOIN folders f ON f.id = wf.folder_id
                   WHERE wf.workspace_id = ? AND wf.is_root = 1""",
                (ws,),
            )
        }
        assert str(tracked_root) in root_paths, root_paths
        assert str(final_dest / "2026" / "2026-06-30") not in root_paths
        # The new photo is queryable under the new leaf.
        leaf_id = db.conn.execute(
            "SELECT id FROM folders WHERE path = ?",
            (str(final_dest / "2026" / "2026-06-30"),),
        ).fetchone()["id"]
        attached = db.conn.execute(
            "SELECT filename FROM photos WHERE folder_id = ?", (leaf_id,)
        ).fetchall()
        assert {r["filename"] for r in attached} == {"shot.jpg"}, attached
    finally:
        db.close()


@pytest.mark.skip(reason="retired pipeline local-processing import/archive path")
def test_pipeline_refuses_destination_that_wraps_tracked_subfolder(
    setup, tmp_path, monkeypatch,
):
    """Regression: ``move_folder(..., allow_tracked_merge=True)`` refuses a
    destination that sits STRICTLY ABOVE an already-tracked folder (e.g.
    ``/Photos/USA`` tracked and the user picks ``/Photos``) — merging would
    wrap a fresh parent around the existing tracked subtree. Without the
    matching preflight check in the pipeline the job would stage and
    process everything, then fail only at the final archive step and leave
    the processed results stranded under ``~/.vireo/staging``. The
    preflight must bail in the storage stage before any staging work
    happens, marking scan and ingest as skipped so the pipeline actually
    terminates.
    """
    app, db_path = setup

    parent_dest = tmp_path / "nas" / "Photos"
    tracked_child = parent_dest / "USA"
    tracked_child.mkdir(parents=True)

    import local_processing
    monkeypatch.setattr(local_processing, "MIN_DERIVED_OVERHEAD_BYTES", 0)
    monkeypatch.setattr(local_processing, "RESERVED_FREE_BYTES", 0)

    # Only the child is tracked in the catalog; the parent is NOT.
    from db import Database
    db = Database(db_path)
    db.add_folder(str(tracked_child))
    db.close()

    src = tmp_path / "card_wrap"
    src.mkdir()
    photo = src / "shot.jpg"
    Image.new("RGB", (16, 16), "purple").save(photo)

    with app.test_client() as c:
        resp = c.post("/api/jobs/pipeline", json={
            "sources": [str(src)],
            "destination": str(parent_dest),
            "local_processing": True,
            "folder_template": "%Y/%Y-%m-%d",
            "skip_classify": True,
            "skip_extract_masks": True,
            "skip_regroup": True,
        })
        assert resp.status_code == 200
        job = wait_for_job_via_client(c, resp.get_json()["job_id"])

    # Job terminated — did not hang waiting for a scan that never runs.
    assert job["status"] == "failed", job

    def _collect_strings(obj):
        if isinstance(obj, str):
            return [obj]
        if isinstance(obj, dict):
            out = []
            for v in obj.values():
                out.extend(_collect_strings(v))
            return out
        if isinstance(obj, (list, tuple)):
            out = []
            for v in obj:
                out.extend(_collect_strings(v))
            return out
        return []

    # Collect raw string field values (not ``str(dict)``) so Windows
    # backslash paths compare cleanly — ``str({"error": "C:\\foo"})``
    # doubles the backslashes via repr, defeating the substring check.
    error_text = " ".join(_collect_strings(job))
    # The specific PREFLIGHT wording ("sits above") — distinct from the
    # archive-step ``move_folder`` refusal ("Destination overlaps a folder
    # Vireo already manages") — proves the storage-stage guard fired
    # before staging, not the archive step at the very end after
    # everything was processed.
    assert "sits above" in error_text, error_text
    assert str(tracked_child) in error_text, error_text

    # Ingest never ran — no photo rows for the source file made it into
    # the catalog. Without the preflight the pipeline would stage the
    # file, scan it, and land a catalog row before failing at the archive
    # move.
    from db import Database as _Db
    db2 = _Db(db_path)
    try:
        photo_count = db2.conn.execute(
            "SELECT COUNT(*) c FROM photos"
        ).fetchone()["c"]
    finally:
        db2.close()
    assert photo_count == 0, photo_count

    # Nothing was copied to the archive destination — the fresh parent
    # dir stays empty (aside from the pre-existing tracked child dir).
    landed = list(parent_dest.rglob("*.jpg"))
    assert landed == [], landed


@pytest.mark.skip(reason="retired pipeline local-processing import/archive path")
def test_pipeline_merges_new_shoot_into_existing_archive(
    setup, tmp_path, monkeypatch
):
    """End-to-end regression for the reported scenario: importing a brand-new
    shoot into an already-managed archive base seamlessly merges. The prior
    shoot's files and catalog rows stay untouched, the new files land at the
    templated date subfolder, no duplicate folders.path is created, and the
    new leaf is linked to the active workspace (queryable in it)."""
    app, db_path = setup

    archive_parent = tmp_path / "nas"
    archive_parent.mkdir()
    archive_base = archive_parent / "USA"
    archive_base.mkdir()

    import local_processing
    monkeypatch.setattr(local_processing, "MIN_DERIVED_OVERHEAD_BYTES", 0)
    monkeypatch.setattr(local_processing, "RESERVED_FREE_BYTES", 0)

    # Existing tracked archive holding a prior shoot — real files on disk and
    # catalog rows, base linked as a workspace root.
    prior_dir = archive_base / "2025" / "2025-01-01"
    prior_dir.mkdir(parents=True)
    prior_a = prior_dir / "prior-1.jpg"
    prior_b = prior_dir / "prior-2.jpg"
    Image.new("RGB", (16, 16), "green").save(prior_a)
    Image.new("RGB", (16, 16), "olive").save(prior_b)

    from db import Database
    db = Database(db_path)
    base_id = db.add_folder(str(archive_base))
    prior_id = db.add_folder(
        str(prior_dir), parent_id=base_id, workspace_root=False
    )
    for f in (prior_a, prior_b):
        db.add_photo(
            prior_id, f.name, ".jpg",
            f.stat().st_size, f.stat().st_mtime,
        )
    prior_photo_ids = {
        r["id"] for r in db.conn.execute(
            "SELECT id FROM photos WHERE folder_id = ?", (prior_id,)
        )
    }
    db.close()

    prior_a_bytes = prior_a.read_bytes()
    prior_b_bytes = prior_b.read_bytes()

    # Two NEW source files whose mtime produces a date subfolder NOT yet
    # present in the archive.
    src = tmp_path / "card"
    src.mkdir()
    new_a = src / "new-1.jpg"
    new_b = src / "new-2.jpg"
    Image.new("RGB", (16, 16), "blue").save(new_a)
    Image.new("RGB", (16, 16), "navy").save(new_b)
    new_mtime = datetime(2026, 6, 30, 9, 30, 0).timestamp()
    for f in (new_a, new_b):
        os.utime(str(f), (new_mtime, new_mtime))

    with app.test_client() as c:
        resp = c.post("/api/jobs/pipeline", json={
            "sources": [str(src)],
            "destination": str(archive_base),
            "local_processing": True,
            "folder_template": "%Y/%Y-%m-%d",
            "skip_classify": True,
            "skip_extract_masks": True,
            "skip_regroup": True,
        })
        assert resp.status_code == 200
        job = wait_for_job_via_client(c, resp.get_json()["job_id"])

    assert job["status"] == "completed", job

    # New files landed at <base>/<year>/<date>/.
    new_leaf = archive_base / "2026" / "2026-06-30"
    assert (new_leaf / "new-1.jpg").is_file(), list(archive_base.rglob("*"))
    assert (new_leaf / "new-2.jpg").is_file()

    # Prior shoot files untouched on disk (same bytes, same paths).
    assert prior_a.read_bytes() == prior_a_bytes
    assert prior_b.read_bytes() == prior_b_bytes

    db = Database(db_path)
    try:
        # No duplicate folders.path anywhere.
        dup = db.conn.execute(
            "SELECT path, COUNT(*) c FROM folders GROUP BY path HAVING c > 1"
        ).fetchall()
        assert dup == [], dup
        # Exactly one row for the new leaf.
        leaf_rows = db.conn.execute(
            "SELECT id FROM folders WHERE path = ?", (str(new_leaf),)
        ).fetchall()
        assert len(leaf_rows) == 1, leaf_rows
        leaf_id = leaf_rows[0]["id"]

        # The new leaf is linked to the active workspace; new photos are
        # queryable there.
        ws = db._active_workspace_id
        linked = db.conn.execute(
            "SELECT is_root FROM workspace_folders "
            "WHERE workspace_id = ? AND folder_id = ?",
            (ws, leaf_id),
        ).fetchone()
        assert linked is not None, "new leaf not linked to workspace"
        assert linked["is_root"] == 0, "new leaf should not be a root"
        new_names = {
            r["filename"] for r in db.conn.execute(
                "SELECT filename FROM photos WHERE folder_id = ?", (leaf_id,)
            )
        }
        assert new_names == {"new-1.jpg", "new-2.jpg"}, new_names

        # Prior shoot rows untouched: same folder id, same photo ids.
        prior_row = db.conn.execute(
            "SELECT id FROM folders WHERE path = ?", (str(prior_dir),)
        ).fetchone()
        assert prior_row is not None and prior_row["id"] == prior_id
        still = {
            r["id"] for r in db.conn.execute(
                "SELECT id FROM photos WHERE folder_id = ?", (prior_id,)
            )
        }
        assert still == prior_photo_ids, (still, prior_photo_ids)
    finally:
        db.close()


@pytest.mark.skip(reason="retired pipeline local-processing import/archive path")
def test_pipeline_local_processing_creates_missing_archive_parent(
    setup, tmp_path, monkeypatch
):
    """Regression: a nested archive destination whose parent doesn't yet
    exist (e.g. /mnt/nas/NewShoot/Photos when NewShoot was never created
    by the user) must be set up during the storage preflight, not left for
    move_folder to discover after every processing step finishes. Without
    the upfront makedirs the run would stage, process, and only fail at
    the final move, leaving the staged copy stranded."""
    app, db_path = setup

    nonexistent_parent = tmp_path / "nas" / "NewShoot"
    assert not nonexistent_parent.exists()
    final_dest = nonexistent_parent / "Photos"

    src = tmp_path / "card_parent"
    src.mkdir()
    Image.new("RGB", (16, 16), "white").save(src / "shot.jpg")

    import local_processing
    monkeypatch.setattr(local_processing, "MIN_DERIVED_OVERHEAD_BYTES", 0)
    monkeypatch.setattr(local_processing, "RESERVED_FREE_BYTES", 0)

    with app.test_client() as c:
        resp = c.post("/api/jobs/pipeline", json={
            "sources": [str(src)],
            "destination": str(final_dest),
            "local_processing": True,
            "folder_template": "",
            "skip_classify": True,
            "skip_extract_masks": True,
            "skip_regroup": True,
        })
        assert resp.status_code == 200
        job = wait_for_job_via_client(c, resp.get_json()["job_id"])

    assert job["status"] == "completed", job
    assert (final_dest / "shot.jpg").is_file()
    assert nonexistent_parent.is_dir()


@pytest.mark.skip(reason="retired pipeline local-processing import/archive path")
def test_pipeline_local_processing_detects_missing_archive_mount_root(monkeypatch):
    """Unmounted NAS paths like /Volumes/NAS/Shoot must not be created as
    local stub directories during archive-parent preflight."""
    import pipeline_job

    real_lexists = os.path.lexists

    def fake_lexists(path):
        if path == "/Volumes/NAS":
            return False
        return real_lexists(path)

    monkeypatch.setattr(pipeline_job.os.path, "lexists", fake_lexists)

    assert (
        pipeline_job._missing_archive_mount_root("/Volumes/NAS/Shoot")
        == "/Volumes/NAS"
    )
    assert pipeline_job._missing_archive_mount_root("/Volumes/NAS") == "/Volumes/NAS"


@pytest.mark.skip(reason="retired pipeline local-processing import/archive path")
def test_pipeline_local_processing_rejects_missing_archive_mount_root(
    setup, tmp_path, monkeypatch
):
    """Regression: storage preflight must fail before makedirs can create a
    missing mount root and accidentally archive onto the local disk."""
    app, _db_path = setup

    final_dest = tmp_path / "missing_mount"

    src = tmp_path / "card_mount"
    src.mkdir()
    Image.new("RGB", (16, 16), "white").save(src / "shot.jpg")

    import local_processing
    import pipeline_job
    monkeypatch.setattr(local_processing, "MIN_DERIVED_OVERHEAD_BYTES", 0)
    monkeypatch.setattr(local_processing, "RESERVED_FREE_BYTES", 0)
    monkeypatch.setattr(
        pipeline_job,
        "_missing_archive_mount_root",
        lambda path: str(final_dest) if path == str(final_dest) else None,
    )

    with app.test_client() as c:
        resp = c.post("/api/jobs/pipeline", json={
            "sources": [str(src)],
            "destination": str(final_dest),
            "local_processing": True,
            "folder_template": "",
            "skip_classify": True,
            "skip_extract_masks": True,
            "skip_regroup": True,
        })
        assert resp.status_code == 200
        job = wait_for_job_via_client(c, resp.get_json()["job_id"])

    assert job["status"] == "failed", job
    errors = job.get("result", {}).get("errors", [])
    assert any(
        f"Archive mount root {final_dest}" in error for error in errors
    ), job
    assert not final_dest.exists()


@pytest.mark.skip(reason="retired pipeline local-processing import/archive path")
def test_pipeline_local_processing_skips_archive_when_previews_fail(
    setup, tmp_path, monkeypatch
):
    """Regression: previews, extract_masks, eye_keypoints, regroup, and
    miss can all fail without aborting the run, but run_pipeline_job
    raises at the end whenever any stage status is "failed". If
    archive_stage ran anyway, the staged folder would already be moved to
    the user's archive root by the time that failure surfaced — publishing
    a partial result from a job marked failed. Skip the archive when an
    earlier stage failed and leave staging intact instead."""
    app, db_path = setup

    final_parent = tmp_path / "nas2"
    final_parent.mkdir()
    final_dest = final_parent / "Photos"

    src = tmp_path / "card_fail"
    src.mkdir()
    Image.new("RGB", (16, 16), "white").save(src / "boom.jpg")

    import local_processing
    monkeypatch.setattr(local_processing, "MIN_DERIVED_OVERHEAD_BYTES", 0)
    monkeypatch.setattr(local_processing, "RESERVED_FREE_BYTES", 0)

    # Force previews_stage to fail without setting abort. The stage's
    # outer try/except catches load_image() failures and marks the stage
    # status "failed"; abort stays clear and the rest of the pipeline
    # continues toward archive.
    import image_loader

    def boom(*args, **kwargs):
        raise RuntimeError("synthetic preview failure")

    monkeypatch.setattr(image_loader, "load_image", boom)

    import json

    from db import Database
    from pipeline import _results_cache_path

    seed_db = Database(db_path)
    ws_id = seed_db._active_workspace_id
    seed_db.set_workspace_group_state(
        ws_id, fingerprint="stale-group-fingerprint", when_ts=1714579200,
    )
    seed_db.close()
    cache_path = _results_cache_path(os.path.dirname(db_path), ws_id)
    with open(cache_path, "w") as f:
        json.dump(
            {
                "photos": [{"id": 999999}],
                "encounters": [],
                "summary": {},
            },
            f,
        )

    with app.test_client() as c:
        resp = c.post("/api/jobs/pipeline", json={
            "sources": [str(src)],
            "destination": str(final_dest),
            "local_processing": True,
            "folder_template": "",
            "skip_classify": True,
            "skip_extract_masks": True,
            "skip_regroup": True,
        })
        assert resp.status_code == 200
        job_id = resp.get_json()["job_id"]
        job = wait_for_job_via_client(c, job_id)

    # The job fails because previews_stage marked itself failed and
    # run_pipeline_job re-raises at the end for any failed stage.
    assert job["status"] == "failed", job

    # archive must NOT publish files when an earlier stage failed.
    assert not (final_dest / "boom.jpg").exists()

    # Staging stays intact so the user can recover or retry.
    staging_file = (
        tmp_path / "staging" / job_id / "Photos" / "boom.jpg"
    )
    assert staging_file.is_file(), (
        f"staging file should remain when archive is skipped, "
        f"missing at {staging_file}"
    )

    # The staging folder and its photo hashes must be removed from the
    # catalog. Otherwise ingest()'s known-hash skip would treat a retry's
    # files as duplicates and stage nothing, letting the next archive
    # publish an empty destination.
    check_db = Database(db_path)
    staging_dir = str(staging_file.parent)
    folder_row = check_db.conn.execute(
        "SELECT id FROM folders WHERE path = ?", (staging_dir,),
    ).fetchone()
    assert folder_row is None, (
        f"staging folder must be deindexed on archive skip, still present "
        f"at {staging_dir}"
    )
    photo_rows = check_db.conn.execute(
        "SELECT id FROM photos WHERE folder_id IN ("
        "  SELECT id FROM folders WHERE path LIKE ?"
        ")",
        (str(tmp_path / "staging" / job_id) + "%",),
    ).fetchall()
    assert not photo_rows, (
        "no photo rows should remain under the abandoned staging tree, "
        f"got {len(photo_rows)}"
    )
    assert not os.path.exists(cache_path), (
        "abandoning staged rows must remove the grouping cache, otherwise "
        "pipeline review can render deleted photo IDs"
    )
    ws_row = check_db.conn.execute(
        "SELECT last_grouped_at, last_group_fingerprint "
        "FROM workspaces WHERE id = ?",
        (ws_id,),
    ).fetchone()
    assert ws_row["last_grouped_at"] is None
    assert ws_row["last_group_fingerprint"] is None
    check_db.close()


@pytest.mark.skip(reason="retired pipeline local-processing import/archive path")
def test_pipeline_local_processing_retry_after_skip_actually_copies(
    setup, tmp_path, monkeypatch
):
    """End-to-end regression: a local-processing run whose archive was
    skipped (due to an earlier failed stage) must leave the catalog in a
    state where the user can retry the same source folder and have ingest
    actually re-copy the files. Without deindexing the failed staging
    tree, ingest's known-hash gate would skip every file in the new
    staging dir and the second run would publish an empty destination."""
    app, db_path = setup

    src = tmp_path / "card_retry"
    src.mkdir()
    Image.new("RGB", (16, 16), "white").save(src / "again.jpg")

    final_parent = tmp_path / "nas_retry"
    final_parent.mkdir()
    final_dest = final_parent / "Photos"

    import local_processing
    monkeypatch.setattr(local_processing, "MIN_DERIVED_OVERHEAD_BYTES", 0)
    monkeypatch.setattr(local_processing, "RESERVED_FREE_BYTES", 0)

    # First run: previews fail, archive is skipped, staging is left on
    # disk but deindexed from the catalog.
    import image_loader
    real_load_image = image_loader.load_image

    def boom(*args, **kwargs):
        raise RuntimeError("synthetic preview failure")

    monkeypatch.setattr(image_loader, "load_image", boom)

    with app.test_client() as c:
        resp = c.post("/api/jobs/pipeline", json={
            "sources": [str(src)],
            "destination": str(final_dest),
            "local_processing": True,
            "folder_template": "",
            "skip_classify": True,
            "skip_extract_masks": True,
            "skip_regroup": True,
        })
        first_job = wait_for_job_via_client(c, resp.get_json()["job_id"])
    assert first_job["status"] == "failed", first_job
    assert not (final_dest / "again.jpg").exists()

    # Second run: previews succeed (restore the real loader). The retry
    # must actually re-stage the file and publish to final_dest — the
    # previous-run staging hashes must no longer block ingest.
    monkeypatch.setattr(image_loader, "load_image", real_load_image)

    with app.test_client() as c:
        resp = c.post("/api/jobs/pipeline", json={
            "sources": [str(src)],
            "destination": str(final_dest),
            "local_processing": True,
            "folder_template": "",
            "skip_classify": True,
            "skip_extract_masks": True,
            "skip_regroup": True,
        })
        second_job = wait_for_job_via_client(c, resp.get_json()["job_id"])
    assert second_job["status"] == "completed", second_job
    assert (final_dest / "again.jpg").is_file(), (
        "retry must actually copy the file to the archive; deindex on "
        "archive-skip is what makes that possible"
    )


@pytest.mark.skip(reason="retired pipeline local-processing import/archive path")
def test_pipeline_local_processing_fails_ingest_when_files_fail_to_copy(
    setup, tmp_path, monkeypatch
):
    """Regression: ingest() catches per-file copy errors and returns a
    non-zero ``failed`` count without raising, but the scanner stage was
    marking ingest as "completed" regardless. archive_stage's "any earlier
    stage failed" gate then let the partial staging tree publish to the
    final destination. Local-processing mode must fail the ingest stage
    when files fail to copy so the archive is skipped and staging is
    preserved for the user to recover.
    """
    app, db_path = setup

    final_parent = tmp_path / "nas_fail_ingest"
    final_parent.mkdir()
    final_dest = final_parent / "Photos"

    src = tmp_path / "card_partial"
    src.mkdir()
    Image.new("RGB", (16, 16), "white").save(src / "good.jpg")
    Image.new("RGB", (16, 16), "red").save(src / "bad.jpg")

    import local_processing
    monkeypatch.setattr(local_processing, "MIN_DERIVED_OVERHEAD_BYTES", 0)
    monkeypatch.setattr(local_processing, "RESERVED_FREE_BYTES", 0)

    # Simulate a partial-card copy failure: ingest() catches the OSError
    # and bumps `failed` by 1, but returns normally so the run continues.
    import ingest as ingest_mod

    real_copy2 = ingest_mod.shutil.copy2

    def flaky_copy2(src_path, dest_path, *args, **kwargs):
        if os.path.basename(str(src_path)) == "bad.jpg":
            raise OSError("synthetic copy failure")
        return real_copy2(src_path, dest_path, *args, **kwargs)

    monkeypatch.setattr(ingest_mod.shutil, "copy2", flaky_copy2)

    with app.test_client() as c:
        resp = c.post("/api/jobs/pipeline", json={
            "sources": [str(src)],
            "destination": str(final_dest),
            "local_processing": True,
            "folder_template": "",
            "skip_classify": True,
            "skip_extract_masks": True,
            "skip_regroup": True,
        })
        assert resp.status_code == 200
        job_id = resp.get_json()["job_id"]
        job = wait_for_job_via_client(c, job_id)

    # Ingest is marked failed and the whole run is recorded as failed.
    assert job["status"] == "failed", job

    # Nothing is published at the final destination, even though good.jpg
    # made it into staging.
    assert not (final_dest / "good.jpg").exists()
    assert not final_dest.exists() or not any(final_dest.iterdir())

    # The successfully-copied file stays in staging so the user can recover.
    staging_root = tmp_path / "staging" / job_id / "Photos"
    assert staging_root.is_dir(), staging_root
    assert (staging_root / "good.jpg").is_file()


@pytest.mark.skip(reason="retired pipeline local-processing import/archive path")
def test_pipeline_local_processing_ingest_failure_short_circuits_pipeline(
    setup, tmp_path, monkeypatch
):
    """Regression: a fatal local-processing ingest failure marked the
    ingest step "failed" but left ``abort`` clear, so scanner_stage went
    on to scan the partial staging tree and previews/classify/regroup
    ran on the photos that did copy. In a normal pipeline run with
    regroup enabled, that overwrote the workspace pipeline results with
    photo IDs that archive_stage then deindexed during staging cleanup —
    the workspace was left pointing at rows that no longer existed,
    after spending hours processing a partial import. A fatal ingest
    failure must set ``abort`` so every downstream stage short-circuits.
    """
    app, _db_path = setup

    final_parent = tmp_path / "nas_short_circuit"
    final_parent.mkdir()
    final_dest = final_parent / "Photos"

    src = tmp_path / "card_short_circuit"
    src.mkdir()
    Image.new("RGB", (16, 16), "white").save(src / "good.jpg")
    Image.new("RGB", (16, 16), "red").save(src / "bad.jpg")

    import local_processing
    monkeypatch.setattr(local_processing, "MIN_DERIVED_OVERHEAD_BYTES", 0)
    monkeypatch.setattr(local_processing, "RESERVED_FREE_BYTES", 0)

    # ingest() catches per-file copy errors and continues — bad.jpg will
    # bump ``failed`` while good.jpg still lands in staging.
    import ingest as ingest_mod

    real_copy2 = ingest_mod.shutil.copy2

    def flaky_copy2(src_path, dest_path, *args, **kwargs):
        if os.path.basename(str(src_path)) == "bad.jpg":
            raise OSError("synthetic copy failure")
        return real_copy2(src_path, dest_path, *args, **kwargs)

    monkeypatch.setattr(ingest_mod.shutil, "copy2", flaky_copy2)

    # Track whether scanner.scan ever ran. If the abort propagation in
    # the ingest-failure branch is wired correctly the scan stage is
    # marked skipped without calling do_scan; if it ever fires we know
    # the scanner/previews/classify/regroup chain executed against the
    # partial copy.
    import scanner as scanner_mod
    scan_calls: list[str] = []
    real_scan = scanner_mod.scan

    def tracking_scan(root, *args, **kwargs):
        scan_calls.append(str(root))
        return real_scan(root, *args, **kwargs)

    with patch.object(scanner_mod, "scan", tracking_scan):
        with app.test_client() as c:
            # Deliberately leave skip_regroup unset so a regression here
            # would actually run the regroup stage against the partial
            # subset.
            resp = c.post("/api/jobs/pipeline", json={
                "sources": [str(src)],
                "destination": str(final_dest),
                "local_processing": True,
                "folder_template": "",
                "skip_classify": True,
                "skip_extract_masks": True,
            })
            assert resp.status_code == 200
            job_id = resp.get_json()["job_id"]
            job = wait_for_job_via_client(c, job_id)

    assert job["status"] == "failed", job
    assert not scan_calls, (
        "scanner.scan must not be called after a fatal local-processing "
        f"ingest failure; called with {scan_calls!r}"
    )
    # Nothing publishes at the destination.
    assert not (final_dest / "good.jpg").exists()
    # Staging stays intact for recovery.
    staging_root = tmp_path / "staging" / job_id / "Photos"
    assert staging_root.is_dir(), staging_root
    assert (staging_root / "good.jpg").is_file()


@pytest.mark.skip(reason="retired pipeline local-processing import/archive path")
def test_pipeline_local_processing_completes_when_cancel_during_archive(
    setup, tmp_path, monkeypatch
):
    """Regression: move_folder does not accept a cancellation signal, and
    tearing it down mid-flight would leave a partial archive at the
    destination. A Stop press once the archive begins must therefore be
    consumed so the job records "completed" rather than recording a
    "cancelled" status for a run that actually published its output.
    """
    app, db_path = setup

    final_parent = tmp_path / "nas_cancel"
    final_parent.mkdir()
    final_dest = final_parent / "Photos"

    src = tmp_path / "card_cancel"
    src.mkdir()
    Image.new("RGB", (16, 16), "white").save(src / "kept.jpg")

    import local_processing
    monkeypatch.setattr(local_processing, "MIN_DERIVED_OVERHEAD_BYTES", 0)
    monkeypatch.setattr(local_processing, "RESERVED_FREE_BYTES", 0)

    # Simulate the user clicking Stop while move_folder is mid-copy: wrap
    # the real move_folder so that, just as it begins, the job is added
    # to the runner's cancellation set. Without clear_cancellation in
    # archive_stage, JobRunner._run_job's atomic terminal check would
    # then record the run as "cancelled" even though the archive
    # committed and the originals were removed.
    import move as move_mod
    real_move_folder_fn = move_mod.move_folder

    runner = app._job_runner

    def cancelling_move_folder(db, folder_id, destination, **kwargs):
        # Resolve the current job id off the runner — we can't get it
        # passed in directly. There's exactly one running pipeline job at
        # this point, so pick it out of the runner's job map.
        with runner._lock:
            running = [
                jid for jid, j in runner._jobs.items()
                if j.get("status") == "running"
                and jid.startswith("pipeline-")
            ]
        assert len(running) == 1, running
        runner.cancel_job(running[0])
        return real_move_folder_fn(db, folder_id, destination, **kwargs)

    monkeypatch.setattr(move_mod, "move_folder", cancelling_move_folder)

    with app.test_client() as c:
        resp = c.post("/api/jobs/pipeline", json={
            "sources": [str(src)],
            "destination": str(final_dest),
            "local_processing": True,
            "folder_template": "",
            "skip_classify": True,
            "skip_extract_masks": True,
            "skip_regroup": True,
        })
        assert resp.status_code == 200
        job_id = resp.get_json()["job_id"]
        job = wait_for_job_via_client(c, job_id)

    # The archive committed, so the job must NOT record cancelled.
    assert job["status"] == "completed", job
    assert (final_dest / "kept.jpg").is_file()
    assert job["result"]["archive"]["final_destination"] == str(final_dest)


@pytest.mark.skip(reason="retired pipeline local-processing import/archive path")
def test_pipeline_local_processing_cancels_before_archive_commit(
    setup, tmp_path, monkeypatch
):
    """A Stop press that lands just before archive commit begins is still
    honorably cancellable because nothing has been published yet."""
    app, db_path = setup

    final_parent = tmp_path / "nas_cancel_before_commit"
    final_parent.mkdir()
    final_dest = final_parent / "Photos"

    src = tmp_path / "card_cancel_before_commit"
    src.mkdir()
    Image.new("RGB", (16, 16), "white").save(src / "kept.jpg")

    import local_processing
    monkeypatch.setattr(local_processing, "MIN_DERIVED_OVERHEAD_BYTES", 0)
    monkeypatch.setattr(local_processing, "RESERVED_FREE_BYTES", 0)

    runner = app._job_runner
    real_begin_uncancellable = runner.begin_uncancellable
    cancelled_once = {"done": False}

    def cancelling_begin_uncancellable(job_id):
        if not cancelled_once["done"]:
            cancelled_once["done"] = True
            assert runner.cancel_job(job_id) is True
        return real_begin_uncancellable(job_id)

    monkeypatch.setattr(
        runner, "begin_uncancellable", cancelling_begin_uncancellable,
    )

    with app.test_client() as c:
        resp = c.post("/api/jobs/pipeline", json={
            "sources": [str(src)],
            "destination": str(final_dest),
            "local_processing": True,
            "folder_template": "",
            "skip_classify": True,
            "skip_extract_masks": True,
            "skip_regroup": True,
        })
        assert resp.status_code == 200
        job_id = resp.get_json()["job_id"]
        job = wait_for_job_via_client(c, job_id)

    assert job["status"] == "cancelled", job
    assert not (final_dest / "kept.jpg").exists()

    from db import Database
    check_db = Database(db_path)
    folder_row = check_db.conn.execute(
        "SELECT id FROM folders WHERE path = ?",
        (str(tmp_path / "staging" / job_id / "Photos"),),
    ).fetchone()
    check_db.close()
    assert folder_row is None


@pytest.mark.skip(reason="retired pipeline local-processing import/archive path")
def test_pipeline_local_processing_failed_archive_reports_failed_even_after_cancel(
    setup, tmp_path, monkeypatch
):
    """Regression: clear_cancellation must consume the Stop flag BEFORE
    move_folder runs, not after. If clear is deferred to after a successful
    move, a Stop press that landed during a move which then FAILED (ENOSPC,
    rsync error, verify mismatch) would leave the cancel flag set; the
    outer JobRunner terminal check would record the run as "cancelled",
    masking the archive failure from the user. The job must report
    "failed" so the user sees the real outcome.
    """
    app, db_path = setup

    final_parent = tmp_path / "nas_cancel_fail"
    final_parent.mkdir()
    final_dest = final_parent / "Photos"

    src = tmp_path / "card_cancel_fail"
    src.mkdir()
    Image.new("RGB", (16, 16), "white").save(src / "kept.jpg")

    import local_processing
    monkeypatch.setattr(local_processing, "MIN_DERIVED_OVERHEAD_BYTES", 0)
    monkeypatch.setattr(local_processing, "RESERVED_FREE_BYTES", 0)

    # Simulate the user clicking Stop while move_folder is mid-copy AND the
    # move then fails (e.g. verification mismatch). Two requirements:
    # (1) clear_cancellation must already have run before move_folder is
    # invoked, so the cancel flag is gone; (2) when move_folder returns a
    # failure result, the job must report "failed", not "cancelled".
    import move as move_mod

    runner = app._job_runner

    def cancelling_and_failing_move_folder(db, folder_id, destination, **kwargs):
        with runner._lock:
            running = [
                jid for jid, j in runner._jobs.items()
                if j.get("status") == "running"
                and jid.startswith("pipeline-")
            ]
        assert len(running) == 1, running
        # cancel_job must be a no-op here: the archive stage has already
        # marked the job uncancellable via clear_cancellation. Without that
        # ordering, the Stop press below would land in _cancelled and
        # survive into _run_job's terminal check, recording "cancelled".
        accepted = runner.cancel_job(running[0])
        assert accepted is False, (
            "cancel_job accepted after the archive stage started; "
            "clear_cancellation must run BEFORE move_folder to make the "
            "job uncancellable for the duration of the commit"
        )
        # Now simulate the move itself failing — e.g. an rsync verify
        # mismatch. archive_stage raises and falls into the except branch.
        return {"moved": 0, "errors": [
            "Verification failed: 'kept.jpg' is missing or differs at the "
            "destination. Originals preserved."
        ]}

    monkeypatch.setattr(move_mod, "move_folder", cancelling_and_failing_move_folder)

    with app.test_client() as c:
        resp = c.post("/api/jobs/pipeline", json={
            "sources": [str(src)],
            "destination": str(final_dest),
            "local_processing": True,
            "folder_template": "",
            "skip_classify": True,
            "skip_extract_masks": True,
            "skip_regroup": True,
        })
        assert resp.status_code == 200
        job_id = resp.get_json()["job_id"]
        job = wait_for_job_via_client(c, job_id)

    # The archive raised — the run must report failed, not cancelled.
    assert job["status"] == "failed", job


@pytest.mark.skip(reason="retired pipeline local-processing import/archive path")
def test_pipeline_local_processing_post_commit_cleanup_error_reports_completed(
    setup, tmp_path, monkeypatch
):
    """Regression: post-commit rmtree failures must not flip the job to failed.

    ``move_folder`` repoints the catalog at ``final_destination`` before
    deleting the staging originals. If the post-commit rmtree raises (a
    locked file, a permission glitch in ``~/.vireo/staging``), the archive
    IS committed: files exist at the destination and the catalog points
    there. Previously the broad except in archive_stage caught the rmtree
    exception, marked the stage failed, and told the user "results remain
    in local staging" — but the files were at ``final_destination`` and a
    freshly created tracked folder row was now in the catalog. The fix
    distinguishes the post-commit cleanup error via ``move_folder``'s
    new ``cleanup_error`` return key and reports the job completed with a
    warning instead.
    """
    app, db_path = setup

    final_parent = tmp_path / "nas_post_commit"
    final_parent.mkdir()
    final_dest = final_parent / "Photos"

    src = tmp_path / "card_post_commit"
    src.mkdir()
    Image.new("RGB", (16, 16), "white").save(src / "kept.jpg")

    import local_processing
    monkeypatch.setattr(local_processing, "MIN_DERIVED_OVERHEAD_BYTES", 0)
    monkeypatch.setattr(local_processing, "RESERVED_FREE_BYTES", 0)

    import move as move_mod
    real_move_folder_fn = move_mod.move_folder

    def cleanup_failing_move_folder(db, folder_id, destination, **kwargs):
        # Run the real move (it commits) but inject a cleanup error via
        # rmtree. We do this by patching shutil.rmtree only for the
        # duration of this call, so the real verify/move_folder_path runs
        # but the final src delete raises.
        import shutil
        orig_rmtree = shutil.rmtree

        def raising_rmtree(path, *a, **k):
            raise OSError("permission denied: staging file locked")

        shutil.rmtree = raising_rmtree
        try:
            return real_move_folder_fn(db, folder_id, destination, **kwargs)
        finally:
            shutil.rmtree = orig_rmtree

    monkeypatch.setattr(move_mod, "move_folder", cleanup_failing_move_folder)

    with app.test_client() as c:
        resp = c.post("/api/jobs/pipeline", json={
            "sources": [str(src)],
            "destination": str(final_dest),
            "local_processing": True,
            "folder_template": "",
            "skip_classify": True,
            "skip_extract_masks": True,
            "skip_regroup": True,
        })
        assert resp.status_code == 200
        job_id = resp.get_json()["job_id"]
        job = wait_for_job_via_client(c, job_id)

    # The archive committed despite the staging cleanup error.
    assert job["status"] == "completed", job
    assert (final_dest / "kept.jpg").is_file()
    archive_result = job["result"]["archive"]
    assert archive_result["final_destination"] == str(final_dest)
    assert "cleanup_error" in archive_result
    assert job["result"]["errors"] == []


@pytest.mark.skip(reason="retired pipeline local-processing import/archive path")
def test_pipeline_local_processing_deindexes_staging_on_cancel_after_scan(
    setup, tmp_path, monkeypatch
):
    """Regression: when the user clicks Stop after scanner_stage has
    already indexed the local staging folder but before archive_stage runs,
    the cancel_watcher sets abort and every downstream stage skips with
    status "skipped" (not "failed"). archive_stage's abort/cancel
    early-return must still deindex the staging folder — otherwise the
    abandoned staging rows would gate ingest()'s known-hash skip on the
    next retry, letting that retry "successfully" archive an empty
    destination. The already_failed branch's deindex doesn't help here:
    cancellation marks stages "skipped", not "failed", so the existing
    failed-stage cleanup path never runs.
    """
    app, db_path = setup

    final_parent = tmp_path / "nas_cancel_pre"
    final_parent.mkdir()
    final_dest = final_parent / "Photos"

    src = tmp_path / "card_cancel_pre"
    src.mkdir()
    Image.new("RGB", (16, 16), "white").save(src / "kept.jpg")

    import local_processing
    monkeypatch.setattr(local_processing, "MIN_DERIVED_OVERHEAD_BYTES", 0)
    monkeypatch.setattr(local_processing, "RESERVED_FREE_BYTES", 0)

    # Trigger cancellation in scanner_stage's finally block, after the
    # staging folder has been scanned and the photo hashes committed. The
    # pipeline's cancel_watcher polls runner.is_cancelled and sets `abort`
    # within 0.25s, so by the time archive_stage runs it sees abort.is_set()
    # and takes the early-return path.
    import new_images
    runner = app._job_runner
    cancelled_once = {"done": False}
    real_invalidate = new_images.invalidate_new_images_after_scan

    def cancelling_invalidate(db, root):
        if not cancelled_once["done"]:
            with runner._lock:
                running = [
                    jid for jid, j in runner._jobs.items()
                    if j.get("status") == "running"
                    and jid.startswith("pipeline-")
                ]
            if running:
                runner.cancel_job(running[0])
                cancelled_once["done"] = True
        return real_invalidate(db, root)

    monkeypatch.setattr(
        new_images, "invalidate_new_images_after_scan",
        cancelling_invalidate,
    )

    with app.test_client() as c:
        resp = c.post("/api/jobs/pipeline", json={
            "sources": [str(src)],
            "destination": str(final_dest),
            "local_processing": True,
            "folder_template": "",
            "skip_classify": True,
            "skip_extract_masks": True,
            "skip_regroup": True,
        })
        assert resp.status_code == 200
        job_id = resp.get_json()["job_id"]
        job = wait_for_job_via_client(c, job_id)

    # archive must NOT publish files when cancelled before commit.
    assert not (final_dest / "kept.jpg").exists()
    # The terminal status reflects the cancellation, not a successful run.
    assert job["status"] != "completed", job

    # Staging files stay on disk so the user can recover.
    staging_file = (
        tmp_path / "staging" / job_id / "Photos" / "kept.jpg"
    )
    assert staging_file.is_file(), (
        f"staging files should remain when archive is skipped, "
        f"missing at {staging_file}"
    )

    # The staging folder must be removed from the catalog. Without the
    # abort branch's deindex, this row would survive and ingest()'s
    # known-hash gate would skip every file on retry — producing an empty
    # archive on the next run of the same source.
    from db import Database
    check_db = Database(db_path)
    staging_dir = str(staging_file.parent)
    folder_row = check_db.conn.execute(
        "SELECT id FROM folders WHERE path = ?", (staging_dir,),
    ).fetchone()
    assert folder_row is None, (
        f"staging folder must be deindexed when archive is skipped via "
        f"abort/cancel, still present at {staging_dir}"
    )
    photo_rows = check_db.conn.execute(
        "SELECT id FROM photos WHERE folder_id IN ("
        "  SELECT id FROM folders WHERE path LIKE ?"
        ")",
        (str(tmp_path / "staging" / job_id) + "%",),
    ).fetchall()
    assert not photo_rows, (
        "no photo rows should remain under the abandoned staging tree, "
        f"got {len(photo_rows)}"
    )
    check_db.close()


@pytest.mark.skip(reason="retired pipeline local-processing import/archive path")
def test_pipeline_local_processing_preflight_filters_duplicates(
    setup, tmp_path, monkeypatch
):
    """When skip_duplicates is on and the source is mostly already in the
    catalog, the storage preflight must measure only the bytes ingest would
    actually copy. Without the duplicate-aware re-check, the naive byte sum
    would set batching_required and abort an import that would fit in
    staging."""
    app, db_path = setup

    final_parent = tmp_path / "nas"
    final_parent.mkdir()
    final_dest = final_parent / "Photos"

    src = tmp_path / "card3"
    src.mkdir()
    Image.new("RGB", (16, 16), "white").save(src / "dup.jpg")
    Image.new("RGB", (16, 16), "red").save(src / "fresh.jpg")

    # Insert the dup file's hash into the catalog so skip_duplicates will
    # treat it as already-known. selected_source_files only walks the source
    # tree, so the photos table just needs the hash present.
    from db import Database
    from scanner import compute_file_hash
    dup_hash = compute_file_hash(str(src / "dup.jpg"))

    db = Database(db_path)
    seed_folder = tmp_path / "seed"
    seed_folder.mkdir()
    folder_id = db.add_folder(str(seed_folder))
    ws_id = db._active_workspace_id
    db.add_workspace_folder(ws_id, folder_id)
    db.add_photo(
        folder_id=folder_id, filename="dup.jpg", extension=".jpg",
        file_size=(src / "dup.jpg").stat().st_size, file_mtime=1.0,
        file_hash=dup_hash,
    )
    db.close()

    # Pick a disk_usage and reserve combination so that summing both files
    # would exceed the usable budget, but the fresh file alone fits.
    fresh_bytes = (src / "fresh.jpg").stat().st_size
    dup_bytes = (src / "dup.jpg").stat().st_size
    total_bytes = fresh_bytes + dup_bytes
    # required_bytes ≈ source_bytes * 2.25 here: staging and archive_parent
    # both resolve to tmp_path so storage_plan's same-device branch doubles
    # the source-byte allotment (originals in staging + originals at the
    # destination during copy-verify-delete) on top of the 0.25 derived
    # overhead. Pick free so the duplicate-filtered plan fits but the
    # naive plan doesn't.
    reserve = 4
    free = int(fresh_bytes * 2.5) + reserve + 1
    assert free < int(total_bytes * 2.25) + reserve, "free must exceed filtered need but not the naive need"

    from collections import namedtuple
    Usage = namedtuple("Usage", "total used free")

    import local_processing
    monkeypatch.setattr(local_processing, "MIN_DERIVED_OVERHEAD_BYTES", 0)
    monkeypatch.setattr(local_processing, "RESERVED_FREE_BYTES", reserve)
    monkeypatch.setattr(
        local_processing.shutil, "disk_usage",
        lambda path: Usage(total=free * 10, used=0, free=free),
    )

    with app.test_client() as c:
        resp = c.post("/api/jobs/pipeline", json={
            "sources": [str(src)],
            "destination": str(final_dest),
            "local_processing": True,
            "skip_duplicates": True,
            "folder_template": "",
            "skip_classify": True,
            "skip_extract_masks": True,
            "skip_regroup": True,
        })
        assert resp.status_code == 200
        job = wait_for_job_via_client(c, resp.get_json()["job_id"])

    assert job["status"] == "completed", job
    # Storage plan recorded on the job reflects the filtered bytes, not the
    # naive sum that would have aborted.
    plan = job["result"]["local_processing"]
    assert plan["batching_required"] is False, plan
    assert plan["source_bytes"] <= fresh_bytes, plan


@pytest.mark.skip(reason="retired pipeline local-processing import/archive path")
def test_pipeline_local_processing_plans_archive_credit_after_duplicate_filter(
    setup, tmp_path, monkeypatch
):
    """Storage preflight must plan archive credit from survivor files, not from
    skipped duplicates, before accepting a plan."""
    app, db_path = setup

    final_parent = tmp_path / "nas-filtered-credit"
    final_parent.mkdir()
    final_dest = final_parent / "Photos"
    final_dest.mkdir()

    src = tmp_path / "card-filtered-credit"
    src.mkdir()
    Image.new("RGB", (16, 16), "white").save(src / "dup.jpg")
    Image.new("RGB", (16, 16), "red").save(src / "fresh.jpg")
    shutil.copy2(src / "dup.jpg", final_dest / "dup.jpg")

    from db import Database
    from scanner import compute_file_hash
    dup_hash = compute_file_hash(str(src / "dup.jpg"))

    db = Database(db_path)
    seed_folder = tmp_path / "seed-filtered-credit"
    seed_folder.mkdir()
    folder_id = db.add_folder(str(seed_folder))
    ws_id = db._active_workspace_id
    db.add_workspace_folder(ws_id, folder_id)
    db.add_photo(
        folder_id=folder_id, filename="dup.jpg", extension=".jpg",
        file_size=(src / "dup.jpg").stat().st_size, file_mtime=1.0,
        file_hash=dup_hash,
    )
    db.close()

    fresh_bytes = (src / "fresh.jpg").stat().st_size
    dup_bytes = (src / "dup.jpg").stat().st_size
    total_bytes = fresh_bytes + dup_bytes
    reserve = 4
    free = int(fresh_bytes * 2.5) + reserve + 1
    assert free < int(total_bytes * 1.25 + fresh_bytes) + reserve

    from collections import namedtuple
    Usage = namedtuple("Usage", "total used free")

    import local_processing
    real_storage_plan = local_processing.storage_plan
    storage_calls = []

    def recording_storage_plan(staging_dir, source_bytes, **kwargs):
        storage_calls.append({
            "source_bytes": source_bytes,
            "archive_existing_bytes": kwargs.get("archive_existing_bytes"),
        })
        return real_storage_plan(staging_dir, source_bytes, **kwargs)

    monkeypatch.setattr(local_processing, "MIN_DERIVED_OVERHEAD_BYTES", 0)
    monkeypatch.setattr(local_processing, "RESERVED_FREE_BYTES", reserve)
    monkeypatch.setattr(local_processing, "storage_plan", recording_storage_plan)
    monkeypatch.setattr(
        local_processing.shutil, "disk_usage",
        lambda path: Usage(total=free * 10, used=0, free=free),
    )

    with app.test_client() as c:
        resp = c.post("/api/jobs/pipeline", json={
            "sources": [str(src)],
            "destination": str(final_dest),
            "local_processing": True,
            "skip_duplicates": True,
            "folder_template": "",
            "skip_classify": True,
            "skip_extract_masks": True,
            "skip_regroup": True,
        })
        assert resp.status_code == 200
        job = wait_for_job_via_client(c, resp.get_json()["job_id"])

    assert job["status"] == "completed", job
    assert len(storage_calls) == 1
    assert storage_calls[0]["source_bytes"] == fresh_bytes
    assert storage_calls[0]["archive_existing_bytes"] == 0


@pytest.mark.skip(reason="retired pipeline local-processing import/archive path")
def test_pipeline_local_processing_preflight_probes_existing_final_destination(
    setup, tmp_path, monkeypatch
):
    """Existing archive folders may be mount roots and must be probed directly."""
    app, _db_path = setup

    src = tmp_path / "card4"
    src.mkdir()
    Image.new("RGB", (16, 16), "blue").save(src / "fresh.jpg")

    final_parent = tmp_path / "Volumes"
    final_parent.mkdir()
    final_dest = final_parent / "Archive"
    final_dest.mkdir()

    import local_processing

    monkeypatch.setattr(local_processing, "MIN_DERIVED_OVERHEAD_BYTES", 0)
    monkeypatch.setattr(local_processing, "RESERVED_FREE_BYTES", 0)
    real_storage_plan = local_processing.storage_plan
    archive_paths = []

    def recording_storage_plan(staging_dir, source_bytes, **kwargs):
        archive_paths.append(kwargs.get("archive_parent"))
        return real_storage_plan(staging_dir, source_bytes, **kwargs)

    monkeypatch.setattr(local_processing, "storage_plan", recording_storage_plan)

    with app.test_client() as c:
        resp = c.post("/api/jobs/pipeline", json={
            "sources": [str(src)],
            "destination": str(final_dest),
            "local_processing": True,
            "folder_template": "",
            "skip_classify": True,
            "skip_extract_masks": True,
            "skip_regroup": True,
        })
        assert resp.status_code == 200
        job = wait_for_job_via_client(c, resp.get_json()["job_id"])

    assert job["status"] == "completed", job
    assert archive_paths
    assert set(archive_paths) == {str(final_dest)}


@pytest.mark.skip(reason="retired pipeline local-processing import/archive path")
def test_pipeline_local_processing_rejects_existing_file_archive_destination(
    setup, tmp_path, monkeypatch
):
    app, _db_path = setup

    src = tmp_path / "card5"
    src.mkdir()
    Image.new("RGB", (16, 16), "green").save(src / "fresh.jpg")

    final_dest = tmp_path / "Archive"
    final_dest.write_text("not a directory")

    import local_processing

    monkeypatch.setattr(local_processing, "MIN_DERIVED_OVERHEAD_BYTES", 0)
    monkeypatch.setattr(local_processing, "RESERVED_FREE_BYTES", 0)

    with app.test_client() as c:
        resp = c.post("/api/jobs/pipeline", json={
            "sources": [str(src)],
            "destination": str(final_dest),
            "local_processing": True,
            "folder_template": "",
            "skip_classify": True,
            "skip_extract_masks": True,
            "skip_regroup": True,
        })
        assert resp.status_code == 200
        job = wait_for_job_via_client(c, resp.get_json()["job_id"])

    assert job["status"] == "failed", job
    error_text = (job.get("error") or "") + str(job.get("result", ""))
    assert "not a directory" in error_text


@pytest.mark.skip(reason="retired pipeline local-processing import/archive path")
def test_pipeline_local_processing_rejects_broken_symlink_archive_destination(
    setup, tmp_path, monkeypatch
):
    """A broken/dangling symlink left at ``final_destination`` — e.g. by an
    unmounted or moved archive root — must be rejected up front. The
    earlier ``os.path.exists`` guard followed the symlink and returned
    False for a dangling target, so the pipeline staged and processed
    every source before ``move_folder`` ultimately failed trying to
    create a directory at a pathname already occupied by the symlink
    entry. The lexists guard catches the entry regardless of whether
    its target resolves."""
    app, _db_path = setup

    src = tmp_path / "card-broken-symlink"
    src.mkdir()
    Image.new("RGB", (16, 16), "green").save(src / "fresh.jpg")

    archive_parent = tmp_path / "archive-parent"
    archive_parent.mkdir()
    final_dest = archive_parent / "Archive"
    missing_target = tmp_path / "missing-mount-point"
    try:
        os.symlink(str(missing_target), str(final_dest))
    except (OSError, NotImplementedError):
        pytest.skip("symlinks not supported on this filesystem")
    assert os.path.lexists(str(final_dest))
    assert not os.path.exists(str(final_dest))

    import local_processing

    monkeypatch.setattr(local_processing, "MIN_DERIVED_OVERHEAD_BYTES", 0)
    monkeypatch.setattr(local_processing, "RESERVED_FREE_BYTES", 0)

    with app.test_client() as c:
        resp = c.post("/api/jobs/pipeline", json={
            "sources": [str(src)],
            "destination": str(final_dest),
            "local_processing": True,
            "folder_template": "",
            "skip_classify": True,
            "skip_extract_masks": True,
            "skip_regroup": True,
        })
        assert resp.status_code == 200
        job = wait_for_job_via_client(c, resp.get_json()["job_id"])

    assert job["status"] == "failed", job
    error_text = (job.get("error") or "") + str(job.get("result", ""))
    assert "not a directory" in error_text


@pytest.mark.skip(reason="retired pipeline local-processing import/archive path")
def test_pipeline_local_processing_rejects_archive_path_conflicts(
    setup, tmp_path, monkeypatch
):
    app, _db_path = setup

    src = tmp_path / "card-conflict"
    src.mkdir()
    source_path = src / "fresh.jpg"
    Image.new("RGB", (16, 16), "green").save(source_path)

    final_parent = tmp_path / "archive-conflict-parent"
    final_parent.mkdir()
    final_dest = final_parent / "Archive"
    final_dest.mkdir()
    (final_dest / "fresh.jpg").write_bytes(b"x" * source_path.stat().st_size)

    import local_processing

    monkeypatch.setattr(local_processing, "MIN_DERIVED_OVERHEAD_BYTES", 0)
    monkeypatch.setattr(local_processing, "RESERVED_FREE_BYTES", 0)

    with app.test_client() as c:
        resp = c.post("/api/jobs/pipeline", json={
            "sources": [str(src)],
            "destination": str(final_dest),
            "local_processing": True,
            "folder_template": "",
            "skip_classify": True,
            "skip_extract_masks": True,
            "skip_regroup": True,
        })
        assert resp.status_code == 200
        job = wait_for_job_via_client(c, resp.get_json()["job_id"])

    assert job["status"] == "failed", job
    error_text = (job.get("error") or "") + str(job.get("result", ""))
    assert "different files at the same import paths" in error_text


@pytest.mark.skip(reason="retired pipeline local-processing import/archive path")
def test_pipeline_local_processing_reports_incomplete_archive_files(
    setup, tmp_path, monkeypatch
):
    app, _db_path = setup

    src = tmp_path / "card-incomplete"
    src.mkdir()
    Image.new("RGB", (16, 16), "green").save(src / "fresh.jpg")

    final_parent = tmp_path / "archive-incomplete-parent"
    final_parent.mkdir()
    final_dest = final_parent / "Archive"
    final_dest.mkdir()
    (final_dest / "fresh.jpg").write_bytes(b"")

    import local_processing

    monkeypatch.setattr(local_processing, "MIN_DERIVED_OVERHEAD_BYTES", 0)
    monkeypatch.setattr(local_processing, "RESERVED_FREE_BYTES", 0)

    with app.test_client() as c:
        resp = c.post("/api/jobs/pipeline", json={
            "sources": [str(src)],
            "destination": str(final_dest),
            "local_processing": True,
            "folder_template": "",
            "skip_classify": True,
            "skip_extract_masks": True,
            "skip_regroup": True,
        })
        assert resp.status_code == 200
        job = wait_for_job_via_client(c, resp.get_json()["job_id"])

    assert job["status"] == "failed", job
    error_text = (job.get("error") or "") + str(job.get("result", ""))
    assert "1 empty unindexed file" in error_text
    assert "interrupted previous archive copy" in error_text
    assert "will not suffix around likely corrupt archive files" in error_text


@pytest.mark.skip(reason="retired pipeline local-processing import/archive path")
def test_pipeline_local_processing_recognises_indexed_archive_via_alias(
    setup, tmp_path, monkeypatch
):
    """When the destination is a symlink alias of a tracked archive folder,
    the indexed-path lookup must fold the alias so cataloged rows still
    count as indexed. Otherwise a zero-byte tracked archive file would be
    mis-reported as unindexed 'interrupted previous archive copy' debris,
    telling the user to delete a file Vireo already manages."""
    app, db_path = setup

    src = tmp_path / "card-alias"
    src.mkdir()
    Image.new("RGB", (16, 16), "green").save(src / "fresh.jpg")

    real_parent = tmp_path / "archive-alias-real-parent"
    real_parent.mkdir()
    real_dest = real_parent / "Archive"
    real_dest.mkdir()
    # Zero bytes so it hits the "empty" branch of archive_conflict_report
    # when it (wrongly) looks unindexed.
    (real_dest / "fresh.jpg").write_bytes(b"")

    alias_parent = tmp_path / "archive-alias-link-parent"
    alias_parent.mkdir()
    alias_dest = alias_parent / "Archive"
    try:
        os.symlink(real_dest, alias_dest, target_is_directory=True)
    except (OSError, NotImplementedError):
        pytest.skip("symlink creation not supported on this platform")

    from db import Database
    db = Database(db_path)
    folder_id = db.add_folder(str(real_dest))
    ws_id = db._active_workspace_id
    db.add_workspace_folder(ws_id, folder_id)
    db.add_photo(
        folder_id=folder_id, filename="fresh.jpg", extension=".jpg",
        file_size=0, file_mtime=1.0, file_hash=None,
    )
    db.close()

    import local_processing

    monkeypatch.setattr(local_processing, "MIN_DERIVED_OVERHEAD_BYTES", 0)
    monkeypatch.setattr(local_processing, "RESERVED_FREE_BYTES", 0)

    with app.test_client() as c:
        resp = c.post("/api/jobs/pipeline", json={
            "sources": [str(src)],
            "destination": str(alias_dest),
            "local_processing": True,
            "folder_template": "",
            "skip_classify": True,
            "skip_extract_masks": True,
            "skip_regroup": True,
        })
        assert resp.status_code == 200
        job = wait_for_job_via_client(c, resp.get_json()["job_id"])

    assert job["status"] == "failed", job
    error_text = (job.get("error") or "") + str(job.get("result", ""))
    # The indexed archive row should be recognised through the alias, so
    # the preflight must not label it as unindexed failed-copy debris.
    assert "interrupted previous archive copy" not in error_text
    assert "different files at the same import paths" in error_text


@pytest.mark.skip(reason="retired pipeline local-processing import/archive path")
def test_pipeline_local_processing_limits_incomplete_examples_to_incomplete(
    setup, tmp_path, monkeypatch
):
    """Mixed incomplete + real-conflict runs must only list the incomplete
    paths as examples in the 'interrupted previous archive copy' message —
    the message's wording only describes empty/partial files, so leaking
    full-content conflict paths into the example list points the user at
    files that are neither empty nor truncated."""
    app, _db_path = setup

    src = tmp_path / "card-mixed"
    src.mkdir()
    Image.new("RGB", (16, 16), "green").save(src / "incomplete.jpg")
    Image.new("RGB", (16, 16), "blue").save(src / "conflict.jpg")

    final_parent = tmp_path / "archive-mixed-parent"
    final_parent.mkdir()
    final_dest = final_parent / "Archive"
    final_dest.mkdir()
    # Empty (incomplete) archive file for the first source.
    (final_dest / "incomplete.jpg").write_bytes(b"")
    # Different-content archive file at least as big as the source so
    # the conflict is a real content mismatch, not partial debris.
    src_size = (src / "conflict.jpg").stat().st_size
    (final_dest / "conflict.jpg").write_bytes(b"x" * max(src_size, 1))

    import local_processing

    monkeypatch.setattr(local_processing, "MIN_DERIVED_OVERHEAD_BYTES", 0)
    monkeypatch.setattr(local_processing, "RESERVED_FREE_BYTES", 0)

    with app.test_client() as c:
        resp = c.post("/api/jobs/pipeline", json={
            "sources": [str(src)],
            "destination": str(final_dest),
            "local_processing": True,
            "folder_template": "",
            "skip_classify": True,
            "skip_extract_masks": True,
            "skip_regroup": True,
        })
        assert resp.status_code == 200
        job = wait_for_job_via_client(c, resp.get_json()["job_id"])

    assert job["status"] == "failed", job
    error_text = (job.get("error") or "") + str(job.get("result", ""))
    assert "interrupted previous archive copy" in error_text
    assert "incomplete.jpg" in error_text
    # The conflict-only file must not appear in the incomplete branch's
    # example list — the message wording is about empty/partial files.
    assert "conflict.jpg" not in error_text


@pytest.mark.skip(reason="retired pipeline local-processing import/archive path")
def test_pipeline_local_processing_conflict_preflight_skips_known_duplicates(
    setup, tmp_path, monkeypatch
):
    """A source whose hash is already in the catalog is skipped by ingest
    before staging, so a same-name different-content file at the archive
    must not cause the preflight to abort the run."""
    app, db_path = setup

    src = tmp_path / "card-dup-conflict"
    src.mkdir()
    dup_path = src / "dup.jpg"
    Image.new("RGB", (16, 16), "white").save(dup_path)
    fresh_path = src / "fresh.jpg"
    Image.new("RGB", (16, 16), "red").save(fresh_path)

    from db import Database
    from scanner import compute_file_hash

    dup_hash = compute_file_hash(str(dup_path))

    db = Database(db_path)
    seed_folder = tmp_path / "seed"
    seed_folder.mkdir()
    folder_id = db.add_folder(str(seed_folder))
    ws_id = db._active_workspace_id
    db.add_workspace_folder(ws_id, folder_id)
    db.add_photo(
        folder_id=folder_id, filename="dup.jpg", extension=".jpg",
        file_size=dup_path.stat().st_size, file_mtime=1.0,
        file_hash=dup_hash,
    )
    db.close()

    final_parent = tmp_path / "archive-parent"
    final_parent.mkdir()
    final_dest = final_parent / "Archive"
    final_dest.mkdir()
    # Same archive-relative path as the dup source, different content.
    # Without the duplicate filter the preflight would reject this run.
    (final_dest / "dup.jpg").write_bytes(b"different existing bytes")

    import local_processing
    monkeypatch.setattr(local_processing, "MIN_DERIVED_OVERHEAD_BYTES", 0)
    monkeypatch.setattr(local_processing, "RESERVED_FREE_BYTES", 0)

    with app.test_client() as c:
        resp = c.post("/api/jobs/pipeline", json={
            "sources": [str(src)],
            "destination": str(final_dest),
            "local_processing": True,
            "skip_duplicates": True,
            "folder_template": "",
            "skip_classify": True,
            "skip_extract_masks": True,
            "skip_regroup": True,
        })
        assert resp.status_code == 200
        job = wait_for_job_via_client(c, resp.get_json()["job_id"])

    error_text = (job.get("error") or "") + str(job.get("result", ""))
    assert "different files at the same import paths" not in error_text
    assert job["status"] == "completed", job


@pytest.mark.skip(reason="retired pipeline local-processing import/archive path")
def test_pipeline_local_processing_credits_existing_archive_for_resume(
    setup, tmp_path, monkeypatch
):
    """When a previous archive attempt left bytes at the destination, the
    preflight calls storage_plan with archive_existing_bytes set so the
    delta fits even when the destination volume is tight."""
    app, _db_path = setup

    src = tmp_path / "card-resume"
    src.mkdir()
    source_file = src / "fresh.jpg"
    Image.new("RGB", (16, 16), "red").save(source_file)

    final_parent = tmp_path / "archive-parent"
    final_parent.mkdir()
    final_dest = final_parent / "Archive"
    final_dest.mkdir()
    # Seed the destination with the exact file that a previous archive
    # attempt would have left behind so the resume credit is valid.
    shutil.copy2(source_file, final_dest / "fresh.jpg")

    import local_processing

    real_storage_plan = local_processing.storage_plan
    credits: list = []

    def recording_storage_plan(staging_dir, source_bytes, **kwargs):
        credits.append(kwargs.get("archive_existing_bytes"))
        return real_storage_plan(staging_dir, source_bytes, **kwargs)

    monkeypatch.setattr(local_processing, "storage_plan", recording_storage_plan)

    with app.test_client() as c:
        resp = c.post("/api/jobs/pipeline", json={
            "sources": [str(src)],
            "destination": str(final_dest),
            "local_processing": True,
            "folder_template": "",
            "skip_classify": True,
            "skip_extract_masks": True,
            "skip_regroup": True,
        })
        assert resp.status_code == 200
        job = wait_for_job_via_client(c, resp.get_json()["job_id"])

    assert credits, "storage_plan was not called"
    assert all(c is not None for c in credits)
    # Credit equals the matching source bytes already at the destination.
    assert credits[0] == source_file.stat().st_size
    assert job["status"] == "completed", job


@pytest.mark.skip(reason="retired pipeline local-processing import/archive path")
def test_pipeline_local_processing_does_not_credit_unrelated_archive_content(
    setup, tmp_path, monkeypatch
):
    """Existing destination bytes only count when they match selected source
    files at the same archive-relative path."""
    app, _db_path = setup

    src = tmp_path / "card-unrelated"
    src.mkdir()
    Image.new("RGB", (16, 16), "red").save(src / "fresh.jpg")

    final_parent = tmp_path / "archive-parent"
    final_parent.mkdir()
    final_dest = final_parent / "Archive"
    final_dest.mkdir()
    (final_dest / "unrelated.jpg").write_bytes(b"existing-payload")

    import local_processing

    real_storage_plan = local_processing.storage_plan
    credits: list = []

    def recording_storage_plan(staging_dir, source_bytes, **kwargs):
        credits.append(kwargs.get("archive_existing_bytes"))
        return real_storage_plan(staging_dir, source_bytes, **kwargs)

    monkeypatch.setattr(local_processing, "storage_plan", recording_storage_plan)

    with app.test_client() as c:
        resp = c.post("/api/jobs/pipeline", json={
            "sources": [str(src)],
            "destination": str(final_dest),
            "local_processing": True,
            "folder_template": "",
            "skip_classify": True,
            "skip_extract_masks": True,
            "skip_regroup": True,
        })
        assert resp.status_code == 200
        job = wait_for_job_via_client(c, resp.get_json()["job_id"])

    assert credits, "storage_plan was not called"
    assert credits[0] == 0
    assert job["status"] == "completed", job


def test_import_full_scan_only_returns_job_id(setup):
    """copy=false skips ingest, just scans the source folder."""
    app, db_path = setup
    src = tempfile.mkdtemp()
    try:
        jpeg_bytes = bytes([
            0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x10, 0x4A, 0x46, 0x49, 0x46, 0x00,
            0x01, 0x01, 0x00, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00, 0xFF, 0xD9
        ])
        with open(os.path.join(src, "test.jpg"), "wb") as f:
            f.write(jpeg_bytes)

        with app.test_client() as c:
            resp = c.post("/api/jobs/import-full", json={
                "source": src,
                "copy": False,
            })
            data = resp.get_json()
            assert resp.status_code == 200
            assert "job_id" in data
    finally:
        shutil.rmtree(src, ignore_errors=True)


def test_import_full_scan_only_no_destination_required(setup):
    """copy=false does not require destination."""
    app, db_path = setup
    src = tempfile.mkdtemp()
    try:
        with app.test_client() as c:
            resp = c.post("/api/jobs/import-full", json={
                "source": src,
                "copy": False,
            })
            assert resp.status_code == 200
    finally:
        shutil.rmtree(src, ignore_errors=True)


def test_import_full_copy_true_still_requires_destination(setup):
    """copy=true (explicit) still requires destination."""
    app, db_path = setup
    src = tempfile.mkdtemp()
    try:
        with app.test_client() as c:
            resp = c.post("/api/jobs/import-full", json={
                "source": src,
                "copy": True,
            })
            assert resp.status_code == 400
    finally:
        shutil.rmtree(src, ignore_errors=True)


def test_pipeline_rejects_sources_list(setup):
    """Process cannot admit filesystem paths; callers must use Import."""
    app, db_path = setup
    src1 = tempfile.mkdtemp()
    src2 = tempfile.mkdtemp()
    try:
        # Create minimal JPEG in each
        jpeg_bytes = bytes([
            0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x10, 0x4A, 0x46, 0x49, 0x46, 0x00,
            0x01, 0x01, 0x00, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00, 0xFF, 0xD9
        ])
        for src in [src1, src2]:
            with open(os.path.join(src, "test.jpg"), "wb") as f:
                f.write(jpeg_bytes)

        with app.test_client() as c:
            resp = c.post("/api/jobs/pipeline", json={
                "sources": [src1, src2],
                "skip_classify": True,
                "skip_extract_masks": True,
                "skip_regroup": True,
            })
            assert resp.status_code == 400
            assert "use Import" in resp.get_json()["error"]
    finally:
        shutil.rmtree(src1, ignore_errors=True)
        shutil.rmtree(src2, ignore_errors=True)


def test_pipeline_accepts_skip_classify(setup):
    """Pipeline endpoint should accept skip_classify parameter."""
    app, db_path = setup
    with app.test_client() as c:
        resp = c.post("/api/jobs/pipeline", json={
            "collection_id": 1,
            "skip_classify": True,
        })
        assert resp.status_code == 200


def test_pipeline_accepts_preview_max_size(setup):
    """Pipeline endpoint should accept preview_max_size parameter."""
    app, db_path = setup
    with app.test_client() as c:
        resp = c.post("/api/jobs/pipeline", json={
            "collection_id": 1,
            "preview_max_size": 2560,
        })
        assert resp.status_code == 200


def test_destination_preview_returns_folder_structure(setup, tmp_path):
    app, db_path = setup
    src = tmp_path / "src"
    dst = tmp_path / "dst"
    src.mkdir()
    dst.mkdir()

    img = Image.new("RGB", (100, 100))
    img.save(str(src / "photo.jpg"))
    mtime = datetime(2026, 3, 25, 10, 0, 0).timestamp()
    os.utime(str(src / "photo.jpg"), (mtime, mtime))

    with app.test_client() as c:
        resp = c.post("/api/import/destination-preview", json={
            "sources": [str(src)],
            "destination": str(dst),
            "folder_template": "%Y/%Y-%m-%d",
        })
        data = resp.get_json()
        assert resp.status_code == 200
        assert data["total_photos"] == 1
        assert data["total_folders"] == 1
        assert data["new_folders"] == 1
        assert len(data["folders"]) == 1
        assert data["folders"][0]["path"] == "2026/2026-03-25"
        assert data["folders"][0]["full_path"] == str(dst / "2026" / "2026-03-25")
        assert data["folders"][0]["exists"] is False


def test_destination_preview_requires_sources(setup):
    app, _ = setup
    with app.test_client() as c:
        resp = c.post("/api/import/destination-preview", json={
            "destination": "/tmp/dst",
        })
        assert resp.status_code == 400


def test_destination_preview_requires_destination(setup, tmp_path):
    app, _ = setup
    src = tmp_path / "src"
    src.mkdir()
    with app.test_client() as c:
        resp = c.post("/api/import/destination-preview", json={
            "sources": [str(src)],
        })
        assert resp.status_code == 400


def test_destination_preview_rejects_traversal_template(setup, tmp_path):
    app, _ = setup
    src = tmp_path / "src"
    src.mkdir()
    with app.test_client() as c:
        resp = c.post("/api/import/destination-preview", json={
            "sources": [str(src)],
            "destination": str(tmp_path / "dst"),
            "folder_template": "../escape/%Y",
        })
        assert resp.status_code == 400
        assert "relative path" in resp.get_json()["error"]


def test_destination_preview_rejects_absolute_template(setup, tmp_path):
    app, _ = setup
    src = tmp_path / "src"
    src.mkdir()
    with app.test_client() as c:
        resp = c.post("/api/import/destination-preview", json={
            "sources": [str(src)],
            "destination": str(tmp_path / "dst"),
            "folder_template": "/tmp/%Y",
        })
        assert resp.status_code == 400


def test_destination_preview_flags_managed_archive(setup, tmp_path):
    """When the destination is an already-tracked Vireo folder, the preview
    surfaces it as an existing managed archive with its catalog photo count,
    so the UI can frame the import as a merge instead of a fresh copy."""
    app, db_path = setup

    # A real on-disk archive base that the catalog already manages, with a
    # prior shoot under a date subfolder.
    archive = tmp_path / "arch" / "USA"
    prior = archive / "2025" / "2025-01-01"
    prior.mkdir(parents=True)
    p1 = prior / "old-1.jpg"
    p2 = prior / "old-2.jpg"
    Image.new("RGB", (16, 16), "green").save(p1)
    Image.new("RGB", (16, 16), "olive").save(p2)

    # A second date subfolder under the same base whose folder row is
    # 'missing' (e.g. its files went offline). Ingest treats the archive as
    # status IN ('ok','partial'), so these photos must NOT count toward the
    # callout's "N photos" even though their rows exist in the catalog.
    gone = archive / "2024" / "2024-01-01"
    gone.mkdir(parents=True)
    g1 = gone / "gone-1.jpg"
    Image.new("RGB", (16, 16), "gray").save(g1)

    from db import Database
    seed = Database(db_path)
    base_id = seed.add_folder(str(archive))
    prior_id = seed.add_folder(str(prior), parent_id=base_id, workspace_root=False)
    for f in (p1, p2):
        seed.add_photo(prior_id, f.name, ".jpg", f.stat().st_size, f.stat().st_mtime)
    gone_id = seed.add_folder(str(gone), parent_id=base_id, workspace_root=False)
    seed.add_photo(gone_id, g1.name, ".jpg", g1.stat().st_size, g1.stat().st_mtime)
    seed.conn.execute(
        "UPDATE folders SET status = 'missing' WHERE id = ?", (gone_id,)
    )
    seed.conn.commit()
    seed.close()

    # A fresh source card whose files aren't in the archive yet.
    src = tmp_path / "card"
    src.mkdir()
    new = src / "new.jpg"
    Image.new("RGB", (16, 16), "blue").save(new)
    mtime = datetime(2026, 6, 30, 9, 30, 0).timestamp()
    os.utime(str(new), (mtime, mtime))

    with app.test_client() as c:
        resp = c.post("/api/import/destination-preview", json={
            "sources": [str(src)],
            "destination": str(archive),
            "folder_template": "%Y/%Y-%m-%d",
        })
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["managed_archive"] is not None
        assert data["managed_archive"]["path"] == str(archive)
        # Only the two ok-status photos count; the missing folder's photo is
        # excluded (matches what ingest considers "the archive").
        assert data["managed_archive"]["photo_count"] == 2


def test_destination_preview_fresh_destination_has_no_managed_archive(setup, tmp_path):
    """A destination that isn't at/inside any tracked folder reports no
    managed archive, so the UI presents it as a fresh copy target."""
    app, _ = setup
    src = tmp_path / "card"
    src.mkdir()
    new = src / "new.jpg"
    Image.new("RGB", (16, 16), "blue").save(new)
    mtime = datetime(2026, 6, 30, 9, 30, 0).timestamp()
    os.utime(str(new), (mtime, mtime))

    with app.test_client() as c:
        resp = c.post("/api/import/destination-preview", json={
            "sources": [str(src)],
            "destination": str(tmp_path / "fresh"),
            "folder_template": "%Y/%Y-%m-%d",
        })
        assert resp.status_code == 200
        assert resp.get_json()["managed_archive"] is None


def test_destination_preview_inside_managed_archive_flags_ancestor(setup, tmp_path):
    """A destination NESTED inside a tracked root surfaces the ancestor archive
    (and its full photo count), not the nested path — so importing into a new
    subfolder of a managed root is still flagged as a merge."""
    app, db_path = setup

    archive = tmp_path / "arch" / "USA"
    prior = archive / "2025" / "2025-01-01"
    prior.mkdir(parents=True)
    p1 = prior / "old-1.jpg"
    Image.new("RGB", (16, 16), "green").save(p1)

    from db import Database
    seed = Database(db_path)
    base_id = seed.add_folder(str(archive))
    prior_id = seed.add_folder(str(prior), parent_id=base_id, workspace_root=False)
    seed.add_photo(prior_id, p1.name, ".jpg", p1.stat().st_size, p1.stat().st_mtime)
    seed.close()

    src = tmp_path / "card"
    src.mkdir()
    new = src / "new.jpg"
    Image.new("RGB", (16, 16), "blue").save(new)
    mtime = datetime(2026, 6, 30, 9, 30, 0).timestamp()
    os.utime(str(new), (mtime, mtime))

    nested = archive / "NewShoot"

    with app.test_client() as c:
        resp = c.post("/api/import/destination-preview", json={
            "sources": [str(src)],
            "destination": str(nested),
            "folder_template": "%Y/%Y-%m-%d",
        })
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["managed_archive"] is not None
        assert data["managed_archive"]["path"] == str(archive)
        assert data["managed_archive"]["photo_count"] == 1


def _wait_for_job(client, job_id, timeout=30.0):
    """Poll the job-status endpoint until the job completes or fails."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        resp = client.get(f"/api/jobs/{job_id}")
        data = resp.get_json()
        if data["status"] in ("completed", "failed"):
            return data
        time.sleep(0.05)
    raise AssertionError(f"job {job_id} did not finish within {timeout}s")


def test_import_full_copy_restricts_scan_to_new_subfolders(setup, tmp_path, monkeypatch):
    """Regression: importing with copy=true into a destination that already
    contains a large unrelated subtree must NOT walk the whole destination.

    Bug: ``api_job_import_full`` used ``scan_target = destination`` with no
    restriction, so after copying ~2.2k RAWs into two dated subfolders the
    scanner walked the full 59k-file destination tree. Fix: pass
    ``restrict_dirs`` to ``scanner.scan`` derived from
    ``ingest_result["copied_paths"]`` so only the touched folders are
    re-enumerated. This test patches scanner.scan to capture the call and
    asserts the unrelated subtree stays out of ``restrict_dirs``.
    """
    app, _ = setup
    monkeypatch.setenv("HOME", str(tmp_path))

    # Destination already has an unrelated subtree — this is the "59k files"
    # case in miniature. It MUST NOT end up in restrict_dirs.
    dest = tmp_path / "dest"
    dest.mkdir()
    unrelated = dest / "2019" / "2019-01-01"
    unrelated.mkdir(parents=True)
    for i in range(3):
        Image.new("RGB", (50, 50), "blue").save(str(unrelated / f"old_{i}.jpg"))

    # Source is a fresh folder of new photos stamped with 2024 mtime so
    # ingest routes them into new YYYY/YYYY-MM-DD subfolders.
    src = tmp_path / "source"
    src.mkdir()
    for i in range(2):
        fpath = src / f"new_{i}.jpg"
        Image.new("RGB", (60, 60), "red").save(str(fpath))
        mtime = datetime(2024, 6, 15, 12, 0, 0).timestamp()
        os.utime(str(fpath), (mtime, mtime))

    # Patch scanner.scan where app.py looks it up at runtime. app.py does
    # ``from scanner import scan as do_scan`` inside the job's work(), so the
    # target is the ``scanner`` module's attribute.
    import scanner as scanner_mod
    calls = []
    original = scanner_mod.scan

    def tracking_scan(root, *args, **kwargs):
        calls.append({
            "root": str(root),
            "restrict_dirs": kwargs.get("restrict_dirs"),
        })
        return original(root, *args, **kwargs)

    with patch.object(scanner_mod, "scan", tracking_scan):
        with app.test_client() as c:
            resp = c.post("/api/jobs/import-full", json={
                "source": str(src),
                "destination": str(dest),
                "copy": True,
            })
            assert resp.status_code == 200
            job_id = resp.get_json()["job_id"]
            status = _wait_for_job(c, job_id)
            assert status["status"] == "completed", status

    assert calls, "scanner.scan was not invoked"
    call = calls[-1]
    assert call["root"] == str(dest), (
        f"scan root should be destination for folder hierarchy, got {call['root']!r}"
    )
    restrict = call["restrict_dirs"]
    assert restrict is not None, (
        "restrict_dirs should be populated from copied_paths so the scanner "
        "doesn't re-walk the entire destination tree after ingest."
    )
    restrict_set = {os.path.normpath(d) for d in restrict}
    assert os.path.normpath(str(unrelated)) not in restrict_set, (
        f"Unrelated pre-existing folder {unrelated!r} must not be scanned; "
        f"restrict_dirs={restrict_set!r}"
    )
    # The new dated folder (2024/2024-06-15) is what ingest created — it
    # should be the only thing we re-scan.
    expected_new = dest / "2024" / "2024-06-15"
    assert os.path.normpath(str(expected_new)) in restrict_set, (
        f"Expected new folder {expected_new!r} in restrict_dirs; got {restrict_set!r}"
    )


def test_import_full_copy_false_still_scans_source_root(setup, tmp_path, monkeypatch):
    """copy=false path must be unchanged: scan the source root with no
    restrict_dirs so scan-in-place still walks the whole tree."""
    app, _ = setup
    monkeypatch.setenv("HOME", str(tmp_path))

    src = tmp_path / "source"
    sub = src / "nested"
    sub.mkdir(parents=True)
    Image.new("RGB", (40, 40), "green").save(str(sub / "a.jpg"))

    import scanner as scanner_mod
    calls = []
    original = scanner_mod.scan

    def tracking_scan(root, *args, **kwargs):
        calls.append({
            "root": str(root),
            "restrict_dirs": kwargs.get("restrict_dirs"),
        })
        return original(root, *args, **kwargs)

    with patch.object(scanner_mod, "scan", tracking_scan):
        with app.test_client() as c:
            resp = c.post("/api/jobs/import-full", json={
                "source": str(src),
                "copy": False,
            })
            assert resp.status_code == 200
            job_id = resp.get_json()["job_id"]
            status = _wait_for_job(c, job_id)
            assert status["status"] == "completed", status

    assert calls, "scanner.scan was not invoked"
    call = calls[-1]
    assert call["root"] == str(src), (
        f"copy=false should scan source, got {call['root']!r}"
    )
    assert call["restrict_dirs"] is None, (
        f"copy=false must leave restrict_dirs unset; got {call['restrict_dirs']!r}"
    )


def test_pipeline_rejects_source_snapshot_id(setup, tmp_path):
    """New-images snapshots cross the catalog boundary through Import,
    never Process."""
    app, db_path = setup

    # Create a snapshot in the active workspace so the request body references
    # a real id (run_pipeline_job itself is spied — we only assert what gets
    # passed to it).
    from db import Database
    db = Database(db_path)
    folder = tmp_path / "photos"
    folder.mkdir()
    img_path = folder / "IMG_001.JPG"
    Image.new("RGB", (1, 1), "white").save(str(img_path), "JPEG")
    db.add_folder(str(folder))
    snap_id = db.create_new_images_snapshot([str(img_path)])
    db.conn.close()

    with app.test_client() as c:
        resp = c.post("/api/jobs/pipeline", json={
            "source_snapshot_id": snap_id,
            "skip_classify": True,
            "skip_extract_masks": True,
            "skip_regroup": True,
        })
    assert resp.status_code == 400
    assert "use Import" in resp.get_json()["error"]


def test_pipeline_endpoint_forwards_missing_originals_invalidator(
    setup, tmp_path,
):
    """POST /api/jobs/pipeline must pass the app-level Missing Originals
    invalidator through to run_pipeline_job.

    Regression: without this callback the pipeline's finally block only
    invalidates the new-images cache, so a ready GET /api/photos/missing
    payload can survive a scan that added or removed photo rows. See
    Codex review on 63f6ac78. This test spies on run_pipeline_job and
    asserts the handler forwards a callable so the pipeline_job side
    (covered by test_pipeline_job.py) can actually fire it.
    """
    import threading

    from db import Database
    app, db_path = setup

    db = Database(db_path)
    collection_id = db.add_collection("Existing photos", json.dumps([]))
    db.conn.close()

    import pipeline_job
    original = pipeline_job.run_pipeline_job
    captured = {}
    called = threading.Event()

    def spy_run(job, runner, db_path_arg, ws_id, params, **kwargs):
        captured["missing_originals_invalidator"] = kwargs.get(
            "missing_originals_invalidator"
        )
        called.set()

    pipeline_job.run_pipeline_job = spy_run
    try:
        with app.test_client() as c:
            resp = c.post("/api/jobs/pipeline", json={
                "collection_id": collection_id,
                "skip_classify": True,
                "skip_extract_masks": True,
                "skip_regroup": True,
            })
            assert resp.status_code == 200, resp.get_json()

        assert called.wait(timeout=5.0), (
            "run_pipeline_job spy was not invoked"
        )
        # The handler must forward a callable (the create_app closure's
        # _invalidate_missing_originals_cache), not omit or None it out.
        assert callable(captured.get("missing_originals_invalidator")), (
            "POST /api/jobs/pipeline did not forward the "
            "missing_originals_invalidator kwarg — pipeline scans will not "
            "drop the GET /api/photos/missing cache after touching disk"
        )
    finally:
        pipeline_job.run_pipeline_job = original


def test_pipeline_rejects_snapshot_combined_with_stale_sources(setup, tmp_path):
    """Combining two filesystem admission scopes remains a Process boundary
    violation regardless of whether either path is valid."""
    app, db_path = setup

    from db import Database
    db = Database(db_path)
    folder = tmp_path / "photos"
    folder.mkdir()
    img_path = folder / "IMG_001.JPG"
    Image.new("RGB", (1, 1), "white").save(str(img_path), "JPEG")
    db.add_folder(str(folder))
    snap_id = db.create_new_images_snapshot([str(img_path)])
    db.conn.close()

    with app.test_client() as c:
        resp = c.post("/api/jobs/pipeline", json={
            "source_snapshot_id": snap_id,
            "sources": ["/does/not/exist/stale"],
            "skip_classify": True,
            "skip_extract_masks": True,
            "skip_regroup": True,
        })
    assert resp.status_code == 400
    assert "use Import" in resp.get_json()["error"]


def test_pipeline_rejects_destination_with_snapshot(setup, tmp_path):
    """A snapshot-backed run walks the folders that already hold the files
    — there is no valid `destination` combination. If the handler accepted
    both, the copy stage would ingest entire source folders (not just the
    snapshot set), snapshot filtering would then drop the destination-scanned
    photos, and the user would pay for an expensive copy that produces
    nothing downstream. Reject synchronously."""
    app, db_path = setup

    from db import Database
    db = Database(db_path)
    folder = tmp_path / "photos"
    folder.mkdir()
    img_path = folder / "IMG_001.JPG"
    Image.new("RGB", (1, 1), "white").save(str(img_path), "JPEG")
    db.add_folder(str(folder))
    snap_id = db.create_new_images_snapshot([str(img_path)])
    db.conn.close()

    dest = tmp_path / "dest"
    dest.mkdir()
    with app.test_client() as c:
        resp = c.post("/api/jobs/pipeline", json={
            "source_snapshot_id": snap_id,
            "destination": str(dest),
            "skip_classify": True,
            "skip_extract_masks": True,
            "skip_regroup": True,
        })
        assert resp.status_code == 400, (
            f"destination is incompatible with snapshot runs, got "
            f"{resp.status_code}: {resp.get_json()}"
        )


def test_pipeline_rejects_unknown_snapshot_id(setup, tmp_path):
    """Process rejects snapshot admission before snapshot lookup."""
    app, db_path = setup

    with app.test_client() as c:
        resp = c.post("/api/jobs/pipeline", json={
            "source_snapshot_id": 99999,
            "skip_classify": True,
            "skip_extract_masks": True,
            "skip_regroup": True,
        })
        assert resp.status_code == 400
        assert "use Import" in resp.get_json()["error"]


def test_pipeline_rejects_oversized_snapshot_id(setup):
    """An integer outside SQLite's signed 64-bit range would raise
    OverflowError during parameter binding, surfacing as a 500. The endpoint
    must reject it cleanly before reaching SQLite."""
    app, _ = setup
    huge = 10 ** 100
    with app.test_client() as c:
        resp = c.post("/api/jobs/pipeline", json={
            "source_snapshot_id": huge,
            "skip_classify": True,
            "skip_extract_masks": True,
            "skip_regroup": True,
        })
        assert 400 <= resp.status_code < 500, (
            f"oversized snapshot id must be rejected with 4xx, "
            f"got {resp.status_code}: {resp.get_json()}"
        )


@pytest.mark.parametrize("bad_id", [{}, [], [1, 2], {"id": 3}, "abc", 1.5, True])
def test_pipeline_rejects_non_integer_snapshot_id(setup, bad_id):
    """Malformed source_snapshot_id values (objects, arrays, non-numeric
    strings, floats, booleans) must be rejected with a 4xx before reaching
    the DB layer. Without validation, SQLite raises InterfaceError and the
    client sees an opaque 500."""
    app, _ = setup

    with app.test_client() as c:
        resp = c.post("/api/jobs/pipeline", json={
            "source_snapshot_id": bad_id,
            "skip_classify": True,
            "skip_extract_masks": True,
            "skip_regroup": True,
        })
        assert 400 <= resp.status_code < 500, (
            f"bad snapshot id {bad_id!r} must be rejected with 4xx, "
            f"got {resp.status_code}: {resp.get_json()}"
        )


def _seed_workspace_with_masks(db_path):
    """Build a workspace with two photos and a few mask variants so the
    pipeline coverage endpoint has something to report."""
    from db import Database
    db = Database(db_path)
    ws_id = db._active_workspace_id  # Default workspace, auto-created
    fid = db.add_folder("/photos/seed", name="seed")
    db.add_workspace_folder(ws_id, fid)
    p1 = db.add_photo(folder_id=fid, filename="a.jpg", extension=".jpg",
                      file_size=1, file_mtime=1.0)
    p2 = db.add_photo(folder_id=fid, filename="b.jpg", extension=".jpg",
                      file_size=1, file_mtime=1.0)
    db.upsert_photo_mask(p1, "sam2-small", "/m/a.small.png",
        detector_model="md", prompt_x=0, prompt_y=0, prompt_w=0, prompt_h=0)
    db.upsert_photo_mask(p2, "sam2-small", "/m/b.small.png",
        detector_model="md", prompt_x=0, prompt_y=0, prompt_w=0, prompt_h=0)
    db.upsert_photo_mask(p1, "sam2-large", "/m/a.large.png",
        detector_model="md", prompt_x=0, prompt_y=0, prompt_w=0, prompt_h=0)
    db.set_active_mask_variant(p1, "sam2-small")
    db.set_active_mask_variant(p2, "sam2-small")
    db.close()
    return p1, p2


def _seed_workspace_with_large_only_masks(db_path):
    from db import Database
    db = Database(db_path)
    ws_id = db._active_workspace_id
    fid = db.add_folder("/photos/large-only", name="large-only")
    db.add_workspace_folder(ws_id, fid)
    photo_ids = []
    for name in ("a.jpg", "b.jpg"):
        pid = db.add_photo(folder_id=fid, filename=name, extension=".jpg",
                           file_size=1, file_mtime=1.0)
        db.save_detections(
            pid,
            [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
              "confidence": 0.9, "category": "animal"}],
            detector_model="megadetector-v6",
        )
        db.upsert_photo_mask(
            pid, "sam2-large", f"/m/{pid}.large.png",
            detector_model="megadetector-v6",
            prompt_x=0.1, prompt_y=0.1, prompt_w=0.5, prompt_h=0.5,
        )
        db.set_active_mask_variant(pid, "sam2-large")
        photo_ids.append(pid)
    db.close()
    return photo_ids


def test_pipeline_page_init_includes_mask_variant_coverage(setup):
    """page-init exposes mask_variant_coverage so the SAM2 dropdown card
    can render per-variant counts and an active-variant selector."""
    app, db_path = setup
    _seed_workspace_with_masks(db_path)

    with app.test_client() as c:
        resp = c.get("/api/pipeline/page-init")
        assert resp.status_code == 200
        data = resp.get_json()
        assert "mask_variant_coverage" in data
        cov = {row["variant"]: row for row in data["mask_variant_coverage"]}
        assert cov["sam2-small"]["count"] == 2
        assert cov["sam2-small"]["active_count"] == 2
        assert cov["sam2-large"]["count"] == 1
        assert cov["sam2-large"]["active_count"] == 0


def test_pipeline_page_init_warns_when_selected_sam_has_poor_coverage(setup):
    app, db_path = setup
    _seed_workspace_with_large_only_masks(db_path)

    with app.test_client() as c:
        resp = c.get("/api/pipeline/page-init")
        assert resp.status_code == 200
        warning = resp.get_json()["sam_variant_warning"]
        assert warning["selected_variant"] == "sam2-small"
        assert warning["alternate_variant"] == "sam2-large"
        assert warning["target_count"] == 2
        assert "Starting will rerun SAM" in warning["message"]


def test_extract_readiness_includes_sam_variant_coverage_warning(setup, tmp_path, monkeypatch):
    app, db_path = setup
    monkeypatch.setenv("HOME", str(tmp_path))
    _seed_workspace_with_large_only_masks(db_path)

    with app.test_client() as c:
        resp = c.get("/api/pipeline/extract-readiness?sam2_variant=sam2-small")
        assert resp.status_code == 200
        warning = resp.get_json()["sam_variant_warning"]
        assert warning["selected_variant"] == "sam2-small"
        assert warning["alternate_count"] == 2


def test_pipeline_page_init_includes_review_readiness(setup):
    """page-init exposes review_readiness so the review page can render
    a diagnostic empty state and decide whether to offer Compute now."""
    app, db_path = setup
    # _seed_workspace_with_masks leaves photos with masks but no Group cache.
    _seed_workspace_with_masks(db_path)

    with app.test_client() as c:
        resp = c.get("/api/pipeline/page-init")
        assert resp.status_code == 200
        data = resp.get_json()
        assert "review_readiness" in data
        rr = data["review_readiness"]
        assert rr["state"] == "computable"
        assert rr["total_photos"] >= 2
        assert rr["with_masks"] >= 2


def test_pipeline_page_init_review_readiness_state_ready_when_cache_exists(setup, tmp_path):
    """When the grouping cache is already on disk, state should be 'ready'."""
    app, db_path = setup
    _seed_workspace_with_masks(db_path)

    # Drop a minimal cache file at <db_dir>/pipeline_results_ws{N}.json
    import json as _json

    from db import Database
    db = Database(db_path)
    ws = db._active_workspace_id
    db.close()
    cache_path = os.path.join(
        os.path.dirname(db_path), f"pipeline_results_ws{ws}.json"
    )
    with open(cache_path, "w") as f:
        _json.dump({"encounters": [], "photos": [], "summary": {}}, f)

    with app.test_client() as c:
        resp = c.get("/api/pipeline/page-init")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["review_readiness"]["state"] == "ready"
        # The cache letting the page render does not erase enhancing_missing —
        # the seed has masks but no embeddings, so the degraded banner should
        # still see "embeddings" as a quality gap.
        assert "embeddings" in data["review_readiness"]["enhancing_missing"]


def test_pipeline_page_init_reports_partial_review_cache(setup):
    """page-init tells the review UI when cached results cover only a subset."""
    app, db_path = setup
    p1, _p2 = _seed_workspace_with_masks(db_path)

    import json as _json

    from db import Database
    db = Database(db_path)
    ws = db._active_workspace_id
    db.close()
    cache_path = os.path.join(
        os.path.dirname(db_path), f"pipeline_results_ws{ws}.json"
    )
    with open(cache_path, "w") as f:
        _json.dump({
            "encounters": [],
            "photos": [{"id": p1, "filename": "a.jpg", "label": "REVIEW"}],
            "summary": {"total_photos": 1},
        }, f)

    with app.test_client() as c:
        resp = c.get("/api/pipeline/page-init")
        assert resp.status_code == 200
        info = resp.get_json()["results_cache_info"]
        assert info["workspace_photo_count"] == 2
        assert info["cached_photo_count"] == 1
        assert info["missing_photo_count"] == 1
        assert info["is_partial"] is True
        assert info["group_fingerprint_status"] == "untracked"


def test_pipeline_regroup_live_view_scope_does_not_overwrite_cache(setup):
    """Review-page scope changes can compute all-workspace results as a view."""
    app, db_path = setup
    p1, _p2 = _seed_workspace_with_masks(db_path)

    import json as _json

    from db import Database
    db = Database(db_path)
    ws = db._active_workspace_id
    db.close()
    cache_path = os.path.join(
        os.path.dirname(db_path), f"pipeline_results_ws{ws}.json"
    )
    original_cache = {
        "encounters": [],
        "photos": [{"id": p1, "filename": "a.jpg", "label": "REVIEW"}],
        "summary": {"total_photos": 1},
    }
    with open(cache_path, "w") as f:
        _json.dump(original_cache, f)

    with app.test_client() as c:
        resp = c.post("/api/pipeline/regroup-live", json={"save_cache": False})
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["summary"]["total_photos"] == 2

    with open(cache_path) as f:
        assert _json.load(f) == original_cache


def test_pipeline_regroup_live_default_persists_cache(setup):
    """Latest-review-scope slider tunes (no photo_ids, no save_cache flag)
    must persist to the saved cache so a reload restores the user's
    latest review adjustments. This is the pre-scope-switcher default
    behavior; the client's reviewScopePayload sends the same body shape
    when scope == 'cache'."""
    app, db_path = setup
    p1, _p2 = _seed_workspace_with_masks(db_path)

    import json as _json

    from db import Database
    db = Database(db_path)
    ws = db._active_workspace_id
    db.close()
    cache_path = os.path.join(
        os.path.dirname(db_path), f"pipeline_results_ws{ws}.json"
    )
    original_cache = {
        "encounters": [],
        "photos": [{"id": p1, "filename": "a.jpg", "label": "REVIEW"}],
        "summary": {"total_photos": 1},
    }
    with open(cache_path, "w") as f:
        _json.dump(original_cache, f)

    with app.test_client() as c:
        resp = c.post("/api/pipeline/regroup-live", json={"config": {}})
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["summary"]["total_photos"] == 2

    with open(cache_path) as f:
        saved = _json.load(f)
    # The default request shape must have overwritten the partial cache
    # with the fresh workspace-wide regroup result.
    assert saved != original_cache
    assert saved["summary"]["total_photos"] == 2


def test_pipeline_page_init_state_ready_folds_blocking_gaps_into_enhancing(setup, tmp_path):
    """When the grouping cache is on disk but mask coverage is below the
    25% threshold, compute_review_readiness returns missing_required=["masks"]
    without "masks_partial" in enhancing_missing. The page-init route should
    force state="ready" (cache lets the page render) AND fold "masks" into
    enhancing_missing as "masks_partial" so the degraded banner surfaces it.
    """
    app, db_path = setup

    # Seed photos but DON'T add masks — so cov["mask"] = 0 and state is
    # "insufficient" with missing_required=["masks"] before the route fixup.
    from db import Database
    db = Database(db_path)
    ws = db._active_workspace_id
    fid = db.add_folder("/photos/seed_no_masks", name="seed_no_masks")
    db.add_workspace_folder(ws, fid)
    db.add_photo(folder_id=fid, filename="a.jpg", extension=".jpg",
                 file_size=1, file_mtime=1.0)
    db.add_photo(folder_id=fid, filename="b.jpg", extension=".jpg",
                 file_size=1, file_mtime=1.0)
    db.close()

    # Drop a minimal cache file at <db_dir>/pipeline_results_ws{N}.json
    import json as _json

    cache_path = os.path.join(
        os.path.dirname(db_path), f"pipeline_results_ws{ws}.json"
    )
    with open(cache_path, "w") as f:
        _json.dump({"encounters": [], "photos": [], "summary": {}}, f)

    with app.test_client() as c:
        resp = c.get("/api/pipeline/page-init")
        assert resp.status_code == 200
        data = resp.get_json()
        rr = data["review_readiness"]
        assert rr["state"] == "ready"
        assert rr["missing_required"] == []
        assert "masks_partial" in rr["enhancing_missing"]


def test_active_mask_variant_endpoint_switches_workspace_photos(setup):
    """POST /api/pipeline/active-mask-variant flips active_mask_variant on
    every workspace photo that has a row for the requested variant."""
    app, db_path = setup
    p1, p2 = _seed_workspace_with_masks(db_path)

    with app.test_client() as c:
        resp = c.post(
            "/api/pipeline/active-mask-variant",
            json={"variant": "sam2-large"},
        )
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["ok"] is True
        # Only p1 has a sam2-large row, so only p1 is updated.
        assert body["updated"] == 1

        # p2 stays on sam2-small (no sam2-large row to switch to).
        from db import Database
        db = Database(db_path)
        try:
            row = db.conn.execute(
                "SELECT id, active_mask_variant FROM photos WHERE id IN (?, ?) "
                "ORDER BY id",
                (p1, p2),
            ).fetchall()
            by_id = {r["id"]: r["active_mask_variant"] for r in row}
            assert by_id[p1] == "sam2-large"
            assert by_id[p2] == "sam2-small"
        finally:
            db.close()


def test_active_mask_variant_endpoint_is_workspace_scoped(setup):
    """Switching the active mask variant in workspace A must not affect
    photos that live in workspace B. ``photo_masks`` and ``photos`` are
    global tables — the endpoint relies on a workspace_folders join to
    scope the UPDATE. Regression guard: a buggy version that drops the
    join would silently flip every photo in the DB.
    """
    app, db_path = setup

    from db import Database
    db = Database(db_path)
    try:
        ws_a = db.create_workspace("A")
        ws_b = db.create_workspace("B")

        f_a = db.add_folder("/a", name="a")
        f_b = db.add_folder("/b", name="b")
        db.add_workspace_folder(ws_a, f_a)
        db.add_workspace_folder(ws_b, f_b)

        p_a = db.add_photo(folder_id=f_a, filename="a.jpg",
                           extension=".jpg", file_size=1, file_mtime=1.0)
        p_b = db.add_photo(folder_id=f_b, filename="b.jpg",
                           extension=".jpg", file_size=1, file_mtime=1.0)

        # Seed the same variant in both workspaces so the only thing
        # keeping ws_b out of the update is the workspace join.
        db.upsert_photo_mask(p_a, "sam2-small", "/m/a.small.png",
            detector_model="md", prompt_x=0, prompt_y=0,
            prompt_w=0, prompt_h=0)
        db.upsert_photo_mask(p_b, "sam2-small", "/m/b.small.png",
            detector_model="md", prompt_x=0, prompt_y=0,
            prompt_w=0, prompt_h=0)

        # Mark ws_a as the most-recently-opened so a fresh Database()
        # inside _get_db() picks it as the active workspace.
        db.update_workspace(ws_b, last_opened_at="2026-01-01T00:00:00")
        db.update_workspace(ws_a, last_opened_at="2026-05-01T00:00:00")
    finally:
        db.close()

    with app.test_client() as c:
        resp = c.post(
            "/api/pipeline/active-mask-variant",
            json={"variant": "sam2-small"},
        )
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["ok"] is True
        # Only the workspace-A photo should have been updated.
        assert body["updated"] == 1

    db = Database(db_path)
    try:
        rows = db.conn.execute(
            "SELECT id, active_mask_variant FROM photos WHERE id IN (?, ?)",
            (p_a, p_b),
        ).fetchall()
        by_id = {r["id"]: r["active_mask_variant"] for r in rows}
        assert by_id[p_a] == "sam2-small"
        # ws_b's photo MUST be untouched — it lives in a different
        # workspace, even though it has the same variant in photo_masks.
        assert by_id[p_b] is None
    finally:
        db.close()


def test_active_mask_variant_endpoint_requires_variant(setup):
    app, _ = setup
    with app.test_client() as c:
        resp = c.post("/api/pipeline/active-mask-variant", json={})
        assert resp.status_code == 400
