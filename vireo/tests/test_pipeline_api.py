"""Tests for the full-chain import pipeline endpoint."""
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


def test_pipeline_local_processing_rejects_collection_id(setup, tmp_path):
    # Collection pipelines set skip_scan and never run ingest, so the
    # staging folder is never created/indexed. Without this rejection
    # the job would burn through every processing stage and then fail
    # at archive_stage with "local staging folder was not indexed".
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
        assert "local_processing" in err
        assert "source" in err


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


def test_pipeline_local_processing_rejects_already_tracked_destination(
    setup, tmp_path, monkeypatch
):
    """Regression: a second local-processing import to a destination that
    Vireo already manages must fail fast at the storage preflight. Without
    the upfront check the pipeline would stage everything, complete every
    processing step, and only fail in move_folder's tracked-overlap guard —
    leaving the staged copy stranded under ~/.vireo/staging."""
    app, db_path = setup

    final_parent = tmp_path / "nas"
    final_parent.mkdir()
    final_dest = final_parent / "Photos"

    import local_processing
    monkeypatch.setattr(local_processing, "MIN_DERIVED_OVERHEAD_BYTES", 0)
    monkeypatch.setattr(local_processing, "RESERVED_FREE_BYTES", 0)

    # Seed the catalog with a tracked folder at the prospective archive path,
    # matching the post-state of an earlier successful local-processing run.
    from db import Database
    db = Database(db_path)
    db.add_folder(str(final_dest))
    db.close()

    src = tmp_path / "card2"
    src.mkdir()
    Image.new("RGB", (16, 16), "blue").save(src / "again.jpg")

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
    assert "Vireo already manages" in error_text, job


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
        file_size=10, file_mtime=1.0, file_hash=dup_hash,
    )
    db.close()

    # Pick a disk_usage and reserve combination so that summing both files
    # would exceed the usable budget, but the fresh file alone fits.
    fresh_bytes = (src / "fresh.jpg").stat().st_size
    dup_bytes = (src / "dup.jpg").stat().st_size
    total_bytes = fresh_bytes + dup_bytes
    # required_bytes ≈ source_bytes * 1.25 (overhead ratio) when
    # MIN_DERIVED_OVERHEAD_BYTES=0. Pick free = roughly fresh_bytes * 2 plus
    # reserve so the duplicate-filtered plan fits but the naive plan doesn't.
    reserve = 4
    free = int(fresh_bytes * 1.5) + reserve + 1
    assert free < int(total_bytes * 1.25) + reserve, "free must exceed filtered need but not the naive need"

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


def test_pipeline_accepts_sources_list(setup):
    """Pipeline endpoint should accept sources as a list of folders."""
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
            assert resp.status_code == 200
            data = resp.get_json()
            assert "job_id" in data
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


def test_pipeline_accepts_source_snapshot_id(setup, tmp_path):
    """POST /api/jobs/pipeline should propagate source_snapshot_id from the
    request body into the PipelineParams passed to run_pipeline_job."""
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

    # The handler does ``from pipeline_job import PipelineParams, run_pipeline_job``
    # inside the request, so patching the attribute on the module swaps what
    # the handler's local binding will see on next request.
    import threading

    import pipeline_job
    captured = {}
    called = threading.Event()
    original = pipeline_job.run_pipeline_job

    def spy_run(job, runner, db_path_arg, ws_id, params, **_kwargs):
        captured["source_snapshot_id"] = params.source_snapshot_id
        called.set()

    pipeline_job.run_pipeline_job = spy_run
    try:
        with app.test_client() as c:
            resp = c.post("/api/jobs/pipeline", json={
                "source_snapshot_id": snap_id,
                "skip_classify": True,
                "skip_extract_masks": True,
                "skip_regroup": True,
            })
            assert resp.status_code == 200, resp.get_json()

        # JobRunner runs work() on a worker thread; wait briefly for spy to fire.
        assert called.wait(timeout=5.0), "run_pipeline_job spy was not invoked"
        assert captured["source_snapshot_id"] == snap_id
    finally:
        pipeline_job.run_pipeline_job = original


def test_pipeline_snapshot_overrides_stale_source_paths(setup, tmp_path):
    """When a valid source_snapshot_id is present, the job overrides any
    source/sources the caller passed. The handler must not preflight-validate
    those stale paths — rejecting an otherwise-valid snapshot run because
    the accompanying placeholder folder no longer exists is a false 400."""
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

    import pipeline_job
    original = pipeline_job.run_pipeline_job
    pipeline_job.run_pipeline_job = lambda *a, **kw: None
    try:
        with app.test_client() as c:
            resp = c.post("/api/jobs/pipeline", json={
                "source_snapshot_id": snap_id,
                "sources": ["/does/not/exist/stale"],  # stale placeholder
                "skip_classify": True,
                "skip_extract_masks": True,
                "skip_regroup": True,
            })
            assert resp.status_code == 200, (
                f"snapshot should override stale sources, got "
                f"{resp.status_code}: {resp.get_json()}"
            )
    finally:
        pipeline_job.run_pipeline_job = original


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
    """A pipeline request with a non-existent source_snapshot_id must be
    rejected synchronously with 404 rather than accepted and failing later
    on the worker thread with a generic job error. This gives the client
    an actionable response at request time."""
    app, db_path = setup

    with app.test_client() as c:
        resp = c.post("/api/jobs/pipeline", json={
            "source_snapshot_id": 99999,
            "skip_classify": True,
            "skip_extract_masks": True,
            "skip_regroup": True,
        })
        assert resp.status_code == 404, (
            f"stale snapshot id must be rejected synchronously, "
            f"got {resp.status_code}: {resp.get_json()}"
        )


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
