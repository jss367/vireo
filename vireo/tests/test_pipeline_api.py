"""Tests for the full-chain import pipeline endpoint."""
import os
import shutil
import tempfile
from datetime import datetime

import pytest
from app import create_app
from PIL import Image


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
