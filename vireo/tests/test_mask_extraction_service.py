"""Exercise standalone extraction without a Flask application or request."""

import sqlite3
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import config as cfg
import numpy as np
import pipeline_locks
import pytest
from db import Database
from resource_ledger import ResourceWaitCancelled
from services import mask_extraction, photo_ingest, workspace_folders
from services.mask_extraction import MaskExtractionOptions, extract_masks


class Runner:
    def __init__(self):
        self.cancelled = False
        self.events = []

    def is_cancelled(self, job_id):
        return self.cancelled

    def pause_requested(self, job_id):
        return False

    def cancellation_requested(self, job_id):
        return self.cancelled

    def push_event(self, job_id, event, payload):
        self.events.append((event, payload))


@pytest.fixture
def extraction(db, tmp_path, monkeypatch):
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    deps = mask_extraction._import_mask_extraction_deps()
    generated = []

    def generate(proxy, box, variant):
        generated.append((variant, box))
        return np.ones((4, 4), dtype=bool)

    deps.render_proxy = lambda path, longest_edge: np.zeros((4, 4, 3), dtype=np.uint8)
    deps.generate_mask = generate
    deps.crop_completeness = lambda mask: 0.9
    deps.compute_all_quality_features = lambda proxy, mask: {"subject_tenengrad": 1.5}
    deps.measure_subject_detail = lambda proxy, mask, *, source_path, frame_size=None: {
        "subject_detail": 100.0, "subject_detail_px": 40.0}
    deps.measure_cached_subject_detail = lambda mask_path, source_path, frame_size=None: {
        "subject_detail": 100.0, "subject_detail_px": 40.0}
    deps.crop_subject = lambda proxy, mask, margin: None
    deps.embed = lambda proxy, variant: np.zeros(384, dtype=np.float32)
    monkeypatch.setattr(mask_extraction, "_import_mask_extraction_deps", lambda: deps)
    workspace_id = db.active_workspace_id
    opened = []
    db_path = str(tmp_path / "test.db")

    def open_db():
        worker = Database(db_path)
        worker.set_active_workspace(workspace_id)
        opened.append(worker)
        return worker

    return SimpleNamespace(
        deps=deps, generated=generated, opened=opened, open_db=open_db,
        runner=Runner(), job={"id": "extract-test", "errors": []}, db_path=db_path,
        options=MaskExtractionOptions(
            collection_id=None, sam2_variant="sam2-base", dinov2_variant="dinov2-base",
            proxy_longest_edge=512, min_detector_conf=0.2,
        ),
    )


def seed_photo(db, tmp_path, name):
    folder = workspace_folders.add_folder(db, str(tmp_path / name))
    pid = photo_ingest.add_photo(db, folder, name + ".jpg", ".jpg", 1000, 1.0)
    db.detections.save(pid, [{
        "box": {"x": 0.1, "y": 0.2, "w": 0.3, "h": 0.4},
        "confidence": 0.9, "category": "animal",
    }], detector_model="MegaDetector")
    db.commit()
    return pid


def run(extraction):
    return extract_masks(
        extraction.job, extraction.db_path, runner=extraction.runner,
        open_db=extraction.open_db, options=extraction.options,
    )


def assert_released(extraction, *photo_ids):
    assert extraction.opened and all(db.closed for db in extraction.opened)
    for pid in photo_ids:
        lock = pipeline_locks.acquire_photo_mask(pid)
        assert lock.acquire(blocking=False), "extraction left a photo locked"
        lock.release()


def test_service_uses_captured_workspace_and_reports_progress(db, tmp_path, extraction):
    first = seed_photo(db, tmp_path, "launch-workspace")
    other_workspace = db.create_workspace("Other")
    db.commit()
    db.set_active_workspace(other_workspace)
    other = seed_photo(db, tmp_path, "other-workspace")

    result = run(extraction)

    assert result == {
        "masked": 1, "skipped": 0, "failed": 0, "unreadable": 0,
        "total": 1, "detail_measured": 0, "detail_unreadable": 0,
        "mask_cache_invalidated": 0, "ok": True, "errors": [],
    }
    assert db.masks_features.get_mask(first, "sam2-base")["subject_detail"] == 100.0
    assert len(extraction.generated) == 1
    assert db.masks_features.get_mask(first, "sam2-base") is not None
    assert db.masks_features.get_mask(other, "sam2-base") is None
    event, progress = extraction.runner.events[0]
    assert event == "progress"
    assert progress["current"] == progress["total"] == 1
    assert progress["current_file"] == "launch-workspace.jpg"
    assert progress["phase"] == "Extracting features (SAM2 + DINOv2)"
    assert_released(extraction, first, other)


@pytest.mark.parametrize("cancel_at", ["before-photo", "inference"])
def test_service_cancellation_stops_before_later_photos(db, tmp_path, extraction, cancel_at):
    first = seed_photo(db, tmp_path, "first")
    second = seed_photo(db, tmp_path, "second")
    attempted = []

    def cancel_inference(proxy, box, variant):
        attempted.append(box)
        extraction.runner.cancelled = True
        raise ResourceWaitCancelled("cancelled inference wait")

    if cancel_at == "before-photo":
        extraction.runner.cancelled = True
    else:
        extraction.deps.generate_mask = cancel_inference

    result = run(extraction)

    assert len(attempted) == (1 if cancel_at == "inference" else 0)
    assert result["masked"] == result["failed"] == 0
    assert result["total"] == 2
    assert extraction.job["errors"] == []
    assert db.masks_features.get_mask(first, "sam2-base") is None
    assert db.masks_features.get_mask(second, "sam2-base") is None
    assert extraction.runner.events == []
    assert_released(extraction, first, second)


def test_failed_commit_restores_previous_mask_and_continues(db, tmp_path, extraction, monkeypatch):
    first = seed_photo(db, tmp_path, "first")
    assert run(extraction)["masked"] == 1
    previous = db.masks_features.get_mask(first, "sam2-base")
    previous_path = Path(previous["path"])
    previous_bytes = previous_path.read_bytes()
    second = seed_photo(db, tmp_path, "second")
    extraction.options = replace(extraction.options, dinov2_variant="different-dino")
    open_db = extraction.open_db

    def open_failing_db():
        worker = open_db()
        commit = worker.commit_with_retry

        def fail_first_photo_commit():
            row = worker.masks_features.get_mask(first, "sam2-base")
            if row is not None and row["path"] != str(previous_path):
                raise sqlite3.OperationalError("injected mask commit failure")
            return commit()

        monkeypatch.setattr(worker, "commit_with_retry", fail_first_photo_commit)
        return worker

    extraction.open_db = open_failing_db
    result = run(extraction)

    assert result["failed"] == result["masked"] == 1
    assert result["ok"] is False
    assert extraction.job["errors"] == [f"Photo {first}: mask extraction failed"]
    assert db.masks_features.get_mask(first, "sam2-base") == previous
    assert previous_path.read_bytes() == previous_bytes
    second_path = Path(db.masks_features.get_mask(second, "sam2-base")["path"])
    assert second_path.is_file()
    assert set(previous_path.parent.iterdir()) == {previous_path, second_path}
    assert_released(extraction, first, second)


def test_dependency_failure_closes_worker_database(extraction, monkeypatch):
    def unavailable():
        raise RuntimeError("model dependencies unavailable")

    monkeypatch.setattr(mask_extraction, "_import_mask_extraction_deps", unavailable)
    with pytest.raises(RuntimeError, match="model dependencies unavailable"):
        run(extraction)
    assert_released(extraction)


def test_fresh_mask_whose_detail_reread_fails_credits_detail_unreadable(db, tmp_path, extraction):
    # A source that becomes unreadable between the proxy decode and the
    # larger-edge detail re-read: the fresh mask is stored but its detail
    # measurement stays pending, counted the same way as a cached mask
    # with pending detail so the job result does not claim completion
    # with planned work left behind.
    first = seed_photo(db, tmp_path, "source-lost-midrun")
    extraction.deps.measure_subject_detail = (
        lambda proxy, mask, *, source_path, frame_size=None: {}
    )

    result = run(extraction)

    assert result["masked"] == 1
    assert result["detail_unreadable"] == 1
    assert result["ok"] is False
    assert any("subject detail measurement" in e for e in result["errors"])
    stored = db.masks_features.get_mask(first, "sam2-base")
    assert stored is not None and stored["subject_detail"] is None
    assert_released(extraction, first)


def test_cached_mask_whose_png_will_not_decode_is_dropped_so_extract_rebuilds_it(
    db, tmp_path, extraction,
):
    # A photo_masks row whose PNG is corrupt would be reused forever:
    # os.path.isfile passes, we take the cache hit, the detail re-read
    # fails on every run, and the mask is never regenerated. Dropping
    # the file here makes the next Extract miss the cache and rebuild.
    import quality

    first = seed_photo(db, tmp_path, "cached-but-corrupt")
    assert run(extraction)["masked"] == 1
    mask_path = Path(db.masks_features.get_mask(first, "sam2-base")["path"])
    assert mask_path.is_file()
    db.conn.execute(
        "UPDATE photo_masks SET subject_detail=NULL, subject_detail_px=NULL "
        "WHERE photo_id=?",
        (first,),
    )
    db.commit()
    extraction.deps.measure_cached_subject_detail = (
        lambda mask_path, source_path, frame_size=None: quality.MASK_CACHE_INVALID
    )

    result = run(extraction)

    assert result["mask_cache_invalidated"] == 1
    assert result["detail_unreadable"] == 0
    assert result["ok"] is False
    assert any("will be rebuilt on the next Extract run" in e for e in result["errors"])
    assert not mask_path.is_file()
    assert_released(extraction, first)
