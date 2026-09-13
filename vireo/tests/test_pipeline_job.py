"""Tests for the streaming pipeline job orchestrator."""

import contextlib
import json
import os
import sqlite3
import sys
import threading

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from pipeline_job import (
    STAGE_WEIGHTS,
    PipelineParams,
    _cached_classify_detections,
    _classification_eta_progress,
    _record_unattempted_cache_hit,
    _remove_attempted_cache_hits,
    _stage_fraction,
    _weighted_progress,
    run_pipeline_job,
)


def _drop_jpeg(folder_path, filename):
    """Write a tiny valid JPEG at folder_path/filename so previews/thumbnails
    can load it. Tests that use db.add_photo need a matching file on disk now
    that missing files count as stage failures."""
    from PIL import Image
    path = os.path.join(folder_path, filename)
    Image.new("RGB", (16, 16), "black").save(path)
    return path


def _make_job():
    return {
        "id": "pipeline-test-1",
        "type": "pipeline",
        "status": "running",
        "started_at": "2026-01-01T00:00:00",
        "finished_at": None,
        "progress": {"current": 0, "total": 0, "current_file": ""},
        "result": None,
        "errors": [],
        "config": {},
        "workspace_id": 1,
    }


class FakeRunner:
    def __init__(self):
        self.events = []
        self.step_updates = []
        self.cancelled_ids = set()

    def push_event(self, job_id, event_type, data):
        self.events.append((job_id, event_type, data))

    def set_steps(self, job_id, steps):
        self.steps_defined = list(steps)

    def update_step(self, job_id, step_id, **kwargs):
        self.step_updates.append((job_id, step_id, kwargs))

    def is_cancelled(self, job_id):
        return job_id in self.cancelled_ids


def test_contextual_weak_cached_candidates_ignore_foreign_detectors():
    detections = [
        {
            "id": 1,
            "detector_model": "other-detector",
            "category": "animal",
            "confidence": 0.9,
        },
        {
            "id": 2,
            "detector_model": "megadetector-v6",
            "category": "animal",
            "confidence": 0.3,
        },
    ]

    ordinary = _cached_classify_detections(detections, 0.2)
    contextual = _cached_classify_detections(
        detections, 0.2, contextual_weak=True,
    )

    assert [d["id"] for d in ordinary] == [1, 2]
    assert [d["id"] for d in contextual] == [2]


def test_attempted_photo_is_not_still_reported_as_cached():
    cache_hits = {10, 20}

    removed = _remove_attempted_cache_hits(
        attempted_photo_ids={10, 30},
        cache_hit_ids=cache_hits,
    )

    assert removed == 1
    assert cache_hits == {20}


def test_later_cache_hit_does_not_restore_attempted_photo():
    cache_hits = set()

    recorded = _record_unattempted_cache_hit(
        photo_id=10,
        inferred_photo_ids=set(),
        attempted_photo_ids={10},
        cache_hit_ids=cache_hits,
    )

    assert recorded is False
    assert cache_hits == set()


def test_classification_eta_excludes_fast_cache_hits_from_rate():
    """A cache-heavy prefix must not masquerade as model throughput."""
    fields = _classification_eta_progress(
        total=15_418,
        seen=6_112,
        cached_estimate=4_062,
        cache_hits=4_157,
        inference_attempts=913,
        classified=913,
        elapsed=1_480,
    )

    assert fields["eta_state"] == "ready"
    assert fields["eta_rate_per_min"] == pytest.approx(37.0, abs=0.1)
    assert fields["remaining_uncached"] == 9_306
    assert fields["eta_seconds"] == pytest.approx(15_085, abs=1)
    # The broken UI used seen / elapsed: roughly 248/min and a 37-minute ETA.
    assert fields["eta_seconds"] > 4 * 60 * 60


def test_classification_eta_waits_for_a_representative_uncached_batch():
    fields = _classification_eta_progress(
        total=1_000,
        seen=700,
        cached_estimate=650,
        cache_hits=650,
        inference_attempts=15,
        classified=15,
        elapsed=60,
    )

    assert fields["eta_state"] == "estimating"
    assert fields["eta_seconds"] is None
    assert fields["eta_rate_per_min"] is None


def test_classification_eta_subtracts_expected_future_cache_hits():
    fields = _classification_eta_progress(
        total=100,
        seen=36,
        cached_estimate=60,
        cache_hits=20,
        inference_attempts=16,
        classified=16,
        elapsed=120,
    )

    assert fields["remaining_uncached"] == 24
    assert fields["eta_rate_per_min"] == 8.0
    assert fields["eta_seconds"] == 180


def test_classification_eta_reconciles_overcounted_cache_estimate():
    """The preflight counts a photo as cached whenever every qualifying
    detection has a classifier_runs row, but the runtime cache-hit
    predicate additionally requires cached predictions to exist. On a
    collection dominated by run-key-without-predictions rows, the
    preflight overcounts; without reconciliation ``remaining_uncached``
    collapses to zero after the first inference batch and the UI would
    report "finishing…" while most photos still need inference.
    """
    # 100 photos: preflight said all 100 are cached, but every single one
    # falls through at runtime (run key present, predictions missing).
    # After 20 photos we have 20 observed overcounts and 20 inferred; the
    # remaining 80 photos are ALL uncached work.
    fields = _classification_eta_progress(
        total=100,
        seen=20,
        cached_estimate=100,
        cache_hits=0,
        inference_attempts=20,
        classified=20,
        elapsed=120,
        cache_overcount=20,
    )

    assert fields["remaining_uncached"] == 80
    assert fields["eta_state"] == "ready"
    assert fields["eta_rate_per_min"] == 10.0
    assert fields["eta_seconds"] == 480


def test_classification_eta_overcount_floors_at_observed_cache_hits():
    """``cache_overcount`` must never push corrected cached_estimate
    below the cache hits we've already observed — otherwise a burst of
    overcounts on later photos could retroactively erase legitimate
    cache hits from the projection.
    """
    fields = _classification_eta_progress(
        total=100,
        seen=40,
        cached_estimate=50,
        cache_hits=30,
        inference_attempts=10,
        classified=10,
        elapsed=60,
        # Absurdly large overcount that would drive corrected estimate
        # negative if not floored.
        cache_overcount=1000,
    )

    # remaining_photos = 60; corrected_cached_estimate floors at
    # cache_hits (30) so future expected hits = 0.
    assert fields["remaining_uncached"] == 60


def test_classification_eta_subtracts_future_unclassifiable_photos():
    """Photos the runtime deterministically skips at the ``raw_real_dets``
    continue branch (real detections but none above the classify floor,
    not contextual-weak) must not inflate ``remaining_uncached`` — the
    runtime never enters an inference batch for them, so the ETA
    otherwise reports a large numeric estimate for a tail that will be
    traversed almost immediately (Codex #1468 P2).
    """
    # 100 photos total; preflight identified 60 as unclassifiable. After
    # 20 seen (0 skipped so far — the prefix happens to be all
    # classifiable), we have 20 real inference attempts. The remaining
    # 80 photos include 60 unclassifiable — only 20 are real work.
    fields = _classification_eta_progress(
        total=100,
        seen=20,
        cached_estimate=0,
        cache_hits=0,
        inference_attempts=20,
        classified=20,
        elapsed=120,
        unclassifiable_estimate=60,
        unclassifiable_seen=0,
    )

    assert fields["remaining_uncached"] == 20
    assert fields["eta_state"] == "ready"
    assert fields["eta_rate_per_min"] == 10.0
    assert fields["eta_seconds"] == 120


def test_classification_eta_elapsed_is_inference_active_seconds_only():
    """``elapsed`` is the accumulated inference-active seconds (time
    spent inside ``_flush_batch``), not walltime since the spec started.
    Passing walltime would bleed the cache-traversal prefix into the
    rate: on a spec that walked a cached prefix for 500s before hitting
    inference, walltime-based ``elapsed`` would derate the inferred
    rate by that entire prefix, inflating the ETA for the uncached
    tail (Codex #1468 P2).

    Same 20 inference attempts, same 100-photo total. With walltime-
    based elapsed (600s including a 500s cache prefix), the rate reads
    2/min and the 60-photo tail projects to ~30 minutes. With
    inference-active elapsed (100s of pure model work), the rate reads
    12/min and the tail projects to 5 minutes.
    """
    walltime_fields = _classification_eta_progress(
        total=100,
        seen=40,
        cached_estimate=0,
        cache_hits=0,
        inference_attempts=20,
        classified=20,
        elapsed=600,  # walltime — includes cache-traversal prefix
    )
    inference_only_fields = _classification_eta_progress(
        total=100,
        seen=40,
        cached_estimate=0,
        cache_hits=0,
        inference_attempts=20,
        classified=20,
        elapsed=100,  # inference-active seconds only
    )

    # The fix is at the caller (pass inference_seconds, not walltime).
    # This test pins the contract by demonstrating that the ETA
    # function relies on the caller to hand it the right denominator —
    # so a future regression that reverts to walltime here would be
    # caught immediately by the walltime ETA being ~6x too large.
    assert walltime_fields["eta_rate_per_min"] == 2.0
    assert walltime_fields["eta_seconds"] == 1800
    assert inference_only_fields["eta_rate_per_min"] == 12.0
    assert inference_only_fields["eta_seconds"] == 300


def test_classification_eta_finishing_when_preflight_leaves_no_uncached_work():
    """When preflight determines the entire remaining collection is either
    cached or unclassifiable, ``expected_uncached`` collapses to zero and
    the runtime will never enter an inference batch. In that state the
    "Estimating after the first uncached batch…" placeholder can never
    resolve, so the Jobs page would linger on it throughout the traversal.
    ``_classification_eta_progress`` must instead return ``eta_state ==
    "finishing"`` so the UI shows the correct signal from tick zero
    (Codex #1468 P2).
    """
    # Fully cached: 100 photos, preflight said all 100 are cached, no
    # inference has run yet.
    fully_cached = _classification_eta_progress(
        total=100,
        seen=0,
        cached_estimate=100,
        cache_hits=0,
        inference_attempts=0,
        classified=0,
        elapsed=0.0,
    )
    assert fully_cached["eta_state"] == "finishing"
    assert fully_cached["eta_seconds"] == 0

    # Fully unclassifiable: 100 photos, preflight said all 100 will be
    # skipped at the raw_real_dets branch, no inference will run.
    fully_unclassifiable = _classification_eta_progress(
        total=100,
        seen=0,
        cached_estimate=0,
        cache_hits=0,
        inference_attempts=0,
        classified=0,
        elapsed=0.0,
        unclassifiable_estimate=100,
        unclassifiable_seen=0,
    )
    assert fully_unclassifiable["eta_state"] == "finishing"
    assert fully_unclassifiable["eta_seconds"] == 0

    # Mix that leaves no uncached work: 40 cached + 60 unclassifiable.
    fully_covered = _classification_eta_progress(
        total=100,
        seen=10,
        cached_estimate=40,
        cache_hits=10,
        inference_attempts=0,
        classified=0,
        elapsed=0.0,
        unclassifiable_estimate=60,
        unclassifiable_seen=0,
    )
    assert fully_covered["eta_state"] == "finishing"
    assert fully_covered["eta_seconds"] == 0

    # A genuinely uncached tail still waits for the first representative
    # batch — the finishing early-return must not swallow the estimating
    # state when there is real inference work ahead.
    uncached_tail = _classification_eta_progress(
        total=100,
        seen=0,
        cached_estimate=50,
        cache_hits=0,
        inference_attempts=0,
        classified=0,
        elapsed=0.0,
        unclassifiable_estimate=10,
        unclassifiable_seen=0,
    )
    assert uncached_tail["eta_state"] == "estimating"
    assert uncached_tail["eta_seconds"] is None


def test_classification_eta_unclassifiable_seen_capped_at_estimate():
    """The seen count for unclassifiable photos should not exceed the
    preflight estimate — an unexpected extra skip cannot make future
    unclassifiable count go negative and cancel out real work.
    """
    fields = _classification_eta_progress(
        total=100,
        seen=40,
        cached_estimate=0,
        cache_hits=0,
        inference_attempts=20,
        classified=20,
        elapsed=60,
        unclassifiable_estimate=10,
        unclassifiable_seen=15,  # runtime skipped more than preflight said
    )

    # remaining_photos = 60; expected_future_unclassifiable clamps at 0
    # (all 10 preflight-predicted skips accounted for by seen), so all
    # 60 remaining photos count as work.
    assert fields["remaining_uncached"] == 60


def test_pipeline_params_has_skip_classify():
    """PipelineParams should support skip_classify flag."""
    params = PipelineParams(collection_id=1, skip_classify=True)
    assert params.skip_classify is True


def test_pipeline_params_skip_classify_defaults_false():
    params = PipelineParams(collection_id=1)
    assert params.skip_classify is False


def test_pipeline_params_has_skip_eye_keypoints():
    """PipelineParams should support skip_eye_keypoints flag."""
    params = PipelineParams(collection_id=1, skip_eye_keypoints=True)
    assert params.skip_eye_keypoints is True


def test_pipeline_params_skip_eye_keypoints_defaults_false():
    params = PipelineParams(collection_id=1)
    assert params.skip_eye_keypoints is False


def test_pipeline_params_has_preview_max_size():
    """PipelineParams should support preview_max_size."""
    params = PipelineParams(collection_id=1, preview_max_size=2560)
    assert params.preview_max_size == 2560


def test_pipeline_params_preview_max_size_defaults_to_config():
    params = PipelineParams(collection_id=1)
    assert params.preview_max_size is None


def test_pipeline_params_sources_list():
    """PipelineParams should accept a list of source folders."""
    params = PipelineParams(sources=["/photos/card1", "/photos/card2"])
    assert params.sources == ["/photos/card1", "/photos/card2"]


def test_pipeline_params_sources_defaults_none():
    params = PipelineParams(collection_id=1)
    assert params.sources is None


def test_pipeline_params_defaults():
    """PipelineParams should have sensible defaults."""
    params = PipelineParams(collection_id=42)
    assert params.collection_id == 42
    assert params.source is None
    assert params.destination is None
    assert params.file_types == "both"
    assert params.folder_template == "%Y/%Y-%m-%d"
    assert params.skip_duplicates is True
    assert params.labels_file is None
    assert params.labels_files is None
    assert params.model_id is None
    assert params.reclassify is False
    assert params.skip_extract_masks is False
    assert params.skip_regroup is False
    assert params.sources is None
    assert params.skip_classify is False
    assert params.preview_max_size is None


def test_pipeline_params_all_fields():
    """PipelineParams should accept all fields."""
    params = PipelineParams(
        collection_id=1,
        source="/src",
        sources=["/src1", "/src2"],
        destination="/dst",
        file_types="raw",
        folder_template="%Y",
        skip_duplicates=False,
        labels_file="/labels.txt",
        labels_files=["/a.txt", "/b.txt"],
        model_id="bioclip-2",
        model_ids=["bioclip-2", "timm-inat21-eva02-l"],
        reclassify=True,
        skip_extract_masks=True,
        skip_regroup=True,
        skip_classify=True,
        preview_max_size=2560,
    )
    assert params.source == "/src"
    assert params.sources == ["/src1", "/src2"]
    assert params.destination == "/dst"
    assert params.file_types == "raw"
    assert params.model_ids == ["bioclip-2", "timm-inat21-eva02-l"]
    assert params.reclassify is True
    assert params.skip_extract_masks is True
    assert params.skip_regroup is True
    assert params.skip_classify is True
    assert params.preview_max_size == 2560


def test_pipeline_params_model_ids_defaults_none():
    """model_ids defaults to None (single-model / back-compat path)."""
    params = PipelineParams(collection_id=1)
    assert params.model_ids is None


def test_run_pipeline_job_rejects_retired_import_archive_params(tmp_path):
    import pytest

    cases = [
        PipelineParams(destination=str(tmp_path / "archive")),
        PipelineParams(local_processing=True),
        PipelineParams(remote_target_id="nas1"),
    ]

    for params in cases:
        with pytest.raises(RuntimeError, match="Pipeline import/archive mode"):
            run_pipeline_job(
                _make_job(), FakeRunner(), str(tmp_path / "test.db"), 1, params
            )


def test_archive_stage_rejects_retired_import_archive_params_late(
    tmp_path, monkeypatch
):
    import pipeline_job
    import pytest
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    col_id = db.add_collection("Empty", "[]")
    params = PipelineParams(
        collection_id=col_id,
        skip_classify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    @contextlib.contextmanager
    def mutate_after_regroup(_workspace_id):
        yield
        params.destination = str(tmp_path / "archive")

    monkeypatch.setattr(pipeline_job, "acquire_workspace_regroup", mutate_after_regroup)

    with pytest.raises(RuntimeError, match="Pipeline import/archive mode"):
        run_pipeline_job(_make_job(), FakeRunner(), db_path, ws_id, params)


def test_pipeline_job_with_collection_skips_scan(tmp_path, monkeypatch):
    """When collection_id is provided, pipeline should skip scan entirely."""
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    # Create an empty collection so classify has something to query
    col_id = db.add_collection("Test", "[]")

    params = PipelineParams(
        collection_id=col_id,
        skip_classify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()

    result = run_pipeline_job(job, runner, db_path, ws_id, params)

    assert isinstance(result, dict)
    # Should have stage results
    assert "stages" in result
    # Duration should be tracked
    assert "duration" in result
    assert result["duration"] >= 0
    # Scan should not have run (collection_id was provided)
    # Check that no scan events were emitted with phase "Scanning photos"
    scan_events = [
        e for e in runner.events
        if e[1] == "progress" and e[2].get("phase") == "Scanning photos"
    ]
    assert len(scan_events) == 0


def test_pipeline_abort_event_stops_stages():
    """Setting pipeline_abort should cause _should_abort to return True."""
    from pipeline_job import _should_abort

    abort = threading.Event()
    assert not _should_abort(abort)
    abort.set()
    assert _should_abort(abort)


def test_pipeline_cancel_via_runner_skips_remaining_stages(tmp_path, monkeypatch):
    """When runner.is_cancelled returns True, the pipeline watcher should set
    the local abort event, and remaining stages should bail without raising."""
    import config as cfg
    from db import Database
    from PIL import Image

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    photo_dir = tmp_path / "photos"
    photo_dir.mkdir()
    for name in ["a.jpg", "b.jpg", "c.jpg"]:
        Image.new("RGB", (50, 50), "red").save(str(photo_dir / name))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    params = PipelineParams(
        source=str(photo_dir),
        skip_classify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()
    # Pre-cancel the job: the watcher thread should pick this up almost
    # immediately and set abort.
    runner.cancelled_ids.add(job["id"])

    result = run_pipeline_job(job, runner, db_path, ws_id, params)

    assert isinstance(result, dict)
    assert "duration" in result
    # The pipeline should return without raising. It may still have run scan
    # (no interruption hook in scanner), but classify/extract_masks/regroup
    # were skip_* anyway, so this just verifies graceful completion under
    # cancellation.


@pytest.mark.skip(reason="retired pipeline import/archive destination path")
def test_pipeline_abort_on_nonexistent_source(tmp_path, monkeypatch):
    """Pipeline with nonexistent source should complete gracefully.

    The scanner silently returns for nonexistent dirs (no photos found).
    With skip_classify=True we bypass model_loader (no model in test env),
    so the pipeline finishes without raising. If any stage regresses into
    a real failure, the fail-propagation path in run_pipeline_job now
    raises, which also fails the test.
    """
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    params = PipelineParams(
        source=str(tmp_path / "nonexistent_dir"),
        destination=str(tmp_path / "dest"),
        skip_classify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()

    result = run_pipeline_job(job, runner, db_path, ws_id, params)

    assert isinstance(result, dict)
    assert "duration" in result
    # With skip_classify set, the scanner should handle the missing source
    # gracefully and end without error. If this regresses — i.e. a real stage
    # failure creeps in — the pipeline now raises, which also fails the test.


def test_pipeline_scan_thumbnail_collection_stages(tmp_path, monkeypatch):
    """Pipeline should scan photos, generate thumbnails, and create collection."""
    import config as cfg
    from db import Database
    from PIL import Image

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    # Create test images
    photo_dir = tmp_path / "photos"
    photo_dir.mkdir()
    for name in ["a.jpg", "b.jpg", "c.jpg"]:
        img = Image.new("RGB", (100, 100), "red")
        img.save(str(photo_dir / name))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    params = PipelineParams(
        source=str(photo_dir),
        skip_classify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()

    result = run_pipeline_job(job, runner, db_path, ws_id, params)

    assert isinstance(result, dict)
    assert "stages" in result

    # Thumbnails should have been generated
    thumb_dir = os.path.join(os.path.dirname(db_path), "thumbnails")
    assert os.path.isdir(thumb_dir)
    thumb_result = result["stages"].get("thumbnails", {})
    assert thumb_result.get("generated", 0) == 3

    # A collection should have been created
    assert "collection_id" in result

    # Verify collection exists in DB
    db2 = Database(db_path)
    db2.set_active_workspace(ws_id)
    photos = db2.get_collection_photos(result["collection_id"], per_page=999999)
    assert len(photos) == 3


def test_pipeline_scan_invokes_missing_originals_invalidator(tmp_path, monkeypatch):
    """Pipeline scans must invalidate the Missing Originals cache too.

    Regression: the standalone /api/jobs/scan and /api/jobs/import-* routes
    already drop the cached ``GET /api/photos/missing`` payload once their
    ``do_scan`` runs, but a Process/pipeline scan went through the same
    scanner.scan() call in pipeline_job without wiring the invalidator.
    If a ready ghost payload existed and the user restored or deleted an
    original before running Process, the banner/modal continued to show
    the pre-pipeline photo list until an unrelated Missing Originals scan
    replaced the entry. See Codex review on 63f6ac78.
    """
    import config as cfg
    from db import Database
    from PIL import Image

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    photo_dir = tmp_path / "photos"
    photo_dir.mkdir()
    Image.new("RGB", (32, 32), "black").save(str(photo_dir / "keep.jpg"))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    params = PipelineParams(
        source=str(photo_dir),
        skip_classify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()

    invalidator_calls = []

    def fake_invalidator():
        invalidator_calls.append(True)

    run_pipeline_job(
        job, runner, db_path, ws_id, params,
        missing_originals_invalidator=fake_invalidator,
    )

    # scanner.scan committed a photo row for keep.jpg, so a ready
    # Missing Originals payload computed before this run is now
    # potentially stale — the invalidator must fire at least once for
    # the scanned root. Matches the try/finally in api_job_scan and
    # api_job_import_full.
    assert invalidator_calls, (
        "missing_originals_invalidator was not called after the "
        "pipeline scan touched disk"
    )


def test_pipeline_collection_repair_scan_invokes_missing_originals_invalidator(
    tmp_path, monkeypatch,
):
    """Collection-mode metadata repair scans must invalidate the cache too.

    Regression: when ``skip_scan`` is on (a collection-scoped Process run)
    and ``_find_broken_metadata_folders`` flags rows, the pipeline runs a
    targeted ``do_scan()`` against the affected folder to repair metadata.
    That call touches disk and can revalidate a restored original that a
    ready ``/api/photos/missing`` payload still lists as a ghost, but the
    repair path did not append its folder to ``scanned_roots`` — so the
    outer finally's Missing Originals invalidation never fired for repair
    scans. Codex review on 7fec89bd.
    """
    import config as cfg
    from db import Database
    from PIL import Image

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    photo_dir = tmp_path / "photos"
    photo_dir.mkdir()
    photo_path = photo_dir / "broken.jpg"
    Image.new("RGB", (32, 32), "black").save(str(photo_path))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    folder_id = db.add_folder(str(photo_dir), name="photos")
    photo_id = db.add_photo(
        folder_id=folder_id,
        filename="broken.jpg",
        extension=".jpg",
        file_size=os.path.getsize(photo_path),
        file_mtime=os.path.getmtime(photo_path),
        width=32,
        height=32,
    )
    # Force _find_broken_metadata_folders to flag this row so the pipeline
    # takes the repair branch instead of the "Skipped (using collection)"
    # early return.
    db.conn.execute(
        "UPDATE photos SET timestamp=NULL WHERE id=?", (photo_id,),
    )
    db.conn.commit()

    collection_id = db.add_collection(
        "Repair me",
        json.dumps([{"field": "photo_ids", "op": "in", "value": [photo_id]}]),
    )

    params = PipelineParams(
        collection_id=collection_id,
        skip_classify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    invalidator_calls = []

    def fake_invalidator():
        invalidator_calls.append(True)

    run_pipeline_job(
        _make_job(), FakeRunner(), db_path, ws_id, params,
        missing_originals_invalidator=fake_invalidator,
    )

    assert invalidator_calls, (
        "missing_originals_invalidator was not called after the "
        "collection-mode metadata repair scan"
    )


def test_pipeline_scan_invalidator_is_optional(tmp_path, monkeypatch):
    """Callers that don't pass ``missing_originals_invalidator`` still work.

    The parameter defaults to None so tests and any non-Flask harness can
    call run_pipeline_job without wiring the app-level invalidator. This
    guards the default path against a NoneType-not-callable regression.
    """
    import config as cfg
    from db import Database
    from PIL import Image

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    photo_dir = tmp_path / "photos"
    photo_dir.mkdir()
    Image.new("RGB", (32, 32), "black").save(str(photo_dir / "keep.jpg"))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    params = PipelineParams(
        source=str(photo_dir),
        skip_classify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    # No missing_originals_invalidator passed — must not raise.
    result = run_pipeline_job(job=_make_job(), runner=FakeRunner(),
                              db_path=db_path, workspace_id=ws_id,
                              params=params)
    assert isinstance(result, dict)


def test_pipeline_scan_swallows_invalidator_exceptions(tmp_path, monkeypatch):
    """A failing invalidator must not abort the pipeline finally block.

    The finally block also invalidates the new-images cache; if the
    missing-originals callback raised through, subsequent bookkeeping
    (SSE sentinel, stage-status update) would be skipped and the
    pipeline would hang. Mirror the try/except log-and-continue guard
    already applied to invalidate_new_images_after_scan.
    """
    import config as cfg
    from db import Database
    from PIL import Image

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    photo_dir = tmp_path / "photos"
    photo_dir.mkdir()
    Image.new("RGB", (32, 32), "black").save(str(photo_dir / "keep.jpg"))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    params = PipelineParams(
        source=str(photo_dir),
        skip_classify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    def raising_invalidator():
        raise RuntimeError("boom")

    # The pipeline must finish normally even though the invalidator
    # raised — the exception is logged and swallowed inside the finally
    # block, same as the new-images invalidation.
    result = run_pipeline_job(
        _make_job(), FakeRunner(), db_path, ws_id, params,
        missing_originals_invalidator=raising_invalidator,
    )
    assert isinstance(result, dict)


def test_pipeline_stages_dict_in_progress_events(tmp_path, monkeypatch):
    """Progress events should include a 'stages' dict showing all stage statuses."""
    import config as cfg
    from db import Database
    from PIL import Image

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    photo_dir = tmp_path / "photos"
    photo_dir.mkdir()
    img = Image.new("RGB", (100, 100), "red")
    img.save(str(photo_dir / "test.jpg"))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    params = PipelineParams(
        source=str(photo_dir),
        skip_classify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()

    run_pipeline_job(job, runner, db_path, ws_id, params)

    # Find progress events with stages dict
    stage_events = [
        e for e in runner.events
        if e[1] == "progress" and "stages" in e[2]
    ]
    assert len(stage_events) > 0

    # Each stages dict should have all expected stage keys
    expected_keys = {"ingest", "scan", "thumbnails", "previews", "model_loader", "classify", "extract_masks", "regroup"}
    for _, _, data in stage_events:
        assert expected_keys.issubset(data["stages"].keys())


# ---------------------------------------------------------------------------
# Integration tests — full pipeline end-to-end
# ---------------------------------------------------------------------------


def test_pipeline_scan_and_thumbnail_overlap(tmp_path, monkeypatch):
    """Scan and thumbnail stages should both process photos from a real dir."""
    import config as cfg
    from db import Database
    from PIL import Image

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    # Create 5 test images
    photo_dir = tmp_path / "photos"
    photo_dir.mkdir()
    for i in range(5):
        img = Image.new("RGB", (100, 100), "blue")
        img.save(str(photo_dir / f"photo_{i}.jpg"))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    params = PipelineParams(
        source=str(photo_dir),
        skip_classify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()

    result = run_pipeline_job(job, runner, db_path, ws_id, params)

    # Result should have stages dict
    assert result is not None
    assert isinstance(result["stages"], dict)

    # Scan should have found photos — check via progress events or thumbnail count
    scan_events = [
        e for e in runner.events
        if isinstance(e[2], dict) and e[2].get("phase", "").startswith("Scanning")
    ]
    assert (
        len(scan_events) > 0
        or result["stages"].get("thumbnails", {}).get("generated", 0) > 0
    )

    # Thumbnails should have been generated on the filesystem
    thumb_dir = os.path.join(os.path.dirname(db_path), "thumbnails")
    assert os.path.isdir(thumb_dir)
    thumb_files = [f for f in os.listdir(thumb_dir) if not f.startswith(".")]
    assert len(thumb_files) == 5


def test_pipeline_skips_scan_with_collection_id(tmp_path, monkeypatch):
    """When collection_id is given, no scan-phase events should be emitted."""
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    col_id = db.add_collection("Test", json.dumps([]))

    params = PipelineParams(
        collection_id=col_id,
        skip_classify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()

    result = run_pipeline_job(job, runner, db_path, ws_id, params)

    assert result is not None
    # No scan-phase events should have been emitted
    scan_events = [
        e for e in runner.events
        if isinstance(e[2], dict) and "Scanning" in e[2].get("phase", "")
    ]
    assert len(scan_events) == 0


def test_pipeline_nonexistent_source_scans_nothing(tmp_path, monkeypatch):
    """Pipeline with a nonexistent source should complete with 0 photos scanned."""
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    params = PipelineParams(
        source="/nonexistent/path/that/does/not/exist",
        skip_classify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()

    result = run_pipeline_job(job, runner, db_path, ws_id, params)

    assert result is not None
    # No collection created since no photos were found
    assert result.get("collection_id") is None


def test_pipeline_result_has_duration(tmp_path, monkeypatch):
    """Pipeline result dict should always contain a positive duration."""
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    col_id = db.add_collection("Empty", json.dumps([]))

    params = PipelineParams(
        collection_id=col_id,
        skip_classify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()

    result = run_pipeline_job(job, runner, db_path, ws_id, params)

    assert "duration" in result
    assert isinstance(result["duration"], float)
    assert result["duration"] >= 0


def test_pipeline_collection_created_after_scan(tmp_path, monkeypatch):
    """Pipeline should create a collection from scanned photos."""
    import config as cfg
    from db import Database
    from PIL import Image

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    # Create test images
    photo_dir = tmp_path / "photos"
    photo_dir.mkdir()
    for name in ["bird1.jpg", "bird2.jpg", "bird3.jpg"]:
        img = Image.new("RGB", (80, 80), "green")
        img.save(str(photo_dir / name))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    params = PipelineParams(
        source=str(photo_dir),
        skip_classify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()

    result = run_pipeline_job(job, runner, db_path, ws_id, params)

    # collection_id should be in the result
    assert "collection_id" in result
    assert isinstance(result["collection_id"], int)

    # Verify the collection exists in the DB and has the right photos
    db2 = Database(db_path)
    db2.set_active_workspace(ws_id)
    photos = db2.get_collection_photos(result["collection_id"], per_page=999999)
    assert len(photos) == 3


def test_pipeline_previews_stage_runs(tmp_path, monkeypatch):
    """Pipeline should run a previews stage after thumbnails."""
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    # Create an empty collection so classify has something to query
    col_id = db.add_collection("Test", "[]")

    # Patch scanner, thumbnails, etc. to be no-ops — we just need to verify
    # the previews stage appears in the result
    params = PipelineParams(
        collection_id=col_id,
        skip_classify=True,
        skip_extract_masks=True,
        skip_regroup=True,
        preview_max_size=1920,
    )

    runner = FakeRunner()
    job = _make_job()
    result = run_pipeline_job(job, runner, db_path, ws_id, params)

    # The stages dict in progress events should include "previews"
    stage_events = [e[2]["stages"] for e in runner.events
                    if e[1] == "progress" and "stages" in e[2]]
    assert any("previews" in s for s in stage_events), \
        "Expected 'previews' stage in progress events"


def test_pipeline_previews_stage_writes_atomically(tmp_path, monkeypatch):
    """previews_stage must write to a sibling temp file then os.replace into
    the deterministic ``previews/{id}_{max_size}.jpg`` path.

    With SLOT_CAP > 1, two pipelines processing the same photo can both miss
    the os.path.exists() cache check and race on the same deterministic path.
    A direct img.save(cache_path) would interleave/truncate bytes, leaving a
    corrupt JPEG that preview_cache claims is valid. Regression for Codex
    P2 review on PR #907.
    """
    import config as cfg
    from db import Database
    from PIL import Image

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    photo_dir = tmp_path / "photos"
    photo_dir.mkdir()
    Image.new("RGB", (100, 100), "red").save(str(photo_dir / "a.jpg"))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    # Capture atomic publications. The shared materializer encodes in memory,
    # fsyncs a sibling temp file, then makes that file visible with replace.
    replacements = []
    real_replace = os.replace

    def tracking_replace(source, destination):
        replacements.append((os.fspath(source), os.fspath(destination)))
        return real_replace(source, destination)

    monkeypatch.setattr(os, "replace", tracking_replace)

    params = PipelineParams(
        source=str(photo_dir),
        skip_classify=True,
        skip_extract_masks=True,
        skip_regroup=True,
        preview_max_size=1920,
    )

    runner = FakeRunner()
    job = _make_job()
    run_pipeline_job(job, runner, db_path, ws_id, params)

    preview_dir = os.path.join(os.path.dirname(db_path), "previews")
    db2 = Database(db_path)
    db2.set_active_workspace(ws_id)
    photo_id = db2.get_photos(per_page=1)[0]["id"]
    final_path = os.path.join(preview_dir, f"{photo_id}_1920.jpg")

    # The final preview must exist and be a complete, openable JPEG (proves
    # os.replace ran).
    assert os.path.isfile(final_path)
    with Image.open(final_path) as img:
        img.verify()

    preview_publications = [
        (source, destination)
        for source, destination in replacements
        if destination == final_path
    ]
    assert len(preview_publications) == 1
    temporary, destination = preview_publications[0]
    assert os.path.dirname(temporary) == preview_dir
    assert temporary.endswith(".jpg.tmp")
    assert destination == final_path


def test_pipeline_previews_stage_bounds_non_crop_recipe_loads(tmp_path, monkeypatch):
    import config as cfg
    import image_loader
    from db import Database
    from PIL import Image

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    photo_dir = tmp_path / "photos"
    photo_dir.mkdir()
    source_path = photo_dir / "edited.jpg"
    Image.new("RGB", (800, 600), "red").save(source_path, "JPEG")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    folder_id = db.add_folder(str(photo_dir), name="photos")
    photo_id = db.add_photo(
        folder_id=folder_id,
        filename="edited.jpg",
        extension=".jpg",
        file_size=os.path.getsize(source_path),
        file_mtime=os.path.getmtime(source_path),
        width=800,
        height=600,
    )
    db.set_photo_edit_recipe(photo_id, {"rotation": 90})
    collection_id = db.add_collection(
        "Edited",
        json.dumps([{"field": "photo_ids", "op": "in", "value": [photo_id]}]),
    )

    original_load_image = image_loader.load_image
    seen_max_sizes = []

    def tracking_load_image(file_path, max_size=1024):
        seen_max_sizes.append(max_size)
        return original_load_image(file_path, max_size=max_size)

    monkeypatch.setattr(image_loader, "load_image", tracking_load_image)

    runner = FakeRunner()
    job = _make_job()
    run_pipeline_job(
        job,
        runner,
        db_path,
        ws_id,
        PipelineParams(
            collection_id=collection_id,
            skip_classify=True,
            skip_extract_masks=True,
            skip_regroup=True,
            preview_max_size=1920,
        ),
    )

    assert seen_max_sizes[-1] == 1920


def test_pipeline_params_sources_used_over_source():
    """When sources is provided, it should take precedence over source."""
    params = PipelineParams(source="/single", sources=["/a", "/b"])
    assert params.sources == ["/a", "/b"]


def test_pipeline_skip_classify_skips_model_loader(tmp_path, monkeypatch):
    """When skip_classify=True, model_loader and classify should be skipped."""
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    params = PipelineParams(
        collection_id=1,
        skip_classify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()
    result = run_pipeline_job(job, runner, db_path, ws_id, params)

    # Check that classify was skipped in the last stages event
    last_stages = None
    for _, evt_type, data in reversed(runner.events):
        if evt_type == "progress" and "stages" in data:
            last_stages = data["stages"]
            break

    assert last_stages is not None
    assert last_stages["classify"]["status"] == "skipped"
    assert last_stages["model_loader"]["status"] == "skipped"


def test_pipeline_passes_recursive_false_to_scan(tmp_path, monkeypatch):
    """Pipeline forwards recursive=False to scanner.scan()."""
    import config as cfg
    from db import Database
    from pipeline_job import PipelineParams, run_pipeline_job

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    src = tmp_path / "photos"
    src.mkdir()
    (src / "img.jpg").write_bytes(b"\xff\xd8\xff\xe0" + b"\x00" * 100)

    scan_kwargs = {}

    def fake_scan(root, db_arg, **kwargs):
        scan_kwargs.update(kwargs)

    monkeypatch.setattr("scanner.scan", fake_scan)

    params = PipelineParams(
        source=str(src),
        recursive=False,
        skip_classify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()

    run_pipeline_job(job, runner, db_path, ws_id, params)

    assert scan_kwargs.get("recursive") is False


def test_pipeline_passes_vireo_dir_to_scan(tmp_path, monkeypatch):
    """Pipeline must forward vireo_dir to scanner.scan() so the
    content-change cache invalidation (thumbnail/working-copy/preview)
    actually fires for pipeline-triggered rescans.

    Without this, _invalidate_derived_caches short-circuits (guard:
    ``if not vireo_dir: return``) and the bird/squirrel divergence this
    PR fixes still occurs for anyone using the pipeline to scan.
    """
    import config as cfg
    from db import Database
    from pipeline_job import PipelineParams, run_pipeline_job

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "vireo.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    src = tmp_path / "photos"
    src.mkdir()
    (src / "img.jpg").write_bytes(b"\xff\xd8\xff\xe0" + b"\x00" * 100)

    scan_kwargs = {}

    def fake_scan(root, db_arg, **kwargs):
        scan_kwargs.update(kwargs)

    monkeypatch.setattr("scanner.scan", fake_scan)

    params = PipelineParams(
        source=str(src),
        recursive=False,
        skip_classify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()

    run_pipeline_job(job, runner, db_path, ws_id, params)

    assert scan_kwargs.get("vireo_dir") == os.path.dirname(db_path), (
        "Pipeline must pass vireo_dir (the DB's parent dir) to scan() so "
        "derived-cache invalidation is reachable on pipeline rescans."
    )


def test_pipeline_forwards_thumb_cache_dir_to_scan(tmp_path, monkeypatch):
    """Pipeline must forward the configured thumb_cache_dir to scanner.scan().

    ``--thumb-dir`` can point outside ``vireo_dir/thumbnails`` — scanner's
    invalidation now accepts a ``thumb_cache_dir`` override for exactly
    that reason. If pipeline scans drop it, the default fallback
    (``vireo_dir/thumbnails``) targets the wrong directory on custom
    layouts and stale thumbnails survive.
    """
    import config as cfg
    from db import Database
    from pipeline_job import PipelineParams, run_pipeline_job

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "vireo.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    src = tmp_path / "photos"
    src.mkdir()
    (src / "img.jpg").write_bytes(b"\xff\xd8\xff\xe0" + b"\x00" * 100)

    scan_kwargs = {}

    def fake_scan(root, db_arg, **kwargs):
        scan_kwargs.update(kwargs)

    monkeypatch.setattr("scanner.scan", fake_scan)

    custom_thumb_dir = str(tmp_path / "custom-thumbs")

    params = PipelineParams(
        source=str(src),
        recursive=False,
        skip_classify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )
    runner = FakeRunner()
    job = _make_job()

    run_pipeline_job(
        job, runner, db_path, ws_id, params,
        thumb_cache_dir=custom_thumb_dir,
    )

    assert scan_kwargs.get("thumb_cache_dir") == custom_thumb_dir, (
        "Pipeline must thread the configured thumb_cache_dir to scan() "
        "so invalidation targets the real cache on custom --thumb-dir "
        "layouts."
    )


def test_pipeline_vireo_dir_aligns_with_thumb_cache_dir_parent(tmp_path, monkeypatch):
    """On custom --thumb-dir layouts, the Flask serve path computes
    ``vireo_dir = os.path.dirname(THUMB_CACHE_DIR)`` — that's where it
    reads previews/ and working/. Pipeline scans must align with that
    convention, or invalidation runs against one tree while the app
    serves from another (so stale previews/working_copies survive).
    """
    import config as cfg
    from db import Database
    from pipeline_job import PipelineParams, run_pipeline_job

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    # DB and thumb cache on *different* roots (simulates
    # --db ~/.vireo/vireo.db --thumb-dir /data/thumbs).
    db_dir = tmp_path / "dbstore"
    db_dir.mkdir()
    db_path = str(db_dir / "vireo.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    custom_thumb_dir = tmp_path / "cache" / "thumbs"
    custom_thumb_dir.mkdir(parents=True)

    src = tmp_path / "photos"
    src.mkdir()
    (src / "img.jpg").write_bytes(b"\xff\xd8\xff\xe0" + b"\x00" * 100)

    scan_kwargs = {}

    def fake_scan(root, db_arg, **kwargs):
        scan_kwargs.update(kwargs)

    monkeypatch.setattr("scanner.scan", fake_scan)

    params = PipelineParams(
        source=str(src),
        recursive=False,
        skip_classify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )
    runner = FakeRunner()
    job = _make_job()

    run_pipeline_job(
        job, runner, db_path, ws_id, params,
        thumb_cache_dir=str(custom_thumb_dir),
    )

    expected_vireo_dir = os.path.dirname(str(custom_thumb_dir))
    assert scan_kwargs.get("vireo_dir") == expected_vireo_dir, (
        f"Pipeline should derive vireo_dir from thumb_cache_dir's parent "
        f"({expected_vireo_dir}); got {scan_kwargs.get('vireo_dir')!r}. "
        "Otherwise scan's previews/working paths diverge from the "
        "Flask serve paths."
    )


def test_pipeline_scan_progress_includes_rate_and_eta(tmp_path, monkeypatch):
    """Scan progress events should include rate and eta_seconds fields."""
    import time

    import config as cfg
    from db import Database
    from jobs import JobRunner
    from pipeline_job import PipelineParams, run_pipeline_job

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    from PIL import Image
    src = tmp_path / "photos"
    src.mkdir()
    for i in range(12):
        Image.new("RGB", (40, 40), "blue").save(str(src / f"img{i:02d}.jpg"))

    runner = JobRunner()
    progress_events = []
    orig_push = runner.push_event

    def capture_push(job_id, event_type, data):
        if event_type == "progress" and data.get("phase") == "Scanning photos":
            progress_events.append(data)
        orig_push(job_id, event_type, data)

    monkeypatch.setattr(runner, "push_event", capture_push)

    params = PipelineParams(
        source=str(src),
        skip_classify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    job = {
        "id": "test-scan-rate",
        "type": "pipeline",
        "status": "running",
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "finished_at": None,
        "progress": {"current": 0, "total": 0, "current_file": ""},
        "result": None,
        "errors": [],
        "config": {},
        "workspace_id": ws_id,
        "steps": [],
    }

    run_pipeline_job(job, runner, db_path, ws_id, params)

    assert len(progress_events) > 0, "Expected at least one scan progress event"
    last = progress_events[-1]
    assert "rate" in last, "Progress event should include rate"
    assert "eta_seconds" in last, "Progress event should include eta_seconds"
    assert isinstance(last["rate"], int | float)
    assert isinstance(last["eta_seconds"], int | float)


def test_pipeline_multi_folder_scan_progress_is_monotonic(tmp_path, monkeypatch):
    """Scan progress must not move backward at folder boundaries.

    When sources is a list of folders, pipeline_job loops calling scan()
    once per folder. Each scan() reports progress as local (current, total).
    The weighted overall bar reads stages["scan"]["count"]/.total, so if
    those get overwritten rather than accumulated, the UI progress jumps
    backward when folder N+1 starts.
    """
    import time

    import config as cfg
    from db import Database
    from jobs import JobRunner
    from pipeline_job import PipelineParams, run_pipeline_job

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    from PIL import Image
    folder_a = tmp_path / "folderA"
    folder_a.mkdir()
    for i in range(6):
        Image.new("RGB", (40, 40), "blue").save(str(folder_a / f"a{i:02d}.jpg"))
    folder_b = tmp_path / "folderB"
    folder_b.mkdir()
    for i in range(6):
        Image.new("RGB", (40, 40), "red").save(str(folder_b / f"b{i:02d}.jpg"))

    runner = JobRunner()
    scan_counts = []
    scan_totals = []
    orig_push = runner.push_event

    def capture_push(job_id, event_type, data):
        if event_type == "progress":
            stages = data.get("stages") or {}
            scan_info = stages.get("scan") or {}
            if scan_info.get("status") == "running":
                scan_counts.append(scan_info.get("count") or 0)
                scan_totals.append(scan_info.get("total") or 0)
        orig_push(job_id, event_type, data)

    monkeypatch.setattr(runner, "push_event", capture_push)

    params = PipelineParams(
        sources=[str(folder_a), str(folder_b)],
        skip_classify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    job = {
        "id": "test-multi-scan-mono",
        "type": "pipeline",
        "status": "running",
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "finished_at": None,
        "progress": {"current": 0, "total": 0, "current_file": ""},
        "result": None,
        "errors": [],
        "config": {},
        "workspace_id": ws_id,
        "steps": [],
    }

    run_pipeline_job(job, runner, db_path, ws_id, params)

    assert len(scan_counts) > 0, "Expected at least one running scan progress event"
    for i in range(1, len(scan_counts)):
        assert scan_counts[i] >= scan_counts[i - 1], (
            f"scan count moved backward: {scan_counts[i - 1]} -> "
            f"{scan_counts[i]} at event {i}; full sequence={scan_counts}"
        )
    for i in range(1, len(scan_totals)):
        assert scan_totals[i] >= scan_totals[i - 1], (
            f"scan total moved backward: {scan_totals[i - 1]} -> "
            f"{scan_totals[i]} at event {i}; full sequence={scan_totals}"
        )
    assert scan_totals[-1] >= 12, (
        f"final scan total should cover both folders (>=12), got {scan_totals[-1]}"
    )


@pytest.mark.skip(reason="retired pipeline import/archive destination path")
def test_pipeline_multi_source_ingest_progress_is_monotonic(tmp_path, monkeypatch):
    """Ingest progress must not move backward at source folder boundaries.

    Copy mode with sources=[folderA, folderB] calls do_ingest() once per
    folder. Each call reports (current, total) local to that folder. The
    weighted overall bar reads stages["ingest"]["count"]/.total, so if
    those get overwritten rather than accumulated, overall progress
    rewinds each time a new source starts — the exact regression the
    scan accumulator already prevents. Same treatment needed for ingest.
    """
    import time

    import config as cfg
    from db import Database
    from jobs import JobRunner
    from pipeline_job import PipelineParams, run_pipeline_job

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    from PIL import Image
    src_a = tmp_path / "srcA"
    src_a.mkdir()
    for i in range(5):
        Image.new("RGB", (40, 40), "blue").save(str(src_a / f"a{i:02d}.jpg"))
    src_b = tmp_path / "srcB"
    src_b.mkdir()
    for i in range(5):
        Image.new("RGB", (40, 40), "red").save(str(src_b / f"b{i:02d}.jpg"))
    dest = tmp_path / "dest"
    dest.mkdir()

    runner = JobRunner()
    ingest_counts = []
    ingest_totals = []
    orig_push = runner.push_event

    def capture_push(job_id, event_type, data):
        if event_type == "progress":
            stages = data.get("stages") or {}
            ingest_info = stages.get("ingest") or {}
            if ingest_info.get("status") == "running":
                ingest_counts.append(ingest_info.get("count") or 0)
                ingest_totals.append(ingest_info.get("total") or 0)
        orig_push(job_id, event_type, data)

    monkeypatch.setattr(runner, "push_event", capture_push)

    params = PipelineParams(
        sources=[str(src_a), str(src_b)],
        destination=str(dest),
        skip_classify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    job = {
        "id": "test-multi-ingest-mono",
        "type": "pipeline",
        "status": "running",
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "finished_at": None,
        "progress": {"current": 0, "total": 0, "current_file": ""},
        "result": None,
        "errors": [],
        "config": {},
        "workspace_id": ws_id,
        "steps": [],
    }

    run_pipeline_job(job, runner, db_path, ws_id, params)

    assert len(ingest_counts) > 0, "Expected at least one running ingest progress event"
    for i in range(1, len(ingest_counts)):
        assert ingest_counts[i] >= ingest_counts[i - 1], (
            f"ingest count moved backward: {ingest_counts[i - 1]} -> "
            f"{ingest_counts[i]} at event {i}; full sequence={ingest_counts}"
        )
    for i in range(1, len(ingest_totals)):
        assert ingest_totals[i] >= ingest_totals[i - 1], (
            f"ingest total moved backward: {ingest_totals[i - 1]} -> "
            f"{ingest_totals[i]} at event {i}; full sequence={ingest_totals}"
        )
    assert ingest_totals[-1] >= 10, (
        f"final ingest total should cover both sources (>=10), got {ingest_totals[-1]}"
    )


def test_progress_lock_held_during_update_stages_push():
    """`_update_stages` must call `push_event` while holding `_progress_lock`.

    The lock makes (snapshot stages, append event) atomic across pipeline
    threads. Without it, a thread can build the stages snapshot, get
    preempted between the dict comprehension and the push_event call, and
    finally land its stale snapshot after another thread has already
    pushed events with newer counts — producing a non-monotonic captured
    sequence (the trailing stale `5` after `10` that flaked
    test_pipeline_multi_source_ingest_progress_is_monotonic on CI).
    """
    import pipeline_job as pj

    seen = []

    class TrackingRunner:
        def push_event(self, job_id, event_type, data):
            seen.append(pj._progress_lock.locked())

    stages = {
        "ingest": {
            "count": 0, "status": "running", "weight": 1.0, "label": "Ingest",
        },
    }
    pj._update_stages(TrackingRunner(), "job-1", stages)

    assert seen == [True], (
        "_update_stages must call push_event while holding _progress_lock; "
        f"got lock-held sequence {seen}"
    )


def test_emit_progress_lock_held_during_push():
    """The `_emit_progress` helper (used by per-stage cb pushes) must take
    the same lock so its snapshot+push is atomic with `_update_stages`."""
    import pipeline_job as pj

    seen = []

    class TrackingRunner:
        def push_event(self, job_id, event_type, data):
            seen.append(pj._progress_lock.locked())

    stages = {
        "ingest": {
            "count": 5, "status": "running", "weight": 1.0, "label": "Ingest",
        },
    }
    pj._emit_progress(TrackingRunner(), "job-1", stages, "ingest", "Importing")

    assert seen == [True], (
        "_emit_progress must call push_event while holding _progress_lock; "
        f"got lock-held sequence {seen}"
    )


@pytest.mark.skip(reason="retired pipeline import/archive destination path")
def test_pipeline_ingest_updates_step_progress(tmp_path, monkeypatch):
    """Ingest (import) phase should call update_step so the jobs page shows progress."""
    import config as cfg
    from db import Database
    from PIL import Image

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    # Create source images
    src = tmp_path / "source"
    src.mkdir()
    for name in ["a.jpg", "b.jpg"]:
        img = Image.new("RGB", (100, 100), "red")
        img.save(str(src / name))

    dest = tmp_path / "dest"
    dest.mkdir()

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    params = PipelineParams(
        source=str(src),
        destination=str(dest),
        skip_classify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()

    run_pipeline_job(job, runner, db_path, ws_id, params)

    # The ingest step should have received update_step calls with progress
    ingest_progress_updates = [
        (step_id, kwargs) for _, step_id, kwargs in runner.step_updates
        if step_id == "ingest" and "progress" in kwargs
        and kwargs["progress"].get("total", 0) > 0
    ]
    assert len(ingest_progress_updates) > 0, \
        "Ingest phase should call update_step with progress for the ingest step"

    # Ingest step should have been marked completed
    ingest_completed = [
        kwargs for _, step_id, kwargs in runner.step_updates
        if step_id == "ingest" and kwargs.get("status") == "completed"
    ]
    assert len(ingest_completed) > 0, \
        "Ingest step should be marked completed after import finishes"


def test_pipeline_scan_step_gets_status_updates(tmp_path, monkeypatch):
    """Scanner should report status messages (e.g. 'Discovering files...')
    via update_step current_file during blocking phases."""
    import config as cfg
    from db import Database
    from PIL import Image

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    photo_dir = tmp_path / "photos"
    photo_dir.mkdir()
    img = Image.new("RGB", (100, 100), "red")
    img.save(str(photo_dir / "test.jpg"))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    params = PipelineParams(
        source=str(photo_dir),
        skip_classify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()

    run_pipeline_job(job, runner, db_path, ws_id, params)

    # Scanner should have sent a "Discovering files..." status via update_step
    scan_status_messages = [
        kwargs.get("current_file", "")
        for _, step_id, kwargs in runner.step_updates
        if step_id == "scan" and "current_file" in kwargs
    ]
    assert any("Discovering" in msg for msg in scan_status_messages), \
        f"Expected 'Discovering files...' status update, got: {scan_status_messages[:5]}"

    # Status updates should also emit SSE progress events for real-time subscribers
    status_sse_events = [
        e[2] for e in runner.events
        if e[1] == "progress" and "Discovering" in e[2].get("phase", "")
    ]
    assert len(status_sse_events) > 0, \
        "Status updates should also push SSE progress events"


@pytest.mark.skip(reason="retired pipeline import/archive destination path")
def test_pipeline_ingest_records_safe_to_eject_counts_on_success(tmp_path, monkeypatch):
    """Once ingest copies everything off the source with no failures, the
    stage (and final result) should carry 'copied'/'skipped_duplicate' counts
    so the UI can tell the user the source (e.g. an SD card) is safe to eject.
    """
    import config as cfg
    from db import Database
    from PIL import Image

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    src = tmp_path / "source"
    src.mkdir()
    for name, color in [("a.jpg", "red"), ("b.jpg", "blue")]:
        img = Image.new("RGB", (100, 100), color)
        img.save(str(src / name))

    dest = tmp_path / "dest"
    dest.mkdir()

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    params = PipelineParams(
        source=str(src),
        destination=str(dest),
        skip_classify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()

    result = run_pipeline_job(job, runner, db_path, ws_id, params)

    assert result["stages"]["ingest"]["copied"] == 2
    assert result["stages"]["ingest"]["skipped_duplicate"] == 0

    # The live SSE stream should carry the same counts on the completed event.
    ingest_completed_events = [
        e[2]["stages"]["ingest"] for e in runner.events
        if e[1] == "progress" and e[2]["stages"].get("ingest", {}).get("status") == "completed"
    ]
    assert ingest_completed_events, "expected a progress event with ingest completed"
    assert ingest_completed_events[-1]["copied"] == 2


@pytest.mark.skip(reason="retired pipeline import/archive destination path")
def test_pipeline_ingest_omits_safe_to_eject_counts_on_partial_failure(tmp_path, monkeypatch):
    """Plain copy mode (no local_processing) doesn't abort the run when a file
    fails to copy — it still marks the ingest stage 'completed'. The
    safe-to-eject counts must NOT be published in that case, since the source
    still holds a file that never made it to the destination.
    """
    import config as cfg
    import ingest as ingest_module
    from db import Database
    from PIL import Image

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    src = tmp_path / "source"
    src.mkdir()
    for name, color in [("a.jpg", "red"), ("b.jpg", "blue")]:
        img = Image.new("RGB", (100, 100), color)
        img.save(str(src / name))

    dest = tmp_path / "dest"
    dest.mkdir()

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    real_copy2 = ingest_module.shutil.copy2

    def flaky_copy2(source, destination, *args, **kwargs):
        if str(source).endswith("b.jpg"):
            raise OSError("simulated card read error")
        return real_copy2(source, destination, *args, **kwargs)

    monkeypatch.setattr(ingest_module.shutil, "copy2", flaky_copy2)

    params = PipelineParams(
        source=str(src),
        destination=str(dest),
        skip_classify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()

    result = run_pipeline_job(job, runner, db_path, ws_id, params)

    assert "ingest" not in result["stages"] or "copied" not in result["stages"]["ingest"]

    ingest_completed_events = [
        e[2]["stages"]["ingest"] for e in runner.events
        if e[1] == "progress" and e[2]["stages"].get("ingest", {}).get("status") == "completed"
    ]
    assert ingest_completed_events, "expected a progress event with ingest completed"
    assert "copied" not in ingest_completed_events[-1]


@pytest.mark.skip(reason="retired pipeline import/archive destination path")
def test_pipeline_ingest_step_present_only_with_destination(tmp_path, monkeypatch):
    """The 'ingest' step should only appear in step_defs when destination is set."""
    import config as cfg
    from db import Database
    from PIL import Image

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    photo_dir = tmp_path / "photos"
    photo_dir.mkdir()
    img = Image.new("RGB", (100, 100), "red")
    img.save(str(photo_dir / "test.jpg"))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    # Without destination — no ingest step
    runner_no_dest = FakeRunner()
    job = _make_job()
    params = PipelineParams(
        source=str(photo_dir),
        skip_classify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )
    run_pipeline_job(job, runner_no_dest, db_path, ws_id, params)
    step_ids = [s["id"] for s in runner_no_dest.steps_defined]
    assert "ingest" not in step_ids, "ingest step should not appear without destination"

    # With destination — ingest step present
    dest = tmp_path / "dest"
    dest.mkdir()
    runner_dest = FakeRunner()
    job2 = _make_job()
    params2 = PipelineParams(
        source=str(photo_dir),
        destination=str(dest),
        skip_classify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )
    run_pipeline_job(job2, runner_dest, db_path, ws_id, params2)
    step_ids2 = [s["id"] for s in runner_dest.steps_defined]
    assert "ingest" in step_ids2, "ingest step should appear when destination is set"
    assert step_ids2.index("ingest") < step_ids2.index("scan"), \
        "ingest step should come before scan"


@pytest.mark.skip(reason="retired pipeline import/archive destination path")
def test_pipeline_all_duplicates_restricts_scan_to_existing_folders(tmp_path, monkeypatch):
    """When every source file is a duplicate of an existing photo in the DB,
    the scan phase must be restricted to just the folders that hold those
    existing duplicates — not left with restrict_dirs=None, which makes the
    scanner walk the entire destination tree.

    Regression test: user selects N photos from an SD card that have already
    been imported, clicks pipeline, and expects those photos to become linked
    to their current workspace. With restrict_dirs=None the scan either takes
    far too long (17+ minutes for a 50k-file library) or skips folder linking
    entirely for the workspaces the user cares about.
    """
    import shutil

    import config as cfg
    from db import Database
    from PIL import Image

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    # Destination tree with two populated date folders plus an unrelated
    # folder that should NOT be walked by the restricted scan.
    dest = tmp_path / "dest"
    duplicate_home = dest / "2024" / "2024-06-15"
    duplicate_home.mkdir(parents=True)
    unrelated = dest / "2023" / "2023-01-01"
    unrelated.mkdir(parents=True)
    for i in range(2):
        Image.new("RGB", (100, 100), (i * 80, 50, 50)).save(
            str(duplicate_home / f"dup_{i}.jpg")
        )
    Image.new("RGB", (100, 100), "blue").save(str(unrelated / "unrelated.jpg"))

    # Scan so the existing photos land in the DB with their hashes.
    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    from scanner import scan as do_scan
    do_scan(str(dest), db)

    # Source is a fresh directory containing byte-identical copies of the
    # duplicates (same hashes), simulating an SD card that still has the
    # already-imported photos on it.
    src = tmp_path / "source"
    src.mkdir()
    for i in range(2):
        shutil.copy2(
            str(duplicate_home / f"dup_{i}.jpg"),
            str(src / f"dup_{i}.jpg"),
        )

    params = PipelineParams(
        source=str(src),
        destination=str(dest),
        skip_classify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )
    runner = FakeRunner()
    job = _make_job()

    scan_calls = []
    from unittest.mock import patch

    import scanner as scanner_mod
    original_scan = scanner_mod.scan

    def tracking_scan(root, *args, **kwargs):
        scan_calls.append({
            "root": str(root),
            "restrict_dirs": kwargs.get("restrict_dirs"),
        })
        return original_scan(root, *args, **kwargs)

    with patch.object(scanner_mod, "scan", tracking_scan):
        run_pipeline_job(job, runner, db_path, ws_id, params)

    # Find the pipeline's scan-stage call (root == destination).
    pipeline_scans = [c for c in scan_calls if c["root"] == str(dest)]
    assert pipeline_scans, \
        f"Pipeline did not call scan on destination; calls={scan_calls}"
    call = pipeline_scans[-1]

    restrict = call["restrict_dirs"]
    assert restrict is not None, (
        "When every file is a duplicate, pipeline should restrict the scan "
        "to the existing-duplicates' folders instead of walking the entire "
        "destination tree (restrict_dirs=None)."
    )
    restrict_set = set(restrict)
    assert str(duplicate_home) in restrict_set, (
        f"Expected {duplicate_home!r} in restrict_dirs; got {restrict_set!r}"
    )
    assert str(unrelated) not in restrict_set, (
        f"Unrelated folder {unrelated!r} must not be in restrict_dirs; "
        f"got {restrict_set!r}"
    )


@pytest.mark.skip(reason="retired pipeline import/archive destination path")
def test_pipeline_all_duplicates_links_existing_folders_to_workspace(tmp_path, monkeypatch):
    """When every source file is a duplicate, the folders holding those
    existing duplicates should end up linked to the active workspace after
    the pipeline runs — even if the workspace had no folders linked before.
    """
    import shutil

    import config as cfg
    from db import Database
    from PIL import Image

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    dest = tmp_path / "dest"
    dup_folder = dest / "2024" / "2024-06-15"
    dup_folder.mkdir(parents=True)
    for i in range(2):
        Image.new("RGB", (100, 100), (i * 80, 40, 40)).save(
            str(dup_folder / f"dup_{i}.jpg")
        )

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    default_ws = db._active_workspace_id

    from scanner import scan as do_scan
    do_scan(str(dest), db)

    # Switch to a fresh workspace that has no folders.
    other_ws = db.create_workspace("Other")
    db.set_active_workspace(other_ws)
    assert db.get_folder_tree() == []

    src = tmp_path / "source"
    src.mkdir()
    for i in range(2):
        shutil.copy2(
            str(dup_folder / f"dup_{i}.jpg"),
            str(src / f"dup_{i}.jpg"),
        )

    params = PipelineParams(
        source=str(src),
        destination=str(dest),
        skip_classify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )
    runner = FakeRunner()
    job = _make_job()
    run_pipeline_job(job, runner, db_path, other_ws, params)

    # Re-open DB to pick up writes made on the worker thread's own connection.
    db2 = Database(db_path)
    db2.set_active_workspace(other_ws)
    other_folders = {f["path"] for f in db2.get_folder_tree()}
    assert str(dup_folder) in other_folders, (
        f"Expected {dup_folder!r} to be linked to Other workspace after "
        f"pipeline dedupped all source files; got {other_folders!r}"
    )


@pytest.mark.skip(reason="retired pipeline import/archive destination path")
def test_pipeline_copy_mode_scans_subfolders(tmp_path, monkeypatch):
    """After ingest, scan should use restrict_dirs to target only subfolders
    that received files, while keeping the destination as root for folder hierarchy."""
    import config as cfg
    from db import Database
    from PIL import Image

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    # Create source images
    src = tmp_path / "source"
    src.mkdir()
    for name in ["a.jpg", "b.jpg"]:
        img = Image.new("RGB", (100, 100), "red")
        img.save(str(src / name))

    # Create destination with existing files in a different subfolder
    dest = tmp_path / "dest"
    dest.mkdir()
    existing_folder = dest / "2025" / "01-01"
    existing_folder.mkdir(parents=True)
    for i in range(5):
        img = Image.new("RGB", (100, 100), "blue")
        img.save(str(existing_folder / f"existing_{i}.jpg"))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    params = PipelineParams(
        source=str(src),
        destination=str(dest),
        skip_classify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()

    # Track scan() calls
    scan_calls = []
    from unittest.mock import patch

    import scanner as scanner_mod
    original_scan = scanner_mod.scan

    def tracking_scan(root, *args, **kwargs):
        scan_calls.append({"root": str(root), "restrict_dirs": kwargs.get("restrict_dirs")})
        return original_scan(root, *args, **kwargs)

    with patch.object(scanner_mod, "scan", tracking_scan):
        run_pipeline_job(job, runner, db_path, ws_id, params)

    # Scan should be called with the destination as root (for folder hierarchy)
    assert len(scan_calls) > 0, "Scan should have been called"
    assert scan_calls[-1]["root"] == str(dest), \
        f"Scan root should be the destination, got: {scan_calls[-1]['root']}"
    # restrict_dirs should be set to only the subfolders that received files
    restrict = scan_calls[-1]["restrict_dirs"]
    assert restrict is not None, "restrict_dirs should be set when files were copied"
    # The restrict dirs should NOT include the existing subfolder
    for d in restrict:
        assert str(existing_folder) != d, \
            f"restrict_dirs should not include pre-existing folder {existing_folder}"


def test_pipeline_progress_events_carry_stage_id(tmp_path, monkeypatch):
    """Each per-stage progress event should carry a stage_id so the
    Pipeline UI can route concurrent stages (scan + thumbnails) to their
    own progress bars instead of colliding."""
    import config as cfg
    from db import Database
    from PIL import Image

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    photo_dir = tmp_path / "photos"
    photo_dir.mkdir()
    for i in range(3):
        img = Image.new("RGB", (100, 100), "green")
        img.save(str(photo_dir / f"p_{i}.jpg"))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    params = PipelineParams(
        source=str(photo_dir),
        skip_classify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()
    run_pipeline_job(job, runner, db_path, ws_id, params)

    # Gather all progress events that include a stage_id
    stage_ids_seen = {
        e[2]["stage_id"]
        for e in runner.events
        if e[1] == "progress" and "stage_id" in e[2]
    }
    # Scan and thumbnails are the minimum we expect for a scan-in-place
    # run with classify/extract/regroup skipped.
    assert "scan" in stage_ids_seen, \
        f"Expected scan stage_id in events; saw: {stage_ids_seen}"
    assert "thumbnails" in stage_ids_seen, \
        f"Expected thumbnails stage_id in events; saw: {stage_ids_seen}"


@pytest.mark.skip(reason="retired pipeline import/archive destination path")
def test_pipeline_scan_not_running_during_ingest(tmp_path, monkeypatch):
    """In copy mode, stages.scan should stay 'pending' while ingest runs,
    so the Scan card doesn't pulse during the import sub-phase."""
    import config as cfg
    from db import Database
    from PIL import Image

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    src = tmp_path / "source"
    src.mkdir()
    for name in ["a.jpg", "b.jpg"]:
        img = Image.new("RGB", (100, 100), "red")
        img.save(str(src / name))

    dest = tmp_path / "dest"
    dest.mkdir()

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    params = PipelineParams(
        source=str(src),
        destination=str(dest),
        skip_classify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()
    run_pipeline_job(job, runner, db_path, ws_id, params)

    # Find events emitted while ingest was running
    ingest_running_events = [
        e[2] for e in runner.events
        if e[1] == "progress"
        and e[2].get("stages", {}).get("ingest", {}).get("status") == "running"
    ]
    assert len(ingest_running_events) > 0, \
        "Expected some events emitted while ingest was running"
    # During ingest, scan should still be pending (not running)
    for ev in ingest_running_events:
        scan_status = ev.get("stages", {}).get("scan", {}).get("status")
        assert scan_status == "pending", \
            f"scan should be 'pending' while ingest is running, got: {scan_status}"


def test_pipeline_collection_mode_marks_scan_skipped(tmp_path, monkeypatch):
    """In collection mode, stages.scan should be 'skipped' (not stuck
    on 'pending') so the Scan card renders as resolved."""
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    # Create an empty collection to reference
    coll_id = db.add_collection("test collection", json.dumps([]))

    params = PipelineParams(
        collection_id=coll_id,
        skip_classify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()
    run_pipeline_job(job, runner, db_path, ws_id, params)

    # Check that at least one event shows scan as 'skipped'
    scan_statuses = {
        e[2]["stages"]["scan"]["status"]
        for e in runner.events
        if e[1] == "progress" and "stages" in e[2] and "scan" in e[2]["stages"]
    }
    assert "skipped" in scan_statuses, \
        f"scan should be 'skipped' in collection mode, saw: {scan_statuses}"


def test_pipeline_collection_mode_generates_missing_thumbnails(tmp_path, monkeypatch):
    """In collection mode the thumbnail stage must still process the collection's
    photos. Previously it drained an empty queue (only fed by the scanner) and
    completed with '0 thumbnails' even when photos were missing thumbs."""
    import config as cfg
    from db import Database
    from PIL import Image

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    photo_dir = tmp_path / "photos"
    photo_dir.mkdir()
    for name in ["a.jpg", "b.jpg", "c.jpg"]:
        Image.new("RGB", (100, 100), "red").save(str(photo_dir / name))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    # First pipeline run: scan + build collection + generate thumbnails.
    runner = FakeRunner()
    job = _make_job()
    result = run_pipeline_job(
        job, runner, db_path, ws_id,
        PipelineParams(
            source=str(photo_dir),
            skip_classify=True, skip_extract_masks=True, skip_regroup=True,
        ),
    )
    coll_id = result["collection_id"]

    # Wipe the thumbnail cache to simulate thumbs that were lost or never built.
    thumb_dir = os.path.join(os.path.dirname(db_path), "thumbnails")
    for f in os.listdir(thumb_dir):
        os.remove(os.path.join(thumb_dir, f))

    # Second run: replay the pipeline against the existing collection
    # (skip_scan path). Thumbnails must be regenerated for all 3 photos.
    runner2 = FakeRunner()
    job2 = _make_job()
    result2 = run_pipeline_job(
        job2, runner2, db_path, ws_id,
        PipelineParams(
            collection_id=coll_id,
            skip_classify=True, skip_extract_masks=True, skip_regroup=True,
        ),
    )

    thumb_result = result2["stages"].get("thumbnails", {})
    assert thumb_result.get("generated", 0) == 3, (
        f"Expected 3 thumbnails regenerated in collection mode, "
        f"got {thumb_result}"
    )
    thumb_files = [f for f in os.listdir(thumb_dir) if not f.startswith(".")]
    assert len(thumb_files) == 3


def test_pipeline_collection_mode_edited_thumbnail_uses_working_copy(
    tmp_path, monkeypatch,
):
    """Collection thumbnail replay should fall back to usable working copies."""
    import config as cfg
    from db import Database
    from PIL import Image

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    photo_dir = tmp_path / "photos"
    photo_dir.mkdir()
    original = photo_dir / "a.jpg"
    Image.new("RGB", (100, 100), "red").save(str(original))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    runner = FakeRunner()
    job = _make_job()
    result = run_pipeline_job(
        job,
        runner,
        db_path,
        ws_id,
        PipelineParams(
            source=str(photo_dir),
            skip_classify=True,
            skip_extract_masks=True,
            skip_regroup=True,
        ),
    )
    coll_id = result["collection_id"]

    photo = db.get_collection_photos(coll_id, per_page=999999)[0]
    working_dir = tmp_path / "working"
    working_dir.mkdir()
    Image.new("RGB", (100, 100), "blue").save(str(working_dir / f"{photo['id']}.jpg"))
    db.conn.execute(
        "UPDATE photos SET working_copy_path=? WHERE id=?",
        (f"working/{photo['id']}.jpg", photo["id"]),
    )
    db.conn.commit()
    db.set_photo_edit_recipe(
        photo["id"],
        {"crop": {"x": 0, "y": 0, "w": 0.5, "h": 0.5}},
    )
    os.remove(original)

    thumb_dir = os.path.join(os.path.dirname(db_path), "thumbnails")
    for f in os.listdir(thumb_dir):
        os.remove(os.path.join(thumb_dir, f))

    runner2 = FakeRunner()
    job2 = _make_job()
    result2 = run_pipeline_job(
        job2,
        runner2,
        db_path,
        ws_id,
        PipelineParams(
            collection_id=coll_id,
            skip_classify=True,
            skip_extract_masks=True,
            skip_regroup=True,
        ),
    )

    thumb_result = result2["stages"].get("thumbnails", {})
    assert thumb_result.get("failed") == 0
    assert thumb_result.get("generated") == 1
    with Image.open(os.path.join(thumb_dir, f"{photo['id']}.jpg")) as thumb:
        r, g, b = thumb.resize((1, 1)).getpixel((0, 0))
    assert b > r and b > g


# ---------------------------------------------------------------------------
# Stage failure propagation (fixes the silent model-loader failure incident)
# ---------------------------------------------------------------------------


def _make_stage_failer(monkeypatch, stage_name, err_message):
    """Monkeypatch a specific pipeline stage to raise when invoked."""
    import pipeline_job

    real_run = pipeline_job.run_pipeline_job

    def wrapped(job, runner, db_path, ws_id, params):
        # Replace the stage function inside run_pipeline_job by patching the
        # classifier module that model_loader_stage imports lazily. We use a
        # targeted env toggle instead so the test stays hermetic.
        raise NotImplementedError(
            "Use direct classifier monkeypatch in the test instead."
        )

    return real_run


def _write_fake_model_files(model_dir, extra_files=()):
    """Materialize a fake model directory that passes _classify_model_state."""
    import models
    model_dir.mkdir(parents=True, exist_ok=True)
    (model_dir / "image_encoder.onnx").write_bytes(b"stub")
    with open(model_dir / "image_encoder.onnx.data", "wb") as f:
        f.truncate(models._MIN_BINARY_MODEL_BYTES + 1024)
    (model_dir / "text_encoder.onnx").write_bytes(b"stub")
    with open(model_dir / "text_encoder.onnx.data", "wb") as f:
        f.truncate(models._MIN_BINARY_MODEL_BYTES + 1024)
    (model_dir / "tokenizer.json").write_text("{}")
    (model_dir / "config.json").write_text("{}")
    for extra in extra_files:
        (model_dir / extra).write_text("{}")


def _setup_fake_downloaded_model(tmp_path, monkeypatch):
    """Put a validation-passing fake model on disk so model_loader_stage can
    get past the model-lookup / labels / taxonomy steps and into Classifier().
    Returns the model id that was set active.
    """
    import classify_job
    import model_verify
    import models
    import taxonomy
    monkeypatch.setattr(models, "CONFIG_PATH", str(tmp_path / "models.json"))
    monkeypatch.setattr(models, "DEFAULT_MODELS_DIR", str(tmp_path / "models"))
    _write_fake_model_files(tmp_path / "models" / "bioclip-vit-b-16")
    models.set_active_model("bioclip-vit-b-16")
    # Short-circuit taxonomy and label loading so the test stays focused on
    # model-loading behavior.
    monkeypatch.setattr(classify_job, "_load_taxonomy", lambda *a, **k: {})
    monkeypatch.setattr(
        classify_job, "_load_labels", lambda *a, **k: (["test-label"], False, [])
    )
    # model_loader_stage triggers a real iNat DWCA download whenever
    # params.download_taxonomy is True (the default) and no taxonomy file is
    # available — which is always the case in the test's isolated HOME. That
    # download is hundreds of MB and can exceed the per-test timeout on
    # slower CI runners, surfacing as an opaque "worker crashed" failure.
    # Stub it to a no-op; tests that exercise the real download path
    # re-monkeypatch this after calling the helper.
    monkeypatch.setattr(taxonomy, "download_taxonomy", lambda *a, **k: None)
    # Short-circuit hash verification — these tests use stub files that
    # would never match any real HF hash, and the verification path has
    # its own dedicated unit tests.
    monkeypatch.setattr(
        model_verify,
        "verify_if_needed",
        lambda model_id, model_dir, hf_subdir, optional_files=None: None,
    )
    return "bioclip-vit-b-16"


def _setup_two_fake_downloaded_models(tmp_path, monkeypatch):
    """Install two bioclip models side-by-side; both reported as downloaded."""
    import classify_job
    import models
    import taxonomy
    monkeypatch.setattr(models, "CONFIG_PATH", str(tmp_path / "models.json"))
    monkeypatch.setattr(models, "DEFAULT_MODELS_DIR", str(tmp_path / "models"))
    _write_fake_model_files(tmp_path / "models" / "bioclip-vit-b-16")
    _write_fake_model_files(
        tmp_path / "models" / "bioclip-2",
        extra_files=("tol_embeddings.npy", "tol_classes.json"),
    )
    models.set_active_model("bioclip-vit-b-16")
    monkeypatch.setattr(classify_job, "_load_taxonomy", lambda *a, **k: {})
    monkeypatch.setattr(
        classify_job, "_load_labels", lambda *a, **k: (["test-label"], False, [])
    )
    # See _setup_fake_downloaded_model for why this stub is required.
    monkeypatch.setattr(taxonomy, "download_taxonomy", lambda *a, **k: None)
    return ["bioclip-vit-b-16", "bioclip-2"]


def test_pipeline_raises_when_stage_fails(tmp_path, monkeypatch):
    """If any pipeline stage ends in 'failed', run_pipeline_job must raise.

    This is the fix for the silent model-loader crash incident: a stage
    caught its own exception and returned normally, so jobs.py recorded the
    run as 'completed' despite the failure. Now stage failures propagate.
    """
    import classifier as classifier_mod
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    col_id = db.add_collection("Test", "[]")

    _setup_fake_downloaded_model(tmp_path, monkeypatch)

    def boom(*args, **kwargs):
        raise RuntimeError("model_path must not be empty")

    monkeypatch.setattr(classifier_mod, "Classifier", boom)

    params = PipelineParams(
        collection_id=col_id,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()

    import pytest
    with pytest.raises(RuntimeError):
        run_pipeline_job(job, runner, db_path, ws_id, params)

    # The pipeline must stash its structured result on the job before raising,
    # so the pipeline UI's _onPipelineComplete handler can still read
    # result.result.errors and map the "[model_loader] Fatal: ..." prefix to
    # the right stage card. Without this, users on a failed run lose the
    # actionable "Failed: <stage error>" label on the card that broke.
    assert isinstance(job["result"], dict), \
        "Failed pipeline must leave a dict result on the job for UI rendering"
    assert "errors" in job["result"]
    assert any(
        "model_loader" in e for e in job["result"]["errors"]
    ), f"Expected a [model_loader]-prefixed error, got: {job['result']['errors']}"
    assert "duration" in job["result"]
    assert "stages" in job["result"]


def test_pipeline_translates_verify_failure_to_repair_message(tmp_path, monkeypatch):
    """When model_verify.verify_if_needed raises ModelCorruptError during the
    model_loader preflight, the pipeline should fail with the same Repair
    message used for other incomplete-model errors.

    This is the lazy-verification path: a silent bit-rot or unfinished
    download is surfaced right before the model is handed to ONNXRuntime,
    with an actionable recovery hint for the user.
    """
    import classifier as classifier_mod
    import config as cfg
    import model_verify
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    col_id = db.add_collection("Test", "[]")

    _setup_fake_downloaded_model(tmp_path, monkeypatch)

    def raise_corrupt(model_id, model_dir, hf_subdir, optional_files=None):
        raise model_verify.ModelCorruptError(
            model_id,
            model_verify.VerifyResult(
                ok=False, mismatches=["image_encoder.onnx.data"]
            ),
        )

    monkeypatch.setattr(model_verify, "verify_if_needed", raise_corrupt)

    # Classifier should never be reached because verify_if_needed fails first.
    def classifier_should_not_be_called(*args, **kwargs):
        raise AssertionError(
            "Classifier was constructed despite verify_if_needed raising"
        )

    monkeypatch.setattr(classifier_mod, "Classifier", classifier_should_not_be_called)

    params = PipelineParams(
        collection_id=col_id,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()

    import pytest
    with pytest.raises(RuntimeError):
        run_pipeline_job(job, runner, db_path, ws_id, params)

    model_loader_errors = [
        kwargs.get("error", "")
        for (_, step_id, kwargs) in runner.step_updates
        if step_id == "model_loader" and "error" in kwargs
    ]
    assert any("Repair" in e for e in model_loader_errors), \
        f"Expected a Repair hint in model_loader errors, saw: {model_loader_errors}"


def test_pipeline_preflight_accepts_unverified_model(tmp_path, monkeypatch):
    """Models in 'unverified' state (files present, SHA256 check skipped
    because HF was unreachable) must pass preflight. get_models() already
    reports downloaded=True for them, so rejecting them at pipeline start
    would turn a transient outage into a hard pipeline failure with no way
    to clear it short of deleting and redownloading the model.
    """
    import classifier as classifier_mod
    import config as cfg
    import model_verify
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    col_id = db.add_collection("Test", "[]")

    _setup_fake_downloaded_model(tmp_path, monkeypatch)

    # Write the verify-skipped sentinel so _classify_model_state returns
    # "unverified" rather than "ok".
    model_dir = tmp_path / "models" / "bioclip-vit-b-16"
    (model_dir / model_verify.VERIFY_SKIPPED_SENTINEL).write_text(
        "Transient HF outage (test fixture)"
    )

    # Stub Classifier so the preflight check is the only thing that could
    # fail on this path. If preflight rejects the unverified state, we'll
    # see a "Repair" error in model_loader step updates before the stub
    # is ever invoked.
    class _StubClassifier:
        def __init__(self, *args, **kwargs):
            pass

    monkeypatch.setattr(classifier_mod, "Classifier", _StubClassifier)

    params = PipelineParams(
        collection_id=col_id,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()
    run_pipeline_job(job, runner, db_path, ws_id, params)

    model_loader_errors = [
        kwargs.get("error", "")
        for (_, step_id, kwargs) in runner.step_updates
        if step_id == "model_loader" and "error" in kwargs
    ]
    assert not any("Repair" in e for e in model_loader_errors), (
        "Preflight must accept 'unverified' models, but saw Repair error: "
        f"{model_loader_errors}"
    )


def test_pipeline_translates_incomplete_model_error(tmp_path, monkeypatch):
    """Model loader failures from missing external-data get a friendly message.

    Users should see "open Settings → Models and click Repair" rather than
    the raw ONNXRuntime stack.
    """
    import classifier as classifier_mod
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    col_id = db.add_collection("Test", "[]")

    _setup_fake_downloaded_model(tmp_path, monkeypatch)

    def boom(*args, **kwargs):
        raise RuntimeError(
            "[ONNXRuntimeError] model_path must not be empty. Ensure that "
            "a path is provided when the model is created or loaded."
        )

    monkeypatch.setattr(classifier_mod, "Classifier", boom)

    params = PipelineParams(
        collection_id=col_id,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()

    import pytest
    with pytest.raises(RuntimeError) as exc:
        run_pipeline_job(job, runner, db_path, ws_id, params)

    # Either the model_loader stage raised with the friendly message directly,
    # or the pipeline raised its own failure wrapping the original; in either
    # case the errors list (accessible via the model_loader step update) should
    # contain the actionable "Repair" hint.
    model_loader_errors = [
        kwargs.get("error", "")
        for (_, step_id, kwargs) in runner.step_updates
        if step_id == "model_loader" and "error" in kwargs
    ]
    assert any("Repair" in e for e in model_loader_errors), \
        f"Expected a Repair hint in model_loader errors, saw: {model_loader_errors}"


def test_pipeline_cancellation_takes_precedence_over_failure(tmp_path, monkeypatch):
    """A cancelled pipeline must not be recorded as 'failed' even if a stage
    crashed on the way down. Cancellation intent beats failure.
    """
    import classifier as classifier_mod
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    col_id = db.add_collection("Test", "[]")

    _setup_fake_downloaded_model(tmp_path, monkeypatch)

    def boom(*args, **kwargs):
        raise RuntimeError("model_path must not be empty")

    monkeypatch.setattr(classifier_mod, "Classifier", boom)

    params = PipelineParams(
        collection_id=col_id,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()
    # Mark cancelled BEFORE the pipeline starts so the post-stage check sees it.
    runner.cancelled_ids.add(job["id"])

    # Should NOT raise — cancellation wins over stage failure.
    result = run_pipeline_job(job, runner, db_path, ws_id, params)
    assert isinstance(result, dict)


def test_pipeline_loops_over_multiple_models(tmp_path, monkeypatch):
    """When model_ids contains multiple models, each one must be loaded and
    run through classify. This is the fix for the UI-dropped-multi-select bug:
    the pipeline page collects multiple checked models but only the first one
    was forwarded to the backend. The backend now honors model_ids."""
    import classifier as classifier_mod
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    col_id = db.add_collection("Test", "[]")

    model_ids = _setup_two_fake_downloaded_models(tmp_path, monkeypatch)

    construction_calls = []

    class FakeClassifier:
        def __init__(self, *args, **kwargs):
            construction_calls.append(kwargs)
            kwargs["embedding_progress_callback"](0, 10)
            kwargs["embedding_progress_callback"](7, 10)
            kwargs["embedding_progress_callback"](10, 10)

        def encode_image(self, *args, **kwargs):
            import numpy as np
            return np.zeros(512, dtype=np.float32)

    monkeypatch.setattr(classifier_mod, "Classifier", FakeClassifier)

    params = PipelineParams(
        collection_id=col_id,
        model_ids=model_ids,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()

    run_pipeline_job(job, runner, db_path, ws_id, params)

    assert len(construction_calls) == len(model_ids), (
        f"Expected Classifier() to be constructed {len(model_ids)} times "
        f"(one per model_id), got {len(construction_calls)}"
    )
    for step in ("model_loader", f"classify:{model_ids[1]}"):
        updates = [kw for _, sid, kw in runner.step_updates if sid == step]
        assert any(kw.get("progress") == {"current": 7, "total": 10, "unit": "labels"} for kw in updates)
        assert any("7 / 10 labels ready" in kw.get("current_file", "") for kw in updates)
    # model_loader must record completion with a summary naming each model.
    model_loader_summaries = [
        kwargs.get("summary", "")
        for (_, step_id, kwargs) in runner.step_updates
        if step_id == "model_loader" and kwargs.get("status") == "completed"
    ]
    joined = " ".join(model_loader_summaries)
    assert "BioCLIP" in joined and "BioCLIP-2" in joined, (
        f"model_loader summary should mention both models, saw: {model_loader_summaries}"
    )


def test_pipeline_classify_step_names_the_label_set(tmp_path, monkeypatch):
    """The classify row must say which species list the model ran against.

    "Classify with BioCLIP-2.5" alone doesn't tell the user whether these
    photos were matched against their regional lists or the whole Tree of
    Life, and that decides which species the run could possibly return.
    """
    import classifier as classifier_mod
    import classify_job
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    col_id = db.add_collection("Test", "[]")

    model_id = _setup_fake_downloaded_model(tmp_path, monkeypatch)
    monkeypatch.setattr(
        classify_job, "_load_labels",
        lambda *a, **k: (
            ["Northern Cardinal", "Blue Jay", "Steller's Jay"],
            False,
            [{"labels_file": "/l/birds.txt", "name": "California, US Birds"}],
        ),
    )
    monkeypatch.setattr(
        classify_job, "get_active_labels",
        lambda: [{"labels_file": "/l/birds.txt", "name": "California, US Birds"}],
    )

    class FakeClassifier:
        def __init__(self, *args, **kwargs):
            pass

        def encode_image(self, *args, **kwargs):
            import numpy as np
            return np.zeros(512, dtype=np.float32)

    monkeypatch.setattr(classifier_mod, "Classifier", FakeClassifier)

    params = PipelineParams(
        collection_id=col_id,
        model_ids=[model_id],
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    run_pipeline_job(_make_job(), runner, db_path, ws_id, params)

    published = [
        kwargs["label_source"]
        for (_, step_id, kwargs) in runner.step_updates
        if step_id == f"classify:{model_id}" and "label_source" in kwargs
    ]
    assert published == ["3 species from California, US Birds"], (
        f"classify step should name the active label set; got {published!r}"
    )


def test_pipeline_pause_during_classifier_load_parks_and_resumes(
    tmp_path, monkeypatch,
):
    """Pause arriving while the model loader is computing label embeddings
    must take effect promptly: the factory steps down (checkpointing its
    progress and releasing the shared load lock), the model_loader
    participant parks at the pipeline pause gate, and construction runs
    again after Resume. The run then completes normally.
    """
    import classifier as classifier_mod
    import config as cfg
    from classifier import ClassifierLoadPaused
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    col_id = db.add_collection("Test", "[]")

    _setup_fake_downloaded_model(tmp_path, monkeypatch)

    events = []

    class PausingRunner(FakeRunner):
        def __init__(self):
            super().__init__()
            self.paused = False

        def cancellation_requested(self, job_id):
            return False

        def pause_requested(self, job_id):
            return self.paused

        def mark_paused(self, job_id):
            return True

        def wait_if_paused(self, job_id, *, publish_paused=False):
            if self.paused:
                events.append("park")
                self.paused = False
            return False

    class FakeClassifier:
        def __init__(self, *args, pause_check=None, **kwargs):
            events.append("construct")
            if events.count("construct") == 1:
                # The user clicks Pause while embeddings are computing.
                runner.paused = True
            if pause_check is not None and pause_check():
                # Mirrors the real embedding loop stepping down mid-way.
                raise ClassifierLoadPaused("classifier load paused")

        def encode_image(self, *args, **kwargs):
            import numpy as np
            return np.zeros(512, dtype=np.float32)

    monkeypatch.setattr(classifier_mod, "Classifier", FakeClassifier)

    params = PipelineParams(
        collection_id=col_id,
        skip_extract_masks=True,
        skip_regroup=True,
    )
    runner = PausingRunner()
    job = _make_job()

    run_pipeline_job(job, runner, db_path, ws_id, params)

    assert events == ["construct", "park", "construct"], (
        "the factory must step down for the pause, the loader must park "
        "outside the load lock, and construction must run again after "
        f"Resume; got {events}"
    )
    assert runner.paused is False
    assert not job["errors"], job["errors"]
    model_loader_status = [
        kwargs.get("status")
        for (_, step_id, kwargs) in runner.step_updates
        if step_id == "model_loader"
    ]
    assert "completed" in model_loader_status, model_loader_status


def test_pipeline_model_ids_back_compat_with_model_id(tmp_path, monkeypatch):
    """A job with only the legacy `model_id` field (no `model_ids`) must still
    load exactly that one model — preserving back-compat with older callers."""
    import classifier as classifier_mod
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    col_id = db.add_collection("Test", "[]")

    _setup_fake_downloaded_model(tmp_path, monkeypatch)

    construction_calls = []

    class FakeClassifier:
        def __init__(self, *args, **kwargs):
            construction_calls.append(kwargs)

        def encode_image(self, *args, **kwargs):
            import numpy as np
            return np.zeros(512, dtype=np.float32)

    monkeypatch.setattr(classifier_mod, "Classifier", FakeClassifier)

    params = PipelineParams(
        collection_id=col_id,
        model_id="bioclip-vit-b-16",
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()

    run_pipeline_job(job, runner, db_path, ws_id, params)

    assert len(construction_calls) == 1, (
        f"Legacy model_id should load exactly one classifier, "
        f"got {len(construction_calls)}"
    )


def test_pipeline_classifies_full_image_when_detector_finds_nothing(
    tmp_path, monkeypatch
):
    """Pipeline classify should not strand photos where MegaDetector ran and
    found no subject. It should classify the full image once, persist the
    synthetic anchor, and use classifier_runs on the next pass.
    """
    import classifier as classifier_mod
    import classify_job
    import config as cfg
    import detector
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)
    photo_id = db.add_photo(
        folder_id, "empty.jpg", ".jpg", 100, 1_000_000.0,
    )
    _drop_jpeg(folder_path, "empty.jpg")
    col_id = db.add_collection(
        "No detections",
        json.dumps([{"field": "photo_ids", "value": [photo_id]}]),
    )

    _setup_fake_downloaded_model(tmp_path, monkeypatch)
    monkeypatch.setattr(detector, "ensure_megadetector_weights", lambda **_k: None)
    monkeypatch.setattr(classify_job, "detect_animals", lambda _path: [])
    monkeypatch.setattr(classify_job, "get_primary_detection", lambda _dets: None)

    classify_calls = []

    class FakeClassifier:
        def __init__(self, *args, **kwargs):
            pass

        def classify_batch_with_embedding(self, images, threshold=0):
            import numpy as np

            classify_calls.append(len(images))
            return [
                (
                    [{"species": "Full-image Robin", "score": 0.91}],
                    np.zeros(512, dtype=np.float32),
                )
                for _ in images
            ]

    monkeypatch.setattr(classifier_mod, "Classifier", FakeClassifier)

    params = PipelineParams(
        collection_id=col_id,
        model_id="bioclip-vit-b-16",
        skip_extract_masks=True,
        skip_regroup=True,
    )

    first = run_pipeline_job(
        _make_job(), FakeRunner(), db_path, ws_id, params,
    )
    assert first["stages"]["detect"] == {
        "total": 1,
        "detected": 0,
        "processed": 1,
    }
    assert first["stages"]["classify"]["full_image_fallbacks"] == 1
    assert classify_calls == [1]

    check = Database(db_path)
    check.set_active_workspace(ws_id)
    full = check.get_detections(
        photo_id, detector_model="full-image", min_conf=0,
    )
    assert len(full) == 1
    pred = check.get_predictions_for_detection(
        full[0]["id"],
        classifier_model="BioCLIP",
        min_classifier_conf=0,
    )
    assert len(pred) == 1
    assert pred[0]["species"] == "Full-image Robin"
    assert (
        "BioCLIP", pred[0]["labels_fingerprint"],
    ) in check.get_classifier_run_keys(full[0]["id"])

    # Rerun: detector_run + full-image classifier_run should make this a
    # metadata/cache pass, not another model inference.
    second = run_pipeline_job(
        _make_job(), FakeRunner(), db_path, ws_id, params,
    )
    assert second["stages"]["classify"]["full_image_fallbacks"] == 1
    assert classify_calls == [1]
    full_after = check.get_detections(
        photo_id, detector_model="full-image", min_conf=0,
    )
    assert [d["id"] for d in full_after] == [full[0]["id"]]

    # Forced reclassify should infer again, but the stale-detection purge must
    # treat the synthetic full-image anchor as fresh. Otherwise the purge
    # cascades through predictions.detection_id and deletes the new result.
    reclassify_params = PipelineParams(
        collection_id=col_id,
        model_id="bioclip-vit-b-16",
        reclassify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )
    third = run_pipeline_job(
        _make_job(), FakeRunner(), db_path, ws_id, reclassify_params,
    )
    assert third["stages"]["classify"]["full_image_fallbacks"] == 1
    assert classify_calls == [1, 1]
    verify = Database(db_path)
    verify.set_active_workspace(ws_id)
    full_reclassified = verify.get_detections(
        photo_id, detector_model="full-image", min_conf=0,
    )
    assert [d["id"] for d in full_reclassified] == [full[0]["id"]]
    pred_after_reclassify = verify.get_predictions_for_detection(
        full[0]["id"],
        classifier_model="BioCLIP",
        min_classifier_conf=0,
    )
    assert len(pred_after_reclassify) == 1
    assert pred_after_reclassify[0]["species"] == "Full-image Robin"


def test_pipeline_classifies_full_image_when_only_raw_noise_boxes_exist(
    tmp_path, monkeypatch,
):
    """A raw detector box below every eligibility floor must not suppress
    full-image classification.

    MegaDetector persists output down to its raw confidence floor so future
    threshold changes can reuse it. Previously, the mere presence of one of
    those rows made classify skip the photo: it was too weak to crop, but also
    blocked the full-image fallback. That stranded clear subjects whenever the
    detector emitted only noise-level boxes.
    """
    import classifier as classifier_mod
    import classify_job
    import config as cfg
    from db import Database
    from PIL import Image

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)
    photo_id = db.add_photo(
        folder_id, "bird-in-branches.jpg", ".jpg", 100, 1_000_000.0,
    )
    _drop_jpeg(folder_path, "bird-in-branches.jpg")
    collection_id = db.add_collection(
        "Raw noise box",
        json.dumps([{"field": "photo_ids", "value": [photo_id]}]),
    )

    model_id = _setup_fake_downloaded_model(tmp_path, monkeypatch)
    raw_detection = {
        "box": {"x": 0.42, "y": 0.48, "w": 0.15, "h": 0.16},
        "confidence": 0.02,
        "category": "animal",
    }
    raw_detection_id = db.write_detection_batch(
        photo_id, "megadetector-v6", [raw_detection],
    )[0]

    def fake_detect_batch(batch, folders, runner, job, reclassify, db_,
                          det_conf_threshold=None, already_detected_ids=None,
                          cached_detections=None):
        det = {
            "id": raw_detection_id,
            "box_x": raw_detection["box"]["x"],
            "box_y": raw_detection["box"]["y"],
            "box_w": raw_detection["box"]["w"],
            "box_h": raw_detection["box"]["h"],
            "confidence": raw_detection["confidence"],
            "category": "animal",
            "detector_model": "megadetector-v6",
        }
        return {photo_id: [det]}, 1, {photo_id}

    monkeypatch.setattr(classify_job, "_detect_batch", fake_detect_batch)

    prepared_detections = []

    def fake_prepare_image(photo, folders, detection, vireo_dir=None):
        prepared_detections.append(detection)
        return Image.new("RGB", (32, 32), "white"), folder_path, str(
            os.path.join(folder_path, photo["filename"])
        )

    monkeypatch.setattr(classify_job, "_prepare_image", fake_prepare_image)

    class FakeClassifier:
        def __init__(self, *args, **kwargs):
            pass

        def classify_batch_with_embedding(self, images, threshold=0):
            import numpy as np

            return [(
                [{"species": "Downy Woodpecker", "score": 0.94}],
                np.zeros(512, dtype=np.float32),
            ) for _ in images]

    monkeypatch.setattr(classifier_mod, "Classifier", FakeClassifier)

    result = run_pipeline_job(
        _make_job(), FakeRunner(), db_path, ws_id,
        PipelineParams(
            collection_id=collection_id,
            model_id=model_id,
            skip_extract_masks=True,
            skip_regroup=True,
        ),
    )

    assert result["stages"]["classify"]["full_image_fallbacks"] == 1
    assert prepared_detections == [None]

    check = Database(db_path)
    check.set_active_workspace(ws_id)
    rows = check.conn.execute(
        """SELECT d.detector_model, pr.species
             FROM detections d
             LEFT JOIN predictions pr ON pr.detection_id = d.id
            WHERE d.photo_id = ?
            ORDER BY d.detector_model""",
        (photo_id,),
    ).fetchall()
    assert [dict(row) for row in rows] == [
        {"detector_model": "full-image", "species": "Downy Woodpecker"},
        {"detector_model": "megadetector-v6", "species": None},
    ]


def test_pipeline_skips_full_image_when_only_confident_non_animal_box(
    tmp_path, monkeypatch,
):
    """A confident person/vehicle box must still suppress the full-image
    fallback.

    The animal-filter above ``detections_to_classify`` strips non-animal
    rows before the emptiness check, so a photo whose only MegaDetector
    output is a high-confidence person or vehicle box would otherwise slip
    into the fallback and get a spurious wildlife prediction. The guard
    only whitelists photos whose non-animal rows are all sub-threshold
    (i.e. actual noise).
    """
    import classifier as classifier_mod
    import classify_job
    import config as cfg
    from db import Database
    from PIL import Image

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)
    photo_id = db.add_photo(
        folder_id, "hiker.jpg", ".jpg", 100, 1_000_000.0,
    )
    _drop_jpeg(folder_path, "hiker.jpg")
    collection_id = db.add_collection(
        "Confident person",
        json.dumps([{"field": "photo_ids", "value": [photo_id]}]),
    )

    model_id = _setup_fake_downloaded_model(tmp_path, monkeypatch)
    person_det = {
        "box": {"x": 0.30, "y": 0.20, "w": 0.20, "h": 0.60},
        "confidence": 0.95,
        "category": "person",
    }
    person_det_id = db.write_detection_batch(
        photo_id, "megadetector-v6", [person_det],
    )[0]

    def fake_detect_batch(batch, folders, runner, job, reclassify, db_,
                          det_conf_threshold=None, already_detected_ids=None,
                          cached_detections=None):
        det = {
            "id": person_det_id,
            "box_x": person_det["box"]["x"],
            "box_y": person_det["box"]["y"],
            "box_w": person_det["box"]["w"],
            "box_h": person_det["box"]["h"],
            "confidence": person_det["confidence"],
            "category": "person",
            "detector_model": "megadetector-v6",
        }
        return {photo_id: [det]}, 1, {photo_id}

    monkeypatch.setattr(classify_job, "_detect_batch", fake_detect_batch)

    prepared_detections = []

    def fake_prepare_image(photo, folders, detection, vireo_dir=None):
        prepared_detections.append(detection)
        return Image.new("RGB", (32, 32), "white"), folder_path, str(
            os.path.join(folder_path, photo["filename"])
        )

    monkeypatch.setattr(classify_job, "_prepare_image", fake_prepare_image)

    class FakeClassifier:
        def __init__(self, *args, **kwargs):
            pass

        def classify_batch_with_embedding(self, images, threshold=0):
            import numpy as np

            return [(
                [{"species": "Bald Eagle", "score": 0.99}],
                np.zeros(512, dtype=np.float32),
            ) for _ in images]

    monkeypatch.setattr(classifier_mod, "Classifier", FakeClassifier)

    result = run_pipeline_job(
        _make_job(), FakeRunner(), db_path, ws_id,
        PipelineParams(
            collection_id=collection_id,
            model_id=model_id,
            skip_extract_masks=True,
            skip_regroup=True,
        ),
    )

    assert result["stages"]["classify"]["full_image_fallbacks"] == 0
    assert prepared_detections == []

    check = Database(db_path)
    check.set_active_workspace(ws_id)
    rows = check.conn.execute(
        """SELECT d.detector_model, d.category, pr.species
             FROM detections d
             LEFT JOIN predictions pr ON pr.detection_id = d.id
            WHERE d.photo_id = ?
            ORDER BY d.detector_model""",
        (photo_id,),
    ).fetchall()
    assert [dict(row) for row in rows] == [
        {
            "detector_model": "megadetector-v6",
            "category": "person",
            "species": None,
        },
    ]


def test_pipeline_precreates_full_image_anchor_for_noise_only_photo(
    tmp_path, monkeypatch,
):
    """The detect stage must pre-create a full-image anchor for a photo whose
    only detections are sub-threshold noise.

    Without this pre-creation, the post-detect ``materialize_local_store``
    reapply has nothing for a cached full-image classifier artifact to
    attach to (the anchor is only created lazily during ``classify_stage``
    later). The classify stage then re-runs the classifier for an answer
    that is already in the local store. The runtime fallback fires under
    the same predicate the pre-materialization anchor selection now
    mirrors — no usable animal box at the strict threshold and no
    confident non-animal box — so the pre-created anchor is what the
    fallback would use.
    """
    import classifier as classifier_mod
    import classify_job
    import computation_cache as cc
    import config as cfg
    from db import Database
    from PIL import Image

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)
    photo_id = db.add_photo(
        folder_id, "faint-warbler.jpg", ".jpg", 100, 1_000_000.0,
    )
    _drop_jpeg(folder_path, "faint-warbler.jpg")
    collection_id = db.add_collection(
        "Noise only",
        json.dumps([{"field": "photo_ids", "value": [photo_id]}]),
    )

    model_id = _setup_fake_downloaded_model(tmp_path, monkeypatch)
    raw_detection = {
        "box": {"x": 0.10, "y": 0.10, "w": 0.05, "h": 0.05},
        "confidence": 0.03,
        "category": "animal",
    }
    raw_detection_id = db.write_detection_batch(
        photo_id, "megadetector-v6", [raw_detection],
    )[0]

    def fake_detect_batch(batch, folders, runner, job, reclassify, db_,
                          det_conf_threshold=None, already_detected_ids=None,
                          cached_detections=None):
        det = {
            "id": raw_detection_id,
            "box_x": raw_detection["box"]["x"],
            "box_y": raw_detection["box"]["y"],
            "box_w": raw_detection["box"]["w"],
            "box_h": raw_detection["box"]["h"],
            "confidence": raw_detection["confidence"],
            "category": "animal",
            "detector_model": "megadetector-v6",
        }
        return {photo_id: [det]}, 1, {photo_id}

    monkeypatch.setattr(classify_job, "_detect_batch", fake_detect_batch)

    # Capture the DB state at the moment the post-detect reapply runs.
    # A pre-created full-image detection row here proves the anchor is
    # available for cached classifier artifacts to attach to during
    # materialize — i.e., before classify_stage would otherwise have to
    # rerun the classifier.
    anchors_at_reapply = []
    original_materialize = cc.materialize_local_store

    def spy_materialize(db_obj, *args, **kwargs):
        rows = db_obj.conn.execute(
            "SELECT detector_model, runtime_fingerprint FROM detections "
            "WHERE photo_id = ? ORDER BY detector_model",
            (photo_id,),
        ).fetchall()
        anchors_at_reapply.append([dict(r) for r in rows])
        return original_materialize(db_obj, *args, **kwargs)

    monkeypatch.setattr(cc, "materialize_local_store", spy_materialize)

    def fake_prepare_image(photo, folders, detection, vireo_dir=None):
        return Image.new("RGB", (32, 32), "white"), folder_path, str(
            os.path.join(folder_path, photo["filename"])
        )

    monkeypatch.setattr(classify_job, "_prepare_image", fake_prepare_image)

    class FakeClassifier:
        def __init__(self, *args, **kwargs):
            pass

        def classify_batch_with_embedding(self, images, threshold=0):
            import numpy as np

            return [(
                [{"species": "Yellow Warbler", "score": 0.87}],
                np.zeros(512, dtype=np.float32),
            ) for _ in images]

    monkeypatch.setattr(classifier_mod, "Classifier", FakeClassifier)

    result = run_pipeline_job(
        _make_job(), FakeRunner(), db_path, ws_id,
        PipelineParams(
            collection_id=collection_id,
            model_id=model_id,
            skip_extract_masks=True,
            skip_regroup=True,
        ),
    )

    # The pipeline runs materialize twice: once before detect (no anchor
    # expected yet) and once after detect (anchor must exist so cached
    # full-image classifier runs can attach). The post-detect call is the
    # second one — assert it saw the full-image anchor for this noise-only
    # photo.
    assert len(anchors_at_reapply) >= 2, (
        f"expected pre- and post-detect materialize calls, "
        f"saw {len(anchors_at_reapply)}"
    )
    post_detect_state = anchors_at_reapply[-1]
    detector_models = {row["detector_model"] for row in post_detect_state}
    assert "full-image" in detector_models, (
        "detect-stage reapply must see a pre-created full-image anchor "
        "for a noise-only photo so cached full-image classifiers can "
        f"attach; saw {post_detect_state}"
    )

    # And the run still classifies via full-image fallback — the guard
    # cannot regress the noise-only rescue path itself.
    assert result["stages"]["classify"]["full_image_fallbacks"] == 1


def test_pipeline_redownloads_taxonomy_when_existing_file_is_corrupt(
    tmp_path, monkeypatch
):
    """A 0-byte stub from an interrupted download "exists" on disk but is
    not a usable taxonomy. /api/pipeline/page-init reports unavailable in
    that case (so the "Download taxonomy if missing" checkbox stays
    visible), and the runner must honor the same gate — otherwise checking
    the box from the UI silently no-ops on the exact corruption case the
    page-init change targets, leaving users with no in-app recovery path.
    """
    import classifier as classifier_mod
    import config as cfg
    import taxonomy as taxonomy_mod
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    fake_path = tmp_path / "taxonomy.json"
    fake_path.write_bytes(b"")  # 0-byte stub from an interrupted download
    monkeypatch.setattr(taxonomy_mod, "TAXONOMY_JSON_PATH", str(fake_path))
    monkeypatch.setattr(
        taxonomy_mod, "find_taxonomy_json", lambda: str(fake_path)
    )
    monkeypatch.setattr(taxonomy_mod, "load_local_taxonomy", lambda: None)

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    col_id = db.add_collection("Test", "[]")

    # Helper installs a no-op download_taxonomy stub; override it AFTER the
    # helper so this test's recording stub wins. Order matters: the helper's
    # monkeypatch.setattr would clobber an earlier override.
    _setup_fake_downloaded_model(tmp_path, monkeypatch)

    download_calls = []

    def fake_download(output_path, progress_callback=None):
        download_calls.append(output_path)

    monkeypatch.setattr(taxonomy_mod, "download_taxonomy", fake_download)

    class FakeClassifier:
        def __init__(self, *args, **kwargs):
            pass

        def encode_image(self, *args, **kwargs):
            import numpy as np
            return np.zeros(512, dtype=np.float32)

    monkeypatch.setattr(classifier_mod, "Classifier", FakeClassifier)

    params = PipelineParams(
        collection_id=col_id,
        download_taxonomy=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )
    runner = FakeRunner()
    job = _make_job()

    run_pipeline_job(job, runner, db_path, ws_id, params)

    assert download_calls, (
        "Pipeline must trigger taxonomy download when the existing file is "
        "corrupt/undersized; gate must match get_taxonomy_info() availability"
    )


def test_pipeline_skips_taxonomy_download_when_file_is_usable(
    tmp_path, monkeypatch
):
    """Converse of the corruption case: a valid taxonomy file (>= 1MB) on
    disk must NOT trigger a re-download even when download_taxonomy=True is
    set, otherwise the checkbox would silently re-download on every run.
    """
    import classifier as classifier_mod
    import config as cfg
    import taxonomy as taxonomy_mod
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    fake_path = tmp_path / "taxonomy.json"
    with open(fake_path, "wb") as f:
        f.truncate(2_000_000)
    monkeypatch.setattr(taxonomy_mod, "TAXONOMY_JSON_PATH", str(fake_path))
    monkeypatch.setattr(
        taxonomy_mod, "find_taxonomy_json", lambda: str(fake_path)
    )
    monkeypatch.setattr(taxonomy_mod, "load_local_taxonomy", lambda: None)

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    col_id = db.add_collection("Test", "[]")

    # Override the helper's no-op download stub AFTER calling the helper, so
    # this test's recording stub wins. See sibling test above.
    _setup_fake_downloaded_model(tmp_path, monkeypatch)

    download_calls = []
    monkeypatch.setattr(
        taxonomy_mod,
        "download_taxonomy",
        lambda output_path, progress_callback=None: download_calls.append(
            output_path
        ),
    )

    class FakeClassifier:
        def __init__(self, *args, **kwargs):
            pass

        def encode_image(self, *args, **kwargs):
            import numpy as np
            return np.zeros(512, dtype=np.float32)

    monkeypatch.setattr(classifier_mod, "Classifier", FakeClassifier)

    params = PipelineParams(
        collection_id=col_id,
        download_taxonomy=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )
    runner = FakeRunner()
    job = _make_job()

    run_pipeline_job(job, runner, db_path, ws_id, params)

    assert not download_calls, (
        "Valid (>= 1MB) taxonomy must not be redundantly redownloaded"
    )


def test_pipeline_reclassify_multimodel_ignores_stale_detection_ids(
    tmp_path, monkeypatch
):
    """On reclassify with multiple models, already_detected must be cleared
    before model 1's batch loop so model 2+ only reuse detections produced in
    this run, not stale rows from a prior pipeline pass.

    Regression: before the fix, already_detected was pre-seeded from
    get_existing_detection_photo_ids() before the model loop.  When model 1
    ran with reclassify=True but did NOT produce a detection for a photo that
    already had a prior-run detection row, model 2 (reclassify=False) still
    found that photo in already_detected and called db.get_detections(),
    binding its predictions to outdated detection_ids.
    """
    import json

    import classifier as classifier_mod
    import classify_job
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    # Create a folder + photo and insert a prior-run detection row so that
    # get_existing_detection_photo_ids() returns this photo's id.
    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)
    photo_id = db.add_photo(folder_id, "test.jpg", ".jpg", 12345, 1_000_000.0)
    _drop_jpeg(folder_path, "test.jpg")
    db.save_detections(
        photo_id,
        [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
          "confidence": 0.9, "category": "animal"}],
        detector_model="MegaDetector",
    )
    # Static collection containing exactly that one photo.
    col_id = db.add_collection(
        "Test",
        json.dumps([{"field": "photo_ids", "value": [photo_id]}]),
    )

    model_ids = _setup_two_fake_downloaded_models(tmp_path, monkeypatch)

    # Capture the already_detected_ids and cached_detections passed to each
    # _detect_batch call so we can verify model 2 gets fresh cache entries
    # from model 1 rather than stale DB rows.
    detect_calls = []

    def fake_detect_batch(batch, folders, runner, job, reclassify, db_,
                          det_conf_threshold=None, already_detected_ids=None,
                          cached_detections=None):
        detect_calls.append({
            "already_detected_ids": frozenset(already_detected_ids or set()),
            "cached_detections": dict(cached_detections) if cached_detections else {},
            "reclassify": reclassify,
        })
        # Model 1 "detects" nothing in this run — empty det_map, but every
        # photo in the batch completed its iteration.
        return {}, 0, {p["id"] for p in batch}

    monkeypatch.setattr(classify_job, "_detect_batch", fake_detect_batch)

    class FakeClassifier:
        def __init__(self, *args, **kwargs):
            pass

        def encode_image(self, *args, **kwargs):
            import numpy as np
            return np.zeros(512, dtype=np.float32)

        def classify_with_embedding(self, img, threshold=0):
            import numpy as np
            return [{"species": "Robin", "score": 0.9}], np.zeros(
                512, dtype=np.float32,
            )

        def classify_batch_with_embedding(self, images, threshold=0):
            import numpy as np
            zero = np.zeros(512, dtype=np.float32)
            return [(
                [{"species": "Robin", "score": 0.9}],
                zero,
            ) for _ in images]

    monkeypatch.setattr(classifier_mod, "Classifier", FakeClassifier)

    params = PipelineParams(
        collection_id=col_id,
        model_ids=model_ids,
        reclassify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()

    run_pipeline_job(job, runner, db_path, ws_id, params)

    # With the detect pre-pass, _detect_batch runs ONCE across the whole
    # collection regardless of how many models are classifying downstream.
    # Every subsequent classify stage reads from the shared cache rather
    # than invoking the detector again.
    assert len(detect_calls) == 1, (
        f"Expected exactly 1 _detect_batch call (shared pre-pass), got "
        f"{len(detect_calls)}"
    )

    # Reclassify: the shared pre-pass must start with an empty
    # already_detected so every photo's detection is recomputed — no stale
    # prior-run IDs should leak in.
    assert photo_id not in detect_calls[0]["already_detected_ids"], (
        f"Prior-run photo_id {photo_id} leaked into already_detected_ids on "
        "the reclassify pre-pass. already_detected must start empty."
    )
    assert detect_calls[0]["reclassify"] is True, (
        "Detect pre-pass should be called with reclassify=True on a "
        "reclassify run so MegaDetector re-runs instead of short-circuiting "
        "against existing DB rows."
    )


def test_detect_batch_prefers_cached_detections_over_db(monkeypatch):
    """_detect_batch must use cached_detections when provided instead of
    calling db.get_detections(), so model 2+ in a multi-model reclassify run
    bind predictions to the detection rows model 1 produced in *this* run
    rather than stale rows from a prior pipeline pass.

    Regression test for the second Codex P1 comment on #506 ('Restrict model
    2+ reuse to detections created in this run').
    """
    import os
    import sys
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
    import classify_job

    photo = {"id": 42, "folder_id": 1, "filename": "bird.jpg"}

    cached_det = [{"id": 99, "box_x": 0.1, "box_y": 0.1,
                   "box_w": 0.5, "box_h": 0.5,
                   "confidence": 0.95, "category": "animal"}]

    db_called = {"n": 0}

    class FakeDB:
        def get_detections(self, photo_id):
            db_called["n"] += 1
            return []

    det_map, count, processed = classify_job._detect_batch(
        photos=[photo],
        folders={1: "/fake"},
        runner=None,
        job=None,
        reclassify=False,
        db=FakeDB(),
        already_detected_ids={42},
        cached_detections={42: cached_det},
    )

    assert db_called["n"] == 0, (
        "db.get_detections() must NOT be called when cached_detections "
        "already has an entry for the photo."
    )
    assert det_map.get(42) == cached_det, (
        "detection_map must contain the cached detection list, not a DB result."
    )
    assert count == 1
    assert 42 in processed


def test_pipeline_classify_passes_each_qualifying_detection_to_prepare_image(
    tmp_path, monkeypatch
):
    """classify_stage must pass every qualifying detection dict (with
    box_x/y/w/h keys) to _prepare_image, not the raw {photo_id: [dets]}
    det_map. Raw detections below the workspace threshold stay excluded.

    Regression: classify_stage called
        _prepare_image(photo, folders, det_map)
    where det_map is {photo_id: [detection, ...]}.  Because det_map is truthy
    once any photo in a batch has a detection, _prepare_image entered its
    crop branch and evaluated det_map["box_w"] -> KeyError: 'box_w', aborting
    classify the moment the first detection came back.  The fix is to look
    up the detections for this specific photo and pass each one (or None for
    the full-image fallback) to _prepare_image.
    """
    import classifier as classifier_mod
    import classify_job
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)
    photo_id = db.add_photo(folder_id, "bird.jpg", ".jpg", 12345, 1_000_000.0)
    _drop_jpeg(folder_path, "bird.jpg")
    col_id = db.add_collection(
        "Test",
        json.dumps([{"field": "photo_ids", "value": [photo_id]}]),
    )

    model_id = _setup_fake_downloaded_model(tmp_path, monkeypatch)

    primary_det = {
        "id": 77,
        "box_x": 0.1, "box_y": 0.1, "box_w": 0.5, "box_h": 0.5,
        "confidence": 0.95, "category": "animal",
    }
    secondary_det = {
        "id": 78,
        "box_x": 0.6, "box_y": 0.2, "box_w": 0.25, "box_h": 0.3,
        "confidence": 0.45, "category": "animal",
    }
    below_threshold_det = {
        "id": 79,
        "box_x": 0.8, "box_y": 0.8, "box_w": 0.05, "box_h": 0.05,
        "confidence": 0.1, "category": "animal",
    }

    def fake_detect_batch(batch, folders, runner, job, reclassify, db_,
                          det_conf_threshold=None, already_detected_ids=None,
                          cached_detections=None):
        det_map = {
            p["id"]: [primary_det, secondary_det, below_threshold_det]
            for p in batch
        }
        return det_map, len(batch), {p["id"] for p in batch}

    monkeypatch.setattr(classify_job, "_detect_batch", fake_detect_batch)

    captured = []

    def capturing_prepare_image(photo, folders, detection, vireo_dir=None):
        captured.append(detection)
        # Returning (None, ...) tells the caller this photo failed to load,
        # which short-circuits _flush_batch.  We only care about the
        # arguments passed in.
        return None, "", ""

    monkeypatch.setattr(classify_job, "_prepare_image", capturing_prepare_image)

    class FakeClassifier:
        def __init__(self, *args, **kwargs):
            pass

        def encode_image(self, *args, **kwargs):
            import numpy as np
            return np.zeros(512, dtype=np.float32)

        def classify_with_embedding(self, img, threshold=0):
            import numpy as np
            return [{"species": "Robin", "score": 0.9}], np.zeros(
                512, dtype=np.float32,
            )

        def classify_batch_with_embedding(self, images, threshold=0):
            import numpy as np
            zero = np.zeros(512, dtype=np.float32)
            return [(
                [{"species": "Robin", "score": 0.9}],
                zero,
            ) for _ in images]

    monkeypatch.setattr(classifier_mod, "Classifier", FakeClassifier)

    params = PipelineParams(
        collection_id=col_id,
        model_ids=[model_id],
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()
    # The fake _prepare_image returns None by design to short-circuit before
    # the classifier runs — we only care about what argument it received.
    # That now counts as a classify failure, which propagates to a pipeline
    # RuntimeError; swallow it so we can still inspect `captured`.
    with contextlib.suppress(RuntimeError):
        run_pipeline_job(job, runner, db_path, ws_id, params)

    assert captured, (
        "_prepare_image was never called — test setup no longer exercises "
        "the classify crop path."
    )
    # Every call must receive either None (no detection) or a detection dict
    # with a 'box_w' key — never the raw {photo_id: [dets]} map.
    for det in captured:
        assert det is None or (isinstance(det, dict) and "box_w" in det), (
            f"_prepare_image received {det!r}; expected a detection dict "
            "with 'box_w' (or None), not the {photo_id: [dets]} map."
        )
    captured_ids = [d.get("id") for d in captured if isinstance(d, dict)]
    assert captured_ids == [77, 78], (
        f"Expected both detections above the 0.2 workspace threshold, "
        f"and no raw low-confidence detection; got {captured!r}."
    )
    # And the KeyError must not have leaked into job errors.
    assert not any("'box_w'" in e for e in job["errors"]), (
        f"KeyError 'box_w' leaked into job errors: {job['errors']}"
    )


def test_pipeline_classifies_bracketed_weak_detection_without_lowering_threshold(
    tmp_path, monkeypatch,
):
    """A weak box between two strong, tightly timed frames gets one targeted
    classifier attempt even though the workspace threshold remains 0.20.
    """
    import classifier as classifier_mod
    import classify_job
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)

    photo_ids = []
    for index, confidence in enumerate((0.9, 0.18, 0.9)):
        filename = f"bird{index}.jpg"
        photo_id = db.add_photo(
            folder_id, filename, ".jpg", 12345, 1_000_000.0 + index,
            timestamp=f"2026-07-18T08:36:3{index}",
        )
        _drop_jpeg(folder_path, filename)
        db.write_detection_batch(
            photo_id,
            "megadetector-v6",
            [{
                "box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
                "confidence": confidence,
                "category": "animal",
            }],
        )
        photo_ids.append(photo_id)

    collection_id = db.add_collection(
        "Weak bridge",
        json.dumps([{"field": "photo_ids", "value": photo_ids}]),
    )
    model_id = _setup_fake_downloaded_model(tmp_path, monkeypatch)

    captured = []

    def capturing_prepare_image(photo, folders, detection, vireo_dir=None):
        captured.append((photo["filename"], detection["confidence"]))
        return None, "", ""

    monkeypatch.setattr(classify_job, "_prepare_image", capturing_prepare_image)

    class FakeClassifier:
        def __init__(self, *args, **kwargs):
            pass

    monkeypatch.setattr(classifier_mod, "Classifier", FakeClassifier)

    params = PipelineParams(
        collection_id=collection_id,
        model_ids=[model_id],
        skip_extract_masks=True,
        skip_regroup=True,
    )
    runner = FakeRunner()
    job = _make_job()
    with contextlib.suppress(RuntimeError):
        run_pipeline_job(job, runner, db_path, ws_id, params)

    assert ("bird1.jpg", 0.18) in captured
    assert [name for name, _ in captured].count("bird1.jpg") == 1
    assert db.get_detections(
        photo_ids[1], detector_model="full-image", min_conf=0,
    ) == []


def test_pipeline_reclassify_purges_stale_detection_rows(tmp_path, monkeypatch):
    """On reclassify, prior-run detection rows must be deleted after model 1
    re-runs MegaDetector so that subsequent non-reclassify runs don't reuse
    stale bounding boxes via get_existing_detection_photo_ids + get_detections.

    Scenario: a photo had a prior detection (potential false positive). The
    reclassify run finds NO animals this time (fake_detect_batch returns {}).
    After reclassify the old detection row must be gone so future runs
    actually call MegaDetector rather than short-circuiting to the stale box.

    Regression for Codex P1 review on #511 line 848.
    """
    import json

    import classifier as classifier_mod
    import classify_job
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)
    photo_id = db.add_photo(folder_id, "test.jpg", ".jpg", 12345, 1_000_000.0)
    _drop_jpeg(folder_path, "test.jpg")

    # Prior-run detection row (e.g. a prior false positive).
    prior_det_ids = db.save_detections(
        photo_id,
        [
            {"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
             "confidence": 0.9, "category": "animal"},
        ],
        detector_model="MegaDetector",
    )
    assert prior_det_ids, "setup sanity: prior detection was inserted"

    col_id = db.add_collection(
        "Test",
        json.dumps([{"field": "photo_ids", "value": [photo_id]}]),
    )

    model_ids = _setup_two_fake_downloaded_models(tmp_path, monkeypatch)

    # _detect_batch stub: reclassify finds no animals this time (false pos fixed).
    def fake_detect_batch(batch, folders, runner, job, reclassify, db_,
                          det_conf_threshold=None, already_detected_ids=None,
                          cached_detections=None):
        return {}, 0, {p["id"] for p in batch}

    monkeypatch.setattr(classify_job, "_detect_batch", fake_detect_batch)

    class FakeClassifier:
        def __init__(self, *args, **kwargs):
            pass

        def encode_image(self, *args, **kwargs):
            import numpy as np
            return np.zeros(512, dtype=np.float32)

        def classify_with_embedding(self, img, threshold=0):
            import numpy as np
            return [{"species": "Robin", "score": 0.9}], np.zeros(
                512, dtype=np.float32,
            )

        def classify_batch_with_embedding(self, images, threshold=0):
            import numpy as np
            zero = np.zeros(512, dtype=np.float32)
            return [(
                [{"species": "Robin", "score": 0.9}],
                zero,
            ) for _ in images]

    monkeypatch.setattr(classifier_mod, "Classifier", FakeClassifier)

    params = PipelineParams(
        collection_id=col_id,
        model_ids=model_ids[:1],
        reclassify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()

    run_pipeline_job(job, runner, db_path, ws_id, params)

    # The stale prior-run detection must be gone after reclassify so that
    # future non-reclassify runs don't reuse it via the already-detected path.
    verify_db = Database(db_path)
    verify_db.set_active_workspace(ws_id)
    remaining = verify_db.get_detections(photo_id)
    assert remaining == [], (
        f"Stale prior-run detection rows must be purged during reclassify but "
        f"db.get_detections({photo_id}) returned {remaining!r}. "
        "Without this cleanup, future non-reclassify runs short-circuit to "
        "stale boxes via get_existing_detection_photo_ids + get_detections, "
        "causing false-positive detections to persist indefinitely. "
        "Regression for Codex P1 review on #511 line 848."
    )


def test_pipeline_reclassify_same_boxes_preserves_predictions(
    tmp_path, monkeypatch,
):
    """Reclassify that re-detects the SAME boxes must NOT purge the rows it
    just rewrote.

    Detection ids are content-addressed, so re-detecting an unchanged box
    yields the same id as the pre-run snapshot. write_detection_batch UPSERTs
    that row and classify writes fresh predictions onto it. The reclassify
    purge used to delete every pre-run id unconditionally — which, with stable
    ids, meant deleting the live detection and cascading the just-written
    predictions for every photo whose boxes didn't change (the common case).
    The purge must subtract the current live set so only genuinely-dropped
    rows are deleted. (Codex P1 family on PR #907 / latent since the
    content-addressed-id merge.)
    """
    import classifier as classifier_mod
    import classify_job
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)
    photo_id = db.add_photo(folder_id, "test.jpg", ".jpg", 12345, 1_000_000.0)
    _drop_jpeg(folder_path, "test.jpg")

    box = {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5}
    prior = db.save_detections(
        photo_id,
        [{"box": box, "confidence": 0.9, "category": "animal"}],
        detector_model="megadetector-v6",
    )
    prior_id = prior[0]

    col_id = db.add_collection(
        "Test", json.dumps([{"field": "photo_ids", "value": [photo_id]}]),
    )
    model_ids = _setup_two_fake_downloaded_models(tmp_path, monkeypatch)

    # Reclassify re-detects the SAME box → content-addressed id == prior_id.
    def fake_detect_batch(batch, folders, runner, job, reclassify, db_,
                          det_conf_threshold=None, already_detected_ids=None,
                          cached_detections=None):
        dmap = {}
        for p in batch:
            ids = db_.write_detection_batch(
                p["id"], "megadetector-v6",
                [{"box": box, "confidence": 0.9, "category": "animal"}],
            )
            dmap[p["id"]] = [{
                "id": ids[0], "box_x": box["x"], "box_y": box["y"],
                "box_w": box["w"], "box_h": box["h"], "confidence": 0.9,
                "category": "animal", "detector_model": "megadetector-v6",
            }]
        return dmap, len(batch), {p["id"] for p in batch}

    monkeypatch.setattr(classify_job, "_detect_batch", fake_detect_batch)

    class FakeClassifier:
        def __init__(self, *a, **k):
            pass

        def classify_with_embedding(self, img, threshold=0):
            import numpy as np
            return ([{"species": "Robin", "score": 0.9}],
                    np.zeros(512, dtype=np.float32))

        def classify_batch_with_embedding(self, images, threshold=0):
            import numpy as np
            z = np.zeros(512, dtype=np.float32)
            return [([{"species": "Robin", "score": 0.9}], z) for _ in images]

    monkeypatch.setattr(classifier_mod, "Classifier", FakeClassifier)

    params = PipelineParams(
        collection_id=col_id, model_ids=model_ids[:1], reclassify=True,
        skip_extract_masks=True, skip_regroup=True,
    )
    run_pipeline_job(_make_job(), FakeRunner(), db_path, ws_id, params)

    verify = Database(db_path)
    verify.set_active_workspace(ws_id)
    assert verify.get_detections(photo_id) != [], (
        "re-detected detection (same box → same content id) must survive "
        "reclassify, not be purged as 'stale'"
    )
    preds = verify.conn.execute(
        "SELECT COUNT(*) AS c FROM predictions WHERE detection_id=?",
        (prior_id,),
    ).fetchone()["c"]
    assert preds >= 1, (
        f"freshly-written predictions must survive the reclassify purge; "
        f"got {preds}"
    )


def test_detect_batch_skips_empty_photo_on_rerun(tmp_path, monkeypatch):
    """A photo with no animals, recorded in detector_runs, must not be
    re-detected on a subsequent non-reclassify pipeline run.

    Mirrors test_classify_job.test_detect_batch_skips_empty_photo_on_rerun
    but drives through run_pipeline_job's detect_stage so we exercise the
    pipeline's own already_detected seeding (which must use
    get_detector_run_photo_ids, not the legacy get_existing_detection_photo_ids
    shim that misses empty-scene photos).
    """
    import json

    import classifier as classifier_mod
    import classify_job
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)
    photo_id = db.add_photo(folder_id, "empty.jpg", ".jpg", 12345, 1_000_000.0)
    _drop_jpeg(folder_path, "empty.jpg")

    # Simulate a prior run where MegaDetector scanned the photo and found
    # NOTHING — there are no detection rows, but detector_runs records the
    # scan so the next pipeline pass can skip re-invoking the detector.
    db.save_detections(photo_id, [], detector_model="megadetector-v6")
    db.record_detector_run(photo_id, "megadetector-v6", box_count=0)

    col_id = db.add_collection(
        "Test",
        json.dumps([{"field": "photo_ids", "value": [photo_id]}]),
    )

    model_id = _setup_fake_downloaded_model(tmp_path, monkeypatch)

    # Remove the legacy shim so the pipeline must call
    # get_detector_run_photo_ids directly. If pipeline still uses the
    # legacy name (via getattr) it will fall through to the default
    # `lambda: set()` and miss our empty-scene photo.
    monkeypatch.delattr(Database, "get_existing_detection_photo_ids")

    # Capture what already_detected_ids the pipeline passes to _detect_batch.
    # If the pipeline seeds correctly from get_detector_run_photo_ids, our
    # empty-scene photo will appear in already_detected_ids — meaning the
    # real _detect_batch would skip re-invoking MegaDetector for it.
    detect_calls = []

    def fake_detect_batch(batch, folders, runner, job, reclassify, db_,
                          det_conf_threshold=None, already_detected_ids=None,
                          cached_detections=None):
        detect_calls.append({
            "already_detected_ids": frozenset(already_detected_ids or set()),
            "batch_ids": [p["id"] for p in batch],
        })
        # Simulate the real _detect_batch's skip behaviour: if the photo
        # is already in already_detected_ids, don't re-"detect" it.
        processed = {p["id"] for p in batch}
        return {}, 0, processed

    monkeypatch.setattr(classify_job, "_detect_batch", fake_detect_batch)

    # Patch classify_stage's prediction-photo lookup so classify_stage
    # doesn't trip on pre-existing schema issues in other migrations.
    # (This test focuses narrowly on detect_stage's already_detected seed.)
    monkeypatch.setattr(
        Database, "get_existing_prediction_photo_ids",
        lambda self, model_name: set(),
    )

    class FakeClassifier:
        def __init__(self, *args, **kwargs):
            pass

        def encode_image(self, *args, **kwargs):
            import numpy as np
            return np.zeros(512, dtype=np.float32)

        def classify_with_embedding(self, img, threshold=0):
            import numpy as np
            return [{"species": "Robin", "score": 0.9}], np.zeros(
                512, dtype=np.float32,
            )

        def classify_batch_with_embedding(self, images, threshold=0):
            import numpy as np
            zero = np.zeros(512, dtype=np.float32)
            return [(
                [{"species": "Robin", "score": 0.9}],
                zero,
            ) for _ in images]

    monkeypatch.setattr(classifier_mod, "Classifier", FakeClassifier)

    params = PipelineParams(
        collection_id=col_id,
        model_ids=[model_id],
        reclassify=False,  # non-reclassify: must honour detector_runs skip
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()
    # classify_stage may still fail against pre-migration schema bits that
    # this task doesn't own. We only need detect_stage to have run.
    with contextlib.suppress(RuntimeError):
        run_pipeline_job(job, runner, db_path, ws_id, params)

    assert detect_calls, "expected detect_stage to call _detect_batch"
    first_call = detect_calls[0]
    assert photo_id in first_call["already_detected_ids"], (
        f"Empty-scene photo {photo_id} was recorded in detector_runs but "
        f"pipeline did not seed it into already_detected_ids "
        f"(got {set(first_call['already_detected_ids'])!r}). "
        f"detect_stage must seed from get_detector_run_photo_ids "
        f"('megadetector-v6'), not the legacy "
        f"get_existing_detection_photo_ids shim which misses empty-scene photos."
    )


def test_pipeline_reclassify_partial_abort_preserves_unprocessed_detections(
    tmp_path, monkeypatch
):
    """A reclassify aborted before any model finishes classifying must NOT
    delete pre-run detection rows. The purge is gated on a successful
    model run (see Codex P1 on #566) — otherwise a cancel mid-detect would
    destroy prior detections with no replacement predictions.

    Scenario: 2 photos each have a prior detection row. Batch size is
    patched to 1 so each photo is its own batch in detect_stage. After
    the first detect batch completes, _should_abort returns True so the
    rest of the pipeline short-circuits before classify_stage writes any
    predictions.

    Expected outcome: BOTH photos' prior detection rows are preserved,
    because `models_succeeded` never reaches 1.
    """
    import json

    import classifier as classifier_mod
    import classify_job
    import config as cfg
    import pipeline_job as pj
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)
    photo1_id = db.add_photo(folder_id, "photo1.jpg", ".jpg", 11111, 1_000_000.0)
    photo2_id = db.add_photo(folder_id, "photo2.jpg", ".jpg", 22222, 1_000_000.0)
    _drop_jpeg(folder_path, "photo1.jpg")
    _drop_jpeg(folder_path, "photo2.jpg")

    # Give each photo a prior-run detection row.
    prior_det1 = db.save_detections(
        photo1_id,
        [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
          "confidence": 0.9, "category": "animal"}],
        detector_model="MegaDetector",
    )
    prior_det2 = db.save_detections(
        photo2_id,
        [{"box": {"x": 0.2, "y": 0.2, "w": 0.3, "h": 0.3},
          "confidence": 0.8, "category": "animal"}],
        detector_model="MegaDetector",
    )
    assert prior_det1 and prior_det2, "setup sanity: prior detections inserted"

    col_id = db.add_collection(
        "Test",
        json.dumps([{"field": "photo_ids", "value": [photo1_id, photo2_id]}]),
    )

    model_ids = _setup_two_fake_downloaded_models(tmp_path, monkeypatch)

    # Process one photo per batch so we can abort between them.
    monkeypatch.setattr(classify_job, "_BATCH_SIZE", 1)

    # After the first _detect_batch call, all subsequent _should_abort checks
    # return True, preventing the second batch from being processed.
    detect_call_count = [0]
    original_should_abort = pj._should_abort

    def patched_should_abort(event):
        if detect_call_count[0] >= 1:
            return True
        return original_should_abort(event)

    monkeypatch.setattr(pj, "_should_abort", patched_should_abort)

    def fake_detect_batch(batch, folders, runner, job, reclassify, db_,
                          det_conf_threshold=None, already_detected_ids=None,
                          cached_detections=None):
        detect_call_count[0] += 1
        return {}, 0, {p["id"] for p in batch}

    monkeypatch.setattr(classify_job, "_detect_batch", fake_detect_batch)

    class FakeClassifier:
        def __init__(self, *args, **kwargs):
            pass

        def encode_image(self, *args, **kwargs):
            import numpy as np
            return np.zeros(512, dtype=np.float32)

        def classify_with_embedding(self, img, threshold=0):
            import numpy as np
            return [{"species": "Robin", "score": 0.9}], np.zeros(
                512, dtype=np.float32,
            )

        def classify_batch_with_embedding(self, images, threshold=0):
            import numpy as np
            zero = np.zeros(512, dtype=np.float32)
            return [(
                [{"species": "Robin", "score": 0.9}],
                zero,
            ) for _ in images]

    monkeypatch.setattr(classifier_mod, "Classifier", FakeClassifier)

    params = PipelineParams(
        collection_id=col_id,
        model_ids=model_ids[:1],
        reclassify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()

    run_pipeline_job(job, runner, db_path, ws_id, params)

    assert detect_call_count[0] == 1, (
        "Expected exactly one _detect_batch call before abort; "
        f"got {detect_call_count[0]}"
    )

    verify_db = Database(db_path)
    verify_db.set_active_workspace(ws_id)

    remaining1 = verify_db.get_detections(photo1_id)
    remaining2 = verify_db.get_detections(photo2_id)

    # No model ran to completion (abort fired before classify could store
    # predictions), so neither photo's prior row may be purged — otherwise
    # cancelling a reclassify would destroy prior detections and their
    # cascaded predictions with no replacement data.
    assert remaining1, (
        f"photo1's prior detection row must be preserved on an aborted "
        f"reclassify — no classifier ran to completion, so the stale purge "
        f"must not fire. get_detections returned {remaining1!r}."
    )
    assert remaining2, (
        "photo2's prior detection row must be preserved on an aborted "
        "reclassify — no classifier ran to completion."
    )


def test_pipeline_reclassify_partial_batch_exception_preserves_detections(
    tmp_path, monkeypatch
):
    """A reclassify where _detect_batch exits mid-batch on an exception must
    NOT delete detection rows for the photos that were never actually
    reached inside that batch.

    Scenario: two photos share a single batch.  _detect_batch only
    completes the per-photo iteration for the first photo and returns early
    (simulating detect_animals raising while processing photo2 — the real
    _detect_batch catches the exception at function level and returns the
    accumulated detection_map with only the already-processed photos).

    Expected outcome:
    - photo1 (whose iteration completed) has its stale prior-run row purged.
    - photo2 (whose iteration never ran) keeps its stale prior-run row.

    Regression for Codex P1 review on #513 line 981 — the purge must be
    keyed to per-photo processing completion, not the full submitted batch.
    """
    import json

    import classifier as classifier_mod
    import classify_job
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)
    photo1_id = db.add_photo(folder_id, "photo1.jpg", ".jpg", 11111, 1_000_000.0)
    photo2_id = db.add_photo(folder_id, "photo2.jpg", ".jpg", 22222, 1_000_000.0)
    _drop_jpeg(folder_path, "photo1.jpg")
    _drop_jpeg(folder_path, "photo2.jpg")

    prior_det1 = db.save_detections(
        photo1_id,
        [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
          "confidence": 0.9, "category": "animal"}],
        detector_model="MegaDetector",
    )
    prior_det2 = db.save_detections(
        photo2_id,
        [{"box": {"x": 0.2, "y": 0.2, "w": 0.3, "h": 0.3},
          "confidence": 0.8, "category": "animal"}],
        detector_model="MegaDetector",
    )
    assert prior_det1 and prior_det2, "setup sanity: prior detections inserted"

    col_id = db.add_collection(
        "Test",
        json.dumps([{"field": "photo_ids", "value": [photo1_id, photo2_id]}]),
    )

    model_ids = _setup_two_fake_downloaded_models(tmp_path, monkeypatch)

    # Both photos land in a single batch.  The stub returns a processed_ids
    # set containing ONLY photo1, mirroring what _detect_batch does when
    # detect_animals raises while processing photo2: the try/except at the
    # function level returns the accumulated results and photo2 never makes
    # it into processed_ids.
    def fake_detect_batch(batch, folders, runner, job, reclassify, db_,
                          det_conf_threshold=None, already_detected_ids=None,
                          cached_detections=None):
        return {}, 0, {photo1_id}

    monkeypatch.setattr(classify_job, "_detect_batch", fake_detect_batch)

    class FakeClassifier:
        def __init__(self, *args, **kwargs):
            pass

        def encode_image(self, *args, **kwargs):
            import numpy as np
            return np.zeros(512, dtype=np.float32)

        def classify_with_embedding(self, img, threshold=0):
            import numpy as np
            return [{"species": "Robin", "score": 0.9}], np.zeros(
                512, dtype=np.float32,
            )

        def classify_batch_with_embedding(self, images, threshold=0):
            import numpy as np
            zero = np.zeros(512, dtype=np.float32)
            return [(
                [{"species": "Robin", "score": 0.9}],
                zero,
            ) for _ in images]

    monkeypatch.setattr(classifier_mod, "Classifier", FakeClassifier)

    params = PipelineParams(
        collection_id=col_id,
        model_ids=model_ids[:1],
        reclassify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()

    # Classification may fail (no real JPEGs on disk, stub classifier
    # misses methods) — we only care about the purge scope here, not the
    # pipeline exit code. Any RuntimeError gets swallowed.
    with contextlib.suppress(RuntimeError):
        run_pipeline_job(job, runner, db_path, ws_id, params)

    verify_db = Database(db_path)
    verify_db.set_active_workspace(ws_id)

    remaining1 = verify_db.get_detections(photo1_id)
    remaining2 = verify_db.get_detections(photo2_id)

    assert remaining1 == [], (
        f"photo1's per-photo iteration completed in _detect_batch; its stale "
        f"prior-run row must be purged, but get_detections returned "
        f"{remaining1!r}. Regression for Codex P1 review on #513 line 981."
    )
    assert remaining2 != [], (
        "photo2's iteration never ran (simulated mid-batch exception in "
        "_detect_batch).  Its stale prior-run detection row must be "
        "preserved — purging it would cascade-delete predictions for a "
        "photo that was never re-detected.  "
        "Regression for Codex P1 review on #513 line 981."
    )


def test_pipeline_classify_mid_batch_cancel_skips_storage(tmp_path, monkeypatch):
    """A mid-classify cancel must take effect within roughly one photo's
    worth of work (not at the next 32-photo batch boundary), and must skip
    _store_grouped_predictions, which can take a minute on large
    collections.  The per-model step is finalized with a 'Cancelled'
    summary so the user sees the partial state in the job tree.
    """
    import threading

    import classifier as classifier_mod
    import classify_job
    import config as cfg
    import pipeline_job as pj
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)
    photo_ids = []
    for i in range(3):
        name = f"photo{i}.jpg"
        pid = db.add_photo(folder_id, name, ".jpg", 1000 + i, 1_000_000.0 + i)
        _drop_jpeg(folder_path, name)
        db.save_detections(
            pid,
            [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
              "confidence": 0.9, "category": "animal"}],
            detector_model="MegaDetector",
        )
        photo_ids.append(pid)

    col_id = db.add_collection(
        "Test",
        json.dumps([{"field": "photo_ids", "value": photo_ids}]),
    )

    model_id = _setup_fake_downloaded_model(tmp_path, monkeypatch)

    # detect_stage stub: surface the prior-run detection rows we inserted
    # above as if MegaDetector just produced them, so classify_stage's
    # cached_detections lookup hits with real DB ids (record_classifier_run
    # has a FK to detections.id).
    def fake_detect_batch(batch, folders, runner, job, reclassify, db_,
                          det_conf_threshold=None, already_detected_ids=None,
                          cached_detections=None):
        det_map = {}
        for p in batch:
            existing = db_.get_detections(p["id"])
            det_map[p["id"]] = [{
                "id": d["id"],
                "box_x": d["box_x"], "box_y": d["box_y"],
                "box_w": d["box_w"], "box_h": d["box_h"],
                "confidence": d["detector_confidence"],
                "category": d["category"],
            } for d in existing if d["detector_model"] != "full-image"]
        return det_map, len(batch), {p["id"] for p in batch}

    monkeypatch.setattr(classify_job, "_detect_batch", fake_detect_batch)

    # _prepare_image opens the real image and crops it. Stub it to a fake
    # PIL image so the per-photo loop progresses to _flush_batch regardless
    # of the dummy 16x16 black JPEGs on disk.
    from PIL import Image as _PILImage

    def fake_prepare_image(photo, folders, detection, vireo_dir=None):
        folder_path = folders.get(photo["folder_id"], "")
        image_path = os.path.join(folder_path, photo["filename"])
        return _PILImage.new("RGB", (16, 16), "black"), folder_path, image_path

    monkeypatch.setattr(classify_job, "_prepare_image", fake_prepare_image)

    # Spy on _flush_batch: count calls, populate raw_results with a fake
    # prediction so the per-model step has something to report, and trigger
    # abort after the FIRST call so the inner-loop abort check is exercised
    # on the second photo.
    abort_after_classify = threading.Event()
    flush_calls = [0]

    def spy_flush_batch(batch, clf, model_type, model_name, db_, raw_results,
                        top_k=1):
        flush_calls[0] += 1
        for entry in batch:
            raw_results.append({
                "photo": entry["photo"],
                "detection_id": entry.get("detection_id"),
                "folder_path": entry["folder_path"],
                "image_path": entry["image_path"],
                "prediction": "Robin",
                "confidence": 0.9,
                "timestamp": None,
                "filename": entry["photo"]["filename"],
                "embedding": None,
                "taxonomy": None,
            })
        abort_after_classify.set()
        return 0

    monkeypatch.setattr(classify_job, "_flush_batch", spy_flush_batch)

    # Spy on _store_grouped_predictions to verify the cancel path skips it.
    store_calls = [0]

    def spy_store(*args, **kwargs):
        store_calls[0] += 1
        return {"predictions_stored": 0, "burst_groups": 0,
                "already_labeled": 0}

    monkeypatch.setattr(classify_job, "_store_grouped_predictions", spy_store)

    original_should_abort = pj._should_abort

    def patched_should_abort(event):
        if abort_after_classify.is_set():
            return True
        return original_should_abort(event)

    monkeypatch.setattr(pj, "_should_abort", patched_should_abort)

    class FakeClassifier:
        def __init__(self, *args, **kwargs):
            pass

        def encode_image(self, *args, **kwargs):
            import numpy as np
            return np.zeros(512, dtype=np.float32)

    monkeypatch.setattr(classifier_mod, "Classifier", FakeClassifier)

    params = PipelineParams(
        collection_id=col_id,
        reclassify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()

    run_pipeline_job(job, runner, db_path, ws_id, params)

    # Without the fix, all 3 photos are classified before the next outer
    # batch boundary fires (batch_size=32 > 3 photos). With the fix, the
    # inner-loop check catches abort on photo 2.
    assert 1 <= flush_calls[0] <= 2, (
        f"Expected classify to stop within ~1 photo of abort; got "
        f"{flush_calls[0]} _flush_batch calls. Without the inner-loop "
        f"abort check this would be 3."
    )

    # _store_grouped_predictions is the slow tail that the user reported
    # as 'still going' after cancel.  The cancel path must skip it.
    assert store_calls[0] == 0, (
        f"_store_grouped_predictions must NOT run on a mid-batch cancel; "
        f"got {store_calls[0]} calls."
    )

    classify_step_id = f"classify:{model_id}"
    cancelled_updates = [
        kw for (_, sid, kw) in runner.step_updates
        if sid == classify_step_id and "Cancelled" in (kw.get("summary") or "")
    ]
    assert cancelled_updates, (
        f"Expected at least one update on {classify_step_id!r} with a "
        f"'Cancelled' summary; got step_updates={runner.step_updates!r}"
    )

    # Progress on the cancelled step must reflect what was *actually*
    # classified (1 photo), not what the per-batch progress event claimed
    # (the entire batch).  Without the corrected progress emit, the step
    # would show 3/3 even though only 1 photo was inferred — Codex P2.
    cancelled_kw = cancelled_updates[-1]
    assert cancelled_kw.get("progress") == {"current": 1, "total": 3}, (
        f"Cancelled step must show actual processed count (1/3), not "
        f"the pre-emptive batch claim (3/3). Got progress="
        f"{cancelled_kw.get('progress')!r}"
    )
    assert "1 of 3 processed" in (cancelled_kw.get("summary") or ""), (
        f"Cancelled summary should report actual processed count; got "
        f"{cancelled_kw.get('summary')!r}"
    )


def test_pipeline_reclassify_cancel_preserves_existing_predictions(
    tmp_path, monkeypatch,
):
    """A reclassify cancelled mid-classify must NOT erase the user's prior
    predictions. The reclassify clear runs only once we're committed to
    storing fresh results — Codex P1 review on #710. Without this guard,
    `clear_predictions` had already wiped the predictions table when the
    cancel guard skipped `_store_grouped_predictions`, leaving the model
    with no predictions for the entire collection.
    """
    import threading

    import classifier as classifier_mod
    import classify_job
    import config as cfg
    import pipeline_job as pj
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)
    photo_ids = []
    detection_ids = []
    for i in range(3):
        name = f"photo{i}.jpg"
        pid = db.add_photo(folder_id, name, ".jpg", 3000 + i, 3_000_000.0 + i)
        _drop_jpeg(folder_path, name)
        det_ids = db.save_detections(
            pid,
            [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
              "confidence": 0.9, "category": "animal"}],
            detector_model="MegaDetector",
        )
        photo_ids.append(pid)
        detection_ids.append(det_ids[0])

    # Insert pre-existing predictions for each detection under the model
    # name and fingerprint the pipeline will use ('BioCLIP' / 'legacy' for
    # the test stubs).  These are what must survive the cancelled reclassify.
    for det_id in detection_ids:
        db.add_prediction(
            detection_id=det_id,
            species="Pre-existing Sparrow",
            confidence=0.95,
            model="BioCLIP",
            labels_fingerprint="legacy",
        )

    col_id = db.add_collection(
        "Test",
        json.dumps([{"field": "photo_ids", "value": photo_ids}]),
    )

    model_id = _setup_fake_downloaded_model(tmp_path, monkeypatch)

    # Force the bundle's labels_fingerprint to 'legacy' so it matches the
    # add_prediction calls above.
    import labels_fingerprint as lfp
    monkeypatch.setattr(lfp, "compute_fingerprint", lambda *a, **k: "legacy")

    def fake_detect_batch(batch, folders, runner, job, reclassify, db_,
                          det_conf_threshold=None, already_detected_ids=None,
                          cached_detections=None):
        det_map = {}
        for p in batch:
            existing = db_.get_detections(p["id"])
            det_map[p["id"]] = [{
                "id": d["id"],
                "box_x": d["box_x"], "box_y": d["box_y"],
                "box_w": d["box_w"], "box_h": d["box_h"],
                "confidence": d["detector_confidence"],
                "category": d["category"],
            } for d in existing if d["detector_model"] != "full-image"]
        return det_map, len(batch), {p["id"] for p in batch}

    monkeypatch.setattr(classify_job, "_detect_batch", fake_detect_batch)

    from PIL import Image as _PILImage

    def fake_prepare_image(photo, folders, detection, vireo_dir=None):
        folder_path = folders.get(photo["folder_id"], "")
        image_path = os.path.join(folder_path, photo["filename"])
        return _PILImage.new("RGB", (16, 16), "black"), folder_path, image_path

    monkeypatch.setattr(classify_job, "_prepare_image", fake_prepare_image)

    abort_after_classify = threading.Event()

    def spy_flush_batch(batch, clf, model_type, model_name, db_, raw_results,
                        top_k=1):
        for entry in batch:
            raw_results.append({
                "photo": entry["photo"],
                "detection_id": entry.get("detection_id"),
                "folder_path": entry["folder_path"],
                "image_path": entry["image_path"],
                "prediction": "Robin",
                "confidence": 0.9,
                "timestamp": None,
                "filename": entry["photo"]["filename"],
                "embedding": None,
                "taxonomy": None,
            })
        abort_after_classify.set()
        return 0

    monkeypatch.setattr(classify_job, "_flush_batch", spy_flush_batch)

    original_should_abort = pj._should_abort

    def patched_should_abort(event):
        if abort_after_classify.is_set():
            return True
        return original_should_abort(event)

    monkeypatch.setattr(pj, "_should_abort", patched_should_abort)

    class FakeClassifier:
        def __init__(self, *args, **kwargs):
            pass

        def encode_image(self, *args, **kwargs):
            import numpy as np
            return np.zeros(512, dtype=np.float32)

    monkeypatch.setattr(classifier_mod, "Classifier", FakeClassifier)

    params = PipelineParams(
        collection_id=col_id,
        reclassify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()

    run_pipeline_job(job, runner, db_path, ws_id, params)

    verify_db = Database(db_path)
    verify_db.set_active_workspace(ws_id)

    # All three pre-existing predictions must still be in the table.
    # Before the fix, clear_predictions ran at the top of the per-spec body
    # and wiped them; the cancel guard skipped _store_grouped_predictions,
    # leaving the predictions table empty for this model.
    surviving = verify_db.conn.execute(
        "SELECT COUNT(*) FROM predictions "
        "WHERE classifier_model = ? AND labels_fingerprint = ? "
        "AND species = ?",
        ("BioCLIP", "legacy", "Pre-existing Sparrow"),
    ).fetchone()[0]
    assert surviving == 3, (
        f"A cancelled reclassify must NOT wipe the user's prior "
        f"predictions. Expected all 3 'Pre-existing Sparrow' rows to "
        f"survive; found {surviving}."
    )


def test_pipeline_reclassify_success_preserves_classifier_run_keys(
    tmp_path, monkeypatch,
):
    """A successful reclassify must leave fresh classifier_runs rows in
    place for the processed detections so the next non-reclassify pass
    hits the skip gate.  The deferred reclassify clear runs AFTER the
    per-photo ``record_classifier_run`` calls; without ``clear_run_keys=False``
    it wipes the just-written run keys, forcing the next normal classify to
    re-infer the entire collection.  Codex P1 review on #710.
    """
    import classifier as classifier_mod
    import classify_job
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)
    photo_ids = []
    detection_ids = []
    for i in range(3):
        name = f"photo{i}.jpg"
        pid = db.add_photo(folder_id, name, ".jpg", 4000 + i, 4_000_000.0 + i)
        _drop_jpeg(folder_path, name)
        det_ids = db.save_detections(
            pid,
            [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
              "confidence": 0.9, "category": "animal"}],
            detector_model="MegaDetector",
        )
        photo_ids.append(pid)
        detection_ids.append(det_ids[0])

    col_id = db.add_collection(
        "Test",
        json.dumps([{"field": "photo_ids", "value": photo_ids}]),
    )

    _setup_fake_downloaded_model(tmp_path, monkeypatch)

    import labels_fingerprint as lfp
    monkeypatch.setattr(lfp, "compute_fingerprint", lambda *a, **k: "legacy")

    def fake_detect_batch(batch, folders, runner, job, reclassify, db_,
                          det_conf_threshold=None, already_detected_ids=None,
                          cached_detections=None):
        det_map = {}
        for p in batch:
            existing = db_.get_detections(p["id"])
            det_map[p["id"]] = [{
                "id": d["id"],
                "box_x": d["box_x"], "box_y": d["box_y"],
                "box_w": d["box_w"], "box_h": d["box_h"],
                "confidence": d["detector_confidence"],
                "category": d["category"],
            } for d in existing if d["detector_model"] != "full-image"]
        return det_map, len(batch), {p["id"] for p in batch}

    monkeypatch.setattr(classify_job, "_detect_batch", fake_detect_batch)

    from PIL import Image as _PILImage

    def fake_prepare_image(photo, folders, detection, vireo_dir=None):
        folder_path = folders.get(photo["folder_id"], "")
        image_path = os.path.join(folder_path, photo["filename"])
        return _PILImage.new("RGB", (16, 16), "black"), folder_path, image_path

    monkeypatch.setattr(classify_job, "_prepare_image", fake_prepare_image)

    # Wrap clear_predictions to verify the deferred reclassify clear runs
    # with clear_run_keys=False AND that the just-written classifier_runs
    # rows survive that clear.  We can't assert via the final post-pipeline
    # state because the reclassify stale-detection purge runs after
    # _store_grouped_predictions and FK-cascades the run keys away — but
    # that's pre-existing reclassify behavior, unrelated to the deferred
    # clear's scope.  The fix is about the clear itself, so test the clear.
    from db import Database as _Db
    original_clear = _Db.clear_predictions
    clear_calls = []

    def wrapped_clear(self, model=None, collection_photo_ids=None,
                     labels_fingerprint=None, clear_run_keys=True):
        before = self.conn.execute(
            "SELECT COUNT(*) FROM classifier_runs"
        ).fetchone()[0]
        result = original_clear(
            self, model=model,
            collection_photo_ids=collection_photo_ids,
            labels_fingerprint=labels_fingerprint,
            clear_run_keys=clear_run_keys,
        )
        after = self.conn.execute(
            "SELECT COUNT(*) FROM classifier_runs"
        ).fetchone()[0]
        clear_calls.append({
            "model": model,
            "fp": labels_fingerprint,
            "clear_run_keys": clear_run_keys,
            "runs_before": before,
            "runs_after": after,
        })
        return result

    monkeypatch.setattr(_Db, "clear_predictions", wrapped_clear)

    def fake_flush_batch(batch, clf, model_type, model_name, db_, raw_results,
                        top_k=1):
        for entry in batch:
            raw_results.append({
                "photo": entry["photo"],
                "detection_id": entry.get("detection_id"),
                "folder_path": entry["folder_path"],
                "image_path": entry["image_path"],
                "prediction": "Robin",
                "confidence": 0.9,
                "timestamp": None,
                "filename": entry["photo"]["filename"],
                "embedding": None,
                "taxonomy": None,
            })
        return 0

    monkeypatch.setattr(classify_job, "_flush_batch", fake_flush_batch)

    class FakeClassifier:
        def __init__(self, *args, **kwargs):
            pass

        def encode_image(self, *args, **kwargs):
            import numpy as np
            return np.zeros(512, dtype=np.float32)

    monkeypatch.setattr(classifier_mod, "Classifier", FakeClassifier)

    params = PipelineParams(
        collection_id=col_id,
        reclassify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()

    run_pipeline_job(job, runner, db_path, ws_id, params)

    # Find the deferred reclassify clear (the only call inside classify_stage
    # that scopes by model+fingerprint+photos).  It must have been invoked
    # with clear_run_keys=False so the per-photo record_classifier_run
    # writes survive into _store_grouped_predictions.
    deferred = [c for c in clear_calls if c["model"] == "BioCLIP"
                and c["fp"] == "legacy"]
    assert len(deferred) == 1, (
        f"Expected exactly one deferred reclassify clear; got "
        f"{len(deferred)}: {clear_calls!r}"
    )
    call = deferred[0]
    assert call["clear_run_keys"] is False, (
        f"Deferred reclassify clear must run with clear_run_keys=False so "
        f"the just-written classifier_runs survive into the next "
        f"non-reclassify pass.  Got clear_run_keys={call['clear_run_keys']!r}."
    )
    assert call["runs_before"] >= 3 and call["runs_after"] == call["runs_before"], (
        f"clear_predictions(clear_run_keys=False) must NOT touch "
        f"classifier_runs. Got runs_before={call['runs_before']}, "
        f"runs_after={call['runs_after']}."
    )


def test_pipeline_classify_cancel_does_not_raise_when_earlier_model_load_failed(
    tmp_path, monkeypatch,
):
    """If model 0 failed to load (populating skipped_model_names) and model 1
    is then cancelled mid-classify, the post-loop
    `if models_succeeded == 0 and skipped_model_names: raise` check must
    NOT fire — cancellation takes precedence over the all-models-failed
    signal. Without this guard, a user cancel gets misclassified as a
    fatal load failure (Codex P2 review).
    """
    import threading

    import classifier as classifier_mod
    import classify_job
    import config as cfg
    import pipeline_job as pj
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)
    photo_ids = []
    for i in range(3):
        name = f"photo{i}.jpg"
        pid = db.add_photo(folder_id, name, ".jpg", 2000 + i, 2_000_000.0 + i)
        _drop_jpeg(folder_path, name)
        db.save_detections(
            pid,
            [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
              "confidence": 0.9, "category": "animal"}],
            detector_model="MegaDetector",
        )
        photo_ids.append(pid)

    col_id = db.add_collection(
        "Test",
        json.dumps([{"field": "photo_ids", "value": photo_ids}]),
    )

    model_ids = _setup_two_fake_downloaded_models(tmp_path, monkeypatch)

    def fake_detect_batch(batch, folders, runner, job, reclassify, db_,
                          det_conf_threshold=None, already_detected_ids=None,
                          cached_detections=None):
        det_map = {}
        for p in batch:
            existing = db_.get_detections(p["id"])
            det_map[p["id"]] = [{
                "id": d["id"],
                "box_x": d["box_x"], "box_y": d["box_y"],
                "box_w": d["box_w"], "box_h": d["box_h"],
                "confidence": d["detector_confidence"],
                "category": d["category"],
            } for d in existing if d["detector_model"] != "full-image"]
        return det_map, len(batch), {p["id"] for p in batch}

    monkeypatch.setattr(classify_job, "_detect_batch", fake_detect_batch)

    from PIL import Image as _PILImage

    def fake_prepare_image(photo, folders, detection, vireo_dir=None):
        folder_path = folders.get(photo["folder_id"], "")
        image_path = os.path.join(folder_path, photo["filename"])
        return _PILImage.new("RGB", (16, 16), "black"), folder_path, image_path

    monkeypatch.setattr(classify_job, "_prepare_image", fake_prepare_image)

    abort_after_classify = threading.Event()

    def spy_flush_batch(batch, clf, model_type, model_name, db_, raw_results,
                        top_k=1):
        for entry in batch:
            raw_results.append({
                "photo": entry["photo"],
                "detection_id": entry.get("detection_id"),
                "folder_path": entry["folder_path"],
                "image_path": entry["image_path"],
                "prediction": "Robin",
                "confidence": 0.9,
                "timestamp": None,
                "filename": entry["photo"]["filename"],
                "embedding": None,
                "taxonomy": None,
            })
        abort_after_classify.set()
        return 0

    monkeypatch.setattr(classify_job, "_flush_batch", spy_flush_batch)
    monkeypatch.setattr(
        classify_job, "_store_grouped_predictions",
        lambda *a, **k: {"predictions_stored": 0, "burst_groups": 0,
                         "already_labeled": 0},
    )

    original_should_abort = pj._should_abort

    def patched_should_abort(event):
        if abort_after_classify.is_set():
            return True
        return original_should_abort(event)

    monkeypatch.setattr(pj, "_should_abort", patched_should_abort)

    # Make model 0 fail to load (so skipped_model_names gets populated) and
    # model 1 succeed. The pipeline tries model 0 twice — once in
    # model_loader_stage's preload, once in classify_stage's spec_idx==0
    # branch — so the first 2 Classifier() calls fail; call 3 (model 1)
    # succeeds.
    classifier_calls = [0]

    class FakeClassifier:
        def __init__(self, *args, **kwargs):
            classifier_calls[0] += 1
            if classifier_calls[0] <= 2:
                raise RuntimeError(
                    "simulated load failure for model 0"
                )

        def encode_image(self, *args, **kwargs):
            import numpy as np
            return np.zeros(512, dtype=np.float32)

    monkeypatch.setattr(classifier_mod, "Classifier", FakeClassifier)

    # Order matters: model 0 fails, model 1 must come second so it actually
    # runs classify and is the one we cancel. Pass them explicitly to lock
    # the order against any reordering inside the pipeline.
    params = PipelineParams(
        collection_id=col_id,
        model_ids=model_ids,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()

    # Pre-fix, this would raise RuntimeError("All 1 model(s) failed to load: ...")
    # because models_succeeded=0 and skipped_model_names=[model 0 name].
    # With the guard, the cancel takes precedence and the call returns cleanly.
    run_pipeline_job(job, runner, db_path, ws_id, params)

    # Sanity: the cancelled model 1 step has a 'Cancelled' summary.
    classify_step_id = f"classify:{model_ids[1]}"
    cancelled_updates = [
        kw for (_, sid, kw) in runner.step_updates
        if sid == classify_step_id and "Cancelled" in (kw.get("summary") or "")
    ]
    assert cancelled_updates, (
        f"Expected {classify_step_id!r} to finalize as 'Cancelled'; "
        f"got step_updates={runner.step_updates!r}"
    )


def test_pipeline_later_model_load_cancel_marks_model_skipped(
    tmp_path, monkeypatch,
):
    """A cancellation observed while loading model 2 must not mark that
    per-model row as a load failure.
    """
    import classifier as classifier_mod
    import classify_job
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)
    pid = db.add_photo(folder_id, "photo.jpg", ".jpg", 2000, 2_000_000.0)
    _drop_jpeg(folder_path, "photo.jpg")
    db.save_detections(
        pid,
        [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
          "confidence": 0.9, "category": "animal"}],
        detector_model="MegaDetector",
    )
    col_id = db.add_collection(
        "Test",
        json.dumps([{"field": "photo_ids", "value": [pid]}]),
    )

    model_ids = _setup_two_fake_downloaded_models(tmp_path, monkeypatch)

    def fake_detect_batch(batch, folders, runner, job, reclassify, db_,
                          det_conf_threshold=None, already_detected_ids=None,
                          cached_detections=None):
        det_map = {}
        for p in batch:
            existing = db_.get_detections(p["id"])
            det_map[p["id"]] = [{
                "id": d["id"],
                "box_x": d["box_x"], "box_y": d["box_y"],
                "box_w": d["box_w"], "box_h": d["box_h"],
                "confidence": d["detector_confidence"],
                "category": d["category"],
            } for d in existing if d["detector_model"] != "full-image"]
        return det_map, len(batch), {p["id"] for p in batch}

    monkeypatch.setattr(classify_job, "_detect_batch", fake_detect_batch)

    from PIL import Image as _PILImage

    def fake_prepare_image(photo, folders, detection, vireo_dir=None):
        folder_path = folders.get(photo["folder_id"], "")
        image_path = os.path.join(folder_path, photo["filename"])
        return _PILImage.new("RGB", (16, 16), "black"), folder_path, image_path

    monkeypatch.setattr(classify_job, "_prepare_image", fake_prepare_image)

    class FakeClassifier:
        def __init__(self, *args, **kwargs):
            pass

        def encode_image(self, *args, **kwargs):
            import numpy as np
            return np.zeros(512, dtype=np.float32)

    monkeypatch.setattr(classifier_mod, "Classifier", FakeClassifier)

    runner = FakeRunner()
    job = _make_job()

    def cancelling_flush(batch, clf, model_type, model_name, db_, raw_results,
                         top_k=1):
        for entry in batch:
            raw_results.append({
                "photo": entry["photo"],
                "detection_id": entry.get("detection_id"),
                "folder_path": entry["folder_path"],
                "image_path": entry["image_path"],
                "prediction": "Robin",
                "confidence": 0.9,
                "timestamp": None,
                "filename": entry["photo"]["filename"],
                "embedding": None,
                "taxonomy": None,
            })
        runner.cancelled_ids.add(job["id"])
        return 0

    monkeypatch.setattr(classify_job, "_flush_batch", cancelling_flush)
    monkeypatch.setattr(
        classify_job, "_store_grouped_predictions",
        lambda *a, **k: {"predictions_stored": 0, "burst_groups": 0,
                         "already_labeled": 0},
    )

    run_pipeline_job(
        job,
        runner,
        db_path,
        ws_id,
        PipelineParams(
            collection_id=col_id,
            model_ids=model_ids,
            skip_extract_masks=True,
            skip_regroup=True,
        ),
    )

    later_step_id = f"classify:{model_ids[1]}"
    later_updates = [
        kw for _, sid, kw in runner.step_updates if sid == later_step_id
    ]
    assert any(
        kw.get("summary") == "Skipped (cancelled)"
        and kw.get("status") == "completed"
        for kw in later_updates
    ), later_updates
    assert not any(kw.get("status") == "failed" for kw in later_updates)


# ---------------------------------------------------------------------------
# Sentinel written on ONNX load failure
# ---------------------------------------------------------------------------


def test_onnx_load_failure_writes_verify_failed_sentinel(tmp_path, monkeypatch):
    """When ONNXRuntime fails with a missing-external-data error, the
    .verify_failed sentinel must be written so that _classify_model_state
    reports 'incomplete' and the Settings UI shows a Repair button.

    This is the fix for the bug where the pipeline tells the user
    "Open Settings -> Models and click Repair" but Settings shows the
    model as healthy because no sentinel was written.
    """
    import classifier as classifier_mod
    import config as cfg
    import model_verify
    import models
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    col_id = db.add_collection("Test", "[]")

    model_id = _setup_fake_downloaded_model(tmp_path, monkeypatch)
    model_dir = tmp_path / "models" / model_id

    # Simulate ONNXRuntime raising a missing-external-data error.
    def boom(*args, **kwargs):
        raise RuntimeError(
            "[ONNXRuntimeError] model_path must not be empty. Ensure that "
            "a path is provided when the model is created or loaded."
        )

    monkeypatch.setattr(classifier_mod, "Classifier", boom)
    # Force hash check to report the files as bad, so the ONNX failure
    # handler commits the sentinel write (the "real corruption" path).
    monkeypatch.setattr(
        model_verify,
        "verify_model",
        lambda *a, **k: model_verify.VerifyResult(
            ok=False, mismatches=["image_encoder.onnx.data"]
        ),
    )

    params = PipelineParams(
        collection_id=col_id,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()

    import pytest
    with pytest.raises(RuntimeError):
        run_pipeline_job(job, runner, db_path, ws_id, params)

    # 1. The .verify_failed sentinel must have been written.
    sentinel = model_dir / model_verify.VERIFY_FAILED_SENTINEL
    assert sentinel.exists(), (
        ".verify_failed sentinel must be written when ONNXRuntime fails "
        "with a missing-external-data error, otherwise Settings shows the "
        "model as healthy and no Repair button appears."
    )
    assert "onnx-load-failure" in sentinel.read_text()

    # 2. After the sentinel is written, _classify_model_state must return
    #    'incomplete' so the Settings UI surfaces the Repair button.
    known = [m for m in models.KNOWN_MODELS if m["id"] == model_id]
    assert known, f"Expected to find {model_id} in KNOWN_MODELS"
    files = known[0].get("files", [])
    state = models._classify_model_state(str(model_dir), files)
    assert state == "incomplete", (
        f"_classify_model_state should return 'incomplete' after the "
        f"sentinel is written, but got '{state}'"
    )


def test_onnx_load_failure_skips_sentinel_when_files_verify_ok(
    tmp_path, monkeypatch
):
    """If ONNX Runtime fails to load but SHA256 verification reports the
    files are intact, the .verify_failed sentinel must NOT be written.

    This is the guard against a transient ONNXRuntime hiccup (memory
    pressure, mmap race, a pytest monkeypatch from a worktree running
    against the same $HOME) permanently marking a healthy model as
    'Incomplete — repair available'. Left unchecked, every subsequent
    pipeline run told the user to click Repair, and Repair succeeded
    but the sentinel came back on the next transient failure.
    """
    import classifier as classifier_mod
    import config as cfg
    import model_verify
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    col_id = db.add_collection("Test", "[]")

    model_id = _setup_fake_downloaded_model(tmp_path, monkeypatch)
    model_dir = tmp_path / "models" / model_id

    def boom(*args, **kwargs):
        raise RuntimeError(
            "[ONNXRuntimeError] model_path must not be empty. Ensure that "
            "a path is provided when the model is created or loaded."
        )

    monkeypatch.setattr(classifier_mod, "Classifier", boom)
    # Files hash-check clean — the ONNX error is transient, not corruption.
    monkeypatch.setattr(
        model_verify,
        "verify_model",
        lambda *a, **k: model_verify.VerifyResult(ok=True),
    )

    params = PipelineParams(
        collection_id=col_id,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()

    import pytest
    with pytest.raises(RuntimeError) as exc:
        run_pipeline_job(job, runner, db_path, ws_id, params)

    sentinel = model_dir / model_verify.VERIFY_FAILED_SENTINEL
    assert not sentinel.exists(), (
        "verified-clean files must not get a .verify_failed sentinel "
        "from a transient ONNXRuntime error — that's what traps users "
        "in the 'Repair never sticks' loop."
    )
    # And the user-facing error should NOT say "click Repair" since the
    # files are fine; it should suggest a retry instead.
    assert "Repair" not in str(exc.value), (
        f"Expected transient-failure message, got: {exc.value}"
    )


# ---------------------------------------------------------------------------
# Multi-model pipeline resilience to individual model failures
# ---------------------------------------------------------------------------


def test_pipeline_continues_when_first_model_fails(tmp_path, monkeypatch):
    """When the first model in a multi-model run fails to load, the pipeline
    must NOT abort.  The second model should still classify photos and the
    pipeline should complete successfully.

    This is the fix for the multi-model pipeline abort bug: previously
    model_loader_stage set abort on ANY preload failure, which killed the
    entire pipeline even when other models were available.
    """
    import classifier as classifier_mod
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    col_id = db.add_collection("Test", "[]")

    model_ids = _setup_two_fake_downloaded_models(tmp_path, monkeypatch)

    # The first model ("bioclip-vit-b-16") always fails; the second
    # ("bioclip-2") always succeeds. Use the pretrained_str kwarg to
    # distinguish which model is being loaded.
    construction_calls = []

    class FakeClassifier:
        def __init__(self, *args, **kwargs):
            pretrained = kwargs.get("pretrained_str", "") or ""
            if "bioclip-vit-b-16" in pretrained:
                raise RuntimeError("simulated first model failure")
            construction_calls.append(kwargs)

        def encode_image(self, *args, **kwargs):
            import numpy as np
            return np.zeros(512, dtype=np.float32)

    monkeypatch.setattr(classifier_mod, "Classifier", FakeClassifier)

    params = PipelineParams(
        collection_id=col_id,
        model_ids=model_ids,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()

    # Should NOT raise — second model should succeed.
    result = run_pipeline_job(job, runner, db_path, ws_id, params)

    # The second model must have been constructed successfully.
    assert len(construction_calls) >= 1, (
        "Expected at least one successful Classifier construction (second model), "
        f"got {len(construction_calls)}"
    )

    # model_loader summary should note the preload failure.
    model_loader_summaries = [
        kwargs.get("summary", "")
        for (_, step_id, kwargs) in runner.step_updates
        if step_id == "model_loader" and kwargs.get("status") == "completed"
    ]
    assert model_loader_summaries, "model_loader should complete (not fail)"
    assert "failed to preload" in " ".join(model_loader_summaries)

    # The failing model's per-model classify row should be 'failed'; the
    # surviving model's row should be 'completed'.
    bad_id, good_id = model_ids
    bad_statuses = [
        kwargs.get("status")
        for (_, step_id, kwargs) in runner.step_updates
        if step_id == f"classify:{bad_id}" and "status" in kwargs
    ]
    assert "failed" in bad_statuses, (
        f"Failing model's row should be marked failed, got {bad_statuses}"
    )
    good_statuses = [
        kwargs.get("status")
        for (_, step_id, kwargs) in runner.step_updates
        if step_id == f"classify:{good_id}" and "status" in kwargs
    ]
    assert "completed" in good_statuses, (
        f"Surviving model's row should be completed, got {good_statuses}"
    )

    # The returned result must record the skipped model info.
    assert isinstance(result, dict)
    classify_result = result.get("stages", {}).get("classify", {})
    assert classify_result.get("models_skipped", 0) >= 1
    assert classify_result.get("models_succeeded", 0) >= 1


def test_pipeline_continues_when_secondary_model_fails(tmp_path, monkeypatch):
    """When the second model in a multi-model run fails to load, the first
    model's results are kept and the pipeline completes with a partial success.
    """
    import classifier as classifier_mod
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    col_id = db.add_collection("Test", "[]")

    model_ids = _setup_two_fake_downloaded_models(tmp_path, monkeypatch)

    # The second model ("bioclip-2") always fails; the first succeeds.
    construction_calls = []

    class FakeClassifier:
        def __init__(self, *args, **kwargs):
            pretrained = kwargs.get("pretrained_str", "") or ""
            if "bioclip-2" in pretrained:
                raise RuntimeError("simulated second model failure")
            construction_calls.append(kwargs)

        def encode_image(self, *args, **kwargs):
            import numpy as np
            return np.zeros(512, dtype=np.float32)

    monkeypatch.setattr(classifier_mod, "Classifier", FakeClassifier)

    params = PipelineParams(
        collection_id=col_id,
        model_ids=model_ids,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()

    # Should NOT raise — first model succeeded.
    result = run_pipeline_job(job, runner, db_path, ws_id, params)

    # The first model must have been constructed.
    assert len(construction_calls) >= 1, (
        f"Expected at least 1 construction call, got {len(construction_calls)}"
    )

    good_id, bad_id = model_ids
    good_statuses = [
        kwargs.get("status")
        for (_, step_id, kwargs) in runner.step_updates
        if step_id == f"classify:{good_id}" and "status" in kwargs
    ]
    assert "completed" in good_statuses, (
        f"First (good) model row should be completed, got {good_statuses}"
    )
    bad_statuses = [
        kwargs.get("status")
        for (_, step_id, kwargs) in runner.step_updates
        if step_id == f"classify:{bad_id}" and "status" in kwargs
    ]
    assert "failed" in bad_statuses, (
        f"Second (bad) model row should be failed, got {bad_statuses}"
    )

    assert isinstance(result, dict)
    classify_result = result.get("stages", {}).get("classify", {})
    assert classify_result.get("models_skipped", 0) >= 1
    assert classify_result.get("models_succeeded", 0) >= 1


def test_pipeline_single_model_still_aborts_on_failure(tmp_path, monkeypatch):
    """When there is only one model and it fails to load, the pipeline must
    still abort — the resilience logic should NOT swallow single-model errors.
    This preserves the existing behavior tested by
    test_pipeline_raises_when_stage_fails.
    """
    import classifier as classifier_mod
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    col_id = db.add_collection("Test", "[]")

    _setup_fake_downloaded_model(tmp_path, monkeypatch)

    def boom(*args, **kwargs):
        raise RuntimeError("simulated single model failure")

    monkeypatch.setattr(classifier_mod, "Classifier", boom)

    params = PipelineParams(
        collection_id=col_id,
        model_ids=["bioclip-vit-b-16"],
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()

    import pytest
    with pytest.raises(RuntimeError):
        run_pipeline_job(job, runner, db_path, ws_id, params)

    # model_loader should be marked as failed.
    model_loader_failures = [
        kwargs
        for (_, step_id, kwargs) in runner.step_updates
        if step_id == "model_loader" and kwargs.get("status") == "failed"
    ]
    assert model_loader_failures, (
        "Single-model pipeline must mark model_loader as failed"
    )
    assert isinstance(job["result"], dict)
    assert any(
        "model_loader" in e for e in job["result"]["errors"]
    ), f"Expected a model_loader error, got: {job['result']['errors']}"


def test_pipeline_classify_stores_predictions_with_detection_id(
    tmp_path, monkeypatch
):
    """Predictions written by the pipeline classify stage MUST carry a valid
    detection_id. Without it, predictions are orphaned — the workspace-scoped
    skip query (get_existing_prediction_photo_ids) inner-joins on
    detection_id, so every subsequent run re-classifies the same photos
    instead of reusing the stored predictions.

    Regression: pipeline_job built img_batch entries without a detection_id
    key, so _flush_batch stored detection_id=None for every pipeline-written
    prediction.
    """
    import classifier as classifier_mod
    import classify_job
    import config as cfg
    from db import Database
    from PIL import Image

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)
    photo_with_det = db.add_photo(
        folder_id, "hawk.jpg", ".jpg", 12345, 1_000_000.0
    )
    photo_without_det = db.add_photo(
        folder_id, "empty.jpg", ".jpg", 12346, 1_000_100.0
    )
    _drop_jpeg(folder_path, "hawk.jpg")
    _drop_jpeg(folder_path, "empty.jpg")
    col_id = db.add_collection(
        "Test",
        json.dumps([
            {"field": "photo_ids", "value": [photo_with_det, photo_without_det]}
        ]),
    )

    model_id = _setup_fake_downloaded_model(tmp_path, monkeypatch)

    # Persist a real detection row for photo_with_det so det_map carries a
    # valid DB id the pipeline can bind predictions to.
    real_det_ids = db.save_detections(
        photo_with_det,
        [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
          "confidence": 0.95, "category": "animal"}],
        detector_model="MegaDetector",
    )
    real_det_id = real_det_ids[0]

    def fake_detect_batch(batch, folders, runner, job, reclassify, db_,
                          det_conf_threshold=None, already_detected_ids=None,
                          cached_detections=None):
        det_map = {}
        for p in batch:
            if p["id"] == photo_with_det:
                det_map[p["id"]] = [{
                    "id": real_det_id,
                    "box_x": 0.1, "box_y": 0.1, "box_w": 0.5, "box_h": 0.5,
                    "confidence": 0.95, "category": "animal",
                }]
        return det_map, len(det_map), {p["id"] for p in batch}

    monkeypatch.setattr(classify_job, "_detect_batch", fake_detect_batch)

    def fake_prepare_image(photo, folders, detection, vireo_dir=None):
        return (
            Image.new("RGB", (32, 32), "white"),
            folders.get(photo["folder_id"], ""),
            os.path.join(folders.get(photo["folder_id"], ""), photo["filename"]),
        )

    monkeypatch.setattr(classify_job, "_prepare_image", fake_prepare_image)

    class FakeClassifier:
        def __init__(self, *args, **kwargs):
            pass

        def classify_batch_with_embedding(self, images, threshold=0):
            import numpy as np
            emb = np.zeros(512, dtype=np.float32)
            return [
                ([{"species": "Red-tailed Hawk", "score": 0.99}], emb)
                for _ in images
            ]

    monkeypatch.setattr(classifier_mod, "Classifier", FakeClassifier)

    params = PipelineParams(
        collection_id=col_id,
        model_ids=[model_id],
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()
    run_pipeline_job(job, runner, db_path, ws_id, params)

    # At least one prediction should have been stored.
    preds = db.conn.execute(
        "SELECT id, detection_id, species, classifier_model AS model FROM predictions"
    ).fetchall()
    assert preds, (
        "Pipeline classify stage produced no predictions — test setup did "
        "not exercise the write path."
    )

    # None of those predictions should have a NULL detection_id.
    orphans = [dict(p) for p in preds if p["detection_id"] is None]
    assert not orphans, (
        f"Pipeline wrote predictions with NULL detection_id: {orphans}. "
        "get_existing_prediction_photo_ids filters these out, so the skip "
        "logic never matches and every pipeline run re-classifies everything."
    )

    # Every prediction's detection_id must resolve to a real detection row.
    # Detections are global post-refactor — workspace scoping is through
    # workspace_folders, so we verify that join resolves instead of reading
    # the dropped workspace_id column on detections.
    for p in preds:
        det = db.conn.execute(
            """SELECT d.id, d.photo_id, wf.workspace_id
               FROM detections d
               JOIN photos ph ON ph.id = d.photo_id
               JOIN workspace_folders wf
                 ON wf.folder_id = ph.folder_id
               WHERE d.id = ? AND wf.workspace_id = ?""",
            (p["detection_id"], ws_id),
        ).fetchone()
        assert det is not None, (
            f"Prediction {dict(p)} references detection_id "
            f"{p['detection_id']} which doesn't resolve to a detection in "
            f"workspace {ws_id} via workspace_folders."
        )

    # photo_with_det must be in the skip set so the second run reuses its
    # prediction instead of re-classifying.
    skip_set = db.get_existing_prediction_photo_ids(preds[0]["model"])
    assert photo_with_det in skip_set, (
        f"photo_with_det ({photo_with_det}) missing from skip set {skip_set} — "
        "its prediction is not reachable via the predictions→detections join."
    )

    # photo_without_det should now have a full-image prediction anchored to a
    # synthetic full-image detection. That preserves the non-NULL detection_id
    # invariant while still letting no-detection photos be classified and cached.
    no_det_preds = db.conn.execute(
        """SELECT pr.id, d.detector_model
             FROM predictions pr
             JOIN detections d ON d.id = pr.detection_id
            WHERE d.photo_id = ?""",
        (photo_without_det,),
    ).fetchall()
    assert [r["detector_model"] for r in no_det_preds] == ["full-image"], (
        "No-detection photos should be classified against a synthetic "
        f"full-image anchor, got: {[dict(r) for r in no_det_preds]}"
    )
    leftover_dets = db.conn.execute(
        "SELECT id, detector_model FROM detections WHERE photo_id = ?",
        (photo_without_det,),
    ).fetchall()
    assert [r["detector_model"] for r in leftover_dets] == ["full-image"], (
        "No-detection photos should get exactly one synthetic full-image "
        f"detection anchor, got: {[dict(r) for r in leftover_dets]}"
    )


def test_extract_masks_stage_ignores_synthetic_full_image_detections(
    tmp_path, monkeypatch
):
    """extract_masks_stage must treat detector_model='full-image' rows as
    non-detections: they are classify-anchor rows (created by the
    standalone classify path in classify_job.py for photos where
    MegaDetector found nothing), not real subject boxes. Counting them
    toward photos_with_detections hides the "weights missing / no
    detections" diagnostic and drives mask extraction on useless full-frame
    boxes.

    The pipeline classify stage no longer creates synthetic full-image
    detections — but classify_job.py still does, and this filter is the
    last line of defense regardless of the source.
    """
    import classifier as classifier_mod
    import classify_job
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)
    photo_id = db.add_photo(folder_id, "empty.jpg", ".jpg", 12345, 1_000_000.0)
    _drop_jpeg(folder_path, "empty.jpg")
    col_id = db.add_collection(
        "Test",
        json.dumps([{"field": "photo_ids", "value": [photo_id]}]),
    )

    # Simulate a prior standalone classify run that inserted a full-image
    # detection anchor for this photo.
    db.save_detections(
        photo_id,
        [{"box": {"x": 0, "y": 0, "w": 1, "h": 1},
          "confidence": 0, "category": "animal"}],
        detector_model="full-image",
    )

    model_id = _setup_fake_downloaded_model(tmp_path, monkeypatch)

    # No real MegaDetector hits in this pipeline pass, either.
    def fake_detect_batch(batch, folders, runner, job, reclassify, db_,
                          det_conf_threshold=None, already_detected_ids=None,
                          cached_detections=None):
        return {}, 0, {p["id"] for p in batch}

    monkeypatch.setattr(classify_job, "_detect_batch", fake_detect_batch)

    class FakeClassifier:
        def __init__(self, *args, **kwargs):
            pass

        def classify_batch_with_embedding(self, images, threshold=0):
            import numpy as np
            emb = np.zeros(512, dtype=np.float32)
            return [
                ([{"species": "Unknown", "score": 0.5}], emb)
                for _ in images
            ]

    monkeypatch.setattr(classifier_mod, "Classifier", FakeClassifier)

    params = PipelineParams(
        collection_id=col_id,
        model_ids=[model_id],
        skip_extract_masks=False,  # exercise extract_masks_stage
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()
    run_pipeline_job(job, runner, db_path, ws_id, params)

    # Confirm the pre-existing full-image detection is still the only row
    # for this photo — the pipeline run did not create additional rows.
    all_dets = db.conn.execute(
        "SELECT detector_model FROM detections WHERE photo_id = ?",
        (photo_id,),
    ).fetchall()
    assert all_dets and all(
        d["detector_model"] == "full-image" for d in all_dets
    ), (
        f"Expected only full-image anchor detections, got: "
        f"{[dict(d) for d in all_dets]}"
    )

    # extract_masks_stage should have reported the no-detections diagnostic
    # rather than silently completing with masked=0 masked photos.
    extract_summaries = [
        kwargs.get("summary", "")
        for (_, step_id, kwargs) in runner.step_updates
        if step_id == "extract_masks" and kwargs.get("status") in (
            "completed", "failed", "skipped",
        )
    ]
    joined = " ".join(extract_summaries).lower()
    assert "no detections" in joined or "megadetector" in joined, (
        f"extract_masks_stage should surface the no-detections diagnostic "
        f"when every detection row is synthetic, got summaries: "
        f"{extract_summaries}"
    )


def test_extract_masks_stage_warns_when_all_detections_below_threshold(
    tmp_path, monkeypatch
):
    """If a photo has detections but every one is below detector_confidence,
    extract_masks silently completes with masked=0 and the user has no way to
    discover why their unmasked photos were skipped — get_detections returns
    [] at the workspace threshold, so the photo never enters photo_det_map.

    Regression observed in production: 727 of 5054 photos had only
    sub-threshold detections, so extract_masks finished in 0.5s with
    "0 masked, 0 skipped" and no error. Surface a clear diagnostic that
    names the threshold and points at the workaround.
    """
    import classifier as classifier_mod
    import classify_job
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)
    photo_id = db.add_photo(folder_id, "lowconf.jpg", ".jpg", 12345, 1_000_000.0)
    _drop_jpeg(folder_path, "lowconf.jpg")
    col_id = db.add_collection(
        "Test",
        json.dumps([{"field": "photo_ids", "value": [photo_id]}]),
    )

    # Real MegaDetector hit, but at confidence 0.05 — well below the default
    # 0.2 detector_confidence threshold. The detection row is real, but
    # get_detections() filters it out at read time.
    db.save_detections(
        photo_id,
        [{"box": {"x": 0, "y": 0, "w": 100, "h": 100},
          "confidence": 0.05, "category": "animal"}],
        detector_model="megadetector-v6",
    )
    # Mark the photo as already detected so the detect stage reuses the
    # cached row instead of re-running MegaDetector against the stub jpeg.
    db.record_detector_run(photo_id, "megadetector-v6", box_count=1)

    model_id = _setup_fake_downloaded_model(tmp_path, monkeypatch)

    def fake_detect_batch(batch, folders, runner, job, reclassify, db_,
                          det_conf_threshold=None, already_detected_ids=None,
                          cached_detections=None):
        return {}, 0, {p["id"] for p in batch}

    monkeypatch.setattr(classify_job, "_detect_batch", fake_detect_batch)

    class FakeClassifier:
        def __init__(self, *args, **kwargs):
            pass

        def classify_batch_with_embedding(self, images, threshold=0):
            import numpy as np
            emb = np.zeros(512, dtype=np.float32)
            return [
                ([{"species": "Unknown", "score": 0.5}], emb)
                for _ in images
            ]

    monkeypatch.setattr(classifier_mod, "Classifier", FakeClassifier)

    params = PipelineParams(
        collection_id=col_id,
        model_ids=[model_id],
        skip_extract_masks=False,  # exercise extract_masks_stage
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()
    run_pipeline_job(job, runner, db_path, ws_id, params)

    extract_summaries = [
        kwargs.get("summary", "")
        for (_, step_id, kwargs) in runner.step_updates
        if step_id == "extract_masks" and kwargs.get("status") in (
            "completed", "failed", "skipped",
        )
    ]
    joined = " ".join(extract_summaries).lower()
    assert "below" in joined and (
        "threshold" in joined or "detector_confidence" in joined
    ), (
        f"extract_masks_stage should explain why nothing was masked when all "
        f"detections are sub-threshold; got summaries: {extract_summaries}"
    )


def test_extract_masks_stage_warns_on_mixed_already_masked_and_subthreshold(
    tmp_path, monkeypatch
):
    """Production hit: 4166/5054 photos already had masks (photos_with_detections
    > 0), and the remaining 727 had only sub-threshold detections. The
    existing "no detections" guard requires photos_with_detections == 0, so
    it didn't fire — extract_masks completed silently with "0 masked, 0
    skipped" while 727 unmasked photos sat untouched. The mixed-state guard
    must fire instead.
    """
    import classifier as classifier_mod
    import classify_job
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")
    _stub_extract_masks_heavy_ops(monkeypatch)

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)

    # Photo A: already has a mask AND a qualifying detection — drives
    # photos_with_detections > 0 so the existing no-detections guard does
    # NOT fire.
    masked_id = db.add_photo(folder_id, "masked.jpg", ".jpg", 12345, 1_000_000.0)
    _drop_jpeg(folder_path, "masked.jpg")
    db.save_detections(
        masked_id,
        [{"box": {"x": 0, "y": 0, "w": 100, "h": 100},
          "confidence": 0.9, "category": "animal"}],
        detector_model="megadetector-v6",
    )
    db.record_detector_run(masked_id, "megadetector-v6", box_count=1)
    db.update_photo_pipeline_features(
        masked_id, mask_path=str(tmp_path / "mask_a.png"),
    )

    # Photo B: no mask, only sub-threshold detection → should be flagged.
    lowconf_id = db.add_photo(folder_id, "lowconf.jpg", ".jpg", 23456, 1_000_001.0)
    _drop_jpeg(folder_path, "lowconf.jpg")
    db.save_detections(
        lowconf_id,
        [{"box": {"x": 0, "y": 0, "w": 50, "h": 50},
          "confidence": 0.05, "category": "animal"}],
        detector_model="megadetector-v6",
    )
    db.record_detector_run(lowconf_id, "megadetector-v6", box_count=1)

    col_id = db.add_collection(
        "Test",
        json.dumps([{"field": "photo_ids", "value": [masked_id, lowconf_id]}]),
    )

    model_id = _setup_fake_downloaded_model(tmp_path, monkeypatch)

    def fake_detect_batch(batch, folders, runner, job, reclassify, db_,
                          det_conf_threshold=None, already_detected_ids=None,
                          cached_detections=None):
        return {}, 0, {p["id"] for p in batch}

    monkeypatch.setattr(classify_job, "_detect_batch", fake_detect_batch)

    class FakeClassifier:
        def __init__(self, *args, **kwargs):
            pass

        def classify_batch_with_embedding(self, images, threshold=0):
            import numpy as np
            emb = np.zeros(512, dtype=np.float32)
            return [
                ([{"species": "Unknown", "score": 0.5}], emb)
                for _ in images
            ]

    monkeypatch.setattr(classifier_mod, "Classifier", FakeClassifier)

    params = PipelineParams(
        collection_id=col_id,
        model_ids=[model_id],
        skip_extract_masks=False,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()
    run_pipeline_job(job, runner, db_path, ws_id, params)

    extract_summaries = [
        kwargs.get("summary", "")
        for (_, step_id, kwargs) in runner.step_updates
        if step_id == "extract_masks" and kwargs.get("status") in (
            "completed", "failed", "skipped",
        )
    ]
    joined = " ".join(extract_summaries).lower()
    assert "below" in joined and (
        "threshold" in joined or "detector_confidence" in joined
    ), (
        f"extract_masks_stage should flag the sub-threshold photo even when "
        f"another photo already has a mask; got summaries: {extract_summaries}"
    )


def test_pipeline_rerun_with_existing_prediction_and_bursts_does_not_crash(
    tmp_path, monkeypatch
):
    """Regression: after the detection_id fix made pipeline predictions
    eligible for get_existing_prediction_photo_ids, a second non-reclassify
    run routes skipped photos through the _existing raw_results branch. If
    those raw_results lack a detection_id key, _store_grouped_predictions
    crashes the first time burst grouping kicks in — it calls
    update_prediction_group_info(detection_id=item["detection_id"], ...)
    for every _existing item in a multi-item group.

    This test runs two photos through a pipeline pass that groups them into
    a single burst, then runs the pipeline again: the second run must
    complete without a KeyError on detection_id.
    """
    import classifier as classifier_mod
    import classify_job
    import config as cfg
    from db import Database
    from PIL import Image

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)
    # Two photos one second apart — burst grouping will fuse them.
    p1 = db.add_photo(folder_id, "a.jpg", ".jpg", 1, 1_000_000.0,
                      timestamp="2026-01-01T12:00:00")
    p2 = db.add_photo(folder_id, "b.jpg", ".jpg", 2, 1_000_001.0,
                      timestamp="2026-01-01T12:00:01")
    _drop_jpeg(folder_path, "a.jpg")
    _drop_jpeg(folder_path, "b.jpg")
    col_id = db.add_collection(
        "Test",
        json.dumps([{"field": "photo_ids", "value": [p1, p2]}]),
    )

    det_p1 = db.save_detections(
        p1, [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
              "confidence": 0.9, "category": "animal"}],
        detector_model="MegaDetector",
    )[0]
    det_p2 = db.save_detections(
        p2, [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
              "confidence": 0.9, "category": "animal"}],
        detector_model="MegaDetector",
    )[0]

    model_id = _setup_fake_downloaded_model(tmp_path, monkeypatch)

    det_rows = {
        p1: [{"id": det_p1, "box_x": 0.1, "box_y": 0.1,
              "box_w": 0.5, "box_h": 0.5, "confidence": 0.9,
              "category": "animal"}],
        p2: [{"id": det_p2, "box_x": 0.1, "box_y": 0.1,
              "box_w": 0.5, "box_h": 0.5, "confidence": 0.9,
              "category": "animal"}],
    }

    def fake_detect_batch(batch, folders, runner, job, reclassify, db_,
                          det_conf_threshold=None, already_detected_ids=None,
                          cached_detections=None):
        det_map = {p["id"]: det_rows[p["id"]] for p in batch}
        return det_map, len(det_map), {p["id"] for p in batch}

    monkeypatch.setattr(classify_job, "_detect_batch", fake_detect_batch)

    def fake_prepare_image(photo, folders, detection, vireo_dir=None):
        return (
            Image.new("RGB", (32, 32), "white"),
            folders.get(photo["folder_id"], ""),
            os.path.join(folders.get(photo["folder_id"], ""), photo["filename"]),
        )

    monkeypatch.setattr(classify_job, "_prepare_image", fake_prepare_image)

    class FakeClassifier:
        def __init__(self, *args, **kwargs):
            pass

        def classify_batch_with_embedding(self, images, threshold=0):
            import numpy as np
            emb = np.zeros(512, dtype=np.float32)
            return [
                ([{"species": "Red-tailed Hawk", "score": 0.99}], emb)
                for _ in images
            ]

    monkeypatch.setattr(classifier_mod, "Classifier", FakeClassifier)

    params = PipelineParams(
        collection_id=col_id,
        model_ids=[model_id],
        skip_extract_masks=True,
        skip_regroup=True,
    )

    # First run: stores predictions.
    run_pipeline_job(_make_job(), FakeRunner(), db_path, ws_id, params)

    preds_after_first = db.conn.execute(
        "SELECT COUNT(*) AS c FROM predictions WHERE detection_id IS NOT NULL"
    ).fetchone()["c"]
    assert preds_after_first >= 2, (
        f"First run should have stored predictions for both photos, got "
        f"{preds_after_first} prediction rows"
    )

    # Second run: every photo hits the _existing branch, and burst grouping
    # calls _store_grouped_predictions with multi-item groups of _existing
    # items. Before the fix, this raised KeyError: 'detection_id'.
    job2 = _make_job()
    run_pipeline_job(job2, FakeRunner(), db_path, ws_id, params)

    assert not job2["errors"], (
        f"Second pipeline run raised errors: {job2['errors']}. "
        "The _existing raw_results dict must carry a detection_id so "
        "_store_grouped_predictions does not crash on bursts of reused "
        "predictions."
    )
    assert job2["status"] != "failed", (
        f"Second pipeline run status is {job2['status']} (expected not "
        "'failed')"
    )


def test_pipeline_step_defs_include_detect_and_per_model_classify(
    tmp_path, monkeypatch
):
    """With multiple models, step_defs should contain one 'detect' row and
    one 'classify:<model_id>' row per model. The detect row must come before
    every classify row so users see detection progress as its own phase."""
    import classifier as classifier_mod
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    col_id = db.add_collection("Test", "[]")

    model_ids = _setup_two_fake_downloaded_models(tmp_path, monkeypatch)

    class FakeClassifier:
        def __init__(self, *args, **kwargs):
            pass

        def encode_image(self, *args, **kwargs):
            import numpy as np
            return np.zeros(512, dtype=np.float32)

        def classify_with_embedding(self, img, threshold=0):
            import numpy as np
            return [{"species": "Robin", "score": 0.9}], np.zeros(
                512, dtype=np.float32,
            )

        def classify_batch_with_embedding(self, images, threshold=0):
            import numpy as np
            zero = np.zeros(512, dtype=np.float32)
            return [(
                [{"species": "Robin", "score": 0.9}],
                zero,
            ) for _ in images]

    monkeypatch.setattr(classifier_mod, "Classifier", FakeClassifier)

    params = PipelineParams(
        collection_id=col_id,
        model_ids=model_ids,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()
    run_pipeline_job(job, runner, db_path, ws_id, params)

    step_ids = [s["id"] for s in runner.steps_defined]

    assert "detect" in step_ids, (
        f"Expected a standalone 'detect' step in step_defs, got {step_ids}"
    )
    per_model_ids = [
        sid for sid in step_ids if sid.startswith("classify:")
    ]
    assert len(per_model_ids) == len(model_ids), (
        f"Expected one 'classify:<model_id>' step per model (got "
        f"{per_model_ids} for models {model_ids})"
    )
    for mid in model_ids:
        assert f"classify:{mid}" in step_ids, (
            f"Missing classify step for model {mid!r}: {step_ids}"
        )
    # Legacy single 'classify' row must not coexist with per-model rows.
    assert "classify" not in step_ids, (
        f"Legacy 'classify' step should be replaced by per-model rows: {step_ids}"
    )

    detect_idx = step_ids.index("detect")
    for pid in per_model_ids:
        assert step_ids.index(pid) > detect_idx, (
            f"'detect' step must come before classify rows (detect={detect_idx}, "
            f"{pid}={step_ids.index(pid)})"
        )


def test_pipeline_step_defs_cover_every_requested_id_on_partial_resolution(
    tmp_path, monkeypatch
):
    """When only a prefix of requested model ids resolves (e.g. a later id
    isn't downloaded), step_defs must still emit one 'classify:<mid>' row per
    REQUESTED id. Driving row creation off a partial resolved_specs hides the
    later failed ids — their later 'failed' update_step calls then no-op
    silently and the user can't see which model broke.

    Regression test for Codex P2 on PR #566 (step_defs at line 203).
    """
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    col_id = db.add_collection("Test", "[]")

    # Install the first model as downloaded; second is requested but not
    # downloaded, so resolution raises partway through.
    import models
    monkeypatch.setattr(models, "CONFIG_PATH", str(tmp_path / "models.json"))
    monkeypatch.setattr(models, "DEFAULT_MODELS_DIR", str(tmp_path / "models"))
    _write_fake_model_files(tmp_path / "models" / "bioclip-vit-b-16")
    # "bioclip-2" deliberately NOT installed.
    models.set_active_model("bioclip-vit-b-16")

    params = PipelineParams(
        collection_id=col_id,
        model_ids=["bioclip-vit-b-16", "bioclip-2"],
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()
    # Resolution failure propagates as a model_loader stage failure, so the
    # pipeline raises. We only care about what was registered in step_defs.
    with contextlib.suppress(RuntimeError):
        run_pipeline_job(job, runner, db_path, ws_id, params)

    step_ids = [s["id"] for s in runner.steps_defined]
    assert "classify:bioclip-vit-b-16" in step_ids, (
        f"Resolved id should have its own row, got {step_ids}"
    )
    assert "classify:bioclip-2" in step_ids, (
        f"Unresolved-but-requested id must still have a row so its failure "
        f"is visible to the user, got {step_ids}"
    )


def test_pipeline_single_model_gets_per_model_classify_row(tmp_path, monkeypatch):
    """Even a single-model run uses one 'classify:<model_id>' row — labeled
    with the model's display name — for consistency with multi-model runs."""
    import classifier as classifier_mod
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    col_id = db.add_collection("Test", "[]")

    _setup_fake_downloaded_model(tmp_path, monkeypatch)

    class FakeClassifier:
        def __init__(self, *args, **kwargs):
            pass

        def encode_image(self, *args, **kwargs):
            import numpy as np
            return np.zeros(512, dtype=np.float32)

        def classify_with_embedding(self, img, threshold=0):
            import numpy as np
            return [{"species": "Robin", "score": 0.9}], np.zeros(
                512, dtype=np.float32,
            )

        def classify_batch_with_embedding(self, images, threshold=0):
            import numpy as np
            zero = np.zeros(512, dtype=np.float32)
            return [(
                [{"species": "Robin", "score": 0.9}],
                zero,
            ) for _ in images]

    monkeypatch.setattr(classifier_mod, "Classifier", FakeClassifier)

    params = PipelineParams(
        collection_id=col_id,
        model_id="bioclip-vit-b-16",
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()
    run_pipeline_job(job, runner, db_path, ws_id, params)

    step_ids = [s["id"] for s in runner.steps_defined]
    assert "classify:bioclip-vit-b-16" in step_ids, (
        f"Single-model run should still produce a per-model classify row, "
        f"got {step_ids}"
    )
    step_by_id = {s["id"]: s for s in runner.steps_defined}
    label = step_by_id["classify:bioclip-vit-b-16"]["label"]
    assert "bioclip" in label.lower() or "BioCLIP" in label, (
        f"Per-model classify row should be labeled with the model's display "
        f"name, got {label!r}"
    )


def test_pipeline_detect_runs_once_before_any_classifier_loads(
    tmp_path, monkeypatch
):
    """Detection should run as its own pre-pass across all photos BEFORE any
    classifier is constructed, so users see detection as a distinct phase
    rather than interleaved with model 1's classify loop."""
    import classifier as classifier_mod
    import classify_job
    import config as cfg
    from db import Database
    from PIL import Image

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    # Create real photos so collection has something to iterate.
    photo_dir = tmp_path / "photos"
    photo_dir.mkdir()
    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    folder_id = db.add_folder(str(photo_dir))
    photo_ids = []
    import json
    for i in range(3):
        img_path = photo_dir / f"p{i}.jpg"
        Image.new("RGB", (64, 64), "red").save(str(img_path))
        photo_ids.append(
            db.add_photo(folder_id, f"p{i}.jpg", ".jpg", 1000 + i, 1_000_000.0)
        )
    col_id = db.add_collection(
        "Test",
        json.dumps([{"field": "photo_ids", "value": photo_ids}]),
    )

    model_ids = _setup_two_fake_downloaded_models(tmp_path, monkeypatch)

    events = []

    def fake_detect_batch(batch, folders, runner, job, reclassify, db_,
                          det_conf_threshold=None, already_detected_ids=None,
                          cached_detections=None):
        events.append(("detect", [p["id"] for p in batch]))
        det_map = {}
        for p in batch:
            det_id = db_.save_detections(
                p["id"],
                [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
                  "confidence": 0.9, "category": "animal"}],
                detector_model="megadetector-v6",
            )[0]
            det_map[p["id"]] = [{
                "id": det_id,
                "box_x": 0.1,
                "box_y": 0.1,
                "box_w": 0.5,
                "box_h": 0.5,
                "confidence": 0.9,
                "category": "animal",
                "detector_model": "megadetector-v6",
            }]
        return det_map, len(det_map), {p["id"] for p in batch}

    monkeypatch.setattr(classify_job, "_detect_batch", fake_detect_batch)

    class FakeClassifier:
        def __init__(self, *args, **kwargs):
            events.append(("classifier_init", kwargs.get("pretrained_str")))

        def encode_image(self, *args, **kwargs):
            import numpy as np
            return np.zeros(512, dtype=np.float32)

        def classify_batch_with_embedding(self, images, threshold=0):
            import numpy as np
            return [
                (
                    [{"species": "Test species", "score": 0.9}],
                    np.zeros(512, dtype=np.float32),
                )
                for _ in images
            ]

    monkeypatch.setattr(classifier_mod, "Classifier", FakeClassifier)

    params = PipelineParams(
        collection_id=col_id,
        model_ids=model_ids,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()
    run_pipeline_job(job, runner, db_path, ws_id, params)

    # Every detect event should come strictly before any classifier_init for a
    # *second* model. (Model 1's classifier is allowed to load in parallel
    # via the model_loader stage, but NO model should classify before detect
    # has finished running on all photos.)
    kinds = [e[0] for e in events]
    assert kinds, "expected detect / classifier events to be recorded"
    # At least one detect must have fired before any call into encode_image
    # (which is the actual classification work).  We check: the LAST detect
    # event must be before any classifier is "used" for classification; since
    # encode_image isn't tracked here, we verify that all detect events occur
    # before any classifier_init that corresponds to model 2+.
    classifier_inits = [i for i, k in enumerate(kinds) if k == "classifier_init"]
    detect_events = [i for i, k in enumerate(kinds) if k == "detect"]
    assert detect_events, "expected detect to run"
    last_detect = max(detect_events)
    # All detects should happen before classify actually starts — i.e. before
    # classifier_init for model 2 (model 1 may preload earlier).
    if len(classifier_inits) > 1:
        second_init = classifier_inits[1]
        assert last_detect < second_init, (
            f"Detection pre-pass should complete before model 2 is loaded, "
            f"but saw event order: {kinds}"
        )


def test_pipeline_one_model_fails_to_load_other_model_still_runs(
    tmp_path, monkeypatch
):
    """If the FIRST of two models fails to load, the second must still run
    and its per-model classify row must complete with predictions. The failed
    model's row must be marked 'failed' so users see exactly which model
    broke, not a buried note inside an aggregate summary."""
    import classifier as classifier_mod
    import config as cfg
    from db import Database
    from PIL import Image

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    photo_dir = tmp_path / "photos"
    photo_dir.mkdir()
    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    folder_id = db.add_folder(str(photo_dir))
    import json
    Image.new("RGB", (64, 64), "red").save(str(photo_dir / "x.jpg"))
    photo_id = db.add_photo(folder_id, "x.jpg", ".jpg", 1000, 1_000_000.0)
    col_id = db.add_collection(
        "Test",
        json.dumps([{"field": "photo_ids", "value": [photo_id]}]),
    )

    model_ids = _setup_two_fake_downloaded_models(tmp_path, monkeypatch)
    bad_id = model_ids[0]
    good_id = model_ids[1]

    class SelectiveClassifier:
        def __init__(self, *args, **kwargs):
            # Fail whenever we're asked to build the BAD model; succeed for
            # the other one. Keyed off the pretrained path so the behavior
            # is stable across however many construction attempts
            # model_loader + classify_stage make.
            pretrained = kwargs.get("pretrained_str", "")
            if bad_id in str(pretrained):
                raise RuntimeError("simulated bad weights for model 1")

        def encode_image(self, *args, **kwargs):
            import numpy as np
            return np.zeros(512, dtype=np.float32)

        def classify_with_embedding(self, img, threshold=0):
            import numpy as np
            return [{"species": "Robin", "score": 0.9}], np.zeros(
                512, dtype=np.float32,
            )

        def classify_batch_with_embedding(self, images, threshold=0):
            import numpy as np
            zero = np.zeros(512, dtype=np.float32)
            return [(
                [{"species": "Robin", "score": 0.9}],
                zero,
            ) for _ in images]

    monkeypatch.setattr(classifier_mod, "Classifier", SelectiveClassifier)

    params = PipelineParams(
        collection_id=col_id,
        model_ids=model_ids,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()
    run_pipeline_job(job, runner, db_path, ws_id, params)

    # Model 1 row should be in failed state.
    bad_statuses = [
        kwargs.get("status")
        for (_, step_id, kwargs) in runner.step_updates
        if step_id == f"classify:{bad_id}" and "status" in kwargs
    ]
    assert "failed" in bad_statuses, (
        f"Failed model's classify row should be marked 'failed', got "
        f"status history {bad_statuses}"
    )

    # Good model row should be in completed state.
    good_statuses = [
        kwargs.get("status")
        for (_, step_id, kwargs) in runner.step_updates
        if step_id == f"classify:{good_id}" and "status" in kwargs
    ]
    assert "completed" in good_statuses, (
        f"Surviving model's classify row should complete, got "
        f"status history {good_statuses}"
    )


def test_pipeline_per_model_step_summary_includes_prediction_count(
    tmp_path, monkeypatch
):
    """Each per-model classify row's completion summary should report
    counts (predictions stored, detections reused, etc.) so users can see
    which model found what without reading the aggregate."""
    import classifier as classifier_mod
    import config as cfg
    from db import Database
    from PIL import Image

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    photo_dir = tmp_path / "photos"
    photo_dir.mkdir()
    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    folder_id = db.add_folder(str(photo_dir))
    import json
    Image.new("RGB", (64, 64), "red").save(str(photo_dir / "p.jpg"))
    photo_id = db.add_photo(folder_id, "p.jpg", ".jpg", 1000, 1_000_000.0)
    col_id = db.add_collection(
        "Test",
        json.dumps([{"field": "photo_ids", "value": [photo_id]}]),
    )

    model_ids = _setup_two_fake_downloaded_models(tmp_path, monkeypatch)

    class FakeClassifier:
        def __init__(self, *args, **kwargs):
            pass

        def encode_image(self, *args, **kwargs):
            import numpy as np
            return np.zeros(512, dtype=np.float32)

        def classify_with_embedding(self, img, threshold=0):
            import numpy as np
            return [{"species": "Robin", "score": 0.9}], np.zeros(
                512, dtype=np.float32,
            )

        def classify_batch_with_embedding(self, images, threshold=0):
            import numpy as np
            zero = np.zeros(512, dtype=np.float32)
            return [(
                [{"species": "Robin", "score": 0.9}],
                zero,
            ) for _ in images]

    monkeypatch.setattr(classifier_mod, "Classifier", FakeClassifier)

    params = PipelineParams(
        collection_id=col_id,
        model_ids=model_ids,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()
    run_pipeline_job(job, runner, db_path, ws_id, params)

    for mid in model_ids:
        completed_summaries = [
            kwargs.get("summary", "")
            for (_, step_id, kwargs) in runner.step_updates
            if step_id == f"classify:{mid}"
            and kwargs.get("status") == "completed"
            and "summary" in kwargs
        ]
        assert completed_summaries, (
            f"classify:{mid} row must record a summary on completion"
        )
        summary = completed_summaries[-1]
        assert "prediction" in summary.lower(), (
            f"per-model summary for {mid} should mention prediction counts, "
            f"got {summary!r}"
        )


def test_pipeline_reclassify_purge_deferred_until_a_model_succeeds(
    tmp_path, monkeypatch
):
    """On a reclassify run where every model fails to load, the pre-run
    detection rows MUST NOT be deleted. Deleting them ahead of a
    successful classify would cascade through the predictions FK and
    destroy prior results even though no new predictions were written.

    Regression test for Codex P1 on PR #566.
    """
    import json

    import classifier as classifier_mod
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    # Seed a prior-run detection so the reclassify purge has something to
    # potentially delete.
    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)
    photo_id = db.add_photo(folder_id, "test.jpg", ".jpg", 12345, 1_000_000.0)
    db.save_detections(
        photo_id,
        [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
          "confidence": 0.9, "category": "animal"}],
        detector_model="MegaDetector",
    )
    col_id = db.add_collection(
        "Test",
        json.dumps([{"field": "photo_ids", "value": [photo_id]}]),
    )

    _setup_two_fake_downloaded_models(tmp_path, monkeypatch)

    # Every Classifier construction raises — simulating the "all models
    # fail to load" case the purge must defend against.
    class AlwaysFailClassifier:
        def __init__(self, *args, **kwargs):
            raise RuntimeError("simulated catastrophic load failure")

    monkeypatch.setattr(classifier_mod, "Classifier", AlwaysFailClassifier)

    # Snapshot pre-run detection row count so we can assert it survived.
    pre_count = db.conn.execute(
        "SELECT COUNT(*) AS c FROM detections"
    ).fetchone()["c"]
    assert pre_count >= 1, "fixture should have inserted at least 1 row"

    params = PipelineParams(
        collection_id=col_id,
        model_ids=["bioclip-vit-b-16", "bioclip-2"],
        reclassify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()
    import pytest
    with pytest.raises(RuntimeError):
        run_pipeline_job(job, runner, db_path, ws_id, params)

    # The original detection row MUST still exist: no model succeeded, so
    # the purge must not have fired.
    post = db.conn.execute(
        "SELECT COUNT(*) AS c FROM detections WHERE id = ?",
        (db.conn.execute(
            "SELECT id FROM detections LIMIT 1"
        ).fetchone()["id"],),
    ).fetchone()
    # Simpler: just confirm some prior detections survived.
    survivors = db.conn.execute(
        "SELECT COUNT(*) AS c FROM detections WHERE detector_model != 'full-image'"
    ).fetchone()["c"]
    assert survivors >= 1, (
        "reclassify must not purge pre-run detection rows when every model "
        "failed to load — it would cascade-destroy prior predictions "
        f"(survivors={survivors})"
    )


def test_pipeline_fatal_error_does_not_overwrite_completed_model_rows(
    tmp_path, monkeypatch
):
    """When classify_stage hits a fatal exception AFTER one model has
    already finished, the completed model's `classify:<id>` row must stay
    `completed` — not be rewritten to `failed` by the catch-all error
    handler. Otherwise per-model status is misreported.

    Regression test for Codex P2 on PR #566.
    """
    import json

    import classifier as classifier_mod
    import classify_job
    import config as cfg
    from db import Database
    from PIL import Image

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    photo_dir = tmp_path / "photos"
    photo_dir.mkdir()
    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    folder_id = db.add_folder(str(photo_dir))
    Image.new("RGB", (64, 64), "red").save(str(photo_dir / "p.jpg"))
    photo_id = db.add_photo(folder_id, "p.jpg", ".jpg", 1000, 1_000_000.0)
    col_id = db.add_collection(
        "Test",
        json.dumps([{"field": "photo_ids", "value": [photo_id]}]),
    )

    model_ids = _setup_two_fake_downloaded_models(tmp_path, monkeypatch)
    first_id, second_id = model_ids

    class FakeClassifier:
        def __init__(self, *args, **kwargs):
            pass

        def encode_image(self, *args, **kwargs):
            import numpy as np
            return np.zeros(512, dtype=np.float32)

        def classify_with_embedding(self, img, threshold=0):
            import numpy as np
            return [{"species": "Robin", "score": 0.9}], np.zeros(
                512, dtype=np.float32,
            )

        def classify_batch_with_embedding(self, images, threshold=0):
            import numpy as np
            zero = np.zeros(512, dtype=np.float32)
            return [(
                [{"species": "Robin", "score": 0.9}],
                zero,
            ) for _ in images]

    monkeypatch.setattr(classifier_mod, "Classifier", FakeClassifier)

    # Let the first model's grouping/storage succeed normally, then blow
    # up when the SECOND model asks _store_grouped_predictions to run.
    call_count = {"n": 0}
    original_store = classify_job._store_grouped_predictions

    def maybe_explode(raw_results, job_id, model_name, *args, **kwargs):
        call_count["n"] += 1
        if call_count["n"] >= 2:
            raise RuntimeError("simulated mid-loop fatal after model 1")
        return original_store(raw_results, job_id, model_name, *args, **kwargs)

    monkeypatch.setattr(
        classify_job, "_store_grouped_predictions", maybe_explode,
    )

    params = PipelineParams(
        collection_id=col_id,
        model_ids=model_ids,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()
    import pytest
    with pytest.raises(RuntimeError):
        run_pipeline_job(job, runner, db_path, ws_id, params)

    # First model's row must end in 'completed' and stay that way; the
    # fatal handler must NOT have overwritten it with 'failed'.
    first_final = None
    for (_, step_id, kwargs) in runner.step_updates:
        if step_id == f"classify:{first_id}" and "status" in kwargs:
            first_final = kwargs["status"]
    assert first_final == "completed", (
        f"First model's row should remain 'completed' after a later fatal "
        f"error, got final status {first_final!r}"
    )


def test_pipeline_loader_abort_finalizes_detect_and_classify_rows(
    tmp_path, monkeypatch
):
    """When model_loader_stage sets abort (single-model preload failure),
    the phase dispatcher must still invoke detect_stage and classify_stage
    so their step rows reach a terminal status. Without this, the newly
    added `detect` and `classify:<id>` rows stay `pending` forever on a
    loader-triggered failure, which is exactly the scenario these rows
    were added to clarify.

    Regression test for Codex P2 on PR #566 (line 1781).
    """
    import classifier as classifier_mod
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    col_id = db.add_collection("Test", "[]")

    _setup_fake_downloaded_model(tmp_path, monkeypatch)

    # Single-model run where construction always fails — this triggers
    # model_loader_stage's fatal path and sets abort before detect_stage.
    def boom(*args, **kwargs):
        raise RuntimeError("simulated single-model preload failure")

    monkeypatch.setattr(classifier_mod, "Classifier", boom)

    params = PipelineParams(
        collection_id=col_id,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()

    import pytest
    with pytest.raises(RuntimeError):
        run_pipeline_job(job, runner, db_path, ws_id, params)

    # The `detect` and every `classify:<id>` row must reach a terminal
    # state. A pending status means the jobs view would display an
    # indeterminate spinner for a run that has already finished — the
    # exact bug this test guards against.
    terminal = {"completed", "failed", "skipped"}
    steps_of_interest = [
        s["id"] for s in runner.steps_defined
        if s["id"] == "detect" or s["id"].startswith("classify:")
    ]
    assert steps_of_interest, (
        "test precondition: expected detect + classify rows in step_defs"
    )
    for sid in steps_of_interest:
        statuses = [
            kw.get("status")
            for (_, s, kw) in runner.step_updates
            if s == sid and "status" in kw
        ]
        final = statuses[-1] if statuses else None
        assert final in terminal, (
            f"Step {sid!r} must reach a terminal status on loader-triggered "
            f"abort, got {final!r} (history={statuses})"
        )


def test_pipeline_loader_failure_marks_classify_rows_failed_not_skipped(
    tmp_path, monkeypatch
):
    """When model_loader_stage fails (e.g. single-model preload failure or
    id resolution failure), classify_stage's early-skip branch must finalize
    the per-model rows as 'failed' — NOT 'completed' with summary='Skipped'.

    Otherwise the failed model is misreported as a clean skip, which hides
    the per-model failure context the row split is meant to surface.

    Regression test for Codex P2 on PR #566 (pipeline_job.py:1173).
    """
    import classifier as classifier_mod
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    col_id = db.add_collection("Test", "[]")

    _setup_fake_downloaded_model(tmp_path, monkeypatch)

    # Single-model run where construction always fails → model_loader_stage
    # catches the error, sets abort, and marks itself failed.
    def boom(*args, **kwargs):
        raise RuntimeError("simulated single-model preload failure")

    monkeypatch.setattr(classifier_mod, "Classifier", boom)

    params = PipelineParams(
        collection_id=col_id,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()

    import pytest
    with pytest.raises(RuntimeError):
        run_pipeline_job(job, runner, db_path, ws_id, params)

    classify_rows = [
        (step_id, kwargs)
        for (_, step_id, kwargs) in runner.step_updates
        if step_id.startswith("classify:") and "status" in kwargs
    ]
    assert classify_rows, (
        "test precondition: expected at least one classify:<id> update"
    )
    for step_id, kwargs in classify_rows:
        # The final status on a loader-failure abort must be 'failed', not
        # 'completed' (which would render as a clean skipped row).
        assert kwargs["status"] == "failed", (
            f"Row {step_id!r} should be 'failed' after loader aborted the "
            f"pipeline, got status={kwargs['status']!r}, "
            f"summary={kwargs.get('summary')!r}"
        )


# ---------------------------------------------------------------------------
# Failure rollup — per-file failures surface at the stage/job level
# ---------------------------------------------------------------------------


def _make_photo_dir(tmp_path, n):
    """Drop n tiny JPEGs in a fresh dir and return it."""
    from PIL import Image
    photo_dir = tmp_path / "photos"
    photo_dir.mkdir()
    for i in range(n):
        img = Image.new("RGB", (40, 40), "red")
        img.save(str(photo_dir / f"p_{i}.jpg"))
    return photo_dir


def test_thumbnail_failures_complete_stage_with_repair_warnings(tmp_path, monkeypatch):
    """Per-photo gaps warn without invalidating successful thumbnails."""
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")
    photo_dir = _make_photo_dir(tmp_path, 4)

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    # Make every second thumbnail "fail" by returning None.
    import thumbnails as thumbnails_mod
    real_gen = thumbnails_mod.generate_thumbnail
    call_count = {"n": 0}

    def flaky_gen(photo_id, photo_path, cache_dir, size=300):
        call_count["n"] += 1
        if call_count["n"] % 2 == 0:
            return None
        return real_gen(photo_id, photo_path, cache_dir, size=size)

    monkeypatch.setattr(thumbnails_mod, "generate_thumbnail", flaky_gen)

    params = PipelineParams(
        source=str(photo_dir),
        skip_classify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )
    job = _make_job()
    runner = FakeRunner()

    pipeline_result = run_pipeline_job(job, runner, db_path, ws_id, params)

    thumb_result = pipeline_result["stages"]["thumbnails"]
    assert thumb_result["failed"] > 0, (
        f"Test setup bug: expected thumbnail failures. Result: {thumb_result}"
    )
    assert thumb_result["generated"] > 0, (
        f"Test setup bug: expected some thumbnail successes. Result: {thumb_result}"
    )

    # Inspect the final stages status as updated on the job runner.
    final_thumb_updates = [
        kwargs for (_, step, kwargs) in runner.step_updates
        if step == "thumbnails" and kwargs.get("status")
    ]
    final_status = final_thumb_updates[-1]["status"]
    assert final_status == "completed", (
        f"Mixed-outcome rollup must report 'completed', got {final_status!r}. "
        f"Result: {thumb_result}"
    )
    assert final_thumb_updates[-1]["error_count"] == thumb_result["failed"]
    assert len(thumb_result["failed_photos"]) == thumb_result["failed"]
    assert all(item["filename"] for item in thumb_result["failed_photos"])
    assert pipeline_result["warnings"] == [
        f"[thumbnails] {thumb_result['failed']} of 4 thumbnails need attention"
    ]


def test_thumbnail_failures_append_rollup_warning_not_job_error(tmp_path, monkeypatch):
    """Coverage gaps surface once as warnings without failing the job."""
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")
    photo_dir = _make_photo_dir(tmp_path, 3)

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    # All thumbnails fail.
    import thumbnails as thumbnails_mod
    monkeypatch.setattr(
        thumbnails_mod, "generate_thumbnail",
        lambda photo_id, photo_path, cache_dir, size=300: None,
    )

    params = PipelineParams(
        source=str(photo_dir),
        skip_classify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )
    job = _make_job()

    result = run_pipeline_job(job, FakeRunner(), db_path, ws_id, params)

    assert job["errors"] == []
    assert result["warnings"] == [
        "[thumbnails] 3 of 3 thumbnails need attention"
    ]
    assert len(result["stages"]["thumbnails"]["failed_photos"]) == 3


def test_pipeline_thumbnail_retry_uses_near_full_working_copy(tmp_path):
    from PIL import Image
    from pipeline_job import _retry_thumbnail_with_working_copy

    vireo_dir = tmp_path / "vireo"
    working_dir = vireo_dir / "working"
    working_dir.mkdir(parents=True)
    wc_path = working_dir / "7.jpg"
    Image.new("RGB", (5392, 3592), "red").save(wc_path)
    photo = {
        "id": 7,
        "filename": "photo.NEF",
        "width": 5408,
        "height": 3608,
        "working_copy_path": "working/7.jpg",
        "file_mtime": None,
    }
    calls = []

    def generate(photo_id, source_path, cache_dir, **kwargs):
        calls.append((photo_id, source_path, kwargs))
        return str(tmp_path / "thumbs" / "7.jpg")

    result = _retry_thumbnail_with_working_copy(
        object(), generate, photo, 7, str(tmp_path / "photo.NEF"),
        str(tmp_path / "thumbs"), 300,
        {"crop": {"x": 0, "y": 0, "w": 0.5, "h": 1}},
        str(vireo_dir),
    )

    assert result.endswith("7.jpg")
    assert os.path.normpath(calls[0][1]) == os.path.normpath(str(wc_path))


def test_thumbnail_progress_counter_includes_failed(tmp_path, monkeypatch):
    """stages['thumbnails']['count'] must include failed items so the UI
    progress bar reflects work actually attempted, not just successes."""
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")
    photo_dir = _make_photo_dir(tmp_path, 2)

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    import thumbnails as thumbnails_mod
    monkeypatch.setattr(
        thumbnails_mod, "generate_thumbnail",
        lambda photo_id, photo_path, cache_dir, size=300: None,
    )

    params = PipelineParams(
        source=str(photo_dir),
        skip_classify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )
    job = _make_job()
    runner = FakeRunner()

    with contextlib.suppress(RuntimeError):
        run_pipeline_job(job, runner, db_path, ws_id, params)

    # stages dict's own counter (shown on the dashboard) must include failed.
    # Apr 5 bug: scan reported "1472 photos" but stages['thumbnails']['count']
    # sat at 0 (generated + skipped = 0 + 0) despite all 1472 files processed.
    thumb_progress_events = [
        data for (_, evt, data) in runner.events
        if evt == "progress" and data.get("stage_id") == "thumbnails"
    ]
    assert thumb_progress_events, "No thumbnails progress events emitted"
    last = thumb_progress_events[-1]
    thumb_stage_count = last["stages"]["thumbnails"].get("count", 0)
    assert thumb_stage_count > 0, (
        f"stages['thumbnails']['count'] must include failed items (was {thumb_stage_count}). "
        f"Last event stages: {last['stages']}"
    )


def test_pipeline_with_snapshot_scans_only_snapshot_folders(tmp_path, monkeypatch):
    """When source_snapshot_id is provided, the scan stage must walk only the
    parent directories of the snapshot's files — sibling folders registered
    with the workspace but not in the snapshot must NOT be scanned."""
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    # Two sibling folders each with one JPEG. Only folder A is in the snapshot.
    folder_a = tmp_path / "folderA"
    folder_b = tmp_path / "folderB"
    folder_a.mkdir()
    folder_b.mkdir()
    folder_a_id = db.add_folder(str(folder_a))
    folder_b_id = db.add_folder(str(folder_b))
    _drop_jpeg(str(folder_a), "IMG_001.JPG")
    _drop_jpeg(str(folder_b), "IMG_002.JPG")

    snap_id = db.create_new_images_snapshot([str(folder_a / "IMG_001.JPG")])

    params = PipelineParams(
        source_snapshot_id=snap_id,
        skip_classify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )
    runner = FakeRunner()
    job = _make_job()

    run_pipeline_job(job, runner, db_path, ws_id, params)

    # Verify via DB state: folder A has its photo ingested, folder B does not.
    verify_db = Database(db_path)
    verify_db.set_active_workspace(ws_id)
    a_photos = verify_db.conn.execute(
        "SELECT filename FROM photos WHERE folder_id = ?", (folder_a_id,),
    ).fetchall()
    b_photos = verify_db.conn.execute(
        "SELECT filename FROM photos WHERE folder_id = ?", (folder_b_id,),
    ).fetchall()
    assert [r["filename"] for r in a_photos] == ["IMG_001.JPG"], (
        f"folder A should have its snapshot file ingested, got {list(a_photos)}"
    )
    assert list(b_photos) == [], (
        f"folder B must NOT be scanned (not in snapshot), got {list(b_photos)}"
    )


def test_pipeline_snapshot_excludes_late_arriving_files(tmp_path, monkeypatch):
    """Files that land in a registered folder AFTER a snapshot is captured
    must NOT be cataloged by the pipeline scan. If they were, downstream
    filtering would keep them out of classify/extract_masks/regroup, but the
    finally-block cache invalidation still runs — orphaning the file
    (cataloged in DB, never processed, never re-surfaced by a later banner
    probe). Instead, the scan is restricted to the snapshot's file set so
    late arrivals remain uncataloged and the next new-images probe
    rediscovers them.
    """
    import classifier as classifier_mod
    import classify_job
    import config as cfg
    from db import Database
    from PIL import Image

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    folder = tmp_path / "photos"
    folder.mkdir()
    db.add_folder(str(folder))

    # "Early" file — exists at snapshot time, goes into the snapshot.
    # Use distinct pixel content so the scanner's hash-based duplicate
    # resolver doesn't collapse the two files into one photo row (same
    # 16x16 black rectangle hashes to the same bytes).
    Image.new("RGB", (16, 16), (10, 10, 10)).save(
        str(folder / "IMG_early.JPG")
    )
    snap_id = db.create_new_images_snapshot([str(folder / "IMG_early.JPG")])

    # "Late" file — arrives after the snapshot but before the pipeline runs.
    # The scanner must NOT catalog it — it lives in the same folder as the
    # snapshot file, but restrict_files scopes the walk to the exact
    # snapshot paths so late arrivals stay new.
    Image.new("RGB", (16, 16), (200, 50, 50)).save(
        str(folder / "IMG_late.JPG")
    )

    # Wire up fake classifier + detect_batch so classify_stage actually runs
    # and writes a predictions row for whatever photo it sees.
    model_id = _setup_fake_downloaded_model(tmp_path, monkeypatch)

    # detect_stage calls ensure_megadetector_weights() whenever any photo
    # lacks a cached detection — which is every fresh-scan run. Short-circuit
    # to avoid a real network download in the test.
    import detector as detector_mod
    monkeypatch.setattr(
        detector_mod, "ensure_megadetector_weights",
        lambda progress_callback=None: "/tmp/fake-md-weights.onnx",
    )

    # Map filename → synthetic detection_id; we need a real detection row per
    # photo fed to classify so _flush_batch has a valid FK to bind to.
    def fake_detect_batch(batch, folders, runner, job, reclassify, db_,
                          det_conf_threshold=None, already_detected_ids=None,
                          cached_detections=None):
        det_map = {}
        processed = set()
        for p in batch:
            det_ids = db_.save_detections(
                p["id"],
                [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
                  "confidence": 0.95, "category": "animal"}],
                detector_model="MegaDetector",
            )
            det_map[p["id"]] = [{
                "id": det_ids[0],
                "box_x": 0.1, "box_y": 0.1, "box_w": 0.5, "box_h": 0.5,
                "confidence": 0.95, "category": "animal",
            }]
            processed.add(p["id"])
        return det_map, len(det_map), processed

    monkeypatch.setattr(classify_job, "_detect_batch", fake_detect_batch)

    def fake_prepare_image(photo, folders, detection, vireo_dir=None):
        return (
            Image.new("RGB", (32, 32), "white"),
            folders.get(photo["folder_id"], ""),
            os.path.join(folders.get(photo["folder_id"], ""), photo["filename"]),
        )

    monkeypatch.setattr(classify_job, "_prepare_image", fake_prepare_image)

    class FakeClassifier:
        def __init__(self, *args, **kwargs):
            pass

        def classify_batch_with_embedding(self, images, threshold=0):
            import numpy as np
            emb = np.zeros(512, dtype=np.float32)
            return [
                ([{"species": "Red-tailed Hawk", "score": 0.99}], emb)
                for _ in images
            ]

    monkeypatch.setattr(classifier_mod, "Classifier", FakeClassifier)

    params = PipelineParams(
        source_snapshot_id=snap_id,
        model_ids=[model_id],
        skip_extract_masks=True,
        skip_regroup=True,
    )
    runner = FakeRunner()
    job = _make_job()
    run_pipeline_job(job, runner, db_path, ws_id, params)

    # Verify via DB: only the snapshot (early) file should be cataloged. The
    # late file stays off-catalog so the next new-images probe finds it and
    # the banner reappears — self-healing freshness the Codex P1 review
    # called out.
    verify_db = Database(db_path)
    verify_db.set_active_workspace(ws_id)

    scanned = {
        r["filename"] for r in verify_db.conn.execute(
            "SELECT filename FROM photos"
        ).fetchall()
    }
    assert scanned == {"IMG_early.JPG"}, (
        f"scan must skip files not in the snapshot so late arrivals stay "
        f"discoverable by future probes, got {scanned}"
    )

    classified_names = {
        r["filename"] for r in verify_db.conn.execute(
            """SELECT p.filename
                 FROM predictions pr
                 JOIN detections d ON d.id = pr.detection_id
                 JOIN photos p ON p.id = d.photo_id"""
        ).fetchall()
    }
    assert "IMG_early.JPG" in classified_names, (
        f"early (snapshot) file should be classified, got {classified_names}"
    )
    assert "IMG_late.JPG" not in classified_names, (
        f"late (post-snapshot) file must NOT be classified, got "
        f"{classified_names}"
    )

    # The self-healing contract end-to-end: a fresh new-images walk still
    # reports the late file as new, so the banner re-raises for it.
    from new_images import count_new_images_for_workspace
    post_run = count_new_images_for_workspace(
        verify_db, ws_id, sample_limit=None,
    )
    assert post_run["new_count"] == 1, (
        f"late file must still be new after a snapshot-scoped run, "
        f"got {post_run}"
    )
    assert post_run["sample"] == [str(folder / "IMG_late.JPG")], (
        f"expected the late file in the new-images sample, got "
        f"{post_run['sample']}"
    )


def test_pipeline_snapshot_collapses_overlapping_scan_roots(tmp_path, monkeypatch):
    """When the snapshot contains files at both a folder and a nested subfolder
    (e.g. /root/a.jpg and /root/sub/b.jpg), deriving scan roots naively would
    produce overlapping paths (/root and /root/sub). The scanner would then
    walk the subtree twice. params.sources must be collapsed to the minimal
    non-overlapping ancestor set."""
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    root = tmp_path / "root"
    sub = root / "sub"
    sub.mkdir(parents=True)
    db.add_folder(str(root))
    db.add_folder(str(sub))

    top_path = root / "a.jpg"
    sub_path = sub / "b.jpg"
    _drop_jpeg(str(root), "a.jpg")
    _drop_jpeg(str(sub), "b.jpg")

    snap_id = db.create_new_images_snapshot([str(top_path), str(sub_path)])

    # Spy on scanner.scan to count how many distinct roots it walks.
    import scanner as scanner_mod
    scan_calls = []
    original_scan = scanner_mod.scan

    def spy_scan(root_path, db_, **kwargs):
        scan_calls.append(root_path)
        return original_scan(root_path, db_, **kwargs)

    monkeypatch.setattr(scanner_mod, "scan", spy_scan)

    params = PipelineParams(
        source_snapshot_id=snap_id,
        skip_classify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )
    runner = FakeRunner()
    job = _make_job()
    run_pipeline_job(job, runner, db_path, ws_id, params)

    # The nested path is a descendant of the top path; the scanner walks root
    # recursively, so sub must NOT be re-scanned as a separate root.
    assert str(root) in scan_calls, f"top root must be scanned, got {scan_calls}"
    assert str(sub) not in scan_calls, (
        f"sub is a descendant of root and must not be scanned separately, "
        f"got {scan_calls}"
    )


def test_collapse_scan_roots_handles_filesystem_root():
    """Unit test for the collapse helper's edge case where a kept root IS
    the filesystem root ('/' on POSIX, 'C:\\' on Windows). The naive
    `kept + os.sep` prefix becomes '//' for '/' and fails to match child
    paths like '/sub'. Descendants of the filesystem root must still be
    collapsed away."""
    from pipeline_job import _collapse_scan_roots

    collapsed = _collapse_scan_roots([os.sep, os.path.join(os.sep, "sub")])
    assert collapsed == [os.sep], (
        f"descendants of filesystem root must collapse, got {collapsed}"
    )

    # Non-overlapping peers are preserved.
    a = os.path.join(os.sep, "a")
    b = os.path.join(os.sep, "b")
    collapsed = _collapse_scan_roots([a, b])
    assert collapsed == sorted([a, b]), (
        f"peers must both be kept, got {collapsed}"
    )

    # Prefix-but-not-descendant isn't collapsed (/foo vs /foobar).
    foo = os.path.join(os.sep, "foo")
    foobar = os.path.join(os.sep, "foobar")
    collapsed = _collapse_scan_roots([foo, foobar])
    assert collapsed == sorted([foo, foobar]), (
        f"/foo and /foobar are peers, got {collapsed}"
    )


def test_pipeline_miss_stage_skipped_when_regroup_fails(tmp_path, monkeypatch):
    """miss_stage depends on burst_id written by regroup. If regroup_stage
    throws, running miss_stage would overwrite miss_* flags with stale
    context during an already-failing job. The gate must check the
    stage's failed status, not just the global abort flag (regroup_stage
    marks itself failed without setting abort)."""
    import config as cfg
    from db import Database
    from PIL import Image

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    photo_dir = tmp_path / "photos"
    photo_dir.mkdir()
    for name in ("a.jpg", "b.jpg"):
        Image.new("RGB", (16, 16), "black").save(str(photo_dir / name))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    # Force regroup to fail before it finishes. pipeline_job imports
    # run_full_pipeline lazily inside regroup_stage; patch at module level.
    import pipeline as pipeline_mod

    def _boom(*args, **kwargs):
        raise RuntimeError("synthetic regroup failure")

    monkeypatch.setattr(pipeline_mod, "run_full_pipeline", _boom)

    # Also mark the single photo with an arbitrary miss_computed_at so we
    # can detect mutation by miss_stage.
    params = PipelineParams(
        source=str(photo_dir),
        skip_classify=True,
        skip_extract_masks=True,
        # Intentionally NOT skip_regroup — regroup must be attempted and fail.
    )

    runner = FakeRunner()
    job = _make_job()

    import contextlib
    with contextlib.suppress(Exception):
        result = run_pipeline_job(job, runner, db_path, ws_id, params)

    # Inspect the stages dict from the last progress event — if miss_stage
    # ran, it would transition out of "pending" to "running"/"completed"/
    # "failed"/"skipped". The fix must leave it "pending" (never entered).
    progress_events = [
        data for (_, evt, data) in runner.events
        if evt == "progress" and "stages" in data
    ]
    assert progress_events, "pipeline emitted no progress events"
    last_stages = progress_events[-1]["stages"]
    assert last_stages["regroup"]["status"] == "failed"
    # Miss stage must not have mutated any miss_* row. Verify by reading
    # miss_computed_at on the scanned photos — all should still be NULL.
    db2 = Database(db_path)
    db2.set_active_workspace(ws_id)
    rows = db2.conn.execute(
        "SELECT miss_computed_at FROM photos"
    ).fetchall()
    assert rows, "scan produced no photo rows"
    for r in rows:
        assert r["miss_computed_at"] is None, (
            "miss_stage ran after regroup failure and overwrote miss state"
        )


def test_workspace_regroup_lock_spans_regroup_and_miss(tmp_path, monkeypatch):
    """The workspace_regroup lock must wrap BOTH regroup_stage and
    miss_stage. Without a single lock spanning both, pipeline B could
    sneak in between A's regroup release and A's miss acquire and
    rewrite burst_id / pipeline_results_ws*.json — leaving the
    persisted miss flags inconsistent with the cached grouping the
    review UI reads for "Review misses".

    Trace acquire/release vs. regroup-work and miss-step events and
    verify release happens AFTER the miss step, not between them."""
    import config as cfg
    import pipeline as pipeline_mod
    import pipeline_job as pj
    import pipeline_locks
    from db import Database
    from PIL import Image

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    photo_dir = tmp_path / "photos"
    photo_dir.mkdir()
    Image.new("RGB", (16, 16), "black").save(str(photo_dir / "a.jpg"))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    events = []
    pause_state = {
        "requested": False,
        "lock_held": False,
        "wait_lock_states": [],
    }

    real_acquire = pipeline_locks.acquire_workspace_regroup

    class TrackingLock:
        def __init__(self, inner, ws):
            self._inner = inner
            self._ws = ws

        def __enter__(self):
            events.append(("acquire", self._ws))
            self._inner.__enter__()
            pause_state["lock_held"] = True
            return self

        def __exit__(self, *args):
            events.append(("release", self._ws))
            try:
                return self._inner.__exit__(*args)
            finally:
                pause_state["lock_held"] = False

    monkeypatch.setattr(
        pj, "acquire_workspace_regroup",
        lambda ws: TrackingLock(real_acquire(ws), ws),
    )

    def _ok_run(photos, config=None, emit_trace=False):
        events.append(("regroup_work", ws_id))
        # Simulate Pause arriving during regroup work. The pipeline must defer
        # its blocking wait until after the shared regroup/misses lock releases.
        pause_state["requested"] = True
        return {"summary": {"groups": 1}, "photos": photos}

    monkeypatch.setattr(pipeline_mod, "run_full_pipeline", _ok_run)
    monkeypatch.setattr(pipeline_mod, "save_results", lambda *a, **kw: None)
    monkeypatch.setattr(
        pipeline_mod, "load_photo_features",
        lambda thread_db, collection_id=None, config=None: [{"id": 1}],
    )

    class TrackingRunner(FakeRunner):
        def pause_requested(self, job_id):
            return pause_state["requested"]

        def cancellation_requested(self, job_id):
            return False

        def mark_paused(self, job_id):
            return True

        def wait_if_paused(self, job_id, *, publish_paused=False):
            pause_state["wait_lock_states"].append(
                pause_state["lock_held"],
            )
            pause_state["requested"] = False
            return False

        def update_step(self, job_id, step_id, **kwargs):
            if step_id == "misses":
                events.append(("miss_step", kwargs.get("status")))
            super().update_step(job_id, step_id, **kwargs)

    # skip_classify=True keeps the test from needing real model files;
    # miss_stage takes its early-skip path but still calls update_step
    # on the "misses" step — which must happen INSIDE the lock.
    params = PipelineParams(
        source=str(photo_dir),
        skip_classify=True,
        skip_extract_masks=True,
    )
    runner = TrackingRunner()
    job = _make_job()

    with contextlib.suppress(Exception):
        run_pipeline_job(job, runner, db_path, ws_id, params)

    # Strip events from earlier stages that don't touch the regroup lock.
    interesting = [
        e for e in events
        if e[0] in ("acquire", "release", "regroup_work", "miss_step")
    ]
    assert ("acquire", ws_id) in interesting, (
        f"workspace_regroup lock was never acquired; events: {interesting}"
    )
    assert ("release", ws_id) in interesting, (
        f"workspace_regroup lock was never released; events: {interesting}"
    )

    acquire_idx = interesting.index(("acquire", ws_id))
    release_idx = interesting.index(("release", ws_id))
    regroup_idx = next(
        (i for i, e in enumerate(interesting) if e[0] == "regroup_work"),
        None,
    )
    miss_idx = next(
        (i for i, e in enumerate(interesting) if e[0] == "miss_step"),
        None,
    )

    assert regroup_idx is not None, (
        f"regroup work never ran; events: {interesting}"
    )
    assert miss_idx is not None, (
        f"miss step never ran; events: {interesting}"
    )
    assert acquire_idx < regroup_idx < release_idx, (
        f"regroup work must run inside the lock; events: {interesting}"
    )
    assert acquire_idx < miss_idx < release_idx, (
        "miss step must run inside the same lock as regroup — otherwise "
        "a second same-workspace pipeline could sneak in and rewrite "
        f"grouping state between them. events: {interesting}"
    )
    assert pause_state["wait_lock_states"] == [False], (
        "Pause must block only after the regroup/misses lock is released; "
        f"wait states: {pause_state['wait_lock_states']}"
    )


def test_pipeline_regroup_stamps_workspace_group_fingerprint(tmp_path, monkeypatch):
    """When regroup_stage completes successfully, last_grouped_at and
    last_group_fingerprint must be written on the active workspace so the
    pipeline page can render "fresh" instead of "Outdated"."""
    import config as cfg
    from db import Database
    from PIL import Image

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    photo_dir = tmp_path / "photos"
    photo_dir.mkdir()
    for name in ("a.jpg", "b.jpg"):
        Image.new("RGB", (16, 16), "black").save(str(photo_dir / name))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    # Stub run_full_pipeline + save_results so regroup completes deterministically.
    import pipeline as pipeline_mod

    def _ok_run(photos, config=None, emit_trace=False):
        return {"summary": {"groups": 1}, "photos": photos}

    def _no_save(results, cache_dir, workspace_id):
        return None

    monkeypatch.setattr(pipeline_mod, "run_full_pipeline", _ok_run)
    monkeypatch.setattr(pipeline_mod, "save_results", _no_save)

    # Regroup uses load_photo_features to decide whether to skip on empty.
    # Return a single fake photo so the success branch (which stamps the
    # fingerprint) is exercised rather than the "no photos to group" branch.
    monkeypatch.setattr(
        pipeline_mod, "load_photo_features",
        lambda thread_db, collection_id=None, config=None: [{"id": 1}],
    )

    params = PipelineParams(
        source=str(photo_dir),
        skip_classify=True,
        skip_extract_masks=True,
        # Regroup must be attempted and succeed.
    )

    runner = FakeRunner()
    job = _make_job()

    with contextlib.suppress(Exception):
        run_pipeline_job(job, runner, db_path, ws_id, params)

    # Verify the workspace row now has last_grouped_at + last_group_fingerprint
    # set to the values that compute_group_fingerprint() yields for this config.
    db2 = Database(db_path)
    db2.set_active_workspace(ws_id)
    row = db2.conn.execute(
        "SELECT last_grouped_at, last_group_fingerprint FROM workspaces WHERE id=?",
        (ws_id,),
    ).fetchone()
    assert row["last_grouped_at"] is not None, (
        "regroup completed but workspace fingerprint timestamp was not stamped"
    )
    from pipeline import compute_group_fingerprint
    effective = db2.get_effective_config(cfg.load())
    assert row["last_group_fingerprint"] == compute_group_fingerprint(effective)



def test_pipeline_regroup_does_not_stamp_for_partial_run(tmp_path, monkeypatch):
    """A regroup run that filtered out workspace photos via exclude_photo_ids
    must NOT stamp last_group_fingerprint — those excluded photos are still
    ungrouped under the current settings, so claiming workspace-level
    freshness would let the pipeline page hide a real stale state."""
    import config as cfg
    from db import Database
    from PIL import Image

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    photo_dir = tmp_path / "photos"
    photo_dir.mkdir()
    for name in ("a.jpg", "b.jpg"):
        Image.new("RGB", (16, 16), "black").save(str(photo_dir / name))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    import pipeline as pipeline_mod

    def _ok_run(photos, config=None, emit_trace=False):
        return {"summary": {"groups": 1}, "photos": photos}

    monkeypatch.setattr(pipeline_mod, "run_full_pipeline", _ok_run)
    monkeypatch.setattr(pipeline_mod, "save_results",
                        lambda results, cache_dir, workspace_id: None)
    monkeypatch.setattr(
        pipeline_mod, "load_photo_features",
        lambda thread_db, collection_id=None, config=None: [{"id": 1}],
    )

    # Pass an arbitrary exclude_photo_ids — its mere presence signals that
    # the regroup ran on a filtered subset.
    params = PipelineParams(
        source=str(photo_dir),
        skip_classify=True,
        skip_extract_masks=True,
        exclude_photo_ids={999},
    )

    runner = FakeRunner()
    job = _make_job()

    with contextlib.suppress(Exception):
        run_pipeline_job(job, runner, db_path, ws_id, params)

    db2 = Database(db_path)
    db2.set_active_workspace(ws_id)
    row = db2.conn.execute(
        "SELECT last_grouped_at, last_group_fingerprint FROM workspaces WHERE id=?",
        (ws_id,),
    ).fetchone()
    assert row["last_grouped_at"] is None, (
        "partial regroup (exclude_photo_ids set) wrongly stamped "
        "last_grouped_at — pipeline page will falsely report 'fresh'"
    )
    assert row["last_group_fingerprint"] is None


def test_pipeline_regroup_invalidates_stamp_on_partial_run(tmp_path, monkeypatch):
    """A partial regroup overwrites the workspace's pipeline_results_ws*.json
    cache with subset output via save_results. Any pre-existing
    last_group_fingerprint would now point at a cache that no longer
    reflects the full workspace, so the pipeline page would falsely report
    Group as 'done-prior'. The stamp must be invalidated (NULL'd) on
    partial runs so pipeline_plan treats the resulting state as outdated."""
    import config as cfg
    from db import Database
    from PIL import Image

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    photo_dir = tmp_path / "photos"
    photo_dir.mkdir()
    for name in ("a.jpg", "b.jpg"):
        Image.new("RGB", (16, 16), "black").save(str(photo_dir / name))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    # Pre-stamp a fingerprint as if a prior FULL workspace regroup had
    # completed cleanly. The partial run we're about to do must wipe
    # this, since save_results will overwrite the cache with subset output.
    db.set_workspace_group_state(
        ws_id, fingerprint="pre-existing-from-full-run", when_ts=1714579200,
    )

    import pipeline as pipeline_mod
    monkeypatch.setattr(
        pipeline_mod, "run_full_pipeline",
        lambda photos, config=None, emit_trace=False: {"summary": {"groups": 1}, "photos": photos},
    )
    monkeypatch.setattr(pipeline_mod, "save_results",
                        lambda results, cache_dir, workspace_id: None)
    monkeypatch.setattr(
        pipeline_mod, "load_photo_features",
        lambda thread_db, collection_id=None, config=None: [{"id": 1}],
    )

    params = PipelineParams(
        source=str(photo_dir),
        skip_classify=True,
        skip_extract_masks=True,
        exclude_photo_ids={999},
    )

    runner = FakeRunner()
    job = _make_job()

    with contextlib.suppress(Exception):
        run_pipeline_job(job, runner, db_path, ws_id, params)

    db2 = Database(db_path)
    row = db2.conn.execute(
        "SELECT last_grouped_at, last_group_fingerprint FROM workspaces WHERE id=?",
        (ws_id,),
    ).fetchone()
    assert row["last_group_fingerprint"] is None, (
        "partial regroup left stale fingerprint behind — pipeline page would "
        "falsely report Group as 'done-prior' against a subset-only cache"
    )
    assert row["last_grouped_at"] is None


def test_pipeline_regroup_does_not_stamp_when_eye_override_differs(
    tmp_path, monkeypatch,
):
    """A per-run ``eye_detect_override`` that flips the effective
    ``eye_detect_enabled`` away from the workspace's own setting means the
    resulting KEEP/REJECT decisions came from settings the workspace's
    normal state would not reproduce. ``compute_group_fingerprint`` reads
    only encounter/burst keys, so stamping it would let a later default-off
    plan run against the workspace's real settings falsely report Group as
    'done-prior' against eye-scored results. Regression for Codex thread
    PRRT_kwDORn8c-s6QN0m3."""
    import config as cfg
    from db import Database
    from PIL import Image

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")
    # Workspace default: eye detection off. The Process-page checkbox on
    # this run flips it via ``eye_detect_override=True``.
    with open(cfg.CONFIG_PATH, "w") as f:
        json.dump({"pipeline": {"eye_detect_enabled": False}}, f)

    photo_dir = tmp_path / "photos"
    photo_dir.mkdir()
    for name in ("a.jpg", "b.jpg"):
        Image.new("RGB", (16, 16), "black").save(str(photo_dir / name))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    # Pre-stamp a fingerprint as if a prior FULL default-off regroup had
    # completed cleanly. The one-off eye-on run must NOT overwrite this
    # with a fresh stamp — that would lie about workspace freshness.
    db.set_workspace_group_state(
        ws_id, fingerprint="pre-existing-default-off", when_ts=1714579200,
    )

    import pipeline as pipeline_mod
    monkeypatch.setattr(
        pipeline_mod, "run_full_pipeline",
        lambda photos, config=None, emit_trace=False: {
            "summary": {"groups": 1}, "photos": photos,
        },
    )
    monkeypatch.setattr(
        pipeline_mod, "save_results",
        lambda results, cache_dir, workspace_id: None,
    )
    monkeypatch.setattr(
        pipeline_mod, "load_photo_features",
        lambda thread_db, collection_id=None, config=None: [{"id": 1}],
    )

    params = PipelineParams(
        source=str(photo_dir),
        skip_classify=True,
        skip_extract_masks=True,
        # Explicit per-run opt-in against workspace's eye-off default.
        eye_detect_override=True,
    )

    runner = FakeRunner()
    job = _make_job()

    with contextlib.suppress(Exception):
        run_pipeline_job(job, runner, db_path, ws_id, params)

    db2 = Database(db_path)
    row = db2.conn.execute(
        "SELECT last_grouped_at, last_group_fingerprint FROM workspaces WHERE id=?",
        (ws_id,),
    ).fetchone()
    assert row["last_group_fingerprint"] is None, (
        "one-off eye-on regroup against a default-off workspace stamped "
        "workspace freshness — a later default-off plan will falsely report "
        "'done-prior' against eye-scored KEEP/REJECT results"
    )
    assert row["last_grouped_at"] is None


def test_pipeline_regroup_stamps_when_eye_override_matches_workspace(
    tmp_path, monkeypatch,
):
    """The eye-override guard must only trigger when the override *differs*
    from the workspace's own eye setting — otherwise a Process-page run
    that ticks the checkbox in a workspace that already has eye detection
    on would be treated as partial and the plan would loop forever on
    'settings changed'."""
    import config as cfg
    from db import Database
    from PIL import Image

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")
    with open(cfg.CONFIG_PATH, "w") as f:
        json.dump({"pipeline": {"eye_detect_enabled": True}}, f)

    photo_dir = tmp_path / "photos"
    photo_dir.mkdir()
    for name in ("a.jpg", "b.jpg"):
        Image.new("RGB", (16, 16), "black").save(str(photo_dir / name))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    import pipeline as pipeline_mod
    monkeypatch.setattr(
        pipeline_mod, "run_full_pipeline",
        lambda photos, config=None, emit_trace=False: {
            "summary": {"groups": 1}, "photos": photos,
        },
    )
    monkeypatch.setattr(
        pipeline_mod, "save_results",
        lambda results, cache_dir, workspace_id: None,
    )
    monkeypatch.setattr(
        pipeline_mod, "load_photo_features",
        lambda thread_db, collection_id=None, config=None: [{"id": 1}],
    )

    params = PipelineParams(
        source=str(photo_dir),
        skip_classify=True,
        skip_extract_masks=True,
        eye_detect_override=True,
    )

    runner = FakeRunner()
    job = _make_job()

    with contextlib.suppress(Exception):
        run_pipeline_job(job, runner, db_path, ws_id, params)

    db2 = Database(db_path)
    db2.set_active_workspace(ws_id)
    row = db2.conn.execute(
        "SELECT last_grouped_at, last_group_fingerprint FROM workspaces WHERE id=?",
        (ws_id,),
    ).fetchone()
    assert row["last_grouped_at"] is not None, (
        "an eye_detect_override matching the workspace's own setting must "
        "still stamp workspace freshness — otherwise the plan would loop "
        "on 'settings changed'"
    )
    from pipeline import compute_group_fingerprint
    effective = db2.get_effective_config(cfg.load())
    assert row["last_group_fingerprint"] == compute_group_fingerprint(effective)



# --- Weighted overall progress ---------------------------------------------

def _empty_stages():
    return {name: {"status": "pending", "count": 0} for name in STAGE_WEIGHTS}


def test_stage_fraction_pending_is_zero():
    assert _stage_fraction({"status": "pending", "count": 0}) == 0.0


def test_stage_fraction_completed_is_one():
    assert _stage_fraction({"status": "completed", "count": 5, "total": 10}) == 1.0


def test_stage_fraction_skipped_is_one():
    """Skipped stages are "done" for overall-progress purposes — their
    weight has been paid out, so don't stall the bar at the last skip."""
    assert _stage_fraction({"status": "skipped"}) == 1.0


def test_stage_fraction_running_uses_count_over_total():
    assert _stage_fraction({"status": "running", "count": 25, "total": 100}) == 0.25


def test_stage_fraction_running_without_total_is_zero():
    """A running stage that hasn't yet reported a total can't compute a
    fraction; report 0 rather than dividing by zero or claiming completion."""
    assert _stage_fraction({"status": "running", "count": 5}) == 0.0


def test_stage_fraction_clamps_to_one():
    """Stage counters sometimes overshoot total (last batch rounding)."""
    assert _stage_fraction({"status": "running", "count": 105, "total": 100}) == 1.0


def test_stage_fraction_failed_counts_partial_work():
    """Stages like classify can process most items and then mark themselves
    'failed' due to per-item errors. Their partial completion must still
    count toward the weighted overall — otherwise the bar drops sharply
    when a near-done heavy stage fails."""
    assert _stage_fraction({"status": "failed", "count": 80, "total": 100}) == 0.8


def test_stage_fraction_failed_without_progress_is_zero():
    """A failed stage with no count/total contributes nothing, same as
    pending/unknown."""
    assert _stage_fraction({"status": "failed"}) == 0.0


def test_stage_fraction_failed_clamps_to_one():
    assert _stage_fraction({"status": "failed", "count": 105, "total": 100}) == 1.0


def test_weighted_progress_all_pending_is_zero():
    current, total = _weighted_progress(_empty_stages())
    assert current == 0
    assert total == sum(STAGE_WEIGHTS.values())


def test_weighted_progress_all_completed_is_full():
    stages = {name: {"status": "completed"} for name in STAGE_WEIGHTS}
    current, total = _weighted_progress(stages)
    assert current == total
    assert total == sum(STAGE_WEIGHTS.values())


def test_weighted_progress_fast_stage_done_heavy_pending():
    """After a fast stage finishes and a heavy one hasn't started, the bar
    should reflect the fast stage's small weight — NOT 100%. This is the
    bug the helper fixes: previously the last-pushed stage-local current/total
    dominated the overall bar."""
    stages = _empty_stages()
    stages["ingest"]["status"] = "completed"  # weight 2
    stages["scan"]["status"] = "completed"    # weight 8
    # classify (weight 30) still pending
    current, total = _weighted_progress(stages)
    pct = current / total * 100
    assert pct < 15, f"Expected <15% with only ingest+scan done, got {pct:.1f}%"


def test_weighted_progress_running_stage_partial():
    stages = _empty_stages()
    stages["ingest"]["status"] = "completed"
    stages["scan"]["status"] = "completed"
    stages["thumbnails"]["status"] = "completed"
    stages["previews"]["status"] = "completed"
    stages["model_loader"]["status"] = "completed"
    stages["detect"]["status"] = "completed"
    stages["classify"].update(status="running", count=50, total=100)
    current, total = _weighted_progress(stages)
    # ingest+scan+thumbs+previews+model_loader+detect = 2+8+6+6+2+15 = 39
    # classify half-done = 15
    # total weight sum via STAGE_WEIGHTS
    expected_done = 39 + 15
    assert current == expected_done
    assert total == sum(STAGE_WEIGHTS.values())


def test_weighted_progress_does_not_round_up_to_full():
    """Overall must not report `current == total` before every stage is
    actually complete. int(round(done)) would report 100/100 when done is
    99.5+, falsely showing 100% while a stage is still running."""
    stages = _empty_stages()
    for name in STAGE_WEIGHTS:
        stages[name]["status"] = "completed"
    # Override the last stage to running at 99/100. Contribution = 5.94
    # (weight 6 * 0.99); others fully completed = 94. Total done = 99.94.
    # A naive round(99.94) = 100 would hit total and falsely signal done.
    stages["regroup"].update(status="running", count=99, total=100)
    current, total = _weighted_progress(stages)
    assert current < total, (
        f"overall hit total ({current}/{total}) before last stage completed"
    )


def test_weighted_progress_does_not_round_up_with_failed_stage():
    """Same premature-100 guard, but via a failed stage that finished
    processing most items. If failed now counts partial work, the weighted
    sum can land at 99.x when only one stage hasn't fully completed."""
    stages = _empty_stages()
    for name in STAGE_WEIGHTS:
        stages[name]["status"] = "completed"
    stages["regroup"].update(status="failed", count=99, total=100)
    current, total = _weighted_progress(stages)
    assert current < total, (
        f"overall hit total ({current}/{total}) with a non-complete stage"
    )


def test_weighted_progress_monotonic_through_pipeline():
    """Completing stages in order should produce a monotonically increasing
    overall percentage — no drops between phases."""
    stages = _empty_stages()
    order = ["ingest", "scan", "thumbnails", "previews", "model_loader",
             "detect", "classify", "extract_masks", "eye_keypoints", "regroup",
             "misses"]
    last_pct = -1.0
    for name in order:
        stages[name]["status"] = "completed"
        current, total = _weighted_progress(stages)
        pct = current / total * 100
        assert pct > last_pct, f"Progress went backwards at {name}: {last_pct} -> {pct}"
        last_pct = pct
    assert last_pct == 100.0


def test_pipeline_thumbnail_stage_records_thumb_path_in_db(tmp_path, monkeypatch):
    """Each successful generate_thumbnail in the pipeline thumbnail_stage must
    set photos.thumb_path so the dashboard's coverage query reflects it.
    Without this, scanning produces JPEGs on disk but the column stays NULL
    and "0 of N thumbnails made" is reported forever."""
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")
    photo_dir = _make_photo_dir(tmp_path, 3)

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    params = PipelineParams(
        source=str(photo_dir),
        skip_classify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )
    job = _make_job()
    runner = FakeRunner()

    with contextlib.suppress(RuntimeError):
        run_pipeline_job(job, runner, db_path, ws_id, params)

    # Re-open the DB connection — the pipeline runs on a thread with its
    # own connection and we want the committed view.
    db2 = Database(db_path)
    rows = db2.conn.execute(
        "SELECT id, thumb_path FROM photos ORDER BY id"
    ).fetchall()
    assert len(rows) == 3
    for r in rows:
        assert r["thumb_path"] is not None, (
            f"photo {r['id']} has thumb_path=NULL after pipeline ran"
        )
        assert r["thumb_path"] == f"{r['id']}.jpg", (
            f"thumb_path should be the bare filename '{r['id']}.jpg', "
            f"got {r['thumb_path']!r}"
        )


def test_update_stages_emits_weighted_current_total():
    """_update_stages must send the weighted overall to push_event instead
    of hardcoded 0/0. This is what makes the 'Overall %' visible in the UI."""
    from pipeline_job import _update_stages

    stages = _empty_stages()
    stages["ingest"]["status"] = "completed"
    stages["scan"]["status"] = "running"
    stages["scan"]["count"] = 50
    stages["scan"]["total"] = 100

    runner = FakeRunner()
    _update_stages(runner, "job-x", stages)
    assert runner.events, "no events pushed"
    _, evt, data = runner.events[-1]
    assert evt == "progress"
    assert data["total"] == sum(STAGE_WEIGHTS.values())
    # ingest (2) + scan half (4) = 6
    assert data["current"] == 6


def test_progress_event_defaults_phase_triple_to_none():
    """Every progress payload must carry an explicit phase_current /
    phase_total / phase_label triple. JobRunner.push_event merges progress
    payloads into job['progress'] rather than replacing them, so a scan
    sub-phase (e.g. 'Extracting metadata' with numeric phase_current /
    phase_total) would linger across every later status emission that
    omits these keys — the jobs page and navbar would keep rendering the
    stale 'Extracting metadata' bar through 'Hashing ...' and downstream
    pipeline stages. Callers with no active sub-phase must therefore emit
    None so the merge actively clears the previous values."""
    from pipeline_job import _progress_event

    stages = _empty_stages()
    stages["scan"]["status"] = "running"

    # No phase_* passed → the triple defaults to None and reaches the payload
    # so the frontend's `typeof phase_current === 'number' && phase_total > 0`
    # guard falls back to overall progress instead of re-rendering a stale bar.
    data = _progress_event(stages, "scan", "Hashing 3 files (2 workers)...")
    assert data["phase_current"] is None
    assert data["phase_total"] is None
    assert data["phase_label"] is None

    # Callers with an active sub-phase override the defaults.
    data = _progress_event(
        stages, "scan", "Extracting metadata (2 / 3 files)...",
        phase_current=2,
        phase_total=3,
        phase_label="Extracting metadata",
    )
    assert data["phase_current"] == 2
    assert data["phase_total"] == 3
    assert data["phase_label"] == "Extracting metadata"


def test_emit_progress_clears_stale_phase_in_merged_job_progress():
    """End-to-end: JobRunner.push_event merges into job['progress']; after a
    scan sub-phase emission followed by a plain status emission, the merged
    progress dict must show the phase triple cleared to None so /api/jobs
    polling stops rendering the stale metadata bar."""
    import queue as _queue
    import sys as _sys

    _sys.path.insert(0, os.path.join(os.path.dirname(__file__)))
    from jobs import JobRunner
    from pipeline_job import _emit_progress
    from wait import wait_for_job_via_runner

    runner = JobRunner()
    seen_progress = _queue.Queue()

    def work(job):
        stages = _empty_stages()
        stages["scan"]["status"] = "running"

        _emit_progress(
            runner, job["id"], stages, "scan",
            "Extracting metadata (2 / 3 files)...",
            phase_current=2,
            phase_total=3,
            phase_label="Extracting metadata",
        )
        seen_progress.put(dict(job["progress"]))

        _emit_progress(
            runner, job["id"], stages, "scan",
            "Hashing 3 files (2 workers)...",
        )
        seen_progress.put(dict(job["progress"]))

        return {"ok": True}

    job_id = runner.start("scan", work)
    wait_for_job_via_runner(runner, job_id)

    metadata_prog = seen_progress.get_nowait()
    assert metadata_prog["phase_current"] == 2
    assert metadata_prog["phase_total"] == 3
    assert metadata_prog["phase_label"] == "Extracting metadata"

    hashing_prog = seen_progress.get_nowait()
    assert hashing_prog["phase_current"] is None
    assert hashing_prog["phase_total"] is None
    assert hashing_prog["phase_label"] is None


# ---------------------------------------------------------------------------
# Cancel responsiveness in extract_masks / eye_keypoints
#
# PR #710 fixed mid-batch cancel for the classify stage. The same hang shape
# (cancel takes minutes, stage finalizes as plain "completed") still affected
# extract_masks and eye_keypoints. These tests pin the corrected behavior:
#   - The per-photo loop in extract_masks breaks promptly on abort.
#   - extract_masks finalizes with a "Cancelled (X of N processed)" summary.
#   - eye_keypoints finalizes with a "Cancelled" summary, not the
#     misleading default "X of N photos processed".
#   - detect_eye_keypoints_stage in pipeline.py honors an abort_check
#     callable so a stuck mid-stage cancel can take effect within one
#     keypoint inference, not at end of stage.
# ---------------------------------------------------------------------------


def _stub_extract_masks_heavy_ops(monkeypatch):
    """Stub the SAM2 + DINOv2 helpers extract_masks_stage imports so the loop
    body runs in microseconds. Returns a dict with the proxy-call counter so
    the test can assert how many photos the loop touched.
    """
    import dino_embed
    import masking
    import numpy as np
    import quality
    from db import Database

    state = {"proxy_calls": 0}

    def fake_render_proxy(image_path, longest_edge=None):
        state["proxy_calls"] += 1
        return np.zeros((16, 16, 3), dtype=np.uint8)

    monkeypatch.setattr(masking, "render_proxy", fake_render_proxy)
    monkeypatch.setattr(
        masking, "generate_mask",
        lambda *a, **k: np.ones((16, 16), dtype=np.uint8),
    )
    def fake_save_mask(mask, dir_, pid_, variant):
        path = os.path.join(dir_, f"{pid_}.{variant}.png")
        os.makedirs(dir_, exist_ok=True)
        with open(path, "wb") as handle:
            handle.write(b"mask")
        return path

    monkeypatch.setattr(masking, "save_mask", fake_save_mask)
    monkeypatch.setattr(masking, "crop_completeness", lambda m: 1.0)
    monkeypatch.setattr(masking, "crop_subject", lambda p, m, margin=0.15: None)
    monkeypatch.setattr(masking, "ensure_sam2_weights", lambda **k: None)
    monkeypatch.setattr(quality, "compute_all_quality_features", lambda p, m: {})
    monkeypatch.setattr(
        dino_embed, "embed",
        lambda p, variant=None: np.zeros(384, dtype=np.float32),
    )
    monkeypatch.setattr(
        dino_embed, "embed_batch",
        lambda imgs, variant=None: np.zeros((len(imgs), 384), dtype=np.float32),
    )
    monkeypatch.setattr(dino_embed, "embedding_to_blob", lambda e: b"")
    monkeypatch.setattr(dino_embed, "ensure_dinov2_weights", lambda **k: None)
    monkeypatch.setattr(
        Database, "update_photo_pipeline_features",
        lambda self, *a, **k: None,
    )
    monkeypatch.setattr(
        Database, "update_photo_embeddings",
        lambda self, *a, **k: None,
    )
    return state


@pytest.mark.parametrize(
    ("right_species", "expected_proxy_calls"),
    (("Great-tailed Grackle", 3), ("Brown-headed Cowbird", 2)),
)
def test_extract_masks_stage_gates_weak_detection_on_matching_anchor_species(
    tmp_path, monkeypatch, right_species, expected_proxy_calls,
):
    """Only a species-validated weak box enters the SAM mask worklist.

    Classification already lowers its crop floor for a bracketed weak frame;
    mask extraction must also apply grouping's matching-anchor-species gate.
    """
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)

    photo_ids = []
    detection_ids = []
    for index, confidence in enumerate((0.9, 0.18, 0.9)):
        filename = f"bird{index}.jpg"
        photo_id = db.add_photo(
            folder_id, filename, ".jpg", 1000, 1_000_000.0 + index,
            timestamp=f"2026-07-18T08:36:3{index}",
        )
        _drop_jpeg(folder_path, filename)
        detection_id = db.write_detection_batch(
            photo_id,
            "megadetector-v6",
            [{
                "box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
                "confidence": confidence,
                "category": "animal",
            }],
        )[0]
        photo_ids.append(photo_id)
        detection_ids.append(detection_id)

    db.add_prediction(
        detection_ids[0], "Great-tailed Grackle", 0.9, "inat21",
    )
    db.add_prediction(
        detection_ids[-1], right_species, 0.9, "inat21",
    )

    collection_id = db.add_collection(
        "Weak mask bridge",
        json.dumps([{"field": "photo_ids", "value": photo_ids}]),
    )
    state = _stub_extract_masks_heavy_ops(monkeypatch)

    params = PipelineParams(
        collection_id=collection_id,
        skip_classify=True,
        skip_extract_masks=False,
        skip_regroup=True,
    )
    runner = FakeRunner()
    run_pipeline_job(_make_job(), runner, db_path, ws_id, params)

    assert state["proxy_calls"] == expected_proxy_calls


def test_pipeline_extract_masks_cancel_marks_stage_cancelled(
    tmp_path, monkeypatch,
):
    """An abort triggered during extract_masks must finalize the stage with
    a 'Cancelled (X of N processed)' summary, not as plain 'completed' (or
    'failed') as if the full set was processed.

    Pre-fix shape: stages["extract_masks"]["status"] was unconditionally set
    to "completed" or "failed" based only on em_failed, regardless of
    abort. The user saw a green "completed" summary on a stage that had
    only processed 173 of 11,285 photos.
    """
    import config as cfg
    import pipeline_job as pj
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)

    photo_ids = []
    for i in range(3):
        name = f"photo{i}.jpg"
        pid = db.add_photo(
            folder_id, name, ".jpg", 1000 + i, 1_000_000.0 + i,
        )
        _drop_jpeg(folder_path, name)
        db.save_detections(
            pid,
            [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
              "confidence": 0.9, "category": "animal"}],
            detector_model="MegaDetector",
        )
        photo_ids.append(pid)

    col_id = db.add_collection(
        "Test",
        json.dumps([{"field": "photo_ids", "value": photo_ids}]),
    )

    state = _stub_extract_masks_heavy_ops(monkeypatch)

    # Trigger abort once the first photo's render_proxy fires. The next
    # iteration's top-of-loop abort check (and the new intra-photo checks)
    # must catch it before any further photo-level work runs.
    abort_after_first = threading.Event()
    real_render = state.get("real_render")  # placeholder; we override below

    import masking
    import numpy as np

    def render_then_abort(image_path, longest_edge=None):
        state["proxy_calls"] += 1
        if state["proxy_calls"] == 1:
            abort_after_first.set()
        return np.zeros((16, 16, 3), dtype=np.uint8)

    state["proxy_calls"] = 0  # reset for the override
    monkeypatch.setattr(masking, "render_proxy", render_then_abort)

    original_should_abort = pj._should_abort

    def patched_should_abort(event):
        if abort_after_first.is_set():
            return True
        return original_should_abort(event)

    monkeypatch.setattr(pj, "_should_abort", patched_should_abort)

    params = PipelineParams(
        collection_id=col_id,
        skip_classify=True,
        skip_extract_masks=False,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()
    run_pipeline_job(job, runner, db_path, ws_id, params)

    # Without the intra-photo / top-of-loop abort honoring, all 3 photos
    # would render their proxies. With the fix, the loop bails after photo 1
    # (and at most one more iteration if abort lands between sub-steps).
    assert 1 <= state["proxy_calls"] <= 2, (
        f"Expected extract_masks to stop within ~1 photo of abort; got "
        f"{state['proxy_calls']} render_proxy calls."
    )

    # The final extract_masks step update must carry a 'Cancelled' summary,
    # not the default 'X masked, Y skipped'.
    em_finals = [
        kw for (_, sid, kw) in runner.step_updates
        if sid == "extract_masks" and kw.get("status") in (
            "completed", "failed",
        ) and "summary" in kw
    ]
    assert em_finals, (
        f"Expected at least one final extract_masks update; got "
        f"step_updates={runner.step_updates!r}"
    )
    final_kw = em_finals[-1]
    final_summary = final_kw.get("summary") or ""
    assert "Cancelled" in final_summary, (
        f"extract_masks final summary must reflect cancellation; got "
        f"{final_summary!r}"
    )
    # The status must NOT be 'failed' on a clean cancel — failure status
    # would inflate the job rollup's error count.
    assert final_kw.get("status") == "completed", (
        f"Cancelled extract_masks should finalize as 'completed'; got "
        f"status={final_kw.get('status')!r}"
    )


# ---------------------------------------------------------------------------
# extract_masks unreadable-source accounting
#
# ``render_proxy`` returning None means the source file could not be read.
# That was counted in the same ``skipped`` bucket as "SAM found no subject",
# so a share that dropped mid-run finalized the stage as a clean "completed"
# with "N masked, M skipped" — and every unmasked photo then got hard-rejected
# downstream by scoring's ``no_subject_mask`` rule with nothing in the job tree
# explaining why. Production hit this: an SMB mount died 10.5 hours into a run
# and 311 of 706 photos were silently left unmasked.
# ---------------------------------------------------------------------------


def _add_photo_with_detection(db, folder_id, folder_path, filename):
    """Add a photo + an animal detection so it enters the mask worklist."""
    photo_id = db.add_photo(
        folder_id, filename, ".jpg", 1000, 1_000_000.0 + hash(filename) % 1000,
    )
    _drop_jpeg(folder_path, filename)
    db.save_detections(
        photo_id,
        [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
          "confidence": 0.9, "category": "animal"}],
        detector_model="MegaDetector",
    )
    return photo_id


def _extract_masks_final_update(runner):
    """The last extract_masks step update carrying a terminal status."""
    finals = [
        kw for (_, sid, kw) in runner.step_updates
        if sid == "extract_masks"
        and kw.get("status") in ("completed", "failed")
        and "summary" in kw
    ]
    assert finals, (
        f"Expected a final extract_masks update; got {runner.step_updates!r}"
    )
    return finals[-1]


def _run_extract_masks_only(db_path, ws_id, collection_id):
    """Run a mask-only pipeline and return ``(runner, result)``.

    A failed stage makes ``run_pipeline_job`` raise at the end of the run
    after stashing the structured result on the job, so the caller still
    gets the per-stage counters either way.
    """
    runner = FakeRunner()
    job = _make_job()
    params = PipelineParams(
        collection_id=collection_id,
        skip_classify=True,
        skip_extract_masks=False,
        skip_regroup=True,
    )
    try:
        result = run_pipeline_job(job, runner, db_path, ws_id, params)
    except RuntimeError:
        result = job["result"]
    return runner, result


def test_extract_masks_source_lost_midrun_fails_stage(tmp_path, monkeypatch):
    """A source that goes away mid-loop must fail the stage, not complete.

    Pre-fix shape: every remaining photo's ``render_proxy`` returned None,
    each landing in ``skipped``, and the finalizer saw ``em_failed == 0`` and
    reported "completed". The user's only signal that two-thirds of the
    library was never masked was a skipped count indistinguishable from
    "SAM found no subject here".
    """
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)

    photo_ids = [
        _add_photo_with_detection(db, folder_id, folder_path, f"bird{i}.jpg")
        for i in range(3)
    ]
    collection_id = db.add_collection(
        "Lost source",
        json.dumps([{"field": "photo_ids", "value": photo_ids}]),
    )

    state = _stub_extract_masks_heavy_ops(monkeypatch)

    import shutil

    import masking

    def render_then_lose_source(image_path, longest_edge=None):
        state["proxy_calls"] += 1
        # The share drops on the first read — as an unmounted volume does,
        # every later read against this folder would fail the same way.
        shutil.rmtree(folder_path, ignore_errors=True)
        return None

    state["proxy_calls"] = 0
    monkeypatch.setattr(masking, "render_proxy", render_then_lose_source)

    runner, result = _run_extract_masks_only(db_path, ws_id, collection_id)

    em = result["stages"]["extract_masks"]
    assert em["masked"] == 0
    # All three photos must be accounted for as unreadable, not as benign
    # skips — including the two the stage never re-probed.
    assert em.get("unreadable") == 3, (
        f"Expected 3 photos reported unreadable; got {em!r}"
    )
    assert em["skipped"] == 0, (
        f"Unreadable photos must not land in the benign skip bucket; got {em!r}"
    )
    # Once the folder is known dead, stop reissuing reads against it.
    assert state["proxy_calls"] == 1, (
        f"Expected the stage to stop probing the dead source after the first "
        f"failed read; got {state['proxy_calls']} render_proxy calls."
    )

    final = _extract_masks_final_update(runner)
    assert final["status"] == "failed", (
        f"A stage that masked nothing because the source vanished must not "
        f"finalize as completed; got {final!r}"
    )
    assert "unreadable" in (final.get("summary") or "").lower(), (
        f"Stage summary must name the unreadable photos; got "
        f"{final.get('summary')!r}"
    )
    assert any(
        "extract_masks" in err and "unreachable" in err.lower()
        for err in result["errors"]
    ), (
        f"Expected an extract_masks source-unreachable error in the job "
        f"rollup; got {result['errors']!r}"
    )


def test_extract_masks_offline_folder_does_not_stop_healthy_folder(
    tmp_path, monkeypatch,
):
    """One dead folder must not strand photos in the collection's other,
    still-reachable folders — the outage is folder-scoped, so the stage keeps
    working through the rest and reports the unreadable ones."""
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    dead_path = str(tmp_path / "dead")
    live_path = str(tmp_path / "live")
    os.makedirs(dead_path, exist_ok=True)
    os.makedirs(live_path, exist_ok=True)
    dead_folder = db.add_folder(dead_path)
    live_folder = db.add_folder(live_path)

    photo_ids = [
        _add_photo_with_detection(db, dead_folder, dead_path, "gone0.jpg"),
        _add_photo_with_detection(db, dead_folder, dead_path, "gone1.jpg"),
        _add_photo_with_detection(db, live_folder, live_path, "here0.jpg"),
    ]
    collection_id = db.add_collection(
        "Mixed reachability",
        json.dumps([{"field": "photo_ids", "value": photo_ids}]),
    )

    state = _stub_extract_masks_heavy_ops(monkeypatch)

    import shutil

    import masking
    import numpy as np

    def render_scoped(image_path, longest_edge=None):
        state["proxy_calls"] += 1
        if image_path.startswith(dead_path):
            shutil.rmtree(dead_path, ignore_errors=True)
            return None
        return np.zeros((16, 16, 3), dtype=np.uint8)

    state["proxy_calls"] = 0
    monkeypatch.setattr(masking, "render_proxy", render_scoped)

    runner, result = _run_extract_masks_only(db_path, ws_id, collection_id)

    em = result["stages"]["extract_masks"]
    assert em["masked"] == 1, (
        f"The reachable folder's photo must still be masked; got {em!r}"
    )
    assert em.get("unreadable") == 2, (
        f"Both photos in the dead folder must be reported unreadable; got "
        f"{em!r}"
    )
    final = _extract_masks_final_update(runner)
    assert final["status"] == "failed", (
        f"A partial outage still has to surface as a stage failure; got "
        f"{final!r}"
    )


def test_extract_masks_mount_outage_still_processes_other_sources(
    tmp_path, monkeypatch,
):
    """A dead volume must not strand photos that live somewhere else.

    A mount-scoped outage proves the photos *on that volume* are gone, not
    the rest of the worklist. A collection can span several volumes plus the
    local disk, so writing off everything still unprocessed would both skip
    masks that could have been made and file those photos under an outage
    they were never affected by (Codex #1392 P1).
    """
    import config as cfg
    import pipeline_job as pj
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    # Names chosen so the share sorts first: the worklist is ordered by
    # path, and a stage that abandons the whole worklist after the outage
    # must be observably unable to reach the local photo.
    local_path = str(tmp_path / "zz_local")
    share_path = str(tmp_path / "aa_share")
    os.makedirs(local_path, exist_ok=True)
    os.makedirs(share_path, exist_ok=True)
    local_folder = db.add_folder(local_path)
    share_folder = db.add_folder(share_path)

    photo_ids = [
        _add_photo_with_detection(db, local_folder, local_path, "zz0.jpg"),
        _add_photo_with_detection(db, share_folder, share_path, "aa0.jpg"),
        _add_photo_with_detection(db, share_folder, share_path, "aa1.jpg"),
    ]
    collection_id = db.add_collection(
        "Volume plus local disk",
        json.dumps([{"field": "photo_ids", "value": photo_ids}]),
    )

    state = _stub_extract_masks_heavy_ops(monkeypatch)

    import masking
    import numpy as np

    read_order = []

    def render_dead_share(image_path, longest_edge=None):
        state["proxy_calls"] += 1
        read_order.append(image_path)
        if image_path.startswith(share_path):
            return None
        return np.zeros((16, 16, 3), dtype=np.uint8)

    state["proxy_calls"] = 0
    monkeypatch.setattr(masking, "render_proxy", render_dead_share)
    # Only the share is gone; the local disk is fine.
    monkeypatch.setattr(
        pj, "_source_offline_reason",
        lambda folder_path, image_path: (
            ("mount", "volume /Volumes/Photography is not mounted")
            if image_path.startswith(share_path) else None
        ),
    )

    runner, result = _run_extract_masks_only(db_path, ws_id, collection_id)

    # Guard the premise: if the worklist ever stops leading with the share,
    # the masked-count assertion below silently stops discriminating.
    assert read_order and read_order[0].startswith(share_path), (
        f"This test needs the share to be read first; got {read_order!r}"
    )

    em = result["stages"]["extract_masks"]
    assert em["masked"] == 1, (
        f"The photo on the healthy local disk must still be masked; got {em!r}"
    )
    assert em.get("unreadable") == 2, (
        f"Only the two photos on the dead volume are unreadable; got {em!r}"
    )
    # One failed read proves the share is gone; its second photo must not
    # cost another round trip to a dead server.
    assert state["proxy_calls"] == 2, (
        f"Expected one failed read on the share plus one good local read; "
        f"got {state['proxy_calls']} render_proxy calls."
    )
    final = _extract_masks_final_update(runner)
    assert final["status"] == "failed"
    assert any(
        "/Volumes/Photography" in err for err in result["errors"]
    ), (
        f"The rollup must name the volume the user has to reconnect; got "
        f"{result['errors']!r}"
    )


def test_extract_masks_offline_folder_still_counts_cached_masks(
    tmp_path, monkeypatch,
):
    """A photo whose mask is already cached needs no source read, so a dead
    folder must not relabel it unreadable.

    The offline shortcut has to sit after the `photo_masks` cache check:
    the cached branch only stats the local mask file, so it succeeds even
    when the source volume is gone (Codex #1392 P2).
    """
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)

    uncached = _add_photo_with_detection(db, folder_id, folder_path, "a.jpg")
    cached = _add_photo_with_detection(db, folder_id, folder_path, "b.jpg")
    collection_id = db.add_collection(
        "Partly cached",
        json.dumps([{"field": "photo_ids", "value": [uncached, cached]}]),
    )

    pipeline_cfg = db.get_effective_config(cfg.load()).get("pipeline", {})
    sam2_variant = pipeline_cfg.get("sam2_variant")
    dinov2_variant = pipeline_cfg.get("dinov2_variant")

    # Seed a complete, matching cache entry for `cached`: mask row, mask
    # file on local disk, and photos-row variants already consistent.
    mask_dir = tmp_path / ".vireo" / "masks"
    os.makedirs(mask_dir, exist_ok=True)
    mask_file = str(mask_dir / f"{cached}.{sam2_variant}.png")
    from PIL import Image
    Image.new("L", (4, 4), 255).save(mask_file)
    db.upsert_photo_mask(
        cached, sam2_variant, mask_file, "MegaDetector",
        0.1, 0.1, 0.5, 0.5,
    )
    db.set_active_mask_variant(cached, sam2_variant)
    db.conn.execute(
        "UPDATE photos SET dino_embedding_variant=? WHERE id=?",
        (dinov2_variant, cached),
    )
    db.conn.commit()

    state = _stub_extract_masks_heavy_ops(monkeypatch)

    import shutil

    import masking

    def render_then_lose_source(image_path, longest_edge=None):
        state["proxy_calls"] += 1
        shutil.rmtree(folder_path, ignore_errors=True)
        return None

    state["proxy_calls"] = 0
    monkeypatch.setattr(masking, "render_proxy", render_then_lose_source)

    runner, result = _run_extract_masks_only(db_path, ws_id, collection_id)

    em = result["stages"]["extract_masks"]
    assert em["masked"] == 1, (
        f"The cached photo needs no source read and must still count as "
        f"masked; got {em!r}"
    )
    assert em.get("unreadable") == 1, (
        f"Only the uncached photo is unreadable; got {em!r}"
    )


def test_extract_masks_preflight_offline_photos_are_counted_unreadable(
    tmp_path, monkeypatch,
):
    """Photos dropped by the pre-flight offline probe still have to show up
    in the stage's counters.

    The pre-flight removes them from the worklist before the loop, so every
    counter came back zero on a stage the user was simultaneously being told
    had failed with unreachable photos — which the Extract card rendered as
    "No photos needed masks" (Codex #1392 P2).
    """
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)

    photo_ids = [
        _add_photo_with_detection(db, folder_id, folder_path, f"bird{i}.jpg")
        for i in range(3)
    ]
    collection_id = db.add_collection(
        "Gone before we started",
        json.dumps([{"field": "photo_ids", "value": photo_ids}]),
    )

    _stub_extract_masks_heavy_ops(monkeypatch)

    # The source is already gone when the stage starts, so the pre-flight
    # probe drops all three before the per-photo loop runs.
    import shutil
    shutil.rmtree(folder_path, ignore_errors=True)

    runner, result = _run_extract_masks_only(db_path, ws_id, collection_id)

    em = result["stages"]["extract_masks"]
    assert em.get("unreadable") == 3, (
        f"Pre-flight-dropped photos are unreadable, not absent; got {em!r}"
    )
    # The reported total has to cover the photos the pre-flight removed, or
    # the stage publishes impossible counters like "3 unreadable of 0" and
    # any consumer computing coverage from them is wrong (Codex #1392 P2).
    assert em["total"] == 3, (
        f"Total must count the pre-flight-dropped candidates; got {em!r}"
    )
    final = _extract_masks_final_update(runner)
    assert final["status"] == "failed"
    assert "3 unreadable" in (final.get("summary") or ""), (
        f"The summary must not read as an empty worklist; got "
        f"{final.get('summary')!r}"
    )


def test_extract_masks_single_unreadable_file_is_reported_not_skipped(
    tmp_path, monkeypatch,
):
    """A corrupt file in an otherwise healthy folder is a per-photo failure:
    the stage works through the rest, but the photo is reported as unreadable
    rather than counted as a benign "no subject found" skip."""
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)

    photo_ids = [
        _add_photo_with_detection(db, folder_id, folder_path, f"bird{i}.jpg")
        for i in range(3)
    ]
    collection_id = db.add_collection(
        "One corrupt file",
        json.dumps([{"field": "photo_ids", "value": photo_ids}]),
    )

    state = _stub_extract_masks_heavy_ops(monkeypatch)

    import masking
    import numpy as np

    def render_one_bad_file(image_path, longest_edge=None):
        state["proxy_calls"] += 1
        if image_path.endswith("bird1.jpg"):
            return None
        return np.zeros((16, 16, 3), dtype=np.uint8)

    state["proxy_calls"] = 0
    monkeypatch.setattr(masking, "render_proxy", render_one_bad_file)

    runner, result = _run_extract_masks_only(db_path, ws_id, collection_id)

    em = result["stages"]["extract_masks"]
    assert em["masked"] == 2, (
        f"A single bad file must not stop the healthy photos; got {em!r}"
    )
    assert em.get("unreadable") == 1, (
        f"The unreadable photo must be reported as such; got {em!r}"
    )
    assert em["skipped"] == 0, (
        f"An unreadable file is not a benign skip; got {em!r}"
    )
    assert state["proxy_calls"] == 3, (
        f"Every photo should still have been attempted; got "
        f"{state['proxy_calls']}"
    )


def test_extract_masks_no_subject_still_counts_as_benign_skip(
    tmp_path, monkeypatch,
):
    """Guard against over-correction: SAM returning no mask for a readable
    photo means "no subject found here", which is a legitimate outcome. It
    stays in ``skipped`` and leaves the stage completed."""
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)

    photo_ids = [
        _add_photo_with_detection(db, folder_id, folder_path, f"bird{i}.jpg")
        for i in range(2)
    ]
    collection_id = db.add_collection(
        "No subject",
        json.dumps([{"field": "photo_ids", "value": photo_ids}]),
    )

    _stub_extract_masks_heavy_ops(monkeypatch)

    import masking
    monkeypatch.setattr(masking, "generate_mask", lambda *a, **k: None)

    runner, result = _run_extract_masks_only(db_path, ws_id, collection_id)

    em = result["stages"]["extract_masks"]
    assert em["masked"] == 0
    assert em["skipped"] == 2, (
        f"A readable photo with no SAM mask stays a benign skip; got {em!r}"
    )
    assert em.get("unreadable", 0) == 0, (
        f"Nothing was unreadable here; got {em!r}"
    )
    final = _extract_masks_final_update(runner)
    assert final["status"] == "completed", (
        f"No-subject skips must not fail the stage; got {final!r}"
    )


# ---------------------------------------------------------------------------
# extract_masks per-variant cache (photo_masks)
#
# Phase 2 of the SAM mask history plan stops the masking stage from re-running
# SAM when a row already exists in `photo_masks` for (photo, configured
# variant) AND its stored prompt + detector still matches the photo's current
# primary detection.  Three regression tests pin the contract:
#
#   - cached_with_same_prompt → generate_mask NOT called, photo_masks
#     unchanged, masked counter still increments (cache hit is a successful
#     outcome, not a "skipped" SAM failure).
#   - variant_differs → switching `pipeline.sam2_variant` re-runs SAM, leaves
#     the previous variant's row in place, adds a new row.
#   - prompt_changed → if the detection bbox shifts (e.g. YOLO re-run with a
#     different threshold), the cached row is replaced with the new prompt.
# ---------------------------------------------------------------------------


def _run_extract_masks_for_test(
    tmp_path, monkeypatch, sam2_variant, photo_specs,
    *, runner=None, on_generate_mask=None,
):
    """Drive a single pipeline run with extract_masks enabled and the heavy
    SAM2/DINOv2 calls stubbed.  Returns (db, runner, generate_mask_calls)
    where generate_mask_calls is a list of (photo_id, variant) tuples
    capturing every call the patched generate_mask saw.

    `photo_specs` is a list of dicts:
        [{"filename": "a.jpg", "box": (x, y, w, h), "model": "MegaDetector"}]

    Each photo gets a unique 1x1 mask whose pixel pattern depends on
    photo_id, so different photos cannot accidentally collide.
    """
    import config as cfg
    import dino_embed
    import masking
    import numpy as np
    import quality
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    cfg.save({
        "pipeline": {"sam2_variant": sam2_variant, "dinov2_variant": "vit-b14"},
    })

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)

    photo_ids = []
    for spec in photo_specs:
        pid = db.add_photo(folder_id, spec["filename"], ".jpg", 1000, 1.0)
        _drop_jpeg(folder_path, spec["filename"])
        x, y, w, h = spec["box"]
        db.save_detections(
            pid,
            [{"box": {"x": x, "y": y, "w": w, "h": h},
              "confidence": 0.9, "category": "animal"}],
            detector_model=spec["model"],
        )
        photo_ids.append(pid)

    col_id = db.add_collection(
        "Test", json.dumps([{"field": "photo_ids", "value": photo_ids}]),
    )

    generate_mask_calls = []

    def fake_render_proxy(image_path, longest_edge=None):
        return np.zeros((4, 4, 3), dtype=np.uint8)

    def fake_generate_mask(proxy, det_box, variant=None):
        # Track every (variant, det_box) we get asked about — the cache
        # short-circuit must skip past this entirely on a hit.
        generate_mask_calls.append((variant, tuple(sorted(det_box.items()))))
        if on_generate_mask is not None:
            on_generate_mask()
        return np.ones((4, 4), dtype=bool)

    monkeypatch.setattr(masking, "render_proxy", fake_render_proxy)
    monkeypatch.setattr(masking, "generate_mask", fake_generate_mask)
    monkeypatch.setattr(masking, "crop_completeness", lambda m: 0.9)
    monkeypatch.setattr(masking, "crop_subject", lambda p, m, margin=0.15: None)
    monkeypatch.setattr(masking, "ensure_sam2_weights", lambda **k: None)
    monkeypatch.setattr(
        quality, "compute_all_quality_features",
        lambda p, m: {
            "subject_tenengrad": 1.5,
            "bg_tenengrad": 0.3,
            "subject_clip_high": 0.01,
            "subject_clip_low": 0.01,
            "subject_y_median": 100.0,
            "bg_separation": 50.0,
            "phash_crop": "deadbeef",
            "noise_estimate": 5.0,
        },
    )
    monkeypatch.setattr(
        dino_embed, "embed",
        lambda p, variant=None: np.zeros(384, dtype=np.float32),
    )
    monkeypatch.setattr(
        dino_embed, "embed_batch",
        lambda imgs, variant=None: np.zeros((len(imgs), 384), dtype=np.float32),
    )
    monkeypatch.setattr(dino_embed, "embedding_to_blob", lambda e: b"")
    monkeypatch.setattr(dino_embed, "ensure_dinov2_weights", lambda **k: None)

    params = PipelineParams(
        collection_id=col_id,
        skip_classify=True,
        skip_extract_masks=False,
        skip_regroup=True,
    )
    runner = runner or FakeRunner()
    job = _make_job()
    run_pipeline_job(job, runner, db_path, ws_id, params)

    return db, runner, generate_mask_calls, photo_ids


def test_extract_masks_rolls_back_failed_photo_before_continuing(
    tmp_path, monkeypatch,
):
    """One failed persistence attempt must not poison the thread connection."""
    from db import Database

    real_upsert = Database.upsert_photo_mask
    attempts = 0

    def fail_first_upsert(self, *args, **kwargs):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            # Reproduce a failed write after sqlite has opened a transaction.
            # The stage's exception handler must roll this back before moving
            # to the next photo.
            self.conn.execute(
                "UPDATE photos SET rating = rating WHERE id = ?",
                (kwargs["photo_id"],),
            )
            raise sqlite3.OperationalError("database is locked")
        if self.conn.in_transaction:
            raise sqlite3.OperationalError("previous transaction still open")
        return real_upsert(self, *args, **kwargs)

    monkeypatch.setattr(Database, "upsert_photo_mask", fail_first_upsert)
    runner = FakeRunner()
    with pytest.raises(RuntimeError, match="1 of 2 photos failed"):
        _run_extract_masks_for_test(
            tmp_path,
            monkeypatch,
            "tiny",
            [
                {"filename": "first.jpg", "box": (0.1, 0.1, 0.5, 0.5),
                 "model": "MegaDetector"},
                {"filename": "second.jpg", "box": (0.1, 0.1, 0.5, 0.5),
                 "model": "MegaDetector"},
            ],
            runner=runner,
        )

    db = Database(str(tmp_path / "test.db"))
    photo_ids = [
        row["id"] for row in db.conn.execute(
            "SELECT id FROM photos ORDER BY id",
        ).fetchall()
    ]

    saved_ids = [
        row["photo_id"] for row in db.conn.execute(
            "SELECT photo_id FROM photo_masks ORDER BY photo_id",
        ).fetchall()
    ]
    assert saved_ids == [photo_ids[1]]
    final = _extract_masks_final_update(runner)
    assert final["status"] == "failed"
    assert final["error_count"] == 1
    assert "1 masked" in final["summary"]
    assert "1 failed" in final["summary"]


def test_extract_masks_rolls_back_all_photo_writes_when_embeddings_fail(
    tmp_path, monkeypatch,
):
    """Mask rows and derived fields commit atomically with embeddings."""
    from db import Database

    real_update = Database.update_photo_embeddings
    attempts = 0

    def fail_first_embedding_update(self, *args, **kwargs):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise sqlite3.OperationalError("embedding write failed")
        return real_update(self, *args, **kwargs)

    monkeypatch.setattr(
        Database, "update_photo_embeddings", fail_first_embedding_update,
    )
    runner = FakeRunner()
    with pytest.raises(RuntimeError, match="1 of 2 photos failed"):
        _run_extract_masks_for_test(
            tmp_path,
            monkeypatch,
            "tiny",
            [
                {"filename": "first.jpg", "box": (0.1, 0.1, 0.5, 0.5),
                 "model": "MegaDetector"},
                {"filename": "second.jpg", "box": (0.1, 0.1, 0.5, 0.5),
                 "model": "MegaDetector"},
            ],
            runner=runner,
        )

    db = Database(str(tmp_path / "test.db"))
    rows = db.conn.execute(
        "SELECT p.id, p.mask_path, p.active_mask_variant, pm.photo_id "
        "FROM photos p LEFT JOIN photo_masks pm ON pm.photo_id = p.id "
        "ORDER BY p.id",
    ).fetchall()
    assert rows[0]["photo_id"] is None
    assert rows[0]["mask_path"] is None
    assert rows[0]["active_mask_variant"] is None
    assert not (tmp_path / "masks" / f"{rows[0]['id']}.tiny.png").exists()
    assert rows[1]["photo_id"] == rows[1]["id"]
    assert rows[1]["mask_path"] is not None


def test_staged_mask_restores_previous_file_on_failure(tmp_path, monkeypatch):
    """A failed rerun cannot leave old DB metadata pointing at a new PNG."""
    import pipeline_job as pj

    masks_dir = tmp_path / "masks"
    masks_dir.mkdir()
    final_path = masks_dir / "42.tiny.png"
    final_path.write_bytes(b"old mask")

    def fake_save(_mask, stage_dir, photo_id, variant):
        staged = os.path.join(stage_dir, f"{photo_id}.{variant}.png")
        with open(staged, "wb") as handle:
            handle.write(b"new mask")
        return staged

    staged = pj._StagedMaskFile.create(
        None, str(masks_dir), 42, "tiny", fake_save,
        previous_path=str(final_path),
    )
    real_replace = pj.os.replace

    def assert_final_exists_before_replace(source, destination):
        if source == staged.staged_path and destination == staged.final_path:
            assert final_path.read_bytes() == b"old mask"
            assert destination != str(final_path)
        return real_replace(source, destination)

    monkeypatch.setattr(pj.os, "replace", assert_final_exists_before_replace)
    staged.install()
    assert final_path.read_bytes() == b"old mask"
    with open(staged.final_path, "rb") as handle:
        assert handle.read() == b"new mask"

    staged.restore()

    assert final_path.read_bytes() == b"old mask"
    assert not os.path.exists(staged.final_path)
    assert not list(masks_dir.glob(".mask-stage-*"))


def test_staged_mask_syncs_bytes_and_name_before_database_commit(
    tmp_path, monkeypatch,
):
    """install() durably orders file, rename, then directory publication."""
    import pipeline_job as pj

    masks_dir = tmp_path / "masks"

    def fake_save(_mask, stage_dir, photo_id, variant):
        staged_path = os.path.join(stage_dir, f"{photo_id}.{variant}.png")
        with open(staged_path, "wb") as handle:
            handle.write(b"new mask")
        return staged_path

    staged = pj._StagedMaskFile.create(
        None, str(masks_dir), 42, "tiny", fake_save,
    )
    calls = []
    real_replace = pj.os.replace
    monkeypatch.setattr(
        pj, "_fsync_mask_file",
        lambda path: calls.append(("file", path)),
    )
    monkeypatch.setattr(
        pj, "_fsync_mask_directory",
        lambda path: calls.append(("directory", path)),
    )

    def tracked_replace(source, destination):
        calls.append(("replace", source, destination))
        return real_replace(source, destination)

    monkeypatch.setattr(pj.os, "replace", tracked_replace)

    staged.install()

    assert calls == [
        ("file", staged.staged_path),
        ("replace", staged.staged_path, staged.final_path),
        ("directory", str(masks_dir)),
    ]
    staged.restore()


def test_mask_file_fsync_opens_writable_for_windows(tmp_path, monkeypatch):
    """CRT _commit requires a writable descriptor on Windows."""
    import pipeline_job as pj

    mask_path = tmp_path / "mask.png"
    mask_path.write_bytes(b"mask")
    opened_modes = []
    real_open = open

    def tracked_open(path, mode):
        opened_modes.append(mode)
        return real_open(path, mode)

    monkeypatch.setattr(pj, "open", tracked_open, raising=False)

    pj._fsync_mask_file(str(mask_path))

    assert opened_modes == ["rb+"]


def test_staged_mask_removes_previous_file_only_after_commit(tmp_path):
    """The old committed path stays valid until finish follows DB commit."""
    import pipeline_job as pj

    masks_dir = tmp_path / "masks"
    masks_dir.mkdir()
    previous_path = (
        masks_dir / "42.tiny.generation-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.png"
    )
    previous_path.write_bytes(b"old mask")

    def fake_save(_mask, stage_dir, photo_id, variant):
        staged_path = os.path.join(stage_dir, f"{photo_id}.{variant}.png")
        with open(staged_path, "wb") as handle:
            handle.write(b"new mask")
        return staged_path

    staged = pj._StagedMaskFile.create(
        None, str(masks_dir), 42, "tiny", fake_save,
        previous_path=str(previous_path),
    )
    staged.install()

    # This is the interruption window before the database commit: both files
    # are complete, and the database's old path still serves the old bytes.
    assert previous_path.read_bytes() == b"old mask"
    with open(staged.final_path, "rb") as handle:
        assert handle.read() == b"new mask"

    staged.finish()

    assert not previous_path.exists()
    with open(staged.final_path, "rb") as handle:
        assert handle.read() == b"new mask"
    assert not list(masks_dir.glob(".mask-stage-*"))


def test_staged_mask_reclaims_deterministic_standalone_path(tmp_path):
    """The shared photo lock makes deterministic predecessor cleanup safe."""
    import pipeline_job as pj

    masks_dir = tmp_path / "masks"
    masks_dir.mkdir()
    standalone_path = masks_dir / "42.tiny.png"
    standalone_path.write_bytes(b"standalone mask")

    def fake_save(_mask, stage_dir, photo_id, variant):
        staged_path = os.path.join(stage_dir, f"{photo_id}.{variant}.png")
        with open(staged_path, "wb") as handle:
            handle.write(b"pipeline mask")
        return staged_path

    staged = pj._StagedMaskFile.create(
        None, str(masks_dir), 42, "tiny", fake_save,
        previous_path=str(standalone_path),
    )
    staged.install()
    staged.finish()

    assert not standalone_path.exists()
    with open(staged.final_path, "rb") as handle:
        assert handle.read() == b"pipeline mask"


def test_staged_mask_restore_swallows_unlink_oserror(tmp_path, monkeypatch):
    """restore()'s cleanup unlink cannot abort the whole extract-masks stage.

    The published path is uniquely suffixed and no committed row references it
    once the transaction rolled back, so an ``OSError`` here — e.g. an
    antivirus scanner briefly holding the fresh PNG open on Windows — must be
    swallowed. If it escaped through the per-photo ``finally``, the outer
    exception handler would mark the entire stage fatal and skip every
    remaining photo even though the database was rolled back cleanly.
    """
    import pipeline_job as pj

    masks_dir = tmp_path / "masks"
    masks_dir.mkdir()

    def fake_save(_mask, stage_dir, photo_id, variant):
        staged_path = os.path.join(stage_dir, f"{photo_id}.{variant}.png")
        with open(staged_path, "wb") as handle:
            handle.write(b"new mask")
        return staged_path

    staged = pj._StagedMaskFile.create(
        None, str(masks_dir), 42, "tiny", fake_save,
    )
    staged.install()
    assert os.path.exists(staged.final_path)

    real_unlink = pj.os.unlink

    def refuse_final_unlink(path):
        if path == staged.final_path:
            raise PermissionError(
                "simulated antivirus lock on rolled-back mask generation"
            )
        return real_unlink(path)

    monkeypatch.setattr(pj.os, "unlink", refuse_final_unlink)

    staged.restore()

    # Rollback survived the cleanup failure; the unreferenced generation is
    # left behind (same outcome as a mid-write process interruption).
    assert os.path.exists(staged.final_path)
    assert not staged.installed
    assert not list(masks_dir.glob(".mask-stage-*"))


def test_extract_masks_aborts_when_rollback_fails():
    """A broken reused connection must not be carried to later photos."""
    import pipeline_job as pj

    class BrokenConnection:
        def rollback(self):
            raise sqlite3.OperationalError("cannot roll back")

    class BrokenDatabase:
        conn = BrokenConnection()

    with pytest.raises(RuntimeError, match="rollback failed for photo 42"):
        pj._rollback_failed_mask_photo(BrokenDatabase(), 42)


def test_extract_masks_pause_waits_outside_photo_lock(tmp_path, monkeypatch):
    """Pause may be requested under a photo lock but must wait after release."""
    import pipeline_job as pj
    import pipeline_locks

    pause_state = {
        "requested": False,
        "lock_held": False,
        "wait_lock_states": [],
    }
    real_acquire = pipeline_locks.acquire_photo_mask

    class TrackingLock:
        def __init__(self, inner):
            self._inner = inner

        def __enter__(self):
            self._inner.__enter__()
            pause_state["lock_held"] = True
            return self

        def __exit__(self, *args):
            try:
                return self._inner.__exit__(*args)
            finally:
                pause_state["lock_held"] = False

    monkeypatch.setattr(
        pj, "acquire_photo_mask",
        lambda photo_id: TrackingLock(real_acquire(photo_id)),
    )

    class PauseTrackingRunner(FakeRunner):
        def pause_requested(self, job_id):
            return pause_state["requested"]

        def cancellation_requested(self, job_id):
            return False

        def mark_paused(self, job_id):
            return True

        def wait_if_paused(self, job_id, *, publish_paused=False):
            pause_state["wait_lock_states"].append(
                pause_state["lock_held"],
            )
            pause_state["requested"] = False
            return False

    def request_pause():
        assert pause_state["lock_held"] is True
        pause_state["requested"] = True

    _run_extract_masks_for_test(
        tmp_path,
        monkeypatch,
        "sam2-small",
        [{
            "filename": "a.jpg",
            "box": (10, 20, 100, 200),
            "model": "MegaDetector",
        }],
        runner=PauseTrackingRunner(),
        on_generate_mask=request_pause,
    )

    assert pause_state["wait_lock_states"] == [False], (
        "Pause must block only after the per-photo mask lock is released; "
        f"wait states: {pause_state['wait_lock_states']}"
    )


def _apply_extract_masks_stubs(monkeypatch, generate_mask_calls):
    """(Re)install the SAM2/DINOv2 stubs for a follow-up extract_masks run on
    an existing DB. generate_mask appends (variant, det_box) to the passed
    list so the caller can assert whether SAM ran.
    """
    import dino_embed
    import masking
    import numpy as np
    import quality

    monkeypatch.setattr(
        masking, "render_proxy",
        lambda *a, **k: np.zeros((4, 4, 3), dtype=np.uint8),
    )

    def fake_generate_mask(proxy, det_box, variant=None):
        generate_mask_calls.append((variant, tuple(sorted(det_box.items()))))
        return np.ones((4, 4), dtype=bool)

    monkeypatch.setattr(masking, "generate_mask", fake_generate_mask)
    monkeypatch.setattr(masking, "crop_completeness", lambda m: 0.9)
    monkeypatch.setattr(masking, "crop_subject", lambda p, m, margin=0.15: None)
    monkeypatch.setattr(masking, "ensure_sam2_weights", lambda **k: None)
    monkeypatch.setattr(
        quality, "compute_all_quality_features",
        lambda p, m: {
            "subject_tenengrad": 1.5, "bg_tenengrad": 0.3,
            "subject_clip_high": 0.01, "subject_clip_low": 0.01,
            "subject_y_median": 100.0, "bg_separation": 50.0,
            "phash_crop": "deadbeef", "noise_estimate": 5.0,
        },
    )
    monkeypatch.setattr(
        dino_embed, "embed",
        lambda p, variant=None: np.zeros(384, dtype=np.float32),
    )
    monkeypatch.setattr(
        dino_embed, "embed_batch",
        lambda imgs, variant=None: np.zeros((len(imgs), 384), dtype=np.float32),
    )
    monkeypatch.setattr(dino_embed, "embedding_to_blob", lambda e: b"")
    monkeypatch.setattr(dino_embed, "ensure_dinov2_weights", lambda **k: None)


def test_extract_masks_recomputes_when_active_variant_differs(
    tmp_path, monkeypatch,
):
    """Cross-variant cache-hit consistency (Codex P2 on PR #907).

    A cached mask exists for the *requested* SAM variant, but the photos
    row is currently active on a DIFFERENT variant (e.g. two workspaces
    share a folder, one configured sam2-small and one sam2-large, both on
    dinov2 vit-b14). The cheap "re-activate only" fast path would call
    set_active_mask_variant — denormalising the requested variant's mask
    features — WITHOUT update_photo_embeddings, leaving photos.dino_*
    describing the previously-active variant's mask. regroup reads both off
    the photos row, so it would mix one variant's mask features with
    another's embedding. The fix forces a recompute when the active variant
    (or dino embedding variant) doesn't already match, so generate_mask runs
    and the photos row ends internally consistent.
    """
    import config as cfg

    spec = {"filename": "a.jpg", "box": (10, 20, 100, 200),
            "model": "MegaDetector"}

    # Run 1: variant small → photo_masks[small], photos.active=small.
    db, _, _, photo_ids = _run_extract_masks_for_test(
        tmp_path, monkeypatch, "sam2-small", [spec],
    )
    pid = photo_ids[0]
    db_path = str(tmp_path / "test.db")
    ws_id = db._active_workspace_id
    col_id = db.conn.execute(
        "SELECT id FROM collections ORDER BY id LIMIT 1"
    ).fetchone()[0]

    def rerun(variant):
        calls = []
        _apply_extract_masks_stubs(monkeypatch, calls)
        cfg.save({"pipeline": {"sam2_variant": variant,
                               "dinov2_variant": "vit-b14"}})
        run_pipeline_job(
            _make_job(), FakeRunner(), db_path, ws_id,
            PipelineParams(collection_id=col_id, skip_classify=True,
                           skip_extract_masks=False, skip_regroup=True),
        )
        return calls

    # Run 2: variant large → adds photo_masks[large], photos.active=large.
    calls_large = rerun("sam2-large")
    assert calls_large, "switching to a new variant must run SAM"
    state = db.conn.execute(
        "SELECT active_mask_variant FROM photos WHERE id=?", (pid,),
    ).fetchone()
    assert state["active_mask_variant"] == "sam2-large"
    # Both variant rows now cached on disk.
    variants = {r["variant"] for r in db.list_masks_for_photo(pid)}
    assert variants == {"sam2-small", "sam2-large"}

    # Run 3: back to small. The small row is cached (prompt+detector match,
    # file on disk) so the OLD code would cheap-skip via set_active_mask_
    # variant and never touch embeddings. With the fix, because the photos
    # row is active on large, the stage recomputes: generate_mask runs for
    # small and the row ends active+consistent on small.
    calls_small = rerun("sam2-small")
    assert calls_small, (
        "cached mask but stale active variant must recompute, not cheap-skip; "
        f"generate_mask was not called: {calls_small}"
    )
    final = db.conn.execute(
        "SELECT active_mask_variant, dino_embedding_variant FROM photos "
        "WHERE id=?", (pid,),
    ).fetchone()
    assert final["active_mask_variant"] == "sam2-small"
    assert final["dino_embedding_variant"] == "vit-b14"


def test_extract_masks_skips_sam_when_cached_with_same_prompt(
    tmp_path, monkeypatch,
):
    """Second pipeline pass over the same photo + same detection must NOT
    call generate_mask: the photo_masks row is already there for the
    configured variant and the cached prompt still matches.  The
    photo_masks row remains intact (one row, same path)."""
    spec = {"filename": "a.jpg", "box": (10, 20, 100, 200),
            "model": "MegaDetector"}

    db, _, calls_first, photo_ids = _run_extract_masks_for_test(
        tmp_path, monkeypatch, "sam2-small", [spec],
    )
    pid = photo_ids[0]
    assert len(calls_first) == 1, (
        f"first run should call generate_mask once; got {calls_first}"
    )
    rows_first = db.list_masks_for_photo(pid)
    assert len(rows_first) == 1
    assert rows_first[0]["variant"] == "sam2-small"
    first_path = rows_first[0]["path"]

    # Re-run the stage in the same workspace with the same DB.  Reuse the
    # helper to drive a *second* pass — but we want the same DB, so call
    # run_pipeline_job again directly.
    import config as cfg
    import dino_embed
    import masking
    import numpy as np
    import quality

    calls_second = []

    def fake_generate_mask_2(proxy, det_box, variant=None):
        calls_second.append((variant, tuple(sorted(det_box.items()))))
        return np.ones((4, 4), dtype=bool)

    monkeypatch.setattr(
        masking, "render_proxy",
        lambda *a, **k: np.zeros((4, 4, 3), dtype=np.uint8),
    )
    monkeypatch.setattr(masking, "generate_mask", fake_generate_mask_2)
    monkeypatch.setattr(masking, "crop_completeness", lambda m: 0.9)
    monkeypatch.setattr(masking, "crop_subject", lambda p, m, margin=0.15: None)
    monkeypatch.setattr(masking, "ensure_sam2_weights", lambda **k: None)
    monkeypatch.setattr(
        quality, "compute_all_quality_features",
        lambda p, m: {
            "subject_tenengrad": 1.5, "bg_tenengrad": 0.3,
            "subject_clip_high": 0.01, "subject_clip_low": 0.01,
            "subject_y_median": 100.0, "bg_separation": 50.0,
            "phash_crop": "deadbeef", "noise_estimate": 5.0,
        },
    )
    monkeypatch.setattr(
        dino_embed, "embed",
        lambda p, variant=None: np.zeros(384, dtype=np.float32),
    )
    monkeypatch.setattr(
        dino_embed, "embed_batch",
        lambda imgs, variant=None: np.zeros((len(imgs), 384), dtype=np.float32),
    )
    monkeypatch.setattr(dino_embed, "embedding_to_blob", lambda e: b"")
    monkeypatch.setattr(dino_embed, "ensure_dinov2_weights", lambda **k: None)

    cfg.save({
        "pipeline": {"sam2_variant": "sam2-small", "dinov2_variant": "vit-b14"},
    })
    # The mask file must exist on disk for the cache check to fire — the
    # first run wrote it, but ensure it's still there.
    assert os.path.isfile(first_path)

    col_id = db.conn.execute(
        "SELECT id FROM collections ORDER BY id LIMIT 1"
    ).fetchone()[0]
    params = PipelineParams(
        collection_id=col_id, skip_classify=True,
        skip_extract_masks=False, skip_regroup=True,
    )
    runner = FakeRunner()
    job = _make_job()
    run_pipeline_job(
        job, runner, str(tmp_path / "test.db"), db._active_workspace_id, params,
    )

    assert calls_second == [], (
        f"cache hit must skip generate_mask entirely; got {calls_second}"
    )
    rows_after = db.list_masks_for_photo(pid)
    assert len(rows_after) == 1
    assert rows_after[0]["path"] == first_path


def test_extract_masks_skips_weight_download_when_all_cached(
    tmp_path, monkeypatch,
):
    """Fully-cached rerun must NOT call ensure_sam2_weights /
    ensure_dinov2_weights. Before the fix, the worklist included every
    photo with a detection (cache hits filtered inside the loop, not by
    a `mask_path IS NULL` prefilter), so total > 0 unconditionally and
    the gate fired the multi-hundred-MB weight downloads on every
    rerun — fatal in offline / fresh-checkout environments where the
    only thing the user wanted was to denormalize the active variant."""
    spec = {"filename": "a.jpg", "box": (10, 20, 100, 200),
            "model": "MegaDetector"}

    # First run: populates photo_masks (this run *is* allowed to fire the
    # download; mocked to no-op).
    db, _, _, photo_ids = _run_extract_masks_for_test(
        tmp_path, monkeypatch, "sam2-small", [spec],
    )
    pid = photo_ids[0]
    rows = db.list_masks_for_photo(pid)
    assert rows and rows[0]["variant"] == "sam2-small"
    assert os.path.isfile(rows[0]["path"])

    # Second run: every photo is a cache hit. Track ensure_*_weights
    # invocations and assert they are zero.
    import config as cfg
    import dino_embed
    import masking
    import numpy as np
    import quality

    sam_calls = []
    dino_calls = []

    def fake_ensure_sam(**k):
        sam_calls.append(k)

    def fake_ensure_dino(**k):
        dino_calls.append(k)

    monkeypatch.setattr(masking, "ensure_sam2_weights", fake_ensure_sam)
    monkeypatch.setattr(dino_embed, "ensure_dinov2_weights", fake_ensure_dino)
    monkeypatch.setattr(
        masking, "render_proxy",
        lambda *a, **k: np.zeros((4, 4, 3), dtype=np.uint8),
    )
    # generate_mask should never be invoked on a cache hit.
    monkeypatch.setattr(
        masking, "generate_mask",
        lambda *a, **k: (_ for _ in ()).throw(
            AssertionError("generate_mask called on a cache hit")
        ),
    )
    monkeypatch.setattr(masking, "crop_completeness", lambda m: 0.9)
    monkeypatch.setattr(masking, "crop_subject", lambda p, m, margin=0.15: None)
    monkeypatch.setattr(
        quality, "compute_all_quality_features",
        lambda p, m: {},
    )
    monkeypatch.setattr(
        dino_embed, "embed",
        lambda p, variant=None: np.zeros(384, dtype=np.float32),
    )
    monkeypatch.setattr(
        dino_embed, "embed_batch",
        lambda imgs, variant=None: np.zeros((len(imgs), 384), dtype=np.float32),
    )
    monkeypatch.setattr(dino_embed, "embedding_to_blob", lambda e: b"")

    cfg.save({
        "pipeline": {"sam2_variant": "sam2-small", "dinov2_variant": "vit-b14"},
    })
    col_id = db.conn.execute(
        "SELECT id FROM collections ORDER BY id LIMIT 1"
    ).fetchone()[0]
    params = PipelineParams(
        collection_id=col_id, skip_classify=True,
        skip_extract_masks=False, skip_regroup=True,
    )
    runner = FakeRunner()
    job = _make_job()
    run_pipeline_job(
        job, runner, str(tmp_path / "test.db"), db._active_workspace_id, params,
    )

    assert sam_calls == [], (
        f"ensure_sam2_weights must not be called on a fully-cached rerun; "
        f"got {sam_calls}"
    )
    assert dino_calls == [], (
        f"ensure_dinov2_weights must not be called on a fully-cached rerun; "
        f"got {dino_calls}"
    )


def test_extract_masks_runs_for_new_variant_keeps_old(tmp_path, monkeypatch):
    """A first pass with sam2-small writes one row; switching the
    configured variant to sam2-large adds a second row — both
    variants for the photo are listed in photo_masks.  Active variant
    on the photos row tracks the most recent run."""
    spec = {"filename": "a.jpg", "box": (10, 20, 100, 200),
            "model": "MegaDetector"}

    db, _, calls_first, photo_ids = _run_extract_masks_for_test(
        tmp_path, monkeypatch, "sam2-small", [spec],
    )
    pid = photo_ids[0]
    assert len(calls_first) == 1
    assert calls_first[0][0] == "sam2-small"

    rows = db.list_masks_for_photo(pid)
    assert {r["variant"] for r in rows} == {"sam2-small"}

    # Switch the configured variant.  Re-run.
    import config as cfg
    import dino_embed
    import masking
    import numpy as np
    import quality

    cfg.save({
        "pipeline": {"sam2_variant": "sam2-large", "dinov2_variant": "vit-b14"},
    })

    calls_second = []

    def fake_generate_mask_2(proxy, det_box, variant=None):
        calls_second.append((variant, tuple(sorted(det_box.items()))))
        return np.ones((4, 4), dtype=bool)

    monkeypatch.setattr(
        masking, "render_proxy",
        lambda *a, **k: np.zeros((4, 4, 3), dtype=np.uint8),
    )
    monkeypatch.setattr(masking, "generate_mask", fake_generate_mask_2)
    monkeypatch.setattr(masking, "crop_completeness", lambda m: 0.9)
    monkeypatch.setattr(masking, "crop_subject", lambda p, m, margin=0.15: None)
    monkeypatch.setattr(masking, "ensure_sam2_weights", lambda **k: None)
    monkeypatch.setattr(
        quality, "compute_all_quality_features",
        lambda p, m: {
            "subject_tenengrad": 2.0, "bg_tenengrad": 0.4,
            "subject_clip_high": 0.0, "subject_clip_low": 0.0,
            "subject_y_median": 110.0, "bg_separation": 60.0,
            "phash_crop": "cafef00d", "noise_estimate": 5.0,
        },
    )
    monkeypatch.setattr(
        dino_embed, "embed",
        lambda p, variant=None: np.zeros(384, dtype=np.float32),
    )
    monkeypatch.setattr(
        dino_embed, "embed_batch",
        lambda imgs, variant=None: np.zeros((len(imgs), 384), dtype=np.float32),
    )
    monkeypatch.setattr(dino_embed, "embedding_to_blob", lambda e: b"")
    monkeypatch.setattr(dino_embed, "ensure_dinov2_weights", lambda **k: None)

    col_id = db.conn.execute(
        "SELECT id FROM collections ORDER BY id LIMIT 1"
    ).fetchone()[0]
    params = PipelineParams(
        collection_id=col_id, skip_classify=True,
        skip_extract_masks=False, skip_regroup=True,
    )
    runner = FakeRunner()
    job = _make_job()
    run_pipeline_job(
        job, runner, str(tmp_path / "test.db"), db._active_workspace_id, params,
    )

    assert len(calls_second) == 1 and calls_second[0][0] == "sam2-large", (
        f"new variant must trigger generate_mask once for sam2-large; "
        f"got {calls_second}"
    )
    rows = db.list_masks_for_photo(pid)
    assert {r["variant"] for r in rows} == {"sam2-small", "sam2-large"}, (
        f"expected both variants present after re-run; got {rows}"
    )
    active = db.conn.execute(
        "SELECT active_mask_variant FROM photos WHERE id=?", (pid,),
    ).fetchone()[0]
    assert active == "sam2-large"


def test_extract_masks_re_runs_when_prompt_changed(tmp_path, monkeypatch):
    """If the photo's primary detection's bbox changes between runs, the
    cached photo_masks row's prompt no longer matches and SAM has to
    re-run.  The row is replaced with the new prompt + path; the
    photo_masks set still has exactly one row for that variant."""
    spec = {"filename": "a.jpg", "box": (10, 20, 100, 200),
            "model": "MegaDetector"}

    db, _, calls_first, photo_ids = _run_extract_masks_for_test(
        tmp_path, monkeypatch, "sam2-small", [spec],
    )
    pid = photo_ids[0]
    assert len(calls_first) == 1
    rows = db.list_masks_for_photo(pid)
    assert len(rows) == 1
    assert (rows[0]["prompt_x"], rows[0]["prompt_w"]) == (10, 100)

    # Mutate the detection so it carries a new bbox (mimics YOLO re-run
    # with a different confidence threshold producing a slightly
    # different box).
    db.conn.execute(
        "UPDATE detections SET box_x = 99 WHERE photo_id=?", (pid,),
    )
    db.conn.commit()

    # Re-run — generate_mask must be called and the row must be replaced.
    import config as cfg
    import dino_embed
    import masking
    import numpy as np
    import quality

    cfg.save({
        "pipeline": {"sam2_variant": "sam2-small", "dinov2_variant": "vit-b14"},
    })

    calls_second = []

    def fake_generate_mask_2(proxy, det_box, variant=None):
        calls_second.append((variant, tuple(sorted(det_box.items()))))
        return np.ones((4, 4), dtype=bool)

    monkeypatch.setattr(
        masking, "render_proxy",
        lambda *a, **k: np.zeros((4, 4, 3), dtype=np.uint8),
    )
    monkeypatch.setattr(masking, "generate_mask", fake_generate_mask_2)
    monkeypatch.setattr(masking, "crop_completeness", lambda m: 0.95)
    monkeypatch.setattr(masking, "crop_subject", lambda p, m, margin=0.15: None)
    monkeypatch.setattr(masking, "ensure_sam2_weights", lambda **k: None)
    monkeypatch.setattr(
        quality, "compute_all_quality_features",
        lambda p, m: {
            "subject_tenengrad": 1.5, "bg_tenengrad": 0.3,
            "subject_clip_high": 0.01, "subject_clip_low": 0.01,
            "subject_y_median": 100.0, "bg_separation": 50.0,
            "phash_crop": "deadbeef", "noise_estimate": 5.0,
        },
    )
    monkeypatch.setattr(
        dino_embed, "embed",
        lambda p, variant=None: np.zeros(384, dtype=np.float32),
    )
    monkeypatch.setattr(
        dino_embed, "embed_batch",
        lambda imgs, variant=None: np.zeros((len(imgs), 384), dtype=np.float32),
    )
    monkeypatch.setattr(dino_embed, "embedding_to_blob", lambda e: b"")
    monkeypatch.setattr(dino_embed, "ensure_dinov2_weights", lambda **k: None)

    col_id = db.conn.execute(
        "SELECT id FROM collections ORDER BY id LIMIT 1"
    ).fetchone()[0]
    params = PipelineParams(
        collection_id=col_id, skip_classify=True,
        skip_extract_masks=False, skip_regroup=True,
    )
    runner = FakeRunner()
    job = _make_job()
    run_pipeline_job(
        job, runner, str(tmp_path / "test.db"), db._active_workspace_id, params,
    )

    assert len(calls_second) == 1, (
        f"prompt change must re-run generate_mask; got {calls_second}"
    )
    rows_after = db.list_masks_for_photo(pid)
    assert len(rows_after) == 1, (
        f"row should be replaced (upsert), not duplicated; got {rows_after}"
    )
    assert rows_after[0]["prompt_x"] == 99


def test_pipeline_eye_keypoints_cancel_marks_stage_cancelled(
    tmp_path, monkeypatch,
):
    """An abort during eye_keypoints must finalize the stage with a
    'Cancelled' summary, not the default 'X of N photos processed' which
    looks indistinguishable from a clean run that happened to process X.
    """
    import config as cfg
    import pipeline as pipeline_mod
    import pipeline_job as pj
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)
    pid = db.add_photo(folder_id, "p.jpg", ".jpg", 100, 1_000_000.0)
    _drop_jpeg(folder_path, "p.jpg")
    db.save_detections(
        pid,
        [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
          "confidence": 0.9, "category": "animal"}],
        detector_model="MegaDetector",
    )
    col_id = db.add_collection(
        "Test", json.dumps([{"field": "photo_ids", "value": [pid]}]),
    )

    # Stub extract_masks heavies so it sails through (eye_keypoints is
    # gated on extract_masks running). We do NOT trigger abort during
    # extract_masks — only during eye_keypoints.
    _stub_extract_masks_heavy_ops(monkeypatch)

    # Make eye_keypoints reachable: preflight returns None.
    monkeypatch.setattr(
        pipeline_mod, "eye_keypoint_stage_preflight", lambda config: None,
    )
    # The wrapping stage uses list_photos_for_eye_keypoint_stage to compute
    # `total`. Force a simple list of one photo.
    monkeypatch.setattr(
        Database, "list_photos_for_eye_keypoint_stage",
        lambda self, **k: [{"id": pid}],
    )
    # Stub ensure_keypoint_weights so the auto-download path doesn't try to
    # hit HuggingFace from a unit test. Pretend weights are already there.
    import keypoints as _kp_mod
    monkeypatch.setattr(
        _kp_mod, "ensure_keypoint_weights",
        lambda name, progress_callback=None: "/fake/model.onnx",
    )

    abort_now = [False]

    def fake_detect_eye_keypoints_stage(
        db_, config, progress_callback=None,
        collection_id=None, exclude_photo_ids=None,
        abort_check=None,
    ):
        # Mid-stage: emit a progress event then trigger abort. The wrapping
        # stage must notice and finalize with a "Cancelled" summary.
        if progress_callback:
            progress_callback("Eye keypoints", 0, 1)
        abort_now[0] = True

    monkeypatch.setattr(
        pipeline_mod, "detect_eye_keypoints_stage",
        fake_detect_eye_keypoints_stage,
    )

    original_should_abort = pj._should_abort

    def patched_should_abort(event):
        if abort_now[0]:
            return True
        return original_should_abort(event)

    monkeypatch.setattr(pj, "_should_abort", patched_should_abort)

    params = PipelineParams(
        collection_id=col_id,
        skip_classify=True,
        skip_extract_masks=False,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()
    run_pipeline_job(job, runner, db_path, ws_id, params)

    ek_finals = [
        kw for (_, sid, kw) in runner.step_updates
        if sid == "eye_keypoints" and kw.get("status") in (
            "completed", "failed",
        ) and "summary" in kw
    ]
    assert ek_finals, (
        f"Expected eye_keypoints final update; got "
        f"step_updates={runner.step_updates!r}"
    )
    summary = ek_finals[-1].get("summary") or ""
    assert "Cancelled" in summary, (
        f"eye_keypoints final summary must reflect cancellation; got "
        f"{summary!r}"
    )


def test_pipeline_eye_keypoints_stage_auto_downloads_superanimal_weights(
    tmp_path, monkeypatch,
):
    """The eye_keypoints stage must call ensure_keypoint_weights for both
    SuperAnimal models before iterating photos when the eligible set
    contains both bird and quadruped subjects. Without this auto-download,
    a fresh install would silently produce zero eye keypoints because the
    per-photo Gate 2 check would skip every photo.
    """
    import config as cfg
    import pipeline as pipeline_mod
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)
    pid = db.add_photo(folder_id, "p.jpg", ".jpg", 100, 1_000_000.0)
    _drop_jpeg(folder_path, "p.jpg")
    db.save_detections(
        pid,
        [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
          "confidence": 0.9, "category": "animal"}],
        detector_model="MegaDetector",
    )
    col_id = db.add_collection(
        "Test", json.dumps([{"field": "photo_ids", "value": [pid]}]),
    )

    _stub_extract_masks_heavy_ops(monkeypatch)

    monkeypatch.setattr(
        pipeline_mod, "eye_keypoint_stage_preflight", lambda config: None,
    )
    # Provide one bird and one quadruped row so the routing-aware download
    # picks both SuperAnimal variants. species_conf is above the default
    # eye_classifier_conf_gate (0.5) so the conf-gate filter doesn't drop
    # them before _resolve_keypoint_model runs.
    monkeypatch.setattr(
        Database, "list_photos_for_eye_keypoint_stage",
        lambda self, **k: [
            {"id": pid, "taxonomy_class": "Mammalia", "species_conf": 0.9},
            {"id": pid + 1000, "taxonomy_class": "Aves", "species_conf": 0.9},
        ],
    )
    # detect_eye_keypoints_stage stub — we're testing the wrapping stage's
    # download orchestration, not per-photo inference.
    monkeypatch.setattr(
        pipeline_mod, "detect_eye_keypoints_stage",
        lambda *a, **k: None,
    )

    downloaded = []
    import keypoints as _kp_mod

    def _spy_ensure(name, progress_callback=None):
        downloaded.append(name)
        return "/fake/model.onnx"

    monkeypatch.setattr(_kp_mod, "ensure_keypoint_weights", _spy_ensure)

    params = PipelineParams(
        collection_id=col_id,
        skip_classify=True,
        skip_extract_masks=False,
        skip_regroup=True,
    )
    runner = FakeRunner()
    job = _make_job()
    run_pipeline_job(job, runner, db_path, ws_id, params)

    assert downloaded == ["superanimal-quadruped", "superanimal-bird"], (
        f"Expected stage to auto-download both SuperAnimal variants in order; "
        f"got {downloaded!r}"
    )


def test_pipeline_eye_keypoints_stage_only_downloads_routable_variants(
    tmp_path, monkeypatch,
):
    """When every eligible photo routes to bird, the stage must skip the
    quadruped variant download (and vice versa). Otherwise a bird-only
    collection pays the full quadruped download cost for weights it would
    never use.
    """
    import config as cfg
    import pipeline as pipeline_mod
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)
    pid = db.add_photo(folder_id, "p.jpg", ".jpg", 100, 1_000_000.0)
    _drop_jpeg(folder_path, "p.jpg")
    db.save_detections(
        pid,
        [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
          "confidence": 0.9, "category": "animal"}],
        detector_model="MegaDetector",
    )
    col_id = db.add_collection(
        "Test", json.dumps([{"field": "photo_ids", "value": [pid]}]),
    )

    _stub_extract_masks_heavy_ops(monkeypatch)

    monkeypatch.setattr(
        pipeline_mod, "eye_keypoint_stage_preflight", lambda config: None,
    )
    # Bird-only eligible set; species_conf above eye_classifier_conf_gate
    # (default 0.5) so the conf-gate filter doesn't drop the row before
    # routing.
    monkeypatch.setattr(
        Database, "list_photos_for_eye_keypoint_stage",
        lambda self, **k: [
            {"id": pid, "taxonomy_class": "Aves", "species_conf": 0.9},
        ],
    )
    monkeypatch.setattr(
        pipeline_mod, "detect_eye_keypoints_stage",
        lambda *a, **k: None,
    )

    downloaded = []
    import keypoints as _kp_mod
    monkeypatch.setattr(
        _kp_mod, "ensure_keypoint_weights",
        lambda name, progress_callback=None: downloaded.append(name) or "/fake",
    )

    params = PipelineParams(
        collection_id=col_id,
        skip_classify=True,
        skip_extract_masks=False,
        skip_regroup=True,
    )
    run_pipeline_job(_make_job(), FakeRunner(), db_path, ws_id, params)

    assert downloaded == ["superanimal-bird"], (
        f"Expected only superanimal-bird to download for a bird-only "
        f"eligible set; got {downloaded!r}"
    )


def test_pipeline_eye_keypoints_stage_skips_download_for_out_of_scope_only(
    tmp_path, monkeypatch,
):
    """A collection of only out-of-scope subjects (fish/reptiles/inverts)
    must not trigger any SuperAnimal download. Without routing-awareness,
    the prior `if total > 0` guard would still pull both ~hundreds-of-MB
    variants even though `_resolve_keypoint_model` skips every photo.
    """
    import config as cfg
    import pipeline as pipeline_mod
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)
    pid = db.add_photo(folder_id, "p.jpg", ".jpg", 100, 1_000_000.0)
    _drop_jpeg(folder_path, "p.jpg")
    db.save_detections(
        pid,
        [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
          "confidence": 0.9, "category": "animal"}],
        detector_model="MegaDetector",
    )
    col_id = db.add_collection(
        "Test", json.dumps([{"field": "photo_ids", "value": [pid]}]),
    )

    _stub_extract_masks_heavy_ops(monkeypatch)

    monkeypatch.setattr(
        pipeline_mod, "eye_keypoint_stage_preflight", lambda config: None,
    )
    # Eligible photos exist (total > 0) but every row carries an
    # out-of-scope taxonomy_class — the per-photo router will return None
    # for each, so no SuperAnimal model is needed. species_conf is above
    # the conf-gate threshold to isolate the routing skip from the
    # confidence skip.
    monkeypatch.setattr(
        Database, "list_photos_for_eye_keypoint_stage",
        lambda self, **k: [
            {"id": pid, "taxonomy_class": "Reptilia", "species_conf": 0.9},
            {"id": pid + 1, "taxonomy_class": "Actinopterygii",
             "species_conf": 0.9},
        ],
    )
    monkeypatch.setattr(
        pipeline_mod, "detect_eye_keypoints_stage",
        lambda *a, **k: None,
    )

    downloaded = []
    import keypoints as _kp_mod
    monkeypatch.setattr(
        _kp_mod, "ensure_keypoint_weights",
        lambda name, progress_callback=None: downloaded.append(name) or "/fake",
    )

    params = PipelineParams(
        collection_id=col_id,
        skip_classify=True,
        skip_extract_masks=False,
        skip_regroup=True,
    )
    run_pipeline_job(_make_job(), FakeRunner(), db_path, ws_id, params)

    assert downloaded == [], (
        f"Expected zero downloads for an out-of-scope-only eligible set; "
        f"got {downloaded!r}"
    )


def test_pipeline_eye_keypoints_stage_download_progress_isolated_from_photo_count(
    tmp_path, monkeypatch,
):
    """The download progress callback must NOT advance the photo
    `processed` counter. Otherwise a cancel during/just after weight
    download surfaces e.g. "Cancelled (1 of N processed)" before any
    photo has actually been touched, misreporting stage outcomes.
    """
    import config as cfg
    import pipeline as pipeline_mod
    import pipeline_job as pj
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)
    pid = db.add_photo(folder_id, "p.jpg", ".jpg", 100, 1_000_000.0)
    _drop_jpeg(folder_path, "p.jpg")
    db.save_detections(
        pid,
        [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
          "confidence": 0.9, "category": "animal"}],
        detector_model="MegaDetector",
    )
    col_id = db.add_collection(
        "Test", json.dumps([{"field": "photo_ids", "value": [pid]}]),
    )

    _stub_extract_masks_heavy_ops(monkeypatch)

    monkeypatch.setattr(
        pipeline_mod, "eye_keypoint_stage_preflight", lambda config: None,
    )
    # Three quadruped rows so total=3 — large enough to make the bug
    # observable if the download callback bumps the photo counter.
    # species_conf above the conf-gate so they reach the download
    # planner.
    monkeypatch.setattr(
        Database, "list_photos_for_eye_keypoint_stage",
        lambda self, **k: [
            {"id": pid, "taxonomy_class": "Mammalia", "species_conf": 0.9},
            {"id": pid + 1, "taxonomy_class": "Mammalia", "species_conf": 0.9},
            {"id": pid + 2, "taxonomy_class": "Mammalia", "species_conf": 0.9},
        ],
    )

    abort_now = [False]

    # Stub ensure_keypoint_weights to (a) invoke the progress callback the
    # way the real implementation does — phase, current=0/total=1 then
    # current=1/total=1 — and (b) trigger an abort, so detect_*_stage
    # exits before any real photo work happens.
    import keypoints as _kp_mod

    def _ensure_with_progress(name, progress_callback=None):
        if progress_callback is not None:
            progress_callback(f"Downloading {name}...", 0, 1)
            progress_callback(f"{name} ready", 1, 1)
        abort_now[0] = True
        return "/fake/model.onnx"

    monkeypatch.setattr(
        _kp_mod, "ensure_keypoint_weights", _ensure_with_progress,
    )
    # Make detect_eye_keypoints_stage a no-op — the abort fires before it
    # would touch a photo and we want to assert what the wrapping stage
    # reports for processed['count'].
    monkeypatch.setattr(
        pipeline_mod, "detect_eye_keypoints_stage",
        lambda *a, **k: None,
    )

    original_should_abort = pj._should_abort

    def patched_should_abort(event):
        if abort_now[0]:
            return True
        return original_should_abort(event)

    monkeypatch.setattr(pj, "_should_abort", patched_should_abort)

    params = PipelineParams(
        collection_id=col_id,
        skip_classify=True,
        skip_extract_masks=False,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()
    run_pipeline_job(job, runner, db_path, ws_id, params)

    ek_finals = [
        kw for (_, sid, kw) in runner.step_updates
        if sid == "eye_keypoints" and kw.get("status") in (
            "completed", "failed",
        ) and "summary" in kw
    ]
    assert ek_finals, (
        f"Expected eye_keypoints final update; got "
        f"step_updates={runner.step_updates!r}"
    )
    summary = ek_finals[-1].get("summary") or ""
    # Cancel summary must report 0 processed (no photo ran), not 1 — the
    # download callback must not bleed into the photo counter.
    assert "Cancelled (0 of 3 processed)" in summary, (
        f"Download progress callback leaked into the photo counter; "
        f"expected 'Cancelled (0 of 3 processed)', got {summary!r}"
    )


def test_pipeline_eye_keypoints_stage_skips_download_when_no_eligible_photos(
    tmp_path, monkeypatch,
):
    """When no photos are eligible (total == 0), the stage must NOT trigger
    a multi-hundred-MB download — gate matches the SAM2/DINOv2 pattern."""
    import config as cfg
    import pipeline as pipeline_mod
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)
    pid = db.add_photo(folder_id, "p.jpg", ".jpg", 100, 1_000_000.0)
    _drop_jpeg(folder_path, "p.jpg")
    db.save_detections(
        pid,
        [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
          "confidence": 0.9, "category": "animal"}],
        detector_model="MegaDetector",
    )
    col_id = db.add_collection(
        "Test", json.dumps([{"field": "photo_ids", "value": [pid]}]),
    )

    _stub_extract_masks_heavy_ops(monkeypatch)

    monkeypatch.setattr(
        pipeline_mod, "eye_keypoint_stage_preflight", lambda config: None,
    )
    # Force "no eligible photos" — preflight passes but the eligibility
    # query returns nothing.
    monkeypatch.setattr(
        Database, "list_photos_for_eye_keypoint_stage",
        lambda self, **k: [],
    )
    monkeypatch.setattr(
        pipeline_mod, "detect_eye_keypoints_stage",
        lambda *a, **k: None,
    )

    downloaded = []
    import keypoints as _kp_mod
    monkeypatch.setattr(
        _kp_mod, "ensure_keypoint_weights",
        lambda name, progress_callback=None: downloaded.append(name) or "/fake",
    )

    params = PipelineParams(
        collection_id=col_id,
        skip_classify=True,
        skip_extract_masks=False,
        skip_regroup=True,
    )
    run_pipeline_job(_make_job(), FakeRunner(), db_path, ws_id, params)

    assert downloaded == [], (
        f"Expected no auto-download when 0 photos are eligible; got {downloaded!r}"
    )


def test_pipeline_eye_keypoints_stage_skips_download_when_all_below_conf_gate(
    tmp_path, monkeypatch,
):
    """When every eligible row has species_conf below
    eye_classifier_conf_gate, _process_photo_for_eye skips it at Gate 1
    before any keypoint inference. The download planner must mirror that
    threshold so an all-low-confidence collection doesn't pull
    multi-hundred-MB SuperAnimal weights that no photo can use.
    """
    import config as cfg
    import pipeline as pipeline_mod
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)
    pid = db.add_photo(folder_id, "p.jpg", ".jpg", 100, 1_000_000.0)
    _drop_jpeg(folder_path, "p.jpg")
    db.save_detections(
        pid,
        [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
          "confidence": 0.9, "category": "animal"}],
        detector_model="MegaDetector",
    )
    col_id = db.add_collection(
        "Test", json.dumps([{"field": "photo_ids", "value": [pid]}]),
    )

    _stub_extract_masks_heavy_ops(monkeypatch)

    monkeypatch.setattr(
        pipeline_mod, "eye_keypoint_stage_preflight", lambda config: None,
    )
    # Default eye_classifier_conf_gate is 0.5; both rows sit below it.
    monkeypatch.setattr(
        Database, "list_photos_for_eye_keypoint_stage",
        lambda self, **k: [
            {"id": pid, "taxonomy_class": "Mammalia", "species_conf": 0.2},
            {"id": pid + 1, "taxonomy_class": "Aves", "species_conf": 0.4},
        ],
    )
    monkeypatch.setattr(
        pipeline_mod, "detect_eye_keypoints_stage",
        lambda *a, **k: None,
    )

    downloaded = []
    import keypoints as _kp_mod
    monkeypatch.setattr(
        _kp_mod, "ensure_keypoint_weights",
        lambda name, progress_callback=None: downloaded.append(name) or "/fake",
    )

    params = PipelineParams(
        collection_id=col_id,
        skip_classify=True,
        skip_extract_masks=False,
        skip_regroup=True,
    )
    run_pipeline_job(_make_job(), FakeRunner(), db_path, ws_id, params)

    assert downloaded == [], (
        f"Expected no downloads for an all-below-conf-gate set; "
        f"got {downloaded!r}"
    )


def test_pipeline_eye_keypoints_stage_aborts_between_keypoint_downloads(
    tmp_path, monkeypatch,
):
    """A cancel that arrives after the first SuperAnimal weights download
    must short-circuit the second. Without the abort check between models
    the user waits through tens-to-hundreds of MB of unwanted bandwidth
    before the stage exits.
    """
    import config as cfg
    import pipeline as pipeline_mod
    import pipeline_job as pj
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)
    pid = db.add_photo(folder_id, "p.jpg", ".jpg", 100, 1_000_000.0)
    _drop_jpeg(folder_path, "p.jpg")
    db.save_detections(
        pid,
        [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
          "confidence": 0.9, "category": "animal"}],
        detector_model="MegaDetector",
    )
    col_id = db.add_collection(
        "Test", json.dumps([{"field": "photo_ids", "value": [pid]}]),
    )

    _stub_extract_masks_heavy_ops(monkeypatch)

    monkeypatch.setattr(
        pipeline_mod, "eye_keypoint_stage_preflight", lambda config: None,
    )
    # One mammal + one bird so the planner queues both variants.
    monkeypatch.setattr(
        Database, "list_photos_for_eye_keypoint_stage",
        lambda self, **k: [
            {"id": pid, "taxonomy_class": "Mammalia", "species_conf": 0.9},
            {"id": pid + 1, "taxonomy_class": "Aves", "species_conf": 0.9},
        ],
    )
    monkeypatch.setattr(
        pipeline_mod, "detect_eye_keypoints_stage",
        lambda *a, **k: None,
    )

    abort_now = [False]
    downloaded = []
    import keypoints as _kp_mod

    def _ensure_then_abort(name, progress_callback=None):
        downloaded.append(name)
        # Trigger abort the moment the first download "finishes" so the
        # next iteration's abort check fires.
        abort_now[0] = True
        return "/fake/model.onnx"

    monkeypatch.setattr(
        _kp_mod, "ensure_keypoint_weights", _ensure_then_abort,
    )

    original_should_abort = pj._should_abort

    def patched_should_abort(event):
        if abort_now[0]:
            return True
        return original_should_abort(event)

    monkeypatch.setattr(pj, "_should_abort", patched_should_abort)

    params = PipelineParams(
        collection_id=col_id,
        skip_classify=True,
        skip_extract_masks=False,
        skip_regroup=True,
    )
    run_pipeline_job(_make_job(), FakeRunner(), db_path, ws_id, params)

    assert downloaded == ["superanimal-quadruped"], (
        f"Cancel after the first weights download must short-circuit the "
        f"second; expected ['superanimal-quadruped'], got {downloaded!r}"
    )


def test_pipeline_eye_keypoints_stage_download_failure_skips_stage_not_pipeline(
    tmp_path, monkeypatch,
):
    """A transient HuggingFace/network failure inside
    ensure_keypoint_weights must degrade Eye Keypoints to a skipped stage
    rather than failing the whole pipeline run. Without this, first-run /
    offline users who never asked to opt out of eye keypoints get a hard
    RuntimeError out of run_pipeline_job for an optional stage.
    """
    import config as cfg
    import pipeline as pipeline_mod
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)
    pid = db.add_photo(folder_id, "p.jpg", ".jpg", 100, 1_000_000.0)
    _drop_jpeg(folder_path, "p.jpg")
    db.save_detections(
        pid,
        [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
          "confidence": 0.9, "category": "animal"}],
        detector_model="MegaDetector",
    )
    col_id = db.add_collection(
        "Test", json.dumps([{"field": "photo_ids", "value": [pid]}]),
    )

    _stub_extract_masks_heavy_ops(monkeypatch)

    monkeypatch.setattr(
        pipeline_mod, "eye_keypoint_stage_preflight", lambda config: None,
    )
    monkeypatch.setattr(
        Database, "list_photos_for_eye_keypoint_stage",
        lambda self, **k: [
            {"id": pid, "taxonomy_class": "Mammalia", "species_conf": 0.9},
        ],
    )

    detect_called = [False]

    def fake_detect_eye_keypoints_stage(*a, **k):
        detect_called[0] = True

    monkeypatch.setattr(
        pipeline_mod, "detect_eye_keypoints_stage",
        fake_detect_eye_keypoints_stage,
    )

    import keypoints as _kp_mod

    def _ensure_raises(name, progress_callback=None):
        raise RuntimeError(
            f"Failed to download {name} weights: connection reset. "
            "Check your network connection and retry."
        )

    monkeypatch.setattr(_kp_mod, "ensure_keypoint_weights", _ensure_raises)

    params = PipelineParams(
        collection_id=col_id,
        skip_classify=True,
        skip_extract_masks=False,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()
    # Must NOT raise: an optional stage's download failure cannot tank
    # the whole pipeline run.
    result = run_pipeline_job(job, runner, db_path, ws_id, params)

    assert detect_called[0] is False, (
        "detect_eye_keypoints_stage must be skipped when weight download "
        "fails; running it would crash on missing weights."
    )

    ek_finals = [
        kw for (_, sid, kw) in runner.step_updates
        if sid == "eye_keypoints" and "summary" in kw
    ]
    assert ek_finals, (
        f"Expected eye_keypoints final update; got "
        f"step_updates={runner.step_updates!r}"
    )
    final = ek_finals[-1]
    assert final.get("status") == "completed", (
        f"eye_keypoints must finalize as completed (skipped variant), not "
        f"failed; got {final!r}"
    )
    summary = final.get("summary") or ""
    assert "Skipped" in summary and "download" in summary.lower(), (
        f"eye_keypoints summary must explain the download was skipped; "
        f"got {summary!r}"
    )

    ek_result = result.get("stages", {}).get("eye_keypoints", {})
    assert ek_result.get("skipped") == "weight_download_failed", (
        f"result.stages.eye_keypoints must record the skip reason; "
        f"got {ek_result!r}"
    )


def test_pipeline_eye_keypoints_download_cancel_finalizes_as_cancelled_not_failure(
    tmp_path, monkeypatch,
):
    """A ``ResourceWaitCancelled`` from ``ensure_keypoint_weights`` — which
    ``acquire_session_cache_lock`` raises when the bound cancel/pause
    probe fires while another thread holds the per-model download lock —
    must be finalized as a cooperative stage cancel, not misreported as
    "failed to download keypoint weights". Regression for the CodeRabbit
    review comment on ``vireo/keypoints.py:95``.
    """
    import config as cfg
    import pipeline as pipeline_mod
    from db import Database
    from resource_ledger import ResourceWaitCancelled

    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)
    pid = db.add_photo(folder_id, "p.jpg", ".jpg", 100, 1_000_000.0)
    _drop_jpeg(folder_path, "p.jpg")
    db.save_detections(
        pid,
        [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
          "confidence": 0.9, "category": "animal"}],
        detector_model="MegaDetector",
    )
    col_id = db.add_collection(
        "Test", json.dumps([{"field": "photo_ids", "value": [pid]}]),
    )

    _stub_extract_masks_heavy_ops(monkeypatch)

    monkeypatch.setattr(
        pipeline_mod, "eye_keypoint_stage_preflight", lambda config: None,
    )
    monkeypatch.setattr(
        Database, "list_photos_for_eye_keypoint_stage",
        lambda self, **k: [
            {"id": pid, "taxonomy_class": "Mammalia", "species_conf": 0.9},
        ],
    )

    detect_called = [False]

    def fake_detect_eye_keypoints_stage(*a, **k):
        detect_called[0] = True

    monkeypatch.setattr(
        pipeline_mod, "detect_eye_keypoints_stage",
        fake_detect_eye_keypoints_stage,
    )

    import keypoints as _kp_mod

    def _ensure_cancelled(name, progress_callback=None):
        raise ResourceWaitCancelled(
            f"Cancelled while waiting for {name} keypoint download"
        )

    monkeypatch.setattr(_kp_mod, "ensure_keypoint_weights", _ensure_cancelled)

    params = PipelineParams(
        collection_id=col_id,
        skip_classify=True,
        skip_extract_masks=False,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()
    # Must NOT raise: a cooperative cancel during weight-lock contention
    # cannot tank the pipeline run.
    result = run_pipeline_job(job, runner, db_path, ws_id, params)

    assert detect_called[0] is False, (
        "detect_eye_keypoints_stage must be skipped when the weight lock "
        "wait was cancelled; running it would crash on missing weights."
    )

    ek_finals = [
        kw for (_, sid, kw) in runner.step_updates
        if sid == "eye_keypoints" and "summary" in kw
    ]
    assert ek_finals, (
        f"Expected eye_keypoints final update; got "
        f"step_updates={runner.step_updates!r}"
    )
    final = ek_finals[-1]
    assert final.get("status") == "completed", (
        f"eye_keypoints must finalize as completed (cancelled variant), "
        f"not failed; got {final!r}"
    )
    summary = final.get("summary") or ""
    assert "Cancelled" in summary, (
        f"eye_keypoints summary must say Cancelled — a user cancel is not "
        f"a download failure; got {summary!r}"
    )
    assert "download" not in summary.lower(), (
        f"eye_keypoints must not attribute a cancel to a download "
        f"failure; got {summary!r}"
    )

    ek_result = result.get("stages", {}).get("eye_keypoints", {})
    assert ek_result.get("cancelled") is True, (
        f"result.stages.eye_keypoints must record the cancel; "
        f"got {ek_result!r}"
    )
    assert ek_result.get("skipped") != "weight_download_failed", (
        f"result.stages.eye_keypoints must not misattribute a cancel to "
        f"a download failure; got {ek_result!r}"
    )
    # The [eye_keypoints] error rollup must not contain a download failure
    # entry for a cooperative cancel — that would surface a scary-looking
    # download-failed banner in the job history for what was a user cancel.
    for err in job.get("errors", []):
        assert not (
            err.startswith("[eye_keypoints]") and "download" in err.lower()
        ), (
            f"[eye_keypoints] errors must not blame a cancel on a "
            f"download failure; got {err!r}"
        )


def test_pipeline_eye_keypoints_stage_excluded_photos_do_not_influence_downloads(
    tmp_path, monkeypatch,
):
    """Photos in params.exclude_photo_ids must not influence which
    SuperAnimal variants are downloaded. detect_eye_keypoints_stage already
    skips them per-photo, so pulling weights to satisfy a deselected row
    wastes bandwidth on a variant that no included photo will use.
    """
    import config as cfg
    import pipeline as pipeline_mod
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)
    pid = db.add_photo(folder_id, "p.jpg", ".jpg", 100, 1_000_000.0)
    _drop_jpeg(folder_path, "p.jpg")
    db.save_detections(
        pid,
        [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
          "confidence": 0.9, "category": "animal"}],
        detector_model="MegaDetector",
    )
    col_id = db.add_collection(
        "Test", json.dumps([{"field": "photo_ids", "value": [pid]}]),
    )

    _stub_extract_masks_heavy_ops(monkeypatch)

    monkeypatch.setattr(
        pipeline_mod, "eye_keypoint_stage_preflight", lambda config: None,
    )
    # One bird (kept) + one mammal (excluded). After exclusion, only the
    # bird variant should be downloaded — the mammal row would route to
    # quadruped, but it's deselected so that variant is wasted bandwidth.
    excluded_id = pid + 7
    monkeypatch.setattr(
        Database, "list_photos_for_eye_keypoint_stage",
        lambda self, **k: [
            {"id": pid, "taxonomy_class": "Aves", "species_conf": 0.9},
            {"id": excluded_id, "taxonomy_class": "Mammalia",
             "species_conf": 0.9},
        ],
    )
    monkeypatch.setattr(
        pipeline_mod, "detect_eye_keypoints_stage",
        lambda *a, **k: None,
    )

    downloaded = []
    import keypoints as _kp_mod
    monkeypatch.setattr(
        _kp_mod, "ensure_keypoint_weights",
        lambda name, progress_callback=None: downloaded.append(name) or "/fake",
    )

    params = PipelineParams(
        collection_id=col_id,
        skip_classify=True,
        skip_extract_masks=False,
        skip_regroup=True,
        exclude_photo_ids={excluded_id},
    )
    run_pipeline_job(_make_job(), FakeRunner(), db_path, ws_id, params)

    assert downloaded == ["superanimal-bird"], (
        f"Expected only superanimal-bird to download — the mammal row was "
        f"excluded and shouldn't influence the download planner; "
        f"got {downloaded!r}"
    )


def test_pipeline_eye_keypoints_per_run_optin_overrides_config_disabled(
    tmp_path, monkeypatch,
):
    """An explicit per-run eye opt-in — ``eye_detect_override=True`` set
    from the Process-page checkbox or an API caller — must override the
    Settings-level ``eye_detect_enabled`` so the stage actually runs even
    when Settings has eye detection off (the new default). Without this,
    the visible checkbox on the Process page is a no-op until the user
    first flips Settings, which is the very "black box" the CLAUDE.md
    philosophy forbids. ``skip_eye_keypoints=False`` alone is NOT the
    signal — ``the "Full" saved process`` also sets it to False as a
    base default (see ``test_pipeline_eye_keypoints_full_strategy_does_not_force_optin``).
    """
    import config as cfg
    import pipeline as pipeline_mod
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")
    with open(cfg.CONFIG_PATH, "w") as f:
        json.dump({"pipeline": {"eye_detect_enabled": False}}, f)

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)
    pid = db.add_photo(folder_id, "p.jpg", ".jpg", 100, 1_000_000.0)
    _drop_jpeg(folder_path, "p.jpg")
    db.save_detections(
        pid,
        [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
          "confidence": 0.9, "category": "animal"}],
        detector_model="MegaDetector",
    )
    col_id = db.add_collection(
        "Test", json.dumps([{"field": "photo_ids", "value": [pid]}]),
    )

    _stub_extract_masks_heavy_ops(monkeypatch)

    # Use the REAL preflight — the fix must feed it a config where
    # eye_detect_enabled=True regardless of the on-disk setting.
    monkeypatch.setattr(
        Database, "list_photos_for_eye_keypoint_stage",
        lambda self, **k: [
            {"id": pid, "taxonomy_class": "Aves", "species_conf": 0.9},
        ],
    )
    import keypoints as _kp_mod
    monkeypatch.setattr(
        _kp_mod, "ensure_keypoint_weights",
        lambda name, progress_callback=None: "/fake/model.onnx",
    )

    calls = {"count": 0, "configs": []}

    def fake_detect_eye_keypoints_stage(
        db_, config, progress_callback=None,
        collection_id=None, exclude_photo_ids=None,
        abort_check=None,
    ):
        calls["count"] += 1
        calls["configs"].append(dict(config))

    monkeypatch.setattr(
        pipeline_mod, "detect_eye_keypoints_stage",
        fake_detect_eye_keypoints_stage,
    )

    params = PipelineParams(
        collection_id=col_id,
        skip_classify=True,
        skip_extract_masks=False,
        skip_eye_keypoints=False,
        eye_detect_override=True,
        skip_regroup=True,
    )
    run_pipeline_job(_make_job(), FakeRunner(), db_path, ws_id, params)

    assert calls["count"] == 1, (
        f"detect_eye_keypoints_stage must run when the user opts in per-run "
        f"even though eye_detect_enabled=False in config; got "
        f"calls={calls['count']}"
    )
    assert calls["configs"][0].get("eye_detect_enabled") is True, (
        f"the per-run opt-in must surface as eye_detect_enabled=True in the "
        f"config passed to detect_eye_keypoints_stage so its internal "
        f"preflight doesn't re-skip; got config={calls['configs'][0]!r}"
    )


def test_pipeline_eye_keypoints_full_strategy_does_not_force_optin(
    tmp_path, monkeypatch,
):
    """Codex thread 5 regression guard: a ``full``-strategy chain (e.g.
    after-import) reaches ``run_pipeline_job`` with
    ``skip_eye_keypoints=False`` from ``the "Full" saved process`` — that
    is a strategy default, NOT an explicit user opt-in. When Settings has
    ``eye_detect_enabled=False`` (the new default) and no
    ``eye_detect_override`` is set, the stage-level preflight must skip
    with "Disabled in config" instead of forcing eye detection on and
    triggering SuperAnimal downloads and eye-based scoring by default.
    """
    import config as cfg
    import pipeline as pipeline_mod
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")
    with open(cfg.CONFIG_PATH, "w") as f:
        json.dump({"pipeline": {"eye_detect_enabled": False}}, f)

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)
    pid = db.add_photo(folder_id, "p.jpg", ".jpg", 100, 1_000_000.0)
    _drop_jpeg(folder_path, "p.jpg")
    db.save_detections(
        pid,
        [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
          "confidence": 0.9, "category": "animal"}],
        detector_model="MegaDetector",
    )
    col_id = db.add_collection(
        "Test", json.dumps([{"field": "photo_ids", "value": [pid]}]),
    )

    _stub_extract_masks_heavy_ops(monkeypatch)

    calls = {"count": 0}

    def fake_detect_eye_keypoints_stage(
        db_, config, progress_callback=None,
        collection_id=None, exclude_photo_ids=None,
        abort_check=None,
    ):
        calls["count"] += 1

    monkeypatch.setattr(
        pipeline_mod, "detect_eye_keypoints_stage",
        fake_detect_eye_keypoints_stage,
    )

    # Mirror what the "Full" saved process produces: skip_eye_keypoints=False
    # (base default), but no eye_detect_override (leave None so config wins).
    from process_strategies import SEED_PROCESSES, seed_flags
    expanded = seed_flags(next(s for s in SEED_PROCESSES if s["name"] == "Full"))
    assert expanded["skip_eye_keypoints"] is False
    params = PipelineParams(
        collection_id=col_id,
        skip_classify=True,
        skip_extract_masks=False,
        skip_eye_keypoints=expanded["skip_eye_keypoints"],
        skip_regroup=True,
    )
    assert params.eye_detect_override is None, (
        "strategy expansion must not set eye_detect_override — it is the "
        "explicit opt-in signal, not a strategy default"
    )
    run_pipeline_job(_make_job(), FakeRunner(), db_path, ws_id, params)

    assert calls["count"] == 0, (
        f"detect_eye_keypoints_stage must not run when Settings has "
        f"eye_detect_enabled=False and the caller did not explicitly "
        f"opt in via eye_detect_override; got calls={calls['count']}"
    )


def test_pipeline_regroup_per_run_eye_optin_reaches_scoring_config(
    tmp_path, monkeypatch,
):
    """The Process-page eye opt-in (``eye_detect_override=True``) must also
    reach regroup/scoring, not just the eye stage. When the user checks
    Eye Keypoints for a run and Settings has ``eye_detect_enabled=False``
    (the new default), the eye stage runs and writes ``eye_tenengrad`` —
    but scoring reloads the workspace config, sees eye disabled, and
    ignores those values so the checkbox never affects culling results.
    The regroup stage must mirror the eye stage's per-run override before
    calling ``run_full_pipeline``.
    """
    import config as cfg
    import pipeline as pipeline_mod
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")
    with open(cfg.CONFIG_PATH, "w") as f:
        json.dump({"pipeline": {"eye_detect_enabled": False}}, f)

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)
    pid = db.add_photo(folder_id, "p.jpg", ".jpg", 100, 1_000_000.0)
    _drop_jpeg(folder_path, "p.jpg")
    col_id = db.add_collection(
        "Test", json.dumps([{"field": "photo_ids", "value": [pid]}]),
    )

    # Give load_photo_features a photo to return so regroup does not
    # short-circuit on "No photos with pipeline features found."
    monkeypatch.setattr(
        pipeline_mod, "load_photo_features",
        lambda *a, **kw: [
            {"id": pid, "filename": "p.jpg", "timestamp": 1_000_000.0},
        ],
    )
    monkeypatch.setattr(pipeline_mod, "save_results", lambda *a, **kw: None)
    monkeypatch.setattr(
        pipeline_mod, "_resolve_collection_photo_ids",
        lambda db_, cid: {pid},
    )
    monkeypatch.setattr(
        pipeline_mod, "compute_group_fingerprint", lambda *a, **kw: "fp",
    )

    calls = {"configs": []}

    def fake_run_full_pipeline(photos, config=None, emit_trace=False):
        calls["configs"].append(dict(config or {}))
        return {"encounters": [], "photos": [], "summary": {"groups": 0}}

    monkeypatch.setattr(
        pipeline_mod, "run_full_pipeline", fake_run_full_pipeline,
    )

    params = PipelineParams(
        collection_id=col_id,
        skip_classify=True,
        skip_extract_masks=True,
        skip_eye_keypoints=False,
        eye_detect_override=True,
        skip_regroup=False,
    )
    run_pipeline_job(_make_job(), FakeRunner(), db_path, ws_id, params)

    assert calls["configs"], (
        f"run_full_pipeline must be invoked by regroup_stage; got "
        f"calls={calls['configs']!r}"
    )
    assert calls["configs"][0].get("eye_detect_enabled") is True, (
        f"the per-run eye opt-in must surface as eye_detect_enabled=True "
        f"in the pipeline_cfg passed to run_full_pipeline so scoring "
        f"honors eye_tenengrad values the eye stage just wrote; got "
        f"config={calls['configs'][0]!r}"
    )


def test_pipeline_regroup_full_strategy_default_does_not_force_scoring_config(
    tmp_path, monkeypatch,
):
    """Codex thread 5 regression guard for the regroup stage: an
    after-import ``full``-strategy chain reaches regroup with
    ``skip_eye_keypoints=False`` from the strategy default, but no
    ``eye_detect_override`` — so scoring must respect Settings'
    ``eye_detect_enabled=False`` rather than forcing eye scoring on.
    """
    import config as cfg
    import pipeline as pipeline_mod
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")
    with open(cfg.CONFIG_PATH, "w") as f:
        json.dump({"pipeline": {"eye_detect_enabled": False}}, f)

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)
    pid = db.add_photo(folder_id, "p.jpg", ".jpg", 100, 1_000_000.0)
    _drop_jpeg(folder_path, "p.jpg")
    col_id = db.add_collection(
        "Test", json.dumps([{"field": "photo_ids", "value": [pid]}]),
    )

    monkeypatch.setattr(
        pipeline_mod, "load_photo_features",
        lambda *a, **kw: [
            {"id": pid, "filename": "p.jpg", "timestamp": 1_000_000.0},
        ],
    )
    monkeypatch.setattr(pipeline_mod, "save_results", lambda *a, **kw: None)
    monkeypatch.setattr(
        pipeline_mod, "_resolve_collection_photo_ids",
        lambda db_, cid: {pid},
    )
    monkeypatch.setattr(
        pipeline_mod, "compute_group_fingerprint", lambda *a, **kw: "fp",
    )

    calls = {"configs": []}

    def fake_run_full_pipeline(photos, config=None, emit_trace=False):
        calls["configs"].append(dict(config or {}))
        return {"encounters": [], "photos": [], "summary": {"groups": 0}}

    monkeypatch.setattr(
        pipeline_mod, "run_full_pipeline", fake_run_full_pipeline,
    )

    from process_strategies import SEED_PROCESSES, seed_flags
    expanded = seed_flags(next(s for s in SEED_PROCESSES if s["name"] == "Full"))
    params = PipelineParams(
        collection_id=col_id,
        skip_classify=True,
        skip_extract_masks=True,
        skip_eye_keypoints=expanded["skip_eye_keypoints"],
        skip_regroup=False,
    )
    assert params.eye_detect_override is None
    run_pipeline_job(_make_job(), FakeRunner(), db_path, ws_id, params)

    assert calls["configs"], "run_full_pipeline must be invoked"
    assert calls["configs"][0].get("eye_detect_enabled") is False, (
        f"strategy default skip_eye_keypoints=False without an explicit "
        f"eye_detect_override must not force eye_detect_enabled=True in "
        f"scoring config: got config={calls['configs'][0]!r}"
    )


def test_pipeline_regroup_no_optin_leaves_scoring_config_untouched(
    tmp_path, monkeypatch,
):
    """When the caller sets ``skip_eye_keypoints=True`` (Process-page
    checkbox off), regroup must fall back to the Settings-level
    ``eye_detect_enabled`` value instead of forcing it on. Without this,
    unchecking the Process-page checkbox would silently ignore workspaces
    where Settings still has eye detection enabled.
    """
    import config as cfg
    import pipeline as pipeline_mod
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")
    with open(cfg.CONFIG_PATH, "w") as f:
        json.dump({"pipeline": {"eye_detect_enabled": False}}, f)

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)
    pid = db.add_photo(folder_id, "p.jpg", ".jpg", 100, 1_000_000.0)
    _drop_jpeg(folder_path, "p.jpg")
    col_id = db.add_collection(
        "Test", json.dumps([{"field": "photo_ids", "value": [pid]}]),
    )

    monkeypatch.setattr(
        pipeline_mod, "load_photo_features",
        lambda *a, **kw: [
            {"id": pid, "filename": "p.jpg", "timestamp": 1_000_000.0},
        ],
    )
    monkeypatch.setattr(pipeline_mod, "save_results", lambda *a, **kw: None)
    monkeypatch.setattr(
        pipeline_mod, "_resolve_collection_photo_ids",
        lambda db_, cid: {pid},
    )
    monkeypatch.setattr(
        pipeline_mod, "compute_group_fingerprint", lambda *a, **kw: "fp",
    )

    calls = {"configs": []}

    def fake_run_full_pipeline(photos, config=None, emit_trace=False):
        calls["configs"].append(dict(config or {}))
        return {"encounters": [], "photos": [], "summary": {"groups": 0}}

    monkeypatch.setattr(
        pipeline_mod, "run_full_pipeline", fake_run_full_pipeline,
    )

    params = PipelineParams(
        collection_id=col_id,
        skip_classify=True,
        skip_extract_masks=True,
        skip_eye_keypoints=True,
        skip_regroup=False,
    )
    run_pipeline_job(_make_job(), FakeRunner(), db_path, ws_id, params)

    assert calls["configs"], "run_full_pipeline must be invoked"
    # Settings said False and user did not opt in — value stays False.
    assert calls["configs"][0].get("eye_detect_enabled") is False, (
        f"without a per-run opt-in, regroup must not force eye detection "
        f"on: got config={calls['configs'][0]!r}"
    )


def test_pipeline_regroup_no_optin_preserves_settings_eye_on(
    tmp_path, monkeypatch,
):
    """Reverse-direction guard for the Process-page checkbox flow: when
    Settings has ``eye_detect_enabled=True`` and the user unchecks the
    Eye Keypoints checkbox on the Process page, the client must NOT send
    ``eye_detect_override=false`` — an unchecked box means "skip the
    stage", not "disable eye scoring". Regroup must fall back to
    Settings ``True`` so the run scores against existing
    ``eye_tenengrad`` values instead of silently ignoring them.
    """
    import config as cfg
    import pipeline as pipeline_mod
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")
    with open(cfg.CONFIG_PATH, "w") as f:
        json.dump({"pipeline": {"eye_detect_enabled": True}}, f)

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)
    pid = db.add_photo(folder_id, "p.jpg", ".jpg", 100, 1_000_000.0)
    _drop_jpeg(folder_path, "p.jpg")
    col_id = db.add_collection(
        "Test", json.dumps([{"field": "photo_ids", "value": [pid]}]),
    )

    monkeypatch.setattr(
        pipeline_mod, "load_photo_features",
        lambda *a, **kw: [
            {"id": pid, "filename": "p.jpg", "timestamp": 1_000_000.0},
        ],
    )
    monkeypatch.setattr(pipeline_mod, "save_results", lambda *a, **kw: None)
    monkeypatch.setattr(
        pipeline_mod, "_resolve_collection_photo_ids",
        lambda db_, cid: {pid},
    )
    monkeypatch.setattr(
        pipeline_mod, "compute_group_fingerprint", lambda *a, **kw: "fp",
    )

    calls = {"configs": []}

    def fake_run_full_pipeline(photos, config=None, emit_trace=False):
        calls["configs"].append(dict(config or {}))
        return {"encounters": [], "photos": [], "summary": {"groups": 0}}

    monkeypatch.setattr(
        pipeline_mod, "run_full_pipeline", fake_run_full_pipeline,
    )

    # Skip the stage but leave the override unset — the shape the Process
    # page now sends when the Eye Keypoints checkbox is off.
    params = PipelineParams(
        collection_id=col_id,
        skip_classify=True,
        skip_extract_masks=True,
        skip_eye_keypoints=True,
        skip_regroup=False,
    )
    assert params.eye_detect_override is None
    run_pipeline_job(_make_job(), FakeRunner(), db_path, ws_id, params)

    assert calls["configs"], "run_full_pipeline must be invoked"
    assert calls["configs"][0].get("eye_detect_enabled") is True, (
        f"a Process-page run that only skips the eye stage (no explicit "
        f"eye_detect_override) must let scoring keep Settings' "
        f"eye_detect_enabled=True: got config={calls['configs'][0]!r}"
    )


def test_detect_eye_keypoints_stage_honors_abort_check(tmp_path, monkeypatch):
    """detect_eye_keypoints_stage must accept an `abort_check` callable and
    break the per-photo loop the first time it returns True. Without this
    hook, a long eye_keypoints run swallows the user's cancel for many
    inferences.
    """
    import pipeline as pipeline_mod
    from db import Database

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)

    # Drive the loop with a controlled photos list. Bypass eligibility and
    # the route check by stubbing helpers; we only care that the abort_check
    # parameter is honored before per-photo work fires.
    monkeypatch.setattr(
        pipeline_mod, "eye_keypoint_stage_preflight", lambda config: None,
    )
    monkeypatch.setattr(
        Database, "list_photos_for_eye_keypoint_stage",
        lambda self, **k: [
            {"id": 1}, {"id": 2}, {"id": 3},
        ],
    )
    monkeypatch.setattr(
        Database, "get_folder_tree",
        lambda self: [],
    )

    process_calls = [0]

    def spy_process(*args, **kwargs):
        process_calls[0] += 1

    monkeypatch.setattr(pipeline_mod, "_process_photo_for_eye", spy_process)

    abort_after_first = [False]

    def abort_check():
        # Returns False on first poll, True on every subsequent poll. The
        # loop polls once per photo at the top of each iteration. So the
        # first photo runs, the second iteration's check breaks.
        result = abort_after_first[0]
        abort_after_first[0] = True
        return result

    pipeline_mod.detect_eye_keypoints_stage(
        db, config={}, abort_check=abort_check,
    )

    assert process_calls[0] == 1, (
        f"detect_eye_keypoints_stage must break on abort_check; "
        f"got {process_calls[0]} _process_photo_for_eye calls (expected 1)."
    )


def test_detect_eye_keypoints_stage_skips_synthetic_100pct_on_abort(
    tmp_path, monkeypatch,
):
    """When abort fires mid-loop, detect_eye_keypoints_stage must NOT emit
    the unconditional final progress(total, total) callback. That synthetic
    100% signal corrupts the wrapping eye_keypoints_stage's processed['count']
    and surfaces "Cancelled (N of N processed)" — indistinguishable from a
    clean run that processed N photos.
    """
    import pipeline as pipeline_mod
    from db import Database

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)

    monkeypatch.setattr(
        pipeline_mod, "eye_keypoint_stage_preflight", lambda config: None,
    )
    monkeypatch.setattr(
        Database, "list_photos_for_eye_keypoint_stage",
        lambda self, **k: [{"id": 1}, {"id": 2}, {"id": 3}],
    )
    monkeypatch.setattr(
        Database, "get_folder_tree",
        lambda self: [],
    )
    monkeypatch.setattr(
        pipeline_mod, "_process_photo_for_eye", lambda *a, **kw: None,
    )

    progress_events = []

    def progress_callback(phase, current, total):
        progress_events.append((current, total))

    abort_after_first = [False]

    def abort_check():
        # First poll returns False (photo 1 runs), subsequent polls True.
        result = abort_after_first[0]
        abort_after_first[0] = True
        return result

    pipeline_mod.detect_eye_keypoints_stage(
        db, config={},
        progress_callback=progress_callback,
        abort_check=abort_check,
    )

    # Stage processed exactly one photo before aborting; the wrapper must see
    # current < total on the final emit, not current == total.
    assert progress_events, "Expected at least one progress event"
    last_current, last_total = progress_events[-1]
    assert last_total == 3, f"Unexpected total in last emit: {progress_events!r}"
    assert last_current < last_total, (
        f"detect_eye_keypoints_stage must not emit progress({last_current}, "
        f"{last_total}) after abort — that 100% signal would mask the cancel "
        f"in the wrapper. Events: {progress_events!r}"
    )
    # And the count should reflect the actual photo processed (1), not 0.
    assert last_current == 1, (
        f"Expected 1 photo to be reported processed before abort; "
        f"got events={progress_events!r}"
    )


def test_detect_eye_keypoints_stage_emits_final_100pct_on_clean_run(
    tmp_path, monkeypatch,
):
    """On a clean (non-aborted) run, detect_eye_keypoints_stage must finish
    by reporting current == total so the wrapping stage's processed['count']
    matches reality.
    """
    import pipeline as pipeline_mod
    from db import Database

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)

    monkeypatch.setattr(
        pipeline_mod, "eye_keypoint_stage_preflight", lambda config: None,
    )
    monkeypatch.setattr(
        Database, "list_photos_for_eye_keypoint_stage",
        lambda self, **k: [{"id": 1}, {"id": 2}],
    )
    monkeypatch.setattr(
        Database, "get_folder_tree",
        lambda self: [],
    )
    monkeypatch.setattr(
        pipeline_mod, "_process_photo_for_eye", lambda *a, **kw: None,
    )

    progress_events = []

    def progress_callback(phase, current, total):
        progress_events.append((current, total))

    pipeline_mod.detect_eye_keypoints_stage(
        db, config={},
        progress_callback=progress_callback,
    )

    assert progress_events, "Expected at least one progress event"
    last_current, last_total = progress_events[-1]
    assert (last_current, last_total) == (2, 2), (
        f"Expected final emit (2, 2) on clean run; got {progress_events!r}"
    )


import pytest


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX permissions required")
def test_pipeline_scan_surfaces_permission_denied(tmp_path, monkeypatch):
    """Pipeline scan stage must surface kernel-level enumeration denials
    (EPERM / EACCES) into job["errors"] as a typed PERMISSION_DENIED entry,
    and accessible siblings must still be scanned. Regression: real-world
    May2026 run on /Volumes/Photography/.../2026-05-01 reported "0 photos"
    while macOS TCC was silently blocking the read.
    """
    import config as cfg
    from db import Database
    from PIL import Image

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    photo_root = tmp_path / "photos"
    ok_dir = photo_root / "ok"
    ok_dir.mkdir(parents=True)
    Image.new("RGB", (16, 16), "red").save(str(ok_dir / "ok.jpg"))

    locked_dir = photo_root / "locked"
    locked_dir.mkdir()
    Image.new("RGB", (16, 16), "blue").save(str(locked_dir / "locked.jpg"))
    os.chmod(str(locked_dir), 0o000)

    try:
        db_path = str(tmp_path / "test.db")
        db = Database(db_path)
        ws_id = db._active_workspace_id

        params = PipelineParams(
            source=str(photo_root),
            skip_classify=True,
            skip_extract_masks=True,
            skip_regroup=True,
        )

        runner = FakeRunner()
        job = _make_job()

        result = run_pipeline_job(job, runner, db_path, ws_id, params)

        denied_errors = [
            e for e in job["errors"] if "PERMISSION_DENIED" in e
        ]
        assert denied_errors, (
            f"Expected PERMISSION_DENIED in job errors. got: {job['errors']}"
        )
        assert any(str(locked_dir) in e for e in denied_errors), (
            f"Locked dir not named in denial. got: {denied_errors}"
        )

        db2 = Database(db_path)
        db2.set_active_workspace(ws_id)
        photos = db2.get_photos(per_page=100)
        names = {p["filename"] for p in photos}
        assert "ok.jpg" in names, "accessible photo must still be scanned"
        assert "locked.jpg" not in names, "denied dir must not yield photos"

        assert isinstance(result, dict)
    finally:
        os.chmod(str(locked_dir), 0o755)


def test_release_classifier_cache_handle_clears_bundle_fields():
    """After release, ``loaded_models`` must not keep the classifier alive.

    Regression for the Codex P2 on PR #899: a late-stage release that
    only popped ``_cache_handle`` left ``clf``/``labels``/``active_model``
    in ``loaded_models``. The cache refcount could then drop to 0 and the
    5-minute idle timer fire while the bundle was still strongly
    referenced — evicting the cache entry without freeing VRAM, and
    forcing a duplicate session on the next same-key acquire.
    """
    import gc
    import time
    import weakref

    from model_cache import ModelCache
    from pipeline_job import _release_classifier_cache_handle

    # Short idle window so the test exercises the actual eviction path
    # the bug occurs on (timer fires, entry drops, VRAM frees) rather
    # than just inspecting dict keys.
    cache = ModelCache(idle_secs=0.05)

    class FakeClassifier:
        pass

    box = [FakeClassifier()]
    clf_ref = weakref.ref(box[0])
    key = ("bioclip", "m1", "model_str", "/path", "fp")

    handle = cache.acquire(key, lambda: box[0])
    handle.__enter__()
    loaded_models = {
        "_cache_handle": handle,
        "clf": box[0],
        "model_type": "bioclip",
        "model_name": "M1",
        "model_str": "model_str",
        "labels": ["a", "b"],
        "use_tol": False,
        "active_model": {"id": "m1"},
        "labels_fingerprint": "fp",
        # Non-bundle keys must be preserved: pipeline-scoped, not per-model.
        "tax": object(),
        "resolved_specs": [{"id": "m1"}],
    }
    box[0] = None  # drop the test's own strong ref to the classifier

    _release_classifier_cache_handle(loaded_models)

    for k in ("_cache_handle", "clf", "model_type", "model_name",
              "model_str", "labels", "use_tol", "active_model",
              "labels_fingerprint"):
        assert k not in loaded_models, (
            f"{k!r} must be cleared so idle eviction can reclaim VRAM"
        )
    for k in ("tax", "resolved_specs"):
        assert k in loaded_models, (
            f"{k!r} is pipeline-scoped and must survive release"
        )

    # Drop the local handle ref so only the cache could plausibly pin the
    # classifier. Wait for the idle timer to evict the entry, then GC. With
    # bundle fields cleared (the fix) the weakref must collapse — meaning
    # VRAM would actually be freed. Without the fix, ``loaded_models["clf"]``
    # would survive and the weakref stays alive.
    del handle
    deadline = time.time() + 2.0
    while time.time() < deadline and cache._has_entry(key):
        time.sleep(0.01)
    gc.collect()
    assert clf_ref() is None, (
        "classifier must be collectable after release+eviction; "
        "if this fails, something in loaded_models is still pinning it"
    )


def test_release_classifier_cache_handle_idempotent_when_no_bundle():
    """Calling release on an empty dict (classify skipped pre-load) is a no-op."""
    from pipeline_job import _release_classifier_cache_handle

    loaded_models = {"tax": object(), "resolved_specs": []}
    _release_classifier_cache_handle(loaded_models)
    assert "tax" in loaded_models
    assert "resolved_specs" in loaded_models


def test_weights_fingerprint_changes_on_in_place_replacement(tmp_path):
    """A Repair / re-register that overwrites weights at the same path must
    produce a different cache-key component.

    Regression for the Codex P2 on PR #899: the classifier cache key
    only contained ``weights_path`` (a stable directory string), so a
    pipeline reusing the in-process cache after a Repair would silently
    classify with stale or corrupt session bytes until the 5-minute idle
    timer fired.
    """
    import os
    import time

    from pipeline_job import _weights_fingerprint

    files = ["image_encoder.onnx", "config.json"]
    for rel in files:
        (tmp_path / rel).write_bytes(b"v1")

    fp1 = _weights_fingerprint(str(tmp_path), files)
    assert fp1 is not None

    # Identical disk state — fingerprint must be byte-identical so cache
    # hits across pipeline runs work.
    assert _weights_fingerprint(str(tmp_path), files) == fp1

    # Bump mtime in a way the OS can resolve (st_mtime_ns is OS-dependent;
    # 10 ms covers every filesystem we care about). The size also changes
    # here, but mtime alone would be enough — in-place overwrites bump it.
    time.sleep(0.01)
    (tmp_path / "image_encoder.onnx").write_bytes(b"v2-longer")
    # Force mtime forward in case the filesystem coalesces same-second writes.
    new_mtime = os.stat(tmp_path / "image_encoder.onnx").st_mtime + 1
    os.utime(tmp_path / "image_encoder.onnx", (new_mtime, new_mtime))

    fp2 = _weights_fingerprint(str(tmp_path), files)
    assert fp2 != fp1, (
        "weights fingerprint must change after in-place file replacement "
        "so the classifier cache misses on the new bytes"
    )


def test_weights_fingerprint_handles_missing_path_and_files():
    """``None``/missing inputs must collapse to ``None`` so the cache key
    stays well-formed when nothing can be stat'd.
    """
    from pipeline_job import _weights_fingerprint

    assert _weights_fingerprint(None, ["a.onnx"]) is None
    assert _weights_fingerprint(None, None) is None
    assert _weights_fingerprint("/nonexistent/path/that/does/not/exist", None) is None
    assert _weights_fingerprint("/nonexistent/path/that/does/not/exist", []) is None


def test_weights_fingerprint_custom_model_directory(tmp_path):
    """Custom models have no declared ``files`` list, so the fingerprint
    must fall back to listing the directory. Without this, a user who
    re-registers a custom model at the same path within the idle window
    keeps hitting the cached session built from the old weights.
    """
    from pipeline_job import _weights_fingerprint

    (tmp_path / "model.onnx").write_bytes(b"v1")
    (tmp_path / "config.json").write_bytes(b"{}")
    fp1 = _weights_fingerprint(str(tmp_path), None)
    assert fp1 is not None
    assert any(part[0] == "model.onnx" for part in fp1)

    # Re-register: overwrite the .onnx with new bytes.
    import os
    import time

    time.sleep(0.01)
    (tmp_path / "model.onnx").write_bytes(b"v2-much-larger-payload")
    os.utime(tmp_path / "model.onnx", None)
    fp2 = _weights_fingerprint(str(tmp_path), None)
    assert fp2 != fp1


def test_weights_fingerprint_custom_model_single_file(tmp_path):
    """Custom models can be registered as a path to a single .onnx file
    rather than a directory; that case must still produce a fingerprint
    so in-place replacement is detected.
    """
    from pipeline_job import _weights_fingerprint

    onnx = tmp_path / "model.onnx"
    onnx.write_bytes(b"v1")
    fp1 = _weights_fingerprint(str(onnx), None)
    assert fp1 is not None

    import os
    import time

    time.sleep(0.01)
    onnx.write_bytes(b"v2-much-larger-payload")
    os.utime(onnx, None)
    fp2 = _weights_fingerprint(str(onnx), None)
    assert fp2 != fp1


def test_weights_fingerprint_missing_file_distinguishes_from_present(tmp_path):
    """A partial-Repair state (file deleted but path still listed) must
    fingerprint differently from the complete state, so the cache misses
    and the load attempt either succeeds with fresh files or raises a
    clear "incomplete" error instead of reusing a stale session.
    """
    from pipeline_job import _weights_fingerprint

    files = ["a.onnx", "b.onnx"]
    (tmp_path / "a.onnx").write_bytes(b"x")
    (tmp_path / "b.onnx").write_bytes(b"y")
    full_fp = _weights_fingerprint(str(tmp_path), files)

    (tmp_path / "b.onnx").unlink()
    partial_fp = _weights_fingerprint(str(tmp_path), files)

    assert partial_fp != full_fp
    # Missing entries collapse to a sentinel so the key still hashes.
    assert any(part[1] is None for part in partial_fp)


# ---------------------------------------------------------------------------
# Thumbnail-stage failure must not deadlock the scanner
# ---------------------------------------------------------------------------


def test_thumbnail_setup_failure_does_not_deadlock_scanner(tmp_path, monkeypatch):
    """If thumbnail_stage dies BEFORE its drain loop (import, Database(),
    cfg.load(), os.makedirs can all raise), the scanner's blocking
    scan_to_thumb.put() would wedge forever once the queue fills — the
    orchestrator's threads["scanner"].join() then never returns and the
    job leaks a pipeline slot until restart. The failure path must set
    abort and drain the queue so the producer can never block forever.

    The scan→thumbnail queue is shrunk to maxsize=2 so a handful of photos
    reproduces the >maxsize condition; the pipeline runs on a worker thread
    with a timeout so a regression fails fast instead of hanging pytest.
    """
    import queue as queue_mod

    import config as cfg
    from db import Database
    from PIL import Image

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    photo_dir = tmp_path / "photos"
    photo_dir.mkdir()
    for i in range(8):
        Image.new("RGB", (16, 16), "red").save(str(photo_dir / f"p{i}.jpg"))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    # Shrink only the pipeline's scan→thumb queue (created with maxsize=200)
    # so the scanner outruns the dead consumer after 2 photos.
    real_queue_cls = queue_mod.Queue

    def small_queue(maxsize=0):
        return real_queue_cls(maxsize=2 if maxsize == 200 else maxsize)

    monkeypatch.setattr(queue_mod, "Queue", small_queue)

    # Break thumbnail_stage before its drain loop: deleting the symbol makes
    # `from thumbnails import generate_thumbnail` raise ImportError.
    import thumbnails as thumbs_mod
    monkeypatch.delattr(thumbs_mod, "generate_thumbnail")

    params = PipelineParams(
        source=str(photo_dir),
        skip_classify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )
    runner = FakeRunner()
    job = _make_job()

    done = threading.Event()
    outcome = {}

    def _run():
        try:
            outcome["result"] = run_pipeline_job(job, runner, db_path, ws_id, params)
        except Exception as e:  # stage failure propagates as RuntimeError
            outcome["error"] = e
        finally:
            done.set()

    worker = threading.Thread(target=_run, daemon=True)
    worker.start()
    assert done.wait(60), (
        "run_pipeline_job did not terminate — the scanner is deadlocked on "
        "scan_to_thumb.put() after the thumbnail consumer died"
    )

    # The thumbnails stage must end failed and the job must propagate it.
    thumb_statuses = [
        kw["status"] for (_, sid, kw) in runner.step_updates
        if sid == "thumbnails" and "status" in kw
    ]
    assert thumb_statuses and thumb_statuses[-1] == "failed"
    assert isinstance(outcome.get("error"), RuntimeError), (
        f"expected the thumbnails stage failure to propagate, got {outcome!r}"
    )


# ---------------------------------------------------------------------------
# Previews must land under the configured thumb dir's parent
# ---------------------------------------------------------------------------


def test_previews_stage_uses_thumb_cache_dir_parent(tmp_path, monkeypatch):
    """With a custom --thumb-dir, previews_stage must write preview files,
    preview_cache rows, and run quota eviction under
    dirname(thumb_cache_dir)/previews — the root app.py serves, reconciles,
    and evicts. Writing under dirname(db_path)/previews instead leaves
    warmed previews that are never served, rows reaped as ghosts, and
    orphan JPEGs outside the quota.
    """
    import config as cfg
    from db import Database
    from PIL import Image

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    photo_dir = tmp_path / "photos"
    photo_dir.mkdir()
    Image.new("RGB", (64, 64), "green").save(str(photo_dir / "a.jpg"))

    db_dir = tmp_path / "dbhome"
    db_dir.mkdir()
    db_path = str(db_dir / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    # Custom cache root, deliberately outside dirname(db_path).
    custom_root = tmp_path / "custom_cache"
    thumb_dir = custom_root / "thumbnails"

    params = PipelineParams(
        source=str(photo_dir),
        skip_classify=True,
        skip_extract_masks=True,
        skip_regroup=True,
        preview_max_size=1920,
    )
    runner = FakeRunner()
    job = _make_job()
    run_pipeline_job(
        job, runner, db_path, ws_id, params, thumb_cache_dir=str(thumb_dir),
    )

    db2 = Database(db_path)
    db2.set_active_workspace(ws_id)
    photo_id = db2.get_photos(per_page=1)[0]["id"]

    served_path = custom_root / "previews" / f"{photo_id}_1920.jpg"
    assert served_path.is_file(), (
        "preview must be written under dirname(thumb_cache_dir)/previews "
        "to match the Flask serve convention"
    )
    orphan_path = db_dir / "previews" / f"{photo_id}_1920.jpg"
    assert not orphan_path.exists(), (
        "preview must NOT be written under dirname(db_path)/previews when "
        "a custom thumb_cache_dir is configured"
    )
    # The preview_cache row must account for the file the app will serve.
    assert db2.preview_cache_get(photo_id, 1920) is not None


def test_pipeline_previews_honor_raw_failure_marker_after_source_selection(
    tmp_path, monkeypatch,
):
    import config as cfg
    import image_loader
    import scanner
    import thumbnails
    from db import Database
    from PIL import Image

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    photo_dir = tmp_path / "photos"
    photo_dir.mkdir()
    raw_path = photo_dir / "source.NEF"
    raw_path.write_bytes(b"raw")
    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    folder_id = db.add_folder(str(photo_dir), name="photos")
    photo_id = db.add_photo(
        folder_id=folder_id,
        filename="source.NEF",
        extension=".nef",
        file_size=raw_path.stat().st_size,
        file_mtime=1234.0,
        width=600,
        height=400,
    )
    working_dir = tmp_path / "working"
    working_dir.mkdir()
    Image.new("RGB", (400, 400), "blue").save(str(working_dir / f"{photo_id}.jpg"))
    db.conn.execute(
        """UPDATE photos
           SET working_copy_path=?,
               working_copy_failed_at=datetime('now'),
               working_copy_failed_mtime=1234.0,
               working_copy_failed_source='source'
           WHERE id=?""",
        (f"working/{photo_id}.jpg", photo_id),
    )
    db.conn.commit()
    db.set_photo_edit_recipe(
        photo_id,
        {"crop": {"x": 0, "y": 0, "w": 0.5, "h": 1}},
    )
    collection_id = db.add_collection("Test", json.dumps([]))

    def fake_generate_thumbnail(photo_id, photo_path, cache_dir, size=300, **kwargs):
        os.makedirs(cache_dir, exist_ok=True)
        thumb_path = os.path.join(cache_dir, f"{photo_id}.jpg")
        Image.new("RGB", (size, size), "green").save(thumb_path)
        return thumb_path

    monkeypatch.setattr(thumbnails, "generate_thumbnail", fake_generate_thumbnail)
    monkeypatch.setattr(scanner, "extract_working_copy", lambda *args, **kwargs: False)

    original_load_image = image_loader.load_image
    raw_loads = []

    def tracking_load_image(file_path, max_size=1024):
        if os.path.abspath(str(file_path)) == os.path.abspath(str(raw_path)):
            raw_loads.append(file_path)
            raise AssertionError("pipeline preview retried failed RAW")
        return original_load_image(file_path, max_size=max_size)

    monkeypatch.setattr(image_loader, "load_image", tracking_load_image)

    result = run_pipeline_job(
        _make_job(),
        FakeRunner(),
        db_path,
        ws_id,
        PipelineParams(
            collection_id=collection_id,
            skip_classify=True,
            skip_extract_masks=True,
            skip_regroup=True,
            preview_max_size=1920,
        ),
    )

    previews = result["stages"]["previews"]
    assert previews["failed"] == 0
    assert previews["generated"] == 0
    assert previews["skipped"] == 1
    assert raw_loads == []
    assert db.preview_cache_get(photo_id, 1920) is None


def test_pipeline_previews_warm_unedited_raw_from_camera_rendered_source(
    tmp_path, monkeypatch,
):
    """Pipeline preview stage must warm from the RAW source, not the
    highlight-preserving working copy. Otherwise the tracked preview cache
    locks in the dark render and /photos/<id>/preview returns those cache
    hits before its own RAW-source branch ever runs."""
    import config as cfg
    import image_loader
    import scanner
    import thumbnails
    from db import Database
    from PIL import Image

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    photo_dir = tmp_path / "photos"
    photo_dir.mkdir()
    raw_path = photo_dir / "source.NEF"
    raw_path.write_bytes(b"raw bytes decoded by the test double")
    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    folder_id = db.add_folder(str(photo_dir), name="photos")
    photo_id = db.add_photo(
        folder_id=folder_id,
        filename="source.NEF",
        extension=".nef",
        file_size=raw_path.stat().st_size,
        file_mtime=raw_path.stat().st_mtime,
        width=800,
        height=600,
    )
    working_dir = tmp_path / "working"
    working_dir.mkdir()
    working_path = working_dir / f"{photo_id}.jpg"
    Image.new("RGB", (800, 600), (25, 25, 25)).save(str(working_path))
    # Setting exif_data non-null keeps _find_broken_metadata_folders from
    # flagging this row for a repair scan that would otherwise call
    # extract_working_copy against the placeholder RAW bytes and mark the
    # source as failed — masking the source-selection behavior we want to
    # exercise here.
    db.conn.execute(
        "UPDATE photos SET working_copy_path=?, exif_data='{}' WHERE id=?",
        (f"working/{photo_id}.jpg", photo_id),
    )
    db.conn.commit()
    collection_id = db.add_collection("Test", json.dumps([]))

    def fake_generate_thumbnail(photo_id, photo_path, cache_dir, size=300, **kwargs):
        os.makedirs(cache_dir, exist_ok=True)
        thumb_path = os.path.join(cache_dir, f"{photo_id}.jpg")
        Image.new("RGB", (size, size), "green").save(thumb_path)
        return thumb_path

    monkeypatch.setattr(thumbnails, "generate_thumbnail", fake_generate_thumbnail)
    monkeypatch.setattr(scanner, "extract_working_copy", lambda *args, **kwargs: False)

    loaded = []

    def tracking_load_image(file_path, max_size=1024, **kwargs):
        loaded.append(os.fspath(file_path))
        color = (
            (220, 220, 220)
            if os.path.abspath(str(file_path)) == os.path.abspath(str(raw_path))
            else (25, 25, 25)
        )
        return Image.new("RGB", (800, 600), color)

    monkeypatch.setattr(image_loader, "load_image", tracking_load_image)

    result = run_pipeline_job(
        _make_job(),
        FakeRunner(),
        db_path,
        ws_id,
        PipelineParams(
            collection_id=collection_id,
            skip_classify=True,
            skip_extract_masks=True,
            skip_regroup=True,
            preview_max_size=1920,
        ),
    )

    previews = result["stages"]["previews"]
    assert previews["failed"] == 0
    assert previews["generated"] == 1
    # The warmer decoded the RAW source, not the dark working copy.
    assert str(raw_path) in loaded
    assert str(working_path) not in loaded
    # base_dir = dirname(db_path) when no thumb_cache_dir override; matches
    # the pipeline's effective_vireo_dir setup at pipeline_job.py:758.
    preview_path = tmp_path / "previews" / f"{photo_id}_1920.jpg"
    assert preview_path.exists()
    with Image.open(preview_path) as warmed:
        assert warmed.getpixel((400, 300))[0] > 200


def test_pipeline_scan_thumbnails_use_recipe_source_before_live_raw(
    tmp_path, monkeypatch,
):
    import config as cfg
    import scanner
    import thumbnails
    from db import Database
    from PIL import Image

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    photo_dir = tmp_path / "photos"
    photo_dir.mkdir()
    raw_path = photo_dir / "source.NEF"
    raw_path.write_bytes(b"raw")
    companion_path = photo_dir / "source.jpg"
    Image.new("RGB", (800, 600), "blue").save(companion_path)

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    scanned = {"done": False}

    def fake_scan(
        root,
        scan_db,
        progress_callback=None,
        photo_callback=None,
        **_kwargs,
    ):
        folder_id = scan_db.add_folder(str(root), name="photos")
        photo_id = scan_db.add_photo(
            folder_id=folder_id,
            filename="source.NEF",
            extension=".nef",
            file_size=raw_path.stat().st_size,
            file_mtime=1234.0,
            width=800,
            height=600,
        )
        scan_db.conn.execute(
            """UPDATE photos
               SET companion_path='source.jpg',
                   working_copy_path=NULL,
                   working_copy_failed_at=datetime('now'),
                   working_copy_failed_mtime=1234.0,
                   working_copy_failed_source='source'
               WHERE id=?""",
            (photo_id,),
        )
        scan_db.conn.commit()
        scan_db.set_photo_edit_recipe(
            photo_id,
            {"crop": {"x": 0, "y": 0, "w": 0.5, "h": 1}},
        )
        scanned["done"] = True
        if progress_callback:
            progress_callback(1, 1)
        if photo_callback:
            photo_callback(photo_id, str(raw_path))

    thumbnail_sources = []

    def fake_generate_thumbnail(photo_id, photo_path, cache_dir, size=300, **kwargs):
        thumbnail_sources.append(photo_path)
        if os.path.abspath(str(photo_path)) == os.path.abspath(str(raw_path)):
            raise AssertionError("scan thumbnail used live RAW path")
        assert kwargs.get("recipe")
        os.makedirs(cache_dir, exist_ok=True)
        thumb_path = os.path.join(cache_dir, f"{photo_id}.jpg")
        Image.new("RGB", (size, size), "green").save(thumb_path)
        return thumb_path

    monkeypatch.setattr(scanner, "scan", fake_scan)
    monkeypatch.setattr(thumbnails, "generate_thumbnail", fake_generate_thumbnail)

    result = run_pipeline_job(
        _make_job(),
        FakeRunner(),
        db_path,
        ws_id,
        PipelineParams(
            source=str(photo_dir),
            skip_classify=True,
            skip_extract_masks=True,
            skip_regroup=True,
        ),
    )

    assert scanned["done"] is True
    assert result["stages"]["thumbnails"]["failed"] == 0
    assert thumbnail_sources == [str(companion_path)]


def test_pipeline_scan_thumbnails_honor_raw_marker_after_source_selection(
    tmp_path, monkeypatch,
):
    import config as cfg
    import scanner
    import thumbnails
    from db import Database
    from PIL import Image

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    photo_dir = tmp_path / "photos"
    photo_dir.mkdir()
    raw_path = photo_dir / "source.NEF"
    raw_path.write_bytes(b"raw")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    def fake_scan(
        root,
        scan_db,
        progress_callback=None,
        photo_callback=None,
        **_kwargs,
    ):
        folder_id = scan_db.add_folder(str(root), name="photos")
        photo_id = scan_db.add_photo(
            folder_id=folder_id,
            filename="source.NEF",
            extension=".nef",
            file_size=raw_path.stat().st_size,
            file_mtime=1234.0,
            width=800,
            height=600,
        )
        working_dir = tmp_path / "working"
        working_dir.mkdir()
        Image.new("RGB", (200, 150), "blue").save(
            working_dir / f"{photo_id}.jpg",
        )
        scan_db.conn.execute(
            """UPDATE photos
               SET working_copy_path=?,
                   working_copy_failed_at=datetime('now'),
                   working_copy_failed_mtime=1234.0,
                   working_copy_failed_source='source'
               WHERE id=?""",
            (f"working/{photo_id}.jpg", photo_id),
        )
        scan_db.conn.commit()
        scan_db.set_photo_edit_recipe(
            photo_id,
            {"crop": {"x": 0, "y": 0, "w": 0.5, "h": 1}},
        )
        if progress_callback:
            progress_callback(1, 1)
        if photo_callback:
            photo_callback(photo_id, str(raw_path))

    def fail_if_called(*_args, **_kwargs):
        raise AssertionError("scan thumbnail retried failed RAW")

    monkeypatch.setattr(scanner, "scan", fake_scan)
    monkeypatch.setattr(thumbnails, "generate_thumbnail", fail_if_called)

    result = run_pipeline_job(
        _make_job(),
        FakeRunner(),
        db_path,
        ws_id,
        PipelineParams(
            source=str(photo_dir),
            skip_classify=True,
            skip_extract_masks=True,
            skip_regroup=True,
        ),
    )

    thumbnails_stage = result["stages"]["thumbnails"]
    assert thumbnails_stage["failed"] == 1
    assert thumbnails_stage["skipped"] == 0
    assert thumbnails_stage["failed_photos"][0]["filename"] == "source.NEF"
    assert result["warnings"] == [
        "[thumbnails] 1 of 1 thumbnails need attention"
    ]


# ---------------------------------------------------------------------------
# Aborted pipelines must not leave step rows stuck at "pending"
# ---------------------------------------------------------------------------


def test_pipeline_abort_finalizes_all_step_rows(tmp_path, monkeypatch):
    """When the pipeline aborts early (user cancel here), every step row
    created by runner.set_steps — including previews, extract_masks,
    eye_keypoints, regroup, and misses — must reach a terminal status.
    Gating those stage calls on `if not abort.is_set()` left their rows
    persisted as "pending" with no finished_at, forever (the same defect
    previously fixed for detect/classify).
    """
    import config as cfg
    from db import Database
    from PIL import Image

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    photo_dir = tmp_path / "photos"
    photo_dir.mkdir()
    for name in ("a.jpg", "b.jpg"):
        Image.new("RGB", (16, 16), "blue").save(str(photo_dir / name))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    # All optional stages enabled so their step rows exist; classify is
    # skipped to keep the test free of model files.
    params = PipelineParams(
        source=str(photo_dir),
        skip_classify=True,
    )
    runner = FakeRunner()
    job = _make_job()
    # Pre-cancel: the cancel watcher sets the local abort event immediately,
    # so the post-scan stages all hit their abort skip paths.
    runner.cancelled_ids.add(job["id"])

    run_pipeline_job(job, runner, db_path, ws_id, params)

    terminal = {"completed", "failed", "cancelled"}
    for step in runner.steps_defined:
        statuses = [
            kw["status"] for (_, sid, kw) in runner.step_updates
            if sid == step["id"] and "status" in kw
        ]
        final = statuses[-1] if statuses else None
        assert final in terminal, (
            f"Step {step['id']!r} must reach a terminal status on an aborted "
            f"pipeline, got {final!r} (history={statuses})"
        )


def test_pipeline_regroup_failure_finalizes_miss_step_row(tmp_path, monkeypatch):
    """When regroup_stage fails (without abort), miss_stage must still be
    invoked so the misses row reaches a terminal "Skipped" state instead of
    persisting as pending — while still never touching miss_* DB state
    (regroup's burst_id output is its prerequisite).
    """
    import config as cfg
    import pipeline as pipeline_mod
    from db import Database
    from PIL import Image

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    photo_dir = tmp_path / "photos"
    photo_dir.mkdir()
    Image.new("RGB", (16, 16), "black").save(str(photo_dir / "a.jpg"))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    def _boom(*args, **kwargs):
        raise RuntimeError("synthetic regroup failure")

    monkeypatch.setattr(pipeline_mod, "run_full_pipeline", _boom)

    params = PipelineParams(
        source=str(photo_dir),
        skip_classify=True,
        skip_extract_masks=True,
    )
    runner = FakeRunner()
    job = _make_job()

    with contextlib.suppress(Exception):
        run_pipeline_job(job, runner, db_path, ws_id, params)

    miss_statuses = [
        kw["status"] for (_, sid, kw) in runner.step_updates
        if sid == "misses" and "status" in kw
    ]
    assert miss_statuses, "misses row got no status update after regroup failure"
    assert miss_statuses[-1] == "completed", (
        f"misses must finalize as a skipped/completed row, got {miss_statuses}"
    )
    # The skip path must not have mutated miss state.
    db2 = Database(db_path)
    db2.set_active_workspace(ws_id)
    rows = db2.conn.execute("SELECT miss_computed_at FROM photos").fetchall()
    assert rows and all(r["miss_computed_at"] is None for r in rows)


# ---------------------------------------------------------------------------
# Remote (SSH) archive destination — resolve_remote_archive unit tests plus
# end-to-end pipeline runs with the SSH/rsync seams monkeypatched, following
# the fake pattern in test_move_remote.py.
# ---------------------------------------------------------------------------

import pytest
from pipeline_job import resolve_remote_archive


def _remote_target(tmp_path, **overrides):
    target = {
        "id": "nas1", "name": "NAS", "host": "nas", "user": "me",
        "port": 22, "ssh_key": "", "bwlimit_kbps": 0,
        "remote_path": "/volume1/Photography",
        "mount_path": str(tmp_path / "mount"),
    }
    target.update(overrides)
    return target


def test_resolve_remote_archive_joins_both_bases(tmp_path):
    ctx = resolve_remote_archive(_remote_target(tmp_path), "2026/trip")
    assert ctx["subpath"] == "2026/trip"
    assert ctx["parent_subpath"] == "2026"
    assert ctx["ssh_final"] == "/volume1/Photography/2026/trip"
    assert ctx["mount_final"] == os.path.join(
        str(tmp_path / "mount"), "2026", "trip")
    assert ctx["display"] == "me@nas:/volume1/Photography/2026/trip"


def test_resolve_remote_archive_single_segment_subpath(tmp_path):
    ctx = resolve_remote_archive(_remote_target(tmp_path), "trip")
    # A single segment has no parent: the move spec's bases are the target's
    # own base paths and the staged leaf ("trip") lands directly under them.
    assert ctx["parent_subpath"] == ""
    assert ctx["ssh_final"] == "/volume1/Photography/trip"


def test_resolve_remote_archive_rejects_bad_input(tmp_path):
    with pytest.raises(ValueError, match="remote_subpath is required"):
        resolve_remote_archive(_remote_target(tmp_path), "")
    with pytest.raises(ValueError):
        resolve_remote_archive(_remote_target(tmp_path), "../escape")
    with pytest.raises(ValueError, match="mount path"):
        resolve_remote_archive(
            _remote_target(tmp_path, mount_path=""), "trip")
    with pytest.raises(ValueError, match="absolute"):
        resolve_remote_archive(
            _remote_target(tmp_path, mount_path="Photos"), "trip")


def test_pipeline_params_remote_archive_defaults():
    params = PipelineParams(collection_id=1)
    assert params.remote_target_id is None
    assert params.remote_subpath == ""
    assert params.remote_target_snapshot is None


class _RemoteArchiveRunner(FakeRunner):
    """FakeRunner + the uncancellable handshake archive_stage requires."""

    def begin_uncancellable(self, job_id):
        return True


def _remote_env(tmp_path, monkeypatch, mount_path=None):
    """Config + SSH/rsync seam fakes for a remote-archive pipeline run.

    Every subprocess-backed seam in move.py is replaced (same seams
    test_move_remote.py fakes) so no test ever shells out to ssh/rsync.
    Returns a dict of captured rsync calls plus the paths the assertions
    need.
    """
    import config as cfg
    import local_processing
    import move as move_mod
    from db import Database
    from PIL import Image

    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    mount = mount_path or str(tmp_path / "mount")
    cfg.save({"remote_targets": [{
        "id": "nas1", "name": "NAS", "host": "nas", "user": "me",
        "remote_path": "/volume1/Photography",
        "mount_path": mount,
    }]})

    monkeypatch.setattr(local_processing, "MIN_DERIVED_OVERHEAD_BYTES", 0)
    monkeypatch.setattr(local_processing, "RESERVED_FREE_BYTES", 0)

    captured = {"rsync_calls": []}

    def fake_rsync(src_path, dest_spec, flags, total, cb, rsync_bin="rsync",
                   extra_args=None, **kw):
        captured["rsync_calls"].append({
            "src": src_path,
            "dest_spec": dest_spec,
            "flags": list(flags or []),
            "rsync_bin": rsync_bin,
            "extra_args": list(extra_args or []),
        })
        return (0, "", False)

    monkeypatch.setattr(
        move_mod, "resolve_rsync_bin", lambda configured="": "/usr/bin/rsync")
    monkeypatch.setattr(move_mod, "is_gnu_rsync", lambda p: True)
    monkeypatch.setattr(
        move_mod, "test_remote_connection",
        lambda t, r: {"ok": True, "message": "Connection OK"})
    monkeypatch.setattr(
        move_mod, "_remote_free_bytes", lambda t, p: 100 * 1024 ** 3)
    monkeypatch.setattr(move_mod, "_remote_dir_exists", lambda r, p: False)
    monkeypatch.setattr(move_mod, "_remote_mkdir_p", lambda r, p: (True, ""))
    monkeypatch.setattr(
        move_mod, "_find_remote_content_conflict", lambda *a, **k: None)
    monkeypatch.setattr(
        move_mod, "_remote_verify_complete", lambda *a, **k: None)
    monkeypatch.setattr(move_mod, "_run_rsync_streamed", fake_rsync)

    src = tmp_path / "card"
    src.mkdir()
    Image.new("RGB", (16, 16), "white").save(str(src / "test.jpg"))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    return {
        "captured": captured, "src": src, "db_path": db_path, "db": db,
        "ws_id": ws_id, "mount": mount, "tmp_path": tmp_path,
    }


def _remote_params(env, **overrides):
    kwargs = {
        "sources": [str(env["src"])],
        "local_processing": True,
        "remote_target_id": "nas1",
        "remote_subpath": "2026/trip",
        "folder_template": "",
        "skip_classify": True,
        "skip_extract_masks": True,
        "skip_regroup": True,
    }
    kwargs.update(overrides)
    return PipelineParams(**kwargs)


@pytest.mark.skip(reason="retired pipeline remote archive stage")
def test_pipeline_remote_archive_end_to_end(tmp_path, monkeypatch):
    """A local-processing run with a remote target stages locally, then
    archives over SSH: rsync is pointed at the NAS-side subpath with
    --partial-dir resume semantics, the catalog is repointed at the mount
    path, and staging is cleaned up."""
    env = _remote_env(tmp_path, monkeypatch)
    runner = _RemoteArchiveRunner()
    job = _make_job()

    result = run_pipeline_job(
        job, runner, env["db_path"], env["ws_id"], _remote_params(env))

    mount_final = os.path.join(env["mount"], "2026", "trip")

    # The archive result names both views of the destination.
    assert result["archive"]["final_destination"] == mount_final
    assert result["archive"]["moved"] == 1
    assert result["archive"]["remote"]["ssh_destination"] == \
        "/volume1/Photography/2026/trip"
    assert result["archive"]["remote"]["target_name"] == "NAS"

    # rsync addressed the NAS-side subpath, over SSH, with the same
    # merge-on-retry transport shape the Move page uses.
    call = env["captured"]["rsync_calls"][-1]
    assert call["dest_spec"] == "me@nas:/volume1/Photography/2026/trip"
    assert call["rsync_bin"] == "/usr/bin/rsync"
    assert "-e" in call["extra_args"]
    assert "--partial-dir=.rsync-partial" in call["extra_args"]
    # Fresh destination — not a merge, so no --ignore-existing.
    assert "--ignore-existing" not in call["flags"]

    # Catalog repointed at the mount path (photos stay in the library).
    from db import Database
    check_db = Database(env["db_path"])
    row = check_db.conn.execute(
        "SELECT id FROM folders WHERE path = ?", (mount_final,)).fetchone()
    assert row is not None
    photo = check_db.conn.execute(
        "SELECT filename FROM photos WHERE folder_id = ?", (row["id"],),
    ).fetchone()
    assert photo["filename"] == "test.jpg"

    # Staging cleaned up (move_folder rmtree'd it after the verify).
    staging = tmp_path / "staging"
    assert not staging.exists() or not any(staging.rglob("*.jpg"))

    # Storage-step summary reports the remote free-space check honestly.
    storage_summaries = [
        kw.get("summary", "") for _, sid, kw in runner.step_updates
        if sid == "storage" and kw.get("status") == "completed"
    ]
    assert storage_summaries and "free at NAS" in storage_summaries[-1]
    assert result["local_processing"]["remote"]["free_space_checked"] is True


@pytest.mark.skip(reason="retired pipeline remote archive stage")
def test_pipeline_remote_archive_preflight_refuses_without_rsync(
    tmp_path, monkeypatch,
):
    """No GNU rsync -> the storage preflight fails BEFORE anything is staged
    or processed, with an actionable message."""
    import move as move_mod
    env = _remote_env(tmp_path, monkeypatch)
    monkeypatch.setattr(
        move_mod, "resolve_rsync_bin", lambda configured="": None)
    runner = _RemoteArchiveRunner()
    job = _make_job()

    with pytest.raises(RuntimeError):
        run_pipeline_job(
            job, runner, env["db_path"], env["ws_id"], _remote_params(env))

    assert any(
        "[storage] Fatal" in e and "rsync" in e.lower() for e in job["errors"]
    ), job["errors"]
    # Nothing was staged — the refusal beat the staging mkdir/copy.
    assert not (tmp_path / "staging").exists()
    # And no transfer was ever attempted.
    assert env["captured"]["rsync_calls"] == []


@pytest.mark.skip(reason="retired pipeline remote archive stage")
def test_pipeline_remote_archive_preflight_refuses_when_connection_fails(
    tmp_path, monkeypatch,
):
    import move as move_mod
    env = _remote_env(tmp_path, monkeypatch)
    monkeypatch.setattr(
        move_mod, "test_remote_connection",
        lambda t, r: {"ok": False, "message": "SSH connection failed - check host"})
    runner = _RemoteArchiveRunner()
    job = _make_job()

    with pytest.raises(RuntimeError):
        run_pipeline_job(
            job, runner, env["db_path"], env["ws_id"], _remote_params(env))

    fatal = [e for e in job["errors"] if "[storage] Fatal" in e]
    assert fatal, job["errors"]
    # The error names the target and carries the connection test's message.
    assert "NAS" in fatal[0]
    assert "SSH connection failed" in fatal[0]
    assert not (tmp_path / "staging").exists()


@pytest.mark.skip(reason="retired pipeline remote archive stage")
def test_pipeline_remote_archive_space_probe_failure_degrades(
    tmp_path, monkeypatch,
):
    """A df probe failure must not fail (or fake) the preflight: the run
    proceeds, the summary says the check was skipped, and the result
    payload marks it unchecked."""
    import move as move_mod
    env = _remote_env(tmp_path, monkeypatch)
    monkeypatch.setattr(move_mod, "_remote_free_bytes", lambda t, p: None)
    runner = _RemoteArchiveRunner()
    job = _make_job()

    result = run_pipeline_job(
        job, runner, env["db_path"], env["ws_id"], _remote_params(env))

    assert result["archive"]["moved"] == 1
    remote_info = result["local_processing"]["remote"]
    assert remote_info["free_space_checked"] is False
    storage_summaries = [
        kw.get("summary", "") for _, sid, kw in runner.step_updates
        if sid == "storage" and kw.get("status") == "completed"
    ]
    assert storage_summaries, runner.step_updates
    assert "free-space check skipped" in storage_summaries[-1]


@pytest.mark.skip(reason="retired pipeline remote archive stage")
def test_pipeline_remote_archive_refuses_when_remote_volume_full(
    tmp_path, monkeypatch,
):
    import move as move_mod
    env = _remote_env(tmp_path, monkeypatch)
    monkeypatch.setattr(move_mod, "_remote_free_bytes", lambda t, p: 0)
    runner = _RemoteArchiveRunner()
    job = _make_job()

    with pytest.raises(RuntimeError):
        run_pipeline_job(
            job, runner, env["db_path"], env["ws_id"], _remote_params(env))

    fatal = [e for e in job["errors"] if "[storage] Fatal" in e]
    assert fatal, job["errors"]
    assert "Remote archive needs about" in fatal[0]
    assert "me@nas:/volume1/Photography/2026/trip" in fatal[0]
    # No transfer was attempted.
    assert env["captured"]["rsync_calls"] == []


@pytest.mark.skip(reason="retired pipeline remote archive stage")
def test_pipeline_remote_archive_merges_on_retry(tmp_path, monkeypatch):
    """An existing remote destination (an earlier interrupted archive) makes
    the archive move a merge/resume: --ignore-existing so already-present
    files are never overwritten, still with --partial-dir resume."""
    import move as move_mod
    env = _remote_env(tmp_path, monkeypatch)
    monkeypatch.setattr(move_mod, "_remote_dir_exists", lambda r, p: True)
    runner = _RemoteArchiveRunner()
    job = _make_job()

    result = run_pipeline_job(
        job, runner, env["db_path"], env["ws_id"], _remote_params(env))

    assert result["archive"]["moved"] == 1
    call = env["captured"]["rsync_calls"][-1]
    assert "--ignore-existing" in call["flags"]
    assert "--partial-dir=.rsync-partial" in call["extra_args"]


@pytest.mark.skip(reason="retired pipeline remote archive stage")
def test_pipeline_remote_archive_failure_deindexes_staging(
    tmp_path, monkeypatch,
):
    """A failed remote transfer must deindex the staged folder/photos —
    otherwise a retry of the same source would hit ingest()'s duplicate
    skip and publish an empty archive — while leaving the staged files on
    disk for recovery. Mirrors the local archive-failure contract."""
    import move as move_mod
    env = _remote_env(tmp_path, monkeypatch)
    monkeypatch.setattr(
        move_mod, "_run_rsync_streamed",
        lambda *a, **k: (1, "rsync error: connection reset", False))
    runner = _RemoteArchiveRunner()
    job = _make_job()

    with pytest.raises(RuntimeError):
        run_pipeline_job(
            job, runner, env["db_path"], env["ws_id"], _remote_params(env))

    fatal = [e for e in job["errors"] if "[archive] Fatal" in e]
    assert fatal, job["errors"]
    # The error names the remote destination, not just the rsync stderr.
    assert "me@nas:/volume1/Photography/2026/trip" in fatal[0]
    assert "connection reset" in fatal[0]
    assert "local staging" in fatal[0]

    # Staged files remain on disk for manual recovery...
    staged = list((tmp_path / "staging").rglob("test.jpg"))
    assert staged, "staged files must remain on disk after a failed archive"
    # ...but the catalog rows are gone, so a retry re-ingests them.
    from db import Database
    check_db = Database(env["db_path"])
    count = check_db.conn.execute(
        "SELECT COUNT(*) AS n FROM photos").fetchone()["n"]
    assert count == 0


@pytest.mark.skip(reason="retired pipeline remote archive stage")
def test_pipeline_remote_archive_snapshot_wins_over_mutated_settings(
    tmp_path, monkeypatch,
):
    """A queued pipeline must archive to the target the user saw at Start,
    not whatever the saved target got edited to before the pipeline slot
    opened. Editing ``remote_targets`` after PipelineParams is built must not
    redirect the archive — the ``remote_target_snapshot`` wins over the
    mutable ``cfg.get_remote_target`` lookup."""
    import config as cfg
    env = _remote_env(tmp_path, monkeypatch)
    snapshot = cfg.get_remote_target("nas1")
    assert snapshot["host"] == "nas"
    assert snapshot["remote_path"] == "/volume1/Photography"

    # Simulate a settings edit between click-Start and slot-open: same id,
    # different host / user / remote_path / mount_path. If the job re-reads
    # Settings the archive lands in the hijacked place.
    hijacked_mount = str(tmp_path / "other-mount")
    cfg.save({"remote_targets": [{
        "id": "nas1", "name": "NAS-edited", "host": "attacker",
        "user": "root", "remote_path": "/tmp/hijack",
        "mount_path": hijacked_mount,
    }]})

    params = _remote_params(env, remote_target_snapshot=dict(snapshot))
    runner = _RemoteArchiveRunner()
    job = _make_job()

    result = run_pipeline_job(
        job, runner, env["db_path"], env["ws_id"], params)

    call = env["captured"]["rsync_calls"][-1]
    assert call["dest_spec"] == "me@nas:/volume1/Photography/2026/trip", (
        "rsync must address the snapshot's host + remote_path, not the "
        "post-edit target"
    )
    assert "attacker" not in call["dest_spec"]
    assert "/tmp/hijack" not in call["dest_spec"]

    # Catalog was repointed at the snapshot's mount path (photos stay in the
    # user's original library), not the hijacked mount.
    assert result["archive"]["final_destination"] == os.path.join(
        env["mount"], "2026", "trip")
    assert hijacked_mount not in result["archive"]["final_destination"]
    # And the job's archive-result payload reflects the snapshot's target
    # name, not the edited one.
    assert result["archive"]["remote"]["target_name"] == "NAS"


@pytest.mark.skip(reason="retired pipeline remote archive stage")
def test_pipeline_remote_archive_falls_back_to_settings_without_snapshot(
    tmp_path, monkeypatch,
):
    """When no snapshot is present (older direct-call test paths) the run
    still resolves the target from Settings so existing callers keep
    working."""
    env = _remote_env(tmp_path, monkeypatch)
    runner = _RemoteArchiveRunner()
    job = _make_job()

    # Note: no ``remote_target_snapshot`` — falls back to cfg.get_remote_target.
    result = run_pipeline_job(
        job, runner, env["db_path"], env["ws_id"], _remote_params(env))

    assert result["archive"]["moved"] == 1
    call = env["captured"]["rsync_calls"][-1]
    assert call["dest_spec"] == "me@nas:/volume1/Photography/2026/trip"


# ---------------------------------------------------------------------------
# miss_enabled per-run override (process strategies, import/process split PR 1)
# ---------------------------------------------------------------------------


def test_pipeline_params_miss_enabled_defaults_none():
    """None means "defer to workspace config" — today's behavior."""
    params = PipelineParams()
    assert params.miss_enabled is None


def _run_pipeline_for_miss_tests(tmp_path, monkeypatch, *, pipeline_cfg,
                                 params_extra, expect_runtime_error=False):
    """Run a collection pipeline far enough to reach miss_stage.

    classify must complete (model_loader failure sets abort, and the misses
    gate skips on abort), so install a fake downloaded model, a fake
    Classifier, and a fake _detect_batch. regroup must succeed (the misses
    gate skips when regroup failed), so stub run_full_pipeline /
    save_results / load_photo_features. Returns (runner, result, spy_calls)
    where spy_calls captures every compute_misses_for_workspace invocation.

    ``expect_runtime_error`` is opt-in: the setup-failure test needs to swallow
    the RuntimeError so it can inspect ``job["_fatal_error"]``, but the
    override tests must let unexpected pipeline aborts propagate — otherwise
    an earlier stage failing would make miss_stage skip for the wrong reason
    and the test would pass on a false green.
    """
    import json as json_mod

    import classifier as classifier_mod
    import classify_job
    import config as cfg
    import numpy as np
    import pipeline as pipeline_mod
    from db import Database

    try:
        import misses as misses_mod
    except ImportError:
        # The setup-failure test poisons sys.modules["misses"] so the miss
        # stage's own import raises; the spy is unused on that path.
        misses_mod = None

    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    with open(cfg.CONFIG_PATH, "w") as f:
        json_mod.dump({"pipeline": pipeline_cfg}, f)

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)
    photo_id = db.add_photo(folder_id, "test.jpg", ".jpg", 12345, 1_000_000.0)
    _drop_jpeg(folder_path, "test.jpg")
    col_id = db.add_collection(
        "Test",
        json_mod.dumps([{"field": "photo_ids", "value": [photo_id]}]),
    )

    _setup_fake_downloaded_model(tmp_path, monkeypatch)

    def fake_detect_batch(batch, folders, runner, job, reclassify, db_,
                          det_conf_threshold=None, already_detected_ids=None,
                          cached_detections=None):
        return {}, 0, {p["id"] for p in batch}

    monkeypatch.setattr(classify_job, "_detect_batch", fake_detect_batch)

    class FakeClassifier:
        def __init__(self, *args, **kwargs):
            pass

        def encode_image(self, *args, **kwargs):
            return np.zeros(512, dtype=np.float32)

        def classify_with_embedding(self, img, threshold=0):
            return (
                [{"species": "Robin", "score": 0.9}],
                np.zeros(512, dtype=np.float32),
            )

        def classify_batch_with_embedding(self, images, threshold=0):
            zero = np.zeros(512, dtype=np.float32)
            return [([{"species": "Robin", "score": 0.9}], zero)
                    for _ in images]

    monkeypatch.setattr(classifier_mod, "Classifier", FakeClassifier)

    monkeypatch.setattr(
        pipeline_mod, "run_full_pipeline",
        lambda photos, config=None, emit_trace=False: {
            "summary": {"groups": 1}, "photos": photos,
        },
    )
    monkeypatch.setattr(
        pipeline_mod, "run_species_review_pipeline",
        lambda photos, config=None, emit_trace=False: {
            "review_mode": "species",
            "summary": {"review_count": len(photos)},
            "photos": photos,
            "encounters": [],
        },
    )
    monkeypatch.setattr(
        pipeline_mod, "save_results", lambda *a, **kw: None,
    )
    monkeypatch.setattr(
        pipeline_mod, "load_photo_features",
        lambda thread_db, collection_id=None, config=None: [{"id": photo_id}],
    )

    spy_calls = []

    def spy_compute(thread_db, p_cfg, collection_id=None,
                    exclude_photo_ids=None, now=None):
        spy_calls.append({"pipeline_cfg": dict(p_cfg)})
        return 3

    if misses_mod is not None:
        monkeypatch.setattr(
            misses_mod, "compute_misses_for_workspace", spy_compute,
        )

    params = PipelineParams(
        collection_id=col_id,
        skip_extract_masks=True,
        **params_extra,
    )
    runner = FakeRunner()
    job = _make_job()
    # Suppression is opt-in — the setup-failure test needs to inspect
    # job["_fatal_error"] after the recorded RuntimeError, but the
    # miss_enabled override tests must let unexpected aborts propagate so
    # the miss_stage.skipped assertion can't pass because an earlier stage
    # failed for an unrelated reason.
    result = None
    if expect_runtime_error:
        with contextlib.suppress(RuntimeError):
            result = run_pipeline_job(job, runner, db_path, ws_id, params)
    else:
        result = run_pipeline_job(job, runner, db_path, ws_id, params)
    return runner, result, spy_calls, job


def _last_stages(runner):
    progress_events = [
        data for (_, evt, data) in runner.events
        if evt == "progress" and "stages" in data
    ]
    assert progress_events, "pipeline emitted no progress events"
    return progress_events[-1]["stages"]


def test_miss_enabled_false_param_short_circuits_before_compute(
    tmp_path, monkeypatch,
):
    """Skip direction: workspace says True, params say False -> the stage
    records skipped and compute_misses_for_workspace is never invoked. A
    local-variable-only implementation (gating just the cache marker)
    would still call compute and fail this test."""
    runner, result, spy_calls, job = _run_pipeline_for_miss_tests(
        tmp_path, monkeypatch,
        pipeline_cfg={"miss_enabled": True},
        params_extra={"miss_enabled": False},
    )
    assert spy_calls == [], "compute_misses_for_workspace ran despite override"
    assert _last_stages(runner)["misses"]["status"] == "skipped"
    miss_steps = [
        kw for (_, step, kw) in runner.step_updates if step == "misses"
    ]
    assert {"status": "completed", "summary": "Skipped"} in miss_steps


def test_identify_run_prepares_species_review_cache(tmp_path, monkeypatch):
    """Identify preset (skip_regroup + classify + review_mode=species)
    writes species review results."""
    runner, result, spy_calls, job = _run_pipeline_for_miss_tests(
        tmp_path, monkeypatch,
        pipeline_cfg={"miss_enabled": True},
        params_extra={
            "skip_regroup": True,
            "miss_enabled": False,
            "review_mode": "species",
        },
    )
    assert result["stages"]["review"] == {"review_count": 1}
    assert spy_calls == []
    assert _last_stages(runner)["regroup"]["status"] == "completed"
    regroup_steps = [
        kw for (_, step, kw) in runner.step_updates if step == "regroup"
    ]
    assert {
        "status": "completed",
        "summary": "Review results ready",
    } in regroup_steps


def test_classify_only_skip_regroup_does_not_write_species_cache(
    tmp_path, monkeypatch,
):
    """Advanced/Custom classify-only path: skip_regroup=True with classify
    on but no ``review_mode`` opt-in must SKIP regroup entirely rather than
    fall through to the identify preset's species-review save. Otherwise a
    user who just disabled Group & Score to refresh classifications would
    silently see the workspace cache overwritten with all-REVIEW output —
    the culling-pipeline downgrade the reviewer flagged."""
    runner, result, spy_calls, job = _run_pipeline_for_miss_tests(
        tmp_path, monkeypatch,
        pipeline_cfg={"miss_enabled": True},
        params_extra={
            "skip_regroup": True,
            "miss_enabled": False,
            # review_mode intentionally left at its default (None) — this
            # is the shape /api/jobs/pipeline gets from a Custom-strategy
            # body that ticks off Group without setting a strategy name.
        },
    )
    # No "review" summary got written — species-review pipeline never ran.
    assert "review" not in result["stages"]
    assert _last_stages(runner)["regroup"]["status"] == "skipped"
    regroup_steps = [
        kw for (_, step, kw) in runner.step_updates if step == "regroup"
    ]
    assert {"status": "completed", "summary": "Skipped"} in regroup_steps


def test_miss_enabled_true_param_overrides_disabled_workspace(
    tmp_path, monkeypatch,
):
    """Enable direction: workspace says False, params say True -> compute IS
    invoked and sees the injected effective value. Without injection the
    compute call reads workspace-False and silently evaluates nothing."""
    runner, result, spy_calls, job = _run_pipeline_for_miss_tests(
        tmp_path, monkeypatch,
        pipeline_cfg={"miss_enabled": False},
        params_extra={"miss_enabled": True},
    )
    assert len(spy_calls) == 1, "compute_misses_for_workspace did not run"
    assert spy_calls[0]["pipeline_cfg"].get("miss_enabled") is True
    assert _last_stages(runner)["misses"]["status"] == "completed"
    miss_steps = [
        kw for (_, step, kw) in runner.step_updates if step == "misses"
    ]
    assert {"status": "completed", "summary": "3 photos evaluated"} in miss_steps


def test_miss_stage_setup_failure_marks_stage_failed(tmp_path, monkeypatch):
    """Setup-failure direction: if the miss stage's setup (imports, DB,
    config load) raises, the stage must record failed — not linger as
    pending while the job completes "successfully". Pins the contract the
    hoisted-setup refactor could otherwise regress. Only miss_stage imports
    the misses module, so poisoning it in sys.modules scopes the failure to
    exactly this stage."""
    import sys as sys_mod

    def run(tmp, mk):
        mk.setitem(sys_mod.modules, "misses", None)
        return _run_pipeline_for_miss_tests(
            tmp, mk,
            pipeline_cfg={"miss_enabled": True},
            params_extra={},
            expect_runtime_error=True,
        )

    runner, result, spy_calls, job = run(tmp_path, monkeypatch)
    assert spy_calls == []
    assert _last_stages(runner)["misses"]["status"] == "failed"
    miss_steps = [
        kw for (_, step, kw) in runner.step_updates if step == "misses"
    ]
    assert any(kw.get("status") == "failed" and kw.get("error")
               for kw in miss_steps), miss_steps
    assert str(job.get("_fatal_error", "")).startswith("[misses] Fatal:"), (
        job.get("_fatal_error"), job.get("errors")
    )


# ---------------------------------------------------------------------------
# per-photo resume contract (import/process split PR 1)
# ---------------------------------------------------------------------------


def test_collection_rerun_redoes_only_missing_work(tmp_path, monkeypatch):
    """The process job's core promise: re-running the same strategy over the
    same photos re-does only what's missing. Second run must report zero
    generated thumbnails/previews and a fully cache-hit classify. If this
    fails, the failure is the finding — fix the specific stage's skip
    check rather than loosening the assertion."""
    import json as json_mod

    import classifier as classifier_mod
    import classify_job
    import config as cfg
    import numpy as np
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)
    photo_ids = []
    for i, name in enumerate(("a.jpg", "b.jpg")):
        pid = db.add_photo(folder_id, name, ".jpg", 1000 + i, 1_000_000.0 + i)
        _drop_jpeg(folder_path, name)
        # Pre-seed one detection per photo so the classify loop has a
        # primary detection to classify (and, on run 2, to find cached
        # predictions for). Mirrors the reclassify tests' setup.
        db.save_detections(
            pid,
            [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
              "confidence": 0.9, "category": "animal"}],
            detector_model="MegaDetector",
        )
        photo_ids.append(pid)
    col_id = db.add_collection(
        "Resume test",
        json_mod.dumps([{"field": "photo_ids", "value": photo_ids}]),
    )

    _setup_fake_downloaded_model(tmp_path, monkeypatch)

    def fake_detect_batch(batch, folders, runner, job, reclassify, db_,
                          det_conf_threshold=None, already_detected_ids=None,
                          cached_detections=None):
        det_map = {}
        for p in batch:
            # The classify loop expects plain dicts (the real _detect_batch
            # returns them); sqlite3.Row has no .get.
            dets = [dict(d) for d in db_.get_detections(p["id"])]
            if dets:
                det_map[p["id"]] = dets
        return det_map, 0, {p["id"] for p in batch}

    monkeypatch.setattr(classify_job, "_detect_batch", fake_detect_batch)

    inference_calls = []

    class FakeClassifier:
        def __init__(self, *args, **kwargs):
            pass

        def encode_image(self, *args, **kwargs):
            inference_calls.append("encode_image")
            return np.zeros(512, dtype=np.float32)

        def classify_with_embedding(self, img, threshold=0):
            inference_calls.append("classify_with_embedding")
            return (
                [{"species": "Robin", "score": 0.9}],
                np.zeros(512, dtype=np.float32),
            )

        def classify_batch_with_embedding(self, images, threshold=0):
            inference_calls.append("classify_batch_with_embedding")
            zero = np.zeros(512, dtype=np.float32)
            return [([{"species": "Robin", "score": 0.9}], zero)
                    for _ in images]

    monkeypatch.setattr(classifier_mod, "Classifier", FakeClassifier)

    def run_once():
        params = PipelineParams(
            collection_id=col_id,
            skip_extract_masks=True,
            skip_regroup=True,
        )
        runner = FakeRunner()
        job = _make_job()
        result = run_pipeline_job(job, runner, db_path, ws_id, params)
        return result, runner

    first, _first_runner = run_once()
    n = len(photo_ids)
    assert first["stages"]["thumbnails"]["generated"] == n, first["stages"]
    assert first["stages"]["previews"]["generated"] == n, first["stages"]
    assert first["stages"]["classify"]["predictions_stored"] == n, (
        first["stages"]
    )
    assert inference_calls, "first run never invoked the classifier"

    inference_calls.clear()
    second, second_runner = run_once()
    assert second["stages"]["thumbnails"] == {
        "generated": 0, "skipped": n, "failed": 0,
    }, second["stages"]
    assert second["stages"]["previews"]["generated"] == 0, second["stages"]
    assert second["stages"]["previews"]["skipped"] == n, second["stages"]
    assert second["stages"]["previews"]["failed"] == 0, second["stages"]
    assert second["stages"]["classify"]["already_classified"] == n, (
        second["stages"]
    )
    # NOTE: predictions_stored is NOT asserted to be 0 on the rerun — the
    # cached branch deliberately re-surfaces cached predictions into
    # raw_results so downstream grouping/storage sees them, and storage
    # re-upserts the same rows (idempotent, cheap). The expensive contract
    # is that no model inference happens at all on a fully-cached rerun:
    assert inference_calls == [], (
        f"rerun invoked the classifier: {inference_calls}"
    )
    eta_updates = [
        kw["progress"]
        for _, step_id, kw in second_runner.step_updates
        if step_id.startswith("classify:")
        and isinstance(kw.get("progress"), dict)
        and kw["progress"].get("eta_kind") == "classification"
    ]
    assert eta_updates, "classify step never published cache-aware ETA fields"
    assert eta_updates[-1]["cache_hits"] == n
    assert eta_updates[-1]["classified"] == 0
    assert eta_updates[-1]["eta_state"] == "finishing"


# ---------------------------------------------------------------------------
# Source-offline detection (dropped network volume mid-run)
#
# A disconnected SMB/NFS share makes EVERY subsequent read fail with EIO.
# Classify used to count each one as a per-photo failure, so a share that
# dropped 200 photos into a 984-photo run reported "779 failed" — which reads
# as "779 of your photos are broken" when the truth is "the volume went away
# and we never looked at them". These tests pin the distinction.
# ---------------------------------------------------------------------------

def test_source_offline_reason_none_for_healthy_local_folder(tmp_path):
    """A readable folder is never 'offline' — a load failure there is the
    individual file's fault (corrupt RAW, bad permissions) and must stay a
    per-photo failure."""
    from pipeline_job import _source_offline_reason

    folder = str(tmp_path / "photos")
    os.makedirs(folder, exist_ok=True)
    missing_file = os.path.join(folder, "does_not_exist.NEF")

    assert _source_offline_reason(folder, missing_file) is None, (
        "A missing/corrupt file inside a healthy folder must NOT be treated "
        "as an offline source, or one bad RAW would pause the whole run."
    )


def test_source_offline_reason_flags_unmounted_volume():
    """The incident case: /Volumes/<share> is gone entirely.

    Scoped ``"mount"`` because every remaining read of the collection will
    fail the same way — classify must pause the whole run, not just this
    folder.
    """
    from pipeline_job import _source_offline_reason

    folder = "/Volumes/DefinitelyNotMounted12345/Raw Files"
    image = os.path.join(folder, "DSC_7280.NEF")

    result = _source_offline_reason(folder, image)
    assert result is not None, (
        "An unmounted /Volumes/... path must be reported as an offline source."
    )
    scope, reason = result
    assert scope == "mount", (
        f"An unmounted volume must scope offline to the mount so the whole "
        f"run pauses; got scope {scope!r}."
    )
    assert "/Volumes/DefinitelyNotMounted12345" in reason, (
        f"The reason must name the mount root the user has to reconnect; "
        f"got {reason!r}"
    )


def test_source_offline_reason_flags_vanished_folder(tmp_path):
    """A single unreadable folder is scoped to that folder alone.

    Codex #1388 P1: with ``reclassify=True`` in a multi-folder collection,
    stopping the whole run on one deleted local folder would strand later
    healthy folders (finalization would clear their existing predictions
    with no replacement). Scope ``"folder"`` lets the run skip the missing
    folder's photos as unreachable and keep processing the rest.
    """
    from pipeline_job import _source_offline_reason

    folder = str(tmp_path / "was_here")
    image = os.path.join(folder, "DSC_0001.NEF")

    result = _source_offline_reason(folder, image)
    assert result is not None, (
        "A folder that no longer resolves as a directory means the source "
        "for THIS folder went away, not that one photo is corrupt."
    )
    scope, reason = result
    assert scope == "folder", (
        f"A single missing folder must not stop the whole run; scope must be "
        f"'folder' so later healthy folders keep processing. Got {scope!r}."
    )
    assert folder in reason, f"Reason must name the folder; got {reason!r}"


def test_source_offline_reason_keeps_empty_mountshape_root_folder_scoped(
    monkeypatch,
):
    """An empty mount-shaped root with no active mount is folder-scoped.

    Codex #1388 P1 (r3664348752) supersedes the earlier r3663493889
    behavior: a directory that exists, is readable, is empty, and is
    not currently a mount point is genuinely ambiguous — it could be
    an unmounted SMB stub OR an ordinary local ``/mnt/photos`` whose
    only child (the deleted collection folder) just went away. The
    two shapes have identical current-state signals, so mount-scoping
    the outage would pause the whole run for an ordinary local
    catalog whenever the user archives their last subfolder. Prefer
    folder-scoped when we can't prove the root was actually a mount;
    the dead-mount-that-still-shows-up-in-mount case (the far more
    common failure mode — network drop leaves ``ismount == True``
    with EIO on every read) is still mount-scoped via the
    ``listdir`` OSError branch (see the ``…flags_stale_mount…``
    tests below).
    """
    import pipeline_job

    folder = "/mnt/photos/2026-07-27"
    image = os.path.join(folder, "DSC_0001.NEF")

    real_lexists = pipeline_job.os.path.lexists
    real_isdir = pipeline_job.os.path.isdir
    real_ismount = pipeline_job.os.path.ismount
    real_listdir = pipeline_job.os.listdir

    def fake_lexists(path):
        # The mount-point directory (or its plain-local twin) is still
        # there.
        if path == "/mnt/photos":
            return True
        return real_lexists(path)

    def fake_isdir(path):
        # The subtree is unreachable — the collection folder is gone.
        if path == folder:
            return False
        return real_isdir(path)

    def fake_ismount(path):
        # No filesystem currently mounted at /mnt/photos. Could be a
        # cleanly-unmounted share OR a plain local dir. We can't tell.
        if path == "/mnt/photos":
            return False
        return real_ismount(path)

    def fake_listdir(path):
        # Empty root: either an unmounted stub or a plain local dir
        # whose only child was the deleted folder. Same signal.
        if path == "/mnt/photos":
            return []
        return real_listdir(path)

    monkeypatch.setattr(pipeline_job.os.path, "lexists", fake_lexists)
    monkeypatch.setattr(pipeline_job.os.path, "isdir", fake_isdir)
    monkeypatch.setattr(pipeline_job.os.path, "ismount", fake_ismount)
    monkeypatch.setattr(pipeline_job.os, "listdir", fake_listdir)

    result = pipeline_job._source_offline_reason(folder, image)
    assert result is not None, (
        "The deleted subfolder is still unreachable, so the helper must "
        "report an outage — just scoped to the folder, not the whole mount."
    )
    scope, reason = result
    assert scope == "folder", (
        f"An empty mount-shaped root without positive mount-identity "
        f"evidence is ambiguous; scoping it as a mount outage would pause "
        f"an ordinary local catalog whenever its last subfolder is "
        f"archived. Got scope {scope!r} reason {reason!r}."
    )


def test_source_offline_reason_keeps_populated_local_mnt_dir_folder_scoped(
    tmp_path,
):
    """A plain local dir under ``/mnt/`` isn't a mount just because ismount says so.

    Codex #1388 P1 (r3663642357): the mount-shape check equated
    ``os.path.ismount == False`` with "mount is offline", but a plain
    local catalog at ``/mnt/photos`` with several sibling folders is
    also ``ismount``-negative. Deleting one subfolder (say the trip
    the user just archived) would then re-classify the whole tree as a
    mount-wide outage — the run would pause, exhaust its retry budget,
    and abandon every healthy sibling. Pin that a mount-shaped root
    with visible sibling entries is treated as an ordinary local dir
    so the deleted subfolder stays folder-scoped.
    """
    import pipeline_job

    mount_root = tmp_path / "mnt_style_local"
    mount_root.mkdir()
    # Siblings that survive whatever happened to the missing folder are
    # the positive proof this is a plain local dir, not an unmounted
    # share (which would take its whole tree with it).
    (mount_root / "beach").mkdir()
    (mount_root / "mountain").mkdir()

    missing_folder = str(mount_root / "trip")
    image_under_missing = os.path.join(missing_folder, "DSC_0001.NEF")

    # Force the mount-root candidate lookup at the tmp path — the real
    # helper only recognises /Volumes, /mnt, /media roots, and we need
    # to exercise the "populated → not offline" branch without touching
    # the real filesystem's /mnt.
    real_candidates = pipeline_job._archive_mount_root_candidates

    def fake_candidates(path):
        if path == image_under_missing:
            return [str(mount_root)]
        return real_candidates(path)

    real_ismount = pipeline_job.os.path.ismount

    def fake_ismount(path):
        # The plain local dir naturally has ``ismount == False`` — mirror
        # that explicitly so the test doesn't depend on the FS layout of
        # the CI machine.
        if path == str(mount_root):
            return False
        return real_ismount(path)

    import unittest.mock as mock
    with (
        mock.patch.object(
            pipeline_job, "_archive_mount_root_candidates", fake_candidates,
        ),
        mock.patch.object(pipeline_job.os.path, "ismount", fake_ismount),
    ):
        result = pipeline_job._source_offline_reason(
            missing_folder, image_under_missing,
        )

    assert result is not None, (
        "The deleted subfolder itself is unreachable, so the helper must "
        "still report an outage — just scoped to the folder, not the whole "
        "mount root."
    )
    scope, reason = result
    assert scope == "folder", (
        f"A mount-shaped root with sibling entries is proof it's a plain "
        f"local dir; scoping the missing subfolder as a mount outage would "
        f"pause the whole run and abandon healthy siblings. Got scope "
        f"{scope!r} reason {reason!r}."
    )


def test_source_offline_reason_flags_stale_mount_still_reporting_ismount_true(
    monkeypatch,
):
    """A dead SMB/NFS mount can keep ``ismount == True``; probe reads too.

    Codex #1388 P1 (r3664211201): ``os.path.ismount`` only inspects
    mount-point metadata, so a share whose server disconnected can keep
    ``ismount`` returning True even though every read against the root
    raises EIO. Pre-fix, ``_mount_root_offline`` short-circuited on
    ``ismount == True`` and returned "not offline", which routed the
    dropped share through the folder-scoped branch — classify would then
    keep reissuing reads across the dead share instead of pausing for
    reconnection. Pin that the helper probes the root with ``listdir``
    even when ``ismount`` is True, so a stale-but-registered mount is
    correctly scoped mount-offline.
    """
    import pipeline_job

    folder = "/mnt/photos/2026-07-27"
    image = os.path.join(folder, "DSC_0001.NEF")

    real_lexists = pipeline_job.os.path.lexists
    real_isdir = pipeline_job.os.path.isdir
    real_ismount = pipeline_job.os.path.ismount
    real_listdir = pipeline_job.os.listdir

    def fake_lexists(path):
        # The mount-point directory is still present — the mount table
        # entry hasn't been cleaned up.
        if path == "/mnt/photos":
            return True
        return real_lexists(path)

    def fake_isdir(path):
        # The subtree read fails — this is what triggered the check in
        # the first place.
        if path == folder:
            return False
        return real_isdir(path)

    def fake_ismount(path):
        # ``mount`` still lists /mnt/photos, so ``ismount`` says True —
        # exactly the case Codex flagged.
        if path == "/mnt/photos":
            return True
        return real_ismount(path)

    def fake_listdir(path):
        # But the underlying filesystem is dead: reading the root
        # returns Input/output error. Without probing this, the helper
        # would trust ``ismount`` alone and mislabel the outage.
        if path == "/mnt/photos":
            raise OSError("Input/output error")
        return real_listdir(path)

    monkeypatch.setattr(pipeline_job.os.path, "lexists", fake_lexists)
    monkeypatch.setattr(pipeline_job.os.path, "isdir", fake_isdir)
    monkeypatch.setattr(pipeline_job.os.path, "ismount", fake_ismount)
    monkeypatch.setattr(pipeline_job.os, "listdir", fake_listdir)

    result = pipeline_job._source_offline_reason(folder, image)
    assert result is not None, (
        "A stale mount whose ``ismount`` still returns True but whose "
        "root read raises EIO must register as offline — otherwise "
        "classify keeps hammering the dead share instead of pausing."
    )
    scope, reason = result
    assert scope == "mount", (
        f"A dead-but-still-registered mount must scope offline to the "
        f"mount so the whole run pauses for reconnection; got scope "
        f"{scope!r}."
    )
    assert "/mnt/photos" in reason, (
        f"Reason must name the mount the user needs to reconnect; got "
        f"{reason!r}."
    )


def test_source_offline_reason_flags_stale_mount_that_raises_from_stat(
    monkeypatch,
):
    """A stale mount whose stat probes raise EIO is still offline.

    ``os.path.ismount`` uses stat calls on the path and its parent; a
    dead NFS/SMB mount can raise ``OSError`` (EIO) from those instead of
    returning a clean answer. Since we only ask about the mount state
    once a read has already failed, an errored probe must count as
    offline — otherwise the same dead share would be treated as healthy
    and folder-scoped for every subsequent read.
    """
    import pipeline_job

    folder = "/mnt/photos/2026-07-27"
    image = os.path.join(folder, "DSC_0001.NEF")

    real_lexists = pipeline_job.os.path.lexists
    real_isdir = pipeline_job.os.path.isdir
    real_listdir = pipeline_job.os.listdir

    def fake_lexists(path):
        if path == "/mnt/photos":
            return True
        return real_lexists(path)

    def fake_isdir(path):
        if path == folder:
            return False
        return real_isdir(path)

    def fake_listdir(path):
        # ``_mount_root_offline`` probes readability of the mount
        # root itself via ``os.listdir``; a stale SMB/NFS mount whose
        # server has disconnected raises ``OSError`` (EIO) from that
        # call even though ``lexists``/``ismount`` still report the
        # mount as present. This is the exact behaviour that lets us
        # scope the outage to the whole mount (pause + resume flow)
        # rather than silently accepting the dropped share as healthy
        # and folder-scoping every subsequent read. Without patching
        # ``os.listdir``, the test would depend on the host machine
        # not having a real ``/mnt/photos`` — pass on a bare CI image,
        # fail on WSL/containers/Linux desktops that do — and the
        # intended branch would never be exercised (CodeRabbit
        # r3664548822).
        if path == "/mnt/photos":
            raise OSError("Input/output error")
        return real_listdir(path)

    monkeypatch.setattr(pipeline_job.os.path, "lexists", fake_lexists)
    monkeypatch.setattr(pipeline_job.os.path, "isdir", fake_isdir)
    monkeypatch.setattr(pipeline_job.os, "listdir", fake_listdir)

    result = pipeline_job._source_offline_reason(folder, image)
    assert result is not None and result[0] == "mount", (
        f"A stale mount whose listdir probe raises must be treated as "
        f"offline, not silently accepted as healthy. Got {result!r}."
    )


def test_source_offline_reason_flags_dead_mount_with_cached_folder_stat(
    monkeypatch,
):
    """A dead mount whose folder ``isdir`` still returns True is mount-offline.

    Codex #1388 P1 (r3665254569): a disconnected SMB/NFS mount can keep
    the containing folder's directory metadata cached, so
    ``os.path.isdir(folder)`` returns True even though every read of the
    folder's files raises EIO. Pre-fix ``_source_offline_reason``
    short-circuited on a truthy ``isdir(folder)`` and returned None — the
    caller then counted the failed read as a per-photo failure and
    classify kept hammering the dead share for every remaining photo
    instead of pausing for reconnection. Pin that the mount-root probe
    still runs when the folder stat looks fine, so a dead mount is
    scoped mount-wide.
    """
    import pipeline_job

    folder = "/mnt/photos/2026-07-27"
    image = os.path.join(folder, "DSC_0001.NEF")

    real_lexists = pipeline_job.os.path.lexists
    real_isdir = pipeline_job.os.path.isdir
    real_listdir = pipeline_job.os.listdir

    def fake_lexists(path):
        if path == "/mnt/photos":
            return True
        return real_lexists(path)

    def fake_isdir(path):
        # The critical bit: the folder's directory-stat cache is stale
        # and still reports True, mirroring the real behaviour of a
        # disconnected SMB/NFS mount where the containing folder's
        # metadata survives the drop while file reads underneath it
        # raise EIO.
        if path == folder:
            return True
        # The mount root itself has to look present for
        # ``_mount_root_offline`` to reach the ``listdir`` probe.
        if path == "/mnt/photos":
            return True
        return real_isdir(path)

    def fake_listdir(path):
        # The mount is truly dead: reading the root raises EIO. This is
        # what the fix relies on to detect the outage even when the
        # folder stat lies about being present.
        if path == "/mnt/photos":
            raise OSError("Input/output error")
        return real_listdir(path)

    monkeypatch.setattr(pipeline_job.os.path, "lexists", fake_lexists)
    monkeypatch.setattr(pipeline_job.os.path, "isdir", fake_isdir)
    monkeypatch.setattr(pipeline_job.os, "listdir", fake_listdir)

    result = pipeline_job._source_offline_reason(folder, image)
    assert result is not None, (
        "A cached folder stat that lies about a dead mount must not let "
        "``_source_offline_reason`` return None — the caller would then "
        "count every remaining read as a per-photo failure instead of "
        "pausing for reconnection."
    )
    scope, reason = result
    assert scope == "mount", (
        f"A dead mount discovered via the mount-root probe must scope "
        f"the outage mount-wide so the whole run pauses; got scope "
        f"{scope!r}."
    )
    assert "/mnt/photos" in reason, (
        f"Reason must name the mount the user needs to reconnect; got "
        f"{reason!r}."
    )


def test_archive_mount_root_candidates_recognises_windows_paths():
    """Windows mapped drives and UNC shares are documented in
    ``docs/WINDOWS_SUPPORT.md`` as supported storage layouts.

    Codex #1388 P2 (r3663816324): before this fix, ``_candidate`` only
    matched POSIX ``/Volumes``, ``/mnt``, ``/media`` prefixes. A
    disconnected SMB share on Windows (mapped drive ``Z:\\photos`` or
    UNC ``\\\\server\\share\\photos``) would therefore never produce a
    mount-root candidate, ``_source_offline_reason`` would fall through
    to the folder-scoped branch, and classify would keep reissuing
    reads across the dead share instead of pausing the run for
    reconnection.
    """
    from pipeline_job import _archive_mount_root_candidates

    drive_cands = _archive_mount_root_candidates(r"Z:\photos\raw\DSC_0001.NEF")
    assert "Z:/" in drive_cands, (
        f"A Windows mapped-drive path must yield ``Z:/`` (trailing separator "
        f"so ``os.path.ismount`` accepts it) as a mount-root candidate. "
        f"Got {drive_cands!r}."
    )

    unc_cands = _archive_mount_root_candidates(
        r"\\photos-nas\raw\2026\DSC_0001.NEF",
    )
    assert "//photos-nas/raw" in unc_cands, (
        f"A UNC share path must yield ``//<server>/<share>`` as a mount-root "
        f"candidate — that's the reconnect boundary the user names when the "
        f"share drops. Got {unc_cands!r}."
    )

    # A drive letter with only ``Z:`` and no path underneath still names
    # a mount root — this covers the corner case where an image path was
    # itself just ``Z:\image.jpg``.
    bare_drive = _archive_mount_root_candidates(r"C:\photo.jpg")
    assert "C:/" in bare_drive, (
        f"A bare drive-letter path must still yield its drive root; got "
        f"{bare_drive!r}."
    )


def test_source_offline_reason_flags_disconnected_windows_share(monkeypatch):
    """A disconnected Windows SMB share must scope its outage to the mount.

    Codex #1388 P2 (r3663816324): without recognising ``Z:/`` and
    ``//server/share`` as mount roots, ``_source_offline_reason`` would
    fall through to the folder-scoped branch when the share drops, and
    classify would skip folder-by-folder while every read into the dead
    share still failed instantly. Pin that the mount-scoped path fires
    for both Windows storage shapes so classify pauses for reconnection
    instead.
    """
    import pipeline_job

    folder = r"Z:\photos\2026-07-27"
    image = folder + r"\DSC_0001.NEF"

    real_isdir = pipeline_job.os.path.isdir
    real_lexists = pipeline_job.os.path.lexists

    def fake_isdir(path):
        if path == folder:
            return False
        return real_isdir(path)

    def fake_lexists(path):
        # Windows disconnects a mapped drive by removing the drive
        # letter entirely — the standard ``lexists`` False path in
        # ``_mount_root_offline`` covers that.
        if path == "Z:/":
            return False
        return real_lexists(path)

    monkeypatch.setattr(pipeline_job.os.path, "isdir", fake_isdir)
    monkeypatch.setattr(pipeline_job.os.path, "lexists", fake_lexists)

    result = pipeline_job._source_offline_reason(folder, image)
    assert result is not None, (
        "A Windows mapped-drive path whose folder no longer resolves and "
        "whose drive letter is gone must register as an offline source."
    )
    scope, reason = result
    assert scope == "mount", (
        f"A disconnected Windows share must scope offline to the mount so "
        f"the whole run pauses for reconnection; got scope {scope!r}."
    )
    assert "Z:" in reason, (
        f"The reason must name the drive the user needs to reconnect; got "
        f"{reason!r}."
    )


def test_mount_root_offline_false_for_empty_ordinary_local_dir(tmp_path):
    """An empty readable directory that isn't a mount is NOT offline.

    Codex #1388 P1 (r3664348752): an ordinary local ``/mnt/photos``
    whose only child (the deleted collection folder) just went away is
    empty + ``ismount=False`` + readable — the same signals as an
    unmounted stub. Pre-fix the helper returned True for either shape,
    upgrading the missing subfolder to a mount-wide outage; the whole
    run would pause and eventually abandon any healthy siblings. Pin
    that ``_mount_root_offline`` treats a readable directory as online
    regardless of whether it's populated or empty and whether it's
    ismount-positive.
    """
    from pipeline_job import _mount_root_offline

    empty_root = tmp_path / "empty_mnt_style"
    empty_root.mkdir()  # readable, empty, not a mount

    assert _mount_root_offline(str(empty_root)) is False, (
        "An empty readable directory must not be reported as offline just "
        "because ismount is False — that would pause runs on an ordinary "
        "local catalog whose last subfolder was archived."
    )


def test_still_offline_folder_ids_prunes_recovered_folders(tmp_path):
    """Folders that recovered must drop out of the still-offline set.

    Codex #1388 P2 (r3664348758): the aggregate
    ``source_offline_state["skipped_photo_ids"]`` accumulates across every
    classifier spec, and a folder that dropped for spec A can recover
    before mask extraction runs. Without a re-probe, the downstream
    filter would silently exclude photos in the now-readable folder and
    they would never get their masks. Pin that
    ``_still_offline_folder_ids`` returns only folders that are still
    missing at re-probe time.
    """
    import config as cfg
    from db import Database
    from pipeline_job import _still_offline_folder_ids

    cfg.CONFIG_PATH = str(tmp_path / "config.json")
    db = Database(str(tmp_path / "test.db"))

    healthy_folder = tmp_path / "healthy"
    healthy_folder.mkdir()
    gone_folder = tmp_path / "vanished"
    gone_folder.mkdir()

    healthy_folder_id = db.add_folder(str(healthy_folder))
    gone_folder_id = db.add_folder(str(gone_folder))

    healthy_id = db.add_photo(
        healthy_folder_id, "a.jpg", ".jpg", 1000, 100.0,
    )
    gone_id = db.add_photo(
        gone_folder_id, "b.jpg", ".jpg", 1000, 101.0,
    )

    # Now delete the "gone" folder — folder A recovered, folder B is
    # still missing. This is the multi-model recovery shape: both
    # photo IDs entered the aggregate skipped set for an earlier
    # classifier spec, but only one of the two folders is still
    # unreachable now.
    os.rmdir(gone_folder)

    still_offline = _still_offline_folder_ids(db, {healthy_id, gone_id})
    assert still_offline == {gone_folder_id}, (
        f"A recovered folder must drop out of the still-offline set so "
        f"downstream stages can process its photos; only the folder that "
        f"remains missing should stay. Got {still_offline!r} (expected "
        f"{{gone_folder_id={gone_folder_id}}})."
    )

    # And the empty-input path is a no-op — no DB query needed.
    assert _still_offline_folder_ids(db, set()) == set()
    assert _still_offline_folder_ids(db, []) == set()


def test_still_offline_folder_ids_expands_beyond_seed_photos(tmp_path):
    """A seed photo's folder scopes filtering for ALL photos in that folder.

    Codex #1388 P2 (r3664694179): in a non-reclassify run, photos with
    cached classifier results take the cache branch without calling
    ``_prepare_image``, so their IDs never enter
    ``source_offline_state["skipped_photo_ids"]``. If another photo in
    the same folder reveals the folder is unavailable, an ID-only filter
    still leaves those cached photos in the downstream mask/eye-keypoint
    worklists, and the stages reopen the missing source. Pin that the
    helper returns the whole *folder* — not just the seed photos — so
    the caller can filter by ``folder_id`` and catch cached siblings.
    """
    import config as cfg
    from db import Database
    from pipeline_job import _still_offline_folder_ids

    cfg.CONFIG_PATH = str(tmp_path / "config.json")
    db = Database(str(tmp_path / "test.db"))

    gone_folder = tmp_path / "vanished"
    gone_folder.mkdir()
    gone_folder_id = db.add_folder(str(gone_folder))

    # ``seed`` is the photo classify observed as unreachable via
    # ``_prepare_image``. ``cached_sibling`` is a photo in the SAME
    # folder that took the cache branch and never touched disk, so it
    # never entered the seed set.
    seed = db.add_photo(gone_folder_id, "seed.jpg", ".jpg", 1000, 100.0)
    cached_sibling = db.add_photo(
        gone_folder_id, "cached.jpg", ".jpg", 1000, 101.0,
    )

    os.rmdir(gone_folder)

    still_offline = _still_offline_folder_ids(db, {seed})
    assert still_offline == {gone_folder_id}, (
        f"Seeding with the reached photo alone must still surface the "
        f"whole folder as offline so cached siblings can be filtered by "
        f"folder_id. Got {still_offline!r}."
    )
    # And the returned set is what a downstream filter uses to exclude
    # the cached sibling — assert the filter shape callers apply.
    photos_for_stage = [
        {"id": seed, "folder_id": gone_folder_id},
        {"id": cached_sibling, "folder_id": gone_folder_id},
    ]
    filtered = [
        p for p in photos_for_stage
        if p["folder_id"] not in still_offline
    ]
    assert filtered == [], (
        f"Cached siblings in an offline folder must be dropped by the "
        f"folder-scoped filter — otherwise the mask/eye-keypoint stages "
        f"would reopen the dead source. Got {filtered!r}."
    )


@pytest.mark.skipif(
    sys.platform == "win32",
    reason=(
        "POSIX mount-shape only: on Windows, ``symlink_to('/Volumes/NAS/photos')`` "
        "treats the target as rooted on the current drive, so ``realpath`` yields "
        "``C:\\Volumes\\NAS\\photos`` and the drive-letter branch matches instead "
        "of ``/Volumes/NAS``. The Windows-shape equivalents are exercised by "
        "``test_archive_mount_root_candidates_recognises_windows_paths`` and "
        "``test_source_offline_reason_flags_disconnected_windows_share``."
    ),
)
def test_archive_mount_root_candidates_resolves_symlink_aliases(tmp_path):
    """A symlink alias to a mount root must retain mount scope.

    Codex #1388 P2 (r3664891998): a common catalog shape is a stable
    alias in the user's home (or a top-level ``/photos``) that points
    into a real mount like ``/Volumes/NAS/photos``. When the underlying
    share disconnects, neither the raw alias nor its ``abspath``
    normalization has a ``/Volumes``/``/mnt``/drive-letter/UNC prefix,
    so pre-fix ``_source_offline_reason`` would classify the dropped
    share as folder-scoped — skipping its photos silently instead of
    offering the reconnect-and-resume flow. Pin that ``realpath``
    resolution surfaces the underlying mount root so the outage scope
    reaches the whole share.
    """
    from pipeline_job import _archive_mount_root_candidates

    # A real symlink whose target is a mount-shaped path. The target
    # doesn't need to exist — ``realpath`` only resolves symlink chains
    # (readlink), it doesn't stat the resolved path — but we build a
    # dead-mount-looking layout on disk to keep the test hermetic.
    volumes = tmp_path / "Volumes"
    volumes.mkdir()
    share_target = volumes / "NAS" / "photos"
    share_target.mkdir(parents=True)

    alias = tmp_path / "photos_alias"
    # Point the alias at ``/Volumes/NAS/photos`` verbatim so realpath
    # yields a canonical mount-shaped absolute path regardless of the
    # tmp_path prefix.
    alias.symlink_to("/Volumes/NAS/photos")

    aliased_image = str(alias / "2026-07-28" / "DSC_0001.NEF")
    cands = _archive_mount_root_candidates(aliased_image)
    assert "/Volumes/NAS" in cands, (
        f"A symlink alias into ``/Volumes/NAS`` must yield the "
        f"underlying mount root via realpath so the outage scope "
        f"reaches the whole share. Got {cands!r}."
    )


def test_still_offline_folder_ids_of_probes_folder_ids_directly(tmp_path):
    """The direct-probe helper accepts folder IDs, not a photo seed.

    Codex #1388 P1 (r3664891993): a fully-cached-classify run
    (every detection and classifier result already stored, only masks
    missing after a SAM variant change) makes no image opens, so
    ``source_offline_state["skipped_photo_ids"]`` stays empty even
    when every remaining file lives on an unreachable share. The
    seed-based ``_still_offline_folder_ids`` therefore returns an
    empty set, and downstream stages would reopen the dead source
    photo-by-photo. Pin that the direct-probe twin handles the
    fully-cached case by taking folder IDs from the downstream
    worklist and probing them independently.
    """
    import config as cfg
    from db import Database
    from pipeline_job import _still_offline_folder_ids_of

    cfg.CONFIG_PATH = str(tmp_path / "config.json")
    db = Database(str(tmp_path / "test.db"))

    healthy_folder = tmp_path / "healthy"
    healthy_folder.mkdir()
    gone_folder = tmp_path / "vanished"
    gone_folder.mkdir()

    healthy_folder_id = db.add_folder(str(healthy_folder))
    gone_folder_id = db.add_folder(str(gone_folder))

    # Delete only the "gone" folder. In the fully-cached scenario the
    # classify seed set is empty, but the downstream worklist still
    # references the gone folder via ``folder_id``.
    os.rmdir(gone_folder)

    still_offline = _still_offline_folder_ids_of(
        db, {healthy_folder_id, gone_folder_id},
    )
    assert still_offline == {gone_folder_id}, (
        f"Direct-probe helper must return only the folder that is "
        f"actually unreachable at probe time; got {still_offline!r}."
    )

    # Empty input is a no-op — no DB query needed.
    assert _still_offline_folder_ids_of(db, set()) == set()
    assert _still_offline_folder_ids_of(db, []) == set()


def test_still_offline_folder_ids_chunks_large_id_sets(tmp_path):
    """Query in bounded chunks so SQLite's bind-variable limit can't 500 us.

    Codex #1388 P2 (r3664525158): an offline folder can hold more photos
    than SQLite's ``SQLITE_MAX_VARIABLE_NUMBER`` (999 on legacy builds
    this repo explicitly accommodates elsewhere), so a single
    ``id IN (?,?,…)`` here would raise ``OperationalError: too many SQL
    variables`` before mask/eye-keypoint stages could get past the
    downstream filter and continue with photos from healthy folders.
    Exercise well over 999 IDs and pin that the caller still gets the
    offline folder back — no exception.
    """
    import config as cfg
    from db import Database
    from pipeline_job import _still_offline_folder_ids

    cfg.CONFIG_PATH = str(tmp_path / "config.json")
    db = Database(str(tmp_path / "test.db"))

    gone_folder = tmp_path / "vanished"
    gone_folder.mkdir()
    gone_folder_id = db.add_folder(str(gone_folder))

    # 2500 > the 999-variable ceiling AND > the 900 chunk size, so the
    # query has to run in at least three chunks to succeed.
    photo_ids = [
        db.add_photo(gone_folder_id, f"p{i:04d}.jpg", ".jpg", 1000, float(i))
        for i in range(2500)
    ]

    os.rmdir(gone_folder)

    still_offline = _still_offline_folder_ids(db, photo_ids)
    assert still_offline == {gone_folder_id}, (
        f"Chunked lookup must still surface the unreachable folder; got "
        f"{still_offline!r}."
    )


def test_pipeline_classify_pauses_when_source_volume_disappears(
    tmp_path, monkeypatch,
):
    """Losing the source volume mid-classify must pause the job, not burn
    through the remaining photos marking them 'failed'.

    Reproduces the 2026-07-28 incident: the SMB share dropped partway through
    classify and every remaining read returned EIO instantly, so the run
    reported 779 failed photos and then marched on to the next model (865
    failed) instead of stopping so the user could reconnect.
    """
    import classifier as classifier_mod
    import classify_job
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)
    photo_ids = []
    for i in range(6):
        name = f"photo{i}.jpg"
        pid = db.add_photo(folder_id, name, ".jpg", 4000 + i, 4_000_000.0 + i)
        _drop_jpeg(folder_path, name)
        db.save_detections(
            pid,
            [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
              "confidence": 0.9, "category": "animal"}],
            detector_model="MegaDetector",
        )
        photo_ids.append(pid)

    col_id = db.add_collection(
        "Test",
        json.dumps([{"field": "photo_ids", "value": photo_ids}]),
    )

    _setup_fake_downloaded_model(tmp_path, monkeypatch)

    import labels_fingerprint as lfp
    monkeypatch.setattr(lfp, "compute_fingerprint", lambda *a, **k: "fp")

    def fake_detect_batch(batch, folders, runner, job, reclassify, db_,
                          det_conf_threshold=None, already_detected_ids=None,
                          cached_detections=None):
        det_map = {}
        for p in batch:
            det_map[p["id"]] = [{
                "id": d["id"],
                "box_x": d["box_x"], "box_y": d["box_y"],
                "box_w": d["box_w"], "box_h": d["box_h"],
                "confidence": d["detector_confidence"],
                "category": d["category"],
            } for d in db_.get_detections(p["id"])
                if d["detector_model"] != "full-image"]
        return det_map, len(batch), {p["id"] for p in batch}

    monkeypatch.setattr(classify_job, "_detect_batch", fake_detect_batch)

    # The share is gone: every load fails, and _prepare_image hands back the
    # archive path it could not read (exactly what it does on EIO today).
    gone_folder = "/Volumes/DefinitelyNotMounted12345/Raw Files"
    prepare_calls = []

    def offline_prepare_image(photo, folders, detection, vireo_dir=None):
        prepare_calls.append(photo["id"])
        return None, gone_folder, os.path.join(gone_folder, photo["filename"])

    monkeypatch.setattr(classify_job, "_prepare_image", offline_prepare_image)

    class FakeClassifier:
        def __init__(self, *args, **kwargs):
            pass

        def encode_image(self, *args, **kwargs):
            import numpy as np
            return np.zeros(512, dtype=np.float32)

    monkeypatch.setattr(classifier_mod, "Classifier", FakeClassifier)

    class PausingRunner(FakeRunner):
        def __init__(self):
            super().__init__()
            self.pause_calls = []
            self._paused = False

        def pause_job(self, job_id):
            self.pause_calls.append(job_id)
            self._paused = True
            return True

        def pause_requested(self, job_id):
            return self._paused

        def mark_paused(self, job_id):
            return True

        def wait_if_paused(self, job_id, *, publish_paused=False):
            # Simulate the user reconnecting and hitting Resume so the test
            # does not block forever.
            self._paused = False
            return False

    params = PipelineParams(
        collection_id=col_id,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = PausingRunner()
    job = _make_job()

    # The share never comes back, so the run ends failed rather than putting
    # a green check on a collection it never opened.
    with pytest.raises(RuntimeError):
        run_pipeline_job(job, runner, db_path, ws_id, params)

    assert runner.pause_calls, (
        "Losing the source volume must request a pause so the user can "
        "reconnect and resume. Instead the run continued and marked every "
        "remaining photo failed."
    )

    joined = " ".join(job["errors"])
    assert "/Volumes/DefinitelyNotMounted12345" in joined, (
        f"The error must name the volume the user has to reconnect; "
        f"got {job['errors']!r}"
    )

    # The whole point: photos we never got to look at are not "failures".
    classify_steps = [
        kwargs for (_jid, sid, kwargs) in runner.step_updates
        if sid.startswith("classify:") and "summary" in kwargs
    ]
    summaries = " ".join(k["summary"] for k in classify_steps)
    assert "failed" not in summaries, (
        f"Photos skipped because the volume vanished must not be reported "
        f"as classification failures; got summary {summaries!r}"
    )
    # After Codex #1388 P1 r3663278142, the pause+retry loop runs against
    # the SAME photo until the pause budget is spent (rather than one
    # pause per photo advancing through the collection). Photos never
    # attempted don't count as "unreachable"; the "stopped after N of Y"
    # line names the outage and accounts for the untouched remainder.
    # Accept either phrasing so the assertion stays about "did we account
    # for what got skipped" and not the specific accounting shape.
    assert (
        "unreachable" in summaries or "stopped after" in summaries
    ), (
        f"The summary must still account for the photos it skipped rather "
        f"than silently dropping them; got {summaries!r}"
    )
    assert "is not mounted" in summaries, (
        f"The summary must say why it stopped; got {summaries!r}"
    )

    # A user who keeps resuming without remounting must not loop forever.
    from pipeline_job import _MAX_SOURCE_OFFLINE_PAUSES
    assert len(runner.pause_calls) <= _MAX_SOURCE_OFFLINE_PAUSES, (
        f"Classify paused {len(runner.pause_calls)} times for the same dead "
        f"volume; it must give up after {_MAX_SOURCE_OFFLINE_PAUSES}."
    )

    # And we must stop pulling photos, not walk the whole collection.
    # Each pause is followed by ONE post-resume retry (Codex #1388 P2), so
    # the upper bound is 2*max + 1 (the initial call that trips the giving-up
    # branch, no retry). Anything more means classify walked past the pause
    # budget and kept EIO-ing the rest of the collection.
    max_prepare_calls = 2 * _MAX_SOURCE_OFFLINE_PAUSES + 1
    assert len(prepare_calls) <= max_prepare_calls, (
        f"Classify kept requesting images after the volume disappeared "
        f"({len(prepare_calls)} attempts across 6 photos, cap "
        f"{max_prepare_calls}); it should stop once it has given up on "
        f"the source."
    )


def test_pipeline_classify_resumes_after_source_volume_reconnects(
    tmp_path, monkeypatch,
):
    """Reconnecting the share and hitting Resume must finish the collection.

    The pause is only worth having if resuming actually classifies the rest —
    otherwise "reconnect and resume" is a false promise and the user has to
    re-run the whole pipeline.
    """
    import classifier as classifier_mod
    import classify_job
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)
    photo_ids = []
    for i in range(6):
        name = f"photo{i}.jpg"
        pid = db.add_photo(folder_id, name, ".jpg", 4000 + i, 4_000_000.0 + i)
        _drop_jpeg(folder_path, name)
        db.save_detections(
            pid,
            [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
              "confidence": 0.9, "category": "animal"}],
            detector_model="MegaDetector",
        )
        photo_ids.append(pid)

    col_id = db.add_collection(
        "Test",
        json.dumps([{"field": "photo_ids", "value": photo_ids}]),
    )

    _setup_fake_downloaded_model(tmp_path, monkeypatch)

    import labels_fingerprint as lfp
    monkeypatch.setattr(lfp, "compute_fingerprint", lambda *a, **k: "fp")

    def fake_detect_batch(batch, folders, runner, job, reclassify, db_,
                          det_conf_threshold=None, already_detected_ids=None,
                          cached_detections=None):
        det_map = {}
        for p in batch:
            det_map[p["id"]] = [{
                "id": d["id"],
                "box_x": d["box_x"], "box_y": d["box_y"],
                "box_w": d["box_w"], "box_h": d["box_h"],
                "confidence": d["detector_confidence"],
                "category": d["category"],
            } for d in db_.get_detections(p["id"])
                if d["detector_model"] != "full-image"]
        return det_map, len(batch), {p["id"] for p in batch}

    monkeypatch.setattr(classify_job, "_detect_batch", fake_detect_batch)

    from PIL import Image as _PILImage

    gone_folder = "/Volumes/DefinitelyNotMounted12345/Raw Files"
    state = {"mounted": True, "seen": 0}

    def flaky_prepare_image(photo, folders, detection, vireo_dir=None):
        state["seen"] += 1
        if state["seen"] == 2:
            # The share drops on the second photo.
            state["mounted"] = False
        if not state["mounted"]:
            return None, gone_folder, os.path.join(
                gone_folder, photo["filename"],
            )
        fp = folders.get(photo["folder_id"], "")
        return (
            _PILImage.new("RGB", (16, 16), "black"),
            fp,
            os.path.join(fp, photo["filename"]),
        )

    monkeypatch.setattr(classify_job, "_prepare_image", flaky_prepare_image)

    def fake_flush_batch(batch, clf, model_type, model_name, db_, raw_results,
                         top_k=1):
        for entry in batch:
            raw_results.append({
                "photo": entry["photo"],
                "detection_id": entry.get("detection_id"),
                "folder_path": entry["folder_path"],
                "image_path": entry["image_path"],
                "prediction": "Robin",
                "confidence": 0.9,
                "timestamp": None,
                "filename": entry["photo"]["filename"],
                "embedding": None,
                "taxonomy": None,
            })
        return 0

    monkeypatch.setattr(classify_job, "_flush_batch", fake_flush_batch)

    class FakeClassifier:
        def __init__(self, *args, **kwargs):
            pass

        def encode_image(self, *args, **kwargs):
            import numpy as np
            return np.zeros(512, dtype=np.float32)

    monkeypatch.setattr(classifier_mod, "Classifier", FakeClassifier)

    class ReconnectingRunner(FakeRunner):
        """Models the user remounting the share, then pressing Resume."""

        def __init__(self):
            super().__init__()
            self.pause_calls = []
            self._paused = False

        def pause_job(self, job_id):
            self.pause_calls.append(job_id)
            self._paused = True
            return True

        def pause_requested(self, job_id):
            return self._paused

        def mark_paused(self, job_id):
            return True

        def wait_if_paused(self, job_id, *, publish_paused=False):
            state["mounted"] = True   # user reconnects the volume
            self._paused = False      # ...and hits Resume
            return False

    params = PipelineParams(
        collection_id=col_id,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = ReconnectingRunner()
    job = _make_job()

    run_pipeline_job(job, runner, db_path, ws_id, params)

    assert len(runner.pause_calls) == 1, (
        f"Expected exactly one pause (the share dropped once); got "
        f"{len(runner.pause_calls)}."
    )

    # The photo in flight when the share died must be retried after resume
    # (Codex #1388 P2), so every photo — including that one — should have
    # been classified. Silently skipping it would leave the user with a
    # stranded photo even though the source is back, and on a reclassify
    # run finalization would clear its old prediction with no replacement.
    stored = db.conn.execute(
        "SELECT COUNT(DISTINCT d.photo_id) FROM predictions p "
        "JOIN detections d ON d.id = p.detection_id",
    ).fetchone()[0]
    assert stored == 6, (
        f"Reconnect+resume must retry the paused photo so every one of the "
        f"6 gets classified, not skip it as unreachable; only {stored} got "
        f"predictions."
    )

    classify_steps = [
        kwargs for (_jid, sid, kwargs) in runner.step_updates
        if sid.startswith("classify:") and "summary" in kwargs
    ]
    summaries = " ".join(k["summary"] for k in classify_steps)
    assert "stopped after" not in summaries, (
        f"The run recovered, so the summary must not claim it stopped early; "
        f"got {summaries!r}"
    )
    assert "unreachable" not in summaries, (
        f"After a successful resume nothing should still be reported as "
        f"unreachable — the retry classified it; got {summaries!r}"
    )


def test_pipeline_classify_resets_pause_budget_after_successful_recovery(
    tmp_path, monkeypatch,
):
    """A successful reconnect must refund the pause budget for later outages.

    Codex #1388 P2 (r3663816327): ``source_offline["pauses"]`` counts
    every pause the run takes for a dead source and gives up once the
    counter reaches ``_MAX_SOURCE_OFFLINE_PAUSES``. The intent is to
    protect against a user who keeps pressing Resume without actually
    remounting the share — an infinite pause/retry ping-pong.

    Pre-fix, the counter never reset even when the user genuinely
    reconnected and the retry succeeded. On a long classification run
    over a share that drops and recovers several times (or over several
    volumes that drop separately), the counter would climb across
    unrelated outages and a later drop would take the give-up branch on
    the first pause — the user would never be offered Resume for it,
    even though the earlier drops were all recovered.

    Pin that after ``_MAX_SOURCE_OFFLINE_PAUSES + 2`` separate
    drop-and-recover cycles every photo still classifies: a
    successful retry is proof the share IS back, so the counter can
    honestly reset.
    """
    import classifier as classifier_mod
    import classify_job
    import config as cfg
    import pipeline_job as pj
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)
    # Enough photos to trigger MORE separate drops than the pause budget
    # allows in a single continuous outage — without the reset, the run
    # would give up partway through even though every drop was recovered.
    n_photos = pj._MAX_SOURCE_OFFLINE_PAUSES + 3
    photo_ids = []
    for i in range(n_photos):
        name = f"photo{i}.jpg"
        pid = db.add_photo(folder_id, name, ".jpg", 4000 + i, 4_000_000.0 + i)
        _drop_jpeg(folder_path, name)
        db.save_detections(
            pid,
            [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
              "confidence": 0.9, "category": "animal"}],
            detector_model="MegaDetector",
        )
        photo_ids.append(pid)

    col_id = db.add_collection(
        "Test",
        json.dumps([{"field": "photo_ids", "value": photo_ids}]),
    )

    _setup_fake_downloaded_model(tmp_path, monkeypatch)

    import labels_fingerprint as lfp
    monkeypatch.setattr(lfp, "compute_fingerprint", lambda *a, **k: "fp")

    def fake_detect_batch(batch, folders, runner, job, reclassify, db_,
                          det_conf_threshold=None, already_detected_ids=None,
                          cached_detections=None):
        det_map = {}
        for p in batch:
            det_map[p["id"]] = [{
                "id": d["id"],
                "box_x": d["box_x"], "box_y": d["box_y"],
                "box_w": d["box_w"], "box_h": d["box_h"],
                "confidence": d["detector_confidence"],
                "category": d["category"],
            } for d in db_.get_detections(p["id"])
                if d["detector_model"] != "full-image"]
        return det_map, len(batch), {p["id"] for p in batch}

    monkeypatch.setattr(classify_job, "_detect_batch", fake_detect_batch)

    from PIL import Image as _PILImage

    gone_folder = "/Volumes/DefinitelyNotMounted12345/Raw Files"
    # ``mounted`` flips False right before each initial read attempt so
    # every photo trips a fresh drop; ``wait_if_paused`` flips it back
    # True to model the user remounting the share for the resume.
    state = {"mounted": True}

    def flaky_prepare_image(photo, folders, detection, vireo_dir=None):
        if not state["mounted"]:
            return None, gone_folder, os.path.join(
                gone_folder, photo["filename"],
            )
        # Flip immediately so the NEXT photo also trips a drop. The
        # current photo already loaded successfully; the drop will be
        # discovered on the next photo's first read attempt.
        state["mounted"] = False
        fp = folders.get(photo["folder_id"], "")
        return (
            _PILImage.new("RGB", (16, 16), "black"),
            fp,
            os.path.join(fp, photo["filename"]),
        )

    monkeypatch.setattr(classify_job, "_prepare_image", flaky_prepare_image)

    def fake_flush_batch(batch, clf, model_type, model_name, db_, raw_results,
                         top_k=1):
        for entry in batch:
            raw_results.append({
                "photo": entry["photo"],
                "detection_id": entry.get("detection_id"),
                "folder_path": entry["folder_path"],
                "image_path": entry["image_path"],
                "prediction": "Robin",
                "confidence": 0.9,
                "timestamp": None,
                "filename": entry["photo"]["filename"],
                "embedding": None,
                "taxonomy": None,
            })
        return 0

    monkeypatch.setattr(classify_job, "_flush_batch", fake_flush_batch)

    class FakeClassifier:
        def __init__(self, *args, **kwargs):
            pass

        def encode_image(self, *args, **kwargs):
            import numpy as np
            return np.zeros(512, dtype=np.float32)

    monkeypatch.setattr(classifier_mod, "Classifier", FakeClassifier)

    class ReconnectingRunner(FakeRunner):
        """The user genuinely remounts the share on every pause."""

        def __init__(self):
            super().__init__()
            self.pause_calls = []
            self._paused = False

        def pause_job(self, job_id):
            self.pause_calls.append(job_id)
            self._paused = True
            return True

        def pause_requested(self, job_id):
            return self._paused

        def mark_paused(self, job_id):
            return True

        def wait_if_paused(self, job_id, *, publish_paused=False):
            state["mounted"] = True   # user reconnects the volume
            self._paused = False
            return False

    params = PipelineParams(
        collection_id=col_id,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = ReconnectingRunner()
    job = _make_job()

    run_pipeline_job(job, runner, db_path, ws_id, params)

    # Setup sanity: we must have paused MORE times than the fixed budget
    # allows in a single continuous outage — otherwise this test isn't
    # exercising the reset path at all.
    assert len(runner.pause_calls) > pj._MAX_SOURCE_OFFLINE_PAUSES, (
        f"Setup sanity: this test exercises the pause-budget reset by "
        f"pausing more than the fixed budget of "
        f"{pj._MAX_SOURCE_OFFLINE_PAUSES} across separate recovered "
        f"outages; only {len(runner.pause_calls)} pauses fired."
    )

    # The heart of the fix: every drop was recovered, so every photo
    # must be classified. Pre-fix the run would give up after the budget
    # was spent (~3 photos in), leaving the tail unreachable.
    stored = db.conn.execute(
        "SELECT COUNT(DISTINCT d.photo_id) FROM predictions p "
        "JOIN detections d ON d.id = p.detection_id",
    ).fetchone()[0]
    assert stored == n_photos, (
        f"Every drop was recovered by the user, so every one of the "
        f"{n_photos} photos must be classified. Only {stored} got "
        f"predictions — the pause budget wasn't refunded after the "
        f"successful retries, so the run gave up partway through."
    )

    # And the terminal errors list must stay clean — a run that recovered
    # every outage is a successful run.
    classify_errors = [
        e for e in (job.get("errors") or [])
        if isinstance(e, str) and e.startswith("[classify]")
    ]
    assert not classify_errors, (
        f"Every outage was recovered, so no [classify] entry should be "
        f"latched in the terminal errors list. Got {classify_errors!r}."
    )


def test_pipeline_classify_offline_source_is_not_a_clean_success(
    tmp_path, monkeypatch,
):
    """Giving up on a dead source must fail the job with an accurate headline.

    Marking classify 'completed' would put a green check on a run that never
    opened most of the collection, and the end-of-run rollup picks the job's
    headline error by the '[classify] Fatal:' prefix — without one it reports
    whatever unrelated warning happened to land in errors[0].
    """
    import classifier as classifier_mod
    import classify_job
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)
    photo_ids = []
    for i in range(3):
        name = f"photo{i}.jpg"
        pid = db.add_photo(folder_id, name, ".jpg", 4000 + i, 4_000_000.0 + i)
        _drop_jpeg(folder_path, name)
        db.save_detections(
            pid,
            [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
              "confidence": 0.9, "category": "animal"}],
            detector_model="MegaDetector",
        )
        photo_ids.append(pid)

    col_id = db.add_collection(
        "Test",
        json.dumps([{"field": "photo_ids", "value": photo_ids}]),
    )

    _setup_fake_downloaded_model(tmp_path, monkeypatch)

    import labels_fingerprint as lfp
    monkeypatch.setattr(lfp, "compute_fingerprint", lambda *a, **k: "fp")

    def fake_detect_batch(batch, folders, runner, job, reclassify, db_,
                          det_conf_threshold=None, already_detected_ids=None,
                          cached_detections=None):
        det_map = {}
        for p in batch:
            det_map[p["id"]] = [{
                "id": d["id"],
                "box_x": d["box_x"], "box_y": d["box_y"],
                "box_w": d["box_w"], "box_h": d["box_h"],
                "confidence": d["detector_confidence"],
                "category": d["category"],
            } for d in db_.get_detections(p["id"])
                if d["detector_model"] != "full-image"]
        return det_map, len(batch), {p["id"] for p in batch}

    monkeypatch.setattr(classify_job, "_detect_batch", fake_detect_batch)

    gone_folder = "/Volumes/DefinitelyNotMounted12345/Raw Files"

    def offline_prepare_image(photo, folders, detection, vireo_dir=None):
        return None, gone_folder, os.path.join(gone_folder, photo["filename"])

    monkeypatch.setattr(classify_job, "_prepare_image", offline_prepare_image)

    class FakeClassifier:
        def __init__(self, *args, **kwargs):
            pass

        def encode_image(self, *args, **kwargs):
            import numpy as np
            return np.zeros(512, dtype=np.float32)

    monkeypatch.setattr(classifier_mod, "Classifier", FakeClassifier)

    params = PipelineParams(
        collection_id=col_id,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    # A plain FakeRunner has no pause support, so classify gives up at once —
    # the same path a non-pausable job takes.
    runner = FakeRunner()
    job = _make_job()

    with pytest.raises(RuntimeError) as excinfo:
        run_pipeline_job(job, runner, db_path, ws_id, params)

    msg = str(excinfo.value)
    assert "/Volumes/DefinitelyNotMounted12345" in msg, (
        f"The job's headline error must name the offline volume rather than "
        f"an unrelated earlier warning; got {msg!r}"
    )
    assert "failed to classify" not in msg, (
        f"An offline share must not be reported as photos failing to "
        f"classify; got {msg!r}"
    )


def test_pipeline_classify_source_offline_publishes_pause_reason(
    tmp_path, monkeypatch,
):
    """A parked classify run must tell the UI *why* it paused, not just that
    it did.

    Codex #1388 P1 r3663383513: the incident fix parked the job on a dead
    source but only wrote the reason to ``vireo.log``. The jobs UI showed
    a generic "paused" pill with no indication of which volume the user
    needed to reconnect, so pressing Resume without remounting could burn
    through the bounded retry budget and turn a recoverable outage into a
    failed run. Publish the reason via transient progress state (mirrored
    onto ``job['progress']``) and pin the classify step's ``current_file``
    so the reason renders under the paused step, next to Resume.
    """
    import classifier as classifier_mod
    import classify_job
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)
    photo_ids = []
    for i in range(3):
        name = f"photo{i}.jpg"
        pid = db.add_photo(folder_id, name, ".jpg", 4000 + i, 4_000_000.0 + i)
        _drop_jpeg(folder_path, name)
        db.save_detections(
            pid,
            [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
              "confidence": 0.9, "category": "animal"}],
            detector_model="MegaDetector",
        )
        photo_ids.append(pid)

    col_id = db.add_collection(
        "Test",
        json.dumps([{"field": "photo_ids", "value": photo_ids}]),
    )

    _setup_fake_downloaded_model(tmp_path, monkeypatch)

    import labels_fingerprint as lfp
    monkeypatch.setattr(lfp, "compute_fingerprint", lambda *a, **k: "fp")

    def fake_detect_batch(batch, folders, runner, job, reclassify, db_,
                          det_conf_threshold=None, already_detected_ids=None,
                          cached_detections=None):
        det_map = {}
        for p in batch:
            det_map[p["id"]] = [{
                "id": d["id"],
                "box_x": d["box_x"], "box_y": d["box_y"],
                "box_w": d["box_w"], "box_h": d["box_h"],
                "confidence": d["detector_confidence"],
                "category": d["category"],
            } for d in db_.get_detections(p["id"])
                if d["detector_model"] != "full-image"]
        return det_map, len(batch), {p["id"] for p in batch}

    monkeypatch.setattr(classify_job, "_detect_batch", fake_detect_batch)

    gone_folder = "/Volumes/NamedShareForBanner/Raw Files"

    def offline_prepare_image(photo, folders, detection, vireo_dir=None):
        return None, gone_folder, os.path.join(gone_folder, photo["filename"])

    monkeypatch.setattr(classify_job, "_prepare_image", offline_prepare_image)

    class FakeClassifier:
        def __init__(self, *args, **kwargs):
            pass

        def encode_image(self, *args, **kwargs):
            import numpy as np
            return np.zeros(512, dtype=np.float32)

    monkeypatch.setattr(classifier_mod, "Classifier", FakeClassifier)

    class PausingRunner(FakeRunner):
        def __init__(self):
            super().__init__()
            self.pause_calls = []
            self._paused = False

        def pause_job(self, job_id):
            self.pause_calls.append(job_id)
            self._paused = True
            return True

        def pause_requested(self, job_id):
            return self._paused

        def mark_paused(self, job_id):
            return True

        def wait_if_paused(self, job_id, *, publish_paused=False):
            self._paused = False
            return False

    params = PipelineParams(
        collection_id=col_id,
        skip_extract_masks=True,
        skip_regroup=True,
    )
    runner = PausingRunner()
    job = _make_job()

    with pytest.raises(RuntimeError):
        run_pipeline_job(job, runner, db_path, ws_id, params)

    # The banner text must name the volume the user has to reconnect —
    # a generic "paused" is exactly what Codex flagged.
    pause_progress_events = [
        data for (_jid, etype, data) in runner.events
        if etype == "progress" and data.get("pause_reason")
    ]
    assert pause_progress_events, (
        "Classify parked on a dead source but never published pause_reason "
        "on a progress event; the UI has no way to tell the user *why* "
        "the job paused."
    )
    reasons = [d["pause_reason"] for d in pause_progress_events]
    joined_reasons = " ".join(reasons)
    assert "NamedShareForBanner" in joined_reasons, (
        f"The pause_reason must name the volume so the user knows what to "
        f"reconnect; got {reasons!r}"
    )
    assert "Resume" in joined_reasons, (
        f"The pause_reason must tell the user what to do after reconnecting "
        f"(press Resume); got {reasons!r}"
    )

    # The classify step's current_file must carry the same reason so it
    # renders directly under the paused step in the job tree, right next
    # to the Resume button — the header banner alone is easy to miss on
    # long collections whose stage tree scrolls off-screen.
    classify_current_file = [
        kwargs["current_file"]
        for (_jid, sid, kwargs) in runner.step_updates
        if sid.startswith("classify:") and "current_file" in kwargs
        and kwargs["current_file"]
    ]
    assert any(
        "NamedShareForBanner" in cf for cf in classify_current_file
    ), (
        f"The classify step should surface the offline reason via "
        f"current_file so it renders under the paused step; got "
        f"{classify_current_file!r}"
    )


def test_pipeline_classify_source_offline_honors_prior_user_pause(
    tmp_path, monkeypatch,
):
    """If the user pressed Pause while a network read was blocked, classify
    must still park on the checkpoint — not treat the second-pause no-op
    as evidence that pausing is impossible.

    Codex #1388 P2 r3663383518: ``JobRunner.pause_job`` returns ``False``
    when the job's status is already ``pausing`` (a Pause request landed
    while classify was blocked on an EIO). The old code interpreted every
    ``False`` return as "nothing to park on" and gave up, throwing away
    the user's Pause click and turning a recoverable outage into a failed
    run. Treat ``pause_requested`` as evidence a pause is genuinely in
    flight and fall through to the checkpoint so the run parks on the
    already-in-flight request instead of latching source offline.
    """
    import classifier as classifier_mod
    import classify_job
    import config as cfg
    from db import Database
    from PIL import Image as _PILImage

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)
    photo_ids = []
    for i in range(4):
        name = f"photo{i}.jpg"
        pid = db.add_photo(folder_id, name, ".jpg", 4000 + i, 4_000_000.0 + i)
        _drop_jpeg(folder_path, name)
        db.save_detections(
            pid,
            [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
              "confidence": 0.9, "category": "animal"}],
            detector_model="MegaDetector",
        )
        photo_ids.append(pid)

    col_id = db.add_collection(
        "Test",
        json.dumps([{"field": "photo_ids", "value": photo_ids}]),
    )

    _setup_fake_downloaded_model(tmp_path, monkeypatch)

    import labels_fingerprint as lfp
    monkeypatch.setattr(lfp, "compute_fingerprint", lambda *a, **k: "fp")

    def fake_detect_batch(batch, folders, runner, job, reclassify, db_,
                          det_conf_threshold=None, already_detected_ids=None,
                          cached_detections=None):
        det_map = {}
        for p in batch:
            det_map[p["id"]] = [{
                "id": d["id"],
                "box_x": d["box_x"], "box_y": d["box_y"],
                "box_w": d["box_w"], "box_h": d["box_h"],
                "confidence": d["detector_confidence"],
                "category": d["category"],
            } for d in db_.get_detections(p["id"])
                if d["detector_model"] != "full-image"]
        return det_map, len(batch), {p["id"] for p in batch}

    monkeypatch.setattr(classify_job, "_detect_batch", fake_detect_batch)

    gone_folder = "/Volumes/DefinitelyNotMounted12345/Raw Files"
    # Give ``flaky_prepare_image`` a handle to the runner so the "user
    # pressed Pause while we were blocked" click can be timed to land at
    # the exact moment the read starts blocking — that's what puts the
    # job into ``pausing`` state before classify's own pause_job() call.
    holder = {"runner": None}
    state = {"mounted": True, "seen": 0}

    def flaky_prepare_image(photo, folders, detection, vireo_dir=None):
        state["seen"] += 1
        if state["seen"] == 2:
            state["mounted"] = False
            # The Pause click lands here (the read is about to block on
            # EIO). By the time _handle_source_offline calls pause_job the
            # job's public state is already ``pausing`` and pause_job
            # returns False — the exact edge case Codex #1388 P2 flagged.
            if holder["runner"] is not None:
                holder["runner"]._paused = True
        if not state["mounted"]:
            return None, gone_folder, os.path.join(
                gone_folder, photo["filename"],
            )
        fp = folders.get(photo["folder_id"], "")
        return (
            _PILImage.new("RGB", (16, 16), "black"),
            fp,
            os.path.join(fp, photo["filename"]),
        )

    monkeypatch.setattr(classify_job, "_prepare_image", flaky_prepare_image)

    def fake_flush_batch(batch, clf, model_type, model_name, db_, raw_results,
                         top_k=1):
        for entry in batch:
            raw_results.append({
                "photo": entry["photo"],
                "detection_id": entry.get("detection_id"),
                "folder_path": entry["folder_path"],
                "image_path": entry["image_path"],
                "prediction": "Robin",
                "confidence": 0.9,
                "timestamp": None,
                "filename": entry["photo"]["filename"],
                "embedding": None,
                "taxonomy": None,
            })
        return 0

    monkeypatch.setattr(classify_job, "_flush_batch", fake_flush_batch)

    class FakeClassifier:
        def __init__(self, *args, **kwargs):
            pass

        def encode_image(self, *args, **kwargs):
            import numpy as np
            return np.zeros(512, dtype=np.float32)

    monkeypatch.setattr(classifier_mod, "Classifier", FakeClassifier)

    class PriorPauseRunner(FakeRunner):
        """Models the user pressing Pause while classify was blocked on EIO.

        ``pause_job`` returns ``False`` — the job's public state is already
        ``pausing`` — but ``pause_requested`` reports ``True`` so classify
        can still tell there is a live pause in flight and park on it.
        """

        def __init__(self):
            super().__init__()
            self.pause_calls = []
            self.wait_calls = 0
            self._paused = False

        def pause_job(self, job_id):
            self.pause_calls.append(job_id)
            return False

        def pause_requested(self, job_id):
            return self._paused

        def mark_paused(self, job_id):
            return True

        def wait_if_paused(self, job_id, *, publish_paused=False):
            self.wait_calls += 1
            state["mounted"] = True   # user reconnects the volume
            self._paused = False      # ...then hits Resume
            return False

    params = PipelineParams(
        collection_id=col_id,
        skip_extract_masks=True,
        skip_regroup=True,
    )
    runner = PriorPauseRunner()
    holder["runner"] = runner
    job = _make_job()

    run_pipeline_job(job, runner, db_path, ws_id, params)

    # The pre-fix behavior gave up as soon as pause_job returned False,
    # never entered the checkpoint, and latched source_offline["reason"]
    # so classify failed with a ``[classify] Fatal:`` error naming the
    # volume. Now that we honor the already-in-flight pause, the checkpoint
    # runs, the user's reconnect+resume flow completes normally, and the
    # collection classifies to green.
    assert runner.wait_calls >= 1, (
        "Classify saw an offline read AND a live pause request, but never "
        "reached the pause checkpoint — the user's Pause click was silently "
        "discarded (Codex #1388 P2)."
    )
    stored = db.conn.execute(
        "SELECT COUNT(DISTINCT d.photo_id) FROM predictions p "
        "JOIN detections d ON d.id = p.detection_id",
    ).fetchone()[0]
    assert stored == 4, (
        f"After the checkpoint parks and the user resumes with the mount "
        f"back, classify should retry and finish the collection — instead "
        f"only {stored}/4 photos got predictions, which is the pre-fix "
        f"give-up path."
    )
    # And crucially, the run must not have logged a headline failure —
    # the whole point of parking (instead of giving up on pause_job=False)
    # is to preserve the recovery path.
    classify_errors = [e for e in job["errors"] if "[classify] Fatal:" in e]
    assert not classify_errors, (
        f"A recovered pause must not surface as a terminal classify error; "
        f"got {classify_errors!r}"
    )


def test_pipeline_classify_missing_folder_does_not_stop_healthy_folders(
    tmp_path, monkeypatch,
):
    """A single deleted local folder must not stop the whole classification.

    Codex #1388 P1: the incident-fix's global-scope offline signal would
    treat one unreadable folder as evidence the entire collection is
    offline, so later photos in healthy folders would be skipped without
    even being tried — and on a ``reclassify=True`` run, finalization
    would clear their existing predictions with no replacement. Scope
    ``"folder"`` in ``_source_offline_reason`` keeps the run going past
    the missing folder so the healthy ones still get classified.
    """
    import classifier as classifier_mod
    import classify_job
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    # Two local folders whose files stay on disk (previews/thumbnails read
    # them). The classify-time _prepare_image mock below simulates one
    # folder going away between preview and classify.
    gone_folder_path = str(tmp_path / "vanished")
    healthy_folder_path = str(tmp_path / "still_here")
    os.makedirs(gone_folder_path, exist_ok=True)
    os.makedirs(healthy_folder_path, exist_ok=True)

    gone_folder_id = db.add_folder(gone_folder_path)
    healthy_folder_id = db.add_folder(healthy_folder_path)

    photo_ids = []
    gone_photo_ids: set = set()
    healthy_photo_ids = []
    for i in range(3):
        name = f"gone{i}.jpg"
        pid = db.add_photo(
            gone_folder_id, name, ".jpg", 4000 + i, 4_000_000.0 + i,
        )
        _drop_jpeg(gone_folder_path, name)
        db.save_detections(
            pid,
            [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
              "confidence": 0.9, "category": "animal"}],
            detector_model="MegaDetector",
        )
        photo_ids.append(pid)
        gone_photo_ids.add(pid)
    for i in range(3):
        name = f"healthy{i}.jpg"
        pid = db.add_photo(
            healthy_folder_id, name, ".jpg", 5000 + i, 5_000_000.0 + i,
        )
        _drop_jpeg(healthy_folder_path, name)
        db.save_detections(
            pid,
            [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
              "confidence": 0.9, "category": "animal"}],
            detector_model="MegaDetector",
        )
        photo_ids.append(pid)
        healthy_photo_ids.append(pid)

    col_id = db.add_collection(
        "Test",
        json.dumps([{"field": "photo_ids", "value": photo_ids}]),
    )

    _setup_fake_downloaded_model(tmp_path, monkeypatch)

    import labels_fingerprint as lfp
    monkeypatch.setattr(lfp, "compute_fingerprint", lambda *a, **k: "fp")

    def fake_detect_batch(batch, folders, runner, job, reclassify, db_,
                          det_conf_threshold=None, already_detected_ids=None,
                          cached_detections=None):
        det_map = {}
        for p in batch:
            det_map[p["id"]] = [{
                "id": d["id"],
                "box_x": d["box_x"], "box_y": d["box_y"],
                "box_w": d["box_w"], "box_h": d["box_h"],
                "confidence": d["detector_confidence"],
                "category": d["category"],
            } for d in db_.get_detections(p["id"])
                if d["detector_model"] != "full-image"]
        return det_map, len(batch), {p["id"] for p in batch}

    monkeypatch.setattr(classify_job, "_detect_batch", fake_detect_batch)

    from PIL import Image as _PILImage

    # The gone folder's classify-time paths point at a subdir that never
    # exists — that trips _source_offline_reason's folder-scoped branch
    # without disturbing previews (which see the real files at the paths
    # stored in the DB).
    fake_missing_root = str(tmp_path / "vanished_at_classify_time")

    def selective_prepare_image(photo, folders, detection, vireo_dir=None):
        if photo["id"] in gone_photo_ids:
            image_path = os.path.join(fake_missing_root, photo["filename"])
            return None, fake_missing_root, image_path
        fp = folders.get(photo["folder_id"], "")
        return (
            _PILImage.new("RGB", (16, 16), "black"),
            fp,
            os.path.join(fp, photo["filename"]),
        )

    monkeypatch.setattr(classify_job, "_prepare_image", selective_prepare_image)

    def fake_flush_batch(batch, clf, model_type, model_name, db_, raw_results,
                         top_k=1):
        for entry in batch:
            raw_results.append({
                "photo": entry["photo"],
                "detection_id": entry.get("detection_id"),
                "folder_path": entry["folder_path"],
                "image_path": entry["image_path"],
                "prediction": "Robin",
                "confidence": 0.9,
                "timestamp": None,
                "filename": entry["photo"]["filename"],
                "embedding": None,
                "taxonomy": None,
            })
        return 0

    monkeypatch.setattr(classify_job, "_flush_batch", fake_flush_batch)

    class FakeClassifier:
        def __init__(self, *args, **kwargs):
            pass

        def encode_image(self, *args, **kwargs):
            import numpy as np
            return np.zeros(512, dtype=np.float32)

    monkeypatch.setattr(classifier_mod, "Classifier", FakeClassifier)

    # A pausing runner would let the whole run stop; use a plain FakeRunner
    # so the folder-scoped branch has to keep going on its own rather than
    # falling back to the pause path.
    class NoPauseRunner(FakeRunner):
        def pause_job(self, job_id):
            return False

        def wait_if_paused(self, job_id, *, publish_paused=False):
            return False

    params = PipelineParams(
        collection_id=col_id,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = NoPauseRunner()
    job = _make_job()

    # The run finishes failed — some photos never got opened — but only AFTER
    # the healthy folder has been fully processed. Codex #1388 P1 (second
    # round): a folder-scoped outage must not report as a clean green job,
    # but that must not come at the cost of stranding the reachable folders.
    with pytest.raises(RuntimeError) as excinfo:
        run_pipeline_job(job, runner, db_path, ws_id, params)

    # The healthy folder's photos must be classified even though the other
    # folder went away. Without the P1 fix, the gone folder trips a global
    # source_offline["reason"] and the batch/photo loops break before the
    # healthy folder gets a turn.
    healthy_stored = db.conn.execute(
        f"SELECT COUNT(DISTINCT d.photo_id) FROM predictions p "
        f"JOIN detections d ON d.id = p.detection_id "
        f"WHERE d.photo_id IN "
        f"({','.join('?' * len(healthy_photo_ids))})",
        list(healthy_photo_ids),
    ).fetchone()[0]
    assert healthy_stored == len(healthy_photo_ids), (
        f"A single missing folder must not strand healthy folders; "
        f"expected {len(healthy_photo_ids)} healthy photos classified but "
        f"got {healthy_stored}."
    )

    # The gone folder's photos should surface as unreachable, not as a
    # global offline that stopped the run.
    classify_steps = [
        kwargs for (_jid, sid, kwargs) in runner.step_updates
        if sid.startswith("classify:") and "summary" in kwargs
    ]
    summaries = " ".join(k["summary"] for k in classify_steps)
    assert "unreachable (source offline)" in summaries, (
        f"Photos in the missing folder should be reported as unreachable "
        f"rather than folded into failures; got {summaries!r}"
    )
    assert "stopped after" not in summaries, (
        f"A folder-scoped outage must not report the whole pass as "
        f"stopped early; got {summaries!r}"
    )
    assert "failed" not in summaries, (
        f"Missing-folder photos must not be counted as failures; "
        f"got {summaries!r}"
    )

    # The headline error must name the outage honestly. Without the fatal
    # entry, the end-of-run rollup falls back to errors[0] (an unrelated
    # per-photo warning logged much earlier) and the user sees a mystery
    # failure instead of "reconnect the missing folder".
    msg = str(excinfo.value)
    assert "unreachable" in msg, (
        f"A folder-scoped outage must produce a headline error naming the "
        f"unreachable photos; got {msg!r}"
    )
    assert "failed to classify" not in msg, (
        f"Unreachable photos must not be reported as classification "
        f"failures in the headline; got {msg!r}"
    )


def test_pipeline_classify_folder_outage_is_not_a_clean_success(
    tmp_path, monkeypatch,
):
    """Folder-scoped outages must not land on the job tree as green.

    Codex #1388 P1 (second round): the folder-scope branch increments
    ``source_skipped`` and continues without latching
    ``source_offline["reason"]``. A collection whose only folder disappeared
    reaches finalization with ``total_failed == 0`` and — pre-fix — landed
    with ``stages["classify"]["status"] == "completed"`` and no headline
    error. The user's photos were never opened; the job must say so.
    """
    import classifier as classifier_mod
    import classify_job
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)
    photo_ids = []
    for i in range(3):
        name = f"photo{i}.jpg"
        pid = db.add_photo(folder_id, name, ".jpg", 4000 + i, 4_000_000.0 + i)
        _drop_jpeg(folder_path, name)
        db.save_detections(
            pid,
            [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
              "confidence": 0.9, "category": "animal"}],
            detector_model="MegaDetector",
        )
        photo_ids.append(pid)

    col_id = db.add_collection(
        "Test",
        json.dumps([{"field": "photo_ids", "value": photo_ids}]),
    )

    _setup_fake_downloaded_model(tmp_path, monkeypatch)

    import labels_fingerprint as lfp
    monkeypatch.setattr(lfp, "compute_fingerprint", lambda *a, **k: "fp")

    def fake_detect_batch(batch, folders, runner, job, reclassify, db_,
                          det_conf_threshold=None, already_detected_ids=None,
                          cached_detections=None):
        det_map = {}
        for p in batch:
            det_map[p["id"]] = [{
                "id": d["id"],
                "box_x": d["box_x"], "box_y": d["box_y"],
                "box_w": d["box_w"], "box_h": d["box_h"],
                "confidence": d["detector_confidence"],
                "category": d["category"],
            } for d in db_.get_detections(p["id"])
                if d["detector_model"] != "full-image"]
        return det_map, len(batch), {p["id"] for p in batch}

    monkeypatch.setattr(classify_job, "_detect_batch", fake_detect_batch)

    # Point every photo at a folder that never existed on disk — trips the
    # folder-scoped branch (mount root is still there, the folder itself is
    # not) for every photo in the collection.
    fake_missing_folder = str(tmp_path / "vanished_folder")

    def folder_missing_prepare_image(photo, folders, detection, vireo_dir=None):
        return None, fake_missing_folder, os.path.join(
            fake_missing_folder, photo["filename"],
        )

    monkeypatch.setattr(
        classify_job, "_prepare_image", folder_missing_prepare_image,
    )

    class FakeClassifier:
        def __init__(self, *args, **kwargs):
            pass

        def encode_image(self, *args, **kwargs):
            import numpy as np
            return np.zeros(512, dtype=np.float32)

    monkeypatch.setattr(classifier_mod, "Classifier", FakeClassifier)

    params = PipelineParams(
        collection_id=col_id,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()

    with pytest.raises(RuntimeError) as excinfo:
        run_pipeline_job(job, runner, db_path, ws_id, params)

    msg = str(excinfo.value)
    assert "[classify] Fatal:" in msg, (
        f"A folder-scoped outage must produce a fatal [classify]-prefixed "
        f"error so the end-of-run rollup picks it as the headline instead "
        f"of an unrelated warning; got {msg!r}"
    )
    assert "unreachable" in msg, (
        f"The fatal error must name the outage as unreachable photos; "
        f"got {msg!r}"
    )
    assert "failed to classify" not in msg, (
        f"Photos we never opened are not classification failures; the "
        f"headline must not describe them as such. Got {msg!r}"
    )

    # The structured result must reflect the outage count so downstream
    # consumers (jobs API, pipeline card) can render it accurately. The
    # raise itself proves stages["classify"]["status"] was set to 'failed'
    # — the only path that reaches the run_pipeline_job failed-stage
    # rollup — so re-asserting status here would double-cover the same
    # signal.
    result_stages = job["result"]["stages"]
    assert result_stages["classify"].get("source_skipped") == len(photo_ids), (
        f"Expected {len(photo_ids)} photos flagged as source_skipped; "
        f"got {result_stages['classify']!r}"
    )


def test_pipeline_classify_folder_outage_counts_each_photo_once(
    tmp_path, monkeypatch,
):
    """A multi-subject unreachable photo counts once, not once per detection.

    Codex #1388 P2 (r3664348763): the per-spec ``source_skipped`` counter
    was incremented inside the per-detection loop. A single photo with N
    qualifying detections would report ``N unreachable`` even though the
    per-spec ``total`` (and the ``spec_source_skipped_photo_ids`` set) are
    photo-scoped, so the step summary could read e.g. ``3 unreachable``
    out of ``1`` photo. Pin that a single unreachable photo with several
    detections counts as one skipped photo.
    """
    import classifier as classifier_mod
    import classify_job
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)
    name = "photo0.jpg"
    pid = db.add_photo(folder_id, name, ".jpg", 4000, 4_000_000.0)
    _drop_jpeg(folder_path, name)
    # Three qualifying animal detections on the same photo — this is what
    # a multi-subject frame looks like to classify.
    db.save_detections(
        pid,
        [
            {"box": {"x": 0.1, "y": 0.1, "w": 0.2, "h": 0.2},
             "confidence": 0.9, "category": "animal"},
            {"box": {"x": 0.4, "y": 0.4, "w": 0.2, "h": 0.2},
             "confidence": 0.85, "category": "animal"},
            {"box": {"x": 0.7, "y": 0.1, "w": 0.2, "h": 0.2},
             "confidence": 0.8, "category": "animal"},
        ],
        detector_model="MegaDetector",
    )

    col_id = db.add_collection(
        "Test",
        json.dumps([{"field": "photo_ids", "value": [pid]}]),
    )

    _setup_fake_downloaded_model(tmp_path, monkeypatch)

    import labels_fingerprint as lfp
    monkeypatch.setattr(lfp, "compute_fingerprint", lambda *a, **k: "fp")

    def fake_detect_batch(batch, folders, runner, job, reclassify, db_,
                          det_conf_threshold=None, already_detected_ids=None,
                          cached_detections=None):
        det_map = {}
        for p in batch:
            det_map[p["id"]] = [{
                "id": d["id"],
                "box_x": d["box_x"], "box_y": d["box_y"],
                "box_w": d["box_w"], "box_h": d["box_h"],
                "confidence": d["detector_confidence"],
                "category": d["category"],
            } for d in db_.get_detections(p["id"])
                if d["detector_model"] != "full-image"]
        return det_map, len(batch), {p["id"] for p in batch}

    monkeypatch.setattr(classify_job, "_detect_batch", fake_detect_batch)

    # Point every detection at a folder that never existed on disk — trips
    # the folder-scoped branch. Because ``_prepare_image`` fails at the
    # per-detection level, pre-fix the counter fires three times for this
    # one photo.
    fake_missing_folder = str(tmp_path / "vanished_folder")

    def folder_missing_prepare_image(photo, folders, detection, vireo_dir=None):
        return None, fake_missing_folder, os.path.join(
            fake_missing_folder, photo["filename"],
        )

    monkeypatch.setattr(
        classify_job, "_prepare_image", folder_missing_prepare_image,
    )

    class FakeClassifier:
        def __init__(self, *args, **kwargs):
            pass

        def encode_image(self, *args, **kwargs):
            import numpy as np
            return np.zeros(512, dtype=np.float32)

    monkeypatch.setattr(classifier_mod, "Classifier", FakeClassifier)

    params = PipelineParams(
        collection_id=col_id,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()

    with pytest.raises(RuntimeError):
        run_pipeline_job(job, runner, db_path, ws_id, params)

    # The per-model step summary must say "1 unreachable" for our single
    # multi-detection photo, not "3 unreachable" (one tick per detection).
    classify_steps = [
        kwargs for (_jid, sid, kwargs) in runner.step_updates
        if sid.startswith("classify:") and "summary" in kwargs
    ]
    summaries = " ".join(k["summary"] for k in classify_steps)
    assert "1 unreachable" in summaries, (
        f"A single unreachable photo with multiple qualifying detections "
        f"must count as one skipped photo in the step summary, not once "
        f"per detection. Got summary {summaries!r}"
    )
    assert "3 unreachable" not in summaries, (
        f"Pre-fix regression: the summary reported N unreachable for a "
        f"multi-subject photo with N detections. Got {summaries!r}"
    )

    # And the stage rollup must report the same photo-scoped count.
    result_stages = job["result"]["stages"]
    assert result_stages["classify"].get("source_skipped") == 1, (
        f"Structured source_skipped is a photo-scoped count for downstream "
        f"consumers; got {result_stages['classify']!r}"
    )


def test_pipeline_classify_give_up_skips_downstream_stages(
    tmp_path, monkeypatch,
):
    """After classify gives up on an offline source, downstream stages skip.

    CodeRabbit #1388: ``extract_masks_stage`` and ``eye_keypoints_stage``
    gate purely on ``abort.is_set()``. Pre-fix, classify latched
    ``source_offline["reason"]`` but never set ``abort``, so both stages
    walked every detected photo and re-opened the same dead share —
    reproducing the exact "N failed" pattern this PR is meant to fix, one
    stage later. Set ``abort`` on the give-up path so extract_masks (and
    eye_keypoints) can't hammer the dead source.
    """
    import classifier as classifier_mod
    import classify_job
    import config as cfg
    import pipeline_job as pj
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)
    photo_ids = []
    for i in range(3):
        name = f"photo{i}.jpg"
        pid = db.add_photo(folder_id, name, ".jpg", 4000 + i, 4_000_000.0 + i)
        _drop_jpeg(folder_path, name)
        db.save_detections(
            pid,
            [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
              "confidence": 0.9, "category": "animal"}],
            detector_model="MegaDetector",
        )
        photo_ids.append(pid)

    col_id = db.add_collection(
        "Test",
        json.dumps([{"field": "photo_ids", "value": photo_ids}]),
    )

    _setup_fake_downloaded_model(tmp_path, monkeypatch)

    import labels_fingerprint as lfp
    monkeypatch.setattr(lfp, "compute_fingerprint", lambda *a, **k: "fp")

    def fake_detect_batch(batch, folders, runner, job, reclassify, db_,
                          det_conf_threshold=None, already_detected_ids=None,
                          cached_detections=None):
        det_map = {}
        for p in batch:
            det_map[p["id"]] = [{
                "id": d["id"],
                "box_x": d["box_x"], "box_y": d["box_y"],
                "box_w": d["box_w"], "box_h": d["box_h"],
                "confidence": d["detector_confidence"],
                "category": d["category"],
            } for d in db_.get_detections(p["id"])
                if d["detector_model"] != "full-image"]
        return det_map, len(batch), {p["id"] for p in batch}

    monkeypatch.setattr(classify_job, "_detect_batch", fake_detect_batch)

    # The share is gone: every load fails.
    gone_folder = "/Volumes/DefinitelyNotMounted12345/Raw Files"

    def offline_prepare_image(photo, folders, detection, vireo_dir=None):
        return None, gone_folder, os.path.join(gone_folder, photo["filename"])

    monkeypatch.setattr(classify_job, "_prepare_image", offline_prepare_image)

    class FakeClassifier:
        def __init__(self, *args, **kwargs):
            pass

        def encode_image(self, *args, **kwargs):
            import numpy as np
            return np.zeros(512, dtype=np.float32)

    monkeypatch.setattr(classifier_mod, "Classifier", FakeClassifier)

    # Spy on the exact function extract_masks_stage would call to re-open
    # source images (masking.render_proxy). If classify's give-up path
    # forgets to set abort, extract_masks runs its body and this spy fires;
    # with the fix it never runs because the stage short-circuits on abort.
    #
    # Patched by attribute — extract_masks_stage does `from masking import
    # (..., render_proxy, ...)` at call time, so setting the attribute on
    # the module before the pipeline runs is what the imported binding
    # sees. The patch is a defensive belt-and-suspenders around the
    # status-Skipped assertion below.
    render_proxy_calls: list = []

    def spy_render_proxy(*args, **kwargs):
        render_proxy_calls.append(args)
        return None

    # Patch unconditionally — a suppressed patch would leave
    # ``render_proxy_calls`` permanently empty, letting the ``assert not
    # render_proxy_calls`` guard below pass vacuously if extract_masks
    # regressed (CodeRabbit nit #1388).
    import masking as masking_mod
    monkeypatch.setattr(masking_mod, "render_proxy", spy_render_proxy)

    # A plain FakeRunner has no pause support, so classify gives up at once —
    # the same path a non-pausable job takes when the source is offline.
    # Crucially, do NOT set skip_extract_masks — the whole point of this test
    # is that extract_masks would otherwise still run.
    params = PipelineParams(
        collection_id=col_id,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()

    with pytest.raises(RuntimeError):
        run_pipeline_job(job, runner, db_path, ws_id, params)

    # Every downstream stage step must have reached a terminal Skipped state
    # so the jobs tree doesn't leave rows dangling as pending, AND the stage
    # must have short-circuited (not fallen through to the "running" summary
    # that its real body writes after entering the try block).
    extract_masks_updates = [
        kwargs for (_jid, sid, kwargs) in runner.step_updates
        if sid == "extract_masks"
    ]
    assert any(
        kwargs.get("summary") == "Skipped"
        for kwargs in extract_masks_updates
    ), (
        f"extract_masks must mark itself Skipped after classify gives up on "
        f"a dead source; without the fix, abort stays clear and this stage "
        f"walks every detected photo re-opening the offline share. "
        f"Got updates {extract_masks_updates!r}"
    )
    eye_keypoints_updates = [
        kwargs for (_jid, sid, kwargs) in runner.step_updates
        if sid == "eye_keypoints"
    ]
    assert any(
        kwargs.get("summary") == "Skipped"
        for kwargs in eye_keypoints_updates
    ), (
        f"eye_keypoints must mark itself Skipped after classify gives up; "
        f"got updates {eye_keypoints_updates!r}"
    )

    # Belt-and-suspenders: extract_masks reaching its inner loop would call
    # masking.render_proxy on every detected photo. If any calls landed,
    # the abort-on-give-up path regressed and downstream stages are once
    # again hammering the dead share.
    assert not render_proxy_calls, (
        f"masking.render_proxy fired {len(render_proxy_calls)} time(s) "
        f"after classify gave up on the offline source — extract_masks "
        f"walked its per-photo loop instead of short-circuiting on abort."
    )

    # Sanity check the constant that gates the give-up path exists so a
    # future rename doesn't silently defeat the test.
    assert hasattr(pj, "_MAX_SOURCE_OFFLINE_PAUSES")


def test_pipeline_classify_reclassify_preserves_predictions_for_unreachable_photos(
    tmp_path, monkeypatch,
):
    """Reclassify must not wipe predictions for photos the run never opened.

    Codex #1388 P1 (r3663159360): with ``reclassify=True``, the folder-scoped
    branch increments ``source_skipped`` and continues without setting
    ``abort``. Finalization then reaches the collection-wide
    ``clear_predictions(collection_photo_ids=[p["id"] for p in photos])`` and
    deletes the existing predictions for photos in the missing folder — even
    though the run had no chance to write a replacement. The user loses
    their prior labels for photos we didn't even open.

    Scope the clear to photos this spec actually reached AND wasn't source-
    skipped, so an unreached photo keeps its prior prediction.
    """
    import classifier as classifier_mod
    import classify_job
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    gone_folder_path = str(tmp_path / "vanished")
    healthy_folder_path = str(tmp_path / "still_here")
    os.makedirs(gone_folder_path, exist_ok=True)
    os.makedirs(healthy_folder_path, exist_ok=True)

    gone_folder_id = db.add_folder(gone_folder_path)
    healthy_folder_id = db.add_folder(healthy_folder_path)

    photo_ids = []
    gone_photo_ids: set = set()
    healthy_photo_ids = []
    gone_detection_ids = []
    for i in range(3):
        name = f"gone{i}.jpg"
        pid = db.add_photo(
            gone_folder_id, name, ".jpg", 4000 + i, 4_000_000.0 + i,
        )
        _drop_jpeg(gone_folder_path, name)
        det_ids = db.save_detections(
            pid,
            [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
              "confidence": 0.9, "category": "animal"}],
            detector_model="MegaDetector",
        )
        photo_ids.append(pid)
        gone_photo_ids.add(pid)
        gone_detection_ids.append(det_ids[0])
    for i in range(3):
        name = f"healthy{i}.jpg"
        pid = db.add_photo(
            healthy_folder_id, name, ".jpg", 5000 + i, 5_000_000.0 + i,
        )
        _drop_jpeg(healthy_folder_path, name)
        db.save_detections(
            pid,
            [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
              "confidence": 0.9, "category": "animal"}],
            detector_model="MegaDetector",
        )
        photo_ids.append(pid)
        healthy_photo_ids.append(pid)

    col_id = db.add_collection(
        "Test",
        json.dumps([{"field": "photo_ids", "value": photo_ids}]),
    )

    _setup_fake_downloaded_model(tmp_path, monkeypatch)

    import labels_fingerprint as lfp
    monkeypatch.setattr(lfp, "compute_fingerprint", lambda *a, **k: "fp")

    # Seed a prior prediction for every photo in the unreachable folder under
    # the same (model, labels_fingerprint) the reclassify run will target.
    # These are exactly the rows the buggy clear used to wipe out even
    # though the run couldn't rewrite them.
    prior_species = "PriorSpecies"
    for det_id in gone_detection_ids:
        db.add_prediction(
            detection_id=det_id,
            species=prior_species,
            confidence=0.42,
            model="clip",
            labels_fingerprint="fp",
        )

    def fake_detect_batch(batch, folders, runner, job, reclassify, db_,
                          det_conf_threshold=None, already_detected_ids=None,
                          cached_detections=None):
        det_map = {}
        for p in batch:
            det_map[p["id"]] = [{
                "id": d["id"],
                "box_x": d["box_x"], "box_y": d["box_y"],
                "box_w": d["box_w"], "box_h": d["box_h"],
                "confidence": d["detector_confidence"],
                "category": d["category"],
            } for d in db_.get_detections(p["id"])
                if d["detector_model"] != "full-image"]
        return det_map, len(batch), {p["id"] for p in batch}

    monkeypatch.setattr(classify_job, "_detect_batch", fake_detect_batch)

    from PIL import Image as _PILImage

    fake_missing_root = str(tmp_path / "vanished_at_classify_time")

    def selective_prepare_image(photo, folders, detection, vireo_dir=None):
        if photo["id"] in gone_photo_ids:
            image_path = os.path.join(fake_missing_root, photo["filename"])
            return None, fake_missing_root, image_path
        fp = folders.get(photo["folder_id"], "")
        return (
            _PILImage.new("RGB", (16, 16), "black"),
            fp,
            os.path.join(fp, photo["filename"]),
        )

    monkeypatch.setattr(classify_job, "_prepare_image", selective_prepare_image)

    def fake_flush_batch(batch, clf, model_type, model_name, db_, raw_results,
                         top_k=1):
        for entry in batch:
            raw_results.append({
                "photo": entry["photo"],
                "detection_id": entry.get("detection_id"),
                "folder_path": entry["folder_path"],
                "image_path": entry["image_path"],
                "prediction": "FreshSpecies",
                "confidence": 0.9,
                "timestamp": None,
                "filename": entry["photo"]["filename"],
                "embedding": None,
                "taxonomy": None,
            })
        return 0

    monkeypatch.setattr(classify_job, "_flush_batch", fake_flush_batch)

    class FakeClassifier:
        def __init__(self, *args, **kwargs):
            pass

        def encode_image(self, *args, **kwargs):
            import numpy as np
            return np.zeros(512, dtype=np.float32)

    monkeypatch.setattr(classifier_mod, "Classifier", FakeClassifier)

    class NoPauseRunner(FakeRunner):
        def pause_job(self, job_id):
            return False

        def wait_if_paused(self, job_id, *, publish_paused=False):
            return False

    params = PipelineParams(
        collection_id=col_id,
        skip_extract_masks=True,
        skip_regroup=True,
        reclassify=True,
    )

    runner = NoPauseRunner()
    job = _make_job()

    # The run finishes failed (the gone folder was never opened), but the
    # unreachable photos' prior predictions must still be there afterwards.
    with pytest.raises(RuntimeError):
        run_pipeline_job(job, runner, db_path, ws_id, params)

    surviving = db.conn.execute(
        f"SELECT COUNT(*) FROM predictions p "
        f"JOIN detections d ON d.id = p.detection_id "
        f"WHERE d.photo_id IN "
        f"({','.join('?' * len(gone_photo_ids))}) "
        f"AND p.species = ?",
        list(gone_photo_ids) + [prior_species],
    ).fetchone()[0]
    assert surviving == len(gone_detection_ids), (
        f"reclassify with a folder-scoped outage must leave the prior "
        f"predictions for unreachable photos in place — the run had no "
        f"chance to rewrite them. Expected {len(gone_detection_ids)} "
        f"surviving {prior_species!r} rows, got {surviving}."
    )

    # And the healthy folder's photos should have been (re)classified with
    # the fresh prediction — the fix must not regress the happy path.
    fresh_stored = db.conn.execute(
        f"SELECT COUNT(DISTINCT d.photo_id) FROM predictions p "
        f"JOIN detections d ON d.id = p.detection_id "
        f"WHERE d.photo_id IN "
        f"({','.join('?' * len(healthy_photo_ids))}) "
        f"AND p.species = 'FreshSpecies'",
        list(healthy_photo_ids),
    ).fetchone()[0]
    assert fresh_stored == len(healthy_photo_ids), (
        f"Healthy folder must still be classified after the fix; "
        f"expected {len(healthy_photo_ids)} FreshSpecies rows, "
        f"got {fresh_stored}."
    )


def test_pipeline_classify_multimodel_reclassify_per_spec_source_skips(
    tmp_path, monkeypatch,
):
    """Multi-model reclassify must clear per-spec, not per-run, source skips.

    Codex #1388 P2 (r3663642360): ``source_skipped_photo_ids`` used to
    accumulate across every spec. If a photo was unreachable for model A
    but the folder was back before model B, model B's reclassify clear
    still excluded the photo — because the aggregate set said "leave it
    alone". ``Database.add_prediction`` uses ``INSERT OR IGNORE``, so
    model B's fresh result then couldn't overwrite the stale prior row,
    and the photo retained a wrong species/confidence for model B
    forever. Pin the fix: model B's clear must cover the photo, so its
    fresh prediction wins.
    """
    import classifier as classifier_mod
    import classify_job
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    gone_folder_path = str(tmp_path / "gone_for_model_a")
    healthy_folder_path = str(tmp_path / "always_here")
    os.makedirs(gone_folder_path, exist_ok=True)
    os.makedirs(healthy_folder_path, exist_ok=True)

    gone_folder_id = db.add_folder(gone_folder_path)
    healthy_folder_id = db.add_folder(healthy_folder_path)

    gone_photo_id = db.add_photo(
        gone_folder_id, "gone.jpg", ".jpg", 4000, 4_000_000.0,
    )
    _drop_jpeg(gone_folder_path, "gone.jpg")
    gone_det_id = db.save_detections(
        gone_photo_id,
        [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
          "confidence": 0.9, "category": "animal"}],
        detector_model="MegaDetector",
    )[0]

    healthy_photo_id = db.add_photo(
        healthy_folder_id, "healthy.jpg", ".jpg", 5000, 5_000_000.0,
    )
    _drop_jpeg(healthy_folder_path, "healthy.jpg")
    db.save_detections(
        healthy_photo_id,
        [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
          "confidence": 0.9, "category": "animal"}],
        detector_model="MegaDetector",
    )

    col_id = db.add_collection(
        "Test",
        json.dumps([{"field": "photo_ids",
                     "value": [gone_photo_id, healthy_photo_id]}]),
    )

    model_ids = _setup_two_fake_downloaded_models(tmp_path, monkeypatch)
    # ``_setup_two_fake_downloaded_models`` only installs the model files;
    # verify_if_needed still fires for bioclip-2 during
    # ``_load_model_bundle`` and would try to reach HuggingFace. Stub it
    # so the second model actually loads under the test's isolated HOME.
    import model_verify
    monkeypatch.setattr(
        model_verify,
        "verify_if_needed",
        lambda model_id, model_dir, hf_subdir, optional_files=None: None,
    )

    import labels_fingerprint as lfp
    monkeypatch.setattr(lfp, "compute_fingerprint", lambda *a, **k: "fp")

    # Pre-seed the stale model-B prediction on the gone photo. This is the
    # row the pre-fix code left in place because the aggregate skipped set
    # still contained gone_photo_id when model B's clear ran — the fresh
    # prediction then couldn't overwrite it via INSERT OR IGNORE, and the
    # user was stuck with OldModelBSpecies forever.
    OLD_B = "OldModelBSpecies"
    db.add_prediction(
        detection_id=gone_det_id,
        species=OLD_B,
        confidence=0.42,
        model="BioCLIP-2",
        labels_fingerprint="fp",
    )

    def fake_detect_batch(batch, folders, runner, job, reclassify, db_,
                          det_conf_threshold=None, already_detected_ids=None,
                          cached_detections=None):
        det_map = {}
        for p in batch:
            det_map[p["id"]] = [{
                "id": d["id"],
                "box_x": d["box_x"], "box_y": d["box_y"],
                "box_w": d["box_w"], "box_h": d["box_h"],
                "confidence": d["detector_confidence"],
                "category": d["category"],
            } for d in db_.get_detections(p["id"])
                if d["detector_model"] != "full-image"]
        return det_map, len(batch), {p["id"] for p in batch}

    monkeypatch.setattr(classify_job, "_detect_batch", fake_detect_batch)

    from PIL import Image as _PILImage

    # Track which model's classifier was most recently constructed. Model
    # A (bioclip-vit-b-16) is preloaded by model_loader_stage; model B
    # (bioclip-2) is constructed by ``_load_model_bundle`` when
    # classify_stage moves to spec_idx==1. That construction is our
    # only in-test signal that we've crossed the spec boundary, so we
    # key the folder-availability behavior off it.
    current_model = ["A"]
    fake_missing_root = str(tmp_path / "vanished_at_classify_time")

    def selective_prepare_image(photo, folders, detection, vireo_dir=None):
        # gone_photo_id is unreachable ONLY during model A. When we're
        # on model B, its folder is back — matching the P2 scenario where
        # a transient outage clears before the next spec starts.
        if (
            photo["id"] == gone_photo_id
            and current_model[0] == "A"
        ):
            image_path = os.path.join(fake_missing_root, photo["filename"])
            return None, fake_missing_root, image_path
        fp = folders.get(photo["folder_id"], "")
        return (
            _PILImage.new("RGB", (16, 16), "black"),
            fp,
            os.path.join(fp, photo["filename"]),
        )

    monkeypatch.setattr(classify_job, "_prepare_image", selective_prepare_image)

    def fake_flush_batch(batch, clf, model_type, model_name, db_, raw_results,
                         top_k=1):
        # Fresh predictions carry the model in the species string so the
        # per-model assertions below can pull them apart.
        for entry in batch:
            raw_results.append({
                "photo": entry["photo"],
                "detection_id": entry.get("detection_id"),
                "folder_path": entry["folder_path"],
                "image_path": entry["image_path"],
                "prediction": f"Fresh_{model_name}",
                "confidence": 0.9,
                "timestamp": None,
                "filename": entry["photo"]["filename"],
                "embedding": None,
                "taxonomy": None,
            })
        return 0

    monkeypatch.setattr(classify_job, "_flush_batch", fake_flush_batch)

    class FakeClassifier:
        def __init__(self, *args, **kwargs):
            pretrained = str(kwargs.get("pretrained_str") or "")
            if model_ids[1] in pretrained:
                current_model[0] = "B"
            else:
                current_model[0] = "A"

        def encode_image(self, *args, **kwargs):
            import numpy as np
            return np.zeros(512, dtype=np.float32)

    monkeypatch.setattr(classifier_mod, "Classifier", FakeClassifier)

    # NoPauseRunner keeps model A on the folder-scoped path instead of
    # falling into pause+cancel: an outage that only affects one folder
    # is exactly the scenario we're pinning.
    class NoPauseRunner(FakeRunner):
        def pause_job(self, job_id):
            return False

        def wait_if_paused(self, job_id, *, publish_paused=False):
            return False

    params = PipelineParams(
        collection_id=col_id,
        model_ids=model_ids,
        reclassify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = NoPauseRunner()
    job = _make_job()

    # The run does NOT raise here: model B reaches every photo, so
    # ``models_succeeded`` is 1 and the earlier folder-scoped skip on
    # model A produces an incomplete rollup but no fatal fatal-source
    # error — the rollup for a partial multi-model outage returns
    # normally with a "stopped after" summary rather than raising.
    with contextlib.suppress(RuntimeError):
        run_pipeline_job(job, runner, db_path, ws_id, params)

    # The heart of the P2 fix: model B's fresh prediction for the
    # previously-skipped photo must win. Pre-fix, model B's clear
    # excluded gone_photo_id (still in the aggregate skipped set from
    # model A) — so INSERT OR IGNORE left the stale OldModelBSpecies in
    # place, and the fresh row was silently dropped.
    stored = db.conn.execute(
        "SELECT species FROM predictions "
        "WHERE detection_id = ? AND classifier_model = ?",
        (gone_det_id, "BioCLIP-2"),
    ).fetchall()
    species = sorted(row[0] for row in stored)
    assert "Fresh_BioCLIP-2" in species, (
        f"Model B's clear must cover a photo model A skipped so the "
        f"fresh reclassify result can overwrite the stale prior row "
        f"(add_prediction is INSERT OR IGNORE). Got species {species!r}."
    )
    assert OLD_B not in species, (
        f"The stale model-B prediction for the previously-skipped photo "
        f"must be cleared before storing the fresh result — otherwise "
        f"INSERT OR IGNORE leaves the wrong species in place. Got "
        f"species {species!r}."
    )


def test_pipeline_classify_stale_purge_preserves_source_skipped_photos(
    tmp_path, monkeypatch,
):
    """The reclassify stale-detection purge must not touch source-skipped photos.

    Codex #1388 P1 (r3663922709): on a reclassify run where detection
    succeeded but the source disappeared before classification, a photo
    ends up in ``source_skipped_photo_ids`` AND — because
    ``first_model_photo_ids`` is added to at the top of the per-photo
    body, before the image read — remains in ``first_model_photo_ids``
    too. The purge at ``pipeline_job.py`` lines 5015-5066 therefore
    included that photo, and if the fresh detect returned different boxes
    than the pre-run snapshot the pre-run detection ids for the skipped
    photo were deleted — cascading through their prior predictions
    despite the new ``clear_predictions`` exclusion that already spared
    them.
    """
    import classifier as classifier_mod
    import classify_job
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    gone_folder_path = str(tmp_path / "vanishes_at_classify")
    healthy_folder_path = str(tmp_path / "always_here")
    os.makedirs(gone_folder_path, exist_ok=True)
    os.makedirs(healthy_folder_path, exist_ok=True)

    gone_folder_id = db.add_folder(gone_folder_path)
    healthy_folder_id = db.add_folder(healthy_folder_path)

    # Pre-existing detection: THIS is the row (and its cascaded
    # prediction) the fix must preserve. Its box is deliberately
    # different from what fake_detect_batch will return below so that
    # the pre-run id is NOT re-produced by the fresh detect — the
    # exact precondition that triggers the purge.
    gone_photo_id = db.add_photo(
        gone_folder_id, "gone.jpg", ".jpg", 4000, 4_000_000.0,
    )
    _drop_jpeg(gone_folder_path, "gone.jpg")
    old_gone_det_id = db.save_detections(
        gone_photo_id,
        [{"box": {"x": 0.05, "y": 0.05, "w": 0.30, "h": 0.30},
          "confidence": 0.9, "category": "animal"}],
        detector_model="MegaDetector",
    )[0]
    OLD_SPECIES = "PreservedPriorSpecies"
    db.add_prediction(
        detection_id=old_gone_det_id,
        species=OLD_SPECIES,
        confidence=0.42,
        model="BioCLIP-2",
        labels_fingerprint="fp",
    )

    # Healthy photo is here to drive ``models_succeeded == 1`` so the
    # purge fires. Its pre-run boxes are the same as fresh, so it does
    # not exercise the purge itself.
    healthy_photo_id = db.add_photo(
        healthy_folder_id, "healthy.jpg", ".jpg", 5000, 5_000_000.0,
    )
    _drop_jpeg(healthy_folder_path, "healthy.jpg")
    db.save_detections(
        healthy_photo_id,
        [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
          "confidence": 0.9, "category": "animal"}],
        detector_model="MegaDetector",
    )

    col_id = db.add_collection(
        "Test",
        json.dumps([{"field": "photo_ids",
                     "value": [gone_photo_id, healthy_photo_id]}]),
    )

    _setup_fake_downloaded_model(tmp_path, monkeypatch)

    import labels_fingerprint as lfp
    monkeypatch.setattr(lfp, "compute_fingerprint", lambda *a, **k: "fp")

    # Fresh detect returns a DIFFERENT box for the gone photo so its
    # content-addressed id (vireo/detection_id.py) does NOT match the
    # pre-run id. The pre-run id therefore lands in the purge's
    # stale-id list — unless the fix excludes source-skipped photos
    # from the purge scope, which is what this test pins.
    from detection_id import detection_id as compute_det_id
    NEW_BOX = (0.75, 0.75, 0.20, 0.20)
    new_gone_det_id = compute_det_id(
        gone_photo_id, "megadetector-v6", NEW_BOX, "animal",
    )
    assert new_gone_det_id != old_gone_det_id, (
        "Test setup precondition: fresh box must produce a different "
        "detection id than the pre-run box, otherwise the purge would "
        "have nothing to consider stale for this photo."
    )

    def fake_detect_batch(batch, folders, runner, job, reclassify, db_,
                          det_conf_threshold=None, already_detected_ids=None,
                          cached_detections=None):
        det_map = {}
        for p in batch:
            if p["id"] == gone_photo_id:
                det_map[p["id"]] = [{
                    "id": new_gone_det_id,
                    "box_x": NEW_BOX[0], "box_y": NEW_BOX[1],
                    "box_w": NEW_BOX[2], "box_h": NEW_BOX[3],
                    "confidence": 0.9, "category": "animal",
                }]
            else:
                det_map[p["id"]] = [{
                    "id": d["id"],
                    "box_x": d["box_x"], "box_y": d["box_y"],
                    "box_w": d["box_w"], "box_h": d["box_h"],
                    "confidence": d["detector_confidence"],
                    "category": d["category"],
                } for d in db_.get_detections(p["id"])
                    if d["detector_model"] != "full-image"]
        return det_map, len(batch), {p["id"] for p in batch}

    monkeypatch.setattr(classify_job, "_detect_batch", fake_detect_batch)

    from PIL import Image as _PILImage

    def selective_prepare_image(photo, folders, detection, vireo_dir=None):
        # The gone photo's folder vanished between detect and classify.
        if photo["id"] == gone_photo_id:
            image_path = os.path.join(
                str(tmp_path / "vanished_at_classify_time"),
                photo["filename"],
            )
            return None, str(tmp_path / "vanished_at_classify_time"), image_path
        fp = folders.get(photo["folder_id"], "")
        return (
            _PILImage.new("RGB", (16, 16), "black"),
            fp,
            os.path.join(fp, photo["filename"]),
        )

    monkeypatch.setattr(classify_job, "_prepare_image", selective_prepare_image)

    def fake_flush_batch(batch, clf, model_type, model_name, db_, raw_results,
                         top_k=1):
        for entry in batch:
            raw_results.append({
                "photo": entry["photo"],
                "detection_id": entry.get("detection_id"),
                "folder_path": entry["folder_path"],
                "image_path": entry["image_path"],
                "prediction": "FreshSpecies",
                "confidence": 0.9,
                "timestamp": None,
                "filename": entry["photo"]["filename"],
                "embedding": None,
                "taxonomy": None,
            })
        return 0

    monkeypatch.setattr(classify_job, "_flush_batch", fake_flush_batch)

    class FakeClassifier:
        def __init__(self, *args, **kwargs):
            pass

        def encode_image(self, *args, **kwargs):
            import numpy as np
            return np.zeros(512, dtype=np.float32)

    monkeypatch.setattr(classifier_mod, "Classifier", FakeClassifier)

    # NoPauseRunner keeps the gone photo on the folder-scoped path so
    # the healthy photo still gets classified (``models_succeeded == 1``
    # is required for the purge to fire).
    class NoPauseRunner(FakeRunner):
        def pause_job(self, job_id):
            return False

        def wait_if_paused(self, job_id, *, publish_paused=False):
            return False

    params = PipelineParams(
        collection_id=col_id,
        reclassify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = NoPauseRunner()
    job = _make_job()

    # A folder-scoped skip on a partial run raises a fatal source-offline
    # error at the end of the run (Codex #1388 P1) — accept the raise so
    # the classify body ran the purge.
    with contextlib.suppress(RuntimeError):
        run_pipeline_job(job, runner, db_path, ws_id, params)

    # The heart of the fix: the pre-run detection row for the
    # source-skipped photo must still exist. Pre-fix, ``first_model_
    # photo_ids`` still contained gone_photo_id (added at the top of the
    # per-photo body before the image read), so the purge treated the
    # pre-run id as stale and deleted it — cascading through the
    # prediction below.
    surviving_det_ids = {
        row[0] for row in db.conn.execute(
            "SELECT id FROM detections WHERE photo_id = ?",
            (gone_photo_id,),
        ).fetchall()
    }
    assert old_gone_det_id in surviving_det_ids, (
        f"The pre-run detection row for a source-skipped photo must "
        f"survive the reclassify stale-detection purge. Got surviving "
        f"ids {surviving_det_ids!r}; expected {old_gone_det_id} to be "
        f"present."
    )

    # And its cascaded prediction must survive too — cascading through
    # the FK delete is exactly what the pre-fix code did.
    surviving_species = sorted(
        row[0] for row in db.conn.execute(
            "SELECT species FROM predictions WHERE detection_id = ?",
            (old_gone_det_id,),
        ).fetchall()
    )
    assert OLD_SPECIES in surviving_species, (
        f"The prior prediction for a source-skipped photo must not be "
        f"cascade-deleted by the stale-detection purge. Got species "
        f"{surviving_species!r}."
    )


def test_pipeline_classify_recovered_pause_leaves_no_terminal_classify_error(
    tmp_path, monkeypatch,
):
    """A successful pause+resume must not leave the run looking failed.

    Codex #1388 P1 (r3663159367): the pause path used to append a
    ``[classify] Source X — paused. Reconnect...`` entry to ``job["errors"]``
    every time it parked. When the user reconnected and the retry
    succeeded, that entry stayed in ``job["errors"]`` even though classify
    completed normally. templates/pipeline.html treats every ``[classify]``
    error as a failed stage — banner shown, success redirect suppressed —
    so the newly supported reconnect-and-resume flow still looked failed to
    the user. After the fix, a run that recovers must not carry a
    ``[classify]`` entry in its terminal errors.
    """
    import classifier as classifier_mod
    import classify_job
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)
    photo_ids = []
    for i in range(6):
        name = f"photo{i}.jpg"
        pid = db.add_photo(folder_id, name, ".jpg", 4000 + i, 4_000_000.0 + i)
        _drop_jpeg(folder_path, name)
        db.save_detections(
            pid,
            [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
              "confidence": 0.9, "category": "animal"}],
            detector_model="MegaDetector",
        )
        photo_ids.append(pid)

    col_id = db.add_collection(
        "Test",
        json.dumps([{"field": "photo_ids", "value": photo_ids}]),
    )

    _setup_fake_downloaded_model(tmp_path, monkeypatch)

    import labels_fingerprint as lfp
    monkeypatch.setattr(lfp, "compute_fingerprint", lambda *a, **k: "fp")

    def fake_detect_batch(batch, folders, runner, job, reclassify, db_,
                          det_conf_threshold=None, already_detected_ids=None,
                          cached_detections=None):
        det_map = {}
        for p in batch:
            det_map[p["id"]] = [{
                "id": d["id"],
                "box_x": d["box_x"], "box_y": d["box_y"],
                "box_w": d["box_w"], "box_h": d["box_h"],
                "confidence": d["detector_confidence"],
                "category": d["category"],
            } for d in db_.get_detections(p["id"])
                if d["detector_model"] != "full-image"]
        return det_map, len(batch), {p["id"] for p in batch}

    monkeypatch.setattr(classify_job, "_detect_batch", fake_detect_batch)

    from PIL import Image as _PILImage

    gone_folder = "/Volumes/DefinitelyNotMounted12345/Raw Files"
    state = {"mounted": True, "seen": 0}

    def flaky_prepare_image(photo, folders, detection, vireo_dir=None):
        state["seen"] += 1
        if state["seen"] == 2:
            state["mounted"] = False
        if not state["mounted"]:
            return None, gone_folder, os.path.join(
                gone_folder, photo["filename"],
            )
        fp = folders.get(photo["folder_id"], "")
        return (
            _PILImage.new("RGB", (16, 16), "black"),
            fp,
            os.path.join(fp, photo["filename"]),
        )

    monkeypatch.setattr(classify_job, "_prepare_image", flaky_prepare_image)

    def fake_flush_batch(batch, clf, model_type, model_name, db_, raw_results,
                         top_k=1):
        for entry in batch:
            raw_results.append({
                "photo": entry["photo"],
                "detection_id": entry.get("detection_id"),
                "folder_path": entry["folder_path"],
                "image_path": entry["image_path"],
                "prediction": "Robin",
                "confidence": 0.9,
                "timestamp": None,
                "filename": entry["photo"]["filename"],
                "embedding": None,
                "taxonomy": None,
            })
        return 0

    monkeypatch.setattr(classify_job, "_flush_batch", fake_flush_batch)

    class FakeClassifier:
        def __init__(self, *args, **kwargs):
            pass

        def encode_image(self, *args, **kwargs):
            import numpy as np
            return np.zeros(512, dtype=np.float32)

    monkeypatch.setattr(classifier_mod, "Classifier", FakeClassifier)

    class ReconnectingRunner(FakeRunner):
        def __init__(self):
            super().__init__()
            self.pause_calls = []
            self._paused = False

        def pause_job(self, job_id):
            self.pause_calls.append(job_id)
            self._paused = True
            return True

        def pause_requested(self, job_id):
            return self._paused

        def mark_paused(self, job_id):
            return True

        def wait_if_paused(self, job_id, *, publish_paused=False):
            state["mounted"] = True   # user reconnects the volume
            self._paused = False      # ...and hits Resume
            return False

    params = PipelineParams(
        collection_id=col_id,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = ReconnectingRunner()
    job = _make_job()

    run_pipeline_job(job, runner, db_path, ws_id, params)

    # The share dropped and came back — we must have paused at least once so
    # this test is exercising the recovery path, not a happy-path run.
    assert runner.pause_calls, (
        "The share dropped mid-run; classify must have paused for the "
        "recovery path to be under test at all. If pause never fired the "
        "test setup regressed, not the code."
    )

    # And the recovery must have actually worked — every photo classified.
    stored = db.conn.execute(
        "SELECT COUNT(DISTINCT d.photo_id) FROM predictions p "
        "JOIN detections d ON d.id = p.detection_id",
    ).fetchone()[0]
    assert stored == 6, (
        f"Setup sanity: reconnect+resume must classify every photo for the "
        f"pause-error assertion below to be meaningful; only {stored} were "
        f"classified."
    )

    # The heart of the fix: nothing about the transient pause may survive
    # into the terminal errors list. Any ``[classify]`` entry here trips
    # pipeline.html's failure banner and blocks the success redirect,
    # making the recovered run look failed to the user.
    classify_errors = [
        e for e in (job.get("errors") or [])
        if isinstance(e, str) and e.startswith("[classify]")
    ]
    assert not classify_errors, (
        f"A recovered pause must not leave a [classify] entry in the "
        f"terminal errors list — pipeline.html would surface it as a "
        f"failed stage even though classify finished successfully. Got "
        f"{classify_errors!r}"
    )


def test_pipeline_classify_failed_retry_on_last_photo_latches_source_offline(
    tmp_path, monkeypatch,
):
    """A failed resume-retry on the last photo must still latch the outage.

    Codex #1388 P1 (r3663278142): when the source stays offline through the
    pause/resume cycle AND this is the final photo/detection that needs an
    image read, the pre-fix retry-failed branch fell through with a silent
    ``source_skipped += 1; continue`` — the classify loop then exited
    normally with ``source_offline["reason"]`` unset and ``abort`` still
    clear. The finalization rollup only sets ``abort`` when
    ``source_offline["reason"]`` is truthy, so ``extract_masks_stage`` and
    ``eye_keypoints_stage`` walked every detected photo and reissued reads
    against the dead share — reproducing the exact "N failed" pattern this
    PR is meant to fix, one stage later.

    Reproduce by giving the run a SINGLE photo (so no subsequent photo can
    re-trigger ``_handle_source_offline``) with a mount that stays dead
    through the resume. The while-loop fix keeps invoking
    ``_handle_source_offline`` on retry failure until the pause budget is
    spent, at which point it latches ``source_offline["reason"]`` and
    downstream stages short-circuit.
    """
    import classifier as classifier_mod
    import classify_job
    import config as cfg
    import pipeline_job as pj
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)
    name = "photo0.jpg"
    pid = db.add_photo(folder_id, name, ".jpg", 4000, 4_000_000.0)
    _drop_jpeg(folder_path, name)
    db.save_detections(
        pid,
        [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
          "confidence": 0.9, "category": "animal"}],
        detector_model="MegaDetector",
    )
    photo_ids = [pid]

    col_id = db.add_collection(
        "Test",
        json.dumps([{"field": "photo_ids", "value": photo_ids}]),
    )

    _setup_fake_downloaded_model(tmp_path, monkeypatch)

    import labels_fingerprint as lfp
    monkeypatch.setattr(lfp, "compute_fingerprint", lambda *a, **k: "fp")

    def fake_detect_batch(batch, folders, runner, job, reclassify, db_,
                          det_conf_threshold=None, already_detected_ids=None,
                          cached_detections=None):
        det_map = {}
        for p in batch:
            det_map[p["id"]] = [{
                "id": d["id"],
                "box_x": d["box_x"], "box_y": d["box_y"],
                "box_w": d["box_w"], "box_h": d["box_h"],
                "confidence": d["detector_confidence"],
                "category": d["category"],
            } for d in db_.get_detections(p["id"])
                if d["detector_model"] != "full-image"]
        return det_map, len(batch), {p["id"] for p in batch}

    monkeypatch.setattr(classify_job, "_detect_batch", fake_detect_batch)

    # The mount is dead and the user's premature resumes don't fix it, so
    # every read — initial AND every retry — fails.
    gone_folder = "/Volumes/DefinitelyNotMounted12345/Raw Files"
    prepare_calls: list = []

    def offline_prepare_image(photo, folders, detection, vireo_dir=None):
        prepare_calls.append(photo["id"])
        return None, gone_folder, os.path.join(gone_folder, photo["filename"])

    monkeypatch.setattr(classify_job, "_prepare_image", offline_prepare_image)

    class FakeClassifier:
        def __init__(self, *args, **kwargs):
            pass

        def encode_image(self, *args, **kwargs):
            import numpy as np
            return np.zeros(512, dtype=np.float32)

    monkeypatch.setattr(classifier_mod, "Classifier", FakeClassifier)

    # Spy on masking.render_proxy — the exact call extract_masks_stage
    # makes to re-open source images. If the fix regresses, abort stays
    # clear on the last-photo path and extract_masks walks its per-photo
    # loop hitting this spy.
    render_proxy_calls: list = []

    def spy_render_proxy(*args, **kwargs):
        render_proxy_calls.append(args)
        return None

    # Patch unconditionally — a suppressed patch would leave
    # ``render_proxy_calls`` permanently empty, letting the ``assert not
    # render_proxy_calls`` guard below pass vacuously if the
    # last-photo retry-fail path regressed (CodeRabbit nit #1388).
    import masking as masking_mod
    monkeypatch.setattr(masking_mod, "render_proxy", spy_render_proxy)

    class ResumingRunner(FakeRunner):
        """User keeps hitting Resume without actually remounting the share."""

        def __init__(self):
            super().__init__()
            self.pause_calls = []
            self._paused = False

        def pause_job(self, job_id):
            self.pause_calls.append(job_id)
            self._paused = True
            return True

        def pause_requested(self, job_id):
            return self._paused

        def mark_paused(self, job_id):
            return True

        def wait_if_paused(self, job_id, *, publish_paused=False):
            self._paused = False  # premature resume
            return False

    # Don't set skip_extract_masks — the whole point is that extract_masks
    # would otherwise re-open the same dead share.
    params = PipelineParams(
        collection_id=col_id,
        skip_regroup=True,
    )

    runner = ResumingRunner()
    job = _make_job()

    with pytest.raises(RuntimeError) as excinfo:
        run_pipeline_job(job, runner, db_path, ws_id, params)

    # The bounded pause loop must have fired — otherwise this test would
    # be testing the initial-failure path, not the resume-retry path we
    # care about.
    assert runner.pause_calls, (
        "Classify must have paused at least once for the resume-retry "
        "path to be under test at all; if pause never fired the setup "
        "regressed, not the code."
    )
    assert len(runner.pause_calls) <= pj._MAX_SOURCE_OFFLINE_PAUSES, (
        f"Classify paused {len(runner.pause_calls)} times for the same "
        f"dead volume on a single photo; the bounded loop must give up "
        f"after {pj._MAX_SOURCE_OFFLINE_PAUSES}."
    )

    # The headline error must name the outage — pre-fix, source_offline
    # ["reason"] stayed unset on this path so the end-of-run rollup fell
    # back to an unrelated error.
    msg = str(excinfo.value)
    assert "/Volumes/DefinitelyNotMounted12345" in msg, (
        f"The job's headline error must name the offline volume; "
        f"got {msg!r}"
    )
    assert "failed to classify" not in msg, (
        f"A dead share must not be reported as photos failing to "
        f"classify; got {msg!r}"
    )

    # The core assertion: extract_masks must have skipped. Pre-fix, the
    # last-photo retry-fail left abort clear, and this stage walked its
    # per-photo loop reissuing reads against the dead mount.
    extract_masks_updates = [
        kwargs for (_jid, sid, kwargs) in runner.step_updates
        if sid == "extract_masks"
    ]
    assert any(
        kwargs.get("summary") == "Skipped"
        for kwargs in extract_masks_updates
    ), (
        f"extract_masks must mark itself Skipped after classify gives up "
        f"on the last-photo retry-fail path; without the fix, abort "
        f"stays clear and this stage walks every detected photo "
        f"re-opening the offline share. Got updates {extract_masks_updates!r}"
    )
    assert not render_proxy_calls, (
        f"masking.render_proxy fired {len(render_proxy_calls)} time(s) "
        f"after classify's last-photo retry-fail — extract_masks walked "
        f"its per-photo loop against the dead share instead of "
        f"short-circuiting on abort (Codex #1388 P1 r3663278142)."
    )


def test_pipeline_classify_folder_outage_skips_unreachable_photos_in_downstream_stages(
    tmp_path, monkeypatch,
):
    """Folder-scoped outages must exclude unreachable photos from downstream stages.

    Codex #1388 P2 (r3664058173): the folder-scoped branch deliberately
    leaves ``abort`` clear so healthy folders keep processing — but the
    unreachable photos still hung off the collection, so
    ``extract_masks_stage`` (and ``eye_keypoints_stage``) would rebuild
    ``photos_to_process`` from the whole collection and call
    ``render_proxy`` against the dead folder for every one of them. Every
    call returned None, they all landed in the "skipped" bucket, and the
    stage summary showed a mask-extraction failure count instead of the
    real diagnosis (missing folder). Filter the classify-time source-
    skipped set out of extract_masks / eye_keypoints so the missing folder
    doesn't get walked twice.
    """
    import classifier as classifier_mod
    import classify_job
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    gone_folder_path = str(tmp_path / "vanished")
    healthy_folder_path = str(tmp_path / "still_here")
    os.makedirs(gone_folder_path, exist_ok=True)
    os.makedirs(healthy_folder_path, exist_ok=True)

    gone_folder_id = db.add_folder(gone_folder_path)
    healthy_folder_id = db.add_folder(healthy_folder_path)

    photo_ids = []
    gone_photo_ids: set = set()
    healthy_photo_ids: set = set()
    for i in range(3):
        name = f"gone{i}.jpg"
        pid = db.add_photo(
            gone_folder_id, name, ".jpg", 4000 + i, 4_000_000.0 + i,
        )
        _drop_jpeg(gone_folder_path, name)
        db.save_detections(
            pid,
            [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
              "confidence": 0.9, "category": "animal"}],
            detector_model="MegaDetector",
        )
        photo_ids.append(pid)
        gone_photo_ids.add(pid)
    for i in range(3):
        name = f"healthy{i}.jpg"
        pid = db.add_photo(
            healthy_folder_id, name, ".jpg", 5000 + i, 5_000_000.0 + i,
        )
        _drop_jpeg(healthy_folder_path, name)
        db.save_detections(
            pid,
            [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
              "confidence": 0.9, "category": "animal"}],
            detector_model="MegaDetector",
        )
        photo_ids.append(pid)
        healthy_photo_ids.add(pid)

    # Drop the DB-recorded folder from disk so downstream stages re-probe
    # (Codex #1388 P2 r3664348758) still see it as offline. Without this,
    # the aggregate skip set would be pruned by the re-probe helper and
    # extract_masks would (correctly) walk the now-reachable folder.
    for entry in os.listdir(gone_folder_path):
        os.remove(os.path.join(gone_folder_path, entry))
    os.rmdir(gone_folder_path)

    col_id = db.add_collection(
        "Test",
        json.dumps([{"field": "photo_ids", "value": photo_ids}]),
    )

    _setup_fake_downloaded_model(tmp_path, monkeypatch)

    import labels_fingerprint as lfp
    monkeypatch.setattr(lfp, "compute_fingerprint", lambda *a, **k: "fp")

    def fake_detect_batch(batch, folders, runner, job, reclassify, db_,
                          det_conf_threshold=None, already_detected_ids=None,
                          cached_detections=None):
        det_map = {}
        for p in batch:
            det_map[p["id"]] = [{
                "id": d["id"],
                "box_x": d["box_x"], "box_y": d["box_y"],
                "box_w": d["box_w"], "box_h": d["box_h"],
                "confidence": d["detector_confidence"],
                "category": d["category"],
            } for d in db_.get_detections(p["id"])
                if d["detector_model"] != "full-image"]
        return det_map, len(batch), {p["id"] for p in batch}

    monkeypatch.setattr(classify_job, "_detect_batch", fake_detect_batch)

    from PIL import Image as _PILImage

    fake_missing_root = str(tmp_path / "vanished_at_classify_time")

    def selective_prepare_image(photo, folders, detection, vireo_dir=None):
        if photo["id"] in gone_photo_ids:
            image_path = os.path.join(fake_missing_root, photo["filename"])
            return None, fake_missing_root, image_path
        fp = folders.get(photo["folder_id"], "")
        return (
            _PILImage.new("RGB", (16, 16), "black"),
            fp,
            os.path.join(fp, photo["filename"]),
        )

    monkeypatch.setattr(classify_job, "_prepare_image", selective_prepare_image)

    def fake_flush_batch(batch, clf, model_type, model_name, db_, raw_results,
                         top_k=1):
        for entry in batch:
            raw_results.append({
                "photo": entry["photo"],
                "detection_id": entry.get("detection_id"),
                "folder_path": entry["folder_path"],
                "image_path": entry["image_path"],
                "prediction": "Robin",
                "confidence": 0.9,
                "timestamp": None,
                "filename": entry["photo"]["filename"],
                "embedding": None,
                "taxonomy": None,
            })
        return 0

    monkeypatch.setattr(classify_job, "_flush_batch", fake_flush_batch)

    class FakeClassifier:
        def __init__(self, *args, **kwargs):
            pass

        def encode_image(self, *args, **kwargs):
            import numpy as np
            return np.zeros(512, dtype=np.float32)

    monkeypatch.setattr(classifier_mod, "Classifier", FakeClassifier)

    # Stub SAM2 / DINOv2 so extract_masks runs end-to-end. Record every
    # render_proxy image_path so the assertion below can distinguish
    # "healthy folder was processed" from "gone folder was retried."
    _stub_extract_masks_heavy_ops(monkeypatch)
    import masking as masking_mod
    render_proxy_paths: list = []
    _original_render_proxy = masking_mod.render_proxy

    def spy_render_proxy(image_path, longest_edge=None):
        render_proxy_paths.append(image_path)
        return _original_render_proxy(image_path, longest_edge=longest_edge)

    monkeypatch.setattr(masking_mod, "render_proxy", spy_render_proxy)

    class NoPauseRunner(FakeRunner):
        def pause_job(self, job_id):
            return False

        def wait_if_paused(self, job_id, *, publish_paused=False):
            return False

    # Do NOT skip_extract_masks — this test is that extract_masks runs on
    # the healthy folder AND skips the gone folder rather than walking it.
    params = PipelineParams(
        collection_id=col_id,
        skip_regroup=True,
        skip_eye_keypoints=True,
    )

    runner = NoPauseRunner()
    job = _make_job()

    with pytest.raises(RuntimeError):
        run_pipeline_job(job, runner, db_path, ws_id, params)

    # extract_masks must have opened ONLY the healthy folder's files.
    # Pre-fix, it also called render_proxy against fake_missing_root for
    # each unreachable photo and reported them as skips.
    assert render_proxy_paths, (
        "extract_masks should have processed the healthy folder — if "
        "no render_proxy calls fired at all, the stage short-circuited "
        "and this test doesn't exercise the filter under review."
    )
    assert not any(
        fake_missing_root in path for path in render_proxy_paths
    ), (
        f"extract_masks re-opened the missing folder via render_proxy — "
        f"the classify-time source-skipped set must be filtered out of "
        f"downstream stages so the folder outage doesn't get walked "
        f"twice (Codex #1388 P2 r3664058173). Got paths: "
        f"{render_proxy_paths!r}"
    )
    # Belt-and-suspenders: every render_proxy path must live under the
    # healthy folder root.
    assert all(
        healthy_folder_path in path for path in render_proxy_paths
    ), (
        f"extract_masks called render_proxy for paths outside the "
        f"healthy folder — expected only {healthy_folder_path!r}, got "
        f"{render_proxy_paths!r}"
    )


def test_pipeline_classify_folder_outage_marks_per_model_step_failed(
    tmp_path, monkeypatch,
):
    """The per-model classify step must land as ``failed`` on a folder outage.

    Codex #1388 P2 (r3664058179): the new source-offline summary described
    the incomplete work in the step's ``summary`` field, but the
    ``runner.update_step`` call still passed ``status="completed"``. The
    Jobs page renders status from ``step.status`` directly and auto-
    collapses ``completed`` rows without warnings, so a failed job would
    show a green, collapsed classifier row — hiding the reason the stage
    stopped short. The later stage-status rollup that flips
    ``stages["classify"]["status"] = "failed"`` isn't mapped back to the
    per-model step, so this must be fixed inline.
    """
    import classifier as classifier_mod
    import classify_job
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)
    photo_ids = []
    for i in range(3):
        name = f"photo{i}.jpg"
        pid = db.add_photo(folder_id, name, ".jpg", 4000 + i, 4_000_000.0 + i)
        _drop_jpeg(folder_path, name)
        db.save_detections(
            pid,
            [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
              "confidence": 0.9, "category": "animal"}],
            detector_model="MegaDetector",
        )
        photo_ids.append(pid)

    col_id = db.add_collection(
        "Test",
        json.dumps([{"field": "photo_ids", "value": photo_ids}]),
    )

    _setup_fake_downloaded_model(tmp_path, monkeypatch)

    import labels_fingerprint as lfp
    monkeypatch.setattr(lfp, "compute_fingerprint", lambda *a, **k: "fp")

    def fake_detect_batch(batch, folders, runner, job, reclassify, db_,
                          det_conf_threshold=None, already_detected_ids=None,
                          cached_detections=None):
        det_map = {}
        for p in batch:
            det_map[p["id"]] = [{
                "id": d["id"],
                "box_x": d["box_x"], "box_y": d["box_y"],
                "box_w": d["box_w"], "box_h": d["box_h"],
                "confidence": d["detector_confidence"],
                "category": d["category"],
            } for d in db_.get_detections(p["id"])
                if d["detector_model"] != "full-image"]
        return det_map, len(batch), {p["id"] for p in batch}

    monkeypatch.setattr(classify_job, "_detect_batch", fake_detect_batch)

    # Every photo lives in a folder that doesn't exist at classify time.
    fake_missing_folder = str(tmp_path / "vanished_folder")

    def folder_missing_prepare_image(photo, folders, detection, vireo_dir=None):
        return None, fake_missing_folder, os.path.join(
            fake_missing_folder, photo["filename"],
        )

    monkeypatch.setattr(
        classify_job, "_prepare_image", folder_missing_prepare_image,
    )

    class FakeClassifier:
        def __init__(self, *args, **kwargs):
            pass

        def encode_image(self, *args, **kwargs):
            import numpy as np
            return np.zeros(512, dtype=np.float32)

    monkeypatch.setattr(classifier_mod, "Classifier", FakeClassifier)

    params = PipelineParams(
        collection_id=col_id,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()

    with pytest.raises(RuntimeError):
        run_pipeline_job(job, runner, db_path, ws_id, params)

    # Every update_step for the classify:<model> row, in order. The final
    # terminal one carries the outage status the Jobs page renders.
    classify_step_updates = [
        (sid, kwargs) for (_jid, sid, kwargs) in runner.step_updates
        if sid.startswith("classify:") and "status" in kwargs
    ]
    assert classify_step_updates, (
        "Setup sanity: no classify:<model> step updates fired at all."
    )
    terminal = classify_step_updates[-1]
    sid, kwargs = terminal
    assert kwargs.get("status") == "failed", (
        f"On a folder-scoped outage the classify:<model> step must land "
        f"as 'failed' — leaving it 'completed' shows a green, collapsed "
        f"row on the Jobs page for a run that never opened the missing "
        f"folder (Codex #1388 P2 r3664058179). Got status="
        f"{kwargs.get('status')!r} on step {sid!r} with summary="
        f"{kwargs.get('summary')!r}."
    )
    # The row must also carry a human-readable error so the collapsed row
    # explains itself.
    assert kwargs.get("error"), (
        f"A failed classify:<model> row must carry a non-empty error "
        f"field naming the outage — that's what the Jobs page shows next "
        f"to the collapsed row. Got kwargs={kwargs!r}"
    )
    assert "unreachable" in kwargs["error"].lower() or (
        "source" in kwargs["error"].lower()
    ), (
        f"The error must name the outage as source-related, not a generic "
        f"failure; got {kwargs['error']!r}"
    )


def test_pipeline_classify_source_offline_give_up_marks_per_model_step_failed(
    tmp_path, monkeypatch,
):
    """A mount give-up must land the responsible classify:<model> step as failed.

    Codex #1388 P2 (r3664058179), mount-outage variant: the model that
    was actively reading when the mount died reaches finalization with
    ``source_offline["reason"]`` latched. Pre-fix, that spec still called
    ``runner.update_step(..., status="completed", ...)``. The Jobs page
    would then show a green, collapsed classifier row for the run that
    gave up — the exact "your photos are fine!" misread this PR fixes.
    """
    import classifier as classifier_mod
    import classify_job
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)
    photo_ids = []
    for i in range(3):
        name = f"photo{i}.jpg"
        pid = db.add_photo(folder_id, name, ".jpg", 4000 + i, 4_000_000.0 + i)
        _drop_jpeg(folder_path, name)
        db.save_detections(
            pid,
            [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
              "confidence": 0.9, "category": "animal"}],
            detector_model="MegaDetector",
        )
        photo_ids.append(pid)

    col_id = db.add_collection(
        "Test",
        json.dumps([{"field": "photo_ids", "value": photo_ids}]),
    )

    _setup_fake_downloaded_model(tmp_path, monkeypatch)

    import labels_fingerprint as lfp
    monkeypatch.setattr(lfp, "compute_fingerprint", lambda *a, **k: "fp")

    def fake_detect_batch(batch, folders, runner, job, reclassify, db_,
                          det_conf_threshold=None, already_detected_ids=None,
                          cached_detections=None):
        det_map = {}
        for p in batch:
            det_map[p["id"]] = [{
                "id": d["id"],
                "box_x": d["box_x"], "box_y": d["box_y"],
                "box_w": d["box_w"], "box_h": d["box_h"],
                "confidence": d["detector_confidence"],
                "category": d["category"],
            } for d in db_.get_detections(p["id"])
                if d["detector_model"] != "full-image"]
        return det_map, len(batch), {p["id"] for p in batch}

    monkeypatch.setattr(classify_job, "_detect_batch", fake_detect_batch)

    # Mount-shaped fake path so the outage escalates to mount-scope
    # (folder scope wouldn't latch source_offline["reason"]).
    gone_mount = "/Volumes/DefinitelyNotMounted12345/Raw Files"

    def offline_prepare_image(photo, folders, detection, vireo_dir=None):
        return None, gone_mount, os.path.join(gone_mount, photo["filename"])

    monkeypatch.setattr(classify_job, "_prepare_image", offline_prepare_image)

    class FakeClassifier:
        def __init__(self, *args, **kwargs):
            pass

        def encode_image(self, *args, **kwargs):
            import numpy as np
            return np.zeros(512, dtype=np.float32)

    monkeypatch.setattr(classifier_mod, "Classifier", FakeClassifier)

    # Plain FakeRunner has no pause support, so classify hits the
    # "can't park → latch source_offline['reason'] and stop" branch.
    params = PipelineParams(
        collection_id=col_id,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()

    with pytest.raises(RuntimeError):
        run_pipeline_job(job, runner, db_path, ws_id, params)

    # The classify:<model> step for the spec that actually gave up must
    # terminate as ``failed`` — the Jobs page renders status directly and
    # a ``completed`` row would collapse silent-green.
    classify_step_updates = [
        (sid, kwargs) for (_jid, sid, kwargs) in runner.step_updates
        if sid.startswith("classify:") and "status" in kwargs
    ]
    assert classify_step_updates, (
        "Setup sanity: no classify:<model> step updates fired at all."
    )
    terminal = classify_step_updates[-1]
    sid, kwargs = terminal
    assert kwargs.get("status") == "failed", (
        f"On a mount give-up the classify:<model> step must land as "
        f"'failed' — 'completed' would leave a green, collapsed row on "
        f"a job that never classified the collection (Codex #1388 P2 "
        f"r3664058179). Got status={kwargs.get('status')!r} with "
        f"summary={kwargs.get('summary')!r}."
    )
    assert kwargs.get("error"), (
        f"The failed row must carry an error field naming the mount "
        f"outage; got kwargs={kwargs!r}"
    )
    assert "source" in kwargs["error"].lower(), (
        f"The error message must name the outage as source-related; "
        f"got {kwargs['error']!r}"
    )


def test_pipeline_extract_masks_offline_survives_finalizer_override(
    tmp_path, monkeypatch,
):
    """A mask-stage-owned outage must stay ``failed`` through finalization.

    Codex #1388 P1 (r3665130244): when classify is fully cached (or skipped)
    but masks are still needed from an offline folder, ``extract_masks_stage``
    latches ``stages["extract_masks"]["status"] = "failed"`` in its
    source-offline branch. But the finalizer at the bottom of the stage
    derives ``final_status`` solely from ``em_failed``: because the offline
    photos were pre-filtered from the worklist, ``em_failed`` is 0, so the
    finalizer flips the stage back to ``"completed"`` — silently erasing
    the outage. The end-of-run rollup only reads stage ``status`` values,
    so the whole job then reports "successfully completed" while the
    ``[extract_masks] Fatal:`` error sits in the errors list with no
    corresponding failed stage to surface it.

    Skip classify entirely to reproduce the fully-cached scenario without
    also having to seed classifier_runs rows: the bug is not about how
    the mask stage learns about the offline source, only about whether
    its ``failed`` verdict survives its own finalizer.
    """
    import shutil

    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)
    photo_ids = []
    for i in range(3):
        name = f"photo{i}.jpg"
        pid = db.add_photo(folder_id, name, ".jpg", 4000 + i, 4_000_000.0 + i)
        _drop_jpeg(folder_path, name)
        db.save_detections(
            pid,
            [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
              "confidence": 0.9, "category": "animal"}],
            detector_model="MegaDetector",
        )
        photo_ids.append(pid)

    col_id = db.add_collection(
        "Test",
        json.dumps([{"field": "photo_ids", "value": photo_ids}]),
    )

    # Delete the folder from disk AFTER seeding the DB. _still_offline_folder_ids_of
    # probes the folder's stored path via os.path.isdir — an absent directory
    # is exactly what a dropped SMB share looks like from userspace.
    shutil.rmtree(folder_path)

    # Skip classify (and thus model_loader). extract_masks_stage runs
    # against the collection photos with no ``source_offline_state``
    # seeded by classify, and must detect the outage on its own via the
    # folder probe — the exact scenario Codex called out for the
    # fully-cached path.
    params = PipelineParams(
        collection_id=col_id,
        skip_classify=True,
        skip_extract_masks=False,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()

    with pytest.raises(RuntimeError) as excinfo:
        run_pipeline_job(job, runner, db_path, ws_id, params)

    # The rollup at the bottom of run_pipeline_job derives failure state
    # ONLY from stage statuses in ``stages`` — it does NOT scan errors
    # for ``[extract_masks] Fatal:`` entries. So this RuntimeError firing
    # at all is the load-bearing assertion: the finalizer preserved
    # ``failed`` rather than overwriting it with ``completed``.
    assert "extract_masks" in str(excinfo.value).lower(), (
        f"The job's headline error must name extract_masks as the failed "
        f"stage; got {excinfo.value!r}"
    )

    # The user-visible step update must also land as ``failed`` — the Jobs
    # page reads status from step updates, and a ``completed`` step here
    # would render a collapsed green row on a run that produced no masks.
    em_step_updates = [
        kwargs for (_jid, sid, kwargs) in runner.step_updates
        if sid == "extract_masks" and "status" in kwargs
    ]
    assert em_step_updates, (
        "Setup sanity: no extract_masks step updates fired at all."
    )
    terminal_em = em_step_updates[-1]
    assert terminal_em.get("status") == "failed", (
        f"The mask-stage terminal step update must be 'failed' — "
        f"'completed' would leave a green, collapsed row on a run that "
        f"never produced masks (Codex #1388 P1 r3665130244). Got "
        f"status={terminal_em.get('status')!r} summary="
        f"{terminal_em.get('summary')!r}"
    )

    # And the Fatal error must be preserved in the job's errors list so
    # the pipeline UI can render the offline diagnostic under the card.
    em_fatal = [
        e for e in (job.get("errors") or [])
        if isinstance(e, str) and e.startswith("[extract_masks] Fatal:")
    ]
    assert em_fatal, (
        f"A source-offline mask stage must record a [extract_masks] "
        f"Fatal: entry so the end-of-run rollup can name it as the "
        f"headline error; got errors={job.get('errors')!r}"
    )
    assert "source offline" in em_fatal[0].lower(), (
        f"The extract_masks Fatal error must name the outage as "
        f"source-offline; got {em_fatal[0]!r}"
    )


# ---------------------------------------------------------------------------
# extract_masks early-return exit state
#
# The stage bails out early when nothing in the worklist carries a qualifying
# detection. That exit used to hard-code `skipped` + a zero total, which
# erased a `failed` status the pre-flight source-offline branch had already
# latched — letting the end-of-run rollup (it reads only stage statuses)
# finish the job green while the dropped photos stayed unmasked and were
# hard-rejected in Process Review (Codex #1392 P1).
# ---------------------------------------------------------------------------


def test_extract_masks_early_exit_preserves_latched_offline_failure():
    from pipeline_job import _extract_masks_early_exit

    status, step_status, _step_extra, payload = _extract_masks_early_exit(
        "no_detections", subthreshold=0, preflight_unreadable=311,
        preflight_masked=4, offline_latched=True,
    )
    assert step_status == "failed", (
        f"An outage exit is a terminal failure on the job tree; got "
        f"{step_status!r}"
    )
    assert status == "failed", (
        "A pre-flight outage already latched a failure and appended a Fatal "
        f"error; the early exit must not downgrade it. Got {status!r}"
    )
    # Reporting 311 unreadable against a hard-coded zero total publishes an
    # impossible tally to anything computing coverage.
    assert payload["unreadable"] == 311
    # Cache hits the pre-flight dropped are still successful outcomes and
    # still part of the stage's coverage.
    assert payload["masked"] == 4
    assert payload["total"] == 315
    assert payload["reason"] == "no_detections"


def test_extract_masks_early_exit_is_a_benign_skip_without_an_outage():
    from pipeline_job import _extract_masks_early_exit

    status, step_status, _step_extra, payload = _extract_masks_early_exit(
        "weights_missing", subthreshold=4, preflight_unreadable=0,
        preflight_masked=0, offline_latched=False,
    )
    assert status == "skipped", (
        f"No outage means the early exit stays a benign skip; got {status!r}"
    )
    # JobRunner.update_step only finalizes completed/failed/cancelled, and
    # the Jobs page only renders summaries for those. Sending "skipped" to
    # the runner leaves a hollow pending-style row with no duration and no
    # explanation of why the stage did nothing (Codex #1392 P2).
    assert step_status == "completed", (
        f"A benign skip still has to close out the runner step; got "
        f"{step_status!r}"
    )
    assert payload["unreadable"] == 0
    assert payload["total"] == 0
    assert payload["subthreshold"] == 4


def test_extract_masks_outage_rollup_counts_only_offline_photos(
    tmp_path, monkeypatch,
):
    """The reconnect message must not claim unrelated bad files.

    A corrupt file in a healthy folder and a photo behind a dead volume both
    land in the unreadable bucket, but only one is fixed by reconnecting the
    source. Attributing the combined count to the single latched outage tells
    the user that plugging the drive back in recovers files it cannot touch
    (Codex #1392 P2).
    """
    import config as cfg
    import pipeline_job as pj
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    # Sorted so the corrupt file in the healthy folder is hit first, which is
    # the ordering that produced the misattribution.
    corrupt_path = str(tmp_path / "aa_healthy")
    share_path = str(tmp_path / "zz_share")
    os.makedirs(corrupt_path, exist_ok=True)
    os.makedirs(share_path, exist_ok=True)
    corrupt_folder = db.add_folder(corrupt_path)
    share_folder = db.add_folder(share_path)

    photo_ids = [
        _add_photo_with_detection(
            db, corrupt_folder, corrupt_path, "aa_bad.jpg",
        ),
        _add_photo_with_detection(db, share_folder, share_path, "zz0.jpg"),
    ]
    collection_id = db.add_collection(
        "One corrupt file, one dead volume",
        json.dumps([{"field": "photo_ids", "value": photo_ids}]),
    )

    state = _stub_extract_masks_heavy_ops(monkeypatch)

    import masking

    monkeypatch.setattr(
        masking, "render_proxy", lambda p, longest_edge=None: None,
    )
    state["proxy_calls"] = 0
    # The healthy folder is readable — that file is simply broken. Only the
    # share is offline.
    monkeypatch.setattr(
        pj, "_source_offline_reason",
        lambda folder_path, image_path: (
            ("mount", "volume /Volumes/Photography is not mounted")
            if image_path.startswith(share_path) else None
        ),
    )

    _runner, result = _run_extract_masks_only(db_path, ws_id, collection_id)

    em = result["stages"]["extract_masks"]
    assert em.get("unreadable") == 2, f"Both photos are unreadable; got {em!r}"

    outage_errors = [
        e for e in result["errors"] if "/Volumes/Photography" in e
    ]
    assert outage_errors, f"Expected an outage rollup; got {result['errors']!r}"
    assert "1 of 2 photos unreachable" in outage_errors[0], (
        f"Only the photo on the dead volume is attributable to the outage; "
        f"got {outage_errors[0]!r}"
    )
    # The corrupt file still has to be reported — just not as an outage.
    assert any(
        "could not be read" in e and "/Volumes/Photography" not in e
        for e in result["errors"]
    ), (
        f"The corrupt file needs its own rollup; got {result['errors']!r}"
    )


def test_extract_masks_preflight_skips_photos_that_already_have_a_mask(
    tmp_path, monkeypatch,
):
    """An offline photo that already carries a mask is not "unreadable".

    The unreadable count exists to explain `no_subject_mask` rejections in
    Process Review. A photo with an active mask for the configured variant
    will not be rejected for that, so counting it makes the stage overstate
    the damage from an outage it was never harmed by (Codex #1392 P2).
    """
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)

    unmasked = _add_photo_with_detection(db, folder_id, folder_path, "a.jpg")
    masked = _add_photo_with_detection(db, folder_id, folder_path, "b.jpg")
    collection_id = db.add_collection(
        "Half already masked",
        json.dumps([{"field": "photo_ids", "value": [unmasked, masked]}]),
    )

    pipeline_cfg = db.get_effective_config(cfg.load()).get("pipeline", {})
    sam2_variant = pipeline_cfg.get("sam2_variant")
    dinov2_variant = pipeline_cfg.get("dinov2_variant")

    mask_dir = tmp_path / ".vireo" / "masks"
    os.makedirs(mask_dir, exist_ok=True)
    mask_file = str(mask_dir / f"{masked}.{sam2_variant}.png")
    from PIL import Image
    Image.new("L", (4, 4), 255).save(mask_file)
    db.upsert_photo_mask(
        masked, sam2_variant, mask_file, "MegaDetector", 0.1, 0.1, 0.5, 0.5,
    )
    db.set_active_mask_variant(masked, sam2_variant)
    db.conn.execute(
        "UPDATE photos SET dino_embedding_variant=? WHERE id=?",
        (dinov2_variant, masked),
    )
    db.conn.commit()

    _stub_extract_masks_heavy_ops(monkeypatch)

    # The source is gone before the stage starts, so the pre-flight probe
    # drops both photos.
    import shutil
    shutil.rmtree(folder_path, ignore_errors=True)

    _runner, result = _run_extract_masks_only(db_path, ws_id, collection_id)

    em = result["stages"]["extract_masks"]
    assert em.get("unreadable") == 1, (
        f"Only the photo without a mask is at risk of a `no_subject_mask` "
        f"rejection; got {em!r}"
    )
    # The cached photo is a successful outcome, exactly as it would be on the
    # online cache-hit path — dropping it from every counter reports
    # "1 unreadable of 1" for a two-photo collection (Codex #1392 P2).
    assert em["masked"] == 1, (
        f"An already-masked photo is a cache hit, not a non-event; got {em!r}"
    )
    assert em["total"] == 2, (
        f"Total must cover both photos; got {em!r}"
    )


def test_extract_masks_preflight_fully_cached_folder_does_not_fail_stage(
    tmp_path, monkeypatch,
):
    """An offline folder whose photos are all already masked is not a failure.

    Nothing in it needed a source read, and nothing in it will be rejected as
    `no_subject_mask`. Latching a Fatal error and failing the stage there
    reports damage that did not happen — and blocks the run behind a
    reconnect instruction that would change nothing (Codex #1392 P2).
    """
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)

    photo_ids = [
        _add_photo_with_detection(db, folder_id, folder_path, f"b{i}.jpg")
        for i in range(2)
    ]
    collection_id = db.add_collection(
        "All already masked",
        json.dumps([{"field": "photo_ids", "value": photo_ids}]),
    )

    pipeline_cfg = db.get_effective_config(cfg.load()).get("pipeline", {})
    sam2_variant = pipeline_cfg.get("sam2_variant")
    dinov2_variant = pipeline_cfg.get("dinov2_variant")

    mask_dir = tmp_path / ".vireo" / "masks"
    os.makedirs(mask_dir, exist_ok=True)
    from PIL import Image
    for pid in photo_ids:
        mask_file = str(mask_dir / f"{pid}.{sam2_variant}.png")
        Image.new("L", (4, 4), 255).save(mask_file)
        db.upsert_photo_mask(
            pid, sam2_variant, mask_file, "MegaDetector", 0.1, 0.1, 0.5, 0.5,
        )
        db.set_active_mask_variant(pid, sam2_variant)
        db.conn.execute(
            "UPDATE photos SET dino_embedding_variant=? WHERE id=?",
            (dinov2_variant, pid),
        )
    db.conn.commit()

    _stub_extract_masks_heavy_ops(monkeypatch)

    import shutil
    shutil.rmtree(folder_path, ignore_errors=True)

    runner, result = _run_extract_masks_only(db_path, ws_id, collection_id)

    em = result["stages"]["extract_masks"]
    assert em.get("unreadable") == 0, (
        f"Nothing here is at risk of a `no_subject_mask` rejection; got {em!r}"
    )
    assert not any("unreachable" in e for e in result["errors"]), (
        f"No outage error belongs on a fully-cached folder; got "
        f"{result['errors']!r}"
    )
    final = _extract_masks_final_update(runner)
    assert final["status"] != "failed", (
        f"A fully-cached offline folder must not fail the stage; got {final!r}"
    )


def test_extract_masks_cancelled_total_covers_preflight_drops(
    tmp_path, monkeypatch,
):
    """A cancelled run must not publish impossible counters either.

    The cancel branch folds pre-flight drops into `unreadable` but reported
    the post-filter loop total beside it, so a cancel after an outage could
    persist "3 unreadable, total: 1" (Codex #1392 P2).
    """
    import config as cfg
    import pipeline_job as pj
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    dead_path = str(tmp_path / "aa_dead")
    live_path = str(tmp_path / "zz_live")
    os.makedirs(dead_path, exist_ok=True)
    os.makedirs(live_path, exist_ok=True)
    dead_folder = db.add_folder(dead_path)
    live_folder = db.add_folder(live_path)

    photo_ids = [
        _add_photo_with_detection(db, dead_folder, dead_path, "aa0.jpg"),
        _add_photo_with_detection(db, dead_folder, dead_path, "aa1.jpg"),
        _add_photo_with_detection(db, live_folder, live_path, "zz0.jpg"),
    ]
    collection_id = db.add_collection(
        "Cancelled after an outage",
        json.dumps([{"field": "photo_ids", "value": photo_ids}]),
    )

    _stub_extract_masks_heavy_ops(monkeypatch)

    # The dead folder is gone before the run, so pre-flight drops its two.
    import shutil
    shutil.rmtree(dead_path, ignore_errors=True)

    # Cancel as soon as the surviving healthy photo is reached.
    import masking
    import numpy as np

    def render_then_cancel(image_path, longest_edge=None):
        pj._should_abort_cancel_flag = True
        return np.zeros((16, 16, 3), dtype=np.uint8)

    monkeypatch.setattr(masking, "render_proxy", render_then_cancel)
    original_should_abort = pj._should_abort
    monkeypatch.setattr(
        pj, "_should_abort",
        lambda event: getattr(pj, "_should_abort_cancel_flag", False)
        or original_should_abort(event),
    )

    _runner, result = _run_extract_masks_only(db_path, ws_id, collection_id)
    if hasattr(pj, "_should_abort_cancel_flag"):
        del pj._should_abort_cancel_flag

    em = result["stages"]["extract_masks"]
    assert em.get("cancelled") is True, f"Expected a cancelled result; got {em!r}"
    assert em["total"] >= em.get("unreadable", 0), (
        f"Cancelled totals must still cover the photos they report; got {em!r}"
    )


def test_extract_masks_cancelled_step_warning_covers_unreadable(
    tmp_path, monkeypatch,
):
    """A cancel after a source dropped must still surface the unreadable
    count as a step warning.

    The mid-loop updates and the normal finalizer both feed
    ``em_failed + em_unreadable`` (and preflight unreadables) into the
    runner's ``error_count`` so the Jobs page badges the row for the
    reader. The cancel branch passed only ``em_failed``, so a cancel
    arriving after the pre-flight had dropped photos left a completed
    Extract row with a zero warning count next to a result that recorded
    positive ``unreadable`` (Codex #1392 P2 r3687499188).
    """
    import config as cfg
    import pipeline_job as pj
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    dead_path = str(tmp_path / "aa_dead")
    live_path = str(tmp_path / "zz_live")
    os.makedirs(dead_path, exist_ok=True)
    os.makedirs(live_path, exist_ok=True)
    dead_folder = db.add_folder(dead_path)
    live_folder = db.add_folder(live_path)

    photo_ids = [
        _add_photo_with_detection(db, dead_folder, dead_path, "aa0.jpg"),
        _add_photo_with_detection(db, dead_folder, dead_path, "aa1.jpg"),
        _add_photo_with_detection(db, live_folder, live_path, "zz0.jpg"),
    ]
    collection_id = db.add_collection(
        "Cancel-after-outage warning count",
        json.dumps([{"field": "photo_ids", "value": photo_ids}]),
    )

    _stub_extract_masks_heavy_ops(monkeypatch)

    # Kill the shared source before the run so pre-flight drops both.
    import shutil
    shutil.rmtree(dead_path, ignore_errors=True)

    import masking
    import numpy as np

    def render_then_cancel(image_path, longest_edge=None):
        pj._should_abort_cancel_flag = True
        return np.zeros((16, 16, 3), dtype=np.uint8)

    monkeypatch.setattr(masking, "render_proxy", render_then_cancel)
    original_should_abort = pj._should_abort
    monkeypatch.setattr(
        pj, "_should_abort",
        lambda event: getattr(pj, "_should_abort_cancel_flag", False)
        or original_should_abort(event),
    )

    runner, result = _run_extract_masks_only(db_path, ws_id, collection_id)
    if hasattr(pj, "_should_abort_cancel_flag"):
        del pj._should_abort_cancel_flag

    em = result["stages"]["extract_masks"]
    assert em.get("cancelled") is True, f"Expected cancelled; got {em!r}"
    assert em["unreadable"] >= 2, (
        f"Cancelled result must include the pre-flight drops in "
        f"``unreadable``; got {em!r}"
    )

    final = _extract_masks_final_update(runner)
    assert final["status"] == "completed", (
        f"Cancel branch finalizes as completed; got {final!r}"
    )
    # Before the fix ``error_count`` was ``em_failed`` (zero here), so a
    # cancel-after-outage Extract row rendered zero warnings alongside a
    # result recording two unreadable photos. Aligning it with the mid-
    # loop and finalizer paths (which include unreadable + preflight
    # unreadable) is the fix (Codex #1392 P2 r3687499188).
    assert final.get("error_count", 0) >= em["unreadable"], (
        f"error_count must at least cover the unreadable photos so the "
        f"Jobs row warns; got final={final!r}, em={em!r}"
    )


def test_extract_masks_preflight_error_denominator_matches_stage_total(
    tmp_path, monkeypatch,
):
    """The preflight Fatal message and the stage result must describe
    the same population.

    ``at_risk_dropped_ids`` counts mask candidates only — photos with no
    qualifying detection are dropped from it, because an outage doesn't
    change their fate (the loop would never have read them for masking).
    The denominator in the message, however, was ``total_before`` — the
    whole collection worklist. A collection with 1 mask candidate on an
    offline folder and additional no-detection photos published messages
    like "1 of 4 photos unreachable" alongside a stage result reporting
    ``total: 2``, and the reader had no way to reconcile them
    (Codex #1392 P2 r3687499184).
    """
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    dead_path = str(tmp_path / "aa_dead")
    live_path = str(tmp_path / "zz_live")
    os.makedirs(dead_path, exist_ok=True)
    os.makedirs(live_path, exist_ok=True)
    dead_folder = db.add_folder(dead_path)
    live_folder = db.add_folder(live_path)

    # 1 mask candidate on the offline folder + 1 mask candidate on the
    # healthy folder + 2 no-detection photos on the healthy folder.
    # Mask-candidate total = 2; whole-collection total = 4.
    photo_ids = [
        _add_photo_with_detection(db, dead_folder, dead_path, "aa0.jpg"),
        _add_photo_with_detection(db, live_folder, live_path, "zz0.jpg"),
    ]
    for filename in ("zz1.jpg", "zz2.jpg"):
        pid = db.add_photo(
            live_folder, filename, ".jpg", 1000,
            1_000_000.0 + hash(filename) % 1000,
        )
        _drop_jpeg(live_path, filename)
        photo_ids.append(pid)

    collection_id = db.add_collection(
        "Mixed detections plus outage",
        json.dumps([{"field": "photo_ids", "value": photo_ids}]),
    )

    _stub_extract_masks_heavy_ops(monkeypatch)

    import shutil
    shutil.rmtree(dead_path, ignore_errors=True)

    _runner, result = _run_extract_masks_only(db_path, ws_id, collection_id)

    em = result["stages"]["extract_masks"]
    assert em["total"] == 2, (
        f"Stage total must count only mask candidates (1 kept + 1 "
        f"dropped mask candidate); got {em!r}"
    )
    fatal = next(
        (e for e in result["errors"]
         if e.startswith("[extract_masks] Fatal:")),
        None,
    )
    assert fatal is not None, (
        f"Expected an extract_masks Fatal preflight error; got "
        f"{result['errors']!r}"
    )
    # Before the fix the denominator was ``total_before`` — the whole
    # collection worklist — so the message read "1 of 4 photos
    # unreachable" while the stage result reported total: 2.
    assert "1 of 2 photos unreachable" in fatal, (
        f"Denominator must match the mask-candidate stage total; got "
        f"{fatal!r}"
    )


def _seed_cached_mask(db, tmp_path, photo_id, sam2_variant, dinov2_variant,
                      prompt=(0.1, 0.1, 0.5, 0.5), detector="MegaDetector"):
    """Give `photo_id` a complete, active mask for the configured variant."""
    from PIL import Image
    mask_dir = tmp_path / ".vireo" / "masks"
    os.makedirs(mask_dir, exist_ok=True)
    mask_file = str(mask_dir / f"{photo_id}.{sam2_variant}.png")
    Image.new("L", (4, 4), 255).save(mask_file)
    db.upsert_photo_mask(photo_id, sam2_variant, mask_file, detector, *prompt)
    db.set_active_mask_variant(photo_id, sam2_variant)
    db.conn.execute(
        "UPDATE photos SET dino_embedding_variant=? WHERE id=?",
        (dinov2_variant, photo_id),
    )
    db.conn.commit()
    return mask_file


def test_extract_masks_preflight_stale_cached_mask_is_still_at_risk(
    tmp_path, monkeypatch,
):
    """A mask built from an obsolete detection is not a usable cache hit.

    If the detection box moved, the photo needs re-masking — which an offline
    source makes impossible. Treating the stale mask as a success suppresses
    the outage and lets scoring keep using a mask and embedding derived from
    a detection that no longer applies (Codex #1392 P1).
    """
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)

    pid = _add_photo_with_detection(db, folder_id, folder_path, "moved.jpg")
    collection_id = db.add_collection(
        "Stale prompt",
        json.dumps([{"field": "photo_ids", "value": [pid]}]),
    )

    pipeline_cfg = db.get_effective_config(cfg.load()).get("pipeline", {})
    # Mask cached against a box that no longer matches the live detection
    # (_add_photo_with_detection stores 0.1/0.1/0.5/0.5).
    _seed_cached_mask(
        db, tmp_path, pid, pipeline_cfg.get("sam2_variant"),
        pipeline_cfg.get("dinov2_variant"), prompt=(0.9, 0.9, 0.05, 0.05),
    )

    _stub_extract_masks_heavy_ops(monkeypatch)

    import shutil
    shutil.rmtree(folder_path, ignore_errors=True)

    _runner, result = _run_extract_masks_only(db_path, ws_id, collection_id)

    em = result["stages"]["extract_masks"]
    assert em.get("unreadable") == 1, (
        f"A stale mask needs a re-read the outage prevents; got {em!r}"
    )
    assert em["masked"] == 0, (
        f"An obsolete mask is not a successful cache hit; got {em!r}"
    )


def test_extract_masks_preflight_ignores_photos_with_no_detection(
    tmp_path, monkeypatch,
):
    """An offline photo the mask worklist would never have touched is not a
    casualty of the outage.

    Photos with no qualifying detection never enter mask extraction, so
    counting them turns an unrelated source outage into a Fatal Extract
    failure and tells the user to reconnect for photos that would still have
    no mask candidate (Codex #1392 P2).
    """
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)

    from PIL import Image
    no_det = db.add_photo(folder_id, "nodet.jpg", ".jpg", 1000, 5_000_000.0)
    Image.new("RGB", (16, 16), "black").save(
        os.path.join(folder_path, "nodet.jpg")
    )
    collection_id = db.add_collection(
        "No mask candidates",
        json.dumps([{"field": "photo_ids", "value": [no_det]}]),
    )

    _stub_extract_masks_heavy_ops(monkeypatch)

    import shutil
    shutil.rmtree(folder_path, ignore_errors=True)

    _runner, result = _run_extract_masks_only(db_path, ws_id, collection_id)

    em = result["stages"]["extract_masks"]
    assert em.get("unreadable", 0) == 0, (
        f"A photo with no detection was never a mask candidate; got {em!r}"
    )
    assert not any("unreachable" in e for e in result["errors"]), (
        f"No reconnect instruction belongs here; got {result['errors']!r}"
    )


def test_extract_masks_early_exit_carries_outage_detail_to_the_step():
    """A failed early exit must tell the Jobs page why it failed.

    The step showed only "Skipped — MegaDetector produced no detections"
    with no error, so a row failed by a source outage hid the reconnect
    instruction and blamed detections instead (Codex #1392 P2).
    """
    from pipeline_job import _extract_masks_early_exit

    outage = "[extract_masks] Fatal: 311 of 315 photos unreachable."
    _stage, step_status, step_extra, _payload = _extract_masks_early_exit(
        "no_detections", subthreshold=0, preflight_unreadable=311,
        preflight_masked=4, offline_latched=True, outage_error=outage,
    )
    assert step_status == "failed"
    assert step_extra.get("error") == outage, (
        f"The failed step needs the outage detail; got {step_extra!r}"
    )
    assert step_extra.get("error_count") == 311

    _stage, step_status, step_extra, _payload = _extract_masks_early_exit(
        "weights_missing", subthreshold=0, preflight_unreadable=0,
        preflight_masked=0, offline_latched=False, outage_error=None,
    )
    assert step_status == "completed"
    assert step_extra == {}, (
        f"A benign skip carries no error fields; got {step_extra!r}"
    )


def test_extract_masks_early_exit_fails_when_preflight_unreadable_without_latch():
    """A classify-owned outage still fails the extract early exit.

    When classify has already emitted a source-offline Fatal, the pre-flight
    intentionally does not re-latch (to avoid a duplicate error), so
    ``offline_latched`` stays False even though ``preflight_unreadable > 0``.
    Prior shape: the early exit reported ``skipped``/``completed``, leaving
    a green Jobs row that hid the fact the extract stage had unreachable
    photos of its own (Codex #1392 P2 r3687403367). It must fail on
    ``preflight_unreadable > 0`` too, and carry the extract-owned outage
    detail so the row shows the reconnect instruction.
    """
    from pipeline_job import _extract_masks_early_exit

    outage = "[extract_masks] Fatal: 2 of 5 photos unreachable."
    stage, step_status, step_extra, _payload = _extract_masks_early_exit(
        "no_detections", subthreshold=0, preflight_unreadable=2,
        preflight_masked=0, offline_latched=False, outage_error=outage,
    )
    assert stage == "failed", (
        f"An early exit with unreadable photos is not a benign skip; "
        f"got {stage!r}"
    )
    assert step_status == "failed", (
        f"The runner step must show failed too; got {step_status!r}"
    )
    assert step_extra.get("error") == outage, (
        f"The step needs the outage detail so the Jobs row shows the "
        f"reconnect instruction; got {step_extra!r}"
    )
    assert step_extra.get("error_count") == 2


def test_extract_masks_preflight_counts_rescued_weak_detections(
    tmp_path, monkeypatch,
):
    """A weak burst frame the rescue path would have masked still counts.

    With weak-detection rescue enabled the mask worklist reaches detections
    below ``detector_confidence`` — but only for frames ``contextual_weak_
    runs`` surfaces with matching-anchor-species classifications. A pre-flight
    predicate that queries only at the main threshold silently drops those
    frames from both outcome sets; if every at-risk photo on a dead source is
    a rescued frame, the outage never latches at all — the silent completion
    this PR exists to prevent (Codex #1392 P2 r3687355839).

    The isolated-weak case is guarded separately below: an offline photo with
    a weak-confidence detection but no rescuing anchor context would never be
    read by the mask loop, so it must NOT inflate the outage report
    (Codex #1392 P2 r3687403366).
    """
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)

    # Bracketed burst: two high-confidence anchors classified with the same
    # species, and a weak middle frame between them — the pattern
    # ``contextual_weak_runs`` picks up. Both anchor detections carry the
    # same prediction so ``load_photo_features`` marks the middle frame
    # ``subject_uncertain`` (i.e. an eligible rescue).
    photo_ids = []
    detection_ids = []
    for index, confidence in enumerate((0.9, 0.15, 0.9)):
        filename = f"burst{index}.jpg"
        pid = db.add_photo(
            folder_id, filename, ".jpg", 1000, 1_000_000.0 + index,
            timestamp=f"2026-07-18T08:36:3{index}",
        )
        _drop_jpeg(folder_path, filename)
        det_id = db.write_detection_batch(
            pid,
            "megadetector-v6",
            [{
                "box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
                "confidence": confidence,
                "category": "animal",
            }],
        )[0]
        photo_ids.append(pid)
        detection_ids.append(det_id)

    db.add_prediction(
        detection_ids[0], "Great-tailed Grackle", 0.9, "inat21",
    )
    db.add_prediction(
        detection_ids[-1], "Great-tailed Grackle", 0.9, "inat21",
    )

    collection_id = db.add_collection(
        "Rescued weak burst",
        json.dumps([{"field": "photo_ids", "value": photo_ids}]),
    )

    _stub_extract_masks_heavy_ops(monkeypatch)

    import shutil
    shutil.rmtree(folder_path, ignore_errors=True)

    _runner, result = _run_extract_masks_only(db_path, ws_id, collection_id)

    em = result["stages"]["extract_masks"]
    # All three photos are mask candidates: two via strict detection, one
    # via contextual weak rescue. All three must show up as unreadable — a
    # stage that reports 2 unreadable of 3 would silently drop the rescued
    # frame, exactly the bug this test guards against.
    assert em.get("unreadable") == 3, (
        f"Rescued weak frame on a dead source must still count as "
        f"unreadable; got {em!r}"
    )
    assert em["total"] == 3, (
        f"Total must cover the rescued candidate too; got {em!r}"
    )
    assert any(
        "3 of 3" in e and "unreachable" in e for e in result["errors"]
    ), (
        f"The reconnect message must claim every dropped candidate — "
        f"rescued frame included; got {result['errors']!r}"
    )


def test_extract_masks_preflight_ignores_isolated_weak_detections(
    tmp_path, monkeypatch,
):
    """An offline weak-conf photo without a rescuing burst must not inflate
    the outage.

    ``contextual_weak_runs`` needs matching-anchor-species neighbours to
    surface a weak frame; a lone weak detection would never enter the mask
    worklist online. A pre-flight that just checks ``weak_detection_
    confidence`` counts it as at-risk anyway, turning an unrelated folder
    outage into a Fatal Extract failure and demanding a reconnect for a
    photo that still wouldn't get a mask (Codex #1392 P2 r3687403366).
    """
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)

    from PIL import Image
    pid = db.add_photo(folder_id, "weak.jpg", ".jpg", 1000, 7_000_000.0)
    Image.new("RGB", (16, 16), "black").save(
        os.path.join(folder_path, "weak.jpg")
    )
    # Between weak_detection_confidence (0.12) and detector_confidence (0.2)
    # — but isolated, so contextual_weak_runs won't surface it and the mask
    # loop would never read it online.
    db.save_detections(
        pid,
        [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
          "confidence": 0.15, "category": "animal"}],
        detector_model="megadetector-v6",
    )
    collection_id = db.add_collection(
        "Isolated weak frame",
        json.dumps([{"field": "photo_ids", "value": [pid]}]),
    )

    _stub_extract_masks_heavy_ops(monkeypatch)

    import shutil
    shutil.rmtree(folder_path, ignore_errors=True)

    _runner, result = _run_extract_masks_only(db_path, ws_id, collection_id)

    em = result["stages"]["extract_masks"]
    assert em.get("unreadable") == 0, (
        f"An isolated weak detection isn't a mask candidate — an outage "
        f"must not report it as unreachable; got {em!r}"
    )
    assert not any(
        "unreachable" in e for e in result["errors"]
    ), (
        f"No reconnect instruction belongs to an unrelated outage; got "
        f"{result['errors']!r}"
    )


def test_archive_mount_baseline_records_only_real_mount_points(
        tmp_path, monkeypatch):
    """The baseline distinguishes a real mount from an ordinary directory.

    An ordinary ``/mnt/photos`` that was never a mount must record False,
    so the staleness check below can never fire for it. That is what keeps
    the guard from refusing legitimate local destinations.
    """
    import os as _os

    import pipeline_job as _pj

    mounted = str(tmp_path / "mounted")
    plain = str(tmp_path / "plain")
    _os.makedirs(mounted)
    _os.makedirs(plain)

    monkeypatch.setattr(
        _pj, "_archive_mount_root_candidates",
        lambda path: [mounted, plain],
    )
    real_ismount = _os.path.ismount
    monkeypatch.setattr(
        _os.path, "ismount",
        lambda p: True if str(p) == mounted else real_ismount(p),
    )

    baseline = _pj._archive_mount_baseline("/anything")

    assert baseline == {mounted: True, plain: False}, baseline


def test_unmounted_since_baseline_fires_only_for_a_lost_mount(
        tmp_path, monkeypatch):
    """Only a root that WAS mounted and no longer is counts as lost."""
    import os as _os

    import pipeline_job as _pj

    lost = str(tmp_path / "lost")
    never = str(tmp_path / "never")
    still = str(tmp_path / "still")
    for p in (lost, never, still):
        _os.makedirs(p)

    real_ismount = _os.path.ismount
    monkeypatch.setattr(
        _os.path, "ismount",
        lambda p: True if str(p) == still else real_ismount(p),
    )

    # ``lost`` was mounted at baseline and now is not -> reported.
    assert _pj._unmounted_since_baseline({lost: True}) == lost
    # ``never`` was not a mount to begin with -> an ordinary directory,
    # never reported no matter what.
    assert _pj._unmounted_since_baseline({never: False}) is None
    # ``still`` is mounted now as it was then -> nothing lost.
    assert _pj._unmounted_since_baseline({still: True}) is None
    # Empty baseline (destination not mount-shaped at all).
    assert _pj._unmounted_since_baseline({}) is None


def test_archive_mount_baseline_seeds_true_from_known_mounted_roots(
        tmp_path, monkeypatch):
    """A candidate ever seen live must baseline True even when detached now.

    Otherwise a share that was already unmounted BEFORE the run started
    escapes the guard: its baseline is False, no mounted → unmounted
    transition can fire against a False baseline, and the persistent
    ``/mnt/<name>`` stub still passes the per-batch check. Cross-run
    history is what closes the hole. See PR #1396 review
    (Codex P1 r3687401636).
    """
    import os as _os

    import pipeline_job as _pj

    detached = str(tmp_path / "detached")
    virgin = str(tmp_path / "virgin")
    _os.makedirs(detached)
    _os.makedirs(virgin)

    monkeypatch.setattr(
        _pj, "_archive_mount_root_candidates",
        lambda path: [detached, virgin],
    )
    real_ismount = _os.path.ismount
    monkeypatch.setattr(
        _os.path, "ismount",
        # Neither candidate is currently a mount — but ``detached`` is in
        # the known-set (a prior run saw it live), so the baseline must
        # record it True so a still-detached state fires the transition.
        lambda p: False if str(p) in (detached, virgin) else real_ismount(p),
    )

    baseline = _pj._archive_mount_baseline(
        "/anything", known_mounted_roots={detached},
    )
    assert baseline == {detached: True, virgin: False}, baseline
    # The staleness check then sees the True → False transition for
    # ``detached``, exactly as it would for a share that dropped
    # mid-run.
    assert _pj._unmounted_since_baseline(baseline) == detached


def test_known_mount_roots_round_trip_through_db_meta(tmp_path):
    """Persisted mount roots survive across runs; only True is remembered.

    Any candidate observed False in the baseline stays out of the record,
    so a hand-made local ``/mnt/photos`` (never mounted) never joins the
    known-set and never trips the staleness check. Merging with the
    existing set is what keeps a mount root once-seen recorded across
    subsequent runs, even when it's not live at capture time.
    """
    import pipeline_job as _pj
    from db import Database

    db = Database(str(tmp_path / "test.db"))

    # Nothing recorded yet.
    assert _pj._load_known_mount_roots(db) == set()

    # A run observes /mnt/nas live but /mnt/photos not.
    _pj._record_known_mount_roots(db, {"/mnt/nas": True, "/mnt/photos": False})
    assert _pj._load_known_mount_roots(db) == {"/mnt/nas"}

    # A second run observes /mnt/photos live; both roots are remembered.
    _pj._record_known_mount_roots(db, {"/mnt/photos": True})
    assert _pj._load_known_mount_roots(db) == {"/mnt/nas", "/mnt/photos"}

    # A third run observes /mnt/nas detached now — the previous True
    # stays recorded so the next baseline still seeds it True.
    _pj._record_known_mount_roots(db, {"/mnt/nas": False})
    assert _pj._load_known_mount_roots(db) == {"/mnt/nas", "/mnt/photos"}

    # Recording an all-False baseline is a no-op (empty ``fresh`` set —
    # a run that saw no live mounts must not blank the record).
    _pj._record_known_mount_roots(db, {"/mnt/nas": False, "/mnt/photos": False})
    assert _pj._load_known_mount_roots(db) == {"/mnt/nas", "/mnt/photos"}


def test_pipeline_classifier_factory_uses_non_parking_cancel_probe():
    """Regression: the ``_construct_classifier`` factory inside
    ``run_pipeline_job`` runs while ``ModelCache._Entry.load_lock`` is
    held. ``Classifier._compute_embeddings_with_progress`` calls the
    factory-supplied ``cancel_check`` between labels; if that closure
    invokes the parking ``_should_abort`` (which parks on pause via
    ``_pause_checkpoint``), a Pause arriving during label-embedding
    setup would keep an unpaused peer requesting the same cache key
    blocked until Resume. The factory must use
    ``_should_abort_without_pause`` — same pattern the classify job
    factory uses (test_classify_factory_cancel_check_uses_pure_cancel_probe
    in test_classify_job.py) and the ``model_cache.py`` factory-context
    fix in commit ``5aee48c``. Pause is still honored at the next outer
    checkpoint after the factory returns and the load lock releases.
    """
    import ast
    import inspect

    import pipeline_job

    source = inspect.getsource(pipeline_job.run_pipeline_job)
    tree = ast.parse(source)

    def _find_construct_classifier(node):
        for child in ast.walk(node):
            if (
                isinstance(child, ast.FunctionDef)
                and child.name == "_construct_classifier"
            ):
                return child
        return None

    construct_fn = _find_construct_classifier(tree)
    assert construct_fn is not None, (
        "_construct_classifier factory not found inside "
        "run_pipeline_job — test needs update if this refactored"
    )

    inner_cancel_check = None
    for child in ast.walk(construct_fn):
        if (
            isinstance(child, ast.FunctionDef)
            and child.name == "cancel_check"
        ):
            inner_cancel_check = child
            break
    assert inner_cancel_check is not None, (
        "cancel_check closure not found inside _construct_classifier"
    )

    body_src = ast.unparse(inner_cancel_check)
    assert "_should_abort_without_pause" in body_src, (
        "_construct_classifier's inner cancel_check must call "
        "_should_abort_without_pause (the non-parking probe), not "
        "_should_abort. Codex thread review#4945197220 on "
        "model_cache.py:237: the factory runs while entry.load_lock is "
        "held and Classifier._compute_embeddings_with_progress invokes "
        "cancel_check between labels; parking there strands every "
        "unpaused peer waiting for the same cache key until Resume."
    )
    # And it must not fall back to the parking probe. Guard against a
    # regression that adds an ``or _should_abort(...)`` branch.
    calls = [
        node for node in ast.walk(inner_cancel_check)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "_should_abort"
    ]
    assert not calls, (
        "_construct_classifier's inner cancel_check must not call the "
        "parking _should_abort — use _should_abort_without_pause"
    )



# ---------------------------------------------------------------------------
# Inconclusive mount-prefix resolution fails closed in every pipeline guard
# ---------------------------------------------------------------------------

def _inconclusive(roots):
    from volume_reachability import MountRootCandidates

    out = MountRootCandidates(roots)
    out.conclusive = False
    return out


def test_archive_mount_baseline_aborts_when_resolution_inconclusive(monkeypatch, tmp_path):
    import pipeline_job

    monkeypatch.setattr(pipeline_job, "_archive_mount_root_candidates", lambda p: _inconclusive(["/mnt/archive"]))
    monkeypatch.setattr(
        pipeline_job.os.path, "ismount",
        lambda p: pytest.fail("inconclusive resolution must short-circuit ismount"),
    )
    with pytest.raises(RuntimeError, match="reconnect it and retry"):
        pipeline_job._archive_mount_baseline(str(tmp_path))


def test_missing_archive_mount_root_refuses_on_inconclusive_resolution(monkeypatch, tmp_path):
    import pipeline_job

    monkeypatch.setattr(pipeline_job, "_archive_mount_root_candidates", lambda p: _inconclusive(["/mnt/archive"]))
    monkeypatch.setattr(pipeline_job.os.path, "lexists", lambda p: True)
    assert pipeline_job._missing_archive_mount_root(str(tmp_path)) == "/mnt/archive"


def test_source_offline_reason_scopes_inconclusive_resolution_to_mount(monkeypatch, tmp_path):
    import pipeline_job

    monkeypatch.setattr(pipeline_job, "_archive_mount_root_candidates", lambda p: _inconclusive(["/mnt/archive"]))
    scope, reason = pipeline_job._source_offline_reason(
        str(tmp_path), str(tmp_path / "img.jpg"),
    )
    assert scope == "mount"
    assert "/mnt/archive" in reason
