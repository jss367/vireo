"""Every job dict the runner publishes must be JSON-serializable.

``GET /api/jobs`` (polled by the Jobs page, the bottom panel and the navbar
job counter) and ``GET /api/jobs/<id>`` jsonify the runner's job snapshots.
A single value ``jsonify`` cannot encode, such as a ``set`` a worker parked on
its job dict, turns every poll into a 500 for as long as the job stays in
memory, so every running job vanishes from the UI.
"""

import threading
from unittest.mock import MagicMock, patch

import pytest
from testing.waits import synchronization_timeout
from wait import wait_for_job_via_client


def _classify_patches(prepare_image):
    """Run the real classify job with a stub model, detector and image load."""
    fake_model = {
        "id": "test-model",
        "name": "TestModel",
        "model_str": "hf-hub:imageomics/bioclip",
        "weights_path": "/tmp/weights.bin",
        "model_type": "bioclip",
        "downloaded": True,
    }
    clf = MagicMock()
    clf.classify_batch_with_embedding.side_effect = (
        lambda imgs, threshold=0: [
            ([{"species": "Northern Cardinal", "score": 0.9}], None)
            for _ in imgs
        ]
    )
    return [
        patch("classify_job.job_run.get_active_model", return_value=fake_model),
        patch("classify_job.job_run.get_models", return_value=[fake_model]),
        patch("classify_job.job_run._load_job_taxonomy", return_value=None),
        patch(
            "classify_job.job_run._load_labels",
            return_value=(["Northern Cardinal"], False, []),
        ),
        patch("classify_job.job_run.Classifier", return_value=clf),
        # MegaDetector finds a person in bird1.jpg and an animal elsewhere,
        # so the detect phase hands the classify phase both a non-animal
        # photo and photos to classify.
        patch(
            "classify_job.detection.detect_animals",
            lambda path: [{
                "box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.3},
                "confidence": 0.9,
                "category": "person" if path.endswith("bird1.jpg") else "animal",
            }],
        ),
        patch(
            "classify_job.detection.get_primary_detection",
            lambda dets: dets[0],
        ),
        patch("classify_job.detection.compute_sharpness", None),
        patch(
            "detector.ensure_megadetector_weights",
            lambda progress_callback=None: "/weights",
        ),
        patch(
            "computation_cache.megadetector_runtime_fingerprint",
            lambda *a: "legacy",
        ),
        patch("subjects.analyze_photo", lambda *a, **k: None),
        patch("classify_job.classify_pass._prepare_image", prepare_image),
    ]


@pytest.mark.parametrize("reclassify", [False, True])
def test_job_list_serializes_while_classify_job_runs(app_and_db, reclassify):
    """The classify job's detect-to-classify handoff must not break polling.

    The detect phase used to park ``set``s of photo ids on the job dict
    (``_detect_processed_ids``, ``_non_animal_photo_ids``,
    ``_detect_reused_ids``), so every ``/api/jobs`` poll answered 500 from
    the end of detection until the finished job was pruned.
    """
    import json

    app, db = app_and_db
    photo_ids = [
        row["id"] for row in db.conn.execute("SELECT id FROM photos ORDER BY id")
    ]
    col_id = db.collections.add(
        "All", json.dumps([{"field": "photo_ids", "value": photo_ids}]),
    )
    in_classify = threading.Event()
    release = threading.Event()

    def blocking_prepare_image(photo, folders, *args, **kwargs):
        in_classify.set()
        assert release.wait(synchronization_timeout(30))
        return MagicMock(info={}), "/photos/2024", "/photos/2024/x.jpg"

    patches = _classify_patches(blocking_prepare_image)
    for p in patches:
        p.start()
    try:
        with app.test_client() as client:
            resp = client.post(
                "/api/jobs/classify",
                json={"collection_id": col_id, "reclassify": reclassify},
            )
            assert resp.status_code == 200, resp.get_json()
            job_id = resp.get_json()["job_id"]
            try:
                assert in_classify.wait(synchronization_timeout(30)), (
                    "classify phase never prepared an image"
                )

                listing = client.get("/api/jobs")
                assert listing.status_code == 200, listing.get_data(as_text=True)
                ids = [j["id"] for j in listing.get_json()["active"]]
                assert job_id in ids

                status = client.get(f"/api/jobs/{job_id}")
                assert status.status_code == 200, status.get_data(as_text=True)
            finally:
                release.set()
            job = wait_for_job_via_client(client, job_id)
            assert job["status"] == "completed", job

            # The finished job stays in memory until pruned; it must keep
            # serializing too.
            listing = client.get("/api/jobs")
            assert listing.status_code == 200, listing.get_data(as_text=True)
    finally:
        for p in reversed(patches):
            p.stop()

    # The handoff still reaches the classify phase: the person-only photo
    # is skipped, the others are classified.
    predicted = {
        row["filename"] for row in db.conn.execute(
            "SELECT DISTINCT ph.filename FROM predictions p "
            "JOIN detections d ON d.id = p.detection_id "
            "JOIN photos ph ON ph.id = d.photo_id"
        )
    }
    assert predicted == {"bird2.jpg", "bird3.jpg"}


def test_unsafe_paths_names_each_value_json_cannot_encode():
    from job_json import unsafe_paths

    assert unsafe_paths({"id": "x", "result": {"n": [1, 2]}}) == []
    assert unsafe_paths({
        "_ids": {1, 2},
        "result": {"items": [1, object()]},
        "progress": {1: "a", "b": "c"},
    }) == [
        "job['_ids'] (set)",
        "job['result']['items'][1] (object)",
        "job['progress'] (keys)",
    ]


def test_guard_flags_a_job_dict_value_json_cannot_encode():
    """The autouse runner fixture records what a snapshot cannot encode.

    The job parks a set on its dict while running and drops it before it
    finishes, so only the mid-run snapshot can see it. The test then clears
    the recorded violation, which would otherwise fail it at teardown.
    """
    import job_json
    from jobs import JobRunner
    from wait import wait_for_job_via_runner

    runner = JobRunner()
    parked = threading.Event()
    release = threading.Event()

    def work(job):
        job["_photo_ids"] = {1, 2}
        parked.set()
        assert release.wait(synchronization_timeout(30))
        del job["_photo_ids"]
        return {"ok": True}

    job_id = runner.start("probe", work, ephemeral=True)
    try:
        assert parked.wait(synchronization_timeout(30))
        runner.list_jobs()
    finally:
        release.set()
    wait_for_job_via_runner(runner, job_id)

    found = set(job_json.VIOLATIONS)
    job_json.VIOLATIONS.clear()
    assert found == {f"probe job {job_id}: job['_photo_ids'] (set)"}

