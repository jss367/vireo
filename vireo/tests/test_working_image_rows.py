"""Jobs hand ``image_loader.load_working_image`` catalog ``sqlite3.Row``s.

``collections.get_photos`` returns rows, and ``sqlite3.Row`` has no
``.get``. ``load_working_image`` called ``photo.get("working_copy_path")``,
so the standalone classify job (``_prepare_image``, for every full-image and
crop it loads) and the sharpness job crashed on the first photo whenever a
working-copy directory was configured, which the routes always do.
"""

import json
import os
from unittest.mock import MagicMock, patch

import pytest
from PIL import Image
from wait import wait_for_job_via_runner


def _give_photos_working_copies(app, db):
    """Write a working-copy JPEG for every photo and point the row at it."""
    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    os.makedirs(os.path.join(vireo_dir, "working"), exist_ok=True)
    photo_ids = [
        row["id"] for row in db.conn.execute("SELECT id FROM photos ORDER BY id")
    ]
    for pid in photo_ids:
        rel = f"working/{pid}.jpg"
        Image.new("RGB", (640, 480), color=(90, 120, 60)).save(
            os.path.join(vireo_dir, rel), "JPEG",
        )
        db.conn.execute(
            "UPDATE photos SET working_copy_path = ? WHERE id = ?", (rel, pid),
        )
    db.commit()
    return photo_ids


def _all_photos_collection(db, photo_ids):
    return db.collections.add(
        "All", json.dumps([{"field": "photo_ids", "value": photo_ids}]),
    )


def _classify_patches():
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
        # An empty scene for every photo: each one goes to the full-image
        # fallback, which loads the whole frame through ``_prepare_image``.
        patch("classify_job.detection.detect_animals", lambda path: []),
        patch("classify_job.detection.get_primary_detection", lambda dets: None),
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
    ]


@pytest.mark.parametrize("reclassify", [False, True])
def test_classify_job_full_image_fallback_loads_working_copy(app_and_db, reclassify):
    app, db = app_and_db
    photo_ids = _give_photos_working_copies(app, db)
    col_id = _all_photos_collection(db, photo_ids)

    patches = _classify_patches()
    for p in patches:
        p.start()
    try:
        with app.test_client() as client:
            resp = client.post(
                "/api/jobs/classify",
                json={"collection_id": col_id, "reclassify": reclassify},
            )
            assert resp.status_code == 200, resp.get_json()
            job = wait_for_job_via_runner(app._job_runner, resp.get_json()["job_id"])
    finally:
        for p in reversed(patches):
            p.stop()

    assert job["status"] == "completed", (job.get("errors"), job.get("result"))
    assert job["result"]["failed"] == 0, job["result"]
    # Every photo was classified on a full-image detection.
    classified = {
        row["photo_id"] for row in db.conn.execute(
            "SELECT DISTINCT d.photo_id FROM predictions p "
            "JOIN detections d ON d.id = p.detection_id "
            "WHERE d.detector_model = 'full-image'"
        )
    }
    assert classified == set(photo_ids)


def test_sharpness_job_scores_from_working_copy(app_and_db):
    app, db = app_and_db
    photo_ids = _give_photos_working_copies(app, db)
    col_id = _all_photos_collection(db, photo_ids)

    with app.test_client() as client:
        resp = client.post("/api/jobs/sharpness", json={"collection_id": col_id})
        assert resp.status_code == 200, resp.get_json()
        job = wait_for_job_via_runner(app._job_runner, resp.get_json()["job_id"])

    assert job["status"] == "completed", (job.get("errors"), job.get("result"))
    scored = {
        row["id"] for row in db.conn.execute(
            "SELECT id FROM photos WHERE sharpness IS NOT NULL"
        )
    }
    assert scored == set(photo_ids)
