"""Regression tests for classify-job cancellation, reclassify scoping and
detection filtering.

Each test pins one defect: a reclassify pre-clear that cascaded away every
photo's predictions before any were rebuilt, a detection-time Stop reported
as "Detection unavailable", fresh passes classifying boxes the cached path
filters out, a batch-inference Stop counted as per-image failures, and a
summary count that could go negative.
"""
from unittest.mock import MagicMock, patch

import pytest
from resource_ledger import ResourceWaitCancelled

BOX = {"x": 0, "y": 0, "w": 1, "h": 1}


class _Runner:
    def __init__(self, is_cancelled=None):
        self.steps = []
        self._is_cancelled = is_cancelled or (lambda job_id: False)

    def push_event(self, *args):
        pass

    def update_step(self, job_id, step_id, **kwargs):
        self.steps.append((step_id, kwargs))

    def is_cancelled(self, job_id):
        return self._is_cancelled(job_id)


def _job(job_id="classify-test"):
    return {
        "id": job_id,
        "progress": {"current": 0, "total": 0, "current_file": "", "rate": 0},
        "errors": [],
    }


@pytest.fixture
def db(tmp_path, monkeypatch):
    import config as cfg
    from db import Database

    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    database = Database(str(tmp_path / "test.db"))
    ws = database.ensure_default_workspace()
    database.set_active_workspace(ws)
    database.folder_id = database.add_folder(str(tmp_path), name="p")
    return database


def _add_photos(db, n):
    photos = []
    for i in range(n):
        pid = db.add_photo(
            db.folder_id, f"p{i}.jpg", extension=".jpg",
            file_size=100, file_mtime=float(i),
        )
        photos.append(
            {"id": pid, "filename": f"p{i}.jpg", "folder_id": db.folder_id,
             "timestamp": None}
        )
    return photos


def _detector_patches(detect):
    return [
        patch("classify_job.detect_animals", detect),
        patch("classify_job.get_primary_detection", lambda dets: dets[0]),
        patch("classify_job.compute_sharpness", None),
        patch("detector.ensure_megadetector_weights",
              lambda progress_callback=None: "/weights"),
        patch("computation_cache.megadetector_runtime_fingerprint",
              lambda *a: "legacy"),
        patch("subjects.analyze_photo", lambda *a, **k: None),
    ]


def _run_patched(patches, fn):
    for p in patches:
        p.start()
    try:
        return fn()
    finally:
        for p in reversed(patches):
            p.stop()


def _sparrow_clf():
    clf = MagicMock()
    clf.classify_batch_with_embedding.side_effect = (
        lambda imgs, threshold=0: [
            ([{"species": "Sparrow", "score": 0.9}], None) for _ in imgs
        ]
    )
    return clf


def _prediction_count(db, photo_id, model=None):
    sql = (
        "SELECT COUNT(*) FROM predictions p "
        "JOIN detections d ON d.id = p.detection_id WHERE d.photo_id = ?"
    )
    args = [photo_id]
    if model is not None:
        sql += " AND p.classifier_model = ?"
        args.append(model)
    return db.conn.execute(sql, args).fetchone()[0]


def test_cancelled_reclassify_keeps_unreached_photos_predictions(db, tmp_path):
    """A Stop mid-classify must leave the photos the loop never reached with
    their old predictions, including other models' rows on the same box.

    Before the fix, ``_detect_subjects`` ran the global ``clear_detections``
    on every photo up front; its cascade wiped all predictions (every model,
    every workspace) before the classify loop rebuilt any of them.
    """
    from classify_job import _classify_photos, _detect_subjects

    photos = _add_photos(db, 2)
    for photo in photos:
        det = db.save_detections(
            photo["id"],
            [{"box": BOX, "confidence": 0.9, "category": "animal"}],
            detector_model="megadetector-v6",
        )[0]
        db.record_detector_run(photo["id"], "megadetector-v6", box_count=1)
        db.add_prediction(det, species="Robin", confidence=0.9,
                          model="BioCLIP", labels_fingerprint="legacy")
        db.add_prediction(det, species="Robin", confidence=0.8,
                          model="OtherModel", labels_fingerprint="other")

    folders = {db.folder_id: str(tmp_path)}
    job = _job()
    detection_map, _ = _run_patched(
        _detector_patches(
            lambda path: [{"box": dict(BOX), "confidence": 0.9,
                           "category": "animal"}]
        ),
        lambda: _detect_subjects(photos, folders, _Runner(), job, True, db),
    )
    assert [_prediction_count(db, p["id"]) for p in photos] == [2, 2]

    calls = {"n": 0}

    def cancel_after_first(job_id):
        calls["n"] += 1
        return calls["n"] >= 2

    with patch("classify_job._prepare_image",
               lambda *a, **k: (MagicMock(info={}), str(tmp_path), "x")):
        _classify_photos(
            photos, folders, detection_map, set(), _sparrow_clf(), "bioclip",
            "BioCLIP", _Runner(cancel_after_first), job, db,
            labels_fingerprint="legacy", reclassify=True,
        )

    unreached = photos[1]["id"]
    assert _prediction_count(db, unreached, "BioCLIP") == 1
    assert _prediction_count(db, unreached, "OtherModel") == 1
    # The reached photo's other-model rows survive its own rebuild too.
    assert _prediction_count(db, photos[0]["id"], "OtherModel") == 1


def test_summary_counts_match_score_skips_without_going_negative(db, tmp_path):
    """Skips backed only by a match-score record raise ``skipped_existing``
    without a ``raw_results`` row; the summary once showed "-3 classified".
    """
    from classify_job import _classify_photos, _classify_summary_parts

    photos = _add_photos(db, 3)
    detection_map = {}
    for photo in photos:
        det = db.save_detections(
            photo["id"],
            [{"box": BOX, "confidence": 0.9, "category": "animal"}],
            detector_model="megadetector-v6",
        )[0]
        db.record_classifier_run(det, "BioCLIP", "legacy", prediction_count=0)
        db.record_classifier_match_score(
            det, "BioCLIP", "legacy", max_match_score=0.1, match_margin=None,
            top_species="x", label_count=10, score_kind="cosine",
        )
        detection_map[photo["id"]] = [{"id": det}]

    raw, failed, skipped = _classify_photos(
        photos, {db.folder_id: str(tmp_path)}, detection_map, set(),
        MagicMock(), "bioclip", "BioCLIP", _Runner(), _job(), db,
        labels_fingerprint="legacy", reclassify=False,
    )

    assert (len(raw), skipped) == (0, 3)
    assert _classify_summary_parts(raw, skipped, failed) == [
        "0 classified", "3 cached",
    ]
    assert _classify_summary_parts(
        [{"_existing": True}, {}], 2, 1,
    ) == ["1 classified", "2 cached", "1 failed"]


def test_fresh_detection_classifies_only_confident_animal_boxes(db, tmp_path):
    """A fresh pass must classify the same boxes a cached rerun reads back:
    animal boxes at or above ``detector_confidence``. Noise and person boxes
    once got species predictions on the first pass only.
    """
    from classify_job import _classify_photos, _detect_subjects

    photos = _add_photos(db, 2)
    animal_photo, person_photo = photos
    boxes = {
        "p0.jpg": [
            {"box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.3},
             "confidence": 0.9, "category": "animal"},
            {"box": {"x": 0.6, "y": 0.6, "w": 0.2, "h": 0.2},
             "confidence": 0.03, "category": "animal"},
            {"box": {"x": 0.5, "y": 0.1, "w": 0.2, "h": 0.4},
             "confidence": 0.85, "category": "person"},
        ],
        "p1.jpg": [
            {"box": {"x": 0.2, "y": 0.2, "w": 0.4, "h": 0.4},
             "confidence": 0.8, "category": "person"},
        ],
    }
    folders = {db.folder_id: str(tmp_path)}
    job = _job()
    detection_map, detected = _run_patched(
        _detector_patches(
            lambda path: [dict(b) for b in boxes[path.rsplit("/", 1)[-1]]]
        ),
        lambda: _detect_subjects(photos, folders, _Runner(), job, False, db),
    )

    assert detected == 1
    assert [
        (d["confidence"], d["category"]) for d in detection_map[animal_photo["id"]]
    ] == [(0.9, "animal")]
    assert person_photo["id"] not in detection_map
    assert job["_non_animal_photo_ids"] == {person_photo["id"]}

    clf = _sparrow_clf()
    with patch("classify_job._prepare_image",
               lambda *a, **k: (MagicMock(info={}), str(tmp_path), "x")):
        raw, _, _ = _classify_photos(
            photos, folders, detection_map, set(), clf, "bioclip", "BioCLIP",
            _Runner(), job, db, labels_fingerprint="fp",
        )
    # The person-only photo is not classified as a full frame either.
    assert [r["photo"]["id"] for r in raw] == [animal_photo["id"]]

    cached_map, _ = _run_patched(
        _detector_patches(lambda path: pytest.fail("cached rerun re-detected")),
        lambda: _detect_subjects(photos, folders, _Runner(), _job("j2"), False, db),
    )
    assert [d["id"] for d in cached_map[animal_photo["id"]]] == [
        d["id"] for d in detection_map[animal_photo["id"]]
    ]


def test_stop_during_detection_keeps_committed_boxes(db, tmp_path):
    """``ResourceWaitCancelled`` subclasses ``RuntimeError``; it was caught
    as "Detection unavailable", which logged a false error and discarded the
    boxes already committed, sending those photos to full-image
    classification alongside their real detections.
    """
    from classify_job import _classify_photos, _detect_subjects

    photos = _add_photos(db, 2)
    state = {"cancel": False}

    def detect(path):
        if path.endswith("p1.jpg"):
            state["cancel"] = True
            raise ResourceWaitCancelled("cancelled waiting for cpu_ml")
        return [{"box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.3},
                 "confidence": 0.9, "category": "animal"}]

    runner = _Runner(lambda job_id: state["cancel"])
    folders = {db.folder_id: str(tmp_path)}
    job = _job()
    detection_map, detected = _run_patched(
        _detector_patches(detect),
        lambda: _detect_subjects(photos, folders, runner, job, True, db),
    )

    first = photos[0]["id"]
    assert detected == 1
    assert list(detection_map) == [first]
    assert job["errors"] == []
    assert job["_detect_cancelled"] is True

    processed = set(job["_detect_processed_ids"]) | set(detection_map)
    todo = [p for p in photos if p["id"] in processed]
    with patch("classify_job._prepare_image",
               lambda *a, **k: (MagicMock(info={}), str(tmp_path), "x")):
        raw, _, _ = _classify_photos(
            todo, folders, detection_map, set(), _sparrow_clf(), "bioclip",
            "BioCLIP", runner, job, db, labels_fingerprint="fp",
            reclassify=True, finish_cleared_only=True,
        )

    models = {
        db.conn.execute(
            "SELECT detector_model FROM detections WHERE id = ?",
            (r["detection_id"],),
        ).fetchone()[0]
        for r in raw
    }
    assert models == {"megadetector-v6"}
    assert [d["detector_model"] for d in db.get_detections(first, min_conf=0)] == [
        "megadetector-v6"
    ]


def test_stop_during_subject_analysis_keeps_in_flight_photo(db, tmp_path):
    """A Stop raised by the subject-analysis checkpoint lands after the
    photo's detections were replaced. ``_detect_batch`` unwound past its
    return, so the in-flight photo was missing from the processed set and
    the reclassify cancel recovery never rebuilt its predictions.
    """
    from classify_job import _detect_subjects

    photos = _add_photos(db, 2)
    state = {"cancel": False}

    def analyze(db_, photo_id, image_path, *, checkpoint, **kwargs):
        state["cancel"] = True
        checkpoint()

    patches = _detector_patches(
        lambda path: [{"box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.3},
                       "confidence": 0.9, "category": "animal"}]
    )
    patches[-1] = patch("subjects.analyze_photo", analyze)
    runner = _Runner(lambda job_id: state["cancel"])
    job = _job()
    detection_map, detected = _run_patched(
        patches,
        lambda: _detect_subjects(
            photos, {db.folder_id: str(tmp_path)}, runner, job, True, db,
        ),
    )

    first = photos[0]["id"]
    assert job["_detect_cancelled"] is True
    assert job["errors"] == []
    assert set(job["_detect_processed_ids"]) == {first}
    assert list(detection_map) == [first]
    assert detected == 1


def test_stop_during_detector_setup_returns_empty_map(db, tmp_path):
    """A Stop raised before the detection loop starts must still reach the
    cancel arm with a defined (empty) result."""
    from classify_job import _detect_subjects

    photos = _add_photos(db, 1)

    def cancelled_download(progress_callback=None):
        raise ResourceWaitCancelled("cancelled waiting for download slot")

    patches = _detector_patches(lambda path: pytest.fail("detector ran"))
    patches[3] = patch("detector.ensure_megadetector_weights", cancelled_download)
    job = _job()
    detection_map, detected = _run_patched(
        patches,
        lambda: _detect_subjects(
            photos, {db.folder_id: str(tmp_path)}, _Runner(), job, True, db,
        ),
    )

    assert (detection_map, detected) == ({}, 0)
    assert job["_detect_cancelled"] is True
    assert job["errors"] == []


def test_failed_redetection_reclassifies_stored_boxes(db, tmp_path):
    """A reclassify photo whose redetection fails keeps its stored boxes,
    but it was left out of ``detection_map``: the classify loop then ran an
    unscoped predictions clear and classified a synthetic full-image box,
    wiping the box predictions (other fingerprints included) the detection
    path had deliberately left intact.
    """
    from classify_job import _classify_photos, _detect_subjects

    photos = _add_photos(db, 2)
    stored = {}
    for photo in photos:
        det = db.save_detections(
            photo["id"],
            [{"box": BOX, "confidence": 0.9, "category": "animal"}],
            detector_model="megadetector-v6",
        )[0]
        db.record_detector_run(photo["id"], "megadetector-v6", box_count=1)
        db.add_prediction(det, species="Robin", confidence=0.9,
                          model="BioCLIP", labels_fingerprint="legacy")
        db.add_prediction(det, species="Robin", confidence=0.8,
                          model="BioCLIP", labels_fingerprint="other-ws")
        stored[photo["id"]] = det

    def detect(path):
        if path.endswith("p0.jpg"):
            return None  # decode/ONNX failure reported by detect_animals
        raise RuntimeError("per-photo detector error")  # swallowed by batch

    folders = {db.folder_id: str(tmp_path)}
    job = _job()
    detection_map, _ = _run_patched(
        _detector_patches(detect),
        lambda: _detect_subjects(photos, folders, _Runner(), job, True, db),
    )

    assert {pid: [d["id"] for d in dets] for pid, dets in detection_map.items()} == {
        pid: [det] for pid, det in stored.items()
    }
    assert job["_detect_processed_ids"] == set()
    assert job["_detect_reused_ids"] == set(stored)

    with patch("classify_job._prepare_image",
               lambda *a, **k: (MagicMock(info={}), str(tmp_path), "x")):
        raw, failed, _ = _classify_photos(
            photos, folders, detection_map, set(), _sparrow_clf(), "bioclip",
            "BioCLIP", _Runner(), job, db, labels_fingerprint="legacy",
            reclassify=True,
        )

    assert failed == 0
    assert {r["detection_id"] for r in raw} == set(stored.values())
    for photo in photos:
        # No synthetic full-image anchor was created.
        assert [
            d["detector_model"]
            for d in db.get_detections(photo["id"], min_conf=0)
        ] == ["megadetector-v6"]
        # The other workspace's fingerprint on the same box survives.
        assert db.conn.execute(
            "SELECT COUNT(*) FROM predictions WHERE detection_id = ? "
            "AND labels_fingerprint = 'other-ws'",
            (stored[photo["id"]],),
        ).fetchone()[0] == 1


def test_flush_batch_propagates_cancel_instead_of_counting_failures():
    """A Stop during batch inference once fell back to per-image
    classification and reported every image as a failure."""
    from classify_job import _flush_batch

    clf = MagicMock()
    clf.classify_batch_with_embedding.side_effect = ResourceWaitCancelled("x")
    clf.classify_with_embedding.side_effect = ResourceWaitCancelled("x")
    batch = [
        {"img": MagicMock(), "detection_id": i, "folder_path": "",
         "image_path": "",
         "photo": {"id": i, "filename": f"p{i}.jpg", "timestamp": None}}
        for i in range(16)
    ]

    with pytest.raises(ResourceWaitCancelled):
        _flush_batch(batch, clf, "bioclip", "BioCLIP", None, [],
                     propagate_cancel=True)
    for entry in batch:
        entry["img"].close.assert_called_once()
    clf.classify_with_embedding.assert_not_called()


def test_classify_photos_stops_cleanly_on_cancel_during_flush(db, tmp_path):
    """A cancel raised by a full batch's inference stops both loops, records
    no failures or classifier runs for the batch, and marks the run
    cancelled."""
    import classify_job
    from classify_job import _classify_photos

    photos = _add_photos(db, classify_job._BATCH_SIZE + 3)
    detection_map = {}
    for photo in photos:
        det = db.save_detections(
            photo["id"],
            [{"box": BOX, "confidence": 0.9, "category": "animal"}],
            detector_model="megadetector-v6",
        )[0]
        detection_map[photo["id"]] = [{"id": det}]

    clf = MagicMock()
    clf.classify_batch_with_embedding.side_effect = ResourceWaitCancelled("x")
    prepared = []

    def prepare(photo, *a, **k):
        prepared.append(photo["id"])
        return MagicMock(info={}), str(tmp_path), "x"

    job = _job()
    with patch("classify_job._prepare_image", prepare):
        raw, failed, skipped = _classify_photos(
            photos, {db.folder_id: str(tmp_path)}, detection_map, set(), clf,
            "bioclip", "BioCLIP", _Runner(), job, db,
            labels_fingerprint="legacy", reclassify=False,
        )

    assert (raw, failed, skipped) == ([], 0, 0)
    assert job["_classify_cancelled"] is True
    assert len(prepared) == classify_job._BATCH_SIZE
    clf.classify_with_embedding.assert_not_called()
    assert db.conn.execute("SELECT COUNT(*) FROM classifier_runs").fetchone()[0] == 0


def test_non_animal_skip_retires_stale_full_image_prediction(db, tmp_path):
    """A photo with a prior full-image species prediction that now shows a
    confident person/vehicle box must not keep surfacing that prediction.

    Before the fix, ``_classify_photos`` skipped ``non_animal_ids`` photos
    with a bare ``continue``. MegaDetector writes only replace the
    ``megadetector-v6`` rows, so the older ``full-image`` detection and its
    cascaded species predictions stayed intact and reads returned them under
    the newest fingerprint per detection.
    """
    from classify_job import _classify_photos, _detect_subjects

    photos = _add_photos(db, 1)
    photo = photos[0]
    full_det = db.save_detections(
        photo["id"],
        [{"box": BOX, "confidence": 0, "category": "animal"}],
        detector_model="full-image",
    )[0]
    db.record_detector_run(photo["id"], "full-image", box_count=1)
    db.add_prediction(full_det, species="Robin", confidence=0.9,
                      model="BioCLIP", labels_fingerprint="legacy")

    folders = {db.folder_id: str(tmp_path)}
    job = _job()
    detection_map, detected = _run_patched(
        _detector_patches(
            lambda path: [{"box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.3},
                           "confidence": 0.85, "category": "person"}]
        ),
        lambda: _detect_subjects(photos, folders, _Runner(), job, False, db),
    )

    assert detected == 0
    assert photo["id"] not in detection_map
    assert job["_non_animal_photo_ids"] == {photo["id"]}
    # Sanity: the stale full-image prediction is still on disk pre-classify.
    assert _prediction_count(db, photo["id"], "BioCLIP") == 1

    clf = _sparrow_clf()
    with patch("classify_job._prepare_image",
               lambda *a, **k: (MagicMock(info={}), str(tmp_path), "x")):
        raw, _, _ = _classify_photos(
            photos, folders, detection_map, set(), clf, "bioclip", "BioCLIP",
            _Runner(), job, db, labels_fingerprint="fp", reclassify=False,
        )

    # No classification ran for the skipped photo.
    assert raw == []
    clf.classify_batch_with_embedding.assert_not_called()
    clf.classify_with_embedding.assert_not_called()
    # The stale full-image detection AND its cascaded predictions are gone;
    # the freshly-written MegaDetector person box stays.
    assert _prediction_count(db, photo["id"]) == 0
    assert sorted({
        d["detector_model"]
        for d in db.get_detections(photo["id"], min_conf=0)
    }) == ["megadetector-v6"]
