"""Person/vehicle detector regions the classifier recognizes as animals.

MegaDetector labels some animals "person" or "vehicle". On a photo with no
retained animal region, classification examines the strongest such region
once per (model, label set) and keeps it as an animal subject when the
classifier clearly recognizes a listed species in it.
"""
from unittest.mock import MagicMock

import background_match
import pytest
from classify_job.candidate_subjects import (
    examine_candidates,
    kept_regions,
    mark_kept_regions,
)
from classify_job.classify_pass import ClassifierModel
from services import detection_store, photo_ingest, workspace_folders

FLOOR = 0.2
PERSON_BOX = {"x": 0.1, "y": 0.2, "w": 0.5, "h": 0.6}


@pytest.fixture
def db(tmp_path, monkeypatch):
    import config as cfg
    from db import Database

    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    database = Database(str(tmp_path / "test.db"))
    ws = database.ensure_default_workspace()
    database.commit()
    database.set_active_workspace(ws)
    database.folder_id = workspace_folders.add_folder(database, str(tmp_path), name="p")
    return database


def _photo(db, name):
    pid = photo_ingest.add_photo(
        db, db.folder_id, name, extension=".jpg", file_size=100, file_mtime=1.0,
    )
    db.commit()
    return {"id": pid, "filename": name, "folder_id": db.folder_id, "timestamp": None}


def _detect(db, photo, detections, runtime="rt1"):
    ids = detection_store.write_detection_batch(
        db, photo["id"], "megadetector-v6", detections, runtime_fingerprint=runtime,
    )
    db.commit()
    return ids


def _region(category, confidence, box=PERSON_BOX):
    return {"box": box, "confidence": confidence, "category": category}


def _clf(answers):
    """A classifier whose answer per crop is looked up by the crop's tag."""
    clf = MagicMock()

    def classify(images, threshold=0):
        out = []
        for img in images:
            match, background = answers[img.tag]
            out.append(([
                {"species": "Wood Duck", "score": 0.9, "raw_score": match,
                 "background_score": background, "background_prompt": "water"},
                {"species": "Mallard", "score": 0.1, "raw_score": match - 0.2},
            ], None))
        return out

    clf.classify_batch_with_embedding.side_effect = classify
    return clf


def _crop(tag):
    img = MagicMock()
    img.tag = tag
    return img


def _model(clf, fp="fp1", model_type="bioclip"):
    return ClassifierModel(
        clf=clf, model_type=model_type, model_name="BioCLIP", labels_fingerprint=fp,
    )


def _category(db, detection_id):
    return db.conn.execute(
        "SELECT category FROM detections WHERE id = ?", (detection_id,),
    ).fetchone()["category"]


# -- the rule ----------------------------------------------------------


@pytest.mark.parametrize(("match", "background", "expected"), [
    (0.55, 0.20, True),    # a Wood Duck on plain water
    (0.46, 0.18, True),    # exactly at both thresholds
    (0.45, 0.10, False),   # below the match score, however wide the gap
    (0.37, 0.26, False),   # a car: no species fits
    (0.50, 0.25, False),   # a trunk named as the warbler that lives on it
    (0.55, None, False),   # no background prompts measured
    (None, 0.20, False),
])
def test_recognized_animal_needs_a_strong_match_and_a_wide_gap(match, background, expected):
    assert background_match.is_recognized_animal(match, background) is expected


def test_recognized_animal_needs_cosine_scores():
    assert not background_match.is_recognized_animal(0.9, 0.1, "logit")


# -- which regions are candidates --------------------------------------


def test_the_strongest_person_or_vehicle_region_is_the_candidate(db):
    photo = _photo(db, "duck.jpg")
    weak, strong, _noise = _detect(db, photo, [
        _region("vehicle", 0.31, {"x": 0, "y": 0, "w": 0.2, "h": 0.2}),
        _region("person", 0.44),
        _region("animal", 0.02, {"x": 0.5, "y": 0.6, "w": 0.1, "h": 0.1}),
    ])

    pending = db.candidate_subjects.pending([photo["id"]], FLOOR, "BioCLIP", "fp1")

    assert [row["id"] for row in pending] == [strong]
    assert pending[0]["category"] == "person"
    assert weak != strong


def test_a_photo_with_a_retained_animal_region_has_no_candidate(db):
    photo = _photo(db, "heron.jpg")
    _detect(db, photo, [
        _region("person", 0.9),
        _region("animal", 0.3, {"x": 0.5, "y": 0.5, "w": 0.2, "h": 0.2}),
    ])

    assert db.candidate_subjects.pending([photo["id"]], FLOOR, "BioCLIP", "fp1") == []


def test_a_region_below_the_floor_or_rejected_by_the_reviewer_is_not_a_candidate(db):
    faint = _photo(db, "faint.jpg")
    _detect(db, faint, [_region("person", 0.15)])
    rejected = _photo(db, "rejected.jpg")
    (rejected_id,) = _detect(db, rejected, [_region("person", 0.8)])
    db.detections.set_subject_verdict(rejected_id, "not_animal")
    db.commit()

    assert db.candidate_subjects.pending(
        [faint["id"], rejected["id"]], FLOOR, "BioCLIP", "fp1",
    ) == []


# -- examining them ----------------------------------------------------


def test_a_recognized_region_becomes_an_animal_region_and_a_car_does_not(db):
    duck, car = _photo(db, "duck.jpg"), _photo(db, "car.jpg")
    (duck_region,) = _detect(db, duck, [_region("person", 0.44)])
    (car_region,) = _detect(db, car, [_region("vehicle", 0.9)])
    clf = _clf({duck["id"]: (0.55, 0.20), car["id"]: (0.37, 0.26)})

    outcome = examine_candidates(
        db, [duck, car], _model(clf), min_conf=FLOOR,
        prepare_crop=lambda photo, region: _crop(photo["id"]),
    )

    assert outcome.examined == 2
    assert outcome.kept_photo_ids == {duck["id"]}
    assert _category(db, duck_region) == "animal"
    assert _category(db, car_region) == "vehicle"
    # The duck's region now reads like any animal region the detector kept.
    assert [d["id"] for d in detection_store.get_detections(db, duck["id"], min_conf=FLOOR)
            if d["category"] == "animal"] == [duck_region]
    evidence = db.candidate_subjects.evidence([duck_region, car_region])
    assert evidence[duck_region]["detector_category"] == "person"
    assert evidence[duck_region]["promoted"] == 1
    assert evidence[duck_region]["top_species"] == "Wood Duck"
    assert evidence[car_region]["promoted"] == 0


def test_an_examined_region_is_not_read_again_until_the_label_set_changes(db):
    car = _photo(db, "car.jpg")
    _detect(db, car, [_region("vehicle", 0.9)])
    clf = _clf({car["id"]: (0.37, 0.26)})
    crops = []

    def prepare(photo, region):
        crops.append(region["id"])
        return _crop(photo["id"])

    for _ in range(2):
        examine_candidates(db, [car], _model(clf), min_conf=FLOOR, prepare_crop=prepare)
    assert len(crops) == 1

    examine_candidates(db, [car], _model(clf, fp="fp2"), min_conf=FLOOR, prepare_crop=prepare)
    assert len(crops) == 2

    examine_candidates(
        db, [car], _model(clf, fp="fp2"), min_conf=FLOOR, prepare_crop=prepare, recheck=True,
    )
    assert len(crops) == 3


def test_an_unreadable_crop_leaves_the_region_pending(db):
    duck = _photo(db, "duck.jpg")
    (region,) = _detect(db, duck, [_region("person", 0.44)])

    outcome = examine_candidates(
        db, [duck], _model(_clf({})), min_conf=FLOOR,
        prepare_crop=lambda photo, region: None,
    )

    assert (outcome.examined, outcome.unread) == (0, 1)
    assert [r["id"] for r in db.candidate_subjects.pending(
        [duck["id"]], FLOOR, "BioCLIP", "fp1",
    )] == [region]


def test_a_classifier_without_raw_scores_keeps_nothing(db):
    duck = _photo(db, "duck.jpg")
    (region,) = _detect(db, duck, [_region("person", 0.44)])
    clf = MagicMock()
    clf.classify_batch_with_embedding.side_effect = lambda images, threshold=0: [
        ([{"species": "Wood Duck", "score": 0.99}], None) for _ in images
    ]

    outcome = examine_candidates(
        db, [duck], _model(clf), min_conf=FLOOR,
        prepare_crop=lambda photo, region: _crop(photo["id"]),
    )

    assert outcome.kept_photo_ids == set()
    assert _category(db, region) == "person"
    assert db.candidate_subjects.evidence([region]) == {}


def test_a_timm_classifier_examines_nothing(db):
    duck = _photo(db, "duck.jpg")
    _detect(db, duck, [_region("person", 0.44)])
    clf = MagicMock()

    outcome = examine_candidates(
        db, [duck], _model(clf, model_type="timm"), min_conf=FLOOR,
        prepare_crop=lambda photo, region: _crop(photo["id"]),
    )

    assert outcome.examined == 0
    clf.classify_batch.assert_not_called()


def test_stop_ends_the_pass_between_regions(db):
    photos = [_photo(db, f"p{i}.jpg") for i in range(3)]
    for photo in photos:
        _detect(db, photo, [_region("person", 0.44)])
    clf = _clf({photo["id"]: (0.55, 0.20) for photo in photos})
    seen = []

    outcome = examine_candidates(
        db, photos, _model(clf), min_conf=FLOOR,
        prepare_crop=lambda photo, region: seen.append(photo["id"]) or _crop(photo["id"]),
        should_stop=lambda: len(seen) >= 1,
    )

    assert outcome.examined == 1


# -- a kept region survives the detector writing it again ---------------


def _keep(db, photo, region_id):
    examine_candidates(
        db, [photo], _model(_clf({photo["id"]: (0.55, 0.20)})), min_conf=FLOOR,
        prepare_crop=lambda photo, region: _crop(photo["id"]),
    )
    assert _category(db, region_id) == "animal"


def test_the_same_detector_output_written_again_keeps_the_region_an_animal(db):
    duck = _photo(db, "duck.jpg")
    (region,) = _detect(db, duck, [_region("person", 0.44)])
    _keep(db, duck, region)

    assert _detect(db, duck, [_region("person", 0.44)]) == [region]

    assert _category(db, region) == "animal"


def test_a_new_detector_runtime_that_reproduces_the_region_keeps_it(db):
    duck = _photo(db, "duck.jpg")
    (region,) = _detect(db, duck, [_region("person", 0.44)])
    _keep(db, duck, region)

    assert _detect(db, duck, [_region("person", 0.47)], runtime="rt2") == [region]

    assert _category(db, region) == "animal"
    assert db.candidate_subjects.evidence([region])[region]["promoted"] == 1


def test_a_new_detector_runtime_that_moves_the_region_asks_again(db):
    duck = _photo(db, "duck.jpg")
    (region,) = _detect(db, duck, [_region("person", 0.44)])
    _keep(db, duck, region)

    moved = {"x": 0.12, "y": 0.2, "w": 0.5, "h": 0.6}
    (new_region,) = _detect(db, duck, [_region("person", 0.44, moved)], runtime="rt2")

    assert new_region != region
    assert _category(db, new_region) == "person"
    assert [r["id"] for r in db.candidate_subjects.pending(
        [duck["id"]], FLOOR, "BioCLIP", "fp1",
    )] == [new_region]


def test_a_later_check_that_disagrees_does_not_withdraw_a_kept_region(db):
    duck = _photo(db, "duck.jpg")
    (region,) = _detect(db, duck, [_region("person", 0.44)])
    _keep(db, duck, region)

    with db.transaction():
        db.candidate_subjects.record(
            region,
            {"detector_category": "person", "classifier_model": "BioCLIP",
             "labels_fingerprint": "fp2", "max_match_score": 0.3,
             "background_score": 0.3},
            promoted=False,
        )

    assert db.candidate_subjects.evidence([region])[region]["promoted"] == 1
    assert _category(db, region) == "animal"


# -- the detect phase's in-memory boxes ---------------------------------


def test_kept_regions_replace_the_detectors_category_in_a_detect_phase_copy(db):
    duck, car = _photo(db, "duck.jpg"), _photo(db, "car.jpg")
    (duck_region,) = _detect(db, duck, [_region("person", 0.44)])
    (car_region,) = _detect(db, car, [_region("vehicle", 0.9)])
    examine_candidates(
        db, [duck, car],
        _model(_clf({duck["id"]: (0.55, 0.20), car["id"]: (0.37, 0.26)})),
        min_conf=FLOOR, prepare_crop=lambda photo, region: _crop(photo["id"]),
    )
    fresh = {
        duck["id"]: [{"id": duck_region, "category": "person"}],
        car["id"]: [{"id": car_region, "category": "vehicle"}],
    }

    mark_kept_regions(db, fresh)

    assert fresh[duck["id"]][0]["category"] == "animal"
    assert fresh[car["id"]][0]["category"] == "vehicle"
    kept = kept_regions(db, [duck["id"], car["id"]])
    assert list(kept) == [duck["id"]]
    assert kept[duck["id"]][0]["id"] == duck_region
    assert kept[duck["id"]][0]["confidence"] == pytest.approx(0.44)


# -- the standalone classify job ----------------------------------------


def _job_run(db, photos, non_animal_ids, clf, tmp_path):
    from classify_job.detection import DetectionHandoff
    from classify_job.job_run import _ClassifyJobRun

    run = object.__new__(_ClassifyJobRun)
    run.db = db
    run.runner = MagicMock()
    run.runner.is_cancelled.return_value = False
    run.job = {"id": "classify-test"}
    run.photos = photos
    run.folders = {db.folder_id: str(tmp_path)}
    run.vireo_dir = None
    run.params = MagicMock(reclassify=False)
    run.clf, run.model_type, run.model_name, run.fp = clf, "bioclip", "BioCLIP", "fp1"
    run.detect_handoff = DetectionHandoff(non_animal_ids=set(non_animal_ids))
    run.detection_map = {}
    return run


def test_the_classify_job_moves_a_kept_photo_out_of_its_skip_set(db, tmp_path, monkeypatch):
    import classify_job.classify_pass as classify_pass

    duck, car = _photo(db, "duck.jpg"), _photo(db, "car.jpg")
    (duck_region,) = _detect(db, duck, [_region("person", 0.44)])
    _detect(db, car, [_region("vehicle", 0.9)])
    monkeypatch.setattr(
        classify_pass, "_prepare_image",
        lambda photo, folders, detection, vireo_dir=None: (_crop(photo["id"]), "", ""),
    )
    run = _job_run(
        db, [duck, car], {duck["id"], car["id"]},
        _clf({duck["id"]: (0.55, 0.20), car["id"]: (0.37, 0.26)}), tmp_path,
    )

    run.keep_recognized_candidates()

    assert run.detect_handoff.non_animal_ids == {car["id"]}
    assert [d["id"] for d in run.detection_map[duck["id"]]] == [duck_region]
    assert run.detection_map[duck["id"]][0]["category"] == "animal"
    assert car["id"] not in run.detection_map


# -- the portable computation cache -------------------------------------


def test_the_portable_cache_reads_a_kept_regions_detector_category(db):
    """An artifact's box maps back to its detection id through the category,
    and the id was derived from the detector's own label."""
    from detection_id import detection_id

    duck = _photo(db, "duck.jpg")
    (region,) = _detect(db, duck, [_region("person", 0.44)])
    _keep(db, duck, region)

    (box,) = db.computation_cache.run_detection_boxes(duck["id"], "megadetector-v6", "rt1")
    identity = db.computation_cache.promotable_detection(region)

    assert box["category"] == "person"
    assert identity["category"] == "person"
    assert detection_id(
        duck["id"], "megadetector-v6",
        (box["box_x"], box["box_y"], box["box_w"], box["box_h"]), box["category"],
    ) == region


# -- RAW+JPEG pairing ----------------------------------------------------


def test_pairing_moves_a_kept_region_to_the_primary_and_keeps_it_kept(db):
    """Pairing re-keys the JPEG's detections onto the RAW. A kept region's
    new id derives from the detector's own label, so the detector writing
    the same box on the RAW lands on that row and leaves it a subject."""
    from detection_id import detection_id
    from scanner.companions import _pair_raw_jpeg_companions

    jpeg = _photo(db, "IMG_1.jpg")
    raw_id = photo_ingest.add_photo(
        db, db.folder_id, "IMG_1.cr3", extension=".cr3", file_size=1, file_mtime=1.0,
    )
    db.commit()
    (region,) = _detect(db, jpeg, [_region("person", 0.44)])
    _keep(db, jpeg, region)

    _pair_raw_jpeg_companions(db)
    db.commit()

    box = PERSON_BOX
    moved = detection_id(
        raw_id, "megadetector-v6", (box["x"], box["y"], box["w"], box["h"]), "person",
    )
    rows = db.conn.execute(
        "SELECT id, category FROM detections WHERE photo_id = ?", (raw_id,),
    ).fetchall()
    assert [(row["id"], row["category"]) for row in rows] == [(moved, "animal")]
    assert db.candidate_subjects.evidence([moved])[moved]["promoted"] == 1

    raw = {"id": raw_id, "filename": "IMG_1.cr3", "folder_id": db.folder_id}
    assert _detect(db, raw, [_region("person", 0.44)]) == [moved]
    assert _category(db, moved) == "animal"


# -- one check per (region, model, label set) ----------------------------


def _reject(db, region, model="BioCLIP", fp="fp1", category="vehicle"):
    with db.transaction():
        assert db.candidate_subjects.record(
            region,
            {"detector_category": category, "classifier_model": model,
             "labels_fingerprint": fp, "max_match_score": 0.3,
             "background_score": 0.2},
            promoted=False,
        )


def test_alternating_models_and_label_sets_each_keep_their_own_check(db):
    """Model A's answer survives model B's, so a multi-model run or a user
    switching label sets does not read the region again every time."""
    car = _photo(db, "car.jpg")
    (region,) = _detect(db, car, [_region("vehicle", 0.8)])

    _reject(db, region)
    _reject(db, region, model="OtherClassifier")
    _reject(db, region, fp="fp2")

    for model, fp in (("BioCLIP", "fp1"), ("OtherClassifier", "fp1"), ("BioCLIP", "fp2")):
        assert db.candidate_subjects.pending([car["id"]], FLOOR, model, fp) == []
    assert [r["id"] for r in db.candidate_subjects.pending(
        [car["id"]], FLOOR, "OtherClassifier", "fp2",
    )] == [region]


def test_evidence_reports_the_check_that_kept_the_region(db):
    duck = _photo(db, "duck.jpg")
    (region,) = _detect(db, duck, [_region("person", 0.44)])
    _keep(db, duck, region)
    _reject(db, region, model="OtherClassifier", category="person")

    check = db.candidate_subjects.evidence([region])[region]

    assert check["promoted"] == 1
    assert check["classifier_model"] == "BioCLIP"
    assert check["detector_category"] == "person"


def test_a_rejected_check_survives_a_new_detector_runtime_that_reproduces_the_region(db):
    car = _photo(db, "car.jpg")
    (region,) = _detect(db, car, [_region("vehicle", 0.8)])
    _reject(db, region)

    assert _detect(db, car, [_region("vehicle", 0.8)], runtime="rt2") == [region]

    assert db.candidate_subjects.pending([car["id"]], FLOOR, "BioCLIP", "fp1") == []
    assert _category(db, region) == "vehicle"


def test_a_region_retired_during_inference_is_not_recorded(db):
    car = _photo(db, "car.jpg")
    (region,) = _detect(db, car, [_region("vehicle", 0.8)])

    def prepare(photo, detection):
        with db.transaction():
            db.detections.clear(photo["id"], detector_model="megadetector-v6")
        return _crop(photo["id"])

    outcome = examine_candidates(
        db, [car], _model(_clf({car["id"]: (0.55, 0.20)})), min_conf=FLOOR,
        prepare_crop=prepare,
    )

    assert outcome.examined == 0
    assert outcome.kept_photo_ids == set()
    assert db.candidate_subjects.evidence([region]) == {}


def test_stop_while_waiting_for_an_inference_slot_ends_the_pass_quietly(db, monkeypatch):
    import classify_job.candidate_subjects as candidates
    from resource_ledger import ResourceWaitCancelled

    car = _photo(db, "car.jpg")
    (region,) = _detect(db, car, [_region("vehicle", 0.8)])

    def stopped(*args, **kwargs):
        raise ResourceWaitCancelled("stop")

    monkeypatch.setattr(candidates, "_classify_batch_images", stopped)
    crop = _crop(car["id"])

    outcome = examine_candidates(
        db, [car], _model(_clf({})), min_conf=FLOOR,
        prepare_crop=lambda photo, region: crop,
    )

    assert (outcome.examined, outcome.unread) == (0, 0)
    crop.close.assert_called_once()
    assert [r["id"] for r in db.candidate_subjects.pending(
        [car["id"]], FLOOR, "BioCLIP", "fp1",
    )] == [region]


# -- a kept region the reviewer rejects ----------------------------------


def test_a_kept_region_the_reviewer_rejects_is_no_longer_a_classify_target(db, tmp_path, monkeypatch):
    """The photo also has a retained vehicle region, so it is in the skip
    set; the rejected region must not pull it back out."""
    import classify_job.classify_pass as classify_pass

    duck = _photo(db, "duck.jpg")
    kept, _other = _detect(db, duck, [
        _region("person", 0.44),
        _region("vehicle", 0.30, {"x": 0.7, "y": 0.7, "w": 0.2, "h": 0.2}),
    ])
    _keep(db, duck, kept)
    db.detections.set_subject_verdict(kept, "not_animal")
    db.commit()
    monkeypatch.setattr(
        classify_pass, "_prepare_image",
        lambda photo, folders, detection, vireo_dir=None: (_crop("other"), "", ""),
    )
    run = _job_run(db, [duck], {duck["id"]}, _clf({"other": (0.37, 0.26)}), tmp_path)

    run.keep_recognized_candidates()

    assert kept_regions(db, [duck["id"]]) == {}
    assert run.detect_handoff.non_animal_ids == {duck["id"]}
    assert run.detection_map == {}


def test_the_classify_job_counts_a_region_it_could_not_read(db, tmp_path, monkeypatch):
    import classify_job.classify_pass as classify_pass

    duck = _photo(db, "duck.jpg")
    _detect(db, duck, [_region("person", 0.44)])
    monkeypatch.setattr(
        classify_pass, "_prepare_image",
        lambda photo, folders, detection, vireo_dir=None: (None, "", ""),
    )
    run = _job_run(db, [duck], {duck["id"]}, _clf({}), tmp_path)

    run.keep_recognized_candidates()

    assert run.unread_candidates == 1
    assert run.detect_handoff.non_animal_ids == {duck["id"]}


# -- pairing onto a primary that already holds the region -----------------


@pytest.mark.parametrize("primary_checked", [False, True])
def test_pairing_onto_a_primary_that_holds_the_same_region_keeps_it_kept(db, primary_checked):
    """The RAW already has the same person/vehicle box, with a rejecting
    check of its own or none. The JPEG's kept region must still end up a
    subject on the RAW."""
    from scanner.companions import _pair_raw_jpeg_companions

    jpeg = _photo(db, "IMG_1.jpg")
    raw_id = photo_ingest.add_photo(
        db, db.folder_id, "IMG_1.cr3", extension=".cr3", file_size=1, file_mtime=1.0,
    )
    db.commit()
    (primary,) = _detect(db, {"id": raw_id}, [_region("vehicle", 0.8)])
    if primary_checked:
        _reject(db, primary)
    (companion,) = _detect(db, jpeg, [_region("vehicle", 0.8)])
    _keep(db, jpeg, companion)

    _pair_raw_jpeg_companions(db)
    db.commit()

    assert _category(db, primary) == "animal"
    assert db.candidate_subjects.evidence([primary])[primary]["promoted"] == 1
    assert db.candidate_subjects.evidence([companion]) == {}


# -- the portable computation cache carries a kept region -----------------


def test_a_kept_regions_portable_subject_carries_the_check_that_kept_it(db):
    from computation_cache import _subject_for_detection

    duck = _photo(db, "duck.jpg")
    (region,) = _detect(db, duck, [_region("person", 0.44)])
    _keep(db, duck, region)

    subject = _subject_for_detection(
        "d0", db.computation_cache.promotable_detection(region), db, region,
    )

    assert subject["category"] == "person"
    assert subject["candidate_subject"]["detector_category"] == "person"
    assert subject["candidate_subject"]["max_match_score"] == pytest.approx(0.55)


def test_a_rejected_regions_portable_subject_carries_no_check(db):
    from computation_cache import _subject_for_detection

    car = _photo(db, "car.jpg")
    (region,) = _detect(db, car, [_region("vehicle", 0.8)])
    _reject(db, region)

    subject = _subject_for_detection(
        "d0", db.computation_cache.promotable_detection(region), db, region,
    )

    assert "candidate_subject" not in subject


def test_importing_a_kept_regions_classification_keeps_it_in_a_fresh_catalog(tmp_path):
    """The artifact names the detector's category so the box maps to its
    detection id; the check it carries makes the region a subject again."""
    from computation_cache import classification_input, materialize_artifacts
    from test_computation_cache import (
        CLASSIFIER_RUNTIME,
        PHOTO_HASH,
        RUNTIME,
        _database_with_photo,
        classification_artifact,
        detection_artifact,
    )

    destination, _, photo = _database_with_photo(tmp_path / "import.db", "cached.jpg")
    detection = detection_artifact()
    detection["subjects"][0]["category"] = "person"
    classification = classification_artifact(match={
        "max_match_score": 0.55, "background_score": 0.19,
        "background_prompt": "water", "score_kind": "cosine",
        "top_species": "Robin",
    })
    classification["subjects"][0]["category"] = "person"
    classification["subjects"][0]["candidate_subject"] = {
        "detector_category": "person", "classifier_model": "BioCLIP",
        "labels_fingerprint": "fp1", "max_match_score": 0.55,
        "background_score": 0.19, "top_species": "Robin",
    }
    classification["input"], classification["input_fingerprint"] = classification_input(
        PHOTO_HASH, RUNTIME, classification["subjects"],
    )

    result = materialize_artifacts(
        destination, [detection, classification], known_runtimes={RUNTIME},
        known_classifier_runtimes={CLASSIFIER_RUNTIME},
    )

    assert result["classifier_runs_applied"] == 1
    row = destination.conn.execute(
        "SELECT id, category FROM detections WHERE photo_id = ?", (photo,),
    ).fetchone()
    assert row["category"] == "animal"
    assert destination.candidate_subjects.evidence([row["id"]])[row["id"]]["promoted"] == 1
