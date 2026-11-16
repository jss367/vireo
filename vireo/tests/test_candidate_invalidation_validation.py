"""Independent model/label invalidation and reopen checks on synthetic catalogs."""
import sqlite3

import pytest
from repositories.candidate_subjects import CandidateSubjectRepository
from test_candidate_subjects import FLOOR, _detect, _photo, _region
from test_candidate_subjects import db as db


@pytest.mark.parametrize("model,labels", [("OtherClassifier", "fp1"), ("BioCLIP", "fp2")])
def test_negative_evidence_persists_but_new_identity_is_pending(db, model, labels):
    photo = _photo(db, "negative.jpg")
    (region,) = _detect(db, photo, [_region("vehicle", .8)])
    db.candidate_subjects.record(region, {
        "detector_category": "vehicle", "classifier_model": "BioCLIP",
        "labels_fingerprint": "fp1", "max_match_score": .3,
        "background_score": .2,
    }, promoted=False)
    db.commit()
    database_path = db.conn.execute("PRAGMA database_list").fetchone()[2]
    with sqlite3.connect(database_path) as reopened:
        reopened.row_factory = sqlite3.Row
        repo = CandidateSubjectRepository(reopened)
        assert repo.pending([photo["id"]], FLOOR, "BioCLIP", "fp1") == []
        assert [row["id"] for row in repo.pending([photo["id"]], FLOOR, model, labels)] == [region]
        assert repo.evidence([region])[region]["detector_category"] == "vehicle"


def _negative_check(db, region, model="BioCLIP", labels="fp1"):
    db.candidate_subjects.record(region, {
        "detector_category": "vehicle", "classifier_model": model,
        "labels_fingerprint": labels, "max_match_score": .3,
        "background_score": .2,
    }, promoted=False)
    db.commit()


def test_alternating_model_checks_do_not_reinfer_previous_identity(db):
    photo = _photo(db, "negative.jpg")
    (region,) = _detect(db, photo, [_region("vehicle", .8)])
    _negative_check(db, region)
    _negative_check(db, region, model="OtherClassifier")
    assert db.candidate_subjects.pending([photo["id"]], FLOOR, "BioCLIP", "fp1") == []


def test_unchanged_negative_region_keeps_check_across_runtime_change(db):
    photo = _photo(db, "negative.jpg")
    (region,) = _detect(db, photo, [_region("vehicle", .8)])
    _negative_check(db, region)
    assert _detect(db, photo, [_region("vehicle", .8)], runtime="rt2") == [region]
    assert db.candidate_subjects.pending([photo["id"]], FLOOR, "BioCLIP", "fp1") == []


def test_portable_person_subject_keeps_promotion_in_fresh_catalog(tmp_path):
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
        "max_match_score": .55, "background_score": .19,
        "background_prompt": "water", "score_kind": "cosine",
        "top_species": "Robin",
    })
    classification["subjects"][0]["category"] = "person"
    classification["subjects"][0]["candidate_subject"] = {
        "detector_category": "person", "classifier_model": "BioCLIP",
        "labels_fingerprint": "fp1", "max_match_score": .55,
        "background_score": .19, "top_species": "Robin",
    }
    classification["input"], classification["input_fingerprint"] = classification_input(
        PHOTO_HASH, RUNTIME, classification["subjects"],
    )
    result = materialize_artifacts(
        destination, [detection, classification], known_runtimes={RUNTIME},
        known_classifier_runtimes={CLASSIFIER_RUNTIME},
    )
    assert result["classifier_runs_applied"] == 1
    assert destination.conn.execute(
        "SELECT category FROM detections WHERE photo_id = ?", (photo,),
    ).fetchone()["category"] == "animal"


def test_stop_during_candidate_inference_is_not_a_failure(db, monkeypatch):
    import classify_job.candidate_subjects as candidates
    from resource_ledger import ResourceWaitCancelled
    from test_candidate_subjects import _clf, _crop, _model

    photo = _photo(db, "cancel.jpg")
    _detect(db, photo, [_region("vehicle", .8)])

    def stopped(*args, **kwargs):
        raise ResourceWaitCancelled("synthetic stop")

    monkeypatch.setattr(candidates, "_classify_batch_images", stopped)
    outcome = candidates.examine_candidates(
        db, [photo], _model(_clf({})), min_conf=FLOOR,
        prepare_crop=lambda photo, region: _crop(photo["id"]),
    )
    assert outcome.examined == 0


@pytest.mark.parametrize("primary_checked", [False, True])
def test_pairing_into_existing_person_detection_preserves_promoted_subject(db, primary_checked):
    from scanner.companions import _pair_raw_jpeg_companions
    from services import photo_ingest
    from test_candidate_subjects import _keep

    jpeg = _photo(db, "IMG_1.jpg")
    raw_id = photo_ingest.add_photo(
        db, db.folder_id, "IMG_1.cr3", extension=".cr3", file_size=1, file_mtime=1.,
    )
    db.commit()
    raw = {"id": raw_id}
    (primary,) = _detect(db, raw, [_region("vehicle", .8)])
    if primary_checked:
        _negative_check(db, primary)
    (companion,) = _detect(db, jpeg, [_region("vehicle", .8)])
    _keep(db, jpeg, companion)
    _pair_raw_jpeg_companions(db)
    db.commit()
    assert db.conn.execute(
        "SELECT category FROM detections WHERE id = ?", (primary,),
    ).fetchone()["category"] == "animal"
    assert db.candidate_subjects.evidence([primary])[primary]["promoted"] == 1


def test_detection_removed_during_inference_is_skipped(db):
    from classify_job.candidate_subjects import examine_candidates
    from test_candidate_subjects import _clf, _crop, _model

    photo = _photo(db, "removed.jpg")
    (region,) = _detect(db, photo, [_region("vehicle", .8)])

    def prepare(photo, detection):
        db.conn.execute("DELETE FROM detections WHERE id = ?", (region,))
        db.commit()
        return _crop(photo["id"])

    outcome = examine_candidates(
        db, [photo], _model(_clf({photo["id"]: (.3, .2)})), min_conf=FLOOR,
        prepare_crop=prepare,
    )
    assert outcome.examined == 0


def test_portable_subject_exports_the_positive_check(db):
    from computation_cache import _subject_for_detection
    from test_candidate_subjects import _keep

    photo = _photo(db, "export.jpg")
    (region,) = _detect(db, photo, [_region("vehicle", .8)])
    _keep(db, photo, region)
    identity = db.computation_cache.promotable_detection(region)
    subject = _subject_for_detection("d0", identity, db, region)
    assert subject["category"] == "vehicle"
    assert subject["candidate_subject"]["max_match_score"] == .55
    assert subject["candidate_subject"]["detector_category"] == "vehicle"


def test_schema_upgrade_backfills_preexisting_completion(db):
    from canonical_schema import create_subject_candidate_checks

    photo = _photo(db, "upgrade.jpg")
    (region,) = _detect(db, photo, [_region("vehicle", .8)])
    _negative_check(db, region)
    db.conn.execute("DROP TABLE subject_candidate_checks")
    create_subject_candidate_checks(db.conn)
    assert db.candidate_subjects.pending([photo["id"]], FLOOR, "BioCLIP", "fp1") == []
