"""Regression tests for context-aware weak animal detection rescue."""

from datetime import datetime, timedelta


def _photos(confidences, *, gap=0.05, folder_id=1):
    base = datetime(2026, 7, 18, 8, 36, 35, 990000)
    photos = []
    detections = {}
    for index, confidence in enumerate(confidences):
        photo_id = index + 1
        photos.append({
            "id": photo_id,
            "folder_id": folder_id,
            "timestamp": (base + timedelta(seconds=index * gap)).isoformat(),
        })
        if confidence is not None:
            detections[photo_id] = [{
                "confidence": confidence,
                "category": "animal",
                "detector_model": "megadetector-v6",
            }]
    return photos, detections


def test_contextual_weak_run_matches_grackle_threshold_cliff():
    from weak_detections import contextual_weak_runs

    photos, detections = _photos([
        0.229, 0.185, 0.156, 0.149, 0.172, 0.193, 0.186, 0.798,
    ])

    assert contextual_weak_runs(photos, detections) == [{
        "photo_ids": [2, 3, 4, 5, 6, 7],
        "left_photo_id": 1,
        "right_photo_id": 8,
        "left_confidence": 0.229,
        "right_confidence": 0.798,
    }]


def test_contextual_weak_run_requires_two_strong_anchors():
    from weak_detections import contextual_weak_runs

    photos, detections = _photos([0.9, 0.18, 0.17, None, 0.9])
    assert contextual_weak_runs(photos, detections) == []


def test_contextual_weak_run_does_not_cross_long_gap_or_folder():
    from weak_detections import contextual_weak_runs

    photos, detections = _photos([0.9, 0.18, 0.9], gap=4.0)
    assert contextual_weak_runs(photos, detections, max_gap=3.0) == []

    photos, detections = _photos([0.9, 0.18, 0.9])
    photos[1]["folder_id"] = 2
    assert contextual_weak_runs(photos, detections) == []


def test_contextual_weak_run_ignores_interleaved_other_folder():
    from weak_detections import contextual_weak_runs

    photos, detections = _photos([0.9, 0.18, 0.9])
    photos.insert(1, {
        "id": 99,
        "folder_id": 2,
        "timestamp": "2026-07-18T08:36:36.015000",
    })

    assert contextual_weak_runs(photos, detections)[0]["photo_ids"] == [2]


def test_matching_anchor_species_requires_agreement():
    from weak_detections import matching_anchor_species

    run = {"photo_ids": [2], "left_photo_id": 1, "right_photo_id": 3}
    species = {
        1: [("Great-tailed Grackle", 0.78, "inat21")],
        3: [("Great-tailed Grackle", 0.90, "inat21")],
    }
    match = matching_anchor_species(run, species)
    assert match["species"] == "Great-tailed Grackle"

    species[3] = [("Brown-headed Cowbird", 0.90, "inat21")]
    assert matching_anchor_species(run, species) is None
