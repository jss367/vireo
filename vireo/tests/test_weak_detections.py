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


def _full_image_bridge():
    photos, detections = _photos([0.224, 0.064, 0.617], gap=0.24)
    for items in detections.values():
        items[0].update(x=0.4, y=0.45, w=0.15, h=0.2)
    species = {pid: [('Common Yellowthroat', 0.9998, 'model', 'taxon:9721')] for pid in (1, 3)}
    fallback = {2: [('Common Yellowthroat', 0.9984, 'model', 'taxon:9721')]}
    return photos, detections, species, fallback


def test_full_image_bridge_requires_own_species_evidence_and_real_box():
    from weak_detections import matching_full_image_runs

    args = _full_image_bridge()
    matches = matching_full_image_runs(*args)
    assert len(matches) == 1
    assert matches[0]['photo_ids'] == [2]
    assert matches[0]['species_key'] == 'taxon:9721'
    assert matches[0]['evidence'] == 'full_image_sequence'


def test_full_image_bridge_rejects_conflicts_empty_scenes_and_unsupported_continuity():
    import copy

    from weak_detections import matching_full_image_runs

    original = _full_image_bridge()
    for reason in ('different_anchor', 'different_middle', 'same_name_different_identity',
                   'empty_scene', 'distant_box', 'long_gap', 'different_folder',
                   'missing_time', 'weak_classifier', 'ambiguous_classifier',
                   'disagreeing_models', 'multiple_anchor_subjects', 'zero_confidence_box'):
        photos, detections, species, fallback = copy.deepcopy(original)
        if reason == 'different_anchor':
            species[3] = [('Sparrow', 0.99, 'model', 'taxon:1')]
        elif reason == 'different_middle':
            fallback[2] = [('Sparrow', 0.99, 'model', 'taxon:1')]
        elif reason == 'same_name_different_identity':
            fallback[2] = [('Common Yellowthroat', 0.99, 'model', 'taxon:123')]
        elif reason == 'empty_scene':
            detections[2] = []
        elif reason == 'distant_box':
            detections[2][0]['x'] = 0.9
        elif reason == 'long_gap':
            photos[2]['timestamp'] = '2026-07-18T08:36:40'
        elif reason == 'different_folder':
            photos[1]['folder_id'] = 2
        elif reason == 'missing_time':
            photos[1]['timestamp'] = None
        elif reason == 'weak_classifier':
            fallback[2] = [('Common Yellowthroat', 0.5, 'model', 'taxon:9721')]
        elif reason == 'ambiguous_classifier':
            fallback[2].append(('Sparrow', 0.8, 'model', 'taxon:1'))
        elif reason == 'disagreeing_models':
            fallback[2].append(('Sparrow', 0.99, 'another-model', 'taxon:1'))
        elif reason == 'multiple_anchor_subjects':
            detections[1].append({**detections[1][0], 'x': 0.8})
        elif reason == 'zero_confidence_box':
            detections[2][0]['confidence'] = 0.0
        assert matching_full_image_runs(photos, detections, species, fallback) == [], reason


def test_full_image_bridge_does_not_chain_over_long_runs():
    from weak_detections import matching_full_image_runs

    photos, detections = _photos([0.9, 0.06, 0.06, 0.06, 0.06, 0.9])
    for items in detections.values():
        items[0].update(x=0.4, y=0.45, w=0.15, h=0.2)
    entry = [('Common Yellowthroat', 0.99, 'model', 'taxon:9721')]
    assert matching_full_image_runs(photos, detections, {1:entry, 6:entry},
                                    {pid:entry for pid in (2,3,4,5)}) == []
