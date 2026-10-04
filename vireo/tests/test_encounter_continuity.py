from copy import deepcopy

import pytest
from encounter_continuity import apply_encounter_continuity, evidence_photo_ids
from encounters import segment_encounters


def compact(photos):
    return {
        p["id"]: [
            {
                "id": d["id"],
                "detector_model": d["detector_model"],
                "category": d["category"],
                "confidence": d["detector_confidence"],
                **{k: d["box_" + k] for k in ("x", "y", "w", "h") if "box_" + k in d},
                "predictions": [
                    (q["name"], q["score"], s["model"], "taxon:" + q["taxon"][5:])
                    for s in d["sources"]
                    for q in s["predictions"]
                ],
            }
            for d in p["evidence"]
        ]
        for p in photos
    }


def suppress_flips(photos, config):
    evidence = compact(photos)
    selected = evidence_photo_ids(photos, config)
    after = apply_encounter_continuity(photos, {pid: d for pid, d in evidence.items() if pid in selected}, config)
    return after, [
        {"photo_id": p["id"], **p["isolated_species_context"]} for p in after if p.get("isolated_species_context")
    ]


def flip_sequence():
    result = []
    for i, (key, score) in enumerate([("10", 0.99), ("20", 0.84), ("10", 0.98)]):
        name = "Bird " + key
        es = [(name, score, "model-a", "taxon:" + key)]
        det = {
            "id": i,
            "detector_model": "megadetector-v6",
            "category": "animal",
            "detector_confidence": 0.7,
            "box_x": 0.2,
            "box_y": 0.2,
            "box_w": 0.3,
            "box_h": 0.3,
            "sources": [{"model": "model-a", "predictions": [{"name": name, "score": score, "taxon": "inat:" + key}]}],
        }
        result.append(
            {
                "id": i + 1,
                "folder_id": 1,
                "timestamp": f"2026-01-01T00:00:00.{i}00+00:00",
                "subject_present": True,
                "subject_absent": False,
                "subject_uncertain": False,
                "species_top5": es,
                "species_keys": {"taxon:" + key: "inat:" + key},
                "evidence": [det],
            }
        )
    return result


def test_repairs_isolated_flip_without_inventing_scores_or_editing_original():
    photos = flip_sequence()
    original = deepcopy(photos)
    before = segment_encounters(photos)
    after, changes = suppress_flips(photos, {})
    assert len(before) == 3 and len(segment_encounters(after)) == 1
    assert len(changes) == 1 and changes[0]["photo_id"] == 2
    assert after[1]["species_top5"] == original[1]["species_top5"]
    assert after[1]["grouping_species_top5"] == []
    assert after[1]["evidence"] == original[1]["evidence"]
    assert photos == original


@pytest.mark.parametrize(
    "guard",
    [
        "sustained_switch",
        "different_anchors",
        "missing_prediction",
        "weak_anchor",
        "strong_conflict",
        "model_agreement",
        "time",
        "reversed_time",
        "same_time",
        "folder",
        "unknown_folder",
        "missing_box",
        "distant_box",
        "invalid_box",
        "weak_detection",
        "absent_subject",
        "secondary_species",
        "multiple_matching_subjects",
        "full_image_confirmation",
        "visual_switch",
        "unknown_subject",
        "anchor_crop_disagreement",
    ],
)
def test_keeps_unsafe_boundaries(guard):
    p = flip_sequence()
    config = {}
    if guard == "sustained_switch":
        p[2]["species_top5"] = deepcopy(p[1]["species_top5"])
    elif guard == "different_anchors":
        p[2]["species_top5"] = [("Third bird", 0.99, "model-a", "taxon:30")]
    elif guard == "missing_prediction":
        p[1]["species_top5"] = []
    elif guard == "weak_anchor":
        p[0]["species_top5"] = [("Bird 10", 0.94, "model-a", "taxon:10")]
    elif guard == "strong_conflict":
        p[1]["species_top5"] = [("Bird 20", 0.96, "model-a", "taxon:20")]
    elif guard == "model_agreement":
        p[1]["species_top5"].append(("Bird 20", 0.84, "model-b", "taxon:20"))
    elif guard == "time":
        p[2]["timestamp"] = "2026-01-01T00:00:01+00:00"
    elif guard == "reversed_time":
        p[0]["timestamp"] = p[2]["timestamp"]
    elif guard == "same_time":
        p[1]["timestamp"] = p[0]["timestamp"]
    elif guard == "folder":
        p[2]["folder_id"] = 2
    elif guard == "unknown_folder":
        for x in p:
            x["folder_id"] = None
    elif guard == "missing_box":
        del p[1]["evidence"][0]["box_w"]
    elif guard == "distant_box":
        p[1]["evidence"][0]["box_x"] = 0.65
    elif guard == "invalid_box":
        p[1]["evidence"][0]["box_w"] = float("nan")
    elif guard == "weak_detection":
        p[1]["evidence"][0]["detector_confidence"] = 0.15
    elif guard == "absent_subject":
        p[1]["subject_absent"] = True
    elif guard == "unknown_subject":
        p[1]["subject_present"] = False
    elif guard == "secondary_species":
        d = deepcopy(p[0]["evidence"][0])
        d["detector_confidence"] = 0.02
        p[1]["evidence"].append(d)
    elif guard == "multiple_matching_subjects":
        p[1]["evidence"].append(deepcopy(p[1]["evidence"][0]))
    elif guard == "full_image_confirmation":
        d = deepcopy(p[1]["evidence"][0])
        d["detector_model"] = "full-image"
        p[1]["evidence"].append(d)
    elif guard == "visual_switch":
        p[0]["dino_subject_embedding"] = [1, 0]
        p[1]["dino_subject_embedding"] = [0, 1]
    elif guard == "anchor_crop_disagreement":
        p[0]["evidence"][0]["sources"][0]["predictions"][0]["taxon"] = "inat:30"
    after, changes = suppress_flips(p, config)
    assert not changes and after == p


def test_reference_labels_cannot_change_candidate_decision():
    p = flip_sequence()
    first = suppress_flips(p, {})
    for x in p:
        x["labels"] = ["unrelated species"]
        x["answers"] = {"taxa": ["inat:999"]}
    after, changes = suppress_flips(p, {})
    assert changes == first[1]


def test_uncertain_extra_detection_does_not_conceal_the_matching_subject():
    p = flip_sequence()
    d = deepcopy(p[2]["evidence"][0])
    d["box_w"] = 0.02
    d["box_h"] = 0.02
    d["sources"][0]["predictions"][0].update(taxon="inat:30", score=0.22)
    p[2]["evidence"].append(d)
    assert len(suppress_flips(p, {})[1]) == 1


def test_repeated_invocation_does_not_cascade():
    p, changes = suppress_flips(flip_sequence(), {})
    assert changes and suppress_flips(p, {})[0] == p


def test_disagreeing_full_image_models_veto_isolated_repair():
    photos = flip_sequence()
    full = deepcopy(photos[1]["evidence"][0])
    full["detector_model"] = "full-image"
    full["sources"] = [
        {"model": "model-a", "predictions": [{"name": "Bird 10", "score": 0.99, "taxon": "inat:10"}]},
        {"model": "model-b", "predictions": [{"name": "Bird 20", "score": 0.99, "taxon": "inat:20"}]},
    ]
    photos[1]["evidence"].append(full)
    after, changes = suppress_flips(photos, {})
    assert not changes and after == photos


def promote(photos, config):
    evidence = compact(photos)
    selected = evidence_photo_ids(photos, config)
    after = apply_encounter_continuity(photos, {pid: d for pid, d in evidence.items() if pid in selected}, config)
    return after, [
        p["id"] for p, q in zip(photos, after, strict=True) if p["subject_absent"] and not q["subject_absent"]
    ]


def weak_sequence():
    ps = []
    for pid, conf in [(1, 0.9), (2, 0.06), (3, 0.9)]:
        pred = {"taxon": "inat:10", "name": "Example bird", "score": 0.99}
        det = {
            "id": pid,
            "detector_model": "megadetector-v6",
            "category": "animal",
            "detector_confidence": conf,
            "box_x": 0.1,
            "box_y": 0.1,
            "box_w": 0.4,
            "box_h": 0.4,
            "sources": [{"model": "model-a", "predictions": [pred]}],
        }
        ps.append(
            {
                "id": pid,
                "folder_id": 1,
                "timestamp": f"2026-01-01T00:00:0{pid}",
                "evidence": [det],
                "subject_absent": pid == 2,
                "subject_present": pid != 2,
                "subject_uncertain": False,
                "species_top5": [] if pid == 2 else [("Example bird", 0.99, "model-a", "taxon:10")],
                "species_keys": {"taxon:10": "inat:10"},
            }
        )
    return ps


def test_crop_evidence_recovers_without_full_image_and_never_mutates_inputs():
    ps = weak_sequence()
    original = deepcopy(ps)
    after, bridges = promote(ps, {})
    assert len(bridges) == 1 and after[1]["subject_uncertain"] and not after[1]["subject_absent"]
    assert after[1]["species_keys"] == {"taxon:10": "inat:10"}
    assert ps == original


@pytest.mark.parametrize("source", ["full-image", "megadetector-v6"])
def test_disagreeing_supplemental_models_veto_weak_rescue(source):
    photos = weak_sequence()
    supplemental = deepcopy(photos[1]["evidence"][0])
    supplemental.update(id=99, detector_model=source, detector_confidence=0.02)
    supplemental["sources"].append(
        {"model": "model-b", "predictions": [{"name": "Other bird", "score": 0.99, "taxon": "inat:20"}]}
    )
    photos[1]["evidence"].append(supplemental)
    after, bridges = promote(photos, {})
    assert not bridges and after == photos


@pytest.mark.parametrize(
    "guard",
    [
        "empty",
        "conflicting_crop",
        "conflicting_full",
        "conflicting_secondary",
        "models_disagree",
        "multiple_anchors",
        "geometry",
        "time",
        "folder",
        "disabled",
        "long_run",
    ],
)
def test_weak_recovery_preserves_safety_boundaries(guard):
    ps = weak_sequence()
    cfg = {}
    if guard == "empty":
        ps[1]["evidence"] = []
    elif guard == "conflicting_crop":
        ps[1]["evidence"][0]["sources"][0]["predictions"][0]["taxon"] = "inat:20"
    elif guard in ("conflicting_full", "conflicting_secondary"):
        d = deepcopy(ps[1]["evidence"][0])
        d["id"] = 99
        d["detector_confidence"] = 0.02
        d["detector_model"] = "full-image" if guard == "conflicting_full" else "megadetector-v6"
        d["sources"][0]["predictions"][0]["taxon"] = "inat:20"
        ps[1]["evidence"].append(d)
    elif guard == "models_disagree":
        ps[1]["evidence"][0]["sources"].append(
            {"model": "model-b", "predictions": [{"name": "Other bird", "score": 0.99, "taxon": "inat:20"}]}
        )
    elif guard == "multiple_anchors":
        ps[0]["evidence"].append(deepcopy(ps[0]["evidence"][0]))
    elif guard == "geometry":
        ps[1]["evidence"][0]["box_x"] = 0.8
    elif guard == "time":
        ps[-1]["timestamp"] = "2026-01-01T00:00:12"
    elif guard == "folder":
        ps[-1]["folder_id"] = 2
    elif guard == "disabled":
        cfg = {"pipeline": {"weak_detection_rescue_enabled": False}}
    elif guard == "long_run":
        mids = [deepcopy(ps[1]) for _ in range(9)]
        for i, p in enumerate(mids):
            p.update(id=10 + i, timestamp=f"2026-01-01T00:00:02.{i}")
        ps = [ps[0], *mids, ps[-1]]
    assert not promote(ps, cfg)[1]


def extended_sequence(count=8, *, score=0.99, box_confidence=0.06):
    from datetime import datetime, timedelta

    left, middle, right = weak_sequence()
    mids = [deepcopy(middle) for _ in range(count)]
    photos = [left, *mids, right]
    for i, photo in enumerate(photos):
        photo.update(id=i + 1, timestamp=(datetime(2026, 1, 1) + timedelta(seconds=i)).isoformat())
    for photo in mids:
        d = photo["evidence"][0]
        d["detector_confidence"] = box_confidence
        d["sources"][0]["predictions"][0]["score"] = score
    return photos


@pytest.mark.parametrize("score,support", [(0.99, "classifier"), (0.4, "anchor_context")])
def test_default_recovers_eight_weak_frames_without_changing_predictions(score, support):
    photos = extended_sequence(score=score)
    original = deepcopy(photos)
    after, bridges = promote(photos, {})
    assert bridges == list(range(2, 10))
    assert photos == original
    for before, photo in zip(photos[1:-1], after[1:-1], strict=True):
        assert photo["species_top5"] == before["species_top5"]
        assert photo["evidence"] == before["evidence"]
        assert photo["weak_detection_context"]["support"] == support
        assert bool(photo["grouping_species_top5"]) == (support == "classifier")
    assert len(segment_encounters(after)) == 1
    assert promote(after, {})[0] == after


@pytest.mark.parametrize("guard", [
    "nine_frames", "over_ten_seconds", "missing_box", "low_box", "overlap", "folder",
    "anchor_prediction", "anchor_full_image", "middle_full_image", "secondary_model",
    "visual_conflict", "isolated_anchor", "multiple_subjects", "disabled",
])
def test_extended_context_guards(guard):
    photos = extended_sequence(9 if guard == "nine_frames" else 8, score=0.4)
    config = {}
    if guard == "over_ten_seconds":
        photos[-1]["timestamp"] = "2026-01-01T00:00:10.001"
    elif guard == "missing_box":
        photos[4]["evidence"] = []
    elif guard == "low_box":
        photos[4]["evidence"][0]["detector_confidence"] = 0.029
    elif guard == "overlap":
        photos[4]["evidence"][0]["box_x"] = 0.45
    elif guard == "folder":
        photos[-1]["folder_id"] = 2
    elif guard == "anchor_prediction":
        photos[0]["species_top5"] = [("Other bird", 0.99, "model-a", "taxon:20")]
    elif guard in ("anchor_full_image", "middle_full_image", "secondary_model"):
        photo = photos[0] if guard == "anchor_full_image" else photos[4]
        d = deepcopy(photo["evidence"][0])
        d["detector_model"] = "full-image" if guard.endswith("full_image") else "megadetector-v6"
        d["detector_confidence"] = 0.01
        d["sources"].append({"model": "model-b", "predictions": [
            {"name": "Other bird", "taxon": "inat:20", "score": 0.99},
        ]})
        photo["evidence"].append(d)
    elif guard == "visual_conflict":
        photos[0]["dino_subject_embedding"] = [1, 0]
        photos[4]["dino_subject_embedding"] = [0, 1]
    elif guard == "isolated_anchor":
        photos[0]["isolated_species_context"] = {"anchor_ids": [99, 100]}
    elif guard == "multiple_subjects":
        photos[0]["evidence"].append(deepcopy(photos[0]["evidence"][0]))
    elif guard == "disabled":
        config = {"pipeline": {"weak_detection_rescue_enabled": False}}
    assert not promote(photos, config)[1]


def test_direct_classification_allows_weaker_boxes_than_anchor_context():
    photos = extended_sequence(box_confidence=0.01)
    assert len(promote(photos, {})[1]) == 8
    for photo in photos[1:-1]:
        photo["evidence"][0]["sources"][0]["predictions"][0]["score"] = 0.4
    assert not promote(photos, {})[1]


def test_ten_second_boundary_and_reference_labels():
    photos = extended_sequence(score=0.4)
    photos[-1]["timestamp"] = "2026-01-01T00:00:10"
    first, _ = promote(photos, {})
    for photo in photos:
        photo["labels"] = ["Something else"]
    second, _ = promote(photos, {})
    assert all(not p["subject_absent"] for p in second)
    assert [p.get("weak_detection_context") for p in first] == [p.get("weak_detection_context") for p in second]


def test_non_animal_subject_does_not_hide_the_true_animal_anchor():
    photos = extended_sequence(4)
    # The loader marks this frame present because of a strong vehicle box,
    # but its animal detection still belongs to the weak run.
    photos[1].update(subject_present=True, subject_absent=False)
    vehicle = deepcopy(photos[1]["evidence"][0])
    vehicle.update(category="vehicle", detector_confidence=0.9)
    photos[1]["evidence"].append(vehicle)
    after, bridges = promote(photos, {})
    assert bridges == [3, 4, 5]
    assert after[2]["weak_detection_context"]["anchor_ids"] == [1, 6]


def test_burst_time_gap_does_not_shift_default_continuity_with_hidden_anchor_conflict():
    # A hidden anchor conflict (a full-image detection on each anchor whose
    # independent classifier picks a different species) would be ignored by the
    # retained legacy _recover_weak pass but vetoed by _recover_extended's
    # anchor-conflict check. Without the burst_time_gap decoupling, lowering
    # the slider past the anchor-to-anchor gap would skip the legacy run that
    # still rescues at the default gap, so the slider would change encounter
    # continuity. Pinning the legacy window inside the default rule keeps the
    # outcome identical across saved burst_time_gap values.
    base = weak_sequence()
    for anchor_idx in (0, 2):
        base[anchor_idx]["evidence"].append({
            "id": 100 + anchor_idx,
            "detector_model": "full-image",
            "category": "animal",
            "detector_confidence": 0.5,
            "box_x": 0.0, "box_y": 0.0, "box_w": 1.0, "box_h": 1.0,
            "sources": [{"model": "model-b", "predictions": [
                {"name": "Other bird", "score": 0.99, "taxon": "inat:20"},
            ]}],
        })
    wide, wide_bridges = promote(deepcopy(base), {"pipeline": {"burst_time_gap": 3.0}})
    narrow, narrow_bridges = promote(deepcopy(base), {"pipeline": {"burst_time_gap": 0.1}})
    assert wide_bridges == narrow_bridges
    assert [p["subject_absent"] for p in wide] == [p["subject_absent"] for p in narrow]
    assert [p["subject_uncertain"] for p in wide] == [p["subject_uncertain"] for p in narrow]
