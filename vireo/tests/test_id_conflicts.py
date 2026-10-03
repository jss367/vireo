"""The derived layer behind the ID Conflicts page.

What a row *means* — its status, whether the models disagree, whether it
still needs review — used to be worked out in the browser from a copy of the
whole collection. It is derived here now, so these tests pin the rules
directly instead of reaching through a rendered page.
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import id_conflicts as ic


def _pred(species, confidence=0.9, category="match", status="pending",
          canonical=None, display=None, pred_id=1, label=None):
    return {
        "id": pred_id,
        "species": species,
        "canonical_species": canonical or species.lower(),
        "canonical_display": display or species,
        "confidence": confidence,
        "status": status,
        "category": category,
        "category_label": label or category.title(),
        "category_detail": "",
        "matched_keyword": None,
        "shared_rank": None,
    }


def _subject(predictions, kind="detected", detection_id=1):
    return {
        "detection_id": detection_id,
        "kind": kind,
        "box": {"x": 0.1, "y": 0.1, "w": 0.2, "h": 0.2},
        "detector_confidence": 0.8,
        "predictions": predictions,
    }


def _photo(subjects, keywords=(), photo_id=1, **fields):
    predictions = {}
    for subject in subjects:
        for model, preds in subject["predictions"].items():
            predictions.setdefault(model, []).extend(preds)
    photo = {
        "photo_id": photo_id,
        "filename": f"photo{photo_id}.jpg",
        "timestamp": "2024-01-01T00:00:00",
        "rating": 0,
        "flag": "none",
        "wildlife_excluded": False,
        "miss_no_subject": False,
        "miss_clipped": False,
        "miss_oof": False,
        "keywords": list(keywords),
        "species_keywords": [],
        "subjects": subjects,
        "predictions": predictions,
    }
    photo.update(fields)
    return photo


def test_models_naming_the_same_taxon_do_not_disagree():
    """Two spellings of one taxon are agreement, not a model conflict."""
    photo = _photo([_subject({
        "a": [_pred("Western Cattle-Egret", canonical="taxon:5", pred_id=1)],
        "b": [_pred("Bubulcus ibis", canonical="taxon:5", display="Western Cattle-Egret", pred_id=2)],
    })])

    assessment = ic.assess_photo(photo, ["a", "b"], 0.4)

    assert assessment["signal"]["model_disagreement_score"] == 0
    assert assessment["signal"]["consensus_count"] == 2
    # The group is named by the taxon, not by whichever model sorted first.
    assert assessment["signal"]["consensus_species"] == "Western Cattle-Egret"


def test_model_disagreement_is_scored_by_the_weaker_side():
    photo = _photo([_subject({
        "a": [_pred("Blue Jay", confidence=0.9, canonical="taxon:1", pred_id=1)],
        "b": [_pred("Cardinal", confidence=0.6, canonical="taxon:2", pred_id=2)],
    })])

    signal = ic.assess_photo(photo, ["a", "b"], 0.4)["signal"]

    assert signal["model_disagreement_score"] == pytest.approx(60)
    assert signal["model_disagreement_label"] == "Blue Jay vs Cardinal"


def test_low_confidence_conflict_falls_below_the_threshold():
    """A conflict nobody is confident about is not counted as one."""
    photo = _photo([_subject({
        "a": [_pred("Blue Jay", confidence=0.2, category="conflict")],
    })])

    below = ic.assess_photo(photo, ["a"], 0.4)
    above = ic.assess_photo(photo, ["a"], 0.1)

    assert below["status"]["category"] == "low_conflict"
    assert below["signal"]["keyword_conflict_count"] == 0
    assert above["status"]["category"] == "conflict"
    assert above["signal"]["keyword_conflict_count"] == 1


def test_second_detected_species_is_additional_not_a_conflict():
    """When another subject already accounts for the keyword, a second
    subject's different species is a suggestion, not a contradiction."""
    photo = _photo([
        _subject({"a": [_pred("Red-tailed Hawk", category="match", pred_id=1)]},
                 detection_id=1),
        _subject({
            "a": [_pred("Cooper's Hawk", category="conflict", canonical="taxon:9", pred_id=2)],
            "b": [_pred("Cooper's Hawk", category="conflict", canonical="taxon:9", pred_id=3)],
        }, detection_id=2),
    ])

    assessment = ic.assess_photo(photo, ["a", "b"], 0.4)

    assert [s["category"] for s in assessment["statuses"]] == ["match", "additional"]
    assert assessment["statuses"][1]["label"] == "Additional species suggested"
    assert assessment["signal"]["additional_subject_count"] == 1
    # An additional-species subject must not inflate the row's keyword
    # conflict: nothing here contradicts the keyword.
    assert assessment["signal"]["keyword_conflict_count"] == 0


def test_single_model_additional_subject_is_only_possible():
    photo = _photo([
        _subject({"a": [_pred("Red-tailed Hawk", category="match", pred_id=1)]},
                 detection_id=1),
        _subject({"a": [_pred("Cooper's Hawk", category="conflict", pred_id=2)]},
                 detection_id=2),
    ])

    statuses = ic.assess_photo(photo, ["a"], 0.4)["statuses"]

    assert statuses[1]["label"] == "Possible additional species"


def test_row_surfaces_unclassified_over_a_pending_match():
    """A detected subject with no predictions has nothing to mark reviewed,
    but it still needs classifying — so it outranks a pending match."""
    photo = _photo([
        _subject({"a": [_pred("Red-tailed Hawk", category="match")]}, detection_id=1),
        _subject({}, detection_id=2),
    ])

    assessment = ic.assess_photo(photo, ["a"], 0.4)

    assert sorted(s["category"] for s in assessment["statuses"]) == [
        "match", "unclassified",
    ]
    assert assessment["status"]["category"] == "unclassified"


def test_subjectless_photo_falls_back_to_missing_prediction():
    photo = _photo([])

    assessment = ic.assess_photo(photo, ["a"], 0.4)
    record = ic.index_record(photo, assessment)

    assert assessment["statuses"] == []
    assert assessment["status"]["category"] == "missing_prediction"
    assert record.filter_mask & ic._FILTER_BIT["missing_prediction"]


def test_unclassified_subject_counts_as_missing_prediction():
    """The filter and the tile agree: a detected subject with no prediction
    needs one, even when another subject on the photo matched."""
    photo = _photo([
        _subject({"a": [_pred("Red-tailed Hawk", category="match")]}, detection_id=1),
        _subject({}, detection_id=2),
    ])

    record = ic.index_record(photo, ic.assess_photo(photo, ["a"], 0.4))
    selection = ic.select([record], ["a"], filter_id="missing_prediction")

    assert selection.total == 1
    assert selection.summary["missing_predictions"] == 1


def test_rejected_predictions_are_ignored_by_the_assessment():
    photo = _photo([_subject({
        "a": [_pred("Blue Jay", category="conflict", status="rejected")],
    })])

    signal = ic.assess_photo(photo, ["a"], 0.4)["signal"]

    assert signal["keyword_conflict_count"] == 0
    assert signal["top_prediction_count"] == 0


def test_missing_shown_model_is_counted_per_subject():
    photo = _photo([_subject({"a": [_pred("Blue Jay")]})])

    signal = ic.assess_photo(photo, ["a", "b"], 0.4)["signal"]

    assert signal["missing_visible_model_count"] == 1


def test_narrowing_to_one_model_still_reports_the_absent_one():
    """Asking for models the photo lacks is the Missing shown model signal,
    not a reason to quietly drop them from the comparison."""
    assert ic.resolve_models(["a"], ["a", "b"]) == ["a", "b"]
    assert ic.resolve_models(["a", "b"], []) == ["a", "b"]


def test_keyword_conflict_counts_predictions_below_the_top_one():
    """A conflicting prediction that is not the model's best still conflicts
    with the keyword — the chip counts predictions, not winners."""
    photo = _photo([_subject({
        "a": [
            _pred("Red-tailed Hawk", confidence=0.95, category="match", pred_id=1),
            _pred("Cooper's Hawk", confidence=0.41, category="conflict", pred_id=2),
        ],
    })])

    record = ic.index_record(photo, ic.assess_photo(photo, ["a"], 0.4))
    selection = ic.select([record], ["a"], filter_id="keyword_model_conflict")

    assert selection.total == 1
    assert selection.filter_counts["keyword_model_conflict"] == 1


def test_pending_match_still_needs_review():
    photo = _photo([_subject({
        "a": [_pred("Cardinal", category="match", status="pending")],
    })])

    assessment = ic.assess_photo(photo, ["a"], 0.4)

    assert assessment["status"]["needs_review"] is True
    assert assessment["status"]["category"] == "match"


def test_settled_predictions_do_not_need_review():
    photo = _photo([_subject({
        "a": [_pred("Cardinal", category="match", status="accepted")],
    })])

    assert ic.assess_photo(photo, ["a"], 0.4)["status"]["needs_review"] is False


def _records(photos, models=("a",), min_confidence=0.4):
    return [
        ic.index_record(photo, ic.assess_photo(photo, list(models), min_confidence))
        for photo in photos
    ]


def test_exclusions_hide_rows_but_still_report_what_they_would_hide():
    photos = [
        _photo([_subject({"a": [_pred("Cardinal")]})], photo_id=1, flag="rejected"),
        _photo([_subject({"a": [_pred("Cardinal")]})], photo_id=2, flag="flagged"),
        _photo([_subject({"a": [_pred("Cardinal")]})], photo_id=3),
    ]
    records = _records(photos)

    selection = ic.select(records, ["a"], filter_id="all", excludes=["rejected"])

    assert selection.total == 2
    assert selection.exclusion_counts["rejected"] == 1
    assert selection.exclusion_counts["unflagged"] == 1
    # Filter chips count what each filter would show *after* the exclusion,
    # so clicking one cannot produce a different number of rows.
    assert selection.filter_counts["all"] == 2


def test_hide_multi_species_keeps_only_single_species_photos():
    """Two species keywords, or a suggested second species, make a photo
    multi-species; one keyword with several same-species subjects does not."""
    one_keyword = _photo(
        [_subject({"a": [_pred("Cardinal")]})],
        photo_id=1, species_keywords=["Cardinal"],
    )
    two_keywords = _photo(
        [_subject({"a": [_pred("Cardinal")]})],
        photo_id=2, species_keywords=["Cardinal", "Blue Jay"],
    )
    suggested_second = _photo([
        _subject({"a": [_pred("Red-tailed Hawk", category="match", pred_id=1)]},
                 detection_id=1),
        _subject({"a": [_pred("Cooper's Hawk", category="conflict", pred_id=2)]},
                 detection_id=2),
    ], photo_id=3, species_keywords=["Red-tailed Hawk"])
    two_of_one_species = _photo([
        _subject({"a": [_pred("Mallard", pred_id=1)]}, detection_id=1),
        _subject({"a": [_pred("Mallard", pred_id=2)]}, detection_id=2),
    ], photo_id=4, species_keywords=["Mallard"])
    records = _records([one_keyword, two_keywords, suggested_second, two_of_one_species])

    selection = ic.select(records, ["a"], filter_id="all", excludes=["multi_species"])

    assert selection.photo_ids == [1, 4]
    assert selection.exclusion_counts["multi_species"] == 2


def test_summary_tiles_describe_the_collection_not_the_page():
    photos = [
        _photo([_subject({"a": [_pred("Cardinal", category="conflict")]})], photo_id=i)
        for i in range(1, 6)
    ]
    records = _records(photos)

    selection = ic.select(records, ["a"], filter_id="all", page=1, per_page=2)

    assert len(selection.photo_ids) == 2
    assert selection.total == 5
    assert selection.summary["photos"] == 5
    assert selection.summary["conflicts"] == 5


def test_paging_walks_the_sorted_list_without_repeating_rows():
    photos = [
        _photo([_subject({"a": [_pred("Cardinal")]})], photo_id=i)
        for i in range(1, 8)
    ]
    records = _records(photos)

    seen = []
    for page in (1, 2, 3, 4):
        seen.extend(ic.select(
            records, ["a"], filter_id="all", sort="filename",
            page=page, per_page=2,
        ).photo_ids)

    assert seen == [1, 2, 3, 4, 5, 6, 7]


def test_paging_past_the_end_clamps_to_the_last_page():
    """After a decision shrinks the queue, the caller may still be past the
    final page. The selection reports the clamped page and returns its rows
    so the pager cannot land on "page 3 of 2" with an empty slice."""
    photos = [
        _photo([_subject({"a": [_pred("Cardinal")]})], photo_id=i)
        for i in range(1, 4)
    ]
    records = _records(photos)

    selection = ic.select(
        records, ["a"], filter_id="all", sort="filename",
        page=7, per_page=2,
    )

    assert selection.total == 3
    assert selection.page == 2
    assert selection.photo_ids == [3]


def test_paging_an_empty_selection_reports_page_one():
    """An empty result still has a valid page — the pager has to render
    something rather than a caller-supplied phantom page number."""
    selection = ic.select([], ["a"], filter_id="all", page=5, per_page=2)

    assert selection.total == 0
    assert selection.page == 1
    assert selection.photo_ids == []


def test_review_priority_sorts_pending_conflicts_first():
    settled = _photo([_subject({"a": [_pred("Cardinal", category="conflict", status="accepted")]})], photo_id=1)
    pending_match = _photo([_subject({"a": [_pred("Cardinal", category="match")]})], photo_id=2)
    pending_conflict = _photo([_subject({"a": [_pred("Blue Jay", category="conflict")]})], photo_id=3)
    records = _records([settled, pending_match, pending_conflict])

    ordered = ic.select(records, ["a"], filter_id="all", sort="review_priority")

    assert ordered.photo_ids == [3, 2, 1]


def test_newest_and_oldest_sorts_are_opposites():
    photos = [
        _photo([_subject({"a": [_pred("Cardinal")]})], photo_id=1,
               timestamp="2024-01-01T00:00:00"),
        _photo([_subject({"a": [_pred("Cardinal")]})], photo_id=2,
               timestamp="2024-06-01T00:00:00"),
    ]
    records = _records(photos)

    newest = ic.select(records, ["a"], filter_id="all", sort="newest").photo_ids
    oldest = ic.select(records, ["a"], filter_id="all", sort="oldest").photo_ids

    assert newest == [2, 1]
    assert oldest == [1, 2]


def test_low_confidence_sorts_a_zero_reading_first_and_no_reading_last():
    """A stored confidence of 0.0 is a real reading, and the least confident
    one there is, so it heads the Low confidence list. A row whose only
    prediction was rejected has no reading at all — that is an absence, not a
    low number, so it sorts to the end instead of ahead of every real score."""
    photos = [
        _photo([_subject({"a": [_pred("Cardinal", confidence=0.5)]})], photo_id=1),
        _photo([_subject({"a": [_pred("Blue Jay", confidence=0.0)]})], photo_id=2),
        _photo([_subject({"a": [_pred("Crow", status="rejected")]})], photo_id=3),
    ]
    records = _records(photos)
    # The two cases the sort has to tell apart really do differ here: one row
    # carries 0.0, the other carries nothing.
    assert [r.min_confidence for r in records] == [0.5, 0.0, None]

    ordered = ic.select(records, ["a"], filter_id="all", sort="low_confidence")

    assert ordered.photo_ids == [2, 1, 3]


def test_search_matches_keywords_species_and_filenames():
    photos = [
        _photo([_subject({"a": [_pred("Cooper's Hawk")]})], photo_id=1),
        _photo(
            [_subject({"a": [_pred("Cardinal")]})],
            keywords=[{"name": "Santee Lakes", "type": "location"}],
            photo_id=2,
        ),
    ]
    records = _records(photos)

    by_species = ic.select(records, ["a"], filter_id="all", query="hawk")
    by_keyword = ic.select(records, ["a"], filter_id="all", query="santee")
    by_filename = ic.select(records, ["a"], filter_id="all", query="photo2")

    assert by_species.photo_ids == [1]
    assert by_keyword.photo_ids == [2]
    assert by_filename.photo_ids == [2]
    # The chips keep describing the collection: search narrows the list, it
    # does not redefine what each filter matches.
    assert by_species.filter_counts["all"] == 2


def test_search_tokens_must_all_match_but_may_span_fields():
    photo = _photo(
        [_subject({"a": [_pred("Cooper's Hawk")]})],
        keywords=[{"name": "Santee Lakes", "type": "location"}],
        photo_id=1,
    )
    records = _records([photo])

    assert ic.select(records, ["a"], filter_id="all", query="hawk santee").total == 1
    assert ic.select(records, ["a"], filter_id="all", query="hawk ramona").total == 0
    # A single token cannot straddle two fields.
    assert ic.select(records, ["a"], filter_id="all", query="hawksantee").total == 0


def test_whole_word_search_does_not_match_inside_a_word():
    photo = _photo([_subject({"a": [_pred("Cardinal")]})], photo_id=1)
    records = _records([photo])

    assert ic.select(records, ["a"], filter_id="all", query="cardin").total == 1
    assert ic.select(
        records, ["a"], filter_id="all", query="cardin", whole_word=True,
    ).total == 0
    assert ic.select(
        records, ["a"], filter_id="all", query="cardinal", whole_word=True,
    ).total == 1


def test_batch_action_target_is_absent_for_multi_subject_rows():
    """A photo whose subjects disagree has no single prediction a batch
    action could mean, so the page is told there is none."""
    single = _photo([_subject({"a": [_pred("Cardinal", pred_id=7)]})], photo_id=1)
    multi = _photo([
        _subject({"a": [_pred("Cardinal", pred_id=8)]}, detection_id=1),
        _subject({"a": [_pred("Blue Jay", pred_id=9)]}, detection_id=2),
    ], photo_id=2)

    assert ic.attach_assessment(single, ["a"], 0.4)["best_prediction_id"] == 7
    assert ic.attach_assessment(multi, ["a"], 0.4)["best_prediction_id"] is None


def test_batch_action_target_prefers_the_strongest_pending_category():
    photo = _photo([_subject({
        "a": [_pred("Cardinal", category="match", confidence=0.99, pred_id=1)],
        "b": [_pred("Blue Jay", category="conflict", confidence=0.5, pred_id=2)],
    })])

    assert ic.attach_assessment(photo, ["a", "b"], 0.4)["best_prediction_id"] == 2


def test_settled_predictions_are_never_a_batch_target():
    photo = _photo([_subject({
        "a": [_pred("Cardinal", status="accepted", pred_id=1)],
    })])

    assert ic.attach_assessment(photo, ["a"], 0.4)["best_prediction_id"] is None


# The consensus and disagreement notes are the strings a user reads off the
# Signals column. They used to be built in the browser, and the leak they
# guard against — a persisted outdated binomial escaping into a label the
# server had already resolved — is invisible from the payload alone.

def _egret(species, confidence, display="Western Cattle-Egret", pred_id=1):
    return _pred(
        species, confidence=confidence, category="match",
        canonical="taxon:1", display=display, pred_id=pred_id,
    )


def test_consensus_label_names_the_taxon_not_the_first_model():
    def photo_for(first, second):
        return _photo([_subject({
            "model-a": [_egret(first, 0.9, pred_id=1)],
            "model-b": [_egret(second, 0.8, pred_id=2)],
        })])

    models = ["model-a", "model-b"]
    binomial_first = ic.assess_photo(
        photo_for("Bubulcus ibis", "Western Cattle-Egret"), models, 0.4,
    )["signal"]
    common_first = ic.assess_photo(
        photo_for("Western Cattle-Egret", "Bubulcus ibis"), models, 0.4,
    )["signal"]

    assert binomial_first["consensus_count"] == 2
    assert binomial_first["consensus_species"] == "Western Cattle-Egret"
    assert binomial_first["consensus_label"] == (
        "2 models agree on Western Cattle-Egret"
    )
    # And it cannot depend on which model happens to sort first.
    assert common_first["consensus_label"] == binomial_first["consensus_label"]


def test_disagreement_label_names_both_taxa():
    photo = _photo([_subject({
        "model-a": [_egret("Bubulcus ibis", 0.9, pred_id=1)],
        "model-b": [_pred(
            "Blue Jay", confidence=0.8, canonical="taxon:2",
            display="Blue Jay", pred_id=2,
        )],
    })])

    signal = ic.assess_photo(photo, ["model-a", "model-b"], 0.4)["signal"]

    assert signal["model_disagreement_label"] == "Western Cattle-Egret vs Blue Jay"


def test_labels_fall_back_to_the_raw_prediction():
    """Predictions the taxonomy could not resolve still get a label."""
    photo = _photo([_subject({
        "model-a": [{
            "id": 1, "species": "Mystery Beast",
            "canonical_species": "mystery beast", "canonical_display": None,
            "confidence": 0.9, "status": "pending", "category": "new",
        }],
        "model-b": [{
            "id": 2, "species": "Other Beast",
            "canonical_species": "other beast", "canonical_display": None,
            "confidence": 0.8, "status": "pending", "category": "new",
        }],
    })])

    signal = ic.assess_photo(photo, ["model-a", "model-b"], 0.4)["signal"]

    assert signal["consensus_species"] == "Mystery Beast"
    assert signal["model_disagreement_label"] == "Mystery Beast vs Other Beast"


def test_multi_subject_labels_name_the_subject_they_came_from():
    """The strongest disagreement and the strongest consensus are often
    different subjects; an unlabelled pair reads as a contradiction."""
    photo = _photo([
        _subject({
            "model-a": [_pred("Cardinal", canonical="taxon:1", pred_id=1)],
            "model-b": [_pred("Cardinal", canonical="taxon:1", pred_id=2)],
        }, detection_id=1),
        _subject({
            "model-a": [_pred("Blue Jay", confidence=0.9, canonical="taxon:2", pred_id=3)],
            "model-b": [_pred("Sparrow", confidence=0.7, canonical="taxon:3", pred_id=4)],
        }, detection_id=2),
    ])

    signal = ic.assess_photo(photo, ["model-a", "model-b"], 0.4)["signal"]

    assert signal["consensus_label"] == "Subject 1: 2 models agree on Cardinal"
    assert signal["model_disagreement_label"] == "Subject 2: Blue Jay vs Sparrow"
