import pytest

from encounter_eval.common import Group
from encounter_eval.label_scoring import eligible, measure, metrics


def inputs():
    p = [{"id": i, "folder_id": 1, "timestamp": f"2026-01-01T00:00:0{i}"} for i in range(1, 4)]
    a = {str(i): {"taxa": ["bird"], "complete": False, "sources": ["manual"]} for i in range(1, 4)}
    return p, a


def test_partial_positive_labels_score_short_sequence_without_manual_group_review():
    p, a = inputs()
    together = measure(p, a, [Group((1, 2, 3), ("bird",), "")])
    split = measure(p, a, [Group((1,), ("bird",), ""), Group((2,), ("bird",), ""), Group((3,), ("bird",), "")])
    assert together["same_label_short_pairs"] == 2 and together["same_label_short_splits"] == 0
    assert split["same_label_short_splits"] == 2
    assert metrics(together)["objective"] < metrics(split)["objective"]
    assert together["complete_labels"] == 0 and together["incorrect_additions"] == 0


@pytest.mark.parametrize(
    "unknown", ["missing_time", "long_gap", "different_folder", "multiple_species", "unlabeled", "reverse_time"]
)
def test_unknown_boundaries_not_invented(unknown):
    p, a = inputs()
    p = p[:2]
    a.pop("3")
    if unknown == "missing_time":
        p[1]["timestamp"] = None
    elif unknown == "long_gap":
        p[1]["timestamp"] = "2026-01-01T00:00:10"
    elif unknown == "different_folder":
        p[1]["folder_id"] = 2
    elif unknown == "multiple_species":
        a["2"]["taxa"].append("other")
    elif unknown == "unlabeled":
        a.pop("2")
    elif unknown == "reverse_time":
        p[1]["timestamp"] = "2026-01-01T00:00:00"
    c = measure(p, a, [Group((1, 2), ("bird",), "")])
    assert c["same_label_short_pairs"] == c["different_label_short_pairs"] == 0


def test_different_partial_labels_are_only_a_conservative_join_proxy():
    p, a = inputs()
    a["2"]["taxa"] = ["other"]
    c = measure(p, a, [Group((1, 2, 3), ("bird", "other"), "")])
    assert c["different_label_short_joins"] == 2 and c["incorrect_additions"] == 0
    assert c["unverified_additions"] == 3


def test_all_species_cannot_game_the_selection():
    p, a = inputs()
    base = metrics(measure(p, a, [Group((1, 2, 3), ("bird",), "")]))
    bad = metrics(measure(p, a, [Group((1, 2, 3), ("bird", "other"), "")]))
    assert not eligible(bad, base)


def test_manual_provenance_separate_from_imported():
    p, a = inputs()
    a["2"]["sources"] = ["unknown"]
    c = measure(p, a, [Group((1, 2, 3), ("bird",), "")])
    assert c["manual_positive_labels"] == 2 and c["manual_recovered_positive_labels"] == 2
    assert c["manual_same_label_pairs"] == 0 and c["same_label_short_pairs"] == 2


def test_abstaining_does_not_win():
    p, a = inputs()
    base = metrics(measure(p, a, [Group((1, 2, 3), ("bird",), "")]))
    bad = metrics(measure(p, a, [Group((1, 2, 3), None, "")]))
    assert not eligible(bad, base) and bad["objective"] > base["objective"]


def test_unverified_addition_is_not_treated_as_a_proven_error():
    base = {"objective": 1.0, "counts": {"recovered_positive_labels": 10, "unverified_additions": 2}}
    improved = {"objective": 0.9, "counts": {"recovered_positive_labels": 12, "unverified_additions": 3}}
    assert eligible(improved, base)
    improved["counts"]["unverified_additions"] = 5
    assert not eligible(improved, base)
