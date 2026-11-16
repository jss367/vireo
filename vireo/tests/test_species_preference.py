"""Ranking a species' chosen photos by recorded comparisons."""

from species_preference import (
    Comparison,
    Preferences,
    choice_note,
    rank_contenders,
    split_contradictions,
)


def _c(choice_id, better, worse):
    return Comparison(choice_id, better, worse)


def test_uncompared_contenders_tie_in_the_order_given():
    ranking = rank_contenders([3, 1, 2], [])

    assert ranking.order == (3, 1, 2)
    assert ranking.leaders == (3, 1, 2)
    assert ranking.tied


def test_a_single_contender_is_settled():
    ranking = rank_contenders([7], [_c(1, 9, 7)])

    assert ranking.order == (7,)
    assert ranking.leaders == (7,)
    assert not ranking.tied


def test_a_comparison_overrides_the_tie_break_order():
    ranking = rank_contenders([2, 1], [_c(1, 1, 2)])

    assert ranking.order == (1, 2)
    assert ranking.leaders == (1,)
    assert not ranking.tied


def test_a_comparison_counts_through_a_photo_outside_the_set():
    comparisons = [_c(1, 1, 5), _c(2, 5, 2)]

    ranking = rank_contenders([2, 1], comparisons)

    assert ranking.order == (1, 2)
    assert ranking.leaders == (1,)


def test_a_chain_through_a_photo_not_chosen_for_the_species_is_inert():
    # 1 beat 5, 5 beat 2, but the ranking is asked under a species 5 is no
    # longer chosen for (a relabel), so the chain breaks.
    comparisons = [_c(1, 1, 5), _c(2, 5, 2)]

    ranking = rank_contenders([2, 1], comparisons, chosen_for_species=[1, 2])

    assert ranking.leaders == (2, 1)
    assert ranking.tied


def test_two_winners_of_separate_sets_stay_tied():
    # 1 beat 2 on one trip, 3 beat 4 on another; nobody compared 1 with 3.
    comparisons = [_c(1, 1, 2), _c(2, 3, 4)]

    ranking = rank_contenders([4, 3, 2, 1], comparisons)

    assert ranking.leaders == (3, 1)
    assert ranking.order == (3, 4, 1, 2)
    assert ranking.tied


def test_the_newest_decision_wins_a_contradiction():
    comparisons = [_c(1, 1, 2), _c(2, 2, 1)]

    kept, contradicted = split_contradictions(comparisons)

    assert kept == [_c(2, 2, 1)]
    assert contradicted == [_c(1, 1, 2)]
    assert rank_contenders([1, 2], comparisons).order == (2, 1)


def test_an_older_comparison_closing_a_cycle_is_contradicted():
    comparisons = [_c(1, 1, 2), _c(2, 2, 3), _c(3, 3, 1)]

    kept, contradicted = split_contradictions(comparisons)

    assert kept == [_c(3, 3, 1), _c(2, 2, 3)]
    assert contradicted == [_c(1, 1, 2)]
    assert rank_contenders([1, 2, 3], comparisons).order == (2, 3, 1)


def test_one_choice_over_several_photos_is_never_self_contradicting():
    comparisons = [_c(4, 1, 2), _c(4, 1, 3)]

    kept, contradicted = split_contradictions(comparisons)

    assert sorted(kept, key=lambda c: c.worse) == comparisons
    assert contradicted == []


def test_a_photo_compared_with_itself_is_ignored():
    kept, contradicted = split_contradictions([_c(1, 5, 5)])

    assert kept == []
    assert contradicted == [_c(1, 5, 5)]


def test_photos_that_are_not_contenders_never_lead():
    ranking = rank_contenders([2], [_c(1, 9, 2)])

    assert ranking.order == (2,)
    assert ranking.leaders == (2,)


def test_duplicate_contenders_are_ranked_once():
    assert rank_contenders([1, 1, 2], []).order == (1, 2)


def test_one_preferences_object_ranks_any_number_of_sets():
    preferences = Preferences([_c(1, 1, 2), _c(2, 3, 4)])

    assert preferences.rank([2, 1]).order == (1, 2)
    assert preferences.rank([4, 3]).order == (3, 4)
    assert preferences.rank([4, 1]).leaders == (4, 1)
    assert Preferences().rank([5, 6]).leaders == (5, 6)


def test_choice_note_only_returns_a_note_recorded_for_the_species():
    # A multi-species photo's note belongs to the species the choice was made
    # for; without a tracked alias, a choice under another name never carries
    # over to the species being rendered.
    notes = {
        7: [
            {"species": "Verdin", "reason": "first"},
            {"species": "Bushtit", "reason": "other species"},
        ],
        8: [{"species": "Old Name", "reason": "before the rename"}],
    }

    assert choice_note(notes, "Verdin", 7)["reason"] == "first"
    assert choice_note(notes, "Bushtit", 7)["reason"] == "other species"
    assert choice_note(notes, "New Name", 8) is None
    assert choice_note(notes, "Verdin", 9) is None
