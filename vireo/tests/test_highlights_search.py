"""Highlights text search over its in-memory photo rows."""

from highlights_payload import filter_highlight_sections


def _photo(pid, species=None, predicted=None):
    return {"id": pid, "filename": f"{pid}.jpg", "species": species,
            "predicted_species": predicted, "is_unidentified": species is None and not predicted}


def test_search_skips_prediction_on_identified_photo():
    # A Wood duck photo whose top prediction disagrees answers search by its
    # species, not by the classifier guess; an unidentified photo still
    # answers by its prediction.
    duck = _photo(1, species="Wood duck", predicted="Least Grebe")
    guessed = _photo(2, predicted="Least Grebe")
    buckets = [{"species": "Wood duck", "photos": [duck]},
               {"species": "Least Grebe", "photos": [guessed]}]

    found, _ = filter_highlight_sections(buckets, [], "least grebe")
    assert [p["id"] for b in found for p in b["photos"]] == [2]

    found, _ = filter_highlight_sections(buckets, [], "wood duck")
    assert [p["id"] for b in found for p in b["photos"]] == [1]


def test_search_uses_canonical_accepted_name_without_classifier_guess():
    from highlights_payload import collect_highlight_buckets
    buckets, other = collect_highlight_buckets(
        [{"id": 1, "filename": "bird.jpg", "species": "Gray Jay",
          "predicted_species": "Least Grebe", "predicted_confidence": 0.9}],
        0.5, canonicalize_species=lambda _: "Canada Jay")
    assert buckets[0]["species"] == "Canada Jay"
    found, _ = filter_highlight_sections(buckets, other, "Canada")
    assert [p["id"] for b in found for p in b["photos"]] == [1]
    found, _ = filter_highlight_sections(buckets, other, "Least Grebe")
    assert not found
