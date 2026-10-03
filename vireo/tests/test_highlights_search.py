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
