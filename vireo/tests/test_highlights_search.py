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


def test_species_canonicalizer_reads_the_case_convention_once(tmp_path, monkeypatch):
    """A workspace-wide Highlights page resolves about a thousand predicted
    names no keyword holds. Learning the casing to apply reloads the settings
    file and scans every species keyword, so it happens once per page."""
    from db import Database
    from highlights_payload import species_canonicalizer
    from repositories.keyword_taxa import KeywordTaxonRepository

    db = Database(str(tmp_path / "catalog.db"))
    try:
        for name in ("Black phoebe", "Wood duck", "Least grebe"):
            db.keywords.add(name, is_species=True)
        names = ["Canada Jay", "Steller's Jay", "Black Phoebe", "Canada Jay"]
        expected = [db.keyword_taxa.resolve_species_display(name) for name in names]
        assert expected[:2] == ["Canada jay", "Steller's jay"]

        calls = []
        detect = KeywordTaxonRepository.species_case_convention
        monkeypatch.setattr(
            KeywordTaxonRepository, "species_case_convention",
            lambda self: calls.append(1) or detect(self),
        )
        canonicalize = species_canonicalizer(db)
        assert [canonicalize(name) for name in names] == expected
        assert len(calls) == 1
    finally:
        db.close()


def test_species_match_key_remembers_names_without_changing_any_answer():
    from keyword_normalization import keyword_match_key
    from species_memberships import species_match_key

    for name in ("Black Phoebe", "  black   PHOEBE ", "Éclair", "’Apapane", "", None):
        assert species_match_key(name) == keyword_match_key(name)
        assert species_match_key(name) == keyword_match_key(name)
