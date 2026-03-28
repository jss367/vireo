# vireo/tests/test_compare.py
import json
import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))


def _write_test_xmp(path, keywords):
    """Write a minimal XMP file with dc:subject keywords."""
    from xmp import write_sidecar
    write_sidecar(path, flat_keywords=set(keywords), hierarchical_keywords=set())


def _create_mock_taxonomy(tmpdir):
    """Create a small taxonomy.json for testing."""
    taxonomy = {
        "last_updated": "2026-03-17",
        "source": "test",
        "taxa_by_common": {
            "song sparrow": {
                "taxon_id": 9135,
                "scientific_name": "Melospiza melodia",
                "common_name": "Song Sparrow",
                "rank": "species",
                "lineage_names": ["Animalia", "Chordata", "Aves", "Passeriformes", "Passerellidae", "Melospiza", "Melospiza melodia"],
                "lineage_ranks": ["kingdom", "phylum", "class", "order", "family", "genus", "species"],
            },
            "sparrow": {
                "taxon_id": 200986,
                "scientific_name": "Passerellidae",
                "common_name": "Sparrow",
                "rank": "family",
                "lineage_names": ["Animalia", "Chordata", "Aves", "Passeriformes", "Passerellidae"],
                "lineage_ranks": ["kingdom", "phylum", "class", "order", "family"],
            },
            "northern cardinal": {
                "taxon_id": 9083,
                "scientific_name": "Cardinalis cardinalis",
                "common_name": "Northern Cardinal",
                "rank": "species",
                "lineage_names": ["Animalia", "Chordata", "Aves", "Passeriformes", "Cardinalidae", "Cardinalis", "Cardinalis cardinalis"],
                "lineage_ranks": ["kingdom", "phylum", "class", "order", "family", "genus", "species"],
            },
            "blue jay": {
                "taxon_id": 8229,
                "scientific_name": "Cyanocitta cristata",
                "common_name": "Blue Jay",
                "rank": "species",
                "lineage_names": ["Animalia", "Chordata", "Aves", "Passeriformes", "Corvidae", "Cyanocitta", "Cyanocitta cristata"],
                "lineage_ranks": ["kingdom", "phylum", "class", "order", "family", "genus", "species"],
            },
            "carolina wren": {
                "taxon_id": 7581,
                "scientific_name": "Thryothorus ludovicianus",
                "common_name": "Carolina Wren",
                "rank": "species",
                "lineage_names": ["Animalia", "Chordata", "Aves", "Passeriformes", "Troglodytidae", "Thryothorus", "Thryothorus ludovicianus"],
                "lineage_ranks": ["kingdom", "phylum", "class", "order", "family", "genus", "species"],
            },
            "northern house wren": {
                "taxon_id": 7582,
                "scientific_name": "Troglodytes aedon",
                "common_name": "Northern House Wren",
                "rank": "species",
                "lineage_names": ["Animalia", "Chordata", "Aves", "Passeriformes", "Troglodytidae", "Troglodytes", "Troglodytes aedon"],
                "lineage_ranks": ["kingdom", "phylum", "class", "order", "family", "genus", "species"],
            },
        },
        "taxa_by_scientific": {},
    }
    path = os.path.join(tmpdir, "taxonomy.json")
    with open(path, 'w') as f:
        json.dump(taxonomy, f)
    return path


def test_categorize_match():
    """Exact match returns 'match'."""
    from compare import categorize
    from taxonomy import Taxonomy

    with tempfile.TemporaryDirectory() as tmpdir:
        tax = Taxonomy(_create_mock_taxonomy(tmpdir))
        result = categorize('Northern cardinal', {'Northern cardinal', 'Dyke Marsh'}, tax)
        assert result == 'match'


def test_categorize_new():
    """No existing species keywords returns 'new'."""
    from compare import categorize
    from taxonomy import Taxonomy

    with tempfile.TemporaryDirectory() as tmpdir:
        tax = Taxonomy(_create_mock_taxonomy(tmpdir))
        result = categorize('Northern cardinal', {'Dyke Marsh', '0Locations'}, tax)
        assert result == 'new'


def test_categorize_refinement_family_to_species():
    """Existing is family, prediction is species in that family -> refinement."""
    from compare import categorize
    from taxonomy import Taxonomy

    with tempfile.TemporaryDirectory() as tmpdir:
        tax = Taxonomy(_create_mock_taxonomy(tmpdir))
        # "sparrow" mapped to family Passerellidae, "Song sparrow" is a species in that family
        result = categorize('Song sparrow', {'sparrow', 'Dyke Marsh'}, tax)
        assert result == 'refinement'


def test_categorize_disagreement_different_species():
    """Different species returns 'disagreement'."""
    from compare import categorize
    from taxonomy import Taxonomy

    with tempfile.TemporaryDirectory() as tmpdir:
        tax = Taxonomy(_create_mock_taxonomy(tmpdir))
        result = categorize('Blue jay', {'Northern cardinal', 'Dyke Marsh'}, tax)
        assert result == 'disagreement'


def test_categorize_disagreement_same_family_species():
    """Two species in different genera of same family -> disagreement (not refinement)."""
    from compare import categorize
    from taxonomy import Taxonomy

    with tempfile.TemporaryDirectory() as tmpdir:
        tax = Taxonomy(_create_mock_taxonomy(tmpdir))
        # Carolina Wren and Northern House Wren are both species (not family->species)
        result = categorize('Northern House Wren', {'Carolina Wren'}, tax)
        assert result == 'disagreement'


def test_categorize_ignores_non_taxa():
    """Keywords not in taxonomy are ignored (locations, categories)."""
    from compare import categorize
    from taxonomy import Taxonomy

    with tempfile.TemporaryDirectory() as tmpdir:
        tax = Taxonomy(_create_mock_taxonomy(tmpdir))
        # "Dyke Marsh" and "0Locations" not in taxonomy -> treated as new
        result = categorize('Northern cardinal', {'Dyke Marsh', '0Locations'}, tax)
        assert result == 'new'
