# auto-labeler/tests/test_taxonomy.py
import json
import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))


def _create_mock_taxonomy(tmpdir):
    """Create a small taxonomy.json for testing without downloading."""
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
            "lincoln's sparrow": {
                "taxon_id": 9136,
                "scientific_name": "Melospiza lincolnii",
                "common_name": "Lincoln's Sparrow",
                "rank": "species",
                "lineage_names": ["Animalia", "Chordata", "Aves", "Passeriformes", "Passerellidae", "Melospiza", "Melospiza lincolnii"],
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
            "new world sparrows": {
                "taxon_id": 200986,
                "scientific_name": "Passerellidae",
                "common_name": "New World Sparrows",
                "rank": "family",
                "lineage_names": ["Animalia", "Chordata", "Aves", "Passeriformes", "Passerellidae"],
                "lineage_ranks": ["kingdom", "phylum", "class", "order", "family"],
            },
            "mallard": {
                "taxon_id": 6930,
                "scientific_name": "Anas platyrhynchos",
                "common_name": "Mallard",
                "rank": "species",
                "lineage_names": ["Animalia", "Chordata", "Aves", "Anseriformes", "Anatidae", "Anas", "Anas platyrhynchos"],
                "lineage_ranks": ["kingdom", "phylum", "class", "order", "family", "genus", "species"],
            },
        },
        "taxa_by_scientific": {
            "melospiza melodia": {
                "taxon_id": 9135,
                "scientific_name": "Melospiza melodia",
                "common_name": "Song Sparrow",
                "rank": "species",
                "lineage_names": ["Animalia", "Chordata", "Aves", "Passeriformes", "Passerellidae", "Melospiza", "Melospiza melodia"],
                "lineage_ranks": ["kingdom", "phylum", "class", "order", "family", "genus", "species"],
            },
            "passerellidae": {
                "taxon_id": 200986,
                "scientific_name": "Passerellidae",
                "common_name": "New World Sparrows",
                "rank": "family",
                "lineage_names": ["Animalia", "Chordata", "Aves", "Passeriformes", "Passerellidae"],
                "lineage_ranks": ["kingdom", "phylum", "class", "order", "family"],
            },
        },
    }
    path = os.path.join(tmpdir, "taxonomy.json")
    with open(path, 'w') as f:
        json.dump(taxonomy, f)
    return path


def test_load_taxonomy():
    """Taxonomy.load() reads taxonomy.json and allows lookups."""
    from taxonomy import Taxonomy

    with tempfile.TemporaryDirectory() as tmpdir:
        tax_path = _create_mock_taxonomy(tmpdir)
        tax = Taxonomy(tax_path)

        result = tax.lookup("Song Sparrow")
        assert result is not None
        assert result['rank'] == 'species'
        assert result['scientific_name'] == 'Melospiza melodia'


def test_lookup_case_insensitive():
    """Lookup is case-insensitive for common names."""
    from taxonomy import Taxonomy

    with tempfile.TemporaryDirectory() as tmpdir:
        tax_path = _create_mock_taxonomy(tmpdir)
        tax = Taxonomy(tax_path)

        assert tax.lookup("song sparrow") is not None
        assert tax.lookup("SONG SPARROW") is not None
        assert tax.lookup("Song Sparrow") is not None


def test_lookup_scientific_name():
    """Lookup works for scientific names too."""
    from taxonomy import Taxonomy

    with tempfile.TemporaryDirectory() as tmpdir:
        tax_path = _create_mock_taxonomy(tmpdir)
        tax = Taxonomy(tax_path)

        result = tax.lookup("Melospiza melodia")
        assert result is not None
        assert result['common_name'] == 'Song Sparrow'


def test_lookup_not_found():
    """Lookup returns None for non-taxa like locations."""
    from taxonomy import Taxonomy

    with tempfile.TemporaryDirectory() as tmpdir:
        tax_path = _create_mock_taxonomy(tmpdir)
        tax = Taxonomy(tax_path)

        assert tax.lookup("Dyke Marsh") is None
        assert tax.lookup("0Locations") is None


def test_is_taxon():
    """is_taxon returns True for taxa, False for non-taxa."""
    from taxonomy import Taxonomy

    with tempfile.TemporaryDirectory() as tmpdir:
        tax_path = _create_mock_taxonomy(tmpdir)
        tax = Taxonomy(tax_path)

        assert tax.is_taxon("Song Sparrow") is True
        assert tax.is_taxon("Dyke Marsh") is False


def test_relationship_same():
    """relationship returns 'same' for identical taxa."""
    from taxonomy import Taxonomy

    with tempfile.TemporaryDirectory() as tmpdir:
        tax_path = _create_mock_taxonomy(tmpdir)
        tax = Taxonomy(tax_path)

        assert tax.relationship("Song Sparrow", "Song Sparrow") == 'same'


def test_relationship_ancestor():
    """relationship returns 'ancestor' when a is an ancestor of b."""
    from taxonomy import Taxonomy

    with tempfile.TemporaryDirectory() as tmpdir:
        tax_path = _create_mock_taxonomy(tmpdir)
        tax = Taxonomy(tax_path)

        # New World Sparrows (family) is an ancestor of Song Sparrow (species)
        assert tax.relationship("New World Sparrows", "Song Sparrow") == 'ancestor'


def test_relationship_descendant():
    """relationship returns 'descendant' when b is an ancestor of a."""
    from taxonomy import Taxonomy

    with tempfile.TemporaryDirectory() as tmpdir:
        tax_path = _create_mock_taxonomy(tmpdir)
        tax = Taxonomy(tax_path)

        # Song Sparrow (species) is a descendant of New World Sparrows (family)
        assert tax.relationship("Song Sparrow", "New World Sparrows") == 'descendant'


def test_relationship_sibling():
    """relationship returns 'sibling' for species in the same genus."""
    from taxonomy import Taxonomy

    with tempfile.TemporaryDirectory() as tmpdir:
        tax_path = _create_mock_taxonomy(tmpdir)
        tax = Taxonomy(tax_path)

        # Song Sparrow and Lincoln's Sparrow are both in genus Melospiza
        assert tax.relationship("Song Sparrow", "Lincoln's Sparrow") == 'sibling'


def test_relationship_unrelated():
    """relationship returns 'unrelated' for taxa in different families."""
    from taxonomy import Taxonomy

    with tempfile.TemporaryDirectory() as tmpdir:
        tax_path = _create_mock_taxonomy(tmpdir)
        tax = Taxonomy(tax_path)

        # Mallard (Anatidae) vs Song Sparrow (Passerellidae)
        assert tax.relationship("Mallard", "Song Sparrow") == 'unrelated'


def test_relationship_same_family_different_genus():
    """Species in the same family but different genus are 'unrelated'."""
    from taxonomy import Taxonomy

    with tempfile.TemporaryDirectory() as tmpdir:
        tax_path = _create_mock_taxonomy(tmpdir)
        tax = Taxonomy(tax_path)

        # Carolina Wren (Thryothorus) and Northern House Wren (Troglodytes)
        # both in family Troglodytidae but different genus
        result = tax.relationship("Carolina Wren", "Northern House Wren")
        assert result == 'unrelated'


def test_relationship_unknown_taxon():
    """relationship returns None when one or both names are not in taxonomy."""
    from taxonomy import Taxonomy

    with tempfile.TemporaryDirectory() as tmpdir:
        tax_path = _create_mock_taxonomy(tmpdir)
        tax = Taxonomy(tax_path)

        assert tax.relationship("Song Sparrow", "Unknown Bird") is None
        assert tax.relationship("Unknown Bird", "Song Sparrow") is None
        assert tax.relationship("Unknown A", "Unknown B") is None


def test_relationship_same_cross_lookup():
    """relationship returns 'same' when common name matches scientific name."""
    from taxonomy import Taxonomy

    with tempfile.TemporaryDirectory() as tmpdir:
        tax_path = _create_mock_taxonomy(tmpdir)
        tax = Taxonomy(tax_path)

        assert tax.relationship("Song Sparrow", "Melospiza melodia") == 'same'
