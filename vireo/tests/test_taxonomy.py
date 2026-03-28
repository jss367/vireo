# vireo/tests/test_taxonomy.py
import gzip
import json
import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from db import Database


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


# ---------------------------------------------------------------------------
# Tests for load_taxa_from_file (iNat AWS open-data taxa.csv.gz loader)
# ---------------------------------------------------------------------------

def _make_taxa_tsv(tmp_path):
    """Create a small test taxa.csv.gz matching iNat AWS format.

    Format: tab-separated, 6 columns:
    taxon_id, ancestry, rank_level, rank, name, active
    """
    lines = [
        "48460\t\t100\tstateofmatter\tLife\ttrue",
        # Animalia
        "1\t48460\t70\tkingdom\tAnimalia\ttrue",
        "2\t48460/1\t60\tphylum\tChordata\ttrue",
        "3\t48460/1/2\t50\tclass\tAves\ttrue",
        "71261\t48460/1/2/3\t40\torder\tAccipitriformes\ttrue",
        "5067\t48460/1/2/3/71261\t30\tfamily\tAccipitridae\ttrue",
        "5269\t48460/1/2/3/71261/5067\t20\tgenus\tIchthyophaga\ttrue",
        "5270\t48460/1/2/3/71261/5067/5269\t10\tspecies\tIchthyophaga ichthyaetus\ttrue",
        # Intermediate rank that should be skipped
        "355675\t48460/1/2\t57\tsubphylum\tVertebrata\ttrue",
        # Plantae
        "47126\t48460\t70\tkingdom\tPlantae\ttrue",
        "211194\t48460/47126\t60\tphylum\tTracheophyta\ttrue",
        # Fungi
        "47170\t48460\t70\tkingdom\tFungi\ttrue",
        "47169\t48460/47170\t60\tphylum\tBasidiomycota\ttrue",
        # Inactive taxon — should be skipped
        "99999\t48460/1/2/3\t40\torder\tObsoleteOrder\tfalse",
        # Bacteria — should be skipped (not Animalia/Plantae/Fungi)
        "67333\t48460\t70\tkingdom\tBacteria\ttrue",
        "67334\t48460/67333\t60\tphylum\tProteobacteria\ttrue",
        # Falconiformes for informal groups test
        "71268\t48460/1/2/3\t40\torder\tFalconiformes\ttrue",
        "5273\t48460/1/2/3/71268\t30\tfamily\tFalconidae\ttrue",
        "4714\t48460/1/2/3/71268/5273\t20\tgenus\tFalco\ttrue",
        "4647\t48460/1/2/3/71268/5273/4714\t10\tspecies\tFalco peregrinus\ttrue",
    ]
    path = str(tmp_path / "taxa.csv.gz")
    with gzip.open(path, 'wt') as f:
        f.write('\n'.join(lines) + '\n')
    return path


def test_load_taxa_from_file(tmp_path):
    """load_taxa_from_file imports filtered taxa into the database."""
    from taxonomy import load_taxa_from_file

    db = Database(str(tmp_path / "test.db"))
    tsv_path = _make_taxa_tsv(tmp_path)

    stats = load_taxa_from_file(db, tsv_path)

    assert stats["loaded"] > 0
    assert stats["skipped"] > 0

    # Animalia kingdom loaded
    row = db.conn.execute(
        "SELECT * FROM taxa WHERE inat_id = 1"
    ).fetchone()
    assert row is not None
    assert row["name"] == "Animalia"
    assert row["rank"] == "kingdom"
    assert row["kingdom"] == "Animalia"

    # Species loaded with correct parent chain
    species = db.conn.execute(
        "SELECT * FROM taxa WHERE inat_id = 5270"
    ).fetchone()
    assert species is not None
    assert species["name"] == "Ichthyophaga ichthyaetus"
    assert species["rank"] == "species"
    assert species["kingdom"] == "Animalia"
    # Parent should be genus (5269), not a skipped intermediate rank
    genus = db.conn.execute(
        "SELECT * FROM taxa WHERE inat_id = 5269"
    ).fetchone()
    assert species["parent_id"] == genus["id"]

    # Plantae and Fungi loaded
    assert db.conn.execute(
        "SELECT 1 FROM taxa WHERE inat_id = 47126"
    ).fetchone() is not None
    assert db.conn.execute(
        "SELECT 1 FROM taxa WHERE inat_id = 47170"
    ).fetchone() is not None

    # Bacteria NOT loaded
    assert db.conn.execute(
        "SELECT 1 FROM taxa WHERE inat_id = 67333"
    ).fetchone() is None

    # Inactive taxon NOT loaded
    assert db.conn.execute(
        "SELECT 1 FROM taxa WHERE inat_id = 99999"
    ).fetchone() is None

    # Intermediate rank (subphylum) NOT loaded
    assert db.conn.execute(
        "SELECT 1 FROM taxa WHERE inat_id = 355675"
    ).fetchone() is None


def test_load_taxa_idempotent(tmp_path):
    """Running load_taxa_from_file twice does not create duplicates."""
    from taxonomy import load_taxa_from_file

    db = Database(str(tmp_path / "test.db"))
    tsv_path = _make_taxa_tsv(tmp_path)

    load_taxa_from_file(db, tsv_path)
    count1 = db.conn.execute("SELECT COUNT(*) FROM taxa").fetchone()[0]

    load_taxa_from_file(db, tsv_path)
    count2 = db.conn.execute("SELECT COUNT(*) FROM taxa").fetchone()[0]

    assert count1 == count2


from unittest.mock import MagicMock, patch


def test_fetch_common_names(tmp_path):
    """fetch_common_names stores names from iNat API into taxa_common_names."""
    from taxonomy import fetch_common_names, load_taxa_from_file

    db = Database(str(tmp_path / "test.db"))
    tsv_path = _make_taxa_tsv(tmp_path)
    load_taxa_from_file(db, tsv_path)

    # Mock the iNat API response
    def mock_get(url, params=None, **kwargs):
        resp = MagicMock()
        resp.status_code = 200
        # Return common names for the taxa IDs requested
        results = []
        for inat_id_str in params.get('id', '').split(','):
            inat_id = int(inat_id_str)
            if inat_id == 5270:
                results.append({
                    'id': 5270,
                    'preferred_common_name': 'Grey-headed Fish Eagle',
                    'names': [
                        {'name': 'Grey-headed Fish Eagle', 'locale': 'en'},
                        {'name': 'Gray-headed Fish-Eagle', 'locale': 'en'},
                    ],
                })
            elif inat_id == 4647:
                results.append({
                    'id': 4647,
                    'preferred_common_name': 'Peregrine Falcon',
                    'names': [
                        {'name': 'Peregrine Falcon', 'locale': 'en'},
                    ],
                })
        resp.json.return_value = {'results': results}
        return resp

    with patch('taxonomy.requests.get', side_effect=mock_get):
        stats = fetch_common_names(db)

    # Check preferred common name set on taxa row
    row = db.conn.execute(
        "SELECT common_name FROM taxa WHERE inat_id = 5270"
    ).fetchone()
    assert row["common_name"] == "Grey-headed Fish Eagle"

    # Check alternate names in taxa_common_names
    names = [
        r["name"] for r in db.conn.execute(
            "SELECT name FROM taxa_common_names WHERE taxon_id = "
            "(SELECT id FROM taxa WHERE inat_id = 5270)"
        ).fetchall()
    ]
    assert "Grey-headed Fish Eagle" in names
    assert "Gray-headed Fish-Eagle" in names

    assert stats["updated"] > 0
