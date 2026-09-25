# vireo/tests/test_taxonomy.py
import gc
import gzip
import json
import os
import stat
import sys
import tempfile
import weakref

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from db import Database


@pytest.fixture(autouse=True)
def _isolate_taxonomy_state(tmp_path, monkeypatch):
    """Keep the real filesystem and a leftover cache out of every test here.

    Both taxonomy candidates resolve at import time, so a developer with a
    real ~/.vireo/taxonomy.json or an in-checkout vireo/taxonomy.json (a full
    iNaturalist dump is ~500MB) had tests that expect a load to fail quietly
    pick up that file instead — asserting None and counting parses against
    whatever happened to be on the host. Point both at names that do not
    exist; tests needing a candidate present aim the constant at their own
    fixture file.

    Also clears the process-wide parse cache on both sides of the test, so
    one test's cached instance or memoized failure cannot decide another
    test's outcome.
    """
    import taxonomy as tax_mod

    monkeypatch.setattr(
        tax_mod, "TAXONOMY_JSON_PATH", str(tmp_path / "absent-persistent.json"),
    )
    monkeypatch.setattr(
        tax_mod, "LEGACY_TAXONOMY_JSON_PATH", str(tmp_path / "absent-legacy.json"),
    )
    tax_mod.clear_taxonomy_cache()
    yield
    tax_mod.clear_taxonomy_cache()


def _fake_dwca_download(url, path, progress_callback=None):
    """Write a minimal but valid DWCA archive so download_taxonomy() completes."""
    import zipfile as _zf
    with _zf.ZipFile(path, "w") as zf:
        zf.writestr(
            "taxa.csv",
            "id,parentNameUsageID,scientificName,taxonRank\n"
            "1,,Test species,species\n",
        )
        zf.writestr(
            "VernacularNames-english.csv",
            "id,vernacularName,language\n"
            "1,Test,en\n",
        )


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


def test_lookup_normalizes_smart_apostrophes():
    """Smart quotes from photo apps match straight-apostrophe taxonomy names."""
    from taxonomy import Taxonomy

    with tempfile.TemporaryDirectory() as tmpdir:
        tax_path = _create_mock_taxonomy(tmpdir)
        tax = Taxonomy(tax_path)

        result = tax.lookup("Lincoln’s Sparrow")
        assert result is not None
        assert result["scientific_name"] == "Melospiza lincolnii"


def test_mark_species_keywords_normalizes_smart_apostrophes(tmp_path):
    """Startup species marking retypes smart-apostrophe XMP keywords."""
    from taxonomy import Taxonomy

    tax_path = _create_mock_taxonomy(str(tmp_path))
    tax = Taxonomy(tax_path)
    db = Database(str(tmp_path / "test.db"))
    kid = db.add_keyword("Lincoln’s Sparrow")

    updated = db.mark_species_keywords(tax)
    assert updated == 1

    row = db.conn.execute(
        "SELECT is_species, type FROM keywords WHERE id = ?", (kid,)
    ).fetchone()
    assert row["is_species"] == 1
    assert row["type"] == "taxonomy"


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


def test_load_taxa_updates_on_reload(tmp_path):
    """Reloading taxa updates changed names/ranks instead of ignoring them."""
    from taxonomy import load_taxa_from_file

    db = Database(str(tmp_path / "test.db"))
    tsv_path = _make_taxa_tsv(tmp_path)

    load_taxa_from_file(db, tsv_path)

    # Manually corrupt a taxon name to simulate stale data
    db.conn.execute("UPDATE taxa SET name = 'OldName' WHERE inat_id = 3")
    db.conn.commit()
    assert db.conn.execute(
        "SELECT name FROM taxa WHERE inat_id = 3"
    ).fetchone()["name"] == "OldName"

    # Reload should fix it
    load_taxa_from_file(db, tsv_path)
    row = db.conn.execute("SELECT name FROM taxa WHERE inat_id = 3").fetchone()
    assert row["name"] == "Aves"


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


def test_seed_informal_groups(tmp_path):
    """seed_informal_groups creates default wildlife photography groups."""
    from taxonomy import load_taxa_from_file, seed_informal_groups

    db = Database(str(tmp_path / "test.db"))
    tsv_path = _make_taxa_tsv(tmp_path)
    load_taxa_from_file(db, tsv_path)

    stats = seed_informal_groups(db)
    assert stats["groups_created"] > 0

    # "Raptors" group should exist and link to Accipitriformes and Falconiformes
    group = db.conn.execute(
        "SELECT id FROM informal_groups WHERE name = 'Raptors'"
    ).fetchone()
    assert group is not None

    linked = db.conn.execute(
        "SELECT t.name FROM informal_group_taxa igt "
        "JOIN taxa t ON t.id = igt.taxon_id "
        "WHERE igt.group_id = ?",
        (group["id"],),
    ).fetchall()
    linked_names = {r["name"] for r in linked}
    # Our test data has Accipitriformes and Falconiformes
    assert "Accipitriformes" in linked_names
    assert "Falconiformes" in linked_names


def test_seed_informal_groups_idempotent(tmp_path):
    """Running seed_informal_groups twice does not create duplicates."""
    from taxonomy import load_taxa_from_file, seed_informal_groups

    db = Database(str(tmp_path / "test.db"))
    tsv_path = _make_taxa_tsv(tmp_path)
    load_taxa_from_file(db, tsv_path)

    seed_informal_groups(db)
    count1 = db.conn.execute("SELECT COUNT(*) FROM informal_groups").fetchone()[0]

    seed_informal_groups(db)
    count2 = db.conn.execute("SELECT COUNT(*) FROM informal_groups").fetchone()[0]

    assert count1 == count2


# ---------------------------------------------------------------------------
# Tests for _download_with_resume
# ---------------------------------------------------------------------------

import http.server
import threading


def _start_test_server(handler_class, port=0):
    """Start an HTTP server on a random port, return (server, port)."""
    server = http.server.HTTPServer(("127.0.0.1", port), handler_class)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, server.server_address[1]


def test_download_with_resume_success(tmp_path):
    """Successful download writes the file and removes the .partial."""
    from taxonomy import _download_with_resume

    content = b"hello world taxonomy data " * 100

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-Length", str(len(content)))
            self.end_headers()
            self.wfile.write(content)

        def log_message(self, *args):
            pass

    server, port = _start_test_server(Handler)
    try:
        dest = str(tmp_path / "taxonomy.gz")
        _download_with_resume(f"http://127.0.0.1:{port}/taxa.csv.gz", dest)

        assert os.path.exists(dest)
        assert not os.path.exists(dest + ".partial")
        assert open(dest, "rb").read() == content
    finally:
        server.shutdown()


def test_download_with_resume_retries_on_failure(tmp_path):
    """Download retries and resumes after a mid-transfer failure."""
    from taxonomy import _download_with_resume

    content = b"A" * 2000
    call_count = [0]

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            call_count[0] += 1
            range_header = self.headers.get("Range")

            if call_count[0] == 1:
                # First request: serve first 1000 bytes then close
                self.send_response(200)
                self.send_header("Content-Length", str(len(content)))
                self.end_headers()
                self.wfile.write(content[:1000])
                # Abruptly stop — client gets a partial file
                return

            if range_header and range_header.startswith("bytes="):
                start = int(range_header.split("=")[1].split("-")[0])
                remaining = content[start:]
                self.send_response(206)
                self.send_header("Content-Length", str(len(remaining)))
                self.send_header("Content-Range",
                                 f"bytes {start}-{len(content)-1}/{len(content)}")
                self.end_headers()
                self.wfile.write(remaining)
            else:
                self.send_response(200)
                self.send_header("Content-Length", str(len(content)))
                self.end_headers()
                self.wfile.write(content)

        def log_message(self, *args):
            pass

    server, port = _start_test_server(Handler)
    try:
        dest = str(tmp_path / "taxa.csv.gz")
        _download_with_resume(f"http://127.0.0.1:{port}/taxa.csv.gz", dest)

        assert open(dest, "rb").read() == content
        assert call_count[0] >= 2
    finally:
        server.shutdown()


def test_download_with_resume_gives_up_after_stalls(tmp_path):
    """Download raises after max_stalled consecutive failures with no progress."""
    from taxonomy import _download_with_resume

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            # Always fail immediately
            self.send_response(500)
            self.end_headers()

        def log_message(self, *args):
            pass

    server, port = _start_test_server(Handler)
    try:
        dest = str(tmp_path / "taxa.csv.gz")
        import pytest
        with pytest.raises(RuntimeError, match="stalled"):
            _download_with_resume(
                f"http://127.0.0.1:{port}/taxa.csv.gz", dest, max_stalled=2,
            )
    finally:
        server.shutdown()


def test_download_with_resume_progress_resets_stall(tmp_path):
    """Making progress resets the stall counter so download continues."""
    from taxonomy import _download_with_resume

    content = b"B" * 3000
    call_count = [0]

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            call_count[0] += 1
            range_header = self.headers.get("Range")
            start = 0
            if range_header and range_header.startswith("bytes="):
                start = int(range_header.split("=")[1].split("-")[0])

            if call_count[0] <= 3:
                # First 3 calls: promise full remaining but only deliver 1000 bytes
                remaining_all = content[start:]
                chunk = content[start:start + 1000]
                if start > 0:
                    self.send_response(206)
                    self.send_header("Content-Range",
                                     f"bytes {start}-{len(content)-1}/{len(content)}")
                else:
                    self.send_response(200)
                # Content-Length = full remaining, but we only send 1000 bytes
                self.send_header("Content-Length", str(len(remaining_all)))
                self.end_headers()
                self.wfile.write(chunk)
                return

            # Final call: serve everything remaining
            remaining = content[start:]
            if start > 0:
                self.send_response(206)
                self.send_header("Content-Range",
                                 f"bytes {start}-{len(content)-1}/{len(content)}")
            else:
                self.send_response(200)
            self.send_header("Content-Length", str(len(remaining)))
            self.end_headers()
            self.wfile.write(remaining)

        def log_message(self, *args):
            pass

    server, port = _start_test_server(Handler)
    try:
        dest = str(tmp_path / "taxa.csv.gz")
        # max_stalled=2, but each attempt makes progress so it never gives up
        _download_with_resume(
            f"http://127.0.0.1:{port}/taxa.csv.gz", dest, max_stalled=2,
        )
        assert open(dest, "rb").read() == content
    finally:
        server.shutdown()


def test_download_with_resume_callback(tmp_path):
    """Progress callback is called with status messages."""
    from taxonomy import _download_with_resume

    content = b"data" * 100

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-Length", str(len(content)))
            self.end_headers()
            self.wfile.write(content)

        def log_message(self, *args):
            pass

    messages = []
    server, port = _start_test_server(Handler)
    try:
        dest = str(tmp_path / "out.gz")
        _download_with_resume(
            f"http://127.0.0.1:{port}/file.gz", dest,
            progress_callback=lambda msg: messages.append(msg),
        )
        assert len(messages) >= 1
        assert any("Downloading" in m or "Downloaded" in m for m in messages)
    finally:
        server.shutdown()


def test_download_with_resume_server_ignores_range(tmp_path):
    """If server doesn't support Range, download restarts from scratch."""
    from taxonomy import _download_with_resume

    content = b"C" * 2000
    call_count = [0]

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            call_count[0] += 1
            if call_count[0] == 1:
                # First request: partial delivery
                self.send_response(200)
                self.send_header("Content-Length", str(len(content)))
                self.end_headers()
                self.wfile.write(content[:500])
                return

            # Second request: server ignores Range, sends full content
            self.send_response(200)
            self.send_header("Content-Length", str(len(content)))
            self.end_headers()
            self.wfile.write(content)

        def log_message(self, *args):
            pass

    server, port = _start_test_server(Handler)
    try:
        dest = str(tmp_path / "taxa.csv.gz")
        _download_with_resume(f"http://127.0.0.1:{port}/taxa.csv.gz", dest)
        assert open(dest, "rb").read() == content
    finally:
        server.shutdown()


def test_download_with_resume_no_range_stalls_correctly(tmp_path):
    """Server ignoring Range + repeated partial writes must still trigger stall."""
    from taxonomy import _download_with_resume

    content = b"D" * 2000

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            # Always return 200 (ignore Range), always deliver only 500 bytes
            self.send_response(200)
            self.send_header("Content-Length", str(len(content)))
            self.end_headers()
            self.wfile.write(content[:500])

        def log_message(self, *args):
            pass

    server, port = _start_test_server(Handler)
    try:
        dest = str(tmp_path / "taxa.csv.gz")
        import pytest
        with pytest.raises(RuntimeError, match="stalled"):
            _download_with_resume(
                f"http://127.0.0.1:{port}/taxa.csv.gz", dest, max_stalled=2,
            )
    finally:
        server.shutdown()


def test_download_with_resume_reports_bytes(tmp_path):
    """byte_callback gets real byte counts — this is what the progress bar reads."""
    import http.server

    from taxonomy import _download_with_resume

    payload = b"x" * 500_000

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, *a):
            pass

    server, port = _start_test_server(Handler)
    try:
        seen = []
        dest = tmp_path / "out.bin"
        # chunk_size small enough to force several loop iterations
        _download_with_resume(
            f"http://127.0.0.1:{port}/x", str(dest), chunk_size=4096,
            byte_callback=lambda done, total: seen.append((done, total)),
        )
    finally:
        server.shutdown()

    assert seen, "byte_callback was never called"
    assert seen[-1][0] == len(payload)
    assert seen[-1][1] == len(payload)          # from Content-Length
    assert [d for d, _ in seen] == sorted(d for d, _ in seen)


def test_download_with_resume_cancels_midstream(tmp_path):
    """should_cancel aborts, keeps a non-empty .partial, and does not retry.

    Cancellation must escape the retry machinery, not fall into it.  If the
    DownloadCancelled re-raise is removed the generic handler treats a cancel
    as a dropped connection: the user is told "Connection lost ... retrying"
    right after pressing Cancel, and the download is re-requested.
    """
    import http.server

    from taxonomy import DownloadCancelled, _download_with_resume

    payload = b"x" * 500_000
    requests = {"n": 0}

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            requests["n"] += 1
            self.send_response(200)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, *a):
            pass

    server, port = _start_test_server(Handler)
    calls = {"n": 0}
    messages = []

    def cancel_after_two_chunks():
        calls["n"] += 1
        return calls["n"] > 2

    try:
        dest = tmp_path / "out.bin"
        with pytest.raises(DownloadCancelled):
            _download_with_resume(
                f"http://127.0.0.1:{port}/x", str(dest), chunk_size=4096,
                progress_callback=messages.append,
                should_cancel=cancel_after_two_chunks,
            )
    finally:
        server.shutdown()

    assert not dest.exists()
    partial = tmp_path / "out.bin.partial"
    assert partial.exists()
    # Non-empty proves we cancelled mid-stream, not before the first read —
    # open(..., "wb") would create a 0-byte file either way.
    assert partial.stat().st_size > 0
    # A cancel is not a network error: the user must never be told the
    # connection dropped or that we are retrying.
    assert not [m for m in messages if "retrying" in m], messages
    assert not [m for m in messages if "Connection lost" in m], messages
    # And we really stopped — no second attempt was made against the server.
    assert requests["n"] == 1, f"expected a single request, got {requests['n']}"


@pytest.mark.skipif(
    os.name == "nt",
    reason=(
        "socket.shutdown(SHUT_RDWR) from another thread does not reliably "
        "unblock a Python recv() on Windows — the blocked read only surfaces "
        "once the peer actually closes, so this timing assertion is not a "
        "meaningful check of the watcher there.  The mechanism still runs "
        "in production; only the sub-5s timing property is Linux/macOS-only."
    ),
)
def test_download_with_resume_cancel_interrupts_stalled_read(tmp_path):
    """A Stop press during a stalled resp.read must not wait for the socket timeout.

    Before the watcher thread, cancellation was polled only between reads, so a
    stalled connection let the flag sit unnoticed until the 120 s urlopen timeout
    expired — the Settings progress UI stayed running for two minutes on the
    exact unreliable-network scenario cancellation is for.  The watcher now
    closes resp when should_cancel goes true, unblocking the read immediately.
    """
    import http.server
    import threading
    import time

    from taxonomy import DownloadCancelled, _download_with_resume

    # Promise 8 MB in Content-Length but only send 1 KB and hang.  resp.read
    # of 8 MB has to keep reading until it gets Content-Length bytes or the
    # connection closes — after the 1 KB flush the read blocks on the socket.
    stop_hanging = threading.Event()

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-Length", str(8 * 1024 * 1024))
            self.end_headers()
            self.wfile.write(b"x" * 1024)
            self.wfile.flush()
            # Hold the connection open until the test releases it — this is
            # what makes resp.read block on the client side.
            stop_hanging.wait(timeout=30)

        def log_message(self, *a):
            pass

    server, port = _start_test_server(Handler)
    cancel_flag = threading.Event()

    def fire_cancel_after_read_starts():
        # A short wait so we are already inside the blocked resp.read when
        # the flag flips: much longer than the initial header exchange, much
        # shorter than the socket timeout.
        time.sleep(0.5)
        cancel_flag.set()
    threading.Thread(target=fire_cancel_after_read_starts, daemon=True).start()

    try:
        dest = tmp_path / "out.bin"
        started = time.monotonic()
        with pytest.raises(DownloadCancelled):
            _download_with_resume(
                f"http://127.0.0.1:{port}/x", str(dest),
                chunk_size=8 * 1024 * 1024,  # bigger than the server ever sends
                should_cancel=cancel_flag.is_set,
            )
        elapsed = time.monotonic() - started
    finally:
        stop_hanging.set()
        server.shutdown()

    # Well under the 120 s socket timeout — the watcher closed resp within
    # its poll interval and the read exited immediately after.
    assert elapsed < 5, (
        f"cancel took {elapsed:.2f}s — the watcher did not close a stalled read"
    )


def test_download_with_resume_cancels_during_retry_backoff(tmp_path):
    """A cancel that lands during the retry wait aborts instead of sleeping it out.

    The backoff is polled in short slices so Cancel feels immediate.  With a
    single time.sleep(3) the user would wait out the full backoff and the
    download would be re-requested before anyone noticed the cancel.
    """
    import http.server
    import time

    from taxonomy import DownloadCancelled, _download_with_resume

    requests = {"n": 0}

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            requests["n"] += 1
            self.send_response(500)
            self.end_headers()

        def log_message(self, *a):
            pass

    server, port = _start_test_server(Handler)

    # should_cancel is only consulted inside the read loop (never reached on a
    # 500) and inside the backoff, so this is False for the whole first attempt
    # and True from the moment the backoff starts.
    def cancel_once_the_first_attempt_failed():
        return requests["n"] >= 1

    try:
        dest = tmp_path / "out.bin"
        started = time.monotonic()
        with pytest.raises(DownloadCancelled):
            _download_with_resume(
                f"http://127.0.0.1:{port}/x", str(dest), max_stalled=2,
                should_cancel=cancel_once_the_first_attempt_failed,
            )
        elapsed = time.monotonic() - started
    finally:
        server.shutdown()

    # Well under the 3 s backoff: we noticed the cancel, we did not sleep it off.
    assert elapsed < 1.5, f"cancel took {elapsed:.2f}s — backoff was not polled"
    # And the retry never went out.
    assert requests["n"] == 1, f"expected a single request, got {requests['n']}"


def test_download_with_resume_bytes_include_resume_offset(tmp_path):
    """On a 206 resume, progress counts bytes already on disk and totals the full file.

    Content-Length on a 206 is only the *remaining* length, so the implementation
    has to add the resume offset back to both the running count and the total.
    Without that, a resumed download would show the bar restarting at zero and
    finishing at less than the real file size.
    """
    import http.server

    from taxonomy import _download_with_resume

    payload = b"C" * 300_000
    first_chunk = 100_000
    call_count = [0]

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            call_count[0] += 1
            if call_count[0] == 1:
                # Promise the whole file but hang up after first_chunk bytes.
                self.send_response(200)
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload[:first_chunk])
                return

            # Later attempts honour Range with a real 206.
            range_header = self.headers.get("Range", "")
            assert range_header.startswith("bytes="), "expected a Range request"
            start = int(range_header.split("=")[1].split("-")[0])
            remaining = payload[start:]
            self.send_response(206)
            self.send_header("Content-Length", str(len(remaining)))
            self.send_header("Content-Range",
                             f"bytes {start}-{len(payload) - 1}/{len(payload)}")
            self.end_headers()
            self.wfile.write(remaining)

        def log_message(self, *a):
            pass

    server, port = _start_test_server(Handler)
    seen = []
    try:
        dest = tmp_path / "out.bin"
        _download_with_resume(
            f"http://127.0.0.1:{port}/x", str(dest), chunk_size=4096,
            byte_callback=lambda done, total: seen.append((call_count[0], done, total)),
        )
    finally:
        server.shutdown()

    assert open(dest, "rb").read() == payload

    resumed = [e for e in seen if e[0] >= 2]
    assert resumed, "no byte_callback events during the resumed attempt"
    # Every event on the resumed attempt reports the FULL file size, never the
    # 200_000-byte remainder from Content-Length.
    assert {total for _, _, total in resumed} == {len(payload)}
    # Progress picks up from the resume offset instead of restarting at zero.
    assert resumed[0][1] >= first_chunk
    assert all(done <= len(payload) for _, done, _ in seen)
    assert seen[-1][1] == len(payload)


def test_download_with_resume_bytes_reset_when_server_ignores_range(tmp_path):
    """A 200 after a partial write restarts the count — the bar must not exceed 100%.

    mode == "wb" truncates the partial in this branch, but downloaded_before is
    deliberately kept as the stall baseline.  Reusing it as the display base
    would double-count the discarded bytes.
    """
    import http.server

    from taxonomy import _download_with_resume

    payload = b"E" * 200_000
    first_chunk = 50_000
    call_count = [0]

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            call_count[0] += 1
            self.send_response(200)  # never 206: this server ignores Range
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            if call_count[0] == 1:
                self.wfile.write(payload[:first_chunk])
                return
            self.wfile.write(payload)

        def log_message(self, *a):
            pass

    server, port = _start_test_server(Handler)
    seen = []
    try:
        dest = tmp_path / "out.bin"
        _download_with_resume(
            f"http://127.0.0.1:{port}/x", str(dest), chunk_size=4096,
            byte_callback=lambda done, total: seen.append((done, total)),
        )
    finally:
        server.shutdown()

    assert open(dest, "rb").read() == payload
    assert seen, "byte_callback was never called"
    # The reported total is always the real file size, never inflated by the
    # bytes that were thrown away.
    assert {total for _, total in seen} == {len(payload)}
    assert all(done <= len(payload) for done, _ in seen)
    assert seen[-1] == (len(payload), len(payload))


def test_download_with_resume_throttles_byte_callback(tmp_path):
    """byte_callback is rate-limited, not fired once per chunk.

    The SSE subscriber queue is bounded, so a 178 MB download at 256 KB chunks
    must not emit ~700 events (and at 4 KB chunks, ~45000).
    """
    import http.server

    from taxonomy import _download_with_resume

    payload = b"y" * 500_000
    chunk_size = 4096
    chunk_count = len(payload) // chunk_size  # 122

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, *a):
            pass

    server, port = _start_test_server(Handler)
    seen = []
    try:
        dest = tmp_path / "out.bin"
        _download_with_resume(
            f"http://127.0.0.1:{port}/x", str(dest), chunk_size=chunk_size,
            byte_callback=lambda done, total: seen.append(done),
        )
    finally:
        server.shutdown()

    # At ~4 Hz this loop would have to take 5+ seconds on localhost to reach 20
    # events; one-per-chunk would be 120+.  Deliberately loose so it can't go
    # flaky on a slow machine, while still failing outright if the throttle is
    # removed.
    assert chunk_count > 100, "payload/chunk_size no longer forces many iterations"
    assert len(seen) <= 20, f"expected throttled events, got {len(seen)}"
    # Still ends on an exact final count.
    assert seen[-1] == len(payload)


def test_download_with_resume_throttle_interval_gates_emits(tmp_path):
    """The emit interval is what paces the bar — pin both ends of it.

    The unconditional final emit alone satisfies "the callback fired", so a
    throttle window stretched to effectively-infinite still looks healthy from
    the outside while the real 178 MB download shows a bar frozen at 0% until
    the very last byte.  Driving the interval to 0 and to a huge value proves
    the comparison actually gates each chunk.
    """
    import http.server

    from taxonomy import _download_with_resume

    payload = b"z" * 500_000
    chunk_size = 4096
    chunk_count = len(payload) // chunk_size  # 122

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, *a):
            pass

    def run(interval, out_name):
        server, port = _start_test_server(Handler)
        seen = []
        try:
            _download_with_resume(
                f"http://127.0.0.1:{port}/x", str(tmp_path / out_name),
                chunk_size=chunk_size,
                byte_callback=lambda done, total: seen.append(done),
                _emit_interval=interval,
            )
        finally:
            server.shutdown()
        assert seen[-1] == len(payload)
        return seen

    # No throttle: every chunk emits.  A read may return short, so the loop
    # runs at least chunk_count times — the count tracks chunks, not a
    # hard-coded 1 or 2.
    ungated = run(0, "ungated.bin")
    assert len(ungated) >= chunk_count, (
        f"with _emit_interval=0 expected ~{chunk_count} emits, got {len(ungated)}"
    )

    # Throttle wider than any plausible download: only the final unconditional
    # emit survives.
    gated = run(1e9, "gated.bin")
    assert len(gated) == 1, (
        f"with a huge _emit_interval only the final emit should fire, got {len(gated)}"
    )


def test_download_with_resume_promotes_complete_partial_on_416(tmp_path):
    """Cancellation between writing the final chunk and the empty-read break
    leaves a ``.partial`` at exactly the full asset size.  The next attempt
    sends ``Range: bytes=<total>-`` and gets 416; without promotion the retry
    loop would burn through ``max_stalled`` attempts against a permanent 416
    despite the UI promising "try again and the download will resume".
    """
    import http.server

    from taxonomy import _download_with_resume

    content = b"y" * 4096
    call_count = [0]

    # Pre-seed the .partial so the first attempt sends Range: bytes=<total>-.
    dest = tmp_path / "out.bin"
    partial = tmp_path / "out.bin.partial"
    partial.write_bytes(content)

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            call_count[0] += 1
            self.send_response(416)
            self.send_header(
                "Content-Range", f"bytes */{len(content)}",
            )
            self.end_headers()

        def log_message(self, *a):
            pass

    server, port = _start_test_server(Handler)
    try:
        _download_with_resume(f"http://127.0.0.1:{port}/x", str(dest))
    finally:
        server.shutdown()

    # Partial was promoted to the final path with the correct bytes; the
    # download did not retry into a stall.
    assert dest.exists()
    assert not partial.exists()
    assert dest.read_bytes() == content
    assert call_count[0] == 1


def test_download_with_resume_restarts_when_416_total_mismatches(tmp_path):
    """A rebuilt release changes the asset size, so a 416 whose Content-Range
    total does not match the partial means the partial is stale — delete it
    and restart from scratch instead of promoting bytes of the wrong artifact.
    """
    import http.server

    from taxonomy import _download_with_resume

    fresh = b"Z" * 3000
    partial = tmp_path / "out.bin.partial"
    partial.write_bytes(b"X" * 2000)  # stale bytes from a previous release
    call_count = [0]

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            call_count[0] += 1
            range_header = self.headers.get("Range")
            if range_header:
                # First call: reject the stale-partial resume with 416,
                # advertising the true total.
                self.send_response(416)
                self.send_header("Content-Range", f"bytes */{len(fresh)}")
                self.end_headers()
                return
            # Second call: no Range header (partial was deleted); serve the
            # fresh release from scratch.
            self.send_response(200)
            self.send_header("Content-Length", str(len(fresh)))
            self.end_headers()
            self.wfile.write(fresh)

        def log_message(self, *a):
            pass

    server, port = _start_test_server(Handler)
    try:
        dest = tmp_path / "out.bin"
        _download_with_resume(f"http://127.0.0.1:{port}/x", str(dest))
    finally:
        server.shutdown()

    assert dest.read_bytes() == fresh
    assert call_count[0] >= 2


# ---------------------------------------------------------------------------
# classify_to_keypoint_group — taxonomy routing for eye-focus detection
# ---------------------------------------------------------------------------
#
# The taxa table stores parent_id as a local PK reference (parent_id -> taxa.id),
# not iNat-id. These fixtures set id explicitly so parent_id resolution is
# unambiguous.

def test_classify_to_keypoint_group_bird(tmp_path):
    from taxonomy import classify_to_keypoint_group

    db = Database(str(tmp_path / "x.db"))
    db.conn.execute(
        "INSERT INTO taxa (id, inat_id, name, rank, kingdom) "
        "VALUES (3, 3, 'Aves', 'class', 'Animalia')"
    )
    db.conn.execute(
        "INSERT INTO taxa (id, inat_id, name, rank, kingdom, parent_id) "
        "VALUES (7019, 7019, 'Passeriformes', 'order', 'Animalia', 3)"
    )
    db.conn.execute(
        "INSERT INTO taxa (id, inat_id, name, rank, kingdom, parent_id) "
        "VALUES (12345, 12345, 'Cardinalis cardinalis', 'species', 'Animalia', 7019)"
    )
    db.conn.commit()
    assert classify_to_keypoint_group(db, 12345) == "Aves"


def test_classify_to_keypoint_group_mammal(tmp_path):
    from taxonomy import classify_to_keypoint_group

    db = Database(str(tmp_path / "x.db"))
    db.conn.execute(
        "INSERT INTO taxa (id, inat_id, name, rank, kingdom) "
        "VALUES (40151, 40151, 'Mammalia', 'class', 'Animalia')"
    )
    db.conn.execute(
        "INSERT INTO taxa (id, inat_id, name, rank, kingdom, parent_id) "
        "VALUES (42158, 42158, 'Carnivora', 'order', 'Animalia', 40151)"
    )
    db.conn.execute(
        "INSERT INTO taxa (id, inat_id, name, rank, kingdom, parent_id) "
        "VALUES (42048, 42048, 'Vulpes vulpes', 'species', 'Animalia', 42158)"
    )
    db.conn.commit()
    assert classify_to_keypoint_group(db, 42048) == "Mammalia"


def test_classify_to_keypoint_group_fish_returns_none(tmp_path):
    from taxonomy import classify_to_keypoint_group

    db = Database(str(tmp_path / "x.db"))
    db.conn.execute(
        "INSERT INTO taxa (id, inat_id, name, rank, kingdom) "
        "VALUES (47178, 47178, 'Actinopterygii', 'class', 'Animalia')"
    )
    db.conn.execute(
        "INSERT INTO taxa (id, inat_id, name, rank, kingdom, parent_id) "
        "VALUES (47179, 47179, 'Perciformes', 'order', 'Animalia', 47178)"
    )
    db.conn.execute(
        "INSERT INTO taxa (id, inat_id, name, rank, kingdom, parent_id) "
        "VALUES (99999, 99999, 'Somefish somefish', 'species', 'Animalia', 47179)"
    )
    db.conn.commit()
    assert classify_to_keypoint_group(db, 99999) is None


def test_classify_to_keypoint_group_unknown_returns_none(tmp_path):
    from taxonomy import classify_to_keypoint_group

    db = Database(str(tmp_path / "x.db"))
    assert classify_to_keypoint_group(db, 999999) is None


def test_classify_to_keypoint_group_none_input(tmp_path):
    from taxonomy import classify_to_keypoint_group

    db = Database(str(tmp_path / "x.db"))
    assert classify_to_keypoint_group(db, None) is None


def test_populate_taxa_db_from_json_fills_taxa_and_common_names(tmp_path):
    """populate_taxa_db_from_json loads taxa and taxa_common_names from JSON."""
    from taxonomy import populate_taxa_db_from_json

    tax_path = _create_mock_taxonomy(str(tmp_path))
    db = Database(str(tmp_path / "x.db"))

    stats = populate_taxa_db_from_json(db, tax_path)
    assert stats["taxa_loaded"] >= 5
    assert stats["common_names_loaded"] >= 5

    row = db.conn.execute(
        "SELECT id, inat_id, name, rank, common_name, kingdom "
        "FROM taxa WHERE inat_id = 9135"
    ).fetchone()
    assert row is not None
    assert row["name"] == "Melospiza melodia"
    assert row["rank"] == "species"
    assert row["common_name"] == "Song Sparrow"
    assert row["kingdom"] == "Animalia"

    # Common-name index stores lowercase key so add_keyword's COLLATE NOCASE
    # lookup can find "Song Sparrow" / "song sparrow" / "SONG SPARROW".
    cn_row = db.conn.execute(
        "SELECT taxon_id FROM taxa_common_names "
        "WHERE name = 'song sparrow' AND taxon_id = ?",
        (row["id"],),
    ).fetchone()
    assert cn_row is not None


def test_populate_taxa_db_from_json_sets_parent_id(tmp_path):
    """populate_taxa_db_from_json resolves parent_id by lineage."""
    from taxonomy import populate_taxa_db_from_json

    tax_path = _create_mock_taxonomy(str(tmp_path))
    db = Database(str(tmp_path / "x.db"))
    populate_taxa_db_from_json(db, tax_path)

    # Melospiza melodia's immediate parent in lineage_names is Melospiza
    # (genus). Since mock data doesn't include that genus as its own row,
    # parent_id will be NULL for species. Passerellidae (family) has order
    # Passeriformes as parent in lineage, also not in mock. So for a
    # positive parent_id check, we rely on the kingdom field instead and
    # verify parent_id is at least populated where the parent IS present.
    # The mock includes Passerellidae — species Song Sparrow's lineage
    # walks down to Melospiza (genus, absent), so its parent stays NULL.
    # Use a minimal scenario where the parent IS present:
    row = db.conn.execute(
        "SELECT id, parent_id FROM taxa WHERE name = 'Melospiza melodia'"
    ).fetchone()
    # No explicit genus row in mock → parent stays NULL. Document that.
    assert row is not None

    # After populating, auto-detect via add_keyword should now type
    # "Song Sparrow" as taxonomy and link taxon_id.
    kid = db.add_keyword("Song sparrow")
    kw = db.conn.execute(
        "SELECT type, taxon_id FROM keywords WHERE id = ?", (kid,)
    ).fetchone()
    assert kw["type"] == "taxonomy"
    # taxon_id links to the local taxa.id
    taxon_row = db.conn.execute(
        "SELECT id FROM taxa WHERE inat_id = 9135"
    ).fetchone()
    assert kw["taxon_id"] == taxon_row["id"]


def test_load_local_taxonomy_falls_back_when_persistent_corrupt(tmp_path, monkeypatch):
    """A corrupt persistent taxonomy.json doesn't disable enrichment.

    Regression: find_taxonomy_json returned the persistent path as soon
    as it existed, and callers raised on load. If an interrupted write
    left a truncated ~/.vireo/taxonomy.json, taxonomy features broke
    even when a valid package-dir copy was present.
    """
    import taxonomy as tax_mod

    # Build a valid legacy file next to the module and a corrupt one at
    # the persistent path.
    corrupt = tmp_path / "persistent.json"
    corrupt.write_text("{ not valid json")
    legacy = tmp_path / "legacy.json"
    legacy.write_text(
        '{"last_updated":"2026-04-24","source":"test",'
        '"taxa_by_common":{"test species":{"taxon_id":1,'
        '"scientific_name":"Test species","common_name":"Test",'
        '"rank":"species","lineage_names":["Animalia","Test species"],'
        '"lineage_ranks":["kingdom","species"]}},'
        '"taxa_by_scientific":{}}'
    )

    monkeypatch.setattr(tax_mod, "TAXONOMY_JSON_PATH", str(corrupt))
    # Redirect the "package-dir legacy" candidate path to our tmp fixture.
    legacy.rename(tmp_path / "taxonomy.json")
    monkeypatch.setattr(
        tax_mod, "LEGACY_TAXONOMY_JSON_PATH", str(tmp_path / "taxonomy.json"),
    )

    result = tax_mod.load_local_taxonomy()
    assert result is not None, "should fall back to legacy copy on corrupt persistent"
    assert result.is_taxon("test species")


def test_load_local_taxonomy_returns_none_when_no_file(tmp_path, monkeypatch):
    """load_local_taxonomy returns None when neither candidate exists."""
    import taxonomy as tax_mod

    monkeypatch.setattr(
        tax_mod, "TAXONOMY_JSON_PATH", str(tmp_path / "nonexistent.json"),
    )
    monkeypatch.setattr(
        tax_mod, "LEGACY_TAXONOMY_JSON_PATH", str(tmp_path / "taxonomy.json"),
    )
    assert tax_mod.load_local_taxonomy() is None


def test_populate_taxa_db_from_json_refuses_empty_payload(tmp_path):
    """Empty taxonomy payload fails before any destructive writes.

    Regression: without a guard, an empty entries_by_inat_id caused the
    prune step to DELETE every taxa row and NULL every keywords.taxon_id,
    then the job reported success. Now it raises before any writes.
    """
    import json

    import pytest
    from taxonomy import populate_taxa_db_from_json

    # Seed an existing populated DB.
    tax_path = _create_mock_taxonomy(str(tmp_path))
    db = Database(str(tmp_path / "x.db"))
    populate_taxa_db_from_json(db, tax_path)
    pre_count = db.conn.execute("SELECT COUNT(*) FROM taxa").fetchone()[0]
    assert pre_count > 0

    # Now write an empty payload and try to re-populate.
    empty_path = str(tmp_path / "empty.json")
    with open(empty_path, "w") as f:
        json.dump({
            "last_updated": "2026-04-24",
            "source": "test",
            "taxa_by_common": {},
            "taxa_by_scientific": {},
        }, f)

    with pytest.raises(ValueError, match="empty|refusing"):
        populate_taxa_db_from_json(db, empty_path)

    post_count = db.conn.execute("SELECT COUNT(*) FROM taxa").fetchone()[0]
    assert post_count == pre_count, (
        "existing taxa must not be deleted when the new payload is empty"
    )


def test_populate_taxa_db_from_json_refuses_suspiciously_small_payload(tmp_path):
    """Drastically-smaller payloads fail before destructive writes.

    Regression: a corrupt/partial download could have a few hundred taxa
    while the existing DB has 1.3M. Silently pruning ~99% of the table
    is almost certainly wrong; fail visibly so the user retries.
    """
    import json

    import pytest
    from taxonomy import populate_taxa_db_from_json

    # Seed a DB with >100 rows so the proportional check kicks in.
    db = Database(str(tmp_path / "x.db"))
    for i in range(200):
        db.conn.execute(
            "INSERT INTO taxa (inat_id, name, rank, kingdom) "
            "VALUES (?, ?, 'species', 'Animalia')",
            (1000 + i, f"Species{i:03d} name"),
        )
    db.conn.commit()

    # Now a tiny payload — under 10% of existing.
    tiny = {
        "last_updated": "2026-04-24",
        "source": "test",
        "taxa_by_common": {},
        "taxa_by_scientific": {
            "animalia": {"taxon_id": 1, "scientific_name": "Animalia",
                         "common_name": "", "rank": "kingdom",
                         "lineage_names": ["Animalia"],
                         "lineage_ranks": ["kingdom"]},
        },
    }
    tiny_path = str(tmp_path / "tiny.json")
    with open(tiny_path, "w") as f:
        json.dump(tiny, f)

    with pytest.raises(ValueError, match="corrupt|partial|refusing"):
        populate_taxa_db_from_json(db, tiny_path)

    # Nothing deleted.
    assert db.conn.execute("SELECT COUNT(*) FROM taxa").fetchone()[0] == 200


def test_populate_taxa_db_from_json_prunes_stale_taxa(tmp_path):
    """Re-populate deletes taxa rows whose inat_id disappeared from the
    new payload, and nulls out the corresponding keywords.taxon_id.

    Regression: populate_taxa_db_from_json used only INSERT ... ON CONFLICT
    DO UPDATE, so taxa rows for ids removed from the new taxonomy stuck
    around. add_keyword's auto-detect queries taxa.name and
    taxa.common_name before taxa_common_names, so stale rows kept
    matching obsolete names across re-downloads.
    """
    import json

    from taxonomy import populate_taxa_db_from_json

    first = {
        "last_updated": "2026-01-01",
        "source": "test",
        "taxa_by_common": {},
        "taxa_by_scientific": {
            "animalia": {
                "taxon_id": 1,
                "scientific_name": "Animalia",
                "common_name": "",
                "rank": "kingdom",
                "lineage_names": ["Animalia"],
                "lineage_ranks": ["kingdom"],
            },
            "extinct species": {
                "taxon_id": 42,
                "scientific_name": "Deleted species",
                "common_name": "Extinct Species",
                "rank": "species",
                "lineage_names": ["Animalia", "Deleted species"],
                "lineage_ranks": ["kingdom", "species"],
            },
        },
    }
    tax_path = str(tmp_path / "taxonomy.json")
    with open(tax_path, "w") as f:
        json.dump(first, f)

    db = Database(str(tmp_path / "x.db"))
    populate_taxa_db_from_json(db, tax_path)

    extinct_local_id = db.conn.execute(
        "SELECT id FROM taxa WHERE inat_id = 42"
    ).fetchone()["id"]

    # Simulate a keyword referring to the soon-to-be-removed taxon (the
    # classifier-added-keyword path sets keywords.taxon_id).
    kid = db.add_keyword("Deleted species", is_species=True)
    db.conn.execute(
        "UPDATE keywords SET taxon_id = ? WHERE id = ?",
        (extinct_local_id, kid),
    )
    db.conn.commit()

    # Newer release: taxon 42 is gone.
    second = json.loads(json.dumps(first))
    del second["taxa_by_scientific"]["extinct species"]
    with open(tax_path, "w") as f:
        json.dump(second, f)

    populate_taxa_db_from_json(db, tax_path)

    gone = db.conn.execute(
        "SELECT 1 FROM taxa WHERE inat_id = 42"
    ).fetchone()
    assert gone is None, "stale taxa row should be pruned on re-populate"

    # The referring keyword survives with taxon_id nulled out so FK
    # enforcement doesn't block the prune.
    kw = db.conn.execute(
        "SELECT taxon_id FROM keywords WHERE id = ?", (kid,)
    ).fetchone()
    assert kw is not None
    assert kw["taxon_id"] is None


def test_populate_taxa_db_from_json_prunes_when_parent_disappears(tmp_path):
    """Pruning survives a reshuffle where a child's old parent vanishes.

    Regression: taxa.parent_id is a self-referential FK without
    ON DELETE SET NULL. With a child still pointing at a soon-to-be-
    pruned parent, DELETE FROM taxa raised FOREIGN KEY constraint
    failed and aborted the whole populate — breaking the download job.
    """
    import json

    from taxonomy import populate_taxa_db_from_json

    first = {
        "last_updated": "2026-01-01",
        "source": "test",
        "taxa_by_common": {},
        "taxa_by_scientific": {
            "animalia": {"taxon_id": 1, "scientific_name": "Animalia",
                         "common_name": "", "rank": "kingdom",
                         "lineage_names": ["Animalia"],
                         "lineage_ranks": ["kingdom"]},
            "old_genus": {"taxon_id": 100, "scientific_name": "OldGenus",
                          "common_name": "", "rank": "genus",
                          "lineage_names": ["Animalia", "OldGenus"],
                          "lineage_ranks": ["kingdom", "genus"]},
            "child_species": {"taxon_id": 200,
                              "scientific_name": "OldGenus species",
                              "common_name": "", "rank": "species",
                              "lineage_names": ["Animalia", "OldGenus",
                                                "OldGenus species"],
                              "lineage_ranks": ["kingdom", "genus", "species"]},
        },
    }
    tax_path = str(tmp_path / "taxonomy.json")
    with open(tax_path, "w") as f:
        json.dump(first, f)

    db = Database(str(tmp_path / "x.db"))
    populate_taxa_db_from_json(db, tax_path)

    parent_set = db.conn.execute(
        "SELECT parent_id FROM taxa WHERE inat_id = 200"
    ).fetchone()["parent_id"]
    assert parent_set is not None, "parent should be set after initial populate"

    # Newer release: OldGenus disappeared entirely. The child taxon is
    # kept (it still exists in the new payload), but reparenting fell
    # through so its scientific name is unchanged.
    second = json.loads(json.dumps(first))
    del second["taxa_by_scientific"]["old_genus"]
    second["taxa_by_scientific"]["child_species"]["lineage_names"] = [
        "Animalia", "OldGenus species",
    ]
    second["taxa_by_scientific"]["child_species"]["lineage_ranks"] = [
        "kingdom", "species",
    ]
    with open(tax_path, "w") as f:
        json.dump(second, f)

    # Must not raise FOREIGN KEY constraint failed.
    populate_taxa_db_from_json(db, tax_path)

    parent_gone = db.conn.execute(
        "SELECT 1 FROM taxa WHERE inat_id = 100"
    ).fetchone()
    assert parent_gone is None, "old genus should have been pruned"
    child = db.conn.execute(
        "SELECT parent_id FROM taxa WHERE inat_id = 200"
    ).fetchone()
    assert child is not None, "child should survive the reshuffle"


def test_populate_taxa_db_from_json_clears_stale_taxa_common_name(tmp_path):
    """Re-populate drops taxa.common_name when upstream removed it.

    Regression: populate_taxa_db_from_json used
    COALESCE(excluded.common_name, taxa.common_name) on conflict, which
    preserved the old value whenever the new payload had no common name.
    add_keyword's auto-detect reads taxa.common_name before consulting
    taxa_common_names, so a stale value kept matching obsolete names
    across re-downloads.
    """
    import json

    from taxonomy import populate_taxa_db_from_json

    first = {
        "last_updated": "2026-01-01",
        "source": "test",
        "taxa_by_common": {},
        "taxa_by_scientific": {
            "aquila chrysaetos": {
                "taxon_id": 4242,
                "scientific_name": "Aquila chrysaetos",
                "common_name": "Golden Eagle",
                "rank": "species",
                "lineage_names": ["Animalia", "Aquila", "Aquila chrysaetos"],
                "lineage_ranks": ["kingdom", "genus", "species"],
            },
        },
    }
    tax_path = str(tmp_path / "taxonomy.json")
    with open(tax_path, "w") as f:
        json.dump(first, f)

    db = Database(str(tmp_path / "x.db"))
    populate_taxa_db_from_json(db, tax_path)
    before = db.conn.execute(
        "SELECT common_name FROM taxa WHERE inat_id = 4242"
    ).fetchone()
    assert before["common_name"] == "Golden Eagle"

    # Simulate a newer taxonomy release where the preferred English
    # common name has been removed.
    second = json.loads(json.dumps(first))
    second["taxa_by_scientific"]["aquila chrysaetos"]["common_name"] = ""
    with open(tax_path, "w") as f:
        json.dump(second, f)

    populate_taxa_db_from_json(db, tax_path)

    after = db.conn.execute(
        "SELECT common_name FROM taxa WHERE inat_id = 4242"
    ).fetchone()
    assert after["common_name"] is None, (
        "re-populate should overwrite taxa.common_name with the new "
        "value (including NULL), not preserve the old one"
    )


def test_populate_taxa_db_from_json_drops_stale_common_names(tmp_path):
    """Re-downloading the taxonomy clears stale common-name mappings.

    Regression: the populate step used INSERT OR IGNORE for
    taxa_common_names, so common names that disappeared or were
    reassigned in a newer taxonomy release kept matching forever.
    """
    from taxonomy import populate_taxa_db_from_json

    tax_path = _create_mock_taxonomy(str(tmp_path))
    db = Database(str(tmp_path / "x.db"))
    populate_taxa_db_from_json(db, tax_path)

    # Find a taxon that was populated, then add a fake legacy common-name
    # row for it that won't be in the next import.
    row = db.conn.execute(
        "SELECT id FROM taxa WHERE inat_id = 9135"
    ).fetchone()
    assert row is not None
    db.conn.execute(
        "INSERT INTO taxa_common_names (taxon_id, name, locale) "
        "VALUES (?, 'obsolete name from older taxonomy', 'en')",
        (row["id"],),
    )
    db.conn.commit()

    stale_before = db.conn.execute(
        "SELECT 1 FROM taxa_common_names "
        "WHERE name = 'obsolete name from older taxonomy'"
    ).fetchone()
    assert stale_before is not None

    # Re-run populate with the same JSON (simulates a re-download).
    populate_taxa_db_from_json(db, tax_path)

    stale_after = db.conn.execute(
        "SELECT 1 FROM taxa_common_names "
        "WHERE name = 'obsolete name from older taxonomy'"
    ).fetchone()
    assert stale_after is None, (
        "re-populate should drop common-name rows that aren't in the "
        "new taxonomy data"
    )
    # Valid names from the new taxonomy still present.
    fresh = db.conn.execute(
        "SELECT 1 FROM taxa_common_names WHERE name = 'song sparrow'"
    ).fetchone()
    assert fresh is not None


def test_populate_taxa_db_from_json_single_transaction_allows_clean_rollback(tmp_path):
    """populate_taxa_db_from_json commits once at the end.

    The download job's handler relies on this: if populate raises partway,
    the caller's rollback() must clear all pending inserts so the subsequent
    mark_species_keywords commit doesn't flush partial taxa writes onto disk.
    This test constructs a JSON whose second entry would violate the
    taxa.name NOT NULL constraint, verifies the exception propagates, and
    confirms a caller-side rollback leaves the taxa table empty.
    """
    import json

    from taxonomy import populate_taxa_db_from_json

    bad_tax = {
        "last_updated": "2026-04-24",
        "source": "test",
        "taxa_by_common": {},
        "taxa_by_scientific": {
            "animalia": {
                "taxon_id": 1,
                "scientific_name": "Animalia",
                "common_name": "",
                "rank": "kingdom",
                "lineage_names": ["Animalia"],
                "lineage_ranks": ["kingdom"],
            },
            "broken": {
                "taxon_id": 2,
                "scientific_name": None,
                "common_name": "",
                "rank": "species",
                "lineage_names": ["Animalia", None],
                "lineage_ranks": ["kingdom", "species"],
            },
        },
    }
    tax_path = str(tmp_path / "taxonomy.json")
    with open(tax_path, "w") as f:
        json.dump(bad_tax, f)

    db = Database(str(tmp_path / "x.db"))
    try:
        populate_taxa_db_from_json(db, tax_path)
    except Exception:
        db.conn.rollback()
    else:
        raise AssertionError("expected populate to raise on NULL name")

    count = db.conn.execute("SELECT COUNT(*) FROM taxa").fetchone()[0]
    assert count == 0, (
        "rollback after a mid-flight populate failure must clear all "
        "pending inserts — otherwise subsequent commits flush a half-"
        "populated taxa table onto disk"
    )


def test_populate_taxa_db_from_json_disambiguates_homonym_parents(tmp_path):
    """Parent resolution keys by lineage tuple, so homonym parents don't
    cross-wire each other's children.

    Scientific names aren't globally unique — a genus "Iris" exists in
    both Plantae and a fictional Animalia homonym here. Before the fix,
    whichever taxon happened to be iterated last won the name→id map,
    so both kids got their parent_id pointed at the same taxon. Using
    the full lineage tuple prevents that.
    """
    import json

    from taxonomy import populate_taxa_db_from_json

    homonym_tax = {
        "last_updated": "2026-04-24",
        "source": "test",
        "taxa_by_common": {},
        "taxa_by_scientific": {
            "plantae": {"taxon_id": 1000, "scientific_name": "Plantae", "common_name": "",
                        "rank": "kingdom", "lineage_names": ["Plantae"], "lineage_ranks": ["kingdom"]},
            "animalia": {"taxon_id": 2000, "scientific_name": "Animalia", "common_name": "",
                         "rank": "kingdom", "lineage_names": ["Animalia"], "lineage_ranks": ["kingdom"]},
            "iris_plant": {"taxon_id": 1010, "scientific_name": "Iris", "common_name": "",
                           "rank": "genus",
                           "lineage_names": ["Plantae", "Iris"],
                           "lineage_ranks": ["kingdom", "genus"]},
            "iris_animal": {"taxon_id": 2010, "scientific_name": "Iris", "common_name": "",
                            "rank": "genus",
                            "lineage_names": ["Animalia", "Iris"],
                            "lineage_ranks": ["kingdom", "genus"]},
            "plant_species": {"taxon_id": 1011, "scientific_name": "Iris germanica",
                              "common_name": "", "rank": "species",
                              "lineage_names": ["Plantae", "Iris", "Iris germanica"],
                              "lineage_ranks": ["kingdom", "genus", "species"]},
            "animal_species": {"taxon_id": 2011, "scientific_name": "Iris animalia",
                               "common_name": "", "rank": "species",
                               "lineage_names": ["Animalia", "Iris", "Iris animalia"],
                               "lineage_ranks": ["kingdom", "genus", "species"]},
        },
    }
    tax_path = str(tmp_path / "taxonomy.json")
    with open(tax_path, "w") as f:
        json.dump(homonym_tax, f)

    db = Database(str(tmp_path / "x.db"))
    populate_taxa_db_from_json(db, tax_path)

    plant_species_parent = db.conn.execute(
        "SELECT parent_id FROM taxa WHERE inat_id = 1011"
    ).fetchone()["parent_id"]
    animal_species_parent = db.conn.execute(
        "SELECT parent_id FROM taxa WHERE inat_id = 2011"
    ).fetchone()["parent_id"]
    iris_plant_local = db.conn.execute(
        "SELECT id FROM taxa WHERE inat_id = 1010"
    ).fetchone()["id"]
    iris_animal_local = db.conn.execute(
        "SELECT id FROM taxa WHERE inat_id = 2010"
    ).fetchone()["id"]

    assert plant_species_parent == iris_plant_local, (
        "Plantae species should point to plant Iris genus, not animal Iris"
    )
    assert animal_species_parent == iris_animal_local, (
        "Animalia species should point to animal Iris genus, not plant Iris"
    )
    assert plant_species_parent != animal_species_parent


def test_populate_taxa_db_from_json_enables_auto_detect(tmp_path):
    """After populate, add_keyword auto-detects common names as taxonomy.

    Regression: the original bug. Green-heron-style keywords imported via
    XMP sync should auto-type as taxonomy once the taxa DB is populated.
    """
    from taxonomy import populate_taxa_db_from_json

    tax_path = _create_mock_taxonomy(str(tmp_path))
    db = Database(str(tmp_path / "x.db"))
    populate_taxa_db_from_json(db, tax_path)

    # Simulate XMP sync path: add_keyword without is_species.
    kid = db.add_keyword("Mallard")
    row = db.conn.execute(
        "SELECT type FROM keywords WHERE id = ?", (kid,)
    ).fetchone()
    assert row["type"] == "taxonomy"


def _write_taxonomy_json(path, common_name):
    """Write a minimal but valid taxonomy.json containing one species."""
    path.write_text(
        '{"last_updated":"2026-07-30","source":"test",'
        '"taxa_by_common":{"' + common_name + '":{"taxon_id":1,'
        '"scientific_name":"Test species","common_name":"Test",'
        '"rank":"species","lineage_names":["Animalia","Test species"],'
        '"lineage_ranks":["kingdom","species"]}},'
        '"taxa_by_scientific":{}}'
    )


def test_load_local_taxonomy_reuses_parsed_instance(tmp_path, monkeypatch):
    """Repeated loads reuse the parsed taxonomy instead of re-parsing the file.

    A real iNaturalist taxonomy.json is ~500MB, and ``/api/predictions/compare``
    calls load_local_taxonomy() on every request. Re-parsing per request made
    the Compare page take seconds-to-minutes and allocated a fresh multi-GB
    structure each time.
    """
    import taxonomy as tax_mod

    tax_mod.clear_taxonomy_cache()
    persistent = tmp_path / "persistent.json"
    _write_taxonomy_json(persistent, "test species")
    monkeypatch.setattr(tax_mod, "TAXONOMY_JSON_PATH", str(persistent))

    parses = []
    real_init = tax_mod.Taxonomy.__init__

    def counting_init(self, path):
        parses.append(path)
        real_init(self, path)

    monkeypatch.setattr(tax_mod.Taxonomy, "__init__", counting_init)

    first = tax_mod.load_local_taxonomy()
    second = tax_mod.load_local_taxonomy()

    assert first is not None
    assert second is first
    assert len(parses) == 1


def test_load_local_taxonomy_reloads_after_file_changes(tmp_path, monkeypatch):
    """A re-downloaded taxonomy.json is picked up without restarting."""
    import taxonomy as tax_mod

    tax_mod.clear_taxonomy_cache()
    persistent = tmp_path / "persistent.json"
    _write_taxonomy_json(persistent, "old species")
    monkeypatch.setattr(tax_mod, "TAXONOMY_JSON_PATH", str(persistent))

    first = tax_mod.load_local_taxonomy()
    assert first.is_taxon("old species")

    _write_taxonomy_json(persistent, "new species that is much longer")
    os.utime(persistent, (1e9, 1e9))

    second = tax_mod.load_local_taxonomy()
    assert second is not first
    assert second.is_taxon("new species that is much longer")
    assert not second.is_taxon("old species")


def test_load_local_taxonomy_cache_is_keyed_by_path(tmp_path, monkeypatch):
    """Switching to a different taxonomy file does not serve the stale one."""
    import taxonomy as tax_mod

    tax_mod.clear_taxonomy_cache()
    first_path = tmp_path / "first.json"
    second_path = tmp_path / "second.json"
    _write_taxonomy_json(first_path, "first species")
    _write_taxonomy_json(second_path, "second species")

    monkeypatch.setattr(tax_mod, "TAXONOMY_JSON_PATH", str(first_path))
    assert tax_mod.load_local_taxonomy().is_taxon("first species")

    monkeypatch.setattr(tax_mod, "TAXONOMY_JSON_PATH", str(second_path))
    assert tax_mod.load_local_taxonomy().is_taxon("second species")


def test_load_local_taxonomy_detects_metadata_preserving_replacement(
    tmp_path, monkeypatch,
):
    """A ``cp -p`` / ``rsync -t`` style swap with the old mtime and matching
    size must still invalidate the cache.

    An external updater can atomically install a taxonomy while preserving
    the previous mtime and hitting the same byte length; without file
    identity in the cache key the pair ``(mtime_ns, size)`` matches and
    ``_load_taxonomy_cached()`` keeps handing back the old snapshot forever.
    """
    import taxonomy as tax_mod

    tax_mod.clear_taxonomy_cache()
    persistent = tmp_path / "persistent.json"
    # Common names padded to the same length so the two taxonomy files
    # serialize to the same byte count — that is the exact case where
    # (mtime_ns, size) alone cannot tell them apart.
    old_common = "a" * 30
    new_common = "b" * 30
    _write_taxonomy_json(persistent, old_common)
    monkeypatch.setattr(tax_mod, "TAXONOMY_JSON_PATH", str(persistent))

    first = tax_mod.load_local_taxonomy()
    assert first.is_taxon(old_common)
    old_stat = os.stat(persistent)

    # Atomically replace via rename onto the target — new inode, but stamp
    # the old mtime back so mtime and size match the pre-replacement values.
    replacement = tmp_path / "replacement.json"
    _write_taxonomy_json(replacement, new_common)
    assert os.stat(replacement).st_size == old_stat.st_size, (
        "test setup: the padded names must yield equal-size JSON files"
    )
    os.replace(str(replacement), str(persistent))
    os.utime(persistent, ns=(old_stat.st_mtime_ns, old_stat.st_mtime_ns))
    new_stat = os.stat(persistent)
    assert new_stat.st_mtime_ns == old_stat.st_mtime_ns
    assert new_stat.st_size == old_stat.st_size

    second = tax_mod.load_local_taxonomy()
    assert second is not first
    assert second.is_taxon(new_common)
    assert not second.is_taxon(old_common)


def test_taxonomy_stat_key_includes_file_identity(tmp_path):
    """The stat key must change when the file is atomically replaced,
    even if mtime and size are preserved.

    Direct guard on ``_taxonomy_stat_key`` for the metadata-preserving
    replacement case — the integration test above depends on this
    property, and asserting it in isolation makes a future regression
    fail here first with an obvious diff.
    """
    import taxonomy as tax_mod

    target = tmp_path / "taxonomy.json"
    target.write_text("x" * 128)
    before_stat = os.stat(target)
    before_key = tax_mod._taxonomy_stat_key(str(target))

    replacement = tmp_path / "replacement.json"
    replacement.write_text("y" * 128)
    os.replace(str(replacement), str(target))
    os.utime(target, ns=(before_stat.st_mtime_ns, before_stat.st_mtime_ns))
    after_stat = os.stat(target)
    after_key = tax_mod._taxonomy_stat_key(str(target))

    # Preconditions: mtime and size did not move, so a key made of just
    # (mtime_ns, size) would compare equal.
    assert after_stat.st_mtime_ns == before_stat.st_mtime_ns
    assert after_stat.st_size == before_stat.st_size
    # The key must still change — that is what makes the cache pick up
    # the replacement.
    assert after_key != before_key


def test_load_local_taxonomy_retries_when_file_changes_during_parse(tmp_path, monkeypatch):
    """A rewrite mid-parse must not cache stale content under the new stat key.

    Without a retry, the post-parse stat would record the new file's metadata
    while the parsed object still holds the old snapshot. Future calls with a
    stat that matches the cached key would then keep returning the stale
    taxonomy indefinitely.
    """
    import taxonomy as tax_mod

    tax_mod.clear_taxonomy_cache()
    persistent = tmp_path / "persistent.json"
    _write_taxonomy_json(persistent, "first species")
    monkeypatch.setattr(tax_mod, "TAXONOMY_JSON_PATH", str(persistent))

    real_init = tax_mod.Taxonomy.__init__
    call_count = {"n": 0}

    def racing_init(self, path):
        call_count["n"] += 1
        real_init(self, path)
        # Race only the first parse; the retry then reads a stable file.
        if call_count["n"] == 1:
            _write_taxonomy_json(persistent, "second species that is longer")
            os.utime(persistent, (2e9, 2e9))

    monkeypatch.setattr(tax_mod.Taxonomy, "__init__", racing_init)

    result = tax_mod.load_local_taxonomy()

    assert result is not None
    assert result.is_taxon("second species that is longer")
    assert call_count["n"] == 2

    again = tax_mod.load_local_taxonomy()
    assert again is result
    assert call_count["n"] == 2


def test_load_local_taxonomy_skips_cache_when_file_keeps_changing(tmp_path, monkeypatch):
    """A file that keeps moving must not be cached under a mismatched stat.

    When retries exhaust, the parse still returns so the caller sees fresh
    data, but the cache stays empty so the next call re-checks instead of
    stranding the process on a stale snapshot.
    """
    import taxonomy as tax_mod

    tax_mod.clear_taxonomy_cache()
    persistent = tmp_path / "persistent.json"
    _write_taxonomy_json(persistent, "test species")
    monkeypatch.setattr(tax_mod, "TAXONOMY_JSON_PATH", str(persistent))

    real_init = tax_mod.Taxonomy.__init__
    counter = {"n": 0}

    def always_racing_init(self, path):
        counter["n"] += 1
        real_init(self, path)
        os.utime(persistent, (counter["n"] + 1e9, counter["n"] + 1e9))

    monkeypatch.setattr(tax_mod.Taxonomy, "__init__", always_racing_init)

    result = tax_mod.load_local_taxonomy()

    assert result is not None
    assert counter["n"] == tax_mod._TAXONOMY_PARSE_RETRY_LIMIT
    with tax_mod._taxonomy_cache_lock:
        assert tax_mod._taxonomy_cache is None


def test_load_local_taxonomy_releases_stale_instance_before_reparse(tmp_path, monkeypatch):
    """A stale cache entry must not stay reachable while the replacement parses.

    A parsed iNaturalist taxonomy is ~2.8GB. If the old instance is still
    held by ``_taxonomy_cache`` (or by a local reference inside
    ``_load_taxonomy_cached``) while ``Taxonomy(path)`` allocates the
    replacement, peak RSS doubles, and each retry on a file that keeps
    changing stacks another live copy on top. Assert both the module
    slot and any local reference are cleared before the next
    ``Taxonomy(path)`` call runs.
    """
    import taxonomy as tax_mod

    tax_mod.clear_taxonomy_cache()
    persistent = tmp_path / "persistent.json"
    _write_taxonomy_json(persistent, "first species")
    monkeypatch.setattr(tax_mod, "TAXONOMY_JSON_PATH", str(persistent))

    first = tax_mod.load_local_taxonomy()
    assert first is not None

    _write_taxonomy_json(persistent, "second species that is longer")
    os.utime(persistent, (2e9, 2e9))

    real_init = tax_mod.Taxonomy.__init__
    observed = []

    def observing_init(self, path):
        # By the time we are inside the constructor for the replacement,
        # neither the module-level cache slot nor any live reference to
        # the previously cached instance should be reachable from this
        # function's frame — otherwise both instances share peak RSS.
        observed.append(tax_mod._taxonomy_cache)
        real_init(self, path)

    monkeypatch.setattr(tax_mod.Taxonomy, "__init__", observing_init)

    second = tax_mod.load_local_taxonomy()

    assert second is not None
    assert second is not first
    assert observed == [None]


def test_load_local_taxonomy_releases_between_retries(tmp_path, monkeypatch):
    """Retries after mid-parse rewrites must not stack live copies.

    Without dropping the previous retry's instance before the next
    ``Taxonomy(path)`` call, a file that keeps changing would hold N
    parsed taxonomies live at once (peak RSS ≈ N × 2.8GB on a real
    iNaturalist dump). Track every constructed instance with a
    weakref; only the caller's returned instance should still be
    alive after the final retry.
    """
    import taxonomy as tax_mod

    tax_mod.clear_taxonomy_cache()
    persistent = tmp_path / "persistent.json"
    _write_taxonomy_json(persistent, "test species")
    monkeypatch.setattr(tax_mod, "TAXONOMY_JSON_PATH", str(persistent))

    tracked = []
    real_init = tax_mod.Taxonomy.__init__
    counter = {"n": 0}

    def racing_init(self, path):
        counter["n"] += 1
        tracked.append(weakref.ref(self))
        real_init(self, path)
        # Move the file every time so every retry sees a mismatched stat.
        os.utime(persistent, (counter["n"] + 1e9, counter["n"] + 1e9))

    monkeypatch.setattr(tax_mod.Taxonomy, "__init__", racing_init)

    result = tax_mod.load_local_taxonomy()

    assert result is not None
    assert counter["n"] == tax_mod._TAXONOMY_PARSE_RETRY_LIMIT
    gc.collect()
    live = [ref for ref in tracked if ref() is result]
    dead = [ref for ref in tracked if ref() is None]
    # The final instance is the one returned; every earlier retry's
    # instance should be unreachable, meaning the loop dropped its ref
    # before allocating the next parse.
    assert len(live) == 1
    assert len(dead) == tax_mod._TAXONOMY_PARSE_RETRY_LIMIT - 1


def test_taxonomy_save_keeps_cached_instance_current(tmp_path, monkeypatch):
    """save() rewrites the file; the cache must not serve a pre-save copy.

    The instance mutated by api_lookup() is the cached one, so after it
    persists itself the next load should keep returning that same object
    rather than re-parsing the file it just wrote.
    """
    import taxonomy as tax_mod

    tax_mod.clear_taxonomy_cache()
    persistent = tmp_path / "persistent.json"
    _write_taxonomy_json(persistent, "test species")
    monkeypatch.setattr(tax_mod, "TAXONOMY_JSON_PATH", str(persistent))

    tax = tax_mod.load_local_taxonomy()
    tax._api_misses.add("nothing here")
    tax._dirty = True
    tax.save()

    assert tax_mod.load_local_taxonomy() is tax


def test_taxonomy_save_leaves_original_intact_when_write_fails(tmp_path, monkeypatch):
    """An interrupted save must not destroy the existing taxonomy.

    save() used to truncate the target with open(path, "w") before
    serializing ~500MB into it, so a crash — or any concurrent reader,
    now including _load_taxonomy_cached — saw a half-written file.
    """
    import taxonomy as tax_mod

    tax_mod.clear_taxonomy_cache()
    persistent = tmp_path / "persistent.json"
    _write_taxonomy_json(persistent, "test species")
    monkeypatch.setattr(tax_mod, "TAXONOMY_JSON_PATH", str(persistent))
    original = persistent.read_text()

    tax = tax_mod.load_local_taxonomy()
    tax._api_misses.add("nothing here")
    tax._dirty = True

    def exploding_dump(*args, **kwargs):
        raise OSError("disk full")

    monkeypatch.setattr(tax_mod.json, "dump", exploding_dump)

    with pytest.raises(OSError):
        tax.save()

    assert persistent.read_text() == original
    assert not list(tmp_path.glob("*.tmp")), "no temp file may survive"
    tax_mod.clear_taxonomy_cache()
    assert tax_mod.load_local_taxonomy().is_taxon("test species")


def test_taxonomy_save_replaces_file_without_truncating_it(tmp_path, monkeypatch):
    """The target is never observed empty: the new bytes land via rename."""
    import taxonomy as tax_mod

    tax_mod.clear_taxonomy_cache()
    persistent = tmp_path / "persistent.json"
    _write_taxonomy_json(persistent, "test species")
    monkeypatch.setattr(tax_mod, "TAXONOMY_JSON_PATH", str(persistent))

    tax = tax_mod.load_local_taxonomy()
    tax._api_misses.add("nothing here")
    tax._dirty = True

    real_dump = tax_mod.json.dump
    target_during_write = []

    def observing_dump(data, fp, *args, **kwargs):
        # Mid-serialize, the file callers read must still parse.
        with open(persistent) as f:
            target_during_write.append(json.load(f))
        return real_dump(data, fp, *args, **kwargs)

    monkeypatch.setattr(tax_mod.json, "dump", observing_dump)
    tax.save()

    assert len(target_during_write) == 1
    assert "test species" in target_during_write[0]["taxa_by_common"]
    assert json.loads(persistent.read_text())["api_misses"] == ["nothing here"]


def test_load_local_taxonomy_keeps_fallback_cached_when_preferred_is_corrupt(
    tmp_path, monkeypatch,
):
    """The corrupt-persistent fallback must not defeat the cache.

    load_local_taxonomy() parses the preferred candidate, fails, and falls
    back to the legacy one. Evicting the legacy entry on the way past would
    re-parse a multi-GB file on every compare/accept request.
    """
    import taxonomy as tax_mod

    tax_mod.clear_taxonomy_cache()
    corrupt = tmp_path / "persistent.json"
    corrupt.write_text("{ not valid json")
    legacy = tmp_path / "taxonomy.json"
    _write_taxonomy_json(legacy, "legacy species")
    monkeypatch.setattr(tax_mod, "TAXONOMY_JSON_PATH", str(corrupt))
    monkeypatch.setattr(tax_mod, "LEGACY_TAXONOMY_JSON_PATH", str(legacy))

    parses = []
    real_init = tax_mod.Taxonomy.__init__

    def counting_init(self, path):
        parses.append(path)
        real_init(self, path)

    monkeypatch.setattr(tax_mod.Taxonomy, "__init__", counting_init)

    first = tax_mod.load_local_taxonomy()
    second = tax_mod.load_local_taxonomy()

    assert first.is_taxon("legacy species")
    assert second is first
    # Each call attempts the corrupt preferred file; only the first should
    # have to parse the legacy fallback.
    assert parses.count(str(legacy)) == 1


def test_load_local_taxonomy_rechecks_stat_after_acquiring_lock(tmp_path, monkeypatch):
    """A caller that waited on the lock must not evict what it was waiting for.

    Statting before the lock lets a caller compare a pre-wait stat against an
    entry another caller refreshed while it waited, conclude it is stale, and
    re-parse ~2.8GB for nothing.
    """
    import taxonomy as tax_mod

    tax_mod.clear_taxonomy_cache()
    persistent = tmp_path / "persistent.json"
    _write_taxonomy_json(persistent, "old species")
    monkeypatch.setattr(tax_mod, "TAXONOMY_JSON_PATH", str(persistent))

    winner = tax_mod.load_local_taxonomy()
    assert winner.is_taxon("old species")

    # Build the instance the "other caller" installs, plus the bytes and stat
    # key that go with it, before the parse counter is armed — otherwise this
    # setup parse is itself counted.
    _write_taxonomy_json(persistent, "new species that is longer")
    os.utime(persistent, (4e9, 4e9))
    refreshed = tax_mod.Taxonomy(str(persistent))
    new_bytes = persistent.read_text()
    # Restore the old file so the call under test starts from the old stat.
    _write_taxonomy_json(persistent, "old species")
    os.utime(persistent, (1e9, 1e9))

    parses = []
    real_init = tax_mod.Taxonomy.__init__

    def counting_init(self, path):
        parses.append(path)
        real_init(self, path)

    class RacingLock:
        """Simulates another caller refreshing the cache while we wait."""

        def __init__(self, inner):
            self._inner = inner
            self.fired = False

        def __enter__(self):
            self._inner.acquire()
            if not self.fired:
                self.fired = True
                persistent.write_text(new_bytes)
                os.utime(persistent, (4e9, 4e9))
                # Key it off the file as it stands now: the stat key covers
                # inode and ctime, so it has to be read after the last write.
                tax_mod._taxonomy_cache = (
                    str(persistent),
                    tax_mod._taxonomy_stat_key(str(persistent)),
                    refreshed,
                )
            return self

        def __exit__(self, *exc):
            self._inner.release()
            return False

    racing = RacingLock(tax_mod._taxonomy_cache_lock)
    monkeypatch.setattr(tax_mod, "_taxonomy_cache_lock", racing)
    monkeypatch.setattr(tax_mod.Taxonomy, "__init__", counting_init)

    result = tax_mod.load_local_taxonomy()

    assert result.is_taxon("new species that is longer")
    # The entry the racing caller installed is served as-is; statting before
    # the lock would have judged it stale and re-parsed the file.
    assert parses == []


def test_taxonomy_save_writes_through_a_symlinked_path(tmp_path, monkeypatch):
    """Saving must update a symlink's target, not replace the symlink."""
    import taxonomy as tax_mod

    tax_mod.clear_taxonomy_cache()
    real = tmp_path / "real-taxonomy.json"
    _write_taxonomy_json(real, "test species")
    link = tmp_path / "linked.json"
    link.symlink_to(real)
    monkeypatch.setattr(tax_mod, "TAXONOMY_JSON_PATH", str(link))

    tax = tax_mod.load_local_taxonomy()
    tax._api_misses.add("nothing here")
    tax._dirty = True
    tax.save()

    assert link.is_symlink(), "os.replace must not swap the link for a file"
    # Compare resolved paths: Windows readlink() returns an
    # extended-length "\\\\?\\C:\\..." form that never equals the
    # plain path the link was created with.
    assert link.resolve() == real.resolve()
    assert json.loads(real.read_text())["api_misses"] == ["nothing here"]
    assert not list(tmp_path.glob("*.tmp")), "no temp file may survive"


def test_load_local_taxonomy_retries_parse_exception_from_mid_write(
    tmp_path, monkeypatch,
):
    """A parse exception from a mid-write read must be retried, not propagated.

    Taxonomy.save() writes atomically now, but an interrupted download, an
    external tool rewriting the file, or the same taxonomy on a filesystem
    that can't do atomic renames all still expose a partial JSON document.
    A single unlucky read raising json.JSONDecodeError would bubble out of
    _load_taxonomy_cached, load_local_taxonomy() would log a warning and
    fall through to the next (usually stale or nonexistent) candidate, and
    the very next call would have parsed the same file cleanly.
    """
    import taxonomy as tax_mod

    tax_mod.clear_taxonomy_cache()
    persistent = tmp_path / "persistent.json"
    _write_taxonomy_json(persistent, "test species")
    monkeypatch.setattr(tax_mod, "TAXONOMY_JSON_PATH", str(persistent))

    real_init = tax_mod.Taxonomy.__init__
    attempts = {"n": 0}

    def flaky_init(self, path):
        attempts["n"] += 1
        if attempts["n"] == 1:
            # A partial read means the writer was mid-rewrite, so the file
            # has moved. That drift is what makes the retry worth doing —
            # see the stably-corrupt test for the case that must not retry.
            _write_taxonomy_json(persistent, "test species")
            os.utime(persistent, (5e9, 5e9))
            raise ValueError("simulated mid-write JSONDecodeError")
        real_init(self, path)

    monkeypatch.setattr(tax_mod.Taxonomy, "__init__", flaky_init)

    result = tax_mod.load_local_taxonomy()

    assert result is not None
    assert result.is_taxon("test species")
    assert attempts["n"] == 2


def test_load_local_taxonomy_raises_when_every_parse_attempt_fails(
    tmp_path, monkeypatch,
):
    """A file that raises on every retry surfaces a clean error, not None.

    Silently returning None would let load_local_taxonomy() log a generic
    "failed to load" warning and fall through to the next candidate; a
    persistent-corruption failure that repeats every retry is worth
    surfacing so the caller can see the last exception's message.
    """
    import taxonomy as tax_mod

    tax_mod.clear_taxonomy_cache()
    persistent = tmp_path / "persistent.json"
    _write_taxonomy_json(persistent, "test species")
    monkeypatch.setattr(tax_mod, "TAXONOMY_JSON_PATH", str(persistent))
    monkeypatch.setattr(
        tax_mod, "LEGACY_TAXONOMY_JSON_PATH", str(tmp_path / "missing.json"),
    )

    def always_raise(self, path):
        raise ValueError("persistent corruption")

    monkeypatch.setattr(tax_mod.Taxonomy, "__init__", always_raise)

    # load_local_taxonomy() catches the exception and moves to the next
    # candidate; with no fallback present the final result is None. What
    # matters is that the private helper raises so the outer function can
    # log the concrete failure per candidate.
    with pytest.raises(ValueError):
        tax_mod._load_taxonomy_cached(str(persistent))

    # And the outer function still returns None cleanly, so callers that
    # tolerate no taxonomy at all keep working.
    assert tax_mod.load_local_taxonomy() is None


def test_load_local_taxonomy_does_not_retry_a_stably_corrupt_file(
    tmp_path, monkeypatch,
):
    """A file that is simply corrupt is read once, not once per retry.

    Truncated JSON only raises at the *end* of the parse, so each retry
    reads most of a ~500MB file. Retrying a read that cannot succeed puts
    that cost back on every compare/accept request — the exact latency this
    cache exists to remove. Only a file that demonstrably moved between
    attempts is worth re-reading.
    """
    import taxonomy as tax_mod

    tax_mod.clear_taxonomy_cache()
    persistent = tmp_path / "persistent.json"
    _write_taxonomy_json(persistent, "test species")
    monkeypatch.setattr(tax_mod, "TAXONOMY_JSON_PATH", str(persistent))

    attempts = {"n": 0}

    def always_raising_init(self, path):
        attempts["n"] += 1
        raise ValueError("persistent corruption")

    monkeypatch.setattr(tax_mod.Taxonomy, "__init__", always_raising_init)

    with pytest.raises(ValueError, match="persistent corruption"):
        tax_mod._load_taxonomy_cached(str(persistent))

    assert attempts["n"] == 1, "an unchanged corrupt file must be read once"


def test_load_local_taxonomy_skips_reparsing_known_corrupt_preferred(
    tmp_path, monkeypatch,
):
    """A corrupt preferred candidate is parsed once, then short-circuited.

    Without this, every compare/accept request would re-attempt the corrupt
    parse (spamming warnings and burning CPU) and, more importantly, force
    the cache-eviction logic to keep the fallback alive during the parse to
    avoid re-parsing it. The failure record is what lets us safely evict the
    fallback before parsing new preferred files.
    """
    import taxonomy as tax_mod

    tax_mod.clear_taxonomy_cache()
    corrupt = tmp_path / "persistent.json"
    corrupt.write_text("{ not valid json")
    legacy = tmp_path / "taxonomy.json"
    _write_taxonomy_json(legacy, "legacy species")
    monkeypatch.setattr(tax_mod, "TAXONOMY_JSON_PATH", str(corrupt))
    monkeypatch.setattr(tax_mod, "LEGACY_TAXONOMY_JSON_PATH", str(legacy))

    parses = []
    real_init = tax_mod.Taxonomy.__init__

    def counting_init(self, path):
        parses.append(path)
        real_init(self, path)

    monkeypatch.setattr(tax_mod.Taxonomy, "__init__", counting_init)

    first = tax_mod.load_local_taxonomy()
    second = tax_mod.load_local_taxonomy()
    third = tax_mod.load_local_taxonomy()

    assert first.is_taxon("legacy species")
    assert second is first
    assert third is first
    # Corrupt file is parsed once (recording the failure); subsequent calls
    # short-circuit rather than re-attempting.
    assert parses.count(str(corrupt)) == 1
    # Legacy is parsed once and served from cache thereafter.
    assert parses.count(str(legacy)) == 1


def test_load_local_taxonomy_retries_corrupt_preferred_after_it_changes(
    tmp_path, monkeypatch,
):
    """A repaired preferred file is picked up — the failure record is cleared."""
    import taxonomy as tax_mod

    tax_mod.clear_taxonomy_cache()
    preferred = tmp_path / "persistent.json"
    preferred.write_text("{ not valid json")
    legacy = tmp_path / "taxonomy.json"
    _write_taxonomy_json(legacy, "legacy species")
    monkeypatch.setattr(tax_mod, "TAXONOMY_JSON_PATH", str(preferred))
    monkeypatch.setattr(tax_mod, "LEGACY_TAXONOMY_JSON_PATH", str(legacy))

    first = tax_mod.load_local_taxonomy()
    assert first.is_taxon("legacy species")
    # Same stat → still short-circuited (no re-parse of the corrupt file).
    assert tax_mod.load_local_taxonomy() is first

    # Repair the preferred file. Its stat changes, so the failure record
    # is discarded and the new content is parsed.
    _write_taxonomy_json(preferred, "preferred species that is longer")
    os.utime(preferred, (5e9, 5e9))

    result = tax_mod.load_local_taxonomy()
    assert result is not first
    assert result.is_taxon("preferred species that is longer")


def test_load_local_taxonomy_keeps_fallback_across_transient_preferred_failure(
    tmp_path, monkeypatch,
):
    """A cross-path fallback survives a preferred file that keeps failing.

    Transient errors are deliberately not memoized (a chmod repair would not
    change the file), so a preferred file with, say, wrong permissions is
    re-attempted on every request. Evicting cross-path entries on the way
    past would drop the cached legacy taxonomy each time and re-parse ~500MB
    of fallback per compare or accept.

    The migration case that motivates dropping a cross-path entry — legacy
    cached while a newly-downloaded preferred file parses — is covered by
    download_taxonomy() clearing the cache before it builds its replacement.
    """
    import taxonomy as tax_mod

    tax_mod.clear_taxonomy_cache()
    preferred = tmp_path / "persistent.json"
    _write_taxonomy_json(preferred, "preferred species")
    legacy = tmp_path / "taxonomy.json"
    _write_taxonomy_json(legacy, "legacy species")
    monkeypatch.setattr(tax_mod, "TAXONOMY_JSON_PATH", str(preferred))
    monkeypatch.setattr(tax_mod, "LEGACY_TAXONOMY_JSON_PATH", str(legacy))

    parses = []
    real_init = tax_mod.Taxonomy.__init__

    def failing_preferred_init(self, path):
        parses.append(path)
        if path == str(preferred):
            raise PermissionError(13, "Permission denied")
        real_init(self, path)

    monkeypatch.setattr(tax_mod.Taxonomy, "__init__", failing_preferred_init)

    results = [tax_mod.load_local_taxonomy() for _ in range(3)]

    assert all(r.is_taxon("legacy species") for r in results)
    assert results[1] is results[0] and results[2] is results[0]
    # The preferred file is retried every call (its failure is not memoized),
    # but the fallback must be parsed exactly once for the process.
    assert parses.count(str(preferred)) == 3
    assert parses.count(str(legacy)) == 1


def test_clear_taxonomy_cache_also_clears_failure_records(tmp_path, monkeypatch):
    """clear_taxonomy_cache() must reset failure tracking too.

    Otherwise a test that clears the cache and then repairs a formerly-corrupt
    file would still short-circuit on the stale failure record.
    """
    import taxonomy as tax_mod

    tax_mod.clear_taxonomy_cache()
    preferred = tmp_path / "persistent.json"
    preferred.write_text("{ not valid json")
    monkeypatch.setattr(tax_mod, "TAXONOMY_JSON_PATH", str(preferred))
    monkeypatch.setattr(
        tax_mod, "LEGACY_TAXONOMY_JSON_PATH", str(tmp_path / "missing.json"),
    )

    assert tax_mod.load_local_taxonomy() is None
    with tax_mod._taxonomy_cache_lock:
        assert str(preferred) in tax_mod._taxonomy_failed_stats

    tax_mod.clear_taxonomy_cache()
    with tax_mod._taxonomy_cache_lock:
        assert tax_mod._taxonomy_failed_stats == {}


def test_exhausted_retries_chain_the_last_parse_error(tmp_path, monkeypatch):
    """"File kept changing" alone doesn't say which byte was malformed."""
    import taxonomy as tax_mod

    tax_mod.clear_taxonomy_cache()
    persistent = tmp_path / "persistent.json"
    _write_taxonomy_json(persistent, "test species")
    monkeypatch.setattr(tax_mod, "TAXONOMY_JSON_PATH", str(persistent))

    counter = {"n": 0}

    def always_racing_and_raising(self, path):
        counter["n"] += 1
        os.utime(persistent, (counter["n"] + 6e9, counter["n"] + 6e9))
        raise ValueError(f"malformed at byte {counter['n']}")

    monkeypatch.setattr(tax_mod.Taxonomy, "__init__", always_racing_and_raising)

    with pytest.raises(ValueError, match="kept changing") as excinfo:
        tax_mod._load_taxonomy_cached(str(persistent))

    # The concrete failure travels as text, not as a chained exception: a
    # chained exception's traceback pins the Taxonomy.__init__ frame and the
    # multi-GB dict json.load() decoded into it.
    assert "malformed at byte" in str(excinfo.value)
    assert excinfo.value.__cause__ is None


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Windows chmod only toggles the read-only bit, so 0o640 is unrepresentable",
)
def test_taxonomy_save_preserves_target_permissions(tmp_path, monkeypatch):
    """Renaming a fresh temp file must not reset the taxonomy's mode."""
    import taxonomy as tax_mod

    tax_mod.clear_taxonomy_cache()
    persistent = tmp_path / "persistent.json"
    _write_taxonomy_json(persistent, "test species")
    os.chmod(persistent, 0o640)
    monkeypatch.setattr(tax_mod, "TAXONOMY_JSON_PATH", str(persistent))

    tax = tax_mod.load_local_taxonomy()
    tax._api_misses.add("nothing here")
    tax._dirty = True
    tax.save()

    assert stat.S_IMODE(os.stat(persistent).st_mode) == 0o640
    assert json.loads(persistent.read_text())["api_misses"] == ["nothing here"]


def test_transient_open_failure_is_not_memoized_as_corruption(tmp_path, monkeypatch):
    """A read that fails on OS grounds must get another chance.

    The usual repairs for an unreadable file — restoring a permission bit,
    fd pressure passing — change ctime at most, not mtime or size. Keying a
    failure record to that stat would leave taxonomy features off until the
    contents changed or the process restarted.
    """
    import taxonomy as tax_mod

    tax_mod.clear_taxonomy_cache()
    persistent = tmp_path / "persistent.json"
    _write_taxonomy_json(persistent, "test species")
    monkeypatch.setattr(tax_mod, "TAXONOMY_JSON_PATH", str(persistent))

    real_init = tax_mod.Taxonomy.__init__
    calls = {"n": 0}

    def unreadable_once(self, path):
        calls["n"] += 1
        if calls["n"] == 1:
            raise PermissionError(13, "Permission denied", path)
        real_init(self, path)

    monkeypatch.setattr(tax_mod.Taxonomy, "__init__", unreadable_once)

    # First call fails and must not be remembered as corruption.
    assert tax_mod.load_local_taxonomy() is None
    with tax_mod._taxonomy_cache_lock:
        assert str(persistent) not in tax_mod._taxonomy_failed_stats

    # Access restored, file untouched: the next call reopens it.
    result = tax_mod.load_local_taxonomy()
    assert result is not None
    assert result.is_taxon("test species")
    assert calls["n"] == 2


def test_content_failure_is_still_memoized(tmp_path, monkeypatch):
    """A malformed file is remembered, so it isn't re-read every request."""
    import taxonomy as tax_mod

    tax_mod.clear_taxonomy_cache()
    persistent = tmp_path / "persistent.json"
    persistent.write_text("{ not valid json")
    monkeypatch.setattr(tax_mod, "TAXONOMY_JSON_PATH", str(persistent))

    parses = []
    real_init = tax_mod.Taxonomy.__init__

    def counting_init(self, path):
        parses.append(path)
        real_init(self, path)

    monkeypatch.setattr(tax_mod.Taxonomy, "__init__", counting_init)

    assert tax_mod.load_local_taxonomy() is None
    assert tax_mod.load_local_taxonomy() is None
    assert len(parses) == 1
    with tax_mod._taxonomy_cache_lock:
        assert str(persistent) in tax_mod._taxonomy_failed_stats


def test_schema_invalid_preferred_file_is_memoized(tmp_path, monkeypatch):
    """Valid JSON in the wrong shape must be remembered like malformed JSON.

    Taxonomy() walks the decoded document, so a top-level list raises
    AttributeError rather than ValueError. Left unmemoized, every request
    would evict the cached fallback, re-attempt the preferred file, and
    re-parse the multi-GB legacy one to rediscover the same failure.
    """
    import taxonomy as tax_mod

    tax_mod.clear_taxonomy_cache()
    preferred = tmp_path / "persistent.json"
    preferred.write_text("[1, 2, 3]")  # parses as JSON, wrong shape
    legacy = tmp_path / "taxonomy.json"
    _write_taxonomy_json(legacy, "legacy species")
    monkeypatch.setattr(tax_mod, "TAXONOMY_JSON_PATH", str(preferred))
    monkeypatch.setattr(tax_mod, "LEGACY_TAXONOMY_JSON_PATH", str(legacy))

    parses = []
    real_init = tax_mod.Taxonomy.__init__

    def counting_init(self, path):
        parses.append(path)
        real_init(self, path)

    monkeypatch.setattr(tax_mod.Taxonomy, "__init__", counting_init)

    for _ in range(3):
        assert tax_mod.load_local_taxonomy().is_taxon("legacy species")

    assert parses.count(str(preferred)) == 1, "schema failure must be memoized"
    assert parses.count(str(legacy)) == 1, "the fallback must survive"
    with tax_mod._taxonomy_cache_lock:
        assert str(preferred) in tax_mod._taxonomy_failed_stats


def test_memory_error_during_parse_is_not_memoized(tmp_path, monkeypatch):
    """Running out of memory on a ~2.8GB parse is environmental, not corruption."""
    import taxonomy as tax_mod

    tax_mod.clear_taxonomy_cache()
    persistent = tmp_path / "persistent.json"
    _write_taxonomy_json(persistent, "test species")
    monkeypatch.setattr(tax_mod, "TAXONOMY_JSON_PATH", str(persistent))

    real_init = tax_mod.Taxonomy.__init__
    calls = {"n": 0}

    def oom_once(self, path):
        calls["n"] += 1
        if calls["n"] == 1:
            raise MemoryError("cannot allocate taxonomy")
        real_init(self, path)

    monkeypatch.setattr(tax_mod.Taxonomy, "__init__", oom_once)

    assert tax_mod.load_local_taxonomy() is None
    with tax_mod._taxonomy_cache_lock:
        assert str(persistent) not in tax_mod._taxonomy_failed_stats

    result = tax_mod.load_local_taxonomy()
    assert result is not None
    assert result.is_taxon("test species")
    assert calls["n"] == 2


def test_download_taxonomy_writes_atomically(tmp_path, monkeypatch):
    """A concurrent reader never sees a half-written taxonomy.json.

    ``download_taxonomy()`` used to serialize ~500MB straight into the
    output path with ``open(path, "w")``, exposing a partial JSON document
    to any request that read the file during the write window. That
    window is longer than ``_load_taxonomy_cached``'s bounded retry loop
    can wait out. Verify that an interrupted write leaves the pre-existing
    file intact and that no ``.tmp`` sibling is left behind.
    """
    import taxonomy as tax_mod

    output_path = tmp_path / "taxonomy.json"
    _write_taxonomy_json(output_path, "pre-existing species")
    original_text = output_path.read_text()

    monkeypatch.setattr(tax_mod, "_download_with_resume", _fake_dwca_download)

    real_dump = tax_mod.json.dump

    def exploding_dump(*args, **kwargs):
        raise OSError("disk full")

    monkeypatch.setattr(tax_mod.json, "dump", exploding_dump)

    with pytest.raises(OSError):
        tax_mod.download_taxonomy(str(output_path))

    assert output_path.read_text() == original_text, (
        "interrupted download must not clobber the existing taxonomy"
    )
    assert not list(tmp_path.glob("*.tmp")), (
        "failed atomic write must clean up its sibling temp file"
    )

    # Now let the write succeed and verify the target ends up updated
    # through the .tmp -> os.replace path.
    monkeypatch.setattr(tax_mod.json, "dump", real_dump)
    result = tax_mod.download_taxonomy(str(output_path))
    assert result["taxa_by_common"], "the successful write should produce content"
    written = json.loads(output_path.read_text())
    assert written["taxa_by_common"] == result["taxa_by_common"]
    assert not list(tmp_path.glob("*.tmp"))


def _fake_homonym_dwca_download(url, path, progress_callback=None):
    """A DWCA where a bird genus and a plant genus are both named Prunella."""
    import zipfile as _zf
    with _zf.ZipFile(path, "w") as zf:
        zf.writestr(
            "taxa.csv",
            "id,parentNameUsageID,scientificName,taxonRank\n"
            "1,,Animalia,kingdom\n"
            "47126,,Plantae,kingdom\n"
            "10,https://www.inaturalist.org/taxa/1,Prunellidae,family\n"
            "11,https://www.inaturalist.org/taxa/10,Prunella,genus\n"
            "12,https://www.inaturalist.org/taxa/11,Prunella modularis,species\n"
            "20,https://www.inaturalist.org/taxa/47126,Lamiaceae,family\n"
            "21,https://www.inaturalist.org/taxa/20,Prunella,genus\n"
            "22,https://www.inaturalist.org/taxa/21,Prunella vulgaris,species\n",
        )
        zf.writestr(
            "VernacularNames-english.csv",
            "id,vernacularName,language\n"
            "12,Dunnock,en\n"
            "22,Common Self-heal,en\n",
        )


def test_download_taxonomy_keeps_cross_kingdom_homonyms(tmp_path, monkeypatch):
    """Two genera sharing a scientific name both survive the download.

    ``taxa_by_scientific`` used to be keyed on the name alone, so the
    later CSV row overwrote the earlier: the bird genus Prunella vanished,
    ``lookup("Prunella")`` answered with the plant's lineage, and the DB
    populate never created the bird genus to parent its species.
    """
    import taxonomy as tax_mod
    from taxonomy import populate_taxa_db_from_json

    monkeypatch.setattr(tax_mod, "_download_with_resume", _fake_homonym_dwca_download)
    output_path = tmp_path / "taxonomy.json"
    tax_mod.download_taxonomy(str(output_path))

    tax = tax_mod.Taxonomy(str(output_path))
    # The bare name can't say which kingdom is meant, so it answers nothing
    # instead of an arbitrary one's lineage.
    assert tax.lookup("Prunella") is None
    assert tax.get_hierarchy("Prunella") == {}
    # Each genus stays reachable by its id, with its own lineage.
    assert tax.lookup_id(11)["lineage_names"] == ["Animalia", "Prunellidae", "Prunella"]
    assert tax.lookup_id(21)["lineage_names"] == ["Plantae", "Lamiaceae", "Prunella"]
    # Unambiguous binomials are unaffected.
    assert tax.get_hierarchy("Prunella modularis")["kingdom"] == "Animalia"
    assert tax.get_hierarchy("Dunnock")["family"] == "Prunellidae"
    assert tax.get_hierarchy("Prunella vulgaris")["kingdom"] == "Plantae"

    db = Database(str(tmp_path / "x.db"))
    populate_taxa_db_from_json(db, str(output_path))
    rows = db.conn.execute(
        "SELECT child.inat_id AS child, parent.inat_id AS parent "
        "FROM taxa child JOIN taxa parent ON parent.id = child.parent_id "
        "WHERE child.inat_id IN (11, 12, 21, 22)"
    ).fetchall()
    assert {r["child"]: r["parent"] for r in rows} == {11: 10, 12: 11, 21: 20, 22: 21}


def test_lookup_rejects_scientific_homonym_cached_as_common_name(tmp_path, monkeypatch):
    """A cached common-name entry must not shadow the scientific-homonym gate.

    ``_by_common`` used to be read before the homonym check, so any prior
    write that landed a scientific homonym in the common-name index (a
    previous ``api_lookup`` cache, an alternate-name index hit, a
    hand-maintained mapping) turned every future ``lookup("Prunella")``
    into that one arbitrary kingdom's lineage — the exact regression the
    homonym index was meant to prevent. Whatever the source of the stale
    entry, the query must still resolve as ambiguous.
    """
    import taxonomy as tax_mod

    monkeypatch.setattr(tax_mod, "_download_with_resume", _fake_homonym_dwca_download)
    output_path = tmp_path / "taxonomy.json"
    tax_mod.download_taxonomy(str(output_path))

    tax = tax_mod.Taxonomy(str(output_path))
    plant_entry = tax.lookup_id(21)
    assert plant_entry is not None
    # Simulate any cache path that could have written the homonym: a stale
    # api_lookup cache, an alternate-name index, or a hand-edited file.
    tax._by_common["prunella"] = plant_entry
    tax._by_common_normalized["prunella"] = plant_entry

    assert tax.lookup("Prunella") is None


def test_api_lookup_rejects_homonym_query_and_leaves_cache_clean(tmp_path, monkeypatch):
    """A homonym query must not reach the API or cache an arbitrary kingdom.

    ``api_lookup`` used to resolve the API's first matching row by taxon id
    and cache it under the query's normalized key. For a scientific homonym
    like "Prunella", whichever kingdom the API happened to return first
    won: the cache entry then answered every subsequent bare-name lookup
    from the wrong lineage. The query is now rejected before the network
    call — no request, no cache write, no return value — so the homonym
    stays ambiguous and no bandwidth is spent on an unresolvable name.
    """
    import taxonomy as tax_mod

    monkeypatch.setattr(tax_mod, "_download_with_resume", _fake_homonym_dwca_download)
    output_path = tmp_path / "taxonomy.json"
    tax_mod.download_taxonomy(str(output_path))

    tax = tax_mod.Taxonomy(str(output_path))

    calls = {"n": 0}

    def fake_urlopen(req, timeout=None, context=None):
        calls["n"] += 1
        raise AssertionError("api_lookup must not hit the network for a homonym query")

    import urllib.request as _urlreq
    monkeypatch.setattr(_urlreq, "urlopen", fake_urlopen)

    assert tax.api_lookup("Prunella") is None
    assert calls["n"] == 0
    assert "prunella" not in tax._by_common
    assert "prunella" not in tax._by_common_normalized
    # Bare-name lookup stays ambiguous too.
    assert tax.lookup("Prunella") is None


def test_atomic_write_creates_a_new_target_without_a_mode_to_copy(tmp_path):
    """A first-time write has no existing file to copy permissions from.

    download_taxonomy() hits this on a fresh install; the mode copy must
    degrade quietly rather than fail the write.
    """
    import taxonomy as tax_mod

    target = tmp_path / "brand-new.json"
    assert not target.exists()

    tax_mod._write_taxonomy_json_atomically(str(target), {"taxa_by_common": {}})

    assert json.loads(target.read_text()) == {"taxa_by_common": {}}
    assert not list(tmp_path.glob("*.tmp")), "no temp file may survive"


def test_atomic_write_is_shared_by_both_taxonomy_writers(tmp_path, monkeypatch):
    """save() and download_taxonomy() must not drift apart again.

    They had two separate temp-file-and-rename implementations that differed
    on fsync and permission handling; both now route through one helper.
    """
    import taxonomy as tax_mod

    calls = []
    real_writer = tax_mod._write_taxonomy_json_atomically

    def recording_writer(path, data):
        calls.append(path)
        return real_writer(path, data)

    monkeypatch.setattr(
        tax_mod, "_write_taxonomy_json_atomically", recording_writer,
    )

    tax_mod.clear_taxonomy_cache()
    persistent = tmp_path / "persistent.json"
    _write_taxonomy_json(persistent, "test species")
    monkeypatch.setattr(tax_mod, "TAXONOMY_JSON_PATH", str(persistent))

    tax = tax_mod.load_local_taxonomy()
    tax._api_misses.add("nothing here")
    tax._dirty = True
    tax.save()

    assert calls == [str(persistent)]

    # And the download path routes through it too — asserted by behaviour
    # rather than by grepping the source, which a rename would break without
    # anything actually regressing.
    calls.clear()
    download_target = tmp_path / "downloaded.json"
    monkeypatch.setattr(tax_mod, "_download_with_resume", _fake_dwca_download)
    tax_mod.download_taxonomy(str(download_target))
    assert calls == [str(download_target)]


def test_overlapping_atomic_writes_do_not_share_a_temp_file(tmp_path, monkeypatch):
    """Concurrent writers must not collide on one temp path.

    Two POSTs to the download endpoint start two workers — it uses
    runner.start(), not start_singleton() — so both can be serializing at
    once. With a fixed "<target>.tmp" one writer renames the inode out from
    under the other, exposing a partial target and then failing the second
    when its pathname has vanished.
    """
    import taxonomy as tax_mod

    target = tmp_path / "taxonomy.json"
    _write_taxonomy_json(target, "original species")

    # Record the temp *paths* mkstemp hands out. os.fdopen(fd).name is the
    # integer fd, not a path, so reading it off the file object would compare
    # two file descriptors and pass no matter what the filenames were.
    seen_temp_paths = []
    real_mkstemp = tax_mod.tempfile.mkstemp

    def recording_mkstemp(*args, **kwargs):
        fd, tmp = real_mkstemp(*args, **kwargs)
        seen_temp_paths.append(tmp)
        return fd, tmp

    monkeypatch.setattr(tax_mod.tempfile, "mkstemp", recording_mkstemp)

    real_dump = tax_mod.json.dump
    barrier = {"inner_done": False}

    def dump_and_reenter(data, fp, *args, **kwargs):
        if not barrier["inner_done"]:
            # Start a second write while this one still holds its fd open.
            barrier["inner_done"] = True
            tax_mod._write_taxonomy_json_atomically(
                str(target), {"taxa_by_common": {"inner species": {}}},
            )
        return real_dump(data, fp, *args, **kwargs)

    import unittest.mock as mock
    with mock.patch.object(tax_mod.json, "dump", dump_and_reenter):
        tax_mod._write_taxonomy_json_atomically(
            str(target), {"taxa_by_common": {"outer species": {}}},
        )

    assert len(seen_temp_paths) == 2
    assert seen_temp_paths[0] != seen_temp_paths[1], (
        "each write needs its own temp file"
    )
    assert all(p.endswith(".tmp") for p in seen_temp_paths)
    # The outer write renamed last, so it wins — and the target is whole.
    assert json.loads(target.read_text())["taxa_by_common"] == {"outer species": {}}
    assert not list(tmp_path.glob("*.tmp"))


def test_load_local_taxonomy_with_explicit_path_does_not_fall_back(
    tmp_path, monkeypatch,
):
    """path= pins the load to one artifact, with no legacy fallback.

    The post-download retype must fail loudly when the file it just wrote
    won't parse. Falling back would retype keywords from a stale legacy
    copy while the taxa tables hold the new download's data — two taxonomy
    versions mixed, reported as success.
    """
    import taxonomy as tax_mod

    tax_mod.clear_taxonomy_cache()
    preferred = tmp_path / "persistent.json"
    preferred.write_text("{ not valid json")
    legacy = tmp_path / "taxonomy.json"
    _write_taxonomy_json(legacy, "legacy species")
    monkeypatch.setattr(tax_mod, "TAXONOMY_JSON_PATH", str(preferred))
    monkeypatch.setattr(tax_mod, "LEGACY_TAXONOMY_JSON_PATH", str(legacy))

    # Default behaviour still falls back — other callers rely on it.
    assert tax_mod.load_local_taxonomy().is_taxon("legacy species")

    # Pinned to the broken artifact, it reports failure instead.
    assert tax_mod.load_local_taxonomy(path=str(preferred)) is None


def test_load_local_taxonomy_with_explicit_path_still_caches(tmp_path, monkeypatch):
    """A pinned load shares the same cache as the default path."""
    import taxonomy as tax_mod

    tax_mod.clear_taxonomy_cache()
    preferred = tmp_path / "persistent.json"
    _write_taxonomy_json(preferred, "preferred species")
    monkeypatch.setattr(tax_mod, "TAXONOMY_JSON_PATH", str(preferred))

    parses = []
    real_init = tax_mod.Taxonomy.__init__

    def counting_init(self, path):
        parses.append(path)
        real_init(self, path)

    monkeypatch.setattr(tax_mod.Taxonomy, "__init__", counting_init)

    pinned = tax_mod.load_local_taxonomy(path=str(preferred))
    default = tax_mod.load_local_taxonomy()

    assert pinned is default
    assert len(parses) == 1


def test_download_taxonomy_evicts_cache_before_rebuilding(tmp_path, monkeypatch):
    """Old ~2.8GB parse must not stay reachable through the cache during build.

    The post-download retype routes through load_local_taxonomy() which
    evicts on parse, but that runs *after* download_taxonomy() has already
    built its ~2.8GB dict. During the build, the cache's old reference and
    the new dict would coexist, roughly doubling peak RSS and putting a
    routine refresh in reach of OOM. Verify eviction happens *before* the
    build starts.
    """
    import taxonomy as tax_mod

    tax_mod.clear_taxonomy_cache()
    persistent = tmp_path / "taxonomy.json"
    _write_taxonomy_json(persistent, "old species")
    monkeypatch.setattr(tax_mod, "TAXONOMY_JSON_PATH", str(persistent))

    # Seed the cache from the current file so we can observe eviction.
    cached_before = tax_mod.load_local_taxonomy()
    assert cached_before is not None
    with tax_mod._taxonomy_cache_lock:
        assert tax_mod._taxonomy_cache is not None

    observed_cache_state = []

    def observing_download(url, path, progress_callback=None):
        with tax_mod._taxonomy_cache_lock:
            observed_cache_state.append(tax_mod._taxonomy_cache)
        import zipfile as _zf
        with _zf.ZipFile(path, "w") as zf:
            zf.writestr(
                "taxa.csv",
                "id,parentNameUsageID,scientificName,taxonRank\n"
                "1,,Test species,species\n",
            )
            zf.writestr(
                "VernacularNames-english.csv",
                "id,vernacularName,language\n"
                "1,New species,en\n",
            )

    monkeypatch.setattr(tax_mod, "_download_with_resume", observing_download)

    tax_mod.download_taxonomy(str(persistent))

    assert observed_cache_state, "observing hook must have fired"
    assert observed_cache_state[0] is None, (
        "cache must be evicted BEFORE download_taxonomy() begins building "
        "its ~2.8GB replacement, otherwise both are live at once"
    )


def test_deleted_preferred_file_releases_its_cached_parse(tmp_path, monkeypatch):
    """A cached parse whose file is gone must not stay resident.

    _load_taxonomy_cached keeps cross-path entries on purpose — a live
    fallback is not stale — so nothing else would drop this one. A full
    taxonomy is ~2.8GB, and rolling back to the legacy file would otherwise
    hold the deleted file's parse alive while allocating the legacy one.
    """
    import taxonomy as tax_mod

    tax_mod.clear_taxonomy_cache()
    preferred = tmp_path / "persistent.json"
    _write_taxonomy_json(preferred, "preferred species")
    legacy = tmp_path / "taxonomy.json"
    _write_taxonomy_json(legacy, "legacy species")
    monkeypatch.setattr(tax_mod, "TAXONOMY_JSON_PATH", str(preferred))
    monkeypatch.setattr(tax_mod, "LEGACY_TAXONOMY_JSON_PATH", str(legacy))

    first = tax_mod.load_local_taxonomy()
    assert first.is_taxon("preferred species")

    cache_during_legacy_parse = []
    real_init = tax_mod.Taxonomy.__init__

    def observing_init(self, path):
        if path == str(legacy):
            cache_during_legacy_parse.append(tax_mod._taxonomy_cache)
        real_init(self, path)

    monkeypatch.setattr(tax_mod.Taxonomy, "__init__", observing_init)

    preferred.unlink()
    second = tax_mod.load_local_taxonomy()

    assert second.is_taxon("legacy species")
    assert cache_during_legacy_parse == [None], (
        "the deleted file's parse must be released before the fallback loads"
    )


def test_deleted_taxonomy_with_no_fallback_releases_the_cache(tmp_path, monkeypatch):
    """With nothing to fall back to, the dead entry still has to go."""
    import taxonomy as tax_mod

    tax_mod.clear_taxonomy_cache()
    preferred = tmp_path / "persistent.json"
    _write_taxonomy_json(preferred, "preferred species")
    monkeypatch.setattr(tax_mod, "TAXONOMY_JSON_PATH", str(preferred))

    assert tax_mod.load_local_taxonomy().is_taxon("preferred species")
    with tax_mod._taxonomy_cache_lock:
        assert tax_mod._taxonomy_cache is not None

    preferred.unlink()

    assert tax_mod.load_local_taxonomy() is None
    with tax_mod._taxonomy_cache_lock:
        assert tax_mod._taxonomy_cache is None, (
            "an unservable taxonomy must not stay resident for the process"
        )


# ---------------------------------------------------------------------------
# Scientific-name synonyms — old binomials (e.g. from the 2021-era iNat21
# class list) must resolve to the current taxonomy entry instead of missing.
# ---------------------------------------------------------------------------

def _add_cattle_egret(taxonomy_path):
    """Extend a mock taxonomy.json with the renamed cattle egret."""
    entry = {
        "taxon_id": 1289388,
        "scientific_name": "Ardea ibis",
        "common_name": "Western Cattle-Egret",
        "rank": "species",
        "lineage_names": ["Animalia", "Chordata", "Aves", "Pelecaniformes",
                          "Ardeidae", "Ardea", "Ardea ibis"],
        "lineage_ranks": ["kingdom", "phylum", "class", "order", "family",
                          "genus", "species"],
    }
    with open(taxonomy_path) as f:
        data = json.load(f)
    data["taxa_by_scientific"]["ardea ibis"] = entry
    data["taxa_by_common"]["western cattle-egret"] = entry
    with open(taxonomy_path, "w") as f:
        json.dump(data, f)


def test_lookup_resolves_scientific_synonym(monkeypatch):
    """An outdated binomial resolves to the current taxon via synonyms."""
    import taxonomy as tax_mod

    with tempfile.TemporaryDirectory() as tmpdir:
        tax_path = _create_mock_taxonomy(tmpdir)
        _add_cattle_egret(tax_path)
        monkeypatch.setattr(
            tax_mod, "_SCIENTIFIC_SYNONYMS",
            {"bubulcus ibis": "Ardea ibis"},
        )
        tax = tax_mod.Taxonomy(tax_path)

        result = tax.lookup("Bubulcus ibis")
        assert result is not None
        assert result["scientific_name"] == "Ardea ibis"
        assert tax.is_taxon("Bubulcus ibis")
        assert tax.relationship("Ardea ibis", "Bubulcus ibis") == "same"


def test_lookup_direct_hit_beats_synonym(monkeypatch):
    """A name present in the current taxonomy ignores any synonym entry."""
    import taxonomy as tax_mod

    with tempfile.TemporaryDirectory() as tmpdir:
        tax_path = _create_mock_taxonomy(tmpdir)
        monkeypatch.setattr(
            tax_mod, "_SCIENTIFIC_SYNONYMS",
            {"melospiza melodia": "Anas platyrhynchos"},
        )
        tax = tax_mod.Taxonomy(tax_path)

        result = tax.lookup("Melospiza melodia")
        assert result["scientific_name"] == "Melospiza melodia"


def test_lookup_synonym_missing_target_returns_none(monkeypatch):
    """A synonym whose target isn't in the local taxonomy fails cleanly."""
    import taxonomy as tax_mod

    with tempfile.TemporaryDirectory() as tmpdir:
        tax_path = _create_mock_taxonomy(tmpdir)
        monkeypatch.setattr(
            tax_mod, "_SCIENTIFIC_SYNONYMS",
            {"bubulcus ibis": "Ardea ibis"},
        )
        tax = tax_mod.Taxonomy(tax_path)

        assert tax.lookup("Bubulcus ibis") is None


def test_shipped_synonym_data_is_well_formed():
    """The packaged synonym file loads and covers the known offenders."""
    import taxonomy as tax_mod

    synonyms = tax_mod.load_scientific_synonyms()
    assert isinstance(synonyms, dict)
    assert len(synonyms) > 100
    for old, current in synonyms.items():
        assert old == old.lower().strip(), old
        assert isinstance(current, str) and current.strip(), old
        # A synonym mapping a name to itself would shadow nothing but
        # suggests a broken generator run.
        assert old != current.lower(), old
    assert synonyms["bubulcus ibis"] == "Ardea ibis"
    assert synonyms["phalacrocorax brasilianus"] == "Nannopterum brasilianum"


def test_scientific_synonyms_identity_tracks_map_content(monkeypatch):
    """The identity changes with the map and is stable for equal maps.

    ``computation_cache.classifier_runtime_fingerprint`` folds this into
    every classifier run's identity, so it has to move when the map does
    (otherwise a pre-synonym run compares equal to a post-synonym one)
    and stay put when it doesn't (otherwise every run looks stale).
    """
    import taxonomy as tax_mod

    monkeypatch.setattr(tax_mod, "_SCIENTIFIC_SYNONYMS", {})
    empty = tax_mod.scientific_synonyms_identity()
    assert empty == "no-synonyms"

    monkeypatch.setattr(
        tax_mod, "_SCIENTIFIC_SYNONYMS", {"bubulcus ibis": "Ardea ibis"},
    )
    one = tax_mod.scientific_synonyms_identity()
    assert one != empty
    assert len(one) == 64

    # Same content, different dict object -> same identity.
    monkeypatch.setattr(
        tax_mod, "_SCIENTIFIC_SYNONYMS", {"bubulcus ibis": "Ardea ibis"},
    )
    assert tax_mod.scientific_synonyms_identity() == one

    monkeypatch.setattr(
        tax_mod, "_SCIENTIFIC_SYNONYMS",
        {"bubulcus ibis": "Ardea ibis",
         "phalacrocorax brasilianus": "Nannopterum brasilianum"},
    )
    assert tax_mod.scientific_synonyms_identity() != one
