# Review Webapp V2 Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add taxonomy-aware comparison, multi-model support, and neighbor photo grouping to the review webapp.

**Architecture:** Download iNaturalist's full taxonomy DWCA and store it as a local JSON lookup. Replace the substring-heuristic comparison with taxonomy-based reasoning. Make analyze.py model-aware so multiple runs merge into one results.json. Group sequential photos by EXIF timestamp and compute consensus predictions. Add a settings page for model/taxonomy/grouping configuration.

**Tech Stack:** Python 3.11, Flask 3.0, BioCLIP/pybioclip, PIL/Pillow, rawpy, iNaturalist DarwinCore Archive (CSV)

---

### Task 1: Taxonomy Download and Lookup

**Files:**
- Create: `vireo/taxonomy.py`
- Create: `vireo/tests/test_taxonomy.py`

**Step 1: Write the failing tests**

Create `vireo/tests/test_taxonomy.py`:

```python
# vireo/tests/test_taxonomy.py
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
```

**Step 2: Run tests to verify they fail**

Run: `pytest vireo/tests/test_taxonomy.py -v`
Expected: FAIL with "ModuleNotFoundError: No module named 'taxonomy'"

**Step 3: Write the implementation**

Create `vireo/taxonomy.py`:

```python
"""iNaturalist taxonomy: download, parse, and lookup.

Usage:
    python vireo/taxonomy.py --download [--output taxonomy.json]
"""

import argparse
import csv
import io
import json
import logging
import os
import zipfile
from datetime import date
from pathlib import Path

log = logging.getLogger(__name__)

DWCA_URL = "https://www.inaturalist.org/taxa/inaturalist-taxonomy.dwca.zip"

# Ranks we care about, in order from broad to specific
RANK_ORDER = ['kingdom', 'phylum', 'class', 'order', 'family', 'genus', 'species', 'subspecies']


class Taxonomy:
    """Taxonomy lookup backed by a local JSON file.

    Args:
        taxonomy_path: path to taxonomy.json
    """

    def __init__(self, taxonomy_path):
        with open(taxonomy_path) as f:
            data = json.load(f)
        self._by_common = data.get('taxa_by_common', {})
        self._by_scientific = data.get('taxa_by_scientific', {})
        self.last_updated = data.get('last_updated')
        self.taxa_count = len(self._by_common) + len(self._by_scientific)
        log.info("Loaded taxonomy: %d entries (updated %s)", self.taxa_count, self.last_updated)

    def lookup(self, name):
        """Look up a taxon by common name or scientific name.

        Args:
            name: common name (e.g., "Song Sparrow") or scientific name

        Returns:
            dict with taxon_id, scientific_name, common_name, rank,
            lineage_names, lineage_ranks — or None if not found
        """
        key = name.lower().strip()
        result = self._by_common.get(key)
        if result:
            return result
        return self._by_scientific.get(key)

    def is_taxon(self, name):
        """Check if a name is a recognized taxon."""
        return self.lookup(name) is not None

    def relationship(self, name_a, name_b):
        """Determine the taxonomic relationship between two names.

        Returns:
            'same' — identical taxon
            'ancestor' — a is an ancestor of b (a's lineage contains b)
            'descendant' — b is an ancestor of a
            'sibling' — same immediate parent (same genus for species)
            'unrelated' — different lineages (no close relationship)
            None — one or both names not found in taxonomy
        """
        taxon_a = self.lookup(name_a)
        taxon_b = self.lookup(name_b)
        if taxon_a is None or taxon_b is None:
            return None

        lineage_a = [n.lower() for n in taxon_a['lineage_names']]
        lineage_b = [n.lower() for n in taxon_b['lineage_names']]

        sci_a = taxon_a['scientific_name'].lower()
        sci_b = taxon_b['scientific_name'].lower()

        # Same taxon
        if sci_a == sci_b:
            return 'same'

        # a is an ancestor of b (a's scientific name appears in b's lineage)
        if sci_a in lineage_b:
            return 'ancestor'

        # b is an ancestor of a
        if sci_b in lineage_a:
            return 'descendant'

        # Sibling: same immediate parent (last shared ancestor is the direct parent of both)
        # For species, this means same genus
        if len(lineage_a) >= 2 and len(lineage_b) >= 2:
            parent_a = lineage_a[-2]
            parent_b = lineage_b[-2]
            if parent_a == parent_b:
                return 'sibling'

        return 'unrelated'


def download_taxonomy(output_path):
    """Download iNaturalist DWCA taxonomy and build taxonomy.json.

    Downloads the zip, parses taxa.csv and VernacularNames.csv,
    and writes a JSON file keyed by common name and scientific name.
    """
    import urllib.request

    log.info("Downloading iNaturalist taxonomy from %s ...", DWCA_URL)
    response = urllib.request.urlopen(DWCA_URL)
    zip_data = response.read()
    log.info("Downloaded %d MB", len(zip_data) // (1024 * 1024))

    # Parse the DWCA zip
    taxa_by_id = {}
    common_names = {}  # taxon_id -> common_name

    with zipfile.ZipFile(io.BytesIO(zip_data)) as zf:
        file_list = zf.namelist()
        log.info("Archive contents: %s", file_list)

        # Parse taxa.csv — columns: id, parentNameUsageID, scientificName, taxonRank
        taxa_file = None
        for name in file_list:
            if name.lower().endswith('taxa.csv') or name.lower() == 'taxa.csv':
                taxa_file = name
                break
        if not taxa_file:
            raise FileNotFoundError("taxa.csv not found in DWCA archive")

        log.info("Parsing %s ...", taxa_file)
        with zf.open(taxa_file) as f:
            reader = csv.DictReader(io.TextIOWrapper(f, encoding='utf-8'))
            for row in reader:
                taxon_id = row.get('id') or row.get('taxonID')
                if not taxon_id:
                    continue
                taxa_by_id[taxon_id] = {
                    'taxon_id': int(taxon_id),
                    'scientific_name': row.get('scientificName', ''),
                    'rank': (row.get('taxonRank') or '').lower(),
                    'parent_id': row.get('parentNameUsageID', ''),
                }
        log.info("Parsed %d taxa", len(taxa_by_id))

        # Parse VernacularNames (English common names)
        vn_file = None
        for name in file_list:
            if 'vernacularname' in name.lower():
                vn_file = name
                break

        if vn_file:
            log.info("Parsing %s ...", vn_file)
            with zf.open(vn_file) as f:
                reader = csv.DictReader(io.TextIOWrapper(f, encoding='utf-8'))
                for row in reader:
                    lang = row.get('language', '')
                    if lang.lower() != 'en':
                        continue
                    taxon_id = row.get('id') or row.get('taxonID')
                    if taxon_id and taxon_id in taxa_by_id:
                        common_names[taxon_id] = row.get('vernacularName', '')
            log.info("Found %d English common names", len(common_names))

    # Build lineages by walking parent chains
    def _build_lineage(taxon_id):
        lineage_names = []
        lineage_ranks = []
        current = taxon_id
        seen = set()
        while current and current in taxa_by_id and current not in seen:
            seen.add(current)
            t = taxa_by_id[current]
            if t['rank'] in RANK_ORDER:
                lineage_names.append(t['scientific_name'])
                lineage_ranks.append(t['rank'])
            current = t['parent_id']
        lineage_names.reverse()
        lineage_ranks.reverse()
        return lineage_names, lineage_ranks

    # Build the lookup dictionaries
    taxa_by_common = {}
    taxa_by_scientific = {}

    for taxon_id, taxon in taxa_by_id.items():
        rank = taxon['rank']
        if rank not in RANK_ORDER:
            continue

        lineage_names, lineage_ranks = _build_lineage(taxon_id)

        entry = {
            'taxon_id': taxon['taxon_id'],
            'scientific_name': taxon['scientific_name'],
            'common_name': common_names.get(taxon_id, ''),
            'rank': rank,
            'lineage_names': lineage_names,
            'lineage_ranks': lineage_ranks,
        }

        # Index by scientific name
        sci_key = taxon['scientific_name'].lower()
        taxa_by_scientific[sci_key] = entry

        # Index by common name if available
        cn = common_names.get(taxon_id, '')
        if cn:
            cn_key = cn.lower()
            taxa_by_common[cn_key] = entry

    result = {
        'last_updated': str(date.today()),
        'source': 'iNaturalist DWCA',
        'taxa_by_common': taxa_by_common,
        'taxa_by_scientific': taxa_by_scientific,
    }

    with open(output_path, 'w') as f:
        json.dump(result, f)

    log.info("Wrote taxonomy to %s (%d common names, %d scientific names)",
             output_path, len(taxa_by_common), len(taxa_by_scientific))
    return result


def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    parser = argparse.ArgumentParser(description="Download and manage iNaturalist taxonomy.")
    parser.add_argument("--download", action="store_true", help="Download taxonomy from iNaturalist")
    parser.add_argument("--output", default=os.path.join(os.path.dirname(__file__), "taxonomy.json"),
                        help="Output path for taxonomy.json")
    args = parser.parse_args()

    if args.download:
        download_taxonomy(args.output)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
```

**Step 4: Run tests to verify they pass**

Run: `pytest vireo/tests/test_taxonomy.py -v`
Expected: all 11 tests PASS

**Step 5: Commit**

```bash
git add vireo/taxonomy.py vireo/tests/test_taxonomy.py
git commit -m "feat: add taxonomy download and lookup from iNaturalist DWCA"
```

---

### Task 2: Taxonomy-Based Comparison Logic

**Files:**
- Modify: `vireo/compare.py`
- Modify: `vireo/tests/test_compare.py`

**Step 1: Rewrite tests for taxonomy-based categorization**

Replace the contents of `vireo/tests/test_compare.py`:

```python
# vireo/tests/test_compare.py
import json
import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'lr-migration'))


def _write_test_xmp(path, keywords):
    """Write a minimal XMP file with dc:subject keywords."""
    from xmp_writer import write_xmp_sidecar
    write_xmp_sidecar(path, flat_keywords=set(keywords), hierarchical_keywords=set())


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


def test_read_xmp_keywords():
    """read_xmp_keywords returns dc:subject values from an XMP file."""
    from compare import read_xmp_keywords
    with tempfile.NamedTemporaryFile(suffix='.xmp', delete=False) as f:
        _write_test_xmp(f.name, ['Northern cardinal', '0Locations', 'Dyke Marsh'])
        result = read_xmp_keywords(f.name)
        assert result == {'Northern cardinal', '0Locations', 'Dyke Marsh'}
        os.unlink(f.name)


def test_read_xmp_keywords_missing_file():
    """read_xmp_keywords returns empty set for missing file."""
    from compare import read_xmp_keywords
    result = read_xmp_keywords('/tmp/nonexistent.xmp')
    assert result == set()


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
    """Existing is family, prediction is species in that family → refinement."""
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
    """Two species in different genera of same family → disagreement (not refinement)."""
    from compare import categorize
    from taxonomy import Taxonomy

    with tempfile.TemporaryDirectory() as tmpdir:
        tax = Taxonomy(_create_mock_taxonomy(tmpdir))
        # Carolina Wren and Northern House Wren are both species (not family→species)
        result = categorize('Northern House Wren', {'Carolina Wren'}, tax)
        assert result == 'disagreement'


def test_categorize_ignores_non_taxa():
    """Keywords not in taxonomy are ignored (locations, categories)."""
    from compare import categorize
    from taxonomy import Taxonomy

    with tempfile.TemporaryDirectory() as tmpdir:
        tax = Taxonomy(_create_mock_taxonomy(tmpdir))
        # "Dyke Marsh" and "0Locations" not in taxonomy → treated as new
        result = categorize('Northern cardinal', {'Dyke Marsh', '0Locations'}, tax)
        assert result == 'new'
```

**Step 2: Run tests to verify they fail**

Run: `pytest vireo/tests/test_compare.py -v`
Expected: FAIL — `categorize` still expects `labels_vocab` not `Taxonomy`

**Step 3: Update compare.py**

Replace the `categorize` function in `vireo/compare.py`:

```python
"""Read XMP keywords and compare against model predictions."""

import logging
from xml.etree import ElementTree as ET
from pathlib import Path

log = logging.getLogger(__name__)

NS_RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
NS_DC = "http://purl.org/dc/elements/1.1/"


def read_xmp_keywords(xmp_path):
    """Read dc:subject keywords from an XMP sidecar file.

    Args:
        xmp_path: path to .xmp file

    Returns:
        set of keyword strings (empty if file missing or corrupt)
    """
    path = Path(xmp_path)
    if not path.exists():
        return set()

    try:
        tree = ET.parse(path)
    except ET.ParseError:
        log.warning("Corrupt XMP file: %s", xmp_path)
        return set()

    root = tree.getroot()
    keywords = set()
    for li in root.findall(f".//{{{NS_DC}}}subject/{{{NS_RDF}}}Bag/{{{NS_RDF}}}li"):
        if li.text:
            keywords.add(li.text)
    return keywords


def categorize(prediction, existing_keywords, taxonomy):
    """Categorize a prediction relative to existing keywords using taxonomy.

    Args:
        prediction: the model's predicted species name
        existing_keywords: set of all dc:subject keywords from the XMP
        taxonomy: a Taxonomy instance for looking up taxa and relationships

    Returns:
        'match' — prediction matches an existing species keyword
        'new' — no existing species keywords found
        'refinement' — prediction is more specific than an existing keyword
        'disagreement' — prediction differs from existing species keyword
    """
    # Filter existing keywords to just those recognized as taxa
    existing_taxa = []
    for kw in existing_keywords:
        if taxonomy.is_taxon(kw):
            existing_taxa.append(kw)
        else:
            log.debug("Ignoring non-taxon keyword: %s", kw)

    # No recognized taxa in existing keywords — this is new info
    if not existing_taxa:
        return 'new'

    # Check each existing taxon against the prediction
    for taxon_kw in existing_taxa:
        rel = taxonomy.relationship(taxon_kw, prediction)

        if rel == 'same':
            return 'match'
        elif rel == 'ancestor':
            # Existing is broader, prediction is more specific → refinement
            return 'refinement'
        elif rel == 'descendant':
            # Existing is more specific, prediction is broader — unusual but treat as match
            return 'match'

    # If we get here, existing taxa exist but none match/contain the prediction
    return 'disagreement'
```

**Step 4: Run tests to verify they pass**

Run: `pytest vireo/tests/test_compare.py -v`
Expected: all 8 tests PASS

**Step 5: Commit**

```bash
git add vireo/compare.py vireo/tests/test_compare.py
git commit -m "feat: replace heuristic comparison with taxonomy-based categorization"
```

---

### Task 3: Neighbor Photo Grouping

**Files:**
- Create: `vireo/grouping.py`
- Create: `vireo/tests/test_grouping.py`

**Step 1: Write the failing tests**

Create `vireo/tests/test_grouping.py`:

```python
# vireo/tests/test_grouping.py
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))


def test_group_by_timestamp_basic():
    """Sequential photos within time window are grouped together."""
    from grouping import group_by_timestamp

    photos = [
        {'filename': 'DSC_0001.jpg', 'timestamp': datetime(2019, 3, 17, 10, 0, 0)},
        {'filename': 'DSC_0002.jpg', 'timestamp': datetime(2019, 3, 17, 10, 0, 3)},
        {'filename': 'DSC_0003.jpg', 'timestamp': datetime(2019, 3, 17, 10, 0, 5)},
        {'filename': 'DSC_0004.jpg', 'timestamp': datetime(2019, 3, 17, 10, 5, 0)},  # 5 min gap
    ]

    groups = group_by_timestamp(photos, window_seconds=10)
    assert len(groups) == 2
    assert len(groups[0]) == 3  # first 3 photos
    assert len(groups[1]) == 1  # DSC_0004 alone


def test_group_by_timestamp_all_separate():
    """Photos far apart in time form individual groups."""
    from grouping import group_by_timestamp

    photos = [
        {'filename': 'DSC_0001.jpg', 'timestamp': datetime(2019, 3, 17, 10, 0, 0)},
        {'filename': 'DSC_0002.jpg', 'timestamp': datetime(2019, 3, 17, 10, 1, 0)},
        {'filename': 'DSC_0003.jpg', 'timestamp': datetime(2019, 3, 17, 10, 2, 0)},
    ]

    groups = group_by_timestamp(photos, window_seconds=10)
    assert len(groups) == 3
    assert all(len(g) == 1 for g in groups)


def test_group_by_timestamp_no_timestamp():
    """Photos without timestamps each form their own group."""
    from grouping import group_by_timestamp

    photos = [
        {'filename': 'DSC_0001.jpg', 'timestamp': None},
        {'filename': 'DSC_0002.jpg', 'timestamp': None},
    ]

    groups = group_by_timestamp(photos, window_seconds=10)
    assert len(groups) == 2


def test_consensus_prediction():
    """consensus_prediction returns the most common prediction with averaged confidence."""
    from grouping import consensus_prediction

    predictions = [
        {'prediction': 'Song sparrow', 'confidence': 0.80},
        {'prediction': 'Song sparrow', 'confidence': 0.90},
        {'prediction': 'Lincoln sparrow', 'confidence': 0.60},
    ]

    result = consensus_prediction(predictions)
    assert result['prediction'] == 'Song sparrow'
    assert result['confidence'] == 0.85  # average of 0.80 and 0.90
    assert result['vote_count'] == 2
    assert result['total_votes'] == 3
    assert result['individual_predictions'] == {'Song sparrow': 2, 'Lincoln sparrow': 1}


def test_consensus_prediction_tie():
    """When tied, consensus picks the one with higher average confidence."""
    from grouping import consensus_prediction

    predictions = [
        {'prediction': 'Song sparrow', 'confidence': 0.60},
        {'prediction': 'Lincoln sparrow', 'confidence': 0.90},
    ]

    result = consensus_prediction(predictions)
    assert result['prediction'] == 'Lincoln sparrow'
    assert result['confidence'] == 0.90


def test_consensus_prediction_single():
    """Single prediction returns it directly."""
    from grouping import consensus_prediction

    predictions = [
        {'prediction': 'Song sparrow', 'confidence': 0.85},
    ]

    result = consensus_prediction(predictions)
    assert result['prediction'] == 'Song sparrow'
    assert result['confidence'] == 0.85
    assert result['vote_count'] == 1
    assert result['total_votes'] == 1
```

**Step 2: Run tests to verify they fail**

Run: `pytest vireo/tests/test_grouping.py -v`
Expected: FAIL with "ModuleNotFoundError: No module named 'grouping'"

**Step 3: Write the implementation**

Create `vireo/grouping.py`:

```python
"""Group sequential photos by EXIF timestamp proximity."""

import logging
from collections import Counter

log = logging.getLogger(__name__)


def group_by_timestamp(photos, window_seconds=10):
    """Group sequential photos that were taken within a time window.

    Args:
        photos: list of dicts, each with 'filename' and 'timestamp' (datetime or None)
        window_seconds: max seconds between consecutive photos to group them

    Returns:
        list of groups, where each group is a list of photo dicts
    """
    if not photos:
        return []

    groups = []
    current_group = [photos[0]]

    for i in range(1, len(photos)):
        prev = photos[i - 1]
        curr = photos[i]

        # If either has no timestamp, start a new group
        if prev['timestamp'] is None or curr['timestamp'] is None:
            groups.append(current_group)
            current_group = [curr]
            continue

        delta = abs((curr['timestamp'] - prev['timestamp']).total_seconds())
        if delta <= window_seconds:
            current_group.append(curr)
        else:
            groups.append(current_group)
            current_group = [curr]

    groups.append(current_group)
    return groups


def consensus_prediction(predictions):
    """Compute a consensus prediction from multiple individual predictions.

    Args:
        predictions: list of dicts with 'prediction' (str) and 'confidence' (float)

    Returns:
        dict with:
            prediction: the winning species name
            confidence: average confidence of the winning predictions
            vote_count: number of frames agreeing with the consensus
            total_votes: total number of frames
            individual_predictions: dict of species -> count
    """
    counts = Counter(p['prediction'] for p in predictions)
    individual = dict(counts)

    # Group confidences by prediction
    conf_by_pred = {}
    for p in predictions:
        conf_by_pred.setdefault(p['prediction'], []).append(p['confidence'])

    # Pick the most common; break ties by higher average confidence
    best = max(
        counts.keys(),
        key=lambda sp: (counts[sp], sum(conf_by_pred[sp]) / len(conf_by_pred[sp])),
    )

    avg_conf = sum(conf_by_pred[best]) / len(conf_by_pred[best])

    return {
        'prediction': best,
        'confidence': round(avg_conf, 4),
        'vote_count': counts[best],
        'total_votes': len(predictions),
        'individual_predictions': individual,
    }


def read_exif_timestamp(image_path):
    """Read EXIF DateTimeOriginal from an image file.

    Args:
        image_path: path to JPEG/TIFF/RAW file

    Returns:
        datetime or None if not available
    """
    from datetime import datetime
    from PIL import Image
    from PIL.ExifTags import Base as ExifBase

    try:
        img = Image.open(str(image_path))
        exif = img.getexif()
        if not exif:
            return None

        # DateTimeOriginal tag
        dt_str = exif.get(ExifBase.DateTimeOriginal) or exif.get(ExifBase.DateTimeDigitized)
        if dt_str:
            return datetime.strptime(dt_str, "%Y:%m:%d %H:%M:%S")
    except Exception:
        log.debug("Could not read EXIF from %s", image_path)

    return None
```

**Step 4: Run tests to verify they pass**

Run: `pytest vireo/tests/test_grouping.py -v`
Expected: all 6 tests PASS

**Step 5: Commit**

```bash
git add vireo/grouping.py vireo/tests/test_grouping.py
git commit -m "feat: add neighbor photo grouping by EXIF timestamp"
```

---

### Task 4: Multi-Model Analyze Script

**Files:**
- Modify: `vireo/analyze.py`
- Modify: `vireo/tests/test_analyze.py`

**Step 1: Rewrite tests for multi-model and grouping**

Replace the contents of `vireo/tests/test_analyze.py`:

```python
# vireo/tests/test_analyze.py
import json
import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'lr-migration'))

from PIL import Image
from xmp_writer import write_xmp_sidecar


def _create_mock_taxonomy(tmpdir):
    """Create a minimal taxonomy.json for testing."""
    taxonomy = {
        "last_updated": "2026-03-17",
        "source": "test",
        "taxa_by_common": {
            "bird": {"taxon_id": 1, "scientific_name": "Aves", "common_name": "Bird",
                     "rank": "class", "lineage_names": ["Animalia", "Chordata", "Aves"],
                     "lineage_ranks": ["kingdom", "phylum", "class"]},
            "cat": {"taxon_id": 2, "scientific_name": "Felis catus", "common_name": "Cat",
                    "rank": "species", "lineage_names": ["Animalia", "Chordata", "Mammalia", "Carnivora", "Felidae", "Felis", "Felis catus"],
                    "lineage_ranks": ["kingdom", "phylum", "class", "order", "family", "genus", "species"]},
            "dog": {"taxon_id": 3, "scientific_name": "Canis familiaris", "common_name": "Dog",
                    "rank": "species", "lineage_names": ["Animalia", "Chordata", "Mammalia", "Carnivora", "Canidae", "Canis", "Canis familiaris"],
                    "lineage_ranks": ["kingdom", "phylum", "class", "order", "family", "genus", "species"]},
            "northern cardinal": {"taxon_id": 9083, "scientific_name": "Cardinalis cardinalis",
                                  "common_name": "Northern Cardinal", "rank": "species",
                                  "lineage_names": ["Animalia", "Chordata", "Aves", "Passeriformes", "Cardinalidae", "Cardinalis", "Cardinalis cardinalis"],
                                  "lineage_ranks": ["kingdom", "phylum", "class", "order", "family", "genus", "species"]},
        },
        "taxa_by_scientific": {},
    }
    path = os.path.join(tmpdir, "taxonomy.json")
    with open(path, 'w') as f:
        json.dump(taxonomy, f)
    return path


def _create_test_folder(tmpdir):
    """Create a folder with test images and some XMP sidecars."""
    img_dir = os.path.join(tmpdir, "photos")
    os.makedirs(img_dir)

    # Photo with existing species keyword
    img = Image.new('RGB', (224, 224), color='red')
    img.save(os.path.join(img_dir, "bird1.jpg"))
    write_xmp_sidecar(
        os.path.join(img_dir, "bird1.xmp"),
        flat_keywords={'Northern cardinal', 'Dyke Marsh'},
        hierarchical_keywords=set(),
    )

    # Photo with no XMP
    img = Image.new('RGB', (224, 224), color='blue')
    img.save(os.path.join(img_dir, "bird2.jpg"))

    return img_dir


def test_analyze_produces_results_json():
    """analyze() creates results.json with multi-model structure."""
    from analyze import analyze

    with tempfile.TemporaryDirectory() as tmpdir:
        img_dir = _create_test_folder(tmpdir)
        output_dir = os.path.join(tmpdir, "output")
        tax_path = _create_mock_taxonomy(tmpdir)

        analyze(
            folder=img_dir,
            output_dir=output_dir,
            labels=['bird', 'cat', 'dog', 'Northern cardinal'],
            taxonomy_path=tax_path,
            model_str='ViT-B-16',
            pretrained_str='/tmp/bioclip_model/open_clip_pytorch_model.bin',
            threshold=0.0,
            thumbnail_size=200,
        )

        results_path = os.path.join(output_dir, "results.json")
        assert os.path.exists(results_path)

        with open(results_path) as f:
            data = json.load(f)

        assert data['folder'] == img_dir
        assert 'models' in data
        assert 'bioclip-vit-b-16' in data['models']
        # Photos should have predictions dict keyed by model
        for photo in data['photos']:
            assert 'predictions' in photo
            assert 'bioclip-vit-b-16' in photo['predictions']


def test_analyze_generates_thumbnails():
    """analyze() creates thumbnail JPEGs in output_dir/thumbnails/."""
    from analyze import analyze

    with tempfile.TemporaryDirectory() as tmpdir:
        img_dir = _create_test_folder(tmpdir)
        output_dir = os.path.join(tmpdir, "output")
        tax_path = _create_mock_taxonomy(tmpdir)

        analyze(
            folder=img_dir,
            output_dir=output_dir,
            labels=['bird', 'cat', 'dog'],
            taxonomy_path=tax_path,
            model_str='ViT-B-16',
            pretrained_str='/tmp/bioclip_model/open_clip_pytorch_model.bin',
            threshold=0.0,
            thumbnail_size=200,
        )

        thumb_dir = os.path.join(output_dir, "thumbnails")
        assert os.path.isdir(thumb_dir)
        thumbs = os.listdir(thumb_dir)
        assert len(thumbs) >= 1
        thumb = Image.open(os.path.join(thumb_dir, thumbs[0]))
        assert max(thumb.size) <= 200


def test_analyze_merges_models():
    """Running analyze twice with different model names merges results."""
    from analyze import analyze

    with tempfile.TemporaryDirectory() as tmpdir:
        img_dir = _create_test_folder(tmpdir)
        output_dir = os.path.join(tmpdir, "output")
        tax_path = _create_mock_taxonomy(tmpdir)

        # First run
        analyze(
            folder=img_dir,
            output_dir=output_dir,
            labels=['bird', 'cat', 'dog'],
            taxonomy_path=tax_path,
            model_str='ViT-B-16',
            pretrained_str='/tmp/bioclip_model/open_clip_pytorch_model.bin',
            model_name='model-a',
            threshold=0.0,
            thumbnail_size=200,
        )

        # Second run with different model name
        analyze(
            folder=img_dir,
            output_dir=output_dir,
            labels=['bird', 'cat', 'dog'],
            taxonomy_path=tax_path,
            model_str='ViT-B-16',
            pretrained_str='/tmp/bioclip_model/open_clip_pytorch_model.bin',
            model_name='model-b',
            threshold=0.0,
            thumbnail_size=200,
        )

        with open(os.path.join(output_dir, "results.json")) as f:
            data = json.load(f)

        assert 'model-a' in data['models']
        assert 'model-b' in data['models']
        # Each photo should have predictions from both models
        for photo in data['photos']:
            assert 'model-a' in photo['predictions']
            assert 'model-b' in photo['predictions']
```

**Step 2: Run tests to verify they fail**

Run: `pytest vireo/tests/test_analyze.py -v`
Expected: FAIL — `analyze()` doesn't accept `taxonomy_path` parameter

**Step 3: Rewrite analyze.py**

Replace the full contents of `vireo/analyze.py`:

```python
"""Scan photos, classify, compare to existing XMP keywords, generate review data.

Usage:
    python vireo/analyze.py --folder /path/to/photos --labels-file labels.txt
"""

import argparse
import json
import logging
import os
import sys
import tempfile
from datetime import date
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lr-migration'))

from classifier import Classifier
from compare import read_xmp_keywords, categorize
from grouping import group_by_timestamp, consensus_prediction, read_exif_timestamp
from image_loader import load_image, SUPPORTED_EXTENSIONS
from taxonomy import Taxonomy

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s: %(message)s",
)
log = logging.getLogger(__name__)


def _model_slug(model_name, model_str):
    """Generate a model key slug."""
    if model_name:
        return model_name
    return f"bioclip-{model_str.lower().replace('/', '-')}"


def analyze(folder, output_dir, labels, taxonomy_path,
            model_str='ViT-B-16',
            pretrained_str='/tmp/bioclip_model/open_clip_pytorch_model.bin',
            model_name=None, threshold=0.4, thumbnail_size=400,
            recursive=True, group_window=10):
    """Scan a folder, classify images, compare to existing keywords, write results.

    Args:
        folder: path to image folder
        output_dir: path to output directory for results.json and thumbnails/
        labels: list of species labels for the classifier
        taxonomy_path: path to taxonomy.json
        model_str: BioCLIP model string
        pretrained_str: path to model weights
        model_name: optional human-readable model name (used as key in results)
        threshold: minimum confidence score
        thumbnail_size: max dimension for thumbnails
        recursive: scan subfolders
        group_window: seconds for neighbor grouping (0 to disable)
    """
    os.makedirs(output_dir, exist_ok=True)
    thumb_dir = os.path.join(output_dir, "thumbnails")
    os.makedirs(thumb_dir, exist_ok=True)

    tax = Taxonomy(taxonomy_path)
    clf = Classifier(labels=labels, model_str=model_str, pretrained_str=pretrained_str)
    slug = _model_slug(model_name, model_str)

    folder_path = Path(folder)
    if recursive:
        image_files = sorted(
            f for f in folder_path.rglob('*')
            if f.suffix.lower() in SUPPORTED_EXTENSIONS and not f.name.startswith('.')
        )
    else:
        image_files = sorted(
            f for f in folder_path.iterdir()
            if f.suffix.lower() in SUPPORTED_EXTENSIONS and not f.name.startswith('.')
        )

    log.info("Found %d images in %s", len(image_files), folder)

    # Load existing results if present (for multi-model merging)
    results_path = os.path.join(output_dir, "results.json")
    existing_results = None
    if os.path.exists(results_path):
        with open(results_path) as f:
            existing_results = json.load(f)
        log.info("Found existing results.json — will merge model '%s'", slug)

    # Build a lookup of existing photo entries by image_path for merging
    existing_photos = {}
    if existing_results:
        for p in existing_results.get('photos', []):
            existing_photos[p['image_path']] = p

    # Phase 1: classify all images and read timestamps
    classified = []
    stats = {'total': len(image_files), 'new': 0, 'refinement': 0,
             'disagreement': 0, 'match': 0, 'failed': 0, 'below_threshold': 0}

    for i, image_path in enumerate(image_files):
        img = load_image(str(image_path))
        if img is None:
            stats['failed'] += 1
            continue

        # Classify via temp file
        with tempfile.NamedTemporaryFile(suffix='.jpg', delete=False) as tmp:
            tmp_path = tmp.name
            img.save(tmp_path, quality=85)

        try:
            predictions = clf.classify(tmp_path, threshold=threshold)
        except Exception:
            log.warning("Classification failed for %s", image_path, exc_info=True)
            stats['failed'] += 1
            continue
        finally:
            os.unlink(tmp_path)

        if not predictions:
            stats['below_threshold'] += 1
            continue

        top = predictions[0]

        # Read existing XMP keywords and categorize
        xmp_path = image_path.with_suffix('.xmp')
        existing = read_xmp_keywords(str(xmp_path))
        category = categorize(top['species'], existing, tax)
        stats[category] += 1

        if category == 'match':
            continue

        # Read EXIF timestamp for grouping
        timestamp = None
        if image_path.suffix.lower() in {'.jpg', '.jpeg', '.tiff', '.tif'}:
            timestamp = read_exif_timestamp(str(image_path))

        # Build unique thumbnail name
        rel_path = image_path.relative_to(folder_path)
        thumb_name = str(rel_path).replace(os.sep, '_')
        thumb_name = Path(thumb_name).stem + ".jpg"

        # Filter existing keywords to just species for display
        existing_species = [kw for kw in existing if tax.is_taxon(kw)]

        classified.append({
            'image_path': str(image_path),
            'xmp_path': str(xmp_path),
            'filename': thumb_name,
            'prediction': top['species'],
            'confidence': round(top['score'], 4),
            'category': category,
            'existing_species': existing_species,
            'timestamp': timestamp,
            'img': img,
        })

        if (i + 1) % 100 == 0:
            log.info("Progress: %d/%d images", i + 1, len(image_files))

    # Phase 2: group neighbors
    photos = []
    if group_window > 0 and classified:
        groups = group_by_timestamp(classified, window_seconds=group_window)
    else:
        groups = [[c] for c in classified]

    group_counter = 0
    for group in groups:
        if len(group) == 1:
            item = group[0]
            # Generate thumbnail
            thumb_path = os.path.join(thumb_dir, item['filename'])
            thumb = item['img'].copy()
            thumb.thumbnail((thumbnail_size, thumbnail_size))
            thumb.save(thumb_path, quality=85)

            model_pred = {
                'prediction': item['prediction'],
                'confidence': item['confidence'],
                'category': item['category'],
            }

            # Merge with existing photo entry if present
            if item['image_path'] in existing_photos:
                photo = existing_photos[item['image_path']]
                photo['predictions'][slug] = model_pred
            else:
                photo = {
                    'filename': item['filename'],
                    'image_path': item['image_path'],
                    'xmp_path': item['xmp_path'],
                    'existing_species': item['existing_species'],
                    'predictions': {slug: model_pred},
                    'status': 'pending',
                }
            photos.append(photo)
        else:
            # Group of multiple photos
            group_counter += 1
            group_id = f"g{group_counter:04d}"

            # Compute consensus
            preds_for_consensus = [
                {'prediction': item['prediction'], 'confidence': item['confidence']}
                for item in group
            ]
            cons = consensus_prediction(preds_for_consensus)

            # Use the best category from the group (prefer the consensus prediction's category)
            # Re-categorize using the consensus prediction
            representative = group[0]
            cons_category = categorize(cons['prediction'], set(representative['existing_species']), tax)
            if cons_category == 'match':
                cons_category = representative['category']  # fallback

            # Generate thumbnail for representative
            rep_thumb = os.path.join(thumb_dir, representative['filename'])
            thumb = representative['img'].copy()
            thumb.thumbnail((thumbnail_size, thumbnail_size))
            thumb.save(rep_thumb, quality=85)

            # Also save individual member thumbnails
            members = []
            for item in group:
                members.append(item['filename'])
                member_thumb_path = os.path.join(thumb_dir, item['filename'])
                if not os.path.exists(member_thumb_path):
                    t = item['img'].copy()
                    t.thumbnail((thumbnail_size, thumbnail_size))
                    t.save(member_thumb_path, quality=85)

            model_consensus = {
                'prediction': cons['prediction'],
                'confidence': cons['confidence'],
                'individual_predictions': cons['individual_predictions'],
            }

            photo = {
                'group_id': group_id,
                'representative': representative['filename'],
                'members': members,
                'member_paths': [item['image_path'] for item in group],
                'member_xmp_paths': [item['xmp_path'] for item in group],
                'existing_species': representative['existing_species'],
                'consensus': {slug: model_consensus},
                'category': cons_category,
                'status': 'pending',
            }
            photos.append(photo)

    # Build final results
    models = {}
    if existing_results:
        models = existing_results.get('models', {})
    models[slug] = {
        'model_str': model_str,
        'pretrained_str': pretrained_str,
        'run_date': str(date.today()),
        'threshold': threshold,
    }

    # For photos that were in existing results but not re-classified (e.g., matches),
    # keep them if they had predictions from other models
    if existing_results:
        existing_image_paths = {p.get('image_path') or '' for p in photos}
        for p in existing_results.get('photos', []):
            ip = p.get('image_path', '')
            if ip and ip not in existing_image_paths:
                photos.append(p)

    results = {
        'folder': str(folder),
        'models': models,
        'settings': {
            'threshold': threshold,
            'thumbnail_size': thumbnail_size,
            'group_window': group_window,
        },
        'stats': stats,
        'photos': photos,
    }

    with open(results_path, 'w') as f:
        json.dump(results, f, indent=2)

    log.info("--- Analysis Summary ---")
    log.info("Model:          %s", slug)
    log.info("Total images:   %d", stats['total'])
    log.info("New:            %d", stats['new'])
    log.info("Refinements:    %d", stats['refinement'])
    log.info("Disagreements:  %d", stats['disagreement'])
    log.info("Matches:        %d (hidden)", stats['match'])
    log.info("Below threshold:%d", stats['below_threshold'])
    log.info("Failed:         %d", stats['failed'])
    log.info("Groups:         %d", group_counter)
    log.info("Results saved to %s", results_path)

    return results


def main():
    parser = argparse.ArgumentParser(
        description="Analyze photos: classify, compare to existing labels, generate review data."
    )
    parser.add_argument("--folder", required=True, help="Path to image folder")
    parser.add_argument("--labels-file", required=True, help="Text file with one label per line")
    parser.add_argument("--taxonomy", default=os.path.join(os.path.dirname(__file__), "taxonomy.json"),
                        help="Path to taxonomy.json")
    parser.add_argument("--output-dir", default="/tmp/photo-review", help="Output directory")
    parser.add_argument("--model-weights", default="/tmp/bioclip_model/open_clip_pytorch_model.bin")
    parser.add_argument("--model-name", default=None, help="Human-readable model name")
    parser.add_argument("--threshold", type=float, default=0.4)
    parser.add_argument("--thumbnail-size", type=int, default=400)
    parser.add_argument("--group-window", type=int, default=10,
                        help="Seconds for neighbor grouping (0 to disable)")
    parser.add_argument("--no-recursive", action="store_true")
    args = parser.parse_args()

    with open(args.labels_file) as f:
        labels = [line.strip() for line in f if line.strip()]
    log.info("Loaded %d labels from %s", len(labels), args.labels_file)

    analyze(
        folder=args.folder,
        output_dir=args.output_dir,
        labels=labels,
        taxonomy_path=args.taxonomy,
        pretrained_str=args.model_weights,
        model_name=args.model_name,
        threshold=args.threshold,
        thumbnail_size=args.thumbnail_size,
        group_window=args.group_window,
        recursive=not args.no_recursive,
    )


if __name__ == "__main__":
    main()
```

**Step 4: Run tests to verify they pass**

Run: `pytest vireo/tests/test_analyze.py -v`
Expected: all 3 tests PASS

**Step 5: Commit**

```bash
git add vireo/analyze.py vireo/tests/test_analyze.py
git commit -m "feat: multi-model analyze with taxonomy and neighbor grouping"
```

---

### Task 5: Update Review Server for Multi-Model and Groups

**Files:**
- Modify: `vireo/review_server.py`
- Modify: `vireo/tests/test_review_server.py`

**Step 1: Update test fixtures and add new tests**

Add to `vireo/tests/test_review_server.py` — update `_create_test_review_data` and `_create_multi_photo_data` to use the new multi-model `predictions` format, and add tests for group accept and settings. The full replacement file:

```python
# vireo/tests/test_review_server.py
import json
import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'lr-migration'))

from PIL import Image


def _create_test_review_data(tmpdir):
    """Create a minimal results.json and thumbnails dir for testing."""
    thumb_dir = os.path.join(tmpdir, "thumbnails")
    os.makedirs(thumb_dir)

    img = Image.new('RGB', (100, 100), color='red')
    img_path = os.path.join(tmpdir, "bird1.jpg")
    img.save(img_path)

    from xmp_writer import write_xmp_sidecar
    xmp_path = os.path.join(tmpdir, "bird1.xmp")
    write_xmp_sidecar(xmp_path, flat_keywords={'Dyke Marsh'}, hierarchical_keywords=set())

    thumb = Image.new('RGB', (100, 100), color='red')
    thumb.save(os.path.join(thumb_dir, "bird1.jpg"))

    results = {
        'folder': tmpdir,
        'models': {
            'bioclip-vit-b-16': {
                'model_str': 'ViT-B-16',
                'run_date': '2026-03-17',
                'threshold': 0.4,
            }
        },
        'settings': {'threshold': 0.4, 'thumbnail_size': 400, 'group_window': 10},
        'stats': {'total': 1, 'new': 1, 'refinement': 0, 'disagreement': 0, 'match': 0},
        'photos': [
            {
                'filename': 'bird1.jpg',
                'image_path': img_path,
                'xmp_path': xmp_path,
                'existing_species': [],
                'predictions': {
                    'bioclip-vit-b-16': {
                        'prediction': 'Northern cardinal',
                        'confidence': 0.85,
                        'category': 'new',
                    }
                },
                'status': 'pending',
            }
        ],
    }

    results_path = os.path.join(tmpdir, "results.json")
    with open(results_path, 'w') as f:
        json.dump(results, f)
    return results_path


def _create_group_data(tmpdir):
    """Create results.json with a photo group for testing."""
    thumb_dir = os.path.join(tmpdir, "thumbnails")
    os.makedirs(thumb_dir)

    from xmp_writer import write_xmp_sidecar

    paths = []
    for name in ['bird_a.jpg', 'bird_b.jpg', 'bird_c.jpg']:
        img_path = os.path.join(tmpdir, name)
        xmp_path = os.path.join(tmpdir, name.replace('.jpg', '.xmp'))
        Image.new('RGB', (100, 100)).save(img_path)
        Image.new('RGB', (50, 50)).save(os.path.join(thumb_dir, name))
        write_xmp_sidecar(xmp_path, flat_keywords=set(), hierarchical_keywords=set())
        paths.append((img_path, xmp_path))

    results = {
        'folder': tmpdir,
        'models': {'bioclip-vit-b-16': {'model_str': 'ViT-B-16', 'run_date': '2026-03-17', 'threshold': 0.4}},
        'settings': {'threshold': 0.4, 'thumbnail_size': 400, 'group_window': 10},
        'stats': {'total': 3, 'new': 3},
        'photos': [
            {
                'group_id': 'g0001',
                'representative': 'bird_a.jpg',
                'members': ['bird_a.jpg', 'bird_b.jpg', 'bird_c.jpg'],
                'member_paths': [p[0] for p in paths],
                'member_xmp_paths': [p[1] for p in paths],
                'existing_species': [],
                'consensus': {
                    'bioclip-vit-b-16': {
                        'prediction': 'Song sparrow',
                        'confidence': 0.82,
                        'individual_predictions': {'Song sparrow': 2, 'Lincoln sparrow': 1},
                    }
                },
                'category': 'new',
                'status': 'pending',
            }
        ],
    }

    results_path = os.path.join(tmpdir, "results.json")
    with open(results_path, 'w') as f:
        json.dump(results, f)
    return results_path


def test_get_photos():
    """GET /api/photos returns the photo list."""
    from review_server import create_app
    with tempfile.TemporaryDirectory() as tmpdir:
        _create_test_review_data(tmpdir)
        app = create_app(tmpdir)
        client = app.test_client()
        resp = client.get('/api/photos')
        assert resp.status_code == 200
        data = resp.get_json()
        assert len(data['photos']) == 1


def test_accept_writes_xmp():
    """POST /api/accept/<filename> writes prediction to XMP."""
    from review_server import create_app
    from compare import read_xmp_keywords
    with tempfile.TemporaryDirectory() as tmpdir:
        results_path = _create_test_review_data(tmpdir)
        app = create_app(tmpdir)
        client = app.test_client()

        resp = client.post('/api/accept/bird1.jpg', json={'model': 'bioclip-vit-b-16'})
        assert resp.status_code == 200

        xmp_path = os.path.join(tmpdir, "bird1.xmp")
        keywords = read_xmp_keywords(xmp_path)
        assert 'Northern cardinal' in keywords
        assert 'Dyke Marsh' in keywords


def test_accept_group_writes_all_xmps():
    """POST /api/accept-group/<group_id> writes prediction to all member XMP files."""
    from review_server import create_app
    from compare import read_xmp_keywords
    with tempfile.TemporaryDirectory() as tmpdir:
        _create_group_data(tmpdir)
        app = create_app(tmpdir)
        client = app.test_client()

        resp = client.post('/api/accept-group/g0001', json={'model': 'bioclip-vit-b-16'})
        assert resp.status_code == 200
        data = resp.get_json()
        assert data['accepted_count'] == 3

        for name in ['bird_a.xmp', 'bird_b.xmp', 'bird_c.xmp']:
            kw = read_xmp_keywords(os.path.join(tmpdir, name))
            assert 'Song sparrow' in kw


def test_settings_get_and_save():
    """GET /api/settings returns settings, POST /api/settings saves them."""
    from review_server import create_app
    with tempfile.TemporaryDirectory() as tmpdir:
        _create_test_review_data(tmpdir)
        app = create_app(tmpdir)
        client = app.test_client()

        resp = client.get('/api/settings')
        assert resp.status_code == 200
        data = resp.get_json()
        assert 'group_window' in data

        resp = client.post('/api/settings', json={'group_window': 20, 'default_threshold': 0.5})
        assert resp.status_code == 200

        # Verify saved
        settings_path = os.path.join(tmpdir, "settings.json")
        assert os.path.exists(settings_path)
        with open(settings_path) as f:
            saved = json.load(f)
        assert saved['group_window'] == 20


def test_skip_updates_status():
    """POST /api/skip/<filename> marks photo as skipped."""
    from review_server import create_app
    with tempfile.TemporaryDirectory() as tmpdir:
        results_path = _create_test_review_data(tmpdir)
        app = create_app(tmpdir)
        client = app.test_client()
        resp = client.post('/api/skip/bird1.jpg')
        assert resp.status_code == 200
        with open(results_path) as f:
            data = json.load(f)
        assert data['photos'][0]['status'] == 'skipped'


def test_index_route():
    """GET / returns 200."""
    from review_server import create_app
    with tempfile.TemporaryDirectory() as tmpdir:
        _create_test_review_data(tmpdir)
        app = create_app(tmpdir)
        client = app.test_client()
        resp = client.get('/')
        assert resp.status_code == 200


def test_settings_page_route():
    """GET /settings returns 200."""
    from review_server import create_app
    with tempfile.TemporaryDirectory() as tmpdir:
        _create_test_review_data(tmpdir)
        app = create_app(tmpdir)
        client = app.test_client()
        resp = client.get('/settings')
        assert resp.status_code == 200
```

**Step 2: Run tests to verify they fail**

Run: `pytest vireo/tests/test_review_server.py -v`
Expected: FAIL — accept endpoint doesn't accept model param, no accept-group or settings endpoints

**Step 3: Update review_server.py**

Replace the full contents of `vireo/review_server.py`:

```python
"""Flask server for reviewing vireo predictions.

Usage:
    python vireo/review_server.py [--data-dir /tmp/photo-review] [--port 8080]
"""

import argparse
import json
import logging
import os
import sys
import webbrowser

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lr-migration'))

from flask import Flask, jsonify, request, send_from_directory, render_template
from xmp_writer import write_xmp_sidecar

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)


def create_app(data_dir):
    """Create the Flask app configured with a data directory.

    Args:
        data_dir: path containing results.json and thumbnails/
    """
    app = Flask(__name__, template_folder=os.path.join(os.path.dirname(__file__), 'templates'))
    app.config['DATA_DIR'] = data_dir

    def _load_results():
        with open(os.path.join(data_dir, 'results.json')) as f:
            return json.load(f)

    def _save_results(data):
        with open(os.path.join(data_dir, 'results.json'), 'w') as f:
            json.dump(data, f, indent=2)

    def _load_settings():
        path = os.path.join(data_dir, 'settings.json')
        if os.path.exists(path):
            with open(path) as f:
                return json.load(f)
        # Default from results.json settings
        data = _load_results()
        return data.get('settings', {})

    def _save_settings(settings):
        path = os.path.join(data_dir, 'settings.json')
        with open(path, 'w') as f:
            json.dump(settings, f, indent=2)

    @app.route('/')
    def index():
        return render_template('review.html')

    @app.route('/settings')
    def settings_page():
        return render_template('settings.html')

    @app.route('/api/photos')
    def get_photos():
        data = _load_results()
        category = request.args.get('category')
        if category:
            data['photos'] = [p for p in data['photos'] if p.get('category') == category]
        return jsonify(data)

    @app.route('/api/accept/<filename>', methods=['POST'])
    def accept(filename):
        body = request.get_json(silent=True) or {}
        model = body.get('model')

        data = _load_results()
        for photo in data['photos']:
            if photo.get('filename') == filename:
                # Get prediction from the specified model or the first available
                preds = photo.get('predictions', {})
                if model and model in preds:
                    prediction = preds[model]['prediction']
                elif preds:
                    first_model = next(iter(preds))
                    prediction = preds[first_model]['prediction']
                else:
                    return jsonify({'error': 'no predictions available'}), 400

                write_xmp_sidecar(
                    photo['xmp_path'],
                    flat_keywords={prediction},
                    hierarchical_keywords=set(),
                )
                photo['status'] = 'accepted'
                _save_results(data)
                return jsonify({'ok': True, 'status': 'accepted', 'prediction': prediction})
        return jsonify({'error': 'not found'}), 404

    @app.route('/api/skip/<filename>', methods=['POST'])
    def skip(filename):
        data = _load_results()
        for photo in data['photos']:
            if photo.get('filename') == filename:
                photo['status'] = 'skipped'
                _save_results(data)
                return jsonify({'ok': True, 'status': 'skipped'})
        return jsonify({'error': 'not found'}), 404

    @app.route('/api/accept-group/<group_id>', methods=['POST'])
    def accept_group(group_id):
        body = request.get_json(silent=True) or {}
        model = body.get('model')

        data = _load_results()
        for photo in data['photos']:
            if photo.get('group_id') == group_id:
                cons = photo.get('consensus', {})
                if model and model in cons:
                    prediction = cons[model]['prediction']
                elif cons:
                    first_model = next(iter(cons))
                    prediction = cons[first_model]['prediction']
                else:
                    return jsonify({'error': 'no consensus available'}), 400

                xmp_paths = photo.get('member_xmp_paths', [])
                written = 0
                for xp in xmp_paths:
                    try:
                        write_xmp_sidecar(
                            xp,
                            flat_keywords={prediction},
                            hierarchical_keywords=set(),
                        )
                        written += 1
                    except Exception:
                        log.warning("Failed to write XMP: %s", xp, exc_info=True)

                photo['status'] = 'accepted'
                _save_results(data)
                return jsonify({'ok': True, 'accepted_count': written, 'prediction': prediction})
        return jsonify({'error': 'group not found'}), 404

    @app.route('/api/accept-batch', methods=['POST'])
    def accept_batch():
        body = request.get_json(silent=True) or {}
        category = body.get('category')
        min_confidence = body.get('min_confidence', 0.0)
        model = body.get('model')

        data = _load_results()
        accepted = 0
        for photo in data['photos']:
            if photo['status'] != 'pending':
                continue
            if category and photo.get('category') != category:
                continue

            # Handle individual photos
            preds = photo.get('predictions', {})
            if preds:
                if model and model in preds:
                    pred = preds[model]
                elif preds:
                    pred = preds[next(iter(preds))]
                else:
                    continue

                if pred['confidence'] < min_confidence:
                    continue

                try:
                    write_xmp_sidecar(
                        photo['xmp_path'],
                        flat_keywords={pred['prediction']},
                        hierarchical_keywords=set(),
                    )
                    photo['status'] = 'accepted'
                    accepted += 1
                except Exception:
                    log.warning("Failed to write XMP for %s", photo.get('filename'), exc_info=True)

            # Handle groups
            elif photo.get('consensus'):
                cons = photo['consensus']
                if model and model in cons:
                    pred = cons[model]
                elif cons:
                    pred = cons[next(iter(cons))]
                else:
                    continue

                if pred['confidence'] < min_confidence:
                    continue

                for xp in photo.get('member_xmp_paths', []):
                    try:
                        write_xmp_sidecar(xp, flat_keywords={pred['prediction']}, hierarchical_keywords=set())
                    except Exception:
                        log.warning("Failed to write XMP: %s", xp, exc_info=True)

                photo['status'] = 'accepted'
                accepted += 1

        _save_results(data)
        return jsonify({'ok': True, 'accepted': accepted})

    @app.route('/api/settings', methods=['GET'])
    def get_settings():
        return jsonify(_load_settings())

    @app.route('/api/settings', methods=['POST'])
    def save_settings():
        body = request.get_json(silent=True) or {}
        settings = _load_settings()
        settings.update(body)
        _save_settings(settings)
        return jsonify({'ok': True})

    @app.route('/thumbnails/<filename>')
    def thumbnail(filename):
        return send_from_directory(os.path.join(data_dir, 'thumbnails'), filename)

    return app


def main():
    parser = argparse.ArgumentParser(description="Review vireo predictions.")
    parser.add_argument("--data-dir", default="/tmp/photo-review", help="Directory with results.json")
    parser.add_argument("--port", type=int, default=8080)
    args = parser.parse_args()

    app = create_app(args.data_dir)
    webbrowser.open(f"http://localhost:{args.port}")
    app.run(host='127.0.0.1', port=args.port, debug=False)


if __name__ == "__main__":
    main()
```

**Step 4: Create placeholder settings.html**

Create `vireo/templates/settings.html`:

```html
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Settings</title>
<style>
  body { font-family: sans-serif; background: #1a1a2e; color: #e0e0e0; padding: 24px; }
  h1 { font-size: 20px; margin-bottom: 16px; }
  a { color: #4ecca3; }
</style>
</head>
<body>
  <h1>Settings</h1>
  <p>Settings page — see Task 6 for full implementation.</p>
  <p><a href="/">Back to Review</a></p>
</body>
</html>
```

**Step 5: Run tests to verify they pass**

Run: `pytest vireo/tests/test_review_server.py -v`
Expected: all 7 tests PASS

**Step 6: Commit**

```bash
git add vireo/review_server.py vireo/tests/test_review_server.py vireo/templates/settings.html
git commit -m "feat: update review server for multi-model, groups, and settings API"
```

---

### Task 6: Update Review UI and Settings Page

**Files:**
- Modify: `vireo/templates/review.html`
- Modify: `vireo/templates/settings.html`

**Step 1: Update review.html**

Replace the full contents of `vireo/templates/review.html` to support:
- Model selector dropdown in header (when multiple models exist)
- Group cards with collapse/expand and "(N photos)" badge
- Link to settings page (gear icon goes to `/settings` instead of toggling inline panel)
- Keep thumbnail size slider inline
- Read predictions from `photo.predictions[currentModel]` instead of flat `photo.prediction`

This is a large HTML file. The subagent implementing this task should read the existing `review.html` at `vireo/templates/review.html` and the design doc at `docs/plans/2026-03-17-review-webapp-v2-design.md` to understand all required changes. Key changes:

1. Add a `<select id="modelSelector">` in the header populated from `allData.models`
2. Update `renderGrid()` to handle both individual photos (`photo.predictions`) and groups (`photo.consensus`, `photo.members`, `photo.group_id`)
3. Group cards show representative thumbnail, consensus prediction, "(N photos)" badge, and an expand button
4. Accept on a group calls `POST /api/accept-group/<group_id>` with `{model: currentModel}`
5. Accept on individual calls `POST /api/accept/<filename>` with `{model: currentModel}`
6. Gear icon links to `/settings` instead of toggling inline panel
7. Keep the inline thumbnail size slider and confidence threshold filter

**Step 2: Update settings.html**

Replace the placeholder `vireo/templates/settings.html` with a full settings page that:
- Shows configured models from `GET /api/photos` response's `models` object
- Shows taxonomy status (from `GET /api/settings`)
- Has grouping toggle and time window slider
- Has default threshold setting
- Saves via `POST /api/settings`
- Links back to review page

**Step 3: Verify the review server tests still pass**

Run: `pytest vireo/tests/test_review_server.py -v`
Expected: all 7 tests PASS

**Step 4: Commit**

```bash
git add vireo/templates/review.html vireo/templates/settings.html
git commit -m "feat: update review UI for multi-model, groups, and settings page"
```

---

### Task 7: Integration Smoke Test

**Step 1: Download the iNaturalist taxonomy**

```bash
python vireo/taxonomy.py --download
```

This creates `vireo/taxonomy.json`. Verify it loaded successfully by checking the log output for taxa counts.

**Step 2: Run analyze on real photos**

```bash
python vireo/analyze.py \
  --folder "/Volumes/Photography/Raw Files/USA/2019/2019-03-17" \
  --labels-file /tmp/usa_labels.txt \
  --output-dir /tmp/photo-review-v2 \
  --group-window 10
```

Verify:
- `results.json` has the multi-model `predictions` structure
- Groups were formed (check "Groups:" in the summary)
- Categories use taxonomy (no "Dyke Marsh" treated as species)
- Thumbnails generated in `/tmp/photo-review-v2/thumbnails/`

**Step 3: Start the review server and verify**

```bash
python vireo/review_server.py --data-dir /tmp/photo-review-v2 --port 8080
```

Verify in the browser:
- Review page loads with photos
- Group cards show with "(N photos)" badge
- Expand/collapse works on groups
- Settings page accessible at `/settings`
- Accept/Skip works for both individual photos and groups

**Step 4: Run all tests**

```bash
pytest vireo/tests/ -v
```

Expected: all tests pass (taxonomy, compare, grouping, analyze, review_server)
