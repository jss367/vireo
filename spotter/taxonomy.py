"""iNaturalist taxonomy: download, parse, and lookup.

Usage:
    python spotter/taxonomy.py --download [--output taxonomy.json]
"""

import argparse
import csv
import io
import json
import logging
import os
import zipfile
from datetime import date

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
        # Build normalized index for fuzzy lookups (handles hyphens, etc.)
        self._by_common_normalized = {}
        for key, val in self._by_common.items():
            nk = self._normalize(key)
            if nk not in self._by_common_normalized:
                self._by_common_normalized[nk] = val
        log.info("Loaded taxonomy: %d entries (updated %s)", self.taxa_count, self.last_updated)

    @staticmethod
    def _normalize(name):
        """Normalize a name for lookup: lowercase, strip hyphens and extra spaces."""
        return name.lower().strip().replace('-', ' ').replace('  ', ' ')

    def lookup(self, name):
        """Look up a taxon by common name or scientific name.

        Handles punctuation differences like "Scrub-Jay" vs "scrub jay".

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
        result = self._by_scientific.get(key)
        if result:
            return result

        # Fuzzy: try normalized lookup (handles hyphens, e.g. "scrub jay" vs "scrub-jay")
        return self._by_common_normalized.get(self._normalize(name))

    def is_taxon(self, name):
        """Check if a name is a recognized taxon."""
        return self.lookup(name) is not None

    def get_hierarchy(self, name):
        """Look up a species and return its full hierarchy as a flat dict.

        Returns:
            dict with keys: kingdom, phylum, class, order, family, genus,
            scientific_name — or empty dict if not found
        """
        taxon = self.lookup(name)
        if not taxon:
            return {}

        hierarchy = {'scientific_name': taxon.get('scientific_name', '')}
        lineage_names = taxon.get('lineage_names', [])
        lineage_ranks = taxon.get('lineage_ranks', [])

        for rank_name, sci_name in zip(lineage_ranks, lineage_names):
            if rank_name in ('kingdom', 'phylum', 'class', 'order', 'family', 'genus'):
                hierarchy[rank_name] = sci_name

        return hierarchy

    def relationship(self, name_a, name_b):
        """Determine the taxonomic relationship between two names.

        Returns:
            'same' — identical taxon
            'ancestor' — a is an ancestor of b (a's name appears in b's lineage)
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


def download_taxonomy(output_path, progress_callback=None):
    """Download iNaturalist DWCA taxonomy and build taxonomy.json.

    Downloads the zip, parses taxa.csv and VernacularNames.csv,
    and writes a JSON file keyed by common name and scientific name.

    Args:
        progress_callback: optional callable(message) for status updates
    """
    import urllib.request

    def _status(msg):
        log.info(msg)
        if progress_callback:
            progress_callback(msg)

    _status("Downloading iNaturalist taxonomy archive...")
    response = urllib.request.urlopen(DWCA_URL)
    zip_data = response.read()
    _status(f"Downloaded {len(zip_data) // (1024 * 1024)} MB — parsing...")

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
                # parentNameUsageID may be a URL like https://www.inaturalist.org/taxa/48460
                parent_raw = row.get('parentNameUsageID', '')
                if parent_raw and '/' in parent_raw:
                    parent_id = parent_raw.rsplit('/', 1)[-1]
                else:
                    parent_id = parent_raw

                taxa_by_id[taxon_id] = {
                    'taxon_id': int(taxon_id),
                    'scientific_name': row.get('scientificName', ''),
                    'rank': (row.get('taxonRank') or '').lower(),
                    'parent_id': parent_id,
                }
        _status(f"Parsed {len(taxa_by_id):,} taxa")

        # Parse VernacularNames (English common names)
        # Prefer VernacularNames-english.csv, fall back to VernacularNames.csv
        vn_file = None
        for name in file_list:
            if name.lower() == 'vernacularnames-english.csv':
                vn_file = name
                break
        if not vn_file:
            for name in file_list:
                if name.lower() == 'vernacularnames.csv':
                    vn_file = name
                    break

        if vn_file:
            log.info("Parsing %s ...", vn_file)
            with zf.open(vn_file) as f:
                reader = csv.DictReader(io.TextIOWrapper(f, encoding='utf-8'))
                for row in reader:
                    # Language-specific files may not have a language column
                    lang = row.get('language', 'en')
                    if lang and lang.lower() != 'en':
                        continue
                    taxon_id = row.get('id') or row.get('taxonID')
                    if taxon_id and taxon_id in taxa_by_id and taxon_id not in common_names:
                        common_names[taxon_id] = row.get('vernacularName', '')
            _status(f"Found {len(common_names):,} English common names")
        else:
            log.warning("No VernacularNames file found in archive")

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
    _status(f"Building lineages for {len(taxa_by_id):,} taxa...")
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

    _status(f"Writing taxonomy ({len(taxa_by_common):,} common + {len(taxa_by_scientific):,} scientific names)...")
    with open(output_path, 'w') as f:
        json.dump(result, f)
    _status(f"Taxonomy complete: {len(taxa_by_common):,} common names, {len(taxa_by_scientific):,} scientific names")
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
