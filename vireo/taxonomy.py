"""iNaturalist taxonomy: download, parse, and lookup.

Includes two loading approaches:
1. DWCA-based: Downloads the DarwinCore Archive zip and builds a JSON lookup file.
2. AWS open-data based: Downloads taxa.csv.gz and loads taxa into the SQLite
   database for structured querying. This is the newer approach used by
   load_taxa_from_file() and load_taxonomy().

Data source (AWS): https://inaturalist-open-data.s3.amazonaws.com/taxa.csv.gz
Format: Tab-separated, 6 columns: taxon_id, ancestry, rank_level, rank, name, active

Usage:
    python vireo/taxonomy.py --download [--output taxonomy.json]
"""

import argparse
import csv
import gzip
import io
import json
import logging
import os
import urllib.request
import zipfile
from datetime import date

import requests

log = logging.getLogger(__name__)

DWCA_URL = "https://www.inaturalist.org/taxa/inaturalist-taxonomy.dwca.zip"

# --- AWS open-data taxa.csv.gz loader constants ---
TAXA_URL = "https://inaturalist-open-data.s3.amazonaws.com/taxa.csv.gz"

TARGET_KINGDOMS = {"Animalia", "Plantae", "Fungi"}
TARGET_KINGDOM_INAT_IDS = {1, 47126, 47170}  # Animalia, Plantae, Fungi

MAJOR_RANK_LEVELS = {70, 60, 50, 40, 30, 20, 10}  # kingdom through species
RANK_LEVEL_TO_NAME = {
    70: "kingdom", 60: "phylum", 50: "class", 40: "order",
    30: "family", 20: "genus", 10: "species",
}

INAT_API_BASE = "https://api.inaturalist.org/v1"
INAT_BATCH_SIZE = 30  # iNat API allows up to 30 IDs per request

# Ranks we care about, in order from broad to specific
RANK_ORDER = [
    "kingdom",
    "phylum",
    "class",
    "order",
    "family",
    "genus",
    "species",
    "subspecies",
]


class Taxonomy:
    """Taxonomy lookup backed by a local JSON file.

    Args:
        taxonomy_path: path to taxonomy.json
    """

    def __init__(self, taxonomy_path):
        self._path = taxonomy_path
        with open(taxonomy_path) as f:
            data = json.load(f)
        self._by_common = data.get("taxa_by_common", {})
        self._by_scientific = data.get("taxa_by_scientific", {})
        self._api_misses = set(data.get("api_misses", []))
        self.last_updated = data.get("last_updated")
        self.taxa_count = len(self._by_common) + len(self._by_scientific)
        # Build normalized index for fuzzy lookups (handles hyphens, etc.)
        self._by_common_normalized = {}
        for key, val in self._by_common.items():
            nk = self._normalize(key)
            if nk not in self._by_common_normalized:
                self._by_common_normalized[nk] = val
        # Track whether new data was added (for save)
        self._dirty = False
        log.info(
            "Loaded taxonomy: %d entries (updated %s)",
            self.taxa_count,
            self.last_updated,
        )

    @staticmethod
    def _normalize(name):
        """Normalize a name for lookup: lowercase, strip hyphens and extra spaces."""
        return name.lower().strip().replace("-", " ").replace("  ", " ")

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

    def api_lookup(self, name):
        """Look up a name via the iNaturalist API (handles alternate/regional names).

        Queries the autocomplete endpoint which matches against all known
        common names, not just the preferred one. If a match is found, the
        alternate name is cached locally so future lookups are instant.
        Names that don't match are also cached to avoid repeated API calls.

        Returns:
            taxon dict (same shape as lookup()), or None
        """
        # Skip names we've already tried and failed to resolve
        norm_name = self._normalize(name)
        if norm_name in self._api_misses:
            return None

        import urllib.request

        try:
            q = urllib.parse.quote(name)
            url = f"https://api.inaturalist.org/v1/taxa/autocomplete?q={q}&per_page=5&rank=species,subspecies,genus,family,order,class,phylum,kingdom"
            req = urllib.request.Request(url)
            req.add_header("User-Agent", "vireo-taxonomy/1.0")
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read())
        except Exception:
            log.debug("iNat API lookup failed for '%s'", name, exc_info=True)
            return None

        # Find a result where the matched_term matches our query
        for result in data.get("results", []):
            matched = result.get("matched_term", "")
            if self._normalize(matched) != norm_name:
                continue
            # Found a match — look up by the taxon's scientific name first
            sci = result.get("name", "").lower()
            existing = self._by_scientific.get(sci)
            if existing:
                # Cache this alternate name for future lookups
                alt_key = name.lower().strip()
                self._by_common[alt_key] = existing
                self._by_common_normalized[norm_name] = existing
                self._dirty = True
                log.info(
                    "Resolved alternate name '%s' -> '%s' (%s) via iNat API",
                    name,
                    existing.get("common_name"),
                    existing.get("scientific_name"),
                )
                return existing

        # No match — remember this so we don't ask again
        self._api_misses.add(norm_name)
        self._dirty = True
        return None

    def save(self):
        """Persist any newly discovered alternate names and misses back to taxonomy.json."""
        if not self._dirty:
            return
        with open(self._path) as f:
            data = json.load(f)
        data["taxa_by_common"] = self._by_common
        data["api_misses"] = sorted(self._api_misses)
        with open(self._path, "w") as f:
            json.dump(data, f)
        self._dirty = False
        log.info("Saved updated taxonomy with new alternate names")

    def get_hierarchy(self, name):
        """Look up a species and return its full hierarchy as a flat dict.

        Returns:
            dict with keys: kingdom, phylum, class, order, family, genus,
            scientific_name — or empty dict if not found
        """
        taxon = self.lookup(name)
        if not taxon:
            return {}

        hierarchy = {"scientific_name": taxon.get("scientific_name", "")}
        lineage_names = taxon.get("lineage_names", [])
        lineage_ranks = taxon.get("lineage_ranks", [])

        for rank_name, sci_name in zip(lineage_ranks, lineage_names):
            if rank_name in ("kingdom", "phylum", "class", "order", "family", "genus"):
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

        lineage_a = [n.lower() for n in taxon_a["lineage_names"]]
        lineage_b = [n.lower() for n in taxon_b["lineage_names"]]

        sci_a = taxon_a["scientific_name"].lower()
        sci_b = taxon_b["scientific_name"].lower()

        # Same taxon
        if sci_a == sci_b:
            return "same"

        # a is an ancestor of b (a's scientific name appears in b's lineage)
        if sci_a in lineage_b:
            return "ancestor"

        # b is an ancestor of a
        if sci_b in lineage_a:
            return "descendant"

        # Sibling: same immediate parent (last shared ancestor is the direct parent of both)
        # For species, this means same genus
        if len(lineage_a) >= 2 and len(lineage_b) >= 2:
            parent_a = lineage_a[-2]
            parent_b = lineage_b[-2]
            if parent_a == parent_b:
                return "sibling"

        return "unrelated"


# --- AWS open-data taxa.csv.gz loader functions ---


def download_taxa(dest_path):
    """Download the iNat taxa.csv.gz file from AWS open data."""
    log.info("Downloading iNat taxonomy from %s ...", TAXA_URL)
    urllib.request.urlretrieve(TAXA_URL, dest_path)
    log.info("Downloaded to %s", dest_path)
    return dest_path


def load_taxa_from_file(db, gz_path):
    """Parse taxa.csv.gz and insert filtered taxa into the database.

    Filters to: active taxa, under Animalia/Plantae/Fungi, at major ranks.
    Resolves parent_id to the nearest ancestor also in the filtered set.

    Returns dict with 'loaded' and 'skipped' counts.
    """
    # Pass 1: read all taxa into memory, filter, and determine kingdoms
    all_taxa = {}   # inat_id -> {name, rank, rank_level, ancestry_ids, kingdom}
    kept_ids = set()

    with gzip.open(gz_path, 'rt') as f:
        reader = csv.reader(f, delimiter='\t')
        for row in reader:
            if len(row) < 6:
                continue
            inat_id = int(row[0])
            ancestry_str = row[1]
            rank_level = float(row[2])
            rank = row[3]
            name = row[4]
            active = row[5].lower() == 'true'

            if not active:
                continue

            ancestry_ids = []
            if ancestry_str:
                ancestry_ids = [int(x) for x in ancestry_str.split('/')]

            all_taxa[inat_id] = {
                'name': name,
                'rank': rank,
                'rank_level': rank_level,
                'ancestry_ids': ancestry_ids,
            }

    # Determine kingdom for each taxon and filter
    filtered = {}
    for inat_id, t in all_taxa.items():
        # Skip non-major ranks
        rl = int(t['rank_level']) if t['rank_level'] == int(t['rank_level']) else None
        if rl not in MAJOR_RANK_LEVELS:
            continue

        # Determine kingdom: check if taxon IS a target kingdom, or has one as ancestor
        kingdom = None
        if inat_id in TARGET_KINGDOM_INAT_IDS:
            kingdom = t['name']
        else:
            for aid in t['ancestry_ids']:
                if aid in TARGET_KINGDOM_INAT_IDS:
                    kingdom = all_taxa[aid]['name'] if aid in all_taxa else None
                    break

        if kingdom is None:
            continue

        filtered[inat_id] = {
            'name': t['name'],
            'rank': RANK_LEVEL_TO_NAME[rl],
            'ancestry_ids': t['ancestry_ids'],
            'kingdom': kingdom,
        }
        kept_ids.add(inat_id)

    # Pass 2: resolve parent_id to nearest kept ancestor
    for _inat_id, t in filtered.items():
        parent_inat_id = None
        for aid in reversed(t['ancestry_ids']):
            if aid in kept_ids:
                parent_inat_id = aid
                break
        t['parent_inat_id'] = parent_inat_id

    # Insert into database
    # First pass: insert all taxa without parent_id (to get local IDs)
    inat_to_local = {}
    for inat_id, t in filtered.items():
        db.conn.execute(
            "INSERT INTO taxa (inat_id, name, rank, kingdom) "
            "VALUES (?, ?, ?, ?) "
            "ON CONFLICT(inat_id) DO UPDATE SET "
            "name = excluded.name, rank = excluded.rank, kingdom = excluded.kingdom",
            (inat_id, t['name'], t['rank'], t['kingdom']),
        )
        row = db.conn.execute(
            "SELECT id FROM taxa WHERE inat_id = ?", (inat_id,)
        ).fetchone()
        if row:
            inat_to_local[inat_id] = row['id']

    # Second pass: set parent_id using local IDs
    for inat_id, t in filtered.items():
        if t['parent_inat_id'] and t['parent_inat_id'] in inat_to_local:
            local_id = inat_to_local[inat_id]
            parent_local_id = inat_to_local[t['parent_inat_id']]
            db.conn.execute(
                "UPDATE taxa SET parent_id = ? WHERE id = ?",
                (parent_local_id, local_id),
            )

    db.conn.commit()

    loaded = len(filtered)
    skipped = len(all_taxa) - loaded
    log.info("Taxonomy loaded: %d taxa imported, %d skipped", loaded, skipped)
    return {"loaded": loaded, "skipped": skipped}


def load_taxonomy(db, data_dir=None):
    """Download and load the full iNat taxonomy.

    Args:
        db: Database instance
        data_dir: directory to store downloaded files (default: ~/.vireo/taxonomy/)
    """
    if data_dir is None:
        data_dir = os.path.expanduser("~/.vireo/taxonomy")
    os.makedirs(data_dir, exist_ok=True)

    gz_path = os.path.join(data_dir, "taxa.csv.gz")
    if not os.path.exists(gz_path):
        download_taxa(gz_path)

    return load_taxa_from_file(db, gz_path)


def fetch_common_names(db, locale='en'):
    """Fetch common names from the iNat API for all taxa in the database.

    Batches requests to the iNat API, updates taxa.common_name with the
    preferred common name, and inserts all English names into taxa_common_names.

    Returns dict with 'updated' count.
    """
    rows = db.conn.execute(
        "SELECT id, inat_id FROM taxa WHERE inat_id IS NOT NULL"
    ).fetchall()

    inat_ids = [(r['id'], r['inat_id']) for r in rows]
    updated = 0

    for i in range(0, len(inat_ids), INAT_BATCH_SIZE):
        batch = inat_ids[i:i + INAT_BATCH_SIZE]
        id_str = ','.join(str(iid) for _, iid in batch)
        local_by_inat = {iid: lid for lid, iid in batch}

        try:
            resp = requests.get(
                f"{INAT_API_BASE}/taxa",
                params={'id': id_str, 'per_page': INAT_BATCH_SIZE},
                timeout=30,
            )
            if resp.status_code != 200:
                log.warning("iNat API returned %d for batch %d", resp.status_code, i)
                continue

            for taxon in resp.json().get('results', []):
                inat_id = taxon['id']
                local_id = local_by_inat.get(inat_id)
                if not local_id:
                    continue

                preferred = taxon.get('preferred_common_name')
                if preferred:
                    db.conn.execute(
                        "UPDATE taxa SET common_name = ? WHERE id = ?",
                        (preferred, local_id),
                    )
                    updated += 1

                for name_entry in taxon.get('names', []):
                    if name_entry.get('locale') == locale:
                        db.conn.execute(
                            "INSERT OR IGNORE INTO taxa_common_names "
                            "(taxon_id, name, locale) VALUES (?, ?, ?)",
                            (local_id, name_entry['name'], locale),
                        )
        except requests.RequestException as e:
            log.warning("iNat API request failed: %s", e)
            continue

    db.conn.commit()
    log.info("Common names: %d taxa updated", updated)
    return {"updated": updated}


# Default informal groups for wildlife photography.
# Each maps a common name to a list of scientific names (order or family level).
DEFAULT_INFORMAL_GROUPS = {
    "Raptors": ["Accipitriformes", "Falconiformes", "Strigiformes"],
    "Shorebirds": ["Charadriiformes"],
    "Waterfowl": ["Anseriformes"],
    "Songbirds": ["Passeriformes"],
    "Hummingbirds": ["Trochilidae"],
    "Wading birds": ["Ardeidae", "Ciconiidae", "Threskiornithidae"],
    "Woodpeckers": ["Picidae"],
    "Gamebirds": ["Galliformes"],
}


def seed_informal_groups(db):
    """Create default informal groups and link them to taxa nodes.

    Only links groups to taxa that exist in the database. Skips groups
    that already exist (idempotent).

    Returns dict with 'groups_created' count.
    """
    created = 0
    for group_name, taxon_names in DEFAULT_INFORMAL_GROUPS.items():
        # Insert group (ignore if exists)
        db.conn.execute(
            "INSERT OR IGNORE INTO informal_groups (name) VALUES (?)",
            (group_name,),
        )
        group_row = db.conn.execute(
            "SELECT id FROM informal_groups WHERE name = ?", (group_name,)
        ).fetchone()
        group_id = group_row["id"]

        linked_any = False
        for taxon_name in taxon_names:
            taxon_row = db.conn.execute(
                "SELECT id FROM taxa WHERE name = ?", (taxon_name,)
            ).fetchone()
            if taxon_row:
                db.conn.execute(
                    "INSERT OR IGNORE INTO informal_group_taxa "
                    "(group_id, taxon_id) VALUES (?, ?)",
                    (group_id, taxon_row["id"]),
                )
                linked_any = True

        if linked_any:
            created += 1

    db.conn.commit()
    log.info("Informal groups: %d created/verified", created)
    return {"groups_created": created}


# --- DWCA-based taxonomy loader (legacy) ---


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
    common_names = {}  # taxon_id -> preferred common_name
    alt_names = {}  # taxon_id -> [all English vernacular names]

    with zipfile.ZipFile(io.BytesIO(zip_data)) as zf:
        file_list = zf.namelist()
        log.info("Archive contents: %s", file_list)

        # Parse taxa.csv — columns: id, parentNameUsageID, scientificName, taxonRank
        taxa_file = None
        for name in file_list:
            if name.lower().endswith("taxa.csv") or name.lower() == "taxa.csv":
                taxa_file = name
                break
        if not taxa_file:
            raise FileNotFoundError("taxa.csv not found in DWCA archive")

        log.info("Parsing %s ...", taxa_file)
        with zf.open(taxa_file) as f:
            reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8"))
            for row in reader:
                taxon_id = row.get("id") or row.get("taxonID")
                if not taxon_id:
                    continue
                # parentNameUsageID may be a URL like https://www.inaturalist.org/taxa/48460
                parent_raw = row.get("parentNameUsageID", "")
                if parent_raw and "/" in parent_raw:
                    parent_id = parent_raw.rsplit("/", 1)[-1]
                else:
                    parent_id = parent_raw

                taxa_by_id[taxon_id] = {
                    "taxon_id": int(taxon_id),
                    "scientific_name": row.get("scientificName", ""),
                    "rank": (row.get("taxonRank") or "").lower(),
                    "parent_id": parent_id,
                }
        _status(f"Parsed {len(taxa_by_id):,} taxa")

        # Parse VernacularNames (English common names)
        # Prefer VernacularNames-english.csv, fall back to VernacularNames.csv
        vn_file = None
        for name in file_list:
            if name.lower() == "vernacularnames-english.csv":
                vn_file = name
                break
        if not vn_file:
            for name in file_list:
                if name.lower() == "vernacularnames.csv":
                    vn_file = name
                    break

        if vn_file:
            log.info("Parsing %s ...", vn_file)
            with zf.open(vn_file) as f:
                reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8"))
                for row in reader:
                    # Language-specific files may not have a language column
                    lang = row.get("language", "en")
                    if lang and lang.lower() != "en":
                        continue
                    taxon_id = row.get("id") or row.get("taxonID")
                    vn = row.get("vernacularName", "")
                    if taxon_id and taxon_id in taxa_by_id and vn:
                        if taxon_id not in common_names:
                            common_names[taxon_id] = vn
                        # Collect all English names per taxon for alternate-name indexing
                        if taxon_id not in alt_names:
                            alt_names[taxon_id] = []
                        alt_names[taxon_id].append(vn)
            _status(f"Found {len(common_names):,} English common names ({sum(len(v) for v in alt_names.values()):,} total including alternates)")
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
            if t["rank"] in RANK_ORDER:
                lineage_names.append(t["scientific_name"])
                lineage_ranks.append(t["rank"])
            current = t["parent_id"]
        lineage_names.reverse()
        lineage_ranks.reverse()
        return lineage_names, lineage_ranks

    # Build the lookup dictionaries
    _status(f"Building lineages for {len(taxa_by_id):,} taxa...")
    taxa_by_common = {}
    taxa_by_scientific = {}

    for taxon_id, taxon in taxa_by_id.items():
        rank = taxon["rank"]
        if rank not in RANK_ORDER:
            continue

        lineage_names, lineage_ranks = _build_lineage(taxon_id)

        entry = {
            "taxon_id": taxon["taxon_id"],
            "scientific_name": taxon["scientific_name"],
            "common_name": common_names.get(taxon_id, ""),
            "rank": rank,
            "lineage_names": lineage_names,
            "lineage_ranks": lineage_ranks,
        }

        # Index by scientific name
        sci_key = taxon["scientific_name"].lower()
        taxa_by_scientific[sci_key] = entry

        # Index by all English common names (preferred + alternates)
        for cn in alt_names.get(taxon_id, []):
            cn_key = cn.lower()
            if cn_key not in taxa_by_common:
                taxa_by_common[cn_key] = entry

    result = {
        "last_updated": str(date.today()),
        "source": "iNaturalist DWCA",
        "taxa_by_common": taxa_by_common,
        "taxa_by_scientific": taxa_by_scientific,
    }

    _status(
        f"Writing taxonomy ({len(taxa_by_common):,} common + {len(taxa_by_scientific):,} scientific names)..."
    )
    with open(output_path, "w") as f:
        json.dump(result, f)
    _status(
        f"Taxonomy complete: {len(taxa_by_common):,} common names, {len(taxa_by_scientific):,} scientific names"
    )
    return result


def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    parser = argparse.ArgumentParser(
        description="Download and manage iNaturalist taxonomy."
    )
    parser.add_argument(
        "--download", action="store_true", help="Download taxonomy from iNaturalist"
    )
    parser.add_argument(
        "--output",
        default=os.path.join(os.path.dirname(__file__), "taxonomy.json"),
        help="Output path for taxonomy.json",
    )
    args = parser.parse_args()

    if args.download:
        download_taxonomy(args.output)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
