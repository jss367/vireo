"""Species identity shared by classifier enrichment and review views.

Model prompts are evidence, not primary keys. Source taxon IDs outrank name
lookups; an ambiguous name must never silently pick the first matching taxon.
"""

import hashlib
import json
from dataclasses import dataclass

from keyword_normalization import keyword_match_key

RESOLUTION_VERSION = "species-identity-v3"

# Verified against iNaturalist taxon 18976 and Cornell's A. viridigenalis
# account. Older DWCA snapshots assign this English name to A. rhodocorytha.
# This is a common-name correction, NOT a synonym between those two species.
COMMON_NAME_CORRECTIONS = {
    "red-crowned amazon": {
        "taxon_id": 18976,
        "scientific_name": "Amazona viridigenalis",
        "common_name": "Red-crowned Parrot",
        "rank": "species",
    },
}
COMMON_NAME_CORRECTIONS["red-crowned parrot"] = COMMON_NAME_CORRECTIONS["red-crowned amazon"]


def resolution_identity():
    return hashlib.sha256(json.dumps(
        [RESOLUTION_VERSION, COMMON_NAME_CORRECTIONS], sort_keys=True,
    ).encode()).hexdigest()


@dataclass(frozen=True)
class SpeciesIdentity:
    key: str
    display_name: str
    scientific_name: str | None = None
    taxon_id: int | None = None  # iNaturalist ID, never a local SQLite row ID
    rank: str | None = None


class SpeciesResolver:
    def __init__(self, taxonomy=None, db=None):
        self.taxonomy = taxonomy
        self.db = db
        self._cache = {}
        self._common_names_verified = False
        self._ambiguous_common = set()
        if db is not None:
            from taxonomy import COMMON_NAME_IDENTITY_VERSION
            self._common_names_verified = db.get_meta("common_name_identity_version") == str(COMMON_NAME_IDENTITY_VERSION)
            self._ambiguous_common = set(json.loads(db.get_meta("ambiguous_common_names") or "[]"))

    def _lookup_id(self, taxon_id):
        if self.taxonomy is not None:
            lookup = getattr(self.taxonomy, "lookup_id", None)
            return lookup(taxon_id) if lookup else None
        if self.db is not None:
            row = self.db.conn.execute(
                "SELECT inat_id AS taxon_id, name AS scientific_name, common_name, rank "
                "FROM taxa WHERE inat_id = ?", (taxon_id,),
            ).fetchone()
            return dict(row) if row else None
        return None

    def _lookup(self, name, scientific=False):
        if not name:
            return None
        if self.taxonomy is not None:
            lookup = getattr(self.taxonomy, "lookup", None)
            return lookup(name) if lookup else None
        if self.db is None:
            return None
        correction = COMMON_NAME_CORRECTIONS.get(keyword_match_key(name)) if not scientific else None
        if correction:
            return self._lookup(correction["scientific_name"], scientific=True) or correction
        # Existing DB indexes also lost alternate-name collisions. Until a
        # taxonomy import records its provenance, only explicit science/IDs
        # and the curated corrections above are evidence of identity.
        if not scientific and (not self._common_names_verified or name.lower().strip() in self._ambiguous_common):
            return self._lookup(name, scientific=True)
        if scientific:
            rows = self.db.conn.execute(
                "SELECT inat_id AS taxon_id, name AS scientific_name, common_name, rank "
                "FROM taxa WHERE name = ?", (name,),
            ).fetchall()
            if not rows:
                from taxonomy import load_scientific_synonyms
                current = load_scientific_synonyms().get(name.lower())
                if current:
                    rows = self.db.conn.execute(
                        "SELECT inat_id AS taxon_id, name AS scientific_name, common_name, rank "
                        "FROM taxa WHERE name = ?", (current,),
                    ).fetchall()
        else:
            rows = self.db.conn.execute(
                "SELECT DISTINCT t.inat_id AS taxon_id, t.name AS scientific_name, "
                "t.common_name, t.rank FROM taxa t WHERE t.name = ? OR t.common_name = ? "
                "UNION SELECT t.inat_id, t.name, t.common_name, t.rank "
                "FROM taxa_common_names cn JOIN taxa t ON t.id = cn.taxon_id "
                "WHERE cn.name = ? COLLATE NOCASE",
                (name, name, name),
            ).fetchall()
        return dict(rows[0]) if len(rows) == 1 else None

    def resolve(self, name, scientific_name=None, source=None):
        name = str(name or "").strip()
        # Most callers resolve a bare name. Serializing ``None`` for the cache
        # key on every one of those costs more than the lookup it guards — a
        # catalog-sized comparison resolves hundreds of thousands of names.
        source_key = json.dumps(source, sort_keys=True) if source else None
        cache_key = (name, scientific_name, source_key)
        if cache_key in self._cache:
            return self._cache[cache_key]
        fallback = SpeciesIdentity("name:" + keyword_match_key(name), name)
        if source and source.get("ambiguous"):
            return fallback
        # Explicit identity from the label source or fixed model head wins.
        evidence = source or COMMON_NAME_CORRECTIONS.get(keyword_match_key(name))
        if scientific_name and not source:
            evidence = {"scientific_name": scientific_name}
        if evidence:
            sci = evidence.get("scientific_name")
            taxon = self._lookup_id(evidence["taxon_id"]) if evidence.get("taxon_id") else None
            taxon = taxon or self._lookup(sci, scientific=True)
            # A changed taxon assignment requires reconciliation, not a blind merge.
            if (taxon and evidence.get("taxon_id") and taxon.get("taxon_id")
                    and taxon["taxon_id"] != evidence["taxon_id"]):
                taxon = None
            taxon = taxon or evidence
        else:
            taxon = self._lookup(name)
        if taxon and (taxon.get("taxon_id") or taxon.get("scientific_name")):
            tid = taxon.get("taxon_id")
            sci = taxon.get("scientific_name")
            display = taxon.get("common_name") or sci or name
            if tid and not sci:
                # The source distinguishes these taxa even when this catalog
                # cannot name them. Make that distinction visible in review.
                display = f"{name} (taxon {tid})"
            if evidence and sci and display != sci and (self.taxonomy is not None or self.db is not None):
                display_taxon = self._lookup(display)
                if (not display_taxon or display_taxon.get("scientific_name") != sci):
                    display = f"{display} ({sci})"
            result = SpeciesIdentity(
                f"taxon:{tid}" if tid else "scientific:" + sci.casefold(),
                display,
                sci, tid, taxon.get("rank"),
            )
        else:
            result = fallback
        self._cache[cache_key] = result
        return result

    def prediction(self, row):
        """Fixed-head scientific names are primary evidence. Old custom-label
        metadata was inferred from text and can contain group-level mistakes.
        New source-backed custom predictions carry the exact source binomial.
        """
        row = dict(row)
        model = row.get("classifier_model", row.get("model", "")) or ""
        native = row.get("labels_fingerprint") == "tol" or model.startswith("iNat")
        source = None
        if row.get("source_taxon_id"):
            source = {"taxon_id": row["source_taxon_id"], "scientific_name": row.get("scientific_name")}
        if not source and not (native and row.get("scientific_name")):
            return self.display(row.get("species"))
        return self.resolve(row.get("species"), row.get("scientific_name") if native else None, source)

    def display(self, name):
        """Resolve review labels without treating arbitrary parentheses as aliases."""
        name = str(name or "").strip()
        prefix, sep, suffix = name.rpartition(" (")
        if sep and suffix.endswith(")"):
            qualifier = suffix[:-1]
            if qualifier.startswith("taxon ") and qualifier[6:].isdecimal() and len(qualifier[6:]) <= 19:
                taxon_id = int(qualifier[6:])
                if 0 < taxon_id < (1 << 63):
                    return self.resolve(prefix, source={"taxon_id": taxon_id})
            taxon = self._lookup(qualifier, scientific=True)
            if taxon:
                return self.resolve(prefix, source=taxon)
        return self.resolve(name)

    def consensus(self, row):
        """Identity the accept action applies, including legacy burst votes.

        A display spelling of the row's own identity retains its source
        evidence. A vote for another species must not inherit that evidence.
        """
        row = dict(row)
        own = self.prediction(row)
        species = row.get("species") or ""
        if row.get("group_id") and row.get("individual"):
            try:
                votes = json.loads(row["individual"])
                if isinstance(votes, dict) and votes:
                    species = max(votes, key=lambda sp: votes[sp])
            except (TypeError, ValueError):
                pass
        spellings = [row.get("species"), own.display_name]
        if own.scientific_name:
            spellings.extend([
                own.scientific_name,
                f"{row.get('species')} ({own.scientific_name})",
            ])
        if keyword_match_key(species) in {keyword_match_key(s) for s in spellings if s}:
            return own
        return self.display(species)


def correct_common_name_index(by_common, by_scientific):
    """Overlay verified corrections without mutating the on-disk taxonomy."""
    for name, evidence in COMMON_NAME_CORRECTIONS.items():
        entry = by_scientific.get(evidence["scientific_name"].lower())
        if entry and entry.get("taxon_id") == evidence["taxon_id"]:
            by_common[name] = entry


def species_entry_key(entry):
    """Read the optional identity key on a serialized prediction tuple."""
    return entry[3] if len(entry) > 3 and entry[3] else entry[0]
