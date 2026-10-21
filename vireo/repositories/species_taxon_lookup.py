"""Shared catalog taxon lookup for keyword writes and identity merge guards."""

import sqlite3
import unicodedata
from collections.abc import Callable

from keyword_normalization import keyword_match_key

_TAXON_LOOKUP_TRANSLATION = str.maketrans({
    "’": "'",
    "‘": "'",
    "`": "'",
    "´": "'",
    "ʼ": "'",
    "ʹ": "'",
    "‛": "'",
    "“": '"',
    "”": '"',
    "„": '"',
    "‟": '"',
    "‐": "-",
    "‑": "-",
    "‒": "-",
    "–": "-",
    "—": "-",
    "―": "-",
})


def taxon_lookup_variants(name: str) -> list[str]:
    """Return ordered taxon-name variants for punctuation-tolerant lookup."""
    stripped = str(name).strip()
    variants = []
    for variant in (
        stripped,
        unicodedata.normalize("NFKC", stripped).translate(_TAXON_LOOKUP_TRANSLATION),
    ):
        collapsed = " ".join(variant.split())
        if collapsed and collapsed not in variants:
            variants.append(collapsed)
    return variants


def lookup_taxon_id(
    conn: sqlite3.Connection, name: str, prefer_species: bool = False, species_only: bool = False,
    *, variants: Callable[[str], list[str]] = taxon_lookup_variants,
) -> int | None:
    """Return the local taxa.id matching a keyword name, if any.

    When ``prefer_species`` is true, break ties in favor of a
    ``rank='species'`` taxon. A catalog can hold homonyms across ranks
    (for example a common species ``Puma`` alongside the genus
    ``Puma``); without a preference, an unordered ``LIMIT 1`` can bind
    an ``is_species=True`` keyword to the non-species taxon, and every
    downstream ``rank='species'`` filter (Life List, Compare, Explorer)
    then silently drops the photo even though the accept appeared to
    succeed.

    When ``prefer_species`` is true, the direct ``taxa`` match is only
    returned immediately if it is species-rank. Otherwise the
    ``taxa_common_names`` fallback is still consulted for a species-rank
    alternate name before the higher-rank direct hit wins.
    ``populate_taxa_db_from_json`` explicitly indexes alternate English
    names in ``taxa_common_names``, so accepting an alternate species
    label that collides with a genus/family can and does happen.

    ``species_only`` tightens ``prefer_species`` from "prefer" to
    "require": if no lookup variant surfaces a species-rank taxon,
    return ``None`` instead of falling back to the higher-rank hit.
    Explicit-species callers (``is_species=True`` inserts and rebinds
    along ``add_keyword``'s taxonomy path) must set this — the row
    gets stamped ``is_species=1`` unconditionally once ``taxon_id``
    is returned, and downstream rank readers restrict to
    ``t.rank = 'species' OR t.rank IS NULL``. Silently binding an
    accepted species to a genus/family would make the just-created
    tag invisible to Life List, Compare, Explorer, and highlight /
    preference eligibility; leaving ``taxon_id`` NULL keeps the
    ``rank IS NULL`` branch honoring the tag until a genuine
    species-rank match becomes available.

    General-keyword auto-detect callers (``is_species=False`` INSERT
    path, rename auto-promotion) must NOT set ``species_only``: they
    legitimately link general/typed keywords to family/genus taxa
    (e.g. a hand-tagged ``Penduline tits`` linked to the family
    taxon), and ``is_keyword_species`` already filters those out via
    ``taxon_rank`` for species-specific readers.
    """
    from species_identity import COMMON_NAME_CORRECTIONS
    correction = COMMON_NAME_CORRECTIONS.get(keyword_match_key(name))
    if correction:
        target = conn.execute(
            "SELECT id FROM taxa WHERE inat_id = ? AND name = ? AND rank = 'species'",
            (correction["taxon_id"], correction["scientific_name"]),
        ).fetchone()
        return target["id"] if target else None
    for variant in variants(name):
        if prefer_species or species_only:
            taxon_id = species_preferring_taxon_match(
                conn, variant, species_only,
            )
            if taxon_id is not None:
                return taxon_id
        else:
            taxon = conn.execute(
                """SELECT t.id FROM taxa t
                   WHERE t.common_name = ? COLLATE NOCASE
                      OR t.name = ? COLLATE NOCASE
                   LIMIT 1""",
                (variant, variant),
            ).fetchone()
            if taxon:
                return taxon["id"]
            taxon = conn.execute(
                """SELECT t.taxon_id AS id FROM taxa_common_names t
                   WHERE t.name = ? COLLATE NOCASE
                   LIMIT 1""",
                (variant,),
            ).fetchone()
            if taxon:
                return taxon["id"]
    return None


def species_preferring_taxon_match(
    conn: sqlite3.Connection, variant: str, species_only: bool,
) -> int | None:
    """``lookup_taxon_id``'s species-preferring match for one variant."""
    direct = conn.execute(
        """SELECT t.id, t.rank FROM taxa t
                   WHERE t.common_name = ? COLLATE NOCASE
                      OR t.name = ? COLLATE NOCASE
                   ORDER BY (t.rank = 'species') DESC, t.id ASC
                   LIMIT 1""",
        (variant, variant),
    ).fetchone()
    if direct and direct["rank"] == "species":
        return direct["id"]
    # Direct match (if any) is not species-rank. Consult the
    # common-names index for a species-rank alternate before
    # returning the higher-rank direct hit.
    common = conn.execute(
        """SELECT tcn.taxon_id AS id, t.rank FROM taxa_common_names tcn
                   JOIN taxa t ON t.id = tcn.taxon_id
                   WHERE tcn.name = ? COLLATE NOCASE
                   ORDER BY (t.rank = 'species') DESC, t.id ASC
                   LIMIT 1""",
        (variant,),
    ).fetchone()
    if common and common["rank"] == "species":
        return common["id"]
    if species_only:
        # Reject the higher-rank fallback: an explicit
        # species add would stamp is_species=1 on this row
        # and every rank reader would then hide the tag.
        return None
    # No species-rank match on this variant; prefer the direct
    # (higher-rank) hit, otherwise fall back to the common-name
    # hit if one exists.
    if direct:
        return direct["id"]
    if common:
        return common["id"]
    return None

