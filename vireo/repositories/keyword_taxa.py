"""Persistence for keyword species identity: taxon links and species names.

This module owns the SQL that decides what species (or other taxon) a keyword
row stands for, and how a species name is spelled:

- species-name resolution: the casing convention the stored species rows use
  (``detect_case_convention``), the stored spelling ``add_keyword`` would land
  on (``resolve_species_display``) and the canonical root spelling for a
  taxon (``species_root_name_for_taxon``);
- keyword-to-taxon linking: the local ``taxa`` id a keyword name resolves to
  (``lookup_taxon_id``), the lineage check that disambiguates a species-rank
  homonym (``resolve_species_by_lineage``), the startup pass that marks and
  links species keywords (``mark_species``), and the relink after a taxonomy
  import (``relink_source_species``);
- species reads of keyword rows: ``is_species``, ``species_identity_rows``,
  the ``/api/species`` name search (``species_names_matching``), and the
  keyword-row reads ``/api/encounters/species`` runs to find the root rows
  standing for the species it replaces or removes (``species_root_by_name``,
  ``other_linked_species_names``).

Inserting a keyword (with its case convention and taxon link) is
``repositories/keywords.py``, which reaches these methods through the
façade (``Database.resolve_species_display_name``,
``Database._lookup_taxon_id_for_keyword`` ...); the species photos carry is
``repositories/photo_species.py``.

The method bodies moved verbatim from ``repositories/keywords.py`` (and
before that from ``Database``); only their signatures gained annotations.
``self._DETECT_CASE_CONVENTION`` and ``self._taxon_lookup_variants`` are the
``db`` module sentinel and helper of those names, injected by
``Database._keyword_taxon_repository``. Every façade method a moved body
calls is bound from the ``Database`` instance under its own name (see
``FACADE_METHODS``) and called as ``self.<name>(...)``, so monkeypatches of
``Database`` methods keep reaching the moved code. The SQL text, parameter
order and commit placement are unchanged. Nothing here writes a
``photo_keywords`` association.

Callers reach it through the ``db.keyword_taxa`` accessor, which builds a
fresh repository per access. Keywords and taxa are catalog-wide, so nothing
here needs the active workspace. The methods other keyword repositories bind
from the façade by name keep their ``Database`` wrappers
(``detect_keyword_case_convention``, ``resolve_species_display_name``,
``_species_root_name_for_taxon``, ``_lookup_taxon_id_for_keyword``,
``_resolve_species_by_lineage``).
"""

import sqlite3
from collections.abc import Callable, Iterable, Mapping
from typing import Any, Protocol

from keyword_normalization import keyword_match_key, normalize_keyword_display

# ``Database`` methods the moved bodies call through the façade.
FACADE_METHODS = (
    "species_case_convention",
    "_apply_case_convention",
    "_resolve_species_by_lineage",
)


class _Taxonomy(Protocol):
    """What ``mark_species`` needs of a ``taxonomy.Taxonomy``."""

    def lookup(self, name: str) -> dict[str, Any] | None: ...


class KeywordTaxonRepository:
    # Bound ``Database`` methods that ``__init__`` installs from
    # ``FACADE_METHODS``, declared so the type checker sees them.
    species_case_convention: Callable[..., Any]
    _apply_case_convention: Callable[..., Any]
    _resolve_species_by_lineage: Callable[..., Any]

    def __init__(
        self,
        conn: sqlite3.Connection,
        *,
        detect_case_convention_sentinel: object,
        taxon_lookup_variants: Callable[[str], list[str]],
        facade: object,
    ) -> None:
        self.conn = conn
        # ``db`` module names, kept under those names so the moved bodies
        # read them as ``self.<name>``.
        self._DETECT_CASE_CONVENTION = detect_case_convention_sentinel
        self._taxon_lookup_variants = taxon_lookup_variants
        # Bound ``Database`` methods, kept under their façade names.
        for name in FACADE_METHODS:
            setattr(self, name, getattr(facade, name))

    # -- species names ---------------------------------------------------------

    def detect_case_convention(self) -> str | None:
        """Detect the casing convention used by existing species keywords.

        Returns:
            'title' if most are Title Case (e.g. "Black Phoebe")
            'lower' if most are lowercase after first word (e.g. "Black phoebe")
            'upper' if most are ALL CAPS
            None if not enough data to determine
        """
        rows = self.conn.execute(
            "SELECT name FROM keywords WHERE is_species = 1"
        ).fetchall()
        if len(rows) < 3:
            return None

        title_count = 0
        lower_count = 0
        for r in rows:
            name = r["name"]
            words = name.split()
            if len(words) < 2:
                continue
            # Check the second word's casing
            second = words[1]
            if second[0].isupper():
                title_count += 1
            else:
                lower_count += 1

        if lower_count > title_count:
            return "lower"
        elif title_count > lower_count:
            return "title"
        return None


    def resolve_species_display(
        self, name: str, apply_case_convention: bool = True,
        *, case_convention: object,
    ) -> str:
        """Predict the stored species name that add_keyword(is_species=True) would use.

        Species relabel endpoints snapshot curation dst_existed before
        add_keyword actually runs, so they need to know the final stored
        spelling in advance. Cases:

        1. A single species-bearing root keyword row matches (SQLite
           ASCII NOCASE) → preserve that stored spelling; existing
           curation rows key on it. A non-species general homonym with
           the same NOCASE key (e.g. a hand-tagged ``Common Waxbill``
           general alongside a taxonomy ``Common waxbill``) is ignored
           here — add_keyword's typed lookup prefers the taxonomy row,
           so returning the general's spelling would key curation onto a
           string add_keyword never stores.
        2. Multiple species-bearing stored spellings match the same
           NOCASE key (intentional homonyms — e.g. legacy general
           ``Robin`` (is_species=1) alongside taxonomy ``robin``) →
           preserve the caller's spelling. Silently picking one would
           route bucket/curation writes across genuinely different
           species rows, so the eligibility check (which compares
           ``bucket["species"]`` to this result exactly) would then
           reject requests coming from the other homonym's bucket.
           Bucket collection, API parse, and DB setters all funnel
           through this call, so preserving keeps them agreeing on the
           same string. (Callers that need the exact spelling
           add_keyword will land on after promotion — e.g. relabel
           snapshots — apply add_keyword's ORDER BY themselves.)
        3. No species-bearing row but a non-species general row shares
           the NOCASE key → return that general's spelling.
           add_keyword(is_species=True) would find and promote that row
           in place, keeping its name, so curation must key on the same
           string.
        4. No matching root keyword row → apply the same species-casing
           convention that add_keyword applies for new species keywords,
           so pre-existing curation from predictions (which inserted
           `Black Phoebe`) is matched even when the request submits
           `black phoebe`.

        A hierarchy leaf whose spelling differs from its root alias is also
        canonicalized through a unique linked taxon. This keeps accepted
        hierarchy buckets and species curation on the same root key while the
        photo itself retains the hierarchy-bearing keyword association.

        Pass ``apply_case_convention=False`` to skip case 4 and return the
        caller's own spelling when nothing is stored. Callers that *display*
        the result want "the spelling some row actually holds, or the name I
        gave you" — case 4 mints a spelling (``Bubulcus ibis`` ->
        ``Bubulcus Ibis``) that no row anywhere carries, which is fine for
        predicting where add_keyword will land but wrong to show a user as
        though a model or the catalog had said it.
        """
        name = normalize_keyword_display(name)
        if not name:
            return name
        rows = self.conn.execute(
            "SELECT name, type, is_species FROM keywords "
            "WHERE name = ? COLLATE NOCASE AND parent_id IS NULL "
            "AND type IN ('taxonomy', 'general') "
            "ORDER BY (type = 'taxonomy') DESC, id ASC",
            (name,),
        ).fetchall()
        if rows:
            species_rows = [
                r for r in rows
                if r["is_species"] or r["type"] == "taxonomy"
            ]
            if len(species_rows) == 1:
                return species_rows[0]["name"]
            if len(species_rows) > 1:
                for r in species_rows:
                    if r["name"] == name:
                        return r["name"]
                return name
            return rows[0]["name"]
        linked_display = self._linked_taxon_display(name)
        if linked_display is not None:
            return linked_display
        if not apply_case_convention:
            return name
        convention = (
            self.species_case_convention()
            if case_convention is self._DETECT_CASE_CONVENTION
            else case_convention
        )
        if convention:
            return self._apply_case_convention(name, convention)
        return name


    def _linked_taxon_display(self, name: str) -> str | None:
        """The stored spelling for a hierarchy leaf linked to one taxon, if any."""
        linked_taxa = self.conn.execute(
            """SELECT DISTINCT taxon_id FROM keywords
               WHERE name = ? COLLATE NOCASE
                 AND parent_id IS NOT NULL
                 AND (is_species = 1 OR type = 'taxonomy')
                 AND taxon_id IS NOT NULL""",
            (name,),
        ).fetchall()
        if len(linked_taxa) == 1:
            root = self.conn.execute(
                """SELECT name FROM keywords
                   WHERE parent_id IS NULL
                     AND taxon_id = ?
                     AND (is_species = 1 OR type = 'taxonomy')
                   ORDER BY id LIMIT 1""",
                (linked_taxa[0]["taxon_id"],),
            ).fetchone()
            if root is not None:
                return root["name"]
            # No canonical root row exists for this linked taxon (for
            # example a hierarchy-only accept whose top-level ``Verdin``
            # never got created). Return the matched leaf's stored
            # spelling so callers that gate on exact ``k.name``
            # (highlight/preference/life-list eligibility for a
            # hierarchy-only tag) still see a bucket the photo actually
            # carries — the case-convention fallback below would mint a
            # different spelling (``black phoebe`` -> ``Black Phoebe``)
            # and the saved highlight would disappear on reload.
            leaf = self.conn.execute(
                """SELECT name FROM keywords
                   WHERE name = ? COLLATE NOCASE
                     AND parent_id IS NOT NULL
                     AND (is_species = 1 OR type = 'taxonomy')
                     AND taxon_id = ?
                   ORDER BY id LIMIT 1""",
                (name, linked_taxa[0]["taxon_id"]),
            ).fetchone()
            if leaf is not None and leaf["name"]:
                return leaf["name"]
        return None


    def species_root_name_for_taxon(self, taxon_id: int | None) -> str | None:
        """Canonical root keyword spelling for a species taxon, if any.

        ``resolve_species_display_name`` uses the same lookup to route
        curation keys through the canonical root when only a hierarchy
        alias is stored (e.g. a leaf ``Desert Verdin`` after
        the retired duplicate-species repair detached the top-level
        ``Verdin``). Callers with the taxon id in hand can skip the
        name-based lookup and go straight to the root row.
        """
        if taxon_id is None:
            return None
        row = self.conn.execute(
            """SELECT name FROM keywords
               WHERE parent_id IS NULL
                 AND taxon_id = ?
                 AND (is_species = 1 OR type = 'taxonomy')
               ORDER BY id LIMIT 1""",
            (taxon_id,),
        ).fetchone()
        if row is None:
            return None
        return row["name"] or None


    # -- keyword-to-taxon linking ----------------------------------------------

    def lookup_taxon_id(
        self, name: str, prefer_species: bool = False, species_only: bool = False,
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
            target = self.conn.execute(
                "SELECT id FROM taxa WHERE inat_id = ? AND name = ? AND rank = 'species'",
                (correction["taxon_id"], correction["scientific_name"]),
            ).fetchone()
            return target["id"] if target else None
        for variant in self._taxon_lookup_variants(name):
            if prefer_species or species_only:
                taxon_id = self._species_preferring_taxon_match(
                    variant, species_only,
                )
                if taxon_id is not None:
                    return taxon_id
            else:
                taxon = self.conn.execute(
                    """SELECT t.id FROM taxa t
                       WHERE t.common_name = ? COLLATE NOCASE
                          OR t.name = ? COLLATE NOCASE
                       LIMIT 1""",
                    (variant, variant),
                ).fetchone()
                if taxon:
                    return taxon["id"]
                taxon = self.conn.execute(
                    """SELECT t.taxon_id AS id FROM taxa_common_names t
                       WHERE t.name = ? COLLATE NOCASE
                       LIMIT 1""",
                    (variant,),
                ).fetchone()
                if taxon:
                    return taxon["id"]
        return None


    def _species_preferring_taxon_match(
        self, variant: str, species_only: bool,
    ) -> int | None:
        """``lookup_taxon_id``'s species-preferring match for one variant."""
        direct = self.conn.execute(
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
        common = self.conn.execute(
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


    def relink_source_species(self) -> None:
        """Refresh local foreign keys after importing source taxa; caller commits."""
        self.conn.execute(
            "UPDATE keywords SET taxon_id = (SELECT id FROM taxa WHERE inat_id = keywords.source_taxon_id) "
            "WHERE source_taxon_id IS NOT NULL AND EXISTS "
            "(SELECT 1 FROM taxa WHERE inat_id = keywords.source_taxon_id)"
        )


    def resolve_species_by_lineage(
        self, species_name: str, expected_ancestors: Iterable[str],
    ) -> sqlite3.Row | None:
        """Pick the species-rank taxon matching ``species_name`` and lineage.

        Every candidate, including a sole local row, must contain every name
        in ``expected_ancestors``. A taxonomy name index can retain only the
        wrong side of a homonym, so candidate count alone is not evidence that
        the local row belongs to the lookup's lineage. Missing lineage context,
        zero matches, or multiple matches all return ``None`` rather than
        permanently crediting the wrong Life List species/class.
        """
        candidates = self.conn.execute(
            "SELECT id, rank, parent_id FROM taxa "
            "WHERE name = ? AND rank = 'species'",
            (species_name,),
        ).fetchall()
        if not candidates:
            return None
        expected = set(expected_ancestors)
        if not expected:
            # With no lineage context, even a sole local row may be the wrong
            # side of a homonym whose intended row is absent from ``taxa``.
            return None
        matches = []
        for candidate in candidates:
            ancestors = set()
            cursor = candidate["parent_id"]
            # Depth cap mirrors ``species_curation.get_class_ancestors`` and guards
            # against pathological cyclic parent_id data.
            for _ in range(12):
                if cursor is None:
                    break
                row = self.conn.execute(
                    "SELECT name, parent_id FROM taxa WHERE id = ?",
                    (cursor,),
                ).fetchone()
                if row is None:
                    break
                ancestors.add(row["name"])
                cursor = row["parent_id"]
            if expected.issubset(ancestors):
                matches.append(candidate)
        if len(matches) == 1:
            return matches[0]
        return None


    def mark_species(self, taxonomy: _Taxonomy) -> int:
        """Mark keywords that are recognized species in the taxonomy.

        Retypes untyped and ``type='general'`` keywords whose names match a
        taxon lookup, and repairs incomplete ``type='taxonomy'`` rows. Explicit
        non-taxonomy types such as ``location``, ``genre``, and ``individual``
        are user intent and must be preserved even when their names are taxon
        homonyms (for example, the location "California" is also a plant
        genus).

        Matching rows get ``is_species=1``, ``type='taxonomy'``, and (if the
        local taxa table is populated and the lookup resolves to a
        species-rank taxon) a ``taxon_id`` link by iNaturalist id. Lookup
        results below species rank (for example Eastern Fox Squirrel or
        Red-eared Slider subspecies) link to their species ancestor: the Life
        List keeps the precise stored label while the Explorer credits the
        containing species once.
        ``taxon_id`` is left NULL when the row had no prior link and only
        a higher-rank (genus/family) match exists — binding an unlinked
        row to a non-species-rank taxon would auto-promote it in a way the
        classifier callers never asked for. A ``type='taxonomy'`` row
        whose ``taxon_id`` was bound by the old species-agnostic lookup
        to a non-species-rank taxon (genus/family) is rebound to the
        species-rank taxon whenever ``taxonomy.lookup`` resolves to one;
        when no species-rank replacement exists, the higher-rank link is
        preserved so ``species_curation.get_life_list_candidates`` can keep surfacing the
        row's ``taxon_rank`` / ``scientific_name`` / ``taxonomic_class``
        metadata for genus/family/class Life List filters.

        Uses the local taxonomy only (no network requests).

        Args:
            taxonomy: a Taxonomy instance with a lookup() method
        """
        # Also include already-typed taxonomy keywords whose taxon_id is
        # still NULL — those were created before the local taxa table was
        # populated (e.g. via add_keyword(..., is_species=True) from the
        # classifier), and still need their hierarchy link filled in. Also
        # revisit already-typed taxonomy keywords whose taxon_id points at a
        # non-species-rank taxon so we can rebind them to a species-rank
        # taxon when one exists. Deliberate non-taxonomy types are excluded
        # before lookup so a taxon homonym can never silently retype them.
        keywords = self.conn.execute(
            "SELECT k.id, k.name, k.type, k.taxon_id, k.is_species, "
            "       t.rank AS taxon_rank "
            "FROM keywords k "
            "LEFT JOIN taxa t ON t.id = k.taxon_id "
            "WHERE (k.type IS NULL OR k.type IN ('general', 'taxonomy')) "
            "  AND ("
            "    k.is_species = 0 OR k.type IS NULL OR k.type != 'taxonomy' "
            "    OR k.taxon_id IS NULL "
            "    OR (t.rank IS NOT NULL AND t.rank != 'species')"
            "  )"
        ).fetchall()
        updated = 0
        for kw in keywords:
            taxon = taxonomy.lookup(kw["name"])
            if not taxon:
                continue
            lookup_local_id, lookup_rank = self._local_species_taxon(taxon)
            if self._mark_species_row(kw, lookup_local_id, lookup_rank):
                updated += 1
        if updated:
            self.conn.commit()
        return updated


    def _local_species_taxon(
        self, taxon: Mapping[str, Any],
    ) -> tuple[int | None, str | None]:
        """``(local id, rank)`` of the taxa row a taxonomy lookup resolves to.

        An infraspecific result resolves to its species ancestor.
        """
        lookup_local_id = None
        lookup_rank = None
        inat_id = taxon.get("taxon_id")
        if inat_id is not None:
            row = self.conn.execute(
                "SELECT id, rank FROM taxa WHERE inat_id = ?", (inat_id,)
            ).fetchone()
            if row:
                lookup_local_id = row["id"]
                lookup_rank = row["rank"]
        # The structured taxa table intentionally keeps major ranks only
        # (kingdom through species), while taxonomy.json can resolve a
        # label to a subspecies or another infraspecific rank. Walk that
        # result's scientific lineage back to its species ancestor rather
        # than leaving a valid, more-precise identification unlinked and
        # absent from Explorer completeness. Higher-rank lookups have no
        # species member in their lineage and therefore remain unchanged.
        if lookup_rank != "species":
            lineage_names = taxon.get("lineage_names") or []
            lineage_ranks = taxon.get("lineage_ranks") or []
            species_index = next(
                (
                    i for i, rank in enumerate(lineage_ranks)
                    if rank == "species"
                ),
                None,
            )
            if species_index is not None:
                species_name = lineage_names[species_index]
                # Ancestor scientific names above the species rank, used
                # to disambiguate homonyms. Two taxa with the same
                # species-rank scientific name can legitimately coexist
                # in the local ``taxa`` table (see the taxonomy loader
                # around ``_build_lineage`` — the schema keys on
                # ``inat_id``, not on ``(name, rank)`` — and iNat itself
                # allows homonyms across kingdoms). A name-only
                # ``LIMIT 1`` binding an infraspecific label to an
                # arbitrary row could permanently credit the wrong Life
                # List species/class; verify each candidate's parent
                # chain matches the taxonomy lookup's lineage instead.
                expected_ancestors = [
                    name for name, rank in zip(
                        lineage_names[:species_index],
                        lineage_ranks[:species_index],
                        strict=False,
                    )
                    if rank in (
                        "genus", "family", "order",
                        "class", "phylum", "kingdom",
                    )
                ]
                species_row = self._resolve_species_by_lineage(
                    species_name, expected_ancestors
                )
                if species_row:
                    lookup_local_id = species_row["id"]
                    lookup_rank = species_row["rank"]
        return lookup_local_id, lookup_rank


    def _mark_species_row(
        self, kw: sqlite3.Row, lookup_local_id: int | None, lookup_rank: str | None,
    ) -> bool:
        """Retype or relink one ``mark_species`` row; False when a no-op."""
        local_taxon_id = kw["taxon_id"]
        if local_taxon_id is None and lookup_rank == "species":
            local_taxon_id = lookup_local_id
        # Only bind ``taxon_id`` to a species-rank local taxon. Binding a
        # species-marked keyword to a genus/family id would let the new
        # rank filters (Life List, Compare, highlight/preference
        # eligibility) silently drop every photo carrying that keyword,
        # because those readers require ``t.rank = 'species' OR
        # t.rank IS NULL``. Leaving ``taxon_id`` NULL for non-species
        # matches mirrors ``add_keyword``'s species-add path (see
        # ``test_add_species_leaves_taxon_null_when_only_higher_rank_matches``)
        # so upgraded catalogs stay visible under the ``rank IS NULL``
        # branch until a species-rank taxon becomes available.
        #
        # Rebind an existing higher-rank link when the taxonomy lookup
        # resolves to a species-rank local taxon. Without this pass,
        # mark_species_keywords skipped fully typed rows and legacy
        # keywords bound by the old species-agnostic lookup to a
        # genus/family stayed bound; the new ``t.rank = 'species'``
        # filter (Life List, Compare) then silently dropped every
        # photo carrying those keywords after upgrade.
        rebind_taxon_id = None
        if (
            kw["taxon_id"] is not None
            and kw["taxon_rank"] is not None
            and kw["taxon_rank"] != "species"
            and lookup_local_id is not None
            and lookup_rank == "species"
            and lookup_local_id != kw["taxon_id"]
        ):
            rebind_taxon_id = lookup_local_id
        # Preserve an existing higher-rank ``taxon_id`` when no
        # species-rank replacement is available. Earlier revisions
        # cleared the link to keep the row visible under the old
        # ``t.rank = 'species' OR t.rank IS NULL`` filters used by
        # Life List, Compare, and highlight/preference eligibility.
        # Those readers now accept linked higher-rank identifications
        # (see :meth:`species_curation.get_life_list_candidates` and
        # :meth:`species_curation.get_life_list_locations`), so clearing on startup
        # would strip the row's ``taxon_rank`` / ``scientific_name`` /
        # ``taxonomic_class`` metadata and silently break the new
        # genus / family / class Life List filters after the first
        # restart.
        # Skip no-op updates so the "updated" count reflects real
        # changes. A matched row is fully consistent when type is
        # 'taxonomy', is_species is 1, and (taxon_id is already set to
        # a species-rank id OR we have no local id to link it to).
        is_type_change = kw["type"] != "taxonomy"
        is_species_fix = kw["is_species"] != 1
        is_taxon_link = kw["taxon_id"] is None and local_taxon_id is not None
        is_rebind = rebind_taxon_id is not None
        if not (
            is_type_change
            or is_species_fix
            or is_taxon_link
            or is_rebind
        ):
            return False
        if is_rebind:
            self.conn.execute(
                "UPDATE keywords SET is_species = 1, type = 'taxonomy', "
                "taxon_id = ? WHERE id = ?",
                (rebind_taxon_id, kw["id"]),
            )
        else:
            self.conn.execute(
                "UPDATE keywords SET is_species = 1, type = 'taxonomy', "
                "taxon_id = COALESCE(taxon_id, ?) WHERE id = ?",
                (local_taxon_id, kw["id"]),
            )
        return True


    # -- species reads of keyword rows -----------------------------------------

    def is_species(self, keyword_id: int) -> bool:
        """Return True if the keyword represents a species-rank taxon.

        Preserve the legacy flag/type fallback when no taxonomy row can be
        resolved, but do not call a linked family/genus keyword a species.
        """
        row = self.conn.execute(
            """SELECT k.is_species, k.type, t.rank AS taxon_rank
               FROM keywords k
               LEFT JOIN taxa t ON t.id = k.taxon_id
               WHERE k.id = ?""",
            (keyword_id,),
        ).fetchone()
        if not row or not (row["is_species"] or row["type"] == "taxonomy"):
            return False
        return row["taxon_rank"] in (None, "species")


    def species_identity_rows(self) -> list[sqlite3.Row]:
        """Every species or taxonomy keyword with what its identity resolves from.

        Rows carry ``id``, ``name``, ``source_id`` (the keyword's
        ``source_taxon_id``, else its linked taxon's ``inat_id``) and
        ``scientific_name`` (the linked taxon's name, None when unlinked), in
        no particular order. Keywords are global, so this is catalog-wide.
        """
        return self.conn.execute(
            "SELECT k.id, k.name, COALESCE(k.source_taxon_id, t.inat_id) AS source_id, "
            "t.name AS scientific_name FROM keywords k LEFT JOIN taxa t ON t.id = k.taxon_id "
            "WHERE k.is_species = 1 OR k.type = 'taxonomy'"
        ).fetchall()


    def species_names_matching(
        self, query: str, match_case: bool, whole_word: bool,
    ) -> list[str]:
        """Names of ``is_species`` keywords that match ``query`` under the
        keyword text-match rules, in table order."""
        return [
            row["name"] for row in self.conn.execute(
                """SELECT name FROM keywords
                   WHERE is_species = 1
                     AND vireo_keyword_text_match(name, ?, ?, ?)""",
                (query, 1 if match_case else 0, 1 if whole_word else 0),
            ).fetchall()
        ]


    # -- species-confirmation reads ------------------------------------------
    #
    # The keyword-row reads ``/api/encounters/species`` (``web/encounters.py``)
    # runs inside its transaction to decide which attached rows stand for the
    # species a confirmation replaces or removes (the per-photo reads are
    # ``repositories/photo_species.py``). A "species row" is ``is_species = 1
    # OR type = 'taxonomy'``.

    def species_root_by_name(
        self, name: str, *, prefer_taxonomy: bool = False,
    ) -> sqlite3.Row | None:
        """One root species row (``id``, ``name``, ``taxon_id``) named ``name``, or None.

        ``name`` matches ``COLLATE NOCASE`` among ``parent_id IS NULL`` rows.
        ``prefer_taxonomy`` orders taxonomy rows first, then by id; without it
        the statement has no ``ORDER BY``.
        """
        if prefer_taxonomy:
            return self.conn.execute(
                """SELECT id, name, taxon_id FROM keywords
                   WHERE name = ? COLLATE NOCASE
                     AND parent_id IS NULL
                     AND (is_species = 1 OR type = 'taxonomy')
                   ORDER BY (type = 'taxonomy') DESC, id""",
                (name,),
            ).fetchone()
        return self.conn.execute(
            """SELECT id, name, taxon_id FROM keywords
               WHERE name = ? COLLATE NOCASE
                 AND parent_id IS NULL
                 AND (is_species = 1 OR type = 'taxonomy')""",
            (name,),
        ).fetchone()


    def other_linked_species_names(
        self, taxon_id: int | None, keyword_id: int,
    ) -> list[str]:
        """Names of the taxon-linked species rows that are a different species.

        With ``taxon_id`` set, rows linked to any other taxon; otherwise
        (an unlinked keyword) every linked row except ``keyword_id``.
        """
        if taxon_id is not None:
            rows = self.conn.execute(
                """SELECT name FROM keywords
                   WHERE (is_species = 1 OR type = 'taxonomy')
                     AND taxon_id IS NOT NULL
                     AND taxon_id != ?""",
                (taxon_id,),
            ).fetchall()
        else:
            rows = self.conn.execute(
                """SELECT name FROM keywords
                   WHERE (is_species = 1 OR type = 'taxonomy')
                     AND taxon_id IS NOT NULL
                     AND id != ?""",
                (keyword_id,),
            ).fetchall()
        return [row["name"] for row in rows]
