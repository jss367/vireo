"""Persistence for keyword rows: inserting them, and reading one row.

This module owns the SQL that creates keyword rows and the plain row reads:

- ``add``: insert a keyword, or return the row it dedupes onto (case- and
  type-aware lookup, the placed-species check, the case convention for new
  species, taxon linking and promotion, the insert race), its source-taxon
  species path ``add_source_species``, and ``top_level_species_keyword``,
  which answers what ``add(name, is_species=True)`` would match;
- the default genre seed (``ensure_default_genres``);
- the dashboard's keyword counts (``count``, ``count_in_workspace``);
- one-row reads: ``name_of``, ``get_row``, ``import_tag_row``.

The rest of the keyword domain lives beside it, each behind its own
``Database`` accessor:

- ``repositories/keyword_tree.py`` (``db.keyword_tree``): the hierarchy
  reads, renames, retypes, duplicate merges and deletes;
- ``repositories/keyword_taxa.py`` (``db.keyword_taxa``): species names and
  keyword-to-taxon linking;
- ``repositories/photo_keywords.py`` (``db.photo_keywords``): which keywords a
  photo carries, and which photos carry a keyword;
- ``repositories/photo_species.py`` (``db.photo_species``): which species a
  photo carries;
- ``repositories/keyword_merges.py`` (``db.keyword_merges``): the statements
  behind ``keyword_identity``'s import aliases and explicit merges.

The repository depends only on its connection and the workspace resolver.
``add`` composes with the species-name and taxon lookups of a
``KeywordTaxonRepository`` and the import-alias lookups of a
``KeywordMergeRepository``, both built on the same connection, and hands
``SpeciesResolver`` a ``TaxonCatalog`` (the ``db_meta`` and ``taxa``
repositories) rather than the ``Database``. The active workspace is resolved
lazily through the resolver ``Database._keyword_repository`` passes.

What lives in ``repositories/keyword_provenance.py`` instead: the
``photo_keywords`` writers that create or converge an association
(``tag_photo``, ``_merge_keyword_into``, ``link_keyword_to_place``), which
``test_keyword_provenance_contract`` keys to that module, and the keyword
methods that call ``_merge_keyword_into`` in the middle of their own work
(``upsert_one_keyword``). This module writes no ``photo_keywords``
association itself; a structural test fails if it ever references one of
those writers.

No method here commits: the route, job or service that adds a keyword owns
the unit of work (``db.transaction()``).
"""

import sqlite3
from collections.abc import Callable
from typing import Any, cast

from keyword_normalization import normalize_keyword_display
from repositories.keyword_identity_sql import identity_sql, resolve_import_alias
from repositories.keyword_merges import KeywordMergeRepository
from repositories.keyword_taxa import KeywordTaxonRepository, apply_species_case_convention
from repositories.meta import MetaRepository
from repositories.taxa import TaxaRepository

# Canonical set of keyword type values stored in keywords.type.
# - taxonomy: a species/genus/etc. (linked to taxa via taxon_id)
# - individual: a named person, pet, or otherwise tracked individual
# - location: a named location ("Yosemite", "backyard")
# - genre: a non-subject visual category ("Landscape", "Sunset")
# - general: catch-all/free-form tag (the legacy default)
KEYWORD_TYPES = frozenset({"taxonomy", "individual", "location", "genre", "general"})


class TaxonCatalog:
    """The ``db_meta`` and ``taxa`` repositories ``SpeciesResolver`` reads.

    ``SpeciesResolver(db=...)`` only reads ``.meta`` and ``.taxa``; this
    hands it those on one connection instead of the ``Database``.
    """

    def __init__(self, conn: sqlite3.Connection) -> None:
        self.meta = MetaRepository(conn)
        self.taxa = TaxaRepository(conn)


class KeywordRepository:
    def __init__(
        self,
        conn: sqlite3.Connection,
        resolve_workspace_id: Callable[[], int],
    ) -> None:
        self.conn = conn
        self._resolve_workspace_id = resolve_workspace_id
        self._taxa = KeywordTaxonRepository(conn)

    @property
    def workspace_id(self):
        """The active workspace id, resolved at each read (raises if none)."""
        return self._resolve_workspace_id()

    def ensure_default_genres(self):
        """Insert the default genre keywords if none exist of type='genre'.

        Idempotent: a single existing genre keyword (user-created or otherwise)
        short-circuits the insert. Keywords are global, so this runs once per
        database (not per workspace).

        Upgrade path: if a same-name top-level keyword exists with type='general'
        (legacy free-form tag with the same name as a default genre), promote
        it to type='genre' rather than silently leaving it as 'general'. Other
        explicit user types (individual, location) are preserved — the user
        meant something specific.
        """
        defaults = ("Landscape", "Sunset", "Architecture", "Abstract")
        # Warm-path short-circuit: if any genre row already exists, the
        # database has already been seeded — nothing to do. Cheap (single
        # SELECT 1 LIMIT 1).
        existing = self.conn.execute(
            "SELECT 1 FROM keywords WHERE type = 'genre' LIMIT 1"
        ).fetchone()
        if existing:
            return
        # Cold / upgrade path: promote any same-name top-level 'general'
        # rows to 'genre' first, so an upgraded DB with a hand-tagged default
        # name ends up with a canonical genre row.
        for name in defaults:
            self.conn.execute(
                """UPDATE keywords SET type = 'genre'
                   WHERE name = ? COLLATE NOCASE
                     AND parent_id IS NULL AND type = 'general'""",
                (name,),
            )
        # Always guarantee a canonical genre row for each default. Skip
        # only when a same-name + same-type ('genre') row already exists.
        # If a user has previously tagged e.g. 'Landscape' as 'location'
        # (a deliberate non-default type), we still create the genre
        # 'Landscape' alongside it.
        # This intentionally permits duplicates BY NAME across different
        # types — disambiguation is handled by add_keyword's lookup,
        # which prefers same-typed matches when kw_type is supplied.
        for name in defaults:
            existing_genre = self.conn.execute(
                """SELECT id FROM keywords
                   WHERE name = ? COLLATE NOCASE
                     AND parent_id IS NULL AND type = 'genre'
                   LIMIT 1""",
                (name,),
            ).fetchone()
            if existing_genre:
                continue
            self.conn.execute(
                "INSERT INTO keywords (name, type, is_species) VALUES (?, 'genre', 0)",
                (name,),
            )

    def count(self):
        """Return count of keywords used by photos in the active workspace.

        Filters out keywords whose only photos sit in folders flagged
        ``'missing'``. For the dashboard's headline (which must agree with
        the unfiltered top_keywords chart in ``get_dashboard_stats``), use
        ``count_keywords_in_workspace`` instead.
        """
        return self.conn.execute(
            """SELECT COUNT(DISTINCT pk.keyword_id)
               FROM photo_keywords pk
               JOIN photos p ON p.id = pk.photo_id
               JOIN photo_workspace_visibility wf ON wf.photo_id = p.id
               JOIN folders f ON f.id = p.folder_id AND f.status IN ('ok', 'partial')
               WHERE wf.workspace_id = ?""",
            (self.workspace_id,),
        ).fetchone()[0]

    def count_in_workspace(self):
        """Return count of keywords used by photos in the active workspace,
        including photos in folders flagged ``'missing'``.

        Pairs with the unfiltered ``top_keywords`` query in
        ``get_dashboard_stats`` so the dashboard's Keywords headline can't
        disagree with the Top Species / Other Keywords charts when a drive
        is unmounted (e.g. headline says 0 while charts list keywords).
        """
        return self.conn.execute(
            f"""SELECT COUNT(DISTINCT ({identity_sql()}))
               FROM photo_keywords pk
               JOIN keywords k ON k.id = pk.keyword_id
               JOIN photos p ON p.id = pk.photo_id
               JOIN photo_workspace_visibility wf ON wf.photo_id = p.id
               WHERE wf.workspace_id = ?""",
            (self.workspace_id,),
        ).fetchone()[0]

    def add_source_species(
        self, name: str, source_taxon_id: int, parent_id: int | None = None,
    ) -> int:
        """Bind accepted source evidence without reassigning same-name tags."""
        if type(source_taxon_id) is not int or not 0 < source_taxon_id < (1 << 63):
            raise ValueError("source_taxon_id must be a positive SQLite integer")
        taxon = self.conn.execute("SELECT id FROM taxa WHERE inat_id = ?", (source_taxon_id,)).fetchone()
        local_id = taxon["id"] if taxon else None
        # Imported catalogs can have several keyword spellings linked to one
        # taxon. Prefer the requested name among those matches, just as the
        # legacy name-only accept does. This keeps the tagged name aligned
        # with the prediction when possible, rather than choosing an older
        # alias (e.g. "Red-eared slider" instead of "Pond slider").
        # The identity predicate still excludes same-name, different taxa.
        existing = self.conn.execute(
            "SELECT id FROM keywords WHERE parent_id IS ? AND type IN ('taxonomy', 'general') "
            "AND (source_taxon_id = ? OR (source_taxon_id IS NULL AND taxon_id = ?)) "
            "ORDER BY (name = ? COLLATE NOCASE) DESC, (type = 'taxonomy') DESC, id LIMIT 1",
            (parent_id, source_taxon_id, local_id, name),
        ).fetchone()
        if existing is None and parent_id is None:
            # An accept names no parent, but the taxon may already be filed
            # in the user's tree; bind to that row rather than mint a
            # top-level twin (see ``_find_add_candidate``).
            placed = self.conn.execute(
                "SELECT id, (name = ? COLLATE NOCASE) AS name_match FROM keywords "
                "WHERE parent_id IS NOT NULL AND type = 'taxonomy' "
                "AND (source_taxon_id = ? OR (source_taxon_id IS NULL AND taxon_id = ?)) "
                "ORDER BY name_match DESC, id LIMIT 2",
                (name, source_taxon_id, local_id),
            ).fetchall()
            if len(placed) == 1 or (placed and placed[0]["name_match"] and not placed[1]["name_match"]):
                existing = placed[0]
        if existing:
            kid = existing["id"]
            self.conn.execute(
                "UPDATE keywords SET source_taxon_id = ?, taxon_id = ?, is_species = 1, type = 'taxonomy' WHERE id = ?",
                (source_taxon_id, local_id, kid),
            )
        else:
            from species_identity import SpeciesResolver
            raw_name = name.removesuffix(f" (taxon {source_taxon_id})")
            identity = SpeciesResolver(db=TaxonCatalog(self.conn)).resolve(raw_name, source={"taxon_id": source_taxon_id})
            display = self._taxa.resolve_species_display(identity.display_name)
            candidate = display
            suffix = 0
            while self.conn.execute(
                "SELECT 1 FROM keywords WHERE name = ? COLLATE NOCASE AND parent_id IS ? LIMIT 1",
                (candidate, parent_id),
            ).fetchone():
                suffix += 1
                candidate = f"{display} (taxon {source_taxon_id})" + (f" ({suffix})" if suffix > 1 else "")
            kid = self.conn.execute(
                "INSERT INTO keywords (name, parent_id, is_species, type, taxon_id, source_taxon_id) "
                "VALUES (?, ?, 1, 'taxonomy', ?, ?)",
                (candidate, parent_id, local_id, source_taxon_id),
            ).lastrowid
        return cast(int, kid)

    def add(
        self,
        name: str,
        parent_id: int | None = None,
        is_species: bool = False,
        kw_type: str | None = None,
        source_taxon_id: int | None = None,
        _resolve_alias: bool = False,
    ) -> int:
        """Insert a keyword. Returns existing id if duplicate (case-insensitive).

        If a keyword with the same name but different casing exists, reuses
        the existing one rather than creating a duplicate.

        For new species keywords, auto-detects the user's casing convention
        from existing keywords and applies it (unless overridden by config).

        Args:
            kw_type: Optional explicit keyword type. Must be one of
                     ``KEYWORD_TYPES`` if provided. When ``None``, the type is
                     auto-detected (``taxonomy`` for species or names matching
                     a known taxon, otherwise ``general``).
            source_taxon_id: Explicit iNaturalist ID for a species keyword;
                     bypass common-name inference and reuse only that identity.
            _resolve_alias: Import callers opt in for leaf keywords only.
                     Manual additions must not inherit imported keyword aliases,
                     and a leaf alias must not relocate a new parent chain.
        """
        if kw_type is not None and kw_type not in KEYWORD_TYPES:
            raise ValueError(f"invalid keyword type: {kw_type!r}")
        # Normalization choke point: every keywords.name write funnels
        # through here (or update_keyword / _upsert_one_keyword), and the
        # v5 migration normalized all pre-existing rows, so stored names
        # are always in normalize_keyword_display() form and the plain
        # COLLATE NOCASE dedupe below is sufficient.
        name = normalize_keyword_display(name)
        # Reject names that normalize to empty. Input like `"'"` is
        # non-empty before normalization (so the API boundary's `if not
        # name` guard passes), but the strip turns it into `""`. Without
        # this check, we would insert an invisible keyword row that could
        # still be tagged, synced to XMP, and reported in duplicate cleanup.
        if not name:
            raise ValueError("keyword name is empty after normalization")
        if _resolve_alias and not is_species and source_taxon_id is None and kw_type in (None, 'location'):
            resolved = resolve_import_alias(
                KeywordMergeRepository(self.conn), name, parent_id, kw_type=kw_type,
            )
            if resolved is not None:
                return resolved
        is_species, kw_type = self._reconcile_species_type(is_species, kw_type)
        if source_taxon_id is not None:
            if not is_species:
                raise ValueError("source_taxon_id requires a species keyword")
            return self.add_source_species(name, source_taxon_id, parent_id)
        # Case-insensitive lookup with type-aware matching:
        #
        # When kw_type is supplied, only same-type or 'general' rows are
        # candidates. Same-type wins; 'general' is promotable to the
        # requested type via the UPDATE below. Other deliberate types
        # (location/individual/etc.) are intentionally NOT candidates —
        # returning one and finding the upgrade no-op'd would leave the
        # caller with a mismatched type. Falling through to INSERT
        # creates a new row of the requested type alongside the
        # deliberate one (duplicates by name across types are
        # intentional in this PR).
        #
        # When kw_type is None, prefer the most "structured"
        # interpretation in a fixed priority — taxonomy > genre >
        # individual > location > general — so a type-agnostic caller
        # (e.g. typing into a generic keyword input) doesn't silently
        # bind to a hand-tagged 'general' duplicate when a canonical
        # typed row exists. Tie-break by id for determinism.
        existing = self._find_add_candidate(name, parent_id, kw_type)
        # The lookup key, before the insert path below rewrites ``name``
        # (case convention) and ``kw_type`` (auto-detection).
        lookup_name, lookup_kw_type, lookup_is_species = name, kw_type, is_species
        if existing:
            self._backfill_candidate_taxon(existing, name, is_species, kw_type)
            self._promote_candidate_type(existing, is_species, kw_type)
            return existing["id"]

        name, kw_type, taxon_id = self._new_keyword_name_and_type(
            name, is_species, kw_type,
        )
        return self._insert_new_keyword(
            name, parent_id, is_species, kw_type, taxon_id, lookup_name,
            lookup_kw_type, lookup_is_species,
        )

    @staticmethod
    def _reconcile_species_type(is_species, kw_type):
        """``add``'s ``(is_species, kw_type)`` after reconciling the two."""
        # Reconcile is_species and kw_type to keep the legacy column coherent
        # with the type enum.
        if is_species and kw_type is not None and kw_type != 'taxonomy':
            raise ValueError(
                f"is_species=True requires kw_type='taxonomy', got {kw_type!r}"
            )
        if kw_type == 'taxonomy':
            is_species = True
        # Symmetric reconciliation: callers like the prediction-accept and
        # pipeline-apply flows pass is_species=True with no kw_type. Treat
        # that as a typed taxonomy lookup so the candidate-filtering below
        # correctly excludes a same-name 'individual'/'location'/'genre'
        # row (e.g. a person tag named "Robin"). Without this, the
        # untyped lookup would return the homonym row, the typed promotion
        # would no-op (only 'general'/'taxonomy' get promoted), and the
        # caller would get back a non-taxonomy id — silently mis-tagging
        # the accepted species.
        if is_species and kw_type is None:
            kw_type = 'taxonomy'
        return is_species, kw_type

    def _backfill_candidate_taxon(self, existing, name, is_species, kw_type):
        """Link the row ``add`` reuses to its species-rank taxon."""
        # Older confirmed-species rows can be correctly typed yet have a
        # NULL taxon_id because the historic is_species=True insert path
        # skipped taxonomy lookup. Backfill opportunistically whenever
        # such a row is resolved so future identity checks use taxon_id.
        # When the preferred lookup returns a species-rank taxon and the
        # row is already bound to a non-species-rank homonym (e.g. an
        # older catalog stamped ``Puma`` with the genus taxon before
        # ``prefer_species`` existed), overwrite that binding. Without
        # this rebind the row stays flagged is_species/taxonomy while
        # its taxon_id points at a genus/family, so every downstream
        # ``t.rank = 'species'`` filter (Life List, Compare, Explorer)
        # silently drops photos carrying the accepted keyword.
        if is_species or kw_type == 'taxonomy':
            # species_only: this branch stamps the row is_species=1
            # via the taxonomy promotion below, so binding to a
            # higher-rank homonym would silently hide the accepted
            # keyword behind every downstream rank='species' filter.
            taxon_id = self._taxa.lookup_taxon_id(
                name, species_only=True,
            )
            if taxon_id:
                self.conn.execute(
                    "UPDATE keywords SET taxon_id = ? "
                    "WHERE id = ? AND type IN ('general', 'taxonomy') "
                    "AND ("
                    "  taxon_id IS NULL "
                    "  OR ("
                    "    (SELECT rank FROM taxa WHERE id = ?) = 'species' "
                    "    AND COALESCE("
                    "      (SELECT rank FROM taxa WHERE id = keywords.taxon_id), ''"
                    "    ) != 'species'"
                    "  )"
                    ")",
                    (taxon_id, existing["id"], taxon_id),
                )
            # Preserve an existing higher-rank ``taxon_id`` when no
            # species-rank replacement is available. Earlier revisions
            # cleared the link so the row would fall under the old
            # ``t.rank = 'species' OR t.rank IS NULL`` filters used by
            # Life List, Compare, and highlight/preference eligibility.
            # Those readers now accept linked higher-rank identifications
            # (see :meth:`species_curation.get_life_list_candidates` and
            # :meth:`species_curation.get_life_list_locations`), so clearing here would
            # strip the row's ``taxon_rank`` / ``scientific_name`` /
            # ``taxonomic_class`` metadata and silently break the new
            # genus / family / class Life List filters — mirroring the
            # startup preservation in :meth:`mark_species_keywords`.
        if kw_type is None and not is_species and existing["type"] == "general":
            taxon_id = self._taxa.lookup_taxon_id(
                name, prefer_species=True,
            )
            if taxon_id:
                self.conn.execute(
                    "UPDATE keywords SET is_species = 1, type = 'taxonomy', "
                    "taxon_id = CASE "
                    "  WHEN taxon_id IS NULL THEN ? "
                    "  WHEN (SELECT rank FROM taxa WHERE id = ?) = 'species' "
                    "    AND COALESCE("
                    "      (SELECT rank FROM taxa WHERE id = keywords.taxon_id), ''"
                    "    ) != 'species' THEN ? "
                    "  ELSE taxon_id END "
                    "WHERE id = ? AND type = 'general'",
                    (taxon_id, taxon_id, taxon_id, existing["id"]),
                )

    def _promote_candidate_type(self, existing, is_species, kw_type):
        """Upgrade the row ``add`` reuses to the type this call asks for."""
        # Promote an unset row to taxonomy when this call indicates a
        # species. Restrict to 'general' (the legacy default for unknown
        # rows) so a deliberate user type — 'individual', 'location',
        # 'genre' — is preserved instead of silently rewritten when a
        # later caller passes is_species=True or kw_type='taxonomy'.
        if is_species:
            self.conn.execute(
                "UPDATE keywords SET is_species = 1, type = 'taxonomy' "
                "WHERE id = ? AND is_species = 0 AND type IN ('general', 'taxonomy')",
                (existing["id"],),
            )
        # Upgrade an existing 'general' row to the explicitly requested type.
        # Without this, explicitly typed callers would hit the
        # case-insensitive fast path and silently get back a wrong-typed row.
        if kw_type and kw_type != 'general':
            self.conn.execute(
                "UPDATE keywords SET type = ? WHERE id = ? AND type = 'general'",
                (kw_type, existing["id"]),
            )
            if kw_type == 'taxonomy':
                # Gate on type='taxonomy' so a preserved deliberate type
                # (e.g. 'individual') doesn't get is_species=1 stamped on
                # it when the type update above was a no-op. Otherwise
                # Subject filters with `OR is_species=1` would otherwise
                # treat that non-taxonomy row as a species.
                self.conn.execute(
                    "UPDATE keywords SET is_species = 1 "
                    "WHERE id = ? AND type = 'taxonomy'",
                    (existing["id"],),
                )

    def _new_keyword_name_and_type(self, name, is_species, kw_type):
        """``(name, kw_type, taxon_id)`` for a keyword ``add`` will insert."""
        # Apply casing convention for new species keywords
        if is_species:
            # The configured override, else the convention detected from the
            # existing species keywords.
            convention = self._taxa.species_case_convention()
            if convention:
                name = apply_species_case_convention(name, convention)

        # Explicit species/taxonomy inserts still need their taxonomy link.
        # The earlier kw_type reconciliation turns is_species=True into
        # kw_type='taxonomy'; limiting lookup to kw_type is None therefore
        # created new confirmed-species rows with taxon_id=NULL, defeating
        # taxon-aware dedupe against hierarchical XMP leaves. Require a
        # species-rank taxon here (species_only=True) — the INSERT below
        # stamps is_species=1 as soon as any taxon is found, and
        # downstream rank filters would drop the just-linked keyword if
        # we bound it to a genus/family homonym. Leaving ``taxon_id``
        # NULL when no species-rank match exists keeps the tag visible
        # to readers via the ``t.rank IS NULL`` branch.
        taxon_id = (
            self._taxa.lookup_taxon_id(name, species_only=True)
            if kw_type == 'taxonomy'
            else None
        )
        if kw_type is None:
            # Auto-detect taxonomy type from taxa table
            kw_type = 'general'
            if is_species:
                kw_type = 'taxonomy'
            else:
                taxon_id = self._taxa.lookup_taxon_id(
                    name, prefer_species=True,
                )
                if taxon_id:
                    kw_type = 'taxonomy'
        return name, kw_type, taxon_id

    def _insert_new_keyword(
        self, name, parent_id, is_species, kw_type, taxon_id, lookup_name,
        lookup_kw_type, lookup_is_species,
    ):
        """Insert the keyword ``add`` found no row for, racing safely."""
        # The lookup above ran without a write lock, so a second connection
        # can pass the same check before either inserts. UNIQUE(name,
        # parent_id) cannot catch that for a top-level keyword (SQLite treats
        # NULL parents as distinct) and is case-sensitive besides. When the
        # caller's unit has not written yet, take the writer lock with a
        # no-op write (it begins the unit's transaction) and look again, so
        # the loser of the race reuses the winner's row. Nothing here
        # commits: the caller's unit of work owns the transaction.
        if not self.conn.in_transaction:
            self.conn.execute("UPDATE keywords SET id = id WHERE 0")
            if self._find_add_candidate(lookup_name, parent_id, lookup_kw_type) is not None:
                # Rerun so the reused row gets the same promotions as any
                # other hit on the lookup.
                return self.add(
                    lookup_name, parent_id, is_species=lookup_is_species,
                    kw_type=lookup_kw_type,
                )
        try:
            cur = self.conn.execute(
                "INSERT INTO keywords (name, parent_id, is_species, type, taxon_id) VALUES (?, ?, ?, ?, ?)",
                (name, parent_id, 1 if is_species else (1 if taxon_id else 0), kw_type, taxon_id),
            )
        except sqlite3.IntegrityError:
            # A same-spelling row under this parent committed after the
            # lookup. SQLite undid only the failed statement; reuse the row
            # when it is one ``add`` would have returned.
            if self._find_add_candidate(lookup_name, parent_id, lookup_kw_type) is None:
                raise
            return self.add(
                lookup_name, parent_id, is_species=lookup_is_species,
                kw_type=lookup_kw_type,
            )
        return cast(int, cur.lastrowid)

    def _find_add_candidate(self, name, parent_id, kw_type):
        """The row ``add`` reuses for ``name`` under ``parent_id``, or None."""
        # NB: SQL literals here are constants, not parameter bindings.
        type_priority_case = (
            "CASE type "
            "WHEN 'taxonomy' THEN 0 "
            "WHEN 'genre' THEN 1 "
            "WHEN 'individual' THEN 2 "
            "WHEN 'location' THEN 3 "
            "ELSE 4 END"
        )
        if parent_id is None:
            if kw_type is None:
                existing = self.conn.execute(
                    f"SELECT id, type FROM keywords WHERE name = ? COLLATE NOCASE "
                    f"AND parent_id IS NULL "
                    f"ORDER BY {type_priority_case}, id ASC LIMIT 1",
                    (name,),
                ).fetchone()
            else:
                existing = self.conn.execute(
                    "SELECT id, type FROM keywords WHERE name = ? COLLATE NOCASE "
                    "AND parent_id IS NULL AND type IN (?, 'general') "
                    "ORDER BY (type = ?) DESC, id ASC LIMIT 1",
                    (name, kw_type, kw_type),
                ).fetchone()
        else:
            if kw_type is None:
                existing = self.conn.execute(
                    f"SELECT id, type FROM keywords WHERE name = ? COLLATE NOCASE "
                    f"AND parent_id = ? "
                    f"ORDER BY {type_priority_case}, id ASC LIMIT 1",
                    (name, parent_id),
                ).fetchone()
            else:
                existing = self.conn.execute(
                    "SELECT id, type FROM keywords WHERE name = ? COLLATE NOCASE "
                    "AND parent_id = ? AND type IN (?, 'general') "
                    "ORDER BY (type = ?) DESC, id ASC LIMIT 1",
                    (name, parent_id, kw_type, kw_type),
                ).fetchone()
        # A top-level add by name (Pipeline Review's Confirm species on a
        # Browse selection, a flat dc:subject on import) names the species
        # the user already filed in their tree (``Birds > ... > Western
        # meadowlark``). Minting a top-level twin instead splits the
        # species across two rows and queues an XMP add of a keyword the
        # sidecar already holds. A top-level ``general`` row is passed over
        # too when taxonomy is asked for: promoting it would mint the twin.
        if parent_id is None and kw_type in (None, 'taxonomy') and (
            existing is None
            or (kw_type == 'taxonomy' and existing['type'] == 'general')
        ):
            existing = self._placed_taxonomy_keyword(name) or existing
        return existing

    def _placed_taxonomy_keyword(self, name):
        """One placed row whose identity agrees with the requested species.

        A name alone cannot reassign a genus or another species homonym.
        Unknown or conflicting identity stays for an explicit user choice.
        """
        # Most top-level adds name nothing in the tree; skip the taxon
        # lookup for them.
        if self.conn.execute(
            "SELECT 1 FROM keywords WHERE name = ? COLLATE NOCASE "
            "AND parent_id IS NOT NULL AND type = 'taxonomy' LIMIT 1",
            (name,),
        ).fetchone() is None:
            return None
        requested = self._taxa.lookup_taxon_id(name, species_only=True)
        if requested is None:
            return None
        rows = self.conn.execute(
            "SELECT id, type FROM keywords WHERE name = ? COLLATE NOCASE "
            "AND parent_id IS NOT NULL AND type = 'taxonomy' "
            "AND (taxon_id = ? OR (taxon_id IS NULL AND source_taxon_id = "
            "(SELECT inat_id FROM taxa WHERE id = ?))) "
            "AND (source_taxon_id IS NULL OR source_taxon_id = "
            "(SELECT inat_id FROM taxa WHERE id = ?)) LIMIT 2",
            (name, requested, requested, requested),
        ).fetchall()
        return rows[0] if len(rows) == 1 else None

    def name_of(self, keyword_id):
        """The stored name of one keyword, or None when the id is unknown."""
        row = self.conn.execute(
            "SELECT name FROM keywords WHERE id = ?", (keyword_id,)
        ).fetchone()
        return row["name"] if row else None

    def top_level_species_keyword(self, name):
        """The species keyword ``add_keyword(name, is_species=True)`` matches.

        A row (``id``, ``name``) or None: a ``parent_id IS NULL`` taxonomy
        or general keyword named ``name`` case-insensitively, taxonomy
        first, then lowest id, unless the one taxonomy row of that name
        filed in the tree stands in for it (see ``_find_add_candidate``).
        Other types (individual, location, genre) never match, so a
        homonym of another kind is not mistaken for the species.
        """
        row = self._find_add_candidate(name, None, 'taxonomy')
        if row is None:
            return None
        return self.conn.execute(
            "SELECT id, name FROM keywords WHERE id = ?", (row["id"],),
        ).fetchone()

    def species_assignment_identity(self, keyword_id: int) -> dict[str, Any] | None:
        """Return the exact stored keyword identity used by a confirmation."""
        row = self.conn.execute(
            "SELECT id, name, parent_id, type, is_species, taxon_id, source_taxon_id "
            "FROM keywords WHERE id = ?", (keyword_id,),
        ).fetchone()
        return dict(row) if row else None

    def species_confirmation_target(self, name: str) -> dict[str, Any]:
        """Snapshot the exact add target, its source identity and inferred taxon.

        Share add's candidate selection (including promotable general rows and
        placed taxonomy leaves). An unattached keyword is still part of an
        approved confirmation: rename/retype/relink must invalidate its preview.
        """
        candidate = self._find_add_candidate(name, None, 'taxonomy')
        keyword = None
        if candidate is not None:
            keyword = self.conn.execute(
                "SELECT id, name, parent_id, type, is_species, taxon_id, source_taxon_id "
                "FROM keywords WHERE id=?", (candidate["id"],),
            ).fetchone()
        inferred = self._taxa.lookup_taxon_id(name, species_only=True)
        taxon_ids = {tid for tid in (inferred, keyword["taxon_id"] if keyword else None) if tid is not None}
        taxa = []
        if taxon_ids:
            marks = ",".join("?" for _ in taxon_ids)
            taxa = [dict(row) for row in self.conn.execute(
                f"SELECT id, inat_id, name, common_name, rank, parent_id FROM taxa "
                f"WHERE id IN ({marks}) ORDER BY id", sorted(taxon_ids),
            )]
        return {"keyword": dict(keyword) if keyword else None, "inferred_taxon_id": inferred, "taxa": taxa}

    def get_row(self, keyword_id):
        """Row (``id``, ``name``, ``type``) of one keyword, or None when the id is unknown."""
        return self.conn.execute(
            "SELECT id, name, type FROM keywords WHERE id = ?", (keyword_id,)
        ).fetchone()

    def import_tag_row(self, keyword_id: int) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT name, parent_id, type FROM keywords WHERE id = ?",
            (keyword_id,),
        ).fetchone()

