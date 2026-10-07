"""Persistence for keywords: rows, tagging reads, species names.

This module owns the SQL behind the keyword domain:

- keyword rows: ``add`` (with case conventions, taxon linking and the
  source-taxon species path), the default genre seed, the keyword tree
  and listing reads, counts, and the rename / retype statements
  ``Database.update_keyword`` issues;
- tagging reads (per photo, per batch, species-rank names, equivalent
  species) and ``untag``;
- species-name resolution (``resolve_species_display``, taxon lookup,
  case-convention detection, lineage resolution, species marking);
- the scope and liveness reads of ``Database._merge_duplicate_keywords_pass``;
- the reads and the delete behind the ``/api/keywords`` and ``/api/species``
  routes (duplicate groups and per-workspace counts, the rename state, the
  photo/workspace pairs a rename or delete re-queues sidecar changes for,
  the species-name search, ``delete``), moved verbatim from ``web/``.
- the species-confirmation reads ``/api/encounters/species`` runs to find
  the rows standing for the species it replaces or removes.

The method bodies were moved verbatim from ``Database``. The only edits are
``self._ws_id()`` -> ``self.workspace_id`` and ``NAME`` -> ``self.NAME`` for
the ``db`` module helpers and constants listed in ``__init__``;
``resolve_species_display`` also takes ``case_convention`` keyword-only,
because its default (the ``db`` detection sentinel) can't be written in the
def line here and the façade always passes it; and the two places a body
handed the ``Database`` itself to a helper (``resolve_import_alias(self,
...)`` in ``add``, ``SpeciesResolver(db=self)`` in ``add_source_species``)
now pass ``self.db``, the façade, since ``self`` is the repository here. The
SQL text, parameter order, chunk sizes and commit placement are unchanged.

What lives in ``repositories/keyword_provenance.py`` instead: the
``photo_keywords`` writers that create or converge an association
(``tag_photo``, ``_merge_keyword_into``, ``retire_builtin_wildlife_genre``,
``link_keyword_to_place``), which ``test_keyword_provenance_contract`` keys
to that module, and the keyword methods that call ``_merge_keyword_into``
in the middle of their own work (``_upsert_one_keyword``). This module writes
no ``photo_keywords``
association itself; a structural test fails if it ever references one of
those writers.

What deliberately stays on ``Database``:

- The control flow of ``_merge_duplicate_keywords_pass`` and
  ``update_keyword``, with their ``self._merge_keyword_into(...)`` call on
  the façade; they delegate only their statements here
  (``duplicate_scope_rows`` / ``live_ids``, ``get_update_target`` /
  ``same_type_peer`` / ``cross_type_peer`` / ``apply_update``).
- The active-workspace state. ``workspace_id`` is resolved lazily through
  ``Database._ws_id`` at exactly the points the original code called it.
- Composition. Every façade method a moved body calls is bound from the
  ``Database`` instance under its own name (see ``FACADE_METHODS``), and the
  body calls it as ``self.<name>(...)``, so monkeypatches of ``Database``
  methods keep reaching the moved code. That includes cross-domain calls
  (the species-curation renames) and this domain's own entry points that
  other methods compose with.
- The methods with no SQL of their own (``_apply_case_convention``,
  ``_sentence_case_first_word``, ``species_case_convention``).

``_commit`` flags are carried through unchanged: ``_commit=False`` means the
caller owns the transaction, and no method here commits unless the
``Database`` method it backs did.
"""

import sqlite3

from keyword_identity import identity_sql
from keyword_normalization import keyword_match_key, normalize_keyword_display

# ``Database`` methods the moved bodies call through the façade.
FACADE_METHODS = (
    "rename_photo_preferences_species",
    "rename_species_highlights_species",
    "species_case_convention",
    "_apply_case_convention",
    "_merge_duplicate_keywords_pass",
    "resolve_species_display_name",
    "detect_keyword_case_convention",
    "_lookup_taxon_id_for_keyword",
    "_add_source_species_keyword",
    "_rename_keyword_dependents",
    "_resolve_species_by_lineage",
)


class KeywordRepository:
    def __init__(
        self,
        conn,
        resolve_workspace_id,
        *,
        chunks,
        keyword_types,
        detect_case_convention_sentinel,
        taxon_lookup_variants,
        resolve_import_alias,
        filter_subject_chunk,
        facade,
    ):
        self.conn = conn
        self._resolve_workspace_id = resolve_workspace_id
        # ``db`` module helpers and constants, kept under their module names
        # so the moved bodies read them as ``self.<name>``. ``_chunks`` is
        # ``db._chunks`` itself (its size default is bound at import).
        self._chunks = chunks
        self.KEYWORD_TYPES = keyword_types
        self._DETECT_CASE_CONVENTION = detect_case_convention_sentinel
        self._taxon_lookup_variants = taxon_lookup_variants
        self.resolve_import_alias = resolve_import_alias
        # ``Database`` class attributes, kept under their class names.
        self._FILTER_SUBJECT_CHUNK = filter_subject_chunk
        # The ``Database`` itself, for the two helpers the moved bodies hand
        # it to (``resolve_import_alias`` and ``SpeciesResolver``).
        self.db = facade
        # Bound ``Database`` methods, kept under their façade names.
        for name in FACADE_METHODS:
            setattr(self, name, getattr(facade, name))

    @property
    def workspace_id(self):
        """The active workspace id, resolved at each read (raises if none)."""
        return self._resolve_workspace_id()

    def embedded_offered_keys(self, photo_id):
        """Normalized embedded keyword keys the scanner has offered this photo.

        Backs ``Database.get_embedded_keyword_offered_keys`` -- see there for
        why the record exists. Returns a set so callers can test membership
        in O(1) when they filter candidate keywords.
        """
        return {
            row["keyword_key"]
            for row in self.conn.execute(
                "SELECT keyword_key FROM photo_embedded_keyword_offered "
                "WHERE photo_id = ?",
                (photo_id,),
            )
        }

    def record_embedded_offered_keys(self, photo_id, keys, _commit=True):
        """Mark these normalized keys as embedded-offered for the photo.

        Backs ``Database.record_embedded_keyword_offered`` -- see there for
        why the record exists. Idempotent (INSERT OR IGNORE on the composite
        primary key). ``keys`` empty is a no-op. ``_commit=False`` leaves the
        commit to the caller for batch work.
        """
        rows = [(photo_id, key) for key in keys if key]
        if not rows:
            return
        self.conn.executemany(
            "INSERT OR IGNORE INTO photo_embedded_keyword_offered "
            "(photo_id, keyword_key) VALUES (?, ?)",
            rows,
        )
        if _commit:
            self.conn.commit()

    def transfer_embedded_offered_keys(self, losing_id, surviving_id):
        """Move the losing row's embedded-offered keys onto the survivor.

        Used by every merge path that deletes a photo row with a different
        row inheriting its identity: the suppression record exists so a
        user's keyword removal survives a later rescan, and that intent must
        follow the surviving row rather than be dropped with the losing one.
        No commit; the caller folds this into its own transaction. The
        non-cascading FK on ``photo_embedded_keyword_offered.photo_id``
        would otherwise block the ``DELETE FROM photos``.
        """
        self.conn.execute(
            "INSERT OR IGNORE INTO photo_embedded_keyword_offered "
            "(photo_id, keyword_key) "
            "SELECT ?, keyword_key FROM photo_embedded_keyword_offered "
            "WHERE photo_id = ?",
            (surviving_id, losing_id),
        )
        self.conn.execute(
            "DELETE FROM photo_embedded_keyword_offered WHERE photo_id = ?",
            (losing_id,),
        )

    def filter_out_subject_tagged(self, photo_ids, subject_types):
        """Return the subset of photo_ids whose photos do NOT have any keyword
        of a type in subject_types. Empty subject_types or empty photo_ids
        returns photo_ids unchanged (preserving input order).

        Photo ids are chunked under SQLite's bind-variable limit so callers
        can safely pass arbitrarily large lists. The classify job sources
        photo ids from get_collection_photos(per_page=999999), which can
        exceed older SQLite builds' 999-variable cap and trip
        OperationalError: too many SQL variables.

        When ``'taxonomy'`` is among the requested types, legacy species rows
        (``is_species=1`` with a non-taxonomy ``type``) also count as
        subject-tagged. Upgraded databases carry these rows until the
        background ``mark_species_keywords`` pass retypes them; without this
        guard, already-identified photos would still be classified and would
        appear in 'Needs Identification' during that window.
        """
        if not subject_types or not photo_ids:
            return list(photo_ids)
        types = [t for t in subject_types if t in self.KEYWORD_TYPES]
        if not types:
            return list(photo_ids)
        type_placeholders = ",".join("?" * len(types))
        type_clause = f"k.type IN ({type_placeholders})"
        if "taxonomy" in types:
            type_clause = f"({type_clause} OR k.is_species = 1)"
        photo_ids_list = list(photo_ids)
        excluded = set()
        chunk_size = self._FILTER_SUBJECT_CHUNK
        for i in range(0, len(photo_ids_list), chunk_size):
            chunk = photo_ids_list[i:i + chunk_size]
            pid_placeholders = ",".join("?" * len(chunk))
            rows = self.conn.execute(
                f"""SELECT DISTINCT pk.photo_id FROM photo_keywords pk
                    JOIN keywords k ON k.id = pk.keyword_id
                    WHERE {type_clause}
                      AND pk.photo_id IN ({pid_placeholders})""",
                types + chunk,
            ).fetchall()
            for r in rows:
                excluded.add(r["photo_id"])
        return [pid for pid in photo_ids_list if pid not in excluded]

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
        self.conn.commit()

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

    def get_accepted_species(self):
        """Return distinct marker species from geolocated photos in the active workspace.

        Uses the same "geolocated" definition as get_geolocated_photos: a
        photo is included if it has EXIF coords OR a ``type='location'``
        keyword with coords. That keeps the species filter dropdown in sync
        with which photos can actually appear as markers — otherwise photos
        placed via location-keyword coords would render on the map but their
        species would be missing from the filter.
        """
        ws = self.workspace_id
        return [
            row[0]
            for row in self.conn.execute(
                """
                SELECT DISTINCT k.name
                FROM photos p
                JOIN photo_workspace_visibility wf ON wf.photo_id = p.id
                JOIN folders f ON f.id = p.folder_id AND f.status IN ('ok', 'partial')
                JOIN photo_keywords pk ON pk.photo_id = p.id
                JOIN keywords k ON k.id = pk.keyword_id AND k.is_species = 1
                WHERE wf.workspace_id = ?
                  AND (
                    (p.latitude IS NOT NULL AND p.longitude IS NOT NULL)
                    OR EXISTS (
                      SELECT 1
                      FROM photo_keywords pk_loc
                      JOIN keywords k_loc ON k_loc.id = pk_loc.keyword_id
                      WHERE pk_loc.photo_id = p.id
                        AND k_loc.type = 'location'
                        AND k_loc.latitude IS NOT NULL
                        AND k_loc.longitude IS NOT NULL
                    )
                  )
                ORDER BY k.name ASC
                """,
                (ws,),
            ).fetchall()
        ]

    def detect_case_convention(self):
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
        self, name, apply_case_convention=True,
        *, case_convention,
    ):
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

    def species_root_name_for_taxon(self, taxon_id):
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

    def lookup_taxon_id(
        self, name, prefer_species=False, species_only=False,
    ):
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
                    continue
                # No species-rank match on this variant; prefer the direct
                # (higher-rank) hit, otherwise fall back to the common-name
                # hit if one exists.
                if direct:
                    return direct["id"]
                if common:
                    return common["id"]
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

    def add_source_species(self, name, source_taxon_id, parent_id=None, _commit=True):
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
        if existing:
            kid = existing["id"]
            self.conn.execute(
                "UPDATE keywords SET source_taxon_id = ?, taxon_id = ?, is_species = 1, type = 'taxonomy' WHERE id = ?",
                (source_taxon_id, local_id, kid),
            )
        else:
            from species_identity import SpeciesResolver
            raw_name = name.removesuffix(f" (taxon {source_taxon_id})")
            identity = SpeciesResolver(db=self.db).resolve(raw_name, source={"taxon_id": source_taxon_id})
            display = self.resolve_species_display_name(identity.display_name)
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
        if _commit:
            self.conn.commit()
        return kid

    def relink_source_species(self):
        """Refresh local foreign keys after importing source taxa; caller commits."""
        self.conn.execute(
            "UPDATE keywords SET taxon_id = (SELECT id FROM taxa WHERE inat_id = keywords.source_taxon_id) "
            "WHERE source_taxon_id IS NOT NULL AND EXISTS "
            "(SELECT 1 FROM taxa WHERE inat_id = keywords.source_taxon_id)"
        )

    def add(self, name, parent_id=None, is_species=False, kw_type=None, _commit=True, source_taxon_id=None,
                    _resolve_alias=False):
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
            _commit: If False, skip the internal commit (caller is responsible
                     for committing the transaction).
            source_taxon_id: Explicit iNaturalist ID for a species keyword;
                     bypass common-name inference and reuse only that identity.
            _resolve_alias: Import callers opt in for leaf keywords only.
                     Manual additions must not inherit imported keyword aliases,
                     and a leaf alias must not relocate a new parent chain.
        """
        if kw_type is not None and kw_type not in self.KEYWORD_TYPES:
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
            resolved = self.resolve_import_alias(self.db, name, parent_id, kw_type=kw_type)
            if resolved is not None:
                return resolved
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
        if source_taxon_id is not None:
            if not is_species:
                raise ValueError("source_taxon_id requires a species keyword")
            return self._add_source_species_keyword(name, source_taxon_id, parent_id, _commit)
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
                taxon_id = self._lookup_taxon_id_for_keyword(
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
                # (see :meth:`get_life_list_candidates` and
                # :meth:`get_life_list_locations`), so clearing here would
                # strip the row's ``taxon_rank`` / ``scientific_name`` /
                # ``taxonomic_class`` metadata and silently break the new
                # genus / family / class Life List filters — mirroring the
                # startup preservation in :meth:`mark_species_keywords`.
            if kw_type is None and not is_species and existing["type"] == "general":
                taxon_id = self._lookup_taxon_id_for_keyword(
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
                    if _commit:
                        self.conn.commit()
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
                if _commit:
                    self.conn.commit()
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
                if _commit:
                    self.conn.commit()
            return existing["id"]

        # Apply casing convention for new species keywords
        if is_species:
            import config as cfg

            override = cfg.get("keyword_case")
            if override and override != "auto":
                name = self._apply_case_convention(name, override)
            else:
                convention = self.detect_keyword_case_convention()
                if convention:
                    name = self._apply_case_convention(name, convention)

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
            self._lookup_taxon_id_for_keyword(name, species_only=True)
            if kw_type == 'taxonomy'
            else None
        )
        if kw_type is None:
            # Auto-detect taxonomy type from taxa table
            kw_type = 'general'
            if is_species:
                kw_type = 'taxonomy'
            else:
                taxon_id = self._lookup_taxon_id_for_keyword(
                    name, prefer_species=True,
                )
                if taxon_id:
                    kw_type = 'taxonomy'

        # The lookup above ran without a write lock, so a second connection
        # can pass the same check before either inserts. UNIQUE(name,
        # parent_id) cannot catch that for a top-level keyword (SQLite treats
        # NULL parents as distinct) and is case-sensitive besides. Whenever
        # the connection is not already in a transaction, take the write
        # lock and look again, so the loser of the race reuses the winner's
        # row. This must include ``_commit=False`` callers (``sync.py``,
        # ``web/encounters.py``, ``web/highlights.py``, the import job at
        # ``web/imports.py``): each passes ``_commit=False`` as the first
        # mutation on a fresh worker connection, so without the write lock
        # the same race lets both sides insert a duplicate root keyword.
        # When we start the transaction under ``_commit=False``, the caller
        # inherits our open ``BEGIN IMMEDIATE`` and finalises it with the
        # rest of their work.
        started_transaction = not self.conn.in_transaction
        if started_transaction:
            self.conn.execute("BEGIN IMMEDIATE")
            if self._find_add_candidate(lookup_name, parent_id, lookup_kw_type) is not None:
                # No writes happened under our new transaction. When we own
                # the commit, finalise it before recursing so the reused row
                # is visible; when the caller owns it, leave the transaction
                # open so the recursion runs under it and the caller commits.
                if _commit:
                    self.conn.commit()
                # Rerun so the reused row gets the same promotions as any
                # other hit on the lookup.
                return self.add(
                    lookup_name, parent_id, is_species=lookup_is_species,
                    kw_type=lookup_kw_type, _commit=_commit,
                )
        try:
            cur = self.conn.execute(
                "INSERT INTO keywords (name, parent_id, is_species, type, taxon_id) VALUES (?, ?, ?, ?, ?)",
                (name, parent_id, 1 if is_species else (1 if taxon_id else 0), kw_type, taxon_id),
            )
        except sqlite3.IntegrityError:
            # A same-spelling row under this parent committed after the
            # lookup. Reuse it when it is one ``add`` would have returned.
            if started_transaction:
                self.conn.rollback()
            if self._find_add_candidate(lookup_name, parent_id, lookup_kw_type) is None:
                raise
            return self.add(
                lookup_name, parent_id, is_species=lookup_is_species,
                kw_type=lookup_kw_type, _commit=_commit,
            )
        except BaseException:
            if started_transaction:
                self.conn.rollback()
            raise
        if _commit:
            self.conn.commit()
        return cur.lastrowid

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
        return existing

    def merge_duplicates(self):
        """Find and merge normalized duplicate keywords in active workspace.

        Duplicates are grouped by (normalized name, parent_id, type,
        species-bearing) — name alone is not identity: the location system
        deliberately creates same-name keywords under different parents
        (Springfield under Illinois vs. Missouri), and same-name keywords of
        different types (species vs. genre) are distinct by design. Legacy
        ``type='general', is_species=1`` rows are also distinct from ordinary
        general homonyms. Merging across those slots retags photos with the
        wrong place/kind.

        A keyword is in scope when it — or any descendant — is tagged on a
        photo in the active workspace. XMP import only tags the leaf of a
        hierarchical keyword ("Birds > Heron" tags Heron, not Birds), so
        duplicate ancestors usually have no photo_keywords rows of their
        own; walking up from the tagged leaves brings them in scope while
        still leaving other workspaces' keywords untouched.

        Keeps the lowest ID (earliest created), moves all photo associations,
        reparents any child keywords onto the survivor (the parent_id FK
        would otherwise block the DELETE), and deletes the duplicates.
        Runs passes until convergence so case-duplicate parent chains
        ("Birds">"Heron" vs "birds">"heron") fully collapse: the children
        only become same-parent duplicates after their parents merge.
        The whole pass is all-or-nothing: an exception rolls back every
        pending merge instead of leaving a half-merged tree on the
        connection for a later unrelated commit to persist.
        Returns count of merges performed.
        """
        ws = self.workspace_id
        total_merged = 0
        try:
            total_merged = self._merge_duplicate_keywords_pass(ws)
        except Exception:
            self.conn.rollback()
            raise
        if total_merged:
            self.conn.commit()
        return total_merged

    def normalize_row_name(self, keyword_id):
        """Trim stray edge punctuation from a surviving keyword row name.

        Stored names are already normalized, so this is a no-op in the
        common case — it exists so the duplicate cleanup can canonicalize a
        survivor whose spelling predates normalization.
        ``_rename_keyword_dependents`` then carries the new spelling into
        every string that mirrors it.
        """
        row = self.conn.execute(
            "SELECT name FROM keywords WHERE id = ?", (keyword_id,)
        ).fetchone()
        if row is None:
            return
        old_name = row["name"]
        cleaned = normalize_keyword_display(old_name)
        if not cleaned or cleaned == old_name:
            return
        # A same-name row in a different dedupe boundary (another type at
        # the same parent) can still occupy the table-level
        # UNIQUE(name, parent_id) slot. The links still merge correctly;
        # keep the stored spelling unchanged in that case.
        try:
            self.conn.execute(
                "UPDATE keywords SET name = ? WHERE id = ?", (cleaned, keyword_id)
            )
        except sqlite3.IntegrityError:
            return
        # Keep every dependent name string in lockstep with the row.
        self._rename_keyword_dependents(keyword_id, old_name, cleaned)

    def rename_dependents(self, keyword_id, old_name, new_name):
        """Carry a keyword row's rename into every string that mirrors its name.

        Pending sidecar edits and the species curation tables store the
        keyword's spelling rather than its id, so a row renamed without this
        leaves an unsynced ``keyword_add`` writing the old word into XMP and
        drops starred photos out of the highlight/life-list queries, which
        compare those strings exact against ``keywords.name``.

        Scoped to photos that actually carry ``keyword_id`` (and, for pending
        changes, the workspaces those (photo, keyword) tags belong to), so a
        separate same-spelling keyword row elsewhere in the DB is never
        rewritten by side effect. Safe to call either side of the
        ``keywords`` UPDATE: only ``photo_keywords`` is read, and a name
        change does not touch it. Caller commits.
        """
        if not old_name or not new_name or old_name == new_name:
            return
        tag_rows = self.conn.execute(
            """SELECT DISTINCT pk.photo_id, wf.workspace_id
               FROM photo_keywords pk
               JOIN photos p ON p.id = pk.photo_id
               JOIN photo_workspace_visibility wf ON wf.photo_id = p.id
               WHERE pk.keyword_id = ?""",
            (keyword_id,),
        ).fetchall()
        photo_workspace_pairs = [
            (r["photo_id"], r["workspace_id"]) for r in tag_rows
        ]
        affected_photo_ids = sorted({r["photo_id"] for r in tag_rows})
        # Retarget pending keyword_add/keyword_remove rows queued under the
        # pre-canonical spelling so a still-unsynced sidecar write can't
        # leak the legacy variant after the DB row was rewritten. A pending
        # row that would collide with an existing (photo_id, change_type,
        # new_name) row is dropped rather than duplicated, matching
        # queue_change's dedupe contract.
        if affected_photo_ids:
            for chunk in self._chunks(affected_photo_ids):
                placeholders = ",".join("?" for _ in chunk)
                self.conn.execute(
                    f"""DELETE FROM pending_changes
                        WHERE change_type IN ('keyword_add', 'keyword_remove')
                          AND value = ?
                          AND photo_id IN ({placeholders})
                          AND EXISTS (
                              SELECT 1 FROM pending_changes pc2
                              WHERE pc2.photo_id = pending_changes.photo_id
                                AND pc2.change_type = pending_changes.change_type
                                AND pc2.value = ?
                                AND COALESCE(pc2.workspace_id, -1)
                                    = COALESCE(pending_changes.workspace_id, -1)
                          )""",
                    [old_name, *chunk, new_name],
                )
                self.conn.execute(
                    f"""UPDATE pending_changes
                        SET value = ?
                        WHERE change_type IN ('keyword_add', 'keyword_remove')
                          AND value = ?
                          AND photo_id IN ({placeholders})""",
                    [new_name, old_name, *chunk],
                )
        # Species curation tables key rows by the species name string, which
        # is compared exact against ``keywords.name``. Now that the UPDATE
        # above rewrote this row to the canonical spelling, rows still keyed
        # on the legacy spelling would drop out of the highlight/life-list
        # queries even though the tag was retained. Rename them for the same
        # old→clean mapping, scoped to the tagged (photo, workspace) pairs.
        # ``rename_photo_preferences_species`` also retargets
        # ``species_representatives`` in its scoped branch, so a separate
        # representatives rename isn't needed here.
        if photo_workspace_pairs:
            self.rename_species_highlights_species(
                old_name, new_name,
                photo_workspace_pairs=photo_workspace_pairs, _commit=False,
            )
            self.rename_photo_preferences_species(
                old_name, new_name,
                photo_workspace_pairs=photo_workspace_pairs, _commit=False,
            )


    def reparent_disambiguated(self, child, dst_id, new_name):
        """Move a colliding child under ``dst_id`` under a free name.

        The three collision branches in ``_merge_keyword_into`` all preserve
        the migrating row rather than folding it away, which means they all
        rename it -- and a rename is never just the ``keywords`` row. Pending
        sidecar edits and the species curation tables key on the spelling, so
        skipping the dependent migration leaves an unsynced ``keyword_add``
        writing the retired name into XMP and drops starred photos out of the
        highlight and life-list queries. Caller commits.
        """
        self.conn.execute(
            "UPDATE keywords SET parent_id = ?, name = ? WHERE id = ?",
            (dst_id, new_name, child["id"]),
        )
        self._rename_keyword_dependents(child["id"], child["name"], new_name)

    def get_tree(self):
        """Return keywords used by photos in the active workspace, plus ancestors."""
        return self.conn.execute(
            """WITH RECURSIVE
               leaf_kw AS (
                   SELECT DISTINCT pk.keyword_id AS id
                   FROM photo_keywords pk
                   JOIN photos p ON p.id = pk.photo_id
                   JOIN photo_workspace_visibility wf ON wf.photo_id = p.id
                   WHERE wf.workspace_id = ?
               ),
               ancestors AS (
                   SELECT id FROM leaf_kw
                   UNION
                   SELECT k.parent_id
                   FROM keywords k
                   JOIN ancestors a ON a.id = k.id
                   WHERE k.parent_id IS NOT NULL
               )
               SELECT k.id, k.name, k.parent_id, k.type
               FROM keywords k
               JOIN ancestors a ON a.id = k.id
               ORDER BY k.name""",
            (self.workspace_id,),
        ).fetchall()

    def untag(self, photo_id, keyword_id, _commit=True):
        """Remove a keyword association from a photo.

        Args:
            _commit: If False, skip the internal commit (caller is responsible
                     for committing the transaction).
        """
        self.conn.execute(
            "DELETE FROM photo_keywords WHERE photo_id = ? AND keyword_id = ?",
            (photo_id, keyword_id),
        )
        if _commit:
            self.conn.commit()

    def name_of(self, keyword_id):
        """The stored name of one keyword, or None when the id is unknown."""
        row = self.conn.execute(
            "SELECT name FROM keywords WHERE id = ?", (keyword_id,)
        ).fetchone()
        return row["name"] if row else None

    def top_level_species_keyword(self, name):
        """The top-level species keyword ``add_keyword(name, is_species=True)`` matches.

        A row (``id``, ``name``) or None: a ``parent_id IS NULL`` taxonomy
        or general keyword named ``name`` case-insensitively, taxonomy
        first, then lowest id. Other types (individual, location, genre)
        never match, so a homonym of another kind is not mistaken for the
        species.
        """
        return self.conn.execute(
            "SELECT id, name FROM keywords WHERE name = ? COLLATE NOCASE "
            "AND parent_id IS NULL AND type IN ('taxonomy', 'general') "
            "ORDER BY (type = 'taxonomy') DESC, id ASC LIMIT 1",
            (name,),
        ).fetchone()

    def get_row(self, keyword_id):
        """Row (``id``, ``name``, ``type``) of one keyword, or None when the id is unknown."""
        return self.conn.execute(
            "SELECT id, name, type FROM keywords WHERE id = ?", (keyword_id,)
        ).fetchone()

    def photo_ids_tagged_with(self, keyword_id, photo_ids):
        """The set of ``photo_ids`` that carry ``keyword_id``."""
        tagged = set()
        for chunk in self._chunks(photo_ids):
            placeholders = ",".join("?" for _ in chunk)
            rows = self.conn.execute(
                f"""SELECT photo_id FROM photo_keywords
                    WHERE keyword_id = ? AND photo_id IN ({placeholders})""",
                [keyword_id] + chunk,
            ).fetchall()
            tagged.update(row["photo_id"] for row in rows)
        return tagged

    def photo_associations_by_name(self, photo_id, name):
        """A photo's keyword associations whose keyword is named ``name``.

        Matched case-insensitively, so homonyms under different parents all
        come back. Each row is ``keyword_id``, ``source`` and
        ``has_exact_history`` (1 when a live ``keyword_add`` history item
        recorded this photo gaining this exact keyword id, else 0).
        """
        return self.conn.execute(
            """SELECT pk.keyword_id, pk.source,
                      EXISTS (
                          SELECT 1
                          FROM edit_history_items item
                          JOIN edit_history edit ON edit.id = item.edit_id
                          WHERE item.photo_id = pk.photo_id
                            AND edit.action_type = 'keyword_add'
                            AND edit.undone = 0
                            AND item.new_value = CAST(pk.keyword_id AS TEXT)
                      ) AS has_exact_history
               FROM photo_keywords pk
               JOIN keywords k ON k.id = pk.keyword_id
               WHERE pk.photo_id = ?
                 AND k.name = ? COLLATE NOCASE""",
            (photo_id, name),
        ).fetchall()

    def parent_rows(self):
        """Every keyword's ``id``, ``name`` and ``parent_id`` (for ``keyword_paths``)."""
        return self.conn.execute(
            'SELECT id, name, parent_id FROM keywords'
        ).fetchall()

    def rename_state(self, keyword_id):
        """Row (``name``, ``is_species``, ``type``) of one keyword, or None.

        What a rename or retype compares before and after the update.
        """
        return self.conn.execute(
            """SELECT name, is_species, type
               FROM keywords WHERE id = ?""",
            (keyword_id,),
        ).fetchone()

    def photo_workspaces_tagged_with(self, keyword_id):
        """Rows (``photo_id``, ``workspace_id``): each photo carrying
        ``keyword_id`` once per workspace that can see it."""
        return self.conn.execute(
            """SELECT pk.photo_id, wf.workspace_id
               FROM photo_keywords pk
               JOIN photos p ON p.id = pk.photo_id
               JOIN photo_workspace_visibility wf ON wf.photo_id = p.id
               WHERE pk.keyword_id = ?""",
            (keyword_id,),
        ).fetchall()

    def subtree_photo_workspaces(self, keyword_id):
        """Distinct rows (``photo_id``, ``workspace_id``) for every photo
        tagged with ``keyword_id`` or any keyword below it, once per
        workspace that can see the photo."""
        return self.conn.execute(
            """WITH RECURSIVE tree(id) AS (
                   SELECT ?
                   UNION ALL
                   SELECT k.id FROM keywords k
                   JOIN tree t ON k.parent_id = t.id
               )
               SELECT DISTINCT p.id AS photo_id, wf.workspace_id
               FROM photos p
               JOIN photo_keywords pk ON pk.photo_id = p.id
               JOIN photo_workspace_visibility wf ON wf.photo_id = p.id
               JOIN tree t ON t.id = pk.keyword_id""",
            (keyword_id,),
        ).fetchall()

    def workspace_duplicate_groups(self, workspace_id):
        """Rows (``lname``, ``ids``, ``names``, ``cnt``) of keywords tagged in
        ``workspace_id`` that share a merge slot: ``LOWER(name)``,
        ``parent_id``, ``type`` and species-bearing. ``workspace_id=None``
        matches nothing."""
        return self.conn.execute(
            """SELECT LOWER(k.name) as lname, GROUP_CONCAT(k.id) as ids,
                      GROUP_CONCAT(k.name, ' | ') as names, COUNT(DISTINCT k.id) as cnt
               FROM keywords k
               JOIN photo_keywords pk ON pk.keyword_id = k.id
               JOIN photos p ON p.id = pk.photo_id
               JOIN photo_workspace_visibility wf ON wf.photo_id = p.id
               WHERE wf.workspace_id = ?
               GROUP BY LOWER(k.name), k.parent_id, k.type,
                        CASE WHEN k.type = 'taxonomy' OR k.is_species = 1
                             THEN 1 ELSE 0 END
               HAVING COUNT(DISTINCT k.id) > 1""",
            (workspace_id,),
        ).fetchall()

    def workspace_photo_count(self, keyword_id, workspace_id):
        """Row (``name``, ``cnt``): the keyword's tagged photos visible in
        ``workspace_id``. Always one row; ``name`` is NULL when ``cnt`` is 0."""
        return self.conn.execute(
            """SELECT k.name, COUNT(pk.photo_id) as cnt
               FROM keywords k
               JOIN photo_keywords pk ON pk.keyword_id = k.id
               JOIN photos p ON p.id = pk.photo_id
               JOIN photo_workspace_visibility wf ON wf.photo_id = p.id
               WHERE k.id = ? AND wf.workspace_id = ?""",
            (keyword_id, workspace_id),
        ).fetchone()

    def species_names_matching(self, query, match_case, whole_word):
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

    def delete(self, keyword_id):
        """Delete one keyword and commit.

        Its children become root keywords and its photo associations are
        removed. Statements the caller left uncommitted share the commit.
        """
        self.conn.execute("UPDATE keywords SET parent_id = NULL WHERE parent_id = ?", (keyword_id,))
        self.conn.execute("DELETE FROM photo_keywords WHERE keyword_id = ?", (keyword_id,))
        self.conn.execute("DELETE FROM keywords WHERE id = ?", (keyword_id,))
        self.conn.commit()

    def species_rank_keywords_for_photo(self, photo_id):
        """A photo's species-rank identification keywords.

        Rows (``id``, ``name``, ``is_species``, ``type``) for keywords that are
        ``is_species`` or ``taxonomy`` and linked to a ``species``-rank taxon or
        to none; ``is_species`` rows first, then most recently tagged first.
        """
        return self.conn.execute(
            """SELECT k.id, k.name, k.is_species, k.type
               FROM photo_keywords pk
               JOIN keywords k ON k.id = pk.keyword_id
               LEFT JOIN taxa t ON t.id = k.taxon_id
               WHERE pk.photo_id = ?
                 AND (k.is_species = 1 OR k.type = 'taxonomy')
                 AND (t.rank = 'species' OR t.rank IS NULL)
               ORDER BY k.is_species DESC, pk.rowid DESC""",
            (photo_id,),
        ).fetchall()

    def photo_ids_with_species_rank_keyword(self, photo_ids):
        """Which of ``photo_ids`` carry a species-rank identification keyword.

        Same keyword filter as :meth:`species_rank_keywords_for_photo`.
        """
        found = set()
        for chunk in self._chunks(photo_ids):
            placeholders = ",".join("?" for _ in chunk)
            found.update(
                row["photo_id"] for row in self.conn.execute(
                    f"""SELECT DISTINCT pk.photo_id
                        FROM photo_keywords pk
                        JOIN keywords k ON k.id = pk.keyword_id
                        LEFT JOIN taxa t ON t.id = k.taxon_id
                        WHERE pk.photo_id IN ({placeholders})
                          AND (k.is_species = 1 OR k.type = 'taxonomy')
                          AND (t.rank = 'species' OR t.rank IS NULL)""",
                    chunk,
                ).fetchall()
            )
        return found

    def species_identity_rows(self):
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

    def get_for_photo(self, photo_id):
        """Return all keywords for a photo."""
        return self.conn.execute(
            """SELECT k.id, k.name, k.parent_id, k.type
               FROM keywords k
               JOIN photo_keywords pk ON pk.keyword_id = k.id
               WHERE pk.photo_id = ?
               ORDER BY k.name""",
            (photo_id,),
        ).fetchall()

    def get_for_photos(self, photo_ids):
        """Return keywords for a batch of photos keyed by photo id."""
        if not photo_ids:
            return {}
        # Dedup-preserving-order: chunking that re-queries the same id
        # in a later chunk would double-append it under setdefault.
        photo_ids = list(dict.fromkeys(photo_ids))
        result = {}
        for chunk in self._chunks(photo_ids):
            placeholders = ",".join("?" for _ in chunk)
            rows = self.conn.execute(
                f"""SELECT pk.photo_id, k.id, k.name, k.parent_id, k.type,
                           k.is_species, k.taxon_id, t.rank AS taxon_rank
                    FROM photo_keywords pk
                    JOIN keywords k ON k.id = pk.keyword_id
                    LEFT JOIN taxa t ON t.id = k.taxon_id
                    WHERE pk.photo_id IN ({placeholders})
                    ORDER BY k.type, k.name""",
                list(chunk),
            ).fetchall()
            for r in rows:
                result.setdefault(r["photo_id"], []).append(dict(r))
        return result

    def get_species_for_photos(self, photo_ids, include_identities=False):
        """Return deduplicated species-rank keyword names for photos.

        Returns a dict mapping photo_id -> list of species name strings.
        With include_identities, each entry contains the stored name and a
        source-aware identity key for comparisons that must preserve homonyms.

        A linked taxon must actually have rank ``species``; linked family,
        genus, and other ancestor keywords remain taxonomy keywords but are
        not presented as species. Taxonomy rows without a resolvable taxon
        retain the legacy behavior so user-created/offline species tags do
        not disappear.

        Multiple keyword nodes can represent the same taxon (for example a
        Lightroom hierarchy leaf plus an older top-level confirmation row).
        Collapse those by taxon_id and canonicalize to the same-taxon root's
        stored spelling when one exists — species_representatives,
        species_highlights, and life-list preference rows key on that root
        spelling, so a photo whose only surviving species tag is a hierarchy
        leaf ("verdin" after repair detached the redundant "Verdin" root)
        would otherwise miss those curation lookups. Falls back to the
        row's own name when no root exists, and taxonomy-less legacy rows
        continue to use the normalized keyword name.
        """
        if not photo_ids:
            return {}
        # Dedup-preserving-order: chunking that re-queries the same id
        # in a later chunk would double-append it under setdefault.
        photo_ids = list(dict.fromkeys(photo_ids))
        chosen = {}
        source_keys = {}
        for chunk in self._chunks(photo_ids):
            placeholders = ",".join("?" for _ in chunk)
            rows = self.conn.execute(
                f"""SELECT pk.photo_id, k.id, k.name, k.parent_id,
                           k.taxon_id, k.source_taxon_id, t.inat_id, t.name AS scientific_name, t.rank AS taxon_rank
                    FROM photo_keywords pk
                    JOIN keywords k ON k.id = pk.keyword_id
                    LEFT JOIN taxa t ON t.id = k.taxon_id
                    WHERE pk.photo_id IN ({placeholders})
                      AND (k.is_species = 1 OR k.type = 'taxonomy')
                      AND (t.rank = 'species' OR t.rank IS NULL)
                    ORDER BY pk.photo_id,
                             CASE WHEN k.parent_id IS NULL THEN 1 ELSE 0 END,
                             k.id""",
                list(chunk),
            ).fetchall()
            for r in rows:
                if r["taxon_id"] is not None:
                    identity = ("taxon", r["taxon_id"])
                else:
                    # Preserve exact spelling for NULL-taxon rows: unlinked
                    # curation compares by exact ``k.name``, so a root ``Foo``
                    # and a hierarchy leaf ``foo`` on the same photo must
                    # both appear here — folding by ``keyword_match_key``
                    # would drop the root and strand its representative /
                    # highlight lookups.
                    identity = ("name", r["name"])
                # Track whether the chosen row is itself a root: attached root
                # rows must keep their own stored spelling (curation is keyed
                # by the actually attached ``k.name``, so a same-taxon sibling
                # root row must not rewrite it). Only hierarchy leaves need the
                # root-name fallback.
                is_root = r["parent_id"] is None
                chosen.setdefault(r["photo_id"], {}).setdefault(
                    identity, (r["name"], is_root)
                )
                source_id = r["source_taxon_id"] or r["inat_id"]
                key = f"taxon:{source_id}" if source_id else (
                    "scientific:" + r["scientific_name"].casefold() if r["scientific_name"]
                    else "name:" + keyword_match_key(r["name"])
                )
                source_keys.setdefault((r["photo_id"], identity), key)
        taxon_ids = {
            identity[1]
            for by_identity in chosen.values()
            for identity in by_identity
            if identity[0] == "taxon"
        }
        canonical_roots = {}
        if taxon_ids:
            for chunk in self._chunks(list(taxon_ids)):
                placeholders = ",".join("?" for _ in chunk)
                root_rows = self.conn.execute(
                    f"""SELECT taxon_id, name FROM keywords
                        WHERE taxon_id IN ({placeholders})
                          AND parent_id IS NULL
                          AND (is_species = 1 OR type = 'taxonomy')
                        ORDER BY id""",
                    list(chunk),
                ).fetchall()
                for row in root_rows:
                    canonical_roots.setdefault(row["taxon_id"], row["name"])
        result = {}
        for photo_id, by_identity in chosen.items():
            names = []
            for identity, (name, is_root) in by_identity.items():
                if identity[0] == "taxon" and not is_root:
                    name = canonical_roots.get(identity[1], name)
                names.append({"name": name, "key": source_keys[(photo_id, identity)]} if include_identities else name)
            result[photo_id] = sorted(names, key=lambda n: keyword_match_key(n["name"] if include_identities else n))
        return result

    def get_photos_with_equivalent_species(
        self, photo_ids, keyword_id, exclude_keyword_ids=None,
    ):
        """Return submitted photo ids already carrying the target species.

        Species identity is the linked taxon when available, not a particular
        keyword row. This lets a hierarchical ``Birds|Verdin`` tag satisfy a
        later confirmation that resolved to the top-level Verdin keyword.
        Unlinked legacy species fall back to the normalized display name.

        When ``exclude_keyword_ids`` is provided, keyword rows with those ids
        are ignored during the match. Callers use this to look past rows that
        are about to be removed — for example, a same-taxon replacement where
        the "already carries this species" answer must reflect only the rows
        that will survive the mutation.
        """
        if not photo_ids:
            return set()
        target = self.conn.execute(
            "SELECT name, taxon_id FROM keywords WHERE id = ?", (keyword_id,)
        ).fetchone()
        if target is None:
            return set()
        result = set()
        ids = list(dict.fromkeys(int(pid) for pid in photo_ids))
        target_key = keyword_match_key(target["name"])
        excluded = tuple(dict.fromkeys(int(x) for x in (exclude_keyword_ids or ())))
        excl_placeholders = ",".join("?" for _ in excluded)
        excl_clause = f" AND k.id NOT IN ({excl_placeholders})" if excluded else ""
        # When another taxonomy/species keyword row shares the target's
        # match key but points at a different taxon (e.g. legacy
        # ``Robin`` alongside taxonomy ``robin``), the taxon_id-is-NULL
        # fallback below is ambiguous: an unlinked same-key row on the
        # photo could be either species. Treating it as the target would
        # let a confirm/accept skip ``tag_photo``/``queue_change`` and
        # leave the intended species keyword absent. Detect the homonym
        # conflict once and gate the fallback.
        #
        # The same guard applies when the *target* itself is unlinked:
        # if any other same-key row IS linked to a taxon, that linked
        # row is a distinct species that must not be folded into the
        # unlinked target during accept/confirm. In that case the
        # name-only fallback matches only the exact target row.
        homonym_conflict = False
        if target["taxon_id"] is not None:
            for row in self.conn.execute(
                """SELECT name FROM keywords
                   WHERE (is_species = 1 OR type = 'taxonomy')
                     AND taxon_id IS NOT NULL
                     AND taxon_id != ?""",
                (target["taxon_id"],),
            ).fetchall():
                if keyword_match_key(row["name"]) == target_key:
                    homonym_conflict = True
                    break
        else:
            for row in self.conn.execute(
                """SELECT name FROM keywords
                   WHERE (is_species = 1 OR type = 'taxonomy')
                     AND taxon_id IS NOT NULL
                     AND id != ?""",
                (keyword_id,),
            ).fetchall():
                if keyword_match_key(row["name"]) == target_key:
                    homonym_conflict = True
                    break
        for chunk in self._chunks(ids):
            placeholders = ",".join("?" for _ in chunk)
            if target["taxon_id"] is not None:
                # Match rows linked to the same taxon first — the fast,
                # authoritative case that survives any display-name rename.
                rows = self.conn.execute(
                    f"""SELECT DISTINCT pk.photo_id
                        FROM photo_keywords pk
                        JOIN keywords k ON k.id = pk.keyword_id
                        WHERE pk.photo_id IN ({placeholders})
                          AND k.taxon_id = ?
                          AND (k.is_species = 1 OR k.type = 'taxonomy')
                          {excl_clause}""",
                    [*chunk, target["taxon_id"], *excluded],
                ).fetchall()
                result.update(row["photo_id"] for row in rows)
                if homonym_conflict:
                    continue
                # Fallback: upgraded libraries can carry a hierarchical
                # species leaf typed as taxonomy/is_species that
                # mark_species_keywords hasn't yet linked to a taxon_id
                # (see the explicit ``taxon_id IS NULL`` branch it
                # handles). A strict taxon_id equality above would miss
                # those legacy leaves, so a follow-up
                # confirm/accept-species after add_keyword created a
                # linked top-level root would not recognize the existing
                # hierarchy and would queue a duplicate root tag plus
                # sidecar add. Match unlinked rows by normalized display
                # name to preserve the hierarchy. Guarded above so an
                # ambiguous same-key homonym doesn't get folded in.
                rows = self.conn.execute(
                    f"""SELECT pk.photo_id, k.name
                        FROM photo_keywords pk
                        JOIN keywords k ON k.id = pk.keyword_id
                        WHERE pk.photo_id IN ({placeholders})
                          AND k.taxon_id IS NULL
                          AND (k.is_species = 1 OR k.type = 'taxonomy')
                          {excl_clause}""",
                    [*chunk, *excluded],
                ).fetchall()
                result.update(
                    row["photo_id"] for row in rows
                    if keyword_match_key(row["name"]) == target_key
                )
                continue
            # Target is unlinked. When a distinct linked row shares this
            # match key, any same-key row on the photo could be either
            # species; only the exact target keyword row is safe to
            # treat as equivalent.
            if homonym_conflict:
                rows = self.conn.execute(
                    f"""SELECT pk.photo_id
                        FROM photo_keywords pk
                        WHERE pk.photo_id IN ({placeholders})
                          AND pk.keyword_id = ?""",
                    [*chunk, keyword_id],
                ).fetchall()
                result.update(row["photo_id"] for row in rows)
                continue
            rows = self.conn.execute(
                f"""SELECT pk.photo_id, k.name
                    FROM photo_keywords pk
                    JOIN keywords k ON k.id = pk.keyword_id
                    WHERE pk.photo_id IN ({placeholders})
                      AND (k.is_species = 1 OR k.type = 'taxonomy')
                      {excl_clause}""",
                [*chunk, *excluded],
            ).fetchall()
            result.update(
                row["photo_id"] for row in rows
                if keyword_match_key(row["name"]) == target_key
            )
        return result

    def list_all(self):
        """Return keywords used in the active workspace (plus ancestors) with photo counts, type, and taxon info.

        Both counts are grouped aggregates over one materialized pass of the
        workspace's keyword links. ``photo_count`` walks each tagged keyword
        up its parent chain (at most 20 levels) and counts distinct photos
        per ancestor, so a photo tagged with two species under one genus
        counts once for the genus. Correlated per-keyword subqueries here
        re-ran the link scan for every row: about 150 s on a 65k-photo
        workspace, which left the Keywords page empty.
        """
        ws = self.workspace_id
        return self.conn.execute(
            """WITH RECURSIVE
               ws_links AS MATERIALIZED (
                   SELECT pk.keyword_id, pk.photo_id
                   FROM photo_keywords pk
                   JOIN photos p ON p.id = pk.photo_id
                   JOIN photo_workspace_visibility wf ON wf.photo_id = p.id
                   WHERE wf.workspace_id = ?
               ),
               ws_kw AS (SELECT DISTINCT keyword_id AS id FROM ws_links),
               ancestors AS (
                   SELECT id FROM ws_kw
                   UNION
                   SELECT k.parent_id
                   FROM keywords k
                   JOIN ancestors a ON a.id = k.id
                   WHERE k.parent_id IS NOT NULL
               ),
               lineage(keyword_id, ancestor_id, depth) AS (
                   SELECT id, id, 0 FROM ws_kw
                   UNION ALL
                   SELECT l.keyword_id, k.parent_id, l.depth + 1
                   FROM lineage l
                   JOIN keywords k ON k.id = l.ancestor_id
                   WHERE k.parent_id IS NOT NULL AND l.depth < 20
               ),
               lineage_pairs AS (
                   SELECT DISTINCT keyword_id, ancestor_id FROM lineage
               ),
               subtree_counts AS (
                   SELECT lp.ancestor_id AS id,
                          COUNT(DISTINCT wl.photo_id) AS photo_count
                   FROM lineage_pairs lp
                   JOIN ws_links wl ON wl.keyword_id = lp.keyword_id
                   GROUP BY lp.ancestor_id
               ),
               direct_counts AS (
                   SELECT keyword_id AS id, COUNT(*) AS direct_photo_count
                   FROM ws_links
                   GROUP BY keyword_id
               )
               SELECT k.id, k.name, k.parent_id, k.type, k.taxon_id,
                      k.latitude, k.longitude, k.place_id,
                      t.name AS taxon_name, t.common_name AS taxon_common_name,
                      COALESCE(sc.photo_count, 0) AS photo_count,
                      COALESCE(dc.direct_photo_count, 0) AS direct_photo_count
               FROM keywords k
               JOIN ancestors a ON a.id = k.id
               LEFT JOIN taxa t ON t.id = k.taxon_id
               LEFT JOIN subtree_counts sc ON sc.id = k.id
               LEFT JOIN direct_counts dc ON dc.id = k.id
               ORDER BY k.name""",
            (ws,),
        ).fetchall()

    def is_species(self, keyword_id):
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

    def resolve_species_by_lineage(self, species_name, expected_ancestors):
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
            # Depth cap mirrors ``get_class_ancestors_for_taxa`` and guards
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

    def mark_species(self, taxonomy):
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
        preserved so ``get_life_list_candidates`` can keep surfacing the
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
            local_taxon_id = kw["taxon_id"]
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
            # (see :meth:`get_life_list_candidates` and
            # :meth:`get_life_list_locations`), so clearing on startup
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
                continue
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
            updated += 1
        if updated:
            self.conn.commit()
        return updated

    # -- SQL of the façade methods that call ``_merge_keyword_into`` ----------
    #
    # ``Database._merge_duplicate_keywords_pass`` and
    # ``Database.update_keyword`` keep their control flow (and the
    # ``self._merge_keyword_into(...)`` call, which runs the merge in
    # ``repositories/keyword_provenance.py``) in db.py; the statements they
    # issue live here, unchanged.

    def duplicate_scope_rows(self, ws):
        """Keywords tagged on a photo in workspace ``ws``, plus their ancestors.

        The candidate set for one ``merge_duplicate_keywords`` pass.
        """
        return self.conn.execute(
            """WITH RECURSIVE
                   tagged AS (
                       SELECT DISTINCT pk.keyword_id AS id
                       FROM photo_keywords pk
                       JOIN photos p ON p.id = pk.photo_id
                       JOIN photo_workspace_visibility wf ON wf.photo_id = p.id
                       WHERE wf.workspace_id = ?
                   ),
                   in_scope AS (
                       SELECT id FROM tagged
                       UNION
                       SELECT k.parent_id
                       FROM keywords k
                       JOIN in_scope s ON s.id = k.id
                       WHERE k.parent_id IS NOT NULL
                   )
                   SELECT k.id, k.name, k.parent_id, k.type, k.is_species
                   FROM keywords k
                   JOIN in_scope s ON s.id = k.id""",
            (ws,),
        ).fetchall()

    def live_ids(self, all_ids):
        """The subset of ``all_ids`` that still exist as keyword rows."""
        placeholders = ",".join("?" * len(all_ids))
        return {
            row["id"] for row in self.conn.execute(
                f"SELECT id FROM keywords WHERE id IN ({placeholders})",
                all_ids,
            )
        }

    def get_update_target(self, keyword_id):
        """The stored (name, type, taxon_id, parent_id) of a keyword, or None."""
        return self.conn.execute(
            "SELECT name, type, taxon_id, parent_id FROM keywords WHERE id = ?",
            (keyword_id,),
        ).fetchone()

    def same_type_peer(self, new_name, parent_id, effective_type, keyword_id):
        """Another keyword of ``effective_type`` in the same (name, parent) slot."""
        if parent_id is None:
            peer = self.conn.execute(
                "SELECT id FROM keywords "
                "WHERE name = ? COLLATE NOCASE "
                "AND parent_id IS NULL AND type = ? AND id != ? LIMIT 1",
                (new_name, effective_type, keyword_id),
            ).fetchone()
        else:
            peer = self.conn.execute(
                "SELECT id FROM keywords "
                "WHERE name = ? COLLATE NOCASE "
                "AND parent_id = ? AND type = ? AND id != ? LIMIT 1",
                (new_name, parent_id, effective_type, keyword_id),
            ).fetchone()
        return peer

    def cross_type_peer(self, new_name, parent_id, keyword_id):
        """Any other keyword occupying (name, parent_id), whatever its type."""
        return self.conn.execute(
            "SELECT id, type FROM keywords "
            "WHERE name = ? COLLATE NOCASE "
            "AND parent_id = ? AND id != ? LIMIT 1",
            (new_name, parent_id, keyword_id),
        ).fetchone()

    def apply_update(self, keyword_id, updates):
        """Write the resolved ``updates`` column map to one keyword and commit."""
        set_clause = ", ".join(f"{k} = ?" for k in updates)
        values = list(updates.values()) + [keyword_id]
        self.conn.execute(f"UPDATE keywords SET {set_clause} WHERE id = ?", values)
        self.conn.commit()

    # -- species-confirmation reads ------------------------------------------
    #
    # The reads ``/api/encounters/species`` (``web/encounters.py``) runs inside
    # its transaction to decide which attached rows stand for the species a
    # confirmation replaces or removes. A "species row" is ``is_species = 1
    # OR type = 'taxonomy'``.

    def species_root_by_name(self, name, *, prefer_taxonomy=False):
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

    def other_linked_species_names(self, taxon_id, keyword_id):
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

    def previous_species_candidates(self, photo_ids, name, linked_root_taxon):
        """Species-rank rows on ``photo_ids`` that may stand for species ``name``.

        Rows (``id``, ``name``, ``taxon_id``, ``photo_id``) of attached species
        rows whose taxon is species-rank or unknown, named ``name`` (NOCASE),
        or also linked to ``linked_root_taxon`` when that is set. Each
        chunk's rows come root rows first, then by keyword id.
        """
        rows = []
        for chunk in self._chunks(photo_ids):
            placeholders = ",".join("?" for _ in chunk)
            if linked_root_taxon is not None:
                rows.extend(self.conn.execute(
                    f"""SELECT k.id, k.name, k.taxon_id, pk.photo_id
                        FROM photo_keywords pk
                        JOIN keywords k ON k.id = pk.keyword_id
                        LEFT JOIN taxa t ON t.id = k.taxon_id
                        WHERE pk.photo_id IN ({placeholders})
                          AND (k.is_species = 1 OR k.type = 'taxonomy')
                          AND (t.rank = 'species' OR t.rank IS NULL)
                          AND (k.taxon_id = ?
                               OR k.name = ? COLLATE NOCASE)
                        ORDER BY CASE WHEN k.parent_id IS NULL
                                      THEN 0 ELSE 1 END,
                                 k.id""",
                    [*chunk, linked_root_taxon, name],
                ).fetchall())
            else:
                rows.extend(self.conn.execute(
                    f"""SELECT k.id, k.name, k.taxon_id, pk.photo_id
                        FROM photo_keywords pk
                        JOIN keywords k ON k.id = pk.keyword_id
                        LEFT JOIN taxa t ON t.id = k.taxon_id
                        WHERE pk.photo_id IN ({placeholders})
                          AND k.name = ? COLLATE NOCASE
                          AND (k.is_species = 1 OR k.type = 'taxonomy')
                          AND (t.rank = 'species' OR t.rank IS NULL)
                        ORDER BY CASE WHEN k.parent_id IS NULL
                                      THEN 0 ELSE 1 END,
                                 k.id""",
                    [*chunk, name],
                ).fetchall())
        return rows

    def attached_species_rows(self, photo_ids):
        """Every species-rank (or unranked) species row attached to ``photo_ids``.

        Rows (``photo_id``, ``id``, ``name``, ``taxon_id``); each chunk's rows
        come by photo, then root rows first, then by keyword id.
        """
        rows = []
        for chunk in self._chunks(photo_ids):
            placeholders = ",".join("?" for _ in chunk)
            rows.extend(self.conn.execute(
                f"""SELECT pk.photo_id, k.id, k.name, k.taxon_id
                    FROM photo_keywords pk
                    JOIN keywords k ON k.id = pk.keyword_id
                    LEFT JOIN taxa t ON t.id = k.taxon_id
                    WHERE pk.photo_id IN ({placeholders})
                      AND (k.is_species = 1 OR k.type = 'taxonomy')
                      AND (t.rank = 'species' OR t.rank IS NULL)
                    ORDER BY pk.photo_id,
                             CASE WHEN k.parent_id IS NULL THEN 0 ELSE 1 END,
                             k.id""",
                list(chunk),
            ).fetchall())
        return rows

    def commit(self):
        """Commit the connection's open transaction."""
        self.conn.commit()
