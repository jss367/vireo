"""Persistence for keywords: rows, tagging reads, species names, the sweeps.

This module owns the SQL behind the keyword domain:

- keyword rows: ``add`` (with case conventions, taxon linking and the
  source-taxon species path), the default genre seed, legacy type
  migration, the keyword tree and listing reads, counts, and the rename /
  retype statements ``Database.update_keyword`` issues;
- tagging reads (per photo, per batch, species-rank names, equivalent
  species) and ``untag``;
- species-name resolution (``resolve_species_display``, taxon lookup,
  case-convention detection, lineage resolution, species marking);
- the one-shot normalization and repair sweeps (``normalize_data``,
  ``repair_duplicate_photo_species``, the curation case alignment, the
  prediction apostrophe fold and its merge helpers), and the scope and
  liveness reads of ``Database._merge_duplicate_keywords_pass``.

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
in the middle of their own work (``_upsert_one_keyword``,
``_normalize_keyword_data_once``). This module writes no ``photo_keywords``
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
  (``queue_change``, ``get_meta`` / ``set_meta``, the species-curation
  renames) and this domain's own entry points that other methods compose
  with.
- The methods with no SQL of their own (``_apply_case_convention``,
  ``_sentence_case_first_word``, ``species_case_convention``,
  ``_canonical_curation_species``).

``_commit`` flags are carried through unchanged: ``_commit=False`` means the
caller owns the transaction, and no method here commits unless the
``Database`` method it backs did.
"""

import json
import sqlite3

from keyword_identity import identity_sql
from keyword_normalization import keyword_match_key, normalize_keyword_display

# ``Database`` methods the moved bodies call through the façade.
FACADE_METHODS = (
    "get_meta",
    "set_meta",
    "queue_change",
    "rename_photo_preferences_species",
    "rename_species_highlights_species",
    "rename_species_representatives_species",
    "species_case_convention",
    "_apply_case_convention",
    "_canonical_curation_species",
    "_merge_duplicate_keywords_pass",
    "_normalize_keyword_data_once",
    "resolve_species_display_name",
    "detect_keyword_case_convention",
    "_lookup_taxon_id_for_keyword",
    "_add_source_species_keyword",
    "_rename_keyword_dependents",
    "_align_curation_species_case",
    "_align_curation_history_species",
    "_fold_prediction_species_apostrophes",
    "_merge_prediction_metadata_before_delete",
    "_merge_prediction_review_before_delete",
    "_retarget_prediction_edit_history",
    "_species_keyword_maps",
    "_resolve_species_by_lineage",
)


class KeywordRepository:
    def __init__(
        self,
        conn,
        resolve_workspace_id,
        *,
        chunks,
        log,
        keyword_types,
        auto_match_review_marker,
        detect_case_convention_sentinel,
        taxon_lookup_variants,
        resolve_import_alias,
        filter_subject_chunk,
        duplicate_photo_species_repair_key,
        facade,
    ):
        self.conn = conn
        self._resolve_workspace_id = resolve_workspace_id
        # ``db`` module helpers and constants, kept under their module names
        # so the moved bodies read them as ``self.<name>``. ``_chunks`` is
        # ``db._chunks`` itself (its size default is bound at import) and
        # ``log`` is the ``db`` logger, so log records keep its name.
        self._chunks = chunks
        self.log = log
        self.KEYWORD_TYPES = keyword_types
        self.AUTO_MATCH_REVIEW_MARKER = auto_match_review_marker
        self._DETECT_CASE_CONVENTION = detect_case_convention_sentinel
        self._taxon_lookup_variants = taxon_lookup_variants
        self.resolve_import_alias = resolve_import_alias
        # ``Database`` class attributes, kept under their class names.
        self._FILTER_SUBJECT_CHUNK = filter_subject_chunk
        self._DUPLICATE_PHOTO_SPECIES_REPAIR_KEY = duplicate_photo_species_repair_key
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

    def migrate_legacy_types(self):
        """One-shot migration of legacy keyword type names to the canonical
        enum. Idempotent — once all rows are migrated, the warm-path
        short-circuits cheaply (single SELECT 1 LIMIT 1) so this is safe to
        call from Database.__init__ on every instantiation.

        This runs before default genre seeding so old rows settle onto the
        canonical enum before same-name defaults are reconciled.
        """
        legacy = self.conn.execute(
            "SELECT 1 FROM keywords WHERE type IN ('people', 'descriptive', 'event') LIMIT 1"
        ).fetchone()
        if not legacy:
            return
        self.conn.execute("UPDATE keywords SET type = 'individual' WHERE type = 'people'")
        self.conn.execute("UPDATE keywords SET type = 'general' WHERE type = 'descriptive'")
        self.conn.execute("UPDATE keywords SET type = 'general' WHERE type = 'event'")
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
               JOIN workspace_folders wf ON wf.folder_id = p.folder_id
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
               JOIN workspace_folders wf ON wf.folder_id = p.folder_id
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
                JOIN workspace_folders wf ON wf.folder_id = p.folder_id
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
        ``repair_duplicate_photo_species`` detached the top-level
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

    def normalize_row_name(self, keyword_id, disambiguate_on_conflict=False):
        """Trim stray edge punctuation from a surviving keyword row name.

        Post-migration, stored names are already normalized, so this is a
        no-op in the common case — it exists so the duplicate-cleanup and
        migration paths can canonicalize a survivor whose spelling predates
        normalization. ``_rename_keyword_dependents`` then carries the new
        spelling into every string that mirrors it.

        ``disambiguate_on_conflict`` — when a different-type keyword already
        occupies (cleaned, parent_id) and the UPDATE would hit
        ``UNIQUE(name, parent_id)``, retry with a ``<cleaned> (id-<id>)``
        suffix so no stored variant survives. Used by the one-shot
        migration so its completion marker can honestly assert the "no
        stored variant" invariant; the runtime dedup path leaves this
        False and keeps the stored spelling in the collision case.
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
        # keep the stored spelling unchanged in that case, unless the
        # caller opts into disambiguation (migration path).
        try:
            self.conn.execute(
                "UPDATE keywords SET name = ? WHERE id = ?", (cleaned, keyword_id)
            )
        except sqlite3.IntegrityError:
            if not disambiguate_on_conflict:
                return
            # Fallback: append an id suffix so the row's name is still in
            # normalize_keyword_display() form (the parenthesized suffix is
            # ASCII and idempotent under the strip) while sidestepping the
            # UNIQUE(name, parent_id) slot the different-type peer holds.
            # The retarget below runs against this disambiguated name so
            # pending sidecar changes and species curation stay in lockstep
            # with the row's stored spelling.
            cleaned = f"{cleaned} (id-{keyword_id})"
            self.conn.execute(
                "UPDATE keywords SET name = ? WHERE id = ?", (cleaned, keyword_id)
            )
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
               JOIN workspace_folders wf ON wf.folder_id = p.folder_id
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

    def normalize_data(self):
        """One-shot, db_meta-gated wrapper around the normalization backfill.

        Runs at most once per database (``db_meta['keyword_names_normalized']``).
        All-or-nothing: an exception rolls the whole sweep back — including
        the marker — so a failed run retries on the next open instead of
        leaving a half-normalized keyword table.
        """
        if self.get_meta("keyword_names_normalized") != "1":
            try:
                self._normalize_keyword_data_once()
                self.set_meta("keyword_names_normalized", "1", _commit=False)
                self.conn.commit()
            except Exception:
                self.conn.rollback()
                raise
        # Second-generation curation case alignment. The v1 sweep above ran
        # on databases before curation writes canonicalized their species
        # input, so highlight/preference rows starred from prediction-cased
        # bucket labels between the v1 run and that fix are still keyed with
        # classifier casing (e.g. `Common Waxbill` vs the `Common waxbill`
        # keyword). Re-run just the case-alignment pass once under its own
        # marker; with the setters now canonicalizing on write, new
        # mismatches cannot form afterwards.
        if self.get_meta("curation_species_case_aligned_v2") != "1":
            try:
                aligned = self._align_curation_species_case()
                # Edit-history snapshots created after v1 but before the
                # setter canonicalization fix still carry prediction-cased
                # species in hl_prev/pref_prev/rep_prev; without this a
                # later undo would recreate the orphaned curation rows v2
                # is meant to repair.
                history_aligned = self._align_curation_history_species()
                if aligned or history_aligned:
                    self.log.info(
                        "curation case alignment v2: moved %d row(s), "
                        "rewrote %d history item(s)",
                        aligned, history_aligned,
                    )
                self.set_meta(
                    "curation_species_case_aligned_v2", "1", _commit=False
                )
                self.conn.commit()
            except Exception:
                self.conn.rollback()
                raise
        # Third-generation sweep: typographic-apostrophe folding.
        # normalize_keyword_display() previously kept an internal U+2019, so
        # `Say’s phoebe` was stored as a row distinct from `Say's phoebe`
        # under SQLite COLLATE NOCASE -- one bird, two keyword rows, two
        # Life List cards, two lifer numbers. Now that the fold happens on
        # write, re-run the whole normalization sweep under its own marker
        # to merge the variants that already exist, and fold the same
        # characters in predictions.species (joined to keywords.name with
        # `COLLATE NOCASE`, so a curly prediction silently fails to match
        # its accepted ASCII keyword).
        if self.get_meta("keyword_apostrophes_folded_v1") != "1":
            try:
                self._normalize_keyword_data_once()
                folded = self._fold_prediction_species_apostrophes()
                if folded:
                    self.log.info(
                        "apostrophe fold: rewrote %d prediction species", folded
                    )
                self.set_meta(
                    "keyword_apostrophes_folded_v1", "1", _commit=False
                )
                self.conn.commit()
            except Exception:
                self.conn.rollback()
                raise

    def fold_prediction_species_apostrophes(self):
        """Rewrite ``predictions.species`` into normalize_keyword_display form.

        Predictions are compared against ``keywords.name`` with exact and
        ``COLLATE NOCASE`` matches (see the predicted-species subqueries in
        :meth:`get_photos`), neither of which can fold U+2019. A prediction
        stored as ``Swinhoe’s White-eye`` therefore never matched the
        accepted ``Swinhoe's white-eye`` keyword, so the photo's own
        prediction looked unaccepted in the UI.

        ``predictions`` has UNIQUE(detection_id, classifier_model,
        labels_fingerprint, species); the constraint is BINARY, so a single
        (detection, model, fingerprint) scope can legally hold three
        NOCASE-equivalent variants at once (e.g. ``Say's Phoebe``,
        ``Say's phoebe``, ``Say’s phoebe``). A per-row ``fetchone`` peer
        lookup would only merge one of the ASCII neighbours: with the
        curly row winning by confidence, the subsequent
        ``UPDATE ... SET species = 'Say's phoebe'`` would then collide with
        the unmerged ASCII-lowercase row and abort the whole migration
        under UNIQUE — and on every open thereafter. Fetch every
        NOCASE-equivalent peer up-front and merge the entire collision set
        around a single winner so the final UPDATE has no peer left to
        clash with.

        Before deleting any loser, migrate its workspace review rows onto
        the surviving prediction so an accepted/rejected decision on a
        variant (``prediction_review.prediction_id`` uses
        ``ON DELETE CASCADE``) is not silently lost, and retarget any
        ``prediction_accept`` edit-history references from the loser id
        to the winner id so undo/redo can still find the prediction after
        the DELETE.
        """
        folded = 0
        processed_ids = set()
        for row in self.conn.execute(
            "SELECT id, detection_id, classifier_model, labels_fingerprint, "
            "species, confidence FROM predictions WHERE species IS NOT NULL"
        ).fetchall():
            if row["id"] in processed_ids:
                # Already merged (or deleted) as a peer of an earlier
                # collision group; skip so we don't reprocess a stale row.
                continue
            clean = normalize_keyword_display(row["species"])
            if clean == row["species"]:
                continue
            if not clean:
                # A prediction whose label is pure stray punctuation has no
                # canonical spelling to match; leave it rather than inventing
                # an empty species that would join to nothing.
                continue
            # ``COLLATE NOCASE`` matches SQLite's ASCII case fold, which is
            # the same equivalence downstream keyword joins already use. Without
            # it, a DB carrying `Say's Phoebe` (ASCII, title-case) alongside a
            # `Say’s phoebe` (curly, lowercase) misses the collision, folds only
            # the curly row, and leaves two case-variant predictions on the same
            # (detection, model, fingerprint) — preserving the split review state
            # and duplicate rendering the fold was meant to erase.
            #
            # ``fetchall`` (not ``fetchone``) so a three-way collision such as
            # ``Say's Phoebe`` + ``Say's phoebe`` + ``Say’s phoebe`` is merged
            # in one pass: leaving even one NOCASE-equivalent ASCII peer would
            # let the winner's final ``UPDATE ... SET species = clean`` collide
            # with it and abort the migration under UNIQUE.
            peers = self.conn.execute(
                "SELECT id, confidence, species FROM predictions "
                "WHERE detection_id = ? AND classifier_model = ? "
                "AND labels_fingerprint = ? "
                "AND species = ? COLLATE NOCASE AND id != ?",
                (row["detection_id"], row["classifier_model"],
                 row["labels_fingerprint"], clean, row["id"]),
            ).fetchall()
            if peers:
                # Whole collision set: this row plus every NOCASE-equivalent
                # peer. Pick the winner by highest confidence, tie-broken by
                # (a) already-clean species (avoids a needless UPDATE and
                # preserves the reviewer's chosen casing) then (b) lowest
                # id for determinism.
                candidates = [
                    (row["id"], row["confidence"] or 0.0, row["species"]),
                ]
                for p in peers:
                    candidates.append(
                        (p["id"], p["confidence"] or 0.0, p["species"])
                    )
                winner_id, _, winner_species = max(
                    candidates,
                    key=lambda c: (
                        c[1],
                        normalize_keyword_display(c[2]) == c[2],
                        -c[0],
                    ),
                )
                for cid, _, _ in candidates:
                    if cid == winner_id:
                        continue
                    self._merge_prediction_metadata_before_delete(
                        loser_id=cid, winner_id=winner_id,
                    )
                    self._merge_prediction_review_before_delete(
                        loser_id=cid, winner_id=winner_id,
                    )
                    self._retarget_prediction_edit_history(
                        loser_id=cid, winner_id=winner_id,
                    )
                    self.conn.execute(
                        "DELETE FROM predictions WHERE id = ?", (cid,)
                    )
                    processed_ids.add(cid)
                # Only rewrite the survivor when it still carries curly
                # punctuation; an already-clean winner keeps its casing.
                if normalize_keyword_display(winner_species) != winner_species:
                    self.conn.execute(
                        "UPDATE predictions SET species = ? WHERE id = ?",
                        (clean, winner_id),
                    )
                processed_ids.add(winner_id)
            else:
                self.conn.execute(
                    "UPDATE predictions SET species = ? WHERE id = ?",
                    (clean, row["id"]),
                )
                processed_ids.add(row["id"])
            folded += 1
        return folded

    def merge_prediction_review_before_delete(self, loser_id, winner_id):
        """Move per-workspace review rows from ``loser_id`` onto ``winner_id``.

        ``prediction_review.prediction_id`` is ``ON DELETE CASCADE``, so a
        bare ``DELETE FROM predictions`` silently drops any accepted /
        rejected user decision (and its group metadata) attached to the
        losing row.  Called from ``_fold_prediction_species_apostrophes``
        just before it deletes a duplicate: the two predictions differ only
        in spelling, so a review on either applies to the same (detection,
        species) pair and must survive the collision-merge.

        For each ``(loser_id, workspace_id)`` review row:

        - If the winner has no row for that workspace, absence encodes an
          implicit ``pending`` state, so treat it as a real row rather than
          a slot to be filled. Move the loser's row onto the winner only
          when the loser carries a genuine user decision
          (``accepted``/``rejected``) or non-status metadata (``group_id``
          etc.) worth preserving. A bare ``status='alternative'`` row on
          the loser is dropped instead: transferring it would turn a
          higher-confidence pending primary into an alternative, hiding
          the sole top-1 prediction from the pending queue.  When the
          decided loser IS itself the auto-accepted taxonomy match (its
          ``individual`` carries ``AUTO_MATCH_REVIEW_MARKER``), the marker
          is preserved on the transfer: it's the provenance
          ``reconcile_match_review_state`` uses to delete the row later
          if the XMP match goes away; scrubbing it would leave a stale
          auto-accept looking like a manual decision no automation can
          revisit. A pending-loser transfer still scrubs the marker
          (a non-decided row carrying it is spurious historical state,
          not a real auto-accept).
        - If the winner already has a row for that workspace (both were
          reviewed independently), keep whichever encodes the stronger
          decision: a non-pending status beats pending, and among two
          non-pending decisions the later ``reviewed_at`` wins.  Ties keep
          the winner's row so the choice stays deterministic.
        - Independently of the status choice, backfill missing group
          metadata (``group_id`` / ``vote_count`` / ``total_votes`` /
          ``individual``) from whichever side carries it: a pending loser
          that carries the current burst's ``group_id`` while the winner is
          also pending would otherwise be cascaded away and the surviving
          prediction would silently drop out of its burst group. This is
          safe because two rows for the same (detection, species) pair
          across spelling variants are always about the same burst.
          ``individual`` is filled only when the source value is not the
          ``AUTO_MATCH_REVIEW_MARKER`` sentinel; that string is provenance
          for auto-accepted taxonomy matches, and copying it onto a
          manually chosen accept/reject would let later automation
          (``preserve_manual_review`` / ``reconcile_match_review_state``)
          overwrite or delete the user's decision.

        The loser's remaining rows are removed by the caller's DELETE via
        the ON DELETE CASCADE, so no explicit cleanup is needed here.
        """
        loser_rows = self.conn.execute(
            "SELECT workspace_id, status, reviewed_at, individual, group_id, "
            "vote_count, total_votes FROM prediction_review "
            "WHERE prediction_id = ?",
            (loser_id,),
        ).fetchall()
        for lr in loser_rows:
            ws = lr["workspace_id"]
            winner = self.conn.execute(
                "SELECT status, reviewed_at, individual, group_id, "
                "vote_count, total_votes FROM prediction_review "
                "WHERE prediction_id = ? AND workspace_id = ?",
                (winner_id, ws),
            ).fetchone()
            loser_status = lr["status"] or "pending"
            loser_decided = loser_status in ("accepted", "rejected")
            # ``AUTO_MATCH_REVIEW_MARKER`` in ``individual`` is a
            # taxonomy-match sentinel, not user-authored metadata worth
            # preserving. Counting it here would let a bare
            # ``status='alternative'`` loser row whose only "metadata" is
            # the sentinel pass the guard below and flip the pending
            # winner to ``alternative``.
            loser_metadata = {
                col: lr[col]
                for col in ("group_id", "vote_count", "total_votes", "individual")
                if lr[col] is not None
                and not (
                    col == "individual" and lr[col] == self.AUTO_MATCH_REVIEW_MARKER
                )
            }
            if winner is None:
                # Winner's absence == implicit pending. Overriding that
                # with the loser's row makes sense only when the loser
                # carries a real user decision or preserved metadata; a
                # bare ``status='alternative'`` transfer would flip the
                # higher-confidence pending primary into an alternative
                # and hide the sole top-1 prediction from the pending
                # queue.
                if not loser_decided and not loser_metadata:
                    continue
                if loser_decided:
                    # Move the row intact, ``individual`` included: the
                    # loser IS the decision, not a competing manual row
                    # the sentinel could pollute. Preserving
                    # ``AUTO_MATCH_REVIEW_MARKER`` when it's set is what
                    # lets ``reconcile_match_review_state`` recognize an
                    # auto-accept later and clean it up if the XMP match
                    # goes away; scrubbing it here would strand the
                    # accept as an apparent manual decision no automation
                    # can revisit.
                    self.conn.execute(
                        "UPDATE prediction_review SET prediction_id = ? "
                        "WHERE prediction_id = ? AND workspace_id = ?",
                        (winner_id, loser_id, ws),
                    )
                else:
                    # Undecided loser with real burst metadata: keep the
                    # metadata but leave the winner implicit-pending by
                    # downgrading the transferred status. ``individual``
                    # on a pending row can be the JSON vote breakdown
                    # ``_store_grouped_predictions`` stores alongside
                    # ``group_id`` / ``vote_count`` / ``total_votes``,
                    # so preserve it verbatim; only the auto-match
                    # sentinel gets scrubbed so future automation
                    # doesn't misread provenance on a row the user has
                    # never reviewed.
                    scrubbed_individual = (
                        None
                        if lr["individual"] == self.AUTO_MATCH_REVIEW_MARKER
                        else lr["individual"]
                    )
                    self.conn.execute(
                        "UPDATE prediction_review "
                        "SET prediction_id = ?, status = 'pending', "
                        "    individual = ? "
                        "WHERE prediction_id = ? AND workspace_id = ?",
                        (winner_id, scrubbed_individual, loser_id, ws),
                    )
                continue
            winner_status = winner["status"] or "pending"
            winner_decided = winner_status in ("accepted", "rejected")
            prefer_loser = loser_decided and (
                not winner_decided
                or (lr["reviewed_at"] or "") > (winner["reviewed_at"] or "")
            )
            chosen = lr if prefer_loser else winner
            other = winner if prefer_loser else lr
            # ``prefer_loser`` only reflects the accepted/rejected decision,
            # so a pending winner whose row lacks ``group_id`` while a
            # pending loser carries the current burst's grouping would lose
            # it via CASCADE without this backfill. Symmetric across sides:
            # whichever side supplied the decision, missing group metadata
            # is filled from the other so the surviving prediction stays
            # inside its burst group. ``individual`` intentionally skips
            # the ``AUTO_MATCH_REVIEW_MARKER`` fill-in: the chosen row is
            # the surviving decision (a manual accept/reject may store
            # ``individual=NULL``), and copying the auto-match sentinel
            # from a stale auto-accepted row would let later runs of
            # ``reconcile_match_review_state`` delete the user's decision
            # or let ``preserve_manual_review`` overwrite it.
            other_individual = other["individual"]
            if other_individual == self.AUTO_MATCH_REVIEW_MARKER:
                other_individual = None
            self.conn.execute(
                """UPDATE prediction_review
                   SET status = ?, reviewed_at = ?, individual = ?,
                       group_id = ?, vote_count = ?, total_votes = ?
                   WHERE prediction_id = ? AND workspace_id = ?""",
                (chosen["status"], chosen["reviewed_at"],
                 chosen["individual"] if chosen["individual"] is not None
                 else other_individual,
                 chosen["group_id"] if chosen["group_id"] is not None
                 else other["group_id"],
                 chosen["vote_count"] if chosen["vote_count"] is not None
                 else other["vote_count"],
                 chosen["total_votes"] if chosen["total_votes"] is not None
                 else other["total_votes"],
                 winner_id, ws),
            )

    def merge_prediction_metadata_before_delete(self, loser_id, winner_id):
        """Backfill non-null loser columns onto the winner before DELETE.

        Two colliding predictions (same detection / model / fingerprint,
        spellings that differ only by apostrophe) can hold different amounts
        of enrichment if they were written by different code paths: the
        classifier-with-taxonomy path fills ``category`` / ``scientific_name``
        / ``taxonomy_*``, but a raw-classifier path (or an older insert made
        before the taxonomy lookup existed) can leave those NULL.  When the
        row selected as the winner (by ``confidence``) happens to be the
        one without the enrichment, deleting the loser strips fields that
        taxonomy filters and review displays rely on.

        Backfills a column only when the winner is currently NULL, so a
        deliberate override on the winner is preserved.  ``category`` also
        promotes from the schema default ``'new'`` to a more specific
        ``'match'`` / ``'change'`` when only the loser carried it, but
        never overrides an explicit non-default winner category.  Called
        from ``_fold_prediction_species_apostrophes`` right before the
        CASCADEd DELETE removes the loser row.
        """
        if loser_id == winner_id:
            return
        winner = self.conn.execute(
            """SELECT category, scientific_name,
                      taxonomy_kingdom, taxonomy_phylum, taxonomy_class,
                      taxonomy_order, taxonomy_family, taxonomy_genus
               FROM predictions WHERE id = ?""",
            (winner_id,),
        ).fetchone()
        loser = self.conn.execute(
            """SELECT category, scientific_name,
                      taxonomy_kingdom, taxonomy_phylum, taxonomy_class,
                      taxonomy_order, taxonomy_family, taxonomy_genus
               FROM predictions WHERE id = ?""",
            (loser_id,),
        ).fetchone()
        if winner is None or loser is None:
            return
        updates = []
        values = []
        winner_cat = winner["category"]
        loser_cat = loser["category"]
        if (
            loser_cat
            and loser_cat != "new"
            and (winner_cat is None or winner_cat == "new")
        ):
            updates.append("category = ?")
            values.append(loser_cat)
        for field in (
            "scientific_name",
            "taxonomy_kingdom",
            "taxonomy_phylum",
            "taxonomy_class",
            "taxonomy_order",
            "taxonomy_family",
            "taxonomy_genus",
        ):
            if winner[field] is None and loser[field] is not None:
                updates.append(f"{field} = ?")
                values.append(loser[field])
        if updates:
            values.append(winner_id)
            self.conn.execute(
                "UPDATE predictions SET " + ", ".join(updates) + " WHERE id = ?",
                values,
            )

    def retarget_prediction_edit_history(self, loser_id, winner_id):
        """Rewrite prediction-id references in edit history from loser to winner.

        Three action types anchor prediction ids into
        ``edit_history_items.old_value``:

        - ``prediction_accept`` (``api_accept_prediction`` /
          ``api_accept_subject_species``): stores either a bare-int string
          (single-model, changed-tag accept), JSON
          ``{"prediction_id": N, "no_tag": true}`` (single no-op accept),
          or JSON ``{"prediction_ids": [N, ...], "no_tag"?: true}``
          (accept-subject collecting agreeing sibling classifier models).
        - ``keyword_add`` and ``species_replace``
          (``api_highlights_relabel``): store a JSON payload whose
          ``prediction_id`` field points at the top prediction captured
          when the relabel ran, so undo can restore its ``pending`` status
          via ``_restore_edit_prediction_status`` (and redo can re-reject
          it via ``_reject_edit_prediction``). Bare-int ``old_value`` for
          these two action types encodes the previous keyword id, not a
          prediction id, so it is left alone.

        When the fold migration deletes a colliding prediction row, an
        undo/redo later would call ``update_prediction_status(loser_id,
        ...)`` on the vanished id: the ``INSERT`` into ``prediction_review``
        then fails the FK to ``predictions``, aborting the undo/redo, and
        for the ``prediction_accept`` accept-subject variant the missing
        id would silently be skipped by ``_apply_undo`` so the surviving
        prediction stays anchored in its accepted state.

        Retargeting the reference from ``loser_id`` -> ``winner_id`` before
        the DELETE keeps undo/redo sound: the two predictions differ only
        in spelling, so any status flip captured on either applies to the
        same (detection, model, labels_fingerprint) scope after the merge.
        """
        if loser_id == winner_id:
            return
        loser_str = str(loser_id)
        winner_str = str(winner_id)
        # (1) Bare-int old_value: rewrite in place.  Scoped strictly to
        #     ``prediction_accept`` because ``keyword_add`` /
        #     ``species_replace`` / ``keyword_remove`` store keyword ids
        #     in these columns and a blanket UPDATE would corrupt any
        #     keyword id that happened to equal loser_id.
        self.conn.execute(
            """UPDATE edit_history_items
               SET old_value = ?
               WHERE old_value = ?
                 AND edit_id IN (
                     SELECT id FROM edit_history
                     WHERE action_type = 'prediction_accept'
                 )""",
            (winner_str, loser_str),
        )
        # (2) JSON old_value: parse, rewrite ``prediction_id`` and every
        #     ``prediction_ids`` entry that matches the loser, re-serialize.
        #     The ``LIKE '{%'`` prefix skips the bare-int rows handled above
        #     without loading them into Python. Scoped to the three action
        #     types that carry prediction ids in their JSON payload so a
        #     ``keyword_remove`` JSON blob (which happens to also start with
        #     ``{`` if it ever gains structured payloads) is unaffected.
        json_rows = self.conn.execute(
            """SELECT ehi.id, ehi.old_value
               FROM edit_history_items ehi
               JOIN edit_history eh ON eh.id = ehi.edit_id
               WHERE eh.action_type IN (
                       'prediction_accept', 'keyword_add', 'species_replace'
                     )
                 AND ehi.old_value IS NOT NULL
                 AND ehi.old_value LIKE ?""",
            ('{%',),
        ).fetchall()
        for row in json_rows:
            try:
                data = json.loads(row["old_value"])
            except (TypeError, ValueError):
                continue
            if not isinstance(data, dict):
                continue
            changed = False
            raw_pid = data.get("prediction_id")
            if raw_pid is not None:
                try:
                    if int(raw_pid) == loser_id:
                        data["prediction_id"] = winner_id
                        changed = True
                except (TypeError, ValueError):
                    pass
            raw_pids = data.get("prediction_ids")
            if isinstance(raw_pids, list):
                new_list = []
                list_changed = False
                for raw in raw_pids:
                    try:
                        if int(raw) == loser_id:
                            new_list.append(winner_id)
                            list_changed = True
                            continue
                    except (TypeError, ValueError):
                        pass
                    new_list.append(raw)
                if list_changed:
                    data["prediction_ids"] = new_list
                    changed = True
            if changed:
                self.conn.execute(
                    "UPDATE edit_history_items SET old_value = ? WHERE id = ?",
                    (json.dumps(data), row["id"]),
                )

    def align_curation_species_case(self):
        """Re-key curation rows whose species differs from the canonical
        spelling only by case.

        ``normalize_keyword_display()`` preserves case, so the punctuation
        sweep leaves a curation row keyed ``Saffron Finch`` untouched while
        the species keyword row is ``Saffron finch`` — and the eligible
        highlight/life-list queries compare those strings EXACT against
        ``keywords.name``, so the curated selection silently drops out.

        Two sources of the canonical spelling, mirroring
        ``resolve_species_display_name`` (the function
        ``collect_highlight_buckets`` uses to canonicalize prediction
        labels):

        1. A single surviving root species keyword for the match_key.
           Intentionally-distinct same-key homonyms (e.g. a legacy
           ``type='general', is_species=1`` ``Robin`` alongside a taxonomy
           ``robin``) must not have every curation row for the other
           spelling rewritten onto the picked one — the joined queries
           would then match a species keyword the photo doesn't carry.
           Ambiguous case-variant homonyms are left as-is.
        2. If no keyword row exists at all — e.g. a highlight starred from
           an unconfirmed prediction bucket before the photo was accepted
           — apply the detected case convention so the row lands on the
           string the bucket will produce after this migration. Without
           this, the bucket-side canonicalization drifts to (say)
           ``Common waxbill`` while the highlight stays at
           ``Common Waxbill``, silently un-starring the photo.

        Returns the number of rows moved; caller commits.
        """
        moved = 0
        unique_species_by_key, all_species_keys = self._species_keyword_maps()
        for table, rename in (
            ("photo_preferences", self.rename_photo_preferences_species),
            ("species_representatives",
             self.rename_species_representatives_species),
            ("species_highlights", self.rename_species_highlights_species),
        ):
            names = [
                r["species"] for r in self.conn.execute(
                    f"SELECT DISTINCT species FROM {table}"
                ).fetchall()
            ]
            for old in names:
                stored = self._canonical_curation_species(
                    old, unique_species_by_key, all_species_keys
                )
                if not stored or stored == old:
                    continue
                moved += rename(old, stored, _commit=False) or 0
                self.conn.execute(
                    f"DELETE FROM {table} WHERE species = ?", (old,)
                )
        return moved

    def align_curation_history_species(self):
        """Rewrite curation species snapshots in edit_history_items.old_value.

        Relabel undo/redo payloads carry snapshots of curation rows keyed
        by species name. Normalizing only the live tables leaves those
        JSON snapshots pointing at the legacy spelling, so a later undo
        would recreate orphaned curation rows that no longer compare
        equal to the string the bucket / eligibility queries expect.
        Route every species value captured by hl_prev/pref_prev/rep_prev
        through the same canonicalization ``_align_curation_species_case``
        applies to the live tables (unambiguous stored spelling, ambiguous
        homonyms left alone, no-keyword predictions case-converted).
        Idempotent on already-normalized rows, so it's safe to re-run in
        the v2 gate after v1 has already normalized the punctuation.
        Returns the number of history rows rewritten; caller commits.
        """
        rewritten = 0
        unique_species_by_key, all_species_keys = self._species_keyword_maps()

        def _normalized_curation_species(value):
            return self._canonical_curation_species(
                value, unique_species_by_key, all_species_keys
            )

        history_rows = self.conn.execute(
            "SELECT id, old_value FROM edit_history_items "
            "WHERE old_value IS NOT NULL AND old_value LIKE ?",
            ('{%curation%',),
        ).fetchall()
        for row in history_rows:
            try:
                payload = json.loads(row["old_value"])
            except (TypeError, ValueError):
                continue
            if not isinstance(payload, dict):
                continue
            curation = payload.get("curation")
            if not isinstance(curation, dict):
                continue
            dirty = False
            highlights = curation.get("hl_prev")
            if isinstance(highlights, list):
                for index, entry in enumerate(highlights):
                    if isinstance(entry, str):
                        normalized = _normalized_curation_species(entry)
                        if normalized != entry:
                            highlights[index] = normalized
                            dirty = True
                    elif isinstance(entry, dict):
                        old = entry.get("species")
                        if isinstance(old, str):
                            normalized = _normalized_curation_species(old)
                            if normalized != old:
                                entry["species"] = normalized
                                dirty = True
            for key in ("pref_prev", "rep_prev"):
                entries = curation.get(key)
                if not isinstance(entries, list):
                    continue
                for entry in entries:
                    if not isinstance(entry, dict):
                        continue
                    old = entry.get("species")
                    if not isinstance(old, str):
                        continue
                    normalized = _normalized_curation_species(old)
                    if normalized != old:
                        entry["species"] = normalized
                        dirty = True
            if dirty:
                self.conn.execute(
                    "UPDATE edit_history_items SET old_value = ? WHERE id = ?",
                    (json.dumps(payload, sort_keys=True), row["id"]),
                )
                rewritten += 1
        return rewritten

    def species_maps(self):
        """Return ``(unique_species_by_key, all_species_keys)`` for
        curation alignment.

        ``unique_species_by_key``: match_key → stored root species
        spelling, ONLY for keys resolving to a single distinct spelling.
        Homonyms (multiple distinct spellings for the same key) are
        omitted so callers can't rewrite curation across genuinely
        different keyword rows.

        ``all_species_keys``: set of match_keys with any root species
        keyword row (ambiguous or not). Used to distinguish "no keyword
        row at all" — safe to canonicalize a curation species via the
        detected case convention — from "ambiguous homonym", which
        must be left alone.
        """
        species_by_key = {}
        for row in self.conn.execute(
            "SELECT name FROM keywords "
            "WHERE parent_id IS NULL AND (is_species = 1 OR type = 'taxonomy')"
        ).fetchall():
            species_by_key.setdefault(keyword_match_key(row["name"]), []).append(
                row["name"]
            )
        unique = {
            key: names[0] for key, names in species_by_key.items()
            if len(set(names)) == 1
        }
        return unique, set(species_by_key.keys())

    def has_possible_duplicate_photo_species(self):
        """Whether any photo carries a typed species/taxonomy tag beside
        another tag.

        The legacy bug ``repair_duplicate_photo_species`` cleans up always
        left such a pair, so ``False`` means the repair cannot find anything
        and its one-shot marker is safe to stamp without parsing taxonomy.
        """
        return self.conn.execute(
            """SELECT 1
               FROM photo_keywords species_pk
               JOIN keywords species_k
                 ON species_k.id = species_pk.keyword_id
               WHERE (species_k.is_species = 1
                      OR species_k.type = 'taxonomy')
                 AND EXISTS (
                     SELECT 1
                     FROM photo_keywords other_pk
                     WHERE other_pk.photo_id = species_pk.photo_id
                       AND other_pk.keyword_id != species_pk.keyword_id
                 )
               LIMIT 1"""
        ).fetchone() is not None

    def repair_duplicate_photo_species(self):
        """Remove redundant same-photo associations for one species taxon.

        Older imports preserved Lightroom hierarchy leaves, while later
        species confirmations attached a second top-level keyword row. Both
        rows are useful globally, but one photo should not carry both for the
        same species-rank taxon. Remove only top-level associations when at
        least one hierarchy-bearing association exists; multiple deliberate
        hierarchy placements remain intact. Leave photo-scoped curation on
        the root spelling because that keyword row remains the canonical
        species key, and leave all keyword rows intact.

        Pending keyword changes are name-based rather than keyword-id-based.
        Once the hierarchy association survives, a queued remove for either
        spelling would incorrectly erase the surviving XMP keyword. Cancel
        matching adds/removes; the hierarchical association originated from
        that sidecar and remains the source of truth.

        When a detached root's spelling does not survive on the photo (for
        example a root ``Verdin`` is detached because a hierarchical alias
        ``Birds|Desert Verdin`` is kept), the sidecar's previously synced
        ``dc:subject: Verdin`` still names a keyword the DB no longer
        carries. Left alone, the next XMP-to-DB scan would flat-import
        ``Verdin`` and re-attach the top-level row this repair just
        removed. Queue a ``keyword_remove`` for those orphaned spellings
        so ``sync_to_xmp`` clears them from the sidecar; skip the queue
        when a surviving row (species or general, hierarchical or not)
        already carries the same normalized name, since the scanner's
        per-photo dedup keeps the flat entry from re-tagging in that case
        and a hierarchical remove would strip the surviving keyword.
        """
        if self.get_meta(self._DUPLICATE_PHOTO_SPECIES_REPAIR_KEY) == "1":
            return 0
        if self.conn.execute(
            "SELECT 1 FROM taxa WHERE rank = 'species' LIMIT 1"
        ).fetchone() is None:
            # Taxonomy JSON can exist before the download job has populated
            # the local taxa table. Without species rows, differently-spelled
            # aliases cannot yet be grouped; leave the marker unset to retry.
            return 0
        removed_count = 0
        try:
            rows = self.conn.execute(
                """SELECT pk.photo_id, k.id AS keyword_id, k.name,
                          k.parent_id, k.taxon_id, t.rank AS taxon_rank
                   FROM photo_keywords pk
                   JOIN keywords k ON k.id = pk.keyword_id
                   LEFT JOIN taxa t ON t.id = k.taxon_id
                   WHERE (k.is_species = 1 OR k.type = 'taxonomy')
                     AND (t.rank = 'species' OR k.taxon_id IS NULL)
                   ORDER BY pk.photo_id,
                            CASE WHEN k.parent_id IS NULL THEN 1 ELSE 0 END,
                            k.id"""
            ).fetchall()
            by_photo = {}
            for row in rows:
                by_photo.setdefault(row["photo_id"], []).append(row)

            # A key with multiple linked taxa anywhere in the catalog is an
            # ambiguous homonym (e.g. legacy ``Robin`` alongside taxonomy
            # ``robin`` bound to different taxa). ``get_photos_with_equivalent_species``
            # gates its NULL-taxon fallback the same way; without the guard
            # here, a NULL-taxon leaf on a photo that also carries one linked
            # root gets folded into that root's group, the root is treated
            # as a redundant duplicate, and the accepted taxonomy species is
            # detached from the photo.
            homonym_keys = set()
            key_taxa = {}
            for row in self.conn.execute(
                """SELECT DISTINCT k.name, k.taxon_id
                   FROM keywords k
                   WHERE (k.is_species = 1 OR k.type = 'taxonomy')
                     AND k.taxon_id IS NOT NULL"""
            ).fetchall():
                key = keyword_match_key(row["name"])
                taxa = key_taxa.setdefault(key, set())
                taxa.add(row["taxon_id"])
                if len(taxa) > 1:
                    homonym_keys.add(key)

            grouped = {}
            for photo_id, photo_rows in by_photo.items():
                linked = {}
                unlinked = {}
                for row in photo_rows:
                    if row["taxon_id"] is not None:
                        linked.setdefault(row["taxon_id"], []).append(row)
                    else:
                        unlinked.setdefault(
                            keyword_match_key(row["name"]), []
                        ).append(row)
                # Fold a legacy NULL-taxon spelling into a unique linked
                # species group on the same photo. If multiple linked taxa
                # share that common name, or the same key is a known homonym
                # bound to different taxa elsewhere, leave it alone rather
                # than guessing.
                for name_key, null_rows in unlinked.items():
                    if name_key in homonym_keys:
                        grouped[(photo_id, "name", name_key)] = null_rows
                        continue
                    candidates = [
                        taxon_id for taxon_id, linked_rows in linked.items()
                        if any(
                            keyword_match_key(row["name"]) == name_key
                            for row in linked_rows
                        )
                    ]
                    if len(candidates) == 1:
                        linked[candidates[0]].extend(null_rows)
                    else:
                        grouped[(photo_id, "name", name_key)] = null_rows
                for taxon_id, linked_rows in linked.items():
                    grouped[(photo_id, "taxon", taxon_id)] = linked_rows

            for group_key, group in grouped.items():
                photo_id = group_key[0]
                if len(group) < 2:
                    continue
                nested = sorted(
                    (row for row in group if row["parent_id"] is not None),
                    key=lambda row: row["keyword_id"],
                )
                remove = [row for row in group if row["parent_id"] is None]
                if not nested or not remove:
                    continue
                if group_key[1] == "name":
                    # Unlinked (NULL-taxon) rows are grouped by
                    # ``keyword_match_key`` only. Curation/eligibility for
                    # unlinked species keys is compared with exact
                    # ``k.name`` — there is no taxon fallback that maps a
                    # differently-spelled leaf back to the root spelling.
                    # Detaching root ``Foo`` while only leaf ``foo``
                    # remains would strand highlights/representatives/
                    # life-list preferences saved under ``Foo``. Restrict
                    # removal to root rows whose exact spelling matches at
                    # least one surviving leaf so exact-name eligibility
                    # keeps applying; different-spelling unlinked
                    # duplicates stay attached until a taxon link makes
                    # canonicalization safe.
                    nested_names = {row["name"] for row in nested}
                    remove = [row for row in remove if row["name"] in nested_names]
                    if not remove:
                        continue
                # Preserve every hierarchy placement; detach only root rows.
                remove_ids = [row["keyword_id"] for row in remove]
                placeholders = ",".join("?" for _ in remove_ids)
                self.conn.execute(
                    f"""DELETE FROM photo_keywords
                        WHERE photo_id = ? AND keyword_id IN ({placeholders})""",
                    [photo_id, *remove_ids],
                )
                removed_count += len(remove_ids)

                # Drop this photo's undo/redo items that reference a root tag
                # the repair detached. Keyword add/remove handlers read the
                # shared parent edit_history.new_value, so merely retargeting
                # edit_history_items would let redo attach the redundant root
                # again. Prediction accepts record each actual tag per item.
                # Deleting only the affected item preserves other photos in
                # a batch; empty parent edits are
                # removed below. Scope by action/column so an unrelated rating
                # or prediction id with the same numeric value is untouched.
                # ``no_tag`` prediction_accept items (JSON old_value carrying
                # ``"no_tag": true``) already skip tag mutations on undo/redo
                # because the photo carried the species via an equivalent
                # row, so keeping them cannot reattach the detached root and
                # dropping them would erase the only audit/undo record of
                # the accepted prediction-status flip.
                for removed in remove:
                    removed_id = str(removed["keyword_id"])
                    self.conn.execute(
                        """DELETE FROM edit_history_items
                           WHERE photo_id = ?
                             AND edit_id IN (
                                 SELECT id FROM edit_history
                                 WHERE (
                                     action_type = 'keyword_add'
                                     AND edit_history_items.new_value = ?
                                 ) OR (
                                     action_type = 'prediction_accept'
                                     AND edit_history_items.new_value = ?
                                     AND (
                                         edit_history_items.old_value IS NULL
                                         OR edit_history_items.old_value
                                             NOT LIKE '%"no_tag"%'
                                     )
                                 ) OR (
                                     action_type = 'keyword_remove'
                                     AND edit_history_items.old_value = ?
                                 ) OR (
                                     action_type = 'species_replace'
                                     AND (
                                         edit_history_items.old_value = ?
                                         OR edit_history_items.new_value = ?
                                     )
                                 )
                             )""",
                        (photo_id, removed_id, removed_id, removed_id,
                         removed_id, removed_id),
                    )

                # species_replace items can store ``old_value`` as a JSON
                # payload carrying ``keyword_id``/``keyword_ids`` when the
                # replace swapped out multiple old species rows for one
                # photo. A bare-string equality misses those, so an undo/redo
                # would parse the JSON and re-tag the detached root, undoing
                # the repair. Scan JSON payloads on this photo and drop any
                # species_replace item whose keyword_id(s) contains the
                # detached root.
                removed_id_ints = {int(row["keyword_id"]) for row in remove}
                json_items = self.conn.execute(
                    """SELECT ehi.id, ehi.old_value
                       FROM edit_history_items ehi
                       JOIN edit_history eh ON eh.id = ehi.edit_id
                       WHERE ehi.photo_id = ?
                         AND eh.action_type = 'species_replace'
                         AND ehi.old_value IS NOT NULL
                         AND ehi.old_value LIKE '{%'""",
                    (photo_id,),
                ).fetchall()
                for item in json_items:
                    try:
                        payload = json.loads(item["old_value"])
                    except (TypeError, ValueError):
                        continue
                    if not isinstance(payload, dict):
                        continue
                    references_removed = False
                    raw_kid = payload.get("keyword_id")
                    if raw_kid is not None:
                        try:
                            if int(raw_kid) in removed_id_ints:
                                references_removed = True
                        except (TypeError, ValueError):
                            pass
                    if not references_removed:
                        for k in (payload.get("keyword_ids") or []):
                            try:
                                if int(k) in removed_id_ints:
                                    references_removed = True
                                    break
                            except (TypeError, ValueError):
                                continue
                    if references_removed:
                        self.conn.execute(
                            "DELETE FROM edit_history_items WHERE id = ?",
                            (item["id"],),
                        )

                self.conn.execute(
                    """DELETE FROM edit_history
                       WHERE id NOT IN (
                           SELECT DISTINCT edit_id FROM edit_history_items
                       )"""
                )

                # Only cancel pending changes for the root spellings actually
                # being detached. Using every name in ``group`` here would
                # also match preserved hierarchy leaves — e.g. a leaf
                # ``Desert Verdin`` that a user tagged shortly before the
                # repair runs. That pending ``keyword_add`` must still reach
                # the sidecar, otherwise ``sync_to_xmp`` writes only the
                # root cleanup and the preserved hierarchy never appears
                # in XMP.
                remove_keys = {
                    keyword_match_key(row["name"]) for row in remove
                }
                pending = self.conn.execute(
                    """SELECT id, change_type, value FROM pending_changes
                       WHERE photo_id = ?
                         AND change_type IN ('keyword_add', 'keyword_remove')""",
                    (photo_id,),
                ).fetchall()
                pending_ids = []
                cancelled_add_keys = set()
                for row in pending:
                    key = keyword_match_key(row["value"] or "")
                    if key not in remove_keys:
                        continue
                    pending_ids.append(row["id"])
                    if row["change_type"] == "keyword_add":
                        cancelled_add_keys.add(key)
                for chunk in self._chunks(pending_ids):
                    pending_placeholders = ",".join("?" for _ in chunk)
                    self.conn.execute(
                        f"DELETE FROM pending_changes WHERE id IN ({pending_placeholders})",
                        chunk,
                    )

                # Split post-repair surviving names into two buckets:
                #
                # * attached-leaf keys — names of keywords still directly
                #   tagged on the photo. The scanner's flat dedup during
                #   a later XMP re-import already skips a matching
                #   ``dc:subject`` entry, so no sidecar remove is needed.
                # * ancestor-only keys — names that appear only in the
                #   parent chain of a surviving hierarchical leaf. The
                #   scanner does NOT count these when building its
                #   per-photo ``existing_keys`` (it uses attached leaf
                #   names only, see ``scanner._import_keywords_for_photo``),
                #   so a stale ``dc:subject: Verdin`` next to a preserved
                #   ``Verdin|Desert Verdin`` will be reimported and
                #   reattach the flat root — recreating the very
                #   duplicate this repair just removed. Queue a
                #   flat-only sidecar remove for these; a plain
                #   ``keyword_remove`` cannot be used because
                #   ``sync_to_xmp`` applies it hierarchically and would
                #   strip the preserved ``lr:hierarchicalSubject`` entry.
                attached_leaf_keys = set()
                ancestor_only_keys = set()
                for row in self.conn.execute(
                    """WITH RECURSIVE anc(id, name, parent_id, is_leaf) AS (
                           SELECT k.id, k.name, k.parent_id, 1
                             FROM photo_keywords pk
                             JOIN keywords k ON k.id = pk.keyword_id
                            WHERE pk.photo_id = ?
                           UNION
                           SELECT k.id, k.name, k.parent_id, 0
                             FROM keywords k
                             JOIN anc ON anc.parent_id = k.id
                       )
                       SELECT name, MAX(is_leaf) AS leaf
                         FROM anc GROUP BY id""",
                    (photo_id,),
                ).fetchall():
                    key = keyword_match_key(row["name"])
                    if not key:
                        continue
                    if row["leaf"]:
                        attached_leaf_keys.add(key)
                    else:
                        ancestor_only_keys.add(key)
                # The repair scans photo_keywords globally, but
                # pending_changes are filtered by workspace at read
                # time (get_pending_changes uses the active workspace).
                # A photo whose folder is not in the active workspace
                # would otherwise get its sidecar remove queued under
                # a workspace that will never sync it, leaving the
                # stale root spelling in XMP for the real workspace(s)
                # to re-import. Queue the remove for every workspace
                # that actually contains this photo; fall back to the
                # active workspace only when the photo has no
                # workspace membership at all.
                photo_workspaces = [
                    row["workspace_id"]
                    for row in self.conn.execute(
                        """SELECT DISTINCT wf.workspace_id
                           FROM photos p
                           JOIN workspace_folders wf
                             ON wf.folder_id = p.folder_id
                           WHERE p.id = ?""",
                        (photo_id,),
                    ).fetchall()
                ]
                for removed in remove:
                    key = keyword_match_key(removed["name"])
                    if not key or key in attached_leaf_keys:
                        continue
                    if key in cancelled_add_keys:
                        # The flat root add was still pending — cancelling
                        # it above already prevents the sidecar from ever
                        # receiving it, so no remove is required.
                        continue
                    # Ancestor-only survivors need flat-only cleanup:
                    # strip the stale ``dc:subject`` entry without the
                    # hierarchical sweep that would also drop the
                    # preserved ``lr:hierarchicalSubject`` line.
                    change_type = (
                        "keyword_remove_flat"
                        if key in ancestor_only_keys
                        else "keyword_remove"
                    )
                    if photo_workspaces:
                        for ws_id in photo_workspaces:
                            self.queue_change(
                                photo_id, change_type, removed["name"],
                                workspace_id=ws_id, _commit=False,
                            )
                    else:
                        self.queue_change(
                            photo_id, change_type, removed["name"],
                            _commit=False,
                        )

            self.set_meta(
                self._DUPLICATE_PHOTO_SPECIES_REPAIR_KEY, "1", _commit=False
            )
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise
        if removed_count:
            self.log.info(
                "repaired %d redundant same-photo species keyword association(s)",
                removed_count,
            )
        return removed_count

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
                   JOIN workspace_folders wf ON wf.folder_id = p.folder_id
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
        """Return keywords used in the active workspace (plus ancestors) with photo counts, type, and taxon info."""
        ws = self.workspace_id
        return self.conn.execute(
            """WITH RECURSIVE
               ws_links AS (
                   SELECT pk.keyword_id, pk.photo_id
                   FROM photo_keywords pk
                   JOIN photos p ON p.id = pk.photo_id
                   JOIN workspace_folders wf ON wf.folder_id = p.folder_id
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
               descendants(ancestor_id, descendant_id, depth) AS (
                   SELECT id, id, 0 FROM ancestors
                   UNION ALL
                   SELECT d.ancestor_id, k.id, d.depth + 1
                   FROM descendants d
                   JOIN keywords k ON k.parent_id = d.descendant_id
                   WHERE d.depth < 20
               )
               SELECT k.id, k.name, k.parent_id, k.type, k.taxon_id,
                      k.latitude, k.longitude, k.place_id,
                      t.name AS taxon_name, t.common_name AS taxon_common_name,
                      (SELECT COUNT(DISTINCT wl.photo_id) FROM descendants d
                       JOIN ws_links wl ON wl.keyword_id = d.descendant_id
                       WHERE d.ancestor_id = k.id) AS photo_count,
                      (SELECT COUNT(*) FROM ws_links wl WHERE wl.keyword_id = k.id) AS direct_photo_count
               FROM keywords k
               JOIN ancestors a ON a.id = k.id
               LEFT JOIN taxa t ON t.id = k.taxon_id
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
                       JOIN workspace_folders wf ON wf.folder_id = p.folder_id
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

    def commit(self):
        """Commit the connection's open transaction."""
        self.conn.commit()
