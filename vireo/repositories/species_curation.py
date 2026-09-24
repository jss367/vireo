"""Persistence for species curation: Highlights, representatives, the Life List.

This module owns the SQL behind the curated-species features:

- ``species_highlights``: the ordered per-workspace Highlights buckets (add,
  promote, move, remove, rename, and the eligibility-filtered read);
- ``species_representatives``: the global representative photos per species,
  newest selection first, with the undo restore and the rename;
- ``photo_preferences``: the per-workspace preference rows kept for older
  callers, their clear and rename;
- the Life List and taxonomy-explorer reads (candidates, found taxa,
  uncounted identifications, class ancestors, best photo per taxon, locations,
  the per-photo eligible identifications);
- the two one-shot legacy backfills from ``photo_preferences``.

The method bodies were moved verbatim from ``Database``. The only edits are
``self._ws_id()`` -> ``self.workspace_id`` and ``NAME`` -> ``self.NAME`` for
the ``db`` module helpers and constants listed in ``__init__``; the SQL text,
parameter order, chunk sizes and commit placement are unchanged.

What deliberately stays on ``Database``:

- The active-workspace state. ``workspace_id`` is resolved lazily through
  ``Database._ws_id`` at exactly the points the original code called it, so a
  method that returns early (``get_life_list_best_photo_by_taxon`` on no ids)
  or canonicalizes a species name first still raises when it always did.
- Composition. Every façade method a moved body calls arrives bound, under
  its ``Database`` name, and the body calls it as ``self.<name>(...)``:
  ``resolve_species_display_name`` and ``get_folder_subtree_ids`` (other
  domains), ``set_meta`` (inside the representatives backfill's transaction,
  with ``_commit=False``), and this domain's own entry points that other
  methods compose with (``get_class_ancestors_for_taxa``,
  ``_next_species_representative_order``,
  ``_set_global_species_representative``,
  ``rename_species_representatives_species``). Monkeypatches of those
  ``Database`` methods therefore keep taking effect.
- The methods with no SQL of their own: ``get_life_list_unmatched_species``,
  ``get_species_representatives`` and ``set_species_representative``.

``_commit`` flags are carried through unchanged: ``_commit=False`` means the
caller owns the transaction, and no method here commits unless the
``Database`` method it backs did.
"""

import json
import sqlite3

from keyword_normalization import keyword_match_key


class SpeciesCurationRepository:
    def __init__(
        self,
        conn,
        resolve_workspace_id,
        *,
        chunks,
        life_list_ancestor_suppression_clause,
        species_highlights_backfill_key,
        species_representatives_backfill_key,
        resolve_species_display_name,
        get_folder_subtree_ids,
        set_meta,
        get_class_ancestors_for_taxa,
        next_species_representative_order,
        set_global_species_representative,
        rename_species_representatives_species,
    ):
        self.conn = conn
        self._resolve_workspace_id = resolve_workspace_id
        # ``db`` module helpers and constants, kept under their module names
        # so the moved bodies read them as ``self.<name>``. ``_chunks`` is
        # ``db._chunks`` itself (its size default is bound at import).
        self._chunks = chunks
        self._LIFE_LIST_ANCESTOR_SUPPRESSION_CLAUSE = life_list_ancestor_suppression_clause
        # ``Database`` class attributes, kept under their class names.
        self._SPECIES_HIGHLIGHTS_BACKFILL_KEY = species_highlights_backfill_key
        self._SPECIES_REPRESENTATIVES_BACKFILL_KEY = species_representatives_backfill_key
        # Bound ``Database`` methods, kept under their façade names.
        self.resolve_species_display_name = resolve_species_display_name
        self.get_folder_subtree_ids = get_folder_subtree_ids
        self.set_meta = set_meta
        self.get_class_ancestors_for_taxa = get_class_ancestors_for_taxa
        self._next_species_representative_order = next_species_representative_order
        self._set_global_species_representative = set_global_species_representative
        self.rename_species_representatives_species = rename_species_representatives_species

    @property
    def workspace_id(self):
        """The active workspace id, resolved at each read (raises if none)."""
        return self._resolve_workspace_id()

    def backfill_highlights_from_legacy_preferences(self):
        """One-shot backfill: seed ``species_highlights`` from legacy
        ``photo_preferences`` rows with ``purpose='highlights'``.

        Before ordered highlights existed, a "Highlights" pick was stored
        as a single ``photo_preferences`` row per (workspace, species).
        The new Highlights UI reads exclusively from ``species_highlights``,
        so upgraded databases would lose those picks — the pill/rank
        indicators and bucket ordering would not surface the old choice
        until the user manually re-added it. This copies each legacy pick
        into ``species_highlights`` at the end of any existing bucket
        (rank = MAX(rank) + 1) so pre-existing curated order is preserved
        and the legacy pick still appears as a highlight.

        Gated by a ``db_meta`` marker so it runs exactly once per DB.
        """
        marker = self.conn.execute(
            "SELECT value FROM db_meta WHERE key = ?",
            (self._SPECIES_HIGHLIGHTS_BACKFILL_KEY,),
        ).fetchone()
        if marker is not None:
            return
        try:
            rows = self.conn.execute(
                """SELECT workspace_id, species, photo_id
                   FROM photo_preferences
                   WHERE purpose = 'highlights'"""
            ).fetchall()
        except sqlite3.OperationalError:
            rows = []
        for row in rows:
            ws = row["workspace_id"]
            sp = row["species"]
            pid = row["photo_id"]
            existing = self.conn.execute(
                """SELECT 1 FROM species_highlights
                   WHERE workspace_id = ? AND species = ? AND photo_id = ?""",
                (ws, sp, pid),
            ).fetchone()
            if existing:
                continue
            next_rank = int(self.conn.execute(
                """SELECT COALESCE(MAX(rank), 0) AS max_rank
                   FROM species_highlights
                   WHERE workspace_id = ? AND species = ?""",
                (ws, sp),
            ).fetchone()["max_rank"] or 0) + 1
            self.conn.execute(
                """INSERT INTO species_highlights
                       (workspace_id, species, photo_id, rank,
                        created_at, updated_at)
                   VALUES (?, ?, ?, ?, datetime('now'), datetime('now'))""",
                (ws, sp, pid, next_rank),
            )
        self.conn.execute(
            "INSERT INTO db_meta(key, value) VALUES (?, '1')",
            (self._SPECIES_HIGHLIGHTS_BACKFILL_KEY,),
        )
        self.conn.commit()

    def next_representative_order(self):
        row = self.conn.execute(
            "SELECT COALESCE(MAX(selected_order), 0) + 1 AS next_order "
            "FROM species_representatives"
        ).fetchone()
        return int(row["next_order"] or 1)

    def backfill_representatives_from_legacy_preferences(self):
        """One-shot backfill from old per-workspace representative rows.

        The current model stores representative markings globally and allows
        multiple photos per species. Older databases stored one row per
        (workspace, purpose, species), with ``species_representative`` taking
        precedence over ``life_list`` and ``highlights`` fallbacks. Copy those
        choices into the global list once so curated picks persist across
        workspaces after upgrade.
        """
        marker = self.conn.execute(
            "SELECT value FROM db_meta WHERE key = ?",
            (self._SPECIES_REPRESENTATIVES_BACKFILL_KEY,),
        ).fetchone()
        if marker is not None:
            return
        try:
            rows = self.conn.execute(
                """SELECT workspace_id, purpose, species, photo_id,
                          COALESCE(updated_at, created_at, '') AS ts
                   FROM photo_preferences
                   WHERE purpose IN ('species_representative', 'life_list', 'highlights')
                   ORDER BY CASE purpose
                              WHEN 'highlights' THEN 0
                              WHEN 'life_list' THEN 1
                              ELSE 2
                            END,
                            ts,
                            workspace_id,
                            species"""
            ).fetchall()
        except sqlite3.OperationalError:
            rows = []
        # Rows are ordered so higher-priority purposes are inserted last
        # (highlights first, then life_list, then species_representative).
        # Use UPSERT so a later canonical species_representative row for a
        # (species, photo_id) already inserted by a fallback purpose promotes
        # its selected_order to the newest value. Otherwise INSERT OR IGNORE
        # would keep the fallback's low order and the reader (which sorts by
        # selected_order DESC) could rank an unrelated life_list photo ahead
        # of the canonical representative — inverting the pre-migration
        # precedence this backfill is supposed to preserve.
        for row in rows:
            order = self._next_species_representative_order()
            self.conn.execute(
                """INSERT INTO species_representatives
                       (species, photo_id, selected_order, created_at, updated_at)
                   VALUES (?, ?, ?, datetime('now'), datetime('now'))
                   ON CONFLICT(species, photo_id) DO UPDATE SET
                       selected_order = excluded.selected_order,
                       updated_at = excluded.updated_at""",
                (row["species"], row["photo_id"], order),
            )
        self.set_meta(
            self._SPECIES_REPRESENTATIVES_BACKFILL_KEY,
            "1",
            _commit=False,
        )
        self.conn.commit()

    def get_highlights_candidates(self, folder_id, min_quality=0.0, photo_id=None):
        """Return photos eligible for highlights selection.

        When ``folder_id`` is an int, returns photos in that folder and its
        descendant folders. When ``folder_id`` is ``None``, returns photos
        across every folder visible in the active workspace. When
        ``photo_id`` is set, the result is additionally restricted to that
        single photo so the photo-detail endpoint can compute its highlight
        eligibility without rebuilding every workspace bucket.

        Each row carries:
          * ``species`` — accepted species keyword (NULL if none accepted)
          * ``prediction_id`` / ``predicted_species`` /
            ``predicted_confidence`` — top-confidence non-rejected prediction
            across the photo's detections (NULL if no usable prediction
            exists)

        Photos with ``quality_score >= min_quality`` that are not
        user-rejected are returned. When ``min_quality <= 0`` (the default),
        photos with no ``quality_score`` yet (not analyzed) are also included
        so picks and other unscored photos still surface on the Highlights
        page; raising the quality floor above 0 excludes them, since they have
        no measured quality to compare. The API layer applies the final
        highlights ranking because it combines these persisted quality fields
        with prediction confidence and user ratings.
        """
        ws = self.workspace_id
        if folder_id is None:
            folder_filter = ""
            folder_params = ()
        else:
            subtree = self.get_folder_subtree_ids(folder_id)
            placeholders = ",".join("?" for _ in subtree)
            folder_filter = f"AND p.folder_id IN ({placeholders})"
            folder_params = tuple(subtree)
        if photo_id is None:
            photo_filter = ""
            photo_params = ()
            bp_filter = ""
            bp_params = ()
            tp_filter = ""
            tp_params = ()
            kw_filter = ""
            kw_params = ()
        else:
            photo_filter = "AND p.id = ?"
            photo_params = (photo_id,)
            # Push the single-photo predicate into every derived subquery so
            # SQLite never materializes workspace-wide keyword/prediction
            # aggregations just to discard them at the outer join.
            bp_filter = "AND pk.photo_id = ?"
            bp_params = (photo_id,)
            tp_filter = "AND d.photo_id = ?"
            tp_params = (photo_id,)
            kw_filter = "WHERE pk.photo_id = ?"
            kw_params = (photo_id,)
        rows = self.conn.execute(
            f"""SELECT p.id, p.folder_id, p.filename, p.extension,
                      p.timestamp, p.width, p.height, p.rating, p.flag,
                      f.name AS folder_name, f.path AS folder_path,
                      p.thumb_path, p.quality_score, p.subject_sharpness,
                      p.subject_size, p.sharpness, p.phash_crop,
                      p.mask_path, p.subject_tenengrad, p.bg_tenengrad,
                      p.crop_complete, p.bg_separation,
                      p.subject_clip_high, p.subject_clip_low,
                      p.subject_y_median, p.noise_estimate,
                      p.eye_tenengrad,
                      p.dino_subject_embedding, p.dino_global_embedding,
                      bp.species,
                      tp.prediction_id,
                      tp.predicted_species,
                      tp.predicted_confidence,
                      kw.keyword_names
               FROM photos p
               JOIN workspace_folders wf ON wf.folder_id = p.folder_id
               JOIN folders f ON f.id = p.folder_id AND f.status IN ('ok', 'partial')
               LEFT JOIN (
                   SELECT photo_id, name AS species FROM (
                       SELECT pk.photo_id, k.name,
                              ROW_NUMBER() OVER (
                                  PARTITION BY pk.photo_id
                                  ORDER BY pk.rowid DESC
                              ) AS rn
                       FROM photo_keywords pk
                       JOIN keywords k ON k.id = pk.keyword_id
                       LEFT JOIN taxa t ON t.id = k.taxon_id
                       WHERE (k.is_species = 1 OR k.type = 'taxonomy')
                         AND (t.rank = 'species' OR t.rank IS NULL)
                         {bp_filter}
                   ) WHERE rn = 1
               ) bp ON bp.photo_id = p.id
               LEFT JOIN (
                   SELECT photo_id,
                          id AS prediction_id,
                          species AS predicted_species,
                          confidence AS predicted_confidence
                   FROM (
                       SELECT d.photo_id, pr.id, pr.species, pr.confidence,
                              ROW_NUMBER() OVER (
                                  PARTITION BY d.photo_id
                                  ORDER BY pr.confidence DESC, pr.id DESC
                              ) AS rn
                       FROM detections d
                       JOIN predictions pr ON pr.detection_id = d.id
                       LEFT JOIN prediction_review pr_rev
                         ON pr_rev.prediction_id = pr.id
                        AND pr_rev.workspace_id = ?
                       WHERE pr.species IS NOT NULL
                         AND COALESCE(pr_rev.status, 'pending') != 'rejected'
                         AND pr.labels_fingerprint = (
                             SELECT pr2.labels_fingerprint FROM predictions pr2
                             WHERE pr2.detection_id = pr.detection_id
                               AND pr2.classifier_model = pr.classifier_model
                             ORDER BY pr2.created_at DESC, pr2.id DESC
                             LIMIT 1
                         )
                         {tp_filter}
                   ) WHERE rn = 1
               ) tp ON tp.photo_id = p.id
               LEFT JOIN (
                   SELECT pk.photo_id,
                          group_concat(DISTINCT k.name) AS keyword_names
                   FROM photo_keywords pk
                   JOIN keywords k ON k.id = pk.keyword_id
                   {kw_filter}
                   GROUP BY pk.photo_id
               ) kw ON kw.photo_id = p.id
               WHERE wf.workspace_id = ?
                 {folder_filter}
                 {photo_filter}
                 AND (p.quality_score >= ?
                      OR (? <= 0 AND p.quality_score IS NULL))
                 AND (p.flag IS NULL OR p.flag != 'rejected')
               ORDER BY p.quality_score DESC""",
            (
                *bp_params,
                ws,
                *tp_params,
                *kw_params,
                ws,
                *folder_params,
                *photo_params,
                min_quality,
                min_quality,
            ),
        ).fetchall()
        return rows

    def get_life_list_candidates(self, species=None):
        """Return (photo x accepted-identification-keyword) life-list rows.

        Every non-rejected photo in a workspace-visible folder carrying an
        accepted identification keyword (``is_species = 1`` or
        ``type = 'taxonomy'``) produces one row per keyword. Taxonomy names
        and ranks ride along from ``taxa`` when the keyword is linked. Linked
        higher-rank identifications are included so the Life List can show and
        filter genus-, family-, and other non-species-level observations; the
        Explorer continues to count only species-rank taxa through
        :meth:`get_life_list_taxon_ids`.

        A linked higher-rank taxonomy keyword is suppressed when the same
        photo carries another linked taxonomy keyword whose taxon is a
        strict descendant of it, so a species-tagged robin also carrying
        Lightroom-imported ``includeParents`` ancestors (``Turdus`` /
        ``Turdidae`` / ``Aves``) or classifier-added broader labels does
        not inflate every ancestor rank's Life List bucket. See
        :data:`_LIFE_LIST_ANCESTOR_SUPPRESSION_CLAUSE` for the shared
        SQL fragment.

        Unlike :meth:`get_highlights_candidates`, photos without a
        ``quality_score`` are included — a species the user confirmed but
        never ran through the pipeline still belongs on the life list. The
        API layer ranks each species' photos with the highlights scorer,
        which falls back gracefully when metric columns are NULL.

        When ``species`` is provided, return the bucket for that species.
        A photo's surviving hierarchy leaf can have a different stored
        spelling from the canonical root keyword (``verdin`` vs
        ``Verdin``) after ``repair_duplicate_photo_species`` detaches the
        redundant root row, but curation stays keyed on the root spelling
        — so the filter also accepts any keyword whose ``taxon_id`` links
        back to a root species keyword named ``species``. Otherwise
        ``/api/life-list/species?species=Verdin`` would 404 for photos
        whose only remaining tag is the hierarchy leaf.
        """
        ws = self.workspace_id
        if species is not None:
            species_filter = """
                 AND (
                     k.name = ?
                     OR (
                         k.taxon_id IS NOT NULL
                         AND EXISTS (
                             SELECT 1 FROM keywords rk
                             WHERE rk.parent_id IS NULL
                               AND rk.taxon_id = k.taxon_id
                               AND (rk.is_species = 1 OR rk.type = 'taxonomy')
                               AND rk.name = ?
                         )
                     )
                 )"""
            params = (ws, species, species)
        else:
            species_filter = ""
            params = (ws,)
        return self.conn.execute(
            f"""SELECT p.id, p.folder_id, p.filename, p.timestamp,
                      p.rating, p.flag, p.quality_score,
                      p.subject_sharpness, p.subject_size, p.sharpness,
                      p.mask_path, p.subject_tenengrad, p.bg_tenengrad,
                      p.crop_complete, p.bg_separation,
                      p.subject_clip_high, p.subject_clip_low,
                      p.subject_y_median, p.noise_estimate,
                      p.eye_tenengrad,
                      k.name AS species,
                      t.id AS taxon_id,
                      t.rank AS taxon_rank,
                      t.name AS scientific_name,
                      t.common_name
               FROM photo_keywords pk
               JOIN keywords k ON k.id = pk.keyword_id
                AND (k.is_species = 1 OR k.type = 'taxonomy')
               JOIN photos p ON p.id = pk.photo_id
               JOIN workspace_folders wf ON wf.folder_id = p.folder_id
                AND wf.workspace_id = ?
               JOIN folders f ON f.id = p.folder_id
                AND f.status IN ('ok', 'partial')
               LEFT JOIN taxa t ON t.id = k.taxon_id
               WHERE COALESCE(p.flag, 'none') != 'rejected'
                 {species_filter}
                 {self._LIFE_LIST_ANCESTOR_SUPPRESSION_CLAUSE}
               ORDER BY k.name, p.timestamp""",
            params,
        ).fetchall()

    def get_explorer_root(self, name="Aves", rank="class"):
        """Return {id,name,common_name,rank} for the default explorer root class,
        or None when the reference taxonomy has not been downloaded."""
        row = self.conn.execute(
            "SELECT id, name, common_name, rank FROM taxa"
            " WHERE name = ? AND rank = ? LIMIT 1",
            (name, rank),
        ).fetchone()
        return dict(row) if row else None

    def get_life_list_taxon_ids(self):
        """Distinct species-rank taxa ids of workspace-scoped tagged species (same
        eligibility as get_life_list_candidates). Excludes species keywords with no
        taxon_id AND taxonomy tags that resolve to a taxon above species rank
        (genus, family, etc.). Higher-rank matches are surfaced via
        get_life_list_unmatched_species so the explorer's found/total math stays
        at species rank and non-species tags aren't silently undercounted."""
        ws = self.workspace_id
        rows = self.conn.execute(
            """SELECT DISTINCT k.taxon_id AS tid
               FROM photo_keywords pk
               JOIN keywords k ON k.id = pk.keyword_id
                AND (k.is_species = 1 OR k.type = 'taxonomy')
               JOIN taxa t ON t.id = k.taxon_id AND t.rank = 'species'
               JOIN photos p ON p.id = pk.photo_id
               JOIN workspace_folders wf ON wf.folder_id = p.folder_id
                AND wf.workspace_id = ?
               JOIN folders f ON f.id = p.folder_id
                AND f.status IN ('ok', 'partial')
               WHERE COALESCE(p.flag, 'none') != 'rejected'""",
            (ws,),
        ).fetchall()
        return {r["tid"] for r in rows}

    def get_life_list_uncounted_identifications(self):
        """Workspace identifications that cannot contribute a species count.

        Return dictionaries with the stored label, linked rank/taxon (when
        available), class ancestor, reason, and affected-photo count. The same
        per-photo ancestor suppression used by the Life List itself is applied
        here: an imported family/order keyword is not reported when that photo
        also carries a descendant identification. If at least one photo carries
        only the broader label, that label remains in the result with a count of
        the photos for which it is genuinely the most specific identification.
        """
        ws = self.workspace_id
        rows = self.conn.execute(
            f"""SELECT k.name AS name,
                      k.taxon_id AS taxon_id,
                      t.rank AS taxon_rank,
                      COUNT(DISTINCT p.id) AS photo_count
               FROM photo_keywords pk
               JOIN keywords k ON k.id = pk.keyword_id
                AND (k.is_species = 1 OR k.type = 'taxonomy')
               LEFT JOIN taxa t ON t.id = k.taxon_id
               JOIN photos p ON p.id = pk.photo_id
               JOIN workspace_folders wf ON wf.folder_id = p.folder_id
                AND wf.workspace_id = ?
               JOIN folders f ON f.id = p.folder_id
                AND f.status IN ('ok', 'partial')
               WHERE COALESCE(p.flag, 'none') != 'rejected'
                 AND (k.taxon_id IS NULL OR t.rank IS NULL OR t.rank != 'species')
                 {self._LIFE_LIST_ANCESTOR_SUPPRESSION_CLAUSE}
               GROUP BY k.name, k.taxon_id, t.rank
               ORDER BY k.name""",
            (ws,),
        ).fetchall()
        class_by_taxon = self.get_class_ancestors_for_taxa(
            [row["taxon_id"] for row in rows]
        )

        return [
            {
                "name": row["name"],
                "taxon_id": row["taxon_id"],
                "taxon_rank": row["taxon_rank"],
                "class": class_by_taxon.get(row["taxon_id"]),
                "reason": (
                    "higher_rank" if row["taxon_id"] is not None
                    and row["taxon_rank"] is not None else "unmatched"
                ),
                "photo_count": row["photo_count"],
                # Keep Browse handoffs compact even when this label affects
                # thousands of photos. The universal-filter engine expands
                # this token into the same server-side ancestor-suppression
                # predicate used by the count above.
                "filter_token": json.dumps(
                    {"name": row["name"], "taxon_id": row["taxon_id"]},
                    ensure_ascii=False,
                    separators=(",", ":"),
                ),
            }
            for row in rows
        ]

    def get_taxon_subtree(self, root_id, max_depth=12):
        """All taxa in the subtree rooted at root_id (inclusive), as dict rows
        with id, name, common_name, rank, parent_id. Uses the parent_id index."""
        # The depth cap is safe because the taxonomy loader keeps only major
        # ranks (see vireo/taxonomy.py MAJOR_RANK_LEVELS), giving a real
        # class->species depth of ~4. Raise the cap if intermediate ranks are
        # ever kept.
        return [dict(r) for r in self.conn.execute(
            """WITH RECURSIVE subtree(id, name, common_name, rank, parent_id, depth) AS (
                   SELECT id, name, common_name, rank, parent_id, 0
                   FROM taxa WHERE id = ?
                   UNION ALL
                   SELECT t.id, t.name, t.common_name, t.rank, t.parent_id, s.depth + 1
                   FROM taxa t JOIN subtree s ON t.parent_id = s.id
                   WHERE s.depth < ?
               )
               SELECT id, name, common_name, rank, parent_id FROM subtree""",
            (root_id, max_depth),
        ).fetchall()]

    def get_classes_for_taxa(self, taxon_ids):
        """Distinct class-rank ancestors of the given taxa, for the explorer's
        class selector. Returns [{id,name,common_name}] ordered by name.

        Callers pass the full life-list `found` set, which can exceed SQLite's
        bound-parameter limit on large life lists — chunk the seed IDs and
        merge the distinct classes across chunks so the endpoint doesn't 500.
        """
        ids = [t for t in taxon_ids if t is not None]
        if not ids:
            return []
        seen = {}
        for chunk in self._chunks(ids):
            placeholders = ",".join("?" for _ in chunk)
            rows = self.conn.execute(
                f"""WITH RECURSIVE up(id) AS (
                        SELECT id FROM taxa WHERE id IN ({placeholders})
                        UNION
                        SELECT t.parent_id FROM taxa t JOIN up u ON t.id = u.id
                        WHERE t.parent_id IS NOT NULL
                    )
                    SELECT DISTINCT t.id, t.name, t.common_name
                    FROM up u JOIN taxa t ON t.id = u.id
                    WHERE t.rank = 'class'""",
                chunk,
            ).fetchall()
            for r in rows:
                if r["id"] not in seen:
                    seen[r["id"]] = dict(r)
        return sorted(seen.values(),
                      key=lambda r: r["common_name"] or r["name"])

    def get_class_ancestors(self, taxon_ids):
        """Map each taxon id to its class-rank ancestor.

        Life List entries can be linked at any major rank, so preserve the
        starting taxon id while walking toward the root.  The depth cap mirrors
        :meth:`get_taxon_subtree` and also prevents malformed cyclic taxonomy
        data from making the recursive query run forever.
        """
        ids = [taxon_id for taxon_id in taxon_ids if taxon_id is not None]
        if not ids:
            return {}
        classes = {}
        for chunk in self._chunks(ids):
            placeholders = ",".join("?" for _ in chunk)
            rows = self.conn.execute(
                f"""WITH RECURSIVE up(
                           origin_id, id, parent_id, rank, name, common_name, depth
                       ) AS (
                           SELECT id, id, parent_id, rank, name, common_name, 0
                           FROM taxa WHERE id IN ({placeholders})
                           UNION ALL
                           SELECT u.origin_id, t.id, t.parent_id, t.rank,
                                  t.name, t.common_name, u.depth + 1
                           FROM up u JOIN taxa t ON t.id = u.parent_id
                           WHERE u.depth < 12
                       )
                       SELECT origin_id, id, name, common_name
                       FROM up WHERE rank = 'class'""",
                chunk,
            ).fetchall()
            for row in rows:
                classes.setdefault(row["origin_id"], {
                    "id": row["id"],
                    "name": row["name"],
                    "common_name": row["common_name"],
                })
        return classes

    def get_life_list_best_photo_by_taxon(self, taxon_ids):
        """Map taxon_id -> {id, filename} of a representative (highest quality_score,
        newest) workspace-scoped photo for that species. Missing taxa are absent."""
        ids = [t for t in taxon_ids if t is not None]
        if not ids:
            return {}
        ws = self.workspace_id
        best = {}
        # Chunk the IN list so we stay under SQLite's per-statement variable
        # limit (as low as 999 on some builds). Each taxon falls in exactly one
        # chunk, so first-row-per-taxon within a chunk is that taxon's global
        # best and chunk results merge without collision. The species-rank
        # explorer view can pass every found species in a class (1k+).
        for start in range(0, len(ids), 900):
            chunk = ids[start:start + 900]
            placeholders = ",".join("?" for _ in chunk)
            rows = self.conn.execute(
                f"""SELECT k.taxon_id AS tid, p.id, p.filename, p.quality_score, p.timestamp
                    FROM photo_keywords pk
                    JOIN keywords k ON k.id = pk.keyword_id AND k.taxon_id IN ({placeholders})
                    JOIN photos p ON p.id = pk.photo_id
                    JOIN workspace_folders wf ON wf.folder_id = p.folder_id
                     AND wf.workspace_id = ?
                    JOIN folders f ON f.id = p.folder_id AND f.status IN ('ok','partial')
                    WHERE COALESCE(p.flag, 'none') != 'rejected'
                    ORDER BY k.taxon_id,
                             COALESCE(p.quality_score, -1) DESC,
                             COALESCE(p.timestamp, '') DESC""",
                chunk + [ws],
            ).fetchall()
            for r in rows:
                if r["tid"] not in best:  # first row per taxon is the best by ORDER BY
                    best[r["tid"]] = {"id": r["id"], "filename": r["filename"]}
        return best

    def get_taxon_by_id(self, taxon_id):
        """Thin getter for a single taxon row (for non-default explorer roots)."""
        row = self.conn.execute(
            "SELECT id, name, common_name, rank, parent_id FROM taxa WHERE id = ?",
            (taxon_id,),
        ).fetchone()
        return dict(row) if row else None

    def get_photo_life_list_species(self, photo_id):
        """Return this photo's lifelist-eligible identification names in the
        active workspace, ordered by name.

        Same eligibility rule as :meth:`get_life_list_candidates`: an accepted
        identification keyword (``is_species = 1`` or ``type = 'taxonomy'``)
        on a non-rejected photo in a workspace-visible folder. Returns ``[]``
        when the photo carries no such identification (or is rejected /
        outside the workspace), which is exactly when no "Set Representative"
        affordance should appear. Linked higher-rank taxonomy identifications
        (genus, family, class, …) are included for the same reason they are
        in :meth:`get_life_list_candidates` — so a photo tagged only with a
        higher-rank identification exposes the shared representative row and
        can complete ``POST /api/photo-preferences`` for the entry it
        actually appears under on the Life List.

        Ancestor suppression mirrors :meth:`get_life_list_candidates`: a
        linked higher-rank taxonomy keyword on the photo is hidden when
        the photo also carries another linked taxonomy keyword whose
        taxon is a strict descendant of it, so the shared Set
        Representative row surfaces only under the specific
        identification the photo actually resolves to.

        Linked-taxon hierarchy leaves are canonicalized to the same-taxon
        root keyword's stored spelling — mirroring
        :meth:`get_species_keywords_for_photos` — so ``api_photo_detail`` can
        still match returned names against
        ``species_representative_lists``/``species_highlights``, which key on
        the canonical root. Without this, a photo whose only surviving species
        tag is a differently-spelled hierarchy leaf (``verdin`` after repair
        detached the ``Verdin`` root) would fail those lookups and the
        lightbox/context menu would offer to set it as representative again.

        Attached top-level rows keep their own stored spelling. When a photo
        carries a root alias such as ``Auriparus flaviceps`` and another root
        ``Verdin`` exists for the same taxon, curation writes preserve exact
        root-name matches, so rewriting the attached alias to an arbitrarily
        first same-taxon root would make representative/highlight state keyed
        to the actually attached name appear missing.

        Dedup identity mirrors :meth:`get_species_keywords_for_photos`:
        linked rows collapse by ``taxon_id`` and NULL-taxon rows key on
        the exact stored name. Two distinct linked homonyms (``Robin`` /
        ``robin`` pointing at different taxa) or preserved NULL-taxon case
        variants (root ``Foo`` alongside hierarchy leaf ``foo``) would
        otherwise collapse under an ASCII case-fold match key and hide
        one from ``api_photo_detail`` even though its keyword remains
        attached and its curation is keyed by the exact stored name.
        """
        ws = self.workspace_id
        rows = self.conn.execute(
            f"""SELECT k.name, k.parent_id, k.taxon_id
               FROM photo_keywords pk
               JOIN keywords k ON k.id = pk.keyword_id
                AND (k.is_species = 1 OR k.type = 'taxonomy')
               JOIN photos p ON p.id = pk.photo_id
                AND COALESCE(p.flag, 'none') != 'rejected'
               JOIN workspace_folders wf ON wf.folder_id = p.folder_id
                AND wf.workspace_id = ?
               JOIN folders f ON f.id = p.folder_id
                AND f.status IN ('ok', 'partial')
               WHERE pk.photo_id = ?
                 {self._LIFE_LIST_ANCESTOR_SUPPRESSION_CLAUSE}
               ORDER BY CASE WHEN k.parent_id IS NULL THEN 1 ELSE 0 END,
                        k.id""",
            (ws, photo_id),
        ).fetchall()
        if not rows:
            return []
        taxon_ids = {r["taxon_id"] for r in rows if r["taxon_id"] is not None}
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
        chosen = {}
        for r in rows:
            if r["taxon_id"] is not None:
                identity = ("taxon", r["taxon_id"])
                is_root = r["parent_id"] is None
                if is_root:
                    name = r["name"]
                else:
                    name = canonical_roots.get(r["taxon_id"], r["name"])
            else:
                identity = ("name", r["name"])
                name = r["name"]
            chosen.setdefault(identity, name)
        return sorted(chosen.values(), key=lambda n: keyword_match_key(n))

    def get_life_list_locations(self, species=None):
        """Return {identification name: [location keyword names]} for the life list.

        A location is attributed to an identification when at least one
        workspace-visible, non-rejected photo carries both the
        identification keyword and a ``type = 'location'`` keyword.
        Higher-rank taxonomy identifications (genus, family, class, …) are
        eligible here for the same reason they are in
        :meth:`get_life_list_candidates` — so a genus-level entry rendered
        on the Life List keeps its location chips and CSV values instead of
        appearing with an empty ``locations`` list.

        When ``species`` is given, only that identification is scanned —
        used by the single-identification paging endpoint so incremental
        loads don't do catalog-wide work. Matching mirrors
        :meth:`get_life_list_candidates`: raw ``k.name`` first, then a
        taxon-linked root fallback so a hierarchy leaf surviving repair
        (``verdin`` vs canonical root ``Verdin``) still contributes its
        location keywords to the requested bucket.

        Ancestor suppression also mirrors :meth:`get_life_list_candidates`:
        a linked higher-rank taxonomy keyword's locations are dropped
        when the same photo carries another linked taxonomy keyword whose
        taxon is a strict descendant of it, so ``Aves`` doesn't inherit
        every location where a robin was tagged.
        """
        ws = self.workspace_id
        species_filter = ""
        params = []
        if species:
            species_filter = """
                 AND (
                     k.name = ?
                     OR (
                         k.taxon_id IS NOT NULL
                         AND EXISTS (
                             SELECT 1 FROM keywords rk
                             WHERE rk.parent_id IS NULL
                               AND rk.taxon_id = k.taxon_id
                               AND (rk.is_species = 1 OR rk.type = 'taxonomy')
                               AND rk.name = ?
                         )
                     )
                 )"""
            params.extend([species, species])
        params.append(ws)
        rows = self.conn.execute(
            f"""SELECT DISTINCT k.name AS species, lk.name AS location
               FROM photo_keywords pk
               JOIN keywords k ON k.id = pk.keyword_id
                AND (k.is_species = 1 OR k.type = 'taxonomy')
                {species_filter}
               JOIN photos p ON p.id = pk.photo_id
                AND COALESCE(p.flag, 'none') != 'rejected'
               JOIN workspace_folders wf ON wf.folder_id = p.folder_id
                AND wf.workspace_id = ?
               JOIN folders f ON f.id = p.folder_id
                AND f.status IN ('ok', 'partial')
               JOIN photo_keywords plk ON plk.photo_id = p.id
               JOIN keywords lk ON lk.id = plk.keyword_id
                AND lk.type = 'location'
               WHERE 1=1
                 {self._LIFE_LIST_ANCESTOR_SUPPRESSION_CLAUSE}
               ORDER BY k.name, lk.name""",
            tuple(params),
        ).fetchall()
        result = {}
        for r in rows:
            result.setdefault(r["species"], []).append(r["location"])
        return result

    def get_photo_preferences(self, purpose):
        """Return {species: photo_id} preferences for the active workspace."""
        ws = self.workspace_id
        rows = self.conn.execute(
            """SELECT species, photo_id
               FROM photo_preferences
               WHERE workspace_id = ? AND purpose = ?""",
            (ws, purpose),
        ).fetchall()
        return {r["species"]: r["photo_id"] for r in rows}

    def get_representative_lists(self, eligible_only=False, species=None):
        """Return {species: [photo_id, ...]} representative photos.

        Representative markings are global, but this read is still scoped to
        the active workspace's folders. Life List callers still apply these
        rows only to actual species buckets, so a representative row alone
        does not make an untagged species appear on the list. Lists are
        newest-selection first, so item 0 is the main representative.

        When ``eligible_only`` is true, omit preferences whose photo is
        rejected, unavailable, or no longer carries the stored species keyword.
        The preference row remains intact for undo.

        When ``species`` is given, only return rows for that species — used
        by the single-species Life List paging endpoint so incremental
        loads don't scan every species' representatives.
        """
        if species is not None:
            species = self.resolve_species_display_name(species)
        ws = self.workspace_id
        eligibility_filter = ""
        if eligible_only:
            # Accept a hierarchy leaf whose taxon links back to a root
            # identification with the curation-keyed name. After
            # repair_duplicate_photo_species detaches a redundant root but
            # leaves the hierarchical leaf attached, the leaf's stored
            # spelling may differ from the root ("verdin" vs "Verdin"), yet
            # the photo still represents the same identification via a
            # shared taxon_id. An exact k.name = sr.species compare would
            # then silently drop that photo from Life List / Representative
            # eligibility even though curation was intentionally preserved
            # on the root key.
            # Eligibility mirrors :meth:`get_life_list_candidates`,
            # :meth:`get_photo_life_list_species`, and
            # ``_photo_can_be_life_list_preference`` — all admit linked
            # higher-rank taxonomy identifications (genus, family, class,
            # …). Without this, a just-saved representative for a
            # higher-rank Life List entry would be dropped by the
            # eligible-only reader that ``GET /api/photos/<id>`` uses to
            # decide ``is_current_photo``, so the shared Set Representative
            # affordance would keep offering the write even after it had
            # already succeeded. The shared ancestor-suppression clause is
            # appended so an ancestor keyword (``Aves``) does not survive
            # eligibility when the same photo also carries a descendant
            # identification (``American Robin``) — otherwise the class
            # bucket the Life List query already hides would still show a
            # current representative here.
            eligibility_filter = f"""
                 AND COALESCE(p.flag, 'none') != 'rejected'
                 AND f.status IN ('ok', 'partial')
                 AND EXISTS (
                     SELECT 1
                     FROM photo_keywords pk
                     JOIN keywords k ON k.id = pk.keyword_id
                      AND (k.is_species = 1 OR k.type = 'taxonomy')
                     WHERE pk.photo_id = sr.photo_id
                       AND (
                           k.name = sr.species
                           OR (
                               k.taxon_id IS NOT NULL
                               AND EXISTS (
                                   SELECT 1 FROM keywords root
                                   WHERE root.parent_id IS NULL
                                     AND (root.is_species = 1
                                          OR root.type = 'taxonomy')
                                     AND root.taxon_id = k.taxon_id
                                     AND root.name = sr.species
                               )
                           )
                       )
                       {self._LIFE_LIST_ANCESTOR_SUPPRESSION_CLAUSE}
                 )"""
        species_filter = ""
        params = [ws]
        if species:
            species_filter = " AND sr.species = ?"
            params.append(species)
        rows = self.conn.execute(
            f"""SELECT sr.species, sr.photo_id
               FROM species_representatives sr
               JOIN photos p ON p.id = sr.photo_id
               JOIN workspace_folders wf ON wf.folder_id = p.folder_id
                AND wf.workspace_id = ?
               JOIN folders f ON f.id = p.folder_id
                 {eligibility_filter}
                 {species_filter}
               ORDER BY sr.species, sr.selected_order DESC, sr.id DESC""",
            tuple(params),
        ).fetchall()
        result = {}
        for row in rows:
            ids = result.setdefault(row["species"], [])
            if row["photo_id"] not in ids:
                ids.append(row["photo_id"])
        return result

    def set_global_representative(self, species, photo_id):
        order = self._next_species_representative_order()
        self.conn.execute(
            """INSERT INTO species_representatives
                   (species, photo_id, selected_order, created_at, updated_at)
               VALUES (?, ?, ?, datetime('now'), datetime('now'))
               ON CONFLICT(species, photo_id) DO UPDATE SET
                   selected_order = excluded.selected_order,
                   updated_at = excluded.updated_at""",
            (species, photo_id, order),
        )

    def restore_representative(
        self, species, photo_id, selected_order=None,
    ):
        """Restore a global species_representatives row on undo.

        When ``selected_order`` is None (legacy edit-history payloads
        recorded before this field was captured), assign a fresh order via
        :meth:`_set_global_species_representative` — preserving the older
        promote-to-newest behavior for those undos. Otherwise write the
        captured order so undoing a relabel of a secondary representative
        does not push it above the pre-existing primary.
        """
        if selected_order is None:
            self._set_global_species_representative(species, photo_id)
            return
        try:
            order = int(selected_order)
        except (TypeError, ValueError):
            self._set_global_species_representative(species, photo_id)
            return
        self.conn.execute(
            """INSERT INTO species_representatives
                   (species, photo_id, selected_order, created_at, updated_at)
               VALUES (?, ?, ?, datetime('now'), datetime('now'))
               ON CONFLICT(species, photo_id) DO UPDATE SET
                   selected_order = excluded.selected_order,
                   updated_at = excluded.updated_at""",
            (species, photo_id, order),
        )

    def set_photo_preference(self, purpose, species, photo_id, _commit=True):
        """Set the preferred photo for a species/purpose in this workspace.

        ``species`` is canonicalized to the spelling ``add_keyword`` would
        store (existing keyword row first, casing convention otherwise), so
        curation keys written from prediction-cased bucket labels — e.g.
        starring a photo in an unconfirmed ``Common Waxbill`` bucket — land
        on the same key the keyword row will use once the species is
        accepted. The eligible highlight/life-list queries compare these
        strings exact against ``keywords.name``.
        """
        species = self.resolve_species_display_name(species)
        ws = self.workspace_id
        self.conn.execute(
            """INSERT INTO photo_preferences
                   (workspace_id, purpose, species, photo_id, created_at, updated_at)
               VALUES (?, ?, ?, ?, datetime('now'), datetime('now'))
               ON CONFLICT(workspace_id, purpose, species) DO UPDATE SET
                   photo_id = excluded.photo_id,
                   updated_at = excluded.updated_at""",
            (ws, purpose, species, photo_id),
        )
        if purpose in {"species_representative", "life_list", "highlights"}:
            self._set_global_species_representative(species, photo_id)
        if _commit:
            self.conn.commit()

    def clear_photo_preference(self, purpose, species, _commit=True):
        """Clear the preferred photo for a species/purpose in this workspace."""
        species = self.resolve_species_display_name(species)
        ws = self.workspace_id
        self.conn.execute(
            """DELETE FROM photo_preferences
               WHERE workspace_id = ? AND purpose = ? AND species = ?""",
            (ws, purpose, species),
        )
        if _commit:
            self.conn.commit()

    def clear_representative(self, species, _commit=True):
        """Clear all representative photos for a species globally."""
        species = self.resolve_species_display_name(species)
        ws = self.workspace_id
        self.conn.execute(
            "DELETE FROM species_representatives WHERE species = ?",
            (species,),
        )
        self.conn.execute(
            """DELETE FROM photo_preferences
               WHERE workspace_id = ?
                 AND species = ?
                 AND purpose IN ('species_representative', 'life_list', 'highlights')""",
            (ws, species),
        )
        if _commit:
            self.conn.commit()

    def get_highlights(self, species=None, eligible_only=False):
        """Return ordered highlighted photo ids for the active workspace.

        When ``eligible_only`` is true, omit rejected photos and photos that
        are no longer eligible for the Highlights page. Stored rows are kept
        intact so un-rejecting a photo restores its selection.

        Eligibility mirrors :meth:`get_highlights_candidates` at the default
        quality floor: not-yet-analyzed (``quality_score IS NULL``) photos
        stay eligible, because they now appear on the Highlights page and can
        be saved as highlights. Filtering them out here would silently drop a
        highlight the user just chose until analysis ran.

        Result shape is ``{species: {photo_id: rank}}``.
        """
        if species is not None:
            species = self.resolve_species_display_name(species)
        ws = self.workspace_id
        eligibility_joins = ""
        eligibility_filter = ""
        if eligible_only:
            eligibility_joins = """
                   JOIN photos p ON p.id = sh.photo_id
                   JOIN workspace_folders wf ON wf.folder_id = p.folder_id
                    AND wf.workspace_id = sh.workspace_id
                   JOIN folders f ON f.id = p.folder_id
                    AND f.status IN ('ok', 'partial')"""
            # Two-branch species eligibility. Accepted keyword compares
            # exact: same-NOCASE homonyms preserved by the migration
            # (legacy general ``Robin`` alongside taxonomy ``robin``) are
            # genuinely different species — a stored highlight for
            # ``Robin`` must not become eligible for a photo whose
            # accepted keyword resolves to ``robin``. The taxon-linked
            # fallback below (mirroring
            # :meth:`get_species_representative_lists`) preserves that
            # boundary: it only matches when a photo keyword's
            # ``taxon_id`` links back to a root species keyword whose
            # name equals ``sh.species``, so a photo carrying only
            # ``robin`` still cannot satisfy a ``Robin`` highlight
            # unless the two keywords share a taxon. It rescues the
            # case where ``repair_duplicate_photo_species`` detached
            # the redundant root and the surviving hierarchical leaf
            # has a different stored spelling from the canonical root
            # (``verdin`` vs ``Verdin``), so a preserved highlight
            # under ``Verdin`` still applies to that photo. Prediction
            # fallback compares NOCASE: the classifier emits an
            # external vocabulary spelling (e.g. ``Common Waxbill``)
            # while sh.species stores the canonical keyword spelling
            # (``Common waxbill``), so an exact compare would drop
            # every highlight starred from an unconfirmed bucket.
            # Applying NOCASE only to the fallback keeps the
            # ambiguous-homonym boundary intact.
            # The prediction subquery also routes ``pr.species`` through
            # the same hierarchy-alias → root canonicalization
            # ``resolve_species_display_name`` applies, so a highlight
            # saved from a canonicalized prediction bucket (leaf
            # ``Desert Verdin`` bucketed under root ``Verdin`` when the
            # linked taxon has a top-level row) still matches on
            # reload. ``COALESCE`` falls back to the raw prediction
            # spelling whenever the alias has no linked species-rank
            # root, mirroring ``add_species_highlight`` /
            # ``collect_highlight_buckets``.
            # The accepted-keyword branch mirrors
            # :meth:`get_species_representative_lists`: restrict to
            # species-rank taxonomy rows so a genus/family keyword named
            # identically to a species curation key (e.g. a genus `Puma`
            # row named `Puma`) does not satisfy a species highlight —
            # sibling species queries already exclude that photo from the
            # species bucket via `(t.rank = 'species' OR t.rank IS NULL)`.
            # The prediction-fallback branch below applies the same
            # `(t.rank = 'species' OR t.rank IS NULL)` filter to the
            # NOT EXISTS condition: a photo carrying only a higher-rank
            # taxonomy keyword (e.g. genus/family) is no longer part of
            # any species accepted bucket, and get_highlights_candidates
            # / collect_highlight_buckets will place it in a prediction
            # bucket (bp.species is NULL). Without the rank filter here,
            # the saved highlight would vanish on reload because the
            # NOT EXISTS still sees the higher-rank taxonomy row.
            eligibility_filter = """
                 AND COALESCE(p.flag, 'none') != 'rejected'
                 AND (
                     EXISTS (
                         SELECT 1
                         FROM photo_keywords pk
                         JOIN keywords k ON k.id = pk.keyword_id
                          AND (k.is_species = 1 OR k.type = 'taxonomy')
                         LEFT JOIN taxa t ON t.id = k.taxon_id
                         WHERE pk.photo_id = sh.photo_id
                           AND (t.rank = 'species' OR t.rank IS NULL)
                           AND (
                               k.name = sh.species
                               OR (
                                   k.taxon_id IS NOT NULL
                                   AND EXISTS (
                                       SELECT 1 FROM keywords root
                                       WHERE root.parent_id IS NULL
                                         AND (root.is_species = 1
                                              OR root.type = 'taxonomy')
                                         AND root.taxon_id = k.taxon_id
                                         AND root.name = sh.species
                                   )
                               )
                           )
                     )
                     OR (
                         NOT EXISTS (
                             SELECT 1
                             FROM photo_keywords pk
                             JOIN keywords k ON k.id = pk.keyword_id
                              AND (k.is_species = 1 OR k.type = 'taxonomy')
                             LEFT JOIN taxa t ON t.id = k.taxon_id
                             WHERE pk.photo_id = sh.photo_id
                               AND (t.rank = 'species' OR t.rank IS NULL)
                         )
                         AND sh.species = (
                             SELECT COALESCE(
                                 (
                                     -- Root-first, exact spelling:
                                     -- mirror ``resolve_species_display_name``'s
                                     -- case 2 preference for an
                                     -- exact-spelling same-name top-level
                                     -- species row. Keeps intentional
                                     -- ASCII/typography homonyms (e.g.
                                     -- general ``Robin`` +
                                     -- taxonomy ``robin``) routed to
                                     -- their own stored spelling instead
                                     -- of collapsing them onto one.
                                     -- (SQLite doesn't support
                                     -- correlated column references in a
                                     -- subquery ORDER BY, so the exact
                                     -- preference is expressed as its
                                     -- own subquery rather than an
                                     -- ORDER BY key on a NOCASE match.)
                                     SELECT root_exact.name
                                     FROM keywords root_exact
                                     WHERE root_exact.name = pr.species
                                       AND root_exact.parent_id IS NULL
                                       AND (
                                           root_exact.is_species = 1
                                           OR root_exact.type = 'taxonomy'
                                       )
                                     ORDER BY (root_exact.type = 'taxonomy') DESC,
                                              root_exact.id ASC
                                     LIMIT 1
                                 ),
                                 (
                                     -- Root-first, NOCASE fallback:
                                     -- mirror
                                     -- ``resolve_species_display_name``'s
                                     -- case 1 — when a single same-name
                                     -- top-level species row exists,
                                     -- ``add_species_highlight`` stored
                                     -- the highlight under its stored
                                     -- spelling regardless of the
                                     -- caller's casing. Without this
                                     -- branch, the hierarchy-alias
                                     -- canonicalization below could pick
                                     -- a different-taxon leaf sharing
                                     -- the label (e.g. root ``Robin``
                                     -- for one taxon plus a hierarchy
                                     -- leaf ``Robin`` under a different
                                     -- taxon), canonicalize to that
                                     -- leaf's root spelling, and drop
                                     -- the saved highlight on reload.
                                     SELECT root_direct.name
                                     FROM keywords root_direct
                                     WHERE root_direct.name = pr.species COLLATE NOCASE
                                       AND root_direct.parent_id IS NULL
                                       AND (
                                           root_direct.is_species = 1
                                           OR root_direct.type = 'taxonomy'
                                       )
                                     ORDER BY (root_direct.type = 'taxonomy') DESC,
                                              root_direct.id ASC
                                     LIMIT 1
                                 ),
                                 (
                                     -- Hierarchy-alias fallback:
                                     -- canonicalize only when the raw
                                     -- prediction label resolves to a
                                     -- unique linked taxon. When multiple
                                     -- hierarchy leaves share the label
                                     -- but point at different taxa, keep
                                     -- the raw ``pr.species`` so buckets
                                     -- key on the ambiguous label instead
                                     -- of an arbitrary root.
                                     -- Ambiguity count and the target-leaf
                                     -- restriction here must mirror
                                     -- ``resolve_species_display_name`` — the
                                     -- canonicalizer ``add_species_highlight``
                                     -- and ``collect_highlight_buckets`` use
                                     -- when writing ``sh.species``. That
                                     -- helper considers all linked hierarchy
                                     -- taxa regardless of rank. If this
                                     -- subquery restricted the count to
                                     -- species-rank taxa, a mixed
                                     -- species+higher-rank alias (e.g.
                                     -- species ``Puma`` and genus ``Puma``)
                                     -- would count as 1 here and canonicalize
                                     -- to the species root while
                                     -- ``add_species_highlight`` kept the raw
                                     -- ``pr.species`` — the eligibility
                                     -- compare below would then drop the
                                     -- highlight on reload.
                                     SELECT root.name
                                     FROM keywords k_pred
                                     JOIN keywords root
                                       ON root.parent_id IS NULL
                                      AND root.taxon_id = k_pred.taxon_id
                                      AND (
                                          root.is_species = 1
                                          OR root.type = 'taxonomy'
                                      )
                                     WHERE k_pred.name = pr.species COLLATE NOCASE
                                       AND k_pred.parent_id IS NOT NULL
                                       AND k_pred.taxon_id IS NOT NULL
                                       AND (
                                           k_pred.is_species = 1
                                           OR k_pred.type = 'taxonomy'
                                       )
                                       AND (
                                           SELECT COUNT(DISTINCT k2.taxon_id)
                                           FROM keywords k2
                                           WHERE k2.name = pr.species COLLATE NOCASE
                                             AND k2.parent_id IS NOT NULL
                                             AND k2.taxon_id IS NOT NULL
                                             AND (
                                                 k2.is_species = 1
                                                 OR k2.type = 'taxonomy'
                                             )
                                       ) = 1
                                     ORDER BY root.id
                                     LIMIT 1
                                 ),
                                 pr.species
                             )
                             FROM detections d
                             JOIN predictions pr ON pr.detection_id = d.id
                             LEFT JOIN prediction_review pr_rev
                              ON pr_rev.prediction_id = pr.id
                             AND pr_rev.workspace_id = sh.workspace_id
                             WHERE d.photo_id = sh.photo_id
                               AND pr.species IS NOT NULL
                               AND COALESCE(pr_rev.status, 'pending') != 'rejected'
                               AND pr.labels_fingerprint = (
                                   SELECT pr2.labels_fingerprint
                                   FROM predictions pr2
                                   WHERE pr2.detection_id = pr.detection_id
                                     AND pr2.classifier_model = pr.classifier_model
                                   ORDER BY pr2.created_at DESC, pr2.id DESC
                                   LIMIT 1
                               )
                             ORDER BY pr.confidence DESC, pr.id DESC
                             LIMIT 1
                         ) COLLATE NOCASE
                     )
                 )"""
        if species:
            rows = self.conn.execute(
                f"""SELECT sh.species, sh.photo_id, sh.rank
                   FROM species_highlights sh
                   {eligibility_joins}
                   WHERE sh.workspace_id = ? AND sh.species = ?
                   {eligibility_filter}
                   ORDER BY sh.rank, sh.created_at, sh.photo_id""",
                (ws, species),
            ).fetchall()
        else:
            rows = self.conn.execute(
                f"""SELECT sh.species, sh.photo_id, sh.rank
                   FROM species_highlights sh
                   {eligibility_joins}
                   WHERE sh.workspace_id = ?
                   {eligibility_filter}
                   ORDER BY sh.species, sh.rank, sh.created_at, sh.photo_id""",
                (ws,),
            ).fetchall()
        result = {}
        for r in rows:
            result.setdefault(r["species"], {})[r["photo_id"]] = r["rank"]
        return result

    def add_highlight(self, species, photo_id, _commit=True):
        """Add a photo to a species' ordered highlights, appending if new.

        ``species`` is canonicalized to the spelling ``add_keyword`` would
        store (see :meth:`set_photo_preference`) so highlight rows written
        from prediction-cased bucket labels key on the same string the
        keyword row and eligibility queries use.
        """
        species = self.resolve_species_display_name(species)
        ws = self.workspace_id
        row = self.conn.execute(
            """SELECT rank FROM species_highlights
               WHERE workspace_id = ? AND species = ? AND photo_id = ?""",
            (ws, species, photo_id),
        ).fetchone()
        if row:
            self.conn.execute(
                """UPDATE species_highlights
                   SET updated_at = datetime('now')
                   WHERE workspace_id = ? AND species = ? AND photo_id = ?""",
                (ws, species, photo_id),
            )
            if _commit:
                self.conn.commit()
            return row["rank"]
        max_rank = self.conn.execute(
            """SELECT COALESCE(MAX(rank), 0) AS max_rank
               FROM species_highlights
               WHERE workspace_id = ? AND species = ?""",
            (ws, species),
        ).fetchone()["max_rank"]
        rank = int(max_rank or 0) + 1
        self.conn.execute(
            """INSERT INTO species_highlights
                   (workspace_id, species, photo_id, rank, created_at, updated_at)
               VALUES (?, ?, ?, ?, datetime('now'), datetime('now'))""",
            (ws, species, photo_id, rank),
        )
        if _commit:
            self.conn.commit()
        return rank

    def promote_highlight(self, species, photo_id, _commit=True):
        """Add a photo to a species' ordered highlights at rank 1."""
        species = self.resolve_species_display_name(species)
        ws = self.workspace_id
        rows = self.conn.execute(
            """SELECT photo_id
               FROM species_highlights
               WHERE workspace_id = ? AND species = ?
               ORDER BY rank, created_at, photo_id""",
            (ws, species),
        ).fetchall()
        ids = [r["photo_id"] for r in rows if r["photo_id"] != photo_id]
        ids.insert(0, photo_id)

        self.conn.execute(
            """INSERT INTO species_highlights
                   (workspace_id, species, photo_id, rank, created_at, updated_at)
               VALUES (?, ?, ?, 1, datetime('now'), datetime('now'))
               ON CONFLICT(workspace_id, species, photo_id) DO UPDATE SET
                   rank = excluded.rank,
                   updated_at = excluded.updated_at""",
            (ws, species, photo_id),
        )
        for rank, pid in enumerate(ids, start=1):
            self.conn.execute(
                """UPDATE species_highlights
                   SET rank = ?, updated_at = datetime('now')
                   WHERE workspace_id = ? AND species = ? AND photo_id = ?""",
                (rank, ws, species, pid),
            )
        if _commit:
            self.conn.commit()
        return 1

    def remove_highlight(self, species, photo_id, _commit=True):
        """Remove a photo from a species' ordered highlights."""
        species = self.resolve_species_display_name(species)
        ws = self.workspace_id
        cur = self.conn.execute(
            """DELETE FROM species_highlights
               WHERE workspace_id = ? AND species = ? AND photo_id = ?""",
            (ws, species, photo_id),
        )
        if _commit:
            self.conn.commit()
        return cur.rowcount

    def move_highlight(self, species, photo_id, direction, _commit=True):
        """Move a highlighted photo one step up/down within its species."""
        species = self.resolve_species_display_name(species)
        ws = self.workspace_id
        rows = self.conn.execute(
            """SELECT photo_id
               FROM species_highlights
               WHERE workspace_id = ? AND species = ?
               ORDER BY rank, created_at, photo_id""",
            (ws, species),
        ).fetchall()
        ids = [r["photo_id"] for r in rows]
        if photo_id not in ids:
            return False
        idx = ids.index(photo_id)
        if direction == "up":
            new_idx = max(0, idx - 1)
        elif direction == "down":
            new_idx = min(len(ids) - 1, idx + 1)
        else:
            return False
        if new_idx == idx:
            return True
        ids.insert(new_idx, ids.pop(idx))
        for rank, pid in enumerate(ids, start=1):
            self.conn.execute(
                """UPDATE species_highlights
                   SET rank = ?, updated_at = datetime('now')
                   WHERE workspace_id = ? AND species = ? AND photo_id = ?""",
                (rank, ws, species, pid),
            )
        if _commit:
            self.conn.commit()
        return True

    def rename_photo_preferences(
        self, old_species, new_species, photo_workspace_pairs=None, _commit=True,
    ):
        """Rename stored representative-photo preferences across workspaces."""
        if not old_species or not new_species or old_species == new_species:
            return 0
        global_photo_ids = None
        if photo_workspace_pairs is not None:
            global_photo_ids = sorted({
                photo_id for photo_id, _workspace_id in photo_workspace_pairs
            })
        self.rename_species_representatives_species(
            old_species, new_species, photo_ids=global_photo_ids, _commit=False
        )
        if photo_workspace_pairs is not None:
            moved = 0
            seen = set()
            for photo_id, workspace_id in photo_workspace_pairs:
                key = (photo_id, workspace_id)
                if key in seen:
                    continue
                seen.add(key)
                cur = self.conn.execute(
                    """INSERT OR IGNORE INTO photo_preferences
                          (workspace_id, purpose, species, photo_id,
                           created_at, updated_at)
                       SELECT workspace_id, purpose, ?, photo_id,
                              created_at, datetime('now')
                       FROM photo_preferences
                       WHERE workspace_id = ?
                         AND species = ?
                         AND photo_id = ?""",
                    (new_species, workspace_id, old_species, photo_id),
                )
                moved += cur.rowcount
                self.conn.execute(
                    """DELETE FROM photo_preferences
                       WHERE workspace_id = ?
                         AND species = ?
                         AND photo_id = ?""",
                    (workspace_id, old_species, photo_id),
                )
            if _commit:
                self.conn.commit()
            return moved

        cur = self.conn.execute(
            """INSERT OR IGNORE INTO photo_preferences
                  (workspace_id, purpose, species, photo_id,
                   created_at, updated_at)
               SELECT workspace_id, purpose, ?, photo_id,
                      created_at, datetime('now')
               FROM photo_preferences
               WHERE species = ?""",
            (new_species, old_species),
        )
        self.conn.execute(
            "DELETE FROM photo_preferences WHERE species = ?",
            (old_species,),
        )
        if _commit:
            self.conn.commit()
        return cur.rowcount

    def rename_representatives(
        self, old_species, new_species, photo_ids=None, _commit=True,
    ):
        """Rename global representative rows for a species.

        ``photo_ids`` limits the rename to selected photos, used by relabel
        operations that only retag a subset of a species bucket.
        """
        if not old_species or not new_species or old_species == new_species:
            return 0
        if photo_ids is None:
            cur = self.conn.execute(
                """INSERT OR IGNORE INTO species_representatives
                       (species, photo_id, selected_order, created_at, updated_at)
                    SELECT ?, photo_id, selected_order, created_at, datetime('now')
                    FROM species_representatives
                    WHERE species = ?""",
                (new_species, old_species),
            )
            moved = cur.rowcount
            self.conn.execute(
                "DELETE FROM species_representatives WHERE species = ?",
                (old_species,),
            )
            if _commit:
                self.conn.commit()
            return moved
        ids = [int(pid) for pid in photo_ids]
        if not ids:
            return 0
        # Chunk to stay under SQLite's SQLITE_MAX_VARIABLE_NUMBER (default
        # 999 on legacy builds). A species can be tagged on tens of
        # thousands of photos, so a bulk relabel or keyword-rename that
        # funnels every affected photo through here would otherwise raise
        # "too many SQL variables" before any rows move.
        moved = 0
        for chunk in self._chunks(ids):
            placeholders = ",".join("?" for _ in chunk)
            cur = self.conn.execute(
                f"""INSERT OR IGNORE INTO species_representatives
                       (species, photo_id, selected_order, created_at, updated_at)
                    SELECT ?, photo_id, selected_order, created_at, datetime('now')
                    FROM species_representatives
                    WHERE species = ?
                      AND photo_id IN ({placeholders})""",
                [new_species, old_species, *chunk],
            )
            moved += cur.rowcount
            self.conn.execute(
                f"""DELETE FROM species_representatives
                    WHERE species = ?
                      AND photo_id IN ({placeholders})""",
                [old_species, *chunk],
            )
        if _commit:
            self.conn.commit()
        return moved

    def rename_highlights(
        self, old_species, new_species, photo_workspace_pairs=None, _commit=True,
    ):
        """Rename ordered species-highlight rows to a new species bucket.

        Companion to :meth:`rename_photo_preferences_species` for the
        ``species_highlights`` table. The rows carry a per-species ``rank``,
        so we can't just rewrite the ``species`` column — a bucket may
        already exist for ``new_species`` with its own ranks. Instead, each
        moved row is deleted from the old bucket and inserted at the end
        of the new bucket (``MAX(rank) + 1``) while preserving its
        old-bucket order. Rows whose photo already appears in the new
        bucket are dropped rather than duplicated.

        When ``photo_workspace_pairs`` is provided, only rows matching
        those ``(photo_id, workspace_id)`` pairs are moved (used by
        ``api_update_keyword`` so we only rebucket highlights for photos
        actually tagged with the renamed keyword). When omitted, all
        workspaces are rebucketed.

        Returns the count of highlight rows that landed in the new bucket
        (excludes rows dropped as duplicates).
        """
        if not old_species or not new_species or old_species == new_species:
            return 0

        if photo_workspace_pairs is not None:
            by_ws = {}
            seen = set()
            for photo_id, workspace_id in photo_workspace_pairs:
                key = (photo_id, workspace_id)
                if key in seen:
                    continue
                seen.add(key)
                by_ws.setdefault(workspace_id, []).append(photo_id)
            workspace_scopes = list(by_ws.items())
        else:
            workspace_ids = [
                r["workspace_id"] for r in self.conn.execute(
                    """SELECT DISTINCT workspace_id
                       FROM species_highlights
                       WHERE species = ?""",
                    (old_species,),
                ).fetchall()
            ]
            workspace_scopes = [(ws, None) for ws in workspace_ids]

        moved = 0
        for workspace_id, photo_ids in workspace_scopes:
            if photo_ids is None:
                src_rows = self.conn.execute(
                    """SELECT photo_id
                       FROM species_highlights
                       WHERE workspace_id = ? AND species = ?
                       ORDER BY rank, created_at, photo_id""",
                    (workspace_id, old_species),
                ).fetchall()
            else:
                # Chunk the IN(...) clause: photo_ids can carry every photo
                # tagged with the renamed keyword in this workspace, which on
                # legacy SQLite builds (SQLITE_MAX_VARIABLE_NUMBER=999) blows
                # the parameter cap once a species passes ~997 tagged photos.
                # Chunk in memory then re-sort so the rebucket order matches
                # the single-query path.
                raw_rows = []
                for chunk in self._chunks(photo_ids):
                    placeholders = ",".join("?" for _ in chunk)
                    raw_rows.extend(self.conn.execute(
                        f"""SELECT photo_id, rank, created_at
                            FROM species_highlights
                            WHERE workspace_id = ? AND species = ?
                              AND photo_id IN ({placeholders})""",
                        (workspace_id, old_species, *chunk),
                    ).fetchall())
                raw_rows.sort(
                    key=lambda r: (r["rank"], r["created_at"], r["photo_id"])
                )
                src_rows = raw_rows
            if not src_rows:
                continue
            existing = {
                r["photo_id"] for r in self.conn.execute(
                    """SELECT photo_id FROM species_highlights
                       WHERE workspace_id = ? AND species = ?""",
                    (workspace_id, new_species),
                ).fetchall()
            }
            next_rank = int(self.conn.execute(
                """SELECT COALESCE(MAX(rank), 0) AS max_rank
                   FROM species_highlights
                   WHERE workspace_id = ? AND species = ?""",
                (workspace_id, new_species),
            ).fetchone()["max_rank"] or 0) + 1
            for r in src_rows:
                pid = r["photo_id"]
                self.conn.execute(
                    """DELETE FROM species_highlights
                       WHERE workspace_id = ? AND species = ? AND photo_id = ?""",
                    (workspace_id, old_species, pid),
                )
                if pid in existing:
                    continue
                self.conn.execute(
                    """INSERT INTO species_highlights
                           (workspace_id, species, photo_id, rank,
                            created_at, updated_at)
                       VALUES (?, ?, ?, ?, datetime('now'), datetime('now'))""",
                    (workspace_id, new_species, pid, next_rank),
                )
                next_rank += 1
                moved += 1
        if _commit:
            self.conn.commit()
        return moved
