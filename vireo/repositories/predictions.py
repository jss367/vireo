"""Persistence for classifier predictions and their per-workspace review state.

This module owns the SQL behind the predictions domain:

- prediction rows (``predictions``): ``add`` (normalize-on-write, INSERT OR
  IGNORE re-use, fingerprint and match-score backfill, refreshed outputs),
  ``retain_candidates``, ``clear`` (with the companion
  ``classifier_match_scores`` and ``classifier_runs`` deletes), and the
  reads Review, Browse, the pipeline and iNat use (``get_rows``,
  ``get_group``, ``get_states``, ``get_top_for_photo``,
  ``get_top_confidences``, ``get_for_photo``, ``get_existing_photo_ids``);
- per-workspace review state (``prediction_review``): status updates, burst
  grouping metadata (``update_group_info`` / ``clear_group_info`` /
  ``ungroup``), the auto-match reconciliation, the raw
  ``get_review_status`` / ``set_review_status`` pair, the legacy
  mixed-species burst repair, and ``accept_subject_species``' target and
  agreeing-row reads.

The method bodies were moved verbatim from ``Database``. The only edits are
``self._ws_id()`` -> ``self.workspace_id`` and ``NAME`` -> ``self.NAME`` for
the ``db`` module helpers and constants listed in ``__init__``; the SQL text,
parameter order, chunk sizes and commit placement are unchanged.

What lives in ``repositories/keyword_provenance.py`` instead:
``accept_prediction``, whole. It tags the accepted species through
``tag_photo`` (and ``queue_change``, the curation renames and
``remove_pending_changes``) in the middle of its own transaction, so it
sits with the ``photo_keywords`` writers that
``test_keyword_provenance_contract`` keys to that module.
``accept_subject_species`` lives in this module, but reaches it through the
façade (``self.accept_prediction(...)``). A structural test fails if this
module ever references ``tag_photo``.

What deliberately stays on ``Database``:

- The prediction-decision mutators keep their ``Database`` names
  (``update_prediction_status``, ``update_predictions_status_by_photo``,
  ``ungroup_prediction``, ``set_review_status``, ``accept_prediction``,
  ``accept_subject_species``), which ``test_route_contract`` looks up.
- The rules helpers with no SQL of their own:
  ``_relax_negated_prediction_leaves`` and
  ``_filter_prediction_rows_by_rules`` (with ``_PREDICTION_ROW_FIELDS`` and
  ``_RELAX_BROAD``), plus ``DECIDED_PREDICTION_STATUSES``.
- The active-workspace state. ``workspace_id`` is resolved lazily through
  ``Database._ws_id`` at exactly the points the original code called it.
- Composition. Every façade method a moved body calls is bound from the
  ``Database`` instance under its own name (see ``FACADE_METHODS``) and
  called as ``self.<name>(...)``, so monkeypatches of ``Database`` methods
  keep reaching the moved code.

``_commit`` flags are carried through unchanged: ``_commit=False`` means the
caller owns the transaction, and no method here commits unless the
``Database`` method it backs did.
"""

import json
import sqlite3

from keyword_normalization import normalize_keyword_display, species_match_key

# ``Database`` methods the moved bodies call through the façade.
FACADE_METHODS = (
    "get_effective_config",
    "_build_query_from_rules",
    "_relax_negated_prediction_leaves",
    "_filter_prediction_rows_by_rules",
    "_top_prediction_confidence_params",
    "get_meta",
    "set_meta",
    "accept_prediction",
)


class PredictionRepository:
    def __init__(
        self,
        conn,
        resolve_workspace_id,
        *,
        chunks,
        commit_with_retry,
        log,
        auto_match_review_marker,
        top_prediction_confidence_expr,
        mixed_species_group_repair_key,
        facade,
    ):
        self.conn = conn
        self._resolve_workspace_id = resolve_workspace_id
        # ``db`` module helpers and constants, kept under their module names
        # so the moved bodies read them as ``self.<name>``. ``_chunks`` is
        # ``db._chunks`` itself (its size default is bound at import),
        # ``commit_with_retry`` is looked up on ``db`` when the façade
        # builds this repository (tests patch it there), and ``log`` is the
        # ``db`` logger, so log records keep its name.
        self._chunks = chunks
        self.commit_with_retry = commit_with_retry
        self.log = log
        self.AUTO_MATCH_REVIEW_MARKER = auto_match_review_marker
        self._TOP_PREDICTION_CONFIDENCE_EXPR = top_prediction_confidence_expr
        # ``Database`` class attributes, kept under their class names.
        self._MIXED_SPECIES_GROUP_REPAIR_KEY = mixed_species_group_repair_key
        # Bound ``Database`` methods, kept under their façade names.
        for name in FACADE_METHODS:
            setattr(self, name, getattr(facade, name))

    @property
    def workspace_id(self):
        """The active workspace id, resolved at each read (raises if none)."""
        return self._resolve_workspace_id()

    def add(
        self,
        detection_id,
        species,
        confidence,
        model,
        category="new",
        status="pending",
        group_id=None,
        vote_count=None,
        total_votes=None,
        individual=None,
        taxonomy=None,
        labels_fingerprint="legacy",
        labels_fingerprint_full=None,
        preserve_manual_review=False,
        match_score=None,
        from_fresh_inference=False,
        refresh_output=False,
    ):
        """Store a classification prediction for a detection.

        Uses INSERT OR IGNORE so re-running classification doesn't destroy
        existing predictions that the user may have already reviewed.
        Use clear_predictions() first if you want a fresh start.

        The `predictions` table stores only the raw, workspace-independent
        classifier output (species, confidence, classifier_model, taxonomy).
        Per-workspace review state (status, group_id, vote_count, individual)
        is written to ``prediction_review`` for the active workspace when the
        caller passes a non-default value.

        Args:
            detection_id: the detection ID (from detections table)
            taxonomy: optional dict with keys kingdom, phylum, class, order,
                      family, genus, scientific_name from taxonomy lookup
            labels_fingerprint: fingerprint of the label set used to classify
                (defaults to 'legacy' for backwards-compatible inserts).
            preserve_manual_review: when True, do not overwrite an existing
                accepted/rejected review row unless it was auto-created for an
                XMP taxonomy match.
            match_score: this species' raw pre-softmax score (cosine or logit,
                per the model). Optional; ``confidence`` alone cannot say
                whether the label fits, only that it fit better than the rest
                of the list.
            from_fresh_inference: True when ``match_score`` comes from a model
                that just ran on this detection, so it supersedes whatever is
                stored. False (the default) when the caller is replaying a
                value it read back from somewhere else — cache
                materialization, a backfill — in which case an existing score
                is left alone and only a NULL is filled.
            refresh_output: replace the output fields of an existing candidate
                while retaining its row ID and manual review decisions.
        """
        if detection_id is None:
            raise ValueError(
                "add_prediction requires a non-null detection_id; "
                "predictions without a detection row are orphaned and "
                "invisible to workspace-scoped queries"
            )
        # Fold the species into keyword-storage form so a curly-apostrophe
        # label file (`Swinhoe’s White-eye`) cannot re-mint a predictions row
        # that fails to match its accepted ASCII keyword (`Swinhoe's
        # white-eye`) under exact and COLLATE NOCASE joins. Doing it here
        # rather than only in the classify_job helpers covers every caller —
        # `_store_pending_detection_prediction`, `_store_match_prediction`,
        # and any future write path — so the invariant that keyword-side and
        # prediction-side spellings agree can't drift by adding a new caller
        # that forgot to normalize. Idempotent on already-folded strings.
        if species is not None:
            normalized_species = normalize_keyword_display(species)
            if normalized_species:
                species = normalized_species
        tax = taxonomy or {}
        cur = self.conn.execute(
            """INSERT OR IGNORE INTO predictions
               (detection_id, classifier_model, labels_fingerprint,
                labels_fingerprint_full,
                species, confidence, category,
                taxonomy_kingdom, taxonomy_phylum, taxonomy_class,
                taxonomy_order, taxonomy_family, taxonomy_genus, scientific_name,
                source_taxon_id, match_score)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                detection_id,
                model,
                labels_fingerprint,
                labels_fingerprint_full,
                species,
                confidence,
                category,
                tax.get("kingdom"),
                tax.get("phylum"),
                tax.get("class"),
                tax.get("order"),
                tax.get("family"),
                tax.get("genus"),
                tax.get("scientific_name"),
                tax.get("taxon_id"),
                match_score,
            ),
        )
        # SQLite's ``cur.lastrowid`` stays at the previous successful insert
        # even when this INSERT OR IGNORE was skipped by the UNIQUE
        # collision — relying on it silently upserted prediction_review for
        # the wrong prediction_id. Use rowcount (0 on IGNORE, 1 on insert)
        # to decide, then always re-query by the unique key.
        if cur.rowcount == 1:
            pred_id = cur.lastrowid
        else:
            row = self.conn.execute(
                """SELECT id FROM predictions
                   WHERE detection_id = ? AND classifier_model = ?
                     AND labels_fingerprint = ? AND species IS ?""",
                (detection_id, model, labels_fingerprint, species),
            ).fetchone()
            pred_id = row["id"] if row else None
            if pred_id is not None and refresh_output:
                self.conn.execute(
                    """UPDATE predictions SET confidence=?, category=?,
                       taxonomy_kingdom=?, taxonomy_phylum=?, taxonomy_class=?,
                       taxonomy_order=?, taxonomy_family=?, taxonomy_genus=?,
                       scientific_name=?, source_taxon_id=?, match_score=?
                       WHERE id=?""",
                    (confidence, category, tax.get("kingdom"), tax.get("phylum"),
                     tax.get("class"), tax.get("order"), tax.get("family"),
                     tax.get("genus"), tax.get("scientific_name"), tax.get("taxon_id"),
                     match_score, pred_id),
                )
            if pred_id is not None and labels_fingerprint_full is not None:
                self.conn.execute(
                    """UPDATE predictions
                       SET labels_fingerprint_full = ?
                       WHERE id = ? AND labels_fingerprint_full IS NULL""",
                    (labels_fingerprint_full, pred_id),
                )
            # Who wins depends on where the incoming score came from, so the
            # caller has to say (``from_fresh_inference``):
            #
            #   fresh inference  -> overwrite. The model just ran on this
            #     detection and this is what it measured.
            #   anything else    -> fill the gap only, like the fingerprint
            #     above. Cache materialization and enrichment backfills are
            #     replaying a recorded value, and a real local measurement
            #     should not be churned by a later import; it also keeps the
            #     column stable for a calibration pass that already read it.
            #
            # The unique key is NOT grounds to keep the stored value. A
            # non-reclassify pass re-runs inference whenever the existing
            # ``classifier_runs`` row has a different runtime fingerprint
            # (new weights under the same model name, different runtime), and
            # the runtime fingerprint is not part of
            # (detection, model, list, species). So the same key genuinely can
            # produce a materially different score, and keeping the old one
            # would leave the Pipeline Inspector showing a per-candidate score
            # from the previous runtime next to the run-level summary
            # ``record_classifier_match_score`` just wrote for the new one —
            # two contradictory facts on one surface.
            if pred_id is not None and match_score is not None:
                if from_fresh_inference:
                    self.conn.execute(
                        "UPDATE predictions SET match_score = ? WHERE id = ?",
                        (match_score, pred_id),
                    )
                else:
                    self.conn.execute(
                        """UPDATE predictions
                           SET match_score = ?
                           WHERE id = ? AND match_score IS NULL""",
                        (match_score, pred_id),
                    )
        # Write workspace-scoped review state only when the caller actually
        # supplied something beyond the defaults. Keeping pending rows out of
        # prediction_review is intentional: absence == pending. A refreshed
        # candidate must also clear an earlier automatic alternative status.
        has_review_state = (
            refresh_output
            or status != "pending"
            or group_id is not None
            or vote_count is not None
            or total_votes is not None
            or individual is not None
        )
        if pred_id is not None and has_review_state:
            ws_id = self.workspace_id
            if preserve_manual_review or refresh_output:
                review = self.conn.execute(
                    """SELECT status, individual FROM prediction_review
                       WHERE prediction_id = ? AND workspace_id = ?""",
                    (pred_id, ws_id),
                ).fetchone()
                if (
                    review is not None
                    and review["status"] in {"accepted", "rejected"}
                    and review["individual"] != self.AUTO_MATCH_REVIEW_MARKER
                ):
                    if refresh_output:
                        # Review decisions survive reinference, but burst
                        # membership is recomputed for the active workspace.
                        self.conn.execute(
                            """UPDATE prediction_review SET group_id=?, vote_count=?,
                               total_votes=?, individual=?
                               WHERE prediction_id=? AND workspace_id=?""",
                            (group_id, vote_count, total_votes,
                             None if individual == self.AUTO_MATCH_REVIEW_MARKER else individual,
                             pred_id, ws_id),
                        )
                    self.conn.commit()
                    return
            metadata_updates = ", ".join(
                f"{field} = excluded.{field}" if refresh_output
                else f"{field} = COALESCE(excluded.{field}, {field})"
                for field in ("individual", "group_id", "vote_count", "total_votes")
            )
            self.conn.execute(
                f"""INSERT INTO prediction_review
                     (prediction_id, workspace_id, status, reviewed_at,
                      individual, group_id, vote_count, total_votes)
                   VALUES (?, ?, ?, datetime('now'), ?, ?, ?, ?)
                   ON CONFLICT(prediction_id, workspace_id)
                   DO UPDATE SET status      = excluded.status,
                                 reviewed_at = excluded.reviewed_at,
                                 {metadata_updates}""",
                (pred_id, ws_id, status, individual, group_id,
                 vote_count, total_votes),
            )
        self.conn.commit()

    def retain_candidates(self, detection_id, model, labels_fingerprint, species):
        """Remove obsolete candidates after their replacement outputs were stored.

        Matching candidates keep their IDs and reviews in every workspace.
        Other detections, models, label sets and classifier run keys are untouched.
        """
        retained = {normalize_keyword_display(s) or s for s in species}
        rows = self.conn.execute(
            "SELECT id, species FROM predictions WHERE detection_id=? "
            "AND classifier_model=? AND labels_fingerprint=?",
            (detection_id, model, labels_fingerprint),
        ).fetchall()
        self.conn.executemany(
            "DELETE FROM predictions WHERE id=?",
            [(row["id"],) for row in rows if row["species"] not in retained],
        )
        self.conn.commit()

    def reconcile_match_review_state(
        self,
        detection_id,
        classifier_model,
        labels_fingerprint,
        species,
        category,
        auto_accept=True,
    ):
        """Re-sync a cached prediction's category and auto-review on reuse.

        Taxonomy ``match`` predictions are auto-accepted and intentionally
        hidden from the pending review queue (``_store_match_prediction``
        writes ``status='accepted'`` with ``AUTO_MATCH_REVIEW_MARKER``).  That
        review row is durable, so when a detection stops being a match — e.g.
        the photo's XMP keywords were edited — a later non-reclassify run
        reuses the cached prediction but the stale auto-accepted row would keep
        it out of the queue until a full reclassify/clear is forced.  Only the
        marked auto-review row is safe to drop here; explicit user decisions
        from before a temporary XMP match must remain intact.

        ``auto_accept`` is False when the caller has decided this reuse must
        stay pending even though ``category`` is still ``match`` — e.g. the
        XMP later gained a second recognized taxon, so a single-species match
        is now ambiguous.  Without dropping the marker in that case,
        ``status='accepted'`` from the earlier unambiguous run would keep the
        detection hidden from the queue.

        The persisted ``category`` is always refreshed to the current value:
        ``add_prediction`` is INSERT-OR-IGNORE so it never updates it on
        reuse, and a stale ``match`` marker would defeat the downgrade above
        on the next flip (and mislead the ``/api/predictions``
        disagreement/refinement enrichment).
        """
        ws = self.workspace_id
        # Mirror ``add_prediction``'s normalize-on-write: the persisted row
        # is keyed on the folded spelling, so a caller that hands us the
        # raw curly form (e.g. a future write path that forgot to fold)
        # would otherwise miss the row and skip the auto-accept marker
        # scrub even though the row plainly exists.
        if species is not None:
            normalized_species = normalize_keyword_display(species)
            if normalized_species:
                species = normalized_species
        row = self.conn.execute(
            """SELECT id, category FROM predictions
               WHERE detection_id = ? AND classifier_model = ?
                 AND labels_fingerprint = ? AND species IS ?""",
            (detection_id, classifier_model, labels_fingerprint, species),
        ).fetchone()
        if row is None:
            return
        pred_id = row["id"]
        if row["category"] == "match" and (category != "match" or not auto_accept):
            self.conn.execute(
                "DELETE FROM prediction_review "
                "WHERE prediction_id = ? AND workspace_id = ? "
                "AND status = 'accepted' AND individual = ?",
                (pred_id, ws, self.AUTO_MATCH_REVIEW_MARKER),
            )
        if row["category"] != category:
            self.conn.execute(
                "UPDATE predictions SET category = ? WHERE id = ?",
                (category, pred_id),
            )
        self.commit_with_retry(self.conn)

    def clear(self, model=None, collection_photo_ids=None,
                          labels_fingerprint=None, clear_run_keys=True):
        """Clear predictions, optionally filtered by model, photo set, and fingerprint.

        The ``predictions`` table is now global (no workspace_id).  This
        still restricts the delete to photos visible in the active workspace
        via ``workspace_folders`` so that calling "clear" in one workspace
        does not nuke another workspace's cached classifier output.

        ``labels_fingerprint`` is strongly recommended for reclassify flows:
        in shared-folder setups where workspace A and workspace B classify
        the same photos with different label sets, a reclassify in A keyed
        only by ``model`` would wipe B's cached predictions under its own
        fingerprint. And because ``classifier_runs`` keys include
        fingerprint, B's later non-reclassify runs would skip inference and
        leave those detections unclassified until forced. With
        ``labels_fingerprint`` passed, we delete only A's rows AND the
        matching ``classifier_runs`` rows so A's next pass actually re-runs.

        ``clear_run_keys=False`` is for callers that have just written fresh
        ``classifier_runs`` rows for these detections and are about to
        replace the predictions in the same transaction (e.g. the pipeline's
        deferred reclassify clear that runs after the per-photo
        ``record_classifier_run`` calls).  Wiping the run keys in that case
        would force the next non-reclassify pass to re-infer the entire
        collection.  Default ``True`` matches the long-standing safety
        behavior — only opt out if the caller guarantees fresh run keys.
        """
        ws = self.workspace_id
        # Build a reusable (cond, params) pair for the predictions subquery.
        extra_conds = []
        extra_params = []
        if model:
            extra_conds.append("pr.classifier_model = ?")
            extra_params.append(model)
        if labels_fingerprint is not None:
            extra_conds.append("pr.labels_fingerprint = ?")
            extra_params.append(labels_fingerprint)

        # The photo-id filter is chunked (one DELETE per id chunk) — a
        # reclassify over a collection larger than SQLite's bound-parameter
        # cap would otherwise fail with "too many SQL variables" after the
        # model already loaded. Chunks partition disjoint photo ids, so the
        # union of chunked DELETEs equals the single big one.
        if collection_photo_ids is not None:
            id_chunks = list(self._chunks(collection_photo_ids))
        else:
            id_chunks = [None]

        # Base filters for the classifier_match_scores companion delete
        # below — same shape as ``extra_conds`` for predictions, but
        # referencing the ``cms.`` alias since match scores have their own
        # (classifier_model, labels_fingerprint) columns.
        cms_extra_conds = []
        cms_extra_params = []
        if model:
            cms_extra_conds.append("cms.classifier_model = ?")
            cms_extra_params.append(model)
        if labels_fingerprint is not None:
            cms_extra_conds.append("cms.labels_fingerprint = ?")
            cms_extra_params.append(labels_fingerprint)

        for chunk in id_chunks:
            conds = list(extra_conds)
            params = list(extra_params)
            if chunk is not None:
                placeholders = ",".join("?" for _ in chunk)
                conds.append(f"d.photo_id IN ({placeholders})")
                params.extend(chunk)
            where_clause = (" WHERE " + " AND ".join(conds)) if conds else ""
            self.conn.execute(
                f"""DELETE FROM predictions WHERE id IN (
                    SELECT pr.id FROM predictions pr
                    JOIN detections d ON d.id = pr.detection_id
                    JOIN photos ph ON ph.id = d.photo_id
                    JOIN workspace_folders wf
                      ON wf.folder_id = ph.folder_id AND wf.workspace_id = ?
                    {where_clause}
                )""",
                [ws, *params],
            )

            # Match summaries live at the same key as the predictions we just
            # cleared, and outlast their predictions when left behind: a
            # detection whose predictions are cleared but whose
            # classifier_match_scores row survives keeps reporting the prior
            # run's verdict in Browse and the Pipeline Inspector, even though
            # the predictions that verdict described are gone. This has to
            # fire whether or not classifier_runs is being cleared — in
            # particular, the pipeline's deferred reclassify path calls
            # ``clear_predictions(..., clear_run_keys=False)`` after writing
            # fresh runs for detections that succeeded, and would otherwise
            # leave the previous run's match score attached to any detection
            # whose inference failed.
            cms_conds = list(cms_extra_conds)
            cms_params = list(cms_extra_params)
            if chunk is not None:
                placeholders = ",".join("?" for _ in chunk)
                cms_conds.append(f"d.photo_id IN ({placeholders})")
                cms_params.extend(chunk)
            cms_where = (
                " WHERE " + " AND ".join(cms_conds)
            ) if cms_conds else ""
            self.conn.execute(
                f"""DELETE FROM classifier_match_scores
                    WHERE rowid IN (
                        SELECT cms.rowid
                        FROM classifier_match_scores cms
                        JOIN detections d ON d.id = cms.detection_id
                        JOIN photos ph ON ph.id = d.photo_id
                        JOIN workspace_folders wf
                          ON wf.folder_id = ph.folder_id AND wf.workspace_id = ?
                        {cms_where}
                    )""",
                [ws, *cms_params],
            )

        if not clear_run_keys:
            self.conn.commit()
            return

        # Also clear matching classifier_runs rows so the next pass actually
        # re-runs the classifier. Without this, the skip gate at
        # classifier_runs would still report "done" even though the cached
        # predictions are gone, leaving detections permanently unclassified
        # unless the user forces a reclassify.
        #
        # classifier_runs has PK (detection_id, classifier_model,
        # labels_fingerprint), so delete by the full composite key, not by
        # detection_id alone — otherwise another fingerprint's run key on
        # the same detection would be wiped too.
        #
        # Run for every clear_predictions() call: when model is None we just
        # built a workspace-wide DELETE on predictions, so leaving the run
        # keys behind would strand those detections (the (detection, model,
        # fingerprint) gate would treat them as already classified).
        base_run_conds = []
        base_run_params = []
        if model is not None:
            base_run_conds.append("cr.classifier_model = ?")
            base_run_params.append(model)
        if labels_fingerprint is not None:
            base_run_conds.append("cr.labels_fingerprint = ?")
            base_run_params.append(labels_fingerprint)
        # Set-based DELETE via a rowid subquery — the previous
        # SELECT + per-row DELETE loop issued one statement per matching
        # run, which on a reclassify of a multi-thousand-detection
        # workspace dominates wall time on the startup-blocking thread.
        # Match semantics are identical: the subquery shape is the same
        # (JOIN through detections/photos/workspace_folders, same
        # optional filters), and rowid uniquely identifies each
        # classifier_runs row under the implicit-rowid default.
        # Photo-id chunking mirrors the predictions DELETE above.
        for chunk in id_chunks:
            run_conds = list(base_run_conds)
            run_params = list(base_run_params)
            if chunk is not None:
                placeholders = ",".join("?" for _ in chunk)
                run_conds.append(f"d.photo_id IN ({placeholders})")
                run_params.extend(chunk)
            run_where = (" WHERE " + " AND ".join(run_conds)) if run_conds else ""
            self.conn.execute(
                f"""DELETE FROM classifier_runs
                    WHERE rowid IN (
                        SELECT cr.rowid
                        FROM classifier_runs cr
                        JOIN detections d ON d.id = cr.detection_id
                        JOIN photos ph ON ph.id = d.photo_id
                        JOIN workspace_folders wf
                          ON wf.folder_id = ph.folder_id AND wf.workspace_id = ?
                        {run_where}
                    )""",
                [ws, *run_params],
            )
        # ``classifier_match_scores`` is already cleared alongside the
        # predictions above, whether or not classifier_runs is being wiped
        # here — see the companion delete inside the predictions loop.
        self.conn.commit()

    def get_states(self, photo_ids):
        """Explain, per photo, why it may have no predictions to show.

        An empty prediction list has four completely different meanings and a
        blank panel implies only the first, so Browse needs them separated:
        nothing has run yet, the detector ran and found no animals, detections
        exist but were never classified, or classification ran and produced
        nothing the user's confidence floor admits.

        ``threshold`` travels with the state so the panel can split the rows
        it already has into visible and below-the-floor, and say the hidden
        count out loud rather than dropping those rows silently.

        ``detection_count`` counts only what the rest of this file calls a
        real detection: ``detector_model = 'full-image'`` rows are the
        synthetic whole-frame anchor written *because* the detector found
        nothing, and rows under the workspace's ``detector_confidence`` floor
        are the noise the classifier never acts on. Counting either would
        report "detections exist but were never classified" for a photo whose
        detector plainly found no animal — the exact conflation this method
        exists to prevent. Same rule as ``count_real_detections_in_scope``.
        """
        if not photo_ids:
            return {}
        import config as cfg
        effective = self.get_effective_config(cfg.load())
        threshold = effective.get("classifier_confidence", 0.0) or 0.0
        detector_floor = effective.get("detector_confidence", 0.2)
        ids = list(dict.fromkeys(int(pid) for pid in photo_ids))
        states = {
            pid: {
                "detector_ran": False,
                "detection_count": 0,
                "classifier_ran": False,
                "threshold": threshold,
            }
            for pid in ids
        }
        for chunk in self._chunks(ids):
            placeholders = ",".join("?" for _ in chunk)
            for row in self.conn.execute(
                f"""SELECT photo_id, COUNT(*) AS n FROM detector_runs
                    WHERE photo_id IN ({placeholders}) GROUP BY photo_id""",
                chunk,
            ):
                states[row["photo_id"]]["detector_ran"] = row["n"] > 0
            for row in self.conn.execute(
                f"""SELECT photo_id, COUNT(*) AS n FROM detections
                    WHERE photo_id IN ({placeholders})
                      AND detector_model != 'full-image'
                      AND detector_confidence >= ?
                    GROUP BY photo_id""",
                [*chunk, detector_floor],
            ):
                states[row["photo_id"]]["detection_count"] = row["n"]
            for row in self.conn.execute(
                f"""SELECT d.photo_id AS photo_id, COUNT(*) AS n
                    FROM classifier_runs cr
                    JOIN detections d ON d.id = cr.detection_id
                    WHERE d.photo_id IN ({placeholders}) GROUP BY d.photo_id""",
                chunk,
            ):
                states[row["photo_id"]]["classifier_ran"] = row["n"] > 0
        return states

    def get_rows(self, photo_ids=None, model=None, status=None,
                        rules=None):
        """Get predictions with photo, detection and review info.

        Workspace scoping is enforced by joining ``workspace_folders``; the
        per-workspace review state (status, group_id, individual, vote_count)
        is left-joined from ``prediction_review`` so absent rows naturally
        surface as ``status = 'pending'``.

        Predictions are filtered to the most recent ``labels_fingerprint``
        per ``(detection_id, classifier_model)`` so stale rows from prior
        label sets don't contaminate ``/api/predictions`` or
        ``/api/predictions/compare`` after re-classification.

        Row-level scoping for prediction-field rules: the workspace/rules
        subquery below is applied at the photo level (``p.id IN (...)``),
        which correctly limits *which photos* surface but returns every
        current prediction for those photos. When the rules tree references
        Review-only fields (``prediction_confidence``, ``prediction_status``,
        ``classifier_model``, ``taxonomy_*``), a photo with a sibling
        prediction that matches would still return low-confidence /
        already-accepted rows that don't satisfy the visible filter — the
        Review grid and Accept All would then act on rows the filter chip
        excluded. ``_filter_prediction_rows_by_rules`` re-evaluates those
        leaves against each returned row so the grid matches the chip. It
        applies only when the tree can be resolved safely per row — see
        that method's docstring for when it falls back to the SQL result.
        """
        ws = self.workspace_id
        base_conditions = ["wf.workspace_id = ?"]
        base_params = [ws]
        if rules is not None:
            # ``_build_query_from_rules`` compiles ``none(...)`` as photo-level
            # ``NOT EXISTS(...)``. For a row-level predicate like
            # ``none(prediction_confidence >= 0.8)`` that drops any photo with
            # a sibling row over the threshold — even when the 0.10 sibling
            # satisfies the outer expression at the row level. Strip prediction
            # leaves nested inside a ``none`` group before scoping photos so
            # ``_filter_prediction_rows_by_rules`` can decide those per row
            # against the ORIGINAL tree — see r3618822252.
            scope_rules = self._relax_negated_prediction_leaves(rules)
            # row_scoped=True selects broader candidate SQL for the negative
            # prediction-field operators — the row filter below drops the
            # matching sibling rows so we can safely widen photo selection.
            # Photo-scoped callers (photo queries, saved collections) must
            # keep the default False to preserve the NOT EXISTS semantics
            # that include photos with no current predictions (r3619275290).
            r_folder_join, r_join_clause, r_where, r_params = (
                self._build_query_from_rules(scope_rules, row_scoped=True)
            )
            base_conditions.append(
                "p.id IN (SELECT DISTINCT p.id FROM photos p "
                f"{r_folder_join} {r_join_clause} {r_where})"
            )
            base_params.extend(r_params)
        if model:
            base_conditions.append("pr.classifier_model = ?")
            base_params.append(model)
        if status:
            base_conditions.append("COALESCE(pr_rev.status, 'pending') = ?")
            base_params.append(status)
        # Latest-fingerprint-per-(detection, classifier_model) filter — same
        # pattern used by get_top_prediction_for_photo.
        base_conditions.append(
            "pr.labels_fingerprint = ("
            "SELECT pr2.labels_fingerprint FROM predictions pr2 "
            "WHERE pr2.detection_id = pr.detection_id "
            "AND pr2.classifier_model = pr.classifier_model "
            "ORDER BY pr2.created_at DESC, pr2.id DESC LIMIT 1)"
        )
        # The photo-id filter is chunked — /api/predictions passes the full
        # resolved collection scope, which can exceed SQLite's bound-parameter
        # cap. Chunks partition disjoint photo ids; the merged rows are
        # re-sorted in Python to preserve the single-query ORDER BY.
        if photo_ids is not None:
            id_chunks = list(self._chunks(list(dict.fromkeys(photo_ids))))
        else:
            id_chunks = [None]
        rows = []
        for chunk in id_chunks:
            conditions = list(base_conditions)
            # first ? = pr_rev.workspace_id, rest = WHERE params
            params = [ws, *base_params]
            if chunk is not None:
                placeholders = ",".join("?" for _ in chunk)
                conditions.append(f"d.photo_id IN ({placeholders})")
                params.extend(chunk)
            where = "WHERE " + " AND ".join(conditions)
            rows.extend(self.conn.execute(
                f"""SELECT pr.*,
                           pr.classifier_model AS model,
                           COALESCE(pr_rev.status, 'pending') AS status,
                           pr_rev.individual AS individual,
                           pr_rev.group_id AS group_id,
                           pr_rev.vote_count AS vote_count,
                           pr_rev.total_votes AS total_votes,
                           d.photo_id, d.box_x, d.box_y, d.box_w, d.box_h,
                           d.detector_confidence, d.detector_model,
                           p.filename, p.timestamp
                    FROM predictions pr
                    JOIN detections d ON d.id = pr.detection_id
                    JOIN photos p ON p.id = d.photo_id
                    JOIN workspace_folders wf ON wf.folder_id = p.folder_id
                    LEFT JOIN prediction_review pr_rev
                      ON pr_rev.prediction_id = pr.id AND pr_rev.workspace_id = ?
                    {where} ORDER BY pr.confidence DESC""",
                params,
            ).fetchall())
        if len(id_chunks) > 1:
            # Match SQL "ORDER BY pr.confidence DESC" (NULLs sort last).
            rows.sort(
                key=lambda r: (r["confidence"] is None, -(r["confidence"] or 0))
            )
        if rules is not None:
            rows = self._filter_prediction_rows_by_rules(rows, rules)
        return rows

    def update_status(self, prediction_id, status, _commit=True):
        """Update per-workspace review status for a prediction.

        Review state lives in ``prediction_review`` keyed by
        (prediction_id, workspace_id); we upsert here rather than UPDATE
        so the "first review in a fresh workspace" path still writes a row.

        Args:
            _commit: If False, skip the internal commit (caller is responsible
                     for committing the transaction).
        """
        ws = self.workspace_id
        self.conn.execute(
            """INSERT INTO prediction_review
                 (prediction_id, workspace_id, status, reviewed_at)
               VALUES (?, ?, ?, datetime('now'))
               ON CONFLICT(prediction_id, workspace_id)
               DO UPDATE SET status = excluded.status,
                             reviewed_at = excluded.reviewed_at""",
            (prediction_id, ws, status),
        )
        if _commit:
            self.conn.commit()

    def get_group(self, group_id):
        """Get all predictions and photo data for a burst group.

        ``group_id`` lives in the workspace-scoped ``prediction_review``
        table now, so we join there to find the member predictions.  Each
        returned row is a dict with an ``alternatives`` list containing the
        per-detection alternative species predictions (review status
        ``'alternative'``), sorted by confidence descending.
        """
        ws = self.workspace_id
        primaries = self.conn.execute(
            """SELECT pr.*,
                      pr.classifier_model AS model,
                      COALESCE(pr_rev.status, 'pending') AS status,
                      pr_rev.individual AS individual,
                      pr_rev.group_id AS group_id,
                      pr_rev.vote_count AS vote_count,
                      pr_rev.total_votes AS total_votes,
                      d.photo_id, d.box_x, d.box_y, d.box_w, d.box_h,
                      d.detector_confidence, p.filename, p.timestamp, p.sharpness,
                      p.quality_score, p.subject_sharpness, p.subject_size,
                      p.rating, p.flag, p.width, p.height,
                      p.eye_x, p.eye_y, p.eye_conf
               FROM predictions pr
               JOIN prediction_review pr_rev
                 ON pr_rev.prediction_id = pr.id AND pr_rev.workspace_id = ?
               JOIN detections d ON d.id = pr.detection_id
               JOIN photos p ON p.id = d.photo_id
               JOIN workspace_folders wf
                 ON wf.folder_id = p.folder_id AND wf.workspace_id = ?
               WHERE pr_rev.group_id = ?
               ORDER BY p.quality_score DESC""",
            (ws, ws, group_id),
        ).fetchall()
        rows = [dict(r) for r in primaries]
        if not rows:
            return rows
        # Alternatives are correlated by
        # (detection_id, classifier_model, labels_fingerprint): a detection
        # may have been classified by multiple models or multiple label
        # sets (and those may share a group), so we must not merge
        # alternatives across any of those dimensions — otherwise stale
        # label-set rows would bleed into the group UI's alternatives
        # column. Alternatives are scoped per-workspace through
        # prediction_review.
        det_keys = {
            (r['detection_id'], r.get('model'), r.get('labels_fingerprint'))
            for r in rows if r.get('detection_id') is not None
        }
        alts_by_key = {k: [] for k in det_keys}
        det_ids = list({did for did, _, _ in det_keys})
        if det_ids:
            placeholders = ','.join('?' * len(det_ids))
            alt_rows = self.conn.execute(
                f"""SELECT pr.detection_id,
                           pr.classifier_model AS model,
                           pr.labels_fingerprint,
                           pr.species, pr.confidence
                    FROM predictions pr
                    JOIN prediction_review pr_rev
                      ON pr_rev.prediction_id = pr.id AND pr_rev.workspace_id = ?
                    WHERE pr_rev.status = 'alternative'
                      AND pr.detection_id IN ({placeholders})
                    ORDER BY pr.confidence DESC""",
                [ws, *det_ids],
            ).fetchall()
            for a in alt_rows:
                key = (a['detection_id'], a['model'], a['labels_fingerprint'])
                if key in alts_by_key:
                    alts_by_key[key].append(
                        {'species': a['species'], 'confidence': a['confidence']}
                    )
        for r in rows:
            r['alternatives'] = alts_by_key.get(
                (r.get('detection_id'), r.get('model'),
                 r.get('labels_fingerprint')),
                [],
            )
        return rows

    def update_status_by_photo(self, photo_id, status,
                                           _commit=True):
        """Upsert review status for every prediction of a photo in the active workspace.

        Review state is workspace-scoped (``prediction_review``); detections
        and predictions are global.  We enumerate the prediction ids via the
        detections join and upsert each review row.

        ``_commit=False`` lets a caller that already holds the prediction
        decision lock (``services.prediction_decisions``'
        ``under_prediction_decision_lock``) fold these writes into that
        transaction.

        Deliberately unconditional. The stale-overwrite this method used to
        enable — a group apply flipping an ``accepted`` row to ``rejected``
        while the keyword the accept added stays on the photo — is guarded
        one level up, in ``api_prediction_group_apply``, against the statuses
        the burst modal displayed (``_stale_group_apply_photos``). A
        ``WHERE status NOT IN DECIDED_PREDICTION_STATUSES`` clause on the
        ``ON CONFLICT`` below was tried first and removed: it cannot tell a
        stale payload from the user re-opening an applied burst and changing
        the split, which is a reachable flow (Review's default tab is "All",
        and every grouped card there renders "Review Burst Group"), so it
        silently froze the statuses while the flag and keyword writes above
        it — which run before this call and outside the lock — went ahead
        anyway. Guarding here is both too strict and too late.
        """
        ws = self.workspace_id
        rows = self.conn.execute(
            """SELECT pr.id FROM predictions pr
               JOIN detections d ON d.id = pr.detection_id
               WHERE d.photo_id = ?""",
            (photo_id,),
        ).fetchall()
        for r in rows:
            self.conn.execute(
                """INSERT INTO prediction_review
                     (prediction_id, workspace_id, status, reviewed_at)
                   VALUES (?, ?, ?, datetime('now'))
                   ON CONFLICT(prediction_id, workspace_id)
                   DO UPDATE SET status = excluded.status,
                                 reviewed_at = excluded.reviewed_at""",
                (r["id"], ws, status),
            )
        if _commit:
            self.conn.commit()

    def repair_mixed_species_groups(self):
        """Ungroup legacy bursts whose stored votes span more than one species.

        ``classify_job._store_grouped_predictions`` only stamps ``group_id``
        and ``individual`` when every frame in the burst folds to a single
        ``species_match_key`` — ``group_reviewable``. That gate arrived in
        #1165; bursts stored before it got a ``group_id`` and an
        unconditional multi-species vote dict regardless of whether the
        frames agreed.

        Those rows are actively wrong, not merely stale. ``accept_prediction``
        derives the species it applies from the vote dict, so a legacy row
        displays its own ``predictions.species`` and tags the vote winner:
        accepting a frame labelled ``Purple Finch`` writes ``Cassin's
        Finch``. That is the black box ``CORE_PHILOSOPHY.md`` forbids, and no
        amount of careful rendering fixes it — the button would still have to
        name one species and apply another.

        So repair the data rather than the symptom: for every review row
        whose ``individual`` holds more than one distinct species key, clear
        ``group_id``, ``individual``, ``vote_count`` and ``total_votes``.
        The row then reads exactly as the current classifier would have
        written it — an ungrouped prediction that accepts as its own species.
        ``status`` is untouched (a decision the user already made stays
        made), and nothing in ``predictions`` is read or written: no
        prediction is deleted, retitled or rescored, only the burst-grouping
        metadata that was never valid.

        Rows whose ``individual`` has several JSON keys that fold to *one*
        species are left grouped. ``species_match_key`` collapses okina,
        typographic-apostrophe and case variants, so ``{"Hawai'i 'Amakihi":
        4, "Hawai’i ’Amakihi": 2}`` is a unanimous burst spelled two
        ways and its grouping is legitimate. This is why the fold runs in
        Python: SQLite's ``lower()`` does not apply that normalization and
        would ungroup those rows.

        Not workspace-scoped. ``prediction_review`` is keyed by
        ``(prediction_id, workspace_id)`` and the same legacy classify run
        wrote rows in whichever workspaces were active at the time, so this
        deliberately runs across every workspace rather than through
        ``_ws_id()``.

        Idempotent and gated by a ``db_meta`` marker rather than
        ``PRAGMA user_version``: this repo has known ``user_version`` drift
        between branches, and a version-gated migration silently skips on a
        database whose number already ran ahead. After the first run the cost
        is one indexed ``db_meta`` lookup.

        Returns the number of review rows cleared.
        """
        if self.get_meta(self._MIXED_SPECIES_GROUP_REPAIR_KEY) == "1":
            return 0
        # A single-species vote dict is ``{"Robin": 4}`` — one JSON member,
        # no comma. Any row that could hold two species therefore contains a
        # comma, so this probe both short-circuits on the first candidate
        # (immediate on a catalog that has any) and lets a database with no
        # legacy groups stamp its marker after one scan instead of decoding
        # every ``individual`` blob in the table. A species name containing a
        # comma would pass the probe and then be left alone by the fold
        # below; the probe only has to avoid false negatives.
        try:
            candidate = self.conn.execute(
                """SELECT 1 FROM prediction_review
                   WHERE individual LIKE '%,%' LIMIT 1"""
            ).fetchone()
        except sqlite3.OperationalError:
            # Schema older than prediction_review (or a connection opened
            # with initialize_schema=False before it exists). Leave the
            # marker unset so a later boot retries.
            return 0
        if candidate is None:
            self.set_meta(self._MIXED_SPECIES_GROUP_REPAIR_KEY, "1")
            self.log.info(
                "Skipped mixed-species prediction-group repair: "
                "no multi-vote burst rows"
            )
            return 0

        rows = self.conn.execute(
            """SELECT pr.prediction_id, pr.workspace_id, pr.individual,
                      d.photo_id
               FROM prediction_review pr
               JOIN predictions p ON p.id = pr.prediction_id
               JOIN detections d ON d.id = p.detection_id
               WHERE pr.individual LIKE '%,%'"""
        ).fetchall()
        targets = []
        photo_ids = set()
        for row in rows:
            try:
                votes = json.loads(row["individual"])
            except (TypeError, ValueError):
                continue
            if not isinstance(votes, dict) or len(votes) < 2:
                continue
            if len({species_match_key(name) for name in votes}) < 2:
                continue
            targets.append((row["prediction_id"], row["workspace_id"]))
            photo_ids.add(row["photo_id"])

        # One transaction for the whole repair, marker included. Partway
        # through is the one state that must not be reachable: a stamped
        # marker over a half-cleared table would strand the rest forever,
        # and cleared rows without the marker would re-scan on every boot.
        # The writes are chunked only to keep executemany's parameter list
        # bounded — ~42k two-column updates on the live catalog is well
        # inside what SQLite holds in a single transaction.
        for chunk in self._chunks(targets):
            self.conn.executemany(
                """UPDATE prediction_review
                      SET group_id = NULL,
                          individual = NULL,
                          vote_count = NULL,
                          total_votes = NULL
                    WHERE prediction_id = ? AND workspace_id = ?""",
                chunk,
            )
        self.set_meta(self._MIXED_SPECIES_GROUP_REPAIR_KEY, "1", _commit=False)
        self.commit_with_retry(self.conn)
        if targets:
            self.log.info(
                "Ungrouped %d legacy prediction review row(s) across %d "
                "photo(s) whose burst votes spanned more than one species; "
                "these now accept as their own species",
                len(targets), len(photo_ids),
            )
        else:
            self.log.info(
                "Mixed-species prediction-group repair found nothing to "
                "clear: every multi-vote burst folds to one species"
            )
        return len(targets)

    def ungroup(self, prediction_id, _commit=True):
        """Remove a prediction from its group in the active workspace.

        ``group_id`` lives in ``prediction_review``; this only clears the
        review row for the current workspace.
        """
        self.conn.execute(
            """UPDATE prediction_review SET group_id = NULL
               WHERE prediction_id = ? AND workspace_id = ?""",
            (prediction_id, self.workspace_id),
        )
        if _commit:
            self.conn.commit()

    def get_existing_photo_ids(self, model, labels_fingerprint=None):
        """Return photo_ids with predictions for a (model, fingerprint), scoped to active workspace.

        The cache identity of a prediction is
        ``(detection_id, classifier_model, labels_fingerprint, species)``, so
        the photo-level short-circuit in classify_job / pipeline_job must key
        on both model AND fingerprint. Keying only on model means changing
        the workspace's label set leaves stale predictions and the classifier
        is never re-run until the user forces ``reclassify``.

        ``labels_fingerprint=None`` preserves the pre-fingerprint behavior
        for callers that haven't plumbed the fingerprint through yet.
        """
        if labels_fingerprint is None:
            rows = self.conn.execute(
                """SELECT DISTINCT d.photo_id FROM predictions pr
                   JOIN detections d ON d.id = pr.detection_id
                   JOIN photos p ON p.id = d.photo_id
                   JOIN workspace_folders wf
                     ON wf.folder_id = p.folder_id AND wf.workspace_id = ?
                   WHERE pr.classifier_model = ?""",
                (self.workspace_id, model),
            ).fetchall()
        else:
            rows = self.conn.execute(
                """SELECT DISTINCT d.photo_id FROM predictions pr
                   JOIN detections d ON d.id = pr.detection_id
                   JOIN photos p ON p.id = d.photo_id
                   JOIN workspace_folders wf
                     ON wf.folder_id = p.folder_id AND wf.workspace_id = ?
                   WHERE pr.classifier_model = ?
                     AND pr.labels_fingerprint = ?""",
                (self.workspace_id, model, labels_fingerprint),
            ).fetchall()
        return {r["photo_id"] for r in rows}

    def get_top_for_photo(self, photo_id, min_detector_confidence=None):
        """Return the highest-confidence *current* prediction for a photo.

        "Current" means: workspace-scoped via workspace_folders, and for
        each (detection, classifier_model) only the most recent
        labels_fingerprint's rows are considered — stale predictions from
        prior label sets on the same detection are skipped so callers
        like /api/inat/prepare don't prefill a taxon from an old label set.

        ``min_detector_confidence``: optional read-time threshold applied to
        the joined detection. With read-time thresholding, predictions tied
        to detections below the active threshold are visually hidden in the
        UI; callers like the iNat endpoints should pass the workspace-
        effective threshold so they don't surface a species from a now-
        hidden detection.

        Returns a dict with ``species``, ``scientific_name``, ``confidence``,
        ``detection_id`` or None if no eligible prediction exists.
        """
        if min_detector_confidence is None:
            return self.conn.execute(
                """SELECT pr.species, pr.scientific_name, pr.confidence,
                          pr.detection_id
                   FROM predictions pr
                   JOIN detections d ON d.id = pr.detection_id
                   JOIN photos p ON p.id = d.photo_id
                   JOIN workspace_folders wf
                     ON wf.folder_id = p.folder_id AND wf.workspace_id = ?
                   WHERE d.photo_id = ?
                     AND pr.labels_fingerprint = (
                        SELECT pr2.labels_fingerprint FROM predictions pr2
                        WHERE pr2.detection_id = pr.detection_id
                          AND pr2.classifier_model = pr.classifier_model
                        ORDER BY pr2.created_at DESC, pr2.id DESC
                        LIMIT 1
                     )
                   ORDER BY pr.confidence DESC LIMIT 1""",
                (self.workspace_id, photo_id),
            ).fetchone()
        return self.conn.execute(
            """SELECT pr.species, pr.scientific_name, pr.confidence,
                      pr.detection_id
               FROM predictions pr
               JOIN detections d ON d.id = pr.detection_id
               JOIN photos p ON p.id = d.photo_id
               JOIN workspace_folders wf
                 ON wf.folder_id = p.folder_id AND wf.workspace_id = ?
               WHERE d.photo_id = ?
                 AND d.detector_confidence >= ?
                 AND pr.labels_fingerprint = (
                    SELECT pr2.labels_fingerprint FROM predictions pr2
                    WHERE pr2.detection_id = pr.detection_id
                      AND pr2.classifier_model = pr.classifier_model
                    ORDER BY pr2.created_at DESC, pr2.id DESC
                    LIMIT 1
                 )
               ORDER BY pr.confidence DESC LIMIT 1""",
            (self.workspace_id, photo_id, min_detector_confidence),
        ).fetchone()

    def get_top_confidences(self, photo_ids):
        """Map photo id → the confidence the Browse sorts rank on.

        Built from the same ``_TOP_PREDICTION_CONFIDENCE_EXPR`` the
        ``prediction_confidence`` sorts order by, so the number a card shows
        is by construction the number that put it where it is — a card badge
        computed from a second, nearly-identical query is exactly the kind of
        cheap proxy CORE_PHILOSOPHY's "no black boxes" rule rules out.

        Photos with no current, unrejected species prediction are absent from
        the mapping rather than present with a 0.0, which would read as "the
        classifier is certain this is nothing".
        """
        if not photo_ids:
            return {}
        conf_params = self._top_prediction_confidence_params()
        result = {}
        for i in range(0, len(photo_ids), 800):
            chunk = photo_ids[i:i + 800]
            placeholders = ",".join("?" for _ in chunk)
            rows = self.conn.execute(
                f"""SELECT p.id AS photo_id,
                           {self._TOP_PREDICTION_CONFIDENCE_EXPR} AS confidence
                    FROM photos p
                    WHERE p.id IN ({placeholders})""",
                (*conf_params, *chunk),
            ).fetchall()
            for row in rows:
                if row["confidence"] is not None:
                    result[row["photo_id"]] = row["confidence"]
        return result

    def get_for_photo(self, photo_id, model, labels_fingerprint=None):
        """Return species, confidence, and detection_id for a photo's prediction.

        Detections and predictions are global; the active workspace is
        enforced through ``workspace_folders``. Since prediction cache
        identity is (detection, model, fingerprint, species), callers
        should pass ``labels_fingerprint`` to avoid returning a row
        written under a different label set. ``labels_fingerprint=None``
        preserves the pre-refactor behavior (any row for the model).
        """
        if labels_fingerprint is None:
            return self.conn.execute(
                """SELECT pr.species, pr.confidence, pr.detection_id FROM predictions pr
                   JOIN detections d ON d.id = pr.detection_id
                   JOIN photos p ON p.id = d.photo_id
                   JOIN workspace_folders wf
                     ON wf.folder_id = p.folder_id AND wf.workspace_id = ?
                   WHERE d.photo_id = ? AND pr.classifier_model = ?""",
                (self.workspace_id, photo_id, model),
            ).fetchone()
        return self.conn.execute(
            """SELECT pr.species, pr.confidence, pr.detection_id FROM predictions pr
               JOIN detections d ON d.id = pr.detection_id
               JOIN photos p ON p.id = d.photo_id
               JOIN workspace_folders wf
                 ON wf.folder_id = p.folder_id AND wf.workspace_id = ?
               WHERE d.photo_id = ?
                 AND pr.classifier_model = ?
                 AND pr.labels_fingerprint = ?""",
            (self.workspace_id, photo_id, model, labels_fingerprint),
        ).fetchone()

    def clear_group_info(self, detection_id, model,
                                    labels_fingerprint=None):
        """Drop stale group metadata from the cached prediction's review row.

        Used when a cached prediction that previously belonged to a
        reviewable burst is reused under a run where the burst is no longer
        group-reviewable (mixed species, singleton, etc.), so the caller
        would otherwise pass ``group_id=None`` to
        ``update_prediction_group_info`` and skip it. Without this, the old
        ``group_id`` / ``individual`` / vote counts stay attached and group
        actions retag the whole stale burst together.

        Only updates an existing ``prediction_review`` row — never inserts
        one — so the "absence == pending" invariant that ``add_prediction``
        enforces for un-reviewed detections stays intact.
        """
        ws = self.workspace_id
        if labels_fingerprint is not None:
            row = self.conn.execute(
                """SELECT pr.id FROM predictions pr
                   LEFT JOIN prediction_review pr_rev
                     ON pr_rev.prediction_id = pr.id AND pr_rev.workspace_id = ?
                   WHERE pr.detection_id = ? AND pr.classifier_model = ?
                     AND pr.labels_fingerprint = ?
                     AND COALESCE(pr_rev.status, 'pending') != 'alternative'
                   ORDER BY pr.confidence DESC LIMIT 1""",
                (ws, detection_id, model, labels_fingerprint),
            ).fetchone()
        else:
            row = self.conn.execute(
                """SELECT pr.id FROM predictions pr
                   LEFT JOIN prediction_review pr_rev
                     ON pr_rev.prediction_id = pr.id AND pr_rev.workspace_id = ?
                   WHERE pr.detection_id = ? AND pr.classifier_model = ?
                     AND COALESCE(pr_rev.status, 'pending') != 'alternative'
                   ORDER BY pr.confidence DESC LIMIT 1""",
                (ws, detection_id, model),
            ).fetchone()
        if not row:
            return
        self.conn.execute(
            """UPDATE prediction_review
                  SET individual  = NULL,
                      group_id    = NULL,
                      vote_count  = NULL,
                      total_votes = NULL
                WHERE prediction_id = ? AND workspace_id = ?""",
            (row["id"], ws),
        )
        self.commit_with_retry(self.conn)

    def update_group_info(self, detection_id, model, group_id,
                                     vote_count, total_votes, individual,
                                     labels_fingerprint=None):
        """Upsert group info for the primary prediction of
        (detection, classifier_model, labels_fingerprint) in the active
        workspace's ``prediction_review``.

        Alternative rows (review status ``'alternative'``) are intentionally
        skipped so they do not inherit grouping metadata that belongs to the
        primary pick.

        ``labels_fingerprint`` scopes the "primary" pick to one label set;
        omitting it picks the highest-confidence row across all fingerprints
        for back-compat with legacy callers, but current callers should
        always pass the active fingerprint so group metadata doesn't land
        on a row produced under a stale label set.
        """
        ws = self.workspace_id
        # Identify the primary prediction row for this (detection, model,
        # [fingerprint]), excluding any prediction already marked
        # 'alternative' in this workspace.
        if labels_fingerprint is not None:
            row = self.conn.execute(
                """SELECT pr.id FROM predictions pr
                   LEFT JOIN prediction_review pr_rev
                     ON pr_rev.prediction_id = pr.id AND pr_rev.workspace_id = ?
                   WHERE pr.detection_id = ? AND pr.classifier_model = ?
                     AND pr.labels_fingerprint = ?
                     AND COALESCE(pr_rev.status, 'pending') != 'alternative'
                   ORDER BY pr.confidence DESC LIMIT 1""",
                (ws, detection_id, model, labels_fingerprint),
            ).fetchone()
        else:
            row = self.conn.execute(
                """SELECT pr.id FROM predictions pr
                   LEFT JOIN prediction_review pr_rev
                     ON pr_rev.prediction_id = pr.id AND pr_rev.workspace_id = ?
                   WHERE pr.detection_id = ? AND pr.classifier_model = ?
                     AND COALESCE(pr_rev.status, 'pending') != 'alternative'
                   ORDER BY pr.confidence DESC LIMIT 1""",
                (ws, detection_id, model),
            ).fetchone()
        if not row:
            return
        pred_id = row["id"]
        self.conn.execute(
            """INSERT INTO prediction_review
                 (prediction_id, workspace_id, status, reviewed_at,
                  individual, group_id, vote_count, total_votes)
               VALUES (?, ?, 'pending', datetime('now'), ?, ?, ?, ?)
               ON CONFLICT(prediction_id, workspace_id)
               DO UPDATE SET individual  = excluded.individual,
                             group_id    = excluded.group_id,
                             vote_count  = excluded.vote_count,
                             total_votes = excluded.total_votes,
                             reviewed_at = excluded.reviewed_at""",
            (pred_id, ws, individual, group_id, vote_count, total_votes),
        )
        self.commit_with_retry(self.conn)

    def accept_subject_species(self, prediction_id, _commit=True):
        """Accept agreeing model predictions for one detected subject.

        Compare uses this for an additional-species suggestion: the species
        keyword is added once to the photo, the existing species keywords are
        preserved, and every current-model prediction that names the same
        species on the same detection is resolved together. Grouped
        predictions are explicitly limited to this photo so accepting a
        subject in Compare cannot silently tag the rest of a burst.

        ``_commit=False`` leaves the surrounding transaction open so the
        caller can bundle the accept with its own writes (edit history,
        precondition checks) inside one ``BEGIN IMMEDIATE`` — used by
        ``api_accept_subject_species`` to serialize with the rest of the
        prediction-decision routes.
        """
        ws = self.workspace_id
        target = self.conn.execute(
            """SELECT pr.id, pr.detection_id, pr.species, d.photo_id
               FROM predictions pr
               JOIN detections d ON d.id = pr.detection_id
               JOIN photos ph ON ph.id = d.photo_id
               JOIN workspace_folders wf
                 ON wf.folder_id = ph.folder_id AND wf.workspace_id = ?
               WHERE pr.id = ?""",
            (ws, prediction_id),
        ).fetchone()
        if target is None:
            return None

        agreeing = self.conn.execute(
            """SELECT pr.id
               FROM predictions pr
               LEFT JOIN prediction_review pr_rev
                 ON pr_rev.prediction_id = pr.id AND pr_rev.workspace_id = ?
               WHERE pr.detection_id = ?
                 AND lower(trim(pr.species)) = lower(trim(?))
                 AND COALESCE(pr_rev.status, 'pending') != 'rejected'
                 AND pr.labels_fingerprint = (
                     SELECT pr2.labels_fingerprint FROM predictions pr2
                     WHERE pr2.detection_id = pr.detection_id
                       AND pr2.classifier_model = pr.classifier_model
                     ORDER BY pr2.created_at DESC, pr2.id DESC
                     LIMIT 1
                 )
               ORDER BY pr.confidence DESC, pr.id ASC""",
            (ws, target["detection_id"], target["species"]),
        ).fetchall()

        accepted_ids = []
        affected = []
        result = None
        try:
            for row in agreeing:
                accepted = self.accept_prediction(
                    row["id"],
                    photo_ids=[target["photo_id"]],
                    _commit=False,
                )
                # A row whose grouped scope excluded this photo accepts
                # nothing, so it must not be reported as an accepted id (nor
                # supply the returned keyword, which is None when the species
                # keyword does not exist yet).
                if accepted and accepted["accepted_prediction_ids"]:
                    result = accepted
                    accepted_ids.append(row["id"])
                    affected.extend(accepted["affected"])
            if _commit:
                self.conn.commit()
        except Exception:
            if _commit:
                self.conn.rollback()
            raise

        if result is None:
            return None
        return {
            "species": result["species"],
            "keyword_id": result["keyword_id"],
            "photo_id": target["photo_id"],
            "prediction_ids": accepted_ids,
            "affected": affected,
        }

    def get_review_status(self, prediction_id, workspace_id):
        row = self.conn.execute(
            """SELECT status FROM prediction_review
               WHERE prediction_id = ? AND workspace_id = ?""",
            (prediction_id, workspace_id),
        ).fetchone()
        return row["status"] if row else "pending"

    def set_review_status(self, prediction_id, workspace_id, status,
                           individual=None, group_id=None):
        self.conn.execute(
            """INSERT INTO prediction_review
                 (prediction_id, workspace_id, status, reviewed_at, individual, group_id)
               VALUES (?, ?, ?, datetime('now'), ?, ?)
               ON CONFLICT(prediction_id, workspace_id)
               DO UPDATE SET status      = excluded.status,
                             reviewed_at = excluded.reviewed_at,
                             individual  = COALESCE(excluded.individual, individual),
                             group_id    = COALESCE(excluded.group_id,   group_id)""",
            (prediction_id, workspace_id, status, individual, group_id),
        )
        self.conn.commit()
