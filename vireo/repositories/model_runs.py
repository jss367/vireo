"""Persistence for model runs: detector and classifier run records.

The tables here (``detector_runs``, ``classifier_runs``,
``classifier_match_scores``, ``labels_fingerprints``) are global across
workspaces: a model's output is a pure function of (photo or detection,
model, label set). ``Database`` keeps the workspace-scoped config lookup:
the classify preflight methods take the active workspace's detector floor
as ``min_conf``, resolved by the façade through
``Database.get_effective_config``. The Pipeline inspector's per-photo run
diagnostics (``get_unscored_current_prediction_runs``,
``get_match_scores_for_photo``, ``current_prediction_detector_confidences``,
``classifier_runs_for_photo``) read ``predictions`` alongside the run tables
here; ``current_prediction_detector_confidences`` and
``classifier_runs_for_photo`` came from ``web/pipeline.py``, each serving the
real-detection and full-image reads its two copies made.

Callers reach it as ``db.model_runs`` (a fresh repository per access, see
``Database.model_runs``). ``Database.detector_run_is_pinned`` is the one
forwarding wrapper left, because the detection writes receive it as their
``is_pinned`` callback.
"""

import sqlite3
from collections.abc import Callable, Iterable, Mapping, Sequence
from typing import Any


class ModelRunsRepository:
    def __init__(
        self,
        conn: sqlite3.Connection,
        *,
        auto_match_review_marker: str,
        commit_with_retry: Callable[[sqlite3.Connection], None],
    ) -> None:
        self.conn = conn
        # ``prediction_review.individual`` value of reproducible auto-created
        # taxonomy-match reviews, which never pin a run.
        self.auto_match_review_marker = auto_match_review_marker
        # ``db.commit_with_retry``, passed in so repositories import no
        # ``db`` code and a monkeypatch of the module function still applies.
        self.commit_with_retry = commit_with_retry

    def record_detector_run(
        self,
        photo_id: int,
        detector_model: str,
        box_count: int,
        runtime_fingerprint: str = "legacy",
        input_fingerprint: str | None = None,
    ) -> None:
        """Record that ``detector_model`` was run on ``photo_id`` and commit.

        Global across workspaces — the output is a pure function of (photo, model).
        """
        self.conn.execute(
            """INSERT INTO detector_runs
                 (photo_id, detector_model, runtime_fingerprint,
                  input_fingerprint, box_count)
               VALUES (?, ?, ?, ?, ?)
               ON CONFLICT(photo_id, detector_model)
               DO UPDATE SET runtime_fingerprint = excluded.runtime_fingerprint,
                             input_fingerprint = excluded.input_fingerprint,
                             box_count = excluded.box_count,
                             run_at = datetime('now')""",
            (photo_id, detector_model, runtime_fingerprint,
             input_fingerprint, box_count),
        )
        self.conn.commit()

    def get_global_detection_stats(self) -> dict[str, int]:
        """Return global (workspace-agnostic) detector-cache counts.

        ``detector_runs`` is shared across workspaces by design — switching
        workspaces or bumping a threshold never invalidates these rows —
        so the settings page surfaces this as a single "N photos x M
        models cached" figure.
        """
        r = self.conn.execute(
            """SELECT COUNT(DISTINCT photo_id) AS photo_count,
                      COUNT(DISTINCT detector_model) AS model_count
               FROM detector_runs"""
        ).fetchone()
        return {"photo_count": r["photo_count"] or 0,
                "model_count": r["model_count"] or 0}

    def detector_run_is_pinned(self, photo_id: int, detector_model: str) -> bool:
        """Return whether a real manual review pins this detector output."""
        row = self.conn.execute(
            """SELECT 1
               FROM detections d
               JOIN predictions p ON p.detection_id = d.id
               JOIN prediction_review pr ON pr.prediction_id = p.id
               WHERE d.photo_id = ? AND d.detector_model = ?
                 AND pr.status IN ('accepted', 'rejected')
                 AND COALESCE(pr.individual, '') != ?
               LIMIT 1""",
            (photo_id, detector_model, self.auto_match_review_marker),
        ).fetchone()
        return row is not None

    def get_detector_run_photo_ids(
        self, detector_model: str, runtime_fingerprint: str | None = None,
    ) -> set[int]:
        """Return the set of photo_ids with a consistent cached detector run.

        Includes empty-scene photos (box_count=0) — which is the whole point:
        without this, we'd re-run the model forever on photos with no animals.

        Excludes torn states where ``detector_runs.box_count > 0`` but no matching
        row exists in ``detections``. That shape happens when a reclassify pass
        clears detections (via ``clear_detections``) and then the job fails
        before writing fresh rows (model init error, etc.). Leaving such
        photos in the skip set would strand them on full-image fallback
        until the user manually forces another reclassify.
        """
        params = [detector_model]
        runtime_clause = ""
        if runtime_fingerprint is not None:
            # A reviewed older runtime remains authoritative until an explicit
            # reclassify.  Include it in the hit set so callers avoid doing
            # inference whose result write_detection_batch would reject.
            runtime_clause = """
                 AND (
                      dr.runtime_fingerprint = ?
                      OR dr.runtime_fingerprint = 'legacy'
                      OR EXISTS (
                          SELECT 1
                          FROM detections pd
                          JOIN predictions pp ON pp.detection_id = pd.id
                          JOIN prediction_review pr ON pr.prediction_id = pp.id
                          WHERE pd.photo_id = dr.photo_id
                            AND pd.detector_model = dr.detector_model
                            AND pr.status IN ('accepted', 'rejected')
                            AND COALESCE(pr.individual, '') != ?
                      )
                 )"""
            params.extend([runtime_fingerprint, self.auto_match_review_marker])
        rows = self.conn.execute(
            """SELECT dr.photo_id
               FROM detector_runs dr
               WHERE dr.detector_model = ?
               """ + runtime_clause + """
                 AND (dr.box_count = 0
                      OR EXISTS (SELECT 1 FROM detections d
                                 WHERE d.photo_id = dr.photo_id
                                   AND d.detector_model = dr.detector_model))""",
            params,
        ).fetchall()
        return {r["photo_id"] for r in rows}

    def record_classifier_run(
        self,
        detection_id: int,
        classifier_model: str,
        labels_fingerprint: str,
        prediction_count: int,
        labels_fingerprint_full: str | None = None,
        runtime_fingerprint: str = "legacy",
        input_fingerprint: str | None = None,
        input_recipe: str | None = None,
    ) -> None:
        """Upsert the classifier_runs row and commit with retry."""
        self.conn.execute(
            """INSERT INTO classifier_runs
                 (detection_id, classifier_model, labels_fingerprint,
                  labels_fingerprint_full, runtime_fingerprint,
                  input_fingerprint, prediction_count, input_recipe)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(detection_id, classifier_model, labels_fingerprint)
               DO UPDATE SET labels_fingerprint_full =
                                 excluded.labels_fingerprint_full,
                             runtime_fingerprint = excluded.runtime_fingerprint,
                             input_fingerprint = excluded.input_fingerprint,
                             prediction_count = excluded.prediction_count,
                             input_recipe = excluded.input_recipe,
                             run_at = datetime('now')""",
            (detection_id, classifier_model, labels_fingerprint,
             labels_fingerprint_full, runtime_fingerprint,
             input_fingerprint, prediction_count, input_recipe),
        )
        self.commit_with_retry(self.conn)

    def record_classifier_match_score(
        self,
        detection_id: int,
        classifier_model: str,
        labels_fingerprint: str,
        max_match_score: float,
        match_margin: float | None = None,
        top_species: str | None = None,
        label_count: int | None = None,
        score_kind: str | None = None,
    ) -> None:
        """Record how well the best label in a list actually matched; commit with retry.

        Written for every completed run, including runs that produced no
        prediction at all — unlike ``record_classifier_run``, whose zero-count
        rows are suppressed because that table gates re-classification. A run
        that matched nothing is the most informative case here, so suppressing
        it would defeat the purpose.

        ``max_match_score`` must be the best score over the entire label list,
        not merely over the predictions that cleared the confidence threshold.
        """
        self.conn.execute(
            """INSERT INTO classifier_match_scores
                 (detection_id, classifier_model, labels_fingerprint,
                  max_match_score, match_margin, top_species, label_count,
                  score_kind)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(detection_id, classifier_model, labels_fingerprint)
               DO UPDATE SET max_match_score = excluded.max_match_score,
                             match_margin = excluded.match_margin,
                             top_species = excluded.top_species,
                             label_count = excluded.label_count,
                             score_kind = excluded.score_kind,
                             run_at = datetime('now')""",
            (detection_id, classifier_model, labels_fingerprint,
             max_match_score, match_margin, top_species, label_count,
             score_kind),
        )
        self.commit_with_retry(self.conn)

    def has_classifier_match_score(
        self, detection_id: int, classifier_model: str, labels_fingerprint: str,
    ) -> bool:
        """True when ``classifier_match_scores`` records this exact run.

        A completed classifier run whose every label fell under the
        confidence floor writes a match-score row but no prediction rows —
        that outcome ("nothing in your list fits") is exactly what the
        feature exists to record, and the per-detection cache gate in
        ``classify_job._classify_photos`` uses this to honor it instead of
        re-running the model on a stored no-match.
        """
        return self.conn.execute(
            """SELECT 1 FROM classifier_match_scores
               WHERE detection_id = ?
                 AND classifier_model = ?
                 AND labels_fingerprint = ?
               LIMIT 1""",
            (detection_id, classifier_model, labels_fingerprint),
        ).fetchone() is not None

    def get_unscored_current_prediction_runs(self, photo_id: int) -> list[dict[str, Any]]:
        """Return ``(detection_id, classifier_model)`` pairs displayed without a score.

        A migrated catalog carries predictions from models that ran before
        ``classifier_match_scores`` existed: those predictions still surface in
        the panel because ``get_predictions`` pins to their (still latest)
        ``labels_fingerprint``, but the score table is empty for them. The
        blanket "no label in this list matches" verdict must not be applied
        over those rows — the legacy model was never judged.

        Returns one entry per current-fingerprint ``(detection, model)`` pair
        that has at least one prediction row on the photo but no row in
        ``classifier_match_scores`` under the same fingerprint (as dicts that also
        carry ``detector_model`` and ``labels_fingerprint``). Callers hand these
        to ``match_confidence.summarize_photo`` so the photo-level rollup can
        degrade to ``uncalibrated`` rather than declaring every displayed
        prediction unlisted.

        Not workspace-scoped — ``photo_id`` is assumed already verified by the
        caller, as the existing per-photo routes do before reaching here.
        """
        rows = self.conn.execute(
            """SELECT DISTINCT pr.detection_id AS detection_id,
                      pr.classifier_model AS classifier_model,
                      d.detector_model AS detector_model,
                      pr.labels_fingerprint AS labels_fingerprint
               FROM predictions pr
               JOIN detections d ON d.id = pr.detection_id
               WHERE d.photo_id = ?
                 AND pr.labels_fingerprint = (
                    SELECT pr2.labels_fingerprint FROM predictions pr2
                    WHERE pr2.detection_id = pr.detection_id
                      AND pr2.classifier_model = pr.classifier_model
                    ORDER BY pr2.created_at DESC, pr2.id DESC
                    LIMIT 1
                 )
                 AND NOT EXISTS (
                    SELECT 1 FROM classifier_match_scores cms
                    WHERE cms.detection_id = pr.detection_id
                      AND cms.classifier_model = pr.classifier_model
                      AND cms.labels_fingerprint = pr.labels_fingerprint
                 )""",
            (photo_id,),
        ).fetchall()
        return [dict(r) for r in rows]

    def get_match_scores_for_photo(self, photo_id: int) -> list[dict[str, Any]]:
        """Return match-score rows for every detection on one photo.

        Rows are returned for all detections regardless of detector threshold:
        the caller decides what to show, and a detection hidden by the current
        threshold is often exactly the one a user is asking about.

        Every run is returned, including ones superseded by a later
        re-classification against a different label list — the Pipeline
        Inspector's per-run table deliberately shows the history. Each row is
        stamped ``is_current`` so the user-facing verdict can be built from the
        same label set as the predictions on screen: re-running a detection
        against a second list leaves the first list's row in this table, and a
        strong match from an abandoned list must not be allowed to certify the
        weak list that replaced it.

        ``is_current`` follows ``get_predictions``: the latest
        ``labels_fingerprint`` per ``(detection_id, classifier_model)`` as the
        predictions table orders it. A run that produced no prediction at all
        has no row to pin against — and that run is the single most important
        one here — so it falls back to the most recent match-score row for the
        same pair.

        Not workspace-scoped — ``photo_id`` is assumed already verified by the
        caller, as the existing per-photo routes do before reaching here.
        """
        rows = self.conn.execute(
            """SELECT cms.*, d.detector_confidence, d.detector_model,
                      CASE WHEN cms.labels_fingerprint = COALESCE(
                             (SELECT pr2.labels_fingerprint FROM predictions pr2
                               WHERE pr2.detection_id = cms.detection_id
                                 AND pr2.classifier_model = cms.classifier_model
                               ORDER BY pr2.created_at DESC, pr2.id DESC
                               LIMIT 1),
                             (SELECT cms2.labels_fingerprint
                                FROM classifier_match_scores cms2
                               WHERE cms2.detection_id = cms.detection_id
                                 AND cms2.classifier_model = cms.classifier_model
                               ORDER BY cms2.run_at DESC, cms2.rowid DESC
                               LIMIT 1)
                           ) THEN 1 ELSE 0 END AS is_current
               FROM classifier_match_scores cms
               JOIN detections d ON d.id = cms.detection_id
               WHERE d.photo_id = ?
               ORDER BY cms.max_match_score DESC""",
            (photo_id,),
        ).fetchall()
        return [dict(r) for r in rows]

    def current_prediction_detector_confidences(self, photo_id: int, *, full_image: bool) -> list[sqlite3.Row]:
        """Rows (``id``, ``detector_confidence``) of a photo's current-label-set predictions.

        Current means the latest ``labels_fingerprint`` per (detection,
        classifier model). ``full_image`` picks the full-image
        pseudo-detection's predictions; otherwise every real detection's,
        whatever its confidence. Not workspace-scoped.
        """
        detector_test = "=" if full_image else "!="
        return self.conn.execute(
            f"""SELECT pr.id, d.detector_confidence
               FROM predictions pr
               JOIN detections d ON d.id = pr.detection_id
               WHERE d.photo_id = ?
                 AND d.detector_model {detector_test} 'full-image'
                 AND pr.labels_fingerprint = (
                    SELECT pr2.labels_fingerprint FROM predictions pr2
                    WHERE pr2.detection_id = pr.detection_id
                      AND pr2.classifier_model = pr.classifier_model
                    ORDER BY pr2.created_at DESC, pr2.id DESC
                    LIMIT 1
                 )""",
            (photo_id,),
        ).fetchall()

    def classifier_runs_for_photo(self, photo_id: int, *, full_image: bool) -> list[sqlite3.Row]:
        """Rows (``prediction_count``, ``detector_confidence``) of a photo's classifier runs.

        ``full_image`` picks the runs on the full-image pseudo-detection;
        otherwise the runs on every real detection, whatever its confidence.
        """
        detector_test = "=" if full_image else "!="
        return self.conn.execute(
            f"""SELECT cr.prediction_count, d.detector_confidence
               FROM classifier_runs cr
               JOIN detections d ON d.id = cr.detection_id
               WHERE d.photo_id = ?
                 AND d.detector_model {detector_test} 'full-image'""",
            (photo_id,),
        ).fetchall()

    def get_classifier_run_keys(
        self, detection_id: int, runtime_fingerprint: str | None = None,
    ) -> set[tuple[str, str]]:
        """Return the (model, fingerprint) keys the runtime gate honors."""
        params = [detection_id]
        runtime_clause = ""
        if runtime_fingerprint is not None:
            runtime_clause = """
               AND (
                    cr.runtime_fingerprint = ?
                    OR cr.runtime_fingerprint = 'legacy'
                    OR EXISTS (
                        SELECT 1 FROM predictions p
                        JOIN prediction_review pr ON pr.prediction_id = p.id
                        WHERE p.detection_id = cr.detection_id
                          AND p.classifier_model = cr.classifier_model
                          AND p.labels_fingerprint = cr.labels_fingerprint
                          AND pr.status IN ('accepted', 'rejected')
                          AND COALESCE(pr.individual, '') != ?
                    )
               )"""
            params.extend([runtime_fingerprint, self.auto_match_review_marker])
        rows = self.conn.execute(
            """SELECT cr.classifier_model, cr.labels_fingerprint
               FROM classifier_runs cr
               WHERE cr.detection_id = ? AND cr.input_recipe IS NULL AND cr.runtime_fingerprint != 'incomplete'""" + runtime_clause,
            params,
        ).fetchall()
        return {(r["classifier_model"], r["labels_fingerprint"]) for r in rows}

    def get_classifier_run_key_gate(
        self, detection_id: int, runtime_fingerprint: str | None,
    ) -> tuple[set[tuple[str, str]], set[tuple[str, str]]]:
        """Return ``(accepted, rejected)`` classifier-run key sets for a detection.

        These gates serve normal-image runs. A RAW recipe always requires
        fresh normal inference, even when a manual decision pins its species.

        ``accepted`` mirrors what ``get_classifier_run_keys(detection_id,
        runtime_fingerprint=runtime_fingerprint)`` returns — keys whose row
        the runtime cache gate would honor for this detection.

        ``rejected`` are keys that DO have a classifier_runs row for the
        detection but whose row would be filtered out by the fingerprint
        rule (fingerprint mismatch, not ``'legacy'``, and no per-
        prediction ``prediction_review`` override marks them as still
        valid). The pipeline uses this set to reconcile the cache-hit
        preflight — ``count_classifier_runs`` counts every existing row
        regardless of runtime_fingerprint, so a photo whose only row is
        rejected here would otherwise sit in ``cached_est`` yet never
        register as a cache hit or as a fall-through miss, leaving
        ``_classification_eta_progress`` believing a phantom future cache
        hit is still coming.

        ``runtime_fingerprint`` must be provided; passing ``None`` would
        make every row look mismatched, which is not a useful signal
        (that's the reclassify path, where the gate is bypassed anyway).
        """
        if runtime_fingerprint is None:
            return set(), set()
        rows = self.conn.execute(
            """SELECT cr.classifier_model,
                      cr.labels_fingerprint,
                      cr.runtime_fingerprint, cr.input_recipe,
                      EXISTS (
                          SELECT 1 FROM predictions p
                          JOIN prediction_review pr ON pr.prediction_id = p.id
                          WHERE p.detection_id = cr.detection_id
                            AND p.classifier_model = cr.classifier_model
                            AND p.labels_fingerprint = cr.labels_fingerprint
                            AND pr.status IN ('accepted', 'rejected')
                            AND COALESCE(pr.individual, '') != ?
                      ) AS has_individual_override
                 FROM classifier_runs cr
                WHERE cr.detection_id = ?""",
            (self.auto_match_review_marker, detection_id),
        ).fetchall()
        accepted, rejected = set(), set()
        for row in rows:
            key = (row["classifier_model"], row["labels_fingerprint"])
            if row["runtime_fingerprint"] != "incomplete" and row["input_recipe"] is None and (
                row["runtime_fingerprint"] == runtime_fingerprint
                or row["runtime_fingerprint"] == "legacy"
                or row["has_individual_override"]
            ):
                accepted.add(key)
            else:
                rejected.add(key)
        return accepted, rejected

    def get_classifier_run_cache_hits(
        self,
        photo_ids: Sequence[int],
        classifier_model: str,
        labels_fingerprint: str,
        *,
        min_conf: float,
        contextual_weak_photo_ids: Iterable[int] | None = None,
        weak_confidence: float | None = None,
        fresh_detections_by_photo: Mapping[int, Sequence[Mapping[str, Any]]] | None = None,
        fresh_processed_photo_ids: Iterable[int] | None = None,
        expected_classifier_runtime_by_detector_runtime: Mapping[str, str | None] | None = None,
    ) -> set[int]:
        """Return the photo ids the classify preflight counts as cached."""
        query = _CacheHitQuery(
            self.conn,
            photo_ids,
            classifier_model,
            labels_fingerprint,
            min_conf=min_conf,
            contextual_weak_photo_ids=contextual_weak_photo_ids,
            weak_confidence=weak_confidence,
            fresh_detections_by_photo=fresh_detections_by_photo,
            fresh_processed_photo_ids=fresh_processed_photo_ids,
            expected_classifier_runtime_by_detector_runtime=(
                expected_classifier_runtime_by_detector_runtime
            ),
            auto_match_review_marker=self.auto_match_review_marker,
        )
        query.select_fresh_candidates()
        query.route_unselected_fresh_weak_photos_to_db()
        query.add_fresh_full_image_anchors()
        query.match_fresh_candidates()
        query.match_db_photos()
        query.match_db_weak_photos()
        query.match_db_full_image_anchors()
        return query.matched

    def get_unclassifiable_photos(
        self,
        photo_ids: Sequence[int],
        *,
        min_conf: float,
        contextual_weak_photo_ids: Iterable[int] | None = None,
        weak_confidence: float | None = None,
        fresh_detections_by_photo: Mapping[int, Sequence[Mapping[str, Any]]] | None = None,
        fresh_processed_photo_ids: Iterable[int] | None = None,
    ) -> set[int]:
        """Return photo ids the classify runtime skips without inference."""
        weak_photo_ids = set(contextual_weak_photo_ids or ())
        weak_conf = (
            float(weak_confidence) if weak_confidence is not None else min_conf
        )
        # Chunk to stay under SQLITE_MAX_VARIABLE_NUMBER (default 999).
        CHUNK = 500

        def _fresh_has_animal_above(photo_id, floor, *, contextual_weak=False):
            dets = fresh_detections_by_photo.get(photo_id) or ()
            for d in dets:
                if d.get("category", "animal") != "animal":
                    continue
                if (
                    contextual_weak
                    and d.get("detector_model") != "megadetector-v6"
                ):
                    continue
                conf = d.get("confidence", d.get("detector_confidence", 0))
                try:
                    if float(conf or 0) >= floor:
                        return True
                except (TypeError, ValueError):
                    continue
            return False

        def _fresh_has_confident_non_animal(photo_id):
            for detection in fresh_detections_by_photo.get(photo_id) or ():
                if detection.get("detector_model") == "full-image":
                    continue
                if detection.get("category", "animal") == "animal":
                    continue
                confidence = detection.get(
                    "confidence", detection.get("detector_confidence", 0),
                )
                try:
                    if float(confidence or 0) >= min_conf:
                        return True
                except (TypeError, ValueError):
                    continue
            return False

        def _db_unclassifiable(pool, floor, out, *, contextual_weak=False):
            animal_model_predicate = (
                "d2.detector_model = 'megadetector-v6'"
                if contextual_weak
                else "d2.detector_model != 'full-image'"
            )
            for i in range(0, len(pool), CHUNK):
                chunk = pool[i:i + CHUNK]
                if not chunk:
                    continue
                placeholders = ",".join("?" * len(chunk))
                rows = self.conn.execute(
                    f"""SELECT DISTINCT d.photo_id
                          FROM detections d
                         WHERE d.detector_model != 'full-image'
                           AND d.category != 'animal'
                           AND d.detector_confidence >= ?
                           AND d.photo_id IN ({placeholders})
                           AND NOT EXISTS (
                                 SELECT 1 FROM detections d2
                                  WHERE d2.photo_id = d.photo_id
                                    AND {animal_model_predicate}
                                    AND d2.category = 'animal'
                                    AND d2.detector_confidence >= ?
                               )""",
                    [min_conf, *chunk, floor],
                ).fetchall()
                for r in rows:
                    out.add(r["photo_id"])

        unclassifiable: set = set()
        normal_ids = [pid for pid in photo_ids if pid not in weak_photo_ids]
        weak_ids = [pid for pid in photo_ids if pid in weak_photo_ids]

        if fresh_detections_by_photo is None:
            for pool, floor, is_weak in (
                (normal_ids, min_conf, False),
                (weak_ids, weak_conf, True),
            ):
                _db_unclassifiable(
                    pool, floor, unclassifiable, contextual_weak=is_weak,
                )
            return unclassifiable

        # Fresh-detection path: the "photo has an animal detection >= floor?"
        # test uses the fresh in-memory map (matching the runtime's
        # ``photo_dets`` filter), while "photo has any real detection?"
        # still reads the DB — that mirrors the runtime's ``raw_real_dets``
        # branch, which also reads the DB and would enter the skip path
        # even if a pre-run row is the only real detection recorded.
        #
        # Photos absent from ``fresh_processed_photo_ids`` are ones the
        # detector failed on this run; runtime falls back to DB rows for
        # them, so evaluate them against the DB predicate too (otherwise
        # the missing entry looks like "no fresh animal" and we'd
        # misclassify them as unclassifiable — Codex #1468 P2).
        processed = (
            set(fresh_processed_photo_ids)
            if fresh_processed_photo_ids is not None else None
        )
        for pool, floor, is_weak in (
            (normal_ids, min_conf, False),
            (weak_ids, weak_conf, True),
        ):
            if processed is None:
                fresh_pool = pool
                db_pool: list = []
            else:
                # Runtime reloads a contextual-weak row from the DB when the
                # ordinary-threshold detector map omitted it. Keep that same
                # fallback instead of treating an empty fresh entry as a skip.
                fresh_pool = [
                    pid for pid in pool
                    if pid in processed
                    and (
                        not is_weak
                        or _fresh_has_animal_above(
                            pid, floor, contextual_weak=True,
                        )
                    )
                ]
                db_pool = [pid for pid in pool if pid not in fresh_pool]
            for pid in fresh_pool:
                if (
                    not _fresh_has_animal_above(
                        pid, floor, contextual_weak=is_weak,
                    )
                    and _fresh_has_confident_non_animal(pid)
                ):
                    unclassifiable.add(pid)
            if db_pool:
                _db_unclassifiable(
                    db_pool, floor, unclassifiable,
                    contextual_weak=is_weak,
                )
        return unclassifiable

    def get_labels_fingerprints(self) -> list[dict[str, Any]]:
        """Return all rows from the labels_fingerprints sidecar, sources decoded.

        Each row records the (fingerprint, sources, label_count) triple a
        classify run wrote — single-file runs list one source, merged-set
        runs list several. Used by the inventory endpoint to identify
        merged fingerprints that are still current (sources on disk and
        unchanged) so they don't get marked stale.
        """
        import json
        rows = self.conn.execute(
            "SELECT fingerprint, full_fingerprint, display_name, "
            "sources_json, label_count "
            "FROM labels_fingerprints"
        ).fetchall()
        out = []
        for r in rows:
            try:
                sources = json.loads(r["sources_json"] or "[]")
            except (TypeError, ValueError):
                sources = []
            out.append({
                "fingerprint": r["fingerprint"],
                "full_fingerprint": r["full_fingerprint"],
                "display_name": r["display_name"],
                "sources": sources,
                "label_count": r["label_count"],
            })
        return out

    def upsert_labels_fingerprint(
        self,
        fingerprint: str,
        display_name: str | None,
        sources: Sequence[str] | None,
        label_count: int | None,
        full_fingerprint: str | None = None,
    ) -> None:
        """Upsert a labels_fingerprints row and commit."""
        import json
        # COALESCE full_fingerprint so a later call recording the same
        # short fingerprint without the 64-char digest (e.g. after an
        # OSError/ValueError fallback in _record_labels_fingerprint) does
        # not overwrite a previously stored full digest with NULL.
        self.conn.execute(
            """INSERT INTO labels_fingerprints
                 (fingerprint, full_fingerprint, display_name,
                  sources_json, label_count)
               VALUES (?, ?, ?, ?, ?)
               ON CONFLICT(fingerprint)
               DO UPDATE SET full_fingerprint = COALESCE(
                                 excluded.full_fingerprint,
                                 labels_fingerprints.full_fingerprint
                             ),
                             display_name = excluded.display_name,
                             sources_json = excluded.sources_json,
                             label_count  = excluded.label_count""",
            (fingerprint, full_fingerprint, display_name,
             json.dumps(sources or []), label_count),
        )
        self.conn.commit()


def _runtime_predicate(
    alias, strict_pairs, permissive_det_rts, auto_match_review_marker,
):
    """Return the runtime-fingerprint predicate for classifier_runs ``alias``.

    Assemble a predicate that, given a classifier_runs alias
    ``{cr}``, requires at least one of:
      1. ``{cr}.runtime_fingerprint`` matches the expected value for
         the anchor detection's ``detections.runtime_fingerprint``.
      2. ``{cr}.runtime_fingerprint`` is ``'legacy'`` (grandfathered).
      3. A prediction on the same row carries a real
         prediction_review override (mirrors ``get_classifier_run_keys``).
    A permissive detector-runtime entry (expected value ``None``)
    accepts any classifier_runs.runtime_fingerprint for its
    detections, matching the pipeline's unfiltered fallback when
    portable identity is not wired up.
    """
    pair_terms = " OR ".join(
        f"(d_src.runtime_fingerprint IS ? AND {alias}.runtime_fingerprint IS ?)"
        for _ in strict_pairs
    )
    permissive_terms = " OR ".join(
        "d_src.runtime_fingerprint IS ?"
        for _ in permissive_det_rts
    )
    detector_match_terms = " OR ".join(
        part for part in (pair_terms, permissive_terms) if part
    )
    pred_sql = (
        f" AND ({alias}.runtime_fingerprint = 'legacy'"
        f" OR EXISTS (SELECT 1 FROM detections d_src"
        f"             WHERE d_src.id = {alias}.detection_id"
        f"               AND ({detector_match_terms}))"
        f" OR EXISTS (SELECT 1 FROM predictions p_ov"
        f"             JOIN prediction_review pr_ov"
        f"               ON pr_ov.prediction_id = p_ov.id"
        f"            WHERE p_ov.detection_id = {alias}.detection_id"
        f"              AND p_ov.classifier_model = {alias}.classifier_model"
        f"              AND p_ov.labels_fingerprint = {alias}.labels_fingerprint"
        f"              AND pr_ov.status IN ('accepted', 'rejected')"
        f"              AND COALESCE(pr_ov.individual, '') != ?))"
    )
    params: list = []
    for det_rt, cls_rt in strict_pairs:
        params.extend([det_rt, cls_rt])
    params.extend(permissive_det_rts)
    params.append(auto_match_review_marker)
    return pred_sql, params


def _classifier_run_predicate(rt_map, auto_match_review_marker):
    """Return the ``cr`` predicate SQL and params every cache query appends.

    Runtime-fingerprint gate for classifier_runs, mirroring what
    ``get_classifier_run_key_gate`` accepts at runtime. Without this
    predicate the preflight would count rows whose ``runtime_fingerprint``
    the runtime rejects (typically stale after a detector fingerprint
    roll), and no observation could correct the estimate until those
    rows were visited — collapsing ``remaining_uncached`` to zero
    prematurely (Codex #1468 P2).
    """
    rt_predicate_sql = ""
    rt_predicate_params: list = []
    if rt_map is not None and rt_map:
        strict_pairs = [
            (det_rt, cls_rt)
            for det_rt, cls_rt in rt_map.items()
            if cls_rt is not None
        ]
        permissive_det_rts = [
            det_rt
            for det_rt, cls_rt in rt_map.items()
            if cls_rt is None
        ]
        rt_predicate_sql, rt_predicate_params = _runtime_predicate(
            "cr", strict_pairs, permissive_det_rts, auto_match_review_marker,
        )

    # RAW outputs cannot satisfy a normal-image run, even when reviewed.
    rt_predicate_sql += " AND cr.input_recipe IS NULL AND cr.runtime_fingerprint != 'incomplete'"
    return rt_predicate_sql, rt_predicate_params


def _fresh_candidate_detections(detections, is_weak, floor):
    """Return the in-memory detections the classify loop would target."""
    candidates = []
    for detection in detections:
        if detection.get("detector_model") == "full-image":
            continue
        if detection.get("category", "animal") != "animal":
            continue
        if (
            is_weak
            and detection.get("detector_model") != "megadetector-v6"
        ):
            continue
        confidence = detection.get(
            "confidence", detection.get("detector_confidence", 0),
        )
        try:
            if float(confidence or 0) < floor:
                continue
        except (TypeError, ValueError):
            continue
        if detection.get("id") is not None:
            candidates.append(detection)
    if is_weak and candidates:
        candidates = sorted(
            candidates,
            key=lambda detection: (
                -float(
                    detection.get(
                        "confidence",
                        detection.get("detector_confidence", 0),
                    ) or 0
                ),
                detection.get("id", 0),
            ),
        )[:1]
    return candidates


def _has_confident_non_animal(detections, min_conf):
    """Whether a real non-animal box at ``min_conf`` blocks the anchor."""
    for detection in detections:
        if detection.get("detector_model") == "full-image":
            continue
        if detection.get("category", "animal") == "animal":
            continue
        confidence = detection.get(
            "confidence", detection.get("detector_confidence", 0),
        )
        try:
            if float(confidence or 0) >= min_conf:
                return True
        except (TypeError, ValueError):
            continue
    return False


class _CacheHitQuery:
    """One ``get_classifier_run_cache_hits`` call: the id partitions, the
    shared ``cr`` predicate, the fresh candidate map and the matched set."""

    # Chunk to stay under SQLITE_MAX_VARIABLE_NUMBER (default 999).
    # Match the 500-element chunks used elsewhere in this file.
    CHUNK = 500

    def __init__(
        self,
        conn,
        photo_ids,
        classifier_model,
        labels_fingerprint,
        *,
        min_conf,
        contextual_weak_photo_ids,
        weak_confidence,
        fresh_detections_by_photo,
        fresh_processed_photo_ids,
        expected_classifier_runtime_by_detector_runtime,
        auto_match_review_marker,
    ):
        self.conn = conn
        self.classifier_model = classifier_model
        self.labels_fingerprint = labels_fingerprint
        self.min_conf = min_conf
        self.fresh_detections_by_photo = fresh_detections_by_photo
        self.weak_photo_ids = set(contextual_weak_photo_ids or ())
        self.weak_conf = (
            float(weak_confidence) if weak_confidence is not None else min_conf
        )
        self.normal_ids = [
            pid for pid in photo_ids if pid not in self.weak_photo_ids
        ]
        self.weak_ids = [pid for pid in photo_ids if pid in self.weak_photo_ids]
        self.fresh_processed = (
            set(fresh_processed_photo_ids or ())
            if fresh_detections_by_photo is not None
            and fresh_processed_photo_ids is not None
            else set()
        )
        self.normal_db_ids = [
            pid for pid in self.normal_ids if pid not in self.fresh_processed
        ]
        self.weak_db_ids = [
            pid for pid in self.weak_ids if pid not in self.fresh_processed
        ]
        self.matched = set()
        self.rt_predicate_sql, self.rt_predicate_params = (
            _classifier_run_predicate(
                expected_classifier_runtime_by_detector_runtime,
                auto_match_review_marker,
            )
        )
        self.fresh_candidates: dict = {}

    def _chunks(self, ids):
        """Yield ``(chunk, placeholders)`` for each CHUNK-sized slice."""
        for i in range(0, len(ids), self.CHUNK):
            chunk = ids[i:i + self.CHUNK]
            yield chunk, ",".join("?" * len(chunk))

    def select_fresh_candidates(self):
        """For photos whose detector iteration completed, mirror the runtime's
        in-memory target selection rather than querying every detection row
        still present in the DB. Old rows can legitimately survive until a
        later purge and are not runtime candidates for this pass.
        """
        for photo_id in self.fresh_processed:
            is_weak = photo_id in self.weak_photo_ids
            floor = self.weak_conf if is_weak else self.min_conf
            candidates = _fresh_candidate_detections(
                self.fresh_detections_by_photo.get(photo_id) or (),
                is_weak,
                floor,
            )
            if candidates:
                self.fresh_candidates[photo_id] = {
                    detection["id"] for detection in candidates
                }

    def route_unselected_fresh_weak_photos_to_db(self):
        """Cached detector reuse loads rows at the ordinary workspace floor.
        A contextual-weak target can therefore be absent from the in-memory
        map even though the classify loop will explicitly reload its MDv6
        weak row from the DB. Route those omitted weak photos through the
        same DB fallback here (Codex #1468 P2).
        """
        self.weak_db_ids.extend(
            photo_id for photo_id in self.weak_ids
            if photo_id in self.fresh_processed
            and photo_id not in self.fresh_candidates
        )

    def add_fresh_full_image_anchors(self):
        """Ordinary processed photos with no usable animal crop now take the
        full-image fallback unless a confident person/vehicle box blocks it.
        The detect stage pre-creates those anchors before this preflight, so
        add the anchor ID to the same candidate map used for fresh crops.
        """
        fresh_full_image_ids = []
        for photo_id in self.normal_ids:
            if (
                photo_id not in self.fresh_processed
                or photo_id in self.fresh_candidates
            ):
                continue
            if not _has_confident_non_animal(
                self.fresh_detections_by_photo.get(photo_id) or (),
                self.min_conf,
            ):
                fresh_full_image_ids.append(photo_id)
        for chunk, placeholders in self._chunks(fresh_full_image_ids):
            rows = self.conn.execute(
                f"""SELECT photo_id, MIN(id) AS detection_id
                      FROM detections
                     WHERE detector_model = 'full-image'
                       AND photo_id IN ({placeholders})
                     GROUP BY photo_id""",
                chunk,
            ).fetchall()
            for row in rows:
                self.fresh_candidates[row["photo_id"]] = {row["detection_id"]}

    def match_fresh_candidates(self):
        """Count a fresh photo when every candidate detection is cached."""
        fresh_detection_ids = {
            detection_id
            for candidate_ids in self.fresh_candidates.values()
            for detection_id in candidate_ids
        }
        cached_fresh_detection_ids: set = set()
        fresh_detection_ids_list = list(fresh_detection_ids)
        for chunk, placeholders in self._chunks(fresh_detection_ids_list):
            rows = self.conn.execute(
                f"""SELECT DISTINCT cr.detection_id
                      FROM classifier_runs cr
                     WHERE cr.detection_id IN ({placeholders})
                       AND cr.classifier_model = ?
                       AND cr.labels_fingerprint = ?
                       AND EXISTS (
                             SELECT 1 FROM predictions p
                              WHERE p.detection_id = cr.detection_id
                                AND p.classifier_model = cr.classifier_model
                                AND p.labels_fingerprint
                                    = cr.labels_fingerprint
                                AND p.confidence >= 0
                           )""" + self.rt_predicate_sql,
                [*chunk, self.classifier_model, self.labels_fingerprint,
                 *self.rt_predicate_params],
            ).fetchall()
            cached_fresh_detection_ids.update(
                row["detection_id"] for row in rows
            )
        for photo_id, candidate_ids in self.fresh_candidates.items():
            if candidate_ids <= cached_fresh_detection_ids:
                self.matched.add(photo_id)

    def match_db_photos(self):
        """A photo counts as fully cached iff it has at least one
        above-threshold real detection AND every above-threshold real
        detection carries a matching (classifier_model,
        labels_fingerprint) run key. The outer NOT EXISTS is the
        "no uncached qualifying detection remains" clause; the outer
        WHERE also requires at least one qualifying detection so
        empty-detection photos don't fall through this branch (they
        are handled by the full-image anchor branch below).
        The category='animal' predicate on both the outer and inner
        detection scans mirrors the runtime classify loop's
        non-animal skip (MegaDetector can return person/vehicle
        boxes above the confidence threshold, and the classifier
        stage filters them out before inference). Without matching
        the runtime filter here, a photo with cached animal
        detections plus one uncached person/vehicle box would be
        excluded from ``cached_estimate`` even though that photo
        will actually be entirely cache-served at runtime, so the
        UI would understate cached work.
        """
        for chunk, placeholders in self._chunks(self.normal_db_ids):
            rows = self.conn.execute(
                f"SELECT DISTINCT d.photo_id "
                f"FROM detections d "
                f"WHERE d.detector_model != 'full-image' "
                f"  AND d.category = 'animal' "
                f"  AND d.detector_confidence >= ? "
                f"  AND d.photo_id IN ({placeholders}) "
                f"  AND NOT EXISTS ( "
                f"    SELECT 1 FROM detections d2 "
                f"    WHERE d2.photo_id = d.photo_id "
                f"      AND d2.detector_model != 'full-image' "
                f"      AND d2.category = 'animal' "
                f"      AND d2.detector_confidence >= ? "
                f"      AND NOT EXISTS ( "
                f"        SELECT 1 FROM classifier_runs cr "
                f"        WHERE cr.detection_id = d2.id "
                f"          AND cr.classifier_model = ? "
                f"          AND cr.labels_fingerprint = ? "
                f"          AND EXISTS ( "
                f"              SELECT 1 FROM predictions p "
                f"              WHERE p.detection_id = cr.detection_id "
                f"                AND p.classifier_model "
                f"                    = cr.classifier_model "
                f"                AND p.labels_fingerprint "
                f"                    = cr.labels_fingerprint "
                f"                AND p.confidence >= 0 "
                f"          ) "
                + self.rt_predicate_sql +
                "      ) "
                "  )",
                [self.min_conf, *chunk, self.min_conf, self.classifier_model,
                 self.labels_fingerprint, *self.rt_predicate_params],
            ).fetchall()
            for r in rows:
                self.matched.add(r["photo_id"])

    def match_db_weak_photos(self):
        """Contextual-weak photos: the runtime classifies a SINGLE
        weak-threshold detection per photo (``photo_dets[:1]`` in the
        classify loop, after ordering by ``detector_confidence DESC,
        id ASC`` in ``get_detections``). Mirror that by counting the
        photo only when THAT top detection carries a matching run
        key. An earlier revision accepted any qualifying weak
        detection with a run key, which meant a photo whose only
        cached row was a lower-ranked box was marked cached even
        though the runtime would infer the uncached top box; that
        miss never registered as a fall-through overcount either
        (the top detection has no run key at all), leaving a phantom
        cache hit in the ETA (Codex #1468 P2).

        Restrict the CTE to ``detector_model = 'megadetector-v6'`` to
        mirror the runtime weak fallback at ``pipeline_job.py``, which
        calls ``get_detections(..., detector_model='megadetector-v6')``
        for contextual-weak photos. Foreign-detector weak rows (e.g. a
        stale detection from another detector model still in the
        database) can otherwise rank first in the ROW_NUMBER window and
        carry the matching run key while the megadetector-v6 top box
        does not; that used to mark the photo cached, but the runtime
        would still infer the uncached megadetector-v6 box, leaving a
        phantom cache hit the overcount tracker cannot correct because
        the runtime-selected detection has no run key of its own
        (Codex #1468 P2).
        """
        for chunk, placeholders in self._chunks(self.weak_db_ids):
            rows = self.conn.execute(
                f"""WITH top_weak_det AS (
                        SELECT photo_id, id AS detection_id,
                               ROW_NUMBER() OVER (
                                   PARTITION BY photo_id
                                   ORDER BY detector_confidence DESC,
                                            id ASC
                               ) AS rn
                          FROM detections
                         WHERE detector_model = 'megadetector-v6'
                           AND category = 'animal'
                           AND detector_confidence >= ?
                           AND photo_id IN ({placeholders})
                      )
                    SELECT DISTINCT td.photo_id
                      FROM top_weak_det td
                      JOIN classifier_runs cr
                        ON cr.detection_id = td.detection_id
                       AND cr.classifier_model = ?
                       AND cr.labels_fingerprint = ?
                     WHERE td.rn = 1
                       AND EXISTS (
                             SELECT 1 FROM predictions p
                              WHERE p.detection_id = cr.detection_id
                                AND p.classifier_model = cr.classifier_model
                                AND p.labels_fingerprint
                                    = cr.labels_fingerprint
                                AND p.confidence >= 0
                           )""" + self.rt_predicate_sql,
                [self.weak_conf, *chunk, self.classifier_model,
                 self.labels_fingerprint, *self.rt_predicate_params],
            ).fetchall()
            for r in rows:
                self.matched.add(r["photo_id"])

    def match_db_full_image_anchors(self):
        """DB-fallback ordinary photos use a full-image anchor when no real box
        at the workspace floor remains. Any confident animal or non-animal
        box blocks the fallback; contextual-weak photos use their selected
        weak crop instead and are intentionally excluded here.
        """
        for chunk, placeholders in self._chunks(self.normal_db_ids):
            rows = self.conn.execute(
                f"""WITH full_anchor AS (
                        SELECT photo_id, MIN(id) AS detection_id
                          FROM detections
                         WHERE detector_model = 'full-image'
                           AND photo_id IN ({placeholders})
                         GROUP BY photo_id
                      )
                    SELECT DISTINCT fa.photo_id
                      FROM full_anchor fa
                      LEFT JOIN detector_runs dr
                        ON dr.photo_id = fa.photo_id
                       AND dr.detector_model = 'megadetector-v6'
                      JOIN classifier_runs cr
                        ON cr.detection_id = fa.detection_id
                       AND cr.classifier_model = ?
                       AND cr.labels_fingerprint = ?
                     WHERE (dr.photo_id IS NULL
                            OR dr.box_count = 0 OR EXISTS (
                             SELECT 1 FROM detections consistent
                              WHERE consistent.photo_id = fa.photo_id
                                AND consistent.detector_model
                                    = dr.detector_model
                           ))
                       AND NOT EXISTS (
                             SELECT 1 FROM detections d
                              WHERE d.photo_id = fa.photo_id
                                AND d.detector_model != 'full-image'
                                AND d.detector_confidence >= ?
                           )
                       AND EXISTS (
                             SELECT 1 FROM predictions p
                              WHERE p.detection_id = cr.detection_id
                                AND p.classifier_model = cr.classifier_model
                                AND p.labels_fingerprint
                                    = cr.labels_fingerprint
                                AND p.confidence >= 0
                           )""" + self.rt_predicate_sql,
                [*chunk, self.classifier_model, self.labels_fingerprint,
                 self.min_conf, *self.rt_predicate_params],
            ).fetchall()
            for r in rows:
                self.matched.add(r["photo_id"])
