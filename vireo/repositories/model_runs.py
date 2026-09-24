"""Persistence for model runs: detector and classifier run records.

The tables here (``detector_runs``, ``classifier_runs``,
``classifier_match_scores``, ``labels_fingerprints``) are global across
workspaces: a model's output is a pure function of (photo or detection,
model, label set). ``Database`` keeps the workspace-scoped config lookup:
the classify preflight methods take the active workspace's detector floor
as ``min_conf``, resolved by the façade through
``Database.get_effective_config``.
"""


class ModelRunsRepository:
    def __init__(self, conn, *, auto_match_review_marker, commit_with_retry):
        self.conn = conn
        # ``prediction_review.individual`` value of reproducible auto-created
        # taxonomy-match reviews, which never pin a run.
        self.auto_match_review_marker = auto_match_review_marker
        # ``db.commit_with_retry``, passed in so repositories import no
        # ``db`` code and a monkeypatch of the module function still applies.
        self.commit_with_retry = commit_with_retry

    def record_detector_run(
        self,
        photo_id,
        detector_model,
        box_count,
        runtime_fingerprint="legacy",
        input_fingerprint=None,
    ):
        """Upsert the detector_runs row for (photo, model) and commit."""
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

    def get_global_detection_stats(self):
        """Return catalog-wide detector-cache photo and model counts."""
        r = self.conn.execute(
            """SELECT COUNT(DISTINCT photo_id) AS photo_count,
                      COUNT(DISTINCT detector_model) AS model_count
               FROM detector_runs"""
        ).fetchone()
        return {"photo_count": r["photo_count"] or 0,
                "model_count": r["model_count"] or 0}

    def detector_run_is_pinned(self, photo_id, detector_model):
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
        self, detector_model, runtime_fingerprint=None,
    ):
        """Return photo ids with a consistent cached detector run."""
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
        detection_id,
        classifier_model,
        labels_fingerprint,
        prediction_count,
        labels_fingerprint_full=None,
        runtime_fingerprint="legacy",
        input_fingerprint=None,
        input_recipe=None,
    ):
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
        detection_id,
        classifier_model,
        labels_fingerprint,
        max_match_score,
        match_margin=None,
        top_species=None,
        label_count=None,
        score_kind=None,
    ):
        """Upsert the classifier_match_scores row and commit with retry."""
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
        self, detection_id, classifier_model, labels_fingerprint,
    ):
        """True when classifier_match_scores records this exact run."""
        return self.conn.execute(
            """SELECT 1 FROM classifier_match_scores
               WHERE detection_id = ?
                 AND classifier_model = ?
                 AND labels_fingerprint = ?
               LIMIT 1""",
            (detection_id, classifier_model, labels_fingerprint),
        ).fetchone() is not None

    def get_unscored_current_prediction_runs(self, photo_id):
        """Return current-fingerprint runs with predictions but no score."""
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

    def get_match_scores_for_photo(self, photo_id):
        """Return every match-score row on one photo, stamped is_current."""
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

    def get_classifier_run_keys(self, detection_id, runtime_fingerprint=None):
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

    def get_classifier_run_key_gate(self, detection_id, runtime_fingerprint):
        """Return (accepted, rejected) classifier-run key sets."""
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
        photo_ids,
        classifier_model,
        labels_fingerprint,
        *,
        min_conf,
        contextual_weak_photo_ids=None,
        weak_confidence=None,
        fresh_detections_by_photo=None,
        fresh_processed_photo_ids=None,
        expected_classifier_runtime_by_detector_runtime=None,
    ):
        """Return the photo ids the classify preflight counts as cached."""
        weak_photo_ids = set(contextual_weak_photo_ids or ())
        weak_conf = (
            float(weak_confidence) if weak_confidence is not None else min_conf
        )
        normal_ids = [pid for pid in photo_ids if pid not in weak_photo_ids]
        weak_ids = [pid for pid in photo_ids if pid in weak_photo_ids]
        fresh_processed = (
            set(fresh_processed_photo_ids or ())
            if fresh_detections_by_photo is not None
            and fresh_processed_photo_ids is not None
            else set()
        )
        normal_db_ids = [pid for pid in normal_ids if pid not in fresh_processed]
        weak_db_ids = [pid for pid in weak_ids if pid not in fresh_processed]
        # Chunk to stay under SQLITE_MAX_VARIABLE_NUMBER (default 999).
        # Match the 500-element chunks used elsewhere in this file.
        CHUNK = 500
        matched = set()

        # Runtime-fingerprint gate for classifier_runs, mirroring what
        # ``get_classifier_run_key_gate`` accepts at runtime. Without this
        # predicate the preflight would count rows whose ``runtime_fingerprint``
        # the runtime rejects (typically stale after a detector fingerprint
        # roll), and no observation could correct the estimate until those
        # rows were visited — collapsing ``remaining_uncached`` to zero
        # prematurely (Codex #1468 P2).
        rt_map = expected_classifier_runtime_by_detector_runtime
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
            # Assemble a predicate that, given a classifier_runs alias
            # ``{cr}``, requires at least one of:
            #   1. ``{cr}.runtime_fingerprint`` matches the expected value for
            #      the anchor detection's ``detections.runtime_fingerprint``.
            #   2. ``{cr}.runtime_fingerprint`` is ``'legacy'`` (grandfathered).
            #   3. A prediction on the same row carries a real
            #      prediction_review override (mirrors ``get_classifier_run_keys``).
            # A permissive detector-runtime entry (expected value ``None``)
            # accepts any classifier_runs.runtime_fingerprint for its
            # detections, matching the pipeline's unfiltered fallback when
            # portable identity is not wired up.
            def _runtime_predicate(alias):
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
                params.append(self.auto_match_review_marker)
                return pred_sql, params

            rt_predicate_sql_cr, rt_predicate_params = _runtime_predicate("cr")
            rt_predicate_sql = rt_predicate_sql_cr

        # RAW outputs cannot satisfy a normal-image run, even when reviewed.
        rt_predicate_sql += " AND cr.input_recipe IS NULL AND cr.runtime_fingerprint != 'incomplete'"

        # For photos whose detector iteration completed, mirror the runtime's
        # in-memory target selection rather than querying every detection row
        # still present in the DB. Old rows can legitimately survive until a
        # later purge and are not runtime candidates for this pass.
        fresh_candidates: dict = {}
        for photo_id in fresh_processed:
            is_weak = photo_id in weak_photo_ids
            floor = weak_conf if is_weak else min_conf
            candidates = []
            for detection in fresh_detections_by_photo.get(photo_id) or ():
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
            if candidates:
                fresh_candidates[photo_id] = {
                    detection["id"] for detection in candidates
                }

        # Cached detector reuse loads rows at the ordinary workspace floor.
        # A contextual-weak target can therefore be absent from the in-memory
        # map even though the classify loop will explicitly reload its MDv6
        # weak row from the DB. Route those omitted weak photos through the
        # same DB fallback here (Codex #1468 P2).
        weak_db_ids.extend(
            photo_id for photo_id in weak_ids
            if photo_id in fresh_processed
            and photo_id not in fresh_candidates
        )

        # Ordinary processed photos with no usable animal crop now take the
        # full-image fallback unless a confident person/vehicle box blocks it.
        # The detect stage pre-creates those anchors before this preflight, so
        # add the anchor ID to the same candidate map used for fresh crops.
        fresh_full_image_ids = []
        for photo_id in normal_ids:
            if photo_id not in fresh_processed or photo_id in fresh_candidates:
                continue
            confident_non_animal = False
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
                        confident_non_animal = True
                        break
                except (TypeError, ValueError):
                    continue
            if not confident_non_animal:
                fresh_full_image_ids.append(photo_id)
        for i in range(0, len(fresh_full_image_ids), CHUNK):
            chunk = fresh_full_image_ids[i:i + CHUNK]
            placeholders = ",".join("?" * len(chunk))
            rows = self.conn.execute(
                f"""SELECT photo_id, MIN(id) AS detection_id
                      FROM detections
                     WHERE detector_model = 'full-image'
                       AND photo_id IN ({placeholders})
                     GROUP BY photo_id""",
                chunk,
            ).fetchall()
            for row in rows:
                fresh_candidates[row["photo_id"]] = {row["detection_id"]}

        fresh_detection_ids = {
            detection_id
            for candidate_ids in fresh_candidates.values()
            for detection_id in candidate_ids
        }
        cached_fresh_detection_ids: set = set()
        fresh_detection_ids_list = list(fresh_detection_ids)
        for i in range(0, len(fresh_detection_ids_list), CHUNK):
            chunk = fresh_detection_ids_list[i:i + CHUNK]
            placeholders = ",".join("?" * len(chunk))
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
                           )""" + rt_predicate_sql,
                [*chunk, classifier_model, labels_fingerprint,
                 *rt_predicate_params],
            ).fetchall()
            cached_fresh_detection_ids.update(
                row["detection_id"] for row in rows
            )
        for photo_id, candidate_ids in fresh_candidates.items():
            if candidate_ids <= cached_fresh_detection_ids:
                matched.add(photo_id)

        for i in range(0, len(normal_db_ids), CHUNK):
            chunk = normal_db_ids[i:i + CHUNK]
            if not chunk:
                continue
            placeholders = ",".join("?" * len(chunk))
            # A photo counts as fully cached iff it has at least one
            # above-threshold real detection AND every above-threshold real
            # detection carries a matching (classifier_model,
            # labels_fingerprint) run key. The outer NOT EXISTS is the
            # "no uncached qualifying detection remains" clause; the outer
            # WHERE also requires at least one qualifying detection so
            # empty-detection photos don't fall through this branch (they
            # are handled by the full-image anchor branch below).
            # The category='animal' predicate on both the outer and inner
            # detection scans mirrors the runtime classify loop's
            # non-animal skip (MegaDetector can return person/vehicle
            # boxes above the confidence threshold, and the classifier
            # stage filters them out before inference). Without matching
            # the runtime filter here, a photo with cached animal
            # detections plus one uncached person/vehicle box would be
            # excluded from ``cached_estimate`` even though that photo
            # will actually be entirely cache-served at runtime, so the
            # UI would understate cached work.
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
                + rt_predicate_sql +
                "      ) "
                "  )",
                [min_conf, *chunk, min_conf, classifier_model,
                 labels_fingerprint, *rt_predicate_params],
            ).fetchall()
            for r in rows:
                matched.add(r["photo_id"])
        # Contextual-weak photos: the runtime classifies a SINGLE
        # weak-threshold detection per photo (``photo_dets[:1]`` in the
        # classify loop, after ordering by ``detector_confidence DESC,
        # id ASC`` in ``get_detections``). Mirror that by counting the
        # photo only when THAT top detection carries a matching run
        # key. An earlier revision accepted any qualifying weak
        # detection with a run key, which meant a photo whose only
        # cached row was a lower-ranked box was marked cached even
        # though the runtime would infer the uncached top box; that
        # miss never registered as a fall-through overcount either
        # (the top detection has no run key at all), leaving a phantom
        # cache hit in the ETA (Codex #1468 P2).
        #
        # Restrict the CTE to ``detector_model = 'megadetector-v6'`` to
        # mirror the runtime weak fallback at ``pipeline_job.py``, which
        # calls ``get_detections(..., detector_model='megadetector-v6')``
        # for contextual-weak photos. Foreign-detector weak rows (e.g. a
        # stale detection from another detector model still in the
        # database) can otherwise rank first in the ROW_NUMBER window and
        # carry the matching run key while the megadetector-v6 top box
        # does not; that used to mark the photo cached, but the runtime
        # would still infer the uncached megadetector-v6 box, leaving a
        # phantom cache hit the overcount tracker cannot correct because
        # the runtime-selected detection has no run key of its own
        # (Codex #1468 P2).
        for i in range(0, len(weak_db_ids), CHUNK):
            chunk = weak_db_ids[i:i + CHUNK]
            if not chunk:
                continue
            placeholders = ",".join("?" * len(chunk))
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
                           )""" + rt_predicate_sql,
                [weak_conf, *chunk, classifier_model, labels_fingerprint,
                 *rt_predicate_params],
            ).fetchall()
            for r in rows:
                matched.add(r["photo_id"])
        # DB-fallback ordinary photos use a full-image anchor when no real box
        # at the workspace floor remains. Any confident animal or non-animal
        # box blocks the fallback; contextual-weak photos use their selected
        # weak crop instead and are intentionally excluded here.
        for i in range(0, len(normal_db_ids), CHUNK):
            chunk = normal_db_ids[i:i + CHUNK]
            placeholders = ",".join("?" * len(chunk))
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
                           )""" + rt_predicate_sql,
                [*chunk, classifier_model, labels_fingerprint, min_conf,
                 *rt_predicate_params],
            ).fetchall()
            for r in rows:
                matched.add(r["photo_id"])
        return matched

    def get_unclassifiable_photos(
        self,
        photo_ids,
        *,
        min_conf,
        contextual_weak_photo_ids=None,
        weak_confidence=None,
        fresh_detections_by_photo=None,
        fresh_processed_photo_ids=None,
    ):
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

    def get_labels_fingerprints(self):
        """Return all labels_fingerprints rows with decoded sources."""
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
        fingerprint,
        display_name,
        sources,
        label_count,
        full_fingerprint=None,
    ):
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
