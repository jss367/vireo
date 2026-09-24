"""Persistence for the dashboard, coverage and pipeline-plan counters.

Everything here is a read over the active workspace's photos, detections,
classifier runs, masks and caches. ``Database`` keeps the composition: it
resolves the workspace-effective ``detector_confidence`` and
``preview_max_size`` through ``Database.get_effective_config``, builds the
photo scope (``_scope_clause`` / ``_dashboard_scope_clause``, which reach
into the folder and collection domains), and passes the resulting SQL
fragment and parameters in. The query text is unchanged from ``db.py``; the
large-library budgets in ``docs/ARCHITECTURE.md`` depend on it.
"""

from keyword_identity import identity_sql


class StatsRepository:
    def __init__(self, conn, workspace_id, *, coverage_photo_columns):
        self.conn = conn
        self.workspace_id = workspace_id
        # ``Database._COVERAGE_PHOTO_COLUMNS``: the (key, predicate) pairs
        # behind the coverage SELECT fragment, in result order.
        self.coverage_photo_columns = coverage_photo_columns

    def folder_linked(self, folder_id):
        """Return whether ``folder_id`` is linked to the workspace."""
        linked = self.conn.execute(
            "SELECT 1 FROM workspace_folders "
            "WHERE workspace_id = ? AND folder_id = ?",
            (self.workspace_id, folder_id),
        ).fetchone()
        return bool(linked)

    def get_coverage(self, min_conf, scope_sql, scope_params, select_fragment):
        """Per-stage coverage counts over accessible workspace photos."""
        ws = self.workspace_id
        photo_row = self.conn.execute(
            f"""SELECT
                COUNT(*) AS total,
                {select_fragment}
            FROM photos p
            JOIN workspace_folders wf ON wf.folder_id = p.folder_id
            JOIN folders f ON f.id = p.folder_id AND f.status IN ('ok', 'partial')
            WHERE wf.workspace_id = ?{scope_sql}""",
            (ws, *scope_params),
        ).fetchone()
        detected = self.conn.execute(
            f"""SELECT COUNT(DISTINCT d.photo_id)
               FROM detections d
               JOIN photos p ON p.id = d.photo_id
               JOIN workspace_folders wf ON wf.folder_id = p.folder_id
               JOIN folders f ON f.id = p.folder_id AND f.status IN ('ok', 'partial')
               WHERE wf.workspace_id = ?
                 AND d.detector_confidence >= ?{scope_sql}""",
            (ws, min_conf, *scope_params),
        ).fetchone()[0] or 0
        classified = self.conn.execute(
            f"""SELECT COUNT(DISTINCT d.photo_id)
               FROM predictions pr
               JOIN detections d ON d.id = pr.detection_id
               JOIN photos p ON p.id = d.photo_id
               JOIN workspace_folders wf ON wf.folder_id = p.folder_id
               JOIN folders f ON f.id = p.folder_id AND f.status IN ('ok', 'partial')
               WHERE wf.workspace_id = ?
                 AND d.detector_confidence >= ?{scope_sql}""",
            (ws, min_conf, *scope_params),
        ).fetchone()[0] or 0
        result = {"total": photo_row["total"] or 0}
        for key, _ in self.coverage_photo_columns:
            result[key] = photo_row[key] or 0
        result["detected"] = detected
        result["classified"] = classified
        return result

    def get_folder_coverage(
        self, min_conf, scope_sql, scope_params, photo_scope_sql,
        photo_scope_params, folder_subtree, select_fragment,
    ):
        """Per-folder coverage rows; ``folder_subtree`` limits the folders."""
        ws = self.workspace_id
        folder_filter_sql = ""
        folder_filter_params = []
        if folder_subtree is not None:
            placeholders = ",".join("?" for _ in folder_subtree)
            folder_filter_sql = f" AND f.id IN ({placeholders})"
            folder_filter_params = folder_subtree
        photo_rows = self.conn.execute(
            f"""SELECT
                f.id AS folder_id,
                f.path AS path,
                f.name AS name,
                COUNT(p.id) AS total,
                {select_fragment}
            FROM folders f
            JOIN workspace_folders wf ON wf.folder_id = f.id
            LEFT JOIN photos p ON p.folder_id = f.id{photo_scope_sql}
            WHERE wf.workspace_id = ? AND f.status IN ('ok', 'partial'){folder_filter_sql}
            GROUP BY f.id
            ORDER BY f.path""",
            (*photo_scope_params, ws, *folder_filter_params),
        ).fetchall()
        det_rows = self.conn.execute(
            f"""SELECT p.folder_id AS folder_id,
                      COUNT(DISTINCT d.photo_id) AS detected
               FROM detections d
               JOIN photos p ON p.id = d.photo_id
               JOIN workspace_folders wf ON wf.folder_id = p.folder_id
               JOIN folders f ON f.id = p.folder_id AND f.status IN ('ok', 'partial')
               WHERE wf.workspace_id = ?
                 AND d.detector_confidence >= ?{scope_sql}
               GROUP BY p.folder_id""",
            (ws, min_conf, *scope_params),
        ).fetchall()
        cls_rows = self.conn.execute(
            f"""SELECT p.folder_id AS folder_id,
                      COUNT(DISTINCT d.photo_id) AS classified
               FROM predictions pr
               JOIN detections d ON d.id = pr.detection_id
               JOIN photos p ON p.id = d.photo_id
               JOIN workspace_folders wf ON wf.folder_id = p.folder_id
               JOIN folders f ON f.id = p.folder_id AND f.status IN ('ok', 'partial')
               WHERE wf.workspace_id = ?
                 AND d.detector_confidence >= ?{scope_sql}
               GROUP BY p.folder_id""",
            (ws, min_conf, *scope_params),
        ).fetchall()
        det_by_folder = {r["folder_id"]: r["detected"] for r in det_rows}
        cls_by_folder = {r["folder_id"]: r["classified"] for r in cls_rows}
        out = []
        for r in photo_rows:
            entry = {
                "folder_id": r["folder_id"],
                "path": r["path"],
                "name": r["name"],
                "total": r["total"] or 0,
            }
            for key, _ in self.coverage_photo_columns:
                entry[key] = r[key] or 0
            entry["detected"] = det_by_folder.get(r["folder_id"], 0)
            entry["classified"] = cls_by_folder.get(r["folder_id"], 0)
            out.append(entry)
        return out

    def stage_scope_ids(self, table, ids):
        """Stage a read scope without opening or committing a caller transaction."""
        if table not in {"scope_ids", "missing_subtree_ids"}:
            raise ValueError("Unknown scope table")
        # An outermost SAVEPOINT releases its own transaction; a nested one
        # preserves the caller's writes. A plain commit here would break
        # atomic edits that happen to query a large selection.
        self.conn.execute("SAVEPOINT stage_read_scope")
        try:
            self.conn.execute(f"CREATE TEMP TABLE IF NOT EXISTS {table} (id INTEGER PRIMARY KEY)")
            self.conn.execute(f"DELETE FROM {table}")
            self.conn.executemany(
                f"INSERT OR IGNORE INTO {table} (id) VALUES (?)", ((i,) for i in ids),
            )
        except BaseException:
            self.conn.execute("ROLLBACK TO stage_read_scope")
            raise
        finally:
            self.conn.execute("RELEASE stage_read_scope")

    def count_real_detections_in_scope(self, min_conf, scope_sql, scope_params):
        """Count photos with real detections and the detections themselves."""
        ws = self.workspace_id
        row = self.conn.execute(
            f"""SELECT COUNT(*) AS total_dets,
                       COUNT(DISTINCT d.photo_id) AS photos_with_dets
                FROM detections d
                JOIN photos p ON p.id = d.photo_id
                JOIN workspace_folders wf
                  ON wf.folder_id = p.folder_id AND wf.workspace_id = ?
                WHERE d.detector_model != 'full-image' AND COALESCE(d.category, 'animal') = 'animal'
                  AND d.detector_confidence >= ?{scope_sql}""",
            (ws, min_conf, *scope_params),
        ).fetchone()
        return {
            "photos_with_dets": row["photos_with_dets"] or 0,
            "total_dets": row["total_dets"] or 0,
        }

    def count_primary_detections_in_scope(self, min_conf, scope_sql, scope_params):
        """Count photos whose primary real detection is pipeline-classifiable."""
        ws = self.workspace_id
        row = self.conn.execute(
            f"""WITH ranked AS (
                    SELECT d.id, d.photo_id,
                           ROW_NUMBER() OVER (
                             PARTITION BY d.photo_id
                             ORDER BY d.detector_confidence DESC, d.id ASC
                           ) AS rn
                      FROM detections d
                      JOIN photos p ON p.id = d.photo_id
                      JOIN workspace_folders wf
                        ON wf.folder_id = p.folder_id AND wf.workspace_id = ?
                     WHERE d.detector_model != 'full-image'
                       AND COALESCE(d.category, 'animal') = 'animal'
                       AND d.detector_confidence >= ?{scope_sql}
                )
                SELECT COUNT(*) AS primary_dets,
                       COUNT(DISTINCT photo_id) AS photos_with_dets
                  FROM ranked
                 WHERE rn = 1""",
            (ws, min_conf, *scope_params),
        ).fetchone()
        return {
            "photos_with_dets": row["photos_with_dets"] or 0,
            "total_dets": row["primary_dets"] or 0,
        }

    def count_classify_pending_pairs(
        self, classifier_model, labels_fingerprint, min_conf, scope_sql,
        scope_params,
    ):
        """Count real detections with no complete run for (model, fp)."""
        ws = self.workspace_id
        row = self.conn.execute(
            f"""SELECT COUNT(*) AS pending
                FROM detections d
                JOIN photos p ON p.id = d.photo_id
                JOIN workspace_folders wf
                  ON wf.folder_id = p.folder_id AND wf.workspace_id = ?
                LEFT JOIN classifier_runs cr
                  ON cr.detection_id = d.id
                 AND cr.classifier_model = ?
                 AND cr.labels_fingerprint = ?
                   AND cr.input_recipe IS NULL AND cr.runtime_fingerprint != 'incomplete'
                WHERE d.detector_model != 'full-image' AND COALESCE(d.category, 'animal') = 'animal'
                  AND d.detector_confidence >= ?
                  AND cr.detection_id IS NULL{scope_sql}""",
            (ws, classifier_model, labels_fingerprint, min_conf, *scope_params),
        ).fetchone()
        return row["pending"] or 0

    def count_primary_classify_pending_pairs(
        self, classifier_model, labels_fingerprint, min_conf, scope_sql,
        scope_params,
    ):
        """Count primary detections lacking a classifier run for (model, fp)."""
        ws = self.workspace_id
        row = self.conn.execute(
            f"""WITH ranked AS (
                    SELECT d.id, d.photo_id,
                           ROW_NUMBER() OVER (
                             PARTITION BY d.photo_id
                             ORDER BY d.detector_confidence DESC, d.id ASC
                           ) AS rn
                      FROM detections d
                      JOIN photos p ON p.id = d.photo_id
                      JOIN workspace_folders wf
                        ON wf.folder_id = p.folder_id AND wf.workspace_id = ?
                     WHERE d.detector_model != 'full-image'
                       AND COALESCE(d.category, 'animal') = 'animal'
                       AND d.detector_confidence >= ?{scope_sql}
                )
                SELECT COUNT(*) AS pending
                  FROM ranked d
                  LEFT JOIN classifier_runs cr
                    ON cr.detection_id = d.id
                   AND cr.classifier_model = ?
                   AND cr.labels_fingerprint = ?
                   AND cr.input_recipe IS NULL AND cr.runtime_fingerprint != 'incomplete'
                 WHERE d.rn = 1
                   AND cr.detection_id IS NULL""",
            (ws, min_conf, *scope_params, classifier_model, labels_fingerprint),
        ).fetchone()
        return row["pending"] or 0

    def count_classify_stale(
        self, classifier_model, labels_fingerprint, min_conf, scope_sql,
        scope_params,
    ):
        """Count real detections with only stale runs for ``classifier_model``."""
        ws = self.workspace_id
        row = self.conn.execute(
            f"""SELECT COUNT(DISTINCT d.id) AS n
                FROM detections d
                JOIN photos p ON p.id = d.photo_id
                JOIN workspace_folders wf
                  ON wf.folder_id = p.folder_id AND wf.workspace_id = ?
               WHERE d.detector_model != 'full-image' AND COALESCE(d.category, 'animal') = 'animal'
                 AND d.detector_confidence >= ?
                 AND EXISTS (
                    SELECT 1 FROM classifier_runs cr_stale
                     WHERE cr_stale.detection_id = d.id
                       AND cr_stale.classifier_model = ?
                       AND cr_stale.labels_fingerprint != ?
                 )
                 AND NOT EXISTS (
                    SELECT 1 FROM classifier_runs cr_cur
                     WHERE cr_cur.detection_id = d.id
                       AND cr_cur.classifier_model = ?
                       AND cr_cur.labels_fingerprint = ?
                 ){scope_sql}""",
            (ws, min_conf, classifier_model, labels_fingerprint,
             classifier_model, labels_fingerprint, *scope_params),
        ).fetchone()
        return row["n"] or 0

    def count_primary_classify_stale(
        self, classifier_model, labels_fingerprint, min_conf, scope_sql,
        scope_params,
    ):
        """Count stale classifier runs on primary detections only."""
        ws = self.workspace_id
        row = self.conn.execute(
            f"""WITH ranked AS (
                    SELECT d.id, d.photo_id,
                           ROW_NUMBER() OVER (
                             PARTITION BY d.photo_id
                             ORDER BY d.detector_confidence DESC, d.id ASC
                           ) AS rn
                      FROM detections d
                      JOIN photos p ON p.id = d.photo_id
                      JOIN workspace_folders wf
                        ON wf.folder_id = p.folder_id AND wf.workspace_id = ?
                     WHERE d.detector_model != 'full-image'
                       AND COALESCE(d.category, 'animal') = 'animal'
                       AND d.detector_confidence >= ?{scope_sql}
                )
                SELECT COUNT(*) AS n
                  FROM ranked d
                 WHERE d.rn = 1
                   AND EXISTS (
                      SELECT 1 FROM classifier_runs cr_stale
                       WHERE cr_stale.detection_id = d.id
                         AND cr_stale.classifier_model = ?
                         AND cr_stale.labels_fingerprint != ?
                   )
                   AND NOT EXISTS (
                      SELECT 1 FROM classifier_runs cr_cur
                       WHERE cr_cur.detection_id = d.id
                         AND cr_cur.classifier_model = ?
                         AND cr_cur.labels_fingerprint = ?
                   )""",
            (ws, min_conf, *scope_params, classifier_model, labels_fingerprint,
             classifier_model, labels_fingerprint),
        ).fetchone()
        return row["n"] or 0

    def count_full_image_fallback_photos(
        self, detector_model, min_conf, scope_sql, scope_params,
    ):
        """Count photos eligible for full-image fallback classification."""
        ws = self.workspace_id
        row = self.conn.execute(
            f"""SELECT COUNT(*) AS n
                  FROM photos p
                  JOIN workspace_folders wf
                    ON wf.folder_id = p.folder_id AND wf.workspace_id = ?
                 JOIN detector_runs dr
                    ON dr.photo_id = p.id
                   AND dr.detector_model = ?
                 WHERE (dr.box_count = 0 OR EXISTS (
                         SELECT 1 FROM detections consistent
                          WHERE consistent.photo_id = p.id
                            AND consistent.detector_model = dr.detector_model
                       ))
                   AND NOT EXISTS (
                         SELECT 1 FROM detections d
                          WHERE d.photo_id = p.id
                            AND d.detector_model != 'full-image'
                            AND d.detector_confidence >= ?
                       ){scope_sql}""",
            (ws, detector_model, min_conf, *scope_params),
        ).fetchone()
        return row["n"] or 0

    def count_full_image_classify_pending_pairs(
        self, classifier_model, labels_fingerprint, detector_model, min_conf,
        scope_sql, scope_params,
    ):
        """Count fallback photos lacking a classifier run for (model, fp)."""
        ws = self.workspace_id
        row = self.conn.execute(
            f"""WITH full_anchor AS (
                    SELECT photo_id, MIN(id) AS detection_id
                      FROM detections
                     WHERE detector_model = 'full-image'
                     GROUP BY photo_id
                  ),
                  fallback AS (
                    SELECT p.id AS photo_id, fa.detection_id
                      FROM photos p
                      JOIN workspace_folders wf
                        ON wf.folder_id = p.folder_id AND wf.workspace_id = ?
                      JOIN detector_runs dr
                        ON dr.photo_id = p.id
                       AND dr.detector_model = ?
                      LEFT JOIN full_anchor fa ON fa.photo_id = p.id
                     WHERE (dr.box_count = 0 OR EXISTS (
                             SELECT 1 FROM detections consistent
                              WHERE consistent.photo_id = p.id
                                AND consistent.detector_model = dr.detector_model
                           ))
                       AND NOT EXISTS (
                             SELECT 1 FROM detections d
                              WHERE d.photo_id = p.id
                                AND d.detector_model != 'full-image'
                                AND d.detector_confidence >= ?
                           ){scope_sql}
                  )
                SELECT COUNT(*) AS pending
                  FROM fallback f
                  LEFT JOIN classifier_runs cr
                    ON cr.detection_id = f.detection_id
                   AND cr.classifier_model = ?
                   AND cr.labels_fingerprint = ?
                   AND cr.input_recipe IS NULL AND cr.runtime_fingerprint != 'incomplete'
                 WHERE f.detection_id IS NULL
                    OR cr.detection_id IS NULL""",
            (
                ws, detector_model, min_conf, *scope_params,
                classifier_model, labels_fingerprint,
            ),
        ).fetchone()
        return row["pending"] or 0

    def count_full_image_classify_stale(
        self, classifier_model, labels_fingerprint, detector_model, min_conf,
        scope_sql, scope_params,
    ):
        """Count fallback anchors with stale runs and no current run."""
        ws = self.workspace_id
        row = self.conn.execute(
            f"""WITH full_anchor AS (
                    SELECT photo_id, MIN(id) AS detection_id
                      FROM detections
                     WHERE detector_model = 'full-image'
                     GROUP BY photo_id
                  ),
                  fallback AS (
                    SELECT p.id AS photo_id, fa.detection_id
                      FROM photos p
                      JOIN workspace_folders wf
                        ON wf.folder_id = p.folder_id AND wf.workspace_id = ?
                      JOIN detector_runs dr
                        ON dr.photo_id = p.id
                       AND dr.detector_model = ?
                      JOIN full_anchor fa ON fa.photo_id = p.id
                     WHERE (dr.box_count = 0 OR EXISTS (
                             SELECT 1 FROM detections consistent
                              WHERE consistent.photo_id = p.id
                                AND consistent.detector_model = dr.detector_model
                           ))
                       AND NOT EXISTS (
                             SELECT 1 FROM detections d
                              WHERE d.photo_id = p.id
                                AND d.detector_model != 'full-image'
                                AND d.detector_confidence >= ?
                           ){scope_sql}
                  )
                SELECT COUNT(*) AS n
                  FROM fallback f
                 WHERE EXISTS (
                         SELECT 1 FROM classifier_runs cr_stale
                          WHERE cr_stale.detection_id = f.detection_id
                            AND cr_stale.classifier_model = ?
                            AND cr_stale.labels_fingerprint != ?
                       )
                   AND NOT EXISTS (
                         SELECT 1 FROM classifier_runs cr_cur
                          WHERE cr_cur.detection_id = f.detection_id
                            AND cr_cur.classifier_model = ?
                            AND cr_cur.labels_fingerprint = ?
                       )""",
            (
                ws, detector_model, min_conf, *scope_params,
                classifier_model, labels_fingerprint,
                classifier_model, labels_fingerprint,
            ),
        ).fetchone()
        return row["n"] or 0

    def classification_inventory_counts(self, workspace_id, min_conf):
        """Return ``(total_real_detections, pair_rows, pred_counts)``.

        ``pair_rows`` are the per-(model, fingerprint) classifier-run
        aggregates; ``pred_counts`` maps (model, fingerprint) to its
        predictions row count.
        """
        # Scalar: total real detections in scope.
        total_row = self.conn.execute(
            """SELECT COUNT(*) AS n
               FROM detections d
               JOIN photos p ON p.id = d.photo_id
               JOIN workspace_folders wf
                 ON wf.folder_id = p.folder_id AND wf.workspace_id = ?
               WHERE d.detector_model != 'full-image'
                 AND d.detector_confidence >= ?""",
            (workspace_id, min_conf),
        ).fetchone()
        total_real_detections = total_row["n"] or 0

        # Per-pair aggregates from classifier_runs joined to in-scope detections.
        pair_rows = self.conn.execute(
            """SELECT cr.classifier_model      AS classifier_model,
                      cr.labels_fingerprint    AS labels_fingerprint,
                      COUNT(DISTINCT cr.detection_id) AS classified_dets,
                      COUNT(DISTINCT d.photo_id)      AS photos_covered,
                      MAX(cr.run_at)           AS last_run
               FROM classifier_runs cr
               JOIN detections d ON d.id = cr.detection_id
               JOIN photos p ON p.id = d.photo_id
               JOIN workspace_folders wf
                 ON wf.folder_id = p.folder_id AND wf.workspace_id = ?
               WHERE d.detector_model != 'full-image'
                 AND d.detector_confidence >= ?
               GROUP BY cr.classifier_model, cr.labels_fingerprint""",
            (workspace_id, min_conf),
        ).fetchall()

        # Per-pair predictions row count (so the grand total can sum it).
        pred_count_rows = self.conn.execute(
            """SELECT pr.classifier_model      AS classifier_model,
                      pr.labels_fingerprint    AS labels_fingerprint,
                      COUNT(*)                 AS n
               FROM predictions pr
               JOIN detections d ON d.id = pr.detection_id
               JOIN photos p ON p.id = d.photo_id
               JOIN workspace_folders wf
                 ON wf.folder_id = p.folder_id AND wf.workspace_id = ?
               WHERE d.detector_model != 'full-image'
                 AND d.detector_confidence >= ?
               GROUP BY pr.classifier_model, pr.labels_fingerprint""",
            (workspace_id, min_conf),
        ).fetchall()
        pred_counts = {
            (r["classifier_model"], r["labels_fingerprint"]): r["n"]
            for r in pred_count_rows
        }
        return total_real_detections, pair_rows, pred_counts

    def sampled_top1_medians(self, workspace_id, min_conf, sample_per_pair):
        """Return {(model, fingerprint): (median, sample_size)} from a sampled
        set of top-1-per-detection prediction confidences.

        SQLite has no built-in median; we pull a per-pair sample (at most
        ``sample_per_pair`` rows) of the max confidence per (detection, model,
        fingerprint) tuple and median in Python. Sampling is fine for the UX
        signal — if classified_dets is small, the sample is the whole set.
        """
        # Top-1 per (detection, model, fp) — predictions UNIQUE on
        # (detection_id, classifier_model, labels_fingerprint, species), so
        # MAX(confidence) within that group is the top-1 confidence. The
        # outer window orders by RANDOM() so the per-pair cap picks an
        # unbiased sample rather than the oldest detection IDs (which would
        # under-represent recent reclassifications and bias the median).
        # Cap rows per (model, fp) pair in SQL via ROW_NUMBER so a workspace
        # with millions of predictions doesn't materialize them all in Python.
        rows = self.conn.execute(
            """SELECT classifier_model, labels_fingerprint, top1
               FROM (
                 SELECT classifier_model,
                        labels_fingerprint,
                        top1,
                        ROW_NUMBER() OVER (
                          PARTITION BY classifier_model, labels_fingerprint
                          ORDER BY RANDOM()
                        ) AS rn
                 FROM (
                   SELECT pr.classifier_model      AS classifier_model,
                          pr.labels_fingerprint    AS labels_fingerprint,
                          pr.detection_id          AS detection_id,
                          MAX(pr.confidence)       AS top1
                   FROM predictions pr
                   JOIN detections d ON d.id = pr.detection_id
                   JOIN photos p ON p.id = d.photo_id
                   JOIN workspace_folders wf
                     ON wf.folder_id = p.folder_id
                    AND wf.workspace_id = ?
                   WHERE d.detector_model != 'full-image'
                     AND d.detector_confidence >= ?
                     AND pr.confidence IS NOT NULL
                   GROUP BY pr.classifier_model, pr.labels_fingerprint,
                            pr.detection_id
                 )
               )
               WHERE rn <= ?""",
            (workspace_id, min_conf, sample_per_pair),
        ).fetchall()

        # Bucket by pair (already capped at sample_per_pair by SQL).
        buckets = {}
        for r in rows:
            key = (r["classifier_model"], r["labels_fingerprint"])
            buckets.setdefault(key, []).append(r["top1"])

        out = {}
        for key, vals in buckets.items():
            if not vals:
                out[key] = (None, 0)
                continue
            vals_sorted = sorted(vals)
            n = len(vals_sorted)
            mid = n // 2
            if n % 2 == 1:
                med = vals_sorted[mid]
            else:
                med = (vals_sorted[mid - 1] + vals_sorted[mid]) / 2.0
            out[key] = (float(med), n)
        return out

    def count_photos_pending_masks(
        self, min_conf, sam2_variant, scope_sql, scope_params,
    ):
        """Return (pending, eligible) for the extract-masks stage."""
        ws = self.workspace_id
        if sam2_variant:
            row = self.conn.execute(
                f"""SELECT
                      COUNT(DISTINCT p.id) AS eligible,
                      COUNT(DISTINCT CASE
                        WHEN p.mask_path IS NULL
                          OR pm.photo_id IS NULL
                          OR pm.path IS NULL
                          OR pm.path = ''
                        THEN p.id END) AS pending
                    FROM photos p
                    JOIN workspace_folders wf
                      ON wf.folder_id = p.folder_id AND wf.workspace_id = ?
                    JOIN detections d
                      ON d.photo_id = p.id
                     AND d.detector_model != 'full-image'
                     AND d.detector_confidence >= ?
                    LEFT JOIN photo_masks pm
                      ON pm.photo_id = p.id AND pm.variant = ?
                    WHERE 1=1{scope_sql}""",
                (ws, min_conf, sam2_variant, *scope_params),
            ).fetchone()
        else:
            row = self.conn.execute(
                f"""SELECT
                      COUNT(DISTINCT p.id) AS eligible,
                      COUNT(DISTINCT CASE WHEN p.mask_path IS NULL THEN p.id END)
                        AS pending
                    FROM photos p
                    JOIN workspace_folders wf
                      ON wf.folder_id = p.folder_id AND wf.workspace_id = ?
                    JOIN detections d
                      ON d.photo_id = p.id
                     AND d.detector_model != 'full-image'
                     AND d.detector_confidence >= ?
                    WHERE 1=1{scope_sql}""",
                (ws, min_conf, *scope_params),
            ).fetchone()
        return {
            "eligible": row["eligible"] or 0,
            "pending": row["pending"] or 0,
        }

    def count_photos_missing_thumb(self, scope_sql, scope_params):
        """Return (eligible, pending) for the thumbnails substage."""
        ws = self.workspace_id
        row = self.conn.execute(
            f"""SELECT
                  COUNT(*) AS eligible,
                  SUM(CASE WHEN p.thumb_path IS NULL THEN 1 ELSE 0 END)
                    AS pending
                FROM photos p
                JOIN workspace_folders wf
                  ON wf.folder_id = p.folder_id AND wf.workspace_id = ?
                WHERE 1=1{scope_sql}""",
            (ws, *scope_params),
        ).fetchone()
        return {
            "eligible": row["eligible"] or 0,
            "pending": row["pending"] or 0,
        }

    def count_photos_missing_preview(self, size, scope_sql, scope_params):
        """Return (eligible, pending) for the previews substage at ``size``."""
        ws = self.workspace_id
        row = self.conn.execute(
            f"""SELECT
                  COUNT(*) AS eligible,
                  SUM(CASE WHEN pc.photo_id IS NULL THEN 1 ELSE 0 END)
                    AS pending
                FROM photos p
                JOIN workspace_folders wf
                  ON wf.folder_id = p.folder_id AND wf.workspace_id = ?
                LEFT JOIN preview_cache pc
                  ON pc.photo_id = p.id AND pc.size = ?
                WHERE 1=1{scope_sql}""",
            (ws, size, *scope_params),
        ).fetchone()
        return {
            "eligible": row["eligible"] or 0,
            "pending": row["pending"] or 0,
        }

    def count_photos_missing_thumb_or_preview(self, size, scope_sql, scope_params):
        """Return (eligible, pending) over photos missing a thumb or preview."""
        ws = self.workspace_id
        row = self.conn.execute(
            f"""SELECT
                  COUNT(*) AS eligible,
                  SUM(CASE
                        WHEN p.thumb_path IS NULL OR pc.photo_id IS NULL
                        THEN 1 ELSE 0 END) AS pending
                FROM photos p
                JOIN workspace_folders wf
                  ON wf.folder_id = p.folder_id AND wf.workspace_id = ?
                LEFT JOIN preview_cache pc
                  ON pc.photo_id = p.id AND pc.size = ?
                WHERE 1=1{scope_sql}""",
            (ws, size, *scope_params),
        ).fetchone()
        return {
            "eligible": row["eligible"] or 0,
            "pending": row["pending"] or 0,
        }

    def count_extract_stale(
        self, sam2_variant, detector_confidence, scope_sql, scope_params,
    ):
        """Count done-looking masks whose prompt no longer matches the primary."""
        ws = self.workspace_id
        from subjects import primary_order_sql
        row = self.conn.execute(
            f"""SELECT COUNT(DISTINCT pm.photo_id) AS n
                FROM photo_masks pm
                JOIN photos p ON p.id = pm.photo_id
                JOIN workspace_folders wf
                  ON wf.folder_id = p.folder_id AND wf.workspace_id = ?
               WHERE pm.variant = ?
                 AND p.mask_path IS NOT NULL
                 AND pm.path IS NOT NULL
                 AND pm.path != ''
                 AND EXISTS (
                    SELECT 1 FROM detections d0
                     WHERE d0.photo_id = pm.photo_id
                       AND d0.detector_model != 'full-image'
                       AND d0.detector_confidence >= ?
                 )
                 AND NOT EXISTS (
                    SELECT 1 FROM detections d
                     WHERE d.id = (
                           SELECT d2.id
                             FROM detections d2
                            WHERE d2.photo_id = pm.photo_id
                              AND d2.detector_model != 'full-image'
                              AND d2.detector_confidence >= ?
                            ORDER BY {primary_order_sql("d2")}
                            LIMIT 1
                       )
                       AND d.detector_model = pm.detector_model
                       AND d.box_x = pm.prompt_x
                       AND d.box_y = pm.prompt_y
                       AND d.box_w = pm.prompt_w
                       AND d.box_h = pm.prompt_h
                 ){scope_sql}""",
            (ws, sam2_variant, detector_confidence, detector_confidence,
             *scope_params),
        ).fetchone()
        return row["n"] or 0

    def count_eye_keypoint_eligible(self, min_conf, scope_sql, scope_params):
        """Count photos eligible for the eye-keypoint stage."""
        from subjects import primary_order_sql
        ws = self.workspace_id
        row = self.conn.execute(
            f"""SELECT COUNT(DISTINCT p.id) AS n
                FROM photos p
                JOIN workspace_folders wf
                  ON wf.folder_id = p.folder_id AND wf.workspace_id = ?
                JOIN detections d
                  ON d.photo_id = p.id
                 AND d.detector_model != 'full-image'
                 AND d.detector_confidence >= ?
                JOIN predictions pr ON pr.detection_id = d.id
                JOIN photo_masks pm
                  ON pm.photo_id = p.id
                 AND pm.variant = p.active_mask_variant
                 AND pm.detector_model = d.detector_model
                 AND pm.prompt_x = d.box_x
                 AND pm.prompt_y = d.box_y
                 AND pm.prompt_w = d.box_w
                 AND pm.prompt_h = d.box_h
                WHERE p.mask_path IS NOT NULL
                  AND p.active_mask_variant IS NOT NULL
                  AND d.id = (
                      SELECT d2.id FROM detections d2
                      WHERE d2.photo_id = p.id
                        AND d2.detector_confidence >= ?
                        AND d2.detector_model != 'full-image'
                      ORDER BY {primary_order_sql("d2")}
                      LIMIT 1
                  ){scope_sql}""",
            (ws, min_conf, min_conf, *scope_params),
        ).fetchone()
        return row["n"] or 0

    def count_eye_keypoint_stale(self, min_conf, scope_sql, scope_params):
        """Count eligible photos stamped under a non-current eye fingerprint."""
        from pipeline import EYE_KP_FINGERPRINT_VERSION
        from subjects import primary_order_sql
        ws = self.workspace_id
        row = self.conn.execute(
            f"""SELECT COUNT(DISTINCT p.id) AS n
                FROM photos p
                JOIN workspace_folders wf
                  ON wf.folder_id = p.folder_id AND wf.workspace_id = ?
                JOIN detections d
                  ON d.photo_id = p.id
                 AND d.detector_model != 'full-image'
                 AND d.detector_confidence >= ?
                JOIN predictions pr ON pr.detection_id = d.id
                JOIN photo_masks pm
                  ON pm.photo_id = p.id
                 AND pm.variant = p.active_mask_variant
                 AND pm.detector_model = d.detector_model
                 AND pm.prompt_x = d.box_x
                 AND pm.prompt_y = d.box_y
                 AND pm.prompt_w = d.box_w
                 AND pm.prompt_h = d.box_h
                WHERE p.mask_path IS NOT NULL
                  AND p.active_mask_variant IS NOT NULL
                  AND p.eye_tenengrad IS NOT NULL
                  AND (p.eye_kp_fingerprint IS NULL
                       OR p.eye_kp_fingerprint != ?)
                  AND d.id = (
                      SELECT d2.id FROM detections d2
                      WHERE d2.photo_id = p.id
                        AND d2.detector_confidence >= ?
                        AND d2.detector_model != 'full-image'
                      ORDER BY {primary_order_sql("d2")}
                      LIMIT 1
                  ){scope_sql}""",
            (ws, min_conf, EYE_KP_FINGERPRINT_VERSION, min_conf, *scope_params),
        ).fetchone()
        return row["n"] or 0

    def count_eye_keypoint_attemptable(
        self, min_species_conf, min_conf, scope_sql, scope_params,
    ):
        """Count photos whose winning prediction the eye stage would attempt."""
        from subjects import primary_order_sql
        ws = self.workspace_id
        # Window function pins the same per-photo prediction the stage
        # would pick (taxonomy-present first, then detector_conf desc,
        # then species_conf desc) so the attemptable filter is applied to
        # the *winner*, not to any prediction the photo happens to carry.
        # The labels_fingerprint subquery mirrors
        # list_photos_for_eye_keypoint_stage so re-classified detections
        # only contribute their latest prediction set. The selected-
        # primary and active-mask predicates mirror
        # list_photos_for_eye_keypoint_stage so predictions on non-primary
        # detections and photos with a stale mask are excluded from the
        # target — the stage cannot produce keypoints for those.
        row = self.conn.execute(
            f"""WITH ranked AS (
                    SELECT p.id AS photo_id,
                           pr.confidence AS species_conf,
                           pr.taxonomy_class,
                           ROW_NUMBER() OVER (
                               PARTITION BY p.id
                               ORDER BY
                                 CASE
                                     WHEN pr.taxonomy_class IS NOT NULL
                                       OR pr.scientific_name IS NOT NULL
                                     THEN 0 ELSE 1
                                 END,
                                 d.detector_confidence DESC,
                                 pr.confidence DESC
                           ) AS rn
                    FROM photos p
                    JOIN workspace_folders wf
                      ON wf.folder_id = p.folder_id
                     AND wf.workspace_id = ?
                    JOIN detections d
                      ON d.photo_id = p.id
                     AND d.detector_model != 'full-image'
                     AND d.detector_confidence >= ?
                    JOIN predictions pr ON pr.detection_id = d.id
                    JOIN photo_masks pm
                      ON pm.photo_id = p.id
                     AND pm.variant = p.active_mask_variant
                     AND pm.detector_model = d.detector_model
                     AND pm.prompt_x = d.box_x
                     AND pm.prompt_y = d.box_y
                     AND pm.prompt_w = d.box_w
                     AND pm.prompt_h = d.box_h
                    WHERE p.mask_path IS NOT NULL
                      AND p.active_mask_variant IS NOT NULL
                      AND d.id = (
                          SELECT d2.id FROM detections d2
                          WHERE d2.photo_id = p.id
                            AND d2.detector_confidence >= ?
                            AND d2.detector_model != 'full-image'
                          ORDER BY {primary_order_sql("d2")}
                          LIMIT 1
                      )
                      AND pr.labels_fingerprint = (
                          SELECT pr2.labels_fingerprint FROM predictions pr2
                          WHERE pr2.detection_id = pr.detection_id
                            AND pr2.classifier_model = pr.classifier_model
                          ORDER BY pr2.created_at DESC, pr2.id DESC
                          LIMIT 1
                      ){scope_sql}
                )
                SELECT COUNT(*) AS n FROM ranked
                WHERE rn = 1
                  AND taxonomy_class IN ('Aves', 'Mammalia')
                  AND species_conf >= ?""",
            (ws, min_conf, min_conf, *scope_params, min_species_conf),
        ).fetchone()
        return row["n"] or 0

    def get_dashboard(
        self, min_conf, preview_size, scope_sql, scope_params,
        location_conditions,
    ):
        """Scoped aggregate statistics and actionable gaps for the Dashboard.

        ``location_conditions`` is the Browse "no location" predicate list
        (``Database._append_location_status_filter(..., "none")``).
        """
        ws = self.workspace_id

        overview = self.conn.execute(
            f"""SELECT COUNT(DISTINCT p.id) AS total_photos,
                       COUNT(DISTINCT p.folder_id) AS folder_count,
                       COUNT(DISTINCT ({identity_sql()})) AS keyword_count,
                       COUNT(DISTINCT CASE
                         WHEN f.status IN ('ok', 'partial') THEN p.id END
                       ) AS accessible_photos,
                       COUNT(DISTINCT CASE
                         WHEN f.status NOT IN ('ok', 'partial') THEN f.id END
                       ) AS missing_folder_count
                FROM photos p
                JOIN workspace_folders wf
                  ON wf.folder_id = p.folder_id AND wf.workspace_id = ?
                JOIN folders f ON f.id = p.folder_id
                LEFT JOIN photo_keywords pk ON pk.photo_id = p.id
                LEFT JOIN keywords k ON k.id = pk.keyword_id
                WHERE 1=1{scope_sql}""",
            (ws, *scope_params),
        ).fetchone()

        # The four pure-metadata aggregates below (top_keywords,
        # photos_by_month, rating_dist, flag_dist) intentionally don't filter
        # on folder status. They read DB-resident metadata that doesn't depend
        # on disk access, so an unmounted drive shouldn't blank the charts —
        # the dashboard should still describe the full workspace inventory.
        # Share taxon/place identity with Keywords and Browse. Unresolved
        # same-name tags stay separate until their identity is established.
        top_keywords = self.conn.execute(
            f"""WITH scoped_tags AS (
                 SELECT pk.keyword_id, pk.photo_id FROM photo_keywords pk
                 JOIN photos p ON p.id = pk.photo_id
                 JOIN workspace_folders wf ON wf.folder_id = p.folder_id
                 WHERE wf.workspace_id = ?{scope_sql}
               ), identified AS (
                 SELECT k.*, {identity_sql()} AS identity FROM keywords k
                 WHERE k.id IN (SELECT keyword_id FROM scoped_tags)
               ), canonical AS (
                 SELECT *, ROW_NUMBER() OVER (
                   PARTITION BY identity ORDER BY parent_id IS NOT NULL, id
                 ) AS rn FROM identified
               ), counts AS (
               SELECT k.identity, COUNT(DISTINCT pk.photo_id) AS photo_count
               FROM identified k
               JOIN scoped_tags pk ON pk.keyword_id = k.id
               GROUP BY k.identity
               )
               SELECT c.id, c.name, c.is_species, c.identity, counts.photo_count
               FROM counts JOIN canonical c ON c.identity = counts.identity AND c.rn = 1
               ORDER BY photo_count DESC, c.name, c.id
               LIMIT 30""",
            (ws, *scope_params),
        ).fetchall()

        photos_by_month = self.conn.execute(
            f"""SELECT substr(p.timestamp, 1, 7) as month, COUNT(*) as count
            FROM photos p
            JOIN workspace_folders wf ON wf.folder_id = p.folder_id
            WHERE p.timestamp IS NOT NULL AND wf.workspace_id = ?{scope_sql}
            GROUP BY month
            ORDER BY month""",
            (ws, *scope_params),
        ).fetchall()

        rating_dist = self.conn.execute(
            f"""SELECT p.rating, COUNT(*) as count
            FROM photos p
            JOIN workspace_folders wf ON wf.folder_id = p.folder_id
            WHERE wf.workspace_id = ?{scope_sql}
            GROUP BY p.rating
            ORDER BY p.rating""",
            (ws, *scope_params),
        ).fetchall()

        flag_dist = self.conn.execute(
            f"""SELECT p.flag, COUNT(*) as count
            FROM photos p
            JOIN workspace_folders wf ON wf.folder_id = p.folder_id
            WHERE wf.workspace_id = ?{scope_sql}
            GROUP BY p.flag""",
            (ws, *scope_params),
        ).fetchall()

        # Review status lives in prediction_review (workspace-scoped).
        # Left-joining lets us count pending rows (those without a review row)
        # and bucket them into the pending column via COALESCE.
        #
        # Filter by detector_confidence so dashboard status counts stay in
        # sync with what the UI threshold actually shows, and scope to the
        # most recent labels_fingerprint per (detection, classifier_model)
        # so stale-label predictions from a prior label set don't drift the
        # totals away from the active labeling context.
        prediction_status = self.conn.execute(
            f"""SELECT COALESCE(pr_rev.status, 'pending') AS status,
                      COUNT(*) AS count
               FROM predictions pr
               JOIN detections d ON d.id = pr.detection_id
               JOIN photos p ON p.id = d.photo_id
               JOIN workspace_folders wf
                 ON wf.folder_id = p.folder_id AND wf.workspace_id = ?
               LEFT JOIN prediction_review pr_rev
                 ON pr_rev.prediction_id = pr.id AND pr_rev.workspace_id = ?
               WHERE d.detector_confidence >= ?
                 AND pr.labels_fingerprint = (
                    SELECT pr2.labels_fingerprint FROM predictions pr2
                    WHERE pr2.detection_id = pr.detection_id
                      AND pr2.classifier_model = pr.classifier_model
                    ORDER BY pr2.created_at DESC, pr2.id DESC
                    LIMIT 1
                 ){scope_sql}
               GROUP BY COALESCE(pr_rev.status, 'pending')""",
            (ws, ws, min_conf, *scope_params),
        ).fetchall()

        # Same threshold + fingerprint rules as prediction_status above, so
        # classified_count can't drift above detected_count as the threshold
        # moves.
        classified_count = self.conn.execute(
            f"""SELECT COUNT(DISTINCT d.photo_id)
               FROM predictions pr
               JOIN detections d ON d.id = pr.detection_id
               JOIN photos p ON p.id = d.photo_id
               JOIN workspace_folders wf
                 ON wf.folder_id = p.folder_id AND wf.workspace_id = ?
               WHERE d.detector_confidence >= ?
                 AND pr.labels_fingerprint = (
                    SELECT pr2.labels_fingerprint FROM predictions pr2
                    WHERE pr2.detection_id = pr.detection_id
                      AND pr2.classifier_model = pr.classifier_model
                    ORDER BY pr2.created_at DESC, pr2.id DESC
                    LIMIT 1
                 ){scope_sql}""",
            (ws, min_conf, *scope_params),
        ).fetchone()[0]

        # Needs Attention links only open photos that are currently
        # accessible. Keep the headline classification aggregate metadata-
        # complete above, but use this reachable subset for operational work.
        accessible_classified_count = self.conn.execute(
            f"""SELECT COUNT(DISTINCT d.photo_id)
               FROM predictions pr
               JOIN detections d ON d.id = pr.detection_id
               JOIN photos p ON p.id = d.photo_id
               JOIN workspace_folders wf
                 ON wf.folder_id = p.folder_id AND wf.workspace_id = ?
               JOIN folders f
                 ON f.id = p.folder_id AND f.status IN ('ok', 'partial')
               WHERE d.detector_confidence >= ?
                 AND pr.labels_fingerprint = (
                    SELECT pr2.labels_fingerprint FROM predictions pr2
                    WHERE pr2.detection_id = pr.detection_id
                      AND pr2.classifier_model = pr.classifier_model
                    ORDER BY pr2.created_at DESC, pr2.id DESC
                    LIMIT 1
                 ){scope_sql}""",
            (ws, min_conf, *scope_params),
        ).fetchone()[0]

        # photos_by_hour and quality_dist are also pure-metadata aggregates;
        # see the comment above the top_keywords block for the rationale.
        photos_by_hour = self.conn.execute(
            f"""SELECT CAST(substr(p.timestamp, 12, 2) AS INTEGER) as hour, COUNT(*) as count
            FROM photos p
            JOIN workspace_folders wf ON wf.folder_id = p.folder_id
            WHERE p.timestamp IS NOT NULL AND length(p.timestamp) >= 13
              AND wf.workspace_id = ?{scope_sql}
            GROUP BY hour
            ORDER BY hour""",
            (ws, *scope_params),
        ).fetchall()

        quality_dist = self.conn.execute(
            f"""SELECT
                CASE
                    WHEN p.quality_score IS NULL THEN -1
                    ELSE CAST(p.quality_score * 10 AS INTEGER)
                END as bucket,
                COUNT(*) as count
            FROM photos p
            JOIN workspace_folders wf ON wf.folder_id = p.folder_id
            WHERE wf.workspace_id = ?{scope_sql}
            GROUP BY bucket
            ORDER BY bucket""",
            (ws, *scope_params),
        ).fetchall()

        # min_conf already hoisted at top of get_dashboard_stats.
        # No folder-status filter — detections persist in the DB regardless
        # of disk presence, and prediction_status / classified_count above
        # don't filter either, so detected_count must match to keep the
        # dashboard's classified-vs-detected ratio internally consistent
        # when a folder is offline.
        detected_count = self.conn.execute(
            f"""SELECT COUNT(DISTINCT d.photo_id)
               FROM detections d
               JOIN photos p ON p.id = d.photo_id
               JOIN workspace_folders wf ON wf.folder_id = p.folder_id
               WHERE wf.workspace_id = ?
                 AND d.detector_confidence >= ?{scope_sql}""",
            (ws, min_conf, *scope_params),
        ).fetchone()[0]

        missing_location = self.conn.execute(
            f"""SELECT COUNT(DISTINCT p.id)
                FROM photos p
                JOIN workspace_folders wf
                  ON wf.folder_id = p.folder_id AND wf.workspace_id = ?
                JOIN folders f
                  ON f.id = p.folder_id AND f.status IN ('ok', 'partial')
                WHERE {' AND '.join(location_conditions)}{scope_sql}""",
            (ws, *scope_params),
        ).fetchone()[0]

        pending_changes = self.conn.execute(
            f"""SELECT COUNT(*)
                FROM pending_changes pc
                JOIN photos p ON p.id = pc.photo_id
                JOIN workspace_folders wf
                  ON wf.folder_id = p.folder_id AND wf.workspace_id = ?
                WHERE pc.workspace_id = ?{scope_sql}""",
            (ws, ws, *scope_params),
        ).fetchone()[0]

        if preview_size:
            missing_previews = self.conn.execute(
                f"""SELECT COUNT(DISTINCT p.id)
                    FROM photos p
                    JOIN workspace_folders wf
                      ON wf.folder_id = p.folder_id AND wf.workspace_id = ?
                    JOIN folders f
                      ON f.id = p.folder_id
                     AND f.status IN ('ok', 'partial')
                    LEFT JOIN preview_cache pc
                      ON pc.photo_id = p.id AND pc.size = ?
                    WHERE pc.photo_id IS NULL{scope_sql}""",
                (ws, preview_size, *scope_params),
            ).fetchone()[0]
        else:
            missing_previews = 0

        duplicate_groups = self.conn.execute(
            f"""SELECT COUNT(*) FROM (
                  SELECT p.file_hash
                  FROM photos p
                  JOIN workspace_folders wf
                    ON wf.folder_id = p.folder_id AND wf.workspace_id = ?
                  JOIN folders f
                    ON f.id = p.folder_id AND f.status IN ('ok', 'partial')
                  WHERE p.file_hash IS NOT NULL
                    AND COALESCE(p.flag, 'none') != 'rejected'{scope_sql}
                  GROUP BY p.file_hash
                  HAVING COUNT(*) > 1
                )""",
            (ws, *scope_params),
        ).fetchone()[0]

        total_photos = overview["total_photos"] or 0

        return {
            "top_keywords": [dict(r) for r in top_keywords],
            "photos_by_month": [dict(r) for r in photos_by_month],
            "rating_distribution": [dict(r) for r in rating_dist],
            "flag_distribution": [dict(r) for r in flag_dist],
            "prediction_status": [dict(r) for r in prediction_status],
            "classified_count": classified_count,
            "photos_by_hour": [dict(r) for r in photos_by_hour],
            "quality_distribution": [dict(r) for r in quality_dist],
            "detected_count": detected_count,
            "total_photos": total_photos,
            "accessible_photos": overview["accessible_photos"] or 0,
            "missing_folder_count": overview["missing_folder_count"] or 0,
            "folder_count": overview["folder_count"] or 0,
            "keyword_count": overview["keyword_count"] or 0,
            "pending_changes": pending_changes,
            "attention": {
                "unclassified": max(
                    0,
                    (overview["accessible_photos"] or 0)
                    - accessible_classified_count,
                ),
                "missing_location": missing_location,
                "missing_previews": missing_previews,
                "preview_size": preview_size,
                "preview_enabled": bool(preview_size),
                "pending_sync": pending_changes,
                "duplicate_groups": duplicate_groups,
            },
        }
