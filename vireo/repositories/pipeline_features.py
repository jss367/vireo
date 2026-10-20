"""Reads behind the grouping pipeline's feature load and review readiness.

``pipeline.load_photo_features`` reads every grouping input of a workspace's
photos (the photo feature columns, each qualifying animal detection with its
subject analysis, and the species predictions of the latest or pinned label
set); ``pipeline.compute_review_readiness`` counts how many of those photos
each stage could fill; the eye-keypoint stage resolves a photo's effective
primary box and routes a species name to a taxon; and the results cache is
reconciled against the photos still in the workspace. Nothing here writes a
catalog table.

Every workspace-scoped read takes the workspace id from its caller, which has
already resolved it (``db.require_workspace_id()``) before reading config, so
the repository holds no workspace callable. The feature load stages its photo
scope, its contextual weak-detection candidates and its continuity evidence
in connection-local TEMP tables (``pipeline_scope_ids``,
``pipeline_weak_detection_ids``, ``pipeline_continuity_detections``); the
``stage_*`` methods own those tables and the readers below name them, so the
table names never leave this module.

Callers reach it as ``db.pipeline_features`` (a fresh repository per access,
see ``Database.pipeline_features``); there are no forwarding wrappers on
``Database``. It keeps nothing that is not SQL: the confidence floors, the
embedding-dimension lookup and every decision stay in ``pipeline``.
"""

import sqlite3
from collections.abc import Iterable, Sequence
from typing import Any

# Photo columns the pipeline stages read for each workspace photo.
PIPELINE_PHOTO_COLS = """
    p.id, p.folder_id, p.filename, p.timestamp,
    p.width, p.height, p.latitude, p.longitude,
    p.subject_size,
    p.mask_path, p.subject_tenengrad, p.bg_tenengrad,
    p.crop_complete, p.bg_separation,
    p.subject_clip_high, p.subject_clip_low, p.subject_y_median,
    p.phash_crop,
    p.dino_subject_embedding, p.dino_global_embedding,
    p.dino_embedding_variant,
    p.focal_length, p.burst_id, p.noise_estimate,
    p.flag, p.rating,
    p.eye_x, p.eye_y, p.eye_conf, p.eye_tenengrad
"""

_SCOPE_SQL = " AND p.id IN (SELECT id FROM pipeline_scope_ids)"

_WEAK_DETECTION_FLOOR_SQL = """AND (
              d.detector_confidence >= ?
              OR (
                  d.detector_confidence >= ?
                  AND d.detector_model = 'megadetector-v6'
                  AND d.category = 'animal'
                  AND d.photo_id IN (
                      SELECT id FROM pipeline_weak_detection_ids
                  )
              )
          )"""

# The results-cache presence lookup stays under SQLite's bound-parameter cap.
_PRESENCE_CHUNK = 900


class PipelineFeaturesRepository:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    # -- connection-local scope tables ------------------------------------------

    def _replace_temp_id_scope(self, table_name: str, ids: Iterable[int]) -> None:
        """Replace a connection-local temp ID table without leaking a transaction.

        SQLite DML against a TEMP table opens a transaction on the connection. If
        that transaction remains open while the caller reads the main WAL database,
        a concurrent writer can advance the main database and make a later write on
        this connection fail immediately with ``SQLITE_BUSY_SNAPSHOT`` (surfaced by
        Python as ``database is locked``). Commit only when this helper opened the
        transaction; an outer transaction still belongs entirely to the caller.
        """
        conn = self.conn
        started_in_transaction = conn.in_transaction
        try:
            conn.execute(
                f"CREATE TEMP TABLE IF NOT EXISTS {table_name} "
                "(id INTEGER PRIMARY KEY)"
            )
            conn.execute(f"DELETE FROM {table_name}")
            conn.executemany(
                f"INSERT OR IGNORE INTO {table_name} (id) VALUES (?)",
                [(photo_id,) for photo_id in ids],
            )
            if not started_in_transaction and conn.in_transaction:
                conn.commit()
        except Exception:
            if not started_in_transaction and conn.in_transaction:
                conn.rollback()
            raise

    def stage_scope_photo_ids(self, photo_ids: Iterable[int]) -> None:
        """Stage the feature load's photo scope (read back with ``scoped=True``).

        A temp table instead of inline placeholders: a large collection
        exceeds SQLite's bound-parameter cap (999 on legacy builds), and the
        scope is applied to several queries.
        """
        self._replace_temp_id_scope("pipeline_scope_ids", photo_ids)

    def stage_weak_detection_photo_ids(self, photo_ids: Iterable[int]) -> None:
        """Stage the photos whose MegaDetector boxes get the lower weak floor."""
        self._replace_temp_id_scope("pipeline_weak_detection_ids", photo_ids)

    def stage_continuity_detection_ids(self, detection_ids: Iterable[int]) -> None:
        """Stage the detections read with ``prediction_rows(continuity_only=True)``."""
        self._replace_temp_id_scope("pipeline_continuity_detections", detection_ids)

    # -- feature load -----------------------------------------------------------

    def feature_rows(self, workspace_id: int, *, scoped: bool) -> list[sqlite3.Row]:
        """``PIPELINE_PHOTO_COLS`` for the workspace's photos, in capture order.

        ``scoped`` limits them to the staged ``stage_scope_photo_ids`` set.
        """
        if scoped:
            return self.conn.execute(
                f"""SELECT {PIPELINE_PHOTO_COLS}
                    FROM photos p
                    JOIN photo_workspace_visibility wf ON wf.photo_id = p.id
                    WHERE wf.workspace_id = ?
                      AND p.id IN (SELECT id FROM pipeline_scope_ids)
                    ORDER BY p.timestamp, p.filename ASC, p.id ASC""",
                (workspace_id,),
            ).fetchall()
        return self.conn.execute(
            f"""SELECT {PIPELINE_PHOTO_COLS}
                FROM photos p
                JOIN photo_workspace_visibility wf ON wf.photo_id = p.id
                WHERE wf.workspace_id = ?
                ORDER BY p.timestamp, p.filename ASC, p.id ASC""",
            (workspace_id,),
        ).fetchall()

    @staticmethod
    def _detection_floor(min_conf: float, weak_confidence: float | None) -> tuple[str, tuple[Any, ...]]:
        """The detector-confidence predicate and its parameters.

        With ``weak_confidence`` the staged weak-candidate photos' MegaDetector
        animal boxes pass at that lower floor; everything else keeps
        ``min_conf``, which stays index-friendly for the rest of the workspace.
        """
        if weak_confidence is None:
            return "AND d.detector_confidence >= ?", (min_conf,)
        return _WEAK_DETECTION_FLOOR_SQL, (min_conf, weak_confidence)

    def subject_detection_rows(
        self, workspace_id: int, *, min_conf: float, weak_confidence: float | None,
        scoped: bool,
    ) -> list[sqlite3.Row]:
        """Every qualifying animal detection with its subject analysis.

        Ordered by photo, then primary-subject order (``subjects.primary_order_sql``).
        """
        from subjects import primary_order_sql
        floor_sql, floor_params = self._detection_floor(min_conf, weak_confidence)
        scope_sql = _SCOPE_SQL if scoped else ""
        return self.conn.execute(
            f"""SELECT d.id AS detection_id, d.photo_id,
                       d.box_x, d.box_y, d.box_w, d.box_h,
                       d.detector_confidence, d.category, d.detector_model,
                       ds.crop AS suggested_crop, ds.quality_score AS subject_quality_score,
                       ds.exposure_ev
                FROM detections d
                LEFT JOIN detection_subjects ds ON ds.detection_id=d.id
                JOIN photos p ON p.id = d.photo_id
                JOIN photo_workspace_visibility wf ON wf.photo_id = p.id
                WHERE wf.workspace_id = ?
                  {floor_sql}
                  AND d.detector_model != 'full-image'
                  AND d.category = 'animal'
                  {scope_sql}
                ORDER BY d.photo_id, {primary_order_sql("d")}""",
            (workspace_id, *floor_params),
        ).fetchall()

    def prediction_rows(
        self, workspace_id: int, *, min_conf: float, weak_confidence: float | None,
        scoped: bool, labels_fingerprint: str | None,
        full_image_only: bool = False, continuity_only: bool = False,
    ) -> list[sqlite3.Row]:
        """Species predictions joined to their detections, by photo then confidence.

        The detection predicate is the confidence floor, or only full-image
        classifications (``full_image_only``), or only the staged continuity
        detections (``continuity_only``, which wins). ``labels_fingerprint``
        pins one label set; without it each (detection, classifier) pair
        contributes only its most recent label set.
        """
        floor_sql, floor_params = self._detection_floor(min_conf, weak_confidence)
        if full_image_only:
            floor_sql, floor_params = "AND d.detector_model = 'full-image'", ()
        if continuity_only:
            floor_sql = "AND d.id IN (SELECT id FROM pipeline_continuity_detections)"
            floor_params = ()
        scope_sql = _SCOPE_SQL if scoped else ""
        if labels_fingerprint is not None:
            return self.conn.execute(
                f"""SELECT d.photo_id, d.id AS detection_id,
                          pr.species, pr.confidence, pr.scientific_name, pr.labels_fingerprint, pr.source_taxon_id,
                          pr.classifier_model AS model,
                          d.detector_confidence, d.detector_model
                   FROM predictions pr
                   JOIN detections d ON d.id = pr.detection_id
                   JOIN photos p ON p.id = d.photo_id
                   JOIN photo_workspace_visibility wf ON wf.photo_id = p.id
                   WHERE wf.workspace_id = ?
                     {floor_sql}
                     AND pr.labels_fingerprint = ?
                     {scope_sql}
                   ORDER BY d.photo_id, pr.confidence DESC""",
                (workspace_id, *floor_params, labels_fingerprint),
            ).fetchall()
        return self.conn.execute(
            f"""SELECT d.photo_id, d.id AS detection_id,
                      pr.species, pr.confidence, pr.scientific_name, pr.labels_fingerprint, pr.source_taxon_id,
                      pr.classifier_model AS model,
                      d.detector_confidence, d.detector_model
               FROM predictions pr
               JOIN detections d ON d.id = pr.detection_id
               JOIN photos p ON p.id = d.photo_id
               JOIN photo_workspace_visibility wf ON wf.photo_id = p.id
               WHERE wf.workspace_id = ?
                 {floor_sql}
                 {scope_sql}
                 AND pr.labels_fingerprint = (
                     SELECT pr2.labels_fingerprint FROM predictions pr2
                     WHERE pr2.detection_id = pr.detection_id
                       AND pr2.classifier_model = pr.classifier_model
                     ORDER BY pr2.created_at DESC, pr2.id DESC
                     LIMIT 1
                 )
               ORDER BY d.photo_id, pr.confidence DESC""",
            (workspace_id, *floor_params),
        ).fetchall()

    def analysis_provenance_rows(self, workspace_id: int, *, scoped: bool) -> list[sqlite3.Row]:
        """Detector completion and active-mask provenance for current workspace inputs.

        Kept separate from quality columns so an empty detections cache never
        masquerades as a completed detector verdict. A detector run can be
        legacy (no portable input fingerprint); its presence still establishes
        completion, while stamped runs can be checked against source identity.
        """
        scope_sql = _SCOPE_SQL if scoped else ""
        return self.conn.execute(
            f"""SELECT p.id, p.file_hash, p.companion_path, p.active_mask_variant,
                       p.quality_input_recipe,
                       dr.detector_model AS run_model, dr.input_fingerprint,
                       pm.path AS stored_mask_path, pm.detector_model AS mask_model,
                       pm.prompt_x, pm.prompt_y, pm.prompt_w, pm.prompt_h
                  FROM photos p
                  JOIN photo_workspace_visibility wf ON wf.photo_id = p.id
                  LEFT JOIN detector_runs dr ON dr.photo_id = p.id
                       AND dr.detector_model != 'full-image'
                  LEFT JOIN photo_masks pm ON pm.photo_id = p.id
                       AND pm.variant = p.active_mask_variant
                 WHERE wf.workspace_id = ? {scope_sql}""",
            (workspace_id,),
        ).fetchall()

    # -- workspace membership -----------------------------------------------------

    def workspace_photo_ids(self, workspace_id: int) -> set[int]:
        """Every photo id visible in ``workspace_id``."""
        return {
            r["id"] for r in self.conn.execute(
                """SELECT p.id
                     FROM photos p
                     JOIN photo_workspace_visibility wf
                       ON wf.photo_id = p.id
                    WHERE wf.workspace_id = ?""",
                (workspace_id,),
            ).fetchall()
        }

    def present_workspace_photo_ids(self, workspace_id: int, photo_ids: Sequence[int]) -> set[int]:
        """The ids in ``photo_ids`` that are still photos visible in ``workspace_id``.

        The ``IN (...)`` lookup is chunked to stay under SQLite's default
        bound-parameter cap.
        """
        present: set = set()
        for i in range(0, len(photo_ids), _PRESENCE_CHUNK):
            chunk = photo_ids[i : i + _PRESENCE_CHUNK]
            placeholders = ",".join("?" * len(chunk))
            rows = self.conn.execute(
                f"""SELECT p.id FROM photos p
                    JOIN photo_workspace_visibility wf ON wf.photo_id = p.id
                    WHERE wf.workspace_id = ? AND p.id IN ({placeholders})""",
                (workspace_id, *chunk),
            ).fetchall()
            present.update(row["id"] for row in rows)
        return present

    # -- review readiness ---------------------------------------------------------

    def count_detected_photos(self, workspace_id: int, min_conf: float) -> int:
        """Workspace photos in reachable folders with a real detector box at ``min_conf``."""
        row = self.conn.execute(
            """SELECT COUNT(DISTINCT p.id) AS n
               FROM photos p
               JOIN photo_workspace_visibility wf ON wf.photo_id = p.id
               JOIN folders f ON f.id = p.folder_id
                             AND f.status IN ('ok', 'partial')
               JOIN detections d ON d.photo_id = p.id
                                AND d.detector_model != 'full-image'
                                AND d.detector_confidence >= ?
               WHERE wf.workspace_id = ?""",
            (min_conf, workspace_id),
        ).fetchone()
        return row["n"] or 0

    def count_subject_embeddings(
        self, workspace_id: int, *, detection_min_conf: float | None = None,
        variant: str | None = None, legacy_bytes: int | None = None,
    ) -> int:
        """Workspace photos in reachable folders with a stored subject embedding.

        ``detection_min_conf`` also requires a real detector box at that
        floor. ``variant`` requires that stored variant; with ``legacy_bytes``
        an unstamped embedding of exactly that byte length also counts.
        """
        join_detection = ""
        params_prefix = []
        if detection_min_conf is not None:
            join_detection = (
                "JOIN detections d ON d.photo_id = p.id "
                "AND d.detector_model != 'full-image' "
                "AND d.detector_confidence >= ?"
            )
            params_prefix.append(detection_min_conf)

        if variant is None:
            row = self.conn.execute(
                f"""SELECT COUNT(DISTINCT p.id) AS n
                   FROM photos p
                   JOIN photo_workspace_visibility wf ON wf.photo_id = p.id
                   JOIN folders f ON f.id = p.folder_id
                                 AND f.status IN ('ok', 'partial')
                   {join_detection}
                   WHERE wf.workspace_id = ?
                     AND p.dino_subject_embedding IS NOT NULL""",
                (*params_prefix, workspace_id),
            ).fetchone()
            return row["n"] or 0

        if legacy_bytes is None:
            row = self.conn.execute(
                f"""SELECT COUNT(DISTINCT p.id) AS n
                   FROM photos p
                   JOIN photo_workspace_visibility wf ON wf.photo_id = p.id
                   JOIN folders f ON f.id = p.folder_id
                                 AND f.status IN ('ok', 'partial')
                   {join_detection}
                   WHERE wf.workspace_id = ?
                     AND p.dino_subject_embedding IS NOT NULL
                     AND p.dino_embedding_variant = ?""",
                (*params_prefix, workspace_id, variant),
            ).fetchone()
            return row["n"] or 0

        row = self.conn.execute(
            f"""SELECT COUNT(DISTINCT p.id) AS n
               FROM photos p
               JOIN photo_workspace_visibility wf ON wf.photo_id = p.id
               JOIN folders f ON f.id = p.folder_id
                             AND f.status IN ('ok', 'partial')
               {join_detection}
               WHERE wf.workspace_id = ?
                 AND p.dino_subject_embedding IS NOT NULL
                 AND (p.dino_embedding_variant = ?
                      OR (p.dino_embedding_variant IS NULL
                          AND length(p.dino_subject_embedding) = ?))""",
            (*params_prefix, workspace_id, variant, legacy_bytes),
        ).fetchone()
        return row["n"] or 0

    def count_eye_keypoint_attempts(self, workspace_id: int, min_conf: float, fingerprint: str) -> int:
        """Masked, detected, classified workspace photos already evaluated by eye keypoints.

        A photo counts when it carries ``fingerprint`` or a legacy ``eye_x``.
        """
        row = self.conn.execute(
            """SELECT COUNT(DISTINCT p.id) AS n
               FROM photos p
               JOIN photo_workspace_visibility wf
                 ON wf.photo_id = p.id AND wf.workspace_id = ?
               JOIN folders f ON f.id = p.folder_id
                             AND f.status IN ('ok', 'partial')
               JOIN detections d
                 ON d.photo_id = p.id
                AND d.detector_model != 'full-image'
                AND d.detector_confidence >= ?
               JOIN predictions pr ON pr.detection_id = d.id
               WHERE p.mask_path IS NOT NULL
                 AND (p.eye_kp_fingerprint = ? OR p.eye_x IS NOT NULL)""",
            (workspace_id, min_conf, fingerprint),
        ).fetchone()
        return row["n"] or 0

    # -- eye-keypoint stage ---------------------------------------------------------

    def taxon_by_name(self, name: str) -> sqlite3.Row | None:
        """The first ``taxa`` row named ``name`` (its ``inat_id``), or None."""
        return self.conn.execute(
            "SELECT inat_id FROM taxa WHERE name = ? LIMIT 1", (name,)
        ).fetchone()

    def effective_primary_box(self, photo_id: int, min_conf: float) -> sqlite3.Row | None:
        """The top-ordered non-full-image detection at ``min_conf``, or None.

        Carries ``id`` and ``box_x/y/w/h``; ordered by
        ``subjects.primary_order_sql``, as the mask and eye stages choose.
        """
        from subjects import primary_order_sql
        return self.conn.execute(
            f"""SELECT d.id, d.box_x, d.box_y, d.box_w, d.box_h
                  FROM detections d
                 WHERE d.photo_id = ?
                   AND d.detector_confidence >= ?
                   AND d.detector_model != 'full-image'
                 ORDER BY {primary_order_sql("d")}
                 LIMIT 1""",
            (photo_id, min_conf),
        ).fetchone()
