"""Persistence for SAM masks, pipeline features, and embeddings.

``photo_masks``, ``photo_embeddings``, ``subject_raw_analysis`` and the
feature columns on ``photos`` are catalog-wide: a mask or an embedding is a
pure function of the photo and the model that produced it. The selectors
that the pipeline page and stages read (variant coverage, the SAM rerun
warning, the mask and eye-keypoint stage queues, stored embeddings) are
scoped to the active workspace through ``self.workspace_id``, which the
façade resolves with ``Database._ws_id()`` when it builds the repository.

``Database`` keeps everything that is not SQL: the workspace detector floor
(``get_effective_config``), the photo-scope clause (``_scope_clause``), the
workspace guard on embedding writes, and the masks-directory containment
check (``_safe_remove_mask_file``), which the cleanup deletes receive as the
``remove_file`` callback so file removal keeps its place between the reads
and the ``DELETE``.
"""

import json
import os
import time

from repositories import UNSET


class MasksFeaturesRepository:
    def __init__(self, conn, workspace_id, *, commit_with_retry):
        self.conn = conn
        self.workspace_id = workspace_id
        # ``db.commit_with_retry``, passed in so repositories import no
        # ``db`` code and a monkeypatch of the module function still applies.
        self.commit_with_retry = commit_with_retry

    # -- mask rows -----------------------------------------------------------

    def get_mask(self, photo_id, variant):
        row = self.conn.execute(
            "SELECT * FROM photo_masks WHERE photo_id=? AND variant=?",
            (photo_id, variant),
        ).fetchone()
        return dict(row) if row else None

    def list_masks_for_photo(self, photo_id):
        rows = self.conn.execute(
            "SELECT * FROM photo_masks WHERE photo_id=? ORDER BY created_at DESC",
            (photo_id,),
        ).fetchall()
        return [dict(r) for r in rows]

    def set_active_variant(self, photo_id, variant, min_conf, _commit=True, *,
                           weak_rescue_min_conf=None):
        """Activate ``variant`` for ``photo_id`` against the ``min_conf`` floor.

        ``min_conf`` is the active workspace's ``detector_confidence``,
        resolved by the façade.
        """
        # Resolve the effective primary detection the same way both
        # mask-extraction paths do: the top-ordered non-full-image
        # detection above the workspace's current detector_confidence
        # floor. ``photo_subject_state.detection_id`` can lag when the
        # floor changes (workspace override, or another workspace
        # sharing this photo runs with a different floor), so checking
        # the mask's prompt against the cached state would reject a
        # mask that extraction just produced from the current primary.
        from subjects import primary_order_sql
        detector_filter = ""
        if weak_rescue_min_conf is not None:
            # Only the pipeline's validated contextual rescue may lower the
            # floor. Bulk activation has no such context and rejects by default.
            min_conf = weak_rescue_min_conf
            detector_filter = "AND detector_model='megadetector-v6' "
        row = self.conn.execute(
            f"SELECT pm.*, d.detector_model AS primary_model, "
            f"d.box_x AS primary_x, d.box_y AS primary_y, "
            f"d.box_w AS primary_w, d.box_h AS primary_h "
            f"FROM photo_masks pm LEFT JOIN detections d ON d.id=("
            f"SELECT id FROM detections WHERE photo_id=pm.photo_id "
            f"AND detector_confidence>=? AND detector_model!='full-image' "
            f"AND category='animal' {detector_filter} "
            f"ORDER BY {primary_order_sql()} LIMIT 1) "
            f"WHERE pm.photo_id=? AND pm.variant=?",
            (min_conf, photo_id, variant),
        ).fetchone()
        if row is None:
            raise ValueError(
                f"No photo_masks row for photo {photo_id} variant {variant!r}"
            )
        if row["primary_model"] is not None:
            # A primary detection sits above the workspace's current floor:
            # it must match the mask exactly, otherwise the mask represents
            # a different subject.
            if (row["detector_model"] != row["primary_model"]
                    or any(row["prompt_" + k] != row["primary_" + k] for k in "xywh")):
                raise ValueError("This mask belongs to another subject; run mask extraction for the primary subject")
        else:
            # Preserve pre-detection migration rows, but never reactivate an
            # orphan or a below-floor detection without explicit rescue context.
            has_detection_context = self.conn.execute(
                "SELECT 1 WHERE EXISTS (SELECT 1 FROM detections WHERE photo_id=? "
                "AND detector_model!='full-image') OR EXISTS (SELECT 1 FROM detector_runs "
                "WHERE photo_id=? AND detector_model!='full-image')",
                (photo_id, photo_id),
            ).fetchone()
            if has_detection_context:
                raise ValueError("This mask belongs to another subject; run mask extraction for the primary subject")
        self.conn.execute(
            "UPDATE photos SET mask_path=?, active_mask_variant=?, "
            "subject_size=?, subject_tenengrad=?, bg_tenengrad=?, "
            "crop_complete=?, quality_input_recipe=?, subject_clip_high=?, subject_clip_low=?, "
            "subject_y_median=?, bg_separation=?, phash_crop=?, noise_estimate=? WHERE id=?",
            (row["path"], variant, row["subject_size"],
             row["subject_tenengrad"], row["bg_tenengrad"],
             row["crop_complete"], row["quality_input_recipe"],
             row["subject_clip_high"], row["subject_clip_low"], row["subject_y_median"],
             row["bg_separation"], row["phash_crop"], row["noise_estimate"], photo_id),
        )
        if _commit:
            self.commit_with_retry(self.conn)

    # -- storage cleanup -----------------------------------------------------

    def delete_for_variant(self, variant, remove_file):
        """Delete all photo_masks rows + files for a variant.
        Refuses if the variant is active for any photo (caller must
        switch active first). ``remove_file`` is the façade's contained
        file delete."""
        active_count = self.conn.execute(
            "SELECT COUNT(*) FROM photos WHERE active_mask_variant=?",
            (variant,),
        ).fetchone()[0]
        if active_count > 0:
            raise ValueError(
                f"Variant {variant!r} is active for {active_count} photo(s); "
                "switch active variant before deleting"
            )
        rows = self.conn.execute(
            "SELECT path FROM photo_masks WHERE variant=?", (variant,),
        ).fetchall()
        for r in rows:
            remove_file(r["path"])
        self.conn.execute("DELETE FROM photo_masks WHERE variant=?", (variant,))
        self.commit_with_retry(self.conn)
        return len(rows)

    def delete_inactive(self, remove_file):
        """Delete all photo_masks rows + files except the active variant
        per photo. Returns the number of rows deleted.

        Photos whose ``active_mask_variant IS NULL`` are skipped entirely.
        """
        rows = self.conn.execute(
            "SELECT pm.photo_id, pm.variant, pm.path FROM photo_masks pm "
            "JOIN photos p ON p.id = pm.photo_id "
            "WHERE p.active_mask_variant IS NOT NULL "
            "  AND p.active_mask_variant != pm.variant"
        ).fetchall()
        for r in rows:
            remove_file(r["path"])
            self.conn.execute(
                "DELETE FROM photo_masks WHERE photo_id=? AND variant=?",
                (r["photo_id"], r["variant"]),
            )
        self.commit_with_retry(self.conn)
        return len(rows)

    def find_stale(self, detector_confidence=None):
        """Return masks whose prompts differ from the selected primary."""
        from subjects import primary_order_sql
        if detector_confidence is None:
            conf_pred = ""
            params = ()
        else:
            conf_pred = " AND d2.detector_confidence >= ?"
            params = (detector_confidence,)
        rows = self.conn.execute(
            f"""
            SELECT pm.photo_id, pm.variant, pm.path,
                   pm.detector_model, pm.prompt_x, pm.prompt_y,
                   pm.prompt_w, pm.prompt_h
              FROM photo_masks pm
             WHERE NOT EXISTS (
                SELECT 1 FROM detections d
                 WHERE d.id = (
                       SELECT d2.id
                         FROM detections d2
                        WHERE d2.photo_id = pm.photo_id
                          AND d2.detector_model != 'full-image'
                          {conf_pred}
                        ORDER BY {primary_order_sql("d2")}
                        LIMIT 1
                   )
                   AND d.detector_model = pm.detector_model
                   AND d.box_x = pm.prompt_x
                   AND d.box_y = pm.prompt_y
                   AND d.box_w = pm.prompt_w
                   AND d.box_h = pm.prompt_h
             )
            """,
            params,
        ).fetchall()
        return [dict(r) for r in rows]

    def delete_stale(self, stale, remove_file):
        """Remove rows + files for the ``stale`` masks (the façade's
        ``find_stale_masks`` result), skipping active variants."""
        deleted = 0
        for s in stale:
            is_active = self.conn.execute(
                "SELECT 1 FROM photos WHERE id=? AND active_mask_variant=?",
                (s["photo_id"], s["variant"]),
            ).fetchone()
            if is_active:
                continue
            remove_file(s["path"])
            self.conn.execute(
                "DELETE FROM photo_masks WHERE photo_id=? AND variant=?",
                (s["photo_id"], s["variant"]),
            )
            deleted += 1
        self.commit_with_retry(self.conn)
        return deleted

    # -- coverage and summaries ----------------------------------------------

    def variant_coverage(self):
        """Per-variant photo coverage in the bound workspace."""
        ws = self.workspace_id
        rows = self.conn.execute(
            """
            SELECT pm.variant,
                   COUNT(DISTINCT pm.photo_id) AS count,
                   SUM(CASE WHEN p.active_mask_variant = pm.variant
                            THEN 1 ELSE 0 END) AS active_count
              FROM photo_masks pm
              JOIN photos p ON p.id = pm.photo_id
              JOIN workspace_folders wf ON wf.folder_id = p.folder_id
             WHERE wf.workspace_id = ?
             GROUP BY pm.variant
             ORDER BY pm.variant
            """,
            (ws,),
        ).fetchall()
        return [
            {"variant": r["variant"],
             "count": r["count"] or 0,
             "active_count": r["active_count"] or 0}
            for r in rows
        ]

    def sam_variant_rerun_warning(
        self,
        sam2_variant,
        min_conf,
        scope_sql,
        scope_params,
        selected_max_ratio=0.25,
        alternate_min_ratio=0.80,
    ):
        """Warn when selected SAM coverage is poor but another variant is high.

        ``min_conf`` is the detector floor and ``scope_sql`` /
        ``scope_params`` the photo-scope clause, both resolved by the façade.
        """
        ws = self.workspace_id
        target_row = self.conn.execute(
            f"""SELECT COUNT(DISTINCT p.id) AS n
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
        target_count = target_row["n"] or 0
        if target_count == 0:
            return None

        coverage_rows = self.conn.execute(
            f"""SELECT pm.variant, COUNT(DISTINCT pm.photo_id) AS count
                  FROM photo_masks pm
                  JOIN photos p ON p.id = pm.photo_id
                  JOIN workspace_folders wf
                    ON wf.folder_id = p.folder_id AND wf.workspace_id = ?
                  JOIN detections d
                    ON d.photo_id = p.id
                   AND d.detector_model != 'full-image'
                   AND d.detector_confidence >= ?
                 WHERE pm.variant != 'unknown'
                   AND pm.path IS NOT NULL
                   AND pm.path != ''
                   AND p.mask_path IS NOT NULL{scope_sql}
                 GROUP BY pm.variant""",
            (ws, min_conf, *scope_params),
        ).fetchall()
        counts = {r["variant"]: r["count"] or 0 for r in coverage_rows}
        selected_count = counts.get(sam2_variant, 0)
        selected_ratio = selected_count / target_count
        if selected_ratio > selected_max_ratio:
            return None

        alternates = [
            (variant, count, count / target_count)
            for variant, count in counts.items()
            if variant != sam2_variant
        ]
        if not alternates:
            return None
        alt_variant, alt_count, alt_ratio = max(
            alternates, key=lambda item: (item[2], item[1], item[0])
        )
        if alt_ratio < alternate_min_ratio:
            return None

        return {
            "code": "sam_variant_rerun",
            "selected_variant": sam2_variant,
            "selected_count": selected_count,
            "selected_ratio": selected_ratio,
            "alternate_variant": alt_variant,
            "alternate_count": alt_count,
            "alternate_ratio": alt_ratio,
            "target_count": target_count,
            "message": (
                f"{sam2_variant} has masks for {selected_count} of "
                f"{target_count} target photos, while {alt_variant} already "
                f"has masks for {alt_count}. Starting will rerun SAM for the "
                f"selected variant."
            ),
        }

    def variants_summary(self):
        """Per-variant summary: count, total bytes (best-effort, sums
        on-disk file sizes), and active_count.

        Returns: list of dicts ordered by variant name.
        """
        rows = self.conn.execute(
            """
            SELECT pm.variant,
                   COUNT(*) AS count,
                   SUM(CASE WHEN p.active_mask_variant = pm.variant
                            THEN 1 ELSE 0 END) AS active_count
              FROM photo_masks pm
              JOIN photos p ON p.id = pm.photo_id
             GROUP BY pm.variant
             ORDER BY pm.variant
            """
        ).fetchall()
        out = []
        for r in rows:
            paths = self.conn.execute(
                "SELECT path FROM photo_masks WHERE variant=?", (r["variant"],),
            ).fetchall()
            total = 0
            for pr in paths:
                try:
                    if pr["path"] and os.path.isfile(pr["path"]):
                        total += os.path.getsize(pr["path"])
                except OSError:
                    pass
            out.append({
                "variant": r["variant"],
                "count": r["count"],
                "active_count": r["active_count"],
                "bytes": total,
            })
        return out

    # -- writers -------------------------------------------------------------

    def upsert_mask(
        self, photo_id, variant, path,
        detector_model, prompt_x, prompt_y, prompt_w, prompt_h,
        subject_size=None, subject_tenengrad=None,
        bg_tenengrad=None, crop_complete=None, _commit=True,
        quality_input_recipe=None,
        subject_clip_high=None, subject_clip_low=None, subject_y_median=None,
        bg_separation=None, phash_crop=None, noise_estimate=None,
    ):
        """Insert or replace a mask row for (photo_id, variant)."""
        self.conn.execute(
            """
            INSERT INTO photo_masks (
                photo_id, variant, path, created_at,
                detector_model, prompt_x, prompt_y, prompt_w, prompt_h,
                subject_size, subject_tenengrad, bg_tenengrad, crop_complete, quality_input_recipe,
                subject_clip_high, subject_clip_low, subject_y_median, bg_separation, phash_crop, noise_estimate
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(photo_id, variant) DO UPDATE SET
                path=excluded.path,
                created_at=excluded.created_at,
                detector_model=excluded.detector_model,
                prompt_x=excluded.prompt_x,
                prompt_y=excluded.prompt_y,
                prompt_w=excluded.prompt_w,
                prompt_h=excluded.prompt_h,
                subject_size=excluded.subject_size,
                subject_tenengrad=excluded.subject_tenengrad,
                bg_tenengrad=excluded.bg_tenengrad,
                crop_complete=excluded.crop_complete,
                quality_input_recipe=excluded.quality_input_recipe,
                subject_clip_high=excluded.subject_clip_high,
                subject_clip_low=excluded.subject_clip_low,
                subject_y_median=excluded.subject_y_median,
                bg_separation=excluded.bg_separation,
                phash_crop=excluded.phash_crop,
                noise_estimate=excluded.noise_estimate
            """,
            (photo_id, variant, path, int(time.time()),
             detector_model, prompt_x, prompt_y, prompt_w, prompt_h,
             subject_size, subject_tenengrad, bg_tenengrad, crop_complete, quality_input_recipe,
             subject_clip_high, subject_clip_low, subject_y_median, bg_separation, phash_crop, noise_estimate),
        )
        if _commit:
            self.commit_with_retry(self.conn)

    def save_subject_raw_analysis(self, detection_id, report, _commit=True):
        """Keep original and corrected measurements together for each detection."""
        self.conn.execute(
            "INSERT INTO subject_raw_analysis(detection_id, recipe, report_json, created_at) "
            "VALUES (?, ?, ?, ?) ON CONFLICT(detection_id) DO UPDATE SET "
            "recipe=excluded.recipe, report_json=excluded.report_json, created_at=excluded.created_at",
            (detection_id, report["recipe"], json.dumps(report, allow_nan=False), int(time.time())),
        )
        if _commit:
            self.commit_with_retry(self.conn)

    def update_pipeline_features(
        self,
        photo_id,
        mask_path=UNSET,
        subject_tenengrad=UNSET,
        bg_tenengrad=UNSET,
        crop_complete=UNSET,
        bg_separation=UNSET,
        subject_clip_high=UNSET,
        subject_clip_low=UNSET,
        subject_y_median=UNSET,
        phash_crop=UNSET,
        noise_estimate=UNSET,
        eye_x=UNSET,
        eye_y=UNSET,
        eye_conf=UNSET,
        eye_tenengrad=UNSET,
        eye_kp_fingerprint=UNSET,
        quality_input_recipe=UNSET,
        _commit=True,
    ):
        """Update pipeline feature columns for a photo.

        Only updates columns whose values are explicitly provided (not UNSET).
        """
        cols = {
            "mask_path": mask_path,
            "subject_tenengrad": subject_tenengrad,
            "bg_tenengrad": bg_tenengrad,
            "crop_complete": crop_complete,
            "bg_separation": bg_separation,
            "subject_clip_high": subject_clip_high,
            "subject_clip_low": subject_clip_low,
            "subject_y_median": subject_y_median,
            "phash_crop": phash_crop,
            "noise_estimate": noise_estimate,
            "eye_x": eye_x,
            "eye_y": eye_y,
            "eye_conf": eye_conf,
            "eye_tenengrad": eye_tenengrad,
            "eye_kp_fingerprint": eye_kp_fingerprint,
            "quality_input_recipe": quality_input_recipe,
        }
        # Filter to only provided values
        updates = {k: v for k, v in cols.items() if v is not UNSET}
        if not updates:
            return
        set_clause = ", ".join(f"{k}=?" for k in updates)
        values = list(updates.values()) + [photo_id]
        self.conn.execute(
            f"UPDATE photos SET {set_clause} WHERE id=?", values
        )
        if _commit:
            self.commit_with_retry(self.conn)

    # -- stage selectors -----------------------------------------------------

    def photos_missing_masks(self, folder_ids, min_conf):
        """Photos in the bound workspace with detections but no mask yet.

        ``min_conf`` is the workspace detector floor, resolved by the façade.
        """
        from subjects import primary_order_sql
        ws_id = self.workspace_id
        if folder_ids:
            # Detections are global post-refactor, so folder filtering alone
            # leaks photos from folders that belong to other workspaces if
            # the caller happens to pass foreign folder ids. Explicitly
            # JOIN workspace_folders to keep this helper workspace-scoped.
            placeholders = ",".join("?" * len(folder_ids))
            rows = self.conn.execute(
                f"""SELECT p.id, p.folder_id, p.filename,
                           d.box_x, d.box_y, d.box_w, d.box_h,
                           d.detector_confidence
                    FROM photos p
                    JOIN workspace_folders wf
                      ON wf.folder_id = p.folder_id AND wf.workspace_id = ?
                    JOIN detections d ON d.photo_id = p.id
                    WHERE p.folder_id IN ({placeholders})
                      AND p.mask_path IS NULL
                      AND d.detector_confidence >= ?
                    ORDER BY p.id, {primary_order_sql("d")}""",
                [ws_id, *folder_ids, min_conf],
            ).fetchall()
        else:
            rows = self.conn.execute(
                f"""SELECT p.id, p.folder_id, p.filename,
                          d.box_x, d.box_y, d.box_w, d.box_h,
                          d.detector_confidence
                   FROM photos p
                   JOIN workspace_folders wf ON wf.folder_id = p.folder_id
                   JOIN detections d ON d.photo_id = p.id
                   WHERE wf.workspace_id = ?
                     AND p.mask_path IS NULL
                     AND d.detector_confidence >= ?
                   ORDER BY p.id, {primary_order_sql("d")}""",
                (ws_id, min_conf),
            ).fetchall()

        # Deduplicate to one row per photo (selected primary first)
        import json as _json
        seen = set()
        result = []
        for r in rows:
            if r["id"] in seen:
                continue
            seen.add(r["id"])
            result.append({
                "id": r["id"],
                "folder_id": r["folder_id"],
                "filename": r["filename"],
                "detection_box": _json.dumps({
                    "x": r["box_x"], "y": r["box_y"],
                    "w": r["box_w"], "h": r["box_h"],
                }),
                "detection_conf": r["detector_confidence"],
            })
        return result

    def list_photos_for_eye_keypoint_stage(
        self, min_conf, extra_where, scope_params, *, eye_kp_fingerprint_version,
    ):
        """Photos in the bound workspace eligible for the eye-keypoint stage.

        ``min_conf`` (detector floor), ``extra_where`` / ``scope_params``
        (photo scope) and ``eye_kp_fingerprint_version`` (the pipeline's
        current keypoint fingerprint) are resolved by the façade.
        """
        from subjects import primary_order_sql
        ws_id = self.workspace_id
        # Resolve the effective primary the same way mask extraction and
        # ``set_active_mask_variant`` do: the top-ordered non-full-image
        # detection above the workspace's current detector_confidence.
        # ``photo_subject_state.detection_id`` can lag when the floor
        # changes (workspace override, or a peer workspace sharing this
        # photo runs with a different floor), so joining against the
        # cached state would exclude every detection — the stored
        # subject fails the confidence join while the current above-
        # floor primary fails the state-ID check — and the photo would
        # never advance to the eye stage until analysis or selection
        # happened to refresh the cache.
        params = (
            ws_id, min_conf, min_conf, eye_kp_fingerprint_version,
            *scope_params,
        )
        # ``set_active_mask_variant`` refuses to activate a mask whose stored
        # prompt no longer matches the primary detection. The eye stage does
        # not extract masks — it consumes ``photos.mask_path`` directly — so
        # filter stale masks here too: the active mask row must have been
        # generated from the currently-selected primary (same detector_model
        # AND same prompt_x/y/w/h). Without this predicate, after a
        # ``detector_confidence`` change the eye stage would run keypoint
        # inference over a mask cropped from the previous primary and stamp
        # the fingerprint on a wrong-subject result. The full Process
        # pipeline regenerates stale masks first, so this only matters for
        # the standalone eye stage where mask extraction is skipped.
        rows = self.conn.execute(
            f"""SELECT p.id, p.folder_id, p.filename, p.width, p.height,
                      p.mask_path,
                      d.id AS detection_id, d.box_x, d.box_y, d.box_w, d.box_h,
                      d.detector_confidence,
                      pr.confidence AS species_conf,
                      pr.taxonomy_class,
                      pr.scientific_name,
                      pr.species
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
                 )
                 AND (p.eye_kp_fingerprint IS NULL
                      OR p.eye_kp_fingerprint != ?){extra_where}
                 AND pr.labels_fingerprint = (
                    SELECT pr2.labels_fingerprint FROM predictions pr2
                    WHERE pr2.detection_id = pr.detection_id
                      AND pr2.classifier_model = pr.classifier_model
                    ORDER BY pr2.created_at DESC, pr2.id DESC
                    LIMIT 1
                 )
               ORDER BY p.id,
                        CASE
                            WHEN pr.taxonomy_class IS NOT NULL
                              OR pr.scientific_name IS NOT NULL THEN 0
                            ELSE 1
                        END,
                        d.detector_confidence DESC,
                        pr.confidence DESC""",
            params,
        ).fetchall()

        seen = set()
        result = []
        for r in rows:
            if r["id"] in seen:
                continue
            seen.add(r["id"])
            result.append({
                "id": r["id"],
                "folder_id": r["folder_id"],
                "filename": r["filename"],
                "width": r["width"],
                "height": r["height"],
                "mask_path": r["mask_path"],
                "detection_id": r["detection_id"],
                "box_x": r["box_x"],
                "box_y": r["box_y"],
                "box_w": r["box_w"],
                "box_h": r["box_h"],
                "species_conf": r["species_conf"],
                "taxonomy_class": r["taxonomy_class"],
                "scientific_name": r["scientific_name"],
                "species": r["species"],
            })
        return result

    # -- embeddings ----------------------------------------------------------

    def update_embeddings(
        self, photo_id, dino_subject_embedding=None, dino_global_embedding=None,
        variant=None, _commit=True,
    ):
        """Store DINOv2 embedding BLOBs (and their variant) on the photo row."""
        self.conn.execute(
            "UPDATE photos SET dino_subject_embedding=?, dino_global_embedding=?, "
            "dino_embedding_variant=? WHERE id=?",
            (dino_subject_embedding, dino_global_embedding, variant, photo_id),
        )
        if _commit:
            self.commit_with_retry(self.conn)

    def get_embedding(self, photo_id, model, variant=''):
        """Return the embedding blob for (photo_id, model, variant), or None."""
        row = self.conn.execute(
            "SELECT embedding FROM photo_embeddings "
            "WHERE photo_id = ? AND model = ? AND variant = ?",
            (photo_id, model, variant),
        ).fetchone()
        return row["embedding"] if row else None

    def upsert_embedding(self, photo_id, model, embedding_bytes, variant=''):
        """Store an embedding blob for (photo_id, model, variant) and commit."""
        self.conn.execute(
            """INSERT INTO photo_embeddings (photo_id, model, variant, embedding)
               VALUES (?, ?, ?, ?)
               ON CONFLICT(photo_id, model, variant)
               DO UPDATE SET embedding = excluded.embedding,
                             created_at = datetime('now')""",
            (photo_id, model, variant, embedding_bytes),
        )
        self.conn.commit()

    def photos_with_embedding(
        self, model, variant='', photo_ids=None, include_offline_folders=False,
    ):
        """Return (photo_id, embedding_blob) pairs in the bound workspace
        with a stored embedding for ``(model, variant)``.

        Pass ``photo_ids`` to restrict the result to a subset.
        """
        ws = self.workspace_id
        folder_join = "JOIN folders f ON f.id = p.folder_id"
        if not include_offline_folders:
            folder_join += " AND f.status IN ('ok', 'partial')"
        sql = (
            "SELECT pe.photo_id, pe.embedding FROM photo_embeddings pe "
            "JOIN photos p ON p.id = pe.photo_id "
            f"{folder_join} "
            "JOIN workspace_folders wf "
            "  ON wf.folder_id = p.folder_id AND wf.workspace_id = ? "
            "WHERE pe.model = ? AND pe.variant = ?"
        )
        params = [ws, model, variant]
        if photo_ids is not None:
            if not photo_ids:
                return []
            # Chunk the id restriction: a broad universal-filter rule tree
            # passes every photo id in the library here, which would blow
            # past SQLITE_MAX_VARIABLE_NUMBER (999 on legacy builds) as a
            # single IN (?,...) clause and fail before scoring runs.
            results = []
            for start in range(0, len(photo_ids), 900):
                chunk = list(photo_ids)[start:start + 900]
                placeholders = ",".join("?" * len(chunk))
                rows = self.conn.execute(
                    sql + f" AND pe.photo_id IN ({placeholders})",
                    params + chunk,
                ).fetchall()
                results.extend(
                    (row["photo_id"], row["embedding"]) for row in rows
                )
            return results
        rows = self.conn.execute(sql, params).fetchall()
        return [(row["photo_id"], row["embedding"]) for row in rows]
