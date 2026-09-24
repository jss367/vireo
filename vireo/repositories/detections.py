"""Persistence for detector output (``detections``) and the misses queue.

The ``detections`` table is global across workspaces: a detector's boxes are a
pure function of (photo, model), and confidence floors apply at read time.
``Database`` keeps everything workspace- or config-dependent: it resolves the
active workspace id and the effective detector/classifier floors, builds the
filter-bar scope clause, asks the model-runs domain whether a detector run is
pinned, and re-syncs the primary subject after id-based deletes. This
repository owns the SQL those steps read and write.
"""


class DetectionsRepository:
    # Miss category -> ``photos`` flag column.
    MISS_COLUMNS = {
        "no_subject": "miss_no_subject",
        "clipped":    "miss_clipped",
        "oof":        "miss_oof",
    }

    def __init__(self, conn, *, chunk_size=800, commit_with_retry):
        self.conn = conn
        self.chunk_size = chunk_size
        # ``db.commit_with_retry``, passed in so repositories import no
        # ``db`` code and a monkeypatch of the module function still applies.
        self.commit_with_retry = commit_with_retry

    def commit(self):
        self.conn.commit()

    # -- writes -------------------------------------------------------------

    def save(self, photo_id, detections, detector_model, runtime_fingerprint):
        """Upsert one (photo, model)'s detections and commit; see ``Database.save_detections``."""
        ids = self.upsert_rows(
            photo_id, detector_model, detections, runtime_fingerprint,
        )
        self.commit_with_retry(self.conn)
        return ids

    def upsert_rows(
        self,
        photo_id,
        detector_model,
        detections,
        runtime_fingerprint="legacy",
    ):
        """Content-addressed UPSERT of detection rows for one (photo, model).

        Returns the list of unique IDs in first-seen order. Does NOT commit —
        the caller controls the transaction so the detector_runs row can be
        written in the same commit (see `write_batch`).
        """
        from detection_id import detection_id as _detection_id

        unique = {}
        ordered_ids = []
        for idx, det in enumerate(detections):
            box = det["box"]
            category = det.get("category", "animal")
            det_id = _detection_id(
                photo_id, detector_model,
                (box["x"], box["y"], box["w"], box["h"]),
                category,
            )
            if det_id not in unique:
                ordered_ids.append(det_id)
                unique[det_id] = (det, category, idx)
                continue
            prev_det, _prev_category, prev_idx = unique[det_id]
            if (
                det["confidence"] > prev_det["confidence"]
                or (
                    det["confidence"] == prev_det["confidence"]
                    and idx > prev_idx
                )
            ):
                unique[det_id] = (det, category, idx)

        ids = []
        for det_id in ordered_ids:
            det, category, _idx = unique[det_id]
            box = det["box"]
            # INSERT ON CONFLICT DO UPDATE — true UPSERT. Do NOT use
            # `INSERT OR REPLACE`, which DELETEs the conflicting row before
            # re-inserting; that DELETE fires `predictions.detection_id`
            # `ON DELETE CASCADE` and silently wipes any predictions another
            # pipeline has already written for this detection.
            self.conn.execute(
                """INSERT INTO detections
                     (id, photo_id, detector_model, runtime_fingerprint,
                      box_x, box_y, box_w, box_h, detector_confidence, category)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(id) DO UPDATE SET
                     photo_id = excluded.photo_id,
                     detector_model = excluded.detector_model,
                     runtime_fingerprint = excluded.runtime_fingerprint,
                     box_x = excluded.box_x,
                     box_y = excluded.box_y,
                     box_w = excluded.box_w,
                     box_h = excluded.box_h,
                     detector_confidence = excluded.detector_confidence,
                     category = excluded.category""",
                (det_id, photo_id, detector_model, runtime_fingerprint,
                 box["x"], box["y"], box["w"], box["h"],
                 det["confidence"], category),
            )
            ids.append(det_id)

        # Retire rows the new run no longer produces. Narrow DELETE: only
        # rows whose ID is NOT in the new set. Safe under concurrent writers
        # because two writers with the same detections compute the same
        # `new_ids` set, so neither deletes the other's rows.
        new_ids = set(ids)
        existing = [r["id"] for r in self.conn.execute(
            """SELECT id FROM detections
               WHERE photo_id = ? AND detector_model = ?
                 AND runtime_fingerprint = ?""",
            (photo_id, detector_model, runtime_fingerprint),
        ).fetchall()]
        stale = [eid for eid in existing if eid not in new_ids]
        # Chunk to stay under SQLite's compile-time SQLITE_MAX_VARIABLE_NUMBER
        # (defaults to 999 in older builds, 32766 in newer). A single photo
        # rarely has >1k detections today, but the chunking is cheap insurance
        # against future detectors that produce many small boxes.
        CHUNK = 500
        for i in range(0, len(stale), CHUNK):
            chunk = stale[i:i + CHUNK]
            placeholders = ",".join("?" for _ in chunk)
            self.conn.execute(
                f"DELETE FROM detections WHERE id IN ({placeholders})",
                chunk,
            )
        return ids

    def write_batch(
        self,
        photo_id,
        detector_model,
        detections,
        runtime_fingerprint,
        input_fingerprint,
        force_runtime_replace,
        *,
        is_pinned,
    ):
        """Replace detections and record the detector_runs row in one commit.

        ``is_pinned(photo_id, detector_model)`` is the façade's
        ``Database.detector_run_is_pinned``; it is consulted only when the
        run's identity changed and the caller did not force the replace.
        See ``Database.write_detection_batch``.
        """
        try:
            previous = self.conn.execute(
                """SELECT runtime_fingerprint, input_fingerprint
                   FROM detector_runs
                   WHERE photo_id = ? AND detector_model = ?""",
                (photo_id, detector_model),
            ).fetchone()
            identity_changed = previous is not None and (
                previous["runtime_fingerprint"] != runtime_fingerprint
                or (
                    previous["input_fingerprint"] is not None
                    and input_fingerprint is not None
                    and previous["input_fingerprint"] != input_fingerprint
                )
            )
            if (
                identity_changed
                and not force_runtime_replace
                and is_pinned(photo_id, detector_model)
            ):
                rows = self.conn.execute(
                    """SELECT id FROM detections
                       WHERE photo_id = ? AND detector_model = ?
                       ORDER BY detector_confidence DESC, id ASC""",
                    (photo_id, detector_model),
                ).fetchall()
                return [row["id"] for row in rows]

            if identity_changed:
                # Runtime ownership changes are an explicit retirement event.
                # Deleting first prevents old-runtime rows from surviving the
                # new run's same-runtime stale-row sweep.
                self.conn.execute(
                    """DELETE FROM detections
                       WHERE photo_id = ? AND detector_model = ?""",
                    (photo_id, detector_model),
                )

            ids = self.upsert_rows(
                photo_id, detector_model, detections, runtime_fingerprint,
            )
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
                 input_fingerprint, len(ids)),
            )
            self.commit_with_retry(self.conn)
            return ids
        except Exception:
            self.conn.rollback()
            raise

    def clear(self, photo_id, detector_model=None):
        """Delete a photo's detections and detector_runs rows (one model or all) and commit."""
        if detector_model is None:
            self.conn.execute(
                "DELETE FROM detections WHERE photo_id = ?", (photo_id,)
            )
            self.conn.execute(
                "DELETE FROM detector_runs WHERE photo_id = ?", (photo_id,)
            )
        else:
            self.conn.execute(
                "DELETE FROM detections WHERE photo_id = ? AND detector_model = ?",
                (photo_id, detector_model),
            )
            self.conn.execute(
                "DELETE FROM detector_runs WHERE photo_id = ? AND detector_model = ?",
                (photo_id, detector_model),
            )
        self.conn.commit()

    def delete_by_ids(self, detection_ids):
        """Delete detection rows by id in chunks of 900, without committing.

        Returns the photo ids that had a ``photo_subject_state`` row before
        the delete, so the caller can re-sync their primary subject.
        """
        ids = list(detection_ids)
        affected_subject_photos = set()
        _CHUNK = 900
        for i in range(0, len(ids), _CHUNK):
            chunk = ids[i : i + _CHUNK]
            placeholders = ",".join("?" * len(chunk))
            affected_subject_photos.update(row[0] for row in self.conn.execute(
                f"SELECT DISTINCT d.photo_id FROM detections d "
                f"JOIN photo_subject_state s ON s.photo_id=d.photo_id "
                f"WHERE d.id IN ({placeholders})", chunk,
            ))
            self.conn.execute(
                f"DELETE FROM detections WHERE id IN ({placeholders})",
                chunk,
            )
        return affected_subject_photos

    # -- reads --------------------------------------------------------------

    def get(self, photo_id, min_conf, detector_model=None):
        """Return a photo's detections at or above ``min_conf``, primary first."""
        q = ("SELECT * FROM detections WHERE photo_id = ? "
             "AND detector_confidence >= ?")
        params = [photo_id, min_conf]
        if detector_model is not None:
            q += " AND detector_model = ?"
            params.append(detector_model)
        # The same ordering drives masks, crop previews, and bulk payloads:
        # manual choice, subject quality, then confidence and stable ID.
        from subjects import primary_order_sql
        q += " ORDER BY " + primary_order_sql()
        return self.conn.execute(q, params).fetchall()

    def get_for_photos(self, photo_ids, min_conf, detector_model=None):
        """Return ``{photo_id: [det_dict, ...]}``; see ``Database.get_detections_for_photos``."""
        # Dedup-preserving-order: same id appearing in two chunks would
        # cause setdefault(...).append(...) below to emit each row twice.
        photo_ids = list(dict.fromkeys(photo_ids))
        result = {}
        for chunk in self._chunks(photo_ids):
            placeholders = ",".join("?" for _ in chunk)
            q = (
                f"SELECT id, photo_id, box_x, box_y, box_w, box_h, "
                f"       detector_confidence, category, detector_model "
                f"FROM detections "
                f"WHERE photo_id IN ({placeholders}) "
                f"  AND detector_confidence >= ?"
            )
            params = [*chunk, min_conf]
            if detector_model is not None:
                q += " AND detector_model = ?"
                params.append(detector_model)
            from subjects import primary_order_sql
            q += " ORDER BY photo_id, " + primary_order_sql()
            rows = self.conn.execute(q, params).fetchall()
            for r in rows:
                result.setdefault(r["photo_id"], []).append({
                    "id": r["id"],
                    "x": r["box_x"],
                    "y": r["box_y"],
                    "w": r["box_w"],
                    "h": r["box_h"],
                    "confidence": r["detector_confidence"],
                    "category": r["category"],
                    "detector_model": r["detector_model"],
                })
        return result

    def get_predictions(self, detection_id, min_classifier_conf,
                        classifier_model=None, labels_fingerprint=None):
        """Return a detection's cached predictions at or above the floor, best first."""
        q = ("SELECT * FROM predictions WHERE detection_id = ? "
             "AND confidence >= ?")
        params = [detection_id, min_classifier_conf]
        if classifier_model is not None:
            q += " AND classifier_model = ?"
            params.append(classifier_model)
        if labels_fingerprint is not None:
            q += " AND labels_fingerprint = ?"
            params.append(labels_fingerprint)
        q += " ORDER BY confidence DESC"
        return self.conn.execute(q, params).fetchall()

    def get_ids_for_photos(self, photo_ids):
        """Return ``{photo_id: set(detection_id, ...)}`` with no threshold, in chunks of 900."""
        if not photo_ids:
            return {}
        result: dict = {}
        ids = list(photo_ids)
        _CHUNK = 900
        for i in range(0, len(ids), _CHUNK):
            chunk = ids[i : i + _CHUNK]
            placeholders = ",".join("?" * len(chunk))
            rows = self.conn.execute(
                f"SELECT id, photo_id FROM detections "
                f"WHERE photo_id IN ({placeholders})",
                tuple(chunk),
            ).fetchall()
            for row in rows:
                result.setdefault(row["photo_id"], set()).add(row["id"])
        return result

    # -- misses -------------------------------------------------------------

    def miss_column(self, category):
        """Return the ``photos`` column for a miss category; ``KeyError`` if unknown."""
        return self.MISS_COLUMNS[category]

    def miss_where(self, category):
        """Return the flag predicate for ``category`` (``None`` = any miss)."""
        if category is None:
            where = (
                "p.miss_no_subject=1 OR p.miss_clipped=1 OR p.miss_oof=1"
            )
        else:
            col = self.miss_column(category)
            where = f"p.{col}=1"
        return where

    def list_miss_photos(self, workspace_id, where, since, scope_clause,
                         scope_params):
        """Return the workspace's non-rejected photos matching ``where``, newest first."""
        params = [workspace_id]
        if since:
            where = f"({where}) AND p.miss_computed_at >= ?"
            params.append(since)
        params.extend(scope_params)

        rows = self.conn.execute(
            f"SELECT p.id, p.folder_id, p.filename, p.companion_path, "
            f"       p.timestamp, p.burst_id, "
            f"       p.subject_size, p.crop_complete, "
            f"       p.subject_tenengrad, p.bg_tenengrad, "
            f"       p.miss_no_subject, p.miss_clipped, p.miss_oof, "
            f"       p.miss_computed_at, p.flag "
            f"FROM photos p "
            f"JOIN workspace_folders wf ON wf.folder_id = p.folder_id "
            f"WHERE wf.workspace_id = ? "
            f"  AND ({where}) "
            f"  AND (p.flag IS NULL OR p.flag != 'rejected') "
            f"  {scope_clause} "
            f"ORDER BY p.timestamp DESC",
            params,
        ).fetchall()
        return [dict(r) for r in rows]

    def attach_miss_detections(self, photos, min_conf):
        """Add each miss photo's primary and raw-best animal detection fields."""
        import json as _json

        from subjects import primary_order_sql
        photo_ids = [p["id"] for p in photos]
        # Chunk to stay under SQLite's SQLITE_MAX_VARIABLE_NUMBER (default 999).
        # A workspace with thousands of flagged misses would otherwise raise
        # ``OperationalError: too many SQL variables``.
        CHUNK = 500
        primary = {}
        raw_primary = {}
        for i in range(0, len(photo_ids), CHUNK):
            chunk = photo_ids[i:i + CHUNK]
            placeholders = ",".join("?" * len(chunk))
            det_rows = self.conn.execute(
                f"SELECT photo_id, box_x, box_y, box_w, box_h, "
                f"       detector_confidence "
                f"FROM detections "
                f"WHERE photo_id IN ({placeholders}) "
                f"  AND (detector_model IS NULL OR detector_model != 'full-image') "
                f"  AND COALESCE(category, 'animal') = 'animal' "
                f"ORDER BY photo_id, {primary_order_sql()}",
                chunk,
            ).fetchall()
            for d in det_rows:
                previous = raw_primary.get(d["photo_id"])
                if previous is None or d["detector_confidence"] > previous["detector_confidence"]:
                    raw_primary[d["photo_id"]] = d
                if d["detector_confidence"] >= min_conf:
                    primary.setdefault(d["photo_id"], d)
        for p in photos:
            d = primary.get(p["id"])
            raw = raw_primary.get(p["id"])
            p["raw_detection_conf"] = (
                raw["detector_confidence"] if raw is not None else None
            )
            p["detector_confidence_threshold"] = min_conf
            if d is not None:
                p["detection_box"] = _json.dumps({
                    "x": d["box_x"], "y": d["box_y"],
                    "w": d["box_w"], "h": d["box_h"],
                })
                p["detection_conf"] = d["detector_confidence"]
            else:
                p["detection_box"] = None
                p["detection_conf"] = None
        return photos

    def clear_miss_flag(self, photo_id, category):
        """Zero one miss column on a photo and commit."""
        col = self.miss_column(category)
        self.conn.execute(
            f"UPDATE photos SET {col}=0 WHERE id=?", (photo_id,)
        )
        self.conn.commit()

    def reject_misses(self, col, workspace_id, since, scope_clause,
                      scope_params):
        """Reject the workspace's non-rejected photos flagged in ``col``.

        Returns ``[{"photo_id", "old_value"}]`` for the changed photos and
        commits only when there were any.
        """
        params = [workspace_id]
        since_clause = ""
        if since:
            since_clause = "    AND p.miss_computed_at >= ? "
            params.append(since)
        params.extend(scope_params)
        rows = self.conn.execute(
            f"SELECT p.id, p.flag FROM photos p "
            f"JOIN workspace_folders wf ON wf.folder_id = p.folder_id "
            f"WHERE wf.workspace_id = ? "
            f"  AND p.{col}=1 "
            f"  AND (p.flag IS NULL OR p.flag != 'rejected') "
            f"{since_clause}"
            f"{scope_clause}",
            params,
        ).fetchall()
        # Preserve NULL flag values in old_value so undo is lossless.
        # Coercing NULL to "" would make _apply_undo restore an empty
        # string instead of the original NULL, leaving rows in a
        # non-canonical state that bypasses code paths expecting
        # none/flagged/rejected (or NULL).
        affected = [
            {"photo_id": r["id"], "old_value": r["flag"]}
            for r in rows
        ]
        if not affected:
            return []
        ids = [a["photo_id"] for a in affected]
        # Chunk to stay under SQLite's SQLITE_MAX_VARIABLE_NUMBER (default 999).
        _CHUNK = 500
        for i in range(0, len(ids), _CHUNK):
            chunk = ids[i:i + _CHUNK]
            placeholders = ",".join("?" * len(chunk))
            self.conn.execute(
                f"UPDATE photos SET flag='rejected' WHERE id IN ({placeholders})",
                chunk,
            )
        self.conn.commit()
        return affected

    def _chunks(self, values):
        values = list(values)
        return (
            values[index:index + self.chunk_size]
            for index in range(0, len(values), self.chunk_size)
        )
