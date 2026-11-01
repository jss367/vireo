"""Persistence for detector output (``detections``) and the misses queue.

The ``detections`` table is global across workspaces: a detector's boxes are a
pure function of (photo, model), and confidence floors apply at read time.
The services keep everything workspace- or config-dependent:
``services/detection_store.py`` resolves the effective detector/classifier
floors, asks the model-runs domain whether a detector run is pinned, and
re-syncs the primary subject after id-based deletes; ``services/miss_flags.py``
resolves the active workspace id and builds the filter-bar scope clause. This
repository owns the SQL those steps read and write, plus the one read of a
detection's subject analysis (``detection_subjects.exposure_ev``) that the
subject-crop preview applies; ``subjects`` still owns that table's writes.

Callers reach the pass-through operations as ``db.detections`` (a fresh
repository per access, see ``Database.detections``): ``save``,
``upsert_rows``, ``clear``, ``get_ids_for_photos``,
``runtime_fingerprints_for_photos``, ``confidence_summary``,
``subject_exposure_ev`` and ``promote_full_image_runtime`` (the classify job's
and the detect stage's legacy full-image anchor promotion, SQL unchanged from
``classify_job``). The rest stay behind the service functions that add the
steps above (``write_detection_batch``, ``get_detections``,
``get_detections_for_photos``, ``get_predictions_for_detection`` and
``delete_detections_by_ids`` in ``detection_store``; the misses functions in
``miss_flags``).

``misses`` derives and stores miss flags through the accessor too:
``workspace_miss_rows``, ``miss_detection_rows`` and ``write_miss_flags``
(the caller resolves the workspace and commits).
"""

import sqlite3
from collections.abc import Callable, Collection, Iterable, Mapping, Sequence
from typing import Any


class DetectionsRepository:
    # Miss category -> ``photos`` flag column.
    MISS_COLUMNS = {
        "no_subject": "miss_no_subject",
        "clipped":    "miss_clipped",
        "oof":        "miss_oof",
    }

    def __init__(
        self,
        conn: sqlite3.Connection,
        *,
        chunk_size: int = 800,
        commit_with_retry: Callable[[sqlite3.Connection], None],
    ) -> None:
        self.conn = conn
        self.chunk_size = chunk_size
        # ``db.commit_with_retry``, passed in so repositories import no
        # ``db`` code and a monkeypatch of the module function still applies.
        self.commit_with_retry = commit_with_retry

    # -- writes -------------------------------------------------------------

    def save(
        self,
        photo_id: int,
        detections: Sequence[Mapping[str, Any]],
        detector_model: str | None,
        runtime_fingerprint: str = "legacy",
    ) -> list[int]:
        """Replace all detections for (photo_id, detector_model) with the given list.

        Global: no workspace scoping. The model's output is a pure function of
        (photo, model); any workspace re-running the same (photo, model) is a
        bug — callers should short-circuit via `get_detector_run_photo_ids`.

        IDs are content-addressed (see vireo.detection_id) so two pipelines
        writing the same (photo, model, detections) produce identical rows
        — the second writer's UPSERT is a no-op rather than a CASCADE-deleting
        DELETE+INSERT. Does not commit.

        Args:
            photo_id: the photo
            detections: list of dicts {box: {x,y,w,h}, confidence, category}
            detector_model: required, e.g. "megadetector-v6"
        Returns:
            list of detection IDs (empty if detections was empty).
        """
        if detector_model is None:
            raise ValueError("detector_model is required")
        ids = self.upsert_rows(
            photo_id, detector_model, detections, runtime_fingerprint,
        )
        return ids

    def upsert_rows(
        self,
        photo_id: int,
        detector_model: str,
        detections: Sequence[Mapping[str, Any]],
        runtime_fingerprint: str = "legacy",
    ) -> list[int]:
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
        photo_id: int,
        detector_model: str,
        detections: Sequence[Mapping[str, Any]],
        runtime_fingerprint: str,
        input_fingerprint: str | None,
        force_runtime_replace: bool,
        *,
        is_pinned: Callable[[int, str], bool],
    ) -> list[int]:
        """Replace detections and record the detector_runs row (no commit).

        ``is_pinned(photo_id, detector_model)`` is
        ``ModelRunsRepository.detector_run_is_pinned``; it is consulted only when the
        run's identity changed and the caller did not force the replace.
        See ``Database.write_detection_batch``.
        """
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
        # No authoritative run record but detection rows may still exist
        # (legacy state, or rows written via ``save_detections`` which
        # never touches ``detector_runs``). A forced replace means the
        # caller wants a full retirement, so treat this like an identity
        # change: without the DELETE, ``upsert_rows``' same-runtime sweep
        # would leave any other-runtime rows behind.
        no_run_replace = previous is None and force_runtime_replace
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

        if identity_changed or no_run_replace:
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
        return ids

    def clear(self, photo_id: int, detector_model: str | None = None) -> None:
        """Remove detections (and cascaded predictions) for a photo (no commit).

        Also clears the matching ``detector_runs`` rows so a subsequent
        non-reclassify pass actually re-runs MegaDetector. Without this,
        a reclassify that clears detections but leaves the run key behind
        (e.g. because model init then failed) would cause future runs to
        skip detection forever — the gate in ``_detect_subjects`` treats
        any ``detector_runs`` entry as authoritative.

        Global: no workspace scoping. If ``detector_model`` is None, all
        detector models for this photo are cleared; otherwise only the
        rows for that model.
        """
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

    def delete_by_ids(self, detection_ids: Iterable[int]) -> set[int]:
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

    def promote_full_image_runtime(
        self, photo_id: int, runtime_fingerprint: str,
    ) -> None:
        """Stamp a photo's full-image detection with ``runtime_fingerprint``, without committing.

        Rows already carrying it are left alone. The classify job promotes a
        legacy full-image anchor this way before recording the matching
        ``detector_runs`` row; the caller commits.
        """
        self.conn.execute(
            """UPDATE detections
                  SET runtime_fingerprint = ?
                WHERE photo_id = ?
                  AND detector_model = 'full-image'
                  AND (runtime_fingerprint IS NULL
                       OR runtime_fingerprint != ?)""",
            (runtime_fingerprint, photo_id, runtime_fingerprint),
        )

    # -- reads --------------------------------------------------------------

    def get(
        self, photo_id: int, min_conf: float, detector_model: str | None = None,
    ) -> list[sqlite3.Row]:
        """Return a photo's detections at or above ``min_conf``, primary first."""
        q = ("SELECT * FROM detections WHERE photo_id = ? "
             "AND detector_confidence >= ?")
        params = [photo_id, min_conf]
        if detector_model is not None:
            q += " AND detector_model = ?"
            params.append(detector_model)
        # The same ordering drives masks, crop previews, and bulk payloads:
        # manual choice, subject quality, then confidence and stable ID.
        from repositories.subject_order import primary_order_sql
        q += " ORDER BY " + primary_order_sql()
        return self.conn.execute(q, params).fetchall()

    def get_for_photos(
        self, photo_ids: Iterable[int], min_conf: float, detector_model: str | None = None,
    ) -> dict[int, list[dict[str, Any]]]:
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
            from repositories.subject_order import primary_order_sql
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

    def get_predictions(
        self,
        detection_id: int,
        min_classifier_conf: float,
        classifier_model: str | None = None,
        labels_fingerprint: str | None = None,
    ) -> list[sqlite3.Row]:
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

    def get_ids_for_photos(self, photo_ids: Collection[int]) -> dict[int, set[int]]:
        """Return ``{photo_id: set(detection_id, ...)}`` for the given photo IDs.

        The detections table is global (no workspace_id). Used to snapshot
        pre-run detection IDs so that a reclassify pass can delete only the
        *stale* rows after fresh ones have been inserted, avoiding the
        cascade-delete that would destroy other-model predictions.

        No threshold filter: the caller needs to see every existing row,
        including low-confidence ones, so they can all be cleaned up.

        IDs are queried in chunks of at most 900 to stay safely under
        SQLite's default bound-parameter limit (SQLITE_LIMIT_VARIABLE_NUMBER,
        typically 999 in production builds).
        """
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

    def runtime_fingerprints_for_photos(self, photo_ids: Sequence[int]) -> set[str | None]:
        """The distinct ``runtime_fingerprint`` values (None included) of the
        detections on ``photo_ids``. One query; the caller chunks the ids."""
        placeholders = ",".join("?" * len(photo_ids))
        rows = self.conn.execute(
            f"SELECT DISTINCT runtime_fingerprint "
            f"FROM detections "
            f"WHERE photo_id IN ({placeholders})",
            list(photo_ids),
        ).fetchall()
        return {row["runtime_fingerprint"] for row in rows}

    def confidence_summary(self, photo_ids: Iterable[int]) -> list[sqlite3.Row]:
        """Rows (``photo_id``, ``max_conf``, ``n``) for the photos with detections.

        ``max_conf`` is the highest ``detector_confidence`` and ``n`` the
        number of detection rows, over every model; photos without a
        detection have no row. One statement per chunk.
        """
        rows = []
        for chunk in self._chunks(photo_ids):
            placeholders = ",".join("?" for _ in chunk)
            rows.extend(self.conn.execute(
                f"""SELECT photo_id,
                           MAX(detector_confidence) AS max_conf,
                           COUNT(*) AS n
                    FROM detections WHERE photo_id IN ({placeholders})
                    GROUP BY photo_id""",
                chunk,
            ).fetchall())
        return rows

    def subject_exposure_ev(self, detection_id: int) -> float | None:
        """The subject analysis's ``exposure_ev`` for one detection, or None if unanalysed."""
        row = self.conn.execute(
            "SELECT exposure_ev FROM detection_subjects WHERE detection_id=?",
            (detection_id,),
        ).fetchone()
        return row["exposure_ev"] if row else None

    # -- misses -------------------------------------------------------------

    def miss_column(self, category: str) -> str:
        """Return the ``photos`` column for a miss category; ``KeyError`` if unknown."""
        return self.MISS_COLUMNS[category]

    def miss_where(self, category: str | None) -> str:
        """Return the flag predicate for ``category`` (``None`` = any miss)."""
        if category is None:
            where = (
                "p.miss_no_subject=1 OR p.miss_clipped=1 OR p.miss_oof=1"
            )
        else:
            col = self.miss_column(category)
            where = f"p.{col}=1"
        return where

    def list_miss_photos(
        self,
        workspace_id: int,
        where: str,
        since: str | None,
        scope_clause: str,
        scope_params: Sequence[Any],
    ) -> list[dict[str, Any]]:
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
            f"JOIN photo_workspace_visibility wf ON wf.photo_id = p.id "
            f"WHERE wf.workspace_id = ? "
            f"  AND ({where}) "
            f"  AND (p.flag IS NULL OR p.flag != 'rejected') "
            f"  {scope_clause} "
            f"ORDER BY p.timestamp DESC",
            params,
        ).fetchall()
        return [dict(r) for r in rows]

    def workspace_miss_rows(
        self, workspace_id: int, detector_confidence: float,
    ) -> list[sqlite3.Row]:
        """Every photo ``workspace_id`` can see, with the inputs a miss
        derivation reads: the stored miss flags and quality signals, the best
        animal detection at or above ``detector_confidence``
        (``detection_conf``) and the best current-label-set prediction (``max_prediction_conf``).
        The classifier override deliberately includes below-floor boxes and
        full-image predictions so strong species evidence can rescue a miss.
        """
        return self.conn.execute(
            "SELECT p.id, p.folder_id, p.filename, p.companion_path, "
            "       p.timestamp, p.burst_id, "
            "       p.subject_size, p.crop_complete, "
            "       p.subject_tenengrad, p.bg_tenengrad, "
            "       p.miss_no_subject, p.miss_clipped, p.miss_oof, "
            "       p.miss_computed_at, p.flag, "
            "       (SELECT MAX(d.detector_confidence) FROM detections d "
            "        WHERE d.photo_id = p.id "
            "          AND (d.detector_model IS NULL "
            "               OR d.detector_model != 'full-image') "
            "          AND COALESCE(d.category, 'animal') = 'animal' "
            "          AND d.detector_confidence >= ?) "
            "         AS detection_conf, "
            "       (SELECT MAX(pr.confidence) FROM predictions pr "
            "        JOIN detections d2 ON d2.id = pr.detection_id "
            "        WHERE d2.photo_id = p.id "
            "          AND pr.labels_fingerprint = ("
            "              SELECT pr2.labels_fingerprint FROM predictions pr2 "
            "              WHERE pr2.detection_id = pr.detection_id "
            "                AND pr2.classifier_model = pr.classifier_model "
            "              ORDER BY pr2.created_at DESC, pr2.id DESC LIMIT 1)) "
            "         AS max_prediction_conf "
            "FROM photos p "
            "JOIN photo_workspace_visibility wf ON wf.photo_id = p.id "
            "WHERE wf.workspace_id = ?",
            (detector_confidence, workspace_id),
        ).fetchall()

    def miss_detection_rows(self, photo_ids: Sequence[int]) -> list[sqlite3.Row]:
        """The animal, non-full-image detections of ``photo_ids`` (one chunk),
        each photo's rows primary first: ``photo_id``, the box and
        ``detector_confidence``. No confidence floor; callers apply one."""
        from repositories.subject_order import primary_order_sql

        placeholders = ",".join("?" * len(photo_ids))
        return self.conn.execute(
            f"SELECT photo_id, box_x, box_y, box_w, box_h, "
            f"       detector_confidence "
            f"FROM detections "
            f"WHERE photo_id IN ({placeholders}) "
            f"  AND (detector_model IS NULL OR detector_model != 'full-image') "
            f"  AND COALESCE(category, 'animal') = 'animal' "
            f"ORDER BY photo_id, {primary_order_sql()}",
            photo_ids,
        ).fetchall()

    def write_miss_flags(
        self, updates: Iterable[tuple[int, int, int, str, int]],
    ) -> None:
        """Store ``(miss_no_subject, miss_clipped, miss_oof, miss_computed_at,
        photo_id)`` for each photo, in the caller's transaction."""
        self.conn.executemany(
            "UPDATE photos SET miss_no_subject=?, miss_clipped=?, miss_oof=?, "
            "miss_computed_at=? WHERE id=?",
            updates,
        )

    def attach_miss_detections(
        self, photos: list[dict[str, Any]], min_conf: float,
    ) -> list[dict[str, Any]]:
        """Add each miss photo's primary and raw-best animal detection fields."""
        import json as _json

        photo_ids = [p["id"] for p in photos]
        # Chunk to stay under SQLite's SQLITE_MAX_VARIABLE_NUMBER (default 999).
        # A workspace with thousands of flagged misses would otherwise raise
        # ``OperationalError: too many SQL variables``.
        CHUNK = 500
        primary = {}
        raw_primary = {}
        for i in range(0, len(photo_ids), CHUNK):
            chunk = photo_ids[i:i + CHUNK]
            det_rows = self.miss_detection_rows(chunk)
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

    def clear_miss_flag(self, photo_id: int, category: str) -> None:
        """Zero one miss column on a photo (no commit)."""
        col = self.miss_column(category)
        self.conn.execute(
            f"UPDATE photos SET {col}=0 WHERE id=?", (photo_id,)
        )

    def select_misses_to_reject(
        self,
        col: str,
        workspace_id: int,
        since: str | None,
        scope_clause: str,
        scope_params: Sequence[Any],
    ) -> list[dict[str, Any]]:
        """Select scoped non-rejected misses without changing flags or attribution."""
        params = [workspace_id]
        since_clause = ""
        if since:
            since_clause = "    AND p.miss_computed_at >= ? "
            params.append(since)
        params.extend(scope_params)
        rows = self.conn.execute(
            f"SELECT p.id, p.flag FROM photos p "
            f"JOIN photo_workspace_visibility wf ON wf.photo_id = p.id "
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
        return [
            {"photo_id": r["id"], "old_value": r["flag"]}
            for r in rows
        ]

    def _chunks(self, values):
        values = list(values)
        return (
            values[index:index + self.chunk_size]
            for index in range(0, len(values), self.chunk_size)
        )
