"""Persistence for candidate subjects: person/vehicle regions the classifier examined.

Owns ``subject_candidates``. MegaDetector labels some animals "person" or
"vehicle" (a Wood Duck drake, a Mandarin Duck, a resting goose). On a photo
with no retained animal region, the strongest retained person/vehicle region
is a candidate: the classify stage asks the classifier about it once per
(classifier model, label set) and records the answer here. A region the
classifier recognizes is kept as a subject by setting its
``detections.category`` to ``animal``; the row here is the record that the
detector said otherwise (``detector_category``) and of the evidence.

Catalog-wide, like ``detections``: the answer is about the photo's pixels,
not the workspace that asked. Deleting the detection cascades.

What stays elsewhere: the rule that turns the classifier's scores into
"recognized" (``background_match.is_recognized_animal``), the pass that reads
the crops (``classify_job.candidate_subjects``), and the detection writer's
duty to keep a kept region's category across a re-detect
(``DetectionsRepository.upsert_rows``).

No method here commits: the caller owns the transaction.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterable, Mapping
from typing import Any

from sql_chunks import chunked

DETECTOR_MODEL = "megadetector-v6"

# Shared by ``pending`` and ``count``: the strongest retained non-animal
# MegaDetector region of each photo that has no retained animal region.
# Mirrors the classify stage's "confident non-animal, no animal target" gate
# (``ModelRunsRepository._db_unclassifiable``), so a candidate is exactly a
# photo that gate would leave unclassified.
_TOP_CANDIDATE_SQL = """
    SELECT d.id, d.photo_id, d.box_x, d.box_y, d.box_w, d.box_h,
           d.detector_confidence, d.category, d.detector_model,
           ROW_NUMBER() OVER (
               PARTITION BY d.photo_id
               ORDER BY d.detector_confidence DESC, d.id ASC
           ) AS rn
      FROM detections d
     WHERE d.detector_model = 'megadetector-v6'
       AND d.category != 'animal'
       AND d.detector_confidence >= ? AND d.subject_verdict IS NULL
       AND d.photo_id IN ({placeholders})
       AND NOT EXISTS (
             SELECT 1 FROM detections a
              WHERE a.photo_id = d.photo_id
                AND a.detector_model != 'full-image'
                AND a.category = 'animal'
                AND a.detector_confidence >= ? AND a.subject_verdict IS NULL
           )
"""


class CandidateSubjectRepository:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    def pending(
        self, photo_ids: Iterable[int], min_conf: float,
        classifier_model: str, labels_fingerprint: str, *, recheck: bool = False,
    ) -> list[sqlite3.Row]:
        """Candidate regions not yet examined under this model and label set.

        One row per photo (``id`` is the detection id), strongest region
        first by ``detector_confidence DESC, id ASC``. ``recheck`` returns
        every candidate, examined or not, for a run that re-infers on purpose.
        """
        rows: list[sqlite3.Row] = []
        for chunk in chunked(sorted({int(pid) for pid in photo_ids})):
            placeholders = ",".join("?" * len(chunk))
            examined = "" if recheck else (
                "AND NOT EXISTS (SELECT 1 FROM subject_candidates sc "
                "WHERE sc.detection_id = c.id AND sc.classifier_model = ? "
                "AND sc.labels_fingerprint = ?)"
            )
            params: list[Any] = [min_conf, *chunk, min_conf]
            if not recheck:
                params += [classifier_model, labels_fingerprint]
            rows.extend(self.conn.execute(
                f"""WITH c AS ({_TOP_CANDIDATE_SQL.format(placeholders=placeholders)})
                    SELECT c.* FROM c WHERE c.rn = 1 {examined}
                    ORDER BY c.photo_id""",
                params,
            ).fetchall())
        return rows

    def count(
        self, photo_ids: Iterable[int], min_conf: float,
        classifier_model: str | None = None, labels_fingerprint: str | None = None,
    ) -> int:
        """Count candidate regions; with a model and label set, only unexamined ones."""
        if classifier_model is not None and labels_fingerprint is not None:
            return len(self.pending(
                photo_ids, min_conf, classifier_model, labels_fingerprint,
            ))
        return len(self.pending(photo_ids, min_conf, "", "", recheck=True))

    def record(self, detection_id: int, evidence: Mapping[str, Any], *, promoted: bool) -> None:
        """Record the classifier's answer for one candidate region.

        ``evidence`` carries ``detector_category``, ``classifier_model``,
        ``labels_fingerprint`` and the match fields of
        ``classify_job.classify_pass._match_stats``. A promoted region's
        ``detections.category`` becomes ``animal``; a region already kept
        stays kept when a later check under another label set disagrees, so
        its subject analysis and species decisions are not withdrawn by a
        label change.
        """
        self.conn.execute(
            """INSERT INTO subject_candidates
                 (detection_id, detector_category, classifier_model,
                  labels_fingerprint, top_species, max_match_score,
                  background_score, background_prompt, promoted)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(detection_id) DO UPDATE SET
                 classifier_model = excluded.classifier_model,
                 labels_fingerprint = excluded.labels_fingerprint,
                 top_species = excluded.top_species,
                 max_match_score = excluded.max_match_score,
                 background_score = excluded.background_score,
                 background_prompt = excluded.background_prompt,
                 promoted = MAX(subject_candidates.promoted, excluded.promoted),
                 checked_at = datetime('now')""",
            (
                detection_id, evidence["detector_category"],
                evidence["classifier_model"], evidence["labels_fingerprint"],
                evidence.get("top_species"), evidence.get("max_match_score"),
                evidence.get("background_score"), evidence.get("background_prompt"),
                1 if promoted else 0,
            ),
        )
        if promoted:
            self.conn.execute(
                "UPDATE detections SET category = 'animal' WHERE id = ?",
                (detection_id,),
            )

    def kept_regions(self, photo_ids: Iterable[int]) -> dict[int, list[sqlite3.Row]]:
        """Kept candidate regions of ``photo_ids``, by photo.

        Each row is the detection (``id``, box, ``detector_confidence``,
        ``category``, ``detector_model``) plus ``detector_category``.
        """
        kept: dict[int, list[sqlite3.Row]] = {}
        for chunk in chunked(sorted({int(pid) for pid in photo_ids})):
            placeholders = ",".join("?" * len(chunk))
            for row in self.conn.execute(
                f"""SELECT d.id, d.photo_id, d.box_x, d.box_y, d.box_w, d.box_h,
                           d.detector_confidence, d.category, d.detector_model,
                           sc.detector_category
                      FROM subject_candidates sc
                      JOIN detections d ON d.id = sc.detection_id
                     WHERE sc.promoted = 1 AND d.photo_id IN ({placeholders})
                     ORDER BY d.photo_id, d.detector_confidence DESC, d.id""",
                chunk,
            ):
                kept.setdefault(row["photo_id"], []).append(row)
        return kept

    def evidence(self, detection_ids: Iterable[int]) -> dict[int, sqlite3.Row]:
        """The recorded check of each examined region in ``detection_ids``."""
        found: dict[int, sqlite3.Row] = {}
        for chunk in chunked(sorted({int(did) for did in detection_ids})):
            placeholders = ",".join("?" * len(chunk))
            for row in self.conn.execute(
                f"""SELECT detection_id, detector_category, classifier_model,
                           labels_fingerprint, top_species, max_match_score,
                           background_score, background_prompt, promoted,
                           checked_at
                      FROM subject_candidates
                     WHERE detection_id IN ({placeholders})""",
                chunk,
            ):
                found[row["detection_id"]] = row
        return found

    def kept_rows_for_photo(self, photo_id: int, detector_model: str) -> list[sqlite3.Row]:
        """Every kept candidate row of one photo's detector, for a rewrite to restore."""
        return self.conn.execute(
            """SELECT sc.* FROM subject_candidates sc
                 JOIN detections d ON d.id = sc.detection_id
                WHERE d.photo_id = ? AND d.detector_model = ? AND sc.promoted = 1""",
            (photo_id, detector_model),
        ).fetchall()

    def restore(self, rows: Iterable[sqlite3.Row], surviving_ids: Iterable[int]) -> None:
        """Re-insert kept rows whose detection a rewrite reproduced.

        Detection ids are content-addressed on (photo, model, box, detector
        category), so a reproduced id is the same region with the same
        detector label, and the classifier's answer about it still holds.
        """
        surviving = {int(did) for did in surviving_ids}
        for row in rows:
            if row["detection_id"] not in surviving:
                continue
            self.conn.execute(
                """INSERT OR REPLACE INTO subject_candidates
                     (detection_id, detector_category, classifier_model,
                      labels_fingerprint, top_species, max_match_score,
                      background_score, background_prompt, promoted, checked_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                tuple(row[key] for key in (
                    "detection_id", "detector_category", "classifier_model",
                    "labels_fingerprint", "top_species", "max_match_score",
                    "background_score", "background_prompt", "promoted",
                    "checked_at",
                )),
            )
            self.conn.execute(
                "UPDATE detections SET category = 'animal' WHERE id = ?",
                (row["detection_id"],),
            )
