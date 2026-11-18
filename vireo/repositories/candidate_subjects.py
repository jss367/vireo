"""Persistence for candidate subjects: person/vehicle regions the classifier examined.

Owns ``subject_candidates``. MegaDetector labels some animals "person" or
"vehicle" (a Wood Duck drake, a Mandarin Duck, a resting goose). On a photo
with no retained animal region, the strongest retained person/vehicle region
is a candidate: the classify stage asks the classifier about it once per
(classifier model, label set) and records each answer as its own row here.
A region is kept as a subject when any check recognized an animal in it: its
``detections.category`` is set to ``animal``, and the rows are the record
that the detector said otherwise (``detector_category``) and of the evidence.

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

# The detector's own category of a kept region ``d``, else its stored one.
# A kept region reads ``animal`` in ``detections`` while its content-addressed
# id derives from the detector's label, so readers that re-derive an id (the
# portable computation cache, RAW+JPEG pairing) select this instead.
DETECTOR_CATEGORY_SQL = (
    "COALESCE((SELECT sc.detector_category FROM subject_candidates sc "
    "WHERE sc.detection_id = d.id AND sc.promoted = 1 LIMIT 1), d.category)"
)

_COLUMNS = (
    "detection_id", "classifier_model", "labels_fingerprint",
    "detector_category", "top_species", "max_match_score",
    "background_score", "background_prompt", "promoted", "checked_at",
)

# Shared by ``pending`` and ``count``: the strongest retained non-animal
# MegaDetector region of each photo that has no retained animal region.
# Also examine a non-animal region with stronger AF evidence than every
# retained animal. An off-center bystander must not hide that candidate.
_TOP_CANDIDATE_SQL = """
    SELECT d.id, d.photo_id, d.box_x, d.box_y, d.box_w, d.box_h,
           d.detector_confidence, d.category, d.detector_model,
           ROW_NUMBER() OVER (
               PARTITION BY d.photo_id
               ORDER BY COALESCE(f.af, 0) DESC, d.detector_confidence DESC, d.id ASC
           ) AS rn
      FROM detections d LEFT JOIN subject_focus f ON f.detection_id=d.id
     WHERE d.detector_model = 'megadetector-v6'
       AND d.category != 'animal'
       AND d.detector_confidence >= ? AND d.subject_verdict IS NULL
       AND d.photo_id IN ({placeholders})
       AND NOT EXISTS (
             SELECT 1 FROM detections a
              WHERE a.photo_id = d.photo_id
                AND a.detector_model != 'full-image'
                AND a.category = 'animal'
                AND COALESCE((SELECT af FROM subject_focus WHERE detection_id=a.id), 0)
                    >= COALESCE(f.af, 0)
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

    def record(self, detection_id: int, evidence: Mapping[str, Any], *, promoted: bool) -> bool:
        """Record one check of a candidate region; False once the region is gone.

        ``evidence`` carries ``detector_category``, ``classifier_model``,
        ``labels_fingerprint`` and the match fields of
        ``classify_job.classify_pass._match_stats``. The check is keyed by
        (region, model, label set), so another model's or label set's answer
        is kept beside it. A promoted check sets ``detections.category`` to
        ``animal``. Recording the same key again replaces its evidence but
        never clears ``promoted``: a region already kept stays kept, so its
        subject analysis and species decisions are not withdrawn by a rerun.

        The crop is classified without the catalog write lock, so a
        concurrent re-detect may have retired the region by now; its checks
        went with it and this one has nothing to attach to.
        """
        if self.conn.execute(
            "SELECT 1 FROM detections WHERE id = ?", (detection_id,),
        ).fetchone() is None:
            return False
        self.conn.execute(
            """INSERT INTO subject_candidates
                 (detection_id, classifier_model, labels_fingerprint,
                  detector_category, top_species, max_match_score,
                  background_score, background_prompt, promoted)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(detection_id, classifier_model, labels_fingerprint)
               DO UPDATE SET
                 detector_category = excluded.detector_category,
                 top_species = excluded.top_species,
                 max_match_score = excluded.max_match_score,
                 background_score = excluded.background_score,
                 background_prompt = excluded.background_prompt,
                 promoted = MAX(subject_candidates.promoted, excluded.promoted),
                 checked_at = datetime('now')""",
            (
                detection_id, evidence["classifier_model"],
                evidence["labels_fingerprint"], evidence["detector_category"],
                evidence.get("top_species"), evidence.get("max_match_score"),
                evidence.get("background_score"), evidence.get("background_prompt"),
                1 if promoted else 0,
            ),
        )
        if promoted:
            self._mark_animal(detection_id)
        return True

    def _mark_animal(self, detection_id: int) -> None:
        self.conn.execute(
            "UPDATE detections SET category = 'animal' WHERE id = ?",
            (detection_id,),
        )

    def kept_regions(self, photo_ids: Iterable[int]) -> dict[int, list[sqlite3.Row]]:
        """Kept candidate regions of ``photo_ids``, by photo.

        Each row is the detection (``id``, box, ``detector_confidence``,
        ``category``, ``detector_model``) plus ``detector_category``. A
        region the reviewer rejected as not an animal is left out, like every
        other reader of retained regions.
        """
        kept: dict[int, list[sqlite3.Row]] = {}
        for chunk in chunked(sorted({int(pid) for pid in photo_ids})):
            placeholders = ",".join("?" * len(chunk))
            for row in self.conn.execute(
                f"""SELECT d.id, d.photo_id, d.box_x, d.box_y, d.box_w, d.box_h,
                           d.detector_confidence, d.category, d.detector_model,
                           {DETECTOR_CATEGORY_SQL} AS detector_category
                      FROM detections d
                     WHERE d.photo_id IN ({placeholders})
                       AND d.subject_verdict IS NULL
                       AND EXISTS (SELECT 1 FROM subject_candidates sc
                                    WHERE sc.detection_id = d.id AND sc.promoted = 1)
                     ORDER BY d.photo_id, d.detector_confidence DESC, d.id""",
                chunk,
            ):
                kept.setdefault(row["photo_id"], []).append(row)
        return kept

    def evidence(self, detection_ids: Iterable[int]) -> dict[int, sqlite3.Row]:
        """One recorded check per examined region in ``detection_ids``.

        The check that kept the region when one did (the newest of them),
        else its newest check, so ``promoted`` answers whether the region is
        kept.
        """
        found: dict[int, sqlite3.Row] = {}
        for chunk in chunked(sorted({int(did) for did in detection_ids})):
            placeholders = ",".join("?" * len(chunk))
            for row in self.conn.execute(
                f"""SELECT {", ".join(_COLUMNS)}
                      FROM subject_candidates
                     WHERE detection_id IN ({placeholders})
                     ORDER BY promoted, checked_at, rowid""",
                chunk,
            ):
                found[row["detection_id"]] = row
        return found

    def checks_for_photo(self, photo_id: int, detector_model: str) -> list[sqlite3.Row]:
        """Every check of one photo's detector regions, for a rewrite to restore."""
        return self.conn.execute(
            f"""SELECT {", ".join("sc." + column for column in _COLUMNS)}
                  FROM subject_candidates sc
                  JOIN detections d ON d.id = sc.detection_id
                 WHERE d.photo_id = ? AND d.detector_model = ?""",
            (photo_id, detector_model),
        ).fetchall()

    def restore(self, rows: Iterable[sqlite3.Row], surviving_ids: Iterable[int]) -> None:
        """Re-insert the checks whose detection a rewrite reproduced.

        Detection ids are content-addressed on (photo, model, box, detector
        category), so a reproduced id is the same region with the same
        detector label, and the classifier's answers about it still hold:
        a kept region stays kept, and a rejected one is not read again.
        """
        surviving = {int(did) for did in surviving_ids}
        for row in rows:
            if row["detection_id"] not in surviving:
                continue
            self.conn.execute(
                f"""INSERT OR REPLACE INTO subject_candidates ({", ".join(_COLUMNS)})
                    VALUES ({", ".join("?" * len(_COLUMNS))})""",
                tuple(row[column] for column in _COLUMNS),
            )
            if row["promoted"]:
                self._mark_animal(row["detection_id"])

    def move(self, old_id: int, new_id: int) -> None:
        """Carry the checks of ``old_id`` onto ``new_id``, the same region re-keyed.

        ``new_id`` may already have checks of its own. A check ``old_id``
        holds under the same model and label set merges into it, keeping
        ``promoted`` from either; the rest move over. ``new_id`` then reads
        ``animal`` when any of its checks kept the region.
        """
        self.conn.execute(
            """UPDATE subject_candidates SET promoted = 1
                WHERE detection_id = ?
                  AND EXISTS (SELECT 1 FROM subject_candidates old
                               WHERE old.detection_id = ?
                                 AND old.classifier_model = subject_candidates.classifier_model
                                 AND old.labels_fingerprint = subject_candidates.labels_fingerprint
                                 AND old.promoted = 1)""",
            (new_id, old_id),
        )
        self.conn.execute(
            "UPDATE OR IGNORE subject_candidates SET detection_id = ? WHERE detection_id = ?",
            (new_id, old_id),
        )
        self.conn.execute(
            "DELETE FROM subject_candidates WHERE detection_id = ?", (old_id,),
        )
        self.conn.execute(
            """UPDATE detections SET category = 'animal'
                WHERE id = ?
                  AND EXISTS (SELECT 1 FROM subject_candidates sc
                               WHERE sc.detection_id = ? AND sc.promoted = 1)""",
            (new_id, new_id),
        )
