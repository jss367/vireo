"""Persistence for per-subject analysis and the photo's primary subject.

``detection_subjects`` (the cached quality/crop/exposure analysis of each
retained animal box), ``photo_subject_state`` (the detection currently
projected as the photo's primary) and ``photo_subject_choices`` (a manual
primary override) are catalog-wide: a subject's analysis is a pure function
of the photo and its detection, so nothing here resolves the active
workspace. The one workspace-dependent read, the visible predictions listed
beside each subject, takes the workspace id from its caller.

Callers reach it as ``db.subjects`` (a fresh repository per access, see
``Database.subjects``); there are no forwarding wrappers on ``Database``.
Nothing here commits: ``subjects.analyze_photo`` / ``select_primary`` commit
through ``db.commit_with_retry()`` at the points they always did, and
``subjects.sync_primary`` leaves the commit to its caller. The decisions —
which detections are retained, which one is primary, whether the active mask
still matches it — stay in ``subjects``.
"""

import json
import sqlite3
from collections.abc import Iterable, Mapping
from typing import Any

from repositories.prediction_review import prediction_review_join, prediction_review_status


class SubjectsRepository:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    # -- analysis cache ---------------------------------------------------------

    def has_stale_primary_state(self, photo_id: int) -> bool:
        """Whether ``photo_id`` carries any subject-derived state to clear.

        True when a ``photo_subject_state`` row exists or any subject-derived
        column on ``photos`` is set. Migrated catalogs have the legacy columns
        without a state row, so both signals count.
        """
        row = self.conn.execute(
            "SELECT 1 WHERE EXISTS (SELECT 1 FROM photo_subject_state WHERE photo_id=?) "
            "OR EXISTS (SELECT 1 FROM photos WHERE id=? AND ("
            "mask_path IS NOT NULL OR active_mask_variant IS NOT NULL "
            "OR quality_score IS NOT NULL OR subject_sharpness IS NOT NULL "
            "OR subject_size IS NOT NULL OR subject_clip_high IS NOT NULL "
            "OR subject_clip_low IS NOT NULL OR subject_y_median IS NOT NULL "
            "OR subject_tenengrad IS NOT NULL OR bg_tenengrad IS NOT NULL "
            "OR crop_complete IS NOT NULL OR bg_separation IS NOT NULL "
            "OR phash_crop IS NOT NULL OR noise_estimate IS NOT NULL "
            "OR dino_subject_embedding IS NOT NULL "
            "OR eye_x IS NOT NULL OR eye_y IS NOT NULL "
            "OR eye_conf IS NOT NULL OR eye_tenengrad IS NOT NULL "
            "OR eye_kp_fingerprint IS NOT NULL))",
            (photo_id, photo_id),
        ).fetchone()
        return row is not None

    def cached_detection_ids(self, photo_id: int, source_key: str) -> set[int]:
        """Detection ids of ``photo_id`` already analyzed for ``source_key``."""
        return {r["detection_id"] for r in self.conn.execute(
            "SELECT s.detection_id FROM detection_subjects s JOIN detections d ON d.id=s.detection_id "
            "WHERE d.photo_id=? AND s.source_key=?", (photo_id, source_key))}

    def save_analyses(self, source_key: str, results: Iterable[Mapping[str, Any]]) -> None:
        """Upsert each analysis result, skipping detections that no longer exist.

        The insert selects from ``detections`` so a detection replaced while
        the image was decoded writes nothing. Each result carries
        ``detection_id``, ``crop``, ``quality_score``, ``exposure_ev`` and
        ``features`` (``analyze_image``'s shape). No commit.
        """
        for result in results:
            self.conn.execute(
                """INSERT INTO detection_subjects
                   (detection_id, source_key, crop, quality_score, exposure_ev, features)
                   SELECT id, ?, ?, ?, ?, ? FROM detections WHERE id=?
                   ON CONFLICT(detection_id) DO UPDATE SET source_key=excluded.source_key,
                   crop=excluded.crop, quality_score=excluded.quality_score,
                   exposure_ev=excluded.exposure_ev, features=excluded.features""",
                (source_key, json.dumps(result["crop"]), result["quality_score"],
                 result["exposure_ev"], json.dumps(result["features"]), result["detection_id"]),
            )

    def clear_analysis(self, detection_id: int) -> None:
        """Retire this region's box measurements; caller owns the transaction."""
        self.conn.execute("DELETE FROM detection_subjects WHERE detection_id=?", (detection_id,))

    def analysis(self, detection_id: int) -> sqlite3.Row | None:
        """The ``detection_subjects`` row of ``detection_id``, or None."""
        return self.conn.execute(
            "SELECT * FROM detection_subjects WHERE detection_id=?", (detection_id,)
        ).fetchone()

    def analyses_for_photo(self, photo_id: int) -> dict[int, dict[str, Any]]:
        """Every ``detection_subjects`` row of ``photo_id``'s detections, by detection id."""
        return {r["detection_id"]: dict(r) for r in self.conn.execute(
            "SELECT s.* FROM detection_subjects s JOIN detections d ON d.id=s.detection_id WHERE d.photo_id=?",
            (photo_id,))}

    def ranking_analysis(self, detection_id: int, source_key: str) -> dict[str, Any] | None:
        """One immutable analysis generation, including its exact mask bytes."""
        row = self.conn.execute(
            "SELECT features, mask_png FROM subject_ranking_analysis "
            "WHERE detection_id=? AND source_key=?", (detection_id, source_key),
        ).fetchone()
        return {"features": json.loads(row["features"]), "mask_png": row["mask_png"]} if row else None

    def save_ranking_analysis(
        self, detection_id: int, source_key: str, features: Mapping[str, Any], mask_png: bytes,
    ) -> None:
        """Publish one immutable generation and prune its superseded cache rows.

        Caller commits the insert and prune together. Contexts with different
        settings may need reanalysis; old URLs already fail key validation.
        """
        self.conn.execute(
            "INSERT OR IGNORE INTO subject_ranking_analysis(detection_id, source_key, features, mask_png) "
            "SELECT id, ?, ?, ? FROM detections WHERE id=?",
            (source_key, json.dumps(features, allow_nan=False), mask_png, detection_id),
        )
        self.conn.execute(
            "DELETE FROM subject_ranking_analysis WHERE detection_id=? AND source_key!=?",
            (detection_id, source_key),
        )

    def discard_ranking_analysis(self, detection_id: int, source_key: str) -> None:
        """Retire one caller-validated invalid generation; caller owns the transaction."""
        self.conn.execute(
            "DELETE FROM subject_ranking_analysis WHERE detection_id=? AND source_key=?",
            (detection_id, source_key),
        )

    # -- primary projection -----------------------------------------------------

    def clear_primary_features(self, photo_id: int) -> None:
        """NULL the mask and every subject-derived feature column of ``photo_id``."""
        self.conn.execute("""UPDATE photos SET mask_path=NULL, active_mask_variant=NULL,
        quality_score=NULL, subject_sharpness=NULL, subject_size=NULL,
        subject_clip_high=NULL, subject_clip_low=NULL, subject_y_median=NULL,
        subject_tenengrad=NULL, bg_tenengrad=NULL, crop_complete=NULL,
        bg_separation=NULL, phash_crop=NULL, noise_estimate=NULL,
        eye_x=NULL, eye_y=NULL, eye_conf=NULL, eye_tenengrad=NULL,
        eye_kp_fingerprint=NULL, dino_subject_embedding=NULL WHERE id=?""", (photo_id,))

    def primary_state(self, photo_id: int) -> sqlite3.Row | None:
        """``photo_id``'s projected primary (``detection_id``), or None."""
        return self.conn.execute(
            "SELECT detection_id FROM photo_subject_state WHERE photo_id=?", (photo_id,)
        ).fetchone()

    def set_primary_state(self, photo_id: int, detection_id: int) -> None:
        """Record ``detection_id`` as ``photo_id``'s projected primary."""
        self.conn.execute("INSERT INTO photo_subject_state(photo_id,detection_id) VALUES (?,?) "
                          "ON CONFLICT(photo_id) DO UPDATE SET detection_id=excluded.detection_id",
                          (photo_id, detection_id))

    def clear_primary_state(self, photo_id: int) -> None:
        """Forget ``photo_id``'s projected primary."""
        self.conn.execute("DELETE FROM photo_subject_state WHERE photo_id=?", (photo_id,))

    def active_mask(self, photo_id: int) -> sqlite3.Row | None:
        """The ``photo_masks`` row of ``photo_id``'s active variant, or None."""
        return self.conn.execute("""SELECT pm.* FROM photo_masks pm JOIN photos p
        ON p.id=pm.photo_id AND p.active_mask_variant=pm.variant WHERE p.id=?""", (photo_id,)).fetchone()

    def set_primary_quality(self, photo_id: int, quality_score: float, subject_sharpness: float) -> None:
        """Project the primary's ``quality_score`` and ``subject_sharpness`` onto ``photos``."""
        self.conn.execute("UPDATE photos SET quality_score=?, subject_sharpness=? WHERE id=?",
                          (quality_score, subject_sharpness, photo_id))

    def set_primary_box_features(self, photo_id: int, features: Mapping[str, Any]) -> None:
        """Project the primary's box-measured size and clipping onto ``photos``."""
        self.conn.execute("""UPDATE photos SET subject_size=?,
                subject_clip_high=?, subject_clip_low=?, subject_y_median=? WHERE id=?""",
                (features["subject_size"], features["subject_clip_high"], features["subject_clip_low"],
                 features["subject_y_median"], photo_id))

    # -- manual choice ----------------------------------------------------------

    def choice(self, photo_id: int) -> sqlite3.Row | None:
        """``photo_id``'s manual primary choice (``detection_id``), or None."""
        return self.conn.execute(
            "SELECT detection_id FROM photo_subject_choices WHERE photo_id=?", (photo_id,)
        ).fetchone()

    def set_choice(self, photo_id: int, detection_id: int) -> None:
        """Remember ``detection_id`` as the manual primary of ``photo_id``.

        No detection FK: content-addressed identities survive clear/recreate
        during reprocessing, so a temporarily missing choice stays remembered.
        """
        self.conn.execute("INSERT INTO photo_subject_choices(photo_id,detection_id) VALUES (?,?) "
                          "ON CONFLICT(photo_id) DO UPDATE SET detection_id=excluded.detection_id",
                          (photo_id, detection_id))

    def clear_choice(self, photo_id: int) -> None:
        """Return ``photo_id`` to automatic primary selection."""
        self.conn.execute("DELETE FROM photo_subject_choices WHERE photo_id=?", (photo_id,))

    # -- payload ----------------------------------------------------------------

    def visible_predictions(self, workspace_id: int, photo_id: int, floor: float) -> list[sqlite3.Row]:
        """Predictions on ``photo_id``'s detections the subject panel lists.

        At or above ``floor``, not rejected in ``workspace_id``, and from each
        detection/classifier pair's newest label set; highest confidence first.
        """
        return self.conn.execute(f"""SELECT pr.*, {prediction_review_status('rv')} AS status
        FROM predictions pr JOIN detections d ON d.id=pr.detection_id
        {prediction_review_join('pr', review_alias='rv')}
        WHERE d.photo_id=? AND pr.confidence>=? AND {prediction_review_status('rv')}!='rejected'
        AND pr.labels_fingerprint=(SELECT pr2.labels_fingerprint FROM predictions pr2
            WHERE pr2.detection_id=pr.detection_id AND pr2.classifier_model=pr.classifier_model
            ORDER BY pr2.created_at DESC, pr2.id DESC LIMIT 1)
        ORDER BY pr.confidence DESC, pr.id ASC""", (workspace_id, photo_id, floor)).fetchall()
