"""Catalog reads and writes of the culling analysis (``culling.analyze_for_culling``).

The analysis groups a scope's photos by predicted species and scene, then
ranks each group. Per photo it reads the highest-confidence prediction on any
of its eligible detections (any classifier, newest labels fingerprint
per detection/model: the caller already scoped the photo ids), the quality,
file-type, time, pHash and filename signals it ranks on, and, for a photo
with no stored pHash, the source identity the pHash backfill loads the image
from; the backfill then stores the hash it computed. Embeddings come from
``db.masks_features.get_embedding`` and the folder paths from
``FolderRepository.all_rows``.

Photo rows and predictions are catalog-wide; the prediction ranking resolves
the active workspace's effective detector floor through its config callback.
``set_phash`` does not commit: the backfill commits once per
photo through ``Database.commit`` so the writer lock is released between
photos.

Callers reach it as ``db.culling`` (a fresh repository per access, see
``Database.culling``); there are no forwarding wrappers on ``Database``.
"""

import sqlite3
from collections.abc import Callable
from typing import Any

from settings_source import config_value


class CullingRepository:
    def __init__(
        self, conn: sqlite3.Connection,
        # Config values span arbitrary settings and workspace overrides.
        effective_config: Callable[[], dict[str, Any]],
    ) -> None:
        self.conn = conn
        self.effective_config = effective_config

    def top_prediction(
        self, photo_id: int, min_detector_confidence: float | None = None,
    ) -> sqlite3.Row | None:
        """The photo's highest-confidence prediction across its detections.

        ``species``, ``confidence``, ``classifier_model``,
        ``scientific_name``, ``labels_fingerprint`` and ``source_taxon_id``,
        or None when the photo has no prediction.
        """
        if min_detector_confidence is None:
            min_detector_confidence = config_value(self.effective_config(), "detector_confidence")
        return self.conn.execute(
            """SELECT pr.species, pr.confidence, pr.classifier_model,
                      pr.scientific_name, pr.labels_fingerprint, pr.source_taxon_id
               FROM predictions pr
               JOIN detections d ON d.id = pr.detection_id
               WHERE d.photo_id = ?
                 AND (d.detector_model = 'full-image' OR d.detector_confidence >= ? AND d.subject_verdict IS NULL)
                 AND pr.labels_fingerprint = (
                     SELECT pr2.labels_fingerprint FROM predictions pr2
                     WHERE pr2.detection_id = pr.detection_id
                       AND pr2.classifier_model = pr.classifier_model
                     ORDER BY pr2.created_at DESC, pr2.id DESC LIMIT 1
                 )
               ORDER BY pr.confidence DESC, pr.id DESC LIMIT 1""",
            (photo_id, min_detector_confidence),
        ).fetchone()

    def photo_signals(self, photo_id: int) -> sqlite3.Row | None:
        """``quality_score``, ``sharpness``, ``subject_sharpness``,
        ``extension``, ``timestamp``, ``phash`` and ``filename`` of the photo,
        or None without a row."""
        return self.conn.execute(
            "SELECT quality_score, sharpness, subject_sharpness, extension, timestamp, phash, filename FROM photos WHERE id = ?",
            (photo_id,),
        ).fetchone()

    def phash_source(self, photo_id: int) -> sqlite3.Row | None:
        """``folder_id``, ``filename`` and ``working_copy_path`` of the photo
        (what the pHash backfill loads its image from), or None."""
        return self.conn.execute(
            "SELECT folder_id, filename, working_copy_path FROM photos WHERE id = ?",
            (photo_id,),
        ).fetchone()

    def set_phash(self, photo_id: int, phash: str) -> None:
        """Store the photo's perceptual hash, in the caller's transaction."""
        self.conn.execute(
            "UPDATE photos SET phash = ? WHERE id = ?",
            (phash, photo_id),
        )
