"""Catalog reads and writes of the culling analysis (``culling.analyze_for_culling``).

The analysis groups a scope's photos by predicted species and scene, then
ranks each group. Per photo it reads the highest-confidence prediction on any
of its detections (any classifier, any labels fingerprint: the analysis
runs over photo ids its caller already scoped), the quality,
file-type, time, pHash and filename signals it ranks on, and, for a photo
with no stored pHash, the source identity the pHash backfill loads the image
from; the backfill then stores the hash it computed. Embeddings come from
``db.masks_features.get_embedding`` and the folder paths from
``Database.get_all_folders``.

Photo rows and predictions are catalog-wide, so the repository takes no
workspace id. ``set_phash`` does not commit: the backfill commits once per
photo through ``Database.commit`` so the writer lock is released between
photos.

Callers reach it as ``db.culling`` (a fresh repository per access, see
``Database.culling``); there are no forwarding wrappers on ``Database``.
"""

import sqlite3


class CullingRepository:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    def top_prediction(self, photo_id: int) -> sqlite3.Row | None:
        """The photo's highest-confidence prediction across its detections.

        ``species``, ``confidence``, ``classifier_model``,
        ``scientific_name``, ``labels_fingerprint`` and ``source_taxon_id``,
        or None when the photo has no prediction.
        """
        return self.conn.execute(
            """SELECT pr.species, pr.confidence, pr.classifier_model,
                      pr.scientific_name, pr.labels_fingerprint, pr.source_taxon_id
               FROM predictions pr
               JOIN detections d ON d.id = pr.detection_id
               WHERE d.photo_id = ?
               ORDER BY pr.confidence DESC LIMIT 1""",
            (photo_id,),
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
