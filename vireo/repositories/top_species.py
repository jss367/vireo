"""Shared ranking for Browse species counts and their drill-down filter."""

# Bind workspace id, then the effective detector-confidence floor.
TOP_SPECIES_RANKING_SQL = """
SELECT det.photo_id, pred.species,
       ROW_NUMBER() OVER (
           PARTITION BY det.photo_id
           ORDER BY pred.confidence DESC, pred.id DESC
       ) AS rn
FROM predictions pred
JOIN detections det ON det.id = pred.detection_id
LEFT JOIN prediction_review pr_rev
  ON pr_rev.prediction_id = pred.id
 AND pr_rev.workspace_id = ?
WHERE det.detector_confidence >= ?
  AND COALESCE(pr_rev.status, 'pending') != 'rejected'
  AND pred.labels_fingerprint = (
      SELECT pr2.labels_fingerprint FROM predictions pr2
      WHERE pr2.detection_id = pred.detection_id
        AND pr2.classifier_model = pred.classifier_model
      ORDER BY pr2.created_at DESC, pr2.id DESC
      LIMIT 1
  )
"""
