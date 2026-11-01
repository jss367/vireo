"""Shared ranking for Browse species counts and their drill-down filter."""

from repositories.prediction_review import prediction_review_join, prediction_review_status

# Bind workspace id, then the effective detector-confidence floor.
TOP_SPECIES_RANKING_SQL = f"""
SELECT det.photo_id, pred.species,
       ROW_NUMBER() OVER (
           PARTITION BY det.photo_id
           ORDER BY pred.confidence DESC, pred.id DESC
       ) AS rn
FROM predictions pred
JOIN detections det ON det.id = pred.detection_id
{prediction_review_join('pred')}
WHERE det.detector_confidence >= ? AND det.subject_verdict IS NULL
  AND {prediction_review_status()} != 'rejected'
  AND pred.labels_fingerprint = (
      SELECT pr2.labels_fingerprint FROM predictions pr2
      WHERE pr2.detection_id = pred.detection_id
        AND pr2.classifier_model = pred.classifier_model
      ORDER BY pr2.created_at DESC, pr2.id DESC
      LIMIT 1
  )
"""
