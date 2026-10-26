"""The SQL ordering that picks one detection per photo as its primary subject.

Readers that choose one detection per photo (the pipeline stage counts, the
mask and eye-keypoint candidates, the review payloads) order by this so they
all agree on which detection is primary: a real animal box first, then the
user's explicit subject choice, then subject quality, then detector
confidence.
"""


def primary_order_sql(alias="detections"):
    """Shared ordering for readers that choose one detection per photo."""
    return f"""CASE WHEN {alias}.category = 'animal'
                     AND {alias}.detector_model != 'full-image' THEN 1 ELSE 0 END DESC,
        COALESCE({alias}.id = (SELECT detection_id FROM photo_subject_choices
                              WHERE photo_id = {alias}.photo_id), 0) DESC,
        (SELECT quality_score FROM detection_subjects
         WHERE detection_id = {alias}.id) DESC,
        {alias}.detector_confidence DESC, {alias}.id ASC"""
