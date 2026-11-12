"""The SQL ordering that picks one detection per photo as its primary subject.

Every reader that chooses one detection per photo orders by this, so the
lightbox, the subject panel, the mask, eye-keypoint and quality stages, the
pipeline stage counts and Highlights (through ``subject_rank``) agree on
which region is the photo's subject:

1. a real animal box before the whole-frame row and non-animal boxes;
2. the region the user chose as the subject;
3. boxes that do not span the frame before ones that do
   (``species_memberships.FRAME_SPANNING_EXTENT``): the detector boxes tree
   trunks and blurred foreground as full-height strips beside the animal,
   often more confidently than the animal itself;
4. regions the classifier scored as an animal before background-like ones
   (``background_match``, the same margin and confident-box override);
5. the region whose best species label matched best in its newest
   classifier run (cosine runs before other score kinds, which are on a
   different scale); a crop of the animal fits a species label better than
   the scenery beside it;
6. the detector's confidence, then the region's most confident prediction,
   then the lower id.

Subject quality is deliberately not an input: it is measured on the chosen
region, and on tiny or textured boxes (a speck, a bush top) it reads higher
than on the animal. Checked by eye on 57 multi-box photos, this order picked
the animal in 53, the quality-first order in 42 and detector-first in 19.
"""

from background_match import BACKGROUND_MARGIN, CONFIDENT_DETECTION
from species_memberships import FRAME_SPANNING_EXTENT

#: Added to a run's best label score so cosine runs sort above runs scored on
#: another scale (supervised logits), and both above an unscored region.
_COSINE_RANK = 2000
_OTHER_SCORE_RANK = 1000


def _newest_match_run(alias: str, expr: str) -> str:
    """``expr`` over the detection's newest ``classifier_match_scores`` row."""
    return f"""(SELECT {expr} FROM classifier_match_scores cms
                WHERE cms.detection_id = {alias}.id
                ORDER BY cms.run_at DESC, cms.rowid DESC LIMIT 1)"""


def chosen_subject_sql(alias="detections"):
    """1 when the detection is the user's chosen subject for its photo, else 0."""
    return f"""COALESCE({alias}.id = (SELECT detection_id FROM photo_subject_choices
                              WHERE photo_id = {alias}.photo_id), 0)"""


def primary_order_sql(alias="detections"):
    """Shared ordering for readers that choose one detection per photo."""
    background_like = _newest_match_run(
        alias,
        "cms.score_kind = 'cosine' "
        f"AND cms.background_score - cms.max_match_score >= -{BACKGROUND_MARGIN}",
    )
    match_rank = _newest_match_run(
        alias,
        f"CASE WHEN cms.score_kind = 'cosine' THEN {_COSINE_RANK} "
        f"ELSE {_OTHER_SCORE_RANK} END + cms.max_match_score",
    )
    return f"""CASE WHEN {alias}.category = 'animal'
                     AND {alias}.detector_model != 'full-image' THEN 1 ELSE 0 END DESC,
        {chosen_subject_sql(alias)} DESC,
        MAX({alias}.box_w, {alias}.box_h) >= {FRAME_SPANNING_EXTENT},
        COALESCE({alias}.detector_confidence < {CONFIDENT_DETECTION}
                 AND {background_like}, 0),
        {match_rank} DESC,
        {alias}.detector_confidence DESC,
        (SELECT MAX(confidence) FROM predictions WHERE detection_id = {alias}.id) DESC,
        {alias}.id ASC"""


def subject_rank_sql(alias="detections"):
    """The detection's position among its photo's rows in ``primary_order_sql``
    (1 = primary), as a window expression over the rows the query keeps."""
    return f"ROW_NUMBER() OVER (PARTITION BY {alias}.photo_id ORDER BY {primary_order_sql(alias)})"
