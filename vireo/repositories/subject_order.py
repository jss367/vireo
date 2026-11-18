"""Shared primary-subject ordering and its deciding evidence."""

from background_match import BACKGROUND_MARGIN, CONFIDENT_DETECTION
from focus_point import FocusMatch
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


def primary_terms(alias="detections"):
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

    def focus_term(column):
        return f"COALESCE((SELECT {column} FROM subject_focus WHERE detection_id={alias}.id), 0)"

    return [
        ("Animal region", f"({alias}.category = 'animal' AND {alias}.detector_model != 'full-image')", "DESC"),
        ("Your choice", chosen_subject_sql(alias), "DESC"),
        ("Contains the camera AF point", f"({focus_term('af')} = {FocusMatch.CENTER})", "DESC"),
        ("Overlaps the camera AF area", f"({focus_term('af')} = {FocusMatch.AREA})", "DESC"),
        ("Region fits within the frame", f"MAX({alias}.box_w, {alias}.box_h) >= {FRAME_SPANNING_EXTENT}", "ASC"),
        ("Animal rather than background", f"COALESCE({alias}.detector_confidence < {CONFIDENT_DETECTION} "
         f"AND {background_like}, 0)", "ASC"),
        ("Near the camera AF area", focus_term("af"), "DESC"),
        ("Clearly sharper region", focus_term("sharp"), "DESC"),
        ("Overlaps the previous burst subject", focus_term("continuity"), "DESC"),
        ("Species-label match", match_rank, "DESC"),
        ("Detector confidence", f"{alias}.detector_confidence", "DESC"),
        ("Classifier confidence", f"(SELECT MAX(confidence) FROM predictions WHERE detection_id={alias}.id)", "DESC"),
        ("Stable region order", f"{alias}.id", "ASC"),
    ]


def primary_order_sql(alias="detections"):
    return ", ".join(f"{expr} {direction}" for _, expr, direction in primary_terms(alias))


def subject_rank_sql(alias="detections"):
    """The detection's position among its photo's rows in ``primary_order_sql``
    (1 = primary), as a window expression over the rows the query keeps."""
    return f"ROW_NUMBER() OVER (PARTITION BY {alias}.photo_id ORDER BY {primary_order_sql(alias)})"
