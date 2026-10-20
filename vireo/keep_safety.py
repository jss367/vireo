"""Positive-KEEP safeguards for independent ranking and bulk application routes."""

from functools import wraps

from subject_readiness import automatic_keep_assessment, explicit_decision

KEEP_REVIEW_MESSAGE = (
    "Absolute focus and animal-subject validity have not been validated; review this frame."
)


def review_ranked_response(ranker):
    """Guard every return, including selected-subject rankers added to Best Batch.

    Ranking eligibility stays separate: useful provisional ranks and measurements
    survive. Rank alone establishes neither usable alternates nor reject-rest.
    """
    @wraps(ranker)
    def guarded(*args, **kwargs):
        result, error = ranker(*args, **kwargs)
        if result is not None and not error:
            review_ranked_suggestions(result)
        return result, error
    return guarded


def review_ranked_suggestions(result):
    """Refresh cached/independent suggestions through the same positive policy."""
    cards = result.get("cards", [])
    result["ranking_status"] = "provisional"
    result["keep_review_message"] = KEEP_REVIEW_MESSAGE
    for card in cards:
        assessment = automatic_keep_assessment(card)
        card.update(automatic_keep_eligible=assessment["eligible"], keep_status=assessment["status"],
                    keep_reasons=assessment["reasons"])
        decision = explicit_decision(card)
        if decision:
            card["label"] = decision
            card["role"] = "kept" if decision == "KEEP" else "reject"
        elif card.get("label") != "REJECT" or not card.get("reject_reasons"):
            card["label"] = "REVIEW"
            card["role"] = "review"
            reasons = card.get("reasons") or []
            card["reasons"] = ([*reasons, KEEP_REVIEW_MESSAGE] if card.get("analysis_reasons")
                               else [KEEP_REVIEW_MESSAGE, *reasons])
        else:
            card["role"] = "reject"
    candidates = [c for c in cards if c.get("rank") is not None and c.get("label") != "REJECT"]
    result["ranked_candidate_id"] = min(candidates, key=lambda c: c["rank"])["id"] if candidates else None
    summary = result.get("summary", {})
    for label in ("KEEP", "REVIEW", "REJECT"):
        key = label.lower() + "_count"
        if key in summary:
            summary[key] = sum(card.get("label") == label for card in cards)
    # No validated positive evidence producer exists yet. These action IDs must
    # never alias the provisional leader, even if another ranker returns KEEP.
    result.update(best_photo_id=None, best_filename=None, best_reasons=[KEEP_REVIEW_MESSAGE],
                  alternate_ids=[], suggested_reject_ids=[
                      c["id"] for c in cards if c.get("label") == "REJECT" and not explicit_decision(c)])


def best_batch_application_error(photos, best_id, reject_ids):
    """Recheck server-side; clients cannot turn a provisional rank into a flag.

    Already explicit decisions may be replayed, but cannot be overwritten by
    this recommendation action. Deliberate individual review uses /batch/flag.
    """
    if explicit_decision(dict(photos[best_id])) != "KEEP":
        return "Best Batch has no validated automatic keeper. Inspect and flag individual photos manually."
    if any(explicit_decision(dict(photos[pid])) != "REJECT" for pid in reject_ids):
        return "Relative ranking cannot reject the remaining photos. Review and flag individual photos manually."
    return None
