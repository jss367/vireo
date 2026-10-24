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


def _review_culling_photo(photo, state):
    """Only a live, recorded photographer confirmation can override review."""
    # Bare flags from legacy callers carry no producer evidence. Never infer a
    # photographer's approval from a flag that a ranking job can also write.
    if not isinstance(state, dict):
        state = {"flag": state if isinstance(state, str) else "none"}
    flag = state.get("flag", "none")
    photo["ranking_selected"] = photo.get("ranking_selected", photo.get("action") == "keep")
    photo["flag"] = flag
    assessment = automatic_keep_assessment(photo)
    photo.update(automatic_keep_eligible=assessment["eligible"], keep_status=assessment["status"],
                 keep_reasons=assessment["reasons"])
    confirmed = state.get("confirmed_flag") == flag and flag != "none"
    decision = explicit_decision(photo) if confirmed else None
    photo["flag_confirmation_status"] = "confirmed" if confirmed else "unverified"
    photo["label"] = decision or "REVIEW"
    photo["action"] = photo["label"].lower()
    photo["decision_source"] = "confirmed" if decision else "auto"


def review_culling_suggestions(result, photo_flags):
    """Guard the legacy culling job and old caches using current catalog flags.

    Redundancy similarity does not establish a usable winner or justify rejecting
    its neighbors. Keep the grouping and quality measurements for inspection,
    but expose only photographer decisions until a validated producer exists.
    Refresh the raw regrouping data too so it cannot revive cached approval.
    """
    result["ranking_status"] = "provisional"
    result["keep_review_message"] = KEEP_REVIEW_MESSAGE
    totals = {"keep": 0, "review": 0, "reject": 0}
    for group in result.get("species_groups", []):
        counts = {"keep": 0, "review": 0, "reject": 0}
        for scene in group.get("scene_groups", group.get("pose_groups", [])):
            for photo in scene["photos"]:
                _review_culling_photo(photo, photo_flags.get(photo["photo_id"], {}))
                counts[photo["action"]] += 1
        for photo in group.get("photos_data", []):
            _review_culling_photo(photo, photo_flags.get(photo["photo_id"], {}))
        for action, field in (("keep", "keepers"), ("review", "reviews"), ("reject", "rejects")):
            group[field] = counts[action]
            totals[action] += counts[action]
    result.update(suggested_keepers=totals["keep"], suggested_reviews=totals["review"],
                  suggested_rejects=totals["reject"])
    return result
