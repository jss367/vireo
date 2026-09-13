"""Absolute match strength for classifier output.

Both classifiers finish with a softmax over whatever label list happened to be
loaded, so the ``confidence`` we store is a *relative* number: it sums to 1
across the list and therefore always names a winner, however badly every label
actually fit. A Yellow-breasted Chat scored against a California list comes back
at 0.999 for the chat; the same photo scored against a list with no chat in it
comes back at ~0.99 for whatever is nearest. The two numbers are indis-
tinguishable and mean opposite things.

The raw pre-softmax score is the part that survives that normalization:

* For BioCLIP (``cosine``) it is the cosine similarity between the image
  embedding and the label's text embedding. Both embeddings are computed
  independently of the list, so this number does not move when labels are added
  or removed around it.
* For a supervised closed-set model such as iNat21 (``logit``) it is the class
  logit. Also absolute, also list-independent, but on an unrelated scale.

The consequence that governs this whole module: raw scores are comparable
*across label lists* but never *across models*. Every threshold is therefore
keyed by model name, and carries the score kind it was calibrated against so a
threshold can never be silently applied to the wrong scale.

There are deliberately no default thresholds. An uncalibrated model reports
``uncalibrated`` — "we recorded the number but are not judging it" — rather than
inventing a cutoff. ``scripts/calibrate_match_threshold.py`` derives a real one
from confirmed identifications in the catalog.
"""

COSINE = "cosine"
LOGIT = "logit"
SCORE_KINDS = (COSINE, LOGIT)

#: Config key holding ``{model_name: {"threshold": float, "score_kind": str}}``.
CONFIG_KEY = "match_thresholds"

#: Assessment states, in the order the UI should prefer to explain them.
LISTED = "listed"
UNLISTED = "unlisted"
UNCALIBRATED = "uncalibrated"
UNAVAILABLE = "unavailable"


def threshold_for(model, config=None):
    """Return ``(threshold, score_kind)`` configured for ``model``.

    Returns ``(None, None)`` when the model has no calibrated threshold, which
    every caller must treat as "do not judge" rather than "passes".
    """
    entry = ((config or {}).get(CONFIG_KEY) or {}).get(model)
    if not isinstance(entry, dict):
        return None, None
    try:
        threshold = float(entry["threshold"])
    except (KeyError, TypeError, ValueError):
        return None, None
    kind = entry.get("score_kind")
    if kind not in SCORE_KINDS:
        return None, None
    return threshold, kind


def assess(model, score_kind, max_match_score, margin=None, config=None):
    """Judge one classifier run's best raw score against its calibrated floor.

    ``score_kind`` is the kind recorded *with the run*. It is compared against
    the kind the threshold was calibrated on, and a mismatch degrades to
    ``uncalibrated`` instead of comparing a cosine to a logit — that comparison
    would silently mark every run unlisted (or every run listed) depending on
    which way the scales happen to fall.

    Returns a dict with ``state``, the inputs, and a sentence for the UI. The
    sentence answers "does the best label in my list actually match this
    image?", which is the question a confidence pill is read as and cannot
    answer on its own.
    """
    result = {
        "state": UNAVAILABLE,
        "model": model,
        "score_kind": score_kind,
        "max_match_score": max_match_score,
        "margin": margin,
        "threshold": None,
        "explanation": "Match strength was not recorded for this run.",
    }
    if max_match_score is None:
        return result

    threshold, calibrated_kind = threshold_for(model, config)
    score_text = _format_score(max_match_score, score_kind)

    if threshold is None or calibrated_kind != score_kind:
        result["state"] = UNCALIBRATED
        result["explanation"] = (
            f"Best label matches at {score_text}. No threshold has been "
            f"calibrated for {model}, so this is recorded but not judged."
        )
        return result

    result["threshold"] = threshold
    floor_text = _format_score(threshold, score_kind)
    if max_match_score < threshold:
        result["state"] = UNLISTED
        result["explanation"] = (
            f"No label in this list matches this image well — the best of them "
            f"reaches only {score_text}, under the {floor_text} floor "
            f"calibrated for {model}. The species shown is the closest "
            f"available label, not a match."
        )
    else:
        result["state"] = LISTED
        result["explanation"] = (
            f"Best label matches at {score_text}, above the {floor_text} floor "
            f"calibrated for {model}."
        )
    return result


def summarize(assessments):
    """Collapse several runs' assessments into one verdict for a photo.

    ``unlisted`` is returned only when at least one model was actually judged
    and *every* judged model came back unlisted. One model finding a good match
    is enough to make the photo a match, so a single ``listed`` wins — a
    detector that found a bad crop for one model should not be able to label
    the photo unidentifiable when another model saw it clearly.

    Runs that could not be judged (uncalibrated, or never recorded) do not vote
    either way. They cannot: no threshold means no verdict, and treating a
    missing verdict as a passing one is precisely how a "99%" comes to mean
    "best of a bad list".
    """
    judged = [a for a in assessments if a.get("state") in (LISTED, UNLISTED)]
    if not judged:
        has_scores = any(
            a.get("state") == UNCALIBRATED for a in assessments
        )
        state = UNCALIBRATED if has_scores else UNAVAILABLE
        return {"state": state, "judged_models": 0, "assessments": list(assessments)}
    unlisted = [a for a in judged if a["state"] == UNLISTED]
    return {
        "state": UNLISTED if len(unlisted) == len(judged) else LISTED,
        "judged_models": len(judged),
        "unlisted_models": len(unlisted),
        "assessments": list(assessments),
    }


def summarize_photo(match_rows, config=None):
    """Assess ``classifier_match_scores`` rows for one photo and summarize.

    A photo can carry several detections per model — a good crop and a bad one,
    plus the full-image pass — and each is its own run. Only each model's BEST
    run is judged: the question is whether that model ever got a good look at
    the subject, and a detector that produced a poor crop should not be able
    to report the photo as unidentifiable on that model's behalf.

    Rows without a score are skipped rather than scored as zero.
    """
    best = {}
    for row in match_rows or []:
        score = row.get("max_match_score")
        if score is None:
            continue
        model = row.get("classifier_model")
        if model not in best or score > best[model].get("max_match_score"):
            best[model] = row
    return summarize([
        assess(
            model,
            row.get("score_kind"),
            row.get("max_match_score"),
            row.get("match_margin"),
            config,
        )
        for model, row in best.items()
    ])


def is_unlisted(assessment):
    """True only for a run positively judged as matching nothing in its list.

    Uncalibrated and unavailable runs are *not* unlisted — absence of a verdict
    is not a verdict, and rendering it as one would recreate the overconfidence
    this module exists to remove.
    """
    return bool(assessment) and assessment.get("state") == UNLISTED


def _format_score(value, score_kind):
    """Render a raw score in the precision its scale deserves."""
    if value is None:
        return "n/a"
    if score_kind == COSINE:
        return f"{value:.3f}"
    return f"{value:.1f}"
