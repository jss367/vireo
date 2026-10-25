"""Which Highlights species buckets a photo belongs to.

One photo can carry several species: a keyword per species it is tagged
with, and a top prediction per detected subject. ``photo_species_memberships``
is the single rule for which buckets those open. The Highlights payload
builds its buckets with it, the confirm route picks the prediction to accept
with it, and ``get_highlights(eligible_only=True)`` keeps a saved highlight
only while its photo is still in that species' bucket. It has no database
access, so the repositories can use it without importing the payload module.
"""

from dataclasses import dataclass

from keyword_normalization import keyword_match_key


@dataclass(frozen=True)
class HighlightMembership:
    """One species bucket a candidate photo belongs to.

    ``accepted_name`` is the stored keyword name when the photo carries the
    species as a keyword (None for an unconfirmed prediction), and
    ``prediction`` the subject prediction behind the bucket, if any.
    """

    species: str
    accepted_name: str | None
    prediction: dict | None


def candidate_subjects(row):
    """The row's per-detection top predictions, highest confidence first.

    Rows built without ``subject_predictions`` (older callers, hand-made
    test rows) fall back to their single top prediction.
    """
    subjects = row.get("subject_predictions")
    if subjects is not None:
        return [s for s in subjects if s.get("species")]
    if not row.get("predicted_species"):
        return []
    return [{
        "detection_id": None,
        "prediction_id": row.get("prediction_id"),
        "species": row["predicted_species"],
        "confidence": row.get("predicted_confidence"),
    }]


def photo_species_memberships(
    row, canonicalize_species=None, confidence_threshold=0.0,
):
    """Every species bucket one ``get_highlights_candidates`` row joins.

    A photo joins the bucket of each species keyword it carries (confirmed),
    and of each subject's top prediction at or above
    ``confidence_threshold`` that names a species it does not carry yet
    (unconfirmed). A photo whose keywords match none of its subjects'
    predictions is one the user has already identified against the model,
    so its predictions open no bucket, the same rule ID Conflicts uses for an
    "additional species": a subject only suggests a second species when
    another subject accounts for the keyword.
    """
    def canonical(name):
        if canonicalize_species is None:
            return name
        return canonicalize_species(name) or name

    names = row.get("species_names")
    if names is None:
        names = [row["species"]] if row.get("species") else []
    memberships = []
    accepted_keys = set()
    subjects = [
        (keyword_match_key(canonical(s["species"])), canonical(s["species"]), s)
        for s in candidate_subjects(row)
    ]
    best_by_key = {}
    for key, _bucket, subject in subjects:
        best_by_key.setdefault(key, subject)
    top = subjects[0][2] if subjects else None
    for name in names:
        bucket = canonical(name)
        key = keyword_match_key(bucket)
        if key in accepted_keys:
            continue
        accepted_keys.add(key)
        memberships.append(HighlightMembership(
            bucket, name, best_by_key.get(key) or top,
        ))
    if accepted_keys and not any(key in accepted_keys for key, _, _ in subjects):
        return memberships
    seen = set(accepted_keys)
    for key, bucket, subject in subjects:
        confidence = subject.get("confidence")
        if key in seen or confidence is None or confidence < confidence_threshold:
            continue
        seen.add(key)
        memberships.append(HighlightMembership(bucket, None, subject))
    return memberships
