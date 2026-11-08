"""Which Highlights species buckets a photo belongs to.

One photo can carry several species: a keyword per species it is tagged
with, and a top prediction per detected subject. ``photo_species_memberships``
is the single rule for which buckets those open. The Highlights payload
builds its buckets with it, the confirm route picks the prediction to accept
with it, and ``get_highlights(eligible_only=True)`` keeps a saved highlight
only while its photo is still in that species' bucket. It has no database
access, so the repositories can use it without importing the payload module.

A prediction opens a bucket on the strength of its region, not of its
softmax: the detector's strongest box is the photo's primary subject, a
weaker box must clear ``SECONDARY_SUBJECT_FLOOR`` to suggest a second
species, a region the classifier scored like background opens nothing, and a
whole-frame classification (no box at all) only speaks when the photo has no
localized subject. A detector box on a tree trunk scoring 0.88 for an owl
no longer outranks the hawk box next to it at 0.52.
"""

from dataclasses import dataclass

from background_match import CONFIDENT_DETECTION, is_background_like
from keyword_normalization import keyword_match_key

#: A subject other than the photo's primary one suggests a second species
#: only when the detector was at least this sure it boxed an animal. Trunk
#: strips and foliage next to a bird land between the 0.2 workspace floor
#: and this line; a second real bird in frame usually clears it. It is the
#: same line above which the background gate trusts the detector.
SECONDARY_SUBJECT_FLOOR = CONFIDENT_DETECTION

FULL_IMAGE_DETECTOR = "full-image"


@dataclass(frozen=True)
class HighlightMembership:
    """One species bucket a candidate photo belongs to.

    ``accepted_name`` is the stored keyword name when the photo carries the
    species as a keyword (None for an unconfirmed prediction), and
    ``prediction`` the subject prediction behind the bucket, if any.
    ``whole_frame`` marks a prediction bucket opened by a whole-frame
    classification: the detector found no animal, so the species is a guess
    about the scene, not an identification of a region.
    """

    species: str
    accepted_name: str | None
    prediction: dict | None
    whole_frame: bool = False


def _field(subject, key):
    """``subject[key]`` or None: subjects arrive as dicts from the payload and
    as ``sqlite3.Row`` from the repositories, and a row has no ``get``."""
    if hasattr(subject, "get"):
        return subject.get(key)
    # ``key in row`` would search a Row's values; only ``keys()`` names columns.
    return subject[key] if key in subject.keys() else None  # noqa: SIM118


def subject_is_whole_frame(subject):
    """True for the synthetic full-image row a photo without boxes gets."""
    return _field(subject, "detector_model") == FULL_IMAGE_DETECTOR


def subject_is_background_like(subject):
    """True when the subject's classifier run matched background about as
    well as any listed species and the detector was not confident about
    the box (``background_match``)."""
    return is_background_like(
        _field(subject, "max_match_score"), _field(subject, "background_score"),
        _field(subject, "score_kind") or "cosine",
        detector_confidence=_field(subject, "detector_confidence"),
    )


def subject_evidence_order(subject):
    """Sort key putting the photo's best-evidenced region first.

    Localized boxes before the whole-frame row, regions the classifier
    scored as a subject before background-like ones, then the detector's
    confidence, then the prediction's confidence; ties fall to the newer
    prediction id. The repositories sort their per-detection rows with it
    so ``candidate_subjects(row)[0]`` is the primary subject everywhere.
    """
    return (
        subject_is_whole_frame(subject),
        subject_is_background_like(subject),
        -(_field(subject, "detector_confidence") or 0.0),
        -(_field(subject, "confidence") or 0.0),
        -(_field(subject, "prediction_id") or _field(subject, "id") or 0),
    )


def candidate_subjects(row):
    """The row's per-detection top predictions in ``subject_evidence_order``.

    Rows built without ``subject_predictions`` (older callers, hand-made
    test rows) fall back to their single top prediction.
    """
    subjects = row.get("subject_predictions")
    if subjects is not None:
        return sorted(
            (s for s in subjects if s.get("species")), key=subject_evidence_order,
        )
    if not row.get("predicted_species"):
        return []
    return [{
        "detection_id": None,
        "prediction_id": row.get("prediction_id"),
        "species": row["predicted_species"],
        "confidence": row.get("predicted_confidence"),
    }]


def bucket_opening_subjects(subjects):
    """The subjects whose top prediction may open a species bucket, primary first.

    ``subjects`` is in ``subject_evidence_order``. Background-like regions
    never open a bucket. Among the rest, the first localized box is the
    primary subject and always may; further boxes may only above
    ``SECONDARY_SUBJECT_FLOOR``; the whole-frame row may only when the
    photo has no localized subject left at all.
    """
    eligible = [s for s in subjects if not subject_is_background_like(s)]
    localized = [s for s in eligible if not subject_is_whole_frame(s)]
    if localized:
        primary, *others = localized
        return [primary] + [
            s for s in others
            if (s.get("detector_confidence") or 0.0) >= SECONDARY_SUBJECT_FLOOR
        ]
    return [s for s in eligible if subject_is_whole_frame(s)][:1]


def photo_species_memberships(
    row, canonicalize_species=None, confidence_threshold=0.0,
):
    """Every species bucket one ``get_highlights_candidates`` row joins.

    A photo joins the bucket of each species keyword it carries (confirmed),
    and of the top prediction of each bucket-opening subject
    (:func:`bucket_opening_subjects`) at or above ``confidence_threshold``
    that names a species it does not carry yet (unconfirmed). A photo whose
    keywords match none of its subjects' predictions is one the user has
    already identified against the model, so its predictions open no bucket,
    the same rule ID Conflicts uses for an "additional species": a subject
    only suggests a second species when another subject accounts for the
    keyword.
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
    subjects = candidate_subjects(row)
    keyed = [
        (keyword_match_key(canonical(s["species"])), canonical(s["species"]), s)
        for s in subjects
    ]
    best_by_key = {}
    for key, _bucket, subject in keyed:
        best_by_key.setdefault(key, subject)
    top = subjects[0] if subjects else None
    for name in names:
        bucket = canonical(name)
        key = keyword_match_key(bucket)
        if key in accepted_keys:
            continue
        accepted_keys.add(key)
        memberships.append(HighlightMembership(
            bucket, name, best_by_key.get(key) or top,
        ))
    if accepted_keys and not any(key in accepted_keys for key, _, _ in keyed):
        return memberships
    seen = set(accepted_keys)
    opening = {id(s) for s in bucket_opening_subjects(subjects)}
    for key, bucket, subject in keyed:
        confidence = subject.get("confidence")
        if (id(subject) not in opening or key in seen or confidence is None
                or confidence < confidence_threshold):
            continue
        seen.add(key)
        memberships.append(HighlightMembership(
            bucket, None, subject, whole_frame=subject_is_whole_frame(subject),
        ))
    return memberships
