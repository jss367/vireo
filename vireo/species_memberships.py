"""Which Highlights species buckets a photo belongs to.

One photo can carry several species: a keyword per species it is tagged
with, and a top prediction per detected subject. ``photo_species_memberships``
is the single rule for which buckets those open. The Highlights payload
builds its buckets with it, the confirm route picks the prediction to accept
with it, and ``get_highlights(eligible_only=True)`` keeps a saved highlight
only while its photo is still in that species' bucket. It has no database
access, so the repositories can use it without importing the payload module.

A prediction opens a bucket on the strength of its region, not of its
softmax. The photo's primary subject is the region the lightbox, the subject
panel and the analysis stages treat as its subject
(``repositories.subject_order.primary_order_sql``, which the repositories
apply in SQL and hand over as ``subject_rank``). Any other box must clear
``SECONDARY_SUBJECT_FLOOR`` and must not span the frame to suggest a second
species, a region the classifier scored like background opens nothing unless
the user chose it, and a whole-frame classification (no box at all) only
speaks when the photo has no localized subject. A strip of tree trunk the
detector scored 0.47 therefore does not name a photo whose hawk box it
scored 0.37: the strip runs the frame's full height and the hawk crop
matches its species label better.
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

#: A box this tall or wide (as a fraction of the frame) ranks after every
#: other box when the primary subject is chosen and never suggests a second
#: species. The detector boxes tree trunks and out-of-focus foreground as
#: full-height strips beside the bird, often above
#: ``SECONDARY_SUBJECT_FLOOR``; an animal that fills the frame is still the
#: subject when it is the only box.
FRAME_SPANNING_EXTENT = 0.95

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
    the box (``background_match``). A whole-frame row is always judged by
    the gap: it has no detector box for the override to trust, and its
    stored confidence is not constrained by the schema."""
    return is_background_like(
        _field(subject, "max_match_score"), _field(subject, "background_score"),
        _field(subject, "score_kind") or "cosine",
        detector_confidence=_field(subject, "detector_confidence"),
        whole_frame=subject_is_whole_frame(subject),
    )


def subject_spans_frame(subject):
    """True for a box reaching ``FRAME_SPANNING_EXTENT`` of the frame's
    height or width. Rows without box dimensions never do."""
    return max(
        _field(subject, "box_w") or 0.0, _field(subject, "box_h") or 0.0,
    ) >= FRAME_SPANNING_EXTENT


def subject_is_chosen(subject):
    """True for the region the user picked as the photo's subject."""
    return bool(_field(subject, "subject_chosen"))


def subject_evidence_order(subject):
    """Sort key putting the photo's primary subject first.

    Rows from the repositories carry ``subject_rank``, the photo's
    ``primary_order_sql`` position, so Highlights and the lightbox always
    agree on the subject. Rows without it (hand-built rows, older callers)
    fall back to the same rule as far as their fields allow: localized
    boxes before the whole-frame row, the user's chosen region, then
    regions that do not span the frame, regions the classifier scored as a
    subject before background-like ones, then the detector's confidence,
    then the prediction's confidence, ties falling to the newer prediction
    id.
    """
    rank = _field(subject, "subject_rank")
    return (
        rank is None,
        rank or 0,
        subject_is_whole_frame(subject),
        not subject_is_chosen(subject),
        subject_spans_frame(subject),
        subject_is_background_like(subject),
        -(_field(subject, "detector_confidence") or 0.0),
        -(_field(subject, "confidence") or 0.0),
        -(_field(subject, "prediction_id") or _field(subject, "id") or 0),
    )


def subject_from_row(row):
    """A per-detection prediction row (``top_unrejected_by_detection``) as
    ``photo_species_memberships`` reads it: the prediction plus the region
    evidence the rule ranks by."""
    return {
        "detection_id": row["detection_id"],
        "prediction_id": row["id"],
        "species": row["species"],
        "confidence": row["confidence"],
        "detector_confidence": row["detector_confidence"],
        "detector_model": row["detector_model"],
        "max_match_score": row["max_match_score"],
        "background_score": row["background_score"],
        "score_kind": row["score_kind"],
        "subject_rank": row["subject_rank"],
        "subject_chosen": bool(row["subject_chosen"]),
        "box_w": row["box_w"],
        "box_h": row["box_h"],
    }


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
    never open a bucket unless the user chose the region as the subject.
    Among the rest, the first localized box is the primary subject and
    always may; further boxes may only above ``SECONDARY_SUBJECT_FLOOR``
    and only when they do not span the frame (``subject_spans_frame``); the
    whole-frame row may only when the photo has no localized subject left
    at all.
    """
    eligible = [
        s for s in subjects
        if subject_is_chosen(s) or not subject_is_background_like(s)
    ]
    localized = [s for s in eligible if not subject_is_whole_frame(s)]
    if localized:
        primary, *others = localized
        return [primary] + [
            s for s in others
            if (s.get("detector_confidence") or 0.0) >= SECONDARY_SUBJECT_FLOOR
            and not subject_spans_frame(s)
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


def explain_subject_roles(row, canonicalize_species=None):
    """``{detection_id: (role, species)}``: what Highlights does with each
    subject of one ``get_highlights_candidates``-shaped row, for the Pipeline
    inspector. Mirrors :func:`photo_species_memberships` (at confidence
    threshold 0) and says why a region opens no bucket:

    ``tagged`` — its species matches a species the photo is tagged with;
    ``bucket`` — it is the primary subject and opens ``species``'s bucket;
    ``second_species`` — a second subject that opens ``species``'s bucket;
    ``same_species`` — it names a species another region already opened;
    ``tags_disagree`` — the photo's tags match no region, so no prediction
    opens a bucket; ``whole_frame_unused`` — a whole-frame guess on a photo
    with a localized subject; ``background`` — it matched background scenery
    about as well as any species; ``below_floor`` — a second region under
    ``SECONDARY_SUBJECT_FLOOR``; ``spans_frame`` — a second region spanning
    the frame.
    """
    def canonical(name):
        if canonicalize_species is None:
            return name
        return canonicalize_species(name) or name

    names = row.get("species_names") or []
    tag_keys = {keyword_match_key(canonical(n)) for n in names}
    subjects = candidate_subjects(row)
    keys = [(s, keyword_match_key(canonical(s["species"]))) for s in subjects]
    opened = {
        id(m.prediction): m.species
        for m in photo_species_memberships(row, canonicalize_species)
        if m.accepted_name is None and m.prediction is not None
    }
    opening = {id(s) for s in bucket_opening_subjects(subjects)}
    tags_disagree = bool(tag_keys) and not any(k in tag_keys for _, k in keys)
    localized = [s for s in subjects if not subject_is_whole_frame(s)]
    primary = localized[0] if localized else None
    roles = {}
    for subject, key in keys:
        species = canonical(subject["species"])
        if key in tag_keys:
            role = ("tagged", species)
        elif id(subject) in opened:
            role = ("bucket" if subject is primary or primary is None else "second_species", opened[id(subject)])
        elif tags_disagree:
            role = ("tags_disagree", None)
        elif id(subject) in opening:
            role = ("same_species", species)
        elif subject_is_whole_frame(subject):
            role = ("whole_frame_unused", None)
        elif subject_is_background_like(subject) and not subject_is_chosen(subject):
            role = ("background", None)
        elif (subject.get("detector_confidence") or 0.0) < SECONDARY_SUBJECT_FLOOR:
            role = ("below_floor", None)
        else:
            role = ("spans_frame", None)
        roles[subject["detection_id"]] = role
    return roles
