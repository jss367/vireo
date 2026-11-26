"""How each species is present in a photo that carries more than one.

A flag, a rating and the quality score belong to the photo, but a photo of a
flock is a good photo of the bird it was framed on and a poor one of the
birds behind it. ``species_presence`` says, for each species bucket a photo
is in (``species_memberships.photo_species_memberships``), which of three
things holds:

``SUBJECT``
    the species is what the photo is a picture of;
``IN_FRAME``
    another bird is the subject, and this species is elsewhere in the frame
    (or Vireo found no region for it at all);
``OUTSIDE_CROP``
    every region Vireo matched to this species lies outside the photo's
    crop, so the picture the user made does not show it.

Highlights counts a pick, and ranks by the photo's score, only where the
species is the subject; the other two sort after and say so on the card.

The photo's main bird is its primary subject
(``species_memberships.primary_subject``), or, when the photo is cropped,
the first region in subject order whose box center is inside the crop: a
crop is the user saying which bird the picture is of. A species is a subject
when one of its regions is that bird, or is in view and at least
``CO_SUBJECT_AREA_RATIO`` of its box area, so two birds side by side are
both subjects and a similar-sized flock is a photo of every species in it.

The rule only demotes on evidence. Every species stays ``SUBJECT`` when the
photo has one species, has no localized region, has a main bird whose
prediction matches none of its species (the user identified it against the
model, so Vireo cannot tell which bird is which), or is cropped and a
region's position is unknown. A species the user tagged is demoted only when
the main bird matches another species they tagged: an unconfirmed prediction
for the main bird is the model's guess, and it does not outrank a tag. It
also stays the subject while a co-subject-sized region in view matches none
of the photo's species: the classifier often names a lookalike, so that
region may be the tagged species. A recipe that also corrects lens
distortion (the manual amount or the lens profile) resamples the frame
before the crop with a warp this module does not reproduce, so its crop
demotes nothing.

Pure: no database, config or I/O.
"""

import math
from dataclasses import dataclass

from species_memberships import (
    candidate_subjects,
    primary_subject,
    species_match_key,
    subject_is_background_like,
    subject_is_chosen,
    subject_is_whole_frame,
)

SUBJECT = "subject"
IN_FRAME = "in_frame"
OUTSIDE_CROP = "outside_crop"

#: A second region counts as a subject of the photo when its box is at least
#: this share of the main bird's box area. Half the area is a bird about 70%
#: as long: clearly a co-subject of a two-bird frame, while a bird a third
#: the length (a ninth of the area) is background to it.
CO_SUBJECT_AREA_RATIO = 0.5


@dataclass(frozen=True)
class Presence:
    """One species in one photo.

    ``frame_share`` is the box area, as a fraction of the uncropped frame,
    of the species' largest region in view. It is None when Vireo matched no
    region to the species or none is in view, and it is the one per-species
    measurement Highlights has to order photos where the species is not the
    subject.
    """

    kind: str
    frame_share: float | None = None


def crop_geometry(recipe):
    """The parts of a normalized edit recipe that decide what its crop
    shows, None for a photo with no crop.

    ``warped`` is set when the recipe corrects lens distortion: the renderer
    resamples the frame radially before the rotation and crop
    (``optics.distort_image``), so a region's source position no longer
    says where it sits in the cropped picture.
    """
    crop = (recipe or {}).get("crop")
    if not crop:
        return None
    adjustments = recipe.get("adjustments") or {}
    return {
        "crop": crop,
        "warped": bool(
            adjustments.get("distortion") or adjustments.get("lens_profile")
        ),
        "rotation": recipe.get("rotation", 0),
        "flip": recipe.get("flip") or {},
        "straighten": recipe.get("straighten", 0),
    }


def edited_point(x, y, geometry, size):
    """Where the source-frame point ``(x, y)`` lands in the frame the crop
    is measured in, both as fractions of their frame.

    Mirrors ``image_edits._apply_geometry``: a clockwise right-angle
    rotation, the flips, then a clockwise straighten about the center that
    keeps the frame's dimensions. ``size`` is the source ``(width, height)``;
    only its aspect ratio matters, and only for the straighten.
    """
    width, height = size
    rotation = geometry.get("rotation") or 0
    if rotation == 90:
        x, y, width, height = 1.0 - y, x, height, width
    elif rotation == 180:
        x, y = 1.0 - x, 1.0 - y
    elif rotation == 270:
        x, y, width, height = y, 1.0 - x, height, width
    flip = geometry.get("flip") or {}
    if flip.get("horizontal"):
        x = 1.0 - x
    if flip.get("vertical"):
        y = 1.0 - y
    angle = math.radians(geometry.get("straighten") or 0.0)
    if angle:
        dx, dy = (x - 0.5) * width, (y - 0.5) * height
        cos, sin = math.cos(angle), math.sin(angle)
        x = 0.5 + (dx * cos - dy * sin) / width
        y = 0.5 + (dx * sin + dy * cos) / height
    return x, y


def _frame_size(row):
    """The photo's ``(width, height)``, square when the catalog has none:
    the right-angle rotations and flips do not need it, and a straighten is
    at most 45 degrees of a point near the middle of its crop."""
    width, height = row.get("width") or 0, row.get("height") or 0
    return (width, height) if width > 0 and height > 0 else (1.0, 1.0)


def _center_in_crop(subject, geometry, size):
    x, y = edited_point(
        subject["box_x"] + subject["box_w"] / 2.0,
        subject["box_y"] + subject["box_h"] / 2.0,
        geometry, size,
    )
    crop = geometry["crop"]
    return (
        crop["x"] <= x <= crop["x"] + crop["w"]
        and crop["y"] <= y <= crop["y"] + crop["h"]
    )


def _has_box(subject):
    return all(
        subject.get(key) is not None
        for key in ("box_x", "box_y", "box_w", "box_h")
    )


def _area(subject):
    return (subject.get("box_w") or 0.0) * (subject.get("box_h") or 0.0)


def species_presence(row, memberships, canonicalize_species=None):
    """``{species: Presence}`` for every bucket ``memberships`` puts the
    photo in.

    ``row`` is a ``get_highlights_candidates`` row: its
    ``subject_predictions`` carry each region's box, and ``crop_geometry``
    (``crop_geometry(recipe)``) is set when the photo is cropped.
    """
    names = [m.species for m in memberships]
    presence = {name: Presence(SUBJECT) for name in names}
    if len(names) < 2:
        return presence

    def key_of(subject):
        name = subject["species"]
        if canonicalize_species is not None:
            name = canonicalize_species(name) or name
        return species_match_key(name)

    subjects = [
        s for s in candidate_subjects(row)
        if not subject_is_whole_frame(s)
        and (subject_is_chosen(s) or not subject_is_background_like(s))
    ]
    geometry = row.get("crop_geometry")
    if geometry:
        if geometry.get("warped") or not all(_has_box(s) for s in subjects):
            return presence
        size = _frame_size(row)
        in_view = [s for s in subjects if _center_in_crop(s, geometry, size)]
        main = in_view[0] if in_view else None
    else:
        in_view = subjects
        main = primary_subject(subjects)
    main_key = key_of(main) if main is not None else None
    if main_key not in {species_match_key(n) for n in names}:
        return presence
    tagged = {
        species_match_key(m.species) for m in memberships
        if m.accepted_name is not None
    }

    viewed = {id(s) for s in in_view}
    floor = CO_SUBJECT_AREA_RATIO * _area(main)
    bucket_keys = {species_match_key(n) for n in names}
    unnamed_co_subject = any(
        key_of(s) not in bucket_keys and floor > 0 and _area(s) >= floor
        for s in in_view
    )
    for name in names:
        key = species_match_key(name)
        if key in tagged and (main_key not in tagged or unnamed_co_subject):
            continue
        located = [s for s in subjects if key_of(s) == key]
        shown = [s for s in located if id(s) in viewed]
        share = max((_area(s) for s in shown), default=None)
        if any(s is main or (floor > 0 and _area(s) >= floor) for s in shown):
            presence[name] = Presence(SUBJECT, share)
        elif geometry and located and not shown:
            presence[name] = Presence(OUTSIDE_CROP)
        else:
            presence[name] = Presence(IN_FRAME, share)
    return presence
