"""Which species a multi-species photo is a picture of (``species_presence``)."""

import pytest
from PIL import Image
from species_memberships import photo_species_memberships
from species_presence import (
    IN_FRAME,
    OUTSIDE_CROP,
    SUBJECT,
    crop_geometry,
    edited_point,
    species_presence,
)


def _subject(species, box, rank, detector_confidence=0.6):
    x, y, w, h = box
    return {
        "detection_id": rank, "prediction_id": rank, "species": species,
        "confidence": 0.9, "detector_confidence": detector_confidence,
        "detector_model": "megadetector-v6", "subject_rank": rank,
        "box_x": x, "box_y": y, "box_w": w, "box_h": h,
    }


def _presence(names, subjects, crop=None, **geometry):
    row = {
        "species_names": list(names), "subject_predictions": list(subjects),
        "width": 5408, "height": 3608,
        "crop_geometry": crop_geometry({"crop": crop, **geometry}) if crop else None,
    }
    return species_presence(row, photo_species_memberships(row))


def _kinds(presence):
    return {name: value.kind for name, value in presence.items()}


DUNLIN = _subject("Dunlin", (0.595, 0.562, 0.016, 0.036), rank=2)
PLOVER = _subject("Black-bellied Plover", (0.673, 0.407, 0.042, 0.056), rank=1)
SMALL_PLOVER = _subject("Black-bellied Plover", (0.539, 0.516, 0.018, 0.033), rank=3)


def test_one_species_is_always_the_subject():
    presence = _presence(["Dunlin"], [DUNLIN])
    assert _kinds(presence) == {"Dunlin": SUBJECT}


def test_an_unconfirmed_prediction_never_demotes_a_tagged_species():
    """The main bird reads as a plover the user has not confirmed. The
    dunlin tag is the user's, so the photo stays a dunlin photo, while the
    plover prediction keeps the bucket its own region opened."""
    presence = _presence(["Dunlin"], [PLOVER, DUNLIN])
    assert _kinds(presence) == {
        "Dunlin": SUBJECT, "Black-bellied Plover": SUBJECT,
    }


def test_an_unconfirmed_second_bird_is_only_in_frame():
    dunlin = _subject("Dunlin", (0.3, 0.3, 0.4, 0.4), rank=1)
    plover = _subject("Black-bellied Plover", (0.8, 0.1, 0.1, 0.1), rank=2)
    presence = _presence(["Dunlin"], [dunlin, plover])
    assert _kinds(presence) == {
        "Dunlin": SUBJECT, "Black-bellied Plover": IN_FRAME,
    }


def test_crop_on_one_bird_leaves_the_other_species_outside_it():
    """The flock frame cropped to a dunlin: every plover box is outside."""
    presence = _presence(
        ["Black-bellied plover", "Dunlin"], [PLOVER, DUNLIN, SMALL_PLOVER],
        crop={"x": 0.54, "y": 0.55, "w": 0.16, "h": 0.12},
    )
    assert _kinds(presence) == {
        "Black-bellied plover": OUTSIDE_CROP, "Dunlin": SUBJECT,
    }
    assert presence["Black-bellied plover"].frame_share is None
    assert presence["Dunlin"].frame_share == pytest.approx(0.016 * 0.036)


def test_uncropped_similar_sized_birds_are_both_subjects():
    plover = _subject("Black-bellied Plover", (0.2, 0.3, 0.3, 0.3), rank=1)
    dunlin = _subject("Dunlin", (0.6, 0.3, 0.25, 0.25), rank=2)
    presence = _presence(["Black-bellied plover", "Dunlin"], [plover, dunlin])
    assert _kinds(presence) == {
        "Black-bellied plover": SUBJECT, "Dunlin": SUBJECT,
    }


def test_a_much_smaller_second_bird_is_only_in_frame():
    dunlin = _subject("Dunlin", (0.3, 0.3, 0.4, 0.4), rank=1)
    plover = _subject("Black-bellied Plover", (0.8, 0.1, 0.1, 0.1), rank=2)
    presence = _presence(["Black-bellied plover", "Dunlin"], [dunlin, plover])
    assert _kinds(presence) == {
        "Black-bellied plover": IN_FRAME, "Dunlin": SUBJECT,
    }
    assert presence["Black-bellied plover"].frame_share == pytest.approx(0.01)


def test_both_species_inside_the_crop_at_similar_size_are_subjects():
    dunlin = _subject("Dunlin", (0.30, 0.30, 0.10, 0.10), rank=2)
    plover = _subject("Black-bellied Plover", (0.45, 0.30, 0.10, 0.10), rank=3)
    far = _subject("Willet", (0.90, 0.90, 0.05, 0.05), rank=1)
    presence = _presence(
        ["Black-bellied plover", "Dunlin", "Willet"], [far, dunlin, plover],
        crop={"x": 0.25, "y": 0.25, "w": 0.35, "h": 0.2},
    )
    assert _kinds(presence) == {
        "Black-bellied plover": SUBJECT, "Dunlin": SUBJECT, "Willet": OUTSIDE_CROP,
    }


def test_a_species_with_no_region_is_in_frame_when_the_main_bird_is_another():
    dunlin = _subject("Dunlin", (0.3, 0.3, 0.4, 0.4), rank=1)
    presence = _presence(["Black-bellied plover", "Dunlin"], [dunlin])
    assert _kinds(presence) == {
        "Black-bellied plover": IN_FRAME, "Dunlin": SUBJECT,
    }
    assert presence["Black-bellied plover"].frame_share is None


@pytest.mark.parametrize("subjects, crop", [
    # The main bird's prediction matches neither tag: the user identified it
    # against the model, so which bird is which is unknown.
    ([_subject("Sanderling", (0.3, 0.3, 0.4, 0.4), rank=1)], None),
    # No region at all.
    ([], None),
    # A whole-frame guess is not a located bird.
    ([{**_subject("Dunlin", (0, 0, 1, 1), rank=1), "detector_model": "full-image"}], None),
    # Cropped where the detector found nothing.
    ([PLOVER, DUNLIN], {"x": 0.0, "y": 0.0, "w": 0.2, "h": 0.2}),
    # Cropped, with a region whose position is unknown.
    ([PLOVER, {**DUNLIN, "box_x": None}], {"x": 0.54, "y": 0.55, "w": 0.16, "h": 0.12}),
])
def test_without_evidence_every_species_stays_the_subject(subjects, crop):
    presence = _presence(["Black-bellied plover", "Dunlin"], subjects, crop=crop)
    assert _kinds(presence) == {
        "Black-bellied plover": SUBJECT, "Dunlin": SUBJECT,
    }


def test_rotation_is_applied_before_the_crop():
    """Rotated 90° clockwise, the source's top-right lands bottom-right."""
    dunlin = _subject("Dunlin", (0.85, 0.05, 0.1, 0.1), rank=2)
    plover = _subject("Black-bellied Plover", (0.05, 0.05, 0.1, 0.1), rank=1)
    presence = _presence(
        ["Black-bellied plover", "Dunlin"], [plover, dunlin],
        crop={"x": 0.7, "y": 0.7, "w": 0.3, "h": 0.3}, rotation=90,
    )
    assert _kinds(presence) == {
        "Black-bellied plover": OUTSIDE_CROP, "Dunlin": SUBJECT,
    }


@pytest.mark.parametrize("geometry", [
    {"rotation": 90},
    {"rotation": 180},
    {"rotation": 270},
    {"flip": {"horizontal": True}},
    {"flip": {"vertical": True}},
    {"straighten": 12.0},
    {"straighten": -20.0},
    {"rotation": 90, "flip": {"horizontal": True}, "straighten": 8.0},
])
def test_edited_point_follows_the_renderer(geometry):
    """A marked pixel lands where ``edited_point`` says it does."""
    from image_edits import _apply_geometry

    width, height = 300, 200
    px, py = 210, 60
    image = Image.new("L", (width, height), 0)
    for dx in range(-3, 4):
        for dy in range(-3, 4):
            image.putpixel((px + dx, py + dy), 255)
    rendered = _apply_geometry(image, geometry)
    out_w, out_h = rendered.size
    bright = [
        (x, y) for y in range(out_h) for x in range(out_w)
        if rendered.getpixel((x, y)) > 128
    ]
    cx = sum(x for x, _ in bright) / len(bright)
    cy = sum(y for _, y in bright) / len(bright)

    x, y = edited_point(
        (px + 0.5) / width, (py + 0.5) / height, geometry, (width, height),
    )

    assert x * out_w == pytest.approx(cx + 0.5, abs=1.5)
    assert y * out_h == pytest.approx(cy + 0.5, abs=1.5)
