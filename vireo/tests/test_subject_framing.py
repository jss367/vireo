"""Crops that frame one subject box."""

import random

import pytest
from subject_framing import frame_subject, parse_aspect

FRAME = (6000, 4000)


def _box(x, y, w, h):
    return {"x": x, "y": y, "w": w, "h": h}


def test_crop_pads_the_box_and_keeps_the_shape():
    out = frame_subject(_box(.4, .4, .1, .1), FRAME, 0.5, (3.0, 2.0))
    # A 600x400 subject padded by half on each side is 1200x800.
    assert out["output_pixels"] == [1200, 800] and out["subject_pixels"] == [600, 400]
    assert out["crop"] == {"x": .35, "y": .35, "w": .2, "h": .2}
    assert out["margin_reduced"] is False and out["subject_cut"] is False


@pytest.mark.parametrize(("box", "aspect"), [(_box(.4, .4, .2, .1), "3:2"), (_box(.4, .4, .05, .2), "4:5")])
def test_auto_shape_follows_the_subjects_pixels(box, aspect):
    out = frame_subject(box, FRAME, 0.2)
    assert out["aspect"] == aspect
    width, height = out["output_pixels"]
    ratio_w, ratio_h = (float(v) for v in aspect.split(":"))
    assert width / height == pytest.approx(ratio_w / ratio_h, rel=0.01)


def test_shape_uses_the_upright_frame():
    """The same fractions are wide in a landscape frame and tall in a portrait one."""
    box = _box(.4, .4, .12, .12)
    assert frame_subject(box, (6000, 4000), 0.2)["aspect"] == "3:2"
    assert frame_subject(box, (4000, 6000), 0.2)["aspect"] == "4:5"


def test_crop_slides_to_stay_inside_the_frame():
    crop = frame_subject(_box(0, .9, .1, .1), FRAME, 0.5, (1.0, 1.0))["crop"]
    assert crop["x"] == 0 and crop["y"] + crop["h"] == pytest.approx(1)


def test_crop_reports_lost_padding_and_a_cut_subject():
    padded = frame_subject(_box(.05, .05, .9, .9), FRAME, 0.35, (3.0, 2.0))
    assert padded["margin_reduced"] is True and padded["subject_cut"] is False
    assert padded["crop"] == {"x": 0, "y": 0, "w": 1, "h": 1}
    cut = frame_subject(_box(.05, .05, .9, .9), FRAME, 0, (1.0, 1.0))
    assert cut["subject_cut"] is True and cut["output_pixels"] == [4000, 4000]


def test_every_crop_is_a_valid_recipe():
    from edit_recipe import normalize_recipe

    rng = random.Random(1)
    for _ in range(5000):
        w, h = rng.uniform(.001, 1), rng.uniform(.001, 1)
        box = _box(rng.uniform(0, 1 - w), rng.uniform(0, 1 - h), w, h)
        frame = (rng.randint(200, 9000), rng.randint(200, 9000))
        crop = frame_subject(box, frame, rng.choice([0, .35, 5]), rng.choice([None, (1.0, 1.0), (16.0, 9.0)]))["crop"]
        normalize_recipe({"crop": crop})


@pytest.mark.parametrize("box", [
    # Both the start and the size of these land on a six-place midpoint.
    _box(.9989985, .2, .0010015, .0010015), _box(.2, .9989985, .0010015, .0010015),
    _box(.9999999, .9999999, .0000001, .0000001),
])
def test_a_crop_at_the_frames_edge_is_a_valid_recipe(box):
    from edit_recipe import normalize_recipe

    for aspect in (None, (1.0, 1.0), (100.0, 1.0)):
        crop = frame_subject(box, FRAME, 0, aspect)["crop"]
        assert crop["w"] > 0 and crop["h"] > 0
        assert normalize_recipe({"crop": crop})["crop"] == crop


@pytest.mark.parametrize(("box", "frame"), [(_box(0, 0, 0, .1), FRAME), (_box(0, 0, .1, .1), (0, 0))])
def test_unusable_geometry_is_refused(box, frame):
    with pytest.raises(ValueError, match="unusable"):
        frame_subject(box, frame, 0.35)


def test_parse_aspect():
    assert parse_aspect(None) is None and parse_aspect("AUTO") is None
    assert parse_aspect("16:9") == (16.0, 9.0)
    for bad in ("3", "a:b", "0:1", "nan:1", "1:2:3", "-3:2", "1e-300:1e300", "1e300:1e-300", "1:1000"):
        with pytest.raises(ValueError, match="aspect must be"):
            parse_aspect(bad)
