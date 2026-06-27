"""Lock the canonical render-source primitives shared across flows.

These encode the exact edge cases that previously drifted between per-flow
copies in app.py, pipeline_job.py, thumbnails.py, export.py and scanner.py:
both-axis (not long-edge-only) dimension checks, the embedded-preview "tie"
case, EXIF-orientation axis swaps, and the request-path (1px) vs scanner (1%)
undersize tolerances.
"""

import json
import os
import sys

from PIL import Image

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import render_source as rs


def _img(w, h):
    return Image.new("RGB", (w, h), "gray")


def test_is_undersized_checks_both_axes():
    # A 6000x3376 embedded preview for a 6000x4000 source matches on the long
    # edge but loses short-edge content — a long-edge-only check would miss it.
    assert rs.is_undersized(6000, 3376, 6000, 4000) is True
    assert rs.is_undersized(6000, 4000, 6000, 4000) is False


def test_is_undersized_unknown_expected_is_never_undersized():
    assert rs.is_undersized(10, 10, 0, 0) is False
    assert rs.is_undersized(10, 10, 6000, 0) is False


def test_is_undersized_default_abs_slack_absorbs_one_px():
    # Request paths tolerate 1px of rounding between decoder output and stored
    # dimensions, but not more.
    assert rs.is_undersized(5999, 3999, 6000, 4000) is False
    assert rs.is_undersized(5998, 4000, 6000, 4000) is True


def test_is_undersized_relative_tolerance_for_scanner():
    # Scanner uses a 1% relative tolerance (abs_slack=0): libraw can emit the
    # active image area a few px narrower than the full sensor.
    assert rs.is_undersized(
        5940, 3960, 6000, 4000, abs_slack=0, rel_slack=0.01,
    ) is False
    assert rs.is_undersized(
        5939, 3960, 6000, 4000, abs_slack=0, rel_slack=0.01,
    ) is True


def test_companion_replaces_undersized_decode_on_tie():
    # img tied the source on the long edge but is short on the other axis; a
    # full-size companion should replace it.
    img = _img(6000, 3376)
    companion = _img(6000, 4000)
    assert rs.companion_image_can_replace_raw_result(
        companion, img, 6000, 4000,
    ) is True


def test_companion_rejected_when_also_undersized():
    img = _img(6000, 3376)
    companion = _img(4000, 3000)  # bigger than img but still < expected
    assert rs.companion_image_can_replace_raw_result(
        companion, img, 6000, 4000,
    ) is False


def test_companion_none_never_replaces():
    assert rs.companion_image_can_replace_raw_result(
        None, _img(10, 10), 6000, 4000,
    ) is False


def test_companion_with_unknown_expected_covers_current():
    # When expected dims are unknown, the companion must cover the current
    # decode on both axes; a None current decode is always replaceable.
    assert rs.companion_image_can_replace_raw_result(
        _img(800, 600), None, 0, 0,
    ) is True
    assert rs.companion_image_can_replace_raw_result(
        _img(800, 600), _img(800, 600), 0, 0,
    ) is True
    assert rs.companion_image_can_replace_raw_result(
        _img(799, 600), _img(800, 600), 0, 0,
    ) is False


def test_recipe_source_dimensions_swaps_for_orientation():
    photo = {
        "width": 6000,
        "height": 4000,
        "exif_data": json.dumps({"EXIF": {"Orientation": 6}}),
    }
    assert rs.recipe_source_dimensions(photo) == (4000, 6000)


def test_recipe_source_dimensions_no_swap_without_orientation():
    photo = {"width": 6000, "height": 4000, "exif_data": None}
    assert rs.recipe_source_dimensions(photo) == (6000, 4000)


def test_recipe_source_dimensions_explicit_exif_overrides_row():
    # export.py passes exif_data separately rather than from the row.
    photo = {"width": 6000, "height": 4000}
    assert rs.recipe_source_dimensions(
        photo, json.dumps({"EXIF": {"Orientation": 6}}),
    ) == (4000, 6000)


def test_scaled_recipe_source_dimensions_scales_long_edge():
    photo = {"width": 6000, "height": 4000, "exif_data": None}
    assert rs.scaled_recipe_source_dimensions(photo, 3000) == (3000, 2000)
    assert rs.scaled_recipe_source_dimensions(photo, None) == (6000, 4000)
