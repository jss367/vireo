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


def test_rendered_recipe_dimensions_no_recipe():
    assert rs.rendered_recipe_dimensions(6000, 4000, None) == (6000.0, 4000.0)


def test_rendered_recipe_dimensions_swaps_for_right_angle_rotation():
    assert rs.rendered_recipe_dimensions(6000, 4000, {"rotation": 90}) == (4000.0, 6000.0)
    assert rs.rendered_recipe_dimensions(6000, 4000, {"rotation": 270}) == (4000.0, 6000.0)


def test_rendered_recipe_dimensions_applies_crop_after_rotation():
    # 90° rotation swaps to (4000, 6000), then a 0.5x0.5 crop yields (2000, 3000).
    result = rs.rendered_recipe_dimensions(
        6000, 4000, {"rotation": 90, "crop": {"w": 0.5, "h": 0.5}},
    )
    assert result == (2000.0, 3000.0)


def test_rendered_recipe_long_edge_matches_max_of_dimensions():
    # Regression guard: long-edge helper must agree with both-axis helper.
    for recipe in (
        None,
        {"rotation": 90},
        {"rotation": 180, "crop": {"w": 0.5, "h": 0.75}},
    ):
        w, h = rs.rendered_recipe_dimensions(6000, 4000, recipe)
        assert rs.rendered_recipe_long_edge(6000, 4000, recipe) == max(w, h)


def _wc_photo(tmp_path, wc_size, orig_w, orig_h):
    """Build a photo dict backed by a real JPEG on disk at ``wc_size``."""
    wc_rel = "wc.jpg"
    _img(*wc_size).save(str(tmp_path / wc_rel), "JPEG")
    return {
        "working_copy_path": wc_rel,
        "width": orig_w,
        "height": orig_h,
        "exif_data": None,
        "filename": "IMG.NEF",
    }


def test_working_copy_satisfies_recipe_render_full_size_wc_passes(tmp_path):
    photo = _wc_photo(tmp_path, (6000, 4000), 6000, 4000)
    assert rs.working_copy_satisfies_recipe_render(
        photo, recipe=None, max_size=None, vireo_dir=str(tmp_path),
    ) is True


def test_working_copy_satisfies_recipe_render_rejects_short_edge_truncated(tmp_path):
    # A failed-RAW embedded JPEG at 6000x3376 covers the long edge of a
    # 6000x4000 source but loses ~15% of the short-edge content. A long-edge-
    # only check would accept it; the both-axis check must reject it.
    photo = _wc_photo(tmp_path, (6000, 3376), 6000, 4000)
    assert rs.working_copy_satisfies_recipe_render(
        photo, recipe=None, max_size=None, vireo_dir=str(tmp_path),
    ) is False


def test_working_copy_satisfies_recipe_render_short_edge_within_rel_slack(tmp_path):
    # 1% rel_slack: 5940 is >= 6000*0.99 and 3960 is >= 4000*0.99, so it passes.
    photo = _wc_photo(tmp_path, (5940, 3960), 6000, 4000)
    assert rs.working_copy_satisfies_recipe_render(
        photo, recipe=None, max_size=None, vireo_dir=str(tmp_path),
        rel_slack=0.01,
    ) is True
    # But a WC whose short edge falls below rel_slack must still be rejected.
    truncated = _wc_photo(tmp_path, (5940, 3376), 6000, 4000)
    assert rs.working_copy_satisfies_recipe_render(
        truncated, recipe=None, max_size=None, vireo_dir=str(tmp_path),
        rel_slack=0.01,
    ) is False


def test_working_copy_satisfies_recipe_render_scales_by_max_size(tmp_path):
    # For a max_size=3000 request over a 6000x4000 source, the required
    # rendered dims are (3000, 2000). A 3000x2000 wc satisfies; a 3000x1688
    # (short-edge truncated after scaling) wc does not.
    ok = _wc_photo(tmp_path, (3000, 2000), 6000, 4000)
    assert rs.working_copy_satisfies_recipe_render(
        ok, recipe=None, max_size=3000, vireo_dir=str(tmp_path),
    ) is True
    truncated = _wc_photo(tmp_path, (3000, 1688), 6000, 4000)
    assert rs.working_copy_satisfies_recipe_render(
        truncated, recipe=None, max_size=3000, vireo_dir=str(tmp_path),
    ) is False


def test_working_copy_satisfies_recipe_render_respects_recipe_rotation(tmp_path):
    # A 90° rotation swaps axes: a wc that looks correct pre-rotation but is
    # short on the post-rotation axis must be rejected on both axes.
    truncated = _wc_photo(tmp_path, (6000, 3376), 6000, 4000)
    assert rs.working_copy_satisfies_recipe_render(
        truncated, recipe={"rotation": 90}, max_size=None,
        vireo_dir=str(tmp_path),
    ) is False


def test_working_copy_satisfies_recipe_render_missing_wc_path(tmp_path):
    photo = {
        "working_copy_path": "does-not-exist.jpg",
        "width": 6000,
        "height": 4000,
        "exif_data": None,
        "filename": "IMG.NEF",
    }
    assert rs.working_copy_satisfies_recipe_render(
        photo, recipe=None, max_size=None, vireo_dir=str(tmp_path),
    ) is False


def test_working_copy_satisfies_recipe_render_scales_wc_before_compare(tmp_path):
    # ``load_image(..., max_size=1024)`` caps the long edge and scales the
    # short edge proportionally. A 6000x3376 embedded preview vs a 1024px
    # target renders as 1024x576 — its short edge falls short of the
    # required 1024x683. Without scaling the WC render dims before the
    # compare, 6000 >= 1024 and 3376 >= 683 both pass and the failed-RAW
    # fallback would cache the truncated preview.
    truncated = _wc_photo(tmp_path, (6000, 3376), 6000, 4000)
    assert rs.working_copy_satisfies_recipe_render(
        truncated, recipe=None, max_size=1024, vireo_dir=str(tmp_path),
        rel_slack=0.01,
    ) is False
    # A full-resolution WC still passes the capped-request compare.
    full = _wc_photo(tmp_path, (6000, 4000), 6000, 4000)
    assert rs.working_copy_satisfies_recipe_render(
        full, recipe=None, max_size=1024, vireo_dir=str(tmp_path),
        rel_slack=0.01,
    ) is True
