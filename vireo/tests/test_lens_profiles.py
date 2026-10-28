"""Strict profile matching, calibrated interpolation, shared renders and caches."""

import io
import json

import lens_profiles as lens
import numpy as np
import pytest
from edit_batch import compose_recipe
from edit_recipe import RecipeError, normalize_recipe
from float_image import FloatImage
from image_edits import _apply_geometry, apply_recipe_to_loaded_image, local_weight_map
from optics import apply_optics, distort_image, profile_inset
from PIL import Image
from render_fingerprint import cache_matches, cache_save_options, render_cache_fields

_CAMERA = """<camera><maker>Nikon</maker><model>Nikon Test Camera</model>
<mount>Nikon Z</mount><cropfactor>1</cropfactor></camera>"""
_LENS = """<lens><maker>Nikon</maker><model>Nikon Test 24-70mm f/2.8</model>
<mount>Nikon Z</mount><cropfactor>1</cropfactor><calibration>
<distortion model="poly3" focal="24" k1="-0.02"/>
<distortion model="poly3" focal="70" k1="0.01"/>
<vignetting model="pa" focal="24" aperture="2" distance="1000" k1="-0.4"/>
<vignetting model="pa" focal="24" aperture="4" distance="1000" k1="-0.2"/>
<vignetting model="pa" focal="70" aperture="2" distance="1000" k1="-0.2"/>
<vignetting model="pa" focal="70" aperture="4" distance="1000" k1="-0.1"/>
</calibration></lens>"""


def _photo(**kwargs):
    return {"extension": ".dng", "camera_make": "NIKON CORPORATION", "camera_model": "NIKON TEST CAMERA",
            "lens": "Test 24-70mm F2.8", "focal_length": 47, "aperture": 2 ** 1.5, **kwargs}


@pytest.fixture
def database(tmp_path, monkeypatch):
    monkeypatch.setattr(lens, "_DATA", tmp_path)
    path = tmp_path / "test.xml"
    path.write_text("<lensdatabase>" + _CAMERA + _LENS + "</lensdatabase>")
    lens._database.cache_clear()
    yield path
    lens._database.cache_clear()


def test_bundled_profiles_match_real_wildlife_lenses():
    photo = {"extension": ".nef", "camera_make": "NIKON CORPORATION", "camera_model": "NIKON Z 8",
             "lens": "NIKKOR Z 180-600mm f/5.6-6.3 VR", "focal_length": 300, "aperture": 6.3}
    profile = lens.resolve_profile(photo)
    assert profile["matched"]
    assert profile["warp"] is not None and profile["vignette"] is not None
    assert profile["crop"] == 1
    assert profile["revision"] == lens._REVISION
    assert (lens._DATA / "LICENSE.txt").exists()


def test_profile_flag_is_boolean_canonical_and_batch_rebinds(database):
    assert normalize_recipe({"adjustments": {"lens_profile": False}}) is None
    for value in (1, 0, None, "true", {}):
        with pytest.raises(RecipeError):
            normalize_recipe({"adjustments": {"lens_profile": value}})
    recipe = compose_recipe({}, {"adjustments": {"lens_profile": True}}, ["adjustments.lens_profile"], "merge")
    assert recipe == {"version": 1, "adjustments": {"lens_profile": True}}
    assert lens.resolve_profile(_photo())["matched"]
    assert not lens.resolve_profile(_photo(lens="Another 24-70mm f/2.8"))["matched"]


def test_calibration_interpolates_focal_and_aperture_in_stops(database):
    profile = lens.resolve_profile(_photo())
    assert profile["matched"]
    assert lens.resolve_profile(_photo(lens="TEST 24.0-70.00mm f/2.800"))["matched"]
    # Midpoint focal gives k=-0.005. Half diagonal / half short side = sqrt(3.25).
    assert profile["warp"] == pytest.approx([1.005, 0, -0.005 * 3.25, 0, 0])
    assert profile["vignette"] == pytest.approx([-0.225, 0, 0])
    assert "nearest calibrated focus" in profile["message"]


@pytest.mark.parametrize("change", [
    {"extension": ".jpg"}, {"lens": "Test 24-70mm f/4"}, {"lens": "Test II 24-70mm f/2.8"},
    {"camera_model": "Unknown camera"}, {"focal_length": None}, {"focal_length": 200},
    {"exif_data": {"EXIF": {"LensMake": "Sigma"}}}, {"lens": "24-70mm f/2.8"},
])
def test_uncertain_or_unsupported_match_does_not_apply(database, change):
    profile = lens.resolve_profile(_photo(**change))
    assert not profile["matched"]
    assert profile["warp"] is None and profile["vignette"] is None
    assert profile["message"]


def test_missing_aperture_skips_vignetting_and_unknown_distance_uses_farthest(database):
    profile = lens.resolve_profile(_photo(aperture=None))
    assert profile["matched"] and profile["warp"] is not None
    assert profile["vignette"] is None
    text = database.read_text().replace('</calibration>', '<vignetting model="pa" focal="24" aperture="2" distance="1" k1="-0.9"/></calibration>')
    database.write_text(text)
    lens._database.cache_clear()
    assert lens.resolve_profile(_photo(focal_length=24, aperture=2))["vignette"][0] == pytest.approx(-0.4)


def test_mount_crop_and_ambiguous_calibration_are_rejected(database):
    text = database.read_text()
    for changed in (
        text.replace('<mount>Nikon Z</mount><cropfactor>1</cropfactor><calibration>', '<mount>Canon RF</mount><cropfactor>1</cropfactor><calibration>'),
        text.replace('<cropfactor>1</cropfactor><calibration>', '<cropfactor>1.5</cropfactor><calibration>'),
        text.replace('</lensdatabase>', _LENS.replace('k1="-0.02"', 'k1="-0.03"') + '</lensdatabase>'),
    ):
        database.write_text(changed)
        lens._database.cache_clear()
        assert not lens.resolve_profile(_photo())["matched"]


def test_crop_factor_scales_the_calibrated_radius(database):
    photo = _photo(exif_data={"Composite": {"ScaleFactor35efl": 1.5}})
    profile = lens.resolve_profile(photo)
    assert profile["warp"][2] == pytest.approx(-0.005 * 3.25 / 1.5 ** 2)
    assert profile["vignette"][0] == pytest.approx(-0.225 / 1.5 ** 2)


def test_invalid_folding_models_and_missing_database_are_visible(database):
    database.write_text(database.read_text().replace('k1="-0.02"', 'k1="-2"').replace('k1="0.01"', 'k1="-2"'))
    lens._database.cache_clear()
    profile = lens.resolve_profile(_photo(aperture=None))
    assert not profile["matched"]
    database.unlink()
    lens._database.cache_clear()
    assert "database is unavailable" in lens.resolve_profile(_photo())["message"]


def test_profile_gain_geometry_and_masks_share_the_linear_raw_path(database):
    photo = _photo()
    profile = lens.resolve_profile(photo)
    width, height = 181, 121
    x, y = np.meshgrid(np.linspace(-1.5, 1.5, width), np.linspace(-1, 1, height))
    r2 = (x * x + y * y) / 3.25
    pixels = np.repeat((0.2 * (1 - 0.225 * r2))[..., None], 3, axis=-1).astype(np.float32)
    source = FloatImage(pixels)
    np.testing.assert_allclose(apply_optics(source, {}, profile), 0.2, atol=1e-7)
    recipe = {"adjustments": {"lens_profile": True, "distortion": -25}, "rotation": 90}
    rendered = apply_recipe_to_loaded_image(source, recipe, camera_metadata=photo)
    assert isinstance(rendered, FloatImage)
    np.testing.assert_array_equal(source, pixels)
    assert np.ptp(np.asarray(rendered)[..., 0]) < 1e-6
    mask = Image.fromarray(np.where(x > 0, 255, 0).astype(np.uint8))
    recipe["local"] = {"mask": {"ref": "123456abcdef", "source_digest": "test"},
                       "regions": [{"region": "subject", "adjustments": {"exposure": 1}}]}
    weight = local_weight_map(mask, mask.size, recipe, camera_metadata=photo)
    np.testing.assert_allclose(weight, np.asarray(_apply_geometry(mask, recipe, profile)) / 255, atol=1e-7)
    assert profile_inset(-25, profile["warp"]) < 1
    flat = FloatImage(np.full((height, width, 3), 0.2, dtype=np.float32))
    np.testing.assert_allclose(distort_image(flat, -25, profile), np.asarray(flat), atol=1e-7)


@pytest.mark.parametrize("warp", [[1.02, 0, -0.065, 0, 0], [1, 0, -0.04, 0, 0.015], [1.02, -0.04, 0.08, -0.02, 0]])
def test_calibrated_warp_restores_known_radial_grid(warp):
    width, height = 321, 241
    x, y = np.meshgrid(np.arange(width) - 160, np.arange(height) - 120)
    rd = np.sqrt((x * x + y * y) / (160 ** 2 + 120 ** 2))
    ru = rd.copy()
    for _ in range(12):
        ratio = np.polynomial.polynomial.polyval(ru, warp)
        derivative = np.polynomial.polynomial.polyval(ru, np.asarray(warp) * np.arange(1, 6))
        ru -= (ru * ratio - rd) / derivative
    ratio = np.divide(ru, rd, out=np.ones_like(rd), where=rd > 0)
    distorted = 0.5 + 0.3 * np.cos(x * ratio / 9)
    image = FloatImage(np.repeat(distorted[..., None], 3, axis=-1).astype(np.float32))
    result = np.asarray(distort_image(image, 0, {"warp": warp}))[..., 0]
    expected = 0.5 + 0.3 * np.cos(x * profile_inset(0, warp) / 9)
    np.testing.assert_allclose(result[40:-40, 60:-60], expected[40:-40, 60:-60], atol=0.002)


def test_jpeg_fallback_skips_profile_even_when_raw_catalog_metadata_matches(database):
    photo = _photo()
    source = Image.new("RGB", (121, 81), (70, 90, 120))
    recipe = {"adjustments": {"lens_profile": True, "vignetting": 20, "distortion": -10}}
    expected = apply_recipe_to_loaded_image(source, {"adjustments": {"vignetting": 20, "distortion": -10}})
    actual = apply_recipe_to_loaded_image(source, recipe, camera_metadata=photo)
    np.testing.assert_array_equal(actual, expected)


def test_auto_tone_meters_on_the_lens_corrected_source(database, monkeypatch):
    import auto_tone

    x, y = np.meshgrid(np.linspace(-1.5, 1.5, 181), np.linspace(-1, 1, 121))
    falloff = 1 - 0.225 * (x * x + y * y) / 3.25
    source = FloatImage(np.repeat((0.2 * falloff)[..., None], 3, axis=-1))
    calls = []

    def fit(pixels, **kwargs):
        calls.append(pixels)
        return {"metering": "global"}

    monkeypatch.setattr(auto_tone, "fit", fit)
    auto_tone.fit_loaded_image(source, {"adjustments": {"lens_profile": True}}, camera_metadata=_photo())
    np.testing.assert_allclose(calls[0], 0.2, atol=1e-7)


def test_render_cache_tracks_lens_metadata_calibration_and_other_camera_edits(database, tmp_path):
    recipe = {"adjustments": {"lens_profile": True}}
    photo = _photo()
    key = render_cache_fields(photo, recipe)
    path = tmp_path / "cached.jpg"
    Image.new("RGB", (10, 10)).save(path, **cache_save_options(photo, recipe))
    assert cache_matches(path, photo, recipe)
    for changed in (_photo(focal_length=24), _photo(aperture=8), _photo(lens="Wrong lens")):
        assert key != render_cache_fields(changed, recipe)
        assert not cache_matches(path, changed, recipe)
    database.write_text(database.read_text().replace('k1="-0.02"', 'k1="-0.03"'))
    lens._database.cache_clear()
    assert key != render_cache_fields(photo, recipe)
    assert not cache_matches(path, photo, recipe)
    assert render_cache_fields(photo, {}) == {}


def test_profile_metadata_flows_to_preview_export_thumbnails_and_source_receipt(database, app_and_db, tmp_path):
    from export import load_export_image
    from image_loader import RAW_DECODE_LINEAR, load_image
    from preview_materializer import render_preview_bytes
    from services import photo_ingest, workspace_folders
    from test_raw_precision import write_dng
    from thumbnails import generate_all

    app, db = app_and_db
    folder = tmp_path / "raws"
    folder.mkdir()
    source = folder / "calibrated.dng"
    write_dng(source)
    fid = workspace_folders.add_folder(db, str(folder))
    pid = photo_ingest.add_photo(db, folder_id=fid, filename=source.name, extension=".dng", width=512, height=64,
                                file_size=source.stat().st_size, file_mtime=source.stat().st_mtime)
    metadata = _photo()
    db.conn.execute('UPDATE photos SET camera_make=?,camera_model=?,lens=?,focal_length=?,aperture=? WHERE id=?',
                    (*[metadata[k] for k in ("camera_make", "camera_model", "lens", "focal_length", "aperture")], pid))
    db.conn.commit()
    photo = db.photos.get(pid)
    bulk = db.photos.get_by_ids([pid])[pid]
    assert lens.resolve_profile(photo) == lens.resolve_profile(bulk)
    recipe = {"adjustments": {"lens_profile": True}}
    db.set_photo_edit_recipe(pid, recipe)
    decoded = load_image(source, raw_decode=RAW_DECODE_LINEAR, max_size=None)
    expected = apply_recipe_to_loaded_image(decoded, recipe, camera_metadata=photo)
    assert not np.allclose(expected, apply_recipe_to_loaded_image(decoded, {}))
    buffer = io.BytesIO()
    expected.save(buffer, format="JPEG", quality=90)
    app.config["EDIT_PREVIEW_IN_PROCESS"] = True
    client = app.test_client()
    info = client.get(f"/api/photos/{pid}").get_json()
    assert info["lens_profile"]["matched"]
    query = {"size": 512, "recipe": json.dumps(recipe), "preview_session": "a" * 32, "preview_seq": "1"}
    preview = client.get(f"/photos/{pid}/edit-preview", query_string=query)
    assert preview.status_code == 200
    assert preview.data == buffer.getvalue()
    receipt = client.get(f"/photos/{pid}/edit-preview", query_string={**query, "source_status": "1"})
    assert receipt.get_json() == {"source": "linear"}
    assert client.get(f"/photos/{pid}/edit-preview", query_string={**query, "source_status": "1", "preview_seq": "2"}).get_json() == {"source": None}
    exported = load_export_image(photo, str(tmp_path), {fid: str(folder)}, recipe=recipe)
    np.testing.assert_array_equal(exported, expected)
    cached = io.BytesIO()
    expected.save(cached, format="JPEG", quality=90, **cache_save_options(photo, recipe))
    assert render_preview_bytes(db, photo, str(folder), size=512, vireo_dir=str(tmp_path), preview_quality=90, recipe=recipe) == cached.getvalue()
    thumbs = tmp_path / "thumbs"
    generate_all(db, str(thumbs), config={"thumbnail_size": 512, "thumbnail_quality": 90}, vireo_dir=str(tmp_path))
    assert (thumbs / f"{pid}.jpg").read_bytes() == cached.getvalue()
    # Lens-only recipes must reach the metadata-aware cache check during
    # batch selection, even when the thumbnail already exists.
    db.conn.execute('UPDATE photos SET focal_length=24 WHERE id=?', (pid,))
    db.conn.commit()
    changed_photo = db.photos.get(pid)
    assert not cache_matches(thumbs / f"{pid}.jpg", changed_photo, recipe)
    generate_all(db, str(thumbs), config={"thumbnail_size": 512, "thumbnail_quality": 90}, vireo_dir=str(tmp_path))
    assert (thumbs / f"{pid}.jpg").read_bytes() != cached.getvalue()
    assert cache_matches(thumbs / f"{pid}.jpg", changed_photo, recipe)
