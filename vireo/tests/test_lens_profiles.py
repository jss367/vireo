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


@pytest.mark.parametrize("source_kind", ["linear", "jpeg", "srgb"])
@pytest.mark.parametrize("crop_rotate", [False, True])
def test_background_protection_matches_source_eligible_lens_geometry(
    database, monkeypatch, source_kind, crop_rotate,
):
    from background_denoise import protection_weight
    from image_edits import detail_render_scale

    photo = _photo(focal_length=24)
    pixels = np.full((241, 321, 3), 0.2, dtype=np.float32)
    source = (Image.fromarray((pixels * 255).astype(np.uint8)) if source_kind == "jpeg"
              else FloatImage(pixels, encoding=source_kind))
    mask_pixels = np.zeros((241, 321), dtype=np.uint8)
    mask_pixels[20:215, 20:110] = 255
    mask_pixels[70:120, 50:85] = 0  # Preserve this painted correction through geometry.
    mask = Image.fromarray(mask_pixels)
    recipe = {
        "adjustments": {"lens_profile": True, "distortion": -25},
        "local": {"mask": {"ref": "123456abcdef", "source_digest": "test", "corrected": True},
                  "regions": [], "background_denoise": 70},
    }
    if crop_rotate:
        recipe.update(rotation=90, flip={"horizontal": True},
                      crop={"x": 0.1, "y": 0.05, "w": 0.85, "h": 0.9})
    weights = []
    def capture_weight(image, weight, amount, scale, metadata):
        weights.append(weight)
        return image
    monkeypatch.setattr("background_denoise.apply_background_denoise", capture_weight)
    rendered = apply_recipe_to_loaded_image(
        source, recipe, camera_metadata=photo, local_mask=mask, native_size=source.size, max_size=200,
    )
    eligible = lens.resolve_profile(photo) if source_kind == "linear" else None
    geometry = _apply_geometry(mask, recipe, eligible).resize(rendered.size, Image.Resampling.BILINEAR)
    scale = detail_render_scale(rendered.size, source.size, recipe)
    expected = protection_weight(geometry, scale)
    np.testing.assert_array_equal(weights[0], expected)
    # The fixture must distinguish missing RAW correction and double-corrected JPEGs.
    other_profile = None if eligible else lens.resolve_profile(photo)
    other = _apply_geometry(mask, recipe, other_profile).resize(rendered.size, Image.Resampling.BILINEAR)
    assert not np.array_equal(expected, protection_weight(other, scale))


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


@pytest.mark.parametrize("artifact", ["thumbnail", "preview"])
@pytest.mark.parametrize("recovery", ["restored", "transient"])
def test_fallback_cache_cannot_masquerade_as_profile_corrected_raw(database, tmp_path, monkeypatch, artifact, recovery):
    from types import SimpleNamespace

    import image_loader
    import render_fingerprint
    import thumbnails
    from preview_materializer import materialize_preview

    source = tmp_path / "photo.dng"
    if recovery == "transient":
        source.write_bytes(b"decoder replaced by controlled sources")
    now = [100000.0]
    monkeypatch.setattr(render_fingerprint, "time", SimpleNamespace(time=lambda: now[0]))
    photo = _photo(id=7, folder_id=1, filename=source.name, width=121, height=81,
                   companion_path="companion.jpg", working_copy_path=None)
    Image.new("RGB", (121, 81), (70, 70, 70)).save(tmp_path / "companion.jpg")
    recipe = {"adjustments": {"lens_profile": True}}
    decoded = []
    linear = False

    def load(*args, **kwargs):
        decoded.append(linear)
        if linear:
            return FloatImage(np.full((81, 121, 3), 0.1, dtype=np.float32))
        return Image.new("RGB", (121, 81), (70, 70, 70))

    monkeypatch.setattr(image_loader, "load_image", load)
    monkeypatch.setattr(thumbnails, "load_image", load)
    cache = tmp_path / "cache"
    cache.mkdir()
    path = cache / "7.jpg"

    def render():
        if artifact == "thumbnail":
            thumbnails.generate_thumbnail(7, str(source), str(cache), recipe=recipe, camera_metadata=photo)
        else:
            materialize_preview(None, photo, str(tmp_path), size=256, vireo_dir=str(tmp_path),
                                preview_quality=90, recipe=recipe, cache_path=str(path))

    render()
    fallback = path.read_bytes()
    assert not cache_matches(path, photo, recipe)
    assert cache_matches(path, photo, recipe, allow_fallback=True)
    assert not cache_matches(path, photo, recipe, source_path=tmp_path / "companion.jpg")
    assert cache_matches(path, photo, recipe, source_path=source)
    render()
    assert decoded == [False]  # Reuse a healthy fallback, not another decode.
    now[0] += render_fingerprint._FALLBACK_RETRY_SECONDS + 1
    if recovery == "restored":
        render()
        assert decoded == [False]  # A still-offline source remains reusable.
        source.write_bytes(b"the RAW original is available again")
    linear = True
    render()
    assert decoded == [False, True]
    assert path.read_bytes() != fallback
    assert cache_matches(path, photo, recipe)
    render()
    assert decoded == [False, True]


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


@pytest.mark.parametrize("artifact", ["external", "inat"])
@pytest.mark.parametrize("recovery", ["restored", "transient"])
def test_handoff_fallback_cache_reuses_and_recovers(database, app_and_db, tmp_path, monkeypatch, artifact, recovery):
    import os
    from types import SimpleNamespace

    import config as cfg
    import image_loader
    import inat
    import render_fingerprint
    from services import photo_ingest, workspace_folders
    from web.photos import _OpenExternal

    app, db = app_and_db
    folder = tmp_path / "raws"
    folder.mkdir()
    source = folder / "photo.dng"
    if recovery == "transient":
        source.write_bytes(b"unsupported RAW")
    Image.new("RGB", (121, 81), (70, 70, 70)).save(folder / "companion.jpg")
    fid = workspace_folders.add_folder(db, str(folder))
    pid = photo_ingest.add_photo(db, folder_id=fid, filename=source.name, extension=".dng",
                                width=121, height=81, file_size=123, file_mtime=1)
    metadata = _photo()
    db.conn.execute('UPDATE photos SET camera_make=?,camera_model=?,lens=?,focal_length=?,aperture=?,companion_path=? WHERE id=?',
                    (*[metadata[k] for k in ("camera_make", "camera_model", "lens", "focal_length", "aperture")], "companion.jpg", pid))
    db.conn.commit()
    db.set_photo_edit_recipe(pid, {"adjustments": {"lens_profile": True}})
    photo = db.photos.get_with_folder_path(pid)
    now = [100000.0]
    monkeypatch.setattr(render_fingerprint, "time", SimpleNamespace(time=lambda: now[0]))
    decoded = []
    linear = False

    def load(*args, **kwargs):
        decoded.append(linear)
        if linear:
            return FloatImage(np.full((81, 121, 3), 0.1, dtype=np.float32))
        return Image.new("RGB", (121, 81), (70, 70, 70))

    monkeypatch.setattr(image_loader, "load_image", load)
    monkeypatch.setattr(inat, "submit_observation", lambda **kwargs: (123, "https://example.com/123"))
    cfg.save({"inat_token": "fake-token"})
    media = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    run = _OpenExternal(db, {fid: str(folder)}, media, lambda *args: None)
    client = app.test_client()

    def render():
        if artifact == "external":
            path, error = run.handoff_path(photo, str(source))
            assert error is None
            return path
        response = client.post("/api/inat/submit", json={"photo_id": pid})
        assert response.status_code == 200, response.get_json()
        return os.path.join(media, "inat-uploads", f"{pid}.jpg")

    path = render()
    with open(path, "rb") as stream:
        fallback = stream.read()
    render()
    assert decoded == [False]
    now[0] += render_fingerprint._FALLBACK_RETRY_SECONDS + 1
    if recovery == "restored":
        render()
        assert decoded == [False]  # Offline fallback remains stable beyond retry age.
        source.write_bytes(b"RAW restored")
    linear = True
    render()
    assert decoded == [False, True]
    with open(path, "rb") as stream:
        assert stream.read() != fallback
    render()
    assert decoded == [False, True]


@pytest.mark.parametrize("artifact", ["thumbnail", "preview"])
def test_background_optical_cache_handles_disappearing_folder(database, app_and_db, tmp_path, monkeypatch, artifact):
    from pipeline_stages.media import _preview_photo, _PreviewTarget
    from services import photo_ingest, workspace_folders
    from thumbnails import generate_all

    app, db = app_and_db
    folder = tmp_path / "raws"
    folder.mkdir()
    working = tmp_path / "working.jpg"
    Image.new("RGB", (121, 81), (70, 70, 70)).save(working)
    fid = workspace_folders.add_folder(db, str(folder))
    pid = photo_ingest.add_photo(db, folder_id=fid, filename="photo.dng", extension=".dng",
                                width=121, height=81, file_size=123, file_mtime=1)
    metadata = _photo()
    db.conn.execute('UPDATE photos SET camera_make=?,camera_model=?,lens=?,focal_length=?,aperture=?,working_copy_path=? WHERE id=?',
                    (*[metadata[k] for k in ("camera_make", "camera_model", "lens", "focal_length", "aperture")], str(working), pid))
    db.conn.commit()
    recipe = {"adjustments": {"lens_profile": True}}
    db.set_photo_edit_recipe(pid, recipe)
    photo = db.photos.get(pid)
    cache = tmp_path / "cache"
    cache.mkdir()
    # Photo selection happens before the folder map is read. A concurrent
    # health check can remove its folder from tree() in between the two.
    monkeypatch.setattr(type(db.folders), "tree", lambda self: [])
    monkeypatch.setattr(type(db.photos), "list_page", lambda self, **kwargs: [photo])
    if artifact == "thumbnail":
        result = generate_all(db, str(cache), vireo_dir=str(tmp_path))
        assert result["generated"] == 1 and result["failed"] == 0
        assert (cache / f"{pid}.jpg").is_file()
    else:
        path = cache / f"{pid}_256.jpg"
        Image.new("RGB", (121, 81)).save(path, **cache_save_options(photo, recipe))
        db.caches.preview_insert(pid, 256, path.stat().st_size)
        target = _PreviewTarget(str(cache), 256, str(tmp_path), 90)
        assert _preview_photo(db, photo, {}, target) == "cached"
        assert path.is_file()


def test_float_image_encodes_identically_across_a_second_boundary(monkeypatch):
    """Concurrent first saves embed the one import-time sRGB profile.

    LittleCMS stamps a profile with its creation time; a profile built per
    save (or per racing cache miss) made identical pixels encode differently.
    Saves must never build a profile, so building one here fails the test.
    """
    import threading

    import float_image
    from float_image import FloatImage
    from PIL import Image, ImageCms
    from testing.waits import synchronization_timeout

    def refuse(*_args, **_kwargs):
        raise AssertionError("a save built its own sRGB profile")

    monkeypatch.setattr(float_image.ImageCms, "createProfile", refuse)
    image = FloatImage(np.full((4, 4, 3), 0.25, dtype=np.float32))
    workers = 8
    barrier = threading.Barrier(workers)
    outputs, errors = [None] * workers, []

    def save(index):
        try:
            buffer = io.BytesIO()
            barrier.wait(synchronization_timeout(5))
            image.save(buffer, format="JPEG", quality=90)
            outputs[index] = buffer.getvalue()
        except Exception as exc:  # broad: surface any worker failure in the assertion below
            errors.append(exc)

    threads = [threading.Thread(target=save, args=(i,)) for i in range(workers)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(synchronization_timeout(10))
    assert not errors, errors
    assert len(set(outputs)) == 1
    embedded = Image.open(io.BytesIO(outputs[0])).info["icc_profile"]
    assert embedded == float_image.SRGB_PROFILE
    description = ImageCms.getProfileDescription(ImageCms.ImageCmsProfile(io.BytesIO(embedded)))
    assert "sRGB" in description
