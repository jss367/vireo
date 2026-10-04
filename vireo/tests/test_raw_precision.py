"""RAW precision, real LibRaw decoding, and the complete edit/export path."""

import io
import json
import os

import image_loader
import numpy as np
import pytest
import rawpy
import tifffile
from float_image import FloatImage
from image_edits import apply_recipe_to_loaded_image
from image_loader import RAW_DECODE_LINEAR, load_image
from PIL import Image
from tone import apply_adjustments, linear_to_srgb


def write_dng(path):
    """Small, deterministic Bayer DNG; exercises LibRaw without camera files."""
    pixels = np.tile(np.linspace(512, 16000, 512, dtype=np.uint16), (64, 1))
    tags = [
        (50706, 'B', 4, (1, 4, 0, 0), False),  # DNGVersion
        (50707, 'B', 4, (1, 1, 0, 0), False),
        (50708, 's', 0, 'Vireo precision test camera', False),
        (33421, 'H', 2, (2, 2), False),  # CFARepeatPatternDim
        (33422, 'B', 4, (0, 1, 1, 2), False),
        (50714, 'I', 1, 512, False),  # BlackLevel
        (50717, 'I', 1, 16383, False),  # WhiteLevel
        (50721, '2i', 9, tuple(v for n in [1, 0, 0, 0, 1, 0, 0, 0, 1]
                              for v in (n, 1)), False),  # ColorMatrix1
        (50728, '2I', 3, (1, 2, 1, 1, 2, 3), False),  # AsShotNeutral
        (50778, 'H', 1, 21, False),  # CalibrationIlluminant1: D65
    ]
    tifffile.imwrite(path, pixels, photometric=32803, metadata=None, extratags=tags)


@pytest.fixture
def dng(tmp_path):
    path = tmp_path / 'ramp.dng'
    write_dng(path)
    return path


def test_real_raw_decode_retains_above_white_and_sub_byte_detail(dng):
    image = load_image(dng, max_size=None, raw_decode=RAW_DECODE_LINEAR)
    assert isinstance(image, FloatImage)
    pixels = np.asarray(image)
    assert pixels.dtype == np.float32
    assert pixels.max() > 1.0
    assert len(np.unique(pixels[32, :, 0])) > 400
    assert image.size == (512, 64)
    assert np.isfinite(pixels).all()


def test_negative_exposure_recovers_distinct_highlights_before_display_mapping():
    source = np.array([[[1.2] * 3, [1.8] * 3, [2.4] * 3]], dtype=np.float32)
    output = apply_adjustments(source, exposure=-2, input_linear=True)
    np.testing.assert_allclose(output, linear_to_srgb(source / 4), atol=1e-6)
    assert np.all(np.diff(output[0, :, 0]) > 0.09)
    # Encoding the source to 8-bit first irreversibly merges all three values.
    clipped = np.clip(linear_to_srgb(source) * 255, 0, 255).astype(np.uint8)
    legacy = apply_adjustments(clipped.astype(np.float32) / 255, exposure=-2)
    assert np.ptp(legacy[0, :, 0]) == 0


def test_neutral_raw_still_maps_highlights_and_out_of_gamut_colors():
    source = np.array([[[1.2] * 3, [2.4] * 3, [2.0, -0.1, 0.2]]], dtype=np.float32)
    output = apply_recipe_to_loaded_image(FloatImage(source), {})
    assert output.encoding == 'srgb'
    assert np.isfinite(np.asarray(output)).all()
    assert np.asarray(output).min() >= 0
    assert np.asarray(output).max() <= 1
    assert output.pixels[0, 0, 0] < output.pixels[0, 1, 0] < 1


@pytest.mark.parametrize('adjustments', [
    {'exposure': -0.5, 'white_balance': {'temperature': 25, 'tint': -10}},
    {'sharpen': 40, 'noise_reduction': 25},
    {'texture': 20, 'clarity': 15, 'dehaze': 10},
])
def test_tone_and_spatial_controls_keep_precision_through_geometry(adjustments):
    pixels = np.broadcast_to(
        np.linspace(0.01, 0.4, 512, dtype=np.float32)[None, :, None], (32, 512, 3),
    ).copy()
    image = FloatImage(pixels)
    recipe = {'rotation': 90, 'flip': {'horizontal': True},
              'crop': {'x': 0, 'y': 0, 'w': 1, 'h': 0.9},
              'adjustments': adjustments}
    result = apply_recipe_to_loaded_image(image, recipe, native_size=image.size)
    assert isinstance(result, FloatImage)
    assert result.encoding == 'srgb'
    assert result.pixels.dtype == np.float32
    assert len(np.unique(result.pixels[:, 15, 0])) > 256
    np.testing.assert_array_equal(image.pixels, pixels)  # Rendering never mutates a source.


def test_local_tone_and_detail_stay_float_and_follow_mask_geometry():
    image = FloatImage(np.full((32, 64, 3), 0.12345, dtype=np.float32))
    mask = Image.fromarray(np.tile(np.repeat([255, 0], 32).astype(np.uint8), (32, 1)))
    recipe = {'flip': {'horizontal': True}, 'local': {
        'mask': {'ref': 'a1b2c3d4e5f6', 'source_digest': 'test'},
        'regions': [
            {'region': 'subject', 'adjustments': {'exposure': 1, 'sharpen': 30}},
            {'region': 'background', 'adjustments': {'noise_reduction': 15}},
        ],
    }}
    output = apply_recipe_to_loaded_image(image, recipe, local_mask=mask)
    assert isinstance(output, FloatImage)
    assert output.pixels[:, 40:].mean() > output.pixels[:, :24].mean()
    # None of the spatial/local blending stages may round to an 8-bit grid.
    assert np.max(np.abs(output.pixels * 255 - np.rint(output.pixels * 255))) > 0.01


def test_tiff_export_has_real_16_bit_samples_and_correct_profile(dng):
    from export import _OUTPUT_FORMATS, _save_export_image

    source = load_image(dng, max_size=None, raw_decode=RAW_DECODE_LINEAR)
    result = apply_recipe_to_loaded_image(source, {'adjustments': {'exposure': -1}})
    stream = io.BytesIO()
    _save_export_image(result, stream, _OUTPUT_FORMATS['tiff'], 92)
    stream.seek(0)
    with tifffile.TiffFile(stream) as tiff:
        pixels = tiff.asarray()
        assert pixels.dtype == np.uint16
        assert len(np.unique(pixels[32, :, 0])) > 400
        np.testing.assert_allclose(pixels / 65535, result.pixels, atol=1 / 65535)
        assert tiff.pages[0].tags[34675].dtype == 7  # ICCProfile requires UNDEFINED.
        assert tiff.pages[0].tags[34675].value[16:20] == b'RGB '


def test_sized_raw_cache_is_bounded_isolated_and_invalidates_on_source_change(dng, monkeypatch):
    monkeypatch.setattr(image_loader, '_linear_cache', image_loader.OrderedDict())
    monkeypatch.setattr(image_loader, '_LINEAR_CACHE_BYTES', 128 * 16 * 3 * 4)
    original = image_loader._postprocess_raw_linear
    calls = []

    def decode(raw):
        calls.append(True)
        return original(raw)

    monkeypatch.setattr(image_loader, '_postprocess_raw_linear', decode)
    first = load_image(dng, max_size=128, raw_decode=RAW_DECODE_LINEAR)
    first.pixels[:] = 0
    second = load_image(dng, max_size=128, raw_decode=RAW_DECODE_LINEAR)
    assert len(calls) == 1
    assert second.pixels.max() > 0
    stat = dng.stat()
    os.utime(dng, ns=(stat.st_atime_ns, stat.st_mtime_ns + 1_000_000))
    load_image(dng, max_size=128, raw_decode=RAW_DECODE_LINEAR)
    assert len(calls) == 2
    assert sum(i.pixels.nbytes for i in image_loader._linear_cache.values()) <= image_loader._LINEAR_CACHE_BYTES


def _signal_when_flight_has_waiters(monkeypatch, count):
    """Event set once ``count`` requests are parked on an in-flight decode."""
    import threading

    waiters = []
    waiting = threading.Event()

    class CountingEvent(threading.Event):
        def wait(self, timeout=None):
            waiters.append(True)
            if len(waiters) == count:
                waiting.set()
            return super().wait(timeout)

    class Flight(image_loader._LinearDecodeFlight):
        def __init__(self):
            super().__init__()
            self.done = CountingEvent()

    monkeypatch.setattr(image_loader, '_LinearDecodeFlight', Flight)
    return waiting


def test_concurrent_sized_raw_misses_share_one_decode(dng, monkeypatch):
    """Slider pauses that miss the cache together must not each decode."""
    import threading

    monkeypatch.setattr(image_loader, '_linear_cache', image_loader.OrderedDict())
    monkeypatch.setattr(image_loader, '_linear_inflight', {})
    original = image_loader._postprocess_raw_linear
    calls = []
    started = threading.Event()
    release = threading.Event()

    def decode(raw):
        calls.append(True)
        started.set()
        assert release.wait(10)
        return original(raw)

    monkeypatch.setattr(image_loader, '_postprocess_raw_linear', decode)
    waiting = _signal_when_flight_has_waiters(monkeypatch, 3)
    results = [None] * 4

    def load(i):
        results[i] = load_image(dng, max_size=128, raw_decode=RAW_DECODE_LINEAR)

    leader = threading.Thread(target=load, args=(0,))
    leader.start()
    assert started.wait(10)
    followers = [threading.Thread(target=load, args=(i,)) for i in range(1, 4)]
    for t in followers:
        t.start()
    # Followers must be parked on the flight before the decode finishes.
    assert waiting.wait(10)
    release.set()
    for t in [leader, *followers]:
        t.join(10)
    assert len(calls) == 1
    assert all(isinstance(r, FloatImage) for r in results)
    assert len({id(r.pixels) for r in results}) == 4
    results[0].pixels[:] = 0
    assert results[1].pixels.max() > 0
    assert image_loader._linear_inflight == {}


def test_sized_raw_follower_decodes_itself_when_leader_raises(dng, monkeypatch):
    import threading

    monkeypatch.setattr(image_loader, '_linear_cache', image_loader.OrderedDict())
    monkeypatch.setattr(image_loader, '_linear_inflight', {})
    original = image_loader._postprocess_raw_linear
    calls = []
    started = threading.Event()
    release = threading.Event()

    def decode(raw):
        calls.append(True)
        if len(calls) == 1:
            started.set()
            assert release.wait(10)
            raise MemoryError('leader decode failed')
        return original(raw)

    monkeypatch.setattr(image_loader, '_postprocess_raw_linear', decode)
    waiting = _signal_when_flight_has_waiters(monkeypatch, 1)
    results = {}

    def load(name):
        results[name] = load_image(dng, max_size=128, raw_decode=RAW_DECODE_LINEAR)

    leader = threading.Thread(target=load, args=('leader',))
    leader.start()
    assert started.wait(10)
    follower = threading.Thread(target=load, args=('follower',))
    follower.start()
    assert waiting.wait(10)
    release.set()
    leader.join(10)
    follower.join(10)
    assert results['leader'] is None  # load_image logs the failure
    assert isinstance(results['follower'], FloatImage)
    assert image_loader._linear_inflight == {}


def test_lone_decode_above_the_entry_ceiling_is_not_duplicated(dng, monkeypatch):
    """A decode too large to cache must not be copied when nobody waits on it."""
    monkeypatch.setattr(image_loader, '_linear_cache', image_loader.OrderedDict())
    monkeypatch.setattr(image_loader, '_linear_inflight', {})
    monkeypatch.setattr(image_loader, '_LINEAR_CACHE_MAX_ENTRY_BYTES', 1)
    original_copy = FloatImage.copy
    copies = []

    def counting_copy(self):
        copies.append(True)
        return original_copy(self)

    monkeypatch.setattr(FloatImage, 'copy', counting_copy)
    image = load_image(dng, max_size=128, raw_decode=RAW_DECODE_LINEAR)
    assert isinstance(image, FloatImage)
    assert copies == []
    assert len(image_loader._linear_cache) == 0


def test_followers_of_an_uncacheable_decode_share_it(dng, monkeypatch):
    import threading

    monkeypatch.setattr(image_loader, '_linear_cache', image_loader.OrderedDict())
    monkeypatch.setattr(image_loader, '_linear_inflight', {})
    monkeypatch.setattr(image_loader, '_LINEAR_CACHE_MAX_ENTRY_BYTES', 1)
    original = image_loader._postprocess_raw_linear
    calls = []
    started = threading.Event()
    release = threading.Event()

    def decode(raw):
        calls.append(True)
        started.set()
        assert release.wait(10)
        return original(raw)

    monkeypatch.setattr(image_loader, '_postprocess_raw_linear', decode)
    waiting = _signal_when_flight_has_waiters(monkeypatch, 2)
    results = [None] * 3

    def load(i):
        results[i] = load_image(dng, max_size=128, raw_decode=RAW_DECODE_LINEAR)

    leader = threading.Thread(target=load, args=(0,))
    leader.start()
    assert started.wait(10)
    followers = [threading.Thread(target=load, args=(i,)) for i in (1, 2)]
    for t in followers:
        t.start()
    assert waiting.wait(10)
    release.set()
    for t in [leader, *followers]:
        t.join(10)
    assert len(calls) == 1
    assert all(isinstance(r, FloatImage) for r in results)
    assert len({id(r.pixels) for r in results}) == 3
    assert len(image_loader._linear_cache) == 0
    assert image_loader._linear_inflight == {}


def test_newest_sized_raw_decode_is_cached_even_above_the_budget(dng, monkeypatch):
    """A zoomed-in editor source larger than the budget must still be reused."""
    monkeypatch.setattr(image_loader, '_linear_cache', image_loader.OrderedDict())
    monkeypatch.setattr(image_loader, '_linear_inflight', {})
    monkeypatch.setattr(image_loader, '_LINEAR_CACHE_BYTES', 1)
    original = image_loader._postprocess_raw_linear
    calls = []

    def decode(raw):
        calls.append(True)
        return original(raw)

    monkeypatch.setattr(image_loader, '_postprocess_raw_linear', decode)
    load_image(dng, max_size=128, raw_decode=RAW_DECODE_LINEAR)
    load_image(dng, max_size=128, raw_decode=RAW_DECODE_LINEAR)
    assert len(calls) == 1
    load_image(dng, max_size=256, raw_decode=RAW_DECODE_LINEAR)
    assert len(calls) == 2
    # Only the newest oversized entry stays resident.
    assert [key[-1] for key in image_loader._linear_cache] == [256]

    monkeypatch.setattr(image_loader, '_LINEAR_CACHE_MAX_ENTRY_BYTES', 1)
    load_image(dng, max_size=192, raw_decode=RAW_DECODE_LINEAR)
    assert [key[-1] for key in image_loader._linear_cache] == [256]


def test_linear_decode_fallback_remains_a_display_image(tmp_path, monkeypatch):
    from test_image_loader import _FakeRaw, _install_fake_raw, _jpeg_bytes

    path = tmp_path / 'unsupported.nef'
    path.write_bytes(b'unsupported RAW')
    raw = _FakeRaw(embedded_jpeg=_jpeg_bytes((64, 32)),
                   postprocess_error=rawpy.LibRawFileUnsupportedError(b'unsupported'))
    _install_fake_raw(monkeypatch, raw)
    image = load_image(path, max_size=128, raw_decode=RAW_DECODE_LINEAR)
    assert isinstance(image, Image.Image)
    assert image.size == (64, 32)
    assert raw.postprocess_kwargs[0]['output_bps'] == 16
    assert raw.postprocess_kwargs[0]['gamma'] == (1, 1)


@pytest.mark.parametrize("saved_edit", [False, True])
def test_edit_preview_and_export_render_same_real_raw(app_and_db, tmp_path, saved_edit):
    from export import export_photos, load_export_image

    app, db = app_and_db
    photo = db.get_photos()[0]
    folder = str(tmp_path / 'photos')
    os.makedirs(folder)
    db.conn.execute('UPDATE folders SET path=? WHERE id=?', (folder, photo['folder_id']))
    path = os.path.join(folder, 'precision.dng')
    write_dng(path)
    db.conn.execute(
        "UPDATE photos SET filename='precision.dng', extension='.dng', width=512, height=64 WHERE id=?",
        (photo['id'],),
    )
    db.conn.commit()
    recipe = {'adjustments': {'exposure': -1, 'highlights': -20, 'sharpen': 10}}
    if saved_edit:
        db.set_photo_edit_recipe(photo['id'], recipe)
    else:
        recipe = None
    photo = db.get_photo(photo['id'])
    vireo_dir = os.path.dirname(app.config['THUMB_CACHE_DIR'])
    export = load_export_image(photo, vireo_dir, {photo['folder_id']: folder}, recipe=recipe, output_ext='tiff')
    assert isinstance(export, FloatImage)
    response = app.test_client().get(f"/photos/{photo['id']}/edit-preview", query_string={
        'size': 512, 'apply_crop': 1, 'recipe': json.dumps(recipe or {}),
    })
    assert response.status_code == 200
    with Image.open(io.BytesIO(response.data)) as preview:
        assert preview.size == export.size
        assert np.abs(np.asarray(preview).astype(float) / 255 - export.pixels).mean() < 0.01
    result = export_photos(db, vireo_dir, [photo['id']], str(tmp_path / 'out'),
                           options={'format': 'tiff', 'metadata_fields': []})
    assert result['exported'] == 1, result
    with tifffile.TiffFile(tmp_path / 'out' / 'precision.tiff') as tiff:
        assert tiff.asarray().dtype == np.uint16


def test_straighten_preserves_float_geometry_and_source():
    pixels = np.broadcast_to(np.linspace(0.1, 0.2, 512, dtype=np.float32)[None, :, None],
                             (64, 512, 3)).copy()
    source = FloatImage(pixels)
    result = apply_recipe_to_loaded_image(source, {'straighten': 1.25})
    assert result.size == source.size
    assert len(np.unique(result.pixels[32, 10:-10, 0])) > 256
    np.testing.assert_array_equal(source.pixels, pixels)


@pytest.mark.parametrize("local", [False, True])
def test_camera_denoise_retains_float_precision_and_16_bit_export(local):
    from camera_denoise import _filter_strength

    rng = np.random.default_rng(1722)
    pixels = np.clip(0.3 + rng.normal(0, 0.02, (64, 96, 3)), 0, 1).astype(np.float32)
    image = FloatImage(pixels, encoding="srgb")
    assert _filter_strength(np.asarray(image.convert("RGB")), None, 1).max() > 0.5
    recipe = {'adjustments': {'denoise_mode': 'camera', 'noise_reduction': 60}}
    mask = None
    if local:
        recipe['local'] = {
            'mask': {'ref': 'a1b2c3d4e5f6', 'source_digest': 'test'},
            'regions': [{'region': 'subject', 'adjustments': {'noise_reduction': 30}}],
        }
        mask = Image.fromarray(np.tile(np.repeat([255, 0], 48).astype(np.uint8), (64, 1)))
    result = apply_recipe_to_loaded_image(image, recipe, local_mask=mask)
    assert isinstance(result, FloatImage)
    assert result.pixels.std() < pixels.std()
    assert np.max(np.abs(result.pixels * 255 - np.rint(result.pixels * 255))) > 0.1
    np.testing.assert_array_equal(image.pixels, pixels)
    stream = io.BytesIO()
    result.save(stream, format='TIFF')
    stream.seek(0)
    exported = tifffile.imread(stream)
    assert exported.dtype == np.uint16
    assert len(np.unique(exported)) > 256


def test_late_follower_joins_during_uncacheable_snapshot_copy(dng, monkeypatch):
    """A reader arriving during snapshot construction must not decode again."""
    import threading

    monkeypatch.setattr(image_loader, '_linear_cache', image_loader.OrderedDict())
    monkeypatch.setattr(image_loader, '_linear_inflight', {})
    monkeypatch.setattr(image_loader, '_LINEAR_CACHE_MAX_ENTRY_BYTES', 1)
    decoded, release_decode = threading.Event(), threading.Event()
    copying, release_copy = threading.Event(), threading.Event()
    calls, copies, results, errors = [], [], [], []
    original_copy = FloatImage.copy

    def decode(path, max_size):
        calls.append(True)
        decoded.set()
        assert release_decode.wait(10)
        return FloatImage(np.ones((2, 2, 3), dtype=np.float32))

    def copy(image):
        copies.append(True)
        if len(copies) == 1:
            copying.set()
            assert release_copy.wait(10)
        return original_copy(image)

    monkeypatch.setattr(image_loader, '_decode_linear_sized', decode)
    monkeypatch.setattr(FloatImage, 'copy', copy)
    # Reuse the fixture's parking hook, observing each join individually.
    first_waiter = _signal_when_flight_has_waiters(monkeypatch, 1)
    first_flight = image_loader._LinearDecodeFlight
    second_waiter = threading.Event()

    class Flight(first_flight):
        def __init__(self):
            super().__init__()
            original_wait = self.done.wait

            def wait(timeout=None):
                if self.waiters == 2:
                    second_waiter.set()
                return original_wait(timeout)

            self.done.wait = wait

    monkeypatch.setattr(image_loader, '_LinearDecodeFlight', Flight)

    def load():
        try:
            results.append(image_loader._load_linear_cached(dng, 128))
        except BaseException as exc:
            errors.append(exc)

    threads = []
    try:
        leader = threading.Thread(target=load)
        threads.append(leader)
        leader.start()
        assert decoded.wait(10)
        first = threading.Thread(target=load)
        threads.append(first)
        first.start()
        assert first_waiter.wait(10)
        release_decode.set()
        assert copying.wait(10)
        late = threading.Thread(target=load)
        threads.append(late)
        late.start()
        assert second_waiter.wait(3)
    finally:
        release_decode.set()
        release_copy.set()
        for thread in threads:
            thread.join(10)
    assert not errors
    assert all(not thread.is_alive() for thread in threads)
    assert len(calls) == 1
    assert len(results) == 3
    assert len({id(image.pixels) for image in results}) == 3
    assert not image_loader._linear_cache
    assert not image_loader._linear_inflight
