"""Check the actual app recipe without importing live application/config code."""
import ast
import io
import logging
import sys
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest
from PIL import Image, ImageCms, ImageOps

from keeper_eval.curate import curate, decode_source, save_assets
from keeper_eval.region_target import (
    APP_RECIPE_FILES,
    LEGACY_PIXEL_RECIPE,
    PIXEL_RECIPE,
    TARGET,
    freeze_region_target,
    region_contract,
)

APP_CODE = Path(__file__).resolve().parents[3] / 'vireo'


@pytest.mark.parametrize('embedded_icc', [False, True])
def test_raw_native_region_matches_app_upright_embedded_recipe(tmp_path, monkeypatch, embedded_icc):
    # Execute only the real pure embedded-preview function, not app imports.
    tree = ast.parse((APP_CODE / 'image_loader.py').read_text())
    function = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == '_extract_embedded_jpeg')
    namespace = {'io': io, 'Image': Image, 'ImageOps': ImageOps, 'log': logging.getLogger(__name__)}
    exec(compile(ast.Module(body=[function], type_ignores=[]), 'app-embedded-preview', 'exec'), namespace)
    pixels = Image.new('RGB', (1800, 1200), '#135790')
    exif = Image.Exif()
    exif[274] = 6
    buffer = io.BytesIO()
    profile = ImageCms.ImageCmsProfile(ImageCms.createProfile('LAB')).tobytes() if embedded_icc else None
    pixels.save(buffer, format='JPEG', exif=exif, icc_profile=profile)
    thumb = SimpleNamespace(format=1, data=buffer.getvalue())
    raw = SimpleNamespace(extract_thumb=lambda: thumb)

    class Context:
        def __enter__(self):
            return raw
        def __exit__(self, *_):
            return False

    monkeypatch.setitem(sys.modules, 'rawpy', SimpleNamespace(imread=lambda _: Context(), ThumbFormat=SimpleNamespace(JPEG=1)))
    monkeypatch.setitem(sys.modules, 'exifread', SimpleNamespace(process_file=lambda *a, **k: {'Image Model': 'test-camera'}))
    original = tmp_path / 'source.nef'
    original.write_bytes(b'fake container; real embedded JPEG')
    app_image = namespace['_extract_embedded_jpeg'](raw)
    evidence_image, kind, _ = decode_source(original)
    assert kind == 'original_embedded_jpeg'
    assert evidence_image.size == app_image.size == (1200, 1800)
    assert evidence_image.tobytes() == app_image.tobytes()
    _, assets = save_assets(tmp_path / 'bundle', '000000000001', evidence_image, [.25, .25, .5, .5])
    with Image.open(tmp_path / 'bundle' / assets['region']['path']) as region:
        assert region.tobytes() == app_image.crop((300, 450, 900, 1350)).tobytes()


def test_app_subject_recipe_still_uses_jpeg_first_1024_normalized_boxes():
    source = (APP_CODE / 'subject_analysis.py').read_text()
    tree = ast.parse(source)
    function = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == 'analyze_detection')
    calls = [n for n in ast.walk(function) if isinstance(n, ast.Call) and isinstance(n.func, ast.Name) and n.func.id == 'load_image']
    assert len(calls) == 1
    assert {k.arg: ast.literal_eval(k.value) for k in calls[0].keywords} == {'max_size': 1024}
    assert 'detection["box_" + k]' in source


def test_region_contract_binds_current_app_source_and_unknown_detector_rendition(evidence):
    value = freeze_region_target(evidence['manifest'], APP_CODE)
    assert set(value['application_source_sha256']) == set(APP_RECIPE_FILES)
    assert all(len(h) == 64 for h in value['application_source_sha256'].values())
    assert value['contract']['name'] == TARGET
    assert 'unknown' in value['contract']['detector_input_rendition']
    # Missing optional target prose does not alter a frozen recipe; do not
    # rewrite frozen manifests, images or independent judgments to add prose.
    legacy = deepcopy(evidence['manifest'])
    legacy.pop('region_target')
    assert region_contract(legacy) == value['contract']


@pytest.mark.parametrize('defect', ['small_preview', 'different_aspect', 'detector_claim'])
def test_unmappable_application_or_detector_input_claims_fail(evidence, defect):
    manifest = deepcopy(evidence['manifest'])
    sample = manifest['samples'][0]
    if defect == 'small_preview':
        sample.update(source_kind='original_embedded_jpeg', source_path='synthetic.nef', decoded_dimensions=[800, 600])
    elif defect == 'different_aspect':
        sample['decoded_dimensions'] = [2000, 600]
    else:
        manifest['region_target'] = 'original-detector-input'
    with pytest.raises(ValueError, match='cannot|mismatch|Unsupported'):
        region_contract(manifest)


def test_curator_refuses_unknown_detector_rendition_without_changing_pixels(evidence, tmp_path):
    spec = dict(evidence['spec'], region_target='original-detector-input')
    with pytest.raises(ValueError, match='unknown detector-input'):
        curate(evidence['db'], spec, tmp_path / 'not-created')
    assert not (tmp_path / 'not-created').exists()


def test_truncated_preview_is_not_substituted_for_app_demosaic(tmp_path):
    tree = ast.parse((APP_CODE / 'image_loader.py').read_text())
    function = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == '_embedded_preview_suffices')
    namespace = {'RAW_DECODE_JPEG_FIRST': 'jpeg-first', 'RAW_DECODE_CAMERA_RENDERED': 'camera-rendered'}
    exec(compile(ast.Module(body=[function], type_ignores=[]), 'app-preview-choice', 'exec'), namespace)
    raw = SimpleNamespace(sizes=SimpleNamespace(width=6000, height=4000))
    choose = namespace['_embedded_preview_suffices']
    assert choose(raw, Image.new('RGB', (1800, 1200)), 1024, 'jpeg-first')
    assert not choose(raw, Image.new('RGB', (800, 600)), 1024, 'jpeg-first')


@pytest.mark.parametrize('kind', ['lab_icc', 'rgb_icc', '16_bit_png'])
def test_raster_native_pixels_match_actual_app_color_and_depth_transforms(tmp_path, monkeypatch, kind):
    tree = ast.parse((APP_CODE / 'image_loader.py').read_text())
    function = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == 'load_image' and not n.decorator_list)
    namespace = {'Path': Path, 'Image': Image, 'ImageOps': ImageOps, 'io': io, 'log': logging.getLogger(__name__),
                 'SUPPORTED_EXTENSIONS': {'.png', '.tiff'}, 'RAW_EXTENSIONS': set(),
                 'RAW_DECODE_JPEG_FIRST': 'jpeg-first', '_RAW_DECODE_MODES': {'jpeg-first'}}
    exec(compile(ast.Module(body=[function], type_ignores=[]), 'app-native-raster', 'exec'), namespace)
    path = tmp_path / ('sample.tiff' if kind == 'lab_icc' else 'sample.png')
    if kind == 'lab_icc':
        image = Image.new('LAB', (5, 3), (128, 145, 110))
        profile = ImageCms.ImageCmsProfile(ImageCms.createProfile('LAB')).tobytes()
        image.save(path, icc_profile=profile)
    elif kind == 'rgb_icc':
        image = Image.new('RGB', (5, 3), '#a6247c')
        profile = ImageCms.ImageCmsProfile(ImageCms.createProfile('sRGB')).tobytes()
        image.save(path, icc_profile=profile)
    else:
        image = Image.fromarray(np.array([[0, 257, 32768, 65535]], dtype=np.uint16))
        image.save(path)
    transforms = []
    transform = ImageCms.profileToProfile

    def counted_transform(*args, **kwargs):
        transforms.append(True)
        return transform(*args, **kwargs)

    monkeypatch.setattr(ImageCms, 'profileToProfile', counted_transform)
    app = namespace['load_image'](path, max_size=None)
    actual, source_kind, _ = decode_source(path)
    assert app is not None and source_kind == 'original_raster'
    assert actual.size == app.size and actual.tobytes() == app.tobytes()
    assert not actual.getexif() and 'icc_profile' not in actual.info
    assert len(transforms) == (0 if kind == '16_bit_png' else 2)
    if kind == '16_bit_png':
        assert [p[0] for p in actual.get_flattened_data()] == [0, 1, 128, 255]


def test_unsupported_raster_profile_fails_instead_of_claiming_conversion(tmp_path):
    path = tmp_path / 'unsupported.png'
    Image.new('RGB', (8, 8), '#c08040').save(path, icc_profile=b'not a valid ICC transform')
    with pytest.raises(ValueError, match='Unsupported raster ICC profile'):
        decode_source(path)


def test_legacy_raster_recipe_cannot_claim_new_app_color_parity(evidence):
    manifest = deepcopy(evidence['manifest'])
    assert manifest['pixel_recipe'] == PIXEL_RECIPE
    manifest['pixel_recipe'] = LEGACY_PIXEL_RECIPE
    for sample in manifest['samples']:
        sample['pixel_recipe'] = LEGACY_PIXEL_RECIPE
    with pytest.raises(ValueError, match='Legacy raster color recipe'):
        region_contract(manifest)
    # The old RAW-only recipe already follows the actual app helper, including
    # its deliberate lack of raster-style ICC conversion. Preserve that scope.
    for sample in manifest['samples']:
        sample.update(source_kind='original_embedded_jpeg', source_path='synthetic.nef', decoded_dimensions=[1600, 1200])
    assert region_contract(manifest)['pixel_recipe'] == LEGACY_PIXEL_RECIPE


def test_source_extension_allowlists_match_actual_app_without_importing_it():
    from keeper_eval import source_formats
    tree = ast.parse((APP_CODE / 'image_loader.py').read_text())
    names = {'RAW_EXTENSIONS', 'IMAGE_EXTENSIONS', 'SUPPORTED_EXTENSIONS'}
    assignments = [n for n in tree.body if isinstance(n, ast.Assign)
                   and any(isinstance(t, ast.Name) and t.id in names for t in n.targets)]
    namespace = {}
    exec(compile(ast.Module(body=assignments, type_ignores=[]), 'app-format-constants', 'exec'), namespace)
    assert set(namespace) >= names
    for name in names:
        assert getattr(source_formats, name) == namespace[name]


@pytest.mark.parametrize('extension', ['.nrw', '.gif', '.ppm', '.avif', '.unknown', ''])
def test_unsupported_source_formats_are_rejected_before_decoding(tmp_path, monkeypatch, extension):
    from keeper_eval.curate import decode_raster
    path = tmp_path / ('source' + extension)
    # Valid Pillow-readable bytes must not override the source extension contract.
    Image.new('RGB', (8, 8), '#abcdef').save(path, format='PNG')

    def forbidden_decode(*args, **kwargs):
        pytest.fail('Unsupported source reached Pillow instead of failing its allowlist')

    monkeypatch.setattr(Image, 'open', forbidden_decode)
    with pytest.raises(ValueError, match='Unsupported application source format'):
        decode_source(path)
    with pytest.raises(ValueError, match='Unsupported application raster format'):
        decode_raster(path)


@pytest.mark.parametrize('extension', ['.NRW', '.gif', '.ppm'])
def test_legacy_frozen_unsupported_source_cannot_claim_app_region(evidence, extension):
    manifest = deepcopy(evidence['manifest'])
    sample = manifest['samples'][0]
    sample['source_path'] = 'source' + extension
    with pytest.raises(ValueError, match='Unsupported application source format'):
        region_contract(manifest)


def test_frozen_source_kind_must_match_application_format(evidence):
    manifest = deepcopy(evidence['manifest'])
    manifest['samples'][0]['source_kind'] = 'original_embedded_jpeg'
    with pytest.raises(ValueError, match='source kind does not match'):
        region_contract(manifest)
