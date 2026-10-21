"""Check the actual app recipe without importing live application/config code."""
import ast
import io
import logging
import sys
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace

import pytest
from PIL import Image, ImageOps

from keeper_eval.curate import curate, decode_source, save_assets
from keeper_eval.region_target import APP_RECIPE_FILES, TARGET, freeze_region_target, region_contract

APP_CODE = Path(__file__).resolve().parents[3] / 'vireo'


def test_raw_native_region_matches_app_upright_embedded_recipe(tmp_path, monkeypatch):
    # Execute only the real pure embedded-preview function, not app imports.
    tree = ast.parse((APP_CODE / 'image_loader.py').read_text())
    function = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == '_extract_embedded_jpeg')
    namespace = {'io': io, 'Image': Image, 'ImageOps': ImageOps, 'log': logging.getLogger(__name__)}
    exec(compile(ast.Module(body=[function], type_ignores=[]), 'app-embedded-preview', 'exec'), namespace)
    pixels = Image.new('RGB', (1800, 1200), '#135790')
    exif = Image.Exif()
    exif[274] = 6
    buffer = io.BytesIO()
    pixels.save(buffer, format='JPEG', exif=exif)
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
    # Existing v1 bundles mean the same app-selected native region; do not
    # rewrite frozen manifests, images or independent judgments to add prose.
    legacy = deepcopy(evidence['manifest'])
    legacy.pop('region_target')
    assert region_contract(legacy) == value['contract']


@pytest.mark.parametrize('defect', ['small_preview', 'different_aspect', 'detector_claim'])
def test_unmappable_application_or_detector_input_claims_fail(evidence, defect):
    manifest = deepcopy(evidence['manifest'])
    sample = manifest['samples'][0]
    if defect == 'small_preview':
        sample.update(source_kind='original_embedded_jpeg', decoded_dimensions=[800, 600])
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
