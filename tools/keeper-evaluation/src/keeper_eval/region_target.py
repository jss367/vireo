"""Bind evidence to the application's selected region, not an unknown detector rendition."""
from pathlib import Path

from .manifest import file_digest

PIXEL_RECIPE = 'upright-rgb-embedded-jpeg-or-raster-v1'
TARGET = 'application-subject-analysis-jpeg-first-normalized-box-v1'
APP_RECIPE_FILES = ('subject_analysis.py', 'image_loader.py', 'focus_crop.py')


def region_contract(manifest):
    if manifest['pixel_recipe'] != PIXEL_RECIPE or manifest.get('region_target', TARGET) != TARGET:
        raise ValueError('Unsupported region target; original detector-input pixels are not reconstructable from legacy boxes')
    for sample in manifest['samples']:
        width, height = sample['decoded_dimensions']
        if (sample['source_kind'] == 'original_embedded_jpeg' and max(width, height) < 1024):
            raise ValueError('Embedded JPEG cannot reproduce application subject-analysis input below its 1024px request')
        catalog = sample['catalog_dimensions']
        if all(catalog):
            expected = max(catalog) / min(catalog)
            actual = max(width, height) / min(width, height)
            if abs(actual / expected - 1) > .02:
                raise ValueError('Application source-raster geometry guard rejects this aspect mismatch; no inferred detector transform')
    return {'name': TARGET, 'pixel_recipe': PIXEL_RECIPE,
            'coordinates': 'stored upright unedited normalized xywh applied to the decoded raster',
            'application_input': 'subject_analysis.analyze_detection JPEG-first load_image max_size=1024',
            'evidence_region': 'native unresampled bounding box; floor left/top and ceil right/bottom',
            'detector_input_rendition': 'unknown; no geometric equivalence to original inference pixels claimed',
            'measurement_limit': 'same selected region geometry, not app resized pixels or SAM mask measurements'}


def freeze_region_target(manifest, application_code):
    contract = region_contract(manifest)
    root = Path(application_code).resolve()
    return {'contract': contract, 'application_source_sha256': {name: file_digest(root / name) for name in APP_RECIPE_FILES}}
