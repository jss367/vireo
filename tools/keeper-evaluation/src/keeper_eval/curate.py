"""Read original bytes through scratch copies; never initialize the live application."""
import io
import math
import re
import secrets
import shutil
import sqlite3
import tempfile
from datetime import UTC, datetime
from pathlib import Path

from PIL import Image, ImageDraw, ImageOps

from .manifest import SCHEMA, digest, file_digest, valid_box, validate_samples, write_json

PIXEL_RECIPE = 'upright-rgb-embedded-jpeg-or-raster-v1'
RAW_EXTENSIONS = {'.nef', '.nrw', '.arw', '.cr2', '.cr3', '.dng', '.orf', '.rw2', '.raf'}


def decode_source(path):
    if path.suffix.lower() in RAW_EXTENSIONS:
        import rawpy
        with rawpy.imread(str(path)) as raw:
            thumb = raw.extract_thumb()
            if thumb.format != rawpy.ThumbFormat.JPEG:
                raise ValueError('An embedded JPEG is required; no silent demosaic/working-copy fallback')
            image = Image.open(io.BytesIO(thumb.data))
            image.load()
        import exifread
        with path.open('rb') as stream:
            tags = exifread.process_file(stream, details=False, stop_tag='Model')
        source_camera = str(tags.get('Image Model') or 'unknown').strip()
        kind = 'original_embedded_jpeg'
    else:
        image = Image.open(path)
        image.load()
        source_camera = str(image.getexif().get(272) or 'unknown').strip()
        kind = 'original_raster'
    image = ImageOps.exif_transpose(image).convert('RGB')
    # Evidence PNGs intentionally carry no source EXIF/GPS/ICC metadata.
    return Image.frombytes('RGB', image.size, image.tobytes()), kind, source_camera


def catalog_sample(conn, requested):
    photo = conn.execute('SELECT p.id,p.filename,p.width,p.height,p.camera_model,f.path FROM photos p '
                         'JOIN folders f ON f.id=p.folder_id WHERE p.id=?', (requested['photo_id'],)).fetchone()
    if photo is None:
        raise ValueError('Requested photo is absent from the catalog')
    detection = conn.execute('SELECT * FROM detections WHERE id=? AND photo_id=?',
                             (requested['detection_id'], photo['id'])).fetchone()
    if detection is None:
        raise ValueError('Selected detection must belong to the requested photo')
    box = [detection['box_' + k] for k in 'xywh']
    if not valid_box(box):
        raise ValueError('Selected detection has invalid normalized geometry')
    return photo, detection, box


def save_assets(bundle, sid, image, box):
    directory = bundle / 'samples' / sid
    directory.mkdir(parents=True)
    width, height = image.size
    x, y, w, h = box
    rect = [max(0, math.floor(x * width)), max(0, math.floor(y * height)),
            min(width, math.ceil((x + w) * width)), min(height, math.ceil((y + h) * height))]
    if rect[2] <= rect[0] or rect[3] <= rect[1]:
        raise ValueError('Selected box has no decoded pixels')
    crop = image.crop(rect)
    image.save(directory / 'source.png')
    crop.save(directory / 'region.png')
    context = image.copy()
    ImageDraw.Draw(context).rectangle(rect, outline='#ff3050', width=max(2, width // 500))
    context.thumbnail((1400, 1000), Image.Resampling.LANCZOS)
    context.save(directory / 'context.jpg', quality=92)
    assets = {}
    for key, filename in [('source', 'source.png'), ('region', 'region.png'), ('context', 'context.jpg')]:
        path = directory / filename
        with Image.open(path) as asset:
            assets[key] = {'path': path.relative_to(bundle).as_posix(), 'sha256': file_digest(path),
                           'width': asset.width, 'height': asset.height}
    return rect, assets


def curate(db_path, specification, output):
    db_path, output = Path(db_path).resolve(), Path(output).resolve()
    requests = specification['samples']
    if output.is_relative_to(db_path.parent):
        raise ValueError("Evidence output must be outside the live catalog directory")
    if output.exists() or not requests:
        raise ValueError('Use a new output directory and a nonempty explicit sample selection')
    # Validate every path before creating output or copying an original. Do not
    # write anywhere under a selected source directory or the catalog itself.
    observations = []
    with sqlite3.connect(db_path.as_uri() + '?mode=ro', uri=True) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute('PRAGMA query_only=ON')
        conn.execute('BEGIN')
        for requested in requests:
            requested = dict(requested)
            requested.setdefault('sample_id', secrets.token_hex(6))
            if (not re.fullmatch(r'[a-f0-9]{12}', requested['sample_id'])
                    or requested.get('partition') not in {'development', 'holdout', 'diagnostic'}
                    or not requested.get('group_id') or not requested.get('session_id')):
                raise ValueError('Declare blinded ID, group/session membership and partition before curation')
            photo, detection, box = catalog_sample(conn, requested)
            folder = Path(photo['path']).resolve()
            if output.is_relative_to(folder) or output == db_path or db_path.is_relative_to(output):
                raise ValueError('Evidence output must be separate from the catalog and source directories')
            source = folder / photo['filename']
            if not source.is_file():
                raise FileNotFoundError(f'Original unavailable for photo {photo["id"]}; no cache fallback')
            observations.append((requested, dict(photo), dict(detection), box, source))
    output.mkdir(parents=True)
    samples = []
    with tempfile.TemporaryDirectory(prefix='vireo-keeper-source-') as scratch:
        for requested, photo, detection, box, source in observations:
            sid = requested.get('sample_id') or secrets.token_hex(6)
            copied = Path(scratch) / ('source' + source.suffix)
            # Hash the same copied bytes that are decoded; catalog hashes and
            # model selections do not constitute independent visual labels.
            shutil.copyfile(source, copied)
            source_hash = file_digest(copied)
            image, kind, source_camera = decode_source(copied)
            rect, assets = save_assets(output, sid, image, box)
            samples.append({'sample_id': sid, 'partition': requested['partition'], 'group_id': requested['group_id'],
                            'session_id': requested['session_id'], 'lineage_id': requested.get('lineage_id', source_hash),
                            'source_sha256': source_hash, 'source_path': str(source), 'photo_id': photo['id'],
                            'detection_id': detection['id'], 'detector_model': detection['detector_model'],
                            'detector_runtime': detection['runtime_fingerprint'], 'box': box, 'pixel_rect': rect,
                            'camera_model': source_camera, 'catalog_camera_model': photo['camera_model'], 'source_kind': kind,
                            'catalog_dimensions': [photo['width'], photo['height']], 'decoded_dimensions': list(image.size),
                            'pixel_recipe': PIXEL_RECIPE, 'assets': assets})
    validate_samples(samples)
    manifest = {'schema': SCHEMA, 'created_at': datetime.now(UTC).isoformat(), 'pixel_recipe': PIXEL_RECIPE,
                'source_catalog': str(db_path), 'label_definition': specification['label_definition'],
                'review_protocol': specification.get('review_protocol', {'minimum_human_reviewers': 1,
                    'description': 'Independent photographer, blind to model scores and flags; no adjacent-frame inference.'}),
                'sampling_notes': specification.get('sampling_notes', ''), 'samples': samples}
    manifest['manifest_sha256'] = digest(manifest)
    write_json(output / 'manifest.json', manifest)
    return manifest
