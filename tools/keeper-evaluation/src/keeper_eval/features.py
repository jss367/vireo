"""Candidate measurements, not usability judgments: native detail plus frozen DINOv2 embeddings."""
import importlib.metadata
import time
from pathlib import Path

import numpy as np
from PIL import Image

from .manifest import digest, read_bundle, tree_fingerprint, write_json

FEATURE_RECIPE = 'native-luma-128-grid4-dinov2-518-center-v1'


def native_detail(image):
    """Measure native tiles without resampling or making a sharp/blur claim."""
    gray = np.asarray(image.convert('L'), dtype=np.float32) / 255.
    height, width = gray.shape
    if min(height, width) < 8:
        return {'status': 'unavailable', 'reason': 'region_too_small_for_native_derivatives'}
    tw, th = min(128, width), min(128, height)
    values = []
    for y in sorted({round(v) for v in np.linspace(0, height - th, 4)}):
        for x in sorted({round(v) for v in np.linspace(0, width - tw, 4)}):
            tile = gray[y:y + th, x:x + tw]
            variance = float(np.var(tile))
            gx = tile[1:-1, 2:] - tile[1:-1, :-2]
            gy = tile[2:, 1:-1] - tile[:-2, 1:-1]
            lap = tile[1:-1, 2:] + tile[1:-1, :-2] + tile[2:, 1:-1] + tile[:-2, 1:-1] - 4 * tile[1:-1, 1:-1]
            values.append([variance, float(np.mean(gx * gx + gy * gy)), float(np.var(lap))])
    measured = np.asarray(values)
    normalizable = measured[:, 0] > 1e-12
    return {'status': 'measured', 'native_region_size': [width, height], 'tile_size': [tw, th],
            'tile_count': len(values), 'luma_mean': float(np.mean(gray)),
            'luma_clip_low': float(np.mean(gray <= 1 / 255)), 'luma_clip_high': float(np.mean(gray >= 254 / 255)),
            'median_variance': float(np.median(measured[:, 0])),
            'median_gradient_energy': float(np.median(measured[:, 1])),
            'median_laplacian_variance': float(np.median(measured[:, 2])),
            'median_contrast_normalized_gradient': float(np.median(measured[normalizable, 1] / measured[normalizable, 0])) if normalizable.any() else None,
            'median_contrast_normalized_laplacian': float(np.median(measured[normalizable, 2] / measured[normalizable, 0])) if normalizable.any() else None}


def dino_tensor(image):
    """Freeze the same 518px resize/center-crop and normalization as Vireo's DINOv2 path."""
    width, height = image.size
    scale = max(518 / width, 518 / height)
    resized = image.resize((int(width * scale + .5), int(height * scale + .5)), Image.Resampling.BICUBIC)
    left, top = (resized.width - 518) // 2, (resized.height - 518) // 2
    arr = np.asarray(resized.crop((left, top, left + 518, top + 518)), dtype=np.float32) / 255.
    arr = (arr - np.array([.485, .456, .406], dtype=np.float32)) / np.array([.229, .224, .225], dtype=np.float32)
    return arr.transpose(2, 0, 1)[None]


def extract(bundle, model_directory, output, *, limit=20, threads=2):
    """One local CPU session, bounded batch; no downloads, live catalog or config access."""
    import onnxruntime as ort
    if not 1 <= threads <= 2 or not 1 <= limit <= 40 or Path(output).exists():
        raise ValueError('Use a new output path, at most 40 samples and at most two CPU threads')
    manifest = read_bundle(bundle)
    if len(manifest['samples']) > limit:
        raise ValueError('Explicit sample limit is smaller than this frozen bundle')
    model_directory = Path(model_directory).resolve()
    model = tree_fingerprint(model_directory)
    source = tree_fingerprint(Path(__file__).parent)
    options = ort.SessionOptions()
    options.intra_op_num_threads = threads
    options.inter_op_num_threads = 1
    options.execution_mode = ort.ExecutionMode.ORT_SEQUENTIAL
    session = ort.InferenceSession(str(model_directory / 'model.onnx'), sess_options=options,
                                   providers=['CPUExecutionProvider'])
    if session.get_inputs()[0].shape[-2:] != [518, 518]:
        raise ValueError('This extractor requires the installed 518px DINOv2 export')
    rows, started = [], time.monotonic()
    for sample in manifest['samples']:
        with Image.open(Path(bundle) / sample['assets']['region']['path']) as opened:
            image = opened.convert('RGB')
        tensor = dino_tensor(image)
        vector = np.asarray(session.run(None, {session.get_inputs()[0].name: tensor})[0][0], dtype=np.float32)
        if vector.ndim != 1 or vector.size != 768 or not np.isfinite(vector).all():
            raise ValueError('Expected finite 768-dimensional DINOv2-B features')
        rows.append({'sample_id': sample['sample_id'], 'region_sha256': sample['assets']['region']['sha256'],
                     'native_detail': native_detail(image), 'embedding': vector.tolist()})
    if tree_fingerprint(model_directory) != model or tree_fingerprint(Path(__file__).parent) != source:
        raise ValueError('Model/code changed during extraction; discard this run')
    result = {'schema': 'vireo-keeper-candidate-features-v1', 'manifest_sha256': manifest['manifest_sha256'],
              'feature_recipe': FEATURE_RECIPE, 'model_artifacts': model, 'producer_code': source,
              'runtime': {name: importlib.metadata.version(name) for name in ('numpy', 'Pillow', 'onnxruntime')},
              'provider': 'CPUExecutionProvider', 'cpu_threads': threads, 'elapsed_seconds': time.monotonic() - started,
              'samples': rows, 'calibrated': False, 'runtime_approved': False,
              'note': 'Measured features only. No focus/animal decision, training labels, threshold or keeper approval.'}
    result['features_sha256'] = digest(result)
    write_json(output, result)
    return result
