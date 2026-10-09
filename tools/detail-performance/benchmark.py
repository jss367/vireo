#!/usr/bin/env python3
"""Compare editing filters on deterministic images or a private RAW corpus.

Each backend/size runs in a fresh process. RAW decoding is outside the timed
warm render; tone, detail, quantization and JPEG encoding are inside. This is
an in-process rendering experiment, not an HTTP/input-to-display benchmark.
"""

from __future__ import annotations

import argparse
import contextlib
import cProfile
import gc
import hashlib
import io
import json
import math
import os
import platform
import pstats
import statistics
import subprocess
import sys
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'vireo'))
BACKENDS = ('original', 'scipy', 'opencv', 'rust', 'rust-opencv', 'production')


@contextlib.contextmanager
def backend(name, threads):
    """Override kernels in this experiment only; restore on exit."""
    import cv2
    import detail
    from detail_backend import detail_thread_budget
    from scipy.ndimage import correlate1d

    cv2.setNumThreads(threads)
    gaussian, bilateral = detail._gaussian_blur, detail._bilateral
    provider = detail.filters_for_image
    if name != 'production':
        # Explicit baselines keep using NumPy after the accelerator is shipped.
        detail.filters_for_image = lambda _pixels: contextlib.nullcontext(None)
    filters = None
    if name.startswith('rust'):
        import vireo_detail_experiment
        filters = vireo_detail_experiment.Filters(threads)

    def scipy_gaussian(array, sigma):
        kernel = detail._gaussian_kernel(sigma)
        return correlate1d(correlate1d(array, kernel, axis=0, mode='mirror'),
                           kernel, axis=1, mode='mirror')

    def opencv_gaussian(array, sigma):
        import numpy as np
        kernel = detail._gaussian_kernel(sigma)
        unit = np.ones(1, dtype=np.float32)
        vertical = cv2.sepFilter2D(array, -1, unit, kernel, borderType=cv2.BORDER_REFLECT_101)
        return cv2.sepFilter2D(vertical, -1, kernel, unit, borderType=cv2.BORDER_REFLECT_101)

    if name == 'scipy':
        detail._gaussian_blur = scipy_gaussian
    elif name in ('opencv', 'rust-opencv'):
        detail._gaussian_blur = opencv_gaussian
    elif name == 'rust':
        detail._gaussian_blur = lambda array, sigma: filters.gaussian(
            array, detail._gaussian_kernel(sigma))
    elif name not in ('original', 'production'):
        raise ValueError(f'Unknown backend: {name}')
    if filters:
        detail._bilateral = filters.bilateral
    try:
        with detail_thread_budget(threads):
            yield
    finally:
        detail._gaussian_blur, detail._bilateral = gaussian, bilateral
        detail.filters_for_image = provider


def synthetic(width, height):
    """Noise, smooth color gradients and hard edges, generated one row at a time."""
    import numpy as np
    from float_image import FloatImage

    rng = np.random.default_rng(42)
    data = np.empty((height, width, 3), dtype=np.float32)
    x = np.linspace(0, 1, width, dtype=np.float32)
    for y in range(height):
        row = np.column_stack((x, np.full(width, y / max(1, height - 1)),
                               .2 + .6 * (x > .5))).astype(np.float32)
        data[y] = np.clip(row + rng.normal(0, .03, (width, 3)), 0, 1)
    return FloatImage(data, encoding='linear')


def load_source(spec):
    from float_image import FloatImage
    from image_loader import RAW_DECODE_LINEAR, load_image

    size = spec['size']
    if spec.get('path'):
        image = load_image(spec['path'], max_size=size, raw_decode=RAW_DECODE_LINEAR)
        if not isinstance(image, FloatImage) or image.encoding != 'linear':
            raise RuntimeError('RAW decode did not deliver a linear floating-point source')
        return image
    return synthetic(size, max(1, round(size * 2 / 3)))


def render(source, recipe, native_size):
    from image_edits import apply_recipe_to_loaded_image

    image = apply_recipe_to_loaded_image(source, recipe, native_size=native_size)
    with image.to_pil() as display:
        encoded = io.BytesIO()
        display.save(encoded, format='JPEG', quality=90)
    return image


def fidelity(actual, expected):
    import numpy as np

    result = {'max_float_error': 0.0}
    squared_error = 0.0
    for bits in (8, 16):
        result[f'changed_{bits}bit_channels'] = 0
        result[f'max_{bits}bit_error'] = 0
    for top in range(0, actual.shape[0], 128):
        a_tile, b_tile = actual[top:top + 128], expected[top:top + 128]
        difference = np.abs(a_tile - b_tile)
        result['max_float_error'] = max(result['max_float_error'], float(difference.max()))
        squared_error += float(np.sum(difference.astype(np.float64) ** 2))
        for bits in (8, 16):
            maximum = (1 << bits) - 1
            a = np.clip(a_tile * maximum + .5, 0, maximum).astype(np.int32)
            b = np.clip(b_tile * maximum + .5, 0, maximum).astype(np.int32)
            delta = np.abs(a - b)
            result[f'changed_{bits}bit_channels'] += int(np.count_nonzero(delta))
            result[f'max_{bits}bit_error'] = max(result[f'max_{bits}bit_error'], int(delta.max()))
    result['rmse'] = float(np.sqrt(squared_error / actual.size))
    result['passes'] = result['max_float_error'] <= 2e-6 and result['max_16bit_error'] <= 1
    return result


def profile_render(source, recipe, native_size):
    profiler = cProfile.Profile()
    result = profiler.runcall(render, source, recipe, native_size)
    result.close()
    rows = []
    for (filename, line, function), (_, calls, own, cumulative, _) in pstats.Stats(profiler).stats.items():
        if Path(filename).name in ('detail.py', 'image_edits.py', 'tone.py', 'float_image.py'):
            rows.append({'file': Path(filename).name, 'line': line, 'function': function,
                         'calls': calls, 'own_ms': round(own * 1000, 2),
                         'cumulative_ms': round(cumulative * 1000, 2)})
    return sorted(rows, key=lambda row: row['cumulative_ms'], reverse=True)[:20]


def worker(spec):
    import psutil

    source = load_source(spec)
    native_size = spec['native_dimensions']
    recipe = {'adjustments': {'exposure': .3, 'shadows': 15}}
    if spec['recipe'] == 'detail':
        recipe['adjustments'].update(sharpen=55, sharpen_radius=1.8, noise_reduction=45)
    process = psutil.Process()
    with backend(spec['backend'], spec['threads']):
        render(source, recipe, native_size).close()  # Exclude imports, thread creation and first-call setup.
        gc.collect()
        baseline = process.memory_info().rss
        peak = [baseline]
        stop = threading.Event()

        def sample():
            while not stop.wait(.005):
                peak[0] = max(peak[0], process.memory_info().rss)

        sampler = threading.Thread(target=sample, daemon=True)
        sampler.start()
        timings = []
        actual = None
        try:
            for _ in range(spec['samples']):
                if actual is not None:
                    actual.close()
                start = time.perf_counter()
                actual = render(source, recipe, native_size)
                timings.append((time.perf_counter() - start) * 1000)
                peak[0] = max(peak[0], process.memory_info().rss)
        finally:
            stop.set()
            sampler.join()
    with backend('original', spec['threads']):
        reference = render(source, recipe, native_size)
        profile = profile_render(source, recipe, native_size) if spec['backend'] == 'original' else None
    comparison = fidelity(actual.pixels, reference.pixels)
    return {'source': spec['name'], 'backend': spec['backend'], 'recipe': spec['recipe'],
            'dimensions': list(source.size), 'samples_ms': [round(t, 2) for t in timings],
            'median_ms': round(statistics.median(timings), 2),
            'p95_ms': round(sorted(timings)[math.ceil(len(timings) * .95) - 1], 2),
            'warm_baseline_rss_mib': round(baseline / 1024**2, 2),
            'peak_rss_mib': round(peak[0] / 1024**2, 2),
            'fidelity': comparison, 'profile': profile}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--manifest', type=Path)
    parser.add_argument('--extension-dir', type=Path)
    parser.add_argument('--output', type=Path)
    parser.add_argument('--sizes', nargs='+', type=int, default=[1024, 2048, 6000])
    parser.add_argument('--backends', nargs='+', choices=BACKENDS, default=list(BACKENDS))
    parser.add_argument('--recipes', nargs='+', choices=['basic', 'detail'], default=['basic', 'detail'])
    parser.add_argument('--threads', type=int, choices=range(1, 5), default=4)
    parser.add_argument('--samples', type=int, default=5)
    parser.add_argument('--worker', help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.worker:
        print(json.dumps(worker(json.loads(args.worker))))
        return
    if args.samples < 1 or any(size < 1 for size in args.sizes):
        parser.error('Samples and sizes must be positive')
    entries = [{'name': 'synthetic', 'path': None, 'native_dimensions': [6000, 4000]}]
    if args.manifest:
        entries = json.loads(args.manifest.read_text())
        if not isinstance(entries, list) or not entries:
            parser.error('Manifest must be a nonempty list of {name, path}')
        entries = [dict(entry, path=str((args.manifest.parent / entry['path']).resolve()))
                   for entry in entries]
    env = dict(os.environ, OMP_NUM_THREADS=str(args.threads),
               OPENBLAS_NUM_THREADS=str(args.threads), MKL_NUM_THREADS=str(args.threads))
    if args.extension_dir:
        env['PYTHONPATH'] = str(args.extension_dir.resolve()) + os.pathsep + env.get('PYTHONPATH', '')
    import cv2
    import numpy as np
    import PIL
    import scipy
    report = {'schema': 1, 'kind': 'warm in-process editing and JPEG encoding',
              'environment': {'platform': platform.platform(), 'python': platform.python_version(),
                              'numpy': np.__version__, 'scipy': scipy.__version__,
                              'opencv': cv2.__version__, 'pillow': PIL.__version__,
                              'threads': args.threads, 'cpu_count': os.cpu_count()},
              'source_revision': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(),
              'working_tree_dirty': bool(subprocess.check_output(['git', 'status', '--porcelain'], cwd=ROOT, text=True)),
              'source_hashes': {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest()
                                for path in [Path(__file__), ROOT / 'vireo/detail.py', ROOT / 'vireo/tone.py',
                                             ROOT / 'vireo/image_edits.py', Path(__file__).parent / 'src/lib.rs',
                                             ROOT / 'vireo/detail_backend.py', ROOT / 'native/detail/src/lib.rs']},
              'results': []}
    for entry in entries:
        if entry['path']:
            import rawpy
            with rawpy.imread(entry['path']) as raw:
                entry['native_dimensions'] = [raw.sizes.width, raw.sizes.height]
            with Path(entry['path']).open('rb') as stream:
                entry['sha256'] = hashlib.file_digest(stream, 'sha256').hexdigest()
        for size in args.sizes:
            for recipe in args.recipes:
                for name in args.backends:
                    spec = dict(entry, size=size, recipe=recipe, backend=name,
                                threads=args.threads, samples=args.samples)
                    child = subprocess.run([sys.executable, str(Path(__file__).resolve()), '--worker', json.dumps(spec)],
                                           env=env, capture_output=True, text=True, check=True)
                    row = json.loads(child.stdout)
                    row['source_sha256'] = entry.get('sha256')
                    report['results'].append(row)
                    print(f"{entry['name']} {size} {recipe} {name}: {row['median_ms']} ms, "
                          f"{row['peak_rss_mib']} MiB, fidelity={row['fidelity']['passes']}", flush=True)
                    if args.output:
                        args.output.parent.mkdir(parents=True, exist_ok=True)
                        args.output.write_text(json.dumps(report, indent=2) + '\n')
    if any(not row['fidelity']['passes'] for row in report['results']):
        raise SystemExit('Output fidelity failed')


if __name__ == '__main__':
    main()
