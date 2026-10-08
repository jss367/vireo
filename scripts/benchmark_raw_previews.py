#!/usr/bin/env python3
"""Benchmark the real edit-preview endpoint with a local, untracked RAW corpus.

Each file/size/cache scenario runs in a fresh process and temporary catalog.
Cold means Vireo's decoded-source cache is empty, not that the OS disk cache is
empty. Timings include decode, edits and JPEG encoding, but not browser display.
"""

import argparse
import gc
import hashlib
import io
import json
import math
import os
import platform
import statistics
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCHEMA = 2
VIEWS = {'quick': 1024, 'fit': 2048, 'native': None}


def summarize(values):
    ordered = sorted(values)
    return {'p50_ms': round(statistics.median(ordered), 2),
            'p95_ms': round(ordered[math.ceil(len(ordered) * .95) - 1], 2)}


def compare_reports(current, baseline):
    """Refuse incomparable runs; tolerate both proportional and absolute noise."""
    for key in ('schema', 'environment', 'samples'):
        if current[key] != baseline[key]:
            raise ValueError(f'Baseline {key} differs; record a comparable baseline')
    old = {row['scenario']: row for row in baseline['results']}
    if set(old) != {row['scenario'] for row in current['results']}:
        raise ValueError('Baseline scenarios differ')
    failures = []
    for row in current['results']:
        previous = old[row['scenario']]
        for key in ('sha256', 'requested_size', 'source_dimensions'):
            if row[key] != previous[key]:
                raise ValueError(f"Baseline input differs for {row['scenario']}")
        for metric, proportion, floor in (
            ('p50_ms', .25, 50), ('p95_ms', .25, 50), ('peak_rss_mib', .15, 64),
        ):
            limit = previous[metric] + max(previous[metric] * proportion, floor)
            if row[metric] > limit:
                failures.append(f"{row['scenario']} {metric}: {row[metric]} > {limit:.2f}")
    return failures


def environment(threads, machine_label):
    import cv2
    import numpy
    import PIL
    import rawpy
    import scipy

    cpu = platform.processor() or platform.machine()
    if platform.system() == 'Darwin':
        detected = subprocess.run(['sysctl', '-n', 'machdep.cpu.brand_string'], capture_output=True, text=True, check=False)
        cpu = detected.stdout.strip() or cpu
    return {'machine_label': machine_label, 'system': platform.system(),
            'machine': platform.machine(), 'cpu_count': os.cpu_count(),
            'python': platform.python_version(), 'threads': threads,
            'rawpy': rawpy.__version__, 'libraw': list(rawpy.libraw_version),
            'numpy': numpy.__version__, 'pillow': PIL.__version__,
            'scipy': scipy.__version__, 'opencv': cv2.__version__,
            'cpu_model': cpu, 'accelerator_mode': 'cpu', 'preview_worker_limit': 2}


def provenance(argv):
    """Record build identity and a reproducible command without local paths."""
    revision = subprocess.run(['git', 'rev-parse', 'HEAD'], cwd=ROOT, capture_output=True, text=True, check=False)
    dirty = subprocess.run(['git', 'status', '--porcelain'], cwd=ROOT, capture_output=True, text=True, check=False)
    command = ['python', 'scripts/benchmark_raw_previews.py']
    redact = None
    for argument in argv:
        flag, separator, _ = argument.partition('=')
        if separator and flag in ('--manifest', '--output', '--baseline'):
            command.append(flag + '=<local ' + flag[2:] + '>')
        elif redact:
            command.append('<local ' + redact + '>')
            redact = None
        else:
            command.append(argument)
            if argument in ('--manifest', '--output', '--baseline'):
                redact = argument[2:]
    return {'source_revision': revision.stdout.strip() or None,
            'working_tree_dirty': bool(dirty.stdout.strip()) or dirty.returncode != 0,
            'command': command, 'command_paths_redacted': True}


def corpus_entries(manifest):
    import rawpy

    entries = json.loads(manifest.read_text())
    if not isinstance(entries, list) or not entries:
        raise ValueError('Manifest must be a nonempty list of {name, path} entries')
    names = set()
    for entry in entries:
        name = entry['name']
        if not isinstance(name, str) or not name or name in names:
            raise ValueError('Each corpus entry needs a unique, nonempty name')
        names.add(name)
        path = (manifest.parent / Path(entry['path']).expanduser()).resolve()
        with path.open('rb') as stream:
            digest = hashlib.file_digest(stream, 'sha256').hexdigest()
        with rawpy.imread(str(path)) as raw:
            width, height = raw.sizes.width, raw.sizes.height
        yield {'name': name, 'path': str(path), 'sha256': digest,
               'source_dimensions': [width, height]}


def run_worker(spec):
    # Thread caps are set in the child environment before numeric imports.
    sys.path.insert(0, str(ROOT / 'vireo'))
    import cv2
    import psutil
    from PIL import Image

    cv2.setNumThreads(spec['threads'])
    os.environ['VIREO_DISABLE_STARTUP_BACKFILL_TIMERS'] = '1'
    os.environ['VIREO_DISABLE_BROWSER_AUTH'] = '1'
    import config as cfg
    from app import create_app
    from db import Database

    with tempfile.TemporaryDirectory(prefix='vireo-raw-benchmark-') as directory:
        root = Path(directory)
        cfg.CONFIG_PATH = str(root / 'config.json')
        cfg.save({'setup_complete': True, 'preview_quality': 90})
        db_path = str(root / 'catalog.db')
        path = Path(spec['path'])
        stat = path.stat()
        with Database(db_path) as db:
            db.set_active_workspace(db.ensure_default_workspace())
            folder = db.add_folder(str(path.parent))
            photo = db.add_photo(
                folder_id=folder, filename=path.name, extension=path.suffix.lower(),
                file_size=stat.st_size, file_mtime=stat.st_mtime,
                width=spec['source_dimensions'][0], height=spec['source_dimensions'][1],
            )
        app = create_app(db_path, thumb_cache_dir=str(root / 'thumbnails'))
        app.config['TESTING'] = True
        app.config['EDIT_PREVIEW_THREADS'] = spec['threads']
        app.config['EDIT_PREVIEW_CLEAR_SOURCE_CACHE'] = spec['cache'] == 'cold'
        app.config['COMPUTATION_CACHE_DIR'] = str(root / 'computation-cache')
        client = app.test_client()

        def render(index):
            recipe = {'adjustments': {'exposure': .2 + .1 * (index % 5), 'shadows': 15}}
            start = time.perf_counter()
            response = client.get(f'/photos/{photo}/edit-preview', query_string={
                'size': spec['requested_size'], 'recipe': json.dumps(recipe), 'apply_crop': '1',
            })
            elapsed = (time.perf_counter() - start) * 1000
            if response.status_code != 200:
                raise RuntimeError(f'Preview returned {response.status_code}')
            if response.headers.get('X-Vireo-Preview-Source') != 'linear':
                raise RuntimeError('RAW decode failed or fell back to an 8-bit source')
            with Image.open(io.BytesIO(response.data)) as image:
                dimensions = list(image.size)
                if max(dimensions) != min(spec['requested_size'], max(spec['source_dimensions'])):
                    raise RuntimeError(f'Unexpected preview dimensions: {dimensions}')
            response.close()
            return elapsed, dimensions

        process = psutil.Process()
        def tree_rss():
            total = process.memory_info().rss
            for child in process.children(recursive=True):
                try:
                    total += child.memory_info().rss
                except psutil.NoSuchProcess:
                    continue
            return total

        peak = [tree_rss()]
        stop = threading.Event()

        def sample_memory():
            while not stop.wait(.01):
                peak[0] = max(peak[0], tree_rss())

        try:
            if spec['cache'] == 'warm':
                render(0)
            gc.collect()
            peak[0] = tree_rss()
            sampler = threading.Thread(target=sample_memory, daemon=True)
            sampler.start()
            values = []
            try:
                for index in range(spec['samples']):
                    elapsed, dimensions = render(index + 1)
                    values.append(elapsed)
                    peak[0] = max(peak[0], tree_rss())
            finally:
                stop.set()
                sampler.join()
            return {'scenario': spec['scenario'], 'sha256': spec['sha256'],
                    'source_dimensions': spec['source_dimensions'],
                    'requested_size': spec['requested_size'], 'output_dimensions': dimensions,
                    'samples_ms': [round(value, 2) for value in values], **summarize(values),
                    'peak_rss_mib': round(peak[0] / 1024**2, 2)}
        finally:
            app._cleanup_app_resources()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--manifest', type=Path)
    parser.add_argument('--output', type=Path)
    parser.add_argument('--baseline', type=Path)
    parser.add_argument('--machine-label', help='Stable label for this benchmark machine')
    parser.add_argument('--samples', type=int, default=5)
    parser.add_argument('--threads', type=int, default=4)
    parser.add_argument('--views', nargs='+', choices=list(VIEWS), default=list(VIEWS))
    parser.add_argument('--worker', help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.worker:
        print(json.dumps(run_worker(json.loads(args.worker))))
        return
    if not args.manifest or not args.output or not args.machine_label:
        parser.error('--manifest, --output and --machine-label are required')
    if args.samples < 1 or not 1 <= args.threads <= 4:
        parser.error('--samples must be positive; --threads must be between 1 and 4')
    report = {'schema': SCHEMA, 'environment': environment(args.threads, args.machine_label),
              'samples': args.samples, 'provenance': provenance(sys.argv[1:]), 'results': []}
    child_env = dict(os.environ, OMP_NUM_THREADS=str(args.threads),
                     OPENBLAS_NUM_THREADS=str(args.threads), MKL_NUM_THREADS=str(args.threads))
    for entry in corpus_entries(args.manifest):
        for view in dict.fromkeys(args.views):
            for cache in ('cold', 'warm'):
                spec = {**entry, 'scenario': f"{entry['name']}/{view}/{cache}", 'cache': cache,
                        'requested_size': VIEWS[view] or max(entry['source_dimensions']),
                        'samples': args.samples, 'threads': args.threads}
                print(f"Measuring {spec['scenario']}…", file=sys.stderr, flush=True)
                result = subprocess.run(
                    [sys.executable, str(Path(__file__).resolve()), '--worker', json.dumps(spec)],
                    env=child_env, capture_output=True, text=True, timeout=1800,
                )
                if result.returncode:
                    raise RuntimeError(result.stderr or result.stdout)
                report['results'].append(json.loads(result.stdout))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + '\n')
    if args.baseline:
        failures = compare_reports(report, json.loads(args.baseline.read_text()))
        if failures:
            raise SystemExit('RAW preview regressions:\n' + '\n'.join(failures))
    print(f'Report written to {args.output}')


if __name__ == '__main__':
    main()
