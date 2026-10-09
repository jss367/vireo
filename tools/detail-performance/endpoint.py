#!/usr/bin/env python3
"""Exercise experimental filters through the existing RAW preview benchmark.

The only substitution is the spawned render worker's callable. Temporary
catalogs, source caches, request handling, supervision and memory accounting
come from scripts/benchmark_raw_previews.py without production changes.
"""

import argparse
import copy
import functools
import hashlib
import json
import os
import platform
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / 'vireo'))

from benchmark import BACKENDS, backend  # noqa: E402

from scripts import benchmark_raw_previews as raw_benchmark  # noqa: E402


def experimental_render(payload, output_path, *, backend_name, threads, recipe):
    from web.media import render_edit_preview_job

    if recipe == 'detail':
        payload = copy.deepcopy(payload)
        for key in ('recipe', 'display_recipe'):
            payload[key]['adjustments'].update(sharpen=55, sharpen_radius=1.8, noise_reduction=45)
    with backend(backend_name, threads):
        return render_edit_preview_job(payload, output_path)


def worker(spec):
    import app
    from services.preview_workers import PreviewWorkers

    def make_workers(_handler, **kwargs):
        handler = functools.partial(experimental_render, backend_name=spec['backend'],
                                    threads=spec['threads'], recipe=spec['recipe'])
        return PreviewWorkers(handler, **kwargs)

    # This process exists only for one experiment. Restore the constructor
    # nevertheless, so run_worker also remains safe to call from a test.
    original = app.PreviewWorkers
    app.PreviewWorkers = make_workers
    try:
        return raw_benchmark.run_worker(spec)
    finally:
        app.PreviewWorkers = original


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--manifest', type=Path)
    parser.add_argument('--extension-dir', type=Path)
    parser.add_argument('--output', type=Path)
    parser.add_argument('--views', nargs='+', choices=raw_benchmark.VIEWS, default=['quick', 'fit', 'native'])
    parser.add_argument('--caches', nargs='+', choices=['cold', 'warm'], default=['warm'])
    parser.add_argument('--backends', nargs='+', choices=BACKENDS, default=['original', 'opencv', 'rust-opencv'])
    parser.add_argument('--recipes', nargs='+', choices=['basic', 'detail'], default=['detail'])
    parser.add_argument('--threads', type=int, choices=range(1, 5), default=4)
    parser.add_argument('--samples', type=int, default=5)
    parser.add_argument('--worker', help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.worker:
        print(json.dumps(worker(json.loads(args.worker))))
        return
    if not args.manifest or not args.output or args.samples < 1:
        parser.error('Provide --manifest, --output and a positive sample count')
    env = dict(os.environ, OMP_NUM_THREADS=str(args.threads), OPENBLAS_NUM_THREADS=str(args.threads),
               MKL_NUM_THREADS=str(args.threads))
    if args.extension_dir:
        env['PYTHONPATH'] = str(args.extension_dir.resolve()) + os.pathsep + env.get('PYTHONPATH', '')
    report = {'schema': 1, 'kind': 'experimental RAW preview HTTP endpoint',
              'base_benchmark_schema': raw_benchmark.SCHEMA,
              'environment': raw_benchmark.environment(args.threads, platform.machine()),
              'provenance': raw_benchmark.provenance(sys.argv[1:]),
              'source_hashes': {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest()
                                for path in [Path(__file__), Path(__file__).with_name('benchmark.py'),
                                             Path(__file__).parent / 'src/lib.rs',
                                             ROOT / 'scripts/benchmark_raw_previews.py', ROOT / 'vireo/detail.py',
                                             ROOT / 'vireo/detail_backend.py', ROOT / 'native/detail/src/lib.rs']},
              'samples': args.samples, 'results': []}
    for entry in raw_benchmark.corpus_entries(args.manifest):
        for view in args.views:
            for cache in args.caches:
                for recipe in args.recipes:
                    for name in args.backends:
                        scenario = f"{entry['name']}/{view}/{cache}/{recipe}/{name}"
                        spec = dict(entry, scenario=scenario, cache=cache, recipe=recipe, backend=name,
                                    requested_size=raw_benchmark.VIEWS[view] or max(entry['source_dimensions']),
                                    samples=args.samples, threads=args.threads)
                        child = subprocess.run([sys.executable, str(Path(__file__).resolve()),
                                                '--worker', json.dumps(spec)],
                                               env=env, capture_output=True, text=True, timeout=1800)
                        if child.returncode:
                            raise RuntimeError(child.stderr or child.stdout)
                        row = json.loads(child.stdout)
                        report['results'].append(row)
                        print(f"{scenario}: {row['p50_ms']} ms, {row['peak_rss_mib']} MiB", flush=True)
                        args.output.parent.mkdir(parents=True, exist_ok=True)
                        args.output.write_text(json.dumps(report, indent=2) + '\n')


if __name__ == '__main__':
    main()
