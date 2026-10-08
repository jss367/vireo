#!/usr/bin/env python3
"""Stress real RAW preview handlers with edits, navigation and independent tabs.

Runs in temporary catalogs, with production worker limits and per-thread HTTP
clients. No browser/network timing is included. Private worker-start markers and
a parent-side stop observer distinguish cancellation acknowledgement from reap.
"""

import argparse
import io
import json
import os
import subprocess
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

# Support both direct execution and test imports.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts.benchmark_raw_previews import ROOT, corpus_entries, environment, provenance, summarize

SCHEMA = 1


def observed_render(payload, output_path):
    """Record which child started a request, then run the unchanged renderer."""
    from web.media import render_edit_preview_job

    marker = Path(payload['vireo_dir']) / 'starts' / f"{payload['recipe']['adjustments']['exposure']:.4f}.json"
    temporary = marker.with_suffix('.tmp')
    temporary.write_text(json.dumps({'pid': os.getpid()}))
    temporary.replace(marker)
    return render_edit_preview_job(payload, output_path)


def metric(values):
    """Keep empty observations explicit rather than reporting invented zeros."""
    return {'samples_ms': [round(value, 2) for value in values],
            **(summarize(values) if values else {'p50_ms': None, 'p95_ms': None})}


def validate_outcomes(requests, latest):
    """Fail on overload, timeout, stale-final results or other render errors."""
    for request in requests:
        if request['status'] not in (200, 409):
            raise RuntimeError(f"Request {request['id']} returned {request['status']}")
    for request in latest:
        if request['status'] != 200:
            raise RuntimeError(f"Latest request {request['id']} did not render")


def aggregate_results(results):
    """Pool timing samples by scenario and retain the largest observed RSS."""
    summary = []
    for scenario in dict.fromkeys(row['scenario'] for row in results):
        trials = [row for row in results if row['scenario'] == scenario]
        summary.append({
            'scenario': scenario, 'trials': len(trials),
            **{key: sum(row[key] for row in trials)
               for key in ('requests', 'completed', 'superseded', 'reaped_workers_observed')},
            **{key: metric([value for row in trials for value in row[key]['samples_ms']])
               for key in ('cancellation_ack', 'worker_reap', 'navigation_cancel', 'latest_preview', 'native_refinement')},
            'peak_rss_mib': max(row['peak_rss_mib'] for row in trials),
        })
    return summary


def run_trial(spec):
    """Run one cold-start scenario in a fresh server and child-process pool."""
    sys.path.insert(0, str(ROOT / 'vireo'))
    import psutil
    from PIL import Image

    os.environ['VIREO_DISABLE_STARTUP_BACKFILL_TIMERS'] = '1'
    os.environ['VIREO_DISABLE_BROWSER_AUTH'] = '1'
    import config as cfg
    from app import create_app
    from db import Database

    with tempfile.TemporaryDirectory(prefix='vireo-preview-stress-') as directory:
        root = Path(directory)
        (root / 'starts').mkdir()
        cfg.CONFIG_PATH = str(root / 'config.json')
        cfg.save({'setup_complete': True, 'preview_quality': 90})
        db_path = str(root / 'catalog.db')
        photos = []
        with Database(db_path) as db:
            db.set_active_workspace(db.ensure_default_workspace())
            for source in spec['sources']:
                path = Path(source['path'])
                stat = path.stat()
                folder = db.add_folder(str(path.parent))
                photo = db.add_photo(folder_id=folder, filename=path.name, extension=path.suffix.lower(),
                                     file_size=stat.st_size, file_mtime=stat.st_mtime,
                                     width=source['source_dimensions'][0], height=source['source_dimensions'][1])
                photos.append({**source, 'id': photo})
        app = create_app(db_path, thumb_cache_dir=str(root / 'thumbnails'))
        app.config['TESTING'] = True
        app.config['EDIT_PREVIEW_THREADS'] = spec['threads']
        pool = app._preview_workers
        pool.threads = spec['threads']
        pool.handler = observed_render
        stopped = []
        original_stop = pool._stop

        def observe_stop(process, connection):
            pid = process.pid if process is not None else None
            original_stop(process, connection)
            if pid is not None:
                ended = time.perf_counter()
                # A completed request can race cancellation and leave a healthy
                # child for its successor. Attribute a reap only to the last
                # request that actually entered that child, never an older one.
                exposures = [float(marker.stem) for marker in (root / 'starts').glob('*.json')
                             if json.loads(marker.read_text())['pid'] == pid]
                stopped.append({'pid': pid, 'ended': ended, 'exposure': max(exposures, default=None)})

        pool._stop = observe_stop
        process = psutil.Process()
        peak = [0]
        memory_errors = []
        sampling_done = threading.Event()

        def sample_memory():
            try:
                while True:
                    rss = process.memory_info().rss
                    for child in process.children(recursive=True):
                        try:
                            rss += child.memory_info().rss
                        except psutil.NoSuchProcess:
                            continue
                    peak[0] = max(peak[0], rss)
                    if sampling_done.wait(.01):
                        break
            except Exception as error:
                memory_errors.append(str(error))

        sampler = threading.Thread(target=sample_memory, daemon=True)
        sampler.start()
        requests, futures, current, sequences = [], [], {}, {}
        cancellations = []
        epoch = time.perf_counter()

        def request_preview(row, photo):
            with app.test_client() as client:
                response = client.get(f"/photos/{photo['id']}/edit-preview", query_string={
                    'size': row['size'], 'recipe': json.dumps({'adjustments': {
                        'exposure': row['exposure'], 'shadows': 15}}), 'apply_crop': 1,
                    'preview_session': f"{row['tab'] + 1:032x}", 'preview_seq': row['sequence'],
                })
                row['ended'] = time.perf_counter()
                row['status'] = response.status_code
                if response.status_code == 200:
                    if response.headers.get('X-Vireo-Preview-Source') != 'linear':
                        raise RuntimeError('RAW preview fell back to an 8-bit source')
                    with Image.open(io.BytesIO(response.data)) as image:
                        expected = min(row['size'], max(photo['source_dimensions']))
                        if max(image.size) != expected:
                            raise RuntimeError(f'Unexpected output size: {image.size}')
                response.close()

        def dispatch(executor, tab, photo, size, *, exposure=None):
            now = time.perf_counter()
            previous = current.get(tab)
            if previous is not None and 'superseded' not in previous:
                previous['superseded'] = now
            sequences[tab] = sequences.get(tab, 0) + 1
            row = {'id': len(requests) + 1, 'tab': tab, 'photo': photo['name'], 'size': size,
                   'sequence': sequences[tab],
                   'exposure': exposure if exposure is not None else round(.1 + (len(requests) + 1) / 10000, 4),
                   'started': now}
            requests.append(row)
            current[tab] = row
            futures.append(executor.submit(request_preview, row, photo))
            return row

        def wait_started(row):
            marker = root / 'starts' / f"{row['exposure']:.4f}.json"
            deadline = time.perf_counter() + 20
            while not marker.exists():
                if time.perf_counter() > deadline:
                    raise RuntimeError('Initial native render did not start')
                time.sleep(.005)

        try:
            # A maximum of three tabs with one superseding request per input
            # round stays within the production eight-request pending bound.
            with ThreadPoolExecutor(max_workers=12) as executor:
                tabs = 3 if spec['kind'] == 'multiple-tabs' else 1
                for tab in range(tabs):
                    photo = photos[tab % len(photos)]
                    row = dispatch(executor, tab, photo, max(photo['source_dimensions']))
                    if tab < 2:
                        wait_started(row)
                for edit in range(spec['edits']):
                    time.sleep(spec['interval_ms'] / 1000)
                    for tab in range(tabs):
                        photo = photos[(edit + 1 if spec['kind'] == 'photo-navigation' else tab) % len(photos)]
                        if spec['kind'] == 'photo-navigation':
                            started = time.perf_counter()
                            current[tab]['superseded'] = started
                            sequences[tab] += 1
                            with app.test_client() as client:
                                response = client.post('/api/edit-preview/cancel', json={
                                    'session': f'{tab + 1:032x}', 'sequence': sequences[tab],
                                })
                                if response.status_code != 204:
                                    raise RuntimeError('Navigation cancellation failed')
                                response.close()
                            cancellations.append((time.perf_counter() - started) * 1000)
                        dispatch(executor, tab, photo, 1024)
                latest = list(current.values())
                for future in futures:
                    future.result(timeout=60)
                validate_outcomes(requests, latest)
                # Concurrent native refinements exercise peak memory after the
                # quick previews settle, including a third tab waiting its turn.
                refinements = []
                if spec['kind'] == 'multiple-tabs':
                    for tab in range(tabs):
                        photo = photos[tab % len(photos)]
                        refinements.append(dispatch(executor, tab, photo, max(photo['source_dimensions']),
                                                    exposure=latest[tab]['exposure']))
                    for future in futures:
                        future.result(timeout=60)
                    validate_outcomes(requests, refinements)
            acknowledged, reaped = [], []
            for row in requests:
                if row['status'] != 409:
                    continue
                acknowledged.append(max(0, row['ended'] - row['superseded']) * 1000)
                marker = root / 'starts' / f"{row['exposure']:.4f}.json"
                if marker.exists():
                    pid = json.loads(marker.read_text())['pid']
                    stop = next((item for item in stopped if item['pid'] == pid and
                                 item['exposure'] == row['exposure'] and item['ended'] >= row['superseded']), None)
                    if stop:
                        reaped.append((stop['ended'] - row['superseded']) * 1000)
            sampling_done.set()
            sampler.join()
            if memory_errors:
                raise RuntimeError('Memory sampling failed: ' + '; '.join(memory_errors))
            return {'scenario': spec['scenario'], 'requests': len(requests),
                    'completed': sum(row['status'] == 200 for row in requests),
                    'superseded': len(acknowledged), 'reaped_workers_observed': len(reaped),
                    'cancellation_ack': metric(acknowledged), 'worker_reap': metric(reaped),
                    'navigation_cancel': metric(cancellations),
                    'latest_preview': metric([(row['ended'] - row['started']) * 1000 for row in latest]),
                    'native_refinement': metric([(row['ended'] - row['started']) * 1000 for row in refinements]),
                    'peak_rss_mib': round(peak[0] / 1024**2, 2),
                    'elapsed_ms': round((time.perf_counter() - epoch) * 1000, 2)}
        finally:
            sampling_done.set()
            sampler.join()
            app._cleanup_app_resources()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--manifest', type=Path)
    parser.add_argument('--output', type=Path)
    parser.add_argument('--machine-label')
    parser.add_argument('--samples', type=int, default=3)
    parser.add_argument('--threads', type=int, default=2)
    parser.add_argument('--edits', type=int, default=6)
    parser.add_argument('--interval-ms', type=int, default=60)
    parser.add_argument('--worker', help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.worker:
        print(json.dumps(run_trial(json.loads(args.worker))))
        return
    if not args.manifest or not args.output or not args.machine_label:
        parser.error('--manifest, --output and --machine-label are required')
    if not 1 <= args.samples <= 20 or not 1 <= args.threads <= 4 or not 1 <= args.edits <= 100 or not 1 <= args.interval_ms <= 1000:
        parser.error('Use 1–20 samples, 1–4 threads, 1–100 edits and a 1–1000 ms interval')
    env = environment(args.threads, args.machine_label)
    sources = list(corpus_entries(args.manifest))
    if len(sources) < 2:
        parser.error('At least two RAW files are needed to measure photo navigation')
    scenarios = [{'scenario': source['name'] + '/rapid-edits', 'kind': 'rapid-edits', 'sources': [source]}
                 for source in sources]
    scenarios += [{'scenario': kind, 'kind': kind, 'sources': sources}
                  for kind in ('photo-navigation', 'multiple-tabs')]
    report = {'schema': SCHEMA, 'environment': env,
              'provenance': provenance(sys.argv[1:], script=Path(__file__).name),
              'protocol': {'samples': args.samples, 'edits': args.edits, 'interval_ms': args.interval_ms,
                           'tabs': 3, 'quick_size': 1024, 'memory_sample_ms': 10},
              'sources': [{key: value for key, value in source.items() if key != 'path'} for source in sources],
              'results': []}
    child_env = dict(os.environ, OMP_NUM_THREADS=str(args.threads), OPENBLAS_NUM_THREADS=str(args.threads),
                     MKL_NUM_THREADS=str(args.threads))
    for scenario in scenarios:
        for sample in range(args.samples):
            print(f"Measuring {scenario['scenario']} ({sample + 1}/{args.samples})…", file=sys.stderr, flush=True)
            spec = {**scenario, 'threads': args.threads, 'edits': args.edits, 'interval_ms': args.interval_ms}
            result = subprocess.run([sys.executable, str(Path(__file__).resolve()), '--worker', json.dumps(spec)],
                                    capture_output=True, text=True, env=child_env, timeout=300)
            if result.returncode:
                raise RuntimeError(result.stderr or result.stdout)
            report['results'].append({**json.loads(result.stdout), 'sample': sample + 1})
    report['summary'] = aggregate_results(report['results'])
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + '\n')
    print(f'Report written to {args.output}')


if __name__ == '__main__':
    main()
