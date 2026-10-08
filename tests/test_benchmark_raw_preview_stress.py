"""Stress-report correctness and a real LibRaw/worker smoke scenario."""

import json
import subprocess
import sys
from pathlib import Path

import pytest

from scripts.benchmark_raw_preview_stress import metric, validate_outcomes


def test_empty_cancellation_measurements_are_not_zero_latency():
    assert metric([]) == {'samples_ms': [], 'p50_ms': None, 'p95_ms': None}
    assert metric([3, 1, 2])['p95_ms'] == 3


@pytest.mark.parametrize('status', [400, 500, 503, 504])
def test_stress_rejects_failed_or_overloaded_requests(status):
    with pytest.raises(RuntimeError, match='returned'):
        validate_outcomes([{'id': 1, 'status': status}], [])


def test_stress_requires_latest_request_to_complete():
    old = {'id': 1, 'status': 409}
    latest = {'id': 2, 'status': 200}
    validate_outcomes([old, latest], [latest])
    with pytest.raises(RuntimeError, match='Latest'):
        validate_outcomes([old], [old])


def test_stress_runs_real_raw_workers_and_omits_private_paths(tmp_path):
    from vireo.tests.test_raw_precision import write_dng

    for name in ('first.dng', 'second.dng'):
        write_dng(tmp_path / name)
    manifest = tmp_path / 'corpus.json'
    manifest.write_text(json.dumps([{'name': name, 'path': name + '.dng'} for name in ('first', 'second')]))
    output = tmp_path / 'report.json'
    root = Path(__file__).resolve().parents[1]
    result = subprocess.run([
        sys.executable, str(root / 'scripts/benchmark_raw_preview_stress.py'),
        '--manifest', str(manifest), '--output', str(output), '--machine-label', 'smoke',
        '--samples', '1', '--edits', '2', '--interval-ms', '10', '--threads', '1',
    ], capture_output=True, text=True, timeout=120)
    assert result.returncode == 0, result.stderr
    data = json.loads(output.read_text())
    assert data['schema'] == 1
    assert data['environment']['threads'] == 1
    assert data['provenance']['command'][1] == 'scripts/benchmark_raw_preview_stress.py'
    assert len(data['results']) == 4
    for row in data['results']:
        assert row['completed'] + row['superseded'] == row['requests']
        assert row['latest_preview']['p50_ms'] > 0
        assert row['peak_rss_mib'] > 0
    tabs = next(row for row in data['results'] if row['scenario'] == 'multiple-tabs')
    assert len(tabs['latest_preview']['samples_ms']) == 3
    assert len(tabs['native_refinement']['samples_ms']) == 3
    assert sum(row['superseded'] for row in data['results']) > 0
    navigation = next(row for row in data['results'] if row['scenario'] == 'photo-navigation')
    assert len(navigation['navigation_cancel']['samples_ms']) == 2
    assert str(tmp_path) not in output.read_text()
