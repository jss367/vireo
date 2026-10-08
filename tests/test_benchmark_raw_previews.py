"""Comparison gates and a real LibRaw/HTTP benchmark smoke run."""

import copy
import json
import subprocess
import sys
from pathlib import Path

import pytest

from scripts.benchmark_raw_previews import SCHEMA, compare_reports, provenance, summarize


def report():
    return {'schema': SCHEMA, 'environment': {'machine_label': 'test'}, 'samples': 5,
            'results': [{'scenario': 'camera/native/warm', 'sha256': 'abc',
                         'requested_size': 6000, 'source_dimensions': [6000, 4000],
                         'p50_ms': 100, 'p95_ms': 200, 'peak_rss_mib': 400}]}


def test_latency_summary_uses_nearest_rank_tail():
    assert summarize([10, 1, 3, 2, 4]) == {'p50_ms': 3, 'p95_ms': 10}


def test_compare_flags_latency_and_memory_regressions_but_tolerates_noise():
    old = report()
    current = copy.deepcopy(old)
    current['results'][0].update(p50_ms=140, p95_ms=240, peak_rss_mib=450)
    assert compare_reports(current, old) == []
    current['results'][0].update(p50_ms=170, p95_ms=270, peak_rss_mib=500)
    assert len(compare_reports(current, old)) == 3


@pytest.mark.parametrize('change', ['schema', 'environment', 'samples', 'sha256', 'scenario', 'requested_size'])
def test_compare_rejects_incomparable_runs(change):
    old = report()
    current = copy.deepcopy(old)
    target = current if change in ('schema', 'environment', 'samples') else current['results'][0]
    target[change] = 'different'
    with pytest.raises(ValueError, match='Baseline'):
        compare_reports(current, old)


def test_benchmark_runs_real_raw_endpoint_in_isolated_process(tmp_path):
    from vireo.tests.test_raw_precision import write_dng

    source = tmp_path / 'small.dng'
    write_dng(source)
    manifest = tmp_path / 'corpus.json'
    manifest.write_text(json.dumps([{'name': 'synthetic-smoke', 'path': source.name}]))
    output = tmp_path / 'report.json'
    root = Path(__file__).resolve().parents[1]
    result = subprocess.run([
        sys.executable, str(root / 'scripts/benchmark_raw_previews.py'),
        '--manifest', str(manifest), '--output', str(output),
        '--machine-label', 'test', '--samples', '1', '--views', 'native',
    ], capture_output=True, text=True, timeout=90)
    assert result.returncode == 0, result.stderr
    data = json.loads(output.read_text())
    assert data['schema'] == SCHEMA == 2
    assert data['environment']['cpu_model']
    assert data['environment']['accelerator_mode'] == 'cpu'
    assert data['provenance']['source_revision']
    assert data['provenance']['command_paths_redacted'] is True
    assert len(data['results']) == 2
    for row in data['results']:
        assert row['output_dimensions'] == [512, 64]
        assert row['p50_ms'] > 0
        assert row['peak_rss_mib'] > 0
        assert len(row['sha256']) == 64
    assert compare_reports(data, data) == []
    assert str(tmp_path) not in output.read_text()  # reports omit local paths


def test_benchmark_refuses_an_eight_bit_preview_source(tmp_path):
    from PIL import Image

    source = tmp_path / 'fallback.jpg'
    Image.new('RGB', (512, 64)).save(source)
    spec = {'path': str(source), 'threads': 1, 'source_dimensions': [512, 64],
            'cache': 'cold', 'samples': 1, 'requested_size': 512,
            'scenario': 'fallback/native/cold', 'sha256': 'unused'}
    root = Path(__file__).resolve().parents[1]
    result = subprocess.run([
        sys.executable, str(root / 'scripts/benchmark_raw_previews.py'),
        '--worker', json.dumps(spec),
    ], capture_output=True, text=True, timeout=90)
    assert result.returncode != 0
    assert 'fell back to an 8-bit source' in result.stderr


def test_provenance_records_command_without_local_file_paths():
    record = provenance(['--manifest=/private/photos/corpus.json', '--output', '/private/results.json',
                         '--machine-label', 'benchmark-host', '--samples', '5'])
    assert '/private' not in json.dumps(record)
    assert record['command'][-4:] == ['--machine-label', 'benchmark-host', '--samples', '5']
    assert isinstance(record['working_tree_dirty'], bool)
