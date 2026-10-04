import gzip
import json
import sqlite3

import pytest

from encounter_eval.algorithms import run_algorithm
from encounter_eval.common import Group, digest, encode
from encounter_eval.grouping_dataset import boundary_errors, check, import_reviews
from encounter_eval.library import prepare, read_bundle


@pytest.fixture
def comparison(library, tmp_path):
    root = tmp_path / 'comparison'
    scope = root / 'scope-0-workspace-1'
    manifest = prepare(library, scope)
    entry = manifest['sessions'][0]
    bundle = read_bundle(scope, entry)
    baseline = scope / 'baseline-inputs'
    baseline.mkdir()
    with gzip.open(baseline / 'photos.gz', 'wt') as handle:
        handle.write(encode(bundle['photos']))
    (scope / 'baseline-manifest.json').write_text(encode({
        'revision': 'test-baseline', 'source_sha256': 'test-source',
        'files': {'1': {'path': 'photos.gz', 'digest': digest(bundle['photos'])}}}))
    (root / 'comparison-summary.json').write_text(encode({'created_at': 'fixture-comparison'}))
    groups = run_algorithm('production', bundle['photos'], grouping_config=manifest['grouping_config'])
    assert len(groups) > 1
    case = {'id': 'case-1', 'workspace': 1, 'session': entry['id'], 'partition': entry['partition'],
            'ids': list(range(1, 13)), 'after': [{'ids': list(g.photo_ids)} for g in groups]}
    (root / 'encounter-review.json').write_text(encode([case]))
    export = tmp_path / 'decisions.json'
    export.write_text(encode({'comparison_created_at': 'fixture-comparison', 'decisions': [
        {'case_id': 'case-1', 'decision': 'good', 'notes': 'Human review', 'updated_at': '2026-10-04T04:00:00Z'}]}))
    return root, export, tmp_path / 'dataset.sqlite'


def test_import_replay_and_idempotence(comparison, library, monkeypatch):
    root, export, dataset = comparison
    assert import_reviews(dataset, root, export)['imported'] == 1
    assert import_reviews(dataset, root, export)['already_imported']
    assert check(dataset)['counts']['passed_cases'] == 1
    original = library.read_bytes()
    assert check(dataset, features='live', library=library)['counts']['passed_cases'] == 1
    assert library.read_bytes() == original
    from encounter_eval import grouping_dataset
    monkeypatch.setattr(grouping_dataset, 'run_algorithm', lambda *a, **k: [Group(tuple(range(1, 13)), None, 'fixture')])
    result = check(dataset)
    assert result['counts']['passed_cases'] == 0
    assert result['counts']['incorrect_merges'] > 0


def test_internal_constraints_do_not_assert_outside_edges():
    assert boundary_errors([2, 3], [[2, 3]], [[1, 2, 3, 4]]) == {'reviewed_joins': 1}
    assert boundary_errors([2, 3], [[2, 3]], [[1, 2], [3, 4]])['unnecessary_splits'] == 1
    assert boundary_errors([2, 3], [[2], [3]], [[1, 2, 3, 4]])['incorrect_merges'] == 1


def test_review_revisions_keep_history_and_old_import_cannot_undo_new(comparison):
    root, export, dataset = comparison
    original = json.loads(export.read_text())
    newer = json.loads(export.read_text())
    newer['decisions'][0].update(decision='split', updated_at='2026-10-05T04:00:00Z')
    export.write_text(encode(newer))
    import_reviews(dataset, root, export)
    export.write_text(encode(original))
    import_reviews(dataset, root, export)
    assert check(dataset)['counts']['needs_explicit_boundary_review'] == 1
    with sqlite3.connect(dataset) as conn:
        assert conn.execute('SELECT COUNT(*) FROM reviews').fetchone()[0] == 2


@pytest.mark.parametrize('error', ['comparison', 'heldout', 'overlap', 'corruption'])
def test_rejects_mismatched_or_corrupted_inputs(comparison, error):
    root, export, dataset = comparison
    if error == 'comparison':
        data = json.loads(export.read_text())
        data['comparison_created_at'] = 'wrong'
        export.write_text(encode(data))
    elif error in {'heldout', 'overlap'}:
        file = root / 'encounter-review.json'
        cases = json.loads(file.read_text())
        if error == 'heldout':
            cases[0]['partition'] = 'test'
        else:
            cases[0]['after'].append({'ids': [1]})
        file.write_text(encode(cases))
    else:
        with gzip.open(root / 'scope-0-workspace-1/baseline-inputs/photos.gz', 'wt') as handle:
            handle.write('[]')
    with pytest.raises(ValueError):
        import_reviews(dataset, root, export)
    if dataset.exists():
        with sqlite3.connect(dataset) as conn:
            assert conn.execute('SELECT COUNT(*) FROM reviews').fetchone()[0] == 0


def test_live_replay_rejects_replaced_photo(comparison, library):
    root, export, dataset = comparison
    import_reviews(dataset, root, export)
    with sqlite3.connect(library) as conn:
        conn.execute("UPDATE photos SET filename='different.jpg' WHERE id=1")
    with pytest.raises(ValueError, match='identity'):
        check(dataset, features='live', library=library)


def test_frozen_replay_survives_source_removal(comparison):
    import shutil

    root, export, dataset = comparison
    import_reviews(dataset, root, export)
    shutil.rmtree(root)
    export.unlink()
    assert check(dataset)['counts']['passed_cases'] == 1


def test_import_never_adds_dataset_tables_to_the_photo_library(comparison, library):
    root, export, _ = comparison
    original = library.read_bytes()
    with pytest.raises(ValueError, match='Destination'):
        import_reviews(library, root, export)
    assert library.read_bytes() == original
