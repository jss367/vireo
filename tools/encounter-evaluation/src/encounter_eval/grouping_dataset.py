"""Import browser grouping reviews and replay their internal boundary constraints."""
from __future__ import annotations

import argparse
import gzip
import json
import sqlite3
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

from .algorithms import run_algorithm
from .common import code_identity, configure_repo, digest, encode, write_json
from .library import FeatureReader, Taxonomy, inference_features, open_library, read_bundle
from .review import same_photo


def _connect(path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    if tables and not {'imports', 'snapshots', 'cases', 'reviews'} <= tables:
        conn.close()
        raise ValueError('Destination is not a grouping review dataset')
    conn.execute('PRAGMA foreign_keys=ON')
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS imports(id TEXT PRIMARY KEY, data TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS snapshots(id TEXT PRIMARY KEY, metadata TEXT NOT NULL, bundle BLOB NOT NULL);
        CREATE TABLE IF NOT EXISTS cases(id TEXT PRIMARY KEY, snapshot TEXT NOT NULL REFERENCES snapshots(id), data TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS reviews(case_id TEXT REFERENCES cases(id), import_id TEXT REFERENCES imports(id),
            updated_at TEXT NOT NULL, data TEXT NOT NULL, PRIMARY KEY(case_id, import_id));
    ''')
    return conn


def _membership(groups):
    result = {}
    for index, group in enumerate(groups):
        for pid in group:
            if pid in result:
                raise ValueError('Overlapping expected groups')
            result[pid] = index
    return result


def boundary_errors(ids, expected, actual):
    """Only internal reviewed boundaries count; the two outside edges are unknown."""
    truth, prediction = _membership(expected), _membership(actual)
    if set(truth) != set(ids) or not set(ids) <= prediction.keys():
        raise ValueError('Grouping does not cover the reviewed photos')
    errors = Counter()
    for left, right in zip(ids, ids[1:], strict=False):
        cut = truth[left] != truth[right]
        predicted_cut = prediction[left] != prediction[right]
        errors['reviewed_splits' if cut else 'reviewed_joins'] += 1
        if cut != predicted_cut:
            errors['incorrect_merges' if cut else 'unnecessary_splits'] += 1
    return dict(errors)


def import_reviews(dataset, run, exported):
    run = Path(run).resolve()
    exported = json.loads(Path(exported).read_text())
    summary = json.loads((run / 'comparison-summary.json').read_text())
    if exported['comparison_created_at'] != summary['created_at']:
        raise ValueError('Review export belongs to another comparison')
    cases = {c['id']: c for c in json.loads((run / 'encounter-review.json').read_text())}
    decisions = exported['decisions']
    if not decisions or len({d['case_id'] for d in decisions}) != len(decisions):
        raise ValueError('Expected nonempty, unique review decisions')
    scopes = {}
    for scope in run.glob('scope-*'):
        manifest = json.loads((scope / 'manifest.json').read_text())
        baseline = json.loads((scope / 'baseline-manifest.json').read_text())
        for entry in manifest['sessions']:
            scopes[(manifest['workspace'], entry['id'])] = (scope, manifest, baseline, entry)
    import_id = digest(exported)
    conn = _connect(dataset)
    try:
        with conn:
            if conn.execute('SELECT 1 FROM imports WHERE id=?', (import_id,)).fetchone():
                return {'imported': 0, 'already_imported': True}
            conn.execute('INSERT INTO imports VALUES(?,?)', (import_id, encode({
                'export': exported, 'summary': summary, 'source_run': str(run),
                'imported_at': datetime.now(UTC).isoformat()})))
            loaded = {}
            for decision in decisions:
                case = cases[decision['case_id']]
                if decision['decision'] not in {'good', 'split', 'join', 'unsure'}:
                    raise ValueError('Unknown review decision')
                reviewed_at = datetime.fromisoformat(decision['updated_at'])
                if reviewed_at.tzinfo is None:
                    raise ValueError('Review timestamp needs a timezone')
                reviewed_at = reviewed_at.astimezone(UTC).isoformat()
                key = (case['workspace'], case['session'])
                scope, manifest, baseline, entry = scopes[key]
                if case['partition'] != entry['partition'] or entry['partition'] not in {'train', 'development'}:
                    raise ValueError('Only matching training/development partitions may be imported')
                if key not in loaded:
                    bundle = read_bundle(scope, entry)
                    old_file = baseline['files'][str(min(p['id'] for p in bundle['photos']))]
                    with gzip.open(scope / 'baseline-inputs' / old_file['path'], 'rt') as handle:
                        old = json.load(handle)
                    if digest(old) != old_file['digest'] or [p['id'] for p in old] != [p['id'] for p in bundle['photos']]:
                        raise ValueError('Baseline evidence changed or photo order differs')
                    payload = {'captured': bundle, 'before': old}
                    metadata = {'manifest': manifest, 'entry': entry, 'baseline_revision': baseline['revision'],
                                'baseline_source_sha256': baseline['source_sha256'], 'payload_digest': digest(payload)}
                    snapshot_id = digest(metadata)
                    conn.execute('INSERT OR IGNORE INTO snapshots VALUES(?,?,?)',
                                 (snapshot_id, encode(metadata), gzip.compress(encode(payload).encode(), mtime=0)))
                    loaded[key] = snapshot_id, bundle
                snapshot_id, bundle = loaded[key]
                ordered = [p['id'] for p in bundle['photos']]
                ids = case['ids']
                start = ordered.index(ids[0])
                if ordered[start:start + len(ids)] != ids or len(set(ids)) != len(ids):
                    raise ValueError('Review must describe a contiguous sequence in its captured session')
                expected = [g['ids'] for g in case['after']]
                if [pid for group in expected for pid in group] != ids:
                    raise ValueError('Expected groups must partition the reviewed sequence in order')
                case_id = digest([summary['created_at'], case['id']])
                case_data = encode({'review_case': case, 'expected_groups': expected})
                prior = conn.execute('SELECT snapshot,data FROM cases WHERE id=?', (case_id,)).fetchone()
                if prior and (prior['snapshot'] != snapshot_id or prior['data'] != case_data):
                    raise ValueError('A previously imported case changed')
                conn.execute('INSERT OR IGNORE INTO cases VALUES(?,?,?)', (case_id, snapshot_id, case_data))
                prior = conn.execute('SELECT data FROM reviews WHERE case_id=? AND updated_at=?',
                                     (case_id, reviewed_at)).fetchone()
                if prior and prior['data'] != encode(decision):
                    raise ValueError('Conflicting reviews have the same timestamp')
                conn.execute('INSERT INTO reviews VALUES(?,?,?,?)', (case_id, import_id, reviewed_at, encode(decision)))
        return {'imported': len(decisions), 'decisions': dict(Counter(d['decision'] for d in decisions))}
    finally:
        conn.close()


def _live_features(conn, metadata, bundle):
    from pipeline import load_photo_features

    manifest = metadata['manifest']
    workspace = manifest['workspace']
    current = {str(r['id']): dict(r) for r in conn.execute('''SELECT p.id,p.filename,p.file_hash,f.path AS folder
        FROM photos p JOIN folders f ON f.id=p.folder_id
        JOIN photo_workspace_visibility wf ON wf.photo_id=p.id WHERE wf.workspace_id=?''', (workspace,))}
    for pid, identity in bundle['presentation'].items():
        if pid not in current or not same_photo(identity, current[pid]):
            raise ValueError(f'Photo {pid} no longer matches the captured identity/workspace')
    ids = [p['id'] for p in bundle['photos']]
    config = manifest['config']
    features = load_photo_features(FeatureReader(conn, workspace), config=config, effective_config=config, photo_ids=ids)
    by_id = {p['id']: p for p in features}
    if set(by_id) != set(ids):
        raise ValueError('Live feature loading changed the session photo inventory')
    taxonomy = Taxonomy(conn)
    return [inference_features(by_id[pid], taxonomy) for pid in ids]


def check(dataset, *, features='after', library=None, params=None):
    repo = configure_repo()
    if features not in {'before', 'after', 'live'} or (features == 'live' and library is None):
        raise ValueError('Choose before/after features, or live with an explicit library path')
    source_code = code_identity(repo)
    conn = sqlite3.connect(Path(dataset).resolve().as_uri() + '?mode=ro', uri=True)
    conn.row_factory = sqlite3.Row
    live = open_library(library) if features == 'live' else None
    counts, results, cached = Counter(), [], {}
    try:
        rows = conn.execute('''SELECT c.*,r.data AS review FROM cases c JOIN reviews r ON r.case_id=c.id
            WHERE r.updated_at=(SELECT MAX(updated_at) FROM reviews WHERE case_id=c.id)
            GROUP BY c.id ORDER BY c.snapshot,c.id''').fetchall()
        for row in rows:
            decision = json.loads(row['review'])
            counts['reviewed_cases'] += 1
            if decision['decision'] != 'good':
                counts['needs_explicit_boundary_review'] += 1
                continue
            if row['snapshot'] not in cached:
                stored = conn.execute('SELECT * FROM snapshots WHERE id=?', (row['snapshot'],)).fetchone()
                metadata = json.loads(stored['metadata'])
                if digest(metadata) != row['snapshot'] or metadata['entry']['partition'] not in {'train', 'development'}:
                    raise ValueError('Snapshot identity or partition changed')
                payload = json.loads(gzip.decompress(stored['bundle']))
                if digest(payload) != metadata['payload_digest']:
                    raise ValueError('Snapshot evidence changed')
                photos = (payload['before'] if features == 'before' else payload['captured']['photos'])
                if live is not None:
                    photos = _live_features(live, metadata, payload['captured'])
                groups = run_algorithm('production', photos, params=params,
                                       grouping_config=metadata['manifest']['grouping_config'])
                cached[row['snapshot']] = [list(g.photo_ids) for g in groups]
            case = json.loads(row['data'])
            errors = boundary_errors(case['review_case']['ids'], case['expected_groups'], cached[row['snapshot']])
            passed = not (errors.get('incorrect_merges', 0) or errors.get('unnecessary_splits', 0))
            counts.update(errors)
            counts['scored_cases'] += 1
            counts['passed_cases'] += passed
            results.append({'case_id': case['review_case']['id'], 'passed': passed, **errors})
    finally:
        conn.close()
        if live is not None:
            live.close()
    return {'features': features, 'code': source_code, 'params': params or {}, 'counts': dict(counts), 'cases': results,
            'scope': 'Internal reviewed boundaries only; species completeness and outside edges are not inferred.',
            'evidence': 'Live cached predictions (may have changed)' if live is not None else 'Frozen captured features'}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    imp = sub.add_parser('import')
    imp.add_argument('--dataset', type=Path, required=True)
    imp.add_argument('--run', type=Path, required=True)
    imp.add_argument('--decisions', type=Path, required=True)
    evaluate = sub.add_parser('check')
    evaluate.add_argument('--dataset', type=Path, required=True)
    evaluate.add_argument('--features', choices=['before', 'after', 'live'], default='after')
    evaluate.add_argument('--db', type=Path)
    evaluate.add_argument('--params', type=Path, help='JSON production grouping parameter overrides')
    evaluate.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.command == 'import':
        result = import_reviews(args.dataset, args.run, args.decisions)
    else:
        result = check(args.dataset, features=args.features, library=args.db,
                       params=json.loads(args.params.read_text()) if args.params else None)
        write_json(args.output, result)
    print(json.dumps({k: v for k, v in result.items() if k not in {'cases', 'code'}}, indent=2))
    if args.command == 'check' and (not result['counts'].get('scored_cases') or
                                   result['counts']['passed_cases'] != result['counts']['scored_cases']):
        raise SystemExit(1)


if __name__ == '__main__':
    main()
