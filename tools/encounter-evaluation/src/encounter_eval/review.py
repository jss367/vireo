"""Durable, local human review of training/development disagreements."""
from __future__ import annotations

import json
import sqlite3
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

from .algorithms import DEFAULTS, run_algorithm
from .common import code_identity, configure_repo, digest
from .library import normalize, read_bundle

ALGORITHMS = ('production', 'sequence', 'independent')
KINDS = ('species', 'boundary', 'abstention', 'agreement', 'identity')


def connect(path):
    conn = sqlite3.connect(path, timeout=20)
    conn.row_factory = sqlite3.Row
    return conn


def same_photo(before, after):
    if before.get('file_hash'):
        return before['file_hash'] == after.get('file_hash')
    return (before['filename'], before['folder']) == (after['filename'], after['folder'])


def identity_conflict(photo, display):
    """Flag same-display-name source disagreements without merging identities."""
    for detection in photo.get('evidence', []):
        if detection['category'] != 'animal' or (detection['detector_confidence'] or 0) < DEFAULTS['detector_confidence']:
            continue
        winners = []
        for source in detection['sources']:
            ps = sorted(source['predictions'], key=lambda p: -p['score'])
            if ps and ps[0]['score'] >= DEFAULTS['confidence'] and ps[0]['score'] - (ps[1]['score'] if len(ps) > 1 else 0) >= DEFAULTS['margin']:
                winners.append(ps[0]['taxon'])
        if len(set(winners)) > 1 and len({normalize(display.get(k, k.removeprefix('name:'))) for k in winners}) == 1:
            return True
    return False


def classify(rosters, cuts, photo, display):
    resolved = {r for r in rosters if r is not None}
    if len(resolved) > 1:
        names = {tuple(sorted(normalize(display.get(k, k.removeprefix('name:'))) for k in r)) for r in resolved}
        return 'identity' if len(names) == 1 else 'species'
    if identity_conflict(photo, display):
        return 'identity'
    if resolved and None in rosters:
        return 'abstention'
    if len(set(cuts)) > 1:
        return 'boundary'
    if len(resolved) == 1 and None not in rosters:
        return 'agreement'
    return None


def build_queue(run, output, library, *, agreement_sample=200, seed=42):
    repo = configure_repo()
    run, output, library = Path(run).resolve(), Path(output).resolve(), Path(library).expanduser().resolve()
    if agreement_sample < 0:
        raise ValueError('Agreement sample must be nonnegative')
    manifest = json.loads((run / 'manifest.json').read_text())
    if manifest.get('source_library') != str(library):
        raise ValueError('Run source library is missing or differs; rebuild the run or use its source library')
    if output.exists():
        raise ValueError('Review database already exists; serve it to resume your reviews')
    output.parent.mkdir(parents=True, exist_ok=True)
    conn = connect(output)
    try:
        conn.executescript('''
            CREATE TABLE metadata(key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE photos(id INTEGER PRIMARY KEY, session TEXT, partition TEXT, data TEXT NOT NULL);
            CREATE TABLE items(photo_id INTEGER PRIMARY KEY, kind TEXT, position INTEGER, representative INTEGER);
            CREATE INDEX items_order ON items(position);
            CREATE TABLE species(key TEXT PRIMARY KEY, name TEXT NOT NULL, scientific TEXT NOT NULL);
            CREATE INDEX species_name ON species(name COLLATE NOCASE);
            CREATE INDEX species_scientific ON species(scientific COLLATE NOCASE);
            CREATE TABLE reviews(photo_id INTEGER PRIMARY KEY REFERENCES photos(id), revision INTEGER NOT NULL,
                status TEXT NOT NULL, taxa TEXT NOT NULL, notes TEXT NOT NULL, updated_at TEXT NOT NULL);
            CREATE TABLE review_history(photo_id INTEGER, revision INTEGER, answer TEXT NOT NULL);
        ''')
        # A static species catalog, never photo labels, supplies picker identities.
        with sqlite3.connect(library.as_uri() + '?mode=ro', uri=True) as source:
            catalog = source.execute("SELECT id,inat_id,name,common_name FROM taxa WHERE rank='species'")
            conn.executemany('INSERT OR IGNORE INTO species VALUES(?,?,?)',
                             ((f'inat:{tid}' if tid else f'taxon:{local}', common or sci, sci)
                              for local, tid, sci, common in catalog))
        conn.executemany('INSERT OR IGNORE INTO species VALUES(?,?,?)',
                         ((k, n, '') for k, n in manifest['taxonomy_display'].items()))
        candidates, agreements, stats = [], [], Counter()
        display = manifest['taxonomy_display']
        sessions = [e for e in manifest['sessions'] if e['partition'] in {'train', 'development'}]
        for si, entry in enumerate(sessions):
            bundle = read_bundle(run, entry)
            photos = bundle['photos']
            maps = {}
            for algorithm in ALGORITHMS:
                groups = run_algorithm(algorithm, photos, grouping_config=manifest['grouping_config'])
                maps[algorithm] = {pid: (group.roster, j, len(group.photo_ids))
                                   for j, group in enumerate(groups) for pid in group.photo_ids}
            previous = None
            block = []
            session_rows = []
            def finish_block(block):
                if block:
                    block[len(block) // 2]['representative'] = True
                    for row in block:
                        row['data']['similar_photos'] = len(block)
            for i, photo in enumerate(photos):
                pid = photo['id']
                rosters = [maps[a][pid][0] for a in ALGORITHMS]
                cuts = [i > 0 and maps[a][pid][1] != maps[a][photos[i - 1]['id']][1] for a in ALGORITHMS]
                kind = classify(rosters, cuts, photo, display)
                meta = bundle['presentation'][str(pid)]
                data = {'id': pid, 'partition': entry['partition'], 'filename': meta['filename'],
                        'folder': meta['folder'], 'file_hash': meta.get('file_hash'),
                        'thumbnail': meta.get('thumbnail'), 'timestamp': photo['timestamp'],
                        'reference': bundle['answers'].get(str(pid)), 'kind': kind,
                        'neighbors': [p['id'] for p in photos[max(0, i-2):i+3]],
                        'suggestions': {a: {'taxa': maps[a][pid][0], 'size': maps[a][pid][2], 'cut': cuts[j]}
                                        for j, a in enumerate(ALGORITHMS)}}
                row = {'id': pid, 'kind': kind, 'data': data, 'representative': False,
                       'random': digest([seed, entry['id'], pid])}
                signature = (kind, tuple(rosters))
                if signature != previous:
                    finish_block(block)
                    block = []
                block.append(row)
                previous = signature
                session_rows.append(row)
                stats['photos_considered'] += 1
                if kind:
                    stats[kind + '_available'] += 1
                    (agreements if kind == 'agreement' else candidates).append(row)
            finish_block(block)
            conn.executemany('INSERT INTO photos VALUES(?,?,?,?)',
                             ((r['id'], entry['id'], entry['partition'], json.dumps(r['data'])) for r in session_rows))
            print(f'Prepared review context for {si + 1}/{len(sessions)} sessions', flush=True)
        sampled = sorted(agreements, key=lambda r: r['random'])[:agreement_sample]
        priorities = {'species': 0, 'boundary': 1, 'abstention': 2, 'identity': 3}
        candidates.sort(key=lambda r: (r['kind'] == 'identity', not r['representative'], priorities[r['kind']], r['random']))
        main = [r for r in candidates if r['kind'] != 'identity']
        identities = [r for r in candidates if r['kind'] == 'identity']
        # Interleave agreement checks so they are encountered during ordinary review.
        ordered = []
        for i, row in enumerate(main):
            ordered.append(row)
            if (i + 1) % 9 == 0 and sampled:
                ordered.append(sampled.pop(0))
        ordered.extend(sampled)
        ordered.extend(identities)
        conn.executemany('INSERT INTO items VALUES(?,?,?,?)',
                         ((r['id'], r['kind'], i, r['representative']) for i, r in enumerate(ordered)))
        metadata = {'format_version': 1, 'created_at': datetime.now(UTC).isoformat(),
                    'source_run': str(run), 'source_created_at': manifest['created_at'],
                    'source_data_digest': manifest['data_digest'], 'capture_date': manifest.get('capture_date'),
                    'source_code': manifest['code'], 'review_code': code_identity(repo),
                    'library': str(library), 'workspace': manifest['workspace'],
                    'split_registry': manifest['split_registry_path'], 'seed': seed,
                    'stats': dict(stats), 'agreement_sample': min(agreement_sample, len(agreements)),
                    'test_sessions_included': False}
        conn.executemany('INSERT INTO metadata VALUES(?,?)', ((k, json.dumps(v)) for k, v in metadata.items()))
        conn.commit()
        return metadata
    except BaseException:
        conn.close()
        output.unlink(missing_ok=True)
        raise
    finally:
        conn.close()


def metadata(conn):
    return {r['key']: json.loads(r['value']) for r in conn.execute('SELECT * FROM metadata')}


def save_review(conn, photo_id, *, taxa, complete, notes='', revision=0, update_vireo_tags=False):
    if type(complete) is not bool or type(revision) is not int or revision < 0:
        raise ValueError('Invalid completion or revision value')
    if not isinstance(taxa, list) or len(taxa) > 50 or any(not isinstance(k, str) for k in taxa):
        raise ValueError('Choose a list of species')
    if not isinstance(notes, str) or len(notes) > 2000:
        raise ValueError('Notes must be at most 2,000 characters')
    taxa = sorted(set(taxa))
    if not complete and not taxa:
        raise ValueError('An empty list requires explicit confirmation of no target species')
    for key in taxa:
        if not conn.execute('SELECT 1 FROM species WHERE key=?', (key,)).fetchone():
            raise ValueError('Choose species from the catalog')
    with conn:
        conn.execute('BEGIN IMMEDIATE')
        photo = conn.execute('SELECT partition FROM photos WHERE id=?', (photo_id,)).fetchone()
        if not photo or photo['partition'] not in {'train', 'development'}:
            raise ValueError('Photo is outside the review pool')
        current = conn.execute('SELECT revision FROM reviews WHERE photo_id=?', (photo_id,)).fetchone()
        if (current['revision'] if current else 0) != revision:
            raise ValueError('This photo was changed in another tab; reload before saving')
        answer = {'photo_id': photo_id, 'revision': revision + 1, 'status': 'complete' if complete else 'partial',
                  'taxa': taxa, 'notes': notes, 'updated_at': datetime.now(UTC).isoformat()}
        conn.execute('INSERT OR REPLACE INTO reviews VALUES(?,?,?,?,?,?)',
                     (photo_id, answer['revision'], answer['status'], json.dumps(taxa), notes, answer['updated_at']))
        conn.execute('INSERT INTO review_history VALUES(?,?,?)', (photo_id, answer['revision'], json.dumps(answer)))
        if update_vireo_tags:
            from .review_tags import ensure_tag_sync

            ensure_tag_sync(conn)
            conn.execute("INSERT OR REPLACE INTO tag_sync(photo_id,revision,status) VALUES(?,?,'pending')",
                         (photo_id, answer['revision']))
    return answer


def load_review_labels(path, db_path, workspace, rows):
    """Read explicit human corrections; algorithms never receive these records."""
    conn = connect_readonly(path)
    try:
        meta = metadata(conn)
        if meta['library'] != str(Path(db_path).expanduser().resolve()) or meta['workspace'] != workspace:
            raise ValueError('Review labels belong to a different library or workspace')
        current = {r['id']: {'folder': r['folder_path'], **r} for r in rows}
        answers = {}
        for r in conn.execute('SELECT r.*,p.partition,p.data FROM reviews r JOIN photos p ON p.id=r.photo_id'):
            before = json.loads(r['data'])
            if r['partition'] not in {'train', 'development'}:
                raise ValueError('Review file contains held-out test labels')
            now = current.get(r['photo_id'])
            if now is None or not same_photo(before, now):
                raise ValueError(f"Reviewed photo {r['photo_id']} no longer matches this library; reconcile the review first")
            answers[r['photo_id']] = {'taxa': set(json.loads(r['taxa'])), 'sources': {'manual'},
                                       'complete': r['status'] == 'complete', 'review_partition': r['partition']}
        return answers
    finally:
        conn.close()


def connect_readonly(path):
    conn = sqlite3.connect(Path(path).resolve().as_uri() + '?mode=ro', uri=True)
    conn.row_factory = sqlite3.Row
    return conn
