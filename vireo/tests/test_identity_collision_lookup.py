"""Resolved-collision identity comparisons read only bound linked keywords."""
import json
import sqlite3

import pytest
from db import Database
from repositories.identity_alternatives import IdentityMergeConflict, _check_merge_confirmations
from services import photo_ingest, workspace_folders


def _setup(tmp_path, alias_count):
    db = Database(str(tmp_path / 'lookup.db'))
    folder = workspace_folders.add_folder(db, str(tmp_path / 'photos'))
    photos = [photo_ingest.add_photo(db, folder_id=folder, filename=f'{i}.jpg', extension='.jpg', file_size=1, file_mtime=1) for i in range(2)]
    common = db.add_keyword('Common alpha', is_species=True, source_taxon_id=88001)
    other = db.add_keyword('Common beta', is_species=True, source_taxon_id=88002)
    ids = []
    for index in range(alias_count):
        row = db.conn.execute("INSERT INTO keywords(name,type,is_species,source_taxon_id) VALUES (?,'taxonomy',1,88001)", (f'Scientific alias {index}',))
        ids.append(row.lastrowid)
    db.conn.executemany("INSERT INTO keywords(name,type,is_species) VALUES (?,'general',0)", [(f'Unrelated {i}',) for i in range(2000)])
    state = {'status': 'resolved', 'candidates': json.dumps([db.keywords.name_of(common), db.keywords.name_of(other)])}
    return db, photos, ids, other, state


def _identity_queries(db):
    queries = []
    db.conn.set_trace_callback(lambda sql: queries.append(sql) if sql.startswith('SELECT k.id,') and 'AS identity FROM keywords k' in sql else None)
    return queries


@pytest.mark.parametrize('conflict', [False, True])
def test_resolved_collision_uses_linked_primary_keys_and_preserves_conflict_semantics(tmp_path, conflict):
    db, photos, aliases, other, state = _setup(tmp_path, 2)
    try:
        selected = [aliases[0], other if conflict else aliases[1]]
        for photo, keyword in zip(photos, selected, strict=True):
            db.tag_photo(photo, keyword, source='accept')
        before = [tuple(row) for row in db.conn.execute('SELECT * FROM photo_keywords')]
        queries = _identity_queries(db)
        if conflict:
            with pytest.raises(IdentityMergeConflict, match='conflicting species confirmations'):
                _check_merge_confirmations(db.conn, *photos, state)
        else:
            _check_merge_confirmations(db.conn, *photos, state)
        db.conn.set_trace_callback(None)
        assert len(queries) == 1
        rows = [row for query in queries for row in db.conn.execute(query)]
        assert {row['id'] for row in rows} == set(selected)
        plans = [row[3] for query in queries for row in db.conn.execute('EXPLAIN QUERY PLAN ' + query)]
        assert any('SEARCH k USING INTEGER PRIMARY KEY' in detail for detail in plans), plans
        assert not any('SCAN k' in detail for detail in plans), plans
        assert [tuple(row) for row in db.conn.execute('SELECT * FROM photo_keywords')] == before
    finally:
        db.close()


def test_resolved_collision_chunks_large_link_union_under_legacy_parameter_limit(tmp_path):
    db, photos, aliases, _, state = _setup(tmp_path, 1200)
    try:
        for index, keyword in enumerate(aliases):
            db.tag_photo(photos[index % 2], keyword, source='accept', _commit=False)
        db.commit()
        db.conn.setlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, 999)
        queries = _identity_queries(db)
        _check_merge_confirmations(db.conn, *photos, state)
        db.conn.set_trace_callback(None)
        assert 2 <= len(queries) <= 3
        rows = [row for query in queries for row in db.conn.execute(query)]
        assert {row['id'] for row in rows} == set(aliases)
    finally:
        db.close()


@pytest.mark.parametrize('linked_photos', [0, 1])
def test_empty_or_one_sided_collision_skips_identity_comparison_query(tmp_path, linked_photos):
    db, photos, aliases, _, state = _setup(tmp_path, 2)
    try:
        if linked_photos:
            db.tag_photo(photos[0], aliases[0])
        queries = _identity_queries(db)
        _check_merge_confirmations(db.conn, *photos, state)
        assert queries == []
    finally:
        db.close()


@pytest.mark.parametrize('empty', [False, True])
def test_pending_identity_remap_bounds_scans_for_empty_and_duplicate_alias_names(tmp_path, empty):
    from repositories.identity_alternatives import remap_identity_keyword_intents
    db, photos, aliases, _, state = _setup(tmp_path, 2)
    try:
        alias = db.keywords.name_of(aliases[0])
        for index in range(3):
            workspace = db.require_workspace_id() if index == 0 else db.create_workspace(f'Pending alias {index}')
            db.queue_change(photos[0], 'keyword_add', alias, workspace_id=workspace)
        before = [dict(row) for row in db.conn.execute('SELECT * FROM pending_changes')]
        scans = []
        db.conn.set_trace_callback(lambda sql: scans.append(sql) if sql.startswith('SELECT k.name,') and 'AS identity FROM keywords k' in sql else None)
        moved = remap_identity_keyword_intents(db.conn, *photos, [] if empty else json.loads(state['candidates'])[:1])
        db.conn.set_trace_callback(None)
        assert len(scans) == (0 if empty else 2)
        assert len(moved) == (0 if empty else 3)
        for row in before:
            current = db.conn.execute('SELECT * FROM pending_changes WHERE id=?', (row['id'],)).fetchone()
            assert current['photo_id'] == photos[0 if empty else 1]
            assert (current['change_token'] == row['change_token']) == empty
    finally:
        db.close()
