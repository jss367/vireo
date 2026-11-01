"""Photo detail explains aliases without rewriting saved labels or evidence."""

import json

import pytest
from mcp_server import photo_detail


@pytest.fixture
def starling_photo(app_and_db):
    app, db = app_and_db
    db.conn.execute("INSERT INTO taxa (id, inat_id, name, common_name, rank) "
                    "VALUES (6424, 14850, 'Sturnus vulgaris', 'European Starling', 'species')")
    parent = db.add_keyword('Birds')
    root = db.add_keyword('Common starling', kw_type='taxonomy', is_species=True)
    leaf = db.add_keyword('European starling', parent_id=parent, kw_type='taxonomy', is_species=True)
    db.conn.execute('UPDATE keywords SET taxon_id=6424 WHERE id IN (?, ?)', (root, leaf))
    # Unused taxonomy aliases need not overwhelm the photo's saved names.
    db.conn.execute("INSERT INTO taxa_common_names (taxon_id, name, locale) "
                    "VALUES (6424, 'cost', 'en')")
    db.tag_photo(1, leaf)
    provenance = {'method': 'direct_visual', 'reviewer': 'Test reviewer',
                  'reason': 'Speckled plumage', 'supporting_photo_ids': [1]}
    db.species_provenance.set(1, leaf, provenance)
    db.commit()
    return app, db, leaf, provenance


def test_detail_and_mcp_explain_summary_alias_without_mutation(starling_photo):
    app, db, leaf, provenance = starling_photo
    before = [tuple(r) for r in db.conn.execute('SELECT * FROM photo_keywords WHERE photo_id=1')]
    response = app.test_client().get('/api/photos/1')
    assert response.status_code == 200
    detail = response.get_json()
    assert 'Common starling' in detail['species']
    for payload in (detail, photo_detail(detail)):
        assignments = [a for a in payload['species_assignments'] if a['keyword_id'] == leaf]
        assert len(assignments) == 1
        assignment = assignments[0]
        keyword = next(k for k in payload['keywords'] if k['id'] == leaf)
        assert assignment['species'] == keyword['name'] == 'European starling'
        for row in (assignment, keyword):
            assert row['display_name'] == 'European Starling'
            assert [s.casefold() for s in row['synonyms']] == ['common starling']
            assert row['taxon_id'] == 6424
            assert row['source_taxon_id'] == 14850
            assert row['scientific_name'] == 'Sturnus vulgaris'
        assert {key: assignment[key] for key in provenance} == provenance
    assert [tuple(r) for r in db.conn.execute('SELECT * FROM photo_keywords WHERE photo_id=1')] == before
    assert json.loads(db.species_provenance.state(1, leaf)['species_provenance']) == provenance


def test_aliases_follow_taxon_not_common_name(starling_photo):
    _, db, leaf, _ = starling_photo
    db.conn.execute("INSERT INTO taxa (id, name, common_name, rank) "
                    "VALUES (6425, 'Other species', 'European Starling', 'species')")
    other = db.add_keyword('Different bird', kw_type='taxonomy', is_species=True)
    db.conn.execute('UPDATE keywords SET taxon_id=6425 WHERE id=?', (other,))
    db.tag_photo(1, other)
    rows = {a['keyword_id']: a for a in db.species_provenance.assignments(1)}
    assert 'Different bird' not in rows[leaf]['synonyms']
    assert rows[other]['synonyms'] == ['Different bird']
    assert rows[other]['taxon_id'] != rows[leaf]['taxon_id']


def test_missing_common_name_and_unlinked_keywords(starling_photo):
    _, db, leaf, _ = starling_photo
    db.conn.execute('UPDATE taxa SET common_name=NULL WHERE id=6424')
    unlinked = db.add_keyword('Unresolved bird', is_species=True)
    db.tag_photo(1, unlinked)
    rows = {a['keyword_id']: a for a in db.species_provenance.assignments(1)}
    assert rows[leaf]['display_name'] == 'Sturnus vulgaris'
    assert 'European starling' in rows[leaf]['synonyms']
    assert rows[unlinked]['species'] == 'Unresolved bird'
    assert 'taxon_id' not in rows[unlinked]


def test_provenance_update_keeps_display_identity(starling_photo):
    app, _, leaf, _ = starling_photo
    response = app.test_client().post(f'/api/photos/1/species/{leaf}/provenance', json={
        'method': 'direct_visual', 'reviewer': 'Another reviewer',
    })
    assert response.status_code == 200
    assignment = next(a for a in response.get_json()['species_assignments'] if a['keyword_id'] == leaf)
    assert assignment['display_name'] == 'European Starling'
    assert assignment['taxon_id'] == 6424
    assert assignment['reviewer'] == 'Another reviewer'


@pytest.mark.parametrize('stale_rank', ['species', 'genus', None])
def test_display_identity_prefers_explicit_source_over_local_link(starling_photo, stale_rank):
    app, db, leaf, provenance = starling_photo
    if stale_rank:
        db.conn.execute("INSERT INTO taxa (id, inat_id, name, common_name, rank) "
                        "VALUES (6425, 99000, 'Other taxon', 'Other bird', ?)", (stale_rank,))
    db.conn.execute('UPDATE keywords SET source_taxon_id=14850, taxon_id=? WHERE id=?',
                    (6425 if stale_rank else None, leaf))
    # A stale local starling link must not make another source's keyword an alias.
    other = db.add_keyword('Unrelated bird alias', is_species=True)
    db.conn.execute('UPDATE keywords SET source_taxon_id=99000, taxon_id=6424 WHERE id=?', (other,))
    db.commit()
    before = [tuple(r) for r in db.conn.execute('SELECT * FROM photo_keywords WHERE photo_id=1')]
    detail = app.test_client().get('/api/photos/1').get_json()
    for payload in (detail, photo_detail(detail)):
        assignment = next(a for a in payload['species_assignments'] if a['keyword_id'] == leaf)
        keyword = next(k for k in payload['keywords'] if k['id'] == leaf)
        for row in (assignment, keyword):
            assert row['display_name'] == 'European Starling'
            assert row['taxon_id'] == 6424
            assert row['source_taxon_id'] == 14850
            assert row['scientific_name'] == 'Sturnus vulgaris'
            assert row['synonyms'] == ['Common starling']
        assert {key: assignment[key] for key in provenance} == provenance
    assert [tuple(r) for r in db.conn.execute('SELECT * FROM photo_keywords WHERE photo_id=1')] == before
    saved = db.conn.execute('SELECT source_taxon_id, taxon_id FROM keywords WHERE id=?', (leaf,)).fetchone()
    assert tuple(saved) == (14850, 6425 if stale_rank else None)


def test_missing_source_taxon_does_not_fall_back_to_conflicting_local_taxon(starling_photo):
    app, db, leaf, _ = starling_photo
    db.conn.execute('UPDATE keywords SET source_taxon_id=999999 WHERE id=?', (leaf,))
    db.commit()
    detail = app.test_client().get('/api/photos/1').get_json()
    for payload in (detail, photo_detail(detail)):
        assignment = next(a for a in payload['species_assignments'] if a['keyword_id'] == leaf)
        keyword = next(k for k in payload['keywords'] if k['id'] == leaf)
        for row in (assignment, keyword):
            assert row['source_taxon_id'] == 999999
            assert row['display_name'] == 'European starling'
            assert not row.get('taxon_id')
            assert not row.get('scientific_name')
            assert not row.get('synonyms')
