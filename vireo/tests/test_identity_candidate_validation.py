"""Alternative labels must represent distinct authoritative species."""
from pathlib import Path

import pytest
from services.identity_alternatives import save_alternatives
from test_identity_alternatives import current_group, save
from test_sync import _setup_photo_with_xmp


def _aliases(db, kind):
    if kind == 'source_only':
        common_parent = db.add_keyword('Common hierarchy')
        scientific_parent = db.add_keyword('Scientific hierarchy')
        common = db.add_keyword('Source common', parent_id=common_parent, is_species=True, source_taxon_id=88001)
        scientific = db.add_keyword('Source scientific', parent_id=scientific_parent, is_species=True, source_taxon_id=88001)
        return [db.keywords.name_of(common), db.keywords.name_of(scientific)], common
    inat = None if kind == 'local_only' else 88001
    db.conn.execute("INSERT INTO taxa(id,inat_id,name,common_name,rank) VALUES (88001,?,'Tyrannus melancholicus','Tropical Kingbird','species')", (inat,))
    db.conn.execute("INSERT INTO taxa_common_names(taxon_id,name) VALUES (88001,'Yellow Kingbird')")
    scientific = db.add_keyword('Tyrannus melancholicus', is_species=True)
    names = ['Tropical Kingbird', 'Yellow Kingbird'] if kind == 'alternate_common' else ['Tropical Kingbird', 'Tyrannus melancholicus']
    return names, scientific


def _snapshot(db):
    return {table: [tuple(row) for row in db.conn.execute(f'SELECT * FROM {table}')]
            for table in ('photos', 'keywords', 'photo_keywords', 'photo_identity_alternatives', 'edit_history', 'edit_history_items', 'pending_changes')}


@pytest.mark.parametrize('entry', ['api', 'service'])
@pytest.mark.parametrize('kind', ['common_scientific', 'alternate_common', 'source_only', 'local_only'])
def test_equivalent_species_candidates_rejected_before_any_domain_write(app_and_db, tmp_path, entry, kind):
    app, db = app_and_db
    client = app.test_client()
    names, confirmed = _aliases(db, kind)
    photo, sidecar = _setup_photo_with_xmp(tmp_path, db, keywords={db.keywords.name_of(confirmed), 'Unrelated note'})
    db.tag_photo(photo, confirmed, source='accept')
    db.photo_review.set_flag(photo, 'rejected')
    db.queue_change(photo, 'rating', '4')
    client.get('/api/identity-alternatives')
    db.commit()
    before = _snapshot(db)
    original_xmp = Path(sidecar).read_bytes()
    if entry == 'api':
        response = save(client, [photo], candidates=names)
        assert response.status_code == 400
        assert 'same species' in response.get_json()['error']
    else:
        with pytest.raises(ValueError, match='same species'), db._commits_held():
            save_alternatives(db, [photo], names, 'Do not lose evidence', 'Call')
    assert _snapshot(db) == before
    assert Path(sidecar).read_bytes() == original_xmp


@pytest.mark.parametrize('entry', ['api', 'service'])
@pytest.mark.parametrize('kind', ['distinct_species', 'unknown', 'source_precedence'])
def test_genuinely_distinct_or_unknown_candidates_remain_valid(app_and_db, entry, kind):
    app, db = app_and_db
    client = app.test_client()
    if kind != 'unknown':
        db.conn.execute("INSERT INTO taxa(id,inat_id,name,common_name,rank) VALUES (88001,88001,'Species alpha','Alpha','species')")
        db.conn.execute("INSERT INTO taxa(id,inat_id,name,common_name,rank) VALUES (88002,88002,'Species beta','Beta','species')")
        db.add_keyword('Alpha', is_species=True, source_taxon_id=88001)
        if kind == 'source_precedence':
            # Local taxonomy suggests the same species; the explicit source
            # bindings authoritatively distinguish these keyword spellings.
            db.conn.execute("UPDATE keywords SET taxon_id=88002 WHERE name='Alpha'")
        db.add_keyword('Species beta', is_species=True, source_taxon_id=88002)
        names = ['Alpha', 'Species beta']
    else:
        names = ['Uncataloged bird alpha', 'Uncataloged bird beta']
    db.commit()
    if entry == 'api':
        assert save(client, [1], candidates=names).status_code == 200
    else:
        with db._commits_held():
            save_alternatives(db, [1], names, 'Distinct alternatives', 'Call')
    assert db.identity_alternatives.for_photos([1])[1]['candidates'] == names


@pytest.mark.parametrize('entry', ['api', 'service'])
def test_maintenance_collapsed_single_candidate_stays_editable_and_unconfirmed(app_and_db, entry):
    from repositories.identity_alternatives import retarget_candidate_names
    app, db = app_and_db
    client = app.test_client()
    assert save(client, [1], candidates=['Uncataloged alpha', 'Uncataloged beta']).status_code == 200
    retarget_candidate_names(db.conn, 'Uncataloged beta', 'Uncataloged alpha')
    db.commit()
    group = current_group(client)
    if entry == 'api':
        assert save(client, [1], candidates=['Uncataloged alpha'], group_id=group['group_id']).status_code == 200
    else:
        with db._commits_held():
            save_alternatives(db, [1], ['Uncataloged alpha'], 'Still needs confirmation', 'Call')
    assert db.identity_alternatives.for_photos([1])[1]['status'] == 'unresolved'
    assert not db.identity_alternatives.candidate_links([1], ['Uncataloged alpha'])


@pytest.mark.parametrize('entry', ['api', 'service'])
@pytest.mark.parametrize('names', [['Uncataloged alpha', ' uncataloged ALPHA '], ['Uncataloged alpha']])
def test_fresh_input_validation_matches_api_and_service_without_domain_writes(app_and_db, entry, names):
    app, db = app_and_db
    client = app.test_client()
    client.get('/api/identity-alternatives')
    db.commit()
    before = _snapshot(db)
    if entry == 'api':
        assert save(client, [1], candidates=names).status_code == 400
    else:
        with pytest.raises(ValueError), db._commits_held():
            save_alternatives(db, [1], names, 'Invalid alternatives', 'Call')
    assert _snapshot(db) == before
