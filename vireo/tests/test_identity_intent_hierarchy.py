from sync import sync_from_xmp, sync_to_xmp
from test_identity_alternatives import CANDIDATES, current_group, resolve, save
from test_sync import _setup_photo_with_xmp
from xmp import read_hierarchical_keywords, read_keywords, write_sidecar


def test_resolution_preserves_retained_taxonomy_alias_hierarchy(app_and_db, tmp_path):
    app, db = app_and_db
    client = app.test_client()
    scientific = 'Tyrannus melancholicus'
    db.conn.execute(
        "INSERT INTO taxa(id, inat_id, name, common_name, rank) VALUES (90001, 90001, ?, ?, 'species')",
        (scientific, CANDIDATES[0]),
    )
    root = db.add_keyword(CANDIDATES[0], is_species=True)
    alias = db.add_keyword(scientific, parent_id=root, is_species=True)
    pid, path = _setup_photo_with_xmp(tmp_path, db)
    hierarchy = CANDIDATES[0] + '|' + scientific
    write_sidecar(path, flat_keywords={CANDIDATES[0], scientific}, hierarchical_keywords={hierarchy})
    assert save(client, [pid]).status_code == 200
    db.tag_photo(pid, alias, source='accept')
    assert resolve(client, current_group(client)).status_code == 200
    assert db.conn.execute('SELECT 1 FROM photo_keywords WHERE photo_id=? AND keyword_id=?', (pid, alias)).fetchone()
    assert sync_to_xmp(db)['failed'] == 0
    assert hierarchy in read_hierarchical_keywords(path)
    assert CANDIDATES[0] not in read_keywords(path)
    assert scientific in read_keywords(path)
    sync_from_xmp(db, [pid])
    assert db.conn.execute('SELECT 1 FROM photo_keywords WHERE photo_id=? AND keyword_id=?', (pid, alias)).fetchone()
