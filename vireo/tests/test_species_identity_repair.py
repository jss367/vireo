"""A name-index refresh must not rewrite historical species identities."""
import json
import sqlite3
from types import SimpleNamespace

import pytest
from species_identity import SpeciesResolver
from species_identity_repair import refresh_common_name_index


@pytest.fixture
def catalog():
    conn = sqlite3.connect(':memory:')
    conn.row_factory = sqlite3.Row
    conn.executescript('''
        CREATE TABLE taxa(id INTEGER PRIMARY KEY, inat_id, name, common_name, rank);
        CREATE TABLE taxa_common_names(taxon_id, name, locale, UNIQUE(taxon_id,name,locale));
        CREATE TABLE db_meta(key TEXT PRIMARY KEY, value);
        CREATE TABLE keywords(id INTEGER PRIMARY KEY, taxon_id, name);
        CREATE TABLE photo_keywords(photo_id, keyword_id);
        INSERT INTO taxa VALUES(1,9176,'Zonotrichia leucophrys','Old sparrow name','species');
        INSERT INTO taxa VALUES(2,42408,'Bison bison','American bison','species');
        INSERT INTO keywords VALUES(1,2,'American bison');
        INSERT INTO photo_keywords VALUES(100,1);
        INSERT INTO taxa_common_names VALUES(1,'Obsolete alias','en');
        INSERT INTO taxa_common_names VALUES(1,'French name','fr');
    ''')
    conn.commit()
    yield conn
    conn.close()


def payload():
    entry = {'taxon_id': 9176, 'scientific_name': 'Zonotrichia leucophrys',
             'common_name': 'White-crowned Sparrow', 'rank': 'species'}
    return {'source': 'iNaturalist DWCA', 'common_name_identity_version': 1,
            'ambiguous_common_names': [],
            'taxa_by_scientific': {'zonotrichia leucophrys': entry},
            'taxa_by_common': {'white-crowned sparrow': entry, 'alternate sparrow': entry}}


def resolver(conn):
    def meta(key):
        row = conn.execute('SELECT value FROM db_meta WHERE key=?', (key,)).fetchone()
        return row[0] if row else None
    return SpeciesResolver(db=SimpleNamespace(conn=conn, get_meta=meta))


def test_refresh_unifies_legacy_and_native_predictions_preserving_references(catalog):
    legacy = {'classifier_model': 'BioCLIP-2.5', 'species': 'White-crowned Sparrow',
              'scientific_name': 'Zonotrichia leucophrys', 'labels_fingerprint': 'custom'}
    native = {**legacy, 'classifier_model': 'iNat21', 'labels_fingerprint': 'tol'}
    assert resolver(catalog).prediction(legacy).key == 'name:white-crowned sparrow'
    with catalog:
        refresh_common_name_index(catalog, payload())
    r = resolver(catalog)
    assert r.prediction(legacy).key == r.prediction(native).key == 'taxon:9176'
    assert r.resolve('Alternate sparrow').key == 'taxon:9176'
    assert r.resolve('Obsolete alias').taxon_id is None
    assert r.resolve('Old sparrow name').taxon_id is None
    assert tuple(catalog.execute('SELECT * FROM keywords').fetchone()) == (1, 2, 'American bison')
    assert tuple(catalog.execute('SELECT * FROM photo_keywords').fetchone()) == (100, 1)
    assert tuple(catalog.execute('SELECT inat_id,name,common_name FROM taxa WHERE id=2').fetchone()) == (42408, 'Bison bison', None)
    assert catalog.execute("SELECT COUNT(*) FROM taxa_common_names WHERE locale='fr'").fetchone()[0] == 1
    with catalog:
        assert refresh_common_name_index(catalog, payload())['preferred_names_changed'] == 0


def test_refresh_keeps_ambiguity_and_explicit_source_identity(catalog):
    data = payload()
    data['ambiguous_common_names'] = ['white-crowned sparrow']
    with catalog:
        refresh_common_name_index(catalog, data)
    r = resolver(catalog)
    assert r.resolve('White-crowned Sparrow').taxon_id is None
    assert r.resolve('White-crowned Sparrow', source={'taxon_id': 42408}).key == 'taxon:42408'
    assert r.resolve('Zonotrichia leucophrys').key == 'taxon:9176'
    assert json.loads(catalog.execute("SELECT value FROM db_meta WHERE key='ambiguous_common_names'").fetchone()[0]) == ['white-crowned sparrow']


@pytest.mark.parametrize('invalid', ['unversioned', 'empty'])
def test_refresh_rejects_unverified_or_empty_catalog_without_writing(catalog, invalid):
    data = payload()
    if invalid == 'unversioned':
        del data['common_name_identity_version']
    else:
        data['taxa_by_scientific'] = {}
    before = list(catalog.iterdump())
    with pytest.raises(ValueError), catalog:
        refresh_common_name_index(catalog, data)
    assert list(catalog.iterdump()) == before


def test_refresh_rolls_back_with_callers_transaction(catalog):
    before = list(catalog.iterdump())
    with pytest.raises(RuntimeError), catalog:
        refresh_common_name_index(catalog, payload())
        raise RuntimeError('abort')
    assert list(catalog.iterdump()) == before


def test_refresh_preserves_preferred_names_for_scientific_homonyms(catalog):
    data = payload()
    bird = {"taxon_id": 1, "scientific_name": "Prunella", "common_name": "Accentors"}
    plant = {"taxon_id": 2, "scientific_name": "Prunella", "common_name": "Self-heals"}
    data["taxa_by_scientific"]["prunella"] = bird
    data["scientific_homonyms"] = {"prunella": [bird, plant]}
    catalog.executemany("INSERT INTO taxa VALUES (?, ?, 'Prunella', 'Old name', 'genus')",
                        [(3, 1), (4, 2)])
    with catalog:
        refresh_common_name_index(catalog, data)
    assert [tuple(row) for row in catalog.execute(
        "SELECT inat_id,common_name FROM taxa WHERE id IN (3,4) ORDER BY id")
    ] == [(1, "Accentors"), (2, "Self-heals")]
