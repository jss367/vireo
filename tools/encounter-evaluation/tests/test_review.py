import json
import sqlite3
import threading
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import pytest

from encounter_eval.common import write_json
from encounter_eval.library import prepare, read_bundle
from encounter_eval.review import build_queue, classify, connect, load_review_labels, same_photo, save_review
from encounter_eval.review_server import make_server


@pytest.fixture
def queue(library, tmp_path):
    run = tmp_path / 'run'
    manifest = prepare(library, run)
    # Fix the fixture's single day to training; production keeps stable day splits.
    for entry in manifest['sessions']:
        entry['partition'] = 'train'
    registry = json.loads(Path(manifest['split_registry_path']).read_text())
    registry['days'] = {day: 'train' for day in registry['days']}
    write_json(Path(manifest['split_registry_path']), registry)
    write_json(run / 'manifest.json', manifest)
    path = tmp_path / 'reviews.sqlite'
    build_queue(run, path, library, agreement_sample=2)
    return path, library, run, manifest


def test_classification_separates_identity_abstention_and_boundary():
    names = {'inat:1': 'Sparrow', 'name:sparrow': 'Sparrow', 'inat:2': 'Wren'}
    photo = {'evidence': []}
    assert classify([('inat:1',), ('inat:2',), None], [False]*3, photo, names) == 'species'
    assert classify([('inat:1',), ('name:sparrow',), None], [False]*3, photo, names) == 'identity'
    assert classify([('inat:1',), ('inat:1',), None], [False]*3, photo, names) == 'abstention'
    assert classify([('inat:1',)]*3, [True, False, False], photo, names) == 'boundary'
    assert classify([('inat:1',)]*3, [False]*3, photo, names) == 'agreement'
    assert classify([None]*3, [False]*3, photo, names) is None


def test_queue_excludes_test_sessions_without_even_loading_them(queue, tmp_path, monkeypatch):
    from encounter_eval import review

    path, library, run, manifest = queue
    manifest['sessions'].append({'id': 'heldout', 'partition': 'test', 'path': 'do-not-read.json.gz'})
    write_json(run/'manifest.json', manifest)
    actual = review.read_bundle
    def guarded(output, entry):
        assert entry['partition'] != 'test'
        return actual(output, entry)
    monkeypatch.setattr(review, 'read_bundle', guarded)
    second = tmp_path/'second.sqlite'
    build_queue(run, second, library, agreement_sample=2)
    with connect(second) as conn, connect(path) as first:
        assert not conn.execute("SELECT 1 FROM photos WHERE partition='test'").fetchone()
        assert [tuple(r) for r in conn.execute('SELECT * FROM items ORDER BY position')] == [tuple(r) for r in first.execute('SELECT * FROM items ORDER BY position')]
        assert conn.execute("SELECT COUNT(*) FROM items WHERE kind='agreement'").fetchone()[0] == 2


def test_saved_labels_persist_with_revision_checks_and_explicit_empty(queue):
    path, library, run, manifest = queue
    original = library.read_bytes()
    with connect(path) as conn:
        assert save_review(conn, 1, taxa=['inat:102'], complete=True)['revision'] == 1
        with pytest.raises(ValueError, match='another tab'):
            save_review(conn, 1, taxa=['inat:101'], complete=True)
        with pytest.raises(ValueError, match='empty list'):
            save_review(conn, 2, taxa=[], complete=False)
        with pytest.raises(ValueError, match='catalog'):
            save_review(conn, 2, taxa=['inat:99999999'], complete=True)
        save_review(conn, 2, taxa=[], complete=True)
        save_review(conn, 1, taxa=['inat:101'], complete=False, revision=1)
        assert conn.execute('SELECT COUNT(*) FROM review_history').fetchone()[0] == 3
    with connect(path) as conn:
        assert conn.execute('SELECT status FROM reviews WHERE photo_id=1').fetchone()[0] == 'partial'
    assert library.read_bytes() == original


def test_fresh_evaluation_consumes_reviews_without_leaking_answers_to_features(queue, tmp_path):
    path, library, run, manifest = queue
    before = read_bundle(run, manifest['sessions'][0])
    with connect(path) as conn:
        save_review(conn, 1, taxa=['inat:102'], complete=True)
        save_review(conn, 2, taxa=[], complete=True)
    output = tmp_path/'corrected'
    corrected = prepare(library, output, review_labels=path, split_registry=manifest['split_registry_path'])
    after = read_bundle(output, corrected['sessions'][0])
    assert after['photos'] == before['photos']
    assert after['answers']['1']['taxa'] == ['inat:102']
    assert after['answers']['1']['complete']
    assert after['answers']['2']['taxa'] == []
    assert after['answers']['2']['complete']
    assert corrected['data_digest'] != manifest['data_digest']
    assert corrected['reviewed_photo_count'] == 2
    assert corrected['inventory']['complete_roster_photos'] == 2


def test_review_import_rejects_identity_and_partition_changes(queue, tmp_path):
    path, library, run, manifest = queue
    with connect(path) as conn:
        save_review(conn, 1, taxa=['inat:101'], complete=True)
    with sqlite3.connect(library) as c:
        c.execute("UPDATE photos SET filename='replacement.jpg' WHERE id=1")
    with pytest.raises(ValueError, match='no longer matches'):
        prepare(library, tmp_path/'bad-identity', review_labels=path, split_registry=manifest['split_registry_path'])
    with sqlite3.connect(library) as c:
        c.execute("UPDATE photos SET filename='photo-001.jpg' WHERE id=1")
    registry = json.loads(Path(manifest['split_registry_path']).read_text())
    registry['days'] = {day: 'test' for day in registry['days']}
    write_json(Path(manifest['split_registry_path']), registry)
    with pytest.raises(ValueError, match='changed partition'):
        prepare(library, tmp_path/'bad-split', review_labels=path, split_registry=manifest['split_registry_path'])
    with pytest.raises(ValueError, match='different library'):
        load_review_labels(path, tmp_path/'other.db', 1, [])


def test_identity_guard_uses_hashes_when_available():
    before = {'filename':'a.jpg','folder':'/a','file_hash':'aaa'}
    assert same_photo(before, {'filename':'b.jpg','folder':'/b','file_hash':'aaa'})
    assert not same_photo(before, {**before,'file_hash':'bbb'})
    assert not same_photo(before, {**before,'file_hash':None})
    assert not same_photo(before, {k:v for k,v in before.items() if k != 'file_hash'})
    assert not same_photo(before, {'filename':'b.jpg','folder':'/a','file_hash':None})


def test_review_http_persistence_and_cross_origin_protection(queue):
    path, *_ = queue
    server = make_server(path)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    from urllib.parse import parse_qs, urlparse
    token = parse_qs(urlparse(server.review_url).query)['token'][0]
    base = server.review_url.split('/?')[0]
    headers = {'Authorization': 'Bearer '+token, 'Content-Type':'application/json'}
    try:
        with urlopen(server.review_url) as response:
            assert b'Review species suggestions' in response.read()
        with pytest.raises(HTTPError) as error:
            urlopen(base+'/api/status')
        assert error.value.code == 403
        body = json.dumps({'photo_id':1,'taxa':['inat:101'],'complete':True}).encode()
        request = Request(base+'/api/review', data=body, headers={**headers,'Origin':'https://unrelated.example'})
        with pytest.raises(HTTPError) as error:
            urlopen(request)
        assert error.value.code == 403
        with urlopen(Request(base+'/api/review', data=body, headers=headers)) as response:
            assert json.load(response)['revision'] == 1
        with urlopen(Request(base+'/api/photo/1', headers=headers)) as response:
            assert json.load(response)['review']['taxa'] == ['inat:101']
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


@pytest.mark.parametrize('missing_identity', [False, True])
def test_queue_rejects_wrong_or_unrecorded_source_library(queue, tmp_path, missing_identity):
    import shutil

    _, library, run, manifest = queue
    other = tmp_path / 'other.db'
    shutil.copy2(library, other)
    if missing_identity:
        manifest.pop('source_library')
        write_json(run / 'manifest.json', manifest)
    output = tmp_path / 'wrong-library-review.sqlite'
    with pytest.raises(ValueError, match='source library'):
        build_queue(run, output, library if missing_identity else other)
    assert not output.exists()
