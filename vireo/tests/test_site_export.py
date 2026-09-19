"""Complete site exports: scope, portable references, and failure handling."""

import json
import sqlite3
import threading
from contextlib import closing
from pathlib import Path

import pytest
from PIL import Image
from test_site_publish import _seed_publish_app
from wait import wait_for_job_via_client


def _run_export(app, destination, **options):
    client = app.test_client()
    response = client.post('/api/jobs/export-site', json={
        'destination': str(destination), **options,
    })
    assert response.status_code == 200
    return wait_for_job_via_client(client, response.json['job_id'])


def _read(root, path):
    return json.loads((root / path).read_text())


def test_complete_export_preserves_albums_metadata_and_workspace_scope(tmp_path, monkeypatch):
    app, db, meta = _seed_publish_app(tmp_path, monkeypatch)
    pids = db.get_photo_ids()
    default_albums = {row['id'] for row in db.get_collections()}
    first = db.add_collection('../Same / name', json.dumps([
        {'field': 'photo_ids', 'value': pids[:2]},
    ]))
    second = db.add_collection('../Same / name', json.dumps([
        {'field': 'photo_ids', 'value': pids[1:]},
    ]))
    empty = db.add_collection('Empty', '[{"field":"photo_ids","value":[]}]')
    db.conn.execute('UPDATE photos SET exif_data = ? WHERE id = ?', (
        json.dumps({'XMP': {'Title': 'A cardinal', 'Description': 'In the garden'},
                    'EXIF': {'UserComment': 'Morning walk'}}), pids[0],
    ))
    db.conn.commit()
    original_ws = db._ws_id()
    other_ws = db.create_workspace('Other site')
    db.set_active_workspace(other_ws)
    other_folder = db.add_folder(str(tmp_path / 'other'), name='other')
    foreign = db.add_photo(folder_id=other_folder, filename='private.jpg', extension='.jpg',
                           file_size=1, file_mtime=1)
    db.add_collection('Other album', '[]')
    db.set_active_workspace(original_ws)

    destination = tmp_path / 'exports'
    destination.mkdir()
    (destination / 'keep.txt').write_text('existing file')
    job = _run_export(app, destination)
    assert job['status'] == 'completed', job
    assert job['result']['exported_images'] == 3
    output = Path(job['result']['destination'])
    site = _read(output, 'site.json')
    photos = _read(output, 'photos.json')
    assert site['status'] == 'complete'
    assert {p['id'] for p in photos} == set(pids)
    assert foreign not in {p['id'] for p in photos}
    assert {a['id'] for a in site['albums']} == default_albums | {first, second, empty}
    for album in site['albums']:
        manifest = _read(output, album['manifest'])
        for ref in manifest['photos']:
            assert (output / ref['image']).is_file()
        if album['id'] == empty:
            assert manifest['photos'] == []
    assert {first, second} <= set(photos[1]['album_ids'])
    assert photos[0]['title'] == 'A cardinal'
    assert photos[0]['caption'] == 'In the garden'
    assert photos[0]['notes'] == 'Morning walk'
    assert photos[0]['species']
    assert all(k['type'] != 'location' for p in photos for k in p['keywords'])
    assert all('latitude' not in p for p in photos)
    assert len(list((output / 'photos').iterdir())) == 3
    with Image.open(output / photos[0]['image']) as rendered:
        assert rendered.size == (1200, 800)
    life = _read(output, 'life-list.json')
    assert life['meta']['species_count'] == 2
    assert all(s['locations'] == [] for s in life['species'])
    assert all((output / s['best']['image']).is_file() for s in life['species'])
    assert 'Photos in multiple' in (output / 'README.md').read_text()
    assert (destination / 'keep.txt').read_text() == 'existing file'
    second_job = _run_export(app, destination, include_locations=True)
    second_output = Path(second_job['result']['destination'])
    assert second_output != output and output.is_dir()
    second_photos = _read(second_output, 'photos.json')
    assert any(k['type'] == 'location' for p in second_photos for k in p['keywords'])
    db.close()


def test_export_has_no_species_limit_and_disambiguates_filenames(tmp_path, monkeypatch):
    app, db, meta = _seed_publish_app(tmp_path, monkeypatch)
    keyword = db.add_keyword('Northern Cardinal', is_species=True)
    folder = db.add_folder(str(meta['photos_dir']), name='photos')
    for i in range(201):
        name = f'extra-{i}.jpg'
        Image.new('RGB', (8, 8)).save(meta['photos_dir'] / name)
        pid = db.add_photo(folder_id=folder, filename=name, extension='.jpg',
                           file_size=1, file_mtime=1)
        db.tag_photo(pid, keyword)
    duplicate_folder = tmp_path / 'duplicates'
    duplicate_folder.mkdir()
    Image.new('RGB', (8, 8)).save(duplicate_folder / 'cardinal.jpg')
    fid = db.add_folder(str(duplicate_folder), name='duplicates')
    duplicate = db.add_photo(folder_id=fid, filename='cardinal.jpg', extension='.jpg',
                             file_size=1, file_mtime=1)
    job = _run_export(app, tmp_path / 'export')
    assert job['status'] == 'completed', job
    assert job['result']['exported_images'] == 205
    output = Path(job['result']['destination'])
    life = _read(output, 'life-list.json')
    cardinal = next(s for s in life['species'] if s['species'] == 'Northern Cardinal')
    assert len(cardinal['photos']) == 202
    assert cardinal['has_more'] is False
    photos = {p['id']: p for p in _read(output, 'photos.json')}
    assert photos[duplicate]['image'] != photos[meta['p1']]['image']
    assert len(list((output / 'photos').iterdir())) == 205
    db.close()


def test_missing_photo_produces_incomplete_export_and_failed_job(tmp_path, monkeypatch):
    app, db, meta = _seed_publish_app(tmp_path, monkeypatch)
    (meta['photos_dir'] / 'cardinal.jpg').unlink()
    db.conn.execute("UPDATE folders SET status = 'missing'")
    db.conn.commit()
    job = _run_export(app, tmp_path / 'export')
    assert job['status'] == 'failed'
    assert job['result']['exported_images'] == 2
    assert job['errors']
    root = Path(job['result']['destination'])
    assert _read(root, 'site.json')['status'] == 'incomplete'
    missing = next(p for p in _read(root, 'photos.json') if p['id'] == meta['p1'])
    assert missing['image'] is None and missing['error']
    assert not list((root / 'photos').glob(f"{meta['p1']}-*"))
    db.close()


@pytest.mark.parametrize('include_locations', [False, True])
def test_album_definitions_respect_location_export_option(tmp_path, monkeypatch, include_locations):
    app, db, meta = _seed_publish_app(tmp_path, monkeypatch)
    latitude, longitude = 37.421234, -122.081234
    db.conn.execute('UPDATE photos SET latitude = ?, longitude = ? WHERE id = ?',
                    (latitude, longitude, meta['p1']))
    db.conn.commit()
    rules = {'mode': 'all', 'rules': [
        {'field': 'gps_lat', 'op': 'equals', 'value': latitude},
        {'mode': 'any', 'rules': [{'field': 'gps_lng', 'op': 'equals', 'value': longitude}]},
    ]}
    album_id = db.add_collection('Selected photos', json.dumps(rules))
    job = _run_export(app, tmp_path / 'export', include_locations=include_locations)
    assert job['status'] == 'completed', job
    root = Path(job['result']['destination'])
    album = next(a for a in _read(root, 'site.json')['albums'] if a['id'] == album_id)
    exported = _read(root, album['manifest'])
    assert exported['photo_ids'] == [meta['p1']]
    assert (root / exported['photos'][0]['image']).is_file()
    if include_locations:
        assert exported['rules'] == rules
    else:
        for manifest in root.rglob('*.json'):
            text = manifest.read_text()
            assert str(latitude) not in text and str(longitude) not in text
        assert 'rules' not in exported and 'visual' not in exported
    db.close()


def test_site_export_applies_saved_crop(tmp_path, monkeypatch):
    app, db, meta = _seed_publish_app(tmp_path, monkeypatch)
    db.set_photo_edit_recipe(meta['p1'], {'crop': {'x': 0, 'y': 0, 'w': 0.5, 'h': 1}})
    job = _run_export(app, tmp_path / 'export')
    assert job['status'] == 'completed', job
    output = Path(job['result']['destination'])
    photo = next(p for p in _read(output, 'photos.json') if p['id'] == meta['p1'])
    assert photo['edits']['crop']['w'] == 0.5
    with Image.open(output / photo['image']) as rendered:
        assert rendered.size == (600, 800)
    db.close()


def test_export_metadata_is_consistent_when_workspace_changes_during_capture(tmp_path, monkeypatch):
    import site_export
    from db import Database

    app, db, meta = _seed_publish_app(tmp_path, monkeypatch)
    pid = meta['p1']
    album_id = db.add_collection('Original album', json.dumps([
        {'field': 'photo_ids', 'value': [pid]},
    ]))
    db.set_photo_edit_recipe(pid, {'crop': {'x': 0, 'y': 0, 'w': 0.5, 'h': 1}})
    db.conn.execute('UPDATE photos SET latitude = 10, longitude = 20, exif_data = ? WHERE id = ?',
                    (json.dumps({'XMP': {'Title': 'Original title'}}), pid))
    db.conn.commit()
    original_collections = Database.get_collections
    original_load = site_export.load_export_image
    readers = []

    def edit_after_ids_are_captured(reader):
        rows = original_collections(reader)
        if readers:
            return rows
        readers.append(reader)
        assert reader.conn.in_transaction
        # A separate connection can commit while the snapshot is being read.
        with closing(sqlite3.connect(meta['vireo_dir'] / 'vireo.db', timeout=2)) as writer:
            with writer:
                writer.execute("UPDATE keywords SET name = 'Changed species' WHERE name = 'Northern Cardinal'")
                writer.execute('UPDATE photos SET latitude = 30, longitude = 40, exif_data = ? WHERE id = ?',
                               (json.dumps({'XMP': {'Title': 'Changed title'}}), pid))
                writer.execute('DELETE FROM photo_edit_recipes WHERE photo_id = ?', (pid,))
                writer.execute("UPDATE collections SET name = 'Changed album', rules = '[]' WHERE id = ?", (album_id,))
                writer.execute('DELETE FROM workspace_folders WHERE workspace_id = ?', (reader._ws_id(),))
        return rows

    def render_after_transaction_is_released(*args, **kwargs):
        assert readers and not readers[0].conn.in_transaction
        return original_load(*args, **kwargs)

    monkeypatch.setattr(Database, 'get_collections', edit_after_ids_are_captured)
    monkeypatch.setattr(site_export, 'load_export_image', render_after_transaction_is_released)
    job = _run_export(app, tmp_path / 'export', include_locations=True)
    assert job['status'] == 'completed', job
    assert job['result']['photo_count'] == 3
    root = Path(job['result']['destination'])
    record = next(p for p in _read(root, 'photos.json') if p['id'] == pid)
    assert record['title'] == 'Original title'
    assert (record['latitude'], record['longitude']) == (10, 20)
    assert record['edits']['crop']['w'] == 0.5
    assert any(k['name'] == 'Northern Cardinal' for k in record['keywords'])
    life = _read(root, 'life-list.json')
    assert any(s['species'] == 'Northern Cardinal' for s in life['species'])
    album = next(a for a in _read(root, 'site.json')['albums'] if a['id'] == album_id)
    assert album['name'] == 'Original album'
    assert _read(root, album['manifest'])['photo_ids'] == [pid]
    with Image.open(root / record['image']) as image:
        assert image.size == (600, 800)
    assert db.get_photo_ids() == []
    assert db.get_photo_edit_recipe(pid) is None
    db.close()


@pytest.mark.parametrize('cancelled', [False, True])
def test_snapshot_transaction_released_on_capture_error(tmp_path, monkeypatch, cancelled):
    from site_export import export_site
    from web.background_jobs import JobCancelled

    _, db, meta = _seed_publish_app(tmp_path, monkeypatch)

    def fail(*args, **kwargs):
        raise OSError('Metadata unavailable')

    def checkpoint():
        assert not db.conn.in_transaction, 'Pause checkpoint held the catalog snapshot'

    with pytest.raises(JobCancelled if cancelled else OSError):
        export_site(
            db, str(meta['vireo_dir']), str(tmp_path / 'export'),
            build_life_list=fail, resolve_visual=None,
            checkpoint=checkpoint, cancel_check=lambda: cancelled,
        )
    assert not db.conn.in_transaction
    assert not (tmp_path / 'export').exists()
    db.close()


def test_empty_site_export_is_valid(tmp_path, monkeypatch):
    app, db, _ = _seed_publish_app(tmp_path, monkeypatch)
    db.conn.execute('DELETE FROM workspace_folders WHERE workspace_id = ?', (db._ws_id(),))
    db.conn.commit()
    job = _run_export(app, tmp_path / 'export')
    assert job['status'] == 'completed', job
    output = Path(job['result']['destination'])
    assert _read(output, 'photos.json') == []
    assert _read(output, 'life-list.json')['species'] == []
    assert _read(output, 'site.json')['photo_count'] == 0
    db.close()


def test_site_export_job_cancels_between_photos_and_removes_partial_folder(tmp_path, monkeypatch):
    import site_export

    app, db, _ = _seed_publish_app(tmp_path, monkeypatch)
    entered, release = threading.Event(), threading.Event()
    original_load = site_export.load_export_image
    loaded = []

    def controlled_load(photo, *args, **kwargs):
        loaded.append(photo['id'])
        entered.set()
        assert release.wait(10), 'Cancellation never arrived'
        return original_load(photo, *args, **kwargs)

    monkeypatch.setattr(site_export, 'load_export_image', controlled_load)
    destination = tmp_path / 'exports'
    client = app.test_client()
    response = client.post('/api/jobs/export-site', json={'destination': str(destination)})
    job_id = response.json['job_id']
    try:
        assert entered.wait(10), 'Export never started'
        assert client.post(f'/api/jobs/{job_id}/cancel').status_code == 200
    finally:
        release.set()
    job = wait_for_job_via_client(client, job_id)
    assert job['status'] == 'cancelled'
    assert len(loaded) == 1
    assert list(destination.iterdir()) == []
    db.close()


@pytest.mark.parametrize('body', [None, [], {}, {'destination': 'relative'},
    {'destination': '/tmp/export', 'include_locations': 'false'},
    {'destination': 42}, {'destination': '/tmp/\x00bad'}])
def test_export_rejects_invalid_requests(tmp_path, monkeypatch, body):
    app, db, _ = _seed_publish_app(tmp_path, monkeypatch)
    assert app.test_client().post('/api/jobs/export-site', json=body).status_code == 400
    db.close()


@pytest.mark.parametrize('failure', ['cancel', 'fatal', 'finish_cancel'])
def test_interrupted_export_removes_only_its_own_files(tmp_path, monkeypatch, failure):
    import site_export
    from web.background_jobs import JobCancelled

    _, db, meta = _seed_publish_app(tmp_path, monkeypatch)
    destination = tmp_path / 'exports'
    destination.mkdir()
    (destination / 'keep.txt').write_text('keep')

    def progress(current, total, name, phase):
        if current == 1 and failure == 'cancel':
            raise JobCancelled('cancelled')

    if failure == 'fatal':
        def fail(*args):
            raise OSError('Disk full')
        monkeypatch.setattr(site_export, '_write_json', fail)
    with pytest.raises((JobCancelled, OSError)):
        site_export.export_site(
            db, str(meta['vireo_dir']), str(destination),
            build_life_list=lambda *a, **kw: {'species': []},
            resolve_visual=None, progress_cb=progress,
            begin_commit=lambda: failure != 'finish_cancel',
        )
    assert [p.name for p in destination.iterdir()] == ['keep.txt']
    db.close()


@pytest.mark.parametrize('mode', ['RGBA', 'P'])
def test_site_export_saves_non_jpeg_modes_as_jpeg(tmp_path, monkeypatch, mode):
    import site_export

    app, db, meta = _seed_publish_app(tmp_path, monkeypatch)
    original_load = site_export.load_export_image

    def load_alpha(photo, *args, **kwargs):
        img = original_load(photo, *args, **kwargs)
        try:
            return img.convert(mode)
        finally:
            img.close()

    monkeypatch.setattr(site_export, 'load_export_image', load_alpha)
    job = _run_export(app, tmp_path / 'export')
    assert job['status'] == 'completed', job
    result = job['result']
    assert result['errors'] == []
    output = Path(result['destination'])
    photos = _read(output, 'photos.json')
    assert photos
    for photo in photos:
        assert 'error' not in photo
        with Image.open(output / photo['image']) as rendered:
            assert rendered.format == 'JPEG'
            assert rendered.mode in ('RGB', 'L')
    db.close()


@pytest.mark.parametrize('visual_available', [True, False])
def test_visual_album_never_silently_exports_metadata_only_matches(tmp_path, monkeypatch, visual_available):
    from site_export import export_site

    _, db, meta = _seed_publish_app(tmp_path, monkeypatch)
    album_id = db.add_collection('Visual birds', '[]', '{"prompt":"bird","strength":"medium"}')

    def resolve(*args, **kwargs):
        return ({'status': 'ok' if visual_available else 'no_model'},
                [meta['p1']] if visual_available else None, None)

    result = export_site(db, str(meta['vireo_dir']), str(tmp_path / 'export'),
                         build_life_list=lambda *a, **kw: {'species': []},
                         resolve_visual=resolve)
    root = Path(result['destination'])
    site = _read(root, 'site.json')
    album = _read(root, next(a['manifest'] for a in site['albums'] if a['id'] == album_id))
    if visual_available:
        assert album['photo_ids'] == [meta['p1']]
        assert result['ok'] is True
    else:
        assert album['photo_ids'] is None
        assert album['status'] == 'error'
        assert result['ok'] is False
        assert 'no_model' in result['errors'][0]
    db.close()
