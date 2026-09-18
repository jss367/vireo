"""Capture-time grouping and its workspace-scoped review API."""

import pytest
from location_review import time_review_groups


def photo(index, timestamp, lat=None, lng=None):
    return {
        "id": index,
        "filename": f"photo-{index}.jpg",
        "companion_path": None,
        "timestamp": timestamp,
        "latitude": lat,
        "longitude": lng,
    }


def ids(groups):
    return [group["photo_ids"] for group in groups]


def test_outings_split_at_gaps_days_and_maximum_span():
    photos = [
        photo(i + 1, timestamp)
        for i, timestamp in enumerate(
            [
                "2026-08-01T08:00:00",
                "2026-08-01T09:00:00",
                "2026-08-01T10:00:00",
                "2026-08-01T11:00:00",
                "2026-08-01T12:00:00",
                "2026-08-01T12:01:00",
                "2026-08-01T13:02:00",
                "2026-08-01T23:59:00",
                "2026-08-02T00:01:00",
            ]
        )
    ]
    groups = time_review_groups(list(reversed(photos)))
    assert ids(groups) == [[1, 2, 3, 4, 5], [6], [7], [8], [9]]
    assert all(group["center"] is None for group in groups)
    assert groups[0]["captured_from"] == photos[0]["timestamp"]
    assert groups[0]["captured_to"] == photos[4]["timestamp"]


def test_undated_and_invalid_dates_are_individual_and_gps_is_excluded():
    groups = time_review_groups(
        [
            photo(1, None),
            photo(2, "bad date"),
            photo(3, "2026-08-01"),
            photo(4, "2026-08-01T12:00:00", 0, 0),
            photo(5, "2026-08-01T12:00:00", 91, 0),
            photo(6, "2026-08-01T12:01:00", 0, None),
            photo(7, "2026-08-01T12:02:00", float("nan"), 0),
        ]
    )
    assert ids(groups) == [[5, 6, 7], [1], [2], [3]]
    assert all(group["captured_from"] is None for group in groups[1:])


def test_offsets_are_normalized_but_never_guessed_for_naive_times():
    groups = time_review_groups(
        [
            photo(1, "2026-08-01T12:00:00+02:00"),
            photo(2, "2026-08-01T10:05:00Z"),
            photo(3, "2026-08-01T10:01:00"),
            photo(4, "2026-08-01T12:00:00Z"),
        ]
    )
    assert ids(groups) == [[3], [1, 2], [4]]


def test_gap_can_be_tightened_and_equal_times_have_stable_order():
    photos = [photo(3, "2026-08-01T10:20:00"), photo(2, "2026-08-01T10:00:00"), photo(1, "2026-08-01T10:00:00")]
    assert ids(time_review_groups(photos, 15)) == [[1, 2], [3]]
    assert ids(time_review_groups(photos, 30)) == [[1, 2, 3]]


@pytest.mark.parametrize("timestamps, expected", [
    (["2026-08-01T16:50:00-07:00", "2026-08-01T17:10:00-07:00"], [[1, 2]]),
    (["2026-08-01T23:50:00-07:00", "2026-08-02T00:10:00-07:00"], [[1], [2]]),
    (["2026-08-01T06:50:00+07:00", "2026-08-01T07:10:00+07:00"], [[1, 2]]),
])
def test_day_boundaries_use_camera_local_dates(timestamps, expected):
    photos = [photo(index + 1, timestamp) for index, timestamp in enumerate(timestamps)]
    assert ids(time_review_groups(photos)) == expected


def test_time_preview_skips_existing_locations_and_gps(app_and_db):
    app, db = app_and_db
    p1, p2, p3 = db.get_photo_ids()
    db.conn.execute("UPDATE photos SET latitude = 0, longitude = 0 WHERE id = ?", (p1,))
    location_id = db.get_or_create_text_location("Already assigned")
    db.set_photo_location(p2, location_id)
    db.conn.commit()
    response = app.test_client().post("/api/location-review/preview", json={"scope": "all", "mode": "time"})
    assert response.status_code == 200
    data = response.get_json()
    assert ids(data["groups"]) == [[p3]]
    assert {p["reason"] for p in data["skipped"]} == {"already_has_location", "has_coordinates"}
    assert data["reviewable"] == 1
    assert db.conn.execute("SELECT latitude FROM photos WHERE id = ?", (p3,)).fetchone()[0] is None


def test_all_photo_scope_respects_active_workspace(app_and_db):
    app, db = app_and_db
    db.create_workspace("Empty workspace")
    workspace = db.conn.execute("SELECT id FROM workspaces WHERE name = 'Empty workspace'").fetchone()[0]
    client = app.test_client()
    assert client.post(f"/api/workspaces/{workspace}/activate").status_code == 200
    response = client.post("/api/location-review/preview", json={"scope": "all", "mode": "time"})
    assert response.status_code == 200
    assert response.get_json()["groups"] == []


@pytest.mark.parametrize(
    "options", [{"mode": "invalid"}, {"gap_minutes": True}, {"gap_minutes": 0}, {"gap_minutes": "60"}]
)
def test_time_preview_rejects_invalid_options(app_and_db, options):
    app, _ = app_and_db
    response = app.test_client().post("/api/location-review/preview", json={"scope": "all", **options})
    assert response.status_code == 400


@pytest.mark.parametrize("source", [{"photo_ids": []}, {"photo_ids": [1]}, {"collection_id": 1}])
def test_all_photo_scope_cannot_expand_an_explicit_selection(app_and_db, source):
    app, _ = app_and_db
    response = app.test_client().post(
        "/api/location-review/preview", json={"scope": "all", "mode": "time", **source}
    )
    assert response.status_code == 400


@pytest.fixture
def discrepancy_catalog(app_and_db, tmp_path):
    import config as cfg

    app, db = app_and_db
    config = cfg.load()
    config["write_assigned_location_to_xmp"] = True
    cfg.save(config)
    photo_ids = db.get_photo_ids()
    folder = tmp_path / 'photos'
    folder.mkdir()
    keyword = db.get_or_create_text_location('Kumeyaay Lake')
    db.conn.execute('UPDATE keywords SET latitude=?, longitude=? WHERE id=?', (32.841515, -117.032998, keyword))
    for pid in photo_ids:
        db.set_photo_location(pid, keyword)
        db.conn.execute('UPDATE photos SET latitude=?, longitude=? WHERE id=?', (32.8360733333333, -117.029203333333, pid))
    folder_id = db.conn.execute('SELECT MIN(id) FROM folders').fetchone()[0]
    db.conn.execute('UPDATE folders SET path=? WHERE id=?', (str(folder), folder_id))
    db.conn.execute('UPDATE photos SET folder_id=?', (folder_id,))
    db.conn.commit()
    return app.test_client(), db, photo_ids, keyword, folder


def discrepancy_preview(client, **kwargs):
    response = client.post('/api/location-review/preview', json={
        'scope': 'all', 'mode': 'discrepancies', **kwargs,
    })
    assert response.status_code == 200, response.get_json()
    return [photo for group in response.get_json()['groups'] for photo in group['photos']]


def resolve(client, photos, action):
    return client.post('/api/location-review/resolve-discrepancies', json={
        'photo_ids': [p['id'] for p in photos], 'action': action,
        'fingerprints': {str(p['id']): p['fingerprint'] for p in photos},
    })


def test_discrepancy_preview_uses_real_distance_and_is_read_only(discrepancy_catalog):
    client, db, photo_ids, _, folder = discrepancy_catalog
    photos = discrepancy_preview(client)
    assert {p['id'] for p in photos} == set(photo_ids)
    assert all(p['distance_m'] == pytest.approx(701.294, abs=.01) for p in photos)
    assert photos[0]['assigned_location']['name'] == 'Kumeyaay Lake'
    assert discrepancy_preview(client, minimum_distance_m=702) == []
    assert db.get_pending_changes() == []
    assert list(folder.iterdir()) == []


def test_keep_is_remembered_but_coordinate_changes_require_review(discrepancy_catalog):
    client, db, photo_ids, keyword, _ = discrepancy_catalog
    photos = discrepancy_preview(client)
    assert resolve(client, photos[:1], 'keep').get_json() == {'reviewed': 1, 'queued': 0}
    assert len(discrepancy_preview(client)) == 2
    assert len(discrepancy_preview(client, include_reviewed=True)) == 3
    assert db.get_pending_changes() == []
    assert len(db.get_edit_history()) == 1
    assert db.undo_last_edit() is None
    db.conn.execute('UPDATE keywords SET latitude=32.85 WHERE id=?', (keyword,))
    db.conn.commit()
    assert len(discrepancy_preview(client)) == len(photo_ids)


def test_explicit_reapply_reaches_sidecar_and_preserves_original(discrepancy_catalog):
    from sync import sync_to_xmp
    from xmp import read_sync_preview_metadata

    client, db, _, _, folder = discrepancy_catalog
    photos = discrepancy_preview(client)
    original = folder / photos[0]['filename']
    original.write_bytes(b'original photo bytes')
    response = resolve(client, photos[:1], 'assigned')
    assert response.get_json() == {'reviewed': 1, 'queued': 1}
    # Queueing is not success on disk, and a retry is idempotent.
    assert len(discrepancy_preview(client)) == 3
    assert resolve(client, photos[:1], 'assigned').status_code == 200
    assert len(db.get_pending_changes()) == 1
    assert sync_to_xmp(db)['synced'] == 1
    metadata = read_sync_preview_metadata(original.with_suffix('.xmp'))
    assert metadata['location']['latitude'] == pytest.approx(32.841515)
    assert metadata['location']['longitude'] == pytest.approx(-117.032998)
    assert metadata['location_source'] == 'keyword'
    assert original.read_bytes() == b'original photo bytes'
    assert db.conn.execute('SELECT latitude FROM photos WHERE id=?', (photos[0]['id'],)).fetchone()[0] == photos[0]['latitude']
    assert len(discrepancy_preview(client)) == 2
    # Removing a correction reopens the discrepancy, even after a successful sync.
    original.with_suffix('.xmp').unlink()
    assert len(discrepancy_preview(client)) == 3


def test_existing_sidecar_gps_is_backed_up(discrepancy_catalog):
    from sync import sync_to_xmp
    from xmp import read_sync_preview_metadata

    client, db, _, _, folder = discrepancy_catalog
    photo = discrepancy_preview(client)[0]
    sidecar = (folder / photo['filename']).with_suffix('.xmp')
    sidecar.write_text('''<x:xmpmeta xmlns:x="adobe:ns:meta/"
        xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#" xmlns:exif="http://ns.adobe.com/exif/1.0/">
        <rdf:RDF><rdf:Description exif:GPSLatitude="32,50.1644N" exif:GPSLongitude="117,1.7522W" /></rdf:RDF>
        </x:xmpmeta>''')
    before = read_sync_preview_metadata(sidecar)['location']
    photos = discrepancy_preview(client)
    assert resolve(client, photos, 'assigned').status_code == 200
    assert sync_to_xmp(db)['failed'] == 0
    assert read_sync_preview_metadata(sidecar)['previous_location'] == before


def test_failed_sync_does_not_hide_discrepancy(discrepancy_catalog, monkeypatch):
    import sync

    client, db, _, _, _ = discrepancy_catalog
    photos = discrepancy_preview(client)
    assert resolve(client, photos, 'assigned').status_code == 200

    def fail(*args, **kwargs):
        raise OSError('Disk unavailable')

    monkeypatch.setattr(sync, '_write_photo_sync', fail)
    assert sync.sync_to_xmp(db)['failed'] == 3
    assert len(discrepancy_preview(client)) == 3


def test_stale_batch_is_rejected_without_partial_changes(discrepancy_catalog):
    client, db, photo_ids, _, _ = discrepancy_catalog
    photos = discrepancy_preview(client)
    db.conn.execute('UPDATE photos SET longitude=-117.04 WHERE id=?', (photo_ids[-1],))
    db.conn.commit()
    assert resolve(client, photos, 'assigned').status_code == 409
    assert db.get_pending_changes() == []
    assert resolve(client, photos, 'keep').status_code == 409
    assert db.conn.execute('SELECT COUNT(*) FROM location_gps_reviews').fetchone()[0] == 0


def test_settings_and_pending_changes_are_respected(discrepancy_catalog):
    import config as cfg

    client, db, _, _, _ = discrepancy_catalog
    photos = discrepancy_preview(client)
    config = cfg.load()
    config['write_assigned_location_to_xmp'] = False
    cfg.save(config)
    assert resolve(client, photos, 'assigned').status_code == 409
    assert db.get_pending_changes() == []
    db.queue_change(photos[0]['id'], 'location', 'effective')
    assert resolve(client, photos, 'keep').status_code == 409
    assert db.conn.execute('SELECT COUNT(*) FROM location_gps_reviews').fetchone()[0] == 0


def test_discrepancy_scope_cannot_cross_workspaces(discrepancy_catalog):
    client, db, photo_ids, _, _ = discrepancy_catalog
    photos = discrepancy_preview(client)
    other_ws = db.create_workspace('Another workspace')
    assert client.post(f'/api/workspaces/{other_ws}/activate').status_code == 200
    assert discrepancy_preview(client) == []
    assert resolve(client, photos, 'assigned').status_code == 403
    response = client.post('/api/location-review/preview', json={
        'photo_ids': photo_ids, 'mode': 'discrepancies',
    })
    assert response.status_code == 403


@pytest.mark.parametrize('value', [True, -1, '500', None, 30000000])
def test_discrepancy_distance_validation(app_and_db, value):
    app, _ = app_and_db
    assert app.test_client().post('/api/location-review/preview', json={
        'scope': 'all', 'mode': 'discrepancies', 'minimum_distance_m': value,
    }).status_code == 400


def test_invalid_or_absent_coordinates_are_excluded(discrepancy_catalog):
    client, db, photo_ids, keyword, _ = discrepancy_catalog
    db.conn.execute('UPDATE photos SET latitude=NULL WHERE id=?', (photo_ids[0],))
    db.conn.execute('UPDATE photos SET longitude=181 WHERE id=?', (photo_ids[1],))
    db.conn.commit()
    assert [p['id'] for p in discrepancy_preview(client)] == [photo_ids[2]]
    db.conn.execute('UPDATE keywords SET longitude=NULL WHERE id=?', (keyword,))
    db.conn.commit()
    assert discrepancy_preview(client) == []


def test_distance_across_antimeridian():
    from location_review import distance_meters

    assert distance_meters({'latitude': 0, 'longitude': 179.999}, {'latitude': 0, 'longitude': -179.999}) == pytest.approx(222.39, abs=.01)


def test_discrepancy_groups_bound_large_days_and_separate_places():
    from location_review import discrepancy_groups

    photos = [photo(i, '2026-09-12T10:31:33', 32, -117) | {
        'distance_m': 701, 'assigned_location': {'keyword_id': 1},
    } for i in range(205)]
    photos[-1]['assigned_location'] = {'keyword_id': 2}
    groups = discrepancy_groups(photos)
    assert [g['count'] for g in groups] == [100, 100, 4, 1]
    assert sorted(pid for g in groups for pid in g['photo_ids']) == list(range(205))


def test_sidecar_change_invalidates_preview(discrepancy_catalog):
    from xmp import write_gps_location

    client, db, _, _, folder = discrepancy_catalog
    photos = discrepancy_preview(client)
    sidecar = (folder / photos[0]['filename']).with_suffix('.xmp')
    write_gps_location(sidecar, 33, -117)
    assert resolve(client, photos, 'assigned').status_code == 409
    assert db.get_pending_changes() == []
