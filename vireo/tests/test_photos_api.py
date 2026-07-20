import json
import sys
import types

import numpy as np
import pytest


def test_api_photos_default(app_and_db):
    """GET /api/photos returns all photos."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get('/api/photos')
    assert resp.status_code == 200
    data = resp.get_json()
    assert len(data['photos']) == 3
    assert 'total' in data


def test_api_photos_includes_edit_recipe(app_and_db):
    """GET /api/photos exposes edit recipes so card overlays can align."""
    app, db = app_and_db
    photo = db.get_photos()[0]
    db.set_photo_edit_recipe(photo["id"], {"rotation": 90})

    client = app.test_client()
    resp = client.get('/api/photos')
    assert resp.status_code == 200
    data = resp.get_json()
    listed = {p["id"]: p for p in data["photos"]}
    assert listed[photo["id"]]["edit_recipe"] == {"version": 1, "rotation": 90}


def test_api_photos_marks_species_representative(app_and_db):
    """List payloads expose representative state for card views and menus."""
    app, db = app_and_db
    photo = db.get_photos()[0]
    kid = db.add_keyword("American Robin", is_species=True)
    db.tag_photo(photo["id"], kid)
    db.set_species_representative("American Robin", photo["id"])

    client = app.test_client()
    resp = client.get("/api/photos")
    assert resp.status_code == 200
    listed = {p["id"]: p for p in resp.get_json()["photos"]}

    assert listed[photo["id"]]["is_species_representative"] is True
    assert listed[photo["id"]]["life_list"] == [
        {
            "species": "American Robin",
            "is_current_photo": True,
            "is_species_representative": True,
        }
    ]


def test_api_photos_pagination(app_and_db):
    """GET /api/photos supports pagination."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get('/api/photos?per_page=2&page=1')
    data = resp.get_json()
    assert len(data['photos']) == 2

    resp = client.get('/api/photos?per_page=2&page=2')
    data = resp.get_json()
    assert len(data['photos']) == 1


def test_api_photos_reports_and_filters_coordinate_sources(app_and_db):
    """Browse separates embedded GPS, assigned map coordinates, and neither."""
    app, db = app_and_db
    photos = db.get_photos(sort="name")
    exif_id, assigned_id, none_id = [photo["id"] for photo in photos]
    with db.conn:
        db.conn.execute(
            "UPDATE photos SET latitude = 37.7749, longitude = -122.4194 "
            "WHERE id = ?",
            (exif_id,),
        )
        cursor = db.conn.execute(
            "INSERT INTO keywords (name, type, latitude, longitude) "
            "VALUES ('Assigned Park', 'location', 40.7829, -73.9654)"
        )
        db.conn.execute(
            "INSERT INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
            (assigned_id, cursor.lastrowid),
        )

    client = app.test_client()
    response = client.get("/api/photos?sort=name")
    assert response.status_code == 200
    statuses = {
        photo["id"]: photo["location_status"]
        for photo in response.get_json()["photos"]
    }
    assert statuses == {
        exif_id: "exif",
        assigned_id: "assigned",
        none_id: "none",
    }

    for status, expected_id in (
        ("exif", exif_id),
        ("assigned", assigned_id),
        ("none", none_id),
    ):
        filtered = client.get(f"/api/photos?location_status={status}&sort=name")
        assert filtered.status_code == 200
        body = filtered.get_json()
        assert body["total"] == 1
        assert [photo["id"] for photo in body["photos"]] == [expected_id]

        ids = client.get(f"/api/photos/ids?location_status={status}&sort=name")
        assert ids.status_code == 200
        assert ids.get_json()["photo_ids"] == [expected_id]

    assert client.get("/api/photos?location_status=gpsish").status_code == 400


def test_api_photos_filter_folder(app_and_db):
    """GET /api/photos?folder_id= filters by folder."""
    app, db = app_and_db
    folders = db.get_folder_tree()
    jan = [f for f in folders if f['name'] == 'January'][0]

    client = app.test_client()
    resp = client.get(f'/api/photos?folder_id={jan["id"]}')
    data = resp.get_json()
    assert len(data['photos']) == 1
    assert data['photos'][0]['filename'] == 'bird2.jpg'


def test_api_photo_ids_matches_browse_filters(app_and_db):
    """GET /api/photos/ids returns every ID matching the current Browse filters."""
    app, db = app_and_db
    folders = db.get_folder_tree()
    jan = [f for f in folders if f['name'] == 'January'][0]
    expected = [p["id"] for p in db.get_photos(folder_id=jan["id"], sort="name")]

    client = app.test_client()
    resp = client.get(f'/api/photos/ids?folder_id={jan["id"]}&sort=name')
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["photo_ids"] == expected
    assert data["total"] == len(expected)


def test_dashboard_and_browse_share_collection_date_scope(app_and_db):
    """Dashboard drill-down parameters preserve collection/date intersections."""
    app, db = app_and_db
    photos = db.get_photos(sort="name")
    collection_id = db.add_collection(
        "Two dates",
        json.dumps([{
            "field": "photo_ids",
            "value": [photos[0]["id"], photos[2]["id"]],
        }]),
    )
    query = (
        f"collection_id={collection_id}"
        "&date_from=2024-06-01&date_to=2024-06-30&sort=name"
    )
    client = app.test_client()

    stats = client.get(f"/api/stats?{query}")
    assert stats.status_code == 200
    assert stats.get_json()["total_photos"] == 1

    coverage = client.get(f"/api/coverage?{query}")
    assert coverage.status_code == 200
    assert coverage.get_json()["overall"]["total"] == 1

    for path in ("/api/browse/init", "/api/photos", "/api/photos/ids"):
        response = client.get(f"{path}?{query}")
        assert response.status_code == 200
        body = response.get_json()
        result_ids = body.get("photo_ids") or [photo["id"] for photo in body["photos"]]
        assert result_ids == [photos[2]["id"]]


def test_dashboard_options_flags_degraded_collections(app_and_db):
    """Collections whose rules can't compile are flagged so the scope
    picker can disable them instead of 400ing /api/stats and /api/coverage."""
    app, db = app_and_db
    healthy = db.add_collection("Healthy", "[]")
    bad_json = db.add_collection("Broken JSON", "{not json")
    bad_rules = db.add_collection(
        "Unresolvable rules",
        json.dumps([{"field": "no_such_field", "op": "equals", "value": 1}]),
    )

    client = app.test_client()
    resp = client.get("/api/dashboard/options")
    assert resp.status_code == 200
    by_id = {c["id"]: c for c in resp.get_json()["collections"]}
    assert by_id[healthy]["degraded"] is False
    assert by_id[bad_json]["degraded"] is True
    assert by_id[bad_rules]["degraded"] is True

    for cid in (bad_json, bad_rules):
        assert client.get(f"/api/stats?collection_id={cid}").status_code == 400
        assert client.get(f"/api/coverage?collection_id={cid}").status_code == 400


def test_dashboard_scope_rejects_foreign_collection(app_and_db):
    """Scope ids cannot cross workspace boundaries."""
    app, db = app_and_db
    other_workspace = db.create_workspace("Other")
    db.set_active_workspace(other_workspace)
    foreign_collection = db.add_collection("Foreign", "[]")
    db.set_active_workspace(db.get_workspaces()[0]["id"])

    client = app.test_client()
    for path in ("/api/stats", "/api/coverage", "/api/photos", "/api/photos/ids"):
        response = client.get(f"{path}?collection_id={foreign_collection}")
        assert response.status_code == 400


def test_api_photos_keyword_whole_word_option(app_and_db):
    """Browse keyword whole-word option excludes embedded token matches."""
    app, db = app_and_db
    photos = db.get_photos(sort="name")
    western_id = photos[0]["id"]
    tern_id = photos[1]["id"]
    db.tag_photo(western_id, db.add_keyword("Western Gull"))
    db.tag_photo(tern_id, db.add_keyword("Common Tern"))

    client = app.test_client()
    resp = client.get("/api/photos?keyword=tern&sort=name")
    assert resp.status_code == 200
    names = {p["filename"] for p in resp.get_json()["photos"]}
    assert photos[0]["filename"] in names
    assert photos[1]["filename"] in names

    resp = client.get("/api/photos?keyword=tern&keyword_whole_word=1&sort=name")
    assert resp.status_code == 200
    data = resp.get_json()
    assert [p["filename"] for p in data["photos"]] == [photos[1]["filename"]]
    assert data["total"] == 1

    resp = client.get("/api/photos/ids?keyword=tern&keyword_whole_word=1&sort=name")
    assert resp.status_code == 200
    assert resp.get_json()["photo_ids"] == [tern_id]


def test_api_photo_search_ids_only_returns_all_matches(app_and_db, monkeypatch):
    """GET /api/photos/search?ids_only=1 returns all CLIP match IDs, not the page."""
    app, db = app_and_db
    photos = db.get_photos(sort="name")
    by_name = {p["filename"]: p["id"] for p in photos}
    model_name = "test-clip"

    db.upsert_photo_embedding(
        by_name["bird1.jpg"], model_name, np.array([0.95, 0.0], dtype=np.float32).tobytes()
    )
    db.upsert_photo_embedding(
        by_name["bird2.jpg"], model_name, np.array([0.8, 0.0], dtype=np.float32).tobytes()
    )
    db.upsert_photo_embedding(
        by_name["bird3.jpg"], model_name, np.array([0.1, 0.0], dtype=np.float32).tobytes()
    )

    import models
    monkeypatch.setattr(models, "get_active_model", lambda: {
        "name": model_name,
        "model_type": "bioclip",
        "model_str": "fake",
        "weights_path": "",
    })
    monkeypatch.setitem(
        sys.modules,
        "text_encoder",
        types.SimpleNamespace(
            encode_text=lambda *_args, **_kwargs: np.array([1.0, 0.0], dtype=np.float32)
        ),
    )

    client = app.test_client()
    resp = client.get('/api/photos/search?q=bird&threshold=0.15&limit=1&ids_only=1')
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["photo_ids"] == [by_name["bird1.jpg"], by_name["bird2.jpg"]]
    assert data["total_matches"] == 2
    assert "results" not in data


def test_api_photos_filter_rating(app_and_db):
    """GET /api/photos?rating_min= filters by minimum rating."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get('/api/photos?rating_min=4')
    data = resp.get_json()
    assert len(data['photos']) == 1
    assert data['photos'][0]['filename'] == 'bird3.jpg'


def test_api_photos_filter_date_range(app_and_db):
    """GET /api/photos?date_from=&date_to= filters by date range."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get('/api/photos?date_from=2024-01-01&date_to=2024-02-01')
    data = resp.get_json()
    assert len(data['photos']) == 2


def test_api_photos_filter_single_day(app_and_db):
    """GET /api/photos with date_to including time captures all photos on that day."""
    app, _ = app_and_db
    client = app.test_client()
    # Without time suffix, a bare date like 2024-01-15 would miss timestamps
    # like 2024-01-15T10:00:00 because string comparison puts it after the date
    resp = client.get('/api/photos?date_from=2024-01-15&date_to=2024-01-15T23:59:59')
    data = resp.get_json()
    assert len(data['photos']) == 1
    assert data['photos'][0]['filename'] == 'bird1.jpg'


def test_api_photos_includes_all_detections(app_and_db):
    """GET /api/photos attaches a `detections` list with every box, not just primary."""
    app, db = app_and_db
    photos = db.get_photos()
    target = [p for p in photos if p['filename'] == 'bird1.jpg'][0]
    db.save_detections(target['id'], [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.2, "h": 0.2}, "confidence": 0.7, "category": "animal"},
        {"box": {"x": 0.5, "y": 0.5, "w": 0.2, "h": 0.2}, "confidence": 0.95, "category": "animal"},
    ], detector_model="MDV6")

    client = app.test_client()
    resp = client.get('/api/photos')
    data = resp.get_json()

    bird1 = [p for p in data['photos'] if p['filename'] == 'bird1.jpg'][0]
    assert 'detections' in bird1
    assert len(bird1['detections']) == 2
    assert bird1['detections'][0]['confidence'] == 0.95
    assert bird1['detections'][0]['x'] == 0.5
    assert bird1['detections'][1]['confidence'] == 0.7
    assert bird1['detections'][0]['category'] == 'animal'

    # Photos without detections get an empty list, not a missing key
    bird3 = [p for p in data['photos'] if p['filename'] == 'bird3.jpg'][0]
    assert bird3['detections'] == []


def test_api_photos_detections_honor_workspace_threshold(app_and_db):
    """Lowering the workspace's `detector_confidence` surfaces more boxes at
    read time, without rewriting any detection rows.

    Exercises the global-detections design: boxes are cached once, each
    workspace filters on its own threshold when reading.
    """
    app, db = app_and_db
    photos = db.get_photos()
    target = [p for p in photos if p['filename'] == 'bird1.jpg'][0]

    # Save two boxes: one above the default 0.2 threshold, one below it.
    db.save_detections(target['id'], [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.2, "h": 0.2}, "confidence": 0.05, "category": "animal"},
        {"box": {"x": 0.5, "y": 0.5, "w": 0.2, "h": 0.2}, "confidence": 0.95, "category": "animal"},
    ], detector_model="MDV6")

    client = app.test_client()

    # Default workspace threshold (0.2) hides the low-confidence box.
    resp = client.get('/api/photos')
    bird1 = [p for p in resp.get_json()['photos'] if p['filename'] == 'bird1.jpg'][0]
    assert len(bird1['detections']) == 1
    assert bird1['detections'][0]['confidence'] == 0.95

    # Lower the workspace threshold via a per-workspace config override —
    # no detection rows are rewritten, only the read-time filter changes.
    db.update_workspace(db._active_workspace_id,
                        config_overrides={"detector_confidence": 0.01})

    resp = client.get('/api/photos')
    bird1 = [p for p in resp.get_json()['photos'] if p['filename'] == 'bird1.jpg'][0]
    assert len(bird1['detections']) == 2, (
        "lowering detector_confidence should surface more cached boxes"
    )
    # Still ordered by confidence DESC.
    assert bird1['detections'][0]['confidence'] == 0.95
    assert bird1['detections'][1]['confidence'] == 0.05

    # And no new rows were written.
    raw = db.conn.execute(
        "SELECT COUNT(*) FROM detections WHERE photo_id = ?", (target['id'],)
    ).fetchone()[0]
    assert raw == 2


def test_api_photos_filter_keyword(app_and_db):
    """GET /api/photos?keyword= filters by keyword."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get('/api/photos?keyword=Cardinal')
    data = resp.get_json()
    assert len(data['photos']) == 1
    assert data['photos'][0]['filename'] == 'bird1.jpg'


def test_api_photos_filter_flag(app_and_db):
    """GET /api/photos?flag= filters to picks or rejects."""
    app, db = app_and_db
    db.conn.execute("UPDATE photos SET flag='flagged' WHERE filename='bird1.jpg'")
    db.conn.execute("UPDATE photos SET flag='rejected' WHERE filename='bird2.jpg'")
    db.conn.commit()

    client = app.test_client()
    picks = client.get('/api/photos?flag=flagged').get_json()
    rejects = client.get('/api/photos?flag=rejected').get_json()

    assert picks['total'] == 1
    assert [p['filename'] for p in picks['photos']] == ['bird1.jpg']
    assert rejects['total'] == 1
    assert [p['filename'] for p in rejects['photos']] == ['bird2.jpg']


def test_api_photos_rejects_unknown_flag_filter(app_and_db):
    """GET /api/photos?flag= validates the flag filter value."""
    app, _ = app_and_db
    client = app.test_client()

    resp = client.get('/api/photos?flag=bogus')

    assert resp.status_code == 400


def _mark_best_batch_quality(db, photo_id, sharpness):
    db.conn.execute(
        """UPDATE photos
           SET mask_path = 'mask.png',
               subject_tenengrad = ?,
               bg_tenengrad = 5,
               crop_complete = 1,
               bg_separation = 0,
               subject_clip_high = 0,
               subject_clip_low = 0,
               subject_y_median = 115,
               subject_size = 0.05,
               noise_estimate = 1
           WHERE id = ?""",
        (sharpness, photo_id),
    )


def test_api_photo_best_batch_ranks_filename_sequence(app_and_db):
    """GET /api/photos/<id>/best-batch finds adjacent camera frames and ranks them."""
    app, db = app_and_db
    folder_id = db.conn.execute(
        "SELECT id FROM folders WHERE path = ?", ('/photos/2024',)
    ).fetchone()["id"]
    pids = []
    for idx, sharp in enumerate([10, 80, 30], start=3069):
        pid = db.add_photo(
            folder_id=folder_id,
            filename=f"DSC_{idx}.jpg",
            extension=".jpg",
            file_size=1000 + idx,
            file_mtime=float(idx),
            timestamp=f"2024-03-01T12:00:0{idx - 3069}",
        )
        _mark_best_batch_quality(db, pid, sharp)
        pids.append(pid)
    db.conn.commit()

    client = app.test_client()
    resp = client.get(f"/api/photos/{pids[1]}/best-batch")

    assert resp.status_code == 200
    data = resp.get_json()
    assert data["scope_method"] == "filename_sequence"
    assert data["photo_ids"] == pids
    assert data["best_photo_id"] == pids[1]
    assert data["best_filename"] == "DSC_3070.jpg"
    assert data["sequence_range"] == [3069, 3071]
    assert data["cards"][0]["role"] == "best"

    selected = client.post("/api/photos/best-batch", json={"photo_ids": pids}).get_json()
    assert selected["scope_method"] == "selected_photos"
    assert selected["best_photo_id"] == pids[1]


def test_api_photo_best_batch_capture_time_requires_real_timestamps(app_and_db):
    """Timestamp fallback should not group unrelated null-timestamp photos."""
    app, db = app_and_db
    folder_id = db.conn.execute(
        "SELECT id FROM folders WHERE path = ?", ('/photos/2024',)
    ).fetchone()["id"]
    pids = []
    for filename in ["alpha.jpg", "beta.jpg", "gamma.jpg"]:
        pid = db.add_photo(
            folder_id=folder_id,
            filename=filename,
            extension=".jpg",
            file_size=1000,
            file_mtime=1.0,
            timestamp=None,
        )
        _mark_best_batch_quality(db, pid, 80)
        pids.append(pid)
    db.conn.commit()

    client = app.test_client()
    resp = client.get(f"/api/photos/{pids[1]}/best-batch")

    assert resp.status_code == 404
    assert resp.get_json()["error"] == "No neighboring batch photos found"


def test_api_photo_best_batch_ranks_capture_time_without_sequence(app_and_db):
    """Timestamp fallback still works for non-sequence filenames with real times."""
    app, db = app_and_db
    folder_id = db.conn.execute(
        "SELECT id FROM folders WHERE path = ?", ('/photos/2024',)
    ).fetchone()["id"]
    pids = []
    for idx, (filename, sharp) in enumerate(
        [("alpha.jpg", 10), ("beta.jpg", 90), ("gamma.jpg", 30)]
    ):
        pid = db.add_photo(
            folder_id=folder_id,
            filename=filename,
            extension=".jpg",
            file_size=1000,
            file_mtime=1.0,
            timestamp=f"2024-03-01T12:00:0{idx * 3}",
        )
        _mark_best_batch_quality(db, pid, sharp)
        pids.append(pid)
    db.conn.commit()

    client = app.test_client()
    resp = client.get(f"/api/photos/{pids[1]}/best-batch")

    assert resp.status_code == 200, resp.get_json()
    data = resp.get_json()
    assert data["scope_method"] == "capture_time"
    assert data["photo_ids"] == pids
    assert data["best_photo_id"] == pids[1]


def test_api_best_batch_flags_records_single_undoable_edit(app_and_db):
    """Best Batch apply updates mixed flags as one atomic undo action."""
    app, db = app_and_db
    photos = db.get_photos()
    photo_ids = [p["id"] for p in photos]
    best_id, reject_id, keep_reject_id = photo_ids
    db.update_photo_flag(reject_id, "flagged")

    client = app.test_client()
    pre_history = db.get_edit_history()
    resp = client.post(
        "/api/batch/best-batch-flags",
        json={
            "best_photo_id": best_id,
            "reject_photo_ids": [reject_id, keep_reject_id],
        },
    )

    assert resp.status_code == 200, resp.get_json()
    assert resp.get_json()["updated"] == 3
    flags = {
        row["id"]: row["flag"]
        for row in db.conn.execute(
            "SELECT id, flag FROM photos WHERE id IN (?, ?, ?)",
            (best_id, reject_id, keep_reject_id),
        )
    }
    assert flags == {
        best_id: "flagged",
        reject_id: "rejected",
        keep_reject_id: "rejected",
    }
    post_history = db.get_edit_history()
    assert len(post_history) == len(pre_history) + 1
    assert post_history[0]["action_type"] == "flag"
    assert post_history[0]["new_value"] == "best_batch_apply"
    assert post_history[0]["item_count"] == 3

    undo = client.post("/api/undo")

    assert undo.status_code == 200, undo.get_json()
    flags = {
        row["id"]: row["flag"]
        for row in db.conn.execute(
            "SELECT id, flag FROM photos WHERE id IN (?, ?, ?)",
            (best_id, reject_id, keep_reject_id),
        )
    }
    assert flags == {
        best_id: "none",
        reject_id: "flagged",
        keep_reject_id: "none",
    }


def test_api_best_batch_flags_rejects_best_photo_in_rejects(app_and_db):
    app, db = app_and_db
    best_id = db.get_photos()[0]["id"]
    client = app.test_client()

    resp = client.post(
        "/api/batch/best-batch-flags",
        json={"best_photo_id": best_id, "reject_photo_ids": [best_id]},
    )

    assert resp.status_code == 400


def test_api_set_flag_queues_xmp_when_enabled(app_and_db):
    """POST /api/photos/<id>/flag queues a flag sync when configured."""
    import config as cfg

    app, db = app_and_db
    config = cfg.load()
    config["sync_flags_to_xmp"] = True
    cfg.save(config)

    target = [p for p in db.get_photos() if p["filename"] == "bird1.jpg"][0]

    client = app.test_client()
    resp = client.post(f'/api/photos/{target["id"]}/flag', json={"flag": "flagged"})

    assert resp.status_code == 200
    changes = db.get_pending_changes()
    assert len(changes) == 1
    assert changes[0]["change_type"] == "flag"
    assert changes[0]["value"] == "flagged"


def test_api_photo_detail(app_and_db):
    """GET /api/photos/<id> returns photo with keywords."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pid = photos[0]['id']

    resp = client.get(f'/api/photos/{pid}')
    assert resp.status_code == 200
    data = resp.get_json()
    assert data['filename'] == 'bird1.jpg'
    assert 'keywords' in data
    assert data['full_uses_original'] is False


def test_api_photo_detail_reports_full_resolution_preview_mode(app_and_db):
    """Photo detail tells the lightbox when /full already serves /original."""
    app, db = app_and_db
    db.update_workspace(
        db._active_workspace_id,
        config_overrides={"preview_max_size": 0},
    )
    pid = db.get_photos()[0]['id']

    resp = app.test_client().get(f'/api/photos/{pid}')

    assert resp.status_code == 200
    assert resp.get_json()['full_uses_original'] is True


def test_api_photo_detail_includes_on_disk_path(app_and_db):
    """GET /api/photos/<id> returns a `path` field equal to folder_path + '/' + filename.

    The browse-grid right-click "Copy Path" action depends on this field being
    present in the photo detail response. PHOTO_DETAIL_COLS intentionally does
    not store the full on-disk path in the photos table, so the route handler
    must compute it by joining the owning folder's path with the photo's
    filename (same idiom as /api/files/reveal).
    """
    import os as _os

    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    target = [p for p in photos if p['filename'] == 'bird1.jpg'][0]
    folder_row = db.conn.execute(
        "SELECT path FROM folders WHERE id = ?", (target['folder_id'],)
    ).fetchone()
    expected_path = _os.path.join(folder_row['path'], target['filename'])

    resp = client.get(f"/api/photos/{target['id']}")
    assert resp.status_code == 200
    data = resp.get_json()
    assert 'path' in data, "photo detail should expose full on-disk path"
    assert data['path'] == expected_path


def test_photo_detail_life_list_lists_eligible_species(app_and_db):
    """GET /api/photos/<id> exposes a life_list block naming the photo's
    lifelist-eligible species so the shared lightbox / browse menu can offer
    a representative action without page-local data."""
    app, db = app_and_db
    client = app.test_client()
    pid = db.get_photos()[0]["id"]
    kid = db.add_keyword("American Robin", is_species=True)
    db.tag_photo(pid, kid)

    data = client.get(f"/api/photos/{pid}").get_json()
    assert data["life_list"] == [
        {
            "species": "American Robin",
            "is_current_photo": False,
            "is_species_representative": False,
        }
    ]


def test_photo_detail_life_list_marks_current_representative(app_and_db):
    """is_current_photo is true once this photo is the species' representative."""
    app, db = app_and_db
    client = app.test_client()
    pid = db.get_photos()[0]["id"]
    kid = db.add_keyword("American Robin", is_species=True)
    db.tag_photo(pid, kid)
    db.set_photo_preference("life_list", "American Robin", pid)

    data = client.get(f"/api/photos/{pid}").get_json()
    assert data["life_list"] == [
        {
            "species": "American Robin",
            "is_current_photo": True,
            "is_species_representative": True,
        }
    ]


def test_photo_detail_life_list_current_false_when_other_photo_is_rep(app_and_db):
    """A species can be on the list with a different representative photo; this
    photo then reports is_current_photo false (button, not selected star)."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    p1, p2 = photos[0]["id"], photos[1]["id"]
    kid = db.add_keyword("American Robin", is_species=True)
    db.tag_photo(p1, kid)
    db.tag_photo(p2, kid)
    db.set_photo_preference("life_list", "American Robin", p2)

    data = client.get(f"/api/photos/{p1}").get_json()
    assert data["life_list"] == [
        {
            "species": "American Robin",
            "is_current_photo": False,
            "is_species_representative": False,
        }
    ]


def test_photo_detail_life_list_marks_only_primary_of_multiple_reps(app_and_db):
    """Secondary reps report is_current_photo=False so the lightbox lifelist
    panel renders "Set Representative" (re-selecting promotes them) and the
    shared context menu doesn't disable the item for a secondary rep."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    p1, p2 = photos[0]["id"], photos[1]["id"]
    kid = db.add_keyword("American Robin", is_species=True)
    db.tag_photo(p1, kid)
    db.tag_photo(p2, kid)
    # p1 selected first, then p2 — get_species_representative_lists is
    # newest-first, so p2 is the primary (index 0) and p1 is secondary.
    db.set_species_representative("American Robin", p1)
    db.set_species_representative("American Robin", p2)

    primary = client.get(f"/api/photos/{p2}").get_json()
    secondary = client.get(f"/api/photos/{p1}").get_json()

    assert primary["life_list"] == [
        {
            "species": "American Robin",
            "is_current_photo": True,
            "is_species_representative": True,
        }
    ]
    assert secondary["life_list"] == [
        {
            "species": "American Robin",
            "is_current_photo": False,
            "is_species_representative": False,
        }
    ]


def test_photo_detail_life_list_uses_primary_eligible_rep(app_and_db):
    """If the newest representative is ineligible, the older eligible rep is
    current so detail menus do not offer a redundant representative action."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    p1, p2 = photos[0]["id"], photos[1]["id"]
    kid = db.add_keyword("American Robin", is_species=True)
    db.tag_photo(p1, kid)
    db.tag_photo(p2, kid)
    db.set_species_representative("American Robin", p1)
    db.set_species_representative("American Robin", p2)
    db.update_photo_flag(p2, "rejected")

    data = client.get(f"/api/photos/{p1}").get_json()

    assert data["life_list"] == [
        {
            "species": "American Robin",
            "is_current_photo": True,
            "is_species_representative": True,
        }
    ]


def test_photo_detail_life_list_excludes_non_species_keyword(app_and_db):
    """Plain (non-species) keywords never produce a lifelist entry."""
    app, db = app_and_db
    client = app.test_client()
    pid = db.get_photos()[0]["id"]
    kid = db.add_keyword("Central Park", kw_type="location")
    db.tag_photo(pid, kid)

    data = client.get(f"/api/photos/{pid}").get_json()
    assert data["life_list"] == []


def test_photo_detail_life_list_includes_taxonomy_type_keyword(app_and_db):
    """type='taxonomy' keywords count even when is_species is 0 — same rule as
    get_life_list_candidates, so the button and the list can't drift."""
    app, db = app_and_db
    client = app.test_client()
    pid = db.get_photos()[0]["id"]
    db.conn.execute(
        "INSERT INTO keywords (name, type, is_species) VALUES ('Bald Eagle', 'taxonomy', 0)"
    )
    kid = db.conn.execute(
        "SELECT id FROM keywords WHERE name='Bald Eagle'"
    ).fetchone()["id"]
    db.tag_photo(pid, kid)
    db.conn.commit()

    data = client.get(f"/api/photos/{pid}").get_json()
    assert data["life_list"] == [
        {
            "species": "Bald Eagle",
            "is_current_photo": False,
            "is_species_representative": False,
        }
    ]


def test_photo_detail_life_list_multiple_species_independent_state(app_and_db):
    """A photo with two species gets two entries, each with its own state."""
    app, db = app_and_db
    client = app.test_client()
    pid = db.get_photos()[0]["id"]
    db.tag_photo(pid, db.add_keyword("American Robin", is_species=True))
    db.tag_photo(pid, db.add_keyword("Blue Jay", is_species=True))
    db.set_photo_preference("life_list", "Blue Jay", pid)

    data = client.get(f"/api/photos/{pid}").get_json()
    by_species = {e["species"]: e["is_current_photo"] for e in data["life_list"]}
    assert by_species == {"American Robin": False, "Blue Jay": True}


def test_photo_detail_life_list_empty_for_rejected_photo(app_and_db):
    """Rejected photos are excluded from the life list, so their block is empty
    — matches _photo_can_be_life_list_preference, the POST-time backstop."""
    app, db = app_and_db
    client = app.test_client()
    pid = db.get_photos()[0]["id"]
    kid = db.add_keyword("American Robin", is_species=True)
    db.tag_photo(pid, kid)
    db.update_photo_flag(pid, "rejected")

    data = client.get(f"/api/photos/{pid}").get_json()
    assert data["life_list"] == []


def test_api_photos_by_ids_preserves_selection_order(app_and_db):
    """POST /api/photos/by-ids returns active-workspace photos in caller order."""
    app, db = app_and_db
    photos = db.get_photos(sort="name")
    by_name = {p["filename"]: p["id"] for p in photos}
    ordered_ids = [by_name["bird3.jpg"], by_name["bird1.jpg"], by_name["bird3.jpg"]]

    client = app.test_client()
    resp = client.post("/api/photos/by-ids", json={"photo_ids": ordered_ids})

    assert resp.status_code == 200
    data = resp.get_json()
    assert [p["filename"] for p in data["photos"]] == ["bird3.jpg", "bird1.jpg"]
    assert all("detections" in p for p in data["photos"])


def test_api_photos_by_ids_validates_payload(app_and_db):
    app, _ = app_and_db
    client = app.test_client()

    resp = client.post("/api/photos/by-ids", json={"photo_ids": ["1"]})

    assert resp.status_code == 400


def test_pipeline_selection_results_uses_full_review_payload(app_and_db):
    """Browse-selected review should return the same rich result shape as Pipeline Review."""
    app, db = app_and_db
    photos = db.get_photos(sort="name")
    by_name = {p["filename"]: p["id"] for p in photos}
    ordered_ids = [by_name["bird3.jpg"], by_name["bird1.jpg"]]

    for idx, pid in enumerate(ordered_ids):
        db.update_photo_pipeline_features(
            pid,
            mask_path=f"/masks/{pid}.png",
            subject_tenengrad=250 + idx * 25,
            bg_tenengrad=30,
            crop_complete=0.9,
            bg_separation=40.0,
            subject_clip_high=0.01,
            subject_clip_low=0.01,
            subject_y_median=120.0,
            phash_crop=f"{pid:016x}",
        )
        db.update_photo_quality(pid, subject_size=0.1)
        det_id = db.save_detections(
            pid,
            [{"box": {"x": 0.2, "y": 0.2, "w": 0.4, "h": 0.4}, "confidence": 0.9}],
            detector_model="megadetector-v6",
        )[0]
        db.add_prediction(
            det_id,
            "Cedar Waxwing" if idx == 0 else "American Robin",
            0.92 - idx * 0.05,
            "bioclip",
        )
    db.set_photo_edit_recipe(ordered_ids[0], {"rotation": 90})

    client = app.test_client()
    resp = client.post("/api/pipeline/selection-results", json={"photo_ids": ordered_ids})

    assert resp.status_code == 200
    data = resp.get_json()
    assert data["source"] == "browse-selection"
    assert [p["id"] for p in data["photos"]] == ordered_ids
    assert data["summary"]["total_photos"] == 2
    assert len(data["encounters"]) == 1
    enc = data["encounters"][0]
    assert enc["photo_ids"] == ordered_ids
    assert enc["bursts"][0]["photo_ids"] == ordered_ids
    assert {p["species"] for p in enc["species_predictions"]} == {
        "Cedar Waxwing",
        "American Robin",
    }
    assert enc["bursts"][0]["species_predictions"] == enc["species_predictions"]
    assert all("quality_composite" in p for p in data["photos"])
    recipes = {p["id"]: p.get("edit_recipe") for p in data["photos"]}
    assert recipes[ordered_ids[0]] == {"version": 1, "rotation": 90}
    assert recipes[ordered_ids[1]] is None


def test_api_photos_calendar(app_and_db):
    """GET /api/photos/calendar returns daily photo counts for a year."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get("/api/photos/calendar?year=2024")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["year"] == 2024
    assert "2024-01-15" in data["days"]
    assert data["min_year"] == 2024
    assert data["max_year"] == 2024


def test_api_photos_calendar_with_filters(app_and_db):
    """GET /api/photos/calendar respects folder_id and rating_min filters."""
    app, db = app_and_db
    client = app.test_client()
    resp = client.get("/api/photos/calendar?year=2024&rating_min=4")
    data = resp.get_json()
    assert list(data["days"].keys()) == ["2024-06-10"]


def test_api_photos_calendar_default_year(app_and_db):
    """GET /api/photos/calendar defaults to current year."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get("/api/photos/calendar")
    assert resp.status_code == 200
    data = resp.get_json()
    assert "year" in data
    assert "days" in data


def test_thumbnail_serving(app_and_db):
    """GET /thumbnails/<id>.jpg serves thumbnail from cache."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pid = photos[0]['id']

    resp = client.get(f'/thumbnails/{pid}.jpg')
    assert resp.status_code == 200
    assert resp.content_type in ('image/jpeg', 'image/jpg')


def test_api_photos_geo_returns_geolocated(app_and_db):
    """GET /api/photos/geo returns only geolocated photos."""
    app, db = app_and_db
    # Set GPS on bird1 only
    db.conn.execute("UPDATE photos SET latitude=37.77, longitude=-122.42 WHERE filename='bird1.jpg'")
    db.conn.commit()

    client = app.test_client()
    resp = client.get('/api/photos/geo')
    assert resp.status_code == 200
    data = resp.get_json()
    assert 'photos' in data
    assert 'total_filtered' in data
    assert 'total_with_gps' in data
    assert 'total_geolocated' in data
    assert 'total_without_coordinates' in data
    assert 'total_photos' in data
    assert data['total_filtered'] == 1
    assert data['total_with_gps'] == 1
    assert data['total_geolocated'] == 1
    assert data['total_without_coordinates'] == 2
    assert data['total_photos'] == 3
    assert len(data['photos']) == 1
    assert data['photos'][0]['latitude'] == 37.77
    assert data['photos'][0]['longitude'] == -122.42


def test_api_photos_geo_with_filters(app_and_db):
    """GET /api/photos/geo passes through rating filter."""
    app, db = app_and_db
    # Set GPS on bird1 (rating 3) and bird3 (rating 5)
    db.conn.execute("UPDATE photos SET latitude=1.0, longitude=2.0 WHERE filename IN ('bird1.jpg','bird3.jpg')")
    db.conn.commit()

    client = app.test_client()
    resp = client.get('/api/photos/geo?rating_min=4')
    assert resp.status_code == 200
    data = resp.get_json()
    assert len(data['photos']) == 1
    assert data['photos'][0]['filename'] == 'bird3.jpg'


def test_api_photos_geo_empty(app_and_db):
    """GET /api/photos/geo returns empty list when no geolocated photos."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get('/api/photos/geo')
    assert resp.status_code == 200
    data = resp.get_json()
    assert data['photos'] == []
    assert data['total_filtered'] == 0


def test_api_photos_geo_includes_gps_stats(app_and_db):
    """GET /api/photos/geo response includes consistent global GPS stats."""
    app, db = app_and_db
    db.conn.execute("UPDATE photos SET latitude=37.77, longitude=-122.42 WHERE filename='bird1.jpg'")
    db.conn.commit()

    client = app.test_client()
    resp = client.get('/api/photos/geo')
    data = resp.get_json()
    assert data['total_with_gps'] == 1
    assert data['total_without_gps'] == 2
    assert data['total_photos'] == 3
    # Verify global stats stay consistent even with filters active
    resp2 = client.get('/api/photos/geo?rating_min=5')
    data2 = resp2.get_json()
    assert data2['total_filtered'] == 0  # no rated-5 geo photos
    assert data2['total_with_gps'] == 1  # global count unchanged
    assert data2['total_without_gps'] == 2  # global count unchanged


def test_api_photos_geo_species_filter(app_and_db):
    """GET /api/photos/geo?species= filters by species."""
    app, db = app_and_db
    db.conn.execute("UPDATE photos SET latitude=1.0, longitude=2.0 WHERE filename IN ('bird1.jpg','bird3.jpg')")
    db.conn.commit()
    # Add detections then predictions
    det1 = db.save_detections(1, [{"box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.4}, "confidence": 0.9, "category": "animal"}], detector_model="MDV6")
    det3 = db.save_detections(3, [{"box": {"x": 0.2, "y": 0.2, "w": 0.3, "h": 0.4}, "confidence": 0.8, "category": "animal"}], detector_model="MDV6")
    db.add_prediction(det1[0], 'Cardinal', 0.9, 'bioclip')
    db.add_prediction(det3[0], 'Sparrow', 0.8, 'bioclip')
    preds = db.get_predictions(photo_ids=[1, 3])
    for pr in preds:
        db.accept_prediction(pr['id'])

    client = app.test_client()
    resp = client.get('/api/photos/geo?species=Cardinal')
    data = resp.get_json()
    assert len(data['photos']) == 1
    assert data['photos'][0]['species'] == 'Cardinal'


def test_api_species_list(app_and_db):
    """GET /api/species returns accepted species from geolocated photos."""
    app, db = app_and_db
    db.conn.execute("UPDATE photos SET latitude=37.0, longitude=-122.0 WHERE id=1")
    det_ids = db.save_detections(1, [{"box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.4}, "confidence": 0.9, "category": "animal"}], detector_model="MDV6")
    db.add_prediction(det_ids[0], 'Cardinal', 0.9, 'bioclip')
    preds = db.get_predictions(photo_ids=[1])
    db.accept_prediction(preds[0]['id'])

    client = app.test_client()
    resp = client.get('/api/species')
    assert resp.status_code == 200
    data = resp.get_json()
    assert 'species' in data
    assert 'Cardinal' in data['species']


def test_api_species_empty(app_and_db):
    """GET /api/species returns empty list with no accepted predictions."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get('/api/species')
    assert resp.status_code == 200
    data = resp.get_json()
    assert data['species'] == []


def test_api_species_summary_filters_to_latest_fingerprint(app_and_db):
    """GET /api/species/summary must surface only the most recent
    labels_fingerprint per (detection, classifier_model). Stale species
    cached under an old label set must NOT contribute to counts —
    otherwise the variant explorer mixes pre- and post-relabel results.
    """
    app, db = app_and_db
    det_ids = db.save_detections(1, [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.4}, "confidence": 0.9, "category": "animal"},
    ], detector_model="MDV6")
    # Stale fingerprint: species the user used to track.
    db.conn.execute(
        "INSERT INTO predictions (detection_id, classifier_model, "
        "labels_fingerprint, species, confidence, created_at) "
        "VALUES (?, 'bioclip-2', 'fp-old', 'Finch', 0.9, '2026-01-01')",
        (det_ids[0],),
    )
    # Current fingerprint: species under the active label set.
    db.conn.execute(
        "INSERT INTO predictions (detection_id, classifier_model, "
        "labels_fingerprint, species, confidence, created_at) "
        "VALUES (?, 'bioclip-2', 'fp-new', 'Robin', 0.8, '2026-04-24')",
        (det_ids[0],),
    )
    db.conn.commit()

    client = app.test_client()
    resp = client.get('/api/species/summary')
    assert resp.status_code == 200
    species = [r['species'] for r in resp.get_json()]
    assert 'Robin' in species
    assert 'Finch' not in species, (
        "Species summary leaked a stale-fingerprint species into counts "
        "— variant explorer would mix pre- and post-relabel results."
    )


def test_photo_detail_includes_metadata(app_and_db):
    """GET /api/photos/<id> includes parsed metadata when exif_data is populated."""
    app, db = app_and_db
    import json

    # Manually set exif_data on a photo
    test_meta = {"EXIF": {"Make": "TestCam", "Model": "X100"}, "File": {"FileType": "JPEG"}}
    db.conn.execute(
        "UPDATE photos SET exif_data = ? WHERE id = 1",
        (json.dumps(test_meta),),
    )
    db.conn.commit()

    client = app.test_client()
    resp = client.get('/api/photos/1')
    assert resp.status_code == 200
    data = resp.get_json()
    assert "metadata" in data
    assert data["metadata"]["EXIF"]["Make"] == "TestCam"


def test_photo_detail_metadata_null_when_empty(app_and_db):
    """GET /api/photos/<id> returns metadata as null when exif_data is not populated."""
    app, db = app_and_db
    client = app.test_client()
    resp = client.get('/api/photos/1')
    assert resp.status_code == 200
    data = resp.get_json()
    assert data.get("metadata") is None


def test_add_keyword_with_type_override(app_and_db):
    """POST /api/photos/<id>/keywords with type param sets keyword type in DB."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pid = photos[0]['id']

    resp = client.post(f'/api/photos/{pid}/keywords',
                       json={"name": "Tim", "type": "individual"})
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["ok"] is True
    kid = data["keyword_id"]

    # Verify the keyword type is "individual" in the database
    row = db.conn.execute("SELECT type FROM keywords WHERE id = ?", (kid,)).fetchone()
    assert row is not None
    assert row["type"] == "individual"


def test_add_keyword_noop_when_already_tagged(app_and_db):
    """POST /api/photos/<id>/keywords must be a no-op when the photo already
    carries the keyword. Otherwise the route queues a keyword_add pending
    change and records a keyword_add edit whose undo calls untag_photo —
    so a repeated/stale Add click would remove the pre-existing tag on
    undo. Mirrors the batch route's already_tagged precheck.
    """
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pid = photos[0]['id']

    # First add: real work — one pending change, one edit.
    resp = client.post(f'/api/photos/{pid}/keywords', json={"name": "Robin"})
    assert resp.status_code == 200
    kid = resp.get_json()["keyword_id"]

    def counts():
        n_pending = db.conn.execute(
            "SELECT COUNT(*) AS n FROM pending_changes "
            "WHERE change_type = 'keyword_add' AND photo_id = ? AND value = ?",
            (pid, "Robin"),
        ).fetchone()["n"]
        n_edits = db.conn.execute(
            "SELECT COUNT(*) AS n FROM edit_history "
            "WHERE action_type = 'keyword_add' AND new_value = ?",
            (str(kid),),
        ).fetchone()["n"]
        return n_pending, n_edits

    first_pending, first_edits = counts()

    # Repeat the same add. Must not queue another pending change or record
    # another edit — the response still succeeds so the client sees no
    # error.
    resp = client.post(f'/api/photos/{pid}/keywords', json={"name": "Robin"})
    assert resp.status_code == 200
    assert resp.get_json()["ok"] is True
    assert resp.get_json()["keyword_id"] == kid

    assert counts() == (first_pending, first_edits), (
        "no-op add must not queue a new pending change or record a new edit"
    )

    # The photo still carries exactly one row for this keyword — a stale
    # undo of the (non-)second click can't remove the tag because no new
    # edit was recorded.
    n_tags = db.conn.execute(
        "SELECT COUNT(*) AS n FROM photo_keywords "
        "WHERE photo_id = ? AND keyword_id = ?",
        (pid, kid),
    ).fetchone()["n"]
    assert n_tags == 1


def test_batch_keyword_with_type_override(app_and_db):
    """POST /api/batch/keyword with type param sets keyword type in DB."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    photo_ids = [p['id'] for p in photos]

    resp = client.post('/api/batch/keyword',
                       json={"photo_ids": photo_ids, "name": "Central Park", "type": "location"})
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["ok"] is True
    assert data["updated"] == len(photo_ids)

    # Verify the keyword type is "location" in the database
    row = db.conn.execute(
        "SELECT type FROM keywords WHERE name = 'Central Park'").fetchone()
    assert row is not None
    assert row["type"] == "location"


# --- Working copy integration tests for serving endpoints ---

import os


def test_preview_uses_working_copy(app_and_db):
    """Preview endpoint loads from working copy instead of original."""
    app, db = app_and_db
    client = app.test_client()

    photos = db.get_photos()
    pid = photos[0]["id"]

    # Set a working copy path on the photo
    from PIL import Image
    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    working_dir = os.path.join(vireo_dir, "working")
    os.makedirs(working_dir, exist_ok=True)
    wc_path = os.path.join(working_dir, f"{pid}.jpg")
    Image.new("RGB", (4096, 2731), color=(0, 255, 0)).save(wc_path, "JPEG")
    db.conn.execute(
        "UPDATE photos SET working_copy_path=? WHERE id=?",
        (f"working/{pid}.jpg", pid),
    )
    db.conn.commit()

    # Clear preview cache
    preview_dir = os.path.join(vireo_dir, "previews")
    cache_file = os.path.join(preview_dir, f"{pid}.jpg")
    if os.path.exists(cache_file):
        os.remove(cache_file)

    resp = client.get(f"/photos/{pid}/full")
    assert resp.status_code == 200


def test_unedited_raw_preview_uses_camera_rendered_source_not_working_copy(
    client_with_photo, monkeypatch,
):
    """Sharper lightbox tiers must not swap to the dark RAW edit source."""
    import io
    import os

    import image_loader
    from PIL import Image

    app, db, photo_id = client_with_photo
    folder = db.conn.execute(
        "SELECT f.path FROM photos p JOIN folders f ON f.id=p.folder_id "
        "WHERE p.id=?",
        (photo_id,),
    ).fetchone()
    raw_path = os.path.join(folder["path"], "source.NEF")
    with open(raw_path, "wb") as raw_file:
        raw_file.write(b"raw bytes decoded by the test double")

    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    working_dir = os.path.join(vireo_dir, "working")
    os.makedirs(working_dir, exist_ok=True)
    working_path = os.path.join(working_dir, f"{photo_id}.jpg")
    Image.new("RGB", (800, 600), (25, 25, 25)).save(
        working_path, "JPEG",
    )
    db.conn.execute(
        """UPDATE photos
           SET filename='source.NEF', extension='.nef',
               working_copy_path=?, width=800, height=600
           WHERE id=?""",
        (f"working/{photo_id}.jpg", photo_id),
    )
    db.conn.commit()

    loaded = []

    def camera_rendered_load(path, max_size=1024, **kwargs):
        loaded.append((os.fspath(path), kwargs))
        color = (220, 220, 220) if os.fspath(path) == raw_path else (25, 25, 25)
        return Image.new("RGB", (800, 600), color)

    monkeypatch.setattr(image_loader, "load_image", camera_rendered_load)

    response = app.test_client().get(
        f"/photos/{photo_id}/preview?size=2560"
    )

    assert response.status_code == 200
    assert loaded == [(raw_path, {})]
    with Image.open(io.BytesIO(response.data)) as rendered:
        assert rendered.getpixel((400, 300))[0] > 200


def test_unedited_raw_preview_falls_back_when_source_extraction_marked_failed(
    client_with_photo, monkeypatch,
):
    """RAW+JPEG pairs with a source-failure marker keep the working-copy
    fallback instead of 500ing on the sharper preview tier."""
    import io
    import os

    import image_loader
    from PIL import Image

    app, db, photo_id = client_with_photo
    folder = db.conn.execute(
        "SELECT f.path FROM photos p JOIN folders f ON f.id=p.folder_id "
        "WHERE p.id=?",
        (photo_id,),
    ).fetchone()
    raw_path = os.path.join(folder["path"], "source.NEF")
    with open(raw_path, "wb") as raw_file:
        raw_file.write(b"raw bytes that libraw cannot decode")
    companion_path = os.path.join(folder["path"], "source.JPG")
    Image.new("RGB", (800, 600), (220, 220, 220)).save(
        companion_path, "JPEG",
    )

    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    working_dir = os.path.join(vireo_dir, "working")
    os.makedirs(working_dir, exist_ok=True)
    working_path = os.path.join(working_dir, f"{photo_id}.jpg")
    Image.new("RGB", (800, 600), (25, 25, 25)).save(
        working_path, "JPEG",
    )
    mtime = os.path.getmtime(raw_path)
    db.conn.execute(
        """UPDATE photos
           SET filename='source.NEF', extension='.nef',
               companion_path='source.JPG',
               working_copy_path=?, width=800, height=600,
               file_mtime=?,
               working_copy_failed_at=datetime('now'),
               working_copy_failed_mtime=?,
               working_copy_failed_source='source'
           WHERE id=?""",
        (f"working/{photo_id}.jpg", mtime, mtime, photo_id),
    )
    db.conn.commit()

    loaded = []

    def tracking_load(path, max_size=1024, **kwargs):
        loaded.append(os.fspath(path))
        if os.fspath(path) == raw_path:
            raise AssertionError(
                "unedited RAW preview retried a source-failed RAW decode"
            )
        # The working copy is the pre-PR fallback for source-failed RAWs.
        # Return its (dark) pixels so the response is a valid JPEG rather
        # than 500ing before the fallback path can run.
        return Image.new("RGB", (800, 600), (25, 25, 25))

    monkeypatch.setattr(image_loader, "load_image", tracking_load)

    response = app.test_client().get(
        f"/photos/{photo_id}/preview?size=2560"
    )

    assert response.status_code == 200, response.get_data(as_text=True)
    assert raw_path not in loaded
    with Image.open(io.BytesIO(response.data)):
        pass


def test_preview_falls_back_to_original(app_and_db, tmp_path):
    """Preview endpoint falls back to original when no working copy exists."""
    app, db = app_and_db
    client = app.test_client()

    photos = db.get_photos()
    pid = photos[0]["id"]

    # Point folder to a writable tmp location and create a real image
    from PIL import Image
    photo_dir = tmp_path / "photo_files"
    photo_dir.mkdir()
    db.conn.execute(
        "UPDATE folders SET path=? WHERE id=?",
        (str(photo_dir), photos[0]["folder_id"]),
    )
    db.conn.commit()
    img_path = os.path.join(str(photo_dir), photos[0]["filename"])
    Image.new("RGB", (2000, 1500)).save(img_path, "JPEG")

    # Clear preview cache
    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    preview_dir = os.path.join(vireo_dir, "previews")
    cache_file = os.path.join(preview_dir, f"{pid}.jpg")
    if os.path.exists(cache_file):
        os.remove(cache_file)

    resp = client.get(f"/photos/{pid}/full")
    assert resp.status_code == 200


def test_preview_sized_caches_per_size(app_and_db):
    """Preview endpoint caches each requested size separately."""
    app, db = app_and_db
    client = app.test_client()

    photos = db.get_photos()
    pid = photos[0]["id"]

    from PIL import Image
    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    working_dir = os.path.join(vireo_dir, "working")
    os.makedirs(working_dir, exist_ok=True)
    wc_path = os.path.join(working_dir, f"{pid}.jpg")
    Image.new("RGB", (4096, 2731), color=(0, 255, 0)).save(wc_path, "JPEG")
    db.conn.execute(
        "UPDATE photos SET working_copy_path=? WHERE id=?",
        (f"working/{pid}.jpg", pid),
    )
    db.conn.commit()

    resp = client.get(f"/photos/{pid}/preview?size=1920")
    assert resp.status_code == 200
    resp = client.get(f"/photos/{pid}/preview?size=2560")
    assert resp.status_code == 200

    preview_dir = os.path.join(vireo_dir, "previews")
    assert os.path.exists(os.path.join(preview_dir, f"{pid}_1920.jpg"))
    assert os.path.exists(os.path.join(preview_dir, f"{pid}_2560.jpg"))

    # The 2560 variant should actually be larger on disk than the 1920 variant
    size_1920 = os.path.getsize(os.path.join(preview_dir, f"{pid}_1920.jpg"))
    size_2560 = os.path.getsize(os.path.join(preview_dir, f"{pid}_2560.jpg"))
    assert size_2560 > size_1920


def test_preview_returns_404_for_deleted_photo_even_with_stale_cache(app_and_db):
    """Defense against SQLite id reuse: don't serve a cached image for a row
    that no longer exists.
    """
    app, db = app_and_db
    client = app.test_client()
    pid = db.get_photos()[0]["id"]

    from PIL import Image
    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    preview_dir = os.path.join(vireo_dir, "previews")
    os.makedirs(preview_dir, exist_ok=True)

    # Delete the photo (cascades FK-dependent rows) then simulate a leftover
    # cache file, e.g. crash-after-commit-before-cleanup.
    db.delete_photos([pid])
    stale = os.path.join(preview_dir, f"{pid}_1920.jpg")
    Image.new("RGB", (10, 10)).save(stale, "JPEG")

    resp = client.get(f"/photos/{pid}/preview?size=1920")
    assert resp.status_code == 404


def test_preview_rejects_unsupported_size(app_and_db):
    """Preview endpoint rejects sizes outside the allowlist to prevent cache-bombing."""
    app, db = app_and_db
    client = app.test_client()

    pid = db.get_photos()[0]["id"]
    resp = client.get(f"/photos/{pid}/preview?size=9999")
    assert resp.status_code == 400
    resp = client.get(f"/photos/{pid}/preview?size=abc")
    assert resp.status_code == 400


def test_original_serves_full_res_working_copy(app_and_db):
    """Original endpoint serves working copy directly when it is full-res."""
    app, db = app_and_db
    client = app.test_client()

    photos = db.get_photos()
    pid = photos[0]["id"]

    # Set photo dimensions to match working copy
    db.conn.execute("UPDATE photos SET width=800, height=600 WHERE id=?", (pid,))
    db.conn.commit()

    from PIL import Image
    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    working_dir = os.path.join(vireo_dir, "working")
    os.makedirs(working_dir, exist_ok=True)
    wc_path = os.path.join(working_dir, f"{pid}.jpg")
    # Working copy is 800x600 which matches original dimensions
    Image.new("RGB", (800, 600)).save(wc_path, "JPEG")
    db.conn.execute(
        "UPDATE photos SET working_copy_path=? WHERE id=?",
        (f"working/{pid}.jpg", pid),
    )
    db.conn.commit()

    resp = client.get(f"/photos/{pid}/original")
    assert resp.status_code == 200


def test_unedited_raw_original_uses_camera_display_cache_not_working_copy(
    app_and_db, monkeypatch, tmp_path,
):
    """Opening an unedited RAW must not swap to the dark edit working copy."""
    import io

    import image_loader
    from image_loader import RAW_DECODE_CAMERA_RENDERED
    from PIL import Image

    app, db = app_and_db
    client = app.test_client()
    photo = db.get_photos()[0]
    photo_id = photo["id"]

    source_dir = tmp_path / "raw-source"
    source_dir.mkdir()
    source_path = source_dir / "source.NEF"
    source_path.write_bytes(b"fake raw")
    db.conn.execute(
        "UPDATE folders SET path=? WHERE id=?",
        (str(source_dir), photo["folder_id"]),
    )

    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    working_dir = os.path.join(vireo_dir, "working")
    os.makedirs(working_dir, exist_ok=True)
    working_path = os.path.join(working_dir, f"{photo_id}.jpg")
    Image.new("RGB", (800, 600), (30, 30, 30)).save(working_path, "JPEG")
    working_bytes = open(working_path, "rb").read()
    db.conn.execute(
        """UPDATE photos
           SET filename='source.NEF', extension='.nef', width=800, height=600,
               working_copy_path=?
           WHERE id=?""",
        (f"working/{photo_id}.jpg", photo_id),
    )
    db.conn.commit()

    calls = []
    replacements = []
    import app as app_module
    real_replace = app_module.os.replace

    def camera_extract(source, output, **kwargs):
        calls.append((os.fspath(source), os.fspath(output), kwargs))
        Image.new("RGB", (800, 600), (220, 220, 220)).save(output, "JPEG")
        return True

    def tracking_replace(source, destination):
        replacements.append((os.fspath(source), os.fspath(destination)))
        return real_replace(source, destination)

    monkeypatch.setattr(image_loader, "extract_working_copy", camera_extract)
    monkeypatch.setattr(app_module.os, "replace", tracking_replace)

    first = client.get(f"/photos/{photo_id}/original")
    second = client.get(f"/photos/{photo_id}/original")

    assert first.status_code == 200
    assert second.status_code == 200
    assert len(calls) == 1, "second request should reuse the display cache"
    called_source, called_output, called_kwargs = calls[0]
    assert called_source == str(source_path)
    assert called_output.endswith(".jpg.tmp")
    assert called_kwargs["raw_decode"] == RAW_DECODE_CAMERA_RENDERED
    display_path = os.path.join(
        vireo_dir, "originals", f"{photo_id}.display.jpg",
    )
    assert replacements == [(called_output, display_path)]
    assert not os.path.exists(called_output)
    with Image.open(io.BytesIO(first.data)) as rendered:
        assert rendered.getpixel((0, 0))[0] > 200
    assert open(working_path, "rb").read() == working_bytes
    assert db.get_photo(photo_id)["working_copy_path"] == f"working/{photo_id}.jpg"


def test_original_trusts_raw_working_copy_even_when_smaller_than_stored_dims(
    app_and_db, monkeypatch,
):
    """Original endpoint trusts RAW working copy when sensor dims slightly exceed wc.

    Stored RAW sensor dimensions can legitimately exceed embedded-JPEG-derived
    working copy dimensions (Nikon NEFs that fall back to the embedded JPEG
    are a known case: sensor 8280×5520 vs embedded JPEG 8256×5504). The
    endpoint must serve the working copy directly without re-extracting,
    because re-extraction would just produce the same embedded JPEG —
    just slower — and burst-review zoom would loop on every request.
    """
    import config as cfg
    import image_loader

    app, db = app_and_db
    client = app.test_client()

    # Full-res working copies (NEF case implies the user has lifted the cap;
    # otherwise the embedded JPEG of 8256 would have been thumbnailed to 4096).
    cfg.save({**cfg.DEFAULTS, "working_copy_max_size": 0})

    photos = db.get_photos()
    pid = photos[0]["id"]

    # Mark this photo as a RAW source so the embedded-JPEG-fallback
    # tolerance applies. Plain JPEGs do NOT get the tolerance — for them
    # any wc smaller than the original means the cap downsized it.
    db.conn.execute(
        "UPDATE photos SET filename=?, extension=?, width=8280, height=5520 WHERE id=?",
        ("DSC_0001.NEF", ".nef", pid),
    )
    db.conn.commit()

    # Working copy is slightly smaller (e.g. embedded-JPEG fallback).
    from PIL import Image
    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    working_dir = os.path.join(vireo_dir, "working")
    os.makedirs(working_dir, exist_ok=True)
    wc_path = os.path.join(working_dir, f"{pid}.jpg")
    Image.new("RGB", (8256, 5504), color=(123, 45, 67)).save(wc_path, "JPEG")
    db.conn.execute(
        "UPDATE photos SET working_copy_path=? WHERE id=?",
        (f"working/{pid}.jpg", pid),
    )
    db.conn.commit()

    def fail_missing_raw_retry(*args, **kwargs):
        raise AssertionError("missing RAW source should use the trusted working copy")

    monkeypatch.setattr(image_loader, "extract_working_copy", fail_missing_raw_retry)
    monkeypatch.setattr(image_loader, "load_image", fail_missing_raw_retry)

    resp = client.get(f"/photos/{pid}/original")
    assert resp.status_code == 200

    # Body must be exactly the working copy bytes — no re-extraction.
    with open(wc_path, "rb") as f:
        assert resp.data == f.read()


def test_original_does_not_trust_raw_working_copy_with_truncated_short_edge(app_and_db):
    """A RAW working copy whose short edge is substantially truncated must NOT
    be served as full-res.

    A failed libraw decode can leave an embedded JPEG whose long edge matches
    the sensor but whose short edge is significantly smaller (e.g. 6000x3376
    for a 6000x4000 source). A long-edge-only tolerance accepted that WC and
    served it as the full-resolution original, silently dropping the missing
    short-edge content. The trust check must reject it on the short edge so
    the endpoint tries to recover the true full-res instead.
    """
    import config as cfg

    app, db = app_and_db
    client = app.test_client()

    cfg.save({**cfg.DEFAULTS, "working_copy_max_size": 0})

    photos = db.get_photos()
    pid = photos[0]["id"]

    db.conn.execute(
        "UPDATE photos SET filename=?, extension=?, width=6000, height=4000 WHERE id=?",
        ("DSC_0001.NEF", ".nef", pid),
    )
    db.conn.commit()

    from PIL import Image
    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    working_dir = os.path.join(vireo_dir, "working")
    os.makedirs(working_dir, exist_ok=True)
    wc_path = os.path.join(working_dir, f"{pid}.jpg")
    # Long edge matches sensor (6000) but short edge is ~15% short — the
    # kind of truncated embedded JPEG rawpy hands back when demosaic fails.
    Image.new("RGB", (6000, 3376), color=(200, 100, 50)).save(wc_path, "JPEG")
    db.conn.execute(
        "UPDATE photos SET working_copy_path=? WHERE id=?",
        (f"working/{pid}.jpg", pid),
    )
    db.conn.commit()

    resp = client.get(f"/photos/{pid}/original")

    # Whatever the endpoint decides to do downstream (re-extract, 500, serve
    # a companion), it must not have returned the truncated WC as-is.
    with open(wc_path, "rb") as f:
        truncated_bytes = f.read()
    if resp.status_code == 200:
        assert resp.data != truncated_bytes, (
            "endpoint trusted a short-edge-truncated RAW working copy as full-res"
        )


def test_original_trusts_portrait_raw_working_copy_with_transposed_dims(app_and_db):
    """Portrait RAWs store sensor axes while extract_working_copy writes the
    EXIF-transposed JPEG, so the trust check must compare in display-orientation
    space.

    For a portrait RAW with sensor 6000x4000 and EXIF Orientation 6, the
    working copy lands as 4000x6000 on disk. Comparing raw sensor axes
    (``wc_w >= orig_w`` where wc_w=4000 and orig_w=6000) rejects a legitimate
    full-resolution WC and forces a redundant re-extract or a 500. The trust
    check must normalize both sides to display orientation.
    """
    import json

    import config as cfg

    app, db = app_and_db
    client = app.test_client()

    cfg.save({**cfg.DEFAULTS, "working_copy_max_size": 0})

    photos = db.get_photos()
    pid = photos[0]["id"]

    exif = json.dumps({"EXIF": {"Orientation": 6}})
    db.conn.execute(
        "UPDATE photos SET filename=?, extension=?, width=6000, height=4000, "
        "exif_data=? WHERE id=?",
        ("DSC_0001.NEF", ".nef", exif, pid),
    )
    db.conn.commit()

    from PIL import Image
    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    working_dir = os.path.join(vireo_dir, "working")
    os.makedirs(working_dir, exist_ok=True)
    wc_path = os.path.join(working_dir, f"{pid}.jpg")
    # extract_working_copy writes the EXIF-transposed JPEG: for a portrait
    # source with sensor 6000x4000 the WC on disk is 4000x6000 (display).
    Image.new("RGB", (4000, 6000), color=(90, 130, 200)).save(wc_path, "JPEG")
    db.conn.execute(
        "UPDATE photos SET working_copy_path=? WHERE id=?",
        (f"working/{pid}.jpg", pid),
    )
    db.conn.commit()

    resp = client.get(f"/photos/{pid}/original")

    assert resp.status_code == 200
    # The endpoint must have trusted and served the WC directly — comparing
    # sensor vs display would have rejected it and either 500'd or re-extracted.
    with open(wc_path, "rb") as f:
        assert resp.data == f.read()


def test_edited_original_uses_working_copy_after_current_raw_failure(
    client_with_photo, monkeypatch,
):
    """An unsupported edited RAW should render from its JPEG working copy.

    Nikon HE* RAWs can fail libraw demosaic and only leave a near-full embedded
    JPEG working copy. Once that source failure is recorded for the current
    mtime, the edited original route should apply the crop to the JPEG instead
    of returning a permanent 500.
    """
    import image_loader
    from PIL import Image

    app, db, photo_id = client_with_photo
    client = app.test_client()
    folder = db.conn.execute(
        "SELECT f.path FROM photos p JOIN folders f ON f.id=p.folder_id WHERE p.id=?",
        (photo_id,),
    ).fetchone()
    raw_path = os.path.join(folder["path"], "bad.NEF")
    with open(raw_path, "wb") as f:
        f.write(b"unsupported raw")

    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    working_dir = os.path.join(vireo_dir, "working")
    os.makedirs(working_dir, exist_ok=True)
    wc_path = os.path.join(working_dir, f"{photo_id}.jpg")
    Image.new("RGB", (792, 594), color=(80, 120, 160)).save(wc_path, "JPEG")

    mtime = os.path.getmtime(raw_path)
    db.conn.execute(
        """UPDATE photos
           SET filename='bad.NEF', extension='.nef',
               width=800, height=600,
               file_mtime=?,
               working_copy_path=?,
               working_copy_failed_at=datetime('now'),
               working_copy_failed_mtime=?,
               working_copy_failed_source='source'
           WHERE id=?""",
        (mtime, f"working/{photo_id}.jpg", mtime, photo_id),
    )
    db.conn.commit()
    db.set_photo_edit_recipe(
        photo_id,
        {"crop": {"x": 0, "y": 0, "w": 0.5, "h": 1}},
    )

    loaded_paths = []
    original_load_image = image_loader.load_image

    def tracking_load_image(file_path, max_size=1024, **kwargs):
        loaded_paths.append(str(file_path))
        if str(file_path).lower().endswith(".nef"):
            raise AssertionError("edited original retried failed RAW")
        return original_load_image(file_path, max_size=max_size, **kwargs)

    monkeypatch.setattr(image_loader, "load_image", tracking_load_image)

    resp = client.get(f"/photos/{photo_id}/original")

    assert resp.status_code == 200, resp.get_data(as_text=True)
    assert [os.path.normpath(path) for path in loaded_paths] == [
        os.path.normpath(wc_path)
    ]


def test_original_reextracts_stale_capped_jpeg_wc_after_cap_raised(app_and_db, tmp_path):
    """Stale working copies generated under a smaller cap must be re-extracted
    after the cap is raised — the endpoint must not trust the wc just
    because the *current* cap is larger than the original.

    Scenario: an existing 4096-px wc was generated for a 6000-px JPEG when
    ``working_copy_max_size`` was 4096. The user later sets the cap to 0
    (no cap). The next ``/original`` request must produce the full 6000 px,
    not silently serve the stale 4096-px wc.
    """
    import config as cfg

    app, db = app_and_db
    client = app.test_client()

    # User has lifted the cap.
    cfg.save({**cfg.DEFAULTS, "working_copy_max_size": 0})

    photos = db.get_photos()
    pid = photos[0]["id"]

    db.conn.execute("UPDATE photos SET width=6000, height=4000 WHERE id=?", (pid,))
    db.conn.commit()

    # Stale 4096-px wc from an older, smaller cap.
    from PIL import Image
    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    working_dir = os.path.join(vireo_dir, "working")
    os.makedirs(working_dir, exist_ok=True)
    wc_path = os.path.join(working_dir, f"{pid}.jpg")
    Image.new("RGB", (4096, 2731), color=(10, 20, 30)).save(wc_path, "JPEG")
    db.conn.execute(
        "UPDATE photos SET working_copy_path=? WHERE id=?",
        (f"working/{pid}.jpg", pid),
    )
    db.conn.commit()

    # Real 6000×4000 source on disk so the on-demand upgrade can run.
    photo_dir = tmp_path / "photo_files"
    photo_dir.mkdir()
    db.conn.execute(
        "UPDATE folders SET path=? WHERE id=?",
        (str(photo_dir), photos[0]["folder_id"]),
    )
    db.conn.commit()
    img_path = os.path.join(str(photo_dir), photos[0]["filename"])
    Image.new("RGB", (6000, 4000), color=(40, 50, 60)).save(img_path, "JPEG")

    resp = client.get(f"/photos/{pid}/original")
    assert resp.status_code == 200

    # The served image must be full-res, not the stale 4096-px wc.
    import io
    with Image.open(io.BytesIO(resp.data)) as served:
        assert max(served.size) == 6000


def test_original_reextracts_jpeg_just_above_cap(app_and_db, tmp_path):
    """A capped wc whose long side is just slightly below the original must
    not be misclassified as full-res. Specifically: a 4100-px JPEG with the
    default 4096 cap produces a 4096-px wc — that wc is genuinely capped,
    not a near-match, and ``/original`` must re-extract.
    """
    app, db = app_and_db
    client = app.test_client()

    photos = db.get_photos()
    pid = photos[0]["id"]

    # Original is 4 pixels above the default 4096 cap.
    db.conn.execute("UPDATE photos SET width=4100, height=2733 WHERE id=?", (pid,))
    db.conn.commit()

    from PIL import Image
    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    working_dir = os.path.join(vireo_dir, "working")
    os.makedirs(working_dir, exist_ok=True)
    wc_path = os.path.join(working_dir, f"{pid}.jpg")
    # Capped wc at the cap value.
    Image.new("RGB", (4096, 2731), color=(10, 20, 30)).save(wc_path, "JPEG")
    db.conn.execute(
        "UPDATE photos SET working_copy_path=? WHERE id=?",
        (f"working/{pid}.jpg", pid),
    )
    db.conn.commit()

    photo_dir = tmp_path / "photo_files"
    photo_dir.mkdir()
    db.conn.execute(
        "UPDATE folders SET path=? WHERE id=?",
        (str(photo_dir), photos[0]["folder_id"]),
    )
    db.conn.commit()
    img_path = os.path.join(str(photo_dir), photos[0]["filename"])
    Image.new("RGB", (4100, 2733), color=(40, 50, 60)).save(img_path, "JPEG")

    resp = client.get(f"/photos/{pid}/original")
    assert resp.status_code == 200

    # Must serve the true 4100-px original, not the 4096-px capped wc.
    import io
    with Image.open(io.BytesIO(resp.data)) as served:
        assert max(served.size) == 4100


def test_original_upgrades_capped_working_copy_to_full_res(app_and_db, tmp_path):
    """Original endpoint re-extracts when working copy was capped below original.

    With the default ``working_copy_max_size`` (4096), a 6000×4000 JPEG gets a
    4096-px working copy. Serving that directly for /original would break 1:1
    zoom — the endpoint must trigger the on-demand full-res upgrade.
    """
    app, db = app_and_db
    client = app.test_client()

    photos = db.get_photos()
    pid = photos[0]["id"]

    # Stored dimensions exceed the default cap (4096).
    db.conn.execute("UPDATE photos SET width=6000, height=4000 WHERE id=?", (pid,))
    db.conn.commit()

    # Capped working copy: 4096-px long side.
    from PIL import Image
    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    working_dir = os.path.join(vireo_dir, "working")
    os.makedirs(working_dir, exist_ok=True)
    wc_path = os.path.join(working_dir, f"{pid}.jpg")
    Image.new("RGB", (4096, 2731), color=(10, 20, 30)).save(wc_path, "JPEG")
    db.conn.execute(
        "UPDATE photos SET working_copy_path=? WHERE id=?",
        (f"working/{pid}.jpg", pid),
    )
    db.conn.commit()

    # Real 6000×4000 source on disk so the on-demand upgrade can run.
    photo_dir = tmp_path / "photo_files"
    photo_dir.mkdir()
    db.conn.execute(
        "UPDATE folders SET path=? WHERE id=?",
        (str(photo_dir), photos[0]["folder_id"]),
    )
    db.conn.commit()
    img_path = os.path.join(str(photo_dir), photos[0]["filename"])
    Image.new("RGB", (6000, 4000), color=(40, 50, 60)).save(img_path, "JPEG")

    resp = client.get(f"/photos/{pid}/original")
    assert resp.status_code == 200

    # The served image must be full-res (6000×4000), not the capped 4096-px wc.
    import io
    with Image.open(io.BytesIO(resp.data)) as served:
        assert max(served.size) == 6000


def test_original_serves_post_upgrade_working_copy_without_reextracting(app_and_db, tmp_path):
    """After the on-demand upgrade overwrites wc at full-res, subsequent
    requests must serve the upgraded wc directly — not loop into another
    re-extract on every burst-review zoom click.
    """
    app, db = app_and_db
    client = app.test_client()

    photos = db.get_photos()
    pid = photos[0]["id"]

    # Stored dims still exceed the default cap (4096), but the wc on disk
    # has already been upgraded to full-res by an earlier request.
    db.conn.execute("UPDATE photos SET width=6000, height=4000 WHERE id=?", (pid,))
    db.conn.commit()

    from PIL import Image
    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    working_dir = os.path.join(vireo_dir, "working")
    os.makedirs(working_dir, exist_ok=True)
    wc_path = os.path.join(working_dir, f"{pid}.jpg")
    # Upgraded wc: matches stored dims.
    Image.new("RGB", (6000, 4000), color=(70, 80, 90)).save(wc_path, "JPEG")
    db.conn.execute(
        "UPDATE photos SET working_copy_path=? WHERE id=?",
        (f"working/{pid}.jpg", pid),
    )
    db.conn.commit()

    # Source file does NOT exist — so if the endpoint tried to re-extract,
    # the test would fail with a 5xx. Serving the wc directly succeeds.
    photo_dir = tmp_path / "photo_files_missing"
    photo_dir.mkdir()
    db.conn.execute(
        "UPDATE folders SET path=? WHERE id=?",
        (str(photo_dir), photos[0]["folder_id"]),
    )
    db.conn.commit()

    resp = client.get(f"/photos/{pid}/original")
    assert resp.status_code == 200
    with open(wc_path, "rb") as f:
        assert resp.data == f.read()


def test_original_serves_native_jpeg_directly(app_and_db, tmp_path):
    """Original endpoint serves JPEG file directly when no working copy."""
    app, db = app_and_db
    client = app.test_client()

    photos = db.get_photos()
    pid = photos[0]["id"]

    # Point folder to a writable tmp location and create a real JPEG
    from PIL import Image
    photo_dir = tmp_path / "photo_files"
    photo_dir.mkdir()
    db.conn.execute(
        "UPDATE folders SET path=? WHERE id=?",
        (str(photo_dir), photos[0]["folder_id"]),
    )
    db.conn.commit()
    img_path = os.path.join(str(photo_dir), photos[0]["filename"])
    Image.new("RGB", (3000, 2000)).save(img_path, "JPEG")

    # Ensure no working copy is set
    db.conn.execute(
        "UPDATE photos SET working_copy_path=NULL WHERE id=?", (pid,)
    )
    db.conn.commit()

    resp = client.get(f"/photos/{pid}/original")
    assert resp.status_code == 200


def test_crop_preview_uses_working_copy(app_and_db):
    """Crop preview endpoint loads from working copy when available."""
    app, db = app_and_db
    client = app.test_client()

    photos = db.get_photos()
    pid = photos[0]["id"]

    # Create a working copy
    from PIL import Image
    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    working_dir = os.path.join(vireo_dir, "working")
    os.makedirs(working_dir, exist_ok=True)
    wc_path = os.path.join(working_dir, f"{pid}.jpg")
    Image.new("RGB", (4096, 2731), color=(255, 0, 0)).save(wc_path, "JPEG")
    db.conn.execute(
        "UPDATE photos SET working_copy_path=? WHERE id=?",
        (f"working/{pid}.jpg", pid),
    )
    db.conn.commit()

    resp = client.get(f"/photos/{pid}/crop")
    assert resp.status_code == 200
    assert resp.content_type == "image/jpeg"


def test_crop_preview_falls_back_to_original(app_and_db, tmp_path):
    """Crop preview endpoint falls back to original when no working copy."""
    app, db = app_and_db
    client = app.test_client()

    photos = db.get_photos()
    pid = photos[0]["id"]

    # Point folder to a writable tmp location and create a real image
    from PIL import Image
    photo_dir = tmp_path / "photo_files"
    photo_dir.mkdir()
    db.conn.execute(
        "UPDATE folders SET path=? WHERE id=?",
        (str(photo_dir), photos[0]["folder_id"]),
    )
    db.conn.commit()
    img_path = os.path.join(str(photo_dir), photos[0]["filename"])
    Image.new("RGB", (2000, 1500)).save(img_path, "JPEG")

    # Ensure no working copy
    db.conn.execute(
        "UPDATE photos SET working_copy_path=NULL WHERE id=?", (pid,)
    )
    db.conn.commit()

    resp = client.get(f"/photos/{pid}/crop")
    assert resp.status_code == 200
    assert resp.content_type == "image/jpeg"


# ---- Preview cache (LRU) tests ----


def test_preview_cache_miss_creates_row(client_with_photo):
    """First request to a size inserts a preview_cache row."""
    app, db, photo_id = client_with_photo
    client = app.test_client()
    resp = client.get(f"/photos/{photo_id}/preview?size=1920")
    assert resp.status_code == 200
    row = db.preview_cache_get(photo_id, 1920)
    assert row is not None
    assert row["bytes"] > 0


def test_preview_cache_hit_updates_last_access(client_with_photo):
    """Second request touches last_access_at."""
    import time
    app, db, photo_id = client_with_photo
    client = app.test_client()
    client.get(f"/photos/{photo_id}/preview?size=1920")
    row1 = db.preview_cache_get(photo_id, 1920)
    time.sleep(0.05)
    client.get(f"/photos/{photo_id}/preview?size=1920")
    row2 = db.preview_cache_get(photo_id, 1920)
    assert row2["last_access_at"] > row1["last_access_at"]


def test_edit_recipe_api_invalidates_preview_cache_and_renders(client_with_photo):
    import io
    import os

    from PIL import Image

    app, db, photo_id = client_with_photo
    client = app.test_client()

    assert client.get(f"/photos/{photo_id}/preview?size=1920").status_code == 200
    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    preview_path = os.path.join(vireo_dir, "previews", f"{photo_id}_1920.jpg")
    assert os.path.exists(preview_path)
    assert db.preview_cache_get(photo_id, 1920) is not None

    resp = client.put(
        f"/api/photos/{photo_id}/edit-recipe",
        json={"recipe": {"rotation": 90}},
    )
    assert resp.status_code == 200
    assert resp.get_json()["recipe"] == {"version": 1, "rotation": 90}
    assert db.preview_cache_get(photo_id, 1920) is None
    assert not os.path.exists(preview_path)

    rendered = client.get(f"/photos/{photo_id}/preview?size=1920")
    assert rendered.status_code == 200
    with Image.open(io.BytesIO(rendered.data)) as img:
        assert img.size == (600, 800)


def test_edit_recipe_api_queues_xmp_sync(client_with_photo):
    app, db, photo_id = client_with_photo
    client = app.test_client()

    resp = client.put(
        f"/api/photos/{photo_id}/edit-recipe",
        json={"recipe": {"straighten": 2.5}},
    )

    assert resp.status_code == 200
    changes = db.get_pending_changes()
    assert len(changes) == 1
    assert changes[0]["photo_id"] == photo_id
    assert changes[0]["change_type"] == "edit_recipe"
    assert '"straighten":2.5' in changes[0]["value"]


def test_bulk_apply_edit_recipe(app_and_db):
    """POST /api/photos/edit-recipe/apply applies one recipe to many photos."""
    app, db = app_and_db
    client = app.test_client()
    ids = [p["id"] for p in db.get_photos()]

    resp = client.post(
        "/api/photos/edit-recipe/apply",
        json={"recipe": {"rotation": 90}, "photo_ids": ids},
    )
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["count"] == len(ids)
    assert sorted(data["applied"]) == sorted(ids)
    assert data["skipped"] == []
    for pid in ids:
        assert db.get_photo_edit_recipe(pid) == {"version": 1, "rotation": 90}


def test_bulk_apply_records_single_undoable_batch(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    ids = [p["id"] for p in db.get_photos()]

    client.post(
        "/api/photos/edit-recipe/apply",
        json={"recipe": {"rotation": 90}, "photo_ids": ids},
    )
    recipe_entries = [
        h for h in db.get_edit_history() if h["action_type"] == "edit_recipe"
    ]
    assert len(recipe_entries) == 1
    assert recipe_entries[0]["is_batch"] == 1
    assert recipe_entries[0]["item_count"] == len(ids)

    # A single undo reverts every photo in the batch.
    db.undo_last_edit()
    for pid in ids:
        assert db.get_photo_edit_recipe(pid) is None


def test_bulk_apply_queues_xmp_sync_per_photo(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    ids = [p["id"] for p in db.get_photos()]

    client.post(
        "/api/photos/edit-recipe/apply",
        json={"recipe": {"straighten": 2.5}, "photo_ids": ids},
    )
    edit_changes = [
        c for c in db.get_pending_changes() if c["change_type"] == "edit_recipe"
    ]
    assert sorted(c["photo_id"] for c in edit_changes) == sorted(ids)


def test_bulk_apply_skips_unknown_photo(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    ids = [p["id"] for p in db.get_photos()]

    resp = client.post(
        "/api/photos/edit-recipe/apply",
        json={"recipe": {"rotation": 90}, "photo_ids": ids + [999999]},
    )
    assert resp.status_code == 200
    data = resp.get_json()
    assert 999999 in data["skipped"]
    assert sorted(data["applied"]) == sorted(ids)


def test_bulk_apply_validation_errors(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    ids = [p["id"] for p in db.get_photos()]

    # Missing photo_ids.
    assert client.post(
        "/api/photos/edit-recipe/apply", json={"recipe": {"rotation": 90}}
    ).status_code == 400
    # Recipe is not an object.
    assert client.post(
        "/api/photos/edit-recipe/apply", json={"recipe": 5, "photo_ids": ids}
    ).status_code == 400
    # Empty selection.
    assert client.post(
        "/api/photos/edit-recipe/apply",
        json={"recipe": {"rotation": 90}, "photo_ids": []},
    ).status_code == 400


def test_edit_page_routes_render(app_and_db):
    """Both /edit/<id> and the parameter-less /edit render the editor."""
    app, db = app_and_db
    client = app.test_client()
    pid = db.get_photos()[0]["id"]
    assert client.get(f"/edit/{pid}").status_code == 200
    assert client.get("/edit").status_code == 200


def test_edit_preview_renders_uncommitted_recipe_without_storing(client_with_photo):
    import io

    from PIL import Image

    app, db, photo_id = client_with_photo
    client = app.test_client()

    rendered = client.get(
        f"/photos/{photo_id}/edit-preview",
        query_string={
            "size": "1920",
            "recipe": '{"rotation":90,"crop":{"x":0,"y":0,"w":0.5,"h":0.5}}',
        },
    )

    assert rendered.status_code == 200
    assert db.get_photo_edit_recipe(photo_id) is None
    with Image.open(io.BytesIO(rendered.data)) as img:
        assert img.size == (600, 800)


def test_edit_preview_scales_detail_to_native_resolution(client_with_photo):
    """A downscaled edit-preview of a sharpen recipe must shrink the USM
    kernel by the render scale (output px per native px): the served bytes
    should match a scale-aware detail pass, not an as-authored (scale 1.0)
    pass on the small render."""
    import io
    import json as jsonlib

    import numpy as np
    from image_edits import apply_recipe_to_loaded_image
    from image_loader import load_image
    from PIL import Image, ImageFilter

    app, db, photo_id = client_with_photo
    client = app.test_client()

    # Re-draw the 800x600 source as a soft vertical edge so sharpening has
    # something to bite on (the fixture default is a flat color).
    folder = db.conn.execute("SELECT path FROM folders").fetchone()
    src_path = os.path.join(folder["path"], "test.jpg")
    arr = np.zeros((600, 800, 3), dtype=np.uint8)
    arr[:, 400:, :] = 200
    arr[:, :400, :] = 55
    edge = Image.fromarray(arr, "RGB").filter(ImageFilter.GaussianBlur(4.0))
    edge.save(src_path, "JPEG", quality=95)

    recipe = {"adjustments": {"sharpen": 100, "sharpen_radius": 3.0}}
    resp = client.get(
        f"/photos/{photo_id}/edit-preview",
        query_string={"size": "256", "recipe": jsonlib.dumps(recipe)},
    )
    assert resp.status_code == 200
    with Image.open(io.BytesIO(resp.data)) as img:
        served = np.asarray(img.convert("RGB")).astype(np.float32)

    scaled = np.asarray(
        apply_recipe_to_loaded_image(
            load_image(src_path, max_size=256), recipe, native_size=(800, 600)
        )
    ).astype(np.float32)
    unscaled = np.asarray(
        apply_recipe_to_loaded_image(load_image(src_path, max_size=256), recipe)
    ).astype(np.float32)

    dist_scaled = float(np.mean(np.abs(served - scaled)))
    dist_unscaled = float(np.mean(np.abs(served - unscaled)))
    assert dist_scaled < dist_unscaled


def test_edit_preview_detail_scale_matches_saved_cropped_render(
    client_with_photo,
):
    """When the in-progress recipe has both a crop and a detail adjustment,
    the uncropped edit-preview must scale sharpen/NR as the saved (cropped)
    render would — using the recipe's crop for the native long edge, not the
    full uncropped native. Otherwise a tighter crop makes the saved render
    visibly sharper than the preview showed."""
    import io
    import json as jsonlib

    import numpy as np
    from image_edits import apply_recipe_to_loaded_image, detail_render_scale
    from image_loader import load_image
    from PIL import Image, ImageFilter

    app, db, photo_id = client_with_photo
    client = app.test_client()

    # Soft vertical edge so sharpening has something to bite on.
    folder = db.conn.execute("SELECT path FROM folders").fetchone()
    src_path = os.path.join(folder["path"], "test.jpg")
    arr = np.zeros((600, 800, 3), dtype=np.uint8)
    arr[:, 400:, :] = 200
    arr[:, :400, :] = 55
    edge = Image.fromarray(arr, "RGB").filter(ImageFilter.GaussianBlur(4.0))
    edge.save(src_path, "JPEG", quality=95)

    recipe = {
        "crop": {"x": 0.25, "y": 0.25, "w": 0.5, "h": 0.5},
        "adjustments": {"sharpen": 100, "sharpen_radius": 3.0},
    }
    resp = client.get(
        f"/photos/{photo_id}/edit-preview",
        query_string={"size": "256", "recipe": jsonlib.dumps(recipe)},
    )
    assert resp.status_code == 200
    with Image.open(io.BytesIO(resp.data)) as img:
        served = np.asarray(img.convert("RGB")).astype(np.float32)

    # "As-saved" reference: render the uncropped preview but scale detail by
    # what the saved cropped render's scale would be (source is 800 wide, size
    # cap is 256, so both scales bound to size=256).
    display_recipe = dict(recipe)
    display_recipe.pop("crop")
    saved_scale = detail_render_scale((256, 256), (800, 600), recipe)
    matched = np.asarray(
        apply_recipe_to_loaded_image(
            load_image(src_path, max_size=256),
            display_recipe,
            max_size=256,
            native_size=(800, 600),
            detail_scale=saved_scale,
        )
    ).astype(np.float32)
    # "As-uncropped" (buggy) reference: crop stripped from the scale too.
    unmatched = np.asarray(
        apply_recipe_to_loaded_image(
            load_image(src_path, max_size=256),
            display_recipe,
            max_size=256,
            native_size=(800, 600),
        )
    ).astype(np.float32)

    dist_matched = float(np.mean(np.abs(served - matched)))
    dist_unmatched = float(np.mean(np.abs(served - unmatched)))
    # The crop shrinks native_long by ~2x, so the crop-aware detail scale is
    # roughly 2x the crop-stripped one; the two reference renders differ enough
    # that the served bytes should clearly track the crop-aware one.
    assert dist_matched < dist_unmatched


def test_edit_preview_returns_400_for_malformed_crop(client_with_photo):
    from PIL import Image

    app, db, photo_id = client_with_photo
    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    working_dir = os.path.join(vireo_dir, "working")
    os.makedirs(working_dir, exist_ok=True)
    wc_rel = f"working/{photo_id}.jpg"
    Image.new("RGB", (800, 600), (180, 90, 40)).save(
        os.path.join(vireo_dir, wc_rel), "JPEG", quality=85,
    )
    db.conn.execute(
        "UPDATE photos SET working_copy_path=? WHERE id=?",
        (wc_rel, photo_id),
    )
    db.conn.commit()

    rendered = app.test_client().get(
        f"/photos/{photo_id}/edit-preview",
        query_string={
            "size": "1920",
            "recipe": '{"crop":{"x":0}}',
        },
    )

    assert rendered.status_code == 400
    assert "crop must include" in rendered.get_data(as_text=True)


def test_edit_preview_skips_recent_failed_raw_before_decode(
    client_with_photo, monkeypatch,
):
    """The crop editor preview should not retry known-bad RAW decodes."""
    import image_loader

    app, db, photo_id = client_with_photo
    file_mtime = db.conn.execute(
        "SELECT file_mtime FROM photos WHERE id=?", (photo_id,)
    ).fetchone()["file_mtime"]
    db.conn.execute(
        """UPDATE photos
           SET filename='bad.NEF', extension='.nef',
               working_copy_path=NULL,
               working_copy_failed_at=datetime('now'),
               working_copy_failed_mtime=?
           WHERE id=?""",
        (file_mtime, photo_id),
    )
    db.conn.commit()

    called = {"load": False}

    def fail_load_if_called(*_args, **_kwargs):
        called["load"] = True
        raise AssertionError("edit-preview retried failed RAW decode")

    monkeypatch.setattr(image_loader, "load_image", fail_load_if_called)

    resp = app.test_client().get(
        f"/photos/{photo_id}/edit-preview",
        query_string={"recipe": '{"straighten":1.5}'},
    )

    assert resp.status_code == 500
    assert called["load"] is False


def test_edit_preview_analysis_keeps_raw_on_recipe_render_path(
    client_with_photo, monkeypatch,
):
    """``analysis=1`` must not let an empty recipe drop a RAW primary onto
    its legacy JPEG working copy. Auto Tone strips tonal adjustments to
    read neutral pixels; for a RAW with no other geometry the resulting
    recipe is empty, which would normally short-circuit
    ``_recipe_render_source`` to the canonical working copy and produce
    clipped pixels for the highlight/exposure heuristics.
    """
    import app as app_module

    app, db, photo_id = client_with_photo
    db.conn.execute(
        "UPDATE photos SET filename='neutral.NEF', extension='.nef' WHERE id=?",
        (photo_id,),
    )
    db.conn.commit()

    seen_recipes = []
    real_render_source = app_module._recipe_render_source

    def spy_render_source(photo, recipe, max_size, vireo_dir, folders):
        seen_recipes.append(recipe)
        return real_render_source(photo, recipe, max_size, vireo_dir, folders)

    monkeypatch.setattr(app_module, "_recipe_render_source", spy_render_source)

    client = app.test_client()
    client.get(
        f"/photos/{photo_id}/edit-preview",
        query_string={"size": "1024", "recipe": "{}"},
    )
    client.get(
        f"/photos/{photo_id}/edit-preview",
        query_string={"size": "1024", "recipe": "{}", "analysis": "1"},
    )
    client.get(
        f"/photos/{photo_id}/edit-preview",
        query_string={
            "size": "1024",
            "recipe": '{"rotation":90}',
            "analysis": "1",
        },
    )

    assert seen_recipes == [
        {},
        {"version": 1},
        {"version": 1, "rotation": 90},
    ]


def test_non_crop_preview_loads_with_requested_size(client_with_photo, monkeypatch):
    import image_loader

    app, db, photo_id = client_with_photo
    db.set_photo_edit_recipe(photo_id, {"rotation": 90})
    original_load_image = image_loader.load_image
    seen_max_sizes = []

    def tracking_load_image(file_path, max_size=1024):
        seen_max_sizes.append(max_size)
        return original_load_image(file_path, max_size=max_size)

    monkeypatch.setattr(image_loader, "load_image", tracking_load_image)

    resp = app.test_client().get(f"/photos/{photo_id}/preview?size=1920")

    assert resp.status_code == 200
    assert 1920 in seen_max_sizes


def test_edit_recipe_api_invalidates_untracked_preview_file(client_with_photo):
    import os

    from PIL import Image

    app, db, photo_id = client_with_photo
    client = app.test_client()
    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    preview_dir = os.path.join(vireo_dir, "previews")
    os.makedirs(preview_dir, exist_ok=True)
    untracked_path = os.path.join(preview_dir, f"{photo_id}_2560.jpg")
    Image.new("RGB", (10, 10), "purple").save(untracked_path, "JPEG")
    assert db.preview_cache_get(photo_id, 2560) is None

    resp = client.put(
        f"/api/photos/{photo_id}/edit-recipe",
        json={"recipe": {"rotation": 90}},
    )

    assert resp.status_code == 200
    assert not os.path.exists(untracked_path)


def test_edited_preview_does_not_adopt_stale_untracked_file_after_unlink_failure(
    client_with_photo, monkeypatch,
):
    import io
    import os

    import app as app_module
    from PIL import Image

    app, db, photo_id = client_with_photo
    client = app.test_client()
    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    preview_dir = os.path.join(vireo_dir, "previews")
    os.makedirs(preview_dir, exist_ok=True)
    stale_path = os.path.join(preview_dir, f"{photo_id}_1920.jpg")
    Image.new("RGB", (10, 10), "purple").save(stale_path, "JPEG")
    db.set_photo_edit_recipe(photo_id, {"rotation": 90})
    assert db.preview_cache_get(photo_id, 1920) is None
    original_remove = app_module.os.remove

    def locked_remove(path):
        if path == stale_path:
            raise OSError("locked")
        return original_remove(path)

    monkeypatch.setattr(app_module.os, "remove", locked_remove)

    rendered = client.get(f"/photos/{photo_id}/preview?size=1920")

    assert rendered.status_code == 200
    with Image.open(io.BytesIO(rendered.data)) as img:
        assert img.size == (600, 800)


def test_cleared_recipe_does_not_adopt_stale_edited_preview_after_unlink_failure(
    client_with_photo, monkeypatch,
):
    import io
    import os

    import app as app_module
    from PIL import Image

    app, db, photo_id = client_with_photo
    client = app.test_client()
    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    preview_path = os.path.join(vireo_dir, "previews", f"{photo_id}_1920.jpg")

    db.set_photo_edit_recipe(photo_id, {"rotation": 90})
    edited = client.get(f"/photos/{photo_id}/preview?size=1920")
    assert edited.status_code == 200
    assert db.preview_cache_get(photo_id, 1920) is not None
    with Image.open(io.BytesIO(edited.data)) as img:
        assert img.size == (600, 800)

    original_remove = app_module.os.remove

    def locked_remove(path):
        if path == preview_path:
            raise OSError("locked")
        return original_remove(path)

    monkeypatch.setattr(app_module.os, "remove", locked_remove)

    cleared = client.delete(f"/api/photos/{photo_id}/edit-recipe")
    assert cleared.status_code == 200
    assert os.path.exists(preview_path)

    rendered = client.get(f"/photos/{photo_id}/preview?size=1920")

    assert rendered.status_code == 200
    with Image.open(io.BytesIO(rendered.data)) as img:
        assert img.size == (800, 600)


def test_failed_preview_invalidation_survives_app_restart(
    client_with_photo, monkeypatch,
):
    import io
    import os

    import app as app_module
    from PIL import Image

    app, db, photo_id = client_with_photo
    client = app.test_client()
    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    preview_path = os.path.join(vireo_dir, "previews", f"{photo_id}_1920.jpg")

    original = client.get(f"/photos/{photo_id}/preview?size=1920")
    assert original.status_code == 200
    assert db.preview_cache_get(photo_id, 1920) is not None
    with Image.open(io.BytesIO(original.data)) as img:
        assert img.size == (800, 600)

    original_remove = app_module.os.remove

    def locked_remove(path):
        if path == preview_path:
            raise OSError("locked")
        return original_remove(path)

    monkeypatch.setattr(app_module.os, "remove", locked_remove)
    edited = client.put(
        f"/api/photos/{photo_id}/edit-recipe",
        json={"recipe": {"rotation": 90}},
    )
    assert edited.status_code == 200
    assert os.path.exists(preview_path)
    assert db.preview_cache_get(photo_id, 1920) is not None

    monkeypatch.setattr(app_module.os, "remove", original_remove)
    restarted = app_module.create_app(
        db._db_path,
        thumb_cache_dir=app.config["THUMB_CACHE_DIR"],
    )
    rendered = restarted.test_client().get(
        f"/photos/{photo_id}/preview?size=1920",
    )

    assert rendered.status_code == 200
    with Image.open(io.BytesIO(rendered.data)) as img:
        assert img.size == (600, 800)


def test_edit_recipe_api_invalidates_thumbnail_and_renders_edit(client_with_photo):
    import io
    import os

    from PIL import Image

    app, db, photo_id = client_with_photo
    client = app.test_client()
    thumb_dir = app.config["THUMB_CACHE_DIR"]
    thumb_path = os.path.join(thumb_dir, f"{photo_id}.jpg")
    Image.new("RGB", (400, 300), "green").save(thumb_path, "JPEG")
    db.conn.execute(
        "UPDATE photos SET thumb_path = ? WHERE id = ?",
        (f"{photo_id}.jpg", photo_id),
    )
    db.conn.commit()

    resp = client.put(
        f"/api/photos/{photo_id}/edit-recipe",
        json={"recipe": {"rotation": 90}},
    )

    assert resp.status_code == 200
    assert not os.path.exists(thumb_path)
    row = db.conn.execute(
        "SELECT thumb_path FROM photos WHERE id = ?", (photo_id,),
    ).fetchone()
    assert row["thumb_path"] is None

    rendered = client.get(f"/thumbnails/{photo_id}.jpg")
    assert rendered.status_code == 200
    with Image.open(io.BytesIO(rendered.data)) as img:
        assert img.size == (300, 400)


def test_edit_recipe_api_invalidates_external_edit_handoff(client_with_photo):
    import json
    import os

    from PIL import Image

    app, _db, photo_id = client_with_photo
    client = app.test_client()
    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    external_dir = os.path.join(vireo_dir, "external-edits")
    os.makedirs(external_dir, exist_ok=True)
    external_path = os.path.join(external_dir, f"{photo_id}.jpg")
    external_meta = os.path.join(external_dir, f"{photo_id}.json")
    Image.new("RGB", (10, 10), "green").save(external_path, "JPEG")
    with open(external_meta, "w", encoding="utf-8") as f:
        json.dump({"recipe": "stale"}, f)

    resp = client.put(
        f"/api/photos/{photo_id}/edit-recipe",
        json={"recipe": {"rotation": 90}},
    )

    assert resp.status_code == 200
    assert not os.path.exists(external_path)
    assert not os.path.exists(external_meta)


def test_edit_recipe_keeps_thumb_path_when_stale_thumbnail_unlink_fails(
    client_with_photo, monkeypatch,
):
    import os

    import app as app_module
    from PIL import Image

    app, db, photo_id = client_with_photo
    client = app.test_client()
    thumb_dir = app.config["THUMB_CACHE_DIR"]
    thumb_path = os.path.join(thumb_dir, f"{photo_id}.jpg")
    Image.new("RGB", (400, 300), "green").save(thumb_path, "JPEG")
    db.conn.execute(
        "UPDATE photos SET thumb_path = ? WHERE id = ?",
        (f"{photo_id}.jpg", photo_id),
    )
    db.conn.commit()
    original_remove = app_module.os.remove

    def locked_remove(path):
        if path == thumb_path:
            raise OSError("locked")
        return original_remove(path)

    monkeypatch.setattr(app_module.os, "remove", locked_remove)

    resp = client.put(
        f"/api/photos/{photo_id}/edit-recipe",
        json={"recipe": {"rotation": 90}},
    )

    assert resp.status_code == 200
    assert os.path.exists(thumb_path)
    row = db.conn.execute(
        "SELECT thumb_path FROM photos WHERE id = ?", (photo_id,),
    ).fetchone()
    assert row["thumb_path"] == f"{photo_id}.jpg"


def test_cropped_thumbnail_uses_original_when_working_copy_is_too_small(
    client_with_photo,
):
    import io
    import os

    from PIL import Image

    app, db, photo_id = client_with_photo
    client = app.test_client()
    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    working_dir = os.path.join(vireo_dir, "working")
    os.makedirs(working_dir, exist_ok=True)
    working_path = os.path.join(working_dir, f"{photo_id}.jpg")
    Image.new("RGB", (400, 300), (30, 120, 200)).save(working_path, "JPEG")
    db.conn.execute(
        "UPDATE photos SET working_copy_path=? WHERE id=?",
        (f"working/{photo_id}.jpg", photo_id),
    )
    db.conn.commit()
    db.set_photo_edit_recipe(
        photo_id,
        {"crop": {"x": 0, "y": 0, "w": 0.5, "h": 0.5}},
    )

    rendered = client.get(f"/thumbnails/{photo_id}.jpg")

    assert rendered.status_code == 200
    with Image.open(io.BytesIO(rendered.data)) as img:
        assert img.size == (400, 300)


def test_cropped_thumbnail_uses_companion_before_raw_failure_marker(
    client_with_photo, monkeypatch,
):
    import io
    import os

    import thumbnails
    from PIL import Image

    app, db, photo_id = client_with_photo
    client = app.test_client()
    folder = db.conn.execute(
        "SELECT f.path FROM photos p JOIN folders f ON f.id=p.folder_id WHERE p.id=?",
        (photo_id,),
    ).fetchone()
    companion_path = os.path.join(folder["path"], "test.jpg")
    db.conn.execute(
        """UPDATE photos
           SET filename='source.NEF', extension='.nef',
               companion_path='test.jpg',
               working_copy_path=NULL,
               width=800, height=600,
               file_mtime=1234.0,
               working_copy_failed_at=datetime('now'),
               working_copy_failed_mtime=1234.0,
               working_copy_failed_source='source'
           WHERE id=?""",
        (photo_id,),
    )
    db.conn.commit()
    db.set_photo_edit_recipe(
        photo_id,
        {"crop": {"x": 0, "y": 0, "w": 0.5, "h": 1}},
    )
    from image_loader import RAW_DECODE_PRESERVE_HIGHLIGHTS
    original_load_image = thumbnails.load_image
    loaded = []

    def tracking_load_image(file_path, max_size=1024, **kwargs):
        loaded.append((file_path, kwargs))
        if str(file_path).lower().endswith(".nef"):
            raise AssertionError("thumbnail retried RAW before companion")
        return original_load_image(file_path, max_size=max_size, **kwargs)

    monkeypatch.setattr(thumbnails, "load_image", tracking_load_image)

    rendered = client.get(f"/thumbnails/{photo_id}.jpg")

    assert rendered.status_code == 200
    loaded_paths = [path for path, _ in loaded]
    assert loaded_paths == [companion_path]
    # Even though the resolved source is the companion JPEG, the
    # thumbnail self-heal must request RAW_DECODE_PRESERVE_HIGHLIGHTS so
    # the call would demosaic the RAW with highlight preservation if
    # _recipe_render_source had returned the RAW path instead. Keying
    # the decode mode off the photo's primary extension (not the
    # resolved source) keeps thumbnails in sync with previews/exports.
    _, loaded_kwargs = loaded[0]
    assert loaded_kwargs.get("raw_decode") == RAW_DECODE_PRESERVE_HIGHLIGHTS
    with Image.open(io.BytesIO(rendered.data)) as img:
        assert img.size == (267, 400)


def test_cropped_thumbnail_uses_working_copy_on_first_raw_decode_failure(
    client_with_photo, monkeypatch,
):
    import io
    import os

    import thumbnails
    from PIL import Image

    app, db, photo_id = client_with_photo
    client = app.test_client()
    folder = db.conn.execute(
        "SELECT f.path FROM photos p JOIN folders f ON f.id=p.folder_id WHERE p.id=?",
        (photo_id,),
    ).fetchone()
    raw_path = os.path.join(folder["path"], "DSC_7062.NEF")
    with open(raw_path, "wb") as f:
        f.write(b"unsupported raw")

    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    working_dir = os.path.join(vireo_dir, "working")
    os.makedirs(working_dir, exist_ok=True)
    wc_path = os.path.join(working_dir, f"{photo_id}.jpg")
    Image.new("RGB", (800, 600), (30, 120, 200)).save(wc_path, "JPEG")
    mtime = os.path.getmtime(raw_path)
    db.conn.execute(
        """UPDATE photos
           SET filename='DSC_7062.NEF', extension='.nef',
               companion_path=NULL,
               working_copy_path=?,
               width=800, height=600,
               file_mtime=?,
               working_copy_failed_at=NULL,
               working_copy_failed_mtime=NULL,
               working_copy_failed_source=NULL
           WHERE id=?""",
        (f"working/{photo_id}.jpg", mtime, photo_id),
    )
    db.conn.commit()
    db.set_photo_edit_recipe(
        photo_id,
        {"crop": {"x": 0, "y": 0, "w": 0.5, "h": 1}},
    )

    original_load_image = thumbnails.load_image
    loaded_paths = []

    def tracking_load_image(file_path, max_size=1024, **kwargs):
        loaded_paths.append(str(file_path))
        if str(file_path).lower().endswith(".nef"):
            return None
        return original_load_image(file_path, max_size=max_size, **kwargs)

    monkeypatch.setattr(thumbnails, "load_image", tracking_load_image)

    rendered = client.get(f"/thumbnails/{photo_id}.jpg")

    assert rendered.status_code == 200, rendered.get_data(as_text=True)
    assert [os.path.normpath(path) for path in loaded_paths] == [
        os.path.normpath(raw_path),
        os.path.normpath(wc_path),
    ]
    with Image.open(io.BytesIO(rendered.data)) as img:
        assert img.size == (267, 400)
    row = db.conn.execute(
        "SELECT working_copy_failed_source FROM photos WHERE id=?",
        (photo_id,),
    ).fetchone()
    assert row["working_copy_failed_source"] == "source"


def test_thumbnail_falls_back_when_raw_short_edge_is_smaller(
    client_with_photo, monkeypatch,
):
    """A RAW embedded preview with the requested long edge is still too small
    when its short edge does not match the expected aspect. The thumbnail
    self-heal path should reject it and retry the companion before caching.
    """
    import io
    import os

    import thumbnails
    from PIL import Image

    app, db, photo_id = client_with_photo
    client = app.test_client()
    folder = db.conn.execute(
        "SELECT f.path FROM photos p JOIN folders f ON f.id=p.folder_id WHERE p.id=?",
        (photo_id,),
    ).fetchone()
    raw_path = os.path.join(folder["path"], "thumb.NEF")
    with open(raw_path, "wb") as f:
        f.write(b"unsupported raw")
    companion_path = os.path.join(folder["path"], "thumb.jpg")
    Image.new("RGB", (6000, 4000), (40, 90, 180)).save(
        companion_path, "JPEG", quality=85,
    )
    db.conn.execute(
        """UPDATE photos
           SET filename='thumb.NEF', extension='.nef',
               companion_path='thumb.jpg',
               working_copy_path=NULL,
               width=6000, height=4000,
               working_copy_failed_at=NULL,
               working_copy_failed_mtime=NULL,
               working_copy_failed_source=NULL
           WHERE id=?""",
        (photo_id,),
    )
    db.conn.commit()
    db.set_photo_edit_recipe(photo_id, {"rotation": 90})

    original_load_image = thumbnails.load_image
    loaded_paths = []

    def tracking_load_image(file_path, max_size=1024, **kwargs):
        loaded_paths.append(str(file_path))
        if str(file_path).lower().endswith(".nef"):
            # Long edge ties the requested thumbnail size (400), but a 3:2
            # source should load as 400x267 before recipe application.
            return Image.new("RGB", (400, 225), (200, 50, 50))
        return original_load_image(file_path, max_size=max_size, **kwargs)

    monkeypatch.setattr(thumbnails, "load_image", tracking_load_image)

    rendered = client.get(f"/thumbnails/{photo_id}.jpg")

    assert rendered.status_code == 200
    assert loaded_paths == [raw_path, companion_path]
    with Image.open(io.BytesIO(rendered.data)) as img:
        assert img.size == (267, 400)


def test_edited_original_uses_trusted_working_copy_when_source_missing(
    client_with_photo,
):
    import io
    import os

    from PIL import Image

    app, db, photo_id = client_with_photo
    client = app.test_client()
    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    working_dir = os.path.join(vireo_dir, "working")
    os.makedirs(working_dir, exist_ok=True)
    working_path = os.path.join(working_dir, f"{photo_id}.jpg")
    Image.new("RGB", (800, 600), (30, 120, 200)).save(working_path, "JPEG")
    db.conn.execute(
        "UPDATE photos SET working_copy_path=? WHERE id=?",
        (f"working/{photo_id}.jpg", photo_id),
    )
    db.conn.commit()
    folder = db.conn.execute(
        "SELECT f.path, p.filename FROM photos p JOIN folders f ON f.id=p.folder_id WHERE p.id=?",
        (photo_id,),
    ).fetchone()
    os.remove(os.path.join(folder["path"], folder["filename"]))

    resp = client.put(
        f"/api/photos/{photo_id}/edit-recipe",
        json={"recipe": {"rotation": 90}},
    )
    assert resp.status_code == 200

    rendered = client.get(f"/photos/{photo_id}/original")
    assert rendered.status_code == 200
    with Image.open(io.BytesIO(rendered.data)) as img:
        assert img.size == (600, 800)


def test_edited_raw_original_uses_trusted_working_copy_when_source_missing(
    client_with_photo,
):
    import io
    import os

    from PIL import Image

    app, db, photo_id = client_with_photo
    client = app.test_client()
    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    working_dir = os.path.join(vireo_dir, "working")
    os.makedirs(working_dir, exist_ok=True)
    wc_rel = f"working/{photo_id}.jpg"
    Image.new("RGB", (800, 600), (30, 120, 200)).save(
        os.path.join(vireo_dir, wc_rel), "JPEG",
    )
    folder = db.conn.execute(
        "SELECT f.path, p.filename FROM photos p JOIN folders f ON f.id=p.folder_id WHERE p.id=?",
        (photo_id,),
    ).fetchone()
    os.remove(os.path.join(folder["path"], folder["filename"]))
    db.conn.execute(
        """UPDATE photos
           SET filename='offline.NEF', extension='.nef',
               working_copy_path=?,
               companion_path=NULL,
               width=800, height=600
           WHERE id=?""",
        (wc_rel, photo_id),
    )
    db.conn.commit()
    db.set_photo_edit_recipe(photo_id, {"rotation": 90})

    rendered = client.get(f"/photos/{photo_id}/original")

    assert rendered.status_code == 200, rendered.get_data(as_text=True)
    with Image.open(io.BytesIO(rendered.data)) as img:
        assert img.size == (600, 800)


def test_edited_original_uses_companion_after_raw_failure_marker(
    client_with_photo, monkeypatch,
):
    """When the scanner has marked the RAW as failed for the current mtime,
    the edited original endpoint must route through the full-resolution
    companion JPEG instead of returning 500. Otherwise the same photo
    that already rendered via the companion fallback once would stop
    serving on every subsequent request until the marker expires —
    exactly what _recipe_render_source already prevents for previews."""
    import image_loader

    app, db, photo_id = client_with_photo
    client = app.test_client()
    folder = db.conn.execute(
        "SELECT f.path FROM photos p JOIN folders f ON f.id=p.folder_id WHERE p.id=?",
        (photo_id,),
    ).fetchone()
    db.conn.execute(
        """UPDATE photos
           SET filename='source.NEF', extension='.nef',
               companion_path='test.jpg',
               working_copy_path=NULL,
               width=800, height=600,
               file_mtime=1234.0,
               working_copy_failed_at=datetime('now'),
               working_copy_failed_mtime=1234.0,
               working_copy_failed_source='source'
           WHERE id=?""",
        (photo_id,),
    )
    db.conn.commit()
    db.set_photo_edit_recipe(photo_id, {"rotation": 90})

    original_load_image = image_loader.load_image
    loaded_paths = []

    def tracking_load_image(file_path, max_size=1024, **kwargs):
        loaded_paths.append(str(file_path))
        return original_load_image(file_path, max_size=max_size, **kwargs)

    monkeypatch.setattr(image_loader, "load_image", tracking_load_image)

    rendered = client.get(f"/photos/{photo_id}/original")

    assert rendered.status_code == 200, rendered.data
    # The endpoint must skip the failed RAW and load the companion JPEG.
    assert len(loaded_paths) == 1
    assert loaded_paths[0].lower().endswith(".jpg")


def test_edited_original_records_raw_marker_when_companion_rescues_decode(
    client_with_photo, monkeypatch,
):
    """When the RAW decode fails on an edited RAW+JPEG with no current
    failure marker, the companion-fallback path must stamp
    working_copy_failed_source='source' so the next request routes
    directly through the companion via the marker-aware branch instead
    of retrying the slow failing RAW decode each hit."""
    import os

    import app as app_module
    import image_loader
    from PIL import Image

    app, db, photo_id = client_with_photo
    client = app.test_client()
    folder = db.conn.execute(
        "SELECT f.path FROM photos p JOIN folders f ON f.id=p.folder_id WHERE p.id=?",
        (photo_id,),
    ).fetchone()
    raw_path = os.path.join(folder["path"], "unsupported.NEF")
    with open(raw_path, "wb") as f:
        f.write(b"\x00")
    companion_path = os.path.join(folder["path"], "unsupported.JPG")
    Image.new("RGB", (800, 600), (40, 80, 120)).save(companion_path, "JPEG")
    db.conn.execute(
        """UPDATE photos
           SET filename='unsupported.NEF', extension='.nef',
               companion_path='unsupported.JPG',
               working_copy_path=NULL,
               working_copy_failed_at=NULL,
               working_copy_failed_mtime=NULL,
               working_copy_failed_source=NULL,
               width=800, height=600,
               file_mtime=1234.0
           WHERE id=?""",
        (photo_id,),
    )
    db.conn.commit()
    db.set_photo_edit_recipe(photo_id, {"rotation": 90})

    def fake_load_image(file_path, max_size=1024, **kwargs):
        if str(file_path).lower().endswith(".nef"):
            return None
        return Image.new("RGB", (800, 600), (40, 80, 120))

    monkeypatch.setattr(image_loader, "load_image", fake_load_image)
    if hasattr(app_module, "load_image"):
        monkeypatch.setattr(app_module, "load_image", fake_load_image, raising=False)

    resp = client.get(f"/photos/{photo_id}/original")
    assert resp.status_code == 200, resp.get_data(as_text=True)

    row = db.conn.execute(
        "SELECT working_copy_failed_source, working_copy_failed_mtime"
        " FROM photos WHERE id=?",
        (photo_id,),
    ).fetchone()
    assert row["working_copy_failed_source"] == "source"
    assert row["working_copy_failed_mtime"] == 1234.0


def test_edited_original_decodes_raw_with_highlight_preservation(
    client_with_photo, monkeypatch,
):
    """Edited RAW+JPEG originals must decode the RAW, not substitute the JPEG.

    The companion JPEG is the camera-baked render with highlights already
    clipped; substituting it bypasses RAW_DECODE_PRESERVE_HIGHLIGHTS and applies
    the user's edits to clipped data.
    """
    import io
    import os

    import app as app_module
    import image_loader
    from image_loader import RAW_DECODE_PRESERVE_HIGHLIGHTS
    from PIL import Image

    app, db, photo_id = client_with_photo
    client = app.test_client()
    folder = db.conn.execute(
        "SELECT f.path FROM photos p JOIN folders f ON f.id=p.folder_id WHERE p.id=?",
        (photo_id,),
    ).fetchone()
    raw_path = os.path.join(folder["path"], "source.NEF")
    with open(raw_path, "wb") as f:
        f.write(b"\x00")
    db.conn.execute(
        """UPDATE photos
           SET filename='source.NEF', extension='.nef',
               companion_path='test.jpg',
               working_copy_path=NULL,
               width=800, height=600,
               file_mtime=1234.0
           WHERE id=?""",
        (photo_id,),
    )
    db.conn.commit()
    db.set_photo_edit_recipe(photo_id, {"rotation": 90})

    load_calls = []

    def tracking_load_image(file_path, max_size=1024, **kwargs):
        load_calls.append((str(file_path), kwargs))
        return Image.new("RGB", (800, 600), color="red")

    monkeypatch.setattr(image_loader, "load_image", tracking_load_image)
    if hasattr(app_module, "load_image"):
        monkeypatch.setattr(app_module, "load_image", tracking_load_image)

    rendered = client.get(f"/photos/{photo_id}/original")

    assert rendered.status_code == 200, rendered.data
    assert len(load_calls) == 1
    loaded_path, loaded_kwargs = load_calls[0]
    assert loaded_path.lower().endswith(".nef"), (
        f"endpoint should load RAW primary, got {loaded_path!r}"
    )
    assert loaded_kwargs.get("raw_decode") == RAW_DECODE_PRESERVE_HIGHLIGHTS
    with Image.open(io.BytesIO(rendered.data)) as img:
        assert img.size == (600, 800)


def test_edited_original_cache_write_is_atomic(client_with_photo, monkeypatch):
    import os

    import app as app_module

    app, db, photo_id = client_with_photo
    db.set_photo_edit_recipe(photo_id, {"rotation": 90})
    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    calls = []
    original_replace = app_module.os.replace

    def tracking_replace(src, dst):
        calls.append((src, dst, os.path.exists(src)))
        return original_replace(src, dst)

    monkeypatch.setattr(app_module.os, "replace", tracking_replace)

    rendered = app.test_client().get(f"/photos/{photo_id}/original")

    assert rendered.status_code == 200
    assert calls
    tmp_path, dst_path, tmp_existed = calls[0]
    assert tmp_existed is True
    assert os.path.dirname(dst_path) == os.path.join(vireo_dir, "originals")
    assert os.path.basename(dst_path).startswith(f"{photo_id}_")
    assert dst_path.endswith(".jpg")
    assert not os.path.exists(tmp_path)
    assert os.path.exists(dst_path)


def test_cropped_preview_uses_original_when_working_copy_is_too_small(
    client_with_photo,
):
    import io
    import os

    from PIL import Image

    app, db, photo_id = client_with_photo
    client = app.test_client()
    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    working_dir = os.path.join(vireo_dir, "working")
    os.makedirs(working_dir, exist_ok=True)
    working_path = os.path.join(working_dir, f"{photo_id}.jpg")
    Image.new("RGB", (400, 300), (30, 120, 200)).save(working_path, "JPEG")
    db.conn.execute(
        "UPDATE photos SET working_copy_path=? WHERE id=?",
        (f"working/{photo_id}.jpg", photo_id),
    )
    db.conn.commit()
    db.set_photo_edit_recipe(
        photo_id,
        {"crop": {"x": 0, "y": 0, "w": 0.5, "h": 0.5}},
    )

    rendered = client.get(f"/photos/{photo_id}/preview?size=1920")

    assert rendered.status_code == 200
    with Image.open(io.BytesIO(rendered.data)) as img:
        assert img.size == (400, 300)


def test_cropped_preview_keeps_full_size_working_copy_fallback(
    client_with_photo,
):
    import io
    import os

    from PIL import Image

    app, db, photo_id = client_with_photo
    client = app.test_client()
    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    working_dir = os.path.join(vireo_dir, "working")
    os.makedirs(working_dir, exist_ok=True)
    working_path = os.path.join(working_dir, f"{photo_id}.jpg")
    Image.new("RGB", (800, 600), (30, 120, 200)).save(working_path, "JPEG")
    db.conn.execute(
        "UPDATE photos SET working_copy_path=? WHERE id=?",
        (f"working/{photo_id}.jpg", photo_id),
    )
    db.conn.commit()
    folder = db.conn.execute(
        "SELECT f.path, p.filename FROM photos p JOIN folders f ON f.id=p.folder_id WHERE p.id=?",
        (photo_id,),
    ).fetchone()
    os.remove(os.path.join(folder["path"], folder["filename"]))
    db.set_photo_edit_recipe(
        photo_id,
        {"crop": {"x": 0, "y": 0, "w": 0.5, "h": 0.5}},
    )

    rendered = client.get(f"/photos/{photo_id}/preview?size=1920")

    assert rendered.status_code == 200
    with Image.open(io.BytesIO(rendered.data)) as img:
        assert img.size == (400, 300)


def test_edit_recipe_api_rejects_invalid_recipe(client_with_photo):
    app, _db, photo_id = client_with_photo
    client = app.test_client()

    resp = client.put(
        f"/api/photos/{photo_id}/edit-recipe",
        json={"recipe": {"rotation": 45}},
    )
    assert resp.status_code == 400
    assert "rotation" in resp.get_json()["error"]


def test_edit_recipe_api_stores_adjustments(client_with_photo):
    app, db, photo_id = client_with_photo
    client = app.test_client()

    resp = client.put(
        f"/api/photos/{photo_id}/edit-recipe",
        json={
            "recipe": {
                "adjustments": {
                    "exposure": 0.5,
                    "highlights": -20,
                    "shadows": 35,
                    "whites": 12,
                    "blacks": -8,
                    "contrast": 12,
                    "temperature": 25,
                    "tint": -8,
                    "vibrance": 22,
                    "saturation": 18,
                },
            },
        },
    )

    assert resp.status_code == 200
    stored = db.get_photo_edit_recipe(photo_id)
    assert stored["adjustments"] == {
        "exposure": 0.5,
        "highlights": -20.0,
        "shadows": 35.0,
        "whites": 12.0,
        "blacks": -8.0,
        "contrast": 12.0,
        "vibrance": 22.0,
        "saturation": 18.0,
        "white_balance": {
            "temperature": 25.0,
            "tint": -8.0,
        },
    }
    assert resp.get_json()["recipe"] == stored


def test_edit_recipe_api_rejects_malformed_body_without_clearing(client_with_photo):
    app, db, photo_id = client_with_photo
    client = app.test_client()

    existing = {"rotation": 180}
    assert client.put(
        f"/api/photos/{photo_id}/edit-recipe",
        json={"recipe": existing},
    ).status_code == 200
    stored = db.get_photo_edit_recipe(photo_id)

    cases = [
        {},
        {"data": "{", "content_type": "application/json"},
        {"json": []},
        {"json": {"recipe": []}},
        {"json": {"recipe": {"crop": {"x": False, "y": False, "w": True, "h": True}}}},
    ]
    for kwargs in cases:
        resp = client.put(f"/api/photos/{photo_id}/edit-recipe", **kwargs)
        assert resp.status_code == 400
        assert db.get_photo_edit_recipe(photo_id) == stored


def test_edit_recipe_api_undo_redo(client_with_photo):
    app, db, photo_id = client_with_photo
    client = app.test_client()

    resp = client.put(
        f"/api/photos/{photo_id}/edit-recipe",
        json={"recipe": {"rotation": 180}},
    )
    assert resp.status_code == 200
    assert db.get_photo_edit_recipe(photo_id)["rotation"] == 180

    undo = client.post("/api/undo")
    assert undo.status_code == 200
    assert undo.get_json()["action_type"] == "edit_recipe"
    assert undo.get_json()["edit_recipes"] == {str(photo_id): None}
    assert db.get_photo_edit_recipe(photo_id) is None

    redo = client.post("/api/redo")
    assert redo.status_code == 200
    assert redo.get_json()["action_type"] == "edit_recipe"
    assert redo.get_json()["edit_recipes"] == {
        str(photo_id): {"rotation": 180, "version": 1}
    }
    assert db.get_photo_edit_recipe(photo_id)["rotation"] == 180


def test_photo_editor_page_renders(client_with_photo):
    app, _db, photo_id = client_with_photo
    client = app.test_client()

    resp = client.get(f"/edit/{photo_id}")

    assert resp.status_code == 200
    html = resp.get_data(as_text=True)
    assert "Photo Editor" in html
    assert "Edit History" in html
    assert "Save Changes" in html
    assert "Feedback" in html
    assert "histogramCanvas" in html
    assert "shadowClipValue" in html
    assert "highlightClipValue" in html
    assert "Before" in html
    assert "100%" in html


def test_photo_edit_history_endpoint_lists_recipe_checkpoints(client_with_photo):
    app, db, photo_id = client_with_photo
    client = app.test_client()

    first = client.put(
        f"/api/photos/{photo_id}/edit-recipe",
        json={
            "recipe": {"rotation": 90},
            "description": "Rotated from editor",
        },
    )
    assert first.status_code == 200
    second = client.put(
        f"/api/photos/{photo_id}/edit-recipe",
        json={
            "recipe": {
                "rotation": 90,
                "adjustments": {"exposure": 0.5},
            },
            "description": "Adjusted exposure",
        },
    )
    assert second.status_code == 200

    resp = client.get(f"/api/photos/{photo_id}/edit-history")

    assert resp.status_code == 200
    data = resp.get_json()
    assert data["current_recipe"] == db.get_photo_edit_recipe(photo_id)
    assert [h["description"] for h in data["history"][:2]] == [
        "Adjusted exposure",
        "Rotated from editor",
    ]
    assert data["history"][0]["new_recipe"]["adjustments"]["exposure"] == 0.5
    assert data["history"][1]["old_recipe"] is None
    assert data["history"][1]["new_recipe"] == {"version": 1, "rotation": 90}


def test_photo_edit_history_restore_records_checkpoint(client_with_photo):
    app, db, photo_id = client_with_photo
    client = app.test_client()

    assert client.put(
        f"/api/photos/{photo_id}/edit-recipe",
        json={"recipe": {"rotation": 90}},
    ).status_code == 200
    assert client.put(
        f"/api/photos/{photo_id}/edit-recipe",
        json={"recipe": {"rotation": 180}},
    ).status_code == 200

    restore = client.put(
        f"/api/photos/{photo_id}/edit-recipe",
        json={
            "recipe": {"rotation": 90},
            "description": "Restored photo edit checkpoint",
        },
    )

    assert restore.status_code == 200
    assert db.get_photo_edit_recipe(photo_id) == {"version": 1, "rotation": 90}
    history = client.get(f"/api/photos/{photo_id}/edit-history").get_json()["history"]
    assert history[0]["description"] == "Restored photo edit checkpoint"
    assert history[0]["old_recipe"] == {"version": 1, "rotation": 180}
    assert history[0]["new_recipe"] == {"version": 1, "rotation": 90}


def test_preview_adopts_existing_file_on_first_access(client_with_photo):
    """A cached file left over from the old scheme is adopted into the LRU."""
    import os
    import time
    app, db, photo_id = client_with_photo
    # Create a cache file manually without a DB row
    preview_dir = os.path.join(
        os.path.dirname(app.config["THUMB_CACHE_DIR"]), "previews"
    )
    os.makedirs(preview_dir, exist_ok=True)
    cache_path = os.path.join(preview_dir, f"{photo_id}_1920.jpg")
    with open(cache_path, "wb") as f:
        f.write(b"x" * 12345)
    # Backdate mtime
    past = time.time() - 3600
    os.utime(cache_path, (past, past))
    assert db.preview_cache_get(photo_id, 1920) is None

    client = app.test_client()
    resp = client.get(f"/photos/{photo_id}/preview?size=1920")
    assert resp.status_code == 200
    row = db.preview_cache_get(photo_id, 1920)
    assert row is not None
    assert row["bytes"] == 12345


def test_full_is_alias_for_preview_at_configured_size(client_with_photo, monkeypatch):
    """/full returns the same bytes as /preview?size=<preview_max_size>."""
    import config as cfg
    # Pin preview_max_size to 1920 for determinism.
    monkeypatch.setattr(
        cfg, "get",
        lambda k: 1920 if k == "preview_max_size" else cfg.DEFAULTS.get(k),
    )
    app, db, photo_id = client_with_photo
    client = app.test_client()
    full = client.get(f"/photos/{photo_id}/full").data
    preview = client.get(f"/photos/{photo_id}/preview?size=1920").data
    assert full == preview
    assert len(full) > 0


def test_preview_skips_recent_failed_raw_working_copy(
    client_with_photo, monkeypatch,
):
    """A RAW whose working-copy extraction already failed at the current mtime
    should fail fast instead of retrying RAW decode in /preview."""
    import image_loader

    app, db, photo_id = client_with_photo
    file_mtime = db.conn.execute(
        "SELECT file_mtime FROM photos WHERE id=?", (photo_id,)
    ).fetchone()["file_mtime"]
    db.conn.execute(
        """UPDATE photos
           SET filename='bad.NEF', extension='.nef',
               working_copy_path=NULL,
               working_copy_failed_at=datetime('now'),
               working_copy_failed_mtime=?
           WHERE id=?""",
        (file_mtime, photo_id),
    )
    db.conn.commit()

    called = {"load": False, "extract": False}

    def fail_load_if_called(*_args, **_kwargs):
        called["load"] = True
        raise AssertionError("preview retried failed RAW decode")

    def fail_extract_if_called(*_args, **_kwargs):
        called["extract"] = True
        raise AssertionError("preview retried failed RAW extraction")

    monkeypatch.setattr(image_loader, "load_image", fail_load_if_called)
    monkeypatch.setattr(image_loader, "extract_working_copy", fail_extract_if_called)

    client = app.test_client()
    resp = client.get(f"/photos/{photo_id}/preview?size=1920")

    assert resp.status_code == 500
    assert called["load"] is False
    assert called["extract"] is False


def test_preview_skips_recent_failed_raw_when_working_copy_path_is_stale(
    client_with_photo, monkeypatch,
):
    """A stale DB working_copy_path whose file is gone should not bypass a
    fresh RAW failure marker."""
    import image_loader

    app, db, photo_id = client_with_photo
    file_mtime = db.conn.execute(
        "SELECT file_mtime FROM photos WHERE id=?", (photo_id,)
    ).fetchone()["file_mtime"]
    db.conn.execute(
        """UPDATE photos
           SET filename='bad.NEF', extension='.nef',
               working_copy_path='working/missing.jpg',
               working_copy_failed_at=datetime('now'),
               working_copy_failed_mtime=?
           WHERE id=?""",
        (file_mtime, photo_id),
    )
    db.conn.commit()

    called = {"load": False}

    def fail_load_if_called(*_args, **_kwargs):
        called["load"] = True
        raise AssertionError("preview retried failed RAW with stale wc path")

    monkeypatch.setattr(image_loader, "load_image", fail_load_if_called)

    client = app.test_client()
    resp = client.get(f"/photos/{photo_id}/preview?size=1920")

    assert resp.status_code == 500
    assert called["load"] is False


def test_cropped_preview_uses_companion_before_raw_failure_marker(
    client_with_photo, monkeypatch,
):
    import io
    import os

    import image_loader
    from PIL import Image

    app, db, photo_id = client_with_photo
    client = app.test_client()
    folder = db.conn.execute(
        "SELECT f.path FROM photos p JOIN folders f ON f.id=p.folder_id WHERE p.id=?",
        (photo_id,),
    ).fetchone()
    companion_path = os.path.join(folder["path"], "test.jpg")
    db.conn.execute(
        """UPDATE photos
           SET filename='source.NEF', extension='.nef',
               companion_path='test.jpg',
               working_copy_path=NULL,
               width=800, height=600,
               file_mtime=1234.0,
               working_copy_failed_at=datetime('now'),
               working_copy_failed_mtime=1234.0,
               working_copy_failed_source='source'
           WHERE id=?""",
        (photo_id,),
    )
    db.conn.commit()
    db.set_photo_edit_recipe(
        photo_id,
        {"crop": {"x": 0, "y": 0, "w": 0.5, "h": 1}},
    )
    original_load_image = image_loader.load_image
    loaded_paths = []

    def tracking_load_image(file_path, max_size=1024):
        loaded_paths.append(file_path)
        if str(file_path).lower().endswith(".nef"):
            raise AssertionError("preview retried RAW before companion")
        return original_load_image(file_path, max_size=max_size)

    monkeypatch.setattr(image_loader, "load_image", tracking_load_image)

    rendered = client.get(f"/photos/{photo_id}/preview?size=1920")

    assert rendered.status_code == 200
    assert loaded_paths == [companion_path]
    with Image.open(io.BytesIO(rendered.data)) as img:
        assert img.size == (400, 600)


def test_non_crop_preview_uses_companion_before_raw_failure_marker(
    client_with_photo, monkeypatch,
):
    import io
    import os

    import image_loader
    from PIL import Image

    app, db, photo_id = client_with_photo
    client = app.test_client()
    folder = db.conn.execute(
        "SELECT f.path FROM photos p JOIN folders f ON f.id=p.folder_id WHERE p.id=?",
        (photo_id,),
    ).fetchone()
    companion_path = os.path.join(folder["path"], "test.jpg")
    db.conn.execute(
        """UPDATE photos
           SET filename='source.NEF', extension='.nef',
               companion_path='test.jpg',
               working_copy_path=NULL,
               width=800, height=600,
               file_mtime=1234.0,
               working_copy_failed_at=datetime('now'),
               working_copy_failed_mtime=1234.0,
               working_copy_failed_source='source'
           WHERE id=?""",
        (photo_id,),
    )
    db.conn.commit()
    db.set_photo_edit_recipe(photo_id, {"rotation": 90})
    original_load_image = image_loader.load_image
    loaded_paths = []

    def tracking_load_image(file_path, max_size=1024):
        loaded_paths.append(file_path)
        if str(file_path).lower().endswith(".nef"):
            raise AssertionError("preview retried RAW before companion")
        return original_load_image(file_path, max_size=max_size)

    monkeypatch.setattr(image_loader, "load_image", tracking_load_image)

    rendered = client.get(f"/photos/{photo_id}/preview?size=1920")

    assert rendered.status_code == 200
    assert loaded_paths == [companion_path]
    with Image.open(io.BytesIO(rendered.data)) as img:
        assert img.size == (600, 800)


def test_preview_retries_raw_when_recent_marker_came_from_companion(
    client_with_photo, monkeypatch,
):
    """A companion-source failure marker should not suppress a readable RAW."""
    import os

    import image_loader
    from PIL import Image

    app, db, photo_id = client_with_photo
    folder = db.conn.execute(
        "SELECT f.path FROM photos p JOIN folders f ON f.id=p.folder_id WHERE p.id=?",
        (photo_id,),
    ).fetchone()
    raw_path = os.path.join(folder["path"], "bad.NEF")
    companion_path = os.path.join(folder["path"], "bad.jpg")
    with open(raw_path, "wb") as f:
        f.write(b"raw bytes")
    Image.new("RGB", (800, 600), (40, 80, 120)).save(
        companion_path, "JPEG",
    )
    file_mtime = db.conn.execute(
        "SELECT file_mtime FROM photos WHERE id=?", (photo_id,)
    ).fetchone()["file_mtime"]
    db.conn.execute(
        """UPDATE photos
           SET filename='bad.NEF', extension='.nef',
               companion_path='bad.jpg',
               working_copy_path=NULL,
               working_copy_failed_at=datetime('now'),
               working_copy_failed_mtime=?,
               working_copy_failed_source='companion'
           WHERE id=?""",
        (file_mtime, photo_id),
    )
    db.conn.commit()

    called = {"path": None}

    def load_raw(path, *_args, **_kwargs):
        called["path"] = path
        return Image.new("RGB", (800, 600), (40, 80, 120))

    monkeypatch.setattr(image_loader, "load_image", load_raw)

    client = app.test_client()
    resp = client.get(f"/photos/{photo_id}/preview?size=1920")

    assert resp.status_code == 200
    assert called["path"] == raw_path


def test_preview_honors_raw_marker_after_companion_bypass_retry_fails(
    client_with_photo, monkeypatch,
):
    """A RAW failure recorded after companion bypass should suppress repeats."""
    import os

    import image_loader
    from PIL import Image

    app, db, photo_id = client_with_photo
    folder = db.conn.execute(
        "SELECT f.path FROM photos p JOIN folders f ON f.id=p.folder_id WHERE p.id=?",
        (photo_id,),
    ).fetchone()
    raw_path = os.path.join(folder["path"], "bad.NEF")
    companion_path = os.path.join(folder["path"], "bad.jpg")
    with open(raw_path, "wb") as f:
        f.write(b"raw bytes")
    Image.new("RGB", (800, 600), (40, 80, 120)).save(
        companion_path, "JPEG",
    )
    file_mtime = db.conn.execute(
        "SELECT file_mtime FROM photos WHERE id=?", (photo_id,)
    ).fetchone()["file_mtime"]
    db.conn.execute(
        """UPDATE photos
           SET filename='bad.NEF', extension='.nef',
               companion_path='bad.jpg',
               working_copy_path=NULL,
               working_copy_failed_at=datetime('now'),
               working_copy_failed_mtime=?,
               working_copy_failed_source='companion'
           WHERE id=?""",
        (file_mtime, photo_id),
    )
    db.conn.commit()

    calls = {"count": 0}

    def fail_raw(_path, *_args, **_kwargs):
        calls["count"] += 1
        return None

    monkeypatch.setattr(image_loader, "load_image", fail_raw)

    client = app.test_client()
    first = client.get(f"/photos/{photo_id}/preview?size=1920")
    second = client.get(f"/photos/{photo_id}/preview?size=1920")

    assert first.status_code == 500
    assert second.status_code == 500
    # First request: RAW load fails → companion fallback attempted → also
    # fails → source marker stamped. Second request: the source marker
    # short-circuits before any load_image call.
    assert calls["count"] == 2
    row = db.conn.execute(
        """SELECT working_copy_failed_mtime, working_copy_failed_source
           FROM photos WHERE id=?""",
        (photo_id,),
    ).fetchone()
    assert row["working_copy_failed_mtime"] == file_mtime
    assert row["working_copy_failed_source"] == "source"


def test_preview_retries_stale_failed_raw_working_copy(
    client_with_photo, monkeypatch,
):
    """A RAW failure older than the retry window should be allowed to retry."""
    import image_loader

    app, db, photo_id = client_with_photo
    file_mtime = db.conn.execute(
        "SELECT file_mtime FROM photos WHERE id=?", (photo_id,)
    ).fetchone()["file_mtime"]
    db.conn.execute(
        """UPDATE photos
           SET filename='bad.NEF', extension='.nef',
               working_copy_path=NULL,
               working_copy_failed_at=datetime('now', '-48 hours'),
               working_copy_failed_mtime=?
           WHERE id=?""",
        (file_mtime, photo_id),
    )
    db.conn.commit()

    called = {"load": False}

    def record_retry(*_args, **_kwargs):
        called["load"] = True
        return None

    monkeypatch.setattr(image_loader, "load_image", record_retry)

    client = app.test_client()
    resp = client.get(f"/photos/{photo_id}/preview?size=1920")

    assert resp.status_code == 500
    assert called["load"] is True


def test_preview_refreshes_failure_marker_when_stale_retry_fails(
    client_with_photo, monkeypatch,
):
    """When the retry window has expired and the request-time decode still
    fails, /preview must refresh ``working_copy_failed_at`` so the next
    request fails fast again instead of repeating the slow decode."""
    import os

    import image_loader

    app, db, photo_id = client_with_photo
    folder = db.conn.execute(
        "SELECT f.path FROM photos p JOIN folders f ON f.id=p.folder_id WHERE p.id=?",
        (photo_id,),
    ).fetchone()
    with open(os.path.join(folder["path"], "bad.NEF"), "wb") as f:
        f.write(b"not a decodable raw")
    file_mtime = db.conn.execute(
        "SELECT file_mtime FROM photos WHERE id=?", (photo_id,)
    ).fetchone()["file_mtime"]
    db.conn.execute(
        """UPDATE photos
           SET filename='bad.NEF', extension='.nef',
               working_copy_path=NULL,
               working_copy_failed_at=datetime('now', '-48 hours'),
               working_copy_failed_mtime=?
           WHERE id=?""",
        (file_mtime, photo_id),
    )
    db.conn.commit()

    monkeypatch.setattr(image_loader, "load_image", lambda *a, **k: None)

    client = app.test_client()
    resp = client.get(f"/photos/{photo_id}/preview?size=1920")
    assert resp.status_code == 500

    # Marker should now be fresh (within a few seconds of "now") and the
    # mtime should match the file's current mtime so the gate fires again
    # on the next request.
    row = db.conn.execute(
        """SELECT working_copy_failed_mtime,
                  (julianday('now') - julianday(working_copy_failed_at))
                  * 24 * 60 * 60 AS age_seconds
           FROM photos WHERE id=?""",
        (photo_id,),
    ).fetchone()
    assert row["working_copy_failed_mtime"] == file_mtime
    assert row["age_seconds"] is not None and row["age_seconds"] < 60


def test_preview_does_not_refresh_failure_marker_when_raw_source_is_missing(
    client_with_photo,
):
    """Missing/offline RAW sources are not decode failures; leave stale
    extraction markers stale so remounting the source can recover quickly."""
    app, db, photo_id = client_with_photo
    file_mtime = db.conn.execute(
        "SELECT file_mtime FROM photos WHERE id=?", (photo_id,)
    ).fetchone()["file_mtime"]
    db.conn.execute(
        """UPDATE photos
           SET filename='missing.NEF', extension='.nef',
               working_copy_path=NULL,
               working_copy_failed_at=datetime('now', '-48 hours'),
               working_copy_failed_mtime=?
           WHERE id=?""",
        (file_mtime, photo_id),
    )
    db.conn.commit()

    client = app.test_client()
    resp = client.get(f"/photos/{photo_id}/preview?size=1920")

    assert resp.status_code == 500
    row = db.conn.execute(
        """SELECT (julianday('now') - julianday(working_copy_failed_at))
                  * 24 * 60 * 60 AS age_seconds
           FROM photos WHERE id=?""",
        (photo_id,),
    ).fetchone()
    assert row["age_seconds"] is not None and row["age_seconds"] > 24 * 60 * 60


def test_preview_does_not_refresh_failure_marker_when_working_copy_jpeg_is_corrupt(
    client_with_photo, monkeypatch,
):
    """A corrupt/unreadable working-copy JPEG must not stamp the RAW marker.

    ``get_canonical_image_path`` prefers an existing working-copy JPEG over the
    original RAW. If that JPEG fails to load, the failure is in the JPEG, not
    in RAW extraction — recording it as a RAW failure would suppress legitimate
    RAW retries for 24h even though the RAW was never tried.
    """
    import os

    import image_loader

    app, db, photo_id = client_with_photo
    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    working_dir = os.path.join(vireo_dir, "working")
    os.makedirs(working_dir, exist_ok=True)
    wc_rel = f"working/{photo_id}.jpg"
    with open(os.path.join(vireo_dir, wc_rel), "wb") as f:
        f.write(b"corrupt jpeg bytes")

    file_mtime = db.conn.execute(
        "SELECT file_mtime FROM photos WHERE id=?", (photo_id,)
    ).fetchone()["file_mtime"]
    db.conn.execute(
        """UPDATE photos
           SET filename='bad.NEF', extension='.nef',
               working_copy_path=?,
               working_copy_failed_at=datetime('now', '-48 hours'),
               working_copy_failed_mtime=?
           WHERE id=?""",
        (wc_rel, file_mtime, photo_id),
    )
    db.conn.commit()

    monkeypatch.setattr(image_loader, "load_image", lambda *a, **k: None)

    client = app.test_client()
    resp = client.get(f"/photos/{photo_id}/preview?size=1920")

    assert resp.status_code == 500
    row = db.conn.execute(
        """SELECT (julianday('now') - julianday(working_copy_failed_at))
                  * 24 * 60 * 60 AS age_seconds
           FROM photos WHERE id=?""",
        (photo_id,),
    ).fetchone()
    assert row["age_seconds"] is not None and row["age_seconds"] > 24 * 60 * 60


def test_preview_falls_back_to_companion_on_first_raw_decode_failure(
    client_with_photo, monkeypatch,
):
    """When an edited RAW+JPEG preview has no failure marker yet,
    ``_recipe_render_source`` returns the RAW for highlight-preserving
    decode. If libraw can't decode that RAW, the preview must try the
    companion JPEG before 500ing — otherwise an unsupported RAW edit fails
    even though a usable sidecar exists. Mirrors
    serve_original_photo's edited-path companion fallback.
    """
    import os

    import image_loader
    from PIL import Image

    app, db, photo_id = client_with_photo
    folder = db.conn.execute(
        "SELECT f.path FROM photos p JOIN folders f ON f.id=p.folder_id WHERE p.id=?",
        (photo_id,),
    ).fetchone()

    raw_path = os.path.join(folder["path"], "bad.NEF")
    with open(raw_path, "wb") as f:
        f.write(b"unsupported raw")
    companion_abs = os.path.join(folder["path"], "bad.jpg")
    Image.new("RGB", (1600, 1200), (40, 90, 180)).save(
        companion_abs, "JPEG", quality=85,
    )

    db.conn.execute(
        """UPDATE photos
           SET filename='bad.NEF', extension='.nef',
               companion_path='bad.jpg',
               working_copy_path=NULL,
               width=1600, height=1200,
               working_copy_failed_at=NULL,
               working_copy_failed_mtime=NULL,
               working_copy_failed_source=NULL
           WHERE id=?""",
        (photo_id,),
    )
    db.conn.commit()
    db.set_photo_edit_recipe(photo_id, {"rotation": 90})

    original_load_image = image_loader.load_image
    loaded_paths = []

    def tracking_load_image(file_path, max_size=1024, **kwargs):
        loaded_paths.append(file_path)
        if str(file_path).lower().endswith(".nef"):
            return None
        return original_load_image(file_path, max_size=max_size, **kwargs)

    monkeypatch.setattr(image_loader, "load_image", tracking_load_image)

    client = app.test_client()
    resp = client.get(f"/photos/{photo_id}/preview?size=1920")

    assert resp.status_code == 200, resp.get_data(as_text=True)
    assert resp.mimetype == "image/jpeg"
    # RAW first (preserve-highlights), then companion as fallback.
    assert len(loaded_paths) == 2
    assert str(loaded_paths[0]).lower().endswith(".nef")
    assert str(loaded_paths[1]) == companion_abs

    # The failed RAW decode must stamp the source marker so subsequent
    # requests skip the slow RAW retry and _recipe_render_source selects
    # the companion directly.
    row = db.conn.execute(
        "SELECT working_copy_failed_source FROM photos WHERE id=?",
        (photo_id,),
    ).fetchone()
    assert row["working_copy_failed_source"] == "source"


def test_preview_falls_back_to_working_copy_on_first_raw_decode_failure(
    client_with_photo, monkeypatch,
):
    import io
    import os

    import image_loader
    from PIL import Image

    app, db, photo_id = client_with_photo
    folder = db.conn.execute(
        "SELECT f.path FROM photos p JOIN folders f ON f.id=p.folder_id WHERE p.id=?",
        (photo_id,),
    ).fetchone()
    raw_path = os.path.join(folder["path"], "DSC_7062.NEF")
    with open(raw_path, "wb") as f:
        f.write(b"unsupported raw")

    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    working_dir = os.path.join(vireo_dir, "working")
    os.makedirs(working_dir, exist_ok=True)
    wc_path = os.path.join(working_dir, f"{photo_id}.jpg")
    Image.new("RGB", (800, 600), (30, 120, 200)).save(wc_path, "JPEG")
    mtime = os.path.getmtime(raw_path)
    db.conn.execute(
        """UPDATE photos
           SET filename='DSC_7062.NEF', extension='.nef',
               companion_path=NULL,
               working_copy_path=?,
               width=800, height=600,
               file_mtime=?,
               working_copy_failed_at=NULL,
               working_copy_failed_mtime=NULL,
               working_copy_failed_source=NULL
           WHERE id=?""",
        (f"working/{photo_id}.jpg", mtime, photo_id),
    )
    db.conn.commit()
    db.set_photo_edit_recipe(
        photo_id,
        {"crop": {"x": 0, "y": 0, "w": 0.5, "h": 1}},
    )

    original_load_image = image_loader.load_image
    loaded_paths = []

    def tracking_load_image(file_path, max_size=1024, **kwargs):
        loaded_paths.append(str(file_path))
        if str(file_path).lower().endswith(".nef"):
            return None
        return original_load_image(file_path, max_size=max_size, **kwargs)

    monkeypatch.setattr(image_loader, "load_image", tracking_load_image)

    resp = app.test_client().get(f"/photos/{photo_id}/preview?size=1920")

    assert resp.status_code == 200, resp.get_data(as_text=True)
    assert [os.path.normpath(path) for path in loaded_paths] == [
        os.path.normpath(raw_path),
        os.path.normpath(wc_path),
    ]
    with Image.open(io.BytesIO(resp.data)) as img:
        assert img.size == (400, 600)
    row = db.conn.execute(
        "SELECT working_copy_failed_source FROM photos WHERE id=?",
        (photo_id,),
    ).fetchone()
    assert row["working_copy_failed_source"] == "source"


def test_preview_falls_back_to_companion_when_raw_short_edge_is_smaller(
    client_with_photo, monkeypatch,
):
    """Preview caching must compare both RAW result axes before accepting it."""
    import io
    import os

    import image_loader
    from PIL import Image

    app, db, photo_id = client_with_photo
    folder = db.conn.execute(
        "SELECT f.path FROM photos p JOIN folders f ON f.id=p.folder_id WHERE p.id=?",
        (photo_id,),
    ).fetchone()

    raw_path = os.path.join(folder["path"], "wide.NEF")
    with open(raw_path, "wb") as f:
        f.write(b"unsupported raw")
    companion_abs = os.path.join(folder["path"], "wide.jpg")
    Image.new("RGB", (6000, 4000), (40, 90, 180)).save(
        companion_abs, "JPEG", quality=85,
    )

    db.conn.execute(
        """UPDATE photos
           SET filename='wide.NEF', extension='.nef',
               companion_path='wide.jpg',
               working_copy_path=NULL,
               width=6000, height=4000,
               working_copy_failed_at=NULL,
               working_copy_failed_mtime=NULL,
               working_copy_failed_source=NULL
           WHERE id=?""",
        (photo_id,),
    )
    db.conn.commit()
    db.set_photo_edit_recipe(photo_id, {"rotation": 0})

    original_load_image = image_loader.load_image
    loaded_paths = []

    def tracking_load_image(file_path, max_size=1024, **kwargs):
        loaded_paths.append(str(file_path))
        if str(file_path).lower().endswith(".nef"):
            # Same requested long edge as the 1920px preview, but the expected
            # 3:2 short edge is 1280. A long-edge-only check would accept this.
            return Image.new("RGB", (1920, 1080), (200, 50, 50))
        return original_load_image(file_path, max_size=max_size, **kwargs)

    monkeypatch.setattr(image_loader, "load_image", tracking_load_image)

    client = app.test_client()
    resp = client.get(f"/photos/{photo_id}/preview?size=1920")

    assert resp.status_code == 200, resp.get_data(as_text=True)
    assert loaded_paths[0] == raw_path
    assert loaded_paths[1] == companion_abs
    with Image.open(io.BytesIO(resp.data)) as img:
        assert img.size == (1920, 1280)


def test_preview_keeps_raw_result_when_companion_is_smaller(
    client_with_photo, monkeypatch,
):
    """Do not replace an undersized RAW preview with a still-smaller sidecar."""
    import io
    import os

    import image_loader
    from PIL import Image

    app, db, photo_id = client_with_photo
    folder = db.conn.execute(
        "SELECT f.path FROM photos p JOIN folders f ON f.id=p.folder_id WHERE p.id=?",
        (photo_id,),
    ).fetchone()

    raw_path = os.path.join(folder["path"], "wide.NEF")
    with open(raw_path, "wb") as f:
        f.write(b"unsupported raw")
    companion_abs = os.path.join(folder["path"], "wide-small.jpg")
    Image.new("RGB", (1000, 667), (40, 90, 180)).save(
        companion_abs, "JPEG", quality=85,
    )

    db.conn.execute(
        """UPDATE photos
           SET filename='wide.NEF', extension='.nef',
               companion_path='wide-small.jpg',
               working_copy_path=NULL,
               width=6000, height=4000,
               working_copy_failed_at=NULL,
               working_copy_failed_mtime=NULL,
               working_copy_failed_source=NULL
           WHERE id=?""",
        (photo_id,),
    )
    db.conn.commit()
    db.set_photo_edit_recipe(photo_id, {"rotation": 0})

    original_load_image = image_loader.load_image
    loaded_paths = []

    def tracking_load_image(file_path, max_size=1024, **kwargs):
        loaded_paths.append(str(file_path))
        if str(file_path).lower().endswith(".nef"):
            return Image.new("RGB", (1920, 1080), (200, 50, 50))
        return original_load_image(file_path, max_size=max_size, **kwargs)

    monkeypatch.setattr(image_loader, "load_image", tracking_load_image)

    client = app.test_client()
    resp = client.get(f"/photos/{photo_id}/preview?size=1920")

    assert resp.status_code == 200, resp.get_data(as_text=True)
    assert loaded_paths[0] == raw_path
    assert loaded_paths[1] == companion_abs
    with Image.open(io.BytesIO(resp.data)) as img:
        assert img.size == (1920, 1080)


def test_original_skips_recent_failed_raw_working_copy(
    client_with_photo, monkeypatch,
):
    """The 1:1 original route should also fail fast for a current RAW
    extraction failure."""
    import image_loader

    app, db, photo_id = client_with_photo
    file_mtime = db.conn.execute(
        "SELECT file_mtime FROM photos WHERE id=?", (photo_id,)
    ).fetchone()["file_mtime"]
    db.conn.execute(
        """UPDATE photos
           SET filename='bad.NEF', extension='.nef',
               working_copy_path=NULL,
               working_copy_failed_at=datetime('now'),
               working_copy_failed_mtime=?
           WHERE id=?""",
        (file_mtime, photo_id),
    )
    db.conn.commit()

    called = {"extract": False}

    def fail_if_called(*_args, **_kwargs):
        called["extract"] = True
        raise AssertionError("original route retried failed RAW extraction")

    monkeypatch.setattr(image_loader, "extract_working_copy", fail_if_called)

    client = app.test_client()
    resp = client.get(f"/photos/{photo_id}/original")

    assert resp.status_code == 500
    assert called["extract"] is False


def test_edited_original_skips_recent_failed_raw_before_decode(
    client_with_photo, monkeypatch,
):
    """Edited originals should honor RAW failure markers before load_image."""
    import image_loader

    app, db, photo_id = client_with_photo
    db.set_photo_edit_recipe(photo_id, {"rotation": 90})
    file_mtime = db.conn.execute(
        "SELECT file_mtime FROM photos WHERE id=?", (photo_id,)
    ).fetchone()["file_mtime"]
    db.conn.execute(
        """UPDATE photos
           SET filename='bad.NEF', extension='.nef',
               working_copy_path=NULL,
               working_copy_failed_at=datetime('now'),
               working_copy_failed_mtime=?
           WHERE id=?""",
        (file_mtime, photo_id),
    )
    db.conn.commit()
    folder = db.conn.execute(
        "SELECT path FROM folders WHERE id = (SELECT folder_id FROM photos WHERE id=?)",
        (photo_id,),
    ).fetchone()
    raw_path = os.path.abspath(os.path.join(folder["path"], "bad.NEF"))
    original_load_image = image_loader.load_image

    called = {"load": False}

    def fail_if_called(file_path, *args, **kwargs):
        if os.path.abspath(os.fspath(file_path)) != raw_path:
            return original_load_image(file_path, *args, **kwargs)
        called["load"] = True
        raise AssertionError("edited original retried failed RAW decode")

    monkeypatch.setattr(image_loader, "load_image", fail_if_called)

    client = app.test_client()
    resp = client.get(f"/photos/{photo_id}/original")

    assert resp.status_code == 500
    assert called["load"] is False


def test_original_skips_recent_failed_raw_after_rejecting_small_working_copy(
    client_with_photo, monkeypatch,
):
    """If a capped working copy is too small for /original, a fresh RAW failure
    marker should still suppress another full-res extraction attempt."""
    import os

    import image_loader
    from PIL import Image

    app, db, photo_id = client_with_photo
    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    working_dir = os.path.join(vireo_dir, "working")
    os.makedirs(working_dir, exist_ok=True)
    Image.new("RGB", (100, 100), (40, 80, 120)).save(
        os.path.join(working_dir, f"{photo_id}.jpg"), "JPEG",
    )

    file_mtime = db.conn.execute(
        "SELECT file_mtime FROM photos WHERE id=?", (photo_id,)
    ).fetchone()["file_mtime"]
    db.conn.execute(
        """UPDATE photos
           SET filename='bad.NEF', extension='.nef',
               working_copy_path=?,
               working_copy_failed_at=datetime('now'),
               working_copy_failed_mtime=?
           WHERE id=?""",
        (f"working/{photo_id}.jpg", file_mtime, photo_id),
    )
    db.conn.commit()

    called = {"extract": False}

    def fail_if_called(*_args, **_kwargs):
        called["extract"] = True
        raise AssertionError("original retried RAW after small wc rejection")

    monkeypatch.setattr(image_loader, "extract_working_copy", fail_if_called)

    client = app.test_client()
    resp = client.get(f"/photos/{photo_id}/original")

    assert resp.status_code == 500
    assert called["extract"] is False


def test_original_uses_companion_after_raw_marker_and_small_working_copy(
    client_with_photo, monkeypatch,
):
    """A source RAW failure marker should not block a full-size sidecar.

    Scanner can preserve ``working_copy_failed_source='source'`` after creating
    a capped companion working copy. When /original rejects that capped working
    copy, it must upgrade from the sidecar instead of failing fast on the RAW
    marker before companion fallback is considered.
    """
    import io
    import os

    import image_loader
    from PIL import Image

    app, db, photo_id = client_with_photo
    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    folder = db.conn.execute(
        "SELECT f.path FROM photos p JOIN folders f ON f.id=p.folder_id WHERE p.id=?",
        (photo_id,),
    ).fetchone()
    folder_path = folder["path"]

    raw_path = os.path.join(folder_path, "marked.NEF")
    with open(raw_path, "wb") as f:
        f.write(b"unsupported raw")
    companion_path = os.path.join(folder_path, "marked.JPG")
    Image.new("RGB", (6000, 4000), (90, 140, 180)).save(
        companion_path, "JPEG", quality=85,
    )
    working_dir = os.path.join(vireo_dir, "working")
    os.makedirs(working_dir, exist_ok=True)
    capped_wc_rel = f"working/{photo_id}.jpg"
    Image.new("RGB", (1000, 667), (20, 40, 60)).save(
        os.path.join(vireo_dir, capped_wc_rel), "JPEG", quality=85,
    )

    db.conn.execute(
        """UPDATE photos
           SET filename='marked.NEF', extension='.nef',
               companion_path='marked.JPG',
               working_copy_path=?,
               working_copy_failed_at=datetime('now'),
               working_copy_failed_mtime=file_mtime,
               working_copy_failed_source='source',
               width=6000, height=4000
           WHERE id=?""",
        (capped_wc_rel, photo_id),
    )
    db.conn.commit()

    real_extract = image_loader.extract_working_copy
    extracted_sources = []

    def tracking_extract(source, output, *args, **kwargs):
        extracted_sources.append(str(source))
        if str(source).lower().endswith(".nef"):
            raise AssertionError("original route retried source-failed RAW")
        return real_extract(source, output, *args, **kwargs)

    monkeypatch.setattr(image_loader, "extract_working_copy", tracking_extract)

    client = app.test_client()
    resp = client.get(f"/photos/{photo_id}/original")

    assert resp.status_code == 200, resp.get_data(as_text=True)
    assert extracted_sources == [companion_path]
    with Image.open(io.BytesIO(resp.data)) as img:
        assert img.size == (6000, 4000)


def test_original_refreshes_failure_marker_when_stale_retry_fails(
    client_with_photo, monkeypatch,
):
    """A stale RAW failure may retry once; if original extraction and fallback
    loading still fail, the route should refresh the failure marker."""
    import os

    import image_loader

    app, db, photo_id = client_with_photo
    folder = db.conn.execute(
        "SELECT f.path FROM photos p JOIN folders f ON f.id=p.folder_id WHERE p.id=?",
        (photo_id,),
    ).fetchone()
    with open(os.path.join(folder["path"], "bad.NEF"), "wb") as f:
        f.write(b"not a decodable raw")
    file_mtime = db.conn.execute(
        "SELECT file_mtime FROM photos WHERE id=?", (photo_id,)
    ).fetchone()["file_mtime"]
    db.conn.execute(
        """UPDATE photos
           SET filename='bad.NEF', extension='.nef',
               working_copy_path=NULL,
               working_copy_failed_at=datetime('now', '-48 hours'),
               working_copy_failed_mtime=?
           WHERE id=?""",
        (file_mtime, photo_id),
    )
    db.conn.commit()

    monkeypatch.setattr(image_loader, "extract_working_copy", lambda *a, **k: False)
    monkeypatch.setattr(image_loader, "load_image", lambda *a, **k: None)

    client = app.test_client()
    resp = client.get(f"/photos/{photo_id}/original")

    assert resp.status_code == 500
    row = db.conn.execute(
        """SELECT working_copy_failed_mtime,
                  (julianday('now') - julianday(working_copy_failed_at))
                  * 24 * 60 * 60 AS age_seconds
           FROM photos WHERE id=?""",
        (photo_id,),
    ).fetchone()
    assert row["working_copy_failed_mtime"] == file_mtime
    assert row["age_seconds"] is not None and row["age_seconds"] < 60


def test_original_records_raw_failure_before_working_copy_fallback(
    client_with_photo, monkeypatch,
):
    """A usable fallback must not leave repeated RAW retries unthrottled."""
    import os

    import image_loader
    from PIL import Image

    app, db, photo_id = client_with_photo
    folder_path = db.conn.execute(
        "SELECT f.path FROM photos p JOIN folders f ON f.id=p.folder_id "
        "WHERE p.id=?",
        (photo_id,),
    ).fetchone()["path"]
    raw_path = os.path.join(folder_path, "failed.NEF")
    with open(raw_path, "wb") as raw_file:
        raw_file.write(b"unsupported raw")

    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    working_dir = os.path.join(vireo_dir, "working")
    os.makedirs(working_dir, exist_ok=True)
    working_path = os.path.join(working_dir, f"{photo_id}.jpg")
    Image.new("RGB", (800, 600), (40, 80, 120)).save(working_path, "JPEG")
    working_rel = f"working/{photo_id}.jpg"
    file_mtime = os.path.getmtime(raw_path)
    db.conn.execute(
        """UPDATE photos
           SET filename='failed.NEF', extension='.nef',
               width=800, height=600, file_mtime=?,
               working_copy_path=?, working_copy_failed_at=NULL,
               working_copy_failed_mtime=NULL,
               working_copy_failed_source=NULL
           WHERE id=?""",
        (file_mtime, working_rel, photo_id),
    )
    db.conn.commit()

    monkeypatch.setattr(image_loader, "extract_working_copy", lambda *a, **k: False)
    monkeypatch.setattr(image_loader, "load_image", lambda *a, **k: None)

    response = app.test_client().get(f"/photos/{photo_id}/original")

    assert response.status_code == 200
    with open(working_path, "rb") as working_file:
        assert response.data == working_file.read()
    row = db.conn.execute(
        """SELECT working_copy_failed_at, working_copy_failed_mtime,
                  working_copy_failed_source
           FROM photos WHERE id=?""",
        (photo_id,),
    ).fetchone()
    assert row["working_copy_failed_at"] is not None
    assert row["working_copy_failed_mtime"] == file_mtime
    assert row["working_copy_failed_source"] == "source"


def test_original_uses_trusted_copy_when_raw_display_is_undersized(
    client_with_photo, monkeypatch,
):
    """A preview-sized embedded JPEG must not become the 1:1 display cache."""
    import os

    import image_loader
    from PIL import Image

    app, db, photo_id = client_with_photo
    folder_path = db.conn.execute(
        "SELECT f.path FROM photos p JOIN folders f ON f.id=p.folder_id "
        "WHERE p.id=?",
        (photo_id,),
    ).fetchone()["path"]
    raw_path = os.path.join(folder_path, "embedded.NEF")
    with open(raw_path, "wb") as raw_file:
        raw_file.write(b"unsupported raw")

    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    working_dir = os.path.join(vireo_dir, "working")
    os.makedirs(working_dir, exist_ok=True)
    working_rel = f"working/{photo_id}.jpg"
    working_path = os.path.join(vireo_dir, working_rel)
    Image.new("RGB", (800, 600), (40, 80, 120)).save(working_path, "JPEG")
    file_mtime = os.path.getmtime(raw_path)
    db.conn.execute(
        """UPDATE photos
           SET filename='embedded.NEF', extension='.nef',
               width=800, height=600, file_mtime=?,
               companion_path=NULL, working_copy_path=?,
               working_copy_failed_at=NULL,
               working_copy_failed_mtime=NULL,
               working_copy_failed_source=NULL
           WHERE id=?""",
        (file_mtime, working_rel, photo_id),
    )
    db.conn.commit()

    extract_calls = []

    def extract_embedded_preview(source, output, *args, **kwargs):
        extract_calls.append(str(source))
        Image.new("RGB", (320, 240), (180, 90, 40)).save(output, "JPEG")
        return True

    monkeypatch.setattr(
        image_loader, "extract_working_copy", extract_embedded_preview,
    )

    client = app.test_client()
    first = client.get(f"/photos/{photo_id}/original")
    second = client.get(f"/photos/{photo_id}/original")

    with open(working_path, "rb") as working_file:
        working_bytes = working_file.read()
    assert first.status_code == 200
    assert second.status_code == 200
    assert first.data == working_bytes
    assert second.data == working_bytes
    assert extract_calls == [raw_path]
    assert not os.path.exists(
        os.path.join(vireo_dir, "originals", f"{photo_id}.display.jpg")
    )
    row = db.conn.execute(
        """SELECT working_copy_failed_mtime, working_copy_failed_source
           FROM photos WHERE id=?""",
        (photo_id,),
    ).fetchone()
    assert row["working_copy_failed_mtime"] == file_mtime
    assert row["working_copy_failed_source"] == "source"


def test_original_falls_back_to_companion_when_raw_extraction_fails(
    client_with_photo, monkeypatch,
):
    """For a RAW+JPEG row whose RAW libraw can't decode, /photos/<id>/original
    must use the full-size companion JPEG instead of returning 500.
    _full_res_companion_path() already confirms a usable sidecar; bailing out
    when the RAW alone fails throws away a request the system can satisfy.
    """
    import os

    import image_loader
    from PIL import Image

    app, db, photo_id = client_with_photo
    folder_row = db.conn.execute(
        "SELECT f.path FROM photos p JOIN folders f ON f.id=p.folder_id WHERE p.id=?",
        (photo_id,),
    ).fetchone()
    folder_path = folder_row["path"]

    # Replace the JPEG with a RAW+JPEG pair so the row looks like RAW+companion.
    raw_path = os.path.join(folder_path, "bad.NEF")
    with open(raw_path, "wb") as f:
        f.write(b"unsupported raw")
    companion_path = os.path.join(folder_path, "bad.JPG")
    Image.new("RGB", (1600, 1200), (90, 140, 180)).save(
        companion_path, "JPEG", quality=85,
    )

    db.conn.execute(
        """UPDATE photos
           SET filename='bad.NEF', extension='.nef',
               companion_path=?,
               working_copy_path=NULL,
               working_copy_failed_at=NULL,
               working_copy_failed_mtime=NULL,
               working_copy_failed_source=NULL
           WHERE id=?""",
        ("bad.JPG", photo_id),
    )
    db.conn.commit()

    real_extract = image_loader.extract_working_copy

    def extract_only_companion(source, output, *args, **kwargs):
        if source.lower().endswith(".nef"):
            return False
        return real_extract(source, output, *args, **kwargs)

    monkeypatch.setattr(image_loader, "extract_working_copy", extract_only_companion)

    client = app.test_client()
    resp = client.get(f"/photos/{photo_id}/original")

    assert resp.status_code == 200, resp.get_data(as_text=True)
    assert resp.mimetype == "image/jpeg"


def test_unedited_raw_original_prefers_near_full_companion(
    client_with_photo, monkeypatch,
):
    """A near-full companion is already the camera-rendered display source.

    Prefer it before decoding the RAW so cameras whose embedded preview is
    just shy of sensor dimensions do not fall through to a differently toned
    libraw demosaic.
    """
    import io
    import os

    import image_loader
    from PIL import Image

    app, db, photo_id = client_with_photo
    folder_row = db.conn.execute(
        "SELECT f.path FROM photos p JOIN folders f ON f.id=p.folder_id WHERE p.id=?",
        (photo_id,),
    ).fetchone()
    folder_path = folder_row["path"]

    raw_path = os.path.join(folder_path, "source.NEF")
    with open(raw_path, "wb") as f:
        f.write(b"unsupported raw")
    companion_path = os.path.join(folder_path, "source.JPG")
    Image.new("RGB", (5976, 3984), (90, 140, 180)).save(
        companion_path, "JPEG", quality=85,
    )

    db.conn.execute(
        """UPDATE photos
           SET filename='source.NEF', extension='.nef',
               companion_path='source.JPG',
               working_copy_path=NULL,
               working_copy_failed_at=NULL,
               working_copy_failed_mtime=NULL,
               working_copy_failed_source=NULL,
               width=6000, height=4000
           WHERE id=?""",
        (photo_id,),
    )
    db.conn.commit()

    real_extract = image_loader.extract_working_copy
    extract_calls = []

    def track_companion_extract(source, output, *args, **kwargs):
        extract_calls.append(str(source))
        if str(source).lower().endswith(".nef"):
            raise AssertionError("unedited display should prefer companion JPEG")
        return real_extract(source, output, *args, **kwargs)

    monkeypatch.setattr(image_loader, "extract_working_copy", track_companion_extract)

    client = app.test_client()
    resp = client.get(f"/photos/{photo_id}/original")

    assert resp.status_code == 200, resp.get_data(as_text=True)
    assert resp.mimetype == "image/jpeg"
    assert extract_calls == [companion_path]
    # The persisted display cache must be the near-full companion render.
    with Image.open(io.BytesIO(resp.data)) as img:
        assert max(img.size) >= 5400


def test_edited_original_uses_companion_when_raw_returns_undersized(
    client_with_photo, monkeypatch,
):
    """For edited RAW originals, ``load_image`` can return a small
    embedded JPEG when ``rawpy.postprocess`` fails — the
    ``preserve_highlights`` mode still falls back to the embedded
    thumb. The endpoint must compare the loaded dimensions against
    the photo's stored full-resolution dimensions and try the
    companion JPEG before caching an undersized full-resolution render.
    """
    import io
    import os

    import image_loader
    from PIL import Image

    app, db, photo_id = client_with_photo
    folder_row = db.conn.execute(
        "SELECT f.path FROM photos p JOIN folders f ON f.id=p.folder_id WHERE p.id=?",
        (photo_id,),
    ).fetchone()
    folder_path = folder_row["path"]

    raw_path = os.path.join(folder_path, "edit.NEF")
    with open(raw_path, "wb") as f:
        f.write(b"unsupported raw")
    companion_path = os.path.join(folder_path, "edit.JPG")
    Image.new("RGB", (6000, 4000), (90, 140, 180)).save(
        companion_path, "JPEG", quality=85,
    )

    db.conn.execute(
        """UPDATE photos
           SET filename='edit.NEF', extension='.nef',
               companion_path='edit.JPG',
               working_copy_path=NULL,
               working_copy_failed_at=NULL,
               working_copy_failed_mtime=NULL,
               working_copy_failed_source=NULL,
               width=6000, height=4000
           WHERE id=?""",
        (photo_id,),
    )
    db.conn.commit()
    # An edit recipe routes the request through the highlight-preserving
    # decode path that the new size-validation guards.
    db.set_photo_edit_recipe(photo_id, {"rotation": 90})

    load_calls = []

    def fake_load_image(file_path, max_size=1024, **kwargs):
        load_calls.append((str(file_path), kwargs))
        if str(file_path).lower().endswith(".nef"):
            # Stand in for `_load_raw` returning an undersized embedded
            # preview when libraw can't demosaic the sensor data.
            return Image.new("RGB", (1600, 1067), (200, 50, 50))
        return Image.new("RGB", (6000, 4000), (90, 140, 180))

    monkeypatch.setattr(image_loader, "load_image", fake_load_image)
    # The endpoint imports load_image into the local module namespace too.
    import app as app_module
    monkeypatch.setattr(app_module, "load_image", fake_load_image, raising=False)

    client = app.test_client()
    resp = client.get(f"/photos/{photo_id}/original")

    assert resp.status_code == 200, resp.get_data(as_text=True)
    # The RAW was attempted first, then the companion JPEG after the
    # undersized result was detected.
    paths = [str(p[0]).lower() for p in load_calls]
    assert any(p.endswith(".nef") for p in paths)
    assert any(p.endswith(".jpg") for p in paths)
    # The cached original must be the full-size companion render.
    with Image.open(io.BytesIO(resp.data)) as img:
        assert max(img.size) >= 5400


def test_eviction_removes_oldest_files_when_over_quota(tmp_path, monkeypatch):
    """When writes push cache over quota, oldest-accessed entries are evicted."""
    import os
    import time

    import config as cfg
    from app import create_app
    from db import Database
    from PIL import Image

    # Custom fixture with TWO photos because we need to race two writes.
    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")
    # Quota of 0 MB → eviction should clear the cache after each write.
    cfg.save({**cfg.DEFAULTS, "preview_cache_max_mb": 0})

    photos_dir = tmp_path / "photos"
    photos_dir.mkdir()
    src1 = photos_dir / "a.jpg"
    src2 = photos_dir / "b.jpg"
    Image.new("RGB", (800, 600), (180, 90, 40)).save(str(src1), "JPEG")
    Image.new("RGB", (800, 600), (40, 180, 90)).save(str(src2), "JPEG")

    vireo_dir = tmp_path / "vireo"
    vireo_dir.mkdir()
    thumb_dir = vireo_dir / "thumbnails"
    thumb_dir.mkdir()
    db_path = str(vireo_dir / "vireo.db")

    db = Database(db_path)
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    fid = db.add_folder(str(photos_dir), name="photos")
    pid1 = db.add_photo(
        folder_id=fid, filename="a.jpg", extension=".jpg",
        file_size=os.path.getsize(src1), file_mtime=os.path.getmtime(src1),
        width=800, height=600,
    )
    pid2 = db.add_photo(
        folder_id=fid, filename="b.jpg", extension=".jpg",
        file_size=os.path.getsize(src2), file_mtime=os.path.getmtime(src2),
        width=800, height=600,
    )

    app = create_app(db_path=db_path, thumb_cache_dir=str(thumb_dir))
    client = app.test_client()

    client.get(f"/photos/{pid1}/preview?size=1920")
    time.sleep(0.05)
    client.get(f"/photos/{pid2}/preview?size=1920")

    # Quota is 0 MB so after each write eviction drains everything.
    assert db.preview_cache_total_bytes() == 0
    preview_dir = vireo_dir / "previews"
    assert not (preview_dir / f"{pid1}_1920.jpg").exists()
    assert not (preview_dir / f"{pid2}_1920.jpg").exists()


def test_reconcile_drops_ghost_rows_when_files_missing(tmp_path):
    """Rows whose on-disk files no longer exist must be removed so
    accounting doesn't keep eviction asleep on phantom bytes."""
    from db import Database
    from preview_cache import reconcile_preview_cache

    vireo_dir = tmp_path / "vireo"
    vireo_dir.mkdir()
    (vireo_dir / "previews").mkdir()
    db = Database(str(vireo_dir / "vireo.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    fid = db.add_folder(str(tmp_path / "src"), name="src")
    pid = db.add_photo(folder_id=fid, filename="a.jpg", extension=".jpg",
                       file_size=1, file_mtime=1.0)

    # Row exists in table but no file on disk — exactly the drift state
    # observed in production (DB says ~2 GB tracked, disk has 0 bytes).
    db.preview_cache_insert(pid, 1920, 450_000)
    assert db.preview_cache_total_bytes() == 450_000

    dropped = reconcile_preview_cache(db, str(vireo_dir))
    assert dropped == 1
    assert db.preview_cache_total_bytes() == 0
    assert db.preview_cache_get(pid, 1920) is None


def test_reconcile_keeps_live_rows(tmp_path):
    """Rows whose on-disk file exists must survive reconcile."""
    from db import Database
    from PIL import Image
    from preview_cache import reconcile_preview_cache

    vireo_dir = tmp_path / "vireo"
    vireo_dir.mkdir()
    preview_dir = vireo_dir / "previews"
    preview_dir.mkdir()
    db = Database(str(vireo_dir / "vireo.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    fid = db.add_folder(str(tmp_path / "src"), name="src")
    pid = db.add_photo(folder_id=fid, filename="a.jpg", extension=".jpg",
                       file_size=1, file_mtime=1.0)

    preview_file = preview_dir / f"{pid}_1920.jpg"
    Image.new("RGB", (1920, 1280), (10, 20, 30)).save(str(preview_file), "JPEG")
    db.preview_cache_insert(pid, 1920, preview_file.stat().st_size)

    dropped = reconcile_preview_cache(db, str(vireo_dir))
    assert dropped == 0
    assert db.preview_cache_get(pid, 1920) is not None


def test_startup_reconciles_before_eviction(tmp_path, monkeypatch):
    """create_app must drop ghost rows so eviction sees real totals.

    Regression for the production drift: 2 GB of phantom rows kept
    eviction asleep while the previews dir was empty, and every
    pipeline run paid the full RAW-decode cost for previews."""
    import config as cfg
    import models
    from app import create_app
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    monkeypatch.setattr(models, "DEFAULT_MODELS_DIR", str(tmp_path / "vm"))
    monkeypatch.setattr(models, "CONFIG_PATH", str(tmp_path / "models.json"))

    vireo_dir = tmp_path / "vireo"
    vireo_dir.mkdir()
    (vireo_dir / "previews").mkdir()
    thumb_dir = vireo_dir / "thumbnails"
    thumb_dir.mkdir()
    db_path = str(vireo_dir / "vireo.db")

    db = Database(db_path)
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    fid = db.add_folder(str(tmp_path / "src"), name="src")
    pid = db.add_photo(folder_id=fid, filename="a.jpg", extension=".jpg",
                       file_size=1, file_mtime=1.0)

    # Seed a ghost row: bytes claimed but no file behind it.
    db.preview_cache_insert(pid, 1920, 450_000)
    db.close()

    create_app(db_path=db_path, thumb_cache_dir=str(thumb_dir),
               api_token="test-token-123")

    db2 = Database(db_path)
    assert db2.preview_cache_total_bytes() == 0, (
        "Startup must reconcile ghost rows so eviction operates on "
        "real bytes, not phantom accounting."
    )
    db2.close()


def test_preview_cache_endpoint_returns_recommended_mb(client_with_photo):
    """/api/preview-cache exposes a recommendation so the UI can warn
    when the configured cap is smaller than the photo library."""
    app, db, photo_id = client_with_photo
    client = app.test_client()
    # No measured rows yet → uses the 500 KB-per-preview fallback.
    resp = client.get("/api/preview-cache")
    assert resp.status_code == 200
    data = resp.get_json()
    assert "recommended_mb" in data
    assert data["recommended_mb"] >= 1


def test_recommendation_uses_measured_avg_when_rows_exist(client_with_photo):
    """Recommendation scales with the actually-observed preview size."""
    app, db, photo_id = client_with_photo
    client = app.test_client()
    # Generate a real preview to populate the table with a measured size.
    client.get(f"/photos/{photo_id}/preview?size=1920")
    resp = client.get("/api/preview-cache")
    data = resp.get_json()
    # 1 photo × measured avg should round to a small positive MB value.
    # The exact value depends on JPEG output; just assert sane bounds.
    assert 1 <= data["recommended_mb"] <= 10


def test_preview_cache_endpoint_uses_db(client_with_photo):
    """/api/preview-cache returns totals from preview_cache table, not filesystem."""
    app, db, photo_id = client_with_photo
    client = app.test_client()
    client.get(f"/photos/{photo_id}/preview?size=1920")

    resp = client.get("/api/preview-cache")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["count"] == 1
    assert data["total_size"] > 0
    assert "quota_bytes" in data


def test_preview_cache_clear_removes_all(client_with_photo):
    """POST /api/preview-cache/clear empties the table and files."""
    import os
    app, db, photo_id = client_with_photo
    client = app.test_client()
    client.get(f"/photos/{photo_id}/preview?size=1920")
    assert db.preview_cache_total_bytes() > 0

    resp = client.post("/api/preview-cache/clear")
    assert resp.status_code == 200
    data = resp.get_json()
    assert "files_removed" in data

    assert db.preview_cache_total_bytes() == 0
    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    assert not os.path.exists(
        os.path.join(vireo_dir, "previews", f"{photo_id}_1920.jpg")
    )


def test_preview_serves_bytes_when_quota_is_zero(client_with_photo, monkeypatch):
    """With preview_cache_max_mb=0, the preview response body is non-empty
    even though eviction runs immediately after generation."""
    import config as cfg
    monkeypatch.setattr(
        cfg, "load",
        lambda: {**cfg.DEFAULTS, "preview_cache_max_mb": 0},
    )
    app, db, photo_id = client_with_photo
    client = app.test_client()
    resp = client.get(f"/photos/{photo_id}/preview?size=1920")
    assert resp.status_code == 200
    assert len(resp.data) > 100  # real JPEG, not empty
    # Eviction clears the table + file
    assert db.preview_cache_total_bytes() == 0


def test_legacy_full_cache_files_are_migrated_at_startup(tmp_path, monkeypatch):
    """Pre-refactor /full cache files ({id}.jpg) get renamed to
    {id}_{preview_max_size}.jpg and inserted into preview_cache on
    app startup so they're visible to accounting and eviction."""
    import os

    import config as cfg
    from app import create_app
    from db import Database
    from PIL import Image

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")
    cfg.save({**cfg.DEFAULTS, "preview_max_size": 1920})

    photos_dir = tmp_path / "photos"
    photos_dir.mkdir()
    src = photos_dir / "test.jpg"
    Image.new("RGB", (800, 600), (180, 90, 40)).save(str(src), "JPEG", quality=85)

    vireo_dir = tmp_path / "vireo"
    thumb_dir = vireo_dir / "thumbnails"
    thumb_dir.mkdir(parents=True)
    db_path = str(vireo_dir / "vireo.db")

    # Set up a photo and a pre-existing legacy preview file.
    db = Database(db_path)
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    fid = db.add_folder(str(photos_dir), name="photos")
    pid = db.add_photo(
        folder_id=fid, filename="test.jpg", extension=".jpg",
        file_size=os.path.getsize(src), file_mtime=os.path.getmtime(src),
        width=800, height=600,
    )

    preview_dir = vireo_dir / "previews"
    preview_dir.mkdir()
    legacy = preview_dir / f"{pid}.jpg"
    with open(legacy, "wb") as f:
        f.write(b"\xff\xd8\xff\xe0" + b"x" * 2048)

    # Creating the app triggers the migration.
    create_app(db_path=db_path, thumb_cache_dir=str(thumb_dir))

    # Legacy file renamed, new sized file exists, row inserted.
    assert not legacy.exists()
    new_path = preview_dir / f"{pid}_1920.jpg"
    assert new_path.exists()
    row = db.preview_cache_get(pid, 1920)
    assert row is not None
    assert row["bytes"] == os.path.getsize(new_path)


def test_preview_job_writes_sized_filename_and_tracks(client_with_photo):
    """The /api/jobs/previews precompute writes {id}_{size}.jpg and
    inserts a preview_cache row, not the legacy {id}.jpg path."""
    import os
    import time

    app, db, photo_id = client_with_photo
    client = app.test_client()
    resp = client.post("/api/jobs/previews", json={})
    assert resp.status_code == 200
    job_id = resp.get_json()["job_id"]

    # Poll until the job finishes (it's a single-photo fixture, so fast).
    deadline = time.time() + 10
    while time.time() < deadline:
        status_resp = client.get(f"/api/jobs/{job_id}")
        if status_resp.status_code != 200:
            time.sleep(0.05)
            continue
        data = status_resp.get_json()
        if data.get("status") in ("completed", "failed"):
            break
        time.sleep(0.05)
    assert data["status"] == "completed"

    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    preview_dir = os.path.join(vireo_dir, "previews")

    # New naming + tracked
    assert os.path.exists(os.path.join(preview_dir, f"{photo_id}_1920.jpg"))
    assert db.preview_cache_get(photo_id, 1920) is not None

    # Legacy naming NOT produced
    assert not os.path.exists(os.path.join(preview_dir, f"{photo_id}.jpg"))


def test_preview_job_applies_edit_recipe_to_warmed_file(
    client_with_photo, monkeypatch,
):
    import os
    import time

    import image_loader
    from PIL import Image

    app, db, photo_id = client_with_photo
    client = app.test_client()
    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    preview_dir = os.path.join(vireo_dir, "previews")
    preview_path = os.path.join(preview_dir, f"{photo_id}_1920.jpg")
    db.set_photo_edit_recipe(photo_id, {"rotation": 90})
    original_load_image = image_loader.load_image
    seen_max_sizes = []

    def tracking_load_image(file_path, max_size=1024):
        seen_max_sizes.append(max_size)
        return original_load_image(file_path, max_size=max_size)

    monkeypatch.setattr(image_loader, "load_image", tracking_load_image)

    resp = client.post("/api/jobs/previews", json={})
    assert resp.status_code == 200
    job_id = resp.get_json()["job_id"]

    deadline = time.time() + 10
    while time.time() < deadline:
        status_resp = client.get(f"/api/jobs/{job_id}")
        if status_resp.status_code != 200:
            time.sleep(0.05)
            continue
        data = status_resp.get_json()
        if data.get("status") in ("completed", "failed"):
            break
        time.sleep(0.05)
    assert data["status"] == "completed"

    assert seen_max_sizes == [1920]
    assert db.preview_cache_get(photo_id, 1920) is not None
    with Image.open(preview_path) as img:
        assert img.size == (600, 800)


def test_preview_job_honors_raw_failure_marker_after_source_selection(
    client_with_photo, monkeypatch,
):
    import os
    import time

    import image_loader

    app, db, photo_id = client_with_photo
    client = app.test_client()
    db.conn.execute(
        """UPDATE photos
           SET filename='source.NEF', extension='.nef',
               companion_path=NULL,
               working_copy_path=NULL,
               width=800, height=600,
               file_mtime=1234.0,
               working_copy_failed_at=datetime('now'),
               working_copy_failed_mtime=1234.0,
               working_copy_failed_source='source'
           WHERE id=?""",
        (photo_id,),
    )
    db.conn.commit()
    db.set_photo_edit_recipe(
        photo_id,
        {"crop": {"x": 0, "y": 0, "w": 0.5, "h": 1}},
    )
    folder = db.conn.execute(
        "SELECT path FROM folders WHERE id = (SELECT folder_id FROM photos WHERE id=?)",
        (photo_id,),
    ).fetchone()
    raw_path = os.path.join(folder["path"], "source.NEF")
    original_load_image = image_loader.load_image
    raw_loads = []

    def tracking_load_image(file_path, max_size=1024):
        if os.path.abspath(str(file_path)) == os.path.abspath(raw_path):
            raw_loads.append(file_path)
            raise AssertionError("preview warmup retried failed RAW")
        return original_load_image(file_path, max_size=max_size)

    monkeypatch.setattr(image_loader, "load_image", tracking_load_image)

    resp = client.post("/api/jobs/previews", json={})
    assert resp.status_code == 200
    job_id = resp.get_json()["job_id"]

    deadline = time.time() + 10
    while time.time() < deadline:
        status_resp = client.get(f"/api/jobs/{job_id}")
        if status_resp.status_code != 200:
            time.sleep(0.05)
            continue
        data = status_resp.get_json()
        if data.get("status") in ("completed", "failed"):
            break
        time.sleep(0.05)

    assert data["status"] == "completed"
    assert raw_loads == []
    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    assert db.preview_cache_get(photo_id, 1920) is None
    assert not os.path.exists(
        os.path.join(vireo_dir, "previews", f"{photo_id}_1920.jpg"),
    )


def test_preview_job_warms_unedited_raw_from_camera_rendered_source(
    client_with_photo, monkeypatch,
):
    """Precompute must warm from the RAW source, not the working copy — otherwise
    the tracked preview cache locks in the highlight-preserving dark render and
    /photos/<id>/preview returns those cache hits before its own RAW-source
    branch ever runs."""
    import os
    import time

    import image_loader
    from PIL import Image

    app, db, photo_id = client_with_photo
    client = app.test_client()
    folder = db.conn.execute(
        "SELECT path FROM folders WHERE id = (SELECT folder_id FROM photos WHERE id=?)",
        (photo_id,),
    ).fetchone()
    raw_path = os.path.join(folder["path"], "source.NEF")
    with open(raw_path, "wb") as raw_file:
        raw_file.write(b"raw bytes decoded by the test double")

    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    working_dir = os.path.join(vireo_dir, "working")
    os.makedirs(working_dir, exist_ok=True)
    working_path = os.path.join(working_dir, f"{photo_id}.jpg")
    Image.new("RGB", (800, 600), (25, 25, 25)).save(working_path, "JPEG")
    db.conn.execute(
        """UPDATE photos
           SET filename='source.NEF', extension='.nef',
               companion_path=NULL,
               working_copy_path=?, width=800, height=600
           WHERE id=?""",
        (f"working/{photo_id}.jpg", photo_id),
    )
    db.conn.commit()

    loaded = []

    def tracking_load(path, max_size=1024, **kwargs):
        loaded.append(os.fspath(path))
        color = (220, 220, 220) if os.fspath(path) == raw_path else (25, 25, 25)
        return Image.new("RGB", (800, 600), color)

    monkeypatch.setattr(image_loader, "load_image", tracking_load)

    resp = client.post("/api/jobs/previews", json={})
    assert resp.status_code == 200
    job_id = resp.get_json()["job_id"]

    deadline = time.time() + 10
    while time.time() < deadline:
        status_resp = client.get(f"/api/jobs/{job_id}")
        if status_resp.status_code != 200:
            time.sleep(0.05)
            continue
        data = status_resp.get_json()
        if data.get("status") in ("completed", "failed"):
            break
        time.sleep(0.05)

    assert data["status"] == "completed"
    # The warmer decoded the RAW source, not the dark working copy.
    assert raw_path in loaded
    assert working_path not in loaded
    preview_path = os.path.join(
        vireo_dir, "previews", f"{photo_id}_1920.jpg",
    )
    assert os.path.exists(preview_path)
    with Image.open(preview_path) as warmed:
        # Camera-rendered brightness, not the flat/dark working-copy tone.
        assert warmed.getpixel((400, 300))[0] > 200


def test_preview_job_does_not_adopt_untracked_edited_preview_after_unlink_failure(
    client_with_photo, monkeypatch,
):
    import os
    import time

    import app as app_module

    app, db, photo_id = client_with_photo
    client = app.test_client()
    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    preview_dir = os.path.join(vireo_dir, "previews")
    os.makedirs(preview_dir, exist_ok=True)
    preview_path = os.path.join(preview_dir, f"{photo_id}_1920.jpg")
    db.set_photo_edit_recipe(photo_id, {"rotation": 90})
    with open(preview_path, "wb") as f:
        f.write(b"stale-preview")
    assert db.preview_cache_get(photo_id, 1920) is None

    original_remove = app_module.os.remove

    def locked_remove(path):
        if os.path.abspath(path) == os.path.abspath(preview_path):
            raise OSError("locked")
        return original_remove(path)

    monkeypatch.setattr(app_module.os, "remove", locked_remove)

    resp = client.post("/api/jobs/previews", json={})
    assert resp.status_code == 200
    job_id = resp.get_json()["job_id"]

    deadline = time.time() + 10
    while time.time() < deadline:
        status_resp = client.get(f"/api/jobs/{job_id}")
        if status_resp.status_code != 200:
            time.sleep(0.05)
            continue
        data = status_resp.get_json()
        if data.get("status") in ("completed", "failed"):
            break
        time.sleep(0.05)
    assert data["status"] == "completed"

    assert os.path.exists(preview_path)
    assert db.preview_cache_get(photo_id, 1920) is None


def test_preview_job_uses_detail_row_exif_for_cropped_source_selection(
    client_with_photo, monkeypatch,
):
    import json
    import os
    import time

    import image_loader
    from PIL import Image

    app, db, photo_id = client_with_photo
    client = app.test_client()
    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    working_dir = os.path.join(vireo_dir, "working")
    os.makedirs(working_dir, exist_ok=True)
    working_path = os.path.join(working_dir, f"{photo_id}.jpg")
    Image.new("RGB", (400, 400), "blue").save(working_path)
    folder = db.conn.execute(
        "SELECT path FROM folders WHERE id = (SELECT folder_id FROM photos WHERE id=?)",
        (photo_id,),
    ).fetchone()
    original_path = os.path.join(folder["path"], "test.jpg")
    db.conn.execute(
        """UPDATE photos
           SET width=600, height=400, exif_data=?, working_copy_path=?
           WHERE id=?""",
        (
            json.dumps({"EXIF": {"Orientation": 6}}),
            f"working/{photo_id}.jpg",
            photo_id,
        ),
    )
    db.conn.commit()
    db.set_photo_edit_recipe(
        photo_id,
        {"crop": {"x": 0, "y": 0, "w": 0.5, "h": 1}},
    )
    loaded_paths = []

    def tracking_load_image(file_path, max_size=1024):
        loaded_paths.append(os.path.abspath(str(file_path)))
        return Image.new("RGB", (600, 400), "red")

    monkeypatch.setattr(image_loader, "load_image", tracking_load_image)

    resp = client.post("/api/jobs/previews", json={})
    assert resp.status_code == 200
    job_id = resp.get_json()["job_id"]

    deadline = time.time() + 10
    while time.time() < deadline:
        status_resp = client.get(f"/api/jobs/{job_id}")
        if status_resp.status_code != 200:
            time.sleep(0.05)
            continue
        data = status_resp.get_json()
        if data.get("status") in ("completed", "failed"):
            break
        time.sleep(0.05)
    assert data["status"] == "completed"

    source_dir = os.path.abspath(os.path.dirname(original_path))
    relevant_paths = [
        path for path in loaded_paths
        if os.path.commonpath([source_dir, path]) == source_dir
    ]
    assert relevant_paths == [os.path.abspath(original_path)]
    assert db.preview_cache_get(photo_id, 1920) is not None


def test_preview_job_preserves_existing_edited_preview_when_source_missing(
    client_with_photo,
):
    import os
    import time

    from PIL import Image

    app, db, photo_id = client_with_photo
    client = app.test_client()
    db.set_photo_edit_recipe(photo_id, {"rotation": 90})
    rendered = client.get(f"/photos/{photo_id}/preview?size=1920")
    assert rendered.status_code == 200

    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    preview_path = os.path.join(vireo_dir, "previews", f"{photo_id}_1920.jpg")
    assert os.path.exists(preview_path)
    folder = db.conn.execute(
        "SELECT f.path, p.filename FROM photos p JOIN folders f ON f.id=p.folder_id WHERE p.id=?",
        (photo_id,),
    ).fetchone()
    os.remove(os.path.join(folder["path"], folder["filename"]))

    resp = client.post("/api/jobs/previews", json={})
    assert resp.status_code == 200
    job_id = resp.get_json()["job_id"]

    deadline = time.time() + 10
    while time.time() < deadline:
        status_resp = client.get(f"/api/jobs/{job_id}")
        if status_resp.status_code != 200:
            time.sleep(0.05)
            continue
        data = status_resp.get_json()
        if data.get("status") in ("completed", "failed"):
            break
        time.sleep(0.05)
    assert data["status"] == "completed"

    assert db.preview_cache_get(photo_id, 1920) is not None
    with Image.open(preview_path) as img:
        assert img.size == (600, 800)


def test_eviction_keeps_row_when_unlink_fails(client_with_photo, monkeypatch):
    """If os.remove raises OSError (not FileNotFoundError), the preview_cache
    row is kept so future passes can retry instead of leaking bytes."""
    import os

    import config as cfg

    app, db, photo_id = client_with_photo
    client = app.test_client()
    client.get(f"/photos/{photo_id}/preview?size=1920")
    assert db.preview_cache_total_bytes() > 0

    # Simulate a permission error on unlink.
    real_remove = os.remove

    def flaky_remove(path, *args, **kwargs):
        if path.endswith(f"{photo_id}_1920.jpg"):
            raise PermissionError("simulated")
        return real_remove(path, *args, **kwargs)

    monkeypatch.setattr(os, "remove", flaky_remove)
    monkeypatch.setattr(
        cfg, "load",
        lambda: {**cfg.DEFAULTS, "preview_cache_max_mb": 0},
    )

    # Trigger eviction via a config save. The unlink will fail, so the
    # row should remain so a subsequent pass can retry.
    resp = client.post("/api/config", json={"preview_cache_max_mb": 0})
    assert resp.status_code == 200
    assert db.preview_cache_get(photo_id, 1920) is not None


def test_startup_evicts_when_migration_pushes_over_quota(tmp_path, monkeypatch):
    """If legacy migration inserts rows that exceed the quota, startup
    eviction drains them without waiting for a later cache write."""
    import os

    import config as cfg
    from app import create_app
    from db import Database
    from PIL import Image

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")
    cfg.save({
        **cfg.DEFAULTS,
        "preview_max_size": 1920,
        "preview_cache_max_mb": 0,  # quota of 0 drains everything
    })

    photos_dir = tmp_path / "photos"
    photos_dir.mkdir()
    src = photos_dir / "test.jpg"
    Image.new("RGB", (800, 600), (180, 90, 40)).save(str(src), "JPEG", quality=85)

    vireo_dir = tmp_path / "vireo"
    thumb_dir = vireo_dir / "thumbnails"
    thumb_dir.mkdir(parents=True)
    db_path = str(vireo_dir / "vireo.db")

    db = Database(db_path)
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    fid = db.add_folder(str(photos_dir), name="photos")
    pid = db.add_photo(
        folder_id=fid, filename="test.jpg", extension=".jpg",
        file_size=os.path.getsize(src), file_mtime=os.path.getmtime(src),
        width=800, height=600,
    )

    preview_dir = vireo_dir / "previews"
    preview_dir.mkdir()
    legacy = preview_dir / f"{pid}.jpg"
    with open(legacy, "wb") as f:
        f.write(b"\xff\xd8\xff\xe0" + b"x" * 4096)

    # Creating the app runs migration (inserts row) then eviction (drains).
    create_app(db_path=db_path, thumb_cache_dir=str(thumb_dir))

    assert db.preview_cache_total_bytes() == 0
    assert not legacy.exists()
    assert not (preview_dir / f"{pid}_1920.jpg").exists()


def test_legacy_migration_skips_orphaned_photo_ids(tmp_path, monkeypatch):
    """Legacy {id}.jpg where id is no longer in photos table is unlinked,
    not inserted (which would fail the FK constraint)."""

    import config as cfg
    from app import create_app

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")
    cfg.save({**cfg.DEFAULTS, "preview_max_size": 1920})

    vireo_dir = tmp_path / "vireo"
    thumb_dir = vireo_dir / "thumbnails"
    thumb_dir.mkdir(parents=True)
    preview_dir = vireo_dir / "previews"
    preview_dir.mkdir()
    db_path = str(vireo_dir / "vireo.db")

    # Drop a legacy file for a photo id that won't exist in the DB.
    orphan = preview_dir / "99999.jpg"
    with open(orphan, "wb") as f:
        f.write(b"\xff\xd8\xff\xe0orphan")

    # Must not raise. The orphan file should be removed so disk doesn't
    # keep pointing at a vanished photo.
    create_app(db_path=db_path, thumb_cache_dir=str(thumb_dir))
    assert not orphan.exists()
    assert not (preview_dir / "99999_1920.jpg").exists()


def test_legacy_sized_preview_files_are_backfilled_at_startup(tmp_path, monkeypatch):
    """Pre-existing sized {id}_{size}.jpg files (written before
    preview_cache existed) get adopted into the LRU at startup so
    accounting and eviction can see them."""
    import os

    import config as cfg
    from app import create_app
    from db import Database
    from PIL import Image

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")
    cfg.save({**cfg.DEFAULTS, "preview_max_size": 1920})

    photos_dir = tmp_path / "photos"
    photos_dir.mkdir()
    src = photos_dir / "test.jpg"
    Image.new("RGB", (800, 600), (40, 90, 180)).save(str(src), "JPEG", quality=85)

    vireo_dir = tmp_path / "vireo"
    thumb_dir = vireo_dir / "thumbnails"
    thumb_dir.mkdir(parents=True)
    db_path = str(vireo_dir / "vireo.db")

    db = Database(db_path)
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    fid = db.add_folder(str(photos_dir), name="photos")
    pid_kept = db.add_photo(
        folder_id=fid, filename="test.jpg", extension=".jpg",
        file_size=os.path.getsize(src), file_mtime=os.path.getmtime(src),
        width=800, height=600,
    )

    preview_dir = vireo_dir / "previews"
    preview_dir.mkdir()
    # Untracked sized preview for an existing photo at two tiers.
    sized_a = preview_dir / f"{pid_kept}_1920.jpg"
    sized_b = preview_dir / f"{pid_kept}_2560.jpg"
    sized_a.write_bytes(b"\xff\xd8\xff\xe0" + b"a" * 1024)
    sized_b.write_bytes(b"\xff\xd8\xff\xe0" + b"b" * 2048)
    # Sized preview pointing at a deleted photo — should be unlinked.
    orphan = preview_dir / "999999_1920.jpg"
    orphan.write_bytes(b"\xff\xd8\xff\xe0orphan")

    assert db.preview_cache_get(pid_kept, 1920) is None
    assert db.preview_cache_get(pid_kept, 2560) is None

    create_app(db_path=db_path, thumb_cache_dir=str(thumb_dir))

    # Both real sized files are now tracked, with correct byte counts.
    row_a = db.preview_cache_get(pid_kept, 1920)
    row_b = db.preview_cache_get(pid_kept, 2560)
    assert row_a is not None and row_a["bytes"] == os.path.getsize(sized_a)
    assert row_b is not None and row_b["bytes"] == os.path.getsize(sized_b)
    # Orphan was removed; no row inserted (would have raised FK error).
    assert not orphan.exists()


def test_legacy_sized_preview_backfill_skips_already_tracked(tmp_path, monkeypatch):
    """Sized files with an existing preview_cache row are left alone —
    the migration must not overwrite last_access_at on a fresh row."""
    import os
    import time

    import config as cfg
    from app import create_app
    from db import Database
    from PIL import Image

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")
    cfg.save({**cfg.DEFAULTS, "preview_max_size": 1920})

    photos_dir = tmp_path / "photos"
    photos_dir.mkdir()
    src = photos_dir / "test.jpg"
    Image.new("RGB", (400, 300), (10, 20, 30)).save(str(src), "JPEG", quality=85)

    vireo_dir = tmp_path / "vireo"
    thumb_dir = vireo_dir / "thumbnails"
    thumb_dir.mkdir(parents=True)
    db_path = str(vireo_dir / "vireo.db")

    db = Database(db_path)
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    fid = db.add_folder(str(photos_dir), name="photos")
    pid = db.add_photo(
        folder_id=fid, filename="test.jpg", extension=".jpg",
        file_size=os.path.getsize(src), file_mtime=os.path.getmtime(src),
        width=400, height=300,
    )

    preview_dir = vireo_dir / "previews"
    preview_dir.mkdir()
    sized = preview_dir / f"{pid}_1920.jpg"
    sized.write_bytes(b"\xff\xd8\xff\xe0" + b"x" * 1024)

    # Row already exists with a recent last_access_at and the real size.
    db.preview_cache_insert(pid, 1920, os.path.getsize(sized))
    original_access = db.preview_cache_get(pid, 1920)["last_access_at"]

    # Wait long enough that an unintended re-insert would change the timestamp.
    time.sleep(0.05)
    create_app(db_path=db_path, thumb_cache_dir=str(thumb_dir))

    row = db.preview_cache_get(pid, 1920)
    assert row is not None
    assert row["last_access_at"] == original_access


def test_legacy_migration_preserves_preview_max_size_zero(tmp_path, monkeypatch):
    """When preview_max_size=0 (full-res), legacy files are left alone —
    they can't be assigned to a size tier."""

    import config as cfg
    from app import create_app

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")
    cfg.save({**cfg.DEFAULTS, "preview_max_size": 0})

    vireo_dir = tmp_path / "vireo"
    thumb_dir = vireo_dir / "thumbnails"
    thumb_dir.mkdir(parents=True)
    preview_dir = vireo_dir / "previews"
    preview_dir.mkdir()
    db_path = str(vireo_dir / "vireo.db")

    legacy = preview_dir / "42.jpg"
    with open(legacy, "wb") as f:
        f.write(b"\xff\xd8\xff\xe0leave-me")

    create_app(db_path=db_path, thumb_cache_dir=str(thumb_dir))

    # File stays exactly where it was; no renamed version was produced.
    assert legacy.exists()
    assert not (preview_dir / "42_1920.jpg").exists()


def test_startup_invalidates_unedited_raw_previews_built_from_working_copies(
    tmp_path, monkeypatch,
):
    """Existing dark RAW tiers are purged once; ordinary JPEGs are retained."""
    import config as cfg
    from app import create_app
    from db import Database
    from PIL import Image

    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    cfg.save({**cfg.DEFAULTS, "preview_max_size": 1920})

    photos_dir = tmp_path / "photos"
    photos_dir.mkdir()
    raw_source = photos_dir / "source.NEF"
    raw_source.write_bytes(b"raw bytes")
    jpeg_source = photos_dir / "plain.jpg"
    Image.new("RGB", (200, 150), (180, 90, 40)).save(
        jpeg_source, "JPEG",
    )

    vireo_dir = tmp_path / "vireo"
    thumb_dir = vireo_dir / "thumbnails"
    preview_dir = vireo_dir / "previews"
    working_dir = vireo_dir / "working"
    thumb_dir.mkdir(parents=True)
    preview_dir.mkdir()
    working_dir.mkdir()
    db_path = str(vireo_dir / "vireo.db")

    db = Database(db_path)
    workspace_id = db.ensure_default_workspace()
    db.set_active_workspace(workspace_id)
    folder_id = db.add_folder(str(photos_dir), name="photos")
    raw_id = db.add_photo(
        folder_id=folder_id,
        filename=raw_source.name,
        extension=".nef",
        file_size=raw_source.stat().st_size,
        file_mtime=raw_source.stat().st_mtime,
        width=200,
        height=150,
    )
    jpeg_id = db.add_photo(
        folder_id=folder_id,
        filename=jpeg_source.name,
        extension=".jpg",
        file_size=jpeg_source.stat().st_size,
        file_mtime=jpeg_source.stat().st_mtime,
        width=200,
        height=150,
    )
    for photo_id in (raw_id, jpeg_id):
        working = working_dir / f"{photo_id}.jpg"
        Image.new("RGB", (200, 150), (25, 25, 25)).save(working, "JPEG")
        db.conn.execute(
            "UPDATE photos SET working_copy_path=? WHERE id=?",
            (f"working/{photo_id}.jpg", photo_id),
        )

    raw_preview = preview_dir / f"{raw_id}_2560.jpg"
    jpeg_preview = preview_dir / f"{jpeg_id}_2560.jpg"
    raw_preview.write_bytes(b"dark raw preview")
    jpeg_preview.write_bytes(b"ordinary jpeg preview")
    db.preview_cache_insert(raw_id, 2560, raw_preview.stat().st_size)
    db.preview_cache_insert(jpeg_id, 2560, jpeg_preview.stat().st_size)
    db.conn.commit()

    create_app(db_path=db_path, thumb_cache_dir=str(thumb_dir))

    assert not raw_preview.exists()
    assert db.preview_cache_get(raw_id, 2560) is None
    assert jpeg_preview.exists()
    assert db.preview_cache_get(jpeg_id, 2560) is not None
    assert db.get_meta("unedited_raw_camera_preview_source_v1") == "1"

    # The source-selection fix prevents new dark entries. The migration itself
    # remains one-shot and does not repeatedly clear healthy future previews.
    raw_preview.write_bytes(b"new camera-rendered preview")
    db.preview_cache_insert(raw_id, 2560, raw_preview.stat().st_size)
    create_app(db_path=db_path, thumb_cache_dir=str(thumb_dir))
    assert raw_preview.exists()
    assert db.preview_cache_get(raw_id, 2560) is not None
    db.close()


def test_edit_math_version_bump_invalidates_edited_photo_caches(tmp_path, monkeypatch):
    """When EDIT_MATH_VERSION bumps, startup must purge cached renders for
    photos that have a non-null edit recipe — keeping them would serve the
    old tone-math bytes after the deploy that changed the math. Recipe-free
    photos render identically across versions and must be left alone."""
    import os

    import config as cfg
    from app import create_app
    from db import Database
    from image_edits import EDIT_MATH_VERSION
    from PIL import Image

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")
    cfg.save({**cfg.DEFAULTS, "preview_max_size": 1920})

    photos_dir = tmp_path / "photos"
    photos_dir.mkdir()
    src_edited = photos_dir / "edited.jpg"
    src_plain = photos_dir / "plain.jpg"
    Image.new("RGB", (200, 150), (40, 90, 180)).save(str(src_edited), "JPEG", quality=85)
    Image.new("RGB", (200, 150), (200, 90, 40)).save(str(src_plain), "JPEG", quality=85)

    vireo_dir = tmp_path / "vireo"
    thumb_dir = vireo_dir / "thumbnails"
    thumb_dir.mkdir(parents=True)
    preview_dir = vireo_dir / "previews"
    preview_dir.mkdir()
    db_path = str(vireo_dir / "vireo.db")

    db = Database(db_path)
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    fid = db.add_folder(str(photos_dir), name="photos")
    pid_edited = db.add_photo(
        folder_id=fid, filename="edited.jpg", extension=".jpg",
        file_size=os.path.getsize(src_edited),
        file_mtime=os.path.getmtime(src_edited),
        width=200, height=150,
    )
    pid_plain = db.add_photo(
        folder_id=fid, filename="plain.jpg", extension=".jpg",
        file_size=os.path.getsize(src_plain),
        file_mtime=os.path.getmtime(src_plain),
        width=200, height=150,
    )
    db.set_photo_edit_recipe(pid_edited, {"adjustments": {"exposure": 1.0}})

    # Simulate a pre-version-bump deployment: cached renders exist on disk
    # and in preview_cache, and the stored version lags behind the current.
    edited_preview = preview_dir / f"{pid_edited}_1920.jpg"
    plain_preview = preview_dir / f"{pid_plain}_1920.jpg"
    edited_thumb = thumb_dir / f"{pid_edited}.jpg"
    plain_thumb = thumb_dir / f"{pid_plain}.jpg"
    edited_raw_thumb = thumb_dir / f"{pid_edited}_raw.jpg"
    edited_jpeg_thumb = thumb_dir / f"{pid_edited}_jpeg.jpg"
    plain_raw_thumb = thumb_dir / f"{pid_plain}_raw.jpg"
    edited_preview.write_bytes(b"\xff\xd8\xff\xe0" + b"o" * 1024)
    plain_preview.write_bytes(b"\xff\xd8\xff\xe0" + b"p" * 1024)
    edited_thumb.write_bytes(b"\xff\xd8\xff\xe0" + b"t" * 1024)
    plain_thumb.write_bytes(b"\xff\xd8\xff\xe0" + b"u" * 1024)
    edited_raw_thumb.write_bytes(b"stale raw")
    edited_jpeg_thumb.write_bytes(b"stale jpeg")
    plain_raw_thumb.write_bytes(b"plain raw")
    db.preview_cache_insert(pid_edited, 1920, edited_preview.stat().st_size)
    db.preview_cache_insert(pid_plain, 1920, plain_preview.stat().st_size)
    db.conn.execute(
        "UPDATE photos SET thumb_path = ? WHERE id = ?",
        (f"{pid_edited}.jpg", pid_edited),
    )
    db.conn.execute(
        "UPDATE photos SET thumb_path = ? WHERE id = ?",
        (f"{pid_plain}.jpg", pid_plain),
    )
    db.set_meta("edit_math_version", str(EDIT_MATH_VERSION - 1))
    db.conn.commit()

    create_app(db_path=db_path, thumb_cache_dir=str(thumb_dir))

    # Edited photo: preview and thumb gone, thumb_path cleared, no row.
    assert not edited_preview.exists()
    assert not edited_thumb.exists()
    assert not edited_raw_thumb.exists()
    assert not edited_jpeg_thumb.exists()
    assert db.preview_cache_get(pid_edited, 1920) is None
    edited_row = db.conn.execute(
        "SELECT thumb_path FROM photos WHERE id = ?", (pid_edited,),
    ).fetchone()
    assert edited_row["thumb_path"] is None

    # Plain photo: cache survives because no recipe → output bytes unchanged.
    assert plain_preview.exists()
    assert plain_thumb.exists()
    assert plain_raw_thumb.exists()
    assert db.preview_cache_get(pid_plain, 1920) is not None
    plain_row = db.conn.execute(
        "SELECT thumb_path FROM photos WHERE id = ?", (pid_plain,),
    ).fetchone()
    assert plain_row["thumb_path"] == f"{pid_plain}.jpg"

    # Version is bumped, so a second create_app is a no-op.
    assert db.get_meta("edit_math_version") == str(EDIT_MATH_VERSION)
    new_preview = preview_dir / f"{pid_edited}_1920.jpg"
    new_preview.write_bytes(b"\xff\xd8\xff\xe0" + b"x" * 1024)
    db.preview_cache_insert(pid_edited, 1920, new_preview.stat().st_size)
    create_app(db_path=db_path, thumb_cache_dir=str(thumb_dir))
    assert new_preview.exists()
    assert db.preview_cache_get(pid_edited, 1920) is not None


def test_edit_math_version_migration_leaves_version_on_failed_purge(
    tmp_path, monkeypatch,
):
    """If a cache unlink fails mid-migration (locked file, transient perm
    error), we must NOT stamp the new version — otherwise the next boot
    skips the migration and the stale render keeps being served. Leaving
    the version old makes the migration self-retry on the next boot."""
    import os

    import app as app_module
    import config as cfg
    from app import create_app
    from db import Database
    from image_edits import EDIT_MATH_VERSION
    from PIL import Image

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")
    cfg.save({**cfg.DEFAULTS, "preview_max_size": 1920})

    photos_dir = tmp_path / "photos"
    photos_dir.mkdir()
    src_edited = photos_dir / "edited.jpg"
    Image.new("RGB", (200, 150), (40, 90, 180)).save(
        str(src_edited), "JPEG", quality=85,
    )

    vireo_dir = tmp_path / "vireo"
    thumb_dir = vireo_dir / "thumbnails"
    thumb_dir.mkdir(parents=True)
    preview_dir = vireo_dir / "previews"
    preview_dir.mkdir()
    db_path = str(vireo_dir / "vireo.db")

    db = Database(db_path)
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    fid = db.add_folder(str(photos_dir), name="photos")
    pid_edited = db.add_photo(
        folder_id=fid, filename="edited.jpg", extension=".jpg",
        file_size=os.path.getsize(src_edited),
        file_mtime=os.path.getmtime(src_edited),
        width=200, height=150,
    )
    db.set_photo_edit_recipe(pid_edited, {"adjustments": {"exposure": 1.0}})

    edited_thumb = thumb_dir / f"{pid_edited}.jpg"
    edited_thumb.write_bytes(b"\xff\xd8\xff\xe0" + b"t" * 1024)
    db.conn.execute(
        "UPDATE photos SET thumb_path = ? WHERE id = ?",
        (f"{pid_edited}.jpg", pid_edited),
    )
    db.set_meta("edit_math_version", str(EDIT_MATH_VERSION - 1))
    db.conn.commit()

    original_remove = app_module.os.remove

    def locked_remove(path):
        if path == str(edited_thumb):
            raise OSError("locked")
        return original_remove(path)

    monkeypatch.setattr(app_module.os, "remove", locked_remove)

    create_app(db_path=db_path, thumb_cache_dir=str(thumb_dir))

    # The unlink failed, so the version must stay behind for a retry.
    assert db.get_meta("edit_math_version") == str(EDIT_MATH_VERSION - 1)

    # Once the file is unlockable, a later boot completes the migration.
    monkeypatch.setattr(app_module.os, "remove", original_remove)
    create_app(db_path=db_path, thumb_cache_dir=str(thumb_dir))
    assert not edited_thumb.exists()
    assert db.get_meta("edit_math_version") == str(EDIT_MATH_VERSION)


def test_edit_math_version_migration_survives_unreadable_preview_dir(
    tmp_path, monkeypatch,
):
    """If preview_dir exists but is temporarily unreadable (locked network
    volume, permission flap), os.listdir raises OSError; create_app must NOT
    abort startup. Skip the orphan scan and leave the version old so the
    migration self-retries — matching the unlink-error contract."""
    import os
    from types import SimpleNamespace

    import app as app_module
    from db import Database
    from image_edits import EDIT_MATH_VERSION

    vireo_dir = tmp_path / "vireo"
    thumb_dir = vireo_dir / "thumbnails"
    thumb_dir.mkdir(parents=True)
    preview_dir = vireo_dir / "previews"
    preview_dir.mkdir()
    db_path = str(vireo_dir / "vireo.db")

    db = Database(db_path)
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    fid = db.add_folder(str(tmp_path), name="photos")
    pid = db.add_photo(
        folder_id=fid, filename="edited.jpg", extension=".jpg",
        file_size=100, file_mtime=0.0, width=40, height=30,
    )
    db.set_photo_edit_recipe(pid, {"adjustments": {"exposure": 0.5}})
    db.set_meta("edit_math_version", str(EDIT_MATH_VERSION - 1))
    db.conn.commit()
    db.conn.close()

    real_listdir = app_module.os.listdir

    def failing_listdir(path):
        if os.path.abspath(path) == os.path.abspath(str(preview_dir)):
            raise PermissionError(path)
        return real_listdir(path)

    monkeypatch.setattr(app_module.os, "listdir", failing_listdir)

    fake_app = SimpleNamespace(
        config={"THUMB_CACHE_DIR": str(thumb_dir), "DB_PATH": db_path},
    )
    # Must NOT raise — disposable cache problem can't take down startup.
    app_module._migrate_edit_math_render_caches(fake_app)

    db2 = Database(db_path)
    try:
        # Version stays old so the next boot retries the scan.
        assert db2.get_meta("edit_math_version") == str(EDIT_MATH_VERSION - 1)
    finally:
        db2.conn.close()

    # Once the dir becomes readable, a later boot completes the migration.
    monkeypatch.setattr(app_module.os, "listdir", real_listdir)
    app_module._migrate_edit_math_render_caches(fake_app)
    db3 = Database(db_path)
    try:
        assert db3.get_meta("edit_math_version") == str(EDIT_MATH_VERSION)
    finally:
        db3.conn.close()


def test_external_edit_handoff_meta_includes_math_version(
    client_with_photo, monkeypatch,
):
    """The external-editor handoff render reuses external-edits/<id>.jpg only
    when its cached metadata matches. That metadata must carry
    EDIT_MATH_VERSION so a math bump (which changes per-pixel output but not
    recipe/source/mtime) invalidates the stale hard-clipped render."""
    import json
    import os
    import subprocess

    from image_edits import EDIT_MATH_VERSION

    # Stub the launcher: with no editor configured this endpoint falls through
    # to a real `open <handoff>.jpg` on macOS, which pops the render up in
    # Preview during the test run. The handoff meta is written before launch,
    # so a no-op launcher doesn't weaken what this test checks.
    monkeypatch.setattr(subprocess, "run",
                        lambda *a, **k: subprocess.CompletedProcess(a[0] if a else [], 0))
    monkeypatch.setattr(subprocess, "Popen", lambda *a, **k: None)

    app, db, photo_id = client_with_photo
    client = app.test_client()
    db.set_photo_edit_recipe(photo_id, {"adjustments": {"exposure": 0.5}})

    resp = client.post(
        "/api/photos/open-external",
        json={"photo_ids": [photo_id], "editor_index": None},
    )
    # Open may fail (no editor configured) but the handoff render + meta
    # are written before the editor launch.
    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    meta_path = os.path.join(vireo_dir, "external-edits", f"{photo_id}.json")
    assert os.path.exists(meta_path), resp.get_data(as_text=True)
    with open(meta_path, encoding="utf-8") as f:
        meta = json.load(f)
    assert meta.get("edit_math_version") == EDIT_MATH_VERSION


def test_edit_math_version_migration_scans_preview_dir_once(
    tmp_path, monkeypatch,
):
    """The migration must scan preview_dir once, not once per edited photo.
    With N edited photos and M preview files the old per-photo os.listdir
    was O(N*M) and could stall startup on large libraries — guard against
    a regression by counting calls."""
    import os
    from types import SimpleNamespace

    import app as app_module
    from db import Database
    from image_edits import EDIT_MATH_VERSION

    vireo_dir = tmp_path / "vireo"
    thumb_dir = vireo_dir / "thumbnails"
    thumb_dir.mkdir(parents=True)
    preview_dir = vireo_dir / "previews"
    preview_dir.mkdir()
    db_path = str(vireo_dir / "vireo.db")

    db = Database(db_path)
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    fid = db.add_folder(str(tmp_path), name="photos")

    edited_pids = []
    for i in range(5):
        pid = db.add_photo(
            folder_id=fid, filename=f"edited_{i}.jpg", extension=".jpg",
            file_size=100, file_mtime=0.0, width=40, height=30,
        )
        db.set_photo_edit_recipe(
            pid, {"adjustments": {"exposure": 0.2 * (i + 1)}},
        )
        edited_pids.append(pid)
        # An untracked preview file (not in preview_cache) so the migration
        # actually exercises the listdir-driven orphan sweep.
        (preview_dir / f"{pid}_640.jpg").write_bytes(b"\xff\xd8\xff\xe0junk")

    db.set_meta("edit_math_version", str(EDIT_MATH_VERSION - 1))
    db.conn.commit()
    db.conn.close()

    real_listdir = app_module.os.listdir
    calls = []

    def counting_listdir(path):
        if os.path.abspath(path) == os.path.abspath(str(preview_dir)):
            calls.append(path)
        return real_listdir(path)

    monkeypatch.setattr(app_module.os, "listdir", counting_listdir)

    fake_app = SimpleNamespace(
        config={"THUMB_CACHE_DIR": str(thumb_dir), "DB_PATH": db_path},
    )
    app_module._migrate_edit_math_render_caches(fake_app)

    # One scan total, regardless of how many edited photos there are.
    assert len(calls) == 1, calls
    # And the orphans were actually purged.
    for pid in edited_pids:
        assert not (preview_dir / f"{pid}_640.jpg").exists()
    db2 = Database(db_path)
    try:
        assert db2.get_meta("edit_math_version") == str(EDIT_MATH_VERSION)
    finally:
        db2.conn.close()


def test_edit_math_version_migration_is_noop_on_fresh_db(tmp_path, monkeypatch):
    """A brand-new DB has no recipes and no stored version. The migration
    must just stamp the current version so the next deploy bumps cleanly."""
    import config as cfg
    from app import create_app
    from db import Database
    from image_edits import EDIT_MATH_VERSION

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")
    cfg.save({**cfg.DEFAULTS, "preview_max_size": 1920})

    vireo_dir = tmp_path / "vireo"
    thumb_dir = vireo_dir / "thumbnails"
    thumb_dir.mkdir(parents=True)
    db_path = str(vireo_dir / "vireo.db")

    create_app(db_path=db_path, thumb_cache_dir=str(thumb_dir))

    db = Database(db_path)
    assert db.get_meta("edit_math_version") == str(EDIT_MATH_VERSION)


def test_storage_clear_previews_resets_preview_cache(client_with_photo):
    """/api/storage/clear type=previews drops preview_cache rows so
    Settings "Current usage" doesn't report phantom bytes."""
    app, db, photo_id = client_with_photo
    client = app.test_client()
    # Populate the cache
    client.get(f"/photos/{photo_id}/preview?size=1920")
    assert db.preview_cache_total_bytes() > 0

    resp = client.post("/api/storage/clear", json={"type": "previews"})
    assert resp.status_code == 200
    assert db.preview_cache_total_bytes() == 0


def test_storage_delete_files_syncs_preview_cache(client_with_photo):
    """/api/storage/delete-files type=previews removes matching
    preview_cache rows for each sized-preview filename deleted."""
    app, db, photo_id = client_with_photo
    client = app.test_client()
    client.get(f"/photos/{photo_id}/preview?size=1920")
    assert db.preview_cache_get(photo_id, 1920) is not None

    resp = client.post(
        "/api/storage/delete-files",
        json={"type": "previews", "files": [f"{photo_id}_1920.jpg"]},
    )
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["deleted"] == 1
    assert db.preview_cache_get(photo_id, 1920) is None


def test_storage_files_limit_returns_bounded_preview_listing(client_with_photo):
    """The storage modal can request a bounded first page instead of
    statting and rendering every preview cache file before opening."""
    import os

    app, _db, photo_id = client_with_photo
    client = app.test_client()
    preview_dir = os.path.join(
        os.path.dirname(app.config["THUMB_CACHE_DIR"]), "previews"
    )
    os.makedirs(preview_dir, exist_ok=True)
    for size in (640, 1280, 1920):
        with open(os.path.join(preview_dir, f"{photo_id}_{size}.jpg"), "wb") as f:
            f.write(b"x" * size)

    resp = client.get("/api/storage/files?type=previews&limit=2")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["limit"] == 2
    assert data["truncated"] is True
    assert len(data["files"]) == 2

    resp = client.get("/api/storage/files?type=previews")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["truncated"] is False
    assert len(data["files"]) == 3


def test_storage_clear_thumbnails_resets_thumb_path(client_with_photo):
    """/api/storage/clear type=thumbnails NULLs photos.thumb_path so the
    pipeline plan's count_photos_missing_thumb doesn't report phantoms.

    Without this, the Thumbnails & Previews pill renders "Already done"
    after a cache wipe even though the next run regenerates everything.
    """
    app, db, photo_id = client_with_photo
    client = app.test_client()
    db.conn.execute(
        "UPDATE photos SET thumb_path = ? WHERE id = ?",
        (f"{photo_id}.jpg", photo_id),
    )
    db.conn.commit()
    assert db.count_photos_missing_thumb()["pending"] == 0

    resp = client.post("/api/storage/clear", json={"type": "thumbnails"})
    assert resp.status_code == 200
    assert db.count_photos_missing_thumb()["pending"] == 1
    row = db.conn.execute(
        "SELECT thumb_path FROM photos WHERE id = ?", (photo_id,),
    ).fetchone()
    assert row["thumb_path"] is None


def test_storage_delete_files_syncs_thumb_path(client_with_photo):
    """/api/storage/delete-files type=thumbnails NULLs photos.thumb_path
    for the photos whose thumb files were removed."""
    import os

    from PIL import Image

    app, db, photo_id = client_with_photo
    client = app.test_client()
    thumb_dir = app.config["THUMB_CACHE_DIR"]
    thumb_file = os.path.join(thumb_dir, f"{photo_id}.jpg")
    Image.new("RGB", (10, 10)).save(thumb_file, "JPEG")
    db.conn.execute(
        "UPDATE photos SET thumb_path = ? WHERE id = ?",
        (thumb_file, photo_id),
    )
    db.conn.commit()

    resp = client.post(
        "/api/storage/delete-files",
        json={"type": "thumbnails", "files": [f"{photo_id}.jpg"]},
    )
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["deleted"] == 1
    row = db.conn.execute(
        "SELECT thumb_path FROM photos WHERE id = ?", (photo_id,),
    ).fetchone()
    assert row["thumb_path"] is None


def test_storage_delete_files_chunks_large_thumb_id_list(
    tmp_path, monkeypatch,
):
    """Deleting more thumbnails than SQLite's variable cap (999 on legacy
    builds) must not raise OperationalError. The handler chunks the
    UPDATE so a bulk wipe stays within ``SQLITE_MAX_VARIABLE_NUMBER``.

    Without chunking, large selections from the storage UI would unlink
    every file successfully and then fail the photos.thumb_path UPDATE,
    leaving the column out of sync with disk and re-introducing the
    phantom "Already done" pill this PR exists to prevent.
    """
    import os

    import config as cfg
    import models
    from app import create_app
    from db import Database
    from PIL import Image

    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    monkeypatch.setattr(
        models, "DEFAULT_MODELS_DIR", str(tmp_path / "vireo-models"),
    )
    monkeypatch.setattr(
        models, "CONFIG_PATH", str(tmp_path / "models.json"),
    )

    photos_dir = tmp_path / "photos"
    photos_dir.mkdir()
    vireo_dir = tmp_path / "vireo"
    vireo_dir.mkdir()
    thumb_dir = vireo_dir / "thumbnails"
    thumb_dir.mkdir()
    db_path = str(vireo_dir / "vireo.db")

    db = Database(db_path)
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    fid = db.add_folder(str(photos_dir), name="photos")

    # 1500 photos > 999 (legacy SQLITE_MAX_VARIABLE_NUMBER).
    n = 1500
    photo_ids = []
    files = []
    for i in range(n):
        src = photos_dir / f"p{i}.jpg"
        # Touch a tiny placeholder for add_photo's mtime/size; not loaded.
        src.write_bytes(b"\xff\xd8\xff\xe0")
        pid = db.add_photo(
            folder_id=fid, filename=f"p{i}.jpg", extension=".jpg",
            file_size=os.path.getsize(src),
            file_mtime=os.path.getmtime(src),
            width=10, height=10,
        )
        photo_ids.append(pid)
        files.append(f"{pid}.jpg")
        thumb_file = thumb_dir / f"{pid}.jpg"
        Image.new("RGB", (8, 8)).save(str(thumb_file), "JPEG")
        db.conn.execute(
            "UPDATE photos SET thumb_path = ? WHERE id = ?",
            (str(thumb_file), pid),
        )
    db.conn.commit()

    # Force the legacy 999 cap so a regression that drops the chunking
    # would actually trip the limit rather than relying on the modern
    # 32766 ceiling masking the bug on most CI machines.
    db.conn.execute("PRAGMA max_variable_number=999")

    app = create_app(
        db_path=db_path, thumb_cache_dir=str(thumb_dir),
        api_token="test-token-123",
    )
    client = app.test_client()
    resp = client.post(
        "/api/storage/delete-files",
        json={"type": "thumbnails", "files": files},
    )
    assert resp.status_code == 200, resp.get_json()
    data = resp.get_json()
    assert data["deleted"] == n
    row = db.conn.execute(
        "SELECT COUNT(*) AS n FROM photos WHERE thumb_path IS NOT NULL"
    ).fetchone()
    assert row["n"] == 0
    db.close()


def test_preview_adoption_enforces_quota(client_with_photo, monkeypatch):
    """Lazily-adopting a legacy on-disk preview file still runs eviction,
    so with preview_cache_max_mb=0 the adopted file is drained like a
    freshly generated one."""
    import os

    import config as cfg

    monkeypatch.setattr(
        cfg, "load",
        lambda: {**cfg.DEFAULTS, "preview_cache_max_mb": 0},
    )
    app, db, photo_id = client_with_photo
    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    preview_dir = os.path.join(vireo_dir, "previews")
    os.makedirs(preview_dir, exist_ok=True)

    # Pre-seed a legacy on-disk preview with no preview_cache row.
    cache_path = os.path.join(preview_dir, f"{photo_id}_1920.jpg")
    with open(cache_path, "wb") as f:
        f.write(b"\xff\xd8\xff\xe0" + b"x" * 4096)
    assert db.preview_cache_get(photo_id, 1920) is None

    client = app.test_client()
    resp = client.get(f"/photos/{photo_id}/preview?size=1920")
    assert resp.status_code == 200
    assert len(resp.data) > 100  # served from memory
    # Quota is 0, so eviction drained the row and file after adoption.
    assert db.preview_cache_total_bytes() == 0
    assert not os.path.exists(cache_path)


def test_preview_cache_clear_removes_untracked_and_legacy(client_with_photo):
    """/api/preview-cache/clear removes orphaned and legacy files, not just tracked rows."""
    import os
    app, db, photo_id = client_with_photo
    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    preview_dir = os.path.join(vireo_dir, "previews")
    os.makedirs(preview_dir, exist_ok=True)

    # Simulate: one tracked preview, one untracked sized preview, one legacy /full cache
    tracked = os.path.join(preview_dir, f"{photo_id}_1920.jpg")
    untracked = os.path.join(preview_dir, f"{photo_id}_2560.jpg")  # no row in preview_cache
    legacy = os.path.join(preview_dir, f"{photo_id}.jpg")

    for p in (tracked, untracked, legacy):
        with open(p, "wb") as f:
            f.write(b"\xff\xd8\xff\xe0fake")
    db.preview_cache_insert(photo_id, 1920, os.path.getsize(tracked))

    client = app.test_client()
    resp = client.post("/api/preview-cache/clear")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["cleared"] == 1
    assert data["files_removed"] == 3  # all three matching files

    # All three files are gone
    assert not os.path.exists(tracked)
    assert not os.path.exists(untracked)
    assert not os.path.exists(legacy)
    assert db.preview_cache_total_bytes() == 0


def test_preview_cache_clear_handles_many_unlink_failures(client_with_photo, monkeypatch):
    """Clear must survive hundreds of unlink failures without hitting the
    SQLite variable limit (~999) on the DELETE NOT IN clause. Accounting
    for failed-to-unlink files must also be preserved so usage reporting
    continues to reflect the leaked bytes.
    """
    import os
    app, db, photo_id = client_with_photo
    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    preview_dir = os.path.join(vireo_dir, "previews")
    os.makedirs(preview_dir, exist_ok=True)

    # Seed ~600 tracked sized preview files; well above SQLite's default
    # 999-parameter limit when multiplied by two bind params per pair.
    N = 600
    sized_files = []
    for size in range(1000, 1000 + N):
        p = os.path.join(preview_dir, f"{photo_id}_{size}.jpg")
        with open(p, "wb") as f:
            f.write(b"\xff\xd8\xff\xe0x")
        db.preview_cache_insert(photo_id, size, os.path.getsize(p))
        sized_files.append((p, photo_id, size))

    # Fail every unlink for these files — simulates a locked/read-only dir.
    real_remove = os.remove
    sized_set = {p for p, _, _ in sized_files}

    def flaky_remove(path, *a, **kw):
        if path in sized_set:
            raise OSError("simulated lock")
        return real_remove(path, *a, **kw)

    monkeypatch.setattr(os, "remove", flaky_remove)

    client = app.test_client()
    resp = client.post("/api/preview-cache/clear")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["failed"] == N
    assert data["files_removed"] == 0

    # All the rows whose files we couldn't unlink must be kept for
    # accounting purposes; usage must still reflect the leaked bytes.
    remaining = db.conn.execute(
        "SELECT COUNT(*) AS c FROM preview_cache"
    ).fetchone()["c"]
    assert remaining == N
    assert db.preview_cache_total_bytes() > 0


def test_settings_save_triggers_eviction_when_quota_shrinks(client_with_photo):
    """POSTing a smaller preview_cache_max_mb evicts down to the new quota."""
    app, db, photo_id = client_with_photo
    client = app.test_client()

    # Populate cache
    client.get(f"/photos/{photo_id}/preview?size=1920")
    assert db.preview_cache_total_bytes() > 0

    # Shrink quota to 0 via the config endpoint (same path the UI uses)
    resp = client.post("/api/config", json={"preview_cache_max_mb": 0})
    assert resp.status_code == 200

    assert db.preview_cache_total_bytes() == 0


def test_full_respects_workspace_preview_max_size_override(client_with_photo):
    """/full uses the workspace-effective preview_max_size, not just global.

    Set a workspace override to 2560 and confirm /full serves the same bytes
    as /preview?size=2560 (the handler must read get_effective_config, not
    plain cfg.get).
    """
    app, db, photo_id = client_with_photo
    # Write a workspace override for preview_max_size.
    db.update_workspace(
        db._active_workspace_id,
        config_overrides={"preview_max_size": 2560},
    )
    client = app.test_client()
    full = client.get(f"/photos/{photo_id}/full")
    preview = client.get(f"/photos/{photo_id}/preview?size=2560")
    assert full.status_code == 200
    assert preview.status_code == 200
    assert full.data == preview.data
    # Sanity: a row at size=2560 was created (not 1920).
    assert db.preview_cache_get(photo_id, 2560) is not None


def test_preview_precompute_respects_workspace_preview_max_size_override(client_with_photo):
    """/api/jobs/previews uses workspace-effective preview_max_size.

    Otherwise precompute warms the wrong tier (global size) while /full
    serves the workspace override, causing a cache miss + regenerate on
    first view and accumulating duplicate tiers on disk.
    """
    import time

    app, db, photo_id = client_with_photo
    db.update_workspace(
        db._active_workspace_id,
        config_overrides={"preview_max_size": 2560},
    )
    client = app.test_client()
    resp = client.post("/api/jobs/previews")
    assert resp.status_code == 200
    job_id = resp.get_json()["job_id"]

    for _ in range(100):
        r = client.get(f"/api/jobs/{job_id}")
        data = r.get_json()
        if data["status"] in ("completed", "failed"):
            break
        time.sleep(0.05)
    assert data["status"] == "completed"

    # Precompute must have warmed the workspace override tier (2560),
    # not the global default (1920).
    assert db.preview_cache_get(photo_id, 2560) is not None
    assert db.preview_cache_get(photo_id, 1920) is None


def test_zero_byte_cache_file_is_regenerated(client_with_photo):
    """An interrupted write leaves a 0-byte cache file; serve regenerates it.

    Simulates a prior crashed write by dropping an empty file at the cache
    path and asserting the next GET produces a real (non-empty) preview
    and leaves a populated file on disk.
    """
    import os
    app, db, photo_id = client_with_photo
    preview_dir = os.path.join(
        os.path.dirname(app.config["THUMB_CACHE_DIR"]), "previews"
    )
    os.makedirs(preview_dir, exist_ok=True)
    cache_path = os.path.join(preview_dir, f"{photo_id}_1920.jpg")
    # Drop a zero-byte file in place as if a prior write was interrupted.
    with open(cache_path, "wb"):
        pass
    assert os.path.getsize(cache_path) == 0

    client = app.test_client()
    resp = client.get(f"/photos/{photo_id}/preview?size=1920")
    assert resp.status_code == 200
    assert len(resp.data) > 0
    # File was regenerated with real bytes.
    assert os.path.getsize(cache_path) > 0
    # And tracked in the cache.
    row = db.preview_cache_get(photo_id, 1920)
    assert row is not None
    assert row["bytes"] > 0


# --- POST/DELETE /api/photos/<id>/location ----------------------------------
#
# The autocomplete pick path: client sends a Google ``place_id``, server
# looks it up via the Places HTTP wrapper and writes a leaf+parent-chain of
# ``type='location'`` keywords. ``places.place_details`` is monkeypatched
# so no HTTP traffic happens during tests.

def _central_park_details():
    """Canned Place Details dict shaped like ``vireo.places.place_details``.

    Mirrors what Google would return for Central Park, NYC: a leaf with
    coords + a four-level parent chain (city -> county -> state -> country).
    Google's ``address_components`` order is narrowest-first, which the
    upsert logic in ``Database._upsert_location_parent_chain`` reverses
    when chaining ``parent_id`` upward.
    """
    return {
        "place_id": "ChIJ4zGFAZpYwokRGUGph3Mf37k",
        "name": "Central Park",
        "lat": 40.7828,
        "lng": -73.9654,
        "address_components": [
            {"name": "New York", "short_name": "New York", "types": ["locality"]},
            {"name": "New York County", "short_name": "New York County",
             "types": ["administrative_area_level_2"]},
            {"name": "New York", "short_name": "NY", "types": ["administrative_area_level_1"]},
            {"name": "United States", "short_name": "US", "types": ["country"]},
        ],
    }


def _central_park_client_place():
    """Canned Google Maps JS Place payload sent by the browser."""
    return {
        "place_id": "ChIJ4zGFAZpYwokRGUGph3Mf37k",
        "name": "Central Park",
        "geometry": {"location": {"lat": 40.7828, "lng": -73.9654}},
        "address_components": [
            {"long_name": "New York", "short_name": "New York", "types": ["locality"]},
            {"long_name": "New York County", "short_name": "New York County",
             "types": ["administrative_area_level_2"]},
            {"long_name": "New York", "short_name": "NY", "types": ["administrative_area_level_1"]},
            {"long_name": "United States", "short_name": "US", "types": ["country"]},
        ],
    }


def test_post_photo_location_with_valid_place_id(app_and_db, monkeypatch):
    """Valid pick: route stores leaf + parents and returns the serialized location."""
    import config as cfg
    import places
    app, db = app_and_db

    # API key must be present or the route short-circuits with no_api_key.
    cfg.save({**cfg.load(), "google_maps_api_key": "FAKE-KEY"})

    captured = {}

    def fake_place_details(place_id, key):
        captured["place_id"] = place_id
        captured["key"] = key
        return _central_park_details()

    # The route imports ``places`` at module level via ``import places``.
    monkeypatch.setattr(places, "place_details", fake_place_details)

    photo = db.get_photos()[0]
    pid = photo["id"]

    client = app.test_client()
    resp = client.post(
        f"/api/photos/{pid}/location",
        json={"place_id": "ChIJ_x"},
    )
    assert resp.status_code == 200, resp.get_json()
    data = resp.get_json()

    # Response shape — leaf fields + parent chain (broadest -> narrowest, no leaf).
    loc = data["location"]
    assert loc["name"] == "Central Park"
    assert loc["place_id"] == "ChIJ4zGFAZpYwokRGUGph3Mf37k"
    assert loc["latitude"] == 40.7828
    assert loc["longitude"] == -73.9654
    assert [p["name"] for p in loc["parent_chain"]] == [
        "United States", "New York", "New York County", "New York",
    ]

    # And the route actually called Google with the body's place_id + config key.
    assert captured == {"place_id": "ChIJ_x", "key": "FAKE-KEY"}

    # Photo now has exactly one type='location' keyword link, pointing at the
    # leaf row that carries the place_id.
    rows = db.conn.execute(
        "SELECT k.id, k.name, k.place_id FROM photo_keywords pk "
        "JOIN keywords k ON k.id = pk.keyword_id "
        "WHERE pk.photo_id = ? AND k.type = 'location'",
        (pid,),
    ).fetchall()
    assert len(rows) == 1
    assert rows[0]["place_id"] == "ChIJ4zGFAZpYwokRGUGph3Mf37k"
    assert rows[0]["name"] == "Central Park"


def test_post_photo_location_accepts_client_place_details_without_server_lookup(
    app_and_db, monkeypatch,
):
    """Autocomplete may succeed with a referrer-restricted browser key even
    when a server-side Place Details request would be denied. If the browser
    sends full details, the route should store them directly.
    """
    import config as cfg
    import places
    app, db = app_and_db

    cfg.save({**cfg.load(), "google_maps_api_key": ""})

    def fail_place_details(place_id, key):
        raise AssertionError("server-side Place Details should not be called")

    monkeypatch.setattr(places, "place_details", fail_place_details)

    photo = db.get_photos()[0]
    pid = photo["id"]

    client = app.test_client()
    resp = client.post(
        f"/api/photos/{pid}/location",
        json={
            "place_id": "ChIJ4zGFAZpYwokRGUGph3Mf37k",
            "place": _central_park_client_place(),
        },
    )
    assert resp.status_code == 200, resp.get_json()
    loc = resp.get_json()["location"]
    assert loc["name"] == "Central Park"
    assert loc["place_id"] == "ChIJ4zGFAZpYwokRGUGph3Mf37k"
    assert loc["latitude"] == 40.7828
    assert loc["longitude"] == -73.9654
    assert [p["name"] for p in loc["parent_chain"]] == [
        "United States", "New York", "New York County", "New York",
    ]


def test_post_photo_location_normalizes_non_string_client_place_id(app_and_db):
    """Nested client place_id values are string-normalized before use."""
    import config as cfg
    app, db = app_and_db

    cfg.save({**cfg.load(), "google_maps_api_key": ""})

    place = _central_park_client_place()
    place["place_id"] = 12345

    photo = db.get_photos()[0]
    pid = photo["id"]

    client = app.test_client()
    resp = client.post(
        f"/api/photos/{pid}/location",
        json={"place": place},
    )
    assert resp.status_code == 200, resp.get_json()
    loc = resp.get_json()["location"]
    assert loc["place_id"] == "12345"


def test_post_photo_location_client_place_types_drops_same_named_leaf_parent(
    app_and_db,
):
    """Client autocomplete must forward `place.types` so a locality named the
    same as a higher administrative area (e.g. city `New York` inside state
    `New York`) does not get duplicated as its own parent in the chain.
    """
    import config as cfg
    app, db = app_and_db

    cfg.save({**cfg.load(), "google_maps_api_key": ""})

    new_york_city_place = {
        "place_id": "ChIJOwg_06VPwokRYv534QaPC8g",
        "name": "New York",
        "types": ["locality", "political"],
        "geometry": {"location": {"lat": 40.7128, "lng": -74.006}},
        "address_components": [
            {"long_name": "New York", "short_name": "New York",
             "types": ["locality", "political"]},
            {"long_name": "New York", "short_name": "NY",
             "types": ["administrative_area_level_1", "political"]},
            {"long_name": "United States", "short_name": "US",
             "types": ["country", "political"]},
        ],
    }

    photo = db.get_photos()[0]
    pid = photo["id"]

    client = app.test_client()
    resp = client.post(
        f"/api/photos/{pid}/location",
        json={"place": new_york_city_place},
    )
    assert resp.status_code == 200, resp.get_json()
    loc = resp.get_json()["location"]
    assert loc["name"] == "New York"
    # Parent chain (broadest -> narrowest, excluding leaf) must NOT include a
    # second "New York" — only the country and the state level.
    parent_names = [p["name"] for p in loc["parent_chain"]]
    assert parent_names == ["United States", "New York"]


def test_post_photo_location_returns_400_on_missing_place_id(app_and_db):
    """Empty body / missing place_id is a 400 — never reaches Google."""
    app, db = app_and_db
    photo = db.get_photos()[0]
    pid = photo["id"]

    client = app.test_client()
    resp = client.post(f"/api/photos/{pid}/location", json={})
    assert resp.status_code == 400
    assert resp.get_json()["error"] == "missing place_id"


def test_post_photo_location_returns_400_on_empty_api_key(app_and_db):
    """No configured API key: degrade to a 400 ``no_api_key`` error."""
    import config as cfg
    app, db = app_and_db
    # Explicitly clear the key so the route hits the empty-key branch even
    # if a previous test left one behind in the same temp config file.
    cfg.save({**cfg.load(), "google_maps_api_key": ""})

    photo = db.get_photos()[0]
    pid = photo["id"]

    client = app.test_client()
    resp = client.post(
        f"/api/photos/{pid}/location",
        json={"place_id": "ChIJ_x"},
    )
    assert resp.status_code == 400
    body = resp.get_json()
    assert body["error"] == "no_api_key"
    assert body["code"] == "invalid_request"
    assert body["message"] == (
        "Google Maps isn’t configured. Add an API key in Settings to use "
        "Google place search."
    )


def test_post_photo_location_returns_404_when_google_returns_none(app_and_db, monkeypatch):
    """Google returns ZERO_RESULTS / NOT_FOUND -> wrapper returns None -> 404."""
    import config as cfg
    import places
    app, db = app_and_db

    cfg.save({**cfg.load(), "google_maps_api_key": "FAKE-KEY"})
    monkeypatch.setattr(places, "place_details", lambda place_id, key: None)

    photo = db.get_photos()[0]
    pid = photo["id"]

    client = app.test_client()
    resp = client.post(
        f"/api/photos/{pid}/location",
        json={"place_id": "ChIJ_unknown"},
    )
    assert resp.status_code == 404
    body = resp.get_json()
    assert body["error"] == "place_not_found"
    assert body["code"] == "not_found"
    assert body["message"] == (
        "Google Maps couldn’t find that place. Search again and choose another "
        "result."
    )


def test_post_photo_location_returns_409_on_name_conflict(app_and_db, monkeypatch):
    """When upsert_place_chain raises RuntimeError (cross-type collision in
    the parent chain), the route must return 409 with name_conflict — same
    contract as /api/keywords/<id>/link-place. Previously the RuntimeError
    propagated and surfaced as a 500."""
    import config as cfg
    import places
    app, db = app_and_db
    cfg.save({**cfg.load(), "google_maps_api_key": "FAKE-KEY"})

    # Pre-build the location parent chain through the state level, then
    # plant a 'general' keyword whose (name, parent_id) collides with one of
    # the components in _central_park_details (which uses New York / New
    # York County / New York / United States). Planting 'general'
    # "New York County" under the state row triggers the cross-type
    # collision when the chain walk tries to INSERT 'location' "New York
    # County" at the same slot.
    us_id = db.conn.execute(
        "INSERT INTO keywords (name, type, parent_id) VALUES ('United States', 'location', NULL)",
    ).lastrowid
    state_id = db.conn.execute(
        "INSERT INTO keywords (name, type, parent_id) VALUES ('New York', 'location', ?)",
        (us_id,),
    ).lastrowid
    db.conn.execute(
        "INSERT INTO keywords (name, type, parent_id) VALUES ('New York County', 'general', ?)",
        (state_id,),
    )
    db.conn.commit()

    monkeypatch.setattr(places, "place_details",
                        lambda place_id, key: _central_park_details())

    photo = db.get_photos()[0]
    pid = photo["id"]

    client = app.test_client()
    resp = client.post(
        f"/api/photos/{pid}/location",
        json={"place_id": "ChIJ_anything"},
    )
    assert resp.status_code == 409
    body = resp.get_json()
    assert body["error"] == "name_conflict"
    assert body["code"] == "name_conflict"
    assert body["message"] == (
        "Couldn’t assign this location because “New York County” is already "
        "used by another keyword. Rename that keyword in Keywords, then try again."
    )
    assert "New York County" in body["error_detail"]


def test_post_photo_location_returns_404_on_missing_photo(app_and_db, monkeypatch):
    """A stale client (e.g. tab open after the photo was deleted) hitting
    POST /api/photos/<id>/location must get a clean 404, not a 500 from
    set_photo_location's FK violation on photo_keywords."""
    import config as cfg
    import places
    app, db = app_and_db
    cfg.save({**cfg.load(), "google_maps_api_key": "FAKE-KEY"})
    monkeypatch.setattr(places, "place_details",
                        lambda place_id, key: _central_park_details())

    client = app.test_client()
    resp = client.post(
        "/api/photos/999999/location",
        json={"place_id": "ChIJ_x"},
    )
    assert resp.status_code == 404
    body = resp.get_json()
    assert body["error"] == "photo_not_found"
    assert body["code"] == "not_found"
    assert body["message"] == (
        "This photo is no longer available in the active workspace. Refresh "
        "the page and try again."
    )


def _add_out_of_workspace_photo(db):
    active_ws = db._active_workspace_id
    other_ws = db.create_workspace("Other")
    db.set_active_workspace(other_ws)
    fid = db.add_folder("/photos/other", name="other")
    pid = db.add_photo(
        folder_id=fid,
        filename="outside.jpg",
        extension=".jpg",
        file_size=100,
        file_mtime=1.0,
    )
    db.set_active_workspace(active_ws)
    return pid


def test_post_photo_location_rejects_out_of_workspace_photo(app_and_db):
    """Location edits must not queue sync changes for hidden workspace photos."""
    app, db = app_and_db
    pid = _add_out_of_workspace_photo(db)

    client = app.test_client()
    resp = client.post(
        f"/api/photos/{pid}/location",
        json={
            "place_id": "ChIJ4zGFAZpYwokRGUGph3Mf37k",
            "place": _central_park_client_place(),
        },
    )
    assert resp.status_code == 403
    assert "active workspace" in resp.get_json()["error"]

    assert db.conn.execute(
        "SELECT 1 FROM photo_keywords WHERE photo_id = ?", (pid,),
    ).fetchone() is None
    assert db.conn.execute(
        "SELECT 1 FROM pending_changes WHERE photo_id = ?", (pid,),
    ).fetchone() is None


def test_post_photo_location_text_returns_404_on_missing_photo(app_and_db):
    """Same FK guard for the free-text fallback path."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.post(
        "/api/photos/999999/location/text",
        json={"name": "the meadow"},
    )
    assert resp.status_code == 404
    assert resp.get_json()["error"] == "photo_not_found"


def test_post_photo_location_text_rejects_out_of_workspace_photo(app_and_db):
    app, db = app_and_db
    pid = _add_out_of_workspace_photo(db)

    client = app.test_client()
    resp = client.post(
        f"/api/photos/{pid}/location/text",
        json={"name": "the meadow"},
    )
    assert resp.status_code == 403
    assert "active workspace" in resp.get_json()["error"]

    assert db.conn.execute(
        "SELECT 1 FROM photo_keywords WHERE photo_id = ?", (pid,),
    ).fetchone() is None
    assert db.conn.execute(
        "SELECT 1 FROM pending_changes WHERE photo_id = ?", (pid,),
    ).fetchone() is None


def test_post_photo_location_text_queues_cleanup_when_xmp_location_disabled(app_and_db):
    """Disabled assigned-location writes still need a cleanup sync opportunity."""
    import config as cfg
    app, db = app_and_db
    cfg.save({**cfg.load(), "write_assigned_location_to_xmp": False})
    photo = db.get_photos()[0]
    pid = photo["id"]

    client = app.test_client()
    resp = client.post(
        f"/api/photos/{pid}/location/text",
        json={"name": "the meadow"},
    )

    assert resp.status_code == 200, resp.get_json()
    row = db.conn.execute(
        "SELECT change_type, value FROM pending_changes WHERE photo_id = ?",
        (pid,),
    ).fetchone()
    assert dict(row) == {"change_type": "location", "value": "effective"}


def test_delete_photo_location_returns_404_on_missing_photo(app_and_db):
    """DELETE on a missing photo returns 404 for consistency with the
    POST routes (was previously a silent 200 because the underlying
    DELETE matched nothing)."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.delete("/api/photos/999999/location")
    assert resp.status_code == 404
    assert resp.get_json()["error"] == "photo_not_found"


def test_delete_photo_location_rejects_out_of_workspace_photo(app_and_db):
    app, db = app_and_db
    active_ws = db._active_workspace_id
    other_ws = db.create_workspace("Other")
    db.set_active_workspace(other_ws)
    fid = db.add_folder("/photos/other", name="other")
    pid = db.add_photo(
        folder_id=fid,
        filename="outside.jpg",
        extension=".jpg",
        file_size=100,
        file_mtime=1.0,
    )
    leaf_id = db.upsert_place_chain(_central_park_details())
    db.set_photo_location(pid, leaf_id)
    db.set_active_workspace(active_ws)

    client = app.test_client()
    resp = client.delete(f"/api/photos/{pid}/location")
    assert resp.status_code == 403
    assert "active workspace" in resp.get_json()["error"]

    assert db.conn.execute(
        "SELECT 1 FROM photo_keywords WHERE photo_id = ?", (pid,),
    ).fetchone() is not None
    assert db.conn.execute(
        "SELECT 1 FROM pending_changes WHERE photo_id = ?", (pid,),
    ).fetchone() is None


def test_delete_photo_location_clears_links(app_and_db):
    """DELETE removes location keyword links but leaves the keyword row intact."""
    app, db = app_and_db
    photo = db.get_photos()[0]
    pid = photo["id"]

    # Set up an existing location link directly via DB methods (no Google
    # round-trip needed for the delete path).
    leaf_id = db.upsert_place_chain(_central_park_details())
    db.set_photo_location(pid, leaf_id)
    # Sanity: the link exists before DELETE.
    pre_links = db.conn.execute(
        "SELECT 1 FROM photo_keywords pk JOIN keywords k ON k.id = pk.keyword_id "
        "WHERE pk.photo_id = ? AND k.type = 'location'",
        (pid,),
    ).fetchall()
    assert len(pre_links) == 1

    client = app.test_client()
    resp = client.delete(f"/api/photos/{pid}/location")
    assert resp.status_code == 200
    assert resp.get_json() == {"ok": True}

    # No location links remain on the photo.
    post_links = db.conn.execute(
        "SELECT 1 FROM photo_keywords pk JOIN keywords k ON k.id = pk.keyword_id "
        "WHERE pk.photo_id = ? AND k.type = 'location'",
        (pid,),
    ).fetchall()
    assert post_links == []

    # The keyword row itself is preserved — other photos / future links may
    # still reference it.
    leaf_row = db.conn.execute(
        "SELECT id, name FROM keywords WHERE id = ?", (leaf_id,),
    ).fetchone()
    assert leaf_row is not None
    assert leaf_row["name"] == "Central Park"
def test_post_photo_location_records_edit(app_and_db, monkeypatch):
    """POST adds an entry to the audit log so the action is undoable/visible."""
    import config as cfg
    import places
    app, db = app_and_db

    cfg.save({**cfg.load(), "google_maps_api_key": "FAKE-KEY"})
    monkeypatch.setattr(places, "place_details",
                        lambda place_id, key: _central_park_details())

    photo = db.get_photos()[0]
    pid = photo["id"]

    pre_history = db.get_edit_history()
    pre_count = len(pre_history)

    client = app.test_client()
    resp = client.post(
        f"/api/photos/{pid}/location",
        json={"place_id": "ChIJ_x"},
    )
    assert resp.status_code == 200, resp.get_json()

    post_history = db.get_edit_history()
    assert len(post_history) == pre_count + 1
    # Most recent first.
    entry = post_history[0]
    assert entry["action_type"] == "location_set"
    assert "Central Park" in entry["description"]


def test_batch_photo_location_sets_all_selected_photos(app_and_db, monkeypatch):
    """Batch location POST stores one place keyword across selected photos."""
    import config as cfg
    import places
    app, db = app_and_db

    cfg.save({**cfg.load(), "google_maps_api_key": ""})

    def fail_place_details(place_id, key):
        raise AssertionError("client place details should avoid server lookup")

    monkeypatch.setattr(places, "place_details", fail_place_details)

    photo_ids = [p["id"] for p in db.get_photos()[:3]]
    client = app.test_client()
    resp = client.post(
        "/api/batch/location",
        json={
            "photo_ids": photo_ids,
            "place_id": "ChIJ4zGFAZpYwokRGUGph3Mf37k",
            "place": _central_park_client_place(),
        },
    )
    assert resp.status_code == 200, resp.get_json()
    assert resp.get_json()["updated"] == len(photo_ids)

    for pid in photo_ids:
        row = db.conn.execute(
            "SELECT k.place_id FROM photo_keywords pk "
            "JOIN keywords k ON k.id = pk.keyword_id "
            "WHERE pk.photo_id = ? AND k.type = 'location'",
            (pid,),
        ).fetchone()
        assert row["place_id"] == "ChIJ4zGFAZpYwokRGUGph3Mf37k"
        queued = db.conn.execute(
            "SELECT value FROM pending_changes "
            "WHERE photo_id = ? AND change_type = 'location'",
            (pid,),
        ).fetchone()
        assert queued["value"] == "effective"

    entry = db.get_edit_history()[0]
    assert entry["action_type"] == "location_set"
    assert entry["item_count"] == len(photo_ids)


def test_batch_photo_location_text_sets_all_selected_photos(app_and_db):
    """Batch free-text location POST stores one location across selected photos."""
    app, db = app_and_db
    photo_ids = [p["id"] for p in db.get_photos()[:3]]

    client = app.test_client()
    resp = client.post(
        "/api/batch/location/text",
        json={"photo_ids": photo_ids, "name": "the meadow"},
    )
    assert resp.status_code == 200, resp.get_json()
    assert resp.get_json()["updated"] == len(photo_ids)

    for pid in photo_ids:
        row = db.conn.execute(
            "SELECT k.name, k.place_id FROM photo_keywords pk "
            "JOIN keywords k ON k.id = pk.keyword_id "
            "WHERE pk.photo_id = ? AND k.type = 'location'",
            (pid,),
        ).fetchone()
        assert dict(row) == {"name": "the meadow", "place_id": None}

    entry = db.get_edit_history()[0]
    assert entry["action_type"] == "location_set"
    assert entry["item_count"] == len(photo_ids)


def test_batch_photo_location_text_can_remember_reviewed_map_point(app_and_db):
    """Custom names created from map review become nearby saved suggestions."""
    app, db = app_and_db
    photo_ids = [p["id"] for p in db.get_photos()[:2]]

    response = app.test_client().post(
        "/api/batch/location/text",
        json={
            "photo_ids": photo_ids,
            "name": "Anza-Borrego Desert State Park",
            "latitude": 33.255,
            "longitude": -116.405,
        },
    )

    assert response.status_code == 200, response.get_json()
    row = db.conn.execute(
        "SELECT name, latitude, longitude FROM keywords "
        "WHERE id = ?",
        (response.get_json()["location"]["keyword_id"],),
    ).fetchone()
    assert dict(row) == {
        "name": "Anza-Borrego Desert State Park",
        "latitude": 33.255,
        "longitude": -116.405,
    }


def test_delete_photo_location_records_edit(app_and_db):
    """DELETE adds an entry to the audit log even though it doesn't write a sidecar."""
    app, db = app_and_db

    leaf_id = db.upsert_place_chain(_central_park_details())
    photo = db.get_photos()[0]
    pid = photo["id"]
    db.set_photo_location(pid, leaf_id)

    pre_history = db.get_edit_history()
    pre_count = len(pre_history)

    client = app.test_client()
    resp = client.delete(f"/api/photos/{pid}/location")
    assert resp.status_code == 200

    post_history = db.get_edit_history()
    assert len(post_history) == pre_count + 1
    entry = post_history[0]
    assert entry["action_type"] == "location_clear"
    assert entry["description"] == "cleared location"


# --- POST /api/photos/<id>/location/text ------------------------------------
#
# Free-text fallback path: user types a name and hits Enter without picking a
# Google suggestion (or no API key is configured). No Google round-trip.

def test_post_photo_location_text_creates_keyword_and_links(app_and_db):
    """Free-text POST creates a no-place_id keyword and links it to the photo."""
    app, db = app_and_db
    photo = db.get_photos()[0]
    pid = photo["id"]

    client = app.test_client()
    resp = client.post(
        f"/api/photos/{pid}/location/text",
        json={"name": "the meadow"},
    )
    assert resp.status_code == 200, resp.get_json()
    data = resp.get_json()

    loc = data["location"]
    assert loc["name"] == "the meadow"
    assert loc["place_id"] is None
    assert loc["parent_chain"] == []

    # DB-level: photo has exactly one type='location' keyword link, on a row
    # with no place_id (free-text).
    rows = db.conn.execute(
        "SELECT k.id, k.name, k.place_id FROM photo_keywords pk "
        "JOIN keywords k ON k.id = pk.keyword_id "
        "WHERE pk.photo_id = ? AND k.type = 'location'",
        (pid,),
    ).fetchall()
    assert len(rows) == 1
    assert rows[0]["name"] == "the meadow"
    assert rows[0]["place_id"] is None


def test_post_photo_location_text_strips_whitespace(app_and_db):
    """Surrounding whitespace is stripped before the keyword is created."""
    app, db = app_and_db
    photo = db.get_photos()[0]
    pid = photo["id"]

    client = app.test_client()
    resp = client.post(
        f"/api/photos/{pid}/location/text",
        json={"name": "  the meadow  "},
    )
    assert resp.status_code == 200, resp.get_json()
    data = resp.get_json()
    assert data["location"]["name"] == "the meadow"

    rows = db.conn.execute(
        "SELECT k.name FROM photo_keywords pk "
        "JOIN keywords k ON k.id = pk.keyword_id "
        "WHERE pk.photo_id = ? AND k.type = 'location'",
        (pid,),
    ).fetchall()
    assert len(rows) == 1
    assert rows[0]["name"] == "the meadow"


def test_post_photo_location_text_400_on_missing_name(app_and_db):
    """Empty body / missing name is a 400."""
    app, db = app_and_db
    photo = db.get_photos()[0]
    pid = photo["id"]

    client = app.test_client()
    resp = client.post(f"/api/photos/{pid}/location/text", json={})
    assert resp.status_code == 400
    assert resp.get_json()["error"] == "missing name"


def test_post_photo_location_text_400_on_empty_name(app_and_db):
    """Whitespace-only name is rejected as 400."""
    app, db = app_and_db
    photo = db.get_photos()[0]
    pid = photo["id"]

    client = app.test_client()
    resp = client.post(
        f"/api/photos/{pid}/location/text",
        json={"name": "   "},
    )
    assert resp.status_code == 400
    assert resp.get_json()["error"] == "missing name"


def test_post_photo_location_text_replaces_existing_location(app_and_db):
    """A second free-text POST replaces (not adds to) the existing location link."""
    app, db = app_and_db
    photo = db.get_photos()[0]
    pid = photo["id"]

    client = app.test_client()
    r1 = client.post(
        f"/api/photos/{pid}/location/text",
        json={"name": "first place"},
    )
    assert r1.status_code == 200
    r2 = client.post(
        f"/api/photos/{pid}/location/text",
        json={"name": "second place"},
    )
    assert r2.status_code == 200

    rows = db.conn.execute(
        "SELECT k.name FROM photo_keywords pk "
        "JOIN keywords k ON k.id = pk.keyword_id "
        "WHERE pk.photo_id = ? AND k.type = 'location'",
        (pid,),
    ).fetchall()
    assert len(rows) == 1
    assert rows[0]["name"] == "second place"


def test_post_photo_location_text_records_edit(app_and_db):
    """Free-text POST adds an audit-log entry under the same 'location_set' type."""
    app, db = app_and_db
    photo = db.get_photos()[0]
    pid = photo["id"]

    pre_history = db.get_edit_history()
    pre_count = len(pre_history)

    client = app.test_client()
    resp = client.post(
        f"/api/photos/{pid}/location/text",
        json={"name": "the meadow"},
    )
    assert resp.status_code == 200, resp.get_json()

    post_history = db.get_edit_history()
    assert len(post_history) == pre_count + 1
    entry = post_history[0]
    assert entry["action_type"] == "location_set"
    assert "the meadow" in entry["description"]


# --- GET /api/places/reverse-geocode ----------------------------------------
#
# Server-side proxy for the Google Geocoding API with a SQLite cache layer
# keyed on the (lat, lng) ~110m grid. The route exists so the API key never
# leaves the server (the autocomplete JS library is the one client-facing
# Google call we make). Tests monkeypatch ``places.reverse_geocode`` so no
# HTTP traffic happens.

def _central_park_geocode_response():
    """Canned reverse-geocode response stored in the cache for hit tests.

    Same shape as ``places.reverse_geocode`` returns — i.e. the value the
    route serializes via ``json.dumps(details)`` before stashing. Reusing the
    Central Park place_id for symmetry with the autocomplete tests above.
    """
    return {
        "place_id": "ChIJ4zGFAZpYwokRGUGph3Mf37k",
        "name": "Central Park",
        "lat": 40.7828,
        "lng": -73.9654,
        "address_components": [
            {"name": "New York", "short_name": "New York", "types": ["locality"]},
            {"name": "New York", "short_name": "NY", "types": ["administrative_area_level_1"]},
            {"name": "United States", "short_name": "US", "types": ["country"]},
        ],
    }


def test_reverse_geocode_cache_hit_returns_summary(app_and_db, monkeypatch):
    """Pre-populated cache: route serves from SQLite, no Google call."""
    import json

    import places
    app, db = app_and_db

    lat, lng = 40.7828, -73.9654
    db.reverse_geocode_cache_put(
        lat, lng,
        place_id="ChIJ4zGFAZpYwokRGUGph3Mf37k",
        response_json=json.dumps(_central_park_geocode_response()),
    )

    # Counter-mock proves we never reached out to Google.
    calls = {"n": 0}

    def fake_reverse_geocode(lat_, lng_, key):
        calls["n"] += 1
        raise AssertionError("places.reverse_geocode should not be called on cache hit")

    monkeypatch.setattr(places, "reverse_geocode", fake_reverse_geocode)

    client = app.test_client()
    resp = client.get(f"/api/places/reverse-geocode?lat={lat}&lng={lng}")
    assert resp.status_code == 200, resp.get_json()
    data = resp.get_json()
    assert data["place_id"] == "ChIJ4zGFAZpYwokRGUGph3Mf37k"
    # Summary: leaf + the 1-2 broadest parents (end-of-list components).
    assert "Central Park" in data["summary"]
    assert "United States" in data["summary"]
    assert calls["n"] == 0


def test_reverse_geocode_cache_negative_hit_returns_null(app_and_db, monkeypatch):
    """Cached negative (place_id=None): route returns null without calling Google."""
    import places
    app, db = app_and_db

    lat, lng = 12.345, 67.890
    # Negative cache entry — Google was previously asked and returned no match.
    db.reverse_geocode_cache_put(lat, lng, place_id=None, response_json="{}")

    calls = {"n": 0}

    def fake_reverse_geocode(lat_, lng_, key):
        calls["n"] += 1
        raise AssertionError("places.reverse_geocode should not be called on negative cache hit")

    monkeypatch.setattr(places, "reverse_geocode", fake_reverse_geocode)

    client = app.test_client()
    resp = client.get(f"/api/places/reverse-geocode?lat={lat}&lng={lng}")
    assert resp.status_code == 200, resp.get_json()
    assert resp.get_json() == {"place_id": None, "summary": None}
    assert calls["n"] == 0


def test_reverse_geocode_cache_miss_calls_google_and_caches(app_and_db, monkeypatch):
    """Empty cache: hit Google, cache the result, second call hits cache."""
    import config as cfg
    import places
    app, db = app_and_db

    cfg.save({**cfg.load(), "google_maps_api_key": "FAKE-KEY"})

    calls = {"n": 0}

    def fake_reverse_geocode(lat_, lng_, key):
        calls["n"] += 1
        return _central_park_geocode_response()

    monkeypatch.setattr(places, "reverse_geocode", fake_reverse_geocode)

    client = app.test_client()
    lat, lng = 40.7828, -73.9654

    r1 = client.get(f"/api/places/reverse-geocode?lat={lat}&lng={lng}")
    assert r1.status_code == 200, r1.get_json()
    data1 = r1.get_json()
    assert data1["place_id"] == "ChIJ4zGFAZpYwokRGUGph3Mf37k"
    assert "Central Park" in data1["summary"]
    assert calls["n"] == 1

    # Second call against the same coords should hit the cache, NOT Google.
    r2 = client.get(f"/api/places/reverse-geocode?lat={lat}&lng={lng}")
    assert r2.status_code == 200
    data2 = r2.get_json()
    assert data2["place_id"] == data1["place_id"]
    assert data2["summary"] == data1["summary"]
    assert calls["n"] == 1  # unchanged — second call was served from cache


def test_reverse_geocode_cache_miss_caches_negative_when_google_returns_none(
    app_and_db, monkeypatch,
):
    """Google returns None: cache the negative so future calls don't hit the API."""
    import config as cfg
    import places
    app, db = app_and_db

    cfg.save({**cfg.load(), "google_maps_api_key": "FAKE-KEY"})

    calls = {"n": 0}

    def fake_reverse_geocode(lat_, lng_, key):
        calls["n"] += 1
        return None

    monkeypatch.setattr(places, "reverse_geocode", fake_reverse_geocode)

    client = app.test_client()
    lat, lng = 0.123, 0.456

    r1 = client.get(f"/api/places/reverse-geocode?lat={lat}&lng={lng}")
    assert r1.status_code == 200, r1.get_json()
    assert r1.get_json() == {"place_id": None, "summary": None}
    assert calls["n"] == 1

    # Negative was cached — second call must NOT re-ask Google.
    r2 = client.get(f"/api/places/reverse-geocode?lat={lat}&lng={lng}")
    assert r2.status_code == 200
    assert r2.get_json() == {"place_id": None, "summary": None}
    assert calls["n"] == 1


def test_reverse_geocode_returns_null_when_no_api_key_and_does_not_cache(
    app_and_db, monkeypatch,
):
    """No API key: degrade to ``{place_id: null}`` AND don't pollute the cache.

    Caching a negative when the user has no key would mean that once they add
    a key, already-asked grid cells would forever return null. So we skip the
    cache write entirely and just return the null shape.
    """
    import config as cfg
    import places
    app, db = app_and_db

    cfg.save({**cfg.load(), "google_maps_api_key": ""})

    calls = {"n": 0}

    def fake_reverse_geocode(lat_, lng_, key):
        calls["n"] += 1
        raise AssertionError("places.reverse_geocode should not be called when no API key")

    monkeypatch.setattr(places, "reverse_geocode", fake_reverse_geocode)

    lat, lng = 51.5074, -0.1278  # somewhere in London — fresh coords

    client = app.test_client()
    resp = client.get(f"/api/places/reverse-geocode?lat={lat}&lng={lng}")
    assert resp.status_code == 200
    assert resp.get_json() == {"place_id": None, "summary": None}
    assert calls["n"] == 0

    # Critically: the cache must be empty for this grid, so once the user
    # eventually adds a key, the next call will actually reach Google.
    cached = db.reverse_geocode_cache_get(lat, lng)
    assert cached is None


def test_reverse_geocode_does_not_cache_transient_errors(app_and_db, monkeypatch):
    """When ``places.reverse_geocode`` raises ``PlacesTransientError``
    (rate limit, network blip, etc.), the route MUST return null without
    writing to the cache. Otherwise the next request for the same grid
    would hit a stale negative-cache entry and silently suppress the EXIF
    suggestion until manual cache cleanup."""
    import config as cfg
    import places
    app, db = app_and_db

    cfg.save({**cfg.load(), "google_maps_api_key": "FAKE-KEY"})

    def transient(lat_, lng_, key):
        raise places.PlacesTransientError("simulated OVER_QUERY_LIMIT")

    monkeypatch.setattr(places, "reverse_geocode", transient)

    lat, lng = 35.6762, 139.6503  # Tokyo — fresh coords

    client = app.test_client()
    resp = client.get(f"/api/places/reverse-geocode?lat={lat}&lng={lng}")
    assert resp.status_code == 200
    assert resp.get_json() == {"place_id": None, "summary": None}

    # The cache MUST NOT have been written. A subsequent request will
    # retry Google.
    cached = db.reverse_geocode_cache_get(lat, lng)
    assert cached is None, (
        "transient failures must not be cached — the cache row must "
        "stay absent so the next request retries Google"
    )


def test_reverse_geocode_400_on_non_finite_coords(app_and_db):
    """float() accepts 'nan' and 'inf' but they blow up downstream when
    we round to the grid (int(round(NaN)) raises ValueError). Reject them
    at the boundary as 400, same as other invalid coords."""
    app, _ = app_and_db
    client = app.test_client()

    for bad in ["nan", "NaN", "inf", "-inf", "Infinity"]:
        r = client.get(f"/api/places/reverse-geocode?lat={bad}&lng=0")
        assert r.status_code == 400, f"lat={bad} should be rejected"
        assert r.get_json()["error"] == "invalid coords"

        r = client.get(f"/api/places/reverse-geocode?lat=0&lng={bad}")
        assert r.status_code == 400, f"lng={bad} should be rejected"


def test_reverse_geocode_400_on_invalid_coords(app_and_db):
    """Missing or unparseable lat/lng is a 400."""
    app, _ = app_and_db
    client = app.test_client()

    # No query params at all.
    r1 = client.get("/api/places/reverse-geocode")
    assert r1.status_code == 400
    assert r1.get_json()["error"] == "invalid coords"

    # Garbage lat.
    r2 = client.get("/api/places/reverse-geocode?lat=foo&lng=1.0")
    assert r2.status_code == 400
    assert r2.get_json()["error"] == "invalid coords"


def _yosemite_geocode_response():
    return {
        "place_id": "ChIJxeyK9Z3wloAR_gOA7SycJC8",
        "name": "Yosemite Valley",
        "lat": 37.7456,
        "lng": -119.5936,
        "address_components": [
            {"name": "Mariposa County", "short_name": "Mariposa County",
             "types": ["administrative_area_level_2"]},
            {"name": "California", "short_name": "CA",
             "types": ["administrative_area_level_1"]},
            {"name": "United States", "short_name": "US", "types": ["country"]},
        ],
    }


def test_location_review_preview_groups_coordinates_without_geocoding(
    app_and_db, monkeypatch,
):
    """The review queue is spatial evidence, not Google's chosen place id."""
    import places

    app, db = app_and_db
    photos = db.get_photos(sort="name")
    p1, p2, p3 = [p["id"] for p in photos]
    db.conn.executemany(
        "UPDATE photos SET latitude = ?, longitude = ? WHERE id = ?",
        [
            (33.2550, -116.4050, p1),
            (33.2554, -116.4053, p2),  # same review area
            (37.7456, -119.5936, p3),  # a separate area
        ],
    )
    db.conn.commit()
    monkeypatch.setattr(
        places,
        "reverse_geocode",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("location review must not reverse-geocode")
        ),
    )

    response = app.test_client().post(
        "/api/location-review/preview",
        json={"photo_ids": [p1, p2, p3]},
    )

    assert response.status_code == 200, response.get_json()
    body = response.get_json()
    assert body["total"] == 3
    assert body["reviewable"] == 3
    assert body["unresolved"] == []
    assert body["skipped"] == []
    assert [group["count"] for group in body["groups"]] == [2, 1]
    assert body["groups"][0]["photo_ids"] == [p1, p2]
    assert body["groups"][0]["spread_m"] > 0
    assert body["groups"][0]["center"]["lat"] == pytest.approx(33.2552)


def test_location_review_preview_skips_assigned_and_reports_missing_gps(
    app_and_db,
):
    app, db = app_and_db
    photos = db.get_photos(sort="name")
    p1, p2, p3 = [p["id"] for p in photos]
    db.conn.execute(
        "UPDATE photos SET latitude = 33.255, longitude = -116.405 WHERE id = ?",
        (p1,),
    )
    db.conn.execute(
        "UPDATE photos SET latitude = 33.256, longitude = -116.406 WHERE id = ?",
        (p2,),
    )
    location_id = db.get_or_create_text_location("Already reviewed")
    db.set_photo_location(p2, location_id)

    response = app.test_client().post(
        "/api/location-review/preview",
        json={"photo_ids": [p1, p2, p3]},
    )

    assert response.status_code == 200, response.get_json()
    body = response.get_json()
    assert body["reviewable"] == 1
    assert body["groups"][0]["photo_ids"] == [p1]
    assert body["skipped"] == [{
        "filename": "bird2.jpg",
        "photo_id": p2,
        "reason": "already_has_location",
    }]
    assert body["unresolved"] == [{
        "filename": "bird3.jpg",
        "photo_id": p3,
        "reason": "missing_gps",
    }]


def test_location_review_saved_suggestions_are_nearby_and_workspace_used(
    app_and_db,
):
    app, db = app_and_db
    p1, p2, _ = [p["id"] for p in db.get_photos(sort="name")]
    near_id = db._upsert_one_keyword(
        "Anza-Borrego Desert State Park", None,
        latitude=33.255, longitude=-116.405,
    )
    far_id = db._upsert_one_keyword(
        "Yosemite Valley", None,
        latitude=37.7456, longitude=-119.5936,
    )
    db.conn.commit()
    db.set_photo_location(p1, near_id)
    db.set_photo_location(p2, far_id)

    response = app.test_client().get(
        "/api/location-review/saved-suggestions"
        "?lat=33.2551&lng=-116.4051&radius_m=25000"
    )

    assert response.status_code == 200, response.get_json()
    suggestions = response.get_json()["suggestions"]
    assert [item["name"] for item in suggestions] == [
        "Anza-Borrego Desert State Park"
    ]
    assert suggestions[0]["keyword_id"] == near_id
    assert suggestions[0]["photo_count"] == 1
    assert suggestions[0]["distance_m"] < 20


def test_batch_location_from_exif_preview_groups_without_linking(
    app_and_db, monkeypatch,
):
    """Preview resolves each photo's own GPS and does not attach locations."""
    import config as cfg
    import places
    app, db = app_and_db
    cfg.save({**cfg.load(), "google_maps_api_key": "FAKE-KEY"})

    photos = db.get_photos(sort="name")
    p1, p2, p3 = [p["id"] for p in photos]
    db.conn.execute(
        "UPDATE photos SET latitude = ?, longitude = ? WHERE id = ?",
        (40.7828, -73.9654, p1),
    )
    db.conn.execute(
        "UPDATE photos SET latitude = ?, longitude = ? WHERE id = ?",
        (37.7456, -119.5936, p2),
    )
    db.conn.commit()

    def fake_reverse_geocode(lat, lng, key):
        if lat > 39:
            return _central_park_geocode_response()
        return _yosemite_geocode_response()

    monkeypatch.setattr(places, "reverse_geocode", fake_reverse_geocode)

    client = app.test_client()
    resp = client.post(
        "/api/batch/location/from-exif",
        json={"photo_ids": [p1, p2, p3], "apply": False},
    )

    assert resp.status_code == 200, resp.get_json()
    body = resp.get_json()
    assert body["total"] == 3
    assert body["resolvable"] == 2
    assert {g["name"] for g in body["groups"]} == {"Central Park", "Yosemite Valley"}
    assert body["unresolved"] == [
        {"filename": "bird3.jpg", "photo_id": p3, "reason": "missing_gps"}
    ]
    assert "_details_by_place_id" not in body
    # The fixture starts with two taxonomy keyword links. Preview may cache
    # reverse-geocode results, but it must not create location links.
    assert db.conn.execute(
        "SELECT COUNT(*) FROM photo_keywords pk "
        "JOIN keywords k ON k.id = pk.keyword_id "
        "WHERE k.type = 'location'"
    ).fetchone()[0] == 0


def test_batch_location_from_exif_accepts_more_than_ten_thousand_photos(
    app_and_db,
):
    """Large libraries are not rejected by the old arbitrary request cap."""
    app, db = app_and_db
    folder_id = db.conn.execute(
        "SELECT id FROM folders WHERE path = ?", ("/photos/2024",),
    ).fetchone()["id"]
    db.conn.executemany(
        "INSERT INTO photos "
        "(folder_id, filename, extension, file_size, file_mtime) "
        "VALUES (?, ?, '.jpg', 1, 1)",
        [
            (folder_id, f"large-library-{index}.jpg")
            for index in range(10_001)
        ],
    )
    db.conn.commit()
    photo_ids = [
        row["id"] for row in db.conn.execute(
            "SELECT id FROM photos WHERE filename LIKE 'large-library-%' "
            "ORDER BY id"
        ).fetchall()
    ]

    resp = app.test_client().post(
        "/api/batch/location/from-exif",
        json={"photo_ids": photo_ids, "apply": False},
    )

    assert resp.status_code == 200, resp.get_json()
    body = resp.get_json()
    assert body["total"] == 10_001
    assert body["resolvable"] == 0
    assert len(body["unresolved"]) == 10_001
    assert {item["reason"] for item in body["unresolved"]} == {"missing_gps"}


def test_batch_location_from_exif_apply_assigns_per_photo_places(
    app_and_db, monkeypatch,
):
    """Apply mode assigns different place keywords to different GPS photos."""
    import config as cfg
    import places
    app, db = app_and_db
    cfg.save({**cfg.load(), "google_maps_api_key": "FAKE-KEY"})

    photos = db.get_photos(sort="name")
    p1, p2, p3 = [p["id"] for p in photos]
    db.conn.execute(
        "UPDATE photos SET latitude = ?, longitude = ? WHERE id = ?",
        (40.7828, -73.9654, p1),
    )
    db.conn.execute(
        "UPDATE photos SET latitude = ?, longitude = ? WHERE id = ?",
        (37.7456, -119.5936, p2),
    )
    db.conn.commit()

    def fake_reverse_geocode(lat, lng, key):
        if lat > 39:
            return _central_park_geocode_response()
        return _yosemite_geocode_response()

    monkeypatch.setattr(places, "reverse_geocode", fake_reverse_geocode)

    client = app.test_client()
    resp = client.post(
        "/api/batch/location/from-exif",
        json={"photo_ids": [p1, p2, p3], "apply": True},
    )

    assert resp.status_code == 200, resp.get_json()
    body = resp.get_json()
    assert body["updated"] == 2
    assert body["resolvable"] == 2
    assert body["group_errors"] == []

    rows = db.conn.execute(
        "SELECT pk.photo_id, k.name, k.place_id "
        "FROM photo_keywords pk "
        "JOIN keywords k ON k.id = pk.keyword_id "
        "WHERE k.type = 'location' "
        "ORDER BY pk.photo_id"
    ).fetchall()
    by_photo = {row["photo_id"]: row for row in rows}
    assert by_photo[p1]["name"] == "Central Park"
    assert by_photo[p2]["name"] == "Yosemite Valley"
    assert p3 not in by_photo

    pending = db.conn.execute(
        "SELECT photo_id, change_type, value FROM pending_changes "
        "WHERE change_type = 'location' ORDER BY photo_id"
    ).fetchall()
    assert [(r["photo_id"], r["value"]) for r in pending] == [
        (p1, "effective"),
        (p2, "effective"),
    ]

    edit = db.conn.execute(
        "SELECT action_type, is_batch, description FROM edit_history "
        "ORDER BY id DESC LIMIT 1"
    ).fetchone()
    assert edit["action_type"] == "location_set"
    assert edit["is_batch"] == 1
    assert "resolved GPS locations for 2 photos across 2 places" in edit["description"]


# --- POST /api/keywords/<id>/link-place -------------------------------------
#
# Attach Google place data to an existing free-text location keyword. Builds
# the parent chain server-side and either UPDATEs the target row or merges it
# into a pre-existing canonical place_id-bearing row. ``places.place_details``
# is monkeypatched so no HTTP traffic happens.

def test_post_keyword_link_place_attaches_metadata(app_and_db, monkeypatch):
    """Successful link: target keyword gains place_id + coords + parent chain."""
    import config as cfg
    import places
    app, db = app_and_db

    cfg.save({**cfg.load(), "google_maps_api_key": "FAKE-KEY"})
    monkeypatch.setattr(places, "place_details",
                        lambda place_id, key: _central_park_details())

    # Pre-create a free-text "Central Park" keyword and tag a photo with it.
    kw_id = db.get_or_create_text_location("Central Park")
    photo = db.get_photos()[0]
    pid = photo["id"]
    db.set_photo_location(pid, kw_id)

    client = app.test_client()
    resp = client.post(
        f"/api/keywords/{kw_id}/link-place",
        json={"place_id": "ChIJ_x"},
    )
    assert resp.status_code == 200, resp.get_json()
    data = resp.get_json()

    assert data["merged"] is False
    kw = data["keyword"]
    assert kw["keyword_id"] == kw_id  # canonical row is the original
    assert kw["place_id"] == "ChIJ4zGFAZpYwokRGUGph3Mf37k"
    assert kw["latitude"] == 40.7828
    assert kw["longitude"] == -73.9654
    assert kw["name"] == "Central Park"
    # Parent chain: broadest -> narrowest, EXCLUDES leaf.
    assert [p["name"] for p in kw["parent_chain"]] == [
        "United States", "New York", "New York County", "New York",
    ]

    # DB-level: original keyword row now has place_id + coords.
    row = db.conn.execute(
        "SELECT name, place_id, latitude, longitude FROM keywords WHERE id = ?",
        (kw_id,),
    ).fetchone()
    assert row["place_id"] == "ChIJ4zGFAZpYwokRGUGph3Mf37k"
    assert row["latitude"] == 40.7828
    assert row["longitude"] == -73.9654

    # Photo is still tagged with the same row.
    rows = db.conn.execute(
        "SELECT k.id FROM photo_keywords pk "
        "JOIN keywords k ON k.id = pk.keyword_id "
        "WHERE pk.photo_id = ? AND k.type = 'location'",
        (pid,),
    ).fetchall()
    assert len(rows) == 1
    assert rows[0]["id"] == kw_id


def test_post_keyword_link_place_accepts_client_place_details_without_server_lookup(
    app_and_db, monkeypatch,
):
    """Keyword linking can use the autocomplete payload directly instead of
    requiring a second backend Place Details request.
    """
    import config as cfg
    import places
    app, db = app_and_db

    cfg.save({**cfg.load(), "google_maps_api_key": ""})

    def fail_place_details(place_id, key):
        raise AssertionError("server-side Place Details should not be called")

    monkeypatch.setattr(places, "place_details", fail_place_details)

    kw_id = db.get_or_create_text_location("Central Park")

    client = app.test_client()
    resp = client.post(
        f"/api/keywords/{kw_id}/link-place",
        json={
            "place_id": "ChIJ4zGFAZpYwokRGUGph3Mf37k",
            "place": _central_park_client_place(),
        },
    )
    assert resp.status_code == 200, resp.get_json()
    data = resp.get_json()

    assert data["merged"] is False
    kw = data["keyword"]
    assert kw["keyword_id"] == kw_id
    assert kw["name"] == "Central Park"
    assert kw["place_id"] == "ChIJ4zGFAZpYwokRGUGph3Mf37k"
    assert kw["latitude"] == 40.7828
    assert kw["longitude"] == -73.9654
    assert [p["name"] for p in kw["parent_chain"]] == [
        "United States", "New York", "New York County", "New York",
    ]


def test_post_keyword_link_place_rejects_non_finite_client_coords(app_and_db):
    """Client-supplied coordinates must be finite and in valid lat/lng bounds."""
    import config as cfg
    app, db = app_and_db

    cfg.save({**cfg.load(), "google_maps_api_key": ""})
    kw_id = db.get_or_create_text_location("Central Park")

    place = _central_park_client_place()
    place["geometry"]["location"]["lat"] = "NaN"

    client = app.test_client()
    resp = client.post(
        f"/api/keywords/{kw_id}/link-place",
        json={
            "place_id": "ChIJ4zGFAZpYwokRGUGph3Mf37k",
            "place": place,
        },
    )
    assert resp.status_code == 400
    assert resp.get_json()["error"] == "no_api_key"

    row = db.conn.execute(
        "SELECT place_id, latitude, longitude FROM keywords WHERE id = ?",
        (kw_id,),
    ).fetchone()
    assert row["place_id"] is None
    assert row["latitude"] is None
    assert row["longitude"] is None


def test_post_keyword_link_place_falls_back_on_malformed_client_geometry(
    app_and_db, monkeypatch,
):
    """Malformed client geometry should not turn a valid request into a 500."""
    import config as cfg
    import places
    app, db = app_and_db

    cfg.save({**cfg.load(), "google_maps_api_key": "FAKE-KEY"})
    captured = {}

    def fake_place_details(place_id, key):
        captured["place_id"] = place_id
        captured["key"] = key
        return _central_park_details()

    monkeypatch.setattr(places, "place_details", fake_place_details)
    kw_id = db.get_or_create_text_location("Central Park")
    place = _central_park_client_place()
    place["geometry"] = "bad"

    client = app.test_client()
    resp = client.post(
        f"/api/keywords/{kw_id}/link-place",
        json={
            "place_id": "ChIJ4zGFAZpYwokRGUGph3Mf37k",
            "place": place,
        },
    )

    assert resp.status_code == 200, resp.get_json()
    assert captured == {
        "place_id": "ChIJ4zGFAZpYwokRGUGph3Mf37k",
        "key": "FAKE-KEY",
    }
    assert resp.get_json()["keyword"]["place_id"] == "ChIJ4zGFAZpYwokRGUGph3Mf37k"


def test_post_keyword_link_place_ignores_malformed_component_types(
    app_and_db, monkeypatch,
):
    """Malformed component metadata should not reject otherwise valid details."""
    import config as cfg
    import places
    app, db = app_and_db

    cfg.save({**cfg.load(), "google_maps_api_key": ""})

    def fail_place_details(place_id, key):
        raise AssertionError("server-side Place Details should not be called")

    monkeypatch.setattr(places, "place_details", fail_place_details)
    kw_id = db.get_or_create_text_location("Central Park")
    place = _central_park_client_place()
    place["address_components"][0]["types"] = 42

    client = app.test_client()
    resp = client.post(
        f"/api/keywords/{kw_id}/link-place",
        json={
            "place_id": "ChIJ4zGFAZpYwokRGUGph3Mf37k",
            "place": place,
        },
    )

    assert resp.status_code == 200, resp.get_json()
    assert resp.get_json()["keyword"]["place_id"] == "ChIJ4zGFAZpYwokRGUGph3Mf37k"


def test_post_keyword_link_place_rejects_mismatched_client_place_id(app_and_db):
    """A client place payload must not bind one place_id to another place's geometry."""
    import config as cfg
    app, db = app_and_db

    cfg.save({**cfg.load(), "google_maps_api_key": ""})
    kw_id = db.get_or_create_text_location("Central Park")

    client = app.test_client()
    resp = client.post(
        f"/api/keywords/{kw_id}/link-place",
        json={
            "place_id": "ChIJDifferentPlace",
            "place": _central_park_client_place(),
        },
    )
    assert resp.status_code == 400
    assert resp.get_json()["error"] == "no_api_key"

    row = db.conn.execute(
        "SELECT place_id, latitude, longitude FROM keywords WHERE id = ?",
        (kw_id,),
    ).fetchone()
    assert row["place_id"] is None
    assert row["latitude"] is None
    assert row["longitude"] is None


def test_post_keyword_link_place_merges_when_place_id_already_taken(
    app_and_db, monkeypatch,
):
    """Second link to the same Google place merges into the canonical row."""
    import config as cfg
    import places
    app, db = app_and_db

    cfg.save({**cfg.load(), "google_maps_api_key": "FAKE-KEY"})
    monkeypatch.setattr(places, "place_details",
                        lambda place_id, key: _central_park_details())

    # Distinct names so they're separate rows in keywords.
    kw_a = db.get_or_create_text_location("Central Park A")
    kw_b = db.get_or_create_text_location("Central Park B")

    client = app.test_client()
    # First link -> kw_a becomes the place_id-bearing canonical row.
    r1 = client.post(
        f"/api/keywords/{kw_a}/link-place",
        json={"place_id": "ChIJ_x"},
    )
    assert r1.status_code == 200, r1.get_json()
    assert r1.get_json()["merged"] is False

    # Second link to the SAME place_id -> kw_b should be absorbed by kw_a.
    r2 = client.post(
        f"/api/keywords/{kw_b}/link-place",
        json={"place_id": "ChIJ_x"},
    )
    assert r2.status_code == 200, r2.get_json()
    data = r2.get_json()
    assert data["merged"] is True
    assert data["keyword"]["keyword_id"] == kw_a  # canonical wins

    # kw_b is gone from the DB after the merge.
    row = db.conn.execute(
        "SELECT 1 FROM keywords WHERE id = ?", (kw_b,),
    ).fetchone()
    assert row is None


def test_post_keyword_link_place_returns_404_on_missing_keyword(
    app_and_db, monkeypatch,
):
    """Unknown keyword id -> 404 keyword_not_found."""
    import config as cfg
    import places
    app, _ = app_and_db

    cfg.save({**cfg.load(), "google_maps_api_key": "FAKE-KEY"})
    monkeypatch.setattr(places, "place_details",
                        lambda place_id, key: _central_park_details())

    client = app.test_client()
    resp = client.post(
        "/api/keywords/999999/link-place",
        json={"place_id": "ChIJ_x"},
    )
    assert resp.status_code == 404
    body = resp.get_json()
    assert body["error"] == "keyword_not_found"
    assert body["code"] == "not_found"
    assert body["message"] == (
        "That saved location no longer exists. Refresh the page and select "
        "another location."
    )


def test_post_keyword_link_place_returns_400_on_wrong_keyword_type(
    app_and_db, monkeypatch,
):
    """Linking a non-'location' keyword should be a 400 wrong_keyword_type
    (the id exists, but it's the wrong kind), not a 404 keyword_not_found
    that misleadingly tells callers the id doesn't exist."""
    import config as cfg
    import places
    app, db = app_and_db

    cfg.save({**cfg.load(), "google_maps_api_key": "FAKE-KEY"})
    monkeypatch.setattr(places, "place_details",
                        lambda place_id, key: _central_park_details())

    # Create a 'general' keyword (not 'location').
    kw_id = db.conn.execute(
        "INSERT INTO keywords (name, type) VALUES ('Bird', 'general')",
    ).lastrowid
    db.conn.commit()

    client = app.test_client()
    resp = client.post(
        f"/api/keywords/{kw_id}/link-place",
        json={"place_id": "ChIJ_x"},
    )
    assert resp.status_code == 400
    body = resp.get_json()
    assert body["error"] == "wrong_keyword_type"
    assert body["code"] == "invalid_request"
    assert body["message"] == (
        "Only location keywords can be linked to Google Maps places."
    )
    assert "general" in body["error_detail"]


def test_post_keyword_link_place_returns_400_on_missing_place_id(app_and_db):
    """Empty body / missing place_id -> 400 (never reaches Google)."""
    app, db = app_and_db
    kw_id = db.get_or_create_text_location("Central Park")

    client = app.test_client()
    resp = client.post(f"/api/keywords/{kw_id}/link-place", json={})
    assert resp.status_code == 400
    assert resp.get_json()["error"] == "missing place_id"


def test_post_keyword_link_place_returns_400_on_empty_api_key(app_and_db):
    """No API key configured -> 400 no_api_key."""
    import config as cfg
    app, db = app_and_db
    cfg.save({**cfg.load(), "google_maps_api_key": ""})
    kw_id = db.get_or_create_text_location("Central Park")

    client = app.test_client()
    resp = client.post(
        f"/api/keywords/{kw_id}/link-place",
        json={"place_id": "ChIJ_x"},
    )
    assert resp.status_code == 400
    assert resp.get_json()["error"] == "no_api_key"


def test_post_keyword_link_place_returns_404_when_google_returns_none(
    app_and_db, monkeypatch,
):
    """Google returns None -> 404 place_not_found."""
    import config as cfg
    import places
    app, db = app_and_db

    cfg.save({**cfg.load(), "google_maps_api_key": "FAKE-KEY"})
    monkeypatch.setattr(places, "place_details", lambda place_id, key: None)

    kw_id = db.get_or_create_text_location("Central Park")
    client = app.test_client()
    resp = client.post(
        f"/api/keywords/{kw_id}/link-place",
        json={"place_id": "ChIJ_unknown"},
    )
    assert resp.status_code == 404
    assert resp.get_json()["error"] == "place_not_found"


def test_post_keyword_link_place_returns_409_on_cross_type_collision(
    app_and_db, monkeypatch,
):
    """Pre-existing non-location keyword in a non-root chain slot -> 409.

    SQLite's UNIQUE(name, parent_id) doesn't fire when ``parent_id`` is NULL
    (NULL != NULL), so collisions can only occur on non-root chain levels.
    We pre-build a ``location`` chain ``United States -> New York`` directly,
    then plant a ``general`` ``"New York County"`` under the state. The
    chain walk will try to INSERT a ``location`` ``"New York County"`` in
    the same slot, hitting UNIQUE(name, parent_id) and surfacing as 409.
    """
    import config as cfg
    import places
    app, db = app_and_db

    cfg.save({**cfg.load(), "google_maps_api_key": "FAKE-KEY"})
    monkeypatch.setattr(places, "place_details",
                        lambda place_id, key: _central_park_details())

    us_id = db.conn.execute(
        "INSERT INTO keywords (name, type, parent_id) VALUES (?, 'location', NULL)",
        ("United States",),
    ).lastrowid
    state_id = db.conn.execute(
        "INSERT INTO keywords (name, type, parent_id) VALUES (?, 'location', ?)",
        ("New York", us_id),
    ).lastrowid
    # Plant a 'general' "New York County" under the state — the chain walk
    # wants a 'location' row in this exact slot.
    db.conn.execute(
        "INSERT INTO keywords (name, type, parent_id) VALUES (?, 'general', ?)",
        ("New York County", state_id),
    )
    db.conn.commit()

    kw_id = db.get_or_create_text_location("Central Park")
    client = app.test_client()
    resp = client.post(
        f"/api/keywords/{kw_id}/link-place",
        json={"place_id": "ChIJ_x"},
    )
    assert resp.status_code == 409, resp.get_json()
    body = resp.get_json()
    assert body["error"] == "name_conflict"
    assert body["code"] == "name_conflict"
    assert "“New York County”" in body["message"]
    assert "Rename that keyword in Keywords" in body["message"]
    # The exception detail should mention the offending name for debugging.
    assert "New York County" in body.get("error_detail", "")


def test_post_keyword_link_place_records_edit(app_and_db, monkeypatch):
    """Successful link adds an audit-log entry under action_type='location_link'."""
    import config as cfg
    import places
    app, db = app_and_db

    cfg.save({**cfg.load(), "google_maps_api_key": "FAKE-KEY"})
    monkeypatch.setattr(places, "place_details",
                        lambda place_id, key: _central_park_details())

    kw_id = db.get_or_create_text_location("Central Park")

    pre_history = db.get_edit_history()
    pre_count = len(pre_history)

    client = app.test_client()
    resp = client.post(
        f"/api/keywords/{kw_id}/link-place",
        json={"place_id": "ChIJ_x"},
    )
    assert resp.status_code == 200, resp.get_json()

    post_history = db.get_edit_history()
    assert len(post_history) == pre_count + 1
    entry = post_history[0]
    assert entry["action_type"] == "location_link"
    assert "Central Park" in entry["description"]


# --- /api/photos/<id>/masks and /api/masks/<pid>/<variant>.png --------------

def _seed_mask(db, masks_dir, pid, variant, body=b"PNGBYTES"):
    """Helper: write a mask file on disk and insert a photo_masks row.

    Centralizes the bookkeeping the lightbox-variant tests need so each
    test only declares what's interesting (which variants exist for which
    photo, and which one is active).
    """
    import os as _os
    _os.makedirs(masks_dir, exist_ok=True)
    path = _os.path.join(masks_dir, f"{pid}.{variant}.png")
    with open(path, "wb") as fh:
        fh.write(body)
    db.upsert_photo_mask(
        photo_id=pid, variant=variant, path=path,
        detector_model="md", prompt_x=0, prompt_y=0, prompt_w=0, prompt_h=0,
    )
    return path


def test_api_photo_masks_lists_variants_and_active(app_and_db):
    """GET /api/photos/<id>/masks returns the photo's variants and the active one."""
    import os as _os
    app, db = app_and_db
    photos = db.get_photos()
    pid = photos[0]["id"]
    masks_dir = _os.path.join(_os.path.dirname(db._db_path), "masks")

    _seed_mask(db, masks_dir, pid, "sam2-small")
    _seed_mask(db, masks_dir, pid, "sam2-large")
    db.set_active_mask_variant(pid, "sam2-large")

    client = app.test_client()
    resp = client.get(f"/api/photos/{pid}/masks")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["photo_id"] == pid
    assert data["active"] == "sam2-large"
    variants = {v["variant"]: v for v in data["variants"]}
    assert set(variants) == {"sam2-small", "sam2-large"}
    assert variants["sam2-small"]["url"] == f"/api/masks/{pid}/sam2-small.png"
    assert "created_at" in variants["sam2-small"]


def test_api_photo_masks_empty_when_no_masks(app_and_db):
    """GET /api/photos/<id>/masks returns an empty list and null active."""
    app, db = app_and_db
    pid = db.get_photos()[0]["id"]
    client = app.test_client()
    resp = client.get(f"/api/photos/{pid}/masks")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["variants"] == []
    assert data["active"] is None


def test_api_serve_mask_png(app_and_db):
    """GET /api/masks/<pid>/<variant>.png serves the on-disk mask."""
    import os as _os
    app, db = app_and_db
    pid = db.get_photos()[0]["id"]
    masks_dir = _os.path.join(_os.path.dirname(db._db_path), "masks")
    _seed_mask(db, masks_dir, pid, "sam2-small", body=b"\x89PNGFAKE")

    client = app.test_client()
    resp = client.get(f"/api/masks/{pid}/sam2-small.png")
    assert resp.status_code == 200
    assert resp.data == b"\x89PNGFAKE"


def test_api_serve_mask_404_when_no_db_row(app_and_db):
    """Even if a file matching the pattern is on disk, no row → 404.

    Defense in depth: mask file existence alone must not be enough to
    serve an arbitrary file matching the URL pattern.
    """
    import os as _os
    app, db = app_and_db
    pid = db.get_photos()[0]["id"]
    masks_dir = _os.path.join(_os.path.dirname(db._db_path), "masks")
    _os.makedirs(masks_dir, exist_ok=True)
    # File on disk, but no photo_masks row.
    with open(_os.path.join(masks_dir, f"{pid}.sam2-small.png"), "wb") as fh:
        fh.write(b"x")

    client = app.test_client()
    resp = client.get(f"/api/masks/{pid}/sam2-small.png")
    assert resp.status_code == 404


def test_api_serve_mask_404_when_file_missing(app_and_db):
    """DB row exists but file missing → 404."""
    app, db = app_and_db
    pid = db.get_photos()[0]["id"]
    db.upsert_photo_mask(
        photo_id=pid, variant="sam2-small", path="/nope/missing.png",
        detector_model="md", prompt_x=0, prompt_y=0, prompt_w=0, prompt_h=0,
    )
    client = app.test_client()
    resp = client.get(f"/api/masks/{pid}/sam2-small.png")
    assert resp.status_code == 404


def test_api_serve_mask_rejects_path_traversal(app_and_db):
    """`..` segments and slashes must not allow escaping the masks dir.

    Flask's int converter on <pid> already blocks tricks at that segment,
    but the variant segment is a string. Any URL whose variant contains
    ``..`` or path separators must 404 — never reach the file system.
    """
    app, db = app_and_db
    pid = db.get_photos()[0]["id"]
    client = app.test_client()
    # `..%2F` would decode to `../` — must not let us climb out.
    resp = client.get(f"/api/masks/{pid}/..%2Fevil.png")
    assert resp.status_code == 404
    # Backslash variants should also fail validation.
    resp = client.get(f"/api/masks/{pid}/sam2..small.png")
    assert resp.status_code == 404


def test_api_serve_mask_uses_stored_db_path_for_legacy_filename(app_and_db):
    """Migrated legacy masks (filename ``{pid}.png``, no variant suffix)
    must still serve under their backfilled ``variant='unknown'`` row.

    The DB-init backfill inserts a ``photo_masks`` row pointing at the
    pre-existing ``{pid}.png`` file; reconstructing ``{pid}.unknown.png``
    would 404 even though the row and file are both present.
    """
    import os as _os
    app, db = app_and_db
    pid = db.get_photos()[0]["id"]
    masks_dir = _os.path.join(_os.path.dirname(db._db_path), "masks")
    _os.makedirs(masks_dir, exist_ok=True)
    legacy_path = _os.path.join(masks_dir, f"{pid}.png")
    with open(legacy_path, "wb") as fh:
        fh.write(b"LEGACYPNG")
    db.upsert_photo_mask(
        photo_id=pid, variant="unknown", path=legacy_path,
        detector_model="unknown",
        prompt_x=-1, prompt_y=-1, prompt_w=-1, prompt_h=-1,
    )

    client = app.test_client()
    resp = client.get(f"/api/masks/{pid}/unknown.png")
    assert resp.status_code == 200
    assert resp.data == b"LEGACYPNG"


def test_legacy_serve_mask_falls_back_to_active_variant(app_and_db):
    """``/masks/{pid}.png`` must keep working after variant-aware extraction.

    Callers like ``openInspect`` in pipeline_review.html still request
    the legacy URL. New masks are written as ``{pid}.{variant}.png`` and
    the DB ``active_mask_variant`` points at one of them, so the route
    must look up the active variant and serve from the stored path
    when the literal ``{pid}.png`` file no longer exists.
    """
    import os as _os
    app, db = app_and_db
    pid = db.get_photos()[0]["id"]
    masks_dir = _os.path.join(_os.path.dirname(db._db_path), "masks")
    _seed_mask(db, masks_dir, pid, "sam2-small", body=b"ACTIVEPNG")
    db.set_active_mask_variant(pid, "sam2-small")
    # No literal `{pid}.png` on disk.
    assert not _os.path.exists(_os.path.join(masks_dir, f"{pid}.png"))

    client = app.test_client()
    resp = client.get(f"/masks/{pid}.png")
    assert resp.status_code == 200
    assert resp.data == b"ACTIVEPNG"


def test_legacy_serve_mask_404_when_no_active_variant(app_and_db):
    """No file, no active variant → 404 (don't leak by serving any mask)."""
    app, db = app_and_db
    pid = db.get_photos()[0]["id"]
    client = app.test_client()
    resp = client.get(f"/masks/{pid}.png")
    assert resp.status_code == 404


def test_legacy_serve_mask_direct_file_still_served(app_and_db):
    """A literal ``{pid}.png`` on disk (legacy backfill) is served as-is."""
    import os as _os
    app, db = app_and_db
    pid = db.get_photos()[0]["id"]
    masks_dir = _os.path.join(_os.path.dirname(db._db_path), "masks")
    _os.makedirs(masks_dir, exist_ok=True)
    with open(_os.path.join(masks_dir, f"{pid}.png"), "wb") as fh:
        fh.write(b"LEGACYDIRECT")

    client = app.test_client()
    resp = client.get(f"/masks/{pid}.png")
    assert resp.status_code == 200
    assert resp.data == b"LEGACYDIRECT"


def test_api_serve_mask_rejects_path_outside_masks_dir(app_and_db, tmp_path):
    """A DB row whose ``path`` resolves outside the masks dir must 404.

    Defense in depth: even if the file exists and the ``photo_masks`` row
    looks valid, serving an arbitrary on-disk file from a corrupted /
    attacker-controlled DB row would be a directory-escape primitive.
    """
    app, db = app_and_db
    pid = db.get_photos()[0]["id"]
    outside = tmp_path / "outside.png"
    outside.write_bytes(b"SECRET")
    db.upsert_photo_mask(
        photo_id=pid, variant="sam2-small", path=str(outside),
        detector_model="md", prompt_x=0, prompt_y=0, prompt_w=0, prompt_h=0,
    )

    client = app.test_client()
    resp = client.get(f"/api/masks/{pid}/sam2-small.png")
    assert resp.status_code == 404


# -- Regression tests: workspace verification and payload validation --


def _add_other_workspace_photo(db):
    """Create a photo whose folder is linked only to another workspace."""
    default_ws = db._active_workspace_id
    other_ws = db.create_workspace("Other")
    db.set_active_workspace(other_ws)
    fid = db.add_folder('/secret/other', name='secret-other')
    pid = db.add_photo(folder_id=fid, filename='hidden.jpg', extension='.jpg',
                       file_size=10, file_mtime=1.0)
    db.set_active_workspace(default_ws)
    return pid


def test_color_label_unknown_photo_returns_404(app_and_db):
    """POST /api/photos/<id>/color_label must 404 on a stale id instead of
    hitting the photo_color_labels FK and 500ing."""
    app, _db = app_and_db
    client = app.test_client()
    resp = client.post('/api/photos/999999/color_label', json={'color': 'red'})
    assert resp.status_code == 404


def test_color_label_rejects_cross_workspace_photo(app_and_db):
    """POST /api/photos/<id>/color_label must 403 for photos outside the
    active workspace, matching the rating/flag endpoints."""
    app, db = app_and_db
    hidden_pid = _add_other_workspace_photo(db)
    client = app.test_client()
    resp = client.post(f'/api/photos/{hidden_pid}/color_label',
                       json={'color': 'red'})
    assert resp.status_code == 403
    assert db.get_color_labels_for_photos([hidden_pid]) == {}


def test_batch_color_label_skips_stale_and_cross_workspace_ids(app_and_db):
    """POST /api/batch/color_label must filter out stale and cross-workspace
    ids (instead of 500ing mid-batch on the FK) and report what was applied."""
    app, db = app_and_db
    hidden_pid = _add_other_workspace_photo(db)
    valid_pid = db.conn.execute(
        "SELECT id FROM photos WHERE filename = 'bird1.jpg'").fetchone()["id"]
    client = app.test_client()

    resp = client.post('/api/batch/color_label',
                       json={'photo_ids': [valid_pid, 999999, hidden_pid],
                             'color': 'green'})
    assert resp.status_code == 200
    assert resp.get_json()["updated"] == 1
    assert db.get_color_label(valid_pid) == 'green'
    assert db.get_color_labels_for_photos([hidden_pid]) == {}


def test_photo_detail_rejects_cross_workspace_photo(app_and_db):
    """GET /api/photos/<id> exposes absolute path/xmp_path — it must 404 for
    photos hidden from the active workspace (mirrors serve_thumbnail)."""
    app, db = app_and_db
    hidden_pid = _add_other_workspace_photo(db)
    client = app.test_client()
    resp = client.get(f'/api/photos/{hidden_pid}')
    assert resp.status_code == 404


def test_photo_pipeline_rejects_cross_workspace_photo(app_and_db):
    """GET /api/photos/<id>/pipeline exposes folder_path — 404 outside ws."""
    app, db = app_and_db
    hidden_pid = _add_other_workspace_photo(db)
    client = app.test_client()
    resp = client.get(f'/api/photos/{hidden_pid}/pipeline')
    assert resp.status_code == 404


def test_image_routes_reject_cross_workspace_photo(app_and_db):
    """Image-serving routes must not leak bytes across workspaces."""
    app, db = app_and_db
    hidden_pid = _add_other_workspace_photo(db)
    client = app.test_client()
    for path in (f'/photos/{hidden_pid}/preview',
                 f'/photos/{hidden_pid}/full',
                 f'/photos/{hidden_pid}/original',
                 f'/photos/{hidden_pid}/crop'):
        resp = client.get(path)
        assert resp.status_code == 404, f"expected 404 for {path}"


def test_api_detections_rejects_cross_workspace_photo(app_and_db):
    """GET /api/detections/<id> must 404 for photos outside the workspace."""
    app, db = app_and_db
    hidden_pid = _add_other_workspace_photo(db)
    db.save_detections(hidden_pid, [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
         "confidence": 0.9, "category": "animal"},
    ], detector_model="MDV6")
    client = app.test_client()
    resp = client.get(f'/api/detections/{hidden_pid}')
    assert resp.status_code == 404


def test_collection_add_photos_rejects_non_integer_ids(app_and_db):
    """POST /api/collections/<id>/add-photos must 400 on non-int entries —
    strings would persist into the photo_ids rule where they never match,
    and mixed int/str payloads crash sorted()."""
    import json

    app, db = app_and_db
    client = app.test_client()
    resp = client.post('/api/collections', json={'name': 'Static'})
    cid = resp.get_json()['id']
    pid = db.conn.execute("SELECT id FROM photos LIMIT 1").fetchone()["id"]
    rules_before = db.conn.execute(
        "SELECT rules FROM collections WHERE id = ?", (cid,)).fetchone()["rules"]

    for bad_ids in (["1", "2"], [pid, str(pid + 1)], [pid, True], "1,2"):
        resp = client.post(f'/api/collections/{cid}/add-photos',
                           json={'photo_ids': bad_ids})
        assert resp.status_code == 400, f"expected 400 for {bad_ids!r}"

    # The collection's rules must be untouched by the rejected requests.
    rules_after = db.conn.execute(
        "SELECT rules FROM collections WHERE id = ?", (cid,)).fetchone()["rules"]
    assert json.loads(rules_after) == json.loads(rules_before)

    # Valid ints still work.
    resp = client.post(f'/api/collections/{cid}/add-photos',
                       json={'photo_ids': [pid]})
    assert resp.status_code == 200
    assert resp.get_json()["total"] == 1


def test_edit_presets_api_crud(app_and_db):
    app, db = app_and_db
    client = app.test_client()

    assert client.get("/api/edit-presets").get_json() == {"presets": []}

    resp = client.post(
        "/api/edit-presets",
        json={
            "name": "Backlit",
            "recipe": {
                "rotation": 90,
                "adjustments": {"exposure": 1, "sharpen": 30},
            },
        },
    )
    assert resp.status_code == 200
    preset = resp.get_json()["preset"]
    assert preset["name"] == "Backlit"
    assert preset["recipe"] == {
        "version": 1,
        "adjustments": {"exposure": 1.0, "sharpen": 30.0},
    }

    # Upsert by name keeps the id, replaces the recipe.
    resp = client.post(
        "/api/edit-presets",
        json={"name": " Backlit ", "recipe": {"adjustments": {"shadows": 25}}},
    )
    assert resp.status_code == 200
    updated = resp.get_json()["preset"]
    assert updated["id"] == preset["id"]
    assert updated["recipe"]["adjustments"] == {"shadows": 25.0}

    client.post(
        "/api/edit-presets",
        json={"name": "alpine", "recipe": {"adjustments": {"contrast": 5}}},
    )
    names = [p["name"] for p in client.get("/api/edit-presets").get_json()["presets"]]
    assert names == ["alpine", "Backlit"]

    assert client.delete(f"/api/edit-presets/{preset['id']}").status_code == 200
    assert client.delete(f"/api/edit-presets/{preset['id']}").status_code == 404
    names = [p["name"] for p in client.get("/api/edit-presets").get_json()["presets"]]
    assert names == ["alpine"]


def test_edit_presets_api_validation(app_and_db):
    app, db = app_and_db
    client = app.test_client()

    cases = [
        {"recipe": {"adjustments": {"exposure": 1}}},              # no name
        {"name": "  ", "recipe": {"adjustments": {"exposure": 1}}},
        {"name": "x" * 200, "recipe": {"adjustments": {"exposure": 1}}},
        {"name": "ok"},                                            # no recipe
        {"name": "ok", "recipe": 5},
        {"name": "ok", "recipe": {"rotation": 90}},                # geometry only
        {"name": "ok", "recipe": {"adjustments": {"exposure": 0}}},
        {"name": "ok", "recipe": {"adjustments": {"exposure": 99}}},  # out of range
    ]
    for payload in cases:
        resp = client.post("/api/edit-presets", json=payload)
        assert resp.status_code == 400, payload

    assert client.get("/api/edit-presets").get_json() == {"presets": []}


def _register_active_mask(db, photo_id, mask_dir, width=800, height=600):
    """Write a left-half subject mask PNG and make it the photo's active mask."""
    import numpy as np
    from PIL import Image as PILImage

    path = os.path.join(mask_dir, f"{photo_id}.sam2-small.png")
    arr = np.zeros((height, width), dtype=np.uint8)
    arr[:, : width // 2] = 255
    PILImage.fromarray(arr, "L").save(path, "PNG")
    db.upsert_photo_mask(
        photo_id, "sam2-small", path, "megadetector-v6",
        0.0, 0.0, 0.5, 1.0,
    )
    db.set_active_mask_variant(photo_id, "sam2-small")
    return path


def _local_recipe_payload(mask, regions=None):
    return {
        "recipe": {
            "local": {
                "mask": mask,
                "regions": regions or [
                    {"region": "subject", "adjustments": {"exposure": 2.0}},
                ],
            }
        }
    }


def test_local_mask_snapshot_endpoint_requires_active_mask(client_with_photo):
    app, db, photo_id = client_with_photo
    client = app.test_client()

    resp = client.post(f"/api/photos/{photo_id}/local-mask/snapshot")
    assert resp.status_code == 400
    assert "mask" in resp.get_json()["error"].lower()

    assert client.post("/api/photos/999999/local-mask/snapshot").status_code == 404


def test_local_mask_snapshot_and_recipe_roundtrip(client_with_photo):
    app, db, photo_id = client_with_photo
    client = app.test_client()
    folder = db.conn.execute("SELECT path FROM folders").fetchone()
    _register_active_mask(db, photo_id, folder["path"])

    resp = client.post(f"/api/photos/{photo_id}/local-mask/snapshot")
    assert resp.status_code == 200
    body = resp.get_json()
    mask = body["mask"]
    assert set(mask) == {"ref", "source_digest"}
    assert body["stale"] is False

    resp = client.put(
        f"/api/photos/{photo_id}/edit-recipe",
        json=_local_recipe_payload(mask),
    )
    assert resp.status_code == 200
    saved = resp.get_json()["recipe"]
    assert saved["local"]["mask"]["ref"] == mask["ref"]

    got = client.get(f"/api/photos/{photo_id}/edit-recipe").get_json()
    assert got["recipe"]["local"]["mask"]["ref"] == mask["ref"]
    assert got["local_mask_stale"] is False


def test_local_recipe_stale_flag_tracks_live_mask(client_with_photo):
    import numpy as np
    from PIL import Image as PILImage

    app, db, photo_id = client_with_photo
    client = app.test_client()
    folder = db.conn.execute("SELECT path FROM folders").fetchone()
    mask_path = _register_active_mask(db, photo_id, folder["path"])

    mask = client.post(
        f"/api/photos/{photo_id}/local-mask/snapshot"
    ).get_json()["mask"]
    client.put(
        f"/api/photos/{photo_id}/edit-recipe",
        json=_local_recipe_payload(mask),
    )

    # Rewrite the live mask (as a detector/SAM re-run would).
    arr = np.zeros((600, 800), dtype=np.uint8)
    arr[:300, :] = 255
    PILImage.fromarray(arr, "L").save(mask_path, "PNG")

    got = client.get(f"/api/photos/{photo_id}/edit-recipe").get_json()
    assert got["local_mask_stale"] is True


def test_local_recipe_rejects_unknown_snapshot_ref(client_with_photo):
    app, db, photo_id = client_with_photo
    client = app.test_client()

    resp = client.put(
        f"/api/photos/{photo_id}/edit-recipe",
        json=_local_recipe_payload(
            {"ref": "eeeeeeeeeeee", "source_digest": "d"}
        ),
    )
    assert resp.status_code == 400
    assert "snapshot" in resp.get_json()["error"].lower()


def test_bulk_apply_local_skips_photos_without_masks(client_with_photo):
    """Bulk apply with a local section re-snapshots per target (PR 2);
    targets without a usable active mask are skipped and reported instead
    of silently receiving a wrong mask."""
    app, db, photo_id = client_with_photo
    client = app.test_client()

    resp = client.post(
        "/api/photos/edit-recipe/apply",
        json={
            "photo_ids": [photo_id],
            "recipe": _local_recipe_payload(
                {"ref": "eeeeeeeeeeee", "source_digest": "d"}
            )["recipe"],
        },
    )
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["applied"] == []
    assert data["skipped"] == [photo_id]
    assert "mask" in data["local_errors"][str(photo_id)].lower()
    assert db.get_photo_edit_recipe(photo_id) is None


def test_edit_preview_renders_local_adjustments(client_with_photo):
    import io
    import json

    import numpy as np
    from PIL import Image as PILImage

    app, db, photo_id = client_with_photo
    client = app.test_client()
    folder = db.conn.execute("SELECT path FROM folders").fetchone()
    _register_active_mask(db, photo_id, folder["path"])

    mask = client.post(
        f"/api/photos/{photo_id}/local-mask/snapshot"
    ).get_json()["mask"]
    recipe = _local_recipe_payload(mask)["recipe"]

    resp = client.get(
        f"/photos/{photo_id}/edit-preview",
        query_string={"size": "800", "recipe": json.dumps(recipe)},
    )
    assert resp.status_code == 200
    with PILImage.open(io.BytesIO(resp.data)) as img:
        arr = np.asarray(img.convert("RGB")).astype(np.float32)

    left = float(np.mean(arr[:, :360]))
    right = float(np.mean(arr[:, 440:]))
    assert left > right + 40  # subject half brightened by +2 EV


def test_local_snapshot_root_matches_thumb_cache_dir(tmp_path, monkeypatch):
    """Regression: renders must read snapshots from the same root the
    snapshot endpoint writes to.

    When ``create_app`` is given a ``thumb_cache_dir`` whose parent
    differs from ``dirname(db_path)``, snapshots land under
    ``dirname(THUMB_CACHE_DIR)/edit-masks`` but earlier revisions of
    every render call site looked in ``dirname(db_path)/edit-masks`` and
    silently rendered without the local pass. This test lays out that
    split-directory topology and asserts the local adjustment actually
    lands in the preview.
    """
    import io
    import json

    import numpy as np
    from PIL import Image as PILImage

    monkeypatch.setenv("HOME", str(tmp_path))
    import config as cfg
    import models
    from app import create_app
    from db import Database

    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    monkeypatch.setattr(
        models, "DEFAULT_MODELS_DIR", str(tmp_path / "vireo-models"),
    )
    monkeypatch.setattr(
        models, "CONFIG_PATH", str(tmp_path / "models.json"),
    )

    photos_dir = tmp_path / "photos"
    photos_dir.mkdir()
    src = photos_dir / "test.jpg"
    PILImage.new("RGB", (800, 600), (180, 90, 40)).save(
        str(src), "JPEG", quality=85,
    )

    # Key: db_root and cache_root have distinct parents. dirname(db_path)
    # is now unrelated to dirname(thumb_cache_dir); the app must not use
    # the former to find snapshots the endpoint saved under the latter.
    db_root = tmp_path / "db_root"
    db_root.mkdir()
    cache_root = tmp_path / "cache_root"
    cache_root.mkdir()
    thumb_dir = cache_root / "thumbnails"
    thumb_dir.mkdir()
    db_path = str(db_root / "vireo.db")

    db = Database(db_path)
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    fid = db.add_folder(str(photos_dir), name="photos")
    photo_id = db.add_photo(
        folder_id=fid, filename="test.jpg", extension=".jpg",
        file_size=os.path.getsize(src),
        file_mtime=os.path.getmtime(src),
        width=800, height=600,
    )

    _register_active_mask(db, photo_id, str(photos_dir))

    app = create_app(
        db_path=db_path, thumb_cache_dir=str(thumb_dir),
        api_token="test-token-123",
    )
    client = app.test_client()

    mask = client.post(
        f"/api/photos/{photo_id}/local-mask/snapshot"
    ).get_json()["mask"]

    # Snapshot must have been written under the thumb-cache root, not
    # under the db root.
    assert (cache_root / "edit-masks").is_dir()
    assert not (db_root / "edit-masks").exists()

    recipe = _local_recipe_payload(mask)["recipe"]
    resp = client.get(
        f"/photos/{photo_id}/edit-preview",
        query_string={"size": "800", "recipe": json.dumps(recipe)},
    )
    assert resp.status_code == 200
    with PILImage.open(io.BytesIO(resp.data)) as img:
        arr = np.asarray(img.convert("RGB")).astype(np.float32)

    left = float(np.mean(arr[:, :360]))
    right = float(np.mean(arr[:, 440:]))
    # If the renderer used dirname(db_path) for the snapshot root the
    # local pass would silently no-op and both halves would match.
    assert left > right + 40, (
        f"local adjustment did not apply (left={left:.1f}, "
        f"right={right:.1f}); snapshot root likely disagrees with the "
        "endpoint's write root"
    )

    db.close()


def test_edit_mask_preview_requires_local_recipe(client_with_photo):
    app, db, photo_id = client_with_photo
    client = app.test_client()

    resp = client.get(
        f"/photos/{photo_id}/edit-mask-preview",
        query_string={"recipe": '{"rotation":90}'},
    )
    assert resp.status_code == 404
    assert client.get("/photos/999999/edit-mask-preview").status_code == 404


def test_edit_mask_preview_serves_transformed_weight_map(client_with_photo):
    import io
    import json

    import numpy as np
    from PIL import Image as PILImage

    app, db, photo_id = client_with_photo
    client = app.test_client()
    folder = db.conn.execute("SELECT path FROM folders").fetchone()
    _register_active_mask(db, photo_id, folder["path"])
    mask = client.post(
        f"/api/photos/{photo_id}/local-mask/snapshot"
    ).get_json()["mask"]

    recipe = _local_recipe_payload(mask)["recipe"]
    resp = client.get(
        f"/photos/{photo_id}/edit-mask-preview",
        query_string={"size": "800", "recipe": json.dumps(recipe)},
    )
    assert resp.status_code == 200
    assert resp.mimetype == "image/png"
    with PILImage.open(io.BytesIO(resp.data)) as img:
        assert img.mode == "RGBA"
        assert img.size == (800, 600)
        alpha = np.asarray(img)[..., 3].astype(np.float32)

    # Left (subject) half opaque-ish, right transparent.
    assert np.mean(alpha[:, :360]) > 60
    assert np.mean(alpha[:, 440:]) < 5

    # Geometry rides along: flipping horizontally moves the weight.
    recipe_flipped = dict(recipe)
    recipe_flipped["flip"] = {"horizontal": True}
    resp = client.get(
        f"/photos/{photo_id}/edit-mask-preview",
        query_string={"size": "800", "recipe": json.dumps(recipe_flipped)},
    )
    with PILImage.open(io.BytesIO(resp.data)) as img:
        alpha = np.asarray(img)[..., 3].astype(np.float32)
    assert np.mean(alpha[:, 440:]) > 60
    assert np.mean(alpha[:, :360]) < 5

    # The editor preview is uncropped, so the overlay ignores crop too.
    recipe_cropped = dict(recipe)
    recipe_cropped["crop"] = {"x": 0.5, "y": 0.0, "w": 0.5, "h": 1.0}
    resp = client.get(
        f"/photos/{photo_id}/edit-mask-preview",
        query_string={"size": "800", "recipe": json.dumps(recipe_cropped)},
    )
    with PILImage.open(io.BytesIO(resp.data)) as img:
        assert img.size == (800, 600)


def test_edit_mask_preview_feather_scale_matches_saved_render(client_with_photo):
    """Overlay feather uses the SAVED (cropped) render's scale.

    Regression: the endpoint strips crop before calling ``local_weight_map``
    so the overlay lines up with the uncropped editor preview, but the
    feather scale must still come from the original (cropped) recipe —
    that is what ``/edit-preview`` passes to
    ``apply_recipe_to_loaded_image`` as ``detail_scale``. Otherwise a
    cropped recipe's overlay halo drifts from the pixels the saved render
    actually weights.
    """
    import io
    import json

    import numpy as np
    from PIL import Image as PILImage

    app, db, photo_id = client_with_photo
    client = app.test_client()
    folder = db.conn.execute("SELECT path FROM folders").fetchone()
    _register_active_mask(db, photo_id, folder["path"])
    mask = client.post(
        f"/api/photos/{photo_id}/local-mask/snapshot"
    ).get_json()["mask"]

    # Large feather + tight crop so the crop-derived scale doubles the
    # crop-stripped one: photo native = 800x600, size = 400.
    #   Old (buggy) scale = 400/800 = 0.5 (crop-stripped, uncropped long)
    #   New scale         = 320/320 = 1.0 (saved cropped render)
    # so the fix should ~2x the Gaussian-feather sigma.
    payload = _local_recipe_payload(dict(mask, feather=50.0))
    payload["recipe"]["crop"] = {"x": 0.1, "y": 0.1, "w": 0.4, "h": 0.4}
    recipe_cropped = payload["recipe"]
    recipe_full = dict(recipe_cropped)
    recipe_full.pop("crop")

    def _overlay_alpha(recipe):
        resp = client.get(
            f"/photos/{photo_id}/edit-mask-preview",
            query_string={"size": "400", "recipe": json.dumps(recipe)},
        )
        assert resp.status_code == 200
        with PILImage.open(io.BytesIO(resp.data)) as img:
            assert img.size == (400, 300)
            return np.asarray(img)[..., 3].astype(np.float32)

    alpha_cropped = _overlay_alpha(recipe_cropped)
    alpha_full = _overlay_alpha(recipe_full)

    # Both overlays are aligned with the uncropped preview (400x300), so
    # the mask's left/right split sits at the same column in each. The
    # difference is only the feather sigma.
    def _transition_width(alpha):
        # Count columns whose mid-row alpha lies in the Gaussian
        # transition band around the step edge at x ~ 200.
        row = alpha[150]
        return int(np.count_nonzero((row > 5) & (row < 145)))

    w_cropped = _transition_width(alpha_cropped)
    w_full = _transition_width(alpha_full)
    assert w_cropped > w_full + 20, (
        f"cropped-recipe overlay halo ({w_cropped}px) should be materially "
        f"wider than the uncropped ({w_full}px) — the endpoint likely used "
        "the crop-stripped recipe's scale instead of the saved-render scale."
    )


def test_edit_mask_preview_missing_snapshot_404s(client_with_photo):
    import json

    app, db, photo_id = client_with_photo
    client = app.test_client()

    recipe = _local_recipe_payload(
        {"ref": "eeeeeeeeeeee", "source_digest": "d"}
    )["recipe"]
    resp = client.get(
        f"/photos/{photo_id}/edit-mask-preview",
        query_string={"recipe": json.dumps(recipe)},
    )
    assert resp.status_code == 404


def test_bulk_apply_resnapshots_local_per_target(client_with_photo):
    import numpy as np
    from PIL import Image as PILImage

    app, db, photo_id = client_with_photo
    client = app.test_client()
    folder = db.conn.execute("SELECT path FROM folders").fetchone()

    # Second photo with its own (different) mask; third with no mask at all.
    src2 = os.path.join(folder["path"], "second.jpg")
    PILImage.new("RGB", (800, 600), (90, 120, 60)).save(src2, "JPEG")
    pid2 = db.add_photo(
        folder_id=db.conn.execute("SELECT id FROM folders").fetchone()["id"],
        filename="second.jpg", extension=".jpg",
        file_size=os.path.getsize(src2), file_mtime=os.path.getmtime(src2),
        width=800, height=600,
    )
    src3 = os.path.join(folder["path"], "third.jpg")
    PILImage.new("RGB", (800, 600), (10, 20, 30)).save(src3, "JPEG")
    pid3 = db.add_photo(
        folder_id=db.conn.execute("SELECT id FROM folders").fetchone()["id"],
        filename="third.jpg", extension=".jpg",
        file_size=os.path.getsize(src3), file_mtime=os.path.getmtime(src3),
        width=800, height=600,
    )

    _register_active_mask(db, photo_id, folder["path"])
    # pid2 gets a top-half mask so its snapshot differs from pid1's.
    path2 = os.path.join(folder["path"], f"{pid2}.sam2-small.png")
    arr = np.zeros((600, 800), dtype=np.uint8)
    arr[:300, :] = 255
    PILImage.fromarray(arr, "L").save(path2, "PNG")
    db.upsert_photo_mask(
        pid2, "sam2-small", path2, "megadetector-v6", 0.0, 0.0, 1.0, 0.5,
    )
    db.set_active_mask_variant(pid2, "sam2-small")

    source_mask = client.post(
        f"/api/photos/{photo_id}/local-mask/snapshot"
    ).get_json()["mask"]
    recipe = _local_recipe_payload(source_mask)["recipe"]

    resp = client.post(
        "/api/photos/edit-recipe/apply",
        json={"photo_ids": [photo_id, pid2, pid3], "recipe": recipe},
    )
    assert resp.status_code == 200
    data = resp.get_json()
    assert sorted(data["applied"]) == sorted([photo_id, pid2])
    assert pid3 in data["skipped"]
    assert str(pid3) in {str(k) for k in data.get("local_errors", {})}

    # Each target references its OWN snapshot, not the source photo's.
    r1 = db.get_photo_edit_recipe(photo_id)
    r2 = db.get_photo_edit_recipe(pid2)
    assert r1["local"]["mask"]["ref"] == source_mask["ref"]
    assert r2["local"]["mask"]["ref"] != source_mask["ref"]
    assert r2["local"]["regions"] == r1["local"]["regions"]
    assert db.get_photo_edit_recipe(pid3) is None

    # The response exposes a per-photo recipe map so the client cache stays
    # in sync — reusing `data.recipe` (the first applied) for every id
    # would silently poison non-first photos with the wrong mask ref until
    # a full refetch.
    recipes = data["recipes"]
    assert set(recipes) == {str(photo_id), str(pid2)}
    assert recipes[str(photo_id)]["local"]["mask"]["ref"] == r1["local"]["mask"]["ref"]
    assert recipes[str(pid2)]["local"]["mask"]["ref"] == r2["local"]["mask"]["ref"]
    assert recipes[str(pid2)]["local"]["mask"]["ref"] != recipes[str(photo_id)]["local"]["mask"]["ref"]


# ---------------------------------------------------------------------------
# Universal filter endpoints (Phase 1).
# Design: docs/plans/2026-07-19-universal-filters-design.md
# ---------------------------------------------------------------------------


def test_api_photos_query_basic(app_and_db):
    """POST /api/photos/query evaluates a rule tree; response shape matches
    /api/photos so pages can switch fetch paths without renderer changes."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.post('/api/photos/query', json={
        "rules": [{"field": "rating", "op": ">=", "value": 4}],
    })
    assert resp.status_code == 200
    data = resp.get_json()
    assert set(data) == {"photos", "total", "page", "per_page"}
    assert data["total"] == 1
    assert [p["filename"] for p in data["photos"]] == ["bird3.jpg"]
    # species attachment matches /api/photos behavior
    assert "species" in data["photos"][0]


def test_api_photos_query_group_tree_and_paging(app_and_db):
    app, _ = app_and_db
    client = app.test_client()
    resp = client.post('/api/photos/query', json={
        "rules": {"mode": "any", "rules": [
            {"field": "rating", "op": ">=", "value": 5},
            {"field": "filename", "op": "contains", "value": "bird1"},
        ]},
        "sort": "name",
        "per_page": 1,
        "page": 2,
    })
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["total"] == 2
    assert [p["filename"] for p in data["photos"]] == ["bird3.jpg"]


def test_api_photos_query_validation(app_and_db, monkeypatch):
    app, _ = app_and_db
    # Force an active visual model so the active-model injection walker
    # actually runs — that's the code path that must survive malformed
    # group rules without raising TypeError. Without a monkeypatch,
    # ``_inject_active_visual_model`` returns rules verbatim and the
    # malformed-group asserts below wouldn't exercise the walker at all.
    import models as models_mod
    monkeypatch.setattr(
        models_mod, "get_active_model",
        lambda: {"name": "some-model", "id": "some-model", "downloaded": True},
    )
    client = app.test_client()
    assert client.post('/api/photos/query', json={
        "rules": [{"field": "nope", "op": "is", "value": 1}],
    }).status_code == 400
    assert client.post('/api/photos/query', json={
        "rules": [], "page": 0,
    }).status_code == 400
    assert client.post('/api/photos/query', json={
        "rules": [], "per_page": "many",
    }).status_code == 400
    # sort must be a string — a JSON array/object would otherwise raise
    # TypeError inside ``sort_map.get(sort, ...)`` and surface as a 500.
    assert client.post('/api/photos/query', json={
        "rules": [], "sort": [],
    }).status_code == 400
    assert client.post('/api/photos/query', json={
        "rules": [], "sort": {"col": "date"},
    }).status_code == 400
    # A group whose ``rules`` key is ``null`` (or any non-list) must land as
    # a 400 from ``_validate_node``. Before the guard, the active-model
    # walker would iterate ``None`` and raise ``TypeError`` → 500.
    assert client.post('/api/photos/query', json={
        "rules": {"mode": "all", "rules": None},
    }).status_code == 400
    assert client.post('/api/photos/query', json={
        "rules": {"mode": "all", "rules": "not-a-list"},
    }).status_code == 400
    assert client.post('/api/photos/query', data="not json",
                       content_type="text/plain").status_code == 400
    # Unsupported operators on numeric fields must land as 400 (validation
    # error), not 200 with an empty result set — otherwise a malformed
    # rule like ``file_size contains 1`` looks like a query that legitimately
    # matched nothing. Covers all fields routed through _numeric_condition:
    # file_size/width/height/focal_length/aperture/shutter_speed/iso,
    # gps_lat/gps_lng, rating/quality_score/sharpness/subject_sharpness/
    # noise_estimate, keyword_count, and prediction_confidence.
    for field in (
        "file_size", "width", "height", "focal_length", "aperture",
        "shutter_speed", "iso", "gps_lat", "gps_lng", "rating",
        "quality_score", "sharpness", "noise_estimate", "keyword_count",
        "prediction_confidence",
    ):
        assert client.post('/api/photos/query', json={
            "rules": [{"field": field, "op": "contains", "value": 1}],
        }).status_code == 400, f"{field} contains 1 must 400"


def test_api_photos_query_has_visual_index_injects_active_model(app_and_db, monkeypatch):
    """``has_visual_index`` rules emitted from the UI have no ``model`` key,
    so the query API must inject the active visual model — otherwise
    photos with only stale embeddings from previously-active models would
    match, disagreeing with visual search (which loads only the active
    model's embeddings)."""
    app, db = app_and_db
    photos = {p["filename"]: p["id"] for p in db.get_photos()}
    # bird1 has a stale embedding from a former model; bird2 has one from
    # the currently-active model. Only bird2 should satisfy the filter.
    db.conn.execute(
        "INSERT INTO photo_embeddings(photo_id, model, variant, embedding) "
        "VALUES (?, 'old-model', '', ?)",
        (photos["bird1.jpg"], b"\x01"),
    )
    db.conn.execute(
        "INSERT INTO photo_embeddings(photo_id, model, variant, embedding) "
        "VALUES (?, 'current-model', '', ?)",
        (photos["bird2.jpg"], b"\x02"),
    )
    db.conn.commit()

    import models as models_mod
    monkeypatch.setattr(
        models_mod, "get_active_model",
        lambda: {"name": "current-model", "id": "current-model", "downloaded": True},
    )

    client = app.test_client()
    # No ``model`` on the rule — API layer must inject the active one.
    resp = client.post('/api/photos/query', json={
        "rules": [{"field": "has_visual_index", "op": "is", "value": 1}],
    })
    assert resp.status_code == 200
    filenames = sorted(p["filename"] for p in resp.get_json()["photos"])
    assert filenames == ["bird2.jpg"]

    # Nested rule groups get the same treatment.
    resp = client.post('/api/photos/query', json={
        "rules": {"mode": "all", "rules": [
            {"field": "has_visual_index", "op": "is", "value": 1},
        ]},
    })
    assert resp.status_code == 200
    filenames = sorted(p["filename"] for p in resp.get_json()["photos"])
    assert filenames == ["bird2.jpg"]

    # An explicit ``model`` in the rule wins over the injection.
    resp = client.post('/api/photos/query', json={
        "rules": [{"field": "has_visual_index", "op": "is", "value": 1,
                   "model": "old-model"}],
    })
    assert resp.status_code == 200
    filenames = sorted(p["filename"] for p in resp.get_json()["photos"])
    assert filenames == ["bird1.jpg"]


def test_api_photos_query_has_visual_index_fails_closed_without_active_model(
    app_and_db, monkeypatch
):
    """When no active visual model is configured — fresh library or every
    installed model was removed — a UI-emitted ``has_visual_index`` rule
    (no ``model`` key) must fail closed: ``is true`` matches nothing and
    ``is false`` matches everything. Otherwise the filter would keep
    matching stale embeddings from a removed model while
    ``/api/photos/search`` returns ``no_model``, so Browse and visual
    search would disagree on what "has a visual index" means.
    """
    app, db = app_and_db
    photos = {p["filename"]: p["id"] for p in db.get_photos()}
    # Every photo has a stale embedding — the previously-active model was
    # uninstalled but its rows remain in ``photo_embeddings``.
    for filename in photos:
        db.conn.execute(
            "INSERT INTO photo_embeddings(photo_id, model, variant, embedding) "
            "VALUES (?, 'removed-model', '', ?)",
            (photos[filename], b"\x01"),
        )
    db.conn.commit()

    import models as models_mod
    monkeypatch.setattr(models_mod, "get_active_model", lambda: None)

    client = app.test_client()
    # ``is true`` must match nothing — there is no usable visual index.
    resp = client.post('/api/photos/query', json={
        "rules": [{"field": "has_visual_index", "op": "is", "value": 1}],
    })
    assert resp.status_code == 200
    assert resp.get_json()["photos"] == []

    # ``is false`` must match every photo for the same reason.
    resp = client.post('/api/photos/query', json={
        "rules": [{"field": "has_visual_index", "op": "is", "value": 0}],
    })
    assert resp.status_code == 200
    filenames = sorted(p["filename"] for p in resp.get_json()["photos"])
    assert filenames == sorted(photos.keys())

    # An explicit ``model`` in the rule is still honored — a saved smart
    # collection with an old model name keeps working even when nothing is
    # active, so it stays portable.
    resp = client.post('/api/photos/query', json={
        "rules": [{"field": "has_visual_index", "op": "is", "value": 1,
                   "model": "removed-model"}],
    })
    assert resp.status_code == 200
    filenames = sorted(p["filename"] for p in resp.get_json()["photos"])
    assert filenames == sorted(photos.keys())


def test_api_photos_query_timestamp_gt_preserves_subsecond(app_and_db):
    """``timestamp > 2024-01-01T12:00:00`` must NOT pad the value to
    ``.999999`` — that would spuriously exclude sub-second photos in the
    same clock second (``12:00:00.5``) that are strictly greater than the
    requested instant. Only bare ``YYYY-MM-DD`` values advance to end of
    day."""
    app, db = app_and_db
    photos = {p["filename"]: p["id"] for p in db.get_photos()}
    db.conn.execute(
        "UPDATE photos SET timestamp='2024-01-01T12:00:00.500000' WHERE id=?",
        (photos["bird1.jpg"],),
    )
    db.conn.execute(
        "UPDATE photos SET timestamp='2024-01-02T09:00:00' WHERE id=?",
        (photos["bird2.jpg"],),
    )
    db.conn.execute(
        "UPDATE photos SET timestamp='2023-12-31T09:00:00' WHERE id=?",
        (photos["bird3.jpg"],),
    )
    db.conn.commit()
    client = app.test_client()

    # Precise timestamp comparison — bird1 at 12:00:00.5 IS strictly > 12:00:00
    resp = client.post('/api/photos/query', json={
        "rules": [{"field": "timestamp", "op": ">",
                   "value": "2024-01-01T12:00:00"}],
    })
    assert resp.status_code == 200
    filenames = sorted(p["filename"] for p in resp.get_json()["photos"])
    assert filenames == ["bird1.jpg", "bird2.jpg"]

    # Bare-date comparison — ``> 2024-01-01`` still means strictly after
    # the whole day, so bird1 (on that day) is excluded.
    resp = client.post('/api/photos/query', json={
        "rules": [{"field": "timestamp", "op": ">",
                   "value": "2024-01-01"}],
    })
    assert resp.status_code == 200
    filenames = sorted(p["filename"] for p in resp.get_json()["photos"])
    assert filenames == ["bird2.jpg"]


def test_api_filter_fields_registry(app_and_db):
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get('/api/filters/fields')
    assert resp.status_code == 200
    fields = {f["key"]: f for f in resp.get_json()["fields"]}
    assert "between" in fields["iso"]["ops"]
    assert fields["flag"]["values"] == ["flagged", "none", "rejected"]
    assert fields["camera_model"]["suggest"] is True
    assert fields["timestamp"]["type"] == "date"


def test_api_filter_values_counts(app_and_db):
    app, db = app_and_db
    photos = {p["filename"]: p["id"] for p in db.get_photos()}
    db.conn.execute("UPDATE photos SET camera_model='Sony A1' WHERE id IN (?, ?)",
                    (photos["bird1.jpg"], photos["bird3.jpg"]))
    db.conn.execute("UPDATE photos SET camera_model='Canon R5' WHERE id=?",
                    (photos["bird2.jpg"],))
    db.conn.commit()

    client = app.test_client()
    resp = client.get('/api/filters/values?field=camera_model')
    assert resp.status_code == 200
    assert resp.get_json()["values"] == [
        {"value": "Sony A1", "count": 2},
        {"value": "Canon R5", "count": 1},
    ]
    # Counts respect the expression-minus-edited-rule passed as ?rules=
    import json as _json
    rules = _json.dumps([{"field": "rating", "op": ">=", "value": 4}])
    resp = client.get(f'/api/filters/values?field=camera_model&rules={rules}')
    assert resp.get_json()["values"] == [{"value": "Sony A1", "count": 1}]
    # Typeahead query narrowing
    resp = client.get('/api/filters/values?field=camera_model&q=can')
    assert resp.get_json()["values"] == [{"value": "Canon R5", "count": 1}]


def test_api_filter_values_rejects_bad_input(app_and_db):
    app, _ = app_and_db
    client = app.test_client()
    assert client.get('/api/filters/values?field=rating').status_code == 400
    assert client.get('/api/filters/values?field=camera_model&rules=notjson').status_code == 400
    assert client.get(
        '/api/filters/values?field=camera_model&rules=[{"field":"nope","op":"is","value":1}]'
    ).status_code == 400


def test_api_filter_values_clamps_limit(app_and_db):
    """Zero/negative/oversized ``limit`` params must not produce unbounded
    queries (SQLite treats a negative ``LIMIT`` as "no limit")."""
    app, db = app_and_db
    photos = {p["filename"]: p["id"] for p in db.get_photos()}
    # Populate enough distinct camera_model values that a working limit clamp
    # is observable — a limit of 0/-1 without the clamp would return them all.
    for i, filename in enumerate(sorted(photos)):
        db.conn.execute(
            "UPDATE photos SET camera_model=? WHERE id=?",
            (f"Model{i}", photos[filename]),
        )
    db.conn.commit()
    client = app.test_client()
    # Negative and zero clamp up to 1 — always at least one result.
    for bad_limit in ("-5", "0"):
        resp = client.get(f'/api/filters/values?field=camera_model&limit={bad_limit}')
        assert resp.status_code == 200, bad_limit
        assert len(resp.get_json()["values"]) == 1, bad_limit
    # Absurdly large values clamp down to the server-side ceiling so a huge
    # library can't stream every distinct value out through one request.
    resp = client.get('/api/filters/values?field=camera_model&limit=999999')
    assert resp.status_code == 200
    assert len(resp.get_json()["values"]) <= 500


def test_api_filter_values_respects_scope(app_and_db):
    """Typeahead counts must respect the folder / dashboard-collection scope
    Browse passes to /api/photos/query; without this the badge beside each
    suggestion counts photos over the whole workspace while the grid is
    folder- or collection-restricted, and a pick can produce fewer visible
    grid rows than the count promised."""
    app, db = app_and_db
    photos = {p["filename"]: p["id"] for p in db.get_photos()}
    folders = db.get_folder_tree()
    jan = [f for f in folders if f["name"] == "January"][0]
    db.conn.execute(
        "UPDATE photos SET camera_model='Sony A1' WHERE id IN (?, ?)",
        (photos["bird1.jpg"], photos["bird3.jpg"]),
    )
    db.conn.execute(
        "UPDATE photos SET camera_model='Canon R5' WHERE id=?",
        (photos["bird2.jpg"],),
    )
    db.conn.commit()

    client = app.test_client()
    # Unscoped: sees both models.
    resp = client.get('/api/filters/values?field=camera_model')
    assert {v["value"] for v in resp.get_json()["values"]} == {"Sony A1", "Canon R5"}
    # folder_id narrows to only the January folder (bird2.jpg — Canon R5).
    resp = client.get(
        f'/api/filters/values?field=camera_model&folder_id={jan["id"]}'
    )
    assert resp.get_json()["values"] == [{"value": "Canon R5", "count": 1}]

    # collection_id narrows to only the two picked photos (bird1 + bird3 — Sony A1).
    collection_id = db.add_collection(
        "Two Sonys",
        json.dumps([{
            "field": "photo_ids",
            "value": [photos["bird1.jpg"], photos["bird3.jpg"]],
        }]),
    )
    resp = client.get(
        f'/api/filters/values?field=camera_model&collection_id={collection_id}'
    )
    assert resp.get_json()["values"] == [{"value": "Sony A1", "count": 2}]


def test_api_photos_query_ids_only(app_and_db):
    """ids_only returns the complete matching id set in display order —
    select-all must resolve exactly what the filtered grid shows."""
    app, db = app_and_db
    client = app.test_client()
    resp = client.post('/api/photos/query', json={
        "rules": [{"field": "rating", "op": ">=", "value": 3}],
        "ids_only": True,
        "sort": "name",
    })
    assert resp.status_code == 200
    data = resp.get_json()
    photos = {p["filename"]: p["id"] for p in db.get_photos()}
    assert data["ids"] == [photos["bird1.jpg"], photos["bird3.jpg"]]
    assert data["total"] == 2


def test_api_browse_summary_accepts_rules(app_and_db):
    import json as _json
    app, _ = app_and_db
    client = app.test_client()
    rules = _json.dumps([{"field": "rating", "op": ">=", "value": 4}])
    resp = client.get(f'/api/browse/summary?rules={rules}')
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["filtered_total"] == 1
    assert client.get('/api/browse/summary?rules=notjson').status_code == 400


def test_api_photos_calendar_accepts_rules(app_and_db):
    import json as _json
    app, _ = app_and_db
    client = app.test_client()
    rules = _json.dumps([{"field": "rating", "op": ">=", "value": 4}])
    resp = client.get(f'/api/photos/calendar?year=2024&rules={rules}')
    assert resp.status_code == 200
    # bird3.jpg (rating 5, 2024-06-10) is the only match in 2024
    assert resp.get_json()["days"] == {"2024-06-10": 1}
    assert client.get('/api/photos/calendar?rules=notjson').status_code == 400


# ---------------------------------------------------------------------------
# Universal filter visual clause (Phase 3).
# ---------------------------------------------------------------------------


def _stub_clip(monkeypatch, model_name="test-clip", model_type="bioclip"):
    import models
    monkeypatch.setattr(models, "get_active_model", lambda: {
        "name": model_name,
        "model_type": model_type,
        "model_str": "fake",
        "weights_path": "",
    } if model_name else None)
    monkeypatch.setitem(
        sys.modules,
        "text_encoder",
        types.SimpleNamespace(
            encode_text=lambda *_args, **_kwargs: np.array([1.0, 0.0], dtype=np.float32)
        ),
    )


def _seed_embeddings(db, model_name="test-clip"):
    photos = {p["filename"]: p["id"] for p in db.get_photos(sort="name")}
    for name, vec in [
        ("bird1.jpg", [0.95, 0.0]),
        ("bird2.jpg", [0.8, 0.0]),
        ("bird3.jpg", [0.05, 0.0]),
    ]:
        db.upsert_photo_embedding(
            photos[name], model_name, np.array(vec, dtype=np.float32).tobytes()
        )
    return photos


def test_api_photos_query_visual_ranks_by_similarity(app_and_db, monkeypatch):
    app, db = app_and_db
    photos = _seed_embeddings(db)
    _stub_clip(monkeypatch)
    client = app.test_client()
    resp = client.post('/api/photos/query', json={
        "rules": [],
        "visual": {"prompt": "a bird", "strength": "balanced"},
    })
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["visual"]["status"] == "ok"
    assert data["visual"]["matched"] == 2
    assert data["visual"]["indexed"] == 3
    assert [p["filename"] for p in data["photos"]] == ["bird1.jpg", "bird2.jpg"]
    assert data["photos"][0]["similarity"] == 0.95
    assert data["total"] == 2

    # Metadata rules constrain the candidate set before scoring.
    resp = client.post('/api/photos/query', json={
        "rules": [{"field": "filename", "op": "is", "value": "bird2.jpg"}],
        "visual": {"prompt": "a bird", "strength": "balanced"},
    })
    data = resp.get_json()
    assert [p["filename"] for p in data["photos"]] == ["bird2.jpg"]

    # Strength changes the threshold: strict drops bird2 (0.8 >= 0.22 stays;
    # broad keeps everything above 0.10).
    resp = client.post('/api/photos/query', json={
        "rules": [],
        "visual": {"prompt": "a bird", "strength": "broad"},
    })
    assert resp.get_json()["visual"]["matched"] == 2

    # ids_only returns the ranked id set.
    resp = client.post('/api/photos/query', json={
        "rules": [], "ids_only": True,
        "visual": {"prompt": "a bird", "strength": "balanced"},
    })
    data = resp.get_json()
    assert data["ids"] == [photos["bird1.jpg"], photos["bird2.jpg"]]


def test_api_photos_query_visual_error_states(app_and_db, monkeypatch):
    app, db = app_and_db
    client = app.test_client()

    # No active model: metadata rules still apply, status is surfaced.
    _stub_clip(monkeypatch, model_name=None)
    resp = client.post('/api/photos/query', json={
        "rules": [{"field": "rating", "op": ">=", "value": 4}],
        "visual": {"prompt": "a bird"},
    })
    data = resp.get_json()
    assert data["visual"]["status"] == "no_model"
    assert data["total"] == 1  # metadata-only fallback, not zero

    # timm models have no text tower.
    _stub_clip(monkeypatch, model_type="timm")
    resp = client.post('/api/photos/query', json={
        "rules": [], "visual": {"prompt": "a bird"},
    })
    assert resp.get_json()["visual"]["status"] == "model_no_text_search"

    # No embeddings stored for the active model.
    _stub_clip(monkeypatch)
    resp = client.post('/api/photos/query', json={
        "rules": [], "visual": {"prompt": "a bird"},
    })
    data = resp.get_json()
    assert data["visual"]["status"] == "no_embeddings"
    assert data["total"] == 3

    # Malformed clauses 400.
    assert client.post('/api/photos/query', json={
        "rules": [], "visual": {"prompt": "   "},
    }).status_code == 400
    assert client.post('/api/photos/query', json={
        "rules": [], "visual": {"prompt": "x", "strength": "extreme"},
    }).status_code == 400


def test_api_summary_and_values_respect_visual(app_and_db, monkeypatch):
    """Facet counts and the summary must describe the visually-filtered set."""
    import json as _json
    app, db = app_and_db
    _seed_embeddings(db)
    _stub_clip(monkeypatch)
    photos = {p["filename"]: p["id"] for p in db.get_photos(sort="name")}
    db.conn.execute("UPDATE photos SET camera_model='Sony A1' WHERE id IN (?, ?)",
                    (photos["bird1.jpg"], photos["bird3.jpg"]))
    db.conn.commit()

    client = app.test_client()
    visual = _json.dumps({"prompt": "a bird", "strength": "balanced"})
    resp = client.get(f'/api/browse/summary?visual={visual}')
    assert resp.status_code == 200
    assert resp.get_json()["filtered_total"] == 2  # bird1 + bird2 only

    # bird3 (Sony, below threshold) must not appear in facet counts.
    resp = client.get(f'/api/filters/values?field=camera_model&visual={visual}')
    assert resp.get_json()["values"] == [{"value": "Sony A1", "count": 1}]


def test_api_calendar_scopes_visual_to_collection(app_and_db, monkeypatch):
    """Calendar's visual clause must resolve within the collection scope.

    If the collection has no active-model embeddings but the wider
    workspace does, the clause must fail 'no_embeddings' and fall back
    to metadata-only within the collection — matching the grid — rather
    than injecting outside-collection matches that get intersected away.
    """
    app, db = app_and_db
    photos = {p["filename"]: p["id"] for p in db.get_photos(sort="name")}
    for name, vec in [("bird1.jpg", [0.95, 0.0]), ("bird2.jpg", [0.8, 0.0])]:
        db.upsert_photo_embedding(
            photos[name], "test-clip", np.array(vec, dtype=np.float32).tobytes()
        )
    # bird3 (the only photo in the collection) has no embedding.
    collection_id = db.add_collection(
        "Only bird3",
        json.dumps([{"field": "photo_ids", "value": [photos["bird3.jpg"]]}]),
    )
    _stub_clip(monkeypatch)
    client = app.test_client()
    visual = json.dumps({"prompt": "a bird", "strength": "balanced"})
    resp = client.get(
        f"/api/photos/calendar?year=2024&collection_id={collection_id}&visual={visual}"
    )
    assert resp.status_code == 200
    assert list(resp.get_json()["days"].keys()) == ["2024-06-10"]


def test_embedding_fetch_chunks_over_999_ids(app_and_db):
    """A broad rule tree passes every photo id as the candidate set; the
    embedding fetch must chunk below SQLITE_MAX_VARIABLE_NUMBER."""
    _, db = app_and_db
    fid = db.get_photos()[0]["folder_id"]
    ids = []
    rows = []
    for i in range(1200):
        pid = db.add_photo(folder_id=fid, filename=f"bulk{i}.jpg", extension=".jpg",
                           file_size=10, file_mtime=1.0)
        ids.append(pid)
        rows.append(pid)
    vec = np.array([0.5, 0.5], dtype=np.float32).tobytes()
    for pid in rows:
        db.upsert_photo_embedding(pid, "test-clip", vec)
    pairs = db.get_photos_with_embedding("test-clip", photo_ids=ids)
    assert len(pairs) == 1200


# ---------------------------------------------------------------------------
# Phase 4: rules on /api/photos/geo and /api/predictions.
# ---------------------------------------------------------------------------


def test_api_photos_geo_accepts_rules(app_and_db):
    import json as _json
    app, db = app_and_db
    photos = {p["filename"]: p["id"] for p in db.get_photos()}
    db.conn.execute(
        "UPDATE photos SET latitude=37.7, longitude=-122.4 WHERE id IN (?, ?)",
        (photos["bird1.jpg"], photos["bird3.jpg"]))
    db.conn.commit()

    client = app.test_client()
    resp = client.get('/api/photos/geo')
    assert resp.status_code == 200
    assert resp.get_json()["total_filtered"] == 2

    rules = _json.dumps([{"field": "rating", "op": ">=", "value": 4}])
    resp = client.get(f'/api/photos/geo?rules={rules}')
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["total_filtered"] == 1  # bird3: rating 5 + coordinates
    assert data["photos"][0]["id"] == photos["bird3.jpg"]
    assert client.get('/api/photos/geo?rules=notjson').status_code == 400


def test_api_predictions_accepts_rules(app_and_db):
    import json as _json
    app, db = app_and_db
    from labels_fingerprint import TOL_SENTINEL
    photos = {p["filename"]: p["id"] for p in db.get_photos()}
    for name, species in [("bird1.jpg", "Cardinal"), ("bird3.jpg", "Blue Jay")]:
        det_ids = db.save_detections(photos[name], [
            {"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
             "confidence": 0.9, "category": "animal"},
        ], detector_model="test-detector")
        db.add_prediction(
            detection_id=det_ids[0], species=species, confidence=0.9,
            model="TestModel", labels_fingerprint=TOL_SENTINEL,
        )

    client = app.test_client()
    resp = client.get('/api/predictions')
    assert resp.status_code == 200
    assert len(resp.get_json()) == 2

    rules = _json.dumps([{"field": "rating", "op": ">=", "value": 4}])
    resp = client.get(f'/api/predictions?rules={rules}')
    assert resp.status_code == 200
    preds = resp.get_json()
    assert len(preds) == 1
    assert preds[0]["photo_id"] == photos["bird3.jpg"]
    assert client.get('/api/predictions?rules=notjson').status_code == 400


def test_api_photos_geo_surfaces_visual_status(app_and_db, monkeypatch):
    """Map endpoint must return the visual clause status so the filter
    bar can warn on fallback. Without this, choosing "Visually similar…"
    on a map with no active model / no embeddings would silently return
    every metadata-matching plottable photo while the visual chip
    remained on-screen.
    """
    import json as _json
    app, db = app_and_db
    photos = {p["filename"]: p["id"] for p in db.get_photos()}
    db.conn.execute(
        "UPDATE photos SET latitude=37.7, longitude=-122.4 WHERE id IN (?, ?, ?)",
        (photos["bird1.jpg"], photos["bird2.jpg"], photos["bird3.jpg"]))
    db.conn.commit()
    client = app.test_client()

    visual = _json.dumps({"prompt": "a bird", "strength": "balanced"})

    # Healthy visual clause: response reports status ok and ranks by
    # similarity within the plottable set.
    _seed_embeddings(db)
    _stub_clip(monkeypatch)
    resp = client.get(f'/api/photos/geo?visual={visual}')
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["visual"]["status"] == "ok"
    assert data["visual"]["matched"] == 2
    assert data["total_filtered"] == 2
    assert {p["id"] for p in data["photos"]} == {
        photos["bird1.jpg"], photos["bird2.jpg"]
    }

    # No active model: fallback to metadata-only, but status is surfaced
    # so the visual chip can warn. Without this the response would look
    # identical to a healthy clause with a broader match.
    _stub_clip(monkeypatch, model_name=None)
    resp = client.get(f'/api/photos/geo?visual={visual}')
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["visual"]["status"] == "no_model"
    assert data["total_filtered"] == 3  # all plottable photos, unchanged

    # No visual clause: no ``visual`` key in the response (the filter
    # bar's visual note should be hidden, not stale).
    resp = client.get('/api/photos/geo')
    assert resp.status_code == 200
    assert "visual" not in resp.get_json()
