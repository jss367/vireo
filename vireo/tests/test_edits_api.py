import os


def test_set_color_label(app_and_db):
    """POST /api/photos/<id>/color_label sets the color label."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pid = photos[0]['id']

    resp = client.post(f'/api/photos/{pid}/color_label', json={'color': 'red'})
    assert resp.status_code == 200
    assert db.get_color_label(pid) == 'red'


def test_remove_color_label(app_and_db):
    """POST /api/photos/<id>/color_label with null removes the label."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pid = photos[0]['id']

    client.post(f'/api/photos/{pid}/color_label', json={'color': 'blue'})
    resp = client.post(f'/api/photos/{pid}/color_label', json={'color': None})
    assert resp.status_code == 200
    assert db.get_color_label(pid) is None


def test_set_color_label_invalid(app_and_db):
    """POST /api/photos/<id>/color_label rejects invalid colors."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pid = photos[0]['id']

    resp = client.post(f'/api/photos/{pid}/color_label', json={'color': 'orange'})
    assert resp.status_code == 400


def test_batch_color_label(app_and_db):
    """POST /api/batch/color_label sets labels on multiple photos."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pids = [p['id'] for p in photos[:2]]

    resp = client.post('/api/batch/color_label', json={'photo_ids': pids, 'color': 'green'})
    assert resp.status_code == 200
    assert db.get_color_label(pids[0]) == 'green'
    assert db.get_color_label(pids[1]) == 'green'


def test_get_color_labels(app_and_db):
    """GET /api/photos/color_labels returns labels keyed by photo id."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pids = [photo['id'] for photo in photos[:2]]
    db.set_color_label(pids[0], 'purple')

    resp = client.get(
        f'/api/photos/color_labels?ids={pids[0]},{pids[1]},not-an-id'
    )

    assert resp.status_code == 200
    assert resp.get_json() == {str(pids[0]): 'purple'}


def test_color_label_description_round_trip(app_and_db):
    """The description API sets, reads, normalizes, and clears meanings."""
    app, db = app_and_db
    client = app.test_client()

    response = client.put(
        "/api/color-label-descriptions/red",
        json={"description": "  Used for\nreptiles  "},
    )
    assert response.status_code == 200
    assert response.get_json() == {
        "color": "red",
        "description": "Used for reptiles",
    }
    assert client.get("/api/color-label-descriptions").get_json() == {
        "red": "Used for reptiles",
    }
    assert db.get_color_label_descriptions() == {"red": "Used for reptiles"}

    response = client.put(
        "/api/color-label-descriptions/red", json={"description": ""}
    )
    assert response.status_code == 200
    assert response.get_json() == {"color": "red", "description": ""}
    assert client.get("/api/color-label-descriptions").get_json() == {}


def test_color_label_description_validation(app_and_db):
    """The description endpoint rejects bad colors, shapes, and long text."""
    app, _ = app_and_db
    client = app.test_client()

    assert client.put(
        "/api/color-label-descriptions/orange", json={"description": "Mammals"}
    ).status_code == 400
    assert client.put(
        "/api/color-label-descriptions/red", json={}
    ).status_code == 400
    for body in (42, "description", ["description"]):
        response = client.put(
            "/api/color-label-descriptions/red", json=body
        )
        assert response.status_code == 400
        assert response.get_json()["error"] == "description required"
    assert client.put(
        "/api/color-label-descriptions/red", json={"description": None}
    ).status_code == 400
    assert client.put(
        "/api/color-label-descriptions/red", json={"description": "x" * 121}
    ).status_code == 400


def test_color_label_routes_are_owned_by_domain_blueprint(app_and_db):
    """The extracted route group must not drift back into the app module."""
    app, _ = app_and_db
    endpoints = {
        rule.rule: rule.endpoint
        for rule in app.url_map.iter_rules()
        if rule.endpoint.startswith('photo_labels.')
    }

    assert endpoints == {
        '/api/batch/color_label': 'photo_labels.set_labels',
        '/api/color-label-descriptions': 'photo_labels.get_descriptions',
        '/api/color-label-descriptions/<color>': 'photo_labels.set_description',
        '/api/photos/<int:photo_id>/color_label': 'photo_labels.set_label',
        '/api/photos/color_labels': 'photo_labels.get_labels',
    }


def test_photo_review_routes_are_owned_by_domain_blueprint(app_and_db):
    """Rating and flag routes stay outside the legacy app module."""
    app, _ = app_and_db
    review_routes = {
        "/api/photos/<int:photo_id>/rating",
        "/api/photos/<int:photo_id>/flag",
        "/api/batch/rating",
        "/api/batch/flag",
    }
    endpoints = {
        rule.rule: rule.endpoint
        for rule in app.url_map.iter_rules()
        if rule.rule in review_routes
    }

    assert endpoints == {
        "/api/photos/<int:photo_id>/rating": "photo_review.set_rating",
        "/api/photos/<int:photo_id>/flag": "photo_review.set_flag",
        "/api/batch/rating": "photo_review.set_ratings",
        "/api/batch/flag": "photo_review.set_flags",
    }


def test_photo_review_routes_preserve_workspace_isolation(app_and_db):
    """Individual and batch review edits reject hidden photos atomically."""
    app, db = app_and_db
    visible_id = db.get_photos()[0]["id"]
    active_workspace_id = db._active_workspace_id
    other_workspace_id = db.create_workspace("Other review workspace")
    db.set_active_workspace(other_workspace_id)
    folder_id = db.add_folder("/photos/other-review", name="other-review")
    hidden_id = db.add_photo(
        folder_id=folder_id,
        filename="hidden-review.jpg",
        extension=".jpg",
        file_size=10,
        file_mtime=1.0,
    )
    db.set_active_workspace(active_workspace_id)
    client = app.test_client()

    for field, value in (("rating", 4), ("flag", "flagged")):
        individual = client.post(
            f"/api/photos/{hidden_id}/{field}", json={field: value}
        )
        assert individual.status_code == 403

        batch = client.post(
            f"/api/batch/{field}",
            json={"photo_ids": [visible_id, hidden_id], field: value},
        )
        assert batch.status_code == 403

    assert db.get_photo(visible_id)["rating"] == 3
    assert db.get_photo(visible_id)["flag"] == "none"
    assert db.get_photo(hidden_id)["rating"] == 0
    assert db.get_photo(hidden_id)["flag"] == "none"


def test_photo_review_batches_skip_stale_ids_and_keep_requested_history_count(
    app_and_db,
):
    """Batch review edits retain their stale-ID and audit-description contract."""
    app, db = app_and_db
    photo_id = db.get_photos()[0]["id"]
    client = app.test_client()

    response = client.post(
        "/api/batch/rating",
        json={"photo_ids": [photo_id, 999999], "rating": 2},
    )

    assert response.status_code == 200
    assert response.get_json() == {"ok": True, "updated": 1}
    assert db.get_photo(photo_id)["rating"] == 2
    history = db.get_edit_history()
    assert history[0]["description"] == "Set rating to 2 on 2 photos"


def test_set_rating(app_and_db):
    """POST /api/photos/<id>/rating updates rating and queues pending change."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pid = photos[0]['id']

    resp = client.post(f'/api/photos/{pid}/rating',
                       json={'rating': 5})
    assert resp.status_code == 200

    photo = db.get_photo(pid)
    assert photo['rating'] == 5

    changes = db.get_pending_changes()
    assert any(c['photo_id'] == pid and c['change_type'] == 'rating' for c in changes)


def test_undo_noop_rating_edit_preserves_earlier_pending_change(app_and_db):
    """Undoing a repeated same-value rating edit should not clear the earlier pending sync."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pid = photos[0]['id']

    resp = client.post(f'/api/photos/{pid}/rating', json={'rating': 4})
    assert resp.status_code == 200

    resp = client.post(f'/api/photos/{pid}/rating', json={'rating': 4})
    assert resp.status_code == 200

    resp = client.post('/api/undo')
    assert resp.status_code == 200

    photo = db.get_photo(pid)
    assert photo['rating'] == 4

    changes = db.get_pending_changes()
    rating_changes = [c for c in changes if c['photo_id'] == pid and c['change_type'] == 'rating']
    assert len(rating_changes) == 1
    assert rating_changes[0]['value'] == '4'


def test_undo_old_rating_action_does_not_clear_new_pending_change_reusing_id(app_and_db):
    """Undo must not delete unrelated pending work even if an old row id is reused."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pid = photos[0]['id']

    resp = client.post(f'/api/photos/{pid}/rating', json={'rating': 4})
    assert resp.status_code == 200

    old_change = next(
        c for c in db.get_pending_changes()
        if c['photo_id'] == pid and c['change_type'] == 'rating' and c['value'] == '4'
    )
    db.clear_pending([old_change['id']])

    db.conn.execute(
        """INSERT INTO pending_changes (id, photo_id, change_type, value, change_token, workspace_id)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (old_change['id'], pid, 'keyword_add', 'Woodpecker', 'replacement-token', db._ws_id()),
    )
    db.conn.commit()

    resp = client.post('/api/undo')
    assert resp.status_code == 200

    changes = db.get_pending_changes()
    assert any(
        c['id'] == old_change['id']
        and c['change_type'] == 'keyword_add'
        and c['value'] == 'Woodpecker'
        for c in changes
    )


def test_set_flag(app_and_db):
    """POST /api/photos/<id>/flag updates the flag and queues XMP sync by default."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pid = photos[0]['id']

    resp = client.post(f'/api/photos/{pid}/flag',
                       json={'flag': 'flagged'})
    assert resp.status_code == 200

    photo = db.get_photo(pid)
    assert photo['flag'] == 'flagged'

    changes = db.get_pending_changes()
    assert any(
        c['photo_id'] == pid
        and c['change_type'] == 'flag'
        and c['value'] == 'flagged'
        for c in changes
    )


def test_set_flag_clears_pending_xmp_when_sync_disabled(app_and_db):
    """Changing a flag with flag sync disabled clears stale queued flag writes."""
    import config as cfg

    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pid = photos[0]['id']

    resp = client.post(f'/api/photos/{pid}/flag', json={'flag': 'flagged'})
    assert resp.status_code == 200
    assert any(
        c['photo_id'] == pid
        and c['change_type'] == 'flag'
        and c['value'] == 'flagged'
        for c in db.get_pending_changes()
    )

    config = cfg.load()
    config['sync_flags_to_xmp'] = False
    cfg.save(config)

    resp = client.post(f'/api/photos/{pid}/flag', json={'flag': 'rejected'})
    assert resp.status_code == 200
    assert db.get_photo(pid)['flag'] == 'rejected'
    assert not any(
        c['photo_id'] == pid and c['change_type'] == 'flag'
        for c in db.get_pending_changes()
    )


def test_add_keyword_to_photo(app_and_db):
    """POST /api/photos/<id>/keywords adds keyword and queues pending change."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pid = photos[0]['id']

    resp = client.post(f'/api/photos/{pid}/keywords',
                       json={'name': 'Woodpecker'})
    assert resp.status_code == 200

    keywords = db.get_photo_keywords(pid)
    kw_names = {k['name'] for k in keywords}
    assert 'Woodpecker' in kw_names

    changes = db.get_pending_changes()
    assert any(c['photo_id'] == pid and c['change_type'] == 'keyword_add' for c in changes)


def test_remove_keyword_from_photo(app_and_db):
    """DELETE /api/photos/<id>/keywords/<kid> removes keyword and queues pending change."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pid = photos[0]['id']

    keywords = db.get_photo_keywords(pid)
    kid = keywords[0]['id']

    resp = client.delete(f'/api/photos/{pid}/keywords/{kid}')
    assert resp.status_code == 200

    keywords = db.get_photo_keywords(pid)
    assert len(keywords) == 0

    changes = db.get_pending_changes()
    assert any(c['photo_id'] == pid and c['change_type'] == 'keyword_remove' for c in changes)


def test_undo_keyword_remove_clears_pending_change(app_and_db):
    """Undoing a keyword removal restores the tag and removes the pending delete."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pid = photos[0]['id']

    keywords = db.get_photo_keywords(pid)
    kid = keywords[0]['id']
    kw_name = keywords[0]['name']

    resp = client.delete(f'/api/photos/{pid}/keywords/{kid}')
    assert resp.status_code == 200

    resp = client.post('/api/undo')
    assert resp.status_code == 200

    keywords = db.get_photo_keywords(pid)
    assert {k['name'] for k in keywords} == {kw_name}

    changes = db.get_pending_changes()
    assert not any(
        c['photo_id'] == pid and c['change_type'] == 'keyword_remove' and c['value'] == kw_name
        for c in changes
    )


def test_readding_removed_keyword_cancels_pending_remove(app_and_db):
    """Removing and re-adding the same keyword before sync leaves no pending keyword change."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pid = photos[0]['id']

    keywords = db.get_photo_keywords(pid)
    kid = keywords[0]['id']
    kw_name = keywords[0]['name']

    resp = client.delete(f'/api/photos/{pid}/keywords/{kid}')
    assert resp.status_code == 200

    resp = client.post(f'/api/photos/{pid}/keywords', json={'name': kw_name})
    assert resp.status_code == 200

    changes = db.get_pending_changes()
    assert not any(c['photo_id'] == pid and c['value'] == kw_name for c in changes)


def test_sync_status(app_and_db):
    """GET /api/sync/status returns pending count."""
    app, db = app_and_db
    client = app.test_client()

    resp = client.get('/api/sync/status')
    assert resp.status_code == 200
    data = resp.get_json()
    assert data['pending_count'] == 0
    assert data['pending_photo_count'] == 0
    assert data['change_type_counts'] == {}

    photos = db.get_photos()
    db.queue_change(photos[0]['id'], 'rating', '3')

    resp = client.get('/api/sync/status')
    data = resp.get_json()
    assert data['pending_count'] == 1
    assert data['pending_photo_count'] == 1
    assert data['change_type_counts'] == {'rating': 1}


def test_sync_preview_pages_photos_with_stable_summary(app_and_db):
    """Progressive preview pages retain totals and a stable revision."""
    app, db = app_and_db
    photo_ids = [photo["id"] for photo in db.get_photos()[:2]]
    assert len(photo_ids) == 2
    for index, photo_id in enumerate(photo_ids, start=3):
        db.queue_change(photo_id, "rating", str(index))

    client = app.test_client()
    first = client.get("/api/sync/preview?limit=1&offset=0")

    assert first.status_code == 200
    first_data = first.get_json()
    assert first_data["total_changes"] == 2
    assert first_data["total_photos"] == 2
    assert first_data["change_type_counts"] == {"rating": 2}
    assert len(first_data["photos"]) == 1
    assert first_data["has_more"] is True
    assert first_data["next_offset"] == 1
    assert first_data["revision"]

    second = client.get(
        "/api/sync/preview?limit=1&offset=1&revision="
        + first_data["revision"]
    )
    assert second.status_code == 200
    second_data = second.get_json()
    assert len(second_data["photos"]) == 1
    assert second_data["photos"][0]["photo_id"] != first_data["photos"][0]["photo_id"]
    assert second_data["total_changes"] == 2
    assert second_data["total_photos"] == 2
    assert second_data["has_more"] is False
    assert second_data["next_offset"] is None


def test_sync_preview_rejects_stale_progressive_revision(app_and_db):
    """A changed pending queue cannot be mixed into an older preview."""
    app, db = app_and_db
    photo_ids = [photo["id"] for photo in db.get_photos()[:2]]
    db.queue_change(photo_ids[0], "rating", "3")
    client = app.test_client()
    first = client.get("/api/sync/preview?limit=1&offset=0").get_json()

    db.queue_change(photo_ids[1], "rating", "4")
    stale = client.get(
        "/api/sync/preview?limit=1&offset=1&revision=" + first["revision"]
    )

    assert stale.status_code == 409
    assert stale.get_json()["code"] == "sync_preview_changed"


def test_sync_preview_revalidates_after_final_page_enrichment(
    app_and_db, tmp_path,
):
    """A queue write during slow final-page enrichment returns a conflict."""
    import app as vireo_app

    app, db = app_and_db
    photo_ids = [photo["id"] for photo in db.get_photos()[:2]]
    first_photo = db.get_photo(photo_ids[0])
    db.conn.execute(
        "UPDATE folders SET path = ? WHERE id = ?",
        (str(tmp_path), first_photo["folder_id"]),
    )
    db.conn.commit()
    db.queue_change(photo_ids[0], "rating", "3")
    original_read = vireo_app.read_sync_preview_metadata
    mutated = False

    def mutate_queue_during_read(path):
        nonlocal mutated
        if not mutated:
            mutated = True
            db.queue_change(photo_ids[1], "rating", "4")
        return original_read(path)

    vireo_app.read_sync_preview_metadata = mutate_queue_during_read
    try:
        response = app.test_client().get(
            "/api/sync/preview?limit=25&offset=0"
        )
    finally:
        vireo_app.read_sync_preview_metadata = original_read

    assert response.status_code == 409
    assert response.get_json()["code"] == "sync_preview_changed"


def test_sync_preview_reuses_snapshot_across_page_requests(app_and_db):
    """Subsequent page requests skip the full pending-changes scan.

    Progressive loading otherwise re-fetches and re-hashes every pending
    change on every page request, making preview preparation quadratic in
    queue size (Codex review, PR #1483). Once the snapshot for a revision
    is cached, later pages must not re-query ``pending_changes``.
    """
    app, db = app_and_db
    photo_ids = [photo["id"] for photo in db.get_photos()[:3]]
    assert len(photo_ids) == 3
    for index, photo_id in enumerate(photo_ids, start=1):
        db.queue_change(photo_id, "rating", str(index))

    client = app.test_client()
    first = client.get("/api/sync/preview?limit=2&offset=0").get_json()
    assert first["total_photos"] == 3
    assert first["revision"]
    revision = first["revision"]

    import app as vireo_app

    call_count = {"n": 0}
    original_build = vireo_app._sync_preview_build_snapshot

    def counting_build(*args, **kwargs):
        call_count["n"] += 1
        return original_build(*args, **kwargs)

    vireo_app._sync_preview_build_snapshot = counting_build
    try:
        second = client.get(
            f"/api/sync/preview?limit=2&offset=2&revision={revision}"
        ).get_json()
    finally:
        vireo_app._sync_preview_build_snapshot = original_build

    assert second["revision"] == revision
    assert len(second["photos"]) == 1
    # The cached snapshot serves page 2 — no re-scan of pending_changes.
    assert call_count["n"] == 0


def test_sync_preview_cache_isolated_by_database(app_and_db, tmp_path):
    """Matching workspace revisions from separate catalogs never cross-read.

    Workspace IDs and pending-change IDs restart in every catalog. A cache
    keyed only by ``(workspace_id, revision)`` can therefore return filenames
    and folders from another database in the same process.
    """
    import app as vireo_app
    from db import Database

    _app, first_db = app_and_db
    first_photo = first_db.get_photos()[0]
    first_db.queue_change(first_photo["id"], "rating", "3")
    first_snapshot = vireo_app._sync_preview_get_snapshot(
        first_db, first_db._ws_id(), None,
    )

    second_db = Database(str(tmp_path / "second-catalog.db"))
    try:
        second_ws = second_db.ensure_default_workspace()
        second_db.set_active_workspace(second_ws)
        second_folder = second_db.add_folder(
            str(tmp_path / "second-photos"), name="second-photos",
        )
        second_db.add_workspace_folder(second_ws, second_folder)
        second_photo = second_db.add_photo(
            folder_id=second_folder,
            filename="second.jpg",
            extension=".jpg",
            file_size=10,
            file_mtime=1.0,
        )
        second_db.queue_change(second_photo, "rating", "3")
        second_snapshot = vireo_app._sync_preview_get_snapshot(
            second_db, second_ws, first_snapshot["revision"],
        )
    finally:
        second_db.close()

    assert second_snapshot["all_photos"][0]["filename"] == "second.jpg"
    assert second_snapshot["all_photos"][0]["folder"] == str(
        tmp_path / "second-photos"
    )


def test_sync_preview_cache_evicts_obsolete_workspace_revision(app_and_db):
    """A changed queue replaces, rather than accumulates, its old snapshot."""
    import app as vireo_app

    _app, db = app_and_db
    ws_id = db._ws_id()
    database_key = os.path.abspath(db._db_path)
    photos = db.get_photos()[:2]
    db.queue_change(photos[0]["id"], "rating", "3")
    first_snapshot = vireo_app._sync_preview_get_snapshot(db, ws_id, None)

    db.queue_change(photos[1]["id"], "rating", "4")
    second_snapshot = vireo_app._sync_preview_get_snapshot(db, ws_id, None)

    assert second_snapshot["revision"] != first_snapshot["revision"]
    with vireo_app._SYNC_PREVIEW_SNAPSHOTS_LOCK:
        workspace_keys = [
            key
            for key in vireo_app._SYNC_PREVIEW_SNAPSHOTS
            if key[:2] == (database_key, ws_id)
        ]
    assert workspace_keys == [
        (database_key, ws_id, second_snapshot["revision"])
    ]


def test_sync_preview_detects_top_id_replacement(app_and_db):
    """A delete+insert that reuses the top pending_changes.id must not hit cache.

    ``pending_changes.id`` is a plain INTEGER PRIMARY KEY (no
    AUTOINCREMENT), so SQLite reuses the highest deleted id on the
    next INSERT. A COUNT/MAX/SUM aggregate fingerprint would stay
    identical across such a replacement and the client would receive
    the stale cached snapshot (Codex review, PR #1483). The per-workspace
    ``pending_changes_version`` counter must bump on both the DELETE and
    the INSERT so the second request rebuilds against the new row.
    """
    app, db = app_and_db
    photo_ids = [photo["id"] for photo in db.get_photos()[:2]]
    assert len(photo_ids) == 2
    original_token = db.queue_change(photo_ids[0], "rating", "3")
    assert original_token is not None

    client = app.test_client()
    first = client.get("/api/sync/preview?limit=1&offset=0").get_json()
    assert first["total_photos"] == 1
    first_change = first["photos"][0]["changes"][0]
    original_id = first_change["id"]

    # Replace the sole pending row with a different change on a different
    # photo. Same value and same change_type keep the aggregate identical
    # (count=1, max_id=sum_id, and the reused id preserves both). Only a
    # write-generation counter — not the aggregate — will notice.
    db.conn.execute("DELETE FROM pending_changes WHERE id = ?", (original_id,))
    db.conn.commit()
    reused_token = db.queue_change(photo_ids[1], "rating", "3")
    assert reused_token is not None
    reused_id = db.conn.execute(
        "SELECT id FROM pending_changes WHERE change_token = ?",
        (reused_token,),
    ).fetchone()[0]
    # Precondition for the regression: SQLite reused the deleted top id.
    # The whole point of this test is to exercise that reuse, so bail out
    # loudly if the platform's SQLite ever changes and we're no longer
    # testing what we think we are.
    assert reused_id == original_id

    second = client.get("/api/sync/preview?limit=1&offset=0").get_json()
    assert second["total_photos"] == 1
    served_change = second["photos"][0]
    assert served_change["photo_id"] == photo_ids[1]
    assert second["revision"] != first["revision"]


def test_sync_preview_describes_location_keyword_as_xmp_delta(
    client_with_photo,
):
    """The internal ``effective`` token never stands in for a location value."""
    import config as cfg
    from xmp import write_gps_location

    app, db, photo_id = client_with_photo
    config = cfg.load()
    config["write_assigned_location_to_xmp"] = True
    cfg.save(config)

    florida_id = db.conn.execute(
        "INSERT INTO keywords (name, type) VALUES ('Florida', 'location')"
    ).lastrowid
    tallahassee_id = db.conn.execute(
        "INSERT INTO keywords "
        "(name, parent_id, type, latitude, longitude) "
        "VALUES ('Tallahassee', ?, 'location', 30.4383, -84.2807)",
        (florida_id,),
    ).lastrowid
    db.conn.commit()
    db.set_photo_location(photo_id, tallahassee_id)
    db.queue_change(photo_id, "location", "effective")

    photo = db.get_photo(photo_id)
    folder = db.conn.execute(
        "SELECT path FROM folders WHERE id = ?", (photo["folder_id"],)
    ).fetchone()["path"]
    write_gps_location(
        os.path.join(folder, "test.xmp"),
        48.8566,
        2.3522,
        source="keyword",
    )

    response = app.test_client().get("/api/sync/preview")

    assert response.status_code == 200
    change = response.get_json()["photos"][0]["changes"][0]
    assert change["presentation"] == {
        "field": "Location",
        "action": "updated",
        "before": "48.85660, 2.35220",
        "after": "Tallahassee, Florida",
        "after_detail": "30.43830, -84.28070 · from a location keyword",
    }


def test_sync_preview_keeps_location_assignment_clear_when_xmp_writes_are_disabled(
    client_with_photo,
):
    """The Vireo assignment stays primary even when XMP GPS is disabled."""
    app, db, photo_id = client_with_photo
    location_id = db.conn.execute(
        "INSERT INTO keywords "
        "(name, type, latitude, longitude) "
        "VALUES ('Tallahassee', 'location', 30.4383, -84.2807)"
    ).lastrowid
    db.conn.commit()
    db.set_photo_location(photo_id, location_id)
    db.queue_change(photo_id, "location", "effective")

    response = app.test_client().get("/api/sync/preview")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["location_sync_enabled"] is False
    change = payload["photos"][0]["changes"][0]
    assert change["presentation"] == {
        "field": "Location",
        "action": "added",
        "before": "No XMP sidecar",
        "after": "Tallahassee",
        "after_detail": (
            "Tallahassee is assigned in Vireo; writing its GPS to XMP is turned off"
        ),
    }


def test_sync_preview_does_not_promise_rating_write_without_sidecar(
    client_with_photo,
):
    """Rating sync cannot create a sidecar, so the preview says it stays in Vireo."""
    app, db, photo_id = client_with_photo
    db.queue_change(photo_id, "rating", "5")

    response = app.test_client().get("/api/sync/preview")

    assert response.status_code == 200
    change = response.get_json()["photos"][0]["changes"][0]
    assert change["presentation"] == {
        "field": "XMP rating",
        "action": "unchanged",
        "before": "No XMP sidecar",
        "after": "No XMP sidecar",
        "after_detail": (
            "5 stars stays in Vireo; rating sync only updates an existing, "
            "readable XMP sidecar"
        ),
    }
    assert change["creates_xmp_sidecar"] is False
    assert change["rating_requires_sidecar"] is True


def test_sync_preview_accounts_for_selected_change_creating_rating_sidecar(
    client_with_photo,
):
    """A selected keyword write creates the sidecar before rating sync runs."""
    app, db, photo_id = client_with_photo
    db.queue_change(photo_id, "rating", "5")
    db.queue_change(photo_id, "keyword_add", "Raptor")

    response = app.test_client().get("/api/sync/preview")

    assert response.status_code == 200
    changes = {
        change["type"]: change
        for change in response.get_json()["photos"][0]["changes"]
    }
    rating = changes["rating"]
    assert changes["keyword_add"]["creates_xmp_sidecar"] is True
    assert rating["rating_requires_sidecar"] is True
    assert rating["presentation"] == rating["presentation_with_sidecar"]
    assert rating["presentation_with_sidecar"] == {
        "field": "Rating",
        "action": "updated",
        "before": "No XMP sidecar",
        "after": "5 stars",
        "after_detail": "Another selected change creates the XMP sidecar first",
    }
    assert rating["presentation_without_sidecar"]["action"] == "unchanged"


def test_sync_preview_reports_rating_persisted_when_location_creates_sidecar(
    client_with_photo,
):
    """Assigned GPS with the toggle on creates the sidecar before rating runs.

    ``sync.py`` writes ``write_gps_location`` before ``write_rating``, so
    the rating actually lands in the new sidecar. The preview must mirror
    that instead of reporting the rating as staying in Vireo.
    """
    import config as cfg

    app, db, photo_id = client_with_photo
    config = cfg.load()
    config["write_assigned_location_to_xmp"] = True
    cfg.save(config)

    location_id = db.conn.execute(
        "INSERT INTO keywords "
        "(name, type, latitude, longitude) "
        "VALUES ('Tallahassee', 'location', 30.4383, -84.2807)"
    ).lastrowid
    db.conn.commit()
    db.set_photo_location(photo_id, location_id)
    db.queue_change(photo_id, "rating", "4")
    db.queue_change(photo_id, "location", "effective")

    response = app.test_client().get("/api/sync/preview")

    assert response.status_code == 200
    changes = {
        change["type"]: change
        for change in response.get_json()["photos"][0]["changes"]
    }
    assert changes["location"]["creates_xmp_sidecar"] is True
    rating = changes["rating"]
    assert rating["presentation"] == rating["presentation_with_sidecar"]
    assert rating["presentation_with_sidecar"]["action"] == "updated"
    assert rating["presentation_with_sidecar"]["after"] == "4 stars"


def test_sync_preview_does_not_persist_rating_when_location_lacks_gps(
    client_with_photo,
):
    """A location keyword without coordinates cannot create the sidecar."""
    import config as cfg

    app, db, photo_id = client_with_photo
    config = cfg.load()
    config["write_assigned_location_to_xmp"] = True
    cfg.save(config)

    # No latitude/longitude on the keyword -- ``sync.py`` will call
    # ``remove_vireo_gps_location`` here, which never creates a sidecar.
    location_id = db.conn.execute(
        "INSERT INTO keywords (name, type) VALUES ('Placeholder', 'location')"
    ).lastrowid
    db.conn.commit()
    db.set_photo_location(photo_id, location_id)
    db.queue_change(photo_id, "rating", "2")
    db.queue_change(photo_id, "location", "effective")

    response = app.test_client().get("/api/sync/preview")

    assert response.status_code == 200
    changes = {
        change["type"]: change
        for change in response.get_json()["photos"][0]["changes"]
    }
    assert changes["location"]["creates_xmp_sidecar"] is False
    assert changes["location"]["presentation"] == {
        "field": "Location",
        "action": "added",
        "before": "No XMP sidecar",
        "after": "Placeholder",
        "after_detail": (
            "Placeholder is assigned in Vireo; it has no GPS coordinates "
            "to write to XMP"
        ),
    }
    assert changes["rating"]["presentation"]["action"] == "unchanged"


def test_sync_preview_reports_rating_persisted_when_edit_recipe_creates_sidecar(
    client_with_photo,
):
    """A non-empty edit recipe creates a sidecar through SidecarEditor."""
    app, db, photo_id = client_with_photo
    db.queue_change(photo_id, "rating", "5")
    db.queue_change(photo_id, "edit_recipe", '{"exposure": 0.5}')

    response = app.test_client().get("/api/sync/preview")

    assert response.status_code == 200
    changes = {
        change["type"]: change
        for change in response.get_json()["photos"][0]["changes"]
    }
    assert changes["edit_recipe"]["creates_xmp_sidecar"] is True
    assert changes["rating"]["presentation"]["action"] == "updated"
    assert changes["rating"]["presentation"]["after"] == "5 stars"


def test_sync_preview_shows_hierarchical_keyword_before_removal(
    client_with_photo,
):
    """A hierarchy-only keyword removal shows the XMP value it will delete."""
    from xmp import write_sidecar

    app, db, photo_id = client_with_photo
    photo = db.get_photo(photo_id)
    folder = db.conn.execute(
        "SELECT path FROM folders WHERE id = ?", (photo["folder_id"],)
    ).fetchone()["path"]
    write_sidecar(
        os.path.join(folder, "test.xmp"),
        flat_keywords=set(),
        hierarchical_keywords={"Animals|Birds|Raptor"},
    )
    db.queue_change(photo_id, "keyword_remove", "Birds")

    response = app.test_client().get("/api/sync/preview")

    assert response.status_code == 200
    change = response.get_json()["photos"][0]["changes"][0]
    assert change["presentation"] == {
        "field": "Keyword",
        "action": "removed",
        "before": "Animals › Birds › Raptor",
        "after": "Not in XMP",
    }


def test_sync_preview_reports_flat_and_hierarchy_when_both_will_be_removed(
    client_with_photo,
):
    """Solo keyword_remove drops flat + hierarchy; both must show in the review."""
    from xmp import write_sidecar

    app, db, photo_id = client_with_photo
    photo = db.get_photo(photo_id)
    folder = db.conn.execute(
        "SELECT path FROM folders WHERE id = ?", (photo["folder_id"],)
    ).fetchone()["path"]
    write_sidecar(
        os.path.join(folder, "test.xmp"),
        flat_keywords={"Raptor"},
        hierarchical_keywords={"Animals|Birds|Raptor"},
    )
    db.queue_change(photo_id, "keyword_remove", "Raptor")

    response = app.test_client().get("/api/sync/preview")

    assert response.status_code == 200
    change = response.get_json()["photos"][0]["changes"][0]
    assert change["paired_keyword_rename"] is False
    assert change["presentation"] == {
        "field": "Keyword",
        "action": "removed",
        "before": "Raptor; Animals › Birds › Raptor",
        "after": "Not in XMP",
    }


def test_sync_preview_preserves_hierarchy_during_paired_keyword_rename(
    client_with_photo,
):
    """A normalized add/remove pair only replaces the flat XMP spelling."""
    from xmp import write_sidecar

    app, db, photo_id = client_with_photo
    photo = db.get_photo(photo_id)
    folder = db.conn.execute(
        "SELECT path FROM folders WHERE id = ?", (photo["folder_id"],)
    ).fetchone()["path"]
    write_sidecar(
        os.path.join(folder, "test.xmp"),
        flat_keywords=set(),
        hierarchical_keywords={"Animals|Birds|Raptor"},
    )
    db.queue_change(photo_id, "keyword_remove", "Birds")
    db.queue_change(photo_id, "keyword_add", "Birds")

    response = app.test_client().get("/api/sync/preview")

    assert response.status_code == 200
    changes = {
        change["type"]: change
        for change in response.get_json()["photos"][0]["changes"]
    }
    removal = changes["keyword_remove"]
    assert removal["paired_keyword_rename"] is True
    assert removal["auto_includes_keyword_add"] is True
    assert removal["creates_xmp_sidecar"] is True
    assert removal["presentation"] == {
        "field": "Keyword hierarchy",
        "action": "unchanged",
        "before": "Animals › Birds › Raptor",
        "after": "Animals › Birds › Raptor",
        "after_detail": (
            "The matching keyword addition replaces only the flat spelling; "
            "this hierarchy stays in XMP"
        ),
    }


def test_sync_preview_clears_rename_flags_after_discarding_paired_keyword_add(
    client_with_photo,
):
    """Discarding one half of an add/remove pair must un-pair the survivor.

    Before the fix, the frontend cached the preview locally and only
    dropped the discarded change from the list, leaving the surviving
    keyword_remove flagged as ``paired_keyword_rename=True`` and
    ``creates_xmp_sidecar=True``. A rating queued alongside that pair
    then displayed as if the sidecar would be created, but the actual
    sync produced no sidecar and silently dropped the rating. Refetching
    the preview after discard must yield up-to-date flags so the rating
    presentation and sync payload agree.
    """
    from xmp import write_sidecar

    app, db, photo_id = client_with_photo
    photo = db.get_photo(photo_id)
    folder = db.conn.execute(
        "SELECT path FROM folders WHERE id = ?", (photo["folder_id"],)
    ).fetchone()["path"]
    write_sidecar(
        os.path.join(folder, "test.xmp"),
        flat_keywords=set(),
        hierarchical_keywords={"Animals|Birds|Raptor"},
    )
    db.queue_change(photo_id, "keyword_remove", "Birds")
    db.queue_change(photo_id, "keyword_add", "Birds")
    db.queue_change(photo_id, "rating", "4")

    client = app.test_client()
    initial = client.get("/api/sync/preview").get_json()
    changes = {c["type"]: c for c in initial["photos"][0]["changes"]}
    assert changes["keyword_remove"]["paired_keyword_rename"] is True
    assert changes["keyword_remove"]["creates_xmp_sidecar"] is True
    add_id = changes["keyword_add"]["id"]

    resp = client.post("/api/sync/discard", json={"change_ids": [add_id]})
    assert resp.status_code == 200

    refreshed = client.get("/api/sync/preview").get_json()
    after = {c["type"]: c for c in refreshed["photos"][0]["changes"]}
    assert "keyword_add" not in after
    removal = after["keyword_remove"]
    assert removal["paired_keyword_rename"] is False
    assert removal["auto_includes_keyword_add"] is False
    assert removal["creates_xmp_sidecar"] is False
    assert removal["presentation"]["action"] == "removed"


def test_sync_preview_treats_flag_as_unchanged_when_sync_is_disabled(
    client_with_photo,
):
    """A stale queued flag does not promise an XMP write after opt-out."""
    import config as cfg
    from xmp import write_pick_flag

    app, db, photo_id = client_with_photo
    photo = db.get_photo(photo_id)
    folder = db.conn.execute(
        "SELECT path FROM folders WHERE id = ?", (photo["folder_id"],)
    ).fetchone()["path"]
    write_pick_flag(os.path.join(folder, "test.xmp"), "rejected")
    db.queue_change(photo_id, "flag", "flagged")
    config = cfg.load()
    config["sync_flags_to_xmp"] = False
    cfg.save(config)

    response = app.test_client().get("/api/sync/preview")

    assert response.status_code == 200
    change = response.get_json()["photos"][0]["changes"][0]
    assert change["creates_xmp_sidecar"] is False
    assert change["presentation"] == {
        "field": "XMP flag",
        "action": "unchanged",
        "before": "Rejected",
        "after": "Rejected",
        "after_detail": "Picked stays in Vireo; flag sync to XMP is turned off",
    }


def test_sync_preview_does_not_promise_removal_from_unreadable_xmp(
    client_with_photo,
):
    """Keyword removal cannot modify a corrupt sidecar."""
    app, db, photo_id = client_with_photo
    photo = db.get_photo(photo_id)
    folder = db.conn.execute(
        "SELECT path FROM folders WHERE id = ?", (photo["folder_id"],)
    ).fetchone()["path"]
    with open(os.path.join(folder, "test.xmp"), "w") as sidecar:
        sidecar.write("not xml")
    db.queue_change(photo_id, "keyword_remove", "Raptor")

    response = app.test_client().get("/api/sync/preview")

    assert response.status_code == 200
    change = response.get_json()["photos"][0]["changes"][0]
    assert change["presentation"] == {
        "field": "XMP keyword",
        "action": "unchanged",
        "before": "Unreadable XMP sidecar",
        "after": "Unreadable XMP sidecar",
        "after_detail": (
            "Raptor cannot be removed because the XMP sidecar is unreadable"
        ),
    }


def test_sync_preview_treats_absent_keyword_removal_as_unchanged(
    client_with_photo,
):
    """A keyword removal against a missing sidecar accurately reports a no-op."""
    app, db, photo_id = client_with_photo
    db.queue_change(photo_id, "keyword_remove", "Raptor")

    response = app.test_client().get("/api/sync/preview")

    assert response.status_code == 200
    change = response.get_json()["photos"][0]["changes"][0]
    assert change["presentation"] == {
        "field": "XMP keyword",
        "action": "unchanged",
        "before": "No XMP sidecar",
        "after": "No XMP sidecar",
        "after_detail": "No XMP sidecar contains Raptor to remove",
    }


def test_sync_preview_reports_paired_flat_rename_as_replacement(
    client_with_photo,
):
    """A paired keyword_add/remove targeting an existing flat variant shows as replaced.

    The sync path dispatches the removal through the flat-only path and
    then ``write_sidecar`` writes the clean spelling, so the sidecar ends
    with the paired add's value. Reporting the remove side as
    ``Not in XMP`` hides that the keyword survives with a canonicalized
    spelling; the review must present it as an unchanged keyword whose
    flat entry is rewritten by the paired addition.
    """
    from xmp import write_sidecar

    app, db, photo_id = client_with_photo
    photo = db.get_photo(photo_id)
    folder = db.conn.execute(
        "SELECT path FROM folders WHERE id = ?", (photo["folder_id"],)
    ).fetchone()["path"]
    # Existing flat variant with a smart quote; both pending values
    # normalize to the same key so ``sync_to_xmp`` pairs them.
    write_sidecar(
        os.path.join(folder, "test.xmp"),
        flat_keywords={"‘apapane"},
        hierarchical_keywords=set(),
    )
    db.queue_change(photo_id, "keyword_remove", "‘apapane")
    db.queue_change(photo_id, "keyword_add", "apapane")

    response = app.test_client().get("/api/sync/preview")

    assert response.status_code == 200
    changes = {
        change["type"]: change
        for change in response.get_json()["photos"][0]["changes"]
    }
    removal = changes["keyword_remove"]
    assert removal["paired_keyword_rename"] is True
    assert removal["auto_includes_keyword_add"] is True
    assert removal["creates_xmp_sidecar"] is True
    assert removal["presentation"] == {
        "field": "Keyword",
        "action": "unchanged",
        "before": "‘apapane",
        "after": "apapane",
        "after_detail": (
            "The matching keyword addition rewrites this flat spelling; "
            "the keyword itself stays in XMP"
        ),
    }


def test_sync_preview_skips_writes_when_folder_is_offline(client_with_photo):
    """A photo whose folder is unmounted must not promise XMP writes.

    ``sync_to_xmp`` guards every write with
    ``os.path.isdir(os.path.dirname(xmp_path))`` and skips the photo with
    ``folder not accessible`` when the check fails (a common NAS-offline
    case). The preview endpoint would otherwise ask
    ``read_sync_preview_metadata`` to inspect the unreachable path,
    which treats it as an absent sidecar and surfaces writes such as
    ``No XMP sidecar → Raptor`` for keyword additions that will never
    actually run.
    """
    import shutil

    app, db, photo_id = client_with_photo
    photo = db.get_photo(photo_id)
    folder = db.conn.execute(
        "SELECT path FROM folders WHERE id = ?", (photo["folder_id"],)
    ).fetchone()["path"]

    db.queue_change(photo_id, "keyword_add", "Raptor")
    db.queue_change(photo_id, "rating", "4")

    # Simulate the NAS going offline between when the photo was cataloged
    # and when the preview is opened.
    shutil.rmtree(folder)

    response = app.test_client().get("/api/sync/preview")

    assert response.status_code == 200
    photos = response.get_json()["photos"]
    assert len(photos) == 1
    assert photos[0]["folder_offline"] is True
    changes = {change["type"]: change for change in photos[0]["changes"]}
    offline_detail = (
        "Sync will skip this photo because its folder is offline; "
        "no XMP will be written"
    )
    assert changes["keyword_add"]["creates_xmp_sidecar"] is False
    assert changes["keyword_add"]["presentation"] == {
        "field": "Keyword",
        "action": "unchanged",
        "before": "Folder not accessible",
        "after": "Folder not accessible",
        "after_detail": offline_detail,
    }
    # Rating normally splits its presentation based on whether another
    # selected change will create the sidecar. An offline folder makes
    # sidecar creation impossible, so both variants collapse to the same
    # "folder not accessible" message and the rating is not asked to
    # split at all.
    assert changes["rating"].get("rating_requires_sidecar") is not True
    assert changes["rating"]["presentation"] == {
        "field": "Rating",
        "action": "unchanged",
        "before": "Folder not accessible",
        "after": "Folder not accessible",
        "after_detail": offline_detail,
    }


def test_sync_preview_does_not_promise_edit_clear_from_unreadable_xmp(
    client_with_photo,
):
    """Clearing a Vireo edit marker cannot modify a corrupt sidecar."""
    app, db, photo_id = client_with_photo
    photo = db.get_photo(photo_id)
    folder = db.conn.execute(
        "SELECT path FROM folders WHERE id = ?", (photo["folder_id"],)
    ).fetchone()["path"]
    with open(os.path.join(folder, "test.xmp"), "w") as sidecar:
        sidecar.write("not xml")
    db.queue_change(photo_id, "edit_recipe", "")

    response = app.test_client().get("/api/sync/preview")

    assert response.status_code == 200
    change = response.get_json()["photos"][0]["changes"][0]
    assert change["presentation"] == {
        "field": "XMP photo edits",
        "action": "unchanged",
        "before": "Unreadable XMP sidecar",
        "after": "Unreadable XMP sidecar",
        "after_detail": (
            "The Vireo edit marker cannot be cleared because the XMP sidecar "
            "is unreadable"
        ),
    }


def test_sync_preview_treats_absent_edit_marker_clear_as_unchanged(
    client_with_photo,
):
    """A clear against a missing sidecar accurately reports a no-op."""
    app, db, photo_id = client_with_photo
    db.queue_change(photo_id, "edit_recipe", "")

    response = app.test_client().get("/api/sync/preview")

    assert response.status_code == 200
    change = response.get_json()["photos"][0]["changes"][0]
    assert change["presentation"] == {
        "field": "XMP photo edits",
        "action": "unchanged",
        "before": "No XMP sidecar",
        "after": "No XMP sidecar",
        "after_detail": "No XMP sidecar contains Vireo edits to clear",
    }


def test_edit_history_recorded_on_rating(app_and_db):
    """Setting a rating records an entry in edit_history."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pid = photos[0]['id']

    client.post(f'/api/photos/{pid}/rating', json={'rating': 5})

    history = db.get_edit_history()
    assert len(history) == 1
    assert history[0]['action_type'] == 'rating'
    assert 'rating' in history[0]['description'].lower()


def test_edit_history_recorded_on_flag(app_and_db):
    """Setting a flag records an entry in edit_history."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pid = photos[0]['id']

    client.post(f'/api/photos/{pid}/flag', json={'flag': 'flagged'})

    history = db.get_edit_history()
    assert len(history) == 1
    assert history[0]['action_type'] == 'flag'


def test_edit_history_recorded_on_keyword_add(app_and_db):
    """Adding a keyword records an entry in edit_history."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pid = photos[0]['id']

    client.post(f'/api/photos/{pid}/keywords', json={'name': 'Eagle'})

    history = db.get_edit_history()
    assert len(history) == 1
    assert history[0]['action_type'] == 'keyword_add'


def test_edit_history_recorded_on_keyword_remove(app_and_db):
    """Removing a keyword records an entry in edit_history."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pid = photos[0]['id']
    keywords = db.get_photo_keywords(pid)
    kid = keywords[0]['id']

    client.delete(f'/api/photos/{pid}/keywords/{kid}')

    history = db.get_edit_history()
    assert len(history) == 1
    assert history[0]['action_type'] == 'keyword_remove'


def test_edit_history_recorded_on_batch_rating(app_and_db):
    """Batch rating records a single grouped entry in edit_history."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pids = [p['id'] for p in photos[:2]]

    client.post('/api/batch/rating', json={'photo_ids': pids, 'rating': 4})

    history = db.get_edit_history()
    assert len(history) == 1
    assert history[0]['is_batch'] == 1
    assert history[0]['item_count'] == 2


def test_undo_api_uses_db(app_and_db):
    """POST /api/undo restores from DB-backed edit history."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pid = photos[0]['id']
    original_rating = photos[0]['rating']

    client.post(f'/api/photos/{pid}/rating', json={'rating': 5})
    assert db.get_photo(pid)['rating'] == 5

    resp = client.post('/api/undo')
    assert resp.status_code == 200
    assert db.get_photo(pid)['rating'] == original_rating
    assert len(db.get_edit_history()) == 0


def test_undo_status_uses_db(app_and_db):
    """GET /api/undo/status reflects DB state."""
    app, db = app_and_db
    client = app.test_client()

    resp = client.get('/api/undo/status')
    assert resp.get_json()['available'] is False

    photos = db.get_photos()
    client.post(f'/api/photos/{photos[0]["id"]}/rating', json={'rating': 5})

    resp = client.get('/api/undo/status')
    data = resp.get_json()
    assert data['available'] is True
    assert data['count'] == 1


def test_edit_history_api(app_and_db):
    """GET /api/edit-history returns paginated history."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pid = photos[0]['id']

    client.post(f'/api/photos/{pid}/rating', json={'rating': 1})
    client.post(f'/api/photos/{pid}/rating', json={'rating': 2})

    resp = client.get('/api/edit-history')
    assert resp.status_code == 200
    data = resp.get_json()
    assert len(data) == 2
    assert data[0]['new_value'] == '2'  # most recent first


# -- History tracking for predictions, culling, labeling, species, discard --


def test_accept_prediction_records_history(app_and_db):
    """Accepting a prediction records prediction_accept in edit history."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pid = photos[0]['id']

    det_ids = db.save_detections(pid, [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.4}, "confidence": 0.9, "category": "animal"}
    ], detector_model="MDV6")
    db.add_prediction(det_ids[0], 'Blue Jay', 0.95, 'test-model')
    preds = db.get_predictions(photo_ids=[pid])
    pred_id = preds[0]['id']

    resp = client.post(f'/api/predictions/{pred_id}/accept')
    assert resp.status_code == 200

    history = db.get_edit_history()
    assert len(history) == 1
    assert history[0]['action_type'] == 'prediction_accept'
    assert 'Blue Jay' in history[0]['description']


def test_accept_prediction_undo_restores_status(app_and_db):
    """Undoing an accepted prediction restores keyword, pending changes, and prediction status."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pid = photos[0]['id']

    det_ids = db.save_detections(pid, [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.4}, "confidence": 0.9, "category": "animal"}
    ], detector_model="MDV6")
    db.add_prediction(det_ids[0], 'Blue Jay', 0.95, 'test-model')
    pred = db.get_predictions(photo_ids=[pid])[0]
    pred_id = pred['id']

    # Accept
    resp = client.post(f'/api/predictions/{pred_id}/accept')
    assert resp.status_code == 200

    # Verify accepted state (review status lives in prediction_review per workspace)
    ws_id = db._active_workspace_id
    assert db.get_review_status(pred_id, ws_id) == 'accepted'
    kws = {k['name'] for k in db.get_photo_keywords(pid)}
    assert 'Blue Jay' in kws

    # Undo
    resp = client.post('/api/undo')
    assert resp.status_code == 200

    # Prediction status restored to pending
    assert db.get_review_status(pred_id, ws_id) == 'pending'

    # Keyword removed
    kws = {k['name'] for k in db.get_photo_keywords(pid)}
    assert 'Blue Jay' not in kws

    # Pending keyword change removed
    changes = db.get_pending_changes()
    assert not any(c['change_type'] == 'keyword_add' and c['value'] == 'Blue Jay' for c in changes)


def test_reject_prediction_records_history(app_and_db):
    """Rejecting a prediction records prediction_reject in edit history."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pid = photos[0]['id']

    det_ids = db.save_detections(pid, [{"box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.4}, "confidence": 0.9, "category": "animal"}], detector_model="MDV6")
    db.add_prediction(det_ids[0], 'House Sparrow', 0.60, 'test-model')
    preds = db.get_predictions(photo_ids=[pid])
    pred_id = preds[0]['id']

    resp = client.post(f'/api/predictions/{pred_id}/reject')
    assert resp.status_code == 200

    history = db.get_edit_history()
    assert len(history) == 1
    assert history[0]['action_type'] == 'prediction_reject'
    assert 'House Sparrow' in history[0]['description']


def test_prediction_group_apply_records_history(app_and_db):
    """Group apply records separate flag and keyword_add history entries."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pids = [p['id'] for p in photos[:3]]

    resp = client.post('/api/predictions/group/apply',
                       json={'picks': [pids[0], pids[1]],
                             'rejects': [pids[2]],
                             'species': 'Northern Cardinal'})
    assert resp.status_code == 200

    history = db.get_edit_history()
    action_types = {h['action_type'] for h in history}
    assert 'keyword_add' in action_types
    assert 'flag' in action_types
    assert len(history) == 2


def test_culling_apply_records_history(app_and_db):
    """Culling apply records flag changes in edit history."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pids = [p['id'] for p in photos[:3]]

    resp = client.post('/api/culling/apply',
                       json={'keepers': [pids[0]], 'rejects': [pids[1], pids[2]]})
    assert resp.status_code == 200

    history = db.get_edit_history()
    assert len(history) == 1
    assert history[0]['action_type'] == 'flag'
    assert history[0]['is_batch'] == 1
    assert history[0]['item_count'] == 3


def test_culling_apply_undo_restores_flags(app_and_db):
    """Undoing culling restores original flag values."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pid = photos[0]['id']
    original_flag = photos[0]['flag'] or 'none'

    client.post('/api/culling/apply', json={'keepers': [pid], 'rejects': []})
    assert db.get_photo(pid)['flag'] == 'flagged'

    resp = client.post('/api/undo')
    assert resp.status_code == 200
    assert (db.get_photo(pid)['flag'] or 'none') == original_flag


def test_culling_apply_unflag_clears_previous_flag(app_and_db):
    """Moving an applied photo back to Review clears its saved flag."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pid = photos[0]['id']

    client.post('/api/culling/apply', json={'keepers': [pid], 'rejects': []})
    assert db.get_photo(pid)['flag'] == 'flagged'

    resp = client.post('/api/culling/apply',
                       json={'keepers': [], 'rejects': [], 'unflag': [pid]})
    assert resp.status_code == 200
    assert resp.get_json()['cleared'] == 1
    assert (db.get_photo(pid)['flag'] or 'none') == 'none'

    history = db.get_edit_history()
    assert history[0]['action_type'] == 'flag'
    assert 'cleared 1' in history[0]['description']


def test_culling_apply_unflag_undo_restores_flag(app_and_db):
    """Undo puts back the flag a Review decision cleared."""
    app, db = app_and_db
    client = app.test_client()
    pid = db.get_photos()[0]['id']

    client.post('/api/culling/apply', json={'keepers': [pid], 'rejects': []})
    client.post('/api/culling/apply',
                json={'keepers': [], 'rejects': [], 'unflag': [pid]})
    assert (db.get_photo(pid)['flag'] or 'none') == 'none'

    resp = client.post('/api/undo')
    assert resp.status_code == 200
    assert db.get_photo(pid)['flag'] == 'flagged'


def test_culling_apply_unflag_ignores_unflagged_photos(app_and_db):
    """Clearing a photo that has no flag is a no-op, not a history entry."""
    app, db = app_and_db
    client = app.test_client()
    pid = db.get_photos()[0]['id']
    assert (db.get_photo(pid)['flag'] or 'none') == 'none'

    resp = client.post('/api/culling/apply',
                       json={'keepers': [], 'rejects': [], 'unflag': [pid]})
    assert resp.status_code == 200
    assert resp.get_json()['cleared'] == 0
    assert db.get_edit_history() == []


def test_culling_apply_rejects_non_list_ids(app_and_db):
    """A malformed body is a 400, not a 500 from list concatenation."""
    app, _db = app_and_db
    client = app.test_client()

    resp = client.post('/api/culling/apply',
                       json={'keepers': 'all', 'rejects': []})
    assert resp.status_code == 400
    assert 'keepers' in resp.get_json()['error']

    resp = client.post('/api/culling/apply',
                       json={'keepers': [], 'rejects': [], 'unflag': 7})
    assert resp.status_code == 400
    assert 'unflag' in resp.get_json()['error']


def test_culling_apply_rejects_overlapping_action_lists(app_and_db):
    """A photo can't be requested for two conflicting flag values at once."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pids = [p['id'] for p in photos[:3]]

    # keepers ∩ rejects
    resp = client.post('/api/culling/apply',
                       json={'keepers': [pids[0]], 'rejects': [pids[0]]})
    assert resp.status_code == 400
    assert 'keepers' in resp.get_json()['error']
    assert 'rejects' in resp.get_json()['error']

    # keepers ∩ unflag
    resp = client.post('/api/culling/apply',
                       json={'keepers': [pids[1]], 'rejects': [],
                             'unflag': [pids[1]]})
    assert resp.status_code == 400
    assert 'unflag' in resp.get_json()['error']

    # rejects ∩ unflag
    resp = client.post('/api/culling/apply',
                       json={'keepers': [], 'rejects': [pids[2]],
                             'unflag': [pids[2]]})
    assert resp.status_code == 400
    assert 'unflag' in resp.get_json()['error']

    # Nothing was mutated for any of the three requests.
    for pid in pids:
        assert (db.get_photo(pid)['flag'] or 'none') == 'none'
    assert db.get_edit_history() == []


def test_encounter_species_records_history(app_and_db):
    """Confirming encounter species records keyword_add in edit history."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pids = [p['id'] for p in photos[:2]]

    resp = client.post('/api/encounters/species',
                       json={'species': 'Red-tailed Hawk', 'photo_ids': pids})
    assert resp.status_code == 200

    history = db.get_edit_history()
    assert len(history) == 1
    assert history[0]['action_type'] == 'keyword_add'
    assert 'Red-tailed Hawk' in history[0]['description']


def test_sync_discard_records_history(app_and_db):
    """Discarding pending changes records discard in edit history."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pid = photos[0]['id']

    db.queue_change(pid, 'rating', '5')
    changes = db.get_pending_changes()
    change_ids = [c['id'] for c in changes]

    resp = client.post('/api/sync/discard', json={'change_ids': change_ids})
    assert resp.status_code == 200

    history = db.get_edit_history()
    assert len(history) == 1
    assert history[0]['action_type'] == 'discard'
    assert db.get_pending_changes() == []


def test_sync_discard_all_rejects_stale_preview_revision(app_and_db):
    """Discard All never removes changes that appeared after the review."""
    app, db = app_and_db
    photos = db.get_photos()[:2]
    db.queue_change(photos[0]["id"], "rating", "3")
    client = app.test_client()
    revision = client.get("/api/sync/preview").get_json()["revision"]

    db.queue_change(photos[1]["id"], "rating", "4")
    response = client.post(
        "/api/sync/discard",
        json={"discard_all": True, "revision": revision},
    )

    assert response.status_code == 409
    assert response.get_json()["code"] == "sync_preview_changed"
    assert len(db.get_pending_changes()) == 2


def test_sync_discard_all_uses_reviewed_revision(app_and_db):
    """A current reviewed revision atomically clears the whole workspace."""
    app, db = app_and_db
    photos = db.get_photos()[:2]
    db.queue_change(photos[0]["id"], "rating", "3")
    db.queue_change(photos[1]["id"], "rating", "4")
    client = app.test_client()
    revision = client.get("/api/sync/preview").get_json()["revision"]

    response = client.post(
        "/api/sync/discard",
        json={"discard_all": True, "revision": revision},
    )

    assert response.status_code == 200
    assert response.get_json()["discarded"] == 2
    assert db.get_pending_changes() == []


def test_undo_skips_non_undoable_entries(app_and_db):
    """Undo skips prediction_reject and discard entries to reach real undoable edits."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pid = photos[0]['id']

    # Create an undoable edit (rating change)
    original_rating = db.get_photo(pid)['rating']
    client.post(f'/api/photos/{pid}/rating', json={'rating': 5})
    assert db.get_photo(pid)['rating'] == 5

    # Create a non-undoable entry (reject prediction)
    det_ids = db.save_detections(pid, [{"box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.4}, "confidence": 0.9, "category": "animal"}], detector_model="MDV6")
    db.add_prediction(det_ids[0], 'House Sparrow', 0.60, 'test-model')
    preds = db.get_predictions(photo_ids=[pid])
    client.post(f'/api/predictions/{preds[-1]["id"]}/reject')

    # History has 2 entries: prediction_reject (most recent) and rating
    history = db.get_edit_history()
    assert len(history) == 2

    # Undo should skip the prediction_reject and undo the rating
    resp = client.post('/api/undo')
    assert resp.status_code == 200
    assert db.get_photo(pid)['rating'] == original_rating

    # prediction_reject entry still in history, rating entry removed
    history = db.get_edit_history()
    assert len(history) == 1
    assert history[0]['action_type'] == 'prediction_reject'


def test_undo_status_skips_non_undoable(app_and_db):
    """Undo status reports the next undoable entry, not a non-undoable one."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pid = photos[0]['id']

    # Create an undoable edit
    client.post(f'/api/photos/{pid}/rating', json={'rating': 5})

    # Create a non-undoable entry on top
    det_ids = db.save_detections(pid, [{"box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.4}, "confidence": 0.9, "category": "animal"}], detector_model="MDV6")
    db.add_prediction(det_ids[0], 'Crow', 0.50, 'test-model')
    preds = db.get_predictions(photo_ids=[pid])
    client.post(f'/api/predictions/{preds[-1]["id"]}/reject')

    # Undo status should show the rating edit, not the reject
    resp = client.get('/api/undo/status')
    data = resp.get_json()
    assert data['available'] is True
    assert 'rating' in data['description'].lower()
    assert data['count'] == 1  # only 1 undoable entry


def test_undo_nothing_when_only_non_undoable(app_and_db):
    """Undo returns error when only non-undoable entries exist."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pid = photos[0]['id']

    # Only non-undoable entries
    det_ids = db.save_detections(pid, [{"box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.4}, "confidence": 0.9, "category": "animal"}], detector_model="MDV6")
    db.add_prediction(det_ids[0], 'Robin', 0.70, 'test-model')
    preds = db.get_predictions(photo_ids=[pid])
    client.post(f'/api/predictions/{preds[0]["id"]}/reject')

    resp = client.post('/api/undo')
    assert resp.status_code == 400  # "nothing to undo"

    resp = client.get('/api/undo/status')
    assert resp.get_json()['available'] is False


# -- Undo coverage for individual action types --


def test_undo_flag_restores_original(app_and_db):
    """Undoing a flag change restores the photo's original flag value."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pid = photos[0]['id']
    original_flag = photos[0]['flag'] or 'none'

    client.post(f'/api/photos/{pid}/flag', json={'flag': 'flagged'})
    assert db.get_photo(pid)['flag'] == 'flagged'

    resp = client.post('/api/undo')
    assert resp.status_code == 200
    assert (db.get_photo(pid)['flag'] or 'none') == original_flag


def test_undo_keyword_add_removes_keyword(app_and_db):
    """Undoing a keyword addition removes the keyword and clears pending change."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pid = photos[0]['id']

    client.post(f'/api/photos/{pid}/keywords', json={'name': 'Heron'})
    kw_names = {k['name'] for k in db.get_photo_keywords(pid)}
    assert 'Heron' in kw_names

    resp = client.post('/api/undo')
    assert resp.status_code == 200

    kw_names = {k['name'] for k in db.get_photo_keywords(pid)}
    assert 'Heron' not in kw_names

    changes = db.get_pending_changes()
    assert not any(c['change_type'] == 'keyword_add' and c['value'] == 'Heron' for c in changes)


# -- Undo coverage for batch operations --


def test_undo_batch_rating_restores_all_photos(app_and_db):
    """Undoing a batch rating restores each photo's original rating."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pids = [p['id'] for p in photos[:3]]
    originals = {p['id']: p['rating'] for p in photos[:3]}

    client.post('/api/batch/rating', json={'photo_ids': pids, 'rating': 1})
    for pid in pids:
        assert db.get_photo(pid)['rating'] == 1

    resp = client.post('/api/undo')
    assert resp.status_code == 200

    for pid in pids:
        assert db.get_photo(pid)['rating'] == originals[pid]


def test_undo_batch_flag_restores_all_photos(app_and_db):
    """Undoing a batch flag restores each photo's original flag."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pids = [p['id'] for p in photos[:3]]
    originals = {p['id']: (p['flag'] or 'none') for p in photos[:3]}

    client.post('/api/batch/flag', json={'photo_ids': pids, 'flag': 'rejected'})
    for pid in pids:
        assert db.get_photo(pid)['flag'] == 'rejected'

    resp = client.post('/api/undo')
    assert resp.status_code == 200

    for pid in pids:
        assert (db.get_photo(pid)['flag'] or 'none') == originals[pid]


def test_redo_batch_flag_restores_per_photo_flag_values(app_and_db):
    """Redoing a batch flag action uses per-item values, not the action summary."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pids = [p['id'] for p in photos[:3]]

    resp = client.post('/api/culling/apply',
                       json={'keepers': [pids[0]], 'rejects': [pids[1], pids[2]]})
    assert resp.status_code == 200

    resp = client.post('/api/undo')
    assert resp.status_code == 200
    resp = client.post('/api/redo')
    assert resp.status_code == 200

    assert db.get_photo(pids[0])['flag'] == 'flagged'
    assert db.get_photo(pids[1])['flag'] == 'rejected'
    assert db.get_photo(pids[2])['flag'] == 'rejected'

    queued = {
        c['photo_id']: c['value']
        for c in db.get_pending_changes()
        if c['change_type'] == 'flag' and c['photo_id'] in pids
    }
    assert queued == {
        pids[0]: 'flagged',
        pids[1]: 'rejected',
        pids[2]: 'rejected',
    }


def test_undo_batch_keyword_add_removes_from_all_photos(app_and_db):
    """Undoing a batch keyword add removes the keyword from every photo."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pids = [p['id'] for p in photos[:3]]

    client.post('/api/batch/keyword', json={'photo_ids': pids, 'name': 'Owl'})
    for pid in pids:
        assert 'Owl' in {k['name'] for k in db.get_photo_keywords(pid)}

    resp = client.post('/api/undo')
    assert resp.status_code == 200

    for pid in pids:
        assert 'Owl' not in {k['name'] for k in db.get_photo_keywords(pid)}

    changes = db.get_pending_changes()
    assert not any(c['change_type'] == 'keyword_add' and c['value'] == 'Owl' for c in changes)


# -- Sequential undo --


def test_multiple_sequential_undos(app_and_db):
    """Multiple undos in sequence each reverse the correct action."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pid = photos[0]['id']
    original_rating = photos[0]['rating']
    original_flag = photos[0]['flag'] or 'none'

    # Action 1: change rating
    client.post(f'/api/photos/{pid}/rating', json={'rating': 2})
    # Action 2: change flag
    client.post(f'/api/photos/{pid}/flag', json={'flag': 'rejected'})
    # Action 3: add keyword
    client.post(f'/api/photos/{pid}/keywords', json={'name': 'Finch'})

    assert len(db.get_edit_history()) == 3

    # Undo 3: keyword add reversed
    resp = client.post('/api/undo')
    assert resp.status_code == 200
    assert 'Finch' not in {k['name'] for k in db.get_photo_keywords(pid)}

    # Undo 2: flag reversed
    resp = client.post('/api/undo')
    assert resp.status_code == 200
    assert (db.get_photo(pid)['flag'] or 'none') == original_flag

    # Undo 1: rating reversed
    resp = client.post('/api/undo')
    assert resp.status_code == 200
    assert db.get_photo(pid)['rating'] == original_rating

    # Nothing left
    resp = client.post('/api/undo')
    assert resp.status_code == 400


# -- Pruning --


def test_history_pruning_respects_max(app_and_db):
    """Old history entries are pruned when exceeding max_edit_history."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pid = photos[0]['id']

    import config as cfg
    cfg.set('max_edit_history', 3)

    # Create 5 edits — only the newest 3 should survive
    for r in range(5):
        client.post(f'/api/photos/{pid}/rating', json={'rating': r})

    history = db.get_edit_history(limit=100)
    assert len(history) == 3
    # Most recent should be the last rating set
    assert history[0]['new_value'] == '4'


# -- Workspace isolation --


def test_history_isolated_between_workspaces(app_and_db):
    """History in one workspace is invisible to another; undo doesn't cross workspaces."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pid = photos[0]['id']

    # Record an edit in the default workspace
    client.post(f'/api/photos/{pid}/rating', json={'rating': 5})
    assert len(db.get_edit_history()) == 1

    # Create and switch to a new workspace
    ws2 = db.create_workspace('Second')
    db.set_active_workspace(ws2)

    # New workspace has no history
    assert len(db.get_edit_history()) == 0

    # Undo in new workspace finds nothing
    result = db.undo_last_edit()
    assert result is None

    # Original workspace still has its history
    ws1 = db.conn.execute("SELECT id FROM workspaces WHERE name = 'Default'").fetchone()['id']
    db.set_active_workspace(ws1)
    assert len(db.get_edit_history()) == 1


def test_set_edit_recipe_removes_regeneration_sidecar(app_and_db, tmp_path):
    """A recipe edit must clear ``<pid>_regen.jpg`` too.

    When the default thumbnail can't be unlinked (Windows lock,
    antivirus, permissions blip), ``serve_thumbnail`` falls back to a
    ``<pid>_regen.jpg`` sidecar. Editing the recipe without also
    invalidating the sidecar leaves it carrying the pre-edit pixels:
    the freshness gate compares against the unchanged source mtime and
    re-serves the sidecar indefinitely. The invalidation must sweep the
    sidecar the same way it sweeps ``<pid>.jpg``.
    """
    app, db = app_and_db
    photos = db.get_photos()
    pid = photos[0]['id']
    thumb_dir = app.config["THUMB_CACHE_DIR"]

    # Simulate the state after serve_thumbnail regenerated to a sidecar
    # because the default was locked.
    sidecar = os.path.join(thumb_dir, f"{pid}_regen.jpg")
    with open(sidecar, "wb") as fh:
        fh.write(b"pre-edit sidecar pixels")
    raw_sidecar = os.path.join(thumb_dir, f"{pid}_raw_regen.jpg")
    with open(raw_sidecar, "wb") as fh:
        fh.write(b"pre-edit paired sidecar")

    client = app.test_client()
    resp = client.post(
        f"/api/photos/{pid}/edit-recipe",
        json={"recipe": {"exposure": 0.5}},
    )

    assert resp.status_code == 200
    assert not os.path.exists(sidecar), (
        f"{pid}_regen.jpg survived the recipe-edit invalidation; if "
        "the default thumbnail is locked, the next request would "
        "keep serving pre-edit pixels through the sidecar path"
    )
    assert not os.path.exists(raw_sidecar), (
        f"{pid}_raw_regen.jpg survived the recipe-edit invalidation; "
        "paired-source sidecars need the same sweep as the default"
    )


def _enable_location_keyword_writes(db):
    """Turn on location keyword writes for the active workspace."""
    import config as cfg

    config = cfg.load()
    config["write_location_keywords_to_xmp"] = True
    cfg.save(config)


def _assign_location(db, photo_id, chain):
    """Link ``photo_id`` to a fresh location keyword chain; returns the leaf id."""
    parent_id = None
    for name in chain:
        parent_id = db.conn.execute(
            "INSERT INTO keywords (name, parent_id, type) VALUES (?, ?, 'location')",
            (name, parent_id),
        ).lastrowid
    db.conn.commit()
    db.set_photo_location(photo_id, parent_id)
    return parent_id


def test_sync_preview_names_the_location_keyword_it_will_write(client_with_photo):
    """The review says which keyword the sidecar is about to gain."""
    app, db, photo_id = client_with_photo
    _enable_location_keyword_writes(db)
    _assign_location(db, photo_id, ["United States", "California", "Kumeyaay Lake"])
    db.queue_change(photo_id, "location", "effective")

    payload = app.test_client().get("/api/sync/preview").get_json()

    assert payload["location_keyword_sync_enabled"] is True
    change = payload["photos"][0]["changes"][0]
    assert change["creates_xmp_sidecar"] is True
    assert change["presentation"]["field"] == "Location"
    assert change["presentation"]["after_detail"].endswith(
        "writes the keyword United States|California|Kumeyaay Lake"
    )


def test_sync_preview_reports_a_location_keyword_already_in_xmp(client_with_photo):
    """A sidecar that already carries the place is not promised a rewrite."""
    import os

    from xmp import SidecarEditor

    app, db, photo_id = client_with_photo
    _enable_location_keyword_writes(db)
    _assign_location(db, photo_id, ["France", "Pont de Gau"])
    db.queue_change(photo_id, "location", "effective")

    photo = db.get_photo(photo_id)
    folder = db.get_folder(photo["folder_id"])["path"]
    editor = SidecarEditor(os.path.join(folder, "test.xmp"))
    editor.set_location_keywords(["France", "Pont de Gau"])
    editor.commit()

    payload = app.test_client().get("/api/sync/preview").get_json()

    change = payload["photos"][0]["changes"][0]
    assert change["presentation"]["after_detail"].endswith(
        "XMP already lists the keyword France|Pont de Gau"
    )
    assert change["creates_xmp_sidecar"] is True


def test_sync_preview_reports_a_normalized_hierarchy_variant_as_already_listed(
    client_with_photo,
):
    """A sidecar spelling that differs only in case matches the writer.

    ``set_location_keywords`` treats a normalized hierarchy variant as
    already present and skips the keyword insert. The preview used to
    do an exact string check and would tell the reviewer the sync is
    about to write the canonical keyword even though the writer would
    make no keyword mutation on that photo.
    """
    import os

    from xmp import LOCATION_KEYWORDS_MARKER, SidecarEditor, write_sidecar

    app, db, photo_id = client_with_photo
    _enable_location_keyword_writes(db)
    _assign_location(db, photo_id, ["France", "Camargue", "Pont de Gau"])
    db.queue_change(photo_id, "location", "effective")

    photo = db.get_photo(photo_id)
    folder = db.get_folder(photo["folder_id"])["path"]
    xmp_path = os.path.join(folder, "test.xmp")
    # Simulate a sidecar where Vireo previously wrote the location but the
    # user (or Lightroom) later rewrote the hierarchy in a different case.
    write_sidecar(
        xmp_path,
        flat_keywords={"Pont de Gau"},
        hierarchical_keywords={"france|camargue|pont de gau"},
    )
    editor = SidecarEditor(xmp_path)
    desc = editor._description()
    desc.set(LOCATION_KEYWORDS_MARKER, "France|Camargue|Pont de Gau")
    editor._dirty = True
    editor.commit()

    payload = app.test_client().get("/api/sync/preview").get_json()

    change = payload["photos"][0]["changes"][0]
    assert change["presentation"]["after_detail"].endswith(
        "XMP already lists the keyword France|Camargue|Pont de Gau"
    )


def test_sync_preview_reports_removing_location_keywords_when_disabled(
    client_with_photo,
):
    """With the setting off, the review says the written keywords come out."""
    import os

    from xmp import SidecarEditor

    app, db, photo_id = client_with_photo
    _assign_location(db, photo_id, ["France", "Pont de Gau"])
    db.queue_change(photo_id, "location", "effective")

    photo = db.get_photo(photo_id)
    folder = db.get_folder(photo["folder_id"])["path"]
    editor = SidecarEditor(os.path.join(folder, "test.xmp"))
    editor.set_location_keywords(["France", "Pont de Gau"])
    editor.commit()

    payload = app.test_client().get("/api/sync/preview").get_json()

    assert payload["location_keyword_sync_enabled"] is False
    change = payload["photos"][0]["changes"][0]
    assert change["presentation"]["after_detail"].endswith(
        "removes the keyword France|Pont de Gau Vireo wrote; "
        "writing location keywords to XMP is turned off"
    )


def test_queue_location_writes_route(client_with_photo):
    """The backfill queues one location change per located photo."""
    app, db, photo_id = client_with_photo
    _enable_location_keyword_writes(db)
    _assign_location(db, photo_id, ["United States", "Kumeyaay Lake"])
    client = app.test_client()

    status = client.get("/api/sync/location-writes").get_json()
    assert status == {
        "photos_with_location": 1,
        "already_queued": 0,
        "location_sync_enabled": False,
        "location_keyword_sync_enabled": True,
    }

    result = client.post("/api/sync/location-writes").get_json()
    assert result == {
        "ok": True, "photos": 1, "queued": 1, "already_queued": 0,
    }
    assert [c["change_type"] for c in db.get_pending_changes()] == ["location"]

    assert client.get("/api/sync/location-writes").get_json()[
        "already_queued"
    ] == 1
    assert client.post("/api/sync/location-writes").get_json()["queued"] == 0


def test_disabling_location_keywords_globally_queues_cleanup(client_with_photo):
    """Flipping the setting off through /api/config queues a cleanup pass.

    Regression: after a successful location-keyword sync, the ``location``
    pending row is gone. Toggling the setting off through the config
    endpoint used to leave those sidecars untouched forever -- the sync
    only walks queued rows, and the config write didn't enqueue any.
    """
    app, db, photo_id = client_with_photo
    _enable_location_keyword_writes(db)
    _assign_location(db, photo_id, ["United States", "Kumeyaay Lake"])
    # Clear anything the assignment queued so the transition is the only
    # source of the location row we assert on.
    _drop_all_pending(db)

    client = app.test_client()
    resp = client.post(
        "/api/config", json={"write_location_keywords_to_xmp": False},
    )
    assert resp.status_code == 200

    assert [c["change_type"] for c in db.get_pending_changes()] == ["location"]


def test_disabling_location_keywords_via_settings_patch_queues_cleanup(
    client_with_photo,
):
    """Same transition through /api/settings/global (per-key PATCH)."""
    app, db, photo_id = client_with_photo
    _enable_location_keyword_writes(db)
    _assign_location(db, photo_id, ["United States", "Kumeyaay Lake"])
    _drop_all_pending(db)

    client = app.test_client()
    resp = client.patch(
        "/api/settings/global",
        json={"key": "write_location_keywords_to_xmp", "value": False},
    )
    assert resp.status_code == 200

    assert [c["change_type"] for c in db.get_pending_changes()] == ["location"]


def test_disabling_location_keywords_via_workspace_override_queues_cleanup(
    client_with_photo,
):
    """A workspace override that flips off queues cleanup for just that workspace."""
    app, db, photo_id = client_with_photo
    _enable_location_keyword_writes(db)  # global on
    _assign_location(db, photo_id, ["United States", "Kumeyaay Lake"])
    _drop_all_pending(db)

    client = app.test_client()
    resp = client.patch(
        "/api/settings/workspace",
        json={"key": "write_location_keywords_to_xmp", "value": False},
    )
    assert resp.status_code == 200

    assert [c["change_type"] for c in db.get_pending_changes()] == ["location"]


def test_enabling_location_keywords_does_not_queue_cleanup(client_with_photo):
    """The False → True direction is a no-op for the cleanup helper.

    Guards against a helper that would queue on any change of the setting
    -- turning writes on is what the backfill button is for, not the
    setting flip.
    """
    app, db, photo_id = client_with_photo
    _assign_location(db, photo_id, ["United States", "Kumeyaay Lake"])
    _drop_all_pending(db)

    client = app.test_client()
    resp = client.post(
        "/api/config", json={"write_location_keywords_to_xmp": True},
    )
    assert resp.status_code == 200
    assert db.get_pending_changes() == []


def _drop_all_pending(db):
    """Remove every pending row so a later assertion sees only new inserts."""
    db.conn.execute("DELETE FROM pending_changes")
    db.conn.commit()


def test_renaming_a_location_leaf_queues_a_location_change(client_with_photo):
    """Renaming a leaf location keyword requeues its tagged photos.

    Regression: ``api_update_keyword`` used to queue only ``keyword_remove``
    and ``keyword_add`` on a rename, so the sidecar's flat ``dc:subject``
    was rewritten but its ``lr:hierarchicalSubject`` and
    ``vireo:locationKeywords`` marker kept pointing at the old leaf. The
    hierarchy in Lightroom stayed stale until another edit re-queued the
    location.
    """
    app, db, photo_id = client_with_photo
    _enable_location_keyword_writes(db)
    leaf_id = _assign_location(db, photo_id, ["France", "OldParis"])
    _drop_all_pending(db)

    client = app.test_client()
    resp = client.put(
        f"/api/keywords/{leaf_id}", json={"name": "NewParis"},
    )
    assert resp.status_code == 200

    queued = [
        (c["photo_id"], c["change_type"]) for c in db.get_pending_changes()
    ]
    assert (photo_id, "location") in queued


def test_renaming_a_location_ancestor_queues_descendant_photos(
    client_with_photo,
):
    """A rename of an ancestor requeues photos tagged with descendant leaves.

    No photo is tagged with the ancestor directly, so the existing
    ``keyword_remove``/``keyword_add`` snapshot iterates an empty list --
    the hierarchy in the sidecar keeps the old ancestor name forever
    without an explicit ``location`` change queued for the descendant leaf.
    """
    app, db, photo_id = client_with_photo
    _enable_location_keyword_writes(db)
    # Build France|Paris and remember France's id for the rename.
    france_id = db.conn.execute(
        "INSERT INTO keywords (name, parent_id, type) VALUES ('France', NULL, 'location')"
    ).lastrowid
    paris_id = db.conn.execute(
        "INSERT INTO keywords (name, parent_id, type) VALUES ('Paris', ?, 'location')",
        (france_id,),
    ).lastrowid
    db.conn.commit()
    db.set_photo_location(photo_id, paris_id)
    _drop_all_pending(db)

    client = app.test_client()
    resp = client.put(
        f"/api/keywords/{france_id}",
        json={"name": "République Française"},
    )
    assert resp.status_code == 200

    queued = [
        (c["photo_id"], c["change_type"]) for c in db.get_pending_changes()
    ]
    assert (photo_id, "location") in queued


def test_renaming_a_non_location_keyword_does_not_queue_location(
    client_with_photo,
):
    """A rename of an ordinary keyword must not touch the location queue.

    Guards against a helper that would queue on any keyword rename -- the
    sidecar location marker and hierarchy are unaffected when the renamed
    keyword is not a ``type='location'`` row (or an ancestor of one).
    """
    app, db, photo_id = client_with_photo
    _enable_location_keyword_writes(db)
    kw_id = db.add_keyword("SomeSpecies", is_species=True)
    db.tag_photo(photo_id, kw_id)
    _drop_all_pending(db)

    client = app.test_client()
    resp = client.put(
        f"/api/keywords/{kw_id}", json={"name": "OtherSpecies"},
    )
    assert resp.status_code == 200

    change_types = {c["change_type"] for c in db.get_pending_changes()}
    assert "location" not in change_types


def test_sync_preview_says_marker_only_when_the_keyword_is_already_gone(
    client_with_photo,
):
    """A keyword deleted in Lightroom is not promised a second removal."""
    import os

    from xmp import SidecarEditor, remove_keywords

    app, db, photo_id = client_with_photo
    _assign_location(db, photo_id, ["France", "Pont de Gau"])
    db.queue_change(photo_id, "location", "effective")

    photo = db.get_photo(photo_id)
    folder = db.get_folder(photo["folder_id"])["path"]
    xmp_path = os.path.join(folder, "test.xmp")
    editor = SidecarEditor(xmp_path)
    editor.set_location_keywords(["France", "Pont de Gau"])
    editor.commit()
    # Someone removed the keyword in Lightroom; Vireo's marker survives.
    remove_keywords(xmp_path, {"Pont de Gau"})

    payload = app.test_client().get("/api/sync/preview").get_json()

    change = payload["photos"][0]["changes"][0]
    assert change["presentation"]["after_detail"].endswith(
        "clears the location-keyword marker Vireo left in XMP; "
        "writing location keywords to XMP is turned off"
    )


def test_renaming_a_location_leaf_skips_keyword_remove_and_keyword_add(
    client_with_photo,
):
    """A location-to-location rename must not queue keyword_remove/keyword_add.

    Regression: if the API queues an ordinary ``keyword_add`` for the new
    leaf, it lands in ``dc:subject`` BEFORE ``set_location_keywords()``
    runs during the sync. The writer then sees the leaf as pre-existing
    (``existed_flat=True``) and claims only hierarchical ownership. Later
    clearing the location strips just the hierarchy, leaving the renamed
    flat leaf orphaned in XMP indefinitely. The queued ``location``
    change alone drives both flat and hierarchical writes with full
    ownership, so keyword_remove/keyword_add on the same photo would
    only interfere.
    """
    app, db, photo_id = client_with_photo
    _enable_location_keyword_writes(db)
    leaf_id = _assign_location(db, photo_id, ["France", "OldParis"])
    _drop_all_pending(db)

    client = app.test_client()
    resp = client.put(
        f"/api/keywords/{leaf_id}", json={"name": "NewParis"},
    )
    assert resp.status_code == 200

    change_types = [c["change_type"] for c in db.get_pending_changes()]
    assert change_types == ["location"]


def test_renaming_a_location_leaf_queues_keyword_requeue_when_setting_off(
    client_with_photo,
):
    """With location keyword writes off, a rename still needs keyword_remove/add.

    Regression: the location-to-location rename branch unconditionally
    skipped ``keyword_remove`` + ``keyword_add``, relying on
    ``set_location_keywords()`` at sync time to rewrite the flat leaf.
    When ``write_location_keywords_to_xmp`` is off, that writer never
    runs -- a pre-existing flat XMP keyword under the OLD name (from a
    manual entry or an earlier period when the setting was on) would
    stay behind forever. The fallback queues the ordinary keyword
    changes so XMP still gets the rename.
    """
    app, db, photo_id = client_with_photo
    # Setting stays at its default (off).
    leaf_id = _assign_location(db, photo_id, ["France", "OldParis"])
    _drop_all_pending(db)

    client = app.test_client()
    resp = client.put(
        f"/api/keywords/{leaf_id}", json={"name": "NewParis"},
    )
    assert resp.status_code == 200

    queued = [
        (c["change_type"], c["value"])
        for c in db.get_pending_changes()
    ]
    assert ("keyword_remove", "OldParis") in queued
    assert ("keyword_add", "NewParis") in queued
    assert ("location", "effective") in queued


def test_retyping_a_location_to_general_queues_keyword_add(client_with_photo):
    """A same-name location→general retype must queue keyword_add.

    Regression: a location→general retype without a name change queues a
    ``location`` change but the name-change block queues no
    ``keyword_add``. sync_to_xmp() resolves no location path (the
    keyword is no longer typed ``location``) and
    ``remove_vireo_location_keywords()`` strips the marker-owned flat
    leaf. The photo remains tagged with the newly ``general`` keyword
    in the DB, so an ordinary ``keyword_add`` is what keeps the flat
    leaf in ``dc:subject`` after the marker cleanup.
    """
    app, db, photo_id = client_with_photo
    _enable_location_keyword_writes(db)
    leaf_id = _assign_location(db, photo_id, ["France", "Paris"])
    _drop_all_pending(db)

    client = app.test_client()
    resp = client.put(
        f"/api/keywords/{leaf_id}", json={"type": "general"},
    )
    assert resp.status_code == 200

    queued = [
        (c["change_type"], c["value"])
        for c in db.get_pending_changes()
    ]
    assert ("keyword_add", "Paris") in queued
    assert ("location", "effective") in queued


def test_deleting_a_location_ancestor_queues_descendant_photos(
    client_with_photo,
):
    """A delete of a location ancestor queues its descendant-tagged photos.

    Regression: no photo is tagged with the ancestor directly, so the
    existing ``affected`` snapshot iterates an empty list, no
    ``keyword_remove`` reaches the sidecar, and the descendant leaf's
    ``lr:hierarchicalSubject`` entry keeps the deleted ancestor name
    forever. Snapshot the descendant-tagged photos and queue a
    ``location`` change so the next sync rewrites the hierarchy under
    the surviving ancestor chain.
    """
    app, db, photo_id = client_with_photo
    _enable_location_keyword_writes(db)
    france_id = db.conn.execute(
        "INSERT INTO keywords (name, parent_id, type) VALUES ('France', NULL, 'location')"
    ).lastrowid
    paris_id = db.conn.execute(
        "INSERT INTO keywords (name, parent_id, type) VALUES ('Paris', ?, 'location')",
        (france_id,),
    ).lastrowid
    db.conn.commit()
    db.set_photo_location(photo_id, paris_id)
    _drop_all_pending(db)

    client = app.test_client()
    resp = client.delete(f"/api/keywords/{france_id}")
    assert resp.status_code == 200

    queued = [
        (c["photo_id"], c["change_type"]) for c in db.get_pending_changes()
    ]
    assert (photo_id, "location") in queued


def test_removing_a_location_tag_queues_a_location_change(client_with_photo):
    """DELETE /api/photos/<id>/keywords/<kid> requeues a location cleanup.

    Regression: ``api_remove_keyword`` queued only ``keyword_remove`` for a
    ``type='location'`` tag, so the generic remover stripped the flat and
    hierarchical entries but left ``vireo:locationKeywords`` and its
    ownership claim in the sidecar. If the user later recreated the
    keyword in Lightroom and assigned another location in Vireo,
    ``set_location_keywords`` would then delete the user's new entry
    under the stale marker's ownership.
    """
    app, db, photo_id = client_with_photo
    _enable_location_keyword_writes(db)
    leaf_id = _assign_location(db, photo_id, ["France", "Paris"])
    _drop_all_pending(db)

    client = app.test_client()
    resp = client.delete(f"/api/photos/{photo_id}/keywords/{leaf_id}")
    assert resp.status_code == 200

    queued = [
        (c["photo_id"], c["change_type"]) for c in db.get_pending_changes()
    ]
    assert (photo_id, "location") in queued


def test_removing_a_non_location_tag_does_not_queue_a_location_change(
    client_with_photo,
):
    """Guards the remove-tag fix from over-queueing.

    Removing an ordinary keyword tag must not touch the location queue --
    it does not affect ``vireo:locationKeywords`` at all.
    """
    app, db, photo_id = client_with_photo
    _enable_location_keyword_writes(db)
    kw_id = db.add_keyword("SomeSpecies", is_species=True)
    db.tag_photo(photo_id, kw_id)
    _drop_all_pending(db)

    client = app.test_client()
    resp = client.delete(f"/api/photos/{photo_id}/keywords/{kw_id}")
    assert resp.status_code == 200

    change_types = {c["change_type"] for c in db.get_pending_changes()}
    assert "location" not in change_types


def test_batch_removing_a_location_tag_queues_a_location_change(client_with_photo):
    """The batch remove endpoint has the same marker-cleanup responsibility.

    ``POST /api/batch/keyword-remove`` also strips a ``type='location'``
    tag through ``keyword_remove`` and must queue a ``location`` change
    per affected photo, otherwise the sidecar marker outlives the tag.
    """
    app, db, photo_id = client_with_photo
    _enable_location_keyword_writes(db)
    leaf_id = _assign_location(db, photo_id, ["France", "Nice"])
    _drop_all_pending(db)

    client = app.test_client()
    resp = client.post(
        "/api/batch/keyword-remove",
        json={"photo_ids": [photo_id], "keyword_id": leaf_id},
    )
    assert resp.status_code == 200

    queued = [
        (c["photo_id"], c["change_type"]) for c in db.get_pending_changes()
    ]
    assert (photo_id, "location") in queued


def test_disabling_location_keywords_via_full_workspace_put_queues_cleanup(
    client_with_photo,
):
    """A full workspace PUT that swaps overrides must run the transition check.

    Regression: ``PUT /api/workspaces/<id>`` accepts an arbitrary
    ``config_overrides`` object and writes it directly, so a payload that
    changes an effective ``write_location_keywords_to_xmp`` value from
    true to false left the already-synced sidecars untouched forever
    -- the per-key PATCH already queued cleanup, but the bulk PUT did
    not.
    """
    app, db, photo_id = client_with_photo
    _enable_location_keyword_writes(db)  # global on
    _assign_location(db, photo_id, ["United States", "Kumeyaay Lake"])
    _drop_all_pending(db)
    ws_id = db._active_workspace_id

    client = app.test_client()
    resp = client.put(
        f"/api/workspaces/{ws_id}",
        json={"config_overrides": {"write_location_keywords_to_xmp": False}},
    )
    assert resp.status_code == 200

    assert [c["change_type"] for c in db.get_pending_changes()] == ["location"]


def test_sync_review_waits_for_active_workspace_job(app_and_db, monkeypatch):
    app, db = app_and_db
    job = {"id": "sync-test", "type": "sync", "status": "running",
           "workspace_id": db._ws_id(),
           "progress": {"current": 20, "total": 100, "synced": 19, "failed": 1, "checkpoint": 2}}
    monkeypatch.setattr(app._job_runner, "list_jobs", lambda: [job])
    client = app.test_client()
    status = client.get('/api/sync/status').get_json()
    assert status["active_job"]["progress"] == job["progress"]
    response = client.get('/api/sync/preview?limit=25')
    assert response.status_code == 409
    assert response.get_json()["code"] == "sync_in_progress"

    # Another workspace's sync must not hide this workspace's review.
    job["workspace_id"] += 1000
    assert client.get('/api/sync/status').get_json()["active_job"] is None
    assert client.get('/api/sync/preview?limit=25').status_code == 200
    job["workspace_id"] = db._ws_id()
    job["status"] = "completed"
    assert client.get('/api/sync/status').get_json()["active_job"] is None
    assert client.get('/api/sync/preview?limit=25').status_code == 200
