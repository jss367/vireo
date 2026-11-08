"""A cull reject is recorded in favor of the photo kept instead of it.

Apply stores the keeper and the shared measurement on the reject's flag
attribution, photo detail and the Cull page read it back, one Undo reverses
it, and a reject whose keeper is later deleted or rejected resurfaces as an
orphan instead of staying silently rejected.
"""

from services import catalog_deletion
from services.photo_review import PhotoReviewService, flag_note

COMPARISON = {"basis": "eye", "measurement_name": "Tenengrad", "photo": 610, "keeper": 1840,
              "scope": "encounter"}


def _three(db):
    photos = db.photos.list_page()
    return [p["id"] for p in photos[:3]]


def _apply(client, keeper, rejects, comparison=COMPARISON):
    return client.post("/api/culling/apply", json={
        "keepers": [keeper], "rejects": rejects, "unflag": [],
        "rejections": [{"photo_id": pid, "keeper_photo_id": keeper, "comparison": comparison}
                       for pid in rejects],
    })


def test_apply_records_the_keeper_and_photo_detail_shows_it(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    keeper, loser, _ = _three(db)
    resp = _apply(client, keeper, [loser])
    assert resp.status_code == 200
    assert resp.get_json()["rejections"] == 1

    note = flag_note(db, loser, "rejected")
    assert note["keeper_photo_id"] == keeper
    assert note["keeper_filename"] == db.photos.get(keeper)["filename"]
    assert note["comparison"] == COMPARISON
    assert note["keeper_status"] == "kept"
    assert "kept " + db.photos.get(keeper)["filename"] + " instead" in note["description"]

    detail = client.get(f"/api/photos/{loser}").get_json()
    assert detail["flag_note"]["keeper_photo_id"] == keeper
    assert detail["flag_note"]["keeper_status"] == "kept"
    # The keeper itself carries no note: an Apply keep is the photographer's own choice.
    assert flag_note(db, keeper, "flagged") is None


def test_apply_without_rejections_records_plain_rejects(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    keeper, loser, _ = _three(db)
    assert client.post("/api/culling/apply", json={"keepers": [keeper], "rejects": [loser]}).status_code == 200
    assert flag_note(db, loser, "rejected") is None
    assert client.get("/api/culling/orphans").get_json() == {"orphans": [], "count": 0}


def test_cull_page_state_carries_the_keeper(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    keeper, loser, _ = _three(db)
    _apply(client, keeper, [loser])
    state = client.post("/api/pipeline/group/state", json={"photo_ids": [keeper, loser]}).get_json()
    assert state["photos"][str(loser)]["reject_keeper"] == {
        "photo_id": keeper, "filename": db.photos.get(keeper)["filename"], "comparison": COMPARISON,
    }
    assert state["photos"][str(keeper)]["reject_keeper"] is None
    assert state["photos"][str(loser)]["flag_confirmation_status"] == "confirmed"


def test_undo_and_redo_carry_the_keeper(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    keeper, loser, _ = _three(db)
    _apply(client, keeper, [loser])
    assert client.post("/api/undo").status_code == 200
    assert (db.photos.get(loser)["flag"] or "none") == "none"
    assert flag_note(db, loser, "none") is None
    assert client.post("/api/redo").status_code == 200
    note = flag_note(db, loser, "rejected")
    assert note["keeper_photo_id"] == keeper
    assert note["comparison"] == COMPARISON


def test_a_hand_flag_drops_the_relation(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    keeper, loser, _ = _three(db)
    _apply(client, keeper, [loser])
    PhotoReviewService(db).set_flag(loser, "flagged")
    assert flag_note(db, loser, "flagged") is None
    assert db.photo_review.orphaned_rejections() == []


def test_rejecting_the_keeper_orphans_its_rejects(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    keeper, loser, other = _three(db)
    _apply(client, keeper, [loser, other])
    assert client.get("/api/culling/orphans").get_json()["count"] == 0

    PhotoReviewService(db).set_flag(keeper, "rejected")
    data = client.get("/api/culling/orphans").get_json()
    assert data["count"] == 2
    assert {o["photo_id"] for o in data["orphans"]} == {loser, other}
    orphan = data["orphans"][0]
    assert orphan["orphan_reason"] == "keeper_rejected"
    assert orphan["keeper_photo_id"] == keeper
    assert orphan["keeper_filename"] == db.photos.get(keeper)["filename"]
    assert orphan["comparison"] == COMPARISON
    assert flag_note(db, loser, "rejected")["keeper_status"] == "rejected"

    # Clearing one back to undecided takes it out of the list, as one undoable edit.
    resp = client.post("/api/culling/apply", json={"keepers": [], "rejects": [], "unflag": [loser]})
    assert resp.status_code == 200
    assert client.get("/api/culling/orphans").get_json()["count"] == 1
    assert client.post("/api/undo").status_code == 200
    assert client.get("/api/culling/orphans").get_json()["count"] == 2


def test_deleting_the_keeper_orphans_its_rejects(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    keeper, loser, _ = _three(db)
    _apply(client, keeper, [loser])
    catalog_deletion.delete_photos(db, [keeper])
    data = client.get("/api/culling/orphans").get_json()
    assert data["count"] == 1
    assert data["orphans"][0]["orphan_reason"] == "keeper_deleted"
    assert data["orphans"][0]["keeper_filename"] is None
    assert flag_note(db, loser, "rejected")["keeper_status"] == "deleted"
    assert flag_note(db, loser, "rejected")["keeper_filename"] is None


def test_apply_rejects_bad_rejections(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    keeper, loser, other = _three(db)

    def post(rejections, **extra):
        body = {"keepers": [keeper], "rejects": [loser], "unflag": [], "rejections": rejections}
        body.update(extra)
        return client.post("/api/culling/apply", json=body)

    assert post("nope").status_code == 400
    assert post([1]).status_code == 400
    assert post([{"photo_id": other, "keeper_photo_id": keeper}]).status_code == 400
    assert post([{"photo_id": loser, "keeper_photo_id": loser}]).status_code == 400
    assert post([{"photo_id": loser, "keeper_photo_id": other}], rejects=[loser, other]).status_code == 400
    assert post([{"photo_id": loser, "keeper_photo_id": keeper, "comparison": "eye"}]).status_code == 400
    assert post([{"photo_id": loser, "keeper_photo_id": keeper, "comparison": {"nested": {"a": 1}}}]).status_code == 400
    assert post([{"photo_id": loser, "keeper_photo_id": keeper},
                 {"photo_id": loser, "keeper_photo_id": keeper}]).status_code == 400
    assert post([{"photo_id": loser, "keeper_photo_id": 10**9}]).status_code == 403
    # Nothing above touched the catalog.
    assert (db.photos.get(loser)["flag"] or "none") == "none"
    assert post([{"photo_id": loser, "keeper_photo_id": keeper, "comparison": None}]).status_code == 200
    assert flag_note(db, loser, "rejected")["comparison"] is None


def test_apply_refuses_a_keeper_that_is_currently_rejected(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    keeper, loser, other = _three(db)
    PhotoReviewService(db).set_flag(other, "rejected")
    resp = client.post("/api/culling/apply", json={
        "keepers": [keeper], "rejects": [loser], "unflag": [],
        "rejections": [{"photo_id": loser, "keeper_photo_id": other, "comparison": None}],
    })
    assert resp.status_code == 403
    assert "rejected" in resp.get_json()["error"]
    assert (db.photos.get(loser)["flag"] or "none") == "none"
    # Naming it among this Apply's keepers makes it kept again, so it may justify the reject.
    resp = client.post("/api/culling/apply", json={
        "keepers": [other], "rejects": [loser], "unflag": [],
        "rejections": [{"photo_id": loser, "keeper_photo_id": other, "comparison": None}],
    })
    assert resp.status_code == 200
    assert flag_note(db, loser, "rejected")["keeper_status"] == "kept"


def test_merging_the_keeper_into_a_survivor_retargets_the_reject(app_and_db):
    """The remap every photo-row merge runs (``PhotoRowDeletion.delete``) follows the keeper."""
    from repositories.photo_review import remap_flag_attribution_keepers

    app, db = app_and_db
    client = app.test_client()
    keeper, loser, survivor = _three(db)
    _apply(client, keeper, [loser])
    with db.transaction():
        remap_flag_attribution_keepers(db.conn, {keeper: survivor, loser: None})
    note = flag_note(db, loser, "rejected")
    assert note["keeper_photo_id"] == survivor
    assert note["keeper_status"] == "kept"
    assert client.get("/api/culling/orphans").get_json()["count"] == 0
    # The replay snapshots follow too, so undo and redo never restore the merged id.
    assert client.post("/api/undo").status_code == 200
    assert client.post("/api/redo").status_code == 200
    assert flag_note(db, loser, "rejected")["keeper_photo_id"] == survivor


def test_a_later_apply_keeps_a_saved_rejects_record(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    keeper, loser, other = _three(db)
    _apply(client, keeper, [loser])
    # Apply again with the reject sent as a flag alone (what the page sends
    # for a reject it did not decide this session) while picking another photo.
    resp = client.post("/api/culling/apply", json={"keepers": [keeper, other], "rejects": [loser], "unflag": []})
    assert resp.status_code == 200
    note = flag_note(db, loser, "rejected")
    assert note["keeper_photo_id"] == keeper
    assert note["comparison"] == COMPARISON
    # The saved record survives undo and redo of that later Apply too.
    assert client.post("/api/undo").status_code == 200
    assert flag_note(db, loser, "rejected")["keeper_photo_id"] == keeper
    assert client.post("/api/redo").status_code == 200
    assert flag_note(db, loser, "rejected")["keeper_photo_id"] == keeper


def test_a_reused_photo_id_cannot_impersonate_a_deleted_keeper(app_and_db):
    from services import photo_ingest

    app, db = app_and_db
    client = app.test_client()
    photos = db.photos.list_page()
    keeper = max(p["id"] for p in photos)
    loser = min(p["id"] for p in photos)
    folder_id = db.photos.get(keeper)["folder_id"]
    _apply(client, keeper, [loser])
    catalog_deletion.delete_photos(db, [keeper])
    # photos has no AUTOINCREMENT: the next insert takes the freed id.
    newcomer = photo_ingest.add_photo(
        db, folder_id=folder_id, filename="newcomer.jpg", extension=".jpg", file_size=1, file_mtime=1.0,
    )
    assert newcomer == keeper
    note = flag_note(db, loser, "rejected")
    assert note["keeper_status"] == "deleted"
    assert note["keeper_filename"] is None
    data = client.get("/api/culling/orphans").get_json()
    assert data["count"] == 1
    assert data["orphans"][0]["orphan_reason"] == "keeper_deleted"
    assert data["orphans"][0]["keeper_filename"] is None
    # Undo and redo of the original Apply carry the deletion stamp along.
    assert client.post("/api/undo").status_code == 200
    assert client.post("/api/redo").status_code == 200
    assert flag_note(db, loser, "rejected")["keeper_status"] == "deleted"


def test_a_deliberate_re_reject_without_a_keeper_replaces_the_saved_record(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    keeper, loser, _ = _three(db)
    _apply(client, keeper, [loser])
    resp = client.post("/api/culling/apply", json={
        "keepers": [], "rejects": [loser], "unflag": [],
        "rejections": [{"photo_id": loser, "keeper_photo_id": None, "comparison": None}],
    })
    assert resp.status_code == 200
    assert flag_note(db, loser, "rejected") is None
    assert client.get("/api/culling/orphans").get_json()["count"] == 0
    # A null keeper carries no comparison.
    resp = client.post("/api/culling/apply", json={
        "keepers": [], "rejects": [loser], "unflag": [],
        "rejections": [{"photo_id": loser, "keeper_photo_id": None, "comparison": COMPARISON}],
    })
    assert resp.status_code == 400


def test_merging_the_keeper_into_the_reject_itself_clears_the_relation(app_and_db):
    from repositories.photo_review import remap_flag_attribution_keepers

    app, db = app_and_db
    client = app.test_client()
    keeper, loser, _ = _three(db)
    _apply(client, keeper, [loser])
    with db.transaction():
        remap_flag_attribution_keepers(db.conn, {keeper: loser})
    assert flag_note(db, loser, "rejected") is None
    assert client.get("/api/culling/orphans").get_json() == {"orphans": [], "count": 0}
    assert client.post("/api/undo").status_code == 200
    assert client.post("/api/redo").status_code == 200
    assert flag_note(db, loser, "rejected") is None


def test_a_merge_of_the_id_reusing_newcomer_keeps_the_deleted_keeper_stamp(app_and_db):
    from repositories.photo_review import remap_flag_attribution_keepers
    from services import photo_ingest

    app, db = app_and_db
    client = app.test_client()
    photos = db.photos.list_page()
    keeper = max(p["id"] for p in photos)
    loser = min(p["id"] for p in photos)
    folder_id = db.photos.get(keeper)["folder_id"]
    _apply(client, keeper, [loser])
    catalog_deletion.delete_photos(db, [keeper])
    newcomer = photo_ingest.add_photo(
        db, folder_id=folder_id, filename="newcomer.jpg", extension=".jpg", file_size=1, file_mtime=1.0,
    )
    assert newcomer == keeper
    # The newcomer merging into the reject itself must not touch the stamped relation.
    with db.transaction():
        remap_flag_attribution_keepers(db.conn, {newcomer: loser})
    note = flag_note(db, loser, "rejected")
    assert note["keeper_status"] == "deleted"
    assert note["comparison"] == COMPARISON
    assert client.get("/api/culling/orphans").get_json()["count"] == 1
    assert client.post("/api/undo").status_code == 200
    assert client.post("/api/redo").status_code == 200
    assert flag_note(db, loser, "rejected")["keeper_status"] == "deleted"
