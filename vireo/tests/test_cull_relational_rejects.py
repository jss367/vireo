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
