"""Regressions for the bugs found during the SQL-boundary review."""

import contextlib
import sqlite3

import pytest
from db import Database
from repositories.photo_review import PhotoReviewRepository
from repositories.sync import SyncRepository
from web.location_edits import serialize_photo_location


def _keyword(db, name, *, kind="location", parent=None, lat=None, lng=None,
             keyword_id=None, taxon_id=None, source_id=None):
    cur = db.conn.execute(
        """INSERT INTO keywords
           (id, name, type, parent_id, latitude, longitude, is_species, taxon_id, source_taxon_id)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (keyword_id, name, kind, parent, lat, lng, int(kind == "taxonomy"), taxon_id, source_id),
    )
    db.commit()
    return cur.lastrowid


def test_selective_discard_blocks_row_replacement(app_and_db, monkeypatch):
    app, db = app_and_db
    pid = db.get_photos()[0]["id"]
    db.queue_change(pid, "flag", "flagged")
    change = db.pending_changes.list_all()[0]
    original = SyncRepository.get_by_ids
    blocked = []

    def read_with_competing_writer(self, ids):
        rows = original(self, ids)
        with contextlib.closing(sqlite3.connect(db._db_path, timeout=0)) as writer:
            try:
                writer.execute("DELETE FROM pending_changes WHERE id = ?", (change["id"],))
                writer.execute(
                    """INSERT INTO pending_changes (photo_id, change_type, value, change_token, workspace_id)
                       VALUES (?, 'flag', 'rejected', 'replacement', ?)""",
                    (pid, db.active_workspace_id),
                )
                writer.commit()
            except sqlite3.OperationalError as exc:
                assert "locked" in str(exc)
                blocked.append(True)
        return rows

    monkeypatch.setattr(SyncRepository, "get_by_ids", read_with_competing_writer)
    response = app.test_client().post("/api/sync/discard", json={"change_ids": [change["id"]]})
    assert response.status_code == 200
    assert blocked == [True]
    assert response.get_json()["discarded"] == 1
    assert db.pending_changes.list_all() == []
    assert db.get_edit_history()[0]["action_type"] == "discard"


def test_selective_discard_rolls_back_when_history_fails(app_and_db, monkeypatch):
    app, db = app_and_db
    pid = db.get_photos()[0]["id"]
    ws2 = db.create_workspace("Sibling")
    db.queue_change(pid, "keyword_remove_flat", "Wildlife")
    db.queue_change(pid, "keyword_remove_flat", "Wildlife", workspace_id=ws2)
    change_id = db.pending_changes.list_all()[0]["id"]

    def fail(*args, **kwargs):
        raise RuntimeError("history failed")

    monkeypatch.setattr(Database, "record_edit", fail)
    response = app.test_client().post("/api/sync/discard", json={"change_ids": [change_id]})
    assert response.status_code == 500
    assert db.conn.execute("SELECT COUNT(*) FROM pending_changes").fetchone()[0] == 2
    assert db.get_edit_history() == []


def test_selective_discard_counts_repeated_ids_once(app_and_db):
    from db import _SQLITE_PARAM_CHUNK_SIZE

    app, db = app_and_db
    db.queue_change(db.get_photos()[0]["id"], "rating", "4")
    cid = db.pending_changes.list_all()[0]["id"]
    response = app.test_client().post(
        "/api/sync/discard", json={"change_ids": [cid] * (_SQLITE_PARAM_CHUNK_SIZE + 1)},
    )
    assert response.status_code == 200
    assert response.get_json()["discarded"] == 1
    assert db.get_edit_history()[0]["item_count"] == 1


@pytest.mark.parametrize("value", [True, "1", {}, [], 2**63])
@pytest.mark.parametrize("endpoint, field", [("/api/sync/discard", "change_ids"),
                                           ("/api/culling/apply", "keepers")])
def test_sync_and_culling_reject_malformed_ids(app_and_db, endpoint, field, value):
    app, db = app_and_db
    response = app.test_client().post(endpoint, json={field: [value]})
    assert response.status_code == 400
    assert all(row["flag"] == "none" for row in db.get_photos())
    assert db.pending_changes.list_all() == []
    assert db.get_edit_history() == []


def test_culling_repeated_ids_produce_one_history_item(app_and_db):
    app, db = app_and_db
    pid = db.get_photos()[0]["id"]
    response = app.test_client().post("/api/culling/apply", json={"keepers": [pid, pid]})
    assert response.status_code == 200
    assert response.get_json()["keepers"] == 1
    assert db.get_edit_history()[0]["item_count"] == 1


@pytest.mark.parametrize("failure", ["flag", "queue", "history"])
def test_culling_failure_rolls_back_flags_queue_and_history(app_and_db, monkeypatch, failure):
    import config as cfg

    app, db = app_and_db
    cfg.save({"sync_flags_to_xmp": True})
    pids = [row["id"] for row in db.get_photos()[:3]]
    db.photo_review.set_flag(pids[2], "rejected")
    db.queue_flag_change_if_enabled(pids[2], "rejected")
    before_flags = {pid: db.get_photo(pid)["flag"] for pid in pids}
    before_queue = [dict(row) for row in db.pending_changes.list_all()]
    owner, method = {"flag": (PhotoReviewRepository, "set_flag"),
                     "queue": (Database, "queue_flag_change_if_enabled"),
                     "history": (Database, "record_edit")}[failure]
    original = getattr(owner, method)
    calls = []

    def fail(self, *args, **kwargs):
        calls.append(args)
        if failure == "history" or len(calls) == 2:
            raise RuntimeError("injected failure")
        return original(self, *args, **kwargs)

    monkeypatch.setattr(owner, method, fail)
    response = app.test_client().post(
        "/api/culling/apply", json={"keepers": [pids[0]], "rejects": [pids[1]], "unflag": [pids[2]]},
    )
    assert response.status_code == 500
    assert {pid: db.get_photo(pid)["flag"] for pid in pids} == before_flags
    assert [dict(row) for row in db.pending_changes.list_all()] == before_queue
    assert db.get_edit_history() == []


def test_culling_flags_are_not_committed_before_queueing(app_and_db, monkeypatch):
    import config as cfg

    app, db = app_and_db
    cfg.save({"sync_flags_to_xmp": True})
    pid = db.get_photos()[0]["id"]
    before = db.get_photo(pid)["flag"]
    original = Database.queue_flag_change_if_enabled
    observed = []

    def check_visibility(self, *args, **kwargs):
        with contextlib.closing(sqlite3.connect(db._db_path)) as reader:
            observed.append(reader.execute("SELECT flag FROM photos WHERE id = ?", (pid,)).fetchone()[0])
        return original(self, *args, **kwargs)

    monkeypatch.setattr(Database, "queue_flag_change_if_enabled", check_visibility)
    response = app.test_client().post("/api/culling/apply", json={"keepers": [pid]})
    assert response.status_code == 200
    assert observed == [before]
    assert db.get_photo(pid)["flag"] == "flagged"
    assert [(row["change_type"], row["value"]) for row in db.pending_changes.list_all()] == [("flag", "flagged")]
    assert db.get_edit_history()[0]["item_count"] == 1


def test_location_detail_and_sync_preview_choose_exported_location(client_with_photo):
    import config as cfg

    app, db, pid = client_with_photo
    cfg.save({"write_assigned_location_to_xmp": True})
    first = _keyword(db, "Old place", lat=1, lng=2)
    root = _keyword(db, "France")
    chosen = _keyword(db, "Paris", parent=root, lat=48.8, lng=2.3)
    coordless = _keyword(db, "Other place", parent=root)
    for kid in (first, chosen, coordless):
        db.tag_photo(pid, kid)
    assert db.get_photo_location_keyword_ids([pid]) == {pid: chosen}
    assert serialize_photo_location(db, pid)["keyword_id"] == chosen
    db.queue_change(pid, "location", "effective")
    photo = app.test_client().get("/api/sync/preview").get_json()["photos"][0]
    assert photo["changes"][0]["presentation"]["after"] == "Paris, France"
    assert db.get_photo_location_leaf(pid)["id"] == chosen


def test_location_breadcrumb_stops_at_general_parent(app_and_db):
    _, db = app_and_db
    pid = db.get_photos()[0]["id"]
    general = _keyword(db, "Trips", kind="general")
    country = _keyword(db, "France", parent=general)
    city = _keyword(db, "Paris", parent=country)
    db.tag_photo(pid, city)
    assert serialize_photo_location(db, pid)["parent_chain"] == [{"id": country, "name": "France"}]
    assert db.get_photo_location_paths([pid]) == {pid: ["France", "Paris"]}


def test_deleting_unknown_keyword_returns_not_found(app_and_db):
    app, db = app_and_db
    response = app.test_client().delete("/api/keywords/987654")
    assert response.status_code == 404
    assert db.pending_changes.list_all() == []


def test_duplicate_keyword_suggestion_keeps_earliest_variant(app_and_db):
    app, db = app_and_db
    pid = db.get_photos()[0]["id"]
    first = _keyword(db, "Woodland", kind="general", keyword_id=31)
    later = _keyword(db, "woodland", kind="general", keyword_id=32)
    db.tag_photo(pid, later)
    db.tag_photo(pid, first)
    group = app.test_client().get("/api/keywords/duplicates").get_json()[0]
    assert [row["id"] for row in group["variants"]] == [first, later]
    assert group["keep"] == "Woodland"


def test_collection_update_reports_concurrent_deletion(app_and_db, monkeypatch):
    app, db = app_and_db
    cid = db.add_collection("Birds", "[]")
    original = Database.update_collection

    def delete_then_update(self, collection_id, **kwargs):
        db.delete_collection(collection_id)
        return original(self, collection_id, **kwargs)

    monkeypatch.setattr(Database, "update_collection", delete_then_update)
    response = app.test_client().put(f"/api/collections/{cid}", json={"name": "Wildlife"})
    assert response.status_code == 404


@pytest.mark.parametrize("endpoint", ["remove", "delete-sidecars"])
def test_missing_originals_requires_active_workspace(app_and_db, monkeypatch, endpoint):
    app, db = app_and_db
    pid = db.get_photos()[0]["id"]
    monkeypatch.setattr(Database, "active_workspace_id", property(lambda self: None))
    response = app.test_client().post(f"/api/photos/missing/{endpoint}", json={"photo_ids": [pid]})
    assert response.status_code == 400
    assert "workspace" in response.get_json()["error"].lower()
    assert db.get_photo(pid) is not None


def test_prediction_suggestions_count_all_keywords_for_same_identity(app_and_db):
    app, db = app_and_db
    pid = db.get_photos()[0]["id"]
    taxon = db.conn.execute(
        "INSERT INTO taxa (name, common_name, rank, inat_id) VALUES ('Haliaeetus leucocephalus', 'Bald Eagle', 'species', 5305)"
    ).lastrowid
    db.commit()
    _keyword(db, "Bald Eagle", kind="taxonomy", source_id=5305)
    linked = _keyword(db, "Bald Eagle", kind="taxonomy", taxon_id=taxon)
    db.tag_photo(pid, linked)
    did = db.save_detections(pid, [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
                                   "confidence": 0.9, "category": "animal"}], detector_model="MDV6")[0]
    db.add_prediction(did, "Bald Eagle", 0.9, "bioclip", labels_fingerprint="fp1")
    response = app.test_client().post("/api/selection/prediction-suggestions", json={"photo_ids": [pid]})
    assert response.status_code == 200
    eagle = next(row for row in response.get_json()["predictions"] if row["species"] == "Bald Eagle")
    assert eagle["keyworded_count"] == 1
    assert eagle["missing_photo_ids"] == []
