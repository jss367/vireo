"""A flag write may say who decided and why; the note follows the flag it set.

Covers the ``source``/``reason`` fields of ``POST /api/photos/<id>/flag`` and
``POST /api/batch/flag``, the edit-history row they land in, the photo
detail's ``flag_note``, undo, and the ``/api/v1`` aliases.
"""

import sqlite3

import pytest
import schema
from canonical_schema import SCHEMA_VERSION
from db import Database
from services.photo_review import PhotoReviewService, flag_description, flag_note


def _photo(db, name="bird1.jpg"):
    return next(p for p in db.photos.list_page() if p["filename"] == name)


def _history_row(db, edit_id):
    return db.conn.execute(
        "SELECT action_type, description, new_value, source, reason, undone "
        "FROM edit_history WHERE id = ?", (edit_id,),
    ).fetchone()


# -- description --------------------------------------------------------------


def test_flag_description_reads_naturally_with_and_without_provenance():
    assert flag_description("rejected") == "Set flag to rejected"
    assert flag_description("flagged", count=3) == "Set flag to flagged on 3 photos"
    assert (
        flag_description("rejected", source="claude-fable-5-1", reason="eye soft at 1:1")
        == "Set flag to rejected — claude-fable-5-1: eye soft at 1:1"
    )
    assert flag_description("rejected", source="claude-fable-5-1") == "Set flag to rejected — by claude-fable-5-1"
    assert flag_description("none", reason="reconsidered") == "Set flag to none — reconsidered"
    assert (
        flag_description("rejected", count=2, source="s", reason="r")
        == "Set flag to rejected on 2 photos — s: r"
    )


# -- repository -----------------------------------------------------------------


def test_record_edit_persists_provenance_and_list_recent_carries_it(db):
    fid = db.add_folder("/photos", name="photos")
    a = db.add_photo(fid, "a.jpg", ".jpg", 100, 1.0)
    b = db.add_photo(fid, "b.jpg", ".jpg", 100, 2.0)

    first = db.record_edit(
        "flag", "Set flag to flagged", "flagged",
        [{"photo_id": a, "old_value": "none", "new_value": "flagged"}],
    )
    assert tuple(_history_row(db, first))[3:5] == (None, None)

    second = db.record_edit(
        "flag", "Set flag to rejected on 2 photos — agent: soft", "rejected",
        [
            {"photo_id": a, "old_value": "flagged", "new_value": "rejected"},
            {"photo_id": b, "old_value": "none", "new_value": "rejected"},
        ],
        is_batch=True, source="agent", reason="soft",
    )
    assert tuple(_history_row(db, second))[3:5] == ("agent", "soft")
    recent = db.edit_history.list_recent()
    assert [(e["id"], e["source"], e["reason"]) for e in recent] == [
        (second, "agent", "soft"), (first, None, None),
    ]


def test_flag_note_requires_a_live_edit_matching_the_current_flag_with_provenance(db):
    fid = db.add_folder("/photos", name="photos")
    a = db.add_photo(fid, "a.jpg", ".jpg", 100, 1.0)
    assert flag_note(db, a, "none") is None
    db.record_edit(
        "flag", "Set flag to rejected", "rejected",
        [{"photo_id": a, "old_value": "none", "new_value": "rejected"}],
    )
    # A hand flag: history, but nothing to attribute.
    assert flag_note(db, a, "rejected") is None
    PhotoReviewService(db).set_flag(a, "rejected", source="agent", reason="soft")
    note = flag_note(db, a, "rejected")
    assert note["flag"] == "rejected" and note["source"] == "agent" and note["reason"] == "soft"
    assert note["description"] == "Set flag to rejected — agent: soft"
    assert note["at"]
    # The flag moved on without history (a pipeline apply): the note is stale.
    assert flag_note(db, a, "flagged") is None
    assert flag_note(db, a, None) is None


# -- migration ------------------------------------------------------------------


def test_version_18_catalog_gains_edit_history_provenance_and_keeps_rows(tmp_path):
    db_path = str(tmp_path / "vireo.db")
    schema.ensure_schema(db_path)
    with Database(db_path, initialize_schema=False) as db:
        db.conn.execute("PRAGMA foreign_keys = OFF")
        db.conn.execute("DROP TABLE edit_history_items")
        db.conn.execute("DROP TABLE edit_history_payloads")
        db.conn.execute("DROP TABLE edit_history")
        db.conn.execute("""CREATE TABLE edit_history (
            id INTEGER PRIMARY KEY,
            workspace_id INTEGER REFERENCES workspaces(id) ON DELETE CASCADE,
            action_type TEXT NOT NULL, description TEXT NOT NULL, new_value TEXT,
            is_batch INTEGER DEFAULT 0, undone INTEGER DEFAULT 0,
            created_at TEXT DEFAULT (datetime('now'))
        )""")
        db.conn.execute(
            "INSERT INTO edit_history (workspace_id, action_type, description, new_value) "
            "VALUES (1, 'flag', 'Set flag to flagged', 'flagged')"
        )
        db.conn.execute("PRAGMA user_version = 18")
        db.commit()

    schema.ensure_schema(db_path)

    with sqlite3.connect(db_path) as conn:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION
        columns = {row[1] for row in conn.execute("PRAGMA table_info(edit_history)")}
        assert {"source", "reason"} <= columns
        assert conn.execute(
            "SELECT action_type, description, new_value, source, reason FROM edit_history"
        ).fetchall() == [("flag", "Set flag to flagged", "flagged", None, None)]


def test_fresh_catalog_needs_no_provenance_migration(tmp_path):
    db_path = str(tmp_path / "fresh.db")
    schema.ensure_schema(db_path)
    with Database(db_path, initialize_schema=False) as db:
        before = db.conn.execute("SELECT type, name, sql FROM sqlite_master ORDER BY name").fetchall()
        schema._add_edit_history_provenance(db.conn)
        schema._validate_edit_history_provenance(db.conn)
        assert db.conn.execute("SELECT type, name, sql FROM sqlite_master ORDER BY name").fetchall() == before


# -- routes -----------------------------------------------------------------------


def test_set_flag_with_provenance_records_it_and_detail_shows_the_note(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    pid = _photo(db)["id"]

    resp = client.post(
        f"/api/photos/{pid}/flag",
        json={"flag": "rejected", "source": " claude-fable-5-1 ", "reason": "eye soft at 1:1; focus on branch "},
    )
    assert resp.status_code == 200, resp.get_json()

    latest = db.edit_history.list_recent()[0]
    assert latest["action_type"] == "flag"
    assert latest["new_value"] == "rejected"
    assert latest["source"] == "claude-fable-5-1"
    assert latest["reason"] == "eye soft at 1:1; focus on branch"
    assert latest["description"] == "Set flag to rejected — claude-fable-5-1: eye soft at 1:1; focus on branch"
    assert client.get("/api/undo/status").get_json()["description"] == latest["description"]

    detail = client.get(f"/api/photos/{pid}").get_json()
    assert detail["flag"] == "rejected"
    assert detail["flag_note"]["source"] == "claude-fable-5-1"
    assert detail["flag_note"]["reason"] == "eye soft at 1:1; focus on branch"
    assert detail["flag_note"]["flag"] == "rejected"
    assert detail["flag_note"]["at"]

    # Undo reverses the decision like any hand flag, and the note goes with it.
    assert client.post("/api/undo").status_code == 200
    detail = client.get(f"/api/photos/{pid}").get_json()
    assert detail["flag"] == "none"
    assert detail["flag_note"] is None


def test_hand_flag_has_no_note_and_clears_a_previous_one(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    pid = _photo(db)["id"]
    client.post(f"/api/photos/{pid}/flag", json={"flag": "rejected", "source": "agent", "reason": "soft"})
    assert client.get(f"/api/photos/{pid}").get_json()["flag_note"]["source"] == "agent"

    resp = client.post(f"/api/photos/{pid}/flag", json={"flag": "flagged"})
    assert resp.status_code == 200
    latest = db.edit_history.list_recent()[0]
    assert latest["description"] == "Set flag to flagged"
    assert latest["source"] is None and latest["reason"] is None
    assert client.get(f"/api/photos/{pid}").get_json()["flag_note"] is None


def test_flag_changed_without_history_hides_a_stale_note(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    pid = _photo(db)["id"]
    client.post(f"/api/photos/{pid}/flag", json={"flag": "rejected", "source": "agent", "reason": "soft"})
    # The pipeline's group apply writes flags through the repository, not the service.
    db.photo_review.set_flag(pid, "flagged")
    detail = client.get(f"/api/photos/{pid}").get_json()
    assert detail["flag"] == "flagged"
    assert detail["flag_note"] is None


@pytest.mark.parametrize("body, message", [
    ({"flag": "rejected", "source": 7}, "source must be a string"),
    ({"flag": "rejected", "reason": ["x"]}, "reason must be a string"),
    ({"flag": "rejected", "source": "s" * 61}, "source must be at most 60 characters"),
    ({"flag": "rejected", "reason": "r" * 301}, "reason must be at most 300 characters"),
])
def test_set_flag_rejects_malformed_provenance(app_and_db, body, message):
    app, db = app_and_db
    client = app.test_client()
    pid = _photo(db)["id"]
    resp = client.post(f"/api/photos/{pid}/flag", json=body)
    assert resp.status_code == 400
    assert resp.get_json()["error"] == message
    assert db.photos.get(pid)["flag"] == "none"
    batch = client.post("/api/batch/flag", json={**body, "photo_ids": [pid]})
    assert batch.status_code == 400
    assert batch.get_json()["error"] == message


def test_blank_provenance_counts_as_absent(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    pid = _photo(db)["id"]
    resp = client.post(f"/api/photos/{pid}/flag", json={"flag": "rejected", "source": "  ", "reason": ""})
    assert resp.status_code == 200
    latest = db.edit_history.list_recent()[0]
    assert latest["description"] == "Set flag to rejected"
    assert latest["source"] is None and latest["reason"] is None
    assert client.get(f"/api/photos/{pid}").get_json()["flag_note"] is None


def test_batch_flag_with_provenance_is_one_attributed_undoable_edit(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    ids = [p["id"] for p in db.photos.list_page()][:2]
    resp = client.post(
        "/api/batch/flag",
        json={"photo_ids": ids, "flag": "rejected", "source": "claude-fable-5-1", "reason": "departure frames"},
    )
    assert resp.status_code == 200
    assert resp.get_json()["updated"] == 2
    latest = db.edit_history.list_recent()[0]
    assert latest["is_batch"] == 1 and latest["item_count"] == 2
    assert latest["source"] == "claude-fable-5-1" and latest["reason"] == "departure frames"
    assert latest["description"] == "Set flag to rejected on 2 photos — claude-fable-5-1: departure frames"
    for pid in ids:
        note = client.get(f"/api/photos/{pid}").get_json()["flag_note"]
        assert note["source"] == "claude-fable-5-1" and note["reason"] == "departure frames"
    assert client.post("/api/undo").status_code == 200
    for pid in ids:
        assert db.photos.get(pid)["flag"] == "none"


def test_v1_flag_aliases_take_the_token_and_share_the_views(app_and_db):
    app, db = app_and_db
    views = app.view_functions
    assert views["v1_set_flag"] is views["photo_review.set_flag"]
    assert views["v1_set_flags"] is views["photo_review.set_flags"]
    client = app.test_client()
    ids = [p["id"] for p in db.photos.list_page()][:2]
    headers = {"X-Vireo-Token": app.config["API_TOKEN"]}
    assert client.post(f"/api/v1/photos/{ids[0]}/flag", json={"flag": "rejected"}).status_code == 401
    resp = client.post(
        f"/api/v1/photos/{ids[0]}/flag",
        json={"flag": "rejected", "source": "agent", "reason": "soft"}, headers=headers,
    )
    assert resp.status_code == 200
    assert db.photos.get(ids[0])["flag"] == "rejected"
    resp = client.post(
        "/api/v1/batch/flag",
        json={"photo_ids": ids, "flag": "flagged", "source": "agent"}, headers=headers,
    )
    assert resp.status_code == 200 and resp.get_json()["updated"] == 2
    assert db.edit_history.list_recent()[0]["description"] == "Set flag to flagged on 2 photos — by agent"


@pytest.mark.parametrize("photo_ids", [[True], [False], [1, True], "1", [1.0], [0], [-1], [2**63], [2**64]])
def test_batch_flag_rejects_ids_that_are_not_photo_keys(app_and_db, photo_ids):
    """Booleans, floats, strings and ints outside SQLite's range are 400s, not writes."""
    app, db = app_and_db
    client = app.test_client()
    before = db.edit_history.list_recent()
    resp = client.post("/api/batch/flag", json={"photo_ids": photo_ids, "flag": "rejected"})
    assert resp.status_code == 400
    assert resp.get_json()["error"] == "photo_ids must be a list of positive signed 64-bit integer IDs"
    assert all(p["flag"] == "none" for p in db.photos.list_page())
    assert db.edit_history.list_recent() == before


def test_single_flag_rejects_photo_id_beyond_sqlite_range(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    resp = client.post(f"/api/photos/{2**63}/flag", json={"flag": "rejected"})
    assert resp.status_code == 400
    assert resp.get_json()["error"] == "photo_id must be a positive signed 64-bit integer"


def test_batch_flag_matching_no_photos_leaves_undo_and_redo_alone(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    pid = _photo(db)["id"]
    assert client.post(f"/api/photos/{pid}/flag", json={"flag": "rejected"}).status_code == 200
    assert client.post("/api/undo").status_code == 200
    assert client.get("/api/redo/status").get_json()["available"] is True

    resp = client.post("/api/batch/flag", json={"photo_ids": [999_999], "flag": "rejected"})
    assert resp.status_code == 200 and resp.get_json()["updated"] == 0
    assert client.get("/api/redo/status").get_json()["available"] is True
    assert client.get("/api/undo/status").get_json()["available"] is False
    assert client.post("/api/redo").status_code == 200
    assert db.photos.get(pid)["flag"] == "rejected"


@pytest.mark.parametrize("body", ["1", "true", '"bad"', "[1]"])
def test_v1_flag_aliases_reject_non_object_json_bodies(app_and_db, body):
    """The app-wide hook answers 400 before a view reaches ``body.get``."""
    app, db = app_and_db
    client = app.test_client()
    pid = _photo(db)["id"]
    headers = {"X-Vireo-Token": app.config["API_TOKEN"], "Content-Type": "application/json"}
    for path in (f"/api/v1/photos/{pid}/flag", "/api/v1/batch/flag"):
        resp = client.post(path, data=body, headers=headers)
        assert resp.status_code == 400, (path, resp.get_json())
        assert resp.get_json()["code"] == "json_body_not_object"
    assert db.photos.get(pid)["flag"] == "none"


@pytest.mark.parametrize("path, extra", [
    ("/api/photos/{pid}/flag", {}),
    ("/api/batch/flag", {"photo_ids": None}),
])
def test_flag_writes_require_an_explicit_flag(app_and_db, path, extra):
    """A body without ``flag`` (or an unparsable one) is a 400, never a silent clear to ``none``."""
    app, db = app_and_db
    client = app.test_client()
    pid = _photo(db)["id"]
    db.photo_review.set_flag(pid, "flagged")
    url = path.format(pid=pid)
    body = {k: v for k, v in extra.items()}
    if "photo_ids" in body:
        body["photo_ids"] = [pid]
    before = len(db.edit_history.list_recent())
    headers = {"X-Vireo-Token": app.config["API_TOKEN"]}

    assert client.post(url, json=body).status_code == 400
    assert client.post(url, data="{not json", content_type="application/json").status_code == 400
    assert client.post(url, json={**body, "flag": None}).status_code == 400
    assert client.post("/api/v1" + url[4:], json=body, headers=headers).status_code == 400

    assert db.photos.get(pid)["flag"] == "flagged"
    assert len(db.edit_history.list_recent()) == before
