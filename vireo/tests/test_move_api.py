"""Tests for move API endpoints."""

import os


def test_move_page_returns_200(app_and_db):
    """GET /move returns 200."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get("/move")
    assert resp.status_code == 200


def test_move_photos_job_starts(app_and_db, tmp_path):
    """POST /api/jobs/move-photos starts a job."""
    app, db = app_and_db
    dst = str(tmp_path / "move_dst")
    os.makedirs(dst)

    # Get a photo ID from the fixture
    photos = db.conn.execute("SELECT id FROM photos LIMIT 1").fetchall()
    pid = photos[0]["id"]

    client = app.test_client()
    resp = client.post("/api/jobs/move-photos", json={
        "photo_ids": [pid],
        "destination": dst,
    })
    assert resp.status_code == 200
    data = resp.get_json()
    assert "job_id" in data
    assert data["job_id"].startswith("move-photos-")


def test_move_photos_requires_params(app_and_db):
    """POST /api/jobs/move-photos without photo_ids returns error."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.post("/api/jobs/move-photos", json={"destination": "/tmp"})
    assert resp.status_code == 400


def test_move_folder_job_starts(app_and_db, tmp_path):
    """POST /api/jobs/move-folder starts a job."""
    app, db = app_and_db
    dst = str(tmp_path / "move_folder_dst")
    os.makedirs(dst)

    folders = db.get_folder_tree()
    fid = folders[0]["id"]

    client = app.test_client()
    resp = client.post("/api/jobs/move-folder", json={
        "folder_id": fid,
        "destination": dst,
    })
    assert resp.status_code == 200
    data = resp.get_json()
    assert "job_id" in data
    assert data["job_id"].startswith("move-folder-")


def test_move_folder_merge_param_accepted(app_and_db, tmp_path):
    """POST /api/jobs/move-folder accepts merge=true and starts a job."""
    app, db = app_and_db
    dst = str(tmp_path / "merge_dst")
    os.makedirs(dst)

    fid = db.get_folder_tree()[0]["id"]

    client = app.test_client()
    resp = client.post("/api/jobs/move-folder", json={
        "folder_id": fid,
        "destination": dst,
        "merge": True,
    })
    assert resp.status_code == 200
    assert resp.get_json()["job_id"].startswith("move-folder-")


def test_move_folder_preflight_dest_missing(app_and_db, tmp_path):
    """Preflight reports exists=False for a destination that doesn't exist."""
    app, db = app_and_db
    dst = str(tmp_path / "nowhere")  # not created on disk

    folder = db.get_folder_tree()[0]
    client = app.test_client()
    resp = client.post("/api/move-folder/preflight", json={
        "folder_id": folder["id"],
        "destination": dst,
    })
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["exists"] is False
    assert data["file_count"] == 0
    assert data["resolved_dest"].startswith(dst)


def test_move_folder_preflight_dest_exists(app_and_db, tmp_path):
    """Preflight reports exists=True and a file count when the resolved
    destination already exists."""
    app, db = app_and_db
    dst = tmp_path / "dest"
    dst.mkdir()

    folder = db.get_folder_tree()[0]
    folder_name = folder["name"] or os.path.basename(folder["path"].rstrip("/\\"))
    landing = dst / folder_name
    landing.mkdir()
    (landing / "already.jpg").write_bytes(b"x")

    client = app.test_client()
    resp = client.post("/api/move-folder/preflight", json={
        "folder_id": folder["id"],
        "destination": str(dst),
    })
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["exists"] is True
    assert data["file_count"] == 1
    assert data["resolved_dest"] == str(landing)


def test_move_folder_preflight_requires_params(app_and_db):
    """Preflight without folder_id returns 400."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.post("/api/move-folder/preflight", json={"destination": "/tmp"})
    assert resp.status_code == 400


def test_move_folder_rejects_non_object_body(app_and_db):
    """A JSON body that isn't an object (e.g. an array) returns 400 instead of
    crashing on body.get(...)."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.post("/api/jobs/move-folder", json=[1, 2, 3])
    assert resp.status_code == 400
    assert "object" in resp.get_json()["error"].lower()


def test_move_folder_rejects_non_string_destination(app_and_db):
    """A non-string destination returns 400 instead of raising TypeError in
    os.path.isabs."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.post("/api/jobs/move-folder", json={
        "folder_id": 1,
        "destination": 42,
    })
    assert resp.status_code == 400
    assert "string" in resp.get_json()["error"].lower()


def test_move_folder_rejects_non_bool_merge(app_and_db):
    """A non-boolean merge parameter returns 400 — strings like "false" must
    not be silently coerced to True."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.post("/api/jobs/move-folder", json={
        "folder_id": 1,
        "destination": "/tmp/dest",
        "merge": "false",  # truthy non-bool
    })
    assert resp.status_code == 400
    assert "boolean" in resp.get_json()["error"].lower()


def test_move_folder_preflight_rejects_non_object_body(app_and_db):
    """Preflight: same type guard as the job endpoint."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.post("/api/move-folder/preflight", json="not-an-object")
    assert resp.status_code == 400


def test_move_rules_crud(app_and_db):
    """CRUD operations on move rules via API."""
    app, _ = app_and_db
    client = app.test_client()

    # Create
    resp = client.post("/api/move-rules", json={
        "name": "Archive hawks",
        "destination": "/nas/archive",
        "criteria": {"rating_min": 3},
    })
    assert resp.status_code == 200
    rule_id = resp.get_json()["id"]

    # List
    resp = client.get("/api/move-rules")
    assert resp.status_code == 200
    rules = resp.get_json()
    assert len(rules) == 1

    # Update
    resp = client.put(f"/api/move-rules/{rule_id}", json={"name": "Updated"})
    assert resp.status_code == 200

    # Delete
    resp = client.delete(f"/api/move-rules/{rule_id}")
    assert resp.status_code == 200

    # Verify deleted
    resp = client.get("/api/move-rules")
    assert len(resp.get_json()) == 0


def test_move_rule_preview(app_and_db):
    """POST /api/move-rules/preview returns matching photo count."""
    app, db = app_and_db
    client = app.test_client()
    resp = client.post("/api/move-rules/preview", json={
        "criteria": {"rating_min": 3},
    })
    assert resp.status_code == 200
    data = resp.get_json()
    assert "count" in data
    assert "photo_ids" in data
