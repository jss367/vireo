"""Tests for move API endpoints."""

import os


def test_move_page_returns_200(app_and_db):
    """GET /move returns 200."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get("/move")
    assert resp.status_code == 200


def test_move_page_folder_browser_exposes_volumes_shortcut(app_and_db):
    """Move destinations should be able to jump directly to mounted volumes.
    Mac uses /Volumes (a real directory). Windows + Linux use /api/volumes
    since mount roots vary per host (drive letters; /media, /run/media,
    /mnt)."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get("/move")
    html = resp.data.decode()

    assert "browseTo('__volumes__')" in html
    # Non-Mac hosts (Windows + Linux) fan through /api/volumes.
    assert "if (navigator.userAgent.indexOf('Mac') < 0)" in html
    assert "await safeFetch('/api/volumes')" in html
    # Mac still goes through /api/browse on /Volumes.
    assert (
        "url = '/api/browse?path=' + encodeURIComponent('/Volumes');"
    ) in html


def test_move_page_browser_stamps_requests(app_and_db):
    """Each browseTo call must stamp a sequence number and drop stale
    responses. Otherwise a slow initial /api/browse for the prefilled
    destination can land after a Pictures/Volumes shortcut click and
    silently revert the list."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get("/move")
    html = resp.data.decode()

    assert "var browseSeq = 0;" in html
    assert "var seq = ++browseSeq;" in html
    assert "if (seq !== browseSeq) return;" in html


def test_move_page_pictures_shortcut_resolves_home_on_demand(app_and_db):
    """The Pictures shortcut must resolve the user's home directory before
    composing the path. Otherwise, opening the modal with a prefilled
    destination (which skips the initial browseTo(null)) sends an absolute
    '/Pictures' to the backend and lands at the wrong root."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get("/move")
    html = resp.data.decode()

    assert "if (path === '__pictures__')" in html
    # The on-demand home fetch must happen inside the Pictures branch.
    assert "if (!browserHomePath)" in html
    assert "await safeFetch('/api/browse')" in html
    assert (
        "url = '/api/browse?path=' + encodeURIComponent("
        "browserHomePath + '/Pictures');"
    ) in html


def test_move_page_windows_parent_preserves_drive_root(app_and_db):
    """When drilling up from 'C:\\Users', the parent must be 'C:\\', not 'C:'.
    Without the trailing backslash the backend treats it as the drive's cwd
    and the '..' link sends users to the wrong place."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get("/move")
    html = resp.data.decode()

    assert "/^[A-Za-z]:$/.test(parent)" in html
    assert "parent += '\\\\'" in html


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


def test_move_folder_failed_move_recorded_as_failed(app_and_db, tmp_path):
    """A move that copies nothing (here: a destination that overlaps the
    source, so move_folder returns {"moved": 0, "errors": [...]} without
    raising) must be recorded in history as 'failed', not 'completed' — and
    error_count must reflect the failure. Regression for move jobs that read
    "completed, 0 errors" despite moving nothing.
    """
    import sys

    sys.path.insert(0, os.path.dirname(__file__))
    from wait import wait_for_job_via_client

    app, db = app_and_db

    # Real on-disk source folder so move_folder's overlap check runs.
    src = tmp_path / "src_folder"
    src.mkdir()
    (src / "a.jpg").write_bytes(b"data")
    fid = db.add_folder(str(src), name="src_folder")

    client = app.test_client()
    # destination == source ⇒ resolved dest nests inside the source ⇒ overlap
    # ⇒ deterministic failure with moved=0 and no needs_merge.
    resp = client.post("/api/jobs/move-folder", json={
        "folder_id": fid,
        "destination": str(src),
    })
    assert resp.status_code == 200
    job_id = resp.get_json()["job_id"]

    job = wait_for_job_via_client(client, job_id, wait_for_history=True)
    assert job["status"] == "failed", job
    # The source must be untouched — a failed move never deletes originals.
    assert (src / "a.jpg").exists()

    row = db.conn.execute(
        "SELECT status, error_count FROM job_history WHERE id = ?", (job_id,)
    ).fetchone()
    assert row["status"] == "failed"
    assert row["error_count"] >= 1


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
    assert data["file_count_truncated"] is False
    assert data["resolved_dest"] == str(landing)


def test_move_folder_preflight_caps_existing_destination_count(app_and_db, tmp_path):
    """Preflight should not recursively count an unbounded destination tree."""
    app, db = app_and_db
    dst = tmp_path / "dest"
    dst.mkdir()

    folder = db.get_folder_tree()[0]
    folder_name = folder["name"] or os.path.basename(folder["path"].rstrip("/\\"))
    landing = dst / folder_name
    landing.mkdir()
    for idx in range(1001):
        (landing / f"already-{idx}.jpg").write_bytes(b"x")

    client = app.test_client()
    resp = client.post("/api/move-folder/preflight", json={
        "folder_id": folder["id"],
        "destination": str(dst),
    })
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["exists"] is True
    assert data["file_count"] == 1000
    assert data["file_count_truncated"] is True


def test_move_folder_preflight_caps_existing_destination_dir_fanout(app_and_db, tmp_path):
    """A flat fanout of subdirectories must trip the dir cap mid-scan, not after enumerating all of them."""
    app, db = app_and_db
    dst = tmp_path / "dest"
    dst.mkdir()

    folder = db.get_folder_tree()[0]
    folder_name = folder["name"] or os.path.basename(folder["path"].rstrip("/\\"))
    landing = dst / folder_name
    landing.mkdir()
    # No files at all; just a large flat set of subdirectories. The cap is 2000,
    # so 2500 children must not all be queued before truncation is reported.
    for idx in range(2500):
        (landing / f"sub-{idx:04d}").mkdir()

    client = app.test_client()
    resp = client.post("/api/move-folder/preflight", json={
        "folder_id": folder["id"],
        "destination": str(dst),
    })
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["exists"] is True
    assert data["file_count"] == 0
    assert data["file_count_truncated"] is True


def test_move_folder_preflight_exact_mode_counts_past_quick_cap(app_and_db, tmp_path):
    """mode='exact' counts past the 1000-file quick-scan cap and reports the
    true number with truncated=False, since the destination's true size is
    well under the larger exact-mode cap."""
    app, db = app_and_db
    dst = tmp_path / "dest"
    dst.mkdir()

    folder = db.get_folder_tree()[0]
    folder_name = folder["name"] or os.path.basename(folder["path"].rstrip("/\\"))
    landing = dst / folder_name
    landing.mkdir()
    for idx in range(1001):
        (landing / f"already-{idx}.jpg").write_bytes(b"x")

    client = app.test_client()
    resp = client.post("/api/move-folder/preflight", json={
        "folder_id": folder["id"],
        "destination": str(dst),
        "mode": "exact",
    })
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["exists"] is True
    assert data["file_count"] == 1001
    assert data["file_count_truncated"] is False
    assert "preview" not in data


def test_move_folder_preflight_exact_mode_is_capped_not_unbounded(
    app_and_db, tmp_path, monkeypatch,
):
    """mode='exact' must NOT walk the destination tree uncapped. The UI's
    requestExactDestCount fires from the keystroke-driven path, so an
    unbounded walk on a hoarder-NAS target with millions of files would pin
    a Flask worker for minutes (the UI seq guard only discards stale replies,
    not server-side work). Exact mode passes a generous but finite cap so the
    worst case is seconds, and the response surfaces truncated=True if the
    cap was hit — the UI already renders that as 'at least N'."""
    import app as app_module

    app, db = app_and_db
    dst = tmp_path / "dest"
    dst.mkdir()
    folder = db.get_folder_tree()[0]
    folder_name = folder["name"] or os.path.basename(folder["path"].rstrip("/\\"))
    landing = dst / folder_name
    landing.mkdir()

    captured = {}

    def fake_scan(root_path, file_limit=None, dir_limit=None):
        captured["file_limit"] = file_limit
        captured["dir_limit"] = dir_limit
        # Pretend the cap was hit so the truncated flag can be checked too.
        return file_limit or 0, True

    monkeypatch.setattr(app_module, "_scan_dir_file_count", fake_scan)

    client = app.test_client()
    resp = client.post("/api/move-folder/preflight", json={
        "folder_id": folder["id"],
        "destination": str(dst),
        "mode": "exact",
    })
    assert resp.status_code == 200
    data = resp.get_json()
    # Both caps must be set (not None) so the walk is bounded, and both must
    # be much larger than the quick-mode caps so realistic libraries never
    # see "at least N" from exact mode.
    assert captured["file_limit"] is not None and captured["file_limit"] >= 100000
    assert captured["dir_limit"] is not None and captured["dir_limit"] >= 50000
    # Truncation from the helper must surface in the response so the UI can
    # render "at least N" instead of implying an exact total.
    assert data["file_count_truncated"] is True
    assert data["file_count"] == captured["file_limit"]


def test_move_folder_preflight_preview_reports_transfer_counts(app_and_db, tmp_path):
    """mode='preview' reports how many source files would copy vs. be skipped
    as already present — counting every file (sidecars included), not just
    tracked photos."""
    app, db = app_and_db

    # Real on-disk source folder with a nested file and an XMP sidecar.
    src = tmp_path / "src_folder"
    src.mkdir()
    (src / "a.jpg").write_bytes(b"a")
    (src / "a.jpg.xmp").write_bytes(b"sidecar")
    (src / "sub").mkdir()
    (src / "sub" / "c.jpg").write_bytes(b"c")
    fid = db.add_folder(str(src), name="src_folder")

    # Destination already holds one of the source files (a resume scenario).
    dst = tmp_path / "dest"
    dst.mkdir()
    landing = dst / "src_folder"
    landing.mkdir()
    (landing / "a.jpg").write_bytes(b"a")

    client = app.test_client()
    resp = client.post("/api/move-folder/preflight", json={
        "folder_id": fid,
        "destination": str(dst),
        "mode": "preview",
    })
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["exists"] is True
    preview = data["preview"]
    # a.jpg already present -> skip; a.jpg.xmp and sub/c.jpg missing -> copy.
    assert preview["will_skip"] == 1
    assert preview["will_copy"] == 2
    assert preview["source_total"] == 3


def test_move_folder_preflight_preview_caps_destination_scan(app_and_db, tmp_path):
    """mode='preview' must NOT walk the destination uncapped. The merge dialog
    uses the preview's copy/skip counts, not the raw destination file_count, so
    a full destination-tree walk here would block a Flask worker on large
    resume targets (NAS folders with millions of unrelated files) for no UI
    benefit. Capped behavior matches mode='quick': file_count plateaus at the
    cap and file_count_truncated flips to True."""
    app, db = app_and_db

    src = tmp_path / "src_folder"
    src.mkdir()
    (src / "a.jpg").write_bytes(b"a")
    fid = db.add_folder(str(src), name="src_folder")

    dst = tmp_path / "dest"
    dst.mkdir()
    landing = dst / "src_folder"
    landing.mkdir()
    for idx in range(1001):
        (landing / f"already-{idx}.jpg").write_bytes(b"x")

    client = app.test_client()
    resp = client.post("/api/move-folder/preflight", json={
        "folder_id": fid,
        "destination": str(dst),
        "mode": "preview",
    })
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["exists"] is True
    assert data["file_count"] == 1000
    assert data["file_count_truncated"] is True
    # The preview block still describes the source -> destination transfer.
    assert "preview" in data


def test_move_folder_preflight_preview_omitted_when_dest_missing(app_and_db, tmp_path):
    """No preview block when the destination doesn't exist — there is nothing
    to merge into, so a fresh move copies everything."""
    app, db = app_and_db
    folder = db.get_folder_tree()[0]

    client = app.test_client()
    resp = client.post("/api/move-folder/preflight", json={
        "folder_id": folder["id"],
        "destination": str(tmp_path / "nonexistent"),
        "mode": "preview",
    })
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["exists"] is False
    assert "preview" not in data


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
