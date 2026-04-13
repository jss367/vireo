"""API tests for /api/duplicates/scan and /api/duplicates/apply."""
import time


def _seed_pair(db, file_hash, fid, name_a="x.jpg", name_b="x (2).jpg"):
    """Seed two dup photos sharing a hash, then undo the hook's auto-reject."""
    p1 = db.add_photo(
        folder_id=fid, filename=name_a, extension=".jpg",
        file_size=1000, file_mtime=100.0, file_hash=file_hash,
    )
    p2 = db.add_photo(
        folder_id=fid, filename=name_b, extension=".jpg",
        file_size=1000, file_mtime=200.0, file_hash=file_hash,
    )
    db.conn.execute(
        "UPDATE photos SET flag='none' WHERE file_hash=?", (file_hash,)
    )
    db.conn.commit()
    return p1, p2


def test_scan_endpoint_starts_job(app_and_db):
    """POST /api/duplicates/scan kicks off a background job and returns job_id."""
    app, db = app_and_db
    fid = db.add_folder("/tmp/dupscan1")
    _seed_pair(db, "H1", fid)

    client = app.test_client()
    resp = client.post("/api/duplicates/scan")
    assert resp.status_code == 200
    body = resp.get_json()
    assert "job_id" in body
    assert body["job_id"].startswith("duplicate-scan-")


def test_scan_endpoint_job_completes_with_proposals(app_and_db):
    """The spawned job completes and stores proposals in its result."""
    app, db = app_and_db
    fid = db.add_folder("/tmp/dupscan2")
    _seed_pair(db, "HSCAN", fid)

    client = app.test_client()
    resp = client.post("/api/duplicates/scan")
    job_id = resp.get_json()["job_id"]

    for _ in range(50):
        resp = client.get(f"/api/jobs/{job_id}")
        data = resp.get_json()
        if data["status"] in ("completed", "failed"):
            break
        time.sleep(0.1)

    assert data["status"] == "completed"
    result = data["result"]
    assert result["group_count"] >= 1
    hashes = [p["file_hash"] for p in result["proposals"]]
    assert "HSCAN" in hashes


def test_apply_endpoint_rejects_losers(app_and_db):
    """POST /api/duplicates/apply flags the losers as rejected."""
    app, db = app_and_db
    fid = db.add_folder("/tmp/dupapply1")
    p1, p2 = _seed_pair(db, "H2", fid, name_a="y.jpg", name_b="y (2).jpg")

    client = app.test_client()
    resp = client.post("/api/duplicates/apply", json={"hashes": ["H2"]})
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["rejected_count"] == 1

    flag1 = db.conn.execute("SELECT flag FROM photos WHERE id=?", (p1,)).fetchone()["flag"]
    flag2 = db.conn.execute("SELECT flag FROM photos WHERE id=?", (p2,)).fetchone()["flag"]
    assert flag1 != "rejected"
    assert flag2 == "rejected"


def test_apply_endpoint_rejects_only_losers_not_all_but_one(app_and_db):
    """With a 3-way dup, exactly the resolver-picked losers are rejected."""
    app, db = app_and_db
    fid = db.add_folder("/tmp/dupapply2")
    p1 = db.add_photo(
        folder_id=fid, filename="z.jpg", extension=".jpg",
        file_size=1000, file_mtime=100.0,
    )
    p2 = db.add_photo(
        folder_id=fid, filename="z (2).jpg", extension=".jpg",
        file_size=1000, file_mtime=100.0,
    )
    p3 = db.add_photo(
        folder_id=fid, filename="z copy.jpg", extension=".jpg",
        file_size=1000, file_mtime=100.0,
    )
    db.conn.execute(
        "UPDATE photos SET file_hash='H3', flag='none' WHERE id IN (?, ?, ?)",
        (p1, p2, p3),
    )
    db.conn.commit()

    client = app.test_client()
    resp = client.post("/api/duplicates/apply", json={"hashes": ["H3"]})
    assert resp.status_code == 200
    assert resp.get_json()["rejected_count"] == 2

    # Clean filename p1 wins; the two dirty ones get rejected.
    flag1 = db.conn.execute("SELECT flag FROM photos WHERE id=?", (p1,)).fetchone()["flag"]
    flag2 = db.conn.execute("SELECT flag FROM photos WHERE id=?", (p2,)).fetchone()["flag"]
    flag3 = db.conn.execute("SELECT flag FROM photos WHERE id=?", (p3,)).fetchone()["flag"]
    assert flag1 != "rejected"
    assert flag2 == "rejected"
    assert flag3 == "rejected"


def test_apply_endpoint_skips_already_resolved_hash(app_and_db):
    """If only one non-rejected row remains, skip — return 0 rejects."""
    app, db = app_and_db
    fid = db.add_folder("/tmp/dupapply3")
    # Hook auto-rejects one; leave as-is so only 1 non-rejected row remains.
    db.add_photo(
        folder_id=fid, filename="w.jpg", extension=".jpg",
        file_size=1000, file_mtime=100.0, file_hash="H4",
    )
    db.add_photo(
        folder_id=fid, filename="w (2).jpg", extension=".jpg",
        file_size=1000, file_mtime=100.0, file_hash="H4",
    )

    client = app.test_client()
    resp = client.post("/api/duplicates/apply", json={"hashes": ["H4"]})
    assert resp.status_code == 200
    assert resp.get_json()["rejected_count"] == 0


def test_apply_endpoint_missing_hashes_returns_400(app_and_db):
    """Missing 'hashes' key -> 400."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.post("/api/duplicates/apply", json={})
    assert resp.status_code == 400
    assert "error" in resp.get_json()


def test_apply_endpoint_empty_hashes_returns_400(app_and_db):
    """Empty list -> 400."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.post("/api/duplicates/apply", json={"hashes": []})
    assert resp.status_code == 400


def test_apply_endpoint_non_list_hashes_returns_400(app_and_db):
    """Non-list 'hashes' value -> 400."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.post("/api/duplicates/apply", json={"hashes": "H1"})
    assert resp.status_code == 400


def test_apply_endpoint_no_json_body_returns_400(app_and_db):
    """Missing body entirely -> 400."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.post("/api/duplicates/apply")
    assert resp.status_code == 400
