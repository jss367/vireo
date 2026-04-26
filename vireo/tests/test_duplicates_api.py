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


def test_apply_endpoint_bad_entry_in_list_returns_400(app_and_db):
    """Any non-string or empty-string entry in the list -> 400 (no silent skip)."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.post(
        "/api/duplicates/apply",
        json={"hashes": ["H1", 42, "", "H2"]},
    )
    assert resp.status_code == 400
    assert "error" in resp.get_json()


# ---------------------------------------------------------------------------
# /api/duplicates/delete-loser-files
# ---------------------------------------------------------------------------

def _seed_pair_with_real_files(db, tmp_path, file_hash):
    """Create two real on-disk files (so trash can succeed) and matching DB rows
    with the same hash, with the second auto-rejected by the hook.

    Returns (winner_id, loser_id, winner_path, loser_path).
    """
    folder = tmp_path / f"d_{file_hash}"
    folder.mkdir()
    winner_path = folder / "owl.jpg"
    loser_path = folder / "owl-2.jpg"
    winner_path.write_bytes(b"x" * 100)
    loser_path.write_bytes(b"x" * 100)

    fid = db.add_folder(str(folder))
    p1 = db.add_photo(folder_id=fid, filename="owl.jpg", extension=".jpg",
                      file_size=100, file_mtime=100.0, file_hash=file_hash)
    p2 = db.add_photo(folder_id=fid, filename="owl-2.jpg", extension=".jpg",
                      file_size=100, file_mtime=200.0, file_hash=file_hash)
    # Hook auto-rejects owl-2 (clean filename wins).
    flag1 = db.conn.execute("SELECT flag FROM photos WHERE id=?", (p1,)).fetchone()["flag"]
    flag2 = db.conn.execute("SELECT flag FROM photos WHERE id=?", (p2,)).fetchone()["flag"]
    assert flag1 != "rejected"
    assert flag2 == "rejected"
    return p1, p2, str(winner_path), str(loser_path)


def test_delete_loser_files_trashes_loser_and_keeps_winner(app_and_db, tmp_path):
    """The endpoint trashes the rejected loser's file but leaves the kept winner."""
    app, db = app_and_db
    w, l, winner_path, loser_path = _seed_pair_with_real_files(db, tmp_path, "TRASH1")

    import os
    assert os.path.isfile(loser_path)

    client = app.test_client()
    resp = client.post(
        "/api/duplicates/delete-loser-files",
        json={"photo_ids": [l]},
    )
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["trashed"] == 1
    assert body["skipped"] == []
    assert body["failed"] == []
    # File is gone from its original location (sent to Trash).
    assert not os.path.isfile(loser_path)
    # Winner file is untouched.
    assert os.path.isfile(winner_path)
    # Loser DB row is gone — without this the disk-cleanup-summary count
    # would keep counting it forever and the navbar banner would re-show
    # on every page load. Winner row stays so existing photo_id references
    # (collections, edits) keep resolving.
    loser_row = db.conn.execute(
        "SELECT 1 FROM photos WHERE id=?", (l,),
    ).fetchone()
    assert loser_row is None
    winner_row = db.conn.execute(
        "SELECT 1 FROM photos WHERE id=?", (w,),
    ).fetchone()
    assert winner_row is not None


def test_delete_loser_files_drops_summary_count_after_trash(app_and_db, tmp_path):
    """After a successful trash, /disk-cleanup-summary count must drop.

    Locks in the bug-fix contract: counting all anchored rejected rows
    without a presence check would inflate the count forever — even after
    Vireo's own trash endpoint cleaned the files. The endpoint compensates
    by deleting the row after trash so the next summary call reflects
    reality.
    """
    app, db = app_and_db
    _w, l, _wp, _lp = _seed_pair_with_real_files(db, tmp_path, "TRASH4")

    client = app.test_client()
    pre = client.get("/api/duplicates/disk-cleanup-summary").get_json()
    assert pre["count"] == 1

    client.post("/api/duplicates/delete-loser-files", json={"photo_ids": [l]})

    post = client.get("/api/duplicates/disk-cleanup-summary").get_json()
    assert post["count"] == 0
    assert post["total_size"] == 0


def test_delete_loser_files_refuses_non_rejected_photo(app_and_db, tmp_path):
    """Trying to trash a non-rejected photo's file is skipped, not executed."""
    app, db = app_and_db
    w, _l, winner_path, _loser_path = _seed_pair_with_real_files(db, tmp_path, "TRASH2")

    import os
    client = app.test_client()
    resp = client.post(
        "/api/duplicates/delete-loser-files",
        json={"photo_ids": [w]},
    )
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["trashed"] == 0
    assert len(body["skipped"]) == 1
    assert body["skipped"][0]["reason"] == "photo is not rejected"
    # Winner file untouched.
    assert os.path.isfile(winner_path)


def test_delete_loser_files_refuses_when_no_kept_anchor(app_and_db, tmp_path):
    """If a hash has no non-rejected photo, the rejected row isn't a duplicate
    loser and the endpoint must not trash its file."""
    app, db = app_and_db

    folder = tmp_path / "lonely"
    folder.mkdir()
    src = folder / "lonely.jpg"
    src.write_bytes(b"y" * 100)

    fid = db.add_folder(str(folder))
    pid = db.add_photo(folder_id=fid, filename="lonely.jpg", extension=".jpg",
                       file_size=100, file_mtime=100.0, file_hash="HALONE")
    # Reject the only copy (e.g. user manually rejected it for non-duplicate reasons).
    db.conn.execute("UPDATE photos SET flag='rejected' WHERE id=?", (pid,))
    db.conn.commit()

    import os
    client = app.test_client()
    resp = client.post(
        "/api/duplicates/delete-loser-files",
        json={"photo_ids": [pid]},
    )
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["trashed"] == 0
    assert len(body["skipped"]) == 1
    assert body["skipped"][0]["reason"] == "no duplicate winner exists"
    # File stays — must not be trashed without a winner anchor.
    assert os.path.isfile(str(src))


def test_delete_loser_files_handles_already_missing_file(app_and_db, tmp_path):
    """If the on-disk file was already removed (e.g. user trashed it manually),
    the endpoint reports 'file already missing' but still drops the orphan
    DB row so the disk-cleanup-summary stops re-counting it.
    """
    app, db = app_and_db
    _w, l, _wp, loser_path = _seed_pair_with_real_files(db, tmp_path, "TRASH3")

    import os
    os.remove(loser_path)
    assert not os.path.isfile(loser_path)

    client = app.test_client()
    resp = client.post(
        "/api/duplicates/delete-loser-files",
        json={"photo_ids": [l]},
    )
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["trashed"] == 0
    assert any(
        s["id"] == l and s["reason"] == "file already missing"
        for s in body["skipped"]
    )
    # Orphan row is gone so the next summary poll won't keep re-counting it.
    loser_row = db.conn.execute(
        "SELECT 1 FROM photos WHERE id=?", (l,),
    ).fetchone()
    assert loser_row is None


def test_delete_loser_files_chunks_large_id_lists(app_and_db, tmp_path, monkeypatch):
    """Bulk cleanup with id counts above ``_SQL_PARAM_CHUNK`` must not raise.

    SQLite's legacy ``SQLITE_MAX_VARIABLE_NUMBER`` cap is 999, so packaging
    the entire id list into a single IN clause would fail on those builds
    before any file gets trashed. We patch the cap down to 2 to cover the
    chunking path without seeding 1000 photos: the test still proves the
    endpoint splits the query.
    """
    import os

    import app as app_module

    monkeypatch.setattr(app_module, "_SQL_PARAM_CHUNK", 2)

    app, db = app_and_db
    loser_ids = []
    loser_paths = []
    # 5 dup pairs > chunk size of 2, so the lookup SELECT and the
    # delete_photos call both have to iterate.
    for i in range(5):
        _w, l, _wp, lp = _seed_pair_with_real_files(db, tmp_path, f"CHUNK{i}")
        loser_ids.append(l)
        loser_paths.append(lp)

    client = app.test_client()
    resp = client.post(
        "/api/duplicates/delete-loser-files",
        json={"photo_ids": loser_ids},
    )
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["trashed"] == 5
    assert body["failed"] == []
    # All loser files trashed and all rows gone — proving each chunk got
    # processed end-to-end (lookup + trash + delete) rather than only the
    # first 2 succeeding.
    for p in loser_paths:
        assert not os.path.isfile(p)
    placeholders = ",".join("?" * len(loser_ids))
    remaining = db.conn.execute(
        f"SELECT COUNT(*) FROM photos WHERE id IN ({placeholders})",
        loser_ids,
    ).fetchone()[0]
    assert remaining == 0


def test_delete_loser_files_validates_input(app_and_db):
    app, _ = app_and_db
    client = app.test_client()

    # Missing body
    assert client.post("/api/duplicates/delete-loser-files").status_code == 400
    # Missing photo_ids
    assert client.post(
        "/api/duplicates/delete-loser-files", json={},
    ).status_code == 400
    # Empty list
    assert client.post(
        "/api/duplicates/delete-loser-files", json={"photo_ids": []},
    ).status_code == 400
    # Non-int entry
    assert client.post(
        "/api/duplicates/delete-loser-files", json={"photo_ids": ["abc"]},
    ).status_code == 400


# ---------------------------------------------------------------------------
# /api/duplicates/disk-cleanup-summary
# ---------------------------------------------------------------------------

def test_disk_cleanup_summary_counts_only_anchored_rejections(app_and_db):
    """Counts rejected photos whose hash is held by a non-rejected anchor.
    Excludes purely-rejected hashes (manual rejections, no duplicate winner).
    """
    app, db = app_and_db
    fid = db.add_folder("/tmp/clsum")
    # Anchored loser: 1 kept + 1 rejected sharing a hash.
    db.add_photo(folder_id=fid, filename="a.jpg", extension=".jpg",
                 file_size=1000, file_mtime=1.0, file_hash="HCLEAN")
    db.add_photo(folder_id=fid, filename="a-2.jpg", extension=".jpg",
                 file_size=2000, file_mtime=2.0, file_hash="HCLEAN")
    # Hook auto-rejected a-2; counted size = 2000.

    # Unrelated rejected row (no hash twin) — must NOT be counted.
    pid_lone = db.add_photo(folder_id=fid, filename="lonely.jpg", extension=".jpg",
                            file_size=9999, file_mtime=3.0, file_hash="HLONE")
    db.conn.execute("UPDATE photos SET flag='rejected' WHERE id=?", (pid_lone,))
    db.conn.commit()

    client = app.test_client()
    resp = client.get("/api/duplicates/disk-cleanup-summary")
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["count"] == 1
    assert body["total_size"] == 2000


def test_disk_cleanup_summary_zero_when_clean(app_and_db):
    """Returns count=0 when there's nothing to clean."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get("/api/duplicates/disk-cleanup-summary")
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["count"] == 0
    assert body["total_size"] == 0
