"""API tests for /api/duplicates/scan and /api/duplicates/apply."""
from wait import wait_for_job_via_client


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

    data = wait_for_job_via_client(client, job_id)
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
    # Booleans must be rejected: ``isinstance(True, int)`` is True in Python,
    # so a naive int-only check would silently accept ``[true]`` and treat
    # it as photo id 1 — which could trash whichever rejected row happens
    # to live at that id.
    assert client.post(
        "/api/duplicates/delete-loser-files", json={"photo_ids": [True]},
    ).status_code == 400
    assert client.post(
        "/api/duplicates/delete-loser-files", json={"photo_ids": [False]},
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


# ---------------------------------------------------------------------------
# /api/duplicates/last-scan — restore the most recent completed scan's result
# so navigating away from /duplicates and back doesn't force a rescan.
# ---------------------------------------------------------------------------


def test_last_scan_returns_not_found_when_no_history(app_and_db):
    """No prior scan -> {found: false}."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get("/api/duplicates/last-scan")
    assert resp.status_code == 200
    assert resp.get_json() == {"found": False}


def test_last_scan_returns_completed_scan_result(app_and_db):
    """After a scan completes, last-scan returns its proposals."""
    app, db = app_and_db
    fid = db.add_folder("/tmp/duplastscan1")
    _seed_pair(db, "HLAST", fid)

    client = app.test_client()
    job_id = client.post("/api/duplicates/scan").get_json()["job_id"]
    wait_for_job_via_client(client, job_id, wait_for_history=True)

    resp = client.get("/api/duplicates/last-scan")
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["found"] is True
    assert body["job_id"] == job_id
    assert body["finished_at"]
    hashes = [p["file_hash"] for p in body["result"]["proposals"]]
    assert "HLAST" in hashes


def test_last_scan_picks_most_recent_completed(app_and_db):
    """Two scans -> last-scan reflects the newer one."""
    app, db = app_and_db
    fid = db.add_folder("/tmp/duplastscan2")
    _seed_pair(db, "HOLD", fid)

    client = app.test_client()
    job1 = client.post("/api/duplicates/scan").get_json()["job_id"]
    wait_for_job_via_client(client, job1, wait_for_history=True)

    # Add another duplicate group then rescan.
    _seed_pair(db, "HNEW", fid, name_a="n.jpg", name_b="n (2).jpg")
    job2 = client.post("/api/duplicates/scan").get_json()["job_id"]
    wait_for_job_via_client(client, job2, wait_for_history=True)

    body = client.get("/api/duplicates/last-scan").get_json()
    assert body["found"] is True
    assert body["job_id"] == job2
    hashes = [p["file_hash"] for p in body["result"]["proposals"]]
    assert "HNEW" in hashes
    assert "HOLD" in hashes  # still present in the library


def test_last_scan_drops_resolved_groups_after_loser_rows_deleted(
    app_and_db, tmp_path,
):
    """After /api/duplicates/delete-loser-files removes the loser rows, the
    cached scan blob in job_history still references them. Without
    filtering on read, reloading /duplicates resurrects the pre-cleanup
    snapshot — same groups, same "still on disk" stats. The endpoint
    must drop resolved groups whose losers no longer exist and recompute
    aggregate counts so the page reflects current state.
    """
    app, db = app_and_db
    w, l, _winner_path, _loser_path = _seed_pair_with_real_files(
        db, tmp_path, "HCLEAN1",
    )

    client = app.test_client()
    job_id = client.post("/api/duplicates/scan").get_json()["job_id"]
    wait_for_job_via_client(client, job_id, wait_for_history=True)

    body = client.get("/api/duplicates/last-scan").get_json()
    hashes = [p["file_hash"] for p in body["result"]["proposals"]]
    assert "HCLEAN1" in hashes
    assert body["result"]["resolved_group_count"] >= 1

    resp = client.post(
        "/api/duplicates/delete-loser-files", json={"photo_ids": [l]},
    )
    assert resp.status_code == 200
    assert resp.get_json()["trashed"] == 1

    body = client.get("/api/duplicates/last-scan").get_json()
    hashes = [p["file_hash"] for p in body["result"]["proposals"]]
    assert "HCLEAN1" not in hashes
    assert body["result"]["resolved_group_count"] == 0
    assert body["result"]["resolved_loser_count"] == 0


def test_last_scan_keeps_resolved_group_with_surviving_losers(
    app_and_db, tmp_path,
):
    """Partial cleanup: a resolved group with two losers, one of which has
    been deleted, still appears with the surviving loser. Without this,
    a single failed-trash + retry would erase the whole group from view.
    """
    app, db = app_and_db
    folder = tmp_path / "dup_partial"
    folder.mkdir()
    fid = db.add_folder(str(folder))
    # Three rows sharing a hash: hook auto-rejects the (2)/(3) suffixes,
    # leaving the bare filename as the kept winner.
    for name in ("owl.jpg", "owl (2).jpg", "owl (3).jpg"):
        (folder / name).write_bytes(b"x" * 100)
    w = db.add_photo(folder_id=fid, filename="owl.jpg", extension=".jpg",
                     file_size=100, file_mtime=100.0, file_hash="HPART")
    l1 = db.add_photo(folder_id=fid, filename="owl (2).jpg", extension=".jpg",
                      file_size=100, file_mtime=200.0, file_hash="HPART")
    l2 = db.add_photo(folder_id=fid, filename="owl (3).jpg", extension=".jpg",
                      file_size=100, file_mtime=300.0, file_hash="HPART")

    client = app.test_client()
    job_id = client.post("/api/duplicates/scan").get_json()["job_id"]
    wait_for_job_via_client(client, job_id, wait_for_history=True)

    # Trash just one of the two losers.
    resp = client.post(
        "/api/duplicates/delete-loser-files", json={"photo_ids": [l1]},
    )
    assert resp.status_code == 200
    assert resp.get_json()["trashed"] == 1

    body = client.get("/api/duplicates/last-scan").get_json()
    proposals = [p for p in body["result"]["proposals"]
                 if p["file_hash"] == "HPART"]
    assert len(proposals) == 1
    surviving_ids = [ll["id"] for ll in proposals[0]["losers"]]
    assert surviving_ids == [l2]
    assert body["result"]["resolved_group_count"] == 1
    assert body["result"]["resolved_loser_count"] == 1


def test_bulk_resolve_endpoint_resolves_by_folder(app_and_db, tmp_path):
    """POST /api/duplicates/bulk-resolve forces winners by keep_folder for
    every supplied hash and returns a summary."""
    app, db = app_and_db
    a_dir = tmp_path / "dupbulka"
    b_dir = tmp_path / "dupbulkb"
    a_dir.mkdir()
    b_dir.mkdir()
    a_fid = db.add_folder(str(a_dir))
    b_fid = db.add_folder(str(b_dir))
    # Seed three groups with one file in each folder. Real on-disk files so
    # the existence guard in bulk_resolve_by_folder is satisfied.
    pairs = []
    for h, name in [("HBA1", "owl.jpg"), ("HBA2", "hawk.jpg"), ("HBA3", "finch.jpg")]:
        (a_dir / name).write_bytes(b"x")
        (b_dir / name).write_bytes(b"x")
        p_a = db.add_photo(folder_id=a_fid, filename=name, extension=".jpg",
                           file_size=1000, file_mtime=100.0, file_hash=h)
        p_b = db.add_photo(folder_id=b_fid, filename=name, extension=".jpg",
                           file_size=1000, file_mtime=100.0, file_hash=h)
        db.conn.execute("UPDATE photos SET flag='none' WHERE file_hash=?", (h,))
        pairs.append((h, p_a, p_b))
    db.conn.commit()

    resp = app.test_client().post("/api/duplicates/bulk-resolve", json={
        "file_hashes": ["HBA1", "HBA2", "HBA3"],
        "keep_folder": str(b_dir),
    })
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["ok"] is True
    assert body["resolved_count"] == 3
    assert body["skipped"] == []
    # All /b photos kept; all /a photos rejected.
    for h, p_a, p_b in pairs:
        flags = {
            r["id"]: r["flag"]
            for r in db.conn.execute(
                "SELECT id, flag FROM photos WHERE id IN (?, ?)", (p_a, p_b)
            ).fetchall()
        }
        assert flags == {p_a: "rejected", p_b: "none"}, f"hash {h}"
    # Loser ids surfaced so the UI can pipe them to delete-loser-files.
    surfaced_loser_ids = sorted(
        lid for r in body["resolved"] for lid in r["loser_ids"]
    )
    assert surfaced_loser_ids == sorted(p_a for _, p_a, _ in pairs)


def test_bulk_resolve_endpoint_skips_hash_with_no_candidate_in_folder(app_and_db, tmp_path):
    """A hash whose candidates all live outside keep_folder is reported in
    skipped, not as a fatal error — the rest of the batch still resolves."""
    app, db = app_and_db
    a_dir = tmp_path / "dupbulkskipa"
    b_dir = tmp_path / "dupbulkskipb"
    c_dir = tmp_path / "dupbulkskipc"
    for d in (a_dir, b_dir, c_dir):
        d.mkdir()
    a_fid = db.add_folder(str(a_dir))
    b_fid = db.add_folder(str(b_dir))
    c_fid = db.add_folder(str(c_dir))
    folder_by_id = {a_fid: a_dir, b_fid: b_dir, c_fid: c_dir}
    # Distinct filenames per group so the UNIQUE(folder_id, filename) on
    # photos doesn't collide when both groups share folder /b.
    for h, fids, name in [("OK", (a_fid, b_fid), "ok.jpg"),
                          ("SKIP", (b_fid, c_fid), "skip.jpg")]:
        for fid in fids:
            (folder_by_id[fid] / name).write_bytes(b"x")
            db.add_photo(folder_id=fid, filename=name, extension=".jpg",
                         file_size=1000, file_mtime=100.0, file_hash=h)
        db.conn.execute("UPDATE photos SET flag='none' WHERE file_hash=?", (h,))
    db.conn.commit()

    resp = app.test_client().post("/api/duplicates/bulk-resolve", json={
        "file_hashes": ["OK", "SKIP"],
        "keep_folder": str(a_dir),
    })
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["resolved_count"] == 1
    assert body["resolved"][0]["file_hash"] == "OK"
    assert body["skipped"] == [
        {"file_hash": "SKIP", "reason": "no candidate in keep_folder"}
    ]


def test_bulk_resolve_endpoint_validates_inputs(app_and_db):
    app, _db = app_and_db
    client = app.test_client()
    # Missing body
    assert client.post("/api/duplicates/bulk-resolve").status_code == 400
    # Missing file_hashes
    assert client.post("/api/duplicates/bulk-resolve",
                       json={"keep_folder": "/x"}).status_code == 400
    # Empty file_hashes
    assert client.post("/api/duplicates/bulk-resolve",
                       json={"file_hashes": [], "keep_folder": "/x"}
                       ).status_code == 400
    # Missing keep_folder
    assert client.post("/api/duplicates/bulk-resolve",
                       json={"file_hashes": ["H"]}).status_code == 400
    # Bad type for keep_folder
    assert client.post("/api/duplicates/bulk-resolve",
                       json={"file_hashes": ["H"], "keep_folder": 123}
                       ).status_code == 400
    # Bad entry in file_hashes
    assert client.post("/api/duplicates/bulk-resolve",
                       json={"file_hashes": [123], "keep_folder": "/x"}
                       ).status_code == 400


def test_last_scan_ignores_failed_jobs(app_and_db):
    """A failed scan in history must not be served as the 'last' result."""
    app, db = app_and_db
    # Insert a synthetic 'failed' duplicate-scan row directly into history.
    db.conn.execute(
        """INSERT INTO job_history
              (id, type, status, started_at, finished_at, duration, result)
           VALUES (?, 'duplicate-scan', 'failed', ?, ?, 0.0, NULL)""",
        ("duplicate-scan-failed-1", "2026-01-01T00:00:00", "2026-01-01T00:00:01"),
    )
    db.conn.commit()

    body = app.test_client().get("/api/duplicates/last-scan").get_json()
    assert body == {"found": False}


# -----------------------------------------------------------------------------
# _ensure_volume_trashes_dir — pre-trash hook that creates the per-user
# .Trashes directory on external/network volumes so send2trash legacy mode
# stops falling back to AppleScript Finder (which times out for hours).
# -----------------------------------------------------------------------------

def test_ensure_volume_trashes_dir_creates_dir_under_volume(monkeypatch):
    """Path under /Volumes/X triggers makedirs for /Volumes/X/.Trashes/$UID."""
    import os

    from app import _ensure_volume_trashes_dir

    calls = []
    def fake_makedirs(path, mode=None, exist_ok=False):
        calls.append((path, mode, exist_ok))
    monkeypatch.setattr("os.makedirs", fake_makedirs)

    seen = set()
    _ensure_volume_trashes_dir("/Volumes/Photography/sub/file.NEF", seen)

    assert len(calls) == 1
    path, mode, exist_ok = calls[0]
    assert path == f"/Volumes/Photography/.Trashes/{os.getuid()}"
    assert mode == 0o700
    assert exist_ok is True
    assert "/Volumes/Photography" in seen


def test_ensure_volume_trashes_dir_idempotent_per_volume(monkeypatch):
    """Repeated calls for files on the same volume only run makedirs once."""
    from app import _ensure_volume_trashes_dir

    calls = []
    monkeypatch.setattr("os.makedirs", lambda *a, **k: calls.append((a, k)))

    seen = set()
    _ensure_volume_trashes_dir("/Volumes/Photography/a.NEF", seen)
    _ensure_volume_trashes_dir("/Volumes/Photography/b.NEF", seen)
    _ensure_volume_trashes_dir("/Volumes/Photography/sub/c.NEF", seen)

    assert len(calls) == 1


def test_ensure_volume_trashes_dir_creates_per_distinct_volume(monkeypatch):
    """Files on different volumes each get their own .Trashes dir created."""
    from app import _ensure_volume_trashes_dir

    calls = []
    monkeypatch.setattr("os.makedirs", lambda p, **k: calls.append(p))

    seen = set()
    _ensure_volume_trashes_dir("/Volumes/Photography/a.NEF", seen)
    _ensure_volume_trashes_dir("/Volumes/Backup/a.NEF", seen)

    assert len(calls) == 2
    assert "/Volumes/Photography/.Trashes/" in calls[0]
    assert "/Volumes/Backup/.Trashes/" in calls[1]


def test_ensure_volume_trashes_dir_skips_non_volume_paths(monkeypatch):
    """System-Trash-managed paths (~, /tmp, /private) don't need this hook."""
    from app import _ensure_volume_trashes_dir

    calls = []
    monkeypatch.setattr("os.makedirs", lambda *a, **k: calls.append(a))

    _ensure_volume_trashes_dir("/Users/julius/photo.NEF", set())
    _ensure_volume_trashes_dir("/private/tmp/photo.NEF", set())
    _ensure_volume_trashes_dir("relative/path.NEF", set())
    _ensure_volume_trashes_dir("/Volumes", set())  # bare /Volumes, no mount

    assert calls == []


def test_ensure_volume_trashes_dir_swallows_oserror(monkeypatch):
    """Read-only mount or ACL block must not break the bulk trash — let the
    actual send2trash call surface the more specific error."""
    from app import _ensure_volume_trashes_dir

    def boom(*a, **k):
        raise OSError("read-only file system")
    monkeypatch.setattr("os.makedirs", boom)

    _ensure_volume_trashes_dir("/Volumes/X/photo.NEF", set())  # must not raise
