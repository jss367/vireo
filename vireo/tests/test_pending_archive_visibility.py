"""Pending-transfer estimates describe cataloged originals without scanning disk."""
from services import photo_ingest, workspace_folders


def test_transfer_summary_counts_the_staging_tree_and_retains_rejected_photos(app_and_db, tmp_path):
    app, db = app_and_db
    staging = tmp_path / "staging" / "shoot"
    staging.mkdir(parents=True)
    root = workspace_folders.add_folder(db, str(staging))
    nested = workspace_folders.add_folder(db, str(staging / "day"))
    elsewhere = workspace_folders.add_folder(db, str(staging) + "-other")
    for folder, name, size in ((root, "keep.NEF", 2**30), (nested, "reject.NEF", 3 * 2**30),
                               (elsewhere, "unrelated.NEF", 10 * 2**30)):
        pid = photo_ingest.add_photo(db, folder, name, ".nef", size, 100)
        if name == "reject.NEF":
            db.photo_review.set_flag(pid, "rejected")
            db.commit()
    db.pending_archives.register("waiting", "/Volumes/NAS/Shoot", str(staging), {})
    db.conn.execute("UPDATE pending_archives SET created_at='2026-10-01 12:00:00' WHERE id='waiting'")
    db.commit()
    item, = app.test_client().get("/api/import/pending-archives").get_json()["items"]
    assert item["photo_count"] == 2
    assert item["cataloged_bytes"] == 4 * 2**30
    assert item["created_at"] == "2026-10-01 12:00:00"
    assert item["destination"] == "/Volumes/NAS/Shoot"
    assert item["source_available"] is True
    assert item["state"] == "ready"
    # The folder can be unavailable while the catalog estimate remains useful.
    staging.rmdir()
    item, = app.test_client().get("/api/import/pending-archives").get_json()["items"]
    assert item["source_available"] is False
    assert item["cataloged_bytes"] == 4 * 2**30


def test_transfer_estimates_are_scoped_to_the_active_workspaces_records(app_and_db, tmp_path):
    app, db = app_and_db
    active = db.require_workspace_id()
    db.pending_archives.register("mine", "/nas/mine", str(tmp_path / "mine"), {})
    db.set_active_workspace(db.create_workspace("Other"))
    db.pending_archives.register("theirs", "/nas/theirs", str(tmp_path / "theirs"), {})
    db.set_active_workspace(active)
    items = app.test_client().get("/api/import/pending-archives").get_json()["items"]
    assert [item["id"] for item in items] == ["mine"]
    assert items[0]["photo_count"] == items[0]["cataloged_bytes"] == 0


def test_catalog_totals_deduplicate_and_chunk_folder_ids(db, tmp_path):
    import sqlite3

    folder = workspace_folders.add_folder(db, str(tmp_path))
    photo_ingest.add_photo(db, folder, "photo.jpg", ".jpg", 1234, 100)
    db.conn.setlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, 999)
    ids = [folder] * 10 + list(range(folder, folder + 1200))
    assert db.pending_archives.cataloged_photo_totals(ids) == (1, 1234)
    assert db.pending_archives.cataloged_photo_totals([]) == (0, 0)
    db.conn.execute("UPDATE photos SET file_size=NULL WHERE folder_id=?", (folder,))
    assert db.pending_archives.cataloged_photo_totals([folder]) == (1, None)
