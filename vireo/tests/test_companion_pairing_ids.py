"""Photo ids handed out by a scan must survive RAW+JPEG pairing.

On a first import a companion JPEG used to be inserted as its own
``photos`` row and merged into its RAW at the end of the scan. Its id had
already been reported through ``photo_callback`` by then, so the
after-import pipeline collection and the import-in-place collection held
an id whose row was gone. ``photos`` has no AUTOINCREMENT, so the next
insert can reuse that id, and an unrelated photo joined the old
collection.
"""

import json
import os
import sqlite3

import pytest
from PIL import Image


def _collection_photo_ids(db, collection_id):
    """The ``photo_ids`` a collection's stored rules name, in order."""
    row = db.conn.execute(
        "SELECT rules FROM collections WHERE id = ?", (collection_id,),
    ).fetchone()
    ids = []
    for rule in json.loads(row["rules"]):
        if rule.get("field") == "photo_ids":
            ids.extend(rule["value"])
    return ids


def _photo_ids_by_filename(db):
    return {
        r["filename"]: r["id"]
        for r in db.conn.execute("SELECT id, filename FROM photos")
    }


def _shoot_pairs(folder, stems, raw_ext=".cr3"):
    """A RAW and its camera JPEG per stem, the way a card holds them."""
    folder.mkdir(parents=True, exist_ok=True)
    for stem in stems:
        Image.new("RGB", (64, 48), "green").save(str(folder / f"{stem}.jpg"))
        (folder / f"{stem}{raw_ext}").write_bytes(os.urandom(256))


def _isolate_config(tmp_path, monkeypatch):
    import config as cfg

    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))


def _run_pipeline(tmp_path, db_path, ws_id, source):
    from pipeline_job import PipelineParams, run_pipeline_job
    from test_pipeline_job import FakeRunner, _make_job

    params = PipelineParams(
        source=str(source),
        skip_classify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )
    return run_pipeline_job(_make_job(), FakeRunner(), db_path, ws_id, params)


def test_pipeline_collection_names_only_surviving_raw_ids(
    tmp_path, monkeypatch,
):
    from db import Database

    _isolate_config(tmp_path, monkeypatch)
    card = tmp_path / "card"
    _shoot_pairs(card, ["IMG_001", "IMG_002", "IMG_003"])
    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    result = _run_pipeline(tmp_path, db_path, ws_id, card)

    ids = _photo_ids_by_filename(db)
    assert set(ids) == {"IMG_001.cr3", "IMG_002.cr3", "IMG_003.cr3"}
    members = _collection_photo_ids(db, result["collection_id"])
    assert sorted(members) == sorted(ids.values())


def test_later_import_does_not_join_an_earlier_pipeline_collection(
    tmp_path, monkeypatch,
):
    """The transient JPEG id with the highest rowid is the next insert's id."""
    from db import Database

    _isolate_config(tmp_path, monkeypatch)
    card = tmp_path / "card"
    _shoot_pairs(card, ["IMG_001", "IMG_002"])
    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    first = _run_pipeline(tmp_path, db_path, ws_id, card)

    later = tmp_path / "later"
    later.mkdir()
    Image.new("RGB", (64, 48), "blue").save(str(later / "heron.jpg"))
    _run_pipeline(tmp_path, db_path, ws_id, later)

    heron_id = _photo_ids_by_filename(db)["heron.jpg"]
    assert heron_id not in _collection_photo_ids(db, first["collection_id"])
    db.set_active_workspace(ws_id)
    first_photos = db.get_collection_photos(
        first["collection_id"], per_page=999999,
    )
    assert sorted(p["filename"] for p in first_photos) == [
        "IMG_001.cr3", "IMG_002.cr3",
    ]


def test_import_in_place_ids_and_collection_survive_pairing(
    app_and_db, tmp_path,
):
    from wait import wait_for_job_via_client

    app, db = app_and_db
    client = app.test_client()
    card = tmp_path / "import-card"
    _shoot_pairs(card, ["DSC_0001", "DSC_0002"], raw_ext=".arw")

    resp = client.post("/api/jobs/import-in-place", json={
        "sources": [str(card)],
        "after_import": None,
    })
    assert resp.status_code == 200, resp.get_json()
    job = wait_for_job_via_client(client, resp.get_json()["job_id"])
    assert job["status"] == "completed", job
    result = job["result"]

    ids = _photo_ids_by_filename(db)
    raw_ids = sorted([ids["DSC_0001.arw"], ids["DSC_0002.arw"]])
    assert "DSC_0001.jpg" not in ids and "DSC_0002.jpg" not in ids
    assert sorted(result["photo_ids"]) == raw_ids
    assert result["indexed"] == 2
    assert sorted(
        _collection_photo_ids(db, result["collection_id"])
    ) == raw_ids


def _import_in_place(client, source):
    from wait import wait_for_job_via_client

    resp = client.post("/api/jobs/import-in-place", json={
        "sources": [str(source)],
        "after_import": None,
    })
    assert resp.status_code == 200, resp.get_json()
    job = wait_for_job_via_client(client, resp.get_json()["job_id"])
    assert job["status"] == "completed", job
    return job["result"]


@pytest.mark.parametrize("raw_ext", [".cr3", ".nef"])
def test_first_scan_never_reports_or_inserts_a_companion_row(
    tmp_path, raw_ext,
):
    """No JPEG of a pair is ever a row, whichever file the scan reads first
    (``IMG.cr3`` sorts before ``IMG.jpg``, ``IMG.nef`` after it)."""
    from db import Database
    from scanner import scan

    card = tmp_path / "card"
    _shoot_pairs(card, ["IMG_001", "IMG_002"], raw_ext=raw_ext)
    db = Database(str(tmp_path / "test.db"))
    reported = []
    jpeg_rows_seen = []

    def on_photo(photo_id, path):
        reported.append((photo_id, os.path.basename(path)))
        jpeg_rows_seen.append(db.conn.execute(
            "SELECT COUNT(*) FROM photos WHERE extension = '.jpg'"
        ).fetchone()[0])

    counts = scan(str(card), db, photo_callback=on_photo)

    ids = _photo_ids_by_filename(db)
    assert set(ids) == {f"IMG_001{raw_ext}", f"IMG_002{raw_ext}"}
    assert {pid for pid, _name in reported} == set(ids.values())
    # Each JPEG is reported under its RAW's id.
    assert (ids[f"IMG_001{raw_ext}"], "IMG_001.jpg") in reported
    assert not any(jpeg_rows_seen)
    rows = db.conn.execute(
        "SELECT filename, companion_path FROM photos ORDER BY filename"
    ).fetchall()
    assert [tuple(r) for r in rows] == [
        (f"IMG_001{raw_ext}", "IMG_001.jpg"),
        (f"IMG_002{raw_ext}", "IMG_002.jpg"),
    ]
    identities = db.conn.execute(
        "SELECT filename, file_hash, needs_sync FROM companion_identities"
        " ORDER BY filename"
    ).fetchall()
    assert [r["filename"] for r in identities] == ["IMG_001.jpg", "IMG_002.jpg"]
    assert all(r["file_hash"] and r["needs_sync"] == 0 for r in identities)
    assert counts["indexed"] == 2
    assert counts["merged_companions"] == 2


def test_jpeg_cataloged_before_its_raw_follows_the_merge_in_pipeline(
    tmp_path, monkeypatch,
):
    """A JPEG that was its own photo is merged when its RAW arrives; the
    pipeline collection takes the RAW, not the merged-away id."""
    from db import Database

    _isolate_config(tmp_path, monkeypatch)
    card = tmp_path / "card"
    card.mkdir()
    Image.new("RGB", (64, 48), "green").save(str(card / "IMG_001.jpg"))
    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    _run_pipeline(tmp_path, db_path, ws_id, card)

    (card / "IMG_001.cr3").write_bytes(os.urandom(256))
    result = _run_pipeline(tmp_path, db_path, ws_id, card)

    ids = _photo_ids_by_filename(db)
    assert set(ids) == {"IMG_001.cr3"}
    assert _collection_photo_ids(db, result["collection_id"]) == [
        ids["IMG_001.cr3"],
    ]


def test_jpeg_cataloged_before_its_raw_follows_the_merge_in_place(
    app_and_db, tmp_path,
):
    app, db = app_and_db
    client = app.test_client()
    card = tmp_path / "import-card"
    card.mkdir()
    Image.new("RGB", (64, 48), "green").save(str(card / "DSC_0001.jpg"))
    _import_in_place(client, card)

    (card / "DSC_0001.arw").write_bytes(os.urandom(256))
    result = _import_in_place(client, card)

    ids = _photo_ids_by_filename(db)
    assert "DSC_0001.jpg" not in ids
    raw_id = ids["DSC_0001.arw"]
    assert result["photo_ids"] == [raw_id]
    assert _collection_photo_ids(db, result["collection_id"]) == [raw_id]


def test_new_photo_drops_a_stale_collection_entry_for_its_reused_id(tmp_path):
    """Catalogs already hold collections naming ids whose photo is gone; the
    photo SQLite next gives such an id must not join them."""
    from db import Database
    from scanner import scan

    db = Database(str(tmp_path / "test.db"))
    first = tmp_path / "first"
    first.mkdir()
    Image.new("RGB", (32, 32), "red").save(str(first / "kept.jpg"))
    scan(str(first), db)
    kept_id = _photo_ids_by_filename(db)["kept.jpg"]
    next_id = kept_id + 1
    stale = db.add_collection(
        "Pipeline old",
        json.dumps([{"field": "photo_ids", "value": [kept_id, next_id]}]),
    )

    later = tmp_path / "later"
    later.mkdir()
    Image.new("RGB", (32, 32), "blue").save(str(later / "heron.jpg"))
    scan(str(later), db)

    assert _photo_ids_by_filename(db)["heron.jpg"] == next_id
    assert _collection_photo_ids(db, stale) == [kept_id]


def test_jpeg_becomes_its_own_photo_when_its_raw_changes_under_it(
    tmp_path, monkeypatch,
):
    """If the RAW row the scan chose no longer matches when the JPEG is
    recorded on it, the JPEG is cataloged as a photo rather than lost."""
    import scanner
    from db import Database

    card = tmp_path / "card"
    _shoot_pairs(card, ["IMG_001"])
    db = Database(str(tmp_path / "test.db"))
    real_rows = scanner._pairing_rows_for_stem

    def stale_owner(db_, folder_id, stem):
        rows = real_rows(db_, folder_id, stem)
        for row in rows:
            row["filename"] = "renamed meanwhile.cr3"
        return rows

    monkeypatch.setattr(scanner, "_pairing_rows_for_stem", stale_owner)
    # Keep the end-of-scan pass from pairing the JPEG photo afterwards, so
    # the fallback itself is what the catalog shows.
    monkeypatch.setattr(scanner, "_pair_raw_jpeg_companions", lambda *a, **k: {})

    scanner.scan(str(card), db)

    assert sorted(_photo_ids_by_filename(db)) == ["IMG_001.cr3", "IMG_001.jpg"]
    assert db.conn.execute(
        "SELECT COUNT(*) FROM companion_identities").fetchone()[0] == 0
    assert db.conn.execute(
        "SELECT COUNT(*) FROM photos WHERE companion_path IS NOT NULL"
    ).fetchone()[0] == 0


def test_attach_companion_commits_before_firing_callback(tmp_path):
    """A thumbnail worker on another connection must see ``companion_path``
    by the time the attach callback fires: the thumbnail stage keys queue
    entries on the owner's canonical path or its companion path, and the
    pre-generation ``_still_owns`` check runs against committed state.
    When the callback fired inside ``_commits_held``, the fresh-connection
    read saw ``companion_path`` IS NULL and the queue entry was dropped.
    """
    from db import Database
    from scanner import scan

    card = tmp_path / "card"
    _shoot_pairs(card, ["IMG_001"])
    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    jpeg_path = str(card / "IMG_001.jpg")
    seen_companion_path = []

    def on_photo(photo_id, path):
        if path != jpeg_path:
            return
        # Open a separate SQLite connection — a thumbnail worker runs on
        # its own connection and only sees committed state. If the attach
        # fired its callback inside the ``_commits_held`` guard, the
        # ``companion_path`` UPDATE would not yet be visible here.
        with sqlite3.connect(db_path) as other:
            other.row_factory = sqlite3.Row
            row = other.execute(
                "SELECT companion_path FROM photos WHERE id = ?", (photo_id,),
            ).fetchone()
            seen_companion_path.append(row["companion_path"] if row else None)

    scan(str(card), db, photo_callback=on_photo)

    assert seen_companion_path == ["IMG_001.jpg"], seen_companion_path


def test_add_photo_losing_a_race_keeps_concurrent_collection_entry(
    tmp_path, monkeypatch,
):
    """When ``add_photo``'s INSERT OR IGNORE loses to a concurrent writer,
    the id it returns is the winner's — not ours. The pre-check SELECT
    cannot see the concurrent insert, so the row looks new to the scanner.
    The stale-id cleanup must branch on the actual INSERT result instead,
    or it would strip the winner's collection entry for that id.
    """
    import scanner as scanner_mod
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    folder = tmp_path / "folder"
    folder.mkdir()
    Image.new("RGB", (32, 32), "red").save(str(folder / "heron.jpg"))

    real_add_photo = Database.add_photo
    raced_photo_ids = []

    def racing_add_photo(self, *args, **kwargs):
        if not raced_photo_ids and kwargs.get("filename") == "heron.jpg":
            # Simulate a concurrent writer: insert the row AND populate a
            # collection with its id before our INSERT OR IGNORE fires.
            cur = self.conn.execute(
                "INSERT INTO photos (folder_id, filename, extension,"
                " file_size, file_mtime)"
                " VALUES (?, ?, ?, ?, ?)",
                (kwargs["folder_id"], kwargs["filename"],
                 kwargs.get("extension", ".jpg"),
                 kwargs.get("file_size", 0),
                 kwargs.get("file_mtime", 0.0)),
            )
            self.conn.commit()
            raced_photo_ids.append(cur.lastrowid)
            self.add_collection(
                "Concurrent",
                json.dumps(
                    [{"field": "photo_ids", "value": [cur.lastrowid]}],
                ),
            )
        return real_add_photo(self, *args, **kwargs)

    monkeypatch.setattr(Database, "add_photo", racing_add_photo)
    scanner_mod.scan(str(folder), db)

    assert raced_photo_ids, "racing_add_photo never fired"
    coll_id = db.conn.execute(
        "SELECT id FROM collections WHERE name = ?", ("Concurrent",),
    ).fetchone()["id"]
    assert _collection_photo_ids(db, coll_id) == raced_photo_ids


def test_photos_repository_add_reports_whether_it_inserted(tmp_path):
    """``PhotoRepository.add`` returns ``(photo_id, inserted)``; the second
    call for the same (folder, filename) is a no-op INSERT OR IGNORE and
    reports ``inserted=False`` with the original id.
    """
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    folder_id = db.add_folder(str(tmp_path / "shoot"))

    repo = db._photos_repository(scoped=False)
    first_id, first_inserted = repo.add(
        folder_id=folder_id, filename="owl.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    assert first_inserted is True

    second_id, second_inserted = repo.add(
        folder_id=folder_id, filename="owl.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    assert second_id == first_id
    assert second_inserted is False


def test_database_add_photo_returning_inserted(tmp_path):
    """``Database.add_photo(return_inserted=True)`` surfaces the signal and
    the plain ``add_photo`` still returns just the id.
    """
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    folder_id = db.add_folder(str(tmp_path / "shoot"))

    first = db.add_photo(
        folder_id=folder_id, filename="owl.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    assert isinstance(first, int)

    second_id, second_inserted = db.add_photo(
        folder_id=folder_id, filename="owl.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
        return_inserted=True,
    )
    assert second_id == first
    assert second_inserted is False

    third_id, third_inserted = db.add_photo(
        folder_id=folder_id, filename="falcon.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
        return_inserted=True,
    )
    assert third_id != first
    assert third_inserted is True
