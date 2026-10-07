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


@pytest.mark.parametrize("already_cataloged", [False, True])
def test_pair_survivor_does_not_add_a_second_snapshot_outcome(tmp_path, already_cataloged):
    from types import SimpleNamespace

    import scanner
    from db import Database
    from services.import_in_place import _InPlaceImportRun

    db = Database(str(tmp_path / "test.db"))
    folder_id = db.add_folder(str(tmp_path))
    raw_id = db.add_photo(folder_id, "PAIR.cr3", ".cr3", 256, None)
    jpeg_id = db.add_photo(folder_id, "PAIR.jpg", ".jpg", 256, None)
    jpeg_path = str(tmp_path / "PAIR.jpg")
    imported = _InPlaceImportRun.__new__(_InPlaceImportRun)
    imported.photo_ids, imported.seen_photo_ids, imported.indexed_paths = [], set(), set()
    imported.runner = SimpleNamespace(update_step=lambda *a, **k: None)
    imported.job, imported.plan = {"id": "snapshot-import"}, SimpleNamespace(source_snapshot_id="frozen")
    imported.snapshot_known_before = {jpeg_path: jpeg_id} if already_cataloged else {}
    imported.snapshot_requested = 1
    imported.snapshot_missing, imported.snapshot_unreadable = [], []
    try:
        imported._photo_cb(jpeg_id, jpeg_path)
        merged = scanner._pair_raw_jpeg_companions(db)
        assert merged == {jpeg_id: raw_id}
        scanner._ScanRun._report_merged_photos(SimpleNamespace(
            db=db, photo_merged_callback=imported._photo_merged_cb,
            _reported_identities={jpeg_id: (folder_id, "PAIR.jpg")},
        ), merged)
        result = imported._snapshot_result_fields([])
        assert result["imported"] == (0 if already_cataloged else 1)
        assert result["already_cataloged"] == (1 if already_cataloged else 0)
        assert result["imported"] + result["already_cataloged"] == result["requested"]
        assert imported.photo_ids == [raw_id]
        assert imported.seen_photo_ids == {raw_id}
        assert imported.indexed_paths == {jpeg_path}
    finally:
        db.close()


def test_later_scan_root_cannot_substitute_an_unrelated_reused_id(tmp_path):
    """A receiver spans roots even though each root creates its own ScanRun."""
    from types import SimpleNamespace

    import scanner
    from db import Database
    from pipeline_stages.scanning import _ScanPass

    first, unrelated, second = (tmp_path / name for name in ("first", "unrelated", "second"))
    for folder in (first, unrelated, second):
        folder.mkdir()
    original, other = first / "ORIGINAL.jpg", second / "SECOND.jpg"
    for path in (original, other, unrelated / "OTHER.jpg"):
        Image.new("RGB", (40, 30), (30, 90, 180)).save(path)
    (unrelated / "OTHER.cr3").write_bytes(b"cataloged RAW placeholder")
    db = Database(str(tmp_path / "test.db"))
    writer = Database(str(tmp_path / "test.db"))
    first_id, unrelated_id = db.add_folder(str(first)), db.add_folder(str(unrelated))
    old_id = db.add_photo(first_id, original.name, ".jpg", original.stat().st_size, original.stat().st_mtime)
    collected = []
    run = SimpleNamespace(
        stages={"scan": {"count": 0}}, job={"id": "multi-root"},
        runner=SimpleNamespace(update_step=lambda *a, **k: None),
        control=SimpleNamespace(should_abort=lambda _a: False, cancellation_requested=lambda: False),
        abort=SimpleNamespace(set=lambda: None, is_set=lambda: False),
    )
    stage = _ScanPass(
        run, sentinel=object(), filter_excluded=None,
        find_broken_metadata_folders=None, missing_archive_mount_root=None,
        put_scan_item=lambda item: None, collected_photo_ids=collected,
        effective_thumb_cache_dir=None, effective_vireo_dir=None,
        final_destination=None, missing_originals_invalidator=None,
        remote_archive=None, skip_scan=False, snapshot_paths=None,
    )
    stage.thread_db, stage.do_scan = db, scanner.scan
    stage.pipeline_cfg = {"extract_full_metadata": False}
    stage._on_scan_progress = lambda *a: None
    stage._on_scan_status = lambda *a, **k: None
    try:
        stage._scan(
            str(first), discovered_files=[original], skip_working_copies=True,
            photo_callback=stage._on_scanned_photo, photo_merged_callback=stage._on_merged_photo,
        )
        assert collected == [old_id]
        writer.delete_photos([old_id])
        assert writer.add_photo(unrelated_id, "OTHER.jpg", ".jpg", 256, None) == old_id
        raw_id = writer.add_photo(unrelated_id, "OTHER.cr3", ".cr3", 256, None)
        stage._scan(
            str(second), discovered_files=[other], skip_working_copies=True,
            photo_callback=stage._on_scanned_photo, photo_merged_callback=stage._on_merged_photo,
        )
        second_id = db.conn.execute("SELECT id FROM photos WHERE filename = 'SECOND.jpg'").fetchone()[0]
        assert raw_id not in collected
        assert collected == [second_id]
        assert run.stages["scan"]["count"] == 1
    finally:
        writer.close()
        db.close()


@pytest.mark.parametrize("reuse_id", [False, True])
def test_merged_owner_deleted_before_publication_drops_import_membership(
    tmp_path, reuse_id,
):
    from types import SimpleNamespace

    import scanner
    from db import Database
    from pipeline_stages.scanning import _ScanPass
    from services.import_in_place import _InPlaceImportRun

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    writer = Database(db_path)
    folder_id = db.add_folder(str(tmp_path))
    raw_id = db.add_photo(folder_id, "IMG_001.cr3", ".cr3", 256, None)
    jpeg_id = db.add_photo(folder_id, "IMG_001.jpg", ".jpg", 256, None)
    merged = scanner._pair_raw_jpeg_companions(db)
    assert merged == {jpeg_id: raw_id}
    writer.delete_photos([raw_id])
    if reuse_id:
        assert writer.add_photo(
            folder_id, "unrelated.jpg", ".jpg", 256, None,
        ) == raw_id
    pipeline = SimpleNamespace(
        _collected_ids={jpeg_id}, _reported_photo_ids={jpeg_id},
        collected_photo_ids=[jpeg_id],
        run=SimpleNamespace(stages={"scan": {"count": 1}}),
    )
    imported = SimpleNamespace(
        seen_photo_ids={jpeg_id}, photo_ids=[jpeg_id],
    )
    published = []

    def report(old_id, new_id, path):
        published.append((old_id, new_id, path))
        _ScanPass._on_merged_photo(pipeline, old_id, new_id, path)
        _InPlaceImportRun._photo_merged_cb(imported, old_id, new_id, path)

    run = SimpleNamespace(db=db, photo_merged_callback=report)
    try:
        scanner._ScanRun._report_merged_photos(run, merged)
        assert published == [(jpeg_id, None, None)]
        assert pipeline.collected_photo_ids == []
        assert pipeline.run.stages["scan"]["count"] == 0
        assert imported.photo_ids == []
        assert imported.seen_photo_ids == set()
    finally:
        writer.close()
        db.close()


def test_reused_old_id_drops_import_membership_instead_of_substituting(
    tmp_path,
):
    """Scanner reported ``old_id`` for one JPEG, then another connection
    freed and reused that id for an unrelated JPEG that pairing merged
    into a DIFFERENT RAW. Substituting the pairing's RAW for ``old_id``
    would add an unrelated RAW to the receiver's collection, so publish
    must detect the identity mismatch and drop ``old_id`` instead.
    """
    from types import SimpleNamespace

    import scanner
    from db import Database
    from pipeline_stages.scanning import _ScanPass
    from services.import_in_place import _InPlaceImportRun

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    writer = Database(db_path)
    folder_id = db.add_folder(str(tmp_path))
    # Seed the unrelated RAW first so the original JPEG claims the
    # table's current max id; deleting it then frees the max rowid,
    # which SQLite (no AUTOINCREMENT) hands to the very next insert.
    unrelated_raw_id = writer.add_photo(
        folder_id, "OTHER.cr3", ".cr3", 256, None,
    )
    original_jpeg_folder = folder_id
    original_jpeg_filename = "ORIG.jpg"
    original_jpeg_id = db.add_photo(
        original_jpeg_folder, original_jpeg_filename, ".jpg", 256, None,
    )
    writer.delete_photos([original_jpeg_id])
    reused_jpeg_id = writer.add_photo(
        folder_id, "OTHER.jpg", ".jpg", 256, None,
    )
    assert reused_jpeg_id == original_jpeg_id
    merged = scanner._pair_raw_jpeg_companions(db)
    assert merged == {reused_jpeg_id: unrelated_raw_id}

    substitutions = []
    pipeline = SimpleNamespace(
        _collected_ids={original_jpeg_id},
        _reported_photo_ids={original_jpeg_id},
        collected_photo_ids=[original_jpeg_id],
        run=SimpleNamespace(stages={"scan": {"count": 1}}),
        _on_scanned_photo=lambda pid, p: substitutions.append((pid, p)),
    )
    imported = SimpleNamespace(
        seen_photo_ids={original_jpeg_id},
        photo_ids=[original_jpeg_id],
        _photo_cb=lambda pid, p: substitutions.append((pid, p)),
    )
    published = []

    def report(old_id, new_id, path):
        published.append((old_id, new_id, path))
        _ScanPass._on_merged_photo(pipeline, old_id, new_id, path)
        _InPlaceImportRun._photo_merged_cb(imported, old_id, new_id, path)

    run = SimpleNamespace(
        db=db,
        photo_merged_callback=report,
        _reported_identities={
            original_jpeg_id: (
                original_jpeg_folder, original_jpeg_filename,
            ),
        },
    )
    try:
        scanner._ScanRun._report_merged_photos(run, merged)
        # Mismatch between what the scanner reported for this id and
        # what pairing merged → drop old_id, do not substitute. If the
        # fix regresses, the publish re-adds ``unrelated_raw_id`` to
        # the receivers via their substitution callbacks.
        assert published == [(original_jpeg_id, None, None)]
        assert substitutions == []
        assert pipeline.collected_photo_ids == []
        assert pipeline.run.stages["scan"]["count"] == 0
        assert imported.photo_ids == []
        assert imported.seen_photo_ids == set()
    finally:
        writer.close()
        db.close()


def test_merged_owner_is_committed_and_cannot_be_deleted_during_publication(
    tmp_path,
):
    from types import SimpleNamespace

    import scanner
    from db import Database

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    writer = Database(db_path)
    writer.conn.execute("PRAGMA busy_timeout=0")
    folder_id = db.add_folder(str(tmp_path))
    raw_id = db.add_photo(folder_id, "IMG_001.cr3", ".cr3", 256, None)
    jpeg_id = db.add_photo(folder_id, "IMG_001.jpg", ".jpg", 256, None)
    merged = scanner._pair_raw_jpeg_companions(db)
    published = []

    def report(old_id, new_id, path):
        assert writer.get_photo(new_id)["companion_path"] == "IMG_001.jpg"
        with pytest.raises(sqlite3.OperationalError, match="locked"):
            writer.conn.execute("DELETE FROM photos WHERE id=?", (new_id,))
        writer.conn.rollback()
        published.append((old_id, new_id, path))

    run = SimpleNamespace(db=db, photo_merged_callback=report)
    try:
        scanner._ScanRun._report_merged_photos(run, merged)
        assert published == [(jpeg_id, raw_id, str(tmp_path / "IMG_001.cr3"))]
        writer.delete_photos([raw_id])
        assert db.get_photo(raw_id) is None
    finally:
        writer.close()
        db.close()


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


def test_pipeline_scan_summary_counts_photos_not_files(tmp_path, monkeypatch):
    """"N photos" in the scan step summary must match the collection.

    ``_on_scan_progress`` writes the file-progress count into
    ``stages["scan"]["count"]``, and rowless-companion attachment produces no
    later merge callback to correct it. The final summary must therefore come
    from ``collected_photo_ids`` (what the pipeline runs on), not from the
    file-walked tally — otherwise a RAW+JPEG card reports twice its photo
    count."""
    from db import Database
    from pipeline_job import PipelineParams, run_pipeline_job
    from test_pipeline_job import FakeRunner, _make_job

    _isolate_config(tmp_path, monkeypatch)
    card = tmp_path / "card"
    _shoot_pairs(card, ["IMG_001", "IMG_002", "IMG_003"])
    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    runner = FakeRunner()
    params = PipelineParams(
        source=str(card),
        skip_classify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )
    result = run_pipeline_job(_make_job(), runner, db_path, ws_id, params)

    members = _collection_photo_ids(db, result["collection_id"])
    assert len(members) == 3  # three RAWs, each JPEG attached rowlessly

    scan_completed = [
        kw for (_jid, sid, kw) in runner.step_updates
        if sid == "scan" and kw.get("status") == "completed"
    ]
    assert scan_completed, runner.step_updates
    summary = scan_completed[-1].get("summary", "")
    leading = summary.split()[0] if summary else ""
    assert leading == str(len(members)), (
        f"scan summary {summary!r} must count the photos the pipeline runs "
        f"on ({len(members)}), not the files walked (6)"
    )


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


def test_new_photo_preserves_collection_created_after_insert(
    tmp_path, monkeypatch,
):
    """A separate writer may name a new photo once its insert is visible."""
    from db import Database
    from scanner import scan

    card = tmp_path / "card"
    card.mkdir()
    Image.new("RGB", (32, 32), "green").save(card / "bird.jpg")
    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    writer = Database(db_path)
    writer.conn.execute("PRAGMA busy_timeout=0")
    real_add = db.add_photo
    state = {}

    def add_membership(photo_id):
        return writer.add_collection(
            "User selected bird",
            json.dumps([{"field": "photo_ids", "value": [photo_id]}]),
        )

    def insert_then_user_selection(*args, **kwargs):
        result = real_add(*args, **kwargs)
        photo_id, inserted = result
        assert inserted
        state["photo_id"] = photo_id
        try:
            state["collection_id"] = add_membership(photo_id)
        except sqlite3.OperationalError as error:
            assert "locked" in str(error).lower()
            writer.conn.rollback()
        return result

    monkeypatch.setattr(db, "add_photo", insert_then_user_selection)
    try:
        scan(str(card), db)
        if "collection_id" not in state:
            state["collection_id"] = add_membership(state["photo_id"])
        assert _photo_ids_by_filename(db)["bird.jpg"] == state["photo_id"]
        assert _collection_photo_ids(writer, state["collection_id"]) == [
            state["photo_id"],
        ]
    finally:
        writer.close()
        db.close()


def test_new_photo_rechecks_collections_between_scan_inserts(tmp_path):
    """A collection write between a scan's inserts for a currently free id.

    The scan's per-insert check used to read the named-id set once, on
    the first insert. If another connection then saved a collection
    naming an id a later insert reused, that cache missed the entry and
    the new photo silently joined the stale collection. The repair at
    startup cannot catch this: the id then names a valid row. Fixing
    the cache means re-reading on every insert, so the check sees any
    collection added since the previous insert.
    """
    from db import Database
    from scanner import scan

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    card = tmp_path / "card"
    card.mkdir()
    # The scanner inserts both files in one run. The first insert loads
    # the named-id cache (empty); the external write between the two
    # inserts names the id the second insert will take.
    Image.new("RGB", (32, 32), "red").save(card / "first.jpg")
    Image.new("RGB", (32, 32), "blue").save(card / "second.jpg")

    writer = Database(db_path)
    writer.conn.execute("PRAGMA busy_timeout=5000")
    state = {"collection_id": None, "inserts_seen": 0}

    def after_each_insert(photo_id, path):
        state["inserts_seen"] += 1
        if state["inserts_seen"] == 1:
            # Simulate a user saving a collection from stale UI state
            # between the scan's inserts. The id (next in sequence) is
            # currently free; the scanner's writer lock has been released
            # for this item, so the external write lands here.
            state["collection_id"] = writer.add_collection(
                "Stale pick",
                json.dumps(
                    [{"field": "photo_ids", "value": [photo_id + 1]}],
                ),
            )

    try:
        scan(str(card), db, photo_callback=after_each_insert)
        ids = _photo_ids_by_filename(db)
        second_id = ids["second.jpg"]
        assert state["collection_id"] is not None
        assert second_id == ids["first.jpg"] + 1
        # The second insert's id must NOT have inherited the stale
        # collection entry: the pre-fix scanner silently left it in
        # because the named-id set was cached once on the first insert.
        assert _collection_photo_ids(writer, state["collection_id"]) == []
    finally:
        writer.close()
        db.close()


def test_drop_inherited_collection_membership_reuses_cache_across_inserts(
    tmp_path, monkeypatch,
):
    """The cache is cheap when no other connection has written.

    Reading ``photo_ids_named_by_collections`` on every insert made
    imports O(new photos * saved photo ids) for catalogs with large
    static collections. The cache is invalidated through SQLite's
    ``PRAGMA data_version`` -- our own writes don't bump it, so a run
    of inserts with no concurrent writer rebuilds it exactly once.
    """
    import repositories.collections as collections_module
    from db import Database
    from scanner import scan

    card = tmp_path / "card"
    card.mkdir()
    # Three photos: enough that a per-insert cache would show up as > 1
    # read, and few enough that the test stays quick.
    for name in ("a.jpg", "b.jpg", "c.jpg"):
        Image.new("RGB", (32, 32), "red").save(card / name)

    db = Database(str(tmp_path / "test.db"))
    real = collections_module.photo_ids_named_by_collections
    calls = {"n": 0}

    def counting(conn):
        calls["n"] += 1
        return real(conn)

    monkeypatch.setattr(
        collections_module, "photo_ids_named_by_collections", counting,
    )

    try:
        scan(str(card), db)
        assert len(_photo_ids_by_filename(db)) == 3
        # Three inserts share one named-id read: no concurrent writer
        # bumped the data_version.
        assert calls["n"] == 1
    finally:
        db.close()


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


def test_attach_companion_invalidates_display_cache_before_firing_callback(
    tmp_path,
):
    """A ``photo_callback`` that raises after the attach commit must still
    leave the RAW's display cache invalidated.

    The attach commits ``companion_path``, so the next incremental scan
    takes ``_finish_known_companion`` for an unchanged JPEG — and that
    path does not re-run ``_defer_primary_display_cache_invalidation``.
    If the first-pairing invalidation ran after the external callback, a
    callback raise would strand a pre-pairing render as the RAW's cache
    forever. Running the cache invalidation first closes that window.
    """
    from db import Database
    from scanner import scan

    card = tmp_path / "card"
    _shoot_pairs(card, ["IMG_001"])
    vireo_dir = tmp_path / "vireo"
    thumbs = tmp_path / "thumbs"
    (vireo_dir / "originals").mkdir(parents=True)
    thumbs.mkdir()
    db = Database(str(tmp_path / "test.db"))
    jpeg_path = str(card / "IMG_001.jpg")
    stale_display_paths = []

    def on_photo(photo_id, path):
        if path != jpeg_path:
            # First call: RAW is reported. Plant the stale display cache
            # that the attach must invalidate.
            display = vireo_dir / "originals" / f"{photo_id}.display.jpg"
            display.write_bytes(b"stale pre-pairing render")
            stale_display_paths.append(display)
            return
        # Second call: companion attach after commit. Raise to prove the
        # cache invalidation already happened.
        raise RuntimeError("callback failure after companion commit")

    with pytest.raises(RuntimeError, match="callback failure"):
        scan(
            str(card), db,
            photo_callback=on_photo,
            vireo_dir=str(vireo_dir),
            thumb_cache_dir=str(thumbs),
        )

    assert stale_display_paths, "RAW photo_callback never fired"
    display_path = stale_display_paths[0]
    assert not display_path.exists(), (
        f"stale display cache survived callback failure: {display_path}"
    )
    # The attach commit itself still landed: ``companion_path`` is set.
    raw_id = int(display_path.stem.split(".")[0])
    row = db.conn.execute(
        "SELECT companion_path FROM photos WHERE id = ?", (raw_id,),
    ).fetchone()
    assert row["companion_path"] == "IMG_001.jpg"


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


def test_attach_companion_refuses_when_owner_metadata_changed_before_commit(
    tmp_path, monkeypatch,
):
    """``_resolve_companion_group`` reads the RAW's capture metadata
    (timestamp, camera_make, camera_model) and ``_pick_compatible_raw_jpeg_pairs``
    decides compatibility from those values. Between that read and the
    guarded UPDATE inside ``_attach_companion``, another writer can
    rewrite the RAW's metadata — the identity predicates on the UPDATE
    (id, folder_id, filename, companion_path) do not catch that change,
    so without a compatibility recheck under the writer lock, the pair
    would commit on stale evidence and permanently attach an unrelated
    JPEG to a different-exposure RAW. Attach must refuse in that race.
    """
    import scanner as scanner_mod
    from db import Database

    card = tmp_path / "card"
    _shoot_pairs(card, ["IMG_001"])
    db_path = str(tmp_path / "test.db")
    db = Database(db_path)

    jpeg_path = str(card / "IMG_001.jpg")
    raw_path = str(card / "IMG_001.cr3")

    # Pretend ExifTool read a Nikon camera off the JPEG and nothing off
    # the RAW. ``_pick_compatible_raw_jpeg_pairs`` then runs with the
    # RAW's capture columns all None (one-side-unknown → compatible), so
    # the pair is returned. The race below rewrites the RAW's camera on
    # a separate connection just before the attach's guarded UPDATE,
    # making the pair incompatible by the time it commits.
    def fake_extract(paths, progress_callback=None, checkpoint=None, **_kw):
        meta = {}
        for p in paths:
            if p.endswith(".jpg"):
                meta[p] = {
                    "EXIF": {"Make": "Nikon", "Model": "Z9"},
                    "File": {},
                    "Composite": {},
                }
            else:
                meta[p] = {}
        return meta

    monkeypatch.setattr(scanner_mod, "extract_metadata", fake_extract)

    real_pick = scanner_mod._pick_compatible_raw_jpeg_pairs

    def racing_pick(members, log_conflicts=True):
        pairs = real_pick(members, log_conflicts=log_conflicts)
        # Simulate a concurrent writer rewriting the RAW's capture
        # metadata between pair decision and attach so the compat
        # decision we return no longer holds under the writer lock.
        for raw, jpeg in pairs:
            if raw["id"] is None or jpeg["id"] is not None:
                continue
            with sqlite3.connect(db_path) as other:
                other.execute(
                    "UPDATE photos SET camera_make = ?, camera_model = ?"
                    " WHERE id = ?",
                    ("Canon", "EOS R5", raw["id"]),
                )
                other.commit()
        return pairs

    monkeypatch.setattr(
        scanner_mod, "_pick_compatible_raw_jpeg_pairs", racing_pick,
    )
    # Keep the end-of-scan pairing pass from re-pairing after the attach
    # refused — the test's assertion is about the attach itself. (It
    # would also see Canon-vs-Nikon as incompatible and still refuse.)
    monkeypatch.setattr(
        scanner_mod, "_pair_raw_jpeg_companions", lambda *a, **k: {},
    )

    callbacks = []

    def on_photo(photo_id, path):
        callbacks.append((photo_id, path))

    scanner_mod.scan(str(card), db, photo_callback=on_photo)

    # The attach must refuse because the RAW's metadata diverged from
    # the JPEG's under the writer lock. The JPEG then falls through to
    # being cataloged as its own photo.
    photos = _photo_ids_by_filename(db)
    assert set(photos) == {"IMG_001.cr3", "IMG_001.jpg"}, photos
    # No companion attachment landed.
    assert db.conn.execute(
        "SELECT COUNT(*) FROM companion_identities"
    ).fetchone()[0] == 0
    assert db.conn.execute(
        "SELECT COUNT(*) FROM photos WHERE companion_path IS NOT NULL"
    ).fetchone()[0] == 0
    # The JPEG was published as its own photo (both files reported
    # under their own ids), not merged into the RAW's id.
    paths_in_callbacks = {c[1] for c in callbacks}
    assert paths_in_callbacks == {raw_path, jpeg_path}, callbacks
    for pid, path in callbacks:
        assert pid == photos[os.path.basename(path)], (pid, path, photos)


def test_attach_companion_skips_publishing_when_owner_vanished_before_callback(
    tmp_path, monkeypatch,
):
    """``_attach_companion`` publishes the RAW's id to ``photo_callback``
    AFTER ``_commits_held`` releases, so the thumbnail worker on another
    connection sees ``companion_path``. In that window another connection
    can delete the RAW and SQLite can reuse its rowid for an unrelated
    photo; without ownership revalidation, the callback would publish
    that stale id into the pipeline or import collection — a dangling id
    (or one that now names a different file) that no end-of-scan merge
    callback can correct, because the JPEG never had a row of its own.
    """
    import scanner as scanner_mod
    from db import Database

    card = tmp_path / "card"
    _shoot_pairs(card, ["IMG_001"])
    vireo_dir = tmp_path / "vireo"
    (vireo_dir / "originals").mkdir(parents=True)
    thumbs = tmp_path / "thumbs"
    thumbs.mkdir()
    db_path = str(tmp_path / "test.db")
    db = Database(db_path)

    real_fs_actions = scanner_mod._run_post_commit_fs_actions

    def racing_fs_actions(actions):
        real_fs_actions(actions)
        # The attach commit (and its ``_commits_held``) have landed; the
        # callback has not fired yet. Replay the exact deletion-then-reuse
        # race on a separate real connection.
        with sqlite3.connect(db_path) as other:
            other.execute("PRAGMA foreign_keys = ON")
            other.row_factory = sqlite3.Row
            row = other.execute(
                "SELECT p.id FROM photos p JOIN folders f ON f.id = p.folder_id"
                " WHERE p.filename = ? AND f.path = ?",
                ("IMG_001.cr3", str(card)),
            ).fetchone()
            if row is None:
                return
            raw_id = row["id"]
            other.execute("DELETE FROM photos WHERE id = ?", (raw_id,))
            # Insert an unrelated photo into a different folder that reuses
            # the freed rowid. ``photos`` has no AUTOINCREMENT, so an
            # explicit id in the INSERT re-claims it.
            unrelated = tmp_path / "unrelated"
            unrelated.mkdir(exist_ok=True)
            folder_id = other.execute(
                "INSERT INTO folders (path) VALUES (?)",
                (str(unrelated),),
            ).lastrowid
            other.execute(
                "INSERT INTO photos (id, folder_id, filename, extension,"
                " file_size, file_mtime) VALUES (?, ?, ?, ?, ?, ?)",
                (raw_id, folder_id, "OTHER.jpg", ".jpg", 0, 0.0),
            )
            other.commit()

    monkeypatch.setattr(
        scanner_mod, "_run_post_commit_fs_actions", racing_fs_actions,
    )
    # Keep the end-of-scan pairing pass out: the attach already fired for
    # the JPEG; the assertion is about the publication for THAT attach.
    monkeypatch.setattr(
        scanner_mod, "_pair_raw_jpeg_companions", lambda *a, **k: {},
    )

    jpeg_path = str(card / "IMG_001.jpg")
    callbacks = []

    def on_photo(photo_id, path):
        callbacks.append((photo_id, path))

    scanner_mod.scan(
        str(card), db,
        photo_callback=on_photo,
        vireo_dir=str(vireo_dir),
        thumb_cache_dir=str(thumbs),
    )

    # The JPEG's attach fired after the RAW was deleted and its id reused
    # for OTHER.jpg. The publication must NOT have called photo_callback
    # with (reused_id, companion_path) — that would adopt an unrelated row
    # into the collection built from these callbacks.
    jpeg_callbacks = [c for c in callbacks if c[1] == jpeg_path]
    assert jpeg_callbacks == [], (
        f"attach callback leaked a stale/reused id for the JPEG: {jpeg_callbacks}"
    )


def test_attach_companion_keeps_owner_locked_during_publication(tmp_path):
    """A collection callback must never observe a recycled attachment ID."""
    import scanner as scanner_mod
    from db import Database

    card = tmp_path / "card"
    _shoot_pairs(card, ["IMG_001"])
    raw = card / "IMG_001.cr3"
    jpeg = card / "IMG_001.jpg"
    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    writer = Database(db_path)
    try:
        # Catalog only the RAW, then scan only its newly discovered JPEG.
        scanner_mod.scan(
            str(card), db, discovered_files=[raw], incremental=False,
            skip_working_copies=True,
        )
        raw_id = _photo_ids_by_filename(db)[raw.name]
        other = tmp_path / "other"
        other.mkdir()
        other_folder = writer.add_folder(str(other))
        writer.conn.execute("PRAGMA busy_timeout = 0")
        published = []
        blocked = []

        def publish(photo_id, path):
            # A real second connection tries to replace the owner while
            # the receiver is consuming its ID. It must remain locked,
            # even though the earlier attachment commit is already visible.
            try:
                writer.delete_photos([raw_id])
            except sqlite3.OperationalError as error:
                assert "locked" in str(error)
                writer.conn.rollback()
                blocked.append(True)
            else:
                replacement = writer.add_photo(
                    other_folder, "OTHER.jpg", ".jpg", 0, None,
                )
                assert replacement == raw_id
            row = db.get_photo(photo_id)
            assert row["filename"] == raw.name
            assert row["companion_path"] == jpeg.name
            published.append((photo_id, path))

        scanner_mod.scan(
            str(card), db, discovered_files=[jpeg], incremental=False,
            skip_working_copies=True, photo_callback=publish,
        )
        assert published == [(raw_id, str(jpeg))]
        assert blocked == [True]
        # The publication guard must release when the scan finishes.
        writer.delete_photos([raw_id])
        assert writer.add_photo(
            other_folder, "OTHER.jpg", ".jpg", 0, None,
        ) == raw_id
    finally:
        writer.close()
        db.close()


@pytest.mark.parametrize("wait_kind", ["backpressure", "pause", "owner_replaced"])
def test_attach_companion_pipeline_wait_does_not_hold_writer(tmp_path, wait_kind):
    """The real pipeline publication waits before acquiring SQLite's lock."""
    import queue
    import threading
    from types import SimpleNamespace

    import scanner as scanner_mod
    from db import Database
    from pipeline_job import _StageState
    from pipeline_stages.scanning import _ScannedPhotoCallback, _ScanPass

    card = tmp_path / "card"
    _shoot_pairs(card, ["IMG_001"])
    raw, jpeg = card / "IMG_001.cr3", card / "IMG_001.jpg"
    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    scanner_mod.scan(str(card), db, discovered_files=[raw], incremental=False,
                     skip_working_copies=True)
    raw_id = _photo_ids_by_filename(db)[raw.name]
    db.close()
    waiting, resume = threading.Event(), threading.Event()
    abort = threading.Event()
    errors, collected = [], []

    def should_abort(event):
        waiting.set()
        if wait_kind == "pause":
            assert resume.wait(10)
        return event.is_set()

    shared = _StageState(SimpleNamespace(), SimpleNamespace(
        abort=abort, should_abort=should_abort,
    ))
    shared.scan_to_thumb = queue.Queue(maxsize=1)
    if wait_kind != "pause":
        shared.scan_to_thumb.put(("prior", "prior.jpg"))

    def producer():
        thread_db = Database(db_path)
        try:
            receiver = _ScanPass.__new__(_ScanPass)
            receiver._reported_photo_ids = set()
            receiver._collected_ids = set()
            receiver.collected_photo_ids = collected
            receiver._canonical_photo_path = lambda pid, path: str(raw)
            receiver.prepare_scan_item = shared.prepare_scan_item
            receiver.put_scan_item = shared.put_scan_item
            receiver.run = SimpleNamespace(
                stages={"scan": {}}, job={"id": "test"},
                runner=SimpleNamespace(update_step=lambda *a, **kw: None),
            )
            scanner_mod.scan(
                str(card), thread_db, discovered_files=[jpeg],
                incremental=False, skip_working_copies=True,
                photo_callback=_ScannedPhotoCallback(receiver),
            )
        except BaseException as error:
            errors.append(error)
        finally:
            thread_db.close()

    consumer = Database(db_path)
    consumer.conn.execute("PRAGMA busy_timeout = 100")
    thread = threading.Thread(target=producer)
    thread.start()
    assert waiting.wait(10)
    try:
        # This is what the thumbnail consumer must do BEFORE taking the
        # next queued item. A callback under the writer lock deadlocks it.
        consumer.conn.execute("UPDATE photos SET thumb_path=? WHERE id=?",
                              ("prior.jpg", raw_id))
        consumer.conn.commit()
        if wait_kind == "owner_replaced":
            consumer.delete_photos([raw_id])
            folder_id = consumer.add_folder(str(tmp_path / "other"))
            assert consumer.add_photo(folder_id, "OTHER.jpg", ".jpg", 0, None) == raw_id
    finally:
        resume.set()
        if wait_kind != "pause":
            shared.scan_to_thumb.get_nowait()
        thread.join(timeout=10)
        consumer.close()
    assert not thread.is_alive()
    assert errors == []
    if wait_kind == "owner_replaced":
        assert collected == []
        assert shared.scan_to_thumb.empty()
    else:
        assert collected == [raw_id]
        assert shared.scan_to_thumb.get_nowait() == (raw_id, str(raw))


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
