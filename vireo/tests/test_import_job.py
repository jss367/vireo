"""Import job: copy card -> archive with hash verification."""
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from db import Database
from PIL import Image


class FakeRunner:
    """Minimal JobRunner stand-in (mirrors test_pipeline_job.FakeRunner)."""

    def __init__(self):
        self.events = []
        self.step_updates = []
        self.cancelled_ids = set()

    def push_event(self, job_id, event_type, data):
        self.events.append((job_id, event_type, data))

    def set_steps(self, job_id, steps):
        self.steps_defined = list(steps)

    def update_step(self, job_id, step_id, **kwargs):
        self.step_updates.append((job_id, step_id, kwargs))

    def is_cancelled(self, job_id):
        return job_id in self.cancelled_ids


def _make_job(job_id="import-test-1"):
    return {
        "id": job_id,
        "type": "import",
        "status": "running",
        "progress": {"current": 0, "total": 0, "current_file": ""},
        "result": None,
        "errors": [],
        "config": {},
        "workspace_id": 1,
    }


def _make_card(tmp_path, specs, card_name="card"):
    """A fake card with tiny JPEGs. ``specs`` is a list of
    ``(filename, mtime_datetime)`` (or ``(filename, mtime, color)``)
    tuples. Distinct mtimes drive folder planning:
    ingest._source_file_timestamps falls back to file mtime when EXIF is
    absent before build_destination_path formats the destination folder.
    """
    card = tmp_path / card_name
    card.mkdir(exist_ok=True)
    for spec in specs:
        name, mtime, color = spec if len(spec) == 3 else (*spec, "red")
        path = card / name
        Image.new("RGB", (16, 16), color).save(str(path))
        ts = mtime.timestamp()
        os.utime(str(path), (ts, ts))
    return card


def _run_import(tmp_path, params, runner=None, job=None):
    from import_job import run_import_job

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    runner = runner or FakeRunner()
    job = job or _make_job()
    result = run_import_job(job, runner, db_path, ws_id, params)
    return db, ws_id, result


def _photo_rows(db):
    return db.conn.execute(
        """SELECT p.id, p.filename, p.file_hash, p.hash_status,
                  p.hash_checked_at, f.path AS folder_path
           FROM photos p JOIN folders f ON f.id = p.folder_id"""
    ).fetchall()


def _ws_linked_folder_paths(db, ws_id):
    return {
        row["path"]
        for row in db.conn.execute(
            """SELECT f.path FROM folders f
               JOIN workspace_folders wf ON wf.folder_id = f.id
               WHERE wf.workspace_id = ?""",
            (ws_id,),
        )
    }


def test_run_import_job_copies_verifies_and_catalogs(tmp_path):
    from import_dedup import compute_file_hash
    from import_job import ImportParams

    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
        ("DSC_0002.jpg", datetime(2026, 7, 3, 11, 0, 0), "green"),
        ("DSC_0003.jpg", datetime(2026, 7, 4, 9, 0, 0), "blue"),
        ("DSC_0004.jpg", datetime(2026, 7, 4, 9, 5, 0), "white"),
    ])
    archive = tmp_path / "archive"

    db, ws_id, result = _run_import(tmp_path, ImportParams(
        sources=[str(card)], destination=str(archive),
    ))

    # Every discovered file landed at its template path.
    expected = {
        str(archive / "2026" / "2026-07-03" / "DSC_0001.jpg"),
        str(archive / "2026" / "2026-07-03" / "DSC_0002.jpg"),
        str(archive / "2026" / "2026-07-04" / "DSC_0003.jpg"),
        str(archive / "2026" / "2026-07-04" / "DSC_0004.jpg"),
    }
    for path in expected:
        assert os.path.isfile(path), f"missing archive file: {path}"

    # A photo row exists for each copied file at its final path, with the
    # verified hash stamped in the integrity-audit vocabulary.
    rows = _photo_rows(db)
    row_paths = {
        os.path.join(r["folder_path"], r["filename"]) for r in rows
    }
    assert row_paths == expected
    for r in rows:
        full = os.path.join(r["folder_path"], r["filename"])
        assert r["file_hash"] == compute_file_hash(full)
        assert r["hash_status"] == "ok"
        assert r["hash_checked_at"] is not None

    # Result counts are consistent.
    assert result["discovered"] == 4
    assert result["copied"] == 4
    assert result["verified"] == 4
    assert result["skipped_duplicate"] == 0
    assert result["failed"] == 0

    # The date folders are linked to the active workspace.
    linked = _ws_linked_folder_paths(db, ws_id)
    assert str(archive / "2026" / "2026-07-03") in linked
    assert str(archive / "2026" / "2026-07-04") in linked


def test_duplicate_only_import_links_matched_folders(tmp_path):
    """Importing a card of only already-cataloged duplicates must still
    scan + link the matched destination folders to the active workspace,
    even though no fresh files were copied."""
    from import_dedup import compute_file_hash
    from import_job import ImportParams

    # Pre-catalog a photo at the archive destination WITHOUT linking its
    # folder to the active workspace (raw SQL, no workspace_folders rows).
    archive = tmp_path / "archive"
    dest_dir = archive / "2026" / "2026-07-03"
    dest_dir.mkdir(parents=True)
    dest_file = dest_dir / "IMG_0100.jpg"
    Image.new("RGB", (16, 16), "red").save(str(dest_file))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES (?, ?, 'ok')",
        (str(dest_dir), dest_dir.name),
    ).lastrowid
    db.conn.execute(
        "INSERT INTO photos (folder_id, filename, extension, file_size,"
        " file_hash) VALUES (?, ?, '.jpg', ?, ?)",
        (
            fid,
            "IMG_0100.jpg",
            os.path.getsize(str(dest_file)),
            compute_file_hash(str(dest_file)),
        ),
    )
    db.conn.commit()
    assert str(dest_dir) not in _ws_linked_folder_paths(db, ws_id)

    # Card holds a byte-identical copy of the cataloged photo.
    card = tmp_path / "card"
    card.mkdir()
    import shutil
    shutil.copy2(str(dest_file), str(card / "IMG_0100.jpg"))

    runner = FakeRunner()
    job = _make_job()
    from import_job import run_import_job
    result = run_import_job(job, runner, db_path, ws_id, ImportParams(
        sources=[str(card)], destination=str(archive),
    ))

    assert result["copied"] == 0
    assert result["skipped_duplicate"] == 1
    assert result["failed"] == 0
    # The matched folder got scanned and linked despite zero fresh copies.
    assert str(dest_dir) in _ws_linked_folder_paths(db, ws_id)
    # Still exactly one photo row — no re-import of known bytes.
    assert len(_photo_rows(db)) == 1


def test_catalog_never_references_missing_files(tmp_path):
    """Invariant: catalog is a subset of verified on-disk files, even when
    some copies fail."""
    from import_job import ImportParams

    card = _make_card(tmp_path, [
        ("DSC_0010.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
        ("DSC_0011.jpg", datetime(2026, 7, 3, 11, 0, 0), "green"),
    ])
    archive = tmp_path / "archive"

    # Sabotage the second copy: corrupt destination bytes for DSC_0011.
    import shutil as shutil_mod

    import import_job as ij
    real_copy2 = shutil_mod.copy2

    def flaky_copy2(s, d):
        real_copy2(s, d)
        if "DSC_0011" in str(d):
            with open(d, "r+b") as f:
                f.write(b"CORRUPT")

    orig = ij.shutil.copy2
    ij.shutil.copy2 = flaky_copy2
    try:
        db, ws_id, result = _run_import(tmp_path, ImportParams(
            sources=[str(card)], destination=str(archive),
        ))
    finally:
        ij.shutil.copy2 = orig

    assert result["copied"] == 1
    assert result["failed"] == 1
    rows = _photo_rows(db)
    # Only the verified file is cataloged, and it exists on disk.
    assert len(rows) == 1
    for r in rows:
        assert os.path.isfile(os.path.join(r["folder_path"], r["filename"]))
    # A failed copy means the card is NOT safe to format, with the
    # failure named.
    assert result["safe_to_format"] is False
    assert len(result["unsafe_files"]) == 1
    assert "DSC_0011" in result["unsafe_files"][0]["path"]
    assert result["unsafe_files"][0]["reason"]


# --- safe-to-format ledger ----------------------------------------------

def test_fresh_import_is_safe_to_format(tmp_path):
    from import_job import ImportParams

    card = _make_card(tmp_path, [
        ("DSC_0020.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
        ("DSC_0021.jpg", datetime(2026, 7, 3, 11, 0, 0), "green"),
    ])
    _, _, result = _run_import(tmp_path, ImportParams(
        sources=[str(card)], destination=str(tmp_path / "archive"),
    ))
    assert result["safe_to_format"] is True
    assert result["unsafe_files"] == []


def test_hash_backed_duplicate_is_safe_to_format(tmp_path):
    """A byte-identical twin already cataloged means the card file's bytes
    verifiably exist — safe."""
    from import_dedup import compute_file_hash
    from import_job import ImportParams

    archive = tmp_path / "archive"
    dest_dir = archive / "2026" / "2026-07-03"
    dest_dir.mkdir(parents=True)
    dest_file = dest_dir / "IMG_0300.jpg"
    Image.new("RGB", (16, 16), "red").save(str(dest_file))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES (?, ?, 'ok')",
        (str(dest_dir), dest_dir.name),
    ).lastrowid
    db.conn.execute(
        "INSERT INTO photos (folder_id, filename, extension, file_size,"
        " file_hash) VALUES (?, ?, '.jpg', ?, ?)",
        (fid, "IMG_0300.jpg", os.path.getsize(str(dest_file)),
         compute_file_hash(str(dest_file))),
    )
    db.conn.commit()

    card = tmp_path / "card"
    card.mkdir()
    import shutil
    shutil.copy2(str(dest_file), str(card / "IMG_0300.jpg"))

    from import_job import run_import_job
    result = run_import_job(_make_job(), FakeRunner(), db_path, ws_id,
                            ImportParams(sources=[str(card)],
                                         destination=str(archive)))
    assert result["skipped_duplicate"] == 1
    assert result["safe_to_format"] is True


def test_stale_hash_row_without_on_disk_twin_imports_as_fresh(tmp_path):
    """A cataloged ``photos.file_hash`` row whose archive file has been
    deleted since scan must NOT let the card be counted as skipped. The
    hash token matches (card bytes hash to the cataloged value) but no
    on-disk twin backs it — safe_to_format would go green while the card
    is the only remaining copy of the bytes. The card must import as a
    fresh photo instead."""
    from import_dedup import compute_file_hash
    from import_job import ImportParams, run_import_job

    # Card file whose bytes hash to a specific value.
    card = tmp_path / "card"
    card.mkdir()
    card_file = card / "IMG_0500.jpg"
    Image.new("RGB", (16, 16), "red").save(str(card_file))
    ts = datetime(2026, 6, 1, 9, 0, 0).timestamp()
    os.utime(str(card_file), (ts, ts))
    card_hash = compute_file_hash(str(card_file))
    card_size = os.path.getsize(str(card_file))

    # Seeded catalog row: a folder that once held a byte-identical twin,
    # but the archive file is GONE. The folder path exists on disk
    # (folder_status still 'ok') to isolate the missing-file case.
    archive = tmp_path / "archive"
    library = archive / "old-library"
    library.mkdir(parents=True)
    ghost_path = library / "IMG_0500.jpg"
    assert not ghost_path.exists()

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES (?, ?, 'ok')",
        (str(library), "old-library"),
    ).lastrowid
    db.conn.execute(
        "INSERT INTO photos (folder_id, filename, extension, file_size,"
        " file_hash) VALUES (?, ?, '.jpg', ?, ?)",
        (fid, "IMG_0500.jpg", card_size, card_hash),
    )
    db.conn.commit()

    result = run_import_job(
        _make_job(), FakeRunner(), db_path, ws_id,
        ImportParams(sources=[str(card)], destination=str(archive),
                     verify_by_hash=True),
    )

    # Card must NOT be counted as a skipped duplicate — no twin backs it.
    assert result["skipped_duplicate"] == 0
    assert result["copied"] == 1
    # Bytes now live at the fresh archive path.
    dest = archive / "2026" / "2026-06-01" / "IMG_0500.jpg"
    assert dest.exists()
    assert compute_file_hash(str(dest)) == card_hash
    # safe_to_format is true because the card's bytes verifiably exist
    # at the fresh archive path — but via copy, not via a stale row.
    assert result["safe_to_format"] is True


def test_stale_hash_row_with_modified_bytes_imports_as_fresh(tmp_path):
    """The archive file at the cataloged path exists but was modified
    since scan (bytes no longer match ``photos.file_hash``). The card's
    hash still matches the stale row; the twin's re-hash does not. The
    card must import as fresh — the stale hash row is not proof that the
    bytes verifiably exist on disk."""
    from import_dedup import compute_file_hash
    from import_job import ImportParams, run_import_job

    card = tmp_path / "card"
    card.mkdir()
    card_file = card / "IMG_0600.jpg"
    Image.new("RGB", (16, 16), "green").save(str(card_file))
    ts = datetime(2026, 6, 2, 9, 0, 0).timestamp()
    os.utime(str(card_file), (ts, ts))
    card_hash = compute_file_hash(str(card_file))

    # Seed a "twin" archive file whose CURRENT bytes differ from the
    # cataloged file_hash (a stale row: the file was modified after the
    # last scan).
    archive = tmp_path / "archive"
    library = archive / "old-library"
    library.mkdir(parents=True)
    twin_path = library / "IMG_0600.jpg"
    Image.new("RGB", (16, 16), "blue").save(str(twin_path))
    twin_current_hash = compute_file_hash(str(twin_path))
    assert twin_current_hash != card_hash

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES (?, ?, 'ok')",
        (str(library), "old-library"),
    ).lastrowid
    # Stale row: file_hash claims card_hash but on-disk bytes hash to
    # twin_current_hash.
    db.conn.execute(
        "INSERT INTO photos (folder_id, filename, extension, file_size,"
        " file_hash) VALUES (?, ?, '.jpg', ?, ?)",
        (fid, "IMG_0600.jpg", os.path.getsize(str(card_file)), card_hash),
    )
    db.conn.commit()

    result = run_import_job(
        _make_job(), FakeRunner(), db_path, ws_id,
        ImportParams(sources=[str(card)], destination=str(archive),
                     verify_by_hash=True),
    )

    assert result["skipped_duplicate"] == 0
    assert result["copied"] == 1
    dest = archive / "2026" / "2026-06-02" / "IMG_0600.jpg"
    assert compute_file_hash(str(dest)) == card_hash
    assert result["safe_to_format"] is True


def test_key_match_with_different_bytes_imports_as_distinct(tmp_path):
    """A metadata-only ("key") match against a cataloged twin whose bytes
    differ must NOT be skipped: the card's bytes were never verified
    anywhere, so skipping would let the safe-to-format pill go green while
    the card holds the only copy. The file imports as a distinct photo."""
    from import_job import ImportParams
    from PIL.ExifTags import Base as ExifBase

    dt = datetime(2026, 5, 1, 10, 15, 30)

    # Card file with a trustworthy EXIF capture time.
    card = tmp_path / "card"
    card.mkdir()
    card_file = card / "IMG_0400.jpg"
    img = Image.new("RGB", (16, 16), "red")
    exif = img.getexif()
    exif[ExifBase.DateTimeOriginal] = dt.strftime("%Y:%m:%d %H:%M:%S")
    img.save(str(card_file), exif=exif)
    card_bytes = card_file.read_bytes()

    # Cataloged twin: same name, same size, same trusted capture time —
    # different bytes (last byte flipped; only ever hashed, never decoded).
    library = tmp_path / "library"
    library.mkdir()
    twin_file = library / "IMG_0400.jpg"
    twin_bytes = card_bytes[:-1] + bytes([card_bytes[-1] ^ 0xFF])
    twin_file.write_bytes(twin_bytes)
    assert len(twin_bytes) == len(card_bytes)

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES (?, ?, 'ok')",
        (str(library), "library"),
    ).lastrowid
    db.conn.execute(
        "INSERT INTO photos (folder_id, filename, extension, file_size,"
        " timestamp) VALUES (?, ?, '.jpg', ?, ?)",
        (fid, "IMG_0400.jpg", len(twin_bytes), "2026-05-01T10:15:30"),
    )
    db.conn.commit()

    archive = tmp_path / "archive"
    from import_job import run_import_job
    result = run_import_job(_make_job(), FakeRunner(), db_path, ws_id,
                            ImportParams(sources=[str(card)],
                                         destination=str(archive)))

    # Imported as a fresh distinct photo, not skipped.
    assert result["copied"] == 1
    assert result["skipped_duplicate"] == 0
    dest = archive / "2026" / "2026-05-01" / "IMG_0400.jpg"
    assert dest.read_bytes() == card_bytes
    # Two catalog rows now: the seeded twin and the new import.
    assert len(_photo_rows(db)) == 2
    # The copy verified, so the card is safe.
    assert result["safe_to_format"] is True


def test_intra_run_key_collision_across_cards_imports_second_as_fresh(tmp_path):
    """Two cards can hold different bytes at the same filename+size+
    capture-second (say, an IMG_XXXX rollover after a firmware reset).
    A metadata-key match against the first card's just-copied file must
    NOT let the second card's file be counted as skipped without a byte
    check: the two files' bytes were never compared, so skipping would
    let safe_to_format go green while the second card is the only copy
    of its bytes."""
    from import_dedup import compute_file_hash
    from import_job import ImportParams, run_import_job
    from PIL.ExifTags import Base as ExifBase

    dt = datetime(2026, 5, 2, 11, 20, 45)

    card1 = tmp_path / "card1"
    card1.mkdir()
    card1_file = card1 / "IMG_0700.jpg"
    img = Image.new("RGB", (16, 16), "red")
    exif = img.getexif()
    exif[ExifBase.DateTimeOriginal] = dt.strftime("%Y:%m:%d %H:%M:%S")
    img.save(str(card1_file), exif=exif)
    card1_bytes = card1_file.read_bytes()

    # Card 2: SAME filename, SAME size, SAME trusted capture time,
    # different bytes (last byte flipped; EXIF header untouched).
    card2 = tmp_path / "card2"
    card2.mkdir()
    card2_file = card2 / "IMG_0700.jpg"
    card2_bytes = card1_bytes[:-1] + bytes([card1_bytes[-1] ^ 0xFF])
    assert len(card2_bytes) == len(card1_bytes)
    assert card2_bytes != card1_bytes
    card2_file.write_bytes(card2_bytes)

    archive = tmp_path / "archive"
    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    result = run_import_job(
        _make_job(), FakeRunner(), db_path, ws_id, ImportParams(
            sources=[str(card1), str(card2)], destination=str(archive),
        ),
    )

    # Second card must NOT be counted as skipped_duplicate.
    assert result["copied"] == 2
    assert result["skipped_duplicate"] == 0
    assert result["failed"] == 0
    # safe_to_format is true because both cards' bytes verifiably landed.
    assert result["safe_to_format"] is True

    dest_dir = archive / "2026" / "2026-05-02"
    landed = sorted(p for p in dest_dir.iterdir() if p.is_file())
    assert len(landed) == 2
    on_disk = {p.read_bytes() for p in landed}
    assert card1_bytes in on_disk
    assert card2_bytes in on_disk
    hashes_on_disk = {compute_file_hash(str(p)) for p in landed}
    assert compute_file_hash(str(card1_file)) in hashes_on_disk
    assert compute_file_hash(str(card2_file)) in hashes_on_disk


def test_key_candidate_source_read_error_fails_only_that_file(
        tmp_path, monkeypatch):
    """When the current-source hash read for a metadata-key duplicate
    candidate raises OSError (removable media pulled mid-check, I/O
    error), that source alone is bucketed as failed. The failure must
    not escape and kill the whole background job — siblings still import
    normally, and the safe-to-format ledger records the failure."""
    import import_dedup
    from import_job import ImportParams, run_import_job
    from PIL.ExifTags import Base as ExifBase

    dt_bad = datetime(2026, 5, 3, 12, 0, 0)
    dt_good = datetime(2026, 5, 3, 12, 5, 0)

    card = tmp_path / "card"
    card.mkdir()
    bad_file = card / "IMG_0800.jpg"
    img = Image.new("RGB", (16, 16), "red")
    exif_bad = img.getexif()
    exif_bad[ExifBase.DateTimeOriginal] = (
        dt_bad.strftime("%Y:%m:%d %H:%M:%S")
    )
    img.save(str(bad_file), exif=exif_bad)

    good_file = card / "IMG_0801.jpg"
    img2 = Image.new("RGB", (16, 16), "green")
    exif_good = img2.getexif()
    exif_good[ExifBase.DateTimeOriginal] = (
        dt_good.strftime("%Y:%m:%d %H:%M:%S")
    )
    img2.save(str(good_file), exif=exif_good)

    # Seed a cataloged twin whose (filename, size, capture-second) matches
    # the bad file — checker.match() will return ('key', …) without needing
    # to hash, so the next read (the current-source hash for byte
    # verification) is the one we make fail.
    library = tmp_path / "library"
    library.mkdir()
    twin_file = library / "IMG_0800.jpg"
    twin_bytes = bad_file.read_bytes()[:-1] + b"\x00"
    twin_file.write_bytes(twin_bytes)

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES (?, ?, 'ok')",
        (str(library), "library"),
    ).lastrowid
    db.conn.execute(
        "INSERT INTO photos (folder_id, filename, extension, file_size,"
        " timestamp) VALUES (?, ?, '.jpg', ?, ?)",
        (fid, "IMG_0800.jpg", os.path.getsize(str(bad_file)),
         dt_bad.strftime("%Y-%m-%dT%H:%M:%S")),
    )
    db.conn.commit()

    real_hash = import_dedup.compute_file_hash
    bad_path_str = str(bad_file)

    def flaky_hash(path):
        if str(path) == bad_path_str:
            raise OSError("card yanked mid-check")
        return real_hash(path)

    monkeypatch.setattr(import_dedup, "compute_file_hash", flaky_hash)

    archive = tmp_path / "archive"
    result = run_import_job(
        _make_job(), FakeRunner(), db_path, ws_id,
        ImportParams(sources=[str(card)], destination=str(archive)),
    )

    assert result["failed"] == 1
    assert result["copied"] == 1
    assert result["skipped_duplicate"] == 0
    assert result["safe_to_format"] is False
    assert len(result["unsafe_files"]) == 1
    assert result["unsafe_files"][0]["path"] == bad_path_str
    assert "duplicate check failed" in result["unsafe_files"][0]["reason"]

    dest_good = archive / "2026" / "2026-05-03" / "IMG_0801.jpg"
    assert dest_good.exists()
    assert dest_good.read_bytes() == good_file.read_bytes()


def test_copy_and_hash_verify_roundtrip(tmp_path):
    from import_dedup import compute_file_hash
    from import_job import copy_and_hash_verify

    src = tmp_path / "card" / "DSC_0001.jpg"
    src.parent.mkdir()
    src.write_bytes(b"pixels" * 1000)
    dst = tmp_path / "archive" / "2026" / "DSC_0001.jpg"

    ok, file_hash = copy_and_hash_verify(str(src), str(dst))
    assert ok is True
    assert file_hash == compute_file_hash(str(src))
    assert dst.read_bytes() == src.read_bytes()


def test_copy_and_hash_verify_detects_corruption(tmp_path, monkeypatch):
    """A copy whose destination bytes differ must fail without deleting any
    previously verified archive file at the destination path."""
    import shutil

    from import_job import copy_and_hash_verify

    src = tmp_path / "card" / "DSC_0002.jpg"
    src.parent.mkdir()
    src.write_bytes(b"good bytes")
    dst = tmp_path / "archive" / "DSC_0002.jpg"
    dst.parent.mkdir()
    dst.write_bytes(b"existing verified archive bytes")

    real_copy2 = shutil.copy2

    def corrupting_copy2(s, d):
        real_copy2(s, d)
        with open(d, "r+b") as f:
            f.write(b"BAD")

    monkeypatch.setattr("import_job.shutil.copy2", corrupting_copy2)
    ok, file_hash = copy_and_hash_verify(str(src), str(dst))
    assert ok is False
    assert file_hash is None
    assert dst.read_bytes() == b"existing verified archive bytes"
    assert not list(dst.parent.glob(".DSC_0002.jpg.*.tmp"))


# --- working copies from the card (Task 2.5) -----------------------------

def _stub_extractor(monkeypatch, outcome):
    """Replace scanner.extract_working_copy, recording calls.

    ``outcome(source_path)`` decides the stubbed return value.
    """
    import scanner
    calls = []

    def fake_extract(source_path, output_path, max_size=4096, quality=92):
        calls.append((str(source_path), str(output_path)))
        return outcome(str(source_path))

    monkeypatch.setattr(scanner, "extract_working_copy", fake_extract)
    return calls


def test_working_copy_extracted_from_card_path(tmp_path, monkeypatch):
    """The working copy reads the CARD copy of a RAW file, not the archive
    copy — after import, no processing stage re-reads originals from the
    (possibly slow) archive volume."""
    from import_job import ImportParams

    calls = _stub_extractor(monkeypatch, lambda src: True)

    card = tmp_path / "card"
    card.mkdir()
    # JPEG bytes under a RAW extension: extraction is stubbed, so only the
    # extension-based RAW candidacy matters.
    Image.new("RGB", (16, 16), "red").save(str(card / "DSC_0500.jpg"))
    os.rename(str(card / "DSC_0500.jpg"), str(card / "DSC_0500.NEF"))

    archive = tmp_path / "archive"
    vireo_dir = tmp_path / "vireo"
    db, ws_id, result = _run_import(tmp_path, ImportParams(
        sources=[str(card)], destination=str(archive),
        vireo_dir=str(vireo_dir),
    ))

    assert result["copied"] == 1
    assert result["safe_to_format"] is True
    assert len(calls) == 1
    src, out = calls[0]
    assert src == str(card / "DSC_0500.NEF"), (
        f"extraction read {src}, expected the card path"
    )
    rows = _photo_rows(db)
    assert len(rows) == 1
    pid = rows[0]["id"]
    assert out == os.path.join(str(vireo_dir), "working", f"{pid}.jpg")
    wc = db.conn.execute(
        "SELECT working_copy_path FROM photos WHERE id = ?", (pid,),
    ).fetchone()["working_copy_path"]
    assert wc == f"working/{pid}.jpg"


def test_working_copy_companion_fallback_reads_card_jpeg(
        tmp_path, monkeypatch):
    """RAW+JPEG pair: when the RAW decode fails, the companion fallback
    must read the CARD's JPEG. Also pins that the companion file (whose
    photo row is deliberately merged away by pairing) is not bucketed as
    a failure by the hash-stamping pass."""
    from import_job import ImportParams

    calls = _stub_extractor(
        monkeypatch, lambda src: not src.lower().endswith(".nef"),
    )

    card = tmp_path / "card"
    card.mkdir()
    Image.new("RGB", (16, 16), "red").save(str(card / "DSC_0501.jpg"))
    # The RAW must be distinct bytes (a real pair always is) or the
    # duplicate gate would skip it as an intra-run twin of the JPEG.
    raw_bytes = (card / "DSC_0501.jpg").read_bytes() + b"RAW-SENSOR-DATA"
    (card / "DSC_0501.NEF").write_bytes(raw_bytes)

    archive = tmp_path / "archive"
    vireo_dir = tmp_path / "vireo"
    db, ws_id, result = _run_import(tmp_path, ImportParams(
        sources=[str(card)], destination=str(archive),
        vireo_dir=str(vireo_dir),
    ))

    assert result["copied"] == 2
    assert result["failed"] == 0
    assert result["safe_to_format"] is True

    # RAW attempt from the card, then companion fallback from the card.
    sources_tried = [src for src, _ in calls]
    assert str(card / "DSC_0501.NEF") in sources_tried
    assert str(card / "DSC_0501.jpg") in sources_tried

    # Pairing merged the JPEG row into the RAW primary.
    rows = _photo_rows(db)
    assert len(rows) == 1
    assert rows[0]["filename"] == "DSC_0501.NEF"
    wc = db.conn.execute(
        "SELECT working_copy_path, companion_path FROM photos WHERE id = ?",
        (rows[0]["id"],),
    ).fetchone()
    assert wc["working_copy_path"] == f"working/{rows[0]['id']}.jpg"
    assert wc["companion_path"] == "DSC_0501.jpg"


def test_failed_extraction_leaves_working_copy_null(tmp_path, monkeypatch):
    from import_job import ImportParams

    _stub_extractor(monkeypatch, lambda src: False)

    card = tmp_path / "card"
    card.mkdir()
    Image.new("RGB", (16, 16), "red").save(str(card / "DSC_0502.jpg"))
    os.rename(str(card / "DSC_0502.jpg"), str(card / "DSC_0502.NEF"))

    db, ws_id, result = _run_import(tmp_path, ImportParams(
        sources=[str(card)], destination=str(tmp_path / "archive"),
        vireo_dir=str(tmp_path / "vireo"),
    ))

    # Import itself still succeeds; the backfill retries extraction later.
    assert result["copied"] == 1
    assert result["safe_to_format"] is True
    rows = _photo_rows(db)
    wc = db.conn.execute(
        "SELECT working_copy_path FROM photos WHERE id = ?",
        (rows[0]["id"],),
    ).fetchone()["working_copy_path"]
    assert wc is None


# --- interruption + resume contract (Task 2.6) ---------------------------
# These tests prove _deindex_staging has no equivalent here: every stopping
# point leaves a valid catalog, and a retry resumes instead of redoing.

class CancelAfterFirstBatchRunner(FakeRunner):
    """Flips to cancelled once progress reports a file in the second
    destination folder (i.e. the second batch has started)."""

    def __init__(self, trigger_fragment):
        super().__init__()
        self.trigger_fragment = trigger_fragment

    def push_event(self, job_id, event_type, data):
        super().push_event(job_id, event_type, data)
        if (
            event_type == "progress"
            and self.trigger_fragment in (data.get("phase") or "")
        ):
            self.cancelled_ids.add(job_id)


def test_cancel_leaves_valid_partial_catalog(tmp_path):
    from import_job import ImportParams

    card = _make_card(tmp_path, [
        ("DSC_0030.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
        ("DSC_0031.jpg", datetime(2026, 7, 3, 11, 0, 0), "green"),
        ("DSC_0032.jpg", datetime(2026, 7, 4, 9, 0, 0), "blue"),
        ("DSC_0033.jpg", datetime(2026, 7, 4, 9, 5, 0), "white"),
    ])
    archive = tmp_path / "archive"
    runner = CancelAfterFirstBatchRunner("2026/2026-07-04")

    db, ws_id, result = _run_import(
        tmp_path,
        ImportParams(sources=[str(card)], destination=str(archive)),
        runner=runner,
    )

    assert result["cancelled"] is True
    assert result["safe_to_format"] is False
    # Partial progress: all of batch 1, some of batch 2 — never zero,
    # never everything.
    assert 0 < result["copied"] < 4
    assert result["failed"] == 0

    # The catalog is valid: every row's file exists on disk, verified.
    rows = _photo_rows(db)
    assert len(rows) == result["copied"]
    for r in rows:
        full = os.path.join(r["folder_path"], r["filename"])
        assert os.path.isfile(full)
        assert r["hash_status"] == "ok"


def test_rerun_resumes_and_completes(tmp_path):
    """Re-running the same import after a cancel skips exactly what landed
    and copies the rest — no unwinding, no redo."""
    from import_job import ImportParams

    card = _make_card(tmp_path, [
        ("DSC_0030.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
        ("DSC_0031.jpg", datetime(2026, 7, 3, 11, 0, 0), "green"),
        ("DSC_0032.jpg", datetime(2026, 7, 4, 9, 0, 0), "blue"),
        ("DSC_0033.jpg", datetime(2026, 7, 4, 9, 5, 0), "white"),
    ])
    archive = tmp_path / "archive"
    params = ImportParams(sources=[str(card)], destination=str(archive))

    runner = CancelAfterFirstBatchRunner("2026/2026-07-04")
    db, ws_id, first = _run_import(tmp_path, params, runner=runner)
    landed_first = first["copied"]
    assert 0 < landed_first < 4

    # Second run: same card, same params, fresh runner/job.
    from import_job import run_import_job
    second = run_import_job(
        _make_job("import-test-2"), FakeRunner(),
        str(tmp_path / "test.db"), ws_id, params,
    )

    assert second["cancelled"] is False
    assert second["failed"] == 0
    # Everything already landed is skipped; only the remainder copies.
    assert second["copied"] == 4 - landed_first
    assert second["copied"] + second["skipped_duplicate"] == 4
    assert second["safe_to_format"] is True

    # Combined catalog: exactly one row per card file, all verified.
    rows = _photo_rows(db)
    assert len(rows) == 4
    names = sorted(r["filename"] for r in rows)
    assert names == [
        "DSC_0030.jpg", "DSC_0031.jpg", "DSC_0032.jpg", "DSC_0033.jpg",
    ]
    for r in rows:
        assert os.path.isfile(os.path.join(r["folder_path"], r["filename"]))


def test_crash_shaped_copies_are_adopted_not_suffixed(tmp_path):
    """Files that landed on disk but died before their batch's scan (no
    catalog rows) must be adopted as already-present on re-run — cataloged
    without creating numeric-suffix duplicates. This is the 'rescan
    self-heals' story from the design doc."""
    import shutil

    from import_job import ImportParams

    card = _make_card(tmp_path, [
        ("DSC_0040.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
        ("DSC_0041.jpg", datetime(2026, 7, 3, 11, 0, 0), "green"),
    ])
    archive = tmp_path / "archive"

    # Simulate the crash: both files already at the destination,
    # byte-identical, with NO catalog rows.
    dest_dir = archive / "2026" / "2026-07-03"
    dest_dir.mkdir(parents=True)
    for name in ("DSC_0040.jpg", "DSC_0041.jpg"):
        shutil.copy2(str(card / name), str(dest_dir / name))

    db, ws_id, result = _run_import(tmp_path, ImportParams(
        sources=[str(card)], destination=str(archive),
    ))

    # Adopted as already-present, cataloged, no re-copy, no suffixes.
    assert result["copied"] == 0
    assert result["skipped_duplicate"] == 2
    assert result["failed"] == 0
    assert result["safe_to_format"] is True
    assert sorted(os.listdir(str(dest_dir))) == [
        "DSC_0040.jpg", "DSC_0041.jpg",
    ]
    rows = _photo_rows(db)
    assert len(rows) == 2
    for r in rows:
        assert r["hash_status"] == "ok"
        assert os.path.isfile(os.path.join(r["folder_path"], r["filename"]))


# --- Codex review 2026-07-05 regressions ---------------------------------


def test_copy_and_hash_verify_refuses_to_overwrite_existing_destination(
        tmp_path):
    """Concurrent imports targeting the same destination/filename cannot
    both pass the pre-copy collision check and then both promote their
    temp file; ``copy_and_hash_verify`` must fail the second promote
    (leaving the first job's verified archive bytes untouched) rather
    than silently overwriting with ``os.replace``.
    """
    from import_job import copy_and_hash_verify

    src = tmp_path / "card" / "DSC_9001.jpg"
    src.parent.mkdir()
    src.write_bytes(b"card bytes")

    dst = tmp_path / "archive" / "DSC_9001.jpg"
    dst.parent.mkdir()
    dst.write_bytes(b"already-verified archive bytes")

    ok, file_hash = copy_and_hash_verify(str(src), str(dst))
    assert ok is False
    assert file_hash is None
    # The pre-existing verified copy must survive the race.
    assert dst.read_bytes() == b"already-verified archive bytes"
    # And the temp file must be cleaned up.
    assert not list(dst.parent.glob(".DSC_9001.jpg.*.tmp"))


def test_import_destination_with_dot_segments_normalizes_scan_root(tmp_path):
    """Absolute destinations containing ``..`` segments must still catalog
    successfully. The scanner stops folder-chain recursion when a parent
    equals the scan root string; if the copy layout normalizes the path
    but the scan root does not, the recursion never reaches root and the
    copied files bucket as catalog failures.
    """
    from import_job import ImportParams

    card = _make_card(tmp_path, [
        ("DSC_0090.jpg", datetime(2026, 7, 4, 10, 0, 0), "red"),
    ])

    real_archive = tmp_path / "archive"
    real_archive.mkdir()
    (tmp_path / "junk").mkdir()

    # Absolute path with a dot segment resolving to real_archive.
    unnormalized = str(tmp_path / "junk" / ".." / "archive")

    db, ws_id, result = _run_import(tmp_path, ImportParams(
        sources=[str(card)], destination=unnormalized,
    ))

    assert result["copied"] == 1
    assert result["failed"] == 0
    assert result["safe_to_format"] is True

    rows = _photo_rows(db)
    assert len(rows) == 1
    assert os.path.isfile(
        os.path.join(rows[0]["folder_path"], rows[0]["filename"])
    )
    # And the file actually lives under the normalized archive root, not
    # a duplicated dot-segment path.
    assert os.path.realpath(rows[0]["folder_path"]).startswith(
        os.path.realpath(str(real_archive))
    )


def test_unreadable_source_subtree_flips_safe_to_format_off(
        tmp_path, monkeypatch):
    """If ``discover_source_files`` cannot enter a source subtree
    (permission denied, TCC block, unreadable removable-media dir), the
    files under that subtree are unseen — ``discovered`` shrinks silently
    and ``safe_to_format`` used to still flip green. Enumeration errors
    must now bubble into the ledger.
    """
    from import_job import ImportParams

    card = _make_card(tmp_path, [
        ("DSC_0100.jpg", datetime(2026, 7, 4, 10, 0, 0), "red"),
    ])

    # Force safe_scan_walk to report an error on the first walk step.
    import image_loader
    real_walk = image_loader.safe_scan_walk

    def broken_walk(top, onerror=None):
        # Simulate a PermissionError bubbling up from os.scandir on the
        # source root; safe_scan_walk's OSError branch would forward it
        # via onerror and yield nothing further.
        if onerror is not None:
            onerror(PermissionError(13, "Operation not permitted", str(top)))
        # Still yield everything real_walk would have produced so we can
        # verify the ledger is unsafe even when copies still landed.
        yield from real_walk(top, onerror=onerror)

    monkeypatch.setattr("ingest.safe_scan_walk", broken_walk)

    db, ws_id, result = _run_import(tmp_path, ImportParams(
        sources=[str(card)], destination=str(tmp_path / "archive"),
    ))

    # The one visible file still lands in the catalog...
    assert result["copied"] == 1
    # ...but safe_to_format is False because we couldn't prove every
    # source subtree was walked cleanly.
    assert result["safe_to_format"] is False
    assert result["ok"] is False
    assert result["discovery_errors"] == 1
    assert any(
        "source enumeration failed" in e for e in result["errors"]
    )


def test_wc_extraction_deferred_to_after_last_batch(tmp_path, monkeypatch):
    """A RAW+JPEG companion pair that straddles a batch boundary must not
    trigger per-batch working-copy extraction while the JPEG's row is
    still uncataloged (pairing has not run yet). Deferring extraction to
    end-of-run guarantees ``_pair_raw_jpeg_companions`` has seen every
    JPEG in every batch before the extractor decides which source to
    read.
    """
    import import_job
    import scanner

    monkeypatch.setattr(import_job, "IMPORT_BATCH_SIZE", 1)

    # Track extraction call order. ``extract_working_copy`` is invoked
    # once per candidate row; we care that when the RAW is processed the
    # JPEG's row has already been merged in (pairing has run).
    calls = []

    def fake_extract(source_path, output_path, max_size=4096, quality=92):
        calls.append(str(source_path))
        # Simulate a RAW decode failure so the companion fallback path
        # is exercised — the reason deferral matters at all.
        return not str(source_path).lower().endswith(".nef")

    monkeypatch.setattr(scanner, "extract_working_copy", fake_extract)

    card = tmp_path / "card"
    card.mkdir()
    Image.new("RGB", (16, 16), "red").save(str(card / "DSC_0700.jpg"))
    raw_bytes = (card / "DSC_0700.jpg").read_bytes() + b"RAW-SENSOR-DATA"
    (card / "DSC_0700.NEF").write_bytes(raw_bytes)
    # Same mtime so both files plan into the same destination folder.
    ts = datetime(2026, 7, 4, 10, 0, 0).timestamp()
    for name in ("DSC_0700.jpg", "DSC_0700.NEF"):
        os.utime(str(card / name), (ts, ts))

    vireo_dir = tmp_path / "vireo"
    db, ws_id, result = _run_import(tmp_path, import_job.ImportParams(
        sources=[str(card)], destination=str(tmp_path / "archive"),
        vireo_dir=str(vireo_dir),
    ))

    assert result["copied"] == 2
    assert result["failed"] == 0
    assert result["safe_to_format"] is True

    # After deferred extraction, pairing has merged the JPEG row into
    # the RAW primary and the extractor read the companion (from the
    # card) after the RAW decode was stubbed as failed.
    rows = _photo_rows(db)
    assert len(rows) == 1
    assert rows[0]["filename"] == "DSC_0700.NEF"
    wc = db.conn.execute(
        "SELECT working_copy_path, companion_path FROM photos WHERE id = ?",
        (rows[0]["id"],),
    ).fetchone()
    assert wc["companion_path"] == "DSC_0700.jpg"
    assert wc["working_copy_path"] == f"working/{rows[0]['id']}.jpg"

    # The JPEG source must have been read from the card, not the archive.
    jpeg_reads = [c for c in calls if c.lower().endswith(".jpg")]
    assert jpeg_reads, "companion fallback should have been attempted"
    assert all(str(card) in c for c in jpeg_reads), (
        f"companion extraction should read from the card, got {jpeg_reads}"
    )
