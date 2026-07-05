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


def test_duplicate_only_import_links_alias_spelled_twin(tmp_path):
    """When a twin folder is cataloged through a symlink alias but the
    import ``destination`` resolves to a different (real) spelling, the
    duplicate-link pass must still workspace-link the twin. Passing an
    alias-spelled ``restrict_dir`` to ``scan(root=destination)`` would
    infinite-recurse in ``_ensure_folder`` (it walks parents lexically
    without ever matching the realpath'd root); the import job routes
    the alias case straight to ``workspace_folders`` instead.
    """
    import sys as _sys

    if _sys.platform == "win32":
        # os.symlink usually requires elevation on Windows; skip.
        import pytest
        pytest.skip("symlinks not routinely available on Windows")

    from import_dedup import compute_file_hash
    from import_job import ImportParams, run_import_job

    # Real archive dir + symlink alias to it.
    real_archive = tmp_path / "real" / "archive"
    real_archive.mkdir(parents=True)
    dest_dir_real = real_archive / "2026" / "2026-07-03"
    dest_dir_real.mkdir(parents=True)
    dest_file = dest_dir_real / "IMG_9000.jpg"
    Image.new("RGB", (16, 16), "red").save(str(dest_file))

    alias_archive = tmp_path / "alias"
    os.symlink(str(real_archive), str(alias_archive))
    alias_dest_dir = alias_archive / "2026" / "2026-07-03"
    # Sanity: the alias resolves to the same folder.
    assert os.path.realpath(str(alias_dest_dir)) == str(dest_dir_real)

    # Catalog the twin under the ALIAS spelling — simulating a prior
    # scan that used ``/alias/…`` as its root — and don't link its
    # folder to the workspace.
    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES (?, ?, 'ok')",
        (str(alias_dest_dir), alias_dest_dir.name),
    ).lastrowid
    db.conn.execute(
        "INSERT INTO photos (folder_id, filename, extension, file_size,"
        " file_hash) VALUES (?, ?, '.jpg', ?, ?)",
        (
            fid,
            "IMG_9000.jpg",
            os.path.getsize(str(dest_file)),
            compute_file_hash(str(dest_file)),
        ),
    )
    db.conn.commit()
    assert str(alias_dest_dir) not in _ws_linked_folder_paths(db, ws_id)

    # Card has a byte-identical copy. Import to the REAL destination.
    card = tmp_path / "card"
    card.mkdir()
    import shutil
    shutil.copy2(str(dest_file), str(card / "IMG_9000.jpg"))

    runner = FakeRunner()
    job = _make_job()
    result = run_import_job(job, runner, db_path, ws_id, ImportParams(
        sources=[str(card)], destination=str(real_archive),
    ))

    assert result["copied"] == 0
    assert result["skipped_duplicate"] == 1
    assert result["failed"] == 0
    # The alias-spelled twin folder is now workspace-linked (via the
    # direct-link path, bypassing scan which would have infinite-
    # recursed in _ensure_folder).
    assert str(alias_dest_dir) in _ws_linked_folder_paths(db, ws_id)
    # Still exactly one photo row — no double-catalog of the twin.
    assert len(_photo_rows(db)) == 1
    # And the run is safe to format the card.
    assert result["safe_to_format"] is True


def test_catalog_never_references_missing_files(tmp_path, monkeypatch):
    """Invariant: catalog is a subset of verified on-disk files, even when
    some copies fail."""
    import shutil as shutil_mod

    from import_job import ImportParams

    card = _make_card(tmp_path, [
        ("DSC_0010.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
        ("DSC_0011.jpg", datetime(2026, 7, 3, 11, 0, 0), "green"),
    ])
    archive = tmp_path / "archive"

    # Sabotage the second copy: corrupt destination bytes for DSC_0011.
    real_copy2 = shutil_mod.copy2

    def flaky_copy2(s, d):
        real_copy2(s, d)
        if "DSC_0011" in str(d):
            with open(d, "r+b") as f:
                f.write(b"CORRUPT")

    monkeypatch.setattr("import_job.shutil.copy2", flaky_copy2)
    db, ws_id, result = _run_import(tmp_path, ImportParams(
        sources=[str(card)], destination=str(archive),
    ))

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


def test_catalog_twin_under_source_root_does_not_prove_duplicate(tmp_path):
    """A cataloged twin whose folder_path IS (or is under) an import
    source is the card file being imported — the user previously scanned
    the mounted card, so ``photos.file_hash`` matches the card's own
    bytes. Re-hashing that "twin" just re-reads the source and proves
    nothing about any archive copy; accepting it as duplicate proof would
    flip ``safe_to_format`` green over a card whose bytes never made it
    to the archive. The card must import fresh instead."""
    from import_dedup import compute_file_hash
    from import_job import ImportParams, run_import_job

    # Card file whose bytes hash to a known value.
    card = tmp_path / "card"
    card.mkdir()
    card_file = card / "IMG_0800.jpg"
    Image.new("RGB", (16, 16), "purple").save(str(card_file))
    ts = datetime(2026, 6, 3, 14, 0, 0).timestamp()
    os.utime(str(card_file), (ts, ts))
    card_hash = compute_file_hash(str(card_file))
    card_size = os.path.getsize(str(card_file))

    # Seed a stale catalog row whose folder_path IS the mounted card
    # (a prior scan of the card left this behind). file_hash matches the
    # card because it WAS computed by hashing the card file.
    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES (?, ?, 'ok')",
        (str(card), card.name),
    ).lastrowid
    db.conn.execute(
        "INSERT INTO photos (folder_id, filename, extension, file_size,"
        " file_hash) VALUES (?, ?, '.jpg', ?, ?)",
        (fid, "IMG_0800.jpg", card_size, card_hash),
    )
    db.conn.commit()

    archive = tmp_path / "archive"
    result = run_import_job(
        _make_job(), FakeRunner(), db_path, ws_id,
        ImportParams(sources=[str(card)], destination=str(archive),
                     verify_by_hash=True),
    )

    # Not skipped: the only "twin" is the card itself, and the card
    # can't prove its own bytes safe.
    assert result["skipped_duplicate"] == 0
    assert result["copied"] == 1
    dest = archive / "2026" / "2026-06-03" / "IMG_0800.jpg"
    assert dest.exists()
    assert compute_file_hash(str(dest)) == card_hash
    # safe_to_format is true because the card's bytes verifiably landed
    # at the archive — via a real copy, not via a stale card-side row.
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


def test_nonexistent_source_root_flips_safe_to_format_off(tmp_path):
    """If a requested source root cannot be positively walked at all
    (unmounted removable media, permission denied on the root itself, or
    a path that no longer exists between enqueue and worker start),
    ``discover_source_files`` used to return ``[]`` at its pre-walk
    ``is_dir()`` guard WITHOUT invoking the ``onerror`` collector. That
    left ``discovered == 0`` and ``discovery_errors`` empty, so the
    predicate reported ``safe_to_format: true`` even though no card
    contents were ever enumerated — the UI would tell the user it's safe
    to format a card whose contents were never verified. See PR #1107
    review (P1 line 927).
    """
    from import_job import ImportParams

    missing_card = tmp_path / "unmounted-card"
    # Deliberately do NOT create the directory. This mirrors a card that
    # was ejected between enqueue and the import worker starting.

    db, ws_id, result = _run_import(tmp_path, ImportParams(
        sources=[str(missing_card)],
        destination=str(tmp_path / "archive"),
    ))

    assert result["discovered"] == 0
    assert result["safe_to_format"] is False
    assert result["ok"] is False
    assert result["discovery_errors"] == 1
    assert any(
        "source enumeration failed" in e for e in result["errors"]
    )
    assert any(str(missing_card) in e for e in result["errors"])


def test_excluded_bundle_source_root_flips_safe_to_format_off(tmp_path):
    """Same guarantee for the excluded-bundle branch of the pre-walk
    guard: if the caller pointed the import at a Photos-library-style
    data bundle directly (or the root is otherwise refused), the run
    must not silently report zero-files-imported-safe-to-format.
    """
    from import_job import ImportParams

    bundle = tmp_path / "Photos Library.photoslibrary"
    bundle.mkdir()
    # A single file inside the bundle so a naive walk would find it —
    # the guard must fire on the ROOT and refuse to enumerate it,
    # bubbling that refusal as a discovery error.
    (bundle / "originals").mkdir()
    (bundle / "originals" / "managed.jpg").write_bytes(b"jpeg")

    db, ws_id, result = _run_import(tmp_path, ImportParams(
        sources=[str(bundle)],
        destination=str(tmp_path / "archive"),
    ))

    assert result["discovered"] == 0
    assert result["safe_to_format"] is False
    assert result["ok"] is False
    assert result["discovery_errors"] == 1


def test_filtered_import_is_never_safe_to_format(tmp_path):
    """A narrowed ``file_types`` (``"raw"``, ``"jpeg"``, or a custom list)
    only enumerates the requested subset, so ``discovered`` covers less
    than the card's actual supported-file footprint. The naive
    ``copied + skipped_duplicate == discovered`` check would still pass
    even though other supported photos on the card were never imported —
    and the pill would then tell the user it's safe to format a card that
    still holds files. See PR #1107 review (P1 line 420).
    """
    from import_job import ImportParams

    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
        ("DSC_0002.jpg", datetime(2026, 7, 3, 11, 0, 0), "green"),
    ])
    archive = tmp_path / "archive"

    # ``file_types="jpeg"`` still copies every file this card actually
    # holds (they're all JPEGs), so copied == discovered would otherwise
    # flip safe_to_format green.
    db, ws_id, result = _run_import(tmp_path, ImportParams(
        sources=[str(card)],
        destination=str(archive),
        file_types="jpeg",
    ))

    assert result["copied"] == 2
    assert result["failed"] == 0
    assert result["discovered"] == 2
    # But the run only asked for JPEGs — a RAW sitting on the same card
    # would have been silently skipped. The pill has no way to prove
    # otherwise without re-walking the card, so it stays false.
    assert result["safe_to_format"] is False


def test_custom_file_types_list_is_never_safe_to_format(tmp_path):
    """Same guarantee for the explicit-extension-list form of
    ``file_types``: any narrowing counts as filtered."""
    from import_job import ImportParams

    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
    ])
    archive = tmp_path / "archive"

    db, ws_id, result = _run_import(tmp_path, ImportParams(
        sources=[str(card)],
        destination=str(archive),
        file_types=[".jpg"],
    ))

    assert result["copied"] == 1
    assert result["safe_to_format"] is False


def test_non_recursive_import_is_never_safe_to_format(tmp_path):
    """``recursive=False`` only enumerates top-level files, so any card
    with photos in subdirectories has files ``discovered`` never saw.
    ``copied + skipped_duplicate == discovered`` would still pass and the
    pill would tell the user it's safe to format a card that still holds
    unimported photos in subfolders. See PR #1107 Codex review on commit
    7dc0cce (import_job.py:1350).
    """
    from import_job import ImportParams

    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
    ])
    # A photo tucked in a subdirectory the non-recursive walk cannot see.
    subdir = card / "subfolder"
    subdir.mkdir()
    Image.new("RGB", (16, 16), "blue").save(str(subdir / "DSC_0002.jpg"))
    ts = datetime(2026, 7, 3, 11, 0, 0).timestamp()
    os.utime(str(subdir / "DSC_0002.jpg"), (ts, ts))
    archive = tmp_path / "archive"

    db, ws_id, result = _run_import(tmp_path, ImportParams(
        sources=[str(card)],
        destination=str(archive),
        recursive=False,
    ))

    # The top-level file copied cleanly, so the naive equality check
    # (copied + skipped == discovered) would otherwise flip green.
    assert result["copied"] == 1
    assert result["failed"] == 0
    assert result["discovered"] == 1
    # But the non-recursive walk never saw ``subfolder/DSC_0002.jpg``;
    # formatting the card would delete an unimported photo.
    assert result["safe_to_format"] is False


def test_deferred_extraction_skipped_when_already_cancelled(
    tmp_path, monkeypatch,
):
    """If the run was cancelled at a batch boundary before the deferred
    working-copy pass, don't spend minutes decoding RAWs the user has
    already asked us to abort. The extractor must not be called at all,
    and the returned status must remain ``cancelled``. See PR #1107
    Codex review on commit 7dc0cce (import_job.py:1296).
    """
    import scanner
    from import_job import ImportParams

    calls = []

    def spy_extract(*args, **kwargs):
        calls.append((args, kwargs))

    monkeypatch.setattr(scanner, "_extract_working_copies", spy_extract)

    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
    ])
    archive = tmp_path / "archive"

    runner = FakeRunner()
    job = _make_job()
    # Cancel before the run starts. The first batch-boundary check flips
    # ``cancelled`` on, and the deferred pass must then be skipped.
    runner.cancelled_ids.add(job["id"])

    db, ws_id, result = _run_import(
        tmp_path,
        ImportParams(
            sources=[str(card)],
            destination=str(archive),
            vireo_dir=str(tmp_path / "vireo_dir"),
        ),
        runner=runner,
        job=job,
    )

    assert result["cancelled"] is True
    assert result["safe_to_format"] is False
    assert calls == [], (
        "deferred _extract_working_copies must be skipped when cancelled"
    )


def test_deferred_extraction_threads_cancel_check(tmp_path, monkeypatch):
    """When the deferred working-copy pass does run, it must receive a
    ``cancel_check`` callable so a cancel issued mid-pass aborts the
    per-row loop instead of decoding every RAW to completion. See PR
    #1107 Codex review on commit 7dc0cce (import_job.py:1296).
    """
    import scanner
    from import_job import ImportParams

    captured = {}

    def spy_extract(*args, **kwargs):
        captured["cancel_check"] = kwargs.get("cancel_check")
        captured["source_paths"] = kwargs.get("source_paths")

    monkeypatch.setattr(scanner, "_extract_working_copies", spy_extract)

    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
    ])
    archive = tmp_path / "archive"

    runner = FakeRunner()
    job = _make_job()

    _run_import(
        tmp_path,
        ImportParams(
            sources=[str(card)],
            destination=str(archive),
            vireo_dir=str(tmp_path / "vireo_dir"),
        ),
        runner=runner,
        job=job,
    )

    cancel_check = captured.get("cancel_check")
    assert callable(cancel_check), (
        "deferred pass must receive a cancel_check callable"
    )
    # Not cancelled yet → callable returns falsy.
    assert not cancel_check()
    # Once the runner records a cancel, the callable flips to truthy so
    # ``_extract_working_copies`` bails out on the next row check.
    runner.cancelled_ids.add(job["id"])
    assert cancel_check()


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


# --- Codex 2026-07-05 followups: two findings not addressed by 9e0834af ----

def test_checker_record_oserror_does_not_kill_job(tmp_path, monkeypatch):
    """If ``DuplicateChecker.record`` re-``os.stat``s the source after a
    verified ``copy_and_hash_verify`` succeeded and the card has since
    been pulled, the OSError must not escape and kill the background
    job — the file is already verified at the archive, so the ledger
    keeps the copy and the run continues to catalog what landed."""
    import import_dedup
    from import_job import ImportParams, run_import_job

    card = _make_card(tmp_path, [
        ("DSC_0A60.jpg", datetime(2026, 8, 4, 10, 0, 0), "red"),
        ("DSC_0A61.jpg", datetime(2026, 8, 4, 10, 5, 0), "green"),
    ])

    real_record = import_dedup.DuplicateChecker.record
    calls = {"n": 0}

    def flaky_record(self, source_file):
        calls["n"] += 1
        if calls["n"] == 1:
            # First file lands, then card "goes away" — record's re-stat
            # raises. The file itself is on the archive already.
            raise OSError("card yanked after copy")
        return real_record(self, source_file)

    monkeypatch.setattr(
        import_dedup.DuplicateChecker, "record", flaky_record,
    )

    archive = tmp_path / "archive"
    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    result = run_import_job(
        _make_job(), FakeRunner(), db_path, ws_id,
        ImportParams(sources=[str(card)], destination=str(archive)),
    )

    # Both files landed and were cataloged (the record OSError was a
    # bookkeeping optimization; the archive is the source of truth).
    assert result["copied"] == 2
    assert result["failed"] == 0
    assert result["safe_to_format"] is True
    rows = _photo_rows(db)
    assert {r["filename"] for r in rows} == {
        "DSC_0A60.jpg", "DSC_0A61.jpg",
    }
    for r in rows:
        assert os.path.isfile(os.path.join(r["folder_path"], r["filename"]))
        assert r["hash_status"] == "ok"


def test_dup_link_scan_failure_marks_unsafe(tmp_path, monkeypatch):
    """When the workspace-linking scan for a duplicate-only batch raises,
    swallowing it would leave safe_to_format=true even though the
    imported duplicates are not visible in the active workspace. The
    failure must surface: safe_to_format false, ok false, and the
    failing folder(s) recorded in unsafe_files."""
    import scanner as scanner_mod
    from import_dedup import compute_file_hash
    from import_job import ImportParams, run_import_job

    # Pre-catalog a photo at the archive destination WITHOUT linking its
    # folder to the active workspace (raw SQL, no workspace_folders rows).
    archive = tmp_path / "archive"
    dest_dir = archive / "2026" / "2026-08-05"
    dest_dir.mkdir(parents=True)
    dest_file = dest_dir / "IMG_0A80.jpg"
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
            "IMG_0A80.jpg",
            os.path.getsize(str(dest_file)),
            compute_file_hash(str(dest_file)),
        ),
    )
    db.conn.commit()

    # Card holds a byte-identical copy → duplicate-only batch.
    card = tmp_path / "card"
    card.mkdir()
    import shutil
    shutil.copy2(str(dest_file), str(card / "IMG_0A80.jpg"))

    real_scan = scanner_mod.scan

    def flaky_scan(root, db_arg, **kwargs):
        # The dup-link scan is called WITHOUT restrict_files, with a
        # restrict_dirs pointing at our seeded dest_dir. Anything else
        # (the fresh-copy path) passes through untouched.
        restrict_dirs = kwargs.get("restrict_dirs") or []
        if (
            "restrict_files" not in kwargs
            or kwargs["restrict_files"] is None
        ) and any(
            os.path.normpath(str(d)) == str(dest_dir)
            for d in restrict_dirs
        ):
            # Not RuntimeError — that's reserved for cancellation
            # (``scanner.scan`` raises ``RuntimeError("scan cancelled")``).
            # A dup-link scan crash is a distinct failure mode.
            raise OSError("simulated dup-link scan failure")
        return real_scan(root, db_arg, **kwargs)

    monkeypatch.setattr(scanner_mod, "scan", flaky_scan)

    result = run_import_job(
        _make_job(), FakeRunner(), db_path, ws_id,
        ImportParams(sources=[str(card)], destination=str(archive)),
    )

    assert result["skipped_duplicate"] == 1
    assert result["safe_to_format"] is False
    assert result["ok"] is False
    assert any(
        str(dest_dir) in u["path"] for u in result["unsafe_files"]
    ), (
        "expected the failing dup-link folder in unsafe_files; got "
        f"{result['unsafe_files']!r}"
    )
    # The seeded folder still isn't linked (that was the whole point).
    assert str(dest_dir) not in _ws_linked_folder_paths(db, ws_id)


def test_dup_link_scan_non_cancel_runtime_error_marks_unsafe(
        tmp_path, monkeypatch):
    """A non-cancellation ``RuntimeError`` from the dup-link scan (a
    library-level RuntimeError, or a RecursionError which inherits from
    RuntimeError) must not be routed to the cancellation branch — the
    runner was never cancelled, and treating the job as cancelled would
    hide the workspace-link failure from ``ok``/``safe_to_format`` and
    let the UI serve an import result that looks successful even though
    the imported duplicates never became visible. See PR #1107 review.
    """
    import scanner as scanner_mod
    from import_dedup import compute_file_hash
    from import_job import ImportParams, run_import_job

    archive = tmp_path / "archive"
    dest_dir = archive / "2026" / "2026-08-06"
    dest_dir.mkdir(parents=True)
    dest_file = dest_dir / "IMG_0B81.jpg"
    Image.new("RGB", (16, 16), "green").save(str(dest_file))

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
            "IMG_0B81.jpg",
            os.path.getsize(str(dest_file)),
            compute_file_hash(str(dest_file)),
        ),
    )
    db.conn.commit()

    card = tmp_path / "card"
    card.mkdir()
    import shutil
    shutil.copy2(str(dest_file), str(card / "IMG_0B81.jpg"))

    real_scan = scanner_mod.scan

    def flaky_scan(root, db_arg, **kwargs):
        restrict_dirs = kwargs.get("restrict_dirs") or []
        if (
            "restrict_files" not in kwargs
            or kwargs["restrict_files"] is None
        ) and any(
            os.path.normpath(str(d)) == str(dest_dir)
            for d in restrict_dirs
        ):
            # A non-sentinel RuntimeError — the runner was never
            # cancelled. RecursionError (a RuntimeError subclass) or a
            # library-level RuntimeError bubbling out of the scan would
            # look like this at the handler.
            raise RuntimeError("library exploded during scan")
        return real_scan(root, db_arg, **kwargs)

    monkeypatch.setattr(scanner_mod, "scan", flaky_scan)

    result = run_import_job(
        _make_job(), FakeRunner(), db_path, ws_id,
        ImportParams(sources=[str(card)], destination=str(archive)),
    )

    assert result["skipped_duplicate"] == 1
    # The whole point: not routed to cancellation; instead reported as a
    # dup-link failure so ``safe_to_format`` reflects reality.
    assert result["cancelled"] is False
    assert result["safe_to_format"] is False
    assert result["ok"] is False
    assert any(
        str(dest_dir) in u["path"] for u in result["unsafe_files"]
    ), (
        "expected the failing dup-link folder in unsafe_files; got "
        f"{result['unsafe_files']!r}"
    )
    assert str(dest_dir) not in _ws_linked_folder_paths(db, ws_id)


def test_wc_extraction_falls_back_to_archive_when_card_vanishes(
        tmp_path, monkeypatch):
    """When the deferred working-copy pass runs after copying and the
    card has been unmounted, ``source_paths`` still points at the card's
    dead path; the extractor must NOT read from that missing path and
    record a failure marker. It must fall back to the verified archive
    copy, so the extraction reads a live file.
    """
    import import_job
    import scanner

    # A RAW file (fake .NEF) is what makes the row a working-copy
    # extraction candidate — a bare small JPEG is skipped by the
    # candidate predicate. Same trick as
    # ``test_wc_extraction_deferred_to_after_last_batch``: bytes that
    # scanner's metadata reader accepts as an image, extra bytes past
    # the end to distinguish RAW from JPEG.
    card = tmp_path / "card"
    card.mkdir()
    Image.new("RGB", (16, 16), "red").save(str(card / "DSC_1001.jpg"))
    raw_bytes = (card / "DSC_1001.jpg").read_bytes() + b"RAW-SENSOR-DATA"
    (card / "DSC_1001.NEF").write_bytes(raw_bytes)
    (card / "DSC_1001.jpg").unlink()  # RAW-only, no companion
    ts = datetime(2026, 7, 4, 10, 0, 0).timestamp()
    os.utime(str(card / "DSC_1001.NEF"), (ts, ts))

    # Track every extract_working_copy call's source path.
    calls = []

    def fake_extract(source_path, output_path, max_size=4096, quality=92):
        calls.append(str(source_path))
        if not os.path.isfile(source_path):
            return False
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, "wb") as f:
            f.write(b"jpeg-bytes")
        return True

    monkeypatch.setattr(scanner, "extract_working_copy", fake_extract)

    vireo_dir = tmp_path / "vireo"
    (vireo_dir / "working").mkdir(parents=True)
    dest = str(tmp_path / "archive")

    # Unmount the card BETWEEN copy and end-of-run extraction: patch
    # ``scanner._extract_working_copies`` to unlink card files first,
    # then delegate to the real extractor.
    real_extract_wc = scanner._extract_working_copies

    def unmount_then_extract(*a, **kw):
        for card_file in list(card.iterdir()):
            card_file.unlink()
        card.rmdir()
        return real_extract_wc(*a, **kw)

    monkeypatch.setattr(
        "scanner._extract_working_copies", unmount_then_extract,
    )

    _db, _ws_id, result = _run_import(tmp_path, import_job.ImportParams(
        sources=[str(card)], destination=dest,
        vireo_dir=str(vireo_dir),
    ))

    assert result["copied"] == 1
    assert result["safe_to_format"] is True

    # The extractor was asked to read the archive path (which exists),
    # not the vanished card path. Without the fallback the extractor
    # would read only the dead card path and return False.
    assert calls, "extractor should have run"
    live_reads = [c for c in calls if os.path.isfile(c)]
    assert live_reads, (
        f"extractor should have read a live path (archive fallback), "
        f"got calls={calls}"
    )
    dead_reads = [c for c in calls if str(card) in c]
    assert not dead_reads, (
        f"extractor should not have read the vanished card path, "
        f"got dead reads={dead_reads}"
    )


def test_wc_extraction_ignores_card_override_when_size_no_longer_matches(
        tmp_path, monkeypatch):
    """The override existence check is not enough: if the card was reused
    (mount point holds a different card, or the same file rewritten with
    different content), reading it caches a working copy for the WRONG
    bytes — and because ``working_copy_path`` gets set, normal backfill
    won't regenerate from the archive. The extractor must compare the
    override's on-disk size against the row's file_size and fall back to
    the verified archive copy on mismatch.
    """
    import import_job
    import scanner

    # Card holds a RAW file with a distinctive size.
    card = tmp_path / "card"
    card.mkdir()
    Image.new("RGB", (16, 16), "red").save(str(card / "DSC_2001.jpg"))
    raw_bytes = (card / "DSC_2001.jpg").read_bytes() + b"RAW-SENSOR-DATA"
    (card / "DSC_2001.NEF").write_bytes(raw_bytes)
    (card / "DSC_2001.jpg").unlink()
    ts = datetime(2026, 7, 5, 10, 0, 0).timestamp()
    os.utime(str(card / "DSC_2001.NEF"), (ts, ts))
    original_size = os.path.getsize(str(card / "DSC_2001.NEF"))

    calls = []

    def fake_extract(source_path, output_path, max_size=4096, quality=92):
        calls.append(str(source_path))
        if not os.path.isfile(source_path):
            return False
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, "wb") as f:
            f.write(b"jpeg-bytes")
        return True

    monkeypatch.setattr(scanner, "extract_working_copy", fake_extract)

    vireo_dir = tmp_path / "vireo"
    (vireo_dir / "working").mkdir(parents=True)
    dest = str(tmp_path / "archive")

    # Between copy+catalog and the end-of-run extraction, rewrite the
    # card file with DIFFERENT bytes (and a different size). This mimics
    # the card being reused for a different shoot, or the same file being
    # rewritten by the camera. os.path.isfile still returns True — only
    # a size compare catches it.
    real_extract_wc = scanner._extract_working_copies

    def rewrite_then_extract(*a, **kw):
        card_raw = card / "DSC_2001.NEF"
        card_raw.write_bytes(b"COMPLETELY DIFFERENT CONTENT")
        assert os.path.getsize(str(card_raw)) != original_size
        return real_extract_wc(*a, **kw)

    monkeypatch.setattr(
        "scanner._extract_working_copies", rewrite_then_extract,
    )

    _db, _ws_id, result = _run_import(tmp_path, import_job.ImportParams(
        sources=[str(card)], destination=dest,
        vireo_dir=str(vireo_dir),
    ))

    assert result["copied"] == 1

    # The extractor must have read the ARCHIVE path (verified bytes),
    # never the rewritten card path. Without the size check the extractor
    # would happily read the card's new bytes and cache a wrong working
    # copy — indistinguishable in the WC file from a real success.
    assert calls, "extractor should have run"
    archive_reads = [c for c in calls if str(card) not in c]
    card_reads = [c for c in calls if str(card) in c]
    assert archive_reads, (
        f"extractor should have read from the archive (size mismatch → "
        f"fall back to catalog primary); got calls={calls}"
    )
    assert not card_reads, (
        f"extractor should not have read the rewritten card path (size "
        f"no longer matches file_size); got card reads={card_reads}"
    )


def test_wc_extraction_ignores_card_override_when_mtime_no_longer_matches(
        tmp_path, monkeypatch):
    """Size alone is not enough: a rewritten card file (same byte count,
    different content) OR a reused card mount holding a coincidentally
    same-sized file would pass a size-only guard and cache a working
    copy for the WRONG bytes. mtime narrows trust from "any same-sized
    file at this path" to "the exact file we just copied" — a rewrite
    or a remount presents a different mtime. This test rewrites the
    card RAW between copy and the deferred extraction with EXACTLY the
    same size (identical original bytes shuffled) but a fresh mtime;
    the extractor must fall back to the verified archive copy.
    """
    import import_job
    import scanner

    card = tmp_path / "card"
    card.mkdir()
    Image.new("RGB", (16, 16), "red").save(str(card / "DSC_4001.jpg"))
    raw_bytes = (card / "DSC_4001.jpg").read_bytes() + b"RAW-SENSOR-DATA"
    (card / "DSC_4001.NEF").write_bytes(raw_bytes)
    (card / "DSC_4001.jpg").unlink()
    original_mtime = datetime(2026, 7, 5, 12, 0, 0).timestamp()
    os.utime(str(card / "DSC_4001.NEF"), (original_mtime, original_mtime))
    original_size = os.path.getsize(str(card / "DSC_4001.NEF"))

    calls = []

    def fake_extract(source_path, output_path, max_size=4096, quality=92):
        calls.append(str(source_path))
        if not os.path.isfile(source_path):
            return False
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, "wb") as f:
            f.write(b"jpeg-bytes")
        return True

    monkeypatch.setattr(scanner, "extract_working_copy", fake_extract)

    vireo_dir = tmp_path / "vireo"
    (vireo_dir / "working").mkdir(parents=True)
    dest = str(tmp_path / "archive")

    # Between copy+catalog and the end-of-run extraction, replace the
    # card file with DIFFERENT bytes of the SAME size and set a
    # DIFFERENT mtime. Size-only guards would accept this override; the
    # tightened identity check rejects it because mtime moved.
    real_extract_wc = scanner._extract_working_copies

    def rewrite_same_size_diff_mtime(*a, **kw):
        card_raw = card / "DSC_4001.NEF"
        different_bytes = bytes(reversed(card_raw.read_bytes()))
        card_raw.write_bytes(different_bytes)
        assert os.path.getsize(str(card_raw)) == original_size, (
            "replacement must keep byte count identical"
        )
        new_mtime = datetime(2026, 7, 6, 9, 0, 0).timestamp()
        os.utime(str(card_raw), (new_mtime, new_mtime))
        return real_extract_wc(*a, **kw)

    monkeypatch.setattr(
        "scanner._extract_working_copies", rewrite_same_size_diff_mtime,
    )

    _db, _ws_id, result = _run_import(tmp_path, import_job.ImportParams(
        sources=[str(card)], destination=dest,
        vireo_dir=str(vireo_dir),
    ))

    assert result["copied"] == 1
    assert calls, "extractor should have run"
    archive_reads = [c for c in calls if str(card) not in c]
    card_reads = [c for c in calls if str(card) in c]
    assert archive_reads, (
        f"extractor should have read from the archive (mtime mismatch → "
        f"fall back to catalog primary); got calls={calls}"
    )
    assert not card_reads, (
        f"extractor should not have read the rewritten card path (mtime "
        f"no longer matches captured identity); got card reads={card_reads}"
    )


def test_wc_extraction_ignores_companion_override_when_mtime_changes(
        tmp_path, monkeypatch):
    """RAW+JPEG pair: after copy the RAW's row carries companion_path
    pointing at the JPEG's archive path. The extractor's companion
    fallback used to accept any existing card-side JPEG at the override
    location without identity verification — a rewritten card-side JPEG
    (or a remounted card holding a same-sized JPEG) would then poison
    the RAW's working copy through the RAW-fails-fall-back-to-companion
    path. Identity-checking the companion override against (size, mtime)
    captured at import time makes the extractor read the verified
    archive companion on any mismatch.
    """
    import import_job
    import scanner

    card = tmp_path / "card"
    card.mkdir()
    Image.new("RGB", (16, 16), "blue").save(str(card / "DSC_5001.jpg"))
    raw_bytes = (card / "DSC_5001.jpg").read_bytes() + b"RAW-SENSOR-DATA"
    (card / "DSC_5001.NEF").write_bytes(raw_bytes)
    original_jpeg_bytes = (card / "DSC_5001.jpg").read_bytes()
    original_jpeg_size = len(original_jpeg_bytes)
    original_mtime = datetime(2026, 7, 5, 13, 0, 0).timestamp()
    os.utime(str(card / "DSC_5001.jpg"), (original_mtime, original_mtime))
    os.utime(str(card / "DSC_5001.NEF"), (original_mtime, original_mtime))

    calls = []

    def fake_extract(source_path, output_path, max_size=4096, quality=92):
        calls.append(str(source_path))
        # Force the RAW primary to fail so the extractor falls into the
        # companion-fallback branch — that's the code path where the
        # companion override identity check lives. .NEF is the RAW
        # extension used above.
        if str(source_path).lower().endswith(".nef"):
            return False
        if not os.path.isfile(source_path):
            return False
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, "wb") as f:
            f.write(b"jpeg-bytes")
        return True

    monkeypatch.setattr(scanner, "extract_working_copy", fake_extract)

    vireo_dir = tmp_path / "vireo"
    (vireo_dir / "working").mkdir(parents=True)
    dest = str(tmp_path / "archive")

    # Between copy+catalog and the end-of-run extraction, rewrite the
    # card-side JPEG companion with DIFFERENT bytes of the SAME size and
    # a fresh mtime. Without the identity check the companion-fallback
    # branch would read this poisoned override.
    real_extract_wc = scanner._extract_working_copies

    def rewrite_companion_same_size(*a, **kw):
        card_jpeg = card / "DSC_5001.jpg"
        card_jpeg.write_bytes(bytes(reversed(original_jpeg_bytes)))
        assert os.path.getsize(str(card_jpeg)) == original_jpeg_size, (
            "replacement must keep byte count identical"
        )
        new_mtime = datetime(2026, 7, 6, 10, 0, 0).timestamp()
        os.utime(str(card_jpeg), (new_mtime, new_mtime))
        return real_extract_wc(*a, **kw)

    monkeypatch.setattr(
        "scanner._extract_working_copies", rewrite_companion_same_size,
    )

    _db, _ws_id, result = _run_import(tmp_path, import_job.ImportParams(
        sources=[str(card)], destination=dest,
        vireo_dir=str(vireo_dir),
    ))

    # Both the RAW and its JPEG landed in the archive.
    assert result["copied"] == 2, (
        f"expected RAW + JPEG both landed, got {result!r}"
    )
    assert calls, "extractor should have run"

    # The RAW extraction attempts (both card and archive) fail; the
    # extractor then falls back to the companion. That companion read
    # must be against the ARCHIVE JPEG, not the rewritten card JPEG —
    # the mtime mismatch on the card override forces archive fallback.
    jpeg_calls = [c for c in calls if c.lower().endswith(".jpg")]
    assert jpeg_calls, (
        f"expected companion fallback to run after RAW failure; "
        f"got calls={calls}"
    )
    card_jpeg_reads = [c for c in jpeg_calls if str(card) in c]
    archive_jpeg_reads = [c for c in jpeg_calls if str(card) not in c]
    assert archive_jpeg_reads, (
        f"extractor should have read the archive JPEG (companion mtime "
        f"mismatch → archive companion); got jpeg_calls={jpeg_calls}"
    )
    assert not card_jpeg_reads, (
        f"extractor should not have read the rewritten card JPEG "
        f"companion; got card jpeg reads={card_jpeg_reads}"
    )


def test_wc_extraction_retries_from_archive_when_card_read_fails(
        tmp_path, monkeypatch):
    """The size check can pass (card intact when we peek), then reading
    the file can still fail (transient I/O error, card unmounted right
    after the stat, permission blip). In that case the row would be
    marked ``working_copy_failed_at`` even though the archive copy is
    hash-verified and available; the extractor must retry from the
    catalog primary before giving up.
    """
    import import_job
    import scanner

    card = tmp_path / "card"
    card.mkdir()
    Image.new("RGB", (16, 16), "red").save(str(card / "DSC_3001.jpg"))
    raw_bytes = (card / "DSC_3001.jpg").read_bytes() + b"RAW-SENSOR-DATA"
    (card / "DSC_3001.NEF").write_bytes(raw_bytes)
    (card / "DSC_3001.jpg").unlink()
    ts = datetime(2026, 7, 5, 11, 0, 0).timestamp()
    os.utime(str(card / "DSC_3001.NEF"), (ts, ts))

    card_raw_path = str(card / "DSC_3001.NEF")
    calls = []

    def fake_extract(source_path, output_path, max_size=4096, quality=92):
        calls.append(str(source_path))
        # Card-side read always fails (simulate an unreadable RAW /
        # transient card I/O error); archive-side read succeeds.
        if str(source_path) == card_raw_path:
            return False
        if not os.path.isfile(source_path):
            return False
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, "wb") as f:
            f.write(b"jpeg-bytes")
        return True

    monkeypatch.setattr(scanner, "extract_working_copy", fake_extract)

    vireo_dir = tmp_path / "vireo"
    (vireo_dir / "working").mkdir(parents=True)
    dest = str(tmp_path / "archive")

    db, _ws_id, result = _run_import(tmp_path, import_job.ImportParams(
        sources=[str(card)], destination=dest,
        vireo_dir=str(vireo_dir),
    ))

    assert result["copied"] == 1

    # The extractor tried the card (size matched), failed, then retried
    # from the archive and succeeded. Both reads visible in the call log.
    assert card_raw_path in calls, (
        f"expected the card override to be tried first; got calls={calls}"
    )
    archive_reads = [c for c in calls if c != card_raw_path]
    assert archive_reads, (
        f"expected retry from archive after card read failed; "
        f"got calls={calls}"
    )

    # The photo row must show a successful working_copy_path — the retry
    # from archive worked, so no failure marker.
    row = db.conn.execute(
        "SELECT working_copy_path, working_copy_failed_at FROM photos"
    ).fetchone()
    assert row["working_copy_path"] is not None, (
        "expected working_copy_path set after archive-retry success"
    )
    assert row["working_copy_failed_at"] is None, (
        "expected no failure marker after archive-retry success"
    )


def test_reclassified_landed_entry_skips_card_source_override(
        tmp_path, monkeypatch):
    """A landed entry reclassified as failed by the hash-stamping loop
    (because the archive file was mutated between ``copy_and_hash_verify``
    and the restricted scan) must not contribute a card-side override to
    the deferred ``_extract_working_copies`` pass. Without the filter the
    extractor would cache a working copy from the still-clean card bytes
    onto a photo whose catalog ``file_hash`` describes the mutated archive
    bytes instead — leaving preview/edit renders that don't match the
    archive contents even though the import was reported unsafe.
    """
    import scanner
    from import_job import ImportParams

    # RAW files (.NEF) are WC-extraction candidates regardless of size;
    # tiny JPEGs would be skipped by the working-copy candidate filter,
    # so RAWs are the smallest fixture that actually exercises the
    # extractor. Seed the RAW body with real JPEG bytes plus a trailing
    # tag so the two "RAW"s have distinct content.
    card = tmp_path / "card"
    card.mkdir()
    seed = card / "_seed.jpg"
    Image.new("RGB", (16, 16), "red").save(str(seed))
    seed_bytes = seed.read_bytes()
    seed.unlink()
    for name, mtime in (
        ("DSC_9100.NEF", datetime(2026, 7, 3, 10, 0, 0)),
        ("DSC_9101.NEF", datetime(2026, 7, 3, 11, 0, 0)),
    ):
        (card / name).write_bytes(seed_bytes + name.encode())
        ts = mtime.timestamp()
        os.utime(str(card / name), (ts, ts))

    archive = tmp_path / "archive"
    calls = []

    def fake_extract(source_path, output_path, max_size=4096, quality=92):
        calls.append(str(source_path))
        if not os.path.isfile(source_path):
            return False
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, "wb") as f:
            f.write(b"jpeg-bytes")
        return True

    monkeypatch.setattr(scanner, "extract_working_copy", fake_extract)

    # Wrap ``scanner.scan`` so DSC_9101's archive bytes are mutated
    # after ``copy_and_hash_verify`` succeeded but BEFORE scan reads
    # them. scan() then hashes and records the mutated bytes; the
    # hash-stamping loop's mismatch branch reclassifies DSC_9101 from
    # ``copied`` to ``failed``. DSC_9100 stays untouched and lands
    # cleanly. ``run_import_job`` does ``from scanner import scan``
    # inside the function, so patching on the scanner module is picked
    # up on each invocation (mirrors ``test_dup_link_scan_failure_...``
    # above).
    real_scan = scanner.scan

    def scan_after_mutating(*args, **kwargs):
        for root, _dirs, files in os.walk(str(archive)):
            for name in files:
                if name == "DSC_9101.NEF":
                    with open(os.path.join(root, name), "r+b") as fh:
                        fh.write(b"MUTATED-ARCHIVE-BYTES")
        return real_scan(*args, **kwargs)

    monkeypatch.setattr(scanner, "scan", scan_after_mutating)

    vireo_dir = tmp_path / "vireo"
    (vireo_dir / "working").mkdir(parents=True)

    _db, _ws_id, result = _run_import(tmp_path, ImportParams(
        sources=[str(card)], destination=str(archive),
        vireo_dir=str(vireo_dir),
    ))

    assert result["copied"] == 1, result
    assert result["failed"] == 1, result
    assert result["safe_to_format"] is False
    assert any("DSC_9101" in u["path"] for u in result["unsafe_files"]), (
        f"expected DSC_9101 in unsafe_files: {result['unsafe_files']!r}"
    )

    # DSC_9100 is a successful landing — it should still use its
    # card-side override (the whole point of source_paths). This anchors
    # the negative assertion below: the filter narrows to reclassified
    # entries and does not blanket-drop the override for the batch.
    card_reads_9100 = [
        c for c in calls if "DSC_9100" in c and str(card) in c
    ]
    assert card_reads_9100, (
        f"successful entry should have used card-side override; "
        f"got calls={calls}"
    )

    # DSC_9101 was reclassified: the extractor must NEVER be told to
    # read the still-clean card bytes. It may still read the mutated
    # archive path (that's fine — it matches whatever the catalog now
    # holds), but the card path is off-limits for this row.
    card_reads_9101 = [
        c for c in calls if "DSC_9101" in c and str(card) in c
    ]
    assert not card_reads_9101, (
        f"reclassified entry must not contribute a card override; "
        f"got calls={calls}"
    )


def test_copy_and_hash_verify_falls_back_when_hardlinks_unsupported(
        tmp_path, monkeypatch):
    """Destinations on FAT/exFAT and some SMB/NFS mounts reject os.link
    with EPERM/ENOTSUP. Without a fallback promotion path every file on
    those archives buckets as a copy failure; with the fallback the
    verified temp file lands via an atomic O_EXCL + os.replace and the
    copy succeeds.
    """
    import errno as errno_mod

    from import_dedup import compute_file_hash
    from import_job import copy_and_hash_verify

    src = tmp_path / "card" / "DSC_9500.jpg"
    src.parent.mkdir()
    src.write_bytes(b"card-file-bytes" * 100)
    dst = tmp_path / "archive" / "DSC_9500.jpg"

    def unsupported_link(a, b):
        raise OSError(errno_mod.EPERM, "operation not permitted")

    monkeypatch.setattr("import_job.os.link", unsupported_link)

    ok, file_hash = copy_and_hash_verify(str(src), str(dst))
    assert ok is True
    assert file_hash == compute_file_hash(str(src))
    assert dst.read_bytes() == src.read_bytes()
    # No stray .tmp / empty-placeholder residue.
    assert not list(dst.parent.glob(".DSC_9500.jpg.*.tmp"))


def test_copy_and_hash_verify_fallback_still_refuses_to_overwrite(
        tmp_path, monkeypatch):
    """The O_EXCL fallback must preserve no-overwrite race protection: an
    existing verified archive file must survive when os.link is not
    available, mirroring the primary os.link path's FileExistsError."""
    import errno as errno_mod

    from import_job import copy_and_hash_verify

    src = tmp_path / "card" / "DSC_9501.jpg"
    src.parent.mkdir()
    src.write_bytes(b"card bytes")

    dst = tmp_path / "archive" / "DSC_9501.jpg"
    dst.parent.mkdir()
    dst.write_bytes(b"already-verified archive bytes")

    def unsupported_link(a, b):
        raise OSError(errno_mod.EPERM, "operation not permitted")

    monkeypatch.setattr("import_job.os.link", unsupported_link)

    ok, file_hash = copy_and_hash_verify(str(src), str(dst))
    assert ok is False
    assert file_hash is None
    # The pre-existing verified copy must survive both the os.link race
    # AND the fallback path — the existence check fires and we never
    # touch dst.
    assert dst.read_bytes() == b"already-verified archive bytes"
    # And the temp file must be cleaned up.
    assert not list(dst.parent.glob(".DSC_9501.jpg.*.tmp"))


def test_copy_and_hash_verify_fallback_leaves_no_placeholder_at_dst_on_promote_failure(
        tmp_path, monkeypatch):
    """Crash-safety on hardlinkless destinations: if the promote step
    fails after the temp file has verified, the fallback path must NOT
    leave a zero-byte file at ``dst``. Otherwise the intended archive
    name is occupied by a stray empty file, retry treats it as an
    existing archive, suffixes the real photo to ``name_1.ext``, and
    the invariant that a dead run leaves only valid archive copies or
    hidden temps breaks. See PR #1107 review.
    """
    import errno as errno_mod

    from import_job import copy_and_hash_verify

    src = tmp_path / "card" / "DSC_9502.jpg"
    src.parent.mkdir()
    src.write_bytes(b"card-file-bytes" * 100)
    dst = tmp_path / "archive" / "DSC_9502.jpg"

    def unsupported_link(a, b):
        raise OSError(errno_mod.EPERM, "operation not permitted")

    def failing_rename(a, b):
        raise OSError(errno_mod.EIO, "simulated FS I/O error during promote")

    monkeypatch.setattr("import_job.os.link", unsupported_link)
    monkeypatch.setattr("import_job.os.rename", failing_rename)

    ok, file_hash = copy_and_hash_verify(str(src), str(dst))
    assert ok is False
    assert file_hash is None
    # No zero-byte placeholder at final dst — that was the specific
    # crash-recovery hole this fix closes.
    assert not dst.exists(), (
        f"fallback promote failure must not leave any file at {dst}; "
        f"a zero-byte stray would trip crash-recovery retries"
    )
    # And the hidden temp must be cleaned up too.
    assert not list(dst.parent.glob(".DSC_9502.jpg.*.tmp"))


def test_copy_and_hash_verify_fallback_serializes_via_directory_flock(
        tmp_path, monkeypatch):
    """The hardlinkless-FS fallback wraps its exists-check + rename in a
    ``fcntl.flock`` on the destination directory. Without this, two
    concurrent imports targeting the same date folder could both pass
    exists() before either rename(), and the later rename would silently
    overwrite the first job's already-verified archive copy — its
    ``safe_to_format`` would still report green after its bytes are
    gone. See PR #1107 review.
    """
    import errno as errno_mod

    from import_job import copy_and_hash_verify

    src = tmp_path / "card" / "DSC_9503.jpg"
    src.parent.mkdir()
    src.write_bytes(b"card bytes for lock test" * 50)
    dst = tmp_path / "archive" / "DSC_9503.jpg"

    def unsupported_link(a, b):
        raise OSError(errno_mod.EPERM, "operation not permitted")

    monkeypatch.setattr("import_job.os.link", unsupported_link)

    flock_calls = []

    real_flock = None
    try:
        import fcntl as fcntl_mod
        real_flock = fcntl_mod.flock

        def spy_flock(fd, op):
            flock_calls.append((fd, op))
            return real_flock(fd, op)

        monkeypatch.setattr("import_job.fcntl.flock", spy_flock)
    except ImportError:  # pragma: no cover - Windows
        pass

    ok, _ = copy_and_hash_verify(str(src), str(dst))
    assert ok is True
    # LOCK_EX must have been requested exactly once during the fallback
    # promote critical section — the exists+rename window is serialized.
    if real_flock is not None:
        assert len(flock_calls) == 1, (
            f"expected one flock(LOCK_EX) on fallback path; got {flock_calls}"
        )
        _, op = flock_calls[0]
        assert op == fcntl_mod.LOCK_EX


def test_landed_file_failed_after_scan_is_not_double_counted(
        tmp_path, monkeypatch):
    """A file that lands (copy verifies), then hits a scan/lookup failure
    must move out of copied/skipped_duplicate into failed — otherwise
    ``copied + skipped_duplicate + failed`` exceeds ``discovered`` and the
    exactly-one-terminal-bucket invariant breaks. Simulate a per-batch
    scan failure and check the counts sum to ``discovered``.
    """
    import scanner as scanner_mod
    from import_job import ImportParams, run_import_job

    card = _make_card(tmp_path, [
        ("DSC_1200.jpg", datetime(2026, 7, 6, 10, 0, 0), "red"),
        ("DSC_1201.jpg", datetime(2026, 7, 6, 11, 0, 0), "green"),
    ])
    archive = tmp_path / "archive"

    real_scan = scanner_mod.scan

    def failing_scan(root, db_arg, **kwargs):
        # Fail the restricted (per-batch) scan — the one with
        # restrict_files. The dup-link path (no restrict_files) isn't
        # exercised here; there are no duplicates.
        if kwargs.get("restrict_files"):
            raise OSError("simulated per-batch scan failure")
            # (Not RuntimeError — that's cancellation.)
        return real_scan(root, db_arg, **kwargs)

    monkeypatch.setattr(scanner_mod, "scan", failing_scan)

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    result = run_import_job(
        _make_job(), FakeRunner(), db_path, ws_id,
        ImportParams(sources=[str(card)], destination=str(archive)),
    )

    # The invariant: every discovered file ends in exactly one terminal
    # bucket. Before the fix, copied stayed at 2 and failed also went to
    # 2, giving a sum of 4 > 2 discovered.
    assert result["discovered"] == 2
    assert (
        result["copied"]
        + result["skipped_duplicate"]
        + result["failed"]
    ) == result["discovered"], (
        f"exactly-one-terminal-bucket violated: {result!r}"
    )
    assert result["failed"] == 2
    assert result["copied"] == 0
    assert result["safe_to_format"] is False

    # Folder counts must also be internally consistent.
    for _rel, counts in result["folders"].items():
        assert counts["copied"] >= 0
        assert counts["skipped_duplicate"] >= 0
        assert counts["failed"] >= 0
        assert (
            counts["copied"]
            + counts["skipped_duplicate"]
            + counts["failed"]
        ) == 2, f"folder count sum mismatch: {counts!r}"


def test_import_photos_rejects_case_variant_source_nested_destination(
        tmp_path, monkeypatch):
    """On case-insensitive filesystems (macOS APFS/HFS+, Windows NTFS)
    ``/Volumes/Card`` and ``/volumes/card`` refer to the same directory.
    The API guard against source-contained destinations must compare
    case-folded on those platforms; a naive prefix check would let a
    differently cased spelling slip past and hit the safe-to-format
    data-loss trap.
    """
    import sys

    from db import Database

    # Config isolation — same pattern as vireo/tests/test_app.py.
    monkeypatch.setenv("HOME", str(tmp_path))
    import config as cfg
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))

    # Force the case-insensitive code path regardless of the test host.
    monkeypatch.setattr(sys, "platform", "darwin")

    db_path = str(tmp_path / "test.db")
    thumb_dir = str(tmp_path / "thumbs")
    os.makedirs(thumb_dir)
    d = Database(db_path)
    d.ensure_default_workspace()
    d.close()

    # Set up a plausibly cased source (real dir) and a differently cased
    # destination that resolves under it. We use case-preserving names on
    # the underlying filesystem; ``realpath`` won't rewrite the case on
    # Linux, so the case-fold comparison is what catches the containment.
    source = tmp_path / "Card"
    source.mkdir()
    dest_inside = str(source).replace("Card", "card") + "/archive"

    from app import create_app

    app = create_app(db_path=db_path, thumb_cache_dir=thumb_dir)
    with app.test_client() as client:
        resp = client.post(
            "/api/jobs/import-photos",
            json={
                "sources": [str(source)],
                "destination": dest_inside,
            },
        )

    assert resp.status_code == 400, resp.get_data(as_text=True)
    payload = resp.get_json()
    assert "inside a source" in (payload.get("error") or "")


def test_crash_recovered_suffix_is_adopted_not_re_copied(tmp_path):
    """When a prior run copied a different-content collision to
    ``DSC_XXX.jpg`` and this source's bytes to the suffixed ``DSC_XXX_1.jpg``
    then died before scan, a retry must hash-match every existing suffix
    candidate and adopt on a match — not advance past it and re-copy to
    ``DSC_XXX_2.jpg``. Without the hash-match, the archive would carry two
    byte-identical copies of the same source photo.
    """
    import shutil

    from import_job import ImportParams

    card = _make_card(tmp_path, [
        ("DSC_0060.jpg", datetime(2026, 7, 3, 10, 0, 0), "yellow"),
    ])
    archive = tmp_path / "archive"
    dest_dir = archive / "2026" / "2026-07-03"
    dest_dir.mkdir(parents=True)

    # A different-content name-collision from an earlier run (some other
    # source photo happened to share the filename+date). Not this card's
    # bytes.
    Image.new("RGB", (16, 16), "blue").save(str(dest_dir / "DSC_0060.jpg"))
    # THIS card's bytes, landed at the suffixed name by a prior run that
    # died before its restricted scan.
    shutil.copy2(str(card / "DSC_0060.jpg"), str(dest_dir / "DSC_0060_1.jpg"))

    db, ws_id, result = _run_import(tmp_path, ImportParams(
        sources=[str(card)], destination=str(archive),
    ))

    # Adopted the crash-recovered suffix — no re-copy, no double.
    assert result["copied"] == 0
    assert result["skipped_duplicate"] == 1
    assert result["failed"] == 0
    assert result["safe_to_format"] is True

    # Only the two files that existed before + no DSC_0060_2.jpg.
    files_on_disk = sorted(os.listdir(str(dest_dir)))
    assert files_on_disk == ["DSC_0060.jpg", "DSC_0060_1.jpg"], files_on_disk

    # The adopted suffix is cataloged with hash_status=ok. (The pre-existing
    # ``DSC_0060.jpg`` with different bytes is a stray outside this import;
    # a future full scan would catalog it — out of scope for this run.)
    rows = _photo_rows(db)
    adopted = [r for r in rows if r["filename"] == "DSC_0060_1.jpg"]
    assert len(adopted) == 1
    assert adopted[0]["hash_status"] == "ok"


def test_paired_companion_archive_mutation_after_scan_reclassifies(
        tmp_path, monkeypatch):
    """The RAW+JPEG pair-merge in ``scanner.scan()`` deletes the JPEG's own
    photo row. Before this fix, the import job's hash-stamping loop
    accepted that as success without re-reading the archive JPEG. If the
    archive JPEG gets rewritten or corrupted between promote and the
    stamping check, ``safe_to_format`` could still go green over bytes we
    never verified. Simulate archive-side mutation of the paired JPEG and
    require the import to reclassify it to failed.
    """
    import scanner as scanner_mod
    from import_job import ImportParams, run_import_job

    # Card carries a RAW+JPEG pair sharing the base name. Same shape as
    # ``test_working_copy_companion_fallback_reads_card_jpeg``: the "RAW"
    # is opaque bytes with a .NEF extension (scanner sniffs by extension,
    # not content), so scan()'s pair-merge deletes the JPEG's photo row
    # and sets companion_path on the RAW primary.
    card = tmp_path / "card"
    card.mkdir()
    Image.new("RGB", (16, 16), "red").save(str(card / "DSC_2000.jpg"))
    raw_bytes = (card / "DSC_2000.jpg").read_bytes() + b"RAW-SENSOR-DATA"
    (card / "DSC_2000.NEF").write_bytes(raw_bytes)
    ts = datetime(2026, 7, 3, 10, 0, 0).timestamp()
    for name in ("DSC_2000.jpg", "DSC_2000.NEF"):
        os.utime(str(card / name), (ts, ts))
    archive = tmp_path / "archive"

    real_scan = scanner_mod.scan

    def mutating_scan(root, db_arg, **kwargs):
        result = real_scan(root, db_arg, **kwargs)
        # AFTER cataloging + pairing but before import_job's hash-stamping
        # loop runs, mutate the archive JPEG. If the fix works this
        # forces the JPEG entry into ``failed``; without it, the JPEG's
        # deleted-by-pair row makes the check silently pass.
        for f in kwargs.get("restrict_files") or set():
            if str(f).lower().endswith("dsc_2000.jpg") and os.path.exists(f):
                with open(f, "r+b") as fh:
                    fh.write(b"CORRUPT-MUTATION-POST-COPY")
        return result

    monkeypatch.setattr(scanner_mod, "scan", mutating_scan)

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    result = run_import_job(
        _make_job(), FakeRunner(), db_path, ws_id,
        ImportParams(sources=[str(card)], destination=str(archive)),
    )

    # The JPEG bytes on disk no longer match copy_and_hash_verify's
    # verified hash — the import must NOT report safe.
    assert result["safe_to_format"] is False
    assert result["failed"] >= 1
    # Terminal-bucket invariant still holds.
    assert (
        result["copied"]
        + result["skipped_duplicate"]
        + result["failed"]
    ) == result["discovered"]
    # The specific failure names the JPEG.
    unsafe_paths = [u["path"] for u in result["unsafe_files"]]
    assert any("DSC_2000.jpg" in p for p in unsafe_paths), unsafe_paths


def test_non_empty_null_scan_hash_reclassifies_when_rehash_disagrees(
        tmp_path, monkeypatch):
    """When ``scanner.scan()`` writes a photo row for a non-empty file but
    leaves ``file_hash`` NULL (its own hash read failed between promote
    and scan), the import job must NOT stamp the copy-time hash and call
    it verified. Re-hashing the archive path is the last check — if it
    also disagrees (file mutated or unreadable), the entry must be
    reclassified to failed. Simulate that shape and require the ledger
    to bucket the file as failed rather than reporting safe.
    """
    import scanner as scanner_mod
    from import_job import ImportParams, run_import_job

    card = _make_card(tmp_path, [
        ("DSC_3000.jpg", datetime(2026, 7, 3, 10, 0, 0), "purple"),
    ])
    archive = tmp_path / "archive"

    real_scan = scanner_mod.scan

    def sabotaging_scan(root, db_arg, **kwargs):
        result = real_scan(root, db_arg, **kwargs)
        # Wipe file_hash for freshly cataloged photos (simulating a
        # scan-side hash-read failure that landed a NULL) AND mutate the
        # archive so a re-hash also disagrees.
        if kwargs.get("restrict_files"):
            for f in kwargs["restrict_files"]:
                if not os.path.exists(f):
                    continue
                with open(f, "r+b") as fh:
                    fh.write(b"POST-SCAN-MUTATION-NULL-HASH")
                db_arg.conn.execute(
                    """UPDATE photos SET file_hash = NULL
                       WHERE filename = ?""",
                    (os.path.basename(f),),
                )
                db_arg.conn.commit()
        return result

    monkeypatch.setattr(scanner_mod, "scan", sabotaging_scan)

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    result = run_import_job(
        _make_job(), FakeRunner(), db_path, ws_id,
        ImportParams(sources=[str(card)], destination=str(archive)),
    )

    assert result["safe_to_format"] is False
    assert result["failed"] == 1
    assert result["copied"] == 0
    assert (
        result["copied"]
        + result["skipped_duplicate"]
        + result["failed"]
    ) == result["discovered"]
    unsafe_paths = [u["path"] for u in result["unsafe_files"]]
    assert any("DSC_3000.jpg" in p for p in unsafe_paths), unsafe_paths


def test_dest_file_nested_under_source_is_rejected(tmp_path):
    """When the destination is a legal ancestor of a source but the folder
    template maps the source right back INTO the source tree, ``dest_file``
    is a different path than the source file (samefile is False) but still
    lives under the card. Copying there is counted as ``copied``,
    ``safe_to_format`` can go green, and formatting the card also erases
    the "archive" copy. The import job must reject that overlap even
    though the two paths are not the same file. See PR #1107 review.
    """
    from import_job import ImportParams

    # Source is /volumes/Card/DCIM (with photos directly in it); the
    # destination is /volumes/Card and the folder template ``DCIM/Archive/
    # %Y`` maps the source back into itself: dest_file lives at
    # /volumes/Card/DCIM/Archive/2026/<name>, which is under the source
    # but is not the source file.
    card = tmp_path / "volumes" / "Card"
    dcim = card / "DCIM"
    dcim.mkdir(parents=True)
    src_file = dcim / "DSC_5000.jpg"
    Image.new("RGB", (16, 16), "goldenrod").save(str(src_file))
    ts = datetime(2026, 7, 5, 10, 0, 0).timestamp()
    os.utime(str(src_file), (ts, ts))
    original_bytes = src_file.read_bytes()

    db, ws_id, result = _run_import(tmp_path, ImportParams(
        sources=[str(dcim)],
        destination=str(card),
        folder_template="DCIM/Archive/%Y",
    ))

    # The source bytes MUST still be on disk.
    assert src_file.exists()
    assert src_file.read_bytes() == original_bytes
    # The nested "archive" copy MUST NOT have been created.
    assert not (dcim / "Archive" / "2026" / "DSC_5000.jpg").exists()
    # It must NOT be counted as copied/skipped; it must be failed.
    assert result["copied"] == 0
    assert result["skipped_duplicate"] == 0
    assert result["failed"] == 1
    # Safe-to-format MUST NOT go green when the archive would live on
    # the card being imported.
    assert result["safe_to_format"] is False
    assert (
        result["copied"]
        + result["skipped_duplicate"]
        + result["failed"]
    ) == result["discovered"]
    unsafe_paths = [u["path"] for u in result["unsafe_files"]]
    assert any("DSC_5000.jpg" in p for p in unsafe_paths), unsafe_paths


def test_batch_destination_under_source_creates_no_directories(tmp_path):
    """When ``dest_folder`` for a batch resolves under a source root, the
    per-file loop rejects each ``dest_file`` — but that check only runs
    AFTER ``os.makedirs(dest_folder)``, which would still materialize the
    archive directory tree on the card (and raise on read-only removable
    media, killing the background job with an uncaught OSError instead of
    returning a controlled unsafe result). The import job must short-circuit
    at the batch boundary: nothing under any source ever gets created, and
    the run returns the same controlled ``failed``/``safe_to_format=False``
    ledger it would for a writable destination inside the card. See PR
    #1107 review.
    """
    from import_job import ImportParams

    # Source ``/volumes/Card/DCIM``; destination ``/volumes/Card`` +
    # template ``DCIM/Archive/%Y`` puts the batch's dest_folder at
    # ``/volumes/Card/DCIM/Archive/2026`` — under the source root.
    card = tmp_path / "volumes" / "Card"
    dcim = card / "DCIM"
    dcim.mkdir(parents=True)
    src_file = dcim / "DSC_6100.jpg"
    Image.new("RGB", (16, 16), "olive").save(str(src_file))
    ts = datetime(2026, 7, 5, 10, 0, 0).timestamp()
    os.utime(str(src_file), (ts, ts))

    db, ws_id, result = _run_import(tmp_path, ImportParams(
        sources=[str(dcim)],
        destination=str(card),
        folder_template="DCIM/Archive/%Y",
    ))

    # No batch directory was created on the card.
    assert not (dcim / "Archive").exists()
    # Same controlled unsafe result as the per-file case.
    assert result["failed"] == 1
    assert result["copied"] == 0
    assert result["skipped_duplicate"] == 0
    assert result["safe_to_format"] is False
    assert result["ok"] is False
    unsafe_paths = [u["path"] for u in result["unsafe_files"]]
    assert any("DSC_6100.jpg" in p for p in unsafe_paths), unsafe_paths


def test_full_coverage_file_types_list_is_treated_as_unfiltered(tmp_path):
    """The pipeline UI's ``getIngestFileTypes()`` returns a list of every
    supported extension when the user checks every box. Semantically
    that is the same as ``file_types="both"``: ``discover_source_files``
    walks it identically. Flagging any list as ``partial_scope``
    therefore leaves ``safe_to_format`` permanently false over an
    unfiltered import even though every card file was verified — the
    pill would deceive the user, ``COPY_PHILOSOPHY.md``'s "show the
    user what's happening" contract. Normalize full-coverage lists to
    the same status as ``"both"``. See PR #1107 review.
    """
    from image_loader import SUPPORTED_EXTENSIONS
    from import_job import ImportParams

    card = _make_card(tmp_path, [
        ("DSC_0500.jpg", datetime(2026, 7, 3, 10, 0, 0), "coral"),
    ])
    archive = tmp_path / "archive"

    # A list covering every supported extension — what the UI sends when
    # the user checks every filetype box, including some casing variance
    # to guarantee the normalization path is exercised.
    full_list = sorted(SUPPORTED_EXTENSIONS)
    full_list[0] = full_list[0].upper()  # e.g. ".ARW"
    full_list.append("jpg")              # no-leading-dot alias for coverage

    db, ws_id, result = _run_import(tmp_path, ImportParams(
        sources=[str(card)],
        destination=str(archive),
        file_types=full_list,
    ))

    assert result["copied"] == 1
    assert result["failed"] == 0
    # Full-coverage list did NOT actually narrow the walk, so
    # safe_to_format may go green just like ``"both"``.
    assert result["safe_to_format"] is True


def test_source_equals_dest_file_is_rejected(tmp_path):
    """When a source lives under the destination and the folder template
    maps it back to the same directory, dest_file resolves to the source
    file itself. The adopt branch would hash the file against itself and
    count it as ``skipped_duplicate`` with safe_to_format=True — then
    formatting/erasing the source erases the only copy. The import job
    must reject that overlap at the worker level (the API rejects
    ``destination inside source`` but not the reverse). See PR #1107 review.
    """
    from import_job import ImportParams

    # Source is /archive/2026/2026-07-05; destination is /archive with the
    # default %Y/%Y-%m-%d template → dest_folder becomes 2026/2026-07-05,
    # so dest_file IS the source file.
    archive = tmp_path / "archive"
    day = archive / "2026" / "2026-07-05"
    day.mkdir(parents=True)
    from PIL import Image
    src_file = day / "DSC_4000.jpg"
    Image.new("RGB", (16, 16), "coral").save(str(src_file))
    ts = datetime(2026, 7, 5, 10, 0, 0).timestamp()
    os.utime(str(src_file), (ts, ts))
    original_bytes = src_file.read_bytes()

    db, ws_id, result = _run_import(tmp_path, ImportParams(
        sources=[str(day)], destination=str(archive),
    ))

    # The source bytes MUST still be on disk (nothing was moved/deleted).
    assert src_file.exists()
    assert src_file.read_bytes() == original_bytes
    # It must NOT be counted as skipped_duplicate; it must be failed.
    assert result["skipped_duplicate"] == 0
    assert result["failed"] == 1
    assert result["copied"] == 0
    # Safe-to-format must NOT go green when the source == dest.
    assert result["safe_to_format"] is False
    assert (
        result["copied"]
        + result["skipped_duplicate"]
        + result["failed"]
    ) == result["discovered"]
    # The failure specifically names the file.
    unsafe_paths = [u["path"] for u in result["unsafe_files"]]
    assert any("DSC_4000.jpg" in p for p in unsafe_paths), unsafe_paths


def test_import_invalidates_new_images_cache(tmp_path):
    """After per-batch and dup-link scans, run_import_job must invalidate
    the /new-images cache for the touched destination folders. Otherwise
    a workspace whose cache was warm before the import keeps reporting
    the just-imported files as new until TTL expires or another full
    scan runs. Mirrors the try/finally in api_job_scan / api_job_import_full
    / pipeline_job. See PR #1107 review.
    """
    from import_job import ImportParams

    card = _make_card(tmp_path, [
        ("DSC_5000.jpg", datetime(2026, 7, 3, 10, 0, 0), "teal"),
    ])
    archive = tmp_path / "archive"

    # Prime the cache with a sentinel value for the active workspace so
    # we can observe invalidation without racing an actual /new-images
    # walk. If run_import_job invalidates correctly, the sentinel is
    # gone by the time the job returns.
    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    db._new_images_cache.set(
        db_path, ws_id, {"new_count": 999, "sample": []},
    )
    assert db._new_images_cache.get(db_path, ws_id) is not None

    from import_job import run_import_job
    result = run_import_job(
        _make_job(), FakeRunner(), db_path, ws_id,
        ImportParams(sources=[str(card)], destination=str(archive)),
    )
    assert result["copied"] == 1  # sanity: the import actually ran

    # The restricted scan invalidation must have cleared the sentinel for
    # the workspace linked to the dest_folder.
    assert db._new_images_cache.get(db_path, ws_id) is None, (
        "run_import_job must invalidate the new-images cache for the "
        "workspace linked to the destination folder after its restricted "
        "scans (mirrors pipeline_job / api_job_scan / api_job_import_full)"
    )


def test_import_promotes_missing_destination_folder_to_ok(tmp_path):
    """A pre-existing ``folders`` row marked ``'missing'`` for the import's
    destination path must transition back to ``'ok'`` before the batch
    scan runs — otherwise workspace queries filter the folder out and the
    just-imported files never appear in the workspace, even though
    safe_to_format goes green.

    Standalone scans run ``check_folder_health()`` as their preflight,
    which handles this globally. The import path calls ``scanner.scan()``
    directly, and scan's success stamp only clears ``'partial'``. So a
    reattached archive drive whose folder rows still say ``'missing'``
    stays invisible after a successful import unless the import job
    itself promotes the row. See PR #1107 review.
    """
    from import_job import ImportParams

    card = _make_card(tmp_path, [
        ("DSC_0500.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
    ])
    archive = tmp_path / "archive"
    dest_dir = archive / "2026" / "2026-07-03"
    dest_dir.mkdir(parents=True)

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    # Pre-existing missing row for the same path that this import will
    # populate (simulates: archive drive was disconnected during a health
    # check and got reattached before this import).
    db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES (?, ?, 'missing')",
        (str(dest_dir), dest_dir.name),
    )
    db.conn.commit()

    from import_job import run_import_job
    result = run_import_job(_make_job(), FakeRunner(), db_path, ws_id,
                            ImportParams(sources=[str(card)],
                                         destination=str(archive)))
    assert result["copied"] == 1
    assert result["safe_to_format"] is True

    status = db.conn.execute(
        "SELECT status FROM folders WHERE path = ?", (str(dest_dir),),
    ).fetchone()["status"]
    assert status == "ok", (
        "import must promote pre-existing missing folder row to 'ok' so "
        "the imported files are visible in the workspace"
    )


def test_import_missing_promotion_narrow_status_guard(tmp_path):
    """The missing→ok promotion must be gated on ``status='missing'`` so it
    can't clobber other statuses. (``'partial'`` gets cleared by
    scanner's success stamp anyway, but the pre-scan targeted UPDATE
    must not overreach — that's what the ``AND status = 'missing'``
    guard is for.)"""
    from import_job import ImportParams

    card = _make_card(tmp_path, [
        ("DSC_0600.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
    ])
    archive = tmp_path / "archive"
    dest_dir = archive / "2026" / "2026-07-03"
    dest_dir.mkdir(parents=True)

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    # A sibling folder marked 'missing' whose path is NOT this import's
    # destination must NOT be promoted — the pre-scan UPDATE is targeted
    # by path, so unrelated rows stay untouched.
    other_dir = archive / "2025" / "2025-01-01"
    db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES (?, ?, 'missing')",
        (str(other_dir), other_dir.name),
    )
    db.conn.commit()

    from import_job import run_import_job
    run_import_job(_make_job(), FakeRunner(), db_path, ws_id,
                   ImportParams(sources=[str(card)], destination=str(archive)))

    status = db.conn.execute(
        "SELECT status FROM folders WHERE path = ?", (str(other_dir),),
    ).fetchone()["status"]
    assert status == "missing", (
        "the missing→ok promotion must be scoped to this batch's dest_folder"
    )


def test_duplicate_only_import_promotes_missing_twin_folder(tmp_path):
    """A duplicate-only import that matches a cataloged twin whose folder
    row is stale-marked ``'missing'`` (but the folder is still on disk
    under the import destination) must promote that folder to ``'ok'``
    before its dup-link scan runs — otherwise the archive stays filtered
    out of workspace queries even though safe_to_format goes green.
    See PR #1107 review.
    """
    from import_dedup import compute_file_hash
    from import_job import ImportParams

    archive = tmp_path / "archive"
    twin_dir = archive / "2026" / "2026-07-03"
    twin_dir.mkdir(parents=True)
    twin_file = twin_dir / "IMG_0300.jpg"
    Image.new("RGB", (16, 16), "red").save(str(twin_file))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    # Pre-catalog the twin at MISSING status (simulates a health check
    # that flipped the folder to missing right before a reattach).
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES (?, ?, 'missing')",
        (str(twin_dir), twin_dir.name),
    ).lastrowid
    db.conn.execute(
        "INSERT INTO photos (folder_id, filename, extension, file_size,"
        " file_hash) VALUES (?, ?, '.jpg', ?, ?)",
        (
            fid,
            "IMG_0300.jpg",
            os.path.getsize(str(twin_file)),
            compute_file_hash(str(twin_file)),
        ),
    )
    db.conn.commit()

    card = tmp_path / "card"
    card.mkdir()
    import shutil
    shutil.copy2(str(twin_file), str(card / "IMG_0300.jpg"))

    from import_job import run_import_job
    result = run_import_job(_make_job(), FakeRunner(), db_path, ws_id,
                            ImportParams(sources=[str(card)],
                                         destination=str(archive)))
    assert result["copied"] == 0
    assert result["skipped_duplicate"] == 1
    assert result["failed"] == 0
    assert result["safe_to_format"] is True

    # Folder status was promoted so workspace queries can see it.
    status = db.conn.execute(
        "SELECT status FROM folders WHERE path = ?", (str(twin_dir),),
    ).fetchone()["status"]
    assert status == "ok", (
        "duplicate-only import must promote a matched 'missing'-marked "
        "twin folder to 'ok' before its dup-link scan runs"
    )
    # And the folder is linked to the active workspace.
    assert str(twin_dir) in _ws_linked_folder_paths(db, ws_id)


def test_duplicate_only_import_links_twin_folder_when_destination_is_symlink(tmp_path):
    """A duplicate-only import whose ``destination`` is a symlink to the
    twin's on-disk archive root must still resolve containment through
    the link and link the twin folder. A lexical prefix check would drop
    the twin from ``dup_dirs``, the duplicate-link scan would never run,
    and the imported duplicate would stay filtered out of the active
    workspace even though safe_to_format flipped green. See PR #1107
    review.
    """
    import pytest
    from import_dedup import compute_file_hash
    from import_job import ImportParams, run_import_job

    real_archive = tmp_path / "real_archive"
    twin_dir = real_archive / "2026" / "2026-07-03"
    twin_dir.mkdir(parents=True)
    twin_file = twin_dir / "IMG_0400.jpg"
    Image.new("RGB", (16, 16), "red").save(str(twin_file))

    # Destination the user hands to the import is a symlink to the real
    # archive root. The twin folder was cataloged under its real path.
    alias_archive = tmp_path / "archive-alias"
    try:
        os.symlink(str(real_archive), str(alias_archive), target_is_directory=True)
    except (OSError, NotImplementedError):
        pytest.skip("symlink creation not supported on this platform")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES (?, ?, 'ok')",
        (str(twin_dir), twin_dir.name),
    ).lastrowid
    db.conn.execute(
        "INSERT INTO photos (folder_id, filename, extension, file_size,"
        " file_hash) VALUES (?, ?, '.jpg', ?, ?)",
        (
            fid,
            "IMG_0400.jpg",
            os.path.getsize(str(twin_file)),
            compute_file_hash(str(twin_file)),
        ),
    )
    db.conn.commit()
    # Nothing linked before the run: proves the run must do the linking.
    assert str(twin_dir) not in _ws_linked_folder_paths(db, ws_id)

    card = tmp_path / "card"
    card.mkdir()
    import shutil
    shutil.copy2(str(twin_file), str(card / "IMG_0400.jpg"))

    result = run_import_job(
        _make_job(), FakeRunner(), db_path, ws_id,
        ImportParams(sources=[str(card)], destination=str(alias_archive)),
    )

    assert result["copied"] == 0
    assert result["skipped_duplicate"] == 1
    assert result["failed"] == 0
    assert result["safe_to_format"] is True
    # The twin folder was scanned + linked despite the destination being
    # a symlink to (not literally equal to) the twin's cataloged root.
    assert str(twin_dir) in _ws_linked_folder_paths(db, ws_id)


def test_import_invalidates_derived_caches_on_content_change(tmp_path):
    """When a landed file replaces bytes at a path whose catalog row already
    has ``working_copy_path`` set (from a prior scan of an older archive
    file at the same path), the import must invalidate that WC — the
    deferred end-of-run ``_extract_working_copies`` skips rows with
    ``working_copy_path IS NOT NULL``, so without invalidation the WC
    persists pointing at bytes the archive no longer holds. See PR #1107
    review.
    """
    from import_dedup import compute_file_hash
    from import_job import ImportParams

    archive = tmp_path / "archive"
    dest_dir = archive / "2026" / "2026-07-03"
    dest_dir.mkdir(parents=True)
    # A stale archive file present before the import; its catalog row
    # captures its OLD hash + a fake WC path (as if a prior scan
    # extracted a WC for it).
    stale_archive = dest_dir / "DSC_0700.jpg"
    Image.new("RGB", (16, 16), "blue").save(str(stale_archive))
    stale_hash = compute_file_hash(str(stale_archive))

    vireo_dir = tmp_path / "vireo_data"
    (vireo_dir / "working").mkdir(parents=True)
    fake_wc = vireo_dir / "working" / "1.jpg"
    Image.new("RGB", (8, 8), "yellow").save(str(fake_wc))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES (?, ?, 'ok')",
        (str(dest_dir), dest_dir.name),
    ).lastrowid
    photo_id = db.conn.execute(
        "INSERT INTO photos (folder_id, filename, extension, file_size,"
        " file_hash, working_copy_path) VALUES (?, ?, '.jpg', ?, ?, ?)",
        (
            fid,
            "DSC_0700.jpg",
            os.path.getsize(str(stale_archive)),
            stale_hash,
            str(fake_wc),
        ),
    ).lastrowid
    db.conn.commit()

    # Overwrite the archive file with DIFFERENT bytes (simulates: the
    # archive file was deleted/replaced between the prior scan and this
    # import, and the import restores the same filename with new bytes).
    stale_archive.unlink()

    # Card holds the NEW bytes at the same filename/date, which will land
    # at the same dest_path.
    card = _make_card(tmp_path, [
        ("DSC_0700.jpg", datetime(2026, 7, 3, 10, 0, 0), "green"),
    ])
    # Force skip_duplicates False so the card's new bytes actually get
    # copied over even though the stale row's hash still exists.
    from import_job import run_import_job
    result = run_import_job(_make_job(), FakeRunner(), db_path, ws_id,
                            ImportParams(sources=[str(card)],
                                         destination=str(archive),
                                         skip_duplicates=False,
                                         vireo_dir=str(vireo_dir)))
    assert result["copied"] == 1
    assert result["failed"] == 0

    # The row's WC path was cleared so the deferred extractor / later
    # backfill can rebuild against the new archive bytes.
    row = db.conn.execute(
        "SELECT working_copy_path FROM photos WHERE id = ?", (photo_id,),
    ).fetchone()
    assert row["working_copy_path"] is None, (
        "content change on a landed row must clear working_copy_path so "
        "the deferred WC pass rebuilds it against the new archive bytes"
    )
    # The stale WC file was also unlinked from disk.
    assert not fake_wc.exists(), (
        "content change on a landed row must delete the stale WC file"
    )


def test_import_invalidates_derived_caches_when_pre_row_had_null_hash(tmp_path):
    """Legacy row invariant: a pre-scan row with ``file_hash IS NULL`` can
    still carry ``working_copy_path``/thumb/preview caches from earlier
    processing (e.g. a prior scan that couldn't read the file cleared the
    hash but left derived rows). Scanner's own content-change path treats
    ``NULL -> concrete hash`` as an invalidating transition; the import
    per-batch invalidation loop must mirror that, or restoring a deleted
    archive file at that path leaves stale WC/thumb bytes cached against
    the fresh hash. See PR #1107 review.
    """
    from import_job import ImportParams, run_import_job

    archive = tmp_path / "archive"
    dest_dir = archive / "2026" / "2026-07-03"
    dest_dir.mkdir(parents=True)

    vireo_dir = tmp_path / "vireo_data"
    (vireo_dir / "working").mkdir(parents=True)
    fake_wc = vireo_dir / "working" / "1.jpg"
    Image.new("RGB", (8, 8), "yellow").save(str(fake_wc))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES (?, ?, 'ok')",
        (str(dest_dir), dest_dir.name),
    ).lastrowid
    # Legacy-shaped row: no file on disk, ``file_hash IS NULL``, but a
    # stale ``working_copy_path`` from an earlier processing pass.
    photo_id = db.conn.execute(
        "INSERT INTO photos (folder_id, filename, extension, file_size,"
        " file_hash, working_copy_path) VALUES (?, ?, '.jpg', ?, NULL, ?)",
        (fid, "DSC_0701.jpg", 12345, str(fake_wc)),
    ).lastrowid
    db.conn.commit()

    # Card holds the NEW bytes at the same filename/date — the import
    # will land them at the archive path whose row currently has
    # ``file_hash IS NULL`` + a stale WC.
    card = _make_card(tmp_path, [
        ("DSC_0701.jpg", datetime(2026, 7, 3, 10, 0, 0), "green"),
    ])
    result = run_import_job(_make_job(), FakeRunner(), db_path, ws_id,
                            ImportParams(sources=[str(card)],
                                         destination=str(archive),
                                         skip_duplicates=False,
                                         vireo_dir=str(vireo_dir)))
    assert result["copied"] == 1
    assert result["failed"] == 0

    # The row's WC path was cleared: NULL -> concrete hash is a real
    # content change, and the deferred extractor / later backfill must be
    # allowed to rebuild the WC against the just-imported archive bytes.
    row = db.conn.execute(
        "SELECT working_copy_path FROM photos WHERE id = ?", (photo_id,),
    ).fetchone()
    assert row["working_copy_path"] is None, (
        "NULL-hash pre-scan row must invalidate its stale derived caches "
        "when the import stamps a concrete hash (mirrors scanner.scan()'s "
        "content-change path)"
    )
    assert not fake_wc.exists(), (
        "NULL-hash pre-scan row must have its stale WC file unlinked"
    )


def test_import_invalidates_raw_caches_when_new_jpeg_pairs(tmp_path):
    """RAW+JPEG companion restore: when a freshly copied JPEG lands as
    companion to an existing RAW row (pair-merge deletes the JPEG's own
    row), the RAW row's derived caches may reflect the pre-pair state
    (RAW-only preview, or a deleted/replaced prior companion). The
    hash-stamping loop treats ``row is None`` as a fresh insert with no
    diff to invalidate, so without an explicit companion-invalidation
    pass the deferred WC pass skips the RAW (working_copy_path is set)
    and the UI keeps serving stale derived files. See PR #1107 review.
    """
    from import_job import ImportParams, run_import_job

    archive = tmp_path / "archive"
    dest_dir = archive / "2026" / "2026-07-03"
    dest_dir.mkdir(parents=True)

    vireo_dir = tmp_path / "vireo_data"
    (vireo_dir / "working").mkdir(parents=True)

    # Pre-existing RAW file at the archive path, cataloged standalone
    # with a stale working_copy_path from a prior RAW-only extraction.
    raw_archive = dest_dir / "DSC_0800.NEF"
    Image.new("RGB", (16, 16), "red").save(str(dest_dir / "_seed.jpg"))
    raw_bytes = (dest_dir / "_seed.jpg").read_bytes() + b"RAW-SENSOR-DATA"
    raw_archive.write_bytes(raw_bytes)
    (dest_dir / "_seed.jpg").unlink()

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES (?, ?, 'ok')",
        (str(dest_dir), dest_dir.name),
    ).lastrowid
    # WC file must live at working/{photo_id}.jpg — that's the layout
    # _invalidate_derived_caches unlinks.
    raw_photo_id = db.conn.execute(
        "INSERT INTO photos (folder_id, filename, extension, file_size,"
        " file_hash, working_copy_path) VALUES (?, ?, '.nef', ?, ?, 'placeholder')",
        (fid, "DSC_0800.NEF", len(raw_bytes),
         "deadbeef" * 8),
    ).lastrowid
    fake_wc = vireo_dir / "working" / f"{raw_photo_id}.jpg"
    Image.new("RGB", (8, 8), "orange").save(str(fake_wc))
    stale_wc_bytes = fake_wc.read_bytes()
    db.conn.execute(
        "UPDATE photos SET working_copy_path = ? WHERE id = ?",
        (str(fake_wc), raw_photo_id),
    )
    db.conn.commit()

    # Card holds a NEW JPEG that will land at DSC_0800.jpg and pair with
    # the existing RAW during the batch scan.
    card = _make_card(tmp_path, [
        ("DSC_0800.jpg", datetime(2026, 7, 3, 10, 0, 0), "green"),
    ])

    result = run_import_job(_make_job(), FakeRunner(), db_path, ws_id,
                            ImportParams(sources=[str(card)],
                                         destination=str(archive),
                                         skip_duplicates=False,
                                         vireo_dir=str(vireo_dir)))
    assert result["copied"] == 1
    assert result["failed"] == 0

    # The RAW row's stale WC path was cleared (invalidation ran) and
    # the on-disk stale WC file was unlinked. The deferred end-of-run
    # ``_extract_working_copies`` then either succeeds with a fresh WC
    # (path differs from the stale one) or leaves working_copy_path
    # NULL for the scanner's later backfill; either way the row no
    # longer points at the pre-pair bytes.
    row = db.conn.execute(
        "SELECT working_copy_path, companion_path FROM photos WHERE id = ?",
        (raw_photo_id,),
    ).fetchone()
    assert row["companion_path"] == "DSC_0800.jpg", (
        "pair-merge must record the newly landed JPEG as the RAW's "
        "companion_path"
    )
    # If invalidation didn't run the row would still point at the
    # pre-pair WC path (which the extractor's candidate predicate would
    # then skip, since working_copy_path is set). Invalidation resets
    # the path, and the deferred WC pass rebuilds fresh: even when the
    # extractor happens to reuse the same on-disk slot
    # (``working/{id}.jpg``), the bytes at that path must differ from
    # the stale orange placeholder we seeded, because the WC now comes
    # from the just-verified companion JPEG.
    if fake_wc.exists():
        assert fake_wc.read_bytes() != stale_wc_bytes, (
            "RAW's stale WC bytes must not survive the import — either "
            "the file is unlinked or overwritten with a fresh WC from "
            "the verified companion JPEG"
        )


def test_key_duplicate_links_only_byte_verified_twin_folder(tmp_path):
    """Metadata-only ('key') duplicate: ``_key_twin_rows`` returns every
    catalog row sharing filename+size+capture-second, but only ONE of
    them may hold the card's actual bytes. The others are key-collisions
    with unrelated content (say, a burst frame with the same DateTime).

    Only the twin whose bytes we hashed and matched is a proven
    duplicate; linking the other key-collision folders would pull
    unrelated archive photos into the active workspace on a
    duplicate-only import. See PR #1107 review.
    """
    from import_job import ImportParams, run_import_job
    from PIL.ExifTags import Base as ExifBase

    dt = datetime(2026, 6, 15, 9, 45, 30)

    # Card file: red bytes, EXIF-timed so the checker generates a
    # trustworthy metadata key.
    card = tmp_path / "card"
    card.mkdir()
    card_file = card / "IMG_1200.jpg"
    img = Image.new("RGB", (16, 16), "red")
    exif = img.getexif()
    exif[ExifBase.DateTimeOriginal] = dt.strftime("%Y:%m:%d %H:%M:%S")
    img.save(str(card_file), exif=exif)
    card_bytes = card_file.read_bytes()

    # Two archive folders both containing IMG_1200.jpg — same filename,
    # same size, same trusted capture time — so both rows produce the
    # same metadata key and both appear in ``_key_twin_rows``.
    archive = tmp_path / "archive"
    archive.mkdir()
    verified_dir = archive / "verified-twin"
    verified_dir.mkdir()
    verified_file = verified_dir / "IMG_1200.jpg"
    # Twin A: SAME bytes as card — the real duplicate.
    verified_file.write_bytes(card_bytes)

    collision_dir = archive / "key-collision"
    collision_dir.mkdir()
    collision_file = collision_dir / "IMG_1200.jpg"
    # Twin B: same size, same key — DIFFERENT bytes.
    collision_bytes = card_bytes[:-1] + bytes([card_bytes[-1] ^ 0xFF])
    assert len(collision_bytes) == len(card_bytes)
    assert collision_bytes != card_bytes
    collision_file.write_bytes(collision_bytes)

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    # Catalog both twins with file_hash=NULL — this forces
    # DuplicateChecker to return a ('key', …) token (not 'hash') so
    # we exercise the metadata-only branch. The timestamp is
    # trustworthy so both rows produce a matching key.
    for folder_dir, file_path in (
        (verified_dir, verified_file),
        (collision_dir, collision_file),
    ):
        fid = db.conn.execute(
            "INSERT INTO folders (path, name, status) VALUES (?, ?, 'ok')",
            (str(folder_dir), folder_dir.name),
        ).lastrowid
        db.conn.execute(
            "INSERT INTO photos (folder_id, filename, extension, file_size,"
            " timestamp) VALUES (?, ?, '.jpg', ?, ?)",
            (
                fid, "IMG_1200.jpg",
                os.path.getsize(str(file_path)),
                dt.strftime("%Y-%m-%dT%H:%M:%S"),
            ),
        )
    db.conn.commit()

    result = run_import_job(
        _make_job(), FakeRunner(), db_path, ws_id, ImportParams(
            sources=[str(card)], destination=str(archive),
        ),
    )

    # The card was byte-identical to the verified twin, so it's a
    # legitimate skip.
    assert result["copied"] == 0
    assert result["skipped_duplicate"] == 1
    assert result["failed"] == 0
    assert result["safe_to_format"] is True

    ws_folder_is_root = {
        row["path"]: row["is_root"]
        for row in db.conn.execute(
            "SELECT f.path, wf.is_root FROM workspace_folders wf "
            "JOIN folders f ON f.id = wf.folder_id "
            "WHERE wf.workspace_id = ?",
            (ws_id,),
        )
    }
    # The byte-verified twin's folder is a user-facing workspace root
    # (``is_root=1``) — the import proved the archive holds the card's
    # bytes and links its folder into the workspace UI.
    assert ws_folder_is_root.get(str(verified_dir)) == 1
    # The key-collision folder was NEVER byte-verified against the
    # card. Before the fix, it was passed to the duplicate-link scan
    # as a restrict_dir and workspace-linked as its own top-level root
    # (``is_root=1``), pulling an unrelated archive folder into the
    # workspace UI. It must NOT surface as a workspace root here, and
    # after the restricted-scan cascade fix (PR #1107, line 1186) it
    # must not appear in ``workspace_folders`` at all.
    assert str(collision_dir) not in ws_folder_is_root

    # Stronger: the collision folder was never scanned this run, so
    # its pre-seeded row's ``file_hash`` is still NULL. Before the
    # fix, the dup-link scan visited the collision folder and would
    # have populated ``file_hash`` from its on-disk bytes.
    photo_hashes = {
        row["folder_path"]: row["file_hash"]
        for row in db.conn.execute(
            "SELECT p.file_hash, f.path AS folder_path "
            "FROM photos p JOIN folders f ON f.id = p.folder_id"
        )
    }
    assert photo_hashes.get(str(verified_dir)) is not None, (
        "verified twin's folder WAS scanned — its file_hash must "
        "be populated"
    )
    assert photo_hashes.get(str(collision_dir)) is None, (
        "key-collision folder must NOT be scanned by a duplicate-"
        "only import: its bytes were never proven to match the card"
    )


def test_hash_duplicate_links_only_byte_verified_twin_folder(tmp_path):
    """Hash-token duplicate: ``_hash_twin_rows`` returns every catalog
    row whose stored ``photos.file_hash`` matches the card. The stored
    hash column reflects the LAST scan, so a stale row can name a
    folder whose archive file has since been overwritten with unrelated
    bytes. Only the twin(s) whose CURRENT on-disk bytes we re-hashed
    and matched are proven duplicates; linking the other rows'
    folders would pull unrelated/missing archive folders into the
    active workspace on a duplicate-only import. See PR #1107 review.
    """
    from import_dedup import compute_file_hash
    from import_job import ImportParams, run_import_job

    # Card file whose bytes hash to a known value.
    card = tmp_path / "card"
    card.mkdir()
    card_file = card / "IMG_1300.jpg"
    Image.new("RGB", (16, 16), "cyan").save(str(card_file))
    ts = datetime(2026, 6, 20, 11, 30, 0).timestamp()
    os.utime(str(card_file), (ts, ts))
    card_hash = compute_file_hash(str(card_file))
    card_size = os.path.getsize(str(card_file))

    # Two archive folders both cataloged as holding a photo with
    # file_hash == card_hash. Twin A really does; Twin B was modified
    # after its scan and now holds different bytes (stale hash row).
    archive = tmp_path / "archive"
    archive.mkdir()
    verified_dir = archive / "verified-hash-twin"
    verified_dir.mkdir()
    verified_file = verified_dir / "IMG_1300.jpg"
    # Real duplicate: same bytes as the card.
    with open(str(card_file), "rb") as src, open(str(verified_file), "wb") as dst:
        dst.write(src.read())

    stale_dir = archive / "stale-hash-twin"
    stale_dir.mkdir()
    stale_file = stale_dir / "IMG_1300.jpg"
    # Stale twin: on-disk bytes NO LONGER match card_hash. Same size
    # (so the size sanity check doesn't reject it) but a byte flipped.
    stale_bytes = bytearray(card_file.read_bytes())
    stale_bytes[-1] ^= 0xFF
    stale_file.write_bytes(bytes(stale_bytes))
    assert compute_file_hash(str(stale_file)) != card_hash

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    # Catalog both twins with file_hash == card_hash. Both rows appear
    # in ``_hash_twin_rows`` — but only ``verified_dir`` currently
    # holds those bytes.
    for folder_dir in (verified_dir, stale_dir):
        fid = db.conn.execute(
            "INSERT INTO folders (path, name, status) VALUES (?, ?, 'ok')",
            (str(folder_dir), folder_dir.name),
        ).lastrowid
        db.conn.execute(
            "INSERT INTO photos (folder_id, filename, extension, file_size,"
            " file_hash) VALUES (?, ?, '.jpg', ?, ?)",
            (fid, "IMG_1300.jpg", card_size, card_hash),
        )
    db.conn.commit()

    result = run_import_job(
        _make_job(), FakeRunner(), db_path, ws_id, ImportParams(
            sources=[str(card)], destination=str(archive),
            verify_by_hash=True,
        ),
    )

    # The verified twin proves the archive still holds the card's
    # bytes — a legitimate skip.
    assert result["copied"] == 0
    assert result["skipped_duplicate"] == 1
    assert result["failed"] == 0
    assert result["safe_to_format"] is True

    ws_folder_is_root = {
        row["path"]: row["is_root"]
        for row in db.conn.execute(
            "SELECT f.path, wf.is_root FROM workspace_folders wf "
            "JOIN folders f ON f.id = wf.folder_id "
            "WHERE wf.workspace_id = ?",
            (ws_id,),
        )
    }
    # The byte-verified twin's folder is a user-facing workspace root
    # (``is_root=1``) — the import proved the archive holds the card's
    # bytes and links its folder into the workspace UI.
    assert ws_folder_is_root.get(str(verified_dir)) == 1
    # The stale-hash-twin folder was NEVER byte-verified against the
    # card this run. Before the fix, the hash path passed the whole
    # ``twin_rows`` set to ``_linkable_twin_dirs`` on the assumption
    # that ``photos.file_hash`` is authoritative, so this folder was
    # workspace-linked as its own top-level root, pulling an unrelated
    # archive folder into the workspace UI. It must NOT surface as a
    # workspace root here, and after the restricted-scan cascade fix
    # (PR #1107, line 1186) it must not appear in ``workspace_folders``
    # at all.
    assert str(stale_dir) not in ws_folder_is_root


def test_restricted_scan_does_not_link_unrelated_archive_subtrees(tmp_path):
    """The per-batch restricted ``scan()`` call in ``run_import_job``
    passes the broad archive ``destination`` as ``root`` and the templated
    ``dest_folder`` as the only ``restrict_dir``. Before the fix,
    scanner's eager ``_ensure_folder(root_path)`` (and every parent-chain
    step between the two) called ``db.add_folder(..., workspace_root=
    False)``, which still fires ``add_workspace_folder`` — and its
    path-prefix subtree cascade in ``_add_workspace_folder_no_commit``
    would link every pre-existing cataloged descendant of ``destination``
    into the active workspace. A one-folder import would therefore make
    unrelated archive subtrees (e.g. shoots from a different card or a
    different workspace) suddenly visible in the current workspace UI.

    See PR #1107 review at line 1186:
    "Avoid linking the whole archive during restricted scans."
    """
    from import_job import ImportParams

    # Card: one file. Templates to <archive>/2026/2026-07-05/.
    card = _make_card(tmp_path, [
        ("DSC_9001.jpg", datetime(2026, 7, 5, 12, 0, 0), "orange"),
    ])
    archive = tmp_path / "archive"
    archive.mkdir()

    # Pre-existing archive tree: two unrelated folders already cataloged
    # in ``folders`` (as if scanned by a prior workspace or a previous
    # session on this workspace), NOT currently linked to the active
    # workspace. We insert them via raw SQL to bypass ``add_folder``'s
    # auto-link so the "unlinked descendants of destination" precondition
    # holds cleanly at the start of the run.
    unrelated_a = archive / "2024" / "2024-01-15-kenya-trip"
    unrelated_a.mkdir(parents=True)
    unrelated_b = archive / "2025" / "2025-09-02-yosemite"
    unrelated_b.mkdir(parents=True)

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    for folder_dir in (unrelated_a, unrelated_b):
        db.conn.execute(
            "INSERT INTO folders (path, name, status) VALUES (?, ?, 'ok')",
            (str(folder_dir), folder_dir.name),
        )
    db.conn.commit()
    unrelated_paths = {str(unrelated_a), str(unrelated_b)}
    # Precondition: neither pre-existing folder is workspace-linked yet.
    assert unrelated_paths.isdisjoint(_ws_linked_folder_paths(db, ws_id))

    # Run the import into ONE new templated dest_folder.
    from import_job import run_import_job
    result = run_import_job(
        _make_job(), FakeRunner(), db_path, ws_id,
        ImportParams(sources=[str(card)], destination=str(archive)),
    )
    assert result["copied"] == 1
    assert result["failed"] == 0

    linked = _ws_linked_folder_paths(db, ws_id)
    # The newly-imported dest_folder must be linked (that's the whole
    # point of the import).
    dest_folder = archive / "2026" / "2026-07-05"
    assert str(dest_folder) in linked, (
        "the imported dest_folder must be visible in the active workspace"
    )
    # Neither pre-existing unrelated archive subtree should have been
    # dragged into the active workspace by the restricted scan.
    for path in unrelated_paths:
        assert path not in linked, (
            f"unrelated pre-existing archive folder {path} was linked "
            f"into the active workspace by the restricted scan — the "
            f"cascade in ``_add_workspace_folder_no_commit`` fired for "
            f"``destination`` even though it was not the user's target"
        )


# ---------------------------------------------------------------------------
# chaining: result carries imported photo ids (import/process split PR 3)
# ---------------------------------------------------------------------------


def test_result_carries_imported_photo_ids(tmp_path):
    """The after-import chaining hook builds the process job's collection
    from the freshly imported rows; the result must name them."""
    from import_job import ImportParams

    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
        ("DSC_0002.jpg", datetime(2026, 7, 4, 9, 0, 0), "green"),
    ])
    archive = tmp_path / "archive"

    db, ws_id, result = _run_import(tmp_path, ImportParams(
        sources=[str(card)], destination=str(archive),
    ))

    all_ids = sorted(r["id"] for r in _photo_rows(db))
    assert sorted(result["photo_ids"]) == all_ids
    assert len(all_ids) == 2

    # Duplicates-only rerun: present but empty — the chaining hook skips
    # with "no new photos" instead of enqueueing an empty process run.
    from import_job import run_import_job

    rerun_result = run_import_job(
        _make_job(), FakeRunner(), str(tmp_path / "test.db"), ws_id,
        ImportParams(sources=[str(card)], destination=str(archive)),
    )
    assert rerun_result["photo_ids"] == []
    assert rerun_result["skipped_duplicate"] == 2


def test_progress_events_carry_live_per_folder_counts(tmp_path):
    """The Import page renders per-folder progress from the SSE stream;
    an in-flight event mid-run must already show nonzero counts for the
    folder being copied — not just at completion (transparency rule:
    never fake per-folder progress from stale counters)."""
    from import_job import ImportParams

    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
        ("DSC_0002.jpg", datetime(2026, 7, 3, 11, 0, 0), "green"),
        ("DSC_0003.jpg", datetime(2026, 7, 4, 9, 0, 0), "blue"),
        ("DSC_0004.jpg", datetime(2026, 7, 4, 9, 5, 0), "white"),
    ])
    runner = FakeRunner()
    _run_import(tmp_path, ImportParams(
        sources=[str(card)], destination=str(tmp_path / "archive"),
    ), runner=runner)

    progress_folder_totals = []
    for (_, evt, data) in runner.events:
        if evt != "progress" or "folders" not in data:
            continue
        total_copied = sum(
            c.get("copied", 0) for c in data["folders"].values()
        )
        progress_folder_totals.append(total_copied)

    assert progress_folder_totals, "no progress event carried folders"
    # Some event fired strictly mid-run: after the first copy landed but
    # before the last one did.
    assert any(0 < t < 4 for t in progress_folder_totals), (
        progress_folder_totals
    )
