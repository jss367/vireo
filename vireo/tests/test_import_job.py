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
