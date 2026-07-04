"""Tests for the metadata-first import duplicate gate (import_dedup)."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from datetime import datetime

import import_dedup
from db import Database
from import_dedup import (
    CatalogIndex,
    DuplicateChecker,
    metadata_key,
    stored_metadata_key,
    timestamp_is_trustworthy,
)
from ingest import ingest
from PIL import Image
from PIL.ExifTags import Base as ExifBase


def _save_jpeg(path, *, exif_dt=None, color="red", size=(80, 80)):
    """Write a JPEG, optionally with an EXIF DateTimeOriginal."""
    img = Image.new("RGB", size, color=color)
    if exif_dt is None:
        img.save(str(path))
    else:
        exif = img.getexif()
        exif[ExifBase.DateTimeOriginal] = exif_dt.strftime("%Y:%m:%d %H:%M:%S")
        img.save(str(path), exif=exif)


def _seed_photo(db, tmp_path, filename, source_path, timestamp=None,
                file_hash=None, folder_name="library"):
    """Insert a folder+photo row mimicking a scanned catalog entry."""
    folder = tmp_path / folder_name
    folder.mkdir(exist_ok=True)
    fid_row = db.conn.execute(
        "SELECT id FROM folders WHERE path = ?", (str(folder),)
    ).fetchone()
    fid = fid_row["id"] if fid_row else db.add_folder(str(folder), name=folder_name)
    db.conn.execute(
        "UPDATE folders SET status = 'ok' WHERE id = ?", (fid,)
    )
    db.conn.execute(
        "INSERT INTO photos (folder_id, filename, extension, file_size,"
        " timestamp, file_hash) VALUES (?, ?, ?, ?, ?, ?)",
        (
            fid,
            filename,
            os.path.splitext(filename)[1],
            os.path.getsize(str(source_path)),
            timestamp,
            file_hash,
        ),
    )
    db.conn.commit()
    return folder


def _forbid_hashing(monkeypatch):
    """Make any content read explode, proving the heuristic never hashed."""

    def _boom(path, *a, **kw):
        raise AssertionError(f"content hash computed for {path}")

    monkeypatch.setattr(import_dedup, "compute_file_hash", _boom)
    import ingest as ingest_module

    monkeypatch.setattr(ingest_module, "compute_file_hash", _boom)


# --- trustworthiness rules ---------------------------------------------

def test_timestamp_trustworthiness():
    assert timestamp_is_trustworthy(datetime(2026, 5, 1, 10, 15, 30))
    # Missing, epoch-era, and exactly-midnight placeholder clocks are out.
    assert not timestamp_is_trustworthy(None)
    assert not timestamp_is_trustworthy(datetime(1980, 1, 1, 10, 15, 30))
    assert not timestamp_is_trustworthy(datetime(2021, 1, 1, 0, 0, 0))
    # Midnight-adjacent real times stay trusted.
    assert timestamp_is_trustworthy(datetime(2026, 5, 1, 0, 0, 1))


def test_stored_key_matches_source_key_with_subseconds():
    """Catalog sub-second timestamps truncate to the source's whole second."""
    dt = datetime(2026, 5, 1, 10, 15, 30)
    assert (
        stored_metadata_key("IMG_0001.NEF", 1234, "2026-05-01T10:15:30.250000")
        == metadata_key("img_0001.nef", 1234, dt)
    )
    assert stored_metadata_key("IMG_0001.NEF", 1234, "1980-01-01T10:15:30") is None
    assert stored_metadata_key("IMG_0001.NEF", 1234, "2021-01-01T00:00:00") is None
    assert stored_metadata_key("IMG_0001.NEF", None, "2026-05-01T10:15:30") is None
    assert stored_metadata_key(None, 1234, "2026-05-01T10:15:30") is None


# --- checker unit behavior ---------------------------------------------

def test_metadata_match_skips_without_reading_content(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "test.db"))
    src = tmp_path / "card"
    src.mkdir()
    dt = datetime(2026, 5, 1, 10, 15, 30)
    _save_jpeg(src / "IMG_0001.jpg", exif_dt=dt)
    _seed_photo(
        db, tmp_path, "IMG_0001.jpg", src / "IMG_0001.jpg",
        timestamp="2026-05-01T10:15:30",
    )

    _forbid_hashing(monkeypatch)
    checker = DuplicateChecker(CatalogIndex.from_db(db))
    token = checker.match(src / "IMG_0001.jpg")
    assert token is not None
    assert token[0] == "key"


def test_size_prefilter_skips_hashing_for_unseen_sizes(tmp_path, monkeypatch):
    """A metadata-less file whose size has no cataloged twin is copied
    without any content read at all."""
    db = Database(str(tmp_path / "test.db"))
    src = tmp_path / "card"
    src.mkdir()
    _save_jpeg(src / "no_exif.jpg")  # no EXIF -> hash-fallback candidate

    _forbid_hashing(monkeypatch)
    checker = DuplicateChecker(CatalogIndex.from_db(db))
    assert checker.match(src / "no_exif.jpg") is None


def test_missing_metadata_falls_back_to_hash(tmp_path):
    """No usable EXIF + size twin in the catalog -> exact content check."""
    from scanner import compute_file_hash

    db = Database(str(tmp_path / "test.db"))
    src = tmp_path / "card"
    src.mkdir()
    _save_jpeg(src / "no_exif.jpg")
    file_hash = compute_file_hash(str(src / "no_exif.jpg"))
    _seed_photo(
        db, tmp_path, "renamed_in_library.jpg", src / "no_exif.jpg",
        timestamp=None, file_hash=file_hash,
    )

    checker = DuplicateChecker(CatalogIndex.from_db(db))
    token = checker.match(src / "no_exif.jpg")
    assert token == ("hash", file_hash)


def test_placeholder_timestamp_falls_back_to_hash(tmp_path):
    """A generic midnight clock is not trusted as identity."""
    from scanner import compute_file_hash

    db = Database(str(tmp_path / "test.db"))
    src = tmp_path / "card"
    src.mkdir()
    _save_jpeg(src / "IMG_0001.jpg", exif_dt=datetime(2021, 1, 1, 0, 0, 0))
    file_hash = compute_file_hash(str(src / "IMG_0001.jpg"))
    _seed_photo(
        db, tmp_path, "IMG_0001.jpg", src / "IMG_0001.jpg",
        timestamp="2021-01-01T00:00:00", file_hash=file_hash,
    )

    checker = DuplicateChecker(CatalogIndex.from_db(db))
    # The placeholder key is excluded on BOTH sides; the hash still hits.
    token = checker.match(src / "IMG_0001.jpg")
    assert token == ("hash", file_hash)


def test_same_second_same_size_burst_frames_not_conflated(
    tmp_path, monkeypatch
):
    """Fixed-size RAW burst frames in the same second differ by filename."""
    db = Database(str(tmp_path / "test.db"))
    src = tmp_path / "card"
    src.mkdir()
    # Two "uncompressed RAW" frames: identical size, same capture second.
    (src / "IMG_0002.dng").write_bytes(b"\x01" * 4096)
    (src / "IMG_0003.dng").write_bytes(b"\x02" * 4096)
    dt_iso = "2026-05-01T10:15:30"
    _seed_photo(
        db, tmp_path, "IMG_0002.dng", src / "IMG_0002.dng", timestamp=dt_iso,
    )

    dt = datetime(2026, 5, 1, 10, 15, 30)
    monkeypatch.setattr(
        import_dedup,
        "source_capture_timestamps",
        lambda files: {f: dt for f in files},
    )
    checker = DuplicateChecker(CatalogIndex.from_db(db))
    checker.prepare([src / "IMG_0002.dng", src / "IMG_0003.dng"])
    assert checker.match(src / "IMG_0002.dng") is not None
    # Same size + same second but a different filename: NOT a duplicate.
    assert checker.match(src / "IMG_0003.dng") is None


def test_key_miss_still_hashes_when_catalog_row_lacks_timestamp(tmp_path):
    """A cataloged photo with a hash but no stored timestamp can never
    key-match, so a trusted-metadata source of the same size must fall
    through to the exact content check instead of being declared new."""
    from scanner import compute_file_hash

    db = Database(str(tmp_path / "test.db"))
    src = tmp_path / "card"
    src.mkdir()
    dt = datetime(2026, 5, 1, 10, 15, 30)
    _save_jpeg(src / "IMG_0001.jpg", exif_dt=dt)
    file_hash = compute_file_hash(str(src / "IMG_0001.jpg"))
    # Scan-time ExifTool failure: hash + size recorded, timestamp NULL.
    _seed_photo(
        db, tmp_path, "IMG_0001.jpg", src / "IMG_0001.jpg",
        timestamp=None, file_hash=file_hash,
    )

    checker = DuplicateChecker(CatalogIndex.from_db(db))
    assert checker.match(src / "IMG_0001.jpg") == ("hash", file_hash)


def test_verify_by_hash_catches_renamed_duplicate(tmp_path):
    """The heuristic intentionally re-imports renamed twins; the verify
    checkbox restores the exact behavior."""
    from scanner import compute_file_hash

    db = Database(str(tmp_path / "test.db"))
    src = tmp_path / "card"
    src.mkdir()
    dt = datetime(2026, 5, 1, 10, 15, 30)
    _save_jpeg(src / "renamed.jpg", exif_dt=dt)
    file_hash = compute_file_hash(str(src / "renamed.jpg"))
    _seed_photo(
        db, tmp_path, "IMG_0001.jpg", src / "renamed.jpg",
        timestamp="2026-05-01T10:15:30", file_hash=file_hash,
    )

    index = CatalogIndex.from_db(db)
    # Heuristic: trusted metadata, filename mismatch -> treated as new.
    assert DuplicateChecker(index).match(src / "renamed.jpg") is None
    # Verify mode: exact content identity wins.
    assert DuplicateChecker(index, verify_by_hash=True).match(
        src / "renamed.jpg"
    ) == ("hash", file_hash)


def test_intra_batch_twins_without_metadata_promote_to_hash(tmp_path):
    """Recording an identity-less file parks its size; a same-size
    fallback candidate later promotes it to a hashed identity."""
    import shutil

    db = Database(str(tmp_path / "test.db"))
    src = tmp_path / "card"
    src.mkdir()
    _save_jpeg(src / "a.jpg")
    shutil.copyfile(str(src / "a.jpg"), str(src / "b.jpg"))

    checker = DuplicateChecker(CatalogIndex.from_db(db))
    assert checker.check_and_record(src / "a.jpg") is False
    assert checker.check_and_record(src / "b.jpg") is True


# --- ingest() end-to-end ------------------------------------------------

def test_ingest_metadata_skip_reads_no_bytes_and_reports_folders(
    tmp_path, monkeypatch
):
    """Re-importing a card of cataloged photos copies nothing, reads no
    file bodies, and reports the destination folders holding the
    originals (for the post-import scan's restrict_dirs)."""
    db = Database(str(tmp_path / "test.db"))
    dst = tmp_path / "nas"
    dst.mkdir()
    src = tmp_path / "card"
    src.mkdir()
    dt = datetime(2026, 5, 1, 10, 15, 30)
    _save_jpeg(src / "IMG_0001.jpg", exif_dt=dt, color="red")
    _save_jpeg(src / "IMG_0002.jpg", exif_dt=dt, color="blue", size=(90, 90))

    # Originals live under the destination in a dated folder.
    lib_folder = dst / "2026" / "2026-05-01"
    lib_folder.mkdir(parents=True)
    for name in ("IMG_0001.jpg", "IMG_0002.jpg"):
        (lib_folder / name).write_bytes((src / name).read_bytes())
        _seed_photo(
            db, tmp_path, name, src / name,
            timestamp="2026-05-01T10:15:30",
            folder_name="unused",
        )
    # Point the seeded folder rows at the real library folder.
    db.conn.execute(
        "UPDATE folders SET path = ?, status = 'ok'", (str(lib_folder),)
    )
    db.conn.commit()

    _forbid_hashing(monkeypatch)
    result = ingest(str(src), str(dst), db=db, skip_duplicates=True)

    assert result["copied"] == 0
    assert result["skipped_duplicate"] == 2
    assert result["duplicate_folders"] == [str(lib_folder)]


def test_ingest_verify_by_hash_matches_old_behavior(tmp_path):
    """verify_by_hash=True skips a renamed duplicate the heuristic
    would re-import."""
    from scanner import compute_file_hash

    db = Database(str(tmp_path / "test.db"))
    dst = tmp_path / "nas"
    dst.mkdir()
    src = tmp_path / "card"
    src.mkdir()
    dt = datetime(2026, 5, 1, 10, 15, 30)
    _save_jpeg(src / "renamed.jpg", exif_dt=dt)
    _seed_photo(
        db, tmp_path, "IMG_0001.jpg", src / "renamed.jpg",
        timestamp="2026-05-01T10:15:30",
        file_hash=compute_file_hash(str(src / "renamed.jpg")),
    )

    result = ingest(
        str(src), str(dst), db=db, skip_duplicates=True, verify_by_hash=True,
    )
    assert result["copied"] == 0
    assert result["skipped_duplicate"] == 1

    # Heuristic mode re-imports it (documented tradeoff, self-healed by
    # the post-import scan's exact duplicate detection).
    result2 = ingest(str(src), str(dst), db=db, skip_duplicates=True)
    assert result2["copied"] == 1


def test_ingest_same_name_collision_still_exact(tmp_path):
    """The destination-collision branch never trusts metadata: same name,
    same size, same second, different bytes -> suffixed copy, not a skip."""
    db = Database(str(tmp_path / "test.db"))
    dst = tmp_path / "nas"
    dst.mkdir()
    src = tmp_path / "card"
    src.mkdir()
    dt = datetime(2026, 5, 1, 10, 15, 30)
    _save_jpeg(src / "IMG_0001.jpg", exif_dt=dt, color="red")

    # A different file with the same name and size already sits at the
    # destination path this import will choose.
    dest_folder = dst / "2026" / "2026-05-01"
    dest_folder.mkdir(parents=True)
    src_bytes = (src / "IMG_0001.jpg").read_bytes()
    altered = bytearray(src_bytes)
    altered[-1] ^= 0xFF
    (dest_folder / "IMG_0001.jpg").write_bytes(bytes(altered))

    result = ingest(str(src), str(dst), db=db, skip_duplicates=True)
    assert result["copied"] == 1
    assert (dest_folder / "IMG_0001_1.jpg").exists()


def test_ingest_same_name_collision_identical_skips(tmp_path):
    """Byte-identical file already at the chosen destination path is
    recognized by exact content compare (companion-JPEG re-import path)."""
    db = Database(str(tmp_path / "test.db"))
    dst = tmp_path / "nas"
    dst.mkdir()
    src = tmp_path / "card"
    src.mkdir()
    dt = datetime(2026, 5, 1, 10, 15, 30)
    _save_jpeg(src / "IMG_0001.jpg", exif_dt=dt)

    dest_folder = dst / "2026" / "2026-05-01"
    dest_folder.mkdir(parents=True)
    (dest_folder / "IMG_0001.jpg").write_bytes(
        (src / "IMG_0001.jpg").read_bytes()
    )

    result = ingest(str(src), str(dst), db=db, skip_duplicates=True)
    assert result["copied"] == 0
    assert result["skipped_duplicate"] == 1
    assert not (dest_folder / "IMG_0001_1.jpg").exists()


def test_shared_checker_dedupes_across_ingest_calls(tmp_path, monkeypatch):
    """One checker shared across a multi-source loop treats files copied
    by an earlier call as duplicates in later calls — without re-reading
    the copied files (the old accumulated-hashes rescan)."""
    db = Database(str(tmp_path / "test.db"))
    dst = tmp_path / "nas"
    dst.mkdir()
    dt = datetime(2026, 5, 1, 10, 15, 30)
    card_a = tmp_path / "card_a"
    card_b = tmp_path / "card_b"
    card_a.mkdir()
    card_b.mkdir()
    _save_jpeg(card_a / "IMG_0001.jpg", exif_dt=dt)
    (card_b / "IMG_0001.jpg").write_bytes(
        (card_a / "IMG_0001.jpg").read_bytes()
    )

    checker = DuplicateChecker(CatalogIndex.from_db(db))
    result_a = ingest(
        str(card_a), str(dst), db=db, skip_duplicates=True,
        duplicate_checker=checker,
    )
    assert result_a["copied"] == 1

    _forbid_hashing(monkeypatch)
    result_b = ingest(
        str(card_b), str(dst), db=db, skip_duplicates=True,
        duplicate_checker=checker,
    )
    assert result_b["copied"] == 0
    assert result_b["skipped_duplicate"] == 1


# --- preflight helpers use the same oracle ------------------------------

def test_non_duplicate_files_accepts_checker_and_legacy_set(tmp_path):
    from local_processing import non_duplicate_files
    from scanner import compute_file_hash

    db = Database(str(tmp_path / "test.db"))
    src = tmp_path / "card"
    src.mkdir()
    dt = datetime(2026, 5, 1, 10, 15, 30)
    _save_jpeg(src / "IMG_0001.jpg", exif_dt=dt)
    _save_jpeg(src / "IMG_0002.jpg", exif_dt=dt, color="blue", size=(90, 90))
    _seed_photo(
        db, tmp_path, "IMG_0001.jpg", src / "IMG_0001.jpg",
        timestamp="2026-05-01T10:15:30",
    )

    files = [src / "IMG_0001.jpg", src / "IMG_0002.jpg"]
    survivors = non_duplicate_files(files, DuplicateChecker(CatalogIndex.from_db(db)))
    assert survivors == [src / "IMG_0002.jpg"]

    # Legacy bare-hash-set callers still get exact behavior.
    legacy = non_duplicate_files(
        files, {compute_file_hash(str(src / "IMG_0001.jpg"))},
    )
    assert legacy == [src / "IMG_0002.jpg"]
