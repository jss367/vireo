"""Regressions for filesystem integrity: copies, moves, pairing, duplicates.

Each test pins one way an import, move, scan or duplicate scan used to lose,
merge or mislabel a user's photo:

* a failed copy left a truncated file under the real name (``ingest``,
  ``move_photos``), which the next scan cataloged and every retry tripped on;
* one failed copy aborted the rest of a ``move_photos`` batch;
* RAW+JPEG pairing matched on the file stem alone, and a collision rename
  split a card's RAW from its JPEG, so unrelated photos were merged;
* an uppercase ``.XMP`` sidecar stayed behind when its photo moved;
* the duplicate scan un-rejected resolved groups when the kept file's volume
  was offline, and un-rejected rows the user had rejected by hand.
"""

import contextlib
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest
import staged_copy
from db import Database
from PIL import Image


def _jpeg(path, color="red", mtime=None):
    path.parent.mkdir(parents=True, exist_ok=True)
    Image.new("RGB", (32, 24), color).save(str(path))
    if mtime is not None:
        ts = mtime.timestamp()
        os.utime(str(path), (ts, ts))
    return path


def _truncating_copy2(fail_basename, real_copy2):
    """A ``shutil.copy2`` that writes 10 bytes of ``fail_basename`` and dies."""

    def copy2(src, dst, *args, **kwargs):
        if os.path.basename(str(src)) == fail_basename:
            with open(src, "rb") as fin, open(dst, "wb") as fout:
                fout.write(fin.read(10))
            raise OSError(28, "No space left on device")
        return real_copy2(src, dst, *args, **kwargs)

    return copy2


def _no_partials(directory):
    return [n for n in os.listdir(directory) if n.endswith(".partial")]


# -- copy_via_temp ------------------------------------------------------------


def test_copy_via_temp_failure_leaves_destination_untouched(tmp_path, monkeypatch):
    src = _jpeg(tmp_path / "IMG_1.jpg")
    dst = tmp_path / "out" / "IMG_1.jpg"
    dst.parent.mkdir()
    monkeypatch.setattr(
        staged_copy.shutil, "copy2",
        _truncating_copy2("IMG_1.jpg", staged_copy.shutil.copy2),
    )

    with pytest.raises(OSError):
        staged_copy.copy_via_temp(str(src), str(dst))

    assert os.listdir(dst.parent) == []


def test_copy_via_temp_never_overwrites(tmp_path):
    src = _jpeg(tmp_path / "a.jpg", "red")
    dst = _jpeg(tmp_path / "out" / "a.jpg", "blue")
    before = dst.read_bytes()

    with pytest.raises(FileExistsError):
        staged_copy.copy_via_temp(str(src), str(dst))

    assert dst.read_bytes() == before
    assert _no_partials(dst.parent) == []


def test_copy_via_temp_no_hardlink_fallback_races_never_overwrite(
    tmp_path, monkeypatch,
):
    """On filesystems without hard links (exFAT, some SMB/NFS shares) the
    fallback promote must still refuse to overwrite ``dst`` when another
    writer created it AFTER an existence check would have said "free".
    A ``lexists``-then-``os.replace`` sequence silently overwrites that
    concurrent write; the atomic ``O_CREAT | O_EXCL`` claim races with
    it at kernel level and rejects the promote before touching bytes.

    Simulated by pre-creating ``dst`` and monkeypatching ``lexists`` to
    lie about it — the exact window a real race would open.
    """
    import errno

    src = _jpeg(tmp_path / "a.jpg", "red")
    dst = _jpeg(tmp_path / "out" / "a.jpg", "blue")
    before = dst.read_bytes()

    def no_hardlinks(*_a, **_kw):
        raise OSError(errno.EOPNOTSUPP, "hard links not supported")

    monkeypatch.setattr(staged_copy.os, "link", no_hardlinks)
    # Stand in for a concurrent writer that created ``dst`` after our
    # existence check would have returned False.
    monkeypatch.setattr(staged_copy.os.path, "lexists", lambda _p: False)

    with pytest.raises(FileExistsError):
        staged_copy.copy_via_temp(str(src), str(dst))

    assert dst.read_bytes() == before
    assert _no_partials(dst.parent) == []


def test_copy_via_temp_no_hardlink_fallback_promotes_when_slot_is_free(
    tmp_path, monkeypatch,
):
    """The fallback still copies when ``dst`` is free — the O_EXCL claim
    just gates against races. On success the destination is the source
    bytes, with no partial or empty placeholder left behind."""
    import errno

    src = _jpeg(tmp_path / "a.jpg", "red")
    dst = tmp_path / "out" / "a.jpg"
    dst.parent.mkdir()

    def no_hardlinks(*_a, **_kw):
        raise OSError(errno.EOPNOTSUPP, "hard links not supported")

    monkeypatch.setattr(staged_copy.os, "link", no_hardlinks)

    staged_copy.copy_via_temp(str(src), str(dst))

    assert dst.read_bytes() == src.read_bytes()
    assert _no_partials(dst.parent) == []


def test_copy_via_temp_no_hardlink_fallback_never_overwrites_replacement(
    tmp_path, monkeypatch,
):
    """A concurrent writer that unlinks the O_EXCL placeholder and
    creates its own file at ``dst`` between our claim and our promote
    used to see its bytes silently overwritten: the old fallback closed
    the claim fd and then ran ``os.replace(tmp, dst)`` over whatever
    was at the name. The new promote transfers bytes into the still-open
    claim fd and re-checks the inode's link count, so the racer's file
    survives untouched and the caller sees FileExistsError.
    """
    import errno

    src = _jpeg(tmp_path / "a.jpg", "red")
    dst_dir = tmp_path / "out"
    dst_dir.mkdir()
    dst = dst_dir / "a.jpg"

    def no_hardlinks(*_a, **_kw):
        raise OSError(errno.EOPNOTSUPP, "hard links not supported")

    monkeypatch.setattr(staged_copy.os, "link", no_hardlinks)

    # After our O_EXCL claim succeeds, simulate a concurrent writer:
    # unlink our placeholder and drop its own file at the same name.
    # ``os.fstat`` on our fd afterwards reports nlink=0. Track the claim
    # fd explicitly (only tripped when fstat runs on THAT fd), because
    # ``shutil.copy2`` staging the temp file also calls ``os.fstat`` and
    # would otherwise trip the racer before the claim-fd nlink check even
    # runs — leaving the check unexercised while the test still passes.
    racer_bytes = b"racer-content"
    original_fstat = os.fstat
    original_open = os.open
    claim_fds = set()
    tripped = {"once": False}

    def tracking_open(path, flags, *args, **kwargs):
        fd = original_open(path, flags, *args, **kwargs)
        if os.fspath(path) == str(dst) and (flags & os.O_EXCL):
            claim_fds.add(fd)
        return fd

    def racing_fstat(fd):
        result = original_fstat(fd)
        if fd in claim_fds and not tripped["once"]:
            tripped["once"] = True
            with contextlib.suppress(FileNotFoundError):
                os.unlink(str(dst))
            with open(str(dst), "wb") as fh:
                fh.write(racer_bytes)
        return result

    monkeypatch.setattr(staged_copy.os, "open", tracking_open)
    monkeypatch.setattr(staged_copy.os, "fstat", racing_fstat)

    with pytest.raises(FileExistsError):
        staged_copy.copy_via_temp(str(src), str(dst))

    assert tripped["once"], "racer never ran against the O_EXCL claim fd"
    assert dst.read_bytes() == racer_bytes
    assert _no_partials(dst_dir) == []


def test_copy_via_temp_no_hardlink_fallback_windows_metadata(
    tmp_path, monkeypatch,
):
    """On Windows ``os.fchmod`` doesn't exist and ``os.utime`` doesn't
    accept an fd, so the fallback promote used to raise
    ``AttributeError``/``TypeError`` after copying the bytes, roll back
    the destination and (in ``move_photos``) bypass the ``except OSError``
    handler and abort the batch. With path-based fallbacks it now
    completes; the copied bytes and metadata land at ``dst`` and no
    partial is left behind."""
    import errno

    src = _jpeg(tmp_path / "a.jpg", "red")
    dst = tmp_path / "out" / "a.jpg"
    dst.parent.mkdir()

    def no_hardlinks(*_a, **_kw):
        raise OSError(errno.EOPNOTSUPP, "hard links not supported")

    monkeypatch.setattr(staged_copy.os, "link", no_hardlinks)
    # Simulate Windows: neither fd-based utime nor fchmod is available.
    monkeypatch.setattr(staged_copy, "_UTIME_SUPPORTS_FD", False)
    monkeypatch.setattr(staged_copy, "_HAS_FCHMOD", False)

    staged_copy.copy_via_temp(str(src), str(dst))

    assert dst.read_bytes() == src.read_bytes()
    assert _no_partials(dst.parent) == []


def test_promote_by_placeholder_closes_claim_fd_before_rollback(
    tmp_path, monkeypatch,
):
    """On Windows a handle opened without delete-sharing blocks
    ``os.rename`` on the same path, so ``_rollback_placeholder`` would
    fail its atomic detach, swallow the OSError, and leave the
    partially-written final ``dst`` behind for the next scan to catalog
    as a corrupt photo. The claim fd MUST close before rollback runs."""
    import errno

    src = _jpeg(tmp_path / "a.jpg", "red")
    dst = tmp_path / "out" / "a.jpg"
    dst.parent.mkdir()

    def no_hardlinks(*_a, **_kw):
        raise OSError(errno.EOPNOTSUPP, "hard links not supported")

    monkeypatch.setattr(staged_copy.os, "link", no_hardlinks)

    # Force the promote to raise after opening the claim fd but before
    # cleanup — an ``OSError`` from ``os.write`` triggers the rollback
    # branch. Track that the fd is closed BEFORE the rollback fires.
    original_write = staged_copy.os.write
    original_close = staged_copy.os.close
    original_rollback = staged_copy._rollback_placeholder
    events = []
    closed_fds = []
    claim_fds = set()
    original_open = staged_copy.os.open

    def tracking_open(path, flags, *args, **kwargs):
        fd = original_open(path, flags, *args, **kwargs)
        if os.fspath(path) == str(dst) and (flags & os.O_EXCL):
            claim_fds.add(fd)
        return fd

    def exploding_write(fd, data):
        if fd in claim_fds:
            raise OSError(errno.EIO, "simulated write failure")
        return original_write(fd, data)

    def tracking_close(fd):
        if fd in claim_fds:
            events.append(("close", fd))
            closed_fds.append(fd)
        return original_close(fd)

    def tracking_rollback(dst_arg, claim_ino):
        events.append(("rollback", dst_arg))
        # Ensure any surviving claim fds have been closed by now.
        assert claim_fds.issubset(set(closed_fds)), (
            "rollback ran before the O_EXCL claim fd was closed; "
            "on Windows os.rename would fail against the open handle."
        )
        return original_rollback(dst_arg, claim_ino)

    monkeypatch.setattr(staged_copy.os, "open", tracking_open)
    monkeypatch.setattr(staged_copy.os, "write", exploding_write)
    monkeypatch.setattr(staged_copy.os, "close", tracking_close)
    monkeypatch.setattr(staged_copy, "_rollback_placeholder", tracking_rollback)

    with pytest.raises(OSError):
        staged_copy.copy_via_temp(str(src), str(dst))

    kinds = [k for (k, _) in events]
    assert kinds == ["close", "rollback"], events
    assert not dst.exists(), "rollback should have removed our placeholder"
    assert _no_partials(dst.parent) == []


# -- ingest: a failed copy leaves nothing behind ------------------------------


def test_ingest_failed_copy_leaves_no_truncated_file(tmp_path, monkeypatch):
    """A copy that dies part-way must not leave ``IMG_0001.JPG`` truncated at
    the destination. Before the fix the retry saw the name taken, routed the
    real bytes to ``IMG_0001_1.JPG``, and the scan cataloged both."""
    from ingest import ingest

    card = tmp_path / "card"
    dst = tmp_path / "nas"
    dst.mkdir()
    day = datetime(2026, 3, 28, 10, 0, 0)
    src = _jpeg(card / "IMG_0001.JPG", mtime=day)
    db = Database(str(tmp_path / "test.db"))

    real_copy2 = staged_copy.shutil.copy2
    monkeypatch.setattr(
        staged_copy.shutil, "copy2", _truncating_copy2("IMG_0001.JPG", real_copy2),
    )
    first = ingest(str(card), str(dst), db=db)
    assert first["failed"] == 1

    day_dir = dst / "2026" / "2026-03-28"
    assert not day_dir.exists() or os.listdir(day_dir) == []

    monkeypatch.setattr(staged_copy.shutil, "copy2", real_copy2)
    second = ingest(str(card), str(dst), db=db)
    assert second["failed"] == 0
    assert sorted(os.listdir(day_dir)) == ["IMG_0001.JPG"]
    assert (day_dir / "IMG_0001.JPG").read_bytes() == src.read_bytes()


# -- move_photos: one failed copy fails one photo -----------------------------


@pytest.fixture
def three_photo_move(tmp_path):
    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    src = tmp_path / "src"
    dst = tmp_path / "dst"
    dst.mkdir()
    fid = db.add_folder(str(src), name="src")
    db.add_folder(str(dst), name="dst")
    ids = []
    for i in (1, 2, 3):
        path = _jpeg(src / f"IMG_{i}.JPG", color=("red", "green", "blue")[i - 1])
        ids.append(db.add_photo(
            folder_id=fid, filename=path.name, extension=".jpg",
            file_size=path.stat().st_size, file_mtime=float(i),
        ))
    return db, src, dst, ids


def test_move_photos_failed_copy_skips_one_photo_and_leaves_no_partial(
    three_photo_move, monkeypatch,
):
    from move import move_photos

    db, src, dst, ids = three_photo_move
    real_copy2 = staged_copy.shutil.copy2
    monkeypatch.setattr(
        staged_copy.shutil, "copy2", _truncating_copy2("IMG_2.JPG", real_copy2),
    )

    result = move_photos(db=db, photo_ids=ids, destination=str(dst))

    assert result["moved"] == 2
    assert any("IMG_2.JPG" in e and "copy failed" in e for e in result["errors"])
    assert sorted(os.listdir(dst)) == ["IMG_1.JPG", "IMG_3.JPG"]
    assert (src / "IMG_2.JPG").exists()

    # The retry is not blocked by a leftover "already exists".
    monkeypatch.setattr(staged_copy.shutil, "copy2", real_copy2)
    retry = move_photos(db=db, photo_ids=[ids[1]], destination=str(dst))
    assert retry["moved"] == 1, retry["errors"]
    assert not (src / "IMG_2.JPG").exists()
    assert sorted(os.listdir(dst)) == ["IMG_1.JPG", "IMG_2.JPG", "IMG_3.JPG"]


# -- move_photos: uppercase .XMP sidecars travel with their photo -------------


def test_xmp_path_finds_uppercase_sidecar(tmp_path):
    from move import _xmp_path

    photo = _jpeg(tmp_path / "IMG_9.JPG")
    (tmp_path / "IMG_9.XMP").write_text("<x/>")

    found = _xmp_path(str(photo))

    assert found is not None
    assert os.path.samefile(found, tmp_path / "IMG_9.XMP")


def test_move_photos_carries_uppercase_xmp_sidecar(tmp_path):
    from move import move_photos

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    src = tmp_path / "src"
    dst = tmp_path / "dst"
    dst.mkdir()
    fid = db.add_folder(str(src), name="src")
    db.add_folder(str(dst), name="dst")
    photo = _jpeg(src / "IMG_9.JPG")
    (src / "IMG_9.XMP").write_text("<x/>")
    pid = db.add_photo(
        folder_id=fid, filename="IMG_9.JPG", extension=".jpg",
        file_size=photo.stat().st_size, file_mtime=1.0,
    )

    result = move_photos(db=db, photo_ids=[pid], destination=str(dst))

    assert result["moved"] == 1, result["errors"]
    # A case-folding volume (macOS default) answers the ``.xmp`` probe for
    # ``IMG_9.XMP`` too, so only the name's letters are pinned there.
    assert sorted(n.lower() for n in os.listdir(dst)) == ["img_9.jpg", "img_9.xmp"]
    assert os.listdir(src) == []


# -- RAW+JPEG pairing ---------------------------------------------------------


def _paired_db(tmp_path, raw_meta, jpeg_meta):
    from scanner import _pair_raw_jpeg_companions

    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder(str(tmp_path / "day"))
    ids = {}
    for name, meta in (("IMG_0001.CR3", raw_meta), ("IMG_0001.JPG", jpeg_meta)):
        ids[name] = db.add_photo(
            folder_id=fid, filename=name,
            extension=os.path.splitext(name)[1].lower(),
            file_size=1, file_mtime=1.0, timestamp=meta.get("timestamp"),
        )
        db.conn.execute(
            "UPDATE photos SET camera_make = ?, camera_model = ? WHERE id = ?",
            (meta.get("make"), meta.get("model"), ids[name]),
        )
    db.conn.commit()
    _pair_raw_jpeg_companions(db)
    return db, ids


def _companions(db):
    return {
        r["filename"]: r["companion_path"]
        for r in db.conn.execute("SELECT filename, companion_path FROM photos")
    }


def test_pairing_merges_raw_and_jpeg_of_one_exposure(tmp_path):
    shot = {"timestamp": "2026-03-28T10:00:00", "make": "Canon", "model": "R5"}
    db, _ = _paired_db(tmp_path, shot, dict(shot, timestamp="2026-03-28T10:00:01"))
    assert _companions(db) == {"IMG_0001.CR3": "IMG_0001.JPG"}


def test_pairing_keeps_same_stem_files_with_different_capture_times(tmp_path):
    """Two bodies with overlapping counters: same stem, different shots."""
    db, _ = _paired_db(
        tmp_path,
        {"timestamp": "2026-03-28T09:14:02", "make": "Canon", "model": "R5"},
        {"timestamp": "2026-03-28T15:40:55", "make": "Canon", "model": "R5"},
    )
    assert _companions(db) == {"IMG_0001.CR3": None, "IMG_0001.JPG": None}


def test_pairing_keeps_same_stem_files_from_different_cameras(tmp_path):
    db, _ = _paired_db(
        tmp_path,
        {"timestamp": "2026-03-28T10:00:00", "make": "Canon", "model": "R5"},
        {"timestamp": "2026-03-28T10:00:00", "make": "Canon", "model": "R7"},
    )
    assert _companions(db) == {"IMG_0001.CR3": None, "IMG_0001.JPG": None}


def test_pairing_still_pairs_when_metadata_is_missing(tmp_path):
    db, _ = _paired_db(tmp_path, {}, {"timestamp": "2026-03-28T10:00:00"})
    assert _companions(db) == {"IMG_0001.CR3": "IMG_0001.JPG"}


def _card_pair(card, day):
    card.mkdir(parents=True, exist_ok=True)
    raw = card / "IMG_0001.CR3"
    raw.write_bytes(b"card B raw bytes " * 64)
    _jpeg(card / "IMG_0001.JPG", "green")
    for path in (raw, card / "IMG_0001.JPG"):
        os.utime(str(path), (day.timestamp(), day.timestamp()))


def test_ingest_collision_renames_card_pair_together(tmp_path):
    """The archive already holds another body's ``IMG_0001.CR3``. The card's
    RAW and JPEG must both take ``_1``; renaming only the RAW left the JPEG
    under the stem of the unrelated RAW, and the scan merged them."""
    from ingest import ingest

    day = datetime(2026, 3, 28, 10, 0, 0)
    dst = tmp_path / "nas"
    day_dir = dst / "2026" / "2026-03-28"
    day_dir.mkdir(parents=True)
    (day_dir / "IMG_0001.CR3").write_bytes(b"body A raw")
    _card_pair(tmp_path / "card", day)

    db = Database(str(tmp_path / "test.db"))
    result = ingest(str(tmp_path / "card"), str(dst), db=db)

    assert result["failed"] == 0
    assert sorted(os.listdir(day_dir)) == [
        "IMG_0001.CR3", "IMG_0001_1.CR3", "IMG_0001_1.JPG",
    ]
    assert (day_dir / "IMG_0001.CR3").read_bytes() == b"body A raw"


def _card_pair_fixed_size(card, day, raw_bytes, jpeg_bytes):
    card.mkdir(parents=True, exist_ok=True)
    raw = card / "IMG_0001.CR3"
    raw.write_bytes(raw_bytes)
    jpeg = card / "IMG_0001.JPG"
    # A JPEG with a real EXIF timestamp: the ingest-side folder plan
    # uses EXIF over mtime when both are available.
    _jpeg(jpeg, "green", mtime=day)
    real_jpeg_size = jpeg.stat().st_size
    # Overwrite with a bytes payload of the SAME size so a same-size
    # collision at the destination is easy to arrange, but keep a real
    # JPEG header so ingest still classifies it as an image (it also
    # reads EXIF; a plain byte blob would still be ingested because
    # ``.JPG`` matches the extension list).
    jpeg.write_bytes(jpeg_bytes[:real_jpeg_size].ljust(real_jpeg_size, b"\0"))
    ts = day.timestamp()
    for path in (raw, jpeg):
        os.utime(str(path), (ts, ts))
    return real_jpeg_size


def test_ingest_same_size_sibling_collision_keeps_pair_together(tmp_path):
    """Codex P1: the archive already holds an unrelated ``IMG_0001.JPG``
    with the SAME size as the card's JPEG but different bytes. The card's
    RAW processes first; without hashing the same-size sibling collision
    it took ``IMG_0001.CR3`` alone, then the card's JPEG later renamed to
    ``IMG_0001_1.JPG`` — splitting the pair, and the scan would merge the
    card's RAW with the archive's unrelated JPEG. Both must land at
    ``_1``.
    """
    from ingest import ingest

    day = datetime(2026, 3, 28, 10, 0, 0)
    dst = tmp_path / "nas"
    day_dir = dst / "2026" / "2026-03-28"
    day_dir.mkdir(parents=True)
    jpeg_size = _card_pair_fixed_size(
        tmp_path / "card", day,
        raw_bytes=b"card raw payload " * 32,
        jpeg_bytes=b"card JPEG payload " * 200,
    )
    # Archive's unrelated JPEG at exactly the same size as the card's.
    archive_jpeg = b"archive JPEG payload " * 200
    (day_dir / "IMG_0001.JPG").write_bytes(
        archive_jpeg[:jpeg_size].ljust(jpeg_size, b"\1"),
    )
    archive_before = (day_dir / "IMG_0001.JPG").read_bytes()

    db = Database(str(tmp_path / "test.db"))
    result = ingest(str(tmp_path / "card"), str(dst), db=db)

    assert result["failed"] == 0
    assert sorted(os.listdir(day_dir)) == [
        "IMG_0001.JPG", "IMG_0001_1.CR3", "IMG_0001_1.JPG",
    ]
    # Archive's original JPEG is untouched.
    assert (day_dir / "IMG_0001.JPG").read_bytes() == archive_before


def test_import_job_same_size_sibling_collision_keeps_pair_together(tmp_path):
    """Same as the ingest test, but through ``run_import_job``'s mirrored
    ``_sibling_blocks_slot`` in ``import_job.py``.
    """
    from import_job import ImportParams, run_import_job

    from vireo.tests.test_import_job import FakeRunner, _make_job

    day = datetime(2026, 3, 28, 10, 0, 0)
    archive = tmp_path / "archive"
    day_dir = archive / "2026" / "2026-03-28"
    day_dir.mkdir(parents=True)
    jpeg_size = _card_pair_fixed_size(
        tmp_path / "card", day,
        raw_bytes=b"card raw payload " * 32,
        jpeg_bytes=b"card JPEG payload " * 200,
    )
    archive_jpeg = b"archive JPEG payload " * 200
    (day_dir / "IMG_0001.JPG").write_bytes(
        archive_jpeg[:jpeg_size].ljust(jpeg_size, b"\1"),
    )
    archive_before = (day_dir / "IMG_0001.JPG").read_bytes()

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    result = run_import_job(
        _make_job(), FakeRunner(), db_path, db._active_workspace_id,
        ImportParams(sources=[str(tmp_path / "card")], destination=str(archive)),
    )

    assert result["failed"] == 0
    assert sorted(n for n in os.listdir(day_dir) if not n.startswith(".")) == [
        "IMG_0001.JPG", "IMG_0001_1.CR3", "IMG_0001_1.JPG",
    ]
    assert (day_dir / "IMG_0001.JPG").read_bytes() == archive_before


def test_import_job_collision_renames_card_pair_together(tmp_path):
    from import_job import (
        ImportParams,
        run_import_job,
    )

    from vireo.tests.test_import_job import FakeRunner, _make_job

    day = datetime(2026, 3, 28, 10, 0, 0)
    archive = tmp_path / "archive"
    day_dir = archive / "2026" / "2026-03-28"
    day_dir.mkdir(parents=True)
    (day_dir / "IMG_0001.CR3").write_bytes(b"body A raw")
    _card_pair(tmp_path / "card", day)

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    result = run_import_job(
        _make_job(), FakeRunner(), db_path, db._active_workspace_id,
        ImportParams(sources=[str(tmp_path / "card")], destination=str(archive)),
    )

    assert result["failed"] == 0
    assert sorted(n for n in os.listdir(day_dir) if not n.startswith(".")) == [
        "IMG_0001.CR3", "IMG_0001_1.CR3", "IMG_0001_1.JPG",
    ]
    assert (day_dir / "IMG_0001.CR3").read_bytes() == b"body A raw"


def test_slot_for_tries_anchor_first_then_walks_the_rest():
    from import_job import _slot_for

    assert [_slot_for(c, None) for c in range(4)] == [0, 1, 2, 3]
    assert [_slot_for(c, 0) for c in range(4)] == [0, 1, 2, 3]
    assert [_slot_for(c, 2) for c in range(5)] == [2, 0, 1, 3, 4]


def test_sibling_blocks_slot_blocks_same_hash_claim_without_checker(tmp_path):
    """Codex P1: with ``skip_duplicates=False`` the sibling's own walk
    (``_resolve_dest_collision``) refuses to adopt a same-hash claim and
    advances to the next suffix, so a same-bytes claim at the primary slot
    would split the pair. ``_sibling_blocks_slot`` must mirror that gate:
    when ``checker is None`` a same-hash claim blocks, so the RAW advances
    with its sibling instead of anchoring at a slot the JPEG walk will refuse.
    """
    from import_job import _ImportBatchState, _sibling_blocks_slot

    card = tmp_path / "card"
    card.mkdir()
    raw = card / "IMG_0001.CR3"
    jpg = card / "IMG_0001.JPG"
    raw.write_bytes(b"raw bytes")
    jpg.write_bytes(b"jpeg bytes")

    dest = tmp_path / "dest"
    dest.mkdir()
    batch_st = _ImportBatchState(rel="rel", dest_folder=str(dest))
    key = (str(card), "img_0001")
    batch_st.companion_siblings[key] = [raw, jpg]

    from ingest import compute_file_hash
    jpg_hash = compute_file_hash(str(jpg))

    class _Ctx:
        fold_basename = staticmethod(lambda name: name.casefold())

    ctx = _Ctx()
    # Earlier file in this batch queued IMG_0001.JPG with the JPEG's exact
    # bytes. With no checker, ``_resolve_dest_collision`` will refuse to
    # adopt that claim, so the RAW must not anchor slot 0.
    claims = {ctx.fold_basename("IMG_0001.JPG"): jpg_hash}
    assert _sibling_blocks_slot(
        batch_st, raw, "IMG_0001", 0,
        checker=None, claims=claims, ctx=ctx,
    ) is True


def test_sibling_blocks_slot_allows_same_hash_claim_with_checker(tmp_path):
    """The mirror of the above: with a checker (``skip_duplicates=True``)
    the sibling's walk WILL adopt the same-hash claim as an intra-batch
    duplicate, so the pair still settles at this slot and the RAW may
    anchor here.
    """
    from import_job import _ImportBatchState, _sibling_blocks_slot

    card = tmp_path / "card"
    card.mkdir()
    raw = card / "IMG_0001.CR3"
    jpg = card / "IMG_0001.JPG"
    raw.write_bytes(b"raw bytes")
    jpg.write_bytes(b"jpeg bytes")

    dest = tmp_path / "dest"
    dest.mkdir()
    batch_st = _ImportBatchState(rel="rel", dest_folder=str(dest))
    key = (str(card), "img_0001")
    batch_st.companion_siblings[key] = [raw, jpg]

    from ingest import compute_file_hash
    jpg_hash = compute_file_hash(str(jpg))

    class _Checker:
        def content_hash(self, path):
            return compute_file_hash(str(path))

    class _Ctx:
        fold_basename = staticmethod(lambda name: name.casefold())

    ctx = _Ctx()
    claims = {ctx.fold_basename("IMG_0001.JPG"): jpg_hash}
    assert _sibling_blocks_slot(
        batch_st, raw, "IMG_0001", 0,
        checker=_Checker(), claims=claims, ctx=ctx,
    ) is False


# -- duplicate scan: offline volumes and hand rejections ----------------------


def _dup_add(db, folder_id, filename, file_hash):
    return db.add_photo(
        folder_id=folder_id, filename=filename,
        extension=os.path.splitext(filename)[1], file_size=1000,
        file_mtime=100.0, file_hash=file_hash,
    )


def _flag_map(db, ids):
    placeholders = ",".join("?" * len(ids))
    return {
        r["id"]: r["flag"]
        for r in db.conn.execute(
            f"SELECT id, flag FROM photos WHERE id IN ({placeholders})", list(ids),
        )
    }


@pytest.fixture
def resolved_pair(tmp_path):
    """NAS copy kept, laptop copy auto-rejected by the duplicate resolver."""
    db = Database(str(tmp_path / "t.db"))
    nas = tmp_path / "nas"
    laptop = tmp_path / "laptop"
    nas.mkdir()
    laptop.mkdir()
    (nas / "owl.jpg").write_bytes(b"x")
    (laptop / "owl-2.jpg").write_bytes(b"x")
    kept = _dup_add(db, db.add_folder(str(nas)), "owl.jpg", "H")
    loser = _dup_add(db, db.add_folder(str(laptop)), "owl-2.jpg", "H")
    assert _flag_map(db, [kept, loser]) == {kept: "none", loser: "rejected"}
    return db, nas, laptop, kept, loser


def test_duplicate_scan_does_not_reopen_when_kept_volume_is_offline(
    resolved_pair, monkeypatch,
):
    """An unmounted NAS looks like a deleted file. The scan must not un-reject
    the laptop copy and propose rejecting the archive original."""
    import duplicate_scan
    from duplicate_scan import run_duplicate_scan

    db, nas, _laptop, kept, loser = resolved_pair
    (nas / "owl.jpg").unlink()  # stands in for the share going away
    monkeypatch.setattr(
        duplicate_scan, "_volume_offline",
        lambda path: os.path.dirname(path) == str(nas),
    )

    result = run_duplicate_scan({"progress": {}}, db, include_resolved=True)

    assert _flag_map(db, [kept, loser]) == {kept: "none", loser: "rejected"}
    [prop] = result["proposals"]
    assert prop["status"] == "resolved"
    assert prop["winner"]["id"] == kept
    assert prop["winner"]["volume_offline"] is True


def test_duplicate_scan_offline_copy_is_not_a_missing_loser(tmp_path, monkeypatch):
    """In an unresolved group, Rule 0 must not make the offline copy the loser."""
    import duplicate_scan
    from duplicate_scan import run_duplicate_scan

    db = Database(str(tmp_path / "t.db"))
    nas = tmp_path / "nas"
    laptop = tmp_path / "laptop"
    nas.mkdir()
    laptop.mkdir()
    (laptop / "owl-2.jpg").write_bytes(b"x")
    nas_fid = db.add_folder(str(nas))
    laptop_fid = db.add_folder(str(laptop))
    # Insert directly so add_photo's auto-resolve leaves the group open.
    for fid, name in ((nas_fid, "owl.jpg"), (laptop_fid, "owl-2.jpg")):
        db.conn.execute(
            "INSERT INTO photos (folder_id, filename, extension, file_size,"
            " file_mtime, file_hash, flag) VALUES (?, ?, '.jpg', 1, 100.0, 'H', 'none')",
            (fid, name),
        )
    db.conn.commit()
    monkeypatch.setattr(
        duplicate_scan, "_volume_offline",
        lambda path: os.path.dirname(path) == str(nas),
    )

    result = run_duplicate_scan({"progress": {}}, db, include_resolved=False)

    [prop] = result["proposals"]
    assert prop["winner"]["filename"] == "owl.jpg"  # clean name, as when mounted
    assert prop["losers"][0]["reason"] != "file missing on disk"


def test_duplicate_scan_all_offline_is_not_reported_as_all_missing(
    tmp_path, monkeypatch,
):
    """Every copy on an unreachable volume means unknown state, not missing:
    the proposal must not tell the UI to clean up 'orphaned' DB rows for a
    NAS that just isn't mounted."""
    import duplicate_scan
    from duplicate_scan import run_duplicate_scan

    db = Database(str(tmp_path / "t.db"))
    nas_a = tmp_path / "nas_a"
    nas_b = tmp_path / "nas_b"
    nas_a.mkdir()
    nas_b.mkdir()
    a_fid = db.add_folder(str(nas_a))
    b_fid = db.add_folder(str(nas_b))
    for fid, name in ((a_fid, "owl.jpg"), (b_fid, "owl-2.jpg")):
        db.conn.execute(
            "INSERT INTO photos (folder_id, filename, extension, file_size,"
            " file_mtime, file_hash, flag) VALUES (?, ?, '.jpg', 1, 100.0, 'H', 'none')",
            (fid, name),
        )
    db.conn.commit()
    # No files on disk anywhere, but both folders are on offline volumes.
    monkeypatch.setattr(duplicate_scan, "_volume_offline", lambda path: True)

    result = run_duplicate_scan({"progress": {}}, db, include_resolved=False)

    [prop] = result["proposals"]
    assert prop["all_missing"] is False, (
        "offline volumes must not count as missing"
    )
    assert prop["all_offline"] is True
    assert prop["winner"]["volume_offline"] is True


def test_duplicate_scan_all_missing_ignores_offline_flag_when_files_gone(
    tmp_path, monkeypatch,
):
    """A truly-missing group on a reachable volume still gets ``all_missing``."""
    import duplicate_scan
    from duplicate_scan import run_duplicate_scan

    db = Database(str(tmp_path / "t.db"))
    a = tmp_path / "a"
    a.mkdir()
    fid = db.add_folder(str(a))
    for name in ("owl.jpg", "owl-2.jpg"):
        db.conn.execute(
            "INSERT INTO photos (folder_id, filename, extension, file_size,"
            " file_mtime, file_hash, flag) VALUES (?, ?, '.jpg', 1, 100.0, 'H', 'none')",
            (fid, name),
        )
    db.conn.commit()
    # Volume is reachable; the files are just gone.
    monkeypatch.setattr(duplicate_scan, "_volume_offline", lambda path: False)

    result = run_duplicate_scan({"progress": {}}, db, include_resolved=False)

    [prop] = result["proposals"]
    assert prop["all_missing"] is True
    assert prop["all_offline"] is False


def test_resolution_plan_probes_reachability_before_stat(
    tmp_path, monkeypatch,
):
    """A stale SMB/NFS mount can wedge ``os.path.exists`` for minutes.
    ``DuplicatesRepository.resolution_plan`` runs inside ``add_photo``
    and ``check_and_resolve_duplicates_for_hash``, so it must consult
    the bounded ``_volume_offline`` probe BEFORE touching the path —
    otherwise the auto-resolver deadlocks the way ``duplicate_scan._row_to_info``
    used to before it was fixed.

    Also verifies the offline-defer contract: an offline candidate whose
    on-disk state is unknown must not be handed to the resolver, whose
    path/mtime rules could promote the unreachable row and cause the only
    reachable copy to be rejected."""
    from repositories import duplicates as duplicates_repo

    db = Database(str(tmp_path / "t.db"))
    nas = tmp_path / "nas"
    laptop = tmp_path / "laptop"
    nas.mkdir()
    laptop.mkdir()
    (laptop / "owl.jpg").write_bytes(b"x")
    nas_fid = db.add_folder(str(nas))
    laptop_fid = db.add_folder(str(laptop))
    for fid, name in ((nas_fid, "owl-nas.jpg"), (laptop_fid, "owl.jpg")):
        db.conn.execute(
            "INSERT INTO photos (folder_id, filename, extension, file_size,"
            " file_mtime, file_hash, flag) VALUES (?, ?, '.jpg', 1, 100.0, 'H', 'none')",
            (fid, name),
        )
    db.conn.commit()

    call_order = []

    def stub_offline(path):
        call_order.append(("offline", path))
        return os.path.dirname(path) == str(nas)

    real_exists = os.path.exists

    def stub_exists(path):
        call_order.append(("exists", path))
        # Any ``exists`` call for a path we said was offline would be the
        # wedge-triggering stat the fix removes. Fail loudly if that
        # ordering regresses.
        if os.path.dirname(path) == str(nas):
            raise AssertionError(
                "resolution_plan stat'd an offline-volume path: "
                + path
            )
        return real_exists(path)

    monkeypatch.setattr(duplicates_repo, "_volume_offline", stub_offline)
    monkeypatch.setattr(duplicates_repo.os.path, "exists", stub_exists)

    photo_ids = [r["id"] for r in db.conn.execute("SELECT id FROM photos")]
    repo = duplicates_repo.DuplicatesRepository(db.conn)
    plan = repo.resolution_plan(photo_ids)

    # Every offline candidate had its reachability probe run BEFORE any
    # exists probe for that same path (they never should have gotten
    # one, but ordering across paths is enough here).
    nas_probes = [k for (k, p) in call_order if os.path.dirname(p) == str(nas)]
    assert nas_probes and all(k == "offline" for k in nas_probes), call_order

    # Auto-resolution defers while any candidate is offline: the NAS row
    # might have been deleted while the volume was down, and picking it
    # by path/mtime would reject the only reachable copy.
    assert plan is None


def test_resolution_plan_defers_when_any_candidate_offline(tmp_path, monkeypatch):
    """Verify the deferral end-to-end: ``apply_duplicate_resolution`` writes
    nothing and returns the no-op shape when a hash group has an offline
    twin, so the reachable copy is not rejected while the volume is down."""
    from repositories import duplicates as duplicates_repo

    db = Database(str(tmp_path / "t.db"))
    nas = tmp_path / "nas"
    laptop = tmp_path / "laptop"
    nas.mkdir()
    laptop.mkdir()
    (laptop / "owl.jpg").write_bytes(b"x")
    nas_fid = db.add_folder(str(nas))
    laptop_fid = db.add_folder(str(laptop))
    for fid, name in ((nas_fid, "owl.jpg"), (laptop_fid, "owl.jpg")):
        db.conn.execute(
            "INSERT INTO photos (folder_id, filename, extension, file_size,"
            " file_mtime, file_hash, flag) VALUES (?, ?, '.jpg', 1, 100.0, 'H', 'none')",
            (fid, name),
        )
    db.conn.commit()

    monkeypatch.setattr(
        duplicates_repo,
        "_volume_offline",
        lambda path: os.path.dirname(path) == str(nas),
    )

    photo_ids = [r["id"] for r in db.conn.execute("SELECT id FROM photos")]
    result = db.apply_duplicate_resolution(photo_ids)
    assert result == {"winner_id": None, "loser_ids": [], "rejected": 0}

    # No photo was rejected — the reachable laptop copy is intact.
    flags = [r["flag"] for r in db.conn.execute("SELECT flag FROM photos")]
    assert flags == ["none", "none"]
    # And no ``duplicate_rejections`` row was written that a later reopen
    # would have to undo.
    (count,) = db.conn.execute(
        "SELECT COUNT(*) FROM duplicate_rejections"
    ).fetchone()
    assert count == 0


def test_duplicate_scan_reopen_keeps_hand_rejected_rows_rejected(resolved_pair):
    from duplicate_scan import run_duplicate_scan

    db, nas, laptop, kept, loser = resolved_pair
    (laptop / "owl-3.jpg").write_bytes(b"x")
    hand = db.conn.execute(
        "INSERT INTO photos (folder_id, filename, extension, file_size,"
        " file_mtime, file_hash, flag)"
        " VALUES ((SELECT id FROM folders WHERE path = ?), 'owl-3.jpg', '.jpg',"
        " 1000, 100.0, 'H', 'rejected')",
        (str(laptop),),
    ).lastrowid
    db.conn.commit()
    (nas / "owl.jpg").unlink()  # genuinely deleted, on a local disk

    result = run_duplicate_scan({"progress": {}}, db, include_resolved=True)

    assert _flag_map(db, [kept, loser, hand]) == {
        kept: "none", loser: "none", hand: "rejected",
    }
    [prop] = result["proposals"]
    assert prop["status"] == "unresolved"
    assert prop["winner"]["id"] == loser


def test_duplicate_scan_does_not_reopen_for_hand_rejected_sibling(tmp_path):
    """The only surviving sibling was rejected by the user, not the resolver."""
    from duplicate_scan import run_duplicate_scan

    db = Database(str(tmp_path / "t.db"))
    a = tmp_path / "a"
    a.mkdir()
    (a / "owl-2.jpg").write_bytes(b"x")
    fid = db.add_folder(str(a))
    for name, flag in (("owl.jpg", "none"), ("owl-2.jpg", "rejected")):
        db.conn.execute(
            "INSERT INTO photos (folder_id, filename, extension, file_size,"
            " file_mtime, file_hash, flag) VALUES (?, ?, '.jpg', 1, 100.0, 'H', ?)",
            (fid, name, flag),
        )
    db.conn.commit()

    result = run_duplicate_scan({"progress": {}}, db, include_resolved=True)

    flags = dict(db.conn.execute("SELECT filename, flag FROM photos").fetchall())
    assert flags == {"owl.jpg": "none", "owl-2.jpg": "rejected"}
    [prop] = result["proposals"]
    assert prop["status"] == "resolved"


def test_unrejecting_a_duplicate_loser_forgets_that_the_resolver_rejected_it(
    resolved_pair,
):
    """A row the user un-rejects and later rejects again is theirs."""
    db, _nas, _laptop, _kept, loser = resolved_pair
    db.conn.execute("UPDATE photos SET flag = 'none' WHERE id = ?", (loser,))
    db.conn.execute("UPDATE photos SET flag = 'rejected' WHERE id = ?", (loser,))
    db.conn.commit()

    assert db.reopen_duplicate_group("H") == 0
    assert _flag_map(db, [loser]) == {loser: "rejected"}
