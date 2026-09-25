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

    Without hard links the rollback cannot atomically restore the
    racer's bytes to ``dst``, so it leaves them at the unique
    ``.rollback`` scratch name rather than race a second writer with a
    check-then-rename (Codex P2 on dbf07e9). The racer's bytes still
    survive — just at a marker name an operator can recover.
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
    # ``dst`` is either absent (we did not restore) or holds racer bytes;
    # what matters is that our promote never wrote its own bytes over
    # the racer's file. The racer's bytes remain recoverable at the
    # unique ``.rollback`` scratch name.
    rollbacks = [n for n in os.listdir(dst_dir) if n.endswith(".rollback")]
    assert len(rollbacks) == 1, os.listdir(dst_dir)
    assert (dst_dir / rollbacks[0]).read_bytes() == racer_bytes
    if dst.exists():
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


def test_copy_via_temp_no_hardlink_fallback_preserves_copy2_metadata(
    tmp_path, monkeypatch,
):
    """The fallback claims a fresh inode at ``dst`` rather than promoting
    ``tmp``'s inode via ``os.link``, so extended metadata that
    ``shutil.copy2`` placed on ``tmp`` (xattrs on Linux, ``st_flags`` on
    macOS/BSD, ACL-related xattrs, resource-fork xattrs, Finder tags)
    is not carried across for free. Without an explicit transfer,
    ``move_photos`` would then delete the source after silently losing
    that metadata. ``_apply_metadata`` must run ``shutil.copystat`` from
    ``tmp`` to the claimed destination after the fd-based times/mode
    ops so all of ``copy2``'s metadata reaches ``dst``.

    On Linux the copystat is bound to ``/proc/self/fd/<fd>`` for
    identity safety (see the ``_apply_metadata`` docstring); the
    destination path in the tracker is that magic entry rather than
    the plain ``dst`` name, but the syscalls still land on the
    claimed inode so the bytes and metadata reach ``dst``.
    """
    import errno

    src = _jpeg(tmp_path / "a.jpg", "red")
    dst = tmp_path / "out" / "a.jpg"
    dst.parent.mkdir()

    def no_hardlinks(*_a, **_kw):
        raise OSError(errno.EOPNOTSUPP, "hard links not supported")

    monkeypatch.setattr(staged_copy.os, "link", no_hardlinks)

    calls = []
    real_copystat = staged_copy.shutil.copystat

    def tracking_copystat(src_arg, dst_arg, *, follow_symlinks=True):
        calls.append((str(src_arg), str(dst_arg), follow_symlinks))
        return real_copystat(src_arg, dst_arg, follow_symlinks=follow_symlinks)

    monkeypatch.setattr(staged_copy.shutil, "copystat", tracking_copystat)

    staged_copy.copy_via_temp(str(src), str(dst))

    assert dst.read_bytes() == src.read_bytes()
    assert _no_partials(dst.parent) == []
    # ``shutil.copy2(src, tmp)`` internally runs ``copystat(src, tmp)``,
    # so the tracker sees that call first. What matters here is the
    # follow-up ``copystat(tmp, <claimed-dst>)`` inside the placeholder
    # promote — that transfer is what carries xattrs/flags/ACLs to
    # ``dst`` when ``os.link`` cannot promote ``tmp``'s inode. On Linux
    # the promote binds to ``/proc/self/fd/<fd>`` for identity safety;
    # on other platforms the dst path itself.
    promote_calls = [
        call for call in calls
        if call[1] == str(dst) or call[1].startswith("/proc/self/fd/")
    ]
    assert len(promote_calls) == 1, calls
    src_arg, dst_arg, follow = promote_calls[0]
    if staged_copy._PROCFS_FD_AVAILABLE:
        assert dst_arg.startswith("/proc/self/fd/")
    else:
        assert dst_arg == str(dst)
    assert follow is True
    # The tmp is a hidden sibling in dst's directory; it is unlinked
    # after the promote returns, but during the copystat call it lives
    # alongside dst.
    assert os.path.dirname(src_arg) == str(dst.parent)
    assert os.path.basename(src_arg).endswith(".partial")


def test_copy_via_temp_metadata_apply_is_identity_safe_against_racer(
    tmp_path, monkeypatch,
):
    """A concurrent writer that unlinks the claimed placeholder and
    recreates ``dst`` between our pre-op inode check and the copystat
    call used to see the racer's file's mode, timestamps and xattrs
    silently rewritten to those of ``tmp``. Rollback cannot restore
    that metadata. Binding the copystat to ``/proc/self/fd/<fd>``
    routes every syscall through the kernel-side fd table, so a
    racer's inode never sees our metadata even inside the TOCTOU
    window ``_guarded_path_op`` failed to close.
    """
    import errno

    if not staged_copy._PROCFS_FD_AVAILABLE:
        pytest.skip("procfs not available on this platform")

    src = _jpeg(tmp_path / "a.jpg", "red")
    dst_dir = tmp_path / "out"
    dst_dir.mkdir()
    dst = dst_dir / "a.jpg"

    def no_hardlinks(*_a, **_kw):
        raise OSError(errno.EOPNOTSUPP, "hard links not supported")

    monkeypatch.setattr(staged_copy.os, "link", no_hardlinks)

    # Distinctive mode on the racer's file that must survive the
    # promote — if the copystat retargets, this becomes ``src``'s mode.
    racer_bytes = b"racer-content"
    racer_mode = 0o600
    racer_atime = 111.0
    racer_mtime = 222.0

    real_copystat = staged_copy.shutil.copystat
    tripped = {"once": False}

    def racing_copystat(src_arg, dst_arg, *, follow_symlinks=True):
        # Trip once, right before the copystat runs: unlink our
        # placeholder (which our fd still keeps alive) and drop a
        # racer's file at ``dst`` with distinctive metadata. If the
        # copystat then retargeted onto the path, the racer's mode
        # would be overwritten.
        if not tripped["once"] and str(dst_arg).startswith("/proc/self/fd/"):
            tripped["once"] = True
            with contextlib.suppress(FileNotFoundError):
                os.unlink(str(dst))
            with open(str(dst), "wb") as fh:
                fh.write(racer_bytes)
            os.chmod(str(dst), racer_mode)
            os.utime(str(dst), (racer_atime, racer_mtime))
        return real_copystat(src_arg, dst_arg, follow_symlinks=follow_symlinks)

    monkeypatch.setattr(staged_copy.shutil, "copystat", racing_copystat)

    with pytest.raises(FileExistsError):
        # The nlink check after the promote should detect that our
        # placeholder was unlinked and refuse to report success.
        staged_copy.copy_via_temp(str(src), str(dst))

    assert tripped["once"], "racer never ran"
    # The racer's file survives with its distinctive metadata intact:
    # the copystat bound to /proc/self/fd/<fd> operated on our (now
    # orphaned) inode, never on the racer's file. Without hard links
    # the rollback leaves the racer's bytes at the unique
    # ``.rollback`` scratch name rather than race a second writer, so
    # look for the racer's file there.
    import stat as stat_mod
    rollbacks = [n for n in os.listdir(dst_dir) if n.endswith(".rollback")]
    assert len(rollbacks) == 1, os.listdir(dst_dir)
    scratch = dst_dir / rollbacks[0]
    assert scratch.read_bytes() == racer_bytes
    st = scratch.stat()
    assert stat_mod.S_IMODE(st.st_mode) == racer_mode
    assert st.st_mtime == racer_mtime
    assert _no_partials(dst_dir) == []


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


def _card_raw_already_archived_beside_unrelated_jpeg(tmp_path, dest_root):
    """Card RAW already sits byte-for-byte at archive slot 0 (an earlier,
    interrupted import) while the archive's ``IMG_0001.JPG`` is an unrelated
    photo. Returns the day folder and the unrelated JPEG's bytes."""
    day = datetime(2026, 3, 28, 10, 0, 0)
    day_dir = dest_root / "2026" / "2026-03-28"
    day_dir.mkdir(parents=True)
    _card_pair(tmp_path / "card", day)
    raw_bytes = (tmp_path / "card" / "IMG_0001.CR3").read_bytes()
    (day_dir / "IMG_0001.CR3").write_bytes(raw_bytes)
    unrelated = b"unrelated archive JPEG " * 50
    (day_dir / "IMG_0001.JPG").write_bytes(unrelated)
    return day_dir, raw_bytes, unrelated


def test_ingest_exact_match_does_not_adopt_slot_that_splits_pair(tmp_path):
    """Codex P1: adopting the card RAW's exact copy at slot 0 left the card
    JPEG, which collides with an unrelated ``IMG_0001.JPG``, alone at
    ``_1`` — the scan could then pair the RAW with the unrelated JPEG. The
    whole card pair must settle at ``_1``."""
    from ingest import ingest

    dst = tmp_path / "nas"
    day_dir, raw_bytes, unrelated = (
        _card_raw_already_archived_beside_unrelated_jpeg(tmp_path, dst)
    )

    db = Database(str(tmp_path / "test.db"))
    result = ingest(str(tmp_path / "card"), str(dst), db=db)

    assert result["failed"] == 0
    assert sorted(os.listdir(day_dir)) == [
        "IMG_0001.CR3", "IMG_0001.JPG", "IMG_0001_1.CR3", "IMG_0001_1.JPG",
    ]
    assert (day_dir / "IMG_0001_1.CR3").read_bytes() == raw_bytes
    assert (day_dir / "IMG_0001.JPG").read_bytes() == unrelated


def test_ingest_retry_adopts_both_siblings_at_their_suffixed_slot(tmp_path):
    """Codex P2: an interrupted import already landed the card pair at
    ``_1`` (slot 0 holds another body's RAW). A retry must adopt both exact
    copies at ``_1`` — the anchored sibling used to skip its own bytes and
    copy a second identical file to ``_2``."""
    from ingest import ingest

    day = datetime(2026, 3, 28, 10, 0, 0)
    dst = tmp_path / "nas"
    day_dir = dst / "2026" / "2026-03-28"
    day_dir.mkdir(parents=True)
    (day_dir / "IMG_0001.CR3").write_bytes(b"body A raw")
    card = tmp_path / "card"
    _card_pair(card, day)
    for name in ("IMG_0001.CR3", "IMG_0001.JPG"):
        stem, ext = os.path.splitext(name)
        (day_dir / f"{stem}_1{ext}").write_bytes((card / name).read_bytes())

    db = Database(str(tmp_path / "test.db"))
    result = ingest(str(card), str(dst), db=db)

    assert result["failed"] == 0
    assert result["copied"] == 0
    assert sorted(os.listdir(day_dir)) == [
        "IMG_0001.CR3", "IMG_0001_1.CR3", "IMG_0001_1.JPG",
    ]


def test_import_job_exact_match_does_not_adopt_slot_that_splits_pair(tmp_path):
    """Same as the ingest test, through ``_shared_collision_walk``'s adopt
    branch in ``import_job.py``."""
    from import_job import ImportParams, run_import_job

    from vireo.tests.test_import_job import FakeRunner, _make_job

    archive = tmp_path / "archive"
    day_dir, raw_bytes, unrelated = (
        _card_raw_already_archived_beside_unrelated_jpeg(tmp_path, archive)
    )

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    result = run_import_job(
        _make_job(), FakeRunner(), db_path, db._active_workspace_id,
        ImportParams(sources=[str(tmp_path / "card")], destination=str(archive)),
    )

    assert result["failed"] == 0
    assert sorted(n for n in os.listdir(day_dir) if not n.startswith(".")) == [
        "IMG_0001.CR3", "IMG_0001.JPG", "IMG_0001_1.CR3", "IMG_0001_1.JPG",
    ]
    assert (day_dir / "IMG_0001_1.CR3").read_bytes() == raw_bytes
    assert (day_dir / "IMG_0001.JPG").read_bytes() == unrelated


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


def test_ingest_adopts_zero_byte_pair_at_anchored_suffix(tmp_path):
    """Codex P2 on 6d750fc: an interrupted paired import left both
    zero-byte siblings at slot ``_1``. On retry, the anchored-suffix
    adoption must recognise the zero-byte match at the anchor slot the
    same way the slot-0 branch and ``_sibling_blocks_slot`` do (both
    treat two empty files as the same file) — otherwise the retry keeps
    advancing past the anchor and copies another empty placeholder at
    every further suffix, splitting the pair.
    """
    from ingest import ingest

    day = datetime(2026, 3, 28, 10, 0, 0)
    dst = tmp_path / "nas"
    day_dir = dst / "2026" / "2026-03-28"
    day_dir.mkdir(parents=True)
    # Unrelated body's RAW blocks slot 0 for ``IMG_0001.CR3``.
    (day_dir / "IMG_0001.CR3").write_bytes(b"body A raw payload")
    # Prior interrupted retry landed both empty placeholders at ``_1``.
    (day_dir / "IMG_0001_1.CR3").write_bytes(b"")
    (day_dir / "IMG_0001_1.JPG").write_bytes(b"")

    # Card's paired files are themselves zero-byte (e.g. a corrupted
    # card). Give them a real ``.CR3`` / ``.JPG`` extension so ingest
    # accepts them.
    card = tmp_path / "card"
    card.mkdir()
    raw = card / "IMG_0001.CR3"
    jpeg = card / "IMG_0001.JPG"
    raw.write_bytes(b"")
    jpeg.write_bytes(b"")
    ts = day.timestamp()
    for path in (raw, jpeg):
        os.utime(str(path), (ts, ts))

    db = Database(str(tmp_path / "test.db"))
    result = ingest(str(tmp_path / "card"), str(dst), db=db)

    # No third copy at ``_2`` — both empty siblings adopted the anchored
    # ``_1`` slot, matching the retry's intent.
    assert result["failed"] == 0
    assert sorted(os.listdir(day_dir)) == [
        "IMG_0001.CR3", "IMG_0001_1.CR3", "IMG_0001_1.JPG",
    ]
    # The prior anchored placeholders are untouched (adoption, not copy).
    assert (day_dir / "IMG_0001_1.CR3").read_bytes() == b""
    assert (day_dir / "IMG_0001_1.JPG").read_bytes() == b""
    # Skipped-duplicate accounting captured the adoption.
    assert result["skipped_duplicate"] >= 2


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
    # by path/mtime would reject the only reachable copy. The distinct
    # ``DEFERRED_PLAN`` sentinel (not None) lets callers tell "state
    # unknown, keep the group visible" apart from "fewer than 2
    # candidates, nothing to do".
    assert plan is duplicates_repo.DEFERRED_PLAN


def test_resolution_plan_defers_when_any_candidate_offline(tmp_path, monkeypatch):
    """Verify the deferral end-to-end: ``apply_duplicate_resolution`` writes
    nothing and returns a ``deferred: True`` result when a hash group has
    an offline twin, so the reachable copy is not rejected while the
    volume is down and the caller (``/api/duplicates/apply``) can tell
    deferrals apart from real no-ops."""
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
    assert result == {
        "winner_id": None,
        "loser_ids": [],
        "rejected": 0,
        "deferred": True,
    }

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


# -- companion-slot planning: filtered siblings and zero-byte anchors ---------


def test_ingest_filtered_card_sibling_still_forces_suffix_on_survivor(tmp_path):
    """Codex P1: when ``skip_duplicates`` drops a card RAW that is already
    cataloged, the surviving JPEG must still see its filtered RAW sibling in
    slot planning. Without it the JPEG lands at ``IMG_0001.JPG`` alongside an
    unrelated ``IMG_0001.CR3`` in the destination folder — the scan then pairs
    them (when metadata is missing or compatible). The JPEG must move to
    ``IMG_0001_1.JPG`` and leave the unrelated RAW alone.
    """
    from ingest import compute_file_hash, ingest

    day = datetime(2026, 3, 28, 10, 0, 0)
    dst = tmp_path / "nas"
    day_dir = dst / "2026" / "2026-03-28"
    day_dir.mkdir(parents=True)
    unrelated_raw = b"unrelated RAW bytes " * 64
    (day_dir / "IMG_0001.CR3").write_bytes(unrelated_raw)

    card = tmp_path / "card"
    _card_pair(card, day)
    card_raw = card / "IMG_0001.CR3"
    card_raw_hash = compute_file_hash(str(card_raw))

    db = Database(str(tmp_path / "test.db"))
    # Pre-catalog the card RAW under an unrelated archive folder so the
    # duplicate checker (verify_by_hash) filters it out of ``to_copy``,
    # exercising the code path where a survivor's card sibling isn't in
    # ``to_copy`` and companion_siblings needs to see it anyway.
    other = tmp_path / "elsewhere"
    other.mkdir()
    (other / "IMG_0001.CR3").write_bytes(card_raw.read_bytes())
    folder_id = db.add_folder(str(other))
    db.add_photo(
        folder_id=folder_id, filename="IMG_0001.CR3", extension=".CR3",
        file_size=card_raw.stat().st_size,
        file_mtime=card_raw.stat().st_mtime,
        file_hash=card_raw_hash,
    )

    result = ingest(
        str(card), str(dst), db=db, verify_by_hash=True,
    )

    assert result["failed"] == 0
    # The unrelated archive RAW must NOT be paired with the card's JPEG.
    assert sorted(os.listdir(day_dir)) == [
        "IMG_0001.CR3", "IMG_0001_1.JPG",
    ]
    assert (day_dir / "IMG_0001.CR3").read_bytes() == unrelated_raw


def test_ingest_retry_adopts_zero_byte_sibling_at_anchored_suffix(tmp_path):
    """Codex P2: an interrupted paired import already left both siblings at
    ``_1`` — one of them zero bytes. The retry used to adopt the non-empty
    sibling at ``_1`` but copy the zero-byte sibling under ``_2`` because the
    anchored-slot adoption guard excluded zero-byte matches. Both members
    must adopt at ``_1`` and leave no ``_2``.
    """
    from ingest import ingest

    day = datetime(2026, 3, 28, 10, 0, 0)
    dst = tmp_path / "nas"
    day_dir = dst / "2026" / "2026-03-28"
    day_dir.mkdir(parents=True)
    # Slot 0 is taken by an unrelated RAW from another body — the retry
    # anchors both card siblings on the shared ``_1`` suffix.
    (day_dir / "IMG_0001.CR3").write_bytes(b"body A raw")
    (day_dir / "IMG_0001.JPG").write_bytes(b"body A jpeg " * 32)

    card = tmp_path / "card"
    card.mkdir()
    # Zero-byte RAW plus a real JPEG. The card RAW is empty (an odd but
    # valid on-disk state — the DuplicateChecker skips zero-byte identity
    # everywhere, so ``skip_duplicates`` never filters it).
    (card / "IMG_0001.CR3").write_bytes(b"")
    _jpeg(card / "IMG_0001.JPG", "green", mtime=day)
    ts = day.timestamp()
    for name in ("IMG_0001.CR3", "IMG_0001.JPG"):
        os.utime(str(card / name), (ts, ts))

    # Previous interrupted run already landed both siblings at ``_1``.
    (day_dir / "IMG_0001_1.CR3").write_bytes(b"")
    (day_dir / "IMG_0001_1.JPG").write_bytes(
        (card / "IMG_0001.JPG").read_bytes()
    )

    db = Database(str(tmp_path / "test.db"))
    result = ingest(str(card), str(dst), db=db)

    assert result["failed"] == 0
    assert result["copied"] == 0
    # Both siblings adopt ``_1``; no ``_2`` placeholder is created.
    assert sorted(os.listdir(day_dir)) == [
        "IMG_0001.CR3", "IMG_0001.JPG",
        "IMG_0001_1.CR3", "IMG_0001_1.JPG",
    ]
    assert (day_dir / "IMG_0001_1.CR3").stat().st_size == 0


def test_ingest_advances_past_dangling_primary_symlink(tmp_path):
    """Codex P2 on dbf07e9: when the unsuffixed destination is a dangling
    symlink, ``Path.exists()`` returns False and the primary-collision
    branch used to fall through to ``copy_via_temp``. That helper's
    ``os.link`` promotion and ``O_EXCL`` fallback both raise
    ``FileExistsError`` against the existing directory entry, so the file
    (and every retry) failed instead of landing at ``_1``. The primary
    slot must probe ``lexists`` and route past any non-following entry
    the way the suffix walk already does."""
    from ingest import ingest

    day = datetime(2026, 3, 28, 10, 0, 0)
    card = tmp_path / "card"
    card.mkdir()
    _jpeg(card / "IMG_0001.JPG", "red", mtime=day)

    dst = tmp_path / "nas"
    day_dir = dst / "2026" / "2026-03-28"
    day_dir.mkdir(parents=True)
    # Dangling symlink at the primary name: lexists sees it, exists()
    # follows through and reports False.
    os.symlink(str(tmp_path / "missing.jpg"), str(day_dir / "IMG_0001.JPG"))
    assert os.path.lexists(day_dir / "IMG_0001.JPG")
    assert not (day_dir / "IMG_0001.JPG").exists()

    db = Database(str(tmp_path / "test.db"))
    result = ingest(str(card), str(dst), db=db)

    assert result["failed"] == 0, result
    assert result["copied"] == 1
    # The dangling entry is untouched; the real bytes landed at ``_1``.
    assert sorted(os.listdir(day_dir)) == ["IMG_0001.JPG", "IMG_0001_1.JPG"]
    assert os.path.islink(day_dir / "IMG_0001.JPG")
    assert (day_dir / "IMG_0001_1.JPG").is_file()


def test_rollback_leaves_racer_bytes_at_scratch_when_hardlinks_unavailable(
    tmp_path, monkeypatch,
):
    """Codex P2 on dbf07e9: after our detach moved a concurrent writer's
    file to the unique ``.rollback`` scratch path, the fallback used to
    ``lexists``-check ``dst`` and then ``os.rename(scratch, dst)`` to put
    the writer's bytes back. That check-then-act pair races a SECOND
    concurrent writer that claims ``dst`` in the window, and POSIX
    ``rename`` overwrites their file — defeating this helper's own
    no-overwrite contract. When atomic no-replace restore is unavailable
    (hard links unsupported here, which is why we were in this fallback)
    the rollback must leave the writer's file at the unique
    ``.rollback`` name instead of racing them."""
    import errno

    src = _jpeg(tmp_path / "a.jpg", "red")
    dst_dir = tmp_path / "out"
    dst_dir.mkdir()
    dst = dst_dir / "a.jpg"

    def no_hardlinks(*_a, **_kw):
        raise OSError(errno.EOPNOTSUPP, "hard links not supported")

    monkeypatch.setattr(staged_copy.os, "link", no_hardlinks)

    # Set up the racer: after our O_EXCL claim succeeds, replace our
    # placeholder with the racer's file (nlink=0 for us). The write then
    # fails, rollback runs, detaches the racer's file into scratch, and
    # then a SECOND writer claims dst before rollback's restore.
    racer_bytes = b"first-racer"
    second_writer_bytes = b"second-writer"
    original_fstat = os.fstat
    original_open = os.open
    original_rename = os.rename
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

    def slot_stealing_rename(src_arg, dst_arg):
        # After rollback detaches racer bytes into scratch, a second
        # writer claims dst before the restore step. Trip once so this
        # doesn't fire on the initial detach (dst -> scratch).
        result = original_rename(src_arg, dst_arg)
        if os.fspath(src_arg) == str(dst) and not os.path.lexists(dst):
            with open(str(dst), "wb") as fh:
                fh.write(second_writer_bytes)
        return result

    monkeypatch.setattr(staged_copy.os, "open", tracking_open)
    monkeypatch.setattr(staged_copy.os, "fstat", racing_fstat)
    monkeypatch.setattr(staged_copy.os, "rename", slot_stealing_rename)

    with pytest.raises(FileExistsError):
        staged_copy.copy_via_temp(str(src), str(dst))

    # The second writer's bytes at dst survive untouched: rollback never
    # overwrote them with the first racer's file.
    assert dst.read_bytes() == second_writer_bytes
    # The first racer's bytes are still recoverable from the unique
    # scratch name (an operator can move them back if needed).
    rollbacks = [n for n in os.listdir(dst_dir) if n.endswith(".rollback")]
    assert len(rollbacks) == 1, os.listdir(dst_dir)
    assert (dst_dir / rollbacks[0]).read_bytes() == racer_bytes
    assert _no_partials(dst_dir) == []


def test_sibling_blocks_slot_rejects_source_backed_candidate(tmp_path):
    """Codex P1 on 86c7b99: when a suffixed sibling candidate is a symlink
    into the source card, ``os.stat`` and any subsequent hash follow the
    link back and the walk would accept the slot as an exact byte match.
    The first companion would anchor there, but the sibling's own
    ``_resolve_dest_collision`` correctly rejects via
    ``_is_source_backed_dest`` and advances, splitting the RAW/JPEG pair;
    a later scan can then pair the landed file with the source-backed
    entry and leave the archive dependent on the card. ``_sibling_blocks_slot``
    must apply the same guard.
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
    # A symlink at ``dest/IMG_0001.JPG`` that resolves back into the
    # source card — the geometry the guard exists to catch.
    os.symlink(str(jpg), str(dest / "IMG_0001.JPG"))

    batch_st = _ImportBatchState(rel="rel", dest_folder=str(dest))
    key = (str(card), "img_0001")
    batch_st.companion_siblings[key] = [raw, jpg]

    class _Ctx:
        def __init__(self, card_root):
            self._card_root = os.path.realpath(str(card_root))

        fold_basename = staticmethod(lambda name: name.casefold())

        def path_under_any_source(self, path):
            resolved = os.path.realpath(str(path))
            return (
                resolved == self._card_root
                or resolved.startswith(self._card_root + os.sep)
            )

    ctx = _Ctx(card)

    # Without the guard the walk would stat the symlink target (jpg on
    # the card), see the same bytes as sibling ``jpg``, and return False.
    # With the guard the source-backed entry is treated as blocking so
    # the RAW walks past this slot instead of anchoring here.
    assert _sibling_blocks_slot(
        batch_st, raw, "IMG_0001", 0, ctx=ctx,
    ) is True
