"""A move proves its copy by content before it removes the source.

A copy can have the right size and the wrong bytes, so a size check alone
must never authorize deleting an original. These tests corrupt copies
without changing their length, and add or change source files after the
copy was verified, and require the originals to survive.
"""
import os
import shutil

import pytest
from db import Database
from services import photo_ingest, workspace_folders

JPEG = b"\xff\xd8"


def _same_size_corruption(path):
    """Flip the file's last byte in place; its length does not change."""
    with open(path, "r+b") as handle:
        handle.seek(-1, os.SEEK_END)
        last = handle.read(1)
        handle.seek(-1, os.SEEK_END)
        handle.write(bytes([last[0] ^ 0xFF]))


def _folder(tmp_path, names=("a.jpg", "b.jpg"), extras=(), root_name="shoot"):
    """A cataloged source folder and an empty destination parent.

    ``extras`` are relative paths of files the catalog does not know (a
    sidecar, a file in a subfolder); a folder move carries them along.
    """
    db = Database(str(tmp_path / "test.db"))
    db.set_active_workspace(db.ensure_default_workspace())
    src = tmp_path / root_name
    src.mkdir()
    dst = tmp_path / "archive"
    dst.mkdir()
    fid = workspace_folders.add_folder(db, str(src), name=root_name)
    ids = {}
    for index, name in enumerate(names):
        path = src / name
        path.write_bytes(JPEG + bytes([index + 1]) * (64 + index))
        st = path.stat()
        ids[name] = photo_ingest.add_photo(
            db, folder_id=fid, filename=name, extension=".jpg",
            file_size=st.st_size, file_mtime=st.st_mtime,
        )
    for extra in extras:
        path = src / extra
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"extra " + extra.encode())
    return db, src, dst, fid, ids


def _copy_tree(src_path, dest_spec):
    for root, _dirs, files in os.walk(src_path):
        rel = os.path.relpath(root, src_path)
        target = dest_spec if rel == "." else os.path.join(dest_spec, rel)
        os.makedirs(target, exist_ok=True)
        for name in files:
            if not os.path.exists(os.path.join(target, name)):
                shutil.copy2(os.path.join(root, name), os.path.join(target, name))


def _transfer_that_corrupts(monkeypatch, victim):
    """Stand in for rsync: copy the tree, then damage ``victim`` at the
    destination without changing its size, and report success."""
    import move as move_mod

    def transfer(src_path, dest_spec, rsync_flags, total_files, progress_cb, **kwargs):
        already_there = os.path.exists(os.path.join(dest_spec, victim))
        _copy_tree(src_path, dest_spec)
        if not already_there:
            _same_size_corruption(os.path.join(dest_spec, victim))
        return 0, "", False

    monkeypatch.setattr(move_mod.folder_transfer, "_run_rsync_streamed", transfer)


def _after_verification(monkeypatch, change):
    """Run ``change()`` once the fresh-move verifier has passed."""
    import move as move_mod

    real_verify = move_mod.folder_transfer._fresh_copy_mismatch

    def verify_then_change(*args, **kwargs):
        mismatch = real_verify(*args, **kwargs)
        assert mismatch is None
        change()
        return mismatch

    monkeypatch.setattr(move_mod.folder_transfer, "_fresh_copy_mismatch", verify_then_change)


# -- one photo at a time ----------------------------------------------------


def test_copy_and_verify_accepts_an_exact_copy(tmp_path):
    from move import _copy_and_verify

    src, dst = tmp_path / "a.jpg", tmp_path / "out" / "a.jpg"
    src.write_bytes(JPEG + b"\x01" * 4096)
    dst.parent.mkdir()

    assert _copy_and_verify(str(src), str(dst)) is True
    assert dst.read_bytes() == src.read_bytes()


def test_copy_and_verify_rejects_a_same_size_copy_with_different_bytes(tmp_path, monkeypatch):
    import move.transfer as transfer

    src, dst = tmp_path / "a.jpg", tmp_path / "out" / "a.jpg"
    src.write_bytes(JPEG + b"\x01" * 4096)
    dst.parent.mkdir()

    def corrupting_copy(source, destination):
        shutil.copy2(source, destination)
        _same_size_corruption(destination)

    monkeypatch.setattr(transfer, "copy_via_temp", corrupting_copy)

    assert transfer._copy_and_verify(str(src), str(dst)) is False
    # The bad copy is gone, so a retry is not blocked by "already exists"
    # and a rescan cannot catalog it.
    assert not dst.exists()
    assert src.read_bytes() == JPEG + b"\x01" * 4096


def test_copy_and_verify_keeps_a_copy_it_could_not_compare(tmp_path, monkeypatch):
    """A read error proves nothing about the copy. On a failing source the
    copy may hold the only readable bytes, so it stays and the error says so."""
    import move.transfer as transfer

    src, dst = tmp_path / "a.jpg", tmp_path / "out" / "a.jpg"
    src.write_bytes(JPEG + b"\x01" * 64)
    dst.parent.mkdir()

    def unreadable(source, destination):
        raise OSError(5, "Input/output error")

    monkeypatch.setattr(transfer, "files_match", unreadable)

    with pytest.raises(transfer.CopyNotCompared) as raised:
        transfer._copy_and_verify(str(src), str(dst))
    assert "could not be compared with the original" in str(raised.value)
    assert "the copy was left in place and the original was kept" in str(raised.value)
    assert dst.read_bytes() == JPEG + b"\x01" * 64
    assert src.exists()


def test_move_photos_reports_a_copy_it_could_not_compare(tmp_path, monkeypatch):
    import move as move_mod
    import move.transfer as transfer

    db, src, dst, _fid, ids = _folder(tmp_path)

    def unreadable(source, destination):
        raise OSError(5, "Input/output error")

    monkeypatch.setattr(transfer, "files_match", unreadable)

    result = move_mod.move_photos(db=db, photo_ids=[ids["a.jpg"]], destination=str(dst))

    assert result["moved"] == 0
    assert len(result["errors"]) == 1
    assert result["errors"][0].startswith("a.jpg: the copy at ")
    assert "so the copy was left in place and the original was kept" in result["errors"][0]
    assert (src / "a.jpg").exists()
    assert (dst / "a.jpg").exists()


def test_move_photos_keeps_finished_copies_when_a_sidecar_cannot_be_compared(tmp_path, monkeypatch):
    """The photo's own copy already matched. A read error on its sidecar may
    mean the source is failing, so that copy is not thrown away."""
    import move as move_mod
    import move.transfer as transfer

    db, src, dst, _fid, ids = _folder(tmp_path, names=("a.jpg",), extras=("a.xmp",))
    real_match = transfer.files_match

    def sidecar_unreadable(source, destination):
        if str(source).endswith(".xmp"):
            raise OSError(5, "Input/output error")
        return real_match(source, destination)

    monkeypatch.setattr(transfer, "files_match", sidecar_unreadable)

    result = move_mod.move_photos(db=db, photo_ids=[ids["a.jpg"]], destination=str(dst))

    assert result["moved"] == 0
    assert len(result["errors"]) == 1
    assert result["errors"][0].startswith("a.xmp: the copy at ")
    assert "The copies of a.jpg made before it were left in place too" in result["errors"][0]
    assert (dst / "a.jpg").read_bytes() == (src / "a.jpg").read_bytes()
    assert (dst / "a.xmp").exists()
    assert (src / "a.jpg").exists()
    assert (src / "a.xmp").exists()


@pytest.mark.parametrize("merge", [False, True])
def test_a_source_folder_that_cannot_be_listed_fails_the_comparison(tmp_path, monkeypatch, merge):
    """A folder the verifier cannot list holds files nobody compared, so the
    move must not go on to remove them."""
    import move as move_mod

    db, src, dst, fid, _ids = _folder(tmp_path, extras=("sub/deep.txt",))
    locked = str(src / "sub")
    real_scandir = os.scandir
    verifying = []

    def scandir(path="."):
        if verifying and os.fspath(path) == locked:
            raise PermissionError(13, "Permission denied", locked)
        return real_scandir(path)

    def wrap(name):
        real = getattr(move_mod.folder_transfer, name)

        def verifier(*args, **kwargs):
            verifying.append(True)
            try:
                return real(*args, **kwargs)
            finally:
                verifying.clear()

        monkeypatch.setattr(move_mod.folder_transfer, name, verifier)

    wrap("_fresh_copy_mismatch")
    wrap("_first_missing_source_file")
    monkeypatch.setattr(move_mod.verify.os, "scandir", scandir)

    result = move_mod.move_folder(db=db, folder_id=fid, destination=str(dst), merge=merge)

    assert result["moved"] == 0
    assert any("Verification could not be completed" in error for error in result["errors"])
    assert (src / "sub" / "deep.txt").exists()
    assert (src / "a.jpg").exists()
    assert (dst / "shoot" / "a.jpg").exists()


def test_move_photos_keeps_the_original_when_the_copy_is_corrupted(tmp_path, monkeypatch):
    import move as move_mod
    import move.transfer as transfer

    db, src, dst, _fid, ids = _folder(tmp_path)
    real_copy = transfer.copy_via_temp

    def corrupt_a(source, destination):
        real_copy(source, destination)
        if os.path.basename(source) == "a.jpg":
            _same_size_corruption(destination)

    monkeypatch.setattr(transfer, "copy_via_temp", corrupt_a)
    original = (src / "a.jpg").read_bytes()

    result = move_mod.move_photos(
        db=db, photo_ids=[ids["a.jpg"], ids["b.jpg"]], destination=str(dst))

    assert result["moved"] == 1
    assert len(result["errors"]) == 1
    assert "a.jpg" in result["errors"][0]
    assert "did not match the original byte for byte" in result["errors"][0]
    assert (src / "a.jpg").read_bytes() == original
    assert not (dst / "a.jpg").exists()
    # The photo that copied correctly still moved.
    assert not (src / "b.jpg").exists()
    assert (dst / "b.jpg").exists()
    row = db.conn.execute(
        "SELECT f.path FROM photos p JOIN folders f ON f.id = p.folder_id "
        "WHERE p.id = ?", (ids["a.jpg"],)).fetchone()
    assert row["path"] == str(src)


# -- a whole folder ---------------------------------------------------------


def test_fresh_folder_move_keeps_originals_when_a_copy_has_wrong_bytes(tmp_path, monkeypatch):
    import move as move_mod

    db, src, dst, fid, _ids = _folder(tmp_path)
    originals = {name: (src / name).read_bytes() for name in ("a.jpg", "b.jpg")}
    _transfer_that_corrupts(monkeypatch, "a.jpg")

    result = move_mod.move_folder(db=db, folder_id=fid, destination=str(dst))

    assert result["moved"] == 0
    assert any("'a.jpg' contents differ at destination" in error
               and "Originals preserved" in error for error in result["errors"])
    assert {name: (src / name).read_bytes() for name in originals} == originals
    # The destination was created by this move, so the bad copy is removed.
    assert not (dst / "shoot").exists()
    assert db.conn.execute(
        "SELECT path FROM folders WHERE id = ?", (fid,)).fetchone()["path"] == str(src)


def test_merge_keeps_originals_and_the_destination_when_a_copy_has_wrong_bytes(tmp_path, monkeypatch):
    import move as move_mod

    db, src, dst, fid, _ids = _folder(tmp_path)
    (dst / "shoot").mkdir()
    (dst / "shoot" / "mine.jpg").write_bytes(b"already in the archive")
    _transfer_that_corrupts(monkeypatch, "b.jpg")

    result = move_mod.move_folder(db=db, folder_id=fid, destination=str(dst), merge=True)

    assert result["moved"] == 0
    assert any("b.jpg" in error and "Originals preserved" in error
               for error in result["errors"])
    assert (src / "a.jpg").exists()
    assert (src / "b.jpg").exists()
    # A destination that was already there is never removed.
    assert (dst / "shoot" / "mine.jpg").read_bytes() == b"already in the archive"


def test_merge_into_a_new_destination_keeps_originals_when_a_copy_has_wrong_bytes(tmp_path, monkeypatch):
    """A move that asks to merge is verified by content even when the
    destination did not exist yet (the NAS archive flows always ask)."""
    import move as move_mod

    db, src, dst, fid, _ids = _folder(tmp_path)
    _transfer_that_corrupts(monkeypatch, "a.jpg")

    result = move_mod.move_folder(db=db, folder_id=fid, destination=str(dst), merge=True)

    assert result["moved"] == 0
    assert any("a.jpg" in error and "Originals preserved" in error
               for error in result["errors"])
    assert (src / "a.jpg").exists()
    assert (src / "b.jpg").exists()
    assert not (dst / "shoot").exists()


def test_a_copy_is_kept_when_the_source_can_no_longer_be_read(tmp_path, monkeypatch):
    """If the source drive drops out after the copy, the copy may be the
    only one that can still be read. Failing verification must not remove it."""
    import move as move_mod

    db, src, dst, fid, _ids = _folder(tmp_path)

    def source_is_gone(src_path):
        raise OSError(6, "Device not configured", src_path)

    monkeypatch.setattr(move_mod.folder_transfer, "record_source_tree", source_is_gone)

    result = move_mod.move_folder(db=db, folder_id=fid, destination=str(dst))

    assert result["moved"] == 0
    assert any("the source folder could not be read" in error
               for error in result["errors"])
    assert (dst / "shoot" / "a.jpg").exists()
    assert (dst / "shoot" / "b.jpg").exists()
    assert (src / "a.jpg").exists()
    assert db.conn.execute(
        "SELECT path FROM folders WHERE id = ?", (fid,)).fetchone()["path"] == str(src)


@pytest.mark.parametrize("merge", [False, True])
def test_a_copy_is_kept_when_a_file_cannot_be_read_to_compare(tmp_path, monkeypatch, merge):
    """A read error proves nothing about the copy. On a failing source disk
    the copy may hold the only readable bytes, so it is not removed."""
    import move as move_mod

    db, src, dst, fid, _ids = _folder(tmp_path)

    def failing_disk(source, destination):
        raise OSError(5, "Input/output error", source)

    monkeypatch.setattr(move_mod.verify, "files_match", failing_disk)

    result = move_mod.move_folder(db=db, folder_id=fid, destination=str(dst), merge=merge)

    assert result["moved"] == 0
    assert any("Verification could not be completed" in error
               and "Originals preserved" in error for error in result["errors"])
    assert (dst / "shoot" / "a.jpg").exists()
    assert (dst / "shoot" / "b.jpg").exists()
    assert (src / "a.jpg").exists()
    assert (src / "b.jpg").exists()
    assert db.conn.execute(
        "SELECT path FROM folders WHERE id = ?", (fid,)).fetchone()["path"] == str(src)


def test_fresh_copy_mismatch_propagates_source_stat_failure(tmp_path, monkeypatch):
    """``_fresh_copy_mismatch`` must propagate an ``OSError`` from the source
    stat instead of returning a mismatch string. Returning one would let
    ``_refuse(copy_disproven=True)`` delete the fresh destination, which may
    hold the only readable bytes of the failing source file."""
    import move.verify as verify

    src = tmp_path / "shoot"
    src.mkdir()
    (src / "a.jpg").write_bytes(JPEG + b"\x01" * 64)
    (src / "b.jpg").write_bytes(JPEG + b"\x02" * 64)
    dst = tmp_path / "copy"
    _copy_tree(str(src), str(dst))

    real_stat = os.stat

    def failing_source_stat(path, *args, **kwargs):
        if str(path).startswith(str(src) + os.sep) and str(path).endswith("a.jpg"):
            raise OSError(5, "Input/output error", str(path))
        return real_stat(path, *args, **kwargs)

    monkeypatch.setattr(verify.os, "stat", failing_source_stat)

    with pytest.raises(OSError):
        verify._fresh_copy_mismatch(str(src), str(dst))


def test_fresh_move_keeps_destination_when_source_stat_fails_mid_verify(tmp_path, monkeypatch):
    """A source stat failure after the copy was made is inconclusive: the
    fresh destination may hold the only readable bytes. Neither side is
    removed, and the job reports the comparison could not be completed."""
    import move as move_mod

    db, src, dst, fid, _ids = _folder(tmp_path)

    def stat_fail_during_verify(src_path, transfer_dest, progress=None):
        raise OSError(5, "Input/output error", str(src_path))

    monkeypatch.setattr(
        move_mod.folder_transfer, "_fresh_copy_mismatch", stat_fail_during_verify)

    result = move_mod.move_folder(db=db, folder_id=fid, destination=str(dst))

    assert result["moved"] == 0
    assert any("Verification could not be completed" in error
               and "Originals preserved" in error for error in result["errors"])
    # The fresh destination the move just made is kept — it may hold the
    # only readable bytes of the file whose source stat failed.
    assert (dst / "shoot" / "a.jpg").exists()
    assert (dst / "shoot" / "b.jpg").exists()
    # The source is not removed either.
    assert (src / "a.jpg").exists()
    assert (src / "b.jpg").exists()
    assert db.conn.execute(
        "SELECT path FROM folders WHERE id = ?", (fid,)).fetchone()["path"] == str(src)


def test_a_copy_is_kept_when_a_source_file_vanishes_before_the_comparison(tmp_path, monkeypatch):
    """The other source files are still there, so the folder lists as
    present, but the copy now holds the only bytes of the file that went."""
    import move as move_mod

    db, src, dst, fid, _ids = _folder(tmp_path)
    real_record = move_mod.folder_transfer.record_source_tree
    calls = []

    def record_then_lose_a_file(src_path):
        record = real_record(src_path)
        if not calls:
            (src / "a.jpg").unlink()
        calls.append(src_path)
        return record

    monkeypatch.setattr(move_mod.folder_transfer, "record_source_tree", record_then_lose_a_file)

    result = move_mod.move_folder(db=db, folder_id=fid, destination=str(dst))

    assert result["moved"] == 0
    assert result["errors"]
    assert (dst / "shoot" / "a.jpg").exists()
    assert (src / "b.jpg").exists()
    assert db.conn.execute(
        "SELECT path FROM folders WHERE id = ?", (fid,)).fetchone()["path"] == str(src)


def test_a_file_added_after_verification_is_kept_and_reported(tmp_path, monkeypatch):
    import move as move_mod

    db, src, dst, fid, _ids = _folder(tmp_path, extras=("sub/notes.txt",))

    def add_files():
        (src / "late.jpg").write_bytes(b"written after the copy was verified")
        (src / "sub" / "late.xmp").write_text("<xmp>new edit</xmp>")

    _after_verification(monkeypatch, add_files)

    result = move_mod.move_folder(db=db, folder_id=fid, destination=str(dst))

    assert result["errors"] == []
    assert result["moved"] == 2
    # The late files were never copied, so they are still in the source.
    assert (src / "late.jpg").read_bytes() == b"written after the copy was verified"
    assert (src / "sub" / "late.xmp").read_text() == "<xmp>new edit</xmp>"
    assert not (dst / "shoot" / "late.jpg").exists()
    # The verified originals are gone and their copies are in place.
    assert not (src / "a.jpg").exists()
    assert not (src / "b.jpg").exists()
    assert not (src / "sub" / "notes.txt").exists()
    assert (dst / "shoot" / "a.jpg").exists()
    assert (dst / "shoot" / "sub" / "notes.txt").exists()
    late_xmp = os.path.join("sub", "late.xmp")
    assert result["source_leftovers"] == {
        "kept_total": 2, "kept": ["late.jpg", late_xmp],
        "failed_total": 0, "failed": [],
    }
    assert "2 files in the source folder were added or changed" in result["cleanup_error"]
    assert "late.jpg" in result["cleanup_error"]
    assert late_xmp in result["cleanup_error"]


def test_a_file_changed_after_verification_is_kept_and_reported(tmp_path, monkeypatch):
    import move as move_mod

    db, src, dst, fid, _ids = _folder(tmp_path)
    verified_bytes = (src / "a.jpg").read_bytes()

    def edit_a():
        (src / "a.jpg").write_bytes(b"edited after the copy was verified")

    _after_verification(monkeypatch, edit_a)

    result = move_mod.move_folder(db=db, folder_id=fid, destination=str(dst))

    assert result["errors"] == []
    assert (src / "a.jpg").read_bytes() == b"edited after the copy was verified"
    assert (dst / "shoot" / "a.jpg").read_bytes() == verified_bytes
    assert not (src / "b.jpg").exists()
    assert result["source_leftovers"]["kept"] == ["a.jpg"]
    assert "1 file in the source folder was added or changed" in result["cleanup_error"]


def test_a_clean_folder_move_removes_the_whole_source(tmp_path, monkeypatch):
    import move as move_mod

    db, src, dst, fid, _ids = _folder(
        tmp_path, extras=("a.xmp", "sub/deep/notes.txt"))
    (src / "empty").mkdir()

    def finder_writes_its_layout():
        (src / ".DS_Store").write_bytes(b"window layout")

    # Finder rewrites .DS_Store whenever the folder is open. It is not the
    # user's data, so it must not keep the source folder alive.
    _after_verification(monkeypatch, finder_writes_its_layout)

    result = move_mod.move_folder(db=db, folder_id=fid, destination=str(dst))

    assert result["errors"] == []
    assert result["moved"] == 2
    assert "cleanup_error" not in result
    assert "source_leftovers" not in result
    assert not src.exists()
    assert (dst / "shoot" / "a.xmp").read_bytes() == b"extra a.xmp"
    assert (dst / "shoot" / "sub" / "deep" / "notes.txt").exists()


# -- the cleanup itself -----------------------------------------------------


def test_cleanup_report_names_at_most_twenty_files_and_counts_all(tmp_path):
    from move.source_cleanup import record_source_tree, remove_verified_originals

    src = tmp_path / "src"
    src.mkdir()
    (src / "verified.jpg").write_bytes(b"verified")
    record = record_source_tree(str(src))
    for index in range(25):
        (src / f"late-{index:02d}.jpg").write_bytes(b"late")

    cleanup = remove_verified_originals(str(src), record)

    assert cleanup.removed == 1
    assert not (src / "verified.jpg").exists()
    report = cleanup.report()
    assert report["kept_total"] == 25
    assert report["kept"] == [f"late-{index:02d}.jpg" for index in range(20)]
    assert cleanup.describe().endswith("late-19.jpg, and 5 more")
    assert len(list(src.iterdir())) == 25


def test_cleanup_reports_an_original_it_could_not_delete(tmp_path, monkeypatch):
    from move import source_cleanup

    src = tmp_path / "src"
    src.mkdir()
    (src / "locked.jpg").write_bytes(b"locked")
    (src / "free.jpg").write_bytes(b"free")
    record = source_cleanup.record_source_tree(str(src))
    real_unlink = os.unlink

    def unlink(path, *args, **kwargs):
        if os.path.basename(path) == "locked.jpg":
            raise PermissionError(13, "Permission denied")
        return real_unlink(path, *args, **kwargs)

    monkeypatch.setattr(source_cleanup.os, "unlink", unlink)

    cleanup = source_cleanup.remove_verified_originals(str(src), record)

    assert cleanup.removed == 1
    assert cleanup.kept == []
    assert [name for name, _reason in cleanup.failed] == ["locked.jpg"]
    assert "1 copied original could not be deleted: locked.jpg" in cleanup.describe()
    assert (src / "locked.jpg").exists()
    assert not (src / "free.jpg").exists()


@pytest.mark.skipif(os.name == "nt", reason="creating symlinks needs a privilege on Windows")
def test_cleanup_removes_a_recorded_directory_link_without_following_it(tmp_path):
    from move.source_cleanup import record_source_tree, remove_verified_originals

    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "keep.jpg").write_bytes(b"not part of the move")
    src = tmp_path / "src"
    src.mkdir()
    (src / "a.jpg").write_bytes(b"a")
    os.symlink(str(outside), str(src / "link"))
    record = record_source_tree(str(src))

    cleanup = remove_verified_originals(str(src), record)

    assert cleanup.complete
    assert not src.exists()
    assert (outside / "keep.jpg").read_bytes() == b"not part of the move"


def test_cleanup_deletes_nothing_without_a_record(tmp_path):
    from move.source_cleanup import remove_verified_originals

    src = tmp_path / "src"
    src.mkdir()
    (src / "a.jpg").write_bytes(b"a")

    cleanup = remove_verified_originals(str(src), {})

    assert cleanup.removed == 0
    assert cleanup.kept == ["a.jpg"]
    assert (src / "a.jpg").exists()


# -- what the job says ------------------------------------------------------


def test_verify_step_label_says_what_is_compared():
    from services.move_steps import folder_move_steps

    def verify_label(**kwargs):
        return next(step["label"] for step in folder_move_steps(**kwargs)
                    if step["id"] == "verify")

    assert verify_label(remote=False) == "Verify copy (compare every byte)"
    assert verify_label(remote=True) == "Verify copy (compare checksums with the NAS)"
