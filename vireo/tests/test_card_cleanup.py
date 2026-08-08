"""Unit tests for card cleanup: manifest IO, classification, and the
scan/delete safety gates. Spec:
docs/superpowers/specs/2026-08-07-card-cleanup-design.md
"""
import os
import shutil
import unittest.mock
from datetime import UTC, datetime, timedelta

import card_cleanup
import pytest
from card_cleanup import (
    ManifestError,
    classify_source_files,
    load_manifest,
    manifest_path,
    prune_manifests,
    write_manifest,
)
from scanner import compute_file_hash as _sha


def _manifest(tmp_path, **overrides):
    m = {
        "schema_version": card_cleanup.MANIFEST_SCHEMA_VERSION,
        "scan_job_id": "scan-1",
        "created_at": datetime.now(UTC).isoformat(),
        "source_root": str(tmp_path / "card"),
        "recursive": True,
        "entries": [],
        "walk_errors": [],
        "totals": {},
    }
    m.update(overrides)
    return m


def test_write_then_load_roundtrip(tmp_path):
    mdir = str(tmp_path / "manifests")
    (tmp_path / "card").mkdir()
    write_manifest(mdir, _manifest(tmp_path))
    loaded = load_manifest(mdir, "scan-1")
    assert loaded["scan_job_id"] == "scan-1"


def test_write_is_atomic_no_leftover_tmp(tmp_path):
    mdir = str(tmp_path / "manifests")
    write_manifest(mdir, _manifest(tmp_path))
    names = os.listdir(mdir)
    assert names == ["scan-1.json"]


def test_load_missing_manifest_is_404(tmp_path):
    with pytest.raises(ManifestError) as exc:
        load_manifest(str(tmp_path), "nope")
    assert exc.value.http_status == 404


def test_load_rejects_corrupt_json(tmp_path):
    mdir = tmp_path / "manifests"
    mdir.mkdir()
    (mdir / "scan-1.json").write_text("{truncated")
    with pytest.raises(ManifestError) as exc:
        load_manifest(str(mdir), "scan-1")
    assert exc.value.http_status == 400


def test_load_rejects_unknown_schema(tmp_path):
    mdir = str(tmp_path / "manifests")
    write_manifest(mdir, _manifest(tmp_path, schema_version=99))
    with pytest.raises(ManifestError):
        load_manifest(mdir, "scan-1")


def test_load_rejects_missing_source_root(tmp_path):
    mdir = str(tmp_path / "manifests")
    write_manifest(mdir, _manifest(tmp_path, source_root=""))
    with pytest.raises(ManifestError):
        load_manifest(mdir, "scan-1")


def test_load_rejects_deletable_entry_outside_source_root(tmp_path):
    (tmp_path / "card").mkdir()
    entry = {
        "path": str(tmp_path / "elsewhere" / "x.nef"),
        "size": 1, "mtime_ns": 1, "hash": "h", "bucket": "deletable",
    }
    mdir = str(tmp_path / "manifests")
    write_manifest(mdir, _manifest(tmp_path, entries=[entry]))
    with pytest.raises(ManifestError):
        load_manifest(mdir, "scan-1")


def test_load_rejects_expired_manifest_at_request_time(tmp_path):
    (tmp_path / "card").mkdir()
    old = (datetime.now(UTC) - timedelta(days=8)).isoformat()
    mdir = str(tmp_path / "manifests")
    write_manifest(mdir, _manifest(tmp_path, created_at=old))
    with pytest.raises(ManifestError) as exc:
        load_manifest(mdir, "scan-1")
    assert exc.value.http_status == 404
    assert "re-scan" in str(exc.value)


def test_prune_removes_only_old_manifests(tmp_path):
    mdir = tmp_path / "manifests"
    mdir.mkdir()
    old_file = mdir / "old.json"
    new_file = mdir / "new.json"
    old_file.write_text("{}")
    new_file.write_text("{}")
    eight_days = 8 * 86400
    stale = os.stat(old_file).st_mtime - eight_days
    os.utime(old_file, (stale, stale))
    prune_manifests(str(mdir))
    assert not old_file.exists()
    assert new_file.exists()


def test_prune_removes_orphaned_tmp_files(tmp_path):
    mdir = tmp_path / "manifests"
    mdir.mkdir()
    orphan = mdir / ".scan-1.json.abc123.tmp"
    orphan.write_text("{")
    eight_days = 8 * 86400
    stale = os.stat(orphan).st_mtime - eight_days
    os.utime(orphan, (stale, stale))
    prune_manifests(str(mdir))
    assert not orphan.exists()


def test_hostile_scan_job_id_stays_inside_manifest_dir(tmp_path):
    (tmp_path / "card").mkdir()
    mdir = str(tmp_path / "manifests")
    hostile_id = "../../etc/passwd"
    path = manifest_path(mdir, hostile_id)
    assert os.path.dirname(path) == mdir

    write_manifest(mdir, _manifest(tmp_path, scan_job_id=hostile_id))
    loaded = load_manifest(mdir, hostile_id)
    assert loaded["scan_job_id"] == hostile_id
    assert sorted(os.listdir(mdir)) == ["passwd.json"]


def test_load_rejects_naive_created_at(tmp_path):
    (tmp_path / "card").mkdir()
    mdir = str(tmp_path / "manifests")
    naive = datetime.now().isoformat()  # deliberately tz-naive
    write_manifest(mdir, _manifest(tmp_path, created_at=naive))
    with pytest.raises(ManifestError) as exc:
        load_manifest(mdir, "scan-1")
    assert exc.value.http_status == 400


def test_write_manifest_json_dump_failure_leaves_no_tmp_file(tmp_path):
    mdir = str(tmp_path / "manifests")
    bad = _manifest(tmp_path, entries={"not serializable": {1, 2, 3}})
    with pytest.raises(TypeError):
        write_manifest(mdir, bad)
    assert os.listdir(mdir) == []


def test_load_rejects_non_dict_entry(tmp_path):
    (tmp_path / "card").mkdir()
    mdir = str(tmp_path / "manifests")
    write_manifest(mdir, _manifest(tmp_path, entries=[1]))
    with pytest.raises(ManifestError):
        load_manifest(mdir, "scan-1")


def test_load_rejects_deletable_entry_with_null_byte(tmp_path):
    (tmp_path / "card").mkdir()
    entry = {
        "path": str(tmp_path / "card" / "x\x00y.nef"),
        "size": 1, "mtime_ns": 1, "hash": "h", "bucket": "deletable",
    }
    mdir = str(tmp_path / "manifests")
    write_manifest(mdir, _manifest(tmp_path, entries=[entry]))
    with pytest.raises(ManifestError):
        load_manifest(mdir, "scan-1")


def test_load_rejects_source_root_with_null_byte(tmp_path):
    # isabs("/x\x00y") passes; realpath() raises ValueError. Covers the
    # hoisted source-root-unresolvable branch distinctly from a bad
    # entry path.
    mdir = str(tmp_path / "manifests")
    write_manifest(
        mdir, _manifest(tmp_path, source_root=str(tmp_path / "x\x00y")))
    with pytest.raises(ManifestError):
        load_manifest(mdir, "scan-1")


def test_load_rejects_deletable_entry_without_path(tmp_path):
    (tmp_path / "card").mkdir()
    entry = {"size": 1, "mtime_ns": 1, "hash": "h", "bucket": "deletable"}
    mdir = str(tmp_path / "manifests")
    write_manifest(mdir, _manifest(tmp_path, entries=[entry]))
    with pytest.raises(ManifestError) as exc:
        load_manifest(mdir, "scan-1")
    assert "malformed" in str(exc.value)


def test_load_rejects_deletable_entry_missing_size_hash_mtime(tmp_path):
    (tmp_path / "card").mkdir()
    entry = {
        "path": str(tmp_path / "card" / "x.nef"), "bucket": "deletable",
    }
    mdir = str(tmp_path / "manifests")
    write_manifest(mdir, _manifest(tmp_path, entries=[entry]))
    with pytest.raises(ManifestError) as exc:
        load_manifest(mdir, "scan-1")
    assert "malformed" in str(exc.value)


def _make_card(tmp_path):
    card = tmp_path / "card"
    (card / "DCIM" / "100").mkdir(parents=True)
    (card / "DCIM" / "100" / "IMG_0001.NEF").write_bytes(b"raw-one")
    (card / "DCIM" / "100" / "IMG_0002.JPG").write_bytes(b"jpg-two")
    (card / "DCIM" / "100" / "IMG_0001.XMP").write_bytes(b"sidecar")
    (card / "DCIM" / "100" / ".hidden.jpg").write_bytes(b"dot")
    (card / "MISC" / "sub").mkdir(parents=True)
    (card / "MISC" / "sub" / "firmware.bin").write_bytes(b"fw")
    (card / "ROOT_0001.JPG").write_bytes(b"root-jpg")
    (card / "readme.txt").write_bytes(b"txt")
    return card


def test_classify_buckets(tmp_path):
    card = _make_card(tmp_path)
    candidates, ignored = classify_source_files(str(card))
    cand_names = {p.name for p in candidates}
    ign_names = {p.name for p in ignored}
    assert cand_names == {"IMG_0001.NEF", "IMG_0002.JPG", "ROOT_0001.JPG"}
    assert ign_names == {
        "IMG_0001.XMP", ".hidden.jpg", "firmware.bin", "readme.txt",
    }


def test_classify_parity_with_discover_source_files(tmp_path):
    # The deletable set may never exceed what an import would consider a
    # photo — pin our filter to discovery's, byte for byte.
    from ingest import discover_source_files
    card = _make_card(tmp_path)
    candidates, _ = classify_source_files(str(card), recursive=True)
    assert candidates == discover_source_files(
        str(card), file_types="both", recursive=True)
    candidates_flat, _ = classify_source_files(str(card), recursive=False)
    assert candidates_flat == discover_source_files(
        str(card), file_types="both", recursive=False)


def test_classify_rejects_symlinks(tmp_path):
    # Codex P2: Path.is_file() follows symlinks, so a symlink to a real
    # photo would be classified as deletable — hashed for the target's
    # size, then os.remove would unlink only the link. delete_verified
    # would then credit the target's full size as "deleted bytes" even
    # though no card space is reclaimed and the actual photo still
    # exists. Symlinks must not enter the deletable set.
    card = tmp_path / "card"
    card.mkdir()
    real = tmp_path / "elsewhere" / "IMG_0001.NEF"
    real.parent.mkdir()
    real.write_bytes(b"raw-one")
    link = card / "IMG_LINK.NEF"
    try:
        os.symlink(str(real), str(link))
    except (OSError, NotImplementedError):
        pytest.skip("symlinks unsupported on this filesystem")
    # Non-symlink control so we know classification is still running.
    (card / "IMG_REAL.NEF").write_bytes(b"raw-two")
    candidates, ignored = classify_source_files(str(card))
    cand_names = {p.name for p in candidates}
    ign_names = {p.name for p in ignored}
    assert "IMG_LINK.NEF" not in cand_names
    assert "IMG_LINK.NEF" not in ign_names
    assert cand_names == {"IMG_REAL.NEF"}


def test_classify_missing_source_reports_onerror(tmp_path):
    errors = []
    candidates, ignored = classify_source_files(
        str(tmp_path / "nope"), onerror=errors.append)
    assert candidates == [] and ignored == []
    assert len(errors) == 1 and isinstance(errors[0], OSError)


# The `db` fixture comes from vireo/tests/conftest.py (~line 158) — a
# Database on a temp file. Do not redefine it here.


def _archive_photo(db, tmp_path, name="IMG_0001.NEF", content=b"raw-one",
                   hash_status="ok", folder="archive/2026/2026-08-01"):
    """Create an archive file on disk + its cataloged, verified row."""
    folder_path = tmp_path / folder
    folder_path.mkdir(parents=True, exist_ok=True)
    f = folder_path / name
    f.write_bytes(content)
    st = os.stat(f)
    fid = db.add_folder(str(folder_path))
    pid = db.add_photo(
        folder_id=fid, filename=name, extension=os.path.splitext(name)[1],
        file_size=st.st_size, file_mtime=st.st_mtime,
        file_hash=_sha(str(f)),
    )
    if hash_status is not None:
        db.update_photo_hash_check(pid, hash_status)
    return f, pid


def _card_file(tmp_path, name="IMG_0001.NEF", content=b"raw-one"):
    card = tmp_path / "card" / "DCIM"
    card.mkdir(parents=True, exist_ok=True)
    f = card / name
    f.write_bytes(content)
    return f


def _scan(db, tmp_path, **kwargs):
    return card_cleanup.scan_card(
        db, str(tmp_path / "card"), True,
        str(tmp_path / "manifests"), "scan-1", **kwargs)


def _entries(result, bucket):
    return [e for e in result["entries"] if e["bucket"] == bucket]


def test_scan_verified_file_is_deletable(db, tmp_path):
    archive_file, _ = _archive_photo(db, tmp_path)
    _card_file(tmp_path)
    result = _scan(db, tmp_path)
    deletable = _entries(result, "deletable")
    assert len(deletable) == 1
    assert deletable[0]["archive_path"] == str(archive_file)
    assert result["totals"]["deletable"]["count"] == 1
    # Manifest landed on disk and revalidates.
    loaded = load_manifest(str(tmp_path / "manifests"), "scan-1")
    assert loaded["source_root"] == os.path.realpath(str(tmp_path / "card"))


def test_scan_uncataloged_file_kept(db, tmp_path):
    _card_file(tmp_path, content=b"never-imported")
    result = _scan(db, tmp_path)
    kept = _entries(result, "kept")
    assert len(kept) == 1 and "not in catalog" in kept[0]["reason"]


def test_scan_null_hash_status_kept_with_audit_remedy(db, tmp_path):
    # Scan-cataloged archives: file_hash set, hash_status NULL. Kept —
    # and the reason must point at the remedy, or the tool reads broken.
    _archive_photo(db, tmp_path, hash_status=None)
    _card_file(tmp_path)
    result = _scan(db, tmp_path)
    kept = _entries(result, "kept")
    assert len(kept) == 1
    assert "integrity audit" in kept[0]["reason"]


def test_scan_failed_hash_status_kept_with_audit_remedy(db, tmp_path):
    # hash_status is a non-ok STRING (a prior verify run flagged the file),
    # not NULL — must be kept exactly like the null/unverified case, not
    # mistaken for "ok" by a truthy/None-only check.
    _archive_photo(db, tmp_path, hash_status="failed")
    _card_file(tmp_path)
    result = _scan(db, tmp_path)
    kept = _entries(result, "kept")
    assert len(kept) == 1
    assert "integrity audit" in kept[0]["reason"]


def test_scan_archive_file_missing_kept(db, tmp_path):
    archive_file, _ = _archive_photo(db, tmp_path)
    _card_file(tmp_path)
    os.unlink(archive_file)
    result = _scan(db, tmp_path)
    assert len(_entries(result, "kept")) == 1
    assert len(_entries(result, "deletable")) == 0


def test_scan_archive_mtime_off_baseline_kept(db, tmp_path):
    # Exact equality, not the audit's 1s window: any drift keeps the file.
    archive_file, _ = _archive_photo(db, tmp_path)
    _card_file(tmp_path)
    st = os.stat(archive_file)
    os.utime(archive_file, (st.st_atime, st.st_mtime + 0.5))
    result = _scan(db, tmp_path)
    assert len(_entries(result, "deletable")) == 0
    assert "changed since verification" in _entries(result, "kept")[0]["reason"]


def test_scan_self_match_inside_source_never_qualifies(db, tmp_path):
    # The only catalog copy lives inside the selected source tree: the
    # file must NOT be deletable (it would be deleting the archive copy).
    _archive_photo(db, tmp_path, folder="card/DCIM")
    result = _scan(db, tmp_path)
    assert len(_entries(result, "deletable")) == 0
    kept = _entries(result, "kept")
    assert len(kept) == 1 and "inside the selected source" in kept[0]["reason"]


def test_qualify_rejects_hardlink_alias_of_card_file(db, tmp_path):
    # Mount-alias/samefile gate: the cataloged "archive copy" lives
    # outside the source tree by path, but it's a hardlink to the card
    # file itself — same dev+inode. Deleting the card file would leave
    # the archive path as the only name for bytes we just proved existed
    # twice; the spec says such a row never qualifies.
    card = _card_file(tmp_path)
    folder_path = tmp_path / "archive" / "2026"
    folder_path.mkdir(parents=True)
    alias = folder_path / "IMG_0001.NEF"
    try:
        os.link(card, alias)
    except (OSError, NotImplementedError):
        pytest.skip("hardlinks unsupported on this filesystem")
    st = os.stat(alias)
    fid = db.add_folder(str(folder_path))
    pid = db.add_photo(
        folder_id=fid, filename="IMG_0001.NEF", extension=".NEF",
        file_size=st.st_size, file_mtime=st.st_mtime,
        file_hash=_sha(str(alias)),
    )
    db.update_photo_hash_check(pid, "ok")
    result = _scan(db, tmp_path)
    assert len(_entries(result, "deletable")) == 0
    kept = _entries(result, "kept")
    assert len(kept) == 1 and "inside the selected source" in kept[0]["reason"]


def test_scan_duplicate_card_files_both_deletable(db, tmp_path):
    _archive_photo(db, tmp_path)
    _card_file(tmp_path, name="IMG_0001.NEF")
    _card_file(tmp_path, name="IMG_0001_copy.NEF")
    result = _scan(db, tmp_path)
    assert len(_entries(result, "deletable")) == 2


def test_scan_two_rows_one_qualifying_is_deletable(db, tmp_path):
    # Same hash cataloged twice; only one row passes → still deletable,
    # preview shows the passing row's path.
    bad_archive, _ = _archive_photo(db, tmp_path, folder="archive/a")
    good_archive, _ = _archive_photo(db, tmp_path, folder="archive/b")
    os.unlink(bad_archive)
    _card_file(tmp_path)
    result = _scan(db, tmp_path)
    deletable = _entries(result, "deletable")
    assert len(deletable) == 1
    assert deletable[0]["archive_path"] == str(good_archive)


def test_scan_cancellation_writes_no_manifest(db, tmp_path):
    _archive_photo(db, tmp_path)
    _card_file(tmp_path)
    result = _scan(db, tmp_path, should_cancel=lambda: True)
    assert result["cancelled"] is True
    assert not os.path.exists(
        card_cleanup.manifest_path(str(tmp_path / "manifests"), "scan-1"))


def test_scan_hashes_each_card_file_once(db, tmp_path, monkeypatch):
    _archive_photo(db, tmp_path)
    _card_file(tmp_path)
    calls = []
    real = card_cleanup.compute_file_hash

    def counting(path, *a, **kw):
        calls.append(str(path))
        return real(path, *a, **kw)

    monkeypatch.setattr(card_cleanup, "compute_file_hash", counting)
    _scan(db, tmp_path)
    card_calls = [p for p in calls if "card" in p]
    assert len(card_calls) == 1


def test_qualify_rows_null_archive_stat_baseline_keeps(db, tmp_path):
    # Scan-cataloged rows can have NULL file_size/file_mtime even when
    # hash_status is "ok" (e.g. legacy rows) — must not raise, must keep.
    archive_file, _ = _archive_photo(db, tmp_path)
    card = _card_file(tmp_path)
    source_root_real = os.path.realpath(str(tmp_path / "card"))
    rows = [{
        "filename": os.path.basename(str(archive_file)),
        "file_size": None, "file_mtime": None,
        "hash_status": "ok",
        "folder_path": os.path.dirname(str(archive_file)),
    }]
    archive_path, reason = card_cleanup.qualify_rows(
        rows, source_root_real, str(card))
    assert archive_path is None
    assert reason == card_cleanup.KEEP_ARCHIVE_CHANGED


def test_qualify_rows_missing_card_file_unreadable(db, tmp_path):
    # Unreachable from scan_card (it only calls qualify_rows on files it
    # just stat'd) — but delete_verified could race a vanished card file
    # in between its own stat and the archive gate call.
    archive_file, _ = _archive_photo(db, tmp_path)
    source_root_real = os.path.realpath(str(tmp_path / "card"))
    rows = [{
        "filename": os.path.basename(str(archive_file)),
        "file_size": None, "file_mtime": None,
        "hash_status": "ok",
        "folder_path": os.path.dirname(str(archive_file)),
    }]
    archive_path, reason = card_cleanup.qualify_rows(
        rows, source_root_real, str(tmp_path / "card" / "gone.NEF"))
    assert archive_path is None
    assert reason == card_cleanup.KEEP_UNREADABLE


def _scan_then_delete(db, tmp_path, mutate=None, should_cancel=None):
    scan = _scan(db, tmp_path)
    assert scan["totals"]["deletable"]["count"] >= 1
    manifest = load_manifest(str(tmp_path / "manifests"), "scan-1")
    if mutate is not None:
        mutate()
    return card_cleanup.delete_verified(
        db, manifest, should_cancel=should_cancel)


def test_delete_happy_path_two_duplicates_both_deleted(db, tmp_path):
    # Spec: two identical card files matching one archive photo — both
    # deletable, both deleted.
    _archive_photo(db, tmp_path)
    card_a = _card_file(tmp_path, name="IMG_0001.NEF")
    card_b = _card_file(tmp_path, name="IMG_0001_copy.NEF")
    summary = _scan_then_delete(db, tmp_path)
    assert summary["deleted"] == 2
    assert not card_a.exists() and not card_b.exists()
    assert summary["skipped"] == [] and summary["failed"] == []


def test_delete_skips_file_changed_since_scan(db, tmp_path):
    _archive_photo(db, tmp_path)
    card = _card_file(tmp_path)

    def rewrite():
        card.write_bytes(b"new-shot-reusing-name")

    summary = _scan_then_delete(db, tmp_path, mutate=rewrite)
    assert summary["deleted"] == 0
    assert len(summary["skipped"]) == 1
    assert card.exists()


def test_delete_rehash_catches_same_size_same_mtime_swap(db, tmp_path):
    # Same byte count, mtime forced back to the manifest value: only the
    # delete-time re-hash can catch this (FAT mtimes are 2s-granular).
    _archive_photo(db, tmp_path)
    card = _card_file(tmp_path)  # content b"raw-one", 7 bytes
    st = os.stat(card)

    def swap():
        card.write_bytes(b"raw-two")  # also 7 bytes
        os.utime(card, ns=(st.st_atime_ns, st.st_mtime_ns))

    summary = _scan_then_delete(db, tmp_path, mutate=swap)
    assert summary["deleted"] == 0
    assert len(summary["skipped"]) == 1
    assert card.exists()


def test_delete_archive_removed_after_scan_skips(db, tmp_path):
    archive_file, _ = _archive_photo(db, tmp_path)
    card = _card_file(tmp_path)
    summary = _scan_then_delete(
        db, tmp_path, mutate=lambda: os.unlink(archive_file))
    assert summary["deleted"] == 0
    assert card.exists()
    assert "archive" in summary["skipped"][0]["reason"]


def test_delete_archive_mutated_after_scan_skips(db, tmp_path):
    # The archive file survives (unlike the removed case above) but its
    # bytes change between scan and delete — the delete-time re-stat's
    # size/mtime mismatch must still catch it and keep the card file.
    archive_file, _ = _archive_photo(db, tmp_path)
    card = _card_file(tmp_path)

    def mutate_archive():
        archive_file.write_bytes(b"mutated!")

    summary = _scan_then_delete(db, tmp_path, mutate=mutate_archive)
    assert summary["deleted"] == 0
    assert card.exists()
    assert len(summary["skipped"]) == 1
    assert "archive" in summary["skipped"][0]["reason"]


def test_delete_archive_mount_unreachable_skips_all(db, tmp_path):
    # Whole archive tree gone (e.g. an unmounted network volume), not
    # just one file — every deletable card file must be skipped, none
    # deleted, and each reason must mention the archive.
    _archive_photo(db, tmp_path)
    card_a = _card_file(tmp_path, name="IMG_0001.NEF")
    card_b = _card_file(tmp_path, name="IMG_0002.NEF")
    archive_root = tmp_path / "archive"

    def remove_archive_tree():
        shutil.rmtree(archive_root)

    summary = _scan_then_delete(db, tmp_path, mutate=remove_archive_tree)
    assert summary["deleted"] == 0
    assert card_a.exists() and card_b.exists()
    assert len(summary["skipped"]) == 2
    assert all("archive" in s["reason"] for s in summary["skipped"])


def test_delete_time_inside_source_uses_manifest_source_root(db, tmp_path):
    # The catalog row qualifies at scan time (archive copy is outside the
    # source), but before delete runs the archive folder is repointed
    # (via SQL, as if re-cataloged) to a path inside the selected source
    # tree. delete_verified must re-derive source_root from the manifest
    # it was handed and re-run the inside-source check itself — this pins
    # that wiring, not just qualify_rows' logic (already covered at scan
    # time by test_scan_self_match_inside_source_never_qualifies).
    archive_file, pid = _archive_photo(db, tmp_path)
    card = _card_file(tmp_path)
    row = db.conn.execute(
        "SELECT folder_id FROM photos WHERE id = ?", (pid,)).fetchone()
    folder_id = row["folder_id"]
    card_dir = str(tmp_path / "card" / "DCIM")

    def repoint_inside_source():
        db.conn.execute(
            "UPDATE folders SET path = ? WHERE id = ?",
            (card_dir, folder_id))
        db.conn.commit()

    summary = _scan_then_delete(db, tmp_path, mutate=repoint_inside_source)
    assert summary["deleted"] == 0
    assert card.exists()
    assert archive_file.exists()  # untouched — never re-read by delete
    assert len(summary["skipped"]) == 1
    assert "inside the selected source" in summary["skipped"][0]["reason"]


def test_delete_no_stat_reuse_across_duplicates(db, tmp_path):
    # Two identical card files; archive copy vanishes after the first
    # deletion. The second file's own fresh stat must fail — a cached
    # scan-time (or first-delete-time) result would wrongly authorize it.
    archive_file, _ = _archive_photo(db, tmp_path)
    card_a = _card_file(tmp_path, name="IMG_0001.NEF")
    card_b = _card_file(tmp_path, name="IMG_0002.NEF")
    scan = _scan(db, tmp_path)
    assert scan["totals"]["deletable"]["count"] == 2
    manifest = load_manifest(str(tmp_path / "manifests"), "scan-1")

    deleted_once = []
    real_remove = os.remove

    def remove_then_kill_archive(path, *a, **kw):
        real_remove(path, *a, **kw)
        if not deleted_once:
            deleted_once.append(path)
            real_remove(archive_file)

    with unittest.mock.patch.object(
            card_cleanup.os, "remove", remove_then_kill_archive):
        summary = card_cleanup.delete_verified(db, manifest)
    assert summary["deleted"] == 1
    assert len(summary["skipped"]) == 1
    assert card_a.exists() != card_b.exists()  # exactly one survived


def test_delete_cancellation_honest_summary(db, tmp_path):
    _archive_photo(db, tmp_path)
    _card_file(tmp_path, name="IMG_0001.NEF")
    _card_file(tmp_path, name="IMG_0002.NEF")
    calls = []

    def cancel_after_first():
        calls.append(1)
        return len(calls) > 1

    summary = _scan_then_delete(
        db, tmp_path, should_cancel=cancel_after_first)
    assert summary["cancelled"] is True
    assert summary["deleted"] + summary["remaining"] == 2


def test_delete_vanished_card_file_counts_skipped(db, tmp_path):
    _archive_photo(db, tmp_path)
    card = _card_file(tmp_path)
    summary = _scan_then_delete(db, tmp_path, mutate=lambda: os.unlink(card))
    assert summary["deleted"] == 0
    assert "already gone" in summary["skipped"][0]["reason"]


def test_delete_per_file_failure_continues(db, tmp_path):
    # A permission error on one file is recorded as failed; the job
    # moves on and still deletes the rest.
    _archive_photo(db, tmp_path)
    card_a = _card_file(tmp_path, name="IMG_0001.NEF")
    card_b = _card_file(tmp_path, name="IMG_0002.NEF")
    scan = _scan(db, tmp_path)
    assert scan["totals"]["deletable"]["count"] == 2
    manifest = load_manifest(str(tmp_path / "manifests"), "scan-1")
    real_remove = os.remove
    failed_path = str(card_a)

    def failing_remove(path, *a, **kw):
        if str(path) == failed_path:
            raise PermissionError(13, "read-only card", failed_path)
        real_remove(path, *a, **kw)

    with unittest.mock.patch.object(
            card_cleanup.os, "remove", failing_remove):
        summary = card_cleanup.delete_verified(db, manifest)
    assert summary["deleted"] == 1
    assert len(summary["failed"]) == 1
    assert summary["failed"][0]["path"] == failed_path
    assert card_a.exists() and not card_b.exists()


def test_delete_only_touches_deletable_bucket(db, tmp_path):
    _archive_photo(db, tmp_path)
    _card_file(tmp_path)
    stray = _card_file(tmp_path, name="IMG_KEEP.NEF", content=b"unimported")
    summary = _scan_then_delete(db, tmp_path)
    assert summary["deleted"] == 1
    assert stray.exists()


def test_delete_skips_file_replaced_during_archive_gate(db, tmp_path):
    # TOCTOU guard (Codex P1): the pathname is replaced while the archive
    # gate runs — after the card re-hash, before os.remove. The
    # replacement has the SAME size and the mtime forced back to the
    # manifest value, so only the final inode re-stat can catch it.
    _archive_photo(db, tmp_path)
    card = _card_file(tmp_path)  # content b"raw-one", 7 bytes
    scan = _scan(db, tmp_path)
    assert scan["totals"]["deletable"]["count"] == 1
    manifest = load_manifest(str(tmp_path / "manifests"), "scan-1")
    entry = next(e for e in manifest["entries"] if e["bucket"] == "deletable")
    real_fetch = card_cleanup.fetch_rows_by_hash

    def fetch_then_replace(db_arg, file_hash):
        rows = real_fetch(db_arg, file_hash)
        # Swap via a coexisting temp file + os.replace: unlink-then-
        # recreate would let ext4 hand the replacement the freed inode
        # number, silently defeating the inode gate this test pins.
        # With both files alive before the swap, the inodes are distinct
        # on every POSIX filesystem — and rename is the realistic
        # replacement primitive anyway.
        tmp = card.parent / "replacement.tmp"
        tmp.write_bytes(b"NEW-ONE")          # same 7-byte size
        os.utime(tmp, ns=(entry["mtime_ns"], entry["mtime_ns"]))
        os.replace(tmp, card)
        return rows

    with unittest.mock.patch.object(
            card_cleanup, "fetch_rows_by_hash", fetch_then_replace):
        summary = card_cleanup.delete_verified(db, manifest)
    assert summary["deleted"] == 0
    assert len(summary["skipped"]) == 1
    assert card.exists()
    assert card.read_bytes() == b"NEW-ONE"


def test_delete_skips_path_redirected_outside_source(db, tmp_path):
    # Second Codex P1: after the manifest is loaded, the card's DCIM
    # directory is replaced with a symlink to an external directory
    # holding a byte-identical file with the manifest's size and mtime.
    # Every content gate passes (the bytes really are verified-archived);
    # only the deletion-time containment re-check can refuse to unlink a
    # path that no longer resolves inside the scanned source.
    _archive_photo(db, tmp_path)
    _card_file(tmp_path)
    scan = _scan(db, tmp_path)
    assert scan["totals"]["deletable"]["count"] == 1
    manifest = load_manifest(str(tmp_path / "manifests"), "scan-1")
    entry = next(e for e in manifest["entries"] if e["bucket"] == "deletable")

    outside = tmp_path / "outside"
    outside.mkdir()
    decoy = outside / "IMG_0001.NEF"
    decoy.write_bytes(b"raw-one")
    os.utime(decoy, ns=(entry["mtime_ns"], entry["mtime_ns"]))

    dcim = tmp_path / "card" / "DCIM"
    import shutil
    shutil.rmtree(dcim)
    try:
        os.symlink(str(outside), str(dcim))
    except (OSError, NotImplementedError):
        pytest.skip("symlinks unsupported on this filesystem")

    summary = card_cleanup.delete_verified(db, manifest)
    assert summary["deleted"] == 0
    assert len(summary["skipped"]) == 1
    assert "no longer resolves inside" in summary["skipped"][0]["reason"]
    assert decoy.exists() and decoy.read_bytes() == b"raw-one"


def test_delete_skips_when_card_path_becomes_symlink_post_scan(db, tmp_path):
    # Codex P2: scan classifies only regular files, but a post-scan swap
    # of a scanned pathname for a symlink to a byte-identical file with
    # the same size and mtime would let os.stat follow the link. The
    # delete would then unlink only the symlink while crediting the
    # target's full bytes as reclaimed. lstat + explicit S_ISLNK
    # rejection at the delete gate keeps the destructive step anchored
    # to the object the scan hashed.
    _archive_photo(db, tmp_path)
    card = _card_file(tmp_path)  # content b"raw-one", 7 bytes
    scan = _scan(db, tmp_path)
    assert scan["totals"]["deletable"]["count"] == 1
    manifest = load_manifest(str(tmp_path / "manifests"), "scan-1")
    entry = next(e for e in manifest["entries"] if e["bucket"] == "deletable")

    target = card.parent / "IMG_0001_copy.NEF"
    target.write_bytes(b"raw-one")
    os.utime(target, ns=(entry["mtime_ns"], entry["mtime_ns"]))
    os.unlink(card)
    try:
        os.symlink(str(target), str(card))
    except (OSError, NotImplementedError):
        pytest.skip("symlinks unsupported on this filesystem")

    summary = card_cleanup.delete_verified(db, manifest)
    assert summary["deleted"] == 0
    assert summary["deleted_bytes"] == 0
    assert len(summary["skipped"]) == 1
    assert summary["skipped"][0]["reason"] == card_cleanup.SKIP_SYMLINK
    # The symlink itself and the byte-identical target both survive: the
    # tool refused to operate on a link it never classified.
    assert card.is_symlink()
    assert target.exists() and target.read_bytes() == b"raw-one"
