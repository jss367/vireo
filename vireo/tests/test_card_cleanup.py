"""Unit tests for card cleanup: manifest IO, classification, and the
scan/delete safety gates. Spec:
docs/superpowers/specs/2026-08-07-card-cleanup-design.md
"""
import os
import shutil
import stat
import sys
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
    assert loaded["revision"] == card_cleanup.INITIAL_MANIFEST_REVISION


@pytest.mark.parametrize("revision", [True, False, 0, -1, "1", 1.5])
def test_load_rejects_invalid_manifest_revision(tmp_path, revision):
    (tmp_path / "card").mkdir()
    mdir = str(tmp_path / "manifests")
    write_manifest(mdir, _manifest(tmp_path, revision=revision))
    with pytest.raises(ManifestError, match="revision invalid"):
        load_manifest(mdir, "scan-1")


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
        db.audit.update_photo_hash_check(pid, hash_status)
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


def test_scan_null_hash_status_kept_with_scoped_verify_remedy(db, tmp_path):
    # Scan-cataloged archives: file_hash set, hash_status NULL. Kept —
    # and the reason must point at the remedy, or the tool reads broken.
    _archive_photo(db, tmp_path, hash_status=None)
    _card_file(tmp_path)
    result = _scan(db, tmp_path)
    kept = _entries(result, "kept")
    assert len(kept) == 1
    assert kept[0]["reason"] == card_cleanup.KEEP_NOT_VERIFIED


def test_scan_failed_hash_status_kept_but_not_audit_remedy(db, tmp_path):
    # hash_status is a non-ok STRING (a prior verify run flagged the file),
    # not NULL — must be kept exactly like the null/unverified case, not
    # mistaken for "ok" by a truthy/None-only check. Distinct from the
    # NULL case: re-running verification would just reproduce the same
    # bad verdict, so the reason must NOT point at the integrity audit
    # (Codex P2 review) — the callout keys off that phrase and would
    # otherwise ask the user to re-verify a file it can't help.
    _archive_photo(db, tmp_path, hash_status="modified")
    _card_file(tmp_path)
    result = _scan(db, tmp_path)
    kept = _entries(result, "kept")
    assert len(kept) == 1
    assert "integrity audit" not in kept[0]["reason"]
    assert "failed" in kept[0]["reason"]
    assert "Audit page" in kept[0]["reason"]


@pytest.mark.parametrize("bad_status", ["modified", "corrupt", "unreadable"])
def test_scan_specific_failed_hash_statuses_all_kept_with_failed_reason(
        db, tmp_path, bad_status):
    # All three real failed-verdict values from verify_hashes should route
    # to KEEP_ARCHIVE_HASH_FAILED, not KEEP_NOT_VERIFIED — the audit page
    # is the remedy for each, not another verify run.
    _archive_photo(db, tmp_path, hash_status=bad_status)
    _card_file(tmp_path)
    result = _scan(db, tmp_path)
    kept = _entries(result, "kept")
    assert len(kept) == 1
    assert kept[0]["reason"] == card_cleanup.KEEP_ARCHIVE_HASH_FAILED


def test_scan_mixed_null_and_failed_prefers_verify_remedy(db, tmp_path):
    # Two archive rows share a card file's hash — one never checked, one
    # already flagged. The NULL row is remediable by a verify run (its
    # verdict could turn "ok"), so the reason must stay on the audit
    # remedy; a failed row alongside it must not silently downgrade the
    # message to "see the Audit page" and lose the offer to verify.
    _archive_photo(db, tmp_path, name="IMG_A.NEF", content=b"raw-one",
                   hash_status=None, folder="archive/2026/a")
    _archive_photo(db, tmp_path, name="IMG_B.NEF", content=b"raw-one",
                   hash_status="modified", folder="archive/2026/b")
    _card_file(tmp_path)
    result = _scan(db, tmp_path)
    kept = _entries(result, "kept")
    assert len(kept) == 1
    assert kept[0]["reason"] == card_cleanup.KEEP_NOT_VERIFIED


def test_scoped_verify_reads_only_matching_archive_and_refreshes_manifest(
        db, tmp_path, monkeypatch):
    archive_file, pid = _archive_photo(db, tmp_path, hash_status=None)
    unrelated, unrelated_pid = _archive_photo(
        db, tmp_path, name="OTHER.NEF", content=b"other",
        hash_status=None, folder="archive/other")
    _card_file(tmp_path)
    scan = _scan(db, tmp_path)
    assert scan["totals"]["deletable"]["count"] == 0

    manifest_dir = str(tmp_path / "manifests")
    manifest = load_manifest(manifest_dir, "scan-1")
    # verify_manifest_archives hashes a pinned descriptor now (CodeRabbit
    # TOCTOU fix), so callers no longer name the path. Correlate hashed
    # descriptors back to files via fstat inode.
    hashed_inodes = []
    real_hash = card_cleanup.compute_fd_hash

    def track(fd, *a, **kw):
        hashed_inodes.append(os.fstat(fd).st_ino)
        return real_hash(fd, *a, **kw)

    monkeypatch.setattr(card_cleanup, "compute_fd_hash", track)
    result = card_cleanup.verify_manifest_archives(
        db, manifest, manifest_dir)

    assert hashed_inodes == [os.stat(archive_file).st_ino]
    assert os.stat(unrelated).st_ino not in hashed_inodes
    assert result["archive_files_read"] == 1
    assert result["unblocked_files"] == 1
    refreshed = load_manifest(manifest_dir, "scan-1")
    assert refreshed["totals"]["deletable"]["count"] == 1
    assert _entries(refreshed, "deletable")[0]["archive_path"] == str(
        archive_file)
    statuses = {
        row["id"]: row["hash_status"]
        for row in db.conn.execute(
            "SELECT id, hash_status FROM photos WHERE id IN (?, ?)",
            (pid, unrelated_pid),
        )
    }
    assert statuses == {pid: "ok", unrelated_pid: None}


def test_scoped_verify_hashes_duplicate_card_content_once(db, tmp_path):
    _archive_photo(db, tmp_path, hash_status=None)
    _card_file(tmp_path, name="IMG_0001.NEF")
    _card_file(tmp_path, name="IMG_0002.NEF")
    _scan(db, tmp_path)
    manifest_dir = str(tmp_path / "manifests")
    result = card_cleanup.verify_manifest_archives(
        db, load_manifest(manifest_dir, "scan-1"), manifest_dir)
    assert result["hashes_total"] == 1
    assert result["archive_files_read"] == 1
    assert result["unblocked_files"] == 2
    refreshed = load_manifest(manifest_dir, "scan-1")
    assert refreshed["totals"]["deletable"]["count"] == 2


def test_scoped_verify_mismatch_stays_kept_and_records_failed_verdict(
        db, tmp_path):
    archive_file, pid = _archive_photo(db, tmp_path, hash_status=None)
    _card_file(tmp_path)
    _scan(db, tmp_path)
    original_mtime = os.stat(archive_file).st_mtime_ns
    archive_file.write_bytes(b"NEW-ONE")  # same size as b"raw-one"
    os.utime(archive_file, ns=(original_mtime, original_mtime))

    manifest_dir = str(tmp_path / "manifests")
    result = card_cleanup.verify_manifest_archives(
        db, load_manifest(manifest_dir, "scan-1"), manifest_dir)

    assert result["verified"] == 0
    assert result["corrupt"] == 1
    assert result["unblocked_files"] == 0
    refreshed = load_manifest(manifest_dir, "scan-1")
    assert refreshed["totals"]["deletable"]["count"] == 0
    assert _entries(refreshed, "kept")[0]["reason"] == (
        card_cleanup.KEEP_ARCHIVE_HASH_FAILED)
    status = db.conn.execute(
        "SELECT hash_status FROM photos WHERE id = ?", (pid,)
    ).fetchone()["hash_status"]
    assert status == "corrupt"


def test_scoped_verify_unreachable_archive_keeps_row_unchecked(
        db, tmp_path, monkeypatch):
    # Codex P2: a transiently unreachable archive (NAS mount down) must
    # not permanently downgrade the row from unchecked → unreadable, or
    # a later re-scan would classify it as a prior integrity failure
    # (KEEP_ARCHIVE_HASH_FAILED) and route the user to the workspace-wide
    # Audit page instead of retrying targeted verification.
    archive_file, pid = _archive_photo(db, tmp_path, hash_status=None)
    _card_file(tmp_path)
    _scan(db, tmp_path)

    # verify_manifest_archives opens the archive fd first (CodeRabbit's
    # descriptor-pinning fix), so a downed-mount symptom surfaces as an
    # os.open failure on the archive path.
    real_open = os.open
    archive_str = str(archive_file)

    def failing_open(path, *args, **kwargs):
        if str(path) == archive_str:
            raise OSError(2, "mount is down")
        return real_open(path, *args, **kwargs)

    monkeypatch.setattr(card_cleanup.os, "open", failing_open)

    manifest_dir = str(tmp_path / "manifests")
    result = card_cleanup.verify_manifest_archives(
        db, load_manifest(manifest_dir, "scan-1"), manifest_dir)

    assert result["unreadable"] == 1
    assert result["verified"] == 0
    assert result["unblocked_files"] == 0

    # DB row must remain NULL so a later scan-after-remount re-issues
    # KEEP_NOT_VERIFIED, not KEEP_ARCHIVE_HASH_FAILED.
    status = db.conn.execute(
        "SELECT hash_status FROM photos WHERE id = ?", (pid,)
    ).fetchone()["hash_status"]
    assert status is None

    # Codex P2: the manifest reason must ALSO stay in the
    # KEEP_NOT_VERIFIED bucket the verify endpoint/UI's
    # pending-reason filter keys off. Overwriting with this run's
    # transient KEEP_ARCHIVE_UNREACHABLE would let a later verify report
    # "nothing needs checking" once the mount returns, forcing the user
    # to re-scan the card even though the DB row is still eligible.
    refreshed = load_manifest(manifest_dir, "scan-1")
    kept = _entries(refreshed, "kept")
    assert len(kept) == 1
    assert kept[0]["reason"] == card_cleanup.KEEP_NOT_VERIFIED


def test_scoped_verify_retry_after_transient_failure_unblocks(
        db, tmp_path, monkeypatch):
    # Codex P2 end-to-end: the first verify run hits a transient
    # os.open failure (mount down); the row must remain retry-eligible
    # so that a second verify run, once the mount is back, actually
    # promotes the entry to deletable without a fresh card scan.
    archive_file, pid = _archive_photo(db, tmp_path, hash_status=None)
    _card_file(tmp_path)
    _scan(db, tmp_path)
    manifest_dir = str(tmp_path / "manifests")

    real_open = os.open
    archive_str = str(archive_file)
    down = {"active": True}

    def flaky_open(path, *args, **kwargs):
        if down["active"] and str(path) == archive_str:
            raise OSError(2, "mount is down")
        return real_open(path, *args, **kwargs)

    monkeypatch.setattr(card_cleanup.os, "open", flaky_open)
    card_cleanup.verify_manifest_archives(
        db, load_manifest(manifest_dir, "scan-1"), manifest_dir)
    kept_first = _entries(load_manifest(manifest_dir, "scan-1"), "kept")
    assert len(kept_first) == 1
    assert kept_first[0]["reason"] == card_cleanup.KEEP_NOT_VERIFIED

    down["active"] = False
    retry = card_cleanup.verify_manifest_archives(
        db, load_manifest(manifest_dir, "scan-1"), manifest_dir)

    assert retry["verified"] == 1
    assert retry["unblocked_files"] == 1
    refreshed = load_manifest(manifest_dir, "scan-1")
    assert refreshed["totals"]["deletable"]["count"] == 1


def test_scoped_verify_preserves_persisted_unreadable_reason(
        db, tmp_path, monkeypatch):
    archive_file, pid = _archive_photo(db, tmp_path, hash_status=None)
    _card_file(tmp_path)
    _scan(db, tmp_path)
    real_hash = card_cleanup.compute_fd_hash

    def fail_archive_read(fd, *args, **kwargs):
        if os.fstat(fd).st_ino == os.stat(archive_file).st_ino:
            raise OSError("archive read failed")
        return real_hash(fd, *args, **kwargs)

    monkeypatch.setattr(card_cleanup, "compute_fd_hash", fail_archive_read)
    manifest_dir = str(tmp_path / "manifests")
    result = card_cleanup.verify_manifest_archives(
        db, load_manifest(manifest_dir, "scan-1"), manifest_dir)

    assert result["unreadable"] == 1
    status = db.conn.execute(
        "SELECT hash_status FROM photos WHERE id = ?", (pid,)
    ).fetchone()["hash_status"]
    assert status == "unreadable"
    kept = _entries(load_manifest(manifest_dir, "scan-1"), "kept")
    assert kept[0]["reason"] == card_cleanup.KEEP_ARCHIVE_HASH_FAILED


def test_scoped_verify_nonblocking_open_rejects_fifo_swap(
        db, tmp_path, monkeypatch):
    if not hasattr(os, "mkfifo") or not card_cleanup.O_NONBLOCK:
        pytest.skip("non-blocking FIFOs unsupported on this platform")
    archive_file, _ = _archive_photo(db, tmp_path, hash_status=None)
    _card_file(tmp_path)
    _scan(db, tmp_path)
    real_open = os.open
    opened_flags = []

    def swap_then_open(path, flags, *args, **kwargs):
        if str(path) == str(archive_file) and not opened_flags:
            opened_flags.append(flags)
            os.unlink(archive_file)
            os.mkfifo(archive_file)
        return real_open(path, flags, *args, **kwargs)

    monkeypatch.setattr(card_cleanup.os, "open", swap_then_open)
    manifest_dir = str(tmp_path / "manifests")
    result = card_cleanup.verify_manifest_archives(
        db, load_manifest(manifest_dir, "scan-1"), manifest_dir)

    assert opened_flags[0] & card_cleanup.O_NONBLOCK
    assert result["archive_files_read"] == 0
    assert result["unreadable"] == 1
    assert stat.S_ISFIFO(os.lstat(archive_file).st_mode)


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="POSIX-only race: Windows locks files opened via os.open, so "
    "os.replace() against the archive path fails inside the wrapper "
    "before the swap can complete. The race this test simulates cannot "
    "occur on Windows in the first place.",
)
def test_scoped_verify_rejects_atomic_replace_during_hash(
        db, tmp_path, monkeypatch):
    # Codex P1: while compute_fd_hash reads the pinned archive fd, a
    # sync/backup tool can atomically replace archive_path with a
    # different file under the same name. The fd's fstat still describes
    # the old (now unlinked) inode, so before/after fstat match — but the
    # bytes we then trust as authoritative for the archive no longer live
    # at that name. Certifying the row would let qualify_rows accept the
    # replacement later (same size+mtime is enough) and delete the card
    # copy against an archive we never actually saw. Row must stay
    # unchecked so a targeted re-verify can settle it once the file is
    # stable.
    archive_file, pid = _archive_photo(db, tmp_path, hash_status=None)
    _card_file(tmp_path)
    _scan(db, tmp_path)

    archive_str = str(archive_file)
    real_hash = card_cleanup.compute_fd_hash
    swapped = {"done": False}

    def swap_then_hash(fd, *a, **kw):
        result = real_hash(fd, *a, **kw)
        if not swapped["done"] and os.fstat(fd).st_ino == os.stat(
                archive_str).st_ino:
            replacement = tmp_path / "replacement.NEF"
            # Same size + mtime as the original — a naïve later
            # qualify_rows stat would otherwise accept it.
            replacement.write_bytes(b"raw-one")
            original_st = os.stat(archive_str)
            os.utime(
                replacement,
                ns=(original_st.st_atime_ns, original_st.st_mtime_ns),
            )
            os.replace(str(replacement), archive_str)
            swapped["done"] = True
        return result

    monkeypatch.setattr(card_cleanup, "compute_fd_hash", swap_then_hash)

    manifest_dir = str(tmp_path / "manifests")
    result = card_cleanup.verify_manifest_archives(
        db, load_manifest(manifest_dir, "scan-1"), manifest_dir)

    assert swapped["done"]
    assert result["verified"] == 0
    assert result["unblocked_files"] == 0
    status = db.conn.execute(
        "SELECT hash_status FROM photos WHERE id = ?", (pid,)
    ).fetchone()["hash_status"]
    assert status is None
    refreshed = load_manifest(manifest_dir, "scan-1")
    assert refreshed["totals"]["deletable"]["count"] == 0
    # Codex P2: the DB row is still unchecked (verify did not persist a
    # verdict), so the manifest must stay in the KEEP_NOT_VERIFIED
    # bucket — a targeted retry after the file settles is the correct
    # next lever, not "archive changed" which would look terminal.
    assert (
        _entries(refreshed, "kept")[0]["reason"]
        == card_cleanup.KEEP_NOT_VERIFIED
    )


def test_scoped_verify_drops_deletable_entries_whose_card_files_are_gone(
        db, tmp_path):
    # Codex P2: delete_verified unlinks card files but never rewrites the
    # manifest, so a subsequent scoped-verify run sees stale deletable
    # entries whose card paths no longer exist. The recomputed totals
    # must not credit those bytes to the deletable bucket — otherwise
    # the refreshed UI re-enables Delete asking the user to confirm
    # counts that include files already gone from the card, and the
    # revision bump would let a delete request sweep past the freshness
    # guard.
    already_deleted, _ = _archive_photo(
        db, tmp_path, name="IMG_0001.NEF", content=b"raw-one",
        hash_status="ok")
    _archive_photo(
        db, tmp_path, name="IMG_0002.NEF", content=b"raw-two",
        hash_status=None, folder="archive/2026/2026-08-02")
    gone_card = _card_file(tmp_path, name="IMG_0001.NEF", content=b"raw-one")
    _card_file(tmp_path, name="IMG_0002.NEF", content=b"raw-two")
    scan = _scan(db, tmp_path)
    assert scan["totals"]["deletable"]["count"] == 1
    assert scan["totals"]["kept"]["count"] == 1

    # Simulate a prior delete_verified run: the card file was unlinked
    # but the manifest still lists it as deletable.
    os.unlink(gone_card)

    manifest_dir = str(tmp_path / "manifests")
    manifest_before = load_manifest(manifest_dir, "scan-1")
    revision_before = int(manifest_before.get(
        "revision", card_cleanup.INITIAL_MANIFEST_REVISION))

    card_cleanup.verify_manifest_archives(
        db, manifest_before, manifest_dir)

    refreshed = load_manifest(manifest_dir, "scan-1")
    deletable = _entries(refreshed, "deletable")
    # Only the newly-verified file is deletable; the already-gone one
    # was dropped rather than counted.
    assert [e["path"] for e in deletable] == [
        str(tmp_path / "card" / "DCIM" / "IMG_0002.NEF")]
    assert refreshed["totals"]["deletable"]["count"] == 1
    assert refreshed["totals"]["deletable"]["bytes"] == len(b"raw-two")
    # The revision still bumps so a stale confirmation for the pre-verify
    # manifest is rejected by the delete endpoint's freshness gate.
    assert int(refreshed["revision"]) == revision_before + 1


def test_scoped_verify_keeps_deletable_entry_when_lstat_error_is_transient(
        db, tmp_path, monkeypatch):
    # Companion to the drop test: a transient lstat error (mount blip,
    # EACCES) is NOT proof the card file is gone. Dropping the entry on
    # any OSError would shrink the preview during an ordinary hiccup.
    # Preserve the entry so the next scoped verify — or the next delete
    # gate — gets the real answer.
    archive_file, _ = _archive_photo(
        db, tmp_path, name="IMG_0001.NEF", content=b"raw-one",
        hash_status="ok")
    card_file = _card_file(tmp_path, name="IMG_0001.NEF", content=b"raw-one")
    scan = _scan(db, tmp_path)
    assert scan["totals"]["deletable"]["count"] == 1

    real_lstat = os.lstat
    card_str = str(card_file)

    def flaky_lstat(path, *args, **kwargs):
        if str(path) == card_str:
            raise OSError(13, "permission denied")
        return real_lstat(path, *args, **kwargs)

    monkeypatch.setattr(card_cleanup.os, "lstat", flaky_lstat)

    manifest_dir = str(tmp_path / "manifests")
    card_cleanup.verify_manifest_archives(
        db, load_manifest(manifest_dir, "scan-1"), manifest_dir)

    refreshed = load_manifest(manifest_dir, "scan-1")
    assert refreshed["totals"]["deletable"]["count"] == 1
    assert [e["path"] for e in _entries(refreshed, "deletable")] == [card_str]


def test_scoped_verify_reclassifies_changed_deletable_entry_as_kept(
        db, tmp_path):
    # Codex P2: a prior delete_verified run skipped the file because its
    # type (or size, or mtime) no longer matched the scanned baseline —
    # SKIP_NOT_REGULAR / SKIP_SYMLINK / SKIP_CHANGED. lstat still
    # succeeds, so the FileNotFoundError-only gone-file check retained
    # the entry, inflating the refreshed deletable totals with bytes the
    # delete gates will refuse. Mirror the delete-time gates here so
    # scoped verify's revised preview matches what delete would accept.
    #
    # Codex P2 (follow-up 2): the file is still on the card, so dropping
    # the entry entirely undercounted the kept list and the totals hid a
    # file that will remain after deletion. Reclassify into the kept
    # bucket with SKIP_CHANGED instead; only confirmed-absent paths
    # should leave the manifest.
    archive_file, _ = _archive_photo(
        db, tmp_path, name="IMG_0001.NEF", content=b"raw-one",
        hash_status="ok")
    _archive_photo(
        db, tmp_path, name="IMG_0002.NEF", content=b"raw-two",
        hash_status=None, folder="archive/2026/2026-08-02")
    swapped_card = _card_file(
        tmp_path, name="IMG_0001.NEF", content=b"raw-one")
    _card_file(tmp_path, name="IMG_0002.NEF", content=b"raw-two")
    scan = _scan(db, tmp_path)
    assert scan["totals"]["deletable"]["count"] == 1
    assert scan["totals"]["kept"]["count"] == 1

    # Simulate the SKIP_CHANGED delete-time skip: same size, different
    # mtime_ns. delete_verified would refuse to unlink; scoped verify
    # must not re-enable Delete for these bytes.
    os.utime(swapped_card, ns=(0, 0))

    manifest_dir = str(tmp_path / "manifests")
    card_cleanup.verify_manifest_archives(
        db, load_manifest(manifest_dir, "scan-1"), manifest_dir)

    refreshed = load_manifest(manifest_dir, "scan-1")
    deletable = _entries(refreshed, "deletable")
    assert [e["path"] for e in deletable] == [
        str(tmp_path / "card" / "DCIM" / "IMG_0002.NEF")]
    assert refreshed["totals"]["deletable"]["count"] == 1
    assert refreshed["totals"]["deletable"]["bytes"] == len(b"raw-two")
    # Changed card file is now kept (reclassified, not dropped).
    # IMG_0002.NEF, originally kept with KEEP_NOT_VERIFIED, is
    # promoted to deletable by the verify pass, so IMG_0001.NEF is the
    # sole kept entry after verify.
    kept = _entries(refreshed, "kept")
    assert [e["path"] for e in kept] == [str(swapped_card)]
    assert kept[0]["reason"] == card_cleanup.SKIP_CHANGED
    assert "archive_path" not in kept[0]
    assert refreshed["totals"]["kept"]["count"] == 1
    assert refreshed["totals"]["kept"]["bytes"] == len(b"raw-one")


def test_scoped_verify_gates_pending_entry_before_promotion(
        db, tmp_path):
    # Codex P2 (follow-up 3): the deletable-entry pre-pass revalidates
    # the card file against the manifest baseline before the promotion
    # loop, but pending kept entries (KEEP_NOT_VERIFIED) went straight
    # into qualify_rows — which only inspects catalog rows and cannot
    # see that the card file has been replaced/resized/turned into a
    # symlink since the scan. delete_verified would reject the changed
    # file (SKIP_CHANGED / SKIP_SYMLINK / SKIP_NOT_REGULAR), so the
    # refreshed preview must not credit its bytes to the deletable
    # totals or re-enable Delete for it. Applying the same gates to
    # KEEP_NOT_VERIFIED entries reclassifies the changed file as kept
    # with the appropriate SKIP_* reason before qualify_rows sees it.
    archive_file, _ = _archive_photo(
        db, tmp_path, name="IMG_0001.NEF", content=b"raw-one",
        hash_status=None)
    card = _card_file(tmp_path, name="IMG_0001.NEF", content=b"raw-one")

    scan = _scan(db, tmp_path)
    # The unchecked archive row keeps the card entry pending.
    assert scan["totals"]["deletable"]["count"] == 0
    kept = _entries(scan, "kept")
    assert len(kept) == 1
    assert kept[0]["reason"] == card_cleanup.KEEP_NOT_VERIFIED
    assert kept[0]["path"] == str(card)

    # Simulate the SKIP_CHANGED delete-time skip: same size, different
    # mtime_ns. Archive hash still verifies fine, so qualify_rows would
    # happily promote — but delete_verified would refuse to unlink.
    os.utime(card, ns=(0, 0))

    manifest_dir = str(tmp_path / "manifests")
    result = card_cleanup.verify_manifest_archives(
        db, load_manifest(manifest_dir, "scan-1"), manifest_dir)
    # The archive still verified successfully; the pending entry just
    # did not get promoted.
    assert result["verified"] == 1
    assert result["unblocked_files"] == 0
    assert result["unblocked_bytes"] == 0

    refreshed = load_manifest(manifest_dir, "scan-1")
    assert refreshed["totals"]["deletable"]["count"] == 0
    assert refreshed["totals"]["deletable"]["bytes"] == 0
    kept = _entries(refreshed, "kept")
    assert [e["path"] for e in kept] == [str(card)]
    assert kept[0]["reason"] == card_cleanup.SKIP_CHANGED
    assert "archive_path" not in kept[0]
    assert refreshed["totals"]["kept"]["count"] == 1
    assert refreshed["totals"]["kept"]["bytes"] == len(b"raw-one")


def test_scoped_verify_drops_missing_pending_entry(db, tmp_path):
    _archive_photo(db, tmp_path, hash_status=None)
    card = _card_file(tmp_path)
    scan = _scan(db, tmp_path)
    assert _entries(scan, "kept")[0]["reason"] == (
        card_cleanup.KEEP_NOT_VERIFIED)
    os.unlink(card)

    manifest_dir = str(tmp_path / "manifests")
    result = card_cleanup.verify_manifest_archives(
        db, load_manifest(manifest_dir, "scan-1"), manifest_dir)

    assert result["verified"] == 1
    refreshed = load_manifest(manifest_dir, "scan-1")
    assert refreshed["entries"] == []
    assert refreshed["totals"]["deletable"] == {"count": 0, "bytes": 0}
    assert refreshed["totals"]["kept"] == {"count": 0, "bytes": 0}


def test_scoped_verify_terminal_alias_promotes_to_inside_source(
        db, tmp_path):
    # Codex P2: the only unchecked catalog row for a card file is a
    # hardlink (or alt-mount alias) of the card file itself. The initial
    # scan classifies the entry as KEEP_NOT_VERIFIED because
    # qualify_rows skips NULL-status rows before the same-file check.
    # Scoped verify then detects the alias via the fd's dev+inode and
    # sets its outcome_reason to KEEP_INSIDE_SOURCE, but does not
    # persist a verdict on the DB row — the row stays NULL. Without a
    # terminal override, qualify_rows would return KEEP_NOT_VERIFIED
    # again on the next click, trapping the entry in an infinite retry
    # loop with the "audit hasn't run" callout. The refreshed manifest
    # must adopt KEEP_INSIDE_SOURCE so the user sees the real story:
    # no independent archive copy exists.
    card = _card_file(tmp_path, name="IMG_0001.NEF", content=b"raw-one")
    folder_path = tmp_path / "archive" / "2026"
    folder_path.mkdir(parents=True)
    alias = folder_path / "IMG_0001.NEF"
    try:
        os.link(card, alias)
    except (OSError, NotImplementedError):
        pytest.skip("hardlinks unsupported on this filesystem")
    st = os.stat(alias)
    fid = db.add_folder(str(folder_path))
    db.add_photo(
        folder_id=fid, filename="IMG_0001.NEF", extension=".NEF",
        file_size=st.st_size, file_mtime=st.st_mtime,
        file_hash=_sha(str(alias)),
    )
    # NULL hash_status: the alias row was cataloged but never audited.

    scan = _scan(db, tmp_path)
    kept = _entries(scan, "kept")
    assert len(kept) == 1
    assert kept[0]["reason"] == card_cleanup.KEEP_NOT_VERIFIED

    manifest_dir = str(tmp_path / "manifests")
    result = card_cleanup.verify_manifest_archives(
        db, load_manifest(manifest_dir, "scan-1"), manifest_dir)
    assert result["verified"] == 0
    assert result["unblocked_files"] == 0

    refreshed = load_manifest(manifest_dir, "scan-1")
    kept = _entries(refreshed, "kept")
    assert [e["path"] for e in kept] == [str(card)]
    assert kept[0]["reason"] == card_cleanup.KEEP_INSIDE_SOURCE
    # And the retry-eligible bucket is empty — the verify endpoint
    # would tell the user "nothing needs checking" without the loop.
    pending = [e for e in refreshed["entries"]
               if e.get("reason") == card_cleanup.KEEP_NOT_VERIFIED]
    assert pending == []


def test_scoped_verify_keeps_pending_when_alias_and_transient_row_coexist(
        db, tmp_path):
    # Guard the terminal-alias override against overreach: when a hash
    # has one unchecked alias row AND one unchecked transient-failure
    # row (e.g. the real archive's mount is momentarily down), the
    # entry must stay retry-eligible. A future click after the mount
    # returns has a real lever — the transient row can still succeed.
    card = _card_file(tmp_path, name="IMG_0001.NEF", content=b"raw-one")
    # Alias row (hardlink into the card, NULL hash_status).
    alias_folder = tmp_path / "archive" / "alias"
    alias_folder.mkdir(parents=True)
    alias = alias_folder / "IMG_0001.NEF"
    try:
        os.link(card, alias)
    except (OSError, NotImplementedError):
        pytest.skip("hardlinks unsupported on this filesystem")
    alias_st = os.stat(alias)
    alias_fid = db.add_folder(str(alias_folder))
    db.add_photo(
        folder_id=alias_fid, filename="IMG_0001.NEF", extension=".NEF",
        file_size=alias_st.st_size, file_mtime=alias_st.st_mtime,
        file_hash=_sha(str(alias)),
    )
    # Real independent archive row (NULL hash_status), but the file is
    # deleted so scoped verify sees KEEP_ARCHIVE_UNREACHABLE — the same
    # shape as a mount that comes back later.
    real_folder = tmp_path / "archive" / "2026"
    real_folder.mkdir(parents=True)
    real_archive = real_folder / "IMG_0001.NEF"
    real_archive.write_bytes(b"raw-one")
    real_st = os.stat(real_archive)
    real_fid = db.add_folder(str(real_folder))
    db.add_photo(
        folder_id=real_fid, filename="IMG_0001.NEF", extension=".NEF",
        file_size=real_st.st_size, file_mtime=real_st.st_mtime,
        file_hash=_sha(str(real_archive)),
    )
    os.unlink(real_archive)

    scan = _scan(db, tmp_path)
    assert _entries(scan, "kept")[0]["reason"] == \
        card_cleanup.KEEP_NOT_VERIFIED

    manifest_dir = str(tmp_path / "manifests")
    card_cleanup.verify_manifest_archives(
        db, load_manifest(manifest_dir, "scan-1"), manifest_dir)

    refreshed = load_manifest(manifest_dir, "scan-1")
    kept = _entries(refreshed, "kept")
    # Retry-eligible: KEEP_NOT_VERIFIED wins because the real row still
    # gives verify a lever once its mount returns.
    assert kept[0]["reason"] == card_cleanup.KEEP_NOT_VERIFIED


def test_scoped_verify_terminal_alias_does_not_mask_persisted_external_failure(
        db, tmp_path):
    # Codex P2: a hash with one same-source alias row (still NULL) and
    # one external row already persisted as corrupt (from an earlier
    # verify pass) hits an edge case in the terminal-alias override.
    # This run only processes the alias — the external row is no longer
    # unchecked — so scoped verify records the alias as terminally
    # inside-source and adds the hash to terminal_inside_source_hashes.
    # qualify_rows then still returns KEEP_NOT_VERIFIED (the alias's
    # NULL row wins priority over the corrupt row's KEEP_ARCHIVE_HASH_
    # FAILED). Without this fix, the reclassification pass would
    # overwrite that to KEEP_INSIDE_SOURCE and tell the user "no
    # independent archive copy exists", hiding the corrupt external
    # archive and the Audit-page remedy it needs. The refreshed
    # manifest must surface KEEP_ARCHIVE_HASH_FAILED instead: the
    # external failure was cataloged, and the Audit page — not
    # another verify click — is the remedy.
    card = _card_file(tmp_path, name="IMG_0001.NEF", content=b"raw-one")

    # External archive row that was hashed as corrupt in an earlier
    # pass. Store the row without a real file on disk so this run's
    # verify loop cannot revisit it; hash_status = "corrupt" makes it
    # non-unchecked so it never enters the scoped verify's unchecked
    # list. The row remains visible to qualify_rows and to the
    # reclassification's row scan.
    external_folder = tmp_path / "archive" / "2026"
    external_folder.mkdir(parents=True)
    external_fid = db.add_folder(str(external_folder))
    external_pid = db.add_photo(
        folder_id=external_fid, filename="IMG_0001.NEF", extension=".NEF",
        file_size=len(b"raw-one"), file_mtime=1000.0,
        file_hash=_sha(str(card)),
    )
    db.audit.update_photo_hash_check(external_pid, "corrupt")

    # Alias row: a hardlink into the card file itself. Still NULL —
    # never audited. Scoped verify will detect it as same-as-card via
    # the fd's dev/inode and leave it NULL.
    alias_folder = tmp_path / "archive" / "alias"
    alias_folder.mkdir(parents=True)
    alias = alias_folder / "IMG_0001.NEF"
    try:
        os.link(card, alias)
    except (OSError, NotImplementedError):
        pytest.skip("hardlinks unsupported on this filesystem")
    alias_st = os.stat(alias)
    alias_fid = db.add_folder(str(alias_folder))
    db.add_photo(
        folder_id=alias_fid, filename="IMG_0001.NEF", extension=".NEF",
        file_size=alias_st.st_size, file_mtime=alias_st.st_mtime,
        file_hash=_sha(str(alias)),
    )

    scan = _scan(db, tmp_path)
    kept = _entries(scan, "kept")
    assert len(kept) == 1
    # Initial scan: the alias's NULL status makes KEEP_NOT_VERIFIED the
    # winning reason via qualify_rows' priority order.
    assert kept[0]["reason"] == card_cleanup.KEEP_NOT_VERIFIED

    manifest_dir = str(tmp_path / "manifests")
    card_cleanup.verify_manifest_archives(
        db, load_manifest(manifest_dir, "scan-1"), manifest_dir)

    refreshed = load_manifest(manifest_dir, "scan-1")
    kept = _entries(refreshed, "kept")
    assert len(kept) == 1
    # Not KEEP_INSIDE_SOURCE (which would tell the user no independent
    # archive exists — false; one is cataloged) and not
    # KEEP_NOT_VERIFIED (another verify click would find nothing new to
    # do since the only remaining NULL row is a same-source alias).
    # The correct surface is the terminal external verdict.
    assert kept[0]["reason"] == card_cleanup.KEEP_ARCHIVE_HASH_FAILED


def test_scoped_verify_surfaces_new_failed_archive_beside_unchecked_alias(
        db, tmp_path):
    """A failure persisted in this run also outranks a NULL alias."""
    card = _card_file(tmp_path, name="IMG_0001.NEF", content=b"raw-one")
    expected_hash = _sha(str(card))

    alias_folder = tmp_path / "archive" / "alias-current"
    alias_folder.mkdir(parents=True)
    alias = alias_folder / "IMG_0001.NEF"
    try:
        os.link(card, alias)
    except (OSError, NotImplementedError):
        pytest.skip("hardlinks unsupported on this filesystem")
    alias_st = os.stat(alias)
    alias_fid = db.add_folder(str(alias_folder))
    alias_pid = db.add_photo(
        folder_id=alias_fid, filename=alias.name, extension=".NEF",
        file_size=alias_st.st_size, file_mtime=alias_st.st_mtime,
        file_hash=expected_hash,
    )

    failed_folder = tmp_path / "archive" / "independent"
    failed_folder.mkdir(parents=True)
    failed_archive = failed_folder / "IMG_0001.NEF"
    failed_archive.write_bytes(b"bad-copy")
    failed_st = os.stat(failed_archive)
    failed_fid = db.add_folder(str(failed_folder))
    failed_pid = db.add_photo(
        folder_id=failed_fid, filename=failed_archive.name, extension=".NEF",
        file_size=failed_st.st_size, file_mtime=failed_st.st_mtime,
        file_hash=expected_hash,
    )

    _scan(db, tmp_path)
    manifest_dir = str(tmp_path / "manifests")
    result = card_cleanup.verify_manifest_archives(
        db, load_manifest(manifest_dir, "scan-1"), manifest_dir)

    assert result["corrupt"] == 1
    statuses = {
        row["id"]: row["hash_status"]
        for row in db.conn.execute(
            "SELECT id, hash_status FROM photos WHERE id IN (?, ?)",
            (alias_pid, failed_pid),
        )
    }
    assert statuses == {alias_pid: None, failed_pid: "corrupt"}
    kept = _entries(load_manifest(manifest_dir, "scan-1"), "kept")
    assert kept[0]["reason"] == card_cleanup.KEEP_ARCHIVE_HASH_FAILED


def test_scoped_verify_does_not_certify_repurposed_catalog_row(
        db, tmp_path, monkeypatch):
    archive_file, pid = _archive_photo(db, tmp_path, hash_status=None)
    _card_file(tmp_path)
    _scan(db, tmp_path)
    real_hash = card_cleanup.compute_fd_hash

    def repurpose_row_while_hashing(fd, *args, **kwargs):
        result = real_hash(fd, *args, **kwargs)
        if os.fstat(fd).st_ino == os.stat(archive_file).st_ino:
            db.conn.execute(
                "UPDATE photos SET filename = ? WHERE id = ?",
                ("REUSED.NEF", pid),
            )
        return result

    monkeypatch.setattr(
        card_cleanup, "compute_fd_hash", repurpose_row_while_hashing)
    manifest_dir = str(tmp_path / "manifests")
    result = card_cleanup.verify_manifest_archives(
        db, load_manifest(manifest_dir, "scan-1"), manifest_dir)

    row = db.conn.execute(
        "SELECT filename, hash_status FROM photos WHERE id = ?", (pid,)
    ).fetchone()
    assert row["filename"] == "REUSED.NEF"
    assert row["hash_status"] is None
    assert result["verified"] == 0
    assert result["unblocked_files"] == 0
    refreshed = load_manifest(manifest_dir, "scan-1")
    assert refreshed["totals"]["deletable"]["count"] == 0


def test_scoped_verify_drops_pending_kept_entries_whose_card_files_are_gone(
        db, tmp_path):
    # Codex P2 (follow-up): the deletable pre-pass already drops
    # confirmed-absent card files, but pending KEEP_NOT_VERIFIED
    # entries were never revalidated the same way. If a pending card
    # file is removed after the scan, the previous code kept the
    # entry with its original byte count credited to the "kept"
    # bucket; the refreshed preview then claimed a nonexistent file
    # would remain on the card. The absent entry must be dropped so
    # the totals match what is actually on disk.
    #
    # Pair with a second pending file that has a viable archive so
    # verify actually does something and the missing entry is the
    # only kept survivor difference we're measuring.
    archive_gone, _ = _archive_photo(
        db, tmp_path, name="IMG_0001.NEF", content=b"raw-one",
        hash_status=None)
    archive_kept, _ = _archive_photo(
        db, tmp_path, name="IMG_0002.NEF", content=b"raw-two",
        hash_status=None, folder="archive/2026/2026-08-02")
    gone_card = _card_file(
        tmp_path, name="IMG_0001.NEF", content=b"raw-one")
    _card_file(tmp_path, name="IMG_0002.NEF", content=b"raw-two")

    scan = _scan(db, tmp_path)
    kept = _entries(scan, "kept")
    assert sorted(e["path"] for e in kept) == sorted([
        str(gone_card),
        str(tmp_path / "card" / "DCIM" / "IMG_0002.NEF"),
    ])
    assert all(e["reason"] == card_cleanup.KEEP_NOT_VERIFIED for e in kept)

    # Simulate the card file getting removed after the scan (for
    # example, ejected and re-inserted, or the user deleted it
    # directly from the card in another tool).
    os.unlink(gone_card)

    manifest_dir = str(tmp_path / "manifests")
    manifest_before = load_manifest(manifest_dir, "scan-1")
    revision_before = int(manifest_before.get(
        "revision", card_cleanup.INITIAL_MANIFEST_REVISION))

    card_cleanup.verify_manifest_archives(
        db, manifest_before, manifest_dir)

    refreshed = load_manifest(manifest_dir, "scan-1")
    # The IMG_0002 pending entry promoted to deletable. The gone-card
    # entry did NOT survive as a kept-with-original-bytes row.
    kept = _entries(refreshed, "kept")
    assert kept == [], (
        "Confirmed-absent pending entries must leave the manifest so "
        "the refreshed totals do not claim a nonexistent file will "
        "remain on the card."
    )
    deletable = _entries(refreshed, "deletable")
    assert [e["path"] for e in deletable] == [
        str(tmp_path / "card" / "DCIM" / "IMG_0002.NEF")]
    assert refreshed["totals"]["kept"]["count"] == 0
    assert refreshed["totals"]["kept"]["bytes"] == 0
    assert refreshed["totals"]["deletable"]["count"] == 1
    assert refreshed["totals"]["deletable"]["bytes"] == len(b"raw-two")
    # Revision still bumps so any confirmation against the pre-verify
    # manifest is rejected by the delete endpoint's freshness gate.
    assert int(refreshed["revision"]) == revision_before + 1


def test_scoped_verify_keeps_pending_entry_when_lstat_error_is_transient(
        db, tmp_path, monkeypatch):
    # Companion to the drop test: a transient lstat error on a
    # pending card file (mount blip, EACCES) is NOT proof the file is
    # gone. Dropping the entry would shrink the preview during an
    # ordinary hiccup and — worse — make a still-eligible file
    # invisible until the next scan. Pair the transient lstat with a
    # transient archive-open failure so the archive-side verify keeps
    # the entry in the KEEP_NOT_VERIFIED bucket (rather than
    # promoting it via qualify_rows, which uses os.stat and is not
    # affected by an os.lstat monkeypatch); this isolates that the
    # pre-pass itself preserves the entry.
    archive_file, _ = _archive_photo(db, tmp_path, hash_status=None)
    card_file = _card_file(tmp_path)
    _scan(db, tmp_path)

    real_lstat = os.lstat
    real_open = os.open
    card_str = str(card_file)
    archive_str = str(archive_file)

    def flaky_lstat(path, *args, **kwargs):
        if str(path) == card_str:
            raise OSError(13, "permission denied")
        return real_lstat(path, *args, **kwargs)

    def flaky_open(path, *args, **kwargs):
        if str(path) == archive_str:
            raise OSError(2, "mount is down")
        return real_open(path, *args, **kwargs)

    monkeypatch.setattr(card_cleanup.os, "lstat", flaky_lstat)
    monkeypatch.setattr(card_cleanup.os, "open", flaky_open)

    manifest_dir = str(tmp_path / "manifests")
    card_cleanup.verify_manifest_archives(
        db, load_manifest(manifest_dir, "scan-1"), manifest_dir)

    refreshed = load_manifest(manifest_dir, "scan-1")
    kept = _entries(refreshed, "kept")
    # The transient lstat did NOT drop the pending entry from the
    # manifest — it survived the pre-pass, and with the archive
    # transiently unreachable it stays retry-eligible.
    assert [e["path"] for e in kept] == [card_str]
    assert kept[0]["reason"] == card_cleanup.KEEP_NOT_VERIFIED


def test_scoped_verify_records_hash_failed_when_open_fd_hash_fails(
        db, tmp_path, monkeypatch):
    # Codex P2: when os.open on the archive path succeeds but the
    # subsequent fd read (compute_fd_hash / fstat / realpath) fails,
    # the DB row is persisted as hash_status='unreadable' — a
    # permanent verdict. qualify_rows returns KEEP_ARCHIVE_HASH_FAILED
    # for that persisted row on the next pass. The scoped-verify
    # promotion-loop override used to replace that verdict with the
    # run's outcome_reason, still initialized to
    # KEEP_ARCHIVE_UNREACHABLE, hiding the failure from the refreshed
    # preview. The UI shows the retry affordance for
    # KEEP_ARCHIVE_UNREACHABLE and the Audit-page guidance only for
    # KEEP_ARCHIVE_HASH_FAILED, so the mismatched reason misroutes
    # the user. outcome_reason must match the persisted verdict.
    _, pid = _archive_photo(db, tmp_path, hash_status=None)
    _card_file(tmp_path)
    _scan(db, tmp_path)

    def failing_compute(fd, *args, **kwargs):
        # Simulate a mid-read failure on the pinned archive fd.
        raise OSError(5, "input/output error")

    monkeypatch.setattr(card_cleanup, "compute_fd_hash", failing_compute)

    manifest_dir = str(tmp_path / "manifests")
    result = card_cleanup.verify_manifest_archives(
        db, load_manifest(manifest_dir, "scan-1"), manifest_dir)

    # The row was reached (os.open succeeded), so this counts as
    # unreadable, not verified.
    assert result["unreadable"] == 1
    assert result["verified"] == 0
    assert result["unblocked_files"] == 0

    # The DB row must be persisted as 'unreadable' — permanent.
    status = db.conn.execute(
        "SELECT hash_status FROM photos WHERE id = ?", (pid,)
    ).fetchone()["hash_status"]
    assert status == "unreadable"

    # The manifest reason must match qualify_rows' terminal verdict
    # for a persisted-unreadable row: KEEP_ARCHIVE_HASH_FAILED, NOT
    # the run's initial KEEP_ARCHIVE_UNREACHABLE. This is the signal
    # the UI keys off to point users at the Audit page.
    refreshed = load_manifest(manifest_dir, "scan-1")
    kept = _entries(refreshed, "kept")
    assert len(kept) == 1
    assert kept[0]["reason"] == card_cleanup.KEEP_ARCHIVE_HASH_FAILED
    # And it must not have been overridden back to unreachable
    # (which would incorrectly offer a retry).
    assert kept[0]["reason"] != card_cleanup.KEEP_ARCHIVE_UNREACHABLE


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
    db.audit.update_photo_hash_check(pid, "ok")
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


def test_scan_unreadable_after_stat_credits_size_to_kept_bytes(
        db, tmp_path, monkeypatch):
    # Codex P2: os.stat succeeds but hashing fails (transient read error
    # or missing read permission). st.st_size is truthful, so it must be
    # included both in the entry and in totals.kept.bytes — a
    # wholesale-unreadable card would otherwise report gigabytes as
    # "0 bytes kept" while the preview shows those files under the kept
    # bucket.
    unreadable = _card_file(
        tmp_path, name="IMG_UNREADABLE.NEF",
        content=b"twelve-bytes")  # 12 bytes
    real_hash = card_cleanup.compute_file_hash

    def failing_hash(path, *a, **kw):
        if str(path) == str(unreadable):
            raise PermissionError(13, "denied", str(path))
        return real_hash(path, *a, **kw)

    monkeypatch.setattr(card_cleanup, "compute_file_hash", failing_hash)
    result = _scan(db, tmp_path)
    kept = _entries(result, "kept")
    assert len(kept) == 1
    assert kept[0]["path"] == str(unreadable)
    assert kept[0]["size"] == 12
    assert card_cleanup.KEEP_UNREADABLE in kept[0]["reason"]
    assert result["totals"]["kept"]["count"] == 1
    assert result["totals"]["kept"]["bytes"] == 12


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


def test_qualify_rows_prefers_verify_remedy_over_ok_row_specific_reason(
        db, tmp_path):
    """Codex P2: an ok archive row that fails a later gate (missing
    file, changed size/mtime, etc.) used to set the specific reason
    verbatim, which then hid rows that still had an unchecked sibling
    verify could try. Ensure the never-checked sibling wins so the
    card-cleanup callout offers to verify it."""
    archive_file, _ = _archive_photo(db, tmp_path)
    source_root_real = os.path.realpath(str(tmp_path / "card"))
    card = _card_file(tmp_path)
    # Row A is 'ok' but its archive file is missing on disk — the row
    # itself would raise KEEP_ARCHIVE_UNREACHABLE via the ok-path stat
    # failure. Row B is a never-checked sibling that verify could still
    # try; its presence must promote the reason to KEEP_NOT_VERIFIED.
    rows = [
        {"filename": "GONE.NEF",
         "file_size": 7, "file_mtime": 0.0,
         "hash_status": "ok",
         "folder_path": str(tmp_path / "archive_missing")},
        {"filename": os.path.basename(str(archive_file)),
         "file_size": None, "file_mtime": None,
         "hash_status": None,
         "folder_path": os.path.dirname(str(archive_file))},
    ]
    _, reason = card_cleanup.qualify_rows(
        rows, source_root_real, str(card))
    assert reason == card_cleanup.KEEP_NOT_VERIFIED


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


def test_delete_skips_inplace_rewrite_during_archive_gate(db, tmp_path):
    # The card hash runs after archive gate 1. If the file is rewritten in
    # place during that potentially slow gate — same inode, size, and mtime
    # — the single content read still catches it.
    _archive_photo(db, tmp_path)
    card = _card_file(tmp_path)  # content b"raw-one", 7 bytes
    scan = _scan(db, tmp_path)
    assert scan["totals"]["deletable"]["count"] == 1
    manifest = load_manifest(str(tmp_path / "manifests"), "scan-1")
    entry = next(e for e in manifest["entries"] if e["bucket"] == "deletable")
    real_fetch = card_cleanup.fetch_rows_by_hash
    original_ino = os.stat(card).st_ino

    def fetch_then_inplace_rewrite(db_arg, file_hash):
        rows = real_fetch(db_arg, file_hash)
        # Open the same inode and overwrite in place — do NOT unlink and
        # recreate. This is the scenario the metadata gates cannot see:
        # inode, size, and mtime all preserved.
        with open(card, "r+b") as f:
            f.seek(0)
            f.write(b"NEW-ONE")  # also 7 bytes
        os.utime(card, ns=(entry["mtime_ns"], entry["mtime_ns"]))
        return rows

    with unittest.mock.patch.object(
            card_cleanup, "fetch_rows_by_hash", fetch_then_inplace_rewrite):
        summary = card_cleanup.delete_verified(db, manifest)
    # Inode preserved: metadata gates would have passed. The content hash
    # catches the swap — assert both the outcome and the reason.
    assert os.stat(card).st_ino == original_ino
    assert summary["deleted"] == 0
    assert len(summary["skipped"]) == 1
    assert summary["skipped"][0]["reason"] == card_cleanup.SKIP_CONTENT_CHANGED
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


def test_delete_uses_manifest_source_root_not_re_resolved(db, tmp_path):
    # Codex P1 review of commit 2efff02f: if the scanned root itself is
    # renamed and replaced by a symlink pointing at a different tree,
    # re-resolving the manifest's already-canonical source_root at delete
    # time would re-anchor every containment check to the swap target.
    # A byte-identical file at the same relative path there would then
    # pass every gate and be unlinked outside the scanned card. The delete
    # must use the manifest's stored source_root verbatim.
    _archive_photo(db, tmp_path)
    card_file = _card_file(tmp_path)
    contents = card_file.read_bytes()
    scan = _scan(db, tmp_path)
    assert scan["totals"]["deletable"]["count"] == 1
    manifest = load_manifest(str(tmp_path / "manifests"), "scan-1")
    entry = next(e for e in manifest["entries"] if e["bucket"] == "deletable")

    swapped = tmp_path / "swapped_root"
    (swapped / "DCIM").mkdir(parents=True)
    twin = swapped / "DCIM" / card_file.name
    twin.write_bytes(contents)
    os.utime(twin, ns=(entry["mtime_ns"], entry["mtime_ns"]))

    original = tmp_path / "card"
    original.rename(tmp_path / "card_moved")
    try:
        os.symlink(str(swapped), str(original))
    except (OSError, NotImplementedError):
        pytest.skip("symlinks unsupported on this filesystem")

    summary = card_cleanup.delete_verified(db, manifest)

    assert summary["deleted"] == 0
    assert twin.exists() and twin.read_bytes() == contents
    assert len(summary["skipped"]) == 1
    assert summary["skipped"][0]["reason"] == card_cleanup.SKIP_OUTSIDE_SOURCE


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


def test_delete_skips_when_card_path_becomes_fifo_post_scan(
        db, tmp_path, monkeypatch):
    # Codex P2: rejecting only symlinks at the delete gate leaves other
    # non-regular replacements open. A scanned zero-byte photo swapped
    # for a FIFO whose mtime is forced back to the manifest value passes
    # the S_ISLNK reject and the size/mtime check; compute_file_hash
    # then blocks forever opening the pipe, and cancellation is checked
    # only at file boundaries, so the whole delete job hangs. Both lstat
    # gates must reject any non-regular file BEFORE the hash open.
    if not hasattr(os, "mkfifo"):
        pytest.skip("mkfifo unsupported on this platform")
    _archive_photo(db, tmp_path, content=b"")
    card = _card_file(tmp_path, content=b"")
    scan = _scan(db, tmp_path)
    assert scan["totals"]["deletable"]["count"] == 1
    manifest = load_manifest(str(tmp_path / "manifests"), "scan-1")
    entry = next(e for e in manifest["entries"] if e["bucket"] == "deletable")

    os.unlink(card)
    try:
        os.mkfifo(str(card))
    except OSError:
        pytest.skip("mkfifo unsupported on this filesystem")
    os.utime(card, ns=(entry["mtime_ns"], entry["mtime_ns"]))

    # compute_file_hash / compute_fd_hash on a FIFO would block on read
    # indefinitely. Prove the gate rejects the replacement BEFORE any
    # hash call happens. Both hash entry points are patched — the delete
    # path uses compute_fd_hash after the CodeRabbit TOCTOU fix, but
    # guarding both keeps future regressions from silently slipping the
    # bad object through.
    hash_calls = []
    real_hash = card_cleanup.compute_file_hash
    real_fd_hash = card_cleanup.compute_fd_hash

    def guarded_hash(*a, **kw):
        hash_calls.append(a)
        raise AssertionError(
            "hash function must not be called on a non-regular replacement")

    monkeypatch.setattr(card_cleanup, "compute_file_hash", guarded_hash)
    monkeypatch.setattr(card_cleanup, "compute_fd_hash", guarded_hash)
    try:
        summary = card_cleanup.delete_verified(db, manifest)
    finally:
        monkeypatch.setattr(card_cleanup, "compute_file_hash", real_hash)
        monkeypatch.setattr(card_cleanup, "compute_fd_hash", real_fd_hash)

    assert hash_calls == []
    assert summary["deleted"] == 0
    assert summary["deleted_bytes"] == 0
    assert len(summary["skipped"]) == 1
    assert summary["skipped"][0]["reason"] == card_cleanup.SKIP_NOT_REGULAR
    # The FIFO is left untouched — the tool refused to operate on it.
    assert stat.S_ISFIFO(os.lstat(card).st_mode)


def test_delete_nonblocking_open_rejects_fifo_swap_after_lstat(
        db, tmp_path, monkeypatch):
    if not hasattr(os, "mkfifo") or not card_cleanup.O_NONBLOCK:
        pytest.skip("non-blocking FIFOs unsupported on this platform")
    _archive_photo(db, tmp_path)
    card = _card_file(tmp_path)
    _scan(db, tmp_path)
    manifest = load_manifest(str(tmp_path / "manifests"), "scan-1")
    real_open = os.open
    opened_flags = []

    def swap_then_open(path, flags, *args, **kwargs):
        if str(path) == str(card) and not opened_flags:
            opened_flags.append(flags)
            os.unlink(card)
            os.mkfifo(card)
        return real_open(path, flags, *args, **kwargs)

    monkeypatch.setattr(card_cleanup.os, "open", swap_then_open)
    summary = card_cleanup.delete_verified(db, manifest)

    assert opened_flags[0] & card_cleanup.O_NONBLOCK
    assert summary["deleted"] == 0
    assert summary["skipped"] == [{
        "path": str(card), "reason": card_cleanup.SKIP_NOT_REGULAR,
    }]
    assert stat.S_ISFIFO(os.lstat(card).st_mode)


def test_delete_skips_when_archive_dies_during_card_hash(db, tmp_path):
    # Gate 1 authorizes, then the single card hash reads the
    # whole file (seconds on real photos). If the archive vanishes during
    # that hash, gate 1's authorization is stale. A second archive gate
    # immediately before os.remove must catch the death — otherwise the
    # card copy would be the ONLY remaining copy at unlink time.
    archive_file, _ = _archive_photo(db, tmp_path)
    card = _card_file(tmp_path)
    scan = _scan(db, tmp_path)
    assert scan["totals"]["deletable"]["count"] == 1
    manifest = load_manifest(str(tmp_path / "manifests"), "scan-1")
    # delete_verified reads the card via compute_fd_hash on a pinned
    # descriptor (CodeRabbit TOCTOU fix), so intercept that instead of
    # compute_file_hash. Kill the archive during the single card hash to
    # prove gate 2 catches the death before os.remove.
    real_hash = card_cleanup.compute_fd_hash
    hash_calls = []

    def hash_kill_archive(fd, *a, **kw):
        hash_calls.append(fd)
        result = real_hash(fd, *a, **kw)
        if len(hash_calls) == 1:
            os.unlink(archive_file)
        return result

    with unittest.mock.patch.object(
            card_cleanup, "compute_fd_hash", hash_kill_archive):
        summary = card_cleanup.delete_verified(db, manifest)
    assert summary["deleted"] == 0
    assert card.exists()
    assert len(summary["skipped"]) == 1
    assert "archive" in summary["skipped"][0]["reason"]


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="POSIX-only race: Windows cannot rmtree a directory that "
    "contains an open file, so the parent-directory swap inside the "
    "wrapper fails before the containment recheck runs. The race this "
    "test simulates cannot occur on Windows in the first place.",
)
def test_delete_skips_when_parent_redirected_during_card_hash(db, tmp_path):
    # Codex P1: after both archive gate 1 and gate 2 authorize, if the
    # parent directory is swapped for a symlink to a byte-identical
    # external copy during the final hash, every content gate still
    # passes — but os.remove would unlink the external file. The final
    # containment recheck must run AFTER the final hash, not before it.
    _archive_photo(db, tmp_path)
    card = _card_file(tmp_path)  # content b"raw-one", 7 bytes
    scan = _scan(db, tmp_path)
    assert scan["totals"]["deletable"]["count"] == 1
    manifest = load_manifest(str(tmp_path / "manifests"), "scan-1")
    entry = next(e for e in manifest["entries"] if e["bucket"] == "deletable")

    outside = tmp_path / "outside"
    outside.mkdir()
    decoy = outside / "IMG_0001.NEF"
    decoy.write_bytes(b"raw-one")
    os.utime(decoy, ns=(entry["mtime_ns"], entry["mtime_ns"]))

    # Same intent as above: delete_verified now reads the card via
    # compute_fd_hash (CodeRabbit TOCTOU fix), so hook that entry point
    # to run the parent-directory swap during the pinned-fd hash.
    real_hash = card_cleanup.compute_fd_hash
    hash_calls = []

    def hash_then_swap_parent(fd, *a, **kw):
        hash_calls.append(fd)
        result = real_hash(fd, *a, **kw)
        if len(hash_calls) == 1:
            # Swap the parent for a symlink after the single card read has
            # committed to bytes but before containment is re-verified.
            dcim = tmp_path / "card" / "DCIM"
            shutil.rmtree(dcim)
            try:
                os.symlink(str(outside), str(dcim))
            except (OSError, NotImplementedError):
                pytest.skip("symlinks unsupported on this filesystem")
        return result

    with unittest.mock.patch.object(
            card_cleanup, "compute_fd_hash", hash_then_swap_parent):
        summary = card_cleanup.delete_verified(db, manifest)
    assert summary["deleted"] == 0
    assert len(summary["skipped"]) == 1
    assert "no longer resolves inside" in summary["skipped"][0]["reason"]
    # The decoy — the only file behind the redirected path — must survive.
    assert decoy.exists() and decoy.read_bytes() == b"raw-one"


def test_delete_hashes_each_card_file_once(db, tmp_path, monkeypatch):
    _archive_photo(db, tmp_path)
    card = _card_file(tmp_path)
    card_inode = os.stat(card).st_ino
    _scan(db, tmp_path)
    manifest = load_manifest(str(tmp_path / "manifests"), "scan-1")
    # delete_verified hashes the card via compute_fd_hash on a pinned
    # descriptor (CodeRabbit TOCTOU fix). Correlate fds back to the card
    # by inode so this assertion still says "one read per card file".
    hashed_inodes = []
    real_hash = card_cleanup.compute_fd_hash

    def counting_hash(fd, *args, **kwargs):
        hashed_inodes.append(os.fstat(fd).st_ino)
        return real_hash(fd, *args, **kwargs)

    monkeypatch.setattr(card_cleanup, "compute_fd_hash", counting_hash)
    summary = card_cleanup.delete_verified(db, manifest)
    assert summary["deleted"] == 1
    assert hashed_inodes == [card_inode]


def test_qualify_binds_containment_to_statted_object(db, tmp_path, monkeypatch):
    # Codex P1: realpath() (containment) and os.stat() (metadata gate)
    # are separate path walks. Swap the archive parent for a symlink
    # BETWEEN them, pointing at an in-source decoy dir whose file has
    # the row's exact baseline and a different inode than the candidate:
    # without the post-stat re-resolution check, the row qualifies on an
    # "archive copy" that actually lives on the card.
    archive_file, _ = _archive_photo(db, tmp_path)
    card = _card_file(tmp_path)
    row = card_cleanup.fetch_rows_by_hash(db, _sha(str(card)))[0]

    decoy_dir = tmp_path / "card" / "DCIM2"
    decoy_dir.mkdir(parents=True)
    decoy = decoy_dir / row["filename"]
    decoy.write_bytes(b"raw-one")
    st_arch = os.stat(archive_file)
    os.utime(decoy, (st_arch.st_atime, st_arch.st_mtime))

    archive_parent = archive_file.parent
    swapped = []
    real_stat = os.stat

    def swapping_stat(p, *a, **kw):
        if str(p) == str(archive_file) and not swapped:
            swapped.append(True)
            import shutil
            shutil.rmtree(archive_parent)
            try:
                os.symlink(str(decoy_dir), str(archive_parent))
            except (OSError, NotImplementedError):
                pytest.skip("symlinks unsupported on this filesystem")
        return real_stat(p, *a, **kw)

    monkeypatch.setattr(card_cleanup.os, "stat", swapping_stat)
    source_root_real = os.path.realpath(str(tmp_path / "card"))
    result, reason = card_cleanup.qualify_rows(
        [row], source_root_real, str(card))
    assert result is None
    assert reason == card_cleanup.KEEP_INSIDE_SOURCE
    assert decoy.exists()
