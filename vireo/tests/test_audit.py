# vireo/tests/test_audit.py
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from PIL import Image


def test_check_drift_detects_xmp_change(tmp_path):
    """check_drift detects when XMP was modified after scan."""
    from audit import check_drift
    from db import Database
    from scanner import scan
    from xmp import write_sidecar

    root = str(tmp_path / "photos")
    os.makedirs(root)
    Image.new('RGB', (100, 100)).save(os.path.join(root, 'bird.jpg'))
    write_sidecar(
        os.path.join(root, 'bird.xmp'),
        flat_keywords={'Sparrow'},
        hierarchical_keywords=set(),
    )

    db = Database(str(tmp_path / "test.db"))
    scan(root, db)

    # Modify XMP after scan
    time.sleep(0.05)
    write_sidecar(
        os.path.join(root, 'bird.xmp'),
        flat_keywords={'Cardinal'},
        hierarchical_keywords=set(),
    )

    drifts = check_drift(db)
    assert len(drifts) >= 1
    assert drifts[0]['filename'] == 'bird.jpg'


def test_check_drift_ignores_case_only_difference(tmp_path):
    """A case-only keyword difference is not drift.

    resolve_drift('use_xmp') goes through sync_from_xmp, which reconciles
    case-insensitively and treats a case-only difference as already in
    sync — so reporting it here would create a permanently unresolvable
    drift entry.
    """
    from audit import check_drift
    from db import Database
    from scanner import scan
    from xmp import write_sidecar

    root = str(tmp_path / "photos")
    os.makedirs(root)
    Image.new('RGB', (100, 100)).save(os.path.join(root, 'bird.jpg'))
    xmp_path = os.path.join(root, 'bird.xmp')
    write_sidecar(xmp_path, flat_keywords={'Sparrow'}, hierarchical_keywords=set())

    db = Database(str(tmp_path / "test.db"))
    scan(root, db)

    # Rewrite the sidecar with the same keyword in a different case
    # (write_sidecar merges, so replace the file outright)
    os.unlink(xmp_path)
    write_sidecar(xmp_path, flat_keywords={'SPARROW'}, hierarchical_keywords=set())

    assert check_drift(db) == []


def test_check_drift_reports_real_difference_despite_case_noise(tmp_path):
    """Genuinely different keyword sets still drift; case-only pairs don't
    inflate the added/removed lists, and reported values keep the actual
    stored strings."""
    from audit import check_drift
    from db import Database
    from scanner import scan
    from xmp import write_sidecar

    root = str(tmp_path / "photos")
    os.makedirs(root)
    Image.new('RGB', (100, 100)).save(os.path.join(root, 'bird.jpg'))
    xmp_path = os.path.join(root, 'bird.xmp')
    write_sidecar(xmp_path, flat_keywords={'Sparrow'}, hierarchical_keywords=set())

    db = Database(str(tmp_path / "test.db"))
    scan(root, db)

    os.unlink(xmp_path)
    write_sidecar(
        xmp_path, flat_keywords={'SPARROW', 'Cardinal'}, hierarchical_keywords=set()
    )

    drifts = check_drift(db)
    assert len(drifts) == 1
    assert drifts[0]['added_in_xmp'] == ['Cardinal']
    assert drifts[0]['removed_in_xmp'] == []
    assert drifts[0]['direction'] == 'xmp_ahead'
    # Reported values show the strings as actually stored on each side
    assert drifts[0]['db_value'] == ['Sparrow']
    assert drifts[0]['xmp_value'] == ['Cardinal', 'SPARROW']


def test_check_orphans_detects_deleted_file(tmp_path):
    """check_orphans finds DB entries with no file on disk."""
    from audit import check_orphans
    from db import Database
    from scanner import scan

    root = str(tmp_path / "photos")
    os.makedirs(root)
    img_path = os.path.join(root, 'bird.jpg')
    Image.new('RGB', (100, 100)).save(img_path)

    db = Database(str(tmp_path / "test.db"))
    scan(root, db)

    # Delete file after scan
    os.unlink(img_path)

    orphans = check_orphans(db)
    assert len(orphans) == 1
    assert orphans[0]['filename'] == 'bird.jpg'


def test_check_untracked_finds_new_files(tmp_path):
    """check_untracked finds files on disk not in the DB."""
    from audit import check_untracked
    from db import Database
    from scanner import scan

    root = str(tmp_path / "photos")
    os.makedirs(root)
    Image.new('RGB', (100, 100)).save(os.path.join(root, 'known.jpg'))

    db = Database(str(tmp_path / "test.db"))
    scan(root, db)

    # Add new file after scan
    Image.new('RGB', (200, 200)).save(os.path.join(root, 'new_file.jpg'))

    untracked = check_untracked(db, [root])
    assert len(untracked) == 1
    assert 'new_file.jpg' in untracked[0]['path']


def test_check_untracked_skips_dangling_symlink(tmp_path):
    """A broken symlink with an image extension is not reported untracked.

    os.walk lists dangling symlinks in filenames; since scanner.scan skips
    non-files, flagging one would raise a warning a rescan can never clear.
    """
    from audit import check_untracked
    from db import Database

    root = str(tmp_path / "photos")
    os.makedirs(root)
    # A symlink named like an image but pointing at a nonexistent target.
    os.symlink(os.path.join(root, 'nonexistent.jpg'),
               os.path.join(root, 'broken.jpg'))

    db = Database(str(tmp_path / "test.db"))
    untracked = check_untracked(db, [root])
    assert untracked == []


def test_check_untracked_skips_photo_library_bundle(tmp_path):
    """check_untracked must not descend into a Photos library bundle."""
    from audit import check_untracked
    from db import Database

    root = str(tmp_path / "photos")
    lib = os.path.join(root, 'Photos Library.photoslibrary', 'originals')
    os.makedirs(lib)
    Image.new('RGB', (100, 100)).save(os.path.join(lib, 'managed.jpg'))
    Image.new('RGB', (100, 100)).save(os.path.join(root, 'real.jpg'))

    db = Database(str(tmp_path / "test.db"))
    untracked = check_untracked(db, [root])
    paths = [u['path'] for u in untracked]
    assert any('real.jpg' in p for p in paths)
    assert not any('.photoslibrary' in p for p in paths)


def test_check_untracked_skips_excluded_root_itself(tmp_path):
    """check_untracked must not open a root that *is* the excluded bundle.

    A configured folder root of ``~/Pictures/Photos Library.photoslibrary``
    would otherwise have os.walk open the bundle (prune_scan_dirs only
    filters *children*), defeating the TCC-prompt guard.
    """
    from audit import check_untracked
    from db import Database

    root = str(tmp_path / "Photos Library.photoslibrary")
    os.makedirs(os.path.join(root, 'originals'))
    Image.new('RGB', (100, 100)).save(
        os.path.join(root, 'originals', 'managed.jpg')
    )

    db = Database(str(tmp_path / "test.db"))
    assert check_untracked(db, [root]) == []


def test_check_untracked_skips_root_nested_in_excluded_bundle(tmp_path):
    """check_untracked must also reject roots that *sit inside* an excluded
    bundle. A leaf-only check would let
    ``.../Photos Library.photoslibrary/originals`` through (basename
    ``originals`` is unremarkable) and os.walk would open the protected
    bundle subtree, defeating the TCC-prompt guard.
    """
    from audit import check_untracked
    from db import Database

    root = str(tmp_path / "Photos Library.photoslibrary" / "originals")
    os.makedirs(os.path.join(root, '0'))
    Image.new('RGB', (100, 100)).save(os.path.join(root, '0', 'managed.jpg'))

    db = Database(str(tmp_path / "test.db"))
    assert check_untracked(db, [root]) == []


def test_check_stray_sidecars_skips_root_nested_in_excluded_bundle(tmp_path):
    """check_stray_sidecars must also reject roots nested inside an excluded
    bundle so .xmp files inside the protected subtree don't trip the macOS
    TCC prompt the bundle guard exists to avoid.
    """
    from audit import check_stray_sidecars
    from xmp import write_sidecar

    root = str(tmp_path / "Photos Library.photoslibrary" / "originals")
    os.makedirs(root)
    write_sidecar(os.path.join(root, 'ghost.xmp'),
                  flat_keywords={'Gone'}, hierarchical_keywords=set())

    assert check_stray_sidecars([root]) == []


def test_audit_rejects_excluded_root_before_statting(tmp_path, monkeypatch):
    """Both audit walkers must run the bundle guard BEFORE ``os.path.isdir``.

    ``isdir`` follows symlinks and stat's the target, which alone trips the
    macOS TCC prompt for a directly selected bundle or a symlink to one.
    Tested by failing if ``os.path.isdir`` is called on a path the
    exclusion check covers — if the order is wrong, the stat sneaks in
    before the guard returns.
    """
    from audit import check_stray_sidecars, check_untracked
    from db import Database
    from image_loader import is_excluded_scan_path

    real_isdir = os.path.isdir

    def guarded_isdir(path):
        if is_excluded_scan_path(path):
            raise AssertionError(
                f"os.path.isdir called on excluded path before guard: {path}"
            )
        return real_isdir(path)

    monkeypatch.setattr(os.path, "isdir", guarded_isdir)

    bundle = str(tmp_path / "Photos Library.photoslibrary")
    os.makedirs(os.path.join(bundle, "originals"))

    db = Database(str(tmp_path / "test.db"))
    assert check_untracked(db, [bundle]) == []
    assert check_stray_sidecars([bundle]) == []


def test_remove_orphans(tmp_path):
    """remove_orphans deletes DB entries for missing files."""
    from audit import remove_orphans
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder('/gone', name='gone')
    pid = db.add_photo(folder_id=fid, filename='missing.jpg', extension='.jpg',
                       file_size=100, file_mtime=1.0)

    remove_orphans(db, [pid])

    photo = db.get_photo(pid)
    assert photo is None


def test_check_stray_sidecars_skips_symlinked_directory_with_image_suffix(tmp_path):
    """A symlink to a directory whose name ends in .xmp/.jpg must not be
    treated as a sidecar/image.

    ``safe_scan_walk`` classifies entries with ``follow_symlinks=False``,
    so a ``ghost.xmp -> RealAlbum`` link lands in ``filenames`` rather
    than ``dirnames``. Without the ``os.path.isfile`` guard the audit
    would report a bogus stray (and ``delete_stray_sidecars`` would then
    unlink a real directory link).
    """
    from audit import check_stray_sidecars

    root = str(tmp_path / "photos")
    os.makedirs(root)
    real_dir = os.path.join(root, "RealAlbum")
    os.makedirs(real_dir)
    # Symlink with a sidecar-shaped name that points at a real directory.
    os.symlink(real_dir, os.path.join(root, "ghost.xmp"))
    # And one shaped like an image, to confirm the image side is ignored
    # too (so a real ``ghost.xmp`` next to it wouldn't be silently matched
    # against the link's basename).
    os.symlink(real_dir, os.path.join(root, "decoy.jpg"))

    assert check_stray_sidecars([root]) == []


def test_check_stray_sidecars(tmp_path):
    """check_stray_sidecars flags .xmp files with no matching image,
    in both bird.xmp and bird.jpg.xmp naming styles."""
    from audit import check_stray_sidecars
    from xmp import write_sidecar

    root = str(tmp_path / "photos")
    os.makedirs(root)
    # Matched: classic style (bird.jpg + bird.xmp)
    Image.new('RGB', (50, 50)).save(os.path.join(root, 'bird.jpg'))
    write_sidecar(os.path.join(root, 'bird.xmp'),
                  flat_keywords={'Sparrow'}, hierarchical_keywords=set())
    # Matched: darktable style (owl.jpg + owl.jpg.xmp)
    Image.new('RGB', (50, 50)).save(os.path.join(root, 'owl.jpg'))
    write_sidecar(os.path.join(root, 'owl.jpg.xmp'),
                  flat_keywords={'Owl'}, hierarchical_keywords=set())
    # Stray: no image anywhere
    write_sidecar(os.path.join(root, 'ghost.xmp'),
                  flat_keywords={'Gone'}, hierarchical_keywords=set())

    strays = check_stray_sidecars([root])
    assert len(strays) == 1
    assert strays[0]['path'].endswith('ghost.xmp')


def test_delete_stray_sidecars_refuses_matched_sidecar(tmp_path):
    """delete_stray_sidecars re-verifies at deletion time: a sidecar whose
    image reappeared since the check is kept, and non-xmp paths are
    never touched."""
    from audit import delete_stray_sidecars
    from xmp import write_sidecar

    root = str(tmp_path / "photos")
    os.makedirs(root)
    matched = os.path.join(root, 'bird.xmp')
    stray = os.path.join(root, 'ghost.xmp')
    not_xmp = os.path.join(root, 'precious.jpg')
    Image.new('RGB', (50, 50)).save(not_xmp)
    write_sidecar(matched, flat_keywords={'A'}, hierarchical_keywords=set())
    write_sidecar(stray, flat_keywords={'B'}, hierarchical_keywords=set())
    Image.new('RGB', (50, 50)).save(os.path.join(root, 'bird.jpg'))

    deleted = delete_stray_sidecars([matched, stray, not_xmp], [root])

    assert deleted == 1
    assert os.path.exists(matched), "sidecar with a living image was deleted"
    assert os.path.exists(not_xmp), "non-xmp file was deleted"
    assert not os.path.exists(stray)


def test_delete_stray_sidecars_refuses_paths_outside_roots(tmp_path):
    """The client path list is untrusted: sidecars outside the allowed
    roots are refused, including prefix-trap siblings (/root-evil must
    not match root /root)."""
    from audit import delete_stray_sidecars
    from xmp import write_sidecar

    root = str(tmp_path / "photos")
    evil_sibling = str(tmp_path / "photos-evil")
    elsewhere = str(tmp_path / "elsewhere")
    for d in (root, evil_sibling, elsewhere):
        os.makedirs(d)

    inside = os.path.join(root, 'stray.xmp')
    sibling = os.path.join(evil_sibling, 'stray.xmp')
    outside = os.path.join(elsewhere, 'stray.xmp')
    for p in (inside, sibling, outside):
        write_sidecar(p, flat_keywords={'X'}, hierarchical_keywords=set())

    deleted = delete_stray_sidecars([inside, sibling, outside], [root])

    assert deleted == 1
    assert not os.path.exists(inside)
    assert os.path.exists(sibling), "prefix-trap sibling dir was not refused"
    assert os.path.exists(outside), "path outside roots was not refused"


def test_verify_hashes_ok_and_baseline(tmp_path):
    """Untouched files verify as ok; photos without a stored hash get
    baselined; the integrity run is recorded for the summary."""
    from audit import verify_hashes
    from db import Database
    from scanner import scan

    root = str(tmp_path / "photos")
    os.makedirs(root)
    Image.new('RGB', (60, 60)).save(os.path.join(root, 'good.jpg'))
    Image.new('RGB', (70, 70)).save(os.path.join(root, 'nohash.jpg'))

    db = Database(str(tmp_path / "test.db"))
    scan(root, db)
    # Simulate a photo imported before hashing existed
    db.conn.execute(
        "UPDATE photos SET file_hash = NULL WHERE filename = 'nohash.jpg'")
    db.conn.commit()

    stats = verify_hashes(db)

    assert stats['ok'] == 1
    assert stats['baselined'] == 1
    assert stats['corrupt'] == 0 and stats['modified'] == 0
    row = db.conn.execute(
        "SELECT file_hash, hash_status FROM photos "
        "WHERE filename = 'nohash.jpg'").fetchone()
    assert row['file_hash'] is not None
    assert row['hash_status'] == 'ok'
    runs = db.get_audit_runs()
    assert runs['integrity']['problem_count'] == 0


def test_verify_hashes_flags_corruption_when_mtime_unchanged(tmp_path):
    """Content change with the original mtime is the bit-rot signature."""
    from audit import verify_hashes
    from db import Database
    from scanner import scan

    root = str(tmp_path / "photos")
    os.makedirs(root)
    path = os.path.join(root, 'rot.jpg')
    Image.new('RGB', (60, 60)).save(path)

    db = Database(str(tmp_path / "test.db"))
    scan(root, db)

    # Flip bytes but restore the original mtime: nothing legitimately
    # wrote this file, yet its content changed.
    st = os.stat(path)
    with open(path, 'r+b') as f:
        f.seek(10)
        f.write(b'\xde\xad\xbe\xef')
    os.utime(path, (st.st_atime, st.st_mtime))

    stats = verify_hashes(db)

    assert stats['corrupt'] == 1
    row = db.conn.execute(
        "SELECT hash_status FROM photos WHERE filename = 'rot.jpg'"
    ).fetchone()
    assert row['hash_status'] == 'corrupt'
    assert db.get_audit_runs()['integrity']['problem_count'] == 1
    flagged = db.get_integrity_flagged()
    assert len(flagged) == 1
    assert flagged[0]['filename'] == 'rot.jpg'


def test_verify_hashes_flags_external_edit_as_modified(tmp_path):
    """Content change with a moved mtime is an external edit, not rot."""
    from audit import verify_hashes
    from db import Database
    from scanner import scan

    root = str(tmp_path / "photos")
    os.makedirs(root)
    path = os.path.join(root, 'edited.jpg')
    Image.new('RGB', (60, 60)).save(path)

    db = Database(str(tmp_path / "test.db"))
    scan(root, db)

    Image.new('RGB', (60, 60), (200, 0, 0)).save(path)
    st = os.stat(path)
    os.utime(path, (st.st_atime, st.st_mtime + 10))

    stats = verify_hashes(db)

    assert stats['modified'] == 1
    row = db.conn.execute(
        "SELECT hash_status FROM photos WHERE filename = 'edited.jpg'"
    ).fetchone()
    assert row['hash_status'] == 'modified'


def test_accept_current_hash_clears_flag(tmp_path):
    """Accepting a flagged file stores its current content as the new
    baseline and clears the problem flag."""
    from audit import accept_current_hash, verify_hashes
    from db import Database
    from scanner import scan

    root = str(tmp_path / "photos")
    os.makedirs(root)
    path = os.path.join(root, 'edited.jpg')
    Image.new('RGB', (60, 60)).save(path)

    db = Database(str(tmp_path / "test.db"))
    scan(root, db)

    Image.new('RGB', (60, 60), (0, 200, 0)).save(path)
    st = os.stat(path)
    os.utime(path, (st.st_atime, st.st_mtime + 10))
    verify_hashes(db)
    assert len(db.get_integrity_flagged()) == 1

    pid = db.conn.execute(
        "SELECT id FROM photos WHERE filename = 'edited.jpg'").fetchone()['id']
    accepted = accept_current_hash(db, [pid])

    assert accepted == 1
    assert db.get_integrity_flagged() == []
    # Re-verifying immediately is clean: the new baseline matches disk.
    stats = verify_hashes(db)
    assert stats['ok'] == 1 and stats['modified'] == 0


def test_rescan_resets_hash_coverage_after_external_edit(tmp_path):
    """When a rescan replaces the stored baseline hash, the verification
    markers must be cleared: 'checked' means THIS baseline was verified,
    not that some earlier content once was. A rescan that recomputes the
    same hash leaves coverage intact."""
    from audit import verify_hashes
    from db import Database
    from scanner import scan

    root = str(tmp_path / "photos")
    os.makedirs(root)
    path = os.path.join(root, 'edited.jpg')
    Image.new('RGB', (60, 60)).save(path)

    db = Database(str(tmp_path / "test.db"))
    scan(root, db)
    verify_hashes(db)
    assert db.get_integrity_stats()['unchecked'] == 0

    # Touch-only rescan: mtime moves but bytes are identical, so the
    # recomputed hash matches the verified baseline — coverage survives.
    st = os.stat(path)
    os.utime(path, (st.st_atime, st.st_mtime + 5))
    scan(root, db)
    assert db.get_integrity_stats()['unchecked'] == 0

    # External edit + rescan: the scanner adopts a new baseline that this
    # audit never verified, so the verdict and coverage must reset.
    Image.new('RGB', (60, 60), (0, 0, 200)).save(path)
    st = os.stat(path)
    os.utime(path, (st.st_atime, st.st_mtime + 10))
    scan(root, db)

    row = db.conn.execute(
        "SELECT hash_checked_at, hash_status FROM photos "
        "WHERE filename = 'edited.jpg'").fetchone()
    assert row['hash_checked_at'] is None
    assert row['hash_status'] is None
    stats = db.get_integrity_stats()
    assert stats['unchecked'] == 1
    assert stats['checked'] == 0


def test_build_summary_states(tmp_path):
    """The green light requires every check to have run AND found nothing
    AND full hash coverage — never 'no evidence of problems'."""
    from audit import build_summary, verify_hashes
    from db import Database
    from scanner import scan

    root = str(tmp_path / "photos")
    os.makedirs(root)
    Image.new('RGB', (60, 60)).save(os.path.join(root, 'good.jpg'))

    db = Database(str(tmp_path / "test.db"))
    scan(root, db)

    # Nothing has run: unverified, never intact
    s = build_summary(db)
    assert s['status'] == 'unverified'

    # Four checks ran clean but hashes never verified: still not intact
    for name in ('drift', 'orphans', 'untracked', 'sidecars'):
        db.record_audit_run(name, 0)
    s = build_summary(db)
    assert s['status'] == 'unverified'

    # All five ran, all clean, full coverage: intact
    verify_hashes(db)
    s = build_summary(db)
    assert s['status'] == 'intact'
    assert s['problem_count'] == 0

    # A new photo lands after the verify run: clean but stale
    Image.new('RGB', (60, 60)).save(os.path.join(root, 'new.jpg'))
    scan(root, db, incremental=True)
    db.record_audit_run('untracked', 0)
    s = build_summary(db)
    assert s['status'] == 'stale'
    assert s['integrity']['unchecked'] == 1

    # A check with findings: problems
    db.record_audit_run('drift', 3)
    s = build_summary(db)
    assert s['status'] == 'problems'
    assert s['problem_count'] == 3


def test_verify_hashes_missing_file_updates_orphans_verdict(tmp_path):
    """A file deleted after everything ran clean must not leave the banner
    green when only the hash verifier re-runs: verify_hashes applies the
    orphans check's exact predicate to its exact population, so a
    completed run records the orphans result too."""
    from audit import build_summary, verify_hashes
    from db import Database
    from scanner import scan

    root = str(tmp_path / "photos")
    os.makedirs(root)
    Image.new('RGB', (60, 60)).save(os.path.join(root, 'keep.jpg'))
    Image.new('RGB', (60, 60)).save(os.path.join(root, 'gone.jpg'))

    db = Database(str(tmp_path / "test.db"))
    scan(root, db)
    for name in ('drift', 'orphans', 'untracked', 'sidecars'):
        db.record_audit_run(name, 0)
    verify_hashes(db)
    assert build_summary(db)['status'] == 'intact'

    # File disappears; only the hash verifier re-runs.
    os.unlink(os.path.join(root, 'gone.jpg'))
    stats = verify_hashes(db)
    assert stats['missing'] == 1

    # The missing file lands on the orphans verdict, not integrity's.
    runs = db.get_audit_runs()
    assert runs['orphans']['problem_count'] == 1
    assert runs['integrity']['problem_count'] == 0

    s = build_summary(db)
    assert s['status'] == 'problems'
    assert s['problem_count'] == 1

    # Restoring the file and re-verifying clears the verdict again.
    Image.new('RGB', (60, 60)).save(os.path.join(root, 'gone.jpg'))
    db.conn.execute(
        "UPDATE photos SET file_hash = NULL WHERE filename = 'gone.jpg'")
    db.conn.commit()
    verify_hashes(db)
    assert db.get_audit_runs()['orphans']['problem_count'] == 0
    assert build_summary(db)['status'] == 'intact'


def test_build_summary_missing_folder_blocks_intact(tmp_path):
    """A workspace folder marked missing must surface as a problem even
    when every recorded check is clean: the scoped queries all exclude
    missing folders, so without this the banner would show green over
    an entire offline folder of photos."""
    from audit import build_summary, verify_hashes
    from db import Database
    from scanner import scan

    root = str(tmp_path / "photos")
    os.makedirs(root)
    Image.new('RGB', (60, 60)).save(os.path.join(root, 'good.jpg'))

    db = Database(str(tmp_path / "test.db"))
    scan(root, db)
    for name in ('drift', 'orphans', 'untracked', 'sidecars'):
        db.record_audit_run(name, 0)
    verify_hashes(db)
    assert build_summary(db)['status'] == 'intact'

    # The folder goes offline and the health loop flags it.
    db.conn.execute("UPDATE folders SET status = 'missing'")
    db.conn.commit()

    s = build_summary(db)
    assert s['status'] == 'problems'
    assert s['missing_folders'] == 1
    assert s['missing_folder_photos'] == 1
    # Outranks 'unverified' too: a missing folder is direct evidence
    # of a problem even before any check has run.
    db.conn.execute("DELETE FROM audit_runs")
    db.conn.commit()
    assert build_summary(db)['status'] == 'problems'


def test_untracked_and_sidecar_endpoints_use_workspace_roots(
    tmp_path, monkeypatch,
):
    """/api/audit/untracked and /api/audit/sidecars derive their scan
    roots server-side from the active workspace: a request with no
    ``root`` params still scans everything, stray client params are
    ignored, and the recorded audit run reflects the real workspace —
    a client-scoped request can never certify a clean workspace check.
    """
    monkeypatch.setenv("HOME", str(tmp_path))
    import config as cfg
    import models
    from app import create_app
    from db import Database
    from scanner import scan
    from xmp import write_sidecar

    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    monkeypatch.setattr(
        models, "DEFAULT_MODELS_DIR", str(tmp_path / "vireo-models"),
    )
    monkeypatch.setattr(models, "CONFIG_PATH", str(tmp_path / "models.json"))

    root = str(tmp_path / "photos")
    os.makedirs(root)
    Image.new('RGB', (60, 60)).save(os.path.join(root, 'known.jpg'))

    db_path = str(tmp_path / "vireo.db")
    db = Database(db_path)
    scan(root, db)

    # Material that only a real scan of the workspace root would find
    Image.new('RGB', (60, 60)).save(os.path.join(root, 'new.jpg'))
    write_sidecar(os.path.join(root, 'ghost.xmp'),
                  flat_keywords={'Gone'}, hierarchical_keywords=set())

    app = create_app(
        db_path=db_path,
        thumb_cache_dir=str(tmp_path / "thumbnails"),
        api_token="test-token-123",
    )
    client = app.test_client()

    # No root params: the server derives the workspace roots itself.
    resp = client.get("/api/audit/untracked")
    assert resp.status_code == 200
    untracked = resp.get_json()
    assert len(untracked) == 1
    assert untracked[0]['path'].endswith('new.jpg')

    # A stray client root pointing at an empty dir is ignored, not
    # honored — the workspace root is still what gets scanned.
    empty = str(tmp_path / "empty")
    os.makedirs(empty)
    resp = client.get("/api/audit/sidecars", query_string={"root": empty})
    assert resp.status_code == 200
    strays = resp.get_json()
    assert len(strays) == 1
    assert strays[0]['path'].endswith('ghost.xmp')

    runs = db.get_audit_runs()
    assert runs['untracked']['problem_count'] == 1
    assert runs['sidecars']['problem_count'] == 1


def test_remove_orphans_endpoint_unlinks_cached_thumbnail(
    tmp_path, monkeypatch,
):
    """The /api/audit/remove-orphans endpoint must remove cached
    thumbnails for the photos it drops. Without this cleanup, the
    next photo to inherit the same SQLite rowid (``photos.id`` is
    INTEGER PRIMARY KEY without AUTOINCREMENT, so deleted IDs at the
    high end are reused on the next insert) would inherit the orphaned
    JPEG and the user would see the wrong photo on the encounter
    grid.
    """
    monkeypatch.setenv("HOME", str(tmp_path))
    import config as cfg
    import models
    from app import create_app
    from db import Database

    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    monkeypatch.setattr(
        models, "DEFAULT_MODELS_DIR", str(tmp_path / "vireo-models"),
    )
    monkeypatch.setattr(
        models, "CONFIG_PATH", str(tmp_path / "models.json"),
    )

    vireo_dir = tmp_path / "vireo"
    vireo_dir.mkdir()
    thumb_dir = vireo_dir / "thumbnails"
    thumb_dir.mkdir()
    db_path = str(vireo_dir / "vireo.db")

    db = Database(db_path)
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    fid = db.add_folder("/gone", name="gone")
    pid = db.add_photo(
        folder_id=fid, filename="missing.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    # Stage the cached thumbnail that will become orphaned by the
    # remove-orphans call. This mirrors a real-world state where the
    # source file vanished from disk but the cached JPEG persists.
    thumb_file = thumb_dir / f"{pid}.jpg"
    Image.new("RGB", (50, 50), (1, 2, 3)).save(str(thumb_file), "JPEG")
    assert thumb_file.exists(), "precondition: cached thumb staged"

    app = create_app(
        db_path=db_path, thumb_cache_dir=str(thumb_dir),
        api_token="test-token-123",
    )
    client = app.test_client()
    resp = client.post(
        "/api/audit/remove-orphans",
        json={"photo_ids": [pid]},
    )

    assert resp.status_code == 200
    body = resp.get_json()
    assert body["ok"] is True
    assert body["removed"] == 1

    # The DB row is gone …
    assert db.get_photo(pid) is None
    # … and so is the cached thumbnail. A future photo that inherits
    # this ID will get a fresh thumbnail from its own source.
    assert not thumb_file.exists(), (
        "remove-orphans left a cached thumbnail behind; the next photo "
        "to inherit this rowid will be served the orphaned JPEG"
    )
