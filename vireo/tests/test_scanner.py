# vireo/tests/test_scanner.py
import json
import os
import shutil
import sys
import time

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from PIL import Image

requires_exiftool = pytest.mark.skipif(
    shutil.which("exiftool") is None,
    reason="exiftool not installed",
)


def _create_test_images(root, structure):
    """Create test image files in a directory structure.

    Args:
        root: base directory path
        structure: dict of {relative_path: [filenames]}
    """
    for rel_path, filenames in structure.items():
        folder = os.path.join(root, rel_path) if rel_path else root
        os.makedirs(folder, exist_ok=True)
        for fname in filenames:
            img = Image.new('RGB', (200, 100), color='green')
            img.save(os.path.join(folder, fname))


def test_scan_discovers_folders(tmp_path):
    """scan() creates folder entries for all directories containing images."""
    from db import Database
    from scanner import scan

    root = str(tmp_path / "photos")
    _create_test_images(root, {
        '': ['root.jpg'],
        '2024': ['a.jpg'],
        '2024/January': ['b.jpg'],
    })

    db = Database(str(tmp_path / "test.db"))
    scan(root, db)

    folders = db.get_folder_tree()
    paths = [f['path'] for f in folders]
    assert root in paths
    assert os.path.join(root, '2024') in paths
    assert os.path.join(root, '2024', 'January') in paths


def test_scan_skips_app_managed_library_bundles(tmp_path):
    """scan() must not descend into macOS app-managed library bundles.

    Walking into "*.photoslibrary" (which ~/Pictures contains by default)
    or "*.musiclibrary" (which ~/Music can contain) triggers the recurring
    macOS "access data from other apps" TCC prompt and would ingest
    app-managed derivatives or media internals. Images inside the bundle must
    be ignored while real sibling photos are still discovered.
    """
    from db import Database
    from scanner import scan

    root = str(tmp_path / "photos")
    _create_test_images(root, {
        '': ['real.jpg'],
        'Photos Library.photoslibrary/originals/0': ['managed.jpg'],
        'Music Library.musiclibrary/Media.localized': ['cover.jpg'],
        'Photo Booth Library/Pictures': ['booth.jpg'],
    })

    db = Database(str(tmp_path / "test.db"))
    scan(root, db)

    photos = db.get_photos(per_page=100)
    filenames = {p['filename'] for p in photos}
    assert filenames == {'real.jpg'}

    folder_paths = [f['path'] for f in db.get_folder_tree()]
    assert not any('.photoslibrary' in p for p in folder_paths)
    assert not any('.musiclibrary' in p for p in folder_paths)
    assert not any('Photo Booth Library' in p for p in folder_paths)


def test_scan_skips_excluded_root_itself(tmp_path):
    """scan() must not open the root when the root *is* the excluded bundle.

    prune_scan_dirs only filters children, so if a user selects (or imports)
    ``~/Pictures/Photos Library.photoslibrary`` directly as a scan root,
    os.walk would still open the bundle and trip the macOS TCC prompt this
    guard exists to avoid. The guard must short-circuit before any walk.
    """
    from db import Database
    from scanner import scan

    root = str(tmp_path / "Photos Library.photoslibrary")
    _create_test_images(root, {
        'originals/0': ['managed.jpg'],
    })

    db = Database(str(tmp_path / "test.db"))
    scan(root, db)

    assert db.get_photos(per_page=100) == []
    assert db.get_folder_tree() == []


def test_scan_skips_root_nested_in_excluded_bundle(tmp_path):
    """scan() must reject roots that sit *inside* an excluded bundle, not
    just roots whose leaf name matches.

    A user can select ``.../Photos Library.photoslibrary/originals`` directly,
    or a stale folder row from before this guard existed can carry the same
    shape. The leaf basename (``originals``) is unremarkable, so a leaf-only
    check passes — and os.walk then opens the protected bundle subtree and
    re-trips the macOS TCC prompt this guard exists to avoid.
    """
    from db import Database
    from scanner import scan

    nested_root = str(tmp_path / "Photos Library.photoslibrary" / "originals")
    _create_test_images(nested_root, {
        '0': ['managed.jpg'],
    })

    db = Database(str(tmp_path / "test.db"))
    scan(nested_root, db)

    assert db.get_photos(per_page=100) == []
    assert db.get_folder_tree() == []


def test_scan_skips_restrict_dirs_inside_excluded_bundle(tmp_path):
    """scan() must reject ``restrict_dirs`` entries that point inside an
    excluded bundle even when the outer ``root`` is unremarkable.

    The pipeline / repair paths build ``restrict_dirs`` from existing
    folder rows in the workspace, which can include stale entries from
    before the bundle guards landed (e.g. ``.../Photos Library.photoslibrary/
    originals``). The outer root guard (``is_excluded_scan_path(root_path)``)
    only checks ``root``; without a per-entry guard the restrict_dirs branch
    still calls ``dp.is_dir()`` / ``dp.iterdir()`` on the protected
    subtree and re-trips the macOS TCC prompt this change exists to avoid.
    """
    from db import Database
    from scanner import scan

    root = str(tmp_path / "photos")
    _create_test_images(root, {
        '': ['real.jpg'],
    })
    bundle_sub = str(
        tmp_path / "photos" / "Photos Library.photoslibrary" / "originals"
    )
    _create_test_images(bundle_sub, {'': ['managed.jpg']})

    db = Database(str(tmp_path / "test.db"))
    # Both the real top-level folder and a bundle-internal "originals"
    # subfolder are passed as restrict_dirs (the shape pipeline_job would
    # produce from a workspace that had previously linked the bundle).
    scan(root, db, restrict_dirs=[root, bundle_sub])

    photos = db.get_photos(per_page=100)
    assert {p['filename'] for p in photos} == {'real.jpg'}
    folder_paths = [f['path'] for f in db.get_folder_tree()]
    assert not any('.photoslibrary' in p for p in folder_paths)


def test_scan_filters_excluded_restrict_dirs_from_working_copy_scope(
    tmp_path, monkeypatch,
):
    """The working-copy extraction pass must scope to the
    ``restrict_dirs`` entries the discovery loop actually walked, not
    the raw caller-supplied list.

    The discovery loop already drops excluded restrict_dirs (covered by
    :func:`test_scan_skips_restrict_dirs_inside_excluded_bundle`), but the
    post-scan ``_extract_working_copies`` call used to reuse the raw
    ``restrict_dirs`` to build ``wc_scope``. If a stale DB row already
    pointed at a bundle-internal folder (e.g. from before the bundle guard
    landed), the extractor's SQL match would still pick it up and the
    follow-up file read of ``folder_path/filename`` would re-trip the
    macOS "access data from other apps" TCC prompt — exactly the prompt
    this guard exists to avoid. The scope passed to the extractor must
    therefore mirror the filtered set.
    """
    import scanner
    from db import Database

    root = str(tmp_path / "photos")
    _create_test_images(root, {'': ['real.jpg']})
    bundle_sub = str(
        tmp_path / "photos" / "Photos Library.photoslibrary" / "originals"
    )
    _create_test_images(bundle_sub, {'': ['managed.jpg']})

    captured = {}

    def fake_extract_working_copies(db, vireo_dir, *, progress_callback=None,
                                    status_callback=None, scope=None,
                                    cancel_check=None):
        captured["scope"] = scope

    monkeypatch.setattr(scanner, "_extract_working_copies",
                        fake_extract_working_copies)

    db = Database(str(tmp_path / "test.db"))
    scanner.scan(
        root, db,
        restrict_dirs=[root, bundle_sub],
        vireo_dir=str(tmp_path / "vireo_dir"),
    )

    assert "scope" in captured, "expected _extract_working_copies to be called"
    scope_paths = [entry[0] if isinstance(entry, tuple) else entry
                   for entry in (captured["scope"] or [])]
    assert root in scope_paths
    assert all(".photoslibrary" not in p for p in scope_paths), (
        f"bundle-internal restrict_dir leaked into wc_scope: {scope_paths}"
    )


def test_scan_skips_symlinked_excluded_bundle_child(tmp_path):
    """A child symlink in the scan root whose target is an excluded
    bundle must be dropped before ``os.walk``'s classification stat
    follows the link.

    The previous walker called ``os.walk`` → ``DirEntry.is_dir()`` to
    classify each child; that follows symlinks, so for a child like
    ``LibraryAlias -> Photos Library.photoslibrary`` the stat alone
    reached into the protected bundle and re-tripped the macOS TCC
    prompt this change exists to avoid — even though
    ``prune_scan_dirs`` would have removed the entry from recursion
    afterwards. The walker has to skip the link textually
    (``os.readlink``) before any followed call.
    """
    if sys.platform == "win32":
        pytest.skip("POSIX symlinks required")
    from db import Database
    from scanner import scan

    bundle = tmp_path / "Photos Library.photoslibrary"
    _create_test_images(str(bundle), {'originals/0': ['managed.jpg']})

    root = str(tmp_path / "photos")
    _create_test_images(root, {'': ['real.jpg']})
    os.symlink(str(bundle), os.path.join(root, "LibraryAlias"))

    db = Database(str(tmp_path / "test.db"))
    scan(root, db)

    photos = db.get_photos(per_page=100)
    filenames = {p['filename'] for p in photos}
    assert filenames == {'real.jpg'}

    folder_paths = [f['path'] for f in db.get_folder_tree()]
    assert not any('.photoslibrary' in p for p in folder_paths)
    assert not any('LibraryAlias' in p for p in folder_paths)


def test_scan_rejects_excluded_root_before_statting(tmp_path, monkeypatch):
    """The bundle guard must run BEFORE ``Path.is_dir`` on the root.

    ``Path.is_dir`` follows symlinks and stat's the target, so for a
    directly selected bundle (or a symlink to one) the existence test
    alone is enough to trip the macOS "access data from other apps" TCC
    prompt the guard exists to avoid. Tested by failing the test if
    ``Path.is_dir`` is ever called on a path the exclusion check covers —
    if the order is wrong, the stat sneaks in before the guard returns.
    """
    from pathlib import Path

    from db import Database
    from scanner import scan

    real_is_dir = Path.is_dir

    def guarded_is_dir(self):
        from image_loader import is_excluded_scan_path
        if is_excluded_scan_path(self):
            raise AssertionError(
                f"is_dir() called on excluded path before guard: {self}"
            )
        return real_is_dir(self)

    monkeypatch.setattr(Path, "is_dir", guarded_is_dir)

    bundle = tmp_path / "Photos Library.photoslibrary"
    _create_test_images(str(bundle), {'originals/0': ['managed.jpg']})

    db = Database(str(tmp_path / "test.db"))
    scan(str(bundle), db)

    assert db.get_photos(per_page=100) == []


def test_scan_discovers_photos(tmp_path):
    """scan() creates photo entries for all image files."""
    from db import Database
    from scanner import scan

    root = str(tmp_path / "photos")
    _create_test_images(root, {
        '': ['img1.jpg', 'img2.jpg'],
        'sub': ['img3.jpg'],
    })

    db = Database(str(tmp_path / "test.db"))
    scan(root, db)

    photos = db.get_photos(per_page=100)
    filenames = {p['filename'] for p in photos}
    assert filenames == {'img1.jpg', 'img2.jpg', 'img3.jpg'}


def test_scan_zero_byte_images_are_not_duplicate_photos(tmp_path):
    """Empty image files are corruption/placeholders, not duplicate photos."""
    from db import Database
    from scanner import EMPTY_FILE_SHA256, scan

    root = tmp_path / "photos"
    root.mkdir()
    (root / "DSC_0001.NEF").write_bytes(b"")
    (root / "DSC_0002.NEF").write_bytes(b"")

    db = Database(str(tmp_path / "test.db"))
    scan(str(root), db)

    rows = db.conn.execute(
        "SELECT filename, file_size, file_hash, flag FROM photos ORDER BY filename"
    ).fetchall()
    assert [r["filename"] for r in rows] == ["DSC_0001.NEF", "DSC_0002.NEF"]
    assert all(r["file_size"] == 0 for r in rows)
    assert all(r["file_hash"] is None for r in rows)
    assert all(r["flag"] != "rejected" for r in rows)

    # Historical repair: older scans stored the SHA-256 of empty bytes, which
    # made unrelated empty files look like exact duplicates. A rescan should
    # clear that duplicate identity. The ``flag`` column is left as-is
    # because a 'rejected' value could come from the user (Browse / culling)
    # just as easily as from past duplicate auto-resolution — silently
    # un-rejecting a manually rejected placeholder would be worse than
    # leaving it; the duplicates page calls out empty groups for review.
    db.conn.execute(
        "UPDATE photos SET file_hash = ?, flag = 'rejected'",
        (EMPTY_FILE_SHA256,),
    )
    db.conn.commit()

    scan(str(root), db, incremental=True)

    repaired = db.conn.execute(
        "SELECT file_hash, flag FROM photos ORDER BY filename"
    ).fetchall()
    assert all(r["file_hash"] is None for r in repaired)
    assert all(r["flag"] == "rejected" for r in repaired)


def test_scan_cancel_check_aborts_before_discovery(tmp_path):
    """scan() honors cancel_check before doing scan work."""
    from db import Database
    from scanner import scan

    root = str(tmp_path / "photos")
    _create_test_images(root, {'': ['img1.jpg']})
    db = Database(str(tmp_path / "test.db"))

    with pytest.raises(RuntimeError, match="scan cancelled"):
        scan(root, db, cancel_check=lambda: True)

    assert db.get_photos(per_page=100) == []


def test_scan_cancel_check_aborts_before_metadata_extraction(tmp_path, monkeypatch):
    """A cancel requested after discovery stops before expensive metadata work."""
    import scanner
    from db import Database

    root = str(tmp_path / "photos")
    _create_test_images(root, {'': ['img1.jpg', 'img2.jpg']})
    db = Database(str(tmp_path / "test.db"))
    cancelled = {"value": False}

    def progress_cb(current, total):
        if current == 0 and total == 2:
            cancelled["value"] = True

    def fail_extract_metadata(_paths):
        raise AssertionError("extract_metadata should not run after cancellation")

    monkeypatch.setattr(scanner, "extract_metadata", fail_extract_metadata)

    with pytest.raises(RuntimeError, match="scan cancelled"):
        scanner.scan(
            root,
            db,
            progress_callback=progress_cb,
            cancel_check=lambda: cancelled["value"],
        )

    assert db.get_photos(per_page=100) == []


def test_scan_reports_metadata_phase_progress(tmp_path, monkeypatch):
    """ExifTool batch progress is surfaced as status callback metadata."""
    import scanner
    from db import Database

    root = str(tmp_path / "photos")
    _create_test_images(root, {'': ['img1.jpg', 'img2.jpg', 'img3.jpg']})
    db = Database(str(tmp_path / "test.db"))
    status_events = []

    def fake_extract_metadata(paths, progress_callback=None, checkpoint=None):
        if checkpoint:
            checkpoint()
        if progress_callback:
            progress_callback(2, len(paths))
            progress_callback(len(paths), len(paths))
        if checkpoint:
            checkpoint()
        return {}

    def status_cb(message, **kwargs):
        status_events.append((message, kwargs))

    monkeypatch.setattr(scanner, "extract_metadata", fake_extract_metadata)

    scanner.scan(root, db, status_callback=status_cb)

    metadata_events = [
        event for event in status_events
        if event[1].get("phase_label") == "Extracting metadata"
    ]
    assert metadata_events[0][1]["phase_current"] == 0
    assert metadata_events[0][1]["phase_total"] == 3
    assert metadata_events[-1][1]["phase_current"] == 3
    assert metadata_events[-1][1]["phase_total"] == 3


def test_scan_non_recursive_only_finds_root_photos(tmp_path):
    """scan(recursive=False) only finds photos in the root folder, not subfolders."""
    from db import Database
    from scanner import scan

    root = str(tmp_path / "photos")
    _create_test_images(root, {
        '': ['root.jpg'],
        'sub': ['sub.jpg'],
        'sub/deep': ['deep.jpg'],
    })

    db = Database(str(tmp_path / "test.db"))
    scan(root, db, recursive=False)

    photos = db.get_photos(per_page=100)
    filenames = {p['filename'] for p in photos}
    assert filenames == {'root.jpg'}


def test_scan_non_recursive_skips_bundle_children_before_stat(tmp_path):
    """In ``scan(recursive=False)``, a normal root like ``~/Pictures``
    still contains ``Photos Library.photoslibrary`` (or a symlink to
    one) as a direct child. A bare ``iterdir() + is_file()`` would
    stat the bundle target while filtering by extension and re-trip
    the macOS "access data from other apps" TCC prompt this change
    exists to avoid. The non-recursive branch must drop excluded
    children before any followed stat.
    """
    if sys.platform == "win32":
        pytest.skip("POSIX symlinks required")
    from db import Database
    from scanner import scan

    bundle = tmp_path / "Photos Library.photoslibrary"
    _create_test_images(str(bundle), {'originals': ['managed.jpg']})

    root = str(tmp_path / "photos")
    _create_test_images(root, {'': ['real.jpg']})
    # Direct bundle child as a sibling of real.jpg.
    direct_bundle = os.path.join(root, "Photos Library.photoslibrary")
    _create_test_images(direct_bundle, {'originals': ['direct_managed.jpg']})
    # Symlinked bundle child.
    os.symlink(str(bundle), os.path.join(root, "LibraryAlias"))

    db = Database(str(tmp_path / "test.db"))
    scan(root, db, recursive=False)

    photos = db.get_photos(per_page=100)
    filenames = {p['filename'] for p in photos}
    assert filenames == {'real.jpg'}

    folder_paths = [f['path'] for f in db.get_folder_tree()]
    assert not any('.photoslibrary' in p for p in folder_paths)
    assert not any('LibraryAlias' in p for p in folder_paths)


def test_scan_non_recursive_does_not_stat_bundle_children(tmp_path, monkeypatch):
    """Belt-and-braces for the non-recursive branch: if any code path
    calls ``Path.is_file`` on a child that ``is_excluded_scan_path``
    covers, the test fails — that ``is_file`` follows symlinks and
    would re-trip the macOS TCC prompt before the extension filter
    could reject the entry.
    """
    if sys.platform == "win32":
        pytest.skip("POSIX symlinks required")
    from pathlib import Path

    from db import Database
    from scanner import scan

    real_is_file = Path.is_file

    def guarded_is_file(self):
        from image_loader import is_excluded_scan_path
        if is_excluded_scan_path(self):
            raise AssertionError(
                f"is_file() called on excluded path before guard: {self}"
            )
        return real_is_file(self)

    monkeypatch.setattr(Path, "is_file", guarded_is_file)

    bundle = tmp_path / "Photos Library.photoslibrary"
    _create_test_images(str(bundle), {'originals': ['managed.jpg']})

    root = str(tmp_path / "photos")
    _create_test_images(root, {'': ['real.jpg']})
    _create_test_images(
        os.path.join(root, "Photos Library.photoslibrary"),
        {'originals': ['direct_managed.jpg']},
    )
    os.symlink(str(bundle), os.path.join(root, "LibraryAlias"))

    db = Database(str(tmp_path / "test.db"))
    scan(root, db, recursive=False)

    photos = db.get_photos(per_page=100)
    filenames = {p['filename'] for p in photos}
    assert filenames == {'real.jpg'}


def test_scan_reads_dimensions(tmp_path):
    """scan() reads image dimensions."""
    from db import Database
    from scanner import scan

    root = str(tmp_path / "photos")
    os.makedirs(root)
    img = Image.new('RGB', (640, 480), color='blue')
    img.save(os.path.join(root, 'test.jpg'))

    db = Database(str(tmp_path / "test.db"))
    scan(root, db)

    photos = db.get_photos()
    assert photos[0]['width'] == 640
    assert photos[0]['height'] == 480


def test_scan_records_file_mtime(tmp_path):
    """scan() records file modification time."""
    from db import Database
    from scanner import scan

    root = str(tmp_path / "photos")
    os.makedirs(root)
    img_path = os.path.join(root, 'test.jpg')
    Image.new('RGB', (100, 100)).save(img_path)

    db = Database(str(tmp_path / "test.db"))
    scan(root, db)

    photos = db.get_photos()
    assert photos[0]['file_mtime'] is not None
    assert photos[0]['file_mtime'] > 0


def test_scan_progress_callback(tmp_path):
    """scan() calls progress callback with (current, total)."""
    from db import Database
    from scanner import scan

    root = str(tmp_path / "photos")
    _create_test_images(root, {'': ['a.jpg', 'b.jpg', 'c.jpg']})

    db = Database(str(tmp_path / "test.db"))
    progress = []
    scan(root, db, progress_callback=lambda cur, tot: progress.append((cur, tot)))

    assert len(progress) == 4
    assert progress[0] == (0, 3)   # initial discovery report
    assert progress[-1] == (3, 3)


def test_scan_ignores_non_image_files(tmp_path):
    """scan() skips files that aren't images."""
    from db import Database
    from scanner import scan

    root = str(tmp_path / "photos")
    os.makedirs(root)
    Image.new('RGB', (100, 100)).save(os.path.join(root, 'photo.jpg'))
    with open(os.path.join(root, 'notes.txt'), 'w') as f:
        f.write('not an image')
    with open(os.path.join(root, '.hidden.jpg'), 'w') as f:
        f.write('hidden')

    db = Database(str(tmp_path / "test.db"))
    scan(root, db)

    photos = db.get_photos()
    assert len(photos) == 1
    assert photos[0]['filename'] == 'photo.jpg'


def test_scan_updates_folder_counts(tmp_path):
    """scan() updates photo_count on folders."""
    from db import Database
    from scanner import scan

    root = str(tmp_path / "photos")
    _create_test_images(root, {
        '': ['a.jpg', 'b.jpg'],
        'sub': ['c.jpg'],
    })

    db = Database(str(tmp_path / "test.db"))
    scan(root, db)

    folders = db.get_folder_tree()
    root_folder = [f for f in folders if f['path'] == root][0]
    sub_folder = [f for f in folders if f['name'] == 'sub'][0]
    assert root_folder['photo_count'] == 2
    assert sub_folder['photo_count'] == 1


def test_scan_imports_xmp_keywords(tmp_path):
    """scan() reads XMP sidecars and imports keywords into the database."""
    from db import Database
    from scanner import scan
    from xmp import write_sidecar

    root = str(tmp_path / "photos")
    os.makedirs(root)
    Image.new('RGB', (100, 100)).save(os.path.join(root, 'bird.jpg'))

    # Create XMP sidecar with keywords
    write_sidecar(
        os.path.join(root, 'bird.xmp'),
        flat_keywords={'Northern cardinal', 'Birds'},
        hierarchical_keywords={'Birds|Northern cardinal'},
    )

    db = Database(str(tmp_path / "test.db"))
    scan(root, db)

    photos = db.get_photos()
    assert len(photos) == 1
    keywords = db.get_photo_keywords(photos[0]['id'])
    kw_names = {k['name'] for k in keywords}
    assert 'Northern cardinal' in kw_names
    assert 'Birds' in kw_names


def test_scan_skips_empty_normalized_keywords(tmp_path):
    """A sidecar with a lone quote must not abort the scan.

    add_keyword() now raises ValueError when a name normalizes to `""`
    (e.g. `"'"` or a bare smart quote). Without a caller-side filter, the
    unhandled ValueError inside _import_keywords_for_photo would propagate
    and take down the whole scanner run for one malformed <rdf:li>.
    """
    from db import Database
    from scanner import scan
    from xmp import write_sidecar

    root = str(tmp_path / "photos")
    os.makedirs(root)
    Image.new('RGB', (100, 100)).save(os.path.join(root, 'bird.jpg'))

    write_sidecar(
        os.path.join(root, 'bird.xmp'),
        flat_keywords={"'", "Northern cardinal"},
        hierarchical_keywords={"Birds|'|Northern cardinal", "Birds|Raptors"},
    )

    db = Database(str(tmp_path / "test.db"))
    scan(root, db)

    photos = db.get_photos()
    assert len(photos) == 1
    kw_names = {k['name'] for k in db.get_photo_keywords(photos[0]['id'])}
    # The valid flat keyword lands; the lone-quote entry is skipped without
    # raising. The empty-segment hierarchy is dropped entirely (chain
    # broken), but the well-formed sibling still lands as a hierarchy.
    assert 'Northern cardinal' in kw_names
    tree_names = {k['name'] for k in db.get_keyword_tree()}
    assert 'Raptors' in tree_names
    assert 'Birds' in tree_names


def test_scan_imports_hierarchical_keywords(tmp_path):
    """scan() creates keyword hierarchy from lr:hierarchicalSubject."""
    from db import Database
    from scanner import scan
    from xmp import write_sidecar

    root = str(tmp_path / "photos")
    os.makedirs(root)
    Image.new('RGB', (100, 100)).save(os.path.join(root, 'bird.jpg'))

    write_sidecar(
        os.path.join(root, 'bird.xmp'),
        flat_keywords={'Black kite'},
        hierarchical_keywords={'Birds|Raptors|Black kite'},
    )

    db = Database(str(tmp_path / "test.db"))
    scan(root, db)

    tree = db.get_keyword_tree()
    names = {k['name'] for k in tree}
    assert 'Birds' in names
    assert 'Raptors' in names
    assert 'Black kite' in names

    # Verify hierarchy: Raptors parent is Birds
    raptors = [k for k in tree if k['name'] == 'Raptors'][0]
    birds = [k for k in tree if k['name'] == 'Birds'][0]
    assert raptors['parent_id'] == birds['id']


def test_incremental_scan_skips_unchanged(tmp_path):
    """Incremental scan skips files that haven't changed since last scan."""
    from db import Database
    from scanner import scan

    root = str(tmp_path / "photos")
    os.makedirs(root)
    Image.new('RGB', (100, 100)).save(os.path.join(root, 'old.jpg'))

    db = Database(str(tmp_path / "test.db"))
    scan(root, db)

    # Add a new file
    time.sleep(0.05)
    Image.new('RGB', (200, 200)).save(os.path.join(root, 'new.jpg'))

    # Track what gets processed
    processed = []
    scan(root, db, incremental=True,
         progress_callback=lambda cur, tot: processed.append(cur))

    photos = db.get_photos(per_page=100)
    filenames = {p['filename'] for p in photos}
    assert 'old.jpg' in filenames
    assert 'new.jpg' in filenames


def test_incremental_scan_converges_after_file_change(tmp_path):
    """A changed file is re-processed once, then skipped on later scans.

    add_photo is INSERT OR IGNORE, so the scan loop must explicitly persist
    the fresh file_mtime/file_size onto existing rows — without that, the
    pre-pass compares against the stale stored mtime and re-hashes the file
    on every incremental scan forever.
    """
    from db import Database
    from scanner import scan

    root = str(tmp_path / "photos")
    os.makedirs(root)
    path = os.path.join(root, 'bird.jpg')
    Image.new('RGB', (100, 100)).save(path)

    db = Database(str(tmp_path / "test.db"))
    scan(root, db)

    # Modify the file's content (and mtime)
    time.sleep(0.05)
    Image.new('RGB', (150, 150)).save(path)

    scan(root, db, incremental=True)
    photo = db.get_photos()[0]
    assert photo['width'] == 150  # re-processed
    assert photo['file_mtime'] == os.stat(path).st_mtime  # fresh mtime stored

    # Simulate ExifTool having run (it isn't installed in CI) — without the
    # exif_data marker the metadata_missing retry path re-processes the file
    # regardless of mtime, masking what this test asserts.
    db.conn.execute("UPDATE photos SET exif_data = '{}' WHERE exif_data IS NULL")
    db.conn.commit()

    # Next incremental scan must skip the file entirely — metadata
    # extraction / hashing phases only announce themselves when there is
    # at least one file to process.
    statuses = []
    scan(root, db, incremental=True, status_callback=statuses.append)
    assert not any(
        s.startswith('Extracting metadata') or s.startswith('Hashing')
        for s in statuses
    )


def test_incremental_scan_retries_after_hash_failure(tmp_path, monkeypatch):
    """A changed file whose content hash fails is retried on the next scan.

    When _compute_file_features can't read the file (transient permission /
    I-O error → file_hash None), the scan loop must NOT advance the stored
    file_mtime — otherwise the next incremental scan sees the mtime as
    current and skips the file forever with a stale hash and stale derived
    caches.
    """
    import scanner
    from db import Database
    from scanner import scan

    root = str(tmp_path / "photos")
    os.makedirs(root)
    path = os.path.join(root, 'bird.jpg')
    Image.new('RGB', (100, 100)).save(path)

    db = Database(str(tmp_path / "test.db"))
    scan(root, db)

    def _row():
        return db.conn.execute(
            "SELECT file_hash, file_mtime FROM photos"
        ).fetchone()

    old_row = _row()
    assert old_row['file_hash'] is not None

    # Simulate ExifTool having run (it isn't installed in CI) so the
    # metadata_missing retry path doesn't mask the mtime-based check.
    db.conn.execute("UPDATE photos SET exif_data = '{}' WHERE exif_data IS NULL")
    db.conn.commit()

    # Modify the file's content (and mtime)
    time.sleep(0.05)
    Image.new('RGB', (150, 150)).save(path)

    # Scan with feature computation failing (simulated unreadable file)
    monkeypatch.setattr(scanner, '_compute_file_features', lambda p: (None, None))
    scan(root, db, incremental=True)
    photo = _row()
    assert photo['file_hash'] == old_row['file_hash']  # hash untouched
    # Stored mtime must remain stale so the file is retried next scan
    assert photo['file_mtime'] == old_row['file_mtime']
    assert photo['file_mtime'] != os.stat(path).st_mtime

    # With hashing working again, the next incremental scan picks it up
    monkeypatch.undo()
    scan(root, db, incremental=True)
    photo = _row()
    assert photo['file_hash'] != old_row['file_hash']
    assert photo['file_mtime'] == os.stat(path).st_mtime


def test_incremental_scan_forgets_deleted_xmp(tmp_path):
    """Deleting an XMP sidecar clears the stored xmp_mtime after one
    re-process, so later scans don't re-trip the "XMP changed" check."""
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
    assert db.get_photos()[0]['xmp_mtime'] is not None

    # Simulate ExifTool having run (it isn't installed in CI) so the
    # metadata_missing retry path doesn't force re-processing.
    db.conn.execute("UPDATE photos SET exif_data = '{}' WHERE exif_data IS NULL")
    db.conn.commit()

    os.remove(os.path.join(root, 'bird.xmp'))
    scan(root, db, incremental=True)
    assert db.get_photos()[0]['xmp_mtime'] is None

    statuses = []
    scan(root, db, incremental=True, status_callback=statuses.append)
    assert not any(
        s.startswith('Extracting metadata') or s.startswith('Hashing')
        for s in statuses
    )


def test_scan_survives_file_vanishing_mid_scan(tmp_path):
    """A file deleted between discovery and processing is skipped — it must
    not abort the scan and flag every folder in scope 'partial'."""
    from db import Database
    from scanner import scan

    root = str(tmp_path / "photos")
    os.makedirs(root)
    Image.new('RGB', (100, 100)).save(os.path.join(root, 'a.jpg'))
    Image.new('RGB', (100, 100)).save(os.path.join(root, 'b.jpg'))

    db = Database(str(tmp_path / "test.db"))

    # On the first processed photo, delete the other file — it has been
    # discovered but not yet processed.
    deleted = []
    def cb(photo_id, path_str):
        if deleted:
            return
        other = 'b.jpg' if path_str.endswith('a.jpg') else 'a.jpg'
        os.remove(os.path.join(root, other))
        deleted.append(other)

    scan(root, db, photo_callback=cb)

    photos = db.get_photos(per_page=100)
    assert len(photos) == 1  # only the survivor was ingested
    status = db.conn.execute(
        "SELECT status FROM folders WHERE path = ?", (root,)
    ).fetchone()["status"]
    assert status == "ok"


def test_incremental_scan_detects_xmp_changes(tmp_path):
    """Incremental scan re-reads XMP when xmp_mtime changes."""
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

    # Verify initial keyword
    photos = db.get_photos()
    kws = db.get_photo_keywords(photos[0]['id'])
    assert {k['name'] for k in kws} == {'Sparrow'}

    # Modify XMP - add a keyword
    time.sleep(0.05)
    write_sidecar(
        os.path.join(root, 'bird.xmp'),
        flat_keywords={'Cardinal'},
        hierarchical_keywords=set(),
    )

    scan(root, db, incremental=True)

    # Should now have both keywords (merge from XMP)
    kws = db.get_photo_keywords(photos[0]['id'])
    kw_names = {k['name'] for k in kws}
    assert 'Sparrow' in kw_names
    assert 'Cardinal' in kw_names


@requires_exiftool
def test_scan_populates_exif_data(tmp_path):
    """scan() populates the exif_data JSON column when extract_full_metadata is on."""
    from db import Database
    from scanner import scan

    root = str(tmp_path / "photos")
    os.makedirs(root)
    img = Image.new('RGB', (640, 480), color='blue')
    img.save(os.path.join(root, 'test.jpg'))

    db = Database(str(tmp_path / "test.db"))
    scan(root, db)

    row = db.conn.execute("SELECT exif_data FROM photos LIMIT 1").fetchone()
    assert row["exif_data"] is not None
    meta = json.loads(row["exif_data"])
    assert isinstance(meta, dict)
    # Should have at least a File group
    assert "File" in meta


def test_rescan_clears_absent_exif_summary_columns(tmp_path, monkeypatch):
    """Promoted EXIF columns (camera_make / camera_model / lens /
    focal_length / aperture / shutter_speed / iso) are derived from the
    current file metadata, so a rescan whose metadata omits a field must
    clear that column to NULL. Otherwise a replaced or edited file leaves
    stale values that /api/photos/query and /api/filters/values keep
    matching. focal_length is included because it's exposed as a
    filter/typeahead field just like the other camera-exposure columns."""
    import scanner
    from db import Database
    from scanner import scan

    root = tmp_path / "photos"
    root.mkdir()
    photo = root / "test.jpg"
    Image.new("RGB", (100, 100), color="red").save(str(photo))

    def make_extract(payload):
        def fake(paths, restricted_tags=None, progress_callback=None,
                checkpoint=None):
            return {p: payload for p in paths}
        return fake

    # First scan: file reports full EXIF summary.
    monkeypatch.setattr(scanner, "extract_metadata", make_extract({
        "EXIF": {
            "Make": "Sony", "Model": "ILCE-1", "LensModel": "FE 200-600",
            "FocalLength": 450.0,
            "FNumber": 6.3, "ExposureTime": 0.001, "ISO": 800,
        },
        "Composite": {},
    }))
    db = Database(str(tmp_path / "test.db"))
    scan(str(root), db)
    row = db.conn.execute(
        "SELECT camera_make, camera_model, lens, focal_length, aperture, "
        "shutter_speed, iso FROM photos LIMIT 1"
    ).fetchone()
    assert row["camera_make"] == "Sony"
    assert row["camera_model"] == "ILCE-1"
    assert row["lens"] == "FE 200-600"
    assert row["focal_length"] == pytest.approx(450.0)
    assert row["aperture"] == pytest.approx(6.3)
    assert row["iso"] == 800

    # Bump mtime so the incremental scan re-examines the file, then rescan
    # with metadata that omits every promoted field. All promoted columns
    # must go to NULL — not stay at their prior values.
    future = time.time() + 3600
    os.utime(str(photo), (future, future))
    monkeypatch.setattr(scanner, "extract_metadata", make_extract({
        "EXIF": {},
        "Composite": {},
        "File": {"ImageWidth": 100, "ImageHeight": 100},
    }))
    scan(str(root), db)
    row = db.conn.execute(
        "SELECT camera_make, camera_model, lens, focal_length, aperture, "
        "shutter_speed, iso FROM photos LIMIT 1"
    ).fetchone()
    assert row["camera_make"] is None
    assert row["camera_model"] is None
    assert row["lens"] is None
    assert row["focal_length"] is None
    assert row["aperture"] is None
    assert row["shutter_speed"] is None
    assert row["iso"] is None


def test_incremental_rescan_populates_phase1_summary_for_partial_marker(tmp_path, monkeypatch):
    """Rows scanned before Phase 1 with only the ``exif_data='{}'`` marker
    (extract_full_metadata=False path) get promoted-column values on the
    next incremental scan. The DB migration clears '{}' to NULL, and the
    scanner's pre-pass then re-flags the row as ``metadata_missing`` via
    ``summary_needs_extract`` — otherwise the row's timestamp is populated
    and the standard skip triggers, leaving camera_make etc. permanently
    NULL on upgraded libraries."""
    import scanner
    from db import Database
    from scanner import scan

    root = tmp_path / "photos"
    root.mkdir()
    photo = root / "test.jpg"
    Image.new("RGB", (100, 100), color="red").save(str(photo))

    def make_extract(payload):
        def fake(paths, restricted_tags=None, progress_callback=None,
                checkpoint=None):
            return {p: payload for p in paths}
        return fake

    # Seed the row as if a pre-Phase-1 scan with extract_full_metadata=False
    # had run: exif_data='{}', promoted cols NULL, timestamp populated.
    monkeypatch.setattr(scanner, "extract_metadata", make_extract({
        "EXIF": {
            "DateTimeOriginal": "2024:01:15 10:30:00",
            "Make": "Sony", "Model": "ILCE-1",
        },
        "Composite": {},
    }))
    db = Database(str(tmp_path / "test.db"))
    scan(str(root), db)
    # Simulate the pre-Phase-1 storage shape: keep the timestamp, clear
    # the promoted cols, and put the '{}' marker back on exif_data.
    db.conn.execute(
        "UPDATE photos SET exif_data='{}', camera_make=NULL, camera_model=NULL, "
        "lens=NULL, aperture=NULL, shutter_speed=NULL, iso=NULL")
    # Reset the migration marker and re-open so the migration reruns and
    # clears the '{}' marker to NULL (Phase-1 backfill behavior).
    db.conn.execute("DELETE FROM db_meta WHERE key='exif_summary_backfill_v1'")
    db.conn.commit()
    db.close()
    db = Database(str(tmp_path / "test.db"))
    row = db.conn.execute(
        "SELECT exif_data, camera_make FROM photos LIMIT 1").fetchone()
    assert row["exif_data"] is None  # cleared by migration
    assert row["camera_make"] is None

    # Incremental scan with the file unchanged (same mtime, same content).
    # Without the summary_needs_extract trigger, the pre-pass would skip
    # this row because timestamp is populated. With it, the row is re-
    # extracted and camera_make gets filled from the re-run metadata.
    monkeypatch.setattr(scanner, "extract_metadata", make_extract({
        "EXIF": {
            "DateTimeOriginal": "2024:01:15 10:30:00",
            "Make": "Sony", "Model": "ILCE-1",
        },
        "Composite": {},
    }))
    scan(str(root), db)
    row = db.conn.execute(
        "SELECT camera_make, camera_model FROM photos LIMIT 1").fetchone()
    assert row["camera_make"] == "Sony"
    assert row["camera_model"] == "ILCE-1"


def test_scan_pairs_raw_and_jpeg(tmp_path):
    """When a folder has IMG.cr3 and IMG.jpg, they become one photo with companion_path."""
    from db import Database
    from scanner import scan

    img_dir = tmp_path / "photos"
    img_dir.mkdir()

    # Create a JPEG
    Image.new("RGB", (200, 100), color="green").save(str(img_dir / "IMG_001.jpg"))
    # Create a fake raw file with the same base name
    with open(str(img_dir / "IMG_001.cr3"), "wb") as f:
        f.write(b"\x00" * 200)

    db = Database(str(tmp_path / "test.db"))
    scan(str(img_dir), db)

    photos = db.conn.execute("SELECT filename, companion_path FROM photos").fetchall()
    # Should be one photo record, not two
    assert len(photos) == 1

    photo = photos[0]
    # Raw is primary, JPEG is companion
    assert photo["filename"] == "IMG_001.cr3"
    assert photo["companion_path"] == "IMG_001.jpg"


def test_rescan_changed_companion_invalidates_jpeg_thumbnail_variant(tmp_path):
    """Re-pairing a changed companion drops its source-specific thumbnail
    even when the replacement preserves filesystem mtime."""
    from db import Database
    from scanner import scan

    img_dir = tmp_path / "photos"
    img_dir.mkdir()
    jpeg_path = img_dir / "IMG_001.jpg"
    Image.new("RGB", (200, 100), color="green").save(jpeg_path)
    (img_dir / "IMG_001.cr3").write_bytes(b"\x00" * 200)

    vireo_dir = tmp_path / "vireo"
    thumb_dir = vireo_dir / "thumbnails"
    thumb_dir.mkdir(parents=True)
    db = Database(str(vireo_dir / "test.db"))
    scan(
        str(img_dir), db, vireo_dir=str(vireo_dir),
        thumb_cache_dir=str(thumb_dir),
    )
    primary = db.conn.execute(
        "SELECT id FROM photos WHERE filename='IMG_001.cr3'",
    ).fetchone()
    variant = thumb_dir / f"{primary['id']}_jpeg.jpg"
    variant.write_bytes(b"stale companion pixels")

    original_mtime = jpeg_path.stat().st_mtime
    Image.new("RGB", (200, 100), color="blue").save(jpeg_path)
    os.utime(jpeg_path, (original_mtime, original_mtime))
    scan(
        str(img_dir), db, vireo_dir=str(vireo_dir),
        thumb_cache_dir=str(thumb_dir),
    )

    assert not variant.exists()


def test_scan_late_arriving_raw_pairs_with_existing_jpeg(tmp_path):
    """Importing raws after JPEGs matches them to existing photo records."""
    import os
    import sys
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
    from db import Database
    from scanner import scan

    img_dir = tmp_path / "photos"
    img_dir.mkdir()

    from PIL import Image

    # First scan: JPEG only
    Image.new("RGB", (200, 100), color="green").save(str(img_dir / "IMG_001.jpg"))
    db = Database(str(tmp_path / "test.db"))
    scan(str(img_dir), db)

    photos_before = db.conn.execute("SELECT id FROM photos").fetchall()
    assert len(photos_before) == 1

    # Add metadata to the JPEG record (simulating user edits before raw arrives)
    jpeg_id = photos_before[0]["id"]
    db.conn.execute(
        "UPDATE photos SET rating = 4, flag = 'flagged', timestamp = '2024-06-15T10:30:00' WHERE id = ?",
        (jpeg_id,),
    )
    # Add a keyword to the JPEG
    kw_id = db.add_keyword("Robin")
    db.tag_photo(jpeg_id, kw_id)
    db.conn.commit()

    # Now add the raw file and rescan
    with open(str(img_dir / "IMG_001.cr3"), "wb") as f:
        f.write(b"\x00" * 200)
    scan(str(img_dir), db)

    photos_after = db.conn.execute(
        "SELECT id, filename, companion_path, rating, flag, timestamp FROM photos"
    ).fetchall()
    # Still one photo — raw becomes primary, JPEG becomes companion
    assert len(photos_after) == 1
    assert photos_after[0]["filename"] == "IMG_001.cr3"
    assert photos_after[0]["companion_path"] == "IMG_001.jpg"

    # Metadata should have been transferred from the JPEG record
    assert photos_after[0]["rating"] == 4
    assert photos_after[0]["flag"] == "flagged"
    assert photos_after[0]["timestamp"] == "2024-06-15T10:30:00"

    # Keywords should have been transferred
    raw_id = photos_after[0]["id"]
    keywords = db.get_photo_keywords(raw_id)
    kw_names = {k["name"] for k in keywords}
    assert "Robin" in kw_names


def test_pairing_transfers_edit_recipe_from_companion(tmp_path):
    """Pairing raw+JPEG preserves a recipe stored on the deleted companion."""
    from db import Database
    from image_edits import recipe_to_json
    from scanner import _pair_raw_jpeg_companions

    img_dir = tmp_path / "photos"
    img_dir.mkdir()

    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder(str(img_dir), name="photos")
    jpeg_id = db.add_photo(
        folder_id=fid, filename="IMG_001.jpg", extension=".jpg",
        file_size=1000, file_mtime=1.0,
    )
    raw_id = db.add_photo(
        folder_id=fid, filename="IMG_001.cr3", extension=".cr3",
        file_size=2000, file_mtime=1.0,
    )
    db.set_photo_edit_recipe(jpeg_id, {"rotation": 90})
    recipe_json = recipe_to_json({"rotation": 90}) or ""
    db.record_edit(
        "edit_recipe",
        "Updated photo edit recipe",
        recipe_json,
        [{"photo_id": jpeg_id, "old_value": "", "new_value": recipe_json}],
    )
    db.conn.execute(
        "UPDATE photos SET thumb_path = ? WHERE id = ?",
        ("thumbnails/raw.jpg", raw_id),
    )
    db.preview_cache_insert(raw_id, 800, 1234)

    _pair_raw_jpeg_companions(db)

    photo = db.conn.execute(
        "SELECT id, filename, thumb_path FROM photos",
    ).fetchone()
    assert photo["filename"] == "IMG_001.cr3"
    assert db.get_photo_edit_recipe(photo["id"]) == {
        "rotation": 90,
        "version": 1,
    }
    assert photo["thumb_path"] is None
    assert db.preview_cache_get(photo["id"], 800) is None
    history_item = db.conn.execute(
        "SELECT photo_id FROM edit_history_items",
    ).fetchone()
    assert history_item["photo_id"] == photo["id"]

    undone = db.undo_last_edit()
    assert undone is not None
    assert db.get_photo_edit_recipe(photo["id"]) is None


def test_pairing_invalidates_existing_raw_display_cache(tmp_path):
    """A newly paired camera JPEG must replace a pre-pairing RAW rendition."""
    from db import Database
    from scanner import _pair_raw_jpeg_companions

    img_dir = tmp_path / "photos"
    img_dir.mkdir()
    db = Database(str(tmp_path / "test.db"))
    folder_id = db.add_folder(str(img_dir), name="photos")
    raw_id = db.add_photo(
        folder_id=folder_id, filename="IMG_002.cr3", extension=".cr3",
        file_size=2000, file_mtime=1.0,
    )
    db.add_photo(
        folder_id=folder_id, filename="IMG_002.jpg", extension=".jpg",
        file_size=1000, file_mtime=1.0,
    )

    originals_dir = tmp_path / "originals"
    originals_dir.mkdir()
    display_cache = originals_dir / f"{raw_id}.display.jpg"
    display_cache.write_bytes(b"pre-pairing RAW display")

    _pair_raw_jpeg_companions(db, vireo_dir=str(tmp_path))

    photo = db.get_photo(raw_id)
    assert photo["companion_path"] == "IMG_002.jpg"
    assert not display_cache.exists()


def test_pairing_transfers_local_mask_snapshot_files(tmp_path):
    """Pairing raw+JPEG must move edit-mask snapshot files to the primary id.

    Snapshot lookup uses ``<photo_id>.<ref>.png``. Without renaming the
    files when the recipe row's photo_id changes, ``load_snapshot`` misses
    the file and every render silently disables the local pass.
    """
    from db import Database
    from local_masks import edit_masks_dir, snapshot_path
    from scanner import _pair_raw_jpeg_companions

    img_dir = tmp_path / "photos"
    img_dir.mkdir()

    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder(str(img_dir), name="photos")
    jpeg_id = db.add_photo(
        folder_id=fid, filename="IMG_002.jpg", extension=".jpg",
        file_size=1000, file_mtime=1.0,
    )
    raw_id = db.add_photo(
        folder_id=fid, filename="IMG_002.cr3", extension=".cr3",
        file_size=2000, file_mtime=1.0,
    )

    # Set a recipe on the JPEG (any recipe — this test is about the file
    # rename, not recipe schema). The pairing code moves the row to the RAW.
    db.set_photo_edit_recipe(jpeg_id, {"rotation": 90})

    # Drop a snapshot file at <jpeg_id>.<ref>.png as if we'd called the
    # snapshot endpoint on the JPEG before pairing.
    ref = "abcdef012345"
    os.makedirs(edit_masks_dir(str(tmp_path)), exist_ok=True)
    src_snap = snapshot_path(str(tmp_path), jpeg_id, ref)
    with open(src_snap, "wb") as f:
        f.write(b"snapshot-bytes")
    # Also leave a decoy file for a different photo_id so the transfer
    # doesn't over-match.
    decoy = os.path.join(edit_masks_dir(str(tmp_path)), f"99999.{ref}.png")
    with open(decoy, "wb") as f:
        f.write(b"decoy-bytes")

    _pair_raw_jpeg_companions(db, vireo_dir=str(tmp_path))

    photo = db.conn.execute(
        "SELECT id, filename FROM photos"
    ).fetchone()
    assert photo["filename"] == "IMG_002.cr3"
    primary_id = photo["id"]
    assert primary_id == raw_id

    # The companion's snapshot file must have moved to the primary id.
    assert not os.path.exists(src_snap)
    assert os.path.exists(snapshot_path(str(tmp_path), primary_id, ref))
    # Decoy for an unrelated photo id must not be touched.
    assert os.path.exists(decoy)


def test_pairing_does_not_copy_rejected_flag_to_raw(tmp_path):
    """A companion JPEG's 'rejected' flag (e.g. set by the duplicate
    auto-resolver when the JPEG has a byte-identical twin) must not be
    stamped onto the unique RAW primary during pairing."""
    from db import Database
    from scanner import scan

    img_dir = tmp_path / "photos"
    img_dir.mkdir()

    Image.new("RGB", (200, 100), color="green").save(str(img_dir / "IMG_001.jpg"))
    db = Database(str(tmp_path / "test.db"))
    scan(str(img_dir), db)

    jpeg_id = db.conn.execute("SELECT id FROM photos").fetchone()["id"]
    db.conn.execute("UPDATE photos SET flag = 'rejected' WHERE id = ?", (jpeg_id,))
    db.conn.commit()

    with open(str(img_dir / "IMG_001.cr3"), "wb") as f:
        f.write(b"\x00" * 200)
    scan(str(img_dir), db)

    photos = db.conn.execute(
        "SELECT filename, companion_path, flag FROM photos"
    ).fetchall()
    assert len(photos) == 1
    assert photos[0]["filename"] == "IMG_001.cr3"
    assert photos[0]["flag"] == "none"


def test_pairing_merges_predictions_without_unique_violation(tmp_path):
    """Pairing raw+JPEG deduplicates predictions that would violate UNIQUE(photo_id, model, workspace_id)."""
    from db import Database
    from scanner import scan

    img_dir = tmp_path / "photos"
    img_dir.mkdir()

    # First scan: JPEG only
    Image.new("RGB", (200, 100), color="green").save(str(img_dir / "IMG_001.jpg"))
    db = Database(str(tmp_path / "test.db"))
    scan(str(img_dir), db)

    jpeg_id = db.conn.execute("SELECT id FROM photos").fetchone()["id"]

    # Classify the JPEG — create detection then add a prediction
    det_ids = db.save_detections(jpeg_id, [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.4}, "confidence": 0.9, "category": "animal"}
    ], detector_model="MDV6")
    db.add_prediction(det_ids[0], "Robin", 0.85, "bioclip")

    # Now add the raw file and rescan — this creates a new photo record for the raw,
    # then the classify job also runs on the raw (simulated here)
    with open(str(img_dir / "IMG_001.cr3"), "wb") as f:
        f.write(b"\x00" * 200)
    scan(str(img_dir), db)

    # At this point, the raw should have picked up the JPEG's prediction.
    # There were two records (raw + jpeg), both classified with same model/workspace,
    # and pairing merged them without IntegrityError.
    photos = db.conn.execute("SELECT id, filename FROM photos").fetchall()
    assert len(photos) == 1
    assert photos[0]["filename"] == "IMG_001.cr3"

    raw_id = photos[0]["id"]
    preds = db.conn.execute(
        """SELECT pr.species, pr.confidence FROM predictions pr
           JOIN detections d ON d.id = pr.detection_id
           WHERE d.photo_id = ?""",
        (raw_id,),
    ).fetchall()
    assert len(preds) == 1
    assert preds[0]["species"] == "Robin"


def test_pairing_collapses_duplicate_detections_across_raw_jpeg(tmp_path):
    """When raw and JPEG already have identical detections (same model + box +
    category) from prior detector runs, pairing collapses them into one row.

    With content-addressed detection IDs, an identical (model, box, category)
    on the *same* primary photo collapses to one detection — the companion's
    moved detection re-hashes to the primary's existing id, the UPSERT
    no-ops, and the duplicate prediction is dropped by the UNIQUE
    (detection_id, classifier_model) constraint. This is the correct
    semantics: RAW + JPEG of the same scene with the same detector output
    is logically one detection, not two.
    """
    from db import Database

    img_dir = tmp_path / "photos"
    img_dir.mkdir()

    # Create both files
    Image.new("RGB", (200, 100), color="green").save(str(img_dir / "IMG_001.jpg"))
    with open(str(img_dir / "IMG_001.cr3"), "wb") as f:
        f.write(b"\x00" * 200)

    db = Database(str(tmp_path / "test.db"))
    from scanner import _pair_raw_jpeg_companions

    fid = db.add_folder(str(img_dir), name="photos")
    jpeg_id = db.add_photo(folder_id=fid, filename="IMG_001.jpg", extension=".jpg",
                           file_size=1000, file_mtime=1.0)
    raw_id = db.add_photo(folder_id=fid, filename="IMG_001.cr3", extension=".cr3",
                          file_size=2000, file_mtime=1.0)

    # Both classified with same model + same box + same category.
    jpeg_det = db.save_detections(jpeg_id, [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.4}, "confidence": 0.9, "category": "animal"}
    ], detector_model="MDV6")
    raw_det = db.save_detections(raw_id, [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.4}, "confidence": 0.9, "category": "animal"}
    ], detector_model="MDV6")
    db.add_prediction(jpeg_det[0], "Robin", 0.95, "bioclip")
    db.add_prediction(raw_det[0], "Robin", 0.70, "bioclip")

    # Run pairing — should NOT raise IntegrityError
    _pair_raw_jpeg_companions(db)

    photos = db.conn.execute("SELECT id, filename, companion_path FROM photos").fetchall()
    assert len(photos) == 1
    assert photos[0]["filename"] == "IMG_001.cr3"

    # Detections collapse to one: same primary, same (model, box, category)
    # hashes to a single id.
    dets = db.conn.execute(
        "SELECT id FROM detections WHERE photo_id = ?", (photos[0]["id"],),
    ).fetchall()
    assert len(dets) == 1, "duplicate detections must collapse to one row"

    preds = db.conn.execute(
        """SELECT pr.species, pr.confidence FROM predictions pr
           JOIN detections d ON d.id = pr.detection_id
           WHERE d.photo_id = ?""",
        (photos[0]["id"],),
    ).fetchall()
    # UNIQUE(detection_id, classifier_model) means only one Robin@bioclip
    # prediction can survive on the collapsed detection. We don't pin which
    # confidence wins — UPDATE OR IGNORE keeps the primary's existing row.
    assert len(preds) == 1
    assert preds[0]["species"] == "Robin"


def test_pairing_transfers_inat_submissions(tmp_path):
    """Pairing raw+JPEG transfers iNat submissions from companion to primary."""
    from db import Database
    from scanner import _pair_raw_jpeg_companions

    img_dir = tmp_path / "photos"
    img_dir.mkdir()

    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder(str(img_dir), name="photos")
    jpeg_id = db.add_photo(folder_id=fid, filename="IMG_001.jpg", extension=".jpg",
                           file_size=1000, file_mtime=1.0)
    raw_id = db.add_photo(folder_id=fid, filename="IMG_001.cr3", extension=".cr3",
                          file_size=2000, file_mtime=1.0)

    # JPEG was submitted to iNaturalist
    db.record_inat_submission(jpeg_id, observation_id=12345,
                              observation_url="https://inaturalist.org/observations/12345")

    # Verify submission exists
    subs_before = db.get_inat_submissions([jpeg_id])
    assert jpeg_id in subs_before

    # Run pairing
    _pair_raw_jpeg_companions(db)

    photos = db.conn.execute("SELECT id, filename FROM photos").fetchall()
    assert len(photos) == 1
    assert photos[0]["filename"] == "IMG_001.cr3"

    # Submission should be on the raw (primary) now, not lost
    raw_id_after = photos[0]["id"]
    subs_after = db.get_inat_submissions([raw_id_after])
    assert raw_id_after in subs_after
    assert subs_after[raw_id_after]["observation_id"] == 12345


def test_pairing_deduplicates_inat_submissions(tmp_path):
    """When both raw and JPEG have iNat submissions for the same observation, pairing doesn't crash."""
    from db import Database
    from scanner import _pair_raw_jpeg_companions

    img_dir = tmp_path / "photos"
    img_dir.mkdir()

    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder(str(img_dir), name="photos")
    jpeg_id = db.add_photo(folder_id=fid, filename="IMG_001.jpg", extension=".jpg",
                           file_size=1000, file_mtime=1.0)
    raw_id = db.add_photo(folder_id=fid, filename="IMG_001.cr3", extension=".cr3",
                          file_size=2000, file_mtime=1.0)

    # Both photos submitted for the same observation (e.g., user submitted JPEG,
    # then raw was auto-submitted via a script)
    db.record_inat_submission(jpeg_id, observation_id=12345,
                              observation_url="https://inaturalist.org/observations/12345")
    db.record_inat_submission(raw_id, observation_id=12345,
                              observation_url="https://inaturalist.org/observations/12345")
    # JPEG also has a different observation
    db.record_inat_submission(jpeg_id, observation_id=67890,
                              observation_url="https://inaturalist.org/observations/67890")

    # Should NOT raise IntegrityError
    _pair_raw_jpeg_companions(db)

    photos = db.conn.execute("SELECT id, filename FROM photos").fetchall()
    assert len(photos) == 1
    assert photos[0]["filename"] == "IMG_001.cr3"

    raw_id_after = photos[0]["id"]
    # Both observations should be preserved on the primary
    subs = db.conn.execute(
        "SELECT observation_id FROM inat_submissions WHERE photo_id = ? ORDER BY observation_id",
        (raw_id_after,),
    ).fetchall()
    obs_ids = [s["observation_id"] for s in subs]
    assert 12345 in obs_ids
    assert 67890 in obs_ids


def test_scan_stores_file_hash(tmp_path):
    """Scanning a folder computes and stores SHA-256 file_hash for each photo."""
    from db import Database
    from scanner import scan

    # Create a test image
    img_dir = tmp_path / "photos"
    img_dir.mkdir()
    img = Image.new("RGB", (200, 100), color="green")
    img.save(str(img_dir / "test.jpg"))

    db = Database(str(tmp_path / "test.db"))
    scan(str(img_dir), db)

    photo = db.conn.execute("SELECT file_hash FROM photos LIMIT 1").fetchone()
    assert photo["file_hash"] is not None
    assert len(photo["file_hash"]) == 64  # SHA-256 hex digest length


def test_extract_dimensions_raw_skips_exif_thumbnail_size():
    """For RAW files, ExifImageWidth/Height is the embedded JPEG thumbnail (e.g. 160x120),
    not the actual image. _extract_dimensions should return the real dimensions from
    File:ImageWidth/Height instead."""
    from scanner import _extract_dimensions

    # Simulate ExifTool output for a Nikon NEF file:
    # EXIF:ExifImageWidth/Height = 160x120 (embedded thumbnail)
    # File:ImageWidth/Height = 8256x5504 (actual RAW image)
    exif_group = {
        "ExifImageWidth": 160,
        "ExifImageHeight": 120,
        "ImageWidth": 160,
        "ImageHeight": 120,
    }
    file_group = {
        "ImageWidth": 8256,
        "ImageHeight": 5504,
    }

    width, height = _extract_dimensions(exif_group, file_group, extension=".nef")

    assert width == 8256, f"Expected actual RAW width 8256, got {width} (embedded thumbnail)"
    assert height == 5504, f"Expected actual RAW height 5504, got {height} (embedded thumbnail)"


def test_extract_dimensions_jpeg_still_uses_exif():
    """For JPEG files, ExifImageWidth/Height should still be the first priority."""
    from scanner import _extract_dimensions

    exif_group = {
        "ExifImageWidth": 6000,
        "ExifImageHeight": 4000,
    }
    file_group = {
        "ImageWidth": 6000,
        "ImageHeight": 4000,
    }

    width, height = _extract_dimensions(exif_group, file_group, extension=".jpg")

    assert width == 6000
    assert height == 4000


def test_extract_dimensions_raw_falls_back_to_exif_imagewidth():
    """For RAW files without File dimensions, EXIF:ImageWidth (non-ExifImageWidth) is used."""
    from scanner import _extract_dimensions

    exif_group = {
        "ExifImageWidth": 160,
        "ExifImageHeight": 120,
        "ImageWidth": 8256,
        "ImageHeight": 5504,
    }
    file_group = {}

    width, height = _extract_dimensions(exif_group, file_group, extension=".nef")

    # Should skip ExifImageWidth (thumbnail) but still find ImageWidth
    assert width == 8256
    assert height == 5504


def test_extract_dimensions_all_raw_extensions():
    """All supported RAW extensions should skip ExifImageWidth/Height."""
    from scanner import _extract_dimensions

    raw_exts = [".nef", ".cr2", ".cr3", ".arw", ".raf", ".dng", ".rw2", ".orf"]

    for ext in raw_exts:
        exif_group = {"ExifImageWidth": 160, "ExifImageHeight": 120}
        file_group = {"ImageWidth": 8256, "ImageHeight": 5504}

        width, height = _extract_dimensions(exif_group, file_group, extension=ext)
        assert width == 8256, f"Failed for {ext}: got width {width}"
        assert height == 5504, f"Failed for {ext}: got height {height}"


def test_pair_raw_jpeg_transfers_gps_and_metadata(tmp_path):
    """Pairing raw+JPEG transfers GPS, exif_data, and focal_length from companion."""
    import json

    from db import Database
    from scanner import _pair_raw_jpeg_companions

    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder(str(tmp_path), name="photos")

    # JPEG has GPS and metadata, RAW does not
    jpeg_id = db.add_photo(folder_id=fid, filename="IMG.jpg", extension=".jpg",
                           file_size=1000, file_mtime=1.0)
    raw_id = db.add_photo(folder_id=fid, filename="IMG.nef", extension=".nef",
                          file_size=25000000, file_mtime=1.0)

    exif_json = json.dumps({"EXIF": {"Make": "Nikon", "Model": "Z9", "ISO": 400}})
    db.conn.execute(
        "UPDATE photos SET latitude=32.88, longitude=-117.25, exif_data=?, focal_length=400.0 WHERE id=?",
        (exif_json, jpeg_id),
    )
    db.conn.commit()

    _pair_raw_jpeg_companions(db)

    photo = db.conn.execute(
        "SELECT filename, latitude, longitude, exif_data, focal_length FROM photos"
    ).fetchone()
    assert photo["filename"] == "IMG.nef"
    assert photo["latitude"] == 32.88
    assert photo["longitude"] == -117.25
    assert photo["focal_length"] == 400.0
    meta = json.loads(photo["exif_data"])
    assert meta["EXIF"]["Make"] == "Nikon"


def test_pair_raw_jpeg_transfers_promoted_exif_summary_columns(tmp_path):
    """Pairing transfers ``camera_make``/``camera_model``/``lens``/``aperture``/
    ``shutter_speed``/``iso`` from the JPEG companion so the RAW row still
    populates the universal-filter fields after the JPEG row is deleted."""
    from db import Database
    from scanner import _pair_raw_jpeg_companions

    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder(str(tmp_path), name="photos")

    jpeg_id = db.add_photo(folder_id=fid, filename="IMG.jpg", extension=".jpg",
                           file_size=1000, file_mtime=1.0)
    raw_id = db.add_photo(folder_id=fid, filename="IMG.cr3", extension=".cr3",
                          file_size=20000000, file_mtime=1.0)

    db.conn.execute(
        "UPDATE photos SET camera_make=?, camera_model=?, lens=?, "
        "aperture=?, shutter_speed=?, iso=? WHERE id=?",
        ("Canon", "R5", "RF 100-500mm", 5.6, 0.002, 800, jpeg_id),
    )
    db.conn.commit()

    _pair_raw_jpeg_companions(db)

    photo = db.conn.execute(
        "SELECT filename, camera_make, camera_model, lens, aperture, "
        "shutter_speed, iso FROM photos"
    ).fetchone()
    assert photo["filename"] == "IMG.cr3"
    assert photo["camera_make"] == "Canon"
    assert photo["camera_model"] == "R5"
    assert photo["lens"] == "RF 100-500mm"
    assert photo["aperture"] == 5.6
    assert photo["shutter_speed"] == 0.002
    assert photo["iso"] == 800


def test_pair_raw_jpeg_preserves_primary_exif_summary_columns(tmp_path):
    """A RAW with its own EXIF summary values (e.g. from its own ExifTool
    extract) must not be overwritten by a JPEG companion's values."""
    from db import Database
    from scanner import _pair_raw_jpeg_companions

    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder(str(tmp_path), name="photos")

    jpeg_id = db.add_photo(folder_id=fid, filename="IMG.jpg", extension=".jpg",
                           file_size=1000, file_mtime=1.0)
    raw_id = db.add_photo(folder_id=fid, filename="IMG.cr3", extension=".cr3",
                          file_size=20000000, file_mtime=1.0)

    db.conn.execute(
        "UPDATE photos SET camera_make=?, camera_model=?, iso=? WHERE id=?",
        ("Sony", "A1", 200, raw_id),
    )
    db.conn.execute(
        "UPDATE photos SET camera_make=?, camera_model=?, iso=? WHERE id=?",
        ("Canon", "R5", 800, jpeg_id),
    )
    db.conn.commit()

    _pair_raw_jpeg_companions(db)

    photo = db.conn.execute(
        "SELECT camera_make, camera_model, iso FROM photos"
    ).fetchone()
    assert photo["camera_make"] == "Sony"
    assert photo["camera_model"] == "A1"
    assert photo["iso"] == 200


def test_pair_raw_jpeg_keeps_primary_gps_when_present(tmp_path):
    """If RAW already has GPS, companion GPS is not overwritten."""
    from db import Database
    from scanner import _pair_raw_jpeg_companions

    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder(str(tmp_path), name="photos")

    jpeg_id = db.add_photo(folder_id=fid, filename="IMG.jpg", extension=".jpg",
                           file_size=1000, file_mtime=1.0)
    raw_id = db.add_photo(folder_id=fid, filename="IMG.cr3", extension=".cr3",
                          file_size=20000000, file_mtime=1.0)

    # Both have GPS but different coords — primary should keep its own
    db.conn.execute(
        "UPDATE photos SET latitude=40.0, longitude=-74.0 WHERE id=?", (jpeg_id,))
    db.conn.execute(
        "UPDATE photos SET latitude=32.0, longitude=-117.0 WHERE id=?", (raw_id,))
    db.conn.commit()

    _pair_raw_jpeg_companions(db)

    photo = db.conn.execute("SELECT latitude, longitude FROM photos").fetchone()
    assert photo["latitude"] == 32.0
    assert photo["longitude"] == -117.0


def test_pair_raw_jpeg_transfers_zero_gps_from_companion(tmp_path):
    """A companion with latitude=0.0 (equator) should be transferred to a RAW that has no GPS."""
    from db import Database
    from scanner import _pair_raw_jpeg_companions

    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder(str(tmp_path), name="photos")

    jpeg_id = db.add_photo(folder_id=fid, filename="IMG.jpg", extension=".jpg",
                           file_size=1000, file_mtime=1.0)
    raw_id = db.add_photo(folder_id=fid, filename="IMG.nef", extension=".nef",
                          file_size=25000000, file_mtime=1.0)

    # JPEG is on the equator/prime meridian (0.0, 0.0) — falsy but valid
    db.conn.execute(
        "UPDATE photos SET latitude=0.0, longitude=0.0 WHERE id=?", (jpeg_id,))
    db.conn.commit()

    _pair_raw_jpeg_companions(db)

    photo = db.conn.execute("SELECT filename, latitude, longitude FROM photos").fetchone()
    assert photo["filename"] == "IMG.nef"
    assert photo["latitude"] == 0.0
    assert photo["longitude"] == 0.0


def test_pair_raw_jpeg_does_not_overwrite_zero_primary_gps(tmp_path):
    """A RAW with latitude=0.0 (equator) should NOT be overwritten by companion GPS."""
    from db import Database
    from scanner import _pair_raw_jpeg_companions

    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder(str(tmp_path), name="photos")

    jpeg_id = db.add_photo(folder_id=fid, filename="IMG.jpg", extension=".jpg",
                           file_size=1000, file_mtime=1.0)
    raw_id = db.add_photo(folder_id=fid, filename="IMG.cr3", extension=".cr3",
                          file_size=20000000, file_mtime=1.0)

    # JPEG has non-zero GPS, RAW sits at equator (0.0, 0.0) — must not be overwritten
    db.conn.execute(
        "UPDATE photos SET latitude=51.5, longitude=-0.1 WHERE id=?", (jpeg_id,))
    db.conn.execute(
        "UPDATE photos SET latitude=0.0, longitude=0.0 WHERE id=?", (raw_id,))
    db.conn.commit()

    _pair_raw_jpeg_companions(db)

    photo = db.conn.execute("SELECT latitude, longitude FROM photos").fetchone()
    assert photo["latitude"] == 0.0
    assert photo["longitude"] == 0.0


def test_scan_extracts_working_copy_for_raw(tmp_path, monkeypatch):
    """Scanning a RAW file creates a working copy JPEG."""
    import config as cfg
    import scanner
    from db import Database

    # Isolate ``cfg.CONFIG_PATH`` so this test reads defaults instead of
    # whatever a prior test on the same xdist worker may have left in the
    # global config (e.g. a small ``working_copy_max_size`` would change
    # which photos are working-copy candidates and flake the assertion).
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))

    # Set up vireo dir structure
    vireo_dir = tmp_path / "vireo"
    vireo_dir.mkdir()

    # Create a fake NEF file
    photo_dir = tmp_path / "photos"
    photo_dir.mkdir()
    nef_file = photo_dir / "IMG_001.nef"
    nef_file.write_bytes(b"fake raw data")

    # Mock ExifTool to return empty metadata
    monkeypatch.setattr(scanner, "extract_metadata", lambda paths, **_kwargs: {})

    # Mock extract_working_copy to actually create a file (simulates success)
    def fake_extract(source, output, max_size=4096, quality=92):
        os.makedirs(os.path.dirname(output), exist_ok=True)
        Image.new("RGB", (4096, 2731)).save(output, "JPEG")
        return True

    monkeypatch.setattr(scanner, "extract_working_copy", fake_extract)

    db = Database(str(vireo_dir / "test.db"))
    scanner.scan(str(photo_dir), db, vireo_dir=str(vireo_dir))

    photos = db.get_photos(per_page=999999)
    assert len(photos) == 1
    assert photos[0]["working_copy_path"] is not None
    assert os.path.exists(os.path.join(str(vireo_dir), photos[0]["working_copy_path"]))


def test_scan_skips_working_copy_for_jpeg(tmp_path, monkeypatch):
    """Scanning a JPEG file does not create a working copy."""
    import config as cfg
    import scanner
    from db import Database

    # Pin ``working_copy_max_size`` to its default by isolating CONFIG_PATH
    # — without this, a leaked small cap from another test on the same
    # xdist worker turns this 3000×2000 JPEG into an oversized candidate
    # and the scan extracts a working copy.
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))

    vireo_dir = tmp_path / "vireo"
    vireo_dir.mkdir()

    photo_dir = tmp_path / "photos"
    photo_dir.mkdir()
    jpg_file = photo_dir / "IMG_001.jpg"
    Image.new("RGB", (3000, 2000)).save(str(jpg_file), "JPEG")

    monkeypatch.setattr(scanner, "extract_metadata", lambda paths, **_kwargs: {})

    # Mock extract_working_copy -- should never be called for JPEGs
    calls = []

    def fake_extract(source, output, max_size=4096, quality=92):
        calls.append(source)
        return True

    monkeypatch.setattr(scanner, "extract_working_copy", fake_extract)

    db = Database(str(vireo_dir / "test.db"))
    scanner.scan(str(photo_dir), db, vireo_dir=str(vireo_dir))

    photos = db.get_photos(per_page=999999)
    assert len(photos) == 1
    assert photos[0]["working_copy_path"] is None
    assert len(calls) == 0


def test_scan_uses_raw_primary_for_raw_working_copy(tmp_path, monkeypatch):
    """RAW working copies decode the RAW primary, not the companion JPEG."""
    import config as cfg
    import scanner
    from db import Database

    # Isolate CONFIG_PATH so the candidate predicate (which depends on
    # ``working_copy_max_size``) reads defaults instead of leaked state.
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))

    vireo_dir = tmp_path / "vireo"
    vireo_dir.mkdir()

    photo_dir = tmp_path / "photos"
    photo_dir.mkdir()

    # Create RAW + JPEG pair
    nef_file = photo_dir / "IMG_001.nef"
    nef_file.write_bytes(b"fake raw data")
    jpg_file = photo_dir / "IMG_001.jpg"
    Image.new("RGB", (6000, 4000), color=(255, 0, 0)).save(str(jpg_file), "JPEG")

    # Mock ExifTool
    monkeypatch.setattr(scanner, "extract_metadata", lambda paths, **_kwargs: {})

    # Track which source file extract_working_copy is called with
    sources_used = []

    def fake_extract(source, output, max_size=4096, quality=92):
        sources_used.append(source)
        os.makedirs(os.path.dirname(output), exist_ok=True)
        Image.new("RGB", (4096, 2731)).save(output, "JPEG")
        return True

    monkeypatch.setattr(scanner, "extract_working_copy", fake_extract)

    db = Database(str(vireo_dir / "test.db"))
    scanner.scan(str(photo_dir), db, vireo_dir=str(vireo_dir))

    # After companion pairing, the RAW should have a working copy
    photos = db.get_photos(per_page=999999)
    raw_photos = [p for p in photos if p["extension"] == ".nef"]
    assert len(raw_photos) == 1
    assert raw_photos[0]["working_copy_path"] is not None

    # Verify the RAW primary was used as the source, not the companion JPEG.
    # Working copies are the edit-quality path, so they must preserve the RAW
    # highlight headroom that a camera JPEG may have already clipped.
    assert len(sources_used) == 1
    assert sources_used[0].endswith("IMG_001.nef"), (
        f"Expected RAW primary as source, got: {sources_used[0]}"
    )


def test_scan_falls_back_to_companion_when_raw_extraction_fails(
    tmp_path, monkeypatch,
):
    """RAW+JPEG pairs still get a working copy from the companion when the
    RAW decode itself fails (e.g. libraw cannot decode the RAW variant and
    the embedded preview is unusable). Without the fallback the row would
    be marked as a working-copy failure even though a full-size JPEG copy
    is sitting right next to it.
    """
    import config as cfg
    import scanner
    from db import Database

    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))

    vireo_dir = tmp_path / "vireo"
    vireo_dir.mkdir()

    photo_dir = tmp_path / "photos"
    photo_dir.mkdir()

    nef_file = photo_dir / "IMG_002.nef"
    nef_file.write_bytes(b"fake raw data")
    jpg_file = photo_dir / "IMG_002.jpg"
    Image.new("RGB", (6000, 4000), color=(0, 255, 0)).save(str(jpg_file), "JPEG")

    monkeypatch.setattr(scanner, "extract_metadata", lambda paths, **_kwargs: {})

    sources_used = []

    def fake_extract(source, output, max_size=4096, quality=92):
        sources_used.append(source)
        # Simulate libraw failure on the RAW; succeed when called with the
        # companion JPEG.
        if source.endswith(".nef"):
            return False
        os.makedirs(os.path.dirname(output), exist_ok=True)
        Image.new("RGB", (4096, 2731)).save(output, "JPEG")
        return True

    monkeypatch.setattr(scanner, "extract_working_copy", fake_extract)

    db = Database(str(vireo_dir / "test.db"))
    scanner.scan(str(photo_dir), db, vireo_dir=str(vireo_dir))

    photos = db.get_photos(per_page=999999)
    raw_photos = [p for p in photos if p["extension"] == ".nef"]
    assert len(raw_photos) == 1
    assert raw_photos[0]["working_copy_path"] is not None

    # Both calls should have happened: RAW first (failed), then companion.
    assert len(sources_used) == 2
    assert sources_used[0].endswith("IMG_002.nef")
    assert sources_used[1].endswith("IMG_002.jpg")

    # The companion-derived working copy is set, but the source failure marker
    # must remain so _recipe_render_source still allows companion selection for
    # edited RAW renders. Without this, _has_current_working_copy_failure
    # returns False, allow_companion stays False for the RAW primary, and the
    # render paths retry the unsupported RAW and 500.
    raw_row = db.conn.execute(
        "SELECT working_copy_path, working_copy_failed_source,"
        " working_copy_failed_at, working_copy_failed_mtime, file_mtime"
        " FROM photos WHERE extension = '.nef'"
    ).fetchone()
    assert raw_row["working_copy_path"] is not None
    assert raw_row["working_copy_failed_source"] == "source"
    assert raw_row["working_copy_failed_at"] is not None
    assert float(raw_row["working_copy_failed_mtime"]) == float(raw_row["file_mtime"])


def test_scan_falls_back_when_raw_working_copy_short_edge_is_smaller(
    tmp_path, monkeypatch,
):
    """A RAW embedded preview can tie the expected long edge while missing
    short-edge pixels. Scanner must compare both axes before accepting the
    RAW-derived working copy, otherwise the companion fallback never runs.
    """
    import config as cfg
    import scanner
    from db import Database

    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))

    vireo_dir = tmp_path / "vireo"
    vireo_dir.mkdir()

    photo_dir = tmp_path / "photos"
    photo_dir.mkdir()

    nef_file = photo_dir / "IMG_004.nef"
    nef_file.write_bytes(b"fake raw data")
    jpg_file = photo_dir / "IMG_004.jpg"
    Image.new("RGB", (6000, 4000), color=(0, 255, 0)).save(str(jpg_file), "JPEG")

    monkeypatch.setattr(scanner, "extract_metadata", lambda paths, **_kwargs: {})

    sources_used = []

    def fake_extract(source, output, max_size=4096, quality=92):
        sources_used.append(source)
        os.makedirs(os.path.dirname(output), exist_ok=True)
        if source.endswith(".nef"):
            # Same expected long edge after scaling (4096), but short edge is
            # too small for a 6000x4000 source scaled to 4096x2731.
            Image.new("RGB", (4096, 2305)).save(output, "JPEG")
        else:
            Image.new("RGB", (4096, 2731)).save(output, "JPEG")
        return True

    monkeypatch.setattr(scanner, "extract_working_copy", fake_extract)

    db = Database(str(vireo_dir / "test.db"))
    scanner.scan(str(photo_dir), db, vireo_dir=str(vireo_dir))

    assert len(sources_used) == 2
    assert sources_used[0].endswith("IMG_004.nef")
    assert sources_used[1].endswith("IMG_004.jpg")

    raw_row = db.conn.execute(
        "SELECT working_copy_path, working_copy_failed_source"
        " FROM photos WHERE extension = '.nef'"
    ).fetchone()
    assert raw_row["working_copy_path"] is not None
    assert raw_row["working_copy_failed_source"] == "source"

    with Image.open(vireo_dir / raw_row["working_copy_path"]) as img:
        assert img.size == (4096, 2731)


def test_scan_accepts_near_full_raw_working_copy(tmp_path, monkeypatch):
    """Tiny RAW active-area differences should not force companion fallback."""
    import config as cfg
    import scanner
    from db import Database

    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))

    vireo_dir = tmp_path / "vireo"
    vireo_dir.mkdir()

    photo_dir = tmp_path / "photos"
    photo_dir.mkdir()

    nef_file = photo_dir / "IMG_006.nef"
    nef_file.write_bytes(b"fake raw data")
    jpg_file = photo_dir / "IMG_006.jpg"
    Image.new("RGB", (6000, 4000), color=(0, 255, 0)).save(str(jpg_file), "JPEG")

    monkeypatch.setattr(scanner, "extract_metadata", lambda paths, **_kwargs: {})

    sources_used = []

    def fake_extract(source, output, max_size=4096, quality=92):
        sources_used.append(source)
        os.makedirs(os.path.dirname(output), exist_ok=True)
        # Expected scaled dimensions are 4096x2731. This is within 1% of
        # the expected short edge and should be accepted as a RAW decode.
        Image.new("RGB", (4096, 2705)).save(output, "JPEG")
        return True

    monkeypatch.setattr(scanner, "extract_working_copy", fake_extract)

    db = Database(str(vireo_dir / "test.db"))
    scanner.scan(str(photo_dir), db, vireo_dir=str(vireo_dir))

    assert len(sources_used) == 1
    assert sources_used[0].endswith("IMG_006.nef")

    raw_row = db.conn.execute(
        "SELECT working_copy_path, working_copy_failed_source"
        " FROM photos WHERE extension = '.nef'"
    ).fetchone()
    assert raw_row["working_copy_path"] is not None
    assert raw_row["working_copy_failed_source"] is None

    with Image.open(vireo_dir / raw_row["working_copy_path"]) as img:
        assert img.size == (4096, 2705)


def test_scan_accepts_portrait_raw_working_copy_with_exif_orientation(
    tmp_path, monkeypatch,
):
    """Stored width/height are the unrotated sensor axes, so a portrait shot
    on a landscape sensor is e.g. 6000x4000 with EXIF Orientation 6.
    ``extract_working_copy`` writes the orientation-normalized JPEG (4000x6000
    scaled to 4096). The scanner's RAW-undersize check must apply the same
    orientation swap that the request-path helpers use; otherwise it sees
    4096-on-the-short-edge as catastrophically undersized vs. an expected
    4096-on-the-long-edge and falls back to the companion JPEG, leaving
    ``working_copy_failed_source='source'`` so edited renders bypass the
    preserve-highlights RAW path for every portrait photo.
    """
    import config as cfg
    import scanner
    from db import Database

    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))

    vireo_dir = tmp_path / "vireo"
    vireo_dir.mkdir()

    photo_dir = tmp_path / "photos"
    photo_dir.mkdir()

    nef_file = photo_dir / "IMG_005.nef"
    nef_file.write_bytes(b"fake raw data")
    jpg_file = photo_dir / "IMG_005.jpg"
    # Companion JPEG written with sensor-axis dimensions; the orientation
    # flag below is what tells consumers it should display as portrait.
    Image.new("RGB", (6000, 4000), color=(255, 0, 255)).save(str(jpg_file), "JPEG")

    portrait_meta = {
        "File": {"ImageWidth": 6000, "ImageHeight": 4000},
        "EXIF": {
            "ImageWidth": 6000,
            "ImageHeight": 4000,
            "Orientation": 6,
        },
    }

    def fake_metadata(paths, restricted_tags=None, progress_callback=None,
                      checkpoint=None):
        return {str(p): portrait_meta for p in paths}

    monkeypatch.setattr(scanner, "extract_metadata", fake_metadata)

    sources_used = []

    def fake_extract(source, output, max_size=4096, quality=92):
        sources_used.append(source)
        os.makedirs(os.path.dirname(output), exist_ok=True)
        # libraw + image_loader normalize EXIF orientation: a 6000x4000 sensor
        # readout with Orientation 6 is written as a portrait 4000x6000 JPEG,
        # which here scales to 2731x4096 to fit max_size=4096.
        Image.new("RGB", (2731, 4096)).save(output, "JPEG")
        return True

    monkeypatch.setattr(scanner, "extract_working_copy", fake_extract)

    db = Database(str(vireo_dir / "test.db"))
    scanner.scan(str(photo_dir), db, vireo_dir=str(vireo_dir))

    # Only the RAW should have been extracted — no companion fallback.
    assert len(sources_used) == 1, sources_used
    assert sources_used[0].endswith("IMG_005.nef")

    raw_row = db.conn.execute(
        "SELECT working_copy_path, working_copy_failed_source"
        " FROM photos WHERE extension = '.nef'"
    ).fetchone()
    assert raw_row is not None
    assert raw_row["working_copy_path"] is not None
    assert raw_row["working_copy_failed_source"] is None, (
        "Portrait RAW with orientation-normalized working copy must not be "
        "marked as a source failure; got "
        f"{raw_row['working_copy_failed_source']!r}"
    )


def test_scan_marks_source_failure_when_raw_and_companion_both_fail(
    tmp_path, monkeypatch,
):
    """When the RAW fails AND the companion fallback also fails, the failure
    marker must stay 'source' so request paths
    (_has_current_working_copy_failure) skip the slow RAW retry. Marking
    'companion' here would silently un-shield request paths from the known
    RAW failure: that helper explicitly ignores companion markers while both
    files exist.
    """
    import config as cfg
    import scanner
    from db import Database

    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))

    vireo_dir = tmp_path / "vireo"
    vireo_dir.mkdir()

    photo_dir = tmp_path / "photos"
    photo_dir.mkdir()

    nef_file = photo_dir / "IMG_003.nef"
    nef_file.write_bytes(b"fake raw data")
    jpg_file = photo_dir / "IMG_003.jpg"
    Image.new("RGB", (6000, 4000), color=(0, 0, 255)).save(str(jpg_file), "JPEG")

    monkeypatch.setattr(scanner, "extract_metadata", lambda paths, **_kwargs: {})

    def fake_extract(source, output, max_size=4096, quality=92):
        # Both RAW and companion fail (e.g. RAW unsupported + companion
        # write-locked or corrupt).
        return False

    monkeypatch.setattr(scanner, "extract_working_copy", fake_extract)

    db = Database(str(vireo_dir / "test.db"))
    scanner.scan(str(photo_dir), db, vireo_dir=str(vireo_dir))

    raw_row = db.conn.execute(
        "SELECT working_copy_path, working_copy_failed_source"
        " FROM photos WHERE extension = '.nef'"
    ).fetchone()
    assert raw_row is not None
    assert raw_row["working_copy_path"] is None
    assert raw_row["working_copy_failed_source"] == "source", (
        "RAW failure marker must remain 'source' so "
        "_has_current_working_copy_failure honors it; got "
        f"{raw_row['working_copy_failed_source']!r}"
    )


# --- _extract_timestamp tests ---

def test_extract_timestamp_subsec():
    """_extract_timestamp includes sub-second precision from SubSecTimeOriginal."""
    from scanner import _extract_timestamp
    exif = {"DateTimeOriginal": "2024:06:15 14:30:00", "SubSecTimeOriginal": "123"}
    ts = _extract_timestamp(exif)
    assert ts == "2024-06-15T14:30:00.123000"


def test_extract_timestamp_no_subsec():
    """_extract_timestamp works without SubSecTimeOriginal."""
    from scanner import _extract_timestamp
    exif = {"DateTimeOriginal": "2024:06:15 14:30:00"}
    ts = _extract_timestamp(exif)
    assert ts == "2024-06-15T14:30:00"


def test_extract_timestamp_garbage_subsec():
    """_extract_timestamp ignores non-numeric SubSecTimeOriginal."""
    from scanner import _extract_timestamp
    exif = {"DateTimeOriginal": "2024:06:15 14:30:00", "SubSecTimeOriginal": "abc"}
    ts = _extract_timestamp(exif)
    assert ts == "2024-06-15T14:30:00"


def test_extract_timestamp_subsec_fallback():
    """_extract_timestamp falls back to SubSecTime when SubSecTimeOriginal is absent."""
    from scanner import _extract_timestamp
    exif = {"DateTimeOriginal": "2024:06:15 14:30:00", "SubSecTime": "50"}
    ts = _extract_timestamp(exif)
    assert ts == "2024-06-15T14:30:00.500000"


def test_extract_timestamp_subsec_long():
    """_extract_timestamp truncates sub-second values longer than 6 digits."""
    from scanner import _extract_timestamp
    exif = {"DateTimeOriginal": "2024:06:15 14:30:00", "SubSecTimeOriginal": "12345678"}
    ts = _extract_timestamp(exif)
    assert ts == "2024-06-15T14:30:00.123456"


# --- Incremental rescan metadata_missing heuristic tests ---

def _setup_scanned_photo(tmp_path, pil_size=(640, 480)):
    """Create a JPEG, run a fresh scan, return (db, photo_id, image_path)."""
    import scanner
    from db import Database

    root = str(tmp_path / "photos")
    os.makedirs(root)
    image_path = os.path.join(root, "photo.jpg")
    Image.new("RGB", pil_size, color="green").save(image_path, "JPEG")

    db = Database(str(tmp_path / "test.db"))
    # Mock ExifTool so the first scan populates exif_data with real
    # dimensions, independent of whether exiftool is installed.
    def fake_extract(paths, restricted_tags=None, progress_callback=None,
                     checkpoint=None):
        return {
            p: {"File": {"ImageWidth": pil_size[0], "ImageHeight": pil_size[1]},
                "EXIF": {}, "Composite": {}}
            for p in paths
        }
    import metadata
    original = metadata.extract_metadata
    metadata.extract_metadata = fake_extract
    scanner.extract_metadata = fake_extract
    try:
        scanner.scan(root, db)
    finally:
        metadata.extract_metadata = original
        scanner.extract_metadata = original

    row = db.conn.execute(
        "SELECT id FROM photos WHERE filename='photo.jpg'"
    ).fetchone()
    return db, root, image_path, row["id"]


def test_incremental_rescan_reextracts_when_timestamp_null(tmp_path, monkeypatch):
    """Incremental scan re-processes a photo whose timestamp is NULL
    and exif_data is NULL (existing behavior — regression guard)."""
    import scanner

    db, root, image_path, pid = _setup_scanned_photo(tmp_path)

    # Simulate broken state: timestamp lost, dims wrong, exif_data cleared.
    db.conn.execute(
        "UPDATE photos SET timestamp=NULL, width=100, height=100, "
        "exif_data=NULL WHERE id=?", (pid,)
    )
    db.conn.commit()

    def fake_extract(paths, restricted_tags=None, progress_callback=None,
                     checkpoint=None):
        return {p: {"File": {"ImageWidth": 640, "ImageHeight": 480},
                    "EXIF": {}, "Composite": {}} for p in paths}
    monkeypatch.setattr(scanner, "extract_metadata", fake_extract)

    scanner.scan(root, db, incremental=True)

    row = db.conn.execute(
        "SELECT width, height, exif_data FROM photos WHERE id=?", (pid,)
    ).fetchone()
    assert row["width"] == 640  # repopulated from fake ExifTool
    assert row["height"] == 480
    assert row["exif_data"] is not None


def test_incremental_rescan_reextracts_when_raw_dims_suspect(tmp_path, monkeypatch):
    """Incremental scan re-processes a row where extension is RAW and
    width < 1000 (the 160x120 embedded-thumb bug), even when timestamp
    is populated — provided exif_data is NULL so the guard doesn't block."""
    import scanner

    db, root, image_path, pid = _setup_scanned_photo(tmp_path)

    # Simulate broken state: fake RAW extension with thumbnail dims and
    # populated timestamp. exif_data=NULL so the exif_extracted guard
    # doesn't block re-extraction.
    db.conn.execute(
        "UPDATE photos SET extension='.nef', width=160, height=120, "
        "timestamp='2020-01-01T12:00:00', exif_data=NULL WHERE id=?", (pid,)
    )
    db.conn.commit()

    def fake_extract(paths, restricted_tags=None, progress_callback=None,
                     checkpoint=None):
        return {p: {"File": {"ImageWidth": 640, "ImageHeight": 480},
                    "EXIF": {}, "Composite": {}} for p in paths}
    monkeypatch.setattr(scanner, "extract_metadata", fake_extract)

    scanner.scan(root, db, incremental=True)

    row = db.conn.execute(
        "SELECT width, height, exif_data FROM photos WHERE id=?", (pid,)
    ).fetchone()
    assert row["width"] == 640
    assert row["height"] == 480
    assert row["exif_data"] is not None


def test_incremental_rescan_skips_small_jpeg_dims_not_raw(tmp_path, monkeypatch):
    """Incremental scan does NOT re-process a non-RAW row with suspicious
    small dimensions. The dims heuristic is RAW-specific so JPEGs, PNGs,
    etc. that are legitimately tiny aren't re-extracted repeatedly."""
    import scanner

    db, root, image_path, pid = _setup_scanned_photo(tmp_path)

    # Simulate small-dims on a non-RAW extension; timestamp populated so
    # the NULL-timestamp branch doesn't fire either. camera_make stays
    # non-NULL so the Phase-1 ``summary_needs_extract`` trigger — which
    # re-extracts rows whose promoted EXIF cols are all NULL alongside a
    # NULL ``exif_data`` (the pre-Phase-1 upgrade shape) — doesn't fire
    # here; this test is about the RAW-only dim heuristic in isolation.
    db.conn.execute(
        "UPDATE photos SET extension='.jpg', width=160, height=120, "
        "timestamp='2020-01-01T12:00:00', exif_data=NULL, "
        "camera_make='Sony' WHERE id=?", (pid,)
    )
    db.conn.commit()

    called_with = []
    def fake_extract(paths, restricted_tags=None, progress_callback=None,
                     checkpoint=None):
        called_with.append(list(paths))
        return {p: {"File": {"ImageWidth": 640, "ImageHeight": 480},
                    "EXIF": {}, "Composite": {}} for p in paths}
    monkeypatch.setattr(scanner, "extract_metadata", fake_extract)

    scanner.scan(root, db, incremental=True)

    # width stays at the synthetic broken value because we didn't reprocess.
    row = db.conn.execute(
        "SELECT width, height FROM photos WHERE id=?", (pid,)
    ).fetchone()
    assert row["width"] == 160
    assert row["height"] == 120
    # And extract_metadata was never called with this file.
    assert all(image_path not in batch for batch in called_with)


def test_scan_restrict_files_ignores_files_not_in_list(tmp_path, monkeypatch):
    """When scan is called with restrict_files, files in restrict_dirs
    that are not in the list are left untouched — even if they're brand
    new and not yet in the DB. This prevents the pipeline's repair path
    from ingesting new files as a side effect of fixing broken metadata."""
    import scanner
    from db import Database

    root = str(tmp_path / "photos")
    os.makedirs(root)
    existing_file = os.path.join(root, "existing.jpg")
    Image.new("RGB", (640, 480), color="green").save(existing_file, "JPEG")

    db = Database(str(tmp_path / "test.db"))

    def fake_extract(paths, restricted_tags=None, progress_callback=None,
                     checkpoint=None):
        return {p: {"File": {"ImageWidth": 640, "ImageHeight": 480},
                    "EXIF": {}, "Composite": {}} for p in paths}
    monkeypatch.setattr(scanner, "extract_metadata", fake_extract)

    # Seed the DB with only the existing file, then force broken state.
    scanner.scan(root, db)
    pid = db.conn.execute(
        "SELECT id FROM photos WHERE filename='existing.jpg'"
    ).fetchone()["id"]
    db.conn.execute(
        "UPDATE photos SET timestamp=NULL, exif_data=NULL WHERE id=?", (pid,)
    )
    db.conn.commit()

    # NOW add an untracked file to the same folder (after initial scan).
    new_file = os.path.join(root, "new_untracked.jpg")
    Image.new("RGB", (640, 480), color="blue").save(new_file, "JPEG")

    # Second scan with restrict_files constrained to the existing file only.
    scanner.scan(
        root, db,
        incremental=True,
        restrict_dirs=[root],
        restrict_files={existing_file},
    )

    # new_untracked.jpg should NOT have been ingested.
    filenames = [p["filename"] for p in db.get_photos(per_page=999999)]
    assert "new_untracked.jpg" not in filenames
    assert "existing.jpg" in filenames


def test_update_only_scan_cannot_admit_restricted_file(tmp_path, monkeypatch):
    import scanner
    from db import Database

    root = str(tmp_path / "photos")
    os.makedirs(root)
    path = os.path.join(root, "uncataloged.jpg")
    Image.new("RGB", (64, 64), color="blue").save(path, "JPEG")
    db = Database(str(tmp_path / "test.db"))

    monkeypatch.setattr(
        scanner,
        "extract_metadata",
        lambda paths, restricted_tags=None, progress_callback=None,
        checkpoint=None: {},
    )
    scanner.scan(
        root,
        db,
        restrict_dirs=[root],
        restrict_files={path},
        allow_photo_inserts=False,
        register_restrict_dirs_as_roots=False,
    )

    assert db.conn.execute("SELECT COUNT(*) FROM photos").fetchone()[0] == 0


def test_incremental_rescan_respects_exif_extracted_guard(tmp_path, monkeypatch):
    """Incremental scan does NOT re-process a row when exif_data is
    populated, even if the row otherwise looks broken. The guard prevents
    retry loops on photos where ExifTool has already produced output
    (e.g. files with genuinely missing EXIF timestamps)."""
    import scanner

    db, root, image_path, pid = _setup_scanned_photo(tmp_path)

    # Broken-looking state, but exif_data is populated (ExifTool already
    # ran once). Scanner must skip this row.
    db.conn.execute(
        "UPDATE photos SET extension='.nef', width=160, height=120, "
        "timestamp='2020-01-01T12:00:00', "
        "exif_data='{\"File\":{}}' WHERE id=?", (pid,)
    )
    db.conn.commit()

    called_with = []
    def fake_extract(paths, restricted_tags=None, progress_callback=None,
                     checkpoint=None):
        called_with.append(list(paths))
        return {p: {"File": {"ImageWidth": 640, "ImageHeight": 480},
                    "EXIF": {}, "Composite": {}} for p in paths}
    monkeypatch.setattr(scanner, "extract_metadata", fake_extract)

    scanner.scan(root, db, incremental=True)

    row = db.conn.execute(
        "SELECT width, height FROM photos WHERE id=?", (pid,)
    ).fetchone()
    assert row["width"] == 160
    assert row["height"] == 120
    assert all(image_path not in batch for batch in called_with)


def test_resolve_worker_count_tiny_batch_is_sequential():
    """Batches below 8 files always use 1 worker."""
    from scanner import _resolve_worker_count
    assert _resolve_worker_count(list(range(7))) == 1


def test_resolve_worker_count_capped_by_batch_size(monkeypatch):
    """Worker count never exceeds the batch size."""
    import config as cfg
    import scanner

    monkeypatch.setattr(cfg, "get", lambda _k: 0)
    monkeypatch.setattr(scanner.os, "cpu_count", lambda: 32)
    # 10 files on a 32-core box should top out at 10 workers.
    assert scanner._resolve_worker_count(list(range(10))) == 10


def test_resolve_worker_count_clamps_to_windows_limit(monkeypatch):
    """On Windows, ProcessPoolExecutor rejects max_workers > 61, so clamp."""
    import config as cfg
    import scanner

    monkeypatch.setattr(cfg, "get", lambda _k: 0)
    monkeypatch.setattr(scanner.os, "cpu_count", lambda: 128)
    monkeypatch.setattr(scanner.sys, "platform", "win32")
    # Batch is large enough that it wouldn't otherwise clamp the count.
    workers = scanner._resolve_worker_count(list(range(200)))
    assert workers == scanner._WINDOWS_MAX_WORKERS == 61


def test_resolve_worker_count_clamps_configured_value_on_windows(monkeypatch):
    """Explicit scan_workers above 61 is still clamped on Windows."""
    import config as cfg
    import scanner

    monkeypatch.setattr(cfg, "get", lambda _k: 96)
    monkeypatch.setattr(scanner.os, "cpu_count", lambda: 128)
    monkeypatch.setattr(scanner.sys, "platform", "win32")
    assert scanner._resolve_worker_count(list(range(200))) == 61


def test_resolve_worker_count_no_windows_cap_on_posix(monkeypatch):
    """The 61-worker cap must not apply on non-Windows platforms."""
    import config as cfg
    import scanner

    monkeypatch.setattr(cfg, "get", lambda _k: 0)
    monkeypatch.setattr(scanner.os, "cpu_count", lambda: 128)
    monkeypatch.setattr(scanner.sys, "platform", "linux")
    assert scanner._resolve_worker_count(list(range(200))) == 128


# -- scan resilience: retry on locked DB, mark folder partial on abort --


class _FlakyConn:
    """Connection proxy that injects commit failures for testing.

    sqlite3.Connection.commit is read-only at the instance level, so tests
    that need to simulate transient commit failures wrap the real connection
    in this proxy. All other attributes pass through to the real connection
    so code that calls ``conn.execute(...)`` etc. behaves identically.
    """

    def __init__(self, real, fail_on_calls):
        """fail_on_calls: dict {call_number: exception_to_raise}."""
        self._real = real
        self._fail_on_calls = dict(fail_on_calls)
        self._call_count = 0

    def commit(self):
        self._call_count += 1
        exc = self._fail_on_calls.get(self._call_count)
        if exc is not None:
            raise exc
        return self._real.commit()

    # sqlite3.Connection is used as a context manager in db.py
    # (``with self.conn:`` for transactions). Python bypasses ``__getattr__``
    # for dunder lookups, so we must forward these explicitly. Route commit
    # through our own method so the fail injection still fires.
    def __enter__(self):
        self._real.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            try:
                self.commit()
            except BaseException:
                self._real.rollback()
                raise
        else:
            self._real.rollback()
        return False

    def __getattr__(self, name):
        return getattr(self._real, name)


def test_scan_retries_on_database_is_locked(tmp_path):
    """If a commit hits 'database is locked', scan retries instead of aborting.

    busy_timeout covers most cases, but a retry wrapper handles the tail where
    a contended DB exceeds the timeout mid-scan. Without it, a single transient
    lock aborts the whole scan and leaves the folder partially populated.
    """
    import sqlite3

    import scanner as scanner_mod
    from db import Database

    root = str(tmp_path / "photos")
    _create_test_images(root, {'': ['a.jpg', 'b.jpg']})
    db = Database(str(tmp_path / "test.db"))

    # First two commits raise 'database is locked'; subsequent commits succeed.
    locked = sqlite3.OperationalError("database is locked")
    db.conn = _FlakyConn(db.conn, fail_on_calls={1: locked, 2: locked})

    scanner_mod.scan(root, db)

    filenames = {
        p["filename"]
        for p in db.conn.execute("SELECT filename FROM photos").fetchall()
    }
    assert filenames == {"a.jpg", "b.jpg"}, (
        f"expected both photos persisted after retries, got {filenames}"
    )


def test_scan_marks_folder_partial_on_unrecoverable_failure(tmp_path):
    """When scan can't recover, the folder is marked 'partial' before raising.

    Visible state: user sees the folder in its UI with a 'partial' badge and
    knows to rescan, instead of believing the folder is fully imported.
    """
    import sqlite3

    import scanner as scanner_mod
    from db import Database

    root = str(tmp_path / "photos")
    _create_test_images(root, {'': ['a.jpg', 'b.jpg', 'c.jpg']})
    db = Database(str(tmp_path / "test.db"))

    # Second commit raises a non-lock OperationalError that retry won't
    # swallow. Scan must mark the folder partial and re-raise.
    db.conn = _FlakyConn(
        db.conn,
        fail_on_calls={2: sqlite3.OperationalError("disk I/O error")},
    )

    with pytest.raises(sqlite3.OperationalError):
        scanner_mod.scan(root, db)

    # Unwrap proxy for the final assertion.
    real_conn = db.conn._real
    row = real_conn.execute(
        "SELECT status FROM folders WHERE path = ?", (root,)
    ).fetchone()
    assert row is not None, "folder row should exist despite aborted scan"
    assert row["status"] == "partial", (
        f"expected folder.status='partial' after mid-scan failure, got {row['status']!r}"
    )


def test_partial_folder_is_visible_in_folder_tree(tmp_path):
    """Folders flagged 'partial' must still render in the browse-page tree.

    get_folder_tree() historically required status='ok'. After marking a
    folder partial we need it to STILL appear so the user can see the badge
    and initiate a rescan — otherwise 'partial' silently hides the folder.
    """
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder("/p")
    db.conn.execute("UPDATE folders SET status = 'partial' WHERE id = ?", (fid,))
    db.conn.commit()

    tree = db.get_folder_tree()
    ids = {row["id"] for row in tree}
    assert fid in ids, "partial folder should still appear in get_folder_tree"
    # Status should be queryable so the UI can render the badge.
    partial_row = next(row for row in tree if row["id"] == fid)
    assert partial_row["status"] == "partial"


def test_successful_scan_clears_partial_flag(tmp_path):
    """A successful rescan of a previously-partial folder restores 'ok'."""
    import sqlite3

    import scanner as scanner_mod
    from db import Database

    root = str(tmp_path / "photos")
    _create_test_images(root, {'': ['a.jpg', 'b.jpg']})
    db = Database(str(tmp_path / "test.db"))

    # First scan: fail partway through to leave the folder 'partial'.
    db.conn = _FlakyConn(
        db.conn,
        fail_on_calls={2: sqlite3.OperationalError("disk I/O error")},
    )
    with pytest.raises(sqlite3.OperationalError):
        scanner_mod.scan(root, db)
    real_conn = db.conn._real
    row = real_conn.execute(
        "SELECT status FROM folders WHERE path = ?", (root,)
    ).fetchone()
    assert row["status"] == "partial"

    # Second scan: succeed and clear the flag.
    db.conn = real_conn
    scanner_mod.scan(root, db)
    row = db.conn.execute(
        "SELECT status FROM folders WHERE path = ?", (root,)
    ).fetchone()
    assert row["status"] == "ok", (
        f"successful rescan should flip partial → ok, got {row['status']!r}"
    )


def test_successful_scan_clears_partial_on_touched_subfolders(tmp_path):
    """Recursive scan clears 'partial' on subfolders the scan actually touched.

    The exception path flags every touched subfolder as partial; the success
    path must reset those same subfolders so a user who rescans after a
    failure sees a clean tree.
    """
    import scanner as scanner_mod
    from db import Database

    root = str(tmp_path / "photos")
    _create_test_images(root, {
        '': ['root.jpg'],
        'sub': ['nested.jpg'],
    })
    db = Database(str(tmp_path / "test.db"))

    scanner_mod.scan(root, db)
    # Mark both folders partial to simulate a prior mid-scan abort.
    db.conn.execute("UPDATE folders SET status = 'partial'")
    db.conn.commit()

    scanner_mod.scan(root, db)

    rows = db.conn.execute(
        "SELECT path, status FROM folders ORDER BY path"
    ).fetchall()
    statuses = {r["path"]: r["status"] for r in rows}
    assert statuses[root] == "ok", (
        f"root should be cleared back to ok, got {statuses[root]!r}"
    )
    sub_path = os.path.join(root, "sub")
    assert statuses[sub_path] == "ok", (
        f"touched subfolder should be cleared back to ok, "
        f"got {statuses[sub_path]!r}"
    )


def test_partial_folder_photos_remain_visible_in_queries(tmp_path):
    """Photos in 'partial' folders must stay queryable through read paths.

    Before this fix, `f.status = 'ok'` joins across `db.py` excluded photos
    from partial folders, so an interrupted scan could make already-imported
    photos disappear from the UI.
    """
    import scanner as scanner_mod
    from db import Database

    root = str(tmp_path / "photos")
    _create_test_images(root, {'': ['seen.jpg']})
    db = Database(str(tmp_path / "test.db"))

    scanner_mod.scan(root, db)
    db.conn.execute(
        "UPDATE folders SET status = 'partial' WHERE path = ?", (root,)
    )
    db.conn.commit()

    # Photo queries should still return the photo.
    photos = db.get_photos(per_page=100)
    filenames = {p["filename"] for p in photos}
    assert "seen.jpg" in filenames, (
        f"photo in partial folder should remain visible, got {filenames}"
    )

    # And coverage stats should still count it.
    stats = db.get_coverage_stats()
    assert stats["total"] >= 1, (
        f"coverage should count photos in partial folders, "
        f"got total={stats['total']!r}"
    )


def test_successful_noop_incremental_scan_clears_partial(tmp_path):
    """A no-op incremental rescan must still clear 'partial' on scoped folders.

    If the success-path reset is gated only on the main loop's
    ``touched_folder_ids`` set, a successful incremental scan that processes
    zero files (all photos unchanged) leaves ``status='partial'`` stuck and
    the folder hidden from ``status='ok'`` read paths. The reset must also
    run for the outer scan scope (root + restrict_dirs).
    """
    import scanner as scanner_mod
    from db import Database

    root = str(tmp_path / "photos")
    _create_test_images(root, {'': ['a.jpg', 'b.jpg']})
    db = Database(str(tmp_path / "test.db"))

    # Initial clean scan so photos are indexed.
    scanner_mod.scan(root, db)

    # Simulate the after-failure state: folder got flagged 'partial' by a
    # prior aborted scan even though all photo rows are already present.
    db.conn.execute(
        "UPDATE folders SET status = 'partial' WHERE path = ?", (root,)
    )
    db.conn.commit()

    # Incremental rescan — no files changed, so the main loop touches zero
    # folders. The scan scope fallback should still clear 'partial'.
    scanner_mod.scan(root, db, incremental=True)

    row = db.conn.execute(
        "SELECT status FROM folders WHERE path = ?", (root,)
    ).fetchone()
    assert row["status"] == "ok", (
        f"no-op incremental scan should flip partial → ok, got {row['status']!r}"
    )


def test_pre_pass_failure_marks_folder_partial(tmp_path):
    """A non-retryable DB error during the pre-pass XMP commit flags the folder.

    Pre-pass XMP re-imports commit before the main scan loop begins. If that
    commit raises a non-transient error, the scan aborts with the folder row
    still ``status='ok'`` unless the pre-pass is wrapped in the same partial-
    status recovery path as the main loop.
    """
    import sqlite3

    import scanner as scanner_mod
    from db import Database

    root = str(tmp_path / "photos")
    _create_test_images(root, {'': ['a.jpg']})
    db = Database(str(tmp_path / "test.db"))

    # Initial clean scan so the photo row exists for incremental mode.
    scanner_mod.scan(root, db)

    # Touch the XMP sidecar so the pre-pass re-imports keywords and commits.
    xmp_path = os.path.join(root, "a.xmp")
    with open(xmp_path, "w") as f:
        f.write("<x:xmpmeta xmlns:x='adobe:ns:meta/'></x:xmpmeta>")
    # Make the existing row's xmp_mtime stale so pre-pass treats it as changed.
    db.conn.execute("UPDATE photos SET xmp_mtime = 0 WHERE filename = 'a.jpg'")
    db.conn.commit()

    # First commit after scan starts is the pre-pass XMP UPDATE. Raise a
    # non-retryable OperationalError there.
    db.conn = _FlakyConn(
        db.conn,
        fail_on_calls={1: sqlite3.OperationalError("disk I/O error")},
    )

    with pytest.raises(sqlite3.OperationalError):
        scanner_mod.scan(root, db, incremental=True)

    real_conn = db.conn._real
    row = real_conn.execute(
        "SELECT status FROM folders WHERE path = ?", (root,)
    ).fetchone()
    assert row is not None
    assert row["status"] == "partial", (
        f"expected folder.status='partial' after pre-pass failure, got {row['status']!r}"
    )


def test_rescan_invalidates_stale_thumbnail_when_file_content_changes(tmp_path):
    """When a file's content changes on disk, re-scan must invalidate the
    stale thumbnail so the next serve regenerates from fresh pixels.

    Regression test for the _D851925.NEF bug where a photo's thumbnail showed
    a bird but the full image (derived from the current file) was a squirrel:
    the source had been replaced, file_hash was updated on re-scan, but the
    thumbnail cache was never invalidated.
    """
    from db import Database
    from scanner import scan
    from thumbnails import generate_thumbnail

    root = str(tmp_path / "photos")
    os.makedirs(root)
    img_path = os.path.join(root, "photo.jpg")
    # Original content: solid red
    Image.new("RGB", (800, 600), color=(255, 0, 0)).save(img_path, "JPEG")

    vireo_dir = tmp_path / "vireo"
    vireo_dir.mkdir()
    cache_dir = vireo_dir / "thumbnails"
    cache_dir.mkdir()

    db = Database(str(vireo_dir / "test.db"))
    scan(root, db, vireo_dir=str(vireo_dir))

    photos = db.get_photos(per_page=100)
    assert len(photos) == 1
    photo_id = photos[0]["id"]
    original_hash = db.conn.execute(
        "SELECT file_hash FROM photos WHERE id = ?", (photo_id,)
    ).fetchone()[0]
    assert original_hash is not None

    thumb_path = str(cache_dir / f"{photo_id}.jpg")
    generate_thumbnail(photo_id, img_path, str(cache_dir))
    assert os.path.exists(thumb_path)
    raw_variant = cache_dir / f"{photo_id}_raw.jpg"
    jpeg_variant = cache_dir / f"{photo_id}_jpeg.jpg"
    raw_variant.write_bytes(b"stale raw thumbnail")
    jpeg_variant.write_bytes(b"stale jpeg thumbnail")

    # Replace file content (same filename, different pixels → new hash + new mtime)
    time.sleep(0.05)
    Image.new("RGB", (800, 600), color=(0, 0, 255)).save(img_path, "JPEG")
    # Ensure file_mtime differs from the DB value so incremental scan re-processes.
    new_mtime = os.path.getmtime(img_path)
    db_mtime = db.conn.execute(
        "SELECT file_mtime FROM photos WHERE id = ?", (photo_id,)
    ).fetchone()[0]
    assert new_mtime != db_mtime

    # Re-scan: scanner must detect the content change and drop the stale thumbnail.
    scan(root, db, incremental=True, vireo_dir=str(vireo_dir))

    updated_hash = db.conn.execute(
        "SELECT file_hash FROM photos WHERE id = ?", (photo_id,)
    ).fetchone()[0]
    assert updated_hash != original_hash, "sanity: scanner should have updated file_hash"

    assert not os.path.exists(thumb_path), (
        "Scanner must invalidate the cached thumbnail when file content changes; "
        "leaving it on disk is how thumbnail/full-image mismatches get baked in."
    )
    assert not raw_variant.exists()
    assert not jpeg_variant.exists()


def test_rescan_clears_thumb_path_column_when_content_changes(tmp_path):
    """``photos.thumb_path`` must be NULLed alongside the on-disk thumbnail
    when ``_invalidate_derived_caches`` runs, mirroring the
    ``working_copy_path`` and ``preview_cache`` cleanup. The column is the
    Thumbnails & Previews plan card's fast proxy
    (``count_photos_missing_thumb``); leaving it populated after the JPEG
    is gone re-introduces the phantom "Already done" pill the f16722b /
    storage-clear fix already eliminated for the storage-UI path.
    """
    from db import Database
    from scanner import scan
    from thumbnails import generate_thumbnail

    root = str(tmp_path / "photos")
    os.makedirs(root)
    img_path = os.path.join(root, "photo.jpg")
    Image.new("RGB", (800, 600), color=(255, 0, 0)).save(img_path, "JPEG")

    vireo_dir = tmp_path / "vireo"
    vireo_dir.mkdir()
    cache_dir = vireo_dir / "thumbnails"
    cache_dir.mkdir()

    db = Database(str(vireo_dir / "test.db"))
    scan(root, db, vireo_dir=str(vireo_dir))
    photo_id = db.get_photos(per_page=100)[0]["id"]

    # Generate a thumbnail and stamp the column the way thumbnail_stage does.
    generate_thumbnail(photo_id, img_path, str(cache_dir))
    db.conn.execute(
        "UPDATE photos SET thumb_path = ? WHERE id = ?",
        (f"{photo_id}.jpg", photo_id),
    )
    db.conn.commit()

    # Replace the source so the next incremental scan invalidates derived caches.
    time.sleep(0.05)
    Image.new("RGB", (800, 600), color=(0, 0, 255)).save(img_path, "JPEG")
    scan(root, db, incremental=True, vireo_dir=str(vireo_dir))

    row = db.conn.execute(
        "SELECT thumb_path FROM photos WHERE id = ?", (photo_id,),
    ).fetchone()
    assert row["thumb_path"] is None, (
        "Scanner removed the thumbnail file but left photos.thumb_path "
        "populated; the pipeline-plan substage will report 'Already done' "
        "for work that the next pipeline run actually performs."
    )


def test_rescan_invalidates_preview_cache_rows_when_file_content_changes(tmp_path):
    """preview_cache LRU rows must be removed alongside preview files when
    a photo's content changes, or total_bytes accounting reports ghost
    bytes for files that no longer exist and quota eviction starts
    targeting valid previews.
    """
    from db import Database
    from scanner import scan

    root = str(tmp_path / "photos")
    os.makedirs(root)
    img_path = os.path.join(root, "photo.jpg")
    Image.new("RGB", (800, 600), color=(255, 0, 0)).save(img_path, "JPEG")

    vireo_dir = tmp_path / "vireo"
    vireo_dir.mkdir()
    preview_dir = vireo_dir / "previews"
    preview_dir.mkdir()

    db = Database(str(vireo_dir / "test.db"))
    scan(root, db, vireo_dir=str(vireo_dir))

    photo_id = db.get_photos(per_page=100)[0]["id"]

    # Seed a preview file + accounting row, as /photos/<id>/preview would.
    preview_file = preview_dir / f"{photo_id}_1920.jpg"
    Image.new("RGB", (1920, 1440), color=(255, 0, 0)).save(str(preview_file), "JPEG")
    originals_dir = vireo_dir / "originals"
    originals_dir.mkdir()
    display_file = originals_dir / f"{photo_id}.display.jpg"
    Image.new("RGB", (800, 600), color=(255, 0, 0)).save(display_file, "JPEG")
    file_bytes = preview_file.stat().st_size
    db.preview_cache_insert(photo_id, 1920, file_bytes)
    assert db.preview_cache_total_bytes() == file_bytes

    # Replace source pixels → new file_hash → invalidation should fire.
    time.sleep(0.05)
    Image.new("RGB", (800, 600), color=(0, 0, 255)).save(img_path, "JPEG")
    scan(root, db, incremental=True, vireo_dir=str(vireo_dir))

    assert not preview_file.exists(), "preview file should be deleted"
    assert not display_file.exists(), "RAW display cache should be deleted"
    assert db.preview_cache_get(photo_id, 1920) is None, (
        "preview_cache row must be deleted alongside the file; "
        "leaving it inflates preview_cache_total_bytes and triggers "
        "unnecessary eviction of valid previews."
    )
    assert db.preview_cache_total_bytes() == 0


def test_rescan_keeps_preview_cache_row_when_file_unlink_fails(tmp_path, monkeypatch):
    """If a preview file can't be deleted (e.g. locked on Windows), the
    matching preview_cache row must stay. Otherwise the serve path's
    lazy-adoption shortcut (app.py ~L8131) re-adopts the stale file on
    the next /photos/<id>/preview and hands out pre-change content, and
    quota eviction stops accounting for the leaked bytes.
    """
    from db import Database
    from scanner import scan

    root = str(tmp_path / "photos")
    os.makedirs(root)
    img_path = os.path.join(root, "photo.jpg")
    Image.new("RGB", (800, 600), color=(255, 0, 0)).save(img_path, "JPEG")

    vireo_dir = tmp_path / "vireo"
    vireo_dir.mkdir()
    preview_dir = vireo_dir / "previews"
    preview_dir.mkdir()

    db = Database(str(vireo_dir / "test.db"))
    scan(root, db, vireo_dir=str(vireo_dir))
    photo_id = db.get_photos(per_page=100)[0]["id"]

    preview_file = preview_dir / f"{photo_id}_1920.jpg"
    Image.new("RGB", (1920, 1440), color=(255, 0, 0)).save(str(preview_file), "JPEG")
    file_bytes = preview_file.stat().st_size
    db.preview_cache_insert(photo_id, 1920, file_bytes)

    # Simulate the preview file being un-removable (locked, ACL, etc.).
    real_remove = os.remove
    stuck = str(preview_file)

    def selective_remove(path):
        if os.fspath(path) == stuck:
            raise PermissionError("simulated lock")
        real_remove(path)

    monkeypatch.setattr(os, "remove", selective_remove)

    # Force a content-change rescan so invalidation fires.
    time.sleep(0.05)
    Image.new("RGB", (800, 600), color=(0, 0, 255)).save(img_path, "JPEG")
    scan(root, db, incremental=True, vireo_dir=str(vireo_dir))

    assert preview_file.exists(), "sanity: stuck preview file should remain"
    assert db.preview_cache_get(photo_id, 1920) is not None, (
        "When preview unlink fails, the cache row must stay so quota "
        "accounting keeps the leaked bytes visible and the serve path "
        "does not lazy-adopt stale content."
    )


def test_rescan_sweeps_untracked_preview_files(tmp_path):
    """Legacy preview files that pre-date preview_cache accounting (no
    row) must still be cleaned up when a photo's content changes.
    Otherwise app.py's lazy-adoption path (~L8131) re-adopts them on
    the next /photos/<id>/preview request and hands out pre-change
    bytes.
    """
    from db import Database
    from scanner import scan

    root = str(tmp_path / "photos")
    os.makedirs(root)
    img_path = os.path.join(root, "photo.jpg")
    Image.new("RGB", (800, 600), color=(255, 0, 0)).save(img_path, "JPEG")

    vireo_dir = tmp_path / "vireo"
    vireo_dir.mkdir()
    preview_dir = vireo_dir / "previews"
    preview_dir.mkdir()

    db = Database(str(vireo_dir / "test.db"))
    scan(root, db, vireo_dir=str(vireo_dir))
    photo_id = db.get_photos(per_page=100)[0]["id"]

    # Seed an *untracked* preview (file exists, no preview_cache row).
    untracked = preview_dir / f"{photo_id}_800.jpg"
    Image.new("RGB", (800, 600), color=(255, 0, 0)).save(str(untracked), "JPEG")
    assert db.preview_cache_get(photo_id, 800) is None

    time.sleep(0.05)
    Image.new("RGB", (800, 600), color=(0, 0, 255)).save(img_path, "JPEG")
    scan(root, db, incremental=True, vireo_dir=str(vireo_dir))

    assert not untracked.exists(), (
        "Untracked preview files for invalidated photos must be swept "
        "or serve's lazy-adoption path re-adopts stale pre-change bytes."
    )


def test_rescan_preview_sweep_is_batched_not_per_photo(tmp_path, monkeypatch):
    """The untracked-preview sweep must run at most once per scan,
    not once per invalidated photo. Per-photo os.listdir on the
    preview dir turns large rescans into O(N × M) work.
    """
    from db import Database
    from scanner import scan

    root = str(tmp_path / "photos")
    os.makedirs(root)
    paths = []
    for i in range(3):
        p = os.path.join(root, f"photo_{i}.jpg")
        Image.new("RGB", (800, 600), color=(255, i * 40, 0)).save(p, "JPEG")
        paths.append(p)

    vireo_dir = tmp_path / "vireo"
    vireo_dir.mkdir()
    preview_dir = vireo_dir / "previews"
    preview_dir.mkdir()

    db = Database(str(vireo_dir / "test.db"))
    scan(root, db, vireo_dir=str(vireo_dir))

    # Replace all three files' contents so each invalidation fires.
    time.sleep(0.05)
    for i, p in enumerate(paths):
        Image.new("RGB", (800, 600), color=(0, 0, 255 - i * 30)).save(p, "JPEG")

    preview_listings = []
    real_listdir = os.listdir

    def counting_listdir(path):
        if str(path).rstrip(os.sep).endswith("previews"):
            preview_listings.append(str(path))
        return real_listdir(path)

    monkeypatch.setattr(os, "listdir", counting_listdir)

    scan(root, db, incremental=True, vireo_dir=str(vireo_dir))

    assert len(preview_listings) <= 1, (
        f"previews/ must be enumerated at most once per scan regardless "
        f"of how many photos were invalidated; got {len(preview_listings)} "
        f"listings across 3 invalidations."
    )


def test_audit_import_untracked_invalidates_using_caller_vireo_dir(tmp_path):
    """audit.import_untracked must accept vireo_dir and forward it to
    scan() so invalidation hits the real cache root.

    The DB and thumb directory are independently configurable (--db vs
    --thumb-dir). A fallback that derives vireo_dir from ``db._db_path``
    touches the wrong filesystem when those flags diverge, leaving the
    actual thumbnails/previews stale. The caller knows the configured
    cache root; it must pass it.
    """
    from audit import import_untracked
    from db import Database
    from scanner import scan
    from thumbnails import generate_thumbnail

    # DB and thumb cache intentionally on different roots (simulates
    # --db /fast-ssd/vireo.db --thumb-dir /big-hdd/cache).
    db_dir = tmp_path / "dbstore"
    db_dir.mkdir()
    vireo_dir = tmp_path / "cache"
    vireo_dir.mkdir()
    (vireo_dir / "thumbnails").mkdir()

    root = tmp_path / "photos"
    root.mkdir()
    img = root / "photo.jpg"
    Image.new("RGB", (800, 600), color=(255, 0, 0)).save(str(img), "JPEG")

    db = Database(str(db_dir / "vireo.db"))
    scan(str(root), db, vireo_dir=str(vireo_dir))

    photo_id = db.get_photos(per_page=100)[0]["id"]
    thumb_path = vireo_dir / "thumbnails" / f"{photo_id}.jpg"
    generate_thumbnail(photo_id, str(img), str(vireo_dir / "thumbnails"))
    assert thumb_path.exists()

    time.sleep(0.05)
    Image.new("RGB", (800, 600), color=(0, 0, 255)).save(str(img), "JPEG")

    # Caller supplies the real cache root — no fallback guessing.
    import_untracked(db, [str(img)], vireo_dir=str(vireo_dir))

    assert not thumb_path.exists(), (
        "audit.import_untracked must invalidate the caller-provided "
        "cache root, not a path guessed from db._db_path."
    )


def test_preview_sweep_chunks_large_photo_id_sets(tmp_path):
    """_sweep_untracked_previews_for_photos must chunk its IN (...) query.

    Older SQLite builds cap bound parameters at 999
    (SQLITE_MAX_VARIABLE_NUMBER). A rescan that invalidates thousands
    of photos would otherwise raise ``too many SQL variables`` during
    post-processing and fail the whole scan.
    """
    import scanner
    from db import Database

    vireo_dir = tmp_path / "vireo"
    vireo_dir.mkdir()
    (vireo_dir / "previews").mkdir()

    db = Database(str(vireo_dir / "test.db"))

    real_conn = db.conn
    max_params_seen = 0

    class TrackingConn:
        def __init__(self, inner):
            self._inner = inner

        def execute(self, sql, params=()):
            nonlocal max_params_seen
            if isinstance(params, list | tuple):
                max_params_seen = max(max_params_seen, len(params))
            return self._inner.execute(sql, params)

        def executemany(self, sql, seq):
            return self._inner.executemany(sql, seq)

        def __getattr__(self, name):
            return getattr(self._inner, name)

    db.conn = TrackingConn(real_conn)

    scanner._sweep_untracked_previews_for_photos(
        db, str(vireo_dir), list(range(1, 2001)),
    )

    assert max_params_seen <= 999, (
        f"Preview sweep sent {max_params_seen} bound parameters in one "
        f"query; would crash on SQLite builds with "
        f"SQLITE_MAX_VARIABLE_NUMBER=999."
    )


def test_invalidation_honors_custom_thumb_cache_dir(tmp_path):
    """``--thumb-dir`` can point to any directory — not necessarily a
    ``thumbnails/`` subdirectory of the vireo data dir. Invalidation
    must target the caller-supplied thumb cache dir directly, not
    assume a ``vireo_dir/thumbnails/`` layout. Otherwise stale
    thumbnails survive and an unrelated sibling ``thumbnails/`` can
    have files removed.
    """
    from db import Database
    from scanner import scan
    from thumbnails import generate_thumbnail

    # Custom layout: thumb dir has a non-default basename, not adjacent
    # to a "thumbnails" subdir of vireo_dir.
    vireo_dir = tmp_path / "vireo"
    vireo_dir.mkdir()
    thumb_dir = tmp_path / "custom-thumbs"
    thumb_dir.mkdir()

    root = tmp_path / "photos"
    root.mkdir()
    img = root / "p.jpg"
    Image.new("RGB", (400, 300), color=(255, 0, 0)).save(str(img), "JPEG")

    db = Database(str(vireo_dir / "test.db"))
    scan(str(root), db,
         vireo_dir=str(vireo_dir),
         thumb_cache_dir=str(thumb_dir))
    photo_id = db.get_photos(per_page=100)[0]["id"]

    thumb = thumb_dir / f"{photo_id}.jpg"
    generate_thumbnail(photo_id, str(img), str(thumb_dir))
    assert thumb.exists()

    time.sleep(0.05)
    Image.new("RGB", (400, 300), color=(0, 0, 255)).save(str(img), "JPEG")

    scan(str(root), db, incremental=True,
         vireo_dir=str(vireo_dir),
         thumb_cache_dir=str(thumb_dir))

    assert not thumb.exists(), (
        "Invalidation must target the configured thumb_cache_dir, not "
        "the vireo_dir/thumbnails convention."
    )
    # Sanity: no accidental 'thumbnails/' directory got created either.
    assert not (vireo_dir / "thumbnails").exists()


def test_initial_scan_does_not_invoke_invalidation_for_new_photos(tmp_path, monkeypatch):
    """Brand-new rows have no derived caches to flush. Firing
    _invalidate_derived_caches for every new photo turns a 50k-file
    initial scan into 50k pointless UPDATE/commit round-trips and
    preview-sweep bookkeeping. Invalidation should only fire for rows
    that already existed before this scan.
    """
    import scanner
    from db import Database

    root = str(tmp_path / "photos")
    os.makedirs(root)
    for i in range(3):
        Image.new("RGB", (200, 150), color=(i * 80, 0, 0)).save(
            os.path.join(root, f"photo_{i}.jpg"), "JPEG",
        )

    vireo_dir = tmp_path / "vireo"
    vireo_dir.mkdir()

    calls = []
    real_invalidate = scanner._invalidate_derived_caches

    def counting_invalidate(db, vireo_dir_arg, photo_id, thumb_cache_dir=None):
        calls.append(photo_id)
        return real_invalidate(db, vireo_dir_arg, photo_id, thumb_cache_dir=thumb_cache_dir)

    monkeypatch.setattr(scanner, "_invalidate_derived_caches", counting_invalidate)

    db = Database(str(vireo_dir / "test.db"))
    scanner.scan(root, db, vireo_dir=str(vireo_dir))

    assert calls == [], (
        f"Invalidation fired {len(calls)} times for brand-new rows with "
        f"no derived caches to flush. On large initial scans that turns "
        f"into O(N) wasted SQL + commit round-trips."
    )


def test_rescan_invalidates_when_prev_file_hash_was_null(tmp_path):
    """Legacy photo rows predating file_hash tracking (or where prior
    hash computation failed) have file_hash=NULL. When a later rescan
    computes a concrete hash, derived caches written during the NULL
    era must be invalidated — we can't prove the bytes are unchanged,
    so safer to flush than leave a stale thumbnail in place.
    """
    from db import Database
    from scanner import scan
    from thumbnails import generate_thumbnail

    root = str(tmp_path / "photos")
    os.makedirs(root)
    img_path = os.path.join(root, "legacy.jpg")
    Image.new("RGB", (800, 600), color=(255, 0, 0)).save(img_path, "JPEG")

    vireo_dir = tmp_path / "vireo"
    vireo_dir.mkdir()
    cache_dir = vireo_dir / "thumbnails"
    cache_dir.mkdir()

    db = Database(str(vireo_dir / "test.db"))
    scan(root, db, vireo_dir=str(vireo_dir))
    photo_id = db.get_photos(per_page=100)[0]["id"]

    thumb_path = str(cache_dir / f"{photo_id}.jpg")
    generate_thumbnail(photo_id, img_path, str(cache_dir))
    assert os.path.exists(thumb_path)

    # Simulate a legacy row: wipe the hash as if it was recorded before
    # file_hash tracking existed.
    db.conn.execute("UPDATE photos SET file_hash = NULL WHERE id = ?", (photo_id,))
    # Also bump file_mtime backwards so incremental scan reprocesses the row.
    db.conn.execute("UPDATE photos SET file_mtime = 0 WHERE id = ?", (photo_id,))
    db.conn.commit()

    scan(root, db, incremental=True, vireo_dir=str(vireo_dir))

    new_hash = db.conn.execute(
        "SELECT file_hash FROM photos WHERE id = ?", (photo_id,)
    ).fetchone()[0]
    assert new_hash is not None, "sanity: rescan must populate the hash"

    assert not os.path.exists(thumb_path), (
        "Invalidation must fire on NULL → concrete transitions too; "
        "otherwise legacy rows keep stale derived caches forever."
    )


def test_rescan_invalidates_caches_when_file_truncated_to_zero(tmp_path):
    """Truncating a previously-hashed photo to zero bytes is a content
    change — derived thumbnails and working copies were rendered from
    the old non-empty bytes and no longer match what's on disk. The
    zero-byte branch clears ``file_hash`` (so the empty SHA never
    becomes a duplicate identity) which must NOT bypass the
    invalidation: otherwise the old thumbnail and working copy stay
    cached forever, even though the source file is empty.
    """
    from db import Database
    from scanner import scan
    from thumbnails import generate_thumbnail

    root = str(tmp_path / "photos")
    os.makedirs(root)
    img_path = os.path.join(root, "shot.jpg")
    Image.new("RGB", (800, 600), color=(255, 0, 0)).save(img_path, "JPEG")

    vireo_dir = tmp_path / "vireo"
    vireo_dir.mkdir()
    cache_dir = vireo_dir / "thumbnails"
    cache_dir.mkdir()

    db = Database(str(vireo_dir / "test.db"))
    scan(root, db, vireo_dir=str(vireo_dir))
    photo_id = db.get_photos(per_page=100)[0]["id"]

    thumb_path = str(cache_dir / f"{photo_id}.jpg")
    generate_thumbnail(photo_id, img_path, str(cache_dir))
    assert os.path.exists(thumb_path)

    # Replace with a zero-byte file. Bump mtime backwards so the
    # incremental scan actually reprocesses the row instead of taking
    # the file_unchanged fast path.
    os.truncate(img_path, 0)
    db.conn.execute(
        "UPDATE photos SET file_mtime = 0 WHERE id = ?", (photo_id,)
    )
    db.conn.commit()

    scan(root, db, incremental=True, vireo_dir=str(vireo_dir),
         thumb_cache_dir=str(cache_dir))

    new_hash = db.conn.execute(
        "SELECT file_hash, file_size FROM photos WHERE id = ?", (photo_id,)
    ).fetchone()
    assert new_hash["file_hash"] is None, (
        "sanity: zero-byte file must not carry an empty-SHA duplicate identity"
    )
    assert new_hash["file_size"] == 0

    assert not os.path.exists(thumb_path), (
        "Invalidation must fire on the truncate-to-zero transition; "
        "otherwise the thumbnail rendered from the original (non-empty) "
        "bytes stays cached even though the source file is now empty."
    )


def test_rescan_invalidates_caches_on_null_to_empty_transition(tmp_path):
    """Legacy/pre-hash rows have ``file_hash = NULL`` but may still carry
    thumbnails or working copies rendered from their old non-empty bytes.
    When such a file is later truncated to zero, the scanner clears
    ``file_hash`` (the empty SHA must not collide as a duplicate
    identity), so the prior ``prev_file_hash != file_hash`` guard alone
    couldn't see the transition (None → None). Cache invalidation must
    still fire in this case — otherwise the stale thumbnail rendered
    from the original non-empty bytes survives forever.
    """
    from db import Database
    from scanner import scan
    from thumbnails import generate_thumbnail

    root = str(tmp_path / "photos")
    os.makedirs(root)
    img_path = os.path.join(root, "shot.jpg")
    Image.new("RGB", (800, 600), color=(0, 255, 0)).save(img_path, "JPEG")

    vireo_dir = tmp_path / "vireo"
    vireo_dir.mkdir()
    cache_dir = vireo_dir / "thumbnails"
    cache_dir.mkdir()

    db = Database(str(vireo_dir / "test.db"))
    scan(root, db, vireo_dir=str(vireo_dir))
    photo_id = db.get_photos(per_page=100)[0]["id"]

    thumb_path = str(cache_dir / f"{photo_id}.jpg")
    generate_thumbnail(photo_id, img_path, str(cache_dir))
    assert os.path.exists(thumb_path)

    # Simulate a legacy pre-hash row whose file_hash was never recorded.
    # The actual on-disk bytes are still the original non-empty image,
    # but the DB has no hash baseline to compare against.
    db.conn.execute(
        "UPDATE photos SET file_hash = NULL WHERE id = ?", (photo_id,),
    )
    db.conn.commit()

    # Replace with a zero-byte file and force the incremental scan to
    # reprocess by clearing the stored mtime.
    os.truncate(img_path, 0)
    db.conn.execute(
        "UPDATE photos SET file_mtime = 0 WHERE id = ?", (photo_id,)
    )
    db.conn.commit()

    scan(root, db, incremental=True, vireo_dir=str(vireo_dir),
         thumb_cache_dir=str(cache_dir))

    row = db.conn.execute(
        "SELECT file_hash, file_size FROM photos WHERE id = ?", (photo_id,)
    ).fetchone()
    assert row["file_hash"] is None, (
        "sanity: zero-byte file must not carry an empty-SHA duplicate identity"
    )
    assert row["file_size"] == 0

    assert not os.path.exists(thumb_path), (
        "NULL → empty transition must invalidate derived caches; "
        "otherwise legacy rows keep the thumbnail rendered from their "
        "old non-empty bytes after the source file is truncated."
    )


def test_rescan_regenerates_working_copy_when_file_content_changes(tmp_path):
    """When a large JPEG's content changes, re-scan must invalidate the stale
    working copy so the subsequent extraction reflects current pixels."""
    from db import Database
    from scanner import scan

    root = str(tmp_path / "photos")
    os.makedirs(root)
    img_path = os.path.join(root, "big.jpg")
    # Larger than the default working_copy_max_size (4096) so a working copy is extracted.
    Image.new("RGB", (5000, 3000), color=(255, 0, 0)).save(img_path, "JPEG")

    vireo_dir = tmp_path / "vireo"
    vireo_dir.mkdir()

    db = Database(str(vireo_dir / "test.db"))
    scan(root, db, vireo_dir=str(vireo_dir))

    photos = db.get_photos(per_page=100)
    assert len(photos) == 1
    photo_id = photos[0]["id"]
    assert photos[0]["working_copy_path"] is not None
    wc_path = vireo_dir / photos[0]["working_copy_path"]
    assert wc_path.exists()

    # Record what the working copy looks like now (top-left pixel = red).
    with Image.open(wc_path) as img:
        assert img.convert("RGB").getpixel((0, 0))[0] > 200  # red channel dominant

    # Replace file content with a very different image.
    time.sleep(0.05)
    Image.new("RGB", (5000, 3000), color=(0, 0, 255)).save(img_path, "JPEG")

    scan(root, db, incremental=True, vireo_dir=str(vireo_dir))

    # Working copy should point to a regenerated file whose pixels match the new source.
    photos = db.get_photos(per_page=100)
    wc_rel = photos[0]["working_copy_path"]
    assert wc_rel is not None, "Scanner must re-set working_copy_path after invalidation"
    wc_path = vireo_dir / wc_rel
    assert wc_path.exists()
    with Image.open(wc_path) as img:
        pixel = img.convert("RGB").getpixel((0, 0))
    assert pixel[2] > 200 and pixel[0] < 100, (
        f"Working copy pixel {pixel} does not reflect updated (blue) source; "
        "stale extraction from pre-change content was served."
    )


def test_scan_mp_method_is_spawn_when_frozen(monkeypatch):
    """Frozen (PyInstaller) sidecars must use spawn, not forkserver.

    forkserver in a frozen bundle on macOS forks workers from a parent
    that has loaded PIL/Foundation; the worker's first Cocoa-touching
    call crashes the child and the parent sees EOFError on the handshake.
    spawn does fork+exec for a clean worker process.
    """
    import importlib

    import scanner

    monkeypatch.setattr(sys, "frozen", True, raising=False)
    reloaded = importlib.reload(scanner)
    try:
        assert reloaded._SCAN_MP_METHOD == "spawn"
    finally:
        # Restore the unfrozen module-level constant so other tests in
        # the same process see the dev-mode value.
        monkeypatch.delattr(sys, "frozen", raising=False)
        importlib.reload(scanner)


def test_scan_links_root_when_all_files_skipped(tmp_path):
    """Scanning a folder where every file is in skip_paths still links the
    folder to the active workspace.

    Repro for the bug where importing folders whose photos are already in
    the global photos table left those folders unlinked from the active
    workspace — the scanner's per-photo loop never ran, so _ensure_folder
    (which auto-links via db.add_folder) never fired.
    """
    from db import Database
    from scanner import scan

    root = str(tmp_path / "photos")
    _create_test_images(root, {'': ['a.jpg', 'b.jpg']})

    db = Database(str(tmp_path / "test.db"))
    ws_id = db._active_workspace_id

    skip = {os.path.join(root, 'a.jpg'), os.path.join(root, 'b.jpg')}
    scan(root, db, skip_paths=skip)

    linked = {f["path"] for f in db.get_workspace_folders(ws_id)}
    assert root in linked, (
        f"Folder {root} should be linked to the active workspace even when "
        f"all files were skipped. Linked folders: {linked}"
    )


def test_scan_links_restrict_dirs_when_all_files_skipped(tmp_path):
    """Scanning with restrict_dirs links each restrict_dir to the active
    workspace, even when 0 files in those dirs survive skip_paths.

    Mirrors the ingest path where ``do_scan`` is called with
    ``restrict_dirs`` containing folders that already hold duplicates of
    the source files; per the comment in pipeline_job.py, those folders
    must end up linked to the active workspace.
    """
    from db import Database
    from scanner import scan

    root = str(tmp_path / "photos")
    _create_test_images(root, {
        'sub1': ['a.jpg'],
        'sub2': ['b.jpg'],
    })

    db = Database(str(tmp_path / "test.db"))
    ws_id = db._active_workspace_id

    sub1 = os.path.join(root, 'sub1')
    sub2 = os.path.join(root, 'sub2')
    skip = {os.path.join(sub1, 'a.jpg'), os.path.join(sub2, 'b.jpg')}
    scan(root, db, skip_paths=skip, restrict_dirs=[sub1, sub2])

    linked = {f["path"] for f in db.get_workspace_folders(ws_id)}
    assert sub1 in linked and sub2 in linked, (
        f"Both restrict_dirs should be linked. Linked folders: {linked}"
    )


def test_scan_does_not_promote_discovered_descendants_to_workspace_roots(tmp_path):
    """Scanner-created child links should stay hidden behind the chosen root."""
    from db import Database
    from scanner import scan

    root = str(tmp_path / "USA2026")
    _create_test_images(root, {
        "day1": ["bird.jpg"],
    })

    db = Database(str(tmp_path / "test.db"))
    ws_id = db._active_workspace_id
    db.set_active_workspace(None)
    root_id = db.add_folder(root, name="USA2026")
    db.add_workspace_folder(ws_id, root_id)
    db.set_active_workspace(ws_id)

    scan(root, db)

    linked_paths = {f["path"] for f in db.get_workspace_folders(ws_id)}
    root_paths = [f["path"] for f in db.get_workspace_folder_roots(ws_id)]
    assert os.path.join(root, "day1") in linked_paths
    assert root_paths == [root]


def test_scan_promotes_top_level_target_to_workspace_root(tmp_path):
    """A normal scan should make its selected root visible in workspace APIs."""
    from db import Database
    from scanner import scan

    root = str(tmp_path / "photos")
    _create_test_images(root, {
        "day1": ["bird.jpg"],
    })

    db = Database(str(tmp_path / "test.db"))
    ws_id = db._active_workspace_id

    scan(root, db)

    linked_paths = {f["path"] for f in db.get_workspace_folders(ws_id)}
    root_paths = [f["path"] for f in db.get_workspace_folder_roots(ws_id)]
    assert os.path.join(root, "day1") in linked_paths
    assert root_paths == [root]


def test_restricted_scan_roots_subfolders_not_destination_base(tmp_path):
    """A templated import scans the destination *base* but only ingests the
    leaf subfolders it wrote into (passed as restrict_dirs). Those leaves —
    not the base — must become the workspace roots, so the new-images walk
    doesn't later treat un-imported siblings under the base as "new".

    Regression: importing into ``/Volumes/.../USA`` (a whole archive root)
    promoted ``USA`` to a workspace root, and the new-images detector then
    walked every un-imported shoot in the archive.
    """
    from db import Database
    from scanner import scan

    base = str(tmp_path / "USA")
    _create_test_images(base, {
        "2026-06-22": ["a.jpg", "b.jpg"],   # the import
        "2026-01-01": ["old.jpg"],          # a pre-existing sibling, NOT imported
    })
    imported = os.path.join(base, "2026-06-22")

    db = Database(str(tmp_path / "test.db"))
    ws_id = db._active_workspace_id

    scan(base, db, restrict_dirs=[imported])

    root_paths = [f["path"] for f in db.get_workspace_folder_roots(ws_id)]
    linked_paths = {f["path"] for f in db.get_workspace_folders(ws_id)}
    # The imported leaf is the user-facing root; the archive base is
    # NOT linked to the workspace. Before PR #1107's line-1186 fix the
    # base was linked as a non-root, which fired ``add_workspace_
    # folder``'s subtree cascade and would pull every pre-existing
    # cataloged descendant of the base into the workspace UI (unrelated
    # archive subtrees from prior scans / other workspaces). Now the
    # parent chain up to the scan root just exists in ``folders`` for
    # ``parent_id`` integrity, and ``get_folder_tree`` walks up through
    # the non-visible base via its recursive CTE so the imported leaf
    # still renders as a top-level sidebar entry.
    assert root_paths == [imported]
    assert base not in linked_paths
    # Ingestion stayed scoped to the restricted dir — the sibling shoot's
    # files were never pulled in.
    photo_paths = {
        os.path.join(r["folder_path"], r["filename"])
        for r in db.conn.execute(
            "SELECT f.path AS folder_path, p.filename "
            "FROM photos p JOIN folders f ON f.id = p.folder_id"
        ).fetchall()
    }
    assert os.path.join(imported, "a.jpg") in photo_paths
    assert os.path.join(base, "2026-01-01", "old.jpg") not in photo_paths


def test_restricted_scan_below_registered_root_does_not_promote_leaf(tmp_path):
    from db import Database
    from scanner import scan

    root = str(tmp_path / "registered")
    leaf = os.path.join(root, "trip")
    _create_test_images(root, {"trip": ["bird.jpg"]})
    db = Database(str(tmp_path / "test.db"))
    ws_id = db._active_workspace_id
    db.add_folder(root, name="registered")

    scan(
        root,
        db,
        restrict_dirs=[leaf],
        restrict_files={os.path.join(leaf, "bird.jpg")},
        register_restrict_dirs_as_roots=False,
    )

    root_paths = [f["path"] for f in db.get_workspace_folder_roots(ws_id)]
    linked_paths = {f["path"] for f in db.get_workspace_folders(ws_id)}
    assert root_paths == [root]
    assert leaf in linked_paths


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX permissions required")
def test_scan_surfaces_permission_denied_subdirs(tmp_path):
    """A subdir the kernel won't let us enter must surface as a denied path,
    not silently disappear into a 'Found 0 images' lie. Accessible siblings
    must still be discovered.

    Regression: the scan of /Volumes/Photography/.../2026-05-01 returned
    'Found 0 images' for a folder containing 1122 NEFs because macOS TCC
    blocked Vireo's process. Path.rglob("*") swallows PermissionError per
    directory; switching to os.walk(onerror=...) lets us see them.
    """
    from db import Database
    from scanner import scan

    root = str(tmp_path / "photos")
    _create_test_images(root, {
        '': ['ok.jpg'],
        'forbidden': ['hidden.jpg'],
    })

    forbidden = os.path.join(root, 'forbidden')
    os.chmod(forbidden, 0o000)
    try:
        db = Database(str(tmp_path / "test.db"))
        denied = []
        scan(root, db, permission_error_callback=denied.append)

        photos = {p['filename'] for p in db.get_photos(per_page=100)}
        assert 'ok.jpg' in photos, "accessible files must still be scanned"
        assert 'hidden.jpg' not in photos, "denied dir must not yield photos"
        assert any(forbidden in p for p in denied), (
            f"denied path not reported via callback. got: {denied}"
        )
    finally:
        os.chmod(forbidden, 0o755)


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX permissions required")
def test_scan_continues_after_permission_denied(tmp_path):
    """A denied subtree must not abort the scan — sibling subtrees keep being
    walked. This is the partial-discovery property: a single locked-down
    folder shouldn't poison an entire run."""
    from db import Database
    from scanner import scan

    root = str(tmp_path / "photos")
    _create_test_images(root, {
        'a': ['a.jpg'],
        'b': ['b.jpg'],
        'c': ['c.jpg'],
    })

    locked = os.path.join(root, 'b')
    os.chmod(locked, 0o000)
    try:
        db = Database(str(tmp_path / "test.db"))
        denied = []
        scan(root, db, permission_error_callback=denied.append)

        photos = {p['filename'] for p in db.get_photos(per_page=100)}
        assert photos == {'a.jpg', 'c.jpg'}
        assert any(locked in p for p in denied)
    finally:
        os.chmod(locked, 0o755)


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX symlinks required")
def test_scan_skips_broken_symlinks(tmp_path):
    """A dangling symlink with a supported extension must not abort the scan.

    Regression: switching from Path.rglob (which used is_file() — False for
    broken symlinks) to os.walk (which lists broken symlinks in `filenames`)
    caused dangling *.jpg symlinks to be queued for processing. The pre-pass
    then called image_path.stat() and raised FileNotFoundError, killing the
    whole scan. os.walk-mode must filter non-files like the rglob path did.
    """
    from db import Database
    from scanner import scan

    root = str(tmp_path / "photos")
    _create_test_images(root, {'': ['real.jpg']})
    # Create a dangling symlink whose target does not exist.
    os.symlink(
        os.path.join(root, "missing-target.jpg"),
        os.path.join(root, "broken.jpg"),
    )

    db = Database(str(tmp_path / "test.db"))
    # Must not raise FileNotFoundError; must still scan the real file.
    scan(root, db)

    photos = {p['filename'] for p in db.get_photos(per_page=100)}
    assert 'real.jpg' in photos
    assert 'broken.jpg' not in photos


def test_scan_restrict_dirs_surfaces_permission_denied(tmp_path, monkeypatch):
    """The pipeline copy-mode path drives scan() with restrict_dirs. A denied
    directory in that list must surface via permission_error_callback and not
    abort the whole scan — accessible siblings must still be discovered.

    Regression: permission_error_callback was only wired into the os.walk
    branch (restrict_dirs is None). With restrict_dirs set, dp.iterdir()
    raised PermissionError unhandled and the entire pipeline scan stage
    failed with no PERMISSION_DENIED entry in job["errors"].

    Mocks os.scandir for the locked subtree (rather than chmod 0o000) so
    the assertion holds regardless of test-runner uid: chmod-based denial
    silently bypasses for root, which would let the locked file slip in.
    Targets ``image_loader.os.scandir`` because ``safe_iter_dir`` (the
    restrict_dirs enumerator) calls ``os.scandir`` directly — patching
    ``Path.iterdir`` would miss the actual code path.
    """
    import errno as _errno

    import image_loader
    from db import Database
    from scanner import scan

    root = str(tmp_path / "photos")
    _create_test_images(root, {
        'allowed': ['ok.jpg'],
        'forbidden': ['hidden.jpg'],
    })
    allowed = os.path.join(root, 'allowed')
    forbidden = os.path.join(root, 'forbidden')

    real_scandir = image_loader.os.scandir

    def fake_scandir(path):
        if str(path) == forbidden:
            raise PermissionError(
                _errno.EACCES, "Permission denied", str(path),
            )
        return real_scandir(path)

    monkeypatch.setattr(image_loader.os, "scandir", fake_scandir)

    db = Database(str(tmp_path / "test.db"))
    denied = []
    # Must not raise; must surface the denied dir via the callback.
    scan(
        root, db,
        restrict_dirs=[allowed, forbidden],
        permission_error_callback=denied.append,
    )

    photos = {p['filename'] for p in db.get_photos(per_page=100)}
    assert 'ok.jpg' in photos, (
        "accessible restrict_dir entries must still be scanned"
    )
    assert 'hidden.jpg' not in photos, (
        "denied restrict_dir must not yield photos"
    )
    assert any(forbidden in p for p in denied), (
        f"denied restrict_dir not reported via callback. got: {denied}"
    )


def test_scan_restrict_dirs_raises_permission_error_without_callback(
    tmp_path, monkeypatch,
):
    """Without ``permission_error_callback``, a denied restrict_dir must still
    raise PermissionError — *not* be silently swallowed.

    Regression: pipeline_job's repair scan calls ``do_scan(..., restrict_dirs=
    [folder_path])`` without a callback and wraps it in
    ``except (OSError, RuntimeError)`` to count unreachable folders. An
    earlier fix caught PermissionError and unconditionally `continue`d,
    which silently turned denied folders into "successfully repaired" —
    a black-box regression. Partial-success is opt-in via the callback;
    every other caller must keep the loud failure semantics they had.

    Patches ``image_loader.os.scandir`` because ``safe_iter_dir`` (the
    restrict_dirs enumerator) calls ``os.scandir`` — patching
    ``Path.iterdir`` would miss the actual code path.
    """
    import errno as _errno

    import image_loader
    from db import Database
    from scanner import scan

    root = str(tmp_path / "photos")
    _create_test_images(root, {'forbidden': ['hidden.jpg']})
    forbidden = os.path.join(root, 'forbidden')

    real_scandir = image_loader.os.scandir

    def fake_scandir(path):
        if str(path) == forbidden:
            raise PermissionError(
                _errno.EACCES, "Permission denied", str(path),
            )
        return real_scandir(path)

    monkeypatch.setattr(image_loader.os, "scandir", fake_scandir)

    db = Database(str(tmp_path / "test.db"))
    with pytest.raises(PermissionError):
        scan(root, db, restrict_dirs=[forbidden])


def test_scan_recursive_raises_permission_error_without_callback(
    tmp_path, monkeypatch,
):
    """Without ``permission_error_callback``, a denied subdir during a
    recursive walk must raise PermissionError. The os.walk ``onerror``
    callback re-raises so the failure is visible to the caller — silent
    "Found 0 images" is the very black-box behavior this stack was
    refactored to eliminate.
    """
    import errno as _errno

    from db import Database
    from scanner import scan

    root = str(tmp_path / "photos")
    _create_test_images(root, {'': ['ok.jpg'], 'forbidden': ['hidden.jpg']})
    forbidden = os.path.join(root, 'forbidden')

    real_scandir = os.scandir

    def fake_scandir(path, *args, **kwargs):
        if os.fspath(path) == forbidden:
            raise PermissionError(
                _errno.EACCES, "Permission denied", forbidden,
            )
        return real_scandir(path, *args, **kwargs)

    monkeypatch.setattr(os, "scandir", fake_scandir)

    db = Database(str(tmp_path / "test.db"))
    with pytest.raises(PermissionError):
        scan(root, db)
