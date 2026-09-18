# vireo/tests/test_importer.py
import os
import sqlite3
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))



def _create_test_catalog(path, root_path, photos_with_keywords):
    """Create a minimal .lrcat SQLite database.

    Args:
        path: where to write the .lrcat file
        root_path: absolute path prefix for files
        photos_with_keywords: list of (filename, folder_suffix, [(keyword_name, parent_name)])
    """
    conn = sqlite3.connect(path)
    c = conn.cursor()

    c.execute("""CREATE TABLE AgLibraryRootFolder (
        id_local INTEGER PRIMARY KEY, id_global UNIQUE NOT NULL,
        absolutePath UNIQUE NOT NULL DEFAULT '', name NOT NULL DEFAULT '',
        relativePathFromCatalog)""")
    c.execute("""CREATE TABLE AgLibraryFolder (
        id_local INTEGER PRIMARY KEY, id_global UNIQUE NOT NULL,
        parentId INTEGER, pathFromRoot NOT NULL DEFAULT '',
        rootFolder INTEGER NOT NULL DEFAULT 0, visibility INTEGER)""")
    c.execute("""CREATE TABLE AgLibraryFile (
        id_local INTEGER PRIMARY KEY, id_global UNIQUE NOT NULL,
        baseName NOT NULL DEFAULT '', extension NOT NULL DEFAULT '',
        folder INTEGER NOT NULL DEFAULT 0,
        idx_filename NOT NULL DEFAULT '', lc_idx_filename NOT NULL DEFAULT '',
        lc_idx_filenameExtension NOT NULL DEFAULT '',
        originalFilename NOT NULL DEFAULT '', sidecarExtensions)""")
    c.execute("""CREATE TABLE Adobe_images (
        id_local INTEGER PRIMARY KEY, id_global UNIQUE NOT NULL,
        rootFile INTEGER)""")
    c.execute("""CREATE TABLE AgLibraryKeyword (
        id_local INTEGER PRIMARY KEY, id_global UNIQUE NOT NULL,
        dateCreated NOT NULL DEFAULT '', genealogy NOT NULL DEFAULT '',
        includeOnExport INTEGER NOT NULL DEFAULT 1,
        includeParents INTEGER NOT NULL DEFAULT 1,
        lc_name, name, parent INTEGER)""")
    c.execute("""CREATE TABLE AgLibraryKeywordImage (
        id_local INTEGER PRIMARY KEY, image INTEGER NOT NULL DEFAULT 0,
        tag INTEGER NOT NULL DEFAULT 0)""")

    # Root folder
    c.execute("INSERT INTO AgLibraryRootFolder VALUES (1, 'rf1', ?, 'Root', NULL)", (root_path,))

    # Create folders and files
    folder_ids = {}
    file_id = 10
    image_id = 100
    keyword_id = 100
    kwimage_id = 1
    keyword_ids = {}  # name -> id

    for fname, folder_suffix, kws in photos_with_keywords:
        # Ensure folder exists
        if folder_suffix not in folder_ids:
            fid = len(folder_ids) + 1
            folder_ids[folder_suffix] = fid
            c.execute("INSERT INTO AgLibraryFolder VALUES (?, ?, NULL, ?, 1, NULL)",
                      (fid, f'f{fid}', folder_suffix))

        fid = folder_ids[folder_suffix]
        base, ext = os.path.splitext(fname)
        ext = ext.lstrip('.')

        c.execute("INSERT INTO AgLibraryFile VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)",
                  (file_id, f'fi{file_id}', base, ext, fid,
                   fname, fname.lower(), fname.lower(), fname))
        c.execute("INSERT INTO Adobe_images VALUES (?, ?, ?)",
                  (image_id, f'ai{image_id}', file_id))

        # Create keywords and assign
        for kw_name, parent_name in kws:
            # Create parent if needed
            if parent_name and parent_name not in keyword_ids:
                keyword_ids[parent_name] = keyword_id
                c.execute("INSERT INTO AgLibraryKeyword VALUES (?, ?, '', '', 1, 1, ?, ?, NULL)",
                          (keyword_id, f'k{keyword_id}', parent_name.lower(), parent_name))
                keyword_id += 1

            parent_kid = keyword_ids.get(parent_name)
            if kw_name not in keyword_ids:
                keyword_ids[kw_name] = keyword_id
                c.execute("INSERT INTO AgLibraryKeyword VALUES (?, ?, '', '', 1, 1, ?, ?, ?)",
                          (keyword_id, f'k{keyword_id}', kw_name.lower(), kw_name, parent_kid))
                keyword_id += 1

            kid = keyword_ids[kw_name]
            c.execute("INSERT INTO AgLibraryKeywordImage VALUES (?, ?, ?)",
                      (kwimage_id, image_id, kid))
            kwimage_id += 1

        file_id += 1
        image_id += 1

    conn.commit()
    conn.close()


def test_preview_catalog(tmp_path):
    """preview_catalog returns summary statistics."""
    from db import Database
    from importer import preview_catalog

    root = str(tmp_path / "photos") + '/'
    os.makedirs(root)
    # Create actual files so they're "found"
    # Create a placeholder file (can't save PIL as .NEF)
    with open(os.path.join(root, 'DSC_0001.NEF'), 'wb') as f:
        f.write(b'\x00' * 100)

    cat_path = str(tmp_path / "test.lrcat")
    _create_test_catalog(cat_path, root, [
        ('DSC_0001.NEF', '', [('Cardinal', 'Birds')]),
        ('DSC_0002.NEF', '', [('Sparrow', 'Birds')]),
    ])

    db = Database(str(tmp_path / "test.db"))
    result = preview_catalog(cat_path, db)

    assert result['total_files'] == 2
    assert result['matched_files'] >= 1  # DSC_0001 exists
    assert result['unmatched_files'] >= 1  # DSC_0002 doesn't exist


def test_execute_import_populates_db(tmp_path):
    """execute_import imports keywords into the database."""
    from db import Database
    from importer import execute_import

    root = str(tmp_path / "photos") + '/'
    os.makedirs(root)
    # Create a placeholder file (can't save PIL as .NEF)
    with open(os.path.join(root, 'DSC_0001.NEF'), 'wb') as f:
        f.write(b'\x00' * 100)

    cat_path = str(tmp_path / "test.lrcat")
    _create_test_catalog(cat_path, root, [
        ('DSC_0001.NEF', '', [('Cardinal', 'Birds')]),
    ])

    db = Database(str(tmp_path / "test.db"))

    # Add photo to DB directly (scanner can't read fake NEF)
    fid = db.add_folder(root, name='photos')
    db.add_photo(folder_id=fid, filename='DSC_0001.NEF', extension='.nef',
                 file_size=100, file_mtime=1.0)

    result = execute_import([cat_path], db, write_xmp=False)
    assert result['imported'] >= 1

    # Check keywords were imported
    photos = db.get_photos()
    if photos:
        kws = db.get_photo_keywords(photos[0]['id'])
        kw_names = {k['name'] for k in kws}
        assert 'Cardinal' in kw_names
        cardinal_source = db.conn.execute(
            """SELECT pk.source
               FROM photo_keywords pk
               JOIN keywords k ON k.id = pk.keyword_id
               WHERE pk.photo_id = ? AND k.name = 'Cardinal'""",
            (photos[0]['id'],),
        ).fetchone()
        assert cardinal_source["source"] == "manual"


def test_execute_import_skips_empty_normalized_keywords(tmp_path):
    """A catalog keyword whose name normalizes to `""` must be dropped, not
    aborting the import.

    add_keyword() rejects names that normalize to empty (a lone smart quote,
    stray whitespace). Without a caller-side filter here, the ValueError
    would propagate to execute_import's try/except and mark the whole photo
    as failed; the review contract is to skip the malformed catalog keyword
    and continue.
    """
    from db import Database
    from importer import execute_import

    root = str(tmp_path / "photos") + '/'
    os.makedirs(root)
    with open(os.path.join(root, 'DSC_0001.NEF'), 'wb') as f:
        f.write(b'\x00' * 100)

    cat_path = str(tmp_path / "test.lrcat")
    # `'` normalizes to `""`. The importer must skip it and still tag
    # `Cardinal`.
    _create_test_catalog(cat_path, root, [
        ('DSC_0001.NEF', '', [("'", 'Birds'), ('Cardinal', 'Birds')]),
    ])

    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder(root, name='photos')
    db.add_photo(folder_id=fid, filename='DSC_0001.NEF', extension='.nef',
                 file_size=100, file_mtime=1.0)

    result = execute_import([cat_path], db, write_xmp=False)

    assert result['failed'] == 0
    assert result['imported'] >= 1
    photos = db.get_photos()
    assert photos
    kw_names = {k['name'] for k in db.get_photo_keywords(photos[0]['id'])}
    assert 'Cardinal' in kw_names
    assert "'" not in kw_names


def test_execute_import_writes_normalized_keywords_to_xmp(tmp_path):
    """When write_xmp=True the sidecar must be built from the same
    normalized keyword set the DB got. Otherwise a catalog value like
    `‘apapane` stores a clean `apapane` DB row but writes a stray-quote
    `<rdf:li>` to XMP, and a later sync/import diff would treat the two
    as different keywords.
    """
    from db import Database
    from importer import execute_import

    root = str(tmp_path / "photos") + '/'
    os.makedirs(root)
    with open(os.path.join(root, 'DSC_0001.NEF'), 'wb') as f:
        f.write(b'\x00' * 100)

    cat_path = str(tmp_path / "test.lrcat")
    # Edge-quote flat + hierarchical keywords and a lone `'` that
    # normalizes to `""` (must be dropped from the sidecar too).
    _create_test_catalog(cat_path, root, [
        ('DSC_0001.NEF', '', [
            ('‘apapane', None),
            ("'", None),
            ('juvenile', '‘apapane'),
        ]),
    ])

    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder(root, name='photos')
    db.add_photo(folder_id=fid, filename='DSC_0001.NEF', extension='.nef',
                 file_size=100, file_mtime=1.0)

    result = execute_import([cat_path], db, write_xmp=True)
    assert result['imported'] >= 1

    xmp_path = os.path.join(root, 'DSC_0001.xmp')
    assert os.path.exists(xmp_path)
    with open(xmp_path, encoding='utf-8') as f:
        xmp_body = f.read()
    # The clean normalized spelling reaches the sidecar…
    assert 'apapane' in xmp_body
    # …and the stray-quote and empty-normalized variants do not.
    assert '‘apapane' not in xmp_body
    assert '<rdf:li></rdf:li>' not in xmp_body
    assert "<rdf:li>'</rdf:li>" not in xmp_body


def test_preview_import_detects_conflicts(tmp_path):
    """preview_import flags files that appear in multiple catalogs."""
    from db import Database
    from importer import preview_import

    root = str(tmp_path / "photos") + '/'
    os.makedirs(root)
    # Create a placeholder file (can't save PIL as .NEF)
    with open(os.path.join(root, 'DSC_0001.NEF'), 'wb') as f:
        f.write(b'\x00' * 100)

    cat1 = str(tmp_path / "cat1.lrcat")
    cat2 = str(tmp_path / "cat2.lrcat")
    _create_test_catalog(cat1, root, [
        ('DSC_0001.NEF', '', [('Cardinal', 'Birds')]),
    ])
    _create_test_catalog(cat2, root, [
        ('DSC_0001.NEF', '', [('Blue jay', 'Birds')]),
    ])

    db = Database(str(tmp_path / "test.db"))
    result = preview_import([cat1, cat2], db)

    assert result['conflict_count'] >= 1


def _cased_apart(root):
    """The same directory as `root`, spelled the way a second catalog might."""
    return root.replace('/photos/', '/PHOTOS/')


def test_execute_import_prefers_last_across_cased_paths(tmp_path, monkeypatch):
    """Two catalogs spelling one photo apart in case are a single entry.

    On Windows the same file is reachable as `D:/Pictures/a.nef` and
    `d:/pictures/a.nef`. Group those apart and both entries resolve to the one
    photo, so prefer_last imports the earlier catalog's keywords too instead of
    letting the last one win. Windows' normalizers are patched in because the
    POSIX ones are identities and the mismatch is otherwise unreachable here.
    """
    import ntpath

    import importer
    from db import Database
    from importer import execute_import

    root = str(tmp_path / "photos") + '/'
    os.makedirs(root)
    cat1 = str(tmp_path / "cat1.lrcat")
    cat2 = str(tmp_path / "cat2.lrcat")
    _create_test_catalog(cat1, root, [('DSC_0001.NEF', '', [('Cardinal', None)])])
    _create_test_catalog(cat2, _cased_apart(root), [('DSC_0001.NEF', '', [('Blue jay', None)])])

    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder(root, name='photos')
    pid = db.add_photo(folder_id=fid, filename='DSC_0001.NEF', extension='.nef',
                       file_size=100, file_mtime=1.0)

    monkeypatch.setattr(importer.os.path, 'normpath', ntpath.normpath)
    monkeypatch.setattr(importer.os.path, 'normcase', ntpath.normcase)
    result = execute_import([cat1, cat2], db, write_xmp=False, strategy='prefer_last')

    assert result['imported'] == 1
    names = {k['name'] for k in db.get_photo_keywords(pid)}
    assert 'Blue jay' in names
    assert 'Cardinal' not in names


def test_preview_import_detects_conflicts_across_cased_paths(tmp_path, monkeypatch):
    """The preview must report that cased-apart spellings are one file.

    Otherwise two catalogs that both claim the photo look like two untroubled
    singletons and the user is never told there is a conflict to resolve.
    """
    import ntpath

    import importer
    from db import Database
    from importer import preview_import

    root = str(tmp_path / "photos") + '/'
    os.makedirs(root)
    cat1 = str(tmp_path / "cat1.lrcat")
    cat2 = str(tmp_path / "cat2.lrcat")
    _create_test_catalog(cat1, root, [('DSC_0001.NEF', '', [('Cardinal', None)])])
    _create_test_catalog(cat2, _cased_apart(root), [('DSC_0001.NEF', '', [('Blue jay', None)])])

    db = Database(str(tmp_path / "test.db"))
    monkeypatch.setattr(importer.os.path, 'normpath', ntpath.normpath)
    monkeypatch.setattr(importer.os.path, 'normcase', ntpath.normcase)
    result = preview_import([cat1, cat2], db)

    assert result['conflict_count'] == 1
    conflict = result['conflicts'][0]
    assert conflict['file_path'].endswith('DSC_0001.NEF')
    assert set(conflict['keywords_by_catalog']) == {'cat1', 'cat2'}


def _windows_paths(monkeypatch):
    """Make importer path handling behave the way it does on Windows.

    normcase and normpath are identities on POSIX, so nothing about case
    folding is observable from this runner without them.
    """
    import ntpath

    import importer

    monkeypatch.setattr(importer.os.path, 'normpath', ntpath.normpath)
    monkeypatch.setattr(importer.os.path, 'normcase', ntpath.normcase)


def test_execute_import_keeps_case_distinct_photos_apart(tmp_path, monkeypatch):
    """Two photos that differ only in case each keep their own keywords.

    A Windows directory with per-directory case sensitivity enabled can hold
    both Bird.NEF and bird.NEF. Case folding is what lets a catalog path find
    its photo at all, but folding these two together would hand one photo both
    catalogs' keywords and leave the other untouched, so an exact spelling has
    to win whenever there is one.
    """
    from db import Database
    from importer import execute_import

    root = str(tmp_path / "photos") + '/'
    os.makedirs(root)
    cat_path = str(tmp_path / "test.lrcat")
    _create_test_catalog(cat_path, root, [
        ('Bird.NEF', '', [('Cardinal', None)]),
        ('bird.NEF', '', [('Blue jay', None)]),
    ])

    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder(root, name='photos')
    upper = db.add_photo(folder_id=fid, filename='Bird.NEF', extension='.nef',
                         file_size=100, file_mtime=1.0)
    lower = db.add_photo(folder_id=fid, filename='bird.NEF', extension='.nef',
                         file_size=100, file_mtime=1.0)

    _windows_paths(monkeypatch)
    result = execute_import([cat_path], db, write_xmp=False)

    assert result['imported'] == 2
    assert {k['name'] for k in db.get_photo_keywords(upper)} == {'Cardinal'}
    assert {k['name'] for k in db.get_photo_keywords(lower)} == {'Blue jay'}


def test_execute_import_skips_a_path_that_folds_onto_two_photos(tmp_path, monkeypatch):
    """An inexact path matching two case-apart photos must tag neither.

    Folding is a fallback for finding the one photo a catalog means. When it
    names two, there is no answer, and picking one at random would tag the
    wrong photo.
    """
    from db import Database
    from importer import execute_import

    root = str(tmp_path / "photos") + '/'
    os.makedirs(root)
    cat_path = str(tmp_path / "test.lrcat")
    _create_test_catalog(cat_path, root, [('BIRD.NEF', '', [('Cardinal', None)])])

    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder(root, name='photos')
    upper = db.add_photo(folder_id=fid, filename='Bird.NEF', extension='.nef',
                         file_size=100, file_mtime=1.0)
    lower = db.add_photo(folder_id=fid, filename='bird.NEF', extension='.nef',
                         file_size=100, file_mtime=1.0)

    _windows_paths(monkeypatch)
    result = execute_import([cat_path], db, write_xmp=False)

    assert result['imported'] == 0
    assert result['skipped'] == 1
    assert db.get_photo_keywords(upper) == []
    assert db.get_photo_keywords(lower) == []


def test_preview_import_keeps_two_case_distinct_files_apart(tmp_path, monkeypatch):
    """Cased-apart spellings of two real files are not one conflict.

    The filesystem is what decides: on a case-insensitive directory the two
    spellings open the same file, and on a case-sensitive one they do not.
    samefile stands in for the latter here, which macOS and Linux runners
    cannot produce on their own.
    """
    import importer
    from db import Database
    from importer import preview_import

    root = str(tmp_path / "photos") + '/'
    os.makedirs(root)
    cat1 = str(tmp_path / "cat1.lrcat")
    cat2 = str(tmp_path / "cat2.lrcat")
    _create_test_catalog(cat1, root, [('Bird.NEF', '', [('Cardinal', None)])])
    _create_test_catalog(cat2, root, [('bird.NEF', '', [('Blue jay', None)])])

    db = Database(str(tmp_path / "test.db"))
    _windows_paths(monkeypatch)
    monkeypatch.setattr(importer.os.path, 'samefile', lambda one, other: False)
    result = preview_import([cat1, cat2], db)

    assert result['conflict_count'] == 0
