# vireo/tests/test_importer.py
import os
import sqlite3
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'lr-migration'))



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
