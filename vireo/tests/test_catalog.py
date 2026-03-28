import os
import sqlite3
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))


def create_test_catalog(path):
    """Create a minimal .lrcat SQLite database with test data."""
    conn = sqlite3.connect(path)
    c = conn.cursor()

    c.execute("""CREATE TABLE AgLibraryRootFolder (
        id_local INTEGER PRIMARY KEY,
        id_global UNIQUE NOT NULL,
        absolutePath UNIQUE NOT NULL DEFAULT '',
        name NOT NULL DEFAULT '',
        relativePathFromCatalog
    )""")

    c.execute("""CREATE TABLE AgLibraryFolder (
        id_local INTEGER PRIMARY KEY,
        id_global UNIQUE NOT NULL,
        parentId INTEGER,
        pathFromRoot NOT NULL DEFAULT '',
        rootFolder INTEGER NOT NULL DEFAULT 0,
        visibility INTEGER
    )""")

    c.execute("""CREATE TABLE AgLibraryFile (
        id_local INTEGER PRIMARY KEY,
        id_global UNIQUE NOT NULL,
        baseName NOT NULL DEFAULT '',
        extension NOT NULL DEFAULT '',
        folder INTEGER NOT NULL DEFAULT 0,
        idx_filename NOT NULL DEFAULT '',
        lc_idx_filename NOT NULL DEFAULT '',
        lc_idx_filenameExtension NOT NULL DEFAULT '',
        originalFilename NOT NULL DEFAULT '',
        sidecarExtensions
    )""")

    c.execute("""CREATE TABLE Adobe_images (
        id_local INTEGER PRIMARY KEY,
        id_global UNIQUE NOT NULL,
        rootFile INTEGER
    )""")

    c.execute("""CREATE TABLE AgLibraryKeyword (
        id_local INTEGER PRIMARY KEY,
        id_global UNIQUE NOT NULL,
        dateCreated NOT NULL DEFAULT '',
        genealogy NOT NULL DEFAULT '',
        includeOnExport INTEGER NOT NULL DEFAULT 1,
        includeParents INTEGER NOT NULL DEFAULT 1,
        lc_name,
        name,
        parent INTEGER
    )""")

    c.execute("""CREATE TABLE AgLibraryKeywordImage (
        id_local INTEGER PRIMARY KEY,
        image INTEGER NOT NULL DEFAULT 0,
        tag INTEGER NOT NULL DEFAULT 0
    )""")

    # Root folder
    c.execute("INSERT INTO AgLibraryRootFolder VALUES (1, 'rf1', '/Volumes/Photography/Raw Files/Australia/', 'Australia', NULL)")

    # Folders
    c.execute("INSERT INTO AgLibraryFolder VALUES (1, 'f1', NULL, '', 1, NULL)")
    c.execute("INSERT INTO AgLibraryFolder VALUES (2, 'f2', NULL, '2024/', 1, NULL)")
    c.execute("INSERT INTO AgLibraryFolder VALUES (3, 'f3', NULL, '2024/January/', 1, NULL)")

    # Files
    c.execute("INSERT INTO AgLibraryFile VALUES (10, 'fi1', 'DSC_0001', 'NEF', 2, 'DSC_0001.NEF', 'dsc_0001.nef', 'dsc_0001.nef', 'DSC_0001.NEF', NULL)")
    c.execute("INSERT INTO AgLibraryFile VALUES (11, 'fi2', 'DSC_0002', 'NEF', 3, 'DSC_0002.NEF', 'dsc_0002.nef', 'dsc_0002.nef', 'DSC_0002.NEF', NULL)")

    # Adobe_images (link image id to file id)
    c.execute("INSERT INTO Adobe_images VALUES (100, 'ai1', 10)")
    c.execute("INSERT INTO Adobe_images VALUES (101, 'ai2', 11)")

    # Keywords hierarchy:
    #   (root, id=1) -> Birds (id=2) -> Raptors (id=3) -> Black kite (id=4)
    #                                                   -> Whistling kite (id=5)
    #                 -> Mammals (id=6) -> Koala (id=7)
    c.execute("INSERT INTO AgLibraryKeyword VALUES (1, 'k1', '', '', 0, 0, '', '', NULL)")
    c.execute("INSERT INTO AgLibraryKeyword VALUES (2, 'k2', '', '', 1, 1, 'birds', 'Birds', 1)")
    c.execute("INSERT INTO AgLibraryKeyword VALUES (3, 'k3', '', '', 1, 1, 'raptors', 'Raptors', 2)")
    c.execute("INSERT INTO AgLibraryKeyword VALUES (4, 'k4', '', '', 1, 1, 'black kite', 'Black kite', 3)")
    c.execute("INSERT INTO AgLibraryKeyword VALUES (5, 'k5', '', '', 1, 1, 'whistling kite', 'Whistling kite', 3)")
    c.execute("INSERT INTO AgLibraryKeyword VALUES (6, 'k6', '', '', 1, 1, 'mammals', 'Mammals', 1)")
    c.execute("INSERT INTO AgLibraryKeyword VALUES (7, 'k7', '', '', 1, 1, 'koala', 'Koala', 6)")

    # Keyword assignments:
    #   Image 100 (DSC_0001) -> Black kite
    #   Image 101 (DSC_0002) -> Koala, Whistling kite
    c.execute("INSERT INTO AgLibraryKeywordImage VALUES (1, 100, 4)")
    c.execute("INSERT INTO AgLibraryKeywordImage VALUES (2, 101, 7)")
    c.execute("INSERT INTO AgLibraryKeywordImage VALUES (3, 101, 5)")

    conn.commit()
    conn.close()


def test_read_catalog_keywords():
    """read_catalog returns a dict mapping file paths to keyword data."""
    from catalog import read_catalog

    with tempfile.TemporaryDirectory() as tmpdir:
        cat_path = os.path.join(tmpdir, "test.lrcat")
        create_test_catalog(cat_path)

        result = read_catalog(cat_path)

        # Should have 2 files
        assert len(result) == 2

        # Check DSC_0001 -> Black kite with hierarchy
        path1 = "/Volumes/Photography/Raw Files/Australia/2024/DSC_0001.NEF"
        assert path1 in result
        assert "Black kite" in result[path1]["flat_keywords"]
        assert "Raptors" in result[path1]["flat_keywords"]
        assert "Birds" in result[path1]["flat_keywords"]
        assert any("Black kite" in h for h in result[path1]["hierarchical_keywords"])

        # Check DSC_0002 -> Koala + Whistling kite
        path2 = "/Volumes/Photography/Raw Files/Australia/2024/January/DSC_0002.NEF"
        assert path2 in result
        assert "Koala" in result[path2]["flat_keywords"]
        assert "Whistling kite" in result[path2]["flat_keywords"]


def test_keyword_hierarchy_includes_parents():
    """When includeParents=1, hierarchical keywords include parent names."""
    from catalog import read_catalog

    with tempfile.TemporaryDirectory() as tmpdir:
        cat_path = os.path.join(tmpdir, "test.lrcat")
        create_test_catalog(cat_path)

        result = read_catalog(cat_path)
        path1 = "/Volumes/Photography/Raw Files/Australia/2024/DSC_0001.NEF"

        # Black kite's hierarchy should be Birds|Raptors|Black kite
        assert "Birds|Raptors|Black kite" in result[path1]["hierarchical_keywords"]


def test_skips_non_exportable_keywords():
    """Keywords with includeOnExport=0 are skipped."""
    from catalog import read_catalog

    with tempfile.TemporaryDirectory() as tmpdir:
        cat_path = os.path.join(tmpdir, "test.lrcat")
        create_test_catalog(cat_path)

        result = read_catalog(cat_path)
        path1 = "/Volumes/Photography/Raw Files/Australia/2024/DSC_0001.NEF"

        # The unnamed root keyword (includeOnExport=0) should not appear
        for kw in result[path1]["flat_keywords"]:
            assert kw != ""
