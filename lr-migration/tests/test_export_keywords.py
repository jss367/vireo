import os
import sys
import tempfile
from xml.etree import ElementTree as ET

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

from test_catalog_reader import create_test_catalog


NS = {
    "x": "adobe:ns:meta/",
    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "dc": "http://purl.org/dc/elements/1.1/",
    "lr": "http://ns.adobe.com/lightroom/1.0/",
}


def test_end_to_end_dry_run(capsys):
    """Dry run processes catalogs and prints stats without writing files."""
    from export_keywords import run

    with tempfile.TemporaryDirectory() as tmpdir:
        # Create catalog
        cat_path = os.path.join(tmpdir, "test.lrcat")
        create_test_catalog(cat_path)

        # Create fake photo files so path resolution works
        photos_root = os.path.join(tmpdir, "photos")
        aus_dir = os.path.join(photos_root, "Australia", "2024", "January")
        os.makedirs(os.path.join(photos_root, "Australia", "2024"), exist_ok=True)
        os.makedirs(aus_dir, exist_ok=True)
        open(os.path.join(photos_root, "Australia", "2024", "DSC_0001.NEF"), "w").close()
        open(os.path.join(aus_dir, "DSC_0002.NEF"), "w").close()

        # Patch the root folder path in the catalog to point to our temp dir
        import sqlite3
        conn = sqlite3.connect(cat_path)
        conn.execute(
            "UPDATE AgLibraryRootFolder SET absolutePath = ?",
            (os.path.join(photos_root, "Australia") + "/",),
        )
        conn.commit()
        conn.close()

        stats = run(catalogs=[cat_path], photos_root=photos_root, write=False)

        assert stats["files_with_keywords"] == 2
        assert stats["sidecars_written"] == 0  # dry run


def test_end_to_end_write():
    """Write mode creates XMP sidecars next to image files."""
    from export_keywords import run

    with tempfile.TemporaryDirectory() as tmpdir:
        cat_path = os.path.join(tmpdir, "test.lrcat")
        create_test_catalog(cat_path)

        photos_root = os.path.join(tmpdir, "photos")
        aus_2024 = os.path.join(photos_root, "Australia", "2024")
        aus_jan = os.path.join(aus_2024, "January")
        os.makedirs(aus_2024, exist_ok=True)
        os.makedirs(aus_jan, exist_ok=True)
        open(os.path.join(aus_2024, "DSC_0001.NEF"), "w").close()
        open(os.path.join(aus_jan, "DSC_0002.NEF"), "w").close()

        import sqlite3
        conn = sqlite3.connect(cat_path)
        conn.execute(
            "UPDATE AgLibraryRootFolder SET absolutePath = ?",
            (os.path.join(photos_root, "Australia") + "/",),
        )
        conn.commit()
        conn.close()

        stats = run(catalogs=[cat_path], photos_root=photos_root, write=True)

        assert stats["sidecars_written"] == 2

        # Verify sidecar content
        xmp_path = os.path.join(aus_2024, "DSC_0001.xmp")
        assert os.path.exists(xmp_path)
        tree = ET.parse(xmp_path)
        root = tree.getroot()
        subjects = root.findall(".//dc:subject/rdf:Bag/rdf:li", NS)
        values = {s.text for s in subjects}
        assert "Black kite" in values
