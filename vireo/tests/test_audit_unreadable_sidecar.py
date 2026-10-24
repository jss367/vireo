"""The Drift check names sidecars it cannot read and offers only resolutions that work.

Before #2218 a corrupt or unreadable sidecar was read as empty, so Drift listed
it as "every keyword removed in XMP", and "Use XMP" untagged the photo. Since
#2218 the readers return None and ``check_drift`` skipped the photo, so the
check said "No drift" for a sidecar it never compared. Now each one is a row
of its own:

- corrupt (not valid XMP): only "Use DB" works. The next Sync to XMP writes a
  fresh sidecar from the catalog's keywords in place of the damaged one.
- unreadable (permission denied, I/O error): neither works. Sync cannot write
  over a file it cannot open, and there is nothing to read.
"""

import os
import sys

import pytest
from PIL import Image

CORRUPT_XMP = "<x:xmpmeta xmlns:x='adobe:ns:meta/'><broken"

needs_posix_permissions = pytest.mark.skipif(
    sys.platform == "win32" or (hasattr(os, "geteuid") and os.geteuid() == 0),
    reason="chmod 000 does not make a file unreadable on Windows or as root",
)


@pytest.fixture
def scanned(tmp_path):
    """A scanned photo tagged Sparrow by its sidecar; yields (db, photo_id, xmp_path)."""
    from db import Database
    from scanner import scan
    from xmp import write_sidecar

    root = tmp_path / "photos"
    root.mkdir()
    Image.new("RGB", (100, 100)).save(root / "bird.jpg")
    xmp_path = str(root / "bird.xmp")
    write_sidecar(xmp_path, flat_keywords={"Sparrow"}, hierarchical_keywords=set())
    db = Database(str(tmp_path / "test.db"))
    scan(str(root), db)
    photo_id = db.photos.list_page(per_page=10)[0]["id"]
    yield db, photo_id, xmp_path
    os.chmod(xmp_path, 0o644)
    db.close()


def _corrupt(xmp_path):
    with open(xmp_path, "w") as f:
        f.write(CORRUPT_XMP)


def test_corrupt_sidecar_is_reported_as_unreadable_with_only_use_db(scanned):
    from audit import check_drift

    db, photo_id, xmp_path = scanned
    _corrupt(xmp_path)

    [row] = check_drift(db)
    assert row["photo_id"] == photo_id
    assert row["direction"] == "sidecar_unreadable"
    assert row["xmp_problem"] == "corrupt"
    assert row["xmp_error"].startswith("it is not valid XMP (")
    assert row["xmp_value"] is None
    # Nothing is claimed about which keywords the sidecar holds.
    assert row["added_in_xmp"] == [] and row["removed_in_xmp"] == []
    assert row["db_value"] == ["Sparrow"]
    assert row["resolutions"] == ["use_db"]


def test_corrupt_sidecar_with_empty_db_keywords_offers_no_resolution(tmp_path):
    """``resolve_drift('use_db')`` queues one write per DB keyword; with none it
    queues nothing and the next sync never rewrites the sidecar. The row says
    so rather than advertising a repair that would silently be a no-op."""
    from audit import check_drift
    from db import Database
    from scanner import scan

    root = tmp_path / "photos"
    root.mkdir()
    Image.new("RGB", (100, 100)).save(root / "bird.jpg")
    db = Database(str(tmp_path / "test.db"))
    try:
        scan(str(root), db)
        xmp_path = str(root / "bird.xmp")
        with open(xmp_path, "w") as f:
            f.write(CORRUPT_XMP)

        [row] = check_drift(db)
        assert row["direction"] == "sidecar_unreadable"
        assert row["xmp_problem"] == "corrupt"
        assert row["db_value"] == []
        assert row["resolutions"] == []
        assert row["advice"] == (
            "This photo has no keywords in Vireo, so Use DB would not rewrite "
            "the sidecar. Delete the sidecar file to clear the drift, or add "
            "keywords to the photo first."
        )
    finally:
        db.close()


@needs_posix_permissions
def test_unopenable_sidecar_is_reported_with_no_resolution(scanned):
    from audit import check_drift

    db, _photo_id, xmp_path = scanned
    os.chmod(xmp_path, 0)

    [row] = check_drift(db)
    assert row["direction"] == "sidecar_unreadable"
    assert row["xmp_problem"] == "unreadable"
    assert row["xmp_error"] == "it could not be opened (Permission denied)"
    assert row["resolutions"] == []


def test_readable_drift_offers_both_resolutions(scanned):
    from audit import check_drift
    from xmp import write_sidecar

    db, _photo_id, xmp_path = scanned
    os.unlink(xmp_path)
    write_sidecar(xmp_path, flat_keywords={"Cardinal"}, hierarchical_keywords=set())

    [row] = check_drift(db)
    assert row["direction"] == "both"
    assert row["resolutions"] == ["use_db", "use_xmp"]
    assert "xmp_error" not in row


def test_use_db_replaces_a_corrupt_sidecar_on_the_next_sync(scanned):
    """The one resolution offered for a corrupt sidecar actually resolves it."""
    from audit import check_drift, resolve_drift
    from sync import sync_to_xmp
    from xmp import read_keywords

    db, photo_id, xmp_path = scanned
    _corrupt(xmp_path)

    resolve_drift(db, photo_id, "use_db")
    db.commit()
    assert sync_to_xmp(db)["ok"] is True
    assert read_keywords(xmp_path) == {"Sparrow"}
    assert check_drift(db) == []


def _point_folder_at(db, folder_path, tmp_path):
    root = tmp_path / "library"
    root.mkdir()
    db.conn.execute("UPDATE folders SET path = ? WHERE path = ?", (str(root), folder_path))
    db.conn.commit()
    return root


def test_resolve_refuses_a_direction_the_row_does_not_offer(app_and_db, tmp_path):
    app, db = app_and_db
    root = _point_folder_at(db, "/photos/2024", tmp_path)
    Image.new("RGB", (100, 100)).save(root / "bird1.jpg")
    (root / "bird1.xmp").write_text(CORRUPT_XMP)
    photo_id = db.conn.execute("SELECT id FROM photos WHERE filename = 'bird1.jpg'").fetchone()["id"]
    client = app.test_client()

    resp = client.post("/api/audit/resolve", json={"photo_id": photo_id, "direction": "use_xmp"})
    assert resp.status_code == 409
    body = resp.get_json()
    assert body["code"] == "drift_resolution_unavailable"
    assert body["error"].startswith(
        "Vireo could not read the XMP sidecar for bird1.jpg: it is not valid XMP ("
    )
    assert body["message"] == (
        "Use DB replaces it with the database's keywords at the next Sync to XMP. "
        "Use XMP cannot apply: there is nothing in it Vireo can read."
    )

    resp = client.post("/api/audit/resolve", json={"photo_id": photo_id, "direction": "use_db"})
    assert resp.status_code == 200


def test_resolve_refuses_use_db_for_corrupt_sidecar_with_empty_db_keywords(app_and_db, tmp_path):
    """``use_db`` on a corrupt sidecar whose photo has no catalog keywords is a
    silent no-op (nothing is queued, the sync never rewrites the sidecar); the
    row offers no resolution and the API refuses the attempt with the advice
    telling the user what to do instead."""
    app, db = app_and_db
    root = _point_folder_at(db, "/photos/2024", tmp_path)
    Image.new("RGB", (100, 100)).save(root / "bird3.jpg")
    (root / "bird3.xmp").write_text(CORRUPT_XMP)
    photo_id = db.conn.execute("SELECT id FROM photos WHERE filename = 'bird3.jpg'").fetchone()["id"]
    # bird3 is created by the ``app_and_db`` fixture without any keyword tags.
    assert db.photo_keywords.get_for_photo(photo_id) == []
    client = app.test_client()

    resp = client.post("/api/audit/resolve", json={"photo_id": photo_id, "direction": "use_db"})
    assert resp.status_code == 409
    body = resp.get_json()
    assert body["code"] == "drift_resolution_unavailable"
    assert body["error"].startswith(
        "Vireo could not read the XMP sidecar for bird3.jpg: it is not valid XMP ("
    )
    assert body["message"] == (
        "This photo has no keywords in Vireo, so Use DB would not rewrite "
        "the sidecar. Delete the sidecar file to clear the drift, or add "
        "keywords to the photo first."
    )

    resp = client.post("/api/audit/resolve", json={"photo_id": photo_id, "direction": "use_xmp"})
    assert resp.status_code == 409


def test_resolve_all_skips_rows_that_do_not_offer_the_direction(app_and_db, tmp_path):
    app, db = app_and_db
    root = _point_folder_at(db, "/photos/2024", tmp_path)
    Image.new("RGB", (100, 100)).save(root / "bird1.jpg")
    (root / "bird1.xmp").write_text(CORRUPT_XMP)
    client = app.test_client()

    drift = client.get("/api/audit/drift").get_json()
    unreadable = [d for d in drift if d["direction"] == "sidecar_unreadable"]
    assert [d["filename"] for d in unreadable] == ["bird1.jpg"]

    body = client.post("/api/audit/resolve-all", json={"direction": "use_xmp"}).get_json()
    assert body["resolved"] == len(drift) - 1
    assert body["skipped"] == 1
    assert body["warning"] == (
        "Sync All from XMP left 1 photo alone: Vireo could not read its XMP "
        "sidecar. Its row on the Drift tab says why."
    )

    body = client.post("/api/audit/resolve-all", json={"direction": "use_db"}).get_json()
    assert "skipped" not in body and "warning" not in body
