"""A sidecar whose keywords a scan could not import is reported, not dropped.

A companion JPEG's XMP sidecar that failed to import was logged (or, when it
could not be read, not even that), so its keywords went missing without a
word. A photo's own unreadable sidecar was deferred to the next scan the same
silent way. The scanner now reports each one through ``ScanHooks.sidecar_error``
and the scan job fails, naming the photo, per the job rollup rule.
"""

import os

import pytest
from PIL import Image
from scanner import ScanHooks, ScanOptions
from test_scanner import _PairedCatalog

CORRUPT_XMP = "<x:xmpmeta xmlns:x='adobe:ns:meta/'><broken"
UNREADABLE = "Vireo could not read it, or it is not valid XMP"


def _companion_with_own_sidecar(cat, xmp_text):
    """Rename the pair's JPEG so its sidecar is its own (not the RAW's)."""
    other = cat.img_dir / "OTHER.jpg"
    cat.jpeg.rename(other)
    cat.db.conn.execute("UPDATE photos SET companion_path=? WHERE id=?", (other.name, cat.raw_id))
    cat.db.conn.execute(
        "UPDATE companion_identities SET filename=? WHERE photo_id=?", (other.name, cat.raw_id),
    )
    cat.db.conn.commit()
    (cat.img_dir / "OTHER.xmp").write_text(xmp_text)
    return other


def _scan(cat, incremental):
    reported = []
    cat.scanner.scan(
        str(cat.img_dir), cat.db,
        options=ScanOptions(
            incremental=incremental, vireo_dir=str(cat.vireo_dir),
            thumb_cache_dir=str(cat.thumb_dir),
        ),
        hooks=ScanHooks(sidecar_error=lambda *args: reported.append(args)),
    )
    return reported


@pytest.mark.parametrize("incremental", [True, False])
def test_unreadable_companion_sidecar_is_reported(tmp_path, monkeypatch, incremental):
    cat = _PairedCatalog(tmp_path, monkeypatch)
    _companion_with_own_sidecar(cat, CORRUPT_XMP)

    reported = _scan(cat, incremental)

    assert reported == [(str(cat.img_dir / "OTHER.xmp"), "OTHER.jpg", UNREADABLE)]


def test_companion_sidecar_import_error_is_reported(tmp_path, monkeypatch):
    import scanner

    cat = _PairedCatalog(tmp_path, monkeypatch)
    _companion_with_own_sidecar(cat, CORRUPT_XMP)

    def conflict(*_args, **_kwargs):
        raise ValueError("invented location conflict")

    monkeypatch.setattr(scanner.companion_keep, "_import_keywords_for_photo", conflict)

    reported = _scan(cat, incremental=True)

    assert reported == [(str(cat.img_dir / "OTHER.xmp"), "OTHER.jpg", "invented location conflict")]


@pytest.mark.parametrize("incremental", [True, False])
def test_unreadable_primary_sidecar_is_reported_once(tmp_path, incremental):
    from db import Database
    from scanner import scan

    root = tmp_path / "photos"
    root.mkdir()
    Image.new("RGB", (60, 40), "blue").save(root / "heron.jpg")
    (root / "heron.xmp").write_text(CORRUPT_XMP)
    db = Database(str(tmp_path / "t.db"))
    reported = []
    hooks = ScanHooks(sidecar_error=lambda *args: reported.append(args))

    scan(str(root), db, hooks=hooks)
    assert reported == [(str(root / "heron.xmp"), "heron.jpg", UNREADABLE)]

    # Still unread, so the next scan retries it and says so again.
    reported.clear()
    scan(str(root), db, options=ScanOptions(incremental=incremental), hooks=hooks)
    assert reported == [(str(root / "heron.xmp"), "heron.jpg", UNREADABLE)]


def test_readable_sidecars_report_nothing(tmp_path):
    from db import Database
    from scanner import scan
    from xmp import write_sidecar

    root = tmp_path / "photos"
    root.mkdir()
    Image.new("RGB", (60, 40), "blue").save(root / "heron.jpg")
    write_sidecar(str(root / "heron.xmp"), flat_keywords={"Great Blue Heron"}, hierarchical_keywords=set())
    reported = []

    scan(str(root), Database(str(tmp_path / "t.db")),
         hooks=ScanHooks(sidecar_error=lambda *args: reported.append(args)))

    assert reported == []


def test_scan_job_fails_and_names_the_photo(app_and_db, tmp_path):
    from wait import wait_for_job_via_client

    app, _db = app_and_db
    root = tmp_path / "scanme"
    root.mkdir()
    Image.new("RGB", (60, 40), "blue").save(root / "heron.jpg")
    Image.new("RGB", (60, 40), "red").save(root / "egret.jpg")
    (root / "heron.xmp").write_text(CORRUPT_XMP)
    client = app.test_client()

    job_id = client.post("/api/jobs/scan", json={"root": str(root)}).get_json()["job_id"]
    job = wait_for_job_via_client(client, job_id)

    assert job["status"] == "failed", job
    assert job["errors"] == [
        f"heron.jpg: keywords in {root / 'heron.xmp'} were not imported "
        f"({UNREADABLE}). Vireo tries again on the next scan."
    ]
    # Every photo was still cataloged and the thumbnail step still ran.
    assert job["result"]["photos_indexed"] == 2
    steps = {step["id"]: step for step in job["steps"]}
    assert steps["scan"]["status"] == "failed"
    assert steps["scan"]["summary"] == "2 photos (keywords from 1 sidecar not imported)"
    assert steps["thumbnails"]["status"] == "completed"
    assert os.path.exists(root / "heron.xmp")
