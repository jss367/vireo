"""Working copies for imports and scans reached through a symlinked folder."""

import os

from PIL import Image
from wait import wait_for_job_via_client


def _fake_raw_tools(monkeypatch):
    import scanner

    monkeypatch.setattr(scanner.scan_run, "extract_metadata", lambda paths, **kwargs: {})

    def fake_extract(_source, output, **_kwargs):
        os.makedirs(os.path.dirname(output), exist_ok=True)
        Image.new("RGB", (64, 48), "green").save(output, "JPEG")
        return True

    monkeypatch.setattr(scanner.working_copy_batch, "extract_working_copy", fake_extract)


def _symlinked_raw_folder(tmp_path):
    real = tmp_path / "real-card"
    real.mkdir()
    (real / "IMG_0001.nef").write_bytes(b"fake raw data")
    link = tmp_path / "linked-card"
    link.symlink_to(real, target_is_directory=True)
    return real, link


def _working_copies_under(db, root):
    """``{cataloged path: has a working copy}`` for photos below ``root``."""
    rows = db.conn.execute(
        "SELECT f.path, p.filename, p.working_copy_path"
        " FROM photos p JOIN folders f ON f.id = p.folder_id"
    ).fetchall()
    return {
        os.path.join(r["path"], r["filename"]): bool(r["working_copy_path"])
        for r in rows
        if r["path"].startswith(str(root))
    }


def test_in_place_import_through_symlink_creates_working_copies(app_and_db, tmp_path, monkeypatch):
    app, db = app_and_db
    _fake_raw_tools(monkeypatch)
    real, link = _symlinked_raw_folder(tmp_path)

    client = app.test_client()
    resp = client.post("/api/jobs/import-in-place", json={
        "sources": [str(link)],
        "after_import": None,
    })
    assert resp.status_code == 200, resp.get_json()
    job = wait_for_job_via_client(client, resp.get_json()["job_id"])
    assert job["status"] == "completed", job

    assert _working_copies_under(db, tmp_path) == {
        os.path.join(str(real), "IMG_0001.nef"): True,
    }


def _run(client, url, body):
    resp = client.post(url, json=body)
    assert resp.status_code == 200, resp.get_json()
    job = wait_for_job_via_client(client, resp.get_json()["job_id"])
    assert job["status"] == "completed", job
    return job


def test_in_place_import_through_symlink_non_recursive(app_and_db, tmp_path, monkeypatch):
    app, db = app_and_db
    _fake_raw_tools(monkeypatch)
    real, link = _symlinked_raw_folder(tmp_path)

    _run(app.test_client(), "/api/jobs/import-in-place", {
        "sources": [str(link)], "recursive": False, "after_import": None,
    })

    assert _working_copies_under(db, tmp_path) == {
        os.path.join(str(real), "IMG_0001.nef"): True,
    }


def test_scan_through_symlink_creates_working_copies(app_and_db, tmp_path, monkeypatch):
    app, db = app_and_db
    _fake_raw_tools(monkeypatch)
    real, link = _symlinked_raw_folder(tmp_path)

    _run(app.test_client(), "/api/jobs/scan", {"root": str(link)})

    assert _working_copies_under(db, tmp_path) == {
        os.path.join(str(real), "IMG_0001.nef"): True,
    }


def test_import_full_in_place_through_symlink(app_and_db, tmp_path, monkeypatch):
    app, db = app_and_db
    _fake_raw_tools(monkeypatch)
    real, link = _symlinked_raw_folder(tmp_path)

    job = _run(app.test_client(), "/api/jobs/import-full", {
        "source": str(link), "copy": False, "file_types": [".nef"],
    })

    assert _working_copies_under(db, tmp_path) == {
        os.path.join(str(real), "IMG_0001.nef"): True,
    }
    assert job["result"]["collection_id"] is not None


def _card_and_symlinked_archive(tmp_path):
    card = tmp_path / "card"
    card.mkdir()
    (card / "IMG_0001.nef").write_bytes(b"fake raw data")
    archive = tmp_path / "archive"
    archive.mkdir()
    link = tmp_path / "archive-link"
    link.symlink_to(archive, target_is_directory=True)
    return card, archive, link


def test_import_full_copy_into_symlinked_destination(app_and_db, tmp_path, monkeypatch):
    app, db = app_and_db
    _fake_raw_tools(monkeypatch)
    card, archive, link = _card_and_symlinked_archive(tmp_path)

    job = _run(app.test_client(), "/api/jobs/import-full", {
        "source": str(card), "destination": str(link), "copy": True,
        "file_types": [".nef"],
    })

    copies = _working_copies_under(db, archive)
    assert len(copies) == 1 and all(copies.values()), copies
    assert job["result"]["collection_id"] is not None, job["result"]


def test_import_photos_into_symlinked_destination(app_and_db, tmp_path, monkeypatch):
    app, db = app_and_db
    _fake_raw_tools(monkeypatch)
    card, archive, link = _card_and_symlinked_archive(tmp_path)

    _run(app.test_client(), "/api/jobs/import-photos", {
        "sources": [str(card)], "destination": str(link), "after_import": None,
    })

    copies = _working_copies_under(db, archive)
    assert len(copies) == 1 and all(copies.values()), copies
