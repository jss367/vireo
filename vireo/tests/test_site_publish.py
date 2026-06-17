import json

from PIL import Image
from wait import wait_for_job_via_client


def _seed_publish_app(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    import config as cfg
    import models
    from app import create_app
    from db import Database

    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    monkeypatch.setattr(
        models, "DEFAULT_MODELS_DIR", str(tmp_path / "vireo-models"),
    )
    monkeypatch.setattr(models, "CONFIG_PATH", str(tmp_path / "models.json"))

    photos_dir = tmp_path / "photos"
    photos_dir.mkdir()
    Image.new("RGB", (1200, 800), (180, 30, 40)).save(photos_dir / "cardinal.jpg")
    Image.new("RGB", (900, 900), (90, 120, 40)).save(photos_dir / "sparrow.jpg")
    Image.new("RGB", (1000, 700), (40, 80, 150)).save(photos_dir / "mystery.jpg")

    vireo_dir = tmp_path / "vireo"
    thumb_dir = vireo_dir / "thumbs"
    thumb_dir.mkdir(parents=True)
    db_path = str(vireo_dir / "vireo.db")

    db = Database(db_path)
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    fid = db.add_folder(str(photos_dir), name="photos")

    p1 = db.add_photo(
        folder_id=fid,
        filename="cardinal.jpg",
        extension=".jpg",
        file_size=1000,
        file_mtime=1.0,
        timestamp="2024-01-15T10:00:00",
    )
    p2 = db.add_photo(
        folder_id=fid,
        filename="sparrow.jpg",
        extension=".jpg",
        file_size=1000,
        file_mtime=2.0,
        timestamp="2024-02-01T10:00:00",
    )
    p3 = db.add_photo(
        folder_id=fid,
        filename="mystery.jpg",
        extension=".jpg",
        file_size=1000,
        file_mtime=3.0,
        timestamp="2024-03-01T10:00:00",
    )
    db.conn.execute("UPDATE photos SET quality_score = 0.9 WHERE id = ?", (p1,))
    db.conn.execute("UPDATE photos SET quality_score = 0.7 WHERE id = ?", (p2,))
    db.conn.execute("UPDATE photos SET quality_score = 0.8 WHERE id = ?", (p3,))
    db.conn.execute("UPDATE photos SET mask_path = ? WHERE id = ?", ("masks/cardinal.png", p1))
    db.conn.execute("UPDATE photos SET mask_path = ? WHERE id = ?", ("masks/mystery.png", p3))

    cardinal = db.add_keyword("Northern Cardinal", is_species=True)
    sparrow = db.add_keyword("House Sparrow", is_species=True)
    backyard = db.add_keyword("Backyard", kw_type="location")
    db.tag_photo(p1, cardinal)
    db.tag_photo(p1, backyard)
    db.tag_photo(p2, sparrow)
    db.conn.commit()

    app = create_app(db_path=db_path, thumb_cache_dir=str(thumb_dir))
    return app, db


def test_publish_site_job_writes_life_list_highlights_and_images(tmp_path, monkeypatch):
    app, db = _seed_publish_app(tmp_path, monkeypatch)
    client = app.test_client()
    dest = tmp_path / "published"

    resp = client.post("/api/jobs/publish-site", json={
        "destination": str(dest),
        "photos_per_species": 2,
        "limit_per_bucket": 2,
        "max_size": 512,
    })
    assert resp.status_code == 200

    job = wait_for_job_via_client(client, resp.get_json()["job_id"])
    assert job["status"] == "completed"
    assert job["result"]["exported_images"] == 3
    assert job["result"]["errors"] == []

    site = json.loads((dest / "data" / "site.json").read_text())
    life = json.loads((dest / "data" / "life-list.json").read_text())
    highlights = json.loads((dest / "data" / "highlights.json").read_text())

    assert site["schema_version"] == 1
    assert site["counts"]["life_list_species"] == 2
    assert life["meta"]["species_count"] == 2
    cardinal = next(e for e in life["species"] if e["species"] == "Northern Cardinal")
    assert cardinal["locations"] == []
    assert cardinal["best"]["image"].startswith("images/photos/")
    assert (dest / cardinal["best"]["image"]).exists()
    assert [b["species"] for b in highlights["buckets"]] == [
        "Northern Cardinal",
        "House Sparrow",
    ]
    assert "mask_path" not in highlights["buckets"][0]["photos"][0]
    unidentified = highlights["unidentified"]["photos"][0]
    assert "mask_path" not in unidentified
    assert unidentified["image"].startswith("images/photos/unidentified-")
    assert (dest / unidentified["image"]).exists()

    db.close()


def test_publish_site_job_can_include_locations(tmp_path, monkeypatch):
    app, db = _seed_publish_app(tmp_path, monkeypatch)
    client = app.test_client()
    dest = tmp_path / "published"

    resp = client.post("/api/jobs/publish-site", json={
        "destination": str(dest),
        "include_locations": True,
    })
    assert resp.status_code == 200
    wait_for_job_via_client(client, resp.get_json()["job_id"])

    life = json.loads((dest / "data" / "life-list.json").read_text())
    cardinal = next(e for e in life["species"] if e["species"] == "Northern Cardinal")
    assert cardinal["locations"] == ["Backyard"]

    db.close()


def test_publish_site_job_rejects_relative_destination(app_and_db):
    app, _db = app_and_db
    resp = app.test_client().post("/api/jobs/publish-site", json={
        "destination": "relative/out",
    })
    assert resp.status_code == 400
