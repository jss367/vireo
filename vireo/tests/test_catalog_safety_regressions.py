"""Regression coverage for catalog safety and review-state integrity."""
import json
import os
import sqlite3

import pytest
from PIL import Image


def seed(db, path, name="bird.jpg"):
    path.mkdir(parents=True, exist_ok=True)
    Image.new("RGB", (32, 24), "red").save(path / name)
    fid = db.add_folder(str(path), name=path.name)
    pid = db.add_photo(folder_id=fid, filename=name, extension=".jpg",
                       file_size=(path / name).stat().st_size, file_mtime=1)
    return fid, pid


def test_large_scope_releases_its_read_transaction(db, tmp_path):
    from db import Database
    _, pid = seed(db, tmp_path / "photos")
    other = Database(db._db_path)
    try:
        db.conn.commit()
        clause, params = db._scope_clause(range(1, 802))
        db.conn.execute("SELECT p.id FROM photos p WHERE 1=1" + clause, params).fetchall()
        assert not db.conn.in_transaction
        other.conn.execute("UPDATE photos SET rating=4 WHERE id=?", (pid,))
        other.conn.commit()
        assert db.get_photo(pid)["rating"] == 4
        db.conn.execute("UPDATE photos SET rating=5 WHERE id=?", (pid,))
    finally:
        db.conn.rollback()
        other.close()


def test_remove_orphans_retains_existing_photo(app_and_db, tmp_path):
    app, db = app_and_db
    _, pid = seed(db, tmp_path / "present")
    response = app.test_client().post("/api/audit/remove-orphans", json={"photo_ids": [pid]})
    assert response.status_code == 200
    assert response.json["removed"] == 0
    assert (tmp_path / "present/bird.jpg").exists()
    assert db.get_photo(pid) is not None


def test_alias_move_and_cleanup_protect_winner_file(app_and_db, tmp_path, monkeypatch):
    import app as app_module
    from move import move_photos
    from scanner import scan
    app, db = app_and_db
    _, pid = seed(db, tmp_path / "source")
    dst = tmp_path / "destination"
    dst.mkdir()
    canonical_fid = db.add_folder(str(dst), name="destination")
    result = move_photos(db, [pid], str(dst) + "/")
    assert result["moved"] == 1
    assert db.get_photo(pid)["folder_id"] == canonical_fid
    scan(str(dst), db, incremental=True, skip_working_copies=True)
    rows = db.conn.execute("SELECT p.id, f.path FROM photos p JOIN folders f ON p.folder_id=f.id WHERE filename='bird.jpg'").fetchall()
    assert len(rows) == 1
    # Also protect catalogs that already contain an alias from older versions.
    alias_fid = db.add_folder(str(dst) + "/", name="legacy alias")
    alias_pid = db.add_photo(folder_id=alias_fid, filename="bird.jpg", extension=".jpg", file_size=1, file_mtime=1)
    rows = db.conn.execute("SELECT p.id, f.path FROM photos p JOIN folders f ON p.folder_id=f.id WHERE filename='bird.jpg'").fetchall()
    assert len(rows) == 2
    assert os.path.samefile(*(os.path.join(r["path"], "bird.jpg") for r in rows))
    winner, loser = [r["id"] for r in rows]
    db.conn.execute("UPDATE photos SET file_hash='same-content', flag='' WHERE id=?", (winner,))
    db.conn.execute("UPDATE photos SET file_hash='same-content', flag='rejected' WHERE id=?", (loser,))
    db.conn.commit()
    requested = []
    def record_trash(paths, **kwargs):
        requested.extend(paths)
        return len(paths), set(paths), []
    monkeypatch.setattr(app_module, "_trash_paths", record_trash)
    monkeypatch.setattr(app_module, "_network_volume_roots", lambda: [])
    response = app.test_client().post("/api/duplicates/delete-loser-files", json={"photo_ids": [loser]})
    assert response.status_code == 200, response.json
    assert requested == []
    assert response.json["skipped"]
    assert db.get_photo(alias_pid) is not None
    assert db.get_photo(winner) is not None


def test_refresh_capture_metadata_updates_file_identity(db, tmp_path, monkeypatch):
    import capture_time
    _, pid = seed(db, tmp_path / "photos")
    db.conn.execute("UPDATE photos SET file_hash='old-hash', file_size=1 WHERE id=?", (pid,))
    monkeypatch.setattr(capture_time, "extract_metadata", lambda paths: {paths[0]: {"EXIF": {"DateTimeOriginal": "2026:01:02 12:00:00"}}})
    capture_time._refresh_photo_metadata(db, pid, str(tmp_path / "photos/bird.jpg"))
    row = db.conn.execute("SELECT * FROM photos WHERE id=?", (pid,)).fetchone()
    assert row["file_hash"] == __import__("scanner").compute_file_hash(str(tmp_path / "photos/bird.jpg"))
    assert row["file_size"] == (tmp_path / "photos/bird.jpg").stat().st_size
    assert row["timestamp"].startswith("2026-01-02")


def test_import_untracked_limits_scan_to_selected_files(db, tmp_path, monkeypatch):
    import config
    from audit import import_untracked
    monkeypatch.setattr(config, "CONFIG_PATH", str(tmp_path / "config.json"))
    root = tmp_path / "photos"
    (root / "nested").mkdir(parents=True)
    for path in (root / "chosen.jpg", root / "other.jpg", root / "nested/deep.jpg"):
        Image.new("RGB", (32, 24)).save(path)
    assert import_untracked(db, [str(root / "chosen.jpg")]) == 1
    names = {r[0] for r in db.conn.execute("SELECT filename FROM photos")}
    assert names == {"chosen.jpg"}
    assert import_untracked(db, [str(root / "chosen.jpg")]) == 0


def test_batch_metadata_preserves_orientation(db, tmp_path):
    from render_source import recipe_source_dimensions
    _, pid = seed(db, tmp_path / "photos")
    db.conn.execute("UPDATE photos SET width=6000,height=4000,exif_data=? WHERE id=?", (json.dumps({"EXIF": {"Orientation": 6}}), pid))
    db.conn.commit()
    assert recipe_source_dimensions(db.get_photo(pid)) == (4000, 6000)
    assert recipe_source_dimensions(db.get_photos_by_ids([pid], include_exif=True)[pid]) == (4000, 6000)


def test_remove_absent_keyword_has_no_history(app_and_db):
    app, db = app_and_db
    pid = db.get_photos()[0]["id"]
    kid = db.add_keyword("Absent tag")
    response = app.test_client().delete(f"/api/photos/{pid}/keywords/{kid}")
    assert response.status_code == 200
    assert not any(k["id"] == kid for k in db.get_photo_keywords(pid))
    assert db.conn.execute("SELECT COUNT(*) FROM edit_history").fetchone()[0] == 0
    db.undo_last_edit()
    assert not any(k["id"] == kid for k in db.get_photo_keywords(pid))


def test_deleted_keyword_does_not_block_undo(app_and_db):
    app, db = app_and_db
    pid = db.get_photos()[0]["id"]
    kid = db.add_keyword("Removed then deleted")
    db.tag_photo(pid, kid)
    client = app.test_client()
    assert client.delete(f"/api/photos/{pid}/keywords/{kid}").status_code == 200
    assert client.delete(f"/api/keywords/{kid}").status_code == 200
    db.undo_last_edit()
    assert not any(k["id"] == kid for k in db.get_photo_keywords(pid))


def test_batch_keyword_validates_all_ids_before_mutation(app_and_db):
    app, db = app_and_db
    pid = db.get_photos()[0]["id"]
    before = db.conn.execute("SELECT count(*) FROM edit_history").fetchone()[0]
    response = app.test_client().post("/api/batch/keyword", json={"photo_ids": [pid, 99999999], "name": "Partial batch"})
    assert response.status_code == 403
    assert not any(k["name"] == "Partial batch" for k in db.get_photo_keywords(pid))
    assert db.conn.execute("SELECT count(*) FROM edit_history").fetchone()[0] == before


def test_16_bit_grayscale_scales_to_midgray(tmp_path):
    from image_loader import load_image
    source = tmp_path / "gray.tiff"
    Image.new("I;16", (5, 5), 32768).save(source)
    assert load_image(str(source)).getpixel((0, 0)) == (128, 128, 128)


def test_settings_roundtrip_preserves_migration_markers(app_and_db):
    import config
    app, db = app_and_db
    config.save({"_migrations_applied": [config.MIGRATION_MISS_THRESHOLDS],
                 "pipeline": {"miss_det_confidence": 0.25, "miss_det_confidence_burst": 0.15}})
    client = app.test_client()
    exported = client.get("/api/settings/export").json
    response = client.post("/api/settings/import", json={"json": json.dumps(exported)})
    assert response.status_code == 200, response.json
    assert config.MIGRATION_MISS_THRESHOLDS in config._read_raw().get("_migrations_applied", [])
    assert not config.migrate_legacy_miss_thresholds()
    assert config.load()["pipeline"]["miss_det_confidence"] == 0.25


def test_paired_jpeg_remains_in_dedup_index(db, tmp_path):
    from import_dedup import CatalogIndex, DuplicateChecker
    from scanner import _pair_raw_jpeg_companions, compute_file_hash
    fid, jpeg_id = seed(db, tmp_path / "photos")
    source = tmp_path / "photos/bird.jpg"
    jpeg_hash = compute_file_hash(str(source))
    raw = tmp_path / "photos/bird.nef"
    raw.write_bytes(b"placeholder RAW bytes")
    raw_id = db.add_photo(folder_id=fid, filename="bird.nef", extension=".nef", file_size=raw.stat().st_size, file_mtime=1)
    db.conn.execute("UPDATE photos SET file_hash=? WHERE id=?", (jpeg_hash, jpeg_id))
    db.conn.execute("UPDATE photos SET file_hash=? WHERE id=?", (compute_file_hash(str(raw)), raw_id))
    db.conn.commit()
    assert DuplicateChecker(CatalogIndex.from_db(db), verify_by_hash=True).match(source)
    _pair_raw_jpeg_companions(db)
    assert db.get_photo(raw_id)["companion_path"] == "bird.jpg"
    assert DuplicateChecker(CatalogIndex.from_db(db), verify_by_hash=True).match(source) is not None
    import shutil

    from ingest import ingest

    card = tmp_path / "card"
    card.mkdir()
    shutil.copy2(source, card / source.name)
    imported = ingest(str(card), str(source.parent), db, verify_by_hash=True)
    assert imported["copied"] == 0
    assert str(source.parent) in imported["duplicate_folders"]
    # Old paired catalogs can recover the companion identity once.
    db.conn.execute("DELETE FROM companion_identities")
    db.conn.commit()
    assert DuplicateChecker(CatalogIndex.from_db(db), verify_by_hash=True).match(source)
    assert not db.conn.in_transaction



def test_image_loader_and_export_preserve_color_profile(tmp_path):
    from export import _save_export_image
    from image_loader import load_image
    from PIL import ImageCms
    profile = ImageCms.ImageCmsProfile(ImageCms.createProfile("sRGB")).tobytes()
    source, output = tmp_path / "input.png", tmp_path / "output.jpg"
    Image.new("RGB", (10, 10), (90, 140, 190)).save(source, icc_profile=profile)
    loaded = load_image(str(source))
    assert loaded.info["icc_profile"]
    assert loaded.getpixel((0, 0)) == (90, 140, 190)
    _save_export_image(loaded, str(output), {"pil_format": "JPEG", "quality": True}, 95)
    with Image.open(output) as result:
        assert result.info["icc_profile"]


def test_labels_delete_rejects_unrelated_path(app_and_db, tmp_path):
    app, _ = app_and_db
    (tmp_path / ".vireo").mkdir(exist_ok=True)
    unrelated = tmp_path / "unrelated.txt"
    unrelated.write_text("must be retained")
    response = app.test_client().delete("/api/labels", json={"labels_file": str(unrelated)})
    assert response.status_code == 400, response.json
    assert unrelated.exists()


def test_real_capture_time_edit_refreshes_hash(db, tmp_path, monkeypatch):
    import shutil

    if not shutil.which("exiftool"):
        pytest.skip("ExifTool is required for an actual metadata rewrite")
    import config
    from capture_time import adjust_capture_time
    from scanner import compute_file_hash, scan
    monkeypatch.setattr(config, "CONFIG_PATH", str(tmp_path / "config.json"))
    root = tmp_path / "photos"
    root.mkdir()
    path = root / "dated.jpg"
    exif = Image.Exif()
    exif[36867] = "2026:01:02 12:00:00"
    exif[36868] = "2026:01:02 12:00:00"
    exif[306] = "2026:01:02 12:00:00"
    Image.new("RGB", (32, 24)).save(path, exif=exif)
    scan(str(root), db, skip_working_copies=True)
    row = db.conn.execute("SELECT * FROM photos WHERE filename='dated.jpg'").fetchone()
    old_hash = row["file_hash"]
    assert old_hash == compute_file_hash(str(path))
    result = adjust_capture_time(db, [row["id"]], mode="manual", shift_minutes=60, keep_backups=False)
    assert result["updated"] == 1, result
    assert compute_file_hash(str(path)) != old_hash
    scan(str(root), db, incremental=True, skip_working_copies=True)
    assert db.conn.execute("SELECT file_hash FROM photos WHERE id=?", (row["id"],)).fetchone()[0] == compute_file_hash(str(path))


def test_cropped_raw_route_uses_sensor_dimensions(app_and_db, tmp_path, monkeypatch):
    import io

    import image_loader
    app, db = app_and_db
    root = tmp_path / "raw"
    root.mkdir()
    path = root / "thumbnail.nef"
    # Synthetic TIFF container models Pillow opening a RAW's thumbnail IFD.
    # RAW decoding itself is stubbed; this exercises the route's size bound.
    Image.new("RGB", (160, 120)).save(path, format="TIFF")
    fid = db.add_folder(str(root), name="raw")
    pid = db.add_photo(folder_id=fid, filename=path.name, extension=".nef", file_size=path.stat().st_size, file_mtime=path.stat().st_mtime)
    db.conn.execute("UPDATE photos SET width=6000,height=4000 WHERE id=?", (pid,))
    db.conn.commit()
    sizes = []
    def fake_decode(path, max_size=None, **kwargs):
        sizes.append(max_size)
        return Image.new("RGB", (max_size, int(max_size * 2/3)))
    monkeypatch.setattr(image_loader, "load_image", fake_decode)
    response = app.test_client().get(f"/photos/{pid}/edit-preview", query_string={"size": 1920, "apply_crop": 1, "recipe": json.dumps({"crop": {"x": 0.25, "y": 0.25, "w": 0.5, "h": 0.5}})})
    assert response.status_code == 200, response.data
    assert sizes[0] == 3840
    with Image.open(io.BytesIO(response.data)) as rendered:
        assert max(rendered.size) == 1920


def test_legacy_review_server_rejects_cross_origin_form(tmp_path):
    from review_server import create_app
    sidecar = tmp_path / "photo.xmp"
    (tmp_path / "results.json").write_text(json.dumps({"photos": [{"filename": "photo.jpg", "status": "pending", "predictions": {"model": {"prediction": "Robin", "confidence": 0.95}}, "xmp_path": str(sidecar)}]}))
    response = create_app(str(tmp_path)).test_client().post("/api/accept-batch", data="", content_type="application/x-www-form-urlencoded", headers={"Origin": "https://untrusted.example"})
    assert response.status_code == 403, response.data
    assert not sidecar.exists()
    assert json.loads((tmp_path / "results.json").read_text())["photos"][0]["status"] == "pending"


@pytest.mark.parametrize("action, status", [("accept", "accepted"), ("reject", "rejected"), ("reviewed", "reviewed")])
def test_prediction_routes_reject_foreign_workspace(app_and_db, tmp_path, action, status):
    app, db = app_and_db
    ws = db._ws_id()
    fid, pid = seed(db, tmp_path / "foreign")
    det_id = db.save_detections(pid, [{"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.9}], "test-detector")[0]
    db.add_prediction(det_id, "Robin", .9, "test-model")
    pred_id = db.conn.execute("SELECT id FROM predictions WHERE detection_id=?", (det_id,)).fetchone()[0]
    db.conn.execute("DELETE FROM workspace_folders WHERE workspace_id=? AND folder_id=?", (ws, fid))
    db.conn.commit()
    assert db.get_photo(pid, verify_workspace=True) is None
    response = app.test_client().post(f"/api/predictions/{pred_id}/{action}", json={})
    assert response.status_code == 404, response.json
    assert db.get_review_status(pred_id, ws) != status


def test_classifier_partial_flush_is_not_a_cache_hit(db, tmp_path):
    from classify_job import _record_batch_classifier_runs
    _, pid = seed(db, tmp_path / "photos")
    did = db.save_detections(pid, [{"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": .9}], "test-detector")[0]
    _record_batch_classifier_runs(db, [{"detection_id": did}], "test-model", "test-labels", [{"detection_id": did}])
    assert db.conn.execute("SELECT count(*) FROM predictions WHERE detection_id=?", (did,)).fetchone()[0] == 0
    accepted, _ = db.get_classifier_run_key_gate(did, "some-current-runtime")
    assert ("test-model", "test-labels") not in accepted


def test_failed_folder_transfer_leaves_no_workspace(app_and_db):
    app, db = app_and_db
    child = db.conn.execute("SELECT id FROM folders WHERE parent_id IS NOT NULL LIMIT 1").fetchone()[0]
    before = len(db.get_workspaces())
    response = app.test_client().post(f"/api/workspaces/{db._ws_id()}/move-folders", json={"folder_ids": [child], "new_workspace_name": "Failed transfer destination"})
    assert response.status_code == 400, response.json
    assert len(db.get_workspaces()) == before


def test_staging_lock_failure_allows_retry(db, tmp_path):
    from db import Database
    from services.local_folder import stage_folder
    fid, _ = seed(db, tmp_path / "photos")
    other = Database(db._db_path)
    db.conn.execute("PRAGMA busy_timeout=0")
    other.conn.execute("BEGIN IMMEDIATE")
    try:
        with pytest.raises(sqlite3.OperationalError, match="locked"):
            stage_folder(db, fid, str(tmp_path / "vireo"), local_base=str(tmp_path / "custom"))
    finally:
        other.conn.rollback()
        other.close()
    stage_folder(db, fid, str(tmp_path / "vireo"), local_base=str(tmp_path / "custom"))


def test_large_scope_preserves_caller_transaction(db, tmp_path):
    _, pid = seed(db, tmp_path / "photos")
    db.conn.execute("UPDATE photos SET rating=4 WHERE id=?", (pid,))
    db._scope_clause(range(1, 802))
    assert db.conn.in_transaction
    db.conn.rollback()
    assert db.get_photo(pid)["rating"] != 4


@pytest.mark.parametrize("offline", [False, True])
def test_orphan_removal_requires_accessible_parent(app_and_db, tmp_path, offline):
    app, db = app_and_db
    root = tmp_path / "photos"
    _, pid = seed(db, root)
    (root / "bird.jpg").unlink()
    if offline:
        root.rmdir()
    response = app.test_client().post("/api/audit/remove-orphans", json={"photo_ids": [pid]})
    assert response.status_code == 200
    assert response.json["removed"] == (0 if offline else 1)
    assert (db.get_photo(pid) is not None) == offline


@pytest.mark.parametrize("method, suffix, body", [
    ("post", "/keywords", {"name": "Foreign tag"}),
    ("delete", "/keywords/1", None),
    ("get", "/masks", None),
])
def test_photo_actions_reject_foreign_workspace(app_and_db, tmp_path, method, suffix, body):
    app, db = app_and_db
    fid, pid = seed(db, tmp_path / "foreign")
    db.conn.execute("DELETE FROM workspace_folders WHERE folder_id=?", (fid,))
    db.conn.commit()
    response = getattr(app.test_client(), method)(f"/api/photos/{pid}{suffix}", json=body)
    assert response.status_code == 404


def test_custom_models_have_unique_safe_ids(app_and_db, tmp_path):
    import re

    import models

    app, _ = app_and_db
    client = app.test_client()
    ids = []
    for name in ["Bird's / model", "Bird's / model", "bird-S-model"]:
        response = client.post("/api/models/custom", json={"name": name, "weights_path": str(tmp_path / "missing.onnx")})
        assert response.status_code == 200
        model_id = response.json["model_id"]
        assert re.fullmatch(r"custom-[a-z0-9-]+", model_id)
        ids.append(model_id)
    assert len(set(ids)) == 3
    assert set(ids).issubset({m["id"] for m in models.get_models()})
    for model_id in ids:
        assert client.delete(f"/api/models/{model_id}").status_code == 200


@pytest.mark.parametrize("route", ["/api/models/custom", "/api/batch/keyword", "/api/audit/remove-orphans"])
def test_object_routes_reject_json_arrays(app_and_db, route):
    app, _ = app_and_db
    assert app.test_client().post(route, json=[1]).status_code == 400


def test_label_delete_rejects_symlink_escape(app_and_db, tmp_path, monkeypatch):
    import labels

    app, _ = app_and_db
    root = tmp_path / "labels"
    root.mkdir()
    monkeypatch.setattr(labels, "LABELS_DIR", str(root))
    victim = tmp_path / "unrelated.txt"
    victim.write_text("preserve")
    alias = root / "list.txt"
    alias.symlink_to(victim)
    response = app.test_client().delete("/api/labels", json={"labels_file": str(alias)})
    assert response.status_code == 400
    assert victim.read_text() == "preserve"
    assert alias.is_symlink()


@pytest.mark.parametrize("override, confirmed, expected", [
    (None, True, ["Robin"]),
    ({"species_list": ["Eagle"], "confirmed": False}, False, ["Eagle"]),
    ({"species_list": [], "confirmed": False}, False, []),
])
def test_detach_preserves_confirmation_without_promoting_mixed_overrides(app_and_db, override, confirmed, expected):
    from pathlib import Path

    app, db = app_and_db
    ids = [p["id"] for p in db.get_photos()]
    results = {
        "encounters": [{
            "species": ["Robin", .9], "species_confirmed": True,
            "confirmed_species": "Robin", "photo_ids": ids,
            "photo_count": len(ids), "burst_count": 2,
            "bursts": [{"photo_ids": ids[:-1]}, {"photo_ids": ids[-1:], "species_override": override}],
        }],
        "photos": [{"id": pid, "label": "KEEP", "species_top5": []} for pid in ids],
        "summary": {"total_photos": len(ids), "encounter_count": 1, "burst_count": 2},
    }
    cache = Path(app.config["DB_PATH"]).parent / f"pipeline_results_ws{db._ws_id()}.json"
    cache.write_text(json.dumps(results))
    response = app.test_client().post("/api/pipeline/detach-burst", json={"encounter_index": 0, "burst_index": 1})
    assert response.status_code == 200, response.json
    detached = response.json["encounters"][-1]
    assert detached["confirmed_species_list"] == expected
    assert detached["species_confirmed"] == confirmed


def test_review_server_allows_same_origin_json(tmp_path):
    from review_server import create_app

    results = tmp_path / "results.json"
    results.write_text(json.dumps({"photos": [{"filename": "bird.jpg", "status": "pending"}]}))
    response = create_app(str(tmp_path)).test_client().post(
        "/api/skip/bird.jpg", json={}, headers={"Origin": "http://localhost"},
    )
    assert response.status_code == 200
    assert json.loads(results.read_text())["photos"][0]["status"] == "skipped"


@pytest.mark.parametrize("alias_kind", ["slash", "dot", "symlink"])
def test_scans_reuse_catalog_folder_for_path_aliases(db, tmp_path, alias_kind):
    from scanner import scan

    root = tmp_path / "photos"
    root.mkdir()
    Image.new("RGB", (24, 16)).save(root / "bird.jpg")
    scan(str(root), db, skip_working_copies=True)
    if alias_kind == "slash":
        alias = str(root) + "/"
    elif alias_kind == "dot":
        alias = str(root) + "/../photos"
    else:
        link = tmp_path / "alias"
        link.symlink_to(root, target_is_directory=True)
        alias = str(link)
    scan(alias, db, incremental=True, skip_working_copies=True)
    assert db.conn.execute("SELECT COUNT(*) FROM photos WHERE filename='bird.jpg'").fetchone()[0] == 1


def test_batch_keyword_rolls_back_failed_write(app_and_db, monkeypatch):
    from db import Database

    app, db = app_and_db
    ids = [p["id"] for p in db.get_photos()]
    kid = db.add_keyword("Atomic batch")
    original = Database.tag_photo

    def fail_second(self, photo_id, keyword_id, *args, **kwargs):
        if photo_id == ids[1]:
            raise sqlite3.OperationalError("simulated write failure")
        return original(self, photo_id, keyword_id, *args, **kwargs)

    monkeypatch.setattr(Database, "tag_photo", fail_second)
    response = app.test_client().post("/api/batch/keyword", json={"photo_ids": ids, "keyword_id": kid})
    assert response.status_code == 500
    assert db.conn.execute("SELECT COUNT(*) FROM photo_keywords WHERE keyword_id=?", (kid,)).fetchone()[0] == 0
    assert db.conn.execute("SELECT COUNT(*) FROM edit_history").fetchone()[0] == 0
    assert db.conn.execute("SELECT COUNT(*) FROM pending_changes WHERE value='Atomic batch'").fetchone()[0] == 0
