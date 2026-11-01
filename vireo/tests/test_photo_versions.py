"""Versions must stay independent across edits, undo, export and catalog rebuilds."""

import json
import os

import pytest
from PIL import Image


def create(client, pid, **body):
    response = client.post(f"/api/photos/{pid}/versions", json=body)
    assert response.status_code == 200, response.get_json()
    return response.get_json()["version"]


def test_virtual_copy_is_independent_and_history_is_reversible(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    pid = db.photos.list_page()[0]["id"]
    db.set_photo_edit_recipe(pid, {"rotation": 90})
    db.commit()
    before_count = db.conn.execute("SELECT count(*) FROM photos").fetchone()[0]
    copy = create(client, pid)
    other = create(client, pid, source_version_id=copy["id"])
    url = f"/api/photos/{pid}/versions/{copy['id']}"
    response = client.put(url, json={"recipe": {"adjustments": {"exposure": 1}}, "revision": copy["revision"]})
    assert response.status_code == 200
    updated = response.get_json()["version"]
    assert db.get_photo_edit_recipe(pid)["rotation"] == 90
    assert db.photo_versions.get(pid, other["id"])["recipe"]["rotation"] == 90
    assert db.conn.execute("SELECT count(*) FROM photos").fetchone()[0] == before_count
    assert len(client.get(url + "/history").get_json()["history"]) == 2
    assert client.post("/api/undo").status_code == 200
    assert db.photo_versions.get(pid, copy["id"])["recipe"]["rotation"] == 90
    assert client.post("/api/redo").status_code == 200
    current = db.photo_versions.get(pid, copy["id"])
    assert current["recipe"]["adjustments"]["exposure"] == 1
    # Even undo+redo cannot make a stale editor's old token current again.
    assert client.put(url, json={"recipe": {}, "revision": updated["revision"]}).status_code == 409
    assert client.delete(url, json={"revision": current["revision"]}).status_code == 200
    assert client.get(url).status_code == 404
    assert db.photos.get(pid) is not None
    assert client.post("/api/undo").status_code == 200
    assert db.photo_versions.get(pid, copy["id"])["recipe"]["adjustments"]["exposure"] == 1


def test_snapshots_are_fixed_and_can_seed_copies(app_and_db):
    app, db = app_and_db
    pid = db.photos.list_page()[0]["id"]
    client = app.test_client()
    snapshot = create(client, pid, kind="snapshot", name="Wide crop", recipe={"rotation": 180})
    url = f"/api/photos/{pid}/versions/{snapshot['id']}"
    assert client.put(url, json={"recipe": {}, "revision": snapshot["revision"]}).status_code == 400
    copy = create(client, pid, source_version_id=snapshot["id"])
    assert copy["recipe"] == snapshot["recipe"]
    assert copy["id"] != snapshot["id"]
    assert db.get_photo_edit_recipe(pid) is None


@pytest.mark.parametrize("body", [
    {"kind": "anything"}, {"name": " "}, {"name": "a" * 121},
    {"recipe": {"adjustments": {"exposure": 99}}}, {"source_version_id": "missing"},
    {"source_version_id": [1]},
])
def test_invalid_versions_do_not_leave_records_or_history(app_and_db, body):
    app, db = app_and_db
    pid = db.photos.list_page()[0]["id"]
    response = app.test_client().post(f"/api/photos/{pid}/versions", json=body)
    assert response.status_code in (400, 404)
    assert db.photo_versions.list(pid) == []
    assert not db.conn.execute("SELECT 1 FROM edit_history WHERE action_type='photo_version'").fetchone()


def test_version_cannot_be_accessed_through_another_photo(app_and_db):
    app, db = app_and_db
    first, second = db.photos.list_page()[:2]
    client = app.test_client()
    version = create(client, first["id"])
    url = f"/api/photos/{second['id']}/versions/{version['id']}"
    assert client.get(url).status_code == 404
    assert client.put(url, json={"revision": version["revision"], "recipe": {}}).status_code == 404
    assert client.get("/api/photos/999999/versions").status_code == 404


def test_version_export_renders_its_crop_and_captures_recipe(app_and_db, tmp_path):
    from export import export_photos
    from services.photo_versions import export_version_options

    app, db = app_and_db
    photo = db.photos.list_page()[0]
    pid = photo["id"]
    source = tmp_path / "source"
    source.mkdir()
    Image.new("RGB", (80, 60), "red").save(source / photo["filename"])
    db.conn.execute("UPDATE folders SET path=? WHERE id=?", (str(source), photo["folder_id"]))
    db.commit()
    version = create(app.test_client(), pid, recipe={"crop": {"x": 0, "y": 0, "w": .5, "h": 1}})
    options = export_version_options(db, [pid], {"version_id": version["id"], "version_revision": version["revision"]})
    # Changing the version after job creation must not change that job's pixels.
    app.test_client().put(f"/api/photos/{pid}/versions/{version['id']}",
                          json={"recipe": {}, "revision": version["revision"]})
    result = export_photos(db, str(tmp_path), [pid], str(tmp_path / "output"),
                           options={**options, "format": "png", "metadata_fields": [], "collect_files": True})
    assert not result["errors"], result
    with Image.open(result["files"][0]) as image:
        assert image.size == (40, 60)
    assert db.get_photo_edit_recipe(pid) is None
    with pytest.raises(ValueError):
        export_version_options(db, [pid], {"version_id": "missing"})
    with pytest.raises(ValueError):
        export_version_options(db, [pid], {"version_id": version["id"], "version_revision": version["revision"]})


def test_versions_survive_xmp_sync_and_rebuild(app_and_db, tmp_path):
    from sync import sync_to_xmp
    from xmp.readers import read_sync_preview_metadata

    app, db = app_and_db
    photo = db.photos.list_page()[0]
    pid = photo["id"]
    folder = tmp_path / "originals"
    folder.mkdir()
    db.conn.execute("UPDATE folders SET path=? WHERE id=?", (str(folder), photo["folder_id"]))
    db.commit()
    version = create(app.test_client(), pid, name="Color & crop", recipe={"rotation": 90})
    result = sync_to_xmp(db)
    assert result["synced"] >= 1, result
    sidecar = folder / (os.path.splitext(photo["filename"])[0] + ".xmp")
    raw = read_sync_preview_metadata(str(sidecar))["photo_versions"]
    assert json.loads(raw)["versions"][0]["name"] == "Color & crop"
    db.photo_versions.delete(pid, version["id"])
    db.photo_versions.import_document(pid, raw)
    assert db.photo_versions.get(pid, version["id"])["recipe"]["rotation"] == 90


def test_masks_referenced_only_by_versions_survive_cleanup(app_and_db, tmp_path):
    import hashlib

    import local_masks

    app, db = app_and_db
    pid = db.photos.list_page()[0]["id"]
    data = b"an immutable mask reference"
    ref = hashlib.sha1(data).hexdigest()[:12]
    path = local_masks.snapshot_path(str(tmp_path), pid, ref)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as stream:
        stream.write(data)
    recipe = {"local": {"mask": {"ref": ref, "source_digest": "a" * 16}, "regions": [{"region": "subject", "adjustments": {"exposure": 1}}]}}
    version = db.photo_versions.create(pid, recipe)
    db.commit()
    local_masks.gc_edit_masks(db, str(tmp_path), grace_seconds=0)
    assert os.path.exists(path)
    db.photo_versions.delete(pid, version["id"])
    db.commit()
    local_masks.gc_edit_masks(db, str(tmp_path), grace_seconds=0)
    assert not os.path.exists(path)


def test_merge_keeps_versions_history_and_mask_pixels(app_and_db, tmp_path):
    import hashlib
    import io

    import local_masks
    from repositories.photo_row_deletion import photo_row_deletion

    app, db = app_and_db
    from services import photo_ingest
    second = db.photos.list_page()[0]
    survivor = second["id"]
    pid = photo_ingest.add_photo(db, second["folder_id"], "merge-source.jpg", ".jpg", 100, 1.0)
    data = io.BytesIO()
    Image.new("L", (8, 8), 255).save(data, format="PNG")
    ref = hashlib.sha1(data.getvalue()).hexdigest()[:12]
    path = local_masks.snapshot_path(str(tmp_path), pid, ref)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as stream:
        stream.write(data.getvalue())
    recipe = {"local": {"mask": {"ref": ref, "source_digest": "a" * 16},
                        "regions": [{"region": "subject", "adjustments": {"exposure": 1}}]}}
    copy = db.photo_versions.create(pid, recipe)
    db.commit()
    with photo_row_deletion(db.conn) as deletion:
        deletion.delete({pid: survivor})
    db.commit()
    moved = db.photo_versions.get(survivor, copy["id"])
    assert moved["recipe"] == copy["recipe"]
    recovered = local_masks.load_snapshot(str(tmp_path), survivor, moved["recipe"])
    assert recovered is not None and recovered.getpixel((0, 0)) == 255
    assert os.path.isfile(local_masks.snapshot_path(str(tmp_path), survivor, ref))


def test_sidecar_recovery_validates_before_writing_and_keeps_pending_deletions(app_and_db):
    app, db = app_and_db
    pid = db.photos.list_page()[0]["id"]
    version = create(app.test_client(), pid)
    raw = db.photo_versions.serialize(pid)
    app.test_client().delete(f"/api/photos/{pid}/versions/{version['id']}", json={"revision": version["revision"]})
    db.photo_versions.import_document(pid, raw)
    assert db.photo_versions.list(pid) == []  # an older sidecar must not resurrect a deleted copy
    db.conn.execute("DELETE FROM pending_changes WHERE photo_id=?", (pid,))
    malformed = json.loads(raw)
    malformed["versions"].append({"id": "invalid"})
    with pytest.raises(ValueError):
        db.photo_versions.import_document(pid, json.dumps(malformed))
    assert db.photo_versions.list(pid) == []


def test_all_workspaces_queue_current_versions_with_fresh_sync_tokens(app_and_db):
    app, db = app_and_db
    pid = db.photos.list_page()[0]["id"]
    version = create(app.test_client(), pid)
    first = db.conn.execute("SELECT change_token FROM pending_changes WHERE change_type='photo_versions'").fetchone()[0]
    workspace = db.create_workspace("Second editing workspace")
    db.set_active_workspace(workspace)
    changed = dict(version, name="Renamed in the second workspace")
    db.photo_versions.put(pid, changed)
    db.photo_versions.queue_sync(pid, db.require_workspace_id())
    db.commit()
    rows = db.conn.execute("SELECT value, change_token FROM pending_changes WHERE change_type='photo_versions'").fetchall()
    assert len(rows) == 2
    assert all(json.loads(row["value"])["versions"][0]["name"] == changed["name"] for row in rows)
    assert all(row["change_token"] != first for row in rows)


def test_export_api_passes_selected_version_to_preflight_and_worker(app_and_db, monkeypatch, tmp_path):
    import export
    from wait import wait_for_job_via_client

    app, db = app_and_db
    client = app.test_client()
    pid = db.photos.list_page()[0]["id"]
    version = create(client, pid, recipe={"rotation": 90})
    captured = {}

    def preview(**kwargs):
        captured["preview"] = kwargs["options"]["recipe_overrides"]
        return {"renames": [], "destination_folders": []}

    def run(**kwargs):
        captured["worker"] = kwargs["options"]["recipe_overrides"]
        return {"exported": 1, "errors": []}

    monkeypatch.setattr(export, "preview_export", preview)
    monkeypatch.setattr(export, "export_photos", run)
    body = {"photo_ids": [pid], "version_id": version["id"], "version_revision": version["revision"],
            "destination": str(tmp_path / "exports"), "metadata_fields": ["location"]}
    assert client.post("/api/jobs/export/preflight", json=body).status_code == 200
    response = client.post("/api/jobs/export", json=body)
    assert response.status_code == 200, response.get_json()
    wait_for_job_via_client(client, response.get_json()["job_id"])
    assert captured["preview"][pid] == captured["worker"][pid] == version["recipe"]


def test_queue_failure_rolls_back_version_and_history_together(app_and_db, monkeypatch):
    import sqlite3

    from repositories.photo_versions import PhotoVersionsRepository

    app, db = app_and_db
    pid = db.photos.list_page()[0]["id"]

    def fail(self, photo_id, workspace_id):
        with sqlite3.connect(db._db_path) as reader:
            assert reader.execute("SELECT COUNT(*) FROM photo_versions").fetchone()[0] == 0
            assert reader.execute("SELECT COUNT(*) FROM edit_history WHERE action_type='photo_version'").fetchone()[0] == 0
        raise ValueError("Simulated queue failure")

    monkeypatch.setattr(PhotoVersionsRepository, "queue_sync", fail)
    response = app.test_client().post(f"/api/photos/{pid}/versions", json={})
    assert response.status_code == 400
    assert db.photo_versions.list(pid) == []
    assert not db.conn.execute("SELECT 1 FROM edit_history WHERE action_type='photo_version'").fetchone()
