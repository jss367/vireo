"""HDR export through the job API, including clipped-highlight recovery."""

from pathlib import Path

import cv2
import numpy as np
import pytest
from PIL import Image
from services import photo_ingest, workspace_folders
from wait import wait_for_job_via_client


@pytest.fixture
def bracket(app_and_db, tmp_path):
    app, db = app_and_db
    fid = workspace_folders.add_folder(db, str(tmp_path))
    ids = []
    for i, shutter in enumerate((1.0, 0.25)):
        # The long exposure clips both bright patches to the same white.
        scene = np.tile([0.05, 0.5, 1.2, 2.4], (12, 12))
        encoded = 1.055 * np.minimum(scene * shutter, 1) ** (1 / 2.4) - 0.055
        path = tmp_path / f"frame{i}.png"
        Image.fromarray(np.rint(encoded * 255).astype(np.uint8)).convert("RGB").save(path)
        pid = photo_ingest.add_photo(db, fid, path.name, ".png", path.stat().st_size, 1)
        db.conn.execute("UPDATE photos SET shutter_speed=? WHERE id=?", (shutter, pid))
        ids.append(pid)
    db.commit()
    return app.test_client(), db, ids, tmp_path


@pytest.mark.parametrize("fmt", ["jpg", "hdr"])
def test_export_recovers_highlight_detail(bracket, fmt):
    client, _, ids, folder = bracket
    existing = folder / f"frame0_hdr.{fmt}"
    existing.write_bytes(b"existing")
    response = client.post("/api/jobs/hdr", json={"photo_ids": ids, "format": fmt, "reveal": False})
    assert response.status_code == 200
    job = wait_for_job_via_client(client, response.json["job_id"])
    assert job["status"] == "completed", job
    output = Path(job["result"]["path"])
    assert output == folder / f"frame0_hdr_2.{fmt}"
    assert existing.read_bytes() == b"existing"
    pixels = cv2.imread(str(output), cv2.IMREAD_UNCHANGED)
    assert pixels.shape == (12, 48, 3)
    if fmt == "hdr":
        np.testing.assert_allclose(pixels[4, :4, 0], [0.05, 0.5, 1.2, 2.4], rtol=0.03)
    else:
        assert np.all(np.diff(pixels[4, :4, 0].astype(float)) > 0)


@pytest.mark.parametrize("shutter", [None, 1.0])
def test_invalid_bracket_saves_nothing(bracket, shutter):
    client, db, ids, folder = bracket
    db.conn.execute("UPDATE photos SET shutter_speed=? WHERE id=?", (shutter, ids[1]))
    db.commit()
    response = client.post("/api/jobs/hdr", json={"photo_ids": ids, "reveal": False})
    job = wait_for_job_via_client(client, response.json["job_id"])
    assert job["status"] == "failed"
    assert "shutter speed" in str(job["errors"])
    assert not list(folder.glob("*_hdr.*"))


def test_linear_raw_buffers_are_deferred_without_clipping_provenance(monkeypatch):
    import hdr_merge
    from float_image import FloatImage

    scene = np.array([[[0.0001, 0.4, 1.5]]], dtype=np.float32)
    times = [0.25, 1.0]
    images = iter(FloatImage(np.minimum(scene * time, 1)) for time in times)
    monkeypatch.setattr(hdr_merge, "load_image", lambda *args, **kwargs: next(images))
    frames = [({"filename": "frame.nef", "shutter_speed": time}, Path("frame.nef")) for time in times]
    with pytest.raises(ValueError, match="RAW HDR"):
        hdr_merge._merge_frames(frames, 2048, lambda: None, lambda *args: None)


def test_offline_originals_resolve_before_rendering_and_keep_source_name(tmp_path, monkeypatch):
    from types import SimpleNamespace

    import hdr_merge

    photos = {pid: {"id": pid, "filename": f"source{pid}.png", "extension": ".png", "folder_id": 99}
              for pid in (1, 2)}
    db = SimpleNamespace(photos=SimpleNamespace(get_by_ids=lambda ids: photos),
                         photo_metadata=SimpleNamespace(camera_data_by_ids=lambda ids: {
                             1: {"shutter_speed": .25, "iso": 100, "aperture": 4},
                             2: {"shutter_speed": 1., "iso": 100, "aperture": 4}}),
                         folders=SimpleNamespace(tree=lambda: []),
                         photo_visibility=SimpleNamespace(visible_photo_ids=lambda ids: ids))
    cache, output = tmp_path / "managed", tmp_path / "output"
    cache.mkdir()
    output.mkdir()
    cached = {pid: cache / f"cached{pid}.png" for pid in photos}
    for path in cached.values():
        path.write_bytes(b"synthetic original; decoder mocked")
    resolved, loaded = [], []

    def resolve(db_arg, photo, root, folders):
        assert db_arg is db and root == str(cache) and folders == {}
        resolved.append(photo["id"])
        return str(cached[photo["id"]]), True

    def merge(frames, *args):
        loaded.extend(path for _, path in frames)
        return np.full((1, 1, 3), .25, np.float32)

    monkeypatch.setattr(hdr_merge, "resolve_original_path", resolve)
    monkeypatch.setattr(hdr_merge, "_merge_frames", merge)
    options = dict(photo_ids=[1, 2], output_format="hdr", input_size=2048,
                   checkpoint=lambda: None, progress=lambda *args: None)
    result = hdr_merge.create_hdr(db, str(cache), destination=str(output), **options)
    assert resolved == [1, 2] and loaded == list(cached.values())
    assert Path(result["path"]).name == "source1_hdr.hdr"
    assert Path(result["path"]).parent == output
    assert all(path.read_bytes() == b"synthetic original; decoder mocked" for path in cached.values())
    with pytest.raises(ValueError, match="destination"):
        hdr_merge.create_hdr(db, str(cache), destination="", **options)
    monkeypatch.setattr(hdr_merge, "resolve_original_path", lambda *args: (str(cache / "missing.png"), False))
    with pytest.raises(ValueError, match="Original unavailable"):
        hdr_merge.create_hdr(db, str(cache), destination=str(output), **options)
    assert sorted(path.name for path in cache.iterdir()) == ["cached1.png", "cached2.png"]
    assert [path.name for path in output.iterdir()] == ["source1_hdr.hdr"]


def test_raw_hdr_is_rejected_before_resolving_or_decoding(monkeypatch):
    from types import SimpleNamespace

    import hdr_merge

    db = SimpleNamespace(photos=SimpleNamespace(get_by_ids=lambda ids: {
        1: {"id": 1, "filename": "synthetic.NEF", "extension": ".nef"}}),
        photo_visibility=SimpleNamespace(visible_photo_ids=lambda ids: ids))
    monkeypatch.setattr(hdr_merge, "load_image", lambda *args, **kwargs: pytest.fail("RAW decoded"))
    with pytest.raises(ValueError, match="RAW HDR"):
        hdr_merge.create_hdr(db, "unused", photo_ids=[1], destination="", output_format="hdr", input_size=2048,
                             checkpoint=lambda: None, progress=lambda *args: None)


def test_mixed_raw_selection_fails_explicitly_without_decoding_or_output(bracket, monkeypatch):
    import hdr_merge

    client, db, ids, folder = bracket
    db.conn.execute("UPDATE photos SET extension='.NEF' WHERE id=?", (ids[1],))
    db.commit()
    monkeypatch.setattr(hdr_merge, "load_image", lambda *args, **kwargs: pytest.fail("mixed bracket decoded"))
    response = client.post("/api/jobs/hdr", json={"photo_ids": ids, "format": "hdr", "reveal": False})
    assert response.status_code == 200
    job = wait_for_job_via_client(client, response.json["job_id"])
    assert job["status"] == "failed"
    assert "RAW HDR" in str(job["errors"])
    assert not list(folder.glob("*_hdr.*"))
