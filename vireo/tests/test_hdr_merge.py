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


def test_linear_raw_samples_keep_sensor_precision(monkeypatch):
    import hdr_merge
    from float_image import FloatImage

    scene = np.array([[[0.0001, 0.4, 1.5]]], dtype=np.float32)
    times = [0.25, 1.0]
    images = iter(FloatImage(np.minimum(scene * time, 1)) for time in times)
    monkeypatch.setattr(hdr_merge, "load_image", lambda *args, **kwargs: next(images))
    frames = [({"filename": "frame.nef", "shutter_speed": time}, Path("frame.nef")) for time in times]
    result = hdr_merge._merge_frames(frames, 2048, lambda: None, lambda *args: None)
    np.testing.assert_allclose(result, scene, rtol=1e-5)
