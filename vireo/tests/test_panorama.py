"""Real stitching and safety regressions for panorama creation."""

from pathlib import Path
from unittest.mock import Mock

import cv2
import numpy as np
import panorama
import pytest
from PIL import Image
from wait import wait_for_job_via_client
from web.background_jobs import JobCancelled


@pytest.fixture
def panorama_env(db, tmp_path):
    db.set_active_workspace(db.ensure_default_workspace())
    folder = tmp_path / "photos"
    folder.mkdir()
    fid = db.add_folder(str(folder))
    rng = np.random.default_rng(42)
    scene = cv2.GaussianBlur(rng.integers(0, 256, (300, 900, 3), dtype=np.uint8), (5, 5), 0)
    ids = []
    for i, pixels in enumerate((scene[:, :600], scene[:, 300:])):
        path = folder / f"view{i}.png"
        Image.fromarray(pixels).save(path)
        ids.append(
            db.add_photo(
                folder_id=fid,
                filename=path.name,
                extension=".png",
                file_size=path.stat().st_size,
                file_mtime=path.stat().st_mtime,
                width=600,
                height=300,
            )
        )
    return db, folder, ids


def run_panorama(env, **overrides):
    db, folder, ids = env
    kwargs = dict(
        photo_ids=ids,
        destination="",
        output_format="png",
        input_size=2048,
        config={},
        checkpoint=lambda: None,
        progress=lambda *args: None,
    )
    kwargs.update(overrides)
    return panorama.create_panorama(db, str(folder.parent), **kwargs)


@pytest.mark.parametrize("output_format", ["png", "jpg"])
def test_real_stitch_preserves_sources_and_existing_outputs(panorama_env, output_format):
    _, folder, _ = panorama_env
    originals = {p.name: p.read_bytes() for p in folder.iterdir()}
    existing = folder / f"view0_panorama.{output_format}"
    existing.write_bytes(b"existing panorama")
    result = run_panorama(panorama_env, output_format=output_format)
    assert result["photo_count"] == 2
    assert result["width"] > 800
    assert 270 < result["height"] < 330
    assert Path(result["path"]).name == f"view0_panorama_2.{output_format}"
    with Image.open(result["path"]) as img:
        assert img.size == (result["width"], result["height"])
        assert img.mode == "RGB"
        # Color channels are not reversed by the BGR stitching round trip.
        expected = np.asarray(Image.open(folder / "view0.png"))
        actual = np.asarray(img)
        assert np.abs(actual[150, 150].astype(float) - expected[150, 150]).mean() < 35
    assert existing.read_bytes() == b"existing panorama"
    for name, data in originals.items():
        assert (folder / name).read_bytes() == data


def test_unrelated_photos_fail_without_output(panorama_env):
    _, folder, _ = panorama_env
    for path in folder.glob("*.png"):
        Image.new("RGB", (600, 300), "blue").save(path)
    with pytest.raises(ValueError, match="overlap"):
        run_panorama(panorama_env)
    assert not list(folder.glob("*panorama*"))


def test_no_silent_omission_of_selected_frames(panorama_env, monkeypatch):
    stitcher = Mock()
    stitcher.estimateTransform.return_value = 0
    stitcher.component.return_value = [0]
    monkeypatch.setattr(cv2, "Stitcher_create", lambda mode: stitcher)
    with pytest.raises(ValueError, match="Could not match all selected photos: view1.png"):
        run_panorama(panorama_env)
    stitcher.composePanorama.assert_not_called()


def test_cancel_after_save_removes_only_new_output(panorama_env):
    _, folder, _ = panorama_env
    existing = folder / "view0_panorama.png"
    existing.write_bytes(b"keep me")

    def checkpoint():
        if (folder / "view0_panorama_2.png").exists():
            raise JobCancelled()

    with pytest.raises(JobCancelled):
        run_panorama(panorama_env, checkpoint=checkpoint)
    assert not (folder / "view0_panorama_2.png").exists()
    assert existing.read_bytes() == b"keep me"


def test_missing_source_aborts_before_stitching(panorama_env, monkeypatch):
    _, folder, _ = panorama_env
    (folder / "view1.png").unlink()
    stitch = Mock()
    monkeypatch.setattr(cv2, "Stitcher_create", stitch)
    with pytest.raises(ValueError, match="view1.png: source file missing"):
        run_panorama(panorama_env)
    stitch.assert_not_called()


def test_missing_destination_not_recreated(panorama_env):
    _, folder, _ = panorama_env
    destination = folder / "offline"
    with pytest.raises(ValueError, match="unavailable"):
        run_panorama(panorama_env, destination=str(destination))
    assert not destination.exists()


def test_stitch_uses_edited_render_and_bounds_each_input(panorama_env, monkeypatch):
    db, folder, ids = panorama_env
    recipe = {"exposure": 1}
    monkeypatch.setattr(db, "get_photo_edit_recipes", lambda _: {ids[0]: recipe})
    loader = Mock(side_effect=lambda *args, **kwargs: Image.new("RGB", (5000, 100), "red"))
    monkeypatch.setattr(panorama, "load_export_image", loader)
    stitcher = Mock()

    def estimate(images):
        assert all(img.shape[1] == 2048 for img in images)
        assert tuple(images[0][0, 0]) == (0, 0, 255)
        return 1

    stitcher.estimateTransform.side_effect = estimate
    monkeypatch.setattr(cv2, "Stitcher_create", lambda _: stitcher)
    with pytest.raises(ValueError, match="overlap"):
        run_panorama(panorama_env)
    assert loader.call_args_list[0].kwargs["recipe"] == recipe


@pytest.mark.parametrize(
    "body",
    [
        None,
        [],
        {},
        {"photo_ids": [1]},
        {"photo_ids": list(range(1, 14))},
        {"photo_ids": [1, 1]},
        {"photo_ids": [True, 2]},
        {"photo_ids": [1.5, 2]},
        {"photo_ids": [0, 2]},
        {"photo_ids": [1, 2], "destination": "relative"},
        {"photo_ids": [1, 2], "destination": []},
        {"photo_ids": [1, 2], "format": []},
        {"photo_ids": [1, 2], "input_size": True},
        {"photo_ids": [1, 2], "input_size": 100000},
        {"photo_ids": [1, 2], "reveal": "false"},
    ],
)
def test_invalid_options(body):
    with pytest.raises(ValueError):
        panorama.validate_options(body)


def test_api_validation_and_workspace_boundaries(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    for body in ([], {"photo_ids": [1]}, {"photo_ids": [1, 999999]}):
        assert client.post("/api/jobs/panorama", json=body).status_code == 400
    other_ws = db.create_workspace("Other")
    db.set_active_workspace(other_ws)
    other_folder = db.add_folder("/other")
    foreign_id = db.add_photo(folder_id=other_folder, filename="other.jpg", extension=".jpg", file_size=1, file_mtime=1)
    response = client.post("/api/jobs/panorama", json={"photo_ids": [1, foreign_id]})
    assert response.status_code == 400
    assert "current workspace" in response.json["error"]
    assert not app._job_runner.list_jobs()


def test_api_job_result_and_progress(app_and_db, tmp_path, monkeypatch):
    app, db = app_and_db
    captured = {}

    def create(thread_db, vireo_dir, **kwargs):
        captured.update(kwargs)
        captured["workspace_id"] = thread_db._active_workspace_id
        kwargs["checkpoint"]()
        kwargs["progress"](5, 5, "Panorama saved")
        return {"path": str(tmp_path / "panorama.png"), "photo_count": 2, "width": 900, "height": 300}

    monkeypatch.setattr(panorama, "create_panorama", create)
    client = app.test_client()
    response = client.post("/api/jobs/panorama", json={"photo_ids": [1, 2], "reveal": False})
    assert response.status_code == 200
    job = wait_for_job_via_client(client, response.json["job_id"])
    assert job["status"] == "completed", job
    assert captured["photo_ids"] == [1, 2]
    assert captured["workspace_id"] == db._active_workspace_id
    assert job["result"]["width"] == 900
    assert job["result"]["revealed"] is False
    assert job["progress"]["current"] == 5


def test_save_failure_removes_partial_file(panorama_env, monkeypatch):
    def failing_save(img, stream, *args, **kwargs):
        stream.write(b"partial")
        raise OSError("disk full")

    monkeypatch.setattr(Image.Image, "save", failing_save)
    with pytest.raises(OSError, match="disk full"):
        run_panorama(panorama_env)
    assert not list(panorama_env[1].glob("*panorama*"))


def test_panorama_summary_contains_output_path():
    from job_summaries import describe_result

    result = describe_result(
        "panorama", {"path": "/pictures/result.png", "photo_count": 3, "width": 8000, "height": 2000}, {}
    )
    assert "3 photos" in str(result)
    assert "/pictures/result.png" in str(result)
