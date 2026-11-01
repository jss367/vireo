"""Preview warmup works without Flask and closes worker connections."""

from pathlib import Path

import config as cfg
import pytest
from db import Database
from PIL import Image
from services import photo_ingest, workspace_folders
from services import preview_warmup as warmup


class Runner:
    def __init__(self, cancelled=False):
        self.cancelled = cancelled
        self.progress = []

    def is_cancelled(self, job_id):
        return self.cancelled

    def push_event(self, job_id, event, payload):
        self.progress.append(payload)


def _photo(db, tmp_path, name):
    directory = tmp_path / name
    directory.mkdir()
    path = directory / f"{name}.jpg"
    Image.new("RGB", (40, 30), "green").save(path)
    folder = workspace_folders.add_folder(db, str(directory))
    return photo_ingest.add_photo(
        db, folder, path.name, ".jpg", path.stat().st_size, path.stat().st_mtime,
        width=40, height=30,
    )


@pytest.mark.parametrize("exit_path", ["success", "cancelled", "exception", "disabled"])
def test_service_binds_workspace_and_closes_database(db, tmp_path, monkeypatch, exit_path):
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    workspace = db.create_workspace("Preview workspace", config_overrides={
        "preview_max_size": 0 if exit_path == "disabled" else 1024,
        "preview_quality": 73,
    })
    db.commit()
    db.set_active_workspace(workspace)
    selected = _photo(db, tmp_path, "selected")
    other_workspace = db.create_workspace("Other")
    db.commit()
    db.set_active_workspace(other_workspace)
    other = _photo(db, tmp_path, "other")
    opened = []
    runner = Runner(cancelled=exit_path == "cancelled")

    def open_db():
        worker = Database(str(tmp_path / "test.db"))
        worker.set_active_workspace(workspace)
        opened.append(worker)
        return worker

    def broken(*args, **kwargs):
        raise RuntimeError("unexpected renderer failure")

    if exit_path == "exception":
        monkeypatch.setattr(warmup, "materialize_preview", broken)

    def work():
        return warmup.warm_previews(
            {"id": "service-test", "errors": []}, str(tmp_path / "media"),
            runner=runner, open_db=open_db, photo_ids=[selected, other],
        )

    if exit_path == "exception":
        with pytest.raises(RuntimeError, match="unexpected renderer failure"):
            work()
    else:
        result = work()
        assert result["ok"] is True
        assert result["errors_total"] == 0
        assert result["generated"] == (1 if exit_path == "success" else 0)
        assert result["total"] == (0 if exit_path == "disabled" else 1)
        if exit_path == "success":
            assert db.caches.preview_get(selected, 1024) is not None
            assert db.caches.preview_get(other, 1024) is None
            assert runner.progress[0]["current_file"] == "selected.jpg"
            assert runner.progress[0]["current"] == runner.progress[0]["total"] == 1
            assert (Path(tmp_path / "media" / "previews") / f"{selected}_1024.jpg").is_file()
    assert opened and all(worker.closed for worker in opened)
