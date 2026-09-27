"""Boundaries and shared-state behavior of the extracted pipeline stages."""

import ast
import inspect
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import config as cfg
import pipeline_job
import pipeline_stages
from db import Database
from PIL import Image
from pipeline_stages import media
from test_pipeline_job import FakeRunner, _make_job


def test_stages_do_not_import_the_orchestrator():
    for path in Path(pipeline_stages.__file__).parent.glob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        imports = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imports.extend(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom):
                imports.append(node.module or "")
        assert "pipeline_job" not in imports, path.name


def test_orchestrator_does_not_define_processing_stages():
    tree = ast.parse(inspect.getsource(pipeline_job.run_pipeline_job))
    stages = {
        node.name for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef) and node.name.endswith("_stage")
    }
    # This guard only rejects the retired import/archive mode; it does no work.
    assert stages <= {"archive_stage"}


def test_concurrent_runs_keep_their_created_collections(tmp_path, monkeypatch):
    """Later stages must read their own collection after both scans finish."""
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    db_path = str(tmp_path / "catalog.db")
    db = Database(db_path)
    workspaces = [db._active_workspace_id, db.create_workspace("Second run")]
    folders = []
    for index in range(2):
        folder = tmp_path / f"photos-{index}"
        folder.mkdir()
        Image.new("RGB", (32, 32), "black").save(folder / f"photo-{index}.jpg")
        folders.append(folder)

    previews_ready = threading.Barrier(2, timeout=20)
    previews = media.previews_stage
    observed = {}

    def synchronized_previews(run, **kwargs):
        # Force both collection stages to publish before either consumer reads.
        previews_ready.wait()
        observed[run.workspace_id] = run.collection_id
        return previews(run, **kwargs)

    monkeypatch.setattr(media, "previews_stage", synchronized_previews)

    def process(index):
        job = _make_job()
        job["id"] = f"pipeline-{index}"
        job["workspace_id"] = workspaces[index]
        params = pipeline_job.PipelineParams(
            source=str(folders[index]),
            skip_classify=True,
            skip_extract_masks=True,
            skip_regroup=True,
        )
        return pipeline_job.run_pipeline_job(
            job, FakeRunner(), db_path, workspaces[index], params,
        )

    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(process, range(2)))
        assert results[0]["collection_id"] != results[1]["collection_id"]
        for index, result in enumerate(results):
            workspace_id = workspaces[index]
            assert observed[workspace_id] == result["collection_id"]
            assert result["errors"] == []
            db.set_active_workspace(workspace_id)
            ids = db.get_collection_photo_ids(result["collection_id"])
            assert [db.get_photo(photo_id)["filename"] for photo_id in ids] == [
                f"photo-{index}.jpg",
            ]
    finally:
        db.close()
