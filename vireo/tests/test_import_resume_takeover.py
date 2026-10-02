"""The rule that stops Resume on an interrupted import once a later run took
it over, on the server (``import_resume_takeover``) and on the Jobs page
(``importResumeTakeover`` in templates/jobs.html), which must agree."""
import json
import shutil
import subprocess
from pathlib import Path

import pytest
from services.imports import ImportService, import_resume_takeover

PARENT_ID = "parent"


def _parent(**marks):
    return {
        "id": PARENT_ID, "type": "import", "status": "failed",
        "started_at": "2026-09-01T10:00:00",
        "config": {"sources": ["/card"], "destination": "/arch"},
        "result": {"interrupted": True, "photo_ids": [1, 2], **marks},
    }


def _row(job_id, started_at, result, *, status="completed",
         parent=PARENT_ID, root=PARENT_ID, job_type="import"):
    config = {"sources": ["/card"], "destination": "/arch"}
    if parent is not None:
        config["parent_import_job_id"] = parent
    if root is not None:
        config["root_import_job_id"] = root
    return {
        "id": job_id, "type": job_type, "status": status,
        "started_at": started_at, "config": config, "result": result,
    }


DONE = {"ok": True, "photo_ids": [], "tags_applied": True, "chained": True}
INTERRUPTED = {"interrupted": True, "photo_ids": [3]}


def _scenarios():
    return {
        "no descendants": (_parent(), []),
        "finished resume": (
            _parent(), [_row("d", "2026-09-01T11:00:00", DONE)],
        ),
        "resume interrupted too": (
            _parent(),
            [_row("d", "2026-09-01T11:00:00", INTERRUPTED, status="failed")],
        ),
        "resume crashed before any work": (
            _parent(),
            [_row("d", "2026-09-01T11:00:00", {"error": "boom"},
                  status="failed")],
        ),
        "resume cancelled during copy": (
            _parent(),
            [_row("d", "2026-09-01T11:00:00",
                  {"cancelled": True, "photo_ids": [], "tags_applied": False,
                   "chained": False}, status="cancelled")],
        ),
        "resume cancelled after its tag pass": (
            _parent(),
            [_row("d", "2026-09-01T11:00:00",
                  {"cancelled": True, "photo_ids": [], "tags_applied": True,
                   "chained": False}, status="cancelled")],
        ),
        "resume failed files": (
            _parent(),
            [_row("d", "2026-09-01T11:00:00",
                  {"ok": False, "failed": 2, "photo_ids": [],
                   "tags_applied": True, "chained": True}, status="failed")],
        ),
        "resume failed files with tags owed": (
            _parent(),
            [_row("d", "2026-09-01T11:00:00",
                  {"ok": False, "failed": 2, "photo_ids": [],
                   "tags_applied": False, "chained": False}, status="failed")],
        ),
        "retry of the failed resume finished": (
            _parent(),
            [
                _row("d", "2026-09-01T11:00:00",
                     {"ok": False, "failed": 2, "photo_ids": [],
                      "tags_applied": True, "chained": True}, status="failed"),
                _row("r", "2026-09-01T12:00:00", DONE, parent="d"),
            ],
        ),
        "resume of the interrupted resume finished": (
            _parent(),
            [
                _row("d", "2026-09-01T11:00:00", INTERRUPTED, status="failed"),
                _row("d2", "2026-09-01T12:00:00", DONE, parent="d"),
            ],
        ),
        "finished resume from before marks were kept": (
            _parent(),
            [_row("d", "2026-09-01T11:00:00",
                  {"ok": True, "photo_ids": [], "collection_id": 7})],
        ),
        "legacy tag-only resume completed": (
            _parent(chained=True),
            [_row("d", "2026-09-01T11:00:00", {
                "ok": True, "photo_ids": [],
                "after_import_skipped": "chain already ran on the interrupted parent",
            })],
        ),
        "legacy tag-only resume with errors": (
            _parent(chained=True),
            [_row("d", "2026-09-01T11:00:00", {
                "ok": True, "photo_ids": [],
                "after_import_skipped": "chain already ran on the interrupted parent",
                "tagging": {"errors": ["tags failed"]},
            })],
        ),
        "failed resume from before marks were kept": (
            _parent(),
            [_row("d", "2026-09-01T11:00:00",
                  {"ok": False, "failed": 1, "photo_ids": []},
                  status="failed")],
        ),
        "descendant linked only by parent id": (
            _parent(),
            [_row("d", "2026-09-01T11:00:00", DONE, root=None)],
        ),
        "descendant linked only by root id": (
            _parent(),
            [_row("d3", "2026-09-01T13:00:00", DONE, parent="pruned")],
        ),
        "unrelated import": (
            _parent(),
            [_row("x", "2026-09-01T11:00:00", DONE, parent="other",
                  root="other")],
        ),
        "never-started row": (
            _parent(),
            [_row("d", "2026-09-01T11:00:00",
                  {**INTERRUPTED, "never_started": True}, status="failed")],
        ),
        "parent chained, resume applied the owed tags": (
            _parent(chained=True),
            [_row("d", "2026-09-01T11:00:00",
                  {"ok": True, "photo_ids": [], "tags_applied": True,
                   "chained": False}, status="cancelled")],
        ),
        "parent already did both itself": (
            _parent(chained=True, tags_applied=True), [],
        ),
        "newest of two finished resumes": (
            _parent(),
            [
                _row("d1", "2026-09-01T11:00:00", DONE),
                _row("d2", "2026-09-01T12:00:00", DONE),
            ],
        ),
        # ``_chain_after_import`` returns via its "import failed" branch
        # when ``ok`` is False, so the chain never actually enqueues
        # processing — but ``chained=True`` was still recorded on the
        # checkpoint. If a crash lands between that checkpoint and the
        # terminal row, the parent must stay resumable so the retry can
        # recover the failed files AND run processing.
        "parent chained mark discounted when ok is False": (
            {
                "id": PARENT_ID, "type": "import", "status": "failed",
                "started_at": "2026-09-01T10:00:00",
                "config": {"sources": ["/card"], "destination": "/arch"},
                "result": {
                    "interrupted": True, "photo_ids": [1, 2],
                    "ok": False, "failed": 1,
                    "tags_applied": True, "chained": True,
                },
            },
            [],
        ),
    }


def _server(parent, rows):
    takeover = import_resume_takeover(parent["id"], parent["result"], rows)
    resume = ImportService._interrupted_parent_resume(
        parent["config"], parent["result"], takeover,
    )
    return {
        "tags_applied": takeover["tags_applied"],
        "chained": takeover["chained"],
        "by": takeover["by"],
        "kind": takeover["kind"],
        "offers_resume": (
            takeover["by"] is None and resume is not None
            and isinstance(parent["result"].get("photo_ids"), list)
        ),
    }


EXPECTED = {
    "no descendants": (None, None, True),
    "finished resume": ("d", "done", False),
    "resume interrupted too": ("d", "resume", False),
    "resume crashed before any work": (None, None, True),
    "resume cancelled during copy": (None, None, True),
    "resume cancelled after its tag pass": (None, None, True),
    "resume failed files": ("d", "retry", False),
    "resume failed files with tags owed": (None, None, True),
    "retry of the failed resume finished": ("r", "done", False),
    "resume of the interrupted resume finished": ("d2", "done", False),
    "finished resume from before marks were kept": ("d", "done", False),
    "legacy tag-only resume completed": ("d", "done", False),
    "legacy tag-only resume with errors": (None, None, True),
    "failed resume from before marks were kept": (None, None, True),
    "descendant linked only by parent id": ("d", "done", False),
    "descendant linked only by root id": ("d3", "done", False),
    "unrelated import": (None, None, True),
    "never-started row": (None, None, True),
    "parent chained, resume applied the owed tags": ("d", "done", False),
    "parent already did both itself": (None, None, False),
    "newest of two finished resumes": ("d2", "done", False),
    "parent chained mark discounted when ok is False": (None, None, True),
}


@pytest.mark.parametrize("name", list(EXPECTED))
def test_takeover_rule(name):
    parent, rows = _scenarios()[name]
    got = _server(parent, rows)
    assert (got["by"], got["kind"], got["offers_resume"]) == EXPECTED[name]


def test_a_resume_that_paid_the_tags_leaves_only_the_chain_owed():
    """A resume cancelled after its tag pass applied the parent's tags, so
    resuming the parent again replays the chain but not the tags (which
    would overwrite locations the user corrected since)."""
    parent, rows = _scenarios()["resume cancelled after its tag pass"]
    takeover = import_resume_takeover(PARENT_ID, parent["result"], rows)
    resume = ImportService._interrupted_parent_resume(
        parent["config"], parent["result"], takeover,
    )
    assert resume["tags_applied"] is True
    assert resume["untagged_ids"] == []
    assert resume["chain_already_ran"] is False


def test_intermediate_resume_sees_only_its_own_descendants():
    """An interrupted resume is taken over by its own resume, not by a
    sibling that resumed the original: the sibling never carried the
    intermediate's landings."""
    rows = [
        _row("d", "2026-09-01T11:00:00", INTERRUPTED, status="failed"),
        _row("s", "2026-09-01T12:00:00", DONE),
    ]
    assert import_resume_takeover("d", INTERRUPTED, rows)["by"] is None
    rows.append(_row("d2", "2026-09-01T13:00:00", DONE, parent="d"))
    assert import_resume_takeover("d", INTERRUPTED, rows)["by"] == "d2"


@pytest.fixture
def node():
    executable = shutil.which("node")
    if executable is None:
        pytest.skip("Node.js is required for JavaScript regression tests")
    return executable


def test_jobs_page_agrees_with_the_server(node, tmp_path):
    """The Jobs page decides from the same history rows the server reads;
    every scenario must offer (or refuse) Resume the same way on both."""
    scenarios = _scenarios()
    names = list(scenarios)
    payload = [
        {"parent": scenarios[name][0], "rows": scenarios[name][1]}
        for name in names
    ]
    path = tmp_path / "scenarios.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    root = Path(__file__).resolve().parents[2]
    result = subprocess.run(
        [node, str(Path(__file__).with_name("jobs_import_resume.cjs")),
         str(path)],
        cwd=root, capture_output=True, encoding="utf-8", timeout=60,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    client = json.loads(result.stdout)
    for name, page in zip(names, client, strict=True):
        server = _server(*scenarios[name])
        assert {k: page[k] for k in server} == server, name
        if page["by"]:
            # The note names where to continue, never a bare "resumed".
            assert page["note"].startswith("Already resumed by the import started")


@pytest.mark.parametrize("status", ["completed", "failed"])
def test_terminal_runner_descendant_overrides_unpersisted_history(tmp_path, status):
    """The completion event can precede the terminal history write."""
    from unittest.mock import Mock

    from db import Database
    from jobs import JobRunner

    runner = Mock()
    row = _row("resume", "2026-09-01T11:00:00", DONE, status=status)
    with Database(str(tmp_path / "catalog.db")) as db:
        history_runner = JobRunner(db)
        assert history_runner.shutdown()
        row["workspace_id"] = db._active_workspace_id
        runner.list_jobs.return_value = [row]
        service = ImportService(
            lambda: runner, str(tmp_path / "catalog.db"), {},
            invalidate_missing_originals=Mock(), enqueue_process_job=Mock(),
            chain_after_move=Mock(), bulk_gps_location_payload=Mock(),
        )
        rows = service._import_resume_rows(db, PARENT_ID, {}, db._active_workspace_id)
        assert import_resume_takeover(PARENT_ID, _parent()["result"], rows)["by"] == "resume"


def test_descendant_chain_with_tag_errors_leaves_only_tag_replay():
    parent = _parent()
    child = _row("resume", "2026-09-01T11:00:00", {
        "ok": True, "photo_ids": [1, 2], "tags_applied": False,
        "chained": True, "tagging": {"errors": ["temporary lock"]},
    })
    takeover = import_resume_takeover(PARENT_ID, parent["result"], [child])
    resume = ImportService._interrupted_parent_resume(parent["config"], parent["result"], takeover)
    assert resume["chain_already_ran"] is True
    assert resume["tags_applied"] is False
