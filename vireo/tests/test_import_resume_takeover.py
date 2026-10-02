"""The rule that stops Resume on an interrupted import, and Retry on one
that failed files, once a later run took it over: on the server
(``import_resume_takeover``) and on the Jobs page (``importResumeTakeover``
in templates/jobs.html), which must agree."""
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


def _failed_parent(**result):
    """A finished import that failed files: its chain step ran (and marked)
    but skipped processing, so its button is Retry."""
    return {
        "id": PARENT_ID, "type": "import", "status": "failed",
        "started_at": "2026-09-01T10:00:00",
        "config": {"sources": ["/card"], "destination": "/arch"},
        "result": {
            "ok": False, "failed": 2, "photo_ids": [1, 2],
            "tags_applied": True, "chained": True, **result,
        },
    }


DONE = {"ok": True, "photo_ids": [], "tags_applied": True, "chained": True}
INTERRUPTED = {"interrupted": True, "photo_ids": [3]}
FAILED_FILES = {"ok": False, "failed": 1, "photo_ids": [],
                "tags_applied": True, "chained": True}


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
                  {"ok": True, "photo_ids": [], "collection_id": 7, "process_job_id": "process-d"})],
        ),
        "legacy collection with failed processing handoff": (
            _parent(), [_row("d", "2026-09-01T11:00:00", {
                "ok": True, "collection_id": 7,
                "after_import_skipped": "failed to enqueue processing: unavailable",
            })],
        ),
        "descendant processing never started": (
            _parent(), [
                _row("d", "2026-09-01T11:00:00", {**DONE, "process_job_id": "process-d"}),
                _row("process-d", "2026-09-01T11:01:00", {"never_started": True},
                     status="failed", job_type="pipeline"),
            ],
        ),
        "legacy completed chain with unpaid tags": (
            _parent(), [_row("d", "2026-09-01T11:00:00", {
                "ok": True, "collection_id": 7, "process_job_id": "process-d",
                "tagging": {"errors": ["tag write failed"]},
            })],
        ),
        "legacy collection cancelled": (
            _parent(), [_row("d", "2026-09-01T11:00:00", {
                "ok": True, "collection_id": 7, "cancelled": True,
                "after_import_skipped": "import cancelled",
            }, status="cancelled")],
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
        # Retry: a finished import that failed files.
        "failed import, no retry yet": (_failed_parent(), []),
        "finished retry": (
            _failed_parent(), [_row("r", "2026-09-01T11:00:00", DONE)],
        ),
        "retry failed files too": (
            _failed_parent(),
            [_row("r", "2026-09-01T11:00:00", FAILED_FILES, status="failed")],
        ),
        "retry of the retry finished": (
            _failed_parent(),
            [
                _row("r", "2026-09-01T11:00:00", FAILED_FILES,
                     status="failed"),
                _row("r2", "2026-09-01T12:00:00", DONE, parent="r"),
            ],
        ),
        "retry interrupted": (
            _failed_parent(),
            [_row("r", "2026-09-01T11:00:00", INTERRUPTED, status="failed")],
        ),
        "retry crashed before any work": (
            _failed_parent(),
            [_row("r", "2026-09-01T11:00:00", {"error": "boom"},
                  status="failed")],
        ),
        "retry cancelled during copy": (
            _failed_parent(),
            [_row("r", "2026-09-01T11:00:00",
                  {"ok": False, "cancelled": True, "failed": 1,
                   "photo_ids": [], "tags_applied": False, "chained": False},
                  status="cancelled")],
        ),
        "retry cancelled after its tag pass": (
            _failed_parent(),
            [_row("r", "2026-09-01T11:00:00",
                  {"cancelled": True, "photo_ids": [], "tags_applied": True,
                   "chained": False}, status="cancelled")],
        ),
        "finished retry from before marks were kept": (
            _failed_parent(tags_applied=None, chained=None),
            [_row("r", "2026-09-01T11:00:00",
                  {"ok": True, "photo_ids": [], "collection_id": 7, "process_job_id": "process-d"})],
        ),
        "completed import offers no retry": (
            {**_failed_parent(), "status": "completed",
             "result": {"ok": True, "failed": 0, "photo_ids": [1],
                        "tags_applied": True, "chained": True}},
            [_row("r", "2026-09-01T11:00:00", DONE)],
        ),
    }


def _server(parent, rows):
    takeover = import_resume_takeover(parent["id"], parent["result"], rows)
    resume = ImportService._interrupted_parent_resume(
        parent["config"], parent["result"], takeover,
    )
    offers_resume = (
        takeover["by"] is None and resume is not None
        and isinstance(parent["result"].get("photo_ids"), list)
    )
    failed = parent["result"].get("failed")
    return {
        "tags_applied": takeover["tags_applied"],
        "chained": takeover["chained"],
        "by": takeover["by"],
        "kind": takeover["kind"],
        "offers_resume": offers_resume,
        # The server accepts a Retry of this row unless taken over.
        "offers_retry": (
            isinstance(failed, int) and failed > 0
            and not offers_resume and takeover["by"] is None
        ),
    }


# (by, kind, offers Resume, offers Retry)
EXPECTED = {
    "no descendants": (None, None, True, False),
    "finished resume": ("d", "done", False, False),
    "resume interrupted too": ("d", "resume", False, False),
    "resume crashed before any work": (None, None, True, False),
    "resume cancelled during copy": (None, None, True, False),
    "resume cancelled after its tag pass": (None, None, True, False),
    "resume failed files": ("d", "retry", False, False),
    "resume failed files with tags owed": (None, None, True, False),
    "retry of the failed resume finished": ("r", "done", False, False),
    "resume of the interrupted resume finished": ("d2", "done", False, False),
    "finished resume from before marks were kept": ("d", "done", False, False),
    "legacy collection with failed processing handoff": (None, None, True, False),
    "descendant processing never started": (None, None, True, False),
    "legacy completed chain with unpaid tags": (None, None, True, False),
    "legacy collection cancelled": (None, None, True, False),
    "legacy tag-only resume completed": ("d", "done", False, False),
    "legacy tag-only resume with errors": (None, None, True, False),
    "failed resume from before marks were kept": (None, None, True, False),
    "descendant linked only by parent id": ("d", "done", False, False),
    "descendant linked only by root id": ("d3", "done", False, False),
    "unrelated import": (None, None, True, False),
    "never-started row": (None, None, True, False),
    "parent chained, resume applied the owed tags": ("d", "done", False, False),
    "parent already did both itself": (None, None, False, False),
    "newest of two finished resumes": ("d2", "done", False, False),
    "failed import, no retry yet": (None, None, False, True),
    "finished retry": ("r", "done", False, False),
    "retry failed files too": ("r", "retry", False, False),
    "retry of the retry finished": ("r2", "done", False, False),
    "retry interrupted": ("r", "resume", False, False),
    "retry crashed before any work": (None, None, False, True),
    "retry cancelled during copy": (None, None, False, True),
    "retry cancelled after its tag pass": (None, None, False, True),
    "finished retry from before marks were kept": ("r", "done", False, False),
    "completed import offers no retry": (None, None, False, False),
    "parent chained mark discounted when ok is False": (None, None, True, False),

}


@pytest.mark.parametrize("name", list(EXPECTED))
def test_takeover_rule(name):
    parent, rows = _scenarios()[name]
    got = _server(parent, rows)
    assert (
        got["by"], got["kind"], got["offers_resume"], got["offers_retry"],
    ) == EXPECTED[name]


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
            # The note says what happened and names where to continue.
            verb = (
                "resumed" if scenarios[name][0]["result"].get("interrupted")
                else "retried"
            )
            assert page["note"].startswith(
                f"Already {verb} by the import started"), page["note"]


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


def test_partial_descendant_landings_are_inherited_for_remaining_chain():
    parent = _parent(landed_files={"/arch/parent.jpg": [1, 2, "hash-parent"]})
    descendant = _row("d", "2026-09-01T11:00:00", {
        "ok": True, "tags_applied": True, "chained": False,
        "photo_ids": [3], "landed_files": {"/arch/child.jpg": [3, 4, "hash-child"]},
    })
    takeover = import_resume_takeover(PARENT_ID, parent["result"], [descendant])
    resume = ImportService._interrupted_parent_resume(parent["config"], parent["result"], takeover)
    assert takeover["by"] is None
    assert resume["tags_applied"] is True
    assert resume["chain_already_ran"] is False
    assert resume["landed_files"] == {
        "/arch/parent.jpg": [1, 2, "hash-parent"],
        "/arch/child.jpg": [3, 4, "hash-child"],
    }


def test_legacy_processing_with_tag_errors_leaves_only_tag_replay():
    parent, rows = _scenarios()["legacy completed chain with unpaid tags"]
    takeover = import_resume_takeover(PARENT_ID, parent["result"], rows)
    resume = ImportService._interrupted_parent_resume(parent["config"], parent["result"], takeover)
    assert takeover["by"] is None
    assert resume["tags_applied"] is False
    assert resume["chain_already_ran"] is True


def test_takeover_reads_never_started_descendant_pipeline_history(tmp_path):
    from unittest.mock import Mock

    from db import Database
    from jobs import JobRunner

    with Database(str(tmp_path / "catalog.db")) as db:
        history_runner = JobRunner(db)
        assert history_runner.shutdown()
        runner = Mock()
        runner.list_jobs.return_value = []
        child = _row("d", "2026-09-01T11:00:00", {**DONE, "process_job_id": "process-d"})
        process = _row("process-d", "2026-09-01T11:01:00", {"never_started": True},
                       status="failed", job_type="pipeline")
        for row in (child, process):
            db.conn.execute(
                "INSERT INTO job_history (id,type,status,started_at,config,result,workspace_id) VALUES (?,?,?,?,?,?,?)",
                (row["id"], row["type"], row["status"], row["started_at"],
                 json.dumps(row["config"]), json.dumps(row["result"]), db._active_workspace_id),
            )
        db.conn.commit()
        service = ImportService(lambda: runner, db._db_path, {},
                                invalidate_missing_originals=Mock(), enqueue_process_job=Mock(),
                                chain_after_move=Mock(), bulk_gps_location_payload=Mock())
        rows = service._import_resume_rows(db, PARENT_ID, {}, db._active_workspace_id)
        assert {row["id"] for row in rows} == {"d", "process-d"}
        takeover = import_resume_takeover(PARENT_ID, _parent()["result"], rows)
        assert takeover["by"] is None
        assert takeover["chained"] is False


@pytest.mark.parametrize("unpersisted_child", [False, True])
def test_takeover_fetches_transitive_legacy_descendants(tmp_path, unpersisted_child):
    from unittest.mock import Mock

    from db import Database
    from jobs import JobRunner

    with Database(str(tmp_path / "catalog.db")) as db:
        history_runner = JobRunner(db)
        assert history_runner.shutdown()
        runner = Mock()
        runner.list_jobs.return_value = []
        child = _row("legacy-child", "2026-09-01T11:00:00",
                     {"ok": False, "failed": 1, "photo_ids": [3]}, root=None, status="failed")
        grandchild = _row("grandchild", "2026-09-01T12:00:00", DONE,
                          parent="legacy-child", root="legacy-child")
        child["workspace_id"] = db._active_workspace_id
        runner.list_jobs.return_value = [child] if unpersisted_child else []
        for row in ((grandchild,) if unpersisted_child else (child, grandchild)):
            db.conn.execute(
                "INSERT INTO job_history (id,type,status,started_at,config,result,workspace_id) VALUES (?,?,?,?,?,?,?)",
                (row["id"], row["type"], row["status"], row["started_at"],
                 json.dumps(row["config"]), json.dumps(row["result"]), db._active_workspace_id),
            )
        db.conn.commit()
        service = ImportService(lambda: runner, db._db_path, {},
                                invalidate_missing_originals=Mock(), enqueue_process_job=Mock(),
                                chain_after_move=Mock(), bulk_gps_location_payload=Mock())
        rows = service._import_resume_rows(db, PARENT_ID, {}, db._active_workspace_id)
        assert {row["id"] for row in rows} == {"legacy-child", "grandchild"}
        assert import_resume_takeover(PARENT_ID, _parent()["result"], rows)["by"] == "grandchild"


@pytest.mark.parametrize("current", [
    "/nas/child.jpg|s=12|h=original", "/nas/child.jpg|s=12|h=replaced",
])
def test_moved_descendant_recovers_only_matching_bytes(current):
    from unittest.mock import Mock

    service = ImportService(lambda: Mock(), "unused", {},
                            invalidate_missing_originals=Mock(), enqueue_process_job=Mock(),
                            chain_after_move=Mock(), bulk_gps_location_payload=Mock())
    child = _row("d", "2026-09-01T11:00:00", {
        "ok": True, "tags_applied": True, "chained": False,
        "photo_ids": [3], "photo_fingerprints": {"3": "/local/child.jpg|s=12|h=original"},
    })
    takeover = import_resume_takeover(PARENT_ID, _parent()["result"], [child])
    service._capture_photo_fingerprints_for_ids = Mock(return_value={3: current})
    service._recover_relocated_descendant_landings(Mock(), takeover)
    resume = service._interrupted_parent_resume(_parent()["config"], _parent()["result"], takeover)
    assert ("/nas/child.jpg" in resume["landed_files"]) == current.endswith("h=original")
