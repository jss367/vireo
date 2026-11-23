"""The MCP server: Vireo's catalog as tools for Claude Code and friends.

Most tests bind the tools to a Flask test client (no sockets) and check what
they send to the app and what they hand back. The last one is the real
thing: the test app served on a loopback port, ``python vireo/app.py --mcp``
spawned as an MCP client would spawn it, and a stdio session driven with the
SDK's client.
"""

import base64
import io
import json
import os
import shlex
import sys
import threading
import tomllib
from pathlib import Path

import anyio
import mcp_server
import pytest
from mcp import types as mcp_types
from mcp.client.session import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client
from mcp.server.mcpserver.exceptions import ToolError
from mcp_server import (
    BAD_TOKEN_MESSAGE,
    NOT_RUNNING_MESSAGE,
    VireoClient,
    build_server,
    connect_info,
    keyword_paths,
    photo_detail,
    photo_summary,
    server_command,
)
from PIL import Image as PILImage
from services import keyword_tagging
from testing.waits import synchronization_timeout

TOKEN = "test-token-123"
REPO = Path(__file__).resolve().parents[2]
APP = REPO / "vireo" / "app.py"


class FlaskTransport(VireoClient):
    """``VireoClient`` whose requests go through a Flask test client, and
    which records every call so tests can assert the bodies."""

    def __init__(self, app, token=TOKEN):
        super().__init__(base_url="http://test", token=token)
        self._client = app.test_client()
        self._token = token
        self.calls = []

    def request(self, method, path, params=None, json_body=None):
        self.calls.append((method, path, params, json_body))
        response = self._client.open(
            path,
            method=method,
            query_string={k: v for k, v in (params or {}).items() if v is not None},
            json=json_body,
            headers={"X-Vireo-Token": self._token},
        )
        return response.status_code, response.headers, response.get_data()


@pytest.fixture
def mcp(app_and_db):
    app, db = app_and_db
    transport = FlaskTransport(app)
    return build_server(transport), db, transport


def call(server, tool_name, **arguments):
    """Call a tool and return its parsed JSON (or the content blocks)."""
    result = anyio.run(server.call_tool, tool_name, arguments)
    assert not result.is_error, result.content
    if result.content and result.content[0].type == "text":
        return json.loads(result.content[0].text)
    return result.content


def call_blocks(server, tool_name, **arguments):
    """Call a tool that returns several content blocks."""
    result = anyio.run(server.call_tool, tool_name, arguments)
    assert not result.is_error, result.content
    return result.content


def call_error(server, tool_name, **arguments):
    with pytest.raises(ToolError) as excinfo:
        anyio.run(server.call_tool, tool_name, arguments)
    return str(excinfo.value)


def test_tools_are_the_documented_set(mcp):
    server, _, _ = mcp
    names = {tool.name for tool in anyio.run(server.list_tools)}
    assert names == {
        "vireo_status", "list_workspaces", "activate_workspace", "list_folders",
        "list_collections", "search_keywords", "search_photos",
        "find_photos_by_text", "get_photo", "view_photo", "burst", "list_bursts", "focus_crop",
        "set_flag", "set_rating", "add_keyword", "remove_keyword",
        "get_species_evidence", "preview_species_assignment", "apply_species_assignment",
        "list_assignment_conflicts", "review_species_assignment", "reject_region", "restore_region",
        "best_of_each_species", "choose_best_photo",
        "get_edit", "suggest_crop", "suggest_tone", "view_edit", "save_edit", "save_edit_copy",
        "list_export_presets", "preview_export", "start_export", "get_job",
    }
    for tool in anyio.run(server.list_tools):
        assert tool.description, f"{tool.name} has no description"
        # The context parameter is injected, never asked of the client.
        assert "ctx" not in tool.input_schema["properties"], tool.name


def test_species_tools_advertise_side_effects_to_clients(mcp):
    server, _, _ = mcp
    tools = {tool.name: tool for tool in anyio.run(server.list_tools)}
    for name in ("get_species_evidence", "preview_species_assignment"):
        hints = tools[name].model_dump(by_alias=True)["annotations"]
        assert hints["readOnlyHint"] is True
    hints = tools["apply_species_assignment"].model_dump(by_alias=True)["annotations"]
    assert hints["readOnlyHint"] is False
    assert hints["destructiveHint"] is False


def test_status_reports_version_workspace_and_count(mcp, monkeypatch):
    server, db, _ = mcp
    # Status never names the catalog path, override or not.
    monkeypatch.delenv("VIREO_URL", raising=False)
    status = call(server, "vireo_status")
    assert status["running"] is True
    assert status["version"]
    assert status["active_workspace"]["id"] == db.active_workspace_id
    assert status["photo_count"] == 3
    assert "db_path" not in status


def test_frozen_status_and_api_versions_match_release_manifest(mcp, monkeypatch, tmp_path):
    """API and connector versions must identify the same packaged desktop release."""
    server, _, transport = mcp
    # A sidecar extracts this synchronized manifest at its PyInstaller root.
    # Distribution metadata from the build environment can be stale.
    manifest = tomllib.loads((REPO / "pyproject.toml").read_text())
    desktop = json.loads((REPO / "src-tauri/tauri.conf.json").read_text())
    expected = desktop["version"]
    assert manifest["project"]["version"] == expected
    (tmp_path / "pyproject.toml").write_bytes((REPO / "pyproject.toml").read_bytes())
    commit = "a" * 40
    (tmp_path / "build-commit.txt").write_text(commit + "\n")
    monkeypatch.setattr(sys, "frozen", True, raising=False)
    monkeypatch.setattr(sys, "_MEIPASS", str(tmp_path), raising=False)
    from web import system

    monkeypatch.setattr(system, "package_version", lambda _: "0.0.0")
    status = call(server, "vireo_status")
    assert status["version"] == expected
    assert status["build_commit"] == commit
    for route in ("/api/version", "/api/v1/version"):
        response = transport._client.get(route, headers={"X-Vireo-Token": TOKEN})
        assert response.status_code == 200
        assert response.get_json() == {"version": expected, "build_commit": commit}


def test_status_omits_db_path_when_an_override_targets_another_instance(mcp, monkeypatch):
    # With VIREO_URL in play the local runtime.json names a different
    # instance's catalog; mixing it into the status misnames the target.
    server, _, _ = mcp
    monkeypatch.setenv("VIREO_URL", "http://127.0.0.1:9999/")
    monkeypatch.setattr(
        mcp_server, "read_runtime_json", lambda: {"db_path": "/wrong/catalog.db"}
    )
    status = call(server, "vireo_status")
    assert "db_path" not in status


def test_workspaces_mark_the_active_one(mcp):
    server, db, _ = mcp
    listing = call(server, "list_workspaces")
    assert [ws["active"] for ws in listing["workspaces"]] == [True]
    assert set(listing["workspaces"][0]) == {"id", "name", "active"}


def test_search_photos_returns_trimmed_summaries(mcp):
    server, _, transport = mcp
    data = call(server, "search_photos", per_page=2)
    assert data["total"] == 3
    assert len(data["photos"]) == 2
    photo = data["photos"][0]
    assert {"id", "filename", "folder_id", "rating", "flag", "quality_analyzed",
            "analysis_status", "analysis_reasons", "quality_preliminary"} <= set(photo)
    # Nothing has been processed: not analyzed, and the app says why.
    assert photo["quality_analyzed"] is False
    assert photo["analysis_status"] == "not_run"
    assert photo["analysis_reasons"] == ["detection_not_run"]
    assert photo["quality_preliminary"] is False
    assert "analysis_explanation" not in photo
    for noisy in ("exif_data", "thumb_path", "render_key", "life_list", "xmp_mtime"):
        assert noisy not in photo
    _, path, params, _ = _photos_call(transport)
    assert path == "/api/photos"
    assert params["per_page"] == 2
    # One readiness call for the page, with exactly the page's ids.
    readiness_calls = [c for c in transport.calls if c[1] == mcp_server.READINESS_PATH]
    assert len(readiness_calls) == 1
    assert readiness_calls[0][3] == {"photo_ids": [p["id"] for p in data["photos"]]}


def _photos_call(transport, path="/api/photos"):
    """The latest recorded request to ``path`` (the readiness POST follows it)."""
    return next(c for c in reversed(transport.calls) if c[1] == path)


def test_search_photos_passes_filters_through(mcp):
    server, _, transport = mcp
    data = call(server, "search_photos", rating_min=4, flag="none")
    assert [p["filename"] for p in data["photos"]] == ["bird3.jpg"]
    _, _, params, _ = _photos_call(transport)
    assert params["rating_min"] == 4 and params["flag"] == "none"
    assert "Vireo answered 400" in call_error(server, "search_photos", flag="maybe")


def test_search_photos_caps_the_page_size(mcp):
    server, _, transport = mcp
    call(server, "search_photos", per_page=10_000)
    assert _photos_call(transport)[2]["per_page"] == mcp_server.MAX_PAGE_SIZE


def test_get_photo_trims_and_optionally_includes_exif(mcp):
    server, db, _ = mcp
    pid = db.photos.list_page()[0]["id"]
    detail = call(server, "get_photo", photo_id=pid)
    assert detail["id"] == pid
    assert detail["path"].endswith(detail["filename"])
    assert [k["name"] for k in detail["keywords"]] == ["Cardinal"]
    assert detail["exposure"] == {}
    assert "exif" not in detail and "metadata" not in detail
    with_exif = call(server, "get_photo", photo_id=pid, include_exif=True)
    assert "exif" in with_exif
    assert "Vireo answered 404" in call_error(server, "get_photo", photo_id=999_999)


def test_photo_detail_pulls_exposure_out_of_exif_groups():
    detail = photo_detail({
        "id": 1, "filename": "a.jpg",
        "metadata": {
            "Composite": {"Aperture": 5.6, "ShutterSpeed": "1/2000"},
            "EXIF": {"ExposureTime": "1/2000", "FNumber": 5.6, "FocalLength": "500.0 mm",
                     "Make": "Nikon"},
        },
        "detections": [{"id": 7, "category": "animal", "confidence": 0.9,
                        "detector_model": "md", "x": 0.1}],
        "edit_recipe": '{"adjustments": {}}',
    })
    assert detail["exposure"] == {
        "Aperture": 5.6, "ShutterSpeed": "1/2000", "ExposureTime": "1/2000",
        "FNumber": 5.6, "FocalLength": "500.0 mm",
    }
    assert detail["detections"] == [
        {"id": 7, "category": "animal", "confidence": 0.9, "detector_model": "md"}
    ]
    assert detail["has_edits"] is True and detail["subjects_detected"] == 1
    assert "exif" not in detail


def test_photo_detail_preserves_species_assignment_provenance():
    assignment = {"photo_id": 1, "species": "Robin", "method": "adjacent_frames",
                  "reviewer": "Reviewer", "supporting_photo_ids": [2], "confidence": None}
    assert photo_detail({"id": 1, "species_assignments": [assignment]})["species_assignments"] == [assignment]


def test_species_evidence_tool_uses_exact_scope(mcp):
    server, _, transport = mcp
    result = call(server, "get_species_evidence", photo_ids=[3, 1])
    assert result["photo_ids"] == [3, 1]
    assert [p["id"] for p in result["photos"]] == [3, 1]
    assert result["automatic_assignment_eligible"] is False
    assert transport.calls[-1][1:] == ("/api/photos/species-evidence", None, {"photo_ids": [3, 1]})
    assert "unique" in call_error(server, "get_species_evidence", photo_ids=[1, 1])


def test_species_tools_preview_apply_and_stale_provenance(mcp, app_and_db):
    from test_app import _seed_encounter_cache

    server, db, transport = mcp
    app, _ = app_and_db
    _seed_encounter_cache(app, db, [1, 2, 3])
    db.photo_review.set_flag(1, "flagged")
    db.commit()
    provenance = {"method": "direct_visual", "reviewer": "Test agent", "reason": "Visible plumage"}
    preview = call(server, "preview_species_assignment", photo_ids=[1], species="Robin", provenance=provenance)
    assert db.species_provenance.assignments(1) == []
    assert preview["provenance"]["reviewer"] == "Test agent"
    assert "409" in call_error(server, "apply_species_assignment", photo_ids=[1], species="Robin",
                               provenance=provenance | {"reviewer": "Different reviewer"},
                               preview_token=preview["preview_token"])
    assert db.species_provenance.assignments(1) == []
    result = call(server, "apply_species_assignment", photo_ids=[1], species="Robin", provenance=provenance,
                  preview_token=preview["preview_token"])
    assert result["photo_ids"] == [1]
    assert result["response_mode"] == "compact"
    assert result["undo"]["edit_id"]
    assert db.species_provenance.assignments(1)[0]["reviewer"] == "Test agent"
    assert db.species_provenance.assignments(2) == []
    assert db.photos.get(1)["flag"] == "flagged"
    assert transport.calls[-1][3]["add"] is True
    assert "required" in call_error(server, "apply_species_assignment", photo_ids=[1], species="Robin",
                                    provenance=provenance, preview_token="")
    assert app.test_client().post("/api/undo", json={"edit_id": result["undo"]["edit_id"]}).status_code == 200
    assert db.species_provenance.assignments(1) == []
    assert db.photos.get(1)["flag"] == "flagged"


def test_explicit_subject_view_never_falls_back_to_default_subject(mcp):
    server, _, transport = mcp
    assert "404" in call_error(server, "view_photo", photo_id=1, view="subject", detection_id=99999)
    assert transport.calls[-1][1:3] == ("/photos/1/crop", {"detection_id": 99999})
    assert "requires" in call_error(server, "view_photo", photo_id=1, view="preview", detection_id=99999)


def test_species_preview_binds_existing_assignment_evidence(mcp, app_and_db):
    from test_app import _seed_encounter_cache

    server, db, _ = mcp
    app, _ = app_and_db
    _seed_encounter_cache(app, db, [1, 2])
    keyword = db.keywords.add("Mallard", is_species=True)
    keyword_tagging.tag_photo(db, 1, keyword)
    db.commit()
    provenance = {"method": "direct_visual", "reviewer": "Agent", "reason": "Visible plumage"}
    preview = call(server, "preview_species_assignment", photo_ids=[1], species="Robin", provenance=provenance)
    response = app.test_client().post(f"/api/photos/1/species/{keyword}/provenance", json={
        "method": "direct_visual", "reviewer": "User", "reason": "Updated identification evidence",
    })
    assert response.status_code == 200
    assert "409" in call_error(server, "apply_species_assignment", photo_ids=[1], species="Robin",
                               provenance=provenance, preview_token=preview["preview_token"])
    assignments = db.species_provenance.assignments(1)
    assert len(assignments) == 1
    assert assignments[0]["species"] == "Mallard"
    assert assignments[0]["reviewer"] == "User"


def test_species_tools_preserve_existing_user_evidence_in_mixed_selection(mcp, app_and_db):
    from test_app import _seed_encounter_cache

    server, db, _ = mcp
    app, _ = app_and_db
    _seed_encounter_cache(app, db, [1, 2, 3])
    keyword = db.keywords.add("Robin", is_species=True)
    keyword_tagging.tag_photo(db, 1, keyword)
    db.commit()
    assert app.test_client().post(f"/api/photos/1/species/{keyword}/provenance", json={
        "method": "direct_visual", "reviewer": "User", "reason": "User-reviewed identification",
    }).status_code == 200
    original = db.species_provenance.assignments(1)
    provenance = {"method": "adjacent_frames", "reviewer": "Agent", "reason": "Reviewed sequence",
                  "supporting_photo_ids": [1]}
    preview = call(server, "preview_species_assignment", photo_ids=[1, 2], species="Robin", provenance=provenance)
    assert preview["preserve_existing_provenance"] is True
    assert preview["photos"][0]["additions"] == []
    assert preview["photos"][1]["additions"] == ["Robin"]
    body = {"photo_ids": [1, 2], "source_photo_ids": [1, 2], "species": "Robin", "provenance": provenance,
            "add": True, "preserve_existing_provenance": False, "preview_token": preview["preview_token"]}
    assert app.test_client().post("/api/encounters/species", json=body).status_code == 409
    result = call(server, "apply_species_assignment", photo_ids=[1, 2], species="Robin", provenance=provenance,
                  preview_token=preview["preview_token"])
    assert db.species_provenance.assignments(1) == original
    assert db.species_provenance.assignments(2)[0]["reviewer"] == "Agent"
    assert db.species_provenance.assignments(3) == []
    assert app.test_client().post("/api/undo", json={"edit_id": result["undo"]["edit_id"]}).status_code == 200
    assert db.species_provenance.assignments(1) == original
    assert db.species_provenance.assignments(2) == []


@pytest.mark.parametrize("provenance", [
    {"method": "direct_visual", "reason": "Visible plumage"},
    {"method": "direct_visual", "reviewer": "Agent"},
    {"method": "manual_unspecified", "reviewer": "Agent", "reason": "No inspected evidence"},
    {"method": "model_proposed", "reviewer": "Agent", "reason": "Unreviewed prediction"},
])
def test_species_tools_require_attributed_identification_evidence(mcp, provenance):
    server, _, transport = mcp
    assert "provenance" in call_error(server, "preview_species_assignment", photo_ids=[1],
                                      species="Robin", provenance=provenance)
    assert transport.calls == []


@pytest.mark.parametrize("kind", ["general", "untyped_taxonomy", "genus"])
def test_species_tools_reject_shared_keyword_promotion_or_rebinding(mcp, app_and_db, kind):
    from pipeline import load_results_raw
    from test_app import _seed_encounter_cache

    server, db, _ = mcp
    app, _ = app_and_db
    _seed_encounter_cache(app, db, [1, 2, 3])
    db.conn.executemany("INSERT INTO taxa (id, name, common_name, rank) VALUES (?, ?, ?, ?)", [
        (900001, "Reviewus", "Review bird", "genus"),
        (900002, "Reviewus birdus", "Review bird", "species"),
    ])
    keyword = db.conn.execute(
        "INSERT INTO keywords (name, type, is_species, taxon_id) VALUES ('Review bird', ?, ?, ?)",
        ("general" if kind == "general" else "taxonomy", int(kind == "genus"),
         900001 if kind == "genus" else None),
    ).lastrowid
    db.commit()
    keyword_tagging.tag_photo(db, 3, keyword)
    db.commit()
    original = db.keywords.species_assignment_identity(keyword)
    original_species = db.photo_species.get_species_for_photos([1, 2, 3])
    original_history = db.edit_history.list_recent()
    original_cache = load_results_raw(os.path.dirname(db._db_path), db.active_workspace_id)
    provenance = {"method": "direct_visual", "reviewer": "Agent", "reason": "Visible plumage"}
    for tool, extra in [("preview_species_assignment", {}), ("apply_species_assignment", {"preview_token": "unused"})]:
        error = call_error(server, tool, photo_ids=[1], species="Review bird", provenance=provenance, **extra)
        assert "409" in error and "keyword management" in error
    assert db.keywords.species_assignment_identity(keyword) == original
    assert db.photo_species.get_species_for_photos([1, 2, 3]) == original_species
    assert db.edit_history.list_recent() == original_history
    assert load_results_raw(os.path.dirname(db._db_path), db.active_workspace_id) == original_cache


@pytest.mark.parametrize("linked", [False, True])
def test_species_tools_reuse_shared_species_metadata_through_undo_and_redo(mcp, app_and_db, linked):
    from test_app import _seed_encounter_cache

    server, db, _ = mcp
    app, _ = app_and_db
    _seed_encounter_cache(app, db, [1, 2, 3])
    db.conn.execute("INSERT INTO taxa (id, name, common_name, rank) VALUES (900002, 'Reviewus birdus', 'Review bird', 'species')")
    keyword = db.conn.execute(
        "INSERT INTO keywords (name, type, is_species, taxon_id) VALUES ('Review bird', 'taxonomy', ?, ?)",
        (0 if linked else 1, 900002 if linked else None),
    ).lastrowid
    db.commit()
    keyword_tagging.tag_photo(db, 3, keyword)
    db.commit()
    original = db.keywords.species_assignment_identity(keyword)
    outside = db.species_provenance.assignments(3)
    provenance = {"method": "direct_visual", "reviewer": "Agent", "reason": "Visible plumage"}
    preview = call(server, "preview_species_assignment", photo_ids=[1], species="Review bird", provenance=provenance)
    assert preview["preserve_keyword_metadata"] is True
    client = app.test_client()
    body = {"photo_ids": [1], "source_photo_ids": [1], "species": "Review bird", "provenance": provenance,
            "add": True, "preserve_existing_provenance": True, "preserve_keyword_metadata": False,
            "preview_token": preview["preview_token"]}
    assert client.post("/api/encounters/species", json=body).status_code == 409
    result = call(server, "apply_species_assignment", photo_ids=[1], species="Review bird", provenance=provenance,
                  preview_token=preview["preview_token"])
    assert db.keywords.species_assignment_identity(keyword) == original
    assert db.species_provenance.assignments(1)[0]["keyword_id"] == keyword
    assert db.species_provenance.assignments(2) == []
    assert db.species_provenance.assignments(3) == outside
    assert client.post("/api/undo", json={"edit_id": result["undo"]["edit_id"]}).status_code == 200
    assert db.species_provenance.assignments(1) == []
    assert db.keywords.species_assignment_identity(keyword) == original
    assert db.species_provenance.assignments(3) == outside
    assert client.post("/api/redo").status_code == 200
    assert db.species_provenance.assignments(1)[0]["keyword_id"] == keyword
    assert db.keywords.species_assignment_identity(keyword) == original
    assert db.species_provenance.assignments(3) == outside


@pytest.mark.parametrize("method", [[], {}, None, 1, True])
def test_species_tools_reject_non_string_methods_before_http(mcp, method):
    server, _, transport = mcp
    provenance = {"method": method, "reviewer": "Agent", "reason": "Visible plumage"}
    assert "provenance.method must be" in call_error(
        server, "preview_species_assignment", photo_ids=[1], species="Robin", provenance=provenance,
    )
    assert transport.calls == []


def test_photo_summary_takes_analysis_state_from_readiness_not_the_score():
    complete = {"status": "complete", "eligible": True, "reasons": [], "explanation": "done"}
    partial = {"status": "incomplete", "eligible": False,
               "reasons": ["segmentation_incomplete"], "explanation": "missing"}
    assert photo_summary({"id": 1, "quality_score": 0.4}, complete) == {
        "id": 1, "quality_score": 0.4, "quality_analyzed": True,
        "analysis_status": "complete", "analysis_reasons": [], "quality_preliminary": False,
    }
    # A score written mid-run is preliminary, never "analyzed".
    preliminary = photo_summary({"id": 1, "quality_score": 0.4}, partial)
    assert preliminary["quality_analyzed"] is False
    assert preliminary["analysis_status"] == "incomplete"
    assert preliminary["quality_preliminary"] is True
    # A completed detector run without a qualifying subject is complete but ineligible.
    no_subject = {"status": "complete", "eligible": False,
                  "reasons": ["no_qualifying_subject"], "explanation": "review"}
    assert photo_summary({"id": 1}, no_subject)["quality_analyzed"] is False
    # No readiness at all: unknown, with the reason spelled out, and never True.
    unknown = photo_summary({"id": 1, "quality_score": 0.4})
    assert unknown["quality_analyzed"] is False and unknown["analysis_status"] == "unknown"
    assert unknown["quality_preliminary"] is True
    assert unknown["analysis_explanation"] == mcp_server.READINESS_UNCHECKED
    failed = photo_summary({"id": 1}, {"error": "Vireo answered 400: nope"})
    assert failed["analysis_status"] == "unknown" and failed["quality_preliminary"] is False
    assert failed["analysis_explanation"] == "Vireo answered 400: nope"
    assert photo_detail({"id": 1}, readiness=complete)["analysis_explanation"] == "done"
    # Any surfaced quality number (not just quality_score) counts as preliminary
    # when analysis is incomplete: /api/jobs/sharpness writes sharpness while
    # leaving quality_score null, and the mcp response still exposes that
    # number alongside quality_analyzed: false.
    for field in ("sharpness", "subject_sharpness", "subject_size"):
        row = photo_summary({"id": 1, field: 0.5}, partial)
        assert row["quality_preliminary"] is True, field
        assert row[field] == 0.5


def _photo_mid_process(db):
    """A photo as a Process run leaves it between detection and segmentation:
    a detector run, a subject, a stored quality score, but no mask yet."""
    pid = db.photos.list_page()[0]["id"]
    db.detections.save(pid, [{"box": {"x": .1, "y": .1, "w": .6, "h": .6},
                              "confidence": .9, "category": "animal"}], "megadetector-v6")
    db.model_runs.record_detector_run(pid, "megadetector-v6", 1)
    db.commit()
    db.conn.execute("UPDATE photos SET quality_score = 0.4 WHERE id = ?", (pid,))
    db.commit()
    return pid


def test_preliminary_quality_scores_are_not_reported_as_analyzed(mcp, app_and_db):
    """The observed bug: get_photo said quality_analyzed while the app's own
    readiness said segmentation was incomplete. Detail and list must both
    follow the readiness verdict and mark the numbers preliminary."""
    app, _ = app_and_db
    server, db, _ = mcp
    pid = _photo_mid_process(db)
    readiness = app.test_client().post("/api/photos/analysis-readiness", json={"photo_ids": [pid]}).json
    assert (readiness["photos"][0]["status"], readiness["photos"][0]["eligible"]) == ("incomplete", False)

    detail = call(server, "get_photo", photo_id=pid)
    assert detail["quality_score"] == 0.4
    assert detail["quality_analyzed"] is False
    assert detail["analysis_status"] == "incomplete"
    assert detail["analysis_reasons"] == ["segmentation_incomplete"]
    assert detail["quality_preliminary"] is True
    assert detail["analysis_explanation"] == readiness["photos"][0]["explanation"]

    listed = next(p for p in call(server, "search_photos")["photos"] if p["id"] == pid)
    assert listed["quality_score"] == 0.4
    assert {k: listed[k] for k in ("quality_analyzed", "analysis_status", "quality_preliminary")} == {
        k: detail[k] for k in ("quality_analyzed", "analysis_status", "quality_preliminary")
    }
    assert listed["analysis_reasons"] == ["segmentation_incomplete"]


def test_readiness_failure_never_claims_analyzed(mcp, monkeypatch):
    server, db, transport = mcp
    pid = _photo_mid_process(db)
    original = transport.request

    def failing_readiness(method, path, params=None, json_body=None):
        if path == mcp_server.READINESS_PATH:
            return 400, {"Content-Type": "application/json"}, json.dumps({"error": "Every selected photo must be visible"}).encode()
        return original(method, path, params, json_body)

    monkeypatch.setattr(transport, "request", failing_readiness)
    detail = call(server, "get_photo", photo_id=pid)
    assert detail["quality_analyzed"] is False
    assert detail["analysis_status"] == "unknown"
    assert detail["quality_preliminary"] is True
    assert detail["analysis_explanation"] == "Vireo answered 400: Every selected photo must be visible"
    listed = call(server, "search_photos")["photos"]
    assert all(p["analysis_status"] == "unknown" and p["quality_analyzed"] is False for p in listed)
    assert all("Vireo answered 400" in p["analysis_explanation"] for p in listed)


def test_readiness_is_batched_at_the_endpoints_limit():
    from services.analysis_retry import MAX_SELECTED_PHOTOS
    assert mcp_server.MAX_READINESS_IDS == MAX_SELECTED_PHOTOS
    assert mcp_server.MAX_PAGE_SIZE <= mcp_server.MAX_READINESS_IDS

    class Client:
        calls = []

        def post_json(self, path, body=None):
            self.calls.append(body["photo_ids"])
            return {"photos": [{"photo_id": pid, "status": "complete", "eligible": True,
                                "reasons": [], "explanation": "done"}
                               for pid in body["photo_ids"] if pid != 3]}

    ids = list(range(1, 2 * MAX_SELECTED_PHOTOS + 2))
    rows = mcp_server.fetch_readiness(Client(), ids + ids[:5] + ["7", None])
    assert [len(chunk) for chunk in Client.calls] == [MAX_SELECTED_PHOTOS, MAX_SELECTED_PHOTOS, 1]
    assert rows[1]["eligible"] is True and len(rows) == len(ids)
    # A photo the app left out of its answer is not silently analyzed.
    assert "error" in rows[3] and mcp_server.analysis_fields({}, rows[3])["analysis_status"] == "unknown"


def test_view_photo_thumbnail_returns_the_jpeg(mcp):
    server, db, _ = mcp
    pid = db.photos.list_page()[0]["id"]
    content = call(server, "view_photo", photo_id=pid, view="thumbnail")
    assert content[0].type == "image"
    assert content[0].mime_type == "image/jpeg"
    assert "view must be" in call_error(server, "view_photo", photo_id=pid, view="huge")


def test_view_photo_preview_renders_the_frame(mcp, app_and_db, tmp_path):
    server, db, transport = mcp
    photo = db.photos.list_page()[0]
    photo_dir = tmp_path / "photo_files"
    photo_dir.mkdir()
    with db.transaction():
        db.conn.execute("UPDATE folders SET path=? WHERE id=?", (str(photo_dir), photo["folder_id"]))
    PILImage.new("RGB", (640, 480), (10, 200, 30)).save(photo_dir / photo["filename"], "JPEG")
    content = call(server, "view_photo", photo_id=photo["id"], view="preview")
    assert content[0].type == "image" and content[0].mime_type == "image/jpeg"
    _, path, params, _ = transport.calls[-1]
    assert path == f"/photos/{photo['id']}/preview"
    assert params == {"size": mcp_server.PREVIEW_SIZE}


def test_burst_passes_the_apps_precondition_through(mcp):
    server, db, _ = mcp
    pid = db.photos.list_page()[0]["id"]
    message = call_error(server, "burst", photo_id=pid)
    assert "Vireo answered 404: No neighboring batch photos found" in message



@pytest.mark.parametrize("has_candidate", [True, False])
def test_burst_exposes_provisional_candidate_without_claiming_a_keeper(mcp, monkeypatch, has_candidate):
    from keep_safety import review_ranked_suggestions

    server, _, transport = mcp
    response = {
        "seed_photo_id": 42,
        "cards": [{"id": 42, "filename": "frame.jpg", "rank": 1, "label": "KEEP",
                   "selection_eligible": False, "analysis_status": "incomplete",
                   "analysis_reasons": ["segmentation_incomplete"], "quality": None}]
        if has_candidate else [],
        "best_photo_id": 42,
    }
    review_ranked_suggestions(response)
    paths = []

    def get_json(path, params=None):
        paths.append(path)
        return response

    monkeypatch.setattr(transport, "get_json", get_json)
    result = call(server, "burst", photo_id=42)
    assert paths == ["/api/v1/photos/42/burst-evidence"]
    assert result["ranked_candidate_id"] == (42 if has_candidate else None)
    assert result["ranking_status"] == "provisional"
    if has_candidate:
        # The frame's readiness travels with it, so a null quality is explained.
        frame = result["frames"][0]
        assert frame["selection_eligible"] is False
        assert frame["analysis_status"] == "incomplete"
        assert frame["analysis_reasons"] == ["segmentation_incomplete"]
    assert result["keep_review_message"] == response["keep_review_message"]
    assert result["best_photo_id"] is None
    assert result["best_filename"] is None
    assert result["alternate_ids"] == []
    assert result["suggested_reject_ids"] == []
    if has_candidate:
        frame = result["frames"][0]
        assert frame["label"] == "REVIEW"
        assert frame["automatic_keep_eligible"] is False
        assert frame["keep_status"] == response["cards"][0]["keep_status"]
        assert frame["keep_reasons"] == response["cards"][0]["keep_reasons"]


def test_burst_nulls_quality_fields_on_ineligible_frames(mcp, monkeypatch):
    """The best-batch cleanup leaves ``subject_tenengrad`` (exposed as
    ``sharpness``) in place even when a frame is ineligible, so the burst
    tool nulls every surfaced quality metric itself to match its guidance.
    """
    from keep_safety import review_ranked_suggestions

    server, _, transport = mcp
    response = {
        "seed_photo_id": 42,
        "best_photo_id": 42,
        "cards": [{
            "id": 42, "filename": "frame.jpg", "rank": 1, "label": "KEEP",
            "selection_eligible": False, "analysis_status": "incomplete",
            "analysis_reasons": ["segmentation_incomplete"],
            # None of these should survive on an ineligible frame.
            "quality": 0.72, "quality_pct": 72, "focus": 0.9,
            "focus_basis": "subject", "sharpness": 0.5, "exposure": 0.4,
        }],
    }
    review_ranked_suggestions(response)
    monkeypatch.setattr(transport, "get_json", lambda path, params=None: response)
    frame = call(server, "burst", photo_id=42)["frames"][0]
    assert frame["selection_eligible"] is False
    for key in ("quality", "quality_pct", "focus", "focus_basis",
                "sharpness", "exposure"):
        assert frame[key] is None, f"{key} must be null on an ineligible frame"


def test_burst_keeps_quality_fields_on_eligible_frames(mcp, monkeypatch):
    from keep_safety import review_ranked_suggestions

    server, _, transport = mcp
    response = {
        "seed_photo_id": 42,
        "best_photo_id": 42,
        "cards": [{
            "id": 42, "filename": "frame.jpg", "rank": 1, "label": "KEEP",
            "selection_eligible": True, "analysis_status": "complete",
            "analysis_reasons": [],
            "quality": 0.72, "quality_pct": 72, "focus": 0.9,
            "focus_basis": "subject", "sharpness": 0.5, "exposure": 0.4,
        }],
    }
    review_ranked_suggestions(response)
    monkeypatch.setattr(transport, "get_json", lambda path, params=None: response)
    frame = call(server, "burst", photo_id=42)["frames"][0]
    assert frame["sharpness"] == 0.5
    assert frame["quality"] == 0.72


def test_burst_reports_unscored_frames_and_why(mcp):
    from test_burst_evidence import _folder_id, _seed_burst

    server, db, _ = mcp
    pids = _seed_burst(db, _folder_id(db), 3069, [None, None, None])
    result = call(server, "burst", photo_id=pids[1])
    assert result["scored"] is False and result["unscored_reason"]
    assert result["best_photo_id"] is None
    assert result["coverage"]["with_quality"] == 0
    assert [r["code"] for r in result["review_reasons"]] == ["unanalyzed_frames"]
    assert [f["id"] for f in result["frames"]] == pids
    assert all(f["rank"] is None and f["selection_eligible"] is False for f in result["frames"])


def test_burst_carries_the_evidence_behind_the_ranking(mcp):
    from test_burst_evidence import _folder_id, _seed_burst

    server, db, _ = mcp
    pids = _seed_burst(db, _folder_id(db), 3069, [50, 49])
    result = call(server, "burst", photo_id=pids[0])
    assert result["scored"] is True and result["ranked_candidate_id"] == pids[0]
    assert result["best_photo_id"] is None
    reasons = {r["code"]: r["detail"] for r in result["review_reasons"]}
    assert set(reasons) == {"close_call", "positive_keep_not_validated"}
    assert "DSC_3069.jpg" in reasons["close_call"]
    frame = result["frames"][0]
    assert frame["detection_count"] == 1 and frame["has_mask"] is True
    assert frame["reject_reasons"] == [] and frame["crop_complete"] == 1.0
    assert not any(key.endswith("_url") for key in frame)


def test_burst_reaches_every_frame_of_a_long_burst(mcp):
    from test_burst_evidence import _folder_id, _seed_burst

    server, db, transport = mcp
    pids = _seed_burst(db, _folder_id(db), 3001, list(range(10, 140)))
    db.conn.executemany(
        "UPDATE photos SET timestamp = '2024-03-01T12:00:00' WHERE id = ?", [(pid,) for pid in pids])
    db.commit()
    listed = call(server, "list_bursts", folder_id=_folder_id(db))["bursts"][0]
    assert (listed["frames"], listed["ranked_frames"]) == (130, 120)

    assert call(server, "burst", photo_id=pids[0])["count"] == 120
    seen, offset = [], 0
    while offset is not None:
        page = call(server, "burst", photo_id=pids[0], max_photos=listed["frames"], offset=offset)
        assert page["count"] == 130 and page["coverage"]["with_quality"] == 130
        assert len(page["frames"]) <= mcp_server.DEFAULT_FRAME_PAGE
        seen += [frame["id"] for frame in page["frames"]]
        offset = page["next_offset"]
    assert sorted(seen) == sorted(pids) and len(seen) == 130
    assert transport.calls[-1][2] == {"max_photos": 130}
    assert "between 2 and 500" in call_error(
        server, "burst", photo_id=pids[0], max_photos=mcp_server.MAX_RANKED_FRAMES + 1)
    call(server, "burst", photo_id=pids[0], max_photos=mcp_server.MAX_RANKED_FRAMES)


def test_burst_default_matches_the_scorers_window():
    from best_batch import DEFAULT_BEST_BATCH_MAX_PHOTOS

    assert mcp_server.DEFAULT_RANKED_FRAMES == DEFAULT_BEST_BATCH_MAX_PHOTOS


def test_burst_reasons_match_the_scorers_codes():
    from burst_evidence import AMBIGUITY_CODES

    assert mcp_server.BURST_REASONS == AMBIGUITY_CODES


def test_list_bursts_filters_counts_and_pages(mcp):
    from test_burst_evidence import _folder_id, _seed_burst

    server, db, transport = mcp
    folder_id = _folder_id(db)
    clear = _seed_burst(db, folder_id, 3069, [10, 80, 30])
    close = _seed_burst(db, folder_id, 3100, [50, 49], ts_base="2024-03-01T13:00:")
    partial = _seed_burst(db, folder_id, 3200, [60, None], ts_base="2024-03-01T14:00:")
    db.photo_review.set_flag(clear[0], "rejected")
    db.commit()

    data = call(server, "list_bursts", folder_id=folder_id)
    assert transport.calls[-1][1] == f"/api/v1/folders/{folder_id}/bursts"
    assert data["folder"]["id"] == folder_id
    assert data["coverage"]["with_quality"] == 6
    assert data["burst_count"] == data["matching"] == 3
    assert data["next_offset"] is None
    assert data["reason_counts"] == {
        "positive_keep_not_validated": 3, "close_call": 1, "unanalyzed_frames": 1,
    }
    assert data["bursts"][0] == {
        "seed_photo_id": clear[0], "ranked_candidate_id": clear[1], "scored": True,
        "flags": {"none": 2, "flagged": 0, "rejected": 1},
        "first_filename": "DSC_3069.jpg", "last_filename": "DSC_3071.jpg",
        "frames": 3, "ranked_frames": 3, "analyzed_frames": 3,
        "reasons": ["positive_keep_not_validated"],
    }

    def seeds(**kwargs):
        return [b["seed_photo_id"] for b in call(server, "list_bursts", folder_id=folder_id, **kwargs)["bursts"]]

    assert seeds(reasons=["close_call"]) == [close[0]]
    assert seeds(reasons=["close_call", "unanalyzed_frames"]) == [close[0], partial[0]]
    assert seeds(unflagged_only=True) == [close[0], partial[0]]
    assert seeds(reasons=["all_rejected"]) == []
    first = call(server, "list_bursts", folder_id=folder_id, limit=2)
    assert first["next_offset"] == 2 and len(first["bursts"]) == 2
    rest = call(server, "list_bursts", folder_id=folder_id, limit=2, offset=2)
    assert [b["seed_photo_id"] for b in rest["bursts"]] == [partial[0]] and rest["next_offset"] is None
    # Counts describe the folder, not the page or the filter.
    assert rest["reason_counts"] == data["reason_counts"]
    assert "unknown reasons ['close']" in call_error(server, "list_bursts", folder_id=folder_id, reasons=["close"])
    assert "Vireo answered 404" in call_error(server, "list_bursts", folder_id=999_999)


def _focus_photo(app, db):
    from test_burst_evidence import _working_copy

    pid = db.photos.list_page()[0]["id"]
    _working_copy(app, db, pid)
    return pid


def _save_boxes(db, pid, *boxes):
    db.detections.save(pid, [
        {"box": dict(zip("xywh", box, strict=True)), "confidence": 0.9 - 0.1 * i, "category": "animal"}
        for i, box in enumerate(boxes)
    ], detector_model="MDV6")
    db.commit()


def _crop(server, **kwargs):
    facts, image = call_blocks(server, "focus_crop", **kwargs)
    assert image.type == "image" and image.mime_type == "image/jpeg"
    with PILImage.open(io.BytesIO(base64.b64decode(image.data))) as decoded:
        return json.loads(facts.text), decoded.size


def test_focus_crop_returns_the_pixels_and_what_they_are(mcp, app_and_db):
    server, db, transport = mcp
    pid = _focus_photo(app_and_db[0], db)
    facts, size = _crop(server, photo_id=pid, size=256)
    assert size == (256, 256)
    assert facts["basis"] == "frame" and facts["subject_clipped"] is None and facts["detection_id"] is None
    assert any("middle of the frame" in note for note in facts["notes"])

    _save_boxes(db, pid, (.1, .2, .2, .2))
    facts, _ = _crop(server, photo_id=pid, size=256)
    assert facts["basis"] == "subject" and facts["center"] == [0.2, 0.3]
    assert facts["source"] == "working_copy" and facts["original_scale"] == 1.0
    assert facts["image_size"] == [3000, 2000] and facts["mask_shaded"] is False
    # The box is 600x400 pixels, so a 256-pixel window cannot hold the animal.
    assert facts["subject_clipped"] is True
    assert any("usually the body" in note for note in facts["notes"])
    assert any("cuts off part of the animal" in note for note in facts["notes"])

    facts, _ = _crop(server, photo_id=pid, size=256, center_x=0.75, center_y=0.25)
    assert facts["basis"] == "point" and facts["center"] == [0.75, 0.25]
    _, path, params, _ = transport.calls[-1]
    assert path == f"/api/v1/photos/{pid}/focus-crop"
    assert params["center"] == "0.75,0.25" and params["basis"] is None


def test_focus_crop_inspects_the_chosen_animal(mcp, app_and_db):
    server, db, _ = mcp
    pid = _focus_photo(app_and_db[0], db)
    _save_boxes(db, pid, (.1, .2, .1, .1), (.6, .6, .1, .1))
    ids = [d["id"] for d in sorted(db.detections.get(pid, min_conf=0), key=lambda d: d["box_x"])]
    for detection_id, center in zip(ids, ([.15, .25], [.65, .65]), strict=True):
        facts, _ = _crop(server, photo_id=pid, detection_id=detection_id)
        assert facts["detection_id"] == detection_id and facts["center"] == pytest.approx(center)


def test_focus_crop_refuses_ambiguous_or_unavailable_requests(mcp, app_and_db):
    server, db, transport = mcp
    pid = _focus_photo(app_and_db[0], db)
    before = len(transport.calls)
    assert "both center_x and center_y" in call_error(server, "focus_crop", photo_id=pid, center_x=0.5)
    assert "not both" in call_error(
        server, "focus_crop", photo_id=pid, center_x=0.5, center_y=0.5, detection_id=1)
    assert len(transport.calls) == before
    assert "between 0 and 1" in call_error(server, "focus_crop", photo_id=pid, center_x=1.5, center_y=0.5)
    assert "Vireo answered 404" in call_error(server, "focus_crop", photo_id=pid, mask=True)
    assert "Vireo answered 404" in call_error(server, "focus_crop", photo_id=999_999)


@pytest.mark.parametrize(("scale", "source", "expected"), [
    ("0.7574", "working_copy", 'source="original"'),
    ("0.5000", "original", "not available"),
    ("unknown", "original", "unknown"),
    ("0.9970", "original", None),
])
def test_focus_crop_notes_downsized_pixels(scale, source, expected):
    facts = mcp_server.focus_crop_facts({
        "X-Vireo-Focus-Basis": "head", "X-Vireo-Focus-Original-Scale": scale,
        "X-Vireo-Focus-Source": source, "X-Vireo-Focus-Subject-Clipped": "0",
    })
    assert facts["original_scale"] == (None if scale == "unknown" else float(scale))
    if expected is None:
        assert facts["notes"] == []
    else:
        assert len(facts["notes"]) == 1 and expected in facts["notes"][0]


def test_full_scale_matches_the_routes_downsized_line():
    import inspect

    from focus_crop import prefer_original

    assert inspect.signature(prefer_original).parameters["downsized_below"].default == mcp_server.FULL_SCALE


def test_view_photo_source_shows_the_unedited_frame(mcp, app_and_db):
    server, db, transport = mcp
    pid = _focus_photo(app_and_db[0], db)
    content = call(server, "view_photo", photo_id=pid, view="source")
    assert content[0].type == "image"
    assert transport.calls[-1][1:3] == (
        f"/photos/{pid}/preview", {"size": mcp_server.PREVIEW_SIZE, "view": "source"})


def test_burst_tools_are_read_only(mcp):
    server, _, _ = mcp
    tools = {tool.name: tool for tool in anyio.run(server.list_tools)}
    for name in ("list_bursts", "burst", "focus_crop"):
        assert tools[name].model_dump(by_alias=True)["annotations"]["readOnlyHint"] is True


def test_a_slow_answer_is_not_reported_as_vireo_being_closed(monkeypatch):
    class Opener:
        def open(self, req, timeout):
            raise TimeoutError("timed out")

    monkeypatch.setattr(mcp_server.urllib.request, "build_opener", lambda *handlers: Opener())
    client = VireoClient(base_url="http://127.0.0.1:9", token=TOKEN, timeout=90)
    with pytest.raises(mcp_server.VireoError) as excinfo:
        client.get_json("/api/v1/folders/1/bursts")
    assert "did not answer in 90 seconds" in str(excinfo.value)
    assert NOT_RUNNING_MESSAGE not in str(excinfo.value)


# -- edits and exports --------------------------------------------------------


def _editable_photo(db, tmp_path, filename="bird1.jpg", size=(640, 480)):
    """A catalog photo whose original exists; returns ``(id, folder)``."""
    row = db.conn.execute("SELECT id, folder_id FROM photos WHERE filename = ?", (filename,)).fetchone()
    folder = tmp_path / "originals"
    folder.mkdir(exist_ok=True)
    PILImage.new("RGB", size, (90, 110, 70)).save(folder / filename, "JPEG")
    db.conn.execute("UPDATE folders SET path = ? WHERE id = ?", (str(folder), row["folder_id"]))
    db.conn.execute("UPDATE photos SET width = ?, height = ? WHERE id = ?", (*size, row["id"]))
    db.commit()
    return row["id"], folder


def test_recipe_names_match_the_apps():
    import inspect
    import re

    import edit_recipe

    source = inspect.getsource(edit_recipe._normalize_adjustments)
    read = set(re.findall(r'adjustments\.get\("([a-z_]+)"', source))
    read |= set(re.findall(r'adjustments\.get\("([a-z_]+)"', inspect.getsource(edit_recipe._normalize_white_balance)))
    known = read | set(edit_recipe._ADJUSTMENT_RANGES) | set(edit_recipe._WHITE_BALANCE_RANGES)
    assert set(mcp_server.ADJUSTMENT_KEYS) == known
    top = inspect.getsource(edit_recipe.normalize_recipe) + inspect.getsource(edit_recipe._normalize_geometry)
    assert set(mcp_server.RECIPE_KEYS) == set(re.findall(r'recipe\.get\("([a-z_]+)"', top)) | {"version"}


@pytest.mark.parametrize("recipe", [{"exposure": 1}, {"adjustments": {"exposre": 1}}])
def test_edit_tools_refuse_names_the_app_would_ignore(mcp, recipe):
    server, db, transport = mcp
    pid = db.photos.list_page()[0]["id"]
    before = len(transport.calls)
    for name, extra in (("view_edit", {}), ("save_edit", {"reason": "test"}), ("save_edit_copy", {"name": "x"}),
                        ("suggest_tone", {})):
        assert "no recipe setting named" in call_error(server, name, photo_id=pid, recipe=recipe, **extra)
    assert len(transport.calls) == before


def test_suggest_crop_frames_the_chosen_animal(mcp, tmp_path):
    server, db, _ = mcp
    pid, _ = _editable_photo(db, tmp_path)
    assert "No animal was detected" in call_error(server, "suggest_crop", photo_id=pid)
    _save_boxes(db, pid, (.4, .4, .1, .1), (.1, .1, .1, .1))
    first = call(server, "suggest_crop", photo_id=pid, margin=0, aspect="1:1")
    assert first["output_pixels"] == [64, 64] and first["subject_pixels"] == [64, 48]
    assert first["crop"]["x"] == pytest.approx(.4) and first["subject_cut"] is False
    other = next(d["id"] for d in db.detections.get(pid, min_conf=0) if d["id"] != first["detection_id"])
    second = call(server, "suggest_crop", photo_id=pid, margin=0, aspect="1:1", detection_id=other)
    assert second["detection_id"] == other and second["crop"]["x"] == pytest.approx(.1)
    assert "No such animal" in call_error(server, "suggest_crop", photo_id=pid, detection_id=1)
    assert "aspect must be" in call_error(server, "suggest_crop", photo_id=pid, aspect="wide")


def test_view_edit_renders_without_saving(mcp, tmp_path):
    server, db, _ = mcp
    pid, _ = _editable_photo(db, tmp_path)
    crop = {"crop": {"x": 0, "y": 0, "w": .5, "h": 1}}

    def rendered(**kwargs):
        facts, image = call_blocks(server, "view_edit", photo_id=pid, recipe=crop, **kwargs)
        assert json.loads(facts.text)["photo_id"] == pid
        with PILImage.open(io.BytesIO(base64.b64decode(image.data))) as decoded:
            return decoded.size

    assert rendered() == (320, 480)
    assert rendered(whole_frame=True) == (640, 480)
    assert db.get_photo_edit_recipe(pid) is None
    tone = call(server, "suggest_tone", photo_id=pid, recipe=crop)
    assert set(tone["adjustments"]) <= set(mcp_server.ADJUSTMENT_KEYS) and tone["style"] == "subject"


def test_save_edit_never_replaces_an_edit_it_did_not_make(mcp, app_and_db):
    server, db, transport = mcp
    pid = db.photos.list_page()[0]["id"]
    recipe = {"adjustments": {"exposure": 0.5}}
    assert "reason" in call_error(server, "save_edit", photo_id=pid, recipe=recipe, reason=" ")
    assert "empty" in call_error(server, "save_edit", photo_id=pid, recipe={}, reason="x")
    saved = call(server, "save_edit", photo_id=pid, recipe=recipe, reason="show wing bars", source="test-model")
    assert db.get_photo_edit_recipe(pid) == saved["recipe"]
    history = db.conn.execute("SELECT description, source, reason FROM edit_history ORDER BY id DESC").fetchone()
    assert tuple(history) == ("Saved photo edit — test-model: show wing bars", "test-model", "show wing bars")

    # Its own edit can be revised; a stale or absent claim to it cannot.
    assert "already has an edit" in call_error(server, "save_edit", photo_id=pid, recipe=recipe, reason="x")
    revised = call(server, "save_edit", photo_id=pid, recipe={"adjustments": {"exposure": 0.7}}, reason="brighter",
                   replaces=saved["recipe"])
    assert "not the one in replaces" in call_error(
        server, "save_edit", photo_id=pid, recipe=recipe, reason="x", replaces=saved["recipe"])
    assert db.get_photo_edit_recipe(pid) == revised["recipe"]
    assert app_and_db[0].test_client().post("/api/undo").status_code == 200
    assert db.get_photo_edit_recipe(pid) == saved["recipe"]


def test_save_edit_loses_to_an_edit_made_between_its_check_and_its_write(mcp, monkeypatch):
    server, db, transport = mcp
    pid = db.photos.list_page()[0]["id"]
    original = transport.put_json

    def edited_first(path, body):
        db.set_photo_edit_recipe(pid, {"rotation": 90})
        db.commit()
        return original(path, body)

    monkeypatch.setattr(transport, "put_json", edited_first)
    message = call_error(server, "save_edit", photo_id=pid, recipe={"adjustments": {"exposure": 1}}, reason="x")
    assert "409" in message and "edit changed" in message
    assert db.get_photo_edit_recipe(pid)["rotation"] == 90


def test_save_edit_copy_leaves_the_photos_edit_alone(mcp):
    server, db, _ = mcp
    pid = db.photos.list_page()[0]["id"]
    db.set_photo_edit_recipe(pid, {"rotation": 90})
    db.commit()
    copy = call(server, "save_edit_copy", photo_id=pid, name="ID view", recipe={"adjustments": {"exposure": 1}})
    edit = call(server, "get_edit", photo_id=pid)
    assert edit["recipe"]["rotation"] == 90
    assert [(v["id"], v["name"], v["kind"]) for v in edit["versions"]] == [(copy["version_id"], "ID view", "copy")]


def _preset(app, **settings):
    response = app.test_client().post("/api/export/presets", json={
        "name": "Main", "settings": {"metadata_fields": ["species", "rating"], "quality": 85, **settings}})
    assert response.status_code == 200, response.get_json()


def _finished(server, job_id):
    for _ in range(int(synchronization_timeout(20))):
        job = call(server, "get_job", job_id=job_id, wait_seconds=1)
        if job["status"] in mcp_server.TERMINAL_JOB_STATUSES:
            return job
    raise AssertionError(job)


def test_export_runs_what_the_preview_described(mcp, app_and_db, tmp_path):
    from metadata import find_exiftool

    server, db, transport = mcp
    _preset(app_and_db[0])
    pid, _ = _editable_photo(db, tmp_path)
    db.conn.execute("UPDATE photos SET latitude = 34.1, longitude = -118.2 WHERE id = ?", (pid,))
    db.commit()
    hidden = 999_999
    out = tmp_path / "out"
    out.mkdir()
    (out / "bird1.jpg").write_bytes(b"already here")

    preview = call(server, "preview_export", photo_ids=[pid, hidden, pid], preset="Main", destination=str(out))
    assert preview["metadata"]["requested_fields"] == ["species", "rating", "location"]
    assert preview["settings"]["quality"] == 85
    assert preview["file_count"] == preview["with_coordinates"] == 1 and preview["without_coordinates"] == []
    assert preview["files"] == [{"photo_id": pid, "path": str(out / "bird1_2.jpg"), "edited": False}]
    assert preview["renamed"][0]["export_name"] == "bird1_2.jpg"
    assert preview["skipped"] == [{"photo_id": hidden, "reason": "not visible in this workspace"}]
    assert sorted(p.name for p in out.iterdir()) == ["bird1.jpg"]

    started = call(server, "start_export", token=preview["token"])
    job = _finished(server, started["job_id"])
    if find_exiftool():
        assert job["status"] == "completed", job
        assert job["result"]["files"] == [str(out / "bird1_2.jpg")]
        assert job["result"]["metadata"]["categories"]["location"]["written"] == 1
    else:
        # The selected metadata cannot be written, so the file is not kept.
        assert job["status"] == "failed" and job["result"]["files"] == []
    assert (out / "bird1.jpg").read_bytes() == b"already here"
    body = next(c[3] for c in reversed(transport.calls) if c[1] == "/api/jobs/export")
    assert body["metadata_fields"] == ["species", "rating", "location"] and body["photo_ids"] == [pid, hidden]
    assert "unknown or already used" in call_error(server, "start_export", token=preview["token"])


def test_export_without_metadata_writes_the_listed_files(mcp, tmp_path):
    server, db, _ = mcp
    pid, _ = _editable_photo(db, tmp_path)
    out = tmp_path / "out"
    preview = call(server, "preview_export", photo_ids=[pid], destination=str(out), subfolder="Robin 2024-01-15",
                   include_location=False)
    assert preview["metadata"]["metadata_free"] is True
    assert preview["without_coordinates"] == [pid]
    job = _finished(server, call(server, "start_export", token=preview["token"])["job_id"])
    assert job["status"] == "completed", job
    assert job["result"]["files"] == [f["path"] for f in preview["files"]]
    assert (out / "Robin 2024-01-15" / "bird1.jpg").is_file()


@pytest.mark.parametrize("change", ["edit", "file", "location"])
def test_start_export_refuses_when_the_preview_is_stale(mcp, tmp_path, change):
    server, db, transport = mcp
    pid, _ = _editable_photo(db, tmp_path)
    out = tmp_path / "out"
    out.mkdir()
    preview = call(server, "preview_export", photo_ids=[pid], destination=str(out))
    if change == "edit":
        db.set_photo_edit_recipe(pid, {"rotation": 90})
    elif change == "location":
        db.conn.execute("UPDATE photos SET latitude = 1, longitude = 2 WHERE id = ?", (pid,))
    else:
        (out / "bird1.jpg").write_bytes(b"in the way")
    db.commit()
    assert "differ from the preview" in call_error(server, "start_export", token=preview["token"])
    assert not any(c[1] == "/api/jobs/export" for c in transport.calls)


def test_preview_export_refuses_what_it_cannot_describe(mcp, app_and_db, tmp_path, monkeypatch):
    server, db, transport = mcp
    _preset(app_and_db[0])
    pid, folder = _editable_photo(db, tmp_path)
    assert "at least one" in call_error(server, "preview_export", photo_ids=[])
    assert "['Main']" in call_error(server, "preview_export", photo_ids=[pid], preset="Other")
    assert "one photo" in call_error(server, "preview_export", photo_ids=[pid, pid + 1], version_id="x")
    assert "absolute" in call_error(server, "preview_export", photo_ids=[pid], destination="exports")
    (folder / "bird1.jpg").unlink()
    assert "no readable original" in call_error(
        server, "preview_export", photo_ids=[pid], destination=str(tmp_path / "out"))
    monkeypatch.setattr(transport, "post_json", lambda path, body: {"renames": [], "metadata": {}})
    assert "older than this connector" in call_error(
        server, "preview_export", photo_ids=[pid], destination=str(tmp_path / "out"))


def test_export_of_a_virtual_copy_names_that_version(mcp, tmp_path):
    server, db, transport = mcp
    pid, _ = _editable_photo(db, tmp_path)
    copy = call(server, "save_edit_copy", photo_id=pid, name="ID view", recipe={"adjustments": {"exposure": 1}})
    preview = call(server, "preview_export", photo_ids=[pid], destination=str(tmp_path / "out"),
                   version_id=copy["version_id"])
    assert preview["files"][0]["edited"] is True
    body = next(c[3] for c in reversed(transport.calls) if c[1] == "/api/jobs/export/preflight")
    assert body["version_id"] == copy["version_id"] and body["version_revision"]


def test_list_export_presets(mcp, app_and_db):
    _preset(app_and_db[0])
    presets = call(mcp[0], "list_export_presets")["presets"]
    assert [p["name"] for p in presets] == ["Main"]


def test_job_summary_drops_worker_detail_and_cuts_long_lists():
    job = {"id": "j", "type": "export", "status": "failed", "_private": 1, "config": {"photo_ids": [1]},
           "progress": {"current": 2, "total": 3, "current_file": "a.jpg", "noise": 1},
           "errors": [f"e{i}" for i in range(60)], "result": {"exported": 1, "files": list(range(60))}}
    out = mcp_server.job_summary(job)
    assert out["progress"] == {"current": 2, "total": 3, "current_file": "a.jpg"}
    assert len(out["errors"]) == mcp_server.MAX_JOB_LIST and out["error_count"] == 60
    assert len(out["result"]["files"]) == mcp_server.MAX_JOB_LIST and out["result"]["files_total"] == 60
    assert out["result"]["exported"] == 1
    assert "_private" not in out and "config" not in out


def test_get_job_reports_an_unknown_job(mcp):
    assert "Vireo answered 404" in call_error(mcp[0], "get_job", job_id="export/../nope")


def test_edit_and_export_tools_advertise_side_effects(mcp):
    tools = {tool.name: tool.model_dump(by_alias=True)["annotations"] for tool in anyio.run(mcp[0].list_tools)}
    for name in ("get_edit", "suggest_crop", "suggest_tone", "view_edit", "list_export_presets",
                 "preview_export", "get_job"):
        assert tools[name]["readOnlyHint"] is True, name
    for name in ("save_edit", "save_edit_copy", "start_export"):
        assert tools[name]["readOnlyHint"] is False and tools[name]["destructiveHint"] is False, name


def _eagles(db):
    """Two confirmed eagle photos a year apart; returns their ids."""
    from services import keyword_tagging, photo_ingest

    folder_id = db.folders.id_by_path("/photos/2024")
    ids = []
    for name, stamp in (("eagle24.jpg", "2024-03-01T08:00:00"), ("eagle25.jpg", "2025-03-01T08:00:00")):
        pid = photo_ingest.add_photo(
            db, folder_id=folder_id, filename=name, extension=".jpg",
            file_size=10, file_mtime=1.0, timestamp=stamp,
        )
        keyword_tagging.tag_photo(db, pid, db.keywords.add("Bald Eagle", is_species=True))
        ids.append(pid)
    db.commit()
    return ids


def test_best_tools_list_choose_and_scope_by_date(mcp):
    server, db, transport = mcp
    old, new = _eagles(db)

    listed = call(server, "best_of_each_species", species="Bald Eagle", per_species=5)

    eagle = listed["species"][0]
    assert {p["id"] for p in eagle["photos"]} == {old, new}
    assert eagle["decision"]["kind"] in ("score", "unanalyzed")

    chosen = call(
        server, "choose_best_photo", species="Bald Eagle", photo_id=old,
        over=[new], reason="sharper eye", source="claude",
    )

    assert chosen["over"] == [new]
    assert chosen["superseded"] == []
    assert transport.calls[-1][3] == {
        "species": "Bald Eagle", "photo_id": old, "reason": "sharper eye",
        "over": [new], "source": "claude",
    }
    listed = call(server, "best_of_each_species", species="Bald Eagle")
    decision = listed["species"][0]["decision"]
    assert (decision["kind"], decision["photo_id"]) == ("highlight", old)
    assert (decision["source"], decision["reason"]) == ("claude", "sharper eye")

    year = call(
        server, "best_of_each_species", species="Bald Eagle",
        date_from="2025-01-01", date_to="2025-12-31",
    )
    assert [p["id"] for p in year["species"][0]["photos"]] == [new]
    assert year["species"][0]["decision"]["kind"] != "highlight"


def test_choose_best_photo_requires_a_reason_and_reports_the_apps_refusal(mcp):
    server, db, _ = mcp
    old, _new = _eagles(db)

    assert "reason" in call_error(
        server, "choose_best_photo", species="Bald Eagle", photo_id=old, reason=" ",
    )
    assert "confirm the species" in call_error(
        server, "choose_best_photo", species="Osprey", photo_id=old, reason="best",
    )


def test_best_tools_advertise_side_effects_to_clients(mcp):
    server, _, _ = mcp
    tools = {tool.name: tool for tool in anyio.run(server.list_tools)}
    hints = tools["best_of_each_species"].model_dump(by_alias=True)["annotations"]
    assert hints["readOnlyHint"] is True
    hints = tools["choose_best_photo"].model_dump(by_alias=True)["annotations"]
    assert hints["readOnlyHint"] is False
    assert hints["destructiveHint"] is False


def test_set_flag_writes_only_what_the_route_persists(mcp):
    # Omitted provenance preserves the ordinary manual-write payload.
    server, db, transport = mcp
    pid = db.photos.list_page()[0]["id"]
    result = call(server, "set_flag", photo_ids=[pid], flag="rejected")
    assert result == {"updated": 1, "flag": "rejected"}
    method, path, _, body = transport.calls[-1]
    assert (method, path) == ("POST", "/api/batch/flag")
    assert body == {"photo_ids": [pid], "flag": "rejected"}
    assert db.photos.get(pid)["flag"] == "rejected"
    assert "Vireo answered 400" in call_error(server, "set_flag", photo_ids=[pid], flag="maybe")


def test_set_flag_tool_advertises_supported_provenance(mcp):
    server, _, _ = mcp
    tools = {tool.name: tool for tool in anyio.run(server.list_tools)}
    assert set(tools["set_flag"].input_schema["properties"]) == {"photo_ids", "flag", "source", "reason"}


def test_set_rating_writes_the_rating(mcp):
    server, db, _ = mcp
    pid = db.photos.list_page()[1]["id"]
    assert call(server, "set_rating", photo_ids=[pid], rating=4) == {"updated": 1, "rating": 4}
    assert db.photos.get(pid)["rating"] == 4
    assert "Vireo answered 400" in call_error(server, "set_rating", photo_ids=[pid], rating=9)


def test_add_and_remove_keyword(mcp):
    server, db, _ = mcp
    pid = db.photos.list_page()[2]["id"]
    added = call(server, "add_keyword", photo_ids=[pid], name="Great horned owl")
    keyword_id = added["keyword_id"]
    assert "Great horned owl" in [k["name"] for k in db.photo_keywords.get_for_photo(pid)]
    call(server, "add_keyword", photo_ids=[pid], keyword_id=keyword_id)
    call(server, "remove_keyword", photo_ids=[pid], keyword_id=keyword_id)
    assert "Great horned owl" not in [k["name"] for k in db.photo_keywords.get_for_photo(pid)]
    assert "keyword name or keyword_id" in call_error(server, "add_keyword", photo_ids=[pid])


def test_search_keywords_matches_substrings_and_builds_paths(mcp):
    server, db, _ = mcp
    parent = db.keywords.add("Owls")
    child = db.keywords.add("Barn owl", parent_id=parent)
    # The tree holds keywords used by photos in the workspace plus their
    # ancestors, so tagging the child surfaces the parent too.
    keyword_tagging.tag_photo(db, db.photos.list_page()[0]["id"], child)
    db.commit()
    found = call(server, "search_keywords", query="owl")
    assert found["total_matches"] == 2
    assert {k["path"] for k in found["keywords"]} == {"Owls", "Owls / Barn owl"}
    top_level = call(server, "search_keywords")
    assert all(k["parent_id"] is None for k in top_level["keywords"])
    assert {"Cardinal", "Sparrow", "Owls"} <= {k["name"] for k in top_level["keywords"]}


def test_keyword_paths_survive_a_parent_cycle():
    rows = [{"id": 1, "name": "a", "parent_id": 2}, {"id": 2, "name": "b", "parent_id": 1}]
    assert [k["path"] for k in keyword_paths(rows)] == ["b / a", "a / b"]


def test_list_folders_filters_on_path(mcp):
    server, _, _ = mcp
    everything = call(server, "list_folders")
    assert everything["total"] == 2
    january = call(server, "list_folders", query="JANUARY")
    assert [f["name"] for f in january["folders"]] == ["January"]
    assert set(january["folders"][0]) <= set(mcp_server._FOLDER_KEYS)


def test_list_collections_parses_rules(mcp):
    server, _, _ = mcp
    listing = call(server, "list_collections")
    for collection in listing["collections"]:
        assert not isinstance(collection["rules"], str)
        assert "visual_json" not in collection


def test_not_running_is_one_plain_message(tmp_path, monkeypatch):
    monkeypatch.delenv("VIREO_URL", raising=False)
    monkeypatch.delenv("VIREO_TOKEN", raising=False)
    monkeypatch.setattr(mcp_server, "read_runtime_json", lambda: None)
    server = build_server(VireoClient())
    assert NOT_RUNNING_MESSAGE in call_error(server, "vireo_status")

    # A runtime.json pointing at a dead port reads the same.
    import socket
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    monkeypatch.setattr(mcp_server, "read_runtime_json", lambda: {"port": port, "token": "x"})
    assert NOT_RUNNING_MESSAGE in call_error(server, "list_workspaces")


def test_wrong_token_is_explained(app_and_db):
    app, _ = app_and_db
    server = build_server(FlaskTransport(app, token="wrong"))
    assert BAD_TOKEN_MESSAGE in call_error(server, "vireo_status")


def test_client_resolves_runtime_json_per_request(monkeypatch):
    monkeypatch.delenv("VIREO_URL", raising=False)
    monkeypatch.delenv("VIREO_TOKEN", raising=False)
    monkeypatch.setattr(mcp_server, "read_runtime_json", lambda: {"port": 4321, "token": "abc"})
    assert VireoClient()._resolve() == ("http://127.0.0.1:4321", "abc")
    monkeypatch.setenv("VIREO_URL", "http://127.0.0.1:9/")
    monkeypatch.setenv("VIREO_TOKEN", "env")
    assert VireoClient()._resolve() == ("http://127.0.0.1:9", "env")


def test_vireo_url_without_token_never_borrows_the_local_token(monkeypatch):
    # VIREO_URL alone used to fall back to the runtime.json token, which
    # would send the live catalog credential to whatever URL a typo named.
    monkeypatch.setenv("VIREO_URL", "http://evil.example.com/")
    monkeypatch.delenv("VIREO_TOKEN", raising=False)
    monkeypatch.setattr(
        mcp_server, "read_runtime_json",
        lambda: {"port": 4321, "token": "live-secret"},
    )
    with pytest.raises(mcp_server.VireoError) as excinfo:
        VireoClient()._resolve()
    assert mcp_server.UNPAIRED_URL_MESSAGE in str(excinfo.value)


@pytest.mark.parametrize("bad_url", [
    "http://evil.example.com/",
    "http://192.0.2.1/",
    "http://localhost:9000",
    "http://user@127.0.0.1:9000/",
    "http://127.0.0.1:9000/path",
    "https://127.0.0.1:9/",
    "file:///etc/hosts",
])
def test_vireo_url_must_be_loopback_http(monkeypatch, bad_url):
    monkeypatch.setenv("VIREO_URL", bad_url)
    monkeypatch.setenv("VIREO_TOKEN", "paired")
    with pytest.raises(mcp_server.VireoError) as excinfo:
        VireoClient()._resolve()
    assert mcp_server.NON_LOOPBACK_URL_MESSAGE in str(excinfo.value)


@pytest.mark.parametrize("good_url,expected", [
    ("http://127.0.0.1:9/", "http://127.0.0.1:9"),
    ("http://[::1]:9000/", "http://[::1]:9000"),
])
def test_vireo_url_accepts_each_loopback_form(monkeypatch, good_url, expected):
    monkeypatch.setenv("VIREO_URL", good_url)
    monkeypatch.setenv("VIREO_TOKEN", "paired")
    assert VireoClient()._resolve() == (expected, "paired")


@pytest.mark.parametrize("platform", ["darwin", "linux", "win32"])
def test_server_command_for_checkout_and_frozen_build(monkeypatch, platform):
    monkeypatch.setattr(sys, "platform", platform)
    assert server_command() == [sys.executable, str(APP), "--mcp"]
    monkeypatch.setattr(sys, "frozen", True, raising=False)
    monkeypatch.setattr(sys, "executable", "/Applications/Vireo.app/Contents/MacOS/vireo-server")
    assert server_command() == ["/Applications/Vireo.app/Contents/MacOS/vireo-server", "--mcp"]
    info = connect_info()
    prefix = "& 'claude' 'mcp' 'add' 'vireo' '--' " if platform == "win32" else "claude mcp add vireo -- "
    assert info["claude_code"].startswith(prefix)
    assert info["claude_desktop"] == {"mcpServers": {"vireo": {
        "command": "/Applications/Vireo.app/Contents/MacOS/vireo-server", "args": ["--mcp"],
    }}}


def test_mcp_config_route_serves_connect_info(app_and_db):
    app, _ = app_and_db
    response = app.test_client().get("/api/system/mcp-config", headers={"X-Vireo-Token": TOKEN})
    assert response.status_code == 200
    assert response.get_json() == connect_info()
    data = response.get_json()
    assert {"command", "claude_code", "claude_desktop"} <= data.keys()
    assert data["claude_desktop"]["mcpServers"]["vireo"] == {
        "command": data["command"][0], "args": data["command"][1:],
    }
    assert data["codex"]["command"]
    assert tomllib.loads(data["codex"]["config_toml"])["mcp_servers"]["vireo"] == {
        "command": data["command"][0], "args": data["command"][1:],
    }


@pytest.mark.parametrize("frozen", [True, False])
@pytest.mark.parametrize("platform", ["darwin", "linux", "win32"])
def test_generated_snippets_quote_paths_and_preserve_argv(monkeypatch, frozen, platform):
    executable = (
        "C:\\My Photos & Birds\\O'Brien $`é\\vireo-server.exe" if platform == "win32"
        else '/My Photos & Birds/O\'Brien $`é/"vireo-server"'
    )
    monkeypatch.setattr(sys, "executable", executable)
    monkeypatch.setattr(sys, "platform", platform)
    monkeypatch.setattr(sys, "frozen", frozen, raising=False)
    monkeypatch.setattr(mcp_server, "__file__", "/Source Photos/O'Brien $`é/vireo/mcp_server.py")
    command = server_command()
    info = connect_info()
    assert command[0] == executable
    assert command[-1] == "--mcp"
    assert len(command) == (2 if frozen else 3)
    assert tomllib.loads(info["codex"]["config_toml"]) == {"mcp_servers": {"vireo": {
        "command": command[0], "args": command[1:],
    }}}
    assert info["claude_desktop"] == {"mcpServers": {"vireo": {
        "command": command[0], "args": command[1:],
    }}}
    for client, snippet in (("codex", info["codex"]["command"]), ("claude", info["claude_code"])):
        expected = [client, "mcp", "add", "vireo", "--", *command]
        if platform == "win32":
            # Single-quoted PowerShell arguments escape apostrophes by doubling them.
            import re
            assert snippet.startswith("& ")
            assert [arg.replace("''", "'") for arg in re.findall(r"'((?:[^']|'')*)'", snippet)] == expected
            assert info["shell"] == "PowerShell"
        else:
            assert shlex.split(snippet) == expected


@pytest.mark.skipif(sys.platform == "win32",
                    reason="real-process stdio session is covered on Linux/macOS")
def test_stdio_session_against_a_served_app(app_and_db, tmp_path):
    """Spawn ``python vireo/app.py --mcp`` the way Claude Code does and drive
    one session: initialize, list the tools, read status, write a flag."""
    from werkzeug.serving import make_server

    app, db = app_and_db
    pid = db.photos.list_page()[0]["id"]
    http = make_server("127.0.0.1", 0, app, threaded=True)
    thread = threading.Thread(target=http.serve_forever, daemon=True)
    thread.start()
    home = tmp_path / "home"
    home.mkdir()
    env = {**os.environ, "HOME": str(home), "USERPROFILE": str(home),
           "VIREO_URL": f"http://127.0.0.1:{http.server_port}", "VIREO_TOKEN": TOKEN,
           "VIREO_DISABLE_STARTUP_BACKFILL_TIMERS": "1"}
    params = StdioServerParameters(command=sys.executable, args=[str(APP), "--mcp"], env=env)

    async def session_run():
        async with stdio_client(params) as (read, write):
            async with ClientSession(
                read, write,
                client_info=mcp_types.Implementation(name="vireo-test-client", version="1"),
            ) as session:
                init = await session.initialize()
                tools = await session.list_tools()
                status = await session.call_tool("vireo_status", {})
                flagged = await session.call_tool(
                    "set_flag", {"photo_ids": [pid], "flag": "rejected"},
                )
                return init, tools, status, flagged

    try:
        init, tools, status, flagged = anyio.run(session_run)
    finally:
        http.shutdown()
        thread.join(timeout=10)

    assert init.server_info.name == "vireo"
    assert "vireo_status" in {tool.name for tool in tools.tools}
    assert not status.is_error
    assert json.loads(status.content[0].text)["photo_count"] == 3
    assert not flagged.is_error
    written = json.loads(flagged.content[0].text)
    assert written == {"updated": 1, "flag": "rejected"}
    assert db.photos.get(pid)["flag"] == "rejected"


def test_mcp_flag_does_not_start_an_instance(tmp_path):
    """``--mcp`` with no Vireo to talk to must still start, answer the
    handshake, and exit when the client closes the pipe, leaving no
    runtime.json behind."""
    home = tmp_path / "home"
    home.mkdir()
    env = {**os.environ, "HOME": str(home), "USERPROFILE": str(home),
           "VIREO_URL": "http://127.0.0.1:9", "VIREO_TOKEN": "x"}
    params = StdioServerParameters(command=sys.executable, args=[str(APP), "--mcp"], env=env)

    async def session_run():
        async with stdio_client(params) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                return await session.call_tool("vireo_status", {})

    result = anyio.run(session_run)
    assert result.is_error
    assert NOT_RUNNING_MESSAGE in result.content[0].text
    assert not (home / ".vireo" / "runtime.json").exists()

def test_report_and_resolve_assignment_conflict_from_photo_details(mcp):
    server, db, _ = mcp
    kid = db.keywords.add('Mallard', is_species=True)
    keyword_tagging.tag_photo(db, 1, kid)
    db.commit()
    def decide(action):
        photo = call(server, "get_photo", photo_id=1)
        assignment = next(a for a in photo['species_assignments'] if a['keyword_id'] == kid)
        return call(server, "review_species_assignment", photo_id=1, keyword_id=kid,
                    observed_workspace_id=photo['review_workspace_id'], review_version=assignment['review_version'],
                    action=action, reviewer='Reviewer', reason='Reviewed frame', evidence_source='Native image')

    assert decide('report')['entry_id']
    assert call(server, "list_assignment_conflicts")['assignments'][0]['keyword_id'] == kid
    assert decide('remove')['entry_id']
    assert not call(server, "list_assignment_conflicts")['assignments']
    record, = call(server, "get_photo", photo_id=1)['assignment_reviews']
    assert record['resolution']['action'] == 'remove'
