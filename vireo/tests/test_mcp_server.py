"""The MCP server: Vireo's catalog as tools for Claude Code and friends.

Most tests bind the tools to a Flask test client (no sockets) and check what
they send to the app and what they hand back. The last one is the real
thing: the test app served on a loopback port, ``python vireo/app.py --mcp``
spawned as an MCP client would spawn it, and a stdio session driven with the
SDK's client.
"""

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
        return response.status_code, response.content_type, response.get_data()


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
        "find_photos_by_text", "get_photo", "view_photo", "burst",
        "set_flag", "set_rating", "add_keyword", "remove_keyword",
        "get_species_evidence", "preview_species_assignment", "apply_species_assignment",
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
    keyword = db.add_keyword("Mallard", is_species=True)
    db.tag_photo(1, keyword)
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
    keyword = db.add_keyword("Robin", is_species=True)
    db.tag_photo(1, keyword)
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
    db.tag_photo(3, keyword)
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
    db.tag_photo(3, keyword)
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
            return 400, "application/json", json.dumps({"error": "Every selected photo must be visible"}).encode()
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

    def get_json(path):
        paths.append(path)
        return response

    monkeypatch.setattr(transport, "get_json", get_json)
    result = call(server, "burst", photo_id=42)
    assert paths == ["/api/photos/42/best-batch"]
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
    monkeypatch.setattr(transport, "get_json", lambda path: response)
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
    monkeypatch.setattr(transport, "get_json", lambda path: response)
    frame = call(server, "burst", photo_id=42)["frames"][0]
    assert frame["sharpness"] == 0.5
    assert frame["quality"] == 0.72


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
    parent = db.add_keyword("Owls")
    child = db.add_keyword("Barn owl", parent_id=parent)
    # The tree holds keywords used by photos in the workspace plus their
    # ancestors, so tagging the child surfaces the parent too.
    db.tag_photo(db.photos.list_page()[0]["id"], child)
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
