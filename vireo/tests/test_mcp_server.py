"""The MCP server: Vireo's catalog as tools for Claude Code and friends.

Most tests bind the tools to a Flask test client (no sockets) and check what
they send to the app and what they hand back. The last one is the real
thing: the test app served on a loopback port, ``python vireo/app.py --mcp``
spawned as an MCP client would spawn it, and a stdio session driven with the
SDK's client.
"""

import json
import os
import sys
import threading
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
    }
    for tool in anyio.run(server.list_tools):
        assert tool.description, f"{tool.name} has no description"
        # The context parameter is injected, never asked of the client.
        assert "ctx" not in tool.input_schema["properties"], tool.name


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
    assert {"id", "filename", "folder_id", "rating", "flag", "quality_analyzed"} <= set(photo)
    assert photo["quality_analyzed"] is False
    for noisy in ("exif_data", "thumb_path", "render_key", "life_list", "xmp_mtime"):
        assert noisy not in photo
    _, path, params, _ = transport.calls[-1]
    assert path == "/api/photos"
    assert params["per_page"] == 2


def test_search_photos_passes_filters_through(mcp):
    server, _, transport = mcp
    data = call(server, "search_photos", rating_min=4, flag="none")
    assert [p["filename"] for p in data["photos"]] == ["bird3.jpg"]
    _, _, params, _ = transport.calls[-1]
    assert params["rating_min"] == 4 and params["flag"] == "none"
    assert "Vireo answered 400" in call_error(server, "search_photos", flag="maybe")


def test_search_photos_caps_the_page_size(mcp):
    server, _, transport = mcp
    call(server, "search_photos", per_page=10_000)
    assert transport.calls[-1][2]["per_page"] == mcp_server.MAX_PAGE_SIZE


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


def test_photo_summary_reports_quality_analysis_state():
    assert photo_summary({"id": 1, "quality_score": None})["quality_analyzed"] is False
    assert photo_summary({"id": 1, "quality_score": 0.4})["quality_analyzed"] is True


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
        "cards": [{"id": 42, "filename": "frame.jpg", "rank": 1, "label": "KEEP"}]
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


def test_server_command_for_checkout_and_frozen_build(monkeypatch):
    assert server_command() == [sys.executable, str(APP), "--mcp"]
    monkeypatch.setattr(sys, "frozen", True, raising=False)
    monkeypatch.setattr(sys, "executable", "/Applications/Vireo.app/Contents/MacOS/vireo-server")
    assert server_command() == ["/Applications/Vireo.app/Contents/MacOS/vireo-server", "--mcp"]
    info = connect_info()
    assert info["claude_code"].startswith("claude mcp add vireo -- ")
    assert info["claude_desktop"] == {"mcpServers": {"vireo": {
        "command": "/Applications/Vireo.app/Contents/MacOS/vireo-server", "args": ["--mcp"],
    }}}


def test_mcp_config_route_serves_connect_info(app_and_db):
    app, _ = app_and_db
    response = app.test_client().get("/api/system/mcp-config", headers={"X-Vireo-Token": TOKEN})
    assert response.status_code == 200
    assert response.get_json() == connect_info()


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
