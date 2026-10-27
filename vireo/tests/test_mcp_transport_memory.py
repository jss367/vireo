"""Selected MCP definitions with inert HTTP/SDK/image objects only.

Never import the product module, inspect process settings, read discovery or
credentials, open a socket, start a server, or load an actual image.
"""
import ast
import contextlib
import ipaddress
import json
import posixpath
import shlex
import types
import urllib.error
import urllib.parse
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def forbidden(*a, **kw):
    raise AssertionError("Forbidden live system operation")


class Registry:
    def __init__(self, *a, **kw):
        self.tools = {}
    def tool(self, **kwargs):
        def register(fn):
            self.tools[fn.__name__] = fn
            return fn
        return register
    run = forbidden


class FakeImage:
    def __init__(self, **kw):
        self.__dict__.update(kw)


class Request:
    def __init__(self, url, **kw):
        self.url = url
        self.__dict__.update(kw)


class Proxy:
    def __init__(self, proxies):
        self.proxies = proxies


def namespace():
    tree = ast.parse((ROOT / "mcp_server.py").read_text(encoding="utf-8"))
    nodes = [n for n in tree.body if isinstance(n, (ast.Assign, ast.FunctionDef, ast.ClassDef))]
    ns = {"__name__": "inert_mcp", "__file__": "/invented/mcp_server.py",
          "__builtins__": {**vars(__import__("builtins")), "__import__": forbidden, "open": forbidden},
          "contextlib": contextlib, "ipaddress": ipaddress, "json": json, "shlex": shlex,
          "os": types.SimpleNamespace(environ={}, path=types.SimpleNamespace(
              dirname=posixpath.dirname, join=posixpath.join, abspath=lambda value: value)),
          "sys": types.SimpleNamespace(executable="/invented/python", platform="linux"),
          "subprocess": types.SimpleNamespace(list2cmdline=shlex.join, Popen=forbidden, run=forbidden),
          "read_runtime_json": forbidden, "ToolError": RuntimeError, "MCPServer": Registry, "Image": FakeImage,
          "mcp_types": types.SimpleNamespace(ToolAnnotations=types.SimpleNamespace),
          "urllib": types.SimpleNamespace(parse=urllib.parse, error=urllib.error,
              request=types.SimpleNamespace(HTTPRedirectHandler=object, Request=Request,
                  ProxyHandler=Proxy, build_opener=forbidden))}
    exec(compile(ast.Module(body=nodes, type_ignores=[]), "<inert MCP>", "exec",
                 flags=__import__("__future__").annotations.compiler_flag), ns)
    return ns


def test_only_exact_numeric_loopback_origins_are_accepted():
    ns = namespace()
    good = {"http://127.0.0.1:9000/": "http://127.0.0.1:9000", "http://[::1]:9/": "http://[::1]:9"}
    for value, expected in good.items():
        assert ns["_loopback_base_url"](value) == expected
    bad = ("http://example.invalid", "http://localhost:9", "https://127.0.0.1:9", "file:///invented",
           "http://127.0.0.1:0", "http://127.0.0.1:65536", "http://127.0.0.1:",
           "http://user@127.0.0.1:9", "http://user:pass@127.0.0.1:9", "http://127.0.0.1:9/path",
           "http://127.0.0.1:9?", "http://127.0.0.1:9#", "http://127.0.0.1:9/?a=b",
           "http://127.1", "http://2130706433", "http://0x7f000001", "http://127.0.0.1.evil.invalid",
           "http://127.0.0.1:9\n", "http://127.0.0.1:9\t", "http://127.0.0.1\\@evil.invalid",
           "http://[::1%zone]:9", "http://[::1", "http://127.0.0.1:bad")
    for value in bad:
        try:
            ns["_loopback_base_url"](value)
        except ns["VireoError"]:
            pass
        else:
            raise AssertionError(value)


def test_overrides_are_paired_and_never_borrow_discovery():
    ns = namespace()
    cls = ns["VireoClient"]
    for settings in ({"VIREO_URL": "http://127.0.0.1:9"}, {"VIREO_TOKEN": "invented-marker"},
                     {"VIREO_URL": "", "VIREO_TOKEN": "invented-marker"}):
        ns["os"].environ = settings  # Inert object; never the process environment.
        try:
            cls()._resolve()
        except ns["VireoError"] as exc:
            assert ns["UNPAIRED_URL_MESSAGE"] in str(exc)
        else:
            raise AssertionError("unpaired override accepted")
    ns["os"].environ = {}
    assert cls(base_url="http://127.0.0.1:9/", token="invented-marker")._resolve() == (
        "http://127.0.0.1:9", "invented-marker")
    for kw in ({"base_url": "http://127.0.0.1:9"}, {"token": "invented-marker"}):
        try:
            cls(**kw)._resolve()
        except ns["VireoError"]:
            pass
        else:
            raise AssertionError("constructor borrowed discovery")


def test_default_discovery_is_only_an_invented_callback():
    ns = namespace()
    calls = []
    ns["read_runtime_json"] = lambda: calls.append("fake discovery") or {"port": 9, "token": "invented-marker"}
    assert ns["VireoClient"]()._resolve() == ("http://127.0.0.1:9", "invented-marker")
    assert calls == ["fake discovery"]
    for port in (True, 0, 65536, "9"):
        ns["read_runtime_json"] = lambda port=port: {"port": port, "token": "invented-marker"}
        try:
            ns["VireoClient"]()._resolve()
        except ns["VireoError"]:
            pass
        else:
            raise AssertionError("invalid discovery port accepted")


def test_http_uses_no_proxy_or_redirect_and_only_the_paired_marker():
    ns = namespace()
    captured = []
    class Response:
        status = 200
        headers = {"Content-Type": "application/json"}
        def __enter__(self):
            return self
        def __exit__(self, *a):
            return False
        def read(self):
            return b'{"ok":true}'
    def build(*handlers):
        assert isinstance(handlers[0], Proxy) and handlers[0].proxies == {}
        assert isinstance(handlers[1], ns["_NoRedirect"])
        assert handlers[1].redirect_request(None, None, 302, "redirect", {}, "http://example.invalid") is None
        def opened(req, **kw):
            captured.append(req)
            return Response()
        return types.SimpleNamespace(open=opened)
    ns["urllib"].request.build_opener = build
    client = ns["VireoClient"](base_url="http://127.0.0.1:9", token="invented-marker")
    assert client.post_json("/api/batch/flag", {"flag": "none", "photo_ids": [7]}) == {"ok": True}
    req = captured[0]
    assert req.url == "http://127.0.0.1:9/api/batch/flag" and req.method == "POST"
    assert req.headers["X-Vireo-Token"] == "invented-marker"
    assert json.loads(req.data) == {"flag": "none", "photo_ids": [7]}


class Catalog:
    def __init__(self):
        self.calls = []
    def get_json(self, path, params=None):
        self.calls.append(("GET", path, params))
        if path == "/api/v1/version":
            return {"version": "invented", "token": "must-not-surface"}
        if path == "/api/workspaces/active":
            return {"id": 7, "name": "invented"}
        if path == "/api/v1/photos":
            return {"total": 1}
        return {"id": 7, "metadata": {}}
    def post_json(self, path, body):
        self.calls.append(("POST", path, body))
        return {"updated": len(body.get("photo_ids", []))}
    def get_bytes(self, path, params=None):
        self.calls.append(("IMAGE", path, params))
        return "image/png", b"invented-image-bytes"


def test_status_never_reads_a_default_instance_or_exposes_its_path():
    ns = namespace()
    registry = ns["build_server"](Catalog())  # Inert registrar, not an SDK server.
    result = registry.tools["vireo_status"]()
    assert result == {"running": True, "version": "invented",
                      "active_workspace": {"id": 7, "name": "invented"}, "photo_count": 1}
    assert "db_path" not in result and "token" not in result


def test_images_and_writes_use_fixed_routes_without_runtime_or_sdk():
    ns = namespace()
    client = Catalog()
    registry = ns["build_server"](client)
    image = registry.tools["view_photo"](7)
    assert image.data == b"invented-image-bytes" and image.format == "png"
    assert client.calls[-1] == ("IMAGE", "/photos/7/preview", {"size": 1920})
    before = len(client.calls)
    try:
        registry.tools["view_photo"](7, "original-file")
    except ns["VireoError"]:
        pass
    else:
        raise AssertionError("unsupported image view accepted")
    assert len(client.calls) == before
    assert registry.tools["set_flag"]([7], "rejected") == {"updated": 1, "flag": "rejected"}
    assert client.calls[-1] == ("POST", "/api/batch/flag", {"photo_ids": [7], "flag": "rejected"})
    assert "source" not in registry.tools["set_flag"]([7], "none")
    assert len([c for c in client.calls if c[0] == "POST"]) == 2


def test_keyword_write_returns_the_actual_identity_without_name_rediscovery():
    # Compile only the existing route body, with inert request/DB collaborators.
    tree = ast.parse((ROOT / "web/batch.py").read_text(encoding="utf-8"))
    route = next(n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)
                 and n.name == "api_batch_keyword")
    route.decorator_list = []
    import re
    import unicodedata
    normalization = ast.parse((ROOT / "keyword_normalization.py").read_text(encoding="utf-8"))
    norm = {"re": re, "unicodedata": unicodedata}
    exec(compile(ast.Module(body=[n for n in normalization.body if isinstance(
        n, (ast.Assign, ast.FunctionDef))], type_ignores=[]), "<inert normalization>", "exec"), norm)
    rows = [dict(id=21, name="Say's phoebe", parent_id=8, type="location"),
            dict(id=35, name="Say's phoebe", parent_id=None, type="taxonomy")]
    body, tagged, queued = {}, [], []
    def add(name, **kw):
        assert norm["normalize_keyword_display"](name) == rows[1]["name"]
        return rows[1]["id"]
    db = types.SimpleNamespace(
        add_keyword=add,
        keywords=types.SimpleNamespace(
            name_of=lambda kid: next(r["name"] for r in rows if r["id"] == kid),
            get_row=lambda kid: next((r for r in rows if r["id"] == kid), None),
        ),
        photo_keywords=types.SimpleNamespace(
            photo_ids_tagged_with=lambda kid, ids: [],
        ),
        transaction=contextlib.nullcontext,
        tag_photo=lambda pid, kid, **kw: tagged.append((pid, kid)),
        record_edit=lambda *a, **kw: None, _prune_edit_history=lambda: None)
    ns = {"get_db": lambda: db, "request": types.SimpleNamespace(get_json=lambda **kw: body),
          "parse_selection_photo_ids": lambda db, value, **kw: (value["photo_ids"], None),
          "json_error": forbidden, "KEYWORD_TYPES": {"general", "taxonomy", "location"},
          "jsonify": lambda value: value,
          "queue_keyword_add": lambda db, pid, name, **kw: queued.append((pid, name))}
    exec(compile(ast.Module(body=[route], type_ignores=[]), "<inert keyword route>", "exec"), ns)
    class Client:
        get_json = forbidden
        def post_json(self, path, value):
            assert path == "/api/batch/keyword"
            body.clear()
            body.update(value)
            return ns["api_batch_keyword"]()
    registry = namespace()["build_server"](Client())
    assert registry.tools["add_keyword"]([7], name=" “Say’s phoebe” ") == {
        "updated": 1, "keyword_id": 35}
    assert registry.tools["add_keyword"]([7], name="Say's phoebe") == {
        "updated": 1, "keyword_id": 35}
    assert registry.tools["add_keyword"]([7], keyword_id=21) == {
        "updated": 1, "keyword_id": 21}
    assert tagged == [(7, 35), (7, 35), (7, 21)]
    assert queued == [(7, "Say's phoebe")] * 3


def flag_route_fixture():
    """Actual selected route and validator with fake persistence only."""
    tree = ast.parse((ROOT / "web/photo_review.py").read_text(encoding="utf-8"))
    nodes = [n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)
             and n.name in {"flag_provenance", "_valid_photo_id", "set_flags"}]
    for node in nodes:
        node.decorator_list = []
    body, persisted = {}, []
    def write(ids, flag, **kw):
        persisted.append((ids, flag, kw))
        return len(ids)
    def error(message, *a):
        raise ValueError(message)
    ns = {"request": types.SimpleNamespace(get_json=lambda **kw: body),
          "VALID_FLAGS": ("none", "flagged", "rejected"), "FLAG_SOURCE_MAX_CHARS": 60,
          "FLAG_REASON_MAX_CHARS": 300, "get_db": lambda: object(),
          "PhotoReviewService": lambda db: types.SimpleNamespace(set_flags=write),
          "json_error": error, "jsonify": lambda value: value}
    exec(compile(ast.Module(body=nodes, type_ignores=[]), "<inert flag route>", "exec"), ns)
    class Client:
        get_json = forbidden
        def post_json(self, path, value):
            assert path == "/api/batch/flag"
            body.clear()
            body.update(value)
            return ns["set_flags"]()
    return Client(), body, persisted


def test_flag_provenance_reaches_the_validating_route_without_response_echoes():
    client, body, persisted = flag_route_fixture()
    flag = namespace()["build_server"](client).tools["set_flag"]
    assert flag([7], "rejected", source=" invented-model ", reason=" blurred subject ") == {
        "updated": 1, "flag": "rejected"}
    assert body == {"photo_ids": [7], "flag": "rejected",
                    "source": " invented-model ", "reason": " blurred subject "}
    assert persisted[-1] == ([7], "rejected", {"source": "invented-model", "reason": "blurred subject"})
    flag([7], "none")
    assert body == {"photo_ids": [7], "flag": "none"}
    assert persisted[-1] == ([7], "none", {"source": None, "reason": None})
    flag([7], "flagged", reason="   ")
    assert persisted[-1][2] == {"source": None, "reason": None}
    before = list(persisted)
    for kw in ({"source": "x" * 61}, {"reason": "x" * 301}, {"source": 7}, {"reason": []}):
        try:
            flag([7], "rejected", **kw)
        except ValueError:
            pass
        else:
            raise AssertionError("invalid provenance reached fake persistence")
    assert persisted == before


def test_photo_detail_exposes_only_current_allowlisted_flag_note_fields():
    ns = namespace()
    note = {"flag": "rejected", "source": "invented-model", "reason": "blurred subject",
            "description": "invented audit", "at": "invented-time",
            "token": "must-not-surface", "db_path": "/invented/private", "history": [{"secret": True}]}
    class Client:
        def get_json(self, path, params=None):
            assert path == "/api/photos/7"
            return {"id": 7, "flag": "rejected", "flag_note": note, "metadata": {}}
        def post_json(self, path, body=None):
            assert (path, body) == ("/api/photos/analysis-readiness", {"photo_ids": [7]})
            return {"photos": [{"photo_id": 7, "status": "not_run", "eligible": False,
                                "reasons": ["detection_not_run"], "explanation": "invented",
                                "provenance": {"token": "must-not-surface"}}]}
    result = ns["build_server"](Client()).tools["get_photo"](7)
    assert result["flag_note"] == {key: note[key] for key in ("flag", "source", "reason", "description", "at")}
    assert result["analysis_status"] == "not_run" and result["quality_analyzed"] is False
    assert "provenance" not in result and "must-not-surface" not in json.dumps(result)
    assert "flag_note" not in ns["photo_summary"]({"flag_note": note})
    assert ns["photo_detail"]({"flag_note": None})["flag_note"] is None
    assert ns["photo_detail"]({"flag_note": "malformed"})["flag_note"] is None
    assert "flag_note" not in ns["photo_detail"]({"id": 7})


def test_dependency_contract_matches_the_v2_sdk_import_surface():
    import tomllib
    project = tomllib.loads((ROOT.parent / "pyproject.toml").read_text(encoding="utf-8"))
    lock = tomllib.loads((ROOT.parent / "uv.lock").read_text(encoding="utf-8"))
    requirements = [dep for dep in project["project"]["dependencies"] if dep.startswith("mcp>")]
    assert requirements == ["mcp>=2.0,<3"]
    app = next(package for package in lock["package"] if package["name"] == "vireo")
    requirement = next(dep for dep in app["metadata"]["requires-dist"] if dep["name"] == "mcp")
    assert requirement["specifier"] == ">=2.0,<3"
    sdk = next(package for package in lock["package"] if package["name"] == "mcp")
    assert 2 <= int(sdk["version"].split(".")[0]) < 3
    # Upstream v2.0.0 exports these names; v1.29.0 instead has fastmcp.
    # Inspect/execute only import statements against invented module maps.
    tree = ast.parse((ROOT / "mcp_server.py").read_text(encoding="utf-8"))
    imports = [n for n in tree.body if isinstance(n, ast.ImportFrom) and n.module.startswith("mcp.")]
    assert {(n.module, tuple(a.name for a in n.names)) for n in imports} == {
        ("mcp.server.mcpserver", ("Image", "MCPServer")),
        ("mcp.server.mcpserver.exceptions", ("ToolError",))}
    for major in (1, 2):
        def fake_import(name, *a, _major=major, **kw):
            if _major == 1:
                raise ModuleNotFoundError(name)
            return types.SimpleNamespace(Image=FakeImage, MCPServer=Registry, ToolError=RuntimeError)
        ns = {"__builtins__": {"__import__": fake_import}}
        try:
            exec(compile(ast.Module(body=imports, type_ignores=[]), "<inert SDK imports>", "exec"), ns)
        except ModuleNotFoundError:
            assert major == 1
        else:
            assert major == 2 and ns["MCPServer"] is Registry


def test_payload_trimming_and_stdio_are_explicit_without_starting_anything():
    ns = namespace()
    photo = {"id": 7, "quality_score": 0, "token": "must-not-surface", "path": "/invented/a.raw",
             "metadata": {"EXIF": {"LensModel": "invented", "CameraSerialNumber": "private"}}}
    summary = ns["photo_summary"](photo)
    # A stored score alone proves nothing: without the app's readiness the
    # state is unknown and the score is marked preliminary.
    assert summary["quality_analyzed"] is False and summary["analysis_status"] == "unknown"
    assert summary["quality_preliminary"] is True
    assert summary["analysis_explanation"] == ns["READINESS_UNCHECKED"]
    assert "token" not in summary and "path" not in summary
    complete = {"status": "complete", "eligible": True, "reasons": [], "explanation": "done"}
    analyzed = ns["photo_summary"](photo, complete)
    assert analyzed["quality_analyzed"] is True and analyzed["quality_preliminary"] is False
    assert "analysis_explanation" not in analyzed
    detail = ns["photo_detail"](photo)
    assert detail["path"] == "/invented/a.raw" and "exif" not in detail
    assert detail["exposure"] == {"LensModel": "invented"}
    assert ns["photo_detail"](photo, True)["exif"]["EXIF"]["CameraSerialNumber"] == "private"
    info = ns["connect_info"]()
    assert info["command"] == ["/invented/python", "/invented/app.py", "--mcp"]
    assert "invented-marker" not in json.dumps(info)
    tree = ast.parse((ROOT / "mcp_server.py").read_text(encoding="utf-8"))
    main = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "main")
    assert any(isinstance(n, ast.keyword) and n.arg == "transport" and isinstance(n.value, ast.Constant)
               and n.value.value == "stdio" for n in ast.walk(main))
    # No invocation of main, Registry.run, sockets, discovery, SDK or client configuration.


if __name__ == "__main__":
    for name, check in list(globals().items()):
        if name.startswith("test_"):
            check()
            print("PASS", name)
