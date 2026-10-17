"""Vireo's MCP server: the catalog as tools for Claude Code, Claude Desktop
and any other Model Context Protocol client.

The server speaks MCP over stdio (the client spawns it) and reaches the
running Vireo over its HTTP API with the per-runtime token, exactly like a
script following ``docs/headless-api.md``. It opens no database and spawns no
instance: when nothing is running, every tool answers with the same plain
error telling the user to open Vireo.

Entry points: ``vireo-server --mcp`` from the packaged app and
``python vireo/app.py --mcp`` from a checkout (``app.main`` dispatches here
before the single-instance guard, since this process is not an instance).
``connect_info()`` builds the client-side configuration for this install;
``/api/system/mcp-config`` serves it to the Settings page.

Design: ``docs/plans/2026-10-17-mcp-server-design.md``.
"""

import contextlib
import ipaddress
import json
import os
import shlex
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request

from mcp.server.mcpserver import Image, MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from runtime import read_runtime_json

SERVER_NAME = "vireo"

NOT_RUNNING_MESSAGE = (
    "Vireo is not running. Open the Vireo app (or start the sidecar with "
    "--headless) and try again."
)
BAD_TOKEN_MESSAGE = (
    "Vireo rejected the API token. If VIREO_TOKEN is set, make sure it matches "
    "the token in ~/.vireo/runtime.json; otherwise restart Vireo so a fresh "
    "runtime.json is written."
)
UNPAIRED_URL_MESSAGE = (
    "VIREO_URL and VIREO_TOKEN overrides must be supplied together; "
    "runtime credentials are never borrowed by an override."
)
NON_LOOPBACK_URL_MESSAGE = (
    "VIREO_URL must be a numeric loopback HTTP origin "
    "(http://127.0.0.1:... or http://[::1]:...). "
    "Names, remote targets, credentials and URL paths are not supported."
)
_LOOPBACK_HOSTS = frozenset({"127.0.0.1", "::1"})

INSTRUCTIONS = """\
Vireo is the user's wildlife photo catalog. Tools act on the active
workspace, the same one the Vireo window shows. Photo ids are stable; folder,
collection and keyword ids come from the list_* and search_* tools.

Judging photos: never judge focus or sharpness from a thumbnail. Use
view_photo(view="preview") for the frame and view_photo(view="subject") for
the detected animal; quality fields (quality_score, sharpness,
subject_sharpness) are only present when quality_analyzed is true, which
means Vireo's Process step has run on the photo.

Writing: set_flag, set_rating, add_keyword and remove_keyword change the
catalog through the same routes the UI uses, so each call is one Undo step
in Vireo. Do not flag photos the user already flagged unless asked.
"""

_PHOTO_SUMMARY_KEYS = (
    "id", "filename", "folder_id", "timestamp", "rating", "flag", "color_label",
    "width", "height", "camera_make", "camera_model", "iso",
    "quality_score", "sharpness", "subject_sharpness", "subject_size",
    "latitude", "longitude", "location_status", "species",
)
_PHOTO_DETAIL_KEYS = _PHOTO_SUMMARY_KEYS + (
    "path", "folder_name", "companion_path", "working_copy_path",
    "xmp_path", "xmp_exists", "location", "file_size", "wildlife_excluded",
)
# EXIF tags worth surfacing without the whole dump. ExifTool groups them
# (``EXIF``, ``Composite``, ``MakerNotes``...); the first group holding a tag
# wins.
_EXPOSURE_TAGS = ("ExposureTime", "ShutterSpeed", "FNumber", "Aperture",
                  "FocalLength", "LensModel", "LensID", "ExposureCompensation")
_BURST_CARD_KEYS = (
    "id", "filename", "rank", "role", "label", "quality_pct", "quality",
    "focus", "focus_basis", "sharpness", "exposure", "flag", "rating", "reasons",
)
_COLLECTION_KEYS = ("id", "name", "photo_count", "available_photo_count",
                    "offline_photo_count", "has_visual", "can_add_photos")
_FOLDER_KEYS = ("id", "name", "parent_id", "path", "photo_count", "status",
                "is_workspace_root")

PREVIEW_SIZE = 1920
DEFAULT_PAGE_SIZE = 25
MAX_PAGE_SIZE = 200


class VireoError(ToolError):
    """A tool failure whose message is meant for the person reading the chat.

    A ``ToolError`` reaches the client verbatim; any other exception is
    reported as a bare "Error executing tool".
    """


def _loopback_base_url(value):
    """Canonical numeric-loopback origin only; never trust parser normalization."""
    if not isinstance(value, str) or not value or any(c.isspace() or ord(c) < 32 or ord(c) == 127 for c in value):
        raise VireoError(NON_LOOPBACK_URL_MESSAGE)
    try:
        parts = urllib.parse.urlsplit(value)
        address = ipaddress.ip_address(parts.hostname or "")
        port = parts.port
    except ValueError as exc:
        raise VireoError(NON_LOOPBACK_URL_MESSAGE) from exc
    if (
        parts.scheme != "http" or address.compressed not in _LOOPBACK_HOSTS
        or parts.username is not None or parts.password is not None
        or parts.path not in {"", "/"} or "?" in value or "#" in value
        or "%" in (parts.hostname or "") or parts.netloc.endswith(":")
        or (port is not None and not 1 <= port <= 65535)
    ):
        raise VireoError(NON_LOOPBACK_URL_MESSAGE)
    host = f"[{address.compressed}]" if address.version == 6 else address.compressed
    return f"{parts.scheme}://{host}" + (f":{port}" if port is not None else "")


def _is_loopback_http(url):
    """Whether the exact origin satisfies the capability destination boundary."""
    try:
        _loopback_base_url(url)
        return True
    except VireoError:
        return False


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        """Never forward the runtime capability to another origin or port."""
        return None


class VireoClient:
    """HTTP access to the running Vireo.

    ``base_url`` / ``token`` default to ``VIREO_URL`` / ``VIREO_TOKEN`` and
    then to ``~/.vireo/runtime.json``, read on every request so an instance
    started after the MCP client connected is found without a restart.
    """

    def __init__(self, base_url=None, token=None, timeout=120.0):
        self._base_url = base_url
        self._token = token
        self.timeout = timeout

    def _resolve(self):
        base_url = self._base_url if self._base_url is not None else os.environ.get("VIREO_URL")
        token = self._token if self._token is not None else os.environ.get("VIREO_TOKEN")
        if base_url is not None or token is not None:
            if not isinstance(base_url, str) or not base_url or not isinstance(token, str) or not token:
                raise VireoError(UNPAIRED_URL_MESSAGE)
            return _loopback_base_url(base_url), token
        runtime = read_runtime_json()
        if not runtime:
            raise VireoError(NOT_RUNNING_MESSAGE)
        port = runtime.get("port")
        if isinstance(port, bool) or not isinstance(port, int) or not 1 <= port <= 65535:
            raise VireoError(NOT_RUNNING_MESSAGE)
        token = runtime.get("token")
        if not isinstance(token, str) or not token:
            raise VireoError(BAD_TOKEN_MESSAGE)
        return f"http://127.0.0.1:{port}", token

    def request(self, method, path, params=None, json_body=None):
        """Return ``(status, content_type, body_bytes)`` for one request."""
        base_url, token = self._resolve()
        url = base_url + path
        if params:
            query = {k: v for k, v in params.items() if v is not None}
            if query:
                url += "?" + urllib.parse.urlencode(query)
        data = None
        headers = {"X-Vireo-Token": token, "Accept": "application/json, image/jpeg"}
        if json_body is not None:
            data = json.dumps(json_body).encode("utf-8")
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(url, data=data, method=method, headers=headers)
        try:
            opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), _NoRedirect())
            with opener.open(req, timeout=self.timeout) as resp:
                return resp.status, resp.headers.get("Content-Type", ""), resp.read()
        except urllib.error.HTTPError as err:
            return err.code, err.headers.get("Content-Type", ""), err.read()
        except (urllib.error.URLError, ConnectionError, TimeoutError, OSError) as err:
            raise VireoError(NOT_RUNNING_MESSAGE) from err

    # -- helpers over ``request`` ------------------------------------------

    def _check(self, status, content_type, body):
        if 200 <= status < 300:
            return
        if status == 401:
            raise VireoError(BAD_TOKEN_MESSAGE)
        message = ""
        if "json" in (content_type or ""):
            try:
                message = json.loads(body.decode("utf-8")).get("error") or ""
            except (ValueError, AttributeError):
                message = ""
        if not message:
            message = body.decode("utf-8", "replace").strip()[:300]
        raise VireoError(f"Vireo answered {status}: {message or 'no detail'}")

    def get_json(self, path, params=None):
        status, content_type, body = self.request("GET", path, params=params)
        self._check(status, content_type, body)
        return json.loads(body.decode("utf-8"))

    def post_json(self, path, json_body=None):
        status, content_type, body = self.request("POST", path, json_body=json_body or {})
        self._check(status, content_type, body)
        return json.loads(body.decode("utf-8")) if body else {}

    def get_bytes(self, path, params=None):
        status, content_type, body = self.request("GET", path, params=params)
        self._check(status, content_type, body)
        return content_type, body


# -- payload trimming ---------------------------------------------------------

def _pick(row, keys):
    return {key: row[key] for key in keys if key in row}


def photo_summary(photo):
    """The fields a reader needs to recognise and judge a photo."""
    out = _pick(photo, _PHOTO_SUMMARY_KEYS)
    out["quality_analyzed"] = photo.get("quality_score") is not None
    detections = photo.get("detections")
    if isinstance(detections, list):
        out["subjects_detected"] = len(detections)
    if "edit_recipe" in photo:
        out["has_edits"] = bool(photo.get("edit_recipe"))
    return out


def _exposure_from_metadata(metadata):
    if not isinstance(metadata, dict):
        return {}
    found = {}
    for group in metadata.values():
        if not isinstance(group, dict):
            continue
        for tag in _EXPOSURE_TAGS:
            if tag in group and tag not in found:
                found[tag] = group[tag]
    return found


def photo_detail(photo, include_exif=False):
    out = photo_summary(photo)
    out.update(_pick(photo, _PHOTO_DETAIL_KEYS))
    out["keywords"] = [
        _pick(k, ("id", "name", "type")) for k in photo.get("keywords") or []
    ]
    detections = photo.get("detections")
    if isinstance(detections, list):
        out["detections"] = [
            _pick(d, ("id", "category", "confidence", "detector_model"))
            for d in detections
        ]
    metadata = photo.get("metadata")
    out["exposure"] = _exposure_from_metadata(metadata)
    if include_exif:
        out["exif"] = metadata
    return out


def keyword_paths(rows):
    """Each keyword with ``path``: its ancestors joined with `` / ``."""
    by_id = {row["id"]: row for row in rows}
    out = []
    for row in rows:
        names = []
        seen = set()
        cursor = row
        while cursor is not None and cursor["id"] not in seen:
            seen.add(cursor["id"])
            names.append(cursor["name"])
            cursor = by_id.get(cursor.get("parent_id"))
        entry = _pick(row, ("id", "name", "parent_id", "type"))
        entry["path"] = " / ".join(reversed(names))
        out.append(entry)
    return out


# -- the server -----------------------------------------------------------------

def build_server(client: VireoClient):
    """Build the MCP server whose tools call Vireo through ``client``."""
    server = MCPServer(SERVER_NAME, instructions=INSTRUCTIONS)
    _register_catalog_tools(server, client)
    _register_search_tools(server, client)
    _register_photo_tools(server, client)
    _register_write_tools(server, client)
    return server


def _register_catalog_tools(server, client):
    """Status, workspaces, folders and collections."""

    @server.tool()
    def vireo_status() -> dict:
        """Whether Vireo is running, its version, the catalog it has open,
        the active workspace and how many photos it holds."""
        version = client.get_json("/api/v1/version")
        workspace = client.get_json("/api/workspaces/active", {"include_folders": "0"})
        photos = client.get_json("/api/v1/photos", {"per_page": 1})
        out = {
            "running": True,
            "version": version.get("version"),
            "active_workspace": _pick(workspace, ("id", "name")),
            "photo_count": photos.get("total"),
        }
        return out

    @server.tool()
    def list_workspaces() -> dict:
        """Every workspace with its id and which one is active. All other
        tools act on the active workspace."""
        active = client.get_json("/api/workspaces/active", {"include_folders": "0"})
        rows = client.get_json("/api/v1/workspaces")
        return {
            "workspaces": [
                {**_pick(ws, ("id", "name")), "active": ws.get("id") == active.get("id")}
                for ws in rows
            ]
        }

    @server.tool()
    def activate_workspace(workspace_id: int) -> dict:
        """Switch the active workspace (the Vireo window follows)."""
        client.post_json(f"/api/v1/workspaces/{workspace_id}/activate")
        active = client.get_json("/api/workspaces/active", {"include_folders": "0"})
        return {"active_workspace": _pick(active, ("id", "name"))}

    @server.tool()
    def list_folders(query: str | None = None) -> dict:
        """Folders in the active workspace with their ids, paths and photo
        counts. ``query`` keeps only folders whose path contains it
        (case-insensitive)."""
        rows = client.get_json("/api/folders")
        needle = (query or "").casefold()
        folders = [
            _pick(row, _FOLDER_KEYS) for row in rows
            if not needle or needle in (row.get("path") or "").casefold()
        ]
        return {"folders": folders, "total": len(folders)}

    @server.tool()
    def list_collections() -> dict:
        """Collections (saved filters and albums) in the active workspace."""
        rows = client.get_json("/api/v1/collections")
        collections = []
        for row in rows:
            entry = _pick(row, _COLLECTION_KEYS)
            rules = row.get("rules")
            if isinstance(rules, str):
                with contextlib.suppress(ValueError):
                    rules = json.loads(rules)
            entry["rules"] = rules
            collections.append(entry)
        return {"collections": collections}


def _register_search_tools(server, client):
    """Finding keywords and photos."""

    @server.tool()
    def search_keywords(query: str = "", keyword_type: str | None = None, limit: int = 50) -> dict:
        """Find keywords (species, places, people, anything tagged) by a
        case-insensitive substring of the name, among the keywords used by
        photos in the active workspace and their ancestors. Returns each
        with its ancestor ``path``. An empty query lists the top-level ones.
        ``keyword_type`` narrows to one keyword type (e.g. ``taxonomy``,
        ``location``, ``general``)."""
        rows = keyword_paths(client.get_json("/api/keywords"))
        needle = (query or "").casefold()
        matches = [
            row for row in rows
            if (needle in row["name"].casefold() if needle else row.get("parent_id") is None)
            and (keyword_type is None or row.get("type") == keyword_type)
        ]
        limit = max(1, min(limit, 500))
        return {"keywords": matches[:limit], "total_matches": len(matches)}

    @server.tool()
    def search_photos(
        folder_id: int | None = None,
        collection_id: int | None = None,
        keyword: str | None = None,
        rating_min: int | None = None,
        flag: str | None = None,
        color_label: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        sort: str = "date",
        page: int = 1,
        per_page: int = DEFAULT_PAGE_SIZE,
    ) -> dict:
        """Photos in the active workspace matching the Browse filters.
        ``keyword`` matches a keyword name substring (a species name works);
        ``flag`` is ``none``, ``flagged`` or ``rejected``; dates are ISO
        (``2024-05-01``). Paged: ``total`` is the full match count."""
        per_page = max(1, min(per_page, MAX_PAGE_SIZE))
        data = client.get_json("/api/photos", {
            "folder_id": folder_id,
            "collection_id": collection_id,
            "keyword": keyword,
            "rating_min": rating_min,
            "flag": flag,
            "color_label": color_label,
            "date_from": date_from,
            "date_to": date_to,
            "sort": sort,
            "page": page,
            "per_page": per_page,
        })
        return {
            "photos": [photo_summary(p) for p in data.get("photos", [])],
            "total": data.get("total"),
            "page": data.get("page"),
            "per_page": data.get("per_page"),
        }

    @server.tool()
    def find_photos_by_text(
        query: str,
        limit: int = 20,
        folder_id: int | None = None,
        collection_id: int | None = None,
    ) -> dict:
        """Search photos by describing them (CLIP text search over the
        active model's embeddings). ``reason`` explains an empty result the
        model cannot serve (no model, no embeddings, a classifier without
        text search)."""
        data = client.get_json("/api/photos/search", {
            "q": query,
            "limit": max(1, min(limit, MAX_PAGE_SIZE)),
            "folder_id": folder_id,
            "collection_id": collection_id,
        })
        results = []
        for item in data.get("results", []):
            photo = item.get("photo") or {}
            entry = photo_summary(photo)
            entry.update({k: v for k, v in item.items() if k != "photo"})
            results.append(entry)
        out = {
            "results": results,
            "total_matches": data.get("total_matches"),
            "model_used": data.get("model_used"),
        }
        if data.get("reason"):
            out["reason"] = data["reason"]
        return out


def _register_photo_tools(server, client):
    """One photo: its detail, its pixels and its burst."""

    @server.tool()
    def get_photo(photo_id: int, include_exif: bool = False) -> dict:
        """One photo in full: file paths, keywords, species, location,
        detections, exposure settings and quality fields.
        ``include_exif`` adds the complete EXIF dump."""
        return photo_detail(client.get_json(f"/api/photos/{photo_id}"), include_exif)

    @server.tool()
    def view_photo(photo_id: int, view: str = "preview") -> Image:
        """Look at a photo. ``view`` is ``preview`` (the frame at 1920 px,
        use this to judge a photo), ``thumbnail`` (small and fast, for
        composition only) or ``subject`` (the detected animal cropped
        from the frame, for identification)."""
        if view == "preview":
            content_type, body = client.get_bytes(
                f"/photos/{photo_id}/preview", {"size": PREVIEW_SIZE}
            )
        elif view == "thumbnail":
            content_type, body = client.get_bytes(f"/thumbnails/{photo_id}.jpg")
        elif view == "subject":
            content_type, body = client.get_bytes(f"/photos/{photo_id}/crop")
        else:
            raise VireoError("view must be 'preview', 'thumbnail' or 'subject'")
        fmt = "png" if "png" in (content_type or "") else "jpeg"
        return Image(data=body, format=fmt)

    @server.tool()
    def burst(photo_id: int) -> dict:
        """The burst of frames shot around a photo, ranked by Vireo's
        quality analysis: which frame it would keep and why, and which it
        suggests rejecting. Needs the Process step to have run on the
        frames."""
        data = client.get_json(f"/api/photos/{photo_id}/best-batch")
        return {
            **_pick(data, ("seed_photo_id", "best_photo_id", "best_filename",
                           "best_reasons", "count", "scope_method",
                           "suggested_reject_ids", "alternate_ids")),
            "frames": [_pick(card, _BURST_CARD_KEYS) for card in data.get("cards", [])],
        }


def _register_write_tools(server, client):
    """The catalog writes; each is one Undo step in Vireo."""

    @server.tool()
    def set_flag(photo_ids: list[int], flag: str) -> dict:
        """Flag photos: ``flagged`` (a pick), ``rejected`` or ``none``.

        Flag provenance (the reason the flag was set and who set it) will
        land with #2130; this tool will take ``reason`` and ``source``
        parameters then. Until the backing route exists, there is nowhere
        to put those fields and the tool does not pretend otherwise."""
        data = client.post_json(
            "/api/batch/flag", {"photo_ids": photo_ids, "flag": flag}
        )
        return {"updated": data.get("updated"), "flag": flag}

    @server.tool()
    def set_rating(photo_ids: list[int], rating: int) -> dict:
        """Set the star rating (0 to 5) on photos."""
        data = client.post_json("/api/batch/rating", {"photo_ids": photo_ids, "rating": rating})
        return {"updated": data.get("updated"), "rating": rating}

    @server.tool()
    def add_keyword(
        photo_ids: list[int], name: str | None = None, keyword_id: int | None = None
    ) -> dict:
        """Tag photos with a keyword by ``keyword_id`` (from search_keywords)
        or by ``name``, which creates the keyword if it does not exist."""
        body: dict[str, object] = {"photo_ids": photo_ids}
        if keyword_id is not None:
            body["keyword_id"] = keyword_id
        elif name:
            body["name"] = name
        else:
            raise VireoError("give a keyword name or keyword_id")
        data = client.post_json("/api/batch/keyword", body)
        if keyword_id is None and name:
            # The route reports only the tag count; name the keyword it
            # resolved (or created) so a follow-up can use the id.
            stored = name.strip().casefold()
            for row in client.get_json("/api/keywords"):
                if row.get("name", "").casefold() == stored:
                    keyword_id = row["id"]
                    break
        return {"updated": data.get("updated"), "keyword_id": keyword_id}

    @server.tool()
    def remove_keyword(photo_ids: list[int], keyword_id: int) -> dict:
        """Remove a keyword from photos."""
        return client.post_json(
            "/api/batch/keyword-remove", {"photo_ids": photo_ids, "keyword_id": keyword_id}
        )


# -- connection info for clients -----------------------------------------------

def server_command():
    """The argv that starts this MCP server for this install.

    A PyInstaller build is the sidecar binary itself; a checkout runs
    ``app.py`` with the interpreter in use.
    """
    if getattr(sys, "frozen", False):
        return [sys.executable, "--mcp"]
    app_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "app.py")
    return [sys.executable, app_path, "--mcp"]


def connect_info():
    """What a user pastes to connect Claude Code or Claude Desktop."""
    command = server_command()
    if sys.platform == "win32":
        quoted = subprocess.list2cmdline(command)
    else:
        quoted = shlex.join(command)
    return {
        "command": command,
        "claude_code": f"claude mcp add {SERVER_NAME} -- {quoted}",
        "claude_desktop": {
            "mcpServers": {SERVER_NAME: {"command": command[0], "args": command[1:]}}
        },
    }


def main():
    """Serve MCP over stdio until the client closes the pipe."""
    build_server(VireoClient()).run(transport="stdio")


if __name__ == "__main__":
    main()
