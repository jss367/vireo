# Vireo headless API

Vireo is primarily a desktop app, but its Flask server also exposes a small
stable HTTP API for scripts and agents. This page documents what callers can
rely on.

## Using Vireo from Claude (MCP)

Vireo is also a [Model Context Protocol](https://modelcontextprotocol.io)
server, so Claude Code, Claude Desktop and other MCP clients can work on the
catalog on the subscription you already have: search photos, look at them,
read burst rankings, and set flags, ratings and keywords. Every write goes
through the same routes the app uses and is one Undo step in Vireo.

The server runs as a subprocess of the client and talks to the running Vireo
over the API below, so Vireo must be open (or started headless) while the
assistant works. It never starts an instance itself.

Settings shows the exact command for your install under **Claude and AI
assistants**, with a Copy button. From a checkout it is:

```bash
claude mcp add vireo -- python /path/to/vireo/app.py --mcp
```

and from the packaged app it is the sidecar binary, for example
`/Applications/Vireo.app/Contents/MacOS/vireo-server --mcp`. For Claude
Desktop, paste the JSON from the same Settings section into
`claude_desktop_config.json`.

`GET /api/system/mcp-config` returns the command, the Claude Code one-liner
and the Claude Desktop JSON for the running install.

Tools: `vireo_status`, `list_workspaces`, `activate_workspace`,
`list_folders`, `list_collections`, `search_keywords`, `search_photos`,
`find_photos_by_text`, `get_photo`, `view_photo` (the preview, the
thumbnail or the detected subject as an image), `burst`, `set_flag`,
`set_rating`, `add_keyword` and `remove_keyword` (flag provenance — the
`reason` and `source` shown next to the flag — lands with #2130; the tool
takes those fields then). The server reads `runtime.json` on every call;
`VIREO_URL` and `VIREO_TOKEN` override it for tests and second instances
(both required, and restricted to numeric `http://127.0.0.1` or `http://[::1]`
origins). URL names, user-info, paths, queries and fragments are refused. HTTP
redirects and environment proxies are disabled. Status does not read or report
a default-instance catalog path. Design: `docs/plans/2026-10-17-mcp-server-design.md`.

## Discovering a running instance

When the Vireo sidecar starts (either as part of the Tauri GUI or directly
from the command line), it writes `~/.vireo/runtime.json`:

```json
{
  "port": 54321,
  "pid": 12345,
  "version": "0.8.28",
  "db_path": "/home/you/.vireo/vireo.db",
  "started_at": "2026-04-22T19:30:00Z",
  "mode": "gui",
  "token": "…"
}
```

The file is `chmod 600` (user-only read) because the token is sensitive.

**Liveness:** treat `runtime.json` as authoritative only after confirming
`GET http://127.0.0.1:<port>/api/v1/health` (with the token) returns 200.
If the file is stale (process died without cleanup), the next sidecar start
will replace it automatically.

## Spawning a headless instance

If no instance is running, spawn one from the installed binary. The path
depends on how Vireo was installed:

```bash
# macOS (.app bundle)
/Applications/Vireo.app/Contents/MacOS/vireo-server \
  --headless --port 0 --db ~/.vireo/vireo.db

# Linux (run the server module directly from a checkout)
python vireo/app.py --headless --port 0 --db ~/.vireo/vireo.db
```

`--port 0` picks a free port. Poll `~/.vireo/runtime.json` until it appears
(typically <2 s), then read the port and token from it.

**Only one Vireo instance can run at a time.** If the GUI is already open,
the headless spawn will exit with a non-zero status and print a JSON error
to stderr:

```json
{"error":"already_running","port":54321,"pid":12345}
```

In that case, just connect to the running instance using the port and token
in `runtime.json` — both modes serve the same API.

## Authentication

Every `/api/v1/*` request must include the token from `runtime.json`:

```
X-Vireo-Token: <token>
```

Missing or wrong token → 401.

## Stable endpoints

Endpoints under `/api/v1` are covered by a semver contract: breaking changes
require bumping the version prefix. Everything else under `/api/*` is
internal to the GUI and may change at any time.

Current stable set:

| Method | Path | Description |
| --- | --- | --- |
| GET  | `/api/v1/health` | Liveness probe. |
| GET  | `/api/v1/version` | `{"version": "x.y.z"}`. |
| POST | `/api/v1/shutdown` | Gracefully stop the sidecar. |
| GET  | `/api/v1/photos` | List/search photos in the active workspace. |
| GET  | `/api/v1/photos/<id>` | One photo's metadata. |
| GET  | `/api/v1/collections` | Collections in the active workspace. |
| GET  | `/api/v1/collections/<id>/photos` | Photos in a collection. |
| GET  | `/api/v1/workspaces` | All workspaces. |
| POST | `/api/v1/workspaces/<id>/activate` | Switch the active workspace. |
| GET  | `/api/v1/keywords` | Keyword tree. |

Request/response shapes mirror the internal `/api/*` endpoints — see source
or open `/api/v1/<path>` with the token in your browser's dev tools for
concrete examples.

## Worked example

```bash
# 1. Locate the instance.
PORT=$(jq -r .port ~/.vireo/runtime.json)
TOKEN=$(jq -r .token ~/.vireo/runtime.json)

# 2. Probe health.
curl -sf -H "X-Vireo-Token: $TOKEN" "http://127.0.0.1:$PORT/api/v1/health"

# 3. List photos.
curl -sf -H "X-Vireo-Token: $TOKEN" "http://127.0.0.1:$PORT/api/v1/photos"

# 4. Shut down (only if you spawned the instance yourself).
curl -sf -X POST -H "X-Vireo-Token: $TOKEN" \
     "http://127.0.0.1:$PORT/api/v1/shutdown"
```
