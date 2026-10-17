# MCP server design

## Goal

Let people use Vireo from the Claude they already pay for: Claude Code, Claude
Desktop, Cowork, or any other MCP client. Vireo does not grow a chat
interface. A chat UI inside the app would have to run on an API key (a Claude
Pro or Max login is a Claude Code credential and may not be routed through a
third-party app), and it would be a second product surface to own. Instead
Vireo becomes an MCP server, Anthropic keeps the chat surface, auth and agent
loop, and Vireo owns the one part only it can write: the tools.

This is the same shape the burst-adjudication skill already uses from Claude
Code against the headless API, made first class.

## Shape

`vireo/mcp_server.py` is a stdio MCP server built on the official `mcp` SDK
(`FastMCP`). It runs as a subprocess of the MCP client and talks to the
running Vireo over its HTTP API with the per-runtime token, so it needs no
database access of its own and cannot race the app for the catalog.

- **Discovery.** Each tool call reads `~/.vireo/runtime.json` for the port
  and token (the file is tiny; reading it per call means Vireo may be started
  or restarted after Claude connected). `VIREO_URL` and `VIREO_TOKEN`
  override it, which tests and HOME-override instances use. The override is
  paired: `VIREO_URL` requires `VIREO_TOKEN` so the local runtime token is
  never shipped to a URL this server did not pick, and the URL must be
  numeric loopback HTTP (`127.0.0.1` or `::1`) so no DNS interpretation can
  redirect a capability. Redirect following and environment proxies are disabled;
  status never reads a separate default-instance catalog path.
- **Not running.** When nothing answers, every tool returns the same plain
  error: open Vireo (or run the sidecar headless) and try again. The server
  never spawns an instance: a chat client spawning the catalog's single
  process behind the user's back is the opposite of transparent.
- **Entry points.** `vireo-server --mcp` from the packaged app, or
  `python vireo/app.py --mcp` from a checkout. `--mcp` is handled before the
  single-instance guard because the MCP process is not an instance.
- **Connection info.** `GET /api/system/mcp-config` returns the exact
  command for this install, and Settings shows it with the Claude Code
  one-liner and the Claude Desktop JSON, so a user never guesses the path to
  the sidecar binary.

## Tools

Read tools return trimmed JSON: a photo is `id`, `filename`, `folder_id`,
`timestamp`, `rating`, `flag`, size, camera, quality fields, `species`, plus
`quality_analyzed` (true when the quality analysis has run on the photo,
which is what the burst and quality fields depend on). The full row with its
EXIF dump, cached paths and render keys stays behind `get_photo(include_exif=true)`.

| Tool | Backed by | Notes |
| --- | --- | --- |
| `vireo_status` | `/api/v1/version`, `/api/workspaces/active`, `/api/v1/photos` | Version, catalog path, active workspace, photo count. |
| `list_workspaces`, `activate_workspace` | `/api/v1/workspaces`, `.../activate` | Everything else is scoped to the active workspace, as in the UI. |
| `list_folders` | `/api/folders` | Optional substring filter on the path. |
| `list_collections` | `/api/v1/collections` | |
| `search_keywords` | `/api/keywords` | Substring match with the ancestor path, so "owl" finds the species keyword to pass to `search_photos`. |
| `search_photos` | `/api/photos` | The Browse filters: folder, collection, keyword, rating, flag, color label, dates, sort, paging. |
| `find_photos_by_text` | `/api/photos/search` | CLIP text search; reports `reason` when the active model cannot. |
| `get_photo` | `/api/photos/<id>` | |
| `view_photo` | `/thumbnails/<id>.jpg`, `/photos/<id>/preview`, `/photos/<id>/crop` | Returns the image itself (`thumbnail`, `preview` at 1920, or `subject`, the detection crop). |
| `burst` | `/api/photos/<id>/best-batch` | The ranked burst around a photo; the app's own "run the pipeline first" error passes through. |
| `set_flag` | `/api/batch/flag` | Optional `source` and `reason` are validated and persisted by the flag route; `get_photo` returns a trimmed current `flag_note`. |
| `set_rating` | `/api/batch/rating` | |
| `add_keyword`, `remove_keyword` | `/api/batch/keyword`, `/api/batch/keyword-remove` | |

No tool deletes, moves or edits pixels. Every write goes through the same
routes the UI uses, so it is one Undo step in Vireo like a hand edit.

## Testing

`vireo/tests/test_mcp_server.py` drives the tools through a transport bound
to a Flask test client (no sockets), checks the trimming, the error texts and
the posted bodies, and runs one real stdio session: it serves the test app on
a loopback port, spawns `python vireo/app.py --mcp` with `VIREO_URL` and
`VIREO_TOKEN`, and talks MCP to it with the SDK's client.

## Follow-ups

- When the burst-evidence PR lands, add `burst_evidence` and the 1:1
  `focus_crop` so the adjudication skill becomes a thin prompt over the tools.
- A Streamable HTTP transport served by the app itself would remove the
  subprocess, but Flask under waitress has no ASGI mount; revisit if a client
  that cannot spawn processes matters.
