# Headless API design

## Goal

Make Vireo reliably scriptable by agents (and other external callers) when the
user only has the packaged `.app` — not the source tree. Today the Flask
sidecar is already a full HTTP server, but the bound port is known only to
Tauri, the sidecar binary is buried inside the bundle with no documented
invocation, and there is no advertised API contract. This design closes those
gaps with three additions: a runtime-discovery file, a single-instance guard,
and a small versioned API surface.

Scope is deliberately narrow. The UI stays the primary way humans use Vireo.
Headless mode is for automation: scripts, CI, and agents like Claude Code
driving the app on behalf of a user who installed it from a DMG.

## User-facing behavior

Single flow. Claude (or any external caller) follows the same steps whether
Vireo is already running or not:

1. Read `~/.vireo/runtime.json`.
2. If it exists and `GET http://127.0.0.1:<port>/api/v1/health` (with the
   token from the file) returns 200, connect to that instance.
3. If it is missing or the health probe fails, spawn the bundled sidecar
   headlessly:
   ```
   /Applications/Vireo.app/Contents/Resources/bin/vireo-server \
     --headless --port 0 --db ~/.vireo/vireo.db
   ```
   Poll `runtime.json` until it appears, then connect.
4. When finished, callers that spawned the process may `POST /api/v1/shutdown`.
   Callers that attached to a running instance leave it alone.

**Concurrent instances are unsupported.** The sidecar refuses to start if
another healthy instance is already running. Users with the GUI open cannot
also run a standalone headless server at the same time — the second attempt
exits non-zero with a machine-readable error.

## `runtime.json`

Location: `~/.vireo/runtime.json`, next to `vireo.log` and `vireo.db`.

Written by the sidecar itself at the end of startup — after Flask has bound
the port, before it accepts real traffic. The write is atomic (`write to
.tmp`, then `os.replace`). File mode is `0600` so only the user can read the
token.

```json
{
  "port": 54321,
  "pid": 12345,
  "version": "0.8.28",
  "db_path": "/Users/julius/.vireo/vireo.db",
  "started_at": "2026-04-22T19:30:00Z",
  "mode": "gui",
  "token": "b7c9e4f1a2d8…"
}
```

- `port`, `pid` — discovery and liveness.
- `version` — lets callers check compatibility if the API ever breaks.
- `db_path` — surfaces which database the running instance has open.
- `started_at` — debugging only; not load-bearing.
- `mode` — `"gui"` (sidecar spawned by Tauri) or `"headless"` (spawned
  directly). Informational.
- `token` — 32 random bytes, base64url-encoded. Required on every
  `/api/v1/*` request via `X-Vireo-Token` header.

Cleanup: `atexit` handler and SIGTERM handler both unlink the file. Stale
files (process died hard, file left behind) are handled by the single-instance
guard.

## Single-instance guard

Same code path for both GUI-spawn and headless-spawn, runs at the top of the
sidecar's `main()`:

1. If `~/.vireo/runtime.json` does not exist, proceed.
2. If it exists and its JSON is invalid, delete it and proceed.
3. Otherwise, probe `GET http://127.0.0.1:<port>/api/v1/health` with a
   500 ms timeout, using the token from the file.
4. If the probe returns 200, the existing instance is alive. Print
   `{"error":"already_running","port":N,"pid":M}` to stderr and
   `sys.exit(1)`.
5. If the probe fails (connection refused, timeout, non-200), the file is
   stale. Delete it and proceed.

The health probe is authoritative. A PID check is not needed: a port that
answers Vireo's health endpoint is Vireo.

## API contract

Two tiers, both served from the same Flask app.

**`/api/v1/*` — stable, versioned headless API.** Breaking changes only with
a version bump. Initial endpoint set:

- `GET /api/v1/health`
- `GET /api/v1/version`
- `POST /api/v1/shutdown`
- `GET /api/v1/photos` (list/search)
- `GET /api/v1/photos/<id>`
- `GET /api/v1/collections`
- `GET /api/v1/collections/<id>/photos`
- `GET /api/v1/workspaces`
- `POST /api/v1/workspaces/<id>/activate`
- `GET /api/v1/keywords`

**`/api/*` — internal, may change without notice.** Everything the Tauri UI
uses: jobs, SSE log stream, pending changes, thumbnails, preview cache, etc.
Not advertised for external use. The Tauri sidecar code keeps calling these
as it does today — no migration needed.

Rather than duplicate handler bodies, `/api/v1/*` routes are thin wrappers
(or Flask `add_url_rule` aliases) over the existing implementation for the
endpoints in the stable set. The existing `/api/health` continues to work for
Tauri's current wait-for-health check; the Tauri sidecar code can migrate to
`/api/v1/health` later.

## Auth

Every `/api/v1/*` request must include `X-Vireo-Token: <token>` matching the
value in `runtime.json`. Missing or wrong token returns 401. Loopback-only
binding plus the token meaningfully reduces the risk of a hostile local
process (or a DNS-rebinding browser tab) poking the API.

The token is generated fresh on each sidecar start; there is no persistent
credential file. Callers are expected to read `runtime.json` at the start of
every session.

## Testing

Unit tests (`vireo/tests/test_runtime_discovery.py`):

- `write_runtime_json` is atomic (temp + rename), `chmod 600`, includes all
  required fields.
- Guard: healthy peer → exits with `already_running` and correct JSON on
  stderr.
- Guard: stale file (port refuses connection) → deletes file, proceeds.
- Guard: file with invalid JSON → deletes file, proceeds.
- Token middleware: missing header → 401, wrong token → 401, correct
  token → passes.

Integration tests (`tests/test_headless_mode.py`, runs the sidecar binary or
dev entrypoint):

- Spawn sidecar with `--headless --port 0`, poll `runtime.json` until it
  appears, verify `GET /api/v1/health` returns 200 with the token.
- Spawn a second sidecar while the first is running → exits non-zero, stderr
  contains `already_running`, first instance still healthy.
- Kill first sidecar hard (SIGKILL). Spawn a new one → detects stale file,
  starts clean.
- Graceful shutdown via `POST /api/v1/shutdown` removes `runtime.json`.

Out of scope for CI: testing against the packaged `.app`. That stays a manual
smoke test after each release.

## Implementation order

1. `vireo/runtime.py`: write/read/delete `runtime.json`, single-instance
   guard function. Pure unit tests first.
2. Wire guard + writer into sidecar startup in `vireo/app.py`. Keep the
   existing `/api/health` endpoint so the Tauri sidecar code keeps working.
3. Add an explicit `--headless` flag to `vireo/app.py` (currently implicit
   via `--no-browser`; making it explicit gives future knobs somewhere to
   hang).
4. Token generation + `X-Vireo-Token` middleware.
5. `/api/v1/*` routes for the stable set above.
6. Integration tests.
7. `docs/headless-api.md` — user-facing guide. Covers: where `runtime.json`
   lives, schema, how to spawn the sidecar from the bundle, endpoint list,
   token usage, worked example with `curl`. Linked from top-level README.
8. Manual smoke test against a packaged build before merging.

## Non-goals

- Installing a `vireo` CLI launcher at `/usr/local/bin`. Users invoke the
  in-bundle binary directly. Reconsidered later if real demand appears.
- Supporting two Vireo instances simultaneously. The single-instance guard
  makes this an explicit error; the database and filesystem caches are not
  designed for concurrent writers.
- Freezing every `/api/*` endpoint. Only `/api/v1/*` is stable.
- Remote access. Vireo binds to `127.0.0.1` only.
