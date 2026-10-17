# Vireo headless API

Vireo is primarily a desktop app, but its Flask server also exposes a small
stable HTTP API for scripts and agents. This page documents what callers can
rely on.

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
| GET  | `/api/v1/photos/<id>/burst-evidence` | The burst around a photo as an evidence pack: ranked frames, feature coverage, ambiguity reasons, 1:1 crop URLs. `margin`, `focus_size`, `gap_seconds`, `max_photos`. |
| POST | `/api/v1/photos/burst-evidence` | The same pack for an explicit `{"photo_ids": [...]}` selection (2–500 ids). |
| GET  | `/api/v1/photos/<id>/focus-crop` | Unresampled JPEG window around the scored focus point. `size` (128–1024, default 512); `basis` (`auto`, `eye`, `subject`, `frame`); `center=x,y` (normalized frame coordinates) puts the window where the reviewer chose, e.g. on the eye located in the preview; `source` (`auto` default, `working`, `original`): auto serves the working copy unless it is downsized against the catalog dimensions and the original can be decoded; `mask=1` shades pixels outside the SAM mask. Headers: `X-Vireo-Focus-Basis` (`eye`, `subject`, `frame`, `point`), `X-Vireo-Focus-Center`, `X-Vireo-Focus-Window`, `X-Vireo-Focus-Source` (`working_copy`, `original`), `X-Vireo-Focus-Raster-Size`, `X-Vireo-Focus-Original-Scale` (decoded pixels per original pixel, `unknown` without catalog dimensions), `X-Vireo-Focus-Mask`. |
| GET  | `/api/v1/folders/<id>/bursts` | Every burst in one folder with coverage, the scorer's pick and ambiguity; folder-level coverage too. `margin` (relative sharpness gap for a close call, default 0.10). Folders over 5000 photos answer 400. |

Request/response shapes mirror the internal `/api/*` endpoints — see source
or open `/api/v1/<path>` with the token in your browser's dev tools for
concrete examples.

### Burst adjudication

Numeric query values must be finite and within their documented ranges; malformed
values return 400. `gap_seconds` accepts 0–120 and `max_photos` accepts 2–500.
Each pack's `scoring.weights` and `scoring.rejection_thresholds` include the
scorer defaults with effective workspace overrides and nested pipeline precedence.
Each card's `focus_crop_scale` describes decoded-raster pixels; the response's
`X-Vireo-Focus-Original-Scale` header gives the real ratio once the image is
decoded. Auto crops use an eye only when eye scoring is enabled
and an eye measurement exists; `basis=eye` explicitly requests a recorded eye.
Without eye detection the subject crop is centred on the detection box, which
on a perched bird is usually the body; locate the eye on `preview_url` and ask
for `focus-crop?center=x,y` there instead.
Subject crops first use the exact detection prompt of the current active mask,
including a validated weak detection below the normal threshold; other weak
boxes do not lower the workspace threshold.

Folder entries include `scope_method` and a representative `seed_photo_id`.
Capture-time fallback scopes can overlap filename runs, matching Best Batch's
seed rules; `photos_in_bursts` counts distinct photos across the full burst chains. Each
entry scores the same default 120-frame window as its seed endpoint;
`scope_max_photos` records that limit and `burst_photo_count` reports the full
chain size. Counts, coverage, flags and the selected best photo inside an entry
refer to that scored window.

The burst endpoints exist so a reviewer that is not Vireo's UI (a script, an
agent) can second-guess the cull scorer without re-implementing it. Walk a
folder with `/api/v1/folders/<id>/bursts`; every burst carries `coverage`
(how many frames have detection, mask, quality and eye features) and
`ambiguity` (`needs_review` plus reasons: `unanalyzed_frames`, `close_call`,
`no_eye_located`, `subject_touches_edge`, `multiple_subjects`,
`all_rejected`). A burst with `unanalyzed_frames` has not been fully
processed; run Process on the folder before judging it, because the ranking
is the evidence and it is missing. For an ambiguous burst, fetch its pack and
look at each card's `focus_crop_url` (and `focus_crop_mask_url`) at 1:1
before deciding. Record decisions through the normal flag endpoints so they
show up in, and can be reversed from, the Undo panel. Design:
`docs/plans/2026-10-17-burst-evidence-adjudication-design.md`.

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
