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
`set_rating`, `add_keyword` and `remove_keyword`. Identification tools are
`get_species_evidence`, `preview_species_assignment` and
`apply_species_assignment`; see [species review evidence](species-review-evidence.md).
`view_photo(view="subject", detection_id=...)` addresses the exact animal region,
and `get_photo` includes saved `species_assignments` and their provenance.
`set_flag` accepts optional
`source` (who decided, up to 60 characters) and `reason` (why, up to 300);
`get_photo` returns the current `flag_note` with only flag, source, reason,
description and timestamp fields. The server reads `runtime.json` on every call;
`VIREO_URL` and `VIREO_TOKEN` override it for tests and second instances
(both required, and restricted to numeric `http://127.0.0.1` or `http://[::1]`
origins). URL names, user-info, paths, queries and fragments are refused. HTTP
redirects and environment proxies are disabled. Status does not read or report
a default-instance catalog path. Design: `docs/plans/2026-10-17-mcp-server-design.md`.

## Discovering a running instance

When the Vireo sidecar starts (either as part of the Tauri GUI or directly
from the command line), it writes `runtime.json` in the active data folder:

- Existing installations: `~/.vireo/runtime.json`.
- Fresh macOS installations: `~/Library/Application Support/Vireo/runtime.json`.
- Fresh Windows installations: `%LOCALAPPDATA%\Vireo\runtime.json`.
- Fresh Linux installations: `$XDG_DATA_HOME/Vireo/runtime.json`, falling back
  to `~/.local/share/Vireo/runtime.json`.

The [storage guide](uninstalling.md#default-folders-and-upgrades) describes
layout selection and environment overrides. From a source checkout, resolve
the exact path with `python -c 'from vireo.app_paths import default_path; print(default_path("runtime"))'`.

Example runtime contents:

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
  --headless --port 0

# Linux (run the server module directly from a checkout)
python vireo/app.py --headless --port 0
```

`--port 0` picks a free port. Poll the selected `runtime.json` path until it appears
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
| POST | `/api/v1/photos/<id>/flag` | Set one photo's flag (requires a JSON object and an explicit `flag`; malformed input returns 400): `{"flag": "none"\|"flagged"\|"rejected", "source"?: str, "reason"?: str}`. `observed_flag` optionally compares the displayed flag under the flag-write lock; drift returns 409 `flag_conflict` without writing. `observed_workspace_id` optionally guards the displayed workspace (409 `workspace_changed` on drift). `source` (≤ 60 chars) is who decided, `reason` (≤ 300) why; both land in edit history and a durable current note. The Undo panel retains the audit text; `flag_note` describes only the current attributed write. Any later flag write, including Undo/Redo, conservatively clears the current note. |
| POST | `/api/v1/batch/flag` | The same for `{"photo_ids": [...], "flag": ..., "source"?, "reason"?}`; one undoable edit. IDs must be positive integers below `2**63`. |
| GET  | `/api/v1/photos/<id>/burst-evidence` | The burst around a photo as an evidence pack: ranked frames, feature coverage, ambiguity reasons, 1:1 crop URLs. `margin`, `focus_size`, `gap_seconds`, `max_photos`. |
| POST | `/api/v1/photos/burst-evidence` | The same pack for an explicit `{"photo_ids": [...]}` selection (2–500 ids). |
| POST | `/api/v1/photos/species-evidence` | Read-only identification evidence for 1–100 exact unique `photo_ids`: subjects, model candidates, score meanings, uncertainty, list provenance and bound crop URLs. No automatic continuity or assignment claim. |
| GET  | `/api/v1/photos/<id>/focus-crop` | Unresampled JPEG window around the scored focus point. `size` (128–1024, default 512); `basis` (`auto`, `eye`, `subject`, `frame`); `center=x,y` uses normalized coordinates in the unedited upright source raster, before any crop/rotation recipe; `source` (`auto` default, `working`, `original`): working requires a usable working copy and returns 404 without decoding an original when absent; auto serves the working copy unless it is downsized against the catalog dimensions and the original can be decoded; `mask=1` shades pixels outside the SAM mask. Headers: `X-Vireo-Focus-Basis` (`eye`, `subject`, `frame`, `point`), `X-Vireo-Focus-Center`, `X-Vireo-Focus-Window`, `X-Vireo-Focus-Source` (`working_copy`, `original`), `X-Vireo-Focus-Raster-Size`, `X-Vireo-Focus-Original-Scale` (decoded pixels per original pixel, `unknown` without catalog dimensions), `X-Vireo-Focus-Mask`. |
| GET  | `/api/v1/folders/<id>/bursts` | Every burst in one folder with coverage, the scorer's pick and ambiguity; folder-level coverage too. `margin` (relative sharpness gap for a close call, default 0.10). Folders over 5000 photos answer 400. |

Selected analysis recovery uses `POST /api/v1/photos/analysis-readiness` with
`{"photo_ids": [1, 2]}` (1–200 positive integer IDs visible in the active
workspace). Each photo reports current stage provenance, missing measurements,
the required retry stages and the last persisted retry failure when available.
A completed detector run without a qualifying animal remains REVIEW; it does
not establish a failed detector run.

`POST /api/v1/photos/analysis-retry` accepts the same selection plus optional
`expected_workspace_id` from the readiness response; a workspace mismatch
returns 409. It returns `job_id`, exact `photo_ids` and captured `workspace_id`.
The background job runs only required stages on those photos, bypasses incomplete
analysis caches, and reports per-photo stages, failures and final readiness in
its result. Existing job progress and cancellation routes apply. This derived
analysis operation has no metadata Undo step and never changes flags or ratings.

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
on a perched bird is usually the body. `center=x,y` refers explicitly to the
unedited upright source raster (the returned `X-Vireo-Focus-Raster-Size`),
not the recipe-rendered `preview_url`. Locate the point on an unedited upright
reference of that source; preview coordinates require undoing its crop/rotation
recipe first. The endpoint does not perform that coordinate mapping.
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

### Previewing encounter species confirmation

`POST /api/encounters/species` (also `/api/v1/encounters/species`) supports
an exact photo-set preview before writing species keywords. For example:

```json
{"species":"Eared Grebe","photo_ids":[102,103],"source_photo_ids":[101,102,103],"add":true,"split":true,"preview":true}
```

The response includes `photos` (every source photo with its filename, capture
time, flag, existing species, selected state, proposed `additions` and
`removals`, and detection-scoped stored model evidence), `photo_count`, exact
`photo_ids`, and `preview_token`. Each prediction identifies its model, label
fingerprint, review status and whether its label set is historical. Subjects
below the workspace detector threshold are marked. Stored evidence is never
itself a confirmation.

Apply by sending the same request without `preview`, with the returned
`preview_token`. Narrowing photos or changing add/replace/split choices requires
a fresh preview. The server rechecks the encounter, all source photos' keywords
and assignment provenance, requested reviewer/evidence provenance, subject boxes,
classifier output and decisions while holding the
writer lock. Stale previews return HTTP 409 without species changes.
`preserve_existing_provenance: true` leaves attribution on already-present
equivalent species assignments intact; the new MCP confirmation tools set this
option. Its value is bound to the preview token. Existing API callers retain
their default behavior of updating attribution when provenance is supplied.
The token also binds live species identities and provenance for every encounter
member whose summaries will refresh, including sibling bursts outside the
displayed source-photo matrix. Unrelated encounters remain outside that scope.

Use `add: true` to preserve existing species. To replace one explicitly named
species, omit `add` and send `previous_species`; unrelated species are retained.
`split: true` separates exactly the selected frames into another encounter and
requires fewer than all encounter members. Without it, disagreement never
changes membership. Species and membership changes share one Undo step, and
flags are preserved. Legacy requests without a preview token retain their
established API behavior; the Process Review UI always previews its decisions.

For a combined cached Group Review apply, include `group_changes` in both
preview and apply. It contains `picks`, `rejects`, `candidates`, `removed`,
`encounter_index`, `burst_index`, and `expected_burst_photo_ids`. The four
decision lists must partition that exact cached burst; selected species photos
must exclude removed frames. The preview token binds these decisions and the
whole burst's live evidence. Apply performs species, flags, removals and optional
transition splitting in one transaction and one Undo action; `group_photos`
reports the resulting flags. A failure saves neither half.

### Comparing a requested species or chosen subjects

Best Batch and burst-evidence GET/POST requests accept `requested_species` (a
species name, `taxon:<iNaturalist id>`, or `scientific:<binomial>`) and optional
`subject_overrides` mapping photo IDs to detection IDs. GET encodes the override
object as JSON in its query string. `compare_subjects=true` enables subject
comparison without a species filter. Requests without this context retain the
existing primary-subject ranking.

```json
{
  "photo_ids": [216268, 216270, 215514],
  "requested_species": "Vermilion Flycatcher",
  "subject_overrides": {"216268": 123456}
}
```

Automatic species selection requires a current detection-scoped accepted
prediction. Photo-level keywords and pending predictions identify candidates,
but never establish which bird supplies measurements. Multiple matching birds,
conflicting confirmations, and uncertain identities remain unranked review
items. An explicit override selects a retained animal in that photo for this
comparison; it does not confirm a species or change the saved primary subject.
A conflicting accepted species identification must be reviewed first.

Each card's `subject` records its `detection_id`, selection reason, species key,
candidate predictions with review/model provenance, and `analysis_key`.
`evidence_status` distinguishes ready subjects from `subject_analysis_missing`,
`subject_identity_uncertain`, `subject_identity_conflict`, and
`multiple_matching_subjects`. These review reasons also appear in `ambiguity`.
`coverage.with_selected_subject` counts resolved choices; `with_detection`
counts photos with any retained animal. Mask, quality, and eye coverage count
only evidence belonging to the selected subject. Unresolved or unanalyzed
subjects do not enter score percentiles or rejection suggestions. At least two
analyzed subjects are required for a relative comparison, and an all-rejected
comparison has no suggested best frame.

To compute missing evidence, POST the same comparison to
`/api/v1/photos/burst-evidence` with `"analyze": true`. It returns `job_id`;
poll `/api/jobs/<job_id>`, then fetch the comparison again. Job results report
`analyzed`, `cached`, unresolved identities, and per-photo errors separately.
Analysis uses original-derived images capped at 1024 pixels on the longest
edge, SAM masks, and the existing mask-gated eye policy when enabled and its
model is available. Each card reports the actual analysis dimensions and eye
availability. A missing original remains retryable. The cache retains only the
latest published analysis generation per detection, pruning superseded masks in
the same transaction. Switching to another analysis context can require
reanalysis. No flags, ratings, edit recipes, or primary-subject choices are
written by comparison analysis.

Subject focus and mask URLs carry `detection_id` and `analysis_key`; consumers
should use the returned URLs together. Their responses refuse stale or foreign
subject evidence with 404 instead of falling back to the photo's primary bird.
Focus responses include `X-Vireo-Detection-Id` and `X-Vireo-Analysis-Key`.
Full-frame previews and subject crops support visual review of pose, eye
visibility, and obstructions; this feature does not introduce aesthetic weights.


## Exporting photos with metadata

The export endpoints use an explicit `metadata_fields` list. Saved presets are
not applied automatically to API requests: read `/api/export/presets` and copy
the desired preset settings, including its metadata selection. To preserve
available photographic metadata, send the same request to
`POST /api/jobs/export/preflight` for review and `POST /api/jobs/export` to run:

```json
{
  "photo_ids": [123],
  "destination": "/absolute/path/to/exports",
  "format": "jpg",
  "quality": 92,
  "metadata_fields": ["species", "capture_date", "capture_time", "rating", "location", "camera"]
}
```

Preflight's `metadata` object contains `requested_fields`, `metadata_free`, and
a readable `description`. An empty or omitted list explicitly requests a
metadata-free export; Vireo never silently adds fields. Image/container fields
and color profiles may still be present.

Poll `/api/jobs/<job_id>` after starting the export. The completed
`result.metadata.categories` maps each requested category to **photo counts**:
`requested`, `written`, `unavailable`, `failed`, and `not_attempted`.
These last four counts sum to `requested`. A category is available when at least
one of its values can be written; camera/exposure does not imply that every
camera tag was present. Unavailable means no value was available for that
category in the catalog/effective location. No coordinates are invented.
`not_attempted` covers photos that did not reach metadata preparation, such as
render failures or cancellation. `written` counts only successful ExifTool
writes, independently of image creation. A failed metadata write removes the
derivative and reports an export error. Check these counts and `result.errors`
before describing an export as complete. For location-sensitive work, also
verify the output GPS tags with ExifTool when coordinates were available.

## Worked example

```bash
# 1. Locate the instance (run from a source checkout). For packaged installs,
# set VIREO_RUNTIME to the matching path listed above.
VIREO_RUNTIME=$(python -c 'from vireo.app_paths import default_path; print(default_path("runtime"))')
PORT=$(jq -r .port "$VIREO_RUNTIME")
TOKEN=$(jq -r .token "$VIREO_RUNTIME")

# 2. Probe health.
curl -sf -H "X-Vireo-Token: $TOKEN" "http://127.0.0.1:$PORT/api/v1/health"

# 3. List photos.
curl -sf -H "X-Vireo-Token: $TOKEN" "http://127.0.0.1:$PORT/api/v1/photos"

# 4. Shut down (only if you spawned the instance yourself).
curl -sf -X POST -H "X-Vireo-Token: $TOKEN" \
     "http://127.0.0.1:$PORT/api/v1/shutdown"
```
