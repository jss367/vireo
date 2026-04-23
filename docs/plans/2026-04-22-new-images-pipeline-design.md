# New Images → Pipeline Source

## Problem

The "N new images detected in your registered folders" banner (introduced in the 2026-04-15 new-images-banner design) links to `/pipeline`. The pipeline page is a blank wizard: the user lands on it with no indication of which files the banner was talking about, and no way to scope the pipeline to those files specifically. They must manually pick a source folder and trust that the pipeline's incremental scan will pick up the new arrivals.

This is a disconnect: the banner claims specific images are new, but the workflow treats the click as a generic "go build a pipeline" navigation. Users end up running a pipeline against an entire folder when they wanted to process only the one or two files that were just dropped in.

## Goal

Clicking "Create a pipeline" from the new-images banner should land the user on the pipeline wizard with the source already scoped to exactly the images the banner announced.

Also: make "new images" a first-class pipeline source, discoverable from the pipeline page itself — not only from the banner.

## Design Decisions

- **Snapshot at click time.** When the user clicks "Create a pipeline", freeze the current list of new-image file paths into a snapshot. The pipeline processes exactly that set, even if more files arrive between clicking and running. This matches user expectation ("I saw 1 new image, process that one") and avoids the pipeline silently growing.
- **Snapshot stores file paths, not photo IDs.** New images are files on disk not yet ingested (see `vireo/new_images.py`) — there are no photo records to reference yet. The snapshot captures absolute file paths.
- **Pipeline still runs the scan stage.** Scanning is how file paths become photo records. Rather than bypass the scanner, we scope the scan to the snapshot's parent folders (incremental, as today) and then filter downstream stages to the snapshot's photo IDs. The snapshot guarantee (only these files propagate) is enforced at the scan-to-classify seam, not by short-circuiting the scan.
- **"New images" is a first-class Stage 1 source.** Alongside "Import Photos" and "Use Existing Collection", with the card only rendered when `new_count > 0`. No disabled state for caught-up workspaces (YAGNI).
- **Banner deep-links via snapshot ID in URL.** `POST /api/new-images/snapshot` → receive `snapshot_id` → navigate to `/pipeline?new_images=<snapshot_id>`. Snapshot IDs are short; URLs stay small even for thousands of new files.
- **No automatic snapshot GC in v1.** Rows are tiny (workspace_id, created_at, paths). Add cleanup later if snapshots accumulate meaningfully.

## Data Model

Two new tables:

```sql
CREATE TABLE new_image_snapshots (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  workspace_id INTEGER NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
  created_at TEXT NOT NULL,
  file_count INTEGER NOT NULL
);

CREATE TABLE new_image_snapshot_files (
  snapshot_id INTEGER NOT NULL REFERENCES new_image_snapshots(id) ON DELETE CASCADE,
  file_path TEXT NOT NULL,
  PRIMARY KEY (snapshot_id, file_path)
);
```

- Workspace-scoped with `ON DELETE CASCADE`, matching the existing pattern for `predictions`, `pending_changes`, `collections`.
- `file_path` is the absolute filesystem path, matching the identity `new_images.py` and `scanner.py` use.
- No explicit TTL column — snapshots live until their workspace is deleted.

## API

### `POST /api/new-images/snapshot`

Captures the current output of `count_new_images_for_workspace` (with `sample_limit` raised to unlimited so every new-image path is persisted) into a new snapshot.

Response:
```json
{
  "snapshot_id": 42,
  "file_count": 3,
  "folders": ["/Users/me/Photos/2026", "/Users/me/Photos/Archive"]
}
```

Called by both the banner and the pipeline page's Stage 1 card.

### `GET /api/new-images/snapshot/<id>`

Returns the snapshot summary for the pipeline page to render:
```json
{
  "file_count": 3,
  "folder_paths": ["/Users/me/Photos/2026", "/Users/me/Photos/Archive"],
  "files_sample": ["/Users/me/Photos/2026/IMG_0042.JPG", "..."]
}
```

- Full file list is not sent to the client (not needed for UI; pipeline job reads it server-side).
- Returns 404 if the snapshot ID doesn't exist or belongs to a different workspace (isolation check).

## Pipeline Job Integration

`PipelineJob` gains an optional `source_snapshot_id` parameter.

When set:

1. **Scan stage.** Derive unique parent directories from the snapshot's file paths and run the scanner in its existing incremental mode against those directories only. Do not walk the workspace's full mapped roots — that would be wasteful for a 1-file snapshot. Scanner behavior is otherwise unchanged.
2. **Resolve snapshot → photo IDs.** Join `new_image_snapshot_files` against `photos` / `folders` on `folder_path + filename`. Any snapshot path that didn't resolve to a photo (file disappeared between snapshot and run, permissions error, etc.) is dropped from the set with a single log line: `"Snapshot <id> had N files, M ingested, K missing on disk"` at INFO.
3. **Downstream stages.** Classification, extraction, grouping, etc. operate on the resolved photo-ID list only. Any *additional* files the scanner ingested from those folders (arrivals between snapshot and run) are in the DB but do not propagate — the snapshot guarantee holds.

When `source_snapshot_id` is absent, the existing folder-based pipeline behavior is unchanged.

## Frontend

### `vireo/templates/pipeline.html`

Stage 1 gets a third source card:

```
┌─ New images ──────────────────────┐
│ 3 new images detected             │
│ in 2 registered folders           │
│                                   │
│ [▸ /Users/me/Photos/2026]         │
│ [▸ /Users/me/Photos/Archive]      │
│                                   │
│ ○ Select this source              │
└───────────────────────────────────┘
```

**Render logic:**

- **Deep-link mode (`?new_images=<id>`):** skip the probe; fetch `/api/new-images/snapshot/<id>`, render the card with the snapshot's numbers, pre-select it. If the snapshot is missing or cross-workspace, toast "That snapshot has expired" and fall back to normal mode.
- **No deep link:** call `/api/new-images` on page load. If `new_count > 0`, render the card (not pre-selected). If `= 0`, don't render it at all.

**Selection behavior:**

- Selecting the card without a pre-loaded snapshot triggers `POST /api/new-images/snapshot` to freeze the current list. Store `snapshot_id` in page state.
- Pipeline submit POSTs `source_snapshot_id` in lieu of `source_folders` + `file_types`.

Stages 2–4 (Destination, Processing, Advanced) are unchanged.

### `vireo/templates/_navbar.html`

The banner's "Create a pipeline" link becomes a JS-driven button:

1. `POST /api/new-images/snapshot`.
2. On success → navigate to `/pipeline?new_images=<snapshot_id>`.
3. On failure → fall back to navigating to `/pipeline` with no param (existing behavior).

## Edge Cases

| Case | Behavior |
| --- | --- |
| Snapshot resolves to zero files (all ingested by another flow) | Pipeline short-circuits to completed state; not an error |
| `?new_images=<bad_id>` or cross-workspace ID | Toast "snapshot expired"; fall back to normal pipeline mode |
| Files deleted between snapshot and run | Scanner skips missing files (existing behavior); snapshot filter resolves fewer IDs; logged as INFO |
| Files added to folder between snapshot and run | Scanner ingests them; snapshot filter excludes them from downstream stages |
| Workspace switch after banner click, before submit | Snapshot is workspace-scoped; `/api/new-images/snapshot/<id>` returns 404; falls back gracefully |
| Two pipelines running against the same snapshot | Both execute; scan is idempotent, classification upserts — acceptable waste, not locked against in v1 |

## Testing

**Unit (`vireo/tests/test_db.py`):**

- Create a snapshot, read it back, verify `file_count` and file-path list match.
- Snapshot rows cascade-delete when the workspace is deleted.
- Reading a snapshot from a different workspace returns nothing (isolation).

**API (`vireo/tests/test_new_images_api.py` — new file):**

- `POST /api/new-images/snapshot` with pending new images → returns `snapshot_id`; DB row exists with expected paths.
- `POST` with zero new images → returns a snapshot with `file_count: 0` (callers handle empty state; do not 400).
- `GET /api/new-images/snapshot/<id>` → returns expected summary.
- `GET` with unknown ID → 404.
- `GET` with ID from a different workspace → 404 (isolation).

**Pipeline job (extend `vireo/tests/test_pipeline_job.py`):**

- Pipeline with `source_snapshot_id` → scanner walks only the snapshot's parent folders; snapshot's files become photos; downstream stages see only those photo IDs.
- Files missing on disk at run time → logged, pipeline completes with the partial set.
- Extra files arriving in the folder between snapshot and run → ingested by the scanner but NOT propagated to classification.
- Empty snapshot → pipeline short-circuits to completed state with no stage errors.

**E2E (Playwright, per the user-first-testing convention):**

- Drop a file into a registered folder → banner appears.
- Click "Create a pipeline" → lands on `/pipeline` with "New images" card selected showing "1 new image".
- Complete the wizard, submit → job runs to completion; the new image appears in the workspace grid.
- Navigate directly to `/pipeline` with no new images pending → "New images" card is not rendered.

**Explicitly out of scope for v1:**

- Snapshot GC (no GC mechanism exists yet; revisit if rows accumulate).
- Concurrency tests for two pipelines sharing a snapshot (documented as acceptable waste).

## Open Questions

- **"Extra files between snapshot and run" — warn the user?** Currently silent. If users find it confusing that a file they dropped after clicking the banner got ingested but not classified, we could surface a post-run toast "N additional files were imported but not processed by this pipeline". Low priority; add if reports surface.
- **Retention cleanup trigger.** Probably a `DELETE FROM new_image_snapshots WHERE workspace_id = ? AND created_at < datetime('now', '-7 days')` on workspace open, if/when rows become meaningful. Deferring.

## Implementation Scope

**Backend (`vireo/db.py`, `vireo/app.py`, `vireo/pipeline_job.py`, `vireo/new_images.py`):**

- Schema migration for the two new tables.
- `Database.create_new_images_snapshot(paths)` / `get_new_images_snapshot(snapshot_id)` helpers.
- `count_new_images_for_workspace` gains an "all paths" mode (raise `sample_limit` internally or add a parameter) so snapshots persist the full list.
- `POST /api/new-images/snapshot` and `GET /api/new-images/snapshot/<id>` routes.
- `PipelineJob` accepts `source_snapshot_id`; scan stage derives parent dirs from the snapshot; downstream stages filter to resolved photo IDs.

**Frontend (`vireo/templates/pipeline.html`, `vireo/templates/_navbar.html`):**

- New Stage 1 source card with deep-link and empty-state logic.
- Banner click becomes a `POST` + redirect.

**Tests:** as enumerated above.

**No changes to:**

- The new-images banner's detection logic or caching (`vireo/new_images.py` identity/walk behavior is unchanged).
- The scanner itself.
- Classification / extraction internals.
