# Photo Move Feature Design

## Overview

A new "Move" page lets users relocate photos from one location to another, with Vireo handling the physical file transfer via rsync and updating the database in place. All metadata, predictions, collections, and ratings are preserved.

Three move modes:

- **Whole folder** — Select a source folder, pick a destination, move everything.
- **Per-photo** — Select individual photos, pick a destination.
- **Rule-based** — Define filter criteria (classification status, rating, flag, species, folder, import age), preview matches, then execute on-demand.

Files are transferred using copy-verify-delete: rsync copies to the destination, checksums are verified, originals are deleted only after confirmation. Directory structure is preserved relative to the source — moving `/local/2024/march/birds/` to `/nas/photos/` creates `/nas/photos/2024/march/birds/`.

Complements PR #326 (missing folder detection / re-linking), which handles the reverse case: user moved files externally and needs Vireo to catch up.

## Data Model

No new tables for the core move operation — it updates existing `folders.path` values and `photos.folder_id` references. Rules need storage.

### New table: `move_rules`

| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER PK | |
| name | TEXT | User-defined label, e.g., "Archive rated hawks" |
| destination | TEXT | Absolute path to target root |
| criteria | JSON | Filter definition (see below) |
| created_at | TEXT | ISO timestamp |
| last_run_at | TEXT | ISO timestamp, nullable |

### Criteria JSON structure

```json
{
  "rating_min": 3,
  "flag": "flagged",
  "species": ["Red-tailed Hawk"],
  "folder_ids": [12, 15],
  "has_predictions": true,
  "imported_before": "2026-03-25"
}
```

All criteria fields are optional — only present fields are applied (AND logic between them).

Move history is logged to the existing app log. Queryable move history can be added later if needed.

For whole-folder moves, update `folders.path` for the folder and all children (subfolder cascade, similar to #326's relocate).

For per-photo moves, create a new folder record at the destination if needed, then update the photo's `folder_id`.

## Backend Operations

Moves run as background jobs through the existing `JobRunner` system with SSE progress streaming.

### Whole-folder move flow

1. Validate destination is accessible and has sufficient space
2. Create job, stream progress via SSE
3. `rsync -a --checksum <source>/ <destination>/`
4. Verify file counts and checksums match
5. Update `folders.path` for the folder and all child folders (cascade)
6. Delete originals via `rm -rf` on source
7. Update `folders.photo_count` if needed

### Per-photo move flow

1. Group selected photos by source folder
2. For each destination subfolder, ensure a `folders` record exists (preserving relative structure)
3. rsync each file (+ XMP sidecar + companion RAW/JPEG if present)
4. Verify checksums
5. Update `photos.folder_id` to the new folder
6. Delete originals
7. Update `photo_count` on both source and destination folders
8. If source folder is now empty, optionally remove it

### Rule-based move flow

1. Execute the criteria query to get matching photo IDs
2. Run the per-photo move flow on the result set

### Edge cases

- **Companion files** — RAW+JPEG pairs must move together
- **XMP sidecars** — Move alongside their photo
- **Filename collisions** — Fail that file and report it rather than silently overwriting
- **Thumbnails** — Stay valid since they're keyed by photo ID, not file path

## UI Design

### Move page (`/move`)

**Quick Move section:**
- Folder picker (from Vireo's known folders) for whole-folder moves
- Destination path input with folder browser
- "Move Folder" button

**Photo Move section:**
- Photo grid with multi-select (similar to browse page)
- Filter/search to narrow down photos
- Destination path input with folder browser
- Count display: "23 photos selected"
- "Preview" expands to show thumbnails
- "Move" button

**Rules section:**
- List of saved rules with name, criteria summary, last run date
- "New Rule" form: name, destination, criteria filters (rating, flag, species, folder, classification status, import age)
- "Preview" button → shows match count, expandable to thumbnails
- "Run" button → executes the rule
- Edit / delete existing rules

**Progress:**
- Moves appear in the existing bottom panel job stream
- Progress shows file count (e.g., "Moving 142/500 files") and current file name
- Errors collected and displayed as summary when the job finishes

**Navbar:**
- New "Move" entry

## Error Handling & Safety

### Pre-flight checks

- Destination path exists and is writable
- Sufficient disk space (sum of file sizes + 10% buffer)
- No filename collisions at destination
- Source files still exist on disk

### During transfer

- rsync failure → job stops, originals untouched, partial copies remain at destination
- Network interruption → same; user can re-run and rsync resumes where it left off
- Cancellation → stop rsync, originals untouched

### Post-transfer verification

- Compare file counts: source vs destination
- Checksum verification on transferred files
- Only after verification passes: update DB and delete originals
- Verification failure: report which files failed, leave originals in place, skip DB update for those files

**Key safety principle:** Originals are never deleted until verification passes. Worst case is duplicates, never lost photos.

### DB transaction

Folder/photo path updates wrapped in a single SQLite transaction. If the DB update fails after files are moved, PR #326's missing-folder detection catches the state and allows re-linking.

## Testing Strategy

### Unit tests

- Rule criteria query builder — each filter and combinations
- Folder path cascade — parent move updates all child paths
- Companion file detection — RAW+JPEG pairs and XMP sidecars grouped
- Collision detection — filename conflicts at destination
- DB transaction — updates preserve all metadata, predictions, collections

### Integration tests

- Full move cycle with temp directories: create → move → verify destination → verify originals deleted → verify DB
- Partial failure: unreadable file mid-move → reported, others succeed, failed originals untouched
- Per-photo move creates new folder records at destination
- Empty source folder cleanup
- Rule CRUD: create, preview count, execute, verify last_run_at

### Manual tests

- Move folder to actual NAS, verify resume after network interruption
- Large batch (1000+ photos) for progress streaming and performance
- Bottom panel shows move job progress
