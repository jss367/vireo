# Pipeline auto-repair for broken photo metadata

## Problem

Pipeline runs against a collection silently skip the scan stage
(`pipeline_job.py:248`, `skip_scan = collection_id is not None`). Any photo
whose EXIF metadata never got extracted — i.e. `timestamp IS NULL`, and for
RAW files often `width, height = 160, 120` (the embedded JPEG thumbnail
dimensions) — stays broken across every subsequent pipeline run.

Broken timestamps poison encounter segmentation. `encounters.py:82-86` treats
a NULL timestamp as `dt = inf`, which trips the hard-cut rule at
`encounters.py:274` (`dt > 180`) for every adjacent pair, so each affected
photo becomes its own single-photo encounter. Broken dimensions separately
break thumbnails, crops, subject boxes, and working-copy extraction.

The San Elijo Lagoon workspace (id 4) currently shows this: 14,160 photos,
1,510 with `timestamp IS NULL` (1,403 in `/Raw Files/USA/2026/2026-03-28`,
100 in `2026-04-04`), and 1,680 single-photo encounters of which 1,510
trace directly to the NULL-timestamp bug.

## Goal

When the pipeline is about to process photos with broken metadata, silently
re-extract that metadata before the rest of the pipeline runs, so encounter
segmentation and downstream stages see correct data. No new user-visible
action. Runs with no broken data are unchanged.

## Non-goals

- Defining a policy for "what is a photo's canonical timestamp?" when EXIF
  has none (screenshots, exports). Those photos (7 rows in ws4) have
  `exif_data IS NOT NULL` and the scanner already correctly skips them —
  they'd remain singletons. Fallback to `file_mtime` is a separate
  discussion; `file_mtime` drifts when files are copied or restored from
  backup and is not a trustworthy substitute for EXIF `DateTimeOriginal`.
- A user-visible "repair metadata" button. The self-healing framing calls
  for automatic detection and repair.
- Migration of existing broken rows outside a pipeline run.

## Detection rule

A photo needs metadata re-extraction if:

```sql
p.timestamp IS NULL
OR (p.extension IN ('.nef','.cr2','.cr3','.arw','.raf','.dng','.rw2','.orf')
    AND p.width IS NOT NULL
    AND p.width < 1000)
```

The RAW-dimension clause is defense in depth. On ws4 it adds zero new
detections (every dim-suspect row is also `timestamp IS NULL`), but it
catches future failure modes where ExifTool partially succeeds —
timestamp populated, dimensions still 160×120.

Cross-tab from ws4 confirms the buckets:

| timestamp | dims_suspect | exif_data | count |
|-----------|-------------|-----------|-------|
| not NULL | fine | populated | 12,647 |
| NULL | yes (RAW<1000px) | NULL | 1,403 |
| NULL | fine | NULL | 95 |
| NULL | fine | populated | 7 (skipped by exif_extracted guard) |
| NULL (NULL ext) | — | NULL | 5 |
| not NULL | fine | NULL | 3 |

## Architecture

The repair lives inside `scanner_stage()` in `pipeline_job.py`, replacing
the current `if skip_scan: return` early-exit. Flow in collection mode:

1. Pipeline receives `collection_id`; resolve to photo ID set.
2. New helper `_find_broken_metadata_folders(db, photo_ids)` runs one
   indexed SQL query against the detection rule, groups by `folder_id`,
   returns `[(folder_path, broken_count), ...]`.
3. If empty, scan stage reports "Skipped (using collection)" exactly as
   today — no behavior change.
4. If non-empty, scan stage runs `do_scan(folder_path, incremental=True,
   restrict_dirs=[folder_path])` per affected folder. Scanner's existing
   incremental logic re-extracts metadata for broken photos and skips
   healthy ones in the same folder.
5. Control passes to the rest of the pipeline (thumbnails, classify,
   encounters, ...) with fresh data.

The scan stage progress label flips to "Repair metadata (N photos)" when
repair happens, so the user can see where time went when it isn't a no-op.

## Scanner change

`vireo/scanner.py:423-426`. Extend the `metadata_missing` check with a
RAW-dimension-suspect clause.

```python
# Before
metadata_missing = (
    existing["timestamp"] is None
    and existing["id"] not in exif_extracted
)

# After
dims_suspect = (
    existing.get("extension") in RAW_EXTENSIONS
    and existing.get("width") is not None
    and existing["width"] < 1000
)
metadata_missing = (
    (existing["timestamp"] is None or dims_suspect)
    and existing["id"] not in exif_extracted
)
```

`exif_extracted` (populated at `scanner.py:378-380` from
`SELECT id FROM photos WHERE exif_data IS NOT NULL`) is unchanged. It
guards against retry loops: once ExifTool has stored output, we don't
retry regardless of signal. This keeps genuine EXIF-less photos from
being pounded on every run.

`RAW_EXTENSIONS` is already imported in `scanner.py:12`. `get_photos()`
already returns `extension` and `width`, so no DB or loader change.

## Pipeline change

`vireo/pipeline_job.py`. Replace the `if skip_scan:` block inside
`scanner_stage()`:

```python
if skip_scan:
    collection_photo_ids = thread_db.get_collection_photo_ids(collection_id)
    broken = _find_broken_metadata_folders(thread_db, collection_photo_ids)
    if not broken:
        stages["scan"]["status"] = "skipped"
        runner.update_step(job["id"], "scan", status="completed",
                           summary="Skipped (using collection)")
        _update_stages(runner, job["id"], stages)
        scan_to_thumb.put(_SENTINEL)
        return

    total_broken = sum(n for _, n in broken)
    stages["scan"]["label"] = f"Repair metadata ({total_broken} photos)"
    stages["scan"]["status"] = "running"
    runner.update_step(job["id"], "scan", status="running",
                       summary=f"Repairing {total_broken} photos in {len(broken)} folders")
    _update_stages(runner, job["id"], stages)

    unreachable = 0
    for folder_path, _ in broken:
        try:
            do_scan(
                folder_path, thread_db,
                progress_callback=progress_cb,
                incremental=True,
                extract_full_metadata=pipeline_cfg.get("extract_full_metadata", True),
                photo_callback=photo_cb,
                status_callback=status_cb,
                restrict_dirs=[folder_path],
            )
        except (FileNotFoundError, NotADirectoryError, PermissionError) as e:
            log.warning("Repair scan failed for %s: %s", folder_path, e)
            unreachable += 1

    summary = f"{total_broken} photos repaired"
    if unreachable:
        summary += f", {unreachable} folders unreachable"
    stages["scan"]["status"] = "completed"
    runner.update_step(job["id"], "scan", status="completed", summary=summary)
    scan_to_thumb.put(_SENTINEL)
    return
```

New helper at module level:

```python
def _find_broken_metadata_folders(db, photo_ids):
    """Return [(folder_path, broken_count), ...] for folders containing
    any photo in photo_ids that fails the detection rule. Empty list when
    nothing is broken."""
    if not photo_ids:
        return []
    raw_exts = ",".join(f"'{e}'" for e in (
        ".nef", ".cr2", ".cr3", ".arw", ".raf", ".dng", ".rw2", ".orf"))
    placeholders = ",".join("?" * len(photo_ids))
    rows = db.conn.execute(
        f"""SELECT f.path, COUNT(*) AS n
            FROM photos p
            JOIN folders f ON p.folder_id = f.id
            WHERE p.id IN ({placeholders})
              AND (p.timestamp IS NULL
                   OR (p.extension IN ({raw_exts})
                       AND p.width IS NOT NULL AND p.width < 1000))
            GROUP BY f.id
            ORDER BY f.path""",
        tuple(photo_ids),
    ).fetchall()
    return [(r["path"], r["n"]) for r in rows]
```

`Database.get_collection_photo_ids(collection_id)` — add if missing.
Existing `get_collection_photos()` returns full rows; a lighter
ID-only variant avoids loading unused columns for this check.

## Edge cases

- **Folder path unreachable (external drive disconnected).** Each
  per-folder `do_scan` is wrapped in try/except. A failed folder logs a
  warning, increments `unreachable`, and the stage summary records it.
  Pipeline continues; encounter segmentation on those photos still
  produces singletons — unchanged from today.
- **Photo file missing from disk.** Scanner's file iteration doesn't see
  it, so it isn't re-extracted. Harmless.
- **ExifTool binary missing.** `extract_metadata` returns empty; broken
  rows stay broken; pipeline continues. Tested via mock.
- **Very large broken set.** Progress callback reports incrementally.
  No cap needed.
- **Concurrent scans.** Existing job queue serializes. Not a new concern.

## Testing

`vireo/tests/test_scanner.py` — new tests for the extended detection:

1. Incremental rescan re-processes a photo where `timestamp IS NULL`
   (regression guard for existing behavior).
2. Incremental rescan re-processes a RAW row with `width=160, height=120`
   and populated timestamp.
3. Incremental rescan does not re-process a JPEG row with
   `width=160, height=120` (RAW-specific rule).
4. Incremental rescan does not re-process a RAW row with `width=160`
   when `exif_data IS NOT NULL` (guard holds — prevents retry loop).

`vireo/tests/test_jobs_api.py` — pipeline integration:

5. Pipeline with a collection of healthy photos → scan stage
   `"skipped"`, summary "Skipped (using collection)".
6. Pipeline with a collection containing broken-metadata photos →
   scan stage `"completed"`, summary mentions repair, broken rows end
   with populated `timestamp` and corrected dimensions.
7. Pipeline where a broken photo's folder is unreachable → stage
   completes, summary mentions unreachable, other folders repaired.

## End-to-end verification on real data

After landing, on ws4 (San Elijo Lagoon):

Before:
- 1,510 rows match the detection rule
- 2,535 encounters, 1,680 single-photo (1,510 of those from NULL timestamps)

After running pipeline against the same collection used previously:
- ≤7 rows match the detection rule (only the genuinely EXIF-less)
- RAW rows with `width < 1000` → 0
- Encounter count drops significantly; single-photo encounters with
  `time_range=[None,None]` drop to ≤7
- Singleton ratio moves into the range seen in other workspaces

## Rollout

- One branch: `claude/pipeline-auto-repair-metadata`.
- One PR against `main`.
- No migration, no config flag. Behavior is strictly additive:
  previously-skipped scans stay skipped when nothing's broken.
- Self-clearing: after repair, broken rows are no longer broken, so
  subsequent runs see an empty broken set and take the fast path.

## Files touched

- `vireo/scanner.py` — 5-line heuristic extension in `metadata_missing`.
- `vireo/pipeline_job.py` — new helper + replacement of `skip_scan`
  early-return block.
- `vireo/db.py` — `get_collection_photo_ids` helper if not already present.
- `vireo/tests/test_scanner.py` — 4 new tests.
- `vireo/tests/test_jobs_api.py` — 3 new tests.
