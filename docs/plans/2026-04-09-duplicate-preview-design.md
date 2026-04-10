# Duplicate Preview During Import

## Problem
When importing photos with "skip duplicates" checked, users get no visual feedback about which files are duplicates until after import completes (just a text count). Users want reassurance during preview that the right files are being skipped.

## Design Decisions
- Show duplicates **during preview** (before import starts)
- **Dimmed in place** — duplicate thumbnails stay in the grid but fade out with a "DUPLICATE" badge and updated count
- **Progressive/async** — preview loads immediately, duplicates fade out as background hashing completes

## API Changes

### New endpoint: `POST /api/import/check-duplicates`

Accepts a list of file paths, hashes them in batches, and streams results back via SSE.

Request body:
```json
{"paths": ["/path/to/file1.jpg", "/path/to/file2.cr2", ...]}
```

SSE events:
```
data: {"duplicates": ["/path/to/IMG_001.jpg", "/path/to/IMG_003.jpg"], "checked": 20, "total": 100}
data: {"duplicates": ["/path/to/IMG_015.jpg"], "checked": 40, "total": 100}
data: {"done": true, "duplicate_count": 12, "checked": 100, "total": 100}
```

Hashes files using existing `compute_file_hash()`, checks against DB `file_hash` index, streams results in batches of ~20.

## Frontend Behavior

After preview loads, JS opens an EventSource to the new endpoint with all previewed file paths.

As each SSE batch arrives:
- Thumbnails whose paths appear in `duplicates` get the existing `.duplicate` CSS class (opacity 0.4)
- A small "DUPLICATE" badge overlays the thumbnail corner
- Summary line updates progressively: "Checking for duplicates... (20/100)" → "12 duplicates will be skipped"

When done:
- Duplicate count in summary is final
- Duplicates remain dimmed but visible — no reordering
- Unchecking "skip duplicates" clears dimming/badges; re-checking re-applies (cached, no re-hash)

## Implementation Scope

**Backend (app.py + ingest.py):**
- New SSE endpoint `/api/import/check-duplicates`

**Frontend (pipeline.html):**
- EventSource call after preview loads
- Duplicate dimming + badge rendering
- Progress text updates
- Toggle behavior for skip duplicates checkbox

**CSS:**
- `.preview-thumb.duplicate` already exists (opacity 0.4)
- Add badge overlay style

**No changes to:** existing preview endpoint, import/ingest logic, hash computation.
