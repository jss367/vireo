# Spotter Photo Browser Design

**Goal:** Turn Spotter into a full photo browser for organizing ~200k wildlife photos, paired with darktable for editing.

**Key decisions:**
- Flask + SQLite (sufficient for single-user desktop tool)
- XMP sidecars are the durable source of truth; SQLite is a fast local cache
- Database-first writes, batch sync to XMP
- Full offline support via local thumbnail cache and database
- Spotter classification stays as a separate tab/mode

---

## Architecture

Three layers:

**Storage layer** — SQLite database (`~/.spotter/spotter.db`) is the working store. Holds all metadata: paths, keywords, ratings, flags, EXIF data, thumbnail paths. Lives locally so it works when the NAS is offline. Thumbnail cache also lives locally (`~/.spotter/thumbnails/`).

**Sync engine** — Reconciles database and XMP sidecars. Two directions:
- **DB → XMP**: When you edit keywords/ratings in the browser, changes queue in a `pending_changes` table. A background sync writes them to XMP when the NAS is available.
- **XMP → DB**: On scan, detects XMP files modified since last scan (by mtime) and pulls changes into the DB.
- The audit tool uses this same engine to report discrepancies.

**Web layer** — Flask serves the browser UI, classification review (existing), LR import dashboard, and audit views. Single server, multiple pages.

**Data flow:**
```
NAS (photos + XMP sidecars)
    ↕ sync engine
SQLite database (local)
    ↕ Flask API
Browser UI
```

The database is a cache you can rebuild from XMP sidecars. But day-to-day, the database is what you interact with — it's fast, searchable, and works offline.

---

## Database Schema

```sql
-- Where photos live on disk
folders (
    id          INTEGER PRIMARY KEY,
    path        TEXT UNIQUE,
    parent_id   INTEGER REFERENCES folders(id),
    name        TEXT,
    photo_count INTEGER DEFAULT 0
)

-- One row per image file
photos (
    id          INTEGER PRIMARY KEY,
    folder_id   INTEGER REFERENCES folders(id),
    filename    TEXT,
    extension   TEXT,
    file_size   INTEGER,
    file_mtime  REAL,
    xmp_mtime   REAL,
    timestamp   TEXT,              -- EXIF DateTimeOriginal (ISO 8601)
    width       INTEGER,
    height      INTEGER,
    rating      INTEGER DEFAULT 0, -- 0-5 stars
    flag        TEXT DEFAULT 'none', -- 'none', 'flagged', 'rejected'
    thumb_path  TEXT,
    UNIQUE(folder_id, filename)
)

-- Many-to-many: photos ↔ keywords
keywords (
    id          INTEGER PRIMARY KEY,
    name        TEXT,
    parent_id   INTEGER REFERENCES keywords(id),
    UNIQUE(name, parent_id)
)

photo_keywords (
    photo_id    INTEGER REFERENCES photos(id),
    keyword_id  INTEGER REFERENCES keywords(id),
    PRIMARY KEY (photo_id, keyword_id)
)

-- Smart collection definitions
collections (
    id          INTEGER PRIMARY KEY,
    name        TEXT,
    rules       TEXT               -- JSON rules array
)

-- Queued changes not yet written to XMP
pending_changes (
    id          INTEGER PRIMARY KEY,
    photo_id    INTEGER REFERENCES photos(id),
    change_type TEXT,              -- 'keyword_add', 'keyword_remove', 'rating', 'flag'
    value       TEXT,
    created_at  TEXT DEFAULT (datetime('now'))
)
```

Key indexes: `photos.timestamp` for date browsing, `photos.folder_id` for folder view, `keywords.name` for search. The `pending_changes` table is the sync queue — entries get deleted after successful XMP write.

---

## Core Modules

**`scanner.py`** — Walks a folder tree, discovers photos, reads EXIF, populates the database.
- First scan: walks entire tree, reads EXIF timestamps/dimensions, inserts into `photos` and `folders`. Reads existing XMP sidecars to populate `keywords` and `photo_keywords`. Generates thumbnails.
- Incremental scan: checks `file_mtime` and `xmp_mtime` to find new/changed files only.
- Progress reporting via callback (UI can poll or use SSE).
- Handles NAS unavailability gracefully.

**`sync.py`** — Writes pending changes to XMP sidecars.
- Reads `pending_changes` table, groups by photo, writes each XMP sidecar using the existing `xmp_writer`.
- Deletes pending_changes rows after successful write.
- Tracks failures (NAS offline, permission errors) and retries next time.
- Handles XMP → DB direction: when a scan finds an XMP with a newer mtime, re-reads keywords and updates the DB.

**`thumbnails.py`** — Generates and manages the local thumbnail cache.
- Reuses `image_loader.load_image()` for RAW support.
- Stores at `~/.spotter/thumbnails/{photo_id}.jpg` (~400px).
- Generates on first scan, skips existing on incremental scan.
- Independent of NAS availability once generated.

These three modules are the only things that touch the filesystem. Everything else works through the database.

---

## UI Pages

**`/browse`** — Main grid view. Left sidebar: folder tree (collapsible) and keyword tree. Top bar: search field, date range filter, rating filter, sort options (date, name, rating). Main area: thumbnail grid with virtual scrolling. Click a thumbnail to open a detail panel: larger preview, all keywords, rating stars, EXIF summary, file path, "Open in darktable" button. Keyboard shortcuts: arrow keys to navigate, 1-5 for rating, P/X for flag/reject.

**`/classify`** — Existing Spotter review UI. Separate from browsing. Run classification on a folder and review predictions.

**`/import`** — LR migration dashboard. Three phases: select catalogs, preview & resolve conflicts, execute with progress bar.

**`/audit`** — Drift and orphan detection with resolution actions.

**`/settings`** — NAS paths, thumbnail cache location, sync behavior, scan schedule.

Navigation via top nav bar, consistent across all pages.

---

## LR Import & Migration Dashboard

**Phase 1: Select catalogs.** File picker for one or more `.lrcat` files. `catalog_reader.py` reads each one. Preview table: catalog name, photo count, keyword count, files found on disk vs not found. Flags multi-catalog overlaps.

**Phase 2: Preview & resolve.** Before writing:
- Photo count getting keywords written to XMP sidecars.
- Multi-catalog photos — expandable diff showing keywords per catalog. Options: merge all, prefer one catalog, resolve per-file.
- Photos in catalog not found on disk — list with last known paths.
- Ratings, flags, color labels imported into Spotter database (not stored in XMP).

**Phase 3: Execute.** Progress bar. Writes XMP sidecars (keywords, hierarchical keywords) and populates Spotter database (ratings, flags, folder structure). Summary: written, skipped, failed with error details.

`catalog_reader.py` and `xmp_writer.py` already do the hard work. The import UI adds preview, conflict resolution, and progress reporting.

---

## Audit System

Three tabs on `/audit`:

**Tab 1: Drift** — Photos where DB and XMP disagree. Compares `xmp_mtime` in DB against file's actual mtime. If newer, re-reads XMP and diffs. Table shows photo, field, DB value, XMP value. Per-row: "Use DB" or "Use XMP". Bulk: "Sync all to XMP" or "Sync all from XMP."

**Tab 2: Orphan DB entries** — DB rows where file no longer exists. Could be deleted, moved, or unmounted NAS. Actions: "Remove from database" or "Re-scan to find moved files" (matches by filename + EXIF timestamp).

**Tab 3: Untracked files** — Files on disk not in DB. Happens when photos added outside Spotter. Action: "Import to database" (runs scanner on those files).

Runs on-demand or as part of scheduled scan. When NAS is offline, only Tab 2 available.

---

## Smart Collections

Saved queries stored as JSON rules in `collections` table. SQL query built from rules at display time — no materialized table. SQLite is fast enough for 200k rows with proper indexes.

**Rule structure:**
```json
[
  {"field": "keyword", "op": "contains", "value": "hawk"},
  {"field": "rating", "op": ">=", "value": 4},
  {"field": "timestamp", "op": "between", "value": ["2019-01-01", "2019-12-31"]},
  {"field": "folder", "op": "under", "value": "/Volumes/Photography/Raw Files/USA"}
]
```

Rules combine with AND. Fields: `keyword`, `rating`, `flag`, `timestamp`, `folder`, `filename`, `extension`. Operators depend on field type: `contains`, `equals`, `>=`, `<=`, `between`, `under`.

**UI:** Rule builder with dropdowns and inputs. "Add rule" appends another row. Live preview shows matching count. Save with a name.

**Sidebar:** Collections appear below folder tree and keyword tree. Click to see matching photos in grid. Count badge updates on page load.

No OR logic, no nesting. Create two collections if you need OR.
