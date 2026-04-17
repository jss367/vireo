# Preview Pyramid: Derive All Sizes From A Canonical Working Copy

## Problem

Vireo currently generates each cached image size independently from the original source file. For RAW files, this means the expensive rawpy demosaic runs once per size requested (thumbnail, base preview, each sized variant), and at scan time a RAW may be decoded twice — once to extract the working copy, once to make the 400px thumbnail.

The per-size preview cache under `~/.vireo/previews/` also grows unbounded. A culling session that zooms through 500 photos at 2560 and 3840 can add ~1 GB of cache files with no eviction. There is no quota, no LRU, and no way for the user to cap disk use.

Across professional photo apps (Lightroom, darktable, Capture One), the dominant pattern is a **pyramid**: one canonical render per photo, all smaller sizes derived from it. Vireo already has the canonical render — the **working copy** — but the rest of the pipeline doesn't use it as the root.

## Goals

- **One RAW decode per photo, ever.** All sizes (thumbnail, previews at 1920/2560/3840) derived from the working copy.
- **Bounded disk use.** Sized previews evict under a user-configurable quota.
- **No forced migration.** Existing cache files keep working; bring-into-compliance lazily.

## Non-Goals

- Cache invalidation on pixel-affecting edits. Vireo has no such edits today (rotation is read from EXIF at load time; there is no color-profile feature). Re-scan on source mtime change is out of scope for this work.
- Changing the thumbnail size, base preview size, or `/original` behavior.
- Any frontend template changes. `/photos/{id}/full` stays a valid URL.
- RAW+JPEG companion logic (`image_loader.py:7193-7213`) — already works; not touched here.

## Design Decisions

- **Working copy is the canonical root** for every cached derivative below full resolution. For RAW, one is always extracted at scan (already true). For JPEG sources **larger than** `working_copy_max_size` (default 4096px), downsample-and-re-encode at scan. For JPEG sources at or below the cap, the source file itself is the canonical — no working copy file is written.
- **Thumbnails derive from the canonical.** Thumbnail generation reads the canonical image (working copy or source JPEG) instead of re-opening the source. Thumbs stay eager and pre-generated; only the source changes.
- **Preview endpoints unified.** `/photos/{id}/full` becomes an internal alias for `/photos/{id}/preview?size=<preview_max_size>`. One code path, one cache. Frontend URLs are unchanged.
- **LRU cache for sized previews.** New SQLite table `preview_cache` tracks `(photo_id, size, bytes, last_access_at)`. Reads touch `last_access_at`. Writes insert + evict if over quota. Files stay at `~/.vireo/previews/{id}_{size}.jpg`; only the metadata is new.
- **Configurable quota.** New config key `preview_cache_max_mb` (default 2048). Exposed in settings with current-usage indicator. Shrinking the quota triggers immediate eviction.
- **Lazy migration.** Existing preview files are adopted into the LRU on first access (their current mtime seeds `last_access_at`). JPEG working copies backfill on next scan.

## Architecture

### Canonical path resolution

A single helper, `get_canonical_image_path(photo) -> str`, returns the path the pyramid should read from:

1. If `photo.working_copy_path` exists on disk → return it.
2. Otherwise → return `photo.path` (the source file). Applies to JPEG sources at or below the cap, and as a fallback if a working copy was expected but missing (self-healing: log a warning, still serve).

This helper is the sole entry point for all derivation code. Thumbnails, previews, and sized variants all resolve their root through it.

### Working copy extraction for large JPEGs

`scanner.py:extract_working_copy` currently handles RAW only. Extend it:

- If source is RAW → current behavior (rawpy embedded-JPEG or demosaic, capped at `working_copy_max_size`).
- If source is JPEG and `max(width, height) > working_copy_max_size` → open source with Pillow, resize to `working_copy_max_size` long edge, write q92 JPEG to `~/.vireo/working/{id}.jpg`. Store path on the photo row.
- If source is JPEG and already within the cap → do nothing. No working copy file.

Backfill: existing installs have JPEGs with no working copy. Next scan's working-copy pass picks them up via the same rule (the scanner already iterates; the new branch just engages the JPEG path).

### Thumbnail generation

`thumbnails.generate_thumbnail(photo_row, dest_path)`:

- Call `get_canonical_image_path(photo)`.
- Open with Pillow, resize to `thumbnail_size` (default 400), write q85 JPEG to `dest_path`.

For RAW this removes a redundant rawpy call at scan time. For small JPEGs there's no change (source is canonical). For large JPEGs the thumb now reads the 4096-capped working copy — smaller IO, much faster.

### Unified preview endpoint

`/photos/{id}/preview?size=N` becomes the one place sized previews are served.

Request flow:

1. Validate `size` against `PREVIEW_SIZE_ALLOWLIST` (currently `{1920, 2560, 3840}`).
2. Check `preview_cache` table for a row matching `(photo_id, size)`.
3. Cache hit: `UPDATE preview_cache SET last_access_at = now() WHERE photo_id = ? AND size = ?`; stream file.
4. Cache miss: call `get_canonical_image_path(photo)`, resize with Pillow to `size` long edge, write q90 JPEG to `~/.vireo/previews/{id}_{size}.jpg`, `INSERT` row with size/bytes/now, then invoke eviction pass. Stream file.

`/photos/{id}/full` becomes a thin alias that calls the same handler with `size = preview_max_size` resolved from the effective config (workspace-aware via `get_effective_config`).

### `preview_cache` LRU table

```sql
CREATE TABLE preview_cache (
    photo_id INTEGER NOT NULL,
    size INTEGER NOT NULL,
    bytes INTEGER NOT NULL,
    last_access_at REAL NOT NULL,
    PRIMARY KEY (photo_id, size),
    FOREIGN KEY (photo_id) REFERENCES photos(id) ON DELETE CASCADE
);
CREATE INDEX preview_cache_last_access ON preview_cache(last_access_at);
```

- Global (not workspace-scoped). Previews are per-photo and photos are global.
- `ON DELETE CASCADE` guarantees rows disappear when a photo is deleted.
- Index on `last_access_at` makes eviction a single ordered scan.

### Eviction

`evict_preview_cache_if_over_quota(db, max_bytes)`:

```python
total = db.conn.execute("SELECT COALESCE(SUM(bytes), 0) FROM preview_cache").fetchone()[0]
if total <= max_bytes:
    return
# Evict oldest-accessed first until under quota.
rows = db.conn.execute(
    "SELECT photo_id, size, bytes FROM preview_cache ORDER BY last_access_at ASC"
).fetchall()
for photo_id, size, bytes_ in rows:
    if total <= max_bytes:
        break
    path = preview_cache_path(photo_id, size)
    try:
        os.remove(path)
    except FileNotFoundError:
        pass  # self-healing: row exists without file
    db.conn.execute(
        "DELETE FROM preview_cache WHERE photo_id = ? AND size = ?",
        (photo_id, size),
    )
    total -= bytes_
db.conn.commit()
```

Called after every cache write, and from the settings-save handler when `preview_cache_max_mb` shrinks.

### Lazy adoption of existing files

On cache-read for `(photo_id, size)`:

- If the row exists in `preview_cache` → normal LRU touch.
- If the row is missing **but the file exists on disk** → stat the file, `INSERT` with `bytes = st_size` and `last_access_at = st_mtime`. Then LRU touch and serve. This brings existing cached files under LRU governance without a migration step.

### Settings UI

Add one row to `settings.html`'s "Thumbnails & Previews" section:

```
Preview cache size:  [_____ MB]   Current usage: 1.2 / 2.0 GB    [Clear cache]
```

- Input bound to `preview_cache_max_mb`.
- Current usage computed from `SELECT SUM(bytes) FROM preview_cache` at page load.
- "Clear cache" button → `DELETE FROM preview_cache`, remove all `{id}_{size}.jpg` files under `~/.vireo/previews/`. Does **not** touch the base preview file (`{id}.jpg`) left over from the old scheme — those are adopted lazily and eligible for normal LRU eviction.

## Data Flow

### Scan time (per photo)

```
source file
    │
    ├── working copy extraction
    │       RAW: rawpy embedded-JPEG or demosaic → ~/.vireo/working/{id}.jpg (≤4096)
    │       JPEG > cap: Pillow resize → ~/.vireo/working/{id}.jpg (≤4096)
    │       JPEG ≤ cap: skip
    │
    └── canonical = working_copy or source
            │
            └── thumbnail: Pillow resize → ~/.vireo/thumbnails/{id}.jpg (400)
```

### Preview request (cache miss at size=2560)

```
GET /photos/123/preview?size=2560
    │
    ├── get_canonical_image_path(123) → ~/.vireo/working/123.jpg
    │
    ├── Pillow open + resize to 2560 long edge
    │
    ├── encode q90 JPEG → ~/.vireo/previews/123_2560.jpg
    │
    ├── INSERT preview_cache (photo_id=123, size=2560, bytes=712034, last_access_at=now())
    │
    ├── evict_preview_cache_if_over_quota()
    │
    └── stream file
```

## Testing

- **Unit: `get_canonical_image_path`.** Returns working-copy path when present, source path when absent, source path with warning when working-copy path is set but file is missing.
- **Unit: JPEG working copy extraction.** Large JPEG → working copy created at cap. Small JPEG → no working copy file. Verify scanner records path on photo row.
- **Unit: thumbnail derivation.** With working copy present, thumbnail reads from it. Without, reads from source.
- **Unit: `preview_cache` LRU.** Insert rows, check ordering, evict, verify files removed and rows deleted. Quota-shrink triggers eviction.
- **Unit: lazy adoption.** Existing file on disk, no row in `preview_cache` → first read inserts row with stat'd bytes and mtime-seeded access time.
- **Integration: `/photos/{id}/full` returns same bytes as `/preview?size=<preview_max_size>`.** Verifies the alias is correct.
- **Integration: cache eviction under load.** Generate N previews over quota, confirm oldest are evicted.
- **Regression: existing tests in `test_photos_api.py` for `/full`, `/preview`, `/original`, thumbnails continue to pass.**

## Rollout

- Single PR against `main` (per CLAUDE.md workflow).
- No feature flag. Lazy migration means no forced user action.
- Schema migration adds the `preview_cache` table via the existing schema-upgrade path in `db.py`.
- Default `preview_cache_max_mb = 2048`. Users with large existing caches will see eviction on first access pressure, not on upgrade.

## Open Questions

None at time of writing. All brainstorming decisions are recorded above.
