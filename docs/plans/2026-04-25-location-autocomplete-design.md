# Location Autocomplete — Design

**Date**: 2026-04-25
**Status**: Approved, ready for implementation plan

## Problem

Vireo's location keywords are free-text strings entered in the same input as every other keyword. There's no autocomplete, no link to a real-world place, no coords, no map presence for non-EXIF photos. The `keywords` table already has `latitude`/`longitude` columns and a `parent_id` for hierarchy, but the UI never uses them.

We want iNaturalist's pattern: type a place, pick from a Google Places dropdown, get coords + a parent chain (city → state → country), have it show on the map.

## Decisions

### UX

- **Dedicated Location section** in the photo detail panel, separate from the keyword pill area. Single input bound to Google's `places.Autocomplete` widget. Always visible.
- **Single leaf per photo** with auto-rolled-up parents derived from Google's `address_components`. The user picks "Central Park"; the system implicitly tags it with Manhattan / NY / NYS / USA via `parent_id` chain.
- **Free-text fallback**: if the user hits Enter on text Google didn't match, create a `type='location'` keyword with no `place_id` and no coords. No manual pin-drop in v1.
- **EXIF GPS suggest-on-open**: when a photo has lat/lng but no location keyword, the section shows an inline reverse-geocoded suggestion ("EXIF says: Central Park, Manhattan, NY [Accept]"). Click to attach. No background mass-tagging.
- **Filled state** shows the leaf place bold + parent breadcrumbs muted, plus an × to clear.
- **Existing coordless location keywords** stay as-is; `keywords.html` gains a per-row "📍 Link…" button that opens an autocomplete modal and attaches `place_id`/coords/parent chain to the existing row. No bulk auto-match (silent mistagging risk).

### Data model

- **Add column**: `keywords.place_id TEXT` (nullable).
- **Add index**: `CREATE UNIQUE INDEX idx_keywords_place_id ON keywords(place_id) WHERE place_id IS NOT NULL`.
- Existing free-text rows keep deduping by `UNIQUE(name, parent_id)`.
- `parent_id` chain is populated from `address_components` returned by Google Place Details. Each level becomes its own keyword row, deduped by `place_id` when Google supplies one for the component.
- **No data migration.** Existing `type='location'` keywords keep working; the Link button upgrades them on demand.
- **New table** for reverse-geocode caching (server-side):

  ```sql
  CREATE TABLE IF NOT EXISTS place_reverse_geocode_cache (
    lat_grid    INTEGER NOT NULL,   -- round(lat * 1000), ~110m grid
    lng_grid    INTEGER NOT NULL,
    place_id    TEXT,               -- NULL = "Google had no match"
    response    TEXT NOT NULL,      -- raw JSON
    fetched_at  INTEGER NOT NULL,
    PRIMARY KEY (lat_grid, lng_grid)
  );
  ```

### Architecture

Hybrid client/server:

- **Client-side** (Google Maps JS Places library, key embedded in page): the per-keystroke autocomplete dropdown in the Location section and in the Link modal. Uses Google's native UI widget, not reskinned.
- **Server-side proxy** (key in `~/.vireo/config.json`, never sent to browser): Place Details lookup on user pick (so we can do parent-chain upserts atomically) and reverse-geocoding for EXIF suggestions (cached in SQLite).

### Server endpoints (all in `vireo/app.py`)

- `POST /api/photos/<int:photo_id>/location` — body `{place_id}`. Fetches Place Details, builds parent chain, upserts keywords, swaps the photo's location keyword. Returns rendered Location-section HTML fragment.
- `POST /api/photos/<int:photo_id>/location/text` — body `{name}`. Free-text fallback path. Same return shape.
- `DELETE /api/photos/<int:photo_id>/location` — clears the photo's `type='location'` keyword links (does not delete keyword rows).
- `GET /api/places/reverse-geocode?lat=&lng=` — server proxy, cached. Returns `{place_id, summary}` or `{place_id: null}`.
- `POST /api/keywords/<int:keyword_id>/link-place` — body `{place_id}`. Attaches Google data to an existing free-text keyword. Detects unique-index conflict and merges photo associations into the canonical row.

All endpoints that talk to Google read the key at request time. Empty key returns `{"error": "no_api_key"}` with HTTP 400; frontend translates into "Add a Google Maps key in Settings".

### Config

- New field `google_maps_api_key` in `vireo/config.py` `DEFAULTS` (empty default).
- Settings page gets a password-style input + one-line help linking to Google Cloud console.
- One-line note: add HTTP referrer restriction in Google Cloud console (`localhost:8080/*`) for key safety.
- Empty key = feature degrades gracefully (free-text only, no autocomplete dropdown, no EXIF suggestion).

### Map page (`vireo/templates/map.html`)

- Modify the photo-feed query: prefer `photos.latitude/longitude` (EXIF), fall back to the deepest `type='location'` keyword's coords. Photos with neither still don't appear.
- Each marker carries a `source` field (`'exif'` or `'keyword'`); the popup shows a one-line provenance footer.
- No new layer for places-as-markers (deferred to v1.5).

## Out of scope (YAGNI)

- Manual pin-drop on a Leaflet picker for free-text places.
- Background bulk reverse-geocode of all GPS-tagged photos.
- "Locations" layer on the map (places themselves as clickable markers with photo counts).
- Place metadata blob (viewport bbox, place types, etc.).
- Multi-location-per-photo tagging.
- Privacy / location obscuring.
- Re-pointing an already-linked keyword's `place_id`.

## Cost

Solo-user volume is comfortably free under Google's Essentials tier (10k events/month per SKU, three relevant SKUs: Autocomplete, Place Details, Geocoding). Reverse-geocode is heavily cached (a 200-photo trip = one call). HTTP referrer restriction is the meaningful safeguard, not in-app rate limiting.

## Open implementation questions

- Exact JS approach for embedding Google's autocomplete widget while keeping `browse.html` snappy (lazy-load the Maps JS library on first focus of the input).
- How to render the parent breadcrumb in the filled state without overflowing in narrow panels — likely truncate middle parents on hover.
- Whether the "Link" modal in `keywords.html` should also offer to merge if the picked place_id already exists, or just silently merge.
