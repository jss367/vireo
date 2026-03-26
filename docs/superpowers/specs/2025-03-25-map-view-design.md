# Map View Design Spec

## Summary

Add an interactive map page to Vireo that visualizes geolocated photos on a Leaflet.js + OpenStreetMap map. Photos are clustered at zoom levels for performance, and clicking a marker shows a popup with thumbnail, species, date, and a link back to the browse view. A filter bar lets users narrow by folder, rating, keyword, and date range.

## Why

GPS coordinates are already extracted from EXIF and stored in the `photos` table (`latitude REAL`, `longitude REAL`), but there is no way to visualize them. Wildlife photographers care about *where* — seeing photos on a map enables location-based browsing, trip review, and spotting spatial patterns in species sightings.

## Approach

**Leaflet.js + OpenStreetMap** — free, no API key, ~40KB, vanilla JS, loaded from CDN. Fits Vireo's self-hosted, zero-external-dependency philosophy.

## Components

### 1. API Endpoint: `GET /api/photos/geo`

Returns only geolocated photos with a minimal payload for map rendering.

**Query parameters** (all optional, same as `/api/photos`):
- `folder_id` — filter by folder
- `rating_min` — minimum rating
- `date_from`, `date_to` — date range
- `keyword` — keyword/filename search

**Response:**
```json
{
  "photos": [
    {
      "id": 42,
      "latitude": 37.7749,
      "longitude": -122.4194,
      "thumb_path": "abc123.jpg",
      "filename": "IMG_1234.jpg",
      "timestamp": "2024-06-15 08:32:00",
      "rating": 4,
      "species": "Red-tailed Hawk"
    }
  ],
  "total_geo": 342,
  "total_photos": 1204
}
```

**Implementation:** New method `Database.get_geolocated_photos()` that:
- Filters `WHERE latitude IS NOT NULL AND longitude IS NOT NULL`
- Joins workspace_folders for workspace scoping
- Applies the same filter conditions as `get_photos()` (folder, rating, date, keyword)
- Left-joins predictions to get the top species prediction per photo (`species` is `null` when no prediction exists)
- Returns all matching photos (no pagination — map needs all points; clustering handles density). Expected to handle up to ~10K geolocated photos comfortably; this covers the vast majority of wildlife photo libraries.
- Returns `total_geo` (geolocated count) and `total_photos` (total in workspace) for the status indicator

### 2. Route: `GET /map`

New Flask route rendering `map.html` with `active_page='map'`.

### 3. Template: `map.html`

Follows the existing template pattern: extends base styles via `vireo-base.css`, includes `_navbar.html`, page-specific CSS and JS inline.

**Layout:**
- Filter bar at top (folder selector, rating, keyword, date range) — same pattern as browse page
- Full-viewport Leaflet map filling remaining height
- Status bar at bottom: "Showing X of Y geolocated photos (Z total)"

**External dependencies (CDN):**
- `leaflet@1.9.4` — CSS and JS
- `leaflet.markercluster@1.5.3` — CSS and JS

**Map behavior:**
- On load: fetch `/api/photos/geo`, add all points as clustered markers
- Auto-fit bounds to show all markers on initial load
- Cluster markers at zoom levels (MarkerClusterGroup)
- Click marker → popup with:
  - Thumbnail image (from `/thumbnails/{thumb_path}`)
  - Filename, date, rating stars, species name
  - "View in Browse" link that navigates to `/browse?photo_id={id}`
- Filter changes → re-fetch and replace markers

**Browse page deep-link:** The browse page does not currently support a `photo_id` query parameter. The map implementation must add this: when `/browse?photo_id={id}` is loaded, navigate to the folder containing that photo, then scroll to and highlight it in the grid.

### 4. Navbar Update

Add `<a href="/map">Map</a>` to `_navbar.html` between "Dashboard" and "Workspace", with the standard `active_page` highlight pattern.

## What's Excluded (YAGNI)

- Heatmaps
- Drawing/selection tools
- Route/trip tracking
- Bulk editing from map view
- Multiple tile layer options
- Photo upload from map
- Satellite imagery toggle

These can be added later if there's demand.

## Data Flow

```
User opens /map
  → map.html loads
  → JS fetches GET /api/photos/geo
  → Flask route calls db.get_geolocated_photos()
  → SQLite query with workspace scoping + filters + lat/lon NOT NULL
  → JSON response with photo array
  → Leaflet renders clustered markers
  → User clicks marker → popup with thumbnail + metadata
  → User clicks "View in Browse" → navigates to /browse
```

## Error Handling

- No geolocated photos: show centered message "No geolocated photos found. Photos with GPS data in their EXIF metadata will appear here."
- API fetch failure: show toast error (reuse existing `safeFetch` pattern from `vireo-utils.js`)
- Photos without thumbnails: show placeholder icon in popup

## Testing

- Unit test for `get_geolocated_photos()` DB method: verify workspace scoping, filter params, NULL lat/lon exclusion
- Unit test for `/api/photos/geo` route: verify response shape, filter passthrough
- Manual verification: load map with real geotagged photos, verify clustering, popups, and filter behavior
