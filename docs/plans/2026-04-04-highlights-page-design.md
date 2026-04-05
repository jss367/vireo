# Highlights Page Design

## Overview

A dedicated page (`/highlights`) accessible from the navbar that surfaces the best, most diverse photos from a pipeline run. Users can tune the selection interactively and save the result as a collection.

## User Flow

1. User runs a pipeline on a folder (must include regroup stage for quality scores + MMR data)
2. User navigates to Highlights page (always available in navbar)
3. Page auto-loads highlights for the most recent pipeline run's folder
4. User adjusts controls (count, max per species, quality threshold, folder) — grid updates live
5. User saves the selection as a static collection when satisfied

## Page Layout

Top controls bar with photo grid below.

```
┌─────────────────────────────────────────────────┐
│  Navbar                                         │
├─────────────────────────────────────────────────┤
│  [Folder ▼]  Count: ◄──●──► Max/species: ◄──●──►│
│  Min quality: ◄──●──►    [Save as Collection]   │
├─────────────────────────────────────────────────┤
│                                                 │
│  ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐     │
│  │     │ │     │ │     │ │     │ │     │     │
│  └─────┘ └─────┘ └─────┘ └─────┘ └─────┘     │
│  ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐     │
│  │     │ │     │ │     │ │     │ │     │     │
│  └─────┘ └─────┘ └─────┘ └─────┘ └─────┘     │
│                                                 │
│  Showing 24 highlights from 847 photos          │
│  Species: 8 · Avg quality: 0.82                │
└─────────────────────────────────────────────────┘
```

## Controls

| Control | Type | Default | Range |
|---------|------|---------|-------|
| Folder | Dropdown | Most recent pipeline run's folder | All folders with regroup data |
| Count | Slider | Adaptive (see below) | 1 to min(eligible_photos, 100) |
| Max per species | Slider | 5 | 1 to 20 |
| Min quality | Slider | 0.0 | 0.0 to 1.0, step 0.05 |
| Save as Collection | Button | — | — |

**Adaptive default count:** `min(max(total_photos * 0.05, 10), 50)` — 5% of the folder clamped between 10 and 50.

Sliders debounce 300ms then re-fetch from the API.

## API

### GET /api/highlights

Parameters:
- `folder_id` (optional) — defaults to most recent pipeline run's folder
- `count` — target number of highlights (default: adaptive)
- `max_per_species` — cap per species (default: 5)
- `min_quality` — minimum quality score threshold (default: 0.0)

Response:
- `photos` — list of photo objects (same shape as `/api/photos`)
- `meta` — folder name, total photos in folder, total eligible photos, species breakdown
- `folders` — available folders with regroup data (for the dropdown)

### POST /api/highlights/save

Body: `{ photo_ids: [...], folder_id: N, name: "..." }`

Creates a static collection using the `photo_ids` rule type.

## Selection Algorithm

Server-side, reusing existing pipeline infrastructure:

1. Query all photos in the target folder with `quality_score >= min_quality` that aren't triage-rejected
2. Group by species (from accepted predictions). Photos without predictions go under "Unidentified"
3. MMR-style selection: pick the highest quality photo, then iteratively select the next photo that maximizes quality while being sufficiently different (using existing DINO embeddings for diversity). Respect `max_per_species` cap during selection.
4. Stop when `count` is reached or candidates are exhausted

No new ML or heavy computation — reuses quality scores and DINO embeddings from the regroup stage.

## Edge Cases

**Empty state:** If no folders have regroup data, show a message explaining highlights require running a pipeline with quality scoring enabled, with a link to the jobs page.

**Fewer eligible photos than requested:** Return all eligible photos. Slider snaps to actual count.

**Photos without species:** Grouped under "Unidentified." Per-species cap applies to this group too.

**Duplicate folder highlights:** When saving, if a collection named "Highlights - {folder name}" already exists, prompt the user to replace or create new. Replace updates the existing collection's photo_ids. Create new appends "(2)", "(3)", etc.

**Ratings in lightbox:** Rating/flagging photos from the highlights lightbox persists on the photo as usual. The highlights selection doesn't change until controls are adjusted and re-fetched.

## Photo Grid

- Reuses the same card component as the browse page (thumbnail, species label, quality badge)
- Clicking a photo opens the existing lightbox (supports rating/flagging)
- Summary line below the grid: photo count, species count, average quality

## Navigation

- Always visible in navbar
- Also linked from pipeline completion notification
- Folder dropdown in controls bar lists folders with regroup data, most recent first
