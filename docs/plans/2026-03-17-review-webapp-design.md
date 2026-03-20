# Auto-Labeler Review Webapp Design

**Goal:** Instead of writing ML predictions directly to XMP sidecars, generate a visual review webpage where the user can see what the model suggests, compare it to existing Lightroom keywords, and selectively accept predictions.

## Architecture

Three-phase workflow:

1. **Analyze** (`vireo/analyze.py`) — Scans a folder of photos. For each image: reads existing XMP keywords, runs BioCLIP classification, compares them, and generates a thumbnail JPEG. Saves everything to a `results.json` file. No XMP writes happen here.

2. **Review server** (`vireo/review_server.py`) — A lightweight Flask app that serves a single-page review UI. Reads `results.json`, serves thumbnails, and provides an API endpoint for accepting/rejecting suggestions. Shows only photos where the prediction differs from existing tags, grouped by category (New / Refinement / Disagreement).

3. **Apply** — When the user clicks "Accept" in the UI, the server writes the accepted keyword to the XMP sidecar (using the existing `xmp_writer`), then marks it as resolved in the JSON. The user then does "Read Metadata from Files" in Lightroom.

## Comparison Logic

Uses the labels file as the species vocabulary. Any existing keyword that matches (case-insensitive) something in the labels file is treated as a species tag. Everything else (locations, categories like `0Locations`, `8Landscape`, `Dyke Marsh`) is ignored for comparison.

Categories:
- **New**: no existing species keywords → model prediction is entirely new info
- **Refinement**: existing species keyword is a substring of the prediction or shares a word (e.g., `sparrow` → `Song sparrow`, `hawk` → `Red-tailed hawk`)
- **Disagreement**: existing species keyword doesn't match the prediction (e.g., `Northern cardinal` → `Blue jay`)
- **Match** (hidden): prediction matches existing keyword → not shown in review UI

## Review UI

Single-page web app:

**Header bar**: folder name, summary stats (e.g., "23 New, 8 Refinements, 3 Disagreements"), settings gear icon

**Settings panel** (toggled by gear icon): thumbnail size slider (200px–800px, default 400px), confidence threshold slider

**Main content**: filtered by tab/category buttons — New | Refinement | Disagreement | Accepted | All

**Each photo card shows**:
- Thumbnail image
- Filename
- Existing species keywords (if any)
- Model prediction + confidence bar
- Category badge (color-coded: green=new, yellow=refinement, red=disagreement)
- "Accept" button (writes prediction to XMP) and "Skip" button
- For refinements: shows both existing and suggested label

**Batch actions**: "Accept All" per category (e.g., accept all refinements above 80% confidence)

## Data Flow & File Structure

**New files in `vireo/`:**
- `analyze.py` — CLI: scans folder, classifies, compares, generates thumbnails, writes `results.json`
- `review_server.py` — Flask app serving the review UI + REST API
- `templates/review.html` — the single-page review UI
- `tests/test_analyze.py` — tests for comparison logic

**Output directory** (created per run, outside the repo):
```
/tmp/photo-review/
  results.json
  thumbnails/
    DSC_0007.jpg
    DSC_0008.jpg
    ...
```

**`results.json` structure:**
```json
{
  "folder": "/Volumes/Photography/Raw Files/USA/2019/2019-03-17",
  "settings": {"threshold": 0.4, "thumbnail_size": 400},
  "photos": [
    {
      "filename": "DSC_0050.NEF",
      "xmp_path": "/Volumes/.../DSC_0050.xmp",
      "existing_species": [],
      "prediction": "Mallard",
      "confidence": 0.76,
      "category": "new",
      "status": "pending"
    }
  ]
}
```

**REST API:**
- `GET /api/photos` — list photos with filters
- `POST /api/accept/<filename>` — write prediction to XMP, update status
- `POST /api/skip/<filename>` — mark as skipped
- `POST /api/accept-batch` — accept multiple at once
- `GET /thumbnails/<filename>` — serve thumbnail image

**Usage:**
```bash
# Step 1: Analyze
python vireo/analyze.py \
  --folder "/Volumes/Photography/Raw Files/USA/2019/2019-03-17" \
  --labels-file /tmp/usa_labels.txt

# Step 2: Review
python vireo/review_server.py
# Opens browser to http://localhost:5000
```

## Key Decisions

- Accepted predictions are written as plain keywords (e.g., `Song sparrow`), not prefixed with `auto:`
- Thumbnails are pre-generated during analysis for fast page loads
- Settings panel allows changing thumbnail size (200px–800px)
- Only differences are shown — matching predictions are hidden
