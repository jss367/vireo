# Review Webapp V2 Design

**Goal:** Improve the review webapp with taxonomy-aware comparison, multi-model support, and neighbor photo grouping to produce more accurate categories and a faster review experience.

## 1. Taxonomy System

A local file (`vireo/taxonomy.json`) stores the full iNaturalist taxonomy, downloaded from their monthly DarwinCore Archive export (`inaturalist-taxonomy.dwca.zip`). The raw DWCA zip is ~100-200MB; we parse it once and store a JSON keyed by common name and scientific name for fast lookups.

**`vireo/taxonomy.py`** handles:
- **`download_taxonomy()`** — fetches the DWCA zip, parses `taxa.csv` and `VernacularNames.csv`, builds the JSON. Logs stats (e.g., "Loaded 723,481 taxa, 412,309 with common names"). Stores the download date. Labels that don't match any taxon are logged (e.g., `INFO: No taxon match for "Dyke Marsh" — skipping`).
- **`lookup(name)`** — returns the taxon record (rank, lineage) for a common name or scientific name. Returns `None` for non-taxa.
- **`is_species(name)`** / **`is_taxon(name)`** — convenience checks.
- **`relationship(name_a, name_b)`** — returns the taxonomic relationship: `'same'`, `'ancestor'` (a contains b), `'descendant'` (b contains a), `'sibling'` (same parent), or `'unrelated'`.

**Taxonomy record structure:**
```json
{
  "Song sparrow": {
    "taxon_id": 9135,
    "rank": "species",
    "lineage": ["Animalia", "Chordata", "Aves", "Passeriformes", "Passerellidae", "Melospiza", "Melospiza melodia"],
    "rank_levels": ["kingdom", "phylum", "class", "order", "family", "genus", "species"]
  }
}
```

**Updated comparison logic** in `compare.py`:
- Existing keyword not in taxonomy → not a species, ignore it (logged)
- Both are taxa, same species → **match**
- Prediction is a descendant of existing (e.g., family → species) → **refinement**
- Both are species/same rank, different → **disagreement**
- No existing species keywords → **new**

This replaces the current substring/shared-word heuristic with real taxonomic reasoning.

## 2. Multi-Model Results

The analyze step is model-aware. It can be run multiple times with different model configurations, and results accumulate per-model in the same output directory.

**`results.json` structure:**
```json
{
  "folder": "/Volumes/Photography/Raw Files/USA/2019/2019-03-17",
  "models": {
    "bioclip-vit-b-16": {
      "model_str": "ViT-B-16",
      "pretrained_str": "/tmp/bioclip_model/open_clip_pytorch_model.bin",
      "run_date": "2026-03-17",
      "threshold": 0.4
    }
  },
  "photos": [
    {
      "filename": "DSC_0050.jpg",
      "image_path": "...",
      "xmp_path": "...",
      "existing_species": ["sparrow"],
      "predictions": {
        "bioclip-vit-b-16": {
          "prediction": "Song sparrow",
          "confidence": 0.85,
          "category": "refinement"
        }
      },
      "status": "pending"
    }
  ]
}
```

Each model run adds its key to `models` and populates its entry under each photo's `predictions`. Running analyze a second time with a different model config merges into the existing results — it doesn't overwrite.

In the review UI, if multiple models have been run, a dropdown lets you pick which model's predictions to display. If two models agree, that's shown as a confidence boost. If they disagree, both predictions are visible on the card.

The model key is a slug derived from the model name (e.g., `bioclip-vit-b-16`).

## 3. Neighbor Photo Grouping

During the analyze step, after classification, photos are grouped by proximity. The grouping logic lives in `vireo/grouping.py`.

**Grouping algorithm:**
1. Sort images by filename (DSC_NNNN gives chronological order)
2. Read EXIF timestamps from each image
3. Walk through sequentially — consecutive photos within a time window (default: 10 seconds) are grouped together
4. Each group gets a consensus prediction: the most common species prediction across the group's frames, with confidence averaged across agreeing frames

**In `results.json`, grouped photos:**
```json
{
  "group_id": "g001",
  "representative": "DSC_0090.jpg",
  "members": ["DSC_0090.jpg", "DSC_0091.jpg", "DSC_0092.jpg"],
  "consensus": {
    "bioclip-vit-b-16": {
      "prediction": "Song sparrow",
      "confidence": 0.82,
      "individual_predictions": {"Song sparrow": 2, "Lincoln sparrow": 1}
    }
  },
  "category": "refinement",
  "existing_species": ["sparrow"],
  "status": "pending"
}
```

In the review UI, grouped photos show as a single collapsed card with the representative thumbnail, a consensus prediction, and a "(3 photos)" badge. Clicking expands to show individual frames. Accept/Skip applies to all members — accepting writes the keyword to all XMP sidecars in the group.

Ungrouped photos (singletons) work exactly as they do today.

## 4. Settings Page

A separate route at `/settings` with its own template. The review page links to it via the gear icon in the header (replacing the current inline settings panel).

**Settings page sections:**

- **Models**: list of configured models with name, model string, weights path. "Add Model" form. Each model shows last run date. Button to run analyze with a specific model.
- **Taxonomy**: last download date, stats (taxa count). "Update Taxonomy" button. Status indicator (up to date / stale / not downloaded).
- **Grouping**: enable/disable toggle. Time window slider (default 10s, range 2-60s).
- **Defaults**: default confidence threshold, default thumbnail size.

Settings are persisted to `settings.json` in the data directory alongside `results.json`.

**Review page keeps:**
- Thumbnail size slider (view preference, real-time)
- Category filter tabs
- Model selector dropdown (if multiple models have run)
- Confidence threshold filter (for filtering the current view)

## 5. Data Flow

**First-time setup:**
```bash
# Download iNaturalist taxonomy (one-time, re-run to update)
python vireo/taxonomy.py --download
# Creates vireo/taxonomy.json
```

**Per-folder workflow:**
```bash
# Analyze with default model
python vireo/analyze.py \
  --folder "/Volumes/Photography/Raw Files/USA/2019/2019-03-17" \
  --output-dir /tmp/photo-review

# Optional: analyze again with a different model
python vireo/analyze.py \
  --folder "..." \
  --output-dir /tmp/photo-review \
  --model-name "bioclip-vit-l-14" \
  --model-weights /tmp/other_model.bin

# Review
python vireo/review_server.py --data-dir /tmp/photo-review
```

**Analyze step internals:**
1. Load taxonomy from `taxonomy.json`
2. Scan folder for images
3. Read EXIF timestamps for grouping
4. Group neighbors within time window
5. For each image: classify, read XMP keywords, categorize using taxonomy
6. For each group: compute consensus prediction
7. Generate thumbnails (one per group representative, plus individual frames)
8. Merge results into `results.json` under this model's key

## 6. Files Changed and Created

**New files:**
- `vireo/taxonomy.py` — download/parse iNaturalist DWCA, lookup, relationship functions
- `vireo/grouping.py` — neighbor grouping by EXIF timestamp
- `vireo/templates/settings.html` — settings page UI
- `vireo/tests/test_taxonomy.py` — taxonomy lookup and relationship tests
- `vireo/tests/test_grouping.py` — grouping logic tests

**Modified files:**
- `vireo/compare.py` — replace substring heuristic with taxonomy-based categorization
- `vireo/analyze.py` — add multi-model merging, grouping step, taxonomy loading, EXIF reading
- `vireo/review_server.py` — add `/settings` route, model selector API, settings persistence
- `vireo/templates/review.html` — model dropdown, group expand/collapse, link to settings page
- `vireo/tests/test_compare.py` — update tests for taxonomy-based categorization
- `vireo/tests/test_analyze.py` — update for multi-model and grouping
- `vireo/tests/test_review_server.py` — add settings endpoint tests

**Unchanged:**
- `vireo/classifier.py`
- `vireo/image_loader.py`
- `lr-migration/xmp_writer.py`
