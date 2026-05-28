# Highlights page redesign — species-bucketed view

**Date:** 2026-05-27
**Status:** Design — agreed, not yet implemented
**Branch:** `highlights-page-redesign`

## Why

The current Highlights page does not surface a user's most interesting photos.

Concrete failure in the Hawaii workspace:

- 7,138 photos, 4,655 quality-scored, only **1,416 with an accepted species** keyword (`is_species=1`). The other ~80% bucket as "Unidentified" even though Vireo has classifier predictions for most of them.
- 'Apapane: 116 photos predicted (avg confidence 0.56), 42 scored, max quality **0.77**. Highlights never shows them because the species column is `NULL` and they compete for the 5 "Unidentified" slots with random scored photos.
- Top quality scores in the workspace (0.85–0.87) are dominated by predictions like "Hawaiian Petrel, Sheep" and "Laysan × Black-footed Albatross, Sheep" — almost certainly garbage predictions on the wrong subject, with empty `species_kw`. These win on pure quality and squeeze out the actually-interesting birds.

Three compounding issues:

1. **Highlights only sees accepted species, not predictions.** Predictions live in their own table until manually accepted via Compare. The Highlights page is invisible to most of Vireo's identification work.
2. **The per-species cap groups all "Unidentified" together.** Apapane shots end up competing with sheep-meadow shots for the same 5 slots.
3. **`quality_score` is not "interestingness."** It captures sharpness/exposure, not "this is a recognizable, frame-filling bird subject."

## What we're building

A **species-bucketed** highlights page. One row per species you photographed; within a row, your best shots of that species by quality. Unidentified gets its own section below.

### Data model

What counts as a species for a photo:

1. If the photo has an accepted species keyword (`is_species=1`), use that. Multiple? Use the most recently applied keyword (matches today's tiebreak).
2. Otherwise, take the highest-confidence prediction across all of the photo's detections, **if** confidence ≥ the user's confidence threshold slider.
3. Otherwise, the photo is **Unidentified**.

Accepted tags always win. The confidence slider defaults to **0.70**, range **0.50–0.95**.

Eligibility for the page is unchanged: workspace-scoped, `quality_score IS NOT NULL`, `quality_score >= min_quality`, `flag != 'rejected'`, folder filter respected.

Row sort order (default: **fewest photos first**) is computed over what populates each bucket given the current confidence slider — slide confidence up, ʻApapane's count drops, the row reorders. Unidentified is not part of the sort; it is pinned to its own section below.

## UI

### Controls bar

Left to right:

- **Folder** dropdown — unchanged (workspace-wide option still present)
- **Min quality** slider — unchanged (0.00–1.00)
- **Auto-ID confidence** slider — NEW. 0.50–0.95, default 0.70.
- **Per-row** slider — replaces today's "Max/species". 1–20, default 5. Collapsed row size.
- **Sort** dropdown — NEW. Options: *Fewest photos* (default), *Most photos*, *Best photo first*, *Worst photo first*. Last two sort by the row's top `quality_score`.
- **Save as Collection** button — unchanged, pushed right.

Today's *Count* slider goes away — it does not make sense once the page is species-bucketed.

### Species row layout

```
┌──────────────────────────────────────────────────┐
│  ʻApapane                 (42 photos · best 0.77)│
│  ┌───┐ ┌───┐ ┌───┐ ┌───┐ ┌───┐                   │
│  │ 1 │ │ 2 │ │ 3 │ │ 4 │ │ 5 │   + 37 more  →   │
│  └───┘ └───┘ └───┘ └───┘ └───┘                   │
└──────────────────────────────────────────────────┘
```

Header shows species name, total photos in the bucket *given current filters*, and the best `quality_score` in the row (transparency: the user can see why a row ranks where it does). Click a thumbnail → lightbox with the **full bucket** as the navigation set (not just the visible 5). Click "+N more" → row expands inline to show the rest.

Empty buckets (no photos given current filters) are hidden.

### Unidentified section

Sits below all species rows with a distinct divider and header:

> Unidentified — Vireo couldn't ID these

Same row layout, same expand affordance. Always last, regardless of sort.

## Backend

### Query

`get_highlights_candidates` keeps its existing accepted-species LEFT JOIN and adds a second LEFT JOIN to surface, per photo, the highest-confidence prediction across all of its detections:

```sql
LEFT JOIN (
    SELECT photo_id, species, confidence FROM (
        SELECT d.photo_id, pr.species, pr.confidence,
               ROW_NUMBER() OVER (
                   PARTITION BY d.photo_id
                   ORDER BY pr.confidence DESC, pr.id DESC
               ) AS rn
        FROM detections d
        JOIN predictions pr ON pr.detection_id = d.id
    ) WHERE rn = 1
) tp ON tp.photo_id = p.id
```

The effective-species resolution lives in Python at the API layer, not in SQL — clearer, and easy to evolve.

### API shape (`GET /api/highlights`)

```json
{
  "buckets": [
    {
      "species": "ʻApapane",
      "is_accepted": false,
      "photo_count": 42,
      "best_quality": 0.77,
      "photos": [
        { "id": 123, "filename": "DSC_0001.NEF",
          "quality_score": 0.77, "has_accepted_species": false },
        ...
      ]
    }
  ],
  "unidentified": {
    "photo_count": 5223,
    "photos": [ ... ]
  },
  "folders": [ ... ],
  "meta": { "total_in_workspace": 7138, "eligible": 4655 }
}
```

Each bucket returns **all** its photos (sorted by `quality_score` desc), not just top-N. Hawaii worst case ~5k photos → ~1MB JSON, acceptable, and it makes expand and lightbox work without extra requests. If this becomes slow on larger workspaces later, paginate then.

Sort is **client-side** — all bucket metadata is already in the response, sorting in JS is instant.

### Query params

- `folder_id` / `scope=workspace` — unchanged
- `min_quality` — unchanged
- `confidence_threshold` — NEW, default 0.70

`/api/highlights/save` — unchanged.

## Edge cases

- **Accepted species + conflicting high-confidence prediction** → accepted wins.
- **Multiple detections on one photo** → single highest-confidence prediction across them. One photo, one bucket. (Two-buckets-per-photo is a later feature, if ever.)
- **Confidence slider change makes a bucket vanish** → row disappears on next load. No transitions.
- **Confidence threshold so high no predictions admit** → only accepted species drive buckets; Unidentified section still renders.
- **No predictions at all** → behaves like today (accepted-only).

## Tests

Update in `vireo/tests/test_app.py` and `vireo/tests/test_db.py`:

- `get_highlights_candidates` returns `predicted_species` and `predicted_confidence` when present.
- `/api/highlights` returns `buckets` + `unidentified` shape; existing tests adapt.
- Accepted species wins over higher-confidence conflicting prediction.
- Confidence threshold filters predictions into Unidentified.
- Photos with multiple detections pick top-confidence prediction.
- Folder vs workspace scope filters correctly.
- `/api/highlights/save` unchanged — existing tests stay green.

## What dies

- `vireo/highlights.py` (the MMR `select_highlights` function) — no longer used. Delete the module and its `vireo/tests/test_highlights.py`. The MMR helper in `vireo/selection.py` stays; it is used by encounter selection.
- The `count` slider on the page and its corresponding `count` API parameter.

## Migration

None. Solo user app; API response shape changes are fine, the only consumer is the page template.
