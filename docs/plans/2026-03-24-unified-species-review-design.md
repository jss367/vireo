# Unified Species Review on Pipeline Page

## Problem

Vireo has two disconnected review surfaces:

- **Review page** — flat grid of species predictions, accept/reject per-photo or per-burst-group. No encounter context. Can correct species only in the burst group modal.
- **Pipeline page** — encounter-based grouping with quality scoring and KEEP/REVIEW/REJECT triage. Shows species in encounter headers but provides no way to correct them.

A user who imports 2000 photos and runs classification + pipeline overnight has to bounce between two pages. Species identity directly affects culling decisions (a rare warbler keeps more shots than a common robin), but the culling view can't correct species.

## Design

### Data Model

No new tables. Clear separation of AI output vs user truth:

- **`predictions` table** — immutable AI output. One row per photo per model. Never modified by user corrections. Multiple models can have predictions for the same photo.
- **`photo_keywords` table** (with `is_species=True`) — user-confirmed ground truth. When a user confirms a species for an encounter, all photos in that encounter get a species keyword added here. This is what gets written to XMP.

### Display Precedence

When rendering species anywhere:

1. **User-confirmed species keyword** (from `photo_keywords`) — shown with a checkmark
2. **AI prediction vote summary** (aggregated from `predictions`) — shown as "Robin ×12 · Blue Jay ×8"

Per-photo cards always show their individual AI predictions regardless of whether a user override exists, so the user can see what each model said.

### Encounter Header

Current header shows: species name, photo count, burst count, confidence, time range.

New layout adds a vote summary line:

```
Blue Jay ✓                    20 photos · 3 bursts · 10:32 - 10:45
Robin ×12 (68%) · Blue Jay ×8 (54%)                    [Confirm species]
```

- **Line 1:** Confirmed species with checkmark (if set), or top AI vote without checkmark. Plus existing metadata.
- **Line 2:** AI vote summary — always visible, even after confirmation. Shows each species that got votes, with count and average confidence.

The vote summary aggregates across all models. The count is per-photo: how many photos got that species as their top prediction from any model.

When no user confirmation exists, clicking the species summary opens the correction panel. When already confirmed, an edit icon allows re-opening.

### Species Correction Panel

Expands below the encounter header when clicked:

```
┌─────────────────────────────────────────────────────┐
│  What species is this?                              │
│                                                     │
│  [Blue Jay  ×8 photos]  [Robin  ×12 photos]        │
│                                                     │
│  Or type: [___________________________] (autocomplete)
│                                                     │
│  [Cancel]                          [Apply to all 20]│
└─────────────────────────────────────────────────────┘
```

- **AI suggestion buttons** — one per species from predictions, sorted by photo count descending. Click selects (highlighted), "Apply to all N" confirms.
- **Autocomplete text field** — searches against loaded label sets. For when neither AI suggestion is correct.
- **Apply to all N** — adds chosen species as keyword (`is_species=True`) to every photo in the encounter. Marks encounter as confirmed.
- **Cancel** — closes without changes.

After applying: header shows "Blue Jay ✓", vote summary stays visible, pending changes queue picks up new keywords for XMP sync.

### Per-Photo Species Display

Each photo card in the encounter adds:

- **Species prediction** — top-1 from AI with confidence, shown below filename. E.g. `Robin 72%`. With multiple models: `Robin 72% (BioCLIP) · Blue Jay 61% (model-B)`.
- **Disagreement highlight** — if a photo's top prediction doesn't match the encounter consensus or user-confirmed species, subtle visual indicator (border or badge). These photos deserve the most attention.

Species correction happens at the encounter level only. Per-photo predictions are evidence for the encounter-level decision.

### API

One new endpoint:

```
POST /api/encounters/<encounter_id>/species
{
  "species": "Blue Jay",
  "photo_ids": [101, 102, ..., 120]
}
```

1. Creates species keyword if needed (`db.add_keyword("Blue Jay", is_species=True)`)
2. Tags all listed photos via `photo_keywords`
3. Queues pending changes for XMP sync
4. Returns updated encounter state

Photo IDs come from the client (which knows the encounter membership). Encounter groupings are ephemeral pipeline output — no need to persist them in the database.

### Pipeline Data Loading

`load_photo_features()` in `pipeline.py` needs an additional query for species keywords:

```sql
SELECT pk.photo_id, k.name
FROM photo_keywords pk
JOIN keywords k ON k.id = pk.keyword_id
WHERE k.is_species = 1
```

Each photo dict gains `"confirmed_species": "Blue Jay"` (or `None`). Each encounter derives `"confirmed_species"` from its photos.

Encounter species resolution:
1. `confirmed_species` if any photo has one
2. Top vote from `species_top5` across all photos otherwise

Vote summary is always computed from predictions regardless.

## Scope

This design covers adding species review to the Pipeline page. It does not remove or modify the existing Review page. Over time, as Pipeline gains more review capabilities, the Review page can be retired.

## Future Considerations (not in scope)

- Single "Jobs" page consolidating import, classify, and pipeline job launchers
- Per-photo species override (for when one encounter contains two different animals)
- Retiring the Review page once Pipeline covers all its functionality
