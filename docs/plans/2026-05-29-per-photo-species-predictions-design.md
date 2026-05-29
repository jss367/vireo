# Per-photo species predictions in review panels

## Problem

In the pipeline review burst-group UI, the "Species Predictions" panel shows the
same values regardless of which photo is selected. It is an **encounter-level
aggregate** (mean confidence per species/model across all photos in the
encounter), built by `_build_species_predictions()` in `vireo/pipeline.py:653`.
Because the panel sits beside a single selected photo and is titled plainly
"Species Predictions", it reads as if it were *this photo's* predictions — a
black-box / mislabel issue under `CORE_PHILOSOPHY.md` ("No black boxes").

Two render sites have the bug, both passed a single `photo` but rendering
`enc.species_predictions`:

- `buildPipelineMetadataHtml(photo)` — group review modal loupe detail
  (`vireo/templates/pipeline_review.html:4066`)
- `openInspect(photoId)` — standalone inspect panel (same file, `:2615`)

## What we have already

Each per-photo object delivered to the client carries `species_top5`: a list of
`[species, confidence, model]` rows (`pipeline.py:360`, preserved through
`serialize_results` / `_clean_photo`). It is currently unused in the template.
**No backend change is required.**

## Design

Add a per-photo block above the aggregate, and relabel the aggregate so it is
honestly described as a group mean. Order: per-photo first (it is what changes
on click and what you are looking at), group consensus second.

- **"Species Predictions (this photo)"** — built from `photo.species_top5`,
  grouped by species, each species listing its models' confidences inline
  (mirrors the aggregate row layout). Sorted by best confidence descending.
- **"Group consensus — mean across N photos"** — the existing
  `enc.species_predictions`, unchanged data, only relabeled. `N` is
  `enc.photo_count`.

Shared helper `buildSpeciesPredictionsHtml(photo, enc, threshold)` produces both
blocks so the two call sites stay consistent. `threshold` lets `openInspect`
keep its existing confidence-threshold filter (`minConfidence/100`); the group
review modal passes `0` (no filter, matching its current behavior).

Reuse existing CSS (`.inspect-species-predictions`, `.inspect-species-row`,
`.inspect-species-name`, `.inspect-species-conf`) — no new styles.

Empty cases: if a photo has no `species_top5`, the per-photo block is omitted
(no empty header); the group block still renders. If both are empty, nothing
renders (current behavior).

## Out of scope

- No change to aggregation math or backend endpoints.
- No per-burst vs per-encounter scope change for the aggregate (stays
  encounter-scoped, just labeled honestly).

## Verification

No JS test harness exists for this inline-template JS. Verify by driving the
running app in a browser (per the project's user-first testing approach):
open a burst group, click between photos, confirm the "(this photo)" block
changes while "Group consensus" stays fixed. Run the Python suite as a
regression sanity check (it does not cover this JS).
