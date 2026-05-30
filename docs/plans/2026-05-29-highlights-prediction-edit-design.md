# Highlights: confirm & change predictions

**Date:** 2026-05-29
**Branch:** `highlights-page-prediction-edit`

## Problem

The Highlights page groups photos into species "buckets" — an accepted species
keyword wins, otherwise the top prediction above the confidence slider, with an
"Unidentified" section for the rest. The page is **read-only**: if Vireo predicts
"Bald Eagle" but the bird is a House Sparrow, there is no way to tell Vireo it's
wrong, or to confirm a prediction that's right. Confirm/change machinery exists
elsewhere (rapid-review, the predictions flow, `/api/predictions/<id>/accept|reject|replace-keywords`,
`/api/encounters/species`) but Highlights surfaces none of it.

## Goals

- Confirm or change a prediction directly from Highlights.
- Operate at the **row level** (the whole bucket, when it's right or uniformly
  wrong) and at the **per-photo level** (the lightbox, for mixed rows).
- Write the **real species keyword** so changes propagate everywhere (Browse,
  Keywords, export) — same machinery as the rest of the app. This satisfies
  `CORE_PHILOSOPHY.md` "No black boxes": a "Confirmed" badge means the keyword
  is actually written.

## Decisions (from brainstorming)

1. **Scope:** both — row-level by default, per-photo drill-down for mixed rows.
2. **Change input:** text field with autocomplete from known species; typing a
   brand-new name is allowed.
3. **Behavior:** identical to accepting/correcting a prediction elsewhere — it
   writes the accepted keyword; a changed photo gets the new keyword and the
   wrong prediction is rejected.
4. **Confirm scope:** confirming a row applies to **every** photo in the row,
   visible or not (not just the thumbnails currently shown).
5. **Per-photo surface:** the **lightbox**, not inline card controls — keeps the
   grid scannable and is where pixel-peeping already happens.

## UX surfaces

### Row-level (bucket header)

- **Predicted rows** (grey "Predicted" badge): **Confirm** + **Change**.
  - *Confirm* accepts the prediction for all photos in the row.
  - *Change* opens a species autocomplete; submitting relabels every photo in
    the row to the new species and rejects the wrong prediction.
- **Confirmed rows**: no Confirm (no-op); still offer **Change** to fix an
  earlier wrong confirmation.
- **Unidentified section**: **Set species** (a relabel with nothing to reject).

### Per-photo (lightbox)

Opening a Highlights thumbnail shows the photo's current species — either
"Predicted: Bald Eagle (87%)" or "Confirmed: House Sparrow" — with **Confirm**
(only for an unconfirmed prediction) and **Change** (autocomplete) controls.
This is the mixed-row fix path: zoom in, confirm it's a sparrow, relabel that one.

After any action the page reloads `/api/highlights` for the current scope so
buckets and badges reflect the new truth immediately.

## Data flow & endpoints

### `/api/highlights` gains prediction IDs

`get_highlights_candidates` already resolves each photo's top non-rejected
prediction. Surface that prediction's `id` so each photo object in the API
response carries `prediction_id` (nullable) and `predicted_confidence`. The
frontend uses these for the lightbox display and to know what's confirmable.

### `POST /api/highlights/confirm`

Body: `{ "photo_ids": [...] }`.

For each photo, **re-resolve its top non-rejected prediction server-side** (same
query as the candidates resolver, so a stale client can't confirm the wrong
thing) and run the existing `db.accept_prediction(pred_id)`. That already marks
the prediction accepted, rejects siblings, adds the species keyword, queues
sidecar sync, and records an edit.

- Dedup by prediction/group so a shared grouped prediction is processed once.
- Photos with no pending prediction are skipped (no-op success).

Returns a per-photo summary.

### `POST /api/highlights/relabel`

Body: `{ "photo_ids": [...], "species": "House Sparrow" }`.

In **one transaction**, for each photo:

1. Reject its current top prediction if any (`update_prediction_status(..., 'rejected')`)
   so it won't resurface.
2. Strip existing species keywords (`untag_photo` + `_queue_keyword_remove`).
3. `add_keyword(species, is_species=True)` + `tag_photo` + `_queue_keyword_add`.

Record one `species_replace` edit. This is the exact write path
`/api/encounters/species` uses, minus the pipeline-cache bookkeeping (Highlights
has none). Rollback on any failure so a mid-loop error can't half-retag a row.

### Species autocomplete

Reuse the existing species-keyword suggestion source (the same data rapid-review's
species input draws from). A brand-new typed name just creates the keyword via
`add_keyword`.

## Edge cases

- **Unidentified / no prediction:** relabel works with nothing to reject; Confirm
  is hidden (nothing to confirm).
- **Grouped predictions:** dedup on confirm; skip already-accepted.
- **Mixed rows** (same name, some confirmed + some predicted): Confirm accepts
  only the still-pending predictions; Change relabels all.
- **No-op changes:** relabel to the current species is an idempotent tag; confirm
  on an already-accepted photo is skipped.
- **Validation:** both endpoints validate `photo_ids` exist and belong to the
  active workspace; `relabel` requires non-empty `species`.
- **Lightbox safety:** the lightbox is shared across pages. Highlights passes its
  controls + callbacks via the existing `options` argument to `openLightbox`;
  other pages pass nothing and see no change.

## Testing

### Backend (pytest, temp DB)

- `/api/highlights` shape: predicted photo carries `prediction_id` +
  `predicted_confidence`; accepted-only photo carries `prediction_id: null`.
- Confirm: prediction → `accepted`, keyword added, siblings rejected, edit recorded.
- Confirm dedup: two photos sharing one grouped prediction → processed once,
  both tagged, no error.
- Confirm skips non-predicted: no-op success, no duplicate keyword.
- Relabel: old keyword stripped, new applied, old prediction rejected,
  `species_replace` edit recorded, sidecar add/remove queued.
- Relabel on unidentified: keyword applied, no error.
- Relabel atomic rollback: injected mid-loop failure leaves no partial retag.
- Validation: unknown `photo_ids` → error; empty `species` → error.
- Bucket migration: after confirm, reload shows photo in an `is_accepted: true`
  bucket; after relabel, under the new species.

### Frontend

Manual verification via the `verify`/`run` skill — drive a real browser
(user-first testing): confirm a row, change a row, fix one photo in the lightbox,
watch buckets update.
