# Classification Inventory — Design

**Date:** 2026-05-06
**Status:** Design validated, ready for implementation plan
**Owner:** Julius

## Problem

Vireo today shows only one workspace-level classification stat: the dashboard's "X of Y photos classified" bar. This metric is per-photo and counts a photo as classified if *any* detection on it has a prediction from *any* model with *any* label set. It hides three things users need:

1. **Which (model × label-set) pairs have actually been run** on this workspace.
2. **Per-detection coverage**, which is what the classify job actually processes — a photo with five birds counts once on the dashboard but contributes five units of work.
3. **Stale predictions** left over after a label set is edited (different fingerprint).

This makes the Pipeline page's "X to classify" pending count surprising — users assume the dashboard's 67% means they're 67% done, but for a specific (model, label-set) combo they may be at 0%.

## Goal

Add a detailed Classification Inventory panel to the dashboard that shows, per (model × label-set) pair, exactly what has been classified, what is pending, when, with what apparent confidence, and whether there are stale leftovers from past label-set edits. Make every row a launch pad to the action that fills the gap (run classify) or inspects the result (review).

## Non-goals

- Not a replacement for the existing 67% summary; this sits alongside it.
- Not a leaderboard or per-species breakdown. Species-level analysis belongs on the Review page.
- Not retrospective analytics over time. Snapshot only.
- Not a full audit (precise medians, etc.) — sampling is acceptable for the confidence column.

## Source-of-truth definitions

- **Available models:** the list returned by `/api/models/pipeline` (same source as the Pipeline page's model selector). Models found in `classifier_runs` but not in this list appear under an "Other (legacy)" group.
- **Available label sets:** `.txt` files in `~/.vireo/labels/`, plus a synthetic "Tree of Life" entry exposed only on tol-supported models (`bioclip-2`, etc., per `vireo/classify_job.py:144`).
- **Real detection:** `detector_model != 'full-image' AND detector_confidence >= effective_min_conf` (matches `count_classify_pending_pairs` semantics).
- **Workspace scoping:** detections in photos whose folder is in `workspace_folders` for the active workspace.
- **Pair identity:** `(classifier_model, labels_fingerprint)`. For Tree of Life rows, fingerprint is the literal string `tol`.
- **Stale row:** a `(model, fingerprint)` group present in `classifier_runs` whose fingerprint does not match any current `.txt` file's fingerprint and is not the literal `tol`.

## API

**New endpoint:** `GET /api/workspace/classification-inventory`

**Response shape:**

```json
{
  "workspace_id": 5,
  "workspace_name": "USA2026",
  "min_conf": 0.2,
  "total_real_detections": 80803,
  "total_photos": 22709,
  "models": [
    {
      "id": "bioclip-2.5",
      "name": "BioCLIP-2.5",
      "supports_tol": true,
      "legacy": false,
      "subtotal": {
        "classified_dets": 45481,
        "pending_dets": 196928,
        "coverage_pct": 18.8
      },
      "pairs": [
        {
          "label_set": "california-us-birds",
          "label_set_path": "california-us-birds-research-grade.txt",
          "fingerprint": "dad900c8a412",
          "is_tol": false,
          "status": "partial",
          "classified_dets": 22738,
          "pending_dets": 58065,
          "coverage_pct": 28.1,
          "photos_covered": 9412,
          "last_run": "2026-04-22T15:02:11Z",
          "median_top1_conf": 0.78,
          "median_sample_size": 2000,
          "stale_count": 0
        }
      ]
    }
  ],
  "stale": [
    {
      "model": "BioCLIP-2.5",
      "fingerprint": "abc123def456",
      "stale_count": 4521,
      "last_run": "2026-03-10T08:14:22Z"
    }
  ],
  "grand_total": {
    "classified_dets": 68561,
    "pending_dets": 565942,
    "coverage_pct": 10.8,
    "total_predictions_rows": 71203
  }
}
```

Notes:

- `coverage_pct` = `classified / (classified + pending)`. The denominator equals `total_real_detections` for non-stale pairs.
- `status` is one of `complete` (coverage_pct == 100), `partial` (>0 and <100), `never_run` (0), or `stale` (any stale_count > 0 on this row's pair).
- `median_top1_conf` is `null` when `classified_dets < 100`; the frontend dims the column.
- `total_predictions_rows` answers the user's original question — "how many classifications do I have?" — directly.

## Query strategy

Single SQL pass for the bulk of the data, scoped through a CTE:

```sql
WITH ws_dets AS (
  SELECT d.id AS did, d.photo_id AS pid
  FROM detections d
  JOIN photos p ON p.id = d.photo_id
  JOIN workspace_folders wf
    ON wf.folder_id = p.folder_id AND wf.workspace_id = ?
  WHERE d.detector_model != 'full-image'
    AND d.detector_confidence >= ?
)
SELECT
  cr.classifier_model,
  cr.labels_fingerprint,
  COUNT(DISTINCT cr.detection_id) AS classified_dets,
  COUNT(DISTINCT ws_dets.pid)     AS photos_covered,
  MAX(cr.created_at)              AS last_run
FROM classifier_runs cr
JOIN ws_dets ON ws_dets.did = cr.detection_id
GROUP BY cr.classifier_model, cr.labels_fingerprint;
```

`total_real_detections` is one scalar (`SELECT COUNT(*) FROM ws_dets`). `pending = total - classified` per pair.

**Median top-1 confidence** uses sampling for performance:

```sql
WITH top1 AS (
  SELECT pr.detection_id, pr.classifier_model, pr.labels_fingerprint, pr.confidence
  FROM predictions pr
  WHERE pr.rank = 1
    AND pr.detection_id IN (SELECT did FROM ws_dets)
  ORDER BY RANDOM()
  LIMIT 2000
)
SELECT classifier_model, labels_fingerprint, confidence
FROM top1;
```

Python computes the median per (model, fingerprint) group from the sample. Total query budget: <80 ms on 80k detections × ~10 pairs.

**Cross-product construction** is done in Python after the SQL pass:

1. Read `/api/models/pipeline` model list.
2. Read `os.listdir(~/.vireo/labels)` for `.txt` files; compute fingerprints.
3. For each model, enumerate its label-set pairs:
   - Closed-set / timm models (no labels needed): single virtual row keyed by an intrinsic fingerprint (e.g. iNat21's `tol`).
   - Tol-supported models: one row per `.txt` plus one Tree of Life row.
   - Other open-vocab models: one row per `.txt`.
4. Merge in counts from the SQL result by `(model, fingerprint)`. Missing pairs get zeros / `never_run`.
5. Identify stale: any `(model, fingerprint)` in the SQL result not matched by step 3.

## UI

**Placement:** new full-width section below the existing dashboard card grid in `vireo/templates/stats.html`, with header `<h2>Classification inventory</h2>` and a `Refresh` button.

**Table layout** (8 columns + actions, grouped by model):

| Column | Notes |
|---|---|
| Status | Pill: green `Complete`, yellow `Partial`, grey `Never run`, orange `Stale`. |
| Label set | Filename minus `.txt`; `Tree of Life` for tol rows. |
| Detections classified | `toLocaleString()` |
| Pending | `toLocaleString()` |
| Coverage | "%`.`% + thin inline bar |
| Photos | `toLocaleString()` |
| Last run | Relative time ("3 days ago") with absolute on hover |
| Median top-1 | Two decimals, dimmed when sample < 100, "—" when classified = 0 |
| Actions | ▶ Run icon (Never run / Partial / Stale rows) → `/pipeline?models=<id>&labels=<file>`. 🔍 Inspect icon (classified > 0) → `/review?model=<id>&labels_fingerprint=<fp>` |

**Per-model header:**

```
BioCLIP-2.5 — 45,481 / 242,409 detections classified across 3 label sets    [▾]
```

Chevron collapses the model's pairs. Default: expanded.

**Sort within a model:** alphabetical by label set, with "Tree of Life" pinned last.

**Grand total row** at the bottom of the main table, visually separated by a thicker top border, includes `total_predictions_rows`.

**Stale section** below the main table: collapsed by default, header reads `Outdated label-set fingerprints (N rows)`. Expanded view shows model, fingerprint prefix, stale_count, last_run, and "Reclassify with current labels" action that pre-selects the model + the current `.txt` whose name matches the legacy fingerprint's likely intent (best-effort; if no match, just opens pipeline with model preselected).

**Empty / edge states:**

- No models registered → "No classification models installed." with link to Settings.
- 0 detections → "Run subject detection first to populate this view."
- All pairs Complete and no Stale → small ✓ banner: "All combinations classified."

**Refresh:**

- Manual `Refresh` button.
- Auto-refresh on classify job completion: subscribe to the existing SSE stream that `vireo/templates/_navbar.html` already wires up; on a `job.completed` event whose `job_type == 'classify'`, re-fetch.

## Pipeline pre-selection

`/pipeline?models=<id>&labels=<file>` — pipeline.html currently does not read query params for these. Added behavior:

- On load, parse `URLSearchParams` for `models` (CSV) and `labels` (CSV).
- If present, override the workspace's stored selection in the form state and trigger the existing "estimate" recompute (`/api/pipeline/plan`).
- Do not persist this override unless the user actually clicks Run; query-param-driven state is transient.

Affects `vireo/templates/pipeline.html` only — small JS init block.

## Review pre-filtering

`/review?model=<id>&labels_fingerprint=<fp>` — review.html already supports a model filter (`vireo/templates/review.html:762-805`). Add `labels_fingerprint` query param parsing the same way. If the existing filter dropdown does not surface fingerprint, the URL still applies the filter under the hood; we add a small "Filtered to fingerprint <prefix>" pill above the cards with a clear-filter X.

## Tests

`vireo/tests/test_classification_inventory.py` (new):

1. Empty workspace: empty `models`, empty `stale`, zeroed `grand_total`.
2. Workspace with detections but zero predictions: every available pair shows `Never run`, pending = total.
3. Single (model, label-set) fully classified: status `complete`, pending 0, photos_covered ≤ workspace photo count.
4. Two models × two label-sets: cross-product produces 4 pair rows; subtotals sum.
5. Tol pair appears only on tol-supported models.
6. Stale fingerprint: predictions on a fingerprint not on disk → row in `stale`; current-fingerprint pair shows pending = total.
7. Detection below `min_conf` excluded from numerator and denominator.
8. Workspace scoping: prediction on a detection in another workspace's folder not counted.
9. Median sampling: 100 predictions all conf 0.9 → median ≈ 0.9 ± 0.02.
10. `photos_covered` counts a photo once even with multiple classified detections.
11. Legacy model (in `classifier_runs` but not in `/api/models/pipeline`): grouped under "Other (legacy)".

Frontend: manual checks for now (Playwright TODO), covering placement, collapse/expand, action links, refresh-on-job-completion, dark+light themes.

## Edge cases handled in code

- Workspace with `min_conf` overridden via per-workspace config: use `db.get_effective_config(cfg.load())`.
- Label set deleted from disk after past run: no current-fingerprint row, predictions show as stale.
- Two label sets with the same content (same fingerprint): displayed once under whichever filename sorts first; surfaced as a soft warning ("2 files with identical content: <a>, <b>") in the inventory header.
- `tol` fingerprint string treated as never-stale even if no tol-supported model exists currently (defensive).

## Performance budget

- Endpoint target: < 200 ms p95 on a workspace with 100k detections and 12 pairs.
- Single CTE-scoped aggregation query.
- One sampling query for medians, capped at 2000 rows.
- One scalar `count_photos()` reuse for `total_photos`.
- No new indices needed; existing `classifier_runs(detection_id, classifier_model, labels_fingerprint)` index covers the join.

## Rollout

- Single PR against `main` containing endpoint, query, frontend section, and backend tests.
- No DB migration.
- No feature flag.
- Branch: `classify-pipeline-question` (already cut).
