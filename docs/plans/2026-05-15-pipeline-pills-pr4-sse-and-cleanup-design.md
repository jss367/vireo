# Pipeline Pills PR #4 — Live SSE counts + legacy `has_*` cleanup

**Date:** 2026-05-15
**Builds on:** PR #748 (pill formatter + progress bar) and #749 (per-stage outdated flags). The pipeline status makeover is functionally complete after this PR.

---

## Problem

Two small loose ends from the original Phase 2-4 vision still ship value:

**A — Live counts in pills during runs.** The pill says "Running…" while a stage is in flight. The browser already has the data — `current` and `total` arrive on every SSE progress event — it's just not surfaced. Users watching a long classify run see the count in the progress bar text but the pill stays generic.

**C — Legacy `has_detections` / `has_masks` / `has_sharpness` consumers.** The pipeline page had these as a coarse "any prior data exists" signal before PR #745 introduced `/api/pipeline/plan`. The plan endpoint now carries everything those flags fed (per-stage state, counts, completion). The legacy flags still ship from `/api/pipeline/page-init` and feed three minor side-effects in `pipeline.html`: card "complete" badges, initial summary text in `txtDetections` / `txtMasks`, and an auto-expand-extract behavior — all redundant with what the plan-endpoint UI does.

## Scope

**In:**
- Single hook in `_updatePipelineStageUI(p)` that updates the pill text to "Running… X / Y" when the SSE event carries `current`/`total`. Falls back to "Running…" otherwise.
- Delete the three `has_*` keys from `/api/pipeline/page-init`, the `db.get_pipeline_feature_counts()` helper, and every consumer in `pipeline.html`.
- Drop the `txtDetections` / `txtMasks` span elements (they only existed to display the legacy summary).
- Drop the auto-expand-extract behavior in `updateCardStates()` — the pill already conveys "Classify done, Extract pending".

**Out:**
- Provenance line per card ("Last run 2h ago · model X"). Deferred — would require new run-at tracking on Eye Keypoints.
- Throttling SSE pill updates. Browser DOM text-content updates are cheap; revisit only if observed.
- Migrating the auto-expand-extract behavior to the plan endpoint. The pill carries the same signal and the auto-expand was a minor convenience.

## Design — Task A

Hook in `_updatePipelineStageUI(p)` inside the per-card `cardAgg` loop, after the existing `_setPill(suffix, 'running')` call. Override the pill text using the **per-stage** `count`/`total` from `stages[stageName]`, NOT the event top-level `p.current`/`p.total`:

```javascript
var subStages = _cardToStages[cardSuffix2] || [];
var stageDone = 0, stageTot = 0;
for (var i = 0; i < subStages.length; i++) {
  var info = stages[subStages[i]] || {};
  if (info.status !== 'running') continue;
  var t = info.total || 0;
  if (t > stageTot) {
    stageTot = t;
    stageDone = (info.count || 0) + (info.cached || 0);
  }
}
if (stageTot > 0) {
  _setPill(cardSuffix2, 'running',
           'Running… ' + stageDone + ' / ' + stageTot);
}
```

Why **not** use `p.current`/`p.total`: those are the WEIGHTED OVERALL pipeline progress (see `pipeline_job._progress_event` and the existing comment around line 2586). Concurrent cards (e.g. scan + thumbnails) would all show the same global number — `Running… 6 / 38` on every running card — instead of their own per-stage progress. The per-stage counts live in `stages[stageName].count` / `.total` and reach the UI via the SSE event's `stages` snapshot.

Why this shape:
- Reuses `_setPill`'s existing `text` override path. No new infrastructure.
- Iterates the card's mapped substages and picks the running one with the highest `total`. Multi-substage cards (Previews ← thumbnails + previews; Classify ← model_loader + classify) end up showing the substage actually doing photo-level work (the model_loader phase has no count, so it falls through to the static label until classify takes over).
- Stages that emit progress without counts (Group's terminal step, model-load spin-up) keep the static "Running…" rather than a misleading "Running… 0 / 0".
- For Classify, the count includes cache hits (`count + cached`) so the pill matches the progress bar's combined `inferred · cached / total` story.
- Format `X / Y` not `X / Y (16%)`. The progress bar already shows percentage; the pill complements without duplicating.

The static "Running…" label in `_formatPillLabel` stays as the fallback for `_setPill('running')` calls without a text override.

## Design — Task C

**Producer side** (`vireo/app.py`):
- Drop 3 lines from the `/api/pipeline/page-init` jsonify block (`app.py:1206-1208`).
- Drop the now-unused `pipeline_counts = db.get_pipeline_feature_counts()` call (`app.py:1166`).
- Delete the helper itself: `db.get_pipeline_feature_counts` in `db.py`. Sole caller is gone.

**Consumer side** (`vireo/templates/pipeline.html`):
- Lines 1141, 1146-1149, 1153-1157, 1184-1185 (initial complete badges + state hydration).
- Lines 2724, 2739, 3142, 3410 (post-job state setters).
- Line 3758 (auto-expand-extract conditional in `updateCardStates`).
- Drop the `_pipelineState.hasDetections` / `_pipelineState.hasMasks` keys from the global state object.
- Delete the `id="txtDetections"` and `id="txtMasks"` span elements (they only existed to display the legacy summary text).

**Tests:**
- `vireo/tests/test_app.py:1068-1070` (test_pipeline_page_init) — drop the 3 `assert 'has_*' in data` lines.
- No other tests reference the dropped fields.

## Implementation phasing

Single PR, 2 commits:

### Commit 1 — Task A: live SSE counts in pills

- ~5 lines of new JS in `pipeline.html`.
- Browser smoke via Playwright (lead-driven): kick a real or simulated SSE event flow, capture pill mid-run.

### Commit 2 — Task C: legacy `has_*` cleanup

- Delete from `app.py` + `db.py` + `pipeline.html` + `test_app.py`.
- Run focused project test suite green.
- Browser smoke: load page on workspace with prior data, confirm pills/bars/plan summary all render correctly without the legacy fields.

## Total scope

- ~5 lines added (Task A).
- ~50-80 lines deleted (Task C).
- One docs commit + 2 implementation commits + 1 browser-smoke commit if needed.
