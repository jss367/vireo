# Pipeline classify progress counter — design

## Problem

The pipeline classify stage shows `Classifying species: 1,056 / 24,647` — a single counter that ticks for every photo regardless of whether the classifier actually ran. Cache hits (photos with an existing `classifier_runs` row matching the active model + label fingerprint) skip inference entirely, but advance the same counter. This makes a half-cached run look as slow as a full run during the early minutes, before cache hits start arriving in bulk, and produces misleading ETAs from the rate calculation.

In the run that motivated this design (USA2026 workspace, 24,647 photos, BioCLIP‑2.5 / fingerprint `916d13ffb4a2`):

- 13,421 photos already had cached predictions for the active model + fingerprint (54%).
- 11,226 photos required fresh inference (46%).
- The counter advanced for both kinds at the same rate, so the user could not tell that more than half of the work was a fast cache lookup.

## Goal

Surface inferred vs cached photo counts in the live progress UI for the classify stage, and add a one-shot pre-flight estimate so the ETA is honest from the first second of the run.

## Non-goals

- No changes to other stages. Only the `classify` stage's progress payload populates the new fields. UI handles the new fields generically so future stages can adopt them, but no retroactive change.
- No split rates ("inference rate" + "cache hit rate"). Current single rate is good enough.
- No change to `reclassify=True` semantics. When the gate is bypassed, `cached` is naturally 0.
- No per-photo cache-hit log lines. The aggregate counter is what matters.

## Architecture

The progress event already flows from `pipeline_job.py` → `JobRunner` (SSE stream) → pipeline page JS. We add two optional integer fields to the per-stage payload:

- `cached` — count of photos that hit the cache and skipped inference.
- `cached_estimate` — pre-flight estimate of cache hits, set once at stage start.

```
classify loop (pipeline_job.py)
  ├─ on cache hit:   stages["classify"]["cached"] += 1
  └─ on inference:   stages["classify"]["count"] += 1

stage state shape (back-compat — both new fields optional):
  {
    status: "running",
    count: 718,                # photos that ran inference (semantic change)
    cached: 338,               # photos that hit the cache (NEW)
    total: 24647,              # total photos in the run
    cached_estimate: 13421,    # pre-flight estimate (NEW, set once)
    label: "Classifying species",
  }

UI rule (pipeline.html):
  if cached_estimate is set, show pre-flight banner once at stage start
  if cached > 0, render "718 inferred · 338 cached / 24,647"
  else, render existing "718 / 24,647"
  bar fill uses (count + cached) / total so completion percent is honest
```

### Semantic change to `count`

Today `count` represents "photos seen" — pre-advanced by the full batch length at batch start (`pipeline_job.py:1885`). After this change, `count` represents "photos that ran inference" only, and `cached` covers the skipped photos. The sum `count + cached` equals the old "photos seen" semantics, so the progress bar percentage stays correct.

### `cached_estimate` is a hint, not a guarantee

The estimate counts detections with a `classifier_runs` row matching the active `(model, fingerprint)`. The classify loop's gate also checks that `get_predictions_for_detection` returns rows; if a run-key exists with no cached predictions (e.g. a prior pass stored only `category == 'match'`), it falls through to inference (`pipeline_job.py:2000-2004`). So actual `cached` final value can be lower than `cached_estimate`. UI treats the estimate as approximate.

## Backend changes (`vireo/pipeline_job.py`)

### Pre-flight estimate (per spec, before each model's batch loop, ~line 1830)

```python
cached_est = thread_db.count_classifier_runs(
    photo_ids=[p["id"] for p in photos],
    classifier_model=model_name,
    labels_fingerprint=spec_fp,
)
stages["classify"]["cached_estimate"] = (
    stages["classify"].get("cached_estimate", 0) + cached_est
)
runner.push_event(job["id"], "progress", _progress_event(
    stages, "classify", phase_label, step_id=step_id,
))
```

Cumulative across multiple specs in a multi-model run, mirroring how `count` and `total` already span `[0, total * len(resolved_specs)]`.

### Per-photo counter increments

Replace the per-batch pre-advance at line 1885. After the cache-skip block at line 1999, increment `stages["classify"]["cached"]` and continue. After successful inference at line 2044, increment `stages["classify"]["count"]`. Both counters span `[0, total * len(resolved_specs)]`.

### Progress event cadence

Keep one event per batch (~750 events for 24k photos at batch size 32). Push at batch *end* now instead of batch start, with both fields populated.

### Mid-batch cancel cleanup

The corrective fixup at lines 2059-2064 goes away. `count` is per-photo accurate now, so the cancel path just pushes final state and updates the step.

## Database change (`vireo/db.py`)

New method:

```python
def count_classifier_runs(
    self,
    photo_ids: list[int],
    classifier_model: str,
    labels_fingerprint: str,
) -> int:
    """Count distinct photos in `photo_ids` that have at least one
    detection with a classifier_runs row matching (model, fingerprint).
    """
```

Joins `detections` + `classifier_runs`, filtered by `photo_id IN (...)` and the model + fingerprint pair. Chunks `photo_ids` to stay under SQLITE_MAX_VARIABLE_NUMBER (999).

## Frontend changes (`vireo/templates/pipeline.html`, around line 2293)

```javascript
var si = stages[p.stage_id] || {};
var stageCurrent = si.count || 0;
var stageCached = si.cached || 0;
var stageCachedEst = si.cached_estimate || 0;
var stageTotal = si.total || 0;

var parts = [];
if (p.phase) parts.push(p.phase);

if (stageTotal > 0 && (stageCurrent > 0 || stageCached > 0)) {
  if (stageCached > 0) {
    parts.push(
      stageCurrent.toLocaleString() + ' inferred · ' +
      stageCached.toLocaleString() + ' cached / ' +
      stageTotal.toLocaleString()
    );
  } else {
    parts.push(stageCurrent.toLocaleString() + ' / ' + stageTotal.toLocaleString());
  }
}

if (stageCachedEst > 0 && stageCurrent === 0 && stageCached === 0) {
  parts.push(
    '~' + stageCachedEst.toLocaleString() + ' cached, ~' +
    (stageTotal - stageCachedEst).toLocaleString() + ' to classify'
  );
}

// Bar fill must reflect both inferred AND cached photos so the bar
// tracks honest completion, not just inferences.
var pct = Math.round((stageCurrent + stageCached) / stageTotal * 100);
```

Other stages render unchanged: they don't set `cached`, so the new branches are never taken.

## Tests

### `test_db.py::test_count_classifier_runs_filters_by_model_and_fingerprint`

3 photos, 1 detection each. Insert `classifier_runs` rows for:
- detection 1 → (BioCLIP-2.5, `fp-a`)
- detection 2 → (BioCLIP-2.5, `fp-b`)
- detection 3 → (iNat21, `fp-a`)

Assert `count_classifier_runs([1,2,3], "BioCLIP-2.5", "fp-a") == 1`. Also test the empty case returns 0 and the "photo with multiple detections, only one cached" case still counts as 1 photo.

### `test_db.py::test_count_classifier_runs_chunks_large_id_lists`

1500 photos with cached runs. Assert the count returns 1500, verifying chunking works above SQLITE_MAX_VARIABLE_NUMBER.

### `test_pipeline_job.py::test_classify_progress_reports_cached_separately`

Use existing pipeline test scaffolding (collection mode, monkeypatched classifier). 4 photos, 2 with pre-existing `classifier_runs` rows for the test model+fingerprint, 2 without. Run the pipeline classify stage and inspect captured progress events:

- An early event has `stages["classify"]["cached_estimate"] == 2`.
- The final classify event has `stages["classify"]["cached"] == 2` and `stages["classify"]["count"] == 2`.
- `count + cached == total`.

### Manual verification

Drive the running browser. Start a small classify job in a workspace with mixed cached/uncached photos. Confirm UI shows "X inferred · Y cached / Total" and the pre-flight banner appears at stage start.
