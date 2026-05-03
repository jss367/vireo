# Pipeline Review Page Fallback Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make the pipeline review page useful when features exist but the cache file doesn't — show a per-stage diagnostic, offer in-page "Compute now" using the existing in-memory pipeline, and surface a banner when results render with degraded inputs.

**Architecture:** The Group stage of the pipeline is purely computational (seconds, no GPU) and reads features already in the DB. `/api/pipeline/regroup-live` already does `load_photo_features` → `run_full_pipeline` → `save_results`. The only gating signal is the per-workspace cache JSON file `pipeline_results_ws{N}.json`. We add a `review_readiness` block to `/api/pipeline/page-init` that classifies the workspace's feature state, replace the static empty state with a diagnostic + Compute-now panel, and add a degraded banner that appears when results render but enhancing features (eye keypoints, embeddings, full mask coverage) are missing for many photos.

**Tech Stack:** Flask + Jinja2 + vanilla JS (no frontend framework). Tests via pytest (`vireo/tests/test_pipeline.py`, `vireo/tests/test_pipeline_api.py`). Manual verification via Playwright (per `feedback_user_first_testing` memory — drive a real browser, don't just read code).

**Background reading (read first):**
- `CORE_PHILOSOPHY.md` — "Show the user what's happening / No black boxes". The diagnostic must answer the question users actually read it as ("can I compute results from what I have?"), not a cheap proxy.
- `vireo/pipeline.py:496-656` — `serialize_results`, `save_results`, `load_results`. The cache file format and where it lives.
- `vireo/pipeline_plan.py` — existing per-stage plan logic (truth source for the `/pipeline` page status pills).
- `vireo/app.py:1162-1220` — current `/api/pipeline/page-init` handler.
- `vireo/app.py:10127-10167` — `/api/pipeline/regroup-live` (already runs full pipeline in-memory and saves cache).
- `vireo/templates/pipeline_review.html:1054-1057, 1809-1849` — current empty state and init logic.

**Out of scope:**
- Auto-computing on page load (could be slow on large libraries; explicit button only).
- Changing the existing `/api/pipeline/regroup-live` endpoint — we just call it.
- Per-stage warnings on the existing `/pipeline` page — that already has its own status logic.

---

### Task 1: Add `compute_review_readiness` to `pipeline.py`

**Files:**
- Modify: `vireo/pipeline.py` (add new function near `load_results`, ~line 660)
- Test: `vireo/tests/test_pipeline.py` (add test near other readiness/results tests)

**Why this lives in `pipeline.py`:** It's about whether the cached pipeline results can be produced from current DB state. `pipeline_plan.py` answers "what would clicking Run do?" (takes UI selections). This answers "can the review page render meaningfully right now?" (no inputs).

**Step 1: Write the failing test**

In `vireo/tests/test_pipeline.py`, add:

```python
def test_compute_review_readiness_empty_workspace(tmp_db):
    """No photos → state='empty'."""
    from pipeline import compute_review_readiness
    db = tmp_db  # fixture providing a Database with active workspace
    out = compute_review_readiness(db)
    assert out["state"] == "empty"
    assert out["total_photos"] == 0


def test_compute_review_readiness_no_masks(tmp_db_with_photos):
    """Photos but no masks → state='insufficient', mask listed in missing_required."""
    from pipeline import compute_review_readiness
    out = compute_review_readiness(tmp_db_with_photos)
    assert out["state"] == "insufficient"
    assert "masks" in out["missing_required"]


def test_compute_review_readiness_masks_present_no_eye(tmp_db_with_masks_no_eye):
    """Masks present, no eye keypoints → state='computable', eye in enhancing_missing."""
    from pipeline import compute_review_readiness
    out = compute_review_readiness(tmp_db_with_masks_no_eye)
    assert out["state"] == "computable"
    assert out["with_masks"] > 0
    assert "eye_keypoints" in out["enhancing_missing"]


def test_compute_review_readiness_full_features(tmp_db_full):
    """All features present, no cache yet → state='computable', enhancing_missing empty."""
    from pipeline import compute_review_readiness
    out = compute_review_readiness(tmp_db_full)
    assert out["state"] == "computable"
    assert out["enhancing_missing"] == []
```

You'll need to look at how existing tests in `test_pipeline.py` build the DB fixture — match that style. Reuse `db.get_coverage_stats()` (`vireo/db.py:2575`) which already counts every feature we care about.

**Step 2: Run tests to verify they fail**

Run: `python -m pytest vireo/tests/test_pipeline.py::test_compute_review_readiness_empty_workspace -v`
Expected: FAIL with `ImportError: cannot import name 'compute_review_readiness'`

**Step 3: Implement `compute_review_readiness`**

In `vireo/pipeline.py`, add:

```python
def compute_review_readiness(db, mask_threshold=0.25):
    """Classify whether the pipeline review page can render meaningful results.

    The Group stage of the pipeline is purely computational and reads
    features already cached in the DB. A "ready" workspace has the
    grouping cache already on disk; a "computable" workspace has enough
    features that calling /api/pipeline/regroup-live would produce a
    useful triage view; an "insufficient" workspace has too few masks
    for the result to be anything but a wall of REJECTs.

    Args:
        db: Database with active workspace
        mask_threshold: minimum fraction of photos that must have masks
            for the result to be computable (default 25%)

    Returns:
        {
            "state": "ready" | "computable" | "insufficient" | "empty",
            "total_photos": int,
            "with_masks": int,
            "with_sharpness": int,
            "with_embeddings": int,
            "with_eye_keypoints": int,
            "with_predictions": int,
            "missing_required": list[str],   # stages that block computation
            "enhancing_missing": list[str],  # stages that would improve quality
        }
    """
    cov = db.get_coverage_stats()
    total = cov["total"]
    out = {
        "state": "empty",
        "total_photos": total,
        "with_masks": cov["mask"],
        "with_sharpness": cov["subject_sharpness"],
        "with_embeddings": cov["dino_embedding"],
        "with_eye_keypoints": cov["eye"],
        "with_predictions": cov["classified"],
        "missing_required": [],
        "enhancing_missing": [],
    }
    if total == 0:
        return out

    # Required: enough photos have masks (otherwise pipeline rejects all)
    if cov["mask"] < max(1, int(total * mask_threshold)):
        out["state"] = "insufficient"
        out["missing_required"].append("masks")
        # Still surface enhancing_missing so the diagnostic is complete
    else:
        out["state"] = "computable"

    # Enhancing: per-stage gaps that would improve quality if filled
    if cov["mask"] < total:
        out["enhancing_missing"].append("masks_partial")
    if cov["dino_embedding"] < total:
        out["enhancing_missing"].append("embeddings")
    if cov["eye"] < total:
        out["enhancing_missing"].append("eye_keypoints")
    if cov["classified"] < total:
        out["enhancing_missing"].append("species_predictions")

    return out
```

**Step 4: Run tests to verify they pass**

Run: `python -m pytest vireo/tests/test_pipeline.py -k review_readiness -v`
Expected: 4 PASS

**Step 5: Commit**

```bash
git add vireo/pipeline.py vireo/tests/test_pipeline.py
git commit -m "pipeline: add compute_review_readiness for review-page fallback"
```

---

### Task 2: Surface readiness in `/api/pipeline/page-init`

**Files:**
- Modify: `vireo/app.py:1162-1220` (the `api_pipeline_page_init` handler)
- Test: `vireo/tests/test_pipeline_api.py` (add a test alongside `test_pipeline_page_init_includes_mask_variant_coverage`)

**Step 1: Write the failing test**

In `vireo/tests/test_pipeline_api.py`, add:

```python
def test_pipeline_page_init_includes_review_readiness(setup):
    """page-init exposes review_readiness so the review page can render
    a diagnostic empty state and decide whether to offer Compute now."""
    app, db_path = setup
    # Reuse _seed_workspace_with_masks or similar — it leaves photos
    # with masks but no Group cache.
    _seed_workspace_with_masks(db_path)

    with app.test_client() as c:
        resp = c.get("/api/pipeline/page-init")
        assert resp.status_code == 200
        data = resp.get_json()
        assert "review_readiness" in data
        rr = data["review_readiness"]
        assert rr["state"] == "computable"
        assert rr["total_photos"] >= 2
        assert rr["with_masks"] >= 2


def test_pipeline_page_init_review_readiness_state_ready_when_cache_exists(setup, tmp_path):
    """When the grouping cache is already on disk, state should be 'ready'."""
    app, db_path = setup
    _seed_workspace_with_masks(db_path)

    # Drop a minimal cache file at <db_dir>/pipeline_results_ws{N}.json
    import os, json
    from db import Database
    db = Database(db_path)
    ws = db._active_workspace_id
    db.close()
    cache_path = os.path.join(os.path.dirname(db_path), f"pipeline_results_ws{ws}.json")
    with open(cache_path, "w") as f:
        json.dump({"encounters": [], "photos": [], "summary": {}}, f)

    with app.test_client() as c:
        resp = c.get("/api/pipeline/page-init")
        data = resp.get_json()
        assert data["review_readiness"]["state"] == "ready"
```

**Step 2: Run to verify failure**

Run: `python -m pytest vireo/tests/test_pipeline_api.py -k review_readiness -v`
Expected: FAIL — `'review_readiness' not in data`

**Step 3: Wire it into the route**

Edit `vireo/app.py:1162-1220`. Inside `api_pipeline_page_init`, after `results = load_results(...)`:

```python
        from pipeline import compute_review_readiness, load_results
        cache_dir = os.path.dirname(db_path)
        results = load_results(cache_dir, db._active_workspace_id)
        # ... existing flag/rating augmentation ...

        review_readiness = compute_review_readiness(db)
        if results is not None:
            # Cache exists — even if features have changed underneath,
            # the page can render. enhancing_missing still reflects the
            # current gap so the degraded banner can surface accurately.
            review_readiness["state"] = "ready"
```

And add `"review_readiness": review_readiness,` to the returned `jsonify` dict.

**Step 4: Run to verify pass**

Run: `python -m pytest vireo/tests/test_pipeline_api.py -k review_readiness -v`
Expected: 2 PASS

**Step 5: Commit**

```bash
git add vireo/app.py vireo/tests/test_pipeline_api.py
git commit -m "pipeline: surface review_readiness in /api/pipeline/page-init"
```

---

### Task 3: Replace empty state with diagnostic + Compute-now panel

**Files:**
- Modify: `vireo/templates/pipeline_review.html:1054-1057` (the `<div class="empty-state">` block)
- Modify: `vireo/templates/pipeline_review.html:1809-1849` (`initPipelineReviewPage`)

**Step 1: Replace the static empty-state HTML**

Replace lines 1054-1057 with a richer placeholder that the JS will populate:

```html
<div class="empty-state" id="emptyState" style="display:none;">
  <h2 id="emptyStateHeading">No pipeline results yet</h2>
  <p id="emptyStateBody"></p>
  <div class="readiness-stages" id="readinessStages"></div>
  <div class="readiness-actions" id="readinessActions"></div>
</div>
```

Add CSS in the `<style>` block (find an analogous block for `.empty-state` and slot these in next to it):

```css
.readiness-stages {
  display: grid;
  grid-template-columns: auto auto;
  gap: 4px 12px;
  margin: 16px auto;
  font-size: 13px;
  max-width: 360px;
  text-align: left;
}
.readiness-stages .stage-name { color: var(--text-dim); }
.readiness-stages .stage-count { font-variant-numeric: tabular-nums; }
.readiness-stages .stage-ok { color: var(--success, #4caf50); }
.readiness-stages .stage-partial { color: var(--warning, #ff9800); }
.readiness-stages .stage-missing { color: var(--text-dim); }
.readiness-actions { margin-top: 16px; display: flex; justify-content: center; gap: 12px; }
.readiness-actions button { padding: 8px 16px; }
.readiness-actions .compute-btn { background: var(--accent); color: var(--bg); border: none; border-radius: 4px; cursor: pointer; }
.readiness-actions .compute-btn:disabled { opacity: 0.5; cursor: not-allowed; }
```

**Step 2: Render the diagnostic from `review_readiness`**

In `pipeline_review.html`, replace the current empty-state branch (around line 1812) with a call to a new `renderEmptyState(readiness)` function. Add it nearby:

```javascript
function renderEmptyState(readiness) {
  var heading = document.getElementById('emptyStateHeading');
  var body = document.getElementById('emptyStateBody');
  var stages = document.getElementById('readinessStages');
  var actions = document.getElementById('readinessActions');
  document.getElementById('emptyState').style.display = '';

  // Per-stage rows
  var rows = [
    ['Photos', readiness.total_photos, readiness.total_photos],
    ['Masks', readiness.with_masks, readiness.total_photos],
    ['Embeddings', readiness.with_embeddings, readiness.total_photos],
    ['Eye keypoints', readiness.with_eye_keypoints, readiness.total_photos],
    ['Species predictions', readiness.with_predictions, readiness.total_photos],
  ];
  stages.innerHTML = rows.map(function(r) {
    var name = r[0], n = r[1], total = r[2];
    var cls = (n === total && total > 0) ? 'stage-ok'
            : (n > 0 ? 'stage-partial' : 'stage-missing');
    return '<span class="stage-name">' + name + '</span>'
         + '<span class="stage-count ' + cls + '">' + n + ' / ' + total + '</span>';
  }).join('');

  if (readiness.state === 'empty') {
    heading.textContent = 'No photos in this workspace';
    body.textContent = 'Add folders to this workspace from the Folders page.';
    actions.innerHTML = '';
    return;
  }

  if (readiness.state === 'insufficient') {
    heading.textContent = 'Not enough features to compute results yet';
    body.textContent = 'Run mask extraction on the Pipeline page first '
      + '— the review page needs masks to score photo quality.';
    actions.innerHTML = '<a href="/pipeline" class="compute-btn" '
      + 'style="text-decoration:none;display:inline-block;">Open Pipeline</a>';
    return;
  }

  // state === 'computable'
  heading.textContent = 'Ready to compute results';
  var enhancing = readiness.enhancing_missing || [];
  if (enhancing.length === 0) {
    body.textContent = 'All upstream stages are complete. '
      + 'Click below to group, score, and triage your photos.';
  } else {
    var labels = enhancing.map(function(k) {
      return ({masks_partial: 'full mask coverage',
               embeddings: 'embeddings',
               eye_keypoints: 'eye keypoints',
               species_predictions: 'species predictions'})[k] || k;
    });
    body.textContent = 'You can compute results now. Quality will be lower without: '
      + labels.join(', ') + '. Re-run those stages on the Pipeline page later to improve.';
  }
  actions.innerHTML = '<button class="compute-btn" onclick="computeReviewNow()">'
    + 'Compute results now</button>';
}

function computeReviewNow() {
  var btn = document.querySelector('.readiness-actions .compute-btn');
  if (btn) { btn.disabled = true; btn.textContent = 'Computing…'; }
  safeFetch('/api/pipeline/regroup-live', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({})
  }).then(function(data) {
    if (!data || !data.encounters) {
      if (btn) { btn.disabled = false; btn.textContent = 'Compute results now'; }
      return;
    }
    // Hide empty state, render results inline
    document.getElementById('emptyState').style.display = 'none';
    pipelineResults = data;
    document.getElementById('summaryBar').style.display = '';
    updateSummaryBar(data.summary);
    document.getElementById('filterBar').style.display = '';
    document.getElementById('sidebarScoring').style.display = '';
    document.getElementById('sidebarGrouping').style.display = '';
    renderResults();
    refreshMissesReviewBtn();
  });
}
```

**Step 3: Update `initPipelineReviewPage`**

Replace lines 1812-1817 with:

```javascript
    if (!data || !data.results) {
      renderEmptyState(data && data.review_readiness ? data.review_readiness
                                                     : {state: 'empty', total_photos: 0});
      return;
    }
```

**Step 4: Manual verification (Playwright per user-first-testing memory)**

Drive a real browser:

1. Pick a workspace where the cache JSON does not exist but masks/embeddings do (or temporarily delete `~/.vireo/pipeline_results_ws{N}.json`).
2. Visit `/pipeline/review`. Expect: per-stage diagnostic + "Compute results now" button.
3. Click "Compute results now". Expect: button shows "Computing…", then results render inline within seconds (no page reload).
4. Pick a workspace with no masks. Expect: "Not enough features…" with a link to `/pipeline`.

Document what you saw — screenshots in PR description.

**Step 5: Commit**

```bash
git add vireo/templates/pipeline_review.html
git commit -m "pipeline: diagnostic empty state + compute-now button on review page"
```

---

### Task 4: Degraded-features banner when results render with gaps

**Files:**
- Modify: `vireo/templates/pipeline_review.html` (add banner element + render call)

**Step 1: Add the banner element**

In `pipeline_review.html`, add a banner near the top of the main content area (above `summaryBar`):

```html
<div class="degraded-banner" id="degradedBanner" style="display:none;">
  <span id="degradedBannerText"></span>
  <a href="/pipeline" class="degraded-banner-link">Open Pipeline</a>
  <button class="degraded-banner-close" onclick="dismissDegradedBanner()" aria-label="Dismiss">&times;</button>
</div>
```

CSS:

```css
.degraded-banner {
  background: var(--warning-bg, #3a2e1f);
  color: var(--text);
  padding: 8px 16px;
  border-bottom: 1px solid var(--border);
  font-size: 13px;
  display: flex;
  align-items: center;
  gap: 12px;
}
.degraded-banner-link {
  color: var(--accent);
  margin-left: auto;
}
.degraded-banner-close {
  background: transparent;
  border: none;
  color: var(--text-dim);
  font-size: 18px;
  cursor: pointer;
  padding: 0 4px;
}
```

**Step 2: Render the banner from `review_readiness.enhancing_missing`**

In `initPipelineReviewPage`, after the success branch (after `renderResults()`), add:

```javascript
    if (data.review_readiness && data.review_readiness.enhancing_missing
        && data.review_readiness.enhancing_missing.length > 0
        && !sessionStorage.getItem('degradedBannerDismissed')) {
      var labels = data.review_readiness.enhancing_missing.map(function(k) {
        return ({masks_partial: 'full mask coverage',
                 embeddings: 'embeddings',
                 eye_keypoints: 'eye keypoints',
                 species_predictions: 'species predictions'})[k] || k;
      });
      document.getElementById('degradedBannerText').textContent =
        'These results were computed without ' + labels.join(', ')
        + '. Re-run those stages to improve quality.';
      document.getElementById('degradedBanner').style.display = '';
    }
```

And the dismiss handler:

```javascript
function dismissDegradedBanner() {
  sessionStorage.setItem('degradedBannerDismissed', '1');
  document.getElementById('degradedBanner').style.display = 'none';
}
```

Session-scoped dismissal (not localStorage) — so it re-appears next session. Users should see it again if they come back to the page.

**Step 3: Manual verification**

1. With a workspace that has results cached but eye keypoints disabled or missing for some photos, visit `/pipeline/review`. Expect: banner shows at top with the missing-features list and a link to `/pipeline`.
2. Click dismiss. Banner disappears. Reload page. Banner stays gone for the session.
3. Open a fresh tab. Banner re-appears.

**Step 4: Commit**

```bash
git add vireo/templates/pipeline_review.html
git commit -m "pipeline: degraded-features banner on review page"
```

---

### Task 5: Run full test suite + open PR

**Step 1: Run the project test suite**

Per `CLAUDE.md`:

```bash
python -m pytest tests/test_workspaces.py vireo/tests/test_db.py vireo/tests/test_app.py vireo/tests/test_photos_api.py vireo/tests/test_edits_api.py vireo/tests/test_jobs_api.py vireo/tests/test_darktable_api.py vireo/tests/test_config.py vireo/tests/test_pipeline.py vireo/tests/test_pipeline_api.py -v
```

Expected: all PASS except known-failing tests on main (see `project_preexisting_test_failures` memory). Note any unexpected failures and investigate before opening PR.

**Step 2: Push and open PR**

```bash
git push -u origin claude/<branch>
gh pr create --base main --title "pipeline review: diagnostic empty state + compute-now fallback" --body "$(cat <<'EOF'
## Summary

The pipeline review page used to bail to "No pipeline results yet" whenever the per-workspace cache JSON was absent — even when all upstream features were ready and the Group stage would have run in seconds. This PR makes the page useful in that state.

- New `compute_review_readiness` classifies the workspace as `ready` / `computable` / `insufficient` / `empty` based on what's in the DB.
- `/api/pipeline/page-init` now returns `review_readiness`.
- The empty state shows a per-stage diagnostic and (when computable) a "Compute results now" button that calls the existing `/api/pipeline/regroup-live` endpoint and renders inline.
- A dismissible banner appears when results render but enhancing inputs (eye keypoints, embeddings, full mask coverage, species predictions) are missing for some photos, so the user knows what would improve quality.

Eye keypoints, embeddings, and species predictions are all enhancing — the Group stage degrades gracefully without them. Only masks gate computation (without masks the result is a wall of REJECTs).

## Test plan
- [ ] Backend tests: `vireo/tests/test_pipeline.py::test_compute_review_readiness_*`
- [ ] Backend tests: `vireo/tests/test_pipeline_api.py::test_pipeline_page_init_includes_review_readiness`, `test_pipeline_page_init_review_readiness_state_ready_when_cache_exists`
- [ ] Manual: workspace with masks but no cache → diagnostic + compute button
- [ ] Manual: click compute → results render inline in seconds
- [ ] Manual: workspace without masks → "Not enough features" + Pipeline link
- [ ] Manual: results with enhancing gaps → degraded banner shows + dismisses
EOF
)"
```

---

## Verification checklist before merge

- [ ] `compute_review_readiness` correctly distinguishes empty / insufficient / computable / ready
- [ ] `/api/pipeline/page-init` includes `review_readiness` in all responses
- [ ] Empty state shows per-stage counts and a working Compute-now button
- [ ] Compute-now actually populates the cache (verify file appears at `~/.vireo/pipeline_results_ws{N}.json`)
- [ ] Degraded banner appears when expected and dismisses correctly
- [ ] No regression in the existing happy-path (cache exists → page renders normally)
- [ ] Note: `docs/plans/` is gitignored — commit this plan with `git add -f docs/plans/2026-05-03-pipeline-review-fallback.md` per `project_plan_docs_force_add` memory
