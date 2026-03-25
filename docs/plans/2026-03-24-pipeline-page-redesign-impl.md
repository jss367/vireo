# Pipeline Page Redesign — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Split the Pipeline page into Pipeline (configure & run) and Pipeline Review (inspect & tune results), absorbing the Classify page into Pipeline as Stage 1.

**Architecture:** The current `/pipeline` route becomes the configure/run page with 3 stage cards (Classify, Extract Features, Group & Score). A new `/pipeline/review` route gets the existing results view (encounters, threshold sliders, photo inspection). The `/classify` route is removed. The regroup API and `load_photo_features()` gain optional `collection_id` scoping.

**Tech Stack:** Flask, Jinja2 templates, vanilla JS, SSE for job progress

---

### Task 1: Add collection_id scoping to load_photo_features

**Files:**
- Modify: `vireo/pipeline.py:35-58`
- Test: `vireo/tests/test_pipeline.py`

**Step 1: Write the failing test**

Add to `vireo/tests/test_pipeline.py`:

```python
def test_load_photo_features_collection_scoped(tmp_path):
    """load_photo_features with collection_id returns only collection photos."""
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder(str(tmp_path), name="photos")

    p1 = db.add_photo(fid, "a.jpg", ".jpg", 1000, 1.0, timestamp="2026-01-01T10:00:00")
    p2 = db.add_photo(fid, "b.jpg", ".jpg", 1000, 1.0, timestamp="2026-01-01T11:00:00")

    # Create a collection with only p1
    cid = db.create_collection("test-coll")
    db.add_to_collection(cid, [p1])

    from pipeline import load_photo_features

    # Without collection_id — returns both
    all_photos = load_photo_features(db)
    assert len(all_photos) == 2

    # With collection_id — returns only p1
    scoped = load_photo_features(db, collection_id=cid)
    assert len(scoped) == 1
    assert scoped[0]["id"] == p1
```

**Step 2: Run test to verify it fails**

Run: `python -m pytest vireo/tests/test_pipeline.py::test_load_photo_features_collection_scoped -v`
Expected: FAIL — `load_photo_features() got an unexpected keyword argument 'collection_id'`

**Step 3: Write minimal implementation**

In `vireo/pipeline.py`, modify `load_photo_features` signature and SQL:

```python
def load_photo_features(db, collection_id=None):
    """Load all pipeline-relevant features for workspace photos from the database.

    Args:
        db: Database instance with active workspace
        collection_id: optional collection ID to scope results

    Returns:
        list of photo dicts
    """
    ws_id = db._ws_id()

    if collection_id:
        rows = db.conn.execute(
            f"""SELECT {_PIPELINE_PHOTO_COLS}
                FROM photos p
                JOIN workspace_folders wf ON wf.folder_id = p.folder_id
                JOIN collection_photos cp ON cp.photo_id = p.id
                WHERE wf.workspace_id = ? AND cp.collection_id = ?
                ORDER BY p.timestamp""",
            (ws_id, collection_id),
        ).fetchall()
    else:
        rows = db.conn.execute(
            f"""SELECT {_PIPELINE_PHOTO_COLS}
                FROM photos p
                JOIN workspace_folders wf ON wf.folder_id = p.folder_id
                WHERE wf.workspace_id = ?
                ORDER BY p.timestamp""",
            (ws_id,),
        ).fetchall()
```

The rest of the function stays the same — predictions and keywords are joined by photo_id from the rows already fetched.

**Step 4: Run test to verify it passes**

Run: `python -m pytest vireo/tests/test_pipeline.py::test_load_photo_features_collection_scoped -v`
Expected: PASS

**Step 5: Commit**

```bash
git add vireo/pipeline.py vireo/tests/test_pipeline.py
git commit -m "feat: add collection_id scoping to load_photo_features"
```

---

### Task 2: Add collection_id to regroup API

**Files:**
- Modify: `vireo/app.py:4214-4271`
- Test: `vireo/tests/test_app.py`

**Step 1: Write the failing test**

Add to `vireo/tests/test_app.py`:

```python
def test_pipeline_regroup_accepts_collection_id(app_and_db):
    """POST /api/jobs/regroup accepts collection_id parameter."""
    app, db = app_and_db
    client = app.test_client()

    # Create a collection
    cid = db.create_collection("test-pipeline")
    photos = db.conn.execute("SELECT id FROM photos").fetchall()
    db.add_to_collection(cid, [p["id"] for p in photos])

    # The job will fail because no pipeline features exist, but the route
    # should accept collection_id without error
    resp = client.post('/api/jobs/regroup', json={"collection_id": cid})
    assert resp.status_code == 200
    data = resp.get_json()
    assert "job_id" in data
```

**Step 2: Run test to verify it fails**

Run: `python -m pytest vireo/tests/test_app.py::test_pipeline_regroup_accepts_collection_id -v`
Expected: PASS (route already accepts arbitrary body keys) — but we need to verify collection_id is actually passed through to `load_photo_features`. Update the test after wiring is done in step 3.

**Step 3: Write minimal implementation**

In `vireo/app.py`, modify `api_job_regroup()` at line 4214:

```python
@app.route("/api/jobs/regroup", methods=["POST"])
def api_job_regroup():
    """Run pipeline stages 2-6 (grouping + scoring + triage) from cached features."""
    body = request.get_json(silent=True) or {}
    collection_id = body.get("collection_id")

    import config as cfg

    effective_cfg = _get_db().get_effective_config(cfg.load())
    pipeline_cfg = effective_cfg.get("pipeline", {})

    runner = app._job_runner
    active_ws = _get_db()._active_workspace_id

    def work(job):
        from pipeline import (
            load_photo_features,
            run_full_pipeline,
            save_results,
        )

        thread_db = Database(db_path)
        thread_db.set_active_workspace(active_ws)

        runner.push_event(
            job["id"],
            "progress",
            {"phase": "Loading features from database", "current": 0, "total": 3},
        )

        photos = load_photo_features(thread_db, collection_id=collection_id)
        if not photos:
            return {"error": "No photos with pipeline features found. Run extract-masks first."}

        runner.push_event(
            job["id"],
            "progress",
            {"phase": "Grouping encounters and bursts", "current": 1, "total": 3},
        )

        results = run_full_pipeline(photos, config=pipeline_cfg)

        runner.push_event(
            job["id"],
            "progress",
            {"phase": "Saving results", "current": 2, "total": 3},
        )

        cache_dir = os.path.dirname(db_path)
        save_results(results, cache_dir, active_ws)

        return results["summary"]

    job_id = runner.start("regroup", work, config={"pipeline": pipeline_cfg})
    return jsonify({"job_id": job_id})
```

**Step 4: Run test to verify it passes**

Run: `python -m pytest vireo/tests/test_app.py::test_pipeline_regroup_accepts_collection_id -v`
Expected: PASS

**Step 5: Commit**

```bash
git add vireo/app.py vireo/tests/test_app.py
git commit -m "feat: pass collection_id through regroup API to load_photo_features"
```

---

### Task 3: Add /pipeline/review route and navbar changes

**Files:**
- Modify: `vireo/app.py:232-268` (pipeline_page route)
- Modify: `vireo/app.py:212-214` (remove classify route)
- Modify: `vireo/templates/_navbar.html:702-707`
- Test: `vireo/tests/test_app.py`

**Step 1: Write the failing test**

Add to `vireo/tests/test_app.py`:

```python
def test_pipeline_review_page(app_and_db):
    """GET /pipeline/review returns 200."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get('/pipeline/review')
    assert resp.status_code == 200


def test_classify_route_removed(app_and_db):
    """GET /classify should return 404 after removal."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get('/classify')
    assert resp.status_code == 404
```

**Step 2: Run tests to verify they fail**

Run: `python -m pytest vireo/tests/test_app.py::test_pipeline_review_page vireo/tests/test_app.py::test_classify_route_removed -v`
Expected: FAIL — `/pipeline/review` returns 404, `/classify` returns 200

**Step 3: Implement route changes**

**3a. In `vireo/app.py`, remove the classify route (line 212-214):**

Delete:
```python
@app.route("/classify")
def classify():
    return render_template("classify.html")
```

**3b. In `vireo/app.py`, add the `/pipeline/review` route right after the existing `/pipeline` route (after line 268):**

```python
@app.route("/pipeline/review")
def pipeline_review_page():
    db = _get_db()
    has_masks = db.conn.execute(
        """SELECT COUNT(*) FROM photos p
           JOIN workspace_folders wf ON wf.folder_id = p.folder_id
           WHERE wf.workspace_id = ? AND p.mask_path IS NOT NULL""",
        (db._active_workspace_id,),
    ).fetchone()[0]
    has_detections = db.conn.execute(
        """SELECT COUNT(*) FROM photos p
           JOIN workspace_folders wf ON wf.folder_id = p.folder_id
           WHERE wf.workspace_id = ? AND p.detection_box IS NOT NULL""",
        (db._active_workspace_id,),
    ).fetchone()[0]
    total_photos = db.count_photos()

    from pipeline import load_results
    import config as cfg
    cache_dir = os.path.dirname(db_path)
    results = load_results(cache_dir, db._active_workspace_id)
    effective_cfg = db.get_effective_config(cfg.load())
    pipeline_cfg = effective_cfg.get("pipeline", {})

    return render_template(
        "pipeline_review.html",
        total_photos=total_photos,
        has_detections=has_detections,
        has_masks=has_masks,
        results=results,
        pipeline_config={
            "sam2_variant": pipeline_cfg.get("sam2_variant", "sam2-small"),
            "dinov2_variant": pipeline_cfg.get("dinov2_variant", "vit-b14"),
            "proxy_longest_edge": pipeline_cfg.get("proxy_longest_edge", 1536),
        },
    )
```

**3c. Create `vireo/templates/pipeline_review.html` as an empty placeholder (content moved in Task 5):**

```html
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Vireo - Pipeline Review</title>
</head>
<body>
{% set active_page = 'pipeline_review' %}
{% include '_navbar.html' %}
<div style="padding:40px;text-align:center;color:#888;">Pipeline Review — coming soon</div>
</body>
</html>
```

**3d. In `vireo/templates/_navbar.html`, replace the classify link (line 703) and add pipeline review:**

Replace:
```html
<a href="/classify" {% if active_page == 'classify' %}class="active"{% endif %}>Classify</a>
```

With:
```html
<a href="/pipeline/review" {% if active_page == 'pipeline_review' %}class="active"{% endif %}>Pipeline Review</a>
```

**Step 4: Run tests to verify they pass**

Run: `python -m pytest vireo/tests/test_app.py::test_pipeline_review_page vireo/tests/test_app.py::test_classify_route_removed -v`
Expected: PASS

**Step 5: Commit**

```bash
git add vireo/app.py vireo/templates/_navbar.html vireo/templates/pipeline_review.html vireo/tests/test_app.py
git commit -m "feat: add /pipeline/review route, remove /classify route, update navbar"
```

---

### Task 4: Update pipeline_page route to pass classify data

**Files:**
- Modify: `vireo/app.py:232-268` (the `pipeline_page` function)
- Test: `vireo/tests/test_app.py`

**Step 1: Write the failing test**

Add to `vireo/tests/test_app.py`:

```python
def test_pipeline_page_has_collection_picker(app_and_db):
    """GET /pipeline HTML includes collection picker elements."""
    app, db = app_and_db
    client = app.test_client()

    # Create a collection so there's data
    cid = db.create_collection("Test Collection")

    resp = client.get('/pipeline')
    assert resp.status_code == 200
    html = resp.data.decode()
    assert 'collectionPicker' in html
    assert 'modelPicker' in html or 'cfgModel' in html
```

**Step 2: Run test to verify it fails**

Run: `python -m pytest vireo/tests/test_app.py::test_pipeline_page_has_collection_picker -v`
Expected: FAIL — current pipeline.html doesn't have `collectionPicker`

**Step 3: Update the pipeline_page route to pass extra template vars**

The pipeline_page route already passes pipeline_config. We don't need to pass model/label/collection data from the server since those are loaded via JS API calls (same pattern as classify.html). The test will pass once we rebuild the template in Task 6. For now, update the route to also pass `has_sharpness` count:

```python
@app.route("/pipeline")
def pipeline_page():
    db = _get_db()
    has_masks = db.conn.execute(
        """SELECT COUNT(*) FROM photos p
           JOIN workspace_folders wf ON wf.folder_id = p.folder_id
           WHERE wf.workspace_id = ? AND p.mask_path IS NOT NULL""",
        (db._active_workspace_id,),
    ).fetchone()[0]
    has_detections = db.conn.execute(
        """SELECT COUNT(*) FROM photos p
           JOIN workspace_folders wf ON wf.folder_id = p.folder_id
           WHERE wf.workspace_id = ? AND p.detection_box IS NOT NULL""",
        (db._active_workspace_id,),
    ).fetchone()[0]
    has_sharpness = db.conn.execute(
        """SELECT COUNT(*) FROM photos p
           JOIN workspace_folders wf ON wf.folder_id = p.folder_id
           WHERE wf.workspace_id = ? AND p.subject_tenengrad IS NOT NULL""",
        (db._active_workspace_id,),
    ).fetchone()[0]
    total_photos = db.count_photos()

    from pipeline import load_results
    import config as cfg
    cache_dir = os.path.dirname(db_path)
    results = load_results(cache_dir, db._active_workspace_id)
    effective_cfg = db.get_effective_config(cfg.load())
    pipeline_cfg = effective_cfg.get("pipeline", {})

    return render_template(
        "pipeline.html",
        total_photos=total_photos,
        has_detections=has_detections,
        has_masks=has_masks,
        has_sharpness=has_sharpness,
        results=results,
        pipeline_config={
            "sam2_variant": pipeline_cfg.get("sam2_variant", "sam2-small"),
            "dinov2_variant": pipeline_cfg.get("dinov2_variant", "vit-b14"),
            "proxy_longest_edge": pipeline_cfg.get("proxy_longest_edge", 1536),
        },
    )
```

This step is a prerequisite for Task 6. The test will pass after Task 6 rebuilds the template.

**Step 4: Skip test run (will be validated after Task 6)**

**Step 5: Commit**

```bash
git add vireo/app.py vireo/tests/test_app.py
git commit -m "feat: pipeline_page route passes has_sharpness and classify-related data"
```

---

### Task 5: Create pipeline_review.html from existing pipeline results UI

**Files:**
- Modify: `vireo/templates/pipeline_review.html` (replace placeholder from Task 3)
- Source: `vireo/templates/pipeline.html` (lines 100-748 CSS, lines 936-1012 HTML, lines 1013-1928 JS)

**Step 1: Write the failing test**

Add to `vireo/tests/test_app.py`:

```python
def test_pipeline_review_has_threshold_sliders(app_and_db):
    """GET /pipeline/review includes scoring threshold controls."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get('/pipeline/review')
    html = resp.data.decode()
    assert 'slRejectCrop' in html or 'Scoring Thresholds' in html
```

**Step 2: Run test to verify it fails**

Run: `python -m pytest vireo/tests/test_app.py::test_pipeline_review_has_threshold_sliders -v`
Expected: FAIL — placeholder template doesn't have sliders

**Step 3: Build pipeline_review.html**

This is a large file — extract from the current `pipeline.html`:

1. **CSS:** Copy all styles from pipeline.html that relate to the results view: `.pipeline-layout`, `.pipeline-sidebar`, `.pipeline-main`, summary bar, filter bar, encounter cards, photo cards, inspect overlay, slider styles, etc. (lines ~10-748 of current pipeline.html). Remove styles for stage list, config selects, progress bar, stale warning — those stay on the Pipeline page.

2. **HTML body:** Use the sidebar + main layout. The sidebar contains:
   - Summary stats section
   - Filter buttons
   - Scoring threshold sliders (currently lines 841-893)
   - Grouping threshold sliders (currently lines 895-933)
   - Reset buttons
   - "Back to Pipeline" link

   The main area contains:
   - Summary bar (lines 939-971)
   - Filter bar (lines 973-979)
   - Encounters container (line 982)
   - Empty state if no results (lines 984-1004) — modified to link to `/pipeline`
   - Inspect overlay (lines 1008-1011)

3. **JavaScript:** Copy all JS from current pipeline.html:
   - `renderResults()`, `renderPhotoCard()`, `toggleEncounter()`, `setFilter()`
   - Scoring change handlers (`onScoringChange`, `resetScoringDefaults`)
   - Grouping change handlers (`onGroupingChange`, `resetGroupingDefaults`)
   - Reflow/regroup-live API calls
   - Inspection panel (`openInspect`, `closeInspect`)
   - `escapeHtml`, `formatDuration` utilities

   Remove JS for: model config changes, extract-masks job, regroup job — those stay on Pipeline page.

The template variable is `active_page = 'pipeline_review'`.

**Key change in empty state text:**
```html
<div class="empty-state" id="emptyState">
  <h2>No pipeline results yet</h2>
  <p>Run the pipeline from the <a href="/pipeline" style="color:var(--accent);">Pipeline</a> page first.</p>
</div>
```

**Step 4: Run test to verify it passes**

Run: `python -m pytest vireo/tests/test_app.py::test_pipeline_review_has_threshold_sliders -v`
Expected: PASS

**Step 5: Commit**

```bash
git add vireo/templates/pipeline_review.html vireo/tests/test_app.py
git commit -m "feat: create pipeline_review.html with results view, sliders, inspection"
```

---

### Task 6: Rebuild pipeline.html as the configure & run page

**Files:**
- Modify: `vireo/templates/pipeline.html` (full rewrite)

This is the largest task. The new pipeline.html has:

**Step 1: Write the failing test**

The test from Task 4 (`test_pipeline_page_has_collection_picker`) covers this. Also add:

```python
def test_pipeline_page_has_stage_cards(app_and_db):
    """GET /pipeline has the 3 stage cards."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get('/pipeline')
    html = resp.data.decode()
    assert 'card-classify' in html or 'Stage 1' in html or 'Classify' in html
    assert 'card-extract' in html or 'Stage 2' in html or 'Extract Features' in html
    assert 'card-group' in html or 'Stage 3' in html or 'Group' in html
```

**Step 2: Run test to verify it fails**

Run: `python -m pytest vireo/tests/test_app.py::test_pipeline_page_has_stage_cards -v`
Expected: FAIL — current pipeline.html doesn't have `card-classify`

**Step 3: Rewrite pipeline.html**

The new template structure:

```html
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<link rel="icon" type="image/png" href="/favicon.ico">
<link rel="apple-touch-icon" href="/static/apple-touch-icon.png">
<title>Vireo - Pipeline</title>
<style>
/* Base styles (keep from current pipeline.html) */
* { box-sizing: border-box; margin: 0; padding: 0; }
body {
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
  background: var(--bg-primary, #0A1F2E);
  color: var(--text-primary, #E0F0F0);
  min-height: 100vh;
  padding-bottom: 36px;
}
.content { max-width: 900px; margin: 0 auto; padding: 32px 24px; }

/* Collection picker bar */
.collection-bar {
  background: var(--bg-secondary, #0E2A3D);
  border: 1px solid var(--border-primary, #14374E);
  border-radius: 8px;
  padding: 16px 20px;
  margin-bottom: 24px;
  display: flex;
  align-items: center;
  gap: 16px;
}
.collection-bar select {
  background: var(--bg-input, #14374E);
  color: var(--text-primary, #E0F0F0);
  border: 1px solid var(--border-secondary, #1A4560);
  border-radius: 4px;
  padding: 8px 12px;
  font-size: 13px;
  min-width: 250px;
}
.collection-bar .photo-count {
  font-size: 12px;
  color: var(--text-dim, #5A8890);
}

/* Stage cards */
.stage-card {
  background: var(--bg-secondary, #0E2A3D);
  border: 1px solid var(--border-primary, #14374E);
  border-radius: 8px;
  margin-bottom: 16px;
  overflow: hidden;
}
.stage-header {
  display: flex;
  align-items: center;
  gap: 12px;
  padding: 16px 20px;
  cursor: pointer;
}
.stage-header:hover { background: rgba(255,255,255,0.02); }
.stage-num {
  width: 28px; height: 28px;
  border-radius: 50%;
  display: flex; align-items: center; justify-content: center;
  font-size: 13px; font-weight: 700;
  background: var(--bg-tertiary, #14374E);
  color: var(--text-dim, #5A8890);
  flex-shrink: 0;
}
.stage-num.complete { background: var(--accent, #24E5CA); color: #000; }
.stage-num.running { background: var(--info, #12C3B5); color: #000; animation: pulse 1.2s infinite; }
.stage-num.stale { background: var(--warning, #F0C040); color: #000; }
@keyframes pulse { 0%,100% { opacity: 1; } 50% { opacity: 0.4; } }
.stage-name { font-size: 15px; font-weight: 600; }
.stage-summary {
  margin-left: auto;
  font-size: 12px;
  color: var(--text-dim, #5A8890);
}
.stage-chevron {
  color: var(--text-ghost, #2D5058);
  font-size: 14px;
  transition: transform 0.15s;
}
.stage-card.expanded .stage-chevron { transform: rotate(90deg); }

/* Stage body (expandable) */
.stage-body {
  display: none;
  padding: 0 20px 20px;
  border-top: 1px solid var(--border-primary, #14374E);
}
.stage-card.expanded .stage-body { display: block; }

/* Settings rows inside stage body */
.setting-row {
  display: flex; align-items: center; gap: 16px;
  margin-bottom: 12px; flex-wrap: wrap;
}
.setting-item { flex: 1; min-width: 180px; }
.setting-label {
  font-size: 11px; color: var(--text-faint, #5A8890);
  text-transform: uppercase; margin-bottom: 6px;
}
.setting-select {
  background: var(--bg-input, #14374E);
  color: var(--text-primary, #E0F0F0);
  border: 1px solid var(--border-secondary, #1A4560);
  border-radius: 4px; padding: 8px 12px; font-size: 13px; width: 100%;
}

/* Run button row */
.run-row {
  display: flex; align-items: center; gap: 12px; margin-top: 16px; flex-wrap: wrap;
}

/* Buttons — reuse from classify */
.btn {
  border: none; border-radius: 4px; padding: 10px 24px;
  font-size: 14px; font-weight: 600; cursor: pointer; transition: background 0.15s;
}
.btn:disabled { cursor: default; opacity: 0.4; }
.btn-primary { background: var(--accent, #24E5CA); color: var(--bg-primary, #0A1F2E); }
.btn-primary:hover { background: var(--accent-hover, #5AF0DA); }
.btn-secondary { background: var(--bg-tertiary, #14374E); color: var(--text-secondary, #B0CCCC); }
.btn-secondary:hover { background: var(--border-secondary, #1A4560); }
.status-msg { font-size: 13px; color: var(--text-dim, #888); }
.status-msg.ok { color: var(--accent, #24E5CA); }

/* Readiness panel (from classify) */
.readiness-panel {
  padding: 10px 14px;
  background: var(--bg-primary, #0A1F2E);
  border-radius: 6px;
  border: 1px solid var(--border-primary, #14374E);
  font-size: 12px;
  margin-bottom: 12px;
}

/* Progress bar */
.progress-wrap { margin-top: 10px; }
.progress-bar-track {
  height: 4px; background: var(--bg-tertiary, #14374E); border-radius: 2px; overflow: hidden;
}
.progress-bar-fill {
  height: 100%; background: var(--accent, #24E5CA); width: 0%; transition: width 0.3s;
}
.progress-text { font-size: 11px; color: var(--text-dim, #5A8890); margin-top: 4px; }

/* Stale indicator */
.stale-badge {
  font-size: 10px; font-weight: 600;
  color: var(--warning, #F0C040);
  background: rgba(240, 192, 64, 0.15);
  padding: 2px 8px; border-radius: 10px;
  margin-left: 8px;
}

/* View results link */
.view-results-btn {
  display: inline-block;
  margin-top: 12px;
  padding: 10px 20px;
  background: var(--accent, #24E5CA);
  color: #000;
  font-weight: 600;
  font-size: 13px;
  border-radius: 4px;
  text-decoration: none;
}
.view-results-btn:hover { background: var(--accent-hover, #5AF0DA); }
</style>
</head>
<body>
{% set active_page = 'pipeline' %}
{% include '_navbar.html' %}

<div class="content">
  <h2 style="margin-bottom:20px;font-size:20px;">Pipeline</h2>

  <!-- Collection picker -->
  <div class="collection-bar">
    <label style="font-size:13px;font-weight:600;">Collection</label>
    <select id="collectionPicker" onchange="onCollectionChange()">
      <option value="">Select a collection...</option>
    </select>
    <span class="photo-count" id="collectionPhotoCount"></span>
  </div>

  <!-- Card 1: Classify -->
  <div class="stage-card expanded" id="card-classify">
    <div class="stage-header" onclick="toggleCard('classify')">
      <span class="stage-num {{ 'complete' if has_detections else '' }}" id="numClassify">1</span>
      <span class="stage-name">Classify</span>
      <span class="stage-summary" id="summaryClassify">
        {% if has_detections %}{{ has_detections }} detections{% endif %}
      </span>
      <span class="stage-chevron">&#9656;</span>
    </div>
    <div class="stage-body">
      <div class="setting-row">
        <div class="setting-item">
          <div class="setting-label">Model</div>
          <select class="setting-select" id="cfgModel" onchange="updateReadiness()">
            <option value="">Loading models...</option>
          </select>
        </div>
        <div class="setting-item">
          <div class="setting-label">Labels</div>
          <div id="labelsPicker" style="font-size:12px;max-height:100px;overflow-y:auto;"></div>
        </div>
        <div class="setting-item">
          <div class="setting-label">Threshold</div>
          <div style="display:flex;align-items:center;gap:8px;">
            <input type="range" id="cfgThreshold" min="10" max="90" step="5" value="40"
                   style="width:120px;accent-color:var(--accent,#24E5CA);"
                   oninput="document.getElementById('cfgThresholdVal').textContent = this.value + '%'">
            <span style="font-size:13px;" id="cfgThresholdVal">40%</span>
          </div>
        </div>
      </div>
      <div class="readiness-panel" id="readinessPanel" style="display:none;"></div>
      <div class="run-row">
        <button class="btn btn-primary" id="btnClassify" onclick="runClassify(false)" disabled>Classify</button>
        <label style="font-size:12px;color:var(--text-secondary);cursor:pointer;">
          <input type="checkbox" id="chkReclassify" style="accent-color:var(--accent);"> Re-classify
        </label>
        <span class="status-msg" id="statusClassify"></span>
      </div>
      <div class="progress-wrap" id="progressClassify" style="display:none;">
        <div class="progress-bar-track"><div class="progress-bar-fill" id="fillClassify"></div></div>
        <div class="progress-text" id="textClassify"></div>
      </div>
    </div>
  </div>

  <!-- Card 2: Extract Features -->
  <div class="stage-card" id="card-extract">
    <div class="stage-header" onclick="toggleCard('extract')">
      <span class="stage-num {{ 'complete' if has_masks else '' }}" id="numExtract">2</span>
      <span class="stage-name">Extract Features</span>
      <span class="stage-summary" id="summaryExtract">
        {% if has_masks %}{{ has_masks }} masks{% if has_sharpness %}, {{ has_sharpness }} sharpness{% endif %}{% endif %}
      </span>
      <span class="stage-chevron">&#9656;</span>
    </div>
    <div class="stage-body">
      <div class="setting-row">
        <div class="setting-item">
          <div class="setting-label">SAM2</div>
          <select class="setting-select" id="cfgSam2" onchange="onModelConfigChange()">
            <option value="sam2-tiny" {{ 'selected' if pipeline_config.sam2_variant == 'sam2-tiny' }}>Tiny</option>
            <option value="sam2-small" {{ 'selected' if pipeline_config.sam2_variant == 'sam2-small' }}>Small</option>
            <option value="sam2-base-plus" {{ 'selected' if pipeline_config.sam2_variant == 'sam2-base-plus' }}>Base+</option>
            <option value="sam2-large" {{ 'selected' if pipeline_config.sam2_variant == 'sam2-large' }}>Large</option>
          </select>
        </div>
        <div class="setting-item">
          <div class="setting-label">DINOv2</div>
          <select class="setting-select" id="cfgDinov2" onchange="onModelConfigChange()">
            <option value="vit-s14" {{ 'selected' if pipeline_config.dinov2_variant == 'vit-s14' }}>ViT-S/14 (384d)</option>
            <option value="vit-b14" {{ 'selected' if pipeline_config.dinov2_variant == 'vit-b14' }}>ViT-B/14 (768d)</option>
            <option value="vit-l14" {{ 'selected' if pipeline_config.dinov2_variant == 'vit-l14' }}>ViT-L/14 (1024d)</option>
          </select>
        </div>
        <div class="setting-item">
          <div class="setting-label">Proxy res.</div>
          <div style="display:flex;align-items:center;gap:8px;">
            <input type="range" min="1024" max="2048" value="{{ pipeline_config.proxy_longest_edge }}" step="64"
                   id="cfgProxy" oninput="onModelConfigChange()"
                   style="width:120px;accent-color:var(--accent,#24E5CA);">
            <span style="font-size:13px;" id="valProxy">{{ pipeline_config.proxy_longest_edge }}</span>
          </div>
        </div>
      </div>
      <div class="run-row">
        <button class="btn btn-primary" id="btnExtract" onclick="runExtract()" disabled>Extract Features</button>
        <span class="status-msg" id="statusExtract"></span>
      </div>
      <div class="progress-wrap" id="progressExtract" style="display:none;">
        <div class="progress-bar-track"><div class="progress-bar-fill" id="fillExtract"></div></div>
        <div class="progress-text" id="textExtract"></div>
      </div>
    </div>
  </div>

  <!-- Card 3: Group & Score -->
  <div class="stage-card" id="card-group">
    <div class="stage-header" onclick="toggleCard('group')">
      <span class="stage-num {{ 'complete' if results else '' }}" id="numGroup">3</span>
      <span class="stage-name">Group &amp; Score</span>
      <span class="stage-summary" id="summaryGroup">
        {% if results %}
          {{ results.summary.encounter_count }} encounters &mdash;
          {{ results.summary.keep_count }}K / {{ results.summary.review_count }}R / {{ results.summary.reject_count }}X
        {% endif %}
      </span>
      <span class="stage-chevron">&#9656;</span>
    </div>
    <div class="stage-body">
      <p style="font-size:13px;color:var(--text-secondary);margin-bottom:12px;">
        Runs encounter segmentation, burst clustering, quality scoring, and triage.
        Tune scoring and grouping thresholds on the
        <a href="/pipeline/review" style="color:var(--accent);">Pipeline Review</a> page.
      </p>
      <div class="run-row">
        <button class="btn btn-primary" id="btnGroup" onclick="runGroup()" disabled>Group &amp; Score</button>
        <span class="status-msg" id="statusGroup"></span>
      </div>
      <div class="progress-wrap" id="progressGroup" style="display:none;">
        <div class="progress-bar-track"><div class="progress-bar-fill" id="fillGroup"></div></div>
        <div class="progress-text" id="textGroup"></div>
      </div>
      {% if results %}
      <a href="/pipeline/review" class="view-results-btn">View Results</a>
      {% endif %}
    </div>
  </div>
</div>
```

**JavaScript section — key functions to include:**

```javascript
<script>
// -- Init --
loadCollections();
loadModels();
loadLabels();
updateCardStates();

// -- Collection picker (from classify.html) --
async function loadCollections() { /* fetch /api/collections, populate picker */ }
function onCollectionChange() { /* update photo count, enable/disable cards */ }

// -- Card 1: Classify (from classify.html) --
async function loadModels() { /* fetch /api/models, populate cfgModel */ }
async function loadLabels() { /* fetch /api/labels, populate labelsPicker */ }
async function updateReadiness() { /* fetch /api/classify/readiness, update readinessPanel */ }
async function runClassify(reclassify) {
  /* POST /api/jobs/classify with collection_id, model_id, labels_files, threshold
     Stream SSE progress to progressClassify
     On complete: mark card 1 complete, mark cards 2-3 stale, update summaryClassify */
}

// -- Card 2: Extract Features (from pipeline.html) --
function onModelConfigChange() {
  /* Save to /api/pipeline/config, update proxy val display */
}
async function runExtract() {
  /* POST /api/jobs/extract-masks with collection_id
     Stream SSE progress to progressExtract
     On complete: mark card 2 complete, mark card 3 stale, update summaryExtract */
}

// -- Card 3: Group & Score (from pipeline.html) --
async function runGroup() {
  /* POST /api/jobs/regroup with collection_id
     Stream SSE progress to progressGroup
     On complete: mark card 3 complete, update summaryGroup */
}

// -- Shared --
function toggleCard(name) { /* toggle .expanded class on stage-card */ }
function updateCardStates() {
  /* Check has_detections/has_masks/results to enable/disable buttons
     Mark downstream cards as stale when upstream re-runs */
}
function escapeHtml(str) { /* same as classify.html */ }
function formatDuration(s) { /* same as pipeline.html */ }
</script>
```

Each JS function body should be ported from the corresponding function in classify.html or pipeline.html, with `collection_id` added to all API calls.

**Step 4: Run tests to verify they pass**

Run: `python -m pytest vireo/tests/test_app.py::test_pipeline_page_has_collection_picker vireo/tests/test_app.py::test_pipeline_page_has_stage_cards -v`
Expected: PASS

**Step 5: Commit**

```bash
git add vireo/templates/pipeline.html vireo/tests/test_app.py
git commit -m "feat: rebuild pipeline.html with 3 stage cards and collection picker"
```

---

### Task 7: Delete classify.html

**Files:**
- Delete: `vireo/templates/classify.html`

**Step 1: Verify no references remain**

Search for `classify.html` in app.py — the route was removed in Task 3. Search templates for links to `/classify`.

**Step 2: Delete the file**

```bash
git rm vireo/templates/classify.html
```

**Step 3: Run all tests**

Run: `python -m pytest vireo/tests/test_app.py -v`
Expected: All pass, no template-not-found errors

**Step 4: Commit**

```bash
git commit -m "chore: remove classify.html, absorbed into pipeline.html"
```

---

### Task 8: Run full test suite and verify

**Step 1: Run the test suite from CLAUDE.md**

```bash
python -m pytest tests/test_workspaces.py vireo/tests/test_db.py vireo/tests/test_app.py vireo/tests/test_photos_api.py vireo/tests/test_edits_api.py vireo/tests/test_jobs_api.py vireo/tests/test_darktable_api.py vireo/tests/test_config.py -v
```

Expected: All pass

**Step 2: Fix any failures**

Common issues to watch for:
- Tests that reference `/classify` route expecting 200 (should now 404)
- Tests that import from classify-related code
- Template rendering errors from missing variables

**Step 3: Final commit if any fixes needed**

```bash
git add -u
git commit -m "fix: resolve test failures from pipeline redesign"
```
