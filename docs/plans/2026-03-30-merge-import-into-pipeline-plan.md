# Merge Import into Pipeline — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Merge the Import page into the Pipeline page so the Pipeline is the single entry point for all photo processing. Move Lightroom import to its own page. Add "Send to Pipeline" link in audit panel.

**Architecture:** The Pipeline page gains two new stages (Scan & Import becomes explicit, Previews is added after Thumbnails). The `PipelineParams` dataclass gains `sources: list[str]` (replacing single `source`), `skip_classify: bool`, and `preview_max_size: int`. The pipeline_job.py orchestrator gains a previews stage between thumbnails and classify. The Import page template and route are removed. Lightroom import moves to `/lightroom` with its own template.

**Tech Stack:** Python/Flask backend, Jinja2 templates, vanilla JS frontend, SQLite

---

### Task 1: Add `skip_classify` and `preview_max_size` to PipelineParams

**Files:**
- Modify: `vireo/pipeline_job.py:27-42`
- Test: `vireo/tests/test_pipeline_job.py`

**Step 1: Write the failing test**

In `vireo/tests/test_pipeline_job.py`, add:

```python
def test_pipeline_params_has_skip_classify():
    """PipelineParams should support skip_classify flag."""
    params = PipelineParams(collection_id=1, skip_classify=True)
    assert params.skip_classify is True


def test_pipeline_params_skip_classify_defaults_false():
    params = PipelineParams(collection_id=1)
    assert params.skip_classify is False


def test_pipeline_params_has_preview_max_size():
    """PipelineParams should support preview_max_size."""
    params = PipelineParams(collection_id=1, preview_max_size=2560)
    assert params.preview_max_size == 2560


def test_pipeline_params_preview_max_size_defaults_1920():
    params = PipelineParams(collection_id=1)
    assert params.preview_max_size == 1920
```

**Step 2: Run test to verify it fails**

Run: `python -m pytest vireo/tests/test_pipeline_job.py::test_pipeline_params_has_skip_classify vireo/tests/test_pipeline_job.py::test_pipeline_params_has_preview_max_size -v`
Expected: FAIL with `TypeError: unexpected keyword argument`

**Step 3: Write minimal implementation**

In `vireo/pipeline_job.py`, add to `PipelineParams` (after `skip_regroup`):

```python
    skip_classify: bool = False
    preview_max_size: int = 1920
```

**Step 4: Run test to verify it passes**

Run: `python -m pytest vireo/tests/test_pipeline_job.py -v`
Expected: ALL PASS

**Step 5: Commit**

```bash
git add vireo/pipeline_job.py vireo/tests/test_pipeline_job.py
git commit -m "feat: add skip_classify and preview_max_size to PipelineParams"
```

---

### Task 2: Add multi-source support to PipelineParams

**Files:**
- Modify: `vireo/pipeline_job.py:27-42`
- Test: `vireo/tests/test_pipeline_job.py`

**Step 1: Write the failing test**

```python
def test_pipeline_params_sources_list():
    """PipelineParams should accept a list of source folders."""
    params = PipelineParams(sources=["/photos/card1", "/photos/card2"])
    assert params.sources == ["/photos/card1", "/photos/card2"]


def test_pipeline_params_sources_defaults_none():
    params = PipelineParams(collection_id=1)
    assert params.sources is None
```

**Step 2: Run test to verify it fails**

Run: `python -m pytest vireo/tests/test_pipeline_job.py::test_pipeline_params_sources_list -v`
Expected: FAIL

**Step 3: Write minimal implementation**

In `vireo/pipeline_job.py`, add to `PipelineParams` (after `source`):

```python
    sources: list | None = None
```

**Step 4: Run test to verify it passes**

Run: `python -m pytest vireo/tests/test_pipeline_job.py -v`
Expected: ALL PASS

**Step 5: Commit**

```bash
git add vireo/pipeline_job.py vireo/tests/test_pipeline_job.py
git commit -m "feat: add sources list to PipelineParams for multi-folder scanning"
```

---

### Task 3: Add previews stage to pipeline_job.py

**Files:**
- Modify: `vireo/pipeline_job.py`
- Test: `vireo/tests/test_pipeline_job.py`

**Step 1: Write the failing test**

```python
def test_pipeline_previews_stage_runs(tmp_path, monkeypatch):
    """Pipeline should run a previews stage after thumbnails."""
    import sys
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
    from pipeline_job import PipelineParams, run_pipeline_job

    # Create a minimal database with one photo
    from db import Database
    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    # Patch scanner, thumbnails, etc. to be no-ops — we just need to verify
    # the previews stage appears in the result
    params = PipelineParams(
        collection_id=1,  # skip scan
        skip_classify=True,
        skip_extract_masks=True,
        skip_regroup=True,
        preview_max_size=1920,
    )

    runner = FakeRunner()
    job = _make_job()
    result = run_pipeline_job(job, runner, db_path, ws_id, params)

    # The stages dict in progress events should include "previews"
    stage_events = [e[2]["stages"] for e in runner.events
                    if e[1] == "progress" and "stages" in e[2]]
    assert any("previews" in s for s in stage_events), \
        "Expected 'previews' stage in progress events"
```

**Step 2: Run test to verify it fails**

Run: `python -m pytest vireo/tests/test_pipeline_job.py::test_pipeline_previews_stage_runs -v`
Expected: FAIL — no "previews" in stages

**Step 3: Write minimal implementation**

In `vireo/pipeline_job.py`, make these changes:

1. Add `"previews"` to the `stages` dict (after `"thumbnails"`):
```python
        "previews": {"status": "pending", "count": 0, "label": "Generating previews"},
```

2. Add `"previews"` to `step_defs` (after thumbnails):
```python
        {"id": "previews", "label": "Generate previews"},
```

3. Add `"previews"` to the `_current_phase` stage list (after `"thumbnails"`).

4. Add a `previews_stage()` function after `thumbnail_stage()`:
```python
    def previews_stage():
        """Generate preview images for browsed photos."""
        stages["previews"]["status"] = "running"
        runner.update_step(job["id"], "previews", status="running")
        _update_stages(runner, job["id"], stages)

        try:
            import config as cfg
            from image_loader import load_image

            thread_db = Database(db_path)
            thread_db.set_active_workspace(workspace_id)

            max_size = params.preview_max_size
            if max_size == 0:
                max_size = None  # Full resolution
            preview_quality = cfg.load().get("preview_quality", 90)
            preview_dir = os.path.join(
                os.path.dirname(db_path).replace("vireo.db", ""),
                "previews"
            )
            # Use the same preview dir logic as the standalone job
            # The app passes thumb_cache_dir; we derive previews dir from db_path's parent
            base_dir = os.path.dirname(db_path)
            preview_dir = os.path.join(base_dir, "previews")
            os.makedirs(preview_dir, exist_ok=True)

            if collection_id:
                photos = thread_db.get_collection_photos(collection_id, per_page=999999)
            else:
                photos = thread_db.get_photos(per_page=999999)

            folders = {f["id"]: f["path"] for f in thread_db.get_folder_tree()}
            total = len(photos)
            generated = 0
            skipped = 0

            for i, photo in enumerate(photos):
                if _should_abort(abort):
                    break
                cache_path = os.path.join(preview_dir, f'{photo["id"]}.jpg')
                if os.path.exists(cache_path):
                    skipped += 1
                else:
                    folder_path = folders.get(photo["folder_id"], "")
                    image_path = os.path.join(folder_path, photo["filename"])
                    img = load_image(image_path, max_size=max_size)
                    if img:
                        img.save(cache_path, format="JPEG", quality=preview_quality)
                        generated += 1

                stages["previews"]["count"] = i + 1
                runner.push_event(job["id"], "progress", {
                    "phase": "Generating previews",
                    "current": i + 1,
                    "total": total,
                    "current_file": photo["filename"],
                    "rate": round(
                        (i + 1) / max(time.time() - job["_start_time"], 0.01), 1
                    ),
                    "stages": {k: dict(v) for k, v in stages.items()},
                })

            result["stages"]["previews"] = {
                "generated": generated, "skipped": skipped, "total": total
            }
            stages["previews"]["status"] = "completed"
            runner.update_step(job["id"], "previews", status="completed",
                               summary=f"{generated} generated")
        except Exception as e:
            errors.append(f"[previews] Fatal: {e}")
            log.exception("Pipeline previews stage failed")
            stages["previews"]["status"] = "failed"
            runner.update_step(job["id"], "previews", status="failed", error=str(e))

        _update_stages(runner, job["id"], stages)
```

5. In the thread launch section, run `previews_stage()` after thumbnails join and before classify:
```python
    # Wait for scan-related threads to finish
    threads["scanner"].join()
    threads["collection"].join()
    threads["thumbnail"].join()
    threads["model_loader"].join()

    # Phase 1.5: previews (needs scan complete, runs before classify)
    if not abort.is_set():
        previews_stage()

    # Phase 2: classify (needs collection + models)
    if not abort.is_set():
        classify_stage()
```

**Step 4: Run test to verify it passes**

Run: `python -m pytest vireo/tests/test_pipeline_job.py -v`
Expected: ALL PASS

**Step 5: Commit**

```bash
git add vireo/pipeline_job.py vireo/tests/test_pipeline_job.py
git commit -m "feat: add previews stage to pipeline job orchestrator"
```

---

### Task 4: Add skip_classify support to pipeline_job.py

**Files:**
- Modify: `vireo/pipeline_job.py`
- Test: `vireo/tests/test_pipeline_job.py`

**Step 1: Write the failing test**

```python
def test_pipeline_skip_classify_skips_model_loader(tmp_path):
    """When skip_classify=True, model_loader and classify should be skipped."""
    import sys
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
    from pipeline_job import PipelineParams, run_pipeline_job
    from db import Database

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    params = PipelineParams(
        collection_id=1,
        skip_classify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()
    result = run_pipeline_job(job, runner, db_path, ws_id, params)

    # Check that classify was skipped in the last stages event
    last_stages = None
    for _, evt_type, data in reversed(runner.events):
        if evt_type == "progress" and "stages" in data:
            last_stages = data["stages"]
            break

    assert last_stages is not None
    assert last_stages["classify"]["status"] == "skipped"
    assert last_stages["model_loader"]["status"] == "skipped"
```

**Step 2: Run test to verify it fails**

Run: `python -m pytest vireo/tests/test_pipeline_job.py::test_pipeline_skip_classify_skips_model_loader -v`
Expected: FAIL — model_loader still runs/fails instead of being skipped

**Step 3: Write minimal implementation**

In `vireo/pipeline_job.py`:

1. In `model_loader_stage()`, add a skip guard at the top (after `stages["model_loader"]["status"] = "running"`... actually, before that):
```python
    def model_loader_stage():
        if params.skip_classify:
            stages["model_loader"]["status"] = "skipped"
            runner.update_step(job["id"], "model_loader", status="completed",
                               summary="Skipped")
            models_ready.set()
            return
        stages["model_loader"]["status"] = "running"
        # ... rest unchanged
```

2. In `classify_stage()`, add `params.skip_classify` to the existing guard:
```python
        if params.skip_classify or abort.is_set() or not collection_id or "clf" not in loaded_models:
```

3. In the `step_defs` list, conditionally include model_loader and classify:
```python
    step_defs = [
        {"id": "scan", "label": "Scan photos"},
        {"id": "thumbnails", "label": "Generate thumbnails"},
        {"id": "previews", "label": "Generate previews"},
    ]
    if not params.skip_classify:
        step_defs.append({"id": "model_loader", "label": "Load models"})
        step_defs.append({"id": "classify", "label": "Classify species"})
    if not params.skip_extract_masks:
        step_defs.append({"id": "extract_masks", "label": "Extract features"})
    if not params.skip_regroup:
        step_defs.append({"id": "regroup", "label": "Group encounters"})
```

**Step 4: Run test to verify it passes**

Run: `python -m pytest vireo/tests/test_pipeline_job.py -v`
Expected: ALL PASS

**Step 5: Commit**

```bash
git add vireo/pipeline_job.py vireo/tests/test_pipeline_job.py
git commit -m "feat: add skip_classify support to pipeline orchestrator"
```

---

### Task 5: Add multi-source scanning to pipeline_job.py

**Files:**
- Modify: `vireo/pipeline_job.py`
- Test: `vireo/tests/test_pipeline_job.py`

**Step 1: Write the failing test**

```python
def test_pipeline_params_sources_used_over_source():
    """When sources is provided, it should take precedence over source."""
    params = PipelineParams(source="/single", sources=["/a", "/b"])
    assert params.sources == ["/a", "/b"]
```

This is a sanity test. The real test is that `scanner_stage()` iterates over `params.sources` when provided.

**Step 2: Implement multi-source scanning**

In `vireo/pipeline_job.py`, modify `scanner_stage()`:

Replace the section that gets `root = params.source` with:
```python
            # Determine source folder(s)
            sources = params.sources or ([params.source] if params.source else [])

            for src_folder in sources:
                root = src_folder
                if params.destination:
                    from ingest import ingest as do_ingest

                    def ingest_cb(current, total, filename):
                        runner.push_event(job["id"], "progress", {
                            "phase": "Importing photos",
                            "current": current,
                            "total": total,
                            "current_file": filename,
                            "stages": {k: dict(v) for k, v in stages.items()},
                        })

                    do_ingest(
                        source_dir=src_folder,
                        destination_dir=params.destination,
                        db=thread_db,
                        file_types=params.file_types,
                        folder_template=params.folder_template,
                        skip_duplicates=params.skip_duplicates,
                        progress_callback=ingest_cb,
                    )
                    root = params.destination

                do_scan(
                    root, thread_db,
                    progress_callback=progress_cb,
                    incremental=True,
                    extract_full_metadata=pipeline_cfg.get("extract_full_metadata", True),
                    photo_callback=photo_cb,
                )
```

**Step 3: Run tests**

Run: `python -m pytest vireo/tests/test_pipeline_job.py -v`
Expected: ALL PASS

**Step 4: Commit**

```bash
git add vireo/pipeline_job.py vireo/tests/test_pipeline_job.py
git commit -m "feat: support multi-source folder scanning in pipeline"
```

---

### Task 6: Update `/api/jobs/pipeline` endpoint for new params

**Files:**
- Modify: `vireo/app.py:4522-4574`
- Test: `vireo/tests/test_pipeline_api.py`

**Step 1: Write the failing test**

In `vireo/tests/test_pipeline_api.py`, add:

```python
def test_pipeline_accepts_sources_list(setup):
    """Pipeline endpoint should accept sources as a list of folders."""
    app, db_path = setup
    import tempfile, os
    src1 = tempfile.mkdtemp()
    src2 = tempfile.mkdtemp()
    try:
        # Create minimal JPEG in each
        jpeg_bytes = bytes([
            0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x10, 0x4A, 0x46, 0x49, 0x46, 0x00,
            0x01, 0x01, 0x00, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00, 0xFF, 0xD9
        ])
        for src in [src1, src2]:
            with open(os.path.join(src, "test.jpg"), "wb") as f:
                f.write(jpeg_bytes)

        with app.test_client() as c:
            resp = c.post("/api/jobs/pipeline", json={
                "sources": [src1, src2],
                "skip_classify": True,
                "skip_extract_masks": True,
                "skip_regroup": True,
            })
            assert resp.status_code == 200
            data = resp.get_json()
            assert "job_id" in data
    finally:
        import shutil
        shutil.rmtree(src1, ignore_errors=True)
        shutil.rmtree(src2, ignore_errors=True)


def test_pipeline_accepts_skip_classify(setup):
    app, db_path = setup
    with app.test_client() as c:
        resp = c.post("/api/jobs/pipeline", json={
            "collection_id": 1,
            "skip_classify": True,
        })
        assert resp.status_code == 200


def test_pipeline_accepts_preview_max_size(setup):
    app, db_path = setup
    with app.test_client() as c:
        resp = c.post("/api/jobs/pipeline", json={
            "collection_id": 1,
            "preview_max_size": 2560,
        })
        assert resp.status_code == 200
```

**Step 2: Run tests to verify they fail**

Run: `python -m pytest vireo/tests/test_pipeline_api.py -v`
Expected: FAIL — unknown params

**Step 3: Write minimal implementation**

In `vireo/app.py`, modify `api_job_pipeline()` (line ~4522):

1. Accept `sources` list:
```python
        source = body.get("source")
        sources = body.get("sources")
        collection_id = body.get("collection_id")

        if not source and not sources and not collection_id:
            return json_error("source, sources, or collection_id required")

        # Validate all source directories exist
        if sources:
            for s in sources:
                if not os.path.isdir(s):
                    return json_error(f"source directory not found: {s}")
        elif source and not os.path.isdir(source):
            return json_error(f"source directory not found: {source}")
```

2. Pass new params to `PipelineParams`:
```python
        params = PipelineParams(
            collection_id=collection_id,
            source=source,
            sources=sources,
            destination=destination,
            file_types=body.get("file_types", "both"),
            folder_template=body.get("folder_template", "%Y/%m-%d"),
            skip_duplicates=body.get("skip_duplicates", True),
            labels_file=body.get("labels_file"),
            labels_files=body.get("labels_files"),
            model_id=body.get("model_id"),
            reclassify=body.get("reclassify", False),
            skip_classify=body.get("skip_classify", False),
            skip_extract_masks=body.get("skip_extract_masks", False),
            skip_regroup=body.get("skip_regroup", False),
            preview_max_size=body.get("preview_max_size", 1920),
        )
```

3. Update the config dict passed to `runner.start`:
```python
        job_id = runner.start(
            "pipeline", work,
            config={
                "source": source,
                "sources": sources,
                "collection_id": collection_id,
                "skip_classify": params.skip_classify,
                "skip_extract_masks": params.skip_extract_masks,
                "skip_regroup": params.skip_regroup,
            },
            workspace_id=active_ws,
        )
```

**Step 4: Run tests to verify they pass**

Run: `python -m pytest vireo/tests/test_pipeline_api.py -v`
Expected: ALL PASS

**Step 5: Commit**

```bash
git add vireo/app.py vireo/tests/test_pipeline_api.py
git commit -m "feat: accept sources list, skip_classify, preview_max_size in pipeline API"
```

---

### Task 7: Create Lightroom import page

**Files:**
- Create: `vireo/templates/lightroom.html`
- Modify: `vireo/app.py` (add `/lightroom` route)
- Modify: `vireo/templates/_navbar.html` (add nav link)

**Step 1: Create the Lightroom template**

Create `vireo/templates/lightroom.html` by extracting the Lightroom import section from `vireo/templates/import.html` (lines 259-324 for HTML, and the corresponding JS functions: `addCatalog`, `removeCatalog`, `renderCatalogs`, `previewImport`, `executeLrImport`, `lrShowPhase`, `browseForCatalog`).

The template should:
- Extend the same base layout as other pages (use `{% extends "base.html" %}` or include `_navbar.html`)
- Keep the same 3-phase workflow (Select → Preview → Execute)
- Keep all existing JS functions unchanged
- Use the same API endpoints (`/api/import/preview`, `/api/jobs/import`)

Look at existing page templates (e.g., `audit.html`) for the exact pattern.

**Step 2: Add the route in app.py**

After the existing `/import` route (~line 314), add:
```python
    @app.route("/lightroom")
    def lightroom_page():
        return render_template("lightroom.html")
```

**Step 3: Update navbar**

In `vireo/templates/_navbar.html`, replace the Import link:
```html
  <a href="/lightroom">Lightroom</a>
```

Place it logically — after Pipeline or in a tools/utilities section if one exists.

**Step 4: Verify manually**

Run: `python vireo/app.py --db ~/.vireo/vireo.db --port 8080`
Navigate to `/lightroom` — the 3-phase Lightroom import workflow should work identically to before.

**Step 5: Commit**

```bash
git add vireo/templates/lightroom.html vireo/app.py vireo/templates/_navbar.html
git commit -m "feat: move Lightroom import to its own /lightroom page"
```

---

### Task 8: Rebuild pipeline.html with 6 stages

**Files:**
- Modify: `vireo/templates/pipeline.html`

This is the largest task — the Pipeline template gets restructured from 4 stages to 6, with the Source stage enhanced for multi-folder input and new stages for Scan and Previews.

**Step 1: Restructure stage cards**

Replace the current 4-card structure with 6 cards. The existing cards shift numbering:

| Old | New | Name |
|-----|-----|------|
| Card 1 (Source) | Card 1 (Source) | Enhanced with multi-folder |
| — | Card 2 (Scan & Import) | New, no config |
| — | Card 3 (Thumbnails & Previews) | New, preview quality dropdown |
| Card 2 (Classify) | Card 4 (Classify) | Add enable/disable checkbox |
| Card 3 (Extract Features) | Card 5 (Extract Features) | Add enable/disable checkbox |
| Card 4 (Group & Score) | Card 6 (Group & Score) | Add enable/disable checkbox |

**Step 2: Enhance Source card (Card 1)**

Replace the single folder input with a multi-folder input:
```html
<!-- Multi-folder input -->
<div class="form-row">
  <label>Folders / Volumes</label>
  <div style="display:flex; gap:6px;">
    <input id="cfgSourceInput" type="text" placeholder="Type a path or pick a volume..."
           list="volumeList" style="flex:1;">
    <datalist id="volumeList"></datalist>
    <button class="btn btn-sm" onclick="browseForFolder()">Browse</button>
    <button class="btn btn-sm btn-primary" onclick="addSourceFolder()">Add</button>
  </div>
  <div id="sourceFolderList" style="margin-top:8px;"></div>
</div>
```

Add JS for multi-folder management:
```javascript
var _sourceFolders = [];

function addSourceFolder() {
  var val = document.getElementById('cfgSourceInput').value.trim();
  if (!val || _sourceFolders.includes(val)) return;
  _sourceFolders.push(val);
  document.getElementById('cfgSourceInput').value = '';
  renderSourceFolders();
  updateStartButton();
}

function removeSourceFolder(idx) {
  _sourceFolders.splice(idx, 1);
  renderSourceFolders();
  updateStartButton();
}

function renderSourceFolders() {
  var el = document.getElementById('sourceFolderList');
  if (!_sourceFolders.length) { el.innerHTML = ''; return; }
  el.innerHTML = _sourceFolders.map(function(f, i) {
    return '<div class="folder-tag">' + f +
      ' <span class="remove" onclick="removeSourceFolder(' + i + ')">&times;</span></div>';
  }).join('');
}
```

**Step 3: Add Scan & Import card (Card 2)**

```html
<div class="stage-card" id="card-scan">
  <div class="stage-header" onclick="toggleCard('scan')">
    <span class="stage-num" id="badge-scan">2</span>
    <span class="stage-title">Scan & Import</span>
    <span class="stage-summary" id="summary-scan"></span>
    <span class="stage-chevron">&#9656;</span>
  </div>
  <div class="stage-body">
    <p class="stage-desc">Indexes photos, reads EXIF metadata and existing XMP keywords.</p>
    <div class="progress-bar"><div class="progress-fill" id="progress-scan"></div></div>
    <div class="status-msg" id="status-scan"></div>
  </div>
</div>
```

**Step 4: Add Thumbnails & Previews card (Card 3)**

```html
<div class="stage-card" id="card-previews">
  <div class="stage-header" onclick="toggleCard('previews')">
    <span class="stage-num" id="badge-previews">3</span>
    <span class="stage-title">Thumbnails & Previews</span>
    <span class="stage-summary" id="summary-previews"></span>
    <span class="stage-chevron">&#9656;</span>
  </div>
  <div class="stage-body">
    <p class="stage-desc">Generates cached images for fast browsing.</p>
    <div class="form-row">
      <label for="cfgPreviewSize">Preview quality</label>
      <select id="cfgPreviewSize">
        <option value="1280">1280px</option>
        <option value="1920" selected>1920px</option>
        <option value="2560">2560px</option>
        <option value="3840">3840px</option>
        <option value="0">Full resolution</option>
      </select>
    </div>
    <div class="progress-bar"><div class="progress-fill" id="progress-previews"></div></div>
    <div class="status-msg" id="status-previews"></div>
  </div>
</div>
```

**Step 5: Add enable/disable checkboxes to Cards 4-6**

For Classify (Card 4), Extract Features (Card 5), Group & Score (Card 6), add a checkbox in the card header:

```html
<input type="checkbox" id="enableClassify" checked
       onchange="onStageToggle('classify')" onclick="event.stopPropagation()">
```

Add dependency chain JS:
```javascript
function onStageToggle(stage) {
  var chain = ['classify', 'extract', 'regroup'];
  var idx = chain.indexOf(stage);
  var checked = document.getElementById('enable' + stage.charAt(0).toUpperCase() + stage.slice(1)).checked;

  if (!checked) {
    // Uncheck and disable all downstream
    for (var i = idx + 1; i < chain.length; i++) {
      var cb = document.getElementById('enable' + chain[i].charAt(0).toUpperCase() + chain[i].slice(1));
      cb.checked = false;
      cb.disabled = true;
      toggleCardBody(chain[i], false);
    }
  } else {
    // Enable the next downstream checkbox (don't auto-check)
    if (idx + 1 < chain.length) {
      var next = document.getElementById('enable' + chain[idx + 1].charAt(0).toUpperCase() + chain[idx + 1].slice(1));
      next.disabled = false;
    }
  }
  updateStartButton();
}
```

**Step 6: Update `startPipeline()` to send new params**

```javascript
function startPipeline() {
  var body = {};

  // Source
  if (_sourceMode === 'import') {
    if (_sourceFolders.length > 0) {
      body.sources = _sourceFolders;
    }
    if (_copyMode) {
      body.destination = document.getElementById('cfgDestination').value;
      body.folder_template = document.getElementById('cfgFolderTemplate').value || '%Y/%m-%d';
      body.skip_duplicates = document.getElementById('cfgSkipDupes').checked;
    }
    body.file_types = getIngestFileTypes();
  } else {
    body.collection_id = parseInt(document.getElementById('collectionPicker').value);
  }

  // Preview quality
  body.preview_max_size = parseInt(document.getElementById('cfgPreviewSize').value);

  // Optional stages
  var classifyEnabled = document.getElementById('enableClassify').checked;
  var extractEnabled = document.getElementById('enableExtract').checked;
  var regroupEnabled = document.getElementById('enableRegroup').checked;

  body.skip_classify = !classifyEnabled;
  body.skip_extract_masks = !extractEnabled;
  body.skip_regroup = !regroupEnabled;

  if (classifyEnabled) {
    body.model_id = document.getElementById('modelPicker').value;
    body.labels_files = getSelectedLabels();
    body.reclassify = document.getElementById('cfgReclassify').checked;
  }

  // Start pipeline
  safeFetch('/api/jobs/pipeline', { method: 'POST', body: JSON.stringify(body) })
    .then(function(data) { streamPipeline(data.job_id); });
}
```

**Step 7: Update progress streaming to handle 6 stages**

Update the stage-to-card mapping in the progress handler:
```javascript
var stageCardMap = {
  scan: 'scan',
  thumbnails: 'previews',  // thumbnails progress shows on card 3
  previews: 'previews',
  model_loader: 'classify',
  classify: 'classify',
  extract_masks: 'extract',
  regroup: 'regroup'
};
```

**Step 8: Verify manually**

Run app and check:
- 6 cards render correctly
- Multi-folder add/remove works
- Checkboxes enable/disable with dependency chain
- Start pipeline sends correct params

**Step 9: Commit**

```bash
git add vireo/templates/pipeline.html
git commit -m "feat: rebuild pipeline page with 6 stages, multi-folder, and stage toggles"
```

---

### Task 9: Remove Import page

**Files:**
- Delete: `vireo/templates/import.html`
- Modify: `vireo/app.py` (remove `/import` route)
- Modify: `vireo/templates/_navbar.html` (remove Import link)

**Step 1: Remove the `/import` route from app.py**

Remove lines ~314-316:
```python
    @app.route("/import")
    def import_page():
        return render_template("import.html")
```

**Step 2: Remove Import link from navbar**

In `_navbar.html`, remove:
```html
  <a href="/import">Import</a>
```

**Step 3: Delete import.html**

```bash
git rm vireo/templates/import.html
```

**Step 4: Check for any remaining references to `/import`**

Search the codebase for links to `/import` and update them to point to `/pipeline`:
```bash
grep -r '"/import"' vireo/templates/ vireo/app.py
grep -r "'/import'" vireo/templates/ vireo/app.py
```

Update any found references.

**Step 5: Run all tests**

Run: `python -m pytest tests/test_workspaces.py vireo/tests/test_db.py vireo/tests/test_app.py vireo/tests/test_photos_api.py vireo/tests/test_edits_api.py vireo/tests/test_jobs_api.py vireo/tests/test_darktable_api.py vireo/tests/test_config.py vireo/tests/test_pipeline_api.py vireo/tests/test_pipeline_job.py -v`
Expected: ALL PASS

**Step 6: Commit**

```bash
git add -A
git commit -m "feat: remove Import page — Pipeline is now the single entry point"
```

---

### Task 10: Add "Send to Pipeline" link in audit panel

**Files:**
- Modify: `vireo/templates/audit.html`

**Step 1: Add a "Send to Pipeline" button**

In the Untracked tab's action bar (line ~91 of `audit.html`), add a button after "Import All":

```html
<button class="btn btn-secondary" id="sendToPipelineBtn" onclick="sendToPipeline()" style="display:none;">Send to Pipeline</button>
```

**Step 2: Add the JS function**

```javascript
function sendToPipeline() {
  // Collect unique folder paths from untracked files
  var folders = [];
  untrackedData.forEach(function(u) {
    var dir = u.path.substring(0, u.path.lastIndexOf('/'));
    if (dir && folders.indexOf(dir) === -1) folders.push(dir);
  });
  // Navigate to pipeline with folders as query params
  var params = folders.map(function(f) { return 'folder=' + encodeURIComponent(f); }).join('&');
  window.location.href = '/pipeline?' + params;
}
```

**Step 3: Show the button when untracked files are found**

In the `runUntrackedCheck()` function, after setting the badge count, show the button:
```javascript
document.getElementById('sendToPipelineBtn').style.display =
  untrackedData.length > 0 ? '' : 'none';
```

**Step 4: Handle query params in pipeline.html**

In `pipeline.html`'s `initPipelinePage()`, read folder query params and pre-fill:
```javascript
// Pre-fill folders from query params (e.g., from audit panel)
var urlParams = new URLSearchParams(window.location.search);
var prefillFolders = urlParams.getAll('folder');
if (prefillFolders.length > 0) {
  _sourceFolders = prefillFolders;
  renderSourceFolders();
  selectSourceMode('import');
  updateStartButton();
}
```

**Step 5: Verify manually**

1. Go to Audit → Untracked tab
2. Run check, find untracked files
3. Click "Send to Pipeline"
4. Verify Pipeline page opens with folders pre-filled in Source card

**Step 6: Commit**

```bash
git add vireo/templates/audit.html vireo/templates/pipeline.html
git commit -m "feat: add Send to Pipeline link in audit panel's untracked tab"
```

---

### Task 11: Final integration test and cleanup

**Step 1: Run full test suite**

Run: `python -m pytest tests/test_workspaces.py vireo/tests/test_db.py vireo/tests/test_app.py vireo/tests/test_photos_api.py vireo/tests/test_edits_api.py vireo/tests/test_jobs_api.py vireo/tests/test_darktable_api.py vireo/tests/test_config.py vireo/tests/test_pipeline_api.py vireo/tests/test_pipeline_job.py -v`
Expected: ALL PASS

**Step 2: Check for dead code**

Search for any remaining references to removed features:
```bash
grep -r 'wizStart\|wizAddFolder\|startScanJob\|startThumbJob\|startPreviewJob' vireo/
grep -r 'import_page\|import\.html' vireo/
```

Remove any dead references found.

**Step 3: Verify navigation flow**

Manually verify:
1. `/pipeline` loads with 6 stage cards
2. Multi-folder source input works
3. Stage checkboxes enable/disable with dependency chain
4. Pipeline runs with stages 1-3 only (skip classify/extract/regroup)
5. Pipeline runs with all 6 stages
6. `/lightroom` loads and works
7. Audit → Untracked → "Send to Pipeline" navigates correctly
8. No link to `/import` exists anywhere

**Step 4: Final commit if any cleanup was needed**

```bash
git add -A
git commit -m "chore: clean up dead import references"
```
