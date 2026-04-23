# New Images → Pipeline Source Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Wire the "N new images detected" banner into the pipeline so that clicking "Create a pipeline" produces a pipeline scoped to exactly the files the banner announced, and make "New images" a first-class Stage 1 pipeline source.

**Architecture:** A new `new_image_snapshots` workspace-scoped table captures the absolute file paths of new images at click time. Two new endpoints hang off `/api/workspaces/active/new-images/…` for creating and reading snapshots. `PipelineParams` gains a `source_snapshot_id` field; `run_pipeline_job()` scopes its scan stage to the snapshot's parent directories and filters downstream stages to the resolved photo IDs at the scan-to-classify seam. Frontend changes to `pipeline.html` (add third source card) and `_navbar.html` (banner becomes POST + redirect).

**Tech Stack:** Python 3, Flask, SQLite (no ORM — raw cursor), Jinja2 templates, vanilla JS, pytest, Playwright for E2E.

**Design doc:** `docs/plans/2026-04-22-new-images-pipeline-design.md`

---

## Task 1: Schema — add snapshot tables

**Files:**
- Modify: `vireo/db.py` (the `_create_tables()` / `executescript()` block ending around line 298)
- Test: `vireo/tests/test_db.py`

**Step 1: Write the failing test**

Add to `vireo/tests/test_db.py`:

```python
def test_new_image_snapshots_tables_exist(tmp_path):
    db = Database(str(tmp_path / "test.db"))
    tables = {
        r["name"]
        for r in db.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }
    assert "new_image_snapshots" in tables
    assert "new_image_snapshot_files" in tables
```

**Step 2: Run the test — verify it fails**

Run: `python -m pytest vireo/tests/test_db.py::test_new_image_snapshots_tables_exist -v`
Expected: FAIL (tables don't exist).

**Step 3: Add tables to the `executescript()` block**

Append to the initial-schema `executescript()` in `_create_tables()` (before the closing `""")` around line 298):

```sql
CREATE TABLE IF NOT EXISTS new_image_snapshots (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  workspace_id INTEGER NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
  created_at TEXT NOT NULL,
  file_count INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS new_image_snapshot_files (
  snapshot_id INTEGER NOT NULL REFERENCES new_image_snapshots(id) ON DELETE CASCADE,
  file_path TEXT NOT NULL,
  PRIMARY KEY (snapshot_id, file_path)
);

CREATE INDEX IF NOT EXISTS idx_new_image_snapshots_ws
  ON new_image_snapshots(workspace_id);
```

**Step 4: Run the test — verify it passes**

Run: `python -m pytest vireo/tests/test_db.py::test_new_image_snapshots_tables_exist -v`
Expected: PASS.

**Step 5: Commit**

```bash
git add vireo/db.py vireo/tests/test_db.py
git commit -m "db: add new_image_snapshots tables"
```

---

## Task 2: `Database.create_new_images_snapshot` + `get_new_images_snapshot`

**Files:**
- Modify: `vireo/db.py`
- Test: `vireo/tests/test_db.py`

**Step 1: Write the failing tests**

```python
def test_create_and_get_new_images_snapshot(tmp_path):
    db = Database(str(tmp_path / "test.db"))
    ws_id = db._active_workspace_id
    paths = ["/tmp/a/IMG_001.JPG", "/tmp/b/IMG_002.JPG"]
    snap_id = db.create_new_images_snapshot(paths)
    assert isinstance(snap_id, int)

    snap = db.get_new_images_snapshot(snap_id)
    assert snap is not None
    assert snap["file_count"] == 2
    assert snap["workspace_id"] == ws_id
    assert sorted(snap["file_paths"]) == sorted(paths)


def test_get_snapshot_from_different_workspace_returns_none(tmp_path):
    db = Database(str(tmp_path / "test.db"))
    other_ws = db.create_workspace("Other")
    paths = ["/tmp/a/IMG_001.JPG"]
    snap_id = db.create_new_images_snapshot(paths)
    db.set_active_workspace(other_ws)
    assert db.get_new_images_snapshot(snap_id) is None


def test_snapshot_deleted_with_workspace(tmp_path):
    db = Database(str(tmp_path / "test.db"))
    throwaway_ws = db.create_workspace("Throwaway")
    db.set_active_workspace(throwaway_ws)
    snap_id = db.create_new_images_snapshot(["/tmp/a.jpg"])
    db.delete_workspace(throwaway_ws)
    row = db.conn.execute(
        "SELECT id FROM new_image_snapshots WHERE id = ?", (snap_id,)
    ).fetchone()
    assert row is None


def test_create_snapshot_empty_paths(tmp_path):
    db = Database(str(tmp_path / "test.db"))
    snap_id = db.create_new_images_snapshot([])
    snap = db.get_new_images_snapshot(snap_id)
    assert snap["file_count"] == 0
    assert snap["file_paths"] == []
```

**Step 2: Verify they fail**

Run: `python -m pytest vireo/tests/test_db.py -k snapshot -v`
Expected: FAIL (methods don't exist).

**Step 3: Implement in `vireo/db.py`**

Add near the other workspace-scoped helpers (e.g. below `add_collection`):

```python
def create_new_images_snapshot(self, file_paths):
    """Persist a snapshot of new-image file paths for the active workspace.

    Returns the new snapshot id. An empty path list is allowed — the caller
    decides how to handle zero-file snapshots (the pipeline short-circuits).
    """
    ws_id = self._ws_id()
    cur = self.conn.execute(
        "INSERT INTO new_image_snapshots (workspace_id, created_at, file_count) "
        "VALUES (?, datetime('now'), ?)",
        (ws_id, len(file_paths)),
    )
    snap_id = cur.lastrowid
    if file_paths:
        # De-duplicate in case the caller passed repeats; PK would reject them
        # but sending a clean set keeps executemany cheap.
        unique_paths = sorted(set(file_paths))
        self.conn.executemany(
            "INSERT INTO new_image_snapshot_files (snapshot_id, file_path) VALUES (?, ?)",
            [(snap_id, p) for p in unique_paths],
        )
    self.conn.commit()
    return snap_id


def get_new_images_snapshot(self, snapshot_id):
    """Return snapshot metadata + file paths, or None if not found / cross-workspace.

    Isolation: a snapshot created in workspace A is invisible when workspace B
    is active. Callers treat None as 'expired / gone'.
    """
    row = self.conn.execute(
        "SELECT id, workspace_id, created_at, file_count "
        "FROM new_image_snapshots WHERE id = ? AND workspace_id = ?",
        (snapshot_id, self._ws_id()),
    ).fetchone()
    if row is None:
        return None
    paths = [
        r["file_path"]
        for r in self.conn.execute(
            "SELECT file_path FROM new_image_snapshot_files WHERE snapshot_id = ? "
            "ORDER BY file_path",
            (snapshot_id,),
        ).fetchall()
    ]
    return {
        "id": row["id"],
        "workspace_id": row["workspace_id"],
        "created_at": row["created_at"],
        "file_count": row["file_count"],
        "file_paths": paths,
    }
```

**Step 4: Verify tests pass**

Run: `python -m pytest vireo/tests/test_db.py -k snapshot -v`
Expected: PASS (4 tests).

**Step 5: Commit**

```bash
git add vireo/db.py vireo/tests/test_db.py
git commit -m "db: add create/get new-images snapshot helpers"
```

---

## Task 3: Extend new-images detection to return full path list

**Files:**
- Modify: `vireo/new_images.py`
- Modify: `vireo/db.py` (the `get_new_images_for_workspace` wrapper, if any filtering applies)
- Test: `vireo/tests/test_new_images.py`

The existing `count_new_images_for_workspace` has a `sample_limit=5` and only returns up to 5 paths. For snapshot creation we need every path, not a sample.

**Step 1: Write the failing test**

Add to `vireo/tests/test_new_images.py`:

```python
def test_count_new_images_returns_all_paths_when_sample_limit_is_none(tmp_path):
    # Set up a workspace with 10 new files on disk.
    db = Database(str(tmp_path / "test.db"))
    folder = tmp_path / "photos"
    folder.mkdir()
    db.add_folder(str(folder))
    for i in range(10):
        _touch_image(folder / f"IMG_{i:03d}.JPG")
    result = count_new_images_for_workspace(
        db, db._active_workspace_id, sample_limit=None
    )
    assert result["new_count"] == 10
    assert len(result["sample"]) == 10
```

**Step 2: Verify fail**

Run: `python -m pytest vireo/tests/test_new_images.py::test_count_new_images_returns_all_paths_when_sample_limit_is_none -v`
Expected: FAIL (`sample` is capped at 5).

**Step 3: Modify `count_new_images_for_workspace`**

In `vireo/new_images.py`, change the `sample_limit` gate so `None` means unlimited:

```python
def count_new_images_for_workspace(db, workspace_id, sample_limit=5):
    ...
    for root in roots:
        ...
        for dirpath, _dirnames, filenames in os.walk(root_path):
            for name in filenames:
                ...
                full = os.path.join(dirpath, name)
                if full in known:
                    continue
                root_new += 1
                if sample_limit is None or len(sample) < sample_limit:
                    sample.append(full)
        ...
```

Only the `if sample_limit is None or len(sample) < sample_limit:` line changes.

**Step 4: Verify test passes, and existing tests still pass**

Run: `python -m pytest vireo/tests/test_new_images.py -v`
Expected: PASS.

**Step 5: Commit**

```bash
git add vireo/new_images.py vireo/tests/test_new_images.py
git commit -m "new-images: allow sample_limit=None to return all paths"
```

---

## Task 4: `POST /api/workspaces/active/new-images/snapshot` endpoint

**Files:**
- Modify: `vireo/app.py` (near the existing `/api/workspaces/active/new-images` route around line 2046)
- Test: `vireo/tests/test_new_images_api.py`

**Step 1: Write the failing tests**

```python
def test_post_snapshot_creates_row_with_current_new_images(app_and_db, tmp_path):
    app, db = app_and_db
    folder = tmp_path / "photos"
    folder.mkdir()
    db.add_folder(str(folder))
    _touch_image(folder / "IMG_001.JPG")
    _touch_image(folder / "IMG_002.JPG")
    # Bust the in-process new-images cache so the POST sees fresh disk state.
    get_shared_cache().clear()

    with app.test_client() as client:
        resp = client.post("/api/workspaces/active/new-images/snapshot")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["file_count"] == 2
        assert isinstance(data["snapshot_id"], int)
        assert str(folder) in data["folders"]

    snap = db.get_new_images_snapshot(data["snapshot_id"])
    assert snap["file_count"] == 2


def test_post_snapshot_zero_new_images_returns_200(app_and_db):
    app, db = app_and_db
    with app.test_client() as client:
        resp = client.post("/api/workspaces/active/new-images/snapshot")
        assert resp.status_code == 200
        assert resp.get_json()["file_count"] == 0
```

Import `get_shared_cache` from `new_images` at the top of the test file.

**Step 2: Verify fail**

Run: `python -m pytest vireo/tests/test_new_images_api.py -k snapshot -v`
Expected: FAIL (route doesn't exist → 404).

**Step 3: Implement in `vireo/app.py`**

Add below the existing `api_workspace_new_images` handler:

```python
@app.route("/api/workspaces/active/new-images/snapshot", methods=["POST"])
def api_workspace_new_images_snapshot_create():
    db = _get_db()
    ws_id = db._active_workspace_id
    if ws_id is None:
        return jsonify({"error": "no active workspace"}), 400
    from new_images import count_new_images_for_workspace
    result = count_new_images_for_workspace(db, ws_id, sample_limit=None)
    file_paths = list(result["sample"])
    snap_id = db.create_new_images_snapshot(file_paths)
    folders = sorted({os.path.dirname(p) for p in file_paths})
    return jsonify({
        "snapshot_id": snap_id,
        "file_count": len(file_paths),
        "folders": folders,
    })
```

(Confirm `os` is already imported; `vireo/app.py` imports it near the top.)

**Step 4: Verify tests pass**

Run: `python -m pytest vireo/tests/test_new_images_api.py -k snapshot -v`
Expected: PASS (2 tests).

**Step 5: Commit**

```bash
git add vireo/app.py vireo/tests/test_new_images_api.py
git commit -m "api: POST /api/workspaces/active/new-images/snapshot"
```

---

## Task 5: `GET /api/workspaces/active/new-images/snapshot/<id>` endpoint

**Files:**
- Modify: `vireo/app.py`
- Test: `vireo/tests/test_new_images_api.py`

**Step 1: Write the failing tests**

```python
def test_get_snapshot_returns_summary(app_and_db, tmp_path):
    app, db = app_and_db
    folder = tmp_path / "photos"
    folder.mkdir()
    db.add_folder(str(folder))
    _touch_image(folder / "IMG_001.JPG")
    get_shared_cache().clear()

    with app.test_client() as client:
        post = client.post("/api/workspaces/active/new-images/snapshot")
        snap_id = post.get_json()["snapshot_id"]

        resp = client.get(f"/api/workspaces/active/new-images/snapshot/{snap_id}")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["file_count"] == 1
        assert data["folder_paths"] == [str(folder)]
        assert data["files_sample"][0].endswith("IMG_001.JPG")


def test_get_snapshot_unknown_id_returns_404(app_and_db):
    app, _ = app_and_db
    with app.test_client() as client:
        resp = client.get("/api/workspaces/active/new-images/snapshot/99999")
        assert resp.status_code == 404


def test_get_snapshot_cross_workspace_returns_404(app_and_db):
    app, db = app_and_db
    snap_id = db.create_new_images_snapshot(["/tmp/a.jpg"])
    other = db.create_workspace("Other")
    db.set_active_workspace(other)
    with app.test_client() as client:
        resp = client.get(f"/api/workspaces/active/new-images/snapshot/{snap_id}")
        assert resp.status_code == 404
```

**Step 2: Verify fail**

Run: `python -m pytest vireo/tests/test_new_images_api.py -k get_snapshot -v`
Expected: FAIL.

**Step 3: Implement**

Add in `vireo/app.py` below the POST handler:

```python
@app.route(
    "/api/workspaces/active/new-images/snapshot/<int:snapshot_id>",
    methods=["GET"],
)
def api_workspace_new_images_snapshot_get(snapshot_id):
    db = _get_db()
    if db._active_workspace_id is None:
        abort(404)
    snap = db.get_new_images_snapshot(snapshot_id)
    if snap is None:
        abort(404)
    paths = snap["file_paths"]
    folder_paths = sorted({os.path.dirname(p) for p in paths})
    files_sample = paths[:5]
    return jsonify({
        "file_count": snap["file_count"],
        "folder_paths": folder_paths,
        "files_sample": files_sample,
    })
```

**Step 4: Verify tests pass**

Run: `python -m pytest vireo/tests/test_new_images_api.py -k get_snapshot -v`
Expected: PASS (3 tests).

**Step 5: Commit**

```bash
git add vireo/app.py vireo/tests/test_new_images_api.py
git commit -m "api: GET /api/workspaces/active/new-images/snapshot/<id>"
```

---

## Task 6: Add `source_snapshot_id` to `PipelineParams` and scope the scan stage

**Files:**
- Modify: `vireo/pipeline_job.py`
- Test: `vireo/tests/test_pipeline_job.py`

**Step 1: Write the failing test**

```python
def test_pipeline_with_snapshot_scans_only_snapshot_folders(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "test.db"))
    a = tmp_path / "folderA"
    b = tmp_path / "folderB"
    a.mkdir(); b.mkdir()
    db.add_folder(str(a))
    db.add_folder(str(b))
    (a / "IMG_001.JPG").write_bytes(_tiny_jpeg_bytes())
    (b / "IMG_002.JPG").write_bytes(_tiny_jpeg_bytes())
    snap_id = db.create_new_images_snapshot([str(a / "IMG_001.JPG")])

    scanned_dirs = []
    real_scan = pipeline_job.do_scan
    def spy_scan(root, *a, **kw):
        scanned_dirs.append(root)
        return real_scan(root, *a, **kw)
    monkeypatch.setattr(pipeline_job, "do_scan", spy_scan)

    # Run pipeline with snapshot source; just the scan stage is enough here.
    params = PipelineParams(source_snapshot_id=snap_id, skip_classify=True,
                            skip_extract_masks=True, skip_regroup=True)
    runner = make_test_runner()  # existing test helper
    job = runner.create_job("pipeline")
    run_pipeline_job(job, runner, str(tmp_path / "test.db"),
                     db._active_workspace_id, params)

    assert str(a) in scanned_dirs
    assert str(b) not in scanned_dirs
```

(Use whatever existing `make_test_runner` / job-creation helper the test file already provides — mirror neighboring tests.)

**Step 2: Verify fail**

Run: `python -m pytest vireo/tests/test_pipeline_job.py::test_pipeline_with_snapshot_scans_only_snapshot_folders -v`
Expected: FAIL (`source_snapshot_id` is not a valid param).

**Step 3: Extend `PipelineParams`**

In `vireo/pipeline_job.py`, the `@dataclass` near line 28:

```python
@dataclass
class PipelineParams:
    collection_id: int | None = None
    source: str | None = None
    sources: list | None = None
    source_snapshot_id: int | None = None  # <-- add
    destination: str | None = None
    ...
```

**Step 4: Implement snapshot-scoped scan**

In `run_pipeline_job`, before the existing scan-stage setup (around the `scanner_stage()` definition), load the snapshot once and derive the list of folders to scan:

```python
snapshot_paths: list[str] | None = None
if params.source_snapshot_id is not None:
    db_ro = Database(db_path)
    db_ro.set_active_workspace(workspace_id)
    snap = db_ro.get_new_images_snapshot(params.source_snapshot_id)
    db_ro.close()
    if snap is None:
        raise ValueError(f"snapshot {params.source_snapshot_id} not found")
    snapshot_paths = snap["file_paths"]
    scan_roots = sorted({os.path.dirname(p) for p in snapshot_paths}) or []
    # Override whatever source/sources was passed so the scan walks only these dirs.
    params.sources = scan_roots
    params.source = None
    params.collection_id = None
```

The existing scan-stage code walks `params.sources` (or `params.source`), so nothing else changes there.

**Step 5: Verify test passes**

Run: `python -m pytest vireo/tests/test_pipeline_job.py::test_pipeline_with_snapshot_scans_only_snapshot_folders -v`
Expected: PASS.

**Step 6: Commit**

```bash
git add vireo/pipeline_job.py vireo/tests/test_pipeline_job.py
git commit -m "pipeline: scope scan stage to snapshot folders"
```

---

## Task 7: Filter downstream stages to snapshot photo IDs

**Files:**
- Modify: `vireo/pipeline_job.py`
- Test: `vireo/tests/test_pipeline_job.py`

After the scan stage completes, we resolve snapshot file paths to photo IDs and constrain downstream stages to that set. This is the snapshot-guarantee enforcement point.

**Step 1: Write the failing test**

```python
def test_pipeline_snapshot_excludes_late_arriving_files(tmp_path, monkeypatch):
    """Files added to the folder after the snapshot get scanned but NOT classified."""
    db = Database(str(tmp_path / "test.db"))
    folder = tmp_path / "photos"
    folder.mkdir()
    db.add_folder(str(folder))
    (folder / "IMG_early.JPG").write_bytes(_tiny_jpeg_bytes())
    snap_id = db.create_new_images_snapshot([str(folder / "IMG_early.JPG")])

    # Simulate a file arriving between snapshot and run:
    (folder / "IMG_late.JPG").write_bytes(_tiny_jpeg_bytes())

    classified_photo_ids = []
    def spy_classify(photo_ids, *a, **kw):
        classified_photo_ids.extend(photo_ids)
    monkeypatch.setattr(pipeline_job, "_classify_photos", spy_classify)

    params = PipelineParams(source_snapshot_id=snap_id,
                            skip_extract_masks=True, skip_regroup=True)
    runner = make_test_runner()
    job = runner.create_job("pipeline")
    run_pipeline_job(job, runner, str(tmp_path / "test.db"),
                     db._active_workspace_id, params)

    classified_names = {
        db.conn.execute(
            "SELECT filename FROM photos WHERE id = ?", (pid,)
        ).fetchone()["filename"]
        for pid in classified_photo_ids
    }
    assert "IMG_early.JPG" in classified_names
    assert "IMG_late.JPG" not in classified_names
```

(If the exact classification entry point in `pipeline_job.py` is named differently, adjust the `monkeypatch.setattr` target. Read the file to confirm.)

**Step 2: Verify fail**

Run: `python -m pytest vireo/tests/test_pipeline_job.py::test_pipeline_snapshot_excludes_late_arriving_files -v`
Expected: FAIL (no filter yet — both files would be classified).

**Step 3: Resolve snapshot paths → photo IDs after scan, apply filter downstream**

After the scanner completes in `run_pipeline_job`, if `snapshot_paths` is set, resolve them:

```python
snapshot_photo_ids: set[int] | None = None
if snapshot_paths is not None:
    db_resolver = Database(db_path)
    db_resolver.set_active_workspace(workspace_id)
    rows = db_resolver.conn.execute(f"""
        SELECT p.id
        FROM photos p
        JOIN folders f ON f.id = p.folder_id
        WHERE (f.path || '/' || p.filename) IN ({",".join("?" * len(snapshot_paths))})
    """, snapshot_paths).fetchall()
    snapshot_photo_ids = {r["id"] for r in rows}
    db_resolver.close()
    missing = len(snapshot_paths) - len(snapshot_photo_ids)
    if missing:
        logging.info(
            "pipeline: snapshot %s had %d files, %d ingested, %d missing on disk",
            params.source_snapshot_id, len(snapshot_paths),
            len(snapshot_photo_ids), missing,
        )
```

Then, at each downstream stage's entry point, constrain its photo-id set:

```python
if snapshot_photo_ids is not None:
    photo_ids = [pid for pid in photo_ids if pid in snapshot_photo_ids]
```

(Exact placement depends on how each stage selects its input set in the current `run_pipeline_job`. When in doubt, confirm by reading each stage's photo-selection code and apply the filter uniformly.)

**Step 4: Verify test passes**

Run: `python -m pytest vireo/tests/test_pipeline_job.py::test_pipeline_snapshot_excludes_late_arriving_files -v`
Expected: PASS.

**Step 5: Commit**

```bash
git add vireo/pipeline_job.py vireo/tests/test_pipeline_job.py
git commit -m "pipeline: filter downstream stages to snapshot photo ids"
```

---

## Task 8: `/api/jobs/pipeline` accepts `source_snapshot_id`

**Files:**
- Modify: `vireo/app.py` (the `api_job_pipeline` handler around line 6593)
- Test: `vireo/tests/test_pipeline_api.py` (or wherever `/api/jobs/pipeline` is tested)

**Step 1: Write the failing test**

```python
def test_post_pipeline_accepts_source_snapshot_id(app_and_db, tmp_path, monkeypatch):
    app, db = app_and_db
    folder = tmp_path / "photos"
    folder.mkdir()
    db.add_folder(str(folder))
    _touch_image(folder / "IMG_001.JPG")
    snap_id = db.create_new_images_snapshot([str(folder / "IMG_001.JPG")])

    captured = {}
    def spy_run(job, runner, db_path, ws_id, params):
        captured["snap_id"] = params.source_snapshot_id
    monkeypatch.setattr("pipeline_job.run_pipeline_job", spy_run)

    with app.test_client() as client:
        resp = client.post("/api/jobs/pipeline", json={"source_snapshot_id": snap_id})
        assert resp.status_code == 200

    # JobRunner runs in a thread — wait briefly for spy_run to fire.
    wait_for(lambda: "snap_id" in captured, timeout=2.0)
    assert captured["snap_id"] == snap_id
```

**Step 2: Verify fail**

Run: `python -m pytest vireo/tests/test_pipeline_api.py -k snapshot -v`
Expected: FAIL.

**Step 3: Add `source_snapshot_id` to the handler**

In `vireo/app.py`'s `api_job_pipeline`, where it builds `PipelineParams`:

```python
source_snapshot_id = body.get("source_snapshot_id")
...
params = PipelineParams(
    ...
    source_snapshot_id=source_snapshot_id,
    ...
)
```

**Step 4: Verify test passes**

Run: `python -m pytest vireo/tests/test_pipeline_api.py -k snapshot -v`
Expected: PASS.

**Step 5: Commit**

```bash
git add vireo/app.py vireo/tests/test_pipeline_api.py
git commit -m "api: pipeline endpoint accepts source_snapshot_id"
```

---

## Task 9: Pipeline page — add "New images" Stage 1 source card

**Files:**
- Modify: `vireo/templates/pipeline.html` (Stage 1 source area around lines 475–557)

This task is UI-only, no backend changes. Because frontend changes aren't cleanly unit-testable from pytest, verify manually + Playwright in Task 11.

**Step 1: Add the third source card**

After the "Use Existing Collection" card (around line 557), add:

```html
<div class="source-or" id="sourceOrNewImages" style="display:none;">OR</div>
<div class="source-option" id="sourceOptionNewImages" style="display:none;">
  <div class="source-option-header" onclick="selectSourceMode('new_images')">
    <input type="radio" id="radioNewImages" name="sourceMode" value="new_images">
    <label for="radioNewImages">
      <strong id="newImagesCardTitle">New images</strong>
      <span id="newImagesCardSubtitle" class="muted"></span>
    </label>
  </div>
  <div class="source-option-body" id="sourceNewImagesBody" style="display:none;">
    <ul id="newImagesFolderList" class="folder-list"></ul>
  </div>
</div>
```

**Step 2: Add JS probe + deep-link handling**

Inline JS (mirror the patterns used by the other source options). Near the page-init block:

```javascript
let newImagesSnapshotId = null;

async function initNewImagesCard() {
  const params = new URLSearchParams(window.location.search);
  const deepLinkId = params.get("new_images");
  if (deepLinkId) {
    try {
      const r = await fetch(`/api/workspaces/active/new-images/snapshot/${deepLinkId}`);
      if (!r.ok) throw new Error("snapshot expired");
      const snap = await r.json();
      newImagesSnapshotId = parseInt(deepLinkId, 10);
      renderNewImagesCard(snap.file_count, snap.folder_paths);
      selectSourceMode("new_images");
    } catch (e) {
      showToast("That snapshot has expired — please try again from the banner.");
      await probeNewImagesCard();
    }
  } else {
    await probeNewImagesCard();
  }
}

async function probeNewImagesCard() {
  const r = await fetch("/api/workspaces/active/new-images");
  const data = await r.json();
  if ((data.new_count || 0) > 0) {
    const folders = (data.per_root || []).map(pr => pr.path);
    renderNewImagesCard(data.new_count, folders);
  }
}

function renderNewImagesCard(count, folders) {
  const card = document.getElementById("sourceOptionNewImages");
  const orSep = document.getElementById("sourceOrNewImages");
  card.style.display = "";
  orSep.style.display = "";
  document.getElementById("newImagesCardSubtitle").textContent =
    ` — ${count} new image${count === 1 ? "" : "s"} in ${folders.length} folder${folders.length === 1 ? "" : "s"}`;
  const list = document.getElementById("newImagesFolderList");
  list.innerHTML = "";
  for (const f of folders) {
    const li = document.createElement("li");
    li.textContent = f;
    list.appendChild(li);
  }
}

document.addEventListener("DOMContentLoaded", initNewImagesCard);
```

**Step 3: Extend `selectSourceMode` to capture snapshot on select**

In the existing `selectSourceMode(mode)` function, add a branch:

```javascript
if (mode === "new_images") {
  document.getElementById("sourceNewImagesBody").style.display = "";
  if (newImagesSnapshotId === null) {
    // User is selecting the card directly (no deep-link) — freeze the list now.
    const r = await fetch("/api/workspaces/active/new-images/snapshot", {method: "POST"});
    const data = await r.json();
    newImagesSnapshotId = data.snapshot_id;
  }
} else {
  document.getElementById("sourceNewImagesBody").style.display = "none";
}
```

(Make `selectSourceMode` `async` if it isn't already.)

**Step 4: Include `source_snapshot_id` in the pipeline submit payload**

Where the existing submit code builds the JSON body for `POST /api/jobs/pipeline`, add:

```javascript
if (currentSourceMode === "new_images") {
  payload.source_snapshot_id = newImagesSnapshotId;
  delete payload.source;
  delete payload.sources;
  delete payload.file_types;
}
```

**Step 5: Manual smoke test**

```bash
python vireo/app.py --db ~/.vireo/vireo.db --port 8080
```

- Drop a file into a registered folder, reload `/pipeline`. The "New images" card should appear. Click it — confirm subtitle shows correct count and folders render in the body.
- Navigate to `/pipeline?new_images=<valid_id>` — card should auto-select and show the snapshot's counts.
- Navigate to `/pipeline?new_images=999999` — toast "snapshot has expired", card falls back to runtime probe.

**Step 6: Commit**

```bash
git add vireo/templates/pipeline.html
git commit -m "ui: add 'New images' source card to pipeline Stage 1"
```

---

## Task 10: Banner — POST snapshot, then redirect

**Files:**
- Modify: `vireo/templates/_navbar.html` (banner around line 1167–1172)

**Step 1: Convert the anchor to a button + JS**

Replace the existing `<a href="/pipeline">Create a pipeline</a>` with:

```html
<button type="button" class="banner-cta" onclick="createPipelineFromNewImages()">
  Create a pipeline
</button>
```

**Step 2: Add the handler near the other banner JS**

```javascript
async function createPipelineFromNewImages() {
  try {
    const r = await fetch("/api/workspaces/active/new-images/snapshot", {method: "POST"});
    if (!r.ok) throw new Error("snapshot failed");
    const data = await r.json();
    window.location.href = `/pipeline?new_images=${data.snapshot_id}`;
  } catch (e) {
    // Fall back to the existing behavior — blank pipeline wizard.
    window.location.href = "/pipeline";
  }
}
```

**Step 3: Manual smoke test**

- Drop a file in a registered folder, reload any Vireo page.
- Banner appears. Click "Create a pipeline".
- Browser lands on `/pipeline?new_images=<id>` with the new-images card pre-selected.

**Step 4: Commit**

```bash
git add vireo/templates/_navbar.html
git commit -m "ui: banner POSTs snapshot before opening pipeline"
```

---

## Task 11: E2E Playwright test

**Files:**
- Create or extend: an existing Playwright test under `tests/` (follow the user-first-testing convention — see `docs/plans/2026-04-16-user-first-testing-design.md` if unclear).

**Step 1: Write the test**

End-to-end flow:

1. Start Vireo against a fresh temp DB + temp photo folder.
2. Register the folder via the UI or API.
3. Drop a JPEG into the folder.
4. Reload the page. Assert banner appears with "1 new image".
5. Click "Create a pipeline".
6. Assert URL is `/pipeline?new_images=<id>`.
7. Assert the "New images" source card is selected and shows "1 new image".
8. Complete the wizard with minimal options, submit.
9. Wait for job completion. Assert the photo appears in the browse grid.

**Step 2: Run the test**

(Exact command depends on the project's Playwright runner — mirror neighboring E2E tests.)

**Step 3: Commit**

```bash
git add tests/test_new_images_pipeline_e2e.py
git commit -m "test: e2e for new-images banner → pipeline flow"
```

---

## Final verification

**Step 1: Run the full test suite per CLAUDE.md:**

```bash
python -m pytest tests/test_workspaces.py vireo/tests/test_db.py vireo/tests/test_app.py vireo/tests/test_photos_api.py vireo/tests/test_edits_api.py vireo/tests/test_jobs_api.py vireo/tests/test_darktable_api.py vireo/tests/test_config.py vireo/tests/test_new_images.py vireo/tests/test_new_images_api.py vireo/tests/test_pipeline_job.py -v
```

Expected: all pass.

**Step 2: Update PR description**

The PR (#625) was opened for the design doc. Push these commits to the same branch; the PR re-reviews automatically.

**Step 3: Manual end-to-end check**

Drop a file, click the banner's "Create a pipeline", step through the wizard, verify the photo is ingested and classified. This is the user's real-world acceptance test — skipping would mean we shipped without driving it once.
