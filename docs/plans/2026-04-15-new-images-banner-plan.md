# New Images Detected Banner — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Show a global banner — "N new images detected in your registered folders. [Create a pipeline]" — whenever a workspace's mapped folders have image files on disk that aren't in the DB. Banner links to `/pipeline`, which already has a Scan & Import stage.

**Architecture:** Pure helper in a new `vireo/new_images.py` module walks mapped workspace roots (not every nested folder), filters to image extensions, and diffs by absolute path against `photos JOIN folders`. Results are cached per-workspace with a 5-minute TTL and invalidated on scan completion. Frontend mirrors the existing Missing Folders banner pattern in `_navbar.html`.

**Tech Stack:** Python 3 (`os.walk`, `time.monotonic`), Flask route, SQLite JOIN, vanilla JS `fetch()`, Jinja2 template include.

**Design reference:** `docs/plans/2026-04-15-new-images-banner-design.md`.

---

### Task 1: Test — `count_new_images_for_workspace` detects new files

**Files:**
- Create: `vireo/tests/test_new_images.py`

**Step 1: Write the failing test**

```python
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest
from PIL import Image
from db import Database


def _touch_image(path):
    """Create a real 1x1 JPEG at path."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    Image.new("RGB", (1, 1), "white").save(path, "JPEG")


@pytest.fixture
def db_with_workspace(tmp_path):
    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    return db, ws_id, tmp_path


def test_count_new_images_detects_unscanned_files(db_with_workspace):
    db, ws_id, tmp_path = db_with_workspace
    root = tmp_path / "USA2026"
    _touch_image(str(root / "IMG_0001.JPG"))
    _touch_image(str(root / "IMG_0002.JPG"))
    db.add_folder(str(root), name="USA2026")

    from new_images import count_new_images_for_workspace
    result = count_new_images_for_workspace(db, ws_id)

    assert result["new_count"] == 2
    assert len(result["per_root"]) == 1
    assert result["per_root"][0]["new_count"] == 2
    assert len(result["sample"]) == 2
```

**Step 2: Run test — verify it fails**

```
cd /Users/julius/git/vireo/.worktrees/new-images-banner-plan
python -m pytest vireo/tests/test_new_images.py::test_count_new_images_detects_unscanned_files -v
```

Expected: `ModuleNotFoundError: No module named 'new_images'`

**Step 3: Create minimal module**

Create `vireo/new_images.py`:

```python
"""Detect image files present on disk but not yet ingested into a workspace."""
import os
from pathlib import Path

from image_loader import SUPPORTED_EXTENSIONS


def _known_paths_for_workspace(db, workspace_id):
    """Return the set of absolute paths of photos already ingested into the workspace."""
    rows = db.conn.execute(
        """SELECT f.path AS folder_path, p.filename
           FROM photos p
           JOIN folders f ON f.id = p.folder_id
           JOIN workspace_folders wf ON wf.folder_id = f.id
           WHERE wf.workspace_id = ?""",
        (workspace_id,),
    ).fetchall()
    return {os.path.join(r["folder_path"], r["filename"]) for r in rows}


def _mapped_roots(db, workspace_id):
    """Return the workspace's mapped roots — folders whose parent is not also linked.

    Skips folders marked 'missing'.
    """
    rows = db.conn.execute(
        """SELECT f.id, f.path, f.parent_id, f.status
           FROM folders f
           JOIN workspace_folders wf ON wf.folder_id = f.id
           WHERE wf.workspace_id = ? AND f.status = 'ok'""",
        (workspace_id,),
    ).fetchall()
    linked_ids = {r["id"] for r in rows}
    return [
        {"id": r["id"], "path": r["path"]}
        for r in rows
        if r["parent_id"] is None or r["parent_id"] not in linked_ids
    ]


def count_new_images_for_workspace(db, workspace_id, sample_limit=5):
    """Return {'new_count': int, 'per_root': [...], 'sample': [abs_path, ...]}.

    Walks each mapped root recursively, collects image files, and diffs against
    the set of photo paths already ingested into the workspace.
    """
    known = _known_paths_for_workspace(db, workspace_id)
    roots = _mapped_roots(db, workspace_id)

    per_root = []
    sample = []
    total = 0
    for root in roots:
        root_path = root["path"]
        if not os.path.isdir(root_path):
            per_root.append({"folder_id": root["id"], "path": root_path, "new_count": 0})
            continue

        root_new = 0
        for dirpath, _dirnames, filenames in os.walk(root_path):
            for name in filenames:
                ext = Path(name).suffix.lower()
                if ext not in SUPPORTED_EXTENSIONS:
                    continue
                full = os.path.join(dirpath, name)
                if full in known:
                    continue
                root_new += 1
                if len(sample) < sample_limit:
                    sample.append(full)

        total += root_new
        per_root.append({"folder_id": root["id"], "path": root_path, "new_count": root_new})

    return {"new_count": total, "per_root": per_root, "sample": sample}
```

**Step 4: Run test — verify it passes**

```
python -m pytest vireo/tests/test_new_images.py -v
```

Expected: `test_count_new_images_detects_unscanned_files PASSED`

**Step 5: Commit**

```bash
git add vireo/new_images.py vireo/tests/test_new_images.py
git commit -m "feat(new-images): helper detects unscanned images per workspace"
```

---

### Task 2: Test — no double-counting across auto-registered subfolders

Scanner auto-links every discovered subfolder to `workspace_folders` (`vireo/db.py:964`). The helper must walk only mapped roots, not every linked folder.

**Files:**
- Modify: `vireo/tests/test_new_images.py` (add test)

**Step 1: Write the failing test**

Append to `vireo/tests/test_new_images.py`:

```python
def test_count_new_images_no_double_counting_with_nested_linked_folders(db_with_workspace):
    """Nested subfolders auto-linked to workspace_folders must not cause double-counting."""
    db, ws_id, tmp_path = db_with_workspace
    root = tmp_path / "USA2026"
    nested = root / "day1"
    deep = nested / "raw"
    _touch_image(str(deep / "IMG_0001.JPG"))  # one unscanned file, three levels deep

    # Register root AND the intermediate dirs as workspace_folders (mirrors what
    # the scanner's Database.add_folder does for every discovered subdirectory).
    root_id = db.add_folder(str(root), name="USA2026")
    nested_id = db.add_folder(str(nested), name="day1", parent_id=root_id)
    db.add_folder(str(deep), name="raw", parent_id=nested_id)

    from new_images import count_new_images_for_workspace
    result = count_new_images_for_workspace(db, ws_id)

    assert result["new_count"] == 1, (
        f"Expected 1 new image, got {result['new_count']}. "
        f"per_root={result['per_root']}"
    )
    # Only the top-level root should appear in per_root.
    assert len(result["per_root"]) == 1
    assert result["per_root"][0]["path"] == str(root)
```

**Step 2: Run — verify passes** (implementation from Task 1 already handles this):

```
python -m pytest vireo/tests/test_new_images.py::test_count_new_images_no_double_counting_with_nested_linked_folders -v
```

Expected: PASS. If it fails, the `_mapped_roots` filter is wrong.

**Step 3: Commit**

```bash
git add vireo/tests/test_new_images.py
git commit -m "test(new-images): guard against double-counting nested subfolders"
```

---

### Task 3: Test — basename collisions across subdirs are not conflated

Two different subdirectories can both contain `IMG_0001.JPG`. Absolute-path identity must treat them independently.

**Files:**
- Modify: `vireo/tests/test_new_images.py`

**Step 1: Write the failing test**

```python
def test_count_new_images_basename_collision_across_subdirs(db_with_workspace):
    db, ws_id, tmp_path = db_with_workspace
    root = tmp_path / "shoot"
    _touch_image(str(root / "day1" / "IMG_0001.JPG"))
    _touch_image(str(root / "day2" / "IMG_0001.JPG"))
    root_id = db.add_folder(str(root), name="shoot")

    # Ingest only day1's IMG_0001.JPG.
    day1_id = db.add_folder(str(root / "day1"), name="day1", parent_id=root_id)
    db.add_photo(
        folder_id=day1_id, filename="IMG_0001.JPG", extension=".JPG",
        file_size=1, file_mtime=0.0,
    )

    from new_images import count_new_images_for_workspace
    result = count_new_images_for_workspace(db, ws_id)

    assert result["new_count"] == 1  # day2's IMG_0001.JPG is the only new one
    assert any("day2" in s for s in result["sample"])
```

**Step 2: Run — verify**

```
python -m pytest vireo/tests/test_new_images.py::test_count_new_images_basename_collision_across_subdirs -v
```

Expected: PASS.

**Step 3: Commit**

```bash
git add vireo/tests/test_new_images.py
git commit -m "test(new-images): basename collisions distinguished by absolute path"
```

---

### Task 4: Test — cache with TTL and invalidation

**Files:**
- Create: `vireo/tests/test_new_images_cache.py`

**Step 1: Write the failing test**

```python
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest

from new_images import NewImagesCache


def test_cache_returns_cached_value_within_ttl():
    cache = NewImagesCache(ttl_seconds=60)
    cache.set(workspace_id=1, result={"new_count": 5})
    assert cache.get(1) == {"new_count": 5}


def test_cache_expires_after_ttl(monkeypatch):
    clock = [1000.0]
    monkeypatch.setattr("new_images.time.monotonic", lambda: clock[0])
    cache = NewImagesCache(ttl_seconds=60)
    cache.set(workspace_id=1, result={"new_count": 5})
    clock[0] += 61
    assert cache.get(1) is None


def test_cache_invalidate_by_folder_ids_clears_all_workspaces_linking_those_folders():
    """When folder F is scanned, every workspace linked to F must have its cache cleared."""
    cache = NewImagesCache(ttl_seconds=60)
    cache.set(workspace_id=1, result={"new_count": 5})
    cache.set(workspace_id=2, result={"new_count": 7})

    # Caller supplies the mapping: folder_id -> list of workspace_ids linked to it.
    cache.invalidate_workspaces([1, 2])

    assert cache.get(1) is None
    assert cache.get(2) is None


def test_cache_invalidate_workspace_does_not_clear_others():
    cache = NewImagesCache(ttl_seconds=60)
    cache.set(workspace_id=1, result={"new_count": 5})
    cache.set(workspace_id=2, result={"new_count": 7})
    cache.invalidate_workspaces([1])
    assert cache.get(1) is None
    assert cache.get(2) == {"new_count": 7}
```

**Step 2: Run — verify fails**

```
python -m pytest vireo/tests/test_new_images_cache.py -v
```

Expected: `ImportError: cannot import name 'NewImagesCache'`

**Step 3: Implement cache**

Append to `vireo/new_images.py`:

```python
import time
import threading


class NewImagesCache:
    """In-memory per-workspace cache with a TTL ceiling.

    Thread-safe. Invalidation takes a list of workspace_ids (computed by the
    caller from the set of folder_ids touched by a scan).
    """

    def __init__(self, ttl_seconds=300):
        self._ttl = ttl_seconds
        self._entries = {}  # workspace_id -> (result_dict, set_at_monotonic)
        self._lock = threading.Lock()

    def get(self, workspace_id):
        with self._lock:
            entry = self._entries.get(workspace_id)
            if entry is None:
                return None
            result, set_at = entry
            if time.monotonic() - set_at > self._ttl:
                del self._entries[workspace_id]
                return None
            return result

    def set(self, workspace_id, result):
        with self._lock:
            self._entries[workspace_id] = (result, time.monotonic())

    def invalidate_workspaces(self, workspace_ids):
        with self._lock:
            for wid in workspace_ids:
                self._entries.pop(wid, None)

    def clear(self):
        with self._lock:
            self._entries.clear()
```

**Step 4: Run — verify passes**

```
python -m pytest vireo/tests/test_new_images_cache.py -v
```

Expected: 4 passed.

**Step 5: Commit**

```bash
git add vireo/new_images.py vireo/tests/test_new_images_cache.py
git commit -m "feat(new-images): cache with TTL and workspace-scoped invalidation"
```

---

### Task 5: DB method — `get_new_images_for_workspace` with caching

Stitch helper + cache onto the `Database` instance so the rest of the app has one entry point.

**Files:**
- Modify: `vireo/db.py` (add method)
- Modify: `vireo/tests/test_new_images.py` (integration test)

**Step 1: Write the failing test**

Append to `vireo/tests/test_new_images.py`:

```python
def test_db_get_new_images_for_workspace_caches_result(db_with_workspace, monkeypatch):
    db, ws_id, tmp_path = db_with_workspace
    root = tmp_path / "shoot"
    _touch_image(str(root / "IMG_0001.JPG"))
    db.add_folder(str(root), name="shoot")

    calls = [0]
    import new_images
    real = new_images.count_new_images_for_workspace

    def counting_wrapper(*args, **kwargs):
        calls[0] += 1
        return real(*args, **kwargs)

    monkeypatch.setattr(new_images, "count_new_images_for_workspace", counting_wrapper)

    r1 = db.get_new_images_for_workspace(ws_id)
    r2 = db.get_new_images_for_workspace(ws_id)
    assert r1 == r2
    assert calls[0] == 1  # second call served from cache
```

**Step 2: Run — verify fails**

```
python -m pytest vireo/tests/test_new_images.py::test_db_get_new_images_for_workspace_caches_result -v
```

Expected: `AttributeError: 'Database' object has no attribute 'get_new_images_for_workspace'`

**Step 3: Implement**

In `vireo/db.py`, add near the other workspace methods:

```python
def get_new_images_for_workspace(self, workspace_id):
    """Return new-images result for workspace, using cache when fresh."""
    import new_images
    cached = self._new_images_cache.get(workspace_id)
    if cached is not None:
        return cached
    result = new_images.count_new_images_for_workspace(self, workspace_id)
    self._new_images_cache.set(workspace_id, result)
    return result

def invalidate_new_images_cache_for_folders(self, folder_ids):
    """Clear cache for every workspace linked to any of the given folder_ids."""
    if not folder_ids:
        return
    placeholders = ",".join("?" * len(folder_ids))
    rows = self.conn.execute(
        f"SELECT DISTINCT workspace_id FROM workspace_folders "
        f"WHERE folder_id IN ({placeholders})",
        tuple(folder_ids),
    ).fetchall()
    ws_ids = [r["workspace_id"] for r in rows]
    self._new_images_cache.invalidate_workspaces(ws_ids)
```

In `Database.__init__`, initialize the cache. Add this line alongside other instance state setup (grep for `self._active_workspace_id` to find a good spot):

```python
from new_images import NewImagesCache
self._new_images_cache = NewImagesCache()
```

**Step 4: Run — verify passes**

```
python -m pytest vireo/tests/test_new_images.py -v
```

Expected: all tests pass.

**Step 5: Commit**

```bash
git add vireo/db.py vireo/tests/test_new_images.py
git commit -m "feat(db): get_new_images_for_workspace with cache integration"
```

---

### Task 6: Test — scan completion invalidates cache for shared-folder workspaces

**Files:**
- Modify: `vireo/tests/test_new_images.py`

**Step 1: Write the failing test**

```python
def test_invalidate_cache_for_shared_folder_across_workspaces(tmp_path):
    """If workspaces A and B both link folder F, a scan of F must clear both caches."""
    db = Database(str(tmp_path / "test.db"))
    ws_a = db.ensure_default_workspace()
    ws_b = db.create_workspace("B")

    # Link the same folder into both workspaces.
    db.set_active_workspace(ws_a)
    root = tmp_path / "shared"
    _touch_image(str(root / "IMG.JPG"))
    root_id = db.add_folder(str(root), name="shared")
    db.set_active_workspace(ws_b)
    db.add_workspace_folder(ws_b, root_id)

    # Prime both caches.
    db.set_active_workspace(ws_a)
    db.get_new_images_for_workspace(ws_a)
    db.get_new_images_for_workspace(ws_b)
    assert db._new_images_cache.get(ws_a) is not None
    assert db._new_images_cache.get(ws_b) is not None

    # Scan completes for folder root_id.
    db.invalidate_new_images_cache_for_folders([root_id])

    assert db._new_images_cache.get(ws_a) is None
    assert db._new_images_cache.get(ws_b) is None
```

**Step 2: Run — verify passes** (already implemented in Task 5):

```
python -m pytest vireo/tests/test_new_images.py::test_invalidate_cache_for_shared_folder_across_workspaces -v
```

Expected: PASS.

**Step 3: Commit**

```bash
git add vireo/tests/test_new_images.py
git commit -m "test(new-images): cross-workspace invalidation on shared folder scan"
```

---

### Task 7: Wire scan-job completion to invalidate cache

The scan job is dispatched from `vireo/app.py`. Find the scan worker, capture touched `folder_id`s, call `db.invalidate_new_images_cache_for_folders(...)` at the end of a successful scan.

**Files:**
- Modify: `vireo/app.py` (grep for `runner.update_step(job["id"], "scan", status="completed"` — there are two sites around lines 4258 and 4846; both are scan completion paths that know the scanned root/folder)
- Modify: `vireo/tests/test_app.py` (or `test_jobs_api.py` — add an end-to-end test)

**Step 1: Trace the scan worker**

```
grep -n "scan.*status=.completed" vireo/app.py
```

Identify the two completion sites. In each, the local scope has access to the scanned root path. You can look up `folder_id` via `db.conn.execute("SELECT id FROM folders WHERE path = ?", (root,)).fetchone()["id"]` just before marking the step completed — but the scanner itself is a better source of truth. Check whether `scan()` in `vireo/scanner.py:302` returns or yields the set of folder ids it touched; if not, collect them via the `photo_callback` hook that's already passed in, or query `SELECT id FROM folders WHERE path LIKE root || '%'` at completion time.

**Step 2: Add invalidation call**

At each scan-completion site, after `runner.update_step(..., status="completed", ...)`, add:

```python
# Invalidate new-images cache for every workspace linked to any scanned folder.
touched_ids = [r["id"] for r in db.conn.execute(
    "SELECT id FROM folders WHERE path = ? OR path LIKE ?",
    (root, root.rstrip("/") + "/%"),
).fetchall()]
db.invalidate_new_images_cache_for_folders(touched_ids)
```

**Step 3: Add integration test**

Append to `vireo/tests/test_new_images.py`:

```python
def test_scan_job_invalidates_cache(db_with_workspace):
    """After a successful scan, the cached new_count must be re-computed on next read."""
    db, ws_id, tmp_path = db_with_workspace
    root = tmp_path / "shoot"
    _touch_image(str(root / "a.JPG"))
    root_id = db.add_folder(str(root), name="shoot")

    # Prime cache with current state (1 new).
    r1 = db.get_new_images_for_workspace(ws_id)
    assert r1["new_count"] == 1

    # Simulate a scan by inserting the photo row and invalidating.
    db.add_photo(folder_id=root_id, filename="a.JPG", extension=".JPG",
                 file_size=1, file_mtime=0.0)
    db.invalidate_new_images_cache_for_folders([root_id])

    r2 = db.get_new_images_for_workspace(ws_id)
    assert r2["new_count"] == 0
```

**Step 4: Run — verify all tests pass**

```
python -m pytest vireo/tests/test_new_images.py vireo/tests/test_new_images_cache.py -v
```

Expected: all pass.

**Step 5: Commit**

```bash
git add vireo/app.py vireo/tests/test_new_images.py
git commit -m "feat(scan): invalidate new-images cache on scan completion"
```

---

### Task 8: API route — `GET /api/workspace/new-images`

**Files:**
- Modify: `vireo/app.py`
- Create: `vireo/tests/test_new_images_api.py`

**Step 1: Write the failing test**

```python
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest
from PIL import Image


def _touch_image(path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    Image.new("RGB", (1, 1), "white").save(path, "JPEG")


@pytest.fixture
def app_and_db(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    import config as cfg
    from app import create_app
    from db import Database

    cfg.CONFIG_PATH = str(tmp_path / "config.json")
    db_path = str(tmp_path / "test.db")
    thumb_dir = str(tmp_path / "thumbs")
    os.makedirs(thumb_dir)

    db = Database(db_path)
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)

    app = create_app(db_path=db_path, thumb_cache_dir=thumb_dir)
    return app, db, ws_id, tmp_path


def test_api_new_images_reports_unscanned_files(app_and_db):
    app, db, ws_id, tmp_path = app_and_db
    root = tmp_path / "shoot"
    _touch_image(str(root / "IMG.JPG"))
    db.add_folder(str(root), name="shoot")

    client = app.test_client()
    resp = client.get("/api/workspace/new-images")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["new_count"] == 1
    assert len(data["per_root"]) == 1


def test_api_new_images_zero_when_fully_ingested(app_and_db):
    app, db, ws_id, tmp_path = app_and_db
    root = tmp_path / "shoot"
    _touch_image(str(root / "IMG.JPG"))
    fid = db.add_folder(str(root), name="shoot")
    db.add_photo(folder_id=fid, filename="IMG.JPG", extension=".JPG",
                 file_size=1, file_mtime=0.0)

    client = app.test_client()
    resp = client.get("/api/workspace/new-images")
    assert resp.get_json()["new_count"] == 0
```

**Step 2: Run — verify fails**

```
python -m pytest vireo/tests/test_new_images_api.py -v
```

Expected: 404 (route not found).

**Step 3: Add the route**

In `vireo/app.py`, alongside other `@app.route("/api/workspace/...")` or `@app.route("/api/folders/...")` blocks:

```python
@app.route("/api/workspace/new-images")
def api_workspace_new_images():
    db = _get_db()
    ws_id = db._active_workspace_id
    if ws_id is None:
        return jsonify({"new_count": 0, "per_root": [], "sample": []})
    return jsonify(db.get_new_images_for_workspace(ws_id))
```

**Step 4: Run — verify passes**

```
python -m pytest vireo/tests/test_new_images_api.py -v
```

Expected: 2 passed.

**Step 5: Commit**

```bash
git add vireo/app.py vireo/tests/test_new_images_api.py
git commit -m "feat(api): GET /api/workspace/new-images"
```

---

### Task 9: Banner HTML + CSS in `_navbar.html`

Mirror the existing Missing Folders banner exactly so visual treatment stays consistent.

**Files:**
- Modify: `vireo/templates/_navbar.html`

**Step 1: Add the banner markup**

Find the Missing Folders banner at `_navbar.html:1091`:

```html
<!-- Missing Folders Banner -->
<div class="missing-folders-banner" id="missingFoldersBanner" style="display:none;">
  ...
</div>
```

Immediately after it, add:

```html
<!-- New Images Banner -->
<div class="new-images-banner" id="newImagesBanner" style="display:none;">
  <span id="newImagesMsg"></span>
  <a href="/pipeline">Create a pipeline</a>
  <button class="banner-dismiss" onclick="dismissNewImagesBanner()">&times;</button>
</div>
```

**Step 2: Add CSS**

Find the `.missing-folders-banner` CSS rule at `_navbar.html:935` and the stacked rules around `_navbar.html:3218`. Add a sibling block that reuses the same visual style but with an info-color accent (grep existing CSS vars for something like `--info` or `--accent`):

```css
/* ---------- New Images Banner ---------- */
.new-images-banner {
  position: fixed;
  top: 48px; /* same as .missing-folders-banner; stack by margin-top if both visible */
  left: 0; right: 0;
  background: var(--info-bg, #153D55);
  color: var(--text-primary, #E6F1F5);
  padding: 8px 16px;
  display: none;  /* JS toggles to flex */
  align-items: center;
  gap: 12px;
  font-size: 13px;
  z-index: 90;
  border-bottom: 1px solid var(--border-primary, #14374E);
}
.new-images-banner a {
  color: var(--accent, #24E5CA);
  text-decoration: none;
}
.new-images-banner a:hover { text-decoration: underline; }
```

If both banners are visible at once, stack them: have the new-images banner shift down by banner-height when missingFoldersBanner is displayed. Simplest is a small JS helper (next task) that computes top offset, or CSS rule `.missing-folders-banner ~ .new-images-banner { top: 88px; }` if the DOM order permits adjacency.

**Step 3: Verify template renders**

Start the app and load any page:

```
python vireo/app.py --db ~/.vireo/vireo.db --port 8080
```

Open http://localhost:8080/. Open DevTools, confirm `#newImagesBanner` is in the DOM with `display: none`. No console errors.

**Step 4: Commit**

```bash
git add vireo/templates/_navbar.html
git commit -m "feat(ui): add New Images banner markup + styles"
```

---

### Task 10: Banner JS — fetch, render, dismiss, auto-refresh

**Files:**
- Modify: `vireo/templates/_navbar.html`

**Step 1: Add JS after the Missing Folders banner script block (`_navbar.html:3217`)**

```html
<script>
/* ---------- New Images Banner ---------- */
const NEW_IMAGES_DISMISS_KEY = 'newImagesDismissedWorkspace';

async function checkNewImages() {
  try {
    const resp = await fetch('/api/workspace/new-images');
    if (!resp.ok) return;
    const data = await resp.json();
    const banner = document.getElementById('newImagesBanner');
    const msg = document.getElementById('newImagesMsg');
    if (data.new_count > 0 && !isNewImagesDismissed()) {
      const s = data.new_count === 1 ? '' : 's';
      msg.textContent = `${data.new_count} new image${s} detected in your registered folders.`;
      banner.style.display = 'flex';
    } else {
      banner.style.display = 'none';
    }
  } catch (e) { /* ignore */ }
}

function isNewImagesDismissed() {
  const activeWs = window.__activeWorkspaceId || '';
  const dismissed = sessionStorage.getItem(NEW_IMAGES_DISMISS_KEY) || '';
  return dismissed === String(activeWs);
}

function dismissNewImagesBanner() {
  document.getElementById('newImagesBanner').style.display = 'none';
  const activeWs = window.__activeWorkspaceId || '';
  sessionStorage.setItem(NEW_IMAGES_DISMISS_KEY, String(activeWs));
}

// Run on page load and every 60s (TTL is 5 min; more frequent poll lets
// the banner appear promptly after an external Finder import).
checkNewImages();
setInterval(checkNewImages, 60000);

// Re-check whenever the workspace switches. Look for the existing
// workspace-switch event handler and add a call to checkNewImages().
document.addEventListener('vireo:workspace-switched', () => {
  sessionStorage.removeItem(NEW_IMAGES_DISMISS_KEY);
  checkNewImages();
});
</script>
```

**Step 2: Verify `window.__activeWorkspaceId` and the `vireo:workspace-switched` event**

Grep existing code:

```
grep -n "activeWorkspaceId\|workspace-switched\|switchWorkspace" vireo/templates/_navbar.html
```

If these names don't exist, adapt: find where the workspace switcher updates the UI and either dispatch an event there or call `checkNewImages()` directly.

**Step 3: Manual smoke test**

Start the app, switch to a workspace with known unscanned files:

```
python vireo/app.py --db ~/.vireo/vireo.db --port 8080
```

- Confirm the banner appears within ~1 second.
- Click the `×` — banner hides and stays hidden on page reload (sessionStorage).
- Switch to a different workspace and back — banner re-appears (dismissal cleared on workspace switch).
- Click "Create a pipeline" — lands on `/pipeline`.

**Step 4: Commit**

```bash
git add vireo/templates/_navbar.html
git commit -m "feat(ui): New Images banner fetch, render, dismiss, auto-refresh"
```

---

### Task 11: Full test suite + PR

**Step 1: Run the project's required test set (from project CLAUDE.md)**

```
python -m pytest tests/test_workspaces.py vireo/tests/test_db.py vireo/tests/test_app.py vireo/tests/test_photos_api.py vireo/tests/test_edits_api.py vireo/tests/test_jobs_api.py vireo/tests/test_darktable_api.py vireo/tests/test_config.py vireo/tests/test_new_images.py vireo/tests/test_new_images_cache.py vireo/tests/test_new_images_api.py -v
```

Expected: all pass.

**Step 2: Push branch, open PR**

```
git push -u origin claude/new-images-banner-plan
gh pr create --title 'feat: "new images detected" workspace banner' --body "..."
```

Body should reference `docs/plans/2026-04-15-new-images-banner-design.md` as the design doc and include the summary and test results.

**Step 3: Address any review feedback on the same branch**

Per project CLAUDE.md, push fixes directly to this branch; the PR agent will re-review.

---

## Notes for the implementing engineer

- **No backwards compatibility needed** — this is new state. Don't guard with feature flags.
- **The `sample` field** is currently only used for debug logging; don't render it in the banner UI yet.
- **5-minute TTL is a starting point** — if Codex flags it or testing shows problems, adjust the constant in `NewImagesCache(ttl_seconds=...)`.
- **Per-workspace dismissal** — the banner's dismissal state is keyed by active workspace id, not global. Dismissing in workspace A doesn't hide it in workspace B.
- **Unreachable folders are already handled** by the Missing Folders banner; this feature filters them out via `folders.status = 'ok'` in `_mapped_roots`.
