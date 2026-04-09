# Duplicate Preview During Import — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Show which photos are duplicates in the import preview grid, progressively, before the user clicks import.

**Architecture:** New POST SSE endpoint `/api/import/check-duplicates` hashes files in batches and streams results. Frontend calls it after preview loads, dims duplicate thumbnails progressively, shows a badge and count. Results are cached in JS so the skip-duplicates checkbox can toggle visuals without re-hashing.

**Tech Stack:** Flask SSE response, `compute_file_hash()` from `scanner.py`, vanilla JS `fetch()` + `ReadableStream`

---

### Task 1: Backend — check-duplicates SSE endpoint test

**Files:**
- Create: `vireo/tests/test_check_duplicates.py`

**Step 1: Write the failing test**

```python
import os
import sys
import json

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pytest
from PIL import Image
from db import Database


@pytest.fixture
def app_and_db(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    import config as cfg
    from app import create_app

    cfg.CONFIG_PATH = str(tmp_path / "config.json")
    db_path = str(tmp_path / "test.db")
    thumb_dir = str(tmp_path / "thumbs")
    os.makedirs(thumb_dir)

    db = Database(db_path)
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    fid = db.add_folder(str(tmp_path / "library"), name="library")

    app = create_app(db_path=db_path, thumb_cache_dir=thumb_dir)
    return app, db, fid


def parse_sse_events(response_data):
    """Parse SSE events from raw response bytes."""
    text = response_data.decode("utf-8")
    events = []
    for block in text.split("\n\n"):
        for line in block.strip().split("\n"):
            if line.startswith("data: "):
                events.append(json.loads(line[6:]))
    return events


def test_check_duplicates_marks_known_hashes(app_and_db, tmp_path):
    """Files whose hash exists in DB are reported as duplicates."""
    app, db, fid = app_and_db

    # Create an image that exists in the "library" (scanned, hash in DB)
    library_dir = tmp_path / "library"
    library_dir.mkdir(exist_ok=True)
    img = Image.new("RGB", (50, 50), color="red")
    img.save(str(library_dir / "existing.jpg"))

    # Scan to populate file_hash
    from scanner import scan
    scan(str(library_dir), db)

    # Create source folder with a duplicate and a new file
    source = tmp_path / "source"
    source.mkdir()
    img.save(str(source / "duplicate.jpg"))  # Same content = same hash
    Image.new("RGB", (50, 50), color="blue").save(str(source / "unique.jpg"))

    client = app.test_client()
    resp = client.post("/api/import/check-duplicates", json={
        "paths": [str(source / "duplicate.jpg"), str(source / "unique.jpg")],
    })
    assert resp.status_code == 200

    events = parse_sse_events(resp.data)
    # Find the done event
    done = [e for e in events if e.get("done")]
    assert len(done) == 1
    assert done[0]["duplicate_count"] == 1

    # Collect all duplicate paths across batch events
    all_duplicates = []
    for e in events:
        if "duplicates" in e:
            all_duplicates.extend(e["duplicates"])
    assert str(source / "duplicate.jpg") in all_duplicates
    assert str(source / "unique.jpg") not in all_duplicates


def test_check_duplicates_no_paths(app_and_db):
    """Returns error when no paths provided."""
    app, _, _ = app_and_db
    client = app.test_client()
    resp = client.post("/api/import/check-duplicates", json={"paths": []})
    assert resp.status_code == 400


def test_check_duplicates_all_new(app_and_db, tmp_path):
    """When no files match DB hashes, duplicate_count is 0."""
    app, db, fid = app_and_db

    source = tmp_path / "source"
    source.mkdir()
    Image.new("RGB", (50, 50), color="green").save(str(source / "new1.jpg"))
    Image.new("RGB", (50, 50), color="yellow").save(str(source / "new2.jpg"))

    client = app.test_client()
    resp = client.post("/api/import/check-duplicates", json={
        "paths": [str(source / "new1.jpg"), str(source / "new2.jpg")],
    })

    events = parse_sse_events(resp.data)
    done = [e for e in events if e.get("done")]
    assert done[0]["duplicate_count"] == 0


def test_check_duplicates_missing_file_skipped(app_and_db, tmp_path):
    """Missing files are skipped without crashing."""
    app, db, fid = app_and_db

    source = tmp_path / "source"
    source.mkdir()
    Image.new("RGB", (50, 50), color="green").save(str(source / "real.jpg"))

    client = app.test_client()
    resp = client.post("/api/import/check-duplicates", json={
        "paths": [str(source / "real.jpg"), str(source / "gone.jpg")],
    })
    assert resp.status_code == 200

    events = parse_sse_events(resp.data)
    done = [e for e in events if e.get("done")]
    assert len(done) == 1
    assert done[0]["checked"] == 2  # Both counted as checked
```

**Step 2: Run test to verify it fails**

Run: `python -m pytest vireo/tests/test_check_duplicates.py -v`
Expected: FAIL — route `/api/import/check-duplicates` does not exist (404)

**Step 3: Commit**

```bash
git add vireo/tests/test_check_duplicates.py
git commit -m "test: add tests for check-duplicates SSE endpoint"
```

---

### Task 2: Backend — implement check-duplicates endpoint

**Files:**
- Modify: `vireo/app.py` (add new route near the other `/api/import/*` routes, around line 2777)

**Step 1: Implement the endpoint**

Add after the `api_import_folder_preview` route (after line 2777):

```python
@app.route("/api/import/check-duplicates", methods=["POST"])
def api_import_check_duplicates():
    """Stream duplicate detection results via SSE.

    Accepts {"paths": [...]}, hashes each file, checks against DB,
    and streams batches of duplicate paths back to the client.
    """
    body = request.get_json(silent=True) or {}
    paths = body.get("paths", [])
    if not paths:
        return json_error("paths required", 400)

    from scanner import compute_file_hash

    db = _get_db()
    rows = db.conn.execute(
        "SELECT file_hash FROM photos WHERE file_hash IS NOT NULL"
    ).fetchall()
    known_hashes = {r["file_hash"] for r in rows}

    BATCH_SIZE = 20

    def generate():
        total = len(paths)
        duplicate_count = 0
        batch_duplicates = []
        checked = 0

        for path in paths:
            checked += 1
            try:
                file_hash = compute_file_hash(path)
                if file_hash in known_hashes:
                    batch_duplicates.append(path)
                    duplicate_count += 1
            except (OSError, IOError):
                pass  # Skip unreadable/missing files

            if checked % BATCH_SIZE == 0 or checked == total:
                yield f"data: {json.dumps({'duplicates': batch_duplicates, 'checked': checked, 'total': total})}\n\n"
                batch_duplicates = []

        yield f"data: {json.dumps({'done': True, 'duplicate_count': duplicate_count, 'checked': total, 'total': total})}\n\n"

    return Response(
        generate(),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
```

**Step 2: Run tests to verify they pass**

Run: `python -m pytest vireo/tests/test_check_duplicates.py -v`
Expected: All 4 tests PASS

**Step 3: Commit**

```bash
git add vireo/app.py
git commit -m "feat: add check-duplicates SSE endpoint for preview duplicate detection"
```

---

### Task 3: Frontend — CSS for duplicate badge

**Files:**
- Modify: `vireo/templates/pipeline.html` (CSS section, around line 434)

**Step 1: Add badge styles**

After the existing `.preview-thumb.duplicate` rule (line 435-436), add:

```css
.preview-thumb .duplicate-badge {
  position: absolute;
  bottom: 4px;
  left: 4px;
  background: rgba(0,0,0,0.7);
  color: #ccc;
  font-size: 9px;
  font-weight: 600;
  padding: 1px 5px;
  border-radius: 3px;
  letter-spacing: 0.5px;
  pointer-events: none;
}
```

**Step 2: Commit**

```bash
git add vireo/templates/pipeline.html
git commit -m "feat: add CSS for duplicate badge overlay on preview thumbnails"
```

---

### Task 4: Frontend — progressive duplicate check after preview loads

**Files:**
- Modify: `vireo/templates/pipeline.html` (JS section)

**Step 1: Add JS state variable and check function**

Near the top of the `<script>` section, where other state variables like `_previewData` and `_previewSelected` are declared, add:

```javascript
var _duplicateResults = {};   // path -> true for duplicates (cache)
var _duplicateCheckAbort = null;  // AbortController for in-flight check
```

Add the duplicate check function (after `renderPreview` function):

```javascript
function checkForDuplicates() {
  // Abort any in-flight check
  if (_duplicateCheckAbort) _duplicateCheckAbort.abort();

  if (!_previewData || !_previewData.files.length) return;
  if (!document.getElementById('chkSkipDuplicates').checked) return;

  _duplicateResults = {};
  _duplicateCheckAbort = new AbortController();

  var paths = _previewData.files.map(function(f) { return f.path; });

  fetch('/api/import/check-duplicates', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ paths: paths }),
    signal: _duplicateCheckAbort.signal,
  }).then(function(response) {
    var reader = response.body.getReader();
    var decoder = new TextDecoder();
    var buffer = '';

    function read() {
      reader.read().then(function(result) {
        if (result.done) return;
        buffer += decoder.decode(result.value, { stream: true });
        var parts = buffer.split('\n\n');
        buffer = parts.pop();
        parts.forEach(function(part) {
          var match = part.match(/^data: (.+)$/m);
          if (!match) return;
          var data = JSON.parse(match[1]);
          if (data.duplicates) {
            data.duplicates.forEach(function(p) {
              _duplicateResults[p] = true;
            });
            applyDuplicateVisuals();
          }
          updateDuplicateSummary(data);
        });
        read();
      }).catch(function() {});
    }
    read();
  }).catch(function() {});
}

function applyDuplicateVisuals() {
  var skipChecked = document.getElementById('chkSkipDuplicates').checked;
  var grid = document.getElementById('previewGrid');
  var thumbs = grid.querySelectorAll('.preview-thumb');
  thumbs.forEach(function(el) {
    var path = el.dataset.path;
    var isDup = _duplicateResults[path];
    if (isDup && skipChecked) {
      el.classList.add('duplicate');
      if (!el.querySelector('.duplicate-badge')) {
        var badge = document.createElement('span');
        badge.className = 'duplicate-badge';
        badge.textContent = 'DUPLICATE';
        el.appendChild(badge);
      }
    } else {
      el.classList.remove('duplicate');
      var badge = el.querySelector('.duplicate-badge');
      if (badge) badge.remove();
    }
  });
}

function updateDuplicateSummary(data) {
  var summary = document.getElementById('previewSummary');
  // Remove any existing duplicate status
  var existing = summary.querySelector('.dup-status');
  if (existing) existing.remove();

  var span = document.createElement('span');
  span.className = 'stat dup-status';
  if (data.done) {
    if (data.duplicate_count > 0) {
      span.innerHTML = '<span class="stat-value">' + data.duplicate_count + '</span> already imported';
    }
    // If 0 duplicates, show nothing
  } else {
    span.textContent = 'Checking duplicates\u2026 ' + data.checked + '/' + data.total;
  }
  if (span.innerHTML || span.textContent) summary.appendChild(span);
}
```

**Step 2: Hook checkForDuplicates into the preview flow**

In the folder preview fetch success handler (around line 1072, after `renderPreview()` is called), add:

```javascript
checkForDuplicates();
```

Also add the same call in the collection preview success handler (around line 1034, after `renderPreview()` is called):

```javascript
// (No checkForDuplicates for collection preview — those are already imported)
```

Actually only for import mode — collection mode photos are already in the DB.

**Step 3: Commit**

```bash
git add vireo/templates/pipeline.html
git commit -m "feat: progressive duplicate detection in import preview grid"
```

---

### Task 5: Frontend — skip duplicates checkbox toggle

**Files:**
- Modify: `vireo/templates/pipeline.html`

**Step 1: Add change handler to skip duplicates checkbox**

On the `chkSkipDuplicates` checkbox (line 615), add an `onchange` handler:

Change:
```html
<input type="checkbox" id="chkSkipDuplicates" checked style="accent-color:var(--accent);">
```

To:
```html
<input type="checkbox" id="chkSkipDuplicates" checked style="accent-color:var(--accent);" onchange="onSkipDuplicatesToggle()">
```

Add the handler function:

```javascript
function onSkipDuplicatesToggle() {
  if (Object.keys(_duplicateResults).length > 0) {
    applyDuplicateVisuals();
    // Update summary: re-show or remove duplicate count
    var dupCount = Object.keys(_duplicateResults).length;
    var summary = document.getElementById('previewSummary');
    var existing = summary.querySelector('.dup-status');
    if (existing) existing.remove();
    if (document.getElementById('chkSkipDuplicates').checked && dupCount > 0) {
      var span = document.createElement('span');
      span.className = 'stat dup-status';
      span.innerHTML = '<span class="stat-value">' + dupCount + '</span> already imported';
      summary.appendChild(span);
    }
  } else if (document.getElementById('chkSkipDuplicates').checked) {
    checkForDuplicates();
  }
}
```

**Step 2: Commit**

```bash
git add vireo/templates/pipeline.html
git commit -m "feat: toggle duplicate visuals when skip duplicates checkbox changes"
```

---

### Task 6: Run full test suite

**Step 1: Run all tests**

Run: `python -m pytest tests/test_workspaces.py vireo/tests/test_db.py vireo/tests/test_app.py vireo/tests/test_photos_api.py vireo/tests/test_edits_api.py vireo/tests/test_jobs_api.py vireo/tests/test_darktable_api.py vireo/tests/test_config.py vireo/tests/test_check_duplicates.py -v`

Expected: All PASS

**Step 2: If any fail, fix and recommit**

---

### Task 7: Create PR

```bash
gh pr create --title "Show duplicate photos in import preview" --body "$(cat <<'EOF'
## Summary
- New SSE endpoint `/api/import/check-duplicates` that hashes files and checks against DB
- After import preview loads, duplicate detection runs progressively in the background
- Duplicate thumbnails are dimmed (opacity 0.4) with a "DUPLICATE" badge
- Summary bar shows progress ("Checking duplicates... 20/100") then final count ("12 already imported")
- Toggling the "Skip duplicates" checkbox shows/hides the duplicate visuals without re-hashing

## Test plan
- [ ] Import photos from a folder containing files already in the library — duplicates should be dimmed with badge
- [ ] Import from a folder with no duplicates — no visual changes, no "already imported" text
- [ ] Uncheck "Skip duplicates" — dimming and badges disappear
- [ ] Re-check "Skip duplicates" — dimming and badges reappear instantly (cached)
- [ ] Large import (100+ files) — progress text shows during hashing

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```
