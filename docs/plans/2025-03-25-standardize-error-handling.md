# Standardize Error Handling Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Standardize error handling across all routes and templates with `json_error()` on the backend and `safeFetch()`/`safeEventSource()`/`showToast()` on the frontend, eliminating silent failures and duplicated code.

**Architecture:** Add a `json_error()` helper in `app.py` for consistent error responses. Add shared JS utilities (`showToast`, `escapeHtml`, `safeFetch`, `safeEventSource`) to `_navbar.html` (included on every page). Migrate all templates one by one.

**Tech Stack:** Flask/Python backend, vanilla JS frontend, Jinja2 templates.

**Worktree:** `/Users/julius/git/vireo/.worktrees/standardize-error-handling` (branch: `feature/standardize-error-handling`)

---

## Task 1: Add shared frontend utilities to `_navbar.html`

**Files:**
- Modify: `vireo/templates/_navbar.html:1363` (insert before the bottom-panel IIFE)

This is the foundation. All subsequent template migrations depend on these utilities being available.

**Step 1: Add toast CSS, `escapeHtml()`, `showToast()`, `safeFetch()`, and `safeEventSource()` to `_navbar.html`**

Insert the following block between the opening `<script>` tag (line 1363) and the `formatDuration` function (line 1365). These must be in global scope (outside the bottom-panel IIFE that starts at line 1378).

```javascript
/* ---------- Shared Utilities ---------- */
function escapeHtml(str) {
  if (str == null) return '';
  var div = document.createElement('div');
  div.appendChild(document.createTextNode(String(str)));
  return div.innerHTML;
}

/* ---------- Toast Notifications ---------- */
(function() {
  var container = document.createElement('div');
  container.id = 'toastContainer';
  container.style.cssText = 'position:fixed;top:16px;right:16px;z-index:100000;display:flex;flex-direction:column;gap:8px;pointer-events:none;';
  document.body.appendChild(container);
})();

function showToast(msg, type) {
  type = type || 'error';
  var container = document.getElementById('toastContainer');
  if (!container) return;
  var toast = document.createElement('div');
  var bg = type === 'error' ? 'var(--danger, #e74c3c)' : 'var(--accent, #24E5CA)';
  var color = type === 'error' ? '#fff' : 'var(--accent-text, #0A1F2E)';
  toast.style.cssText = 'pointer-events:auto;padding:10px 16px;border-radius:6px;font-size:14px;max-width:400px;word-break:break-word;background:' + bg + ';color:' + color + ';box-shadow:0 2px 8px rgba(0,0,0,0.3);opacity:0;transition:opacity 0.2s;';
  toast.textContent = msg;
  container.appendChild(toast);
  requestAnimationFrame(function() { toast.style.opacity = '1'; });
  setTimeout(function() {
    toast.style.opacity = '0';
    setTimeout(function() { toast.remove(); }, 200);
  }, 5000);
}

/* ---------- Safe Fetch ---------- */
async function safeFetch(url, opts, options) {
  var toast = (!options || options.toast !== false);
  var resp = await fetch(url, opts);
  if (!resp.ok) {
    var body = {};
    try { body = await resp.json(); } catch(e) {}
    var msg = body.error || 'Request failed (' + resp.status + ')';
    if (toast) showToast(msg, 'error');
    var err = new Error(msg);
    err.status = resp.status;
    err.body = body;
    throw err;
  }
  var text = await resp.text();
  if (!text) return null;
  return JSON.parse(text);
}

/* ---------- Safe EventSource ---------- */
function safeEventSource(url, callbacks) {
  callbacks = callbacks || {};
  var source = new EventSource(url);
  source.addEventListener('progress', function(e) {
    if (callbacks.onProgress) callbacks.onProgress(JSON.parse(e.data));
  });
  source.addEventListener('complete', function(e) {
    source.close();
    if (callbacks.onComplete) callbacks.onComplete(JSON.parse(e.data));
  });
  source.onerror = function() {
    source.close();
    showToast('Connection lost', 'error');
    if (callbacks.onError) callbacks.onError();
  };
  return source;
}
```

Note: `safeFetch` handles empty response bodies (some endpoints return 204/empty) by checking `resp.text()` before parsing JSON. The third parameter uses a plain object (`options.toast`) instead of destructuring for compatibility with vanilla JS (no build step).

**Step 2: Verify `_navbar.html` renders without errors**

Run: `cd /Users/julius/git/vireo/.worktrees/standardize-error-handling && python -m pytest vireo/tests/test_app.py::test_browse_page -v`

Expected: PASS (page renders with the new script block)

**Step 3: Commit**

```bash
git add vireo/templates/_navbar.html
git commit -m "feat: add shared safeFetch, safeEventSource, showToast, escapeHtml utilities to navbar"
```

---

## Task 2: Add `json_error()` helper to `app.py` with test

**Files:**
- Modify: `vireo/app.py:142-149` (add helper near the error handler)
- Create: `vireo/tests/test_error_helpers.py`

**Step 1: Write failing test**

Create `vireo/tests/test_error_helpers.py`:

```python
def test_json_error_default_status(app_and_db):
    """json_error returns 400 by default."""
    app, _ = app_and_db
    client = app.test_client()
    # Hit an endpoint that returns json_error — workspace name required
    resp = client.post('/api/workspaces',
                       json={},
                       content_type='application/json')
    assert resp.status_code == 400
    data = resp.get_json()
    assert 'error' in data


def test_json_error_custom_status(app_and_db):
    """json_error can return custom status codes."""
    app, _ = app_and_db
    client = app.test_client()
    # Hit an endpoint that returns 404
    resp = client.get('/api/collections/999999')
    assert resp.status_code == 404
    data = resp.get_json()
    assert 'error' in data
```

**Step 2: Run test to verify it passes (these test existing behavior)**

Run: `cd /Users/julius/git/vireo/.worktrees/standardize-error-handling && python -m pytest vireo/tests/test_error_helpers.py -v`

Expected: PASS (the endpoints already return these shapes — this test locks in the contract)

**Step 3: Add `json_error()` helper to `app.py`**

In `vireo/app.py`, insert after line 149 (after `_handle_error`):

```python
    def json_error(msg, status=400):
        """Return a JSON error response. Standard shape: {"error": "msg"}."""
        return jsonify({"error": msg}), status
```

**Step 4: Run tests to confirm nothing broke**

Run: `cd /Users/julius/git/vireo/.worktrees/standardize-error-handling && python -m pytest vireo/tests/test_error_helpers.py vireo/tests/test_app.py -v`

Expected: All PASS

**Step 5: Commit**

```bash
git add vireo/app.py vireo/tests/test_error_helpers.py
git commit -m "feat: add json_error() helper to app.py with contract tests"
```

---

## Task 3: Migrate `app.py` routes to use `json_error()`

**Files:**
- Modify: `vireo/app.py` (replace ~55 `return jsonify({"error": ...}), NNN` calls)

**Step 1: Replace all `return jsonify({"error": ...}), NNN` with `json_error()`**

This is a mechanical find-and-replace. There are ~55 occurrences. The patterns:

| Before | After |
|--------|-------|
| `return jsonify({"error": "name required"}), 400` | `return json_error("name required")` |
| `return jsonify({"error": "not found"}), 404` | `return json_error("not found", 404)` |
| `return jsonify({"error": str(e)}), 500` | `return json_error(str(e), 500)` |
| `return jsonify({"error": f"directory not found: {root}"}), 400` | `return json_error(f"directory not found: {root}")` |

Do NOT change the one inside `_handle_error` (line 149) — that's the global fallback and already has the right shape.

**Step 2: Run full test suite**

Run: `cd /Users/julius/git/vireo/.worktrees/standardize-error-handling && python -m pytest tests/test_workspaces.py vireo/tests/test_db.py vireo/tests/test_app.py vireo/tests/test_photos_api.py vireo/tests/test_edits_api.py vireo/tests/test_jobs_api.py vireo/tests/test_darktable_api.py vireo/tests/test_config.py vireo/tests/test_error_helpers.py -v`

Expected: All 121+ tests PASS

**Step 3: Commit**

```bash
git add vireo/app.py
git commit -m "refactor: migrate all routes to use json_error() helper"
```

---

## Task 4: Migrate `workspace.html`

**Files:**
- Modify: `vireo/templates/workspace.html`

**Scope:** 13 fetch calls, 1 EventSource, 4 empty catches, 1 `escapeHtml` to remove.

**Step 1: Remove `escapeHtml()` definition**

Delete the `escapeHtml` function at lines 121-126 (now provided by `_navbar.html`). Keep `escapeAttr` — it's workspace-specific.

**Step 2: Replace fetch calls with `safeFetch()`**

For each `fetch()` call in the template, apply one of these patterns:

**Pattern A — Fire-and-forget (currently empty catch):** These load optional UI data. Use `safeFetch` with `{ toast: false }` and catch silently. Example:

```javascript
// Before (loadWsFolders, line 136):
try {
  var resp = await fetch('/api/workspaces/active');
  var ws = await resp.json();
  ...
} catch(e) {}

// After:
try {
  var ws = await safeFetch('/api/workspaces/active', {}, { toast: false });
  ...
} catch(e) {}
```

**Pattern B — User action with inline error display:** Suppress toast, show error inline. Example:

```javascript
// Before (scanAndAddFolder, lines 182-197):
var resp = await fetch('/api/jobs/scan', { method: 'POST', ... });
if (!resp.ok) {
  var err = await resp.json();
  status.style.color = 'var(--danger, #e74c3c)';
  status.textContent = err.error || 'Failed to start scan';
  btn.disabled = false;
  btn.textContent = 'Scan & Add';
  return;
}
var data = await resp.json();

// After:
try {
  var data = await safeFetch('/api/jobs/scan', { method: 'POST', ... }, { toast: false });
} catch(e) {
  status.style.color = 'var(--danger, #e74c3c)';
  status.textContent = e.message;
  btn.disabled = false;
  btn.textContent = 'Scan & Add';
  return;
}
```

**Pattern C — Mutation that should toast on error:** Let `safeFetch` handle errors automatically. Example:

```javascript
// Before (removeFolderFromWs, line 243):
await fetch('/api/workspaces/' + wsId + '/folders/' + folderId, { method: 'DELETE' });
loadWsFolders();

// After:
try {
  await safeFetch('/api/workspaces/' + wsId + '/folders/' + folderId, { method: 'DELETE' });
} catch(e) {}
loadWsFolders();
```

**Step 3: Replace EventSource with `safeEventSource()`**

```javascript
// Before (scanAndAddFolder, lines 202-233):
var source = new EventSource('/api/jobs/' + jobId + '/stream');
source.addEventListener('progress', function(e) { ... });
source.addEventListener('complete', function(e) { source.close(); ... });
source.onerror = function() { source.close(); ... };

// After:
var source = safeEventSource('/api/jobs/' + jobId + '/stream', {
  onProgress: function(p) {
    if (p.total > 0) {
      status.textContent = 'Scanning: ' + p.current + '/' + p.total + (p.current_file ? ' — ' + p.current_file : '');
    } else if (p.current_file) {
      status.textContent = 'Scanning: ' + p.current_file;
    }
  },
  onComplete: function(result) {
    if (result.status === 'completed') {
      status.style.color = 'var(--accent, #24E5CA)';
      status.textContent = 'Done! Folder added to workspace.';
      input.value = '';
      loadWsFolders();
    } else {
      status.style.color = 'var(--danger, #e74c3c)';
      status.textContent = 'Scan failed' + (result.errors && result.errors.length ? ': ' + result.errors[0] : '');
    }
    btn.disabled = false;
    btn.textContent = 'Scan & Add';
    setTimeout(function() { status.style.display = 'none'; }, 5000);
  },
  onError: function() {
    btn.disabled = false;
    btn.textContent = 'Scan & Add';
  }
});
```

**Step 4: Run tests**

Run: `cd /Users/julius/git/vireo/.worktrees/standardize-error-handling && python -m pytest vireo/tests/test_app.py -v`

Expected: All PASS

**Step 5: Commit**

```bash
git add vireo/templates/workspace.html
git commit -m "refactor: migrate workspace.html to safeFetch/safeEventSource"
```

---

## Task 5: Migrate `browse.html`

**Files:**
- Modify: `vireo/templates/browse.html`

**Scope:** 23 fetch calls, 1 EventSource, 10 empty catches, 1 `escapeHtml` to remove.

**Step 1: Remove `escapeHtml()` definition** (line 1427)

**Step 2: Replace fetch calls with `safeFetch()`**

Apply the same patterns as Task 4. This template has the most empty catches (10). Most are loading optional sidebar data (collections, folders, keywords) — use `safeFetch(..., { toast: false })` + catch. The EventSource at line 1468 should use `safeEventSource()`.

Key calls to migrate:
- `loadCollections` (line ~738) — Pattern A (optional data)
- `loadPhotos` — Pattern A (main data load, but empty catch is wrong here — use toast)
- Various sidebar loaders — Pattern A
- `startScan` / EventSource — Pattern B + safeEventSource
- Rating/flag/keyword mutations — Pattern C (auto-toast)

**Step 3: Run tests**

Run: `cd /Users/julius/git/vireo/.worktrees/standardize-error-handling && python -m pytest vireo/tests/test_app.py -v`

**Step 4: Commit**

```bash
git add vireo/templates/browse.html
git commit -m "refactor: migrate browse.html to safeFetch/safeEventSource"
```

---

## Task 6: Migrate `settings.html`

**Files:**
- Modify: `vireo/templates/settings.html`

**Scope:** 33 fetch calls, 6 EventSource instances, 13 empty catches, 1 `escapeHtml` to remove. This is the largest template.

**Step 1: Remove `escapeHtml()` definition** (line 1187)

**Step 2: Replace fetch calls with `safeFetch()`**

This template has many config-loading calls with empty catches (Pattern A) and several job-starting actions with SSE streams (Pattern B + safeEventSource).

Key groups:
- Config loaders (`loadConfig`, `loadModels`, `loadLabels`, etc.) — Pattern A (toast: false)
- Save actions (`saveConfig`, etc.) — Pattern C (auto-toast)
- Job starters (thumbnail regen, cache clear, etc.) — Pattern B + safeEventSource

**Step 3: Replace all 6 EventSource instances with `safeEventSource()`**

Each follows the same pattern as Task 4 Step 3.

**Step 4: Run tests**

Run: `cd /Users/julius/git/vireo/.worktrees/standardize-error-handling && python -m pytest vireo/tests/test_app.py vireo/tests/test_config.py -v`

**Step 5: Commit**

```bash
git add vireo/templates/settings.html
git commit -m "refactor: migrate settings.html to safeFetch/safeEventSource"
```

---

## Task 7: Migrate `review.html`

**Files:**
- Modify: `vireo/templates/review.html`

**Scope:** 16 fetch calls, 1 EventSource, 6 empty catches, 1 `escapeHtml` to remove.

**Step 1: Remove `escapeHtml()` definition** (line 1043)

**Step 2: Replace fetch calls with `safeFetch()`.** Key calls:
- `loadPredictions`, `loadCollections` — Pattern A
- Accept/reject/batch actions — Pattern C (auto-toast)
- Reclassify job start + SSE — Pattern B + safeEventSource

**Step 3: Run tests**

Run: `cd /Users/julius/git/vireo/.worktrees/standardize-error-handling && python -m pytest vireo/tests/test_app.py -v`

**Step 4: Commit**

```bash
git add vireo/templates/review.html
git commit -m "refactor: migrate review.html to safeFetch/safeEventSource"
```

---

## Task 8: Migrate `import.html`

**Files:**
- Modify: `vireo/templates/import.html`

**Scope:** 13 fetch calls, 3 EventSource instances, 5 empty catches, 1 `escapeHtml` to remove.

**Step 1: Remove `escapeHtml()` definition** (line 625)

**Step 2: Replace fetch calls and 3 EventSource instances.** Key calls:
- Catalog loaders — Pattern A
- Import/preview actions — Pattern B + safeEventSource
- 3 separate SSE streams for different import phases

**Step 3: Run tests**

Run: `cd /Users/julius/git/vireo/.worktrees/standardize-error-handling && python -m pytest vireo/tests/test_app.py -v`

**Step 4: Commit**

```bash
git add vireo/templates/import.html
git commit -m "refactor: migrate import.html to safeFetch/safeEventSource"
```

---

## Task 9: Migrate `pipeline.html`

**Files:**
- Modify: `vireo/templates/pipeline.html`

**Scope:** 12 fetch calls, 3 EventSource instances, 0 empty catches, 1 `escapeHtml` to remove.

**Step 1: Remove `escapeHtml()` definition** (line 824)

**Step 2: Replace fetch calls and 3 EventSource instances.**
- Pipeline config loaders — Pattern A
- Pipeline run/regroup/analyze actions — Pattern B + safeEventSource

**Step 3: Run tests**

Run: `cd /Users/julius/git/vireo/.worktrees/standardize-error-handling && python -m pytest vireo/tests/test_app.py -v`

**Step 4: Commit**

```bash
git add vireo/templates/pipeline.html
git commit -m "refactor: migrate pipeline.html to safeFetch/safeEventSource"
```

---

## Task 10: Migrate small templates (audit, stats, variants, cull, logs, pipeline_review)

**Files:**
- Modify: `vireo/templates/audit.html` (10 fetch, 0 SSE, 1 escapeHtml)
- Modify: `vireo/templates/stats.html` (9 fetch, 0 SSE, 1 escapeHtml, 2 empty catches)
- Modify: `vireo/templates/variants.html` (4 fetch, 0 SSE, 1 escapeHtml)
- Modify: `vireo/templates/cull.html` (4 fetch, 1 SSE, 1 escapeHtml, 2 empty catches)
- Modify: `vireo/templates/logs.html` (2 fetch, 1 SSE log stream, 1 escapeHtml)
- Modify: `vireo/templates/pipeline_review.html` (4 fetch, 0 SSE, 0 escapeHtml)

**Step 1: Remove `escapeHtml()` definitions** from audit (line 302), stats (line 453), variants (line 281), cull (line 753), logs (line 145). pipeline_review doesn't have one.

**Step 2: Replace fetch calls in each template** using the same A/B/C patterns.

**Step 3: Replace EventSource in `cull.html`** with `safeEventSource()`.

**Step 4: `logs.html` SSE** — This is a log stream (`/api/logs/stream`), similar to navbar's bottom panel. It has its own reconnection/display logic. Use `safeEventSource()` only if the pattern fits cleanly; if it has custom event types beyond progress/complete, keep it as-is and just ensure it has an `onerror` handler.

**Step 5: Run tests**

Run: `cd /Users/julius/git/vireo/.worktrees/standardize-error-handling && python -m pytest vireo/tests/test_app.py -v`

**Step 6: Commit**

```bash
git add vireo/templates/audit.html vireo/templates/stats.html vireo/templates/variants.html vireo/templates/cull.html vireo/templates/logs.html vireo/templates/pipeline_review.html
git commit -m "refactor: migrate small templates to safeFetch/safeEventSource"
```

---

## Task 11: Migrate `_navbar.html` fetch calls

**Files:**
- Modify: `vireo/templates/_navbar.html`

**Scope:** ~12 fetch calls in the navbar itself (workspace switcher, lightbox, drift check). Do NOT touch the bottom-panel SSE log stream (has reconnection logic inside the IIFE).

**Step 1: Replace fetch calls in global-scope functions**

Key calls:
- `loadWorkspaces` (line 781) — Pattern A
- `switchWorkspace` (line 803) — Pattern C
- `loadFolderCheckboxes` (line 828) — Pattern A
- `createWorkspace` (line 868) — Pattern B (inline error in modal)
- Drift check fetch (line 891) — Pattern A
- Lightbox fetches (lines 1066, 1139, 1287) — Pattern A

**Step 2: Do NOT modify fetches inside the bottom-panel IIFE** (lines 1378-1793). These are for job polling and log streaming with their own error handling and reconnection logic.

**Step 3: Run tests**

Run: `cd /Users/julius/git/vireo/.worktrees/standardize-error-handling && python -m pytest vireo/tests/test_app.py -v`

**Step 4: Commit**

```bash
git add vireo/templates/_navbar.html
git commit -m "refactor: migrate navbar fetch calls to safeFetch"
```

---

## Task 12: Final verification and cleanup

**Files:**
- All modified files

**Step 1: Verify no raw `fetch()` calls remain outside `_navbar.html` IIFE**

Run: `grep -rn "await fetch\|\.then(function(r)" vireo/templates/ | grep -v _navbar.html`

If any remain, they were missed — go back and migrate them.

Also check navbar's global-scope functions:

Run: `grep -n "await fetch\|\.then(function(r)" vireo/templates/_navbar.html | head -20`

Only lines inside the IIFE (1378+) should remain.

**Step 2: Verify no duplicate `escapeHtml` definitions remain**

Run: `grep -rn "function escapeHtml" vireo/templates/`

Expected: Only one result in `_navbar.html`.

**Step 3: Verify no empty catch blocks remain (except intentional ones)**

Run: `grep -rn "catch(e) {}" vireo/templates/`

Any remaining should be from Pattern A (toast: false + intentional silent catch for non-critical loads). Verify each is intentional.

**Step 4: Run full test suite**

Run: `cd /Users/julius/git/vireo/.worktrees/standardize-error-handling && python -m pytest tests/test_workspaces.py vireo/tests/test_db.py vireo/tests/test_app.py vireo/tests/test_photos_api.py vireo/tests/test_edits_api.py vireo/tests/test_jobs_api.py vireo/tests/test_darktable_api.py vireo/tests/test_config.py vireo/tests/test_error_helpers.py -v`

Expected: All tests PASS

**Step 5: Commit any cleanup, then create PR**

```bash
gh pr create --title "Standardize error handling across routes and templates" --body "$(cat <<'EOF'
## Summary
- Add `json_error()` helper to backend for consistent error response shape
- Add `safeFetch()`, `safeEventSource()`, `showToast()` shared utilities to `_navbar.html`
- Migrate all templates to use shared utilities, eliminating:
  - ~55 inline `jsonify({"error": ...})` calls
  - ~45 empty `catch(e) {}` blocks (silent failures)
  - 11 duplicate `escapeHtml()` definitions
  - Inconsistent SSE error handling across ~15 EventSource instances
- Toast notifications auto-display on API errors; inline handling preserved for form validation

## Test plan
- [ ] All existing tests pass (121+)
- [ ] New contract tests for `json_error()` response shape
- [ ] Smoke test each page: workspace, browse, settings, review, import, pipeline, audit, stats, variants, cull, logs
- [ ] Trigger an error (e.g., create workspace with blank name) and verify toast appears
- [ ] Verify SSE connection loss shows toast
- [ ] Verify form validation still shows inline errors (workspace creation modal)

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```
