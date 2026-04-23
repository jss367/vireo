# Right-click context menus — implementation plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add cross-surface right-click context menus to Vireo (photo cards, lightbox, folder tree, keyword row, collection item, burst group modal) without changing any existing keyboard shortcut or button.

**Architecture:** One shared floating-menu component lives in `vireo/templates/_navbar.html` (already included by every page). Each surface attaches its own `contextmenu` handler that calls `openContextMenu(event, items)`. Finder-style selection rule: right-clicking an item outside the current selection replaces selection with that one item before the menu opens. Three new Flask endpoints back the net-new actions (reveal-in-OS-file-manager, folder rescan, collection duplicate). Copy Path is client-side via `navigator.clipboard`.

**Tech Stack:** Flask + Jinja2, vanilla JS in inline `<script>` blocks, Playwright for e2e tests, pytest for server tests.

**Design doc:** `docs/plans/2026-04-22-context-menus-design.md`

**Branch:** `right-click-review` (already in an isolated Conductor worktree — no new worktree needed).

---

## Conventions used throughout this plan

- **Run the full test suite** at the end of each task: `python -m pytest tests/ vireo/tests/ -q`. Individual commands shown per task are the fast-iteration subset.
- **Commit after each task.** Each task is one logical unit.
- **All UI work lives in templates** — no new JS/CSS files. Add to the existing inline `<script>` / `<style>` blocks in `_navbar.html` or the per-page template.
- **Tests.** Server behavior → `vireo/tests/test_*.py` with `app_and_db` fixture. UI behavior → `tests/e2e/test_*.py` with `live_server` + `page` fixtures.

---

## Task 1: Shared context-menu component

**Files:**
- Modify: `vireo/templates/_navbar.html` (add CSS block near existing `.kw-type-dropdown` patterns, add JS `openContextMenu` at bottom of the shared `<script>`).
- Test: `tests/e2e/test_context_menu.py` (new file).

**Step 1: Write the failing test**

Create `tests/e2e/test_context_menu.py`:

```python
from playwright.sync_api import expect


def test_open_context_menu_at_cursor(live_server, page):
    """openContextMenu() places the menu near the event coords and renders items."""
    url = live_server["url"]
    page.goto(f"{url}/browse")

    page.evaluate("""
        openContextMenu({clientX: 200, clientY: 150}, [
            {label: 'Alpha', onClick: () => window.__ctx_hit = 'alpha'},
            {separator: true},
            {label: 'Beta', disabled: true, disabledHint: 'nope'},
        ]);
    """)

    menu = page.locator(".vireo-ctx-menu")
    expect(menu).to_be_visible()
    expect(menu.locator(".vireo-ctx-item", has_text="Alpha")).to_be_visible()
    expect(menu.locator(".vireo-ctx-item", has_text="Beta")).to_have_class(
        # contains "disabled" — use a looser check
        "vireo-ctx-item vireo-ctx-disabled"
    )

    # Click Alpha; menu closes and handler fires.
    menu.locator(".vireo-ctx-item", has_text="Alpha").click()
    expect(menu).to_be_hidden()
    assert page.evaluate("window.__ctx_hit") == "alpha"


def test_context_menu_dismiss_outside_click(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/browse")

    page.evaluate("""
        openContextMenu({clientX: 100, clientY: 100},
            [{label: 'X', onClick: () => {}}]);
    """)
    expect(page.locator(".vireo-ctx-menu")).to_be_visible()

    page.mouse.click(500, 500)
    expect(page.locator(".vireo-ctx-menu")).to_be_hidden()


def test_context_menu_escape_closes(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/browse")

    page.evaluate("""
        openContextMenu({clientX: 50, clientY: 50},
            [{label: 'Y', onClick: () => {}}]);
    """)
    page.keyboard.press("Escape")
    expect(page.locator(".vireo-ctx-menu")).to_be_hidden()
```

**Step 2: Run test to verify it fails**

```
python -m pytest tests/e2e/test_context_menu.py -v
```
Expected: FAIL — `openContextMenu is not defined`.

**Step 3: Implement the shared component**

In `vireo/templates/_navbar.html`, add to the `<style>` block (near existing `.kw-type-dropdown` at keywords.html line ~123, but put it in `_navbar.html` since it's shared):

```css
.vireo-ctx-menu {
  position: fixed; z-index: 1000;
  background: var(--bg-secondary);
  border: 1px solid var(--border-secondary);
  border-radius: 6px; padding: 4px 0;
  min-width: 180px;
  box-shadow: 0 4px 16px rgba(0,0,0,0.4);
  font-size: 13px; color: var(--text-primary);
  user-select: none;
}
.vireo-ctx-item {
  display: flex; align-items: center; gap: 8px;
  padding: 6px 12px; cursor: pointer;
}
.vireo-ctx-item:hover:not(.vireo-ctx-disabled) {
  background: var(--bg-tertiary);
}
.vireo-ctx-disabled {
  color: var(--text-tertiary); cursor: default;
}
.vireo-ctx-sep {
  height: 1px; background: var(--border-secondary);
  margin: 4px 0;
}
.vireo-ctx-chips {
  display: flex; gap: 4px; padding: 6px 12px;
}
.vireo-ctx-chip {
  flex: 0 0 auto; padding: 2px 6px; border-radius: 4px;
  cursor: pointer; line-height: 1;
}
.vireo-ctx-chip:hover { background: var(--bg-tertiary); }
.vireo-ctx-chip.is-active { background: var(--accent); color: white; }
```

In `vireo/templates/_navbar.html`, add to the shared `<script>` block:

```javascript
(function(){
  let _ctxEl = null;
  let _ctxDismiss = null;

  window.closeContextMenu = function(){
    if (_ctxEl) { _ctxEl.remove(); _ctxEl = null; }
    document.removeEventListener('mousedown', _outside, true);
    document.removeEventListener('keydown', _keydown, true);
    window.removeEventListener('blur', closeContextMenu);
    window.removeEventListener('scroll', closeContextMenu, true);
    if (_ctxDismiss) { const f = _ctxDismiss; _ctxDismiss = null; f(); }
  };

  function _outside(e){
    if (_ctxEl && !_ctxEl.contains(e.target)) closeContextMenu();
  }
  function _keydown(e){
    if (e.key === 'Escape') { e.preventDefault(); closeContextMenu(); }
  }

  function _renderItem(item){
    if (item.separator) {
      const s = document.createElement('div');
      s.className = 'vireo-ctx-sep';
      return s;
    }
    if (item.chips) {
      const row = document.createElement('div');
      row.className = 'vireo-ctx-chips';
      item.chips.forEach(c => {
        const b = document.createElement('span');
        b.className = 'vireo-ctx-chip' + (c.active ? ' is-active' : '');
        b.textContent = c.label;
        if (c.title) b.title = c.title;
        b.addEventListener('click', ev => {
          ev.stopPropagation();
          closeContextMenu();
          try { c.onClick && c.onClick(); } catch(err){ console.error(err); }
        });
        row.appendChild(b);
      });
      return row;
    }
    const d = document.createElement('div');
    d.className = 'vireo-ctx-item' + (item.disabled ? ' vireo-ctx-disabled' : '');
    d.textContent = item.label;
    if (item.disabled && item.disabledHint) d.title = item.disabledHint;
    if (!item.disabled) {
      d.addEventListener('click', ev => {
        ev.stopPropagation();
        closeContextMenu();
        try { item.onClick && item.onClick(); } catch(err){ console.error(err); }
      });
    }
    return d;
  }

  window.openContextMenu = function(event, items, opts){
    closeContextMenu();
    const menu = document.createElement('div');
    menu.className = 'vireo-ctx-menu';
    items.forEach(it => menu.appendChild(_renderItem(it)));
    document.body.appendChild(menu);
    // Clamp to viewport.
    const vw = window.innerWidth, vh = window.innerHeight;
    const rect = menu.getBoundingClientRect();
    let x = event.clientX, y = event.clientY;
    if (x + rect.width  > vw) x = Math.max(0, vw - rect.width  - 4);
    if (y + rect.height > vh) y = Math.max(0, vh - rect.height - 4);
    menu.style.left = x + 'px';
    menu.style.top  = y + 'px';
    _ctxEl = menu;
    _ctxDismiss = (opts && opts.onDismiss) || null;
    document.addEventListener('mousedown', _outside, true);
    document.addEventListener('keydown', _keydown, true);
    window.addEventListener('blur', closeContextMenu);
    window.addEventListener('scroll', closeContextMenu, true);
  };
})();
```

**Step 4: Run test to verify it passes**

```
python -m pytest tests/e2e/test_context_menu.py -v
```
Expected: PASS.

**Step 5: Commit**

```
git add vireo/templates/_navbar.html tests/e2e/test_context_menu.py
git commit -m "feat(ui): shared right-click context menu component"
```

---

## Task 2: Finder-style selection-coupling helper

**Files:**
- Modify: `vireo/templates/_navbar.html` (add `coerceSelection` helper at the bottom of the shared script).
- Test: `tests/e2e/test_context_menu.py` (extend).

**Step 1: Write the failing tests**

Append to `tests/e2e/test_context_menu.py`:

```python
def test_coerce_selection_inside_keeps_set(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/browse")
    out = page.evaluate("""() => {
        const sel = new Set([1, 2, 3]);
        const result = coerceSelectionOnContext(sel, 2);
        return { size: sel.size, has1: sel.has(1), has2: sel.has(2), has3: sel.has(3), result: Array.from(result) };
    }""")
    assert out["size"] == 3
    assert out["has1"] and out["has2"] and out["has3"]
    assert sorted(out["result"]) == [1, 2, 3]


def test_coerce_selection_outside_replaces(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/browse")
    out = page.evaluate("""() => {
        const sel = new Set([1, 2, 3]);
        const result = coerceSelectionOnContext(sel, 99);
        return { size: sel.size, has99: sel.has(99), result: Array.from(result) };
    }""")
    assert out["size"] == 1
    assert out["has99"] is True
    assert out["result"] == [99]
```

**Step 2: Run test to verify it fails**

```
python -m pytest tests/e2e/test_context_menu.py::test_coerce_selection_inside_keeps_set tests/e2e/test_context_menu.py::test_coerce_selection_outside_replaces -v
```
Expected: FAIL — `coerceSelectionOnContext is not defined`.

**Step 3: Implement**

In the same IIFE in `_navbar.html`, add:

```javascript
  window.coerceSelectionOnContext = function(selectionSet, clickedId){
    if (!selectionSet.has(clickedId)) {
      selectionSet.clear();
      selectionSet.add(clickedId);
    }
    return Array.from(selectionSet);
  };
```

**Step 4: Run test to verify it passes**

Expected: PASS.

**Step 5: Commit**

```
git add vireo/templates/_navbar.html tests/e2e/test_context_menu.py
git commit -m "feat(ui): finder-style selection coercion helper"
```

---

## Task 3: Server endpoint — reveal in OS file manager

**Files:**
- Modify: `vireo/app.py` (add `api_files_reveal` route).
- Test: `vireo/tests/test_reveal_api.py` (new).

**Step 1: Write the failing tests**

```python
import sys
from unittest.mock import patch, MagicMock

import pytest


def test_reveal_macos(app_and_db):
    app, db = app_and_db
    pid = db.list_photos()[0]["id"]
    with app.test_client() as c, \
         patch("vireo.app.sys.platform", "darwin"), \
         patch("vireo.app.subprocess.run") as run:
        run.return_value = MagicMock(returncode=0)
        resp = c.post("/api/files/reveal", json={"photo_id": pid})
        assert resp.status_code == 200
        assert resp.get_json()["ok"] is True
        args = run.call_args[0][0]
        assert args[0] == "open"
        assert args[1] == "-R"


def test_reveal_linux_opens_parent(app_and_db):
    app, db = app_and_db
    pid = db.list_photos()[0]["id"]
    with app.test_client() as c, \
         patch("vireo.app.sys.platform", "linux"), \
         patch("vireo.app.subprocess.run") as run:
        run.return_value = MagicMock(returncode=0)
        resp = c.post("/api/files/reveal", json={"photo_id": pid})
        assert resp.status_code == 200
        args = run.call_args[0][0]
        assert args[0] == "xdg-open"
        # argv[1] is the parent dir, not the file itself
        assert not args[1].endswith(".jpg")


def test_reveal_windows_select(app_and_db):
    app, db = app_and_db
    pid = db.list_photos()[0]["id"]
    with app.test_client() as c, \
         patch("vireo.app.sys.platform", "win32"), \
         patch("vireo.app.subprocess.run") as run:
        run.return_value = MagicMock(returncode=0)
        resp = c.post("/api/files/reveal", json={"photo_id": pid})
        assert resp.status_code == 200
        args = run.call_args[0][0]
        assert args[0].lower() == "explorer"
        assert args[1].startswith("/select,")


def test_reveal_unknown_photo_returns_error(app_and_db):
    app, _ = app_and_db
    with app.test_client() as c:
        resp = c.post("/api/files/reveal", json={"photo_id": 999999})
        assert resp.status_code == 404


def test_reveal_shell_failure_reports_reason(app_and_db):
    app, db = app_and_db
    pid = db.list_photos()[0]["id"]
    with app.test_client() as c, \
         patch("vireo.app.sys.platform", "darwin"), \
         patch("vireo.app.subprocess.run") as run:
        run.side_effect = FileNotFoundError("no 'open'")
        resp = c.post("/api/files/reveal", json={"photo_id": pid})
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["ok"] is False
        assert "reason" in body
```

**Step 2: Verify it fails**

```
python -m pytest vireo/tests/test_reveal_api.py -v
```
Expected: FAIL — route not registered.

**Step 3: Implement**

In `vireo/app.py`, add a route (near existing file/photo endpoints, e.g. after `api_set_color_label` ~line 1233). Confirm `subprocess` and `sys` are already imported at the top; if not, add them.

```python
@app.route("/api/files/reveal", methods=["POST"])
def api_files_reveal():
    body = request.get_json(silent=True) or {}
    pid = body.get("photo_id")
    if pid is None:
        return json_error("photo_id required")
    db = _get_db()
    photo = db.get_photo(int(pid))
    if not photo:
        return json_error("photo not found", 404)
    path = photo.get("path") or db.photo_path(int(pid))
    if not path:
        return jsonify({"ok": False, "reason": "no path"})
    try:
        if sys.platform == "darwin":
            subprocess.run(["open", "-R", path], timeout=5, check=False)
        elif sys.platform.startswith("win"):
            subprocess.run(["explorer", f"/select,{path}"], timeout=5, check=False)
        else:
            import os as _os
            parent = _os.path.dirname(path) or path
            subprocess.run(["xdg-open", parent], timeout=5, check=False)
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError) as exc:
        return jsonify({"ok": False, "reason": str(exc)})
    return jsonify({"ok": True})
```

If `db.get_photo` / `db.photo_path` don't exist with these names, read `vireo/db.py` to find the right helper (likely `get_photo_by_id` or similar) and adjust.

**Step 4: Verify passing**

```
python -m pytest vireo/tests/test_reveal_api.py -v
```
Expected: PASS (all 5 tests).

**Step 5: Commit**

```
git add vireo/app.py vireo/tests/test_reveal_api.py
git commit -m "feat(api): cross-platform reveal-in-file-manager endpoint"
```

---

## Task 4: Server endpoint — folder rescan

**Files:**
- Modify: `vireo/app.py` (add `api_folder_rescan` route, delegate to existing scan job infra with a folder filter).
- Test: `vireo/tests/test_folder_rescan_api.py` (new).

**Step 1: Write the failing test**

```python
def test_folder_rescan_queues_job(app_and_db):
    app, db = app_and_db
    folder_id = db.list_folders()[0]["id"]
    with app.test_client() as c:
        resp = c.post(f"/api/folders/{folder_id}/rescan", json={})
        assert resp.status_code == 200
        body = resp.get_json()
        assert "job_id" in body
    # The job runner has one queued job tagged with our folder.
    runner = app._job_runner
    jobs = runner.list_jobs()
    assert any(
        j.get("type") == "scan" and j.get("folder_id") == folder_id
        for j in jobs
    )


def test_folder_rescan_unknown_folder(app_and_db):
    app, _ = app_and_db
    with app.test_client() as c:
        resp = c.post("/api/folders/999999/rescan", json={})
        assert resp.status_code == 404
```

**Step 2: Verify fails**

```
python -m pytest vireo/tests/test_folder_rescan_api.py -v
```
Expected: FAIL.

**Step 3: Implement**

Read the existing `POST /api/jobs/scan` (around line 4640 of `vireo/app.py`) and extract the work callable. Add:

```python
@app.route("/api/folders/<int:folder_id>/rescan", methods=["POST"])
def api_folder_rescan(folder_id):
    db = _get_db()
    folder = db.get_folder(folder_id)
    if not folder:
        return json_error("folder not found", 404)
    runner = app._job_runner
    path = folder["path"]

    def work(job):
        # Delegate to the same scan path as /api/jobs/scan but scoped to `path`.
        _run_scan(job, runner, root=path, folder_id=folder_id)

    job = runner.queue(work, label=f"Rescan {folder['name']}",
                       meta={"type": "scan", "folder_id": folder_id})
    return jsonify({"job_id": job["id"]})
```

If `_run_scan` isn't the existing helper name, extract the work body from `api_job_scan` into a shared function `_run_scan(job, runner, root, folder_id=None)` and call it from both places. The meta field (`type`, `folder_id`) is what the test asserts on — if the existing job schema uses different names, adjust both the test and the implementation to match.

**Step 4: Verify passing**

Expected: PASS.

**Step 5: Commit**

```
git add vireo/app.py vireo/tests/test_folder_rescan_api.py
git commit -m "feat(api): per-folder rescan endpoint"
```

---

## Task 5: Server endpoint — collection duplicate

**Files:**
- Modify: `vireo/app.py` (add `api_collection_duplicate` route).
- Modify: `vireo/db.py` (add `duplicate_collection` method if not present).
- Test: `vireo/tests/test_collection_duplicate_api.py` (new).

**Step 1: Write the failing tests**

```python
def test_duplicate_collection_copies_memberships(app_and_db):
    app, db = app_and_db
    pids = [p["id"] for p in db.list_photos()][:3]
    cid = db.create_collection("My Picks")
    for pid in pids:
        db.add_photo_to_collection(cid, pid)

    with app.test_client() as c:
        resp = c.post(f"/api/collections/{cid}/duplicate", json={})
        assert resp.status_code == 200
        new_id = resp.get_json()["id"]
        assert new_id != cid

    cols = {c["id"]: c for c in db.list_collections()}
    assert new_id in cols
    assert cols[new_id]["name"].startswith("My Picks")
    # Membership copied.
    new_members = db.list_photos_in_collection(new_id)
    assert sorted(p["id"] for p in new_members) == sorted(pids)


def test_duplicate_unknown_collection(app_and_db):
    app, _ = app_and_db
    with app.test_client() as c:
        resp = c.post("/api/collections/999999/duplicate", json={})
        assert resp.status_code == 404
```

If helper names like `list_photos_in_collection` or `add_photo_to_collection` differ, read `vireo/db.py` and adjust (common alternatives: `collection_photos`, `add_to_collection`).

**Step 2: Verify fails**

Expected: FAIL.

**Step 3: Implement**

In `vireo/db.py`, add (inside the workspace-scoped collection section):

```python
def duplicate_collection(self, collection_id: int) -> int:
    ws = self._ws_id()
    row = self.conn.execute(
        "SELECT name FROM collections WHERE id = ? AND workspace_id = ?",
        (collection_id, ws),
    ).fetchone()
    if not row:
        raise ValueError("collection not found")
    new_name = f"{row['name']} (copy)"
    new_id = self.create_collection(new_name)
    self.conn.execute(
        "INSERT INTO collection_photos (collection_id, photo_id) "
        "SELECT ?, photo_id FROM collection_photos WHERE collection_id = ?",
        (new_id, collection_id),
    )
    self.conn.commit()
    return new_id
```

If the membership table is not called `collection_photos`, find the right name with `grep -n collection_ vireo/db.py`.

In `vireo/app.py`:

```python
@app.route("/api/collections/<int:collection_id>/duplicate", methods=["POST"])
def api_collection_duplicate(collection_id):
    db = _get_db()
    try:
        new_id = db.duplicate_collection(collection_id)
    except ValueError:
        return json_error("collection not found", 404)
    return jsonify({"id": new_id})
```

**Step 4: Verify passing**

Expected: PASS.

**Step 5: Commit**

```
git add vireo/app.py vireo/db.py vireo/tests/test_collection_duplicate_api.py
git commit -m "feat(api): collection duplicate endpoint"
```

---

## Task 6: Wire photo card (browse grid)

**Files:**
- Modify: `vireo/templates/browse.html` (attach `contextmenu` handler inside the grid-card event delegation; add `buildPhotoContextMenu(photoIds)` helper).
- Test: `tests/e2e/test_browse_context_menu.py` (new).

**Step 1: Write the failing test**

```python
from playwright.sync_api import expect


def test_right_click_photo_opens_menu(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/browse")

    first = page.locator(".grid-card").first
    first.wait_for(state="visible")
    first.click(button="right")

    menu = page.locator(".vireo-ctx-menu")
    expect(menu).to_be_visible()
    # Rating chips row present.
    expect(menu.locator(".vireo-ctx-chip")).to_have_count_greater_than(5)
    # Key actions present.
    expect(menu.locator(".vireo-ctx-item", has_text="Reveal in")).to_be_visible()
    expect(menu.locator(".vireo-ctx-item", has_text="Copy Path")).to_be_visible()
    expect(menu.locator(".vireo-ctx-item", has_text="Delete")).to_be_visible()


def test_right_click_rating_applies(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/browse")
    first = page.locator(".grid-card").first
    first.wait_for(state="visible")
    first.click(button="right")

    # Click the "3" chip in the rating row.
    menu = page.locator(".vireo-ctx-menu")
    menu.locator(".vireo-ctx-chip", has_text="3").click()
    expect(menu).to_be_hidden()

    # Rating got applied — the card's detail panel / rating attribute reflects 3.
    # Wait for a DOM signal. Easiest: poll the card's data attribute or re-fetch.
    page.wait_for_function(
        "() => document.querySelector('.grid-card').dataset.rating === '3'"
    )


def test_right_click_outside_selection_replaces_selection(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/browse")

    cards = page.locator(".grid-card")
    cards.first.wait_for(state="visible")

    # Select cards 0 and 1.
    cards.nth(0).click()
    cards.nth(1).click(modifiers=["Meta"])
    # Right-click card 2, which is NOT in selection.
    cards.nth(2).click(button="right")

    expect(page.locator(".vireo-ctx-menu")).to_be_visible()
    # Selection should now be exactly card 2.
    size = page.evaluate("selectedPhotos.size")
    assert size == 1
```

`to_have_count_greater_than` is pseudo — use `expect(menu.locator('.vireo-ctx-chip').count()).toBeGreaterThan(5)` via `assert menu.locator('.vireo-ctx-chip').count() > 5`.

**Step 2: Verify it fails**

```
python -m pytest tests/e2e/test_browse_context_menu.py -v
```
Expected: FAIL.

**Step 3: Implement**

Read the current grid-card click setup in `browse.html` around line 1924–1940 and the `selectedPhotos` / `selectPhoto` helpers. Add:

```javascript
function buildPhotoContextMenu(photoIds){
  const one = photoIds.length === 1;
  const hint = one ? undefined : 'Select a single photo';

  const rateChip = n => ({
    label: n === 0 ? '☆' : String(n),
    title: n === 0 ? 'No rating' : `Rate ${n}`,
    onClick: () => photoIds.forEach(id => setRatingFor(id, n)),
  });
  const colorChip = (c, icon) => ({
    label: icon,
    title: c ? `Color ${c}` : 'No color',
    onClick: () => photoIds.forEach(id => setColorLabelFor(id, c)),
  });
  const flagChip = (f, icon, title) => ({
    label: icon, title,
    onClick: () => photoIds.forEach(id => setFlagFor(id, f)),
  });

  return [
    { chips: [0,1,2,3,4,5].map(rateChip) },
    { chips: [
      colorChip(null, '○'), colorChip('red', '●'), colorChip('yellow', '●'),
      colorChip('green', '●'), colorChip('blue', '●'),
    ] },
    { chips: [
      flagChip('flagged', '🏳', 'Flag'),
      flagChip('rejected', '⛔', 'Reject'),
      flagChip('none', '◯', 'Unflag'),
    ] },
    { separator: true },
    { label: 'Find Similar',        disabled: !one, disabledHint: hint,
      onClick: () => findSimilar(photoIds[0]) },
    { label: 'Open in Editor',      disabled: !one, disabledHint: hint,
      onClick: () => openInEditor(photoIds[0]) },
    { label: 'Reveal in Finder/Folder', disabled: !one, disabledHint: hint,
      onClick: () => revealPhoto(photoIds[0]) },
    { label: 'Copy Path',
      onClick: () => copyPhotoPaths(photoIds) },
    { separator: true },
    { label: 'Add Keyword…',        onClick: () => batchAddKeyword() },
    { label: 'Add to Collection…',  onClick: () => addToCollection() },
    { separator: true },
    { label: 'Delete',              onClick: () => batchDelete() },
  ];
}

function revealPhoto(photoId){
  fetch('/api/files/reveal', {
    method: 'POST', headers: {'Content-Type':'application/json'},
    body: JSON.stringify({photo_id: photoId}),
  });
}

async function copyPhotoPaths(photoIds){
  const rs = await Promise.all(photoIds.map(id =>
    fetch(`/api/photos/${id}`).then(r => r.json())));
  const paths = rs.map(r => r.path).filter(Boolean).join('\n');
  try { await navigator.clipboard.writeText(paths); } catch(e){ console.error(e); }
}

document.addEventListener('contextmenu', function(e){
  const card = e.target.closest('.grid-card');
  if (!card || !card.dataset.id) return;
  e.preventDefault();
  const pid = parseInt(card.dataset.id, 10);
  const ids = coerceSelectionOnContext(selectedPhotos, pid);
  // Reflect the coerced selection in the UI.
  updateSelectionVisual();
  openContextMenu(e, buildPhotoContextMenu(ids));
});
```

If `setRatingFor`, `setColorLabelFor`, `setFlagFor`, `findSimilar`, `openInEditor`, `updateSelectionVisual` don't exist under those names, find the equivalents in `browse.html` (grep for `updateRating`, `applyRating`, `setColorLabel`, `setFlag`, `renderSelection`, `refreshCardSelection`) and use those. Inline a one-photo wrapper as a helper if the existing code only operates on the "current detail-panel photo."

Also: the test `data-rating` assertion requires the card DOM to carry `data-rating`. If it doesn't, change the test to poll the rating star element (`.grid-card-stars .is-filled`) or the server-side value via `fetch /api/photos/<id>`.

**Step 4: Verify passing**

Expected: PASS.

**Step 5: Commit**

```
git add vireo/templates/browse.html tests/e2e/test_browse_context_menu.py
git commit -m "feat(ui): right-click context menu on browse grid cards"
```

---

## Task 7: Wire lightbox

**Files:**
- Modify: `vireo/templates/_navbar.html` (lightbox contextmenu handler; guard against the zoom-lock click handler firing).
- Test: `tests/e2e/test_lightbox_context_menu.py` (new).

**Step 1: Write the failing test**

```python
from playwright.sync_api import expect


def test_lightbox_right_click_opens_menu(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/browse")
    page.locator(".grid-card").first.dblclick()
    expect(page.locator("#lightboxOverlay")).to_be_visible()

    page.locator("#lightboxImg").click(button="right")
    menu = page.locator(".vireo-ctx-menu")
    expect(menu).to_be_visible()
    expect(menu.locator(".vireo-ctx-item", has_text="Reveal in")).to_be_visible()
    expect(menu.locator(".vireo-ctx-item", has_text="Close Lightbox")).to_be_visible()


def test_lightbox_right_click_does_not_toggle_zoom_lock(live_server, page):
    """Right-click must not trip the click-to-lock zoom handler."""
    url = live_server["url"]
    page.goto(f"{url}/browse")
    page.locator(".grid-card").first.dblclick()
    expect(page.locator("#lightboxOverlay")).to_be_visible()
    before = page.evaluate("typeof _zoomLocked !== 'undefined' ? !!_zoomLocked : false")
    page.locator("#lightboxImg").click(button="right")
    after = page.evaluate("typeof _zoomLocked !== 'undefined' ? !!_zoomLocked : false")
    assert before == after
```

**Step 2: Verify fails**

Expected: FAIL.

**Step 3: Implement**

In `_navbar.html` lightbox script block:

```javascript
document.getElementById('lightboxImg').addEventListener('contextmenu', function(e){
  e.preventDefault();
  e.stopPropagation();
  const pid = window._currentLightboxPhotoId;
  if (!pid) return;
  openContextMenu(e, buildLightboxContextMenu(pid));
});

function buildLightboxContextMenu(pid){
  return [
    { chips: [0,1,2,3,4,5].map(n => ({
        label: n === 0 ? '☆' : String(n),
        onClick: () => setRatingFor(pid, n),
    })) },
    { chips: [
      { label: '○', onClick: () => setColorLabelFor(pid, null) },
      { label: '●', onClick: () => setColorLabelFor(pid, 'red') },
      { label: '●', onClick: () => setColorLabelFor(pid, 'yellow') },
      { label: '●', onClick: () => setColorLabelFor(pid, 'green') },
      { label: '●', onClick: () => setColorLabelFor(pid, 'blue') },
    ] },
    { chips: [
      { label: '🏳', onClick: () => setFlagFor(pid, 'flagged') },
      { label: '⛔', onClick: () => setFlagFor(pid, 'rejected') },
      { label: '◯', onClick: () => setFlagFor(pid, 'none') },
    ] },
    { separator: true },
    { label: 'Find Similar',            onClick: () => findSimilar(pid) },
    { label: 'Open in Editor',          onClick: () => openInEditor(pid) },
    { label: 'Reveal in Finder/Folder', onClick: () => revealPhoto(pid) },
    { label: 'Copy Path',               onClick: () => copyPhotoPaths([pid]) },
    { separator: true },
    { label: 'Close Lightbox',          onClick: () => closeLightbox() },
  ];
}
```

Also: in the existing click handler on the lightbox that toggles `_zoomLocked`, guard:

```javascript
// If the most recent contextmenu fired < 100ms ago, ignore this click.
if (window._ctxMenuJustOpened && Date.now() - window._ctxMenuJustOpened < 120) return;
```

And set `window._ctxMenuJustOpened = Date.now()` at the top of `openContextMenu`.

Find `_currentLightboxPhotoId` / equivalent: search `browse.html` for the lightbox open path — it usually tracks current photo id in a variable like `_lbPhotoId` or `currentLightboxId`. Use the actual name.

**Step 4: Verify passing**

Expected: PASS.

**Step 5: Commit**

```
git add vireo/templates/_navbar.html tests/e2e/test_lightbox_context_menu.py
git commit -m "feat(ui): right-click context menu in lightbox"
```

---

## Task 8: Wire folder tree

**Files:**
- Modify: `vireo/templates/browse.html` (contextmenu delegation on `.tree-item[data-folder-id]`).
- Test: `tests/e2e/test_folder_tree_context_menu.py` (new).

**Step 1: Write the failing test**

```python
from playwright.sync_api import expect


def test_folder_tree_right_click_opens_menu(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/browse")

    item = page.locator(".tree-item[data-folder-id]").first
    item.wait_for(state="visible")
    item.click(button="right")

    menu = page.locator(".vireo-ctx-menu")
    expect(menu).to_be_visible()
    for label in ["Filter by this folder", "Reveal in", "Copy Path",
                  "Rescan this Folder", "Hide from this Workspace"]:
        expect(menu.locator(".vireo-ctx-item", has_text=label)).to_be_visible()


def test_folder_rescan_fires_endpoint(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/browse")
    item = page.locator(".tree-item[data-folder-id]").first
    item.click(button="right")

    with page.expect_response(lambda r: "/rescan" in r.url and r.status == 200):
        page.locator(".vireo-ctx-menu .vireo-ctx-item",
                     has_text="Rescan this Folder").click()
```

**Step 2: Verify fails**

Expected: FAIL.

**Step 3: Implement**

In `browse.html`:

```javascript
document.addEventListener('contextmenu', function(e){
  const ti = e.target.closest('.tree-item[data-folder-id]');
  if (!ti) return;
  e.preventDefault();
  const fid = parseInt(ti.dataset.folderId, 10);
  const name = ti.querySelector('span:not(.tree-toggle)')?.textContent || '';
  openContextMenu(e, [
    { label: 'Filter by this folder', onClick: () => filterByFolder(fid) },
    { label: 'Expand All Children',   onClick: () => expandFolderTree(fid) },
    { label: 'Collapse All Children', onClick: () => collapseFolderTree(fid) },
    { separator: true },
    { label: 'Reveal in Finder/Folder',
      onClick: () => revealFolder(fid) },
    { label: 'Copy Path',
      onClick: () => copyFolderPath(fid) },
    { separator: true },
    { label: 'Hide from this Workspace',
      onClick: () => hideFolderFromWorkspace(fid) },
    { label: 'Rescan this Folder',
      onClick: () => fetch(`/api/folders/${fid}/rescan`, {method:'POST'}) },
  ]);
});

function revealFolder(fid){
  fetch(`/api/folders/${fid}/reveal`, {method:'POST'});
}
async function copyFolderPath(fid){
  const r = await fetch(`/api/folders/${fid}`); const f = await r.json();
  if (f.path) await navigator.clipboard.writeText(f.path);
}
```

If `expandFolderTree`, `collapseFolderTree`, `hideFolderFromWorkspace` don't exist, either:
1. Skip them from the menu for this first pass, or
2. Implement them as thin helpers (e.g. `hideFolderFromWorkspace` = `POST /api/workspaces/current/folders/<id>/hide`).

For `revealFolder`: add a sibling endpoint to `/api/files/reveal` that takes a folder id and reveals its root path directly — OR reuse `/api/files/reveal` by adding support for `folder_id` in that endpoint. Prefer the latter: amend `api_files_reveal` to accept `{folder_id}` as an alternative to `{photo_id}`, resolving the path accordingly, and extend the test file from Task 3 with a folder case.

**Step 4: Verify passing**

Expected: PASS.

**Step 5: Commit**

```
git add vireo/templates/browse.html vireo/app.py vireo/tests/test_reveal_api.py tests/e2e/test_folder_tree_context_menu.py
git commit -m "feat(ui): right-click context menu on folder tree"
```

---

## Task 9: Wire collection sidebar item

**Files:**
- Modify: `vireo/templates/browse.html`.
- Test: `tests/e2e/test_collection_context_menu.py` (new).

**Step 1: Write the failing test**

```python
from playwright.sync_api import expect


def test_collection_right_click_shows_menu(live_server, page):
    url = live_server["url"]
    # Seed a collection via API.
    page.goto(f"{url}/browse")
    page.evaluate("""
        fetch('/api/collections', {
            method:'POST', headers:{'Content-Type':'application/json'},
            body: JSON.stringify({name:'Test Pick'})
        }).then(() => location.reload())
    """)
    page.wait_for_load_state("networkidle")

    item = page.locator(".tree-item", has_text="Test Pick").first
    item.wait_for(state="visible")
    item.click(button="right")

    menu = page.locator(".vireo-ctx-menu")
    expect(menu).to_be_visible()
    for label in ["Filter by this Collection", "Rename", "Duplicate",
                  "Delete Collection"]:
        expect(menu.locator(".vireo-ctx-item", has_text=label)).to_be_visible()
```

**Step 2: Verify fails**

Expected: FAIL.

**Step 3: Implement**

Add a `data-collection-id` attribute to each rendered collection tree-item (in the `renderCollectionList` code), then delegate contextmenu for that attribute. Reuse `filterByCollection`, existing rename/delete helpers if they exist; otherwise add minimal `renameCollection(cid)` and `deleteCollection(cid)` helpers using existing endpoints.

**Step 4: Verify passing**

Expected: PASS.

**Step 5: Commit**

```
git add vireo/templates/browse.html tests/e2e/test_collection_context_menu.py
git commit -m "feat(ui): right-click context menu on collection sidebar"
```

---

## Task 10: Wire keyword row

**Files:**
- Modify: `vireo/templates/keywords.html`.
- Test: `tests/e2e/test_keyword_context_menu.py` (new).

**Step 1: Write the failing test**

```python
from playwright.sync_api import expect


def test_keyword_right_click_menu(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/keywords")
    row = page.locator("tr[data-id]").first
    row.wait_for(state="visible")
    row.click(button="right")

    menu = page.locator(".vireo-ctx-menu")
    expect(menu).to_be_visible()
    for label in ["Rename", "Change Type", "Filter Browse by this Keyword",
                  "Show Photos with this Keyword", "Delete"]:
        expect(menu.locator(".vireo-ctx-item", has_text=label)).to_be_visible()
```

**Step 2: Verify fails**

Expected: FAIL.

**Step 3: Implement**

In `keywords.html`, delegate `contextmenu` on `tr[data-id]`. Apply Finder-style coercion to `selectedIds`. Call existing `renameKeyword`, the existing type dropdown opener, and bulk delete path. "Filter Browse by this Keyword" → `window.location.href = '/browse?keyword=' + encodeURIComponent(name)`.

**Step 4: Verify passing**

Expected: PASS.

**Step 5: Commit**

```
git add vireo/templates/keywords.html tests/e2e/test_keyword_context_menu.py
git commit -m "feat(ui): right-click context menu on keyword rows"
```

---

## Task 11: Wire review photo card

**Files:**
- Modify: `vireo/templates/review.html`.
- Test: `tests/e2e/test_review_context_menu.py` (new).

**Step 1: Write the failing test**

```python
from playwright.sync_api import expect


def test_review_card_right_click_menu(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/review")
    card = page.locator(".card[data-pred-id]").first
    card.wait_for(state="visible")
    card.click(button="right")

    menu = page.locator(".vireo-ctx-menu")
    expect(menu).to_be_visible()
    for label in ["Accept as", "Not", "Open in Lightbox",
                  "Reveal in", "Copy Path"]:
        expect(menu.locator(".vireo-ctx-item", has_text=label)).to_be_visible()
```

**Step 2: Verify fails**

Expected: FAIL.

**Step 3: Implement**

Delegate `contextmenu` on `.card[data-pred-id]`. Menu items call the existing `acceptPrediction(predId)` and `rejectPrediction(predId)`, plus rating/flag chips and the reveal/copy/lightbox trio. No `selectedPhotos` coercion — review grid has no multi-select.

**Step 4: Verify passing**

Expected: PASS.

**Step 5: Commit**

```
git add vireo/templates/review.html tests/e2e/test_review_context_menu.py
git commit -m "feat(ui): right-click context menu on review cards"
```

---

## Task 12: Wire burst group modal photo

**Files:**
- Modify: `vireo/templates/review.html` (burst group section).
- Test: `tests/e2e/test_burst_group_context_menu.py` (new — gated on a seeded burst group, may skip if fixture doesn't supply one).

**Step 1: Write the failing test**

```python
import pytest
from playwright.sync_api import expect


def test_burst_group_card_right_click_menu(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/review")
    # Open burst modal via the test-only button if available.
    btn = page.locator("[data-open-burst]").first
    if btn.count() == 0:
        pytest.skip("no burst group seeded")
    btn.click()

    expect(page.locator("#grmOverlay")).to_be_visible()
    card = page.locator(".grm-card[data-photo-id]").first
    card.click(button="right")

    menu = page.locator(".vireo-ctx-menu")
    expect(menu).to_be_visible()
    for label in ["Move to Picks", "Move to Rejects", "Move to Candidates",
                  "Open in Lightbox", "Reveal in", "Remove from Group"]:
        expect(menu.locator(".vireo-ctx-item", has_text=label)).to_be_visible()
```

**Step 2: Verify fails**

Expected: FAIL or SKIPPED (if no burst seed). If skipped, ask the user to seed a burst group in e2e `conftest.py`. Otherwise proceed.

**Step 3: Implement**

In `review.html` burst section: delegate `contextmenu` on `.grm-card[data-photo-id]`. Menu calls `grmMoveUp`, `grmMoveDown`, `grmMoveCandidate`, `grmRemoveFromGroup`, plus the rating/flag chips and reveal/copy/lightbox trio.

**Step 4: Verify passing**

Expected: PASS (or skipped — that's acceptable; the wiring is still verified by manual smoke-test).

**Step 5: Commit**

```
git add vireo/templates/review.html tests/e2e/test_burst_group_context_menu.py
git commit -m "feat(ui): right-click context menu in burst group modal"
```

---

## Task 13: Full suite + manual smoke-test + PR

**Step 1: Run the full test suite**

```
python -m pytest tests/ vireo/tests/ -q
```
Expected: all pass.

**Step 2: Manual smoke-test (per user-first-testing memory)**

Start the dev server, open browse, review, keywords pages in a real browser. For each surface, right-click at least one item and verify:
- Menu opens at cursor.
- Outside-click dismisses.
- Escape dismisses.
- A rating / flag action actually changes state visible in the detail panel.
- Reveal in Finder opens Finder on the correct file (macOS).
- Copy Path paste into terminal yields the right path(s).

**Step 3: Push and open PR**

```
git push -u origin right-click-review
gh pr create --title "Right-click context menus across all surfaces" --body "$(cat <<'EOF'
## Summary
- Adds a shared floating context-menu component (`openContextMenu`) used across seven surfaces.
- Finder-style selection coupling: right-clicking an item outside the selection replaces selection with that item.
- Menus on photo card (browse + review), lightbox, folder tree, keyword row, collection item, burst group modal.
- New endpoints: `/api/files/reveal`, `/api/folders/<id>/rescan`, `/api/collections/<id>/duplicate`.
- Client-side Copy Path via `navigator.clipboard`.

Additive only — every existing keyboard shortcut and button is unchanged.

See `docs/plans/2026-04-22-context-menus-design.md` for the design write-up.

## Test plan
- [x] `python -m pytest tests/ vireo/tests/ -q` passes
- [x] Manual browser smoke-test on all seven surfaces
- [x] Reveal in Finder opens the correct file on macOS
EOF
)"
```

---

## Notes for the implementer

- The detailed code in each "Implement" step assumes helper names like `setRatingFor`, `setColorLabelFor`, `setFlagFor`, `findSimilar`, `openInEditor`. Grep `vireo/templates/browse.html` for the actual single-photo helpers these pages already use for the detail panel; if none exist, write two-line wrappers that call the existing fetch endpoints directly.
- Don't duplicate the detail-panel UI in the menu. Only the rating/color/flag chips + the genuinely new actions.
- The Finder-style coercion helper (`coerceSelectionOnContext`) must be called *before* the menu opens so the right-click gesture updates the visible selection state first.
- Favor event delegation on `document` over per-card listeners — the grids re-render frequently.
- Prefer editing existing files. No new JS/CSS files.
