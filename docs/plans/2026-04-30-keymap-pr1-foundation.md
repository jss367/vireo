# Keymap PR 1 — Foundation Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Extract Vireo's scattered keyboard handling into a single `keymap.js` registry + dispatcher, with a unified `Esc` stack and consistent input-focus suppression. No new user-visible features — this is plumbing for PRs 2–4.

**Architecture:** New module `vireo/static/keymap.js` exposes `Keymap.register(scope, shortcut)` and `Keymap.pushEsc(handler)`. A single dispatcher owns `keydown` and routes to registered actions, suppressing single-letter keys when an `<input>`/`<textarea>`/`[contenteditable]` is focused. The existing per-page handlers in `_navbar.html` (navigation shortcuts at lines 1981–2001) and the multiple `Esc` handlers (lines 2100–2116, 3080–3129, 1389–1396) get migrated onto the registry. Per-page handlers in `browse.html`/`review.html`/etc. are NOT touched in PR 1 — they migrate in later PRs.

**Tech Stack:** Vanilla JS (ES5-compatible — Vireo doesn't use ES modules in templates), Flask + Jinja2 templates, Playwright for browser-side tests, pytest for backend.

**Reference design:** `docs/plans/2026-04-30-keymap-design.md`

---

## Pre-flight

**This plan executes in a NEW worktree off `main`, not on the `keymap-design` branch.** The design branch is for the design doc only.

```bash
cd /Users/julius/git/vireo  # main checkout
git fetch origin main
git worktree add -b claude/keymap-foundation ../vireo-keymap-foundation origin/main
cd ../vireo-keymap-foundation
```

All file paths below are relative to the worktree root.

---

## Existing code references (read these first)

The plan refers back to these constantly. Read them before Task 1 so you have context.

- **Existing dispatcher in navbar:** `vireo/templates/_navbar.html:1981-2001` (the `keydown` listener with `keyToHref` lookup)
- **Existing nav defaults + config merge:** `vireo/templates/_navbar.html:1922-1935` (`NAV_ROUTES`, `NAV_DEFAULTS`) and `:2005-2018` (config fetch + merge)
- **Existing `parseShortcut` + `matchesShortcut`:** `vireo/templates/_navbar.html:3149-3167`
- **Existing Esc handlers (three of them):**
  - Lightbox/overlay Esc + lightbox keys: `vireo/templates/_navbar.html:3080-3129`
  - Shortcuts cheat sheet Esc + `?` opener: `vireo/templates/_navbar.html:2100-2116`
  - Context menu Esc (capture-phase): `vireo/templates/_navbar.html:1389-1396`
- **Existing JS asset loading:** `vireo/templates/_navbar.html:4951-4957`
- **Backend `/api/config` endpoint:** `vireo/app.py:3988-4032` (no changes needed; we read from this)
- **Existing Playwright e2e setup:** `tests/e2e/conftest.py` (provides `live_server` fixture; `page` fixture is from `pytest-playwright`)
- **Existing page-load smoke test:** `tests/e2e/test_page_loads.py` (we extend this slightly)

---

## Task 1: Create `keymap.js` with helper utilities

Create the module skeleton with two pure functions migrated verbatim from `_navbar.html`. Pure functions — no side effects yet.

**Files:**
- Create: `vireo/static/keymap.js`
- Create: `tests/e2e/test_keymap.py`

**Step 1: Write the failing test**

```python
# tests/e2e/test_keymap.py
"""End-to-end tests for the keymap registry and dispatcher."""

import pytest


def test_keymap_globals_exposed(live_server, page):
    """Loading any page exposes the Keymap module on window."""
    url = live_server["url"]
    page.goto(f"{url}/browse", timeout=5000)
    page.wait_for_load_state("networkidle")

    # Module is loaded
    assert page.evaluate("typeof window.Keymap") == "object"

    # Helpers are exposed
    assert page.evaluate("typeof window.Keymap.parseShortcut") == "function"
    assert page.evaluate("typeof window.Keymap.matchesShortcut") == "function"
    assert page.evaluate("typeof window.Keymap.isInputFocused") == "function"
```

**Step 2: Run test to verify it fails**

```bash
python -m pytest tests/e2e/test_keymap.py::test_keymap_globals_exposed -v
```
Expected: FAIL with `Keymap` undefined (script not loaded yet, file doesn't exist).

**Step 3: Write minimal implementation**

Create `vireo/static/keymap.js`:

```javascript
/**
 * Vireo keymap module — single source of truth for keyboard shortcuts.
 *
 * Public API (this PR):
 *   Keymap.parseShortcut(str)        -> {key, ctrl, meta, shift, alt}
 *   Keymap.matchesShortcut(event, str)
 *   Keymap.isInputFocused()          -> bool
 *
 * More API lands in subsequent tasks.
 */
(function (window) {
  'use strict';

  function parseShortcut(str) {
    var parts = str.toLowerCase().split('+');
    var key = parts.pop();
    var mods = { ctrl: false, meta: false, shift: false, alt: false };
    parts.forEach(function (m) { if (m in mods) mods[m] = true; });
    return { key: key, ctrl: mods.ctrl, meta: mods.meta, shift: mods.shift, alt: mods.alt };
  }

  function matchesShortcut(e, shortcutStr) {
    if (!shortcutStr) return false;
    var sc = parseShortcut(shortcutStr);
    if (e.key.toLowerCase() !== sc.key) return false;
    var wantCtrl = sc.ctrl || sc.meta;
    var hasCtrl = e.ctrlKey || e.metaKey;
    if (wantCtrl !== hasCtrl) return false;
    if (sc.shift !== e.shiftKey) return false;
    if (sc.alt !== e.altKey) return false;
    return true;
  }

  function isInputFocused() {
    var el = document.activeElement;
    if (!el) return false;
    var tag = el.tagName;
    if (tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT') return true;
    if (el.isContentEditable) return true;
    return false;
  }

  window.Keymap = {
    parseShortcut: parseShortcut,
    matchesShortcut: matchesShortcut,
    isInputFocused: isInputFocused
  };
})(window);
```

Then add `<script src="/static/keymap.js"></script>` to `vireo/templates/_navbar.html` at line 4951 (right after `vireo-utils.js`, before `tauri-bridge.js`):

```html
<script src="/static/vireo-utils.js"></script>
<script src="/static/keymap.js"></script>
<script src="/static/tauri-bridge.js"></script>
```

**Step 4: Run test to verify it passes**

```bash
python -m pytest tests/e2e/test_keymap.py::test_keymap_globals_exposed -v
```
Expected: PASS.

**Step 5: Commit**

```bash
git add vireo/static/keymap.js vireo/templates/_navbar.html tests/e2e/test_keymap.py
git commit -m "keymap: add Keymap module skeleton with parse/match/focus helpers"
```

---

## Task 2: Add the registry API

Add `Keymap.register(scope, shortcut)` and `Keymap.shortcutsForScope(scope)`. No dispatcher yet — just the data store.

**Files:**
- Modify: `vireo/static/keymap.js`
- Modify: `tests/e2e/test_keymap.py`

**Step 1: Write the failing test**

Append to `tests/e2e/test_keymap.py`:

```python
def test_keymap_register_and_lookup(live_server, page):
    """register() stores shortcuts; shortcutsForScope() returns them merged with global."""
    url = live_server["url"]
    page.goto(f"{url}/browse", timeout=5000)
    page.wait_for_load_state("networkidle")

    page.evaluate("""
        window.Keymap.register('global', {
            key: 'g', name: 'global-test', description: 'd',
            category: 'Navigation', action: function() {}
        });
        window.Keymap.register('browse', {
            key: 'b', name: 'browse-test', description: 'd',
            category: 'Edit', action: function() {}
        });
    """)

    global_only = page.evaluate("window.Keymap.shortcutsForScope('global').map(s => s.name)")
    assert global_only == ["global-test"]

    browse_scope = page.evaluate("window.Keymap.shortcutsForScope('browse').map(s => s.name)")
    # browse scope returns its own shortcuts plus globals
    assert set(browse_scope) == {"global-test", "browse-test"}
```

**Step 2: Run test to verify it fails**

```bash
python -m pytest tests/e2e/test_keymap.py::test_keymap_register_and_lookup -v
```
Expected: FAIL with `register is not a function`.

**Step 3: Write minimal implementation**

In `vireo/static/keymap.js`, inside the IIFE before the `window.Keymap = ...` assignment, add:

```javascript
  // scope -> array of shortcut definitions
  var _registry = { global: [] };

  function register(scope, shortcut) {
    if (!_registry[scope]) _registry[scope] = [];
    _registry[scope].push(shortcut);
  }

  function shortcutsForScope(scope) {
    var globals = _registry.global || [];
    if (scope === 'global' || !_registry[scope]) return globals.slice();
    return _registry[scope].concat(globals);
  }
```

Update the export:

```javascript
  window.Keymap = {
    parseShortcut: parseShortcut,
    matchesShortcut: matchesShortcut,
    isInputFocused: isInputFocused,
    register: register,
    shortcutsForScope: shortcutsForScope
  };
```

**Step 4: Run test to verify it passes**

```bash
python -m pytest tests/e2e/test_keymap.py::test_keymap_register_and_lookup -v
```
Expected: PASS.

**Step 5: Commit**

```bash
git add vireo/static/keymap.js tests/e2e/test_keymap.py
git commit -m "keymap: add scoped shortcut registry"
```

---

## Task 3: Add the central dispatcher

Wire a single `keydown` listener that consults the registry, suppresses on input focus, and fires the registered `action`. Includes the page-vs-global precedence rule from the existing dispatcher (`_navbar.html:1989-1995`).

**Files:**
- Modify: `vireo/static/keymap.js`
- Modify: `tests/e2e/test_keymap.py`

**Step 1: Write the failing test**

Append to `tests/e2e/test_keymap.py`:

```python
def test_dispatcher_fires_registered_action(live_server, page):
    """Pressing a registered key fires its action; suppressed when input is focused."""
    url = live_server["url"]
    page.goto(f"{url}/browse", timeout=5000)
    page.wait_for_load_state("networkidle")

    page.evaluate("""
        window._kmTestFired = 0;
        window.Keymap.register('global', {
            key: 'q', name: 'test-q', description: '', category: 'System',
            action: function() { window._kmTestFired += 1; }
        });
        window.Keymap.setScope('global');
    """)

    page.keyboard.press("q")
    assert page.evaluate("window._kmTestFired") == 1

    # Focused input suppresses the shortcut
    page.evaluate("""
        var i = document.createElement('input');
        i.id = '_kmTestInput';
        document.body.appendChild(i);
        i.focus();
    """)
    page.keyboard.press("q")
    assert page.evaluate("window._kmTestFired") == 1  # unchanged
```

**Step 2: Run test to verify it fails**

```bash
python -m pytest tests/e2e/test_keymap.py::test_dispatcher_fires_registered_action -v
```
Expected: FAIL — `setScope` doesn't exist and no dispatcher is wired.

**Step 3: Write minimal implementation**

Add to `vireo/static/keymap.js` (inside the IIFE):

```javascript
  var _currentScope = 'global';

  function setScope(scope) { _currentScope = scope; }
  function getScope() { return _currentScope; }

  function _dispatch(e) {
    if (isInputFocused()) return;
    var candidates = shortcutsForScope(_currentScope);
    for (var i = 0; i < candidates.length; i++) {
      var sc = candidates[i];
      if (matchesShortcut(e, sc.key)) {
        e.preventDefault();
        try { sc.action(e); } catch (err) { console.error('Keymap action error', err); }
        return;
      }
    }
  }

  document.addEventListener('keydown', _dispatch);
```

Update the export to include `setScope`/`getScope`:

```javascript
  window.Keymap = {
    parseShortcut: parseShortcut,
    matchesShortcut: matchesShortcut,
    isInputFocused: isInputFocused,
    register: register,
    shortcutsForScope: shortcutsForScope,
    setScope: setScope,
    getScope: getScope
  };
```

**Step 4: Run test to verify it passes**

```bash
python -m pytest tests/e2e/test_keymap.py::test_dispatcher_fires_registered_action -v
```
Expected: PASS.

**Step 5: Commit**

```bash
git add vireo/static/keymap.js tests/e2e/test_keymap.py
git commit -m "keymap: add central keydown dispatcher with input-focus suppression"
```

---

## Task 4: Add the Esc stack

Single `Esc` owner: handlers push themselves onto a stack; pressing `Esc` invokes the top handler only. Replaces the racing `Esc` listeners.

**Files:**
- Modify: `vireo/static/keymap.js`
- Modify: `tests/e2e/test_keymap.py`

**Step 1: Write the failing test**

Append to `tests/e2e/test_keymap.py`:

```python
def test_esc_stack_unwinds_top_first(live_server, page):
    """pushEsc registers handlers; Esc invokes only the top one each press."""
    url = live_server["url"]
    page.goto(f"{url}/browse", timeout=5000)
    page.wait_for_load_state("networkidle")

    page.evaluate("""
        window._kmEscOrder = [];
        window._kmEscToken1 = window.Keymap.pushEsc(function() { window._kmEscOrder.push('first'); });
        window._kmEscToken2 = window.Keymap.pushEsc(function() { window._kmEscOrder.push('second'); });
    """)

    page.keyboard.press("Escape")
    assert page.evaluate("window._kmEscOrder") == ["second"]

    page.keyboard.press("Escape")
    assert page.evaluate("window._kmEscOrder") == ["second", "first"]

    page.keyboard.press("Escape")
    assert page.evaluate("window._kmEscOrder") == ["second", "first"]  # stack empty


def test_esc_stack_remove_by_token(live_server, page):
    """popEsc(token) removes a specific handler regardless of position."""
    url = live_server["url"]
    page.goto(f"{url}/browse", timeout=5000)
    page.wait_for_load_state("networkidle")

    page.evaluate("""
        window._kmEscOrder = [];
        var t1 = window.Keymap.pushEsc(function() { window._kmEscOrder.push('first'); });
        var t2 = window.Keymap.pushEsc(function() { window._kmEscOrder.push('second'); });
        window.Keymap.popEsc(t2);
    """)

    page.keyboard.press("Escape")
    assert page.evaluate("window._kmEscOrder") == ["first"]
```

**Step 2: Run test to verify it fails**

```bash
python -m pytest tests/e2e/test_keymap.py -k esc -v
```
Expected: FAIL — `pushEsc` undefined.

**Step 3: Write minimal implementation**

Add to `vireo/static/keymap.js`:

```javascript
  var _escStack = [];
  var _escNextToken = 1;

  function pushEsc(handler) {
    var token = _escNextToken++;
    _escStack.push({ token: token, handler: handler });
    return token;
  }

  function popEsc(token) {
    for (var i = _escStack.length - 1; i >= 0; i--) {
      if (_escStack[i].token === token) {
        _escStack.splice(i, 1);
        return true;
      }
    }
    return false;
  }

  function _handleEsc(e) {
    if (e.key !== 'Escape') return false;
    if (_escStack.length === 0) return false;
    var top = _escStack.pop();
    e.preventDefault();
    e.stopPropagation();
    try { top.handler(e); } catch (err) { console.error('Esc handler error', err); }
    return true;
  }
```

Modify `_dispatch` to handle Esc first:

```javascript
  function _dispatch(e) {
    if (_handleEsc(e)) return;
    if (isInputFocused()) return;
    // ... rest unchanged
  }
```

Export the new methods:

```javascript
    pushEsc: pushEsc,
    popEsc: popEsc,
```

**Step 4: Run test to verify it passes**

```bash
python -m pytest tests/e2e/test_keymap.py -k esc -v
```
Expected: PASS.

**Step 5: Commit**

```bash
git add vireo/static/keymap.js tests/e2e/test_keymap.py
git commit -m "keymap: add Esc stack with token-based removal"
```

---

## Task 5: Add page-vs-global precedence

The existing dispatcher (`_navbar.html:1989-1995`) lets page-scoped shortcuts shadow global ones with the same key. Match that behavior in our dispatcher: when iterating candidates, page-scope matches win.

**Files:**
- Modify: `vireo/static/keymap.js`
- Modify: `tests/e2e/test_keymap.py`

**Step 1: Write the failing test**

Append to `tests/e2e/test_keymap.py`:

```python
def test_page_scope_shadows_global_for_same_key(live_server, page):
    """When global and page scopes register the same key, page wins."""
    url = live_server["url"]
    page.goto(f"{url}/browse", timeout=5000)
    page.wait_for_load_state("networkidle")

    page.evaluate("""
        window._kmFired = '';
        window.Keymap.register('global', {
            key: 'p', name: 'g', description: '', category: 'System',
            action: function() { window._kmFired = 'global'; }
        });
        window.Keymap.register('browse', {
            key: 'p', name: 'b', description: '', category: 'Edit',
            action: function() { window._kmFired = 'page'; }
        });
        window.Keymap.setScope('browse');
    """)

    page.keyboard.press("p")
    assert page.evaluate("window._kmFired") == "page"
```

**Step 2: Run test to verify it fails**

`shortcutsForScope` currently returns page first, then globals — so `_dispatch` already iterates page-first. The test should actually pass. Run it:

```bash
python -m pytest tests/e2e/test_keymap.py::test_page_scope_shadows_global_for_same_key -v
```
If it PASSES on first run, that's fine — the existing iteration order already implements precedence. Skip step 3 and go to step 5 (commit just the test, with a message documenting the behavior is locked in).

If it FAILS (e.g., dispatcher iterates globals first), reorder the candidates list in `shortcutsForScope` so page comes first, or re-loop in `_dispatch` with a two-pass strategy.

**Step 3 (if needed): Adjust ordering**

`shortcutsForScope` already returns `_registry[scope].concat(globals)` so page is first. No change expected.

**Step 4: Re-run**

```bash
python -m pytest tests/e2e/test_keymap.py -v
```
Expected: ALL PASS.

**Step 5: Commit**

```bash
git add tests/e2e/test_keymap.py
git commit -m "keymap: lock in page-shadows-global precedence with test"
```

---

## Task 6: Migrate global navbar nav shortcuts to the registry

Replace the `keyToHref` dispatcher at `_navbar.html:1981-2001` with `Keymap.register('global', ...)` calls. Each navigation entry becomes one registered shortcut. The action fires `window.location.href = route`.

**Files:**
- Modify: `vireo/templates/_navbar.html` (replace lines 1981-2001 dispatch + extend the existing config-merge block at 2005-2018)
- Add test: `tests/e2e/test_keymap.py`

**Step 1: Write the failing test**

Append to `tests/e2e/test_keymap.py`:

```python
def test_navbar_nav_shortcuts_registered_globally(live_server, page):
    """Each NAV_ROUTES entry is registered as a global Keymap shortcut after config load."""
    url = live_server["url"]
    page.goto(f"{url}/browse", timeout=5000)
    page.wait_for_load_state("networkidle")

    # After the nav shortcut bootstrap runs, every nav entry should be in the global scope.
    names = page.evaluate("""
        window.Keymap.shortcutsForScope('global')
            .filter(s => s.category === 'Navigation')
            .map(s => s.name)
    """)
    expected = {
        'pipeline', 'lightroom', 'pipeline_review', 'review', 'cull',
        'browse', 'map', 'variants', 'dashboard', 'audit', 'compare',
        'workspace', 'shortcuts', 'settings', 'keywords'
    }
    assert expected.issubset(set(names))


def test_pressing_b_navigates_to_browse(live_server, page):
    """Pressing 'b' from a non-browse page navigates to /browse."""
    url = live_server["url"]
    page.goto(f"{url}/cull", timeout=5000)
    page.wait_for_load_state("networkidle")
    page.keyboard.press("b")
    page.wait_for_url(f"{url}/browse", timeout=3000)
```

**Step 2: Run test to verify it fails**

```bash
python -m pytest tests/e2e/test_keymap.py -k "navbar_nav or pressing_b" -v
```
Expected: at least the `shortcuts` filter test fails (no Navigation category yet); `pressing_b` may pass because the legacy dispatcher still fires — that's OK, we'll verify it still passes after migration.

**Step 3: Write the migration**

In `vireo/templates/_navbar.html`:

Find the existing block that merges `NAV_DEFAULTS` with config (lines 2005-2018) and the dispatcher (lines 1981-2001). Replace the dispatcher with registry calls. Inside the `fetch('/api/config').then(...)` callback that merges shortcuts, add:

```javascript
// After window._vireoShortcuts.navigation is populated:
var navMap = window._vireoShortcuts.navigation || {};
Object.keys(NAV_ROUTES).forEach(function (action) {
  var key = navMap[action];
  if (!key) return;
  var route = NAV_ROUTES[action];
  window.Keymap.register('global', {
    key: key,
    name: action,
    description: 'Go to ' + action.replace(/_/g, ' '),
    category: 'Navigation',
    action: function () {
      // Don't navigate if we're already on this page (preserves existing behavior)
      if (window.location.pathname === route) return;
      window.location.href = route;
    }
  });
});
```

Then **delete** the old `document.addEventListener('keydown', ...)` block that used `keyToHref` (lines 1981-2001). Also delete the `keyToHref` table-build code that was feeding it. **Keep** `NAV_ROUTES` and `NAV_DEFAULTS` as data (other code may still read them; we'll audit in a later PR).

Set the page scope on boot. Find where `pageCtx` is determined in `_navbar.html` (search for `pageCtx`) and add right after:

```javascript
if (pageCtx) window.Keymap.setScope(pageCtx);
```

**Step 4: Run all tests + manual smoke**

```bash
python -m pytest tests/e2e/test_keymap.py -v
python -m pytest tests/e2e/test_page_loads.py -v
```
Expected: ALL PASS. Then manually:

- Open `http://localhost:8080/cull`. Press `b`. Should land on `/browse`.
- Open `http://localhost:8080/browse`. Press `b`. Should NOT navigate (already there).
- Open `http://localhost:8080/browse`. Press `m`. Should land on `/map`.
- Open `http://localhost:8080/browse`. Click into the search input. Press `b`. Should NOT navigate (input focused).

**Step 5: Commit**

```bash
git add vireo/templates/_navbar.html tests/e2e/test_keymap.py
git commit -m "keymap: migrate navbar navigation shortcuts to registry"
```

---

## Task 7: Migrate `_navbar.html` Esc handlers to the Esc stack

Three current handlers (`_navbar.html:3080-3129`, `:2100-2116`, `:1389-1396`) each handle `Esc` for different overlays. Migrate each to call `Keymap.pushEsc(handler)` when the overlay opens, and `Keymap.popEsc(token)` when it closes.

The lightbox handler at lines 3080-3129 also handles `ArrowLeft`/`ArrowRight`/`+`/`-`/`0`/`B`/`Z` — **leave those alone** in this task. We're only moving the `Esc` part.

**Files:**
- Modify: `vireo/templates/_navbar.html` (three regions)
- Modify: `tests/e2e/test_keymap.py`

**Step 1: Write the failing test**

Append to `tests/e2e/test_keymap.py`:

```python
def test_esc_closes_shortcuts_cheat_sheet(live_server, page):
    """Opening the cheat sheet pushes an Esc handler; pressing Esc closes it."""
    url = live_server["url"]
    page.goto(f"{url}/browse", timeout=5000)
    page.wait_for_load_state("networkidle")

    page.keyboard.press("?")
    sheet = page.locator("#shortcutsCheatSheet")
    expect_open = sheet.evaluate("el => el.classList.contains('open')")
    assert expect_open is True

    page.keyboard.press("Escape")
    expect_closed = sheet.evaluate("el => el.classList.contains('open')")
    assert expect_closed is False
```

(Use `from playwright.sync_api import expect` if you want assertion helpers; the raw `evaluate` form above is fine.)

**Step 2: Run test to verify it fails or passes**

```bash
python -m pytest tests/e2e/test_keymap.py::test_esc_closes_shortcuts_cheat_sheet -v
```
This test may PASS on first run since the existing handler already does this. That's fine — it locks in behavior we must preserve through the migration.

**Step 3: Migrate each Esc handler**

For each of the three sites, the pattern is the same.

**Site 1 — Lightbox (`_navbar.html:3080-3129`):** The current handler unconditionally calls `closeLightbox`/`closePipeline`/`closeSimilar`/`closeInatModal`/`closeHelpModal`/`closeReportModal` on every `Esc`. Replace with: each `open*` function calls `Keymap.pushEsc(close*)`; each `close*` function calls `Keymap.popEsc(token)` and stores the token on a module-level variable.

Example for lightbox — find `openLightbox` in `_navbar.html` and add at its top:

```javascript
if (window._lbEscToken) Keymap.popEsc(window._lbEscToken);
window._lbEscToken = Keymap.pushEsc(function () { closeLightbox(); });
```

Find `closeLightbox` and add at its top:

```javascript
if (window._lbEscToken) { Keymap.popEsc(window._lbEscToken); window._lbEscToken = null; }
```

Repeat the pattern for `openPipeline`/`closePipeline`, `openSimilar`/`closeSimilar`, `openInatModal`/`closeInatModal`, `openHelpModal`/`closeHelpModal`, `openReportModal`/`closeReportModal`.

After all six are converted, **delete** the `if (e.key === 'Escape') { closeLightbox(); ... }` lines at the top of the lightbox keydown listener (the listener stays; only the Esc cascade is removed). The other lightbox keys (arrows/+/-/0/B/Z) remain in that listener for now.

**Site 2 — Shortcuts cheat sheet (`_navbar.html:2100-2116`):** This handler does two things — opens on `?` and closes on `Esc`. Convert the close path: in `openShortcutsSheet`, add `window._sheetEscToken = Keymap.pushEsc(closeShortcutsSheet);`. In `closeShortcutsSheet`, pop it. Remove the `if (e.key === 'Escape')` branch from the listener. Keep the `?` opener and the `e.preventDefault(); e.stopImmediatePropagation();` consume-all-keys behavior while sheet is open (that part isn't an Esc concern).

**Site 3 — Context menu (`_navbar.html:1389-1396`):** This Esc handler is on a *capture-phase* listener — it cancels the menu without letting browse.html's Esc fire. Convert: in the function that opens the context menu, push an Esc handler. In `closeContextMenu`, pop it. Delete the capture-phase Esc listener entirely.

**Step 4: Run all tests + manual smoke**

```bash
python -m pytest tests/e2e/test_keymap.py -v
python -m pytest tests/e2e/test_page_loads.py -v
```
Expected: ALL PASS. Then manually:

- Browse page: open lightbox on a photo, press Esc → closes lightbox.
- Browse page: open lightbox, then open the shortcuts cheat sheet (`?`), press Esc → closes the sheet but lightbox stays open.
- Browse page: right-click a photo card to open context menu, press Esc → closes context menu but selection stays.

**Step 5: Commit**

```bash
git add vireo/templates/_navbar.html tests/e2e/test_keymap.py
git commit -m "keymap: migrate _navbar.html Esc handlers to Esc stack"
```

---

## Task 8: Final verification

Run the full test suite and execute the manual checklist. No code changes unless something breaks.

**Step 1: Run the curated test command from CLAUDE.md**

```bash
python -m pytest tests/test_workspaces.py vireo/tests/test_db.py vireo/tests/test_app.py vireo/tests/test_photos_api.py vireo/tests/test_edits_api.py vireo/tests/test_jobs_api.py vireo/tests/test_darktable_api.py vireo/tests/test_config.py -v
```
Expected: PASS (modulo the pre-existing failures noted in memory `project_preexisting_test_failures.md` — don't block on those).

**Step 2: Run all e2e tests**

```bash
python -m pytest tests/e2e/ -v
```
Expected: PASS.

**Step 3: Manual checklist**

Start the app:
```bash
python vireo/app.py --db ~/.vireo/vireo.db --port 8080
```

For Browse, Cull, Review, and Lightbox — verify each:
- [ ] Page-letter nav still works (`b`, `c`, `r`, `m`, etc.)
- [ ] No nav fires when typing in any input
- [ ] `?` opens shortcuts cheat sheet
- [ ] `Esc` closes the topmost overlay only
- [ ] Lightbox arrow keys, `+`/`-`/`0`, `B`, `Z` still work
- [ ] Browse: 0-5 ratings, P/X/U flags still work
- [ ] Review: A accept, S skip still work
- [ ] Misses: J/K navigation still works (untouched in this PR)

**Step 4: Open the PR**

```bash
gh pr create --base main --title "keymap: foundation — Keymap registry, dispatcher, Esc stack" --body "$(cat <<'EOF'
## Summary
PR 1 of 4 in the keymap-design rollout. Pure plumbing — no new user-visible features.

- New `vireo/static/keymap.js`: registry + central dispatcher + Esc stack
- Migrated navbar navigation shortcuts onto the registry
- Migrated `_navbar.html` Esc handlers (lightbox, pipeline overlay, similar, inat, help, report, shortcuts cheat sheet, context menu) to use the Esc stack

Per-page handlers in browse.html / review.html / etc. are NOT touched in this PR. They migrate in PR 4.

Design: `docs/plans/2026-04-30-keymap-design.md`
Plan: `docs/plans/2026-04-30-keymap-pr1-foundation.md`

## Test plan
- [x] `pytest tests/e2e/test_keymap.py -v`
- [x] `pytest tests/e2e/test_page_loads.py -v`
- [x] CLAUDE.md curated test command passes
- [x] Manual checklist (see plan section "Final verification")

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

---

## Out of scope for PR 1 (in subsequent plans)

- **PR 2 — Discoverability:** new `?` modal that reads from registry, inline shortcut badges (`[data-shortcut]`), `\` toggle, migrate Browse/Cull/Review/Lightbox shortcuts into registry.
- **PR 3 — Link hints:** hint engine (`f` / `F`), `data-hint` / `data-hint-grid` attributes.
- **PR 4 — Coverage:** migrate remaining user pages (Pipeline, Map, Compare, Variants, Duplicates, Misses, Keywords) onto registry; extend `J`/`K` to all grid/list pages.
