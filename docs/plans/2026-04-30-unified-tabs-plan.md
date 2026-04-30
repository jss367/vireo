# Unified Navbar Tabs — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace Vireo's dual linger-pages / openable-tabs model with a single user-curated tab list, plus a `cmd+K` command palette. Eliminate the navbar-overflow + close-button-induced bouncing bug.

**Source design:** `docs/plans/2026-04-30-unified-tabs-design.md`

**Architecture:**
- **Single ordered list of tab nav-ids per workspace** (DB column `tabs` on `workspaces`), replacing `open_tabs` and the `config_overrides.nav_order` JSON key.
- **All 20 pages are equal** — `ALL_NAV_IDS` constant replaces `OPENABLE_NAV_IDS`. Default tabs for new workspaces: `["browse","pipeline","pipeline_review","review","cull","jobs","highlights","misses","settings"]`.
- **Ephemeral tab slot is JS-only state** (visiting an unpinned page shows it as italic temporary tab, replaced by next unpinned visit).
- **Tab strip overflow → `…` dropdown** on the right; no horizontal scroll. Active and ephemeral tabs always visible.
- **Close button positioned absolutely** so showing/hiding doesn't reflow tab width — structural fix for the "bouncing" bug.
- **Command palette** = modal with Fuse.js fuzzy search over all 20 page labels. Triggered by `cmd/ctrl+K`.
- **One-shot migration:** every existing workspace's `tabs` is reset to the new default. Old `open_tabs` column dropped.

**Tech stack:** Flask (`vireo/app.py`), SQLite via `vireo/db.py`, Jinja2 + vanilla JS in `vireo/templates/_navbar.html`. Tests via `pytest` (`vireo/tests/`, `tests/`) and Playwright (`tests/e2e/`). Fuse.js already vendored.

**Test command (run from repo root):**
```bash
python -m pytest vireo/tests/test_db.py vireo/tests/test_app.py vireo/tests/test_tabs_api.py tests/test_workspaces.py -v
```
E2E: `python -m pytest tests/e2e/test_navigation.py -v`.

**Branch:** `optional-nav-bounce` (already on it; this is the daegu Conductor worktree).

---

## Phase 1 — Backend constants & schema

### Task 1.1: Add `ALL_NAV_IDS` and `DEFAULT_TABS` constants

**Files:**
- Modify: `vireo/db.py` (around line 46 where `OPENABLE_NAV_IDS` lives, and line 129 where `DEFAULT_OPEN_TABS` lives)
- Test: `vireo/tests/test_db.py`

**Step 1: Write failing test**

Append to `vireo/tests/test_db.py`:

```python
def test_all_nav_ids_covers_every_page():
    from db import ALL_NAV_IDS
    expected = {
        "pipeline", "jobs", "pipeline_review", "review", "cull",
        "misses", "highlights", "browse", "map", "variants",
        "dashboard", "audit", "compare",
        "settings", "workspace", "lightroom", "shortcuts",
        "keywords", "duplicates", "logs",
    }
    assert ALL_NAV_IDS == expected


def test_default_tabs_is_the_curated_nine():
    from db import DEFAULT_TABS
    assert DEFAULT_TABS == [
        "browse", "pipeline", "pipeline_review",
        "review", "cull", "jobs",
        "highlights", "misses", "settings",
    ]
```

**Step 2: Run — expect FAIL**

```bash
python -m pytest vireo/tests/test_db.py::test_all_nav_ids_covers_every_page vireo/tests/test_db.py::test_default_tabs_is_the_curated_nine -v
```

Expected: `ImportError: cannot import name 'ALL_NAV_IDS'`.

**Step 3: Implement**

In `vireo/db.py`, leave `OPENABLE_NAV_IDS` in place for now (later tasks remove it). Add **after** the `OPENABLE_NAV_IDS` definition (around line 49):

```python
ALL_NAV_IDS = frozenset({
    "pipeline", "jobs", "pipeline_review", "review", "cull",
    "misses", "highlights", "browse", "map", "variants",
    "dashboard", "audit", "compare",
    "settings", "workspace", "lightroom", "shortcuts",
    "keywords", "duplicates", "logs",
})

DEFAULT_TABS = [
    "browse", "pipeline", "pipeline_review",
    "review", "cull", "jobs",
    "highlights", "misses", "settings",
]
```

**Step 4: Run — expect PASS**

```bash
python -m pytest vireo/tests/test_db.py::test_all_nav_ids_covers_every_page vireo/tests/test_db.py::test_default_tabs_is_the_curated_nine -v
```

**Step 5: Commit**

```bash
git add vireo/db.py vireo/tests/test_db.py
git commit -m "db: add ALL_NAV_IDS and DEFAULT_TABS constants"
```

---

### Task 1.2: Schema migration — add `tabs` column

**Files:**
- Modify: `vireo/db.py` (CREATE TABLE at line 308, migration block at line 603)
- Test: `tests/test_workspaces.py`

**Step 1: Write failing tests**

Add to `tests/test_workspaces.py`:

```python
def test_workspaces_has_tabs_column(db):
    cols = [r["name"] for r in db.conn.execute("PRAGMA table_info(workspaces)")]
    assert "tabs" in cols


def test_legacy_workspaces_get_default_tabs_on_migration(tmp_path):
    """A pre-existing workspaces table without `tabs` should be backfilled with DEFAULT_TABS."""
    import json as _json
    import sqlite3
    db_path = str(tmp_path / "legacy.db")
    conn = sqlite3.connect(db_path)
    conn.execute(
        """CREATE TABLE workspaces (
              id INTEGER PRIMARY KEY,
              name TEXT NOT NULL UNIQUE,
              config_overrides TEXT,
              ui_state TEXT,
              open_tabs TEXT,
              created_at TEXT DEFAULT (datetime('now')),
              last_opened_at TEXT)"""
    )
    conn.execute(
        "INSERT INTO workspaces (name, open_tabs) VALUES (?, ?)",
        ("Legacy", _json.dumps(["settings", "workspace"])),
    )
    conn.commit()
    conn.close()

    from db import Database, DEFAULT_TABS
    db = Database(db_path)
    cols = [r["name"] for r in db.conn.execute("PRAGMA table_info(workspaces)")]
    assert "tabs" in cols
    row = db.conn.execute("SELECT tabs FROM workspaces WHERE name = 'Legacy'").fetchone()
    assert _json.loads(row["tabs"]) == DEFAULT_TABS


def test_new_workspace_gets_default_tabs(db):
    import json as _json
    from db import DEFAULT_TABS
    ws_id = db.create_workspace("Fresh")
    row = db.conn.execute("SELECT tabs FROM workspaces WHERE id = ?", (ws_id,)).fetchone()
    assert row["tabs"] is not None
    assert _json.loads(row["tabs"]) == DEFAULT_TABS
```

**Step 2: Run — expect FAIL**

```bash
python -m pytest tests/test_workspaces.py::test_workspaces_has_tabs_column tests/test_workspaces.py::test_legacy_workspaces_get_default_tabs_on_migration tests/test_workspaces.py::test_new_workspace_gets_default_tabs -v
```

Expected: AssertionError ("tabs" not in cols) / IntegrityError.

**Step 3: Implement**

(a) `vireo/db.py:308` — extend the `CREATE TABLE workspaces` block. Replace:

```sql
CREATE TABLE IF NOT EXISTS workspaces (
    id              INTEGER PRIMARY KEY,
    name            TEXT NOT NULL UNIQUE,
    config_overrides TEXT,
    ui_state        TEXT,
    open_tabs       TEXT,
    created_at      TEXT DEFAULT (datetime('now')),
    last_opened_at  TEXT
);
```

with:

```sql
CREATE TABLE IF NOT EXISTS workspaces (
    id              INTEGER PRIMARY KEY,
    name            TEXT NOT NULL UNIQUE,
    config_overrides TEXT,
    ui_state        TEXT,
    open_tabs       TEXT,
    tabs            TEXT,
    created_at      TEXT DEFAULT (datetime('now')),
    last_opened_at  TEXT
);
```

(`open_tabs` stays for now — it's removed in Task 6.x.)

(b) `vireo/db.py` migration block (around line 603, immediately after the existing `open_tabs` migration) — add:

```python
# Migration: add `tabs` column. Per the unified-tabs design (2026-04-30),
# we reset every workspace's tabs to DEFAULT_TABS — solo-user app, no
# preservation of prior nav_order / open_tabs customizations.
try:
    self.conn.execute("SELECT tabs FROM workspaces LIMIT 0")
except sqlite3.OperationalError:
    self.conn.execute("ALTER TABLE workspaces ADD COLUMN tabs TEXT")
    self.conn.execute(
        "UPDATE workspaces SET tabs = ? WHERE tabs IS NULL",
        (json.dumps(DEFAULT_TABS),),
    )
```

(c) `vireo/db.py:702-712` — update `create_workspace` to also populate `tabs`. Replace:

```python
def create_workspace(self, name, config_overrides=None, ui_state=None):
    """Create a new workspace. Returns the workspace id."""
    cur = self.conn.execute(
        """INSERT INTO workspaces (name, config_overrides, ui_state, open_tabs)
           VALUES (?, ?, ?, ?)""",
        (name,
         json.dumps(config_overrides) if config_overrides else None,
         json.dumps(ui_state) if ui_state else None,
         json.dumps(self.DEFAULT_OPEN_TABS)),
    )
```

with:

```python
def create_workspace(self, name, config_overrides=None, ui_state=None):
    """Create a new workspace. Returns the workspace id."""
    cur = self.conn.execute(
        """INSERT INTO workspaces (name, config_overrides, ui_state, open_tabs, tabs)
           VALUES (?, ?, ?, ?, ?)""",
        (name,
         json.dumps(config_overrides) if config_overrides else None,
         json.dumps(ui_state) if ui_state else None,
         json.dumps(self.DEFAULT_OPEN_TABS),  # legacy column, still populated until task 6.x
         json.dumps(DEFAULT_TABS)),
    )
```

**Step 4: Run — expect PASS**

```bash
python -m pytest tests/test_workspaces.py::test_workspaces_has_tabs_column tests/test_workspaces.py::test_legacy_workspaces_get_default_tabs_on_migration tests/test_workspaces.py::test_new_workspace_gets_default_tabs -v
```

**Step 5: Commit**

```bash
git add vireo/db.py tests/test_workspaces.py
git commit -m "db: add workspaces.tabs column with default backfill"
```

---

### Task 1.3: New DB methods — `get_tabs`, `set_tabs`, `pin_tab`, `unpin_tab`

**Files:**
- Modify: `vireo/db.py` (add methods near existing `open_tab`/`close_tab` at line 876)
- Test: `tests/test_workspaces.py`

**Step 1: Write failing tests**

Append to `tests/test_workspaces.py`:

```python
def test_get_tabs_returns_default_for_new_workspace(db):
    from db import DEFAULT_TABS
    ws_id = db.create_workspace("Fresh")
    db.set_active_workspace(ws_id)
    assert db.get_tabs() == DEFAULT_TABS


def test_pin_tab_appends(db):
    from db import DEFAULT_TABS
    ws_id = db.create_workspace("Fresh")
    db.set_active_workspace(ws_id)
    result = db.pin_tab("logs")
    assert result == DEFAULT_TABS + ["logs"]
    assert db.get_tabs() == DEFAULT_TABS + ["logs"]


def test_pin_tab_idempotent(db):
    ws_id = db.create_workspace("Fresh")
    db.set_active_workspace(ws_id)
    db.pin_tab("logs")
    db.pin_tab("logs")
    assert db.get_tabs().count("logs") == 1


def test_pin_tab_rejects_unknown_id(db):
    import pytest
    ws_id = db.create_workspace("Fresh")
    db.set_active_workspace(ws_id)
    with pytest.raises(ValueError):
        db.pin_tab("not_a_real_page")


def test_unpin_tab_removes(db):
    ws_id = db.create_workspace("Fresh")
    db.set_active_workspace(ws_id)
    db.unpin_tab("settings")
    assert "settings" not in db.get_tabs()


def test_unpin_tab_idempotent_when_not_pinned(db):
    ws_id = db.create_workspace("Fresh")
    db.set_active_workspace(ws_id)
    db.unpin_tab("logs")  # not in defaults
    db.unpin_tab("logs")  # again
    assert "logs" not in db.get_tabs()


def test_set_tabs_replaces_full_list(db):
    ws_id = db.create_workspace("Fresh")
    db.set_active_workspace(ws_id)
    new_order = ["cull", "review", "browse"]
    result = db.set_tabs(new_order)
    assert result == new_order
    assert db.get_tabs() == new_order


def test_set_tabs_rejects_unknown_id(db):
    import pytest
    ws_id = db.create_workspace("Fresh")
    db.set_active_workspace(ws_id)
    with pytest.raises(ValueError):
        db.set_tabs(["browse", "not_a_real_page"])


def test_set_tabs_rejects_duplicates(db):
    import pytest
    ws_id = db.create_workspace("Fresh")
    db.set_active_workspace(ws_id)
    with pytest.raises(ValueError):
        db.set_tabs(["browse", "browse", "review"])


def test_tabs_are_per_workspace(db):
    from db import DEFAULT_TABS
    ws1 = db.create_workspace("WS1")
    ws2 = db.create_workspace("WS2")
    db.set_active_workspace(ws1)
    db.pin_tab("logs")
    assert "logs" in db.get_tabs()
    db.set_active_workspace(ws2)
    assert db.get_tabs() == DEFAULT_TABS
    assert "logs" not in db.get_tabs()
```

**Step 2: Run — expect FAIL**

```bash
python -m pytest tests/test_workspaces.py -k "tabs" -v
```

Expected: `AttributeError: 'Database' object has no attribute 'get_tabs'`.

**Step 3: Implement**

In `vireo/db.py` add these methods after `close_tab` (around line 910):

```python
def get_tabs(self):
    """Return the active workspace's ordered list of pinned tab nav-ids."""
    ws = self.get_workspace(self._ws_id())
    if not ws or not ws["tabs"]:
        return list(DEFAULT_TABS)
    try:
        value = json.loads(ws["tabs"]) if isinstance(ws["tabs"], str) else ws["tabs"]
        return value if isinstance(value, list) else list(DEFAULT_TABS)
    except (json.JSONDecodeError, TypeError):
        return list(DEFAULT_TABS)


def set_tabs(self, tabs):
    """Replace the active workspace's tabs with the given ordered list.

    Validates every entry against ALL_NAV_IDS. Rejects duplicates so the
    UI invariant "each pinned page appears exactly once" is enforced at
    the storage layer.
    Returns the new list.
    """
    if not isinstance(tabs, list):
        raise ValueError("tabs must be a list")
    seen = set()
    for nav_id in tabs:
        if nav_id not in ALL_NAV_IDS:
            raise ValueError(f"{nav_id!r} is not a known nav id")
        if nav_id in seen:
            raise ValueError(f"{nav_id!r} appears more than once")
        seen.add(nav_id)
    self.conn.execute(
        "UPDATE workspaces SET tabs = ? WHERE id = ?",
        (json.dumps(tabs), self._ws_id()),
    )
    self.conn.commit()
    return list(tabs)


def pin_tab(self, nav_id):
    """Append nav_id to the active workspace's tabs if not present.

    Raises ValueError if nav_id is not in ALL_NAV_IDS.
    Returns the new list.
    """
    if nav_id not in ALL_NAV_IDS:
        raise ValueError(f"{nav_id!r} is not a known nav id")
    tabs = self.get_tabs()
    if nav_id not in tabs:
        tabs.append(nav_id)
        self.conn.execute(
            "UPDATE workspaces SET tabs = ? WHERE id = ?",
            (json.dumps(tabs), self._ws_id()),
        )
        self.conn.commit()
    return tabs


def unpin_tab(self, nav_id):
    """Remove nav_id from the active workspace's tabs if present.

    Raises ValueError if nav_id is not in ALL_NAV_IDS.
    Returns the new list.
    """
    if nav_id not in ALL_NAV_IDS:
        raise ValueError(f"{nav_id!r} is not a known nav id")
    tabs = self.get_tabs()
    if nav_id in tabs:
        tabs = [t for t in tabs if t != nav_id]
        self.conn.execute(
            "UPDATE workspaces SET tabs = ? WHERE id = ?",
            (json.dumps(tabs), self._ws_id()),
        )
        self.conn.commit()
    return tabs
```

**Step 4: Run — expect PASS**

```bash
python -m pytest tests/test_workspaces.py -k "tabs" -v
```

**Step 5: Commit**

```bash
git add vireo/db.py tests/test_workspaces.py
git commit -m "db: add get_tabs, set_tabs, pin_tab, unpin_tab methods"
```

---

## Phase 2 — Backend API endpoints

### Task 2.1: `POST /api/workspace/tabs/pin`

**Files:**
- Modify: `vireo/app.py` (around line 3172 next to existing `/tabs/open`)
- Test: `vireo/tests/test_tabs_api.py` (new tests; existing tests stay until Task 6.x)

**Step 1: Write failing test**

Add to `vireo/tests/test_tabs_api.py`:

```python
def test_pin_tab_endpoint_appends(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    r = client.post("/api/workspace/tabs/pin", json={"nav_id": "logs"})
    assert r.status_code == 200
    body = r.get_json()
    assert "logs" in body["tabs"]


def test_pin_tab_endpoint_rejects_unknown_navid(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    r = client.post("/api/workspace/tabs/pin", json={"nav_id": "not_a_real_page"})
    assert r.status_code == 400


def test_pin_tab_endpoint_idempotent(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    client.post("/api/workspace/tabs/pin", json={"nav_id": "logs"})
    r = client.post("/api/workspace/tabs/pin", json={"nav_id": "logs"})
    assert r.status_code == 200
    assert r.get_json()["tabs"].count("logs") == 1
```

**Step 2: Run — expect FAIL**

```bash
python -m pytest vireo/tests/test_tabs_api.py::test_pin_tab_endpoint_appends -v
```

Expected: 404 or 405 (route doesn't exist).

**Step 3: Implement**

Add to `vireo/app.py` (immediately after the existing `api_open_tab` at ~line 3181):

```python
@app.route("/api/workspace/tabs/pin", methods=["POST"])
def api_pin_tab():
    from db import ALL_NAV_IDS
    db = _get_db()
    body = request.get_json(silent=True) or {}
    nav_id = body.get("nav_id")
    if nav_id not in ALL_NAV_IDS:
        return json_error("nav_id is not a known page", 400)
    tabs = db.pin_tab(nav_id)
    return jsonify({"ok": True, "tabs": tabs})
```

**Step 4: Run — expect PASS**

```bash
python -m pytest vireo/tests/test_tabs_api.py -k "pin_tab_endpoint" -v
```

**Step 5: Commit**

```bash
git add vireo/app.py vireo/tests/test_tabs_api.py
git commit -m "api: add POST /api/workspace/tabs/pin"
```

---

### Task 2.2: `POST /api/workspace/tabs/unpin`

**Files:**
- Modify: `vireo/app.py`
- Test: `vireo/tests/test_tabs_api.py`

**Step 1: Write failing test**

Add to `vireo/tests/test_tabs_api.py`:

```python
def test_unpin_tab_endpoint_removes(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    r = client.post("/api/workspace/tabs/unpin", json={"nav_id": "settings"})
    assert r.status_code == 200
    assert "settings" not in r.get_json()["tabs"]


def test_unpin_tab_endpoint_idempotent_when_not_pinned(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    client.post("/api/workspace/tabs/unpin", json={"nav_id": "settings"})
    r = client.post("/api/workspace/tabs/unpin", json={"nav_id": "settings"})
    assert r.status_code == 200


def test_unpin_tab_endpoint_rejects_unknown_navid(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    r = client.post("/api/workspace/tabs/unpin", json={"nav_id": "not_a_real_page"})
    assert r.status_code == 400
```

**Step 2: Run — expect FAIL.**

**Step 3: Implement** (in `vireo/app.py`, after `api_pin_tab`):

```python
@app.route("/api/workspace/tabs/unpin", methods=["POST"])
def api_unpin_tab():
    from db import ALL_NAV_IDS
    db = _get_db()
    body = request.get_json(silent=True) or {}
    nav_id = body.get("nav_id")
    if nav_id not in ALL_NAV_IDS:
        return json_error("nav_id is not a known page", 400)
    tabs = db.unpin_tab(nav_id)
    return jsonify({"ok": True, "tabs": tabs})
```

**Step 4: Run — expect PASS.**

**Step 5: Commit**

```bash
git add vireo/app.py vireo/tests/test_tabs_api.py
git commit -m "api: add POST /api/workspace/tabs/unpin"
```

---

### Task 2.3: `POST /api/workspace/tabs/reorder`

**Files:**
- Modify: `vireo/app.py`
- Test: `vireo/tests/test_tabs_api.py`

**Step 1: Write failing test**

```python
def test_reorder_tabs_endpoint_replaces_order(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    new_order = ["cull", "review", "browse"]
    r = client.post("/api/workspace/tabs/reorder", json={"tabs": new_order})
    assert r.status_code == 200
    assert r.get_json()["tabs"] == new_order


def test_reorder_tabs_rejects_unknown_id(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    r = client.post("/api/workspace/tabs/reorder",
                    json={"tabs": ["browse", "not_a_page"]})
    assert r.status_code == 400


def test_reorder_tabs_rejects_duplicates(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    r = client.post("/api/workspace/tabs/reorder",
                    json={"tabs": ["browse", "browse"]})
    assert r.status_code == 400


def test_reorder_tabs_rejects_non_list(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    r = client.post("/api/workspace/tabs/reorder", json={"tabs": "not-a-list"})
    assert r.status_code == 400
```

**Step 2: Run — expect FAIL.**

**Step 3: Implement** (in `vireo/app.py`, after `api_unpin_tab`):

```python
@app.route("/api/workspace/tabs/reorder", methods=["POST"])
def api_reorder_tabs():
    db = _get_db()
    body = request.get_json(silent=True) or {}
    tabs = body.get("tabs")
    if not isinstance(tabs, list):
        return json_error("tabs must be a list", 400)
    try:
        result = db.set_tabs(tabs)
    except ValueError as e:
        return json_error(str(e), 400)
    return jsonify({"ok": True, "tabs": result})
```

**Step 4: Run — expect PASS.**

**Step 5: Commit**

```bash
git add vireo/app.py vireo/tests/test_tabs_api.py
git commit -m "api: add POST /api/workspace/tabs/reorder"
```

---

### Task 2.4: Update `GET /api/workspace/tabs` response shape

The current shape returns `{open_tabs, openable_pages}`. New shape: `{tabs, all_pages}`.

**Files:**
- Modify: `vireo/app.py:3194-3212`
- Test: `vireo/tests/test_tabs_api.py`

**Step 1: Write failing test**

```python
def test_get_tabs_endpoint_new_shape(app_and_db):
    from db import DEFAULT_TABS
    app, db = app_and_db
    client = app.test_client()
    r = client.get("/api/workspace/tabs")
    assert r.status_code == 200
    body = r.get_json()
    assert body["tabs"] == DEFAULT_TABS
    assert "all_pages" in body
    # all_pages must include every nav id, in a stable order, with label and href
    ids = [p["id"] for p in body["all_pages"]]
    assert "duplicates" in ids
    assert "browse" in ids
    assert len(ids) == 20
    sample = next(p for p in body["all_pages"] if p["id"] == "duplicates")
    assert sample == {"id": "duplicates", "label": "Duplicates", "href": "/duplicates"}
```

**Step 2: Run — expect FAIL.**

**Step 3: Implement**

Replace `vireo/app.py:3194-3212` (`api_get_tabs`) with:

```python
# Stable ordering and labels for the palette + nav rendering.
# The `id` is the nav-id used in `tabs`; `href` is the canonical
# route. Labels match what the navbar showed before the unification.
ALL_PAGES = [
    {"id": "pipeline",        "label": "Pipeline",        "href": "/pipeline"},
    {"id": "jobs",            "label": "Jobs",            "href": "/jobs"},
    {"id": "pipeline_review", "label": "Pipeline Review", "href": "/pipeline/review"},
    {"id": "review",          "label": "Review",          "href": "/review"},
    {"id": "cull",            "label": "Cull",            "href": "/cull"},
    {"id": "misses",          "label": "Misses",          "href": "/misses"},
    {"id": "highlights",      "label": "Highlights",      "href": "/highlights"},
    {"id": "browse",          "label": "Browse",          "href": "/browse"},
    {"id": "map",             "label": "Map",             "href": "/map"},
    {"id": "variants",        "label": "Variants",        "href": "/variants"},
    {"id": "dashboard",       "label": "Dashboard",       "href": "/dashboard"},
    {"id": "audit",           "label": "Audit",           "href": "/audit"},
    {"id": "compare",         "label": "Compare",         "href": "/compare"},
    {"id": "settings",        "label": "Settings",        "href": "/settings"},
    {"id": "workspace",       "label": "Workspace",       "href": "/workspace"},
    {"id": "lightroom",       "label": "Lightroom",       "href": "/lightroom"},
    {"id": "shortcuts",       "label": "Shortcuts",       "href": "/shortcuts"},
    {"id": "keywords",        "label": "Keywords",        "href": "/keywords"},
    {"id": "duplicates",      "label": "Duplicates",      "href": "/duplicates"},
    {"id": "logs",            "label": "Logs",            "href": "/logs"},
]


@app.route("/api/workspace/tabs", methods=["GET"])
def api_get_tabs():
    db = _get_db()
    try:
        tabs = db.get_tabs()
    except Exception:
        from db import DEFAULT_TABS
        tabs = list(DEFAULT_TABS)
    return jsonify({"tabs": tabs, "all_pages": ALL_PAGES})
```

Define `ALL_PAGES` at module-top of `vireo/app.py` (or just above `api_get_tabs`). This list is the single source of truth for label + href.

**Step 4: Run — expect PASS.**

```bash
python -m pytest vireo/tests/test_tabs_api.py::test_get_tabs_endpoint_new_shape -v
```

**Step 5: Commit**

```bash
git add vireo/app.py vireo/tests/test_tabs_api.py
git commit -m "api: GET /api/workspace/tabs returns {tabs, all_pages}"
```

---

## Phase 3 — Frontend tab strip

### Task 3.1: CSS — absolute-positioned close button (kills the bouncing bug)

**Files:**
- Modify: `vireo/templates/_navbar.html` (CSS block around line 1162)

This task is the **structural fix for "bouncing"** — even before the bigger restructure lands, this single change stops the layout reflow on hover.

**Step 1: Write failing e2e test**

Add to `tests/e2e/test_navigation.py`:

```python
def test_tab_close_button_does_not_change_tab_width_on_hover(live_server, page):
    """Hovering a tab in the navbar must not change the tab's bounding box.

    The bounce bug was: hover → close button shows via display change → tab
    grows wider → flex re-layout. With absolute positioning the tab width
    is fixed regardless of hover.
    """
    url = live_server["url"]
    page.set_viewport_size({"width": 1366, "height": 800})
    page.goto(f"{url}/browse")
    # Pin a known tab so it's in the strip
    page.evaluate("""async () => {
        await fetch('/api/workspace/tabs/pin',
                    {method:'POST', headers:{'Content-Type':'application/json'},
                     body: JSON.stringify({nav_id:'logs'})});
    }""")
    page.reload()
    page.wait_for_selector(".nav-tab[data-nav-id='logs']")
    tab = page.query_selector(".nav-tab[data-nav-id='logs']")
    box_before = tab.bounding_box()
    # Hover the tab
    page.mouse.move(box_before["x"] + box_before["width"] / 2,
                    box_before["y"] + box_before["height"] / 2)
    page.wait_for_timeout(150)
    box_after = tab.bounding_box()
    assert abs(box_before["width"] - box_after["width"]) < 1.0, \
        f"Tab width changed on hover ({box_before['width']} → {box_after['width']})"
    assert abs(box_before["height"] - box_after["height"]) < 1.0, \
        f"Tab height changed on hover ({box_before['height']} → {box_after['height']})"
```

**Step 2: Run — expect FAIL** (today's CSS does change width on hover).

**Step 3: Implement**

In `vireo/templates/_navbar.html`, replace the close-button CSS block (around line 1162-1176) with:

```css
/* ---------- Openable tabs ---------- */
.navbar .nav-tab {
  position: relative;
  /* Reserve space at the right for the absolutely-positioned close button
     so it never overlaps the tab label. Width matches close-button visual
     footprint (≈18px including its 4px right offset). */
  padding-right: 22px;
}
.navbar .nav-tab-close {
  display: none;
  position: absolute;
  right: 4px;
  top: 50%;
  transform: translateY(-50%);
  padding: 0 4px;
  font-size: 14px;
  line-height: 1;
  color: var(--text-muted, #999);
  border-radius: 3px;
  cursor: pointer;
}
.navbar .nav-tab:hover .nav-tab-close,
.navbar .nav-tab.active .nav-tab-close { display: inline-block; }
.navbar .nav-tab-close:hover {
  background: var(--bg-tertiary, rgba(255,255,255,0.1));
  color: var(--text-primary);
}
```

**Step 4: Run — expect PASS.**

```bash
python -m pytest tests/e2e/test_navigation.py::test_tab_close_button_does_not_change_tab_width_on_hover -v
```

**Step 5: Commit**

```bash
git add vireo/templates/_navbar.html tests/e2e/test_navigation.py
git commit -m "navbar: absolute-position close button so hover doesn't reflow"
```

---

### Task 3.2: HTML — replace static linger pages + Tools button with single dynamic strip

**Files:**
- Modify: `vireo/templates/_navbar.html` (~line 1225-1269 — the `<nav class="navbar">` block)

**Step 1: Read the existing structure**

The current structure is:

```html
<nav class="navbar">
  <a class="brand" ...>Vireo</a>
  <div class="ws-dropdown" id="wsDropdown"> ... </div>
  <a href="/pipeline" data-nav-id="pipeline" data-testid="nav-pipeline">Pipeline</a>
  <a href="/jobs" data-nav-id="jobs" data-testid="nav-jobs">Jobs<span class="nav-job-badge" id="navJobBadge"></span></a>
  ... (11 more linger anchors) ...
  <span class="nav-tab-divider"></span>
  <span id="navOpenTabs"></span>
  <button class="nav-tools-btn" ...>+ Tools ▾</button>
  <div class="nav-tools-menu" id="navToolsMenu" hidden></div>
  <span class="nav-spacer"></span>
  <span class="nav-icon" onclick="openReportModal()">⚠</span>
  <span class="nav-icon" onclick="openHelpModal()">?</span>
  <span class="nav-icon" onclick="toggleDevMode()">⚙</span>
  <span class="nav-icon" onclick="toggleTheme()">✶</span>
  <span class="nav-icon" onclick="toggleBottomPanel()">▦ ...</span>
  <a class="nav-icon" href="/logs">☰</a>
</nav>
```

**Step 2: Write failing assertion (DOM smoke test)**

Add to `tests/e2e/test_navigation.py`:

```python
def test_navbar_renders_default_tabs_dynamically(live_server, page):
    """The 9 default tabs render as <a class='nav-tab'> dynamically — no
    static linger-page anchors, no '+ Tools' button."""
    url = live_server["url"]
    page.goto(f"{url}/browse")
    page.wait_for_selector(".nav-tab[data-nav-id='browse']")
    nav_ids = page.eval_on_selector_all(
        ".navbar .nav-tab",
        "els => els.map(e => e.dataset.navId)"
    )
    assert "browse" in nav_ids
    assert "pipeline" in nav_ids
    assert "review" in nav_ids
    # No tools button
    assert page.query_selector(".nav-tools-btn") is None
    # No standalone Logs icon (it's now a tab if pinned)
    logs_icons = page.query_selector_all(".nav-icon[href='/logs']")
    assert len(logs_icons) == 0
```

**Step 3: Run — expect FAIL.**

**Step 4: Implement**

Replace `vireo/templates/_navbar.html:1225-1269` (the `<nav class="navbar">` block) with:

```html
<nav class="navbar">
  <a class="brand" href="/browse"><img src="/static/favicon.png" alt="" style="height:22px;width:22px;vertical-align:middle;margin-right:6px;border-radius:4px;">Vireo</a>
  <div class="ws-dropdown" id="wsDropdown">
    <button class="ws-current" onclick="toggleWsDropdown()" id="wsCurrentBtn" data-testid="workspace-dropdown">
      <span id="wsCurrentName">Default</span>
      <span class="ws-arrow">&#9662;</span>
    </button>
    <div class="ws-menu" id="wsMenu">
      <div class="ws-menu-header">Workspaces</div>
      <div id="wsMenuList"></div>
      <div class="ws-menu-divider"></div>
      <button class="ws-menu-action" onclick="showCreateWorkspaceModal()">+ New Workspace</button>
    </div>
  </div>
  <span class="nav-cmd-hint" title="Open command palette (⌘K)" onclick="openCommandPalette()">⌘K</span>
  <span id="navTabStrip" class="nav-tab-strip"></span>
  <button type="button" class="nav-overflow-btn" id="navOverflowBtn" hidden onclick="toggleOverflowMenu(event)">&hellip;</button>
  <div class="nav-overflow-menu" id="navOverflowMenu" hidden></div>
  <span class="nav-spacer"></span>
  <span class="nav-icon" onclick="openReportModal()" title="Report Issue" id="reportToggle">&#9888;</span>
  <span class="nav-icon" onclick="openHelpModal()" title="Help (F1)" id="helpToggle">&#63;</span>
  <span class="nav-icon" onclick="toggleDevMode()" title="Toggle developer mode" id="devModeToggle" style="opacity:0.4;">&#9881;</span>
  <span class="nav-icon" onclick="toggleTheme()" title="Toggle light/dark mode" id="themeToggle">&#9788;</span>
  <span class="nav-icon" onclick="toggleBottomPanel()" title="Toggle panel">
    &#9638;
    <span class="activity-dot" id="navActivityDot"></span>
  </span>
</nav>
```

Removed: 13 static linger anchors, `nav-tab-divider`, `+ Tools` button + `navToolsMenu`, `☰` Logs icon link.

Added: `nav-cmd-hint` (palette discoverability), `navTabStrip` (renamed from `navOpenTabs`), `navOverflowBtn` + `navOverflowMenu` (overflow dropdown).

**Step 5: Add CSS for new nav elements**

In the same file, in the CSS block, **add** (near `.nav-tools-btn` rules around line 1185):

```css
.navbar .nav-tab-strip {
  display: inline-flex;
  align-items: stretch;
  flex-shrink: 1;
  min-width: 0;
  overflow: hidden;
  white-space: nowrap;
}
.navbar .nav-cmd-hint {
  display: inline-block;
  padding: 4px 8px;
  margin: 0 8px;
  font-size: 11px;
  font-family: ui-monospace, monospace;
  color: var(--text-muted);
  border: 1px solid var(--border-primary);
  border-radius: 4px;
  cursor: pointer;
  user-select: none;
}
.navbar .nav-cmd-hint:hover { color: var(--text-primary); border-color: var(--text-primary); }
.navbar .nav-overflow-btn {
  background: none;
  border: 1px dashed var(--border-primary, rgba(255,255,255,0.2));
  color: var(--text-muted, #999);
  padding: 2px 8px;
  border-radius: 4px;
  cursor: pointer;
  font: inherit;
  margin-left: 4px;
}
.navbar .nav-overflow-btn:hover { color: var(--text-primary); border-color: var(--text-primary); }
.navbar .nav-overflow-btn[hidden] { display: none; }
.nav-overflow-menu {
  position: absolute;
  top: 44px;
  background: var(--bg-secondary, #2a2a2a);
  border: 1px solid var(--border-primary, rgba(255,255,255,0.15));
  border-radius: 6px;
  padding: 4px;
  z-index: 1000;
  min-width: 180px;
  box-shadow: 0 4px 12px rgba(0,0,0,0.25);
}
.nav-overflow-menu[hidden] { display: none; }
.nav-overflow-item {
  display: flex; align-items: center; width: 100%;
  background: none; border: 0; color: var(--text-primary);
  text-align: left; padding: 6px 8px; font: inherit;
  cursor: pointer; border-radius: 4px;
}
.nav-overflow-item:hover { background: var(--bg-tertiary, rgba(255,255,255,0.08)); }
/* Ephemeral tab styling — italic + no "active" border */
.navbar .nav-tab.is-ephemeral {
  font-style: italic;
}
```

You may also remove the now-unused `.nav-tools-btn`, `.nav-tools-menu`, `.nav-tools-item`, `.nav-tools-check`, `.nav-tab-divider` rules (Task 6.x cleanup).

**Step 6: Run — expect PASS** (after Task 3.3 plugs in the JS rendering).

**Step 7: Commit**

```bash
git add vireo/templates/_navbar.html tests/e2e/test_navigation.py
git commit -m "navbar: replace static linger pages + tools button with dynamic strip"
```

---

### Task 3.3: JS — render unified tab strip

**Files:**
- Modify: `vireo/templates/_navbar.html` (script block ~1590-1773)

This task replaces the old `fetchAndRender` / `renderTabs` logic that built two parallel lists (linger anchors + open tabs span) with a single rendering pass over `tabs + ephemeral`.

**Step 1: Write failing e2e test**

Add to `tests/e2e/test_navigation.py`:

```python
def test_pinning_a_tab_via_api_makes_it_appear_in_strip(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/browse")
    # Initially no `logs` tab
    assert page.query_selector(".nav-tab[data-nav-id='logs']") is None
    page.evaluate("""async () => {
        await fetch('/api/workspace/tabs/pin',
                    {method:'POST', headers:{'Content-Type':'application/json'},
                     body: JSON.stringify({nav_id:'logs'})});
    }""")
    page.reload()
    page.wait_for_selector(".nav-tab[data-nav-id='logs']", timeout=3000)


def test_unpinning_active_tab_navigates_to_adjacent(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/cull")  # active tab is 'cull'
    page.wait_for_selector(".nav-tab[data-nav-id='cull'].active")
    page.click(".nav-tab[data-nav-id='cull'] .nav-tab-close")
    page.wait_for_load_state("networkidle")
    # Navigated to a sibling — anything that isn't /cull
    assert "/cull" not in page.url
```

**Step 2: Run — expect FAIL.**

**Step 3: Implement**

Replace the IIFE at `vireo/templates/_navbar.html:1590-1772` (the `(function() { ... })();` that does `postJSON`, `currentNavId`, `renderTabs`, `fetchAndRender`, `closeTab`, `openTab`, `toggleTabFromMenu`, `toggleToolsMenu`, etc.) with:

```html
<script>
/* ---------- Unified tab strip + ephemeral slot + overflow ---------- */
(function() {
  const STRIP   = () => document.getElementById('navTabStrip');
  const OVERBTN = () => document.getElementById('navOverflowBtn');
  const OVERMENU = () => document.getElementById('navOverflowMenu');

  let TABS = [];          // ordered list of pinned nav-ids
  let ALL_PAGES = [];     // [{id,label,href}, ...]
  let pageById = {};      // map id → {id,label,href}
  let ephemeralId = null; // nav-id of the ephemeral tab, if any

  function postJSON(url, body) {
    return fetch(url, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(body || {}),
    }).then(r => r.json());
  }

  function currentNavId() {
    const p = window.location.pathname;
    if (p.startsWith('/pipeline/review')) return 'pipeline_review';
    if (p === '/' || p.startsWith('/browse')) return 'browse';
    const seg = (p.split('/')[1] || '').replace(/-/g, '_');
    return seg;
  }

  function buildTabAnchor(page, opts) {
    const a = document.createElement('a');
    a.href = page.href;
    a.className = 'nav-tab';
    if (opts && opts.ephemeral) a.classList.add('is-ephemeral');
    a.dataset.navId = page.id;
    a.dataset.testid = 'nav-' + page.id;
    a.appendChild(document.createTextNode(page.label));
    if (page.id === 'jobs') {
      const badge = document.createElement('span');
      badge.className = 'nav-job-badge';
      badge.id = 'navJobBadge';
      a.appendChild(badge);
    }
    const close = document.createElement('span');
    close.className = 'nav-tab-close';
    close.title = 'Close tab';
    close.innerHTML = '&times;';
    close.addEventListener('click', (ev) => {
      ev.preventDefault();
      ev.stopPropagation();
      if (opts && opts.ephemeral) clearEphemeral(); else unpinTab(page.id);
    });
    a.appendChild(close);
    return a;
  }

  function applyActiveClass() {
    const cur = currentNavId();
    STRIP().querySelectorAll('.nav-tab').forEach(a => {
      if (a.dataset.navId === cur) a.classList.add('active');
      else a.classList.remove('active');
    });
  }

  function renderStrip() {
    const strip = STRIP();
    if (!strip) return;
    strip.textContent = '';
    TABS.forEach(id => {
      const page = pageById[id];
      if (page) strip.appendChild(buildTabAnchor(page, {ephemeral: false}));
    });
    // Ephemeral tab: only if currently on an unpinned page
    const cur = currentNavId();
    if (cur && !TABS.includes(cur) && pageById[cur]) {
      ephemeralId = cur;
      strip.appendChild(buildTabAnchor(pageById[cur], {ephemeral: true}));
    } else {
      ephemeralId = null;
    }
    applyActiveClass();
    recomputeOverflow();
  }

  function recomputeOverflow() {
    const strip = STRIP();
    const overBtn = OVERBTN();
    if (!strip || !overBtn) return;
    // Reset: show all tabs
    const tabs = Array.from(strip.querySelectorAll('.nav-tab'));
    tabs.forEach(t => { t.style.display = ''; });
    overBtn.hidden = true;
    // Detect overflow against parent (the navbar). The strip is a flex
    // child with min-width:0 + overflow:hidden — its scrollWidth >
    // clientWidth means tabs are clipped.
    if (strip.scrollWidth <= strip.clientWidth) return;
    // Hide tabs from the right end until the strip fits, but never hide
    // the active tab or the ephemeral tab.
    overBtn.hidden = false;
    const cur = currentNavId();
    const protectedIds = new Set([cur, ephemeralId].filter(Boolean));
    // Walk right-to-left, hiding pinned tabs that aren't protected.
    for (let i = tabs.length - 1; i >= 0; i--) {
      if (strip.scrollWidth <= strip.clientWidth) break;
      const t = tabs[i];
      if (protectedIds.has(t.dataset.navId)) continue;
      t.style.display = 'none';
    }
    rebuildOverflowMenu(tabs);
  }

  function rebuildOverflowMenu(tabs) {
    const menu = OVERMENU();
    if (!menu) return;
    menu.textContent = '';
    tabs.forEach(t => {
      if (t.style.display !== 'none') return;
      const page = pageById[t.dataset.navId];
      if (!page) return;
      const item = document.createElement('button');
      item.type = 'button';
      item.className = 'nav-overflow-item';
      item.dataset.navId = page.id;
      item.appendChild(document.createTextNode(page.label));
      item.addEventListener('click', () => {
        window.location.href = page.href;
      });
      menu.appendChild(item);
    });
  }

  window.toggleOverflowMenu = function(ev) {
    if (ev) ev.stopPropagation();
    const menu = OVERMENU();
    const btn = OVERBTN();
    if (!menu || !btn) return;
    if (menu.hasAttribute('hidden')) {
      menu.removeAttribute('hidden');
      const r = btn.getBoundingClientRect();
      menu.style.left = r.left + 'px';
      setTimeout(() => {
        document.addEventListener('click', closeOverflowMenuOnOutside, {once: true});
      }, 0);
    } else {
      menu.setAttribute('hidden', '');
    }
  };

  function closeOverflowMenuOnOutside(e) {
    const menu = OVERMENU();
    if (!menu) return;
    if (e.target.closest('.nav-overflow-menu, .nav-overflow-btn')) {
      document.addEventListener('click', closeOverflowMenuOnOutside, {once: true});
      return;
    }
    menu.setAttribute('hidden', '');
  }

  function adjacentTabId(navId) {
    const idx = TABS.indexOf(navId);
    if (idx === -1) return null;
    if (idx + 1 < TABS.length) return TABS[idx + 1];
    if (idx - 1 >= 0) return TABS[idx - 1];
    return null;
  }

  function unpinTab(navId) {
    const isOnTab = currentNavId() === navId;
    const nextId = isOnTab ? adjacentTabId(navId) : null;
    postJSON('/api/workspace/tabs/unpin', {nav_id: navId}).then(() => {
      if (isOnTab) {
        window.location.href = nextId ? pageById[nextId].href : '/browse';
      } else {
        return fetchAndRender();
      }
    });
  }

  function clearEphemeral() {
    // Closing ephemeral === navigate away from the unpinned page.
    // Send the user to an adjacent pinned tab, or /browse.
    const next = TABS[0] ? pageById[TABS[0]] : null;
    window.location.href = next ? next.href : '/browse';
  }

  window.pinTab = function(navId) {
    return postJSON('/api/workspace/tabs/pin', {nav_id: navId})
      .then(() => fetchAndRender());
  };

  function fetchAndRender() {
    return fetch('/api/workspace/tabs')
      .then(r => r.json())
      .then(state => {
        TABS = (state && state.tabs) || [];
        ALL_PAGES = (state && state.all_pages) || [];
        pageById = {};
        ALL_PAGES.forEach(p => { pageById[p.id] = p; });
        renderStrip();
        return state;
      })
      .catch(() => {});
  }

  // Re-recompute overflow on viewport resize
  if (window.ResizeObserver) {
    const ro = new ResizeObserver(() => recomputeOverflow());
    if (document.querySelector('.navbar')) ro.observe(document.querySelector('.navbar'));
  } else {
    window.addEventListener('resize', recomputeOverflow);
  }

  // Expose for the palette + drag-reorder code in the next scripts
  window._navTabs = {
    fetchAndRender,
    getTabs: () => TABS.slice(),
    getAllPages: () => ALL_PAGES.slice(),
    setTabs: (newOrder) => {
      return postJSON('/api/workspace/tabs/reorder', {tabs: newOrder})
        .then(() => fetchAndRender());
    },
  };

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', fetchAndRender);
  } else {
    fetchAndRender();
  }
})();
</script>
```

This replaces the entire previous script — old `closeTab`, `openTab`, `toggleTabFromMenu`, `toggleToolsMenu`, `closeMenuOnOutside` are all gone (their roles are subsumed by `unpinTab`, `pinTab`, `toggleOverflowMenu`, `closeOverflowMenuOnOutside`).

**Step 4: Run — expect PASS.**

```bash
python -m pytest tests/e2e/test_navigation.py::test_navbar_renders_default_tabs_dynamically tests/e2e/test_navigation.py::test_pinning_a_tab_via_api_makes_it_appear_in_strip tests/e2e/test_navigation.py::test_unpinning_active_tab_navigates_to_adjacent -v
```

**Step 5: Commit**

```bash
git add vireo/templates/_navbar.html tests/e2e/test_navigation.py
git commit -m "navbar: render unified tab strip with ephemeral slot and overflow"
```

---

### Task 3.4: Drag-reorder generalised to whole strip

**Files:**
- Modify: `vireo/templates/_navbar.html` (the `Nav reorder` IIFE, ~line 1776-1917)

**Step 1: Write failing test**

```python
def test_drag_reorder_persists_via_reorder_endpoint(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/browse")
    page.wait_for_selector(".nav-tab[data-nav-id='browse']")
    # Move 'browse' to after 'review' via the public reorder endpoint shape.
    # (We test the JS hook, not raw mouse drag — drag in headless is flaky.)
    page.evaluate("""async () => {
        const tabs = window._navTabs.getTabs();
        const a = tabs.indexOf('browse');
        const b = tabs.indexOf('review');
        if (a < 0 || b < 0) throw new Error('expected default tabs');
        const next = tabs.slice();
        next.splice(a, 1);
        next.splice(b, 0, 'browse');
        await window._navTabs.setTabs(next);
    }""")
    page.wait_for_function("""() => {
        const t = window._navTabs.getTabs();
        return t.indexOf('browse') > t.indexOf('review');
    }""", timeout=3000)
```

**Step 2: Run — expect FAIL** (initially `_navTabs.setTabs` exists from Task 3.3 — but there's no drag wiring yet; this test calls `setTabs` directly so it should actually pass once 3.3 lands. The drag wiring is verified by manual smoke + later e2e if added).

**Step 3: Implement — replace the existing drag block**

Replace `vireo/templates/_navbar.html:1776-1917` (the `Nav reorder + Active page highlighting` IIFE) with:

```html
<script>
/* ---------- Drag-reorder over the unified strip ---------- */
(function() {
  function strip() { return document.getElementById('navTabStrip'); }
  function tabAnchors() {
    return Array.from(strip().querySelectorAll('.nav-tab:not(.is-ephemeral)'));
  }

  let draggedEl = null;
  let indicator = null;

  function createIndicator() {
    const el = document.createElement('span');
    el.className = 'nav-drop-indicator';
    return el;
  }
  function removeIndicator() {
    if (indicator && indicator.parentNode) indicator.parentNode.removeChild(indicator);
    indicator = null;
  }

  function bindDragHandlers() {
    tabAnchors().forEach(link => {
      // Idempotent: ensure draggable=true (re-render replaces nodes).
      link.draggable = true;
      if (link.dataset.dragBound) return;
      link.dataset.dragBound = '1';
      link.addEventListener('dragstart', function(e) {
        draggedEl = this;
        this.classList.add('dragging');
        e.dataTransfer.effectAllowed = 'move';
        e.dataTransfer.setData('text/plain', this.dataset.navId);
      });
      link.addEventListener('dragend', function() {
        this.classList.remove('dragging');
        removeIndicator();
        draggedEl = null;
      });
      link.addEventListener('dragover', function(e) {
        if (!draggedEl || draggedEl === this) return;
        e.preventDefault();
        e.dataTransfer.dropEffect = 'move';
        removeIndicator();
        const rect = this.getBoundingClientRect();
        const midX = rect.left + rect.width / 2;
        indicator = createIndicator();
        if (e.clientX < midX) strip().insertBefore(indicator, this);
        else strip().insertBefore(indicator, this.nextSibling);
      });
      link.addEventListener('drop', function(e) {
        if (!draggedEl || draggedEl === this) return;
        e.preventDefault();
        const rect = this.getBoundingClientRect();
        const midX = rect.left + rect.width / 2;
        if (e.clientX < midX) strip().insertBefore(draggedEl, this);
        else strip().insertBefore(draggedEl, this.nextSibling);
        removeIndicator();
        const newOrder = tabAnchors().map(a => a.dataset.navId);
        if (window._navTabs) window._navTabs.setTabs(newOrder);
      });
    });
    // Cancel indicator when leaving the strip
    strip().addEventListener('dragleave', function(e) {
      if (!strip().contains(e.relatedTarget)) removeIndicator();
    });
  }

  // Re-bind every time the strip re-renders. We do that by hooking into
  // _navTabs.fetchAndRender via a small wrapper — but simpler: a
  // MutationObserver on the strip.
  function init() {
    const s = strip();
    if (!s) return;
    bindDragHandlers();
    const mo = new MutationObserver(() => bindDragHandlers());
    mo.observe(s, {childList: true});
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
</script>
```

Old behaviour preserved: `nav-drop-indicator` styling stays the same. Old `reorderNavLinks` (which read `nav_order` from `config_overrides`) is **removed** — order is now driven entirely by `tabs`.

**Step 4: Run — expect PASS.**

```bash
python -m pytest tests/e2e/test_navigation.py::test_drag_reorder_persists_via_reorder_endpoint -v
```

**Step 5: Commit**

```bash
git add vireo/templates/_navbar.html tests/e2e/test_navigation.py
git commit -m "navbar: drag-reorder operates on whole unified strip"
```

---

## Phase 4 — Command palette

### Task 4.1: Palette modal HTML + CSS

**Files:**
- Modify: `vireo/templates/_navbar.html`

**Step 1: Write failing test**

```python
def test_cmdk_opens_palette(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/browse")
    # Modal initially hidden
    palette = page.query_selector("#commandPalette")
    assert palette is not None
    assert palette.is_hidden()
    # Cmd+K (mac) or Ctrl+K elsewhere
    page.keyboard.press("Meta+K")
    page.wait_for_selector("#commandPalette:not([hidden])", timeout=2000)
    # Esc closes
    page.keyboard.press("Escape")
    page.wait_for_function(
        "() => document.getElementById('commandPalette').hasAttribute('hidden')",
        timeout=2000,
    )
```

**Step 2: Run — expect FAIL.**

**Step 3: Implement modal HTML**

Add **inside** `_navbar.html`, immediately after the `</nav>` closing tag (~line 1269) and before any other modals:

```html
<!-- Command palette (cmd+K) -->
<div class="cmd-palette-overlay" id="commandPalette" hidden onclick="if(event.target===this)closeCommandPalette()">
  <div class="cmd-palette">
    <input type="text" id="cmdPaletteInput" autocomplete="off" placeholder="Jump to page…" />
    <div id="cmdPaletteResults" class="cmd-palette-results"></div>
  </div>
</div>
```

**Step 4: Implement CSS** — add to the CSS block:

```css
.cmd-palette-overlay {
  position: fixed; inset: 0; z-index: 10001;
  background: rgba(0,0,0,0.45);
  display: flex; justify-content: center; align-items: flex-start;
  padding-top: 12vh;
}
.cmd-palette-overlay[hidden] { display: none; }
.cmd-palette {
  background: var(--bg-secondary);
  border: 1px solid var(--border-primary);
  border-radius: 8px;
  width: 480px; max-width: 90vw;
  max-height: 60vh; display: flex; flex-direction: column;
  box-shadow: 0 8px 32px rgba(0,0,0,0.4);
}
.cmd-palette input {
  border: 0; outline: none; padding: 14px 16px;
  background: transparent; color: var(--text-primary);
  font-size: 15px;
  border-bottom: 1px solid var(--border-primary);
}
.cmd-palette-results {
  overflow-y: auto; padding: 4px 0;
}
.cmd-palette-result {
  display: flex; align-items: center; gap: 8px;
  padding: 8px 16px; cursor: pointer; color: var(--text-primary);
  font-size: 13px;
}
.cmd-palette-result.selected { background: var(--bg-tertiary, rgba(255,255,255,0.08)); }
.cmd-palette-result.is-current { color: var(--accent); }
.cmd-palette-result-pinned {
  color: var(--text-muted); font-size: 11px; margin-left: auto;
}
```

**Step 5: Implement minimal JS shell** (full search wiring lands in 4.2):

Add a new `<script>` at the bottom of `_navbar.html`:

```html
<script>
window.openCommandPalette = function() {
  const overlay = document.getElementById('commandPalette');
  if (!overlay) return;
  overlay.removeAttribute('hidden');
  const input = document.getElementById('cmdPaletteInput');
  if (input) { input.value = ''; input.focus(); }
  if (window._cmdPaletteRender) window._cmdPaletteRender('');
};
window.closeCommandPalette = function() {
  const overlay = document.getElementById('commandPalette');
  if (overlay) overlay.setAttribute('hidden', '');
};
document.addEventListener('keydown', function(e) {
  const isMac = navigator.platform.toUpperCase().indexOf('MAC') >= 0;
  const mod = isMac ? e.metaKey : e.ctrlKey;
  if (mod && e.key.toLowerCase() === 'k') {
    e.preventDefault();
    window.openCommandPalette();
  } else if (e.key === 'Escape') {
    const overlay = document.getElementById('commandPalette');
    if (overlay && !overlay.hasAttribute('hidden')) {
      window.closeCommandPalette();
    }
  }
});
</script>
```

**Step 6: Run — expect PASS.**

```bash
python -m pytest tests/e2e/test_navigation.py::test_cmdk_opens_palette -v
```

**Step 7: Commit**

```bash
git add vireo/templates/_navbar.html tests/e2e/test_navigation.py
git commit -m "navbar: add command palette modal shell with cmd+K trigger"
```

---

### Task 4.2: Palette search via Fuse.js + keyboard nav

**Files:**
- Modify: `vireo/templates/_navbar.html`

**Step 1: Find Fuse.js include path**

Run:
```bash
grep -rn "fuse" vireo/templates/ vireo/static/ 2>/dev/null | head -5
```

(Verify the URL of the vendored Fuse.js — likely `/static/vendor/fuse.js` or similar. The existing help modal uses it.)

**Step 2: Write failing test**

```python
def test_palette_filters_by_query(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/browse")
    page.keyboard.press("Meta+K")
    page.wait_for_selector("#commandPalette:not([hidden])")
    page.fill("#cmdPaletteInput", "dup")
    # Wait for Duplicates row to be the (only/top) result
    page.wait_for_selector(".cmd-palette-result[data-nav-id='duplicates']", timeout=2000)


def test_palette_enter_navigates(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/browse")
    page.keyboard.press("Meta+K")
    page.fill("#cmdPaletteInput", "dup")
    page.wait_for_selector(".cmd-palette-result[data-nav-id='duplicates'].selected")
    page.keyboard.press("Enter")
    page.wait_for_url(f"{url}/duplicates", timeout=3000)


def test_palette_arrow_keys_change_selection(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/browse")
    page.keyboard.press("Meta+K")
    # Empty query → all 20 pages, selected=top
    first = page.eval_on_selector(".cmd-palette-result.selected", "el => el.dataset.navId")
    page.keyboard.press("ArrowDown")
    second = page.eval_on_selector(".cmd-palette-result.selected", "el => el.dataset.navId")
    assert first != second
```

**Step 3: Run — expect FAIL.**

**Step 4: Implement palette logic**

Add (or extend) the script block from Task 4.1. After the `closeCommandPalette` definition, add:

```javascript
(function() {
  let allPages = [];
  let pinnedSet = new Set();
  let fuse = null;
  let results = [];
  let selectedIndex = 0;

  function refreshFromTabsState() {
    return fetch('/api/workspace/tabs').then(r => r.json()).then(state => {
      allPages = state.all_pages || [];
      pinnedSet = new Set(state.tabs || []);
      // Configure Fuse over labels and ids
      if (window.Fuse) {
        fuse = new Fuse(allPages, {
          keys: [
            {name: 'label', weight: 0.7},
            {name: 'id',    weight: 0.3},
          ],
          threshold: 0.4,
          ignoreLocation: true,
        });
      }
    });
  }

  function currentNavId() {
    const p = window.location.pathname;
    if (p.startsWith('/pipeline/review')) return 'pipeline_review';
    if (p === '/' || p.startsWith('/browse')) return 'browse';
    return (p.split('/')[1] || '').replace(/-/g, '_');
  }

  function defaultSorted() {
    // Pinned in pinned-order (use _navTabs if available), then unpinned
    // alphabetically by label.
    const pinnedOrder = (window._navTabs ? window._navTabs.getTabs() : []);
    const pinnedById = new Map(allPages.map(p => [p.id, p]));
    const out = [];
    pinnedOrder.forEach(id => {
      if (pinnedById.has(id)) out.push(pinnedById.get(id));
    });
    const unpinned = allPages
      .filter(p => !pinnedSet.has(p.id))
      .sort((a, b) => a.label.localeCompare(b.label));
    return out.concat(unpinned);
  }

  function render(query) {
    const list = document.getElementById('cmdPaletteResults');
    if (!list) return;
    list.textContent = '';
    if (!query) {
      results = defaultSorted();
    } else if (fuse) {
      results = fuse.search(query).map(r => r.item);
    } else {
      const q = query.toLowerCase();
      results = allPages.filter(p =>
        p.label.toLowerCase().includes(q) || p.id.includes(q)
      );
    }
    if (selectedIndex >= results.length) selectedIndex = 0;
    const cur = currentNavId();
    results.forEach((p, idx) => {
      const row = document.createElement('div');
      row.className = 'cmd-palette-result' + (idx === selectedIndex ? ' selected' : '');
      if (p.id === cur) row.classList.add('is-current');
      row.dataset.navId = p.id;
      row.appendChild(document.createTextNode(p.label));
      if (pinnedSet.has(p.id)) {
        const pin = document.createElement('span');
        pin.className = 'cmd-palette-result-pinned';
        pin.textContent = '📌';
        row.appendChild(pin);
      }
      row.addEventListener('click', () => navigateTo(p));
      list.appendChild(row);
    });
  }

  function navigateTo(page) {
    window.closeCommandPalette();
    window.location.href = page.href;
  }

  // Wire input handlers
  document.addEventListener('DOMContentLoaded', () => {
    const input = document.getElementById('cmdPaletteInput');
    if (!input) return;
    input.addEventListener('input', () => {
      selectedIndex = 0;
      render(input.value);
    });
    input.addEventListener('keydown', (e) => {
      if (e.key === 'ArrowDown') {
        e.preventDefault();
        if (results.length === 0) return;
        selectedIndex = (selectedIndex + 1) % results.length;
        render(input.value);
      } else if (e.key === 'ArrowUp') {
        e.preventDefault();
        if (results.length === 0) return;
        selectedIndex = (selectedIndex - 1 + results.length) % results.length;
        render(input.value);
      } else if (e.key === 'Enter') {
        e.preventDefault();
        const target = results[selectedIndex];
        if (target) navigateTo(target);
      }
    });
  });

  // Re-fetch state when palette opens (in case tabs changed)
  const origOpen = window.openCommandPalette;
  window.openCommandPalette = function() {
    selectedIndex = 0;
    refreshFromTabsState().then(() => {
      origOpen();
    });
  };
  window._cmdPaletteRender = render;

  refreshFromTabsState();
})();
```

(Make sure Fuse.js is loaded before this script. If the existing F1 help modal already loads `<script src="/static/vendor/fuse.js"></script>`, reuse it. If not, add the include in `_navbar.html` near the top of the script section.)

**Step 5: Run — expect PASS.**

```bash
python -m pytest tests/e2e/test_navigation.py -k "palette" -v
```

**Step 6: Commit**

```bash
git add vireo/templates/_navbar.html tests/e2e/test_navigation.py
git commit -m "navbar: command palette filtering, keyboard nav, and pinned indicator"
```

---

## Phase 5 — Hotkeys

### Task 5.1: `cmd+1..9` jumps to nth pinned tab

**Files:**
- Modify: `vireo/templates/_navbar.html` (the cmd+K keydown handler from 4.1)

**Step 1: Write failing test**

```python
def test_cmd1_jumps_to_first_pinned_tab(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/jobs")  # start somewhere not first
    page.keyboard.press("Meta+1")
    page.wait_for_url(f"{url}/browse", timeout=3000)
```

**Step 2: Run — expect FAIL.**

**Step 3: Implement** — extend the global `keydown` handler in `_navbar.html`:

```javascript
document.addEventListener('keydown', function(e) {
  const isMac = navigator.platform.toUpperCase().indexOf('MAC') >= 0;
  const mod = isMac ? e.metaKey : e.ctrlKey;
  if (mod && e.key.toLowerCase() === 'k') {
    e.preventDefault();
    window.openCommandPalette();
    return;
  }
  if (e.key === 'Escape') {
    const overlay = document.getElementById('commandPalette');
    if (overlay && !overlay.hasAttribute('hidden')) {
      window.closeCommandPalette();
      return;
    }
  }
  // cmd+1..9 → nth pinned tab
  if (mod && /^[1-9]$/.test(e.key)) {
    const tabs = (window._navTabs ? window._navTabs.getTabs() : []);
    const all = (window._navTabs ? window._navTabs.getAllPages() : []);
    const idx = parseInt(e.key, 10) - 1;
    if (idx < tabs.length) {
      e.preventDefault();
      const target = all.find(p => p.id === tabs[idx]);
      if (target) window.location.href = target.href;
    }
  }
  // cmd+W → close current tab
  if (mod && e.key.toLowerCase() === 'w') {
    const cur = (function() {
      const p = window.location.pathname;
      if (p.startsWith('/pipeline/review')) return 'pipeline_review';
      if (p === '/' || p.startsWith('/browse')) return 'browse';
      return (p.split('/')[1] || '').replace(/-/g, '_');
    })();
    const tabs = (window._navTabs ? window._navTabs.getTabs() : []);
    if (cur && tabs.includes(cur)) {
      e.preventDefault();
      // Reuse the unpin path: simulate click on the close button
      const closeBtn = document.querySelector(
        '.nav-tab[data-nav-id="' + cur + '"] .nav-tab-close'
      );
      if (closeBtn) closeBtn.click();
    }
  }
});
```

**Step 4: Run — expect PASS.**

```bash
python -m pytest tests/e2e/test_navigation.py::test_cmd1_jumps_to_first_pinned_tab -v
```

**Step 5: Commit**

```bash
git add vireo/templates/_navbar.html tests/e2e/test_navigation.py
git commit -m "navbar: cmd+1..9 jump to pinned tab; cmd+W closes current tab"
```

---

## Phase 6 — Cleanup of dead code

### Task 6.1: Remove `_auto_open_tab` and openable-page references

**Files:**
- Modify: `vireo/app.py`

**Step 1: Identify call sites**

```bash
grep -n "_auto_open_tab\|openable_pages\|OPENABLE_NAV_IDS" vireo/app.py
```

There are call sites in routes for `/lightroom`, `/workspace`, `/settings`, `/shortcuts`, `/keywords`, `/duplicates`, `/logs` (every openable page route auto-pinned).

**Step 2: Implement**

Delete the `_auto_open_tab` helper (~line 992-1001) and every call to it (`_auto_open_tab("lightroom")` etc. inside the route handlers). The routes themselves stay; just the auto-pin call disappears.

Also delete the old endpoints `api_open_tab` and `api_close_tab` (~line 3172-3192) — replaced by `pin`/`unpin`.

**Step 3: Update existing `test_tabs_api.py` tests**

In `vireo/tests/test_tabs_api.py`:
- **Delete** the `test_open_tab_endpoint_*`, `test_close_tab_endpoint_*`, and `test_visiting_*_url_auto_opens_tab` tests (lines 1-90). Their behaviors are now invariant-killed.
- The new pin/unpin/reorder/get tests (added in Phase 2) remain.

**Step 4: Run full test suite**

```bash
python -m pytest vireo/tests/test_tabs_api.py vireo/tests/test_app.py -v
```

Expected: all pass.

**Step 5: Commit**

```bash
git add vireo/app.py vireo/tests/test_tabs_api.py
git commit -m "remove _auto_open_tab and old open/close tab endpoints"
```

---

### Task 6.2: Remove old DB methods and `OPENABLE_NAV_IDS`

**Files:**
- Modify: `vireo/db.py` (delete `OPENABLE_NAV_IDS`, `DEFAULT_OPEN_TABS`, `get_open_tabs`, `open_tab`, `close_tab`)
- Modify: `tests/test_workspaces.py` (delete tests that referenced the old names)

**Step 1: Delete old code**

In `vireo/db.py`:
- Delete `OPENABLE_NAV_IDS` (line 46)
- Delete `DEFAULT_OPEN_TABS` (line 129)
- Delete `get_open_tabs`, `open_tab`, `close_tab` (lines 865-910)
- Update `create_workspace` to drop the legacy `open_tabs` insert column. Replace lines 702-712 with:

```python
def create_workspace(self, name, config_overrides=None, ui_state=None):
    """Create a new workspace. Returns the workspace id."""
    cur = self.conn.execute(
        """INSERT INTO workspaces (name, config_overrides, ui_state, tabs)
           VALUES (?, ?, ?, ?)""",
        (name,
         json.dumps(config_overrides) if config_overrides else None,
         json.dumps(ui_state) if ui_state else None,
         json.dumps(DEFAULT_TABS)),
    )
```

Drop the legacy `open_tabs` migration block (around line 603-611) — it still safely no-ops for fresh DBs but is dead code.

Add a new column-drop migration:

```python
# Migration: drop legacy open_tabs column (replaced by `tabs`).
try:
    self.conn.execute("SELECT open_tabs FROM workspaces LIMIT 0")
    self.conn.execute("ALTER TABLE workspaces DROP COLUMN open_tabs")
except sqlite3.OperationalError:
    pass  # column already absent (already dropped or fresh schema)
```

(Requires SQLite ≥ 3.35; verify with `python -c "import sqlite3; print(sqlite3.sqlite_version)"`. Python 3.14 ships with sufficient SQLite.)

**Step 2: Delete obsolete tests**

In `tests/test_workspaces.py`, delete:
- `test_workspaces_has_open_tabs_column` (line 812)
- `test_existing_workspaces_get_default_open_tabs_on_migration` (line 817)
- `test_new_workspace_gets_default_open_tabs` (line 847)
- `test_get_open_tabs_returns_default_for_new_workspace` (line 857)
- `test_get_open_tabs_returns_empty_list_when_null` (line 863)
- the `OPENABLE_NAV_IDS` assertion test (line 872)
- the rest of the `open_tab` / `close_tab` / `open_tabs_are_per_workspace` series (lines 884-935)

The Phase 1.3 tests cover the equivalent behavior under the new names.

**Step 3: Update `vireo/templates/_navbar.html`**

Remove now-unused CSS rules (`.nav-tools-btn`, `.nav-tools-menu`, `.nav-tools-item`, `.nav-tools-check`, `.nav-tab-divider`).

**Step 4: Remove the `/api/workspaces/active/nav-order` endpoint**

In `vireo/app.py:3088-3111`, delete `api_set_nav_order`. In `vireo/tests/test_app.py:2155-2178`, delete `test_nav_order_save_and_load` and `test_nav_order_rejects_non_list`.

**Step 5: Run full test suite**

```bash
python -m pytest vireo/tests/ tests/test_workspaces.py -v
```

Expected: all pass. No references to the old names remain.

**Step 6: Commit**

```bash
git add vireo/db.py vireo/app.py vireo/templates/_navbar.html vireo/tests/test_app.py tests/test_workspaces.py
git commit -m "remove legacy open_tabs / OPENABLE_NAV_IDS / nav_order code paths"
```

---

## Phase 7 — Manual smoke + integration checks

### Task 7.1: Manual browser smoke test

Run Vireo (against `~/.vireo/vireo.db` or a temp DB) at three viewport widths:

```bash
HOME=/tmp/vireo-smoke python3 vireo/app.py --db /tmp/vireo-smoke/test.db --port 8088 --thumb-dir /tmp/vireo-smoke/thumbs --no-browser
```

Open in a browser at each width and verify:

- **At 1920px**: all 9 default tabs visible, no `…` overflow button. Hover any tab — width does not change. `cmd+K` opens palette. Type "dup" → `Duplicates` is the top row.
- **At 1366px**: navbar fits; if not, `…` overflow button appears. Click it → menu lists hidden tabs. Click one → navigates to that page; navbar reshuffles to keep it visible.
- **At 1100px**: aggressive overflow. `…` menu has more entries. Hover an active tab — still no width change.
- **Drag-reorder**: drag `cull` to before `browse`; refresh page; order persists.
- **Ephemeral tab**: navigate via URL bar to `/keywords` (which isn't pinned by default). Italic `Keywords` tab appears at the right. Navigate to `/audit`. The italic tab swaps to `Audit`. Navigate back to `/browse`. Italic tab disappears.
- **Close active tab**: click `×` on `Browse` while you're on it. Navigates to the next tab in order. Close enough tabs to be on the last — closing it lands on `/browse` (now ephemeral).
- **`cmd+1..9`**: works for first 9 pinned tabs.
- **`cmd+W`**: closes current pinned tab.

If any of these fail, fix before proceeding.

**Stop the server** when done:
```bash
pkill -f "vireo/app.py.*8088"
```

---

### Task 7.2: Run full test suite

```bash
python -m pytest tests/test_workspaces.py vireo/tests/test_db.py vireo/tests/test_app.py vireo/tests/test_tabs_api.py tests/e2e/test_navigation.py -v
```

Expected: all pass. Address any regression.

Also run the broader suite per CLAUDE.md:

```bash
python -m pytest tests/test_workspaces.py vireo/tests/test_db.py vireo/tests/test_app.py vireo/tests/test_photos_api.py vireo/tests/test_edits_api.py vireo/tests/test_jobs_api.py vireo/tests/test_darktable_api.py vireo/tests/test_config.py -v
```

(Per the `pre-existing test failures` memory: known-flaky tests on main as of 2026-04-22 are not blockers — judge per case.)

---

### Task 7.3: Open PR

**Step 1: Push branch**

```bash
git push -u origin optional-nav-bounce
```

**Step 2: Create PR**

```bash
gh pr create --title "Unified navbar tabs + cmd+K palette" --body "$(cat <<'EOF'
## Summary

- Replaces the dual linger-pages / openable-tabs navbar model with a single user-curated tab list per workspace.
- Adds a `cmd+K` command palette for fuzzy-finding any of the 20 pages.
- Fixes the "bouncing on hover" bug at the root: close button is now absolutely positioned, so showing it doesn't change tab width and never triggers a flex re-layout.
- Adds overflow handling: when tabs don't fit, the tail collapses into a `…` dropdown.

Design: `docs/plans/2026-04-30-unified-tabs-design.md`
Plan: `docs/plans/2026-04-30-unified-tabs-plan.md`

## Migration

Every existing workspace's `tabs` is reset to the new 9-tab default (`browse, pipeline, pipeline_review, review, cull, jobs, highlights, misses, settings`). Per the solo-user-app convention, no preservation of prior `open_tabs` / `nav_order` customizations.

## Test plan

- [ ] Unit / integration: `pytest tests/test_workspaces.py vireo/tests/test_db.py vireo/tests/test_tabs_api.py vireo/tests/test_app.py -v`
- [ ] E2E: `pytest tests/e2e/test_navigation.py -v`
- [ ] Manual smoke at 1920px / 1366px / 1100px viewports (see `docs/plans/2026-04-30-unified-tabs-plan.md` Task 7.1).
- [ ] `cmd+K` opens palette; arrow keys + Enter navigate; Esc closes.
- [ ] `cmd+1..9` jump to nth pinned tab; `cmd+W` closes current.
- [ ] Drag-reorder persists across reload.
- [ ] Ephemeral tab appears when visiting an unpinned page; replaced by next unpinned visit.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

---

## Out of scope (deliberate follow-ups)

- Palette includes workspaces (jump between workspaces from `cmd+K`)
- Palette includes actions (toggle theme, run scan, etc.)
- Per-pinned-tab custom labels
- Per-workspace different default tab sets
- Navbar theming
